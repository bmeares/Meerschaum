//! MCP retains the original authenticated registration source across an awaited hook.
use crate::common;
use async_trait::async_trait;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_server::mcp::{ConnectorRegister, McpBackend, McpConfig, McpServer, SqlReadError};
use serde_json::{Value as Json, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

fn map(value: Json) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

#[derive(Default)]
struct Hook {
    entered: Notify,
    release: Notify,
}
#[async_trait]
impl McpBackend for Hook {
    async fn sync_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::fail("Unused fixture")
    }
    async fn verify_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::fail("Unused fixture")
    }
    async fn read_sql(&self, _: &str, _: &str, _: usize) -> Result<Vec<Value>, SqlReadError> {
        Err(SqlReadError::Unsupported)
    }
    async fn connector_register(&self, _: &Pipe) -> ConnectorRegister {
        self.entered.notify_one();
        if tokio::time::timeout(Duration::from_secs(10), self.release.notified())
            .await
            .is_err()
        {
            return ConnectorRegister::Unavailable("Fixture hook release timed out".into());
        }
        ConnectorRegister::Parameters(map(json!({"columns":{"id":"hook"},"tags":["from_hook"]})))
    }
}

fn request(token: &str, metric: &str) -> axum::http::Request<axum::body::Body> {
    common::req("POST","/mcp",Some(token),Some(("application/json",json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
        "params":{"name":"register_pipe","arguments":{"connector_keys":"plugin:fixture","metric_key":metric,"columns":{"id":"caller"}}}}).to_string().into_bytes())))
}

async fn wait_for_hook(hook: &Hook, pending: &mut tokio::task::JoinHandle<common::Resp>) {
    tokio::select! {
        () = hook.entered.notified() => {},
        response = pending => {
            let response = response.expect("The MCP request task panicked before the hook");
            panic!("The MCP request finished before its hook: {} {}", response.status, response.text());
        },
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("The MCP request remained pending without reaching its hook"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_revocation_during_registration_hook_never_reaches_metadata_write() {
    for replace in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let instance = Arc::new(common::MemoryInstance::new("fixture"));
        instance.seed_user("alice", "pass", None);
        instance.seed_user_attributes(
            "alice",
            map(json!({"role":"custom","scopes":["pipes:register"]})),
        );
        let state =
            meerschaum_server::AppState::builder(common::config(root.path()), instance.clone())
                .secret(common::SECRET.to_vec())
                .build()
                .unwrap();
        let hook = Arc::new(Hook::default());
        let app = meerschaum_server::app_with_mcp(
            state,
            Arc::new(McpServer::new(McpConfig::default()).with_backend(hook.clone())),
        );
        let token = common::login(&app, "alice", "pass", None).await;
        let mut pending = {
            let app = app.clone();
            tokio::spawn(async move { common::send(&app, request(&token, "revoked")).await })
        };
        wait_for_hook(&hook, &mut pending).await;
        if replace {
            assert!(
                instance
                    .delete_user(&meerschaum_core::users::User::new("alice", None))
                    .await
                    .success
            );
            instance.seed_user("alice", "pass", None);
            instance.seed_user_attributes(
                "alice",
                map(json!({"role":"custom","scopes":["pipes:register"]})),
            );
        } else {
            instance.seed_user_attributes(
                "alice",
                map(json!({"role":"custom","scopes":["pipes:read"]})),
            );
        }
        hook.release.notify_one();
        let response = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(response.status.as_u16(), 401 | 403)
                || response.json()["result"]["isError"] == true
                || response.json().get("error").is_some(),
            "Revoked registration continued: {}",
            response.text()
        );
        let pipe = Pipe::builder("plugin:fixture", "revoked")
            .instance("sql:fixture")
            .build()
            .unwrap();
        assert!(
            instance.get_pipe_id(&pipe).await.unwrap().is_none(),
            "An old source proof wrote a new registration"
        );
    }
}

async fn expiry_at_hook(admin: bool) {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        if admin {
            map(json!({"role":"admin"}))
        } else {
            map(json!({"role":"custom","scopes":["pipes:register"]}))
        },
    );
    let state = meerschaum_server::AppState::builder(common::config(root.path()), instance.clone())
        .secret(common::SECRET.to_vec())
        .build()
        .unwrap();
    let hook = Arc::new(Hook::default());
    let app = meerschaum_server::app_with_mcp(
        state,
        Arc::new(McpServer::new(McpConfig::default()).with_backend(hook.clone())),
    );
    let original = common::login(&app, "alice", "pass", None).await;
    let payload = meerschaum_server::auth::jwt_decode(
        original.strip_prefix("Bearer ").unwrap(),
        common::SECRET,
        chrono::Utc::now(),
    )
    .unwrap();
    let expiry = chrono::Utc::now().timestamp() + 2;
    let mut claims = map(serde_json::to_value(payload).unwrap());
    claims.insert("exp".into(), expiry.into());
    let token = format!(
        "Bearer {}",
        meerschaum_server::auth::jwt_encode(&claims, common::SECRET)
    );
    let mut pending = {
        let app = app.clone();
        tokio::spawn(async move { common::send(&app, request(&token, "expired")).await })
    };
    wait_for_hook(&hook, &mut pending).await;
    while chrono::Utc::now().timestamp() <= expiry {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    hook.release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.status == 401
            || response.json()["result"]["isError"] == true
            || response.json().get("error").is_some(),
        "Expired original credential continued registration: {}",
        response.text()
    );
    let pipe = Pipe::builder("plugin:fixture", "expired")
        .instance("sql:fixture")
        .build()
        .unwrap();
    assert!(instance.get_pipe_id(&pipe).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_http_jwt_expiring_during_hook_never_reaches_metadata_write() {
    expiry_at_hook(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_admin_jwt_expiring_during_hook_never_reaches_metadata_write() {
    expiry_at_hook(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_register_only_source_keeps_hook_parameters_and_canonical_owner_without_extra_scopes()
{
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"custom","scopes":["pipes:register"]})),
    );
    let state = meerschaum_server::AppState::builder(common::config(root.path()), instance.clone())
        .secret(common::SECRET.to_vec())
        .build()
        .unwrap();
    let hook = Arc::new(Hook::default());
    let app = meerschaum_server::app_with_mcp(
        state,
        Arc::new(McpServer::new(McpConfig::default()).with_backend(hook.clone())),
    );
    let token = common::login(&app, "alice", "pass", None).await;
    let mut pending = {
        let app = app.clone();
        tokio::spawn(async move { common::send(&app, request(&token, "positive")).await })
    };
    wait_for_hook(&hook, &mut pending).await;
    hook.release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        response.json()["result"]["isError"],
        true,
        "{}",
        response.text()
    );
    let pipe = Pipe::builder("plugin:fixture", "positive")
        .instance("sql:fixture")
        .build()
        .unwrap();
    let attributes = instance.get_pipe_attributes(&pipe).await.unwrap();
    let parameters = attributes["parameters"].as_map().unwrap();
    assert_eq!(
        parameters["columns"].get("id"),
        Some(&Value::Str("caller".into()))
    );
    assert_eq!(parameters["tags"], Value::List(vec!["from_hook".into()]));
    assert_eq!(
        parameters["access"].get("owner"),
        Some(&Value::Str("alice".into()))
    );
}
