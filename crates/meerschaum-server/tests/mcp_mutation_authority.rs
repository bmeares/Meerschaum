//! MCP keeps the presented HTTP credential through actual native database lock waits.
mod common;

use std::{sync::Arc, time::Duration};

use meerschaum_connector::{DataQuery, InstanceConnector, SyncOptions};
use meerschaum_connector_sql::{SqlConnector, driver::SqlDriver, uri::ConnectionSpec};
use meerschaum_core::{
    Map, Pipe, Value,
    access::{Team, TeamRole},
};
use meerschaum_server::{
    AppState, auth,
    backends::InstanceResolver,
    mcp::{McpConfig, McpServer},
};
use serde_json::{Value as Json, json};

fn map(value: Json) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

struct Data(Arc<SqlConnector>);
impl InstanceResolver for Data {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        (keys == "sql:data").then(|| self.0.clone() as Arc<dyn InstanceConnector>)
    }
}

fn request(token: &str, operation: &str) -> axum::http::Request<axum::body::Body> {
    let mut arguments = json!({"connector_keys":"manual:posted", "metric_key":"seeded",
        "instance_keys":"sql:data"});
    if operation == "sync_documents" {
        arguments["documents"] = json!([{"id":2}]);
    } else if operation == "edit_pipe" {
        arguments["parameters"] = json!({"tags":["accepted-after-fresh-login"]});
    }
    common::req(
        "POST",
        "/mcp",
        Some(token),
        Some((
            "application/json",
            json!({
                "jsonrpc":"2.0", "id":1, "method":"tools/call", "params":{
                    "name":operation, "arguments":arguments
                }
            })
            .to_string()
            .into_bytes(),
        )),
    )
}

fn succeeded(response: &common::Resp) -> bool {
    let body = response.json();
    response.status.is_success()
        && body.get("error").is_none()
        && body["result"]["isError"] != true
        && body["result"]["content"].as_array().is_some_and(|content| {
            content
                .iter()
                .filter_map(|entry| entry["text"].as_str())
                .any(|text| {
                    serde_json::from_str::<Json>(text).is_ok_and(|result| result["success"] == true)
                })
        })
}

#[tokio::test]
async fn native_mcp_mutation_dispatch_future_stays_within_the_default_stack_budget() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = common::state_with(common::config(root.path()));
    let server = McpServer::new(McpConfig::default());
    let caller = meerschaum_server::mcp::Caller::stdio();
    let cx = meerschaum_server::mcp::tools::ToolCx {
        server: &server,
        state: &state,
        caller: &caller,
    };
    let arguments = Map::new();
    let future = meerschaum_server::mcp::tools::call("register_pipe", &arguments, &cx);
    let bytes = std::mem::size_of_val(&future);
    assert!(
        bytes <= 128 * 1024,
        "MCP mutation dispatch future is {bytes} bytes; box native admission boundaries rather than increasing the Tokio stack."
    );
}

async fn run_after_lock(change: &str, operations: &[&str]) {
    for operation in operations {
        let root = tempfile::tempdir().unwrap();
        let auth_store = Arc::new(common::MemoryInstance::new("fixture"));
        auth_store.seed_user("alice", "pass", None);
        let rights = |permissions: &[&str]| {
            meerschaum_core::access::attributes_with_permissions(
                &map(json!({"primary_team":"Data"})),
                permissions,
            )
        };
        let permissions = [
            "pipes:read",
            "pipes:write",
            "pipes:register",
            "pipes:delete",
            "pipes:drop",
        ];
        auth_store.seed_user_attributes("alice", rights(&permissions));
        let mut team = Team::new("Data");
        team.members.insert("alice".into(), TeamRole::Member);
        assert!(auth_store.register_team(&mut team).await.success);
        let mut spec = ConnectionSpec::from_uri(
            &format!(
                "sqlite:///{}",
                root.path().join("mcp-owned-data.db").display()
            ),
            Some("data"),
            None,
        )
        .unwrap();
        spec.pool_size = 1;
        spec.max_overflow = 0;
        let data = Arc::new(SqlConnector::connect("data", spec.clone()).await.unwrap());
        let mut pipe = Pipe::builder("manual:posted", "seeded")
            .instance("sql:data")
            .build()
            .unwrap();
        pipe.set_parameters(map(json!({"columns":{"id":"id"}, "access":{
            "owner":"alice", "team":"Data", "visibility":"private"
        }})));
        assert!(data.register_pipe(&pipe).await.success);
        let rows = meerschaum_kernel::docs::frame_from_docs(&[[(
            "id".into(),
            meerschaum_kernel::value::Value::Int(1),
        )]
        .into_iter()
        .collect()])
        .unwrap();
        assert!(
            data.sync_pipe(&pipe, rows, &SyncOptions::default())
                .await
                .success
        );
        let before = data.get_pipe_attributes(&pipe).await.unwrap();
        let mut config = common::config(root.path());
        config.allow_multiple_instances = true;
        config.allowed_instance_keys = vec!["sql:*".into()];
        config.connectors = map(json!({"sql":{"data":{"flavor":"sqlite"}}}));
        let state = AppState::builder(config, auth_store.clone())
            .resolver(Arc::new(Data(data.clone())))
            .secret(common::SECRET.to_vec())
            .build()
            .unwrap();
        let app =
            meerschaum_server::app_with_mcp(state, Arc::new(McpServer::new(McpConfig::default())));
        let token = common::login(&app, "alice", "pass", None).await;
        let expiry = chrono::Utc::now().timestamp() + 2;
        let presented = if change == "expiry" {
            let payload = auth::jwt_decode(
                token.strip_prefix("Bearer ").unwrap(),
                common::SECRET,
                chrono::Utc::now(),
            )
            .unwrap();
            let mut claims = map(serde_json::to_value(payload).unwrap());
            claims.insert("exp".into(), expiry.into());
            format!("Bearer {}", auth::jwt_encode(&claims, common::SECRET))
        } else {
            token
        };
        let holder = SqlDriver::connect(&spec).await.unwrap();
        let held = holder.begin_write().await.unwrap();
        let pending_request = request(&presented, operation);
        let pending_app = app.clone();
        let mut pending =
            tokio::spawn(async move { common::send(&pending_app, pending_request).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err(),
            "{operation}/{change} finished before its held native writer lock"
        );
        if change == "expiry" {
            while chrono::Utc::now().timestamp() <= expiry {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        } else {
            auth_store.seed_user_attributes("alice", rights(&["pipes:read"]));
        }
        held.rollback().await.unwrap();
        let response = tokio::time::timeout(Duration::from_secs(10), pending)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !succeeded(&response),
            "{operation}/{change}: {}",
            response.text()
        );
        assert_eq!(
            data.get_pipe_attributes(&pipe).await.unwrap(),
            before,
            "{operation}/{change}: metadata changed after lost authority"
        );
        assert_eq!(
            data.get_pipe_rowcount(&pipe, &DataQuery::default())
                .await
                .unwrap(),
            Some(1),
            "{operation}/{change}: rows changed after lost authority"
        );
        auth_store.seed_user_attributes("alice", rights(&permissions));
        let fresh = common::login(&app, "alice", "pass", None).await;
        let response = tokio::time::timeout(
            Duration::from_secs(10),
            common::send(&app, request(&fresh, operation)),
        )
        .await
        .unwrap();
        assert!(
            succeeded(&response),
            "fresh {operation}/{change}: {}",
            response.text()
        );
        holder.close().await;
        data.driver().close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_native_writes_recheck_revoked_permissions_after_database_lock_wait() {
    run_after_lock(
        "revoke",
        &[
            "sync_documents",
            "edit_pipe",
            "deduplicate_pipe",
            "clear_pipe",
            "drop_pipe",
            "delete_pipe",
        ],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_native_writes_reject_original_jwt_expiry_after_database_lock_wait() {
    run_after_lock("expiry", &["sync_documents", "edit_pipe", "clear_pipe"]).await;
}
