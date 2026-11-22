use super::*;
use crate::access::Access;
use crate::auth::Principal;
use crate::executor::source_ceiling::Ceiling;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::{Map, Pipe, Value, users::User};
use serde_json::{Value as Json, json};
use std::sync::Arc;
use tower::ServiceExt;

fn map(value: Json) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

async fn send(
    app: &axum::Router,
    path: &str,
    token: Option<&str>,
    body: String,
) -> (StatusCode, Json) {
    let mut request = Request::post(path).header(
        "content-type",
        if path == "/login" {
            "application/x-www-form-urlencoded"
        } else {
            "application/json"
        },
    );
    if let Some(token) = token {
        request = request.header("authorization", token);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn login(app: &axum::Router, state: &AppState, username: &str) -> (String, Access) {
    let (status, body) = send(
        app,
        "/login",
        None,
        format!("username={username}&password=synthetic-pass"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let token = format!("Bearer {}", body["access_token"].as_str().unwrap());
    let mut headers = HeaderMap::new();
    headers.insert("authorization", token.parse().unwrap());
    (
        token,
        crate::access::resolve(state, &headers).await.unwrap(),
    )
}

#[tokio::test]
async fn registration_projection_keeps_isolated_multiteam_and_derived_source_ceilings() {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(
        meerschaum_connector_sql::SqlConnector::from_uri(
            "fixture",
            &format!(
                "sqlite:///{}",
                root.path().join("registration.db").display()
            ),
        )
        .await
        .unwrap(),
    );
    for (name, attributes) in [
        (
            "alice",
            json!({"role":"custom","scopes":["pipes:register"],"primary_team":"A"}),
        ),
        ("open", json!({"role":"custom","scopes":["pipes:register"]})),
    ] {
        let mut user = User::new(name, Some("synthetic-pass"));
        user.user_type = Some("regular".into());
        user.attributes = Some(map(attributes));
        assert!(instance.register_user(&mut user).await.success);
    }
    let mut a = Team::new("A");
    a.members.insert("alice".into(), TeamRole::Member);
    a.attributes = map(
        json!({"isolated":true,"plugins":["a"],"connectors":["sql:fixture"],"instance":"sql:fixture"}),
    );
    assert!(instance.register_team(&mut a).await.success);
    let mut b = Team::new("B");
    b.members.insert("alice".into(), TeamRole::Member);
    b.attributes = map(
        json!({"isolated":true,"plugins":["b"],"connectors":["sql:sibling"],"instance":"sql:sibling"}),
    );
    assert!(instance.register_team(&mut b).await.success);
    let mut config = crate::ServerConfig::for_root("sql:fixture", root.path());
    config.allow_multiple_instances = true;
    config.allowed_instance_keys = vec!["sql:*".into()];
    config.connectors = map(
        json!({"sql":{"fixture":{"teams":["A"]},"sibling":{"teams":["B"]}},"plugin":{"a":{"teams":["A"]},"b":{"teams":["B"]}}}),
    );
    let state = AppState::builder(config, instance.clone())
        .secret(b"0123456789abcdef0123456789abcdef".to_vec())
        .build()
        .unwrap();
    let app = crate::app_with_mcp(
        state.clone(),
        Arc::new(crate::mcp::McpServer::new(crate::mcp::McpConfig::default())),
    );
    let (token, source) = login(&app, &state, "alice").await;
    assert!(source.principal.is_isolated());
    let team_id = Uuid::parse_str(a.id.as_deref().unwrap()).unwrap();
    let grant = callbacks::register(
        &state,
        &source,
        Uuid::new_v4(),
        Some(team_id),
        &["pipes:register"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let identity = callbacks::load_identity(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let own = registration_access(&state, &identity, "plugin:a", "sql:fixture").unwrap();
    assert_eq!(own.principal.team_boundary.as_deref(), Some("A"));
    assert!(
        !own.principal.grants.plugins.contains("b")
            && !own.principal.grants.connectors.contains("sql:sibling")
    );
    assert!(registration_access(&state, &identity, "plugin:b", "sql:fixture").is_err());
    assert!(registration_access(&state, &identity, "plugin:a", "sql:sibling").is_err());
    for (source_keys, instance_keys) in [("plugin:b", "sql:fixture"), ("plugin:a", "sql:sibling")] {
        let (status, body) = send(&app, "/mcp", Some(&token), json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"register_pipe","arguments":{"connector_keys":source_keys,"metric_key":"forbidden","instance_keys":instance_keys}}}).to_string()).await;
        assert!(
            status == StatusCode::FORBIDDEN
                || body["result"]["isError"] == true
                || body.get("error").is_some(),
            "Cross-Team registration succeeded: {body}"
        );
    }
    let forbidden = Pipe::builder("plugin:b", "forbidden")
        .instance("sql:fixture")
        .build()
        .unwrap();
    assert!(instance.get_pipe_id(&forbidden).await.unwrap().is_none());

    // This is a genuine first-party open-side proof, followed by a genuine derived grant.
    // Fresh original visibility must not let its child adopt any previously uncaptured plugin.
    let (_, open) = login(&app, &state, "open").await;
    assert!(!open.principal.is_isolated());
    let parent = callbacks::register_with_sources(
        &state,
        &open,
        Uuid::new_v4(),
        None,
        &["pipes:register"],
        Purpose::Command,
        Ceiling::default(),
    )
    .await
    .unwrap();
    let parent = callbacks::load_identity(&state, parent.id, parent.launch_id)
        .await
        .unwrap();
    let child = callbacks::register_with_sources(
        &state,
        &parent.caller(),
        Uuid::new_v4(),
        None,
        &["pipes:register"],
        Purpose::Command,
        Ceiling::default(),
    )
    .await
    .unwrap();
    let child = callbacks::load_identity(&state, child.id, child.launch_id)
        .await
        .unwrap();
    assert!(child.grant.depth > 0 && matches!(child.caller().auth, Some(Principal::Execution(_))));
    assert!(meerschaum_core::access::can_use_plugin(
        &child.source_access().principal,
        "shared"
    ));
    assert!(registration_access(&state, &child, "plugin:shared", "sql:public").is_err());
    assert!(registration_access(&state, &child, "plugin:a", "sql:fixture").is_err());
    assert!(registration_access(&state, &child, "plugin:b", "sql:sibling").is_err());
}
