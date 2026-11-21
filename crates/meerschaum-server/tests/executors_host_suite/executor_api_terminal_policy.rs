//! API terminal commands retain their original data ceiling and never become system shells.
use crate::common;
use axum::http::{HeaderMap, StatusCode};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::{callbacks, terminal};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

struct Actions;
#[async_trait::async_trait]
impl meerschaum_server::backends::ActionsBackend for Actions {
    fn list(&self) -> Vec<String> {
        vec!["show".into(), "start".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> meerschaum_core::SuccessTuple {
        panic!("Terminal validation must never dispatch an action.");
    }
}
fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
async fn caller(state: &meerschaum_server::AppState) -> meerschaum_server::access::Access {
    let bearer = common::login(
        &meerschaum_server::app(state.clone()),
        "alice",
        "pass",
        None,
    )
    .await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", bearer.parse().unwrap());
    meerschaum_server::access::resolve(state, &headers)
        .await
        .unwrap()
}
async fn ask(app: &axum::Router, bearer: &str, args: &[&str]) -> common::Resp {
    common::send(
        app,
        common::req(
            "POST",
            "/execute/validate",
            Some(bearer),
            Some((
                "application/json",
                json!({"sysargs":args}).to_string().into_bytes(),
            )),
        ),
    )
    .await
}

#[tokio::test]
async fn everyone_api_terminal_validation_has_no_execution_or_recursive_action_route() {
    let root = tempfile::tempdir().unwrap();
    let mut config = common::config(root.path());
    config.actions_non_admin = true;
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes("alice", map(json!({"role":"operator"})));
    let state = meerschaum_server::AppState::builder(config, instance)
        .secret(common::SECRET.to_vec())
        .actions(Arc::new(Actions))
        .build()
        .unwrap();
    let source = caller(&state).await;
    let grant = callbacks::register(
        &state,
        &source,
        Uuid::new_v4(),
        None,
        &["webterm:use", "actions:execute", "jobs:execute"],
        callbacks::Purpose::Command,
    )
    .await
    .unwrap();
    terminal::api_source(&state, grant.id, grant.launch_id, None)
        .await
        .unwrap();
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let bearer = format!("Bearer {}", credential.token);
    let app = meerschaum_server::app(state.clone());
    let allowed = ask(&app, &bearer, &["show", "version"]).await;
    assert_eq!(allowed.status, StatusCode::OK, "{}", allowed.text());
    assert_eq!(allowed.json()["success"], true);
    for command in [
        vec!["sh", "touch", "/never"],
        vec!["python"],
        vec!["touch", "/never"],
        vec!["start", "api"],
        vec!["show", "config"],
        vec!["show", "version", "--_mrsm-confined-python"],
        vec!["show", "version", "-e", "systemd"],
    ] {
        let denied = ask(&app, &bearer, &command).await;
        assert_eq!(
            denied.status,
            StatusCode::FORBIDDEN,
            "{command:?}: {}",
            denied.text()
        );
    }
    for path in ["/execute", "/actions/show"] {
        let denied = common::send(
            &app,
            common::req(
                "POST",
                path,
                Some(&bearer),
                Some(("application/json", b"{}".to_vec())),
            ),
        )
        .await;
        assert_eq!(
            denied.status,
            StatusCode::FORBIDDEN,
            "{path}: {}",
            denied.text()
        );
    }
    callbacks::suspend(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    assert_eq!(
        ask(&app, &bearer, &["show", "version"]).await.status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn registered_api_terminal_keeps_strict_command_policy_after_code_grants_and_rechecks_pipe_scopes()
 {
    let root = tempfile::tempdir().unwrap();
    let mut config = common::config(root.path());
    config.actions_non_admin = true;
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"operator","primary_team":"Harbor"})),
    );
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .secret(common::SECRET.to_vec())
        .actions(Arc::new(Actions))
        .build()
        .unwrap();
    let mut team = Team::new("Harbor");
    // Isolated non-code public permissions remain fenced until the full B profile ships.
    // A registered ordinary host team exercises the Command ceiling without that fence.
    team.attributes = map(json!({"isolated":false,"plugins_write":false}));
    team.members.insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut team).await.success);
    let team_id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
    let pipe = meerschaum_core::Pipe::builder("manual:fixture", "owned")
        .instance("sql:fixture")
        .parameters(map(
            json!({"access":{"owner":"alice","team":"Harbor","visibility":"private"}}),
        ))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let source = caller(&state).await;
    let grant = callbacks::register(
        &state,
        &source,
        Uuid::new_v4(),
        Some(team_id),
        &["webterm:use", "actions:execute", "jobs:execute"],
        callbacks::Purpose::Command,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let bearer = format!("Bearer {}", credential.token);
    let app = meerschaum_server::app(state.clone());
    let command = ["show", "pipes", "-c", "manual:fixture", "-m", "owned"];
    assert_eq!(ask(&app, &bearer, &command).await.status, StatusCode::OK);
    team.attributes.insert("plugins_write".into(), true.into());
    assert!(instance.edit_team(&team).await.success);
    let identity = terminal::api_source(&state, grant.id, grant.launch_id, Some(team_id))
        .await
        .unwrap();
    assert!(meerschaum_server::team_execution::code_running_team(&identity.caller()).is_none());
    assert_eq!(
        ask(&app, &bearer, &["sh", "printf forbidden"]).await.status,
        StatusCode::FORBIDDEN
    );
    let scopes = meerschaum_core::access::Role::Operator
        .permissions()
        .into_iter()
        .filter(|scope| scope != "pipes:read")
        .collect::<Vec<_>>();
    instance.seed_user_scopes(
        "alice",
        &scopes.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let denied = ask(&app, &bearer, &command).await;
    assert_eq!(denied.status, StatusCode::FORBIDDEN, "{}", denied.text());
    assert!(
        terminal::api_source(&state, grant.id, grant.launch_id, Some(Uuid::new_v4()))
            .await
            .is_err()
    );
    assert!(
        instance
            .delete_user(&meerschaum_core::users::User::new("alice", None))
            .await
            .success
    );
    assert_eq!(
        ask(&app, &bearer, &["show", "version"]).await.status,
        StatusCode::UNAUTHORIZED
    );
}
