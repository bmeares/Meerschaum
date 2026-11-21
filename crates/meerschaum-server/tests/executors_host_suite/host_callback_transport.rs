//! A host callback may reach shared API instances while retaining its original team boundary.
use crate::common;
use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use meerschaum_connector::{Connector, InstanceConnector};
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_server::backends::InstanceResolver;
use meerschaum_server::executor::callbacks::{self, Purpose};
use serde_json::json;
use uuid::Uuid;

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

struct Instances(Vec<(String, Arc<dyn InstanceConnector>)>);
impl InstanceResolver for Instances {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        self.0
            .iter()
            .find(|(name, _)| name == keys)
            .map(|(_, instance)| instance.clone())
    }
}

async fn register(instance: &common::MemoryInstance, metric: &str, team: &str) {
    let pipe = Pipe::builder("manual:host", metric)
        .instance(&instance.connector_keys())
        .parameters(map(json!({"columns":{"id":"id"}, "access":{
            "owner":"alice", "team":team, "visibility":"private"
        }})))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
}

#[tokio::test]
async fn fresh_host_transport_keeps_default_and_shared_instances_but_excludes_sibling_identity() {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    let shared = Arc::new(common::MemoryInstance::new("shared"));
    let sibling = Arc::new(common::MemoryInstance::new("sibling"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"operator", "primary_team":"Harbor"})),
    );
    let mut team = Team::new("Harbor");
    team.members.insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut team).await.success);
    let mut other = Team::new("Other");
    other.members.insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut other).await.success);
    register(&instance, "own", "Harbor").await;
    register(&instance, "other", "Other").await;
    register(&shared, "shared", "Harbor").await;
    // An object that would pass the callback's team ACL still cannot authorize a sibling DB.
    register(&sibling, "decoy", "Harbor").await;
    let mut config = common::config(root.path());
    config.allow_multiple_instances = true;
    config.allowed_instance_keys = vec!["sql:*".into()];
    config.connectors = map(json!({"sql":{
        "fixture":{"password":"synthetic-main-private"},
        "shared":{"password":"synthetic-shared-private"},
        "sibling":{"password":"synthetic-sibling-private", "teams":["Other"]},
        "unserved":{"password":"synthetic-unserved-private"}
    }}));
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .resolver(Arc::new(Instances(vec![
            ("sql:shared".into(), shared.clone()),
            ("sql:sibling".into(), sibling.clone()),
        ])))
        .secret(common::SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state.clone());
    let source = common::login(&app, "alice", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", source.parse().unwrap());
    let caller = meerschaum_server::access::resolve(&state, &headers)
        .await
        .unwrap();
    assert!(meerschaum_server::access::guard::may_use_connector(
        &state,
        &caller,
        "sql:sibling"
    ));
    for path in [
        "/pipes/manual:host/own/None/data",
        "/pipes/manual:host/other/None/data",
        "/pipes/manual:host/shared/None/data?instance_keys=sql:shared",
        "/pipes/manual:host/decoy/None/data?instance_keys=sql:sibling",
    ] {
        let result = common::send(
            &app,
            common::req(
                "POST",
                path,
                Some(&source),
                Some((
                    "application/json",
                    json!([{"id":1}]).to_string().into_bytes(),
                )),
            ),
        )
        .await;
        assert_eq!(
            result.status,
            StatusCode::OK,
            "Seed {path}: {}",
            result.text()
        );
        assert_eq!(result.json()[0], true, "Seed {path}: {}", result.text());
    }
    for path in [
        "/pipes/manual:host/other/None/data",
        "/pipes/manual:host/decoy/None/data?instance_keys=sql:sibling",
    ] {
        let result = common::send(&app, common::req("GET", path, Some(&source), None)).await;
        assert_eq!(
            result.status,
            StatusCode::OK,
            "Original source {path}: {}",
            result.text()
        );
    }
    let grant = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        Some(Uuid::parse_str(team.id.as_deref().unwrap()).unwrap()),
        &["jobs:execute"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let identity = callbacks::load_identity(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let bounded = identity.caller();
    assert_eq!(bounded.principal.team_boundary.as_deref(), Some("Harbor"));
    assert!(!bounded.is_admin());
    for keys in ["sql:fixture", "sql:shared"] {
        assert!(identity.transport_connectors().contains(keys));
        assert!(meerschaum_server::access::guard::may_use_connector(
            &state, &bounded, keys
        ));
    }
    assert!(!identity.transport_connectors().contains("sql:sibling"));
    assert!(!meerschaum_server::access::guard::may_use_connector(
        &state,
        &bounded,
        "sql:sibling"
    ));
    let token = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let bearer = format!("Bearer {}", token.token);
    for (path, expected) in [
        ("/pipes/manual:host/own/None/data", StatusCode::OK),
        (
            "/pipes/manual:host/shared/None/data?instance_keys=sql:shared",
            StatusCode::OK,
        ),
        ("/pipes/manual:host/other/None/data", StatusCode::NOT_FOUND),
        (
            "/pipes/manual:host/decoy/None/data?instance_keys=sql:sibling",
            StatusCode::FORBIDDEN,
        ),
    ] {
        let result = common::send(&app, common::req("GET", path, Some(&bearer), None)).await;
        assert_eq!(result.status, expected, "{path}: {}", result.text());
    }
    let labels = common::send(
        &app,
        common::req("GET", "/connectors/sql", Some(&bearer), None),
    )
    .await;
    assert_eq!(labels.status, StatusCode::OK, "{}", labels.text());
    assert!(!labels.text().contains("sibling"));
    assert!(!labels.text().contains("private"));
    // A child's flattened origin cannot recover the source's sibling-team membership.
    let child = callbacks::register(
        &state,
        &bounded,
        Uuid::new_v4(),
        grant.team_id,
        &["jobs:execute"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let child = callbacks::load_identity(&state, child.id, child.launch_id)
        .await
        .unwrap();
    assert_eq!(
        child.caller().principal.team_boundary.as_deref(),
        Some("Harbor")
    );
    assert!(!child.transport_connectors().contains("sql:sibling"));
    // Source permissions are resolved afresh even while a previously issued bearer is valid.
    instance.seed_user_attributes(
        "alice",
        map(json!({"primary_team":"Harbor", "scopes":["jobs:execute","connectors:read"]})),
    );
    let result = common::send(
        &app,
        common::req(
            "GET",
            "/pipes/manual:host/own/None/data",
            Some(&bearer),
            None,
        ),
    )
    .await;
    assert_eq!(result.status, StatusCode::FORBIDDEN, "{}", result.text());
    team.members.shift_remove("alice");
    assert!(instance.edit_team(&team).await.success);
    assert!(
        callbacks::load_identity(&state, grant.id, grant.launch_id)
            .await
            .is_err()
    );
    let result = common::send(
        &app,
        common::req("GET", "/connectors/sql", Some(&bearer), None),
    )
    .await;
    assert_eq!(result.status, StatusCode::UNAUTHORIZED, "{}", result.text());
}

#[tokio::test]
async fn served_policy_and_jobs_only_purpose_never_gain_shared_transport_capability() {
    let root = tempfile::tempdir().unwrap();
    let mut config = common::config(root.path());
    config.allow_multiple_instances = false;
    config.connectors = map(json!({"sql":{"fixture":{},"unserved":{}}}));
    let (state, instance) = common::state_with(config);
    let mut team = Team::new("Harbor");
    assert!(instance.register_team(&mut team).await.success);
    let app = meerschaum_server::app(state.clone());
    let bearer = common::login(&app, "admin", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", bearer.parse().unwrap());
    let caller = meerschaum_server::access::resolve(&state, &headers)
        .await
        .unwrap();
    for purpose in [Purpose::Jobs, Purpose::Command] {
        let grant = callbacks::register(
            &state,
            &caller,
            Uuid::new_v4(),
            Some(Uuid::parse_str(team.id.as_deref().unwrap()).unwrap()),
            &["jobs:execute"],
            purpose,
        )
        .await
        .unwrap();
        let identity = callbacks::load_identity(&state, grant.id, grant.launch_id)
            .await
            .unwrap();
        assert!(!identity.transport_connectors().contains("sql:unserved"));
        assert_eq!(
            identity.transport_connectors().contains("sql:fixture"),
            purpose == Purpose::Command
        );
        assert_eq!(
            identity.caller().principal.team_boundary.as_deref(),
            Some("Harbor")
        );
        assert!(!identity.caller().is_admin());
    }
}

struct ReadyTeam;
#[async_trait::async_trait]
impl meerschaum_server::backends::ActionsBackend for ReadyTeam {
    fn list(&self) -> Vec<String> {
        vec!["show".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> meerschaum_core::SuccessTuple {
        panic!("This admission fixture must never execute its command.")
    }
    async fn executor_ready(&self, _: &Team) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test]
async fn api_parent_never_adopts_team_system_command_authority_from_its_fresh_source() {
    let root = tempfile::tempdir().unwrap();
    let mut config = common::config(root.path());
    config.actions_non_admin = true;
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let mut team = Team::new("Harbor");
    team.attributes = map(json!({"isolated":true,"plugins_write":true,
        "execution":{"mode":"api","connector":"api:owned"}}));
    assert!(instance.register_team(&mut team).await.success);
    let id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
    let state = meerschaum_server::AppState::builder(config, instance)
        .actions(Arc::new(ReadyTeam))
        .secret(common::SECRET.to_vec())
        .build()
        .unwrap();
    callbacks::set_generation(root.path(), id, Some(Uuid::new_v4())).unwrap();
    let app = meerschaum_server::app(state.clone());
    let bearer = common::login(
        &app,
        "admin",
        "pass",
        Some("jobs:execute jobs:read jobs:write actions:execute pipes:read"),
    )
    .await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", bearer.parse().unwrap());
    let source = meerschaum_server::access::resolve(&state, &headers)
        .await
        .unwrap();
    assert!(!source.full_admin());
    let parent = callbacks::register(
        &state,
        &source,
        Uuid::new_v4(),
        Some(id),
        &["jobs:execute"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let parent = callbacks::load_identity(&state, parent.id, parent.launch_id)
        .await
        .unwrap();
    let bounded = parent.caller();
    assert!(meerschaum_server::team_execution::code_running_team(&bounded).is_none());
    assert!(bounded.has("jobs:read"));
    assert!(
        callbacks::register(
            &state,
            &bounded,
            Uuid::new_v4(),
            Some(id),
            &["jobs:read"],
            Purpose::Jobs,
        )
        .await
        .is_err(),
        "An API-only parent changed its callback and credential ceiling."
    );
    let child = callbacks::register(
        &state,
        &bounded,
        Uuid::new_v4(),
        Some(id),
        &["jobs:execute"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let result = meerschaum_server::executor::admission::job(
        &state,
        child.id,
        child.launch_id,
        id,
        &["sh".into(), "printf synthetic".into()],
    )
    .await;
    assert!(
        result.is_err(),
        "An API-only parent adopted Team-only system command authorization"
    );
}
