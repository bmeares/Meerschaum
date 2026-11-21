//! Isolated API execution preserves declared scopes only on the trusted tracked bridge.
use crate::common;
use axum::http::HeaderMap;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::users::{Expiration, Token};
use meerschaum_core::{Map, SuccessTuple, Value, users::User};
use meerschaum_server::access::Access;
use meerschaum_server::backends::{ActionsBackend, ApiExecution};
use meerschaum_server::executor::callbacks;
use std::sync::Arc;
use uuid::Uuid;

struct Actions(ApiExecution);
#[async_trait::async_trait]
impl ActionsBackend for Actions {
    fn api_execution(&self) -> ApiExecution {
        self.0
    }
    fn list(&self) -> Vec<String> {
        vec!["show".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> SuccessTuple {
        panic!("Capability checks may never execute code")
    }
}
fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
async fn setup(
    root: &std::path::Path,
    capability: ApiExecution,
) -> (
    meerschaum_server::AppState,
    Arc<common::MemoryInstance>,
    Uuid,
) {
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(
            serde_json::json!({"role":"operator","primary_team":"ApiTeam",
        "api_execution":{"tracked":{"terminals":true}}}),
        ),
    );
    let mut team = Team::new("ApiTeam");
    team.attributes = map(serde_json::json!({"isolated":true,"plugins_write":false}));
    team.members.insert("alice".into(), TeamRole::Member);
    let registered = instance.register_team(&mut team).await;
    assert!(registered.success, "{registered}");
    let id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
    let mut config = common::config(root);
    config.actions_non_admin = true;
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .secret(common::SECRET.to_vec())
        .actions(Arc::new(Actions(capability)))
        .build()
        .unwrap();
    (state, instance, id)
}
async fn resolve(state: &meerschaum_server::AppState, bearer: &str) -> Access {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", bearer.parse().unwrap());
    meerschaum_server::access::require_fresh(state, &headers, &[])
        .await
        .unwrap()
}

#[tokio::test]
async fn a_tracked_backend_preserves_only_declared_actions_and_keeps_terminal_and_code_fences() {
    for (capability, allowed) in [
        (ApiExecution::Unavailable, false),
        (ApiExecution::Tracked { terminals: false }, true),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (state, _, id) = setup(root.path(), capability).await;
        let app = meerschaum_server::app(state.clone());
        let bearer = common::login(&app, "alice", "pass", None).await;
        let source = resolve(&state, &bearer).await;
        assert_eq!(source.has("actions:execute"), allowed);
        assert!(!source.has("webterm:use"));
        assert!(!source.has("instance:chain"));
        assert!(!source.principal.grants.runs_code);
        assert!(source.principal.grants.isolated);
        let grant = callbacks::register(
            &state,
            &source,
            Uuid::new_v4(),
            Some(id),
            &["actions:execute"],
            callbacks::Purpose::Command,
        )
        .await;
        if allowed {
            let grant = grant.unwrap();
            let bounded = callbacks::load_identity(&state, grant.id, grant.launch_id)
                .await
                .unwrap()
                .caller();
            assert!(bounded.has("actions:execute"));
            assert!(
                !bounded.has("webterm:use")
                    && !bounded.has("instance:chain")
                    && !bounded.principal.grants.runs_code
            );
        } else {
            assert!(
                grant.is_err(),
                "User attributes must not assert backend capability"
            );
        }
    }
}

#[tokio::test]
async fn tracked_api_never_adds_actions_to_narrow_jwts_tokens_or_callback_parents() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance, id) =
        setup(root.path(), ApiExecution::Tracked { terminals: false }).await;
    let app = meerschaum_server::app(state.clone());
    let narrowed = common::login(&app, "alice", "pass", Some("pipes:read")).await;
    let source = resolve(&state, &narrowed).await;
    assert!(
        source.has("pipes:read") && !source.has("actions:execute") && !source.has("webterm:use")
    );
    let mut token = Token::new(
        Some("bounded-source"),
        Expiration::Never,
        Some(vec!["pipes:read".into()]),
        366,
        chrono::Utc::now(),
    );
    token.user = Some(User::new("alice", None));
    assert!(instance.register_token(&mut token).await.success);
    let bounded = resolve(&state, &format!("Bearer {}", token.get_api_key())).await;
    assert!(bounded.has("pipes:read") && !bounded.has("actions:execute"));
    let bearer = common::login(&app, "alice", "pass", None).await;
    let mut source = resolve(&state, &bearer).await;
    assert!(source.has("actions:execute"));
    source.principal.permissions.remove("actions:execute");
    source.principal.permissions.remove("webterm:use");
    let parent = callbacks::register(
        &state,
        &source,
        Uuid::new_v4(),
        Some(id),
        &["jobs:read"],
        callbacks::Purpose::Command,
    )
    .await
    .unwrap();
    let identity = callbacks::load_identity(&state, parent.id, parent.launch_id)
        .await
        .unwrap();
    assert!(identity.source_access().has("actions:execute"));
    let child = identity.caller();
    assert!(!child.has("actions:execute") && !child.has("webterm:use"));
    assert!(
        callbacks::register(
            &state,
            &child,
            Uuid::new_v4(),
            Some(id),
            &["actions:execute"],
            callbacks::Purpose::Command
        )
        .await
        .is_err()
    );
    instance.seed_user_scopes("alice", &["pipes:read"]);
    let current = resolve(&state, &bearer).await;
    assert!(
        !current.has("actions:execute"),
        "A cached old JWT restored removed source scopes"
    );
}

#[tokio::test]
async fn an_open_host_team_boundary_keeps_only_its_actually_captured_plugin_names() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance, id) = setup(root.path(), ApiExecution::Unavailable).await;
    let mut team = instance.get_team("ApiTeam").await.unwrap().unwrap();
    team.attributes.insert("isolated".into(), false.into());
    assert!(instance.edit_team(&team).await.success);
    let app = meerschaum_server::app(state.clone());
    let bearer = common::login(&app, "alice", "pass", None).await;
    let source = resolve(&state, &bearer).await;
    assert!(!source.principal.is_isolated());
    let ceiling = meerschaum_server::executor::source_ceiling::Ceiling::from_files(
        &std::collections::BTreeMap::from([("a.py".into(), "a".repeat(64))]),
    )
    .unwrap();
    let grant = callbacks::register_with_sources(
        &state,
        &source,
        Uuid::new_v4(),
        Some(id),
        &["jobs:read"],
        callbacks::Purpose::Command,
        ceiling,
    )
    .await
    .unwrap();
    let bounded = callbacks::load_identity(&state, grant.id, grant.launch_id)
        .await
        .unwrap()
        .caller();
    assert!(bounded.principal.is_isolated());
    assert!(meerschaum_core::access::can_use_plugin(
        &bounded.principal,
        "a"
    ));
    assert!(!meerschaum_core::access::can_use_plugin(
        &bounded.principal,
        "b"
    ));
    assert!(!meerschaum_server::executor::source_ceiling::permits_name(
        &bounded, "b"
    ));
}
