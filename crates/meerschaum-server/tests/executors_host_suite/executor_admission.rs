//! Admission rechecks original registrations and immutable teams before any restart delivery.
use crate::common;
use std::sync::Arc;

use axum::http::HeaderMap;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::admission;
use meerschaum_server::executor::callbacks::{self, Purpose};
use meerschaum_server::executor::environment::Profile;
use uuid::Uuid;

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
fn words(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

#[tokio::test]
async fn host_api_admission_supports_everyone_but_never_adopts_code_or_jobs_only_authority() {
    let root = tempfile::tempdir().unwrap();
    let mut config = common::config(root.path());
    config.actions_non_admin = true;
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let state = meerschaum_server::AppState::builder(config, instance)
        .secret(common::SECRET.to_vec())
        .actions(Arc::new(Actions))
        .build()
        .unwrap();
    let admin = caller(&state, "admin").await;
    let grant = callbacks::register(
        &state,
        &admin,
        Uuid::new_v4(),
        None,
        &["actions:execute"],
        Purpose::Command,
    )
    .await
    .unwrap();
    callbacks::suspend(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let admitted = admission::host_job(
        &state,
        grant.id,
        grant.launch_id,
        None,
        &words(&["show", "version"]),
    )
    .await
    .unwrap();
    assert_eq!(admitted.profile, Profile::Api);
    assert!(admitted.identity.source_access().full_admin());
    assert!(!admitted.identity.caller().is_admin());
    assert!(
        callbacks::credential(&state, grant.id, grant.launch_id)
            .await
            .is_err()
    );
    assert!(
        admission::host_job(
            &state,
            grant.id,
            grant.launch_id,
            None,
            &words(&["sh", "printf synthetic"]),
        )
        .await
        .is_err(),
        "An API-only host launch gained its original source's code authority."
    );
    assert!(
        admission::host_job(
            &state,
            grant.id,
            grant.launch_id,
            Some(Uuid::new_v4()),
            &words(&["show", "version"]),
        )
        .await
        .is_err()
    );
    let jobs = callbacks::register(
        &state,
        &admin,
        Uuid::new_v4(),
        None,
        &["actions:execute"],
        Purpose::Jobs,
    )
    .await
    .unwrap();
    assert!(
        admission::host_job(
            &state,
            jobs.id,
            jobs.launch_id,
            None,
            &words(&["show", "version"]),
        )
        .await
        .is_err(),
        "Jobs-only authority was upgraded to API data callbacks."
    );
}
fn id(team: &Team) -> Uuid {
    Uuid::parse_str(team.id.as_deref().unwrap()).unwrap()
}

struct Actions;
#[async_trait::async_trait]
impl meerschaum_server::backends::ActionsBackend for Actions {
    fn list(&self) -> Vec<String> {
        vec!["show".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> meerschaum_core::SuccessTuple {
        meerschaum_core::SuccessTuple::fail("Admission fixture does not run commands.")
    }
}

async fn caller(
    state: &meerschaum_server::AppState,
    username: &str,
) -> meerschaum_server::access::Access {
    let app = meerschaum_server::app(state.clone());
    let bearer = common::login(&app, username, "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", bearer.parse().unwrap());
    meerschaum_server::access::resolve(state, &headers)
        .await
        .unwrap()
}

#[tokio::test]
async fn direct_original_admin_keeps_team_profile_without_code_grant_and_suspension_does_not_authenticate()
 {
    let root = tempfile::tempdir().unwrap();
    let mut config = common::config(root.path());
    config.actions_non_admin = true;
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .secret(common::SECRET.to_vec())
        .actions(Arc::new(Actions))
        .build()
        .unwrap();
    let mut team = Team::new("Harbor");
    team.attributes = map(serde_json::json!({"isolated":true,"plugins_write":false}));
    assert!(instance.register_team(&mut team).await.success);
    let admin = caller(&state, "admin").await;
    let grant = callbacks::register(
        &state,
        &admin,
        Uuid::new_v4(),
        Some(id(&team)),
        &["actions:execute"],
        Purpose::Jobs,
    )
    .await
    .unwrap();
    callbacks::suspend(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let admitted = admission::job(
        &state,
        grant.id,
        grant.launch_id,
        id(&team),
        &words(&["sh", "printf synthetic"]),
    )
    .await
    .unwrap();
    assert_eq!(admitted.profile, Profile::Team);
    assert!(admitted.identity.source_access().full_admin());
    assert!(!admitted.identity.caller().is_admin());
    assert_eq!(
        admitted
            .identity
            .caller()
            .principal
            .team_boundary
            .as_deref(),
        Some("Harbor")
    );
    assert!(
        callbacks::credential(&state, grant.id, grant.launch_id)
            .await
            .is_err()
    );
    assert!(
        callbacks::renew(&state, grant.id, grant.launch_id)
            .await
            .is_err()
    );
    assert!(
        admission::job(
            &state,
            grant.id,
            grant.launch_id,
            Uuid::new_v4(),
            &words(&["show", "version"])
        )
        .await
        .is_err()
    );
    let command = callbacks::register(
        &state,
        &admin,
        Uuid::new_v4(),
        Some(id(&team)),
        &["actions:execute"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let admitted = admission::job(
        &state,
        command.id,
        command.launch_id,
        id(&team),
        &words(&["show", "version"]),
    )
    .await
    .unwrap();
    assert_eq!(admitted.profile, Profile::Api);
    assert!(
        admission::job(
            &state,
            command.id,
            command.launch_id,
            id(&team),
            &words(&["sh", "printf synthetic"]),
        )
        .await
        .is_err(),
        "A Command grant adopted its original administrator's code authority."
    );
    assert!(instance.rename_team("Harbor", "Quay").await.success);
    let admitted = admission::job(
        &state,
        grant.id,
        grant.launch_id,
        id(&team),
        &words(&["show", "version"]),
    )
    .await
    .unwrap();
    assert_eq!(admitted.identity.caller().primary_team, "Quay");
    assert!(
        admission::job(
            &state,
            grant.id,
            grant.launch_id,
            id(&team),
            &words(&["show", "version", "__mrsm_job_supervisor__"])
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn admin_origin_child_never_recovers_global_admin_or_upgrades_jobs_purpose() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    let mut team = Team::new("Harbor");
    team.attributes = map(serde_json::json!({"isolated":true,"plugins_write":false}));
    assert!(instance.register_team(&mut team).await.success);
    let admin = caller(&state, "admin").await;
    let parent = callbacks::register(
        &state,
        &admin,
        Uuid::new_v4(),
        Some(id(&team)),
        &["actions:execute"],
        Purpose::Jobs,
    )
    .await
    .unwrap();
    let parent_identity = callbacks::load_identity(&state, parent.id, parent.launch_id)
        .await
        .unwrap();
    assert!(parent_identity.caller().has("jobs:read"));
    assert!(
        callbacks::register(
            &state,
            &parent_identity.caller(),
            Uuid::new_v4(),
            Some(id(&team)),
            &["jobs:read"],
            Purpose::Command,
        )
        .await
        .is_err(),
        "A Jobs parent changed its callback and credential ceiling."
    );
    let child = callbacks::register(
        &state,
        &parent_identity.caller(),
        Uuid::new_v4(),
        Some(id(&team)),
        &["jobs:read"],
        Purpose::Jobs,
    )
    .await
    .unwrap();
    let child_identity = callbacks::load_identity(&state, child.id, child.launch_id)
        .await
        .unwrap();
    assert!(child_identity.source_access().full_admin());
    assert_eq!(child_identity.grant.depth, 1);
    let error = admission::job(
        &state,
        child.id,
        child.launch_id,
        id(&team),
        &words(&["show", "version"]),
    )
    .await
    .err()
    .unwrap();
    assert!(error.detail.unwrap().as_str().unwrap().contains("Recreate"));
    // The same broad administrator making an unrelated request is never an input to admission.
    assert!(
        meerschaum_server::executor::context::scope(
            Some(Arc::new(admin)),
            admission::job(
                &state,
                child.id,
                child.launch_id,
                id(&team),
                &words(&["sh", "printf synthetic"]),
            )
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn code_revocation_requires_new_api_authority_and_team_name_reuse_cannot_adopt_a_job() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(serde_json::json!({"role":"operator", "primary_team":"Harbor"})),
    );
    let mut team = Team::new("Harbor");
    team.attributes = map(serde_json::json!({"isolated":true,"plugins_write":true}));
    team.members.insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut team).await.success);
    let alice = caller(&state, "alice").await;
    let grant = callbacks::register(
        &state,
        &alice,
        Uuid::new_v4(),
        Some(id(&team)),
        &["jobs:read"],
        Purpose::Jobs,
    )
    .await
    .unwrap();
    let initial = callbacks::load_admission_identity(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    assert_eq!(
        admission::profile(&initial, id(&team)).unwrap(),
        Profile::Team
    );
    team.attributes.insert("plugins_write".into(), false.into());
    assert!(instance.edit_team(&team).await.success);
    let error = admission::job(
        &state,
        grant.id,
        grant.launch_id,
        id(&team),
        &words(&["show", "version"]),
    )
    .await
    .err()
    .unwrap();
    assert!(
        error
            .detail
            .unwrap()
            .as_str()
            .unwrap()
            .contains("jobs-only")
    );
    assert!(instance.delete_team(&team.name).await.success);
    let mut replacement = Team::new("Harbor");
    replacement.attributes = team.attributes.clone();
    replacement.members.insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut replacement).await.success);
    assert_ne!(replacement.id, team.id);
    assert!(
        admission::job(
            &state,
            grant.id,
            grant.launch_id,
            id(&replacement),
            &words(&["show", "version"])
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn api_analysis_uses_current_original_command_rights_without_enabling_delivery() {
    let root = tempfile::tempdir().unwrap();
    let mut config = common::config(root.path());
    config.actions_non_admin = true;
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(serde_json::json!({"role":"operator", "primary_team":"Harbor"})),
    );
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .secret(common::SECRET.to_vec())
        .actions(Arc::new(Actions))
        .build()
        .unwrap();
    let mut team = Team::new("Harbor");
    team.members.insert("alice".into(), TeamRole::Member);
    assert!(instance.register_team(&mut team).await.success);
    let alice = caller(&state, "alice").await;
    let grant = callbacks::register(
        &state,
        &alice,
        Uuid::new_v4(),
        Some(id(&team)),
        &["actions:execute"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let admitted = admission::job(
        &state,
        grant.id,
        grant.launch_id,
        id(&team),
        &words(&["show", "version"]),
    )
    .await
    .unwrap();
    assert_eq!(admitted.profile, Profile::Api);
    assert!(!admitted.identity.caller().is_admin());
    assert!(
        admission::job(
            &state,
            grant.id,
            grant.launch_id,
            id(&team),
            &words(&["sh", "printf synthetic"])
        )
        .await
        .is_err()
    );
    // Changing the original source's current privileges cannot change a launch's profile.
    team.attributes = map(serde_json::json!({"isolated":true,"plugins_write":true}));
    assert!(instance.edit_team(&team).await.success);
    let admitted = admission::job(
        &state,
        grant.id,
        grant.launch_id,
        id(&team),
        &words(&["show", "version"]),
    )
    .await
    .unwrap();
    assert_eq!(
        admitted.profile,
        Profile::Api,
        "New code privileges upgraded the launch."
    );
    assert!(
        admission::job(
            &state,
            grant.id,
            grant.launch_id,
            id(&team),
            &words(&["sh", "printf synthetic"])
        )
        .await
        .is_err(),
        "New code privileges bypassed the API-only command policy."
    );
    instance.seed_user_scopes("alice", &["jobs:read"]);
    assert!(
        admission::job(
            &state,
            grant.id,
            grant.launch_id,
            id(&team),
            &words(&["show", "version"])
        )
        .await
        .is_err()
    );
    instance.seed_user_scopes("alice", &["*"]);
    assert!(
        instance
            .edit_user(&User::new("alice", Some("new-pass")))
            .await
            .success
    );
    assert!(
        admission::job(
            &state,
            grant.id,
            grant.launch_id,
            id(&team),
            &words(&["show", "version"])
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn direct_team_pipe_command_keeps_original_scopes_but_jobs_callbacks_never_gain_them() {
    let root = tempfile::tempdir().unwrap();
    let mut config = common::config(root.path());
    config.actions_non_admin = true;
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(serde_json::json!({"role":"operator", "primary_team":"Harbor"})),
    );
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .secret(common::SECRET.to_vec())
        .actions(Arc::new(Actions))
        .build()
        .unwrap();
    let mut team = Team::new("Harbor");
    team.attributes = map(serde_json::json!({"isolated":true,"plugins_write":true}));
    team.members.insert("alice".into(), TeamRole::Member);
    let registered = instance.register_team(&mut team).await;
    assert!(registered.success, "{}", registered.message);
    let pipe = meerschaum_core::Pipe::builder("manual:fixture", "owned_pipe")
        .instance("sql:fixture")
        .parameters(map(
            serde_json::json!({"access":{"owner":"alice","team":"Harbor","visibility":"private"}}),
        ))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let alice = caller(&state, "alice").await;
    let grant = callbacks::register(
        &state,
        &alice,
        Uuid::new_v4(),
        Some(id(&team)),
        &["actions:execute"],
        Purpose::Jobs,
    )
    .await
    .unwrap();
    let command = words(&["show", "pipes", "-c", "manual:fixture", "-m", "owned_pipe"]);
    let admitted = admission::job(&state, grant.id, grant.launch_id, id(&team), &command)
        .await
        .unwrap();
    assert_eq!(admitted.profile, Profile::Team);
    assert!(!admitted.identity.caller().has("pipes:read"));
    assert!(!admitted.identity.grant.permissions.contains("pipes:read"));
    instance.seed_user_scopes("alice", &["actions:execute", "jobs:read"]);
    assert!(
        admission::job(&state, grant.id, grant.launch_id, id(&team), &command)
            .await
            .is_err()
    );
}
