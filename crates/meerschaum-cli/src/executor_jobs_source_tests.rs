//! Source ceilings survive fresh role changes; paused starts never rotate canonical authority.

use super::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Principal, Role, Team, TeamGrants, TeamRole};
use meerschaum_core::users::User;
use meerschaum_server::access::Access;
use meerschaum_server::executor::{callbacks, context, environment::Profile};

fn map(value: Json) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

fn source(user: &User, team: &Team, credential: Option<String>) -> Access {
    let mut caller = Access::everything();
    caller.no_auth = false;
    caller.auth = Some(meerschaum_server::auth::Principal::User(user.clone()));
    caller.credential = credential;
    caller.primary_team = team.name.clone();
    caller.team_records = vec![team.clone()];
    caller.principal = Principal::new(
        &user.username,
        false,
        Role::Operator.permissions(),
        vec![(team.name.clone(), TeamRole::Member)],
    )
    .with_grants(TeamGrants::of(
        &[(team.name.clone(), TeamRole::Member)],
        std::slice::from_ref(team),
        Some(&team.name),
    ));
    caller
}

#[test]
fn typed_pipe_endpoint_selects_exact_null_location_and_refuses_filter_or_flag_names() {
    use meerschaum_server::executor::pipe_operation::PipeOperation;
    let mut operation = PipeOperation {
        row_params: None,
        pipe_ref: "sql:main|plugin:weather|api|None".into(),
        registration: Value::Int(17),
        verify: false,
        begin: Some("-100".into()),
        end: Some("0".into()),
        parents: None,
        children: None,
        metadata: None,
    };
    let argv = operation.sysargs().unwrap();
    let parsed =
        crate::parse::parse_arguments(&argv, &crate::types::DateContext::system()).unwrap();
    let selected = crate::native::pipes::Selection::from_args(&parsed);
    assert_eq!(selected.connector_keys, ["plugin:weather"]);
    assert_eq!(
        selected.metric_keys,
        ["api"],
        "Literal api was removed as a legacy dispatch prefix"
    );
    assert_eq!(
        selected.location_keys,
        [None],
        "A null location widened into all locations"
    );
    assert_eq!(
        crate::native::string(&parsed, "mrsm_instance").as_deref(),
        Some("sql:main")
    );
    assert_eq!(crate::native::words(&parsed, "action"), ["sync", "pipes"]);
    for reference in [
        "sql:main|plugin:weather|--shell|None",
        "sql:main|plugin:weather|*|None",
        "sql:main|plugin:weather|_private|None",
        "sql:main|plugin:weather|[extra]|None",
        "sql:main|plugin:weather|normal|--instance",
        "sql:main|plugin:weather|normal|bad\nlocation",
    ] {
        operation.pipe_ref = reference.into();
        assert!(
            operation.sysargs().is_err(),
            "Ambiguous endpoint selector was accepted: {reference:?}"
        );
    }
}

#[tokio::test]
async fn api_parent_stays_api_after_fresh_code_grant_and_jobs_parent_cannot_gain_data_after_revocation()
 {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(
        meerschaum_connector_sql::SqlConnector::from_uri(
            "main",
            &format!("sqlite:///{}", root.path().join("source.db").display()),
        )
        .await
        .unwrap(),
    );
    let mut user = User::new("alice", Some("synthetic-profile-original-pass"));
    user.user_type = Some("regular".into());
    user.attributes = Some(map(json!({"role":"operator", "primary_team":"Wetlands"})));
    assert!(instance.register_user(&mut user).await.success);
    user.user_id = instance.get_user_id(&user).await.unwrap();
    let mut team = Team::new("Wetlands");
    team.members.insert("alice".into(), TeamRole::Member);
    team.attributes = map(json!({"isolated":true,"plugins_write":false}));
    assert!(instance.register_team(&mut team).await.success);
    let id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
    let config = meerschaum_server::ServerConfig::for_root("sql:main", root.path());
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .secret(b"synthetic-profile-source-secret-01234567890123456789".to_vec())
        .build()
        .unwrap();
    let credential = meerschaum_server::auth::user_credential(&state, &user)
        .await
        .unwrap();
    let caller = source(&user, &team, credential.clone());
    assert_eq!(source_profile(Some(&caller), id).unwrap(), Profile::Api);
    let command = callbacks::register(
        &state,
        &caller,
        Uuid::new_v4(),
        Some(id),
        &["jobs:execute"],
        callbacks::Purpose::Command,
    )
    .await
    .unwrap();
    team.attributes.insert("plugins_write".into(), true.into());
    assert!(instance.edit_team(&team).await.success);
    let current = callbacks::load_identity(&state, command.id, command.launch_id)
        .await
        .unwrap();
    let api_child = current.caller();
    assert!(meerschaum_server::team_execution::code_running_team(&api_child).is_none());
    assert!(api_parent(&api_child));
    assert_eq!(source_profile(Some(&api_child), id).unwrap(), Profile::Api);
    let fresh = source(&user, &team, credential);
    assert_eq!(source_profile(Some(&fresh), id).unwrap(), Profile::Team);
    assert_eq!(
        source_profile(Some(&fresh), Uuid::new_v4()).unwrap(),
        Profile::Api
    );
    let jobs = callbacks::register(
        &state,
        &fresh,
        Uuid::new_v4(),
        Some(id),
        &["jobs:execute"],
        callbacks::Purpose::Jobs,
    )
    .await
    .unwrap();
    team.attributes.insert("plugins_write".into(), false.into());
    assert!(instance.edit_team(&team).await.success);
    let revoked = callbacks::load_identity(&state, jobs.id, jobs.launch_id)
        .await
        .unwrap();
    assert!(
        source_profile(Some(&revoked.caller()), id).is_err(),
        "A jobs-only parent cannot create an API-data ceiling after losing its code grant"
    );
}

struct PausedFactory;
#[async_trait::async_trait]
impl meerschaum_server::executor::gateway::RuntimeFactory for PausedFactory {
    async fn build(
        &self,
        _: meerschaum_server::executor::gateway::Configuration,
    ) -> Result<axum::Router, meerschaum_server::executor::gateway::RuntimeError> {
        Ok(axum::Router::new()
            .route(
                "/jobs/{name}/exists",
                axum::routing::get(|| async { axum::Json(true) }),
            )
            .route(
                "/jobs/{name}",
                axum::routing::get(|| async {
                    axum::Json(
                        json!({"status":"paused", "daemon":{"status":"paused","properties":{}}}),
                    )
                }),
            ))
    }
    async fn revoke(&self) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_paused_start_preserves_revision_and_original_callback_before_admission() {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(
        meerschaum_connector_sql::SqlConnector::from_uri(
            "main",
            &format!("sqlite:///{}", root.path().join("paused.db").display()),
        )
        .await
        .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut team = Team::new("Wetlands");
    team.attributes = map(
        json!({"isolated":true, "instance":"sql:main", "connectors":["sql:main"],
        "execution":{"mode":"api","connector":"api:paused"}}),
    );
    assert!(instance.register_team(&mut team).await.success);
    let id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
    let registry = crate::executor_registry::Registry::new(root.path());
    registry.reconcile(std::slice::from_ref(&team)).unwrap();
    let (_, authority) = registry.authority(id).unwrap();
    let gateway = meerschaum_server::executor::gateway::Gateway::new(
        authority.verifier(),
        Arc::new(PausedFactory),
    )
    .unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, gateway.router()).await.unwrap();
    });
    let mut options = meerschaum_core::config::ConfigOptions::with_env(vec![
        ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
        (
            "MRSM_PLUGINS_DIR".into(),
            root.path().join("plugins").display().to_string(),
        ),
    ]);
    options.allow_writes = false;
    options.home = root.path().into();
    let snapshot = map(json!({"meerschaum":{"instance":"sql:main", "connectors":{
        "sql":{"main":{"flavor":"sqlite","database":root.path().join("paused.db")}},
        "api":{"paused":{"host":"127.0.0.1","port":port}}
    }}}));
    let controller = Arc::new(crate::executor_client::Controller::new(
        root.path().into(),
        instance.clone(),
        options,
        snapshot,
    ));
    let mut config = meerschaum_server::ServerConfig::for_root("sql:main", root.path());
    config.no_auth = true;
    let state = meerschaum_server::AppState::builder(config, instance)
        .secret(b"synthetic-paused-state-secret-01234567890123456789".to_vec())
        .build()
        .unwrap();
    controller.bind_state(&state).unwrap();
    let mut properties = Map::new();
    meerschaum_core::job_access::JobAccess::stamp(&mut properties, Some("alice"), Some(&team.name));
    let mut record = Record::new(
        "paused-proof",
        id,
        vec!["show".into(), "version".into()],
        properties,
    );
    // An unusable original grant makes the ordering observable: admission first would fail
    // with a source error rather than the paused state, and must never replace this proof.
    record.callback = Some(Uuid::new_v4());
    let catalog = Catalog::new(root.path());
    assert!(catalog.reserve(record.clone()).unwrap());
    let jobs = DelegatingJobs::new(
        root.path(),
        Arc::new(meerschaum_server::backends::NoJobs),
        controller,
    );
    let result = context::scope(None, jobs.start(&record.name)).await;
    assert!(!result.success);
    assert!(result.message.contains("paused"), "{}", result.message);
    assert!(result.message.contains("Stop it"), "{}", result.message);
    let current = catalog.get(&record.name).unwrap().unwrap();
    assert_eq!(current.context_revision, record.context_revision);
    assert_eq!(current.callback, record.callback);
    assert_eq!(current.callback_suspended, record.callback_suspended);
    server.abort();
    let _ = server.await;
}
