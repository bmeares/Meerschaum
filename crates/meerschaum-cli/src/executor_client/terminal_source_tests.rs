//! Real registered-source and immutable archive checks for Main's terminal catalog.

use super::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::TeamRole;
use meerschaum_core::{Map, Value, config::ConfigOptions, users::User};

fn archive(directory: &std::path::Path, name: &str, source: &str) {
    std::fs::create_dir_all(directory).unwrap();
    let file = std::fs::File::create(directory.join(format!("{name}.tar.gz"))).unwrap();
    let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
        file,
        flate2::Compression::fast(),
    ));
    let mut header = tar::Header::new_gnu();
    header.set_mode(0o440);
    header.set_size(source.len() as u64);
    header.set_cksum();
    builder
        .append_data(&mut header, format!("{name}.py"), source.as_bytes())
        .unwrap();
    builder.into_inner().unwrap().finish().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_terminal_keeps_actual_original_archive_ceiling_when_team_sources_grow_or_change() {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(
        meerschaum_connector_sql::SqlConnector::from_uri(
            "main",
            &format!("sqlite:///{}", root.path().join("terminal.db").display()),
        )
        .await
        .unwrap(),
    );
    let mut user = User::new("terminal_operator", Some("synthetic-terminal-source-pass"));
    user.attributes = Some(
        Value::from_json_str(r#"{"role":"operator","primary_team":"TerminalSources"}"#)
            .unwrap()
            .to_map(),
    );
    assert!(instance.register_user(&mut user).await.success);
    let owner_id = instance.get_user_id(&user).await.unwrap().unwrap();
    user.user_id = Some(owner_id.clone());
    let mut team = Team::new("TerminalSources");
    // The public non-code remote-terminal gate remains closed. Exercise the shared exact
    // source capture with an authorized Team terminal; signed API runtime tests cover B.
    team.attributes = Value::from_json_str(
        r#"{"isolated":true,"runs_code":true,"plugins_write":true,"plugins":["terminal_probe"],"execution":{"mode":"api","connector":"api:remote"}}"#,
    )
    .unwrap()
    .to_map();
    team.members.insert(user.username.clone(), TeamRole::Member);
    let registered = instance.register_team(&mut team).await;
    assert!(registered.success, "{registered}");
    let team_id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
    crate::executor_registry::Registry::new(root.path())
        .reconcile(&[team.clone()])
        .unwrap();
    let mut config = meerschaum_server::ServerConfig::for_root("sql:main", root.path());
    config.actions_non_admin = true;
    config.execution_callback_url = Some("https://main.example".into());
    let archives = config.plugins_archives_path.clone();
    archive(
        &archives,
        "terminal_probe",
        "raise AssertionError('Source selection must not import code')\n",
    );
    archive(
        &archives,
        "ungranted_probe",
        "raise AssertionError('Unselected source must remain inert')\n",
    );
    let state = meerschaum_server::AppState::builder(config, instance.clone())
        .secret(b"synthetic-terminal-source-secret-0123456789".to_vec())
        .build()
        .unwrap();
    let mut options = ConfigOptions::with_env(vec![(
        "MRSM_ROOT_DIR".into(),
        root.path().display().to_string(),
    )]);
    options.allow_writes = false;
    options.home = root.path().into();
    let controller = Controller::new(root.path().into(), instance.clone(), options, Map::new());
    controller.bind_state(&state).unwrap();
    let mut caller = meerschaum_server::access::access_of_fresh(
        &state,
        Principal::User(user.clone()),
        &Scopes::Str("*".into()),
    )
    .await
    .unwrap();
    caller.credential = auth::user_credential(&state, &user).await.unwrap();
    let selection = controller
        .capture_sources(team_id, crate::executor_jobs::JobPlacement::Remote, &caller)
        .await
        .unwrap();
    let ceiling = super::super::source_selection::ceiling(selection.as_ref()).unwrap();
    assert_eq!(ceiling.names(), BTreeSet::from(["terminal_probe".into()]));
    let grant = callbacks::register_with_sources(
        &state,
        &caller,
        Uuid::new_v4(),
        Some(team_id),
        &["webterm:use", "actions:execute", "jobs:execute"],
        callbacks::Purpose::Jobs,
        ceiling.clone(),
    )
    .await
    .unwrap();
    let record = Record {
        team_id,
        grant: TerminalGrant {
            id: grant.launch_id,
            owner: user.username.clone(),
            owner_id: owner_id.py_str(),
        },
        profile: environment::Profile::Team,
        credential: caller.credential.clone().unwrap(),
        permissions: caller.principal.permissions.clone(),
        callback: Some(grant.id),
        source_selection: selection,
        sources_captured: true,
    };
    controller.terminal_source_identity(&record).await.unwrap();
    team.attributes.insert(
        "plugins".into(),
        Value::List(vec!["terminal_probe".into(), "ungranted_probe".into()]),
    );
    assert!(instance.edit_team(&team).await.success);
    let original = controller.terminal_source_identity(&record).await.unwrap();
    assert_eq!(original.grant.source_ceiling.as_ref(), Some(&ceiling));
    assert!(!meerschaum_server::executor::source_ceiling::permits_name(
        &original.caller(),
        "ungranted_probe"
    ));
    let child = controller
        .capture_sources(
            team_id,
            crate::executor_jobs::JobPlacement::Remote,
            &original.caller(),
        )
        .await
        .unwrap();
    assert_eq!(
        super::super::source_selection::ceiling(child.as_ref()).unwrap(),
        ceiling
    );
    let authority = root
        .path()
        .join("executors/callbacks")
        .join(format!("{}.json", grant.id));
    let unchanged = std::fs::read(&authority).unwrap();
    archive(
        &archives,
        "terminal_probe",
        "raise AssertionError('Changed content needs a new terminal')\n",
    );
    assert!(controller.terminal_source_identity(&record).await.is_err());
    assert_eq!(
        std::fs::read(authority).unwrap(),
        unchanged,
        "Failed source admission minted or rotated authority."
    );
}
