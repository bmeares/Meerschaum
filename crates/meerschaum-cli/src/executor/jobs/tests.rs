//! Canonical catalog records, placement, callback reads and log sanitizing.

use super::*;

#[tokio::test]
async fn absent_or_noauth_task_context_never_adopts_an_authenticated_server_environment() {
    for no_auth in [None, Some(false), Some(true)] {
        let root = tempfile::tempdir().unwrap();
        let instance = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", root.path().join("model.db").display()),
            )
            .await
            .unwrap(),
        );
        let options = meerschaum_core::config::ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
            (
                "MRSM_PLUGINS_DIR".into(),
                root.path().join("plugins").display().to_string(),
            ),
        ]);
        let controller = Arc::new(crate::executor::client::Controller::new(
            root.path().into(),
            instance.clone(),
            options,
            Map::new(),
        ));
        let state = no_auth.map(|no_auth| {
            let mut config = meerschaum_server::ServerConfig::for_root("sql:main", root.path());
            config.no_auth = no_auth;
            let state = meerschaum_server::AppState::builder(config, instance)
                .secret(b"synthetic-host-guard-secret-01234567890123456789".to_vec())
                .build()
                .unwrap();
            controller.bind_state(&state).unwrap();
            state
        });
        let jobs = DelegatingJobs::new(
            root.path(),
            Arc::new(meerschaum_server::backends::NoJobs),
            controller,
        );
        for caller in [
            None,
            Some(Arc::new(meerschaum_server::access::Access::everything())),
        ] {
            let result = meerschaum_server::executor::context::scope(
                caller,
                jobs.create("host-guard", vec!["show".into(), "version".into()], None),
            )
            .await;
            if no_auth == Some(true) {
                assert!(
                    matches!(result, CreateJob::Started(_)),
                    "Explicit NoAuth keeps its legacy backend"
                );
            } else {
                assert!(
                    matches!(result, CreateJob::Unavailable(_)),
                    "A missing accepted caller cannot fall back locally"
                );
                assert!(!root.path().join("jobs/host-guard").exists());
            }
        }
        drop(state);
    }
}

fn record() -> Record {
    let mut properties = Map::new();
    meerschaum_core::job_access::JobAccess::stamp(
        &mut properties,
        Some("operator"),
        Some("Wetlands"),
    );
    properties.insert(
        "mrsm_execution".into(),
        Value::Map(Map::from_iter([(
            "kind".into(),
            "temporary_execution".into(),
        )])),
    );
    Record::new(
        "execute-owned",
        Uuid::new_v4(),
        vec!["show".into(), "version".into()],
        properties,
    )
}

#[test]
fn explicit_host_placement_is_required_for_everyone_and_keeps_original_history_markers() {
    let mut everyone = record();
    everyone.team_id = Uuid::nil();
    meerschaum_core::job_access::JobAccess::stamp(&mut everyone.properties, Some("operator"), None);
    assert!(
        !everyone.valid_placement(),
        "nil cannot select a remote executor"
    );
    everyone.placement = JobPlacement::HostApi;
    everyone.profile = meerschaum_server::executor::environment::Profile::Api;
    assert!(
        !everyone.valid_placement(),
        "host placement requires canonical source authority"
    );
    everyone.callback = Some(Uuid::new_v4());
    everyone.owner_binding = Some(meerschaum_server::executor::callbacks::OwnerBinding::User {
        username: "operator".into(),
        user_id: 42.into(),
    });
    assert!(everyone.valid_placement());
    let properties = delegated_properties(&everyone);
    assert!(properties[meerschaum_core::job_access::TEAM_ID_KEY].is_null());
    assert!(properties.contains_key(meerschaum_core::job_access::OWNER_BINDING_KEY));
    everyone.profile = meerschaum_server::executor::environment::Profile::Team;
    assert!(
        !everyone.valid_placement(),
        "host transport never upgrades to Team"
    );
    let mut old = serde_json::to_value(record()).unwrap();
    old.as_object_mut().unwrap().remove("placement");
    let old: Record = serde_json::from_value(old).unwrap();
    assert_eq!(old.placement, JobPlacement::Remote);
}

#[test]
fn hostile_metadata_cannot_choose_ownership_paths_commands_or_executor_identity() {
    let record = record();
    let hostile = json!({"sysargs": ["sh", "-c", "malicious"], "status": "running", "restart": true,
            "daemon": {"pid": 1, "properties": {"owner": "admin", "team": "Another", "log_path": "/main/private", "mrsm_execution": {"kind": "persistent_job"}}},
            "executor": {"team_id": Uuid::new_v4()}, "result": [false, "failure\u{1b}[31m"], "health": {"state": "made-up", "message": "hello\u{1b}", "consecutive_failures": -2}});
    let metadata = record.metadata(Some(&hostile));
    assert_eq!(metadata["sysargs"], json!(["show", "version"]));
    assert_eq!(metadata["daemon"]["properties"]["owner"], "operator");
    assert_eq!(metadata["daemon"]["properties"]["team"], "Wetlands");
    assert_eq!(
        metadata["daemon"]["properties"]["mrsm_execution"]["kind"],
        "temporary_execution"
    );
    assert!(metadata["daemon"]["properties"].get("log_path").is_none());
    assert_eq!(metadata["daemon"]["pid"], Json::Null);
    assert_eq!(metadata["executor"]["team_id"], record.team_id.to_string());
    assert_eq!(metadata["restart"], false);
    assert!(!metadata.to_string().contains("\\u001b"));
}

#[test]
fn unavailable_metadata_is_explicit_and_corruption_cannot_be_an_empty_catalog() {
    let root = tempfile::tempdir().unwrap();
    let catalog = Catalog::new(root.path());
    let record = record();
    assert!(catalog.reserve(record.clone()).unwrap());
    assert!(
        !catalog
            .reserve(Record::new(
                &record.name,
                Uuid::new_v4(),
                vec![],
                Map::new()
            ))
            .unwrap()
    );
    assert_eq!(record.metadata(None)["status"], "unavailable");
    let persisted = catalog.get(&record.name).unwrap().unwrap();
    assert_eq!(persisted.remote_name, record.remote_name);
    std::fs::remove_file(catalog.directory.join("catalog.json")).unwrap();
    assert!(catalog.records().is_err());
}

#[test]
fn concurrent_callback_readers_see_initialization_and_deletion() {
    let root = tempfile::tempdir().unwrap();
    let catalog = Catalog::new(root.path());
    assert!(catalog.records_if_available().unwrap().is_empty());
    let job = record();
    assert!(catalog.reserve(job.clone()).unwrap());
    let first_reader = catalog.lock_file().unwrap();
    fs4::FileExt::lock_shared(&first_reader).unwrap();
    let records = catalog.records_if_available().unwrap();
    assert_eq!(records[&job.name].remote_name, job.remote_name);
    drop(first_reader);
    assert!(catalog.remove(&job).unwrap());
    assert!(catalog.records_if_available().unwrap().is_empty());
    std::fs::remove_file(catalog.directory.join("catalog.json")).unwrap();
    assert!(catalog.records_if_available().is_err());
}

#[test]
fn callback_read_waits_for_a_short_catalog_write() {
    use std::sync::mpsc;
    use std::time::Duration;

    let root = tempfile::tempdir().unwrap();
    let catalog = Catalog::new(root.path());
    let job = record();
    assert!(catalog.reserve(job.clone()).unwrap());
    let write_lock = catalog.lock().unwrap();
    let (enter, entered) = mpsc::channel();
    let (finish, finished) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        enter.send(()).unwrap();
        finish.send(catalog.records_if_available()).unwrap();
    });
    entered.recv().unwrap();
    assert!(matches!(
        finished.recv_timeout(Duration::from_millis(25)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    drop(write_lock);
    let records = finished
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert_eq!(records[&job.name].remote_name, job.remote_name);
    reader.join().unwrap();
}

#[test]
fn a_delayed_old_launch_cannot_remove_a_new_reservation() {
    let root = tempfile::tempdir().unwrap();
    let catalog = Catalog::new(root.path());
    let first = record();
    assert!(catalog.reserve(first.clone()).unwrap());
    assert!(catalog.remove(&first).unwrap());
    let second = record();
    assert!(catalog.reserve(second.clone()).unwrap());
    assert!(!catalog.remove(&first).unwrap());
    assert_eq!(
        catalog.get(&second.name).unwrap().unwrap().remote_name,
        second.remote_name
    );
}

#[test]
fn ordinary_job_names_keep_spaces_and_unicode_without_becoming_paths() {
    let root = tempfile::tempdir().unwrap();
    let catalog = Catalog::new(root.path());
    let name = "daily estuary 🌊";
    let record = Record::new(name, Uuid::new_v4(), vec![], Map::new());
    assert!(catalog.reserve(record).unwrap());
    assert!(catalog.get(name).unwrap().is_some());
    for name in ["../outside", "..", ".", "a/b", "a\\b", "bad;command"] {
        assert!(!valid_name(name));
    }
}

#[test]
fn log_sanitization_preserves_colors_and_handles_split_clipboard_or_cursor_commands() {
    let mut parser = LogSanitizer::default();
    assert_eq!(parser.push("alpha\u{1b}]52;c;"), "alpha");
    assert_eq!(parser.push("clipboard\u{1b}"), "");
    assert_eq!(
        parser.push("\\beta\u{1b}[31mred\u{1b}[0m\u{1b}[2J"),
        "beta\u{1b}[31mred\u{1b}[0m"
    );
    assert_eq!(
        parser.push("\u{9d}8;;https://malicious\u{7}plain\u{9b}2J"),
        "plain"
    );
}
