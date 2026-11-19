use super::*;
use meerschaum_server::executor::control::Actor;

trait NativeModelConfiguration {
    fn configure_test(&self, base: meerschaum_core::Map) -> Result<(), String>;
}
impl NativeModelConfiguration for Contexts {
    fn configure_test(&self, base: meerschaum_core::Map) -> Result<(), String> {
        native_model_host()?;
        self.configure_runtime(
            base,
            meerschaum_core::process_env::vars()
                .into_iter()
                .filter(|(name, _)| meerschaum_core::team_scope::is_allowed(name))
                .collect(),
        )
    }
}

fn factory_contexts(root: &std::path::Path) -> Contexts {
    Contexts::for_factory(root.join("work"), root.join("control")).unwrap()
}

fn team_generation(contexts: &Contexts, id: Uuid) -> PathBuf {
    let inner = contexts.inner.lock().unwrap();
    let environment = inner.entries[&id].environment.as_ref().unwrap();
    let (_, path) = environment
        .iter()
        .find(|(key, _)| key == "MRSM_CONFIG_FILE")
        .unwrap();
    std::path::Path::new(path).parent().unwrap().to_path_buf()
}

#[test]
fn rebuild_revision_requires_exact_current_job_provenance() {
    use meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY;
    let root = tempfile::tempdir().unwrap();
    let contexts = factory_contexts(root.path());
    contexts
        .configure_test(
            meerschaum_core::Value::from_json_str(r#"{"meerschaum":{"connectors":{"api":{}}}}"#)
                .unwrap()
                .to_map(),
        )
        .unwrap();
    let id = Uuid::new_v4();
    let name = format!("mrsm-{}", id.simple());
    let properties =
        meerschaum_core::Map::from_iter([(JOB_CONTEXT_PROPERTY.into(), id.to_string().into())]);
    assert_eq!(contexts.job_revision(&name, &properties), None);
    let mut current = update(id, 30);
    current.revision = 7;
    current.target = Target::Job { name: name.clone() };
    contexts.update(&actor("alice"), current).unwrap();
    assert_eq!(contexts.job_revision(&name, &properties), Some(7));
    assert_eq!(contexts.job_revision(&name, &Default::default()), None);
    assert_eq!(
        contexts.job_revision(&format!("{name}/child"), &properties),
        None
    );
    let other = meerschaum_core::Map::from_iter([(
        JOB_CONTEXT_PROPERTY.into(),
        Uuid::new_v4().to_string().into(),
    )]);
    assert_eq!(contexts.job_revision(&name, &other), None);
    contexts
        .inner
        .lock()
        .unwrap()
        .entries
        .get_mut(&id)
        .unwrap()
        .expires_at = chrono::Utc::now().timestamp();
    assert_eq!(contexts.job_revision(&name, &properties), None);
    contexts
        .clear(&meerschaum_jobs::JobsContext::for_root(
            root.path().join("control"),
        ))
        .unwrap();
    assert_eq!(contexts.job_revision(&name, &properties), None);
}

#[test]
fn late_renewal_and_delete_cannot_revive_or_stop_a_new_launch_revision() {
    let root = tempfile::tempdir().unwrap();
    let contexts = factory_contexts(root.path());
    contexts
        .configure_test(
            meerschaum_core::Value::from_json_str(r#"{"meerschaum":{"connectors":{"api":{}}}}"#)
                .unwrap()
                .to_map(),
        )
        .unwrap();
    let id = Uuid::new_v4();
    contexts.update(&actor("alice"), update(id, 20)).unwrap();
    contexts.revoke(&actor("alice"), id, Some(1)).unwrap();
    assert!(contexts.update(&actor("alice"), update(id, 30)).is_err());
    let mut next = update(id, 30);
    next.revision = 2;
    contexts.update(&actor("alice"), next).unwrap();
    assert!(contexts.revoke(&actor("alice"), id, Some(1)).is_err());
    assert!(
        team_generation(&contexts, id)
            .join("credential.json")
            .exists()
    );
}

#[test]
fn delayed_job_retirement_keeps_a_new_revision_behind_its_stop_and_never_stops_it_late() {
    use std::sync::mpsc;
    use std::time::Duration;
    let root = tempfile::tempdir().unwrap();
    let contexts = Arc::new(factory_contexts(root.path()));
    contexts
        .configure_test(
            meerschaum_core::Value::from_json_str(r#"{"meerschaum":{"connectors":{"api":{}}}}"#)
                .unwrap()
                .to_map(),
        )
        .unwrap();
    let id = Uuid::new_v4();
    let name = format!("mrsm-{}", id.simple());
    let mut current = update(id, 25);
    current.target = Target::Job { name: name.clone() };
    contexts.update(&actor("alice"), current).unwrap();
    // Without the owning job runtime even a valid actor cannot forget an active target.
    assert!(contexts.revoke(&actor("alice"), id, Some(1)).is_err());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let retiring = contexts.clone();
    let expected = name.clone();
    let retirement = std::thread::spawn(move || {
        retiring.revoke_with(&actor("alice"), id, Some(1), |target| {
            assert_eq!(target, &Target::Job { name: expected });
            entered_tx.send(()).unwrap();
            finish_rx
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| "The owned stop model was not released.".to_string())?;
            Ok(())
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        contexts.inner.try_lock().is_err(),
        "The stop must hold the context lock."
    );
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let (delivered_tx, delivered_rx) = mpsc::channel();
    let delivering = contexts.clone();
    let mut next = update(id, 26);
    next.target = Target::Job { name: name.clone() };
    next.revision = 2;
    let delivery = std::thread::spawn(move || {
        attempt_tx.send(()).unwrap();
        let result = delivering.update(&actor("alice"), next);
        delivered_tx.send(result).unwrap();
    });
    attempt_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        delivered_rx
            .recv_timeout(Duration::from_millis(30))
            .is_err()
    );
    finish_tx.send(()).unwrap();
    assert_eq!(
        retirement.join().unwrap().unwrap(),
        Some(Target::Job { name })
    );
    delivered_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    delivery.join().unwrap();
    let mut stopped_newer = false;
    assert!(
        contexts
            .revoke_with(&actor("alice"), id, Some(1), |_| {
                stopped_newer = true;
                Ok(())
            })
            .is_err()
    );
    assert!(
        !stopped_newer,
        "A delayed old retirement must not invoke the stop callback."
    );
    assert_eq!(contexts.inner.lock().unwrap().entries[&id].revision, 2);
    assert!(
        team_generation(&contexts, id)
            .join("credential.json")
            .is_file()
    );
}

#[test]
fn marked_jobs_require_a_current_exact_context_on_every_restart_and_after_expiry() {
    use meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY;
    let root = tempfile::tempdir().unwrap();
    let contexts = Arc::new(factory_contexts(root.path()));
    contexts
        .configure_test(
            meerschaum_core::Value::from_json_str(r#"{"meerschaum":{"connectors":{"api":{}}}}"#)
                .unwrap()
                .to_map(),
        )
        .unwrap();
    let id = Uuid::new_v4();
    let name = format!("mrsm-{}", id.simple());
    let props =
        meerschaum_core::Map::from_iter([(JOB_CONTEXT_PROPERTY.into(), id.to_string().into())]);
    let jobs = meerschaum_jobs::JobsContext::for_root(root.path().join("control"))
        .with_launch_environment(vec![("MRSM_CONFIG".into(), "ambient".into())])
        .with_launch_resolver(contexts.clone());
    assert!(jobs.environment_for(&name, &props).is_err());
    let mut current = update(id, 30);
    current.target = Target::Job { name: name.clone() };
    contexts.update(&actor("alice"), current).unwrap();
    let environment = jobs.environment_for(&name, &props).unwrap();
    assert!(!environment.iter().any(|(key, _)| key == "MRSM_CONFIG"));
    assert!(environment.iter().any(|(key, _)| key == "MRSM_CONFIG_FILE"));
    assert!(
        jobs.environment_for(&format!("mrsm-{}", Uuid::new_v4().simple()), &props)
            .is_err()
    );
    contexts
        .inner
        .lock()
        .unwrap()
        .entries
        .get_mut(&id)
        .unwrap()
        .expires_at = chrono::Utc::now().timestamp() - 61;
    contexts.expire_jobs(&jobs);
    assert!(jobs.environment_for(&name, &props).is_err());
    let mut late = update(id, 30);
    late.target = Target::Job { name };
    assert!(contexts.update(&actor("alice"), late).is_err());
}

fn actor(owner: &str) -> Actor {
    Actor {
        owner: owner.into(),
        username: Some(owner.into()),
        permissions: Default::default(),
        administrator: false,
    }
}
fn update(id: Uuid, seconds: i64) -> Update {
    Update {
        metadata_deadline: None,
        discovery_catalog_sha256: None,
        extension: None,
        registration: None,
        id,
        revision: 1,
        owner: "alice".into(),
        owner_id: Some("registered-alice".into()),
        profile: Profile::Team,
        target: Target::Terminal,
        callback_url: "https://main.example:8989".into(),
        credential: meerschaum_connector_api::models::ExecutionCredential {
            token: format!("synthetic-{seconds}"),
            expires_at: chrono::Utc::now().timestamp() + seconds,
        },
    }
}
#[test]
fn metadata_renewal_cannot_install_a_callback_or_change_its_profile() {
    use meerschaum_server::executor::environment::{METADATA_ORIGIN, METADATA_TOKEN};
    let root = tempfile::tempdir().unwrap();
    let contexts = factory_contexts(root.path());
    contexts.configure_test(Default::default()).unwrap();
    let id = Uuid::new_v4();
    let directory = contexts.root.join(id.simple().to_string());
    let generation = directory.join("generation-test");
    create_private_dir(&generation).unwrap();
    let deadline = chrono::Utc::now().timestamp() + 40;
    let identity = Identity {
        source_compiler_sha256: None,
        metadata_deadline: Some(deadline),
        extension_sha256: None,
        registration_sha256: None,
        discovery_catalog_sha256: Some("a".repeat(64)),
        owner: "alice".into(),
        owner_id: Some("registered-alice".into()),
        profile: Profile::Metadata,
        target: Target::Job {
            name: format!("mrsm-{}", id.simple()),
        },
        callback_url: METADATA_ORIGIN.into(),
    };
    let proof = ContextProof {
        compiler: None,
        id,
        revision: 1,
        identity: identity.clone(),
        generation: None,
        attempted: false,
        cleared_for_rebuild: false,
        terminal_tracking: 0,
        terminal_spec: None,
    };
    write_private(
        &directory.join("identity.json"),
        &serde_json::to_string(&proof).unwrap(),
    )
    .unwrap();
    let spec_path = generation.join(meerschaum_core::team_scope::sandbox::JOB_SPEC_FILE);
    write_private(
        &spec_path,
        &serde_json::to_string(&meerschaum_core::team_scope::sandbox::Spec {
            mode: meerschaum_core::team_scope::sandbox::Mode::RequireSockets,
            ..Default::default()
        })
        .unwrap(),
    )
    .unwrap();
    contexts.inner.lock().unwrap().entries.insert(
        id,
        Entry {
            compiler: None,
            identity,
            revision: 1,
            expires_at: chrono::Utc::now().timestamp() + 10,
            environment: Some(vec![
                (
                    "MRSM_CONFIG_FILE".into(),
                    generation.join("config.json").display().to_string(),
                ),
                ("MRSM_TEAM_SANDBOX".into(), spec_path.display().to_string()),
            ]),
            generation: None,
        },
    );
    let make_update = || {
        let mut value = update(id, 20);
        value.profile = Profile::Metadata;
        value.metadata_deadline = Some(deadline);
        value.discovery_catalog_sha256 = Some("a".repeat(64));
        value.target = Target::Job {
            name: format!("mrsm-{}", id.simple()),
        };
        value.callback_url = METADATA_ORIGIN.into();
        value.credential.token = METADATA_TOKEN.into();
        value
    };
    contexts.update(&actor("alice"), make_update()).unwrap();
    let token_path = generation.join("credential.json");
    let sentinel = std::fs::read_to_string(&token_path).unwrap();
    assert!(sentinel.contains(METADATA_TOKEN));
    for offset in [-1, 1] {
        let mut changed = make_update();
        changed.metadata_deadline = Some(deadline + offset);
        assert!(contexts.update(&actor("alice"), changed).is_err());
        assert_eq!(std::fs::read_to_string(&token_path).unwrap(), sentinel);
    }
    for revision in [1, 2] {
        let mut changed = make_update();
        changed.revision = revision;
        changed.credential.token = "actual-callback-must-never-be-written".into();
        assert!(contexts.update(&actor("alice"), changed).is_err());
        assert_eq!(std::fs::read_to_string(&token_path).unwrap(), sentinel);
    }
    let mut promoted = make_update();
    promoted.profile = Profile::Api;
    promoted.callback_url = "https://main.example".into();
    promoted.credential.token = "actual-callback-must-never-be-written".into();
    assert!(contexts.update(&actor("alice"), promoted).is_err());
    assert_eq!(std::fs::read_to_string(&token_path).unwrap(), sentinel);
}

#[test]
fn team_configuration_rotates_only_its_bearer_and_keeps_the_launch_identity() {
    let root = tempfile::tempdir().unwrap();
    let contexts = factory_contexts(root.path());
    let base = meerschaum_core::Value::from_json_str(r#"{"meerschaum":{"instance":"sql:team", "connectors":{"sql":{"team":{"password":"synthetic-team-only"},"main":null},"api":{}}}}"#).unwrap().to_map();
    contexts.configure_test(base).unwrap();
    let id = Uuid::new_v4();
    assert!(contexts.update(&actor("bob"), update(id, 20)).is_err());
    contexts.update(&actor("alice"), update(id, 20)).unwrap();
    let directory = team_generation(&contexts, id);
    let config = std::fs::read_to_string(directory.join("config.json")).unwrap();
    let before_environment = contexts.inner.lock().unwrap().entries[&id]
        .environment
        .clone()
        .unwrap();
    assert!(
        before_environment
            .iter()
            .any(|(name, value)| name == "MRSM_ROOT_DIR"
                && std::path::Path::new(value) == root.path().join("work"))
    );
    let descriptor = before_environment
        .iter()
        .find(|(name, _)| name == meerschaum_core::team_scope::source_namespace::ENV)
        .unwrap();
    let selected = meerschaum_core::team_scope::source_namespace::Launch::read(
        std::path::Path::new(&descriptor.1),
    )
    .unwrap();
    assert!(selected.namespace.names.is_empty());
    assert!(selected.namespace.package.starts_with(&directory));
    let json: serde_json::Value = serde_json::from_str(&config).unwrap();
    assert_eq!(json["meerschaum"]["instance"], "sql:team");
    assert!(json["meerschaum"]["connectors"]["sql"]["main"].is_null());
    assert_eq!(json["meerschaum"]["executor"], "api:main_callback");
    assert!(!config.contains("synthetic-20"));
    let mut moved = update(id, 25);
    moved.callback_url = "https://other.example".into();
    assert!(contexts.update(&actor("alice"), moved).is_err());
    let mut replacement = update(id, 25);
    replacement.owner_id = Some("replacement-alice".into());
    assert!(contexts.update(&actor("alice"), replacement).is_err());
    let mut api = update(id, 25);
    api.profile = Profile::Api;
    assert!(contexts.update(&actor("alice"), api).is_err());
    contexts.update(&actor("alice"), update(id, 30)).unwrap();
    assert_eq!(
        contexts.inner.lock().unwrap().entries[&id]
            .environment
            .as_ref(),
        Some(&before_environment)
    );
    assert!(contexts.update(&actor("alice"), update(id, 10)).is_err());
    assert_eq!(
        std::fs::read_to_string(directory.join("config.json")).unwrap(),
        config
    );
    let token = std::fs::read_to_string(directory.join("credential.json")).unwrap();
    assert!(token.contains("synthetic-30"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(directory.join("credential.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    assert!(contexts.revoke(&actor("bob"), id, Some(1)).is_err());
    contexts.revoke(&actor("alice"), id, Some(1)).unwrap();
    assert!(!directory.exists());
    assert!(!root.path().join("unrelated").exists());
}

#[test]
fn terminal_shell_requires_the_registered_identity_and_removes_ambient_config_patches() {
    let root = tempfile::tempdir().unwrap();
    let contexts = factory_contexts(root.path());
    contexts
        .configure_test(
            meerschaum_core::Value::from_json_str(r#"{"meerschaum":{"connectors":{"api":{}}}}"#)
                .unwrap()
                .to_map(),
        )
        .unwrap();
    let id = Uuid::new_v4();
    let mut grant = meerschaum_server::executor::terminal::TerminalGrant {
        id,
        owner: "alice".into(),
        owner_id: "registered-alice".into(),
    };
    let mut shell = ShellCommand::new("mrsm");
    shell.env = [
        "MRSM_CONFIG",
        "MRSM_CONFIG_FILE",
        "MRSM_PATCH",
        "MRSM_PATCH_FILE",
        "MRSM_PLUGINS_DIR",
    ]
    .map(|key| (key.into(), "ambient".into()))
    .into();
    assert!(contexts.shell(id, &grant, &shell).is_err());
    contexts.update(&actor("alice"), update(id, 30)).unwrap();
    grant.owner_id = "replacement-alice".into();
    assert!(contexts.shell(id, &grant, &shell).is_err());
    grant.owner_id = "registered-alice".into();
    let shell = contexts.shell(id, &grant, &shell).unwrap();
    assert!(shell.clear_env);
    assert_eq!(
        shell.args.first().map(String::as_str),
        Some(crate::executor_terminal_guard::TEAM_EXEC_ARG),
    );
    assert!(!shell.env.iter().any(|(key, value)| value == "ambient"
        || matches!(
            key.as_str(),
            "MRSM_CONFIG" | "MRSM_PATCH" | "MRSM_PATCH_FILE" | "TMUX" | "TMUX_PANE"
        )));
    assert!(
        shell
            .env
            .iter()
            .any(|(key, _)| key == meerschaum_core::team_scope::source_namespace::ENV)
    );
    assert!(
        shell
            .env
            .iter()
            .any(|(key, value)| key == "MRSM_CONFIG_FILE" && value.ends_with("/config.json"))
    );
    contexts
        .clear(&meerschaum_jobs::JobsContext::for_root(
            root.path().join("control"),
        ))
        .unwrap();
    assert!(contexts.shell(id, &grant, &shell).is_err());
}
