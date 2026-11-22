//! Tests of how a job's process is launched: the launch environment a team's job receives at
//! every start and restart, and which resolver may own a supervisor's lifecycle.

use super::*;

#[derive(Debug, Default)]
struct CurrentLaunch(std::sync::atomic::AtomicBool);
impl LaunchEnvironment for CurrentLaunch {
    fn environment(
        &self,
        _: &str,
        _: &Map,
        base: &[(String, String)],
    ) -> std::io::Result<Vec<(String, String)>> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(std::io::Error::other("The canonical launch was revoked."));
        }
        let mut environment = base.to_vec();
        environment.push(("TPR_CURRENT_CONTEXT".into(), "fresh".into()));
        Ok(environment)
    }
}

#[cfg(target_os = "linux")]
#[test]
fn only_the_host_resolver_selects_a_supervisor_lifecycle_owner() {
    #[derive(Debug)]
    struct GuardedLaunch;
    impl LaunchEnvironment for GuardedLaunch {
        fn environment(
            &self,
            _: &str,
            _: &Map,
            base: &[(String, String)],
        ) -> std::io::Result<Vec<(String, String)>> {
            Ok(base.to_vec())
        }
        fn owner_guard(
            &self,
            _: &str,
            _: &Map,
        ) -> std::io::Result<Option<crate::supervisor::LifecycleOwner>> {
            crate::supervisor::LifecycleOwner::current().map(Some)
        }
    }
    let root = tempfile::tempdir().unwrap();
    let output = root.path().join("supervisor-argv");
    let temporary = output.with_extension("tmp");
    let quoted_output = crate::shlex::join(&[output.display().to_string()]);
    let quoted_temporary = crate::shlex::join(&[temporary.display().to_string()]);
    let script = format!(
        "printf '%s\\n' \"$@\" > {quoted_temporary} && mv {quoted_temporary} {quoted_output}"
    );
    let ctx = JobsContext::for_root(root.path())
        .with_launcher(Launcher {
            supervisor: vec!["/bin/sh".into(), "-c".into(), script],
            program: vec!["unused".into()],
        })
        .with_launch_environment(vec![("PATH".into(), "/bin:/usr/bin".into())])
        .with_launch_resolver(Arc::new(GuardedLaunch));
    let properties = Map::from_iter([
        (
            "lifecycle_owner".into(),
            Value::from_json_str(r#"{"pid":1,"started_at":"2000-01-01T00:00:00"}"#).unwrap(),
        ),
        (
            "env".into(),
            Value::Map(Map::from_iter([(
                "MRSM_LIFECYCLE_OWNER".into(),
                "untrusted".into(),
            )])),
        ),
    ]);
    let mut daemon = Daemon::new(
        "guarded",
        vec!["show".into(), "version".into()],
        None,
        properties,
        &ctx,
    );
    assert!(daemon.write_properties().success);
    assert!(daemon.supervisor_options(true).lifecycle_owner.is_none());
    daemon.launch(true, None).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !output.exists() {
        assert!(
            Instant::now() < deadline,
            "Owned supervisor fixture wrote no arguments."
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let arguments = std::fs::read_to_string(output)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let options = SupervisorOptions::from_args(arguments).unwrap();
    let owner = options.lifecycle_owner.unwrap();
    assert!(owner.alive());
    assert_eq!(
        serde_json::to_value(owner).unwrap()["pid"],
        std::process::id()
    );
}

#[test]
fn a_restarted_executor_job_cannot_restore_its_recorded_grants_or_environment() {
    let root = tempfile::tempdir().unwrap();
    let output = root.path().join("observed-env");
    let policy = Arc::new(CurrentLaunch::default());
    let ctx = JobsContext::for_root(root.path())
        .with_launcher(Launcher {
            supervisor: vec![
                "/bin/sh".into(),
                "-c".into(),
                format!("env > '{}.tmp' && mv '{0}.tmp' '{0}'", output.display()),
            ],
            program: vec!["unused".into()],
        })
        .with_launch_environment(vec![
            ("PATH".into(), "/bin:/usr/bin".into()),
            ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
            ("MRSM_EXECUTOR_PLUGINS".into(), "[]".into()),
            (
                "MRSM_PLUGINS_DIR".into(),
                root.path().join("current-view").display().to_string(),
            ),
        ])
        .with_launch_resolver(policy.clone());
    let mut props = Map::new();
    props.insert(
        "env".into(),
        Value::Map(Map::from_iter([
            ("MRSM_EXECUTOR_PLUGINS".into(), "[\"revoked\"]".into()),
            ("MRSM_PLUGINS_DIR".into(), "/old/grants".into()),
            ("MRSM_ROOT_DIR".into(), "/old/root".into()),
            ("TPR_OLD_CREDENTIAL".into(), "synthetic-old-key".into()),
            ("TPR_CURRENT_CONTEXT".into(), "stale".into()),
        ])),
    );
    let mut daemon = Daemon::new(
        "restarted",
        vec!["show".into(), "version".into()],
        None,
        props,
        &ctx,
    );
    assert!(daemon.write_properties().success);
    daemon = Daemon::open("restarted", &ctx).unwrap();
    daemon.launch(false, None).unwrap();
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    while !output.exists() {
        assert!(
            Instant::now() < deadline,
            "The owned launch did not write its environment."
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let environment = std::fs::read_to_string(&output).unwrap();
    assert!(
        environment
            .lines()
            .any(|line| line == "MRSM_EXECUTOR_PLUGINS=[]")
    );
    assert!(
        environment
            .lines()
            .any(|line| line == format!("MRSM_PLUGINS_DIR={}/current-view", root.path().display()))
    );
    for old in ["revoked", "/old/grants", "/old/root", "TPR_OLD_CREDENTIAL"] {
        assert!(!environment.contains(old), "{old}");
    }
    assert!(
        environment
            .lines()
            .any(|line| line == "TPR_CURRENT_CONTEXT=fresh")
    );
    policy.0.store(true, std::sync::atomic::Ordering::SeqCst);
    std::fs::remove_file(&output).unwrap();
    daemon = Daemon::open("restarted", &ctx).unwrap();
    assert!(daemon.launch(false, None).is_err());
    assert!(
        !output.exists(),
        "A revoked resolver still started its recorded environment."
    );
}

/// 5.1: an isolated team's job is launched from an empty environment with the allowlist only:
/// neither the launching process's other variables nor those its recorded `env` holds (an API
/// server records its whole environment), except the names passed on; a relaunch (the restart
/// monitor's after a reboot) gets the same, and the recorded `env` keeps the allowlist only.
#[test]
fn a_team_jobs_process_gets_the_allowlisted_environment_at_every_launch() {
    use meerschaum_core::team_scope::{TeamScope, write_scope_record};
    // This unit models preparation in its actual native Rust host; it never runs a fake
    // interpreter/supervisor as if it had established the mandatory boundary. The CLI
    // team_runtime integration covers the real native and SDK launch paths.
    let _ = meerschaum_core::self_exe::register_native_current_executable();
    let root = tempfile::tempdir().unwrap();
    let root = root.path().canonicalize().unwrap();
    std::fs::create_dir_all(root.join("config")).unwrap();
    std::fs::write(
            root.join("config/meerschaum.yaml"),
            "connectors:\n  sql:\n    wetlands:\n      flavor: sqlite\n      database: /tmp/w.db\n      teams: [wetlands]\n",
        )
        .unwrap();
    std::fs::write(
        root.join("config/system.yaml"),
        "engine:\n  python_team_env_passthrough: [TPR_PASSED, MRSM_SQL_LISTED]\n",
    )
    .unwrap();
    write_scope_record(
        &root,
        &TeamScope {
            team: "wetlands".into(),
            team_id: Some("id-w".into()),
            connectors: ["sql:wetlands".to_string()].into_iter().collect(),
            ..TeamScope::default()
        },
    )
    .unwrap();
    let out = root.join("seen-env");
    // A shell can never be the trusted lifecycle launcher of a marked team process.
    let ctx = JobsContext::for_root(&root).with_launcher(Launcher {
        supervisor: vec![
            "/bin/sh".into(),
            "-c".into(),
            format!("env > '{}.tmp' && mv '{0}.tmp' '{0}'", out.display()),
        ],
        program: vec!["unused".into()],
    });
    let mut env = Map::new();
    for (k, v) in [
        ("MRSM_ROOT_DIR", root.to_str().unwrap()),
        ("TPR_SENTINEL_CLOUD_KEY", "recorded-secret"),
        ("TPR_PASSED", "passed"),
        ("MRSM_SQL_LISTED", "postgresql://u:server-pw@db/m"),
        ("LANG", "C"),
    ] {
        env.insert(k.into(), Value::Str(v.into()));
    }
    let mut props = Map::new();
    props.insert("team".into(), Value::Str("wetlands".into()));
    props.insert("env".into(), Value::Map(env));
    let mut daemon = Daemon::new("team-job", vec!["sync".into()], None, props, &ctx);
    assert!(daemon.write_properties().success);
    let seen = |daemon: &mut Daemon| -> Vec<(String, String)> {
        let _ = std::fs::remove_file(&out);
        let properties = daemon.properties();
        let process_env = daemon
            .ctx
            .environment_for(&daemon.daemon_id, &properties)
            .unwrap();
        let worker_env = daemon.worker_env();
        let prepared = daemon
            .team_environment(&process_env, &worker_env)
            .unwrap()
            .unwrap();
        let mut full = process_env;
        full.extend(worker_env);
        let environment = prepared.apply(&full);
        assert!(daemon.launch(false, None).is_err());
        assert!(
            !out.exists(),
            "A fake supervisor ran before the native boundary."
        );
        environment
    };
    for launch in 0..2 {
        let env = seen(&mut daemon);
        let get = |name: &str| env.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());
        // The test process's own variables (cargo's) and the recorded sentinel stay out.
        for absent in [
            "CARGO_MANIFEST_DIR",
            "TPR_SENTINEL_CLOUD_KEY",
            "MRSM_SQL_LISTED",
            "MRSM_CONFIG",
        ] {
            assert_eq!(get(absent), None, "{launch} {absent}: {env:?}");
        }
        assert_eq!(get("TPR_PASSED"), Some("passed"), "{env:?}");
        assert_eq!(get("LANG"), Some("C"));
        assert_eq!(get("MRSM_DAEMON_ID"), Some("team-job"));
        assert_eq!(get("MRSM_TEAM_SCOPE"), Some("wetlands"));
        assert!(get("MRSM_CONFIG_FILE").is_some_and(|f| f.ends_with(".mrsm_config.json")));
        // 5.1: the job's `mrsm` is pointed at the team's filesystem sandbox (`auto`): its
        // rules next to its configuration, the team's own temporary and plugins directories,
        // and a work directory for what its command writes.
        assert!(
            get("MRSM_TEAM_SANDBOX").is_some_and(|f| f.ends_with(".mrsm_sandbox.json")),
            "{env:?}"
        );
        assert!(
            get("TMPDIR").is_some_and(|t| t.contains("-team-")),
            "{env:?}"
        );
        assert!(
            get("MRSM_PLUGINS_DIR").is_some_and(|p| p.contains("team_scopes")),
            "{env:?}"
        );
        assert!(
            daemon
                .paths
                .dir
                .join(meerschaum_core::team_scope::sandbox::JOB_WORK_DIR)
                .is_dir()
        );
        // The next launch reopens the job from its files (as the restart monitor does).
        daemon = Daemon::open("team-job", &ctx).unwrap();
    }
    let recorded = daemon.properties()["env"].to_map();
    let mut names: Vec<&String> = recorded.keys().collect();
    names.sort();
    assert_eq!(names, vec!["LANG", "MRSM_ROOT_DIR", "TPR_PASSED"]);
    // The team's temporary directory is under the system's: leave nothing behind.
    if let Ok(tmp) = meerschaum_core::team_scope::sandbox::team_tmp_dir(&root, "id-w") {
        let _ = std::fs::remove_dir_all(tmp);
    }
}
