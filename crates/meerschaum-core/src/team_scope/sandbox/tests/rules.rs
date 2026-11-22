//! Tests of the spec and its preparation: modes, the rules of a team's paths, protected paths,
//! features and levels by ABI, the spec file and environment, and a team's own virtual
//! environments.

use super::*;

#[test]
fn the_modes_parse_and_unknown_values_require_the_sandbox() {
    assert_eq!(Mode::parse("auto"), Some(Mode::Auto));
    assert_eq!(Mode::parse(" REQUIRE "), Some(Mode::Require));
    assert_eq!(Mode::parse("off"), Some(Mode::Off));
    assert_eq!(Mode::parse("require_sockets"), Some(Mode::RequireSockets));
    assert_eq!(Mode::parse("Require-Sockets"), Some(Mode::RequireSockets));
    assert_eq!(Mode::parse("sometimes"), None);
    assert_eq!(
        serde_json::to_string(&Mode::RequireSockets).unwrap(),
        "\"require_sockets\""
    );
    assert_eq!(
        Mode::Auto.stricter(Mode::RequireSockets),
        Mode::RequireSockets
    );
    assert_eq!(Mode::Require.stricter(Mode::Auto), Mode::Require);
    let config = |text: &str| {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("config")).unwrap();
        std::fs::write(dir.path().join("config/system.yaml"), text).unwrap();
        let env = vec![(
            "MRSM_ROOT_DIR".to_string(),
            dir.path().to_string_lossy().into_owned(),
        )];
        let mut options = crate::config::ConfigOptions::with_env(env);
        options.allow_writes = false;
        (dir, Config::new(options).unwrap())
    };
    let (_d, c) = config("engine: {}\n");
    assert_eq!(Mode::from_config(&c), Mode::Auto);
    let (_d, c) = config("engine:\n  python_team_sandbox: require\n");
    assert_eq!(Mode::from_config(&c), Mode::Require);
    let (_d, c) = config("engine:\n  python_team_sandbox: off\n");
    assert_eq!(Mode::from_config(&c), Mode::Off);
    let (_d, c) = config("engine:\n  python_team_sandbox: sometimes\n");
    assert_eq!(Mode::from_config(&c), Mode::Require);
}

#[test]
fn the_rules_cover_the_teams_own_paths_only() {
    let l = layout();
    let spec = spec_of(&l, Mode::Auto, &["mine", "single", "../evil", ".hidden"]);
    assert_eq!(
        rule_for(&spec, &l.plugins_dir.join("mine")).map(|r| r.access),
        Some(Access::Read)
    );
    assert!(rule_for(&spec, &l.plugins_dir.join("single.py")).is_some());
    assert!(rule_for(&spec, &l.plugins_dir.join("theirs")).is_none());
    assert!(
        !spec
            .rules
            .iter()
            .any(|r| r.path.to_string_lossy().contains("evil")
                || r.path.to_string_lossy().contains("hidden"))
    );
    // 5.1: its own virtual environments, never the server's of a plugin it was granted.
    assert_eq!(
        rule_for(&spec, &l.venvs).map(|r| r.access),
        Some(Access::ReadWrite)
    );
    assert!(rule_for(&spec, &l.root.join("venvs/mine")).is_none());
    assert!(rule_for(&spec, &l.root.join("venvs/theirs")).is_none());
    assert_eq!(
        rule_for(&spec, &l.root.join("venvs/mrsm")).map(|r| r.access),
        Some(Access::ReadExec)
    );
    assert!(
        !spec
            .rules
            .iter()
            .any(|r| r.path.starts_with(l.root.join(".cache"))),
        "no lock file of the server's"
    );
    assert_eq!(
        rule_for(&spec, &l.files.config_file).map(|r| r.access),
        Some(Access::Read)
    );
    assert_eq!(
        rule_for(&spec, &l.job).map(|r| r.access),
        Some(Access::Read)
    );
    assert!(rule_for(&spec, &l.other_job).is_none());
    assert_eq!(
        rule_for(&spec, &l.work).map(|r| r.access),
        Some(Access::ReadWrite)
    );
    assert_eq!(
        rule_for(&spec, &l.tmp).map(|r| r.access),
        Some(Access::ReadWrite)
    );
    assert_eq!(
        rule_for(&spec, &l.root.join(".internal")).map(|r| r.access),
        Some(Access::List)
    );
    // Nothing covers the root, its config, the home directory or another team's scope.
    for path in [
        l.root.clone(),
        l.root.join("config"),
        l.root.join("team_scopes"),
        l.root.join("team_scopes/key-b"),
        l.home.clone(),
        l.home.join(".ssh"),
    ] {
        assert!(
            !spec.rules.iter().any(|r| path.starts_with(&r.path)),
            "{} is covered",
            path.display()
        );
    }
    assert_eq!(spec.canary, l.root.join("team_scopes").join(CANARY_FILE));
    // The system's programs, the loader, certificates and its own /proc entry are readable;
    // /proc as a whole, /tmp and /dev as a whole are not.
    for (path, access) in [
        ("/usr", Access::ReadExec),
        ("/etc/ssl/certs", Access::Read),
        ("/proc/self", Access::Read),
        ("/dev/null", Access::Device),
        ("/dev/shm", Access::SharedMemory),
    ] {
        assert_eq!(
            rule_for(&spec, Path::new(path)).map(|r| r.access),
            Some(access)
        );
    }
    for path in [
        "/proc", "/tmp", "/dev", "/etc", "/etc/ssl", "/etc/pki", "/home", "/run",
    ] {
        assert!(rule_for(&spec, Path::new(path)).is_none(), "{path}");
    }
}

#[test]
fn rules_covering_protected_paths_are_dropped() {
    let l = layout();
    let spec = spec_of(&l, Mode::Auto, &["mine"]);
    let extra = vec![
        Rule::new(l.home.clone(), Access::ReadExec, "a PYTHONPATH of ~"),
        Rule::new(
            l.root.parent().unwrap(),
            Access::Read,
            "a checkout holding the root",
        ),
        Rule::new("/", Access::Read, "everything"),
        Rule::new("relative/path", Access::Read, "relative"),
        Rule::new(l.home.join(".local/lib"), Access::ReadExec, "user site"),
        Rule::new(l.root.join("config/../config"), Access::Read, "sneaky"),
    ];
    let (kept, dropped) = spec.allowed(extra);
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].path, l.home.join(".local/lib"));
    assert_eq!(dropped.len(), 5);
    // A root under /opt drops the /opt rule rather than expose the root.
    let opt_spec = Spec {
        protected: vec![PathBuf::from("/opt/meerschaum")],
        ..spec.clone()
    };
    let (_, dropped) = opt_spec.allowed(vec![Rule::new("/opt", Access::ReadExec, "")]);
    assert_eq!(dropped.len(), 1);
}

#[test]
fn a_team_job_dir_is_found_by_its_scope_key_or_team_name() {
    let l = layout();
    std::fs::create_dir_all(l.root.join("jobs/named")).unwrap();
    std::fs::write(
        l.root.join("jobs/named/properties.json"),
        "{\"team\": \"Alpha\"}",
    )
    .unwrap();
    let dirs = team_job_dirs(&l.root.join("jobs"), "key-a", "alpha");
    assert_eq!(dirs, vec![l.job.clone(), l.root.join("jobs/named")]);
}

#[test]
fn the_verdict_follows_the_mode() {
    let enforced = |canary: Canary, mode: Mode| Report {
        mode,
        enforced: true,
        abi: 9,
        kernel_abi: 11,
        canary,
        ..Report::default()
    };
    assert!(enforced(Canary::Denied, Mode::Require).verdict().is_ok());
    assert!(enforced(Canary::Readable, Mode::Require).verdict().is_err());
    assert!(enforced(Canary::Readable, Mode::Auto).verdict().is_err());
    assert!(enforced(Canary::Missing, Mode::Auto).verdict().is_err());
    assert!(
        Report::not_enforced(Mode::Auto, "no Landlock")
            .verdict()
            .is_ok()
    );
    let refused = Report::not_enforced(Mode::Require, "no Landlock")
        .verdict()
        .unwrap_err();
    assert!(refused.contains("required") && refused.contains("no Landlock"));
    assert!(Report::not_enforced(Mode::Off, "off").verdict().is_ok());
}

#[test]
fn features_follow_the_abi() {
    let (have, lack) = features_of(1);
    assert_eq!(have, vec!["filesystem"]);
    assert!(lack.contains(&"resolve_unix".to_string()));
    let (have, lack) = features_of(6);
    assert!(have.contains(&"scope_signal".to_string()) && have.contains(&"ioctl_dev".to_string()));
    assert_eq!(lack, vec!["all_threads", "resolve_unix"]);
    let (have, lack) = features_of(11);
    assert_eq!(have.len(), FEATURES.len());
    assert!(lack.is_empty());
    let support = Support::Available { kernel_abi: 11 };
    assert_eq!(support.abi(), NEWEST_ABI);
    assert!(
        support.describe().contains("ABI 9") && support.describe().contains("kernel offers ABI 11")
    );
}

#[test]
fn prepare_writes_the_spec_and_points_the_environment_at_it() {
    let l = layout();
    std::fs::write(
        l.root.join("config/system.yaml"),
        "engine:\n  python_team_sandbox: auto\n",
    )
    .unwrap();
    let env = vec![
        (
            "MRSM_ROOT_DIR".to_string(),
            l.root.to_string_lossy().into_owned(),
        ),
        (
            "MRSM_PLUGINS_DIR".to_string(),
            l.plugins_dir.to_string_lossy().into_owned(),
        ),
    ];
    let mut options = crate::config::ConfigOptions::with_env(env);
    options.allow_writes = false;
    let config = Config::new(options).unwrap();
    let scope = TeamScope {
        team: "alpha".into(),
        team_id: Some("key-a".into()),
        plugins: ["mine".to_string()].into_iter().collect(),
        ..TeamScope::default()
    };
    let job_spec = l.job.join(JOB_SPEC_FILE);
    let prepared = prepare(&config, &scope, &l.files, Some(&l.job), &job_spec)
        .unwrap()
        .unwrap();
    assert_eq!(Spec::read(&job_spec).unwrap(), prepared.spec);
    assert!(prepared.tmp_dir.is_dir());
    assert!(
        prepared.tmp_dir.to_string_lossy().len() < 60,
        "{}",
        prepared.tmp_dir.display()
    );
    let mut env = vec![("TMPDIR".to_string(), "/tmp".to_string())];
    apply_env(&mut env, &prepared);
    let get = |name: &str| env.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
    assert_eq!(
        get("TMPDIR"),
        Some(prepared.tmp_dir.to_string_lossy().into_owned())
    );
    assert_eq!(get(SPEC_VAR), Some(job_spec.to_string_lossy().into_owned()));
    assert_eq!(
        env.iter().filter(|(k, _)| k == "TMPDIR").count(),
        1,
        "TMPDIR replaced"
    );
    assert!(rule_for(&prepared.spec, &l.job.join(JOB_WORK_DIR)).is_some());
    // The team's plugins directory links its plugins only, and its processes use it.
    let view: Vec<String> = std::fs::read_dir(&prepared.plugins_dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(view, vec!["mine".to_string()]);
    assert_eq!(
        get("MRSM_PLUGINS_DIR"),
        Some(prepared.plugins_dir.to_string_lossy().into_owned())
    );
    assert!(l.root.join(".internal/plugins/mine").exists());
    // Off writes nothing and removes a stale spec.
    std::fs::write(
        l.root.join("config/system.yaml"),
        "engine:\n  python_team_sandbox: 'off'\n",
    )
    .unwrap();
    let env = vec![(
        "MRSM_ROOT_DIR".to_string(),
        l.root.to_string_lossy().into_owned(),
    )];
    let mut options = crate::config::ConfigOptions::with_env(env);
    options.allow_writes = false;
    let config = Config::new(options).unwrap();
    assert!(
        prepare(&config, &scope, &l.files, Some(&l.job), &job_spec)
            .unwrap()
            .is_none()
    );
    assert!(!job_spec.exists());
    // The team's temporary directory is under the system's: leave nothing behind.
    let _ = std::fs::remove_dir_all(&prepared.tmp_dir);
}

/// 5.1: two teams granted the same plugin each get a copy of the server's virtual environment of
/// it (bytecode left out, links into it pointing into the copy) and a link to the shared `mrsm`
/// environment; neither copy is the other's or the server's, and a team's process may write its
/// own copy only.
#[test]
fn teams_granted_one_plugin_get_their_own_copies_of_its_environment() {
    let l = layout();
    let shared = l.root.join("venvs/mine");
    std::fs::create_dir_all(shared.join("lib/__pycache__")).unwrap();
    std::fs::write(shared.join("lib/__pycache__/site.cpython-314.pyc"), "x").unwrap();
    std::fs::write(shared.join("lib/pkg.py"), "VALUE = 'server'\n").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(shared.join("lib"), shared.join("lib64")).unwrap();
    std::fs::create_dir_all(l.root.join("venvs/mrsm/lib")).unwrap();
    let env = vec![
        (
            "MRSM_ROOT_DIR".to_string(),
            l.root.to_string_lossy().into_owned(),
        ),
        (
            "MRSM_PLUGINS_DIR".to_string(),
            l.plugins_dir.to_string_lossy().into_owned(),
        ),
    ];
    let mut options = crate::config::ConfigOptions::with_env(env);
    options.allow_writes = false;
    let config = Config::new(options).unwrap();
    let mut prepared = Vec::new();
    for key in ["key-c", "key-d"] {
        let scope = TeamScope {
            team: key.into(),
            team_id: Some(key.into()),
            plugins: ["mine".to_string()].into_iter().collect(),
            ..TeamScope::default()
        };
        let dir = l.root.join("team_scopes").join(key);
        let team_files = files(&dir);
        std::fs::create_dir_all(&team_files.config_dir).unwrap();
        std::fs::write(&team_files.config_file, "{}").unwrap();
        let spec_file = dir.join(SPEC_FILE);
        let p = prepare(&config, &scope, &team_files, None, &spec_file)
            .unwrap()
            .unwrap();
        assert_eq!(p.venvs_dir, dir.join(VENVS_DIR));
        let mut env = Vec::new();
        apply_env(&mut env, &p);
        assert!(env.contains(&(
            "MRSM_VENVS_DIR".to_string(),
            p.venvs_dir.to_string_lossy().into_owned()
        )));
        let copy = p.venvs_dir.join("mine");
        assert_eq!(
            std::fs::read_to_string(copy.join("lib/pkg.py")).unwrap(),
            "VALUE = 'server'\n"
        );
        assert!(!copy.join("lib/__pycache__").exists());
        #[cfg(unix)]
        {
            assert_eq!(
                std::fs::read_link(copy.join("lib64")).unwrap(),
                copy.join("lib")
            );
            assert_eq!(
                std::fs::read_link(p.venvs_dir.join(SHARED_VENV)).unwrap(),
                l.root.join("venvs/mrsm")
            );
        }
        // A copy that exists is the team's: a second preparation leaves it as it is.
        std::fs::write(copy.join("lib/installed.py"), key).unwrap();
        prepare(&config, &scope, &team_files, None, &spec_file).unwrap();
        assert_eq!(
            std::fs::read_to_string(copy.join("lib/installed.py")).unwrap(),
            key
        );
        prepared.push(p);
    }
    assert!(!shared.join("lib/installed.py").exists());
    assert_eq!(
        std::fs::read_to_string(prepared[1].venvs_dir.join("mine/lib/installed.py")).unwrap(),
        "key-d"
    );
    #[cfg(target_os = "linux")]
    {
        let s = |v: &str| v.to_string();
        let plan = ChildPlan {
            spec: prepared[0].spec.clone(),
            extra: Vec::new(),
            read: vec![(
                s("read its copy"),
                prepared[0].venvs_dir.join("mine/lib/pkg.py"),
            )],
            write: vec![
                (s("its copy"), prepared[0].venvs_dir.join("mine/lib/new.py")),
                (
                    s("the other team's copy"),
                    prepared[1].venvs_dir.join("mine/lib/new.py"),
                ),
                (s("the server's"), shared.join("lib/new.py")),
                (
                    s("the shared mrsm environment"),
                    prepared[0].venvs_dir.join("mrsm/lib/new.py"),
                ),
            ],
            connect: Vec::new(),
            connect_abstract: Vec::new(),
            signal: None,
            fifo: Vec::new(),
            ..ChildPlan::default()
        };
        if let Some((report, results)) = run_child(&plan) {
            assert!(report.enforced, "{report:?}");
            assert_eq!(results["its copy"], "ok", "{results:?}");
            for label in [
                "the other team's copy",
                "the server's",
                "the shared mrsm environment",
            ] {
                assert_eq!(results[label], "denied", "{label}: {results:?}");
            }
        }
    }
    for p in &prepared {
        let _ = std::fs::remove_dir_all(&p.tmp_dir);
    }
}

/// 5.1: the two levels and the mode that asks for the stronger one.
#[test]
fn the_level_follows_the_abi_and_require_sockets_asks_for_the_stronger_one() {
    assert_eq!(level_of(1), LEVEL_FILESYSTEM);
    assert_eq!(level_of(8), LEVEL_FILESYSTEM);
    assert_eq!(level_of(9), LEVEL_SOCKETS);
    assert_eq!(
        Support::Available { kernel_abi: 11 }.level(),
        Some(LEVEL_SOCKETS)
    );
    assert_eq!(
        Support::Available { kernel_abi: 6 }.level(),
        Some(LEVEL_FILESYSTEM)
    );
    let report = |mode: Mode, abi: u32| {
        let (features, unavailable) = features_of(abi);
        Report {
            mode,
            enforced: true,
            abi,
            kernel_abi: abi,
            features,
            unavailable,
            canary: Canary::Denied,
            level: level_of(abi).to_string(),
            ..Report::default()
        }
    };
    assert!(report(Mode::Require, 8).verdict().is_ok());
    assert!(report(Mode::Auto, 8).verdict().is_ok());
    let refused = report(Mode::RequireSockets, 8).verdict().unwrap_err();
    assert!(
        refused.contains("require_sockets") && refused.contains("Unix sockets are not denied"),
        "{refused}"
    );
    assert!(report(Mode::RequireSockets, 9).verdict().is_ok());
    // 5.1: below ABI 9 the seccomp filter denies Unix sockets: `require_sockets` accepts it
    // where Landlock scopes signals (ABI 6), and any mode refuses a filter that did not hold.
    let filtered = |mode: Mode, abi: u32, probe: SocketProbe| Report {
        unix_sockets: SOCKETS_BY_SECCOMP.to_string(),
        socket_probe: probe,
        ..report(mode, abi)
    };
    assert!(
        filtered(Mode::RequireSockets, 8, SocketProbe::Denied)
            .verdict()
            .is_ok()
    );
    let unscoped = filtered(Mode::RequireSockets, 5, SocketProbe::Denied)
        .verdict()
        .unwrap_err();
    assert!(unscoped.contains("cannot scope signals"), "{unscoped}");
    for mode in [Mode::Auto, Mode::Require] {
        let held = filtered(mode, 8, SocketProbe::Created)
            .verdict()
            .unwrap_err();
        assert!(held.contains("could still create one"), "{held}");
    }
    assert!(
        filtered(Mode::Auto, 8, SocketProbe::Denied)
            .describe()
            .contains("Unix sockets are denied to team code by a seccomp filter")
    );
    assert!(
        report(Mode::Auto, 9)
            .describe()
            .contains("Unix sockets are denied to team code by Landlock")
    );
    assert!(
        Report::not_enforced(Mode::RequireSockets, "no Landlock")
            .verdict()
            .is_err()
    );
    // The level is in every description, and read from the features of an older report.
    assert!(
        report(Mode::Auto, 8)
            .describe()
            .starts_with("enforced, filesystem only (auto)")
    );
    assert!(
        report(Mode::Auto, 9)
            .describe()
            .starts_with("enforced, filesystem and sockets (auto)")
    );
    let older = Report {
        level: String::new(),
        ..report(Mode::Auto, 9)
    };
    assert_eq!(older.level(), LEVEL_SOCKETS);
}
