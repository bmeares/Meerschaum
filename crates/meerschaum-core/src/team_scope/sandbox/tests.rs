use super::*;

fn files(dir: &Path) -> ScopeFiles {
    ScopeFiles {
        dir: dir.to_path_buf(),
        config_dir: dir.join("config"),
        config_file: dir.join("config.json"),
    }
}

struct Layout {
    _temp: tempfile::TempDir,
    root: PathBuf,
    home: PathBuf,
    plugins_dir: PathBuf,
    files: ScopeFiles,
    tmp: PathBuf,
    cache: PathBuf,
    job: PathBuf,
    work: PathBuf,
    other_job: PathBuf,
    spec_file: PathBuf,
}

/// A root with two teams' files, a home directory and a plugins directory (all synthetic).
fn layout() -> Layout {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let root = base.join("root");
    let home = base.join("home");
    let plugins_dir = root.join("plugins");
    let write = |path: PathBuf, text: &str| {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    };
    write(
        root.join("config/meerschaum.yaml"),
        "secret: every-password\n",
    );
    write(root.join("api/.grafana_admin_password"), "secret");
    write(plugins_dir.join("mine/__init__.py"), "MINE = 1\n");
    write(plugins_dir.join("theirs/__init__.py"), "THEIRS = 1\n");
    write(plugins_dir.join("single.py"), "SINGLE = 1\n");
    write(home.join(".ssh/id_ed25519"), "secret");
    let scope = root.join("team_scopes/key-a");
    let files = files(&scope);
    write(files.config_file.clone(), "{}");
    std::fs::create_dir_all(&files.config_dir).unwrap();
    write(
        root.join("team_scopes/key-b/config.json"),
        "{\"secret\": 1}",
    );
    let job = root.join("jobs/a-job");
    write(job.join("properties.json"), "{\"team_scope\": \"key-a\"}");
    let work = job.join(JOB_WORK_DIR);
    std::fs::create_dir_all(&work).unwrap();
    let other_job = root.join("jobs/b-job");
    write(
        other_job.join("properties.json"),
        "{\"team_scope\": \"key-b\"}",
    );
    write(root.join("venvs/mine/lib/site.py"), "");
    write(root.join("venvs/theirs/lib/site.py"), "");
    write(root.join(".internal/plugins/__init__.py"), "");
    let tmp = base.join("tmp-a");
    std::fs::create_dir_all(&tmp).unwrap();
    let cache = scope.join(CACHE_DIR);
    std::fs::create_dir_all(&cache).unwrap();
    ensure_canary(&root).unwrap();
    let spec_file = scope.join(SPEC_FILE);
    Layout {
        _temp: temp,
        root,
        home,
        plugins_dir,
        files,
        tmp,
        cache,
        job,
        work,
        other_job,
        spec_file,
    }
}

fn spec_of(l: &Layout, mode: Mode, plugins: &[&str]) -> Spec {
    let plugins: BTreeSet<String> = plugins.iter().map(|p| p.to_string()).collect();
    let job_dirs = team_job_dirs(&l.root.join("jobs"), "key-a", "alpha");
    build_spec(&SpecInputs {
        mode,
        team: "alpha",
        root: &l.root,
        home: Some(&l.home),
        plugins_dirs: std::slice::from_ref(&l.plugins_dir),
        venvs_dir: &l.root.join("venvs"),
        plugins: &plugins,
        plugins_view: None,
        files: &l.files,
        spec_file: &l.spec_file,
        tmp_dir: &l.tmp,
        cache_dir: &l.cache,
        job_dirs: &job_dirs,
        work_dir: Some(&l.work),
    })
}

fn rule_for<'a>(spec: &'a Spec, path: &Path) -> Option<&'a Rule> {
    spec.rules.iter().find(|r| r.path == path)
}

#[test]
fn the_modes_parse_and_unknown_values_require_the_sandbox() {
    assert_eq!(Mode::parse("auto"), Some(Mode::Auto));
    assert_eq!(Mode::parse(" REQUIRE "), Some(Mode::Require));
    assert_eq!(Mode::parse("off"), Some(Mode::Off));
    assert_eq!(Mode::parse("sometimes"), None);
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
    assert_eq!(
        rule_for(&spec, &l.root.join("venvs/mine")).map(|r| r.access),
        Some(Access::ReadWrite)
    );
    assert!(rule_for(&spec, &l.root.join("venvs/theirs")).is_none());
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
        ("/etc/ssl", Access::Read),
        ("/proc/self", Access::Read),
        ("/dev/null", Access::Device),
        ("/dev/shm", Access::SharedMemory),
    ] {
        assert_eq!(
            rule_for(&spec, Path::new(path)).map(|r| r.access),
            Some(access)
        );
    }
    for path in ["/proc", "/tmp", "/dev", "/etc", "/home", "/run"] {
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

// --- Real sandboxed processes (Linux with Landlock) ---------------------------------------------
//
// The test process never restricts itself: each case re-runs this test binary as a child (the
// `sandboxed_child` test, which does nothing unless `CHILD_VAR` is set), and the child restricts
// itself, tries every path and prints the results.

#[cfg(target_os = "linux")]
const CHILD_VAR: &str = "MRSM_TEST_SANDBOX_CHILD";

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildPlan {
    spec: Spec,
    extra: Vec<Rule>,
    /// Paths to read: (label, path).
    read: Vec<(String, PathBuf)>,
    /// Files to create and write: (label, path).
    write: Vec<(String, PathBuf)>,
    /// Pathname Unix sockets to connect to: (label, path).
    connect: Vec<(String, PathBuf)>,
    /// Abstract Unix socket names to connect to: (label, name).
    connect_abstract: Vec<(String, String)>,
    /// A process to signal (signal 0): its PID.
    signal: Option<i32>,
}

#[cfg(target_os = "linux")]
#[test]
fn sandboxed_child() {
    let Ok(plan) = std::env::var(CHILD_VAR) else {
        return;
    };
    let plan: ChildPlan = serde_json::from_str(&plan).unwrap();
    let report = enforce(&plan.spec, &plan.extra);
    let mut results = serde_json::Map::new();
    let outcome = |r: std::io::Result<()>| -> String {
        match r {
            Ok(()) => "ok".to_string(),
            Err(e) => match e.raw_os_error() {
                Some(13) | Some(1) => "denied".to_string(),
                _ => format!("error: {e}"),
            },
        }
    };
    for (label, path) in &plan.read {
        let r = if path.is_dir() {
            std::fs::read_dir(path).map(|_| ())
        } else {
            std::fs::read(path).map(|_| ())
        };
        results.insert(label.clone(), outcome(r).into());
    }
    for (label, path) in &plan.write {
        results.insert(label.clone(), outcome(std::fs::write(path, b"x")).into());
    }
    for (label, path) in &plan.connect {
        let r = std::os::unix::net::UnixStream::connect(path).map(|_| ());
        results.insert(label.clone(), outcome(r).into());
    }
    for (label, name) in &plan.connect_abstract {
        use std::os::linux::net::SocketAddrExt;
        let r = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())
            .and_then(|addr| std::os::unix::net::UnixStream::connect_addr(&addr))
            .map(|_| ());
        results.insert(label.clone(), outcome(r).into());
    }
    if let Some(pid) = plan.signal {
        let r = rustix::process::Pid::from_raw(pid)
            .ok_or_else(|| std::io::Error::other("pid"))
            .and_then(|pid| rustix::process::test_kill_process(pid).map_err(Into::into));
        results.insert("signal".into(), outcome(r).into());
    }
    let r = std::fs::read_to_string("/proc/self/status").map(|_| ());
    results.insert("own /proc".into(), outcome(r).into());
    println!(
        "SANDBOX-RESULT {}",
        serde_json::json!({"report": report, "results": results})
    );
}

/// Run `plan` in a sandboxed child; `None` (with the reason printed) where Landlock is
/// unavailable.
#[cfg(target_os = "linux")]
fn run_child(plan: &ChildPlan) -> Option<(Report, serde_json::Map<String, serde_json::Value>)> {
    if let Support::Unsupported { reason } = kernel_support() {
        eprintln!("skipped: this system cannot run Landlock ({reason})");
        return None;
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "team_scope::sandbox::tests::sandboxed_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_VAR, serde_json::to_string(plan).unwrap())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = stdout
        .lines()
        .find_map(|l| l.split_once("SANDBOX-RESULT ").map(|(_, json)| json))
        .unwrap_or_else(|| {
            panic!(
                "no result from the child:\n{stdout}\n{}",
                String::from_utf8_lossy(&out.stderr)
            )
        });
    let value: serde_json::Value = serde_json::from_str(line).unwrap();
    let report: Report = serde_json::from_value(value["report"].clone()).unwrap();
    let results = value["results"].as_object().unwrap().clone();
    Some((report, results))
}

#[cfg(target_os = "linux")]
#[test]
fn a_sandboxed_process_reaches_its_own_paths_and_nothing_else() {
    use std::os::linux::net::SocketAddrExt;
    let l = layout();
    let spec = spec_of(&l, Mode::Require, &["mine", "single"]);
    // A pathname socket and an abstract socket held by this (unsandboxed) process.
    let sockets = tempfile::tempdir().unwrap();
    let outside = sockets.path().join("outside.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&outside).unwrap();
    let inside = l.tmp.join("inside.sock");
    let _inside = std::os::unix::net::UnixListener::bind(&inside).unwrap();
    let name = format!("mrsm-test-sandbox-{}", std::process::id());
    let addr = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
    let _abstract = std::os::unix::net::UnixListener::bind_addr(&addr).unwrap();
    let s = |v: &str| v.to_string();
    let plan = ChildPlan {
        spec,
        extra: Vec::new(),
        read: vec![
            (s("server config"), l.root.join("config/meerschaum.yaml")),
            (
                s("grafana password"),
                l.root.join("api/.grafana_admin_password"),
            ),
            (
                s("other team's config"),
                l.root.join("team_scopes/key-b/config.json"),
            ),
            (s("other team's job"), l.other_job.join("properties.json")),
            (s("other plugin"), l.plugins_dir.join("theirs/__init__.py")),
            (s("other venv"), l.root.join("venvs/theirs/lib/site.py")),
            (s("ssh key"), l.home.join(".ssh/id_ed25519")),
            (s("home listing"), l.home.clone()),
            (s("root listing"), l.root.clone()),
            (
                s("parent environ"),
                PathBuf::from(format!("/proc/{}/environ", std::process::id())),
            ),
            (s("canary"), canary_path(&l.root)),
            (s("own config"), l.files.config_file.clone()),
            (s("own plugin"), l.plugins_dir.join("mine/__init__.py")),
            (s("own single-file plugin"), l.plugins_dir.join("single.py")),
            (s("own job"), l.job.join("properties.json")),
            (s("own venv"), l.root.join("venvs/mine/lib/site.py")),
            (
                s("plugins package"),
                l.root.join(".internal/plugins/__init__.py"),
            ),
            (s("user names"), PathBuf::from("/etc/passwd")),
            (s("cpuinfo"), PathBuf::from("/proc/cpuinfo")),
        ],
        write: vec![
            (s("write root"), l.root.join("planted.txt")),
            (s("write own job properties"), l.job.join("properties.json")),
            (
                s("write other venv"),
                l.root.join("venvs/theirs/lib/planted.py"),
            ),
            (
                s("write own config dir"),
                l.files.config_dir.join("planted.yaml"),
            ),
            (s("write tmp"), l.tmp.join("scratch.txt")),
            (s("write work"), l.work.join(".result.json")),
            (s("write cache"), l.cache.join("x")),
            (
                s("write own venv"),
                l.root.join("venvs/mine/lib/installed.py"),
            ),
            (s("write /dev/null"), PathBuf::from("/dev/null")),
        ],
        connect: vec![
            (s("outside socket"), outside.clone()),
            (s("inside socket"), inside.clone()),
        ],
        connect_abstract: vec![(s("abstract socket"), name.clone())],
        signal: Some(std::process::id() as i32),
    };
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert!(report.enforced, "{report:?}");
    assert_eq!(report.canary, Canary::Denied, "{report:?}");
    assert!(report.verdict().is_ok());
    let support = kernel_support();
    let get = |label: &str| results[label].as_str().unwrap().to_string();
    for label in [
        "server config",
        "grafana password",
        "other team's config",
        "other team's job",
        "other plugin",
        "other venv",
        "ssh key",
        "home listing",
        "root listing",
        "parent environ",
        "canary",
        "write root",
        "write own job properties",
        "write other venv",
        "write own config dir",
    ] {
        assert_eq!(get(label), "denied", "{label}: {results:?}");
    }
    for label in [
        "own config",
        "own plugin",
        "own single-file plugin",
        "own job",
        "own venv",
        "plugins package",
        "user names",
        "cpuinfo",
        "write tmp",
        "write work",
        "write cache",
        "write own venv",
        "write /dev/null",
        "own /proc",
        "inside socket",
    ] {
        assert_eq!(get(label), "ok", "{label}: {results:?}");
    }
    if support.abi() >= 6 {
        assert_eq!(get("signal"), "denied", "{results:?}");
        assert_eq!(get("abstract socket"), "denied", "{results:?}");
    } else {
        eprintln!(
            "ABI {}: signal and abstract socket scoping not checked",
            support.abi()
        );
    }
    if support.abi() >= 9 {
        assert_eq!(get("outside socket"), "denied", "{results:?}");
        assert!(report.features.contains(&"resolve_unix".to_string()));
    } else {
        eprintln!(
            "ABI {}: connecting to pathname Unix sockets is not restricted",
            support.abi()
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn an_interpreter_path_covering_the_root_is_dropped_in_the_child() {
    let l = layout();
    let spec = spec_of(&l, Mode::Require, &["mine"]);
    let plan = ChildPlan {
        spec,
        extra: vec![Rule::new(
            l.root.parent().unwrap(),
            Access::ReadExec,
            "a PYTHONPATH holding the root",
        )],
        read: vec![(
            "server config".into(),
            l.root.join("config/meerschaum.yaml"),
        )],
        write: Vec::new(),
        connect: Vec::new(),
        connect_abstract: Vec::new(),
        signal: None,
    };
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert_eq!(results["server config"], "denied");
    assert_eq!(report.dropped.len(), 1, "{report:?}");
}

#[cfg(target_os = "linux")]
#[test]
fn the_kernel_support_probe_restricts_nothing() {
    let support = kernel_support();
    eprintln!("this system: {}", support.describe());
    // This process can still read what a sandbox would deny.
    let l = layout();
    assert!(std::fs::read(l.root.join("config/meerschaum.yaml")).is_ok());
    assert!(current().is_none());
}
