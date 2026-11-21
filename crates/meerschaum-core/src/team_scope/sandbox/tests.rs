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
    /// The team's own virtual environments.
    venvs: PathBuf,
}

/// A root with two teams' files, a home directory and a plugins directory (all synthetic).
fn layout() -> Layout {
    // These fixtures model a native Rust host and inspect specs; only the dedicated CLI
    // acceptance fixtures execute the registered sandbox protocol in an actual mrsm binary.
    let _ = crate::self_exe::register_native_current_executable();
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
    let venvs = scope.join(VENVS_DIR);
    write(venvs.join("mine/lib/site.py"), "");
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
        venvs,
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
        team_venvs_dir: &l.venvs,
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

// --- Real sandboxed processes (Linux with Landlock) ---------------------------------------------

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[test]
#[allow(unsafe_code)]
fn api_inbound_denial_keeps_outbound_callbacks_and_worker_pairs() {
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    const CHILD: &str = "MRSM_TEST_API_INBOUND_CHILD";
    const TEST: &str =
        "team_scope::sandbox::tests::api_inbound_denial_keeps_outbound_callbacks_and_worker_pairs";
    if let Ok(port) = std::env::var(CHILD) {
        assert_eq!(
            // SAFETY: `PR_SET_NO_NEW_PRIVS` takes integer arguments only and affects only this
            // dedicated test subprocess, which a sandbox may restrict.
            unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) },
            0
        );
        imp::deny_inbound_network().unwrap();
        assert_eq!(
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EPERM)
        );
        // Linux listen() can implicitly bind a socket, so that call is independently denied.
        // SAFETY: socket returns a uniquely owned descriptor, immediately guarded.
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
        assert!(fd >= 0);
        // SAFETY: fd is the new unique descriptor returned above.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        // SAFETY: listen uses a valid owned descriptor and no pointers.
        assert_eq!(unsafe { libc::listen(fd.as_raw_fd(), 1) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EPERM)
        );
        let (mut left, mut right) = std::os::unix::net::UnixStream::pair().unwrap();
        left.write_all(b"pair").unwrap();
        let mut answer = [0; 4];
        right.read_exact(&mut answer).unwrap();
        assert_eq!(&answer, b"pair");
        std::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .unwrap()
            .write_all(b"callback")
            .unwrap();
        return;
    }
    assert!(
        deny_inbound_network().is_err(),
        "Unconfined callers cannot claim the listener boundary."
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
        .env(CHILD, listener.local_addr().unwrap().port().to_string())
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    listener.set_nonblocking(true).unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    let mut message = [0; 8];
    stream.read_exact(&mut message).unwrap();
    assert_eq!(&message, b"callback");
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[test]
#[allow(unsafe_code)]
fn metadata_network_denial_preserves_only_connected_worker_pairs_and_is_inherited() {
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::net::{UnixDatagram, UnixStream};
    const CHILD: &str = "MRSM_TEST_METADATA_NETWORK_CHILD";
    const TEST: &str = "team_scope::sandbox::tests::metadata_network_denial_preserves_only_connected_worker_pairs_and_is_inherited";
    let stage = std::env::var(CHILD).ok();
    if stage.is_none() {
        assert!(current().is_none());
        assert!(
            deny_network().is_err(),
            "An unconfined caller cannot report network proof."
        );
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
            .env(CHILD, "install")
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let denied = |error: std::io::Error| assert_eq!(error.raw_os_error(), Some(libc::EPERM));
    if stage.as_deref() == Some("install") {
        // SAFETY: socket returns a new descriptor, immediately placed in an owning guard.
        let raw = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
        assert!(raw >= 0);
        // SAFETY: raw is the unique new descriptor from socket above.
        let existing = unsafe { OwnedFd::from_raw_fd(raw) };
        assert_eq!(
            // SAFETY: `PR_SET_NO_NEW_PRIVS` takes integer arguments only and affects only this
            // dedicated test subprocess, which a sandbox may restrict.
            unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) },
            0
        );
        imp::deny_network().unwrap();
        let address = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: 1u16.to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes([127, 0, 0, 1]),
            },
            sin_zero: [0; 8],
        };
        // SAFETY: address is an initialized sockaddr_in and the supplied length matches it.
        let connected = unsafe {
            libc::connect(
                existing.as_raw_fd(),
                (&address as *const libc::sockaddr_in).cast(),
                std::mem::size_of_val(&address) as libc::socklen_t,
            )
        };
        assert_eq!(connected, -1);
        denied(std::io::Error::last_os_error());
    }
    denied(std::net::TcpListener::bind("127.0.0.1:0").unwrap_err());
    denied(std::net::UdpSocket::bind("[::1]:0").unwrap_err());
    denied(UnixDatagram::unbound().unwrap_err());
    denied(UnixDatagram::pair().unwrap_err());
    let (mut left, mut right) = UnixStream::pair().unwrap();
    left.write_all(b"worker").unwrap();
    let mut message = [0; 6];
    right.read_exact(&mut message).unwrap();
    assert_eq!(&message, b"worker");
    if stage.as_deref() == Some("install") {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
            .env(CHILD, "inherited")
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
//
// The test process never restricts itself: each case re-runs this test binary as a child (the
// `sandboxed_child` test, which does nothing unless `CHILD_VAR` is set), and the child restricts
// itself, tries every path and prints the results.

#[cfg(target_os = "linux")]
const CHILD_VAR: &str = "MRSM_TEST_SANDBOX_CHILD";

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// FIFOs to open for reading and writing without blocking (a job's stdin): (label, path).
    #[serde(default)]
    fifo: Vec<(String, PathBuf)>,
    /// 5.1, the Unix-socket filter: programs to run (label, argv), their exit status recorded.
    #[serde(default)]
    run: Vec<(String, Vec<String>)>,
    /// Create stream, seqpacket and datagram socket pairs.
    #[serde(default)]
    socketpairs: bool,
    /// A connected socket inherited from the parent (its descriptor): send `ping`, expect `pong`.
    #[serde(default)]
    inherited: Option<i32>,
    /// Ask for a Unix socket through the x32 numbers and set up an io_uring.
    #[serde(default)]
    raw_syscalls: bool,
    /// Ask for a Unix socket through the 32-bit `int 0x80` entry (the process is killed).
    #[serde(default)]
    int80: bool,
    /// Time round trips over the inherited channel, then (with the filter) 30 more installs.
    #[serde(default)]
    bench: bool,
}

#[cfg(target_os = "linux")]
#[test]
#[allow(unsafe_code)]
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
                // EAFNOSUPPORT: the seccomp filter refused to create a Unix socket.
                Some(13) | Some(1) | Some(97) => "denied".to_string(),
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
        let r = std::os::unix::net::UnixStream::connect(path);
        if label == "docker"
            && let Ok(stream) = &r
        {
            // Read-only: Docker's health route.
            use std::io::{Read, Write};
            let mut stream = stream;
            let _ = stream.write_all(b"GET /_ping HTTP/1.0\r\nHost: docker\r\n\r\n");
            let mut answer = String::new();
            let _ = stream.read_to_string(&mut answer);
            results.insert(
                "docker ping".into(),
                answer.lines().last().unwrap_or_default().into(),
            );
        }
        results.insert(label.clone(), outcome(r.map(|_| ())).into());
    }
    for (label, name) in &plan.connect_abstract {
        use std::os::linux::net::SocketAddrExt;
        let r = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes())
            .and_then(|addr| std::os::unix::net::UnixStream::connect_addr(&addr))
            .map(|_| ());
        results.insert(label.clone(), outcome(r).into());
    }
    for (label, path) in &plan.fifo {
        use std::os::unix::fs::OpenOptionsExt;
        let r = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
            .map(|_| ());
        results.insert(label.clone(), outcome(r).into());
    }
    if let Some(pid) = plan.signal {
        let r = rustix::process::Pid::from_raw(pid)
            .ok_or_else(|| std::io::Error::other("pid"))
            .and_then(|pid| rustix::process::test_kill_process(pid).map_err(Into::into));
        results.insert("signal".into(), outcome(r).into());
    }
    for (label, argv) in &plan.run {
        let r = std::process::Command::new(&argv[0])
            .args(&argv[1..])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output();
        let text = match r {
            Ok(out) if out.status.success() && label == "extra command" => {
                format!("ok: {}", String::from_utf8_lossy(&out.stdout).trim())
            }
            Ok(out) if out.status.success() => "ok".to_string(),
            Ok(out) => format!(
                "failed: {}",
                String::from_utf8_lossy(&out.stderr)
                    .trim()
                    .replace('\n', " ")
            ),
            Err(e) => format!("error: {e}"),
        };
        results.insert(label.clone(), text.into());
    }
    if plan.socketpairs {
        use std::os::unix::net::{UnixDatagram, UnixStream};
        results.insert(
            "stream pair".into(),
            outcome(UnixStream::pair().map(|_| ())).into(),
        );
        results.insert(
            "datagram pair".into(),
            outcome(UnixDatagram::pair().map(|_| ())).into(),
        );
        let mut fds = [-1i32; 2];
        // SAFETY: `socketpair` writes two descriptors into `fds` (leaked in this short-lived
        // child).
        let rc = unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
                0,
                fds.as_mut_ptr(),
            )
        };
        let seqpacket = if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        };
        results.insert("seqpacket pair".into(), outcome(seqpacket).into());
    }
    if let (true, Some(fd)) = (plan.bench, plan.inherited) {
        use std::io::{Read, Write};
        use std::os::fd::FromRawFd;
        // SAFETY: as below: the parent passed this descriptor for this purpose.
        let mut stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
        let mut batches = Vec::new();
        let mut buf = [0u8; 64];
        for _ in 0..5 {
            let begin = std::time::Instant::now();
            for _ in 0..20_000 {
                stream.write_all(&buf).unwrap();
                stream.read_exact(&mut buf).unwrap();
            }
            batches.push(begin.elapsed().as_nanos() as f64 / 20_000.0);
        }
        batches.sort_by(f64::total_cmp);
        results.insert("round trip ns".into(), format!("{:.0}", batches[2]).into());
        if report.unix_sockets == SOCKETS_BY_SECCOMP {
            let mut installs = Vec::new();
            for _ in 0..30 {
                let begin = std::time::Instant::now();
                let (sockets, uring) = imp::unix_socket_programs().unwrap();
                seccompiler::apply_filter_all_threads(&uring).unwrap();
                seccompiler::apply_filter_all_threads(&sockets).unwrap();
                installs.push(begin.elapsed().as_nanos() as f64 / 1000.0);
            }
            installs.sort_by(f64::total_cmp);
            results.insert(
                "filter install us".into(),
                format!(
                    "median {:.0}, min {:.0}, max {:.0}",
                    installs[15], installs[0], installs[29]
                )
                .into(),
            );
        }
    } else if let Some(fd) = plan.inherited {
        use std::io::{Read, Write};
        use std::os::fd::FromRawFd;
        // SAFETY: the parent passed this descriptor (one end of a socket pair, not close-on-
        // exec) for this purpose; nothing else in the child owns it.
        let mut stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
        let mut answer = [0u8; 4];
        let r = stream
            .write_all(b"ping")
            .and_then(|()| stream.read_exact(&mut answer))
            .and_then(|()| {
                (&answer == b"pong")
                    .then_some(())
                    .ok_or_else(|| std::io::Error::other("no pong"))
            });
        results.insert("inherited channel".into(), outcome(r).into());
    }
    if plan.raw_syscalls {
        let errno = |r: libc::c_long| {
            if r >= 0 {
                "ok".to_string()
            } else {
                match std::io::Error::last_os_error().raw_os_error() {
                    Some(97) => "denied (EAFNOSUPPORT)".to_string(),
                    Some(1) => "denied (EPERM)".to_string(),
                    Some(38) => "ENOSYS".to_string(),
                    other => format!("errno {other:?}"),
                }
            }
        };
        #[cfg(target_arch = "x86_64")]
        {
            // SAFETY: `socket(AF_UNIX, SOCK_STREAM, 0)` through its x32 number takes no pointer;
            // a descriptor it might return is leaked in this short-lived child.
            let r = unsafe {
                libc::syscall(
                    0x4000_0000 | libc::SYS_socket,
                    libc::AF_UNIX,
                    libc::SOCK_STREAM,
                    0,
                )
            };
            results.insert("x32 socket".into(), errno(r).into());
        }
        let mut params = [0u8; 120];
        // SAFETY: `io_uring_setup(1, &params)` reads and writes the 120-byte, zeroed
        // `struct io_uring_params` this buffer holds; a ring it might return is leaked here.
        let r = unsafe { libc::syscall(libc::SYS_io_uring_setup, 1u32, params.as_mut_ptr()) };
        results.insert("io_uring_setup".into(), errno(r).into());
    }
    #[cfg(target_arch = "x86_64")]
    if plan.int80 {
        println!("SANDBOX-RESULT-INT80 before");
        let ret: i64;
        // SAFETY: the i386 entry with `socket` (359): AF_UNIX, SOCK_STREAM, 0 in registers, no
        // memory touched. The filter's architecture check kills this process here.
        unsafe {
            std::arch::asm!(
                "xchg {b}, rbx",
                "int 0x80",
                "xchg {b}, rbx",
                b = inout(reg) libc::AF_UNIX as i64 => _,
                inlateout("rax") 359i64 => ret,
                in("rcx") libc::SOCK_STREAM as i64,
                in("rdx") 0i64,
            );
        }
        results.insert("int80 socket".into(), format!("returned {ret}").into());
    }
    let r = std::fs::read_to_string("/proc/self/status").map(|_| ());
    results.insert("own /proc".into(), outcome(r).into());
    println!(
        "SANDBOX-RESULT {}",
        serde_json::json!({"report": report, "results": results})
    );
}

/// Run `plan` in a child (the test binary again, which applies the sandbox there: never in
/// this process).
#[cfg(target_os = "linux")]
fn child_output(plan: &ChildPlan) -> std::process::Output {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "team_scope::sandbox::tests::sandboxed_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_VAR, serde_json::to_string(plan).unwrap())
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output()
        .unwrap()
}

/// Run `plan` in a sandboxed child; `None` (with the reason printed) where Landlock is
/// unavailable.
#[cfg(target_os = "linux")]
fn run_child(plan: &ChildPlan) -> Option<(Report, serde_json::Map<String, serde_json::Value>)> {
    if let Support::Unsupported { reason } = kernel_support() {
        eprintln!("skipped: this system cannot run Landlock ({reason})");
        return None;
    }
    let out = child_output(plan);
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
fn public_certificate_stores_do_not_expose_sibling_private_keys() {
    let l = layout();
    let system = l._temp.path().join("system");
    let public = [
        "ssl/certs/ca.pem",
        "ssl/cert.pem",
        "pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
        "pki/ca-trust/source/anchors/local-ca.pem",
        "pki/tls/certs/ca-bundle.crt",
        "pki/tls/cert.pem",
    ];
    let private = [
        "ssl/private/service.key",
        "pki/tls/private/service.key",
        "pki/nssdb/key4.db",
        "pki/ca-trust/private/service.key",
    ];
    for path in public.iter().chain(private.iter()) {
        let path = system.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "certificate or private canary").unwrap();
    }
    let mut spec = spec_of(&l, Mode::Require, &[]);
    // Exercise the actual system allowlist against synthetic readable certificate
    // and key trees, without writing into the host's /etc or reading real keys.
    for rule in &mut spec.rules {
        if let Ok(relative) = rule.path.strip_prefix("/etc") {
            rule.path = system.join(relative);
        }
    }
    let plan = ChildPlan {
        spec,
        read: public
            .iter()
            .chain(private.iter())
            .map(|path| (path.to_string(), system.join(path)))
            .collect(),
        ..Default::default()
    };
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert!(report.verdict().is_ok(), "{report:?}");
    for path in public {
        assert_eq!(results[path], "ok", "{path}: {results:?}");
    }
    for path in private {
        assert_eq!(results[path], "denied", "{path}: {results:?}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn a_sandboxed_process_reaches_its_own_paths_and_nothing_else() {
    // At the kernel's level, then (5.1) forced to "filesystem only", where the seccomp filter
    // denies Unix sockets: the acceptance list holds under the filter too.
    for max_abi in [None, Some(8)] {
        reaches_its_own_paths_and_nothing_else(max_abi);
    }
}

#[cfg(target_os = "linux")]
fn reaches_its_own_paths_and_nothing_else(max_abi: Option<u32>) {
    use std::os::linux::net::SocketAddrExt;
    let l = layout();
    let mut spec = spec_of(&l, Mode::Require, &["mine", "single"]);
    spec.max_abi = max_abi;
    // A pathname socket and an abstract socket held by this (unsandboxed) process.
    let sockets = tempfile::tempdir().unwrap();
    let outside = sockets.path().join("outside.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&outside).unwrap();
    let inside = l.tmp.join("inside.sock");
    let _inside = std::os::unix::net::UnixListener::bind(&inside).unwrap();
    let name = format!(
        "mrsm-test-sandbox-{}-{}",
        std::process::id(),
        max_abi.unwrap_or(0)
    );
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
            (s("own venv"), l.venvs.join("mine/lib/site.py")),
            (
                s("server's venv of its plugin"),
                l.root.join("venvs/mine/lib/site.py"),
            ),
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
            (s("write own venv"), l.venvs.join("mine/lib/installed.py")),
            (
                s("write server's venv of its plugin"),
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
        fifo: Vec::new(),
        ..ChildPlan::default()
    };
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert!(report.enforced, "{report:?}");
    assert_eq!(report.canary, Canary::Denied, "{report:?}");
    assert!(report.verdict().is_ok());
    let support = kernel_support();
    let abi = max_abi.unwrap_or(NEWEST_ABI).min(support.abi());
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
        "server's venv of its plugin",
        "write server's venv of its plugin",
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
    ] {
        assert_eq!(get(label), "ok", "{label}: {results:?}");
    }
    if abi >= 6 {
        assert_eq!(get("signal"), "denied", "{results:?}");
        assert_eq!(get("abstract socket"), "denied", "{results:?}");
    } else {
        eprintln!(
            "ABI {}: signal and abstract socket scoping not checked",
            support.abi()
        );
    }
    // Unix sockets: Landlock denies those outside the rules (the team's own still work); the
    // seccomp filter, below ABI 9, refuses to create any.
    assert_eq!(get("outside socket"), "denied", "ABI {abi}: {results:?}");
    assert!(report.denies_unix_sockets(), "{report:?}");
    if abi >= SOCKETS_ABI {
        assert_eq!(report.unix_sockets, SOCKETS_BY_LANDLOCK, "{report:?}");
        assert_eq!(get("inside socket"), "ok", "{results:?}");
        assert!(report.features.contains(&"resolve_unix".to_string()));
    } else {
        assert_eq!(report.unix_sockets, SOCKETS_BY_SECCOMP, "{report:?}");
        assert_eq!(report.socket_probe, SocketProbe::Denied, "{report:?}");
        assert_eq!(get("inside socket"), "denied", "{results:?}");
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
        fifo: Vec::new(),
        ..ChildPlan::default()
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

/// 5.1: a team's process may write nothing that a process outside the team later executes,
/// imports or loads: not the virtual environment of a plugin it was granted (which the server,
/// the open side and other teams granted the same plugin import), the plugins directory, the
/// bytecode next to shared source, the plugins package's links or the lock files the server's
/// processes take.
#[cfg(target_os = "linux")]
#[test]
fn a_team_process_writes_nothing_that_others_load() {
    let l = layout();
    let spec = spec_of(&l, Mode::Require, &["mine", "single"]);
    std::fs::create_dir_all(l.root.join(".cache/venvs")).unwrap();
    std::fs::write(l.root.join(".cache/venvs/mine.mutation.lock"), "").unwrap();
    std::fs::create_dir_all(l.plugins_dir.join("mine/__pycache__")).unwrap();
    let s = |v: &str| v.to_string();
    let plan = ChildPlan {
        spec,
        extra: Vec::new(),
        read: Vec::new(),
        write: vec![
            (
                s("shared venv of a granted plugin"),
                l.root.join("venvs/mine/lib/planted.py"),
            ),
            (
                s("plugins directory"),
                l.plugins_dir.join("mine/planted.py"),
            ),
            (
                s("bytecode next to shared source"),
                l.plugins_dir
                    .join("mine/__pycache__/__init__.cpython-314.pyc"),
            ),
            (
                s("plugins package links"),
                l.root.join(".internal/plugins/planted.py"),
            ),
            (
                s("a lock the server takes"),
                l.root.join(".cache/venvs/mine.mutation.lock"),
            ),
        ],
        connect: Vec::new(),
        connect_abstract: Vec::new(),
        signal: None,
        fifo: Vec::new(),
        ..ChildPlan::default()
    };
    let mut plan = plan;
    plan.write
        .push((s("its own venv"), l.venvs.join("mine/lib/installed.py")));
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert!(report.enforced, "{report:?}");
    for (label, _) in &plan.write {
        let expected = if label == "its own venv" {
            "ok"
        } else {
            "denied"
        };
        assert_eq!(results[label.as_str()], expected, "{label}: {results:?}");
    }
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

/// 5.1: a job's stdin is a FIFO in its directory, written by the server (`mrsm attach`, the
/// console's prompt answers) and read by the job. Opening a FIFO is opening a path, which the
/// filesystem rules (Landlock ABI 1) restrict on every kernel: a team's process cannot write to
/// another job's stdin, nor to its own job's (its job directory is read-only to it).
#[cfg(target_os = "linux")]
#[test]
fn a_team_process_cannot_write_to_a_jobs_stdin() {
    let l = layout();
    let spec = spec_of(&l, Mode::Require, &["mine"]);
    let other = l.other_job.join("b-job.stdin");
    let own = l.job.join("a-job.stdin");
    for fifo in [&other, &own] {
        nix_mkfifo(fifo);
    }
    let plan = ChildPlan {
        spec,
        extra: Vec::new(),
        read: Vec::new(),
        write: Vec::new(),
        connect: Vec::new(),
        connect_abstract: Vec::new(),
        signal: None,
        fifo: vec![
            ("another job's stdin".into(), other.clone()),
            ("its own job's stdin".into(), own.clone()),
        ],
        ..ChildPlan::default()
    };
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    assert!(report.enforced, "{report:?}");
    for label in ["another job's stdin", "its own job's stdin"] {
        assert_eq!(results[label], "denied", "{label}: {results:?}");
    }
}

#[cfg(target_os = "linux")]
fn nix_mkfifo(path: &Path) {
    let status = std::process::Command::new("mkfifo")
        .args(["-m", "600"])
        .arg(path)
        .status()
        .unwrap();
    assert!(status.success(), "mkfifo {}", path.display());
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

/// 5.1: what each level allows, on this kernel, by running a process at an older kernel's
/// Landlock ABI: at "filesystem only" a team's process connects to a pathname Unix socket
/// outside its rules (and before ABI 6 signals another process); at "filesystem and sockets" it
/// does neither. The filesystem rules (a job's stdin FIFO included) hold at every level.
#[cfg(target_os = "linux")]
#[test]
fn unix_sockets_are_denied_at_every_level_by_landlock_or_the_filter() {
    let l = layout();
    let sockets = tempfile::tempdir().unwrap();
    let outside = sockets.path().join("outside.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&outside).unwrap();
    let fifo = l.other_job.join("b-job.stdin");
    nix_mkfifo(&fifo);
    let support = kernel_support();
    for max_abi in [Some(5), Some(8), None] {
        let mut spec = spec_of(&l, Mode::Auto, &["mine"]);
        spec.max_abi = max_abi;
        let plan = ChildPlan {
            spec,
            extra: Vec::new(),
            read: vec![(
                "server config".into(),
                l.root.join("config/meerschaum.yaml"),
            )],
            write: Vec::new(),
            connect: vec![("outside socket".into(), outside.clone())],
            connect_abstract: Vec::new(),
            signal: Some(std::process::id() as i32),
            fifo: vec![("another job's stdin".into(), fifo.clone())],
            ..ChildPlan::default()
        };
        let Some((report, results)) = run_child(&plan) else {
            return;
        };
        let abi = max_abi.unwrap_or(NEWEST_ABI).min(support.abi());
        assert!(report.enforced, "{report:?}");
        assert_eq!(report.abi, abi, "{report:?}");
        assert_eq!(report.level(), level_of(abi), "{report:?}");
        assert_eq!(results["server config"], "denied", "ABI {abi}: {results:?}");
        assert_eq!(
            results["another job's stdin"], "denied",
            "ABI {abi}: {results:?}"
        );
        // 5.1: below ABI 9 the seccomp filter refuses the socket; from ABI 9 Landlock refuses
        // the connection.
        assert_eq!(
            results["outside socket"], "denied",
            "ABI {abi}: {results:?}"
        );
        assert!(report.denies_unix_sockets(), "{report:?}");
        assert!(report.verdict().is_ok(), "{report:?}");
        let expected = if abi >= SOCKETS_ABI {
            SOCKETS_BY_LANDLOCK
        } else {
            SOCKETS_BY_SECCOMP
        };
        assert_eq!(report.unix_sockets, expected, "{report:?}");
        let signal = results["signal"].as_str().unwrap();
        assert_eq!(signal, if abi >= 6 { "denied" } else { "ok" }, "ABI {abi}");
        // `require_sockets` wants the signal scope too (ABI 6).
        let strict = Report {
            mode: Mode::RequireSockets,
            ..report.clone()
        };
        assert_eq!(strict.verdict().is_ok(), abi >= 6, "ABI {abi}: {strict:?}");
    }
}

/// A private tmux server on a scratch socket (`-S`, never the default one), killed on drop.
#[cfg(target_os = "linux")]
struct ScratchTmux(PathBuf);

#[cfg(target_os = "linux")]
impl ScratchTmux {
    fn start(socket: &Path) -> Option<ScratchTmux> {
        let ok = std::process::Command::new("tmux")
            .arg("-S")
            .arg(socket)
            .args([
                "-f",
                "/dev/null",
                "new-session",
                "-d",
                "-s",
                "scratch",
                "sleep 600",
            ])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .status()
            .is_ok_and(|s| s.success());
        ok.then(|| ScratchTmux(socket.to_path_buf()))
    }
}

#[cfg(target_os = "linux")]
impl Drop for ScratchTmux {
    fn drop(&mut self) {
        let _ = std::process::Command::new("tmux")
            .arg("-S")
            .arg(&self.0)
            .arg("kill-server")
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .status();
    }
}

/// 5.1: forced to "filesystem only" (Landlock ABI 8), a team process cannot create a Unix
/// socket: not to a scratch tmux server (whose `run-shell` would run commands outside the
/// sandbox), the session bus or Docker's socket (attempted only), nor through the x32 numbers;
/// io_uring cannot be set up; the stream and seqpacket socket pairs asyncio and
/// `multiprocessing` use still work, a datagram pair does not, and a connected socket inherited
/// from the parent (the worker's channel) keeps working.
#[cfg(target_os = "linux")]
#[test]
#[allow(unsafe_code)]
fn the_filter_denies_unix_sockets_and_keeps_pairs_and_inherited_channels() {
    use std::io::{Read, Write};
    let support = kernel_support();
    if !support.available() {
        eprintln!("skipped: no Landlock here");
        return;
    }
    let l = layout();
    let tmux_dir = tempfile::Builder::new()
        .prefix("mt")
        .tempdir_in("/tmp")
        .unwrap();
    let tmux_socket = tmux_dir.path().join("s");
    let tmux = ScratchTmux::start(&tmux_socket);
    let planted = l.tmp.join("planted-by-tmux");
    let uid = rustix::process::getuid().as_raw();
    let mut connect = Vec::new();
    for (label, path) in [
        ("tmux", tmux_socket.clone()),
        ("session bus", PathBuf::from(format!("/run/user/{uid}/bus"))),
        ("docker", PathBuf::from("/var/run/docker.sock")),
    ] {
        if path.exists() {
            connect.push((label.to_string(), path));
        }
    }
    let mut run = Vec::new();
    if tmux.is_some() {
        run.push((
            "tmux run-shell".to_string(),
            vec![
                "tmux".to_string(),
                "-S".to_string(),
                tmux_socket.display().to_string(),
                "run-shell".to_string(),
                format!("touch '{}'", planted.display()),
            ],
        ));
    }
    let (parent_end, child_end) = std::os::unix::net::UnixStream::pair().unwrap();
    use std::os::fd::AsRawFd;
    let child_fd = child_end.as_raw_fd();
    // SAFETY: clears close-on-exec on a descriptor this test owns, so the child inherits it.
    assert_eq!(unsafe { libc::fcntl(child_fd, libc::F_SETFD, 0) }, 0);
    let answerer = std::thread::spawn(move || {
        let mut parent_end = parent_end;
        let mut ping = [0u8; 4];
        if parent_end.read_exact(&mut ping).is_ok() && &ping == b"ping" {
            let _ = parent_end.write_all(b"pong");
        }
    });
    let mut spec = spec_of(&l, Mode::Auto, &[]);
    spec.max_abi = Some(8);
    let plan = ChildPlan {
        spec,
        connect,
        run,
        socketpairs: true,
        inherited: Some(child_fd),
        raw_syscalls: true,
        ..ChildPlan::default()
    };
    let (report, results) = run_child(&plan).unwrap();
    drop(child_end);
    let _ = answerer.join();
    assert_eq!(report.unix_sockets, SOCKETS_BY_SECCOMP, "{report:?}");
    assert_eq!(report.socket_probe, SocketProbe::Denied, "{report:?}");
    let get = |label: &str| results[label].as_str().unwrap().to_string();
    for (label, _) in &plan.connect {
        assert_eq!(get(label), "denied", "{label}: {results:?}");
    }
    if tmux.is_some() {
        assert!(get("tmux run-shell").starts_with("failed"), "{results:?}");
        assert!(
            !planted.exists(),
            "tmux ran a command for the sandboxed process"
        );
    } else {
        eprintln!("no tmux here: its socket is not checked");
    }
    assert_eq!(get("stream pair"), "ok", "{results:?}");
    assert_eq!(get("seqpacket pair"), "ok", "{results:?}");
    assert_eq!(get("datagram pair"), "denied", "{results:?}");
    assert_eq!(get("inherited channel"), "ok", "{results:?}");
    #[cfg(target_arch = "x86_64")]
    assert_eq!(get("x32 socket"), "denied (EAFNOSUPPORT)", "{results:?}");
    assert_eq!(get("io_uring_setup"), "denied (EPERM)", "{results:?}");
    // Without the filter, the same scratch tmux server does run the command (why it matters).
    if tmux.is_some() {
        let out = std::process::Command::new("tmux")
            .arg("-S")
            .arg(&tmux_socket)
            .args(["run-shell", &format!("touch '{}'", planted.display())])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .unwrap();
        assert!(out.status.success() && planted.exists());
    }
}

/// 5.1: the 32-bit entry (`int 0x80`, where `socketcall` and the i386 `socket` live) is another
/// architecture to the filter, which kills the process instead of running the call.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn the_filter_kills_a_process_that_uses_the_32_bit_entry() {
    use std::os::unix::process::ExitStatusExt;
    if !kernel_support().available() {
        return;
    }
    let l = layout();
    let mut spec = spec_of(&l, Mode::Auto, &[]);
    spec.max_abi = Some(8);
    let out = child_output(&ChildPlan {
        spec,
        int80: true,
        ..ChildPlan::default()
    });
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("SANDBOX-RESULT-INT80 before"), "{stdout}");
    assert!(!stdout.contains("SANDBOX-RESULT {"), "{stdout}");
    assert_eq!(out.status.signal(), Some(libc::SIGSYS), "{:?}", out.status);
}

/// 5.1 (run by hand on a host with a session bus and Docker, `MRSM_TEST_HOST_SOCKETS=1`, with
/// `--ignored`): at "filesystem only" a team's process can connect to the D-Bus session bus and
/// to the Docker socket (and Docker answers its read-only `GET /_ping`); at "filesystem and
/// sockets" it can connect to neither. Nothing is sent to the bus.
#[cfg(target_os = "linux")]
#[test]
#[ignore]
fn host_sockets_at_each_level() {
    if std::env::var_os("MRSM_TEST_HOST_SOCKETS").is_none() {
        return;
    }
    let l = layout();
    let uid = rustix::process::getuid().as_raw();
    let bus = PathBuf::from(format!("/run/user/{uid}/bus"));
    let docker = PathBuf::from("/var/run/docker.sock");
    for max_abi in [Some(8), None] {
        let mut spec = spec_of(&l, Mode::Auto, &[]);
        spec.max_abi = max_abi;
        let plan = ChildPlan {
            spec,
            extra: Vec::new(),
            read: Vec::new(),
            write: Vec::new(),
            connect: vec![
                ("bus".into(), bus.clone()),
                ("docker".into(), docker.clone()),
            ],
            connect_abstract: Vec::new(),
            signal: None,
            fifo: Vec::new(),
            ..ChildPlan::default()
        };
        let Some((report, results)) = run_child(&plan) else {
            return;
        };
        println!("HOST-SOCKETS {} {results:?}", report.level());
    }
}

/// 5.1 (run by hand, `MRSM_TEST_FILTER_FALLBACKS=1` with `--ignored --nocapture`): what common
/// programs do under the Unix-socket filter (Landlock forced to ABI 8): glibc name resolution
/// (nss-resolve's varlink socket, nscd's), Python's DNS, TLS, asyncio, threads, subprocess and
/// `multiprocessing` start methods, and syslog. Prints one line per program.
#[cfg(target_os = "linux")]
#[test]
#[ignore]
fn filter_fallbacks() {
    if std::env::var_os("MRSM_TEST_FILTER_FALLBACKS").is_none() {
        return;
    }
    let l = layout();
    let mut spec = spec_of(&l, Mode::Auto, &[]);
    spec.max_abi = Some(8);
    let python = std::env::var("MRSM_TEST_FILTER_PYTHON").unwrap_or_else(|_| "python3".into());
    let py = |label: &str, code: &str| {
        (
            label.to_string(),
            vec![
                python.clone(),
                "-I".to_string(),
                "-c".to_string(),
                code.to_string(),
            ],
        )
    };
    let mp = |method: &str| {
        format!(
            "import multiprocessing as m\nif __name__ == '__main__':\n    \
             m.set_start_method('{method}')\n    \
             with m.Pool(2) as p: assert p.map(abs, [-1, -2]) == [1, 2]"
        )
    };
    // Extra readable prefixes (`:`-separated) and one more command (`\x1f`-separated argv).
    let extra = std::env::var("MRSM_TEST_FILTER_PREFIX")
        .unwrap_or_else(|_| "/usr".into())
        .split(':')
        .filter(|p| !p.is_empty())
        .map(|p| Rule::new(p, Access::ReadExec, "the interpreter"))
        .collect();
    let mut more: Vec<(String, Vec<String>)> = Vec::new();
    if let Ok(command) = std::env::var("MRSM_TEST_FILTER_COMMAND") {
        // `{tmp}` is the team's temporary directory (also its `TMPDIR`).
        let tmp = l.tmp.display().to_string();
        let mut argv = vec!["env".to_string(), format!("TMPDIR={tmp}")];
        argv.extend(command.split('\x1f').map(|a| a.replace("{tmp}", &tmp)));
        more.push(("extra command".into(), argv));
    }
    let plan = ChildPlan {
        spec,
        extra,
        run: vec![
            (
                "getent localhost".into(),
                vec!["getent".into(), "hosts".into(), "localhost".into()],
            ),
            (
                "getent pypi.org".into(),
                vec!["getent".into(), "ahosts".into(), "pypi.org".into()],
            ),
            (
                "logger".into(),
                vec!["logger".into(), "-t".into(), "mrsm-test".into(), "filter".into()],
            ),
            py(
                "python dns",
                "import socket; socket.getaddrinfo('pypi.org', 443)",
            ),
            py(
                "python tls",
                "import urllib.request; urllib.request.urlopen('https://pypi.org/simple/', timeout=20).read(1)",
            ),
            py("python asyncio", "import asyncio; asyncio.run(asyncio.sleep(0))"),
            py(
                "python threads+subprocess",
                "import threading, subprocess; t = threading.Thread(target=lambda: None); t.start(); t.join(); subprocess.run(['true'], check=True)",
            ),
            py("python mp spawn", &mp("spawn")),
            py("python mp fork", &mp("fork")),
            py("python mp forkserver", &mp("forkserver")),
            py(
                "python unix socket",
                "import socket; socket.socket(socket.AF_UNIX)",
            ),
        ]
        .into_iter()
        .chain(more)
        .collect(),
        ..ChildPlan::default()
    };
    let Some((report, results)) = run_child(&plan) else {
        return;
    };
    println!("FILTER {} {}", report.unix_sockets, report.describe());
    for (label, outcome) in &results {
        println!("FILTER {label}: {outcome}");
    }
}

/// 5.1 (run by hand, `MRSM_TEST_FILTER_COST=1` with `--ignored --nocapture`): the cost of the
/// Unix-socket filter in a sandboxed child: the median of 30 installs (building both programs and
/// installing them on all threads), and the round trip of 64 bytes each way over an inherited
/// socket pair (the shape of a worker's channel to the server), median of five batches of 20,000,
/// with Landlock only (the kernel's level) and with Landlock plus the filter (ABI 8).
#[cfg(target_os = "linux")]
#[test]
#[ignore]
#[allow(unsafe_code)]
fn filter_cost() {
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    if std::env::var_os("MRSM_TEST_FILTER_COST").is_none() || !kernel_support().available() {
        return;
    }
    let l = layout();
    for max_abi in [None, Some(8)] {
        let (parent_end, child_end) = std::os::unix::net::UnixStream::pair().unwrap();
        let child_fd = child_end.as_raw_fd();
        // SAFETY: clears close-on-exec on a descriptor this test owns, for the child.
        assert_eq!(unsafe { libc::fcntl(child_fd, libc::F_SETFD, 0) }, 0);
        let echo = std::thread::spawn(move || {
            let mut parent_end = parent_end;
            let mut buf = [0u8; 64];
            while parent_end.read_exact(&mut buf).is_ok() {
                if parent_end.write_all(&buf).is_err() {
                    break;
                }
            }
        });
        let mut spec = spec_of(&l, Mode::Auto, &[]);
        spec.max_abi = max_abi;
        let (report, results) = run_child(&ChildPlan {
            spec,
            inherited: Some(child_fd),
            bench: true,
            ..ChildPlan::default()
        })
        .unwrap();
        drop(child_end);
        let _ = echo.join();
        println!(
            "COST unix_sockets={} {}",
            report.unix_sockets,
            serde_json::Value::Object(results)
        );
    }
}
