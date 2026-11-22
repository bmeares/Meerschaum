//! Tests of the team sandbox. The child-process machinery and the tests that re-run this test
//! binary by their own names stay here; the others are grouped by what they check.

use super::*;

mod filesystem;
mod rules;
mod sockets;

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
fn nix_mkfifo(path: &Path) {
    let status = std::process::Command::new("mkfifo")
        .args(["-m", "600"])
        .arg(path)
        .status()
        .unwrap();
    assert!(status.success(), "mkfifo {}", path.display());
}
