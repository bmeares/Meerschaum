//! Tests of Unix socket denial at each level, by Landlock or the seccomp filter, in sandboxed
//! child processes (Linux).

use super::*;

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
