//! Integration tests for host-team CPU, memory, file-size, disk-quota, and egress limits (5.1).

use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::team_scope::sandbox::{
    self, EgressProbe, HostEgress, LimitsReport, Mode, ResourceLimits, Spec,
};

#[test]
fn resource_limits_parse_from_config_and_describe() {
    let dir = tempfile::tempdir().unwrap();
    let patch = serde_json::json!({
        "system": {
            "engine": {
                "python_team_max_cpu_seconds": 120,
                "python_team_max_memory_mb": 2048,
                "python_team_max_file_mb": 64,
                "python_team_max_disk_mb": 512,
                "python_team_max_processes": 256,
                "python_team_egress": "allow_ports",
                "python_team_egress_ports": [5432, 443, 443]
            }
        }
    })
    .to_string();
    let cfg = Config::new(ConfigOptions {
        env: [
            (
                "MRSM_ROOT_DIR".to_string(),
                dir.path().to_string_lossy().into_owned(),
            ),
            ("MRSM_PATCH".to_string(), patch),
        ]
        .into_iter()
        .collect(),
        home: dir.path().to_path_buf(),
        windows: cfg!(windows),
        package_root: None,
        cpu_count: 4,
        allow_writes: false,
    })
    .unwrap();

    let limits = ResourceLimits::from_config(&cfg);
    assert!(limits.is_configured());
    assert_eq!(limits.cpu_seconds, Some(120));
    assert_eq!(limits.memory_mb, Some(2048));
    assert_eq!(limits.file_mb, Some(64));
    assert_eq!(limits.disk_mb, Some(512));
    assert_eq!(limits.processes, Some(256));
    assert_eq!(limits.egress, HostEgress::AllowPorts);
    assert_eq!(limits.egress_ports, vec![443, 5432]);
    let desc = limits.describe();
    assert!(desc.contains("cpu=120s"), "{desc}");
    assert!(desc.contains("memory=2048 MiB"), "{desc}");
    assert!(desc.contains("max_file=64 MiB"), "{desc}");
    assert!(desc.contains("disk_quota=512 MiB"), "{desc}");
    assert!(desc.contains("egress=allow_ports([443, 5432])"), "{desc}");
}

#[test]
fn disk_quota_enforces_aggregate_footprint_and_ignores_symlinks() {
    let dir = tempfile::tempdir().unwrap();
    let d1 = dir.path().join("tmp");
    let d2 = dir.path().join("cache");
    std::fs::create_dir_all(&d1).unwrap();
    std::fs::create_dir_all(&d2).unwrap();

    // 700 KiB + 700 KiB = 1.4 MiB -> under 2 MiB, over 1 MiB.
    std::fs::write(d1.join("a.bin"), vec![1u8; 700 * 1024]).unwrap();
    std::fs::write(d2.join("b.bin"), vec![2u8; 700 * 1024]).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(d1.join("a.bin"), d2.join("link.bin")).unwrap();

    let used = sandbox::limits::check_disk_quota("wetlands", &[&d1, &d2], 2).unwrap();
    assert_eq!(used, 1400 * 1024);

    let err = sandbox::limits::check_disk_quota("wetlands", &[&d1, &d2], 1).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("wetlands"), "{msg}");
    assert!(msg.contains("disk quota"), "{msg}");
}

#[test]
fn limits_report_verdict_fails_closed_on_errors_or_unproven_egress() {
    let mut rep = LimitsReport::from_limits(&ResourceLimits {
        egress: HostEgress::Deny,
        ..Default::default()
    });
    assert!(rep.verdict().is_err());

    rep.egress_probe = EgressProbe::Denied;
    assert!(rep.verdict().is_ok());

    rep.errors.push("rlimit_as failed: EPERM".into());
    assert!(rep.verdict().is_err());
}

#[cfg(target_os = "linux")]
mod linux_enforcement {
    use super::*;
    use std::io::Write;
    use std::net::{TcpListener, TcpStream, UdpSocket};
    use std::os::unix::net::UnixStream;
    use std::process::Command;

    const CHILD_CASE_ENV: &str = "MRSM_TEST_LIMITS_CHILD_CASE";
    const CHILD_DIR_ENV: &str = "MRSM_TEST_LIMITS_CHILD_DIR";
    const CHILD_ALLOWED_PORT_ENV: &str = "MRSM_TEST_LIMITS_ALLOWED_PORT";
    const CHILD_DENIED_PORT_ENV: &str = "MRSM_TEST_LIMITS_DENIED_PORT";

    fn run_child(case: &str, dir: &std::path::Path, extra_env: &[(&str, String)]) {
        let exe = std::env::current_exe().unwrap();
        let mut cmd = Command::new(exe);
        cmd.arg("--exact")
            .arg("team_resource_limits::linux_enforcement::child_runner")
            .arg("--nocapture")
            .env(CHILD_CASE_ENV, case)
            .env(CHILD_DIR_ENV, dir);
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let output = cmd.output().unwrap();
        assert!(
            output.status.success(),
            "child case `{case}` failed ({}):\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn child_runner() {
        let Ok(case) = std::env::var(CHILD_CASE_ENV) else {
            return;
        };
        let dir = std::path::PathBuf::from(std::env::var(CHILD_DIR_ENV).unwrap());
        let canary = dir.join("canary");
        std::fs::write(&canary, "secret").unwrap();
        let work = dir.join("work");
        std::fs::create_dir_all(&work).unwrap();

        match case.as_str() {
            "rlimits" => {
                let spec = Spec {
                    mode: Mode::Off,
                    team: "t".into(),
                    canary,
                    limits: ResourceLimits {
                        cpu_seconds: Some(30),
                        memory_mb: Some(2048),
                        file_mb: Some(1),
                        ..Default::default()
                    },
                    ..Spec::default()
                };
                let report = sandbox::enforce(&spec, &[]);
                report.verdict().unwrap();

                let cpu = rustix::process::getrlimit(rustix::process::Resource::Cpu);
                assert_eq!(cpu.current, Some(30));
                assert_eq!(cpu.maximum, Some(30));

                let fsize = rustix::process::getrlimit(rustix::process::Resource::Fsize);
                assert_eq!(fsize.current, Some(1024 * 1024));
                assert_eq!(fsize.maximum, Some(1024 * 1024));

                let mem = rustix::process::getrlimit(rustix::process::Resource::As);
                assert!(mem.current.is_some_and(|v| v <= 2048 * 1024 * 1024));
                assert_eq!(mem.current, mem.maximum);

                // Ignore SIGXFSZ so `write` returns EFBIG instead of killing the process.
                #[allow(unsafe_code)]
                unsafe {
                    libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
                }
                let mut f = std::fs::File::create(work.join("big.bin")).unwrap();
                f.write_all(&vec![b'x'; 512 * 1024]).unwrap();
                let err = f.write_all(&vec![b'y'; 1024 * 1024]).unwrap_err();
                assert_eq!(err.raw_os_error(), Some(libc::EFBIG));
            }
            "egress_deny" => {
                let denied_port: u16 = std::env::var(CHILD_DENIED_PORT_ENV)
                    .unwrap()
                    .parse()
                    .unwrap();
                let spec = Spec {
                    mode: Mode::Off,
                    team: "t".into(),
                    canary,
                    limits: ResourceLimits {
                        egress: HostEgress::Deny,
                        ..Default::default()
                    },
                    ..Spec::default()
                };
                let report = sandbox::enforce(&spec, &[]);
                report.verdict().unwrap();
                assert_eq!(report.limits.egress_mechanism, "seccomp");
                assert_eq!(report.limits.egress_probe, EgressProbe::Denied);

                // IPv4 TCP and UDP must be refused.
                assert!(TcpStream::connect(("127.0.0.1", denied_port)).is_err());
                assert!(UdpSocket::bind("127.0.0.1:0").is_err());
                // Connected Unix stream socketpair (worker IPC / multiprocessing) still works.
                assert!(UnixStream::pair().is_ok());
            }
            "egress_allow_ports" => {
                let allowed_port: u16 = std::env::var(CHILD_ALLOWED_PORT_ENV)
                    .unwrap()
                    .parse()
                    .unwrap();
                let denied_port: u16 = std::env::var(CHILD_DENIED_PORT_ENV)
                    .unwrap()
                    .parse()
                    .unwrap();
                let spec = Spec {
                    mode: Mode::Require,
                    team: "t".into(),
                    canary,
                    rules: vec![sandbox::Rule::new(
                        &work,
                        sandbox::Access::ReadWrite,
                        "work",
                    )],
                    limits: ResourceLimits {
                        egress: HostEgress::AllowPorts,
                        egress_ports: vec![allowed_port],
                        ..Default::default()
                    },
                    ..Spec::default()
                };
                let report = sandbox::enforce(&spec, &[]);
                report.verdict().unwrap();
                assert_eq!(report.limits.egress_mechanism, "landlock+seccomp");
                assert_eq!(report.limits.egress_probe, EgressProbe::Denied);

                // Allowed TCP port succeeds.
                assert!(TcpStream::connect(("127.0.0.1", allowed_port)).is_ok());
                // Disallowed TCP port is blocked by Landlock ABI 4+ AccessNet::ConnectTcp.
                let err = TcpStream::connect(("127.0.0.1", denied_port)).unwrap_err();
                assert_eq!(err.raw_os_error(), Some(libc::EACCES));
                // UDP is blocked by seccomp.
                assert!(UdpSocket::bind("127.0.0.1:0").is_err());
            }
            other => panic!("unknown child case: {other}"),
        }
    }

    #[test]
    fn enforces_rlimits_cpu_memory_and_file_size_in_child() {
        let dir = tempfile::tempdir().unwrap();
        run_child("rlimits", dir.path(), &[]);
    }

    #[test]
    fn enforces_egress_deny_in_child_while_preserving_unix_socketpair() {
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        run_child(
            "egress_deny",
            dir.path(),
            &[(CHILD_DENIED_PORT_ENV, port.to_string())],
        );
    }

    #[test]
    fn enforces_egress_allow_ports_via_landlock_v4_and_seccomp() {
        if sandbox::kernel_support().abi() < 4 {
            eprintln!("Skipping allow_ports test: kernel Landlock ABI < 4");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let allowed = TcpListener::bind("127.0.0.1:0").unwrap();
        let denied = TcpListener::bind("127.0.0.1:0").unwrap();
        let allowed_port = allowed.local_addr().unwrap().port();
        let denied_port = denied.local_addr().unwrap().port();
        run_child(
            "egress_allow_ports",
            dir.path(),
            &[
                (CHILD_ALLOWED_PORT_ENV, allowed_port.to_string()),
                (CHILD_DENIED_PORT_ENV, denied_port.to_string()),
            ],
        );
    }
}
