//! Tests for the API host: manifests, the console probe, socket paths, launching, supervision
//! and health checks.

use super::*;

/// Serve `body` as the answer to every request on a fresh loopback port.
async fn squatter(answer: String) -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let answer = answer.clone();
            tokio::spawn(async move {
                let mut seen = [0u8; 1024];
                let _ = stream.read(&mut seen).await;
                let _ = stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                            answer.len()
                        )
                        .as_bytes(),
                    )
                    .await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn a_tcp_port_must_prove_it_holds_the_token() {
    let proof = host_proof("secret-token", "abc");
    // The value the Python host computes (`hmac.new(token, nonce, sha256).hexdigest()`).
    assert_eq!(
        proof,
        "ab01d25807daceae7b41624d27e76ff480e9d529a2f43afbcd40817414c61eab"
    );
    assert_eq!(proof.len(), 64);
    assert_ne!(proof, host_proof("other-token", "abc"));
    assert_ne!(proof, host_proof("secret-token", "abd"));
    // A process that took the port and answers anything (or a wrong proof) is refused ...
    for answer in [
        "hello".to_string(),
        "{\"console\": {\"mounted\": true}}".to_string(),
        proof.clone(),
    ] {
        // (the real proof is for another nonce, so it does not match either)
        let addr = squatter(answer.clone()).await;
        let endpoint = HostEndpoint::Tcp {
            addr,
            token: "secret-token".into(),
        };
        assert!(!endpoint_proves_token(&endpoint).await.unwrap(), "{answer}");
    }
    // ... a Unix endpoint needs none ...
    assert!(
        endpoint_proves_token(&HostEndpoint::from(PathBuf::from("/nowhere")))
            .await
            .unwrap()
    );
    // ... and nothing listening is an error to retry, not a proof.
    let free = {
        let l = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        l.local_addr().unwrap()
    };
    assert!(
        endpoint_proves_token(&HostEndpoint::Tcp {
            addr: free,
            token: "t".into()
        })
        .await
        .is_err()
    );
}

#[test]
fn manifest_flags() {
    assert!(!HostManifest::default().needs_host());
    let m: HostManifest = serde_json::from_str(r#"{"api_plugins": ["portal"]}"#).unwrap();
    assert!(m.needs_host());
    let dir = tempfile::tempdir().unwrap();
    assert!(
        !HostManifest::from_file(&dir.path().join("missing.json"))
            .unwrap()
            .needs_host()
    );
}

#[test]
#[cfg(unix)]
fn socket_paths() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let short = default_socket_path(root.path());
    assert_eq!(short.parent().unwrap(), root.path().join("api/host"));
    assert_ne!(short, default_socket_path(root.path()));
    assert!(
        short
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("host-")
    );
    let long_root = PathBuf::from(format!("/{}", "x".repeat(200)));
    let long = default_socket_path(&long_root);
    assert!(long.as_os_str().len() <= MAX_SOCKET_PATH, "{long:?}");
    let sibling = default_socket_path(&long_root);
    assert_ne!(long, sibling);
    assert_eq!(long.parent(), sibling.parent());
    assert_eq!(long.extension().unwrap(), "sock");
    std::fs::remove_dir(long.parent().unwrap()).unwrap();
}

#[cfg(unix)]
#[test]
fn socket_path_avoids_inaccessible_or_foreign_directories() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let root = tempfile::tempdir().unwrap();
    let api = root.path().join("api");
    std::fs::create_dir(&api).unwrap();
    // A regular file obstructs the parent for every UID, including root in CI.
    std::fs::write(api.join("host"), "leave this alone").unwrap();
    let fallback = default_socket_path(root.path());
    assert!(!fallback.starts_with(root.path()));
    let sibling = default_socket_path(root.path());
    assert_ne!(fallback, sibling);
    assert_eq!(fallback.parent(), sibling.parent());
    assert_eq!(
        std::fs::read_to_string(api.join("host")).unwrap(),
        "leave this alone"
    );
    let metadata = std::fs::metadata(fallback.parent().unwrap()).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
    assert_eq!(metadata.uid(), rustix::process::geteuid().as_raw());
    std::fs::remove_dir(fallback.parent().unwrap()).unwrap();

    // Real permission denial as an ordinary user; root bypasses mode checks.
    if rustix::process::geteuid().as_raw() != 0 {
        std::fs::remove_file(api.join("host")).unwrap();
        std::fs::set_permissions(&api, std::fs::Permissions::from_mode(0o500)).unwrap();
        let fallback = default_socket_path(root.path());
        assert!(!fallback.starts_with(root.path()));
        assert_eq!(
            std::fs::metadata(&api).unwrap().permissions().mode() & 0o777,
            0o500
        );
        std::fs::set_permissions(&api, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::remove_dir(fallback.parent().unwrap()).unwrap();
    }
}

fn host_config(cfg: &crate::config::ServerConfig) -> serde_json::Value {
    let launcher = PythonHostLauncher::for_config(cfg, PathBuf::from("python3"));
    let (_, config) = launcher
        .env
        .iter()
        .find(|(k, _)| k == "MRSM_API_HOST_CONFIG")
        .expect("host config");
    serde_json::from_str(config).unwrap()
}

#[test]
fn host_config_carries_the_console_flags() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = crate::config::ServerConfig::for_root("sql:main", root.path());
    let config = host_config(&cfg);
    assert_eq!(config["no_dash"], false);
    // The terminal is the Rust server's, whatever the flags say.
    assert_eq!(config["no_webterm"], true);
    cfg.no_webterm = true;
    assert_eq!(host_config(&cfg)["no_webterm"], true);
    cfg.no_webterm = false;
    cfg.no_dash = true;
    let config = host_config(&cfg);
    assert_eq!(config["no_dash"], true);
    assert_eq!(config["no_webterm"], true);
    assert_eq!(config["console"], false);
    assert_eq!(config["webterm"], false);
    // `--secure` (`actions_non_admin: false`) reaches the console: only administrators act.
    assert_eq!(config["actions_non_admin"], true);
    cfg.actions_non_admin = false;
    assert_eq!(host_config(&cfg)["actions_non_admin"], false);
}

#[test]
fn host_config_names_the_console_and_the_server_to_call_back() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = crate::config::ServerConfig::for_root("sql:main", root.path());
    cfg.host = "0.0.0.0".into();
    cfg.port = 8123;
    let config = host_config(&cfg);
    assert_eq!(config["console"], true);
    assert_eq!(config["webterm"], true);
    assert_eq!(config["server_url"], "http://127.0.0.1:8123");
    cfg.no_webterm = true;
    assert_eq!(host_config(&cfg)["webterm"], false);
    cfg.no_webterm = false;
    cfg.host = "::".into();
    assert_eq!(host_config(&cfg)["server_url"], "http://[::1]:8123");
    cfg.host = "192.0.2.7".into();
    assert_eq!(host_config(&cfg)["server_url"], "http://192.0.2.7:8123");
    cfg.host = "fd00::1".into();
    assert_eq!(host_config(&cfg)["server_url"], "http://[fd00::1]:8123");
    // HTTPS: the loopback-only plain listener, never the TLS port.
    cfg.internal_addr = Some("127.0.0.1:40123".parse().unwrap());
    assert_eq!(host_config(&cfg)["server_url"], "http://127.0.0.1:40123");
}

/// The host's stdin is a pipe whose write end this server holds (`PARENT_PIPE_ENV`), and
/// waiting for the host never closes it: only this server going away does, which is how a
/// host outlives no server that was SIGKILLed.
#[cfg(unix)]
#[tokio::test]
async fn the_host_holds_a_pipe_that_closes_only_with_the_server() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let script = dir.path().join("fake-python");
    std::fs::write(
        &script,
        "#!/bin/sh\necho \"pipe=$MRSM_API_HOST_PARENT_PIPE\" > \"$FAKE_OUT\"\ncat > /dev/null\necho eof >> \"$FAKE_OUT\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let launcher = PythonHostLauncher {
        python: script,
        env: vec![("FAKE_OUT".into(), out.display().to_string())],
    };
    // The script was written a moment ago: another test thread that forks while it is open for
    // writing leaves a child holding that descriptor until the child's own `exec`, and
    // starting the script is "text file busy" (`ETXTBSY`) until then. Only that is retried.
    let endpoint = HostEndpoint::from(dir.path().join("host.sock"));
    let mut attempts = 0;
    let mut proc = loop {
        match launcher.spawn_child(&endpoint) {
            Err(e) if e.kind() == std::io::ErrorKind::ExecutableFileBusy && attempts < 100 => {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            other => break other.unwrap(),
        }
    };
    let read = || std::fs::read_to_string(&out).unwrap_or_default();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !read().contains("pipe=") {
        assert!(
            std::time::Instant::now() < deadline,
            "the host never started"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(read().trim(), "pipe=1");
    // Waiting on the host (what the supervisor does all along) leaves the pipe open.
    assert!(
        tokio::time::timeout(Duration::from_millis(500), proc.wait())
            .await
            .is_err(),
        "the host saw end of file while its server runs: {}",
        read()
    );
    // What the kernel does when this server dies.
    drop(proc.parent_pipe.take());
    let code = tokio::time::timeout(Duration::from_secs(10), proc.wait())
        .await
        .expect("the host exits when its server is gone")
        .unwrap();
    assert_eq!(code, Some(0));
    assert!(read().contains("eof"), "{}", read());
}

#[test]
fn endpoints_become_host_arguments_and_environment() {
    let unix = HostEndpoint::from(PathBuf::from("/r/host.sock"));
    assert_eq!(
        PythonHostLauncher::endpoint_args(&unix),
        vec![
            std::ffi::OsString::from("--socket"),
            std::ffi::OsString::from("/r/host.sock")
        ]
    );
    assert!(PythonHostLauncher::endpoint_env(&unix).is_empty());
    // 5.1: a host this server starts on a Unix socket gets its token too.
    let started = HostEndpoint::Unix {
        socket: PathBuf::from("/r/host.sock"),
        token: Some("t0k".into()),
    };
    assert_eq!(
        PythonHostLauncher::endpoint_env(&started),
        vec![(PROXY_TOKEN_ENV, "t0k".to_string())]
    );
    let tcp = HostEndpoint::Tcp {
        addr: "127.0.0.1:4242".parse().unwrap(),
        token: "t0k".into(),
    };
    assert_eq!(
        PythonHostLauncher::endpoint_args(&tcp),
        vec![
            std::ffi::OsString::from("--tcp"),
            std::ffi::OsString::from("127.0.0.1:4242")
        ]
    );
    assert_eq!(
        PythonHostLauncher::endpoint_env(&tcp),
        vec![(PROXY_TOKEN_ENV, "t0k".to_string())]
    );
}

#[test]
fn the_transport_is_the_platforms_unless_overridden() {
    // Only the pure parts: the environment is the process's, so no variable is set here.
    if std::env::var_os(TRANSPORT_ENV).is_none() {
        let expected = if cfg!(windows) {
            HostTransport::Tcp
        } else {
            HostTransport::Unix
        };
        assert_eq!(HostTransport::from_env(), expected);
    }
}

#[test]
fn manifest_reasons_include_the_console() {
    assert!(HostManifest::default().reasons().is_empty());
    let console = HostManifest::default().with_console(true);
    assert!(console.needs_host());
    assert_eq!(console.reasons(), vec!["console"]);
    let m: HostManifest = serde_json::from_str(r#"{"web_pages": ["a"], "console": true}"#).unwrap();
    assert_eq!(m.reasons(), vec!["web_pages", "console"]);
}

/// A launched process that ignores nothing: it exits on SIGTERM, so shutdown returns promptly
/// without the SIGKILL fallback.
#[cfg(unix)]
#[tokio::test]
async fn child_is_terminated_with_sigterm_first() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("terminated");
    let script = format!(
        "trap 'touch {}; exit 0' TERM; while true; do sleep 0.05; done",
        marker.display()
    );
    let mut process = HostChild::spawn(
        tokio::process::Command::new("sh")
            .args(["-c", &script])
            .kill_on_drop(true),
    )
    .unwrap();
    // Let the shell install its trap.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = std::time::Instant::now();
    process.kill().await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(marker.exists(), "the host did not receive SIGTERM");
}
