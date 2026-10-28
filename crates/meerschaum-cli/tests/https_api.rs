//! `start api --keyfile KEY --certfile CERT`: HTTPS served by the binary itself (process-level).
//! Half a pair and bad files stop it with the reason; a running server answers `https://` (and
//! MCP over it), keeps `/healthcheck` on plain HTTP for container healthchecks, redirects the
//! rest, and serves a renewed certificate after `SIGHUP` without restarting.
//!
//! Each server runs on a scratch root with a SQLite instance, `--no-dash` and no Python.
#![cfg(unix)]

use std::io::Read;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill")
                .args(["-TERM", &self.0.id().to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(5);
            while self.0.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Sandbox {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}

impl Sandbox {
    fn new() -> Sandbox {
        Sandbox {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_NOASK", "1")
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env("MRSM_PYTHON", "/nonexistent/python")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }
}

/// A free port in 19000-19999 (the range the shared test hosts leave to these servers), else
/// any free port.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    // Tests of this binary run at once: each call starts further on.
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let start = (std::process::id() % 1000) as u16;
    for _ in 0..1000u16 {
        let offset = NEXT.fetch_add(37, Ordering::SeqCst);
        let port = 19000 + (start.wrapping_add(offset)) % 1000;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.local_addr().unwrap().port()
}

/// A test CA, and leaves for `localhost` it signs.
struct Pki {
    ca: rcgen::Certificate,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

impl Pki {
    fn new() -> Pki {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "Meerschaum test CA");
        let key = rcgen::KeyPair::generate().unwrap();
        let ca = params.self_signed(&key).unwrap();
        Pki {
            ca,
            issuer: rcgen::Issuer::new(params, key),
        }
    }

    /// Write a new key and certificate for `localhost` to `dir`; the certificate's DER.
    fn write_leaf(&self, dir: &Path) -> Vec<u8> {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .signed_by(&key, &self.issuer)
            .unwrap();
        std::fs::write(dir.join("key.pem"), key.serialize_pem()).unwrap();
        std::fs::write(dir.join("cert.pem"), cert.pem()).unwrap();
        cert.der().to_vec()
    }

    fn client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .tls_certs_only([reqwest::Certificate::from_der(self.ca.der()).unwrap()])
            .tls_info(true)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
    }
}

/// Collect a child's output as it comes (so a full pipe never blocks the server).
fn capture(stream: impl Read + Send + 'static) -> Arc<Mutex<String>> {
    let text = Arc::new(Mutex::new(String::new()));
    let sink = text.clone();
    std::thread::spawn(move || {
        let mut stream = stream;
        let mut buf = [0u8; 4096];
        while let Ok(n) = stream.read(&mut buf) {
            if n == 0 {
                break;
            }
            sink.lock()
                .unwrap()
                .push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    });
    text
}

/// Run `start api` with `args` to its end (it refuses to start); its exit status and output.
fn refused(sandbox: &Sandbox, args: &[&str], env: &[(&str, &str)]) -> (bool, String) {
    let port = free_port().to_string();
    let mut command = sandbox.command();
    command.args([
        "start",
        "api",
        "-i",
        "sql:local",
        "--host",
        "127.0.0.1",
        "-p",
        &port,
        "--no-dash",
    ]);
    command.args(args);
    for (key, value) in env {
        command.env(key, value);
    }
    let mut child = ChildGuard(command.spawn().unwrap());
    let stdout = capture(child.0.stdout.take().unwrap());
    let stderr = capture(child.0.stderr.take().unwrap());
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "`start api {args:?}` did not stop: {}",
            stdout.lock().unwrap()
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    // The readers finish once the pipes close.
    std::thread::sleep(Duration::from_millis(100));
    let text = format!("{}{}", stdout.lock().unwrap(), stderr.lock().unwrap());
    (status.success(), text)
}

#[test]
fn half_a_pair_and_bad_files_stop_start_api_with_the_reason() {
    let sandbox = Sandbox::new();
    let pki = Pki::new();
    let certs = tempfile::tempdir().unwrap();
    pki.write_leaf(certs.path());
    let key = certs.path().join("key.pem").display().to_string();
    let cert = certs.path().join("cert.pem").display().to_string();

    let (ok, text) = refused(&sandbox, &["--keyfile", &key], &[]);
    assert!(!ok, "{text}");
    assert!(
        text.contains("HTTPS needs a private key and its certificate, but only the key is set")
            && text.contains("`--certfile`"),
        "{text}"
    );

    let patch = serde_json::json!({"api": {"server": {"ssl_certfile": cert}}}).to_string();
    let (ok, text) = refused(&sandbox, &[], &[("MRSM_CONFIG", &patch)]);
    assert!(!ok, "{text}");
    assert!(
        text.contains("only the certificate is set") && text.contains("`api:server:ssl_certfile`"),
        "{text}"
    );

    let (ok, text) = refused(&sandbox, &["--keyfile", &cert, "--certfile", &key], &[]);
    assert!(!ok, "{text}");
    assert!(text.contains("swapped"), "{text}");

    let missing = certs.path().join("missing.pem").display().to_string();
    let (ok, text) = refused(&sandbox, &["--keyfile", &key, "--certfile", &missing], &[]);
    assert!(!ok, "{text}");
    assert!(
        text.contains("Cannot read the TLS certificate") && text.contains("does not exist"),
        "{text}"
    );
}

#[test]
fn https_serves_mcp_keeps_the_plain_healthcheck_and_reloads_on_sighup() {
    let sandbox = Sandbox::new();
    let pki = Pki::new();
    let certs = tempfile::tempdir().unwrap();
    let first = pki.write_leaf(certs.path());
    let port = free_port();
    let mut child = ChildGuard(
        sandbox
            .command()
            .args([
                "start",
                "api",
                "-i",
                "sql:local",
                "--host",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "--no-dash",
                "--no-auth",
                "--keyfile",
                &certs.path().join("key.pem").display().to_string(),
                "--certfile",
                &certs.path().join("cert.pem").display().to_string(),
            ])
            .spawn()
            .unwrap(),
    );
    let stdout = capture(child.0.stdout.take().unwrap());
    let stderr = capture(child.0.stderr.take().unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = pki.client();
    let base = format!("https://localhost:{port}");
    let served = |client: &reqwest::Client, path: &str| {
        let url = format!("{base}{path}");
        let client = client.clone();
        runtime.block_on(async move {
            let response = client.get(url).send().await?;
            let peer = response
                .extensions()
                .get::<reqwest::tls::TlsInfo>()
                .and_then(|info| info.peer_certificate())
                .map(<[u8]>::to_vec);
            let status = response.status();
            let text = response.text().await?;
            Ok::<_, reqwest::Error>((status, text, peer))
        })
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "the server exited: {}{}",
            stdout.lock().unwrap(),
            stderr.lock().unwrap()
        );
        if served(&client, "/healthcheck").is_ok_and(|(status, _, _)| status.is_success()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no HTTPS answer: {}",
            stderr.lock().unwrap()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let banner = stdout.lock().unwrap().clone();
    assert!(
        banner.contains(&format!(
            "Serving HTTPS on https://localhost:{port} (certificate for localhost"
        )),
        "{banner}"
    );
    let (status, version, peer) = served(&client, "/version").unwrap();
    assert!(status.is_success(), "{version}");
    assert_eq!(peer.as_deref(), Some(first.as_slice()));

    // MCP over HTTPS.
    let initialize = runtime
        .block_on(
            client
                .post(format!("{base}/mcp"))
                .header("accept", "application/json, text/event-stream")
                .json(&serde_json::json!({"jsonrpc": "2.0", "id": 7, "method": "initialize", "params": {}}))
                .send(),
        )
        .unwrap();
    assert!(initialize.status().is_success());
    let text = runtime.block_on(initialize.text()).unwrap();
    assert!(
        text.contains("\"id\":7") && text.contains("\"result\""),
        "{text}"
    );

    // Plain HTTP on the same port: the healthcheck (as the stack's `curl --fail
    // http://localhost:<port>/healthcheck` asks), and a redirect for everything else.
    let plain = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let health = runtime
        .block_on(
            plain
                .get(format!("http://localhost:{port}/healthcheck"))
                .send(),
        )
        .unwrap();
    assert_eq!(health.status(), reqwest::StatusCode::OK);
    let redirect = runtime
        .block_on(plain.get(format!("http://localhost:{port}/docs")).send())
        .unwrap();
    assert_eq!(redirect.status(), reqwest::StatusCode::PERMANENT_REDIRECT);
    assert_eq!(
        redirect.headers()["location"],
        format!("https://localhost:{port}/docs").as_str()
    );

    // A renewal and `SIGHUP`: the new certificate, from the same process.
    let renewed = pki.write_leaf(certs.path());
    assert_ne!(renewed, first);
    let pid = child.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-HUP", &pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        // A fresh connection (and handshake) each time.
        let client = pki.client();
        if served(&client, "/version")
            .is_ok_and(|(_, _, peer)| peer.as_deref() == Some(renewed.as_slice()))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the renewed certificate was not served"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        child.0.try_wait().unwrap().is_none(),
        "SIGHUP stopped the server"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while !stdout
        .lock()
        .unwrap()
        .contains("Reloaded the TLS certificate from")
    {
        assert!(Instant::now() < deadline, "{}", stdout.lock().unwrap());
        std::thread::sleep(Duration::from_millis(20));
    }

    // SIGTERM stops it as usual.
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    while child.0.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "the server did not stop");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Empty flags turn off a pair set in the config (`start gui` relies on it): plain HTTP.
#[test]
fn empty_flags_serve_plain_http_despite_the_config() {
    let sandbox = Sandbox::new();
    let pki = Pki::new();
    let certs = tempfile::tempdir().unwrap();
    pki.write_leaf(certs.path());
    let patch = serde_json::json!({"api": {"server": {
        "ssl_keyfile": certs.path().join("key.pem").display().to_string(),
        "ssl_certfile": certs.path().join("cert.pem").display().to_string(),
    }}})
    .to_string();
    let port = free_port();
    let mut child = ChildGuard(
        sandbox
            .command()
            .env("MRSM_CONFIG", &patch)
            .args([
                "start",
                "api",
                "-i",
                "sql:local",
                "--host",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "--no-dash",
                "--no-auth",
                "--keyfile",
                "",
                "--certfile",
                "",
            ])
            .spawn()
            .unwrap(),
    );
    let stderr = capture(child.0.stderr.take().unwrap());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "the server exited: {}",
            stderr.lock().unwrap()
        );
        let answer = runtime.block_on(client.get(format!("http://127.0.0.1:{port}/docs")).send());
        if let Ok(answer) = answer {
            // Plain HTTP itself, not the redirect of an HTTPS port.
            assert_eq!(answer.status(), reqwest::StatusCode::OK);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no answer: {}",
            stderr.lock().unwrap()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
