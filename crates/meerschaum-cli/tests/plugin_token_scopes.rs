//! A plugin-defined token scope end to end under `mrsm start api` (rc32): per-device ingest
//! tokens for a single-file Rust plugin.
//!
//! - The plugin declares `POST /example/ingest` requiring its own scope `example:ingest` (and
//!   `GET /example/report` requiring `example:report`).
//! - An administrator (a holder of `*`) registers a token with only `example:ingest` and a
//!   device label; its API key, and a JWT minted from it (`client_credentials`), call the
//!   ingest route (`200`) and get `403` on every other authenticated route: the pipes routes,
//!   the plugin's other route and an administrator's route.
//! - The handler's `Principal` carries the token's id and label (no secret).
//! - Revocation is immediate: the request after `invalidate` (or `DELETE`) is refused, for the
//!   API key and for the JWT minted from it (each request reads the token from the instance).
//!
//! Unix only (as `single_file_server.rs`). A temporary root and plugins directory and a scrubbed
//! environment; all data is synthetic.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::json;

#[path = "../../meerschaum-plugin/tests/support/compile_single_file.rs"]
mod compile_single_file;

const SOURCE: &str = r##"//! Device ingest behind a plugin-defined scope.

/// Accept one reading from a device; echo who sent it.
#[api_route(POST, "/example/ingest", scopes = ["example:ingest"])]
fn ingest(request: &RouteRequest) -> Value {
    json!({"principal": request.principal, "scopes": request.scopes,
        "body": request.text().unwrap_or_default()})
}

/// A report only `example:report` may read.
#[api_route(GET, "/example/report", scopes = ["example:report"])]
fn report(_request: &RouteRequest) -> Value {
    json!({"rows": 0})
}
"##;

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}

impl Home {
    fn new() -> Home {
        let home = Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        };
        std::fs::create_dir_all(home.root.path().join("sqlite")).unwrap();
        std::fs::write(home.plugins.path().join("example.rs"), SOURCE).unwrap();
        let builds = home.root.path().join("cache/native_build");
        let found = meerschaum_plugin::discovery::discover_report_in(
            &[home.plugins.path().into()],
            &builds,
        );
        assert!(found.skipped.is_empty(), "{:?}", found.skipped);
        let candidate = &found.candidates[0];
        compile_single_file::compile(candidate);
        std::fs::write(
            builds.join("example/mrsm-source-fingerprint"),
            &candidate.fingerprint,
        )
        .unwrap();
        home
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_NOASK", "true")
            .env(
                "MRSM_CONFIG",
                r#"{"meerschaum":{"instance":"sql:local"},"formatting":{"ansi":false,"unicode":false}}"#,
            );
        command
    }

    /// `admin` holds `*`, so it may grant a plugin's own scopes.
    fn register_admin(&self) {
        let mut registering = self
            .command()
            .env_remove("MRSM_NOASK")
            .args([
                "register",
                "user",
                "admin",
                "-i",
                "sql:local",
                "--nopretty",
                "--scopes",
                "*",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        registering
            .stdin
            .take()
            .unwrap()
            .write_all(b"n\nsynthetic-pass-1\nsynthetic-pass-1\n\n")
            .unwrap();
        let output = registering.wait_with_output().unwrap();
        assert!(output.status.success(), "{output:?}");
    }
}

struct Server {
    child: Child,
    port: u16,
    stderr: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status();
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `mrsm start api` on a random loopback port (never 8989), without the console.
fn serve(home: &Home) -> Server {
    let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    assert_ne!(port, 8989);
    drop(socket);
    let stderr = home.root.path().join("server.err");
    let mut command = home.command();
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let child = command
        .args([
            "start",
            "api",
            "-i",
            "sql:local",
            "--host",
            "127.0.0.1",
            "-p",
        ])
        .arg(port.to_string())
        .args(["--no-dash", "-w", "2"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .spawn()
        .unwrap();
    let mut server = Server {
        child,
        port,
        stderr,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    // Ready (no Python plugin: no API host to wait for).
    while http(port, "GET", "/healthcheck/ready", &[], b"").map(|r| r.status) != Some(200) {
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "server exited before it was ready: {}",
            std::fs::read_to_string(&server.stderr).unwrap_or_default()
        );
        assert!(
            Instant::now() < deadline,
            "server did not get ready in time"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    server
}

struct Reply {
    status: u16,
    body: Vec<u8>,
}

impl Reply {
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(&self.body)))
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// One request (`None` when nothing listens yet).
fn http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Option<Reply> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (key, value) in headers {
        head.push_str(&format!("{key}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes()).ok()?;
    stream.write_all(body).ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).ok()?;
    let split = response.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&response[..split]).into_owned();
    let status = head.split(' ').nth(1)?.parse().ok()?;
    let mut body = response[split + 4..].to_vec();
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body = dechunk(&body);
    }
    Some(Reply { status, body })
}

fn dechunk(mut data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let line_end = data.windows(2).position(|w| w == b"\r\n").unwrap();
        let size =
            usize::from_str_radix(std::str::from_utf8(&data[..line_end]).unwrap().trim(), 16)
                .unwrap();
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&data[line_end + 2..line_end + 2 + size]);
        data = &data[line_end + 2 + size + 2..];
    }
}

fn call(port: u16, method: &str, path: &str, auth: &str, body: &[u8]) -> Reply {
    let mut headers = vec![("Authorization", auth)];
    if !body.is_empty() {
        headers.push(("Content-Type", "application/json"));
    }
    http(port, method, path, &headers, body).expect("the server answers")
}

fn form(port: u16, path: &str, form: &str) -> Reply {
    http(
        port,
        "POST",
        path,
        &[("Content-Type", "application/x-www-form-urlencoded")],
        form.as_bytes(),
    )
    .expect("the server answers")
}

/// A registered device token: `(id, secret, "Bearer <api key>")`.
fn register_device(port: u16, admin: &str, label: &str) -> (String, String, String) {
    let body = json!({"label": label, "scopes": ["example:ingest"]}).to_string();
    let reply = call(port, "POST", "/tokens/register", admin, body.as_bytes());
    assert_eq!(reply.status, 200, "{}", reply.text());
    let token = reply.json();
    assert_eq!(token["label"], label);
    (
        token["id"].as_str().unwrap().to_string(),
        token["secret"].as_str().unwrap().to_string(),
        format!("Bearer {}", token["api_key"].as_str().unwrap()),
    )
}

#[test]
fn a_plugin_scope_admits_only_its_route_and_revocation_is_immediate() {
    let home = Home::new();
    home.register_admin();
    let server = serve(&home);
    let port = server.port;

    let login = form(port, "/login", "username=admin&password=synthetic-pass-1");
    assert_eq!(login.status, 200, "{}", login.text());
    let admin = format!("Bearer {}", login.json()["access_token"].as_str().unwrap());

    let (id, secret, key) = register_device(port, &admin, "device-17");
    // A JWT minted from the device token carries its scopes.
    let minted = form(
        port,
        "/login",
        &format!("grant_type=client_credentials&client_id={id}&client_secret={secret}"),
    );
    assert_eq!(minted.status, 200, "{}", minted.text());
    let jwt = format!("Bearer {}", minted.json()["access_token"].as_str().unwrap());

    for auth in [&key, &jwt] {
        // The plugin's route admits its own scope; the handler sees the token's id and label.
        let ingest = call(port, "POST", "/example/ingest", auth, br#"{"value": 1}"#);
        assert_eq!(ingest.status, 200, "{}", ingest.text());
        let body = ingest.json();
        assert_eq!(body["principal"]["kind"], "token", "{body}");
        assert_eq!(body["principal"]["token_id"], id.as_str(), "{body}");
        assert_eq!(body["principal"]["label"], "device-17", "{body}");
        assert_eq!(body["scopes"], json!(["example:ingest"]), "{body}");
        assert_eq!(body["body"], r#"{"value": 1}"#);
        assert!(
            !ingest.text().contains(&secret),
            "the secret reached the plugin"
        );

        // Every other authenticated route is refused.
        for (method, path, body) in [
            ("GET", "/pipes", &b""[..]),
            ("GET", "/pipes/keys", &b""[..]),
            ("GET", "/example/report", &b""[..]),
            ("POST", "/teams/devices/register", &b"{}"[..]),
        ] {
            let refused = call(port, method, path, auth, body);
            assert_eq!(refused.status, 403, "{method} {path}: {}", refused.text());
        }
    }
    // The administrator passes the plugin's own scopes (`*`).
    let report = call(port, "GET", "/example/report", &admin, b"");
    assert_eq!(report.status, 200, "{}", report.text());

    // Invalidation: the very next request is refused, for the key and for the JWT.
    let invalidated = call(
        port,
        "POST",
        &format!("/tokens/{id}/invalidate"),
        &admin,
        b"",
    );
    assert_eq!(invalidated.status, 200, "{}", invalidated.text());
    for auth in [&key, &jwt] {
        let refused = call(port, "POST", "/example/ingest", auth, b"{}");
        assert_eq!(refused.status, 401, "{}", refused.text());
    }

    // Deletion: likewise.
    let (other_id, _, other_key) = register_device(port, &admin, "device-18");
    let ingest = call(port, "POST", "/example/ingest", &other_key, b"{}");
    assert_eq!(ingest.status, 200, "{}", ingest.text());
    let deleted = call(port, "DELETE", &format!("/tokens/{other_id}"), &admin, b"");
    assert_eq!(deleted.status, 200, "{}", deleted.text());
    let refused = call(port, "POST", "/example/ingest", &other_key, b"{}");
    assert_eq!(refused.status, 401, "{}", refused.text());
}
