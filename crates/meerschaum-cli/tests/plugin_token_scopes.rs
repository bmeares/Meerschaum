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
//! - rc34: the plugin's own routes mint and revoke tokens (`host::mint_token`,
//!   `host::revoke_token`): an administrator's provisioning route mints a device token; the
//!   device's heartbeat (called with that token) mints its successor and revokes itself, and the
//!   old token is refused on the next request. `*`, core scopes and another plugin's scope are
//!   never minted; another user's token and a token with a core scope are never revoked.
//! - rc34: a long poll ends as soon as the server drains (`host::draining()`).
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

/// The answer of a host call: its value, or `403` naming the refusal.
fn answer<T>(result: Result<T>, value: impl FnOnce(T) -> Value) -> (u16, Value) {
    match result {
        Ok(ok) => (200, value(ok)),
        Err(error) => (403, json!({"detail": error.to_string()})),
    }
}

/// Provision a device: `{"label", "scopes"}` -> its new token.
#[api_route(POST, "/example/provision", scopes = ["example:admin"])]
fn provision(request: &RouteRequest) -> Result<(u16, Value)> {
    let body: Value = request.json()?;
    let scopes: Vec<&str> = body["scopes"].as_array().into_iter().flatten()
        .filter_map(Value::as_str).collect();
    let label = body["label"].as_str().unwrap_or_default();
    Ok(answer(host::mint_token(label, &scopes, None), |token| token.to_json()))
}

/// A device's heartbeat: its successor token, and the one it called with revoked.
#[api_route(POST, "/example/heartbeat", scopes = ["example:ingest"])]
fn heartbeat(request: &RouteRequest) -> (u16, Value) {
    let principal = request.principal.clone().unwrap();
    let label = principal.label.unwrap_or_default();
    let next = match host::mint_token(&label, &["example:ingest"], None) {
        Ok(next) => next,
        Err(error) => return (403, json!({"detail": error.to_string()})),
    };
    let old = principal.token_id.unwrap_or_default();
    answer(host::revoke_token(&old), |revoked| {
        json!({"next": next.to_json(), "revoked": revoked.message})
    })
}

/// Revoke `{"id"}`.
#[api_route(POST, "/example/revoke", scopes = ["example:admin"])]
fn revoke(request: &RouteRequest) -> Result<(u16, Value)> {
    let body: Value = request.json()?;
    let id = body["id"].as_str().unwrap_or_default();
    Ok(answer(host::revoke_token(id), |revoked| json!({"revoked": revoked.message})))
}

/// A long poll: waits up to `seconds` (30), and returns as soon as the server drains.
#[api_route(GET, "/example/poll", scopes = ["example:ingest"])]
fn poll(request: &RouteRequest) -> Result<Value> {
    let seconds: u64 = request.query_param("seconds").and_then(|s| s.parse().ok()).unwrap_or(30);
    let deadline = std::time::Instant::now() + Duration::from_secs(seconds);
    while !host::draining()? && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(json!({"drained": host::draining()?}))
}
"##;

/// A second plugin, whose scope `example` may not mint.
const OTHER: &str = r##"//! Another plugin's scope.

#[api_route(GET, "/other/read", scopes = ["other:read"])]
fn read(_request: &RouteRequest) -> Value {
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
        std::fs::write(home.plugins.path().join("other.rs"), OTHER).unwrap();
        let builds = home.root.path().join("cache/native_build");
        let found = meerschaum_plugin::discovery::discover_report_in(
            &[home.plugins.path().into()],
            &builds,
        );
        assert!(found.skipped.is_empty(), "{:?}", found.skipped);
        assert_eq!(found.candidates.len(), 2);
        for candidate in &found.candidates {
            compile_single_file::compile(candidate);
            std::fs::write(
                builds.join(&candidate.name).join("mrsm-source-fingerprint"),
                &candidate.fingerprint,
            )
            .unwrap();
        }
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
        self.register_user("admin", "*");
    }

    /// Register `username` (password `synthetic-pass-1`) with `scopes`.
    fn register_user(&self, username: &str, scopes: &str) {
        let mut registering = self
            .command()
            .env_remove("MRSM_NOASK")
            .args([
                "register",
                "user",
                username,
                "-i",
                "sql:local",
                "--nopretty",
                "--scopes",
                scopes,
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

/// `POST` JSON `body` to `path`.
fn post(port: u16, path: &str, auth: &str, body: serde_json::Value) -> Reply {
    call(port, "POST", path, auth, body.to_string().as_bytes())
}

/// `Bearer <api key>` of a token as the plugin returned it.
fn bearer(token: &serde_json::Value) -> String {
    format!("Bearer {}", token["api_key"].as_str().unwrap())
}

#[test]
fn a_plugin_route_mints_and_rotates_device_tokens_with_its_own_scope() {
    let home = Home::new();
    home.register_admin();
    home.register_user("eve", "example:ingest");
    let server = serve(&home);
    let port = server.port;
    let login = |username: &str| {
        let login = form(
            port,
            "/login",
            &format!("username={username}&password=synthetic-pass-1"),
        );
        assert_eq!(login.status, 200, "{}", login.text());
        format!("Bearer {}", login.json()["access_token"].as_str().unwrap())
    };
    let admin = login("admin");

    // Provisioning: the administrator's request mints a device's token with the plugin scope.
    let provisioned = post(
        port,
        "/example/provision",
        &admin,
        json!({"label": "device-17", "scopes": ["example:ingest"]}),
    );
    assert_eq!(provisioned.status, 200, "{}", provisioned.text());
    let first = provisioned.json();
    assert_eq!(first["label"], "device-17");
    assert_eq!(first["scopes"], json!(["example:ingest"]));
    assert!(first["secret"].as_str().is_some_and(|s| !s.is_empty()));
    let first_key = bearer(&first);

    // The token reaches that plugin route and nothing else.
    let ingest = call(port, "POST", "/example/ingest", &first_key, b"{}");
    assert_eq!(ingest.status, 200, "{}", ingest.text());
    assert_eq!(ingest.json()["principal"]["token_id"], first["id"]);
    assert_eq!(ingest.json()["principal"]["label"], "device-17");
    for (method, path) in [
        ("GET", "/pipes"),
        ("GET", "/example/report"),
        ("GET", "/other/read"),
        ("POST", "/example/provision"),
    ] {
        let refused = call(port, method, path, &first_key, b"{}");
        assert_eq!(refused.status, 403, "{method} {path}: {}", refused.text());
    }

    // Rotation: the device's heartbeat mints its successor and revokes the token it called with.
    let heartbeat = call(port, "POST", "/example/heartbeat", &first_key, b"");
    assert_eq!(heartbeat.status, 200, "{}", heartbeat.text());
    let rotated = heartbeat.json();
    let second = &rotated["next"];
    assert_eq!(second["label"], "device-17");
    assert_ne!(second["id"], first["id"]);
    // The very next request with the old token is refused; the successor works.
    let refused = call(port, "POST", "/example/ingest", &first_key, b"{}");
    assert_eq!(refused.status, 401, "{}", refused.text());
    let second_key = bearer(second);
    let ingest = call(port, "POST", "/example/ingest", &second_key, b"{}");
    assert_eq!(ingest.status, 200, "{}", ingest.text());
    assert_eq!(ingest.json()["principal"]["token_id"], second["id"]);
    // The successor belongs to the administrator, like the token it replaced.
    let model = call(
        port,
        "GET",
        &format!("/tokens/{}", second["id"].as_str().unwrap()),
        &admin,
        b"",
    );
    assert_eq!(model.status, 200, "{}", model.text());
    assert_eq!(model.json()["scopes"], json!(["example:ingest"]));

    // `*`, a core scope, another plugin's scope (the administrator holds them all) and none.
    for scopes in [
        json!(["*"]),
        json!(["pipes:read"]),
        json!(["example:ingest", "users:write"]),
        json!(["other:read"]),
        json!([]),
    ] {
        let refused = post(
            port,
            "/example/provision",
            &admin,
            json!({"label": "device-18", "scopes": scopes}),
        );
        assert_eq!(refused.status, 403, "{scopes}: {}", refused.text());
        assert!(
            refused.json()["detail"]
                .as_str()
                .unwrap()
                .contains("'example'"),
            "{}",
            refused.text()
        );
    }

    // Revocation refuses another user's token, the administrator's token with a core scope and
    // an id that does not exist, alike; each keeps working.
    let eve = login("eve");
    let eves = call(
        port,
        "POST",
        "/tokens/register",
        &eve,
        br#"{"label": "eve-device", "scopes": ["example:ingest"]}"#,
    );
    assert_eq!(eves.status, 200, "{}", eves.text());
    let eves = eves.json();
    let core = call(
        port,
        "POST",
        "/tokens/register",
        &admin,
        br#"{"label": "ops", "scopes": ["example:ingest", "pipes:read"]}"#,
    );
    assert_eq!(core.status, 200, "{}", core.text());
    let core = core.json();
    let missing = "00000000-0000-4000-8000-000000000000";
    for id in [
        eves["id"].as_str().unwrap(),
        core["id"].as_str().unwrap(),
        missing,
    ] {
        let refused = post(port, "/example/revoke", &admin, json!({"id": id}));
        assert_eq!(refused.status, 403, "{}", refused.text());
        assert_eq!(
            refused.json()["detail"],
            format!(
                "Plugin 'example' may not revoke token '{id}': a plugin revokes only its \
                 caller's own tokens that carry nothing but the plugin's own scopes."
            )
        );
    }
    for token in [&eves, &core] {
        let ingest = call(port, "POST", "/example/ingest", &bearer(token), b"{}");
        assert_eq!(ingest.status, 200, "{}", ingest.text());
    }
    // The administrator revokes its device's token through the plugin.
    let revoked = post(port, "/example/revoke", &admin, json!({"id": second["id"]}));
    assert_eq!(revoked.status, 200, "{}", revoked.text());
    let refused = call(port, "POST", "/example/ingest", &second_key, b"{}");
    assert_eq!(refused.status, 401, "{}", refused.text());
}

#[test]
fn a_long_poll_ends_when_the_server_drains() {
    let home = Home::new();
    home.register_admin();
    let server = serve(&home);
    let port = server.port;
    let login = form(port, "/login", "username=admin&password=synthetic-pass-1");
    assert_eq!(login.status, 200, "{}", login.text());
    let admin = format!("Bearer {}", login.json()["access_token"].as_str().unwrap());
    let token = post(
        port,
        "/example/provision",
        &admin,
        json!({"label": "device-17", "scopes": ["example:ingest"]}),
    );
    assert_eq!(token.status, 200, "{}", token.text());
    let key = bearer(&token.json());

    // Serving: a short poll runs out its time, not drained.
    let short = call(port, "GET", "/example/poll?seconds=1", &key, b"");
    assert_eq!(short.status, 200, "{}", short.text());
    assert_eq!(short.json()["drained"], false);

    // A 30-second poll in flight ends as soon as the drain begins, well inside the 8-second
    // shutdown grace period.
    let polling = std::thread::spawn(move || {
        let reply = call(port, "GET", "/example/poll", &key, b"");
        (reply.status, reply.json(), Instant::now())
    });
    std::thread::sleep(Duration::from_millis(1500));
    let drain_began = Instant::now();
    let status = Command::new("kill")
        .args(["-TERM", &server.child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let (status, body, answered) = polling.join().unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["drained"], true, "{body}");
    let waited = answered.duration_since(drain_began);
    assert!(waited < Duration::from_secs(4), "{waited:?}");
}
