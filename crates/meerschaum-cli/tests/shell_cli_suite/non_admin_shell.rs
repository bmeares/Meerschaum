//! 5.1: a caller who is not an administrator can never reach the system shell through the action
//! routes (`POST /execute`, `POST /jobs/{name}`, `POST /actions/{action}`, `POST
//! /execute/validate`, the MCP `execute_action` tool). The dispatcher runs a word that is not an
//! action as a shell command (v4), so a step is allowed for a non-administrator only when its
//! action resolves to a Meerschaum action (native, or a plugin's) that is not refused. Every
//! request here would create a marker file if the shell ran it; none may. An administrator's
//! request still reaches the shell, as before.
//!
//! Unix only. A temporary root and plugins directory and a scrubbed environment; all data is
//! synthetic.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::json;

const PASSWORD: &str = "synthetic-pass-1";

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
    marks: tempfile::TempDir,
}

impl Home {
    fn new() -> Home {
        let home = Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
            marks: tempfile::tempdir().unwrap(),
        };
        std::fs::create_dir_all(home.root.path().join("sqlite")).unwrap();
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
                json!({
                    "meerschaum": {"instance": "sql:local"},
                    "formatting": {"ansi": false, "unicode": false},
                    "api": {
                        "mcp": {"enabled": true},
                        "permissions": {"actions": {"non_admin": true}},
                    },
                    "system": {"api": {"permissions": {"actions": {"non_admin": true}}}},
                    "jobs": {"startup": {"enabled": false}},
                })
                .to_string(),
            );
        if let Some(python) = std::env::var_os("MRSM_PYTHON") {
            command.env("MRSM_PYTHON", python);
        }
        command
    }

    fn register_user(&self, username: &str, admin: bool, scopes: &[&str]) {
        let mut command = self.command();
        command.env_remove("MRSM_NOASK").args([
            "register",
            "user",
            username,
            "-i",
            "sql:local",
            "--nopretty",
        ]);
        if !scopes.is_empty() {
            command.arg("--scopes").args(scopes);
        }
        let mut registering = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        registering
            .stdin
            .take()
            .unwrap()
            .write_all(
                format!(
                    "{}\n{PASSWORD}\n{PASSWORD}\n\n",
                    if admin { "y" } else { "n" }
                )
                .as_bytes(),
            )
            .unwrap();
        let output = registering.wait_with_output().unwrap();
        assert!(output.status.success(), "{output:?}");
    }

    fn mark(&self, name: &str) -> PathBuf {
        self.marks.path().join(name)
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
    let deadline = Instant::now() + Duration::from_secs(60);
    while http(port, "GET", "/healthcheck/ready", &[], b"").map(|r| r.status) != Some(200) {
        assert!(
            server.child.try_wait().unwrap().is_none(),
            "server exited before it was ready: {}",
            std::fs::read_to_string(&server.stderr).unwrap_or_default()
        );
        assert!(Instant::now() < deadline, "server did not get ready");
        std::thread::sleep(Duration::from_millis(50));
    }
    server
}

struct Reply {
    status: u16,
    body: Vec<u8>,
}

impl Reply {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }
}

fn http(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Option<Reply> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
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
        let Some(line_end) = data.windows(2).position(|w| w == b"\r\n") else {
            return out;
        };
        let size =
            usize::from_str_radix(std::str::from_utf8(&data[..line_end]).unwrap().trim(), 16)
                .unwrap_or(0);
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&data[line_end + 2..line_end + 2 + size]);
        data = &data[line_end + 2 + size + 2..];
    }
}

fn login(port: u16, username: &str) -> String {
    let form = format!("username={username}&password={PASSWORD}");
    let reply = http(
        port,
        "POST",
        "/login",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        form.as_bytes(),
    )
    .expect("login");
    format!(
        "Bearer {}",
        reply.json()["access_token"]
            .as_str()
            .unwrap_or_else(|| panic!("no token: {}", reply.text()))
    )
}

fn post(port: u16, auth: &str, path: &str, body: serde_json::Value) -> Reply {
    http(
        port,
        "POST",
        path,
        &[
            ("Authorization", auth),
            ("Content-Type", "application/json"),
        ],
        body.to_string().as_bytes(),
    )
    .unwrap_or_else(|| panic!("no reply from {path}"))
}

fn mcp_call(port: u16, auth: &str, action: &str) -> Reply {
    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "execute_action", "arguments": {"action": action}},
    });
    http(
        port,
        "POST",
        "/mcp",
        &[
            ("Authorization", auth),
            ("Content-Type", "application/json"),
            ("Accept", "application/json, text/event-stream"),
        ],
        body.to_string().as_bytes(),
    )
    .expect("mcp reply")
}

/// Wait up to `within` for `path` to appear.
fn appears(path: &Path, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    path.exists()
}

#[test]
fn a_non_administrator_never_reaches_the_shell_and_an_administrator_still_does() {
    let home = Home::new();
    // The first account of the instance becomes its administrator.
    home.register_user("root", true, &[]);
    home.register_user(
        "bob",
        false,
        &[
            "actions:execute",
            "jobs:execute",
            "jobs:write",
            "jobs:read",
            "pipes:read",
            "pipes:write",
            "pipes:execute",
        ],
    );
    let server = serve(&home);
    let port = server.port;
    let bob = login(port, "bob");

    // A Meerschaum action still runs for bob (the routes are open to him).
    let ok = post(
        port,
        &bob,
        "/execute/validate",
        json!({"sysargs": ["show", "version"]}),
    );
    assert_eq!(ok.status, 200, "{}", ok.text());

    let mut refused: Vec<(String, PathBuf, Reply)> = Vec::new();
    let mut ask = |label: &str, mark: PathBuf, reply: Reply| {
        refused.push((label.to_string(), mark, reply));
    };
    let touch = |name: &str| home.mark(name).to_string_lossy().into_owned();

    for (i, (name, sysargs)) in [
        ("execute", vec!["touch".to_string(), touch("execute")]),
        (
            "execute-chain",
            vec![
                "show".into(),
                "version".into(),
                "+".into(),
                "touch".into(),
                touch("execute-chain"),
            ],
        ),
        (
            "execute-bang",
            vec!["!touch".to_string(), touch("execute-bang")],
        ),
        (
            "execute-pipeline",
            vec![
                "touch".into(),
                touch("execute-pipeline"),
                ":".into(),
                "--debug".into(),
            ],
        ),
        (
            "execute-serialized",
            vec![
                "chain".into(),
                "-P".into(),
                json!({
                    "sub_args_line":
                        format!("show version + touch {}", touch("execute-serialized"))
                })
                .to_string(),
            ],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let reply = post(
            port,
            &bob,
            "/execute",
            json!({"sysargs": sysargs, "name": format!("execute-tpr-{i}")}),
        );
        ask(
            &format!("POST /execute {sysargs:?}"),
            home.mark(name),
            reply,
        );
    }

    let mark = home.mark("jobs");
    let reply = post(
        port,
        &bob,
        "/jobs/execute-tpr-job",
        json!({"sysargs": ["touch", mark.to_string_lossy()], "properties": {}}),
    );
    ask("POST /jobs/{name}", mark, reply);

    // A job turns `++` back into `+`.
    let mark = home.mark("jobs-escaped");
    let reply = post(
        port,
        &bob,
        "/jobs/execute-tpr-job-escaped",
        json!({
            "sysargs": ["show", "version", "++", "touch", mark.to_string_lossy()],
            "properties": {},
        }),
    );
    ask("POST /jobs/{name} (escaped chain)", mark, reply);

    let mark = home.mark("validate");
    let reply = post(
        port,
        &bob,
        "/execute/validate",
        json!({"sysargs": ["touch", mark.to_string_lossy()]}),
    );
    ask("POST /execute/validate", mark, reply);

    let mark = home.mark("actions");
    let reply = post(
        port,
        &bob,
        "/actions/show",
        json!({"action": ["version", "+", "touch", mark.to_string_lossy()]}),
    );
    ask("POST /actions/show (chained)", mark, reply);

    let mark = home.mark("mcp");
    let reply = mcp_call(
        port,
        &bob,
        &format!("show version + touch {}", mark.to_string_lossy()),
    );
    ask("MCP execute_action (chained)", mark, reply);

    let mut reached = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    for (label, mark, reply) in &refused {
        if appears(mark, deadline.saturating_duration_since(Instant::now())) {
            reached.push(format!("{label}: {} {}", reply.status, reply.text()));
        }
    }
    assert!(
        reached.is_empty(),
        "a non-administrator reached the shell:\n{}",
        reached.join("\n")
    );
    // The validation route says no, with a clear reason.
    let (_, _, validate) = refused
        .iter()
        .find(|(label, _, _)| label == "POST /execute/validate")
        .unwrap();
    assert_eq!(validate.status, 403, "{}", validate.text());
    assert!(
        validate.text().contains("not a Meerschaum action"),
        "{}",
        validate.text()
    );

    // An administrator is unaffected.
    let root = login(port, "root");
    let mark = home.mark("admin");
    let reply = post(
        port,
        &root,
        "/execute",
        json!({"sysargs": ["touch", mark.to_string_lossy()], "name": "execute-tpr-admin"}),
    );
    assert!(
        appears(&mark, Duration::from_secs(30)),
        "{} {}\n{}",
        reply.status,
        reply.text(),
        std::fs::read_to_string(&server.stderr).unwrap_or_default()
    );
}
