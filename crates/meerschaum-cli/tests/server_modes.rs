//! Process-level checks for the API and MCP entry points, isolated from user configuration.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill")
                .args(["-INT", &self.0.id().to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            let deadline = Instant::now() + Duration::from_secs(2);
            while self.0.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
        // A failed assertion must also reap a worker or host which was still starting.
        #[cfg(unix)]
        let _ = Command::new("kill")
            .args(["-KILL", "--", &format!("-{}", self.0.id())])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

struct Sandbox {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}
impl Sandbox {
    fn new() -> Self {
        Self {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm"));
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        command
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_NOASK", "1")
            .env(
                "PYTHONPATH",
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap(),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }
}

fn await_exit(child: &mut Child, seconds: u64) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "mrsm exited with {status}");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "mrsm did not exit before timeout"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn mcp_stdio_stdout_contains_only_protocol_messages() {
    let sandbox = Sandbox::new();
    let mut child = ChildGuard(
        sandbox
            .command()
            .args(["--noask", "start", "mcp", "-i", "sql:local"])
            .stdin(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut stdin = child.0.stdin.take().unwrap();
    writeln!(
        stdin,
        "{}",
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    )
    .unwrap();
    drop(stdin);
    await_exit(&mut child.0, 20);
    let mut stdout = String::new();
    child
        .0
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(lines.len(), 1, "unexpected stdout: {stdout}");
    let response: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(response["id"], 1);
    assert!(response.get("result").is_some(), "{response}");
}

fn request(port: u16, method: &str, path: &str, body: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(stream, "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: {}\r\n\r\n{body}", body.len()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

#[test]
fn api_serves_health_and_mcp_on_an_isolated_sqlite_instance() {
    let sandbox = Sandbox::new();
    let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    assert_ne!(port, 8989);
    drop(socket);
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
                "--no-auth",
                "--no-dash",
                "-w",
                "2",
            ])
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "server exited before listening"
        );
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "server did not listen before timeout"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let health = request(port, "GET", "/healthcheck", "");
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
    let body = serde_json::json!({"jsonrpc": "2.0", "id": 7, "method": "initialize", "params": {}})
        .to_string();
    let mcp = request(port, "POST", "/mcp", &body);
    assert!(mcp.starts_with("HTTP/1.1 200"), "{mcp}");
    assert!(
        mcp.contains("\"id\":7") && mcp.contains("\"result\""),
        "{mcp}"
    );
}

#[test]
fn api_serves_the_dash_console_unless_no_dash() {
    for no_dash in [false, true] {
        let sandbox = Sandbox::new();
        let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = socket.local_addr().unwrap().port();
        assert_ne!(port, 8989);
        drop(socket);
        let mut args = vec![
            "start".to_string(),
            "api".into(),
            "-i".into(),
            "sql:local".into(),
            "--host".into(),
            "127.0.0.1".into(),
            "-p".into(),
            port.to_string(),
            "-w".into(),
            "2".into(),
        ];
        if no_dash {
            args.push("--nodash".into());
        }
        let mut child = ChildGuard(sandbox.command().args(&args).spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "server exited before listening"
            );
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            assert!(Instant::now() < deadline, "server did not listen");
            std::thread::sleep(Duration::from_millis(20));
        }
        let login = request(port, "GET", "/dash/login", "");
        if no_dash {
            assert!(!login.starts_with("HTTP/1.1 200"), "{login}");
        } else {
            assert!(login.starts_with("HTTP/1.1 200"), "{login}");
            let dash = request(port, "GET", "/dash", "");
            assert!(dash.starts_with("HTTP/1.1 303"), "{dash}");
            assert!(dash.contains("location: /dash/login"), "{dash}");
        }
        let index = request(port, "GET", "/", "");
        let target = if no_dash { "/docs" } else { "/dash" };
        assert!(index.contains(&format!("location: {target}")), "{index}");
    }
}

#[test]
#[cfg(unix)]
fn api_host_proxies_plugin_routes_and_federates_mcp_tools() {
    let sandbox = Sandbox::new();
    let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let python = std::env::var_os("MRSM_PYTHON")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| repository.join(".venv-ref/bin/python"));
    let available = Command::new(&python)
        .args(["-c", "import pyarrow, fastapi, uvicorn"])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", sandbox.root.path())
        .env("MRSM_ROOT_DIR", sandbox.root.path())
        .env("MRSM_PLUGINS_DIR", sandbox.plugins.path())
        .env("PYTHONPATH", repository)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if !available {
        eprintln!(
            "Skipping Python host integration: set MRSM_PYTHON to an interpreter with pyarrow, fastapi and uvicorn."
        );
        return;
    }
    std::fs::write(
        sandbox.plugins.path().join("wiring_host.py"),
        r#"
from meerschaum.plugins import api_plugin
from meerschaum.mcp import tool
@api_plugin
def wiring_routes(app):
    print("wiring host diagnostic", flush=True)
    @app.get("/wiring-host-check")
    def check():
        return {"host": "wiring"}
@tool("wiring_echo", "Synthetic wiring probe", read_only=True)
def wiring_echo(**kw):
    return {"wiring": True}
"#,
    )
    .unwrap();
    let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    assert_ne!(port, 8989);
    drop(socket);
    let mut child = ChildGuard(
        sandbox
            .command()
            .env("MRSM_PYTHON", &python)
            .env("UV_PYTHON", &python)
            .args([
                "start",
                "api",
                "-i",
                "sql:local",
                "--host",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "--no-auth",
                "--no-dash",
            ])
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "server exited before listening"
        );
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            let response = request(port, "GET", "/wiring-host-check", "");
            if response.starts_with("HTTP/1.1 200") {
                assert!(response.contains("wiring"), "{response}");
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "Python host did not start before timeout"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let list = request(
        port,
        "POST",
        "/mcp",
        &serde_json::json!({"jsonrpc":"2.0", "id":8,"method":"tools/list","params":{}}).to_string(),
    );
    assert!(
        list.starts_with("HTTP/1.1 200") && list.contains("wiring_echo"),
        "{list}"
    );
    let call = request(port, "POST", "/mcp", &serde_json::json!({"jsonrpc":"2.0", "id":9,"method":"tools/call","params":{"name":"wiring_echo","arguments":{}}}).to_string());
    assert!(
        call.starts_with("HTTP/1.1 200") && call.contains("wiring"),
        "{call}"
    );
    assert!(!call.contains("\"isError\":true"), "{call}");
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    await_exit(&mut child.0, 10);
    assert!(!sandbox.root.path().join("api/host/host.sock").exists());
    drop(child);
    let mut stdio = ChildGuard(
        sandbox
            .command()
            .env("MRSM_PYTHON", &python)
            .env("UV_PYTHON", &python)
            .args(["start", "mcp", "-i", "sql:local"])
            .stdin(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut input = stdio.0.stdin.take().unwrap();
    for message in [
        serde_json::json!({"jsonrpc":"2.0", "id":10, "method":"initialize", "params":{}}),
        serde_json::json!({"jsonrpc":"2.0", "id":11, "method":"tools/call", "params":{"name":"wiring_echo", "arguments":{}}}),
    ] {
        writeln!(input, "{message}").unwrap();
    }
    drop(input);
    await_exit(&mut stdio.0, 30);
    let mut stdout = String::new();
    stdio
        .0
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();
    let responses: Vec<serde_json::Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(responses.len(), 2, "{stdout}");
    assert_eq!(responses[1]["id"], 11);
    assert!(responses[1].get("result").is_some(), "{stdout}");
    assert!(
        responses[1]["result"].to_string().contains("wiring"),
        "{stdout}"
    );
    assert_ne!(responses[1]["result"]["isError"], true, "{stdout}");
}
