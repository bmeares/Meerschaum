//! `GET /engine/runs/{run_id}/events/stream` follows an engine run that `mrsm start api` runs
//! in its own process (`POST /actions/sync`) live: the events come from the engine as they are
//! emitted, not only from the persisted tail. With `system.engine.events: nodes` the chunk and
//! planning events are never persisted, so seeing them proves the live path.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        #[cfg(unix)]
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill")
                .args(["-INT", &self.0.id().to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(3);
            while self.0.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn command(root: &std::path::Path, plugins: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", root)
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_PLUGINS_DIR", plugins)
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env(
            "MRSM_CONFIG",
            r#"{"system": {"engine": {"events": "nodes"}}}"#,
        );
    command
}

fn post(port: u16, path: &str, body: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

/// Read the stream's raw response until `run_finished` (or the deadline).
fn follow(port: u16, path: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n"
    )
    .unwrap();
    let mut out = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut buf = [0u8; 4096];
    while Instant::now() < deadline {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(_) => {}
        }
        if String::from_utf8_lossy(&out).contains("event: run_finished") {
            break;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[test]
fn actions_run_in_the_server_stream_live() {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let registered = command(root.path(), plugins.path())
        .args([
            "register",
            "pipe",
            "-c",
            "sql:local",
            "-m",
            "live_events",
            "-i",
            "sql:local",
            "--noask",
            "--nopretty",
            "--params",
            r#"{"fetch": {"definition": "SELECT 1 AS id, 2.0 AS v"}, "columns": {"id": "id"}}"#,
        ])
        .output()
        .unwrap();
    assert!(registered.status.success(), "{registered:?}");

    let port = {
        let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        socket.local_addr().unwrap().port()
    };
    assert_ne!(port, 8989);
    let mut cmd = command(root.path(), plugins.path());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let stderr = root.path().join("server.err");
    let mut server = Server(
        cmd.args([
            "--noask",
            "start",
            "api",
            "-i",
            "sql:local",
            "--host",
            "127.0.0.1",
            "-p",
        ])
        .arg(port.to_string())
        .args(["--no-dash", "--no-auth", "-w", "2"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .spawn()
        .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            server.0.try_wait().unwrap().is_none(),
            "server exited: {}",
            std::fs::read_to_string(&stderr).unwrap_or_default()
        );
        assert!(Instant::now() < deadline, "server did not listen in time");
        std::thread::sleep(Duration::from_millis(50));
    }

    // Open the stream first (`?wait=`), as the console does, then run the sync in the server.
    let following =
        std::thread::spawn(move || follow(port, "/engine/runs/live-e2e-run/events/stream?wait=30"));
    std::thread::sleep(Duration::from_millis(500));
    let reply = post(
        port,
        "/actions/sync",
        r#"{"action": ["pipes"], "connector_keys": ["sql:local"], "metric_keys": ["live_events"],
            "mrsm_instance": "sql:local", "run_id": "live-e2e-run"}"#,
    );
    assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
    let streamed = following.join().unwrap();
    assert!(
        streamed.starts_with("HTTP/1.1 200"),
        "{streamed}\n{}",
        std::fs::read_to_string(&stderr).unwrap_or_default()
    );
    assert!(streamed.contains("event: run_started"), "{streamed}");
    assert!(streamed.contains("event: run_finished"), "{streamed}");
    // Not persisted with `events: nodes`: only the live path delivers it.
    assert!(streamed.contains("event: node_planned"), "{streamed}");
    drop(server);
}
