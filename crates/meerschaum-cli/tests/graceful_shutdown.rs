//! `start api` drains on `SIGTERM` / `SIGINT` (`api:shutdown:grace_seconds`): the listener stops
//! accepting, a request in progress finishes, the process exits once it has (well before the
//! grace period ends), the grace period bounds the wait, and a second signal stops at once.
//!
//! The request in progress is a `POST /mcp` whose body the client sends in two parts, the second
//! after the signal: an existing route, no test hook in the server.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
        // The process group: a host or worker which was still starting.
        let _ = Command::new("kill")
            .args(["-KILL", "--", &format!("-{}", self.0.id())])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

struct Server {
    child: ChildGuard,
    port: u16,
    _root: tempfile::TempDir,
    _plugins: tempfile::TempDir,
}

impl Server {
    /// A server with `api:shutdown:grace_seconds` (`None`: the default).
    fn start(grace_seconds: impl Into<Option<f64>>) -> Server {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let socket = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let config = match grace_seconds.into() {
            Some(grace) => serde_json::json!({"api": {"shutdown": {"grace_seconds": grace}}}),
            None => serde_json::json!({}),
        };
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
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
                &port.to_string(),
                "--no-auth",
                "--no-dash",
            ])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", root.path())
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", plugins.path())
            .env("MRSM_NOASK", "1")
            .env("MRSM_NO_AUTO_INSTALL", "1")
            .env("MRSM_CONFIG", config.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut server = Server {
            child: ChildGuard(child),
            port,
            _root: root,
            _plugins: plugins,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(
                server.child.0.try_wait().unwrap().is_none(),
                "the server exited before listening"
            );
            if request(port, "GET", "/healthcheck/ready", "")
                .is_ok_and(|r| r.starts_with("HTTP/1.1 200"))
            {
                break;
            }
            assert!(Instant::now() < deadline, "the server did not get ready");
            std::thread::sleep(Duration::from_millis(50));
        }
        server
    }

    fn signal(&self, name: &str) {
        assert!(
            Command::new("kill")
                .args([&format!("-{name}"), &self.child.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
    }

    /// Wait for the process to exit; how long it took.
    fn wait_exit(&mut self, seconds: u64) -> Duration {
        let begin = Instant::now();
        let deadline = begin + Duration::from_secs(seconds);
        loop {
            if let Some(status) = self.child.0.try_wait().unwrap() {
                assert!(status.success(), "mrsm exited with {status}");
                return begin.elapsed();
            }
            assert!(Instant::now() < deadline, "the server did not exit");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

fn request(port: u16, method: &str, path: &str, body: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

const PING: &str = r#"{"jsonrpc": "2.0", "id": 7, "method": "ping"}"#;

/// A `POST /mcp` in progress: its headers and the first half of its body are sent.
fn begin_slow_request(port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    write!(
        stream,
        "POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        PING.len(),
        &PING[..10]
    )
    .unwrap();
    stream.flush().unwrap();
    // The server has the request (its handler is reading the body).
    std::thread::sleep(Duration::from_millis(300));
    stream
}

fn finish_slow_request(mut stream: TcpStream) -> String {
    stream.write_all(&PING.as_bytes()[10..]).unwrap();
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    response
}

#[test]
fn sigterm_finishes_the_request_in_progress_then_exits() {
    let mut server = Server::start(30.0);
    let port = server.port;
    let slow = begin_slow_request(port);
    server.signal("TERM");
    std::thread::sleep(Duration::from_millis(500));
    // Draining: a new connection is refused, or (if it got in) told the server is not ready.
    match request(port, "GET", "/healthcheck/ready", "") {
        Err(_) => {}
        Ok(response) => assert!(
            response.starts_with("HTTP/1.1 503") && response.contains("shutting down"),
            "{response}"
        ),
    }
    assert!(server.child.0.try_wait().unwrap().is_none(), "exited early");
    let response = finish_slow_request(slow);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.contains(r#""id": 7"#) || response.contains(r#""id":7"#),
        "{response}"
    );
    // The process exits once the request is done, not when the grace period ends.
    let waited = server.wait_exit(15);
    assert!(waited < Duration::from_secs(10), "{waited:?}");
}

#[test]
fn the_grace_period_bounds_the_drain() {
    let mut server = Server::start(2.0);
    let slow = begin_slow_request(server.port);
    let signalled = Instant::now();
    server.signal("TERM");
    server.wait_exit(20);
    let waited = signalled.elapsed();
    assert!(
        waited >= Duration::from_millis(1800) && waited < Duration::from_secs(10),
        "{waited:?}"
    );
    // The request that never finished was cut off.
    let response = finish_slow_request(slow);
    assert!(!response.starts_with("HTTP/1.1 200"), "{response}");
}

#[test]
fn a_second_signal_stops_at_once() {
    let mut server = Server::start(30.0);
    let _slow = begin_slow_request(server.port);
    server.signal("TERM");
    std::thread::sleep(Duration::from_millis(500));
    let signalled = Instant::now();
    server.signal("INT");
    server.wait_exit(20);
    assert!(
        signalled.elapsed() < Duration::from_secs(8),
        "{:?}",
        signalled.elapsed()
    );
}

#[test]
fn no_grace_stops_at_once() {
    let mut server = Server::start(0.0);
    let slow = begin_slow_request(server.port);
    let signalled = Instant::now();
    server.signal("TERM");
    server.wait_exit(20);
    assert!(
        signalled.elapsed() < Duration::from_secs(8),
        "{:?}",
        signalled.elapsed()
    );
    let response = finish_slow_request(slow);
    assert!(!response.starts_with("HTTP/1.1 200"), "{response}");
}

/// The grace period is an upper bound, never a wait: an idle server exits at once.
#[test]
fn an_idle_server_exits_at_once_with_the_default_grace() {
    let mut server = Server::start(None);
    let signalled = Instant::now();
    server.signal("TERM");
    server.wait_exit(20);
    assert!(
        signalled.elapsed() < Duration::from_secs(2),
        "{:?}",
        signalled.elapsed()
    );
}
