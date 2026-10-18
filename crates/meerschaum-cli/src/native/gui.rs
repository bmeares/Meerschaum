//! `start gui` (`meerschaum/actions/start.py::_start_gui`): run the API server in the background
//! and open the console in a `pywebview` window.
//!
//! v4 started the Tornado web terminal and put the window around `/webterm/mrsm`. In v5 the
//! terminal belongs to the authenticated console, so the GUI starts a local no-auth API
//! (`start api --no-auth`) and the window shows the console at `/dash/`. The server is bound to
//! `127.0.0.1` only, on a free port chosen at start, and is stopped with SIGTERM (SIGKILL after a
//! grace period) when the window closes.
//!
//! SECURITY: the console is not authenticated while the window is open, so any local process can
//! reach it on that loopback port. The server has no one-time-token mode, and the GUI is a
//! single-user desktop feature; use `start api` (with authentication) when the machine is shared.
//! The window itself is `pywebview`, a Python package from the `gui` extra: it is the
//! only part that runs Python, as `python -c` of the interpreter `mrsm` resolves for workers.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use meerschaum_core::SuccessTuple;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::value::ArgsMap;

use super::{flag, int};

/// The webview window script (arguments: URL, debug flag).
const WINDOW: &str = "import sys, webview\n\
webview.create_window('Meerschaum Shell', sys.argv[1], height=650, width=1000)\n\
webview.start(debug=(sys.argv[2] == '1'))\n";

/// `GET <path>` over a plain socket: whether the server answered with a success status.
fn http_ok(host: &str, port: u16, path: &str) -> bool {
    let Ok(mut addrs) = (host, port).to_socket_addrs() else {
        return false;
    };
    let Some(addr) = addrs.next() else {
        return false;
    };
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(500)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let request = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut head = [0u8; 16];
    if stream.read(&mut head).is_err() {
        return false;
    }
    let head = String::from_utf8_lossy(&head);
    head.split_whitespace()
        .nth(1)
        .is_some_and(|code| code.starts_with('2') || code.starts_with('3'))
}

/// The loopback address the GUI's server binds (never the configured webterm host).
const HOST: &str = "127.0.0.1";

/// A free port on the loopback interface (the OS picks it).
fn free_port() -> Option<u16> {
    TcpListener::bind((HOST, 0))
        .ok()?
        .local_addr()
        .ok()
        .map(|a| a.port())
}

/// Stop the server: SIGTERM, up to `grace` for it to exit, then SIGKILL.
fn stop_server(child: &mut std::process::Child, grace: Duration) {
    let _ = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// `start gui`.
pub fn start_gui(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let debug = flag(args, "debug");
    let host = HOST;
    let instance = super::instance_keys(args, rt);

    let python = crate::builtin::python_executable(rt, None);
    let probe = Command::new(&python)
        .args(["-c", "import webview"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if !probe.is_ok_and(|s| s.success()) {
        return Ok(SuccessTuple::fail(
            "`start gui` needs the `gui` extra (`pywebview`): install it with \
             `pip install \"meerschaum[gui]\"`, or set `MRSM_PYTHON` to an interpreter that has it.",
        ));
    }
    let port = match int(args, "port").and_then(|p| u16::try_from(p).ok()) {
        Some(port) => Some(port),
        None => free_port(),
    };
    let Some(port) = port else {
        return Ok(SuccessTuple::fail(
            "Could not find an open port for the GUI's server.",
        ));
    };
    let (program, leading) = meerschaum_core::self_exe::self_program();
    let mut server = Command::new(program);
    server
        .args(leading)
        .args(["start", "api", "--no-auth", "--host", host, "--port"])
        .arg(port.to_string())
        .args(["-i", &instance])
        .stdin(Stdio::null());
    if !debug {
        server.stdout(Stdio::null()).stderr(Stdio::null());
    }
    for (key, value) in (rt.vars)() {
        server.env(key, value);
    }
    let mut child = server.spawn().map_err(|e| {
        crate::builtin::ActionError::new("OSError", format!("Failed to start the server: {e}"))
    })?;
    let timeout = Duration::from_secs(10);
    let start = Instant::now();
    let mut up = false;
    while start.elapsed() < timeout {
        std::thread::sleep(Duration::from_millis(100));
        if child.try_wait().ok().flatten().is_some() {
            break;
        }
        if http_ok(host, port, "/dash/") {
            up = true;
            break;
        }
    }
    if !up {
        stop_server(&mut child, Duration::from_secs(3));
        return Ok(SuccessTuple::fail(format!(
            "The web terminal failed to start within {} seconds.",
            timeout.as_secs()
        )));
    }
    let url = format!("http://127.0.0.1:{port}/dash/");
    let status = Command::new(&python)
        .args(["-c", WINDOW, &url, if debug { "1" } else { "0" }])
        .status();
    stop_server(&mut child, Duration::from_secs(5));
    match status {
        Ok(s) if s.success() => Ok(SuccessTuple::ok("Success")),
        Ok(s) => Ok(SuccessTuple::fail(format!(
            "The GUI window exited with code {}.",
            s.code().unwrap_or(1)
        ))),
        Err(e) => Ok(SuccessTuple::fail(e.to_string())),
    }
}
