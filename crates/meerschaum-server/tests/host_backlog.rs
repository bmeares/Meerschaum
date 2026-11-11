//! A host that binds its port but does not accept (its listen backlog is full) is as hung as one
//! that never answers: every wait for it is bounded. A start whose readiness cannot connect fails
//! within `ready_timeout`, and a request to a host that stops accepting is answered (`502` or
//! `504`) within the readiness and response deadlines, never after the kernel's connect retries.
//! TCP loopback only: a Unix socket with a full backlog refuses a connection at once.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::http::StatusCode;
use common::{req, send};
use meerschaum_server::AppState;
use meerschaum_server::host::{
    HostEndpoint, HostLauncher, HostManifest, HostProcess, HostSupervisor, HostTransport,
    PROOF_PATH, SupervisorOptions, host_proof,
};
use meerschaum_server::proxy::HostProxy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};

const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

/// Connections opened until one does not connect within a moment: the accept queue is full.
async fn saturate(addr: SocketAddr) -> Vec<TcpStream> {
    let mut held = Vec::new();
    for _ in 0..64 {
        match tokio::time::timeout(Duration::from_millis(300), TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => held.push(stream),
            _ => return held,
        }
    }
    panic!("the listen backlog of {addr} never filled");
}

/// One HTTP/1.1 answer: the token proof on [`PROOF_PATH`], else `ok`.
async fn answer(mut stream: TcpStream, token: String) {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte).await {
            Ok(1) => head.push(byte[0]),
            _ => return,
        }
    }
    let head = String::from_utf8_lossy(&head);
    let target = head.split_whitespace().nth(1).unwrap_or("/").to_string();
    let body = match target.split_once('?') {
        Some((PROOF_PATH, query)) => {
            let nonce = query.strip_prefix("nonce=").unwrap_or("");
            host_proof(&token, nonce)
        }
        _ => "ok".to_string(),
    };
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

/// A host on TCP loopback with a small listen backlog. It accepts and answers until `accepting`
/// is cleared (then it holds its port and accepts nothing), or never accepts at all.
#[derive(Default)]
struct Backlogged {
    accepting: Arc<AtomicBool>,
    never: bool,
    addr: Mutex<Option<SocketAddr>>,
}

struct Process(tokio::task::JoinHandle<()>);

#[async_trait]
impl HostProcess for Process {
    async fn wait(&mut self) -> std::io::Result<Option<i32>> {
        let _ = (&mut self.0).await;
        Ok(Some(0))
    }

    async fn kill(&mut self) -> std::io::Result<()> {
        self.0.abort();
        Ok(())
    }
}

#[async_trait]
impl HostLauncher for Backlogged {
    async fn launch(&self, _socket: &Path) -> std::io::Result<Box<dyn HostProcess>> {
        Err(std::io::Error::other("this host listens on TCP only"))
    }

    async fn launch_endpoint(
        &self,
        endpoint: &HostEndpoint,
    ) -> std::io::Result<Box<dyn HostProcess>> {
        let HostEndpoint::Tcp { addr, token } = endpoint else {
            return Err(std::io::Error::other("this host listens on TCP only"));
        };
        let socket = TcpSocket::new_v4()?;
        socket.bind(*addr)?;
        // Both ensure_started and the supervisor probe readiness concurrently. Their TCP
        // connections must fit even before the accept task is scheduled; a backlog of one
        // can drop a probe's SYN, whose retransmission outlasts ready_timeout. saturate()
        // fills this queue explicitly when the test needs an unresponsive host.
        let listener = socket.listen(8)?;
        *self.addr.lock().unwrap() = Some(*addr);
        if self.never {
            let held = saturate(*addr).await;
            return Ok(Box::new(Process(tokio::spawn(async move {
                let _kept = (listener, held);
                std::future::pending::<()>().await;
            }))));
        }
        let (accepting, token) = (self.accepting.clone(), token.clone());
        Ok(Box::new(Process(tokio::spawn(async move {
            loop {
                if !accepting.load(Ordering::SeqCst) {
                    let _kept = &listener;
                    std::future::pending::<()>().await;
                }
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(answer(stream, token.clone()));
            }
        }))))
    }
}

fn app(launcher: Arc<Backlogged>) -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let sup = HostSupervisor::with_transport(
        &HostManifest::default().with_console(true),
        launcher,
        dir.path().join("host.sock"),
        SupervisorOptions {
            ready_timeout: Duration::from_secs(1),
            backoff_initial: Duration::from_millis(20),
            backoff_max: Duration::from_millis(100),
            ..SupervisorOptions::default()
        },
        HostTransport::Tcp,
    );
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    let mut cfg = common::config(dir.path());
    cfg.host_response_timeout = Some(Duration::from_millis(500));
    let state = AppState::builder(cfg, instance)
        .secret(SECRET.to_vec())
        .host(Arc::new(HostProxy::supervised(sup)))
        .build()
        .unwrap();
    (meerschaum_server::app(state), dir)
}

/// The request's status, and how long it took (at most `bound`).
async fn timed(app: &axum::Router, bound: Duration) -> (StatusCode, Duration) {
    let started = Instant::now();
    let r = tokio::time::timeout(bound, send(app, req("GET", "/plugin/ok", None, None)))
        .await
        .unwrap_or_else(|_| panic!("no answer within {bound:?}: a wait was not bounded"));
    (r.status, started.elapsed())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_that_never_accepts_fails_its_start_within_ready_timeout() {
    let launcher = Arc::new(Backlogged {
        never: true,
        ..Backlogged::default()
    });
    let (app, _dir) = app(launcher);
    let (status, _) = timed(&app, Duration::from_secs(8)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_that_stops_accepting_is_answered_within_the_deadlines() {
    let launcher = Arc::new(Backlogged::default());
    launcher.accepting.store(true, Ordering::SeqCst);
    let (app, _dir) = app(launcher.clone());
    let (status, _) = timed(&app, Duration::from_secs(8)).await;
    assert_eq!(status, StatusCode::OK);

    launcher.accepting.store(false, Ordering::SeqCst);
    let addr = launcher.addr.lock().unwrap().expect("the host listens");
    let _held = saturate(addr).await;
    let (status, _) = timed(&app, Duration::from_secs(8)).await;
    assert!(
        matches!(
            status,
            StatusCode::BAD_GATEWAY | StatusCode::GATEWAY_TIMEOUT
        ),
        "{status}"
    );
}
