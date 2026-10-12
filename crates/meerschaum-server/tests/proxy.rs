//! The extension-host proxy (§7.3) against a tiny HTTP server on a Unix socket: unmatched
//! routes are forwarded with method, path, query, headers, cookies and body unchanged; bodies
//! stream; websockets are bridged; the supervisor starts the host on demand and restarts it
//! after a crash.

mod common;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::{any, get};
use common::{SECRET, req, send};
use futures_util::{SinkExt, StreamExt};
use meerschaum_server::AppState;
use meerschaum_server::host::{
    HostLauncher, HostManifest, HostProcess, HostSupervisor, SupervisorOptions,
};
use meerschaum_server::proxy::HostProxy;
use tokio::sync::oneshot;

/// The fake API host: a few plugin routes.
fn host_app(generation: usize) -> Router {
    Router::new()
        .route(
            "/plugin/echo",
            any(move |method: axum::http::Method, uri: Uri, headers: HeaderMap, body: Bytes| async move {
                let h = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
                (
                    [("x-host-generation", generation.to_string())],
                    format!(
                        "{method} {uri} cookie={} x-custom={} connection={} body={}",
                        h("cookie"),
                        h("x-custom"),
                        h("connection"),
                        String::from_utf8_lossy(&body)
                    ),
                )
            }),
        )
        .route(
            "/plugin/stream",
            get(|| async {
                let chunks = futures_util::stream::iter(
                    (0..5).map(|i| Ok::<_, std::io::Error>(Bytes::from(format!("chunk{i};")))),
                );
                Body::from_stream(chunks)
            }),
        )
        .route(
            "/plugin/ws",
            get(|ws: WebSocketUpgrade| async move {
                ws.on_upgrade(|mut socket| async move {
                    while let Some(Ok(Message::Text(t))) = socket.recv().await {
                        let _ = socket.send(Message::Text(format!("echo:{t}").into())).await;
                    }
                })
                .into_response()
            }),
        )
}

async fn serve_host(socket: &Path, generation: usize) -> tokio::task::JoinHandle<()> {
    let _ = std::fs::remove_file(socket);
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, host_app(generation)).await;
    })
}

fn app_with_proxy(proxy: HostProxy) -> (Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let state = AppState::builder(common::config(dir.path()), instance)
        .secret(SECRET.to_vec())
        .host(Arc::new(proxy))
        .build()
        .unwrap();
    (meerschaum_server::app(state), dir)
}

#[tokio::test]
async fn forwards_unmatched_requests_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("host.sock");
    let _host = serve_host(&socket, 1).await;
    let (app, _root) = app_with_proxy(HostProxy::new(&socket));

    let r = send(
        &app,
        axum::http::Request::builder()
            .method("PATCH")
            .uri("/plugin/echo?a=1&b=%20x")
            .header("cookie", "mrsm-session-id=abc")
            .header("x-custom", "yes")
            .header("connection", "keep-alive")
            .body(Body::from("payload"))
            .unwrap(),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(
        r.text(),
        "PATCH /plugin/echo?a=1&b=%20x cookie=mrsm-session-id=abc x-custom=yes connection= body=payload"
    );
    assert_eq!(r.headers["x-host-generation"], "1");

    let r = send(&app, req("GET", "/plugin/stream", None, None)).await;
    assert_eq!(r.text(), "chunk0;chunk1;chunk2;chunk3;chunk4;");

    // The host's own 404 comes back as-is; Rust routes are never forwarded.
    let r = send(&app, req("GET", "/plugin/nope", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(!r.unmatched);
    let r = send(&app, req("GET", "/version", None, None)).await;
    assert!(r.headers.get("x-host-generation").is_none());
}

#[tokio::test]
async fn bridges_websockets() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("host.sock");
    let _host = serve_host(&socket, 1).await;
    let (app, _root) = app_with_proxy(HostProxy::new(&socket));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/plugin/ws"))
        .await
        .unwrap();
    use tokio_tungstenite::tungstenite::Message as M;
    for word in ["a", "bb"] {
        ws.send(M::Text(word.into())).await.unwrap();
        let reply = ws.next().await.unwrap().unwrap();
        assert_eq!(reply, M::Text(format!("echo:{word}").into()));
    }
}

#[tokio::test]
async fn unavailable_host_is_a_bad_gateway() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _root) = app_with_proxy(HostProxy::new(dir.path().join("missing.sock")));
    let r = send(&app, req("GET", "/plugin/echo", None, None)).await;
    assert_eq!(r.status, StatusCode::BAD_GATEWAY);
    assert_eq!(r.json()["detail"], "The API host is unavailable.");
}

/// A fake host process: an in-process server on the socket that can be crashed.
struct FakeProcess {
    server: tokio::task::JoinHandle<()>,
    crash: oneshot::Receiver<()>,
}

#[async_trait]
impl HostProcess for FakeProcess {
    async fn wait(&mut self) -> std::io::Result<Option<i32>> {
        tokio::select! {
            _ = &mut self.server => {}
            _ = &mut self.crash => self.server.abort(),
        }
        Ok(Some(1))
    }
    async fn kill(&mut self) -> std::io::Result<()> {
        self.server.abort();
        Ok(())
    }
}

#[derive(Default)]
struct FakeLauncher {
    launches: Mutex<usize>,
    crash: Mutex<Option<oneshot::Sender<()>>>,
}

#[async_trait]
impl HostLauncher for FakeLauncher {
    async fn launch(&self, socket: &Path) -> std::io::Result<Box<dyn HostProcess>> {
        let generation = {
            let mut n = self.launches.lock().unwrap();
            *n += 1;
            *n
        };
        let server = serve_host(socket, generation).await;
        let (tx, rx) = oneshot::channel();
        *self.crash.lock().unwrap() = Some(tx);
        Ok(Box::new(FakeProcess { server, crash: rx }))
    }
}

fn opts() -> SupervisorOptions {
    SupervisorOptions {
        ready_timeout: Duration::from_secs(5),
        backoff_initial: Duration::from_millis(10),
        backoff_max: Duration::from_millis(50),
    }
}

#[tokio::test]
async fn supervisor_starts_on_demand_and_restarts_on_crash() {
    let dir = tempfile::tempdir().unwrap();
    let socket: PathBuf = dir.path().join("api").join("host.sock");
    let launcher = Arc::new(FakeLauncher::default());

    assert!(
        HostSupervisor::for_manifest(&HostManifest::default(), launcher.clone(), &socket, opts())
            .is_none(),
        "no plugin needs the host: it never starts"
    );
    let manifest = HostManifest {
        api_plugins: vec!["example".into()],
        ..Default::default()
    };
    let sup = HostSupervisor::for_manifest(&manifest, launcher.clone(), &socket, opts()).unwrap();
    assert_eq!(sup.starts(), 0);
    let (app, _root) = app_with_proxy(HostProxy::supervised(sup.clone()));

    let r = send(&app, req("GET", "/plugin/echo", None, None)).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.headers["x-host-generation"], "1");
    assert_eq!(sup.starts(), 1);

    let crash = launcher.crash.lock().unwrap().take().unwrap();
    crash.send(()).unwrap();
    let mut restarted = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if sup.starts() == 2 {
            restarted = true;
            break;
        }
    }
    assert!(restarted, "the host was not restarted");
    let r = send(&app, req("GET", "/plugin/echo", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.headers["x-host-generation"], "2");
    sup.shutdown().await;
}
