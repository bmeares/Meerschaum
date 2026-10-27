//! The Dash console's seam in the server (D21): `/dash` and `/dash/*` are proxied to the Python
//! API host when it reports that it mounted the console (`GET /_mrsm/host/status`, else the
//! registry's `console` object) and serve the static pointer page otherwise; `--no-dash` serves
//! nothing there; the host's status is asked once per host process and again after a restart;
//! the TCP transport (Windows, or `MRSM_API_HOST_TRANSPORT=tcp`) carries a per-start token;
//! `POST /execute` starts an ephemeral job; the console probe reads the interpreter's answer.

mod common;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use common::{SECRET, req, send};
use meerschaum_core::{Map, SuccessTuple};
use meerschaum_server::AppState;
use meerschaum_server::backends::{CreateJob, JobsBackend};
use meerschaum_server::host::{
    ConsoleStatus, HostEndpoint, HostLauncher, HostManifest, HostProcess, HostSupervisor,
    HostTransport, ManifestSource, SupervisorOptions, WithConsole, probe_console,
};
use meerschaum_server::proxy::HostProxy;
use serde_json::{Value as Json, json};
use tokio::sync::oneshot;

/// What a fake host saw.
#[derive(Default)]
struct Seen {
    status_calls: AtomicUsize,
    registry_calls: AtomicUsize,
    dash_calls: AtomicUsize,
}

/// A fake API host app. `status`: the body of `/_mrsm/host/status` (`None`: a 404 as an older
/// host answers); `registry_console`: the `console` object of the registry.
fn host_app(status: Option<Json>, registry_console: Option<Json>, seen: Arc<Seen>) -> Router {
    let (s1, s2, s3) = (seen.clone(), seen.clone(), seen);
    Router::new()
        .route(
            "/_mrsm/host/status",
            get(move |headers: HeaderMap| {
                let (status, seen) = (status.clone(), s1.clone());
                async move {
                    seen.status_calls.fetch_add(1, Ordering::SeqCst);
                    assert!(
                        headers.get("x-mrsm-host-token").is_some(),
                        "the status call carries the internal token"
                    );
                    match status {
                        Some(body) => axum::Json(body).into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }
            }),
        )
        .route(
            "/_mrsm/host/registry",
            get(move || {
                let (console, seen) = (registry_console.clone(), s2.clone());
                async move {
                    seen.registry_calls.fetch_add(1, Ordering::SeqCst);
                    let mut doc = json!({"tools": [], "routes": []});
                    if let Some(console) = console {
                        doc["console"] = console;
                    }
                    axum::Json(doc)
                }
            }),
        )
        .route(
            "/dash/{*rest}",
            any(
                move |method: axum::http::Method, uri: Uri, headers: HeaderMap, body: Bytes| {
                    let seen = s3.clone();
                    async move {
                        seen.dash_calls.fetch_add(1, Ordering::SeqCst);
                        let h = |k: &str| {
                            headers
                                .get(k)
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("")
                                .to_string()
                        };
                        format!(
                            "{method} {uri} cookie={} body={}",
                            h("cookie"),
                            String::from_utf8_lossy(&body)
                        )
                    }
                },
            ),
        )
}

fn mounted() -> Json {
    json!({"console": {"mounted": true, "reason": null, "dash_version": "3.0.0"}})
}

fn unmounted(reason: &str) -> Json {
    json!({"console": {"mounted": false, "reason": reason, "dash_version": null}})
}

#[cfg(unix)]
async fn serve_unix(socket: &Path, app: Router) -> tokio::task::JoinHandle<()> {
    let _ = std::fs::remove_file(socket);
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    })
}

fn app_with(
    host: Option<HostProxy>,
    tweak: impl FnOnce(&mut meerschaum_server::ServerConfig),
) -> (Router, AppState, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let mut cfg = common::config(dir.path());
    tweak(&mut cfg);
    let mut builder = AppState::builder(cfg, instance).secret(SECRET.to_vec());
    if let Some(host) = host {
        builder = builder.host(Arc::new(host));
    }
    let state = builder.build().unwrap();
    (meerschaum_server::app(state.clone()), state, dir)
}

#[tokio::test]
async fn pointer_page_without_a_host() {
    let (app, _state, _root) = app_with(None, |_| {});
    for path in ["/dash", "/dash/"] {
        let r = send(&app, req("GET", path, None, None)).await;
        assert_eq!(r.status, StatusCode::OK, "{path}");
        let text = r.text();
        assert!(text.contains("pip install meerschaum[dash]"), "{text}");
        assert!(text.contains("The web console comes with the Python package"));
        assert_eq!(r.headers["cache-control"], "no-store");
    }
    // Below the root there is nothing to show.
    let r = send(&app, req("GET", "/dash/pipes", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = send(
        &app,
        req("POST", "/dash/_dash-update-component", None, None),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[cfg(unix)]
#[tokio::test]
async fn mounted_console_is_proxied_untouched_and_asked_once() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("host.sock");
    let seen = Arc::new(Seen::default());
    let _host = serve_unix(&socket, host_app(Some(mounted()), None, seen.clone())).await;
    let (app, _state, _root) = app_with(Some(HostProxy::new(&socket)), |_| {});

    let r = send(
        &app,
        axum::http::Request::builder()
            .method("POST")
            .uri("/dash/_dash-update-component?a=1")
            .header("cookie", "mrsm-session-id=abc")
            .body(Body::from("payload"))
            .unwrap(),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(
        r.text(),
        "POST /dash/_dash-update-component?a=1 cookie=mrsm-session-id=abc body=payload"
    );
    // The same host process is asked once, however many requests follow.
    for _ in 0..3 {
        let r = send(&app, req("GET", "/dash/pipes", None, None)).await;
        assert_eq!(r.status, StatusCode::OK);
    }
    assert_eq!(seen.status_calls.load(Ordering::SeqCst), 1);
    assert_eq!(seen.registry_calls.load(Ordering::SeqCst), 0);
    assert_eq!(seen.dash_calls.load(Ordering::SeqCst), 4);
    // Not the pointer page, whatever the root is: `/dash` itself is the host's too (the fake
    // serves only `/dash/{*rest}`, so its own 404 comes back as is).
    let r = send(&app, req("GET", "/dash/", None, None)).await;
    assert!(!r.text().contains("pip install"), "{}", r.text());
}

#[cfg(unix)]
#[tokio::test]
async fn unmounted_console_shows_the_pointer_page_with_the_reason() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("host.sock");
    let seen = Arc::new(Seen::default());
    let reason = "Dash is not installed <b>";
    let _host = serve_unix(
        &socket,
        host_app(Some(unmounted(reason)), None, seen.clone()),
    )
    .await;
    let (app, _state, _root) = app_with(Some(HostProxy::new(&socket)), |_| {});

    let r = send(&app, req("GET", "/dash", None, None)).await;
    assert_eq!(r.status, StatusCode::OK);
    let text = r.text();
    assert!(text.contains("pip install meerschaum[dash]"));
    // The reason is shown, escaped.
    assert!(text.contains("Dash is not installed &lt;b&gt;"), "{text}");
    assert!(!text.contains("<b>"));
    let r = send(&app, req("GET", "/dash/pipes", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(seen.dash_calls.load(Ordering::SeqCst), 0, "never forwarded");
    assert_eq!(seen.status_calls.load(Ordering::SeqCst), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn the_registry_console_object_is_the_fallback_signal() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("host.sock");
    let seen = Arc::new(Seen::default());
    let _host = serve_unix(
        &socket,
        host_app(None, Some(mounted()["console"].clone()), seen.clone()),
    )
    .await;
    let (app, _state, _root) = app_with(Some(HostProxy::new(&socket)), |_| {});
    let r = send(&app, req("GET", "/dash/pipes", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(seen.status_calls.load(Ordering::SeqCst), 1);
    assert_eq!(seen.registry_calls.load(Ordering::SeqCst), 1);

    // A host that says nothing at all mounted nothing.
    let socket2 = dir.path().join("host2.sock");
    let seen2 = Arc::new(Seen::default());
    let _host2 = serve_unix(&socket2, host_app(None, None, seen2.clone())).await;
    let (app2, state2, _root2) = app_with(Some(HostProxy::new(&socket2)), |_| {});
    let r = send(&app2, req("GET", "/dash", None, None)).await;
    assert!(r.text().contains("pip install meerschaum[dash]"));
    assert!(matches!(
        state2.host.as_ref().unwrap().console_status(),
        ConsoleStatus::Unavailable(_)
    ));
}

/// `/static` is the host's when a host runs (it mounts the console's files there); else the
/// server serves `api:static_path` itself.
#[cfg(unix)]
#[tokio::test]
async fn static_files_are_the_hosts_when_a_host_runs() {
    let dir = tempfile::tempdir().unwrap();
    let files = dir.path().join("static");
    std::fs::create_dir_all(&files).unwrap();
    std::fs::write(files.join("hello.txt"), "from the server").unwrap();
    let with_static =
        |cfg: &mut meerschaum_server::ServerConfig| cfg.static_path = Some(files.clone());

    // No host: the server's own mount.
    let (app, _state, _root) = app_with(None, with_static);
    let r = send(&app, req("GET", "/static/hello.txt", None, None)).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.text(), "from the server");

    // A host (here with no `/static` route of its own: its 404 comes back, never the server's file).
    let socket = dir.path().join("host.sock");
    let seen = Arc::new(Seen::default());
    let _host = serve_unix(&socket, host_app(Some(mounted()), None, seen)).await;
    let (app, _state, _root) = app_with(Some(HostProxy::new(&socket)), with_static);
    let r = send(&app, req("GET", "/static/hello.txt", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(!r.unmatched, "the host answered");
}

#[cfg(unix)]
#[tokio::test]
async fn no_dash_serves_nothing_under_dash() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("host.sock");
    let seen = Arc::new(Seen::default());
    let _host = serve_unix(&socket, host_app(Some(mounted()), None, seen.clone())).await;
    let (app, _state, _root) = app_with(Some(HostProxy::new(&socket)), |cfg| cfg.no_dash = true);
    for path in ["/dash", "/dash/", "/dash/pipes"] {
        let r = send(&app, req("GET", path, None, None)).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{path}");
        assert!(r.unmatched, "{path}");
    }
    assert_eq!(seen.status_calls.load(Ordering::SeqCst), 0);
    assert_eq!(seen.dash_calls.load(Ordering::SeqCst), 0);
}

/// A fake host process serving `app` on the endpoint it is given.
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

/// Launches fake hosts: generation 1 mounted nothing, later ones mount the console. Serves TCP
/// (requiring the per-start token) or a Unix socket, per the endpoint.
#[derive(Default)]
struct FakeLauncher {
    launches: Mutex<usize>,
    crash: Mutex<Option<oneshot::Sender<()>>>,
    tokens: Mutex<Vec<String>>,
}

async fn require_token(State(token): State<String>, req: Request, next: Next) -> Response {
    // The identity proof is answered without the token, like the real host's middleware.
    if req.uri().path() == meerschaum_server::host::PROOF_PATH {
        let nonce = req
            .uri()
            .query()
            .and_then(|q| q.strip_prefix("nonce="))
            .unwrap_or("")
            .to_string();
        return meerschaum_server::host::host_proof(&token, &nonce).into_response();
    }
    let ok = req
        .headers()
        .get("x-mrsm-host-proxy-token")
        .and_then(|v| v.to_str().ok())
        == Some(token.as_str());
    if ok {
        next.run(req).await
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}

impl FakeLauncher {
    async fn start(&self, endpoint: &HostEndpoint) -> std::io::Result<Box<dyn HostProcess>> {
        let generation = {
            let mut n = self.launches.lock().unwrap();
            *n += 1;
            *n
        };
        let status = if generation == 1 {
            unmounted("first process")
        } else {
            mounted()
        };
        let app = host_app(Some(status), None, Arc::new(Seen::default())).route(
            "/plugin/echo",
            get(|headers: HeaderMap| async move {
                headers
                    .get("x-mrsm-host-proxy-token")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string()
            }),
        );
        let server = match endpoint {
            HostEndpoint::Tcp { addr, token } => {
                self.tokens.lock().unwrap().push(token.clone());
                let app = app.layer(axum::middleware::from_fn_with_state(
                    token.clone(),
                    require_token,
                ));
                let listener = tokio::net::TcpListener::bind(addr).await?;
                tokio::spawn(async move {
                    let _ = axum::serve(listener, app).await;
                })
            }
            #[cfg(unix)]
            HostEndpoint::Unix(socket) => serve_unix(socket, app).await,
            #[cfg(not(unix))]
            HostEndpoint::Unix(_) => {
                return Err(std::io::Error::other("no Unix sockets here"));
            }
        };
        let (tx, rx) = oneshot::channel();
        *self.crash.lock().unwrap() = Some(tx);
        Ok(Box::new(FakeProcess { server, crash: rx }))
    }
}

#[async_trait]
impl HostLauncher for FakeLauncher {
    async fn launch(&self, socket: &Path) -> std::io::Result<Box<dyn HostProcess>> {
        self.start(&HostEndpoint::from(socket)).await
    }
    async fn launch_endpoint(
        &self,
        endpoint: &HostEndpoint,
    ) -> std::io::Result<Box<dyn HostProcess>> {
        self.start(endpoint).await
    }
}

fn opts() -> SupervisorOptions {
    SupervisorOptions {
        ready_timeout: Duration::from_secs(5),
        backoff_initial: Duration::from_millis(20),
        backoff_max: Duration::from_millis(100),
    }
}

fn console_manifest() -> HostManifest {
    HostManifest::default().with_console(true)
}

async fn crash_and_wait_for_restart(launcher: &FakeLauncher, sup: &HostSupervisor, starts: usize) {
    let crash = launcher.crash.lock().unwrap().take().unwrap();
    crash.send(()).unwrap();
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if sup.starts() == starts {
            return;
        }
    }
    panic!("the host was not restarted");
}

/// The status is per host process: a restart forgets it, and the next request asks again.
async fn status_follows_the_host_process(transport: HostTransport) {
    let dir = tempfile::tempdir().unwrap();
    let socket: PathBuf = dir.path().join("api").join("host.sock");
    let launcher = Arc::new(FakeLauncher::default());
    // The console reason alone starts the host (no plugin needs it).
    assert!(console_manifest().needs_host());
    assert_eq!(console_manifest().reasons(), vec!["console"]);
    let sup = HostSupervisor::with_transport(
        &console_manifest(),
        launcher.clone(),
        &socket,
        opts(),
        transport,
    );
    let proxy = HostProxy::supervised(sup.clone());
    let (app, _state, _root) = app_with(Some(proxy), |_| {});
    assert_eq!(sup.starts(), 0, "nothing starts before the first request");

    // Generation 1 mounted nothing: the pointer page, with its reason.
    let r = send(&app, req("GET", "/dash", None, None)).await;
    assert!(r.text().contains("first process"), "{}", r.text());
    assert_eq!(sup.starts(), 1);
    assert!(matches!(
        sup.console_status(),
        ConsoleStatus::Unavailable(_)
    ));
    let first = sup.endpoint();

    // After a crash the new process is asked again, and it mounted the console.
    crash_and_wait_for_restart(&launcher, &sup, 2).await;
    assert_eq!(sup.console_status(), ConsoleStatus::Unknown);
    let r = send(&app, req("GET", "/dash/pipes", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(sup.console_status(), ConsoleStatus::Mounted);
    if transport == HostTransport::Tcp {
        // A new port or token with every start.
        assert_ne!(first, sup.endpoint());
    }
    sup.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn console_status_follows_the_host_process_over_a_unix_socket() {
    status_follows_the_host_process(HostTransport::Unix).await;
}

#[tokio::test]
async fn console_status_follows_the_host_process_over_tcp() {
    status_follows_the_host_process(HostTransport::Tcp).await;
}

/// Without a needed host the pointer page says so and remembers nothing: a plugin installed later
/// makes the host needed, and the process that then starts is asked afresh.
#[tokio::test]
async fn a_host_that_becomes_needed_is_asked_afresh() {
    let dir = tempfile::tempdir().unwrap();
    let socket: PathBuf = dir.path().join("host.sock");
    let launcher = Arc::new(FakeLauncher::default());
    let sup = HostSupervisor::with_transport(
        &HostManifest::default(),
        launcher.clone(),
        &socket,
        opts(),
        HostTransport::from_env(),
    );
    let (app, _state, _root) = app_with(Some(HostProxy::supervised(sup.clone())), |_| {});
    let r = send(&app, req("GET", "/dash", None, None)).await;
    assert!(r.text().contains("pip install meerschaum[dash]"));
    assert!(r.text().contains("no API host is running"), "{}", r.text());
    assert_eq!(sup.starts(), 0, "nothing was started for the pointer page");
    assert_eq!(sup.console_status(), ConsoleStatus::Unknown);

    let plugin = HostManifest {
        api_plugins: vec!["portal".into()],
        ..Default::default()
    };
    assert!(sup.set_manifest(&plugin).await);
    let r = send(&app, req("GET", "/dash", None, None)).await;
    assert!(r.text().contains("first process"), "{}", r.text());
    assert_eq!(sup.starts(), 1);
    sup.shutdown().await;
}

#[tokio::test]
async fn tcp_transport_sends_its_own_token_and_the_host_refuses_without_it() {
    let dir = tempfile::tempdir().unwrap();
    let socket: PathBuf = dir.path().join("host.sock");
    let launcher = Arc::new(FakeLauncher::default());
    let sup = HostSupervisor::with_transport(
        &console_manifest(),
        launcher.clone(),
        &socket,
        opts(),
        HostTransport::Tcp,
    );
    let (app, _state, _root) = app_with(Some(HostProxy::supervised(sup.clone())), |_| {});

    // A client-supplied token is dropped; the proxy's own is what the host sees.
    let r = send(
        &app,
        axum::http::Request::builder()
            .uri("/plugin/echo")
            .header("x-mrsm-host-proxy-token", "evil")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let token = launcher.tokens.lock().unwrap()[0].clone();
    assert_eq!(r.text(), token);
    assert!(token.len() >= 32);

    // Anything else on the loopback port (a local process) is refused by the host.
    let HostEndpoint::Tcp { addr, .. } = sup.endpoint() else {
        panic!("not TCP");
    };
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(b"GET /plugin/echo HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.unwrap();
    assert!(answer.starts_with("HTTP/1.1 403"), "{answer}");
    sup.shutdown().await;
}

#[tokio::test]
async fn the_console_reason_is_a_property_of_the_process_not_the_manifest_file() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("plugins_manifest.json");
    std::fs::write(&file, json!({"api_plugins": ["portal"]}).to_string()).unwrap();
    let source = meerschaum_server::host::FileManifest(file.clone());
    let manifest = source.load().await.unwrap();
    assert_eq!(manifest.reasons(), vec!["api_plugins"]);
    let with = WithConsole(source.clone(), true).load().await.unwrap();
    assert_eq!(with.reasons(), vec!["api_plugins", "console"]);
    // Forced off over a file that says `console: true`.
    std::fs::write(&file, json!({"console": true}).to_string()).unwrap();
    assert!(source.load().await.unwrap().needs_host());
    assert!(
        !WithConsole(source, false)
            .load()
            .await
            .unwrap()
            .needs_host()
    );
}

#[test]
fn console_status_documents() {
    let status = |v: Json| ConsoleStatus::from_document(&v);
    assert_eq!(status(mounted()), Some(ConsoleStatus::Mounted));
    assert_eq!(
        status(unmounted("no dash")),
        Some(ConsoleStatus::Unavailable("no dash".into()))
    );
    // An unmounted console without a reason still says something.
    assert!(matches!(
        status(json!({"console": {"mounted": false}})),
        Some(ConsoleStatus::Unavailable(r)) if !r.is_empty()
    ));
    assert_eq!(status(json!({"tools": []})), None);
    assert_eq!(status(json!({"console": {"reason": "x"}})), None);
}

#[cfg(unix)]
#[test]
fn the_console_probe_reads_the_interpreters_answer() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let script = |name: &str, body: &str| {
        let path = dir.path().join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    };
    let root = dir.path();
    let ok = probe_console(&script("ok", "echo '[]'"), root, &[]);
    assert!(ok.available && ok.missing.is_empty());
    let partial = probe_console(&script("partial", "echo '[\"a2wsgi\"]'"), root, &[]);
    assert!(!partial.available);
    assert_eq!(partial.missing, vec!["a2wsgi"]);
    let warning = partial.warning();
    assert!(warning.contains("a2wsgi") && warning.contains("pip install \"meerschaum[dash]\""));
    let broken = probe_console(&script("broken", "exit 3"), root, &[]);
    assert!(!broken.available && broken.missing[0].contains("exited"));
    let garbage = probe_console(&script("garbage", "echo nope"), root, &[]);
    assert!(!garbage.available);
    let absent = probe_console(&dir.path().join("no-such-python"), root, &[]);
    assert!(!absent.available && absent.missing[0].contains("no Python interpreter"));
}

#[cfg(unix)]
#[test]
fn a_hung_interpreter_ends_the_probe_at_its_deadline() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hung");
    // A `sitecustomize` that blocks forever, and a grandchild that keeps the pipe open.
    std::fs::write(&path, "#!/bin/sh\n(sleep 30 &)\nsleep 30\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let started = std::time::Instant::now();
    let probe = meerschaum_server::host::probe_console_within(
        &path,
        dir.path(),
        &[],
        std::time::Duration::from_millis(300),
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert!(!probe.available);
    assert!(
        probe.missing[0].contains("did not answer"),
        "{:?}",
        probe.missing
    );
    // The same warning every unavailable console gets: one line, and `/dash` is the pointer page.
    assert!(probe.warning().contains("pointer page"));
    assert_eq!(meerschaum_server::host::PROBE_TIMEOUT.as_secs(), 10);
}

#[test]
fn a_real_interpreter_answers_the_probe_when_one_is_present() {
    // The probe script itself, against whatever `python3` this machine has: it must run and
    // answer a list (the modules may or may not be installed).
    let python = meerschaum_server::host::PythonHostLauncher::python_from_env();
    let dir = tempfile::tempdir().unwrap();
    let probe = probe_console(&python, dir.path(), &[]);
    if probe.available {
        assert!(probe.missing.is_empty());
    } else if !probe.missing.iter().any(|m| m.contains("no Python")) {
        for module in &probe.missing {
            assert!(
                meerschaum_server::host::CONSOLE_MODULES.contains(&module.as_str())
                    || module.contains("exited"),
                "{module}"
            );
        }
    }
}

/// One `create` call: the name, the sysargs and the properties.
type Created = (String, Vec<String>, Option<Map>);

/// Records `create` and `delete`; nothing runs. `listing` is what `list` returns, `stopped` the
/// stop times `stop_time` answers.
#[derive(Default)]
struct RecordingJobs {
    created: Mutex<Vec<Created>>,
    deleted: Mutex<Vec<String>>,
    listing: Mutex<Vec<(String, &'static str)>>,
    stopped: Mutex<Vec<(String, chrono::DateTime<chrono::Utc>)>>,
    success: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl JobsBackend for RecordingJobs {
    async fn list(&self) -> indexmap::IndexMap<String, Json> {
        self.listing
            .lock()
            .unwrap()
            .iter()
            .map(|(name, status)| (name.clone(), json!({"status": status, "result": [self.success.load(Ordering::SeqCst), "finished"]})))
            .collect()
    }
    async fn get(&self, name: &str) -> Option<Json> {
        self.listing.lock().unwrap().iter().find(|(n, _)| n == name)
            .map(|(_, status)| json!({"status": status, "result": [self.success.load(Ordering::SeqCst), "finished"]}))
    }
    async fn exists(&self, name: &str) -> bool {
        self.created
            .lock()
            .unwrap()
            .iter()
            .any(|(n, _, _)| n == name)
    }
    async fn create(&self, name: &str, sysargs: Vec<String>, properties: Option<Map>) -> CreateJob {
        if self.exists(name).await {
            return CreateJob::Exists;
        }
        self.created
            .lock()
            .unwrap()
            .push((name.into(), sysargs, properties));
        CreateJob::Started(SuccessTuple::ok("started"))
    }
    async fn delete(&self, name: &str) -> SuccessTuple {
        self.deleted.lock().unwrap().push(name.into());
        self.listing.lock().unwrap().retain(|(n, _)| n != name);
        SuccessTuple::success()
    }
    async fn start(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn stop(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn pause(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn logs(&self, _name: &str) -> Option<String> {
        None
    }
    async fn stop_time(&self, name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        self.stopped
            .lock()
            .unwrap()
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| *t)
    }
    async fn is_blocking_on_stdin(&self, _name: &str) -> bool {
        false
    }
    async fn prompt_kwargs(&self, _name: &str) -> Map {
        Map::new()
    }
    async fn monitor_logs(&self, _name: &str) -> Option<meerschaum_server::backends::LogMonitor> {
        None
    }
}

fn jobs_app() -> (Router, Arc<RecordingJobs>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("bob", "pw", None);
    let jobs = Arc::new(RecordingJobs::default());
    let state = AppState::builder(common::config(dir.path()), instance)
        .secret(SECRET.to_vec())
        .jobs(jobs.clone())
        .build()
        .unwrap();
    (meerschaum_server::app(state), jobs, dir)
}

fn post_json(uri: &str, auth: Option<&str>, body: Json) -> axum::http::Request<Body> {
    req(
        "POST",
        uri,
        auth,
        Some(("application/json", body.to_string().into_bytes())),
    )
}

#[tokio::test]
async fn execute_starts_an_ephemeral_job() {
    let (app, jobs, _root) = jobs_app();
    let admin = common::login(&app, "admin", "pass", None).await;

    let r = send(
        &app,
        post_json(
            "/execute",
            Some(&admin),
            json!({"sysargs": ["sync", "pipes", "-i", "sql:main", "-e", "api:main"]}),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let body = r.json();
    assert_eq!(body["success"], true);
    assert_eq!(body["message"], "started");
    let name = body["job"].as_str().unwrap().to_string();
    assert!(
        name.starts_with("execute-") && name.len() == "execute-".len() + 8,
        "{name}"
    );
    // This server does not serve the log stream (no `meerschaum-web`): no stream, the log route.
    assert!(body["stream"].is_null(), "{body}");
    assert_eq!(body["logs"], format!("/logs/{name}"));
    // The executor flag (`-e api:main`) is cleaned out like `POST /jobs/{name}` does.
    assert_eq!(body["sysargs"], json!(["sync", "pipes", "-i", "sql:main"]));
    let created = jobs.created.lock().unwrap().clone();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].0, name);
    assert_eq!(created[0].1, vec!["sync", "pipes", "-i", "sql:main"]);
    // Kept after it finishes (the browser opens the stream after the response; a quick action
    // would otherwise be gone, with its output, before the stream connects).
    assert!(
        created[0]
            .2
            .as_ref()
            .is_none_or(|p| p.get("delete_after_completion").is_none())
    );

    // A chosen name is used, and used twice is a conflict.
    let named = json!({"sysargs": ["show", "version"], "name": "my.job_1"});
    let r = send(&app, post_json("/execute", Some(&admin), named.clone())).await;
    assert_eq!(r.json()["job"], "my.job_1");
    let r = send(&app, post_json("/execute", Some(&admin), named)).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn execute_prunes_only_old_finished_execute_jobs() {
    let (app, jobs, root) = jobs_app();
    let admin = common::login(&app, "admin", "pass", None).await;
    let now = chrono::Utc::now();
    let hour = chrono::Duration::hours(1);
    *jobs.listing.lock().unwrap() = vec![
        ("execute-old00001".into(), "stopped"),
        ("execute-new00002".into(), "stopped"),
        ("execute-run00003".into(), "running"),
        ("my-sync-job".into(), "stopped"),
        ("execute-nostop04".into(), "stopped"),
    ];
    *jobs.stopped.lock().unwrap() = vec![
        ("execute-old00001".into(), now - chrono::Duration::days(8)),
        (
            "execute-new00002".into(),
            now - chrono::Duration::minutes(5),
        ),
        ("execute-run00003".into(), now - hour * 5),
        ("my-sync-job".into(), now - hour * 5),
    ];
    let r = send(
        &app,
        post_json(
            "/execute",
            Some(&admin),
            json!({"sysargs": ["show", "version"]}),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // Only a stopped automatic worker beyond its failed/unknown retention goes; a running one, a recent
    // one, one that is not ours and one without a stop time stay.
    assert_eq!(*jobs.deleted.lock().unwrap(), vec!["execute-old00001"]);
    assert!(root.path().join("runs/execute-old00001.json").exists());
    let summaries = send(&app, req("GET", "/execute/runs", Some(&admin), None)).await;
    assert_eq!(summaries.json()["runs"][0]["name"], "execute-old00001");
}

#[tokio::test]
async fn execute_validates_and_authorizes() {
    let (app, jobs, _root) = jobs_app();
    let admin = common::login(&app, "admin", "pass", None).await;

    // Not logged in.
    let r = send(
        &app,
        post_json("/execute", None, json!({"sysargs": ["show", "version"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);

    // A user without the scopes of creating a job to run an action.
    let bob = common::login(&app, "bob", "pw", Some("jobs:read")).await;
    let r = send(
        &app,
        post_json(
            "/execute",
            Some(&bob),
            json!({"sysargs": ["show", "version"]}),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());

    for (body, why) in [
        (json!({}), "missing sysargs"),
        (json!({"sysargs": "show version"}), "a string, not a list"),
        (json!({"sysargs": ["show", 1]}), "a non-string argument"),
        (json!({"sysargs": []}), "empty"),
        (
            json!({"sysargs": ["-e", "api:main"]}),
            "nothing left after cleaning",
        ),
        (
            json!({"sysargs": ["show"], "name": "bad name!"}),
            "bad name",
        ),
        (json!({"sysargs": ["show"], "name": ""}), "empty name"),
        (json!([1, 2]), "a list body"),
    ] {
        // The v5 routes answer an invalid request with `400` (and the errors).
        let r = send(&app, post_json("/execute", Some(&admin), body)).await;
        assert_eq!(r.status.as_u16(), 400, "{why}: {}", r.text());
        assert_eq!(r.json()["success"], false, "{why}");
    }
    let r = send(&app, req("POST", "/execute", Some(&admin), None)).await;
    assert_eq!(r.status.as_u16(), 400);
    assert!(
        jobs.created.lock().unwrap().is_empty(),
        "nothing was started"
    );
}

#[test]
fn execute_is_in_the_actions_route_group() {
    assert_eq!(
        meerschaum_server::routes::route_group_of("/execute"),
        "actions"
    );
    assert_eq!(
        meerschaum_server::routes::route_group_of("/actions/sync"),
        "actions"
    );
}

#[tokio::test]
async fn execute_follows_the_actions_allowlist() {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let mut cfg = common::config(dir.path());
    cfg.routes_allowlist = vec!["pipes".into(), "jobs".into()];
    let state = AppState::builder(cfg, instance)
        .secret(SECRET.to_vec())
        .jobs(Arc::new(RecordingJobs::default()))
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let r = send(
        &app,
        post_json("/execute", None, json!({"sysargs": ["show", "version"]})),
    )
    .await;
    // No `actions` group, no route.
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rc6_disabled_native_routes_do_not_reappear() {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    let mut cfg = common::config(dir.path());
    cfg.no_auth = true;
    cfg.routes_disabled = vec!["login".into(), "pipes".into()];
    assert!(!meerschaum_server::routes::is_proxyable(&cfg, "/login"));
    assert!(!meerschaum_server::routes::is_proxyable(
        &cfg,
        "/pipes/a/b/c"
    ));
    assert!(meerschaum_server::routes::is_proxyable(
        &cfg,
        "/synthetic/login"
    ));
    let state = AppState::builder(cfg, instance)
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    assert_eq!(
        send(&app, post_json("/login", None, json!({})))
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(&app, req("GET", "/pipes", None, None)).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(&app, req("GET", "/version", None, None)).await.status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn rc6_terminal_validation_never_starts_a_worker() {
    let (app, jobs, _root) = jobs_app();
    let admin = common::login(&app, "admin", "pass", None).await;
    let allowed = send(
        &app,
        post_json(
            "/execute/validate",
            Some(&admin),
            json!({"sysargs": ["show", "version"]}),
        ),
    )
    .await;
    assert_eq!(allowed.status, StatusCode::OK);
    assert_eq!(allowed.json()["success"], true);
    assert!(jobs.created.lock().unwrap().is_empty());
}

#[tokio::test]
async fn rc6_cleanup_runs_without_an_execute_request() {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    let jobs = Arc::new(RecordingJobs::default());
    jobs.listing
        .lock()
        .unwrap()
        .push(("execute-expired".into(), "stopped"));
    jobs.stopped.lock().unwrap().push((
        "execute-expired".into(),
        chrono::Utc::now() - chrono::Duration::days(8),
    ));
    let state = AppState::builder(common::config(root.path()), instance)
        .secret(SECRET.to_vec())
        .jobs(jobs.clone())
        .build()
        .unwrap();
    let _app = meerschaum_server::app(state);
    tokio::time::timeout(Duration::from_secs(2), async {
        while jobs.deleted.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(root.path().join("runs/execute-expired.json").exists());
}

#[tokio::test]
async fn rc6_successful_output_lasts_24_hours_and_failures_last_seven_days() {
    let (app, jobs, root) = jobs_app();
    let admin = common::login(&app, "admin", "pass", None).await;
    jobs.success.store(true, Ordering::SeqCst);
    *jobs.listing.lock().unwrap() = vec![
        ("execute-success-old".into(), "stopped"),
        ("execute-success-recent".into(), "stopped"),
    ];
    *jobs.stopped.lock().unwrap() = vec![
        (
            "execute-success-old".into(),
            chrono::Utc::now() - chrono::Duration::hours(25),
        ),
        (
            "execute-success-recent".into(),
            chrono::Utc::now() - chrono::Duration::hours(23),
        ),
    ];
    let response = send(
        &app,
        post_json(
            "/execute",
            Some(&admin),
            json!({"sysargs": ["show", "version"]}),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(*jobs.deleted.lock().unwrap(), vec!["execute-success-old"]);
    assert!(root.path().join("runs/execute-success-old.json").exists());
    assert!(
        !root
            .path()
            .join("runs/execute-success-recent.json")
            .exists()
    );
    jobs.success.store(false, Ordering::SeqCst);
    // The remaining 23-hour worker now has failure status and still must be retained.
    let response = send(
        &app,
        post_json(
            "/execute",
            Some(&admin),
            json!({"sysargs": ["show", "version"]}),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(jobs.deleted.lock().unwrap().len(), 1);
}
