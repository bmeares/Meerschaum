//! `GET /healthcheck/ready` tells the truth about the Python API host (rc32): `503` (`starting`)
//! while the server starts, `200` only once a host it needs serves, `503` (`host_unavailable`)
//! while that host is not serving (it never got ready, refused to start, or died and is being
//! restarted) with no timeout turning it into a `200`, and `503` (`draining`) from the moment the
//! drain begins. A server whose plugins need no host is ready without one.
#![cfg(unix)]

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use common::{SECRET, req, send};
use meerschaum_server::AppState;
use meerschaum_server::host::{
    HostLauncher, HostManifest, HostProcess, HostSupervisor, HostTransport, SupervisorOptions,
};
use meerschaum_server::proxy::HostProxy;
use tokio::time::Instant;

/// What a fake host process does.
#[derive(Debug, Clone, Copy)]
enum Behavior {
    /// Serves `/plugin/echo` until it is killed ([`FakeLauncher::kill_current`]).
    Healthy,
    /// Stays alive and never listens.
    Silent,
    /// Exits at once with the guard's refusal code (a `meerschaum` this program cannot use).
    Refuse,
}

struct FakeProcess {
    behavior: Behavior,
    server: Option<tokio::task::JoinHandle<()>>,
}

#[async_trait]
impl HostProcess for FakeProcess {
    async fn wait(&mut self) -> std::io::Result<Option<i32>> {
        match self.behavior {
            Behavior::Refuse => Ok(Some(meerschaum_core::python_guard::REFUSED_EXIT_CODE)),
            Behavior::Silent => std::future::pending().await,
            Behavior::Healthy => {
                if let Some(server) = self.server.as_mut() {
                    let _ = server.await;
                    self.server = None;
                }
                Ok(Some(1))
            }
        }
    }

    async fn kill(&mut self) -> std::io::Result<()> {
        if let Some(server) = &self.server {
            server.abort();
        }
        // A silent process ends when killed.
        self.behavior = Behavior::Healthy;
        Ok(())
    }
}

/// Launches fake hosts with one behavior; remembers the current server so a test can kill it.
struct FakeLauncher {
    behavior: Behavior,
    current: Mutex<Option<tokio::task::AbortHandle>>,
}

impl FakeLauncher {
    fn new(behavior: Behavior) -> Arc<FakeLauncher> {
        Arc::new(FakeLauncher {
            behavior,
            current: Mutex::new(None),
        })
    }

    /// The current host process dies (its server stops; its `wait` returns).
    fn kill_current(&self) {
        if let Some(server) = self.current.lock().unwrap().take() {
            server.abort();
        }
    }
}

#[async_trait]
impl HostLauncher for FakeLauncher {
    async fn launch(&self, socket: &std::path::Path) -> std::io::Result<Box<dyn HostProcess>> {
        let server = match self.behavior {
            Behavior::Healthy => {
                let _ = std::fs::remove_file(socket);
                let listener = tokio::net::UnixListener::bind(socket)?;
                let app = Router::new().route("/plugin/echo", get(|| async { "echo" }));
                let server = tokio::spawn(async move {
                    let _ = axum::serve(listener, app).await;
                });
                *self.current.lock().unwrap() = Some(server.abort_handle());
                Some(server)
            }
            Behavior::Silent | Behavior::Refuse => None,
        };
        Ok(Box::new(FakeProcess {
            behavior: self.behavior,
            server,
        }))
    }
}

fn manifest() -> HostManifest {
    HostManifest {
        api_plugins: vec!["example".into()],
        ..Default::default()
    }
}

/// A short readiness wait and a restart delay long enough to observe the restart.
fn opts() -> SupervisorOptions {
    SupervisorOptions {
        ready_timeout: Duration::from_millis(300),
        backoff_initial: Duration::from_millis(400),
        backoff_max: Duration::from_millis(400),
        health_interval: Duration::from_secs(30),
        health_timeout: Duration::from_millis(200),
        health_failures: 3,
        stable_after: Duration::from_secs(60),
    }
}

struct Fixture {
    app: Router,
    state: AppState,
    supervisor: Arc<HostSupervisor>,
    launcher: Arc<FakeLauncher>,
    _dir: tempfile::TempDir,
}

fn fixture(behavior: Behavior, manifest: HostManifest) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let launcher = FakeLauncher::new(behavior);
    let supervisor = HostSupervisor::with_transport(
        &manifest,
        launcher.clone(),
        dir.path().join("api").join("host.sock"),
        opts(),
        HostTransport::Unix,
    );
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    let state = AppState::builder(common::config(dir.path()), instance)
        .secret(SECRET.to_vec())
        .host(Arc::new(HostProxy::supervised(supervisor.clone())))
        .build()
        .unwrap();
    Fixture {
        app: meerschaum_server::app(state.clone()),
        state,
        supervisor,
        launcher,
        _dir: dir,
    }
}

/// `(status, reason)` of `GET /healthcheck/ready`.
async fn ready(app: &Router) -> (StatusCode, serde_json::Value) {
    let r = send(app, req("GET", "/healthcheck/ready", None, None)).await;
    let body = r.json();
    assert_eq!(body["success"], r.status == StatusCode::OK, "{body}");
    assert!(body["message"].is_string(), "{body}");
    (r.status, body["reason"].clone())
}

/// Poll readiness until it answers `status`.
async fn wait_ready(app: &Router, status: StatusCode, within: Duration) -> serde_json::Value {
    let until = Instant::now() + within;
    loop {
        let (got, reason) = ready(app).await;
        if got == status {
            return reason;
        }
        assert!(
            Instant::now() < until,
            "readiness stayed {got} ({reason}), expected {status}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn starting_then_ready_once_the_host_serves() {
    let f = fixture(Behavior::Healthy, manifest());
    f.state.lifecycle.set_starting(true);
    let (status, reason) = ready(&f.app).await;
    assert_eq!(
        (status, reason.as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("starting"))
    );
    f.state.lifecycle.set_starting(false);
    // Nothing asked for the host yet: not serving, and the probe itself starts it.
    assert!(!f.supervisor.is_serving());
    let (status, reason) = ready(&f.app).await;
    assert_eq!(
        (status, reason.as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("host_unavailable"))
    );
    assert_eq!(
        wait_ready(&f.app, StatusCode::OK, Duration::from_secs(5)).await,
        serde_json::Value::Null
    );
    assert!(f.supervisor.is_serving());
    assert_eq!(f.supervisor.starts(), 1);
    f.supervisor.shutdown().await;
    assert!(!f.supervisor.is_serving());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_that_never_serves_keeps_the_server_unready() {
    let f = fixture(Behavior::Silent, manifest());
    // Several readiness timeouts and restarts pass: never a `200`.
    let until = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < until {
        let (status, reason) = ready(&f.app).await;
        assert_eq!(
            (status, reason.as_str()),
            (StatusCode::SERVICE_UNAVAILABLE, Some("host_unavailable"))
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(f.supervisor.starts() >= 2, "{}", f.supervisor.starts());
    f.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_that_refuses_to_start_keeps_the_server_unready() {
    let f = fixture(Behavior::Refuse, manifest());
    let until = Instant::now() + Duration::from_millis(800);
    while Instant::now() < until {
        let (status, reason) = ready(&f.app).await;
        assert_eq!(
            (status, reason.as_str()),
            (StatusCode::SERVICE_UNAVAILABLE, Some("host_unavailable"))
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // The refusal is remembered: probes do not relaunch it in a loop.
    assert_eq!(f.supervisor.starts(), 1);
    f.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_that_dies_makes_the_server_unready_until_its_replacement_serves() {
    let f = fixture(Behavior::Healthy, manifest());
    wait_ready(&f.app, StatusCode::OK, Duration::from_secs(5)).await;
    f.launcher.kill_current();
    // Within the restart delay: not ready.
    let reason = wait_ready(
        &f.app,
        StatusCode::SERVICE_UNAVAILABLE,
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(reason, "host_unavailable");
    // The supervisor's replacement serves: ready again.
    wait_ready(&f.app, StatusCode::OK, Duration::from_secs(5)).await;
    assert_eq!(f.supervisor.starts(), 2);
    let r = send(&f.app, req("GET", "/plugin/echo", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    f.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn draining_is_unready_even_while_the_host_serves() {
    let f = fixture(Behavior::Healthy, manifest());
    wait_ready(&f.app, StatusCode::OK, Duration::from_secs(5)).await;
    f.state.lifecycle.begin_drain();
    let (status, reason) = ready(&f.app).await;
    assert_eq!(
        (status, reason.as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("draining"))
    );
    // `/healthcheck` (liveness, v4) is unchanged.
    let r = send(&f.app, req("GET", "/healthcheck", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    f.supervisor.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_whose_plugins_need_no_host_is_ready_without_one() {
    let f = fixture(Behavior::Healthy, HostManifest::default());
    let (status, _) = ready(&f.app).await;
    assert_eq!(status, StatusCode::OK);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        f.supervisor.starts(),
        0,
        "the probe started an unneeded host"
    );
}
