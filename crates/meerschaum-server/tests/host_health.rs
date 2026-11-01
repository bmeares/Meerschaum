//! The API host's health checks (`GET /_mrsm/host/health`): a host process that is alive but no
//! longer answers is terminated, awaited and replaced after `health_failures` failed checks in a
//! row, while requests forwarded to it end with `504`; a slow host that still answers its checks
//! is left alone; a stop during a restart delay or during a check ends the supervisor promptly.
#![cfg(unix)]

mod common;

use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use common::{SECRET, req, send};
use meerschaum_server::AppState;
use meerschaum_server::host::{
    HostLauncher, HostManifest, HostProcess, HostSupervisor, SupervisorOptions,
};
use meerschaum_server::proxy::HostProxy;
use tokio::time::Instant;

/// What a fake host process does.
#[derive(Debug, Clone, Copy)]
enum Behavior {
    /// Serves `/plugin/echo` and answers everything else `404` (its health checks included).
    Healthy,
    /// Like `Healthy`, and `/plugin/slow` takes two seconds to answer.
    Slow,
    /// Accepts connections and never answers anything.
    Hung,
    /// Exits at once.
    Crash,
}

/// A fake host process: a server task on the socket. What happens to it is logged.
struct FakeProcess {
    generation: usize,
    server: Option<tokio::task::JoinHandle<()>>,
    log: Arc<Mutex<Vec<String>>>,
}

impl FakeProcess {
    fn record(&self, event: &str) {
        self.log
            .lock()
            .unwrap()
            .push(format!("{event} {}", self.generation));
    }
}

#[async_trait]
impl HostProcess for FakeProcess {
    async fn wait(&mut self) -> std::io::Result<Option<i32>> {
        match self.server.as_mut() {
            Some(server) => {
                let _ = server.await;
                self.server = None;
                self.record("waited");
                Ok(Some(0))
            }
            None => {
                self.record("exited");
                Ok(Some(1))
            }
        }
    }

    async fn kill(&mut self) -> std::io::Result<()> {
        self.record("killed");
        if let Some(server) = &self.server {
            server.abort();
        }
        Ok(())
    }
}

/// Launches fake hosts with the given behaviors, in order (the last one repeats).
struct FakeLauncher {
    behaviors: Mutex<VecDeque<Behavior>>,
    launches: Mutex<usize>,
    log: Arc<Mutex<Vec<String>>>,
}

impl FakeLauncher {
    fn new(behaviors: &[Behavior]) -> Arc<FakeLauncher> {
        Arc::new(FakeLauncher {
            behaviors: Mutex::new(behaviors.iter().copied().collect()),
            launches: Mutex::new(0),
            log: Arc::default(),
        })
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}

fn healthy_app(generation: usize) -> Router {
    Router::new()
        .route(
            "/plugin/echo",
            get(move || async move { ([("x-host-generation", generation.to_string())], "echo") }),
        )
        .route(
            "/plugin/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                "slow"
            }),
        )
}

async fn serve(
    socket: &Path,
    behavior: Behavior,
    generation: usize,
) -> Option<tokio::task::JoinHandle<()>> {
    if matches!(behavior, Behavior::Crash) {
        return None;
    }
    let _ = std::fs::remove_file(socket);
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    Some(match behavior {
        Behavior::Hung => tokio::spawn(async move {
            // Connections are accepted and held, never read or answered.
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        }),
        _ => tokio::spawn(async move {
            let _ = axum::serve(listener, healthy_app(generation)).await;
        }),
    })
}

#[async_trait]
impl HostLauncher for FakeLauncher {
    async fn launch(&self, socket: &Path) -> std::io::Result<Box<dyn HostProcess>> {
        let generation = {
            let mut n = self.launches.lock().unwrap();
            *n += 1;
            *n
        };
        let behavior = {
            let mut behaviors = self.behaviors.lock().unwrap();
            if behaviors.len() > 1 {
                behaviors.pop_front().unwrap()
            } else {
                *behaviors.front().unwrap()
            }
        };
        self.log
            .lock()
            .unwrap()
            .push(format!("launched {generation}"));
        let server = serve(socket, behavior, generation).await;
        Ok(Box::new(FakeProcess {
            generation,
            server,
            log: self.log.clone(),
        }))
    }
}

fn manifest() -> HostManifest {
    HostManifest {
        api_plugins: vec!["example".into()],
        ..Default::default()
    }
}

/// Checks every 100 ms, each at most 100 ms, three failures in a row: hung.
fn opts() -> SupervisorOptions {
    SupervisorOptions {
        ready_timeout: Duration::from_secs(5),
        backoff_initial: Duration::from_millis(10),
        backoff_max: Duration::from_millis(50),
        health_interval: Duration::from_millis(100),
        health_timeout: Duration::from_millis(100),
        health_failures: 3,
        stable_after: Duration::from_secs(60),
    }
}

/// The application with `proxy`; forwarded requests wait at most 300 ms for an answer. The
/// state gives the supervisor the secret its health checks are signed with.
fn app_with(proxy: HostProxy) -> (Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(dir.path());
    cfg.host_response_timeout = Some(Duration::from_millis(300));
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    let state = AppState::builder(cfg, instance)
        .secret(SECRET.to_vec())
        .host(Arc::new(proxy))
        .build()
        .unwrap();
    (meerschaum_server::app(state), dir)
}

async fn wait_for(what: &str, deadline: Duration, done: impl Fn() -> bool) {
    let until = Instant::now() + deadline;
    while !done() {
        assert!(Instant::now() < until, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_hung_host_is_replaced_after_its_old_process_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("api").join("host.sock");
    let launcher = FakeLauncher::new(&[Behavior::Hung, Behavior::Healthy]);
    let sup = HostSupervisor::new(&manifest(), launcher.clone(), &socket, opts());
    let (app, _root) = app_with(HostProxy::supervised(sup.clone()));

    // The first process accepts the request and never answers: `504`, not a hang.
    let r = tokio::time::timeout(
        Duration::from_secs(10),
        send(&app, req("GET", "/plugin/echo", None, None)),
    )
    .await
    .expect("a request to a hung host never ended");
    assert_eq!(r.status, StatusCode::GATEWAY_TIMEOUT, "{}", r.text());

    // Three failed checks later it is terminated, awaited, and only then replaced.
    wait_for("the replacement", Duration::from_secs(10), || {
        sup.starts() >= 2
    })
    .await;
    let r = send(&app, req("GET", "/plugin/echo", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.headers["x-host-generation"], "2");
    let log = launcher.log();
    let at = |entry: &str| {
        log.iter()
            .position(|e| e == entry)
            .unwrap_or_else(|| panic!("no '{entry}': {log:?}"))
    };
    assert!(at("killed 1") < at("waited 1"), "{log:?}");
    assert!(at("waited 1") < at("launched 2"), "{log:?}");

    // The healthy replacement is kept: no restart storm.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(sup.starts(), 2, "{:?}", launcher.log());
    sup.shutdown().await;
}

#[tokio::test]
async fn a_slow_host_that_answers_its_checks_is_kept() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("api").join("host.sock");
    let launcher = FakeLauncher::new(&[Behavior::Slow]);
    let sup = HostSupervisor::new(&manifest(), launcher.clone(), &socket, opts());
    let (app, _root) = app_with(HostProxy::supervised(sup.clone()));

    // A slow route times out for its client ...
    let r = send(&app, req("GET", "/plugin/slow", None, None)).await;
    assert_eq!(r.status, StatusCode::GATEWAY_TIMEOUT, "{}", r.text());
    // ... while the process keeps answering its checks: it is not replaced.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(sup.starts(), 1, "{:?}", launcher.log());
    let r = send(&app, req("GET", "/plugin/echo", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.headers["x-host-generation"], "1");
    assert!(
        !launcher.log().iter().any(|e| e.starts_with("killed")),
        "{:?}",
        launcher.log()
    );
    sup.shutdown().await;
}

#[tokio::test]
async fn a_stop_during_the_restart_delay_ends_the_supervisor_promptly() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("api").join("host.sock");
    let launcher = FakeLauncher::new(&[Behavior::Crash]);
    let sup = HostSupervisor::new(
        &manifest(),
        launcher.clone(),
        &socket,
        SupervisorOptions {
            ready_timeout: Duration::from_millis(500),
            // A long restart delay: the stop must not wait for it.
            backoff_initial: Duration::from_secs(30),
            backoff_max: Duration::from_secs(60),
            ..opts()
        },
    );
    sup.set_internal_secret(SECRET);
    assert!(sup.ensure_started().await.is_err());
    wait_for("the crash", Duration::from_secs(5), || {
        launcher.log().iter().any(|e| e == "exited 1")
    })
    .await;
    tokio::time::timeout(Duration::from_secs(3), sup.shutdown())
        .await
        .expect("the stop waited for the restart delay");
    assert_eq!(sup.starts(), 1, "{:?}", launcher.log());
}

#[tokio::test]
async fn a_stop_during_a_health_check_ends_the_supervisor_promptly() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("api").join("host.sock");
    let launcher = FakeLauncher::new(&[Behavior::Hung]);
    let sup = HostSupervisor::new(
        &manifest(),
        launcher.clone(),
        &socket,
        SupervisorOptions {
            health_interval: Duration::from_millis(50),
            // A check that would wait a long time for the hung host.
            health_timeout: Duration::from_secs(30),
            ..opts()
        },
    );
    sup.set_internal_secret(SECRET);
    sup.ensure_started().await.unwrap();
    // A check is in flight now.
    tokio::time::sleep(Duration::from_millis(300)).await;
    tokio::time::timeout(Duration::from_secs(3), sup.shutdown())
        .await
        .expect("the stop waited for the health check");
    let log = launcher.log();
    assert!(log.iter().any(|e| e == "killed 1"), "{log:?}");
    assert_eq!(sup.starts(), 1, "{log:?}");
}
