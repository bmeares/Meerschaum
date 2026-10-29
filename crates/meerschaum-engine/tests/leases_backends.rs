//! Lease contention across processes on every instance type that implements the lease pair
//! (§5.8, §14): SQLite (the instance's lease table), PostgreSQL-family (`postgis`, schema
//! `w7_engine`) and Valkey (`SET NX PX`).
//!
//! Each test starts this test binary again as a child process (`lease_backend_child`, ignored
//! unless invoked explicitly) that takes the lease through the same [`InstanceLeases`] the
//! engine uses. The parent then proves that
//!
//! 1. it cannot take a lease the child holds, and takes it after the child releases it;
//! 2. a child that is killed while holding a lease (a crash) blocks the parent only until the
//!    TTL expires.
//!
//! The server backends are gated on `MRSM_TEST_POSTGIS_URI` and `MRSM_TEST_VALKEY_URI`; an
//! empty value selects the disposable test container (`postgis://test:test1234@localhost:5449/testdb`
//! and `valkey://localhost:6399/13`), an unset one skips the test.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_engine::executor::LeaseSource;
use meerschaum_engine::{InstanceLeases, InstanceState, PipeRef, StaticRegistry};

const KEYS: &str = "sql:w7";
const SCHEMA: &str = "w7_engine";

fn env_dir() -> Option<PathBuf> {
    std::env::var_os("MRSM_ENGINE_LEASE_CHILD").map(PathBuf::from)
}

fn wait_for(path: &Path, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if path.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

/// One backend: how to connect (the child reconnects from the same description).
#[derive(Clone)]
struct Backend {
    kind: &'static str,
    target: String,
}

fn postgis_uri() -> Option<String> {
    let raw = std::env::var("MRSM_TEST_POSTGIS_URI").ok()?;
    let base = if raw.is_empty() {
        "postgis://test:test1234@localhost:5449/testdb".to_string()
    } else {
        raw
    };
    let sep = if base.contains('?') { '&' } else { '?' };
    Some(format!("{base}{sep}schema={SCHEMA}"))
}

fn valkey_uri() -> Option<String> {
    let raw = std::env::var("MRSM_TEST_VALKEY_URI").ok()?;
    Some(if raw.is_empty() {
        "valkey://localhost:6399/13".to_string()
    } else {
        raw
    })
}

async fn connect(backend: &Backend) -> Arc<dyn InstanceConnector> {
    match backend.kind {
        "sqlite" => common::connect(Path::new(&backend.target)).await,
        "postgis" => Arc::new(
            SqlConnector::from_uri("w7", &backend.target)
                .await
                .expect("connect to PostgreSQL"),
        ),
        "valkey" => Arc::new(
            meerschaum_connector_valkey::ValkeyConnector::from_uri("w7", &backend.target)
                .await
                .expect("connect to Valkey"),
        ),
        other => panic!("unknown backend {other}"),
    }
}

fn leases(conn: Arc<dyn InstanceConnector>, dir: &Path) -> InstanceLeases {
    let registry: Arc<dyn meerschaum_engine::ConnectorRegistry> =
        Arc::new(StaticRegistry::new().with_instance(KEYS, conn));
    let state = Arc::new(InstanceState::new(registry.clone(), KEYS));
    InstanceLeases::new(registry, state, dir.join("fallback"))
}

fn node(metric: &str) -> PipeRef {
    PipeRef::new(KEYS, KEYS, metric, None)
}

/// The child process: take the lease (`MODE=hold` releases it on `release`; `MODE=crash` is
/// killed by the parent while holding it).
#[tokio::test]
#[ignore = "run by the lease tests as a child process"]
async fn lease_backend_child() {
    let Some(dir) = env_dir() else { return };
    let backend = Backend {
        kind: Box::leak(std::env::var("MRSM_LEASE_KIND").unwrap().into_boxed_str()),
        target: std::env::var("MRSM_LEASE_TARGET").unwrap(),
    };
    let metric = std::env::var("MRSM_LEASE_METRIC").unwrap();
    let ttl = Duration::from_millis(std::env::var("MRSM_LEASE_TTL_MS").unwrap().parse().unwrap());
    let leases = leases(connect(&backend).await, &dir);
    let node = node(&metric);
    assert!(leases.acquire(&node, "child", ttl).await.unwrap());
    std::fs::write(dir.join("ready"), b"").unwrap();
    assert!(wait_for(&dir.join("release"), Duration::from_secs(120)));
    leases.release(&node, "child").await.unwrap();
    std::fs::write(dir.join("released"), b"").unwrap();
}

fn spawn_child(dir: &Path, backend: &Backend, metric: &str, ttl_ms: u64) -> std::process::Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "lease_backend_child", "--nocapture"])
        .env("MRSM_ENGINE_LEASE_CHILD", dir)
        .env("MRSM_LEASE_KIND", backend.kind)
        .env("MRSM_LEASE_TARGET", &backend.target)
        .env("MRSM_LEASE_METRIC", metric)
        .env("MRSM_LEASE_TTL_MS", ttl_ms.to_string())
        .spawn()
        .expect("spawn the child process")
}

async fn contention(backend: Backend) {
    let dir = common::temp_dir("lease_backend");
    let conn = connect(&backend).await;
    let leases = leases(conn, &dir);
    let tag = uuid::Uuid::new_v4().simple().to_string();

    // 1. A held lease excludes this process; a released one does not.
    let metric = format!("w7_engine:hold:{tag}");
    let node_a = node(&metric);
    let hold_dir = dir.join("hold");
    std::fs::create_dir_all(&hold_dir).unwrap();
    let mut child = spawn_child(&hold_dir, &backend, &metric, 60_000);
    assert!(
        wait_for(&hold_dir.join("ready"), Duration::from_secs(120)),
        "the child never took the lease"
    );
    let ttl = Duration::from_secs(30);
    assert!(
        !leases.acquire(&node_a, "parent", ttl).await.unwrap(),
        "a lease held by another process was granted"
    );
    assert!(
        !leases.renew(&node_a, "parent", ttl).await.unwrap(),
        "a non-holder renewed a lease"
    );
    std::fs::write(hold_dir.join("release"), b"").unwrap();
    assert!(child.wait().expect("child exits").success());
    assert!(hold_dir.join("released").exists());
    assert!(leases.acquire(&node_a, "parent", ttl).await.unwrap());
    leases.release(&node_a, "parent").await.unwrap();

    // 2. A crashed holder blocks only until its TTL expires.
    let metric = format!("w7_engine:crash:{tag}");
    let node_b = node(&metric);
    let crash_dir = dir.join("crash");
    std::fs::create_dir_all(&crash_dir).unwrap();
    let mut child = spawn_child(&crash_dir, &backend, &metric, 1500);
    assert!(wait_for(&crash_dir.join("ready"), Duration::from_secs(120)));
    child.kill().expect("kill the child");
    let _ = child.wait();
    assert!(
        !leases.acquire(&node_b, "parent", ttl).await.unwrap(),
        "the crashed holder's lease was granted before its TTL expired"
    );
    let mut granted = false;
    for _ in 0..80 {
        if leases.acquire(&node_b, "parent", ttl).await.unwrap() {
            granted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(granted, "the lease never expired after the holder crashed");
    leases.release(&node_b, "parent").await.unwrap();
    assert!(
        leases.warnings().is_empty(),
        "fell back to file locks: {:?}",
        leases.warnings()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_leases_exclude_another_process() {
    let dir = common::temp_dir("lease_sqlite_db");
    let backend = Backend {
        kind: "sqlite",
        target: dir.join("eng.db").display().to_string(),
    };
    contention(backend).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgis_leases_exclude_another_process() {
    let Some(uri) = postgis_uri() else {
        eprintln!("skipped: MRSM_TEST_POSTGIS_URI is not set");
        return;
    };
    let boot = SqlConnector::from_uri("w7boot", &uri.replace(&format!("schema={SCHEMA}"), ""))
        .await
        .expect("connect to PostgreSQL");
    assert!(
        boot.exec(&format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
            .await
            .is_some()
    );
    contention(Backend {
        kind: "postgis",
        target: uri,
    })
    .await;
    let _ = boot
        .exec(&format!("DROP SCHEMA IF EXISTS {SCHEMA} CASCADE"))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valkey_leases_exclude_another_process() {
    let Some(uri) = valkey_uri() else {
        eprintln!("skipped: MRSM_TEST_VALKEY_URI is not set");
        return;
    };
    contention(Backend {
        kind: "valkey",
        target: uri,
    })
    .await;
}
