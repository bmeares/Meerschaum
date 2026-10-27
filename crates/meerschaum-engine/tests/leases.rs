//! Lease contention across two processes on a SQLite instance (§5.8, §14).
//!
//! The parent test starts this test binary again as a child process (`lease_holder_child`,
//! ignored unless invoked explicitly), which takes the lease on a pipe through the same
//! [`InstanceLeases`] the engine uses and holds it until told to let go. While it is held, the
//! parent's engine run must not sync the pipe; once released, it must.
//!
//! The SQLite instance implements the lease pair (its lease table), so this exercises the
//! instance's leases at engine-run level; `leases_backends.rs` covers PostgreSQL and Valkey.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::*;
use meerschaum_engine::executor::LeaseSource;
use meerschaum_engine::{
    InstanceLeases, InstanceState, KeyFilter, NodeStatus, PipeRef, RunOptions, Selection,
};

const CHILD_ENV: &str = "MRSM_ENGINE_LEASE_CHILD";

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

/// The child process: hold the lease on `a_raw` until `release` appears.
#[tokio::test]
#[ignore = "run by `leases_exclude_another_process` as a child process"]
async fn lease_holder_child() {
    let Some(dir) = std::env::var_os(CHILD_ENV).map(PathBuf::from) else {
        return;
    };
    let conn = connect(&dir.join("eng.db")).await;
    let registry = registry(&conn);
    let state = Arc::new(InstanceState::new(registry.clone(), INSTANCE));
    let leases = InstanceLeases::new(registry, state, dir.join("leases"));
    let node = PipeRef::new(INSTANCE, INSTANCE, "a_raw", None);
    let ttl = Duration::from_secs(30);
    assert!(leases.acquire(&node, "child", ttl).await.unwrap());
    std::fs::write(dir.join("ready"), b"").unwrap();
    assert!(wait_for(&dir.join("release"), Duration::from_secs(60)));
    leases.release(&node, "child").await.unwrap();
    std::fs::write(dir.join("released"), b"").unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leases_exclude_another_process() {
    let env = env("leases").await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec(
        "INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0), ('2026-01-01 01:00:00', 1, 2.0)",
    )
    .await;
    let params = serde_json::json!({
        "sql": "SELECT ts, id, v FROM src",
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
    });
    env.register("a_raw", &params).await;

    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "lease_holder_child", "--nocapture"])
        .env(CHILD_ENV, &env.dir)
        .spawn()
        .expect("spawn the child process");
    assert!(
        wait_for(&env.dir.join("ready"), Duration::from_secs(60)),
        "the child never took the lease"
    );

    let engine = env.engine_with(|c| {
        c.lease_wait = Duration::from_millis(400);
        c.lease_poll = Duration::from_millis(50);
    });
    let flat = || RunOptions {
        selection: Some(Selection::filter(KeyFilter {
            metric_keys: vec!["a_raw".into()],
            ..Default::default()
        })),
        ..Default::default()
    };
    let blocked = engine.run(flat()).await;
    let node = &blocked.record.as_ref().unwrap().nodes[0];
    assert_eq!(node.status, NodeStatus::Failed, "{node:#?}");
    assert!(
        node.message
            .as_deref()
            .unwrap_or_default()
            .contains("another process is syncing"),
        "{node:#?}"
    );
    assert_eq!(env.rowcount("a_raw").await, 0);

    std::fs::write(env.dir.join("release"), b"").unwrap();
    let status = child.wait().expect("child exits");
    assert!(status.success(), "child failed: {status}");
    assert!(env.dir.join("released").exists());

    let ran = engine.run(flat()).await;
    assert!(ran.success, "{}", ran.message);
    assert_eq!(env.rowcount("a_raw").await, 2);
}
