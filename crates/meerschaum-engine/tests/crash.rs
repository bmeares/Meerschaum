//! Crash injection (§14) on SQLite: a crash between the ledger write and the data commit, and
//! between the data commit and the cursor advance, must lose nothing. The next run converges on
//! the tables v4.1 produces for the same data (`fixtures/v4_sync/v4_sync.json`, phase 1).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::*;
use meerschaum_engine::{
    Depth, FaultInjector, NodeStatus, PipeRef, RunOptions, RunResult, Selection, StateSource,
};

/// Fails one node once at one point.
struct CrashOnce {
    node: PipeRef,
    after_ledger: bool,
    armed: AtomicBool,
}

impl CrashOnce {
    fn new(node: PipeRef, after_ledger: bool) -> Arc<CrashOnce> {
        Arc::new(CrashOnce {
            node,
            after_ledger,
            armed: AtomicBool::new(true),
        })
    }

    fn fire(&self, node: &PipeRef, at_ledger: bool) -> Result<(), String> {
        if *node == self.node
            && at_ledger == self.after_ledger
            && self.armed.swap(false, Ordering::SeqCst)
        {
            return Err(format!(
                "injected crash {}",
                if at_ledger {
                    "after the ledger write"
                } else {
                    "after the data commit"
                }
            ));
        }
        Ok(())
    }
}

impl FaultInjector for CrashOnce {
    fn after_ledger_write(&self, node: &PipeRef) -> Result<(), String> {
        self.fire(node, true)
    }

    fn after_data_commit(&self, node: &PipeRef) -> Result<(), String> {
        self.fire(node, false)
    }
}

const CHAIN: [&str; 3] = ["a_raw", "b_clean", "c_hourly"];

fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/v4_sync/v4_sync.json")).expect("valid fixture")
}

async fn setup(tag: &str, fx: &serde_json::Value) -> Env {
    let env = env(tag).await;
    env.exec(fx["create"].as_str().unwrap()).await;
    for p in fx["pipes"].as_array().unwrap() {
        let metric = p["metric"].as_str().unwrap();
        if CHAIN.contains(&metric) {
            let mut parameters = p["parameters"].clone();
            if metric == "b_clean" {
                // One chunk per sync, so a crash after "the" data commit leaves all of the
                // node's new rows committed (the tables do not depend on the chunk width).
                parameters["verify"] = serde_json::json!({"chunk_days": 30});
            }
            env.register(metric, &parameters).await;
        }
    }
    env
}

fn options(env: &Env) -> RunOptions {
    RunOptions {
        selection: Some(Selection::refs([env.pipe_ref("c_hourly")]).with_parents(Some(Depth::All))),
        ..Default::default()
    }
}

async fn apply_phase(env: &Env, fx: &serde_json::Value, phase: usize) {
    for stmt in fx["phases"][phase]["statements"].as_array().unwrap() {
        env.exec(stmt.as_str().unwrap()).await;
    }
}

fn status(result: &RunResult, env: &Env, metric: &str) -> NodeStatus {
    result
        .record
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == env.pipe_ref(metric))
        .unwrap()
        .status
}

async fn assert_matches_v4(env: &Env, fx: &serde_json::Value, phase: usize) {
    for metric in CHAIN {
        let want = Table::from_fixture(&fx["phases"][phase]["expected"][metric]);
        assert_eq!(
            env.table(metric).await,
            want,
            "{metric} after phase {phase}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_between_ledger_write_and_data_commit() {
    let fx = fixture();
    let env = setup("crash_ledger", &fx).await;
    apply_phase(&env, &fx, 0).await;
    let first = env.engine().run(options(&env)).await;
    assert!(first.success, "{}", first.message);
    assert_matches_v4(&env, &fx, 0).await;

    let (a, b) = (env.pipe_ref("a_raw"), env.pipe_ref("b_clean"));
    let seq_before = env.engine().state().latest_seq(INSTANCE, &a).await.unwrap();
    let cursor_before = env.engine().state().cursor(INSTANCE, &a, &b).await.unwrap();
    let rows_before = env.table("a_raw").await;

    apply_phase(&env, &fx, 1).await;
    let engine = env.engine().with_faults(CrashOnce::new(a.clone(), true));
    let crashed = engine.run(options(&env)).await;
    assert!(!crashed.success);
    assert_eq!(status(&crashed, &env, "a_raw"), NodeStatus::Failed);
    assert_eq!(
        status(&crashed, &env, "b_clean"),
        NodeStatus::UpstreamFailed
    );
    assert_eq!(
        status(&crashed, &env, "c_hourly"),
        NodeStatus::UpstreamFailed
    );
    // The ledger entry is written, the data is not, and no cursor moved.
    let seq_after = engine.state().latest_seq(INSTANCE, &a).await.unwrap();
    assert!(seq_after > seq_before, "{seq_before:?} → {seq_after:?}");
    assert_eq!(env.table("a_raw").await, rows_before);
    assert_eq!(
        engine.state().cursor(INSTANCE, &a, &b).await.unwrap(),
        cursor_before
    );

    // The next run syncs everything (the orphan entry is harmless: syncs are idempotent).
    let next = env.engine().run(options(&env)).await;
    assert!(next.success, "{}", next.message);
    assert_matches_v4(&env, &fx, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_between_data_commit_and_cursor_advance() {
    let fx = fixture();
    let env = setup("crash_cursor", &fx).await;
    apply_phase(&env, &fx, 0).await;
    let first = env.engine().run(options(&env)).await;
    assert!(first.success, "{}", first.message);

    let (a, b) = (env.pipe_ref("a_raw"), env.pipe_ref("b_clean"));
    let cursor_before = env
        .engine()
        .state()
        .cursor(INSTANCE, &a, &b)
        .await
        .unwrap()
        .expect("cursor after the first run");

    apply_phase(&env, &fx, 1).await;
    let engine = env.engine().with_faults(CrashOnce::new(b.clone(), false));
    let crashed = engine.run(options(&env)).await;
    assert!(!crashed.success);
    assert_eq!(status(&crashed, &env, "a_raw"), NodeStatus::Succeeded);
    assert_eq!(status(&crashed, &env, "b_clean"), NodeStatus::Failed);
    assert_eq!(
        status(&crashed, &env, "c_hourly"),
        NodeStatus::UpstreamFailed
    );
    // b_clean's data is committed, but its cursor on a_raw did not advance.
    let want_b = Table::from_fixture(&fx["phases"][1]["expected"]["b_clean"]);
    assert_eq!(env.table("b_clean").await, want_b);
    let cursor_after = engine
        .state()
        .cursor(INSTANCE, &a, &b)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor_after.seq, cursor_before.seq);

    // The next run re-plans b_clean from the unconsumed entries (an idempotent re-sync) and
    // propagates to c_hourly.
    let next = env.engine().run(options(&env)).await;
    assert!(next.success, "{}", next.message);
    assert_eq!(status(&next, &env, "b_clean"), NodeStatus::Succeeded);
    assert_eq!(status(&next, &env, "c_hourly"), NodeStatus::Succeeded);
    let cursor_final = env
        .engine()
        .state()
        .cursor(INSTANCE, &a, &b)
        .await
        .unwrap()
        .unwrap();
    assert!(cursor_final.seq > cursor_before.seq);
    assert_matches_v4(&env, &fx, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_reruns_only_failed_and_unrun_nodes() {
    let fx = fixture();
    let env = setup("resume", &fx).await;
    apply_phase(&env, &fx, 0).await;
    assert!(env.engine().run(options(&env)).await.success);

    apply_phase(&env, &fx, 1).await;
    let b = env.pipe_ref("b_clean");
    let crashed = env
        .engine()
        .with_faults(CrashOnce::new(b, false))
        .run(options(&env))
        .await;
    assert!(!crashed.success);
    let run_id = crashed.run_id().unwrap().to_string();

    // `sync pipes --resume <run_id>`: the selection comes from the persisted run.
    let resumed = env
        .engine()
        .run(RunOptions {
            resume: Some(run_id.clone()),
            ..Default::default()
        })
        .await;
    assert!(resumed.success, "{}", resumed.message);
    let record = resumed.record.as_ref().unwrap();
    assert_eq!(record.run.resumed_from.as_deref(), Some(run_id.as_str()));
    assert_eq!(record.nodes.len(), 3);
    assert_eq!(status(&resumed, &env, "a_raw"), NodeStatus::Skipped);
    assert_eq!(status(&resumed, &env, "b_clean"), NodeStatus::Succeeded);
    assert_eq!(status(&resumed, &env, "c_hourly"), NodeStatus::Succeeded);
    assert_matches_v4(&env, &fx, 1).await;
}
