//! Dropping, deleting or fully clearing a pipe forgets its Syncing Engine state (wave 9): the
//! ledger entries it produced, the edge cursors it is part of and its definition hash, so a pipe
//! recreated under the same keys starts fresh, as in v4 (which has no engine state). Partial
//! clears keep the state; run history is always kept.

mod common;

use common::*;
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector};
use meerschaum_engine::{Depth, NodeStatus, RunOptions, RunResult, Selection, StateSource};

fn chain_params(sql: &str) -> serde_json::Value {
    serde_json::json!({
        "sql": sql,
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
    })
}

const CREATE: &str = "CREATE TABLE w9_src (ts TEXT, id INTEGER, v REAL)";
const SEED: &str = "INSERT INTO w9_src VALUES ('2026-01-01 00:00:00', 1, 1.0), \
    ('2026-01-01 01:00:00', 2, 2.0), ('2026-01-01 02:00:00', 1, 3.0)";

async fn register_chain(env: &Env) {
    env.register("w9_a", &chain_params("SELECT ts, id, v FROM w9_src"))
        .await;
    env.register(
        "w9_b",
        &chain_params(
            "SELECT ts, id, v FROM {{ Pipe('sql:eng', 'w9_a', instance='sql:eng') }} WHERE v >= 2",
        ),
    )
    .await;
}

fn options(env: &Env) -> RunOptions {
    RunOptions {
        selection: Some(Selection::refs([env.pipe_ref("w9_b")]).with_parents(Some(Depth::All))),
        ..Default::default()
    }
}

fn reason(result: &RunResult, env: &Env, metric: &str) -> String {
    result
        .plan
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == env.pipe_ref(metric))
        .unwrap()
        .reason
        .clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_delete_and_full_clear_forget_the_engine_state() {
    let env = env("w9_forget").await;
    env.exec(CREATE).await;
    env.exec(SEED).await;
    register_chain(&env).await;
    let engine = env.engine();
    let first = engine.run(options(&env)).await;
    assert!(first.success, "{}", first.message);
    let state = engine.state();
    let (a, b) = (env.pipe_ref("w9_a"), env.pipe_ref("w9_b"));
    assert!(state.latest_seq(INSTANCE, &a).await.unwrap().is_some());
    assert!(state.cursor(INSTANCE, &a, &b).await.unwrap().is_some());
    assert!(state.definition_hash(&b).await.unwrap().is_some());
    let conn: &dyn InstanceConnector = env.conn.as_ref();
    let pipe_a = env.pipe("w9_a", &serde_json::json!({}));
    let pipe_b = env.pipe("w9_b", &serde_json::json!({}));

    // A partial clear keeps the state.
    let partial = conn
        .clear_pipe(
            &pipe_b,
            &DataQuery {
                begin: DateBound::parse("2026-01-01 02:00:00"),
                ..Default::default()
            },
        )
        .await;
    assert!(partial.success, "{}", partial.message);
    assert!(state.cursor(INSTANCE, &a, &b).await.unwrap().is_some());
    assert!(state.definition_hash(&b).await.unwrap().is_some());

    // A full clear of the child forgets its cursor and hash (not the parent's ledger).
    let cleared = conn.clear_pipe(&pipe_b, &DataQuery::default()).await;
    assert!(cleared.success, "{}", cleared.message);
    assert!(state.cursor(INSTANCE, &a, &b).await.unwrap().is_none());
    assert!(state.definition_hash(&b).await.unwrap().is_none());
    assert!(state.latest_seq(INSTANCE, &a).await.unwrap().is_some());

    // Dropping the parent forgets the ledger it produced (and its own hash).
    let second = engine.run(options(&env)).await;
    assert!(second.success, "{}", second.message);
    assert!(state.cursor(INSTANCE, &a, &b).await.unwrap().is_some());
    let dropped = conn.drop_pipe(&pipe_a).await;
    assert!(dropped.success, "{}", dropped.message);
    assert_eq!(state.latest_seq(INSTANCE, &a).await.unwrap(), None);
    assert!(state.cursor(INSTANCE, &a, &b).await.unwrap().is_none());
    assert!(state.definition_hash(&a).await.unwrap().is_none());

    // Deleting forgets too; the run history stays.
    let third = engine.run(options(&env)).await;
    assert!(third.success, "{}", third.message);
    assert!(state.definition_hash(&b).await.unwrap().is_some());
    assert!(conn.delete_pipe(&pipe_b).await.success);
    assert!(state.definition_hash(&b).await.unwrap().is_none());
    assert!(state.cursor(INSTANCE, &a, &b).await.unwrap().is_none());
    assert!(
        state
            .load_run(first.run_id().unwrap())
            .await
            .unwrap()
            .is_some()
    );
}

/// A reset (drop and delete every pipe, recreate and reseed) followed by a graph run behaves
/// like the very first run: no stale cursor, watermark or ledger sequence is reused, and the
/// tables match a run on a fresh instance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reset_chain_starts_fresh() {
    let env = env("w9_reset").await;
    env.exec(CREATE).await;
    env.exec(SEED).await;
    register_chain(&env).await;
    let engine = env.engine();
    let first = engine.run(options(&env)).await;
    assert!(first.success, "{}", first.message);
    let want_a = env.table("w9_a").await;
    let want_b = env.table("w9_b").await;
    assert_eq!(want_b.rows.len(), 2);
    // More data and another run, so the stale state is well ahead of a fresh start.
    env.exec("INSERT INTO w9_src VALUES ('2026-01-01 03:00:00', 2, 4.0)")
        .await;
    assert!(engine.run(options(&env)).await.success);

    // The reset: drop and delete every pipe, recreate the source, register again.
    let conn: &dyn InstanceConnector = env.conn.as_ref();
    for metric in ["w9_b", "w9_a"] {
        let pipe = env.pipe(metric, &serde_json::json!({}));
        assert!(conn.drop_pipe(&pipe).await.success);
        assert!(conn.delete_pipe(&pipe).await.success);
    }
    env.exec("DROP TABLE w9_src").await;
    env.exec(CREATE).await;
    env.exec(SEED).await;
    register_chain(&env).await;

    let after = engine.run(options(&env)).await;
    assert!(after.success, "{}", after.message);
    let b_reason = reason(&after, &env, "w9_b");
    assert!(
        !b_reason.contains("unchanged since its cursor") && !b_reason.contains("unconsumed"),
        "stale state was reused: {b_reason}"
    );
    let status = after
        .record
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == env.pipe_ref("w9_b"))
        .unwrap()
        .status;
    assert_eq!(status, NodeStatus::Succeeded);
    assert_eq!(env.table("w9_a").await, want_a);
    assert_eq!(env.table("w9_b").await, want_b);
}
