//! Locks in the Syncing Engine (rc15 wave 2, AC5): a pipe locked against syncs is not synced in
//! a run and its node ends **locked**, which is not a failure, and the pipes downstream sync
//! from its current data. A rebuild (a definition change) is a drop: a `drop` or `clear` lock
//! keeps it from running, `--yes` included. A bare sync of a locked pipe is refused before its
//! source is read, and a replacing sync needs `clear` unlocked too.

mod common;

use common::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Lock, Locks, is_lock_refusal};
use meerschaum_engine::{Action, Depth, KeyFilter, NodeStatus, RunOptions, Selection, SyncArgs};
use serde_json::json;

async fn setup(tag: &str) -> Env {
    let env = env(tag).await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec(
        "INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0), ('2026-01-01 01:00:00', 1, 2.0)",
    )
    .await;
    env.register(
        "raw",
        &json!({
            "sql": "SELECT ts, id, v FROM src",
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }),
    )
    .await;
    env.register(
        "clean",
        &json!({
            "sql": "SELECT ts, id, v * 2 AS v FROM {{ Pipe('sql:eng', 'raw', instance='sql:eng') }}",
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }),
    )
    .await;
    env
}

fn graph_run(env: &Env, yes: bool) -> RunOptions {
    RunOptions {
        selection: Some(Selection::refs([env.pipe_ref("clean")]).with_parents(Some(Depth::All))),
        yes,
        ..Default::default()
    }
}

async fn lock(env: &Env, metric: &str, locks: &[Lock]) {
    let res = env
        .conn
        .lock_pipe(&env.pipe(metric, &json!({})), &Locks::of(locks))
        .await;
    assert!(res.success, "{}", res.message);
}

async fn unlock(env: &Env, metric: &str) {
    let res = env
        .conn
        .unlock_pipe(&env.pipe(metric, &json!({})), &Locks::all())
        .await;
    assert!(res.success, "{}", res.message);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sync_locked_parent_is_locked_and_its_children_use_its_data() {
    let env = setup("locks_parent").await;
    let engine = env.engine();
    let first = engine.run(graph_run(&env, false)).await;
    assert!(first.success, "{}", first.message);
    assert_eq!(env.rowcount("raw").await, 2);
    assert_eq!(env.rowcount("clean").await, 2);

    // New source rows, but `raw` is locked against syncs.
    env.exec("INSERT INTO src VALUES ('2026-01-01 02:00:00', 1, 3.0)")
        .await;
    lock(&env, "raw", &[Lock::Sync]).await;
    let run = engine.run(graph_run(&env, false)).await;
    assert!(
        run.success,
        "a locked node is not a failure: {}",
        run.message
    );
    assert!(run.message.contains("1 locked"), "{}", run.message);
    let record = run.record.as_ref().expect("a run record");
    let raw = record.node(&env.pipe_ref("raw")).expect("raw's node");
    assert_eq!(raw.status, NodeStatus::Locked);
    assert_eq!(raw.action, Action::Locked);
    assert!(raw.reason.contains("it is locked (sync)"), "{}", raw.reason);
    assert!(
        raw.reason
            .contains("mrsm unlock pipes -c sql:eng -m raw -i sql:eng --locks sync"),
        "{}",
        raw.reason
    );
    let clean = record.node(&env.pipe_ref("clean")).expect("clean's node");
    assert_ne!(clean.status, NodeStatus::Failed);
    assert_ne!(clean.status, NodeStatus::UpstreamFailed);
    assert!(
        run.warnings
            .iter()
            .any(|w| w.contains("it is locked (sync)")),
        "{:?}",
        run.warnings
    );
    // `raw` kept its rows; `clean` still has what `raw` has.
    assert_eq!(env.rowcount("raw").await, 2);
    assert_eq!(env.rowcount("clean").await, 2);

    // Unlocked: both catch up.
    unlock(&env, "raw").await;
    let run = engine.run(graph_run(&env, false)).await;
    assert!(run.success, "{}", run.message);
    assert_eq!(env.rowcount("raw").await, 3);
    assert_eq!(env.rowcount("clean").await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flat_run_skips_locked_pipes_as_locked() {
    let env = setup("locks_flat").await;
    let flat = || RunOptions {
        selection: Some(Selection::filter(KeyFilter::default())),
        ..Default::default()
    };
    let engine = env.engine();
    let first = engine.run(flat()).await;
    assert!(first.success, "{}", first.message);
    assert_eq!(env.rowcount("raw").await, 2);

    env.exec("INSERT INTO src VALUES ('2026-01-01 02:00:00', 1, 3.0)")
        .await;
    lock(&env, "raw", &[Lock::All]).await;
    let run = engine.run(flat()).await;
    assert!(run.success, "{}", run.message);
    let record = run.record.as_ref().expect("a run record");
    let raw = record.node(&env.pipe_ref("raw")).expect("raw's node");
    assert_eq!(raw.status, NodeStatus::Locked);
    assert!(raw.reason.contains("it is locked (all)"), "{}", raw.reason);
    assert!(raw.reason.contains("--locks sync"), "{}", raw.reason);
    assert_eq!(env.rowcount("raw").await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebuild_of_a_drop_locked_pipe_is_locked_even_with_yes() {
    let env = setup("locks_rebuild").await;
    let engine = env.engine();
    let first = engine.run(graph_run(&env, false)).await;
    assert!(first.success, "{}", first.message);
    assert_eq!(env.rowcount("clean").await, 2);

    // A new definition means a rebuild (clear, then sync everything again).
    lock(&env, "clean", &[Lock::Drop]).await;
    let edited = env.pipe(
        "clean",
        &json!({
            "sql": "SELECT ts, id, v * 3 AS v FROM {{ Pipe('sql:eng', 'raw', instance='sql:eng') }}",
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
            "locks": ["drop"],
        }),
    );
    let res = env.conn.edit_pipe(&edited, false).await;
    assert!(
        res.success,
        "a `drop` lock allows parameter edits: {}",
        res.message
    );
    let before = env.table("clean").await;

    let run = engine.run(graph_run(&env, true)).await;
    assert!(run.success, "{}", run.message);
    let record = run.record.as_ref().expect("a run record");
    let clean = record.node(&env.pipe_ref("clean")).expect("clean's node");
    assert_eq!(clean.status, NodeStatus::Locked, "{}", clean.reason);
    assert!(
        clean.reason.contains("its definition changed"),
        "{}",
        clean.reason
    );
    assert!(
        clean.reason.contains("it is locked (drop)"),
        "{}",
        clean.reason
    );
    assert!(clean.reason.contains("--locks drop"), "{}", clean.reason);
    assert_eq!(
        env.table("clean").await,
        before,
        "the table was not cleared"
    );

    // A `clear` lock refuses the rebuild too.
    unlock(&env, "clean").await;
    lock(&env, "clean", &[Lock::Clear]).await;
    let run = engine.run(graph_run(&env, true)).await;
    let clean = run
        .record
        .as_ref()
        .and_then(|r| r.node(&env.pipe_ref("clean")).cloned())
        .expect("clean's node");
    assert_eq!(clean.status, NodeStatus::Locked, "{}", clean.reason);
    assert!(
        clean.reason.contains("it is locked (clear)"),
        "{}",
        clean.reason
    );

    // Unlocked, `--yes` rebuilds it with the new definition.
    unlock(&env, "clean").await;
    let run = engine.run(graph_run(&env, true)).await;
    assert!(run.success, "{}", run.message);
    let clean = run
        .record
        .as_ref()
        .and_then(|r| r.node(&env.pipe_ref("clean")).cloned())
        .expect("clean's node");
    assert_eq!(clean.action, Action::Rebuild, "{}", clean.reason);
    assert_eq!(clean.status, NodeStatus::Succeeded);
    let after = env.table("clean").await;
    assert_eq!(after.rows.len(), 2);
    assert_ne!(after, before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_sync_of_a_locked_pipe_is_refused() {
    let env = setup("locks_bare").await;
    let engine = env.engine();
    let raw = env.pipe("raw", &json!({}));
    assert!(
        engine
            .sync_pipe(&raw, None, &SyncArgs::default())
            .await
            .success
    );
    assert_eq!(env.rowcount("raw").await, 2);

    lock(&env, "raw", &[Lock::Sync]).await;
    env.exec("INSERT INTO src VALUES ('2026-01-01 02:00:00', 1, 3.0)")
        .await;
    let res = engine.sync_pipe(&raw, None, &SyncArgs::default()).await;
    assert!(!res.success);
    assert!(is_lock_refusal(&res.message), "{}", res.message);
    assert!(
        res.message.starts_with(
            "Cannot sync Pipe('sql:eng', 'raw', instance='sql:eng'): it is locked (sync)."
        ),
        "{}",
        res.message
    );
    assert_eq!(env.rowcount("raw").await, 2);

    // A replacing sync deletes the rows of its window: `clear` refuses it as well.
    unlock(&env, "raw").await;
    lock(&env, "raw", &[Lock::Clear]).await;
    let replace = SyncArgs {
        replace: true,
        ..SyncArgs::default()
    };
    let res = engine.sync_pipe(&raw, None, &replace).await;
    assert!(!res.success);
    assert!(
        res.message.contains("it is locked (clear)"),
        "{}",
        res.message
    );
    assert!(res.message.contains("--locks clear"), "{}", res.message);
    // A plain sync is allowed under `clear`.
    let res = engine.sync_pipe(&raw, None, &SyncArgs::default()).await;
    assert!(res.success, "{}", res.message);
    assert_eq!(env.rowcount("raw").await, 3);
}
