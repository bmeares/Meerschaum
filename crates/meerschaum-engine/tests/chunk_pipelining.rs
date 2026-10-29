//! `--chunk-pipelining` (R6): a child pre-syncs the windows its running parent has committed.
//!
//! On SQLite: `src -> raw (root) -> mid (incremental, many chunks) -> leaf`. After a first run,
//! six more months of source rows make `mid` a six-window incremental. With pipelining on,
//! `leaf` starts before `mid` finishes (an extra `node_started`), and every table is identical
//! to a run without pipelining.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use meerschaum_engine::{
    Depth, EventKind, FaultInjector, NodeStatus, PipeRef, RunOptions, Selection,
};

fn params(sql: &str) -> serde_json::Value {
    serde_json::json!({
        "sql": sql,
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        "upsert": true,
        "verify": {"chunk_days": 30},
        "fetch": {"backtrack_minutes": 60},
    })
}

/// Slows `mid` after every window commit so the child's early pass has time to run.
struct SlowMid(PipeRef);
impl FaultInjector for SlowMid {
    fn after_data_commit(&self, node: &PipeRef) -> Result<(), String> {
        if *node == self.0 {
            std::thread::sleep(Duration::from_millis(250));
        }
        Ok(())
    }
}

async fn setup(tag: &str) -> Env {
    let env = env(tag).await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec("INSERT INTO src VALUES ('2025-01-01 00:00:00', 1, 1.0)")
        .await;
    env.register("raw", &params("SELECT ts, id, v FROM src"))
        .await;
    env.register(
        "mid",
        &params("SELECT ts, id, v FROM {{ Pipe('sql:eng', 'raw', instance='sql:eng') }}"),
    )
    .await;
    env.register(
        "leaf",
        &params("SELECT ts, id, v * 2 AS v FROM {{ Pipe('sql:eng', 'mid', instance='sql:eng') }}"),
    )
    .await;
    env
}

async fn run(env: &Env, pipelining: bool) -> (meerschaum_engine::RunResult, Vec<(String, String)>) {
    let engine = env
        .engine()
        .with_faults(Arc::new(SlowMid(env.pipe_ref("mid"))));
    let mut events = engine.subscribe();
    let result = engine
        .run(RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("leaf")]).with_parents(Some(Depth::All))),
            chunk_pipelining: Some(pipelining),
            ..Default::default()
        })
        .await;
    let mut log = Vec::new();
    while let Ok(e) = events.try_recv() {
        if let Some(r) = &e.pipe_ref {
            let name = r.parts().metric_key.to_string();
            match e.kind {
                EventKind::NodeStarted { .. } => log.push((name, "started".to_string())),
                EventKind::NodeFinished { .. } => log.push((name, "finished".to_string())),
                _ => {}
            }
        }
    }
    (result, log)
}

async fn load_months(env: &Env) {
    let mut values = Vec::new();
    for month in 2..=7 {
        for day in [1, 10, 20] {
            values.push(format!(
                "('2025-{month:02}-{day:02} 00:00:00', {}, {}.5)",
                month * 100 + day,
                month
            ));
        }
    }
    env.exec(&format!("INSERT INTO src VALUES {}", values.join(", ")))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_run_overlaps_and_matches_the_sequential_tables() {
    let piped = setup("pipe_on").await;
    let plain = setup("pipe_off").await;
    for env in [&piped, &plain] {
        let (first, _) = run(env, false).await;
        assert!(first.success, "{}", first.message);
        load_months(env).await;
    }
    let (off, off_log) = run(&plain, false).await;
    let (on, on_log) = run(&piped, true).await;
    assert!(off.success, "{}", off.message);
    assert!(on.success, "{}", on.message);

    // Same data either way.
    for metric in ["raw", "mid", "leaf"] {
        assert_eq!(
            piped.table(metric).await.rows,
            plain.table(metric).await.rows,
            "{metric}"
        );
    }
    assert_eq!(piped.rowcount("leaf").await, 19);

    // Sequential: leaf starts after mid finished. Pipelined: leaf starts before mid finishes.
    let pos = |log: &[(String, String)], who: &str, what: &str| {
        log.iter().position(|(n, w)| n == who && w == what)
    };
    let off_start = pos(&off_log, "leaf", "started").unwrap();
    assert!(
        off_start > pos(&off_log, "mid", "finished").unwrap(),
        "{off_log:?}"
    );
    let on_start = pos(&on_log, "leaf", "started").unwrap();
    assert!(
        on_start < pos(&on_log, "mid", "finished").unwrap(),
        "{on_log:?}"
    );

    // Every node ends succeeded; leaf's counts include its early passes.
    let record = on.record.as_ref().unwrap();
    assert!(
        record
            .nodes
            .iter()
            .all(|n| n.status == NodeStatus::Succeeded)
    );
    let leaf = record.node(&piped.pipe_ref("leaf")).unwrap();
    // The early passes' work is counted on the node (upserts are counted per pass).
    assert!(
        leaf.inserted + leaf.updated + leaf.upserted >= 18,
        "{leaf:?}"
    );
    assert!(
        leaf.reason.contains("pre-synced") || leaf.attempts >= 1,
        "{leaf:?}"
    );

    // The cursors advanced exactly like a sequential run's: the next run behaves the same
    // (an upsert root re-upserts its backtrack row, so children see that one entry).
    let (again_on, _) = run(&piped, false).await;
    let (again_off, _) = run(&plain, false).await;
    for metric in ["raw", "mid", "leaf"] {
        let status = |r: &meerschaum_engine::RunResult, env: &Env| {
            let n = r
                .record
                .as_ref()
                .unwrap()
                .node(&env.pipe_ref(metric))
                .unwrap();
            (n.status, n.action, n.upserted)
        };
        assert_eq!(
            status(&again_on, &piped),
            status(&again_off, &plain),
            "{metric}"
        );
    }
    assert_eq!(
        piped.table("leaf").await.rows,
        plain.table("leaf").await.rows
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelining_off_by_default() {
    let env = setup("pipe_default").await;
    let (first, _) = run(&env, false).await;
    assert!(first.success);
    load_months(&env).await;
    let engine = env.engine();
    let mut events = engine.subscribe();
    let result = engine
        .run(RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("leaf")]).with_parents(Some(Depth::All))),
            ..Default::default()
        })
        .await;
    assert!(result.success);
    let mut leaf_starts = 0;
    while let Ok(e) = events.try_recv() {
        if matches!(e.kind, EventKind::NodeStarted { .. })
            && e.pipe_ref
                .as_ref()
                .is_some_and(|r| r.parts().metric_key == "leaf")
        {
            leaf_starts += 1;
        }
    }
    assert_eq!(leaf_starts, 1);
}
