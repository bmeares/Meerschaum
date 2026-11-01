//! Full lookup replays split stored history without losing rows outside the stored extent.
mod common;

use common::*;
use meerschaum_connector::SyncOptions;
use meerschaum_connector_sql::driver::SqlValue;
use meerschaum_engine::{
    ChunkSource, Depth, EventKind, FaultInjector, InstanceState, PipeRef, RunOptions, Selection,
    StateSource,
};
use meerschaum_kernel::{enforce::PipeInput, value::Value};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct FailOnce(PipeRef, AtomicBool);

impl FaultInjector for FailOnce {
    fn after_data_commit(&self, node: &PipeRef) -> Result<(), String> {
        if *node == self.0 && self.1.swap(false, Ordering::SeqCst) {
            return Err("interrupted historical replay".into());
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_lookup_replay_keeps_open_edges_and_retries_without_advancing_cursor() {
    let env = env("lookup_replay").await;
    env.exec("CREATE TABLE event_source (ts INTEGER, id INTEGER, v INTEGER)")
        .await;
    env.exec("INSERT INTO event_source VALUES (5,1,1),(10,1,1),(20,1,1),(30,1,1),(45,1,1)")
        .await;
    env.register(
        "catalog",
        &json!({
            "columns":{"datetime":"ts","id":"id"},
            "dtypes":{"ts":"int","id":"int","multiplier":"int"},
            "sql":"SELECT 0 AS ts, 1 AS id, 2 AS multiplier", "upsert":true,
        }),
    )
    .await;
    let result_pipe = env.register("result", &json!({
        "columns":{"datetime":"ts","id":"id"},
        "dtypes":{"ts":"int","id":"int","v":"int"},
        "sql":"SELECT e.ts, e.id, e.v * c.multiplier AS v FROM event_source e CROSS JOIN {{ Pipe('sql:eng','catalog') }} c",
        "parents":[{"connector":"sql:eng","metric":"catalog","role":"lookup"}],
        "verify":{"chunk_minutes":10}, "upsert":true, "null_indices":false,
    })).await;
    let rows = [10, 20, 30]
        .map(|ts| {
            [
                ("ts".into(), Value::Int(ts)),
                ("id".into(), Value::Int(1)),
                ("v".into(), Value::Int(1)),
            ]
            .into_iter()
            .collect()
        })
        .to_vec();
    let seed = env
        .conn
        .sync(&result_pipe, PipeInput::Docs(rows), &SyncOptions::default())
        .await;
    assert!(seed.success, "{}", seed.message);
    let options = || RunOptions {
        selection: Some(Selection::refs([env.pipe_ref("result")]).with_parents(Some(Depth::All))),
        ..Default::default()
    };
    let engine = env.engine().with_faults(Arc::new(FailOnce(
        env.pipe_ref("result"),
        AtomicBool::new(true),
    )));
    let failed = engine.run(options()).await;
    assert!(!failed.success);
    let state = InstanceState::new(env.registry.clone(), INSTANCE);
    assert!(
        state
            .cursor(INSTANCE, &env.pipe_ref("catalog"), &env.pipe_ref("result"))
            .await
            .unwrap()
            .is_none()
    );

    let mut events = engine.subscribe();
    let retried = engine.run(options()).await;
    assert!(retried.success, "{}", retried.message);
    assert_eq!(env.rowcount("result").await, 5);
    let target = env
        .conn
        .pipe_table_name(&env.conn.sql_pipe(&result_pipe).unwrap());
    assert_eq!(
        env.conn
            .value(&format!("SELECT COUNT(*) FROM {target} WHERE v <> 2"))
            .await,
        Some(SqlValue::Int(0))
    );
    assert!(
        state
            .cursor(INSTANCE, &env.pipe_ref("catalog"), &env.pipe_ref("result"))
            .await
            .unwrap()
            .is_some()
    );
    let mut windows = Vec::new();
    while let Ok(event) = events.try_recv() {
        if event.pipe_ref.as_ref() != Some(&env.pipe_ref("result")) {
            continue;
        }
        if let EventKind::ChunkStarted {
            source: ChunkSource::Window,
            begin,
            end,
            ..
        } = event.kind
        {
            windows.push((begin, end));
        }
    }
    assert!(windows.len() >= 3, "{windows:?}");
    assert!(windows.first().unwrap().0.is_none());
    assert!(windows.last().unwrap().1.is_none());
    assert!(
        windows.windows(2).all(|pair| pair[0].1 == pair[1].0),
        "{windows:?}"
    );
    assert_eq!(
        retried
            .record
            .as_ref()
            .unwrap()
            .node(&env.pipe_ref("result"))
            .unwrap()
            .num_chunks,
        windows.len()
    );
}
