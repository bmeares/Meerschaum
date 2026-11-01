//! Approved UPSERT rebuilds retain data and only accept a new definition after a full pass.
mod common;

use common::*;
use meerschaum_connector_sql::driver::SqlValue;
use meerschaum_engine::{
    Action, ChunkSource, EventKind, FaultInjector, InstanceState, PipeRef, RunOptions, Selection,
    StateSource,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct InterruptFirstCommit(AtomicBool);

impl FaultInjector for InterruptFirstCommit {
    fn after_data_commit(&self, _: &PipeRef) -> Result<(), String> {
        if self.0.swap(false, Ordering::SeqCst) {
            Err("interrupted UPSERT rebuild".into())
        } else {
            Ok(())
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_rebuild_preserves_history_and_retries_without_accepting_a_partial_definition() {
    let env = env("upsert_rebuild").await;
    env.exec("CREATE TABLE source_rows (ts INTEGER, id INTEGER, v INTEGER)")
        .await;
    env.exec("INSERT INTO source_rows VALUES (10,1,1),(20,1,1),(30,1,1)")
        .await;
    let mut parameters = json!({
        "columns":{"datetime":"ts","id":"id"},
        "dtypes":{"ts":"int","id":"int","v":"int"},
        "precision":"second", "verify":{"chunk_range":10},
        "sql":"SELECT ts,id,v FROM source_rows", "upsert":true,"null_indices":false,
    });
    let pipe = env.register("result", &parameters).await;
    let options = |yes| RunOptions {
        selection: Some(Selection::refs([env.pipe_ref("result")]).with_graph(true)),
        yes,
        ..Default::default()
    };
    let first = env.engine().run(options(false)).await;
    assert!(first.success, "{}", first.message);
    let target = env.conn.pipe_table_name(&env.conn.sql_pipe(&pipe).unwrap());
    env.exec(&format!("INSERT INTO {target} VALUES (0,1,99),(50,1,99)"))
        .await;
    env.exec("UPDATE source_rows SET v=2").await;
    let state = InstanceState::new(env.registry.clone(), INSTANCE);
    let original_hash = state
        .definition_hash(&env.pipe_ref("result"))
        .await
        .unwrap();
    assert!(original_hash.is_some());

    parameters["sql"] = json!("SELECT ts,id,v*2 AS v FROM source_rows");
    parameters["rebuild_mode"] = json!("upsert");
    parameters["locks"] = json!({"clear":true,"drop":true});
    let changed = env.pipe("result", &parameters);
    let edited = env.conn.edit_pipe(&changed, false).await;
    assert!(edited.success, "{}", edited.message);
    let engine = env.engine();
    let pending = engine.plan(&options(false)).await.unwrap();
    assert_eq!(
        pending.plan.node(&env.pipe_ref("result")).unwrap().action,
        Action::RebuildPending
    );
    let approved = engine.plan(&options(true)).await.unwrap();
    let node = approved.plan.node(&env.pipe_ref("result")).unwrap();
    assert_eq!(node.action, Action::Backfill);
    assert!(!node.clear);
    assert!(node.chunks.first().unwrap().begin.is_none());
    assert!(node.chunks.last().unwrap().end.is_none());

    let interrupted = env
        .engine()
        .with_faults(Arc::new(InterruptFirstCommit(AtomicBool::new(true))))
        .run(options(true))
        .await;
    assert!(!interrupted.success);
    assert_eq!(
        state
            .definition_hash(&env.pipe_ref("result"))
            .await
            .unwrap(),
        original_hash
    );
    assert_eq!(env.rowcount("result").await, 5);

    let mut events = engine.subscribe();
    let replay = engine.run(options(true)).await;
    assert!(replay.success, "{}", replay.message);
    assert_eq!(env.rowcount("result").await, 5);
    assert_eq!(env.conn.value(&format!(
        "SELECT COUNT(*) FROM {target} WHERE (ts IN (0,50) AND v=99) OR (ts IN (10,20,30) AND v=4)"
    )).await, Some(SqlValue::Int(5)));
    assert_ne!(
        state
            .definition_hash(&env.pipe_ref("result"))
            .await
            .unwrap(),
        original_hash
    );
    let mut windows = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(
            event.kind,
            EventKind::ChunkStarted {
                source: ChunkSource::Window,
                ..
            }
        ) {
            windows += 1;
        }
    }
    assert!(windows > 1);
    assert_eq!(
        engine
            .plan(&options(false))
            .await
            .unwrap()
            .plan
            .node(&env.pipe_ref("result"))
            .unwrap()
            .action,
        Action::Root
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_rebuild_refuses_non_upsert_pipes_and_unknown_modes() {
    for (mode, upsert) in [("upsert", false), ("unknown", true)] {
        let env = env("invalid_rebuild").await;
        let mut parameters = json!({
            "columns":{"datetime":"ts"}, "dtypes":{"ts":"int","v":"int"},
            "sql":"SELECT 1 AS ts,1 AS v", "upsert":upsert,
        });
        env.register("result", &parameters).await;
        let options = RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("result")]).with_graph(true)),
            yes: true,
            ..Default::default()
        };
        let first = env.engine().run(options.clone()).await;
        assert!(first.success, "{}", first.message);
        parameters["sql"] = json!("SELECT 1 AS ts,2 AS v");
        parameters["rebuild_mode"] = json!(mode);
        let result = env
            .conn
            .edit_pipe(&env.pipe("result", &parameters), false)
            .await;
        assert!(result.success, "{}", result.message);
        let plan = env.engine().plan(&options).await.unwrap();
        let node = plan.plan.node(&env.pipe_ref("result")).unwrap();
        assert_eq!(node.action, Action::RebuildPending);
        assert!(!node.clear);
        assert_eq!(env.conn.value("SELECT 1").await, Some(SqlValue::Int(1)));
    }
}
