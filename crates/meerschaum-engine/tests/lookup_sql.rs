//! Exercise lookup invalidation through real SQL staging, ledger writes and cursor persistence.

mod common;

use common::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::{SqlConnector, driver::SqlValue};
use meerschaum_core::success::Bound;
use meerschaum_engine::graph::EdgeRole;
use meerschaum_engine::{
    Action, ChunkSource, Depth, EdgeCursor, EventKind, InstanceState, RunOptions, Selection,
    StateSource, StateWriter, StaticRegistry,
};
use serde_json::json;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_lookup_replays_changed_windows_and_keeps_polling_without_a_time_cap() {
    bounded_lookup_scenario(env("bounded_lookup_sql").await, "").await;
}

async fn bounded_lookup_scenario(env: Env, prefix: &str) {
    let event_source = format!("{prefix}event_source");
    let state_source = format!("{prefix}state_source");
    env.exec(&format!(
        "CREATE TABLE {event_source} (ts INTEGER, id INTEGER)"
    ))
    .await;
    env.exec(&format!(
        "INSERT INTO {event_source} VALUES (10,1),(100,2),(200,3)"
    ))
    .await;
    env.exec(&format!(
        "CREATE TABLE {state_source} (ts INTEGER, id INTEGER, multiplier INTEGER)"
    ))
    .await;
    env.exec(&format!(
        "INSERT INTO {state_source} VALUES (5,1,1),(95,2,2),(195,3,3)"
    ))
    .await;
    let columns = json!({"datetime":"ts","id":"id"});
    env.register("events", &json!({"columns":columns,"dtypes":{"ts":"int","id":"int"},"sql":format!("SELECT * FROM {event_source}")})).await;
    env.register("catalog", &json!({"columns":columns,"dtypes":{"ts":"int","id":"int","multiplier":"int"},"sql":format!("SELECT * FROM {state_source}"),"fetch":{"backtrack_minutes":1000},"upsert":true,"null_indices":false})).await;
    let result_pipe = env.register("result", &json!({
        "columns":columns,"dtypes":{"ts":"int","id":"int","v":"int"},
        "sql":"SELECT e.ts, e.id, (SELECT c.multiplier FROM {{ Pipe('sql:eng','catalog') }} c WHERE c.ts <= e.ts AND c.ts > e.ts - 10 ORDER BY c.ts DESC LIMIT 1) AS v FROM {{ Pipe('sql:eng','events') }} e",
        "parents":["Pipe('sql:eng','events')",{"connector":"sql:eng","metric":"catalog","role":"lookup","lookback":10}],
        "verify":{"chunk_minutes":10},"fetch":{"backtrack_minutes":30},"upsert":true,"null_indices":false,
    })).await;
    let engine = env.engine();
    let seeded = engine
        .run(RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("events")])),
            ..Default::default()
        })
        .await;
    assert!(seeded.success, "{}", seeded.message);
    let options = || RunOptions {
        selection: Some(
            Selection::refs([env.pipe_ref("catalog"), env.pipe_ref("result")]).with_graph(true),
        ),
        workers: Some(2),
        ..Default::default()
    };
    let first = engine.run(options()).await;
    assert!(first.success, "{}", first.message);
    assert_eq!(
        env.rowcount("result").await,
        3,
        "An old lookup watermark must not cap current events."
    );
    let target = env
        .conn
        .pipe_table_name(&env.conn.sql_pipe(&result_pipe).unwrap());
    env.exec(&format!(
        "UPDATE {state_source} SET multiplier = 7 WHERE ts = 95"
    ))
    .await;
    let mut events = engine.subscribe();
    let revised = engine.run(options()).await;
    assert!(revised.success, "{}", revised.message);
    assert_eq!(
        revised
            .record
            .as_ref()
            .unwrap()
            .node(&env.pipe_ref("result"))
            .unwrap()
            .action,
        Action::Backfill
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
    assert_eq!(
        windows,
        vec![
            (Some(Bound::Int(90)), Some(Bound::Int(100))),
            (Some(Bound::Int(100)), Some(Bound::Int(110)))
        ]
    );
    for (ts, value) in [(10, 1), (100, 7), (200, 3)] {
        assert_eq!(
            env.conn
                .value(&format!(
                    "SELECT CAST(v AS BIGINT) FROM {target} WHERE ts={ts}"
                ))
                .await,
            Some(SqlValue::Int(value))
        );
    }
    env.exec(&format!(
        "UPDATE {state_source} SET multiplier = 4 WHERE ts = 195"
    ))
    .await;
    let recent = engine.run(options()).await;
    assert!(recent.success, "{}", recent.message);
    assert_eq!(
        recent
            .record
            .as_ref()
            .unwrap()
            .node(&env.pipe_ref("result"))
            .unwrap()
            .action,
        Action::Incremental
    );
    assert_eq!(
        env.conn
            .value(&format!(
                "SELECT CAST(v AS BIGINT) FROM {target} WHERE ts=200"
            ))
            .await,
        Some(SqlValue::Int(4))
    );
    let quiet = engine.run(options()).await;
    assert!(quiet.success, "{}", quiet.message);
    assert_eq!(
        quiet
            .record
            .as_ref()
            .unwrap()
            .node(&env.pipe_ref("result"))
            .unwrap()
            .action,
        Action::Root
    );
    assert_eq!(env.rowcount("result").await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookup_changes_recompute_history_but_identical_upserts_stay_quiet() {
    let env = env("lookup_sql").await;
    lookup_scenario(env, "").await;
}

async fn lookup_scenario(env: Env, prefix: &str) {
    env.exec(&format!(
        "CREATE TABLE {prefix}event_source (ts TIMESTAMP, id INTEGER, v INTEGER)"
    ))
    .await;
    env.exec(&format!("INSERT INTO {prefix}event_source VALUES ('2024-01-20 10:00:00', 1, 10), ('2024-01-20 11:00:00', 2, 20), ('2024-01-20 12:00:00', 3, 30)")).await;
    env.exec(&format!(
        "CREATE TABLE {prefix}catalog_source (ts TIMESTAMP, id INTEGER, multiplier INTEGER)"
    ))
    .await;
    env.exec(&format!(
        "INSERT INTO {prefix}catalog_source VALUES ('2024-01-01 00:00:00', 1, 2)"
    ))
    .await;
    let columns = json!({"datetime":"ts", "id":"id"});
    env.register(
        "events",
        &json!({
            "sql":format!("SELECT * FROM {prefix}event_source"), "columns":columns,
            "dtypes":{"ts":"datetime","id":"int","v":"int"},
            "fetch":{"backtrack_minutes":60}, "upsert":true, "null_indices":false,
        }),
    )
    .await;
    env.register(
        "catalog",
        &json!({
            "sql":format!("SELECT * FROM {prefix}catalog_source"), "columns":columns,
            "dtypes":{"ts":"datetime","id":"int","multiplier":"int"}, "upsert":true, "null_indices":false,
        }),
    )
    .await;
    env.register("result", &json!({
        "sql":"SELECT e.ts, e.id, e.v * c.multiplier AS v FROM {{ Pipe('sql:eng', 'events') }} e CROSS JOIN {{ Pipe('sql:eng', 'catalog') }} c",
        "columns":columns, "dtypes":{"ts":"datetime","id":"int","v":"int"},
        "parents":["Pipe('sql:eng', 'events')", {"connector":"sql:eng","metric":"catalog","role":"lookup"}],
        "fetch":{"backtrack_minutes":60}, "verify":{"chunk_days":30},
    })).await;
    let options = || RunOptions {
        selection: Some(Selection::refs([env.pipe_ref("result")]).with_parents(Some(Depth::All))),
        workers: Some(2),
        ..Default::default()
    };
    let engine = env.engine();
    let state = InstanceState::new(env.registry.clone(), INSTANCE);
    let first = engine.run(options()).await;
    assert!(first.success, "{}", first.message);
    let initial = env.table("result").await;
    assert_eq!(
        initial.rows.len(),
        3,
        "recent events must not be capped by the old catalog: {initial:?}"
    );
    assert!(
        initial.rows.iter().any(|r| r.contains("40.000000")),
        "{initial:?}"
    );
    let cursor = state
        .cursor(INSTANCE, &env.pipe_ref("catalog"), &env.pipe_ref("result"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.role, EdgeRole::Lookup);
    let before = state
        .latest_seq(INSTANCE, &env.pipe_ref("catalog"))
        .await
        .unwrap();

    let second = engine.run(options()).await;
    assert!(second.success, "{}", second.message);
    let result = second
        .record
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == env.pipe_ref("result"))
        .unwrap();
    assert_eq!(result.action, Action::Skip, "{result:?}");
    assert_eq!(
        state
            .latest_seq(INSTANCE, &env.pipe_ref("catalog"))
            .await
            .unwrap(),
        before,
        "an identical physical UPSERT is not a new logical change"
    );
    assert_eq!(env.table("result").await, initial);

    // The lookup timestamp stays old while its value changes.
    env.exec(&format!("UPDATE {prefix}catalog_source SET multiplier = 3"))
        .await;
    let changed = engine.run(options()).await;
    assert!(changed.success, "{}", changed.message);
    let result = changed
        .record
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == env.pipe_ref("result"))
        .unwrap();
    assert_eq!(result.action, Action::Backfill, "{result:?}");
    let revised = env.table("result").await;
    assert_eq!(revised.rows.len(), 3);
    assert!(
        revised.rows.iter().any(|r| r.contains("60.000000")),
        "{revised:?}"
    );
    assert_ne!(
        state
            .latest_seq(INSTANCE, &env.pipe_ref("catalog"))
            .await
            .unwrap(),
        before
    );

    env.exec(&format!(
        "INSERT INTO {prefix}event_source VALUES ('2024-01-20 13:00:00', 4, 40)"
    ))
    .await;
    env.conn.enable_query_log();
    let tail = engine.run(options()).await;
    assert!(tail.success, "{}", tail.message);
    let result = tail
        .record
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == env.pipe_ref("result"))
        .unwrap();
    assert_eq!(result.action, Action::Incremental, "{result:?}");
    assert_eq!(env.rowcount("result").await, 4);
    let log = env.conn.take_query_log();
    assert!(
        log.iter().any(|sql| sql.contains("_mrsm_pushdown")),
        "the incremental test must exercise source predicate pushdown"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn null_upsert_keys_emit_changed_values() {
    let env = env("lookup_null_keys").await;
    env.exec("CREATE TABLE nullable_source (ts INTEGER, id INTEGER, v INTEGER)")
        .await;
    env.exec("INSERT INTO nullable_source VALUES (10, NULL, 7)")
        .await;
    env.register(
        "events",
        &json!({
            "sql":"SELECT * FROM nullable_source", "columns":{"datetime":"ts","id":"id"},
            "dtypes":{"ts":"int","id":"int","v":"int"},
            "null_indices":false, "upsert":true,
        }),
    )
    .await;
    env.register(
        "sink",
        &json!({
            "sql":"SELECT * FROM {{ Pipe('sql:eng', 'events') }}",
            "columns":{"datetime":"ts","id":"id"},
            "dtypes":{"ts":"int","id":"int","v":"int"},
        }),
    )
    .await;
    let engine = env.engine();
    let options = || RunOptions {
        selection: Some(Selection::refs([env.pipe_ref("sink")]).with_parents(Some(Depth::All))),
        ..Default::default()
    };
    let first = engine.run(options()).await;
    assert!(first.success, "{}", first.message);
    let state = InstanceState::new(env.registry.clone(), INSTANCE);
    let before = state
        .latest_seq(INSTANCE, &env.pipe_ref("events"))
        .await
        .unwrap();
    env.exec("UPDATE nullable_source SET v = 8").await;
    let second = engine.run(options()).await;
    assert!(second.success, "{}", second.message);
    assert!(
        env.table("events")
            .await
            .rows
            .iter()
            .any(|r| r.contains("8.000000"))
    );
    assert!(
        state
            .latest_seq(INSTANCE, &env.pipe_ref("events"))
            .await
            .unwrap()
            > before
    );
}

async fn postgres_env() -> Option<(Env, String)> {
    let raw = std::env::var("MRSM_TEST_POSTGIS_URI").ok()?;
    let base = if raw.is_empty() {
        "postgis://test:test1234@localhost:5449/testdb"
    } else {
        &raw
    };
    let schema = format!("lookup_{}", uuid::Uuid::new_v4().simple());
    let sep = if base.contains('?') { '&' } else { '?' };
    let conn = Arc::new(
        SqlConnector::from_uri("eng", &format!("{base}{sep}schema={schema}"))
            .await
            .unwrap(),
    );
    assert!(
        conn.exec(&format!("CREATE SCHEMA \"{schema}\""))
            .await
            .is_some()
    );
    let instance: Arc<dyn InstanceConnector> = conn.clone();
    let source: Arc<dyn meerschaum_connector::Connector> = conn.clone();
    let sql = conn.clone();
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance(INSTANCE, instance)
            .with_source(INSTANCE, source)
            .with_table_reference(move |pipe| {
                let sp = sql.sql_pipe(pipe).map_err(|e| e.to_string())?;
                Ok(sql.pipe_table_name(&sp))
            }),
    );
    let dir = temp_dir("lookup_postgres");
    Some((
        Env {
            db: dir.join("unused"),
            dir,
            conn,
            registry,
        },
        schema,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_lookup_changes_and_noop_upserts() {
    let Some((env, schema)) = postgres_env().await else {
        return;
    };
    let conn = env.conn.clone();
    lookup_scenario(env, &format!("\"{schema}\".")).await;
    assert!(
        conn.exec(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_bounded_lookup_changes() {
    let Some((env, schema)) = postgres_env().await else {
        return;
    };
    let conn = env.conn.clone();
    bounded_lookup_scenario(env, &format!("\"{schema}\".")).await;
    assert!(
        conn.exec(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .is_some()
    );
}

#[tokio::test]
async fn existing_cursor_tables_gain_lookup_roles_without_losing_cursors() {
    let env = env("cursor_role_migration").await;
    env.exec("CREATE TABLE mrsm_engine_cursors (parent_ref TEXT, child_ref TEXT, seq BIGINT, parent_watermark_at_cursor TEXT, UNIQUE (parent_ref, child_ref))").await;
    let parent = env.pipe_ref("catalog");
    let child = env.pipe_ref("result");
    env.exec(&format!(
        "INSERT INTO mrsm_engine_cursors VALUES ('{parent}', '{child}', 7, NULL)"
    ))
    .await;
    let state = InstanceState::new(env.registry.clone(), INSTANCE);
    let old = state
        .cursor(INSTANCE, &parent, &child)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.role, EdgeRole::Data);
    assert_eq!(old.seq, 7);
    state
        .put_cursor(
            INSTANCE,
            EdgeCursor {
                role: EdgeRole::Lookup,
                ..old
            },
        )
        .await
        .unwrap();
    let migrated = state
        .cursor(INSTANCE, &parent, &child)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(migrated.role, EdgeRole::Lookup);
    assert_eq!(migrated.seq, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_lookups_block_their_children_but_not_independent_work() {
    let env = env("lookup_failure").await;
    env.register("catalog", &json!({"sql":"SELECT * FROM missing_catalog"}))
        .await;
    env.register(
        "result",
        &json!({
            "sql":"SELECT 1 AS id",
            "parents":[{"connector":"sql:eng","metric":"catalog","role":"lookup"}],
        }),
    )
    .await;
    env.register("independent", &json!({"sql":"SELECT 2 AS id"}))
        .await;
    let run = env
        .engine()
        .run(RunOptions {
            selection: Some(
                Selection::refs([env.pipe_ref("result"), env.pipe_ref("independent")])
                    .with_parents(Some(Depth::All)),
            ),
            workers: Some(2),
            ..Default::default()
        })
        .await;
    assert!(!run.success);
    let nodes = &run.record.as_ref().unwrap().nodes;
    let child = nodes
        .iter()
        .find(|n| n.pipe_ref == env.pipe_ref("result"))
        .unwrap();
    assert_eq!(child.action, Action::UpstreamFailed);
    assert_eq!(env.rowcount("result").await, 0);
    assert_eq!(env.rowcount("independent").await, 1);
}
