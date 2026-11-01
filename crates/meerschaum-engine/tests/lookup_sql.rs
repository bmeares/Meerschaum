//! Exercise lookup invalidation through real SQL staging, ledger writes and cursor persistence.

mod common;

use common::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_engine::graph::EdgeRole;
use meerschaum_engine::{
    Action, Depth, EdgeCursor, InstanceState, RunOptions, Selection, StateSource, StateWriter,
    StaticRegistry,
};
use serde_json::json;
use std::sync::Arc;

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
            "fetch":{"backtrack_minutes":60}, "upsert":true,
        }),
    )
    .await;
    env.register(
        "catalog",
        &json!({
            "sql":format!("SELECT * FROM {prefix}catalog_source"), "columns":columns,
            "dtypes":{"ts":"datetime","id":"int","multiplier":"int"}, "upsert":true,
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
    assert!(
        env.conn
            .take_query_log()
            .iter()
            .any(|sql| sql.contains("_mrsm_pushdown")),
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
async fn postgres_inplace_lookup_changes_and_noop_upserts() {
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
