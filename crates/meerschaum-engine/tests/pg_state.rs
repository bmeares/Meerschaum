//! Engine state persists on PostgreSQL-family instances (wave 9): run history, node rows,
//! definition hashes and edge cursors written by one graph run are read back by the next, so a
//! caught-up child is `skip`ped (not re-planned as a first engine run) and new parent data makes
//! it `incremental`.
//!
//! Gated on `MRSM_TEST_POSTGIS_URI`; an empty value selects the disposable test container
//! (`postgis://test:test1234@localhost:5449/testdb`). Tables live in schema `w9s_state` with a
//! per-run suffix and are dropped at the end.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{json_map, temp_dir};
use meerschaum_connector::{DataQuery, InstanceConnector};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::Pipe;
use meerschaum_engine::{
    Action, Depth, Engine, EngineConfig, NodeStatus, PipeRef, RunOptions, RunResult, Selection,
    StateSource, StaticRegistry,
};

const KEYS: &str = "sql:w9state";
const SCHEMA: &str = "w9s_state";

fn uri() -> Option<String> {
    let raw = std::env::var("MRSM_TEST_POSTGIS_URI").ok()?;
    let base = if raw.is_empty() {
        "postgis://test:test1234@localhost:5449/testdb".to_string()
    } else {
        raw
    };
    let sep = if base.contains('?') { '&' } else { '?' };
    Some(format!("{base}{sep}schema={SCHEMA}"))
}

fn registry(conn: &Arc<SqlConnector>) -> Arc<StaticRegistry> {
    let instance: Arc<dyn InstanceConnector> = conn.clone();
    let source: Arc<dyn meerschaum_connector::Connector> = conn.clone();
    let sql = conn.clone();
    Arc::new(
        StaticRegistry::new()
            .with_instance(KEYS, instance)
            .with_source(KEYS, source)
            .with_table_reference(move |pipe: &Pipe| {
                let sp = sql.sql_pipe(pipe).map_err(|e| e.to_string())?;
                Ok(sql.pipe_table_name(&sp))
            }),
    )
}

fn status(result: &RunResult, pipe_ref: &PipeRef) -> NodeStatus {
    result
        .record
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == *pipe_ref)
        .unwrap()
        .status
}

fn planned(result: &RunResult, pipe_ref: &PipeRef) -> (Action, String) {
    let node = result
        .plan
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == *pipe_ref)
        .unwrap();
    (node.action, node.reason.clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_graph_run_sees_the_first_runs_state() {
    let Some(uri) = uri() else {
        eprintln!("skipped: MRSM_TEST_POSTGIS_URI is not set");
        return;
    };
    let conn = Arc::new(SqlConnector::from_uri("w9state", &uri).await.unwrap());
    // Concurrent creation can fail inside the server; the table creation below verifies it.
    let _ = conn
        .exec(&format!("CREATE SCHEMA IF NOT EXISTS \"{SCHEMA}\""))
        .await;
    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let src = format!("\"{SCHEMA}\".\"w9s_src_{tag}\"");
    let (raw, clean) = (format!("w9s_raw_{tag}"), format!("w9s_clean_{tag}"));
    assert!(
        conn.exec(&format!(
            "CREATE TABLE {src} (ts TIMESTAMP, id INTEGER, v DOUBLE PRECISION)"
        ))
        .await
        .is_some()
    );
    let insert = |hours: std::ops::Range<u32>| {
        let values: Vec<String> = hours
            .map(|h| format!("('2026-01-01 {h:02}:00:00', {}, {h}.5)", h % 2))
            .collect();
        format!("INSERT INTO {src} (ts, id, v) VALUES {}", values.join(", "))
    };
    let params = |sql: String| {
        json_map(&serde_json::json!({
            "sql": sql,
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }))
    };
    let raw_pipe = Pipe::builder(KEYS, &raw)
        .instance(KEYS)
        .parameters(params(format!("SELECT ts, id, v FROM {src}")))
        .build()
        .unwrap();
    let clean_pipe = Pipe::builder(KEYS, &clean)
        .instance(KEYS)
        .parameters(params(format!(
            "SELECT ts, id, v FROM {{{{ Pipe('{KEYS}', '{raw}', instance='{KEYS}') }}}} WHERE v > 1"
        )))
        .build()
        .unwrap();
    for p in [&raw_pipe, &clean_pipe] {
        let res = conn.register_pipe(p).await;
        assert!(res.success, "{}", res.message);
    }
    let dir = temp_dir("w9s_pg_state");
    let config = EngineConfig {
        home_instance: KEYS.into(),
        lease_dir: dir.join("leases"),
        lease_ttl: Duration::from_secs(5),
        lease_poll: Duration::from_millis(50),
        state_flush: Duration::from_millis(20),
        holder: format!("w9state:{}", std::process::id()),
        ..EngineConfig::default()
    };
    let engine = Engine::new(registry(&conn), config);
    let options = || RunOptions {
        selection: Some(
            Selection::refs([PipeRef::new(KEYS, KEYS, &clean, None)])
                .with_parents(Some(Depth::All)),
        ),
        ..Default::default()
    };
    let (a, b) = (
        PipeRef::new(KEYS, KEYS, &raw, None),
        PipeRef::new(KEYS, KEYS, &clean, None),
    );

    assert!(conn.exec(&insert(0..6)).await.is_some());
    let first = engine.run(options()).await;
    assert!(first.success, "{}", first.message);
    assert!(!first.message.contains("warning"), "{}", first.message);
    assert_eq!(status(&first, &b), NodeStatus::Succeeded);

    // Everything the first run wrote is on the instance.
    let state = engine.state();
    let run_id = first.run_id().unwrap().to_string();
    let stored = state
        .load_run(&run_id)
        .await
        .unwrap()
        .expect("the run row was persisted");
    assert_eq!(stored.nodes.len(), 2);
    assert!(
        stored
            .nodes
            .iter()
            .all(|n| n.status == NodeStatus::Succeeded),
        "{:?}",
        stored.nodes
    );
    assert!(state.definition_hash(&b).await.unwrap().is_some());
    let cursor = state
        .cursor(KEYS, &a, &b)
        .await
        .unwrap()
        .expect("the edge cursor was persisted");
    assert!(cursor.parent_watermark_at_cursor.is_some());
    assert!(state.watermark(&a).await.unwrap().is_some());

    // Nothing new: the child is caught up (not a first engine run).
    let second = engine.run(options()).await;
    assert!(second.success, "{}", second.message);
    let (action, reason) = planned(&second, &b);
    assert_eq!(action, Action::Skip, "{reason}");
    assert!(!reason.contains("first run with"), "{reason}");
    assert_eq!(status(&second, &b), NodeStatus::Skipped);

    // New parent rows: the child syncs incrementally from its cursor.
    assert!(conn.exec(&insert(6..9)).await.is_some());
    let third = engine.run(options()).await;
    assert!(third.success, "{}", third.message);
    assert_eq!(status(&third, &b), NodeStatus::Succeeded);
    let rows = conn
        .get_pipe_rowcount(&clean_pipe, &DataQuery::default())
        .await
        .unwrap();
    assert_eq!(rows, Some(8));
    assert!(
        state
            .load_run(third.run_id().unwrap())
            .await
            .unwrap()
            .is_some()
    );

    for p in [&raw_pipe, &clean_pipe] {
        let _ = conn.drop_pipe(p).await;
        let _ = conn.delete_pipe(p).await;
    }
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {src}")).await;
    let _ = std::fs::remove_dir_all(&dir);
}
