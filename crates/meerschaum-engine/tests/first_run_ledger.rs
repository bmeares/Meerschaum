//! An edge without a cursor (its first engine run, e.g. right after a reset) is decided by the
//! parent's unconsumed ledger entries, including those the parent writes earlier in the same
//! run, not only by comparing watermarks (wave 9). Late rows that a parent upserts inside the
//! child's already-synced range do not move the parent's watermark, yet the child must re-sync
//! them, as a v4 plain sync of both pipes does.
//!
//! Runs on SQLite in a temporary directory and on PostgreSQL-family when
//! `MRSM_TEST_POSTGIS_URI` is set (empty: the disposable container on 5449; schema
//! `w9s_first_run`, tables suffixed per run and dropped at the end).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{json_map, norm_kvalue, temp_dir};
use meerschaum_connector::pipe_ops::frame_to_docs;
use meerschaum_connector::{DataQuery, InstanceConnector};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::Pipe;
use meerschaum_engine::{
    Action, Depth, Engine, EngineConfig, PipeRef, RunOptions, Selection, StaticRegistry, SyncArgs,
};
use meerschaum_kernel::value::Value as KValue;

const KEYS: &str = "sql:w9first";
const SCHEMA: &str = "w9s_first_run";

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

/// `id | ts | v` lines of a pipe's table, sorted.
async fn table(conn: &SqlConnector, metric: &str) -> Vec<String> {
    let pipe = Pipe::builder(KEYS, metric).instance(KEYS).build().unwrap();
    let Some(df) = conn
        .get_pipe_data(&pipe, &DataQuery::default())
        .await
        .unwrap()
    else {
        return Vec::new();
    };
    let mut rows: Vec<String> = frame_to_docs(&df)
        .iter()
        .map(|d| {
            ["id", "ts", "v"]
                .iter()
                .map(|c| norm_kvalue(d.get(*c).unwrap_or(&KValue::Null)))
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect();
    rows.sort();
    rows
}

async fn scenario(conn: Arc<SqlConnector>, src: String, tag: &str) {
    let (parent, child) = (format!("w9f_parent_{tag}"), format!("w9f_child_{tag}"));
    let params = |sql: String| {
        json_map(&serde_json::json!({
            "sql": sql,
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }))
    };
    let parent_pipe = Pipe::builder(KEYS, &parent)
        .instance(KEYS)
        .parameters(params(format!("SELECT ts, id, v FROM {src}")))
        .build()
        .unwrap();
    let child_pipe = Pipe::builder(KEYS, &child)
        .instance(KEYS)
        .parameters(params(format!(
            "SELECT ts, id, v FROM {{{{ Pipe('{KEYS}', '{parent}', instance='{KEYS}') }}}}"
        )))
        .build()
        .unwrap();
    for p in [&parent_pipe, &child_pipe] {
        let res = conn.register_pipe(p).await;
        assert!(res.success, "{}", res.message);
    }
    let insert = (0..6)
        .map(|h| format!("('2026-01-01 {h:02}:00:00', {h}, {h}.5)"))
        .collect::<Vec<_>>()
        .join(", ");
    assert!(
        conn.exec(&format!("INSERT INTO {src} (ts, id, v) VALUES {insert}"))
            .await
            .is_some()
    );
    let dir = temp_dir(&format!("w9first_{tag}"));
    let engine = Engine::new(
        registry(&conn),
        EngineConfig {
            home_instance: KEYS.into(),
            lease_dir: dir.join("leases"),
            lease_poll: Duration::from_millis(50),
            state_flush: Duration::from_millis(20),
            holder: format!("w9first:{}", std::process::id()),
            ..EngineConfig::default()
        },
    );

    // Both pipes synced the v4 way (bare syncs: no engine state at all), as after a reset.
    for p in [&parent_pipe, &child_pipe] {
        let res = engine.sync_pipe(p, None, &SyncArgs::default()).await;
        assert!(res.success, "{}", res.message);
    }
    assert_eq!(table(&conn, &child).await, table(&conn, &parent).await);

    // Late data: two rows inside the child's synced range change; the parent's watermark does
    // not move.
    assert!(
        conn.exec(&format!("UPDATE {src} SET v = v + 100 WHERE id IN (2, 3)"))
            .await
            .is_some()
    );
    let run = engine
        .run(RunOptions {
            selection: Some(
                Selection::refs([PipeRef::new(KEYS, KEYS, &child, None)])
                    .with_parents(Some(Depth::All)),
            ),
            ..Default::default()
        })
        .await;
    assert!(run.success, "{}", run.message);
    let planned = run
        .plan
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == PipeRef::new(KEYS, KEYS, &child, None))
        .unwrap()
        .clone();
    // Planned against the parent's state before it ran, then re-planned when it finished.
    assert!(planned.provisional, "{planned:?}");
    let executed = run
        .record
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == PipeRef::new(KEYS, KEYS, &child, None))
        .unwrap()
        .clone();
    assert!(
        matches!(executed.action, Action::Backfill | Action::Incremental),
        "{executed:?}"
    );
    assert!(
        executed.reason.contains("new change"),
        "{}",
        executed.reason
    );
    let parent_rows = table(&conn, &parent).await;
    assert!(
        parent_rows.iter().any(|r| r.contains("102.5")),
        "{parent_rows:?}"
    );
    assert_eq!(
        table(&conn, &child).await,
        parent_rows,
        "the child picked up the late rows"
    );

    // The cursor was seeded from those entries: nothing is left to do.
    let again = engine
        .run(RunOptions {
            selection: Some(
                Selection::refs([PipeRef::new(KEYS, KEYS, &child, None)])
                    .with_parents(Some(Depth::All)),
            ),
            ..Default::default()
        })
        .await;
    assert!(again.success, "{}", again.message);
    let action = again
        .record
        .as_ref()
        .unwrap()
        .nodes
        .iter()
        .find(|n| n.pipe_ref == PipeRef::new(KEYS, KEYS, &child, None))
        .unwrap()
        .action;
    assert_eq!(action, Action::Skip);

    for p in [&parent_pipe, &child_pipe] {
        let _ = InstanceConnector::drop_pipe(conn.as_ref(), p).await;
        let _ = InstanceConnector::delete_pipe(conn.as_ref(), p).await;
    }
    let _ = conn.exec(&format!("DROP TABLE IF EXISTS {src}")).await;
    let _ = std::fs::remove_dir_all(&dir);
}

fn tag() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..8].to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_late_parent_rows_reach_a_child_without_a_cursor() {
    let dir = temp_dir("w9first_db");
    let uri = format!("sqlite:///{}", dir.join("w9first.db").display());
    let conn = Arc::new(SqlConnector::from_uri("w9first", &uri).await.unwrap());
    let tag = tag();
    let src = format!("w9f_src_{tag}");
    assert!(
        conn.exec(&format!("CREATE TABLE {src} (ts TEXT, id INTEGER, v REAL)"))
            .await
            .is_some()
    );
    scenario(conn, src, &tag).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgresql_late_parent_rows_reach_a_child_without_a_cursor() {
    let Ok(raw) = std::env::var("MRSM_TEST_POSTGIS_URI") else {
        eprintln!("skipped: MRSM_TEST_POSTGIS_URI is not set");
        return;
    };
    let base = if raw.is_empty() {
        "postgis://test:test1234@localhost:5449/testdb".to_string()
    } else {
        raw
    };
    let sep = if base.contains('?') { '&' } else { '?' };
    let conn = Arc::new(
        SqlConnector::from_uri("w9first", &format!("{base}{sep}schema={SCHEMA}"))
            .await
            .unwrap(),
    );
    let _ = conn
        .exec(&format!("CREATE SCHEMA IF NOT EXISTS \"{SCHEMA}\""))
        .await;
    let tag = tag();
    let src = format!("\"{SCHEMA}\".\"w9f_src_{tag}\"");
    assert!(
        conn.exec(&format!(
            "CREATE TABLE {src} (ts TIMESTAMP, id INTEGER, v DOUBLE PRECISION)"
        ))
        .await
        .is_some()
    );
    scenario(conn, src, &tag).await;
}
