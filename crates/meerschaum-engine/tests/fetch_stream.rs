//! A SQL fetch streamed chunk by chunk (`Connector::fetch_stream`) syncs exactly what the same
//! fetch read whole and cut into chunks syncs: the same messages, row counts and tables. A
//! source sharing its file with the instance is read whole (a held read would block writes).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use meerschaum_connector::pipe_ops::frame_to_docs;
use meerschaum_connector::{Connector, ConnectorResult, DataQuery, InstanceConnector, async_trait};
use meerschaum_connector_sql::{Flavor, SqlConnector};
use meerschaum_core::Pipe;
use meerschaum_core::success::SyncResult;
use meerschaum_engine::{Engine, EngineConfig, StaticRegistry, SyncArgs};
use meerschaum_kernel::Frame;
use serde_json::json;

const SOURCE: &str = "sql:src";

/// The source connector without streaming: the collected `fetch` cut into chunks.
struct Whole(Arc<SqlConnector>);

#[async_trait]
impl Connector for Whole {
    fn connector_type(&self) -> &str {
        "sql"
    }

    fn label(&self) -> &str {
        "src"
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }

    async fn fetch(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Option<Frame>> {
        Connector::fetch(self.0.as_ref(), pipe, query).await
    }
}

/// The source connector `sql:src` over the SQLite file at `db`.
async fn source_at(db: &std::path::Path) -> Arc<SqlConnector> {
    Arc::new(
        SqlConnector::from_uri("src", &format!("sqlite:///{}", db.display()))
            .await
            .expect("connect to SQLite"),
    )
}

/// One instance (`sql:eng`, its own file) fetching from `source`.
struct Side {
    instance: Arc<SqlConnector>,
    engine: Engine,
}

async fn side(dir: &std::path::Path, name: &str, source: Arc<dyn Connector>) -> Side {
    let instance = connect(&dir.join(format!("{name}.db"))).await;
    let as_instance: Arc<dyn InstanceConnector> = instance.clone();
    let registry = StaticRegistry::new()
        .with_instance(INSTANCE, as_instance)
        .with_source(SOURCE, source)
        .with_table_reference(|pipe: &Pipe| {
            let target = pipe.target(None).map_err(|e| e.to_string())?;
            Ok(meerschaum_connector_sql::ident::sql_item_name(
                &target,
                Flavor::Sqlite,
                None,
            ))
        });
    let config = EngineConfig {
        home_instance: INSTANCE.into(),
        lease_dir: dir.join(format!("leases_{name}")),
        lease_wait: Duration::from_secs(5),
        lease_poll: Duration::from_millis(50),
        state_flush: Duration::from_millis(20),
        holder: format!("test:{}", std::process::id()),
        ..EngineConfig::default()
    };
    Side {
        instance,
        engine: Engine::new(Arc::new(registry), config),
    }
}

impl Side {
    async fn sync(&self, pipe: &Pipe, chunksize: Option<i64>) -> SyncResult {
        let args = SyncArgs {
            chunksize,
            ..Default::default()
        };
        self.engine.sync_pipe(pipe, None, &args).await
    }

    /// The pipe's rows (sorted text) and column dtypes.
    async fn table(&self, pipe: &Pipe) -> (Vec<String>, Vec<(String, String)>) {
        let Some(df) = self
            .instance
            .get_pipe_data(pipe, &DataQuery::default())
            .await
            .expect("read pipe data")
        else {
            return Default::default();
        };
        let mut rows: Vec<String> = frame_to_docs(&df)
            .iter()
            .map(|doc| format!("{doc:?}"))
            .collect();
        rows.sort();
        let mut dtypes: Vec<(String, String)> = df.dtypes().into_iter().collect();
        dtypes.sort();
        (rows, dtypes)
    }
}

fn pipe(metric: &str, parameters: serde_json::Value) -> Pipe {
    Pipe::builder(SOURCE, metric)
        .instance(INSTANCE)
        .parameters(json_map(&parameters))
        .build()
        .expect("valid pipe")
}

/// The parts of a result both paths must agree on (durations differ).
fn summary(result: &SyncResult) -> (bool, String, u64, u64, u64) {
    (
        result.success,
        result.message.clone(),
        result.inserted,
        result.updated,
        result.upserted,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_fetch_syncs_like_the_whole_fetch() {
    let dir = temp_dir("fetch_stream");
    let source = source_at(&dir.join("source.db")).await;
    let exec = |sql: &'static str| {
        let source = source.clone();
        async move { assert!(source.exec(sql).await.is_some(), "failed: {sql}") }
    };
    exec("CREATE TABLE readings (ts TEXT, id INTEGER, value REAL, label TEXT)").await;
    exec(
        "INSERT INTO readings VALUES \
         ('2026-01-01 00:00:00', 1, 1.5, 'a'), ('2026-01-01 01:00:00', 1, 2.5, NULL), \
         ('2026-01-01 02:00:00', 2, NULL, 'c'), ('2026-01-01 03:00:00', 2, 4.5, 'd'), \
         ('2026-01-01 04:00:00', 3, 5.5, 'e'), ('2026-01-01 05:00:00', 3, 6.5, 'f'), \
         ('2026-01-01 06:00:00', 4, 7.5, 'g')",
    )
    .await;
    let streamed = side(&dir, "streamed", source.clone()).await;
    let whole = side(&dir, "whole", Arc::new(Whole(source.clone()))).await;

    let readings = pipe(
        "readings",
        json!({
            "fetch": {"definition": "SELECT * FROM readings"},
            "columns": {"datetime": "ts", "id": "id"},
        }),
    );
    let compare = |label: &'static str, chunksize: Option<i64>| {
        let (streamed, whole, readings) = (&streamed, &whole, readings.clone());
        async move {
            let a = streamed.sync(&readings, chunksize).await;
            let b = whole.sync(&readings, chunksize).await;
            assert!(a.success, "{label}: {}", a.message);
            assert_eq!(summary(&a), summary(&b), "{label}");
            assert_eq!(
                streamed.table(&readings).await,
                whole.table(&readings).await,
                "{label}"
            );
            a
        }
    };
    // First load: three chunks of 3, 3 and 1 rows.
    let first = compare("first", Some(3)).await;
    assert_eq!(first.inserted, 7);
    assert!(
        first.message.starts_with("Synced 3 chunks to Pipe("),
        "{}",
        first.message
    );
    assert!(first.message.contains("Synced a chunk (1 row)"));

    // Updates (the NULL value stays NULL) and new rows inside the backtrack window.
    exec("UPDATE readings SET value = value * 10 WHERE id = 2").await;
    exec(
        "INSERT INTO readings VALUES ('2026-01-01 07:00:00', 4, 8.5, 'h'), \
         ('2026-01-01 08:00:00', 5, 9.5, NULL)",
    )
    .await;
    let second = compare("updates", Some(4)).await;
    assert_eq!(
        (second.inserted, second.updated),
        (2, 1),
        "{}",
        second.message
    );

    // Nothing new; the configured default chunk size (one chunk).
    let unchanged = compare("unchanged", Some(-1)).await;
    assert_eq!(unchanged.rows_written(), 0);
    assert!(unchanged.message.starts_with("Synced 1 chunk to Pipe("));

    // `chunksize=None`: one transaction, never chunked or streamed.
    exec("INSERT INTO readings VALUES ('2026-01-01 09:00:00', 6, 10.5, 'j')").await;
    let single = compare("single", None).await;
    assert_eq!(single.inserted, 1);
    assert!(!single.message.contains("chunk"), "{}", single.message);

    // An empty result is one empty chunk.
    let empty = pipe(
        "empty",
        json!({"fetch": {"definition": "SELECT * FROM readings WHERE 1 = 0"}}),
    );
    let a = streamed.sync(&empty, Some(3)).await;
    let b = whole.sync(&empty, Some(3)).await;
    assert!(a.success, "{}", a.message);
    assert_eq!(summary(&a), summary(&b));
    assert!(a.message.starts_with("Synced 1 chunk to Pipe("));

    // A definition that fails before any row: the familiar fetch failure on both paths.
    let broken = pipe(
        "broken",
        json!({"fetch": {"definition": "SELECT * FROM no_such_table"}}),
    );
    let a = streamed.sync(&broken, Some(3)).await;
    let b = whole.sync(&broken, Some(3)).await;
    assert!(!a.success);
    assert_eq!(summary(&a), summary(&b));
    assert!(
        a.message.starts_with("No data were fetched for Pipe("),
        "{}",
        a.message
    );

    drop((streamed, whole, source));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The definition of a read that fails at row 120,001 (after the driver's first batch).
fn failing_definition() -> String {
    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 130000) \
     SELECT x AS id, CASE WHEN x > 120000 THEN json_extract('bad', '$') ELSE x END AS v FROM c"
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_failing_mid_stream_keeps_the_synced_chunks() {
    let dir = temp_dir("fetch_stream_fail");
    let source = source_at(&dir.join("source.db")).await;
    let streamed = side(&dir, "streamed", source.clone()).await;
    let failing = pipe(
        "failing",
        json!({"fetch": {"definition": failing_definition()}, "columns": {"id": "id"}}),
    );
    let result = streamed.sync(&failing, Some(50_000)).await;
    assert!(!result.success, "{}", result.message);
    assert!(
        result.message.contains("(2 succeeded, 1 failed)"),
        "{}",
        result.message
    );
    assert!(
        result
            .message
            .contains("Failed to fetch data from sql:src:\n    "),
        "{}",
        result.message
    );
    assert!(
        result.message.contains("malformed JSON"),
        "{}",
        result.message
    );
    assert_eq!(result.inserted, 100_000);
    let rows = streamed
        .instance
        .get_pipe_rowcount(&failing, &DataQuery::default())
        .await
        .expect("rowcount");
    assert_eq!(rows, Some(100_000));
    drop((streamed, source));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_source_sharing_the_instance_file_is_read_whole() {
    let dir = temp_dir("fetch_stream_shared");
    // Another connector to the instance's own file: streaming would hold a read lock on the
    // database the chunks are written to.
    let source = Arc::new(
        SqlConnector::from_uri(
            "src",
            &format!("sqlite:///{}", dir.join("shared.db").display()),
        )
        .await
        .expect("connect"),
    );
    let shared = side(&dir, "shared", source.clone()).await;
    assert_eq!(
        Connector::exclusive_storage(source.as_ref()),
        Connector::exclusive_storage(shared.instance.as_ref())
    );
    let failing = pipe(
        "failing",
        json!({"fetch": {"definition": failing_definition()}}),
    );
    // Read whole, the failure comes before any chunk is synced.
    let result = shared.sync(&failing, Some(50_000)).await;
    assert!(!result.success);
    assert!(
        result.message.starts_with("No data were fetched for Pipe("),
        "{}",
        result.message
    );
    assert_eq!(result.inserted, 0);
    drop((shared, source));
    let _ = std::fs::remove_dir_all(&dir);
}
