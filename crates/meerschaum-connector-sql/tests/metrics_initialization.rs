//! Cold metrics readiness must tolerate independent writers without hiding real DDL errors.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_connector::InstanceConnector;
use meerschaum_connector::sync_metrics::{MetricsQuery, SyncMetric};
use meerschaum_connector::sync_status::{SyncOutcome, SyncSource};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::queries::table::{
    CreateTableOptions, QueryOrDtypes, get_create_table_queries,
};
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_core::Pipe;

const INSTANCE: &str = "sql:metrics_init";

fn directory() -> tempfile::TempDir {
    let root = std::env::var_os("MRSM_ROOT_DIR").expect("tests require a scratch root");
    tempfile::Builder::new()
        .prefix("metrics_initialization_")
        .tempdir_in(root)
        .unwrap()
}

async fn connector(path: &Path) -> SqlConnector {
    let uri = format!("sqlite:///{}", path.display());
    let mut spec = ConnectionSpec::from_uri(&uri, Some("metrics_init"), None).unwrap();
    spec.pool_size = 1;
    spec.max_overflow = 0;
    SqlConnector::connect("metrics_init", spec).await.unwrap()
}

fn source_pipe() -> Pipe {
    Pipe::builder("sql:synthetic", "probe")
        .instance(INSTANCE)
        .build()
        .unwrap()
}

async fn metric(conn: &SqlConnector) -> SyncMetric {
    let pipe = source_pipe();
    assert!(conn.get_pipe_id(&pipe).await.unwrap().is_some());
    SyncMetric::from_outcome(
        &pipe,
        &SyncOutcome::new(true, "synthetic", SyncSource::Python, None)
            .with_counts(1, 0, 0)
            .with_duration(Some(Duration::from_millis(1))),
    )
}

async fn empty_table(conn: &SqlConnector, metric: &str) -> Pipe {
    let pipe = conn.engine_state_pipe(INSTANCE, metric).unwrap();
    let sp = conn.sql_pipe(&pipe).unwrap();
    let dtypes = pipe.parameters()["dtypes"]
        .as_map()
        .unwrap()
        .iter()
        .map(|(name, dtype)| (name.clone(), dtype.py_str()))
        .collect();
    let queries = get_create_table_queries(
        &QueryOrDtypes::Dtypes(dtypes),
        &sp.target,
        conn.flavor(),
        &CreateTableOptions::default(),
    )
    .unwrap();
    assert!(conn.exec_queries_all(&queries, true, true).await);
    pipe
}

#[tokio::test]
async fn stale_internal_index_plans_are_safe_after_an_independent_initializer() {
    let dir = directory();
    let path = dir.path().join("stale.db");
    let first = connector(&path).await;
    let second = connector(&path).await;
    let pipe = empty_table(&first, "engine_metrics").await;
    // Both callers observe the absent indexes before either executes its DDL.
    // Replaying the stale second plan deterministically exercises the catalog race.
    let first_plan = first.get_create_index_queries(&pipe).await.unwrap();
    let stale_plan = second.get_create_index_queries(&pipe).await.unwrap();
    assert_eq!(first_plan, stale_plan);
    let queries: Vec<_> = first_plan.values().flatten().cloned().collect();
    assert!(
        queries
            .iter()
            .any(|query| query.starts_with("CREATE UNIQUE INDEX"))
    );
    assert!(
        queries
            .iter()
            .any(|query| query.starts_with("CREATE INDEX"))
    );
    assert!(first.exec_queries_all(&queries, true, true).await);
    let queries: Vec<_> = stale_plan.values().flatten().cloned().collect();
    assert!(second.exec_queries_all(&queries, true, true).await);

    // The runtime guard is reserved for temporary mrsm tables. An ordinary pipe
    // retains the public index creation behavior, even if it uses the same target.
    for (keys, temporary) in [("sql:synthetic", true), ("mrsm", false)] {
        let ordinary = Pipe::builder(keys, "ordinary")
            .instance(INSTANCE)
            .parameters(pipe.parameters().clone())
            .kwarg("temporary", temporary)
            .build()
            .unwrap();
        let plan = first.get_create_index_queries(&ordinary).await.unwrap();
        let query = plan
            .values()
            .flatten()
            .find(|query| query.starts_with("CREATE INDEX "))
            .expect("ordinary datetime index is still emitted");
        assert!(!query.contains("IF NOT EXISTS"), "{query}");
        assert!(
            !first
                .exec_queries_all(std::slice::from_ref(query), true, true)
                .await
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_initialization_and_first_metrics_write_share_readiness() {
    let dir = directory();
    for round in 0..6 {
        let path = dir.path().join(format!("init_write_{round}.db"));
        let writer = Arc::new(connector(&path).await);
        let initializer = connector(&path).await;
        assert!(writer.register_pipe(&source_pipe()).await.success);
        let outcome = metric(&writer).await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let write_barrier = barrier.clone();
        let write = async {
            write_barrier.wait().await;
            writer
                .record_sync_metrics(std::slice::from_ref(&outcome))
                .await
        };
        let initialize = async {
            barrier.wait().await;
            initializer.initialize_engine_metrics().await
        };
        let (written, initialized) = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(write, initialize)
        })
        .await
        .expect("one-connection pools must not deadlock");
        written.unwrap();
        initialized.unwrap();
        // A new reader proves the first batch was durably retained, rather than
        // merely treating the duplicate DDL as a successful no-op.
        let reader = connector(&path).await;
        let result = reader
            .get_engine_metrics(&MetricsQuery::default())
            .await
            .unwrap();
        assert_eq!(result.summary.syncs, 1, "round {round}");
        assert_eq!(result.summary.rows, 1);
        assert_eq!(result.history[0].event_id, outcome.event_id);
        reader.initialize_engine_metrics().await.unwrap();
        for metric in ["engine_metrics", "engine_runs", "engine_nodes"] {
            let pipe = reader.engine_state_pipe(INSTANCE, metric).unwrap();
            assert!(reader.pipe_exists(&pipe).await.unwrap());
        }
    }
}

#[tokio::test]
async fn initialization_does_not_swallow_unique_constraint_failures_or_cache_them() {
    let dir = directory();
    let conn = connector(&dir.path().join("invalid.db")).await;
    let pipe = empty_table(&conn, "engine_nodes").await;
    let sp = conn.sql_pipe(&pipe).unwrap();
    let table = conn.pipe_table_name(&sp);
    conn.driver().exec(&format!(
        "INSERT INTO {table} (started, run_id, pipe_ref) VALUES ('2026-01-01', 'duplicate', 'same'), ('2026-01-01', 'duplicate', 'same')"
    )).await.unwrap();
    let error = conn.initialize_engine_metrics().await.unwrap_err();
    assert!(
        error.to_string().contains("Failed to initialize indices"),
        "{error}"
    );
    conn.driver()
        .exec(&format!("DELETE FROM {table}"))
        .await
        .unwrap();
    conn.initialize_engine_metrics().await.unwrap();
    let error = conn.driver().exec(&format!(
        "INSERT INTO {table} (started, run_id, pipe_ref) VALUES ('2026-01-01', 'duplicate', 'same'), ('2026-01-01', 'duplicate', 'same')"
    )).await.unwrap_err();
    assert!(
        error.to_string().contains("UNIQUE constraint failed"),
        "{error}"
    );
}

#[tokio::test]
async fn initialization_adds_declared_node_columns_and_preserves_legacy_history() {
    let dir = directory();
    let conn = connector(&dir.path().join("legacy_nodes.db")).await;
    let pipe = empty_table(&conn, "engine_nodes").await;
    let table = conn.pipe_table_name(&conn.sql_pipe(&pipe).unwrap());
    // These fields were introduced after existing installations had already stored nodes.
    for column in ["actual_started", "registration_id", "registration_ref"] {
        conn.driver()
            .exec(&format!("ALTER TABLE {table} DROP COLUMN \"{column}\""))
            .await
            .unwrap();
    }
    conn.driver().exec(&format!(
        "INSERT INTO {table} (started, run_id, pipe_ref, status) VALUES ('2026-01-01 00:00:00.125000', 'legacy-run', 'sql:metrics_init|fixture|legacy|None', 'succeeded')"
    )).await.unwrap();

    conn.initialize_engine_metrics().await.unwrap();
    let columns = conn.get_pipe_columns_types(&pipe).await.unwrap();
    for column in ["actual_started", "registration_id", "registration_ref"] {
        assert!(
            columns.contains_key(column),
            "{column} absent from {columns:?}"
        );
    }
    let frame = conn
        .get_pipe_data(&pipe, &Default::default())
        .await
        .unwrap()
        .unwrap();
    let rows = meerschaum_connector::pipe_ops::frame_to_docs(&frame);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["run_id"].py_str(), "legacy-run");
    assert_eq!(rows[0]["status"].py_str(), "succeeded");
    // Unknown legacy registration identity remains unknown; initialization must not grant
    // old private history to a newly registered Pipe with the same keys.
    assert!(rows[0]["registration_id"].is_na());
    assert!(rows[0]["registration_ref"].is_na());
    conn.initialize_engine_metrics().await.unwrap();
}

#[tokio::test]
async fn initialization_adds_optional_run_selection_without_fabricating_snapshots() {
    let dir = directory();
    let conn = connector(&dir.path().join("legacy_runs.db")).await;
    let pipe = empty_table(&conn, "engine_runs").await;
    let table = conn.pipe_table_name(&conn.sql_pipe(&pipe).unwrap());
    conn.driver()
        .exec(&format!("ALTER TABLE {table} DROP COLUMN selection"))
        .await
        .unwrap();
    conn.driver().exec(&format!(
        "INSERT INTO {table} (started, run_id) VALUES ('2026-01-01 00:00:00.125000', 'legacy-run')"
    )).await.unwrap();
    conn.initialize_engine_metrics().await.unwrap();
    let columns = conn.get_pipe_columns_types(&pipe).await.unwrap();
    assert!(columns.contains_key("selection"), "{columns:?}");
    let frame = conn
        .get_pipe_data(&pipe, &Default::default())
        .await
        .unwrap()
        .unwrap();
    let rows = meerschaum_connector::pipe_ops::frame_to_docs(&frame);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["run_id"].py_str(), "legacy-run");
    assert!(rows[0]["selection"].is_na());
}

#[tokio::test]
async fn readiness_adds_private_status_stamp_without_inventing_legacy_identity() {
    use meerschaum_connector::SyncOptions;
    use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput, pipe_enforce_dtypes};
    let dir = directory();
    let conn = connector(&dir.path().join("legacy_status.db")).await;
    let pipe = empty_table(&conn, "engine_sync_status").await;
    let table = conn.pipe_table_name(&conn.sql_pipe(&pipe).unwrap());
    conn.driver()
        .exec(&format!("ALTER TABLE {table} DROP COLUMN registration_id"))
        .await
        .unwrap();
    conn.driver().exec(&format!(
        "INSERT INTO {table} (pipe_ref, message) VALUES ('sql:metrics_init|fixture|legacy|None', 'legacy private failure')"
    )).await.unwrap();
    let dtypes: indexmap::IndexMap<String, String> = pipe.parameters()["dtypes"]
        .as_map()
        .unwrap()
        .iter()
        .map(|(name, dtype)| (name.clone(), dtype.py_str()))
        .collect();
    let frame = pipe_enforce_dtypes(
        PipeInput::Docs(Vec::new()),
        &PipeDtypesContext {
            explicit_dtypes: dtypes.clone(),
            dtypes,
            enforce: true,
            tz_aware: true,
            mixed_numerics: true,
        },
    )
    .unwrap();
    let result = InstanceConnector::sync_pipe(&conn, &pipe, frame, &SyncOptions::default()).await;
    assert!(result.success, "{}", result.message);
    let frame = conn
        .get_pipe_data(&pipe, &Default::default())
        .await
        .unwrap()
        .unwrap();
    let rows = meerschaum_connector::pipe_ops::frame_to_docs(&frame);
    assert_eq!(rows.len(), 1);
    assert!(rows[0]["registration_id"].is_na());
    assert_eq!(rows[0]["message"].py_str(), "legacy private failure");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_processes_retain_every_cold_first_metrics_batch() {
    let dir = directory();
    let path = dir.path().join("processes.db");
    let conn = connector(&path).await;
    assert!(conn.register_pipe(&source_pipe()).await.success);
    let mut children = Vec::new();
    for writer in 0..4 {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "metrics_writer_child",
                "--ignored",
                "--nocapture",
            ])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .env("MRSM_METRICS_TEST_DB", &path)
            .env("MRSM_METRICS_TEST_GATE", dir.path())
            .env("MRSM_METRICS_TEST_WRITER", writer.to_string())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        children.push(child);
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while (0..4).any(|writer| !dir.path().join(format!("ready_{writer}")).exists()) {
        assert!(
            Instant::now() < deadline,
            "writers did not reach the start gate"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    std::fs::write(dir.path().join("go"), []).unwrap();
    for child in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "child failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let result = conn
        .get_engine_metrics(&MetricsQuery::default())
        .await
        .unwrap();
    assert_eq!(result.summary.syncs, 4);
    assert_eq!(result.summary.rows, 4);
    let ids: std::collections::BTreeSet<_> =
        result.history.iter().map(|row| &row.event_id).collect();
    assert_eq!(ids.len(), 4);
}

#[tokio::test]
#[ignore = "invoked in independent processes by the parent test"]
async fn metrics_writer_child() {
    let path = std::env::var_os("MRSM_METRICS_TEST_DB").expect("parent supplies an owned database");
    let gate = std::env::var_os("MRSM_METRICS_TEST_GATE").unwrap();
    let writer = std::env::var("MRSM_METRICS_TEST_WRITER").unwrap();
    let gate = Path::new(&gate);
    let conn = connector(Path::new(&path)).await;
    let outcome = metric(&conn).await;
    std::fs::write(gate.join(format!("ready_{writer}")), []).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while !gate.join("go").exists() {
        assert!(
            Instant::now() < deadline,
            "parent did not release the start gate"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::timeout(
        Duration::from_secs(15),
        conn.record_sync_metrics(&[outcome]),
    )
    .await
    .expect("independent writer timed out")
    .unwrap();
}
