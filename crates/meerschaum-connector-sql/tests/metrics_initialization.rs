//! Cold metrics readiness must tolerate independent writers without hiding real DDL errors.

use std::path::Path;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_connector::sync_metrics::{MetricsQuery, SyncMetric};
use meerschaum_connector::sync_status::{SyncOutcome, SyncSource};
use meerschaum_connector::{InstanceConnector, SyncOptions};
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

async fn network_connector(flavor: &str) -> Option<SqlConnector> {
    let variable = match flavor {
        "mysql" => "MRSM_TEST_MYSQL_URI",
        "mariadb" => "MRSM_TEST_MARIADB_URI",
        "mssql" => "MRSM_TEST_MSSQL_URI",
        "postgresql" => "MRSM_TEST_PG_URI",
        _ => panic!("unexpected owned backend"),
    };
    let Ok(uri) = std::env::var(variable) else {
        eprintln!("Skipping {flavor}: {variable} is unset.");
        return None;
    };
    assert_eq!(std::env::var("MRSM_TEST_ISOLATED").as_deref(), Ok("1"));
    let mut spec = ConnectionSpec::from_uri(&uri, Some("metrics_init"), None).unwrap();
    assert_eq!(spec.flavor.as_str(), flavor);
    spec.pool_size = 1;
    spec.max_overflow = 0;
    Some(SqlConnector::connect("metrics_init", spec).await.unwrap())
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
    let network = std::env::var("MRSM_METRICS_TEST_NETWORK").ok();
    let gate = std::env::var_os("MRSM_METRICS_TEST_GATE").unwrap();
    let writer = std::env::var("MRSM_METRICS_TEST_WRITER").unwrap();
    let gate = Path::new(&gate);
    let conn = match &network {
        Some(flavor) => network_connector(flavor)
            .await
            .expect("parent supplies an owned URI"),
        None => {
            let path = std::env::var_os("MRSM_METRICS_TEST_DB")
                .expect("parent supplies an owned database");
            connector(Path::new(&path)).await
        }
    };
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
    tokio::time::timeout(Duration::from_secs(30), async {
        if network.is_some() {
            conn.initialize_engine_metrics().await?;
        }
        conn.record_sync_metrics(&[outcome]).await
    })
    .await
    .expect("independent writer timed out")
    .unwrap();
}

struct NetworkWriters(Vec<Child>);
impl Drop for NetworkWriters {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn network_cold_readiness(flavor: &str) {
    let Some(conn) = network_connector(flavor).await else {
        return;
    };
    assert!(conn.register_pipe(&source_pipe()).await.success);
    let dir = directory();
    let mut writers = NetworkWriters(Vec::new());
    for writer in 0..4 {
        let root = dir.path().join(format!("network_root_{writer}"));
        let plugins = root.join("plugins");
        std::fs::create_dir_all(&plugins).unwrap();
        writers.0.push(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "metrics_writer_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env_remove("TMUX")
                .env_remove("TMUX_PANE")
                .env("MRSM_ROOT_DIR", root)
                .env("MRSM_PLUGINS_DIR", plugins)
                .env("MRSM_METRICS_TEST_NETWORK", flavor)
                .env("MRSM_METRICS_TEST_GATE", dir.path())
                .env("MRSM_METRICS_TEST_WRITER", writer.to_string())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        );
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while (0..4).any(|writer| !dir.path().join(format!("ready_{writer}")).exists()) {
        assert!(
            Instant::now() < deadline,
            "network writers did not reach the gate"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    std::fs::write(dir.path().join("go"), []).unwrap();
    while let Some(child) = writers.0.pop() {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{flavor}:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let result = conn
        .get_engine_metrics(&MetricsQuery::default())
        .await
        .unwrap();
    assert_eq!(result.summary.syncs, 4, "{flavor}");
    assert_eq!(result.summary.rows, 4, "{flavor}");
    assert_eq!(
        result
            .history
            .iter()
            .map(|row| &row.event_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        4
    );

    // A failed unique key on real legacy data must remain fatal, never become
    // cached readiness; correcting that data allows the same connector to retry.
    let pipe = conn.engine_state_pipe(INSTANCE, "engine_nodes").unwrap();
    let sp = conn.sql_pipe(&pipe).unwrap();
    let table = conn.pipe_table_name(&sp);
    conn.driver()
        .exec(&format!("DROP TABLE {table}"))
        .await
        .unwrap();
    let physical = pipe.parameters()["dtypes"]
        .as_map()
        .unwrap()
        .iter()
        .map(|(name, dtype)| {
            let db_type = if matches!(name.as_str(), "run_id" | "pipe_ref") {
                match flavor {
                    "mysql" | "mariadb" => "VARCHAR(255)".into(),
                    "mssql" => "NVARCHAR(255)".into(),
                    _ => "TEXT".into(),
                }
            } else {
                meerschaum_connector_sql::dtypes::get_db_type_from_pd_type(
                    &dtype.py_str(),
                    Some(conn.flavor()),
                )
            };
            (name.clone(), db_type)
        })
        .collect();
    let queries = get_create_table_queries(
        &QueryOrDtypes::Dtypes(physical),
        &sp.target,
        conn.flavor(),
        &CreateTableOptions {
            parse_dtypes: false,
            schema: conn.get_pipe_schema(&sp),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(conn.exec_queries_all(&queries, true, true).await);
    let duplicate = format!(
        "INSERT INTO {table} (started, run_id, pipe_ref) VALUES ('2026-01-01 00:00:00', 'same', 'same')"
    );
    for _ in 0..2 {
        conn.driver().exec(&duplicate).await.unwrap();
    }
    let fresh = network_connector(flavor).await.unwrap();
    let error = fresh.initialize_engine_metrics().await.unwrap_err();
    assert!(
        error.to_string().contains("Failed to initialize indices"),
        "{flavor}: {error}"
    );
    conn.driver()
        .exec(&format!("DELETE FROM {table}"))
        .await
        .unwrap();
    fresh.initialize_engine_metrics().await.unwrap();
    conn.driver().exec(&duplicate).await.unwrap();
    assert!(
        conn.driver().exec(&duplicate).await.is_err(),
        "{flavor}: unique key must be enforced"
    );
    assert!(conn.delete_pipe(&source_pipe()).await.success);
    for metric in ["engine_nodes", "engine_runs", "engine_metrics"] {
        let pipe = conn.engine_state_pipe(INSTANCE, metric).unwrap();
        let table = conn.pipe_table_name(&conn.sql_pipe(&pipe).unwrap());
        conn.driver()
            .exec(&format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_cold_processes_preserve_batches_and_real_unique_errors() {
    network_cold_readiness("mysql").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mariadb_cold_processes_preserve_batches_and_real_unique_errors() {
    network_cold_readiness("mariadb").await;
}

#[cfg(feature = "mssql")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mssql_cold_processes_preserve_batches_and_real_unique_errors() {
    network_cold_readiness("mssql").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_cold_processes_preserve_batches_and_real_unique_errors() {
    network_cold_readiness("postgresql").await;
}

#[cfg(feature = "mssql")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mssql_different_cold_targets_share_schema_readiness_and_preserve_permission_errors() {
    use meerschaum_core::Value;
    use meerschaum_kernel::value::{DateTimeValue, Value as Cell};

    let Some(admin) = network_connector("mssql").await else {
        return;
    };
    let node = |schema: &str, target: &str| {
        let mut parameters =
            meerschaum_connector::pipe_ops::engine_state_parameters("engine_nodes").unwrap();
        parameters.insert("schema".into(), Value::Str(schema.into()));
        parameters.insert("target".into(), Value::Str(target.into()));
        Pipe::builder("mrsm", "engine_nodes")
            .instance(INSTANCE)
            .parameters(parameters)
            .kwarg("temporary", true)
            .kwarg("static", true)
            .build()
            .unwrap()
    };
    let row = |writer: usize| {
        meerschaum_kernel::Frame::from_object_columns(vec![
            (
                "started".into(),
                vec![Cell::Datetime(DateTimeValue::utc(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                ))],
            ),
            ("run_id".into(), vec![Cell::Str("owned-run".into())]),
            (
                "pipe_ref".into(),
                vec![Cell::Str(format!("owned-{writer}"))],
            ),
        ])
        .unwrap()
    };
    for _ in 0..3 {
        let schema = format!("cold_{}", &uuid::Uuid::new_v4().simple().to_string()[..10]);
        assert!(
            admin
                .value(&format!("SELECT SCHEMA_ID(N'{schema}')"))
                .await
                .is_none_or(|v| matches!(v, meerschaum_connector_sql::driver::SqlValue::Null))
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut tasks = Vec::new();
        for writer in 0..2 {
            let conn = network_connector("mssql").await.unwrap();
            let pipe = node(&schema, &format!("node_{writer}"));
            let frame = row(writer);
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let result =
                    InstanceConnector::sync_pipe(&conn, &pipe, frame, &SyncOptions::default())
                        .await;
                assert!(result.success, "{}", result.message);
                assert_eq!(
                    conn.get_pipe_data(&pipe, &Default::default())
                        .await
                        .unwrap()
                        .unwrap()
                        .height(),
                    1
                );
            }));
        }
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(30), task)
                .await
                .unwrap()
                .unwrap();
        }
        for writer in 0..2 {
            admin
                .driver()
                .exec(&format!("DROP TABLE [{schema}].[node_{writer}]"))
                .await
                .unwrap();
        }
        admin
            .driver()
            .exec(&format!("DROP SCHEMA [{schema}]"))
            .await
            .unwrap();
    }

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let login = format!("owned_{}", &suffix[..10]);
    let password = format!("FixtureA1{}", &suffix[10..]);
    admin.driver().exec(&format!("CREATE LOGIN [{login}] WITH PASSWORD = '{password}', CHECK_POLICY = OFF; CREATE USER [{login}] FOR LOGIN [{login}]; GRANT CREATE TABLE TO [{login}]")).await.unwrap();
    let uri = std::env::var("MRSM_TEST_MSSQL_URI").unwrap();
    let mut spec = ConnectionSpec::from_uri(&uri, Some("metrics_limited"), None).unwrap();
    spec.username = Some(login.clone());
    spec.password = Some(password);
    spec.uri = None;
    spec.pool_size = 1;
    spec.max_overflow = 0;
    let limited = SqlConnector::connect("metrics_limited", spec)
        .await
        .unwrap();
    assert!(
        matches!(limited.value("SELECT SUSER_SNAME()").await, Some(meerschaum_connector_sql::driver::SqlValue::Text(value)) if value == login)
    );
    assert_eq!(
        limited
            .value("SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE SCHEMA')")
            .await
            .unwrap()
            .as_i64(),
        Some(0)
    );
    let schema = format!("denied_{}", &suffix[..10]);
    let pipe = node(&schema, "must_not_exist");
    let result =
        InstanceConnector::sync_pipe(&limited, &pipe, row(0), &SyncOptions::default()).await;
    assert!(
        !result.success,
        "permission denial must not be accepted: {}",
        result.message
    );
    assert!(!admin.pipe_exists(&pipe).await.unwrap());
    assert_eq!(
        limited.value("SELECT @@TRANCOUNT").await.unwrap().as_i64(),
        Some(0)
    );
    // The failed one-slot session remains open: a privileged caller must acquire
    // the same schema/target and write without relying on close() to release a lock.
    let recovered = tokio::time::timeout(
        Duration::from_secs(10),
        InstanceConnector::sync_pipe(&admin, &pipe, row(1), &SyncOptions::default()),
    )
    .await
    .expect("failed schema DDL retained its transaction lock");
    assert!(recovered.success, "{}", recovered.message);
    assert_eq!(
        admin
            .get_pipe_data(&pipe, &Default::default())
            .await
            .unwrap()
            .unwrap()
            .height(),
        1
    );
    admin
        .driver()
        .exec(&format!(
            "DROP TABLE [{schema}].[must_not_exist]; DROP SCHEMA [{schema}]"
        ))
        .await
        .unwrap();
    limited.driver().close().await;
    admin
        .driver()
        .exec(&format!("DROP USER [{login}]; DROP LOGIN [{login}]"))
        .await
        .unwrap();
    assert!(
        admin
            .value(&format!("SELECT SCHEMA_ID(N'{schema}')"))
            .await
            .is_none_or(|v| matches!(v, meerschaum_connector_sql::driver::SqlValue::Null))
    );
}
