//! Independent pools must not upsert engine nodes before their unique key is ready.
use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::{SyncOptions, pipe_ops::engine_state_parameters};
use meerschaum_core::Pipe;
use meerschaum_kernel::{
    Frame,
    value::{DateTimeValue, Value as KValue},
};
use meerschaum_sql::SqlConnector;
use meerschaum_sql::queries::table::{CreateTableOptions, QueryOrDtypes, get_create_table_queries};
use meerschaum_sql::uri::ConnectionSpec;

async fn connector(uri: &str, label: &str) -> SqlConnector {
    let mut spec = ConnectionSpec::from_uri(uri, Some(label), None).unwrap();
    spec.pool_size = 1;
    spec.max_overflow = 0;
    SqlConnector::connect(label, spec).await.unwrap()
}

fn nodes(schema: &str, target: &str) -> Pipe {
    let mut parameters = engine_state_parameters("engine_nodes").unwrap();
    parameters.insert("schema".into(), schema.into());
    parameters.insert("target".into(), target.into());
    Pipe::builder("mrsm", "engine_nodes")
        .instance("sql:w10_init")
        .parameters(parameters)
        .kwarg("temporary", true)
        .kwarg("static", true)
        .build()
        .unwrap()
}

fn row(writer: usize) -> Frame {
    Frame::from_object_columns(vec![
        ("run_id".into(), vec![KValue::Str("synthetic-run".into())]),
        (
            "pipe_ref".into(),
            vec![KValue::Str(format!("synthetic-{writer}"))],
        ),
        (
            "started".into(),
            vec![KValue::Datetime(DateTimeValue::utc(
                chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ))],
        ),
        ("message".into(), vec![KValue::Str("initialized".into())]),
    ])
    .unwrap()
}

async fn scenario(env: &str) {
    let Ok(uri) = std::env::var(env) else {
        eprintln!("Skipping: set {env} for disposable PostgreSQL.");
        return;
    };
    let admin = connector(&uri, "w10_admin").await;
    let schema = format!(
        "w10_init_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let mut writers = Vec::new();
    for n in 0..6 {
        writers.push(Arc::new(connector(&uri, &format!("w10_writer_{n}")).await));
    }
    // Different targets may initialize the same absent schema concurrently. Their
    // table locks intentionally differ, so schema creation needs its own protection.
    for round in 0..4 {
        let fresh_schema = format!("{schema}_fresh_{round}");
        let barrier = Arc::new(tokio::sync::Barrier::new(writers.len()));
        let mut tasks = Vec::new();
        for (n, conn) in writers.iter().enumerate() {
            let conn = conn.clone();
            let pipe = nodes(&fresh_schema, &format!("nodes_{n}"));
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                conn.sync_pipe(&pipe, row(n), &SyncOptions::default()).await
            }));
        }
        let mut failures = Vec::new();
        for task in tasks {
            let result = tokio::time::timeout(Duration::from_secs(45), task)
                .await
                .unwrap()
                .unwrap();
            if !result.success {
                failures.push(result.message);
            }
        }
        admin
            .driver()
            .exec(&format!("DROP SCHEMA IF EXISTS {fresh_schema} CASCADE"))
            .await
            .unwrap();
        assert!(
            failures.is_empty(),
            "fresh schema initialization failed: {failures:?}"
        );
    }
    admin
        .driver()
        .exec(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    // Fresh tables exercise simultaneous CREATE / catalog inspection. The partial table
    // deterministically reproduces the observed table-visible/index-not-ready window.
    for round in 0..4 {
        let pipe = nodes(&schema, &format!("nodes_{round}"));
        let sp = admin.sql_pipe(&pipe).unwrap();
        let table = admin.pipe_table_name(&sp);
        if round == 3 {
            let dtypes = engine_state_parameters("engine_nodes").unwrap()["dtypes"]
                .as_map()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.py_str()))
                .collect();
            let queries = get_create_table_queries(
                &QueryOrDtypes::Dtypes(dtypes),
                &sp.target,
                admin.flavor(),
                &CreateTableOptions {
                    schema: Some(schema.clone()),
                    ..Default::default()
                },
            )
            .unwrap();
            assert!(admin.exec_queries_all(&queries, true, true).await);
            let error = admin.driver().exec(&format!(
                "INSERT INTO {table} (run_id, pipe_ref, started) VALUES ('probe', 'probe', '2026-01-01') ON CONFLICT (started, run_id, pipe_ref) DO NOTHING"
            )).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("no unique or exclusion constraint"),
                "{error}"
            );
            admin.driver().exec(&format!(
                "INSERT INTO {table} (run_id, pipe_ref, started) VALUES ('duplicate', 'duplicate', '2026-01-01'), ('duplicate', 'duplicate', '2026-01-01')"
            )).await.unwrap();
            let failed = writers[0]
                .sync_pipe(&pipe, row(0), &SyncOptions::default())
                .await;
            assert!(!failed.success);
            assert!(
                failed.message.contains("Failed to initialize indices"),
                "{}",
                failed.message
            );
            admin
                .driver()
                .exec(&format!("DELETE FROM {table}"))
                .await
                .unwrap();
            // The failed initializer must not cache this incomplete table as ready;
            // its retry below participates in repairing the missing unique key.
        }
        let barrier = Arc::new(tokio::sync::Barrier::new(writers.len()));
        let tasks: Vec<_> = writers
            .iter()
            .enumerate()
            .map(|(n, conn)| {
                let conn = conn.clone();
                let pipe = pipe.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    let opts = SyncOptions {
                        check_existing: false,
                        ..Default::default()
                    };
                    let result = conn.sync_pipe(&pipe, row(n), &opts).await;
                    assert!(result.success, "cold writer {n}: {}", result.message);
                    conn.enable_query_log();
                    let result = conn.sync_pipe(&pipe, row(n), &opts).await;
                    assert!(result.success, "warm writer {n}: {}", result.message);
                    let log = conn.take_query_log();
                    // Default engine nodes retain native partition checks. Apart from
                    // those existing checks, warm writes require only their upsert.
                    let writes: Vec<_> = log
                        .iter()
                        .filter(|query| {
                            !query.contains("pg_partitioned_table")
                                && !query.contains("PARTITION OF")
                        })
                        .collect();
                    assert_eq!(writes.len(), 1, "warm writes use one DML: {log:?}");
                    assert!(writes[0].starts_with("INSERT INTO"), "{log:?}");
                })
            })
            .collect();
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(45), task)
                .await
                .expect("initialization must not deadlock a one-connection pool")
                .unwrap();
        }
        assert_eq!(
            admin
                .value(&format!("SELECT COUNT(*) FROM {table}"))
                .await
                .unwrap()
                .as_i64(),
            Some(6)
        );
    }
    admin
        .driver()
        .exec(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_concurrent_engine_nodes_initialization() {
    scenario("MRSM_TEST_PG_URI").await;
}

#[tokio::test]
async fn postgis_concurrent_engine_nodes_initialization() {
    scenario("MRSM_TEST_POSTGIS_URI").await;
}
