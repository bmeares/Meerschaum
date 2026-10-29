//! Metrics use microsecond epochs, including when upgrading an existing rc17 hypertable.
use chrono::{TimeDelta, Utc};
use meerschaum_connector::pipe_ops::engine_state_parameters;
use meerschaum_connector::sync_metrics::SyncMetric;
use meerschaum_connector::sync_status::{SyncOutcome, SyncSource};
use meerschaum_connector::{InstanceConnector, SyncOptions};
use meerschaum_connector_sql::pipe::{ChunkInterval, SqlPipe};
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_connector_sql::{Flavor, SqlConnector};
use meerschaum_core::{Pipe, Value};
use meerschaum_kernel::enforce::PipeInput;

const MONTH_US: i64 = 30 * 24 * 60 * 60 * 1_000_000;

#[test]
fn metrics_partition_interval_is_thirty_days_in_microseconds() {
    let parameters = engine_state_parameters("engine_metrics").unwrap();
    let pipe = Pipe::builder("mrsm", "engine_metrics")
        .parameters(parameters)
        .build()
        .unwrap();
    let sp = SqlPipe::new(pipe, Flavor::Timescaledb).unwrap();
    assert_eq!(sp.chunk_interval().unwrap(), ChunkInterval::Int(MONTH_US));
}

async fn connectors() -> Option<(SqlConnector, SqlConnector)> {
    let Ok(uri) = std::env::var("MRSM_TEST_TIMESCALE_URI") else {
        eprintln!("Skipping Timescale metrics test: MRSM_TEST_TIMESCALE_URI is unset.");
        return None;
    };
    let mut spec = ConnectionSpec::from_uri(&uri, Some("metrics_partition"), None).unwrap();
    assert!(spec.flavor.is_timescaledb());
    let bootstrap = SqlConnector::connect("metrics_partition", spec.clone())
        .await
        .unwrap();
    let schema = format!("mp_{}", &uuid::Uuid::new_v4().simple().to_string()[..10]);
    bootstrap
        .driver()
        .exec(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();
    spec.schema = Some(schema);
    let first = SqlConnector::connect("metrics_partition", spec.clone())
        .await
        .unwrap();
    let second = SqlConnector::connect("metrics_partition", spec)
        .await
        .unwrap();
    Some((first, second))
}

async fn dimension(conn: &SqlConnector, table: &str, field: &str) -> i64 {
    conn.value(&format!(
        "SELECT {field} FROM timescaledb_information.{} \
         WHERE hypertable_schema = '_mrsm_internal' AND hypertable_name = '{table}'",
        if field == "num_chunks" {
            "hypertables"
        } else {
            "dimensions"
        },
    ))
    .await
    .and_then(|value| value.as_i64())
    .unwrap()
}

async fn cleanup(conn: &SqlConnector) {
    for name in ["engine_metrics", "engine_runs", "engine_nodes"] {
        let pipe = conn
            .engine_state_pipe("sql:metrics_partition", name)
            .unwrap();
        let sp = conn.sql_pipe(&pipe).unwrap();
        conn.driver()
            .exec(&format!(
                "DROP TABLE IF EXISTS {} CASCADE",
                conn.pipe_table_name(&sp)
            ))
            .await
            .unwrap();
    }
    conn.driver()
        .exec(&format!(
            "DROP SCHEMA \"{}\" CASCADE",
            conn.schema().unwrap()
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn timescale_metrics_writes_share_a_month_chunk() {
    let Some((conn, _)) = connectors().await else {
        return;
    };
    conn.initialize_engine_metrics().await.unwrap();
    let source = Pipe::builder("test", "metrics_partition")
        .instance("sql:metrics_partition")
        .build()
        .unwrap();
    assert!(conn.register_pipe(&source).await.success);
    let base = chrono::DateTime::from_timestamp_micros(
        Utc::now().timestamp_micros() / MONTH_US * MONTH_US + 60_000_000,
    )
    .unwrap();
    let mut outcomes = Vec::new();
    for seconds in [0, 60, 3600] {
        let mut metric = SyncMetric::from_outcome(
            &source,
            &SyncOutcome::new(true, "synthetic", SyncSource::Python, None),
        );
        metric.ts = base + TimeDelta::seconds(seconds);
        metric.started = metric.ts;
        outcomes.push(metric);
    }
    conn.record_sync_metrics(&outcomes).await.unwrap();
    let pipe = conn
        .engine_state_pipe("sql:metrics_partition", "engine_metrics")
        .unwrap();
    let sp = conn.sql_pipe(&pipe).unwrap();
    assert_eq!(
        dimension(&conn, &sp.target, "integer_interval").await,
        MONTH_US
    );
    assert_eq!(dimension(&conn, &sp.target, "num_chunks").await, 1);
    assert_eq!(
        conn.value(&format!(
            "SELECT COUNT(*) FROM {}",
            conn.pipe_table_name(&sp)
        ))
        .await
        .unwrap()
        .as_i64(),
        Some(3)
    );
    cleanup(&conn).await;
}

#[tokio::test]
async fn timescale_cold_readiness_repairs_legacy_metrics_interval() {
    let Some((first, fresh)) = connectors().await else {
        return;
    };
    let mut legacy = first
        .engine_state_pipe("sql:metrics_partition", "engine_metrics")
        .unwrap();
    legacy.update_parameters(
        [
            ("precision".into(), Value::Null),
            (
                "verify".into(),
                Value::from_json_str(r#"{"chunk_minutes":43200}"#).unwrap(),
            ),
        ]
        .into(),
    );
    assert!(
        first
            .sync(
                &legacy,
                PipeInput::Docs(Vec::new()),
                &SyncOptions::default()
            )
            .await
            .success
    );
    let sp = first.sql_pipe(&legacy).unwrap();
    assert_eq!(
        dimension(&first, &sp.target, "integer_interval").await,
        43200
    );
    first
        .driver()
        .exec(&format!(
            "INSERT INTO {} (event_id,ts_us) VALUES ('legacy-a',1000000),('legacy-b',61000000)",
            first.pipe_table_name(&sp),
        ))
        .await
        .unwrap();
    assert_eq!(dimension(&first, &sp.target, "num_chunks").await, 2);
    fresh.initialize_engine_metrics().await.unwrap();
    assert_eq!(
        dimension(&fresh, &sp.target, "integer_interval").await,
        MONTH_US
    );
    assert_eq!(
        fresh
            .value(&format!(
                "SELECT COUNT(*) FROM {}",
                fresh.pipe_table_name(&sp)
            ))
            .await
            .unwrap()
            .as_i64(),
        Some(2)
    );
    cleanup(&fresh).await;
}
