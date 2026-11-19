//! Internal engine hypertables use 7-day chunks and a 7-day compression window,
//! migrating legacy rc17 (43200 µs) and 5.0 (30-day) defaults without touching
//! custom admin intervals or retained history.
use chrono::{TimeDelta, Utc};
use meerschaum_connector::pipe_ops::engine_state_parameters;
use meerschaum_connector::sync_metrics::SyncMetric;
use meerschaum_connector::sync_status::{SyncOutcome, SyncSource};
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector, SyncOptions};
use meerschaum_connector_sql::pipe::{ChunkInterval, SqlPipe};
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_connector_sql::{Flavor, SqlConnector};
use meerschaum_core::{Pipe, Value};
use meerschaum_kernel::enforce::PipeInput;

const WEEK_DAYS: i64 = 7;
const WEEK_SECS: i64 = WEEK_DAYS * 24 * 60 * 60;
const WEEK_US: i64 = WEEK_SECS * 1_000_000;
const LEGACY_MONTH_US: i64 = 30 * 24 * 60 * 60 * 1_000_000;

#[test]
fn metrics_partition_interval_is_seven_days_in_microseconds() {
    let parameters = engine_state_parameters("engine_metrics").unwrap();
    let pipe = Pipe::builder("mrsm", "engine_metrics")
        .parameters(parameters)
        .build()
        .unwrap();
    let sp = SqlPipe::new(pipe, Flavor::Timescaledb).unwrap();
    assert_eq!(sp.chunk_interval().unwrap(), ChunkInterval::Int(WEEK_US));
    assert_eq!(
        sp.pipe()
            .param("compress")
            .and_then(|v| v.get("after"))
            .and_then(Value::as_str),
        Some("7 days"),
    );
}

#[test]
fn engine_datetime_pipes_use_seven_day_chunks_and_compression() {
    for metric in [
        "engine_ledger",
        "engine_runs",
        "engine_nodes",
        "engine_events",
    ] {
        let parameters = engine_state_parameters(metric).unwrap();
        let pipe = Pipe::builder("mrsm", metric)
            .parameters(parameters)
            .build()
            .unwrap();
        let sp = SqlPipe::new(pipe, Flavor::Timescaledb).unwrap();
        assert_eq!(
            sp.chunk_interval().unwrap(),
            ChunkInterval::minutes(WEEK_DAYS * 1440),
            "{metric} chunk interval"
        );
        assert_eq!(
            sp.pipe()
                .param("compress")
                .and_then(|v| v.get("after"))
                .and_then(Value::as_str),
            Some("7 days"),
            "{metric} compress.after"
        );
    }
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
    for name in [
        "engine_metrics",
        "engine_runs",
        "engine_nodes",
        "engine_events",
        "engine_ledger",
    ] {
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

async fn compression_after(conn: &SqlConnector, table: &str) -> i64 {
    conn.value(&format!(
        "SELECT (config->>'compress_after')::bigint FROM timescaledb_information.jobs \
         WHERE hypertable_schema='_mrsm_internal' AND hypertable_name='{table}' \
         AND proc_name='policy_compression'"
    ))
    .await
    .and_then(|value| value.as_i64())
    .unwrap()
}

async fn compression_after_seconds(conn: &SqlConnector, table: &str) -> i64 {
    conn.value(&format!(
        "SELECT EXTRACT(EPOCH FROM (config->>'compress_after')::interval)::bigint \
         FROM timescaledb_information.jobs \
         WHERE hypertable_schema='_mrsm_internal' AND hypertable_name='{table}' \
         AND proc_name='policy_compression'"
    ))
    .await
    .and_then(|value| value.as_i64())
    .unwrap()
}

#[tokio::test]
async fn timescale_metrics_writes_share_a_week_chunk() {
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
        Utc::now().timestamp_micros() / WEEK_US * WEEK_US + 60_000_000,
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
        WEEK_US
    );
    assert_eq!(dimension(&conn, &sp.target, "num_chunks").await, 1);
    assert_eq!(compression_after(&conn, &sp.target).await, WEEK_US);
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
            ("compress".into(), Value::Null),
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
    assert_eq!(compression_after(&first, &sp.target).await, 43200);
    fresh.initialize_engine_metrics().await.unwrap();
    assert_eq!(compression_after(&fresh, &sp.target).await, WEEK_US);
    assert_eq!(
        dimension(&fresh, &sp.target, "integer_interval").await,
        WEEK_US
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

#[tokio::test]
async fn timescale_migrates_legacy_30_day_engine_hypertables_and_preserves_custom_intervals() {
    let Some((first, fresh)) = connectors().await else {
        return;
    };
    // Simulate a 5.0 installation with 30-day chunks & 30-day compress_after on both
    // integer-axis (`engine_metrics`) and datetime-axis (`engine_events`) hypertables,
    // plus a custom 14-day admin override on `engine_runs`.
    let mut legacy_metrics = first
        .engine_state_pipe("sql:metrics_partition", "engine_metrics")
        .unwrap();
    legacy_metrics.update_parameters(
        [
            ("compress".into(), Value::Null),
            (
                "verify".into(),
                Value::from_json_str(r#"{"chunk_days":30}"#).unwrap(),
            ),
        ]
        .into(),
    );
    assert!(
        first
            .sync(
                &legacy_metrics,
                PipeInput::Docs(Vec::new()),
                &SyncOptions::default()
            )
            .await
            .success
    );
    let metrics_sp = first.sql_pipe(&legacy_metrics).unwrap();
    assert_eq!(
        dimension(&first, &metrics_sp.target, "integer_interval").await,
        LEGACY_MONTH_US
    );
    assert_eq!(
        compression_after(&first, &metrics_sp.target).await,
        LEGACY_MONTH_US
    );

    let mut legacy_events = first
        .engine_state_pipe("sql:metrics_partition", "engine_events")
        .unwrap();
    legacy_events.update_parameters(
        [
            ("compress".into(), Value::Null),
            (
                "verify".into(),
                Value::from_json_str(r#"{"chunk_days":30}"#).unwrap(),
            ),
        ]
        .into(),
    );
    assert!(
        first
            .sync(
                &legacy_events,
                PipeInput::Docs(Vec::new()),
                &SyncOptions::default()
            )
            .await
            .success
    );
    let events_sp = first.sql_pipe(&legacy_events).unwrap();
    assert_eq!(
        dimension(
            &first,
            &events_sp.target,
            "EXTRACT(EPOCH FROM time_interval)::bigint"
        )
        .await,
        30 * 86_400
    );
    assert_eq!(
        compression_after_seconds(&first, &events_sp.target).await,
        30 * 86_400
    );

    let mut custom_runs = first
        .engine_state_pipe("sql:metrics_partition", "engine_runs")
        .unwrap();
    custom_runs.update_parameters(
        [
            ("compress".into(), Value::Null),
            (
                "verify".into(),
                Value::from_json_str(r#"{"chunk_days":14}"#).unwrap(),
            ),
        ]
        .into(),
    );
    assert!(
        first
            .sync(
                &custom_runs,
                PipeInput::Docs(Vec::new()),
                &SyncOptions::default()
            )
            .await
            .success
    );
    let runs_sp = first.sql_pipe(&custom_runs).unwrap();

    // Fresh connection initializes with 5.1 defaults (7-day chunks & 7-day compress_after).
    fresh.initialize_engine_metrics().await.unwrap();
    let fresh_events = fresh
        .engine_state_pipe("sql:metrics_partition", "engine_events")
        .unwrap();
    assert!(
        fresh
            .sync(
                &fresh_events,
                PipeInput::Docs(Vec::new()),
                &SyncOptions::default()
            )
            .await
            .success
    );

    assert_eq!(
        dimension(&fresh, &metrics_sp.target, "integer_interval").await,
        WEEK_US
    );
    assert_eq!(compression_after(&fresh, &metrics_sp.target).await, WEEK_US);
    assert_eq!(
        dimension(
            &fresh,
            &events_sp.target,
            "EXTRACT(EPOCH FROM time_interval)::bigint"
        )
        .await,
        WEEK_SECS
    );
    assert_eq!(
        compression_after_seconds(&fresh, &events_sp.target).await,
        WEEK_SECS
    );
    // Custom 14-day admin setting must remain untouched.
    assert_eq!(
        dimension(
            &fresh,
            &runs_sp.target,
            "EXTRACT(EPOCH FROM time_interval)::bigint"
        )
        .await,
        14 * 86_400
    );
    assert_eq!(
        compression_after_seconds(&fresh, &runs_sp.target).await,
        14 * 86_400
    );

    // End-only retention clear drops whole expired chunks before DELETE.
    let old_us = 10 * WEEK_US + 1_000_000;
    let mid_us = 12 * WEEK_US + 1_000_000;
    let new_us = 14 * WEEK_US + 1_000_000;
    fresh
        .driver()
        .exec(&format!(
            "INSERT INTO {} (event_id, ts_us) VALUES ('old', {old_us}), ('mid', {mid_us}), ('new', {new_us})",
            fresh.pipe_table_name(&metrics_sp),
        ))
        .await
        .unwrap();
    assert_eq!(dimension(&fresh, &metrics_sp.target, "num_chunks").await, 3);
    let fresh_metrics = fresh
        .engine_state_pipe("sql:metrics_partition", "engine_metrics")
        .unwrap();
    assert!(
        fresh
            .clear_pipe(
                &fresh_metrics,
                &DataQuery {
                    end: Some(DateBound::Int(12 * WEEK_US + 2_000_000)),
                    ..Default::default()
                },
            )
            .await
            .success
    );
    // The fully expired `old` chunk was dropped; only `new` remains (and `mid` was deleted from its boundary chunk).
    assert_eq!(dimension(&fresh, &metrics_sp.target, "num_chunks").await, 2);
    assert_eq!(
        fresh
            .value(&format!(
                "SELECT COUNT(*) FROM {}",
                fresh.pipe_table_name(&metrics_sp)
            ))
            .await
            .unwrap()
            .as_i64(),
        Some(1)
    );
    cleanup(&fresh).await;
}
