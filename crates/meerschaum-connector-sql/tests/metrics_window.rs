//! Whole-window totals and charts do not inherit the recent-history row cap.

mod common;

use chrono::{TimeDelta, Utc};
use meerschaum_connector::sync_metrics::{MetricsQuery, SyncMetric};
use meerschaum_connector::sync_status::{SyncOutcome, SyncSource};
use meerschaum_connector::{Connector, DataQuery, DateBound, InstanceConnector};
use meerschaum_core::Pipe;
use std::time::Duration;

async fn window(flavor: &'static str) {
    if !matches!(flavor, "sqlite" | "duckdb")
        && std::env::var("MRSM_TEST_ISOLATED").as_deref() != Ok("1")
    {
        return;
    }
    let Some(ctx) = common::ctx(flavor, "metric_window").await else {
        return;
    };
    let conn = &ctx.conn;
    let mut pipe = Pipe::builder("window", &ctx.prefix)
        .instance(&conn.connector_keys())
        .build()
        .unwrap();
    assert!(conn.register_pipe(&pipe).await.success);
    assert!(conn.get_pipe_id(&pipe).await.unwrap().is_some());
    let end = chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
    let begin = end - TimeDelta::hours(24);
    let mut rows = Vec::new();
    for index in 0..1205 {
        let mut metric = SyncMetric::from_outcome(
            &pipe,
            &SyncOutcome::new(index % 10 != 0, "", SyncSource::Python, None)
                .with_counts(2, 1, 0)
                .with_duration(Some(Duration::from_millis(250))),
        );
        metric.ts = begin + TimeDelta::microseconds(1 + index * 86_400_000_000 / 1205);
        metric.started = metric.ts - TimeDelta::milliseconds(250);
        rows.push(metric);
    }
    let mut excluded = rows[0].clone();
    excluded.event_id = uuid::Uuid::new_v4().to_string();
    excluded.ts = end;
    rows.push(excluded.clone());
    excluded.event_id = uuid::Uuid::new_v4().to_string();
    excluded.ts = begin + TimeDelta::hours(12);
    excluded.registration_id = "old-private-registration".into();
    rows.push(excluded);
    conn.record_sync_metrics(&rows).await.unwrap();
    let reference = pipe.pipe_ref();
    for limit in [1000, 7] {
        let result = conn
            .get_engine_metrics(&MetricsQuery {
                begin: Some(begin),
                end: Some(end),
                limit: Some(limit),
                buckets: Some(48),
                visible_refs: Some(vec![reference.clone()]),
                visible_pipelines: Some(Vec::new()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.summary.syncs, 1205, "{flavor}: {result:?}");
        assert_eq!(result.summary.failed, 121);
        assert_eq!(result.summary.succeeded, 1084);
        assert_eq!(result.summary.rows, 3615);
        assert_eq!(result.summary.avg_duration_seconds, Some(0.25));
        assert_eq!(result.summary.rows_per_second, Some(12.0));
        assert!(result.summary_complete);
        assert!(result.history_truncated && result.truncated);
        assert_eq!(result.history.len(), limit);
        assert_eq!(result.series.len(), 48);
        assert!(result.series.iter().all(|bucket| bucket.summary.syncs > 0));
        assert_eq!(
            result
                .series
                .iter()
                .map(|bucket| bucket.summary.syncs)
                .sum::<u64>(),
            1205
        );
        assert_eq!(result.pipes.len(), 1);
        assert_eq!(result.pipes[0].summary.syncs, 1205);
    }
    let compact = conn
        .get_engine_metrics(&MetricsQuery {
            begin: Some(begin),
            end: Some(end),
            summary_only: true,
            visible_refs: Some(vec![reference.clone()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(compact.summary.syncs, 1205);
    assert!(compact.summary_complete && compact.history.is_empty() && compact.pipelines.is_empty());
    let bounded = conn
        .aggregate_sync_metrics(
            &DataQuery {
                begin: Some(DateBound::Int(begin.timestamp_micros())),
                end: Some(DateBound::Int(end.timestamp_micros())),
                params: Some(
                    [(
                        "registration_id".into(),
                        meerschaum_core::Value::Str(rows[0].registration_id.clone()),
                    )]
                    .into(),
                ),
                ..Default::default()
            },
            begin.timestamp_micros(),
            1_800_000_000,
            2,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bounded.0.len(), 2);
    assert!(bounded.1, "a group cap must report partial aggregation");
    if flavor == "sqlite" {
        let storage = conn
            .engine_state_pipe(&conn.connector_keys(), "engine_metrics")
            .unwrap();
        let table = conn.pipe_table_name(&conn.sql_pipe(&storage).unwrap());
        conn.driver()
            .exec(&format!(
                "UPDATE {table} SET duration_seconds=NULL WHERE event_id='{}'",
                rows[0].event_id
            ))
            .await
            .unwrap();
        let legacy = conn
            .get_engine_metrics(&MetricsQuery {
                begin: Some(begin),
                end: Some(end),
                visible_refs: Some(vec![reference.clone()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(legacy.summary.syncs, 1205);
        assert_eq!(legacy.summary.duration_seconds, 301.0);
        assert!(legacy.summary_complete);
    }
    // A privacy change must invalidate the old registration segment before aggregation.
    pipe.update_parameters(
        [(
            "access".into(),
            meerschaum_core::Value::from_json_str(
                r#"{"visibility":"private","owner":"somebody-else"}"#,
            )
            .unwrap(),
        )]
        .into(),
    );
    assert!(conn.edit_pipe(&pipe, false).await.success);
    let result = conn
        .get_engine_metrics(&MetricsQuery {
            begin: Some(begin),
            end: Some(end),
            visible_refs: Some(vec![reference]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.summary.syncs, 0);
    assert!(result.history.is_empty());
    assert!(conn.delete_pipe(&pipe).await.success);
}

#[tokio::test]
async fn sqlite_window_outlives_history_cap() {
    window("sqlite").await;
}
#[tokio::test]
async fn postgres_window_outlives_history_cap() {
    window("postgresql").await;
}
#[tokio::test]
async fn mysql_window_outlives_history_cap() {
    window("mysql").await;
}
#[tokio::test]
async fn mssql_window_outlives_history_cap() {
    window("mssql").await;
}
#[tokio::test]
#[cfg(feature = "oracle")]
async fn oracle_window_outlives_history_cap() {
    window("oracle").await;
}
#[tokio::test]
async fn duckdb_window_outlives_history_cap() {
    window("duckdb").await;
}
