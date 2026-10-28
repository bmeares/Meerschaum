//! Native persistence, bounded reads, and privacy across independent instance handles.

mod common;

use chrono::{TimeDelta, Utc};
use common::*;
use meerschaum_connector::sync_metrics::{self, MetricsQuery, SyncMetric};
use meerschaum_connector::sync_status::{SyncOutcome, SyncSource};
use meerschaum_connector::{DataQuery, InstanceConnector};
use meerschaum_core::Pipe;
use meerschaum_engine::{
    InstanceState, NodeRow, NodeStatus, PipeRef, RunRecord, RunRow, StateWriter, SyncArgs,
};
use meerschaum_engine::{RunOptions, Selection};
use meerschaum_kernel::{enforce::PipeInput, value::Value as KValue};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

async fn captured(env: &Env, pipe: &Pipe, rows: u64, success: bool) -> SyncMetric {
    env.conn.get_pipe_id(pipe).await.unwrap().unwrap();
    let outcome = SyncOutcome::new(
        success,
        "private SQL and error text must never be retained",
        SyncSource::Python,
        Some("private-job-label".into()),
    )
    .with_counts(rows, 0, 0)
    .with_duration(Some(Duration::from_secs(2)));
    SyncMetric::from_outcome(pipe, &outcome)
}

#[tokio::test]
async fn an_empty_read_is_read_only_and_initialization_creates_canonical_empty_storage() {
    let env = env("metrics_empty").await;
    let result = env
        .conn
        .get_engine_metrics(&MetricsQuery::default())
        .await
        .unwrap();
    assert_eq!(result.summary.syncs, 0);
    let metric_pipe = sync_metrics::metrics_pipe(env.conn.as_ref()).await.unwrap();
    assert!(!env.conn.pipe_exists(&metric_pipe).await.unwrap());
    env.conn.initialize_engine_metrics().await.unwrap();
    for metric in ["engine_metrics", "engine_runs", "engine_nodes"] {
        let pipe = InstanceConnector::engine_state_pipe(env.conn.as_ref(), INSTANCE, metric)
            .await
            .unwrap()
            .unwrap();
        assert!(env.conn.pipe_exists(&pipe).await.unwrap(), "{metric}");
        assert_eq!(
            env.conn
                .get_pipe_rowcount(&pipe, &DataQuery::default())
                .await
                .unwrap(),
            Some(0)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_burst_syncs_and_failures_are_retained_without_status_debounce() {
    let env = env("metrics_bare").await;
    let pipe = env.register("bare", &json!({"columns":{"id":"id"}})).await;
    let engine = env.engine();
    for value in 0..3 {
        let input = PipeInput::Docs(vec![
            [("id".to_string(), KValue::Int(value))]
                .into_iter()
                .collect(),
        ]);
        let result = engine
            .sync_pipe(&pipe, Some(input), &SyncArgs::default())
            .await;
        assert!(result.success, "{}", result.message);
    }
    let failed = env
        .register(
            "failed",
            &json!({"fetch":{"definition":"SELECT * FROM missing_source"}}),
        )
        .await;
    let result = engine.sync_pipe(&failed, None, &SyncArgs::default()).await;
    assert!(!result.success);
    sync_metrics::flush(Duration::from_secs(4)).await;
    let metrics = env
        .conn
        .get_engine_metrics(&MetricsQuery::default())
        .await
        .unwrap();
    assert_eq!(metrics.summary.syncs, 4, "{:?}", metrics.history);
    assert_eq!(metrics.summary.succeeded, 3);
    assert_eq!(metrics.summary.failed, 1);
    assert_eq!(metrics.summary.rows, 3);
    assert_eq!(metrics.summary.success_rate, Some(0.75));
    assert!(
        metrics
            .history
            .iter()
            .all(|metric| metric.duration_seconds >= 0.0)
    );
    let wire = serde_json::to_string(&metrics).unwrap();
    assert!(
        !wire.contains("missing_source")
            && !wire.contains("message")
            && !wire.contains("private-job-label")
    );
}

#[tokio::test]
async fn persisted_history_is_shared_with_a_new_connector_and_filtered_before_aggregation() {
    let env = env("metrics_shared").await;
    let public = env.register("public", &json!({})).await;
    let private = env
        .register(
            "private",
            &json!({"access":{"owner":"owner", "visibility":"private"}}),
        )
        .await;
    let metrics = vec![
        captured(&env, &public, 3, true).await,
        captured(&env, &private, 1000, false).await,
    ];
    env.conn.record_sync_metrics(&metrics).await.unwrap();
    let other = connect(&env.db).await;
    let result = other
        .get_engine_metrics(&MetricsQuery {
            visible_refs: Some(vec![public.pipe_ref()]),
            visible_pipelines: Some(Vec::new()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.summary.syncs, 1);
    assert_eq!(result.summary.rows, 3);
    assert_eq!(result.summary.failed, 0);
    assert_eq!(result.pipes.len(), 1);
    assert_eq!(
        result
            .series
            .iter()
            .map(|bucket| bucket.summary.rows)
            .sum::<u64>(),
        3
    );
    assert!(!serde_json::to_string(&result).unwrap().contains("private"));
}

#[tokio::test]
async fn old_private_outcomes_from_another_worker_do_not_join_recreated_public_keys() {
    let env = env("metrics_generation").await;
    let old = env
        .register(
            "recreated",
            &json!({"access":{"owner":"old_owner","visibility":"private"}}),
        )
        .await;
    let old_id = env.conn.get_pipe_id(&old).await.unwrap().unwrap();
    let queued = captured(&env, &old, 999, false).await;
    let second_worker = connect(&env.db).await;
    assert!(
        InstanceConnector::delete_pipe(second_worker.as_ref(), &old)
            .await
            .success
    );
    let public = env.pipe("recreated", &json!({}));
    assert!(second_worker.register_pipe(&public).await.success);
    let new_id = second_worker.get_pipe_id(&public).await.unwrap().unwrap();
    assert_eq!(
        old_id, new_id,
        "SQLite reuses the final INTEGER PRIMARY KEY here"
    );
    // A different process may finish its already-captured write after registration changed.
    env.conn.record_sync_metrics(&[queued]).await.unwrap();
    let query = MetricsQuery {
        visible_refs: Some(vec![public.pipe_ref()]),
        ..Default::default()
    };
    assert_eq!(
        second_worker
            .get_engine_metrics(&query)
            .await
            .unwrap()
            .summary
            .syncs,
        0
    );
    let fresh = captured(&env, &public, 7, true).await;
    second_worker.record_sync_metrics(&[fresh]).await.unwrap();
    let result = second_worker.get_engine_metrics(&query).await.unwrap();
    assert_eq!(result.summary.syncs, 1);
    assert_eq!(result.summary.rows, 7);
}

#[tokio::test]
async fn bounds_limits_and_retention_are_enforced_by_the_persisted_store() {
    let env = env("metrics_limits").await;
    let pipe = env.register("limited", &json!({})).await;
    let mut metrics = Vec::new();
    for rows in 1..=4 {
        let mut metric = captured(&env, &pipe, rows, true).await;
        metric.ts -= TimeDelta::seconds(5 - rows as i64);
        metric.started = metric.ts - TimeDelta::seconds(2);
        metrics.push(metric);
    }
    let mut expired = captured(&env, &pipe, 900, false).await;
    expired.ts -= TimeDelta::days(31);
    expired.started = expired.ts - TimeDelta::seconds(2);
    metrics.push(expired);
    env.conn.record_sync_metrics(&metrics).await.unwrap();
    let result = env
        .conn
        .get_engine_metrics(&MetricsQuery {
            limit: Some(2),
            buckets: Some(2),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(result.truncated);
    assert_eq!(result.history.len(), 2);
    assert_eq!(result.summary.rows, 7);
    let storage = sync_metrics::metrics_pipe(env.conn.as_ref()).await.unwrap();
    assert_eq!(
        env.conn
            .get_pipe_rowcount(&storage, &DataQuery::default())
            .await
            .unwrap(),
        Some(4)
    );
    assert!(
        env.conn
            .get_engine_metrics(&MetricsQuery {
                begin: Some(Utc::now() - TimeDelta::days(31)),
                ..Default::default()
            })
            .await
            .is_err()
    );
}

async fn run(env: &Env, id: &str, reference: &str, registration: &str, rows: u64) {
    let metadata = Pipe::builder("mrsm", "pipeline")
        .location(Some("daily"))
        .instance(INSTANCE)
        .parameters(json_map(
            &json!({"pipeline":{"name":"daily", "id":"fixture-pipeline"}}),
        ))
        .build()
        .unwrap();
    if env.conn.get_pipe_id(&metadata).await.unwrap().is_none() {
        assert!(env.conn.register_pipe(&metadata).await.success);
    }
    let finished = Utc::now() - TimeDelta::seconds(1);
    let started = finished - TimeDelta::seconds(4);
    let mut node = NodeRow::new(
        id,
        &PipeRef::from_string(reference),
        meerschaum_engine::Action::Root,
        "",
    );
    node.status = NodeStatus::Succeeded;
    node.started = Some(started);
    node.finished = Some(finished);
    node.registration_id = Some(registration.into());
    node.attempts = 3;
    node.num_chunks = 2;
    node.inserted = rows;
    let record = RunRecord {
        run: RunRow {
            run_id: id.into(),
            started,
            finished: Some(finished),
            success: Some(true),
            message: Some("a private SQL error must not cross the numeric reader".into()),
            targets: vec![node.pipe_ref.clone()],
            workers: 1,
            num_nodes: 1,
            resumed_from: None,
            holder: "private-host-label".into(),
            selection: Some(
                json!({"pipeline_definitions":[{"name":"daily", "id":"fixture-pipeline"}]})
                    .to_string(),
            ),
        },
        nodes: vec![node],
    };
    InstanceState::new(env.registry.clone(), INSTANCE)
        .record_run(&record)
        .await
        .unwrap();
}

#[tokio::test]
async fn pipeline_metrics_reject_foreign_api_collisions_and_hidden_registration_segments() {
    let env = env("metrics_pipeline").await;
    let pipe = env.register("member", &json!({})).await;
    let metric = captured(&env, &pipe, 1, true).await;
    run(
        &env,
        "visible-run",
        &pipe.pipe_ref(),
        &metric.registration_id,
        12,
    )
    .await;
    let foreign = format!(
        "api:foreign|{}|{}|None",
        pipe.connector_keys(),
        pipe.metric_key()
    );
    run(
        &env,
        "foreign-secret-run",
        &foreign,
        &metric.registration_id,
        900,
    )
    .await;
    run(
        &env,
        "previous-private-run",
        &pipe.pipe_ref(),
        "old-registration",
        800,
    )
    .await;
    let result = env
        .conn
        .get_engine_metrics(&MetricsQuery {
            visible_refs: Some(vec![pipe.pipe_ref()]),
            visible_pipelines: Some(vec!["daily".into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.pipelines.len(), 1);
    assert_eq!(result.pipelines[0].summary.syncs, 1);
    assert_eq!(result.pipelines[0].summary.rows, 12);
    assert_eq!(result.pipelines[0].summary.retries, 2);
    let wire = serde_json::to_string(&result).unwrap();
    assert!(
        !wire.contains("foreign")
            && !wire.contains("previous-private")
            && !wire.contains("private-host")
            && !wire.contains("SQL")
    );
    let hidden = env
        .conn
        .get_engine_metrics(&MetricsQuery {
            visible_refs: Some(vec![pipe.pipe_ref()]),
            visible_pipelines: Some(Vec::new()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(hidden.pipelines.is_empty());

    // A member may finish inside a window before its long Pipeline run finishes.
    let state = InstanceState::new(env.registry.clone(), INSTANCE);
    let mut record = state.load_run("visible-run").await.unwrap().unwrap();
    record.run.finished = Some(Utc::now() + TimeDelta::hours(1));
    state.record_run(&record).await.unwrap();
    let mut metric = metric;
    metric.run_id = Some("visible-run".into());
    env.conn.record_sync_metrics(&[metric]).await.unwrap();
    let during_run = env
        .conn
        .get_engine_metrics(&MetricsQuery {
            pipeline: Some("daily".into()),
            visible_refs: Some(vec![pipe.pipe_ref()]),
            visible_pipelines: Some(vec!["daily".into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(during_run.summary.syncs, 1);
    assert_eq!(during_run.history[0].run_id.as_deref(), Some("visible-run"));
    assert!(
        during_run.pipelines.is_empty(),
        "whole-run timing is outside the window"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broken_telemetry_table_cannot_fail_the_original_sync() {
    let env = env("metrics_failure").await;
    env.exec("CREATE TABLE mrsm_engine_metrics (wrong_column INTEGER)")
        .await;
    let pipe = env
        .register("user_data", &json!({"columns":{"id":"id"}}))
        .await;
    let input = PipeInput::Docs(vec![
        [("id".to_string(), KValue::Int(1))].into_iter().collect(),
    ]);
    let result = env
        .engine()
        .sync_pipe(&pipe, Some(input), &SyncArgs::default())
        .await;
    assert!(result.success, "{}", result.message);
    sync_metrics::flush(Duration::from_secs(4)).await;
    assert_eq!(
        env.conn
            .get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .unwrap(),
        Some(1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_real_pipeline_run_records_each_node_once_with_its_registration_and_run_identity() {
    let env = env("metrics_real_run").await;
    let pipe = env
        .register(
            "actual",
            &json!({"fetch":{"definition":"SELECT 1 AS id"},"columns":{"id":"id"}}),
        )
        .await;
    let metadata = Pipe::builder("mrsm", "pipeline")
        .location(Some("daily"))
        .instance(INSTANCE)
        .parameters(json_map(
            &json!({"pipeline":{"id":"real-pipeline","name":"daily"}}),
        ))
        .build()
        .unwrap();
    assert!(env.conn.register_pipe(&metadata).await.success);
    let result = env
        .engine()
        .run(RunOptions {
            selection: Some(Selection::refs([PipeRef::from_pipe(&pipe)]).with_graph(true)),
            pipeline_definitions: vec![json!({"id":"real-pipeline","name":"daily"})],
            ..Default::default()
        })
        .await;
    assert!(result.success, "{}", result.message);
    assert!(
        !serde_json::to_string(&result.record)
            .unwrap()
            .contains("registration_id"),
        "public run details must not expose stable registration or ACL stamps"
    );
    sync_metrics::flush(Duration::from_secs(4)).await;
    let result = env
        .conn
        .get_engine_metrics(&MetricsQuery {
            visible_refs: Some(vec![pipe.pipe_ref()]),
            visible_pipelines: Some(vec!["daily".into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.summary.syncs, 1, "{result:?}");
    assert_eq!(result.summary.rows, 1);
    assert!(result.history[0].run_id.is_some());
    assert_eq!(result.pipelines.len(), 1);
    assert_eq!(result.pipelines[0].summary.rows, 1);
}

#[tokio::test]
async fn valkey_history_is_shared_deduplicated_and_counts_all_changed_rows() {
    use meerschaum_connector_valkey::{MemoryBackend, ValkeyConnector};
    let backend = Arc::new(MemoryBackend::new());
    let conn = ValkeyConnector::with_backend("metrics", Default::default(), backend.clone());
    let pipe = Pipe::builder("plugin:metrics", "shared")
        .instance("valkey:metrics")
        .build()
        .unwrap();
    assert!(conn.register_pipe(&pipe).await.success);
    conn.get_pipe_id(&pipe).await.unwrap().unwrap();
    let mut outcome = SyncOutcome::new(true, "not retained", SyncSource::Python, None)
        .with_counts(2, 1, 3)
        .with_duration(Some(Duration::from_secs(2)));
    outcome.deleted = 4;
    let metric = SyncMetric::from_outcome(&pipe, &outcome);
    assert_eq!(metric.rows, 10);
    conn.record_sync_metrics(std::slice::from_ref(&metric))
        .await
        .unwrap();
    conn.record_sync_metrics(std::slice::from_ref(&metric))
        .await
        .unwrap();
    let other = ValkeyConnector::with_backend("metrics", Default::default(), backend);
    let result = other
        .get_engine_metrics(&MetricsQuery::default())
        .await
        .unwrap();
    assert_eq!(result.summary.syncs, 1, "{result:?}");
    assert_eq!(result.summary.rows, 10);
    assert_eq!(result.summary.deleted, 4);
    assert_eq!(result.summary.rows_per_second, Some(5.0));
    let empty = other
        .get_engine_metrics(&MetricsQuery {
            visible_refs: Some(Vec::new()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(empty.summary.syncs, 0);
}

#[tokio::test]
async fn valkey_metrics_bound_candidates_and_document_fetches_before_visibility_filters() {
    use meerschaum_connector_valkey::{MemoryBackend, ValkeyConnector};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let backend = Arc::new(MemoryBackend::new());
    let conn = ValkeyConnector::with_backend("bounded", Default::default(), backend.clone());
    let public = Pipe::builder("fixture", "public")
        .instance("valkey:bounded")
        .build()
        .unwrap();
    let private = Pipe::builder("fixture", "private")
        .instance("valkey:bounded")
        .parameters(json_map(
            &json!({"access":{"owner":"bob","visibility":"private"}}),
        ))
        .build()
        .unwrap();
    for pipe in [&public, &private] {
        assert!(conn.register_pipe(pipe).await.success);
        conn.get_pipe_id(pipe).await.unwrap().unwrap();
    }
    let now = Utc::now() - TimeDelta::seconds(2);
    let metrics: Vec<_> = (0..1_200)
        .map(|i| {
            let pipe = if i % 2 == 0 { &private } else { &public };
            let mut outcome = SyncOutcome::new(true, "", SyncSource::Python, None).with_counts(
                if i % 2 == 0 { 900 } else { 1 },
                0,
                0,
            );
            outcome.finished = now - TimeDelta::microseconds(i);
            SyncMetric::from_outcome(pipe, &outcome)
        })
        .collect();
    conn.record_sync_metrics(&metrics).await.unwrap();
    let fetched = Arc::new(AtomicUsize::new(0));
    let counter = fetched.clone();
    backend.set_fault_hook(Some(Arc::new(move |command, key| {
        if command == "GET" && key.contains("mrsm_engine_") && key.contains("indices:") {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        None
    })));
    let result = conn
        .get_engine_metrics(&MetricsQuery {
            limit: Some(10),
            visible_refs: Some(vec![public.pipe_ref()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        result.truncated,
        "a bounded pre-filter sample must be labeled incomplete"
    );
    assert!(result.summary.rows > 0 && result.summary.rows <= 10);
    assert!(
        result
            .history
            .iter()
            .all(|metric| metric.metric_key == "public")
    );
    assert!(!serde_json::to_string(&result).unwrap().contains("private"));
    assert!(
        fetched.load(Ordering::Relaxed) <= 11,
        "unbounded document fetches"
    );
    let historical = conn
        .get_engine_metrics(&MetricsQuery {
            begin: Some(now - TimeDelta::microseconds(1_000)),
            end: Some(now - TimeDelta::microseconds(990)),
            limit: Some(10),
            visible_refs: Some(vec![public.pipe_ref()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        historical.summary.rows, 5,
        "native LIMIT must honor historical score bounds"
    );

    backend.set_fault_hook(None);
    for metric in ["engine_runs", "engine_nodes"] {
        let pipe = InstanceConnector::engine_state_pipe(&conn, "valkey:bounded", metric)
            .await
            .unwrap()
            .unwrap();
        let docs = (0..600)
            .map(|i| {
                [
                    ("run_id".into(), KValue::Str(format!("run-{i}"))),
                    ("pipe_ref".into(), KValue::Str(public.pipe_ref())),
                    ("started".into(), KValue::Str(now.to_rfc3339())),
                ]
                .into_iter()
                .collect()
            })
            .collect();
        let context =
            meerschaum_connector::pipe_ops::dtypes_context(&conn, &pipe, pipe.parameters())
                .await
                .unwrap();
        let frame =
            meerschaum_kernel::enforce::pipe_enforce_dtypes(PipeInput::Docs(docs), &context)
                .unwrap();
        assert!(
            InstanceConnector::sync_pipe(&conn, &pipe, frame, &Default::default())
                .await
                .success
        );
        let (rows, capped) = conn
            .read_engine_metric_rows(
                metric,
                &DataQuery {
                    limit: Some(7),
                    order: Some("desc".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 7, "{metric}");
        assert!(capped, "{metric}");
    }
    // New retention deletes exact index members as well as document keys.
    conn.expire_engine_metrics((now + TimeDelta::seconds(1)).timestamp_micros())
        .await
        .unwrap();
    assert_eq!(
        conn.get_engine_metrics(&MetricsQuery::default())
            .await
            .unwrap()
            .summary
            .syncs,
        0
    );
    use meerschaum_connector_valkey::ValkeyBackend;
    let pipe = sync_metrics::metrics_pipe(&conn).await.unwrap();
    assert_eq!(
        backend
            .zcard(&meerschaum_connector_valkey::keys::quote_table(
                &meerschaum_connector_valkey::pipes::pipe_target(&pipe)
            ))
            .await
            .unwrap(),
        0
    );
}
