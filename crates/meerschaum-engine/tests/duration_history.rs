//! Duration estimates stay small as unrelated history grows, including historic Gantt reads.
mod common;

use chrono::{TimeDelta, Timelike, Utc};
use common::*;
use meerschaum_engine::instance_state::{InstanceState, node_doc};
use meerschaum_engine::runs::{EnginePipe, NodeRow, NodeStatus};
use meerschaum_engine::{Action, EngineReader, RunOptions, Selection};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duration_samples_are_projected_bounded_and_scoped_to_each_pipe_and_action() {
    let env = env("duration_samples").await;
    let state = InstanceState::new(env.registry.clone(), INSTANCE);
    let raw = env.pipe_ref("raw");
    let quiet = env.pipe_ref("quiet");
    let unrelated = env.pipe_ref("unrelated");
    let start = Utc::now() - TimeDelta::hours(2);
    let mut docs = Vec::new();
    for index in 0..300 {
        for pipe in [&raw, &unrelated] {
            let time = start + TimeDelta::seconds(index);
            let mut row = NodeRow::new(&format!("old-{index}"), pipe, Action::Root, "fixture");
            row.status = NodeStatus::Succeeded;
            row.started = Some(time);
            row.finished = Some(time + TimeDelta::seconds(1));
            row.duration_seconds = Some(index as f64 + 1.0);
            row.attempts = 1;
            row.message = Some("A wide result that estimates never need. ".repeat(32));
            docs.push(node_doc(&row, time));
        }
    }
    for (id, pipe, action, time, status, duration) in [
        (
            "quiet",
            &quiet,
            Action::Root,
            start,
            NodeStatus::Succeeded,
            7.0,
        ),
        (
            "rare",
            &raw,
            Action::Rebuild,
            start,
            NodeStatus::Succeeded,
            99.0,
        ),
        (
            "failed",
            &raw,
            Action::Root,
            Utc::now(),
            NodeStatus::Failed,
            999.0,
        ),
        (
            "future",
            &raw,
            Action::Root,
            Utc::now() + TimeDelta::hours(1),
            NodeStatus::Succeeded,
            9999.0,
        ),
    ] {
        let mut row = NodeRow::new(id, pipe, action, "fixture");
        row.status = status;
        row.started = Some(time);
        row.finished = Some(time + TimeDelta::seconds(1));
        row.duration_seconds = Some(duration);
        row.attempts = 1;
        docs.push(node_doc(&row, time));
    }
    state
        .write_rows(INSTANCE, EnginePipe::Nodes, docs)
        .await
        .unwrap();
    env.conn.enable_query_log();
    let samples = state
        .node_duration_history(
            &[
                (raw.clone(), Action::Root),
                (quiet.clone(), Action::Rebuild),
            ],
            Some(start - TimeDelta::seconds(1)),
            Some(Utc::now()),
            64,
        )
        .await
        .unwrap();
    assert_eq!(samples.len(), 65);
    let raw_samples: Vec<_> = samples.iter().filter(|row| row.pipe_ref == raw).collect();
    assert_eq!(raw_samples.len(), 64);
    assert_eq!(raw_samples[0].duration_seconds, Some(237.0));
    assert_eq!(raw_samples[63].duration_seconds, Some(300.0));
    assert_eq!(
        samples.iter().filter(|row| row.pipe_ref == quiet).count(),
        1
    );
    assert!(samples.iter().all(|row| row.message.is_none()));
    assert!(samples.iter().all(|row| row.started.unwrap() < Utc::now()));
    let queries = env.conn.take_query_log();
    let reads: Vec<_> = queries
        .iter()
        .filter(|query| query.contains("FROM \"mrsm_engine_nodes\""))
        .collect();
    assert_eq!(reads.len(), 3, "{queries:#?}");
    for query in reads {
        assert!(query.contains("LIMIT 64"), "{query}");
        assert!(query.contains("DESC"), "{query}");
        assert!(
            query
                .split("WHERE")
                .nth(1)
                .unwrap()
                .contains("\"pipe_ref\""),
            "{query}"
        );
        assert!(!query.contains("\"message\""), "{query}");
    }
    let rare = state
        .node_duration_history(&[(raw, Action::Rebuild)], None, Some(Utc::now()), 64)
        .await
        .unwrap();
    assert_eq!(rare.len(), 1);
    assert_eq!(rare[0].duration_seconds, Some(99.0));
    env.conn.enable_query_log();
    assert!(
        state
            .node_duration_history(&[], None, None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        state
            .node_duration_history(&[(quiet, Action::Root)], None, None, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(env.conn.take_query_log().is_empty());
    let indices = env
        .conn
        .get_pipe_columns_indices(&meerschaum_engine::instance_state::engine_pipe(
            INSTANCE,
            EnginePipe::Nodes,
        ))
        .await
        .unwrap();
    assert!(
        format!("{indices:?}").contains("IX_mrsm_engine_nodes_pipe_ref_started"),
        "{indices:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_and_historic_gantt_use_bounded_duration_reads() {
    let env = env("bounded_duration_run").await;
    env.exec("CREATE TABLE source (id INTEGER, value INTEGER)")
        .await;
    env.exec("INSERT INTO source VALUES (1, 7)").await;
    env.register(
        "raw",
        &serde_json::json!({"columns":{"id":"id"},"fetch":{"definition":"SELECT * FROM source"}}),
    )
    .await;
    let engine = env.engine();
    let first = engine
        .run(RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("raw")])),
            ..Default::default()
        })
        .await;
    assert!(first.success, "{}", first.message);
    env.conn.enable_query_log();
    let second = engine
        .run(RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("raw")])),
            ..Default::default()
        })
        .await;
    assert!(second.success, "{}", second.message);
    let reader = EngineReader::new(env.registry.clone(), env.config());
    let detail = reader
        .run_detail(second.run_id().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.bars[0].planned_source.as_deref(), Some("history"));
    let first_duration = first.record.as_ref().unwrap().nodes[0]
        .duration_seconds
        .unwrap();
    assert!((detail.bars[0].planned_duration_seconds.unwrap() - first_duration).abs() < 1e-6);
    let log = env.conn.take_query_log();
    let estimates: Vec<_> = log
        .iter()
        .filter(|query| {
            query.contains("FROM \"mrsm_engine_nodes\"")
                && query
                    .split_once("WHERE")
                    .is_some_and(|(_, filters)| filters.contains("\"status\""))
        })
        .collect();
    assert_eq!(estimates.len(), 2, "{log:#?}");
    for query in estimates {
        assert!(query.contains("LIMIT 64"), "{query}");
        assert!(query.contains("\"pipe_ref\""), "{query}");
        assert!(!query.contains("\"message\""), "{query}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fractional_cutoffs_and_legacy_node_columns_preserve_past_estimates() {
    let env = env("legacy_duration_cutoff").await;
    let state = InstanceState::new(env.registry.clone(), INSTANCE);
    let pipe = env.pipe_ref("raw");
    let base = (Utc::now() - TimeDelta::minutes(1))
        .with_nanosecond(0)
        .unwrap();
    let mut docs = Vec::new();
    for (id, milliseconds) in [("past", 100), ("future", 300)] {
        let started = base + TimeDelta::milliseconds(milliseconds);
        let mut row = NodeRow::new(id, &pipe, Action::Root, "fixture");
        row.status = NodeStatus::Succeeded;
        row.started = Some(started);
        row.finished = Some(started + TimeDelta::milliseconds(1));
        row.duration_seconds = Some(milliseconds as f64);
        row.attempts = 1;
        docs.push(node_doc(&row, started));
    }
    state
        .write_rows(INSTANCE, EnginePipe::Nodes, docs)
        .await
        .unwrap();
    for legacy in [false, true] {
        if legacy {
            env.exec("ALTER TABLE mrsm_engine_nodes DROP COLUMN actual_started")
                .await;
        }
        let connection = connect(&env.db).await;
        let state = InstanceState::new(registry(&connection), INSTANCE);
        let rows = state
            .node_duration_history(
                &[(pipe.clone(), Action::Root)],
                None,
                Some(base + TimeDelta::milliseconds(200)),
                64,
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "legacy={legacy}: {rows:?}");
        assert_eq!(rows[0].run_id, "past");
    }
}
