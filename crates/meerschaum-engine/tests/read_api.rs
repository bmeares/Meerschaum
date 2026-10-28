//! The console read API (`EngineReader`): runs, Gantt bars, events and ledger lag on edges,
//! over a SQLite instance with a three-pipe chain that has run.

mod common;

use common::*;
use meerschaum_engine::{Depth, EngineReader, RunFilter, RunOptions, RunStatus, Selection};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_reads_nodes_in_bulk_and_preserves_timestamp_boundary_matches() {
    use chrono::{Timelike, Utc};
    use meerschaum_engine::instance_state::{InstanceState, node_doc, run_doc};
    use meerschaum_engine::runs::{EnginePipe, NodeRow, NodeStatus, RunRow};
    let env = env("bulk_read_api").await;
    let state = InstanceState::new(env.registry.clone(), &env.config().home_instance);
    let base = Utc::now().with_nanosecond(500_000_000).unwrap();
    let mut runs = Vec::new();
    let mut nodes = Vec::new();
    for index in 0..300 {
        // Several equal timestamps, plus many distinct microseconds in the same second.
        let started = base + chrono::TimeDelta::microseconds(index / 3);
        let run: RunRow = serde_json::from_value(serde_json::json!({
            "run_id": format!("fixture-{index:04}"), "started": started,
            "finished": started, "success": true, "targets": [env.pipe_ref("raw")],
            "workers": 1, "num_nodes": 1, "holder": "fixture",
            "selection": serde_json::json!({"pipeline_definitions": [{"name": format!("named-{index}")}]}).to_string(),
        })).unwrap();
        let mut node = NodeRow::new(
            &run.run_id,
            &env.pipe_ref("raw"),
            meerschaum_engine::Action::Incremental,
            "fixture",
        );
        node.status = NodeStatus::Succeeded;
        runs.push(run_doc(&run));
        nodes.push(node_doc(&node, started));
    }
    state
        .write_rows(&env.config().home_instance, EnginePipe::Runs, runs)
        .await
        .unwrap();
    state
        .write_rows(&env.config().home_instance, EnginePipe::Nodes, nodes)
        .await
        .unwrap();
    let reader = EngineReader::new(env.registry.clone(), env.config());
    env.conn.enable_query_log();
    let history = reader
        .runs(&RunFilter {
            limit: Some(100),
            ..Default::default()
        })
        .await
        .unwrap();
    let log = env.conn.take_query_log();
    assert_eq!(history.len(), 100);
    assert!(
        history
            .iter()
            .all(|run| run.counts.get("succeeded") == Some(&1))
    );
    let reads = |table: &str| {
        log.iter()
            .filter(|query| query.contains(&format!("FROM \"{table}\"")))
            .count()
    };
    assert_eq!(reads("mrsm_engine_runs"), 1, "{log:#?}");
    assert_eq!(reads("mrsm_engine_nodes"), 1, "{log:#?}");
    let page = state.recent_runs_query(128, None, None).await.unwrap();
    let missing = (0..300)
        .find(|index| {
            !page
                .iter()
                .any(|run| run.run_id == format!("fixture-{index:04}"))
        })
        .unwrap();
    let named = reader
        .runs(&RunFilter {
            run_name: Some(format!("named-{missing}")),
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(named.len(), 1);
    assert_eq!(named[0].run.run_id, format!("fixture-{missing:04}"));
    let fractional = reader
        .runs(&RunFilter {
            since: Some(base + chrono::TimeDelta::microseconds(70)),
            limit: Some(1000),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(fractional.len(), 90);
    assert!(
        fractional
            .iter()
            .all(|run| { run.run.started >= base + chrono::TimeDelta::microseconds(70) })
    );
    assert!(
        reader
            .runs(&RunFilter {
                limit: Some(0),
                ..Default::default()
            })
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filtered_history_pages_across_seconds_without_missing_or_repeating_ties() {
    use chrono::{Timelike, Utc};
    use meerschaum_engine::instance_state::{InstanceState, run_doc};
    use meerschaum_engine::runs::{EnginePipe, RunRow};
    let env = env("history_pages").await;
    let state = InstanceState::new(env.registry.clone(), &env.config().home_instance);
    let base = Utc::now().with_nanosecond(0).unwrap();
    let mut runs = Vec::new();
    for index in 0..1500 {
        let started = base
            + chrono::TimeDelta::seconds(index / 500)
            + chrono::TimeDelta::microseconds(index % 500 / 3);
        let selected = index % 500 == 5;
        let run: RunRow = serde_json::from_value(serde_json::json!({
            "run_id": format!("page-{index:04}"), "started": started,
            "finished": started, "success": !selected,
            "targets": [env.pipe_ref(if selected { "raw" } else { "other" })],
            "workers": 1, "num_nodes": 0, "holder": "fixture",
        }))
        .unwrap();
        runs.push(run_doc(&run));
    }
    state
        .write_rows(&env.config().home_instance, EnginePipe::Runs, runs)
        .await
        .unwrap();
    let reader = EngineReader::new(env.registry.clone(), env.config());
    let history = reader
        .runs(&RunFilter {
            limit: Some(1200),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(history.len(), 1200);
    let ids: std::collections::HashSet<_> = history.iter().map(|row| &row.run.run_id).collect();
    assert_eq!(ids.len(), 1200);
    assert!(history.windows(2).all(|pair| {
        let (left, right) = (&pair[0].run, &pair[1].run);
        left.started > right.started
            || (left.started == right.started && left.run_id > right.run_id)
    }));
    let selected = reader
        .runs(&RunFilter {
            status: Some(RunStatus::Failed),
            pipe: Some("raw".into()),
            since: Some(base + chrono::TimeDelta::seconds(1) + chrono::TimeDelta::microseconds(1)),
            limit: Some(10),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|row| row.run.run_id.as_str())
            .collect::<Vec<_>>(),
        vec!["page-1005", "page-0505"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn named_history_follows_live_uuid_across_rename_and_rejects_replaced_identity() {
    use chrono::Utc;
    use meerschaum_connector::sync_metrics::{pipe_key, registrations};
    use meerschaum_core::Pipe;
    use meerschaum_engine::instance_state::{InstanceState, node_doc, run_doc};
    use meerschaum_engine::runs::{EnginePipe, NodeRow, RunRow};
    let env = env("history_rename").await;
    env.register("raw", &serde_json::json!({})).await;
    let stamp = registrations(env.conn.as_ref())
        .await
        .unwrap()
        .remove(&pipe_key(INSTANCE, "raw", None))
        .unwrap();
    let current = |id: &str| {
        Pipe::builder("mrsm", "pipeline")
            .location(Some("renamed"))
            .instance(INSTANCE)
            .parameters(json_map(&serde_json::json!({
                "pipeline": {"id": id, "name": "renamed", "registry": INSTANCE}
            })))
            .build()
            .unwrap()
    };
    assert!(env.conn.register_pipe(&current("stable-id")).await.success);
    let state = InstanceState::new(env.registry.clone(), INSTANCE);
    let mut runs = Vec::new();
    let mut nodes = Vec::new();
    for (id, registry, proof) in [
        ("before-rename", INSTANCE, None),
        ("foreign", "sql:elsewhere", None),
        ("api-client", "api:client", None),
        ("other-api-client", "api:other", None),
        ("contradictory-proof", "api:client", Some("sql:elsewhere")),
    ] {
        let started = Utc::now();
        let mut snapshot = serde_json::json!({
            "name":"original", "id":"stable-id", "registry":registry
        });
        if let Some(proof) = proof {
            snapshot["registration_registry"] = proof.into();
        }
        let run: RunRow = serde_json::from_value(serde_json::json!({
            "run_id": id, "started": started, "finished": started, "success": true,
            "targets": [env.pipe_ref("raw")], "workers": 1, "num_nodes": 1, "holder": "fixture",
            "selection": serde_json::json!({"pipeline_definitions":[snapshot]}).to_string()
        }))
        .unwrap();
        let mut node = NodeRow::new(
            id,
            &env.pipe_ref("raw"),
            meerschaum_engine::Action::Root,
            "fixture",
        );
        node.registration_id = Some(stamp.clone());
        runs.push(run_doc(&run));
        nodes.push(node_doc(&node, started));
    }
    state
        .write_rows(INSTANCE, EnginePipe::Nodes, nodes)
        .await
        .unwrap();
    state
        .write_rows(INSTANCE, EnginePipe::Runs, runs)
        .await
        .unwrap();
    let reader = EngineReader::new(env.registry.clone(), env.config());
    let filter = RunFilter {
        run_name: Some("renamed".into()),
        ..Default::default()
    };
    let history = reader.runs(&filter).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].run.run_id, "before-rename");
    // A separately configured exact home alias can read both server- and client-origin
    // snapshots, while a foreign alias or contradictory explicit proof stays foreign.
    let registry = std::sync::Arc::new(
        meerschaum_engine::StaticRegistry::new().with_instance("api:client", env.conn.clone()),
    );
    let mut config = env.config();
    config.home_instance = "api:client".into();
    let api_reader = EngineReader::new(registry, config);
    let api_history = api_reader.runs(&filter).await.unwrap();
    assert_eq!(
        api_history
            .iter()
            .map(|run| run.run.run_id.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        ["api-client", "before-rename"].into_iter().collect(),
    );
    assert!(env.conn.edit_pipe(&current("new-id"), false).await.success);
    assert!(reader.runs(&filter).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_generation_evidence_is_fresh_complete_batched_and_never_serialized() {
    use chrono::Utc;
    use meerschaum_connector::sync_metrics::{pipe_key, registrations};
    use meerschaum_engine::instance_state::{InstanceState, node_doc, run_doc};
    use meerschaum_engine::runs::{EnginePipe, NodeRow, RunRow};
    let env = env("history_generations").await;
    let private = env
        .register(
            "raw",
            &serde_json::json!({
                "access": {"owner": "bob", "visibility": "private"}
            }),
        )
        .await;
    // Capture before constructing work records; never regenerate evidence from current
    // metadata while reading an older run after an access/registration change.
    let stamp = registrations(env.conn.as_ref())
        .await
        .unwrap()
        .remove(&pipe_key(INSTANCE, "raw", None))
        .unwrap();
    let state = InstanceState::new(env.registry.clone(), INSTANCE);
    let mut runs = Vec::new();
    let mut nodes = Vec::new();
    for (id, count, captured, canonical) in [
        ("current", 1, true, 0),
        ("legacy", 1, false, 0),
        ("incomplete", 2, true, 0),
        ("empty", 0, true, 0),
        ("api-proven", 1, true, 1),
        ("api-unproven", 1, true, 2),
        ("foreign", 1, true, 3),
    ] {
        let reference = if matches!(canonical, 1 | 2) {
            meerschaum_engine::PipeRef::new("api:client", INSTANCE, "raw", None)
        } else {
            env.pipe_ref("raw")
        };
        let run: RunRow = serde_json::from_value(serde_json::json!({
            "run_id": id, "started": Utc::now(), "finished": Utc::now(), "success": false,
            "targets": [reference], "workers": 1, "num_nodes": count, "holder": "fixture",
            "message": "private retained run failure",
        }))
        .unwrap();
        let mut node = NodeRow::new(
            id,
            &reference,
            meerschaum_engine::Action::Incremental,
            "fixture",
        );
        node.registration_id = captured.then(|| stamp.clone());
        node.registration_ref = match canonical {
            1 => Some(env.pipe_ref("raw")),
            3 => Some(meerschaum_engine::PipeRef::new(
                "sql:unavailable",
                INSTANCE,
                "raw",
                None,
            )),
            _ => None,
        };
        runs.push(run_doc(&run));
        nodes.push(node_doc(&node, run.started));
    }
    state
        .write_rows(INSTANCE, EnginePipe::Runs, runs)
        .await
        .unwrap();
    state
        .write_rows(INSTANCE, EnginePipe::Nodes, nodes)
        .await
        .unwrap();
    let ids: Vec<String> = [
        "current",
        "legacy",
        "incomplete",
        "empty",
        "api-proven",
        "api-unproven",
        "foreign",
        "missing",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let reader = EngineReader::new(env.registry.clone(), env.config());
    env.conn.enable_query_log();
    let current = reader.run_generations(&ids).await.unwrap();
    let log = env.conn.take_query_log();
    for id in ["current", "api-proven"] {
        assert!(current[id], "{current:?}");
    }
    for id in [
        "legacy",
        "incomplete",
        "empty",
        "api-unproven",
        "foreign",
        "missing",
    ] {
        assert!(!current[id], "{current:?}");
    }
    let reads = |table: &str| {
        log.iter()
            .filter(|query| query.replace('"', "").contains(&format!("FROM {table}")))
            .count()
    };
    assert_eq!(reads("mrsm_engine_runs"), 1, "{log:#?}");
    assert_eq!(reads("mrsm_engine_nodes"), 1, "{log:#?}");
    assert!(
        reads("mrsm_pipes") <= 2,
        "registration scans repeated per run: {log:#?}"
    );
    let detail = reader.run_detail("api-proven").await.unwrap().unwrap();
    let json = serde_json::to_string(&detail).unwrap();
    assert!(
        !json.contains("registration_id") && !json.contains("registration_ref"),
        "{json}"
    );
    assert!(env.conn.delete_pipe(&private).await.success);
    env.register("raw", &serde_json::json!({})).await;
    let after = reader.run_generations(&ids).await.unwrap();
    assert!(
        after.values().all(|valid| !valid),
        "stale private run was reassigned: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_shows_runs_gantt_events_and_edge_lag() {
    let fx: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/v4_sync/v4_sync.json")).unwrap();
    let env = env("read_api").await;
    env.exec(fx["create"].as_str().unwrap()).await;
    for p in fx["pipes"].as_array().unwrap().iter().take(3) {
        env.register(p["metric"].as_str().unwrap(), &p["parameters"])
            .await;
    }
    for stmt in fx["phases"][0]["statements"].as_array().unwrap() {
        env.exec(stmt.as_str().unwrap()).await;
    }
    let engine = env.engine();
    let r = engine
        .run(RunOptions {
            run_id: Some(meerschaum_engine::runs::new_named_run_id("read-demo")),
            selection: Some(
                Selection::refs([env.pipe_ref("c_hourly")]).with_parents(Some(Depth::All)),
            ),
            ..Default::default()
        })
        .await;
    assert!(r.success, "{}", r.message);
    assert!(!r.message.contains("warning"), "{}", r.message);

    let reader = EngineReader::new(env.registry.clone(), env.config());
    let runs = reader.runs(&RunFilter::default()).await.unwrap();
    assert_eq!(runs.len(), 1);
    let named = reader
        .runs(&RunFilter {
            run_name: Some("read-demo".into()),
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(named.len(), 1);
    let other = reader
        .runs(&RunFilter {
            run_name: Some("read".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(other.is_empty());

    assert_eq!(runs[0].status, RunStatus::Succeeded);
    assert_eq!(runs[0].counts.get("succeeded"), Some(&3));
    let failed = reader
        .runs(&RunFilter {
            status: Some(RunStatus::Failed),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(failed.is_empty());
    let by_pipe = reader
        .runs(&RunFilter {
            pipe: Some("b_clean".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(by_pipe.len(), 1);
    // The selected target is c_hourly; its ancestor must be searchable by its display label.
    for label in [
        env.pipe_ref("b_clean").label(),
        env.pipe_ref("b_clean").label_with_instance(),
    ] {
        let by_label = reader
            .runs(&RunFilter {
                pipe: Some(label),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(by_label.len(), 1);
    }

    let run_id = runs[0].run.run_id.clone();
    let detail = reader.run_detail(&run_id).await.unwrap().unwrap();
    assert_eq!(detail.bars.len(), 3);
    assert!(
        detail
            .bars
            .iter()
            .all(|b| b.actual_start.is_some() && b.actual_end.is_some())
    );
    // A first run has no history: every planned bar that syncs lasts the default duration
    // (30 s), starting at the node's CPM earliest start.
    for (bar, node) in detail.bars.iter().zip(&detail.nodes) {
        assert_eq!(bar.planned_duration_seconds, Some(30.0), "{bar:?}");
        assert_eq!(bar.planned_source.as_deref(), Some("default"));
        let (start, end) = (bar.planned_start.unwrap(), bar.planned_end.unwrap());
        assert_eq!((end - start).num_seconds(), 30);
        let offset = (start - detail.summary.run.started).num_milliseconds() as f64 / 1000.0;
        assert!((offset - node.planned_start_seconds.unwrap()).abs() < 1e-3);
    }
    assert!(
        detail
            .nodes
            .iter()
            .all(|n| n.planned_duration_seconds == Some(30.0))
    );
    assert!(reader.run_detail("nope").await.unwrap().is_none());

    let events = reader.events_after(&run_id, None).await.unwrap();
    assert!(
        events.iter().any(|e| e.event == "run_started"),
        "{events:?}"
    );
    let last = events.last().unwrap().seq;
    assert!(
        reader
            .events_after(&run_id, Some(last))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(reader.current_run_id().await.unwrap(), Some(run_id.clone()));

    // Caught up: three nodes, two implicit edges, no lag.
    let view = reader.pipeline(&[]).await.unwrap();
    assert_eq!(view.nodes.len(), 3);
    assert_eq!(view.edges.len(), 2, "{:?}", view.edges);
    assert!(view.edges.iter().all(|e| e.sources == ["template"]));
    assert!(view.edges.iter().all(|e| !e.lagging()), "{:?}", view.edges);
    assert!(
        view.nodes
            .iter()
            .all(|n| n.last_run_id.as_deref() == Some(run_id.as_str()))
    );

    // New source rows, then sync only a_raw: its edge to b_clean now lags.
    for stmt in fx["phases"][1]["statements"].as_array().unwrap() {
        env.exec(stmt.as_str().unwrap()).await;
    }
    let r = engine
        .run(RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("a_raw")])),
            ..Default::default()
        })
        .await;
    assert!(r.success, "{}", r.message);
    // The second run's planned bar for a_raw comes from the first run's duration.
    let second = reader
        .run_detail(r.run_id().unwrap())
        .await
        .unwrap()
        .unwrap();
    let first_a = detail
        .nodes
        .iter()
        .find(|n| n.pipe_ref == env.pipe_ref("a_raw"))
        .and_then(|n| n.duration_seconds)
        .unwrap();
    let bar = &second.bars[0];
    assert_eq!(bar.planned_source.as_deref(), Some("history"), "{bar:?}");
    let planned = bar.planned_duration_seconds.unwrap();
    assert!((planned - first_a).abs() < 1e-6, "{planned} vs {first_a}");
    // In process, the run's own rows carry the duration the schedule used.
    let row = &r.record.as_ref().unwrap().nodes[0];
    assert!((row.planned_duration_seconds.unwrap() - first_a).abs() < 1e-6);
    let view = reader.pipeline(&[]).await.unwrap();
    let a = env.pipe_ref("a_raw");
    let edge = view.children_of(&a).next().expect("a_raw has a child");
    assert!(edge.lagging(), "{edge:?}");
    assert!(edge.has_cursor && !edge.dirty.is_empty());
    let b = env.pipe_ref("b_clean");
    assert_eq!(view.parents_of(&b).count(), 1);
}

/// Separate CLI/server processes reopen the same SQLite instance with cold metadata caches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_sqlite_connectors_preserve_repeated_runs_nodes_and_events() {
    let fx: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/v4_sync/v4_sync.json")).unwrap();
    let env = env("cold_read_api").await;
    env.exec(fx["create"].as_str().unwrap()).await;
    for pipe in fx["pipes"].as_array().unwrap().iter().take(2) {
        env.register(pipe["metric"].as_str().unwrap(), &pipe["parameters"])
            .await;
    }
    for phase in 0..2 {
        for statement in fx["phases"][phase]["statements"].as_array().unwrap() {
            env.exec(statement.as_str().unwrap()).await;
        }
        let conn = connect(&env.db).await;
        let registry = registry(&conn);
        let engine = meerschaum_engine::Engine::new(registry.clone(), env.config());
        let result = engine
            .run(RunOptions {
                selection: Some(
                    Selection::refs([env.pipe_ref("b_clean")]).with_parents(Some(Depth::All)),
                ),
                ..Default::default()
            })
            .await;
        assert!(result.success, "{}", result.message);
        assert!(!result.message.contains("warning"), "{}", result.message);
        let reader = EngineReader::new(registry, env.config());
        let runs = reader.runs(&RunFilter::default()).await.unwrap();
        assert_eq!(runs.len(), phase + 1, "{}", result.message);
        for summary in runs {
            let run_id = summary.run.run_id;
            let detail = reader.run_detail(&run_id).await.unwrap().unwrap();
            assert_eq!(detail.nodes.len(), 2);
            let events = reader.events_after(&run_id, None).await.unwrap();
            assert!(
                events.iter().any(|event| event.event == "run_started"),
                "{events:?}"
            );
            assert!(
                events.iter().any(|event| event.event == "run_finished"),
                "{events:?}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_sees_running_and_queued_stages_before_sync_finishes() {
    use meerschaum_engine::FaultInjector;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;
    struct Hold {
        entered: tokio::sync::Notify,
        first: AtomicBool,
        released: AtomicBool,
    }
    impl FaultInjector for Hold {
        fn after_data_commit(&self, _: &meerschaum_engine::PipeRef) -> Result<(), String> {
            if !self.first.swap(true, Ordering::SeqCst) {
                self.entered.notify_one();
                while !self.released.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            Ok(())
        }
    }
    struct Release(Arc<Hold>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.released.store(true, Ordering::SeqCst);
        }
    }
    let env = env("live_read_api").await;
    env.register(
        "legacy",
        &serde_json::json!({
            "columns": {"id": "id"}, "fetch": {"definition": "SELECT 1 AS id"}
        }),
    )
    .await;
    let legacy = env
        .engine()
        .run(RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("legacy")])),
            ..Default::default()
        })
        .await;
    assert!(legacy.success);
    let legacy_id = legacy.run_id().unwrap().to_string();
    let reader = EngineReader::new(env.registry.clone(), env.config());
    let original_start = reader.run_detail(&legacy_id).await.unwrap().unwrap().bars[0].actual_start;
    let pipe = env
        .conn
        .engine_state_pipe(INSTANCE, "engine_nodes")
        .unwrap();
    let target = pipe.target(None).unwrap();
    env.exec(&format!("UPDATE \"{target}\" SET started = actual_started"))
        .await;
    env.exec(&format!(
        "ALTER TABLE \"{target}\" DROP COLUMN actual_started"
    ))
    .await;
    assert_eq!(
        reader.run_detail(&legacy_id).await.unwrap().unwrap().bars[0].actual_start,
        original_start
    );
    for metric in ["a", "b"] {
        env.register(
            metric,
            &serde_json::json!({
                "columns": {"id": "id"}, "fetch": {"definition": "SELECT 1 AS id"}
            }),
        )
        .await;
    }
    let hold = Arc::new(Hold {
        entered: tokio::sync::Notify::new(),
        first: AtomicBool::new(false),
        released: AtomicBool::new(false),
    });
    let release = Release(hold.clone());
    let engine = env.engine().with_faults(hold.clone());
    let selection = Selection::refs([env.pipe_ref("a"), env.pipe_ref("b")]);
    let run_id = meerschaum_engine::runs::new_named_run_id("live");
    let id = run_id.clone();
    let task = tokio::spawn(async move {
        engine
            .run(RunOptions {
                run_id: Some(id),
                selection: Some(selection),
                workers: Some(1),
                ..Default::default()
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), hold.entered.notified())
        .await
        .unwrap();
    let reader = EngineReader::new(env.registry.clone(), env.config());
    let detail = reader.run_detail(&run_id).await.unwrap().unwrap();
    assert_eq!(detail.summary.status, RunStatus::Running);
    assert_eq!(detail.bars.len(), 2);
    assert_eq!(
        detail
            .bars
            .iter()
            .filter(|b| b.actual_start.is_some())
            .count(),
        1
    );
    assert!(detail.bars.iter().all(|b| b.actual_end.is_none()));
    drop(release);
    assert!(task.await.unwrap().success);
    assert_eq!(
        reader.run_detail(&legacy_id).await.unwrap().unwrap().bars[0].actual_start,
        original_start
    );
    let detail = reader.run_detail(&run_id).await.unwrap().unwrap();
    assert_eq!(detail.summary.status, RunStatus::Succeeded);
    assert_eq!(
        detail.bars.len(),
        2,
        "progress upserts must not duplicate node rows"
    );
    assert!(
        detail
            .bars
            .iter()
            .all(|b| b.actual_start.is_some() && b.actual_end.is_some())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_runs_initialize_progress_tables_safely() {
    let env = env("concurrent_progress").await;
    for metric in ["a", "b"] {
        env.register(
            metric,
            &serde_json::json!({
                "columns": {"id": "id"}, "fetch": {"definition": "SELECT 1 AS id"}
            }),
        )
        .await;
    }
    let other = connect(&env.db).await;
    let first = env.engine();
    let second = meerschaum_engine::Engine::new(registry(&other), env.config());
    let (a, b) = tokio::join!(
        first.run(RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("a")])),
            ..Default::default()
        }),
        second.run(RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("b")])),
            ..Default::default()
        })
    );
    let reader = EngineReader::new(env.registry.clone(), env.config());
    for run in [a, b] {
        assert!(run.success, "{}", run.message);
        assert!(!run.message.contains("cannot record"), "{}", run.message);
        let detail = reader
            .run_detail(run.run_id().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(detail.bars.len(), 1);
        assert_eq!(detail.summary.status, RunStatus::Succeeded);
    }
}
