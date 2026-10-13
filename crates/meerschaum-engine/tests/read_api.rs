//! The console read API (`EngineReader`): runs, Gantt bars, events and ledger lag on edges,
//! over a SQLite instance with a three-pipe chain that has run.

mod common;

use common::*;
use meerschaum_engine::{Depth, EngineReader, RunFilter, RunOptions, RunStatus, Selection};

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
            selection: Some(
                Selection::refs([env.pipe_ref("c_hourly")]).with_parents(Some(Depth::All)),
            ),
            ..Default::default()
        })
        .await;
    assert!(r.success, "{}", r.message);

    let reader = EngineReader::new(env.registry.clone(), env.config());
    let runs = reader.runs(&RunFilter::default()).await.unwrap();
    assert_eq!(runs.len(), 1);
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

    let run_id = runs[0].run.run_id.clone();
    let detail = reader.run_detail(&run_id).await.unwrap().unwrap();
    assert_eq!(detail.bars.len(), 3);
    assert!(
        detail
            .bars
            .iter()
            .all(|b| b.actual_start.is_some() && b.actual_end.is_some())
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
    let view = reader.pipeline(&[]).await.unwrap();
    let a = env.pipe_ref("a_raw");
    let edge = view.children_of(&a).next().expect("a_raw has a child");
    assert!(edge.lagging(), "{edge:?}");
    assert!(edge.has_cursor && !edge.dirty.is_empty());
    let b = env.pipe_ref("b_clean");
    assert_eq!(view.parents_of(&b).count(), 1);
}
