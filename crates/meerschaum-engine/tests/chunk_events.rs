//! Per-chunk progress events in fetch order (§5.7): one `chunk_started` / `chunk_finished` pair
//! per fetched chunk with its rows, bounds and timing, as the CLI Gantt and the console consume.

mod common;

use common::*;
use meerschaum_engine::{
    ChunkSource, EventKind, KeyFilter, NodeSyncOptions, RunOptions, Selection,
};
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_chunks_emit_progress_in_fetch_order() {
    let env = env("chunk_events").await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec(
        "INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0), ('2026-01-01 01:00:00', 1, 2.0), \
         ('2026-01-01 02:00:00', 1, 3.0), ('2026-01-01 03:00:00', 1, 4.0), \
         ('2026-01-01 04:00:00', 1, 5.0)",
    )
    .await;
    env.register(
        "chunky",
        &json!({
            "sql": "SELECT ts, id, v FROM src",
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }),
    )
    .await;
    let engine = env.engine();
    let mut events = engine.subscribe();
    let result = engine
        .run(RunOptions {
            selection: Some(Selection::filter(KeyFilter {
                metric_keys: vec!["chunky".into()],
                ..Default::default()
            })),
            sync: NodeSyncOptions {
                chunksize: Some(2),
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
    assert!(result.success, "{}", result.message);
    assert_eq!(env.rowcount("chunky").await, 5);

    let mut chunk_events = Vec::new();
    while let Ok(e) = events.try_recv() {
        if matches!(
            e.kind,
            EventKind::ChunkStarted { .. } | EventKind::ChunkFinished { .. }
        ) {
            chunk_events.push(e);
        }
    }
    // started 0, finished 0, started 1, finished 1, started 2, finished 2.
    assert_eq!(chunk_events.len(), 6, "{chunk_events:#?}");
    let rows = [2u64, 2, 1];
    for (i, pair) in chunk_events.chunks(2).enumerate() {
        match (&pair[0].kind, &pair[1].kind) {
            (
                EventKind::ChunkStarted {
                    chunk,
                    source,
                    rows: started_rows,
                    begin,
                    end,
                    ..
                },
                EventKind::ChunkFinished {
                    chunk: finished,
                    success,
                    inserted,
                    rows: finished_rows,
                    duration_ms,
                    ..
                },
            ) => {
                assert_eq!((*chunk, *finished), (i, i));
                assert_eq!(*source, ChunkSource::Fetch);
                assert_eq!(*started_rows, Some(rows[i]));
                assert_eq!(*finished_rows, Some(rows[i]));
                assert!(*success);
                assert_eq!(*inserted, rows[i]);
                assert!(begin.is_some() && end.is_some());
                assert!(duration_ms.is_some());
            }
            other => panic!("unexpected order: {other:?}"),
        }
        assert_eq!(pair[0].seq + 1, pair[1].seq);
    }
    // Chunk bounds follow the data: the chunks are contiguous and ascending.
    let begins: Vec<_> = chunk_events
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::ChunkStarted { begin, .. } => {
                Some(serde_json::to_value(begin).unwrap().to_string())
            }
            _ => None,
        })
        .collect();
    assert!(begins.windows(2).all(|w| w[0] < w[1]), "{begins:?}");

    // The wire shape the console renders (`serde(tag = "event")`, flattened into the envelope).
    let json = serde_json::to_value(&chunk_events[1]).unwrap();
    assert_eq!(json["event"], "chunk_finished");
    assert_eq!(json["source"], "fetch");
    assert_eq!(json["chunk"], 0);
    assert_eq!(json["rows"], 2);
    assert_eq!(json["inserted"], 2);
    assert!(json["duration_ms"].is_u64());
    assert!(json["begin"].is_string() && json["end"].is_string());
}

/// `system.engine.events` defaults to `all`: the chunk events are persisted with the run (the
/// console's live view and `GET /engine/runs/{run_id}/events/stream` replay them), `nodes`
/// leaves them out, and retention prunes them with the rest of the run's history.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_events_persist_by_default() {
    use meerschaum_core::config::Config;
    use meerschaum_engine::{EngineConfig, EventGranularity};

    assert_eq!(EngineConfig::default().events, EventGranularity::All);
    let config = |events: Option<&str>| {
        let c = Config::bare(
            meerschaum_core::config::paths::Paths::for_root(temp_dir("chunk_events_config")),
            2,
            false,
            false,
        );
        if let Some(e) = events {
            c.set_config(json_map(&json!({"system": {"engine": {"events": e}}})));
        }
        EngineConfig::from_config(&c, None).events
    };
    assert_eq!(config(None), EventGranularity::All);
    assert_eq!(config(Some("nodes")), EventGranularity::Nodes);
    assert_eq!(config(Some("off")), EventGranularity::Off);

    let env = env("chunk_events_persist").await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec(
        "INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0), ('2026-01-01 01:00:00', 1, 2.0), \
         ('2026-01-01 02:00:00', 1, 3.0)",
    )
    .await;
    env.register(
        "chunky",
        &json!({
            "sql": "SELECT ts, id, v FROM src",
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }),
    )
    .await;
    let opts = || RunOptions {
        selection: Some(Selection::filter(KeyFilter {
            metric_keys: vec!["chunky".into()],
            ..Default::default()
        })),
        sync: NodeSyncOptions {
            chunksize: Some(2),
            ..Default::default()
        },
        ..Default::default()
    };
    let engine = env.engine();
    let result = engine.run(opts()).await;
    assert!(result.success, "{}", result.message);
    let run_id = result.run_id().unwrap().to_string();
    let events = engine.state().load_events(&run_id).await.unwrap();
    let names: Vec<&str> = events.iter().map(|e| e.event.as_str()).collect();
    assert_eq!(
        names.iter().filter(|n| **n == "chunk_started").count(),
        2,
        "{names:?}"
    );
    assert_eq!(
        names.iter().filter(|n| **n == "chunk_finished").count(),
        2,
        "{names:?}"
    );
    assert!(names.contains(&"node_planned"), "{names:?}");
    assert_eq!(names.last(), Some(&"run_finished"), "{names:?}");
    // Sequence numbers are the emitter's: contiguous, so a tail can resume after any of them.
    let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (0..seqs.len() as u64).collect::<Vec<_>>());
    // The payload is the typed event's.
    let finished = events.iter().find(|e| e.event == "chunk_finished").unwrap();
    let payload: serde_json::Value = serde_json::from_str(&finished.payload).unwrap();
    assert_eq!(payload["source"], "fetch");
    assert_eq!(payload["rows"], 2);

    // `nodes` keeps the run and node transitions only.
    env.exec("INSERT INTO src VALUES ('2026-01-01 03:00:00', 1, 4.0)")
        .await;
    let nodes = env.engine_with(|c| c.events = EventGranularity::Nodes);
    let result = nodes.run(opts()).await;
    assert!(result.success, "{}", result.message);
    let events = nodes
        .state()
        .load_events(result.run_id().unwrap())
        .await
        .unwrap();
    assert!(!events.is_empty());
    assert!(
        events
            .iter()
            .all(|e| !e.event.starts_with("chunk_") && e.event != "node_planned"),
        "{events:#?}"
    );

    // Retention prunes the chunk rows with the run.
    let errors = engine
        .state()
        .prune_history(chrono::Utc::now() + chrono::TimeDelta::seconds(1))
        .await;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(
        engine
            .state()
            .load_events(&run_id)
            .await
            .unwrap()
            .is_empty()
    );
}

/// A process event sink (what `mrsm start api` installs to stream the runs of its actions
/// live) sees every run's events, whatever started the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_sinks_see_every_run() {
    use std::sync::{Arc, Mutex};

    struct Collect(Mutex<Vec<(String, meerschaum_engine::Event)>>);
    impl meerschaum_engine::ProcessEventSink for Collect {
        fn emit(&self, home_instance: &str, event: meerschaum_engine::Event) {
            self.0
                .lock()
                .unwrap()
                .push((home_instance.to_string(), event));
        }
    }
    let sink = Arc::new(Collect(Mutex::new(Vec::new())));
    let registration = meerschaum_engine::add_process_event_sink(sink.clone());

    let env = env("chunk_events_process_sink").await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)")
        .await;
    env.register(
        "sunk",
        &json!({
            "sql": "SELECT ts, id, v FROM src",
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }),
    )
    .await;
    let result = env
        .engine()
        .run(RunOptions {
            selection: Some(Selection::filter(KeyFilter {
                metric_keys: vec!["sunk".into()],
                ..Default::default()
            })),
            ..Default::default()
        })
        .await;
    assert!(result.success, "{}", result.message);
    let run_id = result.run_id().unwrap().to_string();
    let seen: Vec<String> = sink
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, e)| e.run_id == run_id)
        .map(|(instance, e)| {
            // The instance that keeps the run comes with each event.
            assert_eq!(instance, &env.engine().config().home_instance);
            serde_json::to_value(e).unwrap()["event"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(
        seen.first().map(String::as_str),
        Some("run_started"),
        "{seen:?}"
    );
    assert_eq!(
        seen.last().map(String::as_str),
        Some("run_finished"),
        "{seen:?}"
    );
    assert!(seen.iter().any(|e| e == "chunk_finished"), "{seen:?}");

    // Dropping the registration removes the sink and the process's reference to it.
    let held = Arc::strong_count(&sink);
    drop(registration);
    assert_eq!(Arc::strong_count(&sink), held - 1);
}
