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
