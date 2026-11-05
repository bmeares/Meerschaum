//! `fetch_stream` (the streamed SQL fetch a sync consumes chunk by chunk) yields the rows of the
//! collected `fetch`, in the same order, cut at the same row ranges, with the same dtypes.

mod common;

use common::*;
use meerschaum_connector::pipe_ops::frame_to_docs;
use meerschaum_connector::{Connector, DataQuery};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::instance::fetch::FetchArgs;
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::Value as KValue;

/// The frames of a streamed fetch, in order.
async fn stream_frames(
    conn: &SqlConnector,
    pipe: &meerschaum_core::Pipe,
    size: usize,
) -> Vec<Frame> {
    let mut stream = conn
        .fetch_stream(pipe, &FetchArgs::default(), size)
        .await
        .expect("fetch_stream")
        .expect("the flavor streams");
    let mut frames = Vec::new();
    while let Some(frame) = stream.next_frame().await.expect("next chunk") {
        frames.push(frame);
    }
    frames
}

/// Assert that `chunks` are `whole` cut every `size` rows (one empty chunk for no rows).
fn assert_chunks_of(whole: &Frame, chunks: &[Frame], size: usize) {
    let expected_heights: Vec<usize> = if whole.height() == 0 {
        vec![0]
    } else {
        (0..whole.height())
            .step_by(size)
            .map(|start| (whole.height() - start).min(size))
            .collect()
    };
    let heights: Vec<usize> = chunks.iter().map(Frame::height).collect();
    assert_eq!(heights, expected_heights);
    let mut start = 0;
    for chunk in chunks {
        let expected = whole.take(&(start..start + chunk.height()).collect::<Vec<_>>());
        assert_eq!(chunk.names(), whole.names());
        assert_eq!(chunk.dtypes(), whole.dtypes());
        assert_eq!(chunk.source_dtypes(), whole.source_dtypes());
        assert_eq!(frame_to_docs(chunk), frame_to_docs(&expected));
        start += chunk.height();
    }
}

flavor_tests!(stream_matches_collected_fetch, |c: Ctx| async move {
    if c.is_oracle() {
        // Oracle reads whole (unconstrained `NUMBER` typing needs every value).
        return;
    }
    let source = c.pipe(
        "src",
        "readings",
        None,
        r#"{"columns":{"datetime":"ts","id":"id"},"dtypes":{"id":"int","ts":"datetime","value":"float","label":"string","amount":"numeric"}}"#,
    );
    assert!(c.conn.register_pipe(&source).await.success);
    let docs = (0..250)
        .map(|n| {
            doc(vec![
                ("id", i(n)),
                ("ts", ndt(2024, 1, 1 + (n / 24) as u32, (n % 24) as u32, 0)),
                (
                    "value",
                    if n % 7 == 0 {
                        KValue::Null
                    } else {
                        fl(n as f64 / 4.0)
                    },
                ),
                (
                    "label",
                    if n % 5 == 0 {
                        KValue::Null
                    } else {
                        s(&format!("row {n}"))
                    },
                ),
                (
                    "amount",
                    KValue::Decimal(meerschaum_kernel::Decimal::parse(&format!("{n}.25")).unwrap()),
                ),
            ])
        })
        .collect();
    ok(c.sync(&source, docs).await);
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&source).unwrap());
    let fetcher = |definition: String| {
        c.pipe(
            "fetch",
            "readings",
            None,
            &serde_json::json!({
                "columns": {"datetime": "ts", "id": "id"},
                "fetch": {"definition": definition, "backtrack_minutes": 0},
            })
            .to_string(),
        )
    };
    // MSSQL refuses `ORDER BY` in the subquery the definition becomes without `OFFSET`.
    let order = if c.is_mssql() {
        "ORDER BY id OFFSET 0 ROWS"
    } else {
        "ORDER BY id"
    };
    let pipe = fetcher(format!("SELECT * FROM {table} {order}"));
    let whole = c
        .conn
        .fetch(&pipe, &FetchArgs::default())
        .await
        .expect("fetch")
        .expect("a frame");
    assert_eq!(whole.height(), 250);
    for size in [64, 250, 1000] {
        assert_chunks_of(&whole, &stream_frames(&c.conn, &pipe, size).await, size);
    }

    // No rows: one empty chunk with the result's columns.
    let empty = fetcher(format!("SELECT * FROM {table} WHERE id < 0"));
    let whole = c
        .conn
        .fetch(&empty, &FetchArgs::default())
        .await
        .expect("fetch")
        .expect("a frame");
    let chunks = stream_frames(&c.conn, &empty, 64).await;
    assert_chunks_of(&whole, &chunks, 64);
    assert_eq!(chunks[0].names(), whole.names());
    assert!(!chunks[0].names().is_empty());

    // Through the `Connector` trait (what the sync flow calls).
    let mut stream = Connector::fetch_stream(&c.conn, &pipe, &DataQuery::default(), 100)
        .await
        .expect("fetch_stream")
        .expect("the flavor streams");
    let mut heights = Vec::new();
    while let Some(input) = stream.next_chunk().await.expect("next chunk") {
        let PipeInput::Frame(frame) = input else {
            panic!("frames expected");
        };
        heights.push(frame.height());
    }
    assert_eq!(heights, vec![100, 100, 50]);
    c.delete(&source).await;
});

/// Chunks spanning the driver's read batches (100,000 rows) are cut at the same rows, and a
/// read that fails after the first batch fails the stream after the chunks already read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_chunks_span_read_batches_and_fail_mid_stream() {
    let Some(c) = ctx("sqlite", "stream_batches").await else {
        return;
    };
    let rows = 230_001;
    let pipe = c.pipe(
        "src",
        "large",
        None,
        &serde_json::json!({
            "fetch": {"definition": format!(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {rows}) \
                 SELECT x AS id, x * 2 AS doubled, 'v' || x AS label FROM c"
            )},
        })
        .to_string(),
    );
    let whole = c
        .conn
        .fetch(&pipe, &FetchArgs::default())
        .await
        .expect("fetch")
        .expect("a frame");
    assert_eq!(whole.height(), rows);
    assert_chunks_of(&whole, &stream_frames(&c.conn, &pipe, 70_000).await, 70_000);
    assert_chunks_of(
        &whole,
        &stream_frames(&c.conn, &pipe, 130_000).await,
        130_000,
    );
    drop(whole);

    // `json_extract` of invalid JSON fails at row 150,001: after the first read batch.
    let failing = c.pipe(
        "src",
        "failing",
        None,
        &serde_json::json!({
            "fetch": {"definition": format!(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {rows}) \
                 SELECT x AS id, CASE WHEN x > 150000 THEN json_extract('bad', '$') ELSE x END AS v \
                 FROM c"
            )},
        })
        .to_string(),
    );
    let mut stream = c
        .conn
        .fetch_stream(&failing, &FetchArgs::default(), 60_000)
        .await
        .expect("the query starts")
        .expect("the flavor streams");
    let mut heights = Vec::new();
    let error = loop {
        match stream.next_frame().await {
            Ok(Some(frame)) => heights.push(frame.height()),
            Ok(None) => panic!("the read must fail"),
            Err(e) => break e,
        }
    };
    assert_eq!(heights, vec![60_000]);
    assert!(error.to_string().contains("malformed JSON"), "{error}");
    assert!(stream.next_frame().await.expect("ended").is_none());
}

/// The storage identity a sync compares before streaming: one per database file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_exclusive_storage_names_the_file() {
    let path = temp_sqlite_path("exclusive");
    let uri = format!("sqlite:///{}", path.display());
    let a = SqlConnector::from_uri("a", &uri).await.unwrap();
    let b = SqlConnector::from_uri("b", &uri).await.unwrap();
    let other_path = temp_sqlite_path("exclusive_other");
    let other = SqlConnector::from_uri("a", &format!("sqlite:///{}", other_path.display()))
        .await
        .unwrap();
    assert!(a.exclusive_storage().is_some());
    assert_eq!(a.exclusive_storage(), b.exclusive_storage());
    assert_ne!(a.exclusive_storage(), other.exclusive_storage());
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(other_path);
}
