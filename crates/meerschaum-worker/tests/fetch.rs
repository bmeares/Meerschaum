//! Plugin `fetch()` through the worker, for every result shape v4 accepts, synced natively into
//! SQLite and compared with what v4 syncs for the same plugin.

mod common;

use std::sync::Arc;

use meerschaum_connector::{DataQuery, InstanceConnector};
use meerschaum_core::{Pipe, Value as CValue};
use meerschaum_sql::SqlConnector;
use meerschaum_worker::chunk::FetchChunk;
use meerschaum_worker::connector::FetchOptions;
use meerschaum_worker::sync::{PythonSyncOptions, sync_python_pipe};
use meerschaum_worker::{PythonConnector, WorkerError};

use common::{TestEnv, assert_same_data, v4_compare};

const V4: &str = "sql:v4ref";
const RS: &str = "sql:rs";

/// Every shape the `wk_shapes` plugin returns that v4 syncs successfully.
const SHAPES: &[&str] = &[
    "docs",
    "docs_strdates",
    "columns",
    "df",
    "polars",
    "json",
    "lines",
    "generator",
    "special",
    "bounds",
    "hooked",
];

struct Dbs {
    _dir: tempfile::TempDir,
    rs_uri: String,
    env: TestEnv,
}

fn dbs() -> Dbs {
    let dir = tempfile::Builder::new()
        .prefix("mrsm-wk-db-")
        .tempdir()
        .unwrap();
    let rs_uri = format!("sqlite:///{}", dir.path().join("rs.db").display());
    let v4_uri = format!("sqlite:///{}", dir.path().join("v4.db").display());
    let env = TestEnv::with_env(vec![
        ("MRSM_SQL_RS".into(), rs_uri.clone()),
        ("MRSM_SQL_V4REF".into(), v4_uri),
    ]);
    Dbs {
        _dir: dir,
        rs_uri,
        env,
    }
}

fn pipe(ck: &str, mk: &str, instance: &str) -> Pipe {
    Pipe::builder(ck, mk)
        .instance(instance)
        .build()
        .expect("valid pipe")
}

async fn sync_all(
    source: &PythonConnector,
    instance: Arc<dyn InstanceConnector>,
    connector_keys: &str,
    shapes: &[&str],
    syncs: usize,
) {
    for shape in shapes {
        let mut p = pipe(connector_keys, shape, "sql:rs");
        for _ in 0..syncs {
            let st = sync_python_pipe(
                source,
                Arc::clone(&instance),
                &mut p,
                &PythonSyncOptions::default(),
            )
            .await;
            assert!(st.success, "{shape}: {}", st.message);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_fetch_shape_matches_v4() {
    require_python!();
    let d = dbs();
    let pool = d.env.pool(2);
    let instance: Arc<dyn InstanceConnector> =
        Arc::new(SqlConnector::from_uri("rs", &d.rs_uri).await.unwrap());
    let source = PythonConnector::connect("plugin:wk_shapes", pool.clone())
        .await
        .expect("describes the plugin connector");
    assert!(source.info().has_fetch);
    assert!(source.info().has_register);
    assert!(!source.info().has_sync);

    sync_all(
        &source,
        Arc::clone(&instance),
        "plugin:wk_shapes",
        SHAPES,
        1,
    )
    .await;
    // `register()`'s parameters were applied on registration.
    let p = pipe("plugin:wk_shapes", "docs", "sql:rs");
    let attrs = instance.get_pipe_attributes(&p).await.unwrap();
    let params = attrs.get("parameters").cloned().unwrap_or(CValue::Null);
    assert_eq!(
        params.get("columns").and_then(|c| c.get("datetime")),
        Some(&CValue::Str("dt".into())),
        "{attrs:?}"
    );

    let result = v4_compare(&d.env, "plugin:wk_shapes", SHAPES, 1, V4, RS);
    assert_same_data(&result, SHAPES, V4, RS);
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_bounds_match_v4() {
    require_python!();
    let d = dbs();
    let pool = d.env.pool(1);
    let instance: Arc<dyn InstanceConnector> =
        Arc::new(SqlConnector::from_uri("rs", &d.rs_uri).await.unwrap());
    let source = PythonConnector::connect("plugin:wk_shapes", pool.clone())
        .await
        .unwrap();
    // The second sync fetches from the sync time minus `backtrack_minutes` (30).
    sync_all(
        &source,
        Arc::clone(&instance),
        "plugin:wk_shapes",
        &["bounds"],
        2,
    )
    .await;
    let result = v4_compare(&d.env, "plugin:wk_shapes", &["bounds"], 2, V4, RS);
    assert_same_data(&result, &["bounds"], V4, RS);
    let records = &result["data"]["bounds"]["sql:rs"]["records"];
    assert_eq!(
        records[0]["begin"], "2023-12-31 23:30:00+00:00",
        "{records}"
    );
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generator_chunks_stream_separately() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let source = PythonConnector::connect("plugin:wk_shapes", pool.clone())
        .await
        .unwrap();
    let p = pipe("plugin:wk_shapes", "generator", "sql:main");
    let (mut rx, task) = source.fetch_stream(&p, &FetchOptions::default(), 1);
    let mut kinds = Vec::new();
    while let Some(chunk) = rx.recv().await {
        kinds.push(match chunk {
            FetchChunk::Docs(d) => format!("docs:{}", d.len()),
            FetchChunk::Frame(f) => format!("frame:{}", f.height()),
            FetchChunk::Columns(c) => format!("columns:{}", c["dt"].len()),
            FetchChunk::Text(t) => format!("text:{}", t.len()),
        });
    }
    let outcome = task.await.unwrap().unwrap();
    assert!(outcome.generator);
    assert_eq!(outcome.num_chunks, 3);
    assert_eq!(kinds, vec!["docs:2", "frame:2", "columns:1"]);

    // Every non-generator shape arrives as one chunk of the expected kind.
    for (shape, kind) in [
        ("docs", "docs"),
        ("columns", "columns"),
        ("df", "frame"),
        ("polars", "frame"),
        ("json", "text"),
        ("lines", "text"),
    ] {
        let p = pipe("plugin:wk_shapes", shape, "sql:main");
        let outcome = source
            .fetch_chunks(&p, &FetchOptions::default())
            .await
            .unwrap();
        assert_eq!(outcome.chunks.len(), 1, "{shape}");
        let got = match &outcome.chunks[0] {
            FetchChunk::Docs(_) => "docs",
            FetchChunk::Frame(_) => "frame",
            FetchChunk::Columns(_) => "columns",
            FetchChunk::Text(_) => "text",
        };
        assert_eq!(got, kind, "{shape}");
    }

    // `None` and exceptions.
    let p = pipe("plugin:wk_shapes", "none", "sql:main");
    let outcome = source
        .fetch_chunks(&p, &FetchOptions::default())
        .await
        .unwrap();
    assert!(outcome.returned_none);
    let p = pipe("plugin:wk_shapes", "raise", "sql:main");
    let err = source
        .fetch_chunks(&p, &FetchOptions::default())
        .await
        .unwrap_err();
    match err {
        WorkerError::Python {
            exception,
            message,
            traceback,
            ..
        } => {
            assert_eq!(exception, "ValueError");
            assert_eq!(message, "synthetic fetch failure");
            assert!(traceback.unwrap().contains("wk_shapes"));
        }
        other => panic!("expected a Python error, got {other}"),
    }
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connector_fetch_trait_builds_one_frame() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let source = PythonConnector::connect("plugin:wk_shapes", pool.clone())
        .await
        .unwrap();
    let p = pipe("plugin:wk_shapes", "generator", "sql:main");
    let df = meerschaum_connector::Connector::fetch(&source, &p, &DataQuery::default())
        .await
        .unwrap()
        .expect("a frame");
    assert_eq!(df.height(), 5);
    pool.shutdown().await;
}
