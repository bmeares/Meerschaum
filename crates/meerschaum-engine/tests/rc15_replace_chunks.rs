//! `fetch: {replace: true}` with a source that yields chunks (rc15 Q19): the first load, into a
//! table that does not exist yet, has nothing to replace and streams its chunks like any fetch;
//! later fetches are one window, their chunks joined into one frame and replaced at once.

mod common;

use std::sync::{Arc, Mutex};

use common::*;
use meerschaum_connector::{
    ChunkStream, ChunkedFetch, Connector, ConnectorResult, DataQuery, FetchedKind, async_trait,
};
use meerschaum_core::Pipe;
use meerschaum_engine::{Engine, StaticRegistry, SyncArgs};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::frame::Column;
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use serde_json::json;

/// A source whose `fetch()` is a generator of one-row frames (one per day it holds).
struct Days(Arc<Mutex<Vec<u32>>>);

struct Chunks(std::vec::IntoIter<Frame>);

#[async_trait]
impl ChunkStream for Chunks {
    async fn next_chunk(&mut self) -> ConnectorResult<Option<PipeInput>> {
        Ok(self.0.next().map(PipeInput::Frame))
    }

    async fn finish(&mut self) -> ConnectorResult<FetchedKind> {
        Ok(FetchedKind::Data { generator: true })
    }
}

fn day_frame(day: u32) -> Frame {
    let ts = KValue::Datetime(DateTimeValue::naive(
        chrono::NaiveDate::from_ymd_opt(2024, 1, day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    ));
    Frame::new(vec![
        Column::object("ts", vec![ts]),
        Column::object("id", vec![KValue::Int(1)]),
        Column::object("v", vec![KValue::Int(day as i64)]),
    ])
    .unwrap()
}

#[async_trait]
impl Connector for Days {
    fn connector_type(&self) -> &str {
        "plugin"
    }

    fn label(&self) -> &str {
        "days"
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }

    async fn fetch(&self, _pipe: &Pipe, _query: &DataQuery) -> ConnectorResult<Option<Frame>> {
        unreachable!("the chunked fetch answers")
    }

    async fn fetch_chunked(
        &self,
        _pipe: &Pipe,
        _query: &DataQuery,
        _sync_chunks: bool,
    ) -> ConnectorResult<Option<ChunkedFetch>> {
        let frames: Vec<Frame> = self
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|d| day_frame(*d))
            .collect();
        Ok(Some(ChunkedFetch {
            chunks: Box::new(Chunks(frames.into_iter())),
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacing_fetches_stream_the_first_load_and_join_later_chunks() {
    let env = env("rc15_replace_chunks").await;
    let days = Arc::new(Mutex::new(vec![1, 2, 3, 4]));
    let source: Arc<dyn Connector> = Arc::new(Days(days.clone()));
    let registry =
        Arc::new(StaticRegistry::clone(&env.registry).with_source("plugin:days", source));
    let engine = Engine::new(registry, env.config());
    let pipe = Pipe::builder("plugin:days", "readings")
        .instance(INSTANCE)
        .parameters(json_map(&json!({
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "int"},
            "fetch": {"replace": true, "backtrack_minutes": 14400},
        })))
        .build()
        .unwrap();

    // First load: no table, nothing to replace; the generator's chunks are synced one by one.
    let first = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(first.success, "{}", first.message);
    assert!(
        first.message.contains("Synced 4 chunks"),
        "{}",
        first.message
    );
    assert_eq!(first.inserted, 4, "{}", first.message);
    assert_eq!(env.table_of("plugin:days", "readings").await.rows.len(), 4);

    // A later fetch is one window: its chunks are replaced together (day 2 left the source).
    days.lock().unwrap().retain(|d| *d != 2);
    let second = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(second.success, "{}", second.message);
    assert_eq!(
        second.message, "Replaced 4 rows with 3 rows.",
        "{}",
        second.message
    );
    assert_eq!(env.table_of("plugin:days", "readings").await.rows.len(), 3);
}
