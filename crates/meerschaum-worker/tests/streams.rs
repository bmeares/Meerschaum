//! Supervised Python streams (§5.6): the v4 pattern (`sync()` subscribes and returns, callbacks
//! call `pipe.sync()`) and `subscribe(pipe, sink)` connectors.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use meerschaum_connector::{DataQuery, InstanceConnector, async_trait};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::{Map, Pipe, SuccessTuple};
use meerschaum_worker::callback::{CallbackHandler, CallbackRequest, InstanceSyncHandler};
use meerschaum_worker::{PythonStream, StreamEvent, StreamMode, StreamOptions};
use tokio::sync::mpsc::UnboundedReceiver;

use common::TestEnv;

/// Records each callback, then syncs it natively.
struct Recorder {
    inner: InstanceSyncHandler,
    seen: Mutex<Vec<(String, usize)>>,
}

#[async_trait]
impl CallbackHandler for Recorder {
    async fn handle(&self, request: CallbackRequest) -> SuccessTuple {
        let rows = request.chunk.as_ref().map(|c| c.len()).unwrap_or(0);
        self.seen.lock().unwrap().push((request.op.clone(), rows));
        self.inner.handle(request).await
    }
}

async fn sqlite() -> (tempfile::TempDir, Arc<dyn InstanceConnector>) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite:///{}", dir.path().join("rs.db").display());
    let conn = SqlConnector::from_uri("rs", &uri).await.unwrap();
    (dir, Arc::new(conn))
}

fn pipe(ck: &str, mk: &str, lk: Option<&str>) -> Pipe {
    Pipe::builder(ck, mk)
        .location(lk)
        .instance("sql:rs")
        .parameters(
            meerschaum_core::Value::from_json_str(r#"{"columns": {"datetime": "dt", "id": "id"}}"#)
                .unwrap()
                .to_map(),
        )
        .build()
        .unwrap()
}

async fn rowcount(instance: &Arc<dyn InstanceConnector>, pipe: &Pipe) -> i64 {
    instance
        .get_pipe_rowcount(pipe, &DataQuery::default())
        .await
        .ok()
        .flatten()
        .unwrap_or(0)
}

async fn wait_for_rows(instance: &Arc<dyn InstanceConnector>, pipe: &Pipe, n: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while rowcount(instance, pipe).await < n {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {n} rows"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn next_event(events: &mut UnboundedReceiver<StreamEvent>) -> StreamEvent {
    tokio::time::timeout(Duration::from_secs(60), events.recv())
        .await
        .expect("an event in time")
        .expect("the supervisor is running")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn v4_pattern_stream_forwards_callbacks() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let (_dir, instance) = sqlite().await;
    let handler = Arc::new(Recorder {
        inner: InstanceSyncHandler::single(Arc::clone(&instance)),
        seen: Mutex::new(Vec::new()),
    });
    let p = pipe("plugin:wk_streamer", "events", Some("4"));
    let (stream, mut events) = PythonStream::start(
        pool.clone(),
        "plugin:wk_streamer",
        &p,
        &Map::new(),
        StreamMode::V4Sync,
        handler.clone(),
        StreamOptions::default(),
    );
    assert!(matches!(
        next_event(&mut events).await,
        StreamEvent::Started { .. }
    ));
    // `sync()` returns immediately; the worker stays alive for the callbacks.
    assert_eq!(
        next_event(&mut events).await,
        StreamEvent::Returned(SuccessTuple::ok("subscribed"))
    );
    wait_for_rows(&instance, &p, 4).await;
    assert!(stream.is_running());
    stream.stop().await;
    let mut rest = Vec::new();
    while let Ok(Some(e)) = tokio::time::timeout(Duration::from_secs(5), events.recv()).await {
        rest.push(e);
    }
    assert_eq!(rest.last(), Some(&StreamEvent::Stopped), "{rest:?}");
    let seen = handler.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 4, "{seen:?}");
    assert!(seen.iter().all(|(op, rows)| op == "sync" && *rows == 1));
    // The stream's worker is dedicated: the pool never lent it out.
    assert_eq!(pool.idle_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_stream_restarts_with_backoff() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let (_dir, instance) = sqlite().await;
    // `int('broken')` raises inside `sync()`: v4 turns it into a failed `SuccessTuple`.
    let p = pipe("plugin:wk_streamer", "events", Some("broken"));
    let (stream, mut events) = PythonStream::start(
        pool,
        "plugin:wk_streamer",
        &p,
        &Map::new(),
        StreamMode::V4Sync,
        Arc::new(InstanceSyncHandler::single(instance)),
        StreamOptions {
            backoff_initial: Duration::from_millis(50),
            backoff_max: Duration::from_millis(100),
            max_restarts: Some(1),
            ..Default::default()
        },
    );
    let mut seen = Vec::new();
    loop {
        let e = next_event(&mut events).await;
        let stop = e == StreamEvent::Stopped;
        seen.push(e);
        if stop {
            break;
        }
    }
    let starts = seen
        .iter()
        .filter(|e| matches!(e, StreamEvent::Started { .. }))
        .count();
    assert_eq!(starts, 2, "{seen:?}");
    assert!(seen.iter().any(|e| matches!(
        e,
        StreamEvent::Returned(st) if !st.success && st.message.starts_with("Failed to sync")
    )));
    assert!(seen.contains(&StreamEvent::Restarting {
        attempt: 1,
        delay: Duration::from_millis(50)
    }));
    assert!(
        !stream.is_running()
            || tokio::time::timeout(Duration::from_secs(5), stream.stop())
                .await
                .is_ok()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_sink_pushes_batches() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let (_dir, instance) = sqlite().await;
    let handler = Arc::new(Recorder {
        inner: InstanceSyncHandler::single(Arc::clone(&instance)),
        seen: Mutex::new(Vec::new()),
    });
    let p = pipe("wkstream:main", "pushes", None);
    let (stream, mut events) = PythonStream::start(
        pool,
        "wkstream:main",
        &p,
        &Map::new(),
        StreamMode::Subscribe,
        handler.clone(),
        StreamOptions {
            restart: false,
            ..Default::default()
        },
    );
    assert!(matches!(
        next_event(&mut events).await,
        StreamEvent::Started { .. }
    ));
    assert_eq!(
        next_event(&mut events).await,
        StreamEvent::Returned(SuccessTuple::ok("pushed 3 batches"))
    );
    assert_eq!(rowcount(&instance, &p).await, 3);
    stream.stop().await;
    let seen = handler.seen.lock().unwrap().clone();
    assert_eq!(seen, vec![("push".to_string(), 1); 3]);
}
