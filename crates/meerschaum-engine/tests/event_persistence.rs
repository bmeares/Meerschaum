//! Run completion must not overtake its buffered event history.
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::*;
use indexmap::IndexMap;
use meerschaum_connector::{
    Connector, ConnectorResult, DataQuery, DateBound, InstanceConnector, PipeId, PipesKeys,
    PipesKeysQuery, SyncOptions, async_trait,
};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::{Map, Pipe, SuccessTuple};
use meerschaum_engine::{Engine, EngineReader, KeyFilter, RunOptions, Selection, StaticRegistry};
use meerschaum_kernel::Frame;
use serde_json::json;
use tokio::sync::Notify;

struct EventGate {
    inner: Arc<SqlConnector>,
    fail_writes: bool,
    enabled: AtomicBool,
    entered: Notify,
    release: Notify,
}

#[async_trait]
impl Connector for EventGate {
    fn connector_type(&self) -> &str {
        "sql"
    }
    fn label(&self) -> &str {
        "eng"
    }
    async fn test_connection(&self) -> ConnectorResult<bool> {
        Connector::test_connection(self.inner.as_ref()).await
    }
}

#[async_trait]
impl InstanceConnector for EventGate {
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        InstanceConnector::register_pipe(self.inner.as_ref(), pipe).await
    }
    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        InstanceConnector::get_pipe_attributes(self.inner.as_ref(), pipe).await
    }
    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        InstanceConnector::get_pipe_id(self.inner.as_ref(), pipe).await
    }
    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        InstanceConnector::fetch_pipes_keys(self.inner.as_ref(), query).await
    }
    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        InstanceConnector::pipe_exists(self.inner.as_ref(), pipe).await
    }
    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        InstanceConnector::drop_pipe(self.inner.as_ref(), pipe).await
    }
    async fn sync_pipe(&self, pipe: &Pipe, frame: Frame, options: &SyncOptions) -> SuccessTuple {
        if pipe.metric_key() == "engine_events" && self.fail_writes {
            return SuccessTuple::fail("controlled event write failure");
        }
        if pipe.metric_key() == "engine_events" && self.enabled.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        InstanceConnector::sync_pipe(self.inner.as_ref(), pipe, frame, options).await
    }
    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        InstanceConnector::get_pipe_data(self.inner.as_ref(), pipe, query).await
    }
    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        InstanceConnector::get_sync_time(self.inner.as_ref(), pipe, params, newest).await
    }
    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        InstanceConnector::get_pipe_rowcount(self.inner.as_ref(), pipe, query).await
    }
    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        InstanceConnector::get_pipe_columns_types(self.inner.as_ref(), pipe).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_run_is_visible_only_after_its_events_are_persisted() {
    let env = env("event_completion_order").await;
    env.register(
        "one",
        &json!({"sql": "SELECT 1 AS id", "columns": {"id": "id"}}),
    )
    .await;
    let gate = Arc::new(EventGate {
        inner: env.conn.clone(),
        fail_writes: false,
        enabled: AtomicBool::new(true),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let registry = Arc::new(
        StaticRegistry::new()
            .with_source(INSTANCE, env.conn.clone())
            .with_instance(INSTANCE, gate.clone()),
    );
    let mut config = env.config();
    config.state_flush = Duration::from_secs(60);
    let reader = EngineReader::new(registry.clone(), config.clone());
    let engine = Arc::new(Engine::new(registry, config));
    let running = engine.clone();
    let task = tokio::spawn(async move {
        running
            .run(RunOptions {
                run_id: Some("gated-events".into()),
                selection: Some(Selection::filter(KeyFilter {
                    metric_keys: vec!["one".into()],
                    ..Default::default()
                })),
                ..Default::default()
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), gate.entered.notified())
        .await
        .unwrap();
    let during = reader.run_detail("gated-events").await;
    gate.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.success, "{}", result.message);
    let during = during.unwrap().unwrap();
    assert!(
        during.summary.run.finished.is_none(),
        "run was marked finished while its event write was blocked"
    );
    let finished = reader.run_detail("gated-events").await.unwrap().unwrap();
    assert!(finished.summary.run.finished.is_some());
    let events = reader.events_after("gated-events", None).await.unwrap();
    assert_eq!(events.first().unwrap().event, "run_started");
    assert_eq!(events.last().unwrap().event, "run_finished");
    assert!(events.windows(2).all(|pair| pair[0].seq < pair[1].seq));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_event_flush_is_reported_in_the_finished_run() {
    let env = env("event_flush_failure").await;
    env.register(
        "one",
        &json!({"sql": "SELECT 1 AS id", "columns": {"id": "id"}}),
    )
    .await;
    let gate = Arc::new(EventGate {
        inner: env.conn.clone(),
        fail_writes: true,
        enabled: AtomicBool::new(false),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let registry = Arc::new(
        StaticRegistry::new()
            .with_source(INSTANCE, env.conn.clone())
            .with_instance(INSTANCE, gate),
    );
    let mut config = env.config();
    config.state_flush = Duration::from_secs(60);
    let reader = EngineReader::new(registry.clone(), config.clone());
    let engine = Engine::new(registry, config);
    let result = engine
        .run(RunOptions {
            selection: Some(Selection::filter(KeyFilter {
                metric_keys: vec!["one".into()],
                ..Default::default()
            })),
            ..Default::default()
        })
        .await;
    assert!(result.success, "{}", result.message);
    assert!(
        result
            .warnings
            .iter()
            .any(|warning| warning.contains("controlled event write failure")),
        "event persistence failed without a warning: {:?}",
        result.warnings
    );
    let detail = reader
        .run_detail(result.run_id().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(
        detail
            .summary
            .run
            .message
            .unwrap()
            .contains("controlled event write failure")
    );
    assert_eq!(env.rowcount("one").await, 1);
}
