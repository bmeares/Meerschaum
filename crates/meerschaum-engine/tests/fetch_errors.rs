//! Source failures must remain actionable without a tracing subscriber (embedded CLI and
//! Python callers). An absent fetch result still has the existing generic message.

mod common;

use std::sync::Arc;

use common::*;
use meerschaum_connector::{
    ChunkedFetch, Connector, ConnectorError, ConnectorResult, DataQuery, async_trait,
};
use meerschaum_core::Pipe;
use meerschaum_engine::{Engine, StaticRegistry, SyncArgs};
use meerschaum_kernel::Frame;
use serde_json::json;

#[derive(Clone, Copy)]
enum Outcome {
    FetchError,
    ChunkedError,
    NotImplemented,
    Empty,
}

struct Source(Outcome);

#[async_trait]
impl Connector for Source {
    fn connector_type(&self) -> &str {
        "plugin"
    }

    fn label(&self) -> &str {
        "fetch_error"
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }

    async fn fetch(&self, _pipe: &Pipe, _query: &DataQuery) -> ConnectorResult<Option<Frame>> {
        match self.0 {
            Outcome::FetchError => Err(ConnectorError::Backend("Invalid example API key.".into())),
            Outcome::NotImplemented => Err(ConnectorError::NotImplemented(
                "This source requires a fetch implementation.".into(),
            )),
            Outcome::Empty => Ok(None),
            Outcome::ChunkedError => panic!("a chunked fetch error must stop the sync"),
        }
    }

    async fn fetch_chunked(
        &self,
        _pipe: &Pipe,
        _query: &DataQuery,
        _sync_chunks: bool,
    ) -> ConnectorResult<Option<ChunkedFetch>> {
        if matches!(self.0, Outcome::ChunkedError) {
            Err(ConnectorError::Backend(
                "Example stream is unavailable.".into(),
            ))
        } else {
            Ok(None)
        }
    }
}

fn engine(env: &Env, outcome: Outcome) -> Engine {
    let source: Arc<dyn Connector> = Arc::new(Source(outcome));
    let registry =
        Arc::new(StaticRegistry::clone(&env.registry).with_source("plugin:fetch_error", source));
    Engine::new(registry, env.config())
}

fn pipe(parameters: serde_json::Value) -> Pipe {
    Pipe::builder("plugin:fetch_error", "readings")
        .instance(INSTANCE)
        .parameters(json_map(&parameters))
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_errors_return_the_source_and_reason() {
    for (outcome, reason) in [
        (Outcome::FetchError, "Invalid example API key."),
        (Outcome::ChunkedError, "Example stream is unavailable."),
        (
            Outcome::NotImplemented,
            "This source requires a fetch implementation.",
        ),
    ] {
        let env = env("fetch_error").await;
        let pipe = pipe(json!({}));
        let result = engine(&env, outcome)
            .sync_pipe(&pipe, None, &SyncArgs::default())
            .await;
        assert!(!result.success);
        assert_eq!(
            result.message,
            format!(
                "No data were fetched for {pipe}.\nFailed to fetch data from plugin:fetch_error:\n    {reason}"
            ),
        );
        assert!(
            env.table_of("plugin:fetch_error", "readings")
                .await
                .rows
                .is_empty()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn determining_fetch_bounds_preserves_its_error() {
    let env = env("fetch_begin_error").await;
    env.exec("CREATE TABLE fetch_begin_error (ts TEXT)").await;
    env.exec("INSERT INTO fetch_begin_error VALUES ('2026-01-01 00:00:00')")
        .await;
    let pipe = pipe(json!({
        "target": "fetch_begin_error",
        "columns": {"datetime": "ts"},
        "dtypes": {"ts": "datetime"},
        "fetch": {"backtrack_minutes": "invalid"},
    }));
    let result = engine(&env, Outcome::FetchError)
        .sync_pipe(&pipe, None, &SyncArgs::default())
        .await;
    assert!(!result.success);
    assert!(
        result
            .message
            .starts_with(&format!("No data were fetched for {pipe}.\n"))
    );
    assert!(
        result
            .message
            .contains("Failed to fetch data from plugin:fetch_error:")
    );
    assert!(
        result
            .message
            .contains("unsupported type for timedelta component: str"),
        "{}",
        result.message
    );
    assert!(!result.message.contains("Invalid example API key."));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_absent_fetch_result_keeps_the_generic_message() {
    let env = env("fetch_none").await;
    let pipe = pipe(json!({}));
    let result = engine(&env, Outcome::Empty)
        .sync_pipe(&pipe, None, &SyncArgs::default())
        .await;
    assert!(!result.success);
    assert_eq!(result.message, format!("No data were fetched for {pipe}."));
}
