//! A list placeholder is not fresh registration metadata after a deletion or malformed read.

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use common::{SECRET, req, send};
use indexmap::IndexMap;
use meerschaum_connector::{
    Connector, ConnectorResult, DataQuery, DateBound, InstanceConnector, PipeId, PipesKeys,
    PipesKeysQuery, SyncOptions, pipe_ops,
};
use meerschaum_connector_valkey::{MemoryBackend, ValkeyBackend, ValkeyConnector, keys};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::{
    Frame,
    enforce::{PipeInput, pipe_enforce_dtypes},
    value::Value as KValue,
};
use meerschaum_server::{AppState, ServerConfig};

struct MetadataRace {
    inner: Arc<ValkeyConnector>,
    backend: Arc<MemoryBackend>,
    parameters: Option<Value>,
}

#[async_trait]
impl Connector for MetadataRace {
    fn connector_type(&self) -> &str {
        "valkey"
    }
    fn label(&self) -> &str {
        "list_race"
    }
    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }
}

#[async_trait]
impl InstanceConnector for MetadataRace {
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.inner.register_pipe(pipe).await
    }
    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        if let Some(parameters) = &self.parameters {
            // Custom backends may explicitly provide valid empty parameters without IDs.
            return Ok([("parameters".into(), parameters.clone())]
                .into_iter()
                .collect());
        }
        // Simulate metadata disappearing after the list query, while its row and data remain.
        self.backend
            .del(&[keys::get_pipe_key(
                pipe.connector_keys(),
                pipe.metric_key(),
                pipe.location_key(),
            )])
            .await
            .unwrap();
        self.inner.get_pipe_attributes(pipe).await
    }
    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        InstanceConnector::get_pipe_id(self.inner.as_ref(), pipe).await
    }
    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        InstanceConnector::fetch_pipes_keys(self.inner.as_ref(), query).await
    }
    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        self.inner.pipe_exists(pipe).await
    }
    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.inner.drop_pipe(pipe).await
    }
    async fn sync_pipe(&self, pipe: &Pipe, frame: Frame, options: &SyncOptions) -> SuccessTuple {
        self.inner.sync_pipe(pipe, frame, options).await
    }
    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        self.inner.get_pipe_data(pipe, query).await
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
        self.inner.get_pipe_rowcount(pipe, query).await
    }
    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        self.inner.get_pipe_columns_types(pipe).await
    }
}

async fn registration() -> (Arc<ValkeyConnector>, Arc<MemoryBackend>, Pipe) {
    let backend = Arc::new(MemoryBackend::new());
    let inner = Arc::new(ValkeyConnector::with_backend(
        "list_race",
        Map::new(),
        backend.clone(),
    ));
    let parameters: Value = serde_json::from_value(serde_json::json!({
        "columns":{"id":"id"}, "access":{"owner":"owner", "visibility":"private"}
    }))
    .unwrap();
    let pipe = Pipe::builder("fixture", "private")
        .instance("valkey:list_race")
        .parameters(parameters.to_map())
        .build()
        .unwrap();
    assert!(inner.register_pipe(&pipe).await.success);
    (inner, backend, pipe)
}

#[tokio::test]
async fn disappearing_list_metadata_cannot_register_a_retained_private_target_as_public() {
    let (inner, backend, pipe) = registration().await;
    let context = pipe_ops::dtypes_context(inner.as_ref(), &pipe, pipe.parameters())
        .await
        .unwrap();
    let frame = pipe_enforce_dtypes(
        PipeInput::Docs(vec![
            [
                ("id".into(), KValue::Int(1)),
                (
                    "secret".into(),
                    KValue::Str("retained-private-value".into()),
                ),
            ]
            .into_iter()
            .collect(),
        ]),
        &context,
    )
    .unwrap();
    assert!(
        inner
            .sync_pipe(&pipe, frame, &SyncOptions::default())
            .await
            .success
    );
    let raced = Arc::new(MetadataRace {
        inner: inner.clone(),
        backend,
        parameters: None,
    });
    let dir = tempfile::tempdir().unwrap();
    let mut config = ServerConfig::for_root("valkey:list_race", dir.path());
    config.no_auth = true;
    config.no_dash = true;
    config.no_webterm = true;
    let state = AppState::builder(config, raced)
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    assert!(state.pipes(None, true).await.unwrap().is_empty());
    assert!(!state.is_registered(&pipe, None).await.unwrap());
    assert!(
        inner.pipe_exists(&pipe).await.unwrap(),
        "the physical data remains"
    );
    let response = send(
        &meerschaum_server::app(state),
        req("GET", "/pipes/fixture/private/None/data", None, None),
    )
    .await;
    assert_eq!(response.status, StatusCode::CONFLICT, "{}", response.text());
    assert!(!response.text().contains("retained-private-value"));
}

#[tokio::test]
async fn fresh_valid_empty_parameters_survive_and_invalid_parameter_documents_are_skipped() {
    let (inner, backend, _) = registration().await;
    for (parameters, expected) in [
        (Value::Map(Map::new()), 1),
        (Value::Str("{}".into()), 1),
        (Value::Null, 0),
        (Value::Str("[]".into()), 0),
        (Value::Str("broken JSON".into()), 0),
        (Value::List(Vec::new()), 0),
    ] {
        let connector = MetadataRace {
            inner: inner.clone(),
            backend: backend.clone(),
            parameters: Some(parameters),
        };
        let entries = connector
            .registered_pipes(&PipesKeysQuery::default())
            .await
            .unwrap();
        assert_eq!(entries.len(), expected);
        if let Some((identity, entry)) = entries.first() {
            assert!(identity.is_none(), "custom metadata need not invent an ID");
            assert_eq!(entry.extra, Some(Value::Map(Map::new())));
        }
    }
}
