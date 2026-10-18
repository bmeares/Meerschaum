//! A pipe the engine finds unregistered is registered the way `Pipe.register()` does: the
//! connector's `register(pipe)` parameters are the base and the pipe's own parameters win
//! (`ConnectorRegistry::register_hook`).

mod common;

use std::sync::Arc;

use common::*;
use meerschaum_connector::{Connector, InstanceConnector, async_trait};
use meerschaum_core::Pipe;
use meerschaum_engine::{
    ConnectorRegister, ConnectorRegisterHook, ConnectorRegistry, Engine, SyncArgs,
};
use serde_json::json;

struct Hook(ConnectorRegister);

#[async_trait]
impl ConnectorRegisterHook for Hook {
    async fn register_parameters(&self, _pipe: &Pipe) -> ConnectorRegister {
        self.0.clone()
    }
}

struct WithHook {
    inner: Arc<dyn ConnectorRegistry>,
    answer: ConnectorRegister,
}

impl ConnectorRegistry for WithHook {
    fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        self.inner.instance(keys)
    }
    fn source(&self, keys: &str) -> Result<Option<Arc<dyn Connector>>, String> {
        self.inner.source(keys)
    }
    fn register_hook(&self, _keys: &str) -> Option<Arc<dyn ConnectorRegisterHook>> {
        Some(Arc::new(Hook(self.answer.clone())))
    }
    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        self.inner.table_reference(pipe)
    }
}

async fn sync_with(tag: &str, answer: ConnectorRegister) -> serde_json::Value {
    let env = env(tag).await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)")
        .await;
    let registry = Arc::new(WithHook {
        inner: env.registry.clone(),
        answer,
    });
    let engine = Engine::new(registry, env.config());
    // Not registered: only the call-site parameters exist.
    let pipe = env.pipe(
        "hooked",
        &json!({
            "sql": "SELECT ts, id, v FROM src",
            "columns": {"datetime": "ts"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }),
    );
    let res = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(res.success, "{}", res.message);
    let attributes = env
        .conn
        .get_pipe_attributes(&pipe)
        .await
        .expect("the pipe was registered");
    serde_json::to_value(attributes.get("parameters").expect("parameters")).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_hooks_parameters_sit_under_the_pipes_own() {
    let parameters = sync_with(
        "hook_params",
        ConnectorRegister::Parameters(json_map(&json!({
            "columns": {"datetime": "hook_dt", "id": "id"},
            "tags": ["from_hook"],
            "fetch": {"x": 1},
        }))),
    )
    .await;
    assert_eq!(parameters["tags"], json!(["from_hook"]));
    assert_eq!(parameters["fetch"], json!({"x": 1}));
    // The pipe's own `datetime` wins; the hook adds `id`.
    assert_eq!(parameters["columns"], json!({"datetime": "ts", "id": "id"}));
    assert_eq!(parameters["sql"], json!("SELECT ts, id, v FROM src"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_invalid_or_undefined_hook_changes_nothing() {
    for (tag, answer) in [
        ("hook_invalid", ConnectorRegister::Invalid("[1]".into())),
        ("hook_undefined", ConnectorRegister::Undefined),
    ] {
        let parameters = sync_with(tag, answer).await;
        assert!(parameters.get("tags").is_none(), "{parameters}");
        assert_eq!(parameters["columns"], json!({"datetime": "ts"}));
    }
}
