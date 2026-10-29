//! The MCP `register_pipe` tool is `Pipe.register()` (v4 `mcp/_tools.py`): a plugin connector's
//! `register(pipe)` parameters (asked through `McpBackend::connector_register`) are the base and
//! the parameters given to the tool win. A backend that cannot ask (no Python worker) registers
//! the pipe as given.

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_server::mcp::stdio::serve_lines;
use meerschaum_server::mcp::{Caller, ConnectorRegister, McpBackend, McpConfig, McpServer};
use meerschaum_server::mcp::{NoBackend, SqlReadError};
use serde_json::{Value as Json, json};

struct Hook(ConnectorRegister);

#[async_trait]
impl McpBackend for Hook {
    async fn sync_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn verify_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn read_sql(&self, _: &str, _: &str, _: usize) -> Result<Vec<Value>, SqlReadError> {
        Err(SqlReadError::Unsupported)
    }
    async fn connector_register(&self, _: &Pipe) -> ConnectorRegister {
        self.0.clone()
    }
}

fn params(value: Json) -> Map {
    match serde_json::from_value::<Value>(value).expect("value") {
        Value::Map(m) => m,
        other => panic!("not a map: {other:?}"),
    }
}

/// Call `register_pipe` with `arguments` and return what the instance registered.
async fn register(
    backend: Arc<dyn McpBackend>,
    arguments: Json,
) -> (Json, Option<serde_json::Value>) {
    let server = McpServer::new(McpConfig::default()).with_backend(backend);
    let dir = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(dir.path()));
    let request = json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "register_pipe", "arguments": arguments},
    });
    let mut out = Vec::new();
    serve_lines(
        &server,
        &state,
        &Caller::stdio(),
        tokio::io::BufReader::new(format!("{request}\n").as_bytes()),
        &mut out,
    )
    .await
    .unwrap();
    let reply: Json = serde_json::from_slice(&out).unwrap();
    let pipe = Pipe::builder(
        arguments["connector_keys"].as_str().unwrap(),
        arguments["metric_key"].as_str().unwrap(),
    )
    .build()
    .unwrap();
    let attributes = instance.get_pipe_attributes(&pipe).await.unwrap();
    let parameters = attributes
        .get("parameters")
        .map(|v| serde_json::to_value(v).unwrap());
    (reply, parameters)
}

#[tokio::test]
async fn the_plugins_parameters_sit_under_the_given_ones() {
    let hook = Hook(ConnectorRegister::Parameters(params(json!({
        "columns": {"datetime": "hook_dt", "id": "id"},
        "dtypes": {"val": "float"},
        "tags": ["from_plugin"],
    }))));
    let (reply, parameters) = register(
        Arc::new(hook),
        json!({
            "connector_keys": "plugin:w17b", "metric_key": "demo",
            "columns": {"datetime": "given_dt"}, "tags": ["given"],
        }),
    )
    .await;
    assert!(reply["result"].to_string().contains("true"), "{reply}");
    assert_eq!(
        parameters.unwrap(),
        json!({
            "columns": {"datetime": "given_dt", "id": "id"},
            "dtypes": {"val": "float"},
            "tags": ["given"],
        })
    );
}

#[tokio::test]
async fn the_plugins_parameters_alone_register_the_pipe() {
    let hook = Hook(ConnectorRegister::Parameters(params(json!({
        "columns": {"datetime": "hook_dt"},
    }))));
    let (_, parameters) = register(
        Arc::new(hook),
        json!({"connector_keys": "plugin:w17b", "metric_key": "demo"}),
    )
    .await;
    assert_eq!(
        parameters.unwrap(),
        json!({"columns": {"datetime": "hook_dt"}})
    );
}

#[tokio::test]
async fn an_invalid_or_undefined_hook_registers_the_default_columns() {
    for backend in [
        Arc::new(Hook(ConnectorRegister::Invalid("[1]".into()))) as Arc<dyn McpBackend>,
        Arc::new(Hook(ConnectorRegister::Undefined)),
        Arc::new(NoBackend),
    ] {
        let (_, parameters) = register(
            backend,
            json!({"connector_keys": "plugin:w17b", "metric_key": "demo"}),
        )
        .await;
        assert_eq!(
            parameters.unwrap(),
            json!({"columns": {"datetime": null, "id": null}})
        );
    }
}
