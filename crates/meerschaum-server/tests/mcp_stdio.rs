//! The MCP stdio transport (`mrsm start mcp`, `meerschaum/mcp/_stdio.py`): newline-delimited
//! JSON-RPC with every scope and no API permission checks, `json.dumps` formatting, a parse
//! error reported and the stream kept open, notifications answered with nothing.

mod common;

use std::sync::Arc;

use meerschaum_server::mcp::McpServer;
use meerschaum_server::mcp::stdio::serve_lines;
use meerschaum_server::mcp::{Caller, McpConfig};
use serde_json::{Value as Json, json};

async fn run(server: &McpServer, input: &str) -> Vec<String> {
    let dir = tempfile::tempdir().unwrap();
    let (state, _) = common::state_with(common::config(dir.path()));
    let mut out = Vec::new();
    let res = serve_lines(
        server,
        &state,
        &Caller::stdio(),
        tokio::io::BufReader::new(input.as_bytes()),
        &mut out,
    )
    .await
    .unwrap();
    assert!(res.success);
    String::from_utf8(out)
        .unwrap()
        .lines()
        .map(String::from)
        .collect()
}

#[tokio::test]
async fn stdio_round_trip() {
    let server = McpServer::new(McpConfig::default());
    let input = [
        r#"{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}}"#,
        "",
        r#"{"jsonrpc": "2.0", "method": "notifications/initialized"}"#,
        "{not json",
        r#"{"jsonrpc": "2.0", "id": 2, "method": "tools/list"}"#,
        r#"{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "register_pipe", "arguments": {"connector_keys": "demo", "metric_key": "temperature"}}}"#,
        r#"[{"jsonrpc": "2.0", "id": 4, "method": "ping"}]"#,
    ]
    .join("\n");
    let lines = run(&server, &input).await;
    assert_eq!(lines.len(), 5, "{lines:#?}");

    // `json.dumps(message, default=str)`: Python's default separators.
    assert!(
        lines[0].starts_with(
            r#"{"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "2025-06-18""#
        ),
        "{}",
        lines[0]
    );
    let parse: Json = serde_json::from_str(&lines[1]).unwrap();
    assert_eq!(
        parse,
        json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "Parse error"}})
    );

    // Every scope: the write and admin tools are listed.
    let list: Json = serde_json::from_str(&lines[2]).unwrap();
    let names: Vec<&str> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    for t in ["delete_pipe", "execute_action", "read_sql", "register_pipe"] {
        assert!(names.contains(&t), "{t} in {names:?}");
    }

    let call: Json = serde_json::from_str(&lines[3]).unwrap();
    assert_eq!(call["result"]["isError"], json!(false), "{call}");
    let batch: Json = serde_json::from_str(&lines[4]).unwrap();
    assert_eq!(batch, json!([{"jsonrpc": "2.0", "id": 4, "result": {}}]));
}

#[tokio::test]
async fn stdio_read_only_mode() {
    let server = McpServer::new(McpConfig {
        read_only: true,
        ..McpConfig::default()
    });
    let server = Arc::new(server);
    let lines = run(
        &server,
        r#"{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "drop_pipe", "arguments": {"connector_keys": "a", "metric_key": "b"}}}"#,
    )
    .await;
    let r: Json = serde_json::from_str(&lines[0]).unwrap();
    assert_eq!(
        r["result"]["content"][0]["text"],
        "'drop_pipe' modifies data, and this MCP server is running in read-only mode."
    );
}
