//! The stdio transport (`meerschaum/mcp/_stdio.py`, `mrsm start mcp`): newline-delimited
//! JSON-RPC on stdin and stdout, with every scope and no API permission checks (the trust model
//! of the shell that launched it).
//!
//! stdout is the protocol channel: the caller must route logs to stderr. Responses are written as
//! `json.dumps(message, default=str)` plus a newline, flushed per message.

use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::{SuccessTuple, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use super::protocol::{INTERNAL_ERROR, PARSE_ERROR, error};
use super::{Caller, McpServer};
use crate::state::AppState;

async fn write_message<W: AsyncWrite + Unpin>(out: &mut W, message: &Value) -> std::io::Result<()> {
    let mut line = json_dumps(message, JsonStyle::Default, true);
    line.push('\n');
    out.write_all(line.as_bytes()).await?;
    out.flush().await
}

/// `serve_stdio(stdin, stdout, scopes)`: serve until the input closes.
pub async fn serve_lines<R, W>(
    server: &McpServer,
    state: &AppState,
    caller: &Caller,
    input: R,
    mut output: W,
) -> std::io::Result<SuccessTuple>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = input.lines();
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(payload) = serde_json::from_str::<Value>(line) else {
            write_message(&mut output, &error(Value::Null, PARSE_ERROR, "Parse error")).await?;
            continue;
        };
        let id = payload.get("id").cloned().unwrap_or_default();
        // A panic in dispatch must not kill the server: report it and keep reading.
        let handled = std::panic::AssertUnwindSafe(server.handle_payload(state, payload, caller));
        let response = match futures_util::FutureExt::catch_unwind(handled).await {
            Ok(r) => r,
            Err(_) => Some(error(
                id,
                INTERNAL_ERROR,
                "Exception: the request could not be handled.",
            )),
        };
        if let Some(r) = response {
            write_message(&mut output, &r).await?;
        }
    }
    Ok(SuccessTuple::success())
}

/// Serve MCP on the process's stdin and stdout (`mrsm start mcp`).
pub async fn serve_stdio(server: &McpServer, state: &AppState) -> SuccessTuple {
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    match serve_lines(server, state, &Caller::stdio(), stdin, stdout).await {
        Ok(r) => r,
        Err(e) => SuccessTuple::fail(format!("The MCP stdio transport failed: {e}")),
    }
}
