//! The Streamable HTTP transport at `/mcp` (`meerschaum/api/routes/_mcp.py`).
//!
//! `POST /mcp` takes a JSON-RPC message or batch with the caller's `CurrentScopes`; a payload of
//! only notifications gets `202` with no body. `GET /mcp` is v4's convenience summary of what the
//! caller's token can reach (not part of the MCP specification).

use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Extension, Router};
use meerschaum_core::Value;
use meerschaum_core::pyfmt::{JsonStyle, json_dumps};

use super::protocol::{PARSE_ERROR, error, map_of, s};
use super::registry::{Prompt, Resource, Tool, builtin};
use super::{Caller, McpServer};
use crate::auth::current_scopes;
use crate::state::AppState;

/// The `/mcp` endpoint (`STATIC_CONFIG['api']['endpoints']['mcp']`).
pub const ENDPOINT: &str = "/mcp";

/// FastAPI's `JSONResponse` rendering of a [`Value`].
fn json(status: StatusCode, v: &Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(json_dumps(v, JsonStyle::Compact, false)),
    )
        .into_response()
}

/// Register `POST`/`GET /mcp`.
pub fn routes(app: Router<AppState>, server: Arc<McpServer>) -> Router<AppState> {
    app.route(
        ENDPOINT,
        post(post_mcp).get(get_mcp).layer(Extension(server)),
    )
}

async fn caller(state: &AppState, headers: &HeaderMap) -> Result<Caller, Box<Response>> {
    let (principal, scopes) = current_scopes(state, headers)
        .await
        .map_err(|e| Box::new(e.into_response()))?;
    Ok(Caller::api(scopes, principal))
}

async fn post_mcp(
    State(state): State<AppState>,
    Extension(server): Extension<Arc<McpServer>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let caller = match caller(&state, &headers).await {
        Ok(c) => c,
        Err(r) => return *r,
    };
    let Ok(payload) = serde_json::from_slice::<Value>(&body) else {
        return json(
            StatusCode::BAD_REQUEST,
            &error(Value::Null, PARSE_ERROR, "Parse error"),
        );
    };
    match server.handle_payload(&state, payload, &caller).await {
        // Notifications get no body.
        None => StatusCode::ACCEPTED.into_response(),
        Some(r) => json(StatusCode::OK, &r),
    }
}

async fn get_mcp(
    State(state): State<AppState>,
    Extension(server): Extension<Arc<McpServer>>,
    headers: HeaderMap,
) -> Response {
    let caller = match caller(&state, &headers).await {
        Ok(c) => c,
        Err(r) => return *r,
    };
    let tools: Vec<Value> = server
        .visible_tools(&caller)
        .await
        .iter()
        .map(Tool::to_dict)
        .collect();
    let resources: Vec<Value> = server
        .visible_resources(&caller)
        .await
        .iter()
        .map(Resource::to_dict)
        .collect();
    let prompts: Vec<Value> = server
        .visible_prompts(&caller)
        .await
        .iter()
        .map(Prompt::to_dict)
        .collect();
    let b = builtin();
    json(
        StatusCode::OK,
        &Value::Map(map_of([
            ("protocolVersion", s(b.protocol_version.clone())),
            ("serverInfo", McpServer::server_info(&state)),
            ("capabilities", McpServer::capabilities()),
            ("readOnly", Value::Bool(server.config.read_only)),
            ("instructions", s(b.primer.clone())),
            ("tools", Value::List(tools)),
            ("resources", Value::List(resources)),
            ("prompts", Value::List(prompts)),
        ])),
    )
}
