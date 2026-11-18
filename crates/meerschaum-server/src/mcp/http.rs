//! The Streamable HTTP transport at `/mcp` (`meerschaum/api/routes/_mcp.py`).
//!
//! `POST /mcp` takes a JSON-RPC message or batch with the caller's `CurrentScopes`; a payload of
//! only notifications gets `202` with no body. `GET /mcp` is v4's convenience summary of what the
//! caller's token can reach (not part of the MCP specification).

use std::sync::Arc;

use axum::body::Body;
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
use crate::access::Access;
use crate::executor::plugin_caller::RequestSource;
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

struct HttpCaller {
    access: Arc<Access>,
    source: Option<Arc<RequestSource>>,
}
fn projected_caller(state: &AppState, access: &Access) -> Caller {
    let mut caller = Caller::api(access.scopes(), access.auth.clone());
    caller.plugins_hidden = crate::routes::extensions::requires_confined(state, access);
    if !access.full_admin() {
        caller.team_principal = Some(access.principal.clone());
    }
    caller
}
impl HttpCaller {
    async fn fresh(
        &self,
        state: &AppState,
    ) -> Result<(Caller, Arc<Access>), crate::error::ApiError> {
        let access = match &self.source {
            Some(source) => source.refresh(state, &[]).await?,
            None => self.access.clone(),
        };
        Ok((projected_caller(state, &access), access))
    }
}
async fn caller(state: &AppState, headers: &HeaderMap) -> Result<HttpCaller, Box<Response>> {
    let source = if state.config.no_auth {
        None
    } else {
        Some(Arc::new(
            RequestSource::resolve(state, headers, &[])
                .await
                .map_err(|error| Box::new(error.into_response()))?,
        ))
    };
    let access = source
        .as_ref()
        .map(|source| source.access())
        .unwrap_or_else(|| Arc::new(Access::everything()));
    Ok(HttpCaller { access, source })
}

async fn post_mcp(
    State(state): State<AppState>,
    Extension(server): Extension<Arc<McpServer>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let original = match caller(&state, &headers).await {
        Ok(c) => c,
        Err(r) => return *r,
    };
    // Read after authentication, within `api:limits:body_bytes`.
    let body = match crate::routes::body_bytes(body, state.config.limits.body).await {
        Ok(body) => body,
        Err(error) => return error.into_response(),
    };
    let Ok(payload) = serde_json::from_slice::<Value>(&body) else {
        return json(
            StatusCode::BAD_REQUEST,
            &error(Value::Null, PARSE_ERROR, "Parse error"),
        );
    };
    let (caller, _) = match original.fresh(&state).await {
        Ok(fresh) => fresh,
        Err(error) => return error.into_response(),
    };
    let (server, caller, catalog) =
        match super::extensions::scoped(&state, server, caller, original.source.clone()).await {
            Ok(scoped) => scoped,
            Err(error) => return error.into_response(),
        };
    let (mut fresh, access) = match original.fresh(&state).await {
        Ok(fresh) => fresh,
        Err(error) => return error.into_response(),
    };
    fresh.plugins_hidden = caller.plugins_hidden;
    let result = crate::executor::context::request_headers_scope(
        headers,
        crate::executor::context::scope(
            Some(access),
            Box::pin(server.handle_payload(&state, payload, &fresh)),
        ),
    )
    .await;
    let (_, current) = match original.fresh(&state).await {
        Ok(fresh) => fresh,
        Err(error) => return error.into_response(),
    };
    if let Some(catalog) = catalog
        && let Err(error) = crate::routes::extensions::confirm_catalog_source(&catalog, &current)
    {
        return error.into_response();
    }
    match result {
        None => StatusCode::ACCEPTED.into_response(),
        Some(result) => json(StatusCode::OK, &result),
    }
}

async fn get_mcp(
    State(state): State<AppState>,
    Extension(server): Extension<Arc<McpServer>>,
    headers: HeaderMap,
) -> Response {
    let original = match caller(&state, &headers).await {
        Ok(c) => c,
        Err(r) => return *r,
    };
    let (caller, _) = match original.fresh(&state).await {
        Ok(fresh) => fresh,
        Err(error) => return error.into_response(),
    };
    let (server, caller, catalog) =
        match super::extensions::scoped(&state, server, caller, original.source.clone()).await {
            Ok(scoped) => scoped,
            Err(error) => return error.into_response(),
        };
    let tools: Vec<Value> = server
        .visible_tools(&state, &caller)
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
    let (_, current) = match original.fresh(&state).await {
        Ok(fresh) => fresh,
        Err(error) => return error.into_response(),
    };
    if let Some(catalog) = catalog
        && let Err(error) = crate::routes::extensions::confirm_catalog_source(&catalog, &current)
    {
        return error.into_response();
    }
    let b = builtin();
    json(
        StatusCode::OK,
        &Value::Map(map_of([
            ("protocolVersion", s(b.protocol_version.clone())),
            ("serverInfo", McpServer::server_info(&state)),
            ("capabilities", McpServer::capabilities()),
            ("readOnly", Value::Bool(server.config.read_only)),
            ("instructions", s(super::registry::primer().to_string())),
            ("tools", Value::List(tools)),
            ("resources", Value::List(resources)),
            ("prompts", Value::List(prompts)),
        ])),
    )
}
