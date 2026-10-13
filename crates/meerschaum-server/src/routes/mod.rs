//! The HTTP routes (`meerschaum/api/routes/*`), grouped by the route groups that
//! `api:permissions:routes:allowlist` restricts (`api/routes/__init__.py`).
//!
//! `/login` is always served. Unmatched requests fall through to the extension-host proxy
//! (§7.3) when one is configured, else `404 {"detail": "Not Found"}`.

use axum::Router;
use axum::body::Body;
use axum::extract::{FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::error::ApiError;
use crate::query::Query;
use crate::state::AppState;

pub mod actions;
pub mod connectors;
pub mod docs;
pub mod engine;
pub mod jobs;
pub mod login;
pub mod misc;
pub mod pipes;
pub mod plugins;
pub mod tokens;
pub mod users;
pub mod version;

/// The core route groups `api:permissions:routes:allowlist` may restrict, plus the v5 `engine`
/// group. Each name matches a module of `meerschaum.api.routes`.
pub const CORE_ROUTE_GROUPS: &[&str] = &[
    "actions",
    "connectors",
    "index",
    "jobs",
    "misc",
    "pipes",
    "plugins",
    "tokens",
    "users",
    "version",
    "engine",
];

/// The route group of a request path (for rate limits and the allowlist).
pub fn route_group_of(path: &str) -> &'static str {
    let first = path.trim_start_matches('/').split('/').next().unwrap_or("");
    match first {
        "" => "index",
        "pipes" | "metadata" => "pipes",
        "actions" => "actions",
        "connectors" => "connectors",
        "jobs" | "logs" => "jobs",
        "users" => "users",
        "tokens" => "tokens",
        "plugins" => "plugins",
        "version" => "version",
        "login" => "login",
        "engine" => "engine",
        "info" | "chaining" | "healthcheck" | "favicon.ico" | "id" => "misc",
        "docs" | "redoc" | "openapi.json" => "docs",
        "mcp" => "mcp",
        "_mrsm" => "internal",
        _ if first.starts_with("jobs") => "jobs",
        _ => "other",
    }
}

/// The parts of a request every handler needs: the state, the headers and the query.
pub struct Ctx {
    /// The server state.
    pub state: AppState,
    /// The request headers.
    pub headers: HeaderMap,
    /// The query parameters.
    pub query: Query,
}

impl Ctx {
    /// `instance_keys` (`Optional[str] = None`).
    pub fn instance_keys(&self) -> Option<String> {
        self.query.opt_str("instance_keys")
    }
}

impl FromRequestParts<AppState> for Ctx {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        Ok(Ctx {
            state: state.clone(),
            headers: parts.headers.clone(),
            query: Query::parse(parts.uri.query()),
        })
    }
}

/// Marks a response produced by the router's fallback (no route matched).
#[derive(Debug, Clone, Copy)]
pub struct Unmatched;

async fn fallback(State(state): State<AppState>, req: Request) -> Response {
    if let Some(host) = state.host.clone() {
        if host.is_active() && is_proxyable(&state.config, req.uri().path()) {
            return host.forward(req).await;
        }
    }
    let mut resp = ApiError::not_found().into_response();
    resp.extensions_mut().insert(Unmatched);
    resp
}

async fn method_not_allowed() -> Response {
    ApiError::method_not_allowed().into_response()
}

/// The client address used as the rate-limit key: the peer (when known), or the first
/// untrusted `X-Forwarded-For` hop when the peer is in `forwarded_allow_ips`.
pub fn client_key(state: &AppState, req: &Request) -> String {
    let peer = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|c| c.0.ip().to_string());
    let trusted = |ip: &str| {
        state
            .config
            .forwarded_allow_ips
            .iter()
            .any(|p| p == "*" || p == ip)
    };
    let peer_trusted = peer.as_deref().is_none_or(trusted);
    if state.config.proxy_headers && peer_trusted {
        if let Some(fwd) = req
            .headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
        {
            let hops: Vec<&str> = fwd
                .split(',')
                .map(str::trim)
                .filter(|h| !h.is_empty())
                .collect();
            if let Some(h) = hops.iter().rev().find(|h| !trusted(h)).or(hops.first()) {
                return (*h).to_string();
            }
        }
    }
    peer.unwrap_or_else(|| "unknown".into())
}

async fn rate_limit(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let group = route_group_of(req.uri().path());
    if let Some(limit) = state.config.rate_limit_for(group) {
        let key = client_key(&state, &req);
        if let Err(retry_after) = state.rate_limiter.check(group, &key, limit) {
            let mut resp = ApiError::new(429, "Rate limit exceeded.").into_response();
            if let Ok(v) =
                axum::http::HeaderValue::from_str(&retry_after.as_secs().max(1).to_string())
            {
                resp.headers_mut().insert(header::RETRY_AFTER, v);
            }
            return resp;
        }
    }
    next.run(req).await
}

/// Whether an unmatched path may be proxied to the API host (§7.3): never the host's internal
/// routes (`/_mrsm/`), and never a core route group the allowlist removed, so that
/// `api:permissions:routes:allowlist` cannot be bypassed by a plugin serving the same path.
pub fn is_proxyable(cfg: &crate::config::ServerConfig, path: &str) -> bool {
    if path.starts_with(crate::mcp::federation::INTERNAL_PREFIX) {
        return false;
    }
    let group = route_group_of(path);
    let core = CORE_ROUTE_GROUPS.contains(&group) || group == "mcp";
    !(core && !cfg.route_group_is_allowed(group))
}

/// Registers one route group.
type AddRoutes = fn(Router<AppState>, &crate::config::ServerConfig) -> Router<AppState>;

/// Build the application router for a state (with the default MCP server).
pub fn router(state: AppState) -> Router {
    let mcp = std::sync::Arc::new(crate::mcp::McpServer::for_state(&state));
    router_with_mcp(state, mcp)
}

/// Build the application router with a configured MCP server (backend, federation).
pub fn router_with_mcp(state: AppState, mcp: std::sync::Arc<crate::mcp::McpServer>) -> Router {
    let cfg = state.config.clone();
    let mut app: Router<AppState> = Router::new();
    // `/login` is always registered (`api/routes/__init__.py`).
    app = login::routes(app);
    let groups: [(&str, AddRoutes); 11] = [
        ("actions", actions::routes),
        ("connectors", connectors::routes),
        ("index", misc::index_routes),
        ("jobs", jobs::routes),
        ("misc", misc::routes),
        ("pipes", pipes::routes),
        ("plugins", plugins::routes),
        ("tokens", tokens::routes),
        ("users", users::routes),
        ("version", version::routes),
        ("engine", engine::routes),
    ];
    for (group, add) in groups {
        if cfg.route_group_is_allowed(group) {
            app = add(app, &cfg);
        }
    }
    // Registered like v4: when `api:mcp:enabled` and the `mcp` group is allowed.
    if cfg.mcp.enabled && cfg.route_group_is_allowed("mcp") {
        app = crate::mcp::http::routes(app, mcp);
    }
    app = docs::routes(app, &cfg);
    if let Some(static_path) = cfg.static_path.clone() {
        app = app.nest_service("/static", tower_http::services::ServeDir::new(static_path));
    }
    app.layer(axum::middleware::from_fn_with_state(
        state.clone(),
        crate::web::protect_sessions,
    ))
    .fallback(fallback)
    .method_not_allowed_fallback(method_not_allowed)
    .layer(axum::middleware::from_fn_with_state(
        state.clone(),
        rate_limit,
    ))
    .with_state(state)
}

/// Read a request body to bytes (Starlette's `await request.body()`).
pub async fn body_bytes(body: Body) -> Result<bytes::Bytes, ApiError> {
    axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| ApiError::exception("Failed to read the request body", e))
}

/// Parse an `application/x-www-form-urlencoded` body into ordered pairs.
pub fn parse_form(body: &[u8]) -> Vec<(String, String)> {
    meerschaum_api::wire::parse_query(&String::from_utf8_lossy(body))
}

/// The last value of a form field.
pub fn form_get<'a>(form: &'a [(String, String)], key: &str) -> Option<&'a str> {
    form.iter()
        .rev()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// Parse a form body that is either urlencoded or `multipart/form-data` (text fields only).
pub async fn read_form(headers: &HeaderMap, body: bytes::Bytes) -> Vec<(String, String)> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if ct.starts_with("multipart/form-data") {
        if let Some(boundary) = ct.split("boundary=").nth(1) {
            return parse_multipart_text(&body, boundary.trim_matches('"'));
        }
    }
    parse_form(&body)
}

fn parse_multipart_text(body: &[u8], boundary: &str) -> Vec<(String, String)> {
    let text = String::from_utf8_lossy(body);
    let delim = format!("--{boundary}");
    let mut out = Vec::new();
    for part in text.split(&delim) {
        let part = part.trim_start_matches("\r\n");
        let Some((head, content)) = part.split_once("\r\n\r\n") else {
            continue;
        };
        let Some(name) = head
            .split(';')
            .map(str::trim)
            .find_map(|p| p.strip_prefix("name="))
        else {
            continue;
        };
        let name = name.split("\r\n").next().unwrap_or(name).trim_matches('"');
        out.push((
            name.to_string(),
            content.trim_end_matches("\r\n").to_string(),
        ));
    }
    out
}

/// A `Method` and `Uri` pair for logs (never with credentials).
pub fn describe(method: &Method, uri: &Uri) -> String {
    format!("{method} {}", uri.path())
}

/// `200` with an empty-bodied redirect (Starlette's `RedirectResponse`, 307).
pub fn redirect(url: &str) -> Response {
    (
        StatusCode::TEMPORARY_REDIRECT,
        [(header::LOCATION, url.to_string())],
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups() {
        assert_eq!(route_group_of("/pipes/a/b/c/data"), "pipes");
        assert_eq!(route_group_of("/metadata"), "pipes");
        assert_eq!(route_group_of("/logs/x/ws"), "jobs");
        assert_eq!(route_group_of("/jobsx/prompt_kwargs"), "jobs");
        assert_eq!(route_group_of("/"), "index");
        assert_eq!(route_group_of("/favicon.ico"), "misc");
        assert_eq!(route_group_of("/plugin-route"), "other");
        assert_eq!(route_group_of("/mcp"), "mcp");
        assert_eq!(route_group_of("/_mrsm/host/mcp"), "internal");
    }

    #[test]
    fn multipart_text_fields() {
        let body = b"--XX\r\nContent-Disposition: form-data; name=\"username\"\r\n\r\nalice\r\n--XX\r\nContent-Disposition: form-data; name=\"password\"\r\n\r\np w\r\n--XX--\r\n";
        let f = parse_multipart_text(body, "XX");
        assert_eq!(form_get(&f, "username"), Some("alice"));
        assert_eq!(form_get(&f, "password"), Some("p w"));
    }
}
