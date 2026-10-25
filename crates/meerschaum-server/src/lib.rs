//! **Internal crate** of [Meerschaum](https://meerschaum.io): its API may change in any minor
//! release. Depend on the [`meerschaum`](https://docs.rs/meerschaum) crate instead.
//!
//! `meerschaum-server`: the Meerschaum HTTP API as an `axum` application
//! (`docs/design/v5.0.0.md` §7.1, §7.3, §5.8, D7, D17).
//!
//! Every route of `meerschaum/api/routes` is served at the same path, method, query parameters,
//! status codes and JSON shapes as v4.1 (`/dash/*` is the Python host's Dash console, proxied or
//! replaced by a pointer page, [`web`]; the web terminal's routes are [`webterm`], merged by
//! `meerschaum-web`), plus the v5 `/engine/*` family and `POST /execute`. Wire quirks of the v4 server are kept and marked
//! `// PARITY:` with the Python line.
//!
//! | Module | What it does |
//! |---|---|
//! | [`config`] | `api:*` config keys, `start api` flags, `api:uvicorn` → `api:server` renames |
//! | [`auth`] | Secret key file, HS256 JWTs, API keys, `ScopedAuth`, `CurrentScopes` |
//! | [`state`] | Instance connectors, backends, the shared pipes dictionary (`get_pipe`) |
//! | [`routes`] | The route groups and the router (allowlist, rate limits, fallback proxy) |
//! | [`pipe_methods`] | `Pipe.parse_date_bounds`, `get_sync_time`, `get_chunk_bounds`, `sync` |
//! | [`serialize`] | `to_json` / `to_csv` / documents for pipe data |
//! | [`openapi`] | The OpenAPI document (`utoipa`) and its operation table |
//! | [`backends`] | Traits for actions, jobs, engine state and extra instances, with defaults |
//! | [`engine_stream`] | `GET /engine/runs/{run_id}/events/stream`: a run's events as SSE |
//! | [`ratelimit`] | `api:permissions:rate_limits` token buckets |
//! | [`proxy`], [`host`] | The Python API host: reverse proxy over a Unix socket, supervision |
//! | [`webterm`] | `/webterm/{session_id}` and its websocket: the PTY bridge (`portable-pty`, tmux) |
//! | [`mcp`] | The MCP server (`/mcp`, stdio) and its federation with the API host |
//!
//! All pipe operations go through `Arc<dyn InstanceConnector>`.

#![deny(unsafe_code)]

pub mod auth;
pub mod backends;
pub mod config;
pub mod engine_store;
pub mod engine_stream;
pub mod error;
pub mod execute_policy;
pub mod host;
pub mod json;
pub mod mcp;
pub mod native;
pub mod openapi;
pub mod pipe_methods;
pub mod proxy;
pub mod query;
pub mod ratelimit;
pub mod redact;
pub mod routes;
pub mod serialize;
pub mod state;
pub mod web;
pub mod webterm;

pub use config::{ServerConfig, ServerFlags};
pub use state::AppState;

/// The Meerschaum version this server reports (`/version`, `/info`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build the application router.
pub fn app(state: AppState) -> axum::Router {
    routes::router(state)
}

/// Build the application router with a configured MCP server (its backend and federation).
pub fn app_with_mcp(state: AppState, mcp: std::sync::Arc<mcp::McpServer>) -> axum::Router {
    routes::router_with_mcp(state, mcp)
}

/// Serve the application on `host:port` until the future completes or fails.
pub async fn serve(state: AppState) -> std::io::Result<()> {
    for notice in &state.config.notices {
        tracing::warn!("{notice}");
    }
    let mcp = std::sync::Arc::new(mcp::McpServer::for_state(&state));
    serve_with_mcp(state, mcp).await
}

/// [`serve`] with a configured MCP server. When the state proxies an API host, its MCP registry
/// is fetched at startup (federation, §7.2), which also starts the host.
pub async fn serve_with_mcp(
    state: AppState,
    mcp: std::sync::Arc<mcp::McpServer>,
) -> std::io::Result<()> {
    serve_with_mcp_and_console(state, mcp, None).await
}

/// [`serve_with_mcp`] with an optional router of the routes `meerschaum-web` keeps (the web
/// terminal, the job-log stream, native pages) merged into the application. `meerschaum-web` depends on this crate, so the CLI composes
/// the two and passes the console in here.
pub async fn serve_with_mcp_and_console(
    state: AppState,
    mcp: std::sync::Arc<mcp::McpServer>,
    console: Option<axum::Router>,
) -> std::io::Result<()> {
    if let Some(federation) = mcp.federation.clone() {
        tokio::spawn(async move {
            let _ = federation.refresh().await;
        });
    }
    let addr = format!("{}:{}", state.config.host, state.config.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(
        listener,
        app_with_mcp_and_console(state, mcp, console)
            .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
}

/// Compose `meerschaum-web`'s exact routes with the API without making server depend on web.
/// They must not install a fallback (`/dash/*` is the server's: proxied to the host or the
/// pointer page).
pub fn app_with_console(state: AppState, console: axum::Router) -> axum::Router {
    app(state).merge(console)
}

/// [`app_with_mcp`] plus an optional router of `meerschaum-web`'s exact routes (it must not
/// install a fallback, which would shadow the API host proxy).
pub fn app_with_mcp_and_console(
    state: AppState,
    mcp: std::sync::Arc<mcp::McpServer>,
    console: Option<axum::Router>,
) -> axum::Router {
    let app = app_with_mcp(state, mcp);
    match console {
        Some(console) => app.merge(console),
        None => app,
    }
}
