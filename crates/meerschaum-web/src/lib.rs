//! **Internal crate** of [Meerschaum](https://meerschaum.io): its API may change in any minor
//! release. Depend on the [`meerschaum`](https://docs.rs/meerschaum) crate instead.
//!
//! What the Rust server serves of the web console. The console itself is the Plotly Dash app of
//! the Python package, mounted by the API host (design decision D21): the server proxies `/dash`
//! to it, or shows a pointer page when it is not installed (`meerschaum_server::web`). This crate
//! keeps the parts that stay in Rust:
//!
//! - the web terminal: `/webterm/{session_id}` (the page the console's iframe embeds), its
//!   websocket `/websocket/{session_id}` (`meerschaum_server::webterm`, the PTY bridge) and the
//!   vendored xterm.js bundle at `/webterm/assets/{name}`, authenticated through the console's
//!   session store ([`sessions`]);
//! - the job-log stream `GET /logs/{name}/stream` (Server-Sent Events) that follows jobs started
//!   by `POST /execute`, authenticated by bearer token or console session ([`stream`]);
//! - native (Rust source) plugin pages as HTML fragments for the Dash console
//!   (`/_mrsm/native-pages`, [`native_pages`]).
//!
//! Compose with [`app`] (or [`router`] + `meerschaum_server::app_with_mcp_and_console`).
#![deny(unsafe_code)]
mod assets;
pub mod native_pages;
pub mod sessions;
mod stream;
pub mod webterm;
pub use native_pages::{PageContent, PageHandler, PluginPage};
pub use webterm::{ShellCommand, WebtermSettings};

use axum::{
    Router,
    http::{HeaderMap, StatusCode, header},
    routing::get,
};
use meerschaum_core::Map;
use meerschaum_server::{AppState, auth};
use serde_json::{Value as Json, json};
use sessions::{COOKIE, SessionStore};
use std::sync::Arc;

/// What the console routes need.
#[derive(Clone)]
pub struct WebConfig {
    /// The console's session store, shared with the Python console (the `mrsm-session-id`
    /// cookie's rows): authenticates the terminal and the log stream.
    pub sessions: Arc<dyn SessionStore>,
    /// The web terminal (`/webterm/{session_id}`, `/websocket/{session_id}`); `None` serves none.
    pub webterm: Option<WebtermSettings>,
    /// Native plugin pages, served as fragments at `/_mrsm/native-pages`.
    pub pages: Vec<PluginPage>,
}

impl WebConfig {
    /// Prefer the configured cache connector over the instance fallback.
    pub fn with_cache(mut self, cache: &meerschaum_connector_valkey::ValkeyConnector) -> Self {
        self.sessions = Arc::new(sessions::ValkeySessions(cache.backend().clone()));
        self
    }

    /// Sessions in the server's own instance (the `mrsm_sessions` pipe), no terminal, no pages.
    pub fn for_state(state: &AppState) -> Self {
        Self {
            sessions: Arc::new(sessions::InstanceSessions::new(state.instance())),
            webterm: None,
            pages: vec![],
        }
    }
}

#[derive(Clone)]
pub(crate) struct WebState {
    pub server: AppState,
    pub config: WebConfig,
    /// The open job-log streams.
    pub streams: Arc<stream::StreamSlots>,
}

/// The API and the web routes in one router.
pub fn app(state: AppState, config: WebConfig) -> Result<Router, String> {
    let console = router(state.clone(), config)?;
    Ok(meerschaum_server::app_with_console(state, console))
}

/// The routes this crate serves, exact paths only (never a fallback: that is the server's, which
/// proxies `/dash/*` to the Python API host).
pub fn router(server: AppState, config: WebConfig) -> Result<Router, String> {
    if server.config.no_dash {
        return Ok(Router::new());
    }
    let webterm = webterm::config(&server, &config);
    let server_state = server.clone();
    let state = WebState {
        server,
        config: config.clone(),
        streams: Arc::new(stream::StreamSlots::default()),
    };
    let mut router = Router::new();
    // The job-log stream follows the `jobs` route group, like `/logs/{name}`.
    if state.server.config.route_group_is_allowed("jobs") {
        router = router.route("/logs/{name}/stream", get(stream::logs));
    }
    router = native_pages::routes(router, &config.pages)?;
    let mut router = router.with_state(state);
    // The terminal's own routes (`/webterm/{session_id}`, `/websocket/{session_id}`) and its
    // assets, independent of the `dash` route group (as in v4).
    if let Some(webterm) = webterm {
        router = router.merge(meerschaum_server::webterm::routes(
            server_state.clone(),
            webterm,
        ));
        for asset in assets::ASSETS {
            router = router.route(
                &format!("/webterm/assets/{}", asset.name),
                get(assets::serve),
            );
        }
    }
    // These routes are merged next to the API's, which rate limits every route it serves
    // (`api:permissions:rate_limits`): the log stream and the terminal pass through it too.
    Ok(router.layer(axum::middleware::from_fn_with_state(
        server_state,
        meerschaum_server::routes::rate_limit,
    )))
}

pub(crate) fn cookie(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|item| {
            let (key, value) = item.trim().split_once('=')?;
            (key == COOKIE && uuid::Uuid::parse_str(value).is_ok()).then(|| value.into())
        })
}

/// The session behind the request's cookie, with a `username`; the status to answer otherwise
/// (`401`: none or unknown, `503`: the store failed). Never a redirect: the callers are
/// `EventSource` streams, which cannot follow one to a login page.
pub(crate) async fn session(state: &WebState, headers: &HeaderMap) -> Result<Json, StatusCode> {
    let Some(id) = cookie(headers) else {
        return Err(StatusCode::UNAUTHORIZED);
    };
    match state.config.sessions.get(&id).await {
        Ok(Some(data)) if data["username"].is_string() => Ok(data),
        Ok(_) => Err(StatusCode::UNAUTHORIZED),
        Err(_) => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}

/// Request headers carrying a short-lived bearer token for the user of a console session, so
/// that the API's own authorization (user existence, scopes, admin) applies to what the session
/// does.
pub(crate) async fn api_headers(state: &WebState, data: &Json) -> Result<HeaderMap, StatusCode> {
    let mut headers = HeaderMap::new();
    if state.server.config.no_auth {
        return Ok(headers);
    }
    let username = data["username"].as_str().unwrap_or("");
    let login = || StatusCode::UNAUTHORIZED;
    let Some(principal) = auth::load_from_subject(&state.server, username).await else {
        return Err(login());
    };
    let mut scopes = auth::principal_scopes(&state.server, &principal).await;
    if let Some(user) = principal.as_user() {
        if state
            .server
            .instance()
            .get_user_id(user)
            .await
            .ok()
            .flatten()
            .is_none()
        {
            return Err(login());
        }
        if state
            .server
            .instance()
            .get_user_type(user)
            .await
            .ok()
            .flatten()
            .as_deref()
            == Some("admin")
        {
            scopes = vec!["*".into()];
        }
    }
    let claims: Map = serde_json::from_value(
        json!({"sub": username, "scopes": scopes, "exp": chrono::Utc::now().timestamp() + 60}),
    )
    .expect("claims");
    headers.insert(
        header::AUTHORIZATION,
        format!(
            "Bearer {}",
            auth::jwt_encode(&claims, state.server.secret())
        )
        .parse()
        .expect("JWT header"),
    );
    Ok(headers)
}
