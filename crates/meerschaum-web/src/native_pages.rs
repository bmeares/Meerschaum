//! Native (Rust source) plugin pages, for the Dash console.
//!
//! A native plugin declares `web_pages` in its manifest and answers a `page` call with an HTML
//! fragment (the page contract of `@web_page`: `data-mrsm-figure` / `data-mrsm-map` elements). The
//! Dash console is the Python host's, so the host fetches these fragments from the server it runs
//! next to: `GET /_mrsm/native-pages` lists them and `GET /_mrsm/native-pages/{path}` renders one.
//! Both need the host's internal token (`X-Mrsm-Host-Token`, signed with the API secret, never
//! forwarded for clients); the fragment also needs `X-Mrsm-User` (the console session's user, `""`
//! for a page that does not require login).
use super::*;
use axum::{
    extract::{Path, State},
    response::{Html, IntoResponse, Response},
};
use meerschaum_server::mcp::federation::{TOKEN_CLAIM, TOKEN_HEADER};

/// Trusted plugin HTML, a native handler, rendered into a fragment for the Dash console.
#[derive(Clone)]
pub struct PluginPage {
    /// The plugin that owns the page.
    pub plugin: String,
    /// The page path (`/dash/<path>` in the console), exact, never a wildcard.
    pub path: String,
    pub page_group: String,
    pub login_required: bool,
    pub skip_navbar: bool,
    pub dark_theme: Option<bool>,
    /// Inline HTML, or a native handler.
    pub content: PageContent,
}

/// Where a page's fragment comes from.
#[derive(Clone)]
pub enum PageContent {
    /// Inline HTML (same `data-mrsm-figure` and `data-mrsm-map` contract as host fragments).
    Html(String),
    /// A native plugin's `page` handler.
    Handler(Arc<dyn PageHandler>),
}

/// Native page providers return the same trusted HTML fragment and status as Python pages.
#[async_trait::async_trait]
pub trait PageHandler: Send + Sync {
    async fn render(&self, query: &str, session: &Json) -> (String, StatusCode);
}

/// Whether `path` is a page path the console may own: relative segments only, no wildcards.
fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && !path.chars().any(|c| c.is_control() || c.is_whitespace())
        && !path.contains(['{', '}', '*', '?', '#', '%', '\\'])
        && !path
            .split('/')
            .any(|s| s == "." || s == ".." || s.is_empty())
}

/// Add the native-page routes (nothing when there are no pages).
pub(crate) fn routes(
    router: Router<WebState>,
    pages: &[PluginPage],
) -> Result<Router<WebState>, String> {
    if pages.is_empty() {
        return Ok(router);
    }
    let mut seen = std::collections::HashSet::new();
    for page in pages {
        let path = page.path.trim_matches('/');
        if !valid_path(path) {
            return Err(format!("Console cannot own page path: {}", page.path));
        }
        if !seen.insert(path.to_string()) {
            return Err(format!("Duplicate page: {path}"));
        }
    }
    Ok(router
        .route("/_mrsm/native-pages", get(list))
        .route("/_mrsm/native-pages/{*path}", get(fragment)))
}

/// Whether the request carries the host's internal token.
fn internal(state: &WebState, headers: &HeaderMap) -> bool {
    let token = headers
        .get(TOKEN_HEADER)
        .or_else(|| headers.get(header::AUTHORIZATION))
        .and_then(|v| v.to_str().ok())
        .map(|v| v.strip_prefix("Bearer ").unwrap_or(v));
    token
        .and_then(|t| auth::jwt_decode(t, state.server.secret(), chrono::Utc::now()).ok())
        .is_some_and(|claims| claims.get(TOKEN_CLAIM).and_then(Json::as_str) == Some("mcp"))
}

async fn list(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if !internal(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let pages: Vec<Json> = state
        .config
        .pages
        .iter()
        .map(|page| {
            json!({
                "path": page.path.trim_matches('/'),
                "page_group": page.page_group,
                "login_required": page.login_required,
                "skip_navbar": page.skip_navbar,
                "dark_theme": page.dark_theme,
                "plugin": page.plugin,
            })
        })
        .collect();
    axum::Json(pages).into_response()
}

async fn fragment(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(path): Path<String>,
    uri: axum::http::Uri,
) -> Response {
    if !internal(&state, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let path = path.trim_matches('/');
    let Some(page) = state
        .config
        .pages
        .iter()
        .find(|p| p.path.trim_matches('/') == path)
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let username = headers
        .get("x-mrsm-user")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if page.login_required && username.is_empty() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (content, status) = match &page.content {
        PageContent::Html(html) => (html.clone(), StatusCode::OK),
        PageContent::Handler(handler) => {
            handler
                .render(uri.query().unwrap_or(""), &json!({"username": username}))
                .await
        }
    };
    let mut response = (status, Html(content)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    response
}
