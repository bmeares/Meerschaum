//! Server-rendered `/dash` console. Compose with `app`; plugin Dash apps remain unmatched.
#![deny(unsafe_code)]
mod pages;
mod pipelines;
pub mod sessions;
pub mod webterm;
pub use webterm::{ShellCommand, WebtermSettings};

use askama::Template;
use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{any, get, post},
};
use meerschaum_core::Map;
use meerschaum_server::{AppState, auth};
use serde_json::{Value as Json, json};
use sessions::{COOKIE, EXPIRES_SECONDS, SessionStore};
use std::{collections::BTreeMap, sync::Arc};
use tower::ServiceExt;

/// Page contract: trusted plugin HTML, served at an exact path, never a wildcard.
#[derive(Clone)]
pub struct PluginPage {
    pub path: String,
    pub page_group: String,
    pub login_required: bool,
    pub skip_navbar: bool,
    pub dark_theme: Option<bool>,
    /// A same-host API route returning an HTML fragment, or inline HTML from a Rust plugin.
    pub content: PageContent,
}
#[derive(Clone)]
pub enum PageContent {
    Html(String),
    Route(String),
    Handler(Arc<dyn PageHandler>),
}
#[derive(Clone)]
pub struct WebConfig {
    pub sessions: Arc<dyn SessionStore>,
    pub session_ttl: i64,
    pub secure_cookie: bool,
    /// The web terminal (`/dash/webterm`, `/webterm/{session_id}`); `None` serves none.
    pub webterm: Option<WebtermSettings>,
    pub pages: Vec<PluginPage>,
    /// Map old relative page paths to owning plugin names.
    pub legacy_pages: BTreeMap<String, String>,
    pub dash_plugins: Vec<String>,
    /// Read access to engine state for the pipelines page; `None` disables the page.
    pub engine: Option<Arc<meerschaum_engine::EngineReader>>,
}
impl WebConfig {
    /// Merge the Python host's page manifest (`GET /_mrsm/host/registry`, its `web_pages[]`).
    ///
    /// `mode: "html"` entries become page-contract pages served as `PageContent::Route(route)`
    /// (the host returns an HTML fragment at `/_pages/<path>`, the console wraps it in its
    /// chrome and forwards the `Cookie` header). `mode: "dash"` entries are legacy Dash pages
    /// owned by a plugin's app: the console links to `/dash/<plugin>/<path>` and redirects the
    /// old `/dash/<path>`. The plugin name comes from the entry's `plugin` field, else from a
    /// `/dash/<plugin>/...` route. Entries that name no usable plugin are skipped with an error.
    pub fn apply_host_registry(&mut self, registry: &Json) -> Result<usize, String> {
        let mut applied = 0;
        let Some(entries) = registry["web_pages"].as_array() else {
            return Ok(0);
        };
        for entry in entries {
            // v4 registers pages as `/dash/<path>`; the console's paths are relative to `/dash`.
            let path = entry["path"].as_str().unwrap_or("").trim_matches('/');
            let path = path.strip_prefix("dash/").unwrap_or(path);
            if path.is_empty() || path == "dash" {
                return Err("Host page entry without a path".into());
            }
            let group = entry["page_group"].as_str().unwrap_or("").to_string();
            match entry["mode"].as_str().unwrap_or("html") {
                "dash" => {
                    let plugin = entry["plugin"]
                        .as_str()
                        .map(str::to_string)
                        .or_else(|| {
                            entry["route"]
                                .as_str()?
                                .strip_prefix("/dash/")?
                                .split('/')
                                .next()
                                .map(str::to_string)
                        })
                        .filter(|p| !p.is_empty());
                    let Some(plugin) = plugin else {
                        return Err(format!("Dash page '{path}' does not name its plugin"));
                    };
                    if !self.dash_plugins.contains(&plugin) {
                        self.dash_plugins.push(plugin.clone());
                    }
                    self.legacy_pages.insert(path.to_string(), plugin);
                }
                _ => {
                    let Some(route) = entry["route"].as_str() else {
                        return Err(format!("Page '{path}' has no route"));
                    };
                    self.pages.push(PluginPage {
                        path: path.to_string(),
                        page_group: group,
                        login_required: entry["login_required"].as_bool().unwrap_or(true),
                        skip_navbar: entry["skip_navbar"].as_bool().unwrap_or(false),
                        dark_theme: entry["dark_theme"].as_bool(),
                        content: PageContent::Route(route.to_string()),
                    });
                }
            }
            applied += 1;
        }
        Ok(applied)
    }
    /// Read engine state through `reader` (the default reads the server's own instance).
    pub fn with_engine(mut self, reader: Arc<meerschaum_engine::EngineReader>) -> Self {
        self.engine = Some(reader);
        self
    }

    /// Prefer the configured cache connector over the instance fallback.
    pub fn with_cache(mut self, cache: &meerschaum_valkey::ValkeyConnector) -> Self {
        self.sessions = Arc::new(sessions::ValkeySessions(cache.backend().clone()));
        self
    }
    pub fn for_state(state: &AppState) -> Self {
        Self {
            sessions: Arc::new(sessions::InstanceSessions::new(state.instance())),
            session_ttl: EXPIRES_SECONDS,
            secure_cookie: state.config.production,
            webterm: None,
            pages: vec![],
            legacy_pages: BTreeMap::new(),
            dash_plugins: vec![],
            engine: Some(engine_reader(state)),
        }
    }
}
/// An engine reader over the server's own instance (runs, ledger and cursors live there).
fn engine_reader(state: &AppState) -> Arc<meerschaum_engine::EngineReader> {
    let keys = state.config.instance_keys.clone();
    let registry = meerschaum_engine::StaticRegistry::new().with_instance(&keys, state.instance());
    let config = meerschaum_engine::EngineConfig {
        defaults: state.config.pipe_defaults.clone(),
        home_instance: keys,
        ..Default::default()
    };
    Arc::new(meerschaum_engine::EngineReader::new(
        Arc::new(registry),
        config,
    ))
}
#[derive(Clone)]
pub(crate) struct WebState {
    pub server: AppState,
    pub api: Router,
    pub config: WebConfig,
    /// Whether the terminal keeps its shell in a tmux session (shows the new-window control).
    pub webterm_tmux: bool,
}
#[derive(Template)]
#[template(path = "chrome.html")]
struct Chrome<'a> {
    title: &'a str,
    content: &'a str,
    csrf: &'a str,
    links: Vec<(String, String)>,
    skip_navbar: bool,
    theme: &'a str,
}

pub fn app(state: AppState, config: WebConfig) -> Result<Router, String> {
    let console = router(state.clone(), config)?;
    Ok(meerschaum_server::app_with_console(state, console))
}
/// Exact routes preserve the server's Python-host fallback for `/dash/<plugin>/*`.
pub fn router(server: AppState, config: WebConfig) -> Result<Router, String> {
    if server.config.no_dash {
        return Ok(Router::new());
    }
    let webterm = webterm::config(&server, &config);
    let server_state = server.clone();
    let state = WebState {
        api: meerschaum_server::app(server.clone()),
        webterm_tmux: webterm.as_ref().is_some_and(|w| w.uses_tmux()),
        server,
        config,
    };
    let mut router = Router::new();
    // PARITY: v4 serves the console's own pages (`dashboard`, `pipes`, `jobs`, `plugins`, `users`,
    // `tokens`, `register`) only when the `dash` route group is allowed by
    // `api:permissions:routes:allowlist` (`dash/callbacks/dashboard.py::_console_pages_allowed`).
    // The login page, the assets and the pages registered by plugins are always served; the v5
    // pages (`settings`, `pipelines`, `webterm`, the actions and partials) follow the group.
    let console_pages = state.server.config.route_group_is_allowed("dash");
    if console_pages {
        for page in [
            "",
            "/",
            "/dashboard",
            "/pipes",
            "/jobs",
            "/plugins",
            "/users",
            "/tokens",
            "/settings",
            "/password-reset",
            "/pipelines",
            "/webterm",
        ] {
            router = router.route(&format!("/dash{page}"), get(pages::page));
        }
        router = router
            .route("/dash/pipelines/graph", get(pipelines::partial))
            .route("/dash/pipelines/gantt", get(pipelines::partial))
            .route("/dash/pipelines/runs", get(pipelines::partial))
            .route("/dash/pipelines/events", get(pipelines::events))
            .route(
                "/dash/register",
                get(pages::register_page).post(pages::register),
            )
            .route("/dash/action", post(pages::action))
            .route(
                "/dash/plugins/upload",
                post(pages::upload_plugin).layer(DefaultBodyLimit::max(
                    meerschaum_server::routes::plugins::ARCHIVE_MAX_BYTES,
                )),
            )
            .route("/dash/job-logs/{name}", get(pages::logs));
    } else {
        // Hidden exactly like v4's page-not-found: a 404, never a fall-through to the API host.
        for page in [
            "",
            "/",
            "/dashboard",
            "/pipes",
            "/jobs",
            "/plugins",
            "/plugins/upload",
            "/users",
            "/tokens",
            "/settings",
            "/password-reset",
            "/pipelines",
            "/pipelines/graph",
            "/pipelines/gantt",
            "/pipelines/runs",
            "/pipelines/events",
            "/webterm",
            "/register",
            "/action",
            "/job-logs/{name}",
        ] {
            router = router.route(&format!("/dash{page}"), any(hidden_page));
        }
    }
    router = router
        .route("/dash/login", get(login_page).post(login))
        .route("/dash/logout", post(logout));
    for name in [
        "console.js",
        "console.css",
        "htmx.min.js",
        "plotly.min.js",
        "maplibre-gl.js",
        "maplibre-gl.css",
        "cytoscape.min.js",
        "xterm.js",
        "xterm.css",
        "addon-fit.js",
        "addon-unicode11.js",
        "webterm.js",
        "webterm.css",
    ] {
        router = router.route(&format!("/dash/assets/{name}"), get(asset));
    }
    let reserved = [
        "dashboard",
        "pipes",
        "jobs",
        "plugins",
        "users",
        "tokens",
        "settings",
        "password-reset",
        "pipelines",
        "webterm",
        "login",
        "register",
        "logout",
        "action",
        "assets",
        "job-logs",
    ];
    for plugin in &state.config.dash_plugins {
        if reserved.contains(&plugin.as_str())
            || plugin.is_empty()
            || !plugin
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(format!(
                "Dash plugin conflicts with console namespace: {plugin}"
            ));
        }
    }
    let mut paths = std::collections::HashSet::new();
    for page in &state.config.pages {
        let path = page.path.trim_matches('/');
        validate_path(path, &reserved, &state.config.dash_plugins)?;
        if !paths.insert(path.to_string()) {
            return Err(format!("Duplicate page: {path}"));
        }
        router = router.route(&format!("/dash/{path}"), get(pages::plugin_page));
    }
    for (path, plugin) in &state.config.legacy_pages {
        let path = path.trim_matches('/');
        if reserved.contains(&plugin.as_str())
            || plugin.is_empty()
            || !plugin
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err("Invalid Dash plugin name".into());
        }
        // Paths already within the owning Dash app must reach the Python host untouched.
        if path.split('/').next() == Some(plugin.as_str()) {
            continue;
        }
        validate_path(path, &reserved, &state.config.dash_plugins)?;
        if !paths.insert(path.to_string()) {
            return Err(format!("Duplicate page: {path}"));
        }
        router = router.route(&format!("/dash/{path}"), get(legacy));
    }
    let mut router = router.with_state(state);
    // The terminal's own routes (`/webterm/{session_id}`, `/websocket/{session_id}`), outside
    // `/dash` as in v4 and independent of the `dash` route group.
    if let Some(webterm) = webterm {
        router = router.merge(meerschaum_server::webterm::routes(server_state, webterm));
    }
    Ok(router)
}
async fn hidden_page() -> Response {
    StatusCode::NOT_FOUND.into_response()
}
fn validate_path(path: &str, reserved: &[&str], dash: &[String]) -> Result<(), String> {
    let first = path.split('/').next().unwrap_or("");
    if path.is_empty()
        || path.chars().any(|c| c.is_control() || c.is_whitespace())
        || path.contains(['{', '}', '*', '?', '#', '%', '\\'])
        || path
            .split('/')
            .any(|s| s == "." || s == ".." || s.is_empty())
        || reserved.contains(&first)
        || dash.iter().any(|p| p == first)
    {
        return Err(format!("Console cannot own page path: {path}"));
    }
    Ok(())
}
pub(crate) fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
pub(crate) fn encode(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
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
pub(crate) async fn session(state: &WebState, headers: &HeaderMap) -> WebResult<Json> {
    let Some(id) = cookie(headers) else {
        return Err(Redirect::to("/dash/login").into_response().into());
    };
    match state.config.sessions.get(&id).await {
        Ok(Some(mut data)) if data["username"].is_string() => {
            if data["csrf"].as_str().is_none() {
                data["csrf"] = json!(uuid::Uuid::new_v4().to_string());
                if state
                    .config
                    .sessions
                    .set(&id, data.clone(), state.config.session_ttl)
                    .await
                    .is_err()
                {
                    return Err(StatusCode::SERVICE_UNAVAILABLE.into_response().into());
                }
            }
            Ok(data)
        }
        Ok(_) => Err(Redirect::to("/dash/login").into_response().into()),
        Err(_) => Err(
            (StatusCode::SERVICE_UNAVAILABLE, "Session store unavailable")
                .into_response()
                .into(),
        ),
    }
}
pub(crate) async fn api_headers(state: &WebState, data: &Json) -> WebResult<HeaderMap> {
    let mut headers = HeaderMap::new();
    if state.server.config.no_auth {
        return Ok(headers);
    }
    let username = data["username"].as_str().unwrap_or("");
    let Some(principal) = auth::load_from_subject(&state.server, username).await else {
        return Err(Redirect::to("/dash/login").into_response().into());
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
            return Err(Redirect::to("/dash/login").into_response().into());
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
        json!({"sub": username, "scopes": scopes, "exp": chrono::Utc::now().timestamp()+60}),
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
pub(crate) async fn api(
    state: &WebState,
    data: &Json,
    method: Method,
    uri: &str,
    body: String,
    form: bool,
) -> Response {
    api_bytes(
        state,
        data,
        method,
        uri,
        body.into_bytes(),
        if form {
            "application/x-www-form-urlencoded"
        } else {
            "application/json"
        },
    )
    .await
}
/// [`api`] with a raw body and content type (multipart uploads).
pub(crate) async fn api_bytes(
    state: &WebState,
    data: &Json,
    method: Method,
    uri: &str,
    body: Vec<u8>,
    content_type: &str,
) -> Response {
    let headers = match api_headers(state, data).await {
        Ok(h) => h,
        Err(r) => return r.into_response(),
    };
    let mut request = match Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::from(body))
    {
        Ok(r) => r,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    *request.headers_mut() = headers;
    request.headers_mut().insert(
        header::CONTENT_TYPE,
        content_type.parse().expect("content type"),
    );
    state
        .api
        .clone()
        .oneshot(request)
        .await
        .expect("infallible router")
}
pub(crate) async fn read_json(response: Response) -> WebResult<Json> {
    if !response.status().is_success() {
        return Err(response.into());
    }
    let bytes = axum::body::to_bytes(response.into_body(), 8 * 1024 * 1024)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY.into_response())?;
    serde_json::from_slice(&bytes)
        .map_err(|_| WebError::from(StatusCode::BAD_GATEWAY.into_response()))
}
pub(crate) fn render(
    state: &WebState,
    headers: &HeaderMap,
    title: &str,
    content: &str,
    data: &Json,
) -> Response {
    render_options(state, headers, title, content, data, false, None)
}
pub(crate) fn render_options(
    state: &WebState,
    headers: &HeaderMap,
    title: &str,
    content: &str,
    data: &Json,
    skip_navbar: bool,
    dark_theme: Option<bool>,
) -> Response {
    let mut response = if headers.get("hx-request").is_some_and(|h| h == "true") {
        Html(format!("<h1>{}</h1>{content}", escape(title))).into_response()
    } else {
        let console_pages = state.server.config.route_group_is_allowed("dash");
        let mut links: Vec<(String, String)> = if console_pages {
            [
                "dashboard",
                "pipes",
                "jobs",
                "plugins",
                "users",
                "tokens",
                "settings",
                "pipelines",
            ]
            .into_iter()
            .map(|p| (format!("/dash/{p}"), p.into()))
            .collect()
        } else {
            Vec::new()
        };
        if console_pages && state.config.webterm.is_some() && !state.server.config.no_webterm {
            links.push(("/dash/webterm".into(), "Terminal".into()));
        }
        for page in &state.config.pages {
            links.push((
                format!("/dash/{}", page.path.trim_matches('/')),
                format!("{}: {}", page.page_group, page.path),
            ));
        }
        for (path, plugin) in &state.config.legacy_pages {
            links.push((legacy_target(plugin, path), path.clone()));
        }
        Html(
            Chrome {
                title,
                content,
                csrf: data["csrf"].as_str().unwrap_or(""),
                links,
                skip_navbar,
                theme: if dark_theme == Some(true) {
                    "dark"
                } else {
                    "light"
                },
            }
            .render()
            .expect("template"),
        )
        .into_response()
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
        .headers_mut()
        .insert(header::VARY, "HX-Request, Cookie".parse().unwrap());
    response
        .headers_mut()
        .insert("x-content-type-options", "nosniff".parse().unwrap());
    response
}
fn login_content() -> &'static str {
    "<form method=\"post\"><label>Username <input name=\"username\" autocomplete=\"username\" required></label><label>Password <input name=\"password\" type=\"password\" autocomplete=\"current-password\" required></label><button>Log in</button></form><a href=\"/dash/register\">Register</a>"
}
async fn login_page(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if state.server.config.no_auth {
        return login(
            State(state),
            headers,
            "username=no-auth&password=no-auth".into(),
        )
        .await;
    }
    render(&state, &headers, "Login", login_content(), &Json::Null)
}
fn cross_site(headers: &HeaderMap) -> bool {
    if headers
        .get("sec-fetch-site")
        .is_some_and(|h| h == "cross-site")
    {
        return true;
    }
    if let Some(origin) = headers.get(header::ORIGIN) {
        let origin_host = origin
            .to_str()
            .ok()
            .and_then(|s| s.parse::<axum::http::Uri>().ok())
            .and_then(|uri| uri.authority().map(|a| a.as_str().to_owned()));
        return origin_host.as_deref() != headers.get(header::HOST).and_then(|h| h.to_str().ok())
            || origin_host.is_none();
    }
    false
}

async fn login(State(state): State<WebState>, headers: HeaderMap, body: String) -> Response {
    if cross_site(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let fields = meerschaum_server::routes::parse_form(body.as_bytes());
    let username = meerschaum_server::routes::form_get(&fields, "username").unwrap_or("");
    let password = meerschaum_server::routes::form_get(&fields, "password").unwrap_or("");
    if username.is_empty() || password.is_empty() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let body = format!(
        "grant_type=password&username={}&password={}",
        encode(username),
        encode(password)
    );
    let request = Request::builder()
        .method(Method::POST)
        .uri("/login")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from(body))
        .unwrap();
    let response = state
        .api
        .clone()
        .oneshot(request)
        .await
        .expect("infallible");
    let token = match read_json(response).await {
        Ok(t) => t,
        Err(r) => return r.into_response(),
    };
    let Some(token) = token["access_token"].as_str() else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let claims = match auth::jwt_decode(token, state.server.secret(), chrono::Utc::now()) {
        Ok(c) => c,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };
    let username = claims.get("sub").and_then(Json::as_str).unwrap_or("");
    let id = uuid::Uuid::new_v4().to_string();
    let data = json!({"username":username,"csrf":uuid::Uuid::new_v4().to_string()});
    if state
        .config
        .sessions
        .set(&id, data, state.config.session_ttl)
        .await
        .is_err()
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let mut response = Redirect::to("/dash").into_response();
    let secure = if state.config.secure_cookie {
        "; Secure"
    } else {
        ""
    };
    response.headers_mut().insert(
        header::SET_COOKIE,
        format!(
            "{COOKIE}={id}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}{secure}",
            state.config.session_ttl
        )
        .parse()
        .unwrap(),
    );
    response
}
pub(crate) fn csrf_ok(headers: &HeaderMap, data: &Json, form: &[(String, String)]) -> bool {
    !cross_site(headers)
        && data["csrf"]
            .as_str()
            .filter(|s| !s.is_empty())
            .is_some_and(|s| meerschaum_server::routes::form_get(form, "csrf") == Some(s))
}
async fn logout(State(state): State<WebState>, headers: HeaderMap, body: String) -> Response {
    let data = match session(&state, &headers).await {
        Ok(d) => d,
        Err(r) => return r.into_response(),
    };
    if !csrf_ok(
        &headers,
        &data,
        &meerschaum_server::routes::parse_form(body.as_bytes()),
    ) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if let Some(id) = cookie(&headers) {
        if state.config.sessions.delete(&id).await.is_err() {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    }
    let mut response = Redirect::to("/dash/login").into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        format!("{COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0")
            .parse()
            .unwrap(),
    );
    response
}
pub fn legacy_target(plugin: &str, path: &str) -> String {
    let path = path.trim_matches('/');
    if path.split('/').next() == Some(plugin) {
        format!("/dash/{path}")
    } else {
        format!("/dash/{plugin}/{path}")
    }
}
async fn legacy(State(state): State<WebState>, req: Request) -> Response {
    let path = req.uri().path().trim_start_matches("/dash/");
    let Some((old, plugin)) = state
        .config
        .legacy_pages
        .iter()
        .find(|(old, _)| old.trim_matches('/') == path)
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut target = legacy_target(plugin, old);
    if let Some(q) = req.uri().query() {
        target.push('?');
        target.push_str(q);
    }
    Redirect::temporary(&target).into_response()
}
async fn asset(req: Request) -> Response {
    let name = req.uri().path().rsplit('/').next().unwrap_or("");
    let (data, mime): (&'static [u8], &str) = match name {
        "console.js" => (include_bytes!("../assets/console.js"), "text/javascript"),
        "console.css" => (include_bytes!("../assets/console.css"), "text/css"),
        "htmx.min.js" => (include_bytes!("../assets/htmx.min.js"), "text/javascript"),
        "plotly.min.js" => (include_bytes!("../assets/plotly.min.js"), "text/javascript"),
        "maplibre-gl.js" => (
            include_bytes!("../assets/maplibre-gl.js"),
            "text/javascript",
        ),
        "maplibre-gl.css" => (include_bytes!("../assets/maplibre-gl.css"), "text/css"),
        "cytoscape.min.js" => (
            include_bytes!("../assets/cytoscape.min.js"),
            "text/javascript",
        ),
        "xterm.js" => (include_bytes!("../assets/xterm.js"), "text/javascript"),
        "xterm.css" => (include_bytes!("../assets/xterm.css"), "text/css"),
        "addon-fit.js" => (include_bytes!("../assets/addon-fit.js"), "text/javascript"),
        "addon-unicode11.js" => (
            include_bytes!("../assets/addon-unicode11.js"),
            "text/javascript",
        ),
        "webterm.js" => (include_bytes!("../assets/webterm.js"), "text/javascript"),
        "webterm.css" => (include_bytes!("../assets/webterm.css"), "text/css"),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    ([(header::CONTENT_TYPE, mime)], data).into_response()
}

/// Native page providers return the same trusted HTML fragment and status as Python pages.
#[async_trait::async_trait]
pub trait PageHandler: Send + Sync {
    async fn render(&self, query: &str, session: &Json) -> (String, StatusCode);
}

/// Keep HTTP failures off the stack while retaining their status and headers unchanged.
pub(crate) struct WebError(Box<Response>);
pub(crate) type WebResult<T> = Result<T, WebError>;
impl From<Response> for WebError {
    fn from(response: Response) -> Self {
        Self(Box::new(response))
    }
}
impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        *self.0
    }
}
