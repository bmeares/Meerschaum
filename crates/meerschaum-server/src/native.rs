//! Routes served by native (Rust) plugins: the server half of `@api_plugin` for plugins loaded
//! in-process (§9.1). This crate does not load plugins; the CLI implements
//! [`NativeRouteHandler`] over its native plugin registry and passes the declared routes to
//! [`native_router`], which mounts them next to the core API.
//!
//! - **Paths** take `{name}` parameters (with an optional static prefix and suffix and a Starlette
//!   converter: `{id:int}`, `{x:float}`, `{u:uuid}`) and a final `{*rest}` (or `{rest:path}`)
//!   catch-all ([`check_route_path`]). A path that would shadow the core API is refused when the
//!   router is built, as are routes that conflict with an earlier native route. Refusals are
//!   returned as notices; the rest still mount.
//! - **Methods**: any method, or `*` for all. Several routes may share a path with different
//!   methods; a method no route declares is a `405`. `HEAD` falls back to a `GET` route.
//! - **Auth**: a route needs the core API's credentials (a user session or an API key) and every
//!   scope it declares, checked like the core routes' `ScopedAuth`; `public` routes need none
//!   (and may not declare scopes).
//!   Under `--no-auth` every route is open and the caller holds `*`.
//! - The handler receives the full request ([`NativeRouteRequest`]) and returns a full response.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::auth::Principal;
use crate::error::ApiError;
use crate::state::AppState;

/// The largest request body passed to a native route.
pub const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// A route declared by a native plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeRoute {
    /// The plugin serving it.
    pub plugin: String,
    /// The route's name within the plugin.
    pub name: String,
    /// The method (uppercase), or `*` for every method.
    pub method: String,
    /// The declared path (`/my/items/{id}`).
    pub path: String,
    /// The scopes a caller must hold.
    pub scopes: Vec<String>,
    /// Served without credentials.
    pub public: bool,
}

/// One request to a native route.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NativeRouteRequest {
    /// The method (uppercase).
    pub method: String,
    /// The request path.
    pub path: String,
    /// The path parameters, percent-decoded.
    pub path_params: BTreeMap<String, String>,
    /// The raw query string.
    pub query: String,
    /// The decoded query parameters, every value in order.
    pub query_params: BTreeMap<String, Vec<String>>,
    /// The headers (lowercase names) without `authorization`; `cookie` without the console's
    /// session cookie (`mrsm-session-id`).
    pub headers: BTreeMap<String, Vec<String>>,
    /// The body.
    pub body: Vec<u8>,
    /// The principal as JSON ([`principal_json`]), `Null` when anonymous.
    pub principal: serde_json::Value,
    /// The granted scopes (`["*"]` under `--no-auth`).
    pub scopes: Vec<String>,
}

/// The response of a native route.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NativeRouteResponse {
    /// The status code.
    pub status: u16,
    /// The headers, in order.
    pub headers: Vec<(String, String)>,
    /// The body.
    pub body: Vec<u8>,
}

/// Serves native routes (implemented by the CLI over its loaded plugins).
#[async_trait]
pub trait NativeRouteHandler: Send + Sync + 'static {
    /// Serve `request` with `route`. An `Err` is a `500 {"detail": message}`.
    async fn handle(
        &self,
        route: &NativeRoute,
        request: NativeRouteRequest,
    ) -> Result<NativeRouteResponse, String>;
}

/// The principal as passed to plugins: `{"kind": "user", "username", "user_id", "user_type"}`
/// or `{"kind": "token", "token_id", "label", "user_id"}`; `Null` without one. No secrets.
pub fn principal_json(principal: Option<&Principal>) -> serde_json::Value {
    use serde_json::json;
    let id = |v: &Option<meerschaum_core::Value>| {
        v.as_ref()
            .and_then(|v| serde_json::to_value(v).ok())
            .unwrap_or(serde_json::Value::Null)
    };
    match principal {
        None => serde_json::Value::Null,
        Some(Principal::User(u)) => json!({
            "kind": "user",
            "username": u.username,
            "user_id": id(&u.user_id),
            "user_type": u.user_type,
        }),
        Some(Principal::Token(t)) => json!({
            "kind": "token",
            "token_id": t.id.map(|i| i.to_string()),
            "label": t.label,
            "user_id": id(&t.user_id),
        }),
    }
}

fn is_identifier(n: &str) -> bool {
    let mut chars = n.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The type a path parameter must match: Starlette's converters (`{id:int}`, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParamKind {
    /// `{name}` / `{name:str}`: any non-empty text without `/`.
    Str,
    /// `{name:int}`: ASCII digits.
    Int,
    /// `{name:float}`: digits with an optional fraction.
    Float,
    /// `{name:uuid}`: a hyphenated UUID.
    Uuid,
}

impl ParamKind {
    fn accepts(self, value: &str) -> bool {
        let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
        match self {
            ParamKind::Str => !value.is_empty(),
            ParamKind::Int => digits(value),
            ParamKind::Float => match value.split_once('.') {
                Some((whole, fraction)) => digits(whole) && digits(fraction),
                None => digits(value),
            },
            ParamKind::Uuid => value.len() == 36 && uuid::Uuid::parse_str(value).is_ok(),
        }
    }
    fn name(self) -> &'static str {
        match self {
            ParamKind::Str => "str",
            ParamKind::Int => "int",
            ParamKind::Float => "float",
            ParamKind::Uuid => "uuid",
        }
    }
}

/// One segment of a declared route path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Static(String),
    /// `prefix{name[:kind]}suffix`.
    Param {
        prefix: String,
        name: String,
        kind: ParamKind,
        suffix: String,
    },
    /// The final `{*name}` (or `{name:path}`): the rest of the path, `/` included.
    CatchAll(String),
}

/// A parsed route path ([`check_route_path`] documents the syntax).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Template(Vec<Segment>);

impl Template {
    fn parse(path: &str) -> Result<Template, String> {
        if !path.starts_with('/') || path.starts_with("//") {
            return Err("the path must start with a single '/'".into());
        }
        if path.contains(['?', '#', '%', '\\', ' ']) {
            return Err("the path may not contain '?', '#', '%', '\\' or spaces".into());
        }
        let raw: Vec<&str> = path[1..].split('/').collect();
        let mut segments = Vec::with_capacity(raw.len());
        let mut names = std::collections::HashSet::new();
        for (i, segment) in raw.iter().enumerate() {
            let last = i + 1 == raw.len();
            if *segment == "." || *segment == ".." {
                return Err("the path may not contain '.' or '..' segments".into());
            }
            if segment.starts_with(':') {
                return Err(format!(
                    "segment '{segment}': parameters are written '{{name}}', not ':name'"
                ));
            }
            let Some(open) = segment.find('{') else {
                if segment.contains('}') {
                    return Err(format!("segment '{segment}': unbalanced '}}'"));
                }
                if segment.starts_with('*') {
                    return Err(format!(
                        "segment '{segment}': a catch-all is written '{{*name}}'"
                    ));
                }
                segments.push(Segment::Static(segment.to_string()));
                continue;
            };
            let close = segment[open..]
                .find('}')
                .map(|c| open + c)
                .ok_or_else(|| format!("segment '{segment}': unbalanced '{{'"))?;
            let (prefix, inner, suffix) = (
                &segment[..open],
                &segment[open + 1..close],
                &segment[close + 1..],
            );
            if suffix.contains(['{', '}']) || prefix.contains('}') {
                return Err(format!(
                    "segment '{segment}': a segment may hold one parameter"
                ));
            }
            if i == 0 {
                return Err(
                    "a parameter in the first segment would shadow every core route".into(),
                );
            }
            let (name, kind) = match inner.strip_prefix('*') {
                Some(name) => (name, None),
                None => match inner.split_once(':') {
                    None => (inner, Some(ParamKind::Str)),
                    Some((name, "str")) => (name, Some(ParamKind::Str)),
                    Some((name, "int")) => (name, Some(ParamKind::Int)),
                    Some((name, "float")) => (name, Some(ParamKind::Float)),
                    Some((name, "uuid")) => (name, Some(ParamKind::Uuid)),
                    Some((name, "path")) => (name, None),
                    Some((_, other)) => {
                        return Err(format!(
                            "segment '{segment}': unknown converter '{other}' \
                             (str, int, float, uuid or path)"
                        ));
                    }
                },
            };
            if !is_identifier(name) {
                return Err(format!(
                    "segment '{segment}': '{name}' is not a valid parameter name"
                ));
            }
            if !names.insert(name.to_string()) {
                return Err(format!(
                    "segment '{segment}': the parameter '{name}' appears twice"
                ));
            }
            match kind {
                Some(kind) => segments.push(Segment::Param {
                    prefix: prefix.to_string(),
                    name: name.to_string(),
                    kind,
                    suffix: suffix.to_string(),
                }),
                None => {
                    if !prefix.is_empty() || !suffix.is_empty() {
                        return Err(format!(
                            "segment '{segment}': a catch-all must be the whole segment"
                        ));
                    }
                    if !last {
                        return Err(format!(
                            "segment '{segment}': a catch-all must be the last segment"
                        ));
                    }
                    segments.push(Segment::CatchAll(name.to_string()));
                }
            }
        }
        Ok(Template(segments))
    }

    /// The path mounted on the router: each parameter takes its whole segment under a positional
    /// name, so routes differing only in names, prefixes, suffixes or converters share a mount and
    /// are told apart by [`Template::matches`].
    fn mount_path(&self) -> String {
        let mut out = String::new();
        for (i, segment) in self.0.iter().enumerate() {
            out.push('/');
            match segment {
                Segment::Static(s) => out.push_str(s),
                Segment::Param { .. } => out.push_str(&format!("{{p{i}}}")),
                Segment::CatchAll(_) => out.push_str(&format!("{{*p{i}}}")),
            }
        }
        out
    }

    /// The template without its parameter names: two routes with the same shape match the same
    /// requests.
    fn shape(&self) -> String {
        let mut out = String::new();
        for segment in &self.0 {
            out.push('/');
            match segment {
                Segment::Static(s) => out.push_str(s),
                Segment::Param {
                    prefix,
                    kind,
                    suffix,
                    ..
                } => out.push_str(&format!("{prefix}{{:{}}}{suffix}", kind.name())),
                Segment::CatchAll(_) => out.push_str("{*}"),
            }
        }
        out
    }

    /// The percent-decoded parameters when `path` matches, else `None`.
    fn matches(&self, path: &str) -> Option<BTreeMap<String, String>> {
        let mut out = BTreeMap::new();
        let raw: Vec<&str> = path.strip_prefix('/')?.split('/').collect();
        for (i, segment) in self.0.iter().enumerate() {
            match segment {
                Segment::CatchAll(name) => {
                    let rest = raw.get(i..)?.join("/");
                    if rest.is_empty() {
                        return None;
                    }
                    out.insert(name.clone(), percent_decode(&rest));
                    return Some(out);
                }
                Segment::Static(s) => {
                    let actual = raw.get(i)?;
                    if actual != s && percent_decode(actual) != *s {
                        return None;
                    }
                }
                Segment::Param {
                    prefix,
                    name,
                    kind,
                    suffix,
                } => {
                    let value = raw
                        .get(i)?
                        .strip_prefix(prefix.as_str())?
                        .strip_suffix(suffix.as_str())?;
                    let value = percent_decode(value);
                    if !kind.accepts(&value) {
                        return None;
                    }
                    out.insert(name.clone(), value);
                }
            }
        }
        (raw.len() == self.0.len()).then_some(out)
    }
}

/// Whether a native route may take `path`: `Err` names the reason (bad syntax, or the core
/// route it would shadow).
///
/// Segments are static text, a parameter `{name}` with an optional static prefix and suffix
/// (`/items/{id}.json`, one parameter per segment) and an optional Starlette converter
/// (`{id:int}`, `{x:float}`, `{u:uuid}`, `{s:str}`), or a final catch-all `{*rest}` (also
/// written `{rest:path}`). The first segment must be static and may not name a core route group,
/// the console (`/dash`), its page routes (`/_pages`), `/static` or the internal `/_mrsm` routes.
pub fn check_route_path(path: &str) -> Result<(), String> {
    Template::parse(path)?;
    let first = path[1..].split('/').next().unwrap_or("");
    let group = crate::routes::route_group_of(path);
    if group != "other" {
        return Err(format!(
            "it would shadow the core route group '{}'",
            if group == "index" { "/" } else { group }
        ));
    }
    if crate::web::is_console_path(path) || first == "dash" {
        return Err("it would shadow the web console ('/dash')".into());
    }
    if first == "_pages" {
        return Err("it would shadow the console's page routes ('/_pages')".into());
    }
    if first == "static" {
        return Err("it would shadow the static files ('/static')".into());
    }
    if first.starts_with("jobs") || first.starts_with("_mrsm") {
        return Err(format!("it would shadow the core route '/{first}'"));
    }
    Ok(())
}

/// The path parameters of `path` against the declared `template`, percent-decoded (empty when
/// the template is invalid or does not match).
pub fn path_params(template: &str, path: &str) -> BTreeMap<String, String> {
    Template::parse(template)
        .ok()
        .and_then(|t| t.matches(path))
        .unwrap_or_default()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2]))
        {
            out.push((hi * 16 + lo) as u8);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A route with its parsed path.
struct Mounted {
    route: NativeRoute,
    template: Template,
}

/// Build the router serving `routes` with `handler`. Routes that are refused (syntax, shadowing
/// a core route, conflicting with an earlier route, `public` with scopes, disabled by
/// `api:permissions:routes`) are skipped; each refusal is one notice naming the plugin, the route
/// and the reason.
pub fn native_router(
    state: &AppState,
    routes: Vec<NativeRoute>,
    handler: Arc<dyn NativeRouteHandler>,
) -> (Router, Vec<String>) {
    let mut notices = Vec::new();
    let mut matcher = matchit::Router::<usize>::new();
    // Mount path -> the routes sharing it, in declaration order.
    let mut mounts: Vec<(String, Vec<Mounted>)> = Vec::new();
    for mut route in routes {
        route.method = route.method.trim().to_ascii_uppercase();
        let refuse = |reason: String| {
            format!(
                "Native plugin '{}': route {} {} is refused: {reason}.",
                route.plugin, route.method, route.path
            )
        };
        if route.method != "*" && route.method.parse::<Method>().is_err() {
            notices.push(refuse(format!("'{}' is not an HTTP method", route.method)));
            continue;
        }
        if let Err(reason) = check_route_path(&route.path) {
            notices.push(refuse(reason));
            continue;
        }
        if route.public && !route.scopes.is_empty() {
            notices.push(refuse(
                "a public route needs no credentials, so it cannot require scopes".into(),
            ));
            continue;
        }
        if !crate::routes::is_proxyable(&state.config, &route.path) {
            notices.push(refuse(
                "the route group is disabled by `api:permissions:routes`".into(),
            ));
            continue;
        }
        let Ok(template) = Template::parse(&route.path) else {
            continue;
        };
        let mount = template.mount_path();
        if let Some((_, existing)) = mounts.iter_mut().find(|(m, _)| *m == mount) {
            let shape = template.shape();
            let overlaps = |m: &&Mounted| {
                let r = &m.route;
                m.template.shape() == shape
                    && (r.method == route.method || r.method == "*" || route.method == "*")
            };
            if let Some(other) = existing.iter().find(overlaps).map(|m| &m.route) {
                notices.push(refuse(format!(
                    "plugin '{}' already serves {} {}",
                    other.plugin, other.method, other.path
                )));
                continue;
            }
            existing.push(Mounted { route, template });
            continue;
        }
        if let Err(conflict) = matcher.insert(mount.clone(), mounts.len()) {
            notices.push(refuse(format!(
                "it conflicts with another route ({conflict})"
            )));
            continue;
        }
        mounts.push((mount, vec![Mounted { route, template }]));
    }
    let mut router = Router::new();
    for (mount, routes) in mounts {
        let routes = Arc::new(routes);
        let handler = handler.clone();
        let state = state.clone();
        router = router.route(
            &mount,
            axum::routing::any(move |req: Request| {
                let routes = routes.clone();
                let handler = handler.clone();
                let state = state.clone();
                async move { serve(state, routes, handler, req).await }
            }),
        );
    }
    let router = router.layer(axum::middleware::from_fn_with_state(
        state.clone(),
        crate::routes::rate_limit,
    ));
    (router, notices)
}

/// The route serving `method` among those whose path matched, in declaration order: the exact
/// method, then `GET` for `HEAD`, then `*`.
fn select(routes: &[(&NativeRoute, BTreeMap<String, String>)], method: &Method) -> Option<usize> {
    let m = method.as_str();
    routes
        .iter()
        .position(|(r, _)| r.method == m)
        .or_else(|| {
            (method == Method::HEAD)
                .then(|| routes.iter().position(|(r, _)| r.method == "GET"))
                .flatten()
        })
        .or_else(|| routes.iter().position(|(r, _)| r.method == "*"))
}

async fn authenticate(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    route: &NativeRoute,
) -> Result<(Option<Principal>, Vec<String>), ApiError> {
    if state.config.no_auth {
        return Ok((None, vec!["*".into()]));
    }
    if route.public {
        // Credentials are optional: pass the principal along when they are valid.
        if headers.contains_key(header::AUTHORIZATION)
            && let Ok((principal, scopes)) = crate::auth::current_scopes(state, headers).await
        {
            return Ok((principal, scopes.to_list()));
        }
        return Ok((None, Vec::new()));
    }
    let (principal, scopes) = crate::auth::current_scopes(state, headers).await?;
    if !scopes.has("*") {
        for scope in &route.scopes {
            if !scopes.has(scope) {
                return Err(ApiError::new(
                    403,
                    format!("Missing required scope: '{scope}'"),
                ));
            }
        }
    }
    Ok((principal, scopes.to_list()))
}

/// The `cookie` header without the console's session cookie (`mrsm-session-id`), which is a
/// credential like `authorization`; `None` when nothing is left.
fn plugin_cookies(value: &str) -> Option<String> {
    let kept: Vec<&str> = value
        .split(';')
        .map(str::trim)
        .filter(|pair| {
            !pair.is_empty() && pair.split('=').next().map(str::trim) != Some("mrsm-session-id")
        })
        .collect();
    (!kept.is_empty()).then(|| kept.join("; "))
}

/// Whether a body error is the [`MAX_BODY_BYTES`] limit (rather than a client disconnect or a
/// malformed body).
fn is_length_limit(error: &axum::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(e) = source {
        if e.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        source = e.source();
    }
    false
}

async fn serve(
    state: AppState,
    routes: Arc<Vec<Mounted>>,
    handler: Arc<dyn NativeRouteHandler>,
    req: Request,
) -> Response {
    let path = req.uri().path().to_string();
    let matching: Vec<(&NativeRoute, BTreeMap<String, String>)> = routes
        .iter()
        .filter_map(|m| m.template.matches(&path).map(|p| (&m.route, p)))
        .collect();
    if matching.is_empty() {
        return ApiError::not_found().into_response();
    }
    let Some(index) = select(&matching, req.method()) else {
        let mut allow: Vec<&str> = matching.iter().map(|(r, _)| r.method.as_str()).collect();
        allow.dedup();
        let mut response = ApiError::method_not_allowed().into_response();
        if let Ok(v) = HeaderValue::from_str(&allow.join(", ")) {
            response.headers_mut().insert(header::ALLOW, v);
        }
        return response;
    };
    let (route, params) = &matching[index];
    let (parts, body) = req.into_parts();
    let (principal, scopes) = match authenticate(&state, &parts.headers, route).await {
        Ok(auth) => auth,
        Err(error) => return error.into_response(),
    };
    let body = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(body) => body.to_vec(),
        Err(error) if is_length_limit(&error) => {
            return ApiError::new(413, "The request body is too large.").into_response();
        }
        Err(error) => {
            return ApiError::new(400, format!("The request body could not be read: {error}"))
                .into_response();
        }
    };
    let query = parts.uri.query().unwrap_or("").to_string();
    let mut query_params: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (k, v) in meerschaum_connector_api::wire::parse_query(&query) {
        query_params.entry(k).or_default().push(v);
    }
    let mut headers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in &parts.headers {
        if name == header::AUTHORIZATION {
            continue;
        }
        let mut value = String::from_utf8_lossy(value.as_bytes()).into_owned();
        if name == header::COOKIE {
            match plugin_cookies(&value) {
                Some(kept) => value = kept,
                None => continue,
            }
        }
        headers
            .entry(name.as_str().to_string())
            .or_default()
            .push(value);
    }
    let head = parts.method == Method::HEAD;
    let request = NativeRouteRequest {
        method: parts.method.as_str().to_string(),
        path,
        path_params: params.clone(),
        query,
        query_params,
        headers,
        body,
        principal: principal_json(principal.as_ref()),
        scopes,
    };
    match handler.handle(route, request).await {
        Ok(reply) => respond(reply, head),
        Err(message) => ApiError::new(500, message).into_response(),
    }
}

fn respond(reply: NativeRouteResponse, head: bool) -> Response {
    let status = StatusCode::from_u16(reply.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = if head {
        Response::new(axum::body::Body::empty())
    } else {
        Response::new(axum::body::Body::from(reply.body))
    };
    *response.status_mut() = status;
    let mut invalid = Vec::new();
    for (name, value) in reply.headers {
        match (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            (Ok(name), Ok(value)) => {
                response.headers_mut().append(name, value);
            }
            _ => {
                invalid.push(name);
            }
        }
    }
    if !invalid.is_empty() {
        tracing::warn!(
            "A native route returned invalid headers: {}",
            invalid.join(", ")
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shadowing_and_syntax_are_refused() {
        for ok in [
            "/my/items/{id}",
            "/my/files/{*rest}",
            "/hello",
            "/a/{x}/b/{y}",
            "/my/items/{id}.json",
            "/my/v{major:int}/x",
            "/my/files/{p:path}",
            "/my/u/{u:uuid}/{f:float}",
        ] {
            assert_eq!(check_route_path(ok), Ok(()), "{ok}");
        }
        for (bad, why) in [
            ("/pipes/x", "pipes"),
            ("/", "'/'"),
            ("/mcp", "mcp"),
            ("/login", "login"),
            ("/dash/x", "console"),
            ("/static/x", "static"),
            ("/jobsx", "jobs"),
            ("/_mrsm/x", "internal"),
            ("/{x}", "first segment"),
            ("/a/{*r}/b", "last segment"),
            ("/a/:id", ":name"),
            ("/a/{x}{y}", "one parameter"),
            ("/a/x{*r}", "whole segment"),
            ("/a/{p:path}/b", "last segment"),
            ("/a/{x:bool}", "unknown converter"),
            ("/a/{x}/{x}", "twice"),
            ("/a/{x", "unbalanced"),
            ("/_pages/{*rest}", "_pages"),
            ("/x{id}", "first segment"),
            ("/a/../b", ".."),
            ("relative", "single '/'"),
        ] {
            let e = check_route_path(bad).unwrap_err();
            assert!(e.contains(why), "{bad}: {e}");
        }
    }

    #[test]
    fn path_params_are_decoded() {
        let p = path_params("/a/{id}/f/{*rest}", "/a/x%20y/f/b/c%2Fd");
        assert_eq!(p["id"], "x y");
        assert_eq!(p["rest"], "b/c/d");
        assert!(path_params("/a/b", "/a/b").is_empty());
        let p = path_params("/a/{id}.json/{n:int}/{f:path}", "/a/x.y.json/42/b/c");
        assert_eq!(p["id"], "x.y");
        assert_eq!(p["n"], "42");
        assert_eq!(p["f"], "b/c");
    }

    #[test]
    fn templates_match_converters_prefixes_and_suffixes() {
        let t = Template::parse("/a/v{n:int}/{id}.json").unwrap();
        assert_eq!(t.mount_path(), "/a/{p1}/{p2}");
        assert_eq!(t.shape(), "/a/v{:int}/{:str}.json");
        assert!(t.matches("/a/v2/x.json").is_some());
        assert!(t.matches("/a/vx/x.json").is_none());
        assert!(t.matches("/a/v2/.json").is_none());
        assert!(t.matches("/a/v2/x.csv").is_none());
        assert!(t.matches("/a/v2/x.json/more").is_none());
        let f = Template::parse("/a/{f:float}/{u:uuid}").unwrap();
        assert!(
            f.matches("/a/1.5/67e55044-10b1-426f-9247-bb680e5fe0c8")
                .is_some()
        );
        assert!(
            f.matches("/a/1./67e55044-10b1-426f-9247-bb680e5fe0c8")
                .is_none()
        );
        assert!(f.matches("/a/1/not-a-uuid").is_none());
        let c = Template::parse("/a/{*rest}").unwrap();
        assert!(c.matches("/a/").is_none());
        assert_eq!(c.matches("/a/b/c").unwrap()["rest"], "b/c");
    }

    #[tokio::test]
    async fn only_the_length_limit_is_a_413() {
        use axum::body::{Body, to_bytes};
        let e = to_bytes(Body::from(vec![0u8; 10]), 4).await.unwrap_err();
        assert!(is_length_limit(&e));
        let gone = futures_util::stream::iter([Err::<bytes::Bytes, _>(std::io::Error::other(
            "client gone",
        ))]);
        let e = to_bytes(Body::from_stream(gone), 4).await.unwrap_err();
        assert!(!is_length_limit(&e));
    }

    #[test]
    fn session_cookie_is_not_forwarded() {
        assert_eq!(
            plugin_cookies("a=1; mrsm-session-id=abc; b=2").as_deref(),
            Some("a=1; b=2")
        );
        assert_eq!(plugin_cookies("mrsm-session-id=abc"), None);
    }
}
