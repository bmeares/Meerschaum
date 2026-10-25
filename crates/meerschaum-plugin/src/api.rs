//! Native API routes: the `api_routes` manifest entries and the `api_route` call kind.
//!
//! A plugin declares its routes in its manifest entry (`plugins.<name>.api_routes`), so the
//! server mounts them without loading the library. Each request reaches the plugin as one
//! `api_route` call whose JSON context is a [`RouteRequest`]; the reply is a [`RouteResponse`].
//! Bodies are bytes and travel base64-encoded (`body_base64`) inside the JSON envelope, so the
//! v1 ABI is unchanged.
//!
//! ```json
//! {"name": "get_item", "method": "GET", "path": "/my/items/{id}", "scopes": ["pipes:read"]}
//! ```
//!
//! `method` is any HTTP method (`GET`, `POST`, ... or an extension method) or `*` for every
//! method. A path segment is static text, one parameter with an optional static prefix and
//! suffix (`{id}`, `{id}.json`, `v{n:int}`; the Starlette converters `str`, `int`, `float` and
//! `uuid` restrict the value, which still arrives as a string), or a final catch-all (`{*rest}`
//! or `{rest:path}`). When several routes of one path shape match a request, the first declared
//! wins. A route needs the API's credentials and every scope in `scopes`; `"public": true` needs
//! none and may not declare scopes. A route whose path would shadow a core route (a core route
//! group, `/dash`, `/_pages`, `/static`, `/webterm`, `/_mrsm`, or a parameter in the first
//! segment) is refused when the server starts.
//!
//! The server calls run concurrently (each on its own thread, at most 32 at once per plugin; more
//! wait up to 30 s and then answer `503`): a plugin's `api_route`, `mcp_tool`, `mcp_resource`
//! and `mcp_prompt` must be thread-safe. The SDK builds a fresh `P::default()` for each call.
use crate::{Result, error};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeMap;

/// One `api_routes` manifest entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteSpec {
    /// The route's name within the plugin, passed back as [`RouteRequest::name`].
    pub name: String,
    /// The HTTP method (case-insensitive) or `*` for any method.
    #[serde(default = "default_method")]
    pub method: String,
    /// The path, with `{param}` segments (optional prefix, suffix and converter) and an optional
    /// final `{*rest}`.
    pub path: String,
    /// The scopes a caller must hold (`*` holds all of them).
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Serve without credentials (the principal is still passed when the caller sent valid ones).
    /// A public route may not declare `scopes`.
    #[serde(default)]
    pub public: bool,
    /// A one-line description, for the plugin's own documentation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

fn default_method() -> String {
    "GET".into()
}

impl RouteSpec {
    /// The route `name` serving `method path`.
    pub fn new(
        name: impl Into<String>,
        method: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            method: method.into(),
            path: path.into(),
            scopes: Vec::new(),
            public: false,
            description: None,
        }
    }
    /// Require these scopes.
    pub fn scopes<S: Into<String>>(mut self, scopes: impl IntoIterator<Item = S>) -> Self {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }
    /// Serve without credentials.
    pub fn public(mut self) -> Self {
        self.public = true;
        self
    }
    /// The manifest entry.
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// The routes declared in a plugin's manifest entry (`plugins.<name>.api_routes`).
pub fn routes_of(entry: &Value) -> Result<Vec<RouteSpec>> {
    match entry.get("api_routes") {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(routes) => serde_json::from_value(routes.clone()).map_err(error),
    }
}

/// Who called a route or an MCP entry. `None` in a [`RouteRequest`] means the server runs with
/// `--no-auth`, the route is public and no credentials were sent, or (for MCP) the stdio transport.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    /// `user` or `token`.
    pub kind: String,
    /// The username of a user principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// The instance's user ID of the user (or of the token's owner), as JSON.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub user_id: Value,
    /// The user type (`admin`, ...) of a user principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_type: Option<String>,
    /// The token ID of a token principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    /// The token label of a token principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl Principal {
    pub fn is_user(&self) -> bool {
        self.kind == "user"
    }
    pub fn is_token(&self) -> bool {
        self.kind == "token"
    }
}

/// Raw bytes serialized as standard base64.
pub(crate) mod b64 {
    use super::*;
    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(bytes))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Vec<u8>, D::Error> {
        let text = Option::<String>::deserialize(d)?.unwrap_or_default();
        STANDARD
            .decode(text.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

/// The context of an `api_route` call: the whole HTTP request.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RouteRequest {
    /// The [`RouteSpec::name`] of the matched route.
    pub name: String,
    /// The request method (uppercase).
    pub method: String,
    /// The request path.
    pub path: String,
    /// The declared path (`/my/items/{id}`).
    #[serde(default)]
    pub route_path: String,
    /// The path parameters (`{"id": "42"}`), percent-decoded.
    #[serde(default)]
    pub path_params: BTreeMap<String, String>,
    /// The raw query string (without `?`).
    #[serde(default)]
    pub query: String,
    /// The query parameters, decoded, every value of each key in order.
    #[serde(default)]
    pub query_params: BTreeMap<String, Vec<String>>,
    /// The headers, names lowercase, every value of each name in order. `authorization` is removed
    /// (the server already authenticated the caller), as is the console's session cookie
    /// (`mrsm-session-id`) from `cookie`; [`RouteRequest::cookie`] reads the others.
    #[serde(default)]
    pub headers: BTreeMap<String, Vec<String>>,
    /// The body.
    #[serde(default, rename = "body_base64", with = "b64")]
    pub body: Vec<u8>,
    /// The authenticated principal.
    #[serde(default)]
    pub principal: Option<Principal>,
    /// The caller's granted scopes (`["*"]` under `--no-auth`; empty for an anonymous caller).
    #[serde(default)]
    pub scopes: Vec<String>,
}

impl RouteRequest {
    /// The path parameter `name`.
    pub fn path_param(&self, name: &str) -> Option<&str> {
        self.path_params.get(name).map(String::as_str)
    }
    /// The first value of the query parameter `name`.
    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query_params
            .get(name)
            .and_then(|v| v.first())
            .map(String::as_str)
    }
    /// The first value of the header `name` (any case).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .and_then(|v| v.first())
            .map(String::as_str)
    }
    /// The value of the request cookie `name`.
    pub fn cookie(&self, name: &str) -> Option<&str> {
        self.headers
            .get("cookie")?
            .iter()
            .flat_map(|h| h.split(';'))
            .filter_map(|pair| pair.trim().split_once('='))
            .find(|(k, _)| k.trim() == name)
            .map(|(_, v)| v.trim())
    }
    /// The body as UTF-8 text.
    pub fn text(&self) -> Result<&str> {
        std::str::from_utf8(&self.body).map_err(error)
    }
    /// The body parsed as JSON.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body).map_err(error)
    }
    /// Whether the caller holds `scope` (or `*`).
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == "*" || s == scope)
    }
}

/// The reply of an `api_route` call.
///
/// On the wire: `{"status": 200, "headers": {"name": ["value", ...]}, "body_base64": "..."}`.
/// The host also accepts `"json": <value>` (an `application/json` body), `"body": "<text>"`
/// (`text/plain; charset=utf-8` unless a header says otherwise), a `"content_type"` shorthand,
/// and header values given as one string.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteResponse {
    /// The status code (default 200).
    #[serde(default = "default_status")]
    pub status: u16,
    /// The headers, in order.
    #[serde(default, with = "header_pairs")]
    pub headers: Vec<(String, String)>,
    /// The body.
    #[serde(default, rename = "body_base64", with = "b64")]
    pub body: Vec<u8>,
}

fn default_status() -> u16 {
    200
}

mod header_pairs {
    use super::*;
    pub fn serialize<S: Serializer>(
        pairs: &[(String, String)],
        s: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut map: indexmap::IndexMap<&str, Vec<&str>> = indexmap::IndexMap::new();
        for (k, v) in pairs {
            map.entry(k.as_str()).or_default().push(v.as_str());
        }
        map.serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<Vec<(String, String)>, D::Error> {
        let value = Value::deserialize(d)?;
        Ok(super::header_pairs_of(&value))
    }
}

/// Header pairs from `{"name": "v" | ["v", ...]}` or `[["name", "v"], ...]`.
pub fn header_pairs_of(value: &Value) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let scalar = |v: &Value| match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                match v {
                    Value::Array(items) => out.extend(items.iter().map(|i| (k.clone(), scalar(i)))),
                    Value::Null => {}
                    other => out.push((k.clone(), scalar(other))),
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                if let Some([k, v]) = item.as_array().map(Vec::as_slice) {
                    out.push((scalar(k), scalar(v)));
                }
            }
        }
        _ => {}
    }
    out
}

impl Default for RouteResponse {
    fn default() -> Self {
        Self {
            status: 200,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }
}

impl RouteResponse {
    /// A `200` with `content_type` and `body`.
    pub fn bytes(content_type: &str, body: impl Into<Vec<u8>>) -> Self {
        Self::default()
            .header("content-type", content_type)
            .with_body(body)
    }
    /// A `200` `application/json` response.
    pub fn json(value: &impl Serialize) -> Self {
        Self::bytes(
            "application/json",
            serde_json::to_vec(value).unwrap_or_else(|_| b"null".to_vec()),
        )
    }
    /// A `200` `text/plain; charset=utf-8` response.
    pub fn text(text: impl Into<String>) -> Self {
        Self::bytes("text/plain; charset=utf-8", text.into().into_bytes())
    }
    /// An empty response with `status`.
    pub fn empty(status: u16) -> Self {
        Self::default().with_status(status)
    }
    /// A `{"detail": message}` JSON error, the shape the core API uses.
    pub fn error(status: u16, message: impl Into<String>) -> Self {
        Self::json(&serde_json::json!({"detail": message.into()})).with_status(status)
    }
    pub fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }
    /// Append a header (several values of one name are kept, e.g. `set-cookie`).
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
    pub fn with_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    /// Parse a reply, accepting the documented shorthands (`json`, `body`, `content_type`).
    pub fn from_reply(value: &Value) -> Result<Self> {
        if !value.is_object() {
            return Ok(Self::json(value));
        }
        let status = match value.get("status") {
            None | Some(Value::Null) => 200,
            Some(s) => s
                .as_u64()
                .and_then(|s| u16::try_from(s).ok())
                .filter(|s| (100..=999).contains(s))
                .ok_or_else(|| error(format!("invalid route status: {s}")))?,
        };
        let mut headers = value
            .get("headers")
            .map(header_pairs_of)
            .unwrap_or_default();
        let has_type = |h: &[(String, String)]| {
            h.iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        };
        if let Some(ct) = value.get("content_type").and_then(Value::as_str)
            && !has_type(&headers)
        {
            headers.push(("content-type".into(), ct.into()));
        }
        let body = if let Some(json) = value.get("json") {
            if !has_type(&headers) {
                headers.push(("content-type".into(), "application/json".into()));
            }
            serde_json::to_vec(json).map_err(error)?
        } else if let Some(b64) = value.get("body_base64").and_then(Value::as_str) {
            STANDARD.decode(b64.as_bytes()).map_err(error)?
        } else if let Some(text) = value.get("body").and_then(Value::as_str) {
            if !has_type(&headers) {
                headers.push(("content-type".into(), "text/plain; charset=utf-8".into()));
            }
            text.as_bytes().to_vec()
        } else {
            Vec::new()
        };
        Ok(Self {
            status,
            headers,
            body,
        })
    }
}

/// Return types accepted from a route handler: a [`RouteResponse`], a JSON `Value` (served as
/// `application/json`), a `String` (`text/plain`), `(u16, T)` to set the status, or any of these
/// in a `Result` (an `Err` is a `500 {"detail": message}`).
pub trait IntoRouteResponse {
    fn into_route_response(self) -> Result<RouteResponse>;
}
impl IntoRouteResponse for RouteResponse {
    fn into_route_response(self) -> Result<RouteResponse> {
        Ok(self)
    }
}
impl IntoRouteResponse for Value {
    fn into_route_response(self) -> Result<RouteResponse> {
        Ok(RouteResponse::json(&self))
    }
}
impl IntoRouteResponse for String {
    fn into_route_response(self) -> Result<RouteResponse> {
        Ok(RouteResponse::text(self))
    }
}
impl IntoRouteResponse for &str {
    fn into_route_response(self) -> Result<RouteResponse> {
        Ok(RouteResponse::text(self))
    }
}
impl IntoRouteResponse for Vec<u8> {
    fn into_route_response(self) -> Result<RouteResponse> {
        Ok(RouteResponse::bytes("application/octet-stream", self))
    }
}
impl<T: IntoRouteResponse> IntoRouteResponse for (u16, T) {
    fn into_route_response(self) -> Result<RouteResponse> {
        Ok(self.1.into_route_response()?.with_status(self.0))
    }
}
impl<T: IntoRouteResponse> IntoRouteResponse for Result<T> {
    fn into_route_response(self) -> Result<RouteResponse> {
        match self {
            Ok(value) => value.into_route_response(),
            Err(crate::Error::Unsupported(m)) => Err(crate::Error::Unsupported(m)),
            Err(e) => Ok(RouteResponse::error(500, e.to_string())),
        }
    }
}

/// Whether the server will mount a native route at `path`: `Err` names the reason (bad syntax, or
/// the core route it would shadow), in the words the server uses when it refuses the route at
/// startup. Single-file plugins run it when they build, so the error points at the source line.
///
/// It mirrors `meerschaum_server::native::check_route_path`, which this crate cannot depend on;
/// a `meerschaum-cli` test checks that both agree.
pub fn check_route_path(path: &str) -> std::result::Result<(), String> {
    fn identifier(n: &str) -> bool {
        let mut chars = n.chars();
        matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }
    if !path.starts_with('/') || path.starts_with("//") {
        return Err("the path must start with a single '/'".into());
    }
    if path.contains(['?', '#', '%', '\\', ' ']) {
        return Err("the path may not contain '?', '#', '%', '\\' or spaces".into());
    }
    let raw: Vec<&str> = path[1..].split('/').collect();
    let mut names = std::collections::HashSet::new();
    for (i, segment) in raw.iter().enumerate() {
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
            return Err("a parameter in the first segment would shadow every core route".into());
        }
        let (name, catch_all) = match inner.strip_prefix('*') {
            Some(name) => (name, true),
            None => match inner.split_once(':') {
                None => (inner, false),
                Some((name, "str" | "int" | "float" | "uuid")) => (name, false),
                Some((name, "path")) => (name, true),
                Some((_, other)) => {
                    return Err(format!(
                        "segment '{segment}': unknown converter '{other}' \
                         (str, int, float, uuid or path)"
                    ));
                }
            },
        };
        if !identifier(name) {
            return Err(format!(
                "segment '{segment}': '{name}' is not a valid parameter name"
            ));
        }
        if !names.insert(name.to_string()) {
            return Err(format!(
                "segment '{segment}': the parameter '{name}' appears twice"
            ));
        }
        if catch_all {
            if !prefix.is_empty() || !suffix.is_empty() {
                return Err(format!(
                    "segment '{segment}': a catch-all must be the whole segment"
                ));
            }
            if i + 1 != raw.len() {
                return Err(format!(
                    "segment '{segment}': a catch-all must be the last segment"
                ));
            }
        }
    }
    let first = raw[0];
    // The server's route groups (`meerschaum_server::routes::route_group_of`).
    let group = match first {
        "" => "/",
        "pipes" | "metadata" => "pipes",
        "pipelines" => "pipelines",
        "actions" | "execute" => "actions",
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
        "webterm" | "websocket" => "webterm",
        "_mrsm" => "internal",
        _ if first.starts_with("jobs") => "jobs",
        _ => "",
    };
    if !group.is_empty() {
        return Err(format!("it would shadow the core route group '{group}'"));
    }
    match first {
        "dash" => Err("it would shadow the web console ('/dash')".into()),
        "_pages" => Err("it would shadow the console's page routes ('/_pages')".into()),
        "static" => Err("it would shadow the static files ('/static')".into()),
        _ if first.starts_with("_mrsm") => {
            Err(format!("it would shadow the core route '/{first}'"))
        }
        _ => Ok(()),
    }
}

impl crate::LoadedPlugin {
    /// Serve a request with the plugin's `api_route` call kind.
    pub fn api_route(&self, request: &RouteRequest) -> Result<RouteResponse> {
        let context = serde_json::to_value(request).map_err(error)?;
        RouteResponse::from_reply(&self.call_json_concurrent("api_route", context)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_round_trips_bytes_and_lookups() {
        let request = RouteRequest {
            name: "item".into(),
            method: "POST".into(),
            path: "/x/42".into(),
            path_params: [("id".to_string(), "42".to_string())].into(),
            query_params: [("a".to_string(), vec!["1".into(), "2".into()])].into(),
            headers: [("x-key".to_string(), vec!["v".into()])].into(),
            body: vec![0, 159, 146, 150],
            ..Default::default()
        };
        let wire = serde_json::to_value(&request).unwrap();
        assert_eq!(wire["body_base64"], "AJ+Slg==");
        let back: RouteRequest = serde_json::from_value(wire).unwrap();
        assert_eq!(back, request);
        assert_eq!(back.path_param("id"), Some("42"));
        assert_eq!(back.query_param("a"), Some("1"));
        assert_eq!(back.header("X-Key"), Some("v"));
        let cookies = RouteRequest {
            headers: [("cookie".to_string(), vec!["a=1; theme=dark".into()])].into(),
            ..Default::default()
        };
        assert_eq!(cookies.cookie("theme"), Some("dark"));
        assert_eq!(cookies.cookie("b"), None);
        assert!(back.text().is_err());
    }

    #[test]
    fn response_shorthands() {
        let r = RouteResponse::from_reply(&json!({"status": 201, "json": {"a": 1}})).unwrap();
        assert_eq!(
            (r.status, r.body.as_slice()),
            (201, br#"{"a":1}"#.as_slice())
        );
        assert_eq!(
            r.headers,
            vec![("content-type".into(), "application/json".into())]
        );
        let r = RouteResponse::from_reply(
            &json!({"body": "hi", "content_type": "text/html", "headers": {"set-cookie": ["a=1", "b=2"]}}),
        )
        .unwrap();
        assert_eq!(r.headers.len(), 3);
        assert_eq!(r.body, b"hi");
        let typed = RouteResponse::text("x")
            .header("set-cookie", "a")
            .header("set-cookie", "b");
        let wire = serde_json::to_value(&typed).unwrap();
        assert_eq!(wire["headers"]["set-cookie"], json!(["a", "b"]));
        assert_eq!(RouteResponse::from_reply(&wire).unwrap(), typed);
        assert!(RouteResponse::from_reply(&json!({"status": 70000})).is_err());
    }

    #[test]
    fn route_specs_parse_with_defaults() {
        let routes = routes_of(&json!({"api_routes": [{"name": "a", "path": "/a/{id}"}]})).unwrap();
        assert_eq!(routes[0].method, "GET");
        assert!(routes[0].scopes.is_empty() && !routes[0].public);
        assert!(routes_of(&json!({})).unwrap().is_empty());
    }
}
