// Embedded in generated single-file crates. Depends only on Serde and serde_json, never the host SDK.
use crate::abi::*;
use crate::rows_abi::RowsHostV1;
use serde_json::{Value, json};
use std::{marker::PhantomData, rc::Rc};

#[derive(Debug)]
pub enum Error {
    Message(String),
    Unsupported(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(s) | Self::Unsupported(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
fn error(e: impl std::fmt::Display) -> Error {
    Error::Message(e.to_string())
}
#[derive(Debug, Clone, PartialEq)]
pub enum Bound {
    Int(i64),
    Datetime(String),
}
impl serde::Serialize for Bound {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Int(n) => serializer.serialize_i64(*n),
            Self::Datetime(s) => serializer.serialize_str(s),
        }
    }
}
impl<'de> serde::Deserialize<'de> for Bound {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        bound(&value)
            .map_err(serde::de::Error::custom)?
            .ok_or_else(|| serde::de::Error::custom("null bound"))
    }
}
fn bound(v: &Value) -> Result<Option<Bound>> {
    match v {
        Value::Null => Ok(None),
        Value::String(s) if s.is_empty() => Ok(None),
        Value::String(s) => Ok(Some(Bound::Datetime(s.clone()))),
        _ => v
            .as_i64()
            .map(|n| Some(Bound::Int(n)))
            .ok_or_else(|| error("invalid bound")),
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SuccessTuple {
    pub success: bool,
    pub message: String,
}
impl serde::Serialize for SuccessTuple {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        (&self.success, &self.message).serialize(serializer)
    }
}
impl<'de> serde::Deserialize<'de> for SuccessTuple {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let (success, message) = <(bool, String)>::deserialize(deserializer)?;
        Ok(Self { success, message })
    }
}
impl SuccessTuple {
    pub fn success() -> Self {
        Self {
            success: true,
            message: "Success".into(),
        }
    }
    pub fn ok(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: message.into(),
        }
    }
    pub fn as_tuple(&self) -> (bool, &str) {
        (self.success, &self.message)
    }
    pub fn fail(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: message.into(),
        }
    }
}
/// Rows of JSON objects. Typed Arrow frames belong to the full Cargo SDK.
#[derive(Debug, Clone)]
pub struct Frame {
    rows: Vec<Value>,
}
impl Frame {
    pub fn from_rows(rows: Vec<Value>) -> Result<Self> {
        if rows.iter().any(|v| !v.is_object()) {
            return Err(error("rows must be JSON objects"));
        }
        Ok(Self { rows })
    }
    pub fn height(&self) -> usize {
        self.rows.len()
    }
    pub fn rows(&self) -> &[Value] {
        &self.rows
    }
    pub fn into_rows(self) -> Vec<Value> {
        self.rows
    }
}
pub struct Request {
    pub method: String,
    pub connector_type: String,
    pub label: String,
    pub config: Value,
    pub context: Value,
    pub(crate) kwargs: Value,
}
impl Request {
    pub fn connector(&self) -> Connector<'_> {
        Connector { request: self }
    }
    pub fn kwargs(&self) -> &Value {
        &self.kwargs
    }
    pub fn action(&self) -> Result<Vec<String>> {
        let value = &self.kwargs()["action"];
        if value.is_null() {
            return Ok(Vec::new());
        }
        serde_json::from_value(value.clone()).map_err(error)
    }
    /// A `begin` / `end` bound, wherever the host put it: top-level (a plugin's `sync`),
    /// `context.query` (`fetch`, `get_pipe_data`, `clear_pipe`, ...) or `context.options`
    /// (`sync_pipe`). See [`Request::field`].
    pub fn bound(&self, name: &str) -> Result<Option<Bound>> {
        bound(self.field(name))
    }
    /// A field of an instance call's context: `context[name]`, else `context.query[name]`, else
    /// `context.options[name]` (`null` when none has it).
    pub fn field(&self, name: &str) -> &Value {
        [
            &self.context[name],
            &self.context["query"][name],
            &self.context["options"][name],
        ]
        .into_iter()
        .find(|v| !v.is_null())
        .unwrap_or(&Value::Null)
    }
    pub fn pipe(&self) -> Result<Pipe<'_>> {
        Pipe::from_request(self)
    }
    /// The sync result a post-sync hook receives.
    pub fn success_tuple(&self) -> Result<SuccessTuple> {
        serde_json::from_value(self.kwargs()["success_tuple"].clone()).map_err(error)
    }
    /// The seconds a sync took, for post-sync hooks.
    pub fn sync_duration(&self) -> f64 {
        self.kwargs()["sync_duration"].as_f64().unwrap_or_default()
    }
}
/// The resolved connector for this call; no constructor or owned state is needed.
pub struct Connector<'a> {
    request: &'a Request,
}
impl Connector<'_> {
    pub fn connector_type(&self) -> &str {
        &self.request.connector_type
    }
    pub fn label(&self) -> &str {
        &self.request.label
    }
    pub fn config(&self) -> &Value {
        &self.request.config
    }
}
pub struct Reply {
    pub value: Value,
    pub data: Option<Frame>,
}
impl Reply {
    pub fn json(value: Value) -> Self {
        Self { value, data: None }
    }
    pub fn data(data: Option<Frame>) -> Self {
        Self {
            value: Value::Null,
            data,
        }
    }
}
#[derive(Clone, Copy)]
struct ActiveHost {
    table: RowsHostV1,
    generation: u64,
}
thread_local! {
    static HOST: std::cell::Cell<Option<ActiveHost>> = const { std::cell::Cell::new(None) };
    static GENERATION: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
struct Scope(Option<ActiveHost>);
impl Drop for Scope {
    fn drop(&mut self) {
        HOST.with(|h| h.set(self.0));
    }
}
pub struct Pipe<'a> {
    metadata: &'a Value,
    generation: Option<u64>,
    _thread: PhantomData<Rc<()>>,
}
impl<'a> Pipe<'a> {
    fn from_request(request: &'a Request) -> Result<Self> {
        // Sync hooks receive the Pipe in `context.pipe` (older hosts sent it among kwargs).
        let metadata = request
            .context
            .get("pipe")
            .or_else(|| request.kwargs.get("pipe"))
            .filter(|v| v.is_object())
            .ok_or_else(|| error("request has no Pipe metadata"))?;
        Ok(Self {
            metadata,
            generation: HOST.with(|h| h.get().map(|h| h.generation)),
            _thread: PhantomData,
        })
    }
    pub fn keys(&self) -> &Value {
        self.metadata.get("keys").unwrap_or(self.metadata)
    }
    pub fn parameters(&self) -> &Value {
        &self.metadata["parameters"]
    }
    pub fn connector_keys(&self) -> &str {
        self.keys()["connector_keys"].as_str().unwrap_or("")
    }
    pub fn metric_key(&self) -> &str {
        self.keys()["metric_key"].as_str().unwrap_or("")
    }
    pub fn location_key(&self) -> Option<&str> {
        self.keys()["location_key"].as_str()
    }
    pub fn instance_keys(&self) -> &str {
        self.keys()["instance_keys"].as_str().unwrap_or("")
    }
    pub fn get_sync_time(&self) -> Result<Option<Bound>> {
        bound(&self.call("get_sync_time", None)?)
    }
    pub fn sync(&self, data: impl sdk::FetchOutput) -> Result<SuccessTuple> {
        let v = self.call("sync", data.into_frame()?)?;
        Ok(SuccessTuple {
            success: v[0]
                .as_bool()
                .ok_or_else(|| error("invalid host success"))?,
            message: v[1]
                .as_str()
                .ok_or_else(|| error("invalid host message"))?
                .into(),
        })
    }
    fn call(&self, method: &str, data: Option<Frame>) -> Result<Value> {
        let host = HOST
            .with(|h| h.get())
            .filter(|h| Some(h.generation) == self.generation)
            .ok_or_else(|| {
                error("Pipe host operations are unavailable outside a host-enabled plugin call")
            })?
            .table;
        let bytes =
            serde_json::to_vec(&json!({"method": method, "data": data.map(Frame::into_rows)}))
                .map_err(error)?;
        // SAFETY: thread-bound scope pins callback context; borrowed bytes live through the call.
        let result = unsafe { (host.invoke)(host.context, Bytes::borrowed(&bytes)) };
        // SAFETY: the host owns and releases this allocation exactly once.
        unsafe { decode(host.free_bytes, result) }
    }
}
pub trait Plugin: Default {
    const NAME: &'static str;
    const VERSION: &'static str;
    fn manifest(&self) -> Value;
    fn call(&self, request: &Request, data: Option<Frame>) -> Result<Reply> {
        match request.method.as_str() {
            "fetch" => self.fetch(request).map(Reply::data),
            "sync" | "sync_pipe" => self.sync(request, data).map(Reply::json),
            _ => Err(Error::Unsupported(request.method.clone())),
        }
    }
    fn fetch(&self, _: &Request) -> Result<Option<Frame>> {
        Err(Error::Unsupported("fetch is not implemented".into()))
    }
    fn sync(&self, _: &Request, _: Option<Frame>) -> Result<Value> {
        Err(Error::Unsupported("sync is not implemented".into()))
    }
}
pub mod sdk {
    use super::*;
    pub mod fetch_prelude {
        pub use super::super::api::{Principal, RouteRequest, RouteResponse};
        pub use super::super::executor::{ExecutorInfo, LogEvent, LogStream};
        pub use super::super::mcp::{PromptGet, ResourceContents, ResourceRead, ToolCall};
        pub use super::super::page::PageRequest;
        pub use super::super::subscribe::{Batch, ChannelSubscription, Subscription};
        pub use super::super::{Bound, Connector, Error, Frame, Pipe, Result, SuccessTuple};
        pub use serde_json::{Value, json};
        pub use std::time::Duration;
    }
    pub trait FetchOutput {
        fn into_frame(self) -> Result<Option<Frame>>;
    }
    impl FetchOutput for Vec<Value> {
        fn into_frame(self) -> Result<Option<Frame>> {
            if self.is_empty() {
                Ok(None)
            } else {
                Frame::from_rows(self).map(Some)
            }
        }
    }
    impl FetchOutput for Frame {
        fn into_frame(self) -> Result<Option<Frame>> {
            Ok(Some(self))
        }
    }
    impl FetchOutput for Option<Frame> {
        fn into_frame(self) -> Result<Option<Frame>> {
            Ok(self)
        }
    }
    impl<T: FetchOutput> FetchOutput for Result<T> {
        fn into_frame(self) -> Result<Option<Frame>> {
            self?.into_frame()
        }
    }
    pub trait SyncOutput {
        fn into_success(self) -> Result<Value>;
    }
    impl SyncOutput for SuccessTuple {
        fn into_success(self) -> Result<Value> {
            Ok(json!([self.success, self.message]))
        }
    }
    impl<S: Into<String>> SyncOutput for (bool, S) {
        fn into_success(self) -> Result<Value> {
            SuccessTuple {
                success: self.0,
                message: self.1.into(),
            }
            .into_success()
        }
    }
    impl<T: SyncOutput> SyncOutput for Result<T> {
        fn into_success(self) -> Result<Value> {
            self?.into_success()
        }
    }
    pub trait ActionOutput {
        fn into_action(self) -> Result<Value>;
    }
    impl ActionOutput for () {
        fn into_action(self) -> Result<Value> {
            Ok(json!([true, "Success"]))
        }
    }
    impl ActionOutput for SuccessTuple {
        fn into_action(self) -> Result<Value> {
            SyncOutput::into_success(self)
        }
    }
    impl<S: Into<String>> ActionOutput for (bool, S) {
        fn into_action(self) -> Result<Value> {
            SyncOutput::into_success(self)
        }
    }
    impl<T: ActionOutput> ActionOutput for Result<T> {
        fn into_action(self) -> Result<Value> {
            self?.into_action()
        }
    }
    /// `setup` answers like Python's `Plugin.setup`: a bool or a success tuple.
    pub trait SetupOutput {
        fn into_setup(self) -> Result<Value>;
    }
    impl SetupOutput for bool {
        fn into_setup(self) -> Result<Value> {
            Ok(json!(self))
        }
    }
    impl SetupOutput for SuccessTuple {
        fn into_setup(self) -> Result<Value> {
            SyncOutput::into_success(self)
        }
    }
    impl<S: Into<String>> SetupOutput for (bool, S) {
        fn into_setup(self) -> Result<Value> {
            SyncOutput::into_success(self)
        }
    }
    impl<T: SetupOutput> SetupOutput for Result<T> {
        fn into_setup(self) -> Result<Value> {
            self?.into_setup()
        }
    }
    pub trait RegisterOutput {
        fn into_parameters(self) -> Result<Value>;
    }
    impl RegisterOutput for Value {
        fn into_parameters(self) -> Result<Value> {
            if self.is_null() || self.is_object() {
                Ok(self)
            } else {
                Err(error("register must return a JSON object or null"))
            }
        }
    }
    impl RegisterOutput for Option<Value> {
        fn into_parameters(self) -> Result<Value> {
            self.unwrap_or(Value::Null).into_parameters()
        }
    }
    impl<T: RegisterOutput> RegisterOutput for Result<T> {
        fn into_parameters(self) -> Result<Value> {
            self?.into_parameters()
        }
    }
}
/// Standard base64 with padding (route and resource bodies travel base64-encoded in JSON).
pub(crate) mod base64 {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    pub fn encode(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
        for chunk in bytes.chunks(3) {
            let n = (u32::from(chunk[0]) << 16)
                | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
                | u32::from(*chunk.get(2).unwrap_or(&0));
            for i in 0..4 {
                if i <= chunk.len() {
                    out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
                } else {
                    out.push('=');
                }
            }
        }
        out
    }
    pub fn decode(text: &str) -> super::Result<Vec<u8>> {
        let mut out = Vec::with_capacity(text.len() / 4 * 3);
        let (mut buffer, mut bits) = (0u32, 0);
        for c in text
            .bytes()
            .filter(|c| !c.is_ascii_whitespace() && *c != b'=')
        {
            let value = ALPHABET
                .iter()
                .position(|a| *a == c)
                .ok_or_else(|| super::error("invalid base64"))?;
            buffer = (buffer << 6) | value as u32;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((buffer >> bits) as u8);
            }
        }
        Ok(out)
    }
}
fn string_map(value: &Value) -> std::collections::BTreeMap<String, String> {
    value
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
        .collect()
}
fn list_map(value: &Value) -> std::collections::BTreeMap<String, Vec<String>> {
    value
        .as_object()
        .into_iter()
        .flatten()
        .map(|(k, v)| {
            let items = match v {
                Value::Array(items) => items
                    .iter()
                    .filter_map(|i| i.as_str().map(str::to_owned))
                    .collect(),
                Value::String(s) => vec![s.clone()],
                _ => Vec::new(),
            };
            (k.clone(), items)
        })
        .collect()
}
fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect()
}
/// API routes (`#[api_route]`): the same request and response as the packaged SDK's `api` module.
pub mod api {
    use super::*;
    use std::collections::BTreeMap;
    /// Who called a route or an MCP entry (`None`: no credentials, `--no-auth` or stdio).
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct Principal {
        /// `user` or `token`.
        pub kind: String,
        pub username: Option<String>,
        pub user_id: Value,
        pub user_type: Option<String>,
        pub token_id: Option<String>,
        pub label: Option<String>,
    }
    impl Principal {
        pub(crate) fn from_json(value: &Value) -> Option<Self> {
            let text = |key: &str| value[key].as_str().map(str::to_owned);
            Some(Self {
                kind: value.get("kind")?.as_str()?.to_owned(),
                username: text("username"),
                user_id: value["user_id"].clone(),
                user_type: text("user_type"),
                token_id: text("token_id"),
                label: text("label"),
            })
        }
        pub fn is_user(&self) -> bool {
            self.kind == "user"
        }
        pub fn is_token(&self) -> bool {
            self.kind == "token"
        }
        /// The principal as the host sent it.
        pub fn to_json(&self) -> Value {
            let mut value = json!({"kind": self.kind});
            for (key, item) in [
                ("username", &self.username),
                ("user_type", &self.user_type),
                ("token_id", &self.token_id),
                ("label", &self.label),
            ] {
                if let Some(item) = item {
                    value[key] = json!(item);
                }
            }
            if !self.user_id.is_null() {
                value["user_id"] = self.user_id.clone();
            }
            value
        }
    }
    impl serde::Serialize for Principal {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            self.to_json().serialize(serializer)
        }
    }
    pub(crate) fn principal_of(context: &Value) -> Option<Principal> {
        Principal::from_json(&context["principal"])
    }
    /// The whole HTTP request of an `#[api_route]` call.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct RouteRequest {
        /// The route's name (the function's name unless `name = "..."`).
        pub name: String,
        /// The request method (uppercase).
        pub method: String,
        pub path: String,
        /// The declared path (`/my/items/{id}`).
        pub route_path: String,
        /// Percent-decoded path parameters.
        pub path_params: BTreeMap<String, String>,
        /// The raw query string (without `?`).
        pub query: String,
        pub query_params: BTreeMap<String, Vec<String>>,
        /// Lowercase names; `authorization` and the console's session cookie are removed.
        pub headers: BTreeMap<String, Vec<String>>,
        pub body: Vec<u8>,
        pub principal: Option<Principal>,
        /// The caller's scopes (`["*"]` under `--no-auth`).
        pub scopes: Vec<String>,
    }
    impl RouteRequest {
        pub fn from_context(context: &Value) -> Result<Self> {
            let text = |key: &str| context[key].as_str().unwrap_or_default().to_owned();
            Ok(Self {
                name: text("name"),
                method: text("method"),
                path: text("path"),
                route_path: text("route_path"),
                path_params: string_map(&context["path_params"]),
                query: text("query"),
                query_params: list_map(&context["query_params"]),
                headers: list_map(&context["headers"]),
                body: base64::decode(context["body_base64"].as_str().unwrap_or_default())?,
                principal: principal_of(context),
                scopes: strings(&context["scopes"]),
            })
        }
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
        pub fn cookie(&self, name: &str) -> Option<&str> {
            self.headers
                .get("cookie")?
                .iter()
                .flat_map(|h| h.split(';'))
                .filter_map(|pair| pair.trim().split_once('='))
                .find(|(k, _)| k.trim() == name)
                .map(|(_, v)| v.trim())
        }
        pub fn text(&self) -> Result<&str> {
            std::str::from_utf8(&self.body).map_err(error)
        }
        pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
            serde_json::from_slice(&self.body).map_err(error)
        }
        /// Whether the caller holds `scope` (or `*`).
        pub fn has_scope(&self, scope: &str) -> bool {
            self.scopes.iter().any(|s| s == "*" || s == scope)
        }
    }
    /// The reply of an `#[api_route]` function.
    #[derive(Debug, Clone, PartialEq)]
    pub struct RouteResponse {
        pub status: u16,
        pub headers: Vec<(String, String)>,
        pub body: Vec<u8>,
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
        pub fn bytes(content_type: &str, body: impl Into<Vec<u8>>) -> Self {
            Self::default()
                .header("content-type", content_type)
                .with_body(body)
        }
        pub fn json(value: &impl serde::Serialize) -> Self {
            Self::bytes(
                "application/json",
                serde_json::to_vec(value).unwrap_or_else(|_| b"null".to_vec()),
            )
        }
        pub fn text(text: impl Into<String>) -> Self {
            Self::bytes("text/plain; charset=utf-8", text.into().into_bytes())
        }
        pub fn empty(status: u16) -> Self {
            Self::default().with_status(status)
        }
        /// A `{"detail": message}` JSON error, the shape the core API uses.
        pub fn error(status: u16, message: impl Into<String>) -> Self {
            Self::json(&json!({"detail": message.into()})).with_status(status)
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
        /// The wire reply: `{"status", "headers": {name: [values]}, "body_base64"}`.
        pub fn to_json(&self) -> Value {
            let mut headers = serde_json::Map::new();
            for (name, value) in &self.headers {
                let entry = headers.entry(name.clone()).or_insert_with(|| json!([]));
                if let Some(values) = entry.as_array_mut() {
                    values.push(json!(value));
                }
            }
            json!({"status": self.status, "headers": headers,
                "body_base64": base64::encode(&self.body)})
        }
    }
    /// What an `#[api_route]` function may return: a [`RouteResponse`], a JSON `Value`
    /// (`application/json`), a `String` / `&str` (`text/plain`), `Vec<u8>`
    /// (`application/octet-stream`), `(u16, T)` to set the status, or any of these in a `Result`
    /// (an `Err` is a `500 {"detail": message}`).
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
                Err(Error::Unsupported(m)) => Err(Error::Unsupported(m)),
                Err(e) => Ok(RouteResponse::error(500, e.to_string())),
            }
        }
    }
}
/// MCP tools, resources and prompts (`#[mcp_tool]`, `#[mcp_resource]`, `#[mcp_prompt]`).
pub mod mcp {
    use super::api::{Principal, principal_of};
    use super::*;
    type Map = serde_json::Map<String, Value>;
    fn object(value: &Value) -> Map {
        value.as_object().cloned().unwrap_or_default()
    }
    /// An `#[mcp_tool]` call; the arguments are already validated against the input schema.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct ToolCall {
        pub name: String,
        pub arguments: Map,
        pub principal: Option<Principal>,
        pub scopes: Vec<String>,
    }
    impl ToolCall {
        pub fn from_context(context: &Value) -> Self {
            Self {
                name: context["name"].as_str().unwrap_or_default().to_owned(),
                arguments: object(&context["arguments"]),
                principal: principal_of(context),
                scopes: strings(&context["scopes"]),
            }
        }
        /// Deserialize the arguments into a typed value.
        pub fn arguments<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
            serde_json::from_value(Value::Object(self.arguments.clone())).map_err(error)
        }
        pub fn argument(&self, name: &str) -> Option<&Value> {
            self.arguments.get(name)
        }
    }
    /// An `#[mcp_resource]` read.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct ResourceRead {
        pub name: String,
        pub uri: String,
        /// The URI template's variables (`{}` for a static URI).
        pub variables: Map,
        pub principal: Option<Principal>,
        pub scopes: Vec<String>,
    }
    impl ResourceRead {
        pub fn from_context(context: &Value) -> Self {
            Self {
                name: context["name"].as_str().unwrap_or_default().to_owned(),
                uri: context["uri"].as_str().unwrap_or_default().to_owned(),
                variables: object(&context["variables"]),
                principal: principal_of(context),
                scopes: strings(&context["scopes"]),
            }
        }
        pub fn variable(&self, name: &str) -> Option<&str> {
            self.variables.get(name).and_then(Value::as_str)
        }
    }
    /// What a resource read returns.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum ResourceContents {
        Text {
            text: String,
            mime_type: Option<String>,
        },
        Blob {
            bytes: Vec<u8>,
            mime_type: Option<String>,
        },
    }
    impl ResourceContents {
        pub fn text(text: impl Into<String>) -> Self {
            Self::Text {
                text: text.into(),
                mime_type: None,
            }
        }
        /// Pretty JSON text (`application/json`).
        pub fn json(value: &impl serde::Serialize) -> Self {
            Self::Text {
                text: serde_json::to_string_pretty(value).unwrap_or_default(),
                mime_type: Some("application/json".into()),
            }
        }
        pub fn blob(bytes: impl Into<Vec<u8>>, mime_type: impl Into<String>) -> Self {
            Self::Blob {
                bytes: bytes.into(),
                mime_type: Some(mime_type.into()),
            }
        }
        pub fn to_json(&self) -> Value {
            match self {
                Self::Text { text, mime_type } => json!({"text": text, "mime_type": mime_type}),
                Self::Blob { bytes, mime_type } => {
                    json!({"blob_base64": base64::encode(bytes), "mime_type": mime_type})
                }
            }
        }
    }
    /// An `#[mcp_prompt]` request (MCP prompt arguments are strings).
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct PromptGet {
        pub name: String,
        pub arguments: Map,
        pub principal: Option<Principal>,
        pub scopes: Vec<String>,
    }
    impl PromptGet {
        pub fn from_context(context: &Value) -> Self {
            Self {
                name: context["name"].as_str().unwrap_or_default().to_owned(),
                arguments: object(&context["arguments"]),
                principal: principal_of(context),
                scopes: strings(&context["scopes"]),
            }
        }
        pub fn argument(&self, name: &str) -> Option<&str> {
            self.arguments.get(name).and_then(Value::as_str)
        }
    }
    /// A tool returns any serializable value in a `Result`, a JSON `Value` or a `String`.
    pub trait IntoToolResult {
        fn into_tool_result(self) -> Result<Value>;
    }
    impl<T: serde::Serialize> IntoToolResult for Result<T> {
        fn into_tool_result(self) -> Result<Value> {
            self.and_then(|v| serde_json::to_value(v).map_err(error))
        }
    }
    impl IntoToolResult for Value {
        fn into_tool_result(self) -> Result<Value> {
            Ok(self)
        }
    }
    impl IntoToolResult for String {
        fn into_tool_result(self) -> Result<Value> {
            Ok(Value::String(self))
        }
    }
    /// A resource returns [`ResourceContents`], a `String` (text) or a JSON `Value`.
    pub trait IntoResourceContents {
        fn into_resource_contents(self) -> Result<Value>;
    }
    impl IntoResourceContents for ResourceContents {
        fn into_resource_contents(self) -> Result<Value> {
            Ok(self.to_json())
        }
    }
    impl IntoResourceContents for String {
        fn into_resource_contents(self) -> Result<Value> {
            Ok(ResourceContents::text(self).to_json())
        }
    }
    impl IntoResourceContents for Value {
        fn into_resource_contents(self) -> Result<Value> {
            Ok(self)
        }
    }
    impl<T: IntoResourceContents> IntoResourceContents for Result<T> {
        fn into_resource_contents(self) -> Result<Value> {
            self?.into_resource_contents()
        }
    }
    /// A prompt message with a role (`"user"` or `"assistant"`), as the packaged SDK's
    /// `mcp::message`.
    pub fn message(role: &str, text: impl Into<String>) -> Value {
        json!({"role": role, "content": {"type": "text", "text": text.into()}})
    }
    /// A prompt returns a `String` (one user message), messages (`Vec<Value>`; strings are user
    /// messages) or `{"description"?, "messages"}` as a JSON `Value`.
    pub trait IntoPromptReply {
        fn into_prompt_reply(self) -> Result<Value>;
    }
    impl IntoPromptReply for String {
        fn into_prompt_reply(self) -> Result<Value> {
            Ok(Value::String(self))
        }
    }
    impl IntoPromptReply for Value {
        fn into_prompt_reply(self) -> Result<Value> {
            Ok(self)
        }
    }
    impl IntoPromptReply for Vec<Value> {
        fn into_prompt_reply(self) -> Result<Value> {
            Ok(Value::Array(self))
        }
    }
    impl<T: IntoPromptReply> IntoPromptReply for Result<T> {
        fn into_prompt_reply(self) -> Result<Value> {
            self?.into_prompt_reply()
        }
    }
}
/// Job executors (`#[make_executor]`): the generated module answers `executor.<method>`; methods
/// it leaves out are unsupported, except those the packaged SDK derives from others.
pub mod executor {
    use super::*;
    use std::time::Duration;
    /// The executor `<executor_type>:<label>` a call is for.
    #[derive(Debug, Clone, Default)]
    pub struct ExecutorInfo {
        pub executor_type: String,
        pub label: String,
        /// The connector configuration of `<type>:<label>`.
        pub config: Value,
        /// The Meerschaum root the jobs belong to (a compose project's root may differ from
        /// this process's `MRSM_ROOT_DIR`).
        pub root_dir: Option<String>,
        /// The command that runs the host's Meerschaum (`["/path/mrsm"]` or
        /// `["/path/python", "-P", "-m", "meerschaum"]`): run a job as `mrsm + sysargs`.
        pub mrsm: Vec<String>,
    }
    impl ExecutorInfo {
        fn from_request(request: &Request) -> Self {
            let mut mrsm = strings(&request.context["mrsm"]);
            if mrsm.is_empty() {
                mrsm.push(
                    std::env::current_exe()
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_else(|_| "mrsm".into()),
                );
            }
            Self {
                executor_type: request.connector_type.clone(),
                label: request.label.clone(),
                config: request.config.clone(),
                root_dir: request.context["root_dir"].as_str().map(str::to_owned),
                mrsm,
            }
        }
        /// `<type>:<label>`.
        pub fn keys(&self) -> String {
            format!("{}:{}", self.executor_type, self.label)
        }
    }
    /// One step of a followed log.
    #[derive(Debug, Clone, PartialEq)]
    pub enum LogEvent {
        Lines(Vec<String>),
        Timeout,
        /// The stream ended (`stop_on_exit` and the job stopped), with its result if known.
        End(Option<SuccessTuple>),
    }
    /// A followed log: `monitor_logs` returns a closure `FnMut(Duration) -> LogEvent` (or
    /// `-> Result<LogEvent>`) or a type implementing this trait.
    pub trait LogStream: Send {
        fn next_lines(&mut self, timeout: Duration) -> Result<LogEvent>;
        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }
    pub trait IntoLogEvent {
        fn into_log_event(self) -> Result<LogEvent>;
    }
    impl IntoLogEvent for LogEvent {
        fn into_log_event(self) -> Result<LogEvent> {
            Ok(self)
        }
    }
    impl IntoLogEvent for Result<LogEvent> {
        fn into_log_event(self) -> Result<LogEvent> {
            self
        }
    }
    impl<F: FnMut(Duration) -> R + Send, R: IntoLogEvent> LogStream for F {
        fn next_lines(&mut self, timeout: Duration) -> Result<LogEvent> {
            self(timeout).into_log_event()
        }
    }
    /// The packaged SDK's return type: `monitor_logs` may answer `Box<dyn LogStream>`.
    impl LogStream for Box<dyn LogStream> {
        fn next_lines(&mut self, timeout: Duration) -> Result<LogEvent> {
            (**self).next_lines(timeout)
        }
        fn close(&mut self) -> Result<()> {
            (**self).close()
        }
    }
    /// What a generated executor method answers.
    pub enum ExecutorReply {
        Value(Value),
        Logs(Box<dyn LogStream>),
    }
    /// The generated per-type method table.
    pub type Methods = fn(&str, &ExecutorInfo, &Value) -> Result<ExecutorReply>;
    pub fn unsupported(method: &str) -> Error {
        Error::Unsupported(format!("`{method}` is not supported by this executor."))
    }
    pub fn name_of(context: &Value) -> Result<&str> {
        context["name"]
            .as_str()
            .ok_or_else(|| error("missing job name"))
    }
    fn value(
        methods: Methods,
        method: &str,
        info: &ExecutorInfo,
        context: &Value,
    ) -> Result<Value> {
        let reply = match methods(method, info, context) {
            Err(Error::Unsupported(message)) => {
                return derived(methods, method, info, context)
                    .unwrap_or(Err(Error::Unsupported(message)));
            }
            other => other?,
        };
        match reply {
            ExecutorReply::Value(value) => Ok(value),
            ExecutorReply::Logs(_) => Err(error(format!("{method} returned a log stream"))),
        }
    }
    /// The defaults the packaged SDK derives from other methods.
    fn derived(
        methods: Methods,
        method: &str,
        info: &ExecutorInfo,
        context: &Value,
    ) -> Option<Result<Value>> {
        const DERIVED: &[&str] = &[
            "get_job_exists",
            "get_jobs",
            "get_job_properties",
            "get_job_status",
            "get_job_began",
            "get_job_ended",
            "get_job_paused",
        ];
        if !DERIVED.contains(&method) {
            return None;
        }
        let call = |m: &str, c: &Value| value(methods, m, info, c);
        let names = || call("get_job_names", &json!({})).map(|v| strings(&v));
        Some((|| -> Result<Value> {
            Ok(match method {
                "get_job_exists" => {
                    let name = name_of(context)?;
                    json!(names()?.iter().any(|n| n == name))
                }
                "get_jobs" => {
                    let mut jobs = serde_json::Map::new();
                    for name in names()? {
                        let metadata = call("get_job_metadata", &json!({"name": name}))?;
                        if !metadata.is_null() {
                            jobs.insert(name, metadata);
                        }
                    }
                    Value::Object(jobs)
                }
                "get_job_properties" => {
                    match &call("get_job_metadata", context)?["daemon"]["properties"] {
                        Value::Object(p) => Value::Object(p.clone()),
                        _ => json!({}),
                    }
                }
                "get_job_status" => json!(
                    call("get_job_metadata", context)?["daemon"]["status"]
                        .as_str()
                        .unwrap_or("stopped")
                ),
                // get_job_began / _ended / _paused: `properties.process.<key>`.
                _ => {
                    let key = method.trim_start_matches("get_job_");
                    let properties = call("get_job_properties", context)?;
                    properties["process"][key]
                        .as_str()
                        .map_or(Value::Null, |s| json!(s))
                }
            })
        })())
    }
    /// Answer an `executor.<method>` call with the generated `methods`.
    pub fn dispatch(request: &Request, methods: Methods) -> Result<Reply> {
        let method = request
            .method
            .strip_prefix("executor.")
            .unwrap_or(&request.method);
        let info = ExecutorInfo::from_request(request);
        if method == "monitor_logs" {
            return match methods(method, &info, &request.context)? {
                ExecutorReply::Logs(stream) => Ok(Reply::json(
                    json!({"handle": super::streams::MONITORS.insert(stream)}),
                )),
                ExecutorReply::Value(_) => Err(error("monitor_logs returned no log stream")),
            };
        }
        value(methods, method, &info, &request.context).map(Reply::json)
    }
}
/// Console pages (`#[web_page]`): an HTML fragment for the web console, as a packaged plugin's
/// `page` call answers it.
pub mod page {
    use super::*;
    /// A page request: the page path, the raw query string and the console session's user
    /// (`None` when nobody is logged in and the page does not require login).
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct PageRequest {
        pub path: String,
        pub query: String,
        pub username: Option<String>,
    }
    impl PageRequest {
        pub fn from_context(context: &Value) -> Self {
            Self {
                path: context["path"].as_str().unwrap_or_default().to_owned(),
                query: context["query"]
                    .as_str()
                    .unwrap_or_default()
                    .trim_start_matches('?')
                    .to_owned(),
                username: context["username"]
                    .as_str()
                    .filter(|u| !u.is_empty())
                    .map(str::to_owned),
            }
        }
        /// The first value of a query parameter, decoded (`+` is a space).
        pub fn query_param(&self, name: &str) -> Option<String> {
            self.query.split('&').find_map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                (decode(key) == name).then(|| decode(value))
            })
        }
    }
    fn decode(text: &str) -> String {
        let bytes = text.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        let hex = |b: u8| (b as char).to_digit(16);
        while i < bytes.len() {
            let escaped = (bytes[i] == b'%')
                .then(|| {
                    let hi = hex(*bytes.get(i + 1)?)?;
                    let lo = hex(*bytes.get(i + 2)?)?;
                    Some((hi * 16 + lo) as u8)
                })
                .flatten();
            match (bytes[i], escaped) {
                (_, Some(byte)) => {
                    out.push(byte);
                    i += 2;
                }
                (b'+', None) => out.push(b' '),
                (b, None) => out.push(b),
            }
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }
    /// A page returns its HTML (`String` / `&str`), `(status, html)`, or a `Result` of those.
    pub trait IntoPage {
        fn into_page(self) -> Result<Value>;
    }
    impl IntoPage for String {
        fn into_page(self) -> Result<Value> {
            Ok(json!({"html": self}))
        }
    }
    impl IntoPage for &str {
        fn into_page(self) -> Result<Value> {
            Ok(json!({"html": self}))
        }
    }
    impl<T: IntoPage> IntoPage for (u16, T) {
        fn into_page(self) -> Result<Value> {
            let mut page = self.1.into_page()?;
            page["status"] = json!(self.0);
            Ok(page)
        }
    }
    impl<T: IntoPage> IntoPage for Result<T> {
        fn into_page(self) -> Result<Value> {
            self.and_then(IntoPage::into_page)
        }
    }
}
/// Streaming connectors (`fn subscribe` in a `#[make_connector]` module): a pull model where the
/// host asks for each batch.
pub mod subscribe {
    use super::*;
    use std::time::Duration;
    /// The result of one `next_batch`.
    #[derive(Debug, Clone, PartialEq)]
    pub enum Batch {
        /// Rows without an acknowledgement token.
        Rows(Vec<Value>),
        /// Rows to acknowledge with the token once they committed.
        RowsAck(Vec<Value>, u64),
        /// Rows with an optional token; `redelivered` forces `check_existing`.
        Data {
            rows: Vec<Value>,
            ack: Option<u64>,
            redelivered: bool,
        },
        /// Nothing arrived within the timeout; the host asks again.
        Timeout,
        /// The subscription ended (the host closes it and subscribes again after a backoff).
        End,
    }
    pub trait IntoBatch {
        fn into_batch(self) -> Result<Batch>;
    }
    impl IntoBatch for Batch {
        fn into_batch(self) -> Result<Batch> {
            Ok(self)
        }
    }
    impl IntoBatch for Result<Batch> {
        fn into_batch(self) -> Result<Batch> {
            self
        }
    }
    /// An open subscription: `subscribe` returns a closure `FnMut(Duration) -> Batch` (or
    /// `-> Result<Batch>`) or a type implementing this trait. It may block up to the timeout.
    pub trait Subscription: Send {
        fn next_batch(&mut self, timeout: Duration) -> Result<Batch>;
        /// Acknowledge committed batches (only ever called after the commit).
        fn ack(&mut self, tokens: &[u64]) -> Result<()> {
            let _ = tokens;
            Ok(())
        }
        /// Release the source (called once).
        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }
    impl<F: FnMut(Duration) -> R + Send, R: IntoBatch> Subscription for F {
        fn next_batch(&mut self, timeout: Duration) -> Result<Batch> {
            self(timeout).into_batch()
        }
    }
    /// The packaged SDK's return type: `subscribe` may answer `Box<dyn Subscription>`.
    impl Subscription for Box<dyn Subscription> {
        fn next_batch(&mut self, timeout: Duration) -> Result<Batch> {
            (**self).next_batch(timeout)
        }
        fn ack(&mut self, tokens: &[u64]) -> Result<()> {
            (**self).ack(tokens)
        }
        fn close(&mut self) -> Result<()> {
            (**self).close()
        }
    }
    /// A subscription fed by a producer thread over a bounded channel, as the packaged SDK's
    /// `ChannelSubscription`: a full channel blocks the producer (back-pressure), and the
    /// subscription ends when every sender is dropped.
    pub struct ChannelSubscription {
        receiver: Option<std::sync::mpsc::Receiver<Frame>>,
    }
    impl ChannelSubscription {
        /// A sender for the producer thread and the subscription to return from `subscribe`.
        pub fn new(capacity: usize) -> (std::sync::mpsc::SyncSender<Frame>, Self) {
            let (sender, receiver) = std::sync::mpsc::sync_channel(capacity);
            (
                sender,
                Self {
                    receiver: Some(receiver),
                },
            )
        }
    }
    impl Subscription for ChannelSubscription {
        fn next_batch(&mut self, timeout: Duration) -> Result<Batch> {
            use std::sync::mpsc::RecvTimeoutError;
            let Some(receiver) = &self.receiver else {
                return Ok(Batch::End);
            };
            match receiver.recv_timeout(timeout) {
                Ok(frame) => Ok(Batch::Rows(frame.into_rows())),
                Err(RecvTimeoutError::Timeout) => Ok(Batch::Timeout),
                Err(RecvTimeoutError::Disconnected) => Ok(Batch::End),
            }
        }
        fn close(&mut self) -> Result<()> {
            self.receiver = None;
            Ok(())
        }
    }
    /// A module-level `fn ack(tokens: &[u64])`, called after the subscription's own `ack`.
    pub type Ack = fn(&Request, &[u64]) -> Result<()>;
    pub(crate) struct Open {
        pub(crate) subscription: Box<dyn Subscription>,
        pub(crate) ack: Option<Ack>,
    }
    /// Register a subscription and answer `subscribe` with its handle.
    pub fn open(subscription: Box<dyn Subscription>, ack: Option<Ack>) -> Result<Reply> {
        let handle = super::streams::SUBSCRIPTIONS.insert(Box::new(Open { subscription, ack }));
        Ok(Reply::json(json!({"handle": handle})))
    }
    /// `()` or `Result<()>` from a module-level `ack`.
    pub trait AckOutput {
        fn into_ack(self) -> Result<()>;
    }
    impl AckOutput for () {
        fn into_ack(self) -> Result<()> {
            Ok(())
        }
    }
    impl AckOutput for Result<()> {
        fn into_ack(self) -> Result<()> {
            self
        }
    }
}
/// Handle tables of the stream methods, which the host calls without its per-plugin call lock:
/// each entry is locked on its own.
mod streams {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{
            Arc, LazyLock, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };
    pub(crate) struct Handles<T: ?Sized> {
        next: AtomicU64,
        open: Mutex<HashMap<u64, Arc<Mutex<Box<T>>>>>,
    }
    impl<T: ?Sized> Handles<T> {
        fn new() -> Self {
            Self {
                next: AtomicU64::new(1),
                open: Mutex::new(HashMap::new()),
            }
        }
        pub(crate) fn insert(&self, item: Box<T>) -> u64 {
            let handle = self.next.fetch_add(1, Ordering::Relaxed);
            self.open
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(handle, Arc::new(Mutex::new(item)));
            handle
        }
        fn get(&self, handle: u64) -> Result<Arc<Mutex<Box<T>>>> {
            self.open
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&handle)
                .cloned()
                .ok_or_else(|| error(format!("unknown or closed handle {handle}")))
        }
        fn remove(&self, handle: u64) -> Option<Arc<Mutex<Box<T>>>> {
            self.open
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&handle)
        }
    }
    pub(crate) static SUBSCRIPTIONS: LazyLock<Handles<subscribe::Open>> =
        LazyLock::new(Handles::new);
    pub(crate) static MONITORS: LazyLock<Handles<dyn executor::LogStream>> =
        LazyLock::new(Handles::new);
    fn handle_of(context: &Value) -> Result<u64> {
        context["handle"]
            .as_u64()
            .ok_or_else(|| error("missing stream handle"))
    }
    fn timeout_of(context: &Value) -> Duration {
        Duration::from_millis(context["timeout_ms"].as_u64().unwrap_or(250).min(60_000))
    }
    fn batch_rows(rows: Vec<Value>) -> Result<Option<Frame>> {
        // Empty rows cross as no data: the host acknowledges the token without syncing.
        if rows.is_empty() {
            Ok(None)
        } else {
            Frame::from_rows(rows).map(Some)
        }
    }
    /// The stream methods (`None`: not a stream method).
    pub(crate) fn dispatch(request: &Request) -> Option<Result<Reply>> {
        let context = &request.context;
        Some(match request.method.as_str() {
            "next_batch" => (|| {
                let entry = SUBSCRIPTIONS.get(handle_of(context)?)?;
                let mut open = entry.lock().unwrap_or_else(|e| e.into_inner());
                let (status, rows, ack, redelivered) =
                    match open.subscription.next_batch(timeout_of(context))? {
                        subscribe::Batch::Rows(r) => ("batch", r, None, false),
                        subscribe::Batch::RowsAck(r, token) => ("batch", r, Some(token), false),
                        subscribe::Batch::Data {
                            rows,
                            ack,
                            redelivered,
                        } => ("batch", rows, ack, redelivered),
                        subscribe::Batch::Timeout => ("timeout", Vec::new(), None, false),
                        subscribe::Batch::End => ("end", Vec::new(), None, false),
                    };
                Ok(Reply {
                    value: json!({"status": status, "ack": ack, "redelivered": redelivered}),
                    data: batch_rows(rows)?,
                })
            })(),
            "ack_batches" => (|| {
                let tokens: Vec<u64> =
                    serde_json::from_value(context["tokens"].clone()).map_err(error)?;
                let entry = SUBSCRIPTIONS.get(handle_of(context)?)?;
                let mut open = entry.lock().unwrap_or_else(|e| e.into_inner());
                open.subscription.ack(&tokens)?;
                if let Some(ack) = open.ack {
                    ack(request, &tokens)?;
                }
                Ok(Reply::json(Value::Null))
            })(),
            "close_subscription" => (|| {
                // Closing an unknown handle is a no-op: close is idempotent.
                if let Some(entry) = SUBSCRIPTIONS.remove(handle_of(context)?) {
                    entry
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .subscription
                        .close()?;
                }
                Ok(Reply::json(Value::Null))
            })(),
            "executor.next_logs" => (|| {
                let entry = MONITORS.get(handle_of(context)?)?;
                let mut stream = entry.lock().unwrap_or_else(|e| e.into_inner());
                Ok(Reply::json(
                    match stream.next_lines(timeout_of(context))? {
                        executor::LogEvent::Lines(lines) => {
                            json!({"status": "lines", "lines": lines, "result": null})
                        }
                        executor::LogEvent::Timeout => {
                            json!({"status": "timeout", "lines": [], "result": null})
                        }
                        executor::LogEvent::End(result) => {
                            json!({"status": "end", "lines": [], "result": result})
                        }
                    },
                ))
            })(),
            "executor.close_logs" => (|| {
                if let Some(entry) = MONITORS.remove(handle_of(context)?) {
                    entry.lock().unwrap_or_else(|e| e.into_inner()).close()?;
                }
                Ok(Reply::json(Value::Null))
            })(),
            _ => return None,
        })
    }
}
fn boundary(f: impl FnOnce() -> Result<Value>) -> CallResult {
    let (status, bytes) = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        f().and_then(|v| serde_json::to_vec(&v).map_err(error))
    })) {
        Ok(Ok(bytes)) => (0, bytes),
        Ok(Err(Error::Unsupported(s))) => (3, s.into_bytes()),
        Ok(Err(e)) => (1, e.to_string().into_bytes()),
        Err(_) => (2, b"native plugin panicked".to_vec()),
    };
    CallResult {
        status,
        payload: OwnedBytes::new(bytes),
    }
}
unsafe extern "C" fn free_bytes(bytes: OwnedBytes) {
    if !bytes.ptr.is_null() {
        // SAFETY: this library allocated the boxed byte slice, and caller returns it once.
        unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                bytes.ptr, bytes.len,
            )));
        }
    }
}
unsafe fn decode(free: unsafe extern "C" fn(OwnedBytes), result: CallResult) -> Result<Value> {
    if result.payload.len != 0 && result.payload.ptr.is_null() {
        return Err(error("null host result"));
    }
    let bytes = if result.payload.len == 0 {
        Vec::new()
    } else {
        // SAFETY: host guarantees readable bytes until free.
        unsafe { std::slice::from_raw_parts(result.payload.ptr, result.payload.len) }.to_vec()
    };
    unsafe {
        free(result.payload);
    }
    if result.status != 0 {
        return Err(error(String::from_utf8_lossy(&bytes)));
    }
    serde_json::from_slice(&bytes).map_err(error)
}
// The crate's own unit tests compile this runtime without the generated crate's ABI aliases.
#[cfg(not(test))]
pub struct Export<P>(PhantomData<P>);
#[cfg(not(test))]
impl<P: Plugin> Export<P> {
    pub const VTABLE: MrsmPluginV1 = MrsmPluginV1 {
        abi_version: ABI_VERSION,
        struct_size: std::mem::size_of::<MrsmPluginV1>() as u32,
        describe: Self::describe,
        invoke: Self::unsupported,
        free_bytes,
    };
    unsafe extern "C" fn describe() -> CallResult {
        boundary(|| {
            Ok(
                json!({"name": P::NAME, "version": P::VERSION, "abi_version": ABI_VERSION, "manifest": P::default().manifest()}),
            )
        })
    }
    unsafe extern "C" fn unsupported(
        _: Bytes,
        _: *mut std::ffi::c_void,
        _: *mut std::ffi::c_void,
    ) -> CallResult {
        boundary(|| {
            Err(error(
                "this single-file plugin needs a host with the rows-v1 extension",
            ))
        })
    }
    pub unsafe extern "C" fn invoke(bytes: Bytes, host: *const RowsHostV1) -> CallResult {
        boundary(|| {
            if bytes.len != 0 && bytes.ptr.is_null() {
                return Err(error("null request"));
            }
            let active = if host.is_null() {
                None
            } else {
                // SAFETY: caller supplies readable permanent prefix; inspect it before the table.
                let (version, size) =
                    unsafe { (host.cast::<u32>().read(), host.cast::<u32>().add(1).read()) };
                if version != 1 || size < std::mem::size_of::<RowsHostV1>() as u32 {
                    return Err(error("incompatible rows host table"));
                }
                let generation = GENERATION.with(|g| {
                    let n = g.get().wrapping_add(1);
                    g.set(n);
                    n
                });
                Some(ActiveHost {
                    table: unsafe { *host },
                    generation,
                })
            };
            let _scope = Scope(HOST.with(|h| h.replace(active)));
            let envelope: Value =
                serde_json::from_slice(unsafe { bytes.as_slice() }).map_err(error)?;
            let method = envelope["request"]["method"]
                .as_str()
                .ok_or_else(|| error("missing method"))?
                .to_owned();
            let context = envelope["request"]["context"].clone();
            let kwargs = context.get("kwargs").cloned().unwrap_or_else(|| {
                if method == "sync" || method == "sync_pipe" {
                    context.clone()
                } else {
                    json!({})
                }
            });
            let request = Request {
                method,
                connector_type: envelope["request"]["connector_type"]
                    .as_str()
                    .unwrap_or("")
                    .into(),
                label: envelope["request"]["label"].as_str().unwrap_or("").into(),
                config: envelope["request"]["config"].clone(),
                context,
                kwargs,
            };
            // Stream calls (`next_batch`, `executor.next_logs`, ...) arrive concurrently with the
            // plugin's other calls; the runtime owns their handle tables.
            if let Some(reply) = streams::dispatch(&request) {
                let reply = reply?;
                return Ok(json!({"value": reply.value, "data": reply.data.map(Frame::into_rows)}));
            }
            let plugin = P::default();
            let data = if envelope["data"].is_null() {
                None
            } else {
                Some(Frame::from_rows(
                    serde_json::from_value(envelope["data"].clone()).map_err(error)?,
                )?)
            };
            let reply = plugin.call(&request, data)?;
            Ok(json!({"value": reply.value, "data": reply.data.map(Frame::into_rows)}))
        })
    }
}
