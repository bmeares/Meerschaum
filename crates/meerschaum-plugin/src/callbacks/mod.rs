//! Host calls: what a plugin may ask the Meerschaum process that loaded it during a call (read and
//! write pipes, run SQL, read its configuration), and the host side that answers. Only C layouts
//! and Arrow streams cross the boundary; host and plugin allocations are always freed by their
//! owner.
//!
//! # Wire format
//!
//! The plugin calls `HostV1.invoke(context, method, stream)` while the host is calling it. `method`
//! is a bare method name (`get_sync_time`, `sync`: the request's Pipe, no arguments; every host
//! with the extension understands these) or a JSON envelope `{"method", "pipe", "args"}`: `pipe` is
//! the addressed Pipe's metadata (`{"keys", "parameters", "temporary"}`, `null` for the request's
//! Pipe) and `args` the method's arguments ([`method`] lists them). `stream` carries the plugin's
//! data to the host (`sync`) and the host's data back (`get_data`, `read_sql`): the host moves the
//! input out and may write its result in its place, which the plugin imports after the call. A
//! host that does not know a method answers status 3 (unsupported); the SDK turns that into an
//! error that names the call and asks for a newer Meerschaum. No version bump: the envelope is
//! new method bytes over the same `HostV1` table.
use crate::{Frame, Request, Result, SuccessTuple, abi::*, error, sdk};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use indexmap::IndexMap;
use meerschaum_kernel::enforce::PipeInput;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{borrow::Cow, cell::Cell, marker::PhantomData, rc::Rc};

/// A bound on an integer or datetime axis (`begin`, `end`, a sync time). Datetimes are ISO-8601
/// strings, as the host formats them (`2024-01-01T00:00:00+00:00`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Bound {
    Int(i64),
    Datetime(String),
}

impl Bound {
    /// The integer of an integer axis bound.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Bound::Int(i) => Some(*i),
            Bound::Datetime(_) => None,
        }
    }

    /// The ISO text of a datetime bound.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Bound::Datetime(text) => Some(text),
            Bound::Int(_) => None,
        }
    }

    /// Whether this is a datetime bound.
    pub fn is_datetime(&self) -> bool {
        matches!(self, Bound::Datetime(_))
    }

    /// A datetime bound as a UTC timestamp. A timestamp without an offset is read as UTC; an
    /// integer bound is an error (use [`Bound::as_int`]).
    pub fn to_datetime(&self) -> Result<chrono::DateTime<chrono::Utc>> {
        let Bound::Datetime(text) = self else {
            return Err(error(format!("{self} is an integer bound, not a datetime")));
        };
        let value = meerschaum_kernel::value::parse_datetime_value(text)
            .ok_or_else(|| error(format!("'{text}' is not an ISO-8601 timestamp")))?;
        Ok(chrono::DateTime::from_naive_utc_and_offset(
            value.utc,
            chrono::Utc,
        ))
    }

    /// A datetime bound for `datetime` (UTC).
    pub fn datetime(datetime: chrono::DateTime<chrono::Utc>) -> Bound {
        Bound::Datetime(datetime.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, false))
    }

    /// Encode this bound as a typed SQL parameter value for [`crate::host::read_sql_with`],
    /// [`crate::host::exec_sql_with`], and [`Transaction`] queries: integer bounds stay JSON
    /// integers; datetime bounds travel as typed `timestamptz` parameters so PostgreSQL accepts
    /// `WHERE time >= :begin` without casts.
    pub fn to_sql_param(&self) -> Value {
        match self {
            Bound::Int(i) => json!(i),
            Bound::Datetime(s) => json!({"$mrsm_sql": "timestamptz", "value": s}),
        }
    }

    fn json(bound: Option<&Bound>) -> Value {
        match bound {
            None => Value::Null,
            Some(Bound::Int(i)) => json!(i),
            Some(Bound::Datetime(s)) => json!(s),
        }
    }
}

impl std::fmt::Display for Bound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Bound::Int(i) => write!(f, "{i}"),
            Bound::Datetime(s) => f.write_str(s),
        }
    }
}

impl From<i64> for Bound {
    fn from(value: i64) -> Self {
        Bound::Int(value)
    }
}

impl From<i32> for Bound {
    fn from(value: i32) -> Self {
        Bound::Int(value.into())
    }
}

impl From<chrono::DateTime<chrono::Utc>> for Bound {
    fn from(value: chrono::DateTime<chrono::Utc>) -> Self {
        Bound::datetime(value)
    }
}

impl From<chrono::NaiveDateTime> for Bound {
    /// A timestamp without an offset is UTC.
    fn from(value: chrono::NaiveDateTime) -> Self {
        Bound::datetime(chrono::DateTime::from_naive_utc_and_offset(
            value,
            chrono::Utc,
        ))
    }
}

impl From<&str> for Bound {
    fn from(value: &str) -> Self {
        Bound::Datetime(value.to_string())
    }
}

impl From<String> for Bound {
    fn from(value: String) -> Self {
        Bound::Datetime(value)
    }
}

/// The host calls and their `args` (`HostCall::method`). Bounds are bound JSON (an integer, an
/// ISO string or `null`); `params` is a JSON object or `null`.
pub mod method {
    /// `{"params", "newest"}` (none: newest, unfiltered) -> a bound or `null`.
    pub const GET_SYNC_TIME: &str = "get_sync_time";
    /// The data in the stream, and optionally `{"replace": true, "begin", "end"}` (the rows of
    /// `[begin, end)` are replaced by the data in one transaction; a `null` bound comes from the
    /// data) -> a success tuple.
    pub const SYNC: &str = "sync";
    /// `{"begin", "end", "params", "select_columns", "omit_columns", "limit", "order"}` -> the
    /// frame in the stream (value `null`: the Pipe has no table).
    pub const GET_DATA: &str = "get_data";
    /// `{"begin", "end", "params"}` -> an integer.
    pub const GET_ROWCOUNT: &str = "get_rowcount";
    /// `{"begin", "end", "params"}` -> a success tuple.
    pub const CLEAR: &str = "clear";
    /// `{}` -> `{column: database type}`.
    pub const GET_COLUMNS_TYPES: &str = "get_columns_types";
    /// `{}` -> whether the Pipe's table exists.
    pub const EXISTS: &str = "exists";
    /// `{}` -> the Pipe's registered integer id (`i64`) or `null` when unregistered.
    pub const GET_PIPE_ID: &str = "get_pipe_id";
    /// `{}` -> whether the Pipe is registered on its instance (`bool`).
    pub const IS_REGISTERED: &str = "is_registered";
    /// `{}` -> a success tuple (registers the Pipe with the view's parameters).
    pub const REGISTER: &str = "register";
    /// `{"connector_keys", "metric_key", "location_key", "instance_keys"}` -> Pipe metadata
    /// `{"keys", "parameters", "temporary"}` with the registered parameters.
    pub const PIPE: &str = "pipe";
    /// `{"connector_keys", "query"}` -> the result frame in the stream.
    pub const READ_SQL: &str = "read_sql";
    /// `{"connector_keys", "query", "params"}` -> the result frame in the stream.
    pub const READ_SQL_WITH: &str = "read_sql_with";
    /// `{"connector_keys", "query"}` -> a success tuple.
    pub const EXEC_SQL: &str = "exec_sql";
    /// `{"connector_keys", "query", "params"}` -> a success tuple.
    pub const EXEC_SQL_WITH: &str = "exec_sql_with";
    /// `{}` -> the resolved `MRSM_ROOT_DIR` path of the current call (`String`).
    pub const ROOT_DIR: &str = "root_dir";
    /// `{"connector_keys", "lock_key"}` -> `{"tx_id": u64}`.
    pub const TX_BEGIN: &str = "tx_begin";
    /// `{"tx_id", "query", "params"}` -> the result frame in the stream.
    pub const TX_READ_SQL: &str = "tx_read_sql";
    /// `{"tx_id", "query", "params"}` -> a success tuple.
    pub const TX_EXEC_SQL: &str = "tx_exec_sql";
    /// `{"tx_id"}` -> a success tuple.
    pub const TX_COMMIT: &str = "tx_commit";
    /// `{"tx_id"}` -> a success tuple.
    pub const TX_ROLLBACK: &str = "tx_rollback";
    /// `{}` -> the plugin's config section (`plugins:<name>`) or `null`.
    pub const PLUGIN_CONFIG: &str = "plugin_config";
    /// `{"config"}` -> a success tuple.
    pub const WRITE_PLUGIN_CONFIG: &str = "write_plugin_config";
    /// `{"keys": [...]}` -> the config value at the keys or `null`.
    pub const CONFIG: &str = "config";
    /// `{"label", "scopes": [...], "expiration"}` (`expiration`: a bound, Unix seconds or an
    /// ISO timestamp; `null` never expires) -> the registered token `{"id", "label", "scopes",
    /// "expiration", "secret", "api_key"}`. Only during an API request, with the calling
    /// plugin's own custom scopes; the token belongs to the caller's user.
    pub const MINT_TOKEN: &str = "mint_token";
    /// `{"id"}` -> a success tuple (the token is deleted). Only during an API request, for a
    /// token of the caller's user that carries nothing but the calling plugin's own scopes.
    pub const REVOKE_TOKEN: &str = "revoke_token";
    /// `{}` -> whether the API server serving the request has begun to drain (`false` outside
    /// an API request).
    pub const DRAINING: &str = "draining";

    /// The first Meerschaum whose hosts answer `method` (for the upgrade message of an older
    /// host).
    pub(crate) fn minimum_version(method: &str) -> &'static str {
        match method {
            GET_PIPE_ID | IS_REGISTERED | READ_SQL_WITH | EXEC_SQL_WITH | ROOT_DIR | TX_BEGIN
            | TX_READ_SQL | TX_EXEC_SQL | TX_COMMIT | TX_ROLLBACK => "5.1.0",
            MINT_TOKEN | REVOKE_TOKEN | DRAINING => "5.0.0rc34",
            _ => "5.0.0rc15",
        }
    }
}

/// A token [`host::mint_token`](crate::host::mint_token) registered. `secret` and `api_key` (the
/// `Authorization: Bearer` value) are shown here only, once: the instance keeps a hash of the
/// secret. `Debug` leaves both out.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MintedToken {
    /// The token's id (a UUID), what [`host::revoke_token`](crate::host::revoke_token) takes.
    pub id: String,
    pub label: String,
    pub scopes: Vec<String>,
    /// When it expires (ISO-8601, UTC), `None` never.
    #[serde(default)]
    pub expiration: Option<String>,
    /// The secret of `client_credentials` logins (`client_id` is the id).
    pub secret: String,
    /// `mrsm-key:...`, sent as `Authorization: Bearer <api_key>`.
    pub api_key: String,
}

impl std::fmt::Debug for MintedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MintedToken")
            .field("id", &self.id)
            .field("label", &self.label)
            .field("scopes", &self.scopes)
            .field("expiration", &self.expiration)
            .finish_non_exhaustive()
    }
}

/// One call a plugin makes into its host ([`HostServices::host_call`]).
#[derive(Debug, Clone, PartialEq)]
pub struct HostCall {
    /// The calling plugin's name, as the loader knows it (never taken from the plugin's bytes).
    pub plugin: String,
    /// The method ([`method`]).
    pub method: String,
    /// The addressed Pipe's metadata (`{"keys", "parameters", "temporary"}`), or `None` for the
    /// request's Pipe.
    pub pipe: Option<Value>,
    /// The method's arguments (a JSON object).
    pub args: Value,
}

impl HostCall {
    /// A call of the original extension: a bare method name on the request's Pipe.
    pub fn legacy(plugin: &str, method: &str) -> HostCall {
        HostCall {
            plugin: plugin.into(),
            method: method.into(),
            pipe: None,
            args: json!({}),
        }
    }

    /// Whether every host with the extension understands this call (see the module docs).
    pub fn is_legacy(&self) -> bool {
        self.pipe.is_none()
            && self.args.as_object().is_none_or(|a| a.is_empty())
            && (self.method == method::GET_SYNC_TIME || self.method == method::SYNC)
    }

    /// The argument `name` (`null` when absent).
    pub fn arg(&self, name: &str) -> &Value {
        self.args.get(name).unwrap_or(&Value::Null)
    }

    /// A string argument (`None` when absent, `null` or empty).
    pub fn str_arg(&self, name: &str) -> Option<&str> {
        self.arg(name).as_str().filter(|s| !s.is_empty())
    }

    /// A bound argument (`None` for `null` or `""`).
    pub fn bound(&self, name: &str) -> Result<Option<Bound>> {
        match self.arg(name) {
            Value::Null => Ok(None),
            Value::String(s) if s.is_empty() => Ok(None),
            value => serde_json::from_value(value.clone())
                .map(Some)
                .map_err(|e| error(format!("invalid `{name}`: {e}"))),
        }
    }

    /// Decode the method bytes of `HostV1.invoke` (a bare name or a JSON envelope).
    pub fn decode(plugin: &str, bytes: &[u8]) -> Result<HostCall> {
        let text = std::str::from_utf8(bytes).map_err(error)?;
        if !text.trim_start().starts_with('{') {
            return Ok(HostCall::legacy(plugin, text));
        }
        HostCall::from_envelope(plugin, serde_json::from_str(text).map_err(error)?)
    }

    /// A call from its JSON envelope (`{"method", "pipe", "args"}`; other keys are ignored).
    pub fn from_envelope(plugin: &str, mut envelope: Value) -> Result<HostCall> {
        let method = envelope["method"]
            .as_str()
            .filter(|m| !m.is_empty())
            .ok_or_else(|| error("a host call names no method"))?
            .to_owned();
        let pipe = Some(envelope["pipe"].take()).filter(|p| !p.is_null());
        let args = match envelope["args"].take() {
            Value::Null => json!({}),
            args @ Value::Object(_) => args,
            _ => return Err(error("host call arguments must be a JSON object")),
        };
        Ok(HostCall {
            plugin: plugin.into(),
            method,
            pipe,
            args,
        })
    }
}

/// What a host call answers: a JSON value and, for `get_data` and `read_sql`, a frame.
#[derive(Debug, Clone)]
pub struct HostReply {
    pub value: Value,
    pub data: Option<Frame>,
}

impl HostReply {
    pub fn json(value: Value) -> HostReply {
        HostReply { value, data: None }
    }
    pub fn data(data: Option<Frame>) -> HostReply {
        HostReply {
            value: if data.is_some() {
                json!(true)
            } else {
                Value::Null
            },
            data,
        }
    }
}

/// A host's implementation of the host calls. No Rust trait crosses the ABI: the loader adapts
/// both plugin ABIs (Arrow and rows) to this trait.
pub trait HostServices: Send + Sync {
    /// The original calls (`get_sync_time`, `sync`) on the request's Pipe.
    fn call(&self, request: &Request, method: &str, data: Option<Frame>) -> Result<Value>;

    /// [`call`](Self::call) with the data as the plugin sent it: the documents of a rows-ABI
    /// (single-file) plugin stay documents, so a host that syncs them can treat them like a
    /// Python plugin's `pipe.sync(docs)`. The default builds the frame a Python plugin's
    /// documents become (`pd.DataFrame(docs).convert_dtypes()`) and calls `call`.
    fn call_input(
        &self,
        request: &Request,
        method: &str,
        data: Option<PipeInput>,
    ) -> Result<Value> {
        self.call(
            request,
            method,
            data.map(crate::host::input_frame).transpose()?,
        )
    }

    /// Every host call ([`method`]). The default serves the original calls through
    /// [`call_input`](Self::call_input) and answers every other call "unsupported", which the
    /// SDK reports to the plugin as a call this Meerschaum does not offer.
    fn host_call(
        &self,
        request: &Request,
        call: &HostCall,
        data: Option<PipeInput>,
    ) -> Result<HostReply> {
        if call.is_legacy() {
            return self
                .call_input(request, &call.method, data)
                .map(HostReply::json);
        }
        Err(crate::Error::Unsupported(format!(
            "unsupported host call: {}",
            call.method
        )))
    }
}

#[derive(Clone, Copy)]
struct ActiveHost {
    table: HostV1,
    generation: u64,
}
thread_local! {
    static HOST: Cell<Option<ActiveHost>> = const { Cell::new(None) };
    static GENERATION: Cell<u64> = const { Cell::new(0) };
}
pub(crate) struct Scope(Option<ActiveHost>);
impl Drop for Scope {
    fn drop(&mut self) {
        HOST.with(|slot| slot.set(self.0));
    }
}
pub(crate) unsafe fn enter(host: *const HostV1) -> Result<Scope> {
    if host.is_null() {
        return Err(error("missing host services table"));
    }
    // SAFETY: caller guarantees the permanent two-u32 prefix is readable.
    let (version, size) = unsafe { (host.cast::<u32>().read(), host.cast::<u32>().add(1).read()) };
    if version != 1 || size < std::mem::size_of::<HostV1>() as u32 {
        return Err(error("incompatible host services table"));
    }
    // SAFETY: layout was checked and the caller guarantees its lifetime through Scope.
    let table = unsafe { *host };
    let generation = GENERATION.with(|slot| {
        let next = slot.get().wrapping_add(1);
        slot.set(next);
        next
    });
    Ok(Scope(HOST.with(|slot| {
        slot.replace(Some(ActiveHost { table, generation }))
    })))
}

/// Why a host call cannot run here.
const UNAVAILABLE: &str = "Meerschaum host calls are unavailable here: they work only while \
    Meerschaum is calling this plugin (fetch, sync, hooks, actions, routes, pages and MCP \
    entries), on the calling thread";

/// The name a plugin author wrote for a host call, for error messages.
fn call_name(method: &str, on_pipe: bool) -> String {
    if on_pipe {
        format!("pipe.{method}()")
    } else {
        format!("host::{method}()")
    }
}

/// Send one host call: `generation` is the invocation a [`Pipe`] view belongs to (`None`: the
/// current one). Returns the JSON value and the host's frame.
fn invoke(
    generation: Option<u64>,
    method: &str,
    pipe: Option<&Value>,
    args: Value,
    data: Option<&Frame>,
    on_pipe: bool,
) -> Result<(Value, Option<Frame>)> {
    let host = HOST
        .with(|slot| slot.get())
        .filter(|h| generation.is_none_or(|g| g == h.generation))
        .ok_or_else(|| error(UNAVAILABLE))?
        .table;
    let call = HostCall {
        plugin: String::new(),
        method: method.into(),
        pipe: pipe.cloned(),
        args,
    };
    let bytes = if call.is_legacy() {
        method.as_bytes().to_vec()
    } else {
        serde_json::to_vec(&json!({"method": method, "pipe": pipe, "args": call.args}))
            .map_err(error)?
    };
    let mut stream = data
        .map(sdk::stream)
        .transpose()?
        .unwrap_or_else(FFI_ArrowArrayStream::empty);
    // SAFETY: the scope pins callbacks and context; the bytes and the stream live through the
    // call. The host moves the input out of `stream` and may write its result in its place.
    let result = unsafe { (host.invoke)(host.context, Bytes::borrowed(&bytes), &mut stream) };
    // SAFETY: result bytes are owned by this host and released with its callback once.
    let value =
        unsafe { crate::host::decode_with(host.free_bytes, result) }.map_err(|e| match e {
            crate::Error::Unsupported(_) => crate::Error::Unsupported(format!(
                "This Meerschaum cannot run `{}` for plugins: upgrade Meerschaum to {} or later.",
                call_name(method, on_pipe),
                method::minimum_version(method)
            )),
            other => other,
        })?;
    let data = sdk::frame(std::mem::replace(
        &mut stream,
        FFI_ArrowArrayStream::empty(),
    ))?;
    Ok((value, data))
}

fn tuple(value: Value) -> Result<SuccessTuple> {
    serde_json::from_value(value).map_err(|e| error(format!("invalid host success tuple: {e}")))
}

/// The options of [`Pipe::get_data_with`] (Python's `pipe.get_data(...)` keywords).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Query {
    /// Rows at or after this bound.
    pub begin: Option<Bound>,
    /// Rows before this bound.
    pub end: Option<Bound>,
    /// Filters: `{"col": value}` (`=`), `{"col": [a, b]}` (`IN`), `"_value"` negates.
    pub params: Option<Value>,
    /// Only these columns.
    pub select_columns: Option<Vec<String>>,
    /// Not these columns.
    pub omit_columns: Option<Vec<String>>,
    /// At most this many rows.
    pub limit: Option<i64>,
    /// `"asc"` (default) or `"desc"`, by the datetime axis then the other index columns.
    pub order: Option<String>,
}

impl Query {
    pub fn new() -> Query {
        Query::default()
    }
    pub fn begin(mut self, begin: impl Into<Bound>) -> Query {
        self.begin = Some(begin.into());
        self
    }
    pub fn end(mut self, end: impl Into<Bound>) -> Query {
        self.end = Some(end.into());
        self
    }
    pub fn params(mut self, params: Value) -> Query {
        self.params = Some(params);
        self
    }
    pub fn select<S: Into<String>>(mut self, columns: impl IntoIterator<Item = S>) -> Query {
        self.select_columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }
    pub fn omit<S: Into<String>>(mut self, columns: impl IntoIterator<Item = S>) -> Query {
        self.omit_columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }
    pub fn limit(mut self, limit: i64) -> Query {
        self.limit = Some(limit);
        self
    }
    pub fn order(mut self, order: impl Into<String>) -> Query {
        self.order = Some(order.into());
        self
    }
    fn args(&self) -> Value {
        json!({
            "begin": Bound::json(self.begin.as_ref()),
            "end": Bound::json(self.end.as_ref()),
            "params": self.params,
            "select_columns": self.select_columns,
            "omit_columns": self.omit_columns,
            "limit": self.limit,
            "order": self.order,
        })
    }
}

fn window(begin: Option<Bound>, end: Option<Bound>, params: Option<&Value>) -> Value {
    json!({
        "begin": Bound::json(begin.as_ref()),
        "end": Bound::json(end.as_ref()),
        "params": params,
    })
}

/// A Pipe: its keys and parameters, and the operations its host runs on its instance. The
/// request's Pipe comes from [`Request::pipe`]; any other from [`crate::host::pipe`]. A view
/// works only during the plugin call it was made in, on that thread.
pub struct Pipe<'a> {
    metadata: Cow<'a, Value>,
    /// The request's own Pipe, unchanged (the original calls address it by name).
    own: bool,
    host_generation: Option<u64>,
    _thread: PhantomData<Rc<()>>,
}

impl Clone for Pipe<'_> {
    fn clone(&self) -> Self {
        Pipe {
            metadata: self.metadata.clone(),
            own: self.own,
            host_generation: self.host_generation,
            _thread: PhantomData,
        }
    }
}

impl std::fmt::Debug for Pipe<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for Pipe<'_> {
    /// `Pipe('connector', 'metric', 'location', instance='instance')`, like Python's `repr`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Pipe('{}', '{}'",
            self.connector_keys(),
            self.metric_key()
        )?;
        if let Some(location) = self.location_key() {
            write!(f, ", '{location}'")?;
        }
        if !self.instance_keys().is_empty() {
            write!(f, ", instance='{}'", self.instance_keys())?;
        }
        f.write_str(")")
    }
}

impl<'a> Pipe<'a> {
    pub fn from_request(request: &'a Request) -> Result<Self> {
        let metadata = request
            .context
            .get("pipe")
            .filter(|v| v.is_object())
            .ok_or_else(|| error("request has no Pipe metadata"))?;
        Ok(Self {
            metadata: Cow::Borrowed(metadata),
            own: true,
            host_generation: HOST.with(|slot| slot.get().map(|h| h.generation)),
            _thread: PhantomData,
        })
    }

    /// A view of the Pipe the host described (`{"keys", "parameters", "temporary"}`).
    fn from_metadata(metadata: Value) -> Result<Pipe<'static>> {
        if !metadata.is_object() {
            return Err(error("the host returned no Pipe"));
        }
        Ok(Pipe {
            metadata: Cow::Owned(metadata),
            own: false,
            host_generation: HOST.with(|slot| slot.get().map(|h| h.generation)),
            _thread: PhantomData,
        })
    }

    pub fn keys(&self) -> &Value {
        self.metadata.get("keys").unwrap_or(&self.metadata)
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
    /// The datetime axis column (`parameters.columns.datetime`).
    pub fn datetime_column(&self) -> Option<&str> {
        self.parameters()["columns"]["datetime"].as_str()
    }
    /// The target table's name (`pipe.target`): `parameters.target`, else
    /// `<connector_keys with ':' as '_'>_<metric_key>[_<location_key>]`. Quote it for your
    /// database's SQL dialect.
    pub fn target(&self) -> String {
        if let Some(target) = self.parameters()["target"].as_str() {
            return target.to_string();
        }
        let mut target = format!(
            "{}_{}",
            self.connector_keys().replace(':', "_"),
            self.metric_key()
        );
        if let Some(location) = self.location_key() {
            target.push('_');
            target.push_str(location);
        }
        target
    }

    /// This view with other parameters (`mrsm.Pipe(..., parameters=...)`): what [`sync`] and
    /// [`register`] use for a Pipe that is not registered yet.
    ///
    /// [`sync`]: Pipe::sync
    /// [`register`]: Pipe::register
    pub fn with_parameters(mut self, parameters: Value) -> Pipe<'a> {
        self.metadata.to_mut()["parameters"] = parameters;
        self.own = false;
        self
    }

    fn call(
        &self,
        method: &str,
        args: Value,
        data: Option<&Frame>,
    ) -> Result<(Value, Option<Frame>)> {
        let generation = self.host_generation.ok_or_else(|| error(UNAVAILABLE))?;
        let pipe = (!self.own).then_some(self.metadata.as_ref());
        invoke(Some(generation), method, pipe, args, data, true)
    }

    /// The newest value of this Pipe's datetime (or integer) axis on its instance (`None`: no
    /// rows yet).
    pub fn get_sync_time(&self) -> Result<Option<Bound>> {
        self.get_sync_time_with(None, true)
    }

    /// [`get_sync_time`](Pipe::get_sync_time) for the rows matching `params`, newest or oldest.
    pub fn get_sync_time_with(
        &self,
        params: Option<&Value>,
        newest: bool,
    ) -> Result<Option<Bound>> {
        let args = if params.is_none() && newest {
            json!({})
        } else {
            json!({"params": params, "newest": newest})
        };
        let (value, _) = self.call(method::GET_SYNC_TIME, args, None)?;
        serde_json::from_value(value).map_err(error)
    }

    /// Write rows through the host's normal sync (dtype enforcement, existing-row checks,
    /// upserts), without calling the plugin's fetch or sync again. An unregistered Pipe is
    /// registered first. Empty rows succeed without writing anything.
    pub fn sync(&self, data: impl sdk::FetchOutput) -> Result<SuccessTuple> {
        let data = data.into_frame()?;
        tuple(self.call(method::SYNC, json!({}), data.as_ref())?.0)
    }

    /// Replace this Pipe's rows in `[begin, end)` with `data`, in one transaction (Python's
    /// `pipe.sync(df, begin=..., end=..., replace=True)`): rows of the window that are not in
    /// `data` are deleted, and readers never see the window half rewritten. A `None` bound comes
    /// from the rows (`begin` is their earliest datetime, `end` just after their latest), so rows
    /// outside the data's range stay; pass both bounds to empty a window with no rows. A Pipe
    /// without a datetime column has one window, the whole table (both bounds `None`).
    pub fn replace(
        &self,
        data: impl sdk::FetchOutput,
        begin: Option<Bound>,
        end: Option<Bound>,
    ) -> Result<SuccessTuple> {
        let data = data.into_frame()?;
        let args = json!({
            "replace": true,
            "begin": Bound::json(begin.as_ref()),
            "end": Bound::json(end.as_ref()),
        });
        tuple(self.call(method::SYNC, args, data.as_ref())?.0)
    }

    /// The rows in `[begin, end)` matching `params` (Python's `pipe.get_data(begin=, end=,
    /// params=)`), with the Pipe's dtypes. A Pipe without a table returns an empty frame (see
    /// [`exists`](Pipe::exists)).
    pub fn get_data(
        &self,
        begin: Option<Bound>,
        end: Option<Bound>,
        params: Option<&Value>,
    ) -> Result<Frame> {
        self.get_data_with(&Query {
            begin,
            end,
            params: params.cloned(),
            ..Query::default()
        })
    }

    /// [`get_data`](Pipe::get_data) with every option of [`Query`] (columns, limit, order).
    pub fn get_data_with(&self, query: &Query) -> Result<Frame> {
        let (_, data) = self.call(method::GET_DATA, query.args(), None)?;
        Ok(data.unwrap_or_else(Frame::empty))
    }

    /// The number of rows in `[begin, end)` matching `params` (0 without a table).
    pub fn get_rowcount(
        &self,
        begin: Option<Bound>,
        end: Option<Bound>,
        params: Option<&Value>,
    ) -> Result<i64> {
        let (value, _) = self.call(method::GET_ROWCOUNT, window(begin, end, params), None)?;
        Ok(value.as_i64().unwrap_or(0))
    }

    /// Delete the rows in `[begin, end)` matching `params` (Python's `pipe.clear()`); the Pipe
    /// stays registered. To rewrite a window, [`replace`](Pipe::replace) it in one transaction.
    pub fn clear(
        &self,
        begin: Option<Bound>,
        end: Option<Bound>,
        params: Option<&Value>,
    ) -> Result<SuccessTuple> {
        tuple(
            self.call(method::CLEAR, window(begin, end, params), None)?
                .0,
        )
    }

    /// The table's columns and their database types (empty without a table).
    pub fn get_columns_types(&self) -> Result<IndexMap<String, String>> {
        let (value, _) = self.call(method::GET_COLUMNS_TYPES, json!({}), None)?;
        if value.is_null() {
            return Ok(IndexMap::new());
        }
        serde_json::from_value(value).map_err(error)
    }

    /// Whether the Pipe's table exists on its instance.
    pub fn exists(&self) -> Result<bool> {
        let (value, _) = self.call(method::EXISTS, json!({}), None)?;
        Ok(value.as_bool().unwrap_or(false))
    }

    /// The Pipe's registered id on its instance (`None` when unregistered), matching Python's
    /// `pipe.get_id()` / `pipe.id`.
    pub fn id(&self) -> Result<Option<i64>> {
        let (value, _) = self.call(method::GET_PIPE_ID, json!({}), None)?;
        match value {
            Value::Null => Ok(None),
            Value::Number(n) => n
                .as_i64()
                .map(Some)
                .ok_or_else(|| error(format!("pipe id '{n}' is not a 64-bit integer"))),
            Value::String(s) => s
                .parse::<i64>()
                .map(Some)
                .map_err(|_| error(format!("pipe id '{s}' is not an integer"))),
            other => Err(error(format!("invalid pipe id returned by host: {other}"))),
        }
    }

    /// Whether the Pipe is registered on its instance (distinguishes registered-with-empty-params
    /// from unregistered, and works for both integer and string pipe ids).
    pub fn is_registered(&self) -> Result<bool> {
        let (value, _) = self.call(method::IS_REGISTERED, json!({}), None)?;
        Ok(value.as_bool().unwrap_or(false))
    }

    /// Register the Pipe on its instance with this view's parameters.
    pub fn register(&self) -> Result<SuccessTuple> {
        tuple(self.call(method::REGISTER, json!({}), None)?.0)
    }
}

/// An active host-managed SQL transaction opened by [`crate::host::transaction`]. Statements run
/// inside the same database transaction (holding any requested advisory lock); returning `Ok` from
/// the closure commits, while returning `Err` or panicking rolls back automatically.
pub struct Transaction {
    tx_id: u64,
    host_generation: u64,
    finished: Cell<bool>,
    _thread: PhantomData<Rc<()>>,
}

impl Transaction {
    fn call(&self, method: &str, mut args: Value) -> Result<(Value, Option<Frame>)> {
        if self.finished.get() {
            return Err(error("this SQL transaction has already finished"));
        }
        if let Some(obj) = args.as_object_mut() {
            obj.insert("tx_id".into(), json!(self.tx_id));
        }
        invoke(Some(self.host_generation), method, None, args, None, false)
    }

    /// Run a query inside this transaction and return its rows.
    pub fn read_sql(&self, query: &str) -> Result<Frame> {
        self.read_sql_with(query, &json!({}))
    }

    /// Run a parameterized query (`:name` binds) inside this transaction and return its rows.
    pub fn read_sql_with(&self, query: &str, params: &Value) -> Result<Frame> {
        let (_, data) = self.call(
            method::TX_READ_SQL,
            json!({"query": query, "params": params}),
        )?;
        Ok(data.unwrap_or_else(Frame::empty))
    }

    /// Execute a statement inside this transaction.
    pub fn exec_sql(&self, query: &str) -> Result<SuccessTuple> {
        self.exec_sql_with(query, &json!({}))
    }

    /// Execute a parameterized statement (`:name` binds) inside this transaction.
    pub fn exec_sql_with(&self, query: &str, params: &Value) -> Result<SuccessTuple> {
        let (value, _) = self.call(
            method::TX_EXEC_SQL,
            json!({"query": query, "params": params}),
        )?;
        tuple(value)
    }

    fn commit(&self) -> Result<()> {
        if self.finished.replace(true) {
            return Ok(());
        }
        let (value, _) = invoke(
            Some(self.host_generation),
            method::TX_COMMIT,
            None,
            json!({"tx_id": self.tx_id}),
            None,
            false,
        )?;
        let res = tuple(value)?;
        if res.success {
            Ok(())
        } else {
            Err(error(res.message))
        }
    }

    fn rollback(&self) {
        if self.finished.replace(true) {
            return;
        }
        let _ = invoke(
            Some(self.host_generation),
            method::TX_ROLLBACK,
            None,
            json!({"tx_id": self.tx_id}),
            None,
            false,
        );
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.rollback();
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = u32::from(*chunk.get(1).unwrap_or(&0));
        let b2 = u32::from(*chunk.get(2).unwrap_or(&0));
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((triple >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(triple & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Host calls that are not about one Pipe. Each works during a plugin call (fetch, sync, hooks,
/// actions, routes, pages, MCP entries), on the calling thread.
pub(crate) mod calls {
    use super::*;

    /// Any Pipe, by its keys, with its registered parameters (`mrsm.Pipe(...)`): `instance_keys`
    /// `None` is the request's instance (the Pipe being synced, else the default instance). The
    /// view has every [`Pipe`] operation. An unregistered Pipe has empty parameters until it
    /// is registered or synced (see [`Pipe::with_parameters`]).
    pub fn pipe(
        connector_keys: &str,
        metric_key: &str,
        location_key: Option<&str>,
        instance_keys: Option<&str>,
    ) -> Result<Pipe<'static>> {
        let (value, _) = invoke(
            None,
            method::PIPE,
            None,
            json!({
                "connector_keys": connector_keys,
                "metric_key": metric_key,
                "location_key": location_key,
                "instance_keys": instance_keys,
            }),
            None,
            false,
        )?;
        Pipe::from_metadata(value)
    }

    /// Run a query on a SQL connector (`"sql:main"`, or `pipe.instance_keys()` for a Pipe's
    /// instance) and return its rows.
    pub fn read_sql(connector_keys: &str, query: &str) -> Result<Frame> {
        let (_, data) = invoke(
            None,
            method::READ_SQL,
            None,
            json!({"connector_keys": connector_keys, "query": query}),
            None,
            false,
        )?;
        Ok(data.unwrap_or_else(Frame::empty))
    }

    /// Run a parameterized query (`:name` binds) on a SQL connector and return its rows.
    ///
    /// `params` is a JSON object mapping parameter names to values: scalars, `null`, arrays (bound
    /// as native PostgreSQL arrays for `= ANY(:ids)`), or typed values built with
    /// [`Bound::to_sql_param`], [`sql_timestamptz`], [`sql_timestamp`], [`sql_date`],
    /// [`sql_bytes`], or [`sql_json`].
    pub fn read_sql_with(connector_keys: &str, query: &str, params: &Value) -> Result<Frame> {
        let (_, data) = invoke(
            None,
            method::READ_SQL_WITH,
            None,
            json!({"connector_keys": connector_keys, "query": query, "params": params}),
            None,
            false,
        )?;
        Ok(data.unwrap_or_else(Frame::empty))
    }

    /// Execute a statement on a SQL connector (no rows back).
    pub fn exec_sql(connector_keys: &str, query: &str) -> Result<SuccessTuple> {
        let (value, _) = invoke(
            None,
            method::EXEC_SQL,
            None,
            json!({"connector_keys": connector_keys, "query": query}),
            None,
            false,
        )?;
        tuple(value)
    }

    /// Execute a parameterized statement (`:name` binds) on a SQL connector (no rows back).
    pub fn exec_sql_with(
        connector_keys: &str,
        query: &str,
        params: &Value,
    ) -> Result<SuccessTuple> {
        let (value, _) = invoke(
            None,
            method::EXEC_SQL_WITH,
            None,
            json!({"connector_keys": connector_keys, "query": query, "params": params}),
            None,
            false,
        )?;
        tuple(value)
    }

    /// Run `f` inside a host-managed SQL transaction on `connector_keys`.
    ///
    /// When `lock_key` is `Some(key)`, the host acquires a transaction-scoped advisory lock before
    /// invoking `f`: `pg_advisory_xact_lock` on PostgreSQL-family databases and `BEGIN IMMEDIATE`
    /// on SQLite/GeoPackage. Returning `Ok(value)` commits the transaction; returning `Err` or
    /// panicking rolls it back automatically and releases the lock.
    ///
    /// Statements inside `f` should run through the passed [`Transaction`] (`tx.read_sql_with`,
    /// `tx.exec_sql_with`) rather than opening a second pooled connection via [`read_sql`].
    pub fn transaction<T, F>(connector_keys: &str, lock_key: Option<&str>, f: F) -> Result<T>
    where
        F: FnOnce(&Transaction) -> Result<T>,
    {
        let generation = HOST
            .with(|slot| slot.get().map(|h| h.generation))
            .ok_or_else(|| error(UNAVAILABLE))?;
        let (value, _) = invoke(
            Some(generation),
            method::TX_BEGIN,
            None,
            json!({"connector_keys": connector_keys, "lock_key": lock_key}),
            None,
            false,
        )?;
        let tx_id = value["tx_id"]
            .as_u64()
            .ok_or_else(|| error("host returned no transaction id"))?;
        let tx = Transaction {
            tx_id,
            host_generation: generation,
            finished: Cell::new(false),
            _thread: PhantomData,
        };
        match f(&tx) {
            Ok(out) => {
                tx.commit()?;
                Ok(out)
            }
            Err(err) => {
                tx.rollback();
                Err(err)
            }
        }
    }

    /// The resolved Meerschaum root directory (`MRSM_ROOT_DIR`) of the current call (reflecting
    /// Compose's project root under `run_in_process`, not merely the process environment).
    pub fn root_dir() -> Result<std::path::PathBuf> {
        let (value, _) = invoke(None, method::ROOT_DIR, None, json!({}), None, false)?;
        value
            .as_str()
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from)
            .ok_or_else(|| error("host returned no root_dir"))
    }

    /// Encode a datetime (`DateTime<Utc>`, `NaiveDateTime`, [`Bound`], or ISO-8601 string) as a
    /// typed `timestamptz` SQL parameter for [`read_sql_with`] / [`exec_sql_with`].
    pub fn sql_timestamptz(value: impl Into<Bound>) -> Value {
        value.into().to_sql_param()
    }

    /// Encode an ISO-8601 timestamp as a typed naive `timestamp` SQL parameter.
    pub fn sql_timestamp(iso: &str) -> Value {
        json!({"$mrsm_sql": "timestamp", "value": iso})
    }

    /// Encode a `YYYY-MM-DD` date string as a typed `date` SQL parameter.
    pub fn sql_date(iso: &str) -> Value {
        json!({"$mrsm_sql": "date", "value": iso})
    }

    /// Encode binary data as a typed `bytes` (`BYTEA` / `BLOB`) SQL parameter.
    pub fn sql_bytes(bytes: &[u8]) -> Value {
        json!({"$mrsm_sql": "bytes", "value": encode_base64(bytes)})
    }

    /// Encode any JSON value explicitly as a `json` / `jsonb` SQL parameter (for passing a JSON
    /// array into a `jsonb` column instead of binding it as a SQL array).
    pub fn sql_json(value: Value) -> Value {
        json!({"$mrsm_sql": "json", "value": value})
    }

    /// This plugin's configuration (`plugins:<name>` in the Meerschaum config, the section
    /// Python's `get_plugin_config()` reads), or `null` when it has none.
    pub fn plugin_config() -> Result<Value> {
        invoke(None, method::PLUGIN_CONFIG, None, json!({}), None, false).map(|(value, _)| value)
    }

    /// Replace this plugin's configuration (Python's `write_plugin_config()`).
    pub fn write_plugin_config(config: &Value) -> Result<SuccessTuple> {
        let (value, _) = invoke(
            None,
            method::WRITE_PLUGIN_CONFIG,
            None,
            json!({"config": config}),
            None,
            false,
        )?;
        tuple(value)
    }

    /// The Meerschaum config value at `keys` (`get_config(*keys)`), or `null`.
    pub fn config(keys: &[&str]) -> Result<Value> {
        invoke(
            None,
            method::CONFIG,
            None,
            json!({"keys": keys}),
            None,
            false,
        )
        .map(|(value, _)| value)
    }

    /// Mint an API token labeled `label` (a device's id, say) with `scopes`, owned by the
    /// caller's user, during an API request (a route, console page or MCP entry). `scopes` are
    /// custom scopes this plugin's own routes and MCP entries declare, never `*` or a core scope,
    /// and the caller must hold each. `expiration`: a datetime (`Bound::from(datetime)`, an ISO
    /// string) or Unix seconds (`Bound::Int`); `None` never expires. The secret and API key are
    /// in the result only: hand them to the device now.
    pub fn mint_token(
        label: &str,
        scopes: &[&str],
        expiration: Option<Bound>,
    ) -> Result<MintedToken> {
        let (value, _) = invoke(
            None,
            method::MINT_TOKEN,
            None,
            json!({
                "label": label,
                "scopes": scopes,
                "expiration": Bound::json(expiration.as_ref()),
            }),
            None,
            false,
        )?;
        serde_json::from_value(value).map_err(|e| error(format!("invalid minted token: {e}")))
    }

    /// Revoke (delete) the token `id` during an API request: a token of the caller's user that
    /// carries nothing but this plugin's own scopes (a device's previous token, when it rotates).
    /// Any other id is refused alike, whether it exists or not. The next request with the token
    /// is refused.
    pub fn revoke_token(id: &str) -> Result<SuccessTuple> {
        let (value, _) = invoke(
            None,
            method::REVOKE_TOKEN,
            None,
            json!({"id": id}),
            None,
            false,
        )?;
        tuple(value)
    }

    /// Whether the API server serving this request has begun to drain (it is shutting down): a
    /// handler holding a long poll returns then instead of running into the shutdown's grace
    /// period. `false` outside an API request. Cheap enough to check in a polling loop.
    pub fn draining() -> Result<bool> {
        let (value, _) = invoke(None, method::DRAINING, None, json!({}), None, false)?;
        Ok(value.as_bool().unwrap_or(false))
    }

    /// Whether host calls can run here (inside a plugin call that has a host).
    pub fn available() -> bool {
        HOST.with(|slot| slot.get().is_some())
    }
}

pub(crate) struct HostContext<'a> {
    pub services: &'a dyn HostServices,
    pub request: &'a Request,
    /// The loaded plugin's name.
    pub plugin: &'a str,
}
pub(crate) unsafe extern "C" fn invoke_host(
    context: *mut std::ffi::c_void,
    method: Bytes,
    stream: *mut FFI_ArrowArrayStream,
) -> CallResult {
    sdk::boundary(|| {
        if context.is_null() || stream.is_null() || (method.len != 0 && method.ptr.is_null()) {
            return Err(error("null host call argument"));
        }
        // SAFETY: HostContext and the stream are pinned by invoke_with_host for this call.
        let context = unsafe { &*context.cast::<HostContext<'_>>() };
        // SAFETY: a non-empty `method` has a non-null pointer (checked above), and the plugin
        // keeps the borrowed bytes alive through this synchronous call (`Bytes`' contract); the
        // slice is only read before this call returns.
        let call = HostCall::decode(context.plugin, unsafe { method.as_slice() })?;
        // SAFETY: the stream is moved out once and replaced with an empty stream.
        let data = sdk::frame(unsafe { FFI_ArrowArrayStream::from_raw(stream) })?;
        let reply =
            context
                .services
                .host_call(context.request, &call, data.map(PipeInput::Frame))?;
        if let Some(frame) = &reply.data {
            let out = sdk::stream(frame)?;
            // SAFETY: `stream` is valid and holds the released empty stream `from_raw` left; the
            // plugin imports this one after the call and releases it.
            unsafe { std::ptr::write(stream, out) };
        }
        Ok(reply.value)
    })
}

#[cfg(test)]
mod tests;
