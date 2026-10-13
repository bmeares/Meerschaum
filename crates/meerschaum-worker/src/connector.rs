//! `PythonConnector` (§4.4 "Adapter"): implements the Rust `Connector` and `InstanceConnector`
//! traits by forwarding each call to Python plugin code through the worker pool.
//!
//! Covers `plugin:` connectors (`fetch` / `sync` / `register` / `setup` / `subscribe`) and custom
//! `@make_connector` connectors, both plain ones with `fetch` and `IS_INSTANCE = True` ones
//! implementing the instance interface. Data crosses as Arrow.
//!
//! Users, plugins and tokens use the traits' default implementations, which run on top of the
//! connector's own pipe methods (the same generic implementation as v4's `InstanceConnector`
//! base class); Python overrides of those methods are not forwarded.

use std::sync::Arc;

use indexmap::IndexMap;
use meerschaum_connector::{
    ColumnIndexInfo, Connector, ConnectorError, ConnectorResult, DataQuery, DateBound,
    InstanceConnector, PipeId, PipeKeysEntry, PipesKeys, PipesKeysQuery, SyncOptions, async_trait,
};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value as CValue};
use meerschaum_kernel::Frame;
use rmpv::Value as Wire;
use tokio::sync::mpsc;

use crate::callback::{CallbackHandler, NoCallbacks};
use crate::chunk::FetchChunk;
use crate::error::{WorkerError, WorkerResult};
use crate::pool::WorkerPool;
use crate::protocol::{
    Msg, bound_to_wire, core_to_wire, frame_to_ipc, ipc_to_frame, map_to_wire, wire_map_to_core,
    wire_to_bound, wire_to_core,
};
use crate::worker::{CallOptions, Reply};

/// What the worker reports about a connector (`describe_connector`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorInfo {
    /// The connector type.
    pub connector_type: String,
    /// The label.
    pub label: String,
    /// The Python class name.
    pub class_name: String,
    /// `IS_INSTANCE`.
    pub is_instance: bool,
    /// `IS_THREAD_SAFE`.
    pub is_thread_safe: bool,
    /// `IS_STREAMING`.
    pub is_streaming: bool,
    /// Public callable attributes.
    pub methods: Vec<String>,
    /// Whether `fetch` is defined (not `None`).
    pub has_fetch: bool,
    /// Whether `sync` is defined.
    pub has_sync: bool,
    /// Whether `register` is defined.
    pub has_register: bool,
    /// Whether `subscribe` is defined.
    pub has_subscribe: bool,
}

impl ConnectorInfo {
    fn from_wire(v: &Wire) -> ConnectorInfo {
        let m = match v {
            Wire::Map(m) => Msg(m.clone()),
            _ => Msg::new(),
        };
        let b = |k: &str| m.get(k).and_then(Wire::as_bool).unwrap_or(false);
        let s = |k: &str| m.str(k).unwrap_or_default().to_string();
        ConnectorInfo {
            connector_type: s("type"),
            label: s("label"),
            class_name: s("class_name"),
            is_instance: b("is_instance"),
            is_thread_safe: b("is_thread_safe"),
            is_streaming: b("is_streaming"),
            methods: m
                .get("methods")
                .and_then(Wire::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            has_fetch: b("has_fetch"),
            has_sync: b("has_sync"),
            has_register: b("has_register"),
            has_subscribe: b("has_subscribe"),
        }
    }

    /// Whether the Python class defines `method`.
    pub fn has_method(&self, method: &str) -> bool {
        self.methods.iter().any(|m| m == method)
    }
}

/// Arguments for [`PythonConnector::fetch_chunks`].
#[derive(Debug, Clone, Default)]
pub struct FetchOptions {
    /// `begin` (already resolved: sync time minus backtrack, or explicit).
    pub begin: Option<DateBound>,
    /// `end`.
    pub end: Option<DateBound>,
    /// `params`.
    pub params: Option<Map>,
    /// Any other keyword arguments v4 would pass (`chunksize`, `workers`, `debug`, ...).
    pub kwargs: Map,
    /// Provide a `chunk_hook` (v4 passes one during `Pipe.sync()`; chunks synced through it
    /// are forwarded to the callback handler).
    pub sync_chunks: bool,
}

/// The result of a fetch.
#[derive(Debug, Default)]
pub struct FetchOutcome {
    /// The chunks, in order (one per generator item; a single non-generator result is one
    /// chunk). Empty when streamed through [`PythonConnector::fetch_stream`].
    pub chunks: Vec<FetchChunk>,
    /// The number of chunks the worker sent.
    pub num_chunks: usize,
    /// `fetch()` returned the results of its `chunk_hook` calls (a list of tuples).
    pub hook_results: Option<Vec<SuccessTuple>>,
    /// `fetch()` returned `True` ("being synced in parallel").
    pub in_parallel: bool,
    /// `fetch()` returned `None`.
    pub returned_none: bool,
    /// `fetch()` returned a generator.
    pub generator: bool,
}

/// The result of the connector half of `Pipe.register()`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RegisterOutcome {
    /// The parameters returned by `register(pipe)` (`{}` for `None`).
    pub parameters: Map,
    /// Whether the return value was a dict (v4 warns and ignores it otherwise).
    pub valid: bool,
    /// Whether the connector defines `register()`.
    pub defined: bool,
}

/// A connector implemented in Python.
#[derive(Clone)]
pub struct PythonConnector {
    keys: String,
    connector_type: String,
    label: String,
    pool: WorkerPool,
    info: ConnectorInfo,
    callbacks: Arc<dyn CallbackHandler>,
}

impl std::fmt::Debug for PythonConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonConnector")
            .field("keys", &self.keys)
            .field("info", &self.info)
            .finish()
    }
}

fn split_keys(keys: &str) -> (String, String) {
    match keys.split_once(':') {
        Some((t, l)) => (t.to_string(), l.to_string()),
        None => (keys.to_string(), "main".to_string()),
    }
}

fn pipe_wire(pipe: &Pipe) -> Wire {
    map_to_wire(&pipe.to_state())
}

fn success_from(v: &Wire) -> SuccessTuple {
    match v {
        Wire::Array(items) if items.len() == 2 => SuccessTuple {
            success: items[0].as_bool().unwrap_or(false),
            message: match &items[1] {
                Wire::String(s) => s.as_str().unwrap_or_default().to_string(),
                other => meerschaum_core::pyfmt::py_str(&wire_to_core(other)),
            },
        },
        Wire::Boolean(b) => SuccessTuple {
            success: *b,
            message: if *b { "Success" } else { "Failed" }.into(),
        },
        other => SuccessTuple::fail(format!("Invalid SuccessTuple from Python: {other}")),
    }
}

fn data_query_kwargs(query: &DataQuery) -> Msg {
    let mut kw = Msg::new()
        .set("begin", bound_to_wire(query.begin.as_ref()))
        .set("end", bound_to_wire(query.end.as_ref()));
    if let Some(p) = &query.params {
        kw.insert("params", map_to_wire(p));
    }
    if let Some(cols) = &query.select_columns {
        kw.insert(
            "select_columns",
            Wire::Array(cols.iter().map(|c| Wire::from(c.as_str())).collect()),
        );
    }
    if let Some(cols) = &query.omit_columns {
        kw.insert(
            "omit_columns",
            Wire::Array(cols.iter().map(|c| Wire::from(c.as_str())).collect()),
        );
    }
    if let Some(l) = query.limit {
        kw.insert("limit", l);
    }
    if let Some(o) = &query.order {
        kw.insert("order", o.as_str());
    }
    if query.remote {
        kw.insert("remote", true);
    }
    kw
}

impl PythonConnector {
    /// Describe `keys` in a worker and build the adapter.
    pub async fn connect(keys: &str, pool: WorkerPool) -> WorkerResult<PythonConnector> {
        let reply = pool
            .call(
                "describe_connector",
                Msg::new().set("connector", keys),
                Vec::new(),
                CallOptions::default(),
                None,
            )
            .await?;
        Ok(PythonConnector::with_info(
            keys,
            pool,
            ConnectorInfo::from_wire(&reply.value),
        ))
    }

    /// Build the adapter from known capabilities (e.g. from the manifest) without a worker
    /// round trip.
    pub fn with_info(keys: &str, pool: WorkerPool, info: ConnectorInfo) -> PythonConnector {
        let (connector_type, label) = split_keys(keys);
        PythonConnector {
            keys: format!("{connector_type}:{label}"),
            connector_type,
            label,
            pool,
            info,
            callbacks: Arc::new(NoCallbacks),
        }
    }

    /// Use `handler` for `pipe.sync()` calls made by plugin code (plugin `sync()`,
    /// `chunk_hook`, streaming callbacks).
    pub fn with_callbacks(mut self, handler: Arc<dyn CallbackHandler>) -> PythonConnector {
        self.callbacks = handler;
        self
    }

    /// The capabilities reported by the worker.
    pub fn info(&self) -> &ConnectorInfo {
        &self.info
    }

    /// The pool this connector calls into.
    pub fn pool(&self) -> &WorkerPool {
        &self.pool
    }

    async fn call(&self, op: &str, args: Msg, frames: Vec<Vec<u8>>) -> WorkerResult<Reply> {
        self.pool
            .call(
                op,
                args,
                frames,
                CallOptions::default(),
                Some(Arc::clone(&self.callbacks)),
            )
            .await
    }

    fn fetch_args(&self, pipe: &Pipe, opts: &FetchOptions) -> Msg {
        let mut kwargs = opts.kwargs.clone();
        if opts.sync_chunks {
            kwargs.insert("sync_chunks".into(), CValue::Bool(true));
        }
        let mut args = Msg::new()
            .set("connector", self.keys.as_str())
            .set("pipe", pipe_wire(pipe))
            .set("begin", bound_to_wire(opts.begin.as_ref()))
            .set("end", bound_to_wire(opts.end.as_ref()))
            .set("kwargs", map_to_wire(&kwargs));
        if let Some(p) = &opts.params {
            args.insert("params", map_to_wire(p));
        }
        args
    }

    fn fetch_outcome(reply: Reply) -> FetchOutcome {
        let m = match &reply.value {
            Wire::Map(m) => Msg(m.clone()),
            _ => Msg::new(),
        };
        let hook_results = m
            .get("hook_results")
            .and_then(Wire::as_array)
            .map(|a| a.iter().map(success_from).collect());
        FetchOutcome {
            num_chunks: m.u64("chunks").unwrap_or(reply.chunks.len() as u64) as usize,
            chunks: reply.chunks,
            hook_results,
            in_parallel: m.get("parallel").and_then(Wire::as_bool).unwrap_or(false),
            returned_none: m.get("none").and_then(Wire::as_bool).unwrap_or(false),
            generator: m.get("generator").and_then(Wire::as_bool).unwrap_or(false),
        }
    }

    /// `connector.fetch(pipe, begin, end, ...)`, collecting every chunk.
    pub async fn fetch_chunks(
        &self,
        pipe: &Pipe,
        opts: &FetchOptions,
    ) -> WorkerResult<FetchOutcome> {
        let reply = self
            .call("fetch", self.fetch_args(pipe, opts), Vec::new())
            .await?;
        Ok(Self::fetch_outcome(reply))
    }

    /// `connector.fetch(...)`, streaming each chunk into the returned receiver as the worker
    /// produces it (generators are consumed lazily on the Python side). The task resolves to
    /// the outcome (with no chunks) once the generator is exhausted.
    pub fn fetch_stream(
        &self,
        pipe: &Pipe,
        opts: &FetchOptions,
        buffer: usize,
    ) -> (
        mpsc::Receiver<FetchChunk>,
        tokio::task::JoinHandle<WorkerResult<FetchOutcome>>,
    ) {
        let (tx, rx) = mpsc::channel(buffer.max(1));
        let this = self.clone();
        let args = self.fetch_args(pipe, opts);
        let task = tokio::spawn(async move {
            let reply = this
                .pool
                .call(
                    "fetch",
                    args,
                    Vec::new(),
                    CallOptions {
                        timeout: None,
                        chunks: Some(tx),
                    },
                    Some(Arc::clone(&this.callbacks)),
                )
                .await?;
            Ok(Self::fetch_outcome(reply))
        });
        (rx, task)
    }

    /// The plugin's (or custom connector's) `sync(pipe, **kwargs)`. `pipe.sync(df)` calls made
    /// by the plugin reach the callback handler.
    pub async fn sync(&self, pipe: &Pipe, kwargs: &Map) -> WorkerResult<SuccessTuple> {
        let mut kwargs = kwargs.clone();
        let begin = kwargs.shift_remove("begin");
        let end = kwargs.shift_remove("end");
        let args = Msg::new()
            .set("connector", self.keys.as_str())
            .set("pipe", pipe_wire(pipe))
            .set(
                "begin",
                begin.as_ref().map(core_to_wire).unwrap_or(Wire::Nil),
            )
            .set("end", end.as_ref().map(core_to_wire).unwrap_or(Wire::Nil))
            .set("kwargs", map_to_wire(&kwargs));
        let reply = self.call("sync", args, Vec::new()).await?;
        Ok(success_from(&reply.value))
    }

    /// The connector half of `Pipe.register()`: `connector.register(pipe)`.
    pub async fn register(&self, pipe: &Pipe) -> WorkerResult<RegisterOutcome> {
        let reply = self
            .call(
                "register",
                Msg::new()
                    .set("connector", self.keys.as_str())
                    .set("pipe", pipe_wire(pipe)),
                Vec::new(),
            )
            .await?;
        let m = match &reply.value {
            Wire::Map(m) => Msg(m.clone()),
            _ => Msg::new(),
        };
        Ok(RegisterOutcome {
            parameters: match m.get("parameters") {
                Some(Wire::Map(p)) => wire_map_to_core(p),
                _ => Map::new(),
            },
            valid: m.get("valid").and_then(Wire::as_bool).unwrap_or(false),
            defined: m.get("defined").and_then(Wire::as_bool).unwrap_or(false),
        })
    }

    /// Call a method on the Python instance connector with `filter_arguments`
    /// (`method(pipe, *args, **kwargs)`). `frame` is passed as the keyword `frame_arg`.
    pub async fn call_method(
        &self,
        method: &str,
        pipe: Option<&Pipe>,
        args: Vec<Wire>,
        kwargs: Msg,
        frame: Option<(&str, &Frame)>,
    ) -> WorkerResult<Reply> {
        let mut msg = Msg::new()
            .set("connector", self.keys.as_str())
            .set("method", method)
            .set("args", Wire::Array(args))
            .set("kwargs", Wire::from(kwargs));
        if let Some(p) = pipe {
            msg.insert("pipe", pipe_wire(p));
        }
        let mut frames = Vec::new();
        if let Some((name, df)) = frame {
            msg.insert("frame_arg", name);
            frames.push(frame_to_ipc(df)?);
        }
        self.call("instance", msg, frames).await
    }

    async fn method_value(&self, method: &str, pipe: &Pipe, kwargs: Msg) -> ConnectorResult<Wire> {
        Ok(self
            .call_method(method, Some(pipe), Vec::new(), kwargs, None)
            .await?
            .value)
    }

    async fn method_success(&self, method: &str, pipe: &Pipe, kwargs: Msg) -> SuccessTuple {
        match self.method_value(method, pipe, kwargs).await {
            Ok(v) => success_from(&v),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    async fn method_frame(
        &self,
        method: &str,
        pipe: &Pipe,
        kwargs: Msg,
    ) -> ConnectorResult<Option<Frame>> {
        let reply = self
            .call_method(method, Some(pipe), Vec::new(), kwargs, None)
            .await?;
        match reply.frames.first() {
            Some(data) => Ok(Some(ipc_to_frame(data)?)),
            None => match reply.value {
                Wire::Nil => Ok(None),
                Wire::Array(_) => Ok(Some(
                    FetchChunk::Docs(crate::protocol::wire_to_docs(&reply.value)?).into_frame()?,
                )),
                other => Err(ConnectorError::Invalid(format!(
                    "`{method}()` returned {other} instead of a DataFrame."
                ))),
            },
        }
    }
}

impl From<crate::error::WorkerError> for SuccessTuple {
    fn from(e: WorkerError) -> Self {
        SuccessTuple::fail(e.to_string())
    }
}

#[async_trait]
impl Connector for PythonConnector {
    fn connector_type(&self) -> &str {
        &self.connector_type
    }

    fn label(&self) -> &str {
        &self.label
    }

    fn is_thread_safe(&self) -> bool {
        self.info.is_thread_safe
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        if !self.info.has_method("test_connection") {
            return Ok(true);
        }
        let reply = self
            .call_method("test_connection", None, Vec::new(), Msg::new(), None)
            .await?;
        Ok(reply.value.as_bool().unwrap_or(false))
    }

    /// Collect every chunk and build one frame. Docs and columns go through the kernel's
    /// `convert_dtypes` only; callers that sync should prefer [`PythonConnector::fetch_chunks`]
    /// and pass each chunk's [`FetchChunk::into_pipe_input`] to `pipe_enforce_dtypes`, which
    /// applies v4's datetime parsing with the pipe's dtypes.
    async fn fetch(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Option<Frame>> {
        let outcome = self
            .fetch_chunks(
                pipe,
                &FetchOptions {
                    begin: query.begin.clone(),
                    end: query.end.clone(),
                    params: query.params.clone(),
                    ..Default::default()
                },
            )
            .await?;
        if outcome.chunks.is_empty() {
            return Ok(None);
        }
        let mut batches = Vec::new();
        for chunk in outcome.chunks {
            let frame = chunk.into_frame()?;
            batches.push(
                frame
                    .to_record_batch()
                    .map_err(|e| ConnectorError::Invalid(e.to_string()))?,
            );
        }
        if batches.len() == 1 {
            return Ok(Some(
                Frame::from_record_batch(&batches[0])
                    .map_err(|e| ConnectorError::Invalid(e.to_string()))?,
            ));
        }
        // Chunks may differ in schema; align them by concatenating their documents.
        let mut docs = Vec::new();
        for b in &batches {
            let f =
                Frame::from_record_batch(b).map_err(|e| ConnectorError::Invalid(e.to_string()))?;
            docs.extend(meerschaum_connector::pipe_ops::frame_to_docs(&f));
        }
        Ok(Some(
            meerschaum_kernel::docs::frame_from_docs(&docs)
                .map_err(|e| ConnectorError::Invalid(e.to_string()))?,
        ))
    }
}

fn wire_to_pipe_id(v: &Wire) -> Option<PipeId> {
    match v {
        Wire::Nil => None,
        Wire::Integer(i) => i.as_i64().map(PipeId::Int),
        Wire::String(s) => s.as_str().map(|s| PipeId::Str(s.to_string())),
        other => Some(PipeId::Str(meerschaum_core::pyfmt::py_str(&wire_to_core(
            other,
        )))),
    }
}

fn entry_from_wire(v: &Wire) -> Option<PipeKeysEntry> {
    match wire_to_core(v) {
        CValue::List(items) => PipeKeysEntry::from_list(&items),
        _ => None,
    }
}

#[async_trait]
impl InstanceConnector for PythonConnector {
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.method_success("register_pipe", pipe, Msg::new()).await
    }

    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        Ok(
            match self
                .method_value("get_pipe_attributes", pipe, Msg::new())
                .await?
            {
                Wire::Map(m) => wire_map_to_core(&m),
                _ => Map::new(),
            },
        )
    }

    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        Ok(wire_to_pipe_id(
            &self.method_value("get_pipe_id", pipe, Msg::new()).await?,
        ))
    }

    async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        self.method_success("edit_pipe", pipe, Msg::new().set("patch", patch))
            .await
    }

    async fn delete_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.method_success("delete_pipe", pipe, Msg::new()).await
    }

    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        let strs = |v: &[String]| Wire::Array(v.iter().map(|s| Wire::from(s.as_str())).collect());
        let mut kw = Msg::new()
            .set("connector_keys", strs(&query.connector_keys))
            .set("metric_keys", strs(&query.metric_keys))
            .set(
                "location_keys",
                Wire::Array(
                    query
                        .location_keys
                        .iter()
                        .map(|l| l.as_deref().map(Wire::from).unwrap_or(Wire::Nil))
                        .collect(),
                ),
            )
            .set("tags", strs(&query.tags));
        if let Some(p) = &query.params {
            kw.insert("params", map_to_wire(p));
        }
        let reply = self
            .call_method("fetch_pipes_keys", None, Vec::new(), kw, None)
            .await?;
        match &reply.value {
            Wire::Map(m) => {
                let mut out = IndexMap::new();
                for (k, v) in m {
                    let id = wire_to_pipe_id(k).unwrap_or(PipeId::Str(String::new()));
                    let id = match id {
                        PipeId::Str(s) => PipeId::parse(&s),
                        other => other,
                    };
                    if let Some(entry) = entry_from_wire(v) {
                        out.insert(id, entry);
                    }
                }
                Ok(PipesKeys::Dict(out))
            }
            Wire::Array(items) => Ok(PipesKeys::List(
                items.iter().filter_map(entry_from_wire).collect(),
            )),
            Wire::Nil => Ok(PipesKeys::List(Vec::new())),
            other => Err(ConnectorError::Invalid(format!(
                "`fetch_pipes_keys()` returned {other}."
            ))),
        }
    }

    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        let v = self.method_value("pipe_exists", pipe, Msg::new()).await?;
        Ok(v.as_bool().unwrap_or(false))
    }

    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.method_success("drop_pipe", pipe, Msg::new()).await
    }

    async fn drop_pipe_indices(&self, pipe: &Pipe) -> SuccessTuple {
        if !self.info.has_method("drop_pipe_indices") {
            return SuccessTuple::fail(format!(
                "Cannot drop indices for instance connectors of type '{}'.",
                self.connector_type
            ));
        }
        self.method_success("drop_pipe_indices", pipe, Msg::new())
            .await
    }

    async fn create_pipe_indices(&self, pipe: &Pipe) -> SuccessTuple {
        if !self.info.has_method("create_pipe_indices") {
            return SuccessTuple::fail(format!(
                "Cannot create indices for instance connectors of type '{}'.",
                self.connector_type
            ));
        }
        self.method_success("create_pipe_indices", pipe, Msg::new())
            .await
    }

    async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple {
        let mut kw = Msg::new()
            .set("check_existing", opts.check_existing)
            .set(
                "chunksize",
                opts.chunksize.map(Wire::from).unwrap_or(Wire::Nil),
            )
            .set("begin", bound_to_wire(opts.begin.as_ref()))
            .set("end", bound_to_wire(opts.end.as_ref()));
        for (k, v) in &opts.extra {
            // v4 caches `new_special_cols` on the pipe; it is never a `sync_pipe()` argument.
            if k != "new_special_cols" {
                kw.insert(k, core_to_wire(v));
            }
        }
        match self
            .call_method("sync_pipe", Some(pipe), Vec::new(), kw, Some(("df", &df)))
            .await
        {
            Ok(reply) => success_from(&reply.value),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        if !self.info.has_method("clear_pipe") {
            return SuccessTuple::fail(format!(
                "`clear_pipe()` is not implemented for instance connectors of type '{}'.",
                self.connector_type
            ));
        }
        self.method_success("clear_pipe", pipe, data_query_kwargs(query))
            .await
    }

    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        self.method_frame("get_pipe_data", pipe, data_query_kwargs(query))
            .await
    }

    async fn get_backtrack_data(
        &self,
        pipe: &Pipe,
        backtrack_minutes: i64,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        if !self.info.has_method("get_backtrack_data") {
            // The trait default: `get_sync_time` + `get_pipe_data`.
            let sync_time = self
                .get_sync_time(pipe, query.params.as_ref(), true)
                .await?;
            let begin = match (&query.begin, sync_time) {
                (Some(b), _) => Some(b.clone()),
                (None, Some(DateBound::Int(i))) => Some(DateBound::Int(i - backtrack_minutes)),
                (None, Some(DateBound::Datetime(dt))) => Some(DateBound::Datetime(
                    meerschaum_kernel::value::DateTimeValue {
                        utc: dt.utc - chrono::Duration::minutes(backtrack_minutes),
                        tz: dt.tz,
                    },
                )),
                (None, None) => None,
            };
            let q = DataQuery {
                begin,
                ..query.clone()
            };
            return self.get_pipe_data(pipe, &q).await;
        }
        let kw = data_query_kwargs(query).set("backtrack_minutes", backtrack_minutes);
        self.method_frame("get_backtrack_data", pipe, kw).await
    }

    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        let mut kw = Msg::new().set("newest", newest);
        if let Some(p) = params {
            kw.insert("params", map_to_wire(p));
        }
        let v = self.method_value("get_sync_time", pipe, kw).await?;
        Ok(wire_to_bound(&v))
    }

    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        let v = self
            .method_value("get_pipe_rowcount", pipe, data_query_kwargs(query))
            .await?;
        Ok(v.as_i64())
    }

    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        if !self.info.has_method("get_pipe_columns_types") {
            return Ok(IndexMap::new());
        }
        Ok(
            match self
                .method_value("get_pipe_columns_types", pipe, Msg::new())
                .await?
            {
                Wire::Map(m) => wire_map_to_core(&m)
                    .into_iter()
                    .map(|(k, v)| (k, meerschaum_core::pyfmt::py_str(&v)))
                    .collect(),
                _ => IndexMap::new(),
            },
        )
    }

    async fn get_pipe_columns_indices(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, Vec<ColumnIndexInfo>>> {
        if !self.info.has_method("get_pipe_columns_indices") {
            return Ok(IndexMap::new());
        }
        let v = self
            .method_value("get_pipe_columns_indices", pipe, Msg::new())
            .await?;
        let json = crate::protocol::wire_to_json(&v);
        Ok(serde_json::from_value(json).unwrap_or_default())
    }

    async fn get_pipe_index_names(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        Ok(
            match self
                .method_value("get_pipe_index_names", pipe, Msg::new())
                .await?
            {
                Wire::Map(m) => wire_map_to_core(&m),
                _ => Map::new(),
            },
        )
    }

    async fn deduplicate_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        if !self.info.has_method("deduplicate_pipe") {
            return SuccessTuple::fail(format!(
                "Deduplication is not supported for instance connectors of type '{}'.",
                self.connector_type
            ));
        }
        self.method_success("deduplicate_pipe", pipe, data_query_kwargs(query))
            .await
    }

    async fn get_pipe_size(&self, pipe: &Pipe) -> ConnectorResult<Option<i64>> {
        if !self.info.has_method("get_pipe_size") {
            return Err(ConnectorError::NotImplemented(format!(
                "`get_pipe_size()` is not implemented for instance connectors of type '{}'.",
                self.connector_type
            )));
        }
        Ok(self
            .method_value("get_pipe_size", pipe, Msg::new())
            .await?
            .as_i64())
    }

    async fn compress_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if !self.info.has_method("compress_pipe") {
            return SuccessTuple::fail(format!(
                "Compression is not supported for instance connectors of type '{}'.",
                self.connector_type
            ));
        }
        self.method_success("compress_pipe", pipe, Msg::new()).await
    }

    async fn decompress_pipe(&self, pipe: &Pipe, no_policy: bool) -> SuccessTuple {
        if !self.info.has_method("decompress_pipe") {
            return SuccessTuple::fail(format!(
                "Decompression is not supported for instance connectors of type '{}'.",
                self.connector_type
            ));
        }
        self.method_success(
            "decompress_pipe",
            pipe,
            Msg::new().set("no_policy", no_policy),
        )
        .await
    }

    async fn vacuum_pipe(&self, pipe: &Pipe, full: bool) -> SuccessTuple {
        if !self.info.has_method("vacuum_pipe") {
            return SuccessTuple::fail(format!(
                "Vacuuming is not supported for instance connectors of type '{}'.",
                self.connector_type
            ));
        }
        self.method_success("vacuum_pipe", pipe, Msg::new().set("full", full))
            .await
    }

    async fn analyze_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if !self.info.has_method("analyze_pipe") {
            return SuccessTuple::fail(format!(
                "Analyzing is not supported for instance connectors of type '{}'.",
                self.connector_type
            ));
        }
        self.method_success("analyze_pipe", pipe, Msg::new()).await
    }

    async fn partition_pipe(&self, pipe: &Pipe, chunk_minutes: Option<i64>) -> SuccessTuple {
        if !self.info.has_method("partition_pipe") {
            return SuccessTuple::fail(format!(
                "Repartitioning is not supported for instance connectors of type '{}'.",
                self.connector_type
            ));
        }
        self.method_success(
            "partition_pipe",
            pipe,
            Msg::new().set(
                "chunk_minutes",
                chunk_minutes.map(Wire::from).unwrap_or(Wire::Nil),
            ),
        )
        .await
    }
}
