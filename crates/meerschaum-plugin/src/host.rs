//! Trusted-library loading and safe connector adapters, and, for plugin authors, the host calls
//! that are not about one Pipe ([`pipe`], [`read_sql`], [`exec_sql`], [`plugin_config`],
//! [`write_plugin_config`], [`config`]; see [`crate::callbacks`]).
pub use crate::callbacks::calls::{
    available, config, exec_sql, pipe, plugin_config, read_sql, write_plugin_config,
};
use crate::{
    Frame, Result,
    abi::*,
    error,
    sdk::{self, Reply, Request},
};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use indexmap::IndexMap;
use libloading::Library;
use meerschaum_connector::{
    Connector, ConnectorError, ConnectorResult, DataQuery, DateBound, InstanceConnector, PipeId,
    PipeKeysEntry, PipesKeys, PipesKeysQuery, SyncOptions, async_trait,
};
use meerschaum_core::{Map, Pipe, SuccessTuple};
use meerschaum_kernel::enforce::PipeInput;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

thread_local! { static ACTIVE: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) }; }
struct ActiveCall;
impl Drop for ActiveCall {
    fn drop(&mut self) {
        ACTIVE.with(|active| {
            active.borrow_mut().pop();
        });
    }
}

pub struct LoadedPlugin {
    // Library must remain alive through all callbacks and imported Arrow buffers.
    _library: Library,
    table: &'static MrsmPluginV1,
    invoke_host: Option<InvokeWithHost>,
    invoke_rows: Option<crate::rows_abi::InvokeRows>,
    gate: Mutex<()>,
    pub name: String,
    pub version: String,
    pub manifest: Value,
}
impl LoadedPlugin {
    /// Load a trusted native library and validate its ABI and manifest.
    /// # Safety
    /// Loading a library executes arbitrary native code. The library must obey ABI v1,
    /// including valid pointers, ownership, no unwinding, and no background use after unload.
    pub unsafe fn load(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        meerschaum_core::dprint!("Loading native plugin {}.", path.as_ref().display());
        // SAFETY: trust and ABI conformance are the caller's responsibility.
        let library = unsafe { Library::new(path.as_ref()) }.map_err(error)?;
        // SAFETY: entry symbol has the documented C signature.
        let entry =
            unsafe { library.get::<unsafe extern "C" fn() -> *const MrsmPluginV1>(ENTRY_POINT) }
                .map_err(error)?;
        // SAFETY: entry returns a static table or null.
        let ptr = unsafe { entry() };
        if ptr.is_null() {
            return Err(error("null plugin vtable"));
        }
        // SAFETY: the first two u32 fields are the permanent ABI prefix. Do not read a v1
        // reference until the version and size match (older tables may be shorter).
        let (version, size) =
            unsafe { (ptr.cast::<u32>().read(), ptr.cast::<u32>().add(1).read()) };
        validate_header(version, size)?;
        // SAFETY: validated v1 layout; the library owns this table until unload.
        let table = unsafe { &*ptr };
        // SAFETY: validated callback, result released with the same library's allocator.
        let metadata = unsafe { decode(table, (table.describe)()) }?;
        let name = metadata["name"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| error("missing plugin name"))?
            .to_owned();
        let version = metadata["version"]
            .as_str()
            .ok_or_else(|| error("missing plugin version"))?
            .to_owned();
        if metadata["abi_version"] != ABI_VERSION {
            return Err(error("metadata ABI version mismatch"));
        }
        let manifest = metadata["manifest"].clone();
        validate_manifest(&manifest)?;
        if manifest["plugins"][&name]["version"] != version {
            return Err(error("manifest plugin version mismatch"));
        }
        if manifest["plugins"][&name]["name"] != name {
            return Err(error("manifest plugin name mismatch"));
        }
        // Optional extension: old libraries continue to use the original v1 entry point.
        let invoke_host = unsafe { library.get::<InvokeWithHost>(HOST_ENTRY_POINT) }
            .ok()
            .map(|symbol| *symbol);
        let invoke_rows = unsafe {
            library.get::<crate::rows_abi::InvokeRows>(crate::rows_abi::ROWS_ENTRY_POINT)
        }
        .ok()
        .map(|symbol| *symbol);
        Ok(Arc::new(Self {
            _library: library,
            table,
            invoke_host,
            invoke_rows,
            gate: Mutex::new(()),
            name,
            version,
            manifest,
        }))
    }

    /// Serialized call; JSON config and context are preserved, data travels only through Arrow.
    pub fn invoke(&self, request: &Request, data: Option<&Frame>) -> Result<Reply> {
        self.invoke_with_host(request, data, None)
    }

    /// Invoke with call-scoped host services when the library exports the optional extension.
    pub fn invoke_with_host(
        &self,
        request: &Request,
        data: Option<&Frame>,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<Reply> {
        let (value, data) = self.invoke_input(request, data, host)?;
        Ok(Reply {
            value,
            data: data.map(input_frame).transpose()?,
        })
    }

    /// [`invoke_with_host`](Self::invoke_with_host) with the reply's data as the plugin sent
    /// it: an Arrow frame, or the documents of a rows-ABI (single-file) plugin, which a sync
    /// treats like a Python plugin's `list[dict]` (`parse_df_datetimes` with the pipe's dtypes).
    pub fn invoke_input(
        &self,
        request: &Request,
        data: Option<&Frame>,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<(Value, Option<PipeInput>)> {
        let nested = ACTIVE.with(|active| !active.borrow().is_empty());
        if ACTIVE.with(|active| active.borrow().contains(&self.name)) {
            return Err(error(format!(
                "recursive call into native plugin '{}'",
                self.name
            )));
        }
        // Nested calls must never wait for a plugin held by another host-call chain.
        let _guard = if nested {
            self.gate
                .try_lock()
                .map_err(|_| error("native plugin is busy during a host callback"))?
        } else {
            self.gate
                .lock()
                .map_err(|_| error("plugin call lock poisoned"))?
        };
        self.invoke_entered(request, data, host)
    }

    /// A call of a stream method (`next_batch`, `ack_batches`, `close_subscription`,
    /// `executor.next_logs`, `executor.close_logs`): not serialized with the plugin's other
    /// calls, so a blocking `next_batch` holds up neither `fetch` nor another stream. The plugin
    /// locks each handle itself (the SDK does). Without host services; see
    /// [`invoke_stream_with_host`](Self::invoke_stream_with_host).
    pub fn invoke_stream(&self, request: &Request) -> Result<Reply> {
        self.invoke_stream_with_host(request, None)
    }

    /// [`invoke_stream`](Self::invoke_stream) with host services: a stream's batches and an
    /// executor's log follow make host calls with their connector's or executor's services, and
    /// the server call kinds (a route, MCP entry or page) on behalf of their caller.
    pub fn invoke_stream_with_host(
        &self,
        request: &Request,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<Reply> {
        if ACTIVE.with(|active| active.borrow().contains(&self.name)) {
            return Err(error(format!(
                "recursive call into native plugin '{}'",
                self.name
            )));
        }
        self.invoke_entered(request, None, host)
            .and_then(|(value, data)| {
                Ok(Reply {
                    value,
                    data: data.map(input_frame).transpose()?,
                })
            })
    }

    /// A JSON-only call of a server call kind (`api_route`, `mcp_tool`, `mcp_resource`,
    /// `mcp_prompt`): like [`invoke_stream`](Self::invoke_stream), it is not serialized with the
    /// plugin's other calls, so concurrent HTTP requests and MCP calls run in parallel (as
    /// FastAPI serves `@api_plugin` routes). The plugin must be thread-safe for these call kinds;
    /// the SDK builds a fresh `P::default()` per call. No host services are available here; see
    /// [`call_json_concurrent_with_host`](Self::call_json_concurrent_with_host).
    pub fn call_json_concurrent(&self, method: &str, context: Value) -> Result<Value> {
        self.call_json_concurrent_with_host(method, context, None)
    }

    /// [`call_json_concurrent`](Self::call_json_concurrent) with host services.
    pub fn call_json_concurrent_with_host(
        &self,
        method: &str,
        context: Value,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<Value> {
        self.invoke_stream_with_host(&json_request(method, context), host)
            .map(|reply| reply.value)
    }

    fn invoke_entered(
        &self,
        request: &Request,
        data: Option<&Frame>,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<(Value, Option<PipeInput>)> {
        ACTIVE.with(|active| active.borrow_mut().push(self.name.clone()));
        let _active = ActiveCall;
        if let Some(invoke) = self.invoke_rows {
            return self.invoke_rows(invoke, request, data, host);
        }
        let bytes = serde_json::to_vec(request).map_err(error)?;
        let mut input = data
            .map(sdk::stream)
            .transpose()?
            .unwrap_or_else(FFI_ArrowArrayStream::empty);
        let mut output = FFI_ArrowArrayStream::empty();
        // SAFETY: table is validated and pinned; borrowed bytes and exclusive streams live
        // through the call. Remaining streams are released on success and all error paths.
        let result = if let (Some(invoke), Some(services)) = (self.invoke_host, host) {
            let mut context = crate::callbacks::HostContext {
                services,
                request,
                plugin: &self.name,
            };
            let table = HostV1 {
                abi_version: 1,
                struct_size: std::mem::size_of::<HostV1>() as u32,
                context: (&mut context as *mut crate::callbacks::HostContext<'_>).cast(),
                invoke: crate::callbacks::invoke_host,
                free_bytes: sdk::free_bytes,
            };
            // SAFETY: all callback state lives through this synchronous invocation.
            unsafe { invoke(Bytes::borrowed(&bytes), &mut input, &mut output, &table) }
        } else {
            unsafe { (self.table.invoke)(Bytes::borrowed(&bytes), &mut input, &mut output) }
        };
        // SAFETY: result belongs to this table and is consumed exactly once.
        let value = unsafe { decode(self.table, result) }?;
        Ok((value, sdk::frame(output)?.map(PipeInput::Frame)))
    }
    fn invoke_rows(
        &self,
        invoke: crate::rows_abi::InvokeRows,
        request: &Request,
        data: Option<&Frame>,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<(Value, Option<PipeInput>)> {
        let bytes = serde_json::to_vec(&json!({"request": request, "data": data.map(frame_rows)}))
            .map_err(error)?;
        let mut context = host.map(|services| crate::callbacks::HostContext {
            services,
            request,
            plugin: &self.name,
        });
        let table = context.as_mut().map(|context| crate::rows_abi::RowsHostV1 {
            abi_version: 1,
            struct_size: std::mem::size_of::<crate::rows_abi::RowsHostV1>() as u32,
            context: (context as *mut crate::callbacks::HostContext<'_>).cast(),
            invoke: invoke_rows_host,
            free_bytes: sdk::free_bytes,
        });
        // SAFETY: library and context remain pinned through this synchronous call.
        let result = unsafe {
            invoke(
                Bytes::borrowed(&bytes),
                table.as_ref().map_or(std::ptr::null(), |t| t),
            )
        };
        let mut reply = unsafe { decode(self.table, result) }?;
        let docs = rows_docs(reply["data"].take())?;
        Ok((reply["value"].take(), docs.map(PipeInput::Docs)))
    }

    /// Whether this plugin returns data as JSON rows (the rows ABI of single-file plugins)
    /// rather than as typed Arrow frames.
    pub fn returns_rows(&self) -> bool {
        self.invoke_rows.is_some()
    }
    /// Source adapter for the standard `plugin:<name>` connector, even when no custom
    /// connector types are declared in the manifest.
    pub fn source(self: &Arc<Self>, config: Value) -> NativeConnector {
        NativeConnector {
            plugin: self.clone(),
            connector_type: "plugin".into(),
            label: self.name.clone(),
            config,
            fetch_kwargs: Value::Null,
            host: None,
        }
    }
    pub fn connector(
        self: &Arc<Self>,
        connector_type: &str,
        label: &str,
        config: Value,
    ) -> Result<NativeConnector> {
        let desc = &self.manifest["connector_types"][connector_type];
        if !desc.is_object() && !(connector_type == "plugin" && label == self.name) {
            return Err(error(format!(
                "unknown native connector type: {connector_type}"
            )));
        }
        Ok(NativeConnector {
            plugin: self.clone(),
            connector_type: connector_type.into(),
            label: label.into(),
            config,
            fetch_kwargs: Value::Null,
            host: None,
        })
    }
    /// The executor `<executor_type>:<label>` (a manifest `executor_types` entry).
    pub fn executor(
        self: &Arc<Self>,
        executor_type: &str,
        label: &str,
        config: Value,
    ) -> Result<crate::executor::NativeExecutor> {
        if !self.manifest["executor_types"][executor_type].is_object() {
            return Err(error(format!(
                "unknown native executor type: {executor_type}"
            )));
        }
        Ok(crate::executor::NativeExecutor::new(
            self.clone(),
            executor_type,
            label,
            config,
        ))
    }
    /// A JSON-only call of an extension method (`routes`, `route`, `pages`, `page`, ...):
    /// `context` goes in, the reply's JSON value comes out.
    pub fn call_json(&self, method: &str, context: Value) -> Result<Value> {
        self.call_json_with_host(method, context, None)
    }
    /// [`call_json`](Self::call_json) with host services (a console page's host calls).
    pub fn call_json_with_host(
        &self,
        method: &str,
        context: Value,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<Value> {
        self.invoke_with_host(&json_request(method, context), None, host)
            .map(|reply| reply.value)
    }
    pub fn action(&self, name: &str, kwargs: Value) -> Result<SuccessTuple> {
        self.action_with_host(name, kwargs, None)
    }
    /// [`action`](Self::action) with host services for the action's host calls.
    pub fn action_with_host(
        &self,
        name: &str,
        kwargs: Value,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<SuccessTuple> {
        let reply = self.invoke_with_host(
            &json_request("action", json!({"name":name,"kwargs":kwargs})),
            None,
            host,
        )?;
        serde_json::from_value(reply.value).map_err(error)
    }
    pub fn hook(&self, post: bool, name: &str, kwargs: Value) -> Result<SuccessTuple> {
        self.sync_hook(post, name, kwargs, Value::Null, None)
    }
    /// A sync hook with the synced Pipe in `context.pipe` (as for pipe operations; omitted
    /// when `pipe` is null) and the host services the hook's `Pipe` calls go through.
    pub fn sync_hook(
        &self,
        post: bool,
        name: &str,
        kwargs: Value,
        pipe: Value,
        host: Option<&dyn crate::HostServices>,
    ) -> Result<SuccessTuple> {
        let mut context = json!({"name":name,"kwargs":kwargs});
        if !pipe.is_null() {
            context["pipe"] = pipe;
        }
        let reply = self.invoke_with_host(
            &Request {
                method: if post {
                    "post_sync_hook"
                } else {
                    "pre_sync_hook"
                }
                .into(),
                connector_type: String::new(),
                label: String::new(),
                config: Value::Null,
                context,
            },
            None,
            host,
        )?;
        serde_json::from_value(reply.value).map_err(error)
    }
}
/// A request with no connector: actions, hooks and the extension call kinds.
fn json_request(method: &str, context: Value) -> Request {
    Request {
        method: method.into(),
        connector_type: String::new(),
        label: String::new(),
        config: Value::Null,
        context,
    }
}
use crate::rows::frame_rows;
/// The rows of a rows-ABI payload as documents (`null` is no data, `[]` no rows).
fn rows_docs(value: Value) -> Result<Option<sdk::Docs>> {
    if value.is_null() {
        return Ok(None);
    }
    let rows = serde_json::from_value::<Vec<Value>>(value).map_err(error)?;
    sdk::rows_to_docs(rows).map(Some)
}

/// Reply data as a frame. Documents get the dtypes a Python plugin's documents get
/// (`pd.DataFrame(docs).convert_dtypes()`), never all-`object` columns.
pub(crate) fn input_frame(input: PipeInput) -> Result<Frame> {
    match input {
        PipeInput::Frame(frame) => Ok(frame),
        PipeInput::Docs(docs) => sdk::docs_to_frame(&docs),
        PipeInput::Columns(columns) => {
            meerschaum_kernel::docs::frame_from_columns(&columns).map_err(error)
        }
    }
}
unsafe extern "C" fn invoke_rows_host(context: *mut std::ffi::c_void, bytes: Bytes) -> CallResult {
    sdk::boundary(|| {
        if context.is_null() || (bytes.len != 0 && bytes.ptr.is_null()) {
            return Err(error("null rows host argument"));
        }
        // SAFETY: caller is inside the synchronous scope that owns this context and bytes.
        let context = unsafe { &*context.cast::<crate::callbacks::HostContext<'_>>() };
        let mut call: Value = serde_json::from_slice(unsafe { bytes.as_slice() }).map_err(error)?;
        let docs = rows_docs(call["data"].take())?;
        let call = crate::callbacks::HostCall::from_envelope(context.plugin, call)?;
        let reply =
            context
                .services
                .host_call(context.request, &call, docs.map(PipeInput::Docs))?;
        // The rows ABI carries JSON only: a frame comes back as its rows.
        Ok(match reply.data {
            Some(frame) => Value::Array(frame_rows(&frame)),
            None => reply.value,
        })
    })
}
pub fn validate_header(version: u32, size: u32) -> Result<()> {
    if version != ABI_VERSION {
        return Err(error(format!(
            "unsupported native plugin ABI {version}; expected {ABI_VERSION}"
        )));
    }
    if size < std::mem::size_of::<MrsmPluginV1>() as u32 {
        return Err(error("truncated native plugin vtable"));
    }
    Ok(())
}
pub fn validate_manifest(manifest: &Value) -> Result<()> {
    let schema: Value =
        serde_json::from_str(include_str!("../schema/manifest.schema.json")).map_err(error)?;
    let validator = jsonschema::validator_for(&schema).map_err(error)?;
    validator.validate(manifest).map_err(error)
}
unsafe fn decode(table: &MrsmPluginV1, result: CallResult) -> Result<Value> {
    unsafe { decode_with(table.free_bytes, result) }
}
pub(crate) unsafe fn decode_with(
    free: unsafe extern "C" fn(OwnedBytes),
    result: CallResult,
) -> Result<Value> {
    let bytes = if result.payload.len == 0 {
        Vec::new()
    } else {
        if result.payload.ptr.is_null() {
            return Err(error("null plugin result"));
        }
        // SAFETY: ABI result points to initialized bytes through free_bytes.
        unsafe { std::slice::from_raw_parts(result.payload.ptr, result.payload.len) }.to_vec()
    };
    // SAFETY: return allocation to the exporting library exactly once.
    unsafe { free(result.payload) };
    if result.status == 3 {
        return Err(crate::Error::Unsupported(
            String::from_utf8_lossy(&bytes).into_owned(),
        ));
    }
    if result.status != 0 {
        return Err(error(String::from_utf8_lossy(&bytes)));
    }
    serde_json::from_slice(&bytes).map_err(error)
}

#[derive(Clone)]
pub struct NativeConnector {
    plugin: Arc<LoadedPlugin>,
    connector_type: String,
    label: String,
    config: Value,
    fetch_kwargs: Value,
    host: Option<Arc<dyn crate::HostServices>>,
}
impl NativeConnector {
    pub fn with_host_services(mut self, host: Arc<dyn crate::HostServices>) -> Self {
        self.host = Some(host);
        self
    }

    /// Extra keyword arguments for `fetch`, delivered to the plugin as `context.kwargs`: the
    /// sync's CLI and plugin flags and its `check_existing`, `enforce`, `workers`, ... settings,
    /// as a Python plugin's `fetch(pipe, **kw)` receives them. `begin`, `end` and `params`
    /// travel in `context.query` instead and are dropped here (as `PythonConnector` does).
    pub fn with_fetch_kwargs(mut self, kwargs: &Map) -> Self {
        let mut value = serde_json::to_value(kwargs).unwrap_or(Value::Null);
        if let Some(object) = value.as_object_mut() {
            for key in ["begin", "end", "params"] {
                object.remove(key);
            }
        }
        self.fetch_kwargs = value;
        self
    }

    /// Whether this connector is a pull-model stream source: a custom type declaring
    /// `is_streaming` with `subscribe` in its `methods`, or `plugin:<name>` whose
    /// `data_functions` list `subscribe` (see [`crate::subscribe`]).
    pub fn is_streaming(&self) -> bool {
        let manifest = &self.plugin.manifest;
        let listed = |list: &Value| {
            list.as_array()
                .is_some_and(|items| items.iter().any(|m| m == "subscribe"))
        };
        if self.connector_type == "plugin" {
            return listed(&manifest["plugins"][&self.plugin.name]["data_functions"]);
        }
        let desc = &manifest["connector_types"][&self.connector_type];
        desc["is_streaming"].as_bool().unwrap_or(false) && listed(&desc["methods"])
    }

    /// Open a subscription for `pipe` (blocking; run it on a blocking thread). `kwargs` are the
    /// sync's keyword arguments, as `fetch` receives them.
    pub fn subscribe(
        &self,
        pipe: &Pipe,
        kwargs: Value,
    ) -> ConnectorResult<crate::subscribe::NativeSubscription> {
        let mut context = pipe_context(pipe);
        context["kwargs"] = kwargs;
        let reply = self.call("subscribe", context, None)?;
        let handle = reply.value["handle"]
            .as_u64()
            .ok_or_else(|| ConnectorError::Invalid("subscribe returned no handle".into()))?;
        Ok(crate::subscribe::NativeSubscription::new(
            self.plugin.clone(),
            Request {
                method: String::new(),
                connector_type: self.connector_type.clone(),
                label: self.label.clone(),
                config: self.config.clone(),
                context: Value::Null,
            },
            handle,
            self.host.clone(),
        ))
    }

    pub fn is_instance(&self) -> bool {
        self.plugin.manifest["connector_types"][&self.connector_type]["is_instance"]
            .as_bool()
            .unwrap_or(false)
    }
    /// Escape hatch for optional instance methods and future additive methods.
    pub fn call(
        &self,
        method: &str,
        context: Value,
        data: Option<&Frame>,
    ) -> ConnectorResult<Reply> {
        let (value, data) = self.call_input(method, context, data)?;
        Ok(Reply {
            value,
            data: data
                .map(input_frame)
                .transpose()
                .map_err(|e| ConnectorError::Invalid(e.to_string()))?,
        })
    }

    /// [`call`](Self::call) with the reply's data as the plugin sent it (see
    /// [`LoadedPlugin::invoke_input`]).
    pub fn call_input(
        &self,
        method: &str,
        context: Value,
        data: Option<&Frame>,
    ) -> ConnectorResult<(Value, Option<PipeInput>)> {
        self.plugin
            .invoke_input(
                &Request {
                    method: method.into(),
                    connector_type: self.connector_type.clone(),
                    label: self.label.clone(),
                    config: self.config.clone(),
                    context,
                },
                data,
                self.host.as_deref(),
            )
            .map_err(|e| match e {
                crate::Error::Unsupported(message) => ConnectorError::NotImplemented(message),
                other => ConnectorError::Backend(other.to_string()),
            })
    }
    fn value<T: DeserializeOwned>(&self, method: &str, context: Value) -> ConnectorResult<T> {
        Ok(serde_json::from_value(
            self.call(method, context, None)?.value,
        )?)
    }
    fn success(&self, method: &str, context: Value, data: Option<&Frame>) -> SuccessTuple {
        // PARITY: action / instance failures are (false, message), not raised exceptions.
        match self
            .call(method, context, data)
            .and_then(|r| Ok(serde_json::from_value(r.value)?))
        {
            Ok(v) => v,
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }
}
impl NativeConnector {
    fn fetch_context(&self, pipe: &Pipe, q: &DataQuery) -> Value {
        let mut context = query_context(pipe, q);
        if self.fetch_kwargs.as_object().is_some_and(|o| !o.is_empty()) {
            context["kwargs"] = self.fetch_kwargs.clone();
        }
        context
    }
}

/// One `fetch()` result as a chunk sequence (`FetchedKind::Data` without a generator, or
/// `Nothing` when the plugin returned no data).
struct SingleChunk {
    chunk: Option<PipeInput>,
    fetched: bool,
}

#[async_trait]
impl meerschaum_connector::ChunkStream for SingleChunk {
    async fn next_chunk(&mut self) -> ConnectorResult<Option<PipeInput>> {
        Ok(self.chunk.take())
    }

    async fn finish(&mut self) -> ConnectorResult<meerschaum_connector::FetchedKind> {
        Ok(if self.fetched {
            meerschaum_connector::FetchedKind::Data { generator: false }
        } else {
            meerschaum_connector::FetchedKind::Nothing
        })
    }
}

fn pipe_context(pipe: &Pipe) -> Value {
    json!({"pipe": {"keys":pipe.keys(), "parameters":pipe.raw_parameters(), "temporary":pipe.temporary}})
}
fn query_context(pipe: &Pipe, q: &DataQuery) -> Value {
    let mut v = pipe_context(pipe);
    v["query"] = json!({"select_columns":q.select_columns,"omit_columns":q.omit_columns,"begin":bound(q.begin.as_ref()),"end":bound(q.end.as_ref()),"params":q.params,"limit":q.limit,"order":q.order,"as_chunks":q.as_chunks,"remote":q.remote});
    v
}
fn bound(b: Option<&DateBound>) -> Value {
    match b {
        None => Value::Null,
        Some(DateBound::Int(i)) => json!(i),
        Some(d) => json!(d.isoformat()),
    }
}
#[async_trait]
impl Connector for NativeConnector {
    fn connector_type(&self) -> &str {
        &self.connector_type
    }
    fn label(&self) -> &str {
        &self.label
    }
    fn is_thread_safe(&self) -> bool {
        self.plugin.manifest["connector_types"][&self.connector_type]["is_thread_safe"]
            .as_bool()
            .unwrap_or(false)
    }
    async fn test_connection(&self) -> ConnectorResult<bool> {
        self.value("test_connection", json!({}))
    }
    async fn fetch(&self, pipe: &Pipe, q: &DataQuery) -> ConnectorResult<Option<Frame>> {
        let context = self.fetch_context(pipe, q);
        let connector = self.clone();
        tokio::task::spawn_blocking(move || connector.call("fetch", context, None).map(|r| r.data))
            .await
            .map_err(|e| ConnectorError::Backend(e.to_string()))?
    }

    /// A rows-ABI (single-file) plugin's `fetch()` returns documents, as a Python plugin's
    /// `fetch()` returning `list[dict]` does, so a sync receives them as documents: the pipe's
    /// `enforce_dtypes` then parses ISO datetime strings with the pipe's dtypes and timezone
    /// (v4's `parse_df_datetimes`). Plugins that return Arrow frames use [`Connector::fetch`].
    async fn fetch_chunked(
        &self,
        pipe: &Pipe,
        q: &DataQuery,
        _sync_chunks: bool,
    ) -> ConnectorResult<Option<meerschaum_connector::ChunkedFetch>> {
        if !self.plugin.returns_rows() {
            return Ok(None);
        }
        let context = self.fetch_context(pipe, q);
        let connector = self.clone();
        let data = tokio::task::spawn_blocking(move || {
            connector
                .call_input("fetch", context, None)
                .map(|(_, data)| data)
        })
        .await
        .map_err(|e| ConnectorError::Backend(e.to_string()))??;
        Ok(Some(meerschaum_connector::ChunkedFetch {
            chunks: Box::new(SingleChunk {
                fetched: data.is_some(),
                chunk: data,
            }),
        }))
    }
}
#[async_trait]
impl InstanceConnector for NativeConnector {
    // Locks (`parameters['locks']`) apply to native plugin instances too.
    async fn delete_pipe(&self, p: &Pipe) -> SuccessTuple {
        if let Some(refused) =
            meerschaum_connector::lock_refusal(self, p, meerschaum_core::access::LockOp::Delete)
                .await
        {
            return refused;
        }
        self.success("delete_pipe", pipe_context(p), None)
    }
    async fn drop_pipe_indices(&self, p: &Pipe) -> SuccessTuple {
        if let Some(refused) =
            meerschaum_connector::lock_refusal(self, p, meerschaum_core::access::LockOp::Schema)
                .await
        {
            return refused;
        }
        self.success("drop_pipe_indices", pipe_context(p), None)
    }
    async fn create_pipe_indices(&self, p: &Pipe) -> SuccessTuple {
        self.success("create_pipe_indices", pipe_context(p), None)
    }
    async fn compress_pipe(&self, p: &Pipe) -> SuccessTuple {
        self.success("compress_pipe", pipe_context(p), None)
    }
    async fn analyze_pipe(&self, p: &Pipe) -> SuccessTuple {
        self.success("analyze_pipe", pipe_context(p), None)
    }
    async fn edit_pipe(&self, p: &Pipe, patch: bool) -> SuccessTuple {
        if let Some(refused) = meerschaum_connector::edit_refusal(self, p, patch).await {
            return refused;
        }
        let mut c = pipe_context(p);
        c["patch"] = json!(patch);
        self.success("edit_pipe", c, None)
    }
    async fn decompress_pipe(&self, p: &Pipe, no_policy: bool) -> SuccessTuple {
        let mut c = pipe_context(p);
        c["no_policy"] = json!(no_policy);
        self.success("decompress_pipe", c, None)
    }
    async fn vacuum_pipe(&self, p: &Pipe, full: bool) -> SuccessTuple {
        let mut c = pipe_context(p);
        c["full"] = json!(full);
        self.success("vacuum_pipe", c, None)
    }
    async fn partition_pipe(&self, p: &Pipe, chunk_minutes: Option<i64>) -> SuccessTuple {
        for op in [
            meerschaum_core::access::LockOp::Schema,
            meerschaum_core::access::LockOp::EditParameters,
        ] {
            if let Some(refused) = meerschaum_connector::lock_refusal(self, p, op).await {
                return refused;
            }
        }
        let mut c = pipe_context(p);
        c["chunk_minutes"] = json!(chunk_minutes);
        self.success("partition_pipe", c, None)
    }
    async fn clear_pipe(&self, p: &Pipe, q: &DataQuery) -> SuccessTuple {
        if let Some(refused) =
            meerschaum_connector::lock_refusal(self, p, meerschaum_core::access::LockOp::Clear)
                .await
        {
            return refused;
        }
        self.success("clear_pipe", query_context(p, q), None)
    }
    async fn deduplicate_pipe(&self, p: &Pipe, q: &DataQuery) -> SuccessTuple {
        if let Some(refused) = meerschaum_connector::lock_refusal(
            self,
            p,
            meerschaum_core::access::LockOp::Deduplicate,
        )
        .await
        {
            return refused;
        }
        self.success("deduplicate_pipe", query_context(p, q), None)
    }
    async fn get_pipe_size(&self, p: &Pipe) -> ConnectorResult<Option<i64>> {
        self.value("get_pipe_size", pipe_context(p))
    }
    async fn get_pipe_index_names(&self, p: &Pipe) -> ConnectorResult<Map> {
        self.value("get_pipe_index_names", pipe_context(p))
    }
    async fn get_pipe_columns_indices(
        &self,
        p: &Pipe,
    ) -> ConnectorResult<IndexMap<String, Vec<meerschaum_connector::ColumnIndexInfo>>> {
        self.value("get_pipe_columns_indices", pipe_context(p))
    }
    async fn acquire_pipe_lease(
        &self,
        p: &Pipe,
        holder: &str,
        ttl: std::time::Duration,
    ) -> ConnectorResult<bool> {
        let mut c = pipe_context(p);
        c["holder"] = json!(holder);
        c["ttl_seconds"] = json!(ttl.as_secs_f64());
        self.value("acquire_pipe_lease", c)
    }
    async fn release_pipe_lease(&self, p: &Pipe, holder: &str) -> ConnectorResult<bool> {
        let mut c = pipe_context(p);
        c["holder"] = json!(holder);
        self.value("release_pipe_lease", c)
    }

    async fn register_pipe(&self, p: &Pipe) -> SuccessTuple {
        self.success("register_pipe", pipe_context(p), None)
    }
    async fn get_pipe_attributes(&self, p: &Pipe) -> ConnectorResult<Map> {
        self.value("get_pipe_attributes", pipe_context(p))
    }
    async fn get_pipe_id(&self, p: &Pipe) -> ConnectorResult<Option<PipeId>> {
        self.value("get_pipe_id", pipe_context(p))
    }
    async fn pipe_exists(&self, p: &Pipe) -> ConnectorResult<bool> {
        self.value("pipe_exists", pipe_context(p))
    }
    async fn drop_pipe(&self, p: &Pipe) -> SuccessTuple {
        if let Some(refused) =
            meerschaum_connector::lock_refusal(self, p, meerschaum_core::access::LockOp::Drop).await
        {
            return refused;
        }
        self.success("drop_pipe", pipe_context(p), None)
    }
    async fn get_pipe_data(&self, p: &Pipe, q: &DataQuery) -> ConnectorResult<Option<Frame>> {
        Ok(self.call("get_pipe_data", query_context(p, q), None)?.data)
    }
    async fn get_pipe_rowcount(&self, p: &Pipe, q: &DataQuery) -> ConnectorResult<Option<i64>> {
        self.value("get_pipe_rowcount", query_context(p, q))
    }
    async fn get_pipe_columns_types(&self, p: &Pipe) -> ConnectorResult<IndexMap<String, String>> {
        self.value("get_pipe_columns_types", pipe_context(p))
    }
    async fn sync_pipe(&self, p: &Pipe, data: Frame, opts: &SyncOptions) -> SuccessTuple {
        if let Some(refused) = meerschaum_connector::lock_refusal(
            self,
            p,
            meerschaum_connector::locks::sync_op(opts.replace),
        )
        .await
        {
            return refused;
        }
        let mut ctx = pipe_context(p);
        ctx["options"] = json!({"check_existing":opts.check_existing,"chunksize":opts.chunksize,"begin":bound(opts.begin.as_ref()),"end":bound(opts.end.as_ref()),"extra":opts.extra});
        self.success("sync_pipe", ctx, Some(&data))
    }
    async fn get_sync_time(
        &self,
        p: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        let mut ctx = pipe_context(p);
        ctx["params"] = json!(params);
        ctx["newest"] = json!(newest);
        let v: Value = self.value("get_sync_time", ctx)?;
        if v.is_null() {
            return Ok(None);
        }
        let parsed = if let Some(i) = v.as_i64() {
            Some(DateBound::Int(i))
        } else {
            v.as_str().and_then(DateBound::parse)
        };
        parsed
            .map(Some)
            .ok_or_else(|| ConnectorError::Invalid("invalid sync time".into()))
    }
    async fn fetch_pipes_keys(&self, q: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        let v: Value=self.value("fetch_pipes_keys",json!({"query":{"connector_keys":q.connector_keys,"metric_keys":q.metric_keys,"location_keys":q.location_keys,"tags":q.tags,"params":q.params}}))?;
        fn entry(v: Value) -> ConnectorResult<PipeKeysEntry> {
            let items: Vec<meerschaum_core::Value> = serde_json::from_value(v)?;
            PipeKeysEntry::from_list(&items)
                .ok_or_else(|| ConnectorError::Invalid("invalid pipe keys".into()))
        }
        match v {
            Value::Array(a) => Ok(PipesKeys::List(
                a.into_iter().map(entry).collect::<ConnectorResult<_>>()?,
            )),
            Value::Object(m) => Ok(PipesKeys::Dict(
                m.into_iter()
                    .map(|(k, v)| Ok((PipeId::parse(&k), entry(v)?)))
                    .collect::<ConnectorResult<_>>()?,
            )),
            _ => Err(ConnectorError::Invalid("invalid pipe keys response".into())),
        }
    }
}
