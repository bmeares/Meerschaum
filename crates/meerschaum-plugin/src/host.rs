//! Trusted-library loading and safe connector adapters.
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
        Ok(Arc::new(Self {
            _library: library,
            table,
            invoke_host,
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
        ACTIVE.with(|active| active.borrow_mut().push(self.name.clone()));
        let _active = ActiveCall;
        let bytes = serde_json::to_vec(request).map_err(error)?;
        let mut input = data
            .map(sdk::stream)
            .transpose()?
            .unwrap_or_else(FFI_ArrowArrayStream::empty);
        let mut output = FFI_ArrowArrayStream::empty();
        // SAFETY: table is validated and pinned; borrowed bytes and exclusive streams live
        // through the call. Remaining streams are released on success and all error paths.
        let result = if let (Some(invoke), Some(services)) = (self.invoke_host, host) {
            let mut context = crate::callbacks::HostContext { services, request };
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
        Ok(Reply {
            value,
            data: sdk::frame(output)?,
        })
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
    /// A JSON-only call of an extension method (`routes`, `route`, `pages`, `page`, ...):
    /// `context` goes in, the reply's JSON value comes out.
    pub fn call_json(&self, method: &str, context: Value) -> Result<Value> {
        self.invoke(
            &Request {
                method: method.into(),
                connector_type: String::new(),
                label: String::new(),
                config: Value::Null,
                context,
            },
            None,
        )
        .map(|reply| reply.value)
    }
    pub fn action(&self, name: &str, kwargs: Value) -> Result<SuccessTuple> {
        let reply = self.invoke(
            &Request {
                method: "action".into(),
                connector_type: String::new(),
                label: String::new(),
                config: Value::Null,
                context: json!({"name":name,"kwargs":kwargs}),
            },
            None,
        )?;
        serde_json::from_value(reply.value).map_err(error)
    }
    pub fn hook(&self, post: bool, name: &str, kwargs: Value) -> Result<SuccessTuple> {
        let reply = self.invoke(
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
                context: json!({"name":name,"kwargs":kwargs}),
            },
            None,
        )?;
        serde_json::from_value(reply.value).map_err(error)
    }
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
        self.plugin
            .invoke_with_host(
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
        let mut context = query_context(pipe, q);
        if self.fetch_kwargs.as_object().is_some_and(|o| !o.is_empty()) {
            context["kwargs"] = self.fetch_kwargs.clone();
        }
        let connector = self.clone();
        tokio::task::spawn_blocking(move || connector.call("fetch", context, None).map(|r| r.data))
            .await
            .map_err(|e| ConnectorError::Backend(e.to_string()))?
    }
}
#[async_trait]
impl InstanceConnector for NativeConnector {
    async fn delete_pipe(&self, p: &Pipe) -> SuccessTuple {
        self.success("delete_pipe", pipe_context(p), None)
    }
    async fn drop_pipe_indices(&self, p: &Pipe) -> SuccessTuple {
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
        let mut c = pipe_context(p);
        c["chunk_minutes"] = json!(chunk_minutes);
        self.success("partition_pipe", c, None)
    }
    async fn clear_pipe(&self, p: &Pipe, q: &DataQuery) -> SuccessTuple {
        self.success("clear_pipe", query_context(p, q), None)
    }
    async fn deduplicate_pipe(&self, p: &Pipe, q: &DataQuery) -> SuccessTuple {
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
