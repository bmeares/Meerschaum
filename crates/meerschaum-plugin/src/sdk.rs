//! Safe, synchronous author SDK. The host serializes calls into each loaded plugin.
use crate::{Frame, Result, abi::*, error};
use arrow::{
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
    record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader},
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::panic::{AssertUnwindSafe, catch_unwind};

/// JSON envelope. `context` preserves all action/hook kwargs without interpretation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub method: String,
    #[serde(default)]
    pub connector_type: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub context: Value,
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

/// Implement fetch and sync as for a source connector. Instance methods, actions, setup,
/// register, subscribe and hooks use `call`, with the exact method name and JSON context.
/// A new Default value is constructed for each call; shared state may live in a static
/// mutex or in a database keyed by connector label/config. No runtime is imposed.
pub trait Plugin: Default + Send + 'static {
    /// The plugin's name. Leave it out to use the Cargo package name (`export_plugin!` reads
    /// `CARGO_PKG_NAME`), which is also the `plugin.toml` name.
    const NAME: &'static str = "";
    /// The plugin's version. Leave it out to use the Cargo package version.
    const VERSION: &'static str = "";
    /// What the plugin provides: build it with [`crate::Manifest`] (the SDK fills in the
    /// envelope, the name and the version), or return the whole manifest document. The default
    /// declares nothing beyond the name and version.
    fn manifest(&self) -> Value {
        crate::Manifest::new().into()
    }
    /// The executor for `request.connector_type` / `label` / `config` (a manifest
    /// `executor_types` entry), or `None` for a type this plugin does not provide. The SDK
    /// routes every `executor.*` method here (see [`crate::executor`]); they never reach `call`.
    fn executor(&self, _request: &Request) -> Option<Box<dyn crate::executor::Executor>> {
        None
    }
    /// Open a subscription of a streaming connector (see [`crate::subscribe`]). `request.context`
    /// carries `pipe` (as for `fetch`, with host services) and the sync's `kwargs`. The SDK
    /// routes `subscribe`, `next_batch`, `ack_batches` and `close_subscription`; they never
    /// reach `call`.
    fn subscribe(&self, _request: &Request) -> Result<Box<dyn crate::subscribe::Subscription>> {
        Err(crate::Error::Unsupported(
            "subscribe is not implemented".into(),
        ))
    }
    fn fetch(&self, _request: &Request) -> Result<Option<Frame>> {
        Err(crate::Error::Unsupported("fetch is not implemented".into()))
    }
    fn sync(&self, _request: &Request, _data: Option<Frame>) -> Result<Value> {
        Err(crate::Error::Unsupported("sync is not implemented".into()))
    }
    /// Serve one of the routes declared in the manifest's `api_routes` (see [`crate::api`]).
    fn api_route(&self, _request: &crate::api::RouteRequest) -> Result<crate::api::RouteResponse> {
        Err(crate::Error::Unsupported(
            "api_route is not implemented".into(),
        ))
    }
    /// Call one of the manifest's `mcp_tools` (see [`crate::mcp`]); the value is the result.
    fn mcp_tool(&self, _call: &crate::mcp::ToolCall) -> Result<Value> {
        Err(crate::Error::Unsupported(
            "mcp_tool is not implemented".into(),
        ))
    }
    /// Read one of the manifest's `mcp_resources`.
    fn mcp_resource(
        &self,
        _read: &crate::mcp::ResourceRead,
    ) -> Result<crate::mcp::ResourceContents> {
        Err(crate::Error::Unsupported(
            "mcp_resource is not implemented".into(),
        ))
    }
    /// Render one of the manifest's `mcp_prompts` (see [`crate::mcp::prompt_reply`]).
    fn mcp_prompt(&self, _get: &crate::mcp::PromptGet) -> Result<Value> {
        Err(crate::Error::Unsupported(
            "mcp_prompt is not implemented".into(),
        ))
    }
    fn call(&self, request: &Request, data: Option<Frame>) -> Result<Reply> {
        match request.method.as_str() {
            "fetch" => self.fetch(request).map(Reply::data),
            "sync" | "sync_pipe" => self.sync(request, data).map(Reply::json),
            "api_route" | "mcp_tool" | "mcp_resource" | "mcp_prompt" => {
                call_server_extension(self, request)
            }
            _ => Err(crate::Error::Unsupported(format!(
                "{} is not implemented",
                request.method
            ))),
        }
    }
}

/// The default handling of the server call kinds (`api_route`, `mcp_tool`, `mcp_resource`,
/// `mcp_prompt`): decode the context, call the typed trait method, encode the reply. A plugin
/// that overrides [`Plugin::call`] can forward these methods here.
pub fn call_server_extension<P: Plugin>(plugin: &P, request: &Request) -> Result<Reply> {
    fn context<T: serde::de::DeserializeOwned>(request: &Request) -> Result<T> {
        serde_json::from_value(request.context.clone()).map_err(error)
    }
    let value = match request.method.as_str() {
        "api_route" => serde_json::to_value(plugin.api_route(&context(request)?)?),
        "mcp_tool" => return plugin.mcp_tool(&context(request)?).map(Reply::json),
        "mcp_resource" => {
            return Ok(Reply::json(
                plugin.mcp_resource(&context(request)?)?.to_json(),
            ));
        }
        "mcp_prompt" => return plugin.mcp_prompt(&context(request)?).map(Reply::json),
        other => {
            return Err(crate::Error::Unsupported(format!(
                "{other} is not implemented"
            )));
        }
    };
    value.map(Reply::json).map_err(error)
}

pub(crate) fn stream(frame: &Frame) -> Result<FFI_ArrowArrayStream> {
    let batch = frame.to_record_batch().map_err(error)?;
    Ok(FFI_ArrowArrayStream::new(Box::new(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
    )))
}

// Materialize before returning: no imported array may outlive its producer library.
pub(crate) fn frame(stream: FFI_ArrowArrayStream) -> Result<Option<Frame>> {
    if stream.release().is_none() {
        return Ok(None);
    }
    let reader = ArrowArrayStreamReader::try_new(stream).map_err(error)?;
    let schema = reader.schema();
    let mut batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(error)?;
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema));
    }
    Frame::from_record_batches(&batches)
        .map(Some)
        .map_err(error)
}

pub(crate) fn boundary(f: impl FnOnce() -> Result<Value>) -> CallResult {
    let (status, bytes) = match catch_unwind(AssertUnwindSafe(|| {
        f().and_then(|v| serde_json::to_vec(&v).map_err(error))
    })) {
        Ok(Ok(bytes)) => (0, bytes),
        Ok(Err(crate::Error::Unsupported(message))) => (3, message.into_bytes()),
        Ok(Err(e)) => (1, e.to_string().into_bytes()),
        Err(_) => (2, b"native plugin panicked".to_vec()),
    };
    CallResult {
        status,
        payload: OwnedBytes::new(bytes),
    }
}

/// Macro implementation detail; use `export_plugin!`.
pub struct Export<P>(std::marker::PhantomData<P>);
impl<P: Plugin> Export<P> {
    pub const VTABLE: MrsmPluginV1 = MrsmPluginV1 {
        abi_version: ABI_VERSION,
        struct_size: std::mem::size_of::<MrsmPluginV1>() as u32,
        describe: Self::describe,
        invoke: Self::invoke,
        free_bytes,
    };
    unsafe extern "C" fn describe() -> CallResult {
        Self::describe_package("", "")
    }

    /// The vtable `export_plugin!` exports, with its `describe` (which passes the package).
    pub const fn vtable(describe: unsafe extern "C" fn() -> CallResult) -> MrsmPluginV1 {
        MrsmPluginV1 {
            abi_version: ABI_VERSION,
            struct_size: std::mem::size_of::<MrsmPluginV1>() as u32,
            describe,
            invoke: Self::invoke,
            free_bytes,
        }
    }

    /// `describe` for the Cargo package `package` at `version`: the plugin's name and version
    /// are `Plugin::NAME` / `Plugin::VERSION` when set, else the package's, and the manifest is
    /// completed ([`crate::manifest::complete`]).
    pub fn describe_package(package: &str, version: &str) -> CallResult {
        boundary(|| {
            let name = if P::NAME.is_empty() { package } else { P::NAME };
            let version = if P::VERSION.is_empty() {
                version
            } else {
                P::VERSION
            };
            if name.is_empty() {
                return Err(error(
                    "the plugin has no name: set `Plugin::NAME` or export it with `export_plugin!`",
                ));
            }
            Ok(
                json!({"name": name, "version": version, "abi_version": ABI_VERSION,
                "manifest": crate::manifest::complete(P::default().manifest(), name, version)}),
            )
        })
    }
    /// Optional host-call entry point used by `export_plugin!`.
    /// # Safety
    /// The input/output ABI contract is identical to invoke; host must point to a valid
    /// HostV1 table whose callbacks and context live through this synchronous call.
    pub unsafe extern "C" fn invoke_with_host(
        bytes: Bytes,
        input: *mut FFI_ArrowArrayStream,
        output: *mut FFI_ArrowArrayStream,
        host: *const HostV1,
    ) -> CallResult {
        // SAFETY: the extension caller supplies a readable table and live callback context.
        match unsafe { crate::callbacks::enter(host) } {
            Ok(_scope) => unsafe { Self::invoke(bytes, input, output) },
            Err(e) => boundary(|| Err(e)),
        }
    }

    unsafe extern "C" fn invoke(
        bytes: Bytes,
        input: *mut FFI_ArrowArrayStream,
        output: *mut FFI_ArrowArrayStream,
    ) -> CallResult {
        boundary(|| {
            if input.is_null() || output.is_null() || (bytes.len != 0 && bytes.ptr.is_null()) {
                return Err(error("null ABI argument"));
            }
            // SAFETY: ABI caller provides readable bytes and valid, exclusive stream pointers.
            let request: Request =
                serde_json::from_slice(unsafe { bytes.as_slice() }).map_err(error)?;
            // SAFETY: input is moved once and replaced with an empty stream.
            let data = frame(unsafe { FFI_ArrowArrayStream::from_raw(input) })?;
            // Executors and subscriptions are routed by the SDK, not by `call`.
            let reply = match crate::subscribe::dispatch::<P>(&request)
                .or_else(|| crate::executor::dispatch::<P>(&request))
            {
                Some(reply) => reply?,
                None => P::default().call(&request, data)?,
            };
            if let Some(data) = reply.data {
                // SAFETY: caller supplies a valid empty output stream.
                unsafe {
                    *output = stream(&data)?;
                }
            }
            Ok(reply.value)
        })
    }
}

pub(crate) unsafe extern "C" fn free_bytes(bytes: OwnedBytes) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !bytes.ptr.is_null() {
            // SAFETY: bytes was allocated by OwnedBytes::new in this library and is freed once.
            unsafe {
                drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                    bytes.ptr, bytes.len,
                )));
            }
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Default)]
    struct Panics;
    impl Plugin for Panics {
        const NAME: &'static str = "panics";
        const VERSION: &'static str = "1";
        fn manifest(&self) -> Value {
            panic!("manifest panic")
        }
    }

    #[test]
    fn bounds_are_found_at_the_top_level_in_query_and_in_options() {
        let request = |context: Value| Request {
            method: "m".into(),
            connector_type: String::new(),
            label: String::new(),
            config: Value::Null,
            context,
        };
        let int = Some(crate::Bound::Int(2));
        let dt = Some(crate::Bound::Datetime("2026-01-01T00:00:00Z".into()));
        for context in [
            json!({"begin": 2, "end": "2026-01-01T00:00:00Z"}),
            json!({"query": {"begin": 2, "end": "2026-01-01T00:00:00Z"}}),
            json!({"options": {"begin": 2, "end": "2026-01-01T00:00:00Z"}}),
        ] {
            let r = request(context);
            assert_eq!(r.bound("begin").unwrap(), int);
            assert_eq!(r.bound("end").unwrap(), dt);
        }
        let r = request(json!({"options": {"begin": null, "end": ""}}));
        assert_eq!(r.bound("begin").unwrap(), None);
        assert_eq!(r.bound("end").unwrap(), None);
    }

    #[test]
    fn manifest_panic_is_error_status() {
        // SAFETY: SDK-owned static vtable, freed by its matching allocator.
        unsafe {
            let table = &Export::<Panics>::VTABLE;
            let result = (table.describe)();
            assert_eq!(result.status, 2);
            (table.free_bytes)(result.payload);
        }
    }

    #[test]
    fn malformed_json_and_null_streams_are_errors() {
        // SAFETY: borrowed test bytes and valid empty streams; null pointers are explicitly
        // supported as rejected arguments by the SDK trampoline.
        unsafe {
            let table = &Export::<Panics>::VTABLE;
            let mut input = FFI_ArrowArrayStream::empty();
            let mut output = FFI_ArrowArrayStream::empty();
            let malformed = (table.invoke)(Bytes::borrowed(b"{"), &mut input, &mut output);
            assert_eq!(malformed.status, 1);
            (table.free_bytes)(malformed.payload);
            let null = (table.invoke)(Bytes::borrowed(b"{}"), std::ptr::null_mut(), &mut output);
            assert_eq!(null.status, 1);
            (table.free_bytes)(null.payload);
            assert!(output.release().is_none());
        }
    }

    #[test]
    fn empty_stream_is_distinct_from_missing_data() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        assert!(frame(FFI_ArrowArrayStream::empty()).unwrap().is_none());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchIterator::new(
            Vec::<std::result::Result<RecordBatch, arrow::error::ArrowError>>::new(),
            schema,
        )));
        let empty = frame(stream).unwrap().unwrap();
        assert_eq!(empty.height(), 0);
        assert_eq!(
            empty.to_record_batch().unwrap().schema().field(0).name(),
            "id"
        );
    }
}

/// Types made available to single-file fetch plugins. Explicit user imports may override them.
pub mod fetch_prelude {
    pub use crate::{Bound, Error, Frame, FrameExt, Pipe, Query, Result, SuccessTuple, host};
    pub use serde_json::{Value, json};
}

/// Return types accepted by a single-file plugin's `fetch(&Request)` function.
/// Rows are converted to an Arrow-backed frame before crossing the native boundary.
pub trait FetchOutput {
    fn into_frame(self) -> Result<Option<Frame>>;
}

/// JSON rows as documents (`list[dict]`), the way a Python plugin's `fetch()` returns them.
pub type Docs = Vec<IndexMap<String, meerschaum_kernel::Value>>;

/// Convert JSON object rows into documents. A key a row does not have is absent from its
/// document (`pd.DataFrame(docs)` fills it with `NaN`).
pub fn rows_to_docs(rows: Vec<Value>) -> Result<Docs> {
    rows.into_iter()
        .map(|row| match row {
            Value::Object(object) => object
                .into_iter()
                .map(|(key, value)| {
                    let value: meerschaum_core::Value =
                        serde_json::from_value(value).map_err(error)?;
                    Ok((key, meerschaum_kernel::Value::from(value)))
                })
                .collect(),
            _ => Err(error("fetch rows must be JSON objects")),
        })
        .collect()
}

/// Documents as a frame with the dtypes a Python plugin's documents get:
/// `pd.DataFrame(docs).convert_dtypes()` ([`meerschaum_kernel::docs::frame_from_docs`]), so
/// integer, float, bool and string columns are typed rather than `object`.
pub fn docs_to_frame(docs: &[IndexMap<String, meerschaum_kernel::Value>]) -> Result<Frame> {
    meerschaum_kernel::docs::frame_from_docs(docs).map_err(error)
}

impl FetchOutput for Vec<Value> {
    fn into_frame(self) -> Result<Option<Frame>> {
        if self.is_empty() {
            return Ok(None);
        }
        docs_to_frame(&rows_to_docs(self)?).map(Some)
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

/// Accepted results of a single-file `sync` function.
pub trait SyncOutput {
    fn into_success(self) -> Result<Value>;
}
impl SyncOutput for crate::SuccessTuple {
    fn into_success(self) -> Result<Value> {
        serde_json::to_value(self).map_err(error)
    }
}
impl<S: Into<String>> SyncOutput for (bool, S) {
    fn into_success(self) -> Result<Value> {
        crate::SuccessTuple {
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

impl Request {
    /// A typed Pipe view with host operations available during this invocation.
    pub fn pipe(&self) -> Result<crate::Pipe<'_>> {
        crate::Pipe::from_request(self)
    }
    /// A `begin` / `end` bound, wherever the host put it: top-level (a plugin's `sync`),
    /// `context.query` (`fetch`, `get_pipe_data`, `clear_pipe`, ...) or `context.options`
    /// (`sync_pipe`). See [`Request::field`].
    pub fn bound(&self, name: &str) -> Result<Option<crate::Bound>> {
        match self.field(name) {
            Value::Null => Ok(None),
            Value::String(s) if s.is_empty() => Ok(None),
            value => serde_json::from_value(value.clone())
                .map(Some)
                .map_err(error),
        }
    }
    /// A field of a call's context: `context[name]`, else `context.query[name]`, else
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
}
