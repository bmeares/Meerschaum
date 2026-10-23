//! Safe, synchronous author SDK. The host serializes calls into each loaded plugin.
use crate::{Frame, Result, abi::*, error};
use arrow::{
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
    record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader},
};
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
    const NAME: &'static str;
    const VERSION: &'static str;
    /// Full worker manifest, including `plugins` and `connector_types`.
    fn manifest(&self) -> Value;
    fn fetch(&self, _request: &Request) -> Result<Option<Frame>> {
        Err(crate::Error::Unsupported("fetch is not implemented".into()))
    }
    fn sync(&self, _request: &Request, _data: Option<Frame>) -> Result<Value> {
        Err(crate::Error::Unsupported("sync is not implemented".into()))
    }
    fn call(&self, request: &Request, data: Option<Frame>) -> Result<Reply> {
        match request.method.as_str() {
            "fetch" => self.fetch(request).map(Reply::data),
            "sync" | "sync_pipe" => self.sync(request, data).map(Reply::json),
            _ => Err(crate::Error::Unsupported(format!(
                "{} is not implemented",
                request.method
            ))),
        }
    }
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
        boundary(|| {
            Ok(json!({"name": P::NAME, "version": P::VERSION,
            "abi_version": ABI_VERSION, "manifest": P::default().manifest()}))
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
            let reply = P::default().call(&request, data)?;
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
    pub use crate::{Bound, Error, Frame, Pipe, Result, SuccessTuple};
    pub use serde_json::{Value, json};
}

/// Return types accepted by a single-file plugin's `fetch(&Request)` function.
/// Rows are converted to an Arrow-backed frame before crossing the native boundary.
pub trait FetchOutput {
    fn into_frame(self) -> Result<Option<Frame>>;
}

impl FetchOutput for Vec<Value> {
    fn into_frame(self) -> Result<Option<Frame>> {
        if self.is_empty() {
            return Ok(None);
        }
        let mut names = indexmap::IndexSet::new();
        for row in &self {
            let row = row
                .as_object()
                .ok_or_else(|| error("fetch rows must be JSON objects"))?;
            names.extend(row.keys().cloned());
        }
        let columns = names
            .into_iter()
            .map(|name| {
                let values = self
                    .iter()
                    .map(|row| {
                        let value: meerschaum_core::Value =
                            serde_json::from_value(row[&name].clone()).map_err(error)?;
                        Ok(meerschaum_kernel::Value::from(value))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((name, values))
            })
            .collect::<Result<Vec<_>>>()?;
        Frame::from_object_columns(columns).map(Some).map_err(error)
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
    /// Bounds are nested under query for fetch and top-level for sync.
    pub fn bound(&self, name: &str) -> Result<Option<crate::Bound>> {
        let value = self.context.get("query").unwrap_or(&self.context).get(name);
        match value {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) if s.is_empty() => Ok(None),
            Some(value) => serde_json::from_value(value.clone())
                .map(Some)
                .map_err(error),
        }
    }
}
