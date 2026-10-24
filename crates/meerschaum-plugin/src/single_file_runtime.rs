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
#[derive(Debug, Clone)]
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
    kwargs: Value,
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
    pub fn bound(&self, name: &str) -> Result<Option<Bound>> {
        bound(&self.context.get("query").unwrap_or(&self.context)[name])
    }
    pub fn pipe(&self) -> Result<Pipe<'_>> {
        Pipe::from_request(self)
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
        let metadata = request
            .context
            .get("pipe")
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
        pub use super::super::{Bound, Connector, Error, Frame, Pipe, Result, SuccessTuple};
        pub use serde_json::{Value, json};
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
pub struct Export<P>(PhantomData<P>);
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
