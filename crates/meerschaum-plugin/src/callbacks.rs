//! Synchronous host services for the native plugin SDK. Only C layouts and Arrow streams
//! cross the boundary; host and plugin allocations are always freed by their owner.
use crate::{Frame, Request, Result, SuccessTuple, abi::*, error, sdk};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{cell::Cell, marker::PhantomData, rc::Rc};

/// A bound on an integer or datetime axis. Datetimes use the host's ISO string format.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Bound {
    Int(i64),
    Datetime(String),
}

/// Host implementation, bound to the request's Pipe. No Rust trait crosses the ABI.
pub trait HostServices: Send + Sync {
    fn call(&self, request: &Request, method: &str, data: Option<Frame>) -> Result<Value>;
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

/// Pipe metadata and synchronous instance operations. This view is tied to its request
/// and calling thread; it cannot carry a host callback into a background task.
pub struct Pipe<'a> {
    metadata: &'a Value,
    host_generation: Option<u64>,
    _thread: PhantomData<Rc<()>>,
}
impl<'a> Pipe<'a> {
    pub fn from_request(request: &'a Request) -> Result<Self> {
        let metadata = request
            .context
            .get("pipe")
            .filter(|v| v.is_object())
            .ok_or_else(|| error("request has no Pipe metadata"))?;
        Ok(Self {
            metadata,
            host_generation: HOST.with(|slot| slot.get().map(|h| h.generation)),
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

    /// Read the newest value of this Pipe's datetime / integer axis from its instance.
    pub fn get_sync_time(&self) -> Result<Option<Bound>> {
        serde_json::from_value(self.call("get_sync_time", None)?).map_err(error)
    }
    /// Write supplied rows through the host's normal sync engine, without calling the
    /// plugin's fetch/sync again. Empty rows succeed without writing anything.
    pub fn sync(&self, data: impl sdk::FetchOutput) -> Result<SuccessTuple> {
        let data = data.into_frame()?;
        serde_json::from_value(self.call("sync", data.as_ref())?).map_err(error)
    }
    fn call(&self, method: &str, data: Option<&Frame>) -> Result<Value> {
        let host = HOST
            .with(|slot| slot.get())
            .filter(|h| Some(h.generation) == self.host_generation)
            .ok_or_else(|| {
                error("Pipe host operations are unavailable outside a host-enabled plugin call")
            })?;
        let host = host.table;
        let mut input = data
            .map(sdk::stream)
            .transpose()?
            .unwrap_or_else(FFI_ArrowArrayStream::empty);
        // SAFETY: the scope pins callbacks and context; method bytes and stream live through call.
        let result =
            unsafe { (host.invoke)(host.context, Bytes::borrowed(method.as_bytes()), &mut input) };
        // SAFETY: result bytes are owned by this host and released with its callback once.
        unsafe { crate::host::decode_with(host.free_bytes, result) }
    }
}

pub(crate) struct HostContext<'a> {
    pub services: &'a dyn HostServices,
    pub request: &'a Request,
}
pub(crate) unsafe extern "C" fn invoke_host(
    context: *mut std::ffi::c_void,
    method: Bytes,
    input: *mut FFI_ArrowArrayStream,
) -> CallResult {
    sdk::boundary(|| {
        if context.is_null() || input.is_null() || (method.len != 0 && method.ptr.is_null()) {
            return Err(error("null host call argument"));
        }
        // SAFETY: HostContext and input are pinned by invoke_with_host for this call.
        let context = unsafe { &*context.cast::<HostContext<'_>>() };
        let method = std::str::from_utf8(unsafe { method.as_slice() }).map_err(error)?;
        let data = sdk::frame(unsafe { FFI_ArrowArrayStream::from_raw(input) })?;
        context.services.call(context.request, method, data)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validates_the_host_prefix_before_reading_callbacks() {
        assert!(unsafe { enter(std::ptr::null()) }.is_err());
        for prefix in [[99_u32, 8], [1, 8]] {
            // A short prefix is all that may be inspected when its version/size is wrong.
            assert!(unsafe { enter(prefix.as_ptr().cast()) }.is_err());
        }
    }

    #[test]
    fn pipe_views_cannot_reuse_callback_state_after_the_invocation() {
        struct Host;
        impl HostServices for Host {
            fn call(&self, _: &Request, _: &str, _: Option<Frame>) -> Result<Value> {
                Ok(json!(7))
            }
        }
        let request = Request {
            method: "fetch".into(),
            connector_type: "plugin".into(),
            label: "test".into(),
            config: Value::Null,
            context: json!({"pipe": {"keys": {"metric_key": "test"}}}),
        };
        let mut context = HostContext {
            services: &Host,
            request: &request,
        };
        let table = HostV1 {
            abi_version: 1,
            struct_size: std::mem::size_of::<HostV1>() as u32,
            context: (&mut context as *mut HostContext<'_>).cast(),
            invoke: invoke_host,
            free_bytes: sdk::free_bytes,
        };
        let scope = unsafe { enter(&table) }.unwrap();
        let pipe = request.pipe().unwrap();
        assert_eq!(pipe.get_sync_time().unwrap(), Some(Bound::Int(7)));
        drop(scope);
        assert!(pipe.get_sync_time().is_err());
        let _next = unsafe { enter(&table) }.unwrap();
        // Even if the host context has exactly the same address, a prior view is expired.
        assert!(pipe.get_sync_time().is_err());
        assert_eq!(
            request.pipe().unwrap().get_sync_time().unwrap(),
            Some(Bound::Int(7))
        );
    }
}
