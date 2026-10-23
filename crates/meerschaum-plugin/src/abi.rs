//! ABI v1. No Rust layouts, allocators, traits or futures cross this boundary.
use arrow::ffi_stream::FFI_ArrowArrayStream;

pub const ABI_VERSION: u32 = 1;
pub const ENTRY_POINT: &[u8] = b"mrsm_plugin_v1\0";

/// Borrowed UTF-8 bytes. The caller retains ownership through the call.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Bytes {
    pub ptr: *const u8,
    pub len: usize,
}
impl Bytes {
    pub fn borrowed(bytes: &[u8]) -> Self {
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        }
    }
    /// # Safety
    /// Nonempty bytes must point to readable memory valid for the returned borrow.
    pub unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        if self.len == 0 {
            &[]
        } else {
            // SAFETY: required by the caller contract.
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

/// Plugin-allocated boxed bytes; release exactly once using the exporting vtable.
#[repr(C)]
pub struct OwnedBytes {
    pub ptr: *mut u8,
    pub len: usize,
}
impl OwnedBytes {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        let bytes = bytes.into_boxed_slice();
        let len = bytes.len();
        Self {
            ptr: Box::into_raw(bytes).cast(),
            len,
        }
    }
}

/// Status 0 = JSON result, 1 = error, 2 = panic, 3 = unsupported method. Errors are UTF-8.
#[repr(C)]
pub struct CallResult {
    pub status: i32,
    pub payload: OwnedBytes,
}

/// Stable prefix: read `abi_version` and `struct_size` before accessing callbacks.
/// `describe` returns metadata and a worker-shaped manifest as JSON.
/// `invoke` accepts Request JSON and optional Arrow input/output streams. Input is moved
/// by replacing it with an empty stream. Output must initially be empty. Both pointers
/// are non-null. Even on error the caller releases any remaining streams and payload.
#[repr(C)]
pub struct MrsmPluginV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub describe: unsafe extern "C" fn() -> CallResult,
    pub invoke: unsafe extern "C" fn(
        Bytes,
        *mut FFI_ArrowArrayStream,
        *mut FFI_ArrowArrayStream,
    ) -> CallResult,
    pub free_bytes: unsafe extern "C" fn(OwnedBytes),
}

/// Optional, synchronous host-call extension. The original v1 plugin table is unchanged.
/// `context` and this table live only through InvokeWithHost. Callbacks may not be retained
/// or invoked on background threads. The host owns result bytes and frees them itself.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HostV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub context: *mut std::ffi::c_void,
    pub invoke:
        unsafe extern "C" fn(*mut std::ffi::c_void, Bytes, *mut FFI_ArrowArrayStream) -> CallResult,
    pub free_bytes: unsafe extern "C" fn(OwnedBytes),
}

pub const HOST_ENTRY_POINT: &[u8] = b"mrsm_plugin_invoke_host_v1\0";
pub type InvokeWithHost = unsafe extern "C" fn(
    Bytes,
    *mut FFI_ArrowArrayStream,
    *mut FFI_ArrowArrayStream,
    *const HostV1,
) -> CallResult;
