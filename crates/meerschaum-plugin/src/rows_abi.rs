//! Optional row-oriented extension for lightweight plugins. All allocations stay with
//! their producer. Bytes contain JSON; callbacks and context are valid only during invoke.
//!
//! `InvokeRows` borrows `{ "request": Request, "data": null | [objects...] }` and returns
//! `{ "value": JSON, "data": null | [objects...] }`. Status codes and byte ownership are
//! identical to ABI v1; the plugin vtable's `free_bytes` releases the result. The original
//! v1 table remains mandatory for metadata and compatibility validation.
//!
//! The optional host table has the same permanent two-u32 prefix validation rule as v1.
//! Its callback borrows `{ "method": "get_sync_time" | "sync", "data": null | [objects...] }`
//! and returns the operation's JSON result (bound or success tuple), freed by the host.
//! A null table means host operations are unavailable. No callback, context pointer or
//! borrowed byte slice may escape this synchronous call or be used on another thread.
//! JSON rows intentionally carry no Arrow schema; typed data plugins use the original ABI.
use crate::abi::{Bytes, CallResult, OwnedBytes};

pub const ROWS_ENTRY_POINT: &[u8] = b"mrsm_plugin_invoke_rows_v1\0";
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RowsHostV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub context: *mut std::ffi::c_void,
    pub invoke: unsafe extern "C" fn(*mut std::ffi::c_void, Bytes) -> CallResult,
    pub free_bytes: unsafe extern "C" fn(OwnedBytes),
}
pub type InvokeRows = unsafe extern "C" fn(Bytes, *const RowsHostV1) -> CallResult;
