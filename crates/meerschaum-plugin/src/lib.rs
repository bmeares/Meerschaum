//! The native plugin SDK of [Meerschaum](https://meerschaum.io). The C ABI (`mrsm_plugin_v1`) is versioned
//! and stable independently of the Rust types around it.
//!
//! Versioned native plugin ABI, safe author SDK, discovery and connector adapters.
//! See the crate README for ownership, threading and wire contracts.
#![deny(unsafe_op_in_unsafe_fn)]
pub mod abi;
pub mod callbacks;
pub mod discovery;
pub mod host;
pub mod registry;
pub mod sdk;
pub mod source;
pub use arrow;
pub use callbacks::{Bound, HostServices, Pipe};
pub use host::{LoadedPlugin, NativeConnector};
pub use meerschaum_core::SuccessTuple;
pub use meerschaum_kernel::Frame;
pub use registry::NativePlugins;
pub use sdk::{Plugin, Reply, Request};
pub use serde_json;

/// Shared worker/native manifest format version (independent of the native ABI version).
/// Keep the native schema copy and the worker schema in sync; cache fingerprints include it.
pub const MANIFEST_SCHEMA_VERSION: u32 = 5;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Message(String),
    #[error("{0}")]
    Unsupported(String),
}
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn error(e: impl std::fmt::Display) -> Error {
    Error::Message(e.to_string())
}

/// Export one plugin vtable per library. Constructors and calls are panic-contained.
#[macro_export]
macro_rules! export_plugin {
    ($plugin:ty) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn mrsm_plugin_invoke_host_v1(
            bytes: $crate::abi::Bytes,
            input: *mut $crate::arrow::ffi_stream::FFI_ArrowArrayStream,
            output: *mut $crate::arrow::ffi_stream::FFI_ArrowArrayStream,
            host: *const $crate::abi::HostV1,
        ) -> $crate::abi::CallResult {
            // SAFETY: forwarded synchronous extension ABI contract.
            unsafe { $crate::sdk::Export::<$plugin>::invoke_with_host(bytes, input, output, host) }
        }
        #[unsafe(no_mangle)]
        pub extern "C" fn mrsm_plugin_v1() -> *const $crate::abi::MrsmPluginV1 {
            &$crate::sdk::Export::<$plugin>::VTABLE
        }
    };
}
