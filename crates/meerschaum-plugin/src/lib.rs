//! The native plugin SDK of [Meerschaum](https://meerschaum.io). The C ABI (`mrsm_plugin_v1`) is versioned
//! and stable independently of the Rust types around it.
//!
//! Versioned native plugin ABI, safe author SDK, discovery and connector adapters.
//! See the crate README for ownership, threading and wire contracts.
#![deny(unsafe_op_in_unsafe_fn)]
pub mod abi;
pub mod api;
pub mod callbacks;
pub mod discovery;
pub mod executor;
mod handles;
pub mod host;
pub mod manifest;
pub mod mcp;
pub mod registry;
pub mod rows;
pub mod rows_abi;
pub mod sdk;
mod single_file;
pub mod source;
pub mod subscribe;
pub use arrow;
pub use callbacks::{Bound, HostCall, HostReply, HostServices, Pipe, Query};
pub use chrono;
pub use host::{LoadedPlugin, NativeConnector};
pub use indexmap;
pub use manifest::{ConnectorType, Manifest};
pub use meerschaum_core::SuccessTuple;
pub use meerschaum_kernel::Frame;
pub use registry::NativePlugins;
pub use rows::FrameExt;
pub use sdk::{Plugin, Reply, Request};
pub use serde_json;

/// The names most plugins use: `use meerschaum_plugin::prelude::*;`.
pub mod prelude {
    pub use crate::{
        Bound, ConnectorType, Error, Frame, FrameExt, Manifest, Pipe, Plugin, Query, Reply,
        Request, Result, SuccessTuple, export_plugin, host,
    };
    pub use serde_json::{Value, json};
}

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
///
/// The plugin's name and version default to the Cargo package's (`CARGO_PKG_NAME`,
/// `CARGO_PKG_VERSION` of the crate that invokes the macro); `Plugin::NAME` / `Plugin::VERSION`
/// override them. The manifest is completed with this SDK's Meerschaum version.
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
            unsafe extern "C" fn describe() -> $crate::abi::CallResult {
                $crate::sdk::Export::<$plugin>::describe_package(
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_PKG_VERSION"),
                )
            }
            static VTABLE: $crate::abi::MrsmPluginV1 =
                $crate::sdk::Export::<$plugin>::vtable(describe);
            &VTABLE
        }
    };
}
