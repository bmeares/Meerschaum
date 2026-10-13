//! Versioned native plugin ABI, safe author SDK, discovery and connector adapters.
//! See the crate README for ownership, threading and wire contracts.
#![deny(unsafe_op_in_unsafe_fn)]
pub mod abi;
pub mod discovery;
pub mod host;
pub mod sdk;
pub use arrow;
pub use host::{LoadedPlugin, NativeConnector};
pub use meerschaum_kernel::Frame;
pub use sdk::{Plugin, Reply, Request};
pub use serde_json;

/// Worker manifest format version (independent of the native ABI version).
pub const MANIFEST_SCHEMA_VERSION: u32 = 2;

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
        pub extern "C" fn mrsm_plugin_v1() -> *const $crate::abi::MrsmPluginV1 {
            &$crate::sdk::Export::<$plugin>::VTABLE
        }
    };
}
