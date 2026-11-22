// These tests drive the native plugin C ABI, which is `unsafe` by nature.
#![allow(unsafe_code)]

#[path = "../support/compile_single_file.rs"]
pub mod compile_single_file;

mod support {
    pub use crate::compile_single_file;
}

mod api_mcp;
mod executor_subscribe;
mod host_calls;
mod native;
mod single_file;
mod single_file_bounds;
mod single_file_empty_rows;
mod single_file_parity;
mod single_file_server;
#[cfg(unix)]
mod source;
