//! Shared integration-test helpers for `crates/meerschaum-cli/tests/`.
#![allow(dead_code)]

pub mod executor;
pub mod fs;
pub mod http;
pub mod process;
pub mod webterm;

use std::path::Path;
use std::time::{Duration, Instant};

use meerschaum_core::{Map, Value};
use serde_json::Value as Json;

/// Convert a `serde_json::Value` object into a `meerschaum_core::Map`.
pub fn json_map(value: Json) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}

/// Poll until `path` exists on disk or `within` elapses.
pub fn appears(path: &Path, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    path.exists()
}
