//! Shared helpers for the parity tests.
#![allow(dead_code)]

use std::path::PathBuf;

use meerschaum_core::Value;

/// The fixtures directory.
pub fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

/// Load a JSON fixture, preserving key order.
pub fn load_fixture(name: &str) -> Value {
    let text = std::fs::read_to_string(fixtures().join(name))
        .unwrap_or_else(|e| panic!("missing fixture {name}: {e}"));
    Value::from_json_str(&text).unwrap_or_else(|e| panic!("invalid fixture {name}: {e}"))
}

/// Order-sensitive equality: compare the serialized JSON (maps keep insertion order).
pub fn assert_same(actual: &Value, expected: &Value, context: &str) {
    let a = actual.to_json_string();
    let e = expected.to_json_string();
    assert_eq!(a, e, "mismatch for {context}");
}

/// The repository root (for running the Python side).
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("repo root")
}
