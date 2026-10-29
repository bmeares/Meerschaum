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

/// The stack defaults v5 changed on purpose, mapped back to v4.1.4's values in serialized text
/// (PARITY.md W17K1-002..004): the `db` service mounts the setup FILE into the image's directory
/// instead of the directory over it, its healthcheck asks the TCP socket and has a `start_period`,
/// and Grafana's datasource names PostgreSQL 18; the `api` service runs this release's `full` image
/// (`bmeares/meerschaum:<version>-full`, PARITY.md W18PK-001) instead of `bmeares/meerschaum:api`.
/// Works on compact JSON (`Value::to_json_string`,
/// `serde_json::to_string`, including the tagged `{"t": ..., "v": ...}` form of the YAML tests)
/// and on YAML text, also when the YAML is itself a JSON string (escaped newlines).
pub fn v4_stack_text(text: &str) -> String {
    let mut text = text
        .replace(
            &format!(
                "bmeares/meerschaum:{}-full",
                meerschaum_core::python_guard::expected_version()
            ),
            "bmeares/meerschaum:api",
        )
        .replace(
            "/db/initdb/create-extensions.sql:/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro",
            "/db/initdb:/docker-entrypoint-initdb.d:z,ro",
        )
        .replace("pg_isready -h 127.0.0.1 -p 5432 -d ", "pg_isready -d ")
        // JSON (plain and tagged) and YAML spellings of the `start_period` entry.
        .replace(",\"start_period\":\"300s\"", "")
        .replace(
            ",[{\"t\":\"str\",\"v\":\"start_period\"},{\"t\":\"str\",\"v\":\"300s\"}]",
            "",
        )
        // `postgresVersion: 1800` (JSON, tagged JSON, YAML).
        .replace("\"postgresVersion\":1800", "\"postgresVersion\":1500")
        .replace(
            "{\"t\":\"str\",\"v\":\"postgresVersion\"},{\"t\":\"int\",\"v\":\"1800\"}",
            "{\"t\":\"str\",\"v\":\"postgresVersion\"},{\"t\":\"int\",\"v\":\"1500\"}",
        )
        .replace("postgresVersion: 1800", "postgresVersion: 1500");
    // A YAML `start_period: 300s` line: drop it with its leading newline (raw or JSON-escaped) and
    // indentation.
    let needle = "start_period: 300s";
    while let Some(at) = text.find(needle) {
        let head = text[..at].trim_end_matches(' ');
        let start = if head.ends_with("\\n") {
            head.len() - 2
        } else if head.ends_with('\n') {
            head.len() - 1
        } else {
            break;
        };
        text.replace_range(start..at + needle.len(), "");
    }
    text
}

/// [`v4_stack_text`] over a [`Value`].
pub fn v4_stack_value(value: &Value) -> Value {
    Value::from_json_str(&v4_stack_text(&value.to_json_string())).expect("valid JSON")
}
