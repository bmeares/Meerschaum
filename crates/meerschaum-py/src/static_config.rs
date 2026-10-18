//! `_core.static_config()`: the shared static configuration (D15).
//!
//! `meerschaum_core::static_config` owns `crates/meerschaum-core/static.toml` (embedded at compile
//! time) and resolves its two marker tables exactly as Python would:
//!
//! - `{ __null__ = true }` becomes `None`;
//! - `{ __cpu_count_or__ = N }` becomes `os.cpu_count() or N`. This module passes Python's own
//!   `os.cpu_count()`, so the value matches `STATIC_CONFIG` on the same machine.
//!
//! This module only converts the parsed tree into Python objects. Sets and tuples in
//! `STATIC_CONFIG` are lists in the file (sets sorted) and come back as lists.

use meerschaum_core::Value;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

pub use meerschaum_core::static_config::{CPU_COUNT_MARKER, NULL_MARKER};

/// The raw contents of `static.toml` (always embedded by `meerschaum-core`).
pub const STATIC_TOML: Option<&str> = Some(meerschaum_core::static_config::STATIC_TOML);

/// Parse the embedded `static.toml`, resolving the markers with the given CPU count
/// (`None` mimics `os.cpu_count()` returning `None`).
pub fn parse_static_toml(cpu_count: Option<i64>) -> Result<Option<Value>, toml::de::Error> {
    STATIC_TOML
        .map(|text| meerschaum_core::static_config::parse_static_toml(text, cpu_count))
        .transpose()
}

/// Convert a [`Value`] into the equivalent Python object (maps become `dict`, lists `list`).
pub fn value_to_py<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        Value::Null => py.None().into_bound(py),
        Value::Bool(b) => b.into_pyobject(py)?.to_owned().into_any(),
        Value::Int(i) => i.into_pyobject(py)?.into_any(),
        Value::Float(f) => f.into_pyobject(py)?.into_any(),
        Value::Str(s) => s.into_pyobject(py)?.into_any(),
        Value::List(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(value_to_py(py, item)?)?;
            }
            list.into_any()
        }
        Value::Map(map) => {
            let dict = PyDict::new(py);
            for (key, item) in map {
                dict.set_item(key, value_to_py(py, item)?)?;
            }
            dict.into_any()
        }
    })
}

/// `_core.static_config() -> dict | None`. Returns a fresh dict on every call, so callers may
/// mutate it without affecting later calls.
#[pyfunction(name = "static_config")]
pub fn py_static_config(py: Python<'_>) -> PyResult<Option<Bound<'_, PyDict>>> {
    let cpu_count: Option<i64> = py.import("os")?.call_method0("cpu_count")?.extract()?;
    let tree = parse_static_toml(cpu_count)
        .map_err(|e| PyValueError::new_err(format!("Invalid embedded static.toml: {e}")))?;
    match tree {
        Some(tree) => Ok(Some(value_to_py(py, &tree)?.cast_into::<PyDict>()?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_static_toml_parses() {
        let parsed = parse_static_toml(Some(4)).expect("embedded static.toml must be valid TOML");
        let tree = parsed.expect("static.toml is embedded by meerschaum-core");
        assert!(tree.as_map().is_some_and(|m| !m.is_empty()));
    }

    #[test]
    fn markers_are_resolved_by_core() {
        let text = "a = { __null__ = true }\nb = { __cpu_count_or__ = 5 }\n\
                    c = { __null__ = true, x = 1 }\nd = { __null__ = false }";
        let parse = |cpu| meerschaum_core::static_config::parse_static_toml(text, cpu).unwrap();
        let with_cpus = parse(Some(8));
        assert_eq!(with_cpus.get("a"), Some(&Value::Null));
        assert_eq!(with_cpus.get("b"), Some(&Value::Int(8)));
        assert!(with_cpus.get("c").is_some_and(Value::is_map));
        assert!(with_cpus.get("d").is_some_and(Value::is_map));
        // `os.cpu_count() or N`: `None` and `0` both fall back.
        assert_eq!(parse(None).get("b"), Some(&Value::Int(5)));
        assert_eq!(parse(Some(0)).get("b"), Some(&Value::Int(5)));
        assert_eq!(NULL_MARKER, "__null__");
        assert_eq!(CPU_COUNT_MARKER, "__cpu_count_or__");
    }
}
