//! `_core.static_config()`: the shared static configuration (D15).
//!
//! `crates/meerschaum-core/static.toml` is embedded with `include_str!` when `build.rs` finds it.
//! When it did not exist at build time, [`STATIC_TOML`] is `None` and the Python function returns
//! `None`; the Python façade keeps using `meerschaum._internal.static.STATIC_CONFIG`.
//!
//! TOML has no `None` and cannot express runtime values, so `static.toml` uses two single-key
//! marker tables (documented in the file's header), resolved here exactly as Python would:
//!
//! - `{ __null__ = true }` becomes `None`.
//! - `{ __cpu_count_or__ = N }` becomes `os.cpu_count() or N`, calling Python's `os.cpu_count()`
//!   so the value matches `STATIC_CONFIG` on the same machine.
//!
//! Sets and tuples in `STATIC_CONFIG` are lists in the file (sets sorted) and come back as lists.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// The raw contents of `static.toml`, if it existed at build time.
#[cfg(mrsm_static_toml)]
pub const STATIC_TOML: Option<&str> = Some(include_str!(env!("MRSM_STATIC_TOML_PATH")));

/// The raw contents of `static.toml`, if it existed at build time.
#[cfg(not(mrsm_static_toml))]
pub const STATIC_TOML: Option<&str> = None;

/// Parse the embedded `static.toml`, if any.
pub fn parse_static_toml() -> Result<Option<toml::Table>, toml::de::Error> {
    STATIC_TOML.map(str::parse::<toml::Table>).transpose()
}

/// The marker table keys understood by [`toml_to_py`].
pub const NULL_MARKER: &str = "__null__";
/// See [`NULL_MARKER`].
pub const CPU_COUNT_OR_MARKER: &str = "__cpu_count_or__";

/// A marker table found in `static.toml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Marker {
    /// `{ __null__ = true }`
    Null,
    /// `{ __cpu_count_or__ = N }`
    CpuCountOr(i64),
}

/// Recognize a single-key marker table.
pub fn marker(table: &toml::Table) -> Option<Marker> {
    if table.len() != 1 {
        return None;
    }
    let (key, value) = table.iter().next()?;
    match (key.as_str(), value) {
        (NULL_MARKER, toml::Value::Boolean(true)) => Some(Marker::Null),
        (CPU_COUNT_OR_MARKER, toml::Value::Integer(n)) => Some(Marker::CpuCountOr(*n)),
        _ => None,
    }
}

/// Convert a TOML value into the equivalent Python object (tables become `dict`, arrays `list`,
/// datetimes their RFC 3339 string, marker tables their resolved value).
pub fn toml_to_py<'py>(py: Python<'py>, value: &toml::Value) -> PyResult<Bound<'py, PyAny>> {
    if let toml::Value::Table(table) = value {
        match marker(table) {
            Some(Marker::Null) => return Ok(py.None().into_bound(py)),
            Some(Marker::CpuCountOr(default)) => {
                let count = py.import("os")?.call_method0("cpu_count")?;
                // Python: `os.cpu_count() or N` (None and 0 both fall back).
                return if count.is_truthy()? {
                    Ok(count)
                } else {
                    Ok(default.into_pyobject(py)?.into_any())
                };
            }
            None => {}
        }
    }
    Ok(match value {
        toml::Value::String(s) => s.into_pyobject(py)?.into_any(),
        toml::Value::Integer(i) => i.into_pyobject(py)?.into_any(),
        toml::Value::Float(f) => f.into_pyobject(py)?.into_any(),
        toml::Value::Boolean(b) => b.into_pyobject(py)?.to_owned().into_any(),
        toml::Value::Datetime(dt) => dt.to_string().into_pyobject(py)?.into_any(),
        toml::Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(toml_to_py(py, item)?)?;
            }
            list.into_any()
        }
        toml::Value::Table(table) => table_to_py(py, table)?.into_any(),
    })
}

fn table_to_py<'py>(py: Python<'py>, table: &toml::Table) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in table {
        dict.set_item(key, toml_to_py(py, value)?)?;
    }
    Ok(dict)
}

/// `_core.static_config() -> dict | None`. Returns a fresh dict on every call, so callers may
/// mutate it without affecting later calls.
#[pyfunction(name = "static_config")]
pub fn py_static_config(py: Python<'_>) -> PyResult<Option<Bound<'_, PyDict>>> {
    let table = parse_static_toml()
        .map_err(|e| PyValueError::new_err(format!("Invalid embedded static.toml: {e}")))?;
    table.map(|t| table_to_py(py, &t)).transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_static_toml_parses() {
        let parsed = parse_static_toml().expect("embedded static.toml must be valid TOML");
        assert_eq!(parsed.is_some(), cfg!(mrsm_static_toml));
        if let Some(table) = parsed {
            assert!(!table.is_empty());
        }
    }

    #[test]
    fn markers_are_recognized() {
        let table: toml::Table = "a = { __null__ = true }\nb = { __cpu_count_or__ = 5 }\n\
                                  c = { __null__ = true, x = 1 }\nd = { __null__ = false }"
            .parse()
            .unwrap();
        let get = |k: &str| table[k].as_table().unwrap();
        assert_eq!(marker(get("a")), Some(Marker::Null));
        assert_eq!(marker(get("b")), Some(Marker::CpuCountOr(5)));
        assert_eq!(marker(get("c")), None);
        assert_eq!(marker(get("d")), None);
    }
}
