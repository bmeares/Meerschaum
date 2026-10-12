//! An ordered, Python-shaped value used for `params` filters and pipe `parameters`.
//!
//! The query builders interpolate `str(value)` and `json.dumps(value)`, so the exact Python
//! string forms matter (`True`, `None`, `1.0`, `{"a": 1}`). JSON objects keep their key order,
//! like Python dicts, without relying on `serde_json`'s `preserve_order` feature.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::pytext::{py_float_repr, py_json_dumps_str};

/// A JSON-compatible value with Python `str()` / `json.dumps()` renderings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PyValue {
    /// Python `None`.
    None,
    /// Python `bool`.
    Bool(bool),
    /// Python `int` (64-bit).
    Int(i64),
    /// Python `float`.
    Float(f64),
    /// Python `str`.
    Str(String),
    /// Python `list` (also used for tuples).
    List(Vec<PyValue>),
    /// Python `dict` (insertion-ordered).
    Dict(IndexMap<String, PyValue>),
}

impl PyValue {
    /// Python's `str(value)`.
    pub fn py_str(&self) -> String {
        match self {
            PyValue::Str(s) => s.clone(),
            other => other.py_repr(),
        }
    }

    /// Python's `repr(value)` (strings use single quotes like CPython).
    pub fn py_repr(&self) -> String {
        match self {
            PyValue::None => "None".to_string(),
            PyValue::Bool(true) => "True".to_string(),
            PyValue::Bool(false) => "False".to_string(),
            PyValue::Int(i) => i.to_string(),
            PyValue::Float(f) => py_float_repr(*f),
            PyValue::Str(s) => py_str_repr(s),
            PyValue::List(items) => {
                let inner: Vec<String> = items.iter().map(PyValue::py_repr).collect();
                format!("[{}]", inner.join(", "))
            }
            PyValue::Dict(map) => {
                let inner: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", py_str_repr(k), v.py_repr()))
                    .collect();
                format!("{{{}}}", inner.join(", "))
            }
        }
    }

    /// Python's `json.dumps(value)` with the default separators and `ensure_ascii=True`.
    pub fn json_dumps(&self) -> String {
        match self {
            PyValue::None => "null".to_string(),
            PyValue::Bool(true) => "true".to_string(),
            PyValue::Bool(false) => "false".to_string(),
            PyValue::Int(i) => i.to_string(),
            PyValue::Float(f) => {
                if f.is_nan() {
                    "NaN".to_string()
                } else if f.is_infinite() {
                    if *f > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
                } else {
                    py_float_repr(*f)
                }
            }
            PyValue::Str(s) => py_json_dumps_str(s),
            PyValue::List(items) => {
                let inner: Vec<String> = items.iter().map(PyValue::json_dumps).collect();
                format!("[{}]", inner.join(", "))
            }
            PyValue::Dict(map) => {
                let inner: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", py_json_dumps_str(k), v.json_dumps()))
                    .collect();
                format!("{{{}}}", inner.join(", "))
            }
        }
    }

    /// Python truthiness.
    pub fn is_truthy(&self) -> bool {
        match self {
            PyValue::None => false,
            PyValue::Bool(b) => *b,
            PyValue::Int(i) => *i != 0,
            PyValue::Float(f) => *f != 0.0,
            PyValue::Str(s) => !s.is_empty(),
            PyValue::List(l) => !l.is_empty(),
            PyValue::Dict(d) => !d.is_empty(),
        }
    }

    /// Borrow as a string if this is a `str`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PyValue::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Borrow as a dict if this is a `dict`.
    pub fn as_dict(&self) -> Option<&IndexMap<String, PyValue>> {
        match self {
            PyValue::Dict(d) => Some(d),
            _ => None,
        }
    }

    /// Return a `dict` entry if this is a dict.
    pub fn get(&self, key: &str) -> Option<&PyValue> {
        self.as_dict().and_then(|d| d.get(key))
    }

    /// Integer value if this is an `int` (bools excluded, like `isinstance(x, int) and not bool`).
    pub fn as_int(&self) -> Option<i64> {
        match self {
            PyValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Whether this is `None`.
    pub fn is_none(&self) -> bool {
        matches!(self, PyValue::None)
    }
}

impl From<&str> for PyValue {
    fn from(s: &str) -> Self {
        PyValue::Str(s.to_string())
    }
}

impl From<String> for PyValue {
    fn from(s: String) -> Self {
        PyValue::Str(s)
    }
}

impl From<i64> for PyValue {
    fn from(i: i64) -> Self {
        PyValue::Int(i)
    }
}

impl From<bool> for PyValue {
    fn from(b: bool) -> Self {
        PyValue::Bool(b)
    }
}

/// CPython's `repr()` of a `str`.
pub fn py_str_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_forms() {
        assert_eq!(PyValue::Bool(true).py_str(), "True");
        assert_eq!(PyValue::None.py_str(), "None");
        assert_eq!(PyValue::Float(1.0).py_str(), "1.0");
        assert_eq!(PyValue::Str("a'b".into()).py_repr(), "\"a'b\"");
    }

    #[test]
    fn json_forms() {
        let v: PyValue = serde_json::from_str(r#"{"b": 1, "a": [true, null, "é"]}"#).unwrap();
        assert_eq!(
            v.json_dumps(),
            "{\"b\": 1, \"a\": [true, null, \"\\u00e9\"]}"
        );
    }
}
