//! An ordered, JSON-like dynamic value that mirrors the Python objects Meerschaum stores in
//! pipe parameters and configuration (`dict`, `list`, `str`, `int`, `float`, `bool`, `None`).
//!
//! Maps preserve insertion order (like Python dicts), so parameters and config round-trip
//! through YAML and JSON with their key order intact.

use std::fmt;

use indexmap::IndexMap;
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};

/// An insertion-ordered string-keyed map (a Python `dict` with string keys).
pub type Map = IndexMap<String, Value>;

/// A dynamic value: the Rust analogue of the JSON-compatible subset of Python objects.
///
/// Python tuples and sets are represented as [`Value::List`]. Python `int` is limited to `i64`.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Value {
    /// Python `None` / JSON `null`.
    #[default]
    Null,
    /// Python `bool`.
    Bool(bool),
    /// Python `int` (limited to 64 bits).
    Int(i64),
    /// Python `float`.
    Float(f64),
    /// Python `str`.
    Str(String),
    /// Python `list` (also used for `tuple` and `set`).
    List(Vec<Value>),
    /// Python `dict` with string keys, in insertion order.
    Map(Map),
}

impl Value {
    /// Return an empty map value.
    pub fn new_map() -> Value {
        Value::Map(Map::new())
    }

    /// `True` if this is `None`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// `True` if this is a map.
    pub fn is_map(&self) -> bool {
        matches!(self, Value::Map(_))
    }

    /// `True` if this is a list.
    pub fn is_list(&self) -> bool {
        matches!(self, Value::List(_))
    }

    /// `True` if this is a string.
    pub fn is_str(&self) -> bool {
        matches!(self, Value::Str(_))
    }

    /// Borrow the string, if this is a string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Borrow the map, if this is a map.
    pub fn as_map(&self) -> Option<&Map> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Mutably borrow the map, if this is a map.
    pub fn as_map_mut(&mut self) -> Option<&mut Map> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Borrow the list, if this is a list.
    pub fn as_list(&self) -> Option<&Vec<Value>> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    /// The integer value of an `Int` (or a `Bool`, which is an `int` in Python).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            Value::Bool(b) => Some(i64::from(*b)),
            _ => None,
        }
    }

    /// The numeric value of an `Int`, `Float` or `Bool` as a float.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// The boolean value of a `Bool`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Python truthiness (`bool(value)`).
    pub fn truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::Str(s) => !s.is_empty(),
            Value::List(l) => !l.is_empty(),
            Value::Map(m) => !m.is_empty(),
        }
    }

    /// The Python type name of this value (`'dict'`, `'str'`, ...).
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NoneType",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::Str(_) => "str",
            Value::List(_) => "list",
            Value::Map(_) => "dict",
        }
    }

    /// Index a map by key (`value.get(key)` for dicts; `None` otherwise).
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_map().and_then(|m| m.get(key))
    }

    /// Walk nested maps by keys (`value[k1][k2]...`). Returns `None` if any step is missing
    /// or is not a map.
    pub fn get_path(&self, keys: &[&str]) -> Option<&Value> {
        let mut current = self;
        for key in keys {
            current = current.as_map()?.get(*key)?;
        }
        Some(current)
    }

    /// Parse standard JSON into a `Value`, preserving key order.
    pub fn from_json_str(s: &str) -> Result<Value, serde_json::Error> {
        serde_json::from_str(s)
    }

    /// Serialize to standard (compact) JSON via `serde_json`.
    ///
    /// For Python-identical output (`json.dumps`), use [`crate::pyfmt::json_dumps`].
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "null".to_string())
    }

    /// Return a deep copy of the map entries, or an empty map for non-map values.
    pub fn to_map(&self) -> Map {
        self.as_map().cloned().unwrap_or_default()
    }

    /// Python's `str(value)` (strings unquoted, everything else `repr`).
    pub fn py_str(&self) -> String {
        crate::pyfmt::py_str(self)
    }

    /// Python's `repr(value)`.
    pub fn py_repr(&self) -> String {
        crate::pyfmt::py_repr(self)
    }

    /// Python's `json.dumps(value)` with the default separators and `ensure_ascii=True`.
    pub fn json_dumps(&self) -> String {
        crate::pyfmt::json_dumps(self, crate::pyfmt::JsonStyle::Default, true)
    }
}

/// Port of `meerschaum.config._patch.apply_patch_to_config` (cascading dict patch).
///
/// - A non-dict or empty `patch` returns `config` unchanged.
/// - A non-dict or empty `config` returns a copy of `patch`.
/// - Nested dicts are merged recursively; other values are replaced.
///
/// Existing keys keep their position; new keys are appended in patch order.
pub fn apply_patch(config: &Value, patch: &Value) -> Value {
    let patch_map = match patch {
        Value::Map(m) if !m.is_empty() => m,
        _ => return config.clone(),
    };
    let base_map = match config {
        Value::Map(m) if !m.is_empty() => m,
        _ => return patch.clone(),
    };
    let mut base = base_map.clone();
    for (key, value) in patch_map {
        let merged = match (value, base.get(key)) {
            (Value::Map(_), Some(existing @ Value::Map(_))) => apply_patch(existing, value),
            _ => value.clone(),
        };
        base.insert(key.clone(), merged);
    }
    Value::Map(base)
}

/// [`apply_patch`] over maps.
pub fn apply_patch_maps(config: &Map, patch: &Map) -> Map {
    match apply_patch(&Value::Map(config.clone()), &Value::Map(patch.clone())) {
        Value::Map(m) => m,
        _ => Map::new(),
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Str(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Str(s)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Int(i64::from(i))
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Map> for Value {
    fn from(m: Map) -> Self {
        Value::Map(m)
    }
}

impl From<Vec<Value>> for Value {
    fn from(l: Vec<Value>) -> Self {
        Value::List(l)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(o: Option<T>) -> Self {
        match o {
            Some(v) => v.into(),
            None => Value::Null,
        }
    }
}

impl fmt::Display for Value {
    /// Formats like Python's `str()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&crate::pyfmt::py_str(self))
    }
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Int(i) => serializer.serialize_i64(*i),
            Value::Float(f) => serializer.serialize_f64(*f),
            Value::Str(s) => serializer.serialize_str(s),
            Value::List(l) => {
                let mut seq = serializer.serialize_seq(Some(l.len()))?;
                for item in l {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Value::Map(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON-compatible value")
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
        Ok(Value::Int(v))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
        // Python ints are unbounded; values beyond i64 degrade to floats.
        Ok(i64::try_from(v).map_or(Value::Float(v as f64), Value::Int))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
        Ok(Value::Float(v))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
        Ok(Value::Str(v.to_string()))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        Ok(Value::Str(v))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        Value::deserialize(d)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut out = Vec::new();
        while let Some(item) = seq.next_element::<Value>()? {
            out.push(item);
        }
        Ok(Value::List(out))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let mut out = Map::new();
        while let Some((k, v)) = access.next_entry::<String, Value>()? {
            out.insert(k, v);
        }
        Ok(Value::Map(out))
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(ValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(s: &str) -> Value {
        Value::from_json_str(s).unwrap()
    }

    #[test]
    fn patch_semantics() {
        let base = json(r#"{"a": {"x": 1, "y": 2}, "b": 1}"#);
        let patch = json(r#"{"a": {"y": 3, "z": 4}, "c": [1]}"#);
        let out = apply_patch(&base, &patch);
        assert_eq!(
            out.to_json_string(),
            r#"{"a":{"x":1,"y":3,"z":4},"b":1,"c":[1]}"#
        );
        // PARITY: an empty dict in the patch leaves the base value untouched.
        let out = apply_patch(&base, &json(r#"{"a": {}}"#));
        assert_eq!(out, base);
    }

    #[test]
    fn python_renderings() {
        assert_eq!(Value::Bool(true).py_str(), "True");
        assert_eq!(Value::Null.py_str(), "None");
        assert_eq!(Value::Float(1.0).py_str(), "1.0");
        assert_eq!(Value::from("a'b").py_repr(), "\"a'b\"");
        let v = json(r#"{"b": 1, "a": [true, null, "é"]}"#);
        assert_eq!(
            v.json_dumps(),
            "{\"b\": 1, \"a\": [true, null, \"\\u00e9\"]}"
        );
        assert_eq!(v.py_str(), "{'b': 1, 'a': [True, None, 'é']}");
    }

    #[test]
    fn json_order_round_trip() {
        let s = r#"{"z":1,"a":{"m":null,"b":[true,1.5,"x"]}}"#;
        let v = Value::from_json_str(s).unwrap();
        assert_eq!(v.to_json_string(), s);
    }
}
