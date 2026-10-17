//! The lossless value the config registry stores.
//!
//! [`crate::Value`] is the workspace's JSON-like parameters value; the config files are YAML, and
//! PyYAML (and so `get_config`, `read_config` and `write_config` in v4) keeps what JSON cannot:
//! `datetime.date` / `datetime.datetime`, `bytes`, integers of any size, tuples, sets and mapping
//! keys of any hashable type. [`ConfigValue`] holds all of it, so a `when: 2024-01-01` in
//! `config.yaml` is a date through `get_config` and is written back as a date, and nothing is
//! projected through JSON on the way.
//!
//! The common case stays cheap: a mapping whose keys are all strings is a [`ConfigMap`]
//! (`IndexMap<String, ConfigValue>`); a mapping with at least one other key is
//! [`ConfigValue::Pairs`]. A value is canonical when it is a `Map` iff every key is a string.
//!
//! The JSON helpers here reproduce CPython's `json` module (3.14): [`json_loads`] raises the
//! messages of `json.JSONDecodeError`, [`json_dumps`] the `TypeError`s of `json.dumps`, and
//! [`json_dumps_lossless`] writes the leaves JSON cannot hold as placeholder strings that
//! [`restore_placeholders`] swaps back after the text was edited and parsed (`MRSM{}` substitution
//! rewrites a `json.dumps` text, and must not lose a date it never touched).
//!
//! [`ConfigValue::to_value`] is the lossy projection (dates become ISO strings, big integers decimal
//! strings, non-string keys their JSON spelling) for consumers whose data model is JSON.

use std::collections::HashMap;
use std::rc::Rc;

use indexmap::IndexMap;
use num_bigint::BigInt;
use num_traits::{ToPrimitive, Zero};

use crate::pyfmt::{JsonStyle, bytes_repr, float_repr, json_dumps_str, str_repr};
use crate::value::{Map, Value};
use crate::yaml::dumper::value_from_node;
use crate::yaml::node::{Date, DateTime, Node, NodeRef, PyKey, py_key};

/// An insertion-ordered mapping with string keys.
pub type ConfigMap = IndexMap<String, ConfigValue>;

/// A lossless config value: the Rust form of what PyYAML constructs.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ConfigValue {
    /// `None`.
    #[default]
    Null,
    /// `bool`.
    Bool(bool),
    /// `int` that fits 64 bits.
    Int(i64),
    /// `int` beyond 64 bits.
    BigInt(BigInt),
    /// `float`, including `inf` and `nan`.
    Float(f64),
    /// `str`.
    Str(String),
    /// `bytes`.
    Bytes(Vec<u8>),
    /// `datetime.date`.
    Date(Date),
    /// `datetime.datetime`, naive or aware.
    DateTime(DateTime),
    /// `list`.
    List(Vec<ConfigValue>),
    /// `tuple`.
    Tuple(Vec<ConfigValue>),
    /// `set`.
    Set(Vec<ConfigValue>),
    /// `dict` with string keys only.
    Map(ConfigMap),
    /// `dict` with at least one key that is not a string, in insertion order. Keys are unique
    /// under Python equality (`1 == True == 1.0`).
    Pairs(Vec<(ConfigValue, ConfigValue)>),
}

impl From<ConfigMap> for ConfigValue {
    fn from(m: ConfigMap) -> Self {
        ConfigValue::Map(m)
    }
}

impl ConfigValue {
    /// An empty mapping.
    pub fn new_map() -> ConfigValue {
        ConfigValue::Map(ConfigMap::new())
    }

    /// A string value.
    pub fn str(s: impl Into<String>) -> ConfigValue {
        ConfigValue::Str(s.into())
    }

    /// `True` if this is a mapping of either kind (`isinstance(x, dict)`).
    pub fn is_dict(&self) -> bool {
        matches!(self, ConfigValue::Map(_) | ConfigValue::Pairs(_))
    }

    /// `True` if this is `None`.
    pub fn is_null(&self) -> bool {
        matches!(self, ConfigValue::Null)
    }

    /// Borrow the string, if this is one.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ConfigValue::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Borrow the string-keyed map, if this is one.
    pub fn as_map(&self) -> Option<&ConfigMap> {
        match self {
            ConfigValue::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Mutably borrow the string-keyed map, if this is one.
    pub fn as_map_mut(&mut self) -> Option<&mut ConfigMap> {
        match self {
            ConfigValue::Map(m) => Some(m),
            _ => None,
        }
    }

    /// The Python type name (`'dict'`, `'date'`, ...).
    pub fn type_name(&self) -> &'static str {
        match self {
            ConfigValue::Null => "NoneType",
            ConfigValue::Bool(_) => "bool",
            ConfigValue::Int(_) | ConfigValue::BigInt(_) => "int",
            ConfigValue::Float(_) => "float",
            ConfigValue::Str(_) => "str",
            ConfigValue::Bytes(_) => "bytes",
            ConfigValue::Date(_) => "date",
            ConfigValue::DateTime(_) => "datetime",
            ConfigValue::List(_) => "list",
            ConfigValue::Tuple(_) => "tuple",
            ConfigValue::Set(_) => "set",
            ConfigValue::Map(_) | ConfigValue::Pairs(_) => "dict",
        }
    }

    /// Python truthiness (`bool(value)`).
    pub fn truthy(&self) -> bool {
        match self {
            ConfigValue::Null => false,
            ConfigValue::Bool(b) => *b,
            ConfigValue::Int(i) => *i != 0,
            ConfigValue::BigInt(i) => !i.is_zero(),
            ConfigValue::Float(f) => *f != 0.0,
            ConfigValue::Str(s) => !s.is_empty(),
            ConfigValue::Bytes(b) => !b.is_empty(),
            ConfigValue::Date(_) | ConfigValue::DateTime(_) => true,
            ConfigValue::List(l) | ConfigValue::Tuple(l) | ConfigValue::Set(l) => !l.is_empty(),
            ConfigValue::Map(m) => !m.is_empty(),
            ConfigValue::Pairs(p) => !p.is_empty(),
        }
    }

    /// `value[key]` for a string key of a mapping.
    pub fn get(&self, key: &str) -> Option<&ConfigValue> {
        match self {
            ConfigValue::Map(m) => m.get(key),
            ConfigValue::Pairs(p) => p
                .iter()
                .find(|(k, _)| matches!(k, ConfigValue::Str(s) if s == key))
                .map(|(_, v)| v),
            _ => None,
        }
    }

    /// `value[a][b]...` for string keys.
    pub fn get_path(&self, keys: &[&str]) -> Option<&ConfigValue> {
        let mut current = self;
        for key in keys {
            current = current.get(key)?;
        }
        Some(current)
    }

    /// The entries of a mapping, cloned (`dict.items()`).
    pub fn dict_entries(&self) -> Option<Vec<(ConfigValue, ConfigValue)>> {
        match self {
            ConfigValue::Map(m) => Some(
                m.iter()
                    .map(|(k, v)| (ConfigValue::Str(k.clone()), v.clone()))
                    .collect(),
            ),
            ConfigValue::Pairs(p) => Some(p.clone()),
            _ => None,
        }
    }

    /// A mapping from entries, built the way `dict[key] = value` does (a repeated key keeps its
    /// first position and takes the last value); a `Map` when every key is a string.
    ///
    /// An entry whose key is unhashable (list, dict, set) is dropped; Python cannot build one.
    pub fn from_pairs(entries: Vec<(ConfigValue, ConfigValue)>) -> ConfigValue {
        if entries
            .iter()
            .all(|(k, _)| matches!(k, ConfigValue::Str(_)))
        {
            let mut map = ConfigMap::with_capacity(entries.len());
            for (k, v) in entries {
                if let ConfigValue::Str(k) = k {
                    map.insert(k, v);
                }
            }
            return ConfigValue::Map(map);
        }
        let mut index: HashMap<PyKey, usize> = HashMap::new();
        let mut out: Vec<(ConfigValue, ConfigValue)> = Vec::with_capacity(entries.len());
        for (k, v) in entries {
            let Some(key) = py_key(&k.to_node()) else {
                continue;
            };
            match index.get(&key) {
                Some(&pos) => out[pos].1 = v,
                None => {
                    index.insert(key, out.len());
                    out.push((k, v));
                }
            }
        }
        ConfigValue::Pairs(out)
    }

    /// Build from the JSON-like [`Value`] (always lossless in this direction).
    pub fn from_value(value: &Value) -> ConfigValue {
        match value {
            Value::Null => ConfigValue::Null,
            Value::Bool(b) => ConfigValue::Bool(*b),
            Value::Int(i) => ConfigValue::Int(*i),
            Value::Float(f) => ConfigValue::Float(*f),
            Value::Str(s) => ConfigValue::Str(s.clone()),
            Value::List(items) => ConfigValue::List(items.iter().map(Self::from_value).collect()),
            Value::Map(m) => ConfigValue::Map(Self::map_from_value(m)),
        }
    }

    /// [`from_value`](Self::from_value) over a map.
    pub fn map_from_value(map: &Map) -> ConfigMap {
        map.iter()
            .map(|(k, v)| (k.clone(), Self::from_value(v)))
            .collect()
    }

    /// The lossy JSON-like projection (dates become ISO strings, big integers decimal strings,
    /// bytes base64, non-string keys their JSON spelling, tuples and sets lists).
    pub fn to_value(&self) -> Value {
        match self {
            // Fast paths for the JSON-like variants, so a plain config tree is not rebuilt as nodes.
            ConfigValue::Null => Value::Null,
            ConfigValue::Bool(b) => Value::Bool(*b),
            ConfigValue::Int(i) => Value::Int(*i),
            ConfigValue::Float(f) => Value::Float(*f),
            ConfigValue::Str(s) => Value::Str(s.clone()),
            ConfigValue::List(items) => Value::List(items.iter().map(Self::to_value).collect()),
            ConfigValue::Map(m) => Value::Map(Self::map_to_value(m)),
            other => value_from_node(&other.to_node()),
        }
    }

    /// [`to_value`](Self::to_value) over a map.
    pub fn map_to_value(map: &ConfigMap) -> Map {
        map.iter().map(|(k, v)| (k.clone(), v.to_value())).collect()
    }

    /// Build from a lossless YAML node (aliases become copies).
    pub fn from_node(node: &Node) -> ConfigValue {
        match node {
            Node::Null => ConfigValue::Null,
            Node::Bool(b) => ConfigValue::Bool(*b),
            Node::Int(i) => match i.to_i64() {
                Some(n) => ConfigValue::Int(n),
                None => ConfigValue::BigInt(i.clone()),
            },
            Node::Float(f) => ConfigValue::Float(*f),
            Node::Str(s) => ConfigValue::Str(s.clone()),
            Node::Bytes(b) => ConfigValue::Bytes(b.clone()),
            Node::Date(d) => ConfigValue::Date(d.clone()),
            Node::DateTime(dt) => ConfigValue::DateTime(dt.clone()),
            Node::Seq(items) => {
                ConfigValue::List(items.iter().map(|i| Self::from_node(i)).collect())
            }
            Node::Tuple(items) => {
                ConfigValue::Tuple(items.iter().map(|i| Self::from_node(i)).collect())
            }
            Node::Set(items) => {
                ConfigValue::Set(items.iter().map(|i| Self::from_node(i)).collect())
            }
            Node::Map(entries) => {
                if entries.iter().all(|(k, _)| matches!(&**k, Node::Str(_))) {
                    ConfigValue::Map(
                        entries
                            .iter()
                            .filter_map(|(k, v)| match &**k {
                                Node::Str(s) => Some((s.clone(), Self::from_node(v))),
                                _ => None,
                            })
                            .collect(),
                    )
                } else {
                    ConfigValue::Pairs(
                        entries
                            .iter()
                            .map(|(k, v)| (Self::from_node(k), Self::from_node(v)))
                            .collect(),
                    )
                }
            }
        }
    }

    /// The node the YAML dumper writes this value from.
    pub fn to_node(&self) -> NodeRef {
        Rc::new(match self {
            ConfigValue::Null => Node::Null,
            ConfigValue::Bool(b) => Node::Bool(*b),
            ConfigValue::Int(i) => Node::Int(BigInt::from(*i)),
            ConfigValue::BigInt(i) => Node::Int(i.clone()),
            ConfigValue::Float(f) => Node::Float(*f),
            ConfigValue::Str(s) => Node::Str(s.clone()),
            ConfigValue::Bytes(b) => Node::Bytes(b.clone()),
            ConfigValue::Date(d) => Node::Date(d.clone()),
            ConfigValue::DateTime(dt) => Node::DateTime(dt.clone()),
            ConfigValue::List(items) => Node::Seq(items.iter().map(Self::to_node).collect()),
            ConfigValue::Tuple(items) => Node::Tuple(items.iter().map(Self::to_node).collect()),
            ConfigValue::Set(items) => Node::Set(items.iter().map(Self::to_node).collect()),
            ConfigValue::Map(m) => Node::Map(
                m.iter()
                    .map(|(k, v)| (Node::string(k.clone()), v.to_node()))
                    .collect(),
            ),
            ConfigValue::Pairs(p) => {
                Node::Map(p.iter().map(|(k, v)| (k.to_node(), v.to_node())).collect())
            }
        })
    }

    /// Whether any string, key or value, holds `needle` (what `needle in json.dumps(value)`
    /// asks of JSON-like data).
    pub fn contains_text(&self, needle: &str) -> bool {
        match self {
            ConfigValue::Str(s) => s.contains(needle),
            ConfigValue::List(items) | ConfigValue::Tuple(items) | ConfigValue::Set(items) => {
                items.iter().any(|v| v.contains_text(needle))
            }
            ConfigValue::Map(m) => m
                .iter()
                .any(|(k, v)| k.contains(needle) || v.contains_text(needle)),
            ConfigValue::Pairs(p) => p
                .iter()
                .any(|(k, v)| k.contains_text(needle) || v.contains_text(needle)),
            _ => false,
        }
    }
}

/// [`ConfigValue::contains_text`] over a map.
pub fn map_contains_text(map: &ConfigMap, needle: &str) -> bool {
    map.iter()
        .any(|(k, v)| k.contains(needle) || v.contains_text(needle))
}

// ---------------------------------------------------------------------------------------------
// apply_patch_to_config
// ---------------------------------------------------------------------------------------------

/// Port of `meerschaum.config._patch.apply_patch_to_config` (cascading dict patch) over lossless
/// values. A non-dict or empty `patch` returns `config`; a non-dict or empty `config` returns a
/// copy of `patch`; nested dicts merge; everything else is replaced. Existing keys keep their
/// position and new keys are appended in patch order.
pub fn apply_patch(config: &ConfigValue, patch: &ConfigValue) -> ConfigValue {
    if !patch.is_dict() || !patch.truthy() {
        return config.clone();
    }
    if !config.is_dict() || !config.truthy() {
        return patch.clone();
    }
    if let (ConfigValue::Map(base), ConfigValue::Map(p)) = (config, patch) {
        return ConfigValue::Map(apply_patch_maps(base, p));
    }
    let mut entries = config.dict_entries().unwrap_or_default();
    for (key, value) in patch.dict_entries().unwrap_or_default() {
        let found = py_key(&key.to_node()).and_then(|pk| {
            entries
                .iter()
                .position(|(k, _)| py_key(&k.to_node()).as_ref() == Some(&pk))
        });
        match found {
            Some(pos) => {
                let merged = if value.is_dict() && entries[pos].1.is_dict() {
                    apply_patch(&entries[pos].1, &value)
                } else {
                    value
                };
                entries[pos].1 = merged;
            }
            None => entries.push((key, value)),
        }
    }
    ConfigValue::from_pairs(entries)
}

/// [`apply_patch`] over string-keyed maps.
pub fn apply_patch_maps(config: &ConfigMap, patch: &ConfigMap) -> ConfigMap {
    if patch.is_empty() {
        return config.clone();
    }
    if config.is_empty() {
        return patch.clone();
    }
    let mut base = config.clone();
    for (key, value) in patch {
        let merged = match (value, base.get(key)) {
            (v, Some(existing)) if v.is_dict() && existing.is_dict() => apply_patch(existing, v),
            _ => value.clone(),
        };
        base.insert(key.clone(), merged);
    }
    base
}

// ---------------------------------------------------------------------------------------------
// repr / str
// ---------------------------------------------------------------------------------------------

fn timedelta_repr(us: i64) -> String {
    let days = us.div_euclid(86_400_000_000);
    let rest = us.rem_euclid(86_400_000_000);
    let (seconds, micro) = (rest / 1_000_000, rest % 1_000_000);
    let mut parts: Vec<String> = Vec::new();
    if days != 0 {
        parts.push(format!("days={days}"));
    }
    if seconds != 0 {
        parts.push(format!("seconds={seconds}"));
    }
    if micro != 0 {
        parts.push(format!("microseconds={micro}"));
    }
    if parts.is_empty() {
        parts.push("0".to_string());
    }
    format!("datetime.timedelta({})", parts.join(", "))
}

/// Python's `repr()` of a lossless value.
pub fn py_repr(value: &ConfigValue) -> String {
    match value {
        ConfigValue::Null => "None".to_string(),
        ConfigValue::Bool(b) => if *b { "True" } else { "False" }.to_string(),
        ConfigValue::Int(i) => i.to_string(),
        ConfigValue::BigInt(i) => i.to_string(),
        ConfigValue::Float(f) => float_repr(*f),
        ConfigValue::Str(s) => str_repr(s),
        ConfigValue::Bytes(b) => bytes_repr(b),
        ConfigValue::Date(d) => format!("datetime.date({}, {}, {})", d.year, d.month, d.day),
        ConfigValue::DateTime(dt) => {
            let mut fields = vec![
                dt.date.year.to_string(),
                dt.date.month.to_string(),
                dt.date.day.to_string(),
                dt.hour.to_string(),
                dt.minute.to_string(),
            ];
            if dt.microsecond != 0 {
                fields.push(dt.second.to_string());
                fields.push(dt.microsecond.to_string());
            } else if dt.second != 0 {
                fields.push(dt.second.to_string());
            }
            let tz = match dt.utc_offset_us {
                None => String::new(),
                Some(0) => ", tzinfo=datetime.timezone.utc".to_string(),
                Some(us) => format!(", tzinfo=datetime.timezone({})", timedelta_repr(us)),
            };
            format!("datetime.datetime({}{tz})", fields.join(", "))
        }
        ConfigValue::List(items) => {
            format!(
                "[{}]",
                items.iter().map(py_repr).collect::<Vec<_>>().join(", ")
            )
        }
        ConfigValue::Tuple(items) => match items.len() {
            0 => "()".to_string(),
            1 => format!("({},)", py_repr(&items[0])),
            _ => format!(
                "({})",
                items.iter().map(py_repr).collect::<Vec<_>>().join(", ")
            ),
        },
        ConfigValue::Set(items) => {
            if items.is_empty() {
                "set()".to_string()
            } else {
                format!(
                    "{{{}}}",
                    items.iter().map(py_repr).collect::<Vec<_>>().join(", ")
                )
            }
        }
        ConfigValue::Map(m) => format!(
            "{{{}}}",
            m.iter()
                .map(|(k, v)| format!("{}: {}", str_repr(k), py_repr(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ConfigValue::Pairs(p) => format!(
            "{{{}}}",
            p.iter()
                .map(|(k, v)| format!("{}: {}", py_repr(k), py_repr(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Python's `str()` of a lossless value (strings unquoted; dates as ISO text; the rest `repr`).
pub fn py_str(value: &ConfigValue) -> String {
    match value {
        ConfigValue::Str(s) => s.clone(),
        ConfigValue::Date(d) => d.isoformat(),
        ConfigValue::DateTime(dt) => dt.isoformat(' '),
        other => py_repr(other),
    }
}

// ---------------------------------------------------------------------------------------------
// json.dumps
// ---------------------------------------------------------------------------------------------

fn json_float(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        float_repr(f)
    }
}

/// The text of the placeholder for the `n`th lossless leaf (see [`json_dumps_lossless`]).
pub fn placeholder_token(n: usize) -> String {
    format!("\u{1}mrsm-lossless:{n}\u{1}")
}

struct JsonWriter<'a> {
    style: JsonStyle,
    ascii: bool,
    table: Option<&'a mut Vec<ConfigValue>>,
}

impl JsonWriter<'_> {
    fn leaf(&mut self, value: &ConfigValue, out: &mut String) -> Result<(), String> {
        match self.table.as_deref_mut() {
            Some(table) => {
                let token = placeholder_token(table.len());
                table.push(value.clone());
                out.push_str(&json_dumps_str(&token, self.ascii));
                Ok(())
            }
            None => Err(format!(
                "Object of type {} is not JSON serializable",
                value.type_name()
            )),
        }
    }

    fn key(&self, key: &ConfigValue) -> Result<String, String> {
        Ok(match key {
            ConfigValue::Str(s) => s.clone(),
            ConfigValue::Null => "null".to_string(),
            ConfigValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            ConfigValue::Int(i) => i.to_string(),
            ConfigValue::BigInt(i) => i.to_string(),
            ConfigValue::Float(f) => json_float(*f),
            other => {
                return Err(format!(
                    "keys must be str, int, float, bool or None, not {}",
                    other.type_name()
                ));
            }
        })
    }

    fn separators(&self) -> (&'static str, &'static str) {
        match self.style {
            JsonStyle::Default => (", ", ": "),
            JsonStyle::Compact => (",", ":"),
            JsonStyle::Indent(_) => (",", ": "),
        }
    }

    fn newline(&self, out: &mut String, level: usize) {
        if let JsonStyle::Indent(n) = self.style {
            out.push('\n');
            out.push_str(&" ".repeat(n * level));
        }
    }

    fn write_map(&mut self, map: &ConfigMap, level: usize, out: &mut String) -> Result<(), String> {
        let (item_sep, key_sep) = self.separators();
        if map.is_empty() {
            out.push_str("{}");
            return Ok(());
        }
        out.push('{');
        for (i, (k, v)) in map.iter().enumerate() {
            if i > 0 {
                out.push_str(item_sep);
            }
            self.newline(out, level + 1);
            out.push_str(&json_dumps_str(k, self.ascii));
            out.push_str(key_sep);
            self.write(v, level + 1, out)?;
        }
        self.newline(out, level);
        out.push('}');
        Ok(())
    }

    fn write(&mut self, value: &ConfigValue, level: usize, out: &mut String) -> Result<(), String> {
        let (item_sep, key_sep) = self.separators();
        match value {
            ConfigValue::Null => out.push_str("null"),
            ConfigValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            ConfigValue::Int(i) => out.push_str(&i.to_string()),
            ConfigValue::BigInt(i) => out.push_str(&i.to_string()),
            ConfigValue::Float(f) => out.push_str(&json_float(*f)),
            ConfigValue::Str(s) => out.push_str(&json_dumps_str(s, self.ascii)),
            ConfigValue::List(items) | ConfigValue::Tuple(items) => {
                if items.is_empty() {
                    out.push_str("[]");
                    return Ok(());
                }
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(item_sep);
                    }
                    self.newline(out, level + 1);
                    self.write(item, level + 1, out)?;
                }
                self.newline(out, level);
                out.push(']');
            }
            ConfigValue::Map(map) => self.write_map(map, level, out)?,
            ConfigValue::Pairs(pairs) => {
                if pairs.is_empty() {
                    out.push_str("{}");
                    return Ok(());
                }
                out.push('{');
                for (i, (k, v)) in pairs.iter().enumerate() {
                    if i > 0 {
                        out.push_str(item_sep);
                    }
                    self.newline(out, level + 1);
                    out.push_str(&json_dumps_str(&self.key(k)?, self.ascii));
                    out.push_str(key_sep);
                    self.write(v, level + 1, out)?;
                }
                self.newline(out, level);
                out.push('}');
            }
            ConfigValue::Bytes(_)
            | ConfigValue::Date(_)
            | ConfigValue::DateTime(_)
            | ConfigValue::Set(_) => self.leaf(value, out)?,
        }
        Ok(())
    }
}

/// Python's `json.dumps(value, ...)`. The error is the `TypeError` message CPython raises for a
/// leaf JSON cannot hold (`Object of type date is not JSON serializable`) or a mapping key it
/// cannot spell.
pub fn json_dumps(
    value: &ConfigValue,
    style: JsonStyle,
    ensure_ascii: bool,
) -> Result<String, String> {
    let mut out = String::new();
    JsonWriter {
        style,
        ascii: ensure_ascii,
        table: None,
    }
    .write(value, 0, &mut out)?;
    Ok(out)
}

/// [`json_dumps`] that writes each `bytes`, `date`, `datetime` and `set` leaf as a placeholder
/// string and records it in `table`; [`restore_placeholders`] undoes it. A mapping key JSON
/// cannot spell is still an error.
pub fn json_dumps_lossless(
    value: &ConfigValue,
    style: JsonStyle,
    ensure_ascii: bool,
    table: &mut Vec<ConfigValue>,
) -> Result<String, String> {
    let mut out = String::new();
    JsonWriter {
        style,
        ascii: ensure_ascii,
        table: Some(table),
    }
    .write(value, 0, &mut out)?;
    Ok(out)
}

/// [`json_dumps_lossless`] of a string-keyed map (no copy into a [`ConfigValue`]).
pub fn json_dumps_map_lossless(
    map: &ConfigMap,
    style: JsonStyle,
    ensure_ascii: bool,
    table: &mut Vec<ConfigValue>,
) -> Result<String, String> {
    let mut out = String::new();
    JsonWriter {
        style,
        ascii: ensure_ascii,
        table: Some(table),
    }
    .write_map(map, 0, &mut out)?;
    Ok(out)
}

/// Replace every string that is exactly a placeholder with the value it stands for.
pub fn restore_placeholders(value: ConfigValue, table: &[ConfigValue]) -> ConfigValue {
    if table.is_empty() {
        return value;
    }
    match value {
        ConfigValue::Str(s) => {
            if s.starts_with('\u{1}') {
                for (n, original) in table.iter().enumerate() {
                    if s == placeholder_token(n) {
                        return original.clone();
                    }
                }
            }
            ConfigValue::Str(s)
        }
        ConfigValue::List(items) => ConfigValue::List(
            items
                .into_iter()
                .map(|v| restore_placeholders(v, table))
                .collect(),
        ),
        ConfigValue::Tuple(items) => ConfigValue::Tuple(
            items
                .into_iter()
                .map(|v| restore_placeholders(v, table))
                .collect(),
        ),
        ConfigValue::Map(m) => ConfigValue::Map(
            m.into_iter()
                .map(|(k, v)| (k, restore_placeholders(v, table)))
                .collect(),
        ),
        ConfigValue::Pairs(p) => ConfigValue::Pairs(
            p.into_iter()
                .map(|(k, v)| (k, restore_placeholders(v, table)))
                .collect(),
        ),
        other => other,
    }
}

// ---------------------------------------------------------------------------------------------
// json.loads
// ---------------------------------------------------------------------------------------------

/// A `json.JSONDecodeError`: [`message`](Self::message) is `str(exception)`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct JsonDecodeError {
    /// `exception.msg`.
    pub msg: String,
    /// `exception.pos` (a character index).
    pub pos: usize,
    /// `exception.lineno`.
    pub lineno: usize,
    /// `exception.colno`.
    pub colno: usize,
    /// `str(exception)`: `"{msg}: line {lineno} column {colno} (char {pos})"`.
    pub message: String,
    /// `exception.doc`: the text that was being decoded.
    pub doc: String,
}

impl JsonDecodeError {
    fn new(msg: &str, pos: usize, chars: &[char]) -> JsonDecodeError {
        let pos = pos.min(chars.len());
        let before = &chars[..pos];
        let lineno = before.iter().filter(|c| **c == '\n').count() + 1;
        let colno = match before.iter().rposition(|c| *c == '\n') {
            Some(last) => pos - last,
            None => pos + 1,
        };
        JsonDecodeError {
            msg: msg.to_string(),
            pos,
            lineno,
            colno,
            message: format!("{msg}: line {lineno} column {colno} (char {pos})"),
            doc: chars.iter().collect(),
        }
    }
}

const JSON_MAX_DEPTH: usize = 900;

struct JsonParser<'a> {
    chars: &'a [char],
}

/// A failure inside the scanner: either a decode error, or `StopIteration(idx)` ("Expecting value").
type JsonResult<T> = Result<T, JsonDecodeError>;

impl<'a> JsonParser<'a> {
    fn err<T>(&self, msg: &str, pos: usize) -> JsonResult<T> {
        Err(JsonDecodeError::new(msg, pos, self.chars))
    }

    fn at(&self, idx: usize) -> Option<char> {
        self.chars.get(idx).copied()
    }

    fn skip_ws(&self, mut idx: usize) -> usize {
        while matches!(self.at(idx), Some(' ' | '\t' | '\n' | '\r')) {
            idx += 1;
        }
        idx
    }

    fn matches(&self, idx: usize, text: &str) -> bool {
        text.chars()
            .enumerate()
            .all(|(i, c)| self.at(idx + i) == Some(c))
    }

    /// `scan_once(s, idx)`.
    fn value(&self, idx: usize, depth: usize) -> JsonResult<(ConfigValue, usize)> {
        if depth > JSON_MAX_DEPTH {
            return self.err(
                "maximum recursion depth exceeded while decoding a JSON document",
                idx,
            );
        }
        let Some(c) = self.at(idx) else {
            return self.err("Expecting value", idx);
        };
        match c {
            '"' => {
                let (s, end) = self.string(idx + 1)?;
                Ok((ConfigValue::Str(s), end))
            }
            '{' => self.object(idx + 1, depth),
            '[' => self.array(idx + 1, depth),
            'n' if self.matches(idx, "null") => Ok((ConfigValue::Null, idx + 4)),
            't' if self.matches(idx, "true") => Ok((ConfigValue::Bool(true), idx + 4)),
            'f' if self.matches(idx, "false") => Ok((ConfigValue::Bool(false), idx + 5)),
            'N' if self.matches(idx, "NaN") => Ok((ConfigValue::Float(f64::NAN), idx + 3)),
            'I' if self.matches(idx, "Infinity") => {
                Ok((ConfigValue::Float(f64::INFINITY), idx + 8))
            }
            '-' if self.matches(idx, "-Infinity") => {
                Ok((ConfigValue::Float(f64::NEG_INFINITY), idx + 9))
            }
            _ => self.number(idx),
        }
    }

    fn number(&self, start: usize) -> JsonResult<(ConfigValue, usize)> {
        let mut idx = start;
        if self.at(idx) == Some('-') {
            idx += 1;
        }
        match self.at(idx) {
            Some('1'..='9') => {
                while matches!(self.at(idx), Some('0'..='9')) {
                    idx += 1;
                }
            }
            Some('0') => idx += 1,
            _ => return self.err("Expecting value", start),
        }
        let mut is_float = false;
        if self.at(idx) == Some('.') && matches!(self.at(idx + 1), Some('0'..='9')) {
            idx += 1;
            while matches!(self.at(idx), Some('0'..='9')) {
                idx += 1;
            }
            is_float = true;
        }
        if matches!(self.at(idx), Some('e' | 'E')) {
            let mut probe = idx + 1;
            if matches!(self.at(probe), Some('+' | '-')) {
                probe += 1;
            }
            if matches!(self.at(probe), Some('0'..='9')) {
                while matches!(self.at(probe), Some('0'..='9')) {
                    probe += 1;
                }
                idx = probe;
                is_float = true;
            }
        }
        let text: String = self.chars[start..idx].iter().collect();
        if is_float {
            let f = text.parse::<f64>().unwrap_or(f64::NAN);
            return Ok((ConfigValue::Float(f), idx));
        }
        let big = text.parse::<BigInt>().unwrap_or_default();
        Ok((
            match big.to_i64() {
                Some(n) => ConfigValue::Int(n),
                None => ConfigValue::BigInt(big),
            },
            idx,
        ))
    }

    /// `scanstring(s, end)`: `end` is the index just after the opening quote.
    fn string(&self, end: usize) -> JsonResult<(String, usize)> {
        let begin = end - 1;
        let mut out = String::new();
        let mut idx = end;
        loop {
            let Some(c) = self.at(idx) else {
                return self.err("Unterminated string starting at", begin);
            };
            match c {
                '"' => return Ok((out, idx + 1)),
                '\\' => {
                    let Some(esc) = self.at(idx + 1) else {
                        return self.err("Unterminated string starting at", begin);
                    };
                    match esc {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let (unit, next) = self.hex4(idx + 2)?;
                            let mut next = next;
                            let mut code = u32::from(unit);
                            if (0xD800..0xDC00).contains(&unit)
                                && self.at(next) == Some('\\')
                                && self.at(next + 1) == Some('u')
                            {
                                let (low, after) = self.hex4(next + 2)?;
                                if (0xDC00..0xE000).contains(&low) {
                                    code = 0x10000
                                        + ((u32::from(unit) - 0xD800) << 10)
                                        + (u32::from(low) - 0xDC00);
                                    next = after;
                                }
                            }
                            // PARITY (G2): a lone surrogate cannot live in a Rust `String`.
                            out.push(char::from_u32(code).unwrap_or('\u{FFFD}'));
                            idx = next;
                            continue;
                        }
                        _ => return self.err("Invalid \\escape", idx),
                    }
                    idx += 2;
                }
                c if (c as u32) < 0x20 => return self.err("Invalid control character at", idx),
                c => {
                    out.push(c);
                    idx += 1;
                }
            }
        }
    }

    /// Four hex digits at `at` (just after the `u`; errors point at the `u`).
    fn hex4(&self, at: usize) -> JsonResult<(u16, usize)> {
        if at + 4 > self.chars.len() {
            return self.err("Invalid \\uXXXX escape", at - 1);
        }
        let mut code: u32 = 0;
        for i in 0..4 {
            let Some(d) = self.at(at + i).and_then(|c| c.to_digit(16)) else {
                return self.err("Invalid \\uXXXX escape", at - 1);
            };
            code = (code << 4) | d;
        }
        Ok((code as u16, at + 4))
    }

    fn object(&self, mut idx: usize, depth: usize) -> JsonResult<(ConfigValue, usize)> {
        let mut map = ConfigMap::new();
        idx = self.skip_ws(idx);
        if self.at(idx) != Some('}') {
            loop {
                if self.at(idx) != Some('"') {
                    return self.err("Expecting property name enclosed in double quotes", idx);
                }
                let (key, next) = self.string(idx + 1)?;
                idx = self.skip_ws(next);
                if self.at(idx) != Some(':') {
                    return self.err("Expecting ':' delimiter", idx);
                }
                idx = self.skip_ws(idx + 1);
                let (value, next) = self.value(idx, depth + 1)?;
                map.insert(key, value);
                idx = self.skip_ws(next);
                if self.at(idx) == Some('}') {
                    break;
                }
                if self.at(idx) != Some(',') {
                    return self.err("Expecting ',' delimiter", idx);
                }
                let comma = idx;
                idx = self.skip_ws(idx + 1);
                if self.at(idx) == Some('}') {
                    return self.err("Illegal trailing comma before end of object", comma);
                }
            }
        }
        Ok((ConfigValue::Map(map), idx + 1))
    }

    fn array(&self, mut idx: usize, depth: usize) -> JsonResult<(ConfigValue, usize)> {
        let mut items = Vec::new();
        idx = self.skip_ws(idx);
        if self.at(idx) != Some(']') {
            loop {
                let (value, next) = self.value(idx, depth + 1)?;
                items.push(value);
                idx = self.skip_ws(next);
                if self.at(idx) == Some(']') {
                    break;
                }
                if self.at(idx) != Some(',') {
                    return self.err("Expecting ',' delimiter", idx);
                }
                let comma = idx;
                idx = self.skip_ws(idx + 1);
                if self.at(idx) == Some(']') {
                    return self.err("Illegal trailing comma before end of array", comma);
                }
            }
        }
        Ok((ConfigValue::List(items), idx + 1))
    }
}

/// Python's `json.loads(text)` / `json.load(file)`: integers of any size, `NaN` / `Infinity`,
/// duplicate keys keep their first position and take the last value, and the errors are
/// `json.JSONDecodeError`'s.
pub fn json_loads(text: &str) -> Result<ConfigValue, JsonDecodeError> {
    let chars: Vec<char> = text.chars().collect();
    let parser = JsonParser { chars: &chars };
    let start = parser.skip_ws(0);
    let (value, end) = parser.value(start, 0)?;
    let end = parser.skip_ws(end);
    if end != chars.len() {
        return parser.err("Extra data", end);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cv(json: &str) -> ConfigValue {
        json_loads(json).unwrap()
    }

    #[test]
    fn json_round_trip_keeps_big_ints_and_order() {
        let v = cv(r#"{"b": 1, "a": 123456789012345678901234567890, "c": [1.5, null, true]}"#);
        let text = json_dumps(&v, JsonStyle::Compact, true).unwrap();
        assert_eq!(
            text,
            r#"{"b":1,"a":123456789012345678901234567890,"c":[1.5,null,true]}"#
        );
        assert_eq!(json_loads(&text).unwrap(), v);
    }

    #[test]
    fn duplicate_keys_keep_first_position_and_last_value() {
        let v = cv(r#"{"a": 1, "b": 2, "a": 3}"#);
        let m = v.as_map().unwrap();
        assert_eq!(m.keys().collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(m["a"], ConfigValue::Int(3));
    }

    #[test]
    fn strict_dump_rejects_what_json_rejects() {
        let date = ConfigValue::Date(Date {
            year: 2024,
            month: 1,
            day: 1,
        });
        let mut m = ConfigMap::new();
        m.insert("when".into(), date.clone());
        assert_eq!(
            json_dumps(&ConfigValue::Map(m), JsonStyle::Indent(2), true).unwrap_err(),
            "Object of type date is not JSON serializable"
        );
        let keyed = ConfigValue::from_pairs(vec![(ConfigValue::Tuple(vec![]), ConfigValue::Null)]);
        assert_eq!(
            json_dumps(&keyed, JsonStyle::Default, true).unwrap_err(),
            "keys must be str, int, float, bool or None, not tuple"
        );
        // int / bool / None keys are spelled the way `json.dumps` spells them.
        let keyed = ConfigValue::from_pairs(vec![
            (ConfigValue::Int(1), ConfigValue::Int(1)),
            (ConfigValue::Null, ConfigValue::Int(2)),
            (ConfigValue::Str("x".into()), ConfigValue::Int(3)),
        ]);
        assert_eq!(
            json_dumps(&keyed, JsonStyle::Compact, true).unwrap(),
            r#"{"1":1,"null":2,"x":3}"#
        );
    }

    #[test]
    fn placeholders_carry_dates_through_a_json_text() {
        let mut m = ConfigMap::new();
        m.insert(
            "when".into(),
            ConfigValue::Date(Date {
                year: 2024,
                month: 1,
                day: 1,
            }),
        );
        m.insert("raw".into(), ConfigValue::Bytes(b"xyz".to_vec()));
        let original = ConfigValue::Map(m);
        let mut table = Vec::new();
        let text = json_dumps_lossless(&original, JsonStyle::Compact, true, &mut table).unwrap();
        assert!(!text.contains("2024"));
        let back = restore_placeholders(json_loads(&text).unwrap(), &table);
        assert_eq!(back, original);
    }

    #[test]
    fn patch_merges_nested_dicts_and_replaces_scalars() {
        let a = cv(r#"{"x": {"a": 1, "b": 2}, "y": 1}"#);
        let b = cv(r#"{"x": {"b": 3, "c": 4}, "y": {"z": 1}}"#);
        assert_eq!(
            apply_patch(&a, &b),
            cv(r#"{"x": {"a": 1, "b": 3, "c": 4}, "y": {"z": 1}}"#)
        );
        assert_eq!(apply_patch(&a, &ConfigValue::new_map()), a);
        assert_eq!(apply_patch(&ConfigValue::new_map(), &b), b);
    }

    #[test]
    fn typed_keys_survive_a_patch() {
        let a = ConfigValue::from_pairs(vec![
            (ConfigValue::Int(1), ConfigValue::Str("one".into())),
            (ConfigValue::Str("k".into()), ConfigValue::Int(1)),
        ]);
        let b = ConfigValue::from_pairs(vec![
            // `True == 1`: it patches the entry written under `1`.
            (ConfigValue::Bool(true), ConfigValue::Str("uno".into())),
            (ConfigValue::Int(2), ConfigValue::Str("two".into())),
        ]);
        let ConfigValue::Pairs(out) = apply_patch(&a, &b) else {
            panic!("pairs")
        };
        assert_eq!(out.len(), 3);
        assert_eq!(
            out[0],
            (ConfigValue::Int(1), ConfigValue::Str("uno".into()))
        );
    }

    #[test]
    fn repr_and_str_of_exotic_values() {
        let date = ConfigValue::Date(Date {
            year: 2024,
            month: 1,
            day: 1,
        });
        assert_eq!(py_str(&date), "2024-01-01");
        assert_eq!(py_repr(&date), "datetime.date(2024, 1, 1)");
        let dt = ConfigValue::DateTime(DateTime {
            date: Date {
                year: 2024,
                month: 1,
                day: 1,
            },
            hour: 12,
            minute: 30,
            second: 0,
            microsecond: 0,
            utc_offset_us: Some(0),
        });
        assert_eq!(py_str(&dt), "2024-01-01 12:30:00+00:00");
        assert_eq!(
            py_repr(&dt),
            "datetime.datetime(2024, 1, 1, 12, 30, tzinfo=datetime.timezone.utc)"
        );
        assert_eq!(
            py_repr(&ConfigValue::Tuple(vec![ConfigValue::Int(1)])),
            "(1,)"
        );
        assert_eq!(py_repr(&ConfigValue::Set(vec![])), "set()");
    }

    /// Messages recorded from CPython 3.14.8 (`json.loads`), the interpreter of the reference.
    #[test]
    fn json_errors_read_like_cpython() {
        let cases: &[(&str, &str)] = &[
            ("", "Expecting value: line 1 column 1 (char 0)"),
            ("{\"a\": ", "Expecting value: line 1 column 7 (char 6)"),
            (
                "{",
                "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
            ),
            (
                "{'a': 1}",
                "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
            ),
            (
                "{\"a\" 1}",
                "Expecting ':' delimiter: line 1 column 6 (char 5)",
            ),
            (
                "{\"a\": 1 \"b\": 2}",
                "Expecting ',' delimiter: line 1 column 9 (char 8)",
            ),
            (
                "{\"a\": 1,}",
                "Illegal trailing comma before end of object: line 1 column 8 (char 7)",
            ),
            (
                "[1, 2,]",
                "Illegal trailing comma before end of array: line 1 column 6 (char 5)",
            ),
            ("[1 2]", "Expecting ',' delimiter: line 1 column 4 (char 3)"),
            ("[1", "Expecting ',' delimiter: line 1 column 3 (char 2)"),
            ("[", "Expecting value: line 1 column 2 (char 1)"),
            ("{} x", "Extra data: line 1 column 4 (char 3)"),
            ("01", "Extra data: line 1 column 2 (char 1)"),
            (
                "\"abc",
                "Unterminated string starting at: line 1 column 1 (char 0)",
            ),
            (
                "\"a\nb\"",
                "Invalid control character at: line 1 column 3 (char 2)",
            ),
            ("\"a\\xb\"", "Invalid \\escape: line 1 column 3 (char 2)"),
            (
                "\"a\\u12\"",
                "Invalid \\uXXXX escape: line 1 column 4 (char 3)",
            ),
            (
                "\"a\\u12zz\"",
                "Invalid \\uXXXX escape: line 1 column 4 (char 3)",
            ),
            (
                "{\n  \"a\": nul\n}",
                "Expecting value: line 2 column 8 (char 9)",
            ),
            ("-", "Expecting value: line 1 column 1 (char 0)"),
            ("tru", "Expecting value: line 1 column 1 (char 0)"),
            (
                "\"\\u12",
                "Invalid \\uXXXX escape: line 1 column 3 (char 2)",
            ),
            (
                "\"\\u1234",
                "Unterminated string starting at: line 1 column 1 (char 0)",
            ),
            (
                "[1,\n 2,\n ]",
                "Illegal trailing comma before end of array: line 2 column 3 (char 6)",
            ),
            (
                "{\"a\":1,\n}",
                "Illegal trailing comma before end of object: line 1 column 7 (char 6)",
            ),
            (
                "\"abc\\",
                "Unterminated string starting at: line 1 column 1 (char 0)",
            ),
            (
                "{\"a\": [1, 2}",
                "Expecting ',' delimiter: line 1 column 12 (char 11)",
            ),
            ("{\"a\": tru}", "Expecting value: line 1 column 7 (char 6)"),
            ("  ", "Expecting value: line 1 column 3 (char 2)"),
            ("1 2", "Extra data: line 1 column 3 (char 2)"),
            ("[1,,2]", "Expecting value: line 1 column 4 (char 3)"),
            (
                "{,}",
                "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
            ),
            (
                "{\"a\":1,,}",
                "Expecting property name enclosed in double quotes: line 1 column 8 (char 7)",
            ),
            (
                "\"a\\u00\"x",
                "Invalid \\uXXXX escape: line 1 column 4 (char 3)",
            ),
        ];
        for (text, expected) in cases {
            let err = json_loads(text).unwrap_err();
            assert_eq!(&err.message, expected, "{text:?}");
        }
    }

    #[test]
    fn json_numbers_and_constants() {
        assert_eq!(cv("-0"), ConfigValue::Int(0));
        assert_eq!(cv("1e2"), ConfigValue::Float(100.0));
        assert_eq!(
            cv("[NaN]").to_value().to_json_string(),
            "[NaN]".replace("NaN", "null")
        );
        assert_eq!(cv("-Infinity"), ConfigValue::Float(f64::NEG_INFINITY));
        assert_eq!(
            cv("\"\\ud83d\\ude00\""),
            ConfigValue::Str("\u{1F600}".into())
        );
    }
}
