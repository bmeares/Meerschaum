//! PyYAML-compatible YAML loading and dumping for configuration files and pipe parameters.
//!
//! Meerschaum reads YAML with PyYAML's `yaml.load(..., Loader=yaml.Loader)`, which implements
//! YAML **1.1** implicit typing: `yes`/`no`/`on`/`off` are booleans, `017` is octal, `1e5` is a
//! string (floats need a `.`), `190:20:30` is a base-60 integer, and `<<` merge keys are honored.
//! [`load`] reproduces those rules on top of the `yaml-rust2` event parser (which reports the
//! scalar style, so quoted scalars stay strings).
//!
//! [`dump`] emits block-style YAML shaped like Meerschaum's `yaml.dump(..., sort_keys=False)`
//! (two-space indentation, unindented sequences, a blank line between top-level keys, literal
//! blocks for multi-line strings). It round-trips through [`load`] exactly; byte-for-byte equality
//! with PyYAML is not guaranteed for unusual strings.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;
use yaml_rust2::parser::{Event, MarkedEventReceiver, Parser, Tag};
use yaml_rust2::scanner::{Marker, TScalarStyle};

use crate::pyfmt::{float_repr, json_key};
use crate::value::{Map, Value};

/// A YAML loading error.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("YAML error: {0}")]
pub struct YamlError(pub String);

static BOOL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$")
        .expect("valid regex")
});
static FLOAT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?|\.[0-9_]+(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$",
    )
    .expect("valid regex")
});
static INT_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$",
    )
    .expect("valid regex")
});
static NULL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(?:~|null|Null|NULL|)$").expect("valid regex"));
static TIMESTAMP_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)$",
    )
    .expect("valid regex")
});

/// The type PyYAML's implicit resolver assigns to a plain scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Implicit {
    Null,
    Bool,
    Int,
    Float,
    Timestamp,
    Str,
}

fn resolve_plain(s: &str) -> Implicit {
    if NULL_RE.is_match(s) {
        Implicit::Null
    } else if BOOL_RE.is_match(s) {
        Implicit::Bool
    } else if INT_RE.is_match(s) {
        Implicit::Int
    } else if FLOAT_RE.is_match(s) {
        Implicit::Float
    } else if TIMESTAMP_RE.is_match(s) {
        Implicit::Timestamp
    } else {
        Implicit::Str
    }
}

/// `SafeConstructor.construct_yaml_int`.
fn construct_int(s: &str) -> Option<i64> {
    let mut v: String = s.chars().filter(|c| *c != '_').collect();
    let mut sign = 1i64;
    if v.starts_with('-') {
        sign = -1;
        v.remove(0);
    } else if v.starts_with('+') {
        v.remove(0);
    }
    if v == "0" {
        return Some(0);
    }
    let n = if let Some(b) = v.strip_prefix("0b") {
        i64::from_str_radix(b, 2).ok()?
    } else if let Some(h) = v.strip_prefix("0x") {
        i64::from_str_radix(h, 16).ok()?
    } else if v.starts_with('0') {
        i64::from_str_radix(&v, 8).ok()?
    } else if v.contains(':') {
        let mut base = 1i64;
        let mut total = 0i64;
        for digit in v.split(':').rev() {
            total += digit.parse::<i64>().ok()? * base;
            base *= 60;
        }
        total
    } else {
        v.parse::<i64>().ok()?
    };
    Some(sign * n)
}

/// `SafeConstructor.construct_yaml_float`.
fn construct_float(s: &str) -> Option<f64> {
    let mut v: String = s
        .chars()
        .filter(|c| *c != '_')
        .collect::<String>()
        .to_lowercase();
    let mut sign = 1.0;
    if v.starts_with('-') {
        sign = -1.0;
        v.remove(0);
    } else if v.starts_with('+') {
        v.remove(0);
    }
    if v == ".inf" {
        return Some(sign * f64::INFINITY);
    }
    if v == ".nan" {
        return Some(f64::NAN);
    }
    if v.contains(':') {
        let mut base = 1.0;
        let mut total = 0.0;
        for digit in v.split(':').rev() {
            total += digit.parse::<f64>().ok()? * base;
            base *= 60.0;
        }
        return Some(sign * total);
    }
    v.parse::<f64>().ok().map(|f| sign * f)
}

fn construct_scalar(value: String, style: TScalarStyle, tag: Option<&Tag>) -> Value {
    if let Some(tag) = tag {
        if tag.handle == "!!" || tag.handle == "tag:yaml.org,2002:" {
            match tag.suffix.as_str() {
                "str" => return Value::Str(value),
                "null" => return Value::Null,
                "bool" => {
                    return Value::Bool(matches!(
                        value.to_lowercase().as_str(),
                        "yes" | "true" | "on"
                    ));
                }
                "int" => return construct_int(&value).map_or(Value::Str(value), Value::Int),
                "float" => return construct_float(&value).map_or(Value::Str(value), Value::Float),
                _ => {}
            }
        }
    }
    if style != TScalarStyle::Plain {
        return Value::Str(value);
    }
    match resolve_plain(&value) {
        Implicit::Null => Value::Null,
        Implicit::Bool => Value::Bool(matches!(
            value.to_lowercase().as_str(),
            "yes" | "true" | "on"
        )),
        Implicit::Int => construct_int(&value).map_or(Value::Str(value), Value::Int),
        Implicit::Float => construct_float(&value).map_or(Value::Str(value), Value::Float),
        // PARITY: PyYAML constructs `datetime`/`date` objects; they are kept as strings here.
        Implicit::Timestamp | Implicit::Str => Value::Str(value),
    }
}

enum Frame {
    Seq(Vec<Value>, usize),
    Map {
        entries: Vec<(Value, Value)>,
        key: Option<Value>,
        anchor: usize,
        merges: Vec<Value>,
        pending_merge: bool,
    },
}

#[derive(Default)]
struct Loader {
    stack: Vec<Frame>,
    docs: Vec<Value>,
    anchors: HashMap<usize, Value>,
    error: Option<String>,
}

impl Loader {
    fn push_value(&mut self, value: Value, anchor: usize) {
        if anchor > 0 {
            self.anchors.insert(anchor, value.clone());
        }
        match self.stack.last_mut() {
            None => self.docs.push(value),
            Some(Frame::Seq(items, _)) => items.push(value),
            Some(Frame::Map {
                entries,
                key,
                merges,
                pending_merge,
                ..
            }) => {
                if *pending_merge {
                    *pending_merge = false;
                    merges.push(value);
                } else if let Some(k) = key.take() {
                    entries.push((k, value));
                } else {
                    // A `<<` key is only a merge key when it is a plain scalar.
                    if value == Value::Str("<<".to_string()) {
                        *pending_merge = true;
                        *key = None;
                        return;
                    }
                    *key = Some(value);
                }
            }
        }
    }

    fn finish_map(
        &mut self,
        entries: Vec<(Value, Value)>,
        merges: Vec<Value>,
    ) -> Result<Value, String> {
        // PyYAML `flatten_mapping`: merged keys come first; the node's own keys override them.
        // With `<<: [*a, *b]`, earlier entries take precedence over later ones.
        let mut merged: Vec<(String, Value)> = Vec::new();
        for m in merges {
            let sources: Vec<Value> = match m {
                Value::Map(_) => vec![m],
                Value::List(items) => items,
                _ => return Err("expected a mapping or list of mappings for merging".into()),
            };
            for source in sources.into_iter().rev() {
                match source {
                    Value::Map(sm) => merged.extend(sm),
                    _ => return Err("expected a mapping for merging".into()),
                }
            }
        }
        let mut out = Map::new();
        for (k, v) in merged {
            out.insert(k, v);
        }
        for (k, v) in entries {
            let key = match k {
                Value::Str(s) => s,
                Value::List(_) | Value::Map(_) => {
                    return Err("found unhashable key".into());
                }
                // PARITY: Python keeps non-string keys (`1: x`); they are stringified here the
                // way `json.dumps` would write them.
                other => json_key(&other),
            };
            out.insert(key, v);
        }
        Ok(Value::Map(out))
    }
}

impl MarkedEventReceiver for Loader {
    fn on_event(&mut self, ev: Event, _mark: Marker) {
        if self.error.is_some() {
            return;
        }
        match ev {
            Event::Scalar(value, style, anchor, tag) => {
                let v = construct_scalar(value, style, tag.as_ref());
                self.push_value(v, anchor);
            }
            Event::Alias(id) => match self.anchors.get(&id).cloned() {
                Some(v) => self.push_value(v, 0),
                None => self.error = Some(format!("found undefined alias {id}")),
            },
            Event::SequenceStart(anchor, _) => self.stack.push(Frame::Seq(Vec::new(), anchor)),
            Event::SequenceEnd => {
                if let Some(Frame::Seq(items, anchor)) = self.stack.pop() {
                    self.push_value(Value::List(items), anchor);
                }
            }
            Event::MappingStart(anchor, _) => self.stack.push(Frame::Map {
                entries: Vec::new(),
                key: None,
                anchor,
                merges: Vec::new(),
                pending_merge: false,
            }),
            Event::MappingEnd => {
                if let Some(Frame::Map {
                    entries,
                    anchor,
                    merges,
                    ..
                }) = self.stack.pop()
                {
                    match self.finish_map(entries, merges) {
                        Ok(v) => self.push_value(v, anchor),
                        Err(e) => self.error = Some(e),
                    }
                }
            }
            _ => {}
        }
    }
}

/// Load a single YAML document like PyYAML's `yaml.load(text, Loader=yaml.Loader)`.
///
/// An empty document is `None`. More than one document is an error (PyYAML raises
/// `ComposerError`).
pub fn load(text: &str) -> Result<Value, YamlError> {
    let mut loader = Loader::default();
    let mut parser = Parser::new_from_str(text);
    parser
        .load(&mut loader, true)
        .map_err(|e| YamlError(e.to_string()))?;
    if let Some(e) = loader.error {
        return Err(YamlError(e));
    }
    match loader.docs.len() {
        0 => Ok(Value::Null),
        1 => Ok(loader.docs.pop().unwrap_or(Value::Null)),
        _ => Err(YamlError(
            "expected a single document in the stream".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------------------------
// Dumping
// ---------------------------------------------------------------------------------------------

/// Dump a value as block-style YAML (see the module docs).
pub fn dump(value: &Value) -> String {
    let mut out = String::new();
    match value {
        Value::Map(m) if !m.is_empty() => {
            let mut first = true;
            let mut prev_block_scalar = false;
            for (k, v) in m {
                if !first && !prev_block_scalar {
                    // Meerschaum's `CustomDumper` doubles line breaks at the top level.
                    out.push('\n');
                }
                first = false;
                prev_block_scalar = matches!(v, Value::Str(s) if s.contains('\n'));
                emit_entry(k, v, 0, &mut out);
            }
        }
        Value::List(l) if !l.is_empty() => {
            for item in l {
                emit_item(item, 0, &mut out);
            }
        }
        other => {
            out.push_str(&scalar(other, 0));
            out.push('\n');
        }
    }
    out
}

fn emit_entry(key: &str, value: &Value, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str(&scalar(&Value::Str(key.to_string()), indent));
    out.push(':');
    match value {
        Value::Map(m) if !m.is_empty() => {
            out.push('\n');
            for (k, v) in m {
                emit_entry(k, v, indent + 2, out);
            }
        }
        Value::List(l) if !l.is_empty() => {
            out.push('\n');
            for item in l {
                emit_item(item, indent, out);
            }
        }
        other => {
            out.push(' ');
            out.push_str(&scalar(other, indent + 2));
            out.push('\n');
        }
    }
}

fn emit_item(value: &Value, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push('-');
    match value {
        Value::Map(m) if !m.is_empty() => {
            let mut first = true;
            for (k, v) in m {
                if first {
                    out.push(' ');
                    let mut entry = String::new();
                    emit_entry(k, v, indent + 2, &mut entry);
                    out.push_str(entry.trim_start_matches(' '));
                    first = false;
                } else {
                    emit_entry(k, v, indent + 2, out);
                }
            }
        }
        Value::List(l) if !l.is_empty() => {
            out.push(' ');
            let mut nested = String::new();
            for item in l {
                emit_item(item, indent + 2, &mut nested);
            }
            out.push_str(nested.trim_start_matches(' '));
        }
        other => {
            out.push(' ');
            out.push_str(&scalar(other, indent + 2));
            out.push('\n');
        }
    }
}

fn scalar(value: &Value, indent: usize) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_nan() {
                ".nan".to_string()
            } else if f.is_infinite() {
                if *f > 0.0 { ".inf" } else { "-.inf" }.to_string()
            } else {
                let r = float_repr(*f);
                // PyYAML writes `1e+16` as `1.0e+16` so that it resolves as a float.
                if r.contains('e') && !r.contains('.') {
                    r.replacen('e', ".0e", 1)
                } else {
                    r
                }
            }
        }
        Value::Map(_) => "{}".to_string(),
        Value::List(_) => "[]".to_string(),
        Value::Str(s) => string_scalar(s, indent),
    }
}

fn string_scalar(s: &str, indent: usize) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    let needs_double = s
        .chars()
        .any(|c| (c.is_control() && c != '\n') || !c.is_ascii() || c == '\u{feff}');
    if s.contains('\n') && !needs_double {
        let chomp = if !s.ends_with('\n') {
            "-"
        } else if s.ends_with("\n\n") {
            "+"
        } else {
            ""
        };
        let pad = " ".repeat(indent);
        let mut out = format!("|{chomp}\n");
        let body = s.strip_suffix('\n').unwrap_or(s);
        for line in body.split('\n') {
            if !line.is_empty() {
                out.push_str(&pad);
            }
            out.push_str(line);
            out.push('\n');
        }
        // Drop the final newline: the caller appends one.
        out.pop();
        return out;
    }
    if needs_double {
        let mut out = String::from("\"");
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\t' => out.push_str("\\t"),
                '\r' => out.push_str("\\r"),
                c if (c as u32) < 0x100 && (c.is_control() || !c.is_ascii()) => {
                    out.push_str(&format!("\\x{:02X}", c as u32))
                }
                c if !c.is_ascii() && (c as u32) < 0x10000 => {
                    out.push_str(&format!("\\u{:04X}", c as u32))
                }
                c if !c.is_ascii() => out.push_str(&format!("\\U{:08X}", c as u32)),
                c => out.push(c),
            }
        }
        out.push('"');
        return out;
    }
    if plain_allowed(s) {
        return s.to_string();
    }
    format!("'{}'", s.replace('\'', "''"))
}

fn plain_allowed(s: &str) -> bool {
    if resolve_plain(s) != Implicit::Str {
        return false;
    }
    if s.starts_with(' ') || s.ends_with(' ') || s.ends_with(':') {
        return false;
    }
    let first = s.chars().next().unwrap_or(' ');
    let second = s.chars().nth(1);
    if matches!(
        first,
        '#' | ','
            | '['
            | ']'
            | '{'
            | '}'
            | '&'
            | '*'
            | '!'
            | '|'
            | '>'
            | '\''
            | '"'
            | '%'
            | '@'
            | '`'
    ) {
        return false;
    }
    if matches!(first, '-' | '?' | ':') && second.is_none_or(|c| c == ' ') {
        return false;
    }
    if s.contains(": ") || s.contains(" #") || s.contains('\t') {
        return false;
    }
    if s == "<<" || s == "=" {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yaml_1_1_resolution() {
        let doc = "a: yes\nb: on\nc: 1e5\nd: 1.0e+5\ne: 0o17\nf: 017\ng: 1_000\nh: ~\ni: 2024-01-01\nj: 190:20:30\nk: .inf\nl: 0x1F\nm: \"yes\"\nn: !!str 12\no: [1, 2]\np: &a {x: 1}\nq:\n  <<: *a\n  y: 2\n";
        let v = load(doc).unwrap();
        let expected = Value::from_json_str(
            r#"{"a": true, "b": true, "c": "1e5", "d": 100000.0, "e": "0o17", "f": 15, "g": 1000, "h": null, "i": "2024-01-01", "j": 685230, "l": 31, "m": "yes", "n": "12", "o": [1, 2], "p": {"x": 1}, "q": {"x": 1, "y": 2}}"#,
        )
        .unwrap();
        let mut got = v.as_map().unwrap().clone();
        assert_eq!(got.shift_remove("k"), Some(Value::Float(f64::INFINITY)));
        assert_eq!(Value::Map(got), expected);
    }

    #[test]
    fn dump_round_trip() {
        let v = Value::from_json_str(
            r#"{"a": 1, "b": {"c": "x\ny", "d": ["yes", "", " lead", "a: b", 1.5, null, {"e": [1]}], "f": {}, "g": []}, "h": "é", "i": "'q'"}"#,
        )
        .unwrap();
        let text = dump(&v);
        assert_eq!(load(&text).unwrap(), v, "{text}");
        assert!(text.starts_with("a: 1\n\nb:\n  c: |-\n    x\n    y\n  d:\n  - 'yes'\n"));
    }

    #[test]
    fn empty_and_multi() {
        assert_eq!(load("").unwrap(), Value::Null);
        assert!(load("a: 1\n---\nb: 2\n").is_err());
    }
}
