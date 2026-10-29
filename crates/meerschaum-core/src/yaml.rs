//! PyYAML-compatible YAML for configuration files and pipe parameters.
//!
//! Meerschaum reads YAML with PyYAML's `yaml.load(..., Loader=yaml.Loader)` and writes it with
//! `yaml.dump(..., sort_keys=False)` through a `Dumper` subclass. This module reproduces both
//! on top of a port of PyYAML's own reader, scanner, parser, composer, constructor,
//! representer, serializer and emitter, so YAML 1.1 implicit typing (`yes`/`no`/`on`/`off` are
//! booleans, `017` is octal, `1e5` is a string, `190:20:30` is base 60), merge keys, tags and
//! the error messages behave the way users see them from Python.
//!
//! Two layers:
//!
//! - [`node`], [`loader`] and [`dumper`] are the **lossless** layer: a [`node::Node`] keeps
//!   ordered mappings with typed keys, arbitrary-size integers, `date` / `datetime` (naive and
//!   offset, microseconds), `bytes`, tuples and sets, and shares aliased nodes. Python
//!   boundaries (`meerschaum._core`) build real Python objects from it.
//! - [`load`] and [`dump`] below are the JSON-like projection over [`Value`] for Rust consumers
//!   whose data model is JSON (config registry, compose files). The projection is **lossy by
//!   design**: timestamps stay strings, mapping keys become strings, integers beyond `i64`
//!   become strings. Never route a public YAML value through it.

use crate::value::Value;

pub mod dumper;
mod emitter;
pub mod loader;
pub mod node;
pub mod parser;
pub mod scanner;

use dumper::{DumpOptions, dump_node, node_from_value, value_from_node};
use loader::{LoadOptions, load_text};

/// A YAML loading error.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("YAML error: {0}")]
pub struct YamlError(pub String);

/// Load a single YAML document like PyYAML's `yaml.load(text, Loader=yaml.Loader)`, projected
/// into the JSON-like [`Value`] (see the module docs: lossy for dates and non-string keys).
///
/// An empty document is `None`. More than one document is an error (PyYAML raises
/// `ComposerError`).
pub fn load(text: &str) -> Result<Value, YamlError> {
    // PARITY: PyYAML constructs `datetime`/`date` objects and keeps non-string keys; the
    // projection keeps timestamps as strings and stringifies keys the way `json.dumps` would.
    let node = load_text(
        text,
        "<unicode string>",
        true,
        LoadOptions {
            safe: false,
            project: true,
        },
    )
    .map_err(|e| YamlError(e.message))?;
    Ok(value_from_node(&node))
}

/// Dump a value as block-style YAML shaped like Meerschaum's `yaml.dump(..., sort_keys=False)`
/// (two-space indentation, unindented sequences, a blank line between top-level keys, literal
/// blocks for multi-line strings), byte for byte what PyYAML's emitter writes.
///
/// An emitter or representer error is returned, never swallowed: a caller that writes the text
/// to a file must leave the file alone when this fails.
pub fn dump(value: &Value) -> Result<String, YamlError> {
    let opts = DumpOptions {
        sort_keys: false,
        ..DumpOptions::default()
    };
    dump_node(&node_from_value(value), &opts).map_err(|e| YamlError(e.message))
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
        let text = dump(&v).unwrap();
        assert_eq!(load(&text).unwrap(), v, "{text}");
        assert!(text.starts_with("a: 1\n\nb:\n  c: |-\n    x\n    y\n  d:\n  - 'yes'\n"));
    }

    #[test]
    fn empty_and_multi() {
        assert_eq!(load("").unwrap(), Value::Null);
        assert!(load("a: 1\n---\nb: 2\n").is_err());
    }
}
