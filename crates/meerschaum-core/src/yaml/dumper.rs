//! The representer and serializer: a [`Node`] tree to emitter events.
//!
//! This reproduces `yaml.dump(data, Dumper=...)` as Meerschaum calls it (PyYAML's `Dumper`
//! with `sort_keys` / `default_flow_style` options, the multi-line string presenter, and the
//! doubled top-level line breaks). Sharing is by node identity: a container or date that
//! appears twice gets an anchor and an alias, exactly as PyYAML's `id()`-based representer does.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use num_bigint::BigInt;
use num_traits::ToPrimitive;

use super::emitter::{EEvent, Emitter};
use super::node::{
    Node, NodeRef, PyKey, TAG_BINARY, TAG_BOOL, TAG_FLOAT, TAG_INT, TAG_MAP, TAG_NULL,
    TAG_PY_TUPLE, TAG_SEQ, TAG_SET, TAG_STR, TAG_TIMESTAMP, implicit_tag, py_key,
};
use super::scanner::PyYamlError;
use crate::pyfmt::float_repr;
use crate::value::{Map, Value};

/// The options of `yaml.dump` that this implementation honors.
#[derive(Debug, Clone)]
pub struct DumpOptions {
    /// `sort_keys` (PyYAML's default is `True`; Meerschaum passes `False`).
    pub sort_keys: bool,
    /// `default_flow_style` (`None` picks flow style for collections of plain scalars).
    pub default_flow_style: Option<bool>,
    /// `default_style` (`'"'`, `'\''`, `'|'` or `'>'`).
    pub default_style: Option<char>,
    /// `indent`.
    pub indent: Option<usize>,
    /// `width`.
    pub width: Option<usize>,
    /// `allow_unicode`.
    pub allow_unicode: bool,
    /// `line_break`.
    pub line_break: Option<String>,
    /// `explicit_start`.
    pub explicit_start: bool,
    /// `explicit_end`.
    pub explicit_end: bool,
    /// Meerschaum's string presenter: strings with line breaks are written as `|` blocks.
    pub multiline_block_style: bool,
    /// Meerschaum's `CustomDumper`: a blank line between top-level entries.
    pub double_top_level_breaks: bool,
}

impl Default for DumpOptions {
    /// `yaml.dump(data)` as `meerschaum.utils.yaml` runs it.
    fn default() -> Self {
        DumpOptions {
            sort_keys: true,
            default_flow_style: Some(false),
            default_style: None,
            indent: None,
            width: None,
            allow_unicode: false,
            line_break: None,
            explicit_start: false,
            explicit_end: false,
            multiline_block_style: true,
            double_top_level_breaks: true,
        }
    }
}

fn dump_error(message: impl Into<String>) -> PyYamlError {
    PyYamlError {
        kind: super::scanner::ErrorKind::Representer,
        message: message.into(),
    }
}

/// Whether PyYAML would track `node` for aliasing (`ignore_aliases` is false for it).
fn anchorable(node: &Node) -> bool {
    match node {
        Node::Null
        | Node::Bool(_)
        | Node::Int(_)
        | Node::Float(_)
        | Node::Str(_)
        | Node::Bytes(_) => false,
        Node::Tuple(items) => !items.is_empty(),
        _ => true,
    }
}

struct ScalarRepr {
    tag: &'static str,
    value: String,
    style: Option<char>,
}

struct Dumper<'a> {
    opts: &'a DumpOptions,
    emitter: Emitter,
    anchors: HashMap<*const Node, Option<String>>,
    serialized: HashSet<*const Node>,
    last_anchor_id: usize,
}

/// `base64.encodebytes`.
fn encode_base64(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for line in data.chunks(57) {
        for chunk in line.chunks(3) {
            let b = [
                chunk[0],
                chunk.get(1).copied().unwrap_or(0),
                chunk.get(2).copied().unwrap_or(0),
            ];
            let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
            out.push(ALPHABET[(n >> 18) as usize & 63] as char);
            out.push(ALPHABET[(n >> 12) as usize & 63] as char);
            out.push(if chunk.len() > 1 {
                ALPHABET[(n >> 6) as usize & 63] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                ALPHABET[n as usize & 63] as char
            } else {
                '='
            });
        }
        out.push('\n');
    }
    out
}

/// `len(text.splitlines()) > 1`.
fn splitlines_count(text: &str) -> usize {
    let chars: Vec<char> = text.chars().collect();
    let mut count = 0;
    let mut i = 0;
    let mut line_has_content = false;
    while i < chars.len() {
        let c = chars[i];
        if matches!(
            c,
            '\n' | '\r'
                | '\u{b}'
                | '\u{c}'
                | '\u{1c}'
                | '\u{1d}'
                | '\u{1e}'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        ) {
            count += 1;
            line_has_content = false;
            if c == '\r' && chars.get(i + 1) == Some(&'\n') {
                i += 1;
            }
        } else {
            line_has_content = true;
        }
        i += 1;
    }
    if line_has_content {
        count += 1;
    }
    count
}

fn numeric(node: &Node) -> Option<Result<BigInt, f64>> {
    match node {
        Node::Bool(b) => Some(Ok(BigInt::from(i32::from(*b)))),
        Node::Int(i) => Some(Ok(i.clone())),
        Node::Float(f) => Some(Err(*f)),
        _ => None,
    }
}

/// Python's `a < b` ordering for the key types a YAML mapping can hold; `None` is a
/// `TypeError`.
fn py_cmp(a: &Node, b: &Node) -> Option<Ordering> {
    if let (Some(x), Some(y)) = (numeric(a), numeric(b)) {
        return match (x, y) {
            (Ok(x), Ok(y)) => Some(x.cmp(&y)),
            (Ok(x), Err(y)) => x.to_f64()?.partial_cmp(&y),
            (Err(x), Ok(y)) => x.partial_cmp(&y.to_f64()?),
            (Err(x), Err(y)) => x.partial_cmp(&y),
        };
    }
    match (a, b) {
        (Node::Str(x), Node::Str(y)) => Some(x.cmp(y)),
        (Node::Bytes(x), Node::Bytes(y)) => Some(x.cmp(y)),
        (Node::Date(_), Node::Date(_)) | (Node::DateTime(_), Node::DateTime(_)) => {
            let (ka, kb) = (py_key(a)?, py_key(b)?);
            match (ka, kb) {
                (PyKey::Date(x), PyKey::Date(y)) => Some(x.cmp(&y)),
                (PyKey::Naive(x), PyKey::Naive(y)) | (PyKey::Aware(x), PyKey::Aware(y)) => {
                    Some(x.cmp(&y))
                }
                _ => None,
            }
        }
        (Node::Tuple(x), Node::Tuple(y)) => {
            for (i, j) in x.iter().zip(y.iter()) {
                if py_key(i) == py_key(j) && py_key(i).is_some() {
                    continue;
                }
                return py_cmp(i, j);
            }
            Some(x.len().cmp(&y.len()))
        }
        _ => None,
    }
}

/// A stable merge sort with a fallible comparison (Python's `sorted` aborting on `TypeError`).
fn try_sort<T: Clone>(items: Vec<T>, cmp: &dyn Fn(&T, &T) -> Option<Ordering>) -> Option<Vec<T>> {
    if items.len() <= 1 {
        return Some(items);
    }
    let mid = items.len() / 2;
    let right = items[mid..].to_vec();
    let mut left = items;
    left.truncate(mid);
    let left = try_sort(left, cmp)?;
    let right = try_sort(right, cmp)?;
    let mut out = Vec::with_capacity(left.len() + right.len());
    let (mut i, mut j) = (0, 0);
    while i < left.len() && j < right.len() {
        if cmp(&right[j], &left[i])? == Ordering::Less {
            out.push(right[j].clone());
            j += 1;
        } else {
            out.push(left[i].clone());
            i += 1;
        }
    }
    out.extend_from_slice(&left[i..]);
    out.extend_from_slice(&right[j..]);
    Some(out)
}

impl<'a> Dumper<'a> {
    fn scalar_repr(&self, node: &Node) -> Option<ScalarRepr> {
        let default_style = self.opts.default_style;
        Some(match node {
            Node::Null => ScalarRepr {
                tag: TAG_NULL,
                value: "null".to_string(),
                style: default_style,
            },
            Node::Bool(b) => ScalarRepr {
                tag: TAG_BOOL,
                value: if *b { "true" } else { "false" }.to_string(),
                style: default_style,
            },
            Node::Int(i) => ScalarRepr {
                tag: TAG_INT,
                value: i.to_string(),
                style: default_style,
            },
            Node::Float(f) => {
                let value = if f.is_nan() {
                    ".nan".to_string()
                } else if f.is_infinite() {
                    if *f > 0.0 { ".inf" } else { "-.inf" }.to_string()
                } else {
                    let mut v = float_repr(*f).to_lowercase();
                    // `repr(1e17)` is `1e+17`, which does not resolve as a float.
                    if !v.contains('.') && v.contains('e') {
                        v = v.replacen('e', ".0e", 1);
                    }
                    v
                };
                ScalarRepr {
                    tag: TAG_FLOAT,
                    value,
                    style: default_style,
                }
            }
            Node::Str(s) => {
                let block = self.opts.multiline_block_style && splitlines_count(s) > 1;
                ScalarRepr {
                    tag: TAG_STR,
                    value: s.clone(),
                    style: if block { Some('|') } else { default_style },
                }
            }
            Node::Bytes(b) => ScalarRepr {
                tag: TAG_BINARY,
                value: encode_base64(b),
                style: Some('|'),
            },
            Node::Date(d) => ScalarRepr {
                tag: TAG_TIMESTAMP,
                value: d.isoformat(),
                style: default_style,
            },
            Node::DateTime(dt) => ScalarRepr {
                tag: TAG_TIMESTAMP,
                value: dt.isoformat(' '),
                style: default_style,
            },
            _ => return None,
        })
    }

    fn children(node: &Node) -> Vec<&NodeRef> {
        match node {
            Node::Seq(items) | Node::Tuple(items) | Node::Set(items) => items.iter().collect(),
            Node::Map(entries) => entries.iter().flat_map(|(k, v)| [k, v]).collect(),
            _ => Vec::new(),
        }
    }

    fn anchor_node(&mut self, node: &NodeRef) {
        if !anchorable(node) {
            return;
        }
        let key = Rc::as_ptr(node);
        if let Some(slot) = self.anchors.get_mut(&key) {
            if slot.is_none() {
                self.last_anchor_id += 1;
                *slot = Some(format!("id{:03}", self.last_anchor_id));
            }
        } else {
            self.anchors.insert(key, None);
            let children: Vec<NodeRef> = Self::children(node).into_iter().cloned().collect();
            for child in &children {
                self.anchor_node(child);
            }
        }
    }

    /// `represent_mapping`'s `sorted(mapping.items())` (skipped when keys cannot be compared).
    fn ordered(&self, entries: &[(NodeRef, NodeRef)]) -> Vec<(NodeRef, NodeRef)> {
        if self.opts.sort_keys {
            if let Some(sorted) = try_sort(entries.to_vec(), &|a, b| py_cmp(&a.0, &b.0)) {
                return sorted;
            }
        }
        entries.to_vec()
    }

    fn best_flow_style(&self, children: &[NodeRef]) -> bool {
        children
            .iter()
            .all(|c| self.scalar_repr(c).is_some_and(|r| r.style.is_none()))
    }

    fn flow_style(&self, children: &[NodeRef]) -> bool {
        match self.opts.default_flow_style {
            Some(f) => f,
            None => self.best_flow_style(children),
        }
    }

    fn emit(&mut self, event: EEvent) -> Result<(), PyYamlError> {
        self.emitter.emit(event)
    }

    fn serialize_node(&mut self, node: &NodeRef) -> Result<(), PyYamlError> {
        let key = Rc::as_ptr(node);
        let anchor = if anchorable(node) {
            self.anchors.get(&key).cloned().flatten()
        } else {
            None
        };
        if anchorable(node) && self.serialized.contains(&key) {
            return self.emit(EEvent::Alias {
                anchor: anchor.unwrap_or_default(),
            });
        }
        if anchorable(node) {
            self.serialized.insert(key);
        }
        if let Some(repr) = self.scalar_repr(node) {
            let detected = implicit_tag(&repr.value).unwrap_or(TAG_STR);
            let implicit = (repr.tag == detected, repr.tag == TAG_STR);
            return self.emit(EEvent::Scalar {
                anchor,
                tag: Some(repr.tag.to_string()),
                implicit,
                value: repr.value,
                style: repr.style,
            });
        }
        match &**node {
            Node::Seq(items) | Node::Tuple(items) => {
                let is_tuple = matches!(&**node, Node::Tuple(_));
                let tag = if is_tuple { TAG_PY_TUPLE } else { TAG_SEQ };
                let flow_style = self.flow_style(items);
                self.emit(EEvent::SequenceStart {
                    anchor,
                    tag: Some(tag.to_string()),
                    implicit: tag == TAG_SEQ,
                    flow_style,
                })?;
                for item in items {
                    self.serialize_node(item)?;
                }
                self.emit(EEvent::SequenceEnd)
            }
            Node::Map(entries) => {
                let ordered = self.ordered(entries);
                let flat: Vec<NodeRef> = ordered
                    .iter()
                    .flat_map(|(k, v)| [k.clone(), v.clone()])
                    .collect();
                let flow_style = self.flow_style(&flat);
                self.emit(EEvent::MappingStart {
                    anchor,
                    tag: Some(TAG_MAP.to_string()),
                    implicit: true,
                    flow_style,
                })?;
                for (k, v) in &ordered {
                    self.serialize_node(k)?;
                    self.serialize_node(v)?;
                }
                self.emit(EEvent::MappingEnd)
            }
            Node::Set(items) => {
                let null = Rc::new(Node::Null);
                let entries: Vec<(NodeRef, NodeRef)> =
                    items.iter().map(|k| (k.clone(), null.clone())).collect();
                let ordered = self.ordered(&entries);
                let flat: Vec<NodeRef> = ordered
                    .iter()
                    .flat_map(|(k, v)| [k.clone(), v.clone()])
                    .collect();
                let flow_style = self.flow_style(&flat);
                self.emit(EEvent::MappingStart {
                    anchor,
                    tag: Some(TAG_SET.to_string()),
                    implicit: false,
                    flow_style,
                })?;
                for (k, _) in &ordered {
                    self.serialize_node(k)?;
                    self.serialize_node(&Rc::new(Node::Null))?;
                }
                self.emit(EEvent::MappingEnd)
            }
            _ => Err(dump_error("cannot represent an object")),
        }
    }
}

/// `yaml.dump(data, ...)`: the document text for `node`.
pub fn dump_node(node: &NodeRef, opts: &DumpOptions) -> Result<String, PyYamlError> {
    let emitter = Emitter::new(
        opts.indent,
        opts.width,
        opts.allow_unicode,
        opts.line_break.as_deref(),
        opts.double_top_level_breaks,
    );
    let mut dumper = Dumper {
        opts,
        emitter,
        anchors: HashMap::new(),
        serialized: HashSet::new(),
        last_anchor_id: 0,
    };
    dumper.emit(EEvent::StreamStart)?;
    dumper.emit(EEvent::DocumentStart {
        explicit: opts.explicit_start,
    })?;
    dumper.anchor_node(node);
    dumper.serialize_node(node)?;
    dumper.emit(EEvent::DocumentEnd {
        explicit: opts.explicit_end,
    })?;
    dumper.emit(EEvent::StreamEnd)?;
    Ok(dumper.emitter.finish())
}

/// A [`Value`] as a node (always lossless: string keys, `i64` integers).
pub fn node_from_value(value: &Value) -> NodeRef {
    Rc::new(match value {
        Value::Null => Node::Null,
        Value::Bool(b) => Node::Bool(*b),
        Value::Int(i) => Node::Int(BigInt::from(*i)),
        Value::Float(f) => Node::Float(*f),
        Value::Str(s) => Node::Str(s.clone()),
        Value::List(items) => Node::Seq(items.iter().map(node_from_value).collect()),
        Value::Map(m) => Node::Map(
            m.iter()
                .map(|(k, v)| (Node::string(k.clone()), node_from_value(v)))
                .collect(),
        ),
    })
}

/// A node projected into the JSON-like [`Value`]: the projection is lossy by design (dates
/// become ISO strings, big integers decimal strings, non-string keys their JSON spelling) and
/// is only for consumers whose data model is JSON.
pub fn value_from_node(node: &Node) -> Value {
    match node {
        Node::Null => Value::Null,
        Node::Bool(b) => Value::Bool(*b),
        Node::Int(i) => match i.to_i64() {
            Some(n) => Value::Int(n),
            None => Value::Str(i.to_string()),
        },
        Node::Float(f) => Value::Float(*f),
        Node::Str(s) => Value::Str(s.clone()),
        Node::Bytes(b) => Value::Str(encode_base64(b).replace('\n', "")),
        Node::Date(d) => Value::Str(d.isoformat()),
        Node::DateTime(dt) => Value::Str(dt.isoformat('T')),
        Node::Seq(items) | Node::Tuple(items) | Node::Set(items) => {
            Value::List(items.iter().map(|i| value_from_node(i)).collect())
        }
        Node::Map(entries) => {
            let mut m = Map::new();
            for (k, v) in entries {
                let key = match value_from_node(k) {
                    Value::Str(s) => s,
                    other => crate::pyfmt::json_key(&other),
                };
                m.insert(key, value_from_node(v));
            }
            Value::Map(m)
        }
    }
}
