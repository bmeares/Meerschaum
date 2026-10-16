//! The composer and constructor: events to a lossless [`Node`] tree.
//!
//! This follows PyYAML's `composer.py` and `constructor.py` (`SafeConstructor`, plus the
//! `!!python/tuple` tag of the full `Loader`): YAML 1.1 implicit typing, merge keys with
//! PyYAML's precedence, `!!set` / `!!omap` / `!!pairs` / `!!binary` / `!!timestamp`, Python
//! dictionary semantics for repeated and colliding keys (`{1: a, True: b}`), and aliases that
//! yield the same shared node.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use num_bigint::BigInt;
use num_traits::Num;

use super::node::{
    Date, DateTime, Node, NodeRef, TAG_BINARY, TAG_BOOL, TAG_FLOAT, TAG_INT, TAG_MAP, TAG_MERGE,
    TAG_NULL, TAG_OMAP, TAG_PAIRS, TAG_PY_TUPLE, TAG_SEQ, TAG_SET, TAG_STR, TAG_TIMESTAMP,
    TAG_VALUE, dedup_entries, implicit_tag, timestamp_parts,
};
use super::parser::{EventKind, Parser};
use super::scanner::{ErrorKind, Mark, PyYamlError, ScanResult, Source, check_printable};
use crate::pyfmt::str_repr;

/// How a document is loaded.
#[derive(Debug, Clone, Copy, Default)]
pub struct LoadOptions {
    /// `yaml.safe_load`: `!!python/tuple` is refused like any other Python tag.
    pub safe: bool,
    /// Build values for the JSON-like [`crate::value::Value`] projection: timestamps stay
    /// strings and tuples / sets become lists.
    pub project: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Scalar,
    Sequence,
    Mapping,
}

impl Class {
    fn id(self) -> &'static str {
        match self {
            Class::Scalar => "scalar",
            Class::Sequence => "sequence",
            Class::Mapping => "mapping",
        }
    }
}

/// A composed (and constructed) node with what the parents need to know about it.
#[derive(Clone)]
struct Composed {
    node: NodeRef,
    class: Class,
    /// The resolved tag.
    tag: String,
    start: Mark,
    /// An error to raise if this node is used anywhere but as a merge key (`<<`) or `=` key.
    deferred: Option<PyYamlError>,
    /// For a sequence: where each item starts (merge errors point at the item).
    child_starts: Vec<Mark>,
}

const MAX_DEPTH: usize = 400;

struct Loader {
    parser: Parser,
    src: Rc<Source>,
    opts: LoadOptions,
    anchors: HashMap<String, Composed>,
    in_progress: HashSet<String>,
    depth: usize,
}

/// Load a single document from text the way `yaml.load` / `yaml.safe_load` do.
///
/// `name` is `<unicode string>`, `<byte string>` or the stream name; `snippets` controls
/// whether error messages quote the offending line (PyYAML omits them for file objects).
pub fn load_text(
    text: &str,
    name: &str,
    snippets: bool,
    opts: LoadOptions,
) -> Result<NodeRef, PyYamlError> {
    if let Some((position, code)) = check_printable(text) {
        return Err(PyYamlError {
            kind: ErrorKind::Reader,
            message: format!(
                "unacceptable character #x{code:04x}: special characters are not allowed\n  in \"{name}\", position {position}"
            ),
        });
    }
    let src = Rc::new(Source::new(text, name, snippets));
    let mut loader = Loader {
        parser: Parser::new(src.clone()),
        src,
        opts,
        anchors: HashMap::new(),
        in_progress: HashSet::new(),
        depth: 0,
    };
    loader.get_single_node()
}

impl Loader {
    fn err(
        &self,
        kind: ErrorKind,
        context: Option<&str>,
        context_mark: Option<Mark>,
        problem: &str,
        problem_mark: Option<Mark>,
    ) -> PyYamlError {
        PyYamlError {
            kind,
            message: self.src.marked(
                context,
                context_mark.as_ref(),
                Some(problem),
                problem_mark.as_ref(),
            ),
        }
    }

    fn cerr(&self, problem: &str, mark: Mark) -> PyYamlError {
        self.err(ErrorKind::Constructor, None, None, problem, Some(mark))
    }

    fn peek_is_stream_end(&mut self) -> ScanResult<bool> {
        Ok(matches!(
            self.parser.peek_event()?.map(|e| &e.kind),
            Some(EventKind::StreamEnd) | None
        ))
    }

    fn get_single_node(&mut self) -> Result<NodeRef, PyYamlError> {
        // Drop the STREAM-START event.
        self.parser.get_event()?;
        let mut document: Option<Composed> = None;
        if !self.peek_is_stream_end()? {
            document = Some(self.compose_document()?);
        }
        if !self.peek_is_stream_end()? {
            let event = self.parser.get_event()?;
            let event_start = event.map(|e| e.start);
            let doc_start = document.as_ref().map(|d| d.start);
            return Err(self.err(
                ErrorKind::Composer,
                Some("expected a single document in the stream"),
                doc_start,
                "but found another document",
                event_start,
            ));
        }
        match document {
            Some(d) => match d.deferred {
                Some(e) => Err(e),
                None => Ok(d.node),
            },
            None => Ok(Rc::new(Node::Null)),
        }
    }

    fn compose_document(&mut self) -> Result<Composed, PyYamlError> {
        // Drop the DOCUMENT-START event.
        self.parser.get_event()?;
        let node = self.compose_node()?;
        // Drop the DOCUMENT-END event.
        self.parser.get_event()?;
        self.anchors.clear();
        Ok(node)
    }

    fn compose_node(&mut self) -> Result<Composed, PyYamlError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(PyYamlError {
                kind: ErrorKind::Recursion,
                message: "maximum recursion depth exceeded".to_string(),
            });
        }
        let result = self.compose_node_inner();
        self.depth -= 1;
        result
    }

    fn compose_node_inner(&mut self) -> Result<Composed, PyYamlError> {
        let event = self
            .parser
            .peek_event()?
            .cloned()
            .ok_or_else(|| PyYamlError {
                kind: ErrorKind::Composer,
                message: "unexpected end of stream".to_string(),
            })?;
        if let EventKind::Alias(anchor) = &event.kind {
            self.parser.get_event()?;
            if let Some(c) = self.anchors.get(anchor) {
                return Ok(c.clone());
            }
            if self.in_progress.contains(anchor) {
                // PARITY: PyYAML builds self-referential containers (and then fails in most
                // consumers with a RecursionError); an `Rc` tree cannot hold a cycle.
                return Err(PyYamlError {
                    kind: ErrorKind::Recursion,
                    message: "maximum recursion depth exceeded".to_string(),
                });
            }
            return Err(self.err(
                ErrorKind::Composer,
                None,
                None,
                &format!("found undefined alias {}", str_repr(anchor)),
                Some(event.start),
            ));
        }
        let anchor = match &event.kind {
            EventKind::Scalar { anchor, .. }
            | EventKind::SequenceStart { anchor, .. }
            | EventKind::MappingStart { anchor, .. } => anchor.clone(),
            _ => None,
        };
        if let Some(a) = &anchor {
            if let Some(first) = self.anchors.get(a) {
                let first_start = first.start;
                return Err(self.err(
                    ErrorKind::Composer,
                    Some(&format!(
                        "found duplicate anchor {}; first occurrence",
                        str_repr(a)
                    )),
                    Some(first_start),
                    "second occurrence",
                    Some(event.start),
                ));
            }
            if self.in_progress.contains(a) {
                return Err(self.err(
                    ErrorKind::Composer,
                    Some(&format!(
                        "found duplicate anchor {}; first occurrence",
                        str_repr(a)
                    )),
                    Some(event.start),
                    "second occurrence",
                    Some(event.start),
                ));
            }
            self.in_progress.insert(a.clone());
        }
        let composed = match &event.kind {
            EventKind::Scalar { .. } => self.compose_scalar_node(),
            EventKind::SequenceStart { .. } => self.compose_sequence_node(),
            EventKind::MappingStart { .. } => self.compose_mapping_node(),
            other => Err(PyYamlError {
                kind: ErrorKind::Composer,
                message: format!("unexpected event {other:?}"),
            }),
        };
        if let Some(a) = &anchor {
            self.in_progress.remove(a);
        }
        let composed = composed?;
        if let Some(a) = anchor {
            self.anchors.insert(a, composed.clone());
        }
        Ok(composed)
    }

    fn compose_scalar_node(&mut self) -> Result<Composed, PyYamlError> {
        let Some(event) = self.parser.get_event()? else {
            return Err(self.cerr(
                "unexpected end of stream",
                Mark {
                    index: 0,
                    line: 0,
                    column: 0,
                },
            ));
        };
        let EventKind::Scalar {
            tag,
            implicit,
            value,
            ..
        } = event.kind
        else {
            return Err(self.cerr("expected a scalar", event.start));
        };
        let tag = match tag {
            Some(t) if t != "!" => t,
            _ => {
                // `resolve(ScalarNode, value, implicit)`.
                match (implicit.0, implicit_tag(&value)) {
                    (true, Some(t)) => t.to_string(),
                    _ => TAG_STR.to_string(),
                }
            }
        };
        Ok(self.construct_scalar(tag, value, event.start))
    }

    fn compose_sequence_node(&mut self) -> Result<Composed, PyYamlError> {
        let Some(start_event) = self.parser.get_event()? else {
            return Err(self.cerr(
                "unexpected end of stream",
                Mark {
                    index: 0,
                    line: 0,
                    column: 0,
                },
            ));
        };
        let EventKind::SequenceStart { tag, .. } = start_event.kind else {
            return Err(self.cerr("expected a sequence", start_event.start));
        };
        let tag = match tag {
            Some(t) if t != "!" => t,
            _ => TAG_SEQ.to_string(),
        };
        let mut items: Vec<Composed> = Vec::new();
        loop {
            let at_end = matches!(
                self.parser.peek_event()?.map(|e| &e.kind),
                Some(EventKind::SequenceEnd) | None
            );
            if at_end {
                break;
            }
            items.push(self.compose_node()?);
        }
        self.parser.get_event()?;
        Ok(self.construct_sequence(tag, items, start_event.start))
    }

    fn compose_mapping_node(&mut self) -> Result<Composed, PyYamlError> {
        let Some(start_event) = self.parser.get_event()? else {
            return Err(self.cerr(
                "unexpected end of stream",
                Mark {
                    index: 0,
                    line: 0,
                    column: 0,
                },
            ));
        };
        let EventKind::MappingStart { tag, .. } = start_event.kind else {
            return Err(self.cerr("expected a mapping", start_event.start));
        };
        let tag = match tag {
            Some(t) if t != "!" => t,
            _ => TAG_MAP.to_string(),
        };
        let mut pairs: Vec<(Composed, Composed)> = Vec::new();
        loop {
            let at_end = matches!(
                self.parser.peek_event()?.map(|e| &e.kind),
                Some(EventKind::MappingEnd) | None
            );
            if at_end {
                break;
            }
            let key = self.compose_node()?;
            let value = self.compose_node()?;
            pairs.push((key, value));
        }
        self.parser.get_event()?;
        Ok(self.construct_mapping(tag, pairs, start_event.start))
    }

    // ---- constructors ----

    fn scalar_result(&self, tag: String, node: Node, start: Mark) -> Result<Composed, PyYamlError> {
        Ok(Composed {
            node: Rc::new(node),
            class: Class::Scalar,
            tag,
            start,
            child_starts: Vec::new(),
            deferred: None,
        })
    }

    /// A node that failed to construct: PyYAML constructs top-down, so the error is kept
    /// until a parent decides whether its own construction (or an earlier sibling) fails first.
    fn failed(&self, class: Class, tag: String, start: Mark, error: PyYamlError) -> Composed {
        Composed {
            node: Rc::new(Node::Null),
            class,
            tag,
            start,
            child_starts: Vec::new(),
            deferred: Some(error),
        }
    }

    fn construct_scalar(&self, tag: String, value: String, start: Mark) -> Composed {
        match self.construct_scalar_inner(tag.clone(), value, start) {
            Ok(c) => c,
            Err(e) => self.failed(Class::Scalar, tag, start, e),
        }
    }

    fn construct_scalar_inner(
        &self,
        tag: String,
        value: String,
        start: Mark,
    ) -> Result<Composed, PyYamlError> {
        let node = match tag.as_str() {
            TAG_NULL => Node::Null,
            TAG_BOOL => match value.to_lowercase().as_str() {
                "yes" | "true" | "on" => Node::Bool(true),
                "no" | "false" | "off" => Node::Bool(false),
                other => {
                    // The raw key: Python's `str(KeyError(key))` quotes it.
                    return Err(PyYamlError {
                        kind: ErrorKind::Key,
                        message: other.to_string(),
                    });
                }
            },
            TAG_INT => Node::Int(construct_int(&value)?),
            TAG_FLOAT => Node::Float(construct_float(&value)?),
            TAG_BINARY => Node::Bytes(self.construct_binary(&value, start)?),
            TAG_TIMESTAMP => {
                if self.opts.project {
                    Node::Str(value)
                } else {
                    construct_timestamp(&value)?
                }
            }
            TAG_STR => Node::Str(value),
            TAG_MERGE | TAG_VALUE => {
                // Only legal as a mapping key (`<<` / `=`); anywhere else PyYAML has no
                // constructor for the tag.
                let err = self.cerr(
                    &format!(
                        "could not determine a constructor for the tag {}",
                        str_repr(&tag)
                    ),
                    start,
                );
                return Ok(Composed {
                    node: Rc::new(Node::Str(value)),
                    class: Class::Scalar,
                    tag,
                    start,
                    child_starts: Vec::new(),
                    deferred: Some(err),
                });
            }
            TAG_SEQ | TAG_PY_TUPLE if self.tuple_allowed(&tag) || tag == TAG_SEQ => {
                return Err(self.cerr("expected a sequence node, but found scalar", start));
            }
            TAG_MAP | TAG_SET => {
                return Err(self.cerr("expected a mapping node, but found scalar", start));
            }
            TAG_OMAP => {
                return Err(self.err(
                    ErrorKind::Constructor,
                    Some("while constructing an ordered map"),
                    Some(start),
                    "expected a sequence, but found scalar",
                    Some(start),
                ));
            }
            TAG_PAIRS => {
                return Err(self.err(
                    ErrorKind::Constructor,
                    Some("while constructing pairs"),
                    Some(start),
                    "expected a sequence, but found scalar",
                    Some(start),
                ));
            }
            _ => {
                return Err(self.cerr(
                    &format!(
                        "could not determine a constructor for the tag {}",
                        str_repr(&tag)
                    ),
                    start,
                ));
            }
        };
        self.scalar_result(tag, node, start)
    }

    fn tuple_allowed(&self, tag: &str) -> bool {
        tag == TAG_PY_TUPLE && !self.opts.safe
    }

    fn construct_binary(&self, value: &str, start: Mark) -> Result<Vec<u8>, PyYamlError> {
        if let Some((pos, c)) = value.chars().enumerate().find(|(_, c)| !c.is_ascii()) {
            let code = c as u32;
            let repr = if code < 0x100 {
                format!("'\\x{code:02x}'")
            } else if code < 0x10000 {
                format!("'\\u{code:04x}'")
            } else {
                format!("'\\U{code:08x}'")
            };
            return Err(self.cerr(
                &format!(
                    "failed to convert base64 data into ascii: 'ascii' codec can't encode character {repr} in position {pos}: ordinal not in range(128)"
                ),
                start,
            ));
        }
        decode_base64(value)
            .map_err(|m| self.cerr(&format!("failed to decode base64 data: {m}"), start))
    }

    fn take(&self, c: Composed) -> Result<Composed, PyYamlError> {
        match c.deferred {
            Some(e) => Err(e),
            None => Ok(c),
        }
    }

    fn construct_sequence(&self, tag: String, items: Vec<Composed>, start: Mark) -> Composed {
        match self.construct_sequence_inner(tag.clone(), items, start) {
            Ok(c) => c,
            Err(e) => self.failed(Class::Sequence, tag, start, e),
        }
    }

    fn construct_sequence_inner(
        &self,
        tag: String,
        items: Vec<Composed>,
        start: Mark,
    ) -> Result<Composed, PyYamlError> {
        let child_starts: Vec<Mark> = items.iter().map(|i| i.start).collect();
        let mut nodes: Vec<NodeRef> = Vec::with_capacity(items.len());
        match tag.as_str() {
            TAG_SEQ => {
                for item in items {
                    nodes.push(self.take(item)?.node);
                }
                self.collection(tag, Node::Seq(nodes), Class::Sequence, start, child_starts)
            }
            TAG_PY_TUPLE if !self.opts.safe => {
                for item in items {
                    nodes.push(self.take(item)?.node);
                }
                let node = if self.opts.project {
                    Node::Seq(nodes)
                } else {
                    Node::Tuple(nodes)
                };
                self.collection(tag, node, Class::Sequence, start, child_starts)
            }
            TAG_OMAP | TAG_PAIRS => {
                let context = if tag == TAG_OMAP {
                    "while constructing an ordered map"
                } else {
                    "while constructing pairs"
                };
                for sub in items {
                    let sub = self.take(sub)?;
                    let Node::Map(entries) = &*sub.node else {
                        return Err(self.err(
                            ErrorKind::Constructor,
                            Some(context),
                            Some(start),
                            &format!(
                                "expected a mapping of length 1, but found {}",
                                sub.class.id()
                            ),
                            Some(sub.start),
                        ));
                    };
                    if entries.len() != 1 {
                        return Err(self.err(
                            ErrorKind::Constructor,
                            Some(context),
                            Some(start),
                            &format!(
                                "expected a single mapping item, but found {} items",
                                entries.len()
                            ),
                            Some(sub.start),
                        ));
                    }
                    let (k, v) = entries[0].clone();
                    nodes.push(Rc::new(if self.opts.project {
                        Node::Seq(vec![k, v])
                    } else {
                        Node::Tuple(vec![k, v])
                    }));
                }
                self.collection(tag, Node::Seq(nodes), Class::Sequence, start, child_starts)
            }
            TAG_NULL | TAG_BOOL | TAG_INT | TAG_FLOAT | TAG_BINARY | TAG_TIMESTAMP | TAG_STR => {
                Err(self.cerr("expected a scalar node, but found sequence", start))
            }
            TAG_MAP | TAG_SET => {
                Err(self.cerr("expected a mapping node, but found sequence", start))
            }
            _ => Err(self.cerr(
                &format!(
                    "could not determine a constructor for the tag {}",
                    str_repr(&tag)
                ),
                start,
            )),
        }
    }

    fn collection(
        &self,
        tag: String,
        node: Node,
        class: Class,
        start: Mark,
        child_starts: Vec<Mark>,
    ) -> Result<Composed, PyYamlError> {
        Ok(Composed {
            node: Rc::new(node),
            class,
            tag,
            start,
            child_starts,
            deferred: None,
        })
    }

    fn construct_mapping(
        &self,
        tag: String,
        pairs: Vec<(Composed, Composed)>,
        start: Mark,
    ) -> Composed {
        match self.construct_mapping_inner(tag.clone(), pairs, start) {
            Ok(c) => c,
            Err(e) => self.failed(Class::Mapping, tag, start, e),
        }
    }

    fn construct_mapping_inner(
        &self,
        tag: String,
        pairs: Vec<(Composed, Composed)>,
        start: Mark,
    ) -> Result<Composed, PyYamlError> {
        match tag.as_str() {
            TAG_MAP | TAG_SET => {}
            TAG_NULL | TAG_BOOL | TAG_INT | TAG_FLOAT | TAG_BINARY | TAG_TIMESTAMP | TAG_STR => {
                return Err(self.cerr("expected a scalar node, but found mapping", start));
            }
            TAG_SEQ => {
                return Err(self.cerr("expected a sequence node, but found mapping", start));
            }
            TAG_OMAP | TAG_PAIRS => {
                let context = if tag == TAG_OMAP {
                    "while constructing an ordered map"
                } else {
                    "while constructing pairs"
                };
                return Err(self.err(
                    ErrorKind::Constructor,
                    Some(context),
                    Some(start),
                    "expected a sequence, but found mapping",
                    Some(start),
                ));
            }
            _ => {
                return Err(self.cerr(
                    &format!(
                        "could not determine a constructor for the tag {}",
                        str_repr(&tag)
                    ),
                    start,
                ));
            }
        }
        // `flatten_mapping`: merged entries come first, the mapping's own entries override.
        let mut merge: Vec<(NodeRef, NodeRef)> = Vec::new();
        let mut own: Vec<(Composed, Composed)> = Vec::new();
        for (key, value) in pairs {
            if key.tag == TAG_MERGE && key.class == Class::Scalar {
                let value = self.take(value)?;
                match (&*value.node, value.class) {
                    (Node::Map(entries), Class::Mapping) => merge.extend(entries.iter().cloned()),
                    (Node::Seq(items), Class::Sequence) => {
                        let mut submerge: Vec<&Vec<(NodeRef, NodeRef)>> = Vec::new();
                        for (i, sub) in items.iter().enumerate() {
                            match &**sub {
                                Node::Map(entries) => submerge.push(entries),
                                other => {
                                    return Err(self.err(
                                        ErrorKind::Constructor,
                                        Some("while constructing a mapping"),
                                        Some(start),
                                        &format!(
                                            "expected a mapping for merging, but found {}",
                                            other.yaml_kind()
                                        ),
                                        Some(
                                            value
                                                .child_starts
                                                .get(i)
                                                .copied()
                                                .unwrap_or(value.start),
                                        ),
                                    ));
                                }
                            }
                        }
                        submerge.reverse();
                        for entries in submerge {
                            merge.extend(entries.iter().cloned());
                        }
                    }
                    _ => {
                        return Err(self.err(
                            ErrorKind::Constructor,
                            Some("while constructing a mapping"),
                            Some(start),
                            &format!(
                                "expected a mapping or list of mappings for merging, but found {}",
                                value.class.id()
                            ),
                            Some(value.start),
                        ));
                    }
                }
            } else {
                own.push((key, value));
            }
        }
        let mut entries: Vec<(NodeRef, NodeRef)> = merge;
        for (key, value) in own {
            let key_start = key.start;
            // A `=` key (`tag:yaml.org,2002:value`) becomes a plain string key.
            let key_node = if key.tag == TAG_VALUE {
                key.node.clone()
            } else {
                self.take(key)?.node
            };
            if super::node::py_key(&key_node).is_none() {
                return Err(self.err(
                    ErrorKind::Constructor,
                    Some("while constructing a mapping"),
                    Some(start),
                    "found unhashable key",
                    Some(key_start),
                ));
            }
            let value = self.take(value)?;
            entries.push((key_node, value.node));
        }
        let Some(entries) = dedup_entries(entries) else {
            return Err(self.cerr("found unhashable key", start));
        };
        if tag == TAG_SET {
            let keys: Vec<NodeRef> = entries.into_iter().map(|(k, _)| k).collect();
            let node = if self.opts.project {
                Node::Seq(keys)
            } else {
                Node::Set(keys)
            };
            return self.collection(tag, node, Class::Mapping, start, Vec::new());
        }
        self.collection(tag, Node::Map(entries), Class::Mapping, start, Vec::new())
    }
}

fn value_error(message: String) -> PyYamlError {
    PyYamlError {
        kind: ErrorKind::Value,
        message,
    }
}

fn index_error() -> PyYamlError {
    PyYamlError {
        kind: ErrorKind::Index,
        message: "string index out of range".to_string(),
    }
}

/// Python's `int(text, base)`.
fn py_int(text: &str, base: u32) -> Result<BigInt, PyYamlError> {
    let trimmed = text.trim_matches(crate::pyfmt::py_isspace);
    let (neg, body) = match trimmed.as_bytes().first() {
        Some(b'-') => (true, &trimmed[1..]),
        Some(b'+') => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    let lower = body.to_ascii_lowercase();
    let body = match base {
        2 => lower.strip_prefix("0b").unwrap_or(&lower),
        8 => lower.strip_prefix("0o").unwrap_or(&lower),
        16 => lower.strip_prefix("0x").unwrap_or(&lower),
        _ => lower.as_str(),
    };
    // Python allows single underscores between digits; the YAML constructors strip them first.
    let cleaned: String = body.replace('_', "");
    let value = if cleaned.is_empty() || cleaned.contains(|c: char| c.is_whitespace()) {
        None
    } else {
        BigInt::from_str_radix(&cleaned, base).ok()
    };
    match value {
        Some(v) => Ok(if neg { -v } else { v }),
        None => Err(value_error(format!(
            "invalid literal for int() with base {base}: {}",
            str_repr(text)
        ))),
    }
}

/// `SafeConstructor.construct_yaml_int`.
pub(crate) fn construct_int(raw: &str) -> Result<BigInt, PyYamlError> {
    let value = raw.replace('_', "");
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(index_error());
    };
    let sign = if first == '-' { -1 } else { 1 };
    let value = if first == '+' || first == '-' {
        &value[1..]
    } else {
        value.as_str()
    };
    if value == "0" {
        return Ok(BigInt::from(0));
    }
    let n = if let Some(rest) = value.strip_prefix("0b") {
        py_int(rest, 2)?
    } else if let Some(rest) = value.strip_prefix("0x") {
        py_int(rest, 16)?
    } else if value.starts_with('0') {
        py_int(value, 8)?
    } else if value.contains(':') {
        let mut total = BigInt::from(0);
        let mut base = BigInt::from(1);
        for part in value.split(':').rev() {
            total += py_int(part, 10)? * &base;
            base *= 60;
        }
        total
    } else if value.is_empty() {
        return Err(index_error());
    } else {
        py_int(value, 10)?
    };
    Ok(if sign < 0 { -n } else { n })
}

fn py_float(text: &str) -> Result<f64, PyYamlError> {
    let t = text.trim_matches(crate::pyfmt::py_isspace);
    let ok_chars = !t.is_empty()
        && t.chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '.' | 'e' | 'E' | '+' | '-' | '_'))
        || matches!(
            t.to_ascii_lowercase().trim_start_matches(['+', '-']),
            "inf" | "infinity" | "nan"
        );
    let cleaned = t.replace('_', "");
    match (ok_chars, cleaned.parse::<f64>()) {
        (true, Ok(f)) => Ok(f),
        _ => Err(value_error(format!(
            "could not convert string to float: {}",
            str_repr(text)
        ))),
    }
}

/// `SafeConstructor.construct_yaml_float`.
pub(crate) fn construct_float(raw: &str) -> Result<f64, PyYamlError> {
    let value = raw.replace('_', "").to_lowercase();
    let Some(first) = value.chars().next() else {
        return Err(index_error());
    };
    let sign = if first == '-' { -1.0 } else { 1.0 };
    let value = if first == '+' || first == '-' {
        &value[1..]
    } else {
        value.as_str()
    };
    if value == ".inf" {
        return Ok(sign * f64::INFINITY);
    }
    if value == ".nan" {
        return Ok(f64::NAN);
    }
    if value.contains(':') {
        let mut total = 0.0;
        let mut base = 1.0;
        for part in value.split(':').rev() {
            total += py_float(part)? * base;
            base *= 60.0;
        }
        return Ok(sign * total);
    }
    Ok(sign * py_float(value)?)
}

fn construct_timestamp(value: &str) -> Result<Node, PyYamlError> {
    let Some(caps) = timestamp_parts(value) else {
        return Err(PyYamlError {
            kind: ErrorKind::Attribute,
            message: "'NoneType' object has no attribute 'groupdict'".to_string(),
        });
    };
    let num = |name: &str| -> u32 {
        caps.name(name)
            .and_then(|m| m.as_str().parse::<u32>().ok())
            .unwrap_or(0)
    };
    let date = Date {
        year: num("year") as i32,
        month: num("month"),
        day: num("day"),
    };
    if caps.name("hour").is_none_or(|m| m.as_str().is_empty()) {
        date.validate().map_err(value_error)?;
        return Ok(Node::Date(date));
    }
    let mut microsecond = 0;
    if let Some(fraction) = caps.name("fraction").map(|m| m.as_str()) {
        if !fraction.is_empty() {
            let mut f: String = fraction.chars().take(6).collect();
            while f.len() < 6 {
                f.push('0');
            }
            microsecond = f.parse::<u32>().unwrap_or(0);
        }
    }
    let mut utc_offset_us = None;
    if let Some(sign) = caps.name("tz_sign") {
        let tz_hour = i64::from(num("tz_hour"));
        let tz_minute = i64::from(num("tz_minute"));
        let mut delta = (tz_hour * 3600 + tz_minute * 60) * 1_000_000;
        if sign.as_str() == "-" {
            delta = -delta;
        }
        utc_offset_us = Some(delta);
    } else if caps.name("tz").is_some() {
        utc_offset_us = Some(0);
    }
    let datetime = DateTime {
        date,
        hour: num("hour"),
        minute: num("minute"),
        second: num("second"),
        microsecond,
        utc_offset_us,
    };
    datetime.validate().map_err(value_error)?;
    Ok(Node::DateTime(datetime))
}

/// `base64.decodebytes` (`binascii.a2b_base64`, non-strict).
pub(crate) fn decode_base64(text: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut acc: u32 = 0;
    let mut quad_pos = 0usize;
    let mut pads = 0usize;
    let mut data_chars = 0usize;
    let flush = |out: &mut Vec<u8>, acc: u32, quad_pos: usize| match quad_pos {
        2 => out.push((acc >> 4) as u8),
        3 => {
            out.push((acc >> 10) as u8);
            out.push((acc >> 2) as u8);
        }
        _ => {}
    };
    for c in text.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => {
                pads += 1;
                if quad_pos >= 2 && quad_pos + pads >= 4 {
                    // A pad sequence means no more input is parsed.
                    flush(&mut out, acc, quad_pos);
                    return Ok(out);
                }
                continue;
            }
            _ => continue,
        };
        pads = 0;
        data_chars += 1;
        acc = (acc << 6) | u32::from(v);
        quad_pos += 1;
        if quad_pos == 4 {
            out.push((acc >> 16) as u8);
            out.push((acc >> 8) as u8);
            out.push(acc as u8);
            acc = 0;
            quad_pos = 0;
        }
    }
    match quad_pos {
        0 => Ok(out),
        1 => Err(format!(
            "Invalid base64-encoded string: number of data characters ({data_chars}) cannot be 1 more than a multiple of 4"
        )),
        _ => Err("Incorrect padding".to_string()),
    }
}
