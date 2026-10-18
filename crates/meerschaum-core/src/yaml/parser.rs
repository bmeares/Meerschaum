//! A port of PyYAML's `parser.py`: tokens to events.

use std::collections::HashMap;
use std::rc::Rc;

use super::scanner::{
    DirectiveValue, ErrorKind, Mark, PyYamlError, ScanResult, Scanner, Source, Token, TokenKind,
};
use crate::pyfmt::str_repr;

/// The event kinds (`yaml.events`).
#[derive(Debug, Clone, PartialEq)]
pub enum EventKind {
    /// `STREAM-START`.
    StreamStart,
    /// `STREAM-END`.
    StreamEnd,
    /// `DOCUMENT-START`.
    DocumentStart,
    /// `DOCUMENT-END`.
    DocumentEnd,
    /// `ALIAS(anchor)`.
    Alias(String),
    /// `SCALAR`.
    Scalar {
        /// The anchor, if any.
        anchor: Option<String>,
        /// The expanded tag, if any.
        tag: Option<String>,
        /// `(plain implicit, quoted implicit)`.
        implicit: (bool, bool),
        /// The scalar text.
        value: String,
        /// The style character (`'`, `"`, `|`, `>`) or `None` for plain.
        style: Option<char>,
    },
    /// `SEQUENCE-START`.
    SequenceStart {
        /// The anchor, if any.
        anchor: Option<String>,
        /// The expanded tag, if any.
        tag: Option<String>,
        /// Whether the tag is implicit.
        implicit: bool,
    },
    /// `SEQUENCE-END`.
    SequenceEnd,
    /// `MAPPING-START`.
    MappingStart {
        /// The anchor, if any.
        anchor: Option<String>,
        /// The expanded tag, if any.
        tag: Option<String>,
        /// Whether the tag is implicit.
        implicit: bool,
    },
    /// `MAPPING-END`.
    MappingEnd,
}

/// An event with its marks.
#[derive(Debug, Clone)]
pub struct Event {
    /// What happened.
    pub kind: EventKind,
    /// Where it starts.
    pub start: Mark,
    /// Where it ends.
    pub end: Mark,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    StreamStart,
    ImplicitDocumentStart,
    DocumentStart,
    DocumentEnd,
    DocumentContent,
    BlockNode,
    BlockSequenceFirstEntry,
    BlockSequenceEntry,
    IndentlessSequenceEntry,
    BlockMappingFirstKey,
    BlockMappingKey,
    BlockMappingValue,
    FlowSequenceFirstEntry,
    FlowSequenceEntry,
    FlowSequenceEntryMappingKey,
    FlowSequenceEntryMappingValue,
    FlowSequenceEntryMappingEnd,
    FlowMappingFirstKey,
    FlowMappingKey,
    FlowMappingValue,
    FlowMappingEmptyValue,
    Done,
}

/// The parser (`yaml.parser.Parser`).
pub struct Parser {
    scanner: Scanner,
    src: Rc<Source>,
    current_event: Option<Event>,
    tag_handles: HashMap<String, String>,
    states: Vec<State>,
    marks: Vec<Mark>,
    state: State,
}

impl Parser {
    /// Create a parser over `src`.
    pub fn new(src: Rc<Source>) -> Parser {
        Parser {
            scanner: Scanner::new(src.clone()),
            src,
            current_event: None,
            tag_handles: HashMap::new(),
            states: Vec::new(),
            marks: Vec::new(),
            state: State::StreamStart,
        }
    }

    /// The shared source.
    pub fn source(&self) -> &Rc<Source> {
        &self.src
    }

    fn perr(
        &self,
        context: Option<&str>,
        context_mark: Option<Mark>,
        problem: &str,
        problem_mark: Mark,
    ) -> PyYamlError {
        PyYamlError {
            kind: ErrorKind::Parser,
            message: self.src.marked(
                context,
                context_mark.as_ref(),
                Some(problem),
                Some(&problem_mark),
            ),
        }
    }

    /// The next event without consuming it.
    pub fn peek_event(&mut self) -> ScanResult<Option<&Event>> {
        if self.current_event.is_none() && self.state != State::Done {
            self.current_event = Some(self.step()?);
        }
        Ok(self.current_event.as_ref())
    }

    /// Consume the next event.
    pub fn get_event(&mut self) -> ScanResult<Option<Event>> {
        if self.current_event.is_none() && self.state != State::Done {
            self.current_event = Some(self.step()?);
        }
        Ok(self.current_event.take())
    }

    fn step(&mut self) -> ScanResult<Event> {
        match self.state {
            State::StreamStart => self.parse_stream_start(),
            State::ImplicitDocumentStart => self.parse_implicit_document_start(),
            State::DocumentStart => self.parse_document_start(),
            State::DocumentEnd => self.parse_document_end(),
            State::DocumentContent => self.parse_document_content(),
            State::BlockNode => self.parse_node(true, false),
            State::BlockSequenceFirstEntry => self.parse_block_sequence_first_entry(),
            State::BlockSequenceEntry => self.parse_block_sequence_entry(),
            State::IndentlessSequenceEntry => self.parse_indentless_sequence_entry(),
            State::BlockMappingFirstKey => self.parse_block_mapping_first_key(),
            State::BlockMappingKey => self.parse_block_mapping_key(),
            State::BlockMappingValue => self.parse_block_mapping_value(),
            State::FlowSequenceFirstEntry => self.parse_flow_sequence_first_entry(),
            State::FlowSequenceEntry => self.parse_flow_sequence_entry(false),
            State::FlowSequenceEntryMappingKey => self.parse_flow_sequence_entry_mapping_key(),
            State::FlowSequenceEntryMappingValue => self.parse_flow_sequence_entry_mapping_value(),
            State::FlowSequenceEntryMappingEnd => self.parse_flow_sequence_entry_mapping_end(),
            State::FlowMappingFirstKey => self.parse_flow_mapping_first_key(),
            State::FlowMappingKey => self.parse_flow_mapping_key(false),
            State::FlowMappingValue => self.parse_flow_mapping_value(),
            State::FlowMappingEmptyValue => self.parse_flow_mapping_empty_value(),
            State::Done => unreachable!("the parser has finished"),
        }
    }

    fn kind(&mut self) -> ScanResult<TokenKind> {
        Ok(self.scanner.peek_kind()?.unwrap_or(TokenKind::StreamEnd))
    }

    fn peek_start(&mut self) -> ScanResult<Mark> {
        Ok(self.scanner.peek_token()?.map(|t| t.start).unwrap_or(Mark {
            index: 0,
            line: 0,
            column: 0,
        }))
    }

    fn next_token(&mut self) -> ScanResult<Token> {
        Ok(self.scanner.get_token()?.unwrap_or(Token {
            kind: TokenKind::StreamEnd,
            start: Mark {
                index: 0,
                line: 0,
                column: 0,
            },
            end: Mark {
                index: 0,
                line: 0,
                column: 0,
            },
        }))
    }

    fn parse_stream_start(&mut self) -> ScanResult<Event> {
        let token = self.next_token()?;
        self.state = State::ImplicitDocumentStart;
        Ok(Event {
            kind: EventKind::StreamStart,
            start: token.start,
            end: token.end,
        })
    }

    fn parse_implicit_document_start(&mut self) -> ScanResult<Event> {
        if !matches!(
            self.kind()?,
            TokenKind::Directive(..) | TokenKind::DocumentStart | TokenKind::StreamEnd
        ) {
            self.tag_handles = default_tags();
            let start = self.peek_start()?;
            self.states.push(State::DocumentEnd);
            self.state = State::BlockNode;
            Ok(Event {
                kind: EventKind::DocumentStart,
                start,
                end: start,
            })
        } else {
            self.parse_document_start()
        }
    }

    fn parse_document_start(&mut self) -> ScanResult<Event> {
        while matches!(self.kind()?, TokenKind::DocumentEnd) {
            self.next_token()?;
        }
        if !matches!(self.kind()?, TokenKind::StreamEnd) {
            let start = self.peek_start()?;
            self.process_directives()?;
            if !matches!(self.kind()?, TokenKind::DocumentStart) {
                let token = self.scanner.peek_token()?.cloned();
                let (id, mark) = match token {
                    Some(t) => (t.kind.id(), t.start),
                    None => ("<stream end>", start),
                };
                return Err(self.perr(
                    None,
                    None,
                    &format!("expected '<document start>', but found {}", str_repr(id)),
                    mark,
                ));
            }
            let token = self.next_token()?;
            self.states.push(State::DocumentEnd);
            self.state = State::DocumentContent;
            Ok(Event {
                kind: EventKind::DocumentStart,
                start,
                end: token.end,
            })
        } else {
            let token = self.next_token()?;
            self.state = State::Done;
            Ok(Event {
                kind: EventKind::StreamEnd,
                start: token.start,
                end: token.end,
            })
        }
    }

    fn parse_document_end(&mut self) -> ScanResult<Event> {
        let start = self.peek_start()?;
        let mut end = start;
        if matches!(self.kind()?, TokenKind::DocumentEnd) {
            let token = self.next_token()?;
            end = token.end;
        }
        self.state = State::DocumentStart;
        Ok(Event {
            kind: EventKind::DocumentEnd,
            start,
            end,
        })
    }

    fn parse_document_content(&mut self) -> ScanResult<Event> {
        if matches!(
            self.kind()?,
            TokenKind::Directive(..)
                | TokenKind::DocumentStart
                | TokenKind::DocumentEnd
                | TokenKind::StreamEnd
        ) {
            let mark = self.peek_start()?;
            let event = process_empty_scalar(mark);
            self.state = self.states.pop().unwrap_or(State::Done);
            Ok(event)
        } else {
            self.parse_node(true, false)
        }
    }

    fn process_directives(&mut self) -> ScanResult<()> {
        let mut yaml_version: Option<(i64, i64)> = None;
        self.tag_handles = HashMap::new();
        while matches!(self.kind()?, TokenKind::Directive(..)) {
            let token = self.next_token()?;
            if let TokenKind::Directive(name, value) = token.kind {
                if name == "YAML" {
                    if yaml_version.is_some() {
                        return Err(self.perr(
                            None,
                            None,
                            "found duplicate YAML directive",
                            token.start,
                        ));
                    }
                    if let DirectiveValue::Yaml(major, minor) = value {
                        if major != 1 {
                            return Err(self.perr(
                                None,
                                None,
                                "found incompatible YAML document (version 1.* is required)",
                                token.start,
                            ));
                        }
                        yaml_version = Some((major, minor));
                    }
                } else if name == "TAG"
                    && let DirectiveValue::Tag(handle, prefix) = value
                {
                    if self.tag_handles.contains_key(&handle) {
                        return Err(self.perr(
                            None,
                            None,
                            &format!("duplicate tag handle {}", str_repr(&handle)),
                            token.start,
                        ));
                    }
                    self.tag_handles.insert(handle, prefix);
                }
            }
        }
        for (key, value) in default_tags() {
            self.tag_handles.entry(key).or_insert(value);
        }
        Ok(())
    }

    fn parse_node(&mut self, block: bool, indentless_sequence: bool) -> ScanResult<Event> {
        if let TokenKind::Alias(_) = self.kind()? {
            let token = self.next_token()?;
            let TokenKind::Alias(value) = token.kind else {
                unreachable!()
            };
            self.state = self.states.pop().unwrap_or(State::Done);
            return Ok(Event {
                kind: EventKind::Alias(value),
                start: token.start,
                end: token.end,
            });
        }
        let mut anchor: Option<String> = None;
        let mut tag: Option<(Option<String>, String)> = None;
        let mut start_mark: Option<Mark> = None;
        let mut end_mark: Option<Mark> = None;
        let mut tag_mark: Option<Mark> = None;
        if let TokenKind::Anchor(_) = self.kind()? {
            let token = self.next_token()?;
            start_mark = Some(token.start);
            end_mark = Some(token.end);
            if let TokenKind::Anchor(v) = token.kind {
                anchor = Some(v);
            }
            if let TokenKind::Tag(..) = self.kind()? {
                let token = self.next_token()?;
                tag_mark = Some(token.start);
                end_mark = Some(token.end);
                if let TokenKind::Tag(h, s) = token.kind {
                    tag = Some((h, s));
                }
            }
        } else if let TokenKind::Tag(..) = self.kind()? {
            let token = self.next_token()?;
            start_mark = Some(token.start);
            tag_mark = Some(token.start);
            end_mark = Some(token.end);
            if let TokenKind::Tag(h, s) = token.kind {
                tag = Some((h, s));
            }
            if let TokenKind::Anchor(_) = self.kind()? {
                let token = self.next_token()?;
                end_mark = Some(token.end);
                if let TokenKind::Anchor(v) = token.kind {
                    anchor = Some(v);
                }
            }
        }
        let tag: Option<String> = match tag {
            None => None,
            Some((Some(handle), suffix)) => match self.tag_handles.get(&handle) {
                Some(prefix) => Some(format!("{prefix}{suffix}")),
                None => {
                    return Err(self.perr(
                        Some("while parsing a node"),
                        start_mark,
                        &format!("found undefined tag handle {}", str_repr(&handle)),
                        tag_mark.unwrap_or(Mark {
                            index: 0,
                            line: 0,
                            column: 0,
                        }),
                    ));
                }
            },
            Some((None, suffix)) => Some(suffix),
        };
        let start_mark_v = match start_mark {
            Some(m) => m,
            None => {
                let m = self.peek_start()?;
                end_mark = Some(m);
                m
            }
        };
        let mut end_mark_v = end_mark.unwrap_or(start_mark_v);
        let mut implicit_b = tag.is_none() || tag.as_deref() == Some("!");
        let kind = self.kind()?;
        let event;
        if indentless_sequence && matches!(kind, TokenKind::BlockEntry) {
            end_mark_v = self
                .scanner
                .peek_token()?
                .map(|t| t.end)
                .unwrap_or(end_mark_v);
            event = EventKind::SequenceStart {
                anchor,
                tag,
                implicit: implicit_b,
            };
            self.state = State::IndentlessSequenceEntry;
        } else if let TokenKind::Scalar(value, plain, style) = kind {
            let token = self.next_token()?;
            end_mark_v = token.end;
            let implicit = if (plain && tag.is_none()) || tag.as_deref() == Some("!") {
                (true, false)
            } else if tag.is_none() {
                (false, true)
            } else {
                (false, false)
            };
            event = EventKind::Scalar {
                anchor,
                tag,
                implicit,
                value,
                style,
            };
            self.state = self.states.pop().unwrap_or(State::Done);
        } else if matches!(kind, TokenKind::FlowSequenceStart) {
            end_mark_v = self
                .scanner
                .peek_token()?
                .map(|t| t.end)
                .unwrap_or(end_mark_v);
            event = EventKind::SequenceStart {
                anchor,
                tag,
                implicit: implicit_b,
            };
            self.state = State::FlowSequenceFirstEntry;
        } else if matches!(kind, TokenKind::FlowMappingStart) {
            end_mark_v = self
                .scanner
                .peek_token()?
                .map(|t| t.end)
                .unwrap_or(end_mark_v);
            event = EventKind::MappingStart {
                anchor,
                tag,
                implicit: implicit_b,
            };
            self.state = State::FlowMappingFirstKey;
        } else if block && matches!(kind, TokenKind::BlockSequenceStart) {
            end_mark_v = self
                .scanner
                .peek_token()?
                .map(|t| t.start)
                .unwrap_or(end_mark_v);
            event = EventKind::SequenceStart {
                anchor,
                tag,
                implicit: implicit_b,
            };
            self.state = State::BlockSequenceFirstEntry;
        } else if block && matches!(kind, TokenKind::BlockMappingStart) {
            end_mark_v = self
                .scanner
                .peek_token()?
                .map(|t| t.start)
                .unwrap_or(end_mark_v);
            event = EventKind::MappingStart {
                anchor,
                tag,
                implicit: implicit_b,
            };
            self.state = State::BlockMappingFirstKey;
        } else if anchor.is_some() || tag.is_some() {
            // Empty scalars are allowed even if a tag or an anchor is specified.
            // PyYAML passes `(implicit, False)` where `implicit` is a bool.
            implicit_b = tag.is_none() || tag.as_deref() == Some("!");
            event = EventKind::Scalar {
                anchor,
                tag,
                implicit: (implicit_b, false),
                value: String::new(),
                style: None,
            };
            self.state = self.states.pop().unwrap_or(State::Done);
        } else {
            let node = if block { "block" } else { "flow" };
            let token = self.scanner.peek_token()?.cloned();
            let (id, mark) = match token {
                Some(t) => (t.kind.id(), t.start),
                None => ("<stream end>", start_mark_v),
            };
            return Err(self.perr(
                Some(&format!("while parsing a {node} node")),
                Some(start_mark_v),
                &format!("expected the node content, but found {}", str_repr(id)),
                mark,
            ));
        }
        Ok(Event {
            kind: event,
            start: start_mark_v,
            end: end_mark_v,
        })
    }

    fn parse_block_sequence_first_entry(&mut self) -> ScanResult<Event> {
        let token = self.next_token()?;
        self.marks.push(token.start);
        self.parse_block_sequence_entry()
    }

    fn parse_block_sequence_entry(&mut self) -> ScanResult<Event> {
        if matches!(self.kind()?, TokenKind::BlockEntry) {
            let token = self.next_token()?;
            if !matches!(self.kind()?, TokenKind::BlockEntry | TokenKind::BlockEnd) {
                self.states.push(State::BlockSequenceEntry);
                return self.parse_node(true, false);
            } else {
                self.state = State::BlockSequenceEntry;
                return Ok(process_empty_scalar(token.end));
            }
        }
        if !matches!(self.kind()?, TokenKind::BlockEnd) {
            let token = self.scanner.peek_token()?.cloned();
            let (id, mark) = match token {
                Some(t) => (t.kind.id(), t.start),
                None => (
                    "<stream end>",
                    Mark {
                        index: 0,
                        line: 0,
                        column: 0,
                    },
                ),
            };
            let ctx = self.marks.last().copied();
            return Err(self.perr(
                Some("while parsing a block collection"),
                ctx,
                &format!("expected <block end>, but found {}", str_repr(id)),
                mark,
            ));
        }
        let token = self.next_token()?;
        self.state = self.states.pop().unwrap_or(State::Done);
        self.marks.pop();
        Ok(Event {
            kind: EventKind::SequenceEnd,
            start: token.start,
            end: token.end,
        })
    }

    fn parse_indentless_sequence_entry(&mut self) -> ScanResult<Event> {
        if matches!(self.kind()?, TokenKind::BlockEntry) {
            let token = self.next_token()?;
            if !matches!(
                self.kind()?,
                TokenKind::BlockEntry | TokenKind::Key | TokenKind::Value | TokenKind::BlockEnd
            ) {
                self.states.push(State::IndentlessSequenceEntry);
                return self.parse_node(true, false);
            } else {
                self.state = State::IndentlessSequenceEntry;
                return Ok(process_empty_scalar(token.end));
            }
        }
        let start = self.peek_start()?;
        self.state = self.states.pop().unwrap_or(State::Done);
        Ok(Event {
            kind: EventKind::SequenceEnd,
            start,
            end: start,
        })
    }

    fn parse_block_mapping_first_key(&mut self) -> ScanResult<Event> {
        let token = self.next_token()?;
        self.marks.push(token.start);
        self.parse_block_mapping_key()
    }

    fn parse_block_mapping_key(&mut self) -> ScanResult<Event> {
        if matches!(self.kind()?, TokenKind::Key) {
            let token = self.next_token()?;
            if !matches!(
                self.kind()?,
                TokenKind::Key | TokenKind::Value | TokenKind::BlockEnd
            ) {
                self.states.push(State::BlockMappingValue);
                return self.parse_node(true, true);
            } else {
                self.state = State::BlockMappingValue;
                return Ok(process_empty_scalar(token.end));
            }
        }
        if !matches!(self.kind()?, TokenKind::BlockEnd) {
            let token = self.scanner.peek_token()?.cloned();
            let (id, mark) = match token {
                Some(t) => (t.kind.id(), t.start),
                None => (
                    "<stream end>",
                    Mark {
                        index: 0,
                        line: 0,
                        column: 0,
                    },
                ),
            };
            let ctx = self.marks.last().copied();
            return Err(self.perr(
                Some("while parsing a block mapping"),
                ctx,
                &format!("expected <block end>, but found {}", str_repr(id)),
                mark,
            ));
        }
        let token = self.next_token()?;
        self.state = self.states.pop().unwrap_or(State::Done);
        self.marks.pop();
        Ok(Event {
            kind: EventKind::MappingEnd,
            start: token.start,
            end: token.end,
        })
    }

    fn parse_block_mapping_value(&mut self) -> ScanResult<Event> {
        if matches!(self.kind()?, TokenKind::Value) {
            let token = self.next_token()?;
            if !matches!(
                self.kind()?,
                TokenKind::Key | TokenKind::Value | TokenKind::BlockEnd
            ) {
                self.states.push(State::BlockMappingKey);
                self.parse_node(true, true)
            } else {
                self.state = State::BlockMappingKey;
                Ok(process_empty_scalar(token.end))
            }
        } else {
            self.state = State::BlockMappingKey;
            let mark = self.peek_start()?;
            Ok(process_empty_scalar(mark))
        }
    }

    fn parse_flow_sequence_first_entry(&mut self) -> ScanResult<Event> {
        let token = self.next_token()?;
        self.marks.push(token.start);
        self.parse_flow_sequence_entry(true)
    }

    fn parse_flow_sequence_entry(&mut self, first: bool) -> ScanResult<Event> {
        if !matches!(self.kind()?, TokenKind::FlowSequenceEnd) {
            if !first {
                if matches!(self.kind()?, TokenKind::FlowEntry) {
                    self.next_token()?;
                } else {
                    let token = self.scanner.peek_token()?.cloned();
                    let (id, mark) = match token {
                        Some(t) => (t.kind.id(), t.start),
                        None => (
                            "<stream end>",
                            Mark {
                                index: 0,
                                line: 0,
                                column: 0,
                            },
                        ),
                    };
                    let ctx = self.marks.last().copied();
                    return Err(self.perr(
                        Some("while parsing a flow sequence"),
                        ctx,
                        &format!("expected ',' or ']', but got {}", str_repr(id)),
                        mark,
                    ));
                }
            }
            if matches!(self.kind()?, TokenKind::Key) {
                let (start, end) = {
                    let t = self.scanner.peek_token()?;
                    t.map(|t| (t.start, t.end)).unwrap_or_default_marks()
                };
                self.state = State::FlowSequenceEntryMappingKey;
                return Ok(Event {
                    kind: EventKind::MappingStart {
                        anchor: None,
                        tag: None,
                        implicit: true,
                    },
                    start,
                    end,
                });
            } else if !matches!(self.kind()?, TokenKind::FlowSequenceEnd) {
                self.states.push(State::FlowSequenceEntry);
                return self.parse_node(false, false);
            }
        }
        let token = self.next_token()?;
        self.state = self.states.pop().unwrap_or(State::Done);
        self.marks.pop();
        Ok(Event {
            kind: EventKind::SequenceEnd,
            start: token.start,
            end: token.end,
        })
    }

    fn parse_flow_sequence_entry_mapping_key(&mut self) -> ScanResult<Event> {
        let token = self.next_token()?;
        if !matches!(
            self.kind()?,
            TokenKind::Value | TokenKind::FlowEntry | TokenKind::FlowSequenceEnd
        ) {
            self.states.push(State::FlowSequenceEntryMappingValue);
            self.parse_node(false, false)
        } else {
            self.state = State::FlowSequenceEntryMappingValue;
            Ok(process_empty_scalar(token.end))
        }
    }

    fn parse_flow_sequence_entry_mapping_value(&mut self) -> ScanResult<Event> {
        if matches!(self.kind()?, TokenKind::Value) {
            let token = self.next_token()?;
            if !matches!(
                self.kind()?,
                TokenKind::FlowEntry | TokenKind::FlowSequenceEnd
            ) {
                self.states.push(State::FlowSequenceEntryMappingEnd);
                self.parse_node(false, false)
            } else {
                self.state = State::FlowSequenceEntryMappingEnd;
                Ok(process_empty_scalar(token.end))
            }
        } else {
            self.state = State::FlowSequenceEntryMappingEnd;
            let mark = self.peek_start()?;
            Ok(process_empty_scalar(mark))
        }
    }

    fn parse_flow_sequence_entry_mapping_end(&mut self) -> ScanResult<Event> {
        self.state = State::FlowSequenceEntry;
        let mark = self.peek_start()?;
        Ok(Event {
            kind: EventKind::MappingEnd,
            start: mark,
            end: mark,
        })
    }

    fn parse_flow_mapping_first_key(&mut self) -> ScanResult<Event> {
        let token = self.next_token()?;
        self.marks.push(token.start);
        self.parse_flow_mapping_key(true)
    }

    fn parse_flow_mapping_key(&mut self, first: bool) -> ScanResult<Event> {
        if !matches!(self.kind()?, TokenKind::FlowMappingEnd) {
            if !first {
                if matches!(self.kind()?, TokenKind::FlowEntry) {
                    self.next_token()?;
                } else {
                    let token = self.scanner.peek_token()?.cloned();
                    let (id, mark) = match token {
                        Some(t) => (t.kind.id(), t.start),
                        None => (
                            "<stream end>",
                            Mark {
                                index: 0,
                                line: 0,
                                column: 0,
                            },
                        ),
                    };
                    let ctx = self.marks.last().copied();
                    return Err(self.perr(
                        Some("while parsing a flow mapping"),
                        ctx,
                        &format!("expected ',' or '}}', but got {}", str_repr(id)),
                        mark,
                    ));
                }
            }
            if matches!(self.kind()?, TokenKind::Key) {
                let token = self.next_token()?;
                if !matches!(
                    self.kind()?,
                    TokenKind::Value | TokenKind::FlowEntry | TokenKind::FlowMappingEnd
                ) {
                    self.states.push(State::FlowMappingValue);
                    return self.parse_node(false, false);
                } else {
                    self.state = State::FlowMappingValue;
                    return Ok(process_empty_scalar(token.end));
                }
            } else if !matches!(self.kind()?, TokenKind::FlowMappingEnd) {
                self.states.push(State::FlowMappingEmptyValue);
                return self.parse_node(false, false);
            }
        }
        let token = self.next_token()?;
        self.state = self.states.pop().unwrap_or(State::Done);
        self.marks.pop();
        Ok(Event {
            kind: EventKind::MappingEnd,
            start: token.start,
            end: token.end,
        })
    }

    fn parse_flow_mapping_value(&mut self) -> ScanResult<Event> {
        if matches!(self.kind()?, TokenKind::Value) {
            let token = self.next_token()?;
            if !matches!(
                self.kind()?,
                TokenKind::FlowEntry | TokenKind::FlowMappingEnd
            ) {
                self.states.push(State::FlowMappingKey);
                self.parse_node(false, false)
            } else {
                self.state = State::FlowMappingKey;
                Ok(process_empty_scalar(token.end))
            }
        } else {
            self.state = State::FlowMappingKey;
            let mark = self.peek_start()?;
            Ok(process_empty_scalar(mark))
        }
    }

    fn parse_flow_mapping_empty_value(&mut self) -> ScanResult<Event> {
        self.state = State::FlowMappingKey;
        let mark = self.peek_start()?;
        Ok(process_empty_scalar(mark))
    }
}

trait MarksOrDefault {
    fn unwrap_or_default_marks(self) -> (Mark, Mark);
}

impl MarksOrDefault for Option<(Mark, Mark)> {
    fn unwrap_or_default_marks(self) -> (Mark, Mark) {
        self.unwrap_or((
            Mark {
                index: 0,
                line: 0,
                column: 0,
            },
            Mark {
                index: 0,
                line: 0,
                column: 0,
            },
        ))
    }
}

fn default_tags() -> HashMap<String, String> {
    HashMap::from([
        ("!".to_string(), "!".to_string()),
        ("!!".to_string(), "tag:yaml.org,2002:".to_string()),
    ])
}

fn process_empty_scalar(mark: Mark) -> Event {
    Event {
        kind: EventKind::Scalar {
            anchor: None,
            tag: None,
            implicit: (true, false),
            value: String::new(),
            style: None,
        },
        start: mark,
        end: mark,
    }
}
