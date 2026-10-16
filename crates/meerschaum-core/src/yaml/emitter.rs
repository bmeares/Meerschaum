//! A port of PyYAML's `emitter.py` (state machine, scalar analysis and the scalar writers).
//!
//! Meerschaum dumps with a `Dumper` subclass whose `write_line_break` doubles the break while
//! only the top-level indentation level is open (a blank line between top-level keys);
//! [`Emitter::double_top_level_breaks`] switches that on.

use std::collections::VecDeque;

use super::scanner::{ErrorKind, PyYamlError};

pub(crate) type EResult<T> = Result<T, PyYamlError>;

fn emitter_error(message: impl Into<String>) -> PyYamlError {
    PyYamlError {
        kind: ErrorKind::Emitter,
        message: message.into(),
    }
}

/// An event the emitter consumes (`yaml.events`).
#[derive(Debug, Clone)]
pub(crate) enum EEvent {
    StreamStart,
    StreamEnd,
    DocumentStart {
        explicit: bool,
    },
    DocumentEnd {
        explicit: bool,
    },
    Alias {
        anchor: String,
    },
    Scalar {
        anchor: Option<String>,
        tag: Option<String>,
        implicit: (bool, bool),
        value: String,
        style: Option<char>,
    },
    SequenceStart {
        anchor: Option<String>,
        tag: Option<String>,
        implicit: bool,
        flow_style: bool,
    },
    SequenceEnd,
    MappingStart {
        anchor: Option<String>,
        tag: Option<String>,
        implicit: bool,
        flow_style: bool,
    },
    MappingEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    StreamStart,
    FirstDocumentStart,
    DocumentStart,
    DocumentEnd,
    DocumentRoot,
    FlowSequenceFirstItem,
    FlowSequenceItem,
    FlowMappingFirstKey,
    FlowMappingKey,
    FlowMappingSimpleValue,
    FlowMappingValue,
    BlockSequenceFirstItem,
    BlockSequenceItem,
    BlockMappingFirstKey,
    BlockMappingKey,
    BlockMappingSimpleValue,
    BlockMappingValue,
    Nothing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Style {
    Plain,
    Single,
    Double,
    Literal,
    Folded,
}

#[derive(Debug, Clone)]
struct ScalarAnalysis {
    scalar: Vec<char>,
    empty: bool,
    multiline: bool,
    allow_flow_plain: bool,
    allow_block_plain: bool,
    allow_single_quoted: bool,
    allow_block: bool,
}

/// The emitter.
pub(crate) struct Emitter {
    out: String,
    states: Vec<State>,
    state: State,
    events: VecDeque<EEvent>,
    indents: Vec<Option<usize>>,
    indent: Option<usize>,
    flow_level: usize,
    root_context: bool,
    sequence_context: bool,
    mapping_context: bool,
    simple_key_context: bool,
    column: usize,
    whitespace: bool,
    indention: bool,
    open_ended: bool,
    allow_unicode: bool,
    best_indent: usize,
    best_width: usize,
    best_line_break: String,
    tag_prefixes: Vec<(String, String)>,
    prepared_anchor: Option<String>,
    prepared_tag: Option<String>,
    analysis: Option<ScalarAnalysis>,
    style: Option<Style>,
    double_top_level_breaks: bool,
}

fn slice(text: &[char], start: usize, end: usize) -> String {
    if start >= end || start >= text.len() {
        return String::new();
    }
    text[start..end.min(text.len())].iter().collect()
}

fn is_break(c: char) -> bool {
    matches!(c, '\n' | '\u{85}' | '\u{2028}' | '\u{2029}')
}

impl Emitter {
    pub(crate) fn new(
        indent: Option<usize>,
        width: Option<usize>,
        allow_unicode: bool,
        line_break: Option<&str>,
        double_top_level_breaks: bool,
    ) -> Emitter {
        let mut best_indent = 2;
        if let Some(i) = indent {
            if i > 1 && i < 10 {
                best_indent = i;
            }
        }
        let mut best_width = 80;
        if let Some(w) = width {
            if w > best_indent * 2 {
                best_width = w;
            }
        }
        let best_line_break = match line_break {
            Some(l @ ("\r" | "\n" | "\r\n")) => l.to_string(),
            _ => "\n".to_string(),
        };
        Emitter {
            out: String::new(),
            states: Vec::new(),
            state: State::StreamStart,
            events: VecDeque::new(),
            indents: Vec::new(),
            indent: None,
            flow_level: 0,
            root_context: false,
            sequence_context: false,
            mapping_context: false,
            simple_key_context: false,
            column: 0,
            whitespace: true,
            indention: true,
            open_ended: false,
            allow_unicode,
            best_indent,
            best_width,
            best_line_break,
            tag_prefixes: Vec::new(),
            prepared_anchor: None,
            prepared_tag: None,
            analysis: None,
            style: None,
            double_top_level_breaks,
        }
    }

    pub(crate) fn finish(self) -> String {
        self.out
    }

    pub(crate) fn emit(&mut self, event: EEvent) -> EResult<()> {
        self.events.push_back(event);
        while !self.need_more_events() {
            let Some(ev) = self.events.pop_front() else {
                break;
            };
            self.run_state(&ev)?;
        }
        Ok(())
    }

    fn need_more_events(&self) -> bool {
        let Some(first) = self.events.front() else {
            return true;
        };
        match first {
            EEvent::DocumentStart { .. } => self.need_events(1),
            EEvent::SequenceStart { .. } => self.need_events(2),
            EEvent::MappingStart { .. } => self.need_events(3),
            _ => false,
        }
    }

    fn need_events(&self, count: usize) -> bool {
        let mut level: i64 = 0;
        for event in self.events.iter().skip(1) {
            match event {
                EEvent::DocumentStart { .. }
                | EEvent::SequenceStart { .. }
                | EEvent::MappingStart { .. } => level += 1,
                EEvent::DocumentEnd { .. } | EEvent::SequenceEnd | EEvent::MappingEnd => level -= 1,
                EEvent::StreamEnd => level = -1,
                _ => {}
            }
            if level < 0 {
                return false;
            }
        }
        self.events.len() < count + 1
    }

    fn increase_indent(&mut self, flow: bool, indentless: bool) {
        self.indents.push(self.indent);
        match self.indent {
            None => {
                self.indent = Some(if flow { self.best_indent } else { 0 });
            }
            Some(i) => {
                if !indentless {
                    self.indent = Some(i + self.best_indent);
                }
            }
        }
    }

    fn pop_indent(&mut self) {
        self.indent = self.indents.pop().unwrap_or(None);
    }

    fn pop_state(&mut self) {
        self.state = self.states.pop().unwrap_or(State::Nothing);
    }

    fn run_state(&mut self, ev: &EEvent) -> EResult<()> {
        match self.state {
            State::StreamStart => self.expect_stream_start(ev),
            State::FirstDocumentStart => self.expect_document_start(ev, true),
            State::DocumentStart => self.expect_document_start(ev, false),
            State::DocumentEnd => self.expect_document_end(ev),
            State::DocumentRoot => self.expect_document_root(ev),
            State::FlowSequenceFirstItem => self.expect_first_flow_sequence_item(ev),
            State::FlowSequenceItem => self.expect_flow_sequence_item(ev),
            State::FlowMappingFirstKey => self.expect_first_flow_mapping_key(ev),
            State::FlowMappingKey => self.expect_flow_mapping_key(ev),
            State::FlowMappingSimpleValue => self.expect_flow_mapping_simple_value(ev),
            State::FlowMappingValue => self.expect_flow_mapping_value(ev),
            State::BlockSequenceFirstItem => self.expect_block_sequence_item(ev, true),
            State::BlockSequenceItem => self.expect_block_sequence_item(ev, false),
            State::BlockMappingFirstKey => self.expect_block_mapping_key(ev, true),
            State::BlockMappingKey => self.expect_block_mapping_key(ev, false),
            State::BlockMappingSimpleValue => self.expect_block_mapping_simple_value(ev),
            State::BlockMappingValue => self.expect_block_mapping_value(ev),
            State::Nothing => Err(emitter_error(format!("expected nothing, but got {ev:?}"))),
        }
    }

    // ---- stream and document ----

    fn expect_stream_start(&mut self, ev: &EEvent) -> EResult<()> {
        if matches!(ev, EEvent::StreamStart) {
            self.state = State::FirstDocumentStart;
            Ok(())
        } else {
            Err(emitter_error(format!(
                "expected StreamStartEvent, but got {ev:?}"
            )))
        }
    }

    fn expect_document_start(&mut self, ev: &EEvent, first: bool) -> EResult<()> {
        match ev {
            EEvent::DocumentStart { explicit } => {
                self.tag_prefixes = vec![
                    ("!".to_string(), "!".to_string()),
                    ("tag:yaml.org,2002:".to_string(), "!!".to_string()),
                ];
                let implicit = first && !*explicit && !self.check_empty_document();
                if !implicit {
                    self.write_indent();
                    self.write_indicator("---", true, false, false);
                }
                self.state = State::DocumentRoot;
                Ok(())
            }
            EEvent::StreamEnd => {
                if self.open_ended {
                    self.write_indicator("...", true, false, false);
                    self.write_indent();
                }
                self.state = State::Nothing;
                Ok(())
            }
            other => Err(emitter_error(format!(
                "expected DocumentStartEvent, but got {other:?}"
            ))),
        }
    }

    fn expect_document_end(&mut self, ev: &EEvent) -> EResult<()> {
        if let EEvent::DocumentEnd { explicit } = ev {
            self.write_indent();
            if *explicit {
                self.write_indicator("...", true, false, false);
                self.write_indent();
            }
            self.state = State::DocumentStart;
            Ok(())
        } else {
            Err(emitter_error(format!(
                "expected DocumentEndEvent, but got {ev:?}"
            )))
        }
    }

    fn expect_document_root(&mut self, ev: &EEvent) -> EResult<()> {
        self.states.push(State::DocumentEnd);
        self.expect_node(ev, true, false, false, false)
    }

    // ---- nodes ----

    fn expect_node(
        &mut self,
        ev: &EEvent,
        root: bool,
        sequence: bool,
        mapping: bool,
        simple_key: bool,
    ) -> EResult<()> {
        self.root_context = root;
        self.sequence_context = sequence;
        self.mapping_context = mapping;
        self.simple_key_context = simple_key;
        match ev {
            EEvent::Alias { anchor } => {
                if anchor.is_empty() {
                    return Err(emitter_error("anchor is not specified for alias"));
                }
                self.process_anchor("*", ev)?;
                self.pop_state();
                Ok(())
            }
            EEvent::Scalar { .. } | EEvent::SequenceStart { .. } | EEvent::MappingStart { .. } => {
                self.process_anchor("&", ev)?;
                self.process_tag(ev)?;
                match ev {
                    EEvent::Scalar { .. } => self.expect_scalar(ev),
                    EEvent::SequenceStart { flow_style, .. } => {
                        if self.flow_level > 0 || *flow_style || self.check_empty_sequence() {
                            self.expect_flow_sequence();
                        } else {
                            self.expect_block_sequence();
                        }
                        Ok(())
                    }
                    EEvent::MappingStart { flow_style, .. } => {
                        if self.flow_level > 0 || *flow_style || self.check_empty_mapping() {
                            self.expect_flow_mapping();
                        } else {
                            self.expect_block_mapping();
                        }
                        Ok(())
                    }
                    _ => Ok(()),
                }
            }
            other => Err(emitter_error(format!(
                "expected NodeEvent, but got {other:?}"
            ))),
        }
    }

    fn expect_scalar(&mut self, ev: &EEvent) -> EResult<()> {
        self.increase_indent(true, false);
        self.process_scalar(ev)?;
        self.pop_indent();
        self.pop_state();
        Ok(())
    }

    // ---- flow sequences ----

    fn expect_flow_sequence(&mut self) {
        self.write_indicator("[", true, true, false);
        self.flow_level += 1;
        self.increase_indent(true, false);
        self.state = State::FlowSequenceFirstItem;
    }

    fn expect_first_flow_sequence_item(&mut self, ev: &EEvent) -> EResult<()> {
        if matches!(ev, EEvent::SequenceEnd) {
            self.pop_indent();
            self.flow_level -= 1;
            self.write_indicator("]", false, false, false);
            self.pop_state();
            Ok(())
        } else {
            if self.column > self.best_width {
                self.write_indent();
            }
            self.states.push(State::FlowSequenceItem);
            self.expect_node(ev, false, true, false, false)
        }
    }

    fn expect_flow_sequence_item(&mut self, ev: &EEvent) -> EResult<()> {
        if matches!(ev, EEvent::SequenceEnd) {
            self.pop_indent();
            self.flow_level -= 1;
            self.write_indicator("]", false, false, false);
            self.pop_state();
            Ok(())
        } else {
            self.write_indicator(",", false, false, false);
            if self.column > self.best_width {
                self.write_indent();
            }
            self.states.push(State::FlowSequenceItem);
            self.expect_node(ev, false, true, false, false)
        }
    }

    // ---- flow mappings ----

    fn expect_flow_mapping(&mut self) {
        self.write_indicator("{", true, true, false);
        self.flow_level += 1;
        self.increase_indent(true, false);
        self.state = State::FlowMappingFirstKey;
    }

    fn expect_first_flow_mapping_key(&mut self, ev: &EEvent) -> EResult<()> {
        if matches!(ev, EEvent::MappingEnd) {
            self.pop_indent();
            self.flow_level -= 1;
            self.write_indicator("}", false, false, false);
            self.pop_state();
            Ok(())
        } else {
            if self.column > self.best_width {
                self.write_indent();
            }
            if self.check_simple_key(ev) {
                self.states.push(State::FlowMappingSimpleValue);
                self.expect_node(ev, false, false, true, true)
            } else {
                self.write_indicator("?", true, false, false);
                self.states.push(State::FlowMappingValue);
                self.expect_node(ev, false, false, true, false)
            }
        }
    }

    fn expect_flow_mapping_key(&mut self, ev: &EEvent) -> EResult<()> {
        if matches!(ev, EEvent::MappingEnd) {
            self.pop_indent();
            self.flow_level -= 1;
            self.write_indicator("}", false, false, false);
            self.pop_state();
            Ok(())
        } else {
            self.write_indicator(",", false, false, false);
            if self.column > self.best_width {
                self.write_indent();
            }
            if self.check_simple_key(ev) {
                self.states.push(State::FlowMappingSimpleValue);
                self.expect_node(ev, false, false, true, true)
            } else {
                self.write_indicator("?", true, false, false);
                self.states.push(State::FlowMappingValue);
                self.expect_node(ev, false, false, true, false)
            }
        }
    }

    fn expect_flow_mapping_simple_value(&mut self, ev: &EEvent) -> EResult<()> {
        self.write_indicator(":", false, false, false);
        self.states.push(State::FlowMappingKey);
        self.expect_node(ev, false, false, true, false)
    }

    fn expect_flow_mapping_value(&mut self, ev: &EEvent) -> EResult<()> {
        if self.column > self.best_width {
            self.write_indent();
        }
        self.write_indicator(":", true, false, false);
        self.states.push(State::FlowMappingKey);
        self.expect_node(ev, false, false, true, false)
    }

    // ---- block sequences ----

    fn expect_block_sequence(&mut self) {
        let indentless = self.mapping_context && !self.indention;
        self.increase_indent(false, indentless);
        self.state = State::BlockSequenceFirstItem;
    }

    fn expect_block_sequence_item(&mut self, ev: &EEvent, first: bool) -> EResult<()> {
        if !first && matches!(ev, EEvent::SequenceEnd) {
            self.pop_indent();
            self.pop_state();
            Ok(())
        } else {
            self.write_indent();
            self.write_indicator("-", true, false, true);
            self.states.push(State::BlockSequenceItem);
            self.expect_node(ev, false, true, false, false)
        }
    }

    // ---- block mappings ----

    fn expect_block_mapping(&mut self) {
        self.increase_indent(false, false);
        self.state = State::BlockMappingFirstKey;
    }

    fn expect_block_mapping_key(&mut self, ev: &EEvent, first: bool) -> EResult<()> {
        if !first && matches!(ev, EEvent::MappingEnd) {
            self.pop_indent();
            self.pop_state();
            Ok(())
        } else {
            self.write_indent();
            if self.check_simple_key(ev) {
                self.states.push(State::BlockMappingSimpleValue);
                self.expect_node(ev, false, false, true, true)
            } else {
                self.write_indicator("?", true, false, true);
                self.states.push(State::BlockMappingValue);
                self.expect_node(ev, false, false, true, false)
            }
        }
    }

    fn expect_block_mapping_simple_value(&mut self, ev: &EEvent) -> EResult<()> {
        self.write_indicator(":", false, false, false);
        self.states.push(State::BlockMappingKey);
        self.expect_node(ev, false, false, true, false)
    }

    fn expect_block_mapping_value(&mut self, ev: &EEvent) -> EResult<()> {
        self.write_indent();
        self.write_indicator(":", true, false, true);
        self.states.push(State::BlockMappingKey);
        self.expect_node(ev, false, false, true, false)
    }

    // ---- checkers ----

    fn check_empty_sequence(&self) -> bool {
        matches!(self.events.front(), Some(EEvent::SequenceEnd))
    }

    fn check_empty_mapping(&self) -> bool {
        matches!(self.events.front(), Some(EEvent::MappingEnd))
    }

    fn check_empty_document(&self) -> bool {
        match self.events.front() {
            Some(EEvent::Scalar {
                anchor: None,
                tag: None,
                implicit,
                value,
                ..
            }) => implicit.0 && value.is_empty(),
            _ => false,
        }
    }

    fn check_simple_key(&mut self, ev: &EEvent) -> bool {
        let mut length = 0usize;
        let anchor = match ev {
            EEvent::Alias { anchor } => Some(anchor),
            EEvent::Scalar { anchor, .. }
            | EEvent::SequenceStart { anchor, .. }
            | EEvent::MappingStart { anchor, .. } => anchor.as_ref(),
            _ => None,
        };
        if let Some(a) = anchor {
            if self.prepared_anchor.is_none() {
                self.prepared_anchor = self.prepare_anchor(a).ok();
            }
            length += self
                .prepared_anchor
                .as_ref()
                .map_or(0, |p| p.chars().count());
        }
        let tag = match ev {
            EEvent::Scalar { tag, .. }
            | EEvent::SequenceStart { tag, .. }
            | EEvent::MappingStart { tag, .. } => tag.as_ref(),
            _ => None,
        };
        if let Some(t) = tag {
            if self.prepared_tag.is_none() {
                self.prepared_tag = self.prepare_tag(t).ok();
            }
            length += self.prepared_tag.as_ref().map_or(0, |p| p.chars().count());
        }
        if let EEvent::Scalar { value, .. } = ev {
            if self.analysis.is_none() {
                self.analysis = Some(self.analyze_scalar(value));
            }
            length += self.analysis.as_ref().map_or(0, |a| a.scalar.len());
        }
        length < 128
            && (matches!(ev, EEvent::Alias { .. })
                || (matches!(ev, EEvent::Scalar { .. })
                    && self
                        .analysis
                        .as_ref()
                        .is_some_and(|a| !a.empty && !a.multiline))
                || (matches!(ev, EEvent::SequenceStart { .. }) && self.check_empty_sequence())
                || (matches!(ev, EEvent::MappingStart { .. }) && self.check_empty_mapping()))
    }

    // ---- processors ----

    fn process_anchor(&mut self, indicator: &str, ev: &EEvent) -> EResult<()> {
        let anchor = match ev {
            EEvent::Alias { anchor } => Some(anchor.clone()),
            EEvent::Scalar { anchor, .. }
            | EEvent::SequenceStart { anchor, .. }
            | EEvent::MappingStart { anchor, .. } => anchor.clone(),
            _ => None,
        };
        let Some(anchor) = anchor else {
            self.prepared_anchor = None;
            return Ok(());
        };
        if self.prepared_anchor.is_none() {
            self.prepared_anchor = Some(self.prepare_anchor(&anchor)?);
        }
        if let Some(p) = self.prepared_anchor.clone() {
            if !p.is_empty() {
                self.write_indicator(&format!("{indicator}{p}"), true, false, false);
            }
        }
        self.prepared_anchor = None;
        Ok(())
    }

    fn process_tag(&mut self, ev: &EEvent) -> EResult<()> {
        let mut tag: Option<String>;
        match ev {
            EEvent::Scalar {
                tag: t, implicit, ..
            } => {
                tag = t.clone();
                if self.style.is_none() {
                    self.style = Some(self.choose_scalar_style(ev));
                }
                let plain = self.style == Some(Style::Plain);
                if (plain && implicit.0) || (!plain && implicit.1) {
                    self.prepared_tag = None;
                    return Ok(());
                }
                if implicit.0 && tag.is_none() {
                    tag = Some("!".to_string());
                    self.prepared_tag = None;
                }
            }
            EEvent::SequenceStart {
                tag: t, implicit, ..
            }
            | EEvent::MappingStart {
                tag: t, implicit, ..
            } => {
                tag = t.clone();
                if *implicit {
                    self.prepared_tag = None;
                    return Ok(());
                }
            }
            _ => return Ok(()),
        }
        let Some(tag) = tag else {
            return Err(emitter_error("tag is not specified"));
        };
        if self.prepared_tag.is_none() {
            self.prepared_tag = Some(self.prepare_tag(&tag)?);
        }
        if let Some(p) = self.prepared_tag.clone() {
            if !p.is_empty() {
                self.write_indicator(&p, true, false, false);
            }
        }
        self.prepared_tag = None;
        Ok(())
    }

    fn choose_scalar_style(&mut self, ev: &EEvent) -> Style {
        let EEvent::Scalar {
            implicit,
            value,
            style: event_style,
            ..
        } = ev
        else {
            return Style::Double;
        };
        if self.analysis.is_none() {
            self.analysis = Some(self.analyze_scalar(value));
        }
        let analysis = self.analysis.clone().expect("analysis was just computed");
        if *event_style == Some('"') {
            return Style::Double;
        }
        if event_style.is_none()
            && implicit.0
            && !(self.simple_key_context && (analysis.empty || analysis.multiline))
            && ((self.flow_level > 0 && analysis.allow_flow_plain)
                || (self.flow_level == 0 && analysis.allow_block_plain))
        {
            return Style::Plain;
        }
        if let Some(s) = event_style {
            if (*s == '|' || *s == '>')
                && self.flow_level == 0
                && !self.simple_key_context
                && analysis.allow_block
            {
                return if *s == '|' {
                    Style::Literal
                } else {
                    Style::Folded
                };
            }
        }
        if (event_style.is_none() || *event_style == Some('\''))
            && analysis.allow_single_quoted
            && !(self.simple_key_context && analysis.multiline)
        {
            return Style::Single;
        }
        Style::Double
    }

    fn process_scalar(&mut self, ev: &EEvent) -> EResult<()> {
        let EEvent::Scalar { value, .. } = ev else {
            return Ok(());
        };
        if self.analysis.is_none() {
            self.analysis = Some(self.analyze_scalar(value));
        }
        if self.style.is_none() {
            self.style = Some(self.choose_scalar_style(ev));
        }
        let split = !self.simple_key_context;
        let text = self
            .analysis
            .as_ref()
            .map(|a| a.scalar.clone())
            .unwrap_or_default();
        match self.style {
            Some(Style::Double) => self.write_double_quoted(&text, split),
            Some(Style::Single) => self.write_single_quoted(&text, split),
            Some(Style::Folded) => self.write_folded(&text),
            Some(Style::Literal) => self.write_literal(&text),
            _ => self.write_plain(&text, split),
        }
        self.analysis = None;
        self.style = None;
        Ok(())
    }

    // ---- analyzers ----

    fn prepare_tag(&self, tag: &str) -> EResult<String> {
        if tag.is_empty() {
            return Err(emitter_error("tag must not be empty"));
        }
        if tag == "!" {
            return Ok(tag.to_string());
        }
        let mut handle: Option<&str> = None;
        let mut suffix: &str = tag;
        let mut prefixes: Vec<&(String, String)> = self.tag_prefixes.iter().collect();
        prefixes.sort_by(|a, b| a.0.cmp(&b.0));
        for (prefix, h) in prefixes {
            if tag.starts_with(prefix.as_str()) && (prefix == "!" || prefix.len() < tag.len()) {
                handle = Some(h);
                suffix = &tag[prefix.len()..];
            }
        }
        let mut out = String::new();
        for ch in suffix.chars() {
            if ch.is_ascii_alphanumeric()
                || "-;/?:@&=+$,_.~*'()[]".contains(ch)
                || (ch == '!' && handle != Some("!"))
            {
                out.push(ch);
            } else {
                let mut buf = [0u8; 4];
                for b in ch.encode_utf8(&mut buf).bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
        }
        match handle {
            Some(h) if !h.is_empty() => Ok(format!("{h}{out}")),
            _ => Ok(format!("!<{out}>")),
        }
    }

    fn prepare_anchor(&self, anchor: &str) -> EResult<String> {
        if anchor.is_empty() {
            return Err(emitter_error("anchor must not be empty"));
        }
        for ch in anchor.chars() {
            if !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_') {
                return Err(emitter_error(format!(
                    "invalid character {:?} in the anchor: {anchor:?}",
                    ch
                )));
            }
        }
        Ok(anchor.to_string())
    }

    fn analyze_scalar(&self, scalar_str: &str) -> ScalarAnalysis {
        let scalar: Vec<char> = scalar_str.chars().collect();
        if scalar.is_empty() {
            return ScalarAnalysis {
                scalar,
                empty: true,
                multiline: false,
                allow_flow_plain: false,
                allow_block_plain: true,
                allow_single_quoted: true,
                allow_block: false,
            };
        }
        let mut block_indicators = false;
        let mut flow_indicators = false;
        let mut line_breaks = false;
        let mut special_characters = false;
        let mut leading_space = false;
        let mut leading_break = false;
        let mut trailing_space = false;
        let mut trailing_break = false;
        let mut break_space = false;
        let mut space_break = false;

        let starts = |p: &str| scalar_str.starts_with(p);
        if starts("---") || starts("...") {
            block_indicators = true;
            flow_indicators = true;
        }
        let ws = |c: char| "\0 \t\r\n\u{85}\u{2028}\u{2029}".contains(c);
        let mut preceded_by_whitespace = true;
        let mut followed_by_whitespace = scalar.len() == 1 || ws(scalar[1]);
        let mut previous_space = false;
        let mut previous_break = false;
        let mut index = 0;
        while index < scalar.len() {
            let ch = scalar[index];
            if index == 0 {
                if "#,[]{}&*!|>'\"%@`".contains(ch) {
                    flow_indicators = true;
                    block_indicators = true;
                }
                if ch == '?' || ch == ':' {
                    flow_indicators = true;
                    if followed_by_whitespace {
                        block_indicators = true;
                    }
                }
                if ch == '-' && followed_by_whitespace {
                    flow_indicators = true;
                    block_indicators = true;
                }
            } else {
                if ",?[]{}".contains(ch) {
                    flow_indicators = true;
                }
                if ch == ':' {
                    flow_indicators = true;
                    if followed_by_whitespace {
                        block_indicators = true;
                    }
                }
                if ch == '#' && preceded_by_whitespace {
                    flow_indicators = true;
                    block_indicators = true;
                }
            }
            if is_break(ch) {
                line_breaks = true;
            }
            if !(ch == '\n' || ('\x20'..='\x7E').contains(&ch)) {
                if (ch == '\u{85}'
                    || ('\u{a0}'..='\u{d7ff}').contains(&ch)
                    || ('\u{e000}'..='\u{fffd}').contains(&ch)
                    || ('\u{10000}'..'\u{10ffff}').contains(&ch))
                    && ch != '\u{feff}'
                {
                    if !self.allow_unicode {
                        special_characters = true;
                    }
                } else {
                    special_characters = true;
                }
            }
            if ch == ' ' {
                if index == 0 {
                    leading_space = true;
                }
                if index == scalar.len() - 1 {
                    trailing_space = true;
                }
                if previous_break {
                    break_space = true;
                }
                previous_space = true;
                previous_break = false;
            } else if is_break(ch) {
                if index == 0 {
                    leading_break = true;
                }
                if index == scalar.len() - 1 {
                    trailing_break = true;
                }
                if previous_space {
                    space_break = true;
                }
                previous_space = false;
                previous_break = true;
            } else {
                previous_space = false;
                previous_break = false;
            }
            index += 1;
            preceded_by_whitespace = ws(ch);
            followed_by_whitespace = index + 1 >= scalar.len() || ws(scalar[index + 1]);
        }

        let mut allow_flow_plain = true;
        let mut allow_block_plain = true;
        let mut allow_single_quoted = true;
        let mut allow_block = true;
        if leading_space || leading_break || trailing_space || trailing_break {
            allow_flow_plain = false;
            allow_block_plain = false;
        }
        if trailing_space {
            allow_block = false;
        }
        if break_space {
            allow_flow_plain = false;
            allow_block_plain = false;
            allow_single_quoted = false;
        }
        if space_break || special_characters {
            allow_flow_plain = false;
            allow_block_plain = false;
            allow_single_quoted = false;
            allow_block = false;
        }
        if line_breaks {
            allow_flow_plain = false;
            allow_block_plain = false;
        }
        if flow_indicators {
            allow_flow_plain = false;
        }
        if block_indicators {
            allow_block_plain = false;
        }
        ScalarAnalysis {
            scalar,
            empty: false,
            multiline: line_breaks,
            allow_flow_plain,
            allow_block_plain,
            allow_single_quoted,
            allow_block,
        }
    }

    // ---- writers ----

    fn write_str(&mut self, data: &str) {
        self.out.push_str(data);
    }

    fn write_indicator(
        &mut self,
        indicator: &str,
        need_whitespace: bool,
        whitespace: bool,
        indention: bool,
    ) {
        let data = if self.whitespace || !need_whitespace {
            indicator.to_string()
        } else {
            format!(" {indicator}")
        };
        self.whitespace = whitespace;
        self.indention = self.indention && indention;
        self.column += data.chars().count();
        self.open_ended = false;
        self.out.push_str(&data);
    }

    fn write_indent(&mut self) {
        let indent = self.indent.unwrap_or(0);
        if !self.indention || self.column > indent || (self.column == indent && !self.whitespace) {
            self.write_line_break(None);
        }
        if self.column < indent {
            self.whitespace = true;
            let data = " ".repeat(indent - self.column);
            self.column = indent;
            self.out.push_str(&data);
        }
    }

    fn write_line_break(&mut self, data: Option<char>) {
        if self.double_top_level_breaks && self.indents.len() == 1 {
            self.base_write_line_break(data);
        }
        self.base_write_line_break(data);
    }

    fn base_write_line_break(&mut self, data: Option<char>) {
        match data {
            Some(c) => self.out.push(c),
            None => {
                let b = self.best_line_break.clone();
                self.out.push_str(&b);
            }
        }
        self.whitespace = true;
        self.indention = true;
        self.column = 0;
    }

    fn write_single_quoted(&mut self, text: &[char], split: bool) {
        self.write_indicator("'", true, false, false);
        let mut spaces = false;
        let mut breaks = false;
        let mut start = 0usize;
        let mut end = 0usize;
        while end <= text.len() {
            let ch = text.get(end).copied();
            if spaces {
                if ch != Some(' ') {
                    if start + 1 == end
                        && self.column > self.best_width
                        && split
                        && start != 0
                        && end != text.len()
                    {
                        self.write_indent();
                    } else {
                        let data = slice(text, start, end);
                        self.column += data.chars().count();
                        self.write_str(&data);
                    }
                    start = end;
                }
            } else if breaks {
                if ch.is_none_or(|c| !is_break(c)) {
                    if text.get(start) == Some(&'\n') {
                        self.write_line_break(None);
                    }
                    for &br in &text[start..end.min(text.len())] {
                        if br == '\n' {
                            self.write_line_break(None);
                        } else {
                            self.write_line_break(Some(br));
                        }
                    }
                    self.write_indent();
                    start = end;
                }
            } else if ch.is_none_or(|c| c == ' ' || is_break(c) || c == '\'') && start < end {
                let data = slice(text, start, end);
                self.column += data.chars().count();
                self.write_str(&data);
                start = end;
            }
            if ch == Some('\'') {
                self.column += 2;
                self.write_str("''");
                start = end + 1;
            }
            if let Some(c) = ch {
                spaces = c == ' ';
                breaks = is_break(c);
            }
            end += 1;
        }
        self.write_indicator("'", false, false, false);
    }

    fn write_double_quoted(&mut self, text: &[char], split: bool) {
        self.write_indicator("\"", true, false, false);
        let mut start = 0usize;
        let mut end = 0usize;
        while end <= text.len() {
            let ch = text.get(end).copied();
            let needs_escape = match ch {
                None => true,
                Some(c) => {
                    "\"\\\u{85}\u{2028}\u{2029}\u{feff}".contains(c)
                        || !(('\x20'..='\x7E').contains(&c)
                            || (self.allow_unicode
                                && (('\u{a0}'..='\u{d7ff}').contains(&c)
                                    || ('\u{e000}'..='\u{fffd}').contains(&c))))
                }
            };
            if needs_escape {
                if start < end {
                    let data = slice(text, start, end);
                    self.column += data.chars().count();
                    self.write_str(&data);
                    start = end;
                }
                if let Some(c) = ch {
                    let data = match escape_replacement(c) {
                        Some(r) => format!("\\{r}"),
                        None if (c as u32) <= 0xFF => format!("\\x{:02X}", c as u32),
                        None if (c as u32) <= 0xFFFF => format!("\\u{:04X}", c as u32),
                        None => format!("\\U{:08X}", c as u32),
                    };
                    self.column += data.chars().count();
                    self.write_str(&data);
                    start = end + 1;
                }
            }
            if 0 < end
                && end + 1 < text.len()
                && (ch == Some(' ') || start >= end)
                && (self.column as i64) + (end as i64 - start as i64) > self.best_width as i64
                && split
            {
                let mut data = slice(text, start, end);
                data.push('\\');
                if start < end {
                    start = end;
                }
                self.column += data.chars().count();
                self.write_str(&data);
                self.write_indent();
                self.whitespace = false;
                self.indention = false;
                if text.get(start) == Some(&' ') {
                    self.column += 1;
                    self.write_str("\\");
                }
            }
            end += 1;
        }
        self.write_indicator("\"", false, false, false);
    }

    fn determine_block_hints(&self, text: &[char]) -> String {
        let mut hints = String::new();
        if let (Some(&first), Some(&last)) = (text.first(), text.last()) {
            if " \n\u{85}\u{2028}\u{2029}".contains(first) {
                hints.push_str(&self.best_indent.to_string());
            }
            if !is_break(last) {
                hints.push('-');
            } else if text.len() == 1 || is_break(text[text.len() - 2]) {
                hints.push('+');
            }
        }
        hints
    }

    fn write_folded(&mut self, text: &[char]) {
        let hints = self.determine_block_hints(text);
        self.write_indicator(&format!(">{hints}"), true, false, false);
        if hints.ends_with('+') {
            self.open_ended = true;
        }
        self.write_line_break(None);
        let mut leading_space = true;
        let mut spaces = false;
        let mut breaks = true;
        let mut start = 0usize;
        let mut end = 0usize;
        while end <= text.len() {
            let ch = text.get(end).copied();
            if breaks {
                if ch.is_none_or(|c| !is_break(c)) {
                    if !leading_space
                        && ch.is_some_and(|c| c != ' ')
                        && text.get(start) == Some(&'\n')
                    {
                        self.write_line_break(None);
                    }
                    leading_space = ch == Some(' ');
                    for &br in &text[start.min(text.len())..end.min(text.len())] {
                        if br == '\n' {
                            self.write_line_break(None);
                        } else {
                            self.write_line_break(Some(br));
                        }
                    }
                    if ch.is_some() {
                        self.write_indent();
                    }
                    start = end;
                }
            } else if spaces {
                if ch != Some(' ') {
                    if start + 1 == end && self.column > self.best_width {
                        self.write_indent();
                    } else {
                        let data = slice(text, start, end);
                        self.column += data.chars().count();
                        self.write_str(&data);
                    }
                    start = end;
                }
            } else if ch.is_none_or(|c| c == ' ' || is_break(c)) {
                let data = slice(text, start, end);
                self.column += data.chars().count();
                self.write_str(&data);
                if ch.is_none() {
                    self.write_line_break(None);
                }
                start = end;
            }
            if let Some(c) = ch {
                breaks = is_break(c);
                spaces = c == ' ';
            }
            end += 1;
        }
    }

    fn write_literal(&mut self, text: &[char]) {
        let hints = self.determine_block_hints(text);
        self.write_indicator(&format!("|{hints}"), true, false, false);
        if hints.ends_with('+') {
            self.open_ended = true;
        }
        self.write_line_break(None);
        let mut breaks = true;
        let mut start = 0usize;
        let mut end = 0usize;
        while end <= text.len() {
            let ch = text.get(end).copied();
            if breaks {
                if ch.is_none_or(|c| !is_break(c)) {
                    for &br in &text[start.min(text.len())..end.min(text.len())] {
                        if br == '\n' {
                            self.write_line_break(None);
                        } else {
                            self.write_line_break(Some(br));
                        }
                    }
                    if ch.is_some() {
                        self.write_indent();
                    }
                    start = end;
                }
            } else if ch.is_none_or(is_break) {
                let data = slice(text, start, end);
                self.write_str(&data);
                if ch.is_none() {
                    self.write_line_break(None);
                }
                start = end;
            }
            if let Some(c) = ch {
                breaks = is_break(c);
            }
            end += 1;
        }
    }

    fn write_plain(&mut self, text: &[char], split: bool) {
        if self.root_context {
            self.open_ended = true;
        }
        if text.is_empty() {
            return;
        }
        if !self.whitespace {
            self.column += 1;
            self.write_str(" ");
        }
        self.whitespace = false;
        self.indention = false;
        let mut spaces = false;
        let mut breaks = false;
        let mut start = 0usize;
        let mut end = 0usize;
        while end <= text.len() {
            let ch = text.get(end).copied();
            if spaces {
                if ch != Some(' ') {
                    if start + 1 == end && self.column > self.best_width && split {
                        self.write_indent();
                        self.whitespace = false;
                        self.indention = false;
                    } else {
                        let data = slice(text, start, end);
                        self.column += data.chars().count();
                        self.write_str(&data);
                    }
                    start = end;
                }
            } else if breaks {
                if ch.is_none_or(|c| !is_break(c)) {
                    if text.get(start) == Some(&'\n') {
                        self.write_line_break(None);
                    }
                    for &br in &text[start..end.min(text.len())] {
                        if br == '\n' {
                            self.write_line_break(None);
                        } else {
                            self.write_line_break(Some(br));
                        }
                    }
                    self.write_indent();
                    self.whitespace = false;
                    self.indention = false;
                    start = end;
                }
            } else if ch.is_none_or(|c| c == ' ' || is_break(c)) {
                let data = slice(text, start, end);
                self.column += data.chars().count();
                self.write_str(&data);
                start = end;
            }
            if let Some(c) = ch {
                spaces = c == ' ';
                breaks = is_break(c);
            }
            end += 1;
        }
    }
}

fn escape_replacement(c: char) -> Option<char> {
    Some(match c {
        '\0' => '0',
        '\x07' => 'a',
        '\x08' => 'b',
        '\x09' => 't',
        '\x0A' => 'n',
        '\x0B' => 'v',
        '\x0C' => 'f',
        '\x0D' => 'r',
        '\x1B' => 'e',
        '"' => '"',
        '\\' => '\\',
        '\u{85}' => 'N',
        '\u{a0}' => '_',
        '\u{2028}' => 'L',
        '\u{2029}' => 'P',
        _ => return None,
    })
}
