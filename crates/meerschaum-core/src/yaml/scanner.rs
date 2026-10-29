//! A port of PyYAML's `reader.py` and `scanner.py` (PyYAML 6.0.3, pure-Python implementation).
//!
//! The scanner turns the character stream into tokens exactly the way PyYAML does, including
//! the error messages (`while scanning a simple key`, `found character '\t' that cannot start
//! any token`, ...), because those messages reach users through `read_config` warnings.

use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use crate::pyfmt::str_repr;

/// A position in the source (`yaml.error.Mark`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mark {
    /// The character offset.
    pub index: usize,
    /// The zero-based line.
    pub line: usize,
    /// The zero-based column.
    pub column: usize,
}

/// The input text, shared by every component that formats an error.
#[derive(Debug)]
pub struct Source {
    /// `<unicode string>`, `<byte string>` or the stream's name.
    pub name: String,
    /// The characters, terminated by a `'\0'` sentinel like the PyYAML reader's buffer.
    pub chars: Vec<char>,
    /// Whether error messages show a snippet (PyYAML shows none for file objects).
    pub snippets: bool,
}

impl Source {
    /// Build a source from text (the sentinel is appended).
    pub fn new(text: &str, name: &str, snippets: bool) -> Source {
        let mut chars: Vec<char> = text.chars().collect();
        chars.push('\0');
        Source {
            name: name.to_string(),
            chars,
            snippets,
        }
    }

    fn snippet(&self, mark: &Mark) -> Option<String> {
        if !self.snippets {
            return None;
        }
        let buffer = &self.chars;
        let pointer = mark.index;
        let max_length = 75.0_f64;
        let is_break =
            |c: char| matches!(c, '\0' | '\r' | '\n' | '\u{85}' | '\u{2028}' | '\u{2029}');
        let mut head = "";
        let mut start = pointer;
        while start > 0 && !is_break(buffer[start - 1]) {
            start -= 1;
            if (pointer - start) as f64 > max_length / 2.0 - 1.0 {
                head = " ... ";
                start += 5;
                break;
            }
        }
        let mut tail = "";
        let mut end = pointer;
        while end < buffer.len() && !is_break(buffer[end]) {
            end += 1;
            if (end - pointer) as f64 > max_length / 2.0 - 1.0 {
                tail = " ... ";
                end -= 5;
                break;
            }
        }
        let snippet: String = buffer[start..end].iter().collect();
        Some(format!(
            "    {head}{snippet}{tail}\n{}^",
            " ".repeat(4 + pointer - start + head.chars().count())
        ))
    }

    /// `str(mark)`.
    pub fn mark_str(&self, mark: &Mark) -> String {
        let mut where_ = format!(
            "  in \"{}\", line {}, column {}",
            self.name,
            mark.line + 1,
            mark.column + 1
        );
        if let Some(snippet) = self.snippet(mark) {
            where_.push_str(":\n");
            where_.push_str(&snippet);
        }
        where_
    }

    /// `str(MarkedYAMLError(context, context_mark, problem, problem_mark))`.
    pub fn marked(
        &self,
        context: Option<&str>,
        context_mark: Option<&Mark>,
        problem: Option<&str>,
        problem_mark: Option<&Mark>,
    ) -> String {
        let mut lines: Vec<String> = Vec::new();
        if let Some(c) = context {
            lines.push(c.to_string());
        }
        if let Some(cm) = context_mark {
            let show = match (problem, problem_mark) {
                (Some(_), Some(pm)) => cm.line != pm.line || cm.column != pm.column,
                _ => true,
            };
            if show {
                lines.push(self.mark_str(cm));
            }
        }
        if let Some(p) = problem {
            lines.push(p.to_string());
        }
        if let Some(pm) = problem_mark {
            lines.push(self.mark_str(pm));
        }
        lines.join("\n")
    }
}

/// Which PyYAML exception class an error belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// `yaml.reader.ReaderError`.
    Reader,
    /// `yaml.scanner.ScannerError`.
    Scanner,
    /// `yaml.parser.ParserError`.
    Parser,
    /// `yaml.composer.ComposerError`.
    Composer,
    /// `yaml.constructor.ConstructorError`.
    Constructor,
    /// `yaml.representer.RepresenterError`.
    Representer,
    /// `yaml.emitter.EmitterError`.
    Emitter,
    /// A Python `ValueError` raised from a constructor (`int()`, `datetime`, ...).
    Value,
    /// A Python `KeyError`.
    Key,
    /// A Python `IndexError`.
    Index,
    /// A Python `RecursionError`.
    Recursion,
    /// A Python `AttributeError`.
    Attribute,
}

/// A YAML error: the exception class and its `str()`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct PyYamlError {
    /// The PyYAML exception class.
    pub kind: ErrorKind,
    /// `str(exception)`.
    pub message: String,
}

pub(crate) type ScanResult<T> = Result<T, PyYamlError>;

/// The token kinds (`yaml.tokens`).
#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    /// `STREAM-START`.
    StreamStart,
    /// `STREAM-END`.
    StreamEnd,
    /// `DIRECTIVE(name, value)`.
    Directive(String, DirectiveValue),
    /// `DOCUMENT-START`.
    DocumentStart,
    /// `DOCUMENT-END`.
    DocumentEnd,
    /// `BLOCK-SEQUENCE-START`.
    BlockSequenceStart,
    /// `BLOCK-MAPPING-START`.
    BlockMappingStart,
    /// `BLOCK-END`.
    BlockEnd,
    /// `[`.
    FlowSequenceStart,
    /// `{`.
    FlowMappingStart,
    /// `]`.
    FlowSequenceEnd,
    /// `}`.
    FlowMappingEnd,
    /// `-`.
    BlockEntry,
    /// `,`.
    FlowEntry,
    /// `?` (explicit or implied by a simple key).
    Key,
    /// `:`.
    Value,
    /// `*alias`.
    Alias(String),
    /// `&anchor`.
    Anchor(String),
    /// `!handle!suffix`: `(handle, suffix)`.
    Tag(Option<String>, String),
    /// A scalar: value, plain flag and style (`'`, `"`, `|`, `>`).
    Scalar(String, bool, Option<char>),
}

/// The payload of a directive token.
#[derive(Debug, Clone, PartialEq)]
pub enum DirectiveValue {
    /// `%YAML major.minor`.
    Yaml(i64, i64),
    /// `%TAG handle prefix`.
    Tag(String, String),
    /// Any other directive.
    None,
}

impl TokenKind {
    /// The token's `id` as used in PyYAML's error messages.
    pub fn id(&self) -> &'static str {
        match self {
            TokenKind::StreamStart => "<stream start>",
            TokenKind::StreamEnd => "<stream end>",
            TokenKind::Directive(..) => "<directive>",
            TokenKind::DocumentStart => "<document start>",
            TokenKind::DocumentEnd => "<document end>",
            TokenKind::BlockSequenceStart => "<block sequence start>",
            TokenKind::BlockMappingStart => "<block mapping start>",
            TokenKind::BlockEnd => "<block end>",
            TokenKind::FlowSequenceStart => "[",
            TokenKind::FlowMappingStart => "{",
            TokenKind::FlowSequenceEnd => "]",
            TokenKind::FlowMappingEnd => "}",
            TokenKind::BlockEntry => "-",
            TokenKind::FlowEntry => ",",
            TokenKind::Key => "?",
            TokenKind::Value => ":",
            TokenKind::Alias(_) => "<alias>",
            TokenKind::Anchor(_) => "<anchor>",
            TokenKind::Tag(..) => "<tag>",
            TokenKind::Scalar(..) => "<scalar>",
        }
    }
}

/// A token with its marks.
#[derive(Debug, Clone)]
pub struct Token {
    /// What the token is.
    pub kind: TokenKind,
    /// Where it starts.
    pub start: Mark,
    /// Where it ends.
    pub end: Mark,
}

#[derive(Debug, Clone)]
struct SimpleKey {
    token_number: usize,
    required: bool,
    index: usize,
    line: usize,
    mark: Mark,
    column: usize,
}

const NUL_WS_BREAK: &str = "\0 \t\r\n\u{85}\u{2028}\u{2029}";
const NUL_SP_BREAK: &str = "\0 \r\n\u{85}\u{2028}\u{2029}";
const NUL_BREAK: &str = "\0\r\n\u{85}\u{2028}\u{2029}";

fn is_in(ch: char, set: &str) -> bool {
    set.contains(ch)
}

fn is_alnum_dash(ch: char) -> bool {
    ch.is_ascii_digit()
        || ch.is_ascii_uppercase()
        || ch.is_ascii_lowercase()
        || ch == '-'
        || ch == '_'
}

fn repr_char(ch: char) -> String {
    str_repr(&ch.to_string())
}

/// The scanner (`yaml.scanner.Scanner` plus the `Reader` it sits on).
pub struct Scanner {
    src: Rc<Source>,
    pointer: usize,
    index: usize,
    line: usize,
    column: usize,
    done: bool,
    flow_level: usize,
    tokens: VecDeque<Token>,
    tokens_taken: usize,
    indent: i64,
    indents: Vec<i64>,
    allow_simple_key: bool,
    possible_simple_keys: BTreeMap<usize, SimpleKey>,
}

impl Scanner {
    /// Create a scanner over `src` and queue the `STREAM-START` token.
    pub fn new(src: Rc<Source>) -> Scanner {
        let mut s = Scanner {
            src,
            pointer: 0,
            index: 0,
            line: 0,
            column: 0,
            done: false,
            flow_level: 0,
            tokens: VecDeque::new(),
            tokens_taken: 0,
            indent: -1,
            indents: Vec::new(),
            allow_simple_key: true,
            possible_simple_keys: BTreeMap::new(),
        };
        // fetch_stream_start
        let mark = s.get_mark();
        s.tokens.push_back(Token {
            kind: TokenKind::StreamStart,
            start: mark,
            end: mark,
        });
        s
    }

    /// The shared source.
    pub fn source(&self) -> &Rc<Source> {
        &self.src
    }

    // ---- reader ----

    fn peek_at(&self, i: usize) -> char {
        self.src
            .chars
            .get(self.pointer + i)
            .copied()
            .unwrap_or('\0')
    }

    fn peek(&self) -> char {
        self.peek_at(0)
    }

    fn prefix(&self, length: usize) -> String {
        let end = (self.pointer + length).min(self.src.chars.len());
        self.src.chars[self.pointer.min(end)..end].iter().collect()
    }

    fn forward_n(&mut self, length: usize) {
        for _ in 0..length {
            let ch = self.src.chars.get(self.pointer).copied().unwrap_or('\0');
            self.pointer += 1;
            self.index += 1;
            let next = self.src.chars.get(self.pointer).copied().unwrap_or('\0');
            if matches!(ch, '\n' | '\u{85}' | '\u{2028}' | '\u{2029}')
                || (ch == '\r' && next != '\n')
            {
                self.line += 1;
                self.column = 0;
            } else if ch != '\u{feff}' {
                self.column += 1;
            }
        }
    }

    fn forward(&mut self) {
        self.forward_n(1);
    }

    fn get_mark(&self) -> Mark {
        Mark {
            index: self.index,
            line: self.line,
            column: self.column,
        }
    }

    fn err(
        &self,
        context: Option<&str>,
        context_mark: Option<Mark>,
        problem: &str,
        problem_mark: Mark,
    ) -> PyYamlError {
        PyYamlError {
            kind: ErrorKind::Scanner,
            message: self.src.marked(
                context,
                context_mark.as_ref(),
                Some(problem),
                Some(&problem_mark),
            ),
        }
    }

    // ---- public methods ----

    /// The next token without consuming it (`peek_token`).
    pub fn peek_token(&mut self) -> ScanResult<Option<&Token>> {
        while self.need_more_tokens()? {
            self.fetch_more_tokens()?;
        }
        Ok(self.tokens.front())
    }

    /// The next token's kind, cloned.
    pub fn peek_kind(&mut self) -> ScanResult<Option<TokenKind>> {
        Ok(self.peek_token()?.map(|t| t.kind.clone()))
    }

    /// Consume and return the next token (`get_token`).
    pub fn get_token(&mut self) -> ScanResult<Option<Token>> {
        while self.need_more_tokens()? {
            self.fetch_more_tokens()?;
        }
        if let Some(t) = self.tokens.pop_front() {
            self.tokens_taken += 1;
            return Ok(Some(t));
        }
        Ok(None)
    }

    fn need_more_tokens(&mut self) -> ScanResult<bool> {
        if self.done {
            return Ok(false);
        }
        if self.tokens.is_empty() {
            return Ok(true);
        }
        self.stale_possible_simple_keys()?;
        Ok(self.next_possible_simple_key() == Some(self.tokens_taken))
    }

    fn fetch_more_tokens(&mut self) -> ScanResult<()> {
        self.scan_to_next_token();
        self.stale_possible_simple_keys()?;
        self.unwind_indent(self.column as i64);
        let ch = self.peek();
        if ch == '\0' {
            return self.fetch_stream_end();
        }
        if ch == '%' && self.check_directive() {
            return self.fetch_directive();
        }
        if ch == '-' && self.check_document_start() {
            return self.fetch_document_indicator(TokenKind::DocumentStart);
        }
        if ch == '.' && self.check_document_end() {
            return self.fetch_document_indicator(TokenKind::DocumentEnd);
        }
        if ch == '[' {
            return self.fetch_flow_collection_start(TokenKind::FlowSequenceStart);
        }
        if ch == '{' {
            return self.fetch_flow_collection_start(TokenKind::FlowMappingStart);
        }
        if ch == ']' {
            return self.fetch_flow_collection_end(TokenKind::FlowSequenceEnd);
        }
        if ch == '}' {
            return self.fetch_flow_collection_end(TokenKind::FlowMappingEnd);
        }
        if ch == ',' {
            return self.fetch_flow_entry();
        }
        if ch == '-' && self.check_block_entry() {
            return self.fetch_block_entry();
        }
        if ch == '?' && self.check_key() {
            return self.fetch_key();
        }
        if ch == ':' && self.check_value() {
            return self.fetch_value();
        }
        if ch == '*' {
            return self.fetch_alias_or_anchor(true);
        }
        if ch == '&' {
            return self.fetch_alias_or_anchor(false);
        }
        if ch == '!' {
            return self.fetch_tag();
        }
        if ch == '|' && self.flow_level == 0 {
            return self.fetch_block_scalar('|');
        }
        if ch == '>' && self.flow_level == 0 {
            return self.fetch_block_scalar('>');
        }
        if ch == '\'' {
            return self.fetch_flow_scalar('\'');
        }
        if ch == '"' {
            return self.fetch_flow_scalar('"');
        }
        if self.check_plain() {
            return self.fetch_plain();
        }
        Err(self.err(
            Some("while scanning for the next token"),
            None,
            &format!(
                "found character {} that cannot start any token",
                repr_char(ch)
            ),
            self.get_mark(),
        ))
    }

    // ---- simple keys ----

    fn next_possible_simple_key(&self) -> Option<usize> {
        self.possible_simple_keys
            .values()
            .map(|k| k.token_number)
            .min()
    }

    fn stale_possible_simple_keys(&mut self) -> ScanResult<()> {
        let levels: Vec<usize> = self.possible_simple_keys.keys().copied().collect();
        for level in levels {
            let key = self.possible_simple_keys[&level].clone();
            if key.line != self.line || self.index - key.index > 1024 {
                if key.required {
                    return Err(self.err(
                        Some("while scanning a simple key"),
                        Some(key.mark),
                        "could not find expected ':'",
                        self.get_mark(),
                    ));
                }
                self.possible_simple_keys.remove(&level);
            }
        }
        Ok(())
    }

    fn save_possible_simple_key(&mut self) -> ScanResult<()> {
        let required = self.flow_level == 0 && self.indent == self.column as i64;
        if self.allow_simple_key {
            self.remove_possible_simple_key()?;
            let token_number = self.tokens_taken + self.tokens.len();
            let key = SimpleKey {
                token_number,
                required,
                index: self.index,
                line: self.line,
                column: self.column,
                mark: self.get_mark(),
            };
            self.possible_simple_keys.insert(self.flow_level, key);
        }
        Ok(())
    }

    fn remove_possible_simple_key(&mut self) -> ScanResult<()> {
        if let Some(key) = self.possible_simple_keys.get(&self.flow_level) {
            if key.required {
                return Err(self.err(
                    Some("while scanning a simple key"),
                    Some(key.mark),
                    "could not find expected ':'",
                    self.get_mark(),
                ));
            }
            self.possible_simple_keys.remove(&self.flow_level);
        }
        Ok(())
    }

    // ---- indentation ----

    fn unwind_indent(&mut self, column: i64) {
        if self.flow_level > 0 {
            return;
        }
        while self.indent > column {
            let mark = self.get_mark();
            self.indent = self.indents.pop().unwrap_or(-1);
            self.tokens.push_back(Token {
                kind: TokenKind::BlockEnd,
                start: mark,
                end: mark,
            });
        }
    }

    fn add_indent(&mut self, column: i64) -> bool {
        if self.indent < column {
            self.indents.push(self.indent);
            self.indent = column;
            return true;
        }
        false
    }

    // ---- fetchers ----

    fn push(&mut self, kind: TokenKind, start: Mark, end: Mark) {
        self.tokens.push_back(Token { kind, start, end });
    }

    fn fetch_stream_end(&mut self) -> ScanResult<()> {
        self.unwind_indent(-1);
        self.remove_possible_simple_key()?;
        self.allow_simple_key = false;
        self.possible_simple_keys.clear();
        let mark = self.get_mark();
        self.push(TokenKind::StreamEnd, mark, mark);
        self.done = true;
        Ok(())
    }

    fn fetch_directive(&mut self) -> ScanResult<()> {
        self.unwind_indent(-1);
        self.remove_possible_simple_key()?;
        self.allow_simple_key = false;
        let token = self.scan_directive()?;
        self.tokens.push_back(token);
        Ok(())
    }

    fn fetch_document_indicator(&mut self, kind: TokenKind) -> ScanResult<()> {
        self.unwind_indent(-1);
        self.remove_possible_simple_key()?;
        self.allow_simple_key = false;
        let start = self.get_mark();
        self.forward_n(3);
        let end = self.get_mark();
        self.push(kind, start, end);
        Ok(())
    }

    fn fetch_flow_collection_start(&mut self, kind: TokenKind) -> ScanResult<()> {
        self.save_possible_simple_key()?;
        self.flow_level += 1;
        self.allow_simple_key = true;
        let start = self.get_mark();
        self.forward();
        let end = self.get_mark();
        self.push(kind, start, end);
        Ok(())
    }

    fn fetch_flow_collection_end(&mut self, kind: TokenKind) -> ScanResult<()> {
        self.remove_possible_simple_key()?;
        self.flow_level = self.flow_level.saturating_sub(1);
        self.allow_simple_key = false;
        let start = self.get_mark();
        self.forward();
        let end = self.get_mark();
        self.push(kind, start, end);
        Ok(())
    }

    fn fetch_flow_entry(&mut self) -> ScanResult<()> {
        self.allow_simple_key = true;
        self.remove_possible_simple_key()?;
        let start = self.get_mark();
        self.forward();
        let end = self.get_mark();
        self.push(TokenKind::FlowEntry, start, end);
        Ok(())
    }

    fn fetch_block_entry(&mut self) -> ScanResult<()> {
        if self.flow_level == 0 {
            if !self.allow_simple_key {
                return Err(self.err(
                    None,
                    None,
                    "sequence entries are not allowed here",
                    self.get_mark(),
                ));
            }
            if self.add_indent(self.column as i64) {
                let mark = self.get_mark();
                self.push(TokenKind::BlockSequenceStart, mark, mark);
            }
        }
        self.allow_simple_key = true;
        self.remove_possible_simple_key()?;
        let start = self.get_mark();
        self.forward();
        let end = self.get_mark();
        self.push(TokenKind::BlockEntry, start, end);
        Ok(())
    }

    fn fetch_key(&mut self) -> ScanResult<()> {
        if self.flow_level == 0 {
            if !self.allow_simple_key {
                return Err(self.err(
                    None,
                    None,
                    "mapping keys are not allowed here",
                    self.get_mark(),
                ));
            }
            if self.add_indent(self.column as i64) {
                let mark = self.get_mark();
                self.push(TokenKind::BlockMappingStart, mark, mark);
            }
        }
        self.allow_simple_key = self.flow_level == 0;
        self.remove_possible_simple_key()?;
        let start = self.get_mark();
        self.forward();
        let end = self.get_mark();
        self.push(TokenKind::Key, start, end);
        Ok(())
    }

    fn fetch_value(&mut self) -> ScanResult<()> {
        if let Some(key) = self.possible_simple_keys.remove(&self.flow_level) {
            let pos = key.token_number - self.tokens_taken;
            self.tokens.insert(
                pos,
                Token {
                    kind: TokenKind::Key,
                    start: key.mark,
                    end: key.mark,
                },
            );
            if self.flow_level == 0 && self.add_indent(key.column as i64) {
                self.tokens.insert(
                    pos,
                    Token {
                        kind: TokenKind::BlockMappingStart,
                        start: key.mark,
                        end: key.mark,
                    },
                );
            }
            self.allow_simple_key = false;
        } else {
            if self.flow_level == 0 {
                if !self.allow_simple_key {
                    return Err(self.err(
                        None,
                        None,
                        "mapping values are not allowed here",
                        self.get_mark(),
                    ));
                }
                if self.add_indent(self.column as i64) {
                    let mark = self.get_mark();
                    self.push(TokenKind::BlockMappingStart, mark, mark);
                }
            }
            self.allow_simple_key = self.flow_level == 0;
            self.remove_possible_simple_key()?;
        }
        let start = self.get_mark();
        self.forward();
        let end = self.get_mark();
        self.push(TokenKind::Value, start, end);
        Ok(())
    }

    fn fetch_alias_or_anchor(&mut self, alias: bool) -> ScanResult<()> {
        self.save_possible_simple_key()?;
        self.allow_simple_key = false;
        let token = self.scan_anchor(alias)?;
        self.tokens.push_back(token);
        Ok(())
    }

    fn fetch_tag(&mut self) -> ScanResult<()> {
        self.save_possible_simple_key()?;
        self.allow_simple_key = false;
        let token = self.scan_tag()?;
        self.tokens.push_back(token);
        Ok(())
    }

    fn fetch_block_scalar(&mut self, style: char) -> ScanResult<()> {
        self.allow_simple_key = true;
        self.remove_possible_simple_key()?;
        let token = self.scan_block_scalar(style)?;
        self.tokens.push_back(token);
        Ok(())
    }

    fn fetch_flow_scalar(&mut self, style: char) -> ScanResult<()> {
        self.save_possible_simple_key()?;
        self.allow_simple_key = false;
        let token = self.scan_flow_scalar(style)?;
        self.tokens.push_back(token);
        Ok(())
    }

    fn fetch_plain(&mut self) -> ScanResult<()> {
        self.save_possible_simple_key()?;
        self.allow_simple_key = false;
        let token = self.scan_plain()?;
        self.tokens.push_back(token);
        Ok(())
    }

    // ---- checkers ----

    fn check_directive(&self) -> bool {
        self.column == 0
    }

    fn check_document_start(&self) -> bool {
        self.column == 0 && self.prefix(3) == "---" && is_in(self.peek_at(3), NUL_WS_BREAK)
    }

    fn check_document_end(&self) -> bool {
        self.column == 0 && self.prefix(3) == "..." && is_in(self.peek_at(3), NUL_WS_BREAK)
    }

    fn check_block_entry(&self) -> bool {
        is_in(self.peek_at(1), NUL_WS_BREAK)
    }

    fn check_key(&self) -> bool {
        if self.flow_level > 0 {
            true
        } else {
            is_in(self.peek_at(1), NUL_WS_BREAK)
        }
    }

    fn check_value(&self) -> bool {
        if self.flow_level > 0 {
            true
        } else {
            is_in(self.peek_at(1), NUL_WS_BREAK)
        }
    }

    fn check_plain(&self) -> bool {
        let ch = self.peek();
        !is_in(ch, "\0 \t\r\n\u{85}\u{2028}\u{2029}-?:,[]{}#&*!|>'\"%@`")
            || (!is_in(self.peek_at(1), NUL_WS_BREAK)
                && (ch == '-' || (self.flow_level == 0 && (ch == '?' || ch == ':'))))
    }

    // ---- scanners ----

    fn scan_to_next_token(&mut self) {
        if self.index == 0 && self.peek() == '\u{feff}' {
            self.forward();
        }
        let mut found = false;
        while !found {
            while self.peek() == ' ' {
                self.forward();
            }
            if self.peek() == '#' {
                while !is_in(self.peek(), NUL_BREAK) {
                    self.forward();
                }
            }
            if !self.scan_line_break().is_empty() {
                if self.flow_level == 0 {
                    self.allow_simple_key = true;
                }
            } else {
                found = true;
            }
        }
    }

    fn scan_directive(&mut self) -> ScanResult<Token> {
        let start_mark = self.get_mark();
        self.forward();
        let name = self.scan_directive_name(start_mark)?;
        let (value, end_mark);
        if name == "YAML" {
            value = self.scan_yaml_directive_value(start_mark)?;
            end_mark = self.get_mark();
        } else if name == "TAG" {
            value = self.scan_tag_directive_value(start_mark)?;
            end_mark = self.get_mark();
        } else {
            value = DirectiveValue::None;
            end_mark = self.get_mark();
            while !is_in(self.peek(), NUL_BREAK) {
                self.forward();
            }
        }
        self.scan_directive_ignored_line(start_mark)?;
        Ok(Token {
            kind: TokenKind::Directive(name, value),
            start: start_mark,
            end: end_mark,
        })
    }

    fn scan_directive_name(&mut self, start_mark: Mark) -> ScanResult<String> {
        let mut length = 0;
        let mut ch = self.peek_at(length);
        while is_alnum_dash(ch) {
            length += 1;
            ch = self.peek_at(length);
        }
        if length == 0 {
            return Err(self.err(
                Some("while scanning a directive"),
                Some(start_mark),
                &format!(
                    "expected alphabetic or numeric character, but found {}",
                    repr_char(ch)
                ),
                self.get_mark(),
            ));
        }
        let value = self.prefix(length);
        self.forward_n(length);
        let ch = self.peek();
        if !is_in(ch, NUL_SP_BREAK) {
            return Err(self.err(
                Some("while scanning a directive"),
                Some(start_mark),
                &format!(
                    "expected alphabetic or numeric character, but found {}",
                    repr_char(ch)
                ),
                self.get_mark(),
            ));
        }
        Ok(value)
    }

    fn scan_yaml_directive_value(&mut self, start_mark: Mark) -> ScanResult<DirectiveValue> {
        while self.peek() == ' ' {
            self.forward();
        }
        let major = self.scan_yaml_directive_number(start_mark)?;
        if self.peek() != '.' {
            return Err(self.err(
                Some("while scanning a directive"),
                Some(start_mark),
                &format!(
                    "expected a digit or '.', but found {}",
                    repr_char(self.peek())
                ),
                self.get_mark(),
            ));
        }
        self.forward();
        let minor = self.scan_yaml_directive_number(start_mark)?;
        if !is_in(self.peek(), NUL_SP_BREAK) {
            return Err(self.err(
                Some("while scanning a directive"),
                Some(start_mark),
                &format!(
                    "expected a digit or ' ', but found {}",
                    repr_char(self.peek())
                ),
                self.get_mark(),
            ));
        }
        Ok(DirectiveValue::Yaml(major, minor))
    }

    fn scan_yaml_directive_number(&mut self, start_mark: Mark) -> ScanResult<i64> {
        let ch = self.peek();
        if !ch.is_ascii_digit() {
            return Err(self.err(
                Some("while scanning a directive"),
                Some(start_mark),
                &format!("expected a digit, but found {}", repr_char(ch)),
                self.get_mark(),
            ));
        }
        let mut length = 0;
        while self.peek_at(length).is_ascii_digit() {
            length += 1;
        }
        let value = self.prefix(length).parse::<i64>().unwrap_or(i64::MAX);
        self.forward_n(length);
        Ok(value)
    }

    fn scan_tag_directive_value(&mut self, start_mark: Mark) -> ScanResult<DirectiveValue> {
        while self.peek() == ' ' {
            self.forward();
        }
        let handle = self.scan_tag_handle("directive", start_mark)?;
        let ch = self.peek();
        if ch != ' ' {
            return Err(self.err(
                Some("while scanning a directive"),
                Some(start_mark),
                &format!("expected ' ', but found {}", repr_char(ch)),
                self.get_mark(),
            ));
        }
        while self.peek() == ' ' {
            self.forward();
        }
        let prefix = self.scan_tag_uri("directive", start_mark)?;
        let ch = self.peek();
        if !is_in(ch, NUL_SP_BREAK) {
            return Err(self.err(
                Some("while scanning a directive"),
                Some(start_mark),
                &format!("expected ' ', but found {}", repr_char(ch)),
                self.get_mark(),
            ));
        }
        Ok(DirectiveValue::Tag(handle, prefix))
    }

    fn scan_directive_ignored_line(&mut self, start_mark: Mark) -> ScanResult<()> {
        while self.peek() == ' ' {
            self.forward();
        }
        if self.peek() == '#' {
            while !is_in(self.peek(), NUL_BREAK) {
                self.forward();
            }
        }
        let ch = self.peek();
        if !is_in(ch, NUL_BREAK) {
            return Err(self.err(
                Some("while scanning a directive"),
                Some(start_mark),
                &format!(
                    "expected a comment or a line break, but found {}",
                    repr_char(ch)
                ),
                self.get_mark(),
            ));
        }
        self.scan_line_break();
        Ok(())
    }

    fn scan_anchor(&mut self, alias: bool) -> ScanResult<Token> {
        let start_mark = self.get_mark();
        let name = if alias { "alias" } else { "anchor" };
        self.forward();
        let mut length = 0;
        let mut ch = self.peek_at(length);
        while is_alnum_dash(ch) {
            length += 1;
            ch = self.peek_at(length);
        }
        if length == 0 {
            return Err(self.err(
                Some(&format!("while scanning an {name}")),
                Some(start_mark),
                &format!(
                    "expected alphabetic or numeric character, but found {}",
                    repr_char(ch)
                ),
                self.get_mark(),
            ));
        }
        let value = self.prefix(length);
        self.forward_n(length);
        let ch = self.peek();
        if !is_in(ch, "\0 \t\r\n\u{85}\u{2028}\u{2029}?:,]}%@`") {
            return Err(self.err(
                Some(&format!("while scanning an {name}")),
                Some(start_mark),
                &format!(
                    "expected alphabetic or numeric character, but found {}",
                    repr_char(ch)
                ),
                self.get_mark(),
            ));
        }
        let end_mark = self.get_mark();
        Ok(Token {
            kind: if alias {
                TokenKind::Alias(value)
            } else {
                TokenKind::Anchor(value)
            },
            start: start_mark,
            end: end_mark,
        })
    }

    fn scan_tag(&mut self) -> ScanResult<Token> {
        let start_mark = self.get_mark();
        let mut ch = self.peek_at(1);
        let handle: Option<String>;
        let suffix: String;
        if ch == '<' {
            handle = None;
            self.forward_n(2);
            suffix = self.scan_tag_uri("tag", start_mark)?;
            if self.peek() != '>' {
                return Err(self.err(
                    Some("while parsing a tag"),
                    Some(start_mark),
                    &format!("expected '>', but found {}", repr_char(self.peek())),
                    self.get_mark(),
                ));
            }
            self.forward();
        } else if is_in(ch, NUL_WS_BREAK) {
            handle = None;
            suffix = "!".to_string();
            self.forward();
        } else {
            let mut length = 1;
            let mut use_handle = false;
            while !is_in(ch, NUL_SP_BREAK) {
                if ch == '!' {
                    use_handle = true;
                    break;
                }
                length += 1;
                ch = self.peek_at(length);
            }
            if use_handle {
                handle = Some(self.scan_tag_handle("tag", start_mark)?);
            } else {
                handle = Some("!".to_string());
                self.forward();
            }
            suffix = self.scan_tag_uri("tag", start_mark)?;
        }
        let ch = self.peek();
        if !is_in(ch, NUL_SP_BREAK) {
            return Err(self.err(
                Some("while scanning a tag"),
                Some(start_mark),
                &format!("expected ' ', but found {}", repr_char(ch)),
                self.get_mark(),
            ));
        }
        let end_mark = self.get_mark();
        Ok(Token {
            kind: TokenKind::Tag(handle, suffix),
            start: start_mark,
            end: end_mark,
        })
    }

    fn scan_block_scalar(&mut self, style: char) -> ScanResult<Token> {
        let folded = style == '>';
        let mut chunks: Vec<String> = Vec::new();
        let start_mark = self.get_mark();
        self.forward();
        let (chomping, increment) = self.scan_block_scalar_indicators(start_mark)?;
        self.scan_block_scalar_ignored_line(start_mark)?;

        let mut min_indent = self.indent + 1;
        if min_indent < 1 {
            min_indent = 1;
        }
        let mut breaks: Vec<String>;
        let mut end_mark;
        let indent: i64;
        match increment {
            None => {
                let (b, max_indent, e) = self.scan_block_scalar_indentation();
                breaks = b;
                end_mark = e;
                indent = min_indent.max(max_indent as i64);
            }
            Some(inc) => {
                indent = min_indent + inc as i64 - 1;
                let (b, e) = self.scan_block_scalar_breaks(indent);
                breaks = b;
                end_mark = e;
            }
        }
        let mut line_break = String::new();

        while self.column as i64 == indent && self.peek() != '\0' {
            chunks.extend(breaks.iter().cloned());
            let leading_non_space = !is_in(self.peek(), " \t");
            let mut length = 0;
            while !is_in(self.peek_at(length), NUL_BREAK) {
                length += 1;
            }
            chunks.push(self.prefix(length));
            self.forward_n(length);
            line_break = self.scan_line_break();
            let (b, e) = self.scan_block_scalar_breaks(indent);
            breaks = b;
            end_mark = e;
            if self.column as i64 == indent && self.peek() != '\0' {
                if folded && line_break == "\n" && leading_non_space && !is_in(self.peek(), " \t") {
                    if breaks.is_empty() {
                        chunks.push(" ".to_string());
                    }
                } else {
                    chunks.push(line_break.clone());
                }
            } else {
                break;
            }
        }

        if chomping != Some(false) {
            chunks.push(line_break);
        }
        if chomping == Some(true) {
            chunks.extend(breaks);
        }
        Ok(Token {
            kind: TokenKind::Scalar(chunks.concat(), false, Some(style)),
            start: start_mark,
            end: end_mark,
        })
    }

    fn scan_block_scalar_indicators(
        &mut self,
        start_mark: Mark,
    ) -> ScanResult<(Option<bool>, Option<usize>)> {
        let mut chomping = None;
        let mut increment = None;
        let mut ch = self.peek();
        let zero_err = |s: &Scanner| {
            s.err(
                Some("while scanning a block scalar"),
                Some(start_mark),
                "expected indentation indicator in the range 1-9, but found 0",
                s.get_mark(),
            )
        };
        if ch == '+' || ch == '-' {
            chomping = Some(ch == '+');
            self.forward();
            ch = self.peek();
            if ch.is_ascii_digit() {
                let inc = ch.to_digit(10).unwrap_or(0) as usize;
                if inc == 0 {
                    return Err(zero_err(self));
                }
                increment = Some(inc);
                self.forward();
            }
        } else if ch.is_ascii_digit() {
            let inc = ch.to_digit(10).unwrap_or(0) as usize;
            if inc == 0 {
                return Err(zero_err(self));
            }
            increment = Some(inc);
            self.forward();
            ch = self.peek();
            if ch == '+' || ch == '-' {
                chomping = Some(ch == '+');
                self.forward();
            }
        }
        let ch = self.peek();
        if !is_in(ch, NUL_SP_BREAK) {
            return Err(self.err(
                Some("while scanning a block scalar"),
                Some(start_mark),
                &format!(
                    "expected chomping or indentation indicators, but found {}",
                    repr_char(ch)
                ),
                self.get_mark(),
            ));
        }
        Ok((chomping, increment))
    }

    fn scan_block_scalar_ignored_line(&mut self, start_mark: Mark) -> ScanResult<()> {
        while self.peek() == ' ' {
            self.forward();
        }
        if self.peek() == '#' {
            while !is_in(self.peek(), NUL_BREAK) {
                self.forward();
            }
        }
        let ch = self.peek();
        if !is_in(ch, NUL_BREAK) {
            return Err(self.err(
                Some("while scanning a block scalar"),
                Some(start_mark),
                &format!(
                    "expected a comment or a line break, but found {}",
                    repr_char(ch)
                ),
                self.get_mark(),
            ));
        }
        self.scan_line_break();
        Ok(())
    }

    fn scan_block_scalar_indentation(&mut self) -> (Vec<String>, usize, Mark) {
        let mut chunks = Vec::new();
        let mut max_indent = 0;
        let mut end_mark = self.get_mark();
        while is_in(self.peek(), " \r\n\u{85}\u{2028}\u{2029}") {
            if self.peek() != ' ' {
                chunks.push(self.scan_line_break());
                end_mark = self.get_mark();
            } else {
                self.forward();
                if self.column > max_indent {
                    max_indent = self.column;
                }
            }
        }
        (chunks, max_indent, end_mark)
    }

    fn scan_block_scalar_breaks(&mut self, indent: i64) -> (Vec<String>, Mark) {
        let mut chunks = Vec::new();
        let mut end_mark = self.get_mark();
        while (self.column as i64) < indent && self.peek() == ' ' {
            self.forward();
        }
        while is_in(self.peek(), "\r\n\u{85}\u{2028}\u{2029}") {
            chunks.push(self.scan_line_break());
            end_mark = self.get_mark();
            while (self.column as i64) < indent && self.peek() == ' ' {
                self.forward();
            }
        }
        (chunks, end_mark)
    }

    fn scan_flow_scalar(&mut self, style: char) -> ScanResult<Token> {
        let double = style == '"';
        let mut chunks: Vec<String> = Vec::new();
        let start_mark = self.get_mark();
        let quote = self.peek();
        self.forward();
        chunks.extend(self.scan_flow_scalar_non_spaces(double, start_mark)?);
        while self.peek() != quote {
            chunks.extend(self.scan_flow_scalar_spaces(double, start_mark)?);
            chunks.extend(self.scan_flow_scalar_non_spaces(double, start_mark)?);
        }
        self.forward();
        let end_mark = self.get_mark();
        Ok(Token {
            kind: TokenKind::Scalar(chunks.concat(), false, Some(style)),
            start: start_mark,
            end: end_mark,
        })
    }

    fn scan_flow_scalar_non_spaces(
        &mut self,
        double: bool,
        start_mark: Mark,
    ) -> ScanResult<Vec<String>> {
        let mut chunks: Vec<String> = Vec::new();
        loop {
            let mut length = 0;
            while !is_in(self.peek_at(length), "'\"\\\0 \t\r\n\u{85}\u{2028}\u{2029}") {
                length += 1;
            }
            if length > 0 {
                chunks.push(self.prefix(length));
                self.forward_n(length);
            }
            let ch = self.peek();
            if !double && ch == '\'' && self.peek_at(1) == '\'' {
                chunks.push("'".to_string());
                self.forward_n(2);
            } else if (double && ch == '\'') || (!double && (ch == '"' || ch == '\\')) {
                chunks.push(ch.to_string());
                self.forward();
            } else if double && ch == '\\' {
                self.forward();
                let ch = self.peek();
                if let Some(rep) = escape_replacement(ch) {
                    chunks.push(rep.to_string());
                    self.forward();
                } else if let Some(length) = escape_code_length(ch) {
                    self.forward();
                    for k in 0..length {
                        if !self.peek_at(k).is_ascii_hexdigit() {
                            return Err(self.err(
                                Some("while scanning a double-quoted scalar"),
                                Some(start_mark),
                                &format!(
                                    "expected escape sequence of {} hexadecimal numbers, but found {}",
                                    length,
                                    repr_char(self.peek_at(k))
                                ),
                                self.get_mark(),
                            ));
                        }
                    }
                    let code = u32::from_str_radix(&self.prefix(length), 16).unwrap_or(0);
                    match char::from_u32(code) {
                        Some(c) => chunks.push(c.to_string()),
                        None if code > 0x10ffff => {
                            return Err(PyYamlError {
                                kind: ErrorKind::Value,
                                message: "chr() arg not in range(0x110000)".to_string(),
                            });
                        }
                        // PARITY: Python keeps a lone surrogate; a Rust string cannot.
                        None => chunks.push('\u{fffd}'.to_string()),
                    }
                    self.forward_n(length);
                } else if is_in(ch, "\r\n\u{85}\u{2028}\u{2029}") {
                    self.scan_line_break();
                    chunks.extend(self.scan_flow_scalar_breaks(double, start_mark)?);
                } else {
                    return Err(self.err(
                        Some("while scanning a double-quoted scalar"),
                        Some(start_mark),
                        &format!("found unknown escape character {}", repr_char(ch)),
                        self.get_mark(),
                    ));
                }
            } else {
                return Ok(chunks);
            }
        }
    }

    fn scan_flow_scalar_spaces(
        &mut self,
        double: bool,
        start_mark: Mark,
    ) -> ScanResult<Vec<String>> {
        let mut chunks: Vec<String> = Vec::new();
        let mut length = 0;
        while is_in(self.peek_at(length), " \t") {
            length += 1;
        }
        let whitespaces = self.prefix(length);
        self.forward_n(length);
        let ch = self.peek();
        if ch == '\0' {
            return Err(self.err(
                Some("while scanning a quoted scalar"),
                Some(start_mark),
                "found unexpected end of stream",
                self.get_mark(),
            ));
        } else if is_in(ch, "\r\n\u{85}\u{2028}\u{2029}") {
            let line_break = self.scan_line_break();
            let breaks = self.scan_flow_scalar_breaks(double, start_mark)?;
            if line_break != "\n" {
                chunks.push(line_break);
            } else if breaks.is_empty() {
                chunks.push(" ".to_string());
            }
            chunks.extend(breaks);
        } else {
            chunks.push(whitespaces);
        }
        Ok(chunks)
    }

    fn scan_flow_scalar_breaks(
        &mut self,
        _double: bool,
        start_mark: Mark,
    ) -> ScanResult<Vec<String>> {
        let mut chunks = Vec::new();
        loop {
            let prefix = self.prefix(3);
            if (prefix == "---" || prefix == "...") && is_in(self.peek_at(3), NUL_WS_BREAK) {
                return Err(self.err(
                    Some("while scanning a quoted scalar"),
                    Some(start_mark),
                    "found unexpected document separator",
                    self.get_mark(),
                ));
            }
            while is_in(self.peek(), " \t") {
                self.forward();
            }
            if is_in(self.peek(), "\r\n\u{85}\u{2028}\u{2029}") {
                chunks.push(self.scan_line_break());
            } else {
                return Ok(chunks);
            }
        }
    }

    fn scan_plain(&mut self) -> ScanResult<Token> {
        let mut chunks: Vec<String> = Vec::new();
        let start_mark = self.get_mark();
        let mut end_mark = start_mark;
        let indent = self.indent + 1;
        let mut spaces: Vec<String> = Vec::new();
        loop {
            let mut length = 0;
            if self.peek() == '#' {
                break;
            }
            loop {
                let ch = self.peek_at(length);
                let flow_extra = if self.flow_level > 0 { ",[]{}" } else { "" };
                if is_in(ch, NUL_WS_BREAK)
                    || (ch == ':'
                        && (is_in(self.peek_at(length + 1), NUL_WS_BREAK)
                            || (!flow_extra.is_empty()
                                && is_in(self.peek_at(length + 1), flow_extra))))
                    || (self.flow_level > 0 && is_in(ch, ",?[]{}"))
                {
                    break;
                }
                length += 1;
            }
            if length == 0 {
                break;
            }
            self.allow_simple_key = false;
            chunks.extend(spaces.iter().cloned());
            chunks.push(self.prefix(length));
            self.forward_n(length);
            end_mark = self.get_mark();
            match self.scan_plain_spaces(indent) {
                Some(s) => spaces = s,
                None => break,
            }
            if spaces.is_empty()
                || self.peek() == '#'
                || (self.flow_level == 0 && (self.column as i64) < indent)
            {
                break;
            }
        }
        Ok(Token {
            kind: TokenKind::Scalar(chunks.concat(), true, None),
            start: start_mark,
            end: end_mark,
        })
    }

    /// `None` stands for Python's bare `return` (a document separator follows).
    fn scan_plain_spaces(&mut self, _indent: i64) -> Option<Vec<String>> {
        let mut chunks: Vec<String> = Vec::new();
        let mut length = 0;
        while self.peek_at(length) == ' ' {
            length += 1;
        }
        let whitespaces = self.prefix(length);
        self.forward_n(length);
        let ch = self.peek();
        if is_in(ch, "\r\n\u{85}\u{2028}\u{2029}") {
            let line_break = self.scan_line_break();
            self.allow_simple_key = true;
            let prefix = self.prefix(3);
            if (prefix == "---" || prefix == "...") && is_in(self.peek_at(3), NUL_WS_BREAK) {
                return None;
            }
            let mut breaks: Vec<String> = Vec::new();
            while is_in(self.peek(), " \r\n\u{85}\u{2028}\u{2029}") {
                if self.peek() == ' ' {
                    self.forward();
                } else {
                    breaks.push(self.scan_line_break());
                    let prefix = self.prefix(3);
                    if (prefix == "---" || prefix == "...") && is_in(self.peek_at(3), NUL_WS_BREAK)
                    {
                        return None;
                    }
                }
            }
            if line_break != "\n" {
                chunks.push(line_break);
            } else if breaks.is_empty() {
                chunks.push(" ".to_string());
            }
            chunks.extend(breaks);
        } else if !whitespaces.is_empty() {
            chunks.push(whitespaces);
        }
        Some(chunks)
    }

    fn scan_tag_handle(&mut self, name: &str, start_mark: Mark) -> ScanResult<String> {
        let ch = self.peek();
        if ch != '!' {
            return Err(self.err(
                Some(&format!("while scanning a {name}")),
                Some(start_mark),
                &format!("expected '!', but found {}", repr_char(ch)),
                self.get_mark(),
            ));
        }
        let mut length = 1;
        let mut ch = self.peek_at(length);
        if ch != ' ' {
            while is_alnum_dash(ch) {
                length += 1;
                ch = self.peek_at(length);
            }
            if ch != '!' {
                self.forward_n(length);
                return Err(self.err(
                    Some(&format!("while scanning a {name}")),
                    Some(start_mark),
                    &format!("expected '!', but found {}", repr_char(ch)),
                    self.get_mark(),
                ));
            }
            length += 1;
        }
        let value = self.prefix(length);
        self.forward_n(length);
        Ok(value)
    }

    fn scan_tag_uri(&mut self, name: &str, start_mark: Mark) -> ScanResult<String> {
        let mut chunks: Vec<String> = Vec::new();
        let mut length = 0;
        let mut ch = self.peek_at(length);
        while ch.is_ascii_alphanumeric() || is_in(ch, "-;/?:@&=+$,_.!~*'()[]%") {
            if ch == '%' {
                chunks.push(self.prefix(length));
                self.forward_n(length);
                length = 0;
                chunks.push(self.scan_uri_escapes(name, start_mark)?);
            } else {
                length += 1;
            }
            ch = self.peek_at(length);
        }
        if length > 0 {
            chunks.push(self.prefix(length));
            self.forward_n(length);
        }
        if chunks.is_empty() {
            return Err(self.err(
                Some(&format!("while parsing a {name}")),
                Some(start_mark),
                &format!("expected URI, but found {}", repr_char(ch)),
                self.get_mark(),
            ));
        }
        Ok(chunks.concat())
    }

    fn scan_uri_escapes(&mut self, name: &str, start_mark: Mark) -> ScanResult<String> {
        let mut codes: Vec<u8> = Vec::new();
        let mark = self.get_mark();
        while self.peek() == '%' {
            self.forward();
            for k in 0..2 {
                if !self.peek_at(k).is_ascii_hexdigit() {
                    return Err(self.err(
                        Some(&format!("while scanning a {name}")),
                        Some(start_mark),
                        &format!(
                            "expected URI escape sequence of 2 hexadecimal numbers, but found {}",
                            repr_char(self.peek_at(k))
                        ),
                        self.get_mark(),
                    ));
                }
            }
            codes.push(u8::from_str_radix(&self.prefix(2), 16).unwrap_or(0));
            self.forward_n(2);
        }
        match String::from_utf8(codes) {
            Ok(s) => Ok(s),
            Err(e) => Err(self.err(
                Some(&format!("while scanning a {name}")),
                Some(start_mark),
                &format!("'utf-8' codec can't decode bytes: {}", e.utf8_error()),
                mark,
            )),
        }
    }

    fn scan_line_break(&mut self) -> String {
        let ch = self.peek();
        if is_in(ch, "\r\n\u{85}") {
            if self.prefix(2) == "\r\n" {
                self.forward_n(2);
            } else {
                self.forward();
            }
            return "\n".to_string();
        } else if ch == '\u{2028}' || ch == '\u{2029}' {
            self.forward();
            return ch.to_string();
        }
        String::new()
    }
}

fn escape_replacement(ch: char) -> Option<char> {
    Some(match ch {
        '0' => '\0',
        'a' => '\x07',
        'b' => '\x08',
        't' | '\t' => '\x09',
        'n' => '\x0A',
        'v' => '\x0B',
        'f' => '\x0C',
        'r' => '\x0D',
        'e' => '\x1B',
        ' ' => '\x20',
        '"' => '"',
        '\\' => '\\',
        '/' => '/',
        'N' => '\u{85}',
        '_' => '\u{a0}',
        'L' => '\u{2028}',
        'P' => '\u{2029}',
        _ => return None,
    })
}

fn escape_code_length(ch: char) -> Option<usize> {
    match ch {
        'x' => Some(2),
        'u' => Some(4),
        'U' => Some(8),
        _ => None,
    }
}

/// `yaml.reader.Reader.check_printable`: the first character PyYAML refuses.
pub fn check_printable(text: &str) -> Option<(usize, u32)> {
    for (position, c) in text.chars().enumerate() {
        let ok = matches!(c, '\x09' | '\x0A' | '\x0D' | '\x20'..='\x7E' | '\u{85}')
            || ('\u{a0}'..='\u{d7ff}').contains(&c)
            || ('\u{e000}'..='\u{fffd}').contains(&c)
            || ('\u{10000}'..='\u{10ffff}').contains(&c);
        if !ok {
            return Some((position, c as u32));
        }
    }
    None
}
