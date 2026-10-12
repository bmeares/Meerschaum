//! Python-compatible formatting and parsing helpers.
//!
//! Several Meerschaum behaviors are defined by Python's own serializers: `MRSM{}` substitution
//! rewrites a `json.dumps` string, `{{ Pipe() }}` templating inserts `json.dumps` / `str()` output,
//! and the `Pipe(...)` argument syntax is parsed with `ast.literal_eval`. This module reproduces
//! those functions for [`Value`]s, plus the Python string semantics the SQL query builders depend
//! on (`str.isspace` / `strip` / `splitlines`, `textwrap.dedent` / `indent` (CPython 3.14),
//! `str.format` with named fields, `datetime.__str__` / `isoformat`).
//!
//! Every crate uses these helpers; none keeps its own copy.

use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, Offset, Timelike, Utc};

use crate::value::{Map, Value};

/// Python's `repr(float)` (and `json.dumps` for finite floats).
pub fn float_repr(f: f64) -> String {
    if f.is_nan() {
        return "nan".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    if f == 0.0 {
        return if f.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    // Rust's `{:e}` yields the shortest round-trip digits, like Python's `repr` (mode 'r').
    let sci = format!("{:e}", f.abs());
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let sign = if f < 0.0 { "-" } else { "" };
    let decpt = exp + 1;
    if decpt <= -4 || decpt > 16 {
        // Exponent notation: d[.ddd]e+XX
        let mut out = String::from(sign);
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        out.push_str(&format!("{:02}", exp.abs()));
        out
    } else if decpt <= 0 {
        format!(
            "{sign}0.{}{}",
            "0".repeat(usize::try_from(-decpt).unwrap_or(0)),
            digits
        )
    } else {
        let decpt = usize::try_from(decpt).unwrap_or(0);
        if digits.len() <= decpt {
            format!("{sign}{}{}.0", digits, "0".repeat(decpt - digits.len()))
        } else {
            format!("{sign}{}.{}", &digits[..decpt], &digits[decpt..])
        }
    }
}

/// Separator and indentation style for [`json_dumps`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonStyle {
    /// `json.dumps(x)`: `', '` and `': '`.
    Default,
    /// `json.dumps(x, separators=(',', ':'))`.
    Compact,
    /// `json.dumps(x, indent=n)`: `','` plus newlines and `': '`.
    Indent(usize),
}

/// Python's `json.dumps(value, ensure_ascii=..., ...)` for [`Value`]s.
pub fn json_dumps(value: &Value, style: JsonStyle, ensure_ascii: bool) -> String {
    let mut out = String::new();
    write_json(value, style, ensure_ascii, 0, &mut out);
    out
}

/// Python's `json.dumps` of a string (with `ensure_ascii`).
pub fn json_dumps_str(s: &str, ensure_ascii: bool) -> String {
    let mut out = String::new();
    write_json_str(s, ensure_ascii, &mut out);
    out
}

fn write_json_str(s: &str, ensure_ascii: bool, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c if ensure_ascii && !(' '..='~').contains(&c) => {
                let mut buf = [0u16; 2];
                for unit in c.encode_utf16(&mut buf) {
                    out.push_str(&format!("\\u{:04x}", unit));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn json_float(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        float_repr(f)
    }
}

fn write_json(value: &Value, style: JsonStyle, ascii: bool, level: usize, out: &mut String) {
    let (item_sep, key_sep) = match style {
        JsonStyle::Default => (", ", ": "),
        JsonStyle::Compact => (",", ":"),
        JsonStyle::Indent(_) => (",", ": "),
    };
    let newline = |out: &mut String, level: usize| {
        if let JsonStyle::Indent(n) = style {
            out.push('\n');
            out.push_str(&" ".repeat(n * level));
        }
    };
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i) => out.push_str(&i.to_string()),
        Value::Float(f) => out.push_str(&json_float(*f)),
        Value::Str(s) => write_json_str(s, ascii, out),
        Value::List(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(item_sep);
                }
                newline(out, level + 1);
                write_json(item, style, ascii, level + 1, out);
            }
            newline(out, level);
            out.push(']');
        }
        Value::Map(map) => {
            if map.is_empty() {
                out.push_str("{}");
                return;
            }
            out.push('{');
            for (i, (k, v)) in map.iter().enumerate() {
                if i > 0 {
                    out.push_str(item_sep);
                }
                newline(out, level + 1);
                write_json_str(k, ascii, out);
                out.push_str(key_sep);
                write_json(v, style, ascii, level + 1, out);
            }
            newline(out, level);
            out.push('}');
        }
    }
}

/// Python's `repr(str)`.
pub fn str_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::new();
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
            c if !is_printable(c) => {
                let code = c as u32;
                if code < 0x100 {
                    out.push_str(&format!("\\x{code:02x}"));
                } else if code < 0x10000 {
                    out.push_str(&format!("\\u{code:04x}"));
                } else {
                    out.push_str(&format!("\\U{code:08x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

/// Approximation of Python's `str.isprintable()` for a single character.
///
/// Python consults the full Unicode database; this covers control, format, separator and
/// private-use characters, which is what appears in practice.
fn is_printable(c: char) -> bool {
    if c == ' ' {
        return true;
    }
    if c.is_control() || c.is_whitespace() {
        return false;
    }
    let code = c as u32;
    !matches!(
        code,
        0xAD | 0x600..=0x605 | 0x61C | 0x6DD | 0x70F | 0x200B..=0x200F | 0x202A..=0x202E
            | 0x2060..=0x2064 | 0x2066..=0x206F | 0xFEFF | 0xFFF9..=0xFFFB
            | 0xE000..=0xF8FF | 0xF0000..=0x10FFFF | 0xD800..=0xDFFF
    )
}

/// Python's `repr()` of a [`Value`].
pub fn py_repr(value: &Value) -> String {
    match value {
        Value::Null => "None".to_string(),
        Value::Bool(b) => if *b { "True" } else { "False" }.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => float_repr(*f),
        Value::Str(s) => str_repr(s),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(py_repr).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Map(map) => {
            let inner: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", str_repr(k), py_repr(v)))
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
    }
}

/// Python's `str()` of a [`Value`] (strings unquoted, everything else `repr`).
pub fn py_str(value: &Value) -> String {
    match value {
        Value::Str(s) => s.clone(),
        other => py_repr(other),
    }
}

/// Python's `int(str)` for base-10 strings: surrounding whitespace, an optional sign, and
/// single underscores between digits are accepted.
pub fn parse_py_int(s: &str) -> Option<i64> {
    let t = py_strip(s);
    let (neg, body) = match t.as_bytes().first()? {
        b'-' => (true, &t[1..]),
        b'+' => (false, &t[1..]),
        _ => (false, t),
    };
    if !valid_digit_run(body, |c| c.is_ascii_digit()) {
        return None;
    }
    let digits: String = body.chars().filter(|c| *c != '_').collect();
    let v: i64 = digits.parse().ok()?;
    Some(if neg { -v } else { v })
}

/// `True` if `s` is a non-empty run of digits with single underscores between digits.
fn valid_digit_run(s: &str, is_digit: impl Fn(char) -> bool) -> bool {
    if s.is_empty() || s.starts_with('_') || s.ends_with('_') || s.contains("__") {
        return false;
    }
    s.chars().all(|c| c == '_' || is_digit(c))
}

/// Python's `float(str)`.
pub fn parse_py_float(s: &str) -> Option<f64> {
    let t = py_strip(s);
    let lower = t.to_ascii_lowercase();
    let (neg, body) = match lower.as_bytes().first()? {
        b'-' => (true, &lower[1..]),
        b'+' => (false, &lower[1..]),
        _ => (false, lower.as_str()),
    };
    let v = match body {
        "inf" | "infinity" => f64::INFINITY,
        "nan" => f64::NAN,
        _ => {
            // Validate underscores: only between digits.
            let b = body.as_bytes();
            for (i, ch) in b.iter().enumerate() {
                if *ch == b'_' {
                    let prev_ok = i > 0 && b[i - 1].is_ascii_digit();
                    let next_ok = i + 1 < b.len() && b[i + 1].is_ascii_digit();
                    if !prev_ok || !next_ok {
                        return None;
                    }
                }
            }
            let cleaned: String = body.chars().filter(|c| *c != '_').collect();
            if cleaned.is_empty()
                || !cleaned
                    .chars()
                    .all(|c| c.is_ascii_digit() || matches!(c, '.' | 'e' | '+' | '-'))
            {
                return None;
            }
            cleaned.parse::<f64>().ok()?
        }
    };
    Some(if neg { -v } else { v })
}

/// Port of `meerschaum.utils.misc.is_int`: `float(s).is_integer()`.
pub fn is_int(s: &str) -> bool {
    parse_py_float(s).is_some_and(|f| f.is_finite() && f.fract() == 0.0)
}

/// Python's `int(value)` for a [`Value`] (`int` of a float truncates; strings are parsed).
pub fn py_int(value: &Value) -> Option<i64> {
    match value {
        Value::Int(i) => Some(*i),
        Value::Bool(b) => Some(i64::from(*b)),
        Value::Float(f) if f.is_finite() => Some(f.trunc() as i64),
        Value::Str(s) => parse_py_int(s),
        _ => None,
    }
}

// ---------------------------------------------------------------------------------------------
// ast.literal_eval
// ---------------------------------------------------------------------------------------------

/// Python's `ast.literal_eval` for the literal forms Meerschaum uses.
///
/// Supported: strings (all quote styles, `r`/`u` prefixes, escapes, implicit concatenation),
/// integers (decimal, hex, octal, binary, underscores), floats, `None`/`True`/`False`, unary
/// `+`/`-` on numbers, and `[...]`, `(...)`, `{...}` containers (tuples and sets become lists).
/// Bytes, complex numbers and integers beyond 64 bits are rejected.
pub fn literal_eval(src: &str) -> Result<Value, String> {
    let src = src.trim_start_matches([' ', '\t']);
    let mut p = LitParser::new(src);
    p.skip_ws();
    let v = p.parse_expr(false)?;
    p.skip_ws();
    if p.pos < p.chars.len() {
        return Err(format!("malformed node or string: {src:?}"));
    }
    Ok(v)
}

/// Like [`literal_eval`] but only accepts a single constant (an `ast.Constant` node): strings,
/// numbers without a sign, `None`, `True`, `False`. Used for `{{ Pipe() }}` subscripts.
pub fn literal_eval_constant(src: &str) -> Result<Value, String> {
    let mut p = LitParser::new(src);
    p.skip_ws();
    let v = p.parse_atom(true)?;
    p.skip_ws();
    if p.pos < p.chars.len() {
        return Err(format!("unsupported expression: {src:?}"));
    }
    Ok(v)
}

struct LitParser {
    chars: Vec<char>,
    pos: usize,
}

impl LitParser {
    fn new(s: &str) -> Self {
        LitParser {
            chars: s.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\x0c' {
                self.pos += 1;
            } else if c == '\\' && self.peek_at(1) == Some('\n') {
                self.pos += 2;
            } else {
                break;
            }
        }
    }

    fn parse_expr(&mut self, constant_only: bool) -> Result<Value, String> {
        self.skip_ws();
        match self.peek() {
            Some('-') | Some('+') if !constant_only => {
                let neg = self.peek() == Some('-');
                self.pos += 1;
                self.skip_ws();
                let v = self.parse_expr(false)?;
                match v {
                    Value::Int(i) => Ok(Value::Int(if neg { -i } else { i })),
                    Value::Bool(b) => {
                        let i = i64::from(b);
                        Ok(Value::Int(if neg { -i } else { i }))
                    }
                    Value::Float(f) => Ok(Value::Float(if neg { -f } else { f })),
                    _ => Err("malformed node or string".to_string()),
                }
            }
            _ => self.parse_atom(constant_only),
        }
    }

    fn parse_atom(&mut self, constant_only: bool) -> Result<Value, String> {
        self.skip_ws();
        let c = self.peek().ok_or("unexpected EOF")?;
        if c == '[' && !constant_only {
            self.pos += 1;
            let items = self.parse_items(']')?;
            return Ok(Value::List(items.0));
        }
        if c == '(' && !constant_only {
            self.pos += 1;
            let (items, trailing_comma) = self.parse_items(')')?;
            if items.len() == 1 && !trailing_comma {
                return Ok(items.into_iter().next().unwrap_or(Value::Null));
            }
            return Ok(Value::List(items));
        }
        if c == '{' && !constant_only {
            self.pos += 1;
            return self.parse_brace();
        }
        if c.is_ascii_digit() || (c == '.' && self.peek_at(1).is_some_and(|d| d.is_ascii_digit())) {
            return self.parse_number();
        }
        if c == '\'' || c == '"' {
            return self.parse_strings();
        }
        if c.is_alphabetic() || c == '_' {
            // Either a string prefix or a name.
            let start = self.pos;
            while self
                .peek()
                .is_some_and(|ch| ch.is_alphanumeric() || ch == '_')
            {
                self.pos += 1;
            }
            let word: String = self.chars[start..self.pos].iter().collect();
            if matches!(self.peek(), Some('\'') | Some('"')) && word.len() <= 2 {
                let lower = word.to_ascii_lowercase();
                if lower.contains('b') {
                    return Err("bytes literals are not supported".to_string());
                }
                if lower.contains('f') {
                    return Err("malformed node or string: f-string".to_string());
                }
                if matches!(lower.as_str(), "r" | "u") {
                    self.pos = start;
                    return self.parse_strings();
                }
            }
            return match word.as_str() {
                "None" => Ok(Value::Null),
                "True" => Ok(Value::Bool(true)),
                "False" => Ok(Value::Bool(false)),
                _ => Err(format!("malformed node or string: name {word:?}")),
            };
        }
        Err(format!("malformed node or string at {c:?}"))
    }

    fn parse_items(&mut self, close: char) -> Result<(Vec<Value>, bool), String> {
        let mut items = Vec::new();
        let mut trailing_comma = false;
        loop {
            self.skip_ws();
            if self.peek() == Some(close) {
                self.pos += 1;
                return Ok((items, trailing_comma));
            }
            items.push(self.parse_expr(false)?);
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos += 1;
                    trailing_comma = true;
                }
                Some(ch) if ch == close => {
                    self.pos += 1;
                    trailing_comma = false;
                    return Ok((items, trailing_comma));
                }
                _ => return Err("invalid syntax".to_string()),
            }
        }
    }

    fn parse_brace(&mut self) -> Result<Value, String> {
        let mut map = Map::new();
        let mut set_items: Vec<Value> = Vec::new();
        let mut is_set: Option<bool> = None;
        loop {
            self.skip_ws();
            if self.peek() == Some('}') {
                self.pos += 1;
                break;
            }
            let key = self.parse_expr(false)?;
            self.skip_ws();
            if self.peek() == Some(':') {
                if is_set == Some(true) {
                    return Err("invalid syntax".to_string());
                }
                is_set = Some(false);
                self.pos += 1;
                let val = self.parse_expr(false)?;
                map.insert(dict_key(&key)?, val);
            } else {
                if is_set == Some(false) {
                    return Err("invalid syntax".to_string());
                }
                is_set = Some(true);
                set_items.push(key);
            }
            self.skip_ws();
            match self.peek() {
                Some(',') => self.pos += 1,
                Some('}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err("invalid syntax".to_string()),
            }
        }
        if is_set == Some(true) {
            // Python sets are unordered; dedupe in first-seen order.
            let mut out: Vec<Value> = Vec::new();
            for item in set_items {
                if !out.contains(&item) {
                    out.push(item);
                }
            }
            return Ok(Value::List(out));
        }
        Ok(Value::Map(map))
    }

    fn parse_number(&mut self) -> Result<Value, String> {
        let start = self.pos;
        let c0 = self.peek().unwrap_or('0');
        let c1 = self.peek_at(1).map(|c| c.to_ascii_lowercase());
        if c0 == '0' && matches!(c1, Some('x') | Some('o') | Some('b')) {
            let radix = match c1 {
                Some('x') => 16,
                Some('o') => 8,
                _ => 2,
            };
            self.pos += 2;
            let body_start = self.pos;
            while self
                .peek()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                self.pos += 1;
            }
            let body: String = self.chars[body_start..self.pos].iter().collect();
            let body = body.strip_prefix('_').unwrap_or(&body);
            if !valid_digit_run(body, |c| c.is_digit(radix)) {
                return Err("invalid syntax".to_string());
            }
            let digits: String = body.chars().filter(|c| *c != '_').collect();
            return i64::from_str_radix(&digits, radix)
                .map(Value::Int)
                .map_err(|_| "integer out of range".to_string());
        }
        let mut is_float = false;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() || c == '_' {
                self.pos += 1;
            } else if c == '.' {
                is_float = true;
                self.pos += 1;
            } else if c == 'e' || c == 'E' {
                is_float = true;
                self.pos += 1;
                if matches!(self.peek(), Some('+') | Some('-')) {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
        if matches!(self.peek(), Some('j') | Some('J')) {
            return Err("complex numbers are not supported".to_string());
        }
        let text: String = self.chars[start..self.pos].iter().collect();
        if is_float {
            return parse_py_float(&text)
                .map(Value::Float)
                .ok_or_else(|| "invalid syntax".to_string());
        }
        if text.len() > 1 && text.starts_with('0') && text.chars().any(|c| c != '0' && c != '_') {
            return Err("leading zeros in decimal integer literals are not permitted".to_string());
        }
        parse_py_int(&text)
            .map(Value::Int)
            .ok_or_else(|| "invalid syntax or integer out of range".to_string())
    }

    /// Parse one or more adjacent string literals (implicit concatenation).
    fn parse_strings(&mut self) -> Result<Value, String> {
        let mut out = String::new();
        loop {
            self.skip_ws();
            let start = self.pos;
            let mut raw = false;
            while let Some(c) = self.peek() {
                match c.to_ascii_lowercase() {
                    'r' => raw = true,
                    'u' => {}
                    'b' => return Err("bytes literals are not supported".to_string()),
                    'f' => return Err("malformed node or string: f-string".to_string()),
                    _ => break,
                }
                self.pos += 1;
            }
            match self.peek() {
                Some('\'') | Some('"') => {}
                _ => {
                    self.pos = start;
                    break;
                }
            }
            out.push_str(&self.parse_one_string(raw)?);
            let save = self.pos;
            self.skip_ws();
            let next = self.peek();
            let next_is_str = matches!(next, Some('\'') | Some('"'))
                || (matches!(next, Some('r') | Some('R') | Some('u') | Some('U'))
                    && matches!(self.peek_at(1), Some('\'') | Some('"')));
            if !next_is_str {
                self.pos = save;
                break;
            }
        }
        Ok(Value::Str(out))
    }

    fn parse_one_string(&mut self, raw: bool) -> Result<String, String> {
        let q = self.peek().ok_or("unexpected EOF")?;
        let triple = self.peek_at(1) == Some(q) && self.peek_at(2) == Some(q);
        self.pos += if triple { 3 } else { 1 };
        let mut out = String::new();
        loop {
            let c = self.peek().ok_or("unterminated string literal")?;
            if c == q {
                if !triple {
                    self.pos += 1;
                    return Ok(out);
                }
                if self.peek_at(1) == Some(q) && self.peek_at(2) == Some(q) {
                    self.pos += 3;
                    return Ok(out);
                }
                out.push(c);
                self.pos += 1;
                continue;
            }
            if c == '\n' && !triple {
                return Err("unterminated string literal".to_string());
            }
            if c == '\\' {
                let n = self.peek_at(1).ok_or("unterminated string literal")?;
                if raw {
                    out.push('\\');
                    out.push(n);
                    self.pos += 2;
                    continue;
                }
                self.pos += 2;
                match n {
                    '\n' => {}
                    '\\' => out.push('\\'),
                    '\'' => out.push('\''),
                    '"' => out.push('"'),
                    'a' => out.push('\u{7}'),
                    'b' => out.push('\u{8}'),
                    'f' => out.push('\u{c}'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    'v' => out.push('\u{b}'),
                    '0'..='7' => {
                        let mut code = n.to_digit(8).unwrap_or(0);
                        for _ in 0..2 {
                            match self.peek().and_then(|d| d.to_digit(8)) {
                                Some(d) => {
                                    code = code * 8 + d;
                                    self.pos += 1;
                                }
                                None => break,
                            }
                        }
                        out.push(char::from_u32(code).ok_or("invalid escape")?);
                    }
                    'x' | 'u' | 'U' => {
                        let len = match n {
                            'x' => 2,
                            'u' => 4,
                            _ => 8,
                        };
                        let hex: String = (0..len).filter_map(|i| self.peek_at(i)).collect();
                        if hex.len() != len || !hex.chars().all(|h| h.is_ascii_hexdigit()) {
                            return Err(format!("truncated \\{n} escape"));
                        }
                        self.pos += len;
                        let code = u32::from_str_radix(&hex, 16).map_err(|e| e.to_string())?;
                        out.push(char::from_u32(code).ok_or("illegal Unicode character")?);
                    }
                    'N' => return Err("\\N{...} escapes are not supported".to_string()),
                    other => {
                        // Unknown escapes keep the backslash (Python emits a SyntaxWarning).
                        out.push('\\');
                        out.push(other);
                    }
                }
                continue;
            }
            out.push(c);
            self.pos += 1;
        }
    }
}

/// Convert a literal dict key to a string key (Python allows any hashable key).
fn dict_key(key: &Value) -> Result<String, String> {
    match key {
        Value::Str(s) => Ok(s.clone()),
        Value::List(_) | Value::Map(_) => Err("unhashable type".to_string()),
        other => Ok(json_key(other)),
    }
}

/// The key `json.dumps` writes for a non-string dict key (`1` -> `"1"`, `True` -> `"true"`).
pub fn json_key(key: &Value) -> String {
    match key {
        Value::Str(s) => s.clone(),
        Value::Null => "null".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => json_float(*f),
        other => py_repr(other),
    }
}

/// Port of `meerschaum.utils.misc.parse_arguments_str`: parse the inside of a call
/// (`"'a', 'b', instance='sql:main'"`) into positional and keyword arguments.
///
/// PARITY: the Python function splits on every `,` and on the first `=` of each part, so string
/// arguments containing `,` or `=` are split apart (`utils/misc.py:1476`).
pub fn parse_arguments_str(args_str: &str) -> Result<(Vec<Value>, Map), String> {
    let mut args = Vec::new();
    let mut kwargs = Map::new();
    for part in args_str.split(',') {
        if let Some((key, val)) = part.split_once('=') {
            kwargs.insert(key.trim().to_string(), literal_eval(val)?);
        } else {
            args.push(literal_eval(part.trim())?);
        }
    }
    Ok((args, kwargs))
}

/// Port of `meerschaum.utils.misc.string_to_dict`: parse JSON or the simple `a:1,b:2` syntax.
pub fn string_to_dict(params_string: &str) -> Result<Map, String> {
    if params_string.is_empty() {
        return Ok(Map::new());
    }
    let chars: Vec<char> = params_string.chars().collect();
    // PARITY: "a weird edge case" for strings wrapped in one extra character (misc.py:185).
    if chars.len() > 4 && chars[1] == '{' && chars[chars.len() - 2] == '}' {
        let inner: String = chars[1..chars.len() - 1].iter().collect();
        return parse_json_map(&inner);
    }
    if params_string.starts_with('{') {
        return parse_json_map(params_string);
    }

    let mut items: Vec<String> = Vec::new();
    let (mut bracket_level, mut brace_level) = (0i32, 0i32);
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = '\0';
    for (i, &c) in chars.iter().enumerate() {
        if in_quotes {
            if c == quote_char && (i == 0 || chars[i - 1] != '\\') {
                in_quotes = false;
            }
        } else if c == '"' || c == '\'' {
            in_quotes = true;
            quote_char = c;
        } else if c == '[' {
            bracket_level += 1;
        } else if c == ']' {
            bracket_level -= 1;
        } else if c == '{' {
            brace_level += 1;
        } else if c == '}' {
            brace_level -= 1;
        } else if c == ',' && bracket_level == 0 && brace_level == 0 {
            items.push(std::mem::take(&mut current));
            continue;
        }
        current.push(c);
    }
    if !current.is_empty() {
        items.push(current);
    }

    let mut out = Map::new();
    for param in items {
        let param = param.trim();
        if param.is_empty() {
            continue;
        }
        let Some((key, raw_val)) = param.split_once(':') else {
            continue;
        };
        let val = literal_eval(raw_val).unwrap_or_else(|_| Value::Str(raw_val.to_string()));
        out.insert(key.to_string(), val);
    }
    Ok(out)
}

fn parse_json_map(s: &str) -> Result<Map, String> {
    match Value::from_json_str(s).map_err(|e| e.to_string())? {
        Value::Map(m) => Ok(m),
        other => Err(format!("expected a JSON object, got {}", other.type_name())),
    }
}

// ---------------------------------------------------------------------------------------------
// Python string semantics (`str.isspace`, `strip`, `splitlines`, `textwrap`, `str.format`)
// ---------------------------------------------------------------------------------------------

/// CPython's `str.isspace()` for one character.
///
/// This differs from [`char::is_whitespace`] for U+001C..U+001F, which Python treats as
/// whitespace (bidirectional class B/S).
pub fn py_isspace(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n'
            | '\u{0b}'
            | '\u{0c}'
            | '\r'
            | '\u{1c}'
            | '\u{1d}'
            | '\u{1e}'
            | '\u{1f}'
            | ' '
            | '\u{85}'
            | '\u{a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}'
    )
}

/// `str.isspace()` for a whole string (`False` for the empty string).
pub fn py_str_isspace(s: &str) -> bool {
    !s.is_empty() && s.chars().all(py_isspace)
}

/// `str.strip()` with no arguments.
pub fn py_strip(s: &str) -> &str {
    s.trim_matches(py_isspace)
}

/// `str.lstrip()` with no arguments.
pub fn py_lstrip(s: &str) -> &str {
    s.trim_start_matches(py_isspace)
}

/// `str.rstrip()` with no arguments.
pub fn py_rstrip(s: &str) -> &str {
    s.trim_end_matches(py_isspace)
}

/// `str.splitlines(keepends)`, splitting on every boundary Python recognizes.
pub fn py_splitlines(s: &str, keepends: bool) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes_len = s.len();
    let mut start = 0;
    let mut iter = s.char_indices().peekable();
    while let Some((i, c)) = iter.next() {
        let is_break = matches!(
            c,
            '\n' | '\r'
                | '\u{0b}'
                | '\u{0c}'
                | '\u{1c}'
                | '\u{1d}'
                | '\u{1e}'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        );
        if !is_break {
            continue;
        }
        let mut end = i + c.len_utf8();
        if c == '\r' {
            if let Some(&(_, '\n')) = iter.peek() {
                iter.next();
                end += 1;
            }
        }
        out.push(if keepends {
            &s[start..end]
        } else {
            &s[start..i]
        });
        start = end;
    }
    if start < bytes_len {
        out.push(&s[start..]);
    }
    out
}

/// `textwrap.dedent` as implemented in CPython 3.14.
///
/// The margin is the common `' '`/`'\t'` prefix of the lexicographically smallest and largest
/// non-blank lines; whitespace-only lines become empty.
pub fn dedent(text: &str) -> String {
    let lines: Vec<&str> = text.split('\n').collect();
    let non_blank: Vec<&str> = lines
        .iter()
        .copied()
        .filter(|l| !l.is_empty() && !py_str_isspace(l))
        .collect();
    // Python compares strings by code point, which is also UTF-8 byte order.
    let l1 = non_blank.iter().min().copied().unwrap_or("");
    let l2 = non_blank.iter().max().copied().unwrap_or("");
    let l2_chars: Vec<char> = l2.chars().collect();
    let mut margin = 0usize;
    for (i, c) in l1.chars().enumerate() {
        margin = i;
        if l2_chars.get(i) != Some(&c) || !(c == ' ' || c == '\t') {
            break;
        }
    }
    let out: Vec<String> = lines
        .iter()
        .map(|l| {
            if py_str_isspace(l) {
                String::new()
            } else {
                l.chars().skip(margin).collect()
            }
        })
        .collect();
    out.join("\n")
}

/// `textwrap.indent(text, prefix)` with the default predicate.
pub fn indent(text: &str, prefix: &str) -> String {
    let mut out = String::with_capacity(text.len() + prefix.len() * 4);
    for line in py_splitlines(text, true) {
        if !py_str_isspace(line) {
            out.push_str(prefix);
        }
        out.push_str(line);
    }
    out
}

/// `textwrap.dedent(s).lstrip().rstrip()`, the idiom used across `utils/sql.py`.
pub fn dedent_strip(s: &str) -> String {
    py_strip(&dedent(s)).to_string()
}

/// `str.format(**kwargs)` for templates with named fields only (`{name}`, `{{`, `}}`).
///
/// Unknown fields are left untouched (Python would raise `KeyError`; the crate's templates never
/// reference unknown fields).
pub fn py_format(template: &str, args: &[(&str, &str)]) -> String {
    let mut out = String::with_capacity(template.len() + 64);
    let mut chars = template.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        match c {
            '{' => {
                if let Some(&(_, '{')) = chars.peek() {
                    chars.next();
                    out.push('{');
                    continue;
                }
                let rest = &template[i + 1..];
                if let Some(close) = rest.find('}') {
                    let key = &rest[..close];
                    if let Some((_, val)) = args.iter().find(|(k, _)| *k == key) {
                        out.push_str(val);
                        // Skip the key and the closing brace.
                        for _ in 0..key.chars().count() + 1 {
                            chars.next();
                        }
                        continue;
                    }
                }
                out.push('{');
            }
            '}' => {
                if let Some(&(_, '}')) = chars.peek() {
                    chars.next();
                }
                out.push('}');
            }
            c => out.push(c),
        }
    }
    out
}

/// A Python `datetime.datetime`: naive or with a fixed UTC offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyDateTime {
    /// `tzinfo is None`.
    Naive(NaiveDateTime),
    /// Timezone-aware with a fixed offset.
    Aware(DateTime<FixedOffset>),
}

impl PyDateTime {
    /// A UTC-aware datetime.
    pub fn utc(dt: DateTime<Utc>) -> Self {
        PyDateTime::Aware(dt.with_timezone(&Utc.fix()))
    }

    /// Whether `tzinfo is not None`.
    pub fn is_aware(&self) -> bool {
        matches!(self, PyDateTime::Aware(_))
    }

    /// The wall-clock (local) naive datetime.
    pub fn naive_local(&self) -> NaiveDateTime {
        match self {
            PyDateTime::Naive(n) => *n,
            PyDateTime::Aware(a) => a.naive_local(),
        }
    }

    /// Microseconds (Python datetimes have microsecond resolution).
    pub fn microsecond(&self) -> u32 {
        self.naive_local().nanosecond() / 1_000
    }

    /// `dt.astimezone(timezone.utc)` for aware values; naive values are returned unchanged.
    pub fn to_utc(&self) -> Self {
        match self {
            PyDateTime::Naive(_) => *self,
            PyDateTime::Aware(a) => PyDateTime::Aware(a.with_timezone(&Utc.fix())),
        }
    }

    /// `dt.replace(tzinfo=None)`.
    pub fn strip_tz(&self) -> Self {
        PyDateTime::Naive(self.naive_local())
    }

    fn date_time_part(&self, sep: char) -> String {
        let n = self.naive_local();
        let mut s = format!(
            "{:04}-{:02}-{:02}{}{:02}:{:02}:{:02}",
            n.year(),
            n.month(),
            n.day(),
            sep,
            n.hour(),
            n.minute(),
            n.second()
        );
        let us = self.microsecond();
        if us != 0 {
            s.push_str(&format!(".{:06}", us));
        }
        s
    }

    fn offset_part(&self) -> String {
        match self {
            PyDateTime::Naive(_) => String::new(),
            PyDateTime::Aware(a) => {
                let secs = a.offset().local_minus_utc();
                let sign = if secs < 0 { '-' } else { '+' };
                let secs = secs.abs();
                let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
                if s != 0 {
                    format!("{sign}{h:02}:{m:02}:{s:02}")
                } else {
                    format!("{sign}{h:02}:{m:02}")
                }
            }
        }
    }

    /// `str(dt)`: `YYYY-MM-DD HH:MM:SS[.ffffff][+HH:MM]`.
    pub fn py_str(&self) -> String {
        format!("{}{}", self.date_time_part(' '), self.offset_part())
    }

    /// `dt.isoformat()`: `YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM]`.
    pub fn isoformat(&self) -> String {
        format!("{}{}", self.date_time_part('T'), self.offset_part())
    }

    /// `dt.strftime('%Y-%m-%d %H:%M:%S.%f')`.
    pub fn strftime_full(&self) -> String {
        let n = self.naive_local();
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            n.year(),
            n.month(),
            n.day(),
            n.hour(),
            n.minute(),
            n.second(),
            self.microsecond()
        )
    }

    /// `dt.strftime('%Y%m%d%H%M%S')`.
    pub fn strftime_compact(&self) -> String {
        let n = self.naive_local();
        format!(
            "{:04}{:02}{:02}{:02}{:02}{:02}",
            n.year(),
            n.month(),
            n.day(),
            n.hour(),
            n.minute(),
            n.second()
        )
    }

    /// Parse Python's `isoformat()` output (the fixture encoding).
    pub fn parse_isoformat(s: &str) -> Option<Self> {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Some(PyDateTime::Aware(dt));
        }
        for fmt in ["%Y-%m-%dT%H:%M:%S%.f%:z", "%Y-%m-%d %H:%M:%S%.f%:z"] {
            if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
                return Some(PyDateTime::Aware(dt));
            }
        }
        for fmt in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
            if let Ok(n) = NaiveDateTime::parse_from_str(s, fmt) {
                return Some(PyDateTime::Naive(n));
            }
        }
        None
    }
}

/// Python's `str.isupper()`: at least one cased character and no lowercase ones.
pub fn py_isupper(s: &str) -> bool {
    let mut cased = false;
    for c in s.chars() {
        if c.is_lowercase() {
            return false;
        }
        if c.is_uppercase() {
            cased = true;
        }
    }
    cased
}

/// Python's `repr(bytes)`.
pub fn bytes_repr(b: &[u8]) -> String {
    let quote = if b.contains(&b'\'') && !b.contains(&b'"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::from("b");
    out.push(quote);
    for &c in b {
        match c {
            b'\\' => out.push_str("\\\\"),
            b'\t' => out.push_str("\\t"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            c if c == quote as u8 => {
                out.push('\\');
                out.push(c as char);
            }
            0x20..=0x7e => out.push(c as char),
            _ => out.push_str(&format!("\\x{c:02x}")),
        }
    }
    out.push(quote);
    out
}

/// Python's `repr()` of a JSON document parsed with `serde_json` (`{'a': 1, 'b': [True, None]}`).
///
/// Unlike converting to a [`Value`] first, integers beyond `i64` keep their digits.
pub fn py_repr_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "None".to_string(),
        serde_json::Value::Bool(b) => if *b { "True" } else { "False" }.to_string(),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(u) = n.as_u64() {
                u.to_string()
            } else {
                float_repr(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        serde_json::Value::String(s) => str_repr(s),
        serde_json::Value::Array(a) => format!(
            "[{}]",
            a.iter().map(py_repr_json).collect::<Vec<_>>().join(", ")
        ),
        serde_json::Value::Object(o) => format!(
            "{{{}}}",
            o.iter()
                .map(|(k, v)| format!("{}: {}", str_repr(k), py_repr_json(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_reprs() {
        for (f, s) in [
            (1e16, "1e+16"),
            (1e-5, "1e-05"),
            (0.0001, "0.0001"),
            (1.5, "1.5"),
            (123456789.123, "123456789.123"),
            (-0.0, "-0.0"),
            (2.5e-300, "2.5e-300"),
            (1.0, "1.0"),
            (1e15, "1000000000000000.0"),
            (0.1 + 0.2, "0.30000000000000004"),
            (-12.0, "-12.0"),
            (1.0 / 60.0, "0.016666666666666666"),
            (1.0 / 86400.0, "1.1574074074074073e-05"),
            (123456789.125, "123456789.125"),
            (1e20, "1e+20"),
            (9007199254740993.0, "9007199254740992.0"),
        ] {
            assert_eq!(float_repr(f), s);
        }
    }

    #[test]
    fn json_and_repr() {
        let v = Value::from_json_str(r#"{"a": "é\u007f", "b": [1.0, null, true]}"#).unwrap();
        assert_eq!(
            json_dumps(&v, JsonStyle::Default, true),
            r#"{"a": "\u00e9\u007f", "b": [1.0, null, true]}"#
        );
        let v = Value::from_json_str(r#"{"a":"it's","b":"x\"y","c":"both'\"","d":"\u0000\té"}"#)
            .unwrap();
        assert_eq!(
            py_repr(&v),
            r#"{'a': "it's", 'b': 'x"y', 'c': 'both\'"', 'd': '\x00\té'}"#
        );
    }

    #[test]
    fn literals() {
        assert_eq!(literal_eval(" 'x'").unwrap(), Value::from("x"));
        assert_eq!(literal_eval("'a' \"b\"").unwrap(), Value::from("ab"));
        assert_eq!(literal_eval("-1").unwrap(), Value::Int(-1));
        assert_eq!(literal_eval("0x1_f").unwrap(), Value::Int(31));
        assert_eq!(literal_eval("1_000.5").unwrap(), Value::Float(1000.5));
        assert_eq!(literal_eval("None").unwrap(), Value::Null);
        assert!(literal_eval("").is_err());
        assert!(literal_eval("foo").is_err());
        assert_eq!(
            literal_eval("['a', (1,), {'k': None}]").unwrap(),
            Value::from_json_str(r#"["a", [1], {"k": null}]"#).unwrap()
        );
        assert!(literal_eval_constant("-1").is_err());
    }

    #[test]
    fn ints() {
        assert_eq!(parse_py_int(" +1_000 "), Some(1000));
        assert_eq!(parse_py_int("1__0"), None);
        assert!(is_int("4326.0"));
        assert!(!is_int("inf"));
        // Python strips `str.isspace()` characters, including U+001C..U+001F.
        assert_eq!(parse_py_int("\u{1c}7\u{1f}"), Some(7));
    }

    #[test]
    fn dedent_like_python() {
        assert_eq!(dedent("    a\n      b\n    c"), "a\n  b\nc");
        assert_eq!(dedent("  a\n\n  b"), "a\n\nb");
        assert_eq!(dedent("a\n    b"), "a\n    b");
        assert_eq!(dedent("   \n  x"), "\nx");
    }

    #[test]
    fn indent_like_python() {
        assert_eq!(indent("a\n\nb", "    "), "    a\n\n    b");
        assert_eq!(indent("a\r\nb\n", "  "), "  a\r\n  b\n");
    }

    #[test]
    fn format_fields() {
        assert_eq!(py_format("a {x} {{y}} {z}", &[("x", "1")]), "a 1 {y} {z}");
    }

    #[test]
    fn datetime_strings() {
        let dt = PyDateTime::parse_isoformat("2024-01-02T03:04:05.000006+00:00").unwrap();
        assert_eq!(dt.py_str(), "2024-01-02 03:04:05.000006+00:00");
        let n = PyDateTime::parse_isoformat("2024-01-02T03:04:05").unwrap();
        assert_eq!(n.isoformat(), "2024-01-02T03:04:05");
    }

    #[test]
    fn bytes_and_upper() {
        assert_eq!(bytes_repr(b"a\x00'"), "b\"a\\x00'\"");
        assert!(py_isupper("TEXT PRIMARY"));
        assert!(!py_isupper("Text"));
        assert!(!py_isupper("123"));
    }
}
