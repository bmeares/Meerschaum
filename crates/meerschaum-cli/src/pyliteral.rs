//! A port of `ast.literal_eval` for the simple `--params key:value` format.
//!
//! `string_to_dict` (`meerschaum/utils/misc.py`) evaluates each value with `ast.literal_eval` and
//! falls back to the raw string on *any* exception, so this only has to decide "is this a Python
//! literal, and which value is it". Supported: `int` (decimal, hex, octal, binary, underscores),
//! `float`, `complex` (`1j`, `1+2j`), `str`/`bytes` with prefixes, escapes, triple quotes and
//! implicit concatenation, `True`/`False`/`None`, `...`, lists, tuples, sets, dicts, `set()`,
//! unary `+`/`-` on numbers, comments and newlines inside brackets.
//!
//! Not supported (evaluates to an error, i.e. the raw-string fallback): `\N{NAME}` escapes and
//! hex/octal/binary integers wider than 128 bits.

use crate::value::{NotParsed, PyValue};

/// Evaluate a Python literal. `Err(())` stands for any exception `literal_eval` would raise.
pub fn literal_eval(source: &str) -> Result<PyValue, NotParsed> {
    literal_eval_inner(source).map_err(|()| NotParsed)
}

fn literal_eval_inner(source: &str) -> Result<PyValue, ()> {
    // PARITY: `ast.literal_eval` strips leading spaces and tabs only (Lib/ast.py `literal_eval`).
    let source = source.trim_start_matches([' ', '\t']);
    let tokens = tokenize(source)?;
    let mut p = Parser {
        tokens: &tokens,
        pos: 0,
    };
    let node = p.top_level()?;
    if p.pos != tokens.len() {
        return Err(());
    }
    convert(&node)
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Int(PyValue),
    Float(f64),
    Imag(f64),
    Str(String),
    Bytes(Vec<u8>),
    Name(String),
    Op(&'static str),
}

#[derive(Debug, Clone, PartialEq)]
enum Node {
    Const(PyValue),
    List(Vec<Node>),
    Tuple(Vec<Node>),
    Set(Vec<Node>),
    Dict(Vec<(Node, Node)>),
    Name(String),
    Call(String),
    Unary(char, Box<Node>),
    BinOp(Box<Node>, char, Box<Node>),
}

fn is_id_start(c: char) -> bool {
    c == '_' || c.is_alphabetic()
}

fn is_id_continue(c: char) -> bool {
    c == '_' || c.is_alphanumeric()
}

fn tokenize(src: &str) -> Result<Vec<Tok>, ()> {
    let chars: Vec<char> = src.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    let mut depth: i32 = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == ' ' || c == '\t' || c == '\x0c' {
            i += 1;
            continue;
        }
        if c == '\\' && matches!(chars.get(i + 1), Some('\n')) {
            i += 2;
            continue;
        }
        if c == '\n' || c == '\r' {
            // Newlines are insignificant inside brackets; outside, only trailing ones are allowed.
            if depth == 0 {
                let rest: String = chars[i..].iter().collect();
                let rest_trim =
                    rest.trim_matches(|c: char| c == '\n' || c == '\r' || c == ' ' || c == '\t');
                if !rest_trim.is_empty() && !rest_trim.starts_with('#') {
                    return Err(());
                }
            }
            i += 1;
            continue;
        }
        if c == '#' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        // String literals with optional prefixes.
        if let Some((tok, next)) = lex_string(&chars, i)? {
            // Implicit concatenation of adjacent literals.
            match (toks.last_mut(), &tok) {
                (Some(Tok::Str(prev)), Tok::Str(s)) => prev.push_str(s),
                (Some(Tok::Bytes(prev)), Tok::Bytes(b)) => prev.extend_from_slice(b),
                (Some(Tok::Str(_)), Tok::Bytes(_)) | (Some(Tok::Bytes(_)), Tok::Str(_)) => {
                    return Err(());
                }
                _ => toks.push(tok),
            }
            i = next;
            continue;
        }
        if c.is_ascii_digit()
            || (c == '.' && matches!(chars.get(i + 1), Some(d) if d.is_ascii_digit()))
        {
            let (tok, next) = lex_number(&chars, i)?;
            toks.push(tok);
            i = next;
            continue;
        }
        if is_id_start(c) {
            let start = i;
            while i < chars.len() && is_id_continue(chars[i]) {
                i += 1;
            }
            toks.push(Tok::Name(chars[start..i].iter().collect()));
            continue;
        }
        if c == '.' && chars.get(i + 1) == Some(&'.') && chars.get(i + 2) == Some(&'.') {
            toks.push(Tok::Op("..."));
            i += 3;
            continue;
        }
        let op: &'static str = match c {
            '(' => "(",
            ')' => ")",
            '[' => "[",
            ']' => "]",
            '{' => "{",
            '}' => "}",
            ',' => ",",
            ':' => ":",
            '+' => "+",
            '-' => "-",
            _ => return Err(()),
        };
        match op {
            "(" | "[" | "{" => depth += 1,
            ")" | "]" | "}" => depth -= 1,
            _ => {}
        }
        if depth < 0 {
            return Err(());
        }
        // `:=` and similar multi-char operators are not literals.
        if op == ":" && chars.get(i + 1) == Some(&'=') {
            return Err(());
        }
        toks.push(Tok::Op(op));
        i += 1;
    }
    if depth != 0 {
        return Err(());
    }
    Ok(toks)
}

fn lex_string(chars: &[char], start: usize) -> Result<Option<(Tok, usize)>, ()> {
    let mut i = start;
    let mut prefix = String::new();
    while i < chars.len() && chars[i].is_ascii_alphabetic() && prefix.len() < 3 {
        prefix.push(chars[i].to_ascii_lowercase());
        i += 1;
    }
    let quote = match chars.get(i) {
        Some(&q @ ('\'' | '"')) => q,
        _ => return Ok(None),
    };
    // A name immediately before a quote must be a valid prefix, otherwise this is not a string.
    let valid = [
        "", "r", "u", "b", "br", "rb", "f", "fr", "rf", "t", "tr", "rt",
    ];
    if !valid.contains(&prefix.as_str()) {
        if prefix.is_empty() {
            return Ok(None);
        }
        // e.g. `abc'x'` is a name followed by a string: a syntax error.
        return if start > 0 && is_id_continue(chars[start - 1]) {
            Ok(None)
        } else {
            Err(())
        };
    }
    if start > 0 && is_id_continue(chars[start - 1]) && !prefix.is_empty() {
        return Ok(None);
    }
    let raw = prefix.contains('r');
    let is_bytes = prefix.contains('b');
    if prefix.contains('f') || prefix.contains('t') {
        // f-strings and t-strings are not literals for `literal_eval`.
        return Err(());
    }
    let triple = chars.get(i + 1) == Some(&quote) && chars.get(i + 2) == Some(&quote);
    i += if triple { 3 } else { 1 };
    let mut body = String::new();
    loop {
        let c = *chars.get(i).ok_or(())?;
        if c == '\\' {
            let next = *chars.get(i + 1).ok_or(())?;
            body.push(c);
            body.push(next);
            i += 2;
            continue;
        }
        if c == quote {
            if !triple {
                i += 1;
                break;
            }
            if chars.get(i + 1) == Some(&quote) && chars.get(i + 2) == Some(&quote) {
                i += 3;
                break;
            }
        }
        if (c == '\n' || c == '\r') && !triple {
            return Err(());
        }
        body.push(c);
        i += 1;
    }
    let tok = if is_bytes {
        if !body.is_ascii() {
            return Err(());
        }
        Tok::Bytes(
            unescape(&body, raw, true)?
                .chars()
                .map(|c| c as u8)
                .collect(),
        )
    } else {
        Tok::Str(unescape(&body, raw, false)?)
    };
    Ok(Some((tok, i)))
}

fn unescape(body: &str, raw: bool, is_bytes: bool) -> Result<String, ()> {
    if raw {
        return Ok(body.to_string());
    }
    let chars: Vec<char> = body.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c != '\\' {
            out.push(c);
            i += 1;
            continue;
        }
        let e = chars[i + 1];
        i += 2;
        match e {
            '\n' => {}
            '\\' => out.push('\\'),
            '\'' => out.push('\''),
            '"' => out.push('"'),
            'a' => out.push('\u{07}'),
            'b' => out.push('\u{08}'),
            'f' => out.push('\u{0c}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'v' => out.push('\u{0b}'),
            '0'..='7' => {
                let mut code = e.to_digit(8).unwrap_or(0);
                let mut n = 1;
                while n < 3 && i < chars.len() && chars[i].is_digit(8) {
                    code = code * 8 + chars[i].to_digit(8).unwrap_or(0);
                    i += 1;
                    n += 1;
                }
                if is_bytes {
                    out.push(char::from_u32(code & 0xff).ok_or(())?);
                } else {
                    out.push(char::from_u32(code).ok_or(())?);
                }
            }
            'x' => {
                let hex: String = chars.get(i..i + 2).ok_or(())?.iter().collect();
                if hex.len() != 2 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Err(());
                }
                i += 2;
                out.push(char::from_u32(u32::from_str_radix(&hex, 16).map_err(|_| ())?).ok_or(())?);
            }
            'u' | 'U' if !is_bytes => {
                let n = if e == 'u' { 4 } else { 8 };
                let hex: String = chars.get(i..i + n).ok_or(())?.iter().collect();
                if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Err(());
                }
                i += n;
                out.push(char::from_u32(u32::from_str_radix(&hex, 16).map_err(|_| ())?).ok_or(())?);
            }
            'N' if !is_bytes => return Err(()),
            other => {
                // Unknown escapes keep the backslash (with a SyntaxWarning in Python).
                out.push('\\');
                out.push(other);
            }
        }
    }
    Ok(out)
}

fn digits_with_underscores(chars: &[char], mut i: usize, radix: u32) -> (String, usize, bool) {
    // Returns (digits without underscores, next index, valid underscores).
    let mut out = String::new();
    let mut valid = true;
    let mut prev_underscore = false;
    let mut any = false;
    while i < chars.len() {
        let c = chars[i];
        if c == '_' {
            if prev_underscore || !any {
                valid = false;
            }
            prev_underscore = true;
            i += 1;
            continue;
        }
        if c.is_digit(radix) {
            out.push(c);
            any = true;
            prev_underscore = false;
            i += 1;
            continue;
        }
        break;
    }
    if prev_underscore {
        valid = false;
    }
    (out, i, valid)
}

fn lex_number(chars: &[char], start: usize) -> Result<(Tok, usize), ()> {
    let mut i = start;
    // Prefixed integers.
    if chars[i] == '0' && matches!(chars.get(i + 1), Some('x' | 'X' | 'o' | 'O' | 'b' | 'B')) {
        let radix = match chars[i + 1].to_ascii_lowercase() {
            'x' => 16,
            'o' => 8,
            _ => 2,
        };
        i += 2;
        // An underscore may directly follow the base prefix.
        if chars.get(i) == Some(&'_') {
            i += 1;
        }
        let (digits, next, valid) = digits_with_underscores(chars, i, radix);
        if digits.is_empty() || !valid {
            return Err(());
        }
        if matches!(chars.get(next), Some(c) if is_id_continue(*c)) {
            return Err(());
        }
        let value = i128::from_str_radix(&digits, radix).map_err(|_| ())?;
        return Ok((Tok::Int(PyValue::Int(value)), next));
    }
    let (int_part, next, valid_int) = digits_with_underscores(chars, i, 10);
    i = next;
    let mut is_float = false;
    let mut frac = String::new();
    if chars.get(i) == Some(&'.') {
        is_float = true;
        i += 1;
        if matches!(chars.get(i), Some(c) if c.is_ascii_digit()) {
            let (f, next, valid) = digits_with_underscores(chars, i, 10);
            if !valid {
                return Err(());
            }
            frac = f;
            i = next;
        }
    }
    if !valid_int {
        return Err(());
    }
    let mut exp = String::new();
    if matches!(chars.get(i), Some('e' | 'E')) {
        let mut j = i + 1;
        let mut sign = String::new();
        if matches!(chars.get(j), Some('+' | '-')) {
            sign.push(chars[j]);
            j += 1;
        }
        if matches!(chars.get(j), Some(c) if c.is_ascii_digit()) {
            let (e, next, valid) = digits_with_underscores(chars, j, 10);
            if !valid {
                return Err(());
            }
            exp = format!("e{sign}{e}");
            is_float = true;
            i = next;
        } else {
            return Err(());
        }
    }
    let imag = matches!(chars.get(i), Some('j' | 'J'));
    if imag {
        i += 1;
    }
    if matches!(chars.get(i), Some(c) if is_id_continue(*c)) {
        return Err(());
    }
    let text = format!(
        "{}{}{}",
        if int_part.is_empty() { "0" } else { &int_part },
        if is_float {
            format!(".{}", if frac.is_empty() { "0" } else { &frac })
        } else {
            String::new()
        },
        exp
    );
    if imag {
        return Ok((Tok::Imag(text.parse::<f64>().map_err(|_| ())?), i));
    }
    if is_float {
        return Ok((Tok::Float(text.parse::<f64>().map_err(|_| ())?), i));
    }
    // Decimal integers: leading zeros are only allowed for zero itself.
    if int_part.len() > 1 && int_part.starts_with('0') && int_part.chars().any(|c| c != '0') {
        return Err(());
    }
    Ok((
        Tok::Int(crate::types::int_from_decimal_digits(&int_part)),
        i,
    ))
}

struct Parser<'a> {
    tokens: &'a [Tok],
    pos: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos)
    }

    fn eat_op(&mut self, op: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Op(o)) if *o == op) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// Eval-mode input: an expression list (a bare `1, 2` is a tuple).
    fn top_level(&mut self) -> Result<Node, ()> {
        let first = self.expr()?;
        if !matches!(self.peek(), Some(Tok::Op(","))) {
            return Ok(first);
        }
        let mut items = vec![first];
        while self.eat_op(",") {
            if self.peek().is_none() {
                break;
            }
            items.push(self.expr()?);
        }
        Ok(Node::Tuple(items))
    }

    /// `arith_expr` level: binary `+`/`-` (left-associative) over unary expressions.
    fn expr(&mut self) -> Result<Node, ()> {
        let mut left = self.unary()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Op("+")) => '+',
                Some(Tok::Op("-")) => '-',
                _ => break,
            };
            self.pos += 1;
            let right = self.unary()?;
            left = Node::BinOp(Box::new(left), op, Box::new(right));
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Node, ()> {
        match self.peek() {
            Some(Tok::Op("+")) => {
                self.pos += 1;
                Ok(Node::Unary('+', Box::new(self.unary()?)))
            }
            Some(Tok::Op("-")) => {
                self.pos += 1;
                Ok(Node::Unary('-', Box::new(self.unary()?)))
            }
            _ => self.atom(),
        }
    }

    fn atom(&mut self) -> Result<Node, ()> {
        let tok = self.peek().cloned().ok_or(())?;
        self.pos += 1;
        match tok {
            Tok::Int(v) => Ok(Node::Const(v)),
            Tok::Float(f) => Ok(Node::Const(PyValue::Float(f))),
            Tok::Imag(f) => Ok(Node::Const(PyValue::Complex(0.0, f))),
            Tok::Str(s) => Ok(Node::Const(PyValue::Str(s))),
            Tok::Bytes(b) => Ok(Node::Const(PyValue::Bytes(b))),
            Tok::Op("...") => Ok(Node::Const(PyValue::Ellipsis)),
            Tok::Name(name) => {
                let node = match name.as_str() {
                    "True" => Node::Const(PyValue::Bool(true)),
                    "False" => Node::Const(PyValue::Bool(false)),
                    "None" => Node::Const(PyValue::None),
                    _ => Node::Name(name.clone()),
                };
                if self.eat_op("(") {
                    if !self.eat_op(")") {
                        return Err(());
                    }
                    return Ok(Node::Call(name));
                }
                Ok(node)
            }
            Tok::Op("(") => {
                if self.eat_op(")") {
                    return Ok(Node::Tuple(vec![]));
                }
                let first = self.expr()?;
                if self.eat_op(")") {
                    return Ok(first);
                }
                let mut items = vec![first];
                while self.eat_op(",") {
                    if self.eat_op(")") {
                        return Ok(Node::Tuple(items));
                    }
                    items.push(self.expr()?);
                }
                if self.eat_op(")") {
                    Ok(Node::Tuple(items))
                } else {
                    Err(())
                }
            }
            Tok::Op("[") => {
                let mut items = Vec::new();
                loop {
                    if self.eat_op("]") {
                        return Ok(Node::List(items));
                    }
                    items.push(self.expr()?);
                    if !self.eat_op(",") {
                        return if self.eat_op("]") {
                            Ok(Node::List(items))
                        } else {
                            Err(())
                        };
                    }
                }
            }
            Tok::Op("{") => {
                if self.eat_op("}") {
                    return Ok(Node::Dict(vec![]));
                }
                let first = self.expr()?;
                if self.eat_op(":") {
                    let mut items = vec![(first, self.expr()?)];
                    loop {
                        if !self.eat_op(",") {
                            return if self.eat_op("}") {
                                Ok(Node::Dict(items))
                            } else {
                                Err(())
                            };
                        }
                        if self.eat_op("}") {
                            return Ok(Node::Dict(items));
                        }
                        let k = self.expr()?;
                        if !self.eat_op(":") {
                            return Err(());
                        }
                        items.push((k, self.expr()?));
                    }
                }
                let mut items = vec![first];
                loop {
                    if !self.eat_op(",") {
                        return if self.eat_op("}") {
                            Ok(Node::Set(items))
                        } else {
                            Err(())
                        };
                    }
                    if self.eat_op("}") {
                        return Ok(Node::Set(items));
                    }
                    items.push(self.expr()?);
                }
            }
            _ => Err(()),
        }
    }
}

fn convert_num(node: &Node) -> Result<PyValue, ()> {
    // PARITY: `_convert_num` checks `type(value) in (int, float, complex)`, so `bool` is rejected.
    match node {
        Node::Const(
            v @ (PyValue::Int(_) | PyValue::BigInt(_) | PyValue::Float(_) | PyValue::Complex(..)),
        ) => Ok(v.clone()),
        _ => Err(()),
    }
}

fn negate(v: PyValue) -> Result<PyValue, ()> {
    Ok(match v {
        PyValue::Int(i) => PyValue::Int(i.checked_neg().ok_or(())?),
        PyValue::BigInt(s) => PyValue::BigInt(match s.strip_prefix('-') {
            Some(rest) => rest.to_string(),
            None => format!("-{s}"),
        }),
        PyValue::Float(f) => PyValue::Float(-f),
        PyValue::Complex(r, i) => PyValue::Complex(-r, -i),
        _ => return Err(()),
    })
}

fn convert_signed_num(node: &Node) -> Result<PyValue, ()> {
    if let Node::Unary(op, operand) = node {
        let v = convert_num(operand)?;
        return if *op == '+' { Ok(v) } else { negate(v) };
    }
    convert_num(node)
}

fn convert(node: &Node) -> Result<PyValue, ()> {
    match node {
        Node::Const(v) => Ok(v.clone()),
        Node::Tuple(items) => Ok(PyValue::Tuple(
            items.iter().map(convert).collect::<Result<_, _>>()?,
        )),
        Node::List(items) => Ok(PyValue::List(
            items.iter().map(convert).collect::<Result<_, _>>()?,
        )),
        Node::Set(items) => {
            let mut out: Vec<PyValue> = Vec::new();
            for item in items {
                let v = convert(item)?;
                if !v.is_hashable() {
                    return Err(());
                }
                if !out.iter().any(|x| x.py_eq(&v)) {
                    out.push(v);
                }
            }
            Ok(PyValue::Set(out))
        }
        Node::Call(name) if name == "set" => Ok(PyValue::Set(vec![])),
        Node::Dict(items) => {
            let mut out: Vec<(PyValue, PyValue)> = Vec::new();
            for (k, v) in items {
                let k = convert(k)?;
                let v = convert(v)?;
                if !k.is_hashable() {
                    return Err(());
                }
                if let Some(slot) = out.iter_mut().find(|(x, _)| x.py_eq(&k)) {
                    slot.1 = v;
                } else {
                    out.push((k, v));
                }
            }
            Ok(PyValue::Dict(out))
        }
        Node::BinOp(left, op, right) => {
            let l = convert_signed_num(left)?;
            let r = convert_num(right)?;
            let lf = match l {
                PyValue::Int(i) => i as f64,
                PyValue::Float(f) => f,
                _ => return Err(()),
            };
            match r {
                PyValue::Complex(rr, ri) => {
                    if *op == '+' {
                        Ok(PyValue::Complex(lf + rr, ri))
                    } else {
                        Ok(PyValue::Complex(lf - rr, -ri))
                    }
                }
                _ => Err(()),
            }
        }
        _ => convert_signed_num(node),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals() {
        assert_eq!(literal_eval("1"), Ok(PyValue::Int(1)));
        assert_eq!(literal_eval(" -1.5e-3"), Ok(PyValue::Float(-0.0015)));
        assert_eq!(literal_eval("0x1F"), Ok(PyValue::Int(31)));
        assert_eq!(literal_eval("1_000"), Ok(PyValue::Int(1000)));
        assert!(literal_eval("1__0").is_err());
        assert!(literal_eval("01").is_err());
        assert_eq!(literal_eval("'a' 'b'"), Ok(PyValue::str("ab")));
        assert_eq!(
            literal_eval("(1,)"),
            Ok(PyValue::Tuple(vec![PyValue::Int(1)]))
        );
        assert_eq!(literal_eval("()"), Ok(PyValue::Tuple(vec![])));
        assert_eq!(literal_eval("set()"), Ok(PyValue::Set(vec![])));
        assert!(literal_eval("-True").is_err());
        assert!(literal_eval("--5").is_err());
        assert!(literal_eval("hello world").is_err());
        assert!(literal_eval("{[1]: 2}").is_err());
        assert_eq!(literal_eval("1+2j"), Ok(PyValue::Complex(1.0, 2.0)));
        assert_eq!(literal_eval("r'raw\\n'"), Ok(PyValue::str("raw\\n")));
        assert_eq!(literal_eval("'tab\\t'"), Ok(PyValue::str("tab\t")));
    }
}
