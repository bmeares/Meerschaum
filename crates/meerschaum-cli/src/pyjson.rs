//! A port of Python's `json.loads` (default arguments) into [`PyValue`].
//!
//! Differences from strict JSON that Python accepts and this reproduces: `NaN`, `Infinity` and
//! `-Infinity` literals; unbounded integers; duplicate object keys (the last value wins, the key
//! keeps its first position, as with `dict` assignment).

use crate::value::{NotParsed, PyValue};

/// Parse a JSON document the way `json.loads` does. Any error maps to `ValueError`
/// (`json.JSONDecodeError`), which callers report through `argparse`'s generic message.
pub fn loads(s: &str) -> Result<PyValue, NotParsed> {
    loads_inner(s).map_err(|()| NotParsed)
}

fn loads_inner(s: &str) -> Result<PyValue, ()> {
    let chars: Vec<char> = s.chars().collect();
    let mut p = JsonParser {
        chars: &chars,
        pos: 0,
    };
    p.skip_ws();
    let value = p.value(0)?;
    p.skip_ws();
    if p.pos != chars.len() {
        return Err(());
    }
    Ok(value)
}

struct JsonParser<'a> {
    chars: &'a [char],
    pos: usize,
}

/// Python's recursion limit makes very deep documents fail; mirror that loosely.
const MAX_DEPTH: usize = 900;

impl JsonParser<'_> {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.pos += 1;
        }
    }

    fn starts_with(&self, lit: &str) -> bool {
        let n = lit.chars().count();
        self.pos + n <= self.chars.len()
            && self.chars[self.pos..self.pos + n]
                .iter()
                .copied()
                .eq(lit.chars())
    }

    fn value(&mut self, depth: usize) -> Result<PyValue, ()> {
        if depth > MAX_DEPTH {
            return Err(());
        }
        match self.peek().ok_or(())? {
            '{' => self.object(depth),
            '[' => self.array(depth),
            '"' => Ok(PyValue::Str(self.string()?)),
            'n' if self.starts_with("null") => {
                self.pos += 4;
                Ok(PyValue::None)
            }
            't' if self.starts_with("true") => {
                self.pos += 4;
                Ok(PyValue::Bool(true))
            }
            'f' if self.starts_with("false") => {
                self.pos += 5;
                Ok(PyValue::Bool(false))
            }
            'N' if self.starts_with("NaN") => {
                self.pos += 3;
                Ok(PyValue::Float(f64::NAN))
            }
            'I' if self.starts_with("Infinity") => {
                self.pos += 8;
                Ok(PyValue::Float(f64::INFINITY))
            }
            '-' if self.starts_with("-Infinity") => {
                self.pos += 9;
                Ok(PyValue::Float(f64::NEG_INFINITY))
            }
            '-' | '0'..='9' => self.number(),
            _ => Err(()),
        }
    }

    fn object(&mut self, depth: usize) -> Result<PyValue, ()> {
        self.pos += 1;
        let mut items: Vec<(PyValue, PyValue)> = Vec::new();
        self.skip_ws();
        if self.peek() == Some('}') {
            self.pos += 1;
            return Ok(PyValue::Dict(items));
        }
        loop {
            self.skip_ws();
            if self.peek() != Some('"') {
                return Err(());
            }
            let key = self.string()?;
            self.skip_ws();
            if self.peek() != Some(':') {
                return Err(());
            }
            self.pos += 1;
            self.skip_ws();
            let value = self.value(depth + 1)?;
            if let Some(slot) = items
                .iter_mut()
                .find(|(k, _)| k.as_str() == Some(key.as_str()))
            {
                slot.1 = value;
            } else {
                items.push((PyValue::Str(key), value));
            }
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos += 1;
                }
                Some('}') => {
                    self.pos += 1;
                    return Ok(PyValue::Dict(items));
                }
                _ => return Err(()),
            }
        }
    }

    fn array(&mut self, depth: usize) -> Result<PyValue, ()> {
        self.pos += 1;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(']') {
            self.pos += 1;
            return Ok(PyValue::List(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value(depth + 1)?);
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos += 1;
                }
                Some(']') => {
                    self.pos += 1;
                    return Ok(PyValue::List(items));
                }
                _ => return Err(()),
            }
        }
    }

    fn hex4(&mut self) -> Result<u32, ()> {
        if self.pos + 4 > self.chars.len() {
            return Err(());
        }
        let s: String = self.chars[self.pos..self.pos + 4].iter().collect();
        if !s.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(());
        }
        self.pos += 4;
        u32::from_str_radix(&s, 16).map_err(|_| ())
    }

    fn string(&mut self) -> Result<String, ()> {
        self.pos += 1;
        let mut out = String::new();
        loop {
            let c = self.peek().ok_or(())?;
            self.pos += 1;
            match c {
                '"' => return Ok(out),
                '\\' => {
                    let e = self.peek().ok_or(())?;
                    self.pos += 1;
                    match e {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{08}'),
                        'f' => out.push('\u{0c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let mut code = self.hex4()?;
                            if (0xd800..0xdc00).contains(&code) && self.starts_with("\\u") {
                                let save = self.pos;
                                self.pos += 2;
                                let low = self.hex4()?;
                                if (0xdc00..0xe000).contains(&low) {
                                    code = 0x10000 + ((code - 0xd800) << 10) + (low - 0xdc00);
                                } else {
                                    self.pos = save;
                                }
                            }
                            // PARITY: Python keeps lone surrogates in `str`; Rust cannot, so they
                            // become U+FFFD here (json.loads in decoder.py `py_scanstring`).
                            out.push(char::from_u32(code).unwrap_or('\u{fffd}'));
                        }
                        _ => return Err(()),
                    }
                }
                c if (c as u32) < 0x20 => return Err(()),
                c => out.push(c),
            }
        }
    }

    fn number(&mut self) -> Result<PyValue, ()> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.pos += 1;
        }
        match self.peek() {
            Some('0') => self.pos += 1,
            Some('1'..='9') => {
                while matches!(self.peek(), Some('0'..='9')) {
                    self.pos += 1;
                }
            }
            _ => return Err(()),
        }
        let mut is_float = false;
        if self.peek() == Some('.') && matches!(self.chars.get(self.pos + 1), Some('0'..='9')) {
            is_float = true;
            self.pos += 1;
            while matches!(self.peek(), Some('0'..='9')) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            let save = self.pos;
            self.pos += 1;
            if matches!(self.peek(), Some('+' | '-')) {
                self.pos += 1;
            }
            if matches!(self.peek(), Some('0'..='9')) {
                is_float = true;
                while matches!(self.peek(), Some('0'..='9')) {
                    self.pos += 1;
                }
            } else {
                self.pos = save;
            }
        }
        let text: String = self.chars[start..self.pos].iter().collect();
        if is_float {
            text.parse::<f64>().map(PyValue::Float).map_err(|_| ())
        } else {
            Ok(crate::types::int_from_decimal_digits(&text))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_python_extensions() {
        assert!(matches!(loads("NaN"), Ok(PyValue::Float(f)) if f.is_nan()));
        assert_eq!(
            loads("[1, 2.5, null]"),
            Ok(PyValue::List(vec![
                PyValue::Int(1),
                PyValue::Float(2.5),
                PyValue::None
            ]))
        );
        assert_eq!(
            loads(r#"{"a": 1, "a": 2}"#),
            Ok(PyValue::Dict(vec![(PyValue::str("a"), PyValue::Int(2))]))
        );
        assert!(loads("{bad}").is_err());
        assert!(loads("01").is_err());
    }
}
