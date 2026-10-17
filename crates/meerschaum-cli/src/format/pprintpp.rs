//! `pprintpp.pprint` (0.4.0), which `pprint()` uses when ANSI is disabled.
//!
//! The one-line attempt and its write budget (`write_constrain`) follow the Python state
//! objects exactly: `_format` writes through the state it was given and recurses with a clone,
//! so a nested budget is a snapshot of its parent's.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

use super::rich::pretty::PyObj;

#[derive(Debug)]
struct Constrained;

#[derive(Debug, Clone, Default)]
struct Shared {
    cur_line_length: usize,
}

#[derive(Debug, Clone)]
struct State {
    indent: usize,
    level: usize,
    max_width: usize,
    write_constrain: Option<i64>,
    stream: Rc<RefCell<String>>,
    shared: Rc<RefCell<Shared>>,
}

impl State {
    fn write(&mut self, data: &str) -> Result<(), Constrained> {
        let len = data.chars().count();
        if let Some(c) = &mut self.write_constrain {
            *c -= len as i64;
            if *c < 0 {
                return Err(Constrained);
            }
        }
        self.stream.borrow_mut().push_str(data);
        let mut s = self.shared.borrow_mut();
        match data.rfind('\n') {
            None => s.cur_line_length += len,
            Some(i) => s.cur_line_length = data[i + 1..].chars().count(),
        }
        Ok(())
    }

    fn indent_string(&self) -> String {
        " ".repeat(self.level * self.indent)
    }

    /// `state.clone(clone_shared=True)` with a fresh stream.
    fn clone_shared(&self) -> State {
        State {
            shared: Rc::new(RefCell::new(self.shared.borrow().clone())),
            stream: Rc::new(RefCell::new(String::new())),
            ..self.clone()
        }
    }
}

/// `pprintpp.pformat(obj, indent=4, width=80)` plus the trailing newline `pprint` writes.
pub fn pprint(obj: &PyObj, indent: usize, width: usize) -> String {
    let mut state = State {
        indent,
        level: 0,
        max_width: width,
        write_constrain: None,
        stream: Rc::new(RefCell::new(String::new())),
        shared: Rc::new(RefCell::new(Shared::default())),
    };
    let _ = format(obj, &mut state);
    let _ = state.write("\n");
    state.stream.borrow().clone()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Typeish {
    Dict,
    List,
    Tuple,
    Set,
}

fn container(obj: &PyObj) -> Option<(Typeish, &'static str, &'static str, &'static str, usize)> {
    match obj {
        PyObj::Dict(i) => Some((Typeish::Dict, "{", "}", "{}", i.len())),
        PyObj::List(i) => Some((Typeish::List, "[", "]", "[]", i.len())),
        PyObj::Tuple(i) => Some((Typeish::Tuple, "(", ")", "()", i.len())),
        PyObj::Set(i) => Some((Typeish::Set, "set([", "])", "set()", i.len())),
        _ => None,
    }
}

fn format(obj: &PyObj, state: &mut State) -> Result<(), Constrained> {
    let mut cloned = state.clone();
    if let Some((typeish, opener, closer, empty, len)) = container(obj) {
        if len == 0 {
            return state.write(empty);
        }
        state.write(opener)?;
        format_nested(obj, &mut cloned, typeish)?;
        return state.write(closer);
    }
    if let PyObj::Str(s) = obj {
        let (quote, escaped) = if s.contains('\'') && !s.contains('"') {
            ('"', "\\\"")
        } else {
            ('\'', "\\'")
        };
        state.write(&quote.to_string())?;
        for c in s.chars() {
            if (c as u32) > 0x7f && printable_category(c) {
                state.write(&c.to_string())?;
                continue;
            }
            if c == quote {
                state.write(escaped)?;
            } else {
                state.write(&chr_to_ascii(c))?;
            }
        }
        return state.write(&quote.to_string());
    }
    let orepr = obj
        .repr()
        .replace('\n', &format!("\n{}", cloned.indent_string()));
    state.write(&orepr)
}

fn format_nested(obj: &PyObj, state: &mut State, typeish: Typeish) -> Result<(), Constrained> {
    state.level += 1;
    let result = (|| -> Result<bool, Constrained> {
        let mut oneline = state.clone_shared();
        let cur = oneline.shared.borrow().cur_line_length as i64;
        oneline.write_constrain = Some(state.max_width as i64 - cur - 3);
        if write_nested_real(obj, &mut oneline, typeish, true).is_ok() {
            let value = oneline.stream.borrow().clone();
            if !value.contains('\n') {
                state.write(&value)?;
                return Ok(true);
            }
        }
        let indent = state.indent_string();
        state.write(&format!("\n{indent}"))?;
        write_nested_real(obj, state, typeish, false)?;
        Ok(false)
    })();
    state.level -= 1;
    match result {
        Ok(true) => Ok(()),
        Ok(false) => {
            let indent = state.indent_string();
            state.write(&indent)
        }
        Err(e) => Err(e),
    }
}

fn write_nested_real(
    obj: &PyObj,
    state: &mut State,
    typeish: Typeish,
    oneline: bool,
) -> Result<(), Constrained> {
    let indent_str = state.indent_string();
    let joiner = if oneline {
        ", ".to_string()
    } else {
        format!(",\n{indent_str}")
    };
    let mut first = true;
    match obj {
        PyObj::Dict(items) => {
            let mut items: Vec<&(PyObj, PyObj)> = items.iter().collect();
            items.sort_by(|a, b| py_cmp(&a.0, &b.0).then_with(|| py_cmp(&a.1, &b.1)));
            for (k, v) in items {
                if first {
                    first = false;
                } else {
                    state.write(&joiner)?;
                }
                format(k, state)?;
                state.write(": ")?;
                format(v, state)?;
            }
        }
        PyObj::List(items) | PyObj::Tuple(items) | PyObj::Set(items) => {
            let mut items: Vec<&PyObj> = items.iter().collect();
            if typeish == Typeish::Set {
                items.sort_by(|a, b| py_cmp(a, b));
            }
            for o in &items {
                if first {
                    first = false;
                } else {
                    state.write(&joiner)?;
                }
                format(o, state)?;
            }
            if oneline && typeish == Typeish::Tuple && items.len() == 1 {
                state.write(", ")?;
                return Ok(());
            }
        }
        _ => {}
    }
    if !oneline {
        state.write(",\n")?;
    }
    Ok(())
}

/// Python ordering for the values `sorted()` meets here (same-type comparisons).
fn py_cmp(a: &PyObj, b: &PyObj) -> Ordering {
    fn num(o: &PyObj) -> Option<f64> {
        match o {
            PyObj::Int(i) => Some(*i as f64),
            PyObj::Float(f) => Some(*f),
            PyObj::Bool(b) => Some(f64::from(u8::from(*b))),
            _ => None,
        }
    }
    match (a, b) {
        (PyObj::Str(x), PyObj::Str(y)) => x.cmp(y),
        _ => match (num(a), num(b)) {
            (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
            // PARITY: Python raises TypeError comparing unrelated types; order by repr.
            _ => a.repr().cmp(&b.repr()),
        },
    }
}

/// `ascii(c)[1:-1]`.
fn chr_to_ascii(c: char) -> String {
    let code = c as u32;
    match c {
        '\\' => "\\\\".into(),
        '\n' => "\\n".into(),
        '\r' => "\\r".into(),
        '\t' => "\\t".into(),
        _ if (0x20..0x7f).contains(&code) => c.to_string(),
        _ if code < 0x100 => format!("\\x{code:02x}"),
        _ if code < 0x10000 => format!("\\u{code:04x}"),
        _ => format!("\\U{code:08x}"),
    }
}

/// `unicode_printable_categories.get(unicodedata.category(c))`: letters (except modifier
/// letters), numbers, punctuation and symbols print as themselves; marks, separators and
/// control/format characters are escaped.
fn printable_category(c: char) -> bool {
    if c.is_control() || c.is_whitespace() {
        return false;
    }
    let code = c as u32;
    // Combining marks (the common blocks), format characters and private use.
    let excluded = matches!(
        code,
        0x300..=0x36F
            | 0x483..=0x489
            | 0x591..=0x5BD
            | 0x610..=0x61A
            | 0x64B..=0x65F
            | 0x1AB0..=0x1AFF
            | 0x1DC0..=0x1DFF
            | 0x20D0..=0x20FF
            | 0xFE00..=0xFE0F
            | 0xFE20..=0xFE2F
            | 0xAD
            | 0x200B..=0x200F
            | 0x202A..=0x202E
            | 0x2060..=0x206F
            | 0xFEFF
            | 0xE000..=0xF8FF
            | 0x2B0..=0x2FF
    );
    !excluded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_and_long() {
        let d = PyObj::Dict(vec![
            (PyObj::Str("b".into()), PyObj::Int(2)),
            (PyObj::Str("a".into()), PyObj::Int(1)),
        ]);
        assert_eq!(pprint(&d, 4, 80), "{'a': 1, 'b': 2}\n");
        let long = PyObj::List((0..30).map(|i| PyObj::Str(format!("item{i}"))).collect());
        let out = pprint(&long, 4, 80);
        assert!(out.starts_with("[\n    'item0',\n"), "{out}");
        assert!(out.ends_with("    'item29',\n]\n"), "{out}");
    }
}
