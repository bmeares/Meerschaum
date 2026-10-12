//! Python string semantics the query builders depend on, reproduced exactly:
//! `str.isspace`/`strip`, `textwrap.dedent`/`indent` (CPython 3.14), `str.splitlines`,
//! `str.format` with named fields, `repr(float)`, `json.dumps(str)`,
//! `meerschaum.utils.misc.truncate_string_sections`, and `datetime.__str__`/`isoformat`.

use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, Offset, Timelike, Utc};

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

/// CPython's `repr(float)` (shortest round-trip digits, `'r'` formatting rules).
pub fn py_float_repr(f: f64) -> String {
    if f.is_nan() {
        return "nan".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    if f == 0.0 {
        return if f.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    // Rust's `{:e}` yields the shortest round-trip digits, e.g. `1.1574074074074073e-5`.
    let sci = format!("{:e}", f);
    let (mantissa, exp) = sci.split_once('e').expect("scientific notation");
    let exp: i32 = exp.parse().expect("exponent");
    let negative = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(|c| c.is_ascii_digit()).collect();
    let decpt = exp + 1;
    let sign = if negative { "-" } else { "" };
    if decpt <= -4 || decpt > 16 {
        let mut m = digits[..1].to_string();
        if digits.len() > 1 {
            m.push('.');
            m.push_str(&digits[1..]);
        }
        let e = decpt - 1;
        let e_sign = if e < 0 { '-' } else { '+' };
        format!("{sign}{m}e{e_sign}{:02}", e.abs())
    } else if decpt <= 0 {
        format!("{sign}0.{}{}", "0".repeat((-decpt) as usize), digits)
    } else {
        let decpt = decpt as usize;
        if digits.len() <= decpt {
            format!("{sign}{}{}.0", digits, "0".repeat(decpt - digits.len()))
        } else {
            format!("{sign}{}.{}", &digits[..decpt], &digits[decpt..])
        }
    }
}

/// `json.dumps(s)` for a string with `ensure_ascii=True`.
pub fn py_json_dumps_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            ' '..='~' => out.push(c),
            c => {
                let mut buf = [0u16; 2];
                for unit in c.encode_utf16(&mut buf) {
                    out.push_str(&format!("\\u{:04x}", unit));
                }
            }
        }
    }
    out.push('"');
    out
}

/// `meerschaum.utils.misc.truncate_string_sections` (`utils/misc.py:1298`).
///
/// Lengths are counted in code points, like Python.
pub fn truncate_string_sections(item: &str, delimiter: &str, max_len: usize) -> String {
    // PARITY: `if len(item) < max_len: return item` (utils/misc.py:1323), so a name of exactly
    // `max_len` characters is still shortened.
    if item.chars().count() < max_len {
        return item.to_string();
    }
    let sections: Vec<(usize, Vec<char>)> = item
        .split(delimiter)
        .map(|s| s.chars().collect())
        .enumerate()
        .collect();
    let mut sorted = sections.clone();
    // Python's `sorted` is stable; sort by descending length.
    sorted.sort_by_key(|(_, s)| std::cmp::Reverse(s.len()));
    let available_chars = max_len as i64 - sections.len() as i64;
    let mut current = sorted;
    let mut current_len: i64 = current.iter().map(|(_, s)| s.len() as i64).sum();
    while current_len > available_chars {
        for (_, s) in current.iter_mut() {
            if s.len() > 1 {
                s.pop();
            }
        }
        let new_len: i64 = current.iter().map(|(_, s)| s.len() as i64).sum();
        if new_len == current_len {
            // PARITY: Python raises `Exception(f"String could not be truncated: '{item}'")`;
            // return the best effort so callers never panic.
            break;
        }
        current_len = new_len;
    }
    current.sort_by_key(|(i, _)| *i);
    current
        .into_iter()
        .map(|(_, s)| s.into_iter().collect::<String>())
        .collect::<Vec<_>>()
        .join(delimiter)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_repr() {
        assert_eq!(py_float_repr(1.0), "1.0");
        assert_eq!(py_float_repr(1.0 / 60.0), "0.016666666666666666");
        assert_eq!(py_float_repr(1.0 / 86400.0), "1.1574074074074073e-05");
        assert_eq!(py_float_repr(1e16), "1e+16");
        assert_eq!(py_float_repr(1e15), "1000000000000000.0");
        assert_eq!(py_float_repr(0.0001), "0.0001");
        assert_eq!(py_float_repr(-2.5), "-2.5");
        assert_eq!(py_float_repr(123456789.125), "123456789.125");
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
    fn truncate() {
        assert_eq!(truncate_string_sections("abc_def_ghi", "_", 10), "ab_de_gh");
        assert_eq!(truncate_string_sections("abc", "_", 10), "abc");
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
}
