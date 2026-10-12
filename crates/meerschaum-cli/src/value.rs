//! A model of the Python values an `argparse` namespace can hold.
//!
//! `parse_arguments` in v4 returns a `dict` whose values are Python objects: strings, lists,
//! integers (unbounded), floats, `datetime`, `timedelta`, `pathlib.Path`, dictionaries produced by
//! `json.loads` / `ast.literal_eval`, and exception objects. [`PyValue`] carries exactly that set so
//! the Rust parser can be compared value-for-value against the Python one, and so the arguments
//! map can be rendered back into sysargs (`str()`, `repr()`, `json.dumps`) identically.

use std::fmt::Write as _;

use chrono::{DateTime, Datelike, Timelike, Utc};
use indexmap::IndexMap;
use serde_json::{Map, Value};

/// The parsed arguments map (`args_dict` in Python). Key order follows Python's insertion order.
pub type ArgsMap = IndexMap<String, PyValue>;

/// A Python exception, reduced to its class name and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PyException {
    /// The exception class name (e.g. `ValueError`, `InvalidArgumentException`).
    pub class: String,
    /// `str(exception)`, without the ANSI/icon decoration of `meerschaum.utils.warnings.error`.
    pub message: String,
}

impl PyException {
    /// Build an exception from a class name and message.
    pub fn new(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            class: class.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for PyException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PyException {}

/// "Python raised" for helpers whose exception details do not matter to their callers
/// (`json.loads`, `ast.literal_eval`, `dateutil.parser.parse` as used by the parser).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotParsed;

impl std::fmt::Display for NotParsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("not parsed")
    }
}

impl std::error::Error for NotParsed {}

/// A normalized `datetime.timedelta` (`days`, `0 <= seconds < 86400`, `0 <= microseconds < 1e6`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PyTimeDelta {
    /// Whole days (may be negative).
    pub days: i64,
    /// Seconds within the day.
    pub seconds: i64,
    /// Microseconds within the second.
    pub microseconds: i64,
}

/// `datetime.timedelta.max.days`.
pub const TIMEDELTA_MAX_DAYS: i128 = 999_999_999;

impl PyTimeDelta {
    /// Build a normalized timedelta from a total number of microseconds, or an `OverflowError`.
    pub fn from_microseconds(total: i128) -> Result<Self, PyException> {
        let per_day: i128 = 86_400_000_000;
        let days = total.div_euclid(per_day);
        let rem = total.rem_euclid(per_day);
        // CPython converts the normalized days to a C int before the range check.
        if days < i32::MIN as i128 || days > i32::MAX as i128 {
            return Err(PyException::new(
                "OverflowError",
                "Python int too large to convert to C int",
            ));
        }
        if days.abs() > TIMEDELTA_MAX_DAYS {
            return Err(PyException::new(
                "OverflowError",
                format!("days={days}; must have magnitude <= {TIMEDELTA_MAX_DAYS}"),
            ));
        }
        Ok(Self {
            days: days as i64,
            seconds: (rem / 1_000_000) as i64,
            microseconds: (rem % 1_000_000) as i64,
        })
    }

    /// Total microseconds.
    pub fn total_microseconds(&self) -> i128 {
        (self.days as i128 * 86_400 + self.seconds as i128) * 1_000_000 + self.microseconds as i128
    }
}

/// A Python value.
#[derive(Debug, Clone, PartialEq)]
pub enum PyValue {
    /// `None`
    None,
    /// `bool`
    Bool(bool),
    /// `int` within the `i128` range.
    Int(i128),
    /// `int` outside the `i128` range, as normalized decimal digits (optional leading `-`).
    BigInt(String),
    /// `float`
    Float(f64),
    /// `complex` (real, imaginary).
    Complex(f64, f64),
    /// `str`
    Str(String),
    /// `bytes`
    Bytes(Vec<u8>),
    /// `list`
    List(Vec<PyValue>),
    /// `tuple`
    Tuple(Vec<PyValue>),
    /// `set` (insertion order; Python's iteration order is hash-dependent).
    Set(Vec<PyValue>),
    /// `dict` (insertion order).
    Dict(Vec<(PyValue, PyValue)>),
    /// A timezone-aware UTC `datetime` (every datetime produced by the parser is UTC).
    DateTime(DateTime<Utc>),
    /// `datetime.timedelta`
    TimeDelta(PyTimeDelta),
    /// `pathlib.Path` (normalized string form).
    Path(String),
    /// An exception object stored in the arguments (the `_argparse_exception` key).
    Exception(PyException),
    /// `Ellipsis`
    Ellipsis,
}

impl PyValue {
    /// A `str` value.
    pub fn str(s: impl Into<String>) -> Self {
        PyValue::Str(s.into())
    }

    /// A `list` of `str` values.
    pub fn str_list<S: AsRef<str>>(items: &[S]) -> Self {
        PyValue::List(items.iter().map(|s| PyValue::str(s.as_ref())).collect())
    }

    /// Return the string if this is a `str`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PyValue::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Return the items if this is a `list`.
    pub fn as_list(&self) -> Option<&Vec<PyValue>> {
        match self {
            PyValue::List(v) => Some(v),
            _ => None,
        }
    }

    /// Python truthiness (`bool(value)`).
    pub fn is_truthy(&self) -> bool {
        match self {
            PyValue::None => false,
            PyValue::Bool(b) => *b,
            PyValue::Int(i) => *i != 0,
            PyValue::BigInt(_) => true,
            PyValue::Float(f) => *f != 0.0,
            PyValue::Complex(r, i) => *r != 0.0 || *i != 0.0,
            PyValue::Str(s) => !s.is_empty(),
            PyValue::Bytes(b) => !b.is_empty(),
            PyValue::List(v) | PyValue::Tuple(v) | PyValue::Set(v) => !v.is_empty(),
            PyValue::Dict(v) => !v.is_empty(),
            PyValue::TimeDelta(td) => td.total_microseconds() != 0,
            PyValue::DateTime(_) | PyValue::Path(_) | PyValue::Exception(_) | PyValue::Ellipsis => {
                true
            }
        }
    }

    /// Whether the value is an `int` (excluding `bool`, as `type(x) is int`).
    pub fn is_int(&self) -> bool {
        matches!(self, PyValue::Int(_) | PyValue::BigInt(_))
    }

    /// Python's `type(x).__name__`.
    pub fn type_name(&self) -> &'static str {
        match self {
            PyValue::None => "NoneType",
            PyValue::Bool(_) => "bool",
            PyValue::Int(_) | PyValue::BigInt(_) => "int",
            PyValue::Float(_) => "float",
            PyValue::Complex(..) => "complex",
            PyValue::Str(_) => "str",
            PyValue::Bytes(_) => "bytes",
            PyValue::List(_) => "list",
            PyValue::Tuple(_) => "tuple",
            PyValue::Set(_) => "set",
            PyValue::Dict(_) => "dict",
            PyValue::DateTime(_) => "datetime",
            PyValue::TimeDelta(_) => "timedelta",
            PyValue::Path(_) => "PosixPath",
            PyValue::Exception(_) => "Exception",
            PyValue::Ellipsis => "ellipsis",
        }
    }

    /// Whether `hash(value)` succeeds.
    pub fn is_hashable(&self) -> bool {
        match self {
            PyValue::List(_) | PyValue::Dict(_) | PyValue::Set(_) => false,
            PyValue::Tuple(items) => items.iter().all(PyValue::is_hashable),
            _ => true,
        }
    }

    /// Python `==` for the value kinds that can be dictionary keys or set members.
    pub fn py_eq(&self, other: &PyValue) -> bool {
        if let (Some(a), Some(b)) = (self.as_number(), other.as_number()) {
            return a == b;
        }
        match (self, other) {
            (PyValue::Tuple(a), PyValue::Tuple(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.py_eq(y))
            }
            (PyValue::BigInt(a), PyValue::BigInt(b)) => a == b,
            (PyValue::Complex(ar, ai), PyValue::Complex(br, bi)) => ar == br && ai == bi,
            (PyValue::Complex(r, i), other) | (other, PyValue::Complex(r, i)) => {
                *i == 0.0 && other.as_number() == Some(*r)
            }
            _ => self == other,
        }
    }

    fn as_number(&self) -> Option<f64> {
        match self {
            PyValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            PyValue::Int(i) => Some(*i as f64),
            PyValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Encode as tagged JSON, mirroring `enc()` in `tests/fixtures/gen_args.py`.
    pub fn to_fixture_json(&self) -> Value {
        fn tagged(kind: &str, fields: Vec<(&str, Value)>) -> Value {
            let mut map = Map::new();
            map.insert("$type".into(), Value::String(kind.into()));
            for (k, v) in fields {
                map.insert(k.into(), v);
            }
            Value::Object(map)
        }
        fn float_json(f: f64) -> Value {
            match serde_json::Number::from_f64(f) {
                Some(n) => Value::Number(n),
                None => tagged("float", vec![("value", Value::String(float_repr(f)))]),
            }
        }
        match self {
            PyValue::None => Value::Null,
            PyValue::Bool(b) => Value::Bool(*b),
            PyValue::Int(i) => {
                if *i >= i64::MIN as i128 && *i <= i64::MAX as i128 {
                    Value::Number((*i as i64).into())
                } else {
                    tagged("int", vec![("value", Value::String(i.to_string()))])
                }
            }
            PyValue::BigInt(s) => tagged("int", vec![("value", Value::String(s.clone()))]),
            PyValue::Float(f) => float_json(*f),
            PyValue::Complex(r, i) => tagged(
                "complex",
                vec![("real", float_json(*r)), ("imag", float_json(*i))],
            ),
            PyValue::Str(s) => Value::String(s.clone()),
            PyValue::Bytes(b) => {
                let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
                tagged("bytes", vec![("hex", Value::String(hex))])
            }
            PyValue::List(items) => {
                Value::Array(items.iter().map(PyValue::to_fixture_json).collect())
            }
            PyValue::Tuple(items) => tagged(
                "tuple",
                vec![(
                    "items",
                    Value::Array(items.iter().map(PyValue::to_fixture_json).collect()),
                )],
            ),
            PyValue::Set(items) => {
                let mut encoded: Vec<Value> = items.iter().map(PyValue::to_fixture_json).collect();
                encoded.sort_by_key(|v| serde_json::to_string(v).unwrap_or_default());
                tagged("set", vec![("items", Value::Array(encoded))])
            }
            PyValue::Dict(items) => {
                if items.iter().all(|(k, _)| matches!(k, PyValue::Str(_))) {
                    let mut map = Map::new();
                    for (k, v) in items {
                        if let PyValue::Str(k) = k {
                            map.insert(k.clone(), v.to_fixture_json());
                        }
                    }
                    Value::Object(map)
                } else {
                    tagged(
                        "dict",
                        vec![(
                            "items",
                            Value::Array(
                                items
                                    .iter()
                                    .map(|(k, v)| {
                                        Value::Array(vec![k.to_fixture_json(), v.to_fixture_json()])
                                    })
                                    .collect(),
                            ),
                        )],
                    )
                }
            }
            PyValue::DateTime(dt) => tagged(
                "datetime",
                vec![("value", Value::String(datetime_isoformat(dt)))],
            ),
            PyValue::TimeDelta(td) => tagged(
                "timedelta",
                vec![
                    ("days", Value::Number(td.days.into())),
                    ("seconds", Value::Number(td.seconds.into())),
                    ("microseconds", Value::Number(td.microseconds.into())),
                ],
            ),
            PyValue::Path(p) => tagged("path", vec![("value", Value::String(p.clone()))]),
            PyValue::Exception(e) => tagged(
                "exception",
                vec![
                    ("class", Value::String(e.class.clone())),
                    ("message", Value::String(e.message.clone())),
                ],
            ),
            PyValue::Ellipsis => tagged("ellipsis", vec![]),
        }
    }

    /// Python `repr(value)`.
    pub fn py_repr(&self) -> String {
        match self {
            PyValue::None => "None".into(),
            PyValue::Bool(b) => if *b { "True" } else { "False" }.into(),
            PyValue::Int(i) => i.to_string(),
            PyValue::BigInt(s) => s.clone(),
            PyValue::Float(f) => float_repr(*f),
            PyValue::Complex(r, i) => complex_repr(*r, *i),
            PyValue::Str(s) => repr_str(s),
            PyValue::Bytes(b) => meerschaum_core::pyfmt::bytes_repr(b),
            PyValue::List(items) => {
                format!(
                    "[{}]",
                    items
                        .iter()
                        .map(PyValue::py_repr)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            PyValue::Tuple(items) => {
                if items.len() == 1 {
                    format!("({},)", items[0].py_repr())
                } else {
                    format!(
                        "({})",
                        items
                            .iter()
                            .map(PyValue::py_repr)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            }
            PyValue::Set(items) => {
                if items.is_empty() {
                    "set()".into()
                } else {
                    format!(
                        "{{{}}}",
                        items
                            .iter()
                            .map(PyValue::py_repr)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            }
            PyValue::Dict(items) => format!(
                "{{{}}}",
                items
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k.py_repr(), v.py_repr()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            PyValue::DateTime(dt) => {
                let mut s = format!(
                    "datetime.datetime({}, {}, {}, {}, {}",
                    dt.year(),
                    dt.month(),
                    dt.day(),
                    dt.hour(),
                    dt.minute()
                );
                let micro = dt.nanosecond() / 1000;
                if dt.second() != 0 || micro != 0 {
                    let _ = write!(s, ", {}", dt.second());
                }
                if micro != 0 {
                    let _ = write!(s, ", {micro}");
                }
                s.push_str(", tzinfo=datetime.timezone.utc)");
                s
            }
            PyValue::TimeDelta(td) => {
                let mut parts = Vec::new();
                if td.days != 0 {
                    parts.push(format!("days={}", td.days));
                }
                if td.seconds != 0 {
                    parts.push(format!("seconds={}", td.seconds));
                }
                if td.microseconds != 0 {
                    parts.push(format!("microseconds={}", td.microseconds));
                }
                if parts.is_empty() {
                    parts.push("0".into());
                }
                format!("datetime.timedelta({})", parts.join(", "))
            }
            PyValue::Path(p) => format!("PosixPath({})", repr_str(p)),
            PyValue::Exception(e) => format!("{}({})", e.class, repr_str(&e.message)),
            PyValue::Ellipsis => "Ellipsis".into(),
        }
    }

    /// Python `str(value)`.
    pub fn py_str(&self) -> String {
        match self {
            PyValue::Str(s) => s.clone(),
            PyValue::Path(p) => p.clone(),
            PyValue::Exception(e) => e.message.clone(),
            PyValue::DateTime(dt) => {
                let micro = dt.nanosecond() / 1000;
                let mut s = dt.format("%Y-%m-%d %H:%M:%S").to_string();
                if micro != 0 {
                    let _ = write!(s, ".{micro:06}");
                }
                s.push_str("+00:00");
                s
            }
            PyValue::TimeDelta(td) => {
                let h = td.seconds / 3600;
                let m = (td.seconds % 3600) / 60;
                let sec = td.seconds % 60;
                let mut s = String::new();
                if td.days != 0 {
                    let plural = if td.days.abs() != 1 { "s" } else { "" };
                    let _ = write!(s, "{} day{plural}, ", td.days);
                }
                let _ = write!(s, "{h}:{m:02}:{sec:02}");
                if td.microseconds != 0 {
                    let _ = write!(s, ".{:06}", td.microseconds);
                }
                s
            }
            _ => self.py_repr(),
        }
    }

    /// `json.dumps(value, separators=(',', ':'))` (with the default `ensure_ascii=True`).
    pub fn json_dumps_compact(&self) -> Result<String, PyException> {
        let mut out = String::new();
        json_dump_into(self, &mut out)?;
        Ok(out)
    }
}

fn json_dump_into(value: &PyValue, out: &mut String) -> Result<(), PyException> {
    match value {
        PyValue::None => out.push_str("null"),
        PyValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        PyValue::Int(i) => out.push_str(&i.to_string()),
        PyValue::BigInt(s) => out.push_str(s),
        PyValue::Float(f) => out.push_str(&json_float(*f)),
        PyValue::Str(s) => out.push_str(&json_quote(s)),
        PyValue::List(items) | PyValue::Tuple(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_dump_into(item, out)?;
            }
            out.push(']');
        }
        PyValue::Dict(items) => {
            out.push('{');
            for (i, (k, v)) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let key = match k {
                    PyValue::Str(s) => s.clone(),
                    PyValue::Int(_) | PyValue::BigInt(_) => k.py_repr(),
                    PyValue::Float(f) => json_float(*f),
                    PyValue::Bool(b) => if *b { "true" } else { "false" }.into(),
                    PyValue::None => "null".into(),
                    other => {
                        return Err(PyException::new(
                            "TypeError",
                            format!(
                                "keys must be str, int, float, bool or None, not {}",
                                other.type_name()
                            ),
                        ));
                    }
                };
                out.push_str(&json_quote(&key));
                out.push(':');
                json_dump_into(v, out)?;
            }
            out.push('}');
        }
        other => {
            return Err(PyException::new(
                "TypeError",
                format!(
                    "Object of type {} is not JSON serializable",
                    other.type_name()
                ),
            ));
        }
    }
    Ok(())
}

fn json_float(f: f64) -> String {
    if f.is_nan() {
        "NaN".into()
    } else if f.is_infinite() {
        if f > 0.0 { "Infinity" } else { "-Infinity" }.into()
    } else {
        float_repr(f)
    }
}

fn complex_repr(r: f64, i: f64) -> String {
    fn part(f: f64) -> String {
        let s = float_repr(f);
        s.strip_suffix(".0").map(str::to_string).unwrap_or(s)
    }
    if r == 0.0 && !r.is_sign_negative() {
        return format!("{}j", part(i));
    }
    let sign = if i.is_sign_negative() { "-" } else { "+" };
    format!("({}{sign}{}j)", part(r), part(i.abs()))
}

pub use meerschaum_core::pyfmt::{float_repr, str_repr as repr_str};

/// `json.encoder.py_encode_basestring_ascii` (`json.dumps(s)` with `ensure_ascii=True`).
pub fn json_quote(s: &str) -> String {
    meerschaum_core::pyfmt::json_dumps_str(s, true)
}

/// `datetime.isoformat()` for a UTC-aware datetime.
pub fn datetime_isoformat(dt: &DateTime<Utc>) -> String {
    meerschaum_core::pyfmt::PyDateTime::utc(*dt).isoformat()
}

/// Encode an arguments map as tagged JSON (see [`PyValue::to_fixture_json`]).
pub fn args_to_fixture_json(args: &ArgsMap) -> Value {
    let mut map = Map::new();
    for (k, v) in args {
        map.insert(k.clone(), v.to_fixture_json());
    }
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_repr_matches_python() {
        let cases = [
            (300.0, "300.0"),
            (1.5, "1.5"),
            (0.1, "0.1"),
            (1e16, "1e+16"),
            (1e15, "1000000000000000.0"),
            (1.5e-5, "1.5e-05"),
            (0.0001, "0.0001"),
            (-2.0, "-2.0"),
            (123456.789, "123456.789"),
            (1e-3, "0.001"),
        ];
        for (f, expected) in cases {
            assert_eq!(float_repr(f), expected, "{f}");
        }
    }

    #[test]
    fn repr_str_quotes_like_python() {
        assert_eq!(repr_str("abc"), "'abc'");
        assert_eq!(repr_str("it's"), "\"it's\"");
        assert_eq!(repr_str("a'b\"c"), "'a\\'b\"c'");
        assert_eq!(repr_str("tab\there"), "'tab\\there'");
        assert_eq!(repr_str("é"), "'é'");
    }

    #[test]
    fn json_quote_escapes_non_ascii() {
        assert_eq!(json_quote("été"), "\"\\u00e9t\\u00e9\"");
        assert_eq!(json_quote("😀"), "\"\\ud83d\\ude00\"");
    }
}
