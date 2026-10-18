//! The lossless YAML node: what PyYAML's constructors build, kept as a typed tree instead of
//! being projected into the JSON-like [`crate::value::Value`].
//!
//! A [`Node`] keeps ordered mappings with typed keys, arbitrary-size integers, `date` /
//! `datetime` (naive and offset, microseconds), `bytes`, tuples and sets, so that a Python
//! boundary can construct the exact objects PyYAML would. Containers and dates are shared
//! through [`NodeRef`] (`Rc`): an alias in the source yields the *same* node, and the emitter
//! uses node identity to write anchors, as PyYAML does with `id()`.

use std::collections::HashMap;
use std::rc::Rc;

use num_bigint::BigInt;
use num_traits::{FromPrimitive, ToPrimitive};
use regex::Regex;
use std::sync::LazyLock;

use crate::pyfmt::float_repr;

/// A shared node.
pub type NodeRef = Rc<Node>;

/// A calendar date (`datetime.date`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Date {
    /// The year (1..=9999).
    pub year: i32,
    /// The month (1..=12).
    pub month: u32,
    /// The day of the month.
    pub day: u32,
}

/// A `datetime.datetime`, naive or with a UTC offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateTime {
    /// The date part.
    pub date: Date,
    /// The hour (0..=23).
    pub hour: u32,
    /// The minute (0..=59).
    pub minute: u32,
    /// The second (0..=59).
    pub second: u32,
    /// The microsecond (0..=999999).
    pub microsecond: u32,
    /// The UTC offset in microseconds for an aware datetime (`None` = naive).
    pub utc_offset_us: Option<i64>,
}

/// A lossless YAML node.
#[derive(Debug, Clone)]
pub enum Node {
    /// `None`.
    Null,
    /// `bool`.
    Bool(bool),
    /// `int` of any size.
    Int(BigInt),
    /// `float`, including `inf` and `nan`.
    Float(f64),
    /// `str`.
    Str(String),
    /// `bytes` (`!!binary`).
    Bytes(Vec<u8>),
    /// `datetime.date`.
    Date(Date),
    /// `datetime.datetime`.
    DateTime(DateTime),
    /// `list`.
    Seq(Vec<NodeRef>),
    /// `tuple` (`!!python/tuple`).
    Tuple(Vec<NodeRef>),
    /// `dict`: the entries in insertion order, keys already unique under Python equality.
    Map(Vec<(NodeRef, NodeRef)>),
    /// `set` (`!!set`).
    Set(Vec<NodeRef>),
}

impl Node {
    /// A shared integer node.
    pub fn int(i: i64) -> NodeRef {
        Rc::new(Node::Int(BigInt::from(i)))
    }

    /// A shared string node.
    pub fn string(s: impl Into<String>) -> NodeRef {
        Rc::new(Node::Str(s.into()))
    }

    /// The node kind as PyYAML's `node.id` would name the *YAML* node it came from.
    pub fn yaml_kind(&self) -> &'static str {
        match self {
            Node::Seq(_) | Node::Tuple(_) => "sequence",
            Node::Map(_) | Node::Set(_) => "mapping",
            _ => "scalar",
        }
    }
}

impl Date {
    /// `date.isoformat()`.
    pub fn isoformat(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    /// Whether the fields form a valid `datetime.date`; the error is CPython 3.14's message.
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=9999).contains(&self.year) {
            return Err(format!("year must be in 1..9999, not {}", self.year));
        }
        if !(1..=12).contains(&self.month) {
            return Err(format!("month must be in 1..12, not {}", self.month));
        }
        let dim = days_in_month(self.year, self.month);
        if self.day < 1 || self.day > dim {
            return Err(format!(
                "day {} must be in range 1..{} for month {} in year {}",
                self.day, dim, self.month, self.year
            ));
        }
        Ok(())
    }
}

/// `repr(datetime.timedelta(microseconds=us))`.
fn timedelta_repr(us: i64) -> String {
    let days = us.div_euclid(86_400_000_000);
    let rest = us.rem_euclid(86_400_000_000);
    let (seconds, micro) = (rest / 1_000_000, rest % 1_000_000);
    let mut parts: Vec<String> = Vec::new();
    if days != 0 {
        parts.push(format!("days={days}"));
    }
    if seconds != 0 {
        parts.push(format!("seconds={seconds}"));
    }
    if micro != 0 {
        parts.push(format!("microseconds={micro}"));
    }
    if parts.is_empty() {
        parts.push("0".to_string());
    }
    format!("datetime.timedelta({})", parts.join(", "))
}

fn is_leap(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
    }
}

/// Days since 1970-01-01 (proleptic Gregorian).
fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = i64::from(if m <= 2 { y - 1 } else { y });
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

impl DateTime {
    /// `datetime.isoformat(sep)`.
    pub fn isoformat(&self, sep: char) -> String {
        let mut out = format!(
            "{}{sep}{:02}:{:02}:{:02}",
            self.date.isoformat(),
            self.hour,
            self.minute,
            self.second
        );
        if self.microsecond != 0 {
            out.push_str(&format!(".{:06}", self.microsecond));
        }
        if let Some(off) = self.utc_offset_us {
            let sign = if off < 0 { '-' } else { '+' };
            let a = off.unsigned_abs();
            let micro = a % 1_000_000;
            let total_s = a / 1_000_000;
            let (h, m, s) = (total_s / 3600, (total_s / 60) % 60, total_s % 60);
            out.push_str(&format!("{sign}{h:02}:{m:02}"));
            if s != 0 || micro != 0 {
                out.push_str(&format!(":{s:02}"));
                if micro != 0 {
                    out.push_str(&format!(".{micro:06}"));
                }
            }
        }
        out
    }

    /// Whether the fields form a valid `datetime.datetime`; the error is CPython's message.
    pub fn validate(&self) -> Result<(), String> {
        self.date.validate()?;
        if self.hour > 23 {
            return Err(format!("hour must be in 0..23, not {}", self.hour));
        }
        if self.minute > 59 {
            return Err(format!("minute must be in 0..59, not {}", self.minute));
        }
        if self.second > 59 {
            return Err(format!("second must be in 0..59, not {}", self.second));
        }
        if let Some(off) = self.utc_offset_us
            && off.abs() >= 86_400_000_000
        {
            return Err(format!(
                "offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24), not {}",
                timedelta_repr(off)
            ));
        }
        Ok(())
    }

    fn local_micros(&self) -> i128 {
        let days = i128::from(days_from_civil(
            self.date.year,
            self.date.month,
            self.date.day,
        ));
        ((days * 24 + i128::from(self.hour)) * 60 + i128::from(self.minute)) * 60_000_000
            + i128::from(self.second) * 1_000_000
            + i128::from(self.microsecond)
    }
}

// ---------------------------------------------------------------------------------------------
// Python equality / hashing of keys
// ---------------------------------------------------------------------------------------------

/// A hashable key with Python's equality (`1 == True == 1.0`, naive vs aware datetimes, ...).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PyKey {
    /// `None`.
    None,
    /// An integer-valued number (`bool`, `int`, or a whole `float`).
    Int(BigInt),
    /// A non-integral float, by bits.
    Float(u64),
    /// A `nan` (all loaded `.nan` scalars are one object).
    Nan,
    /// A string.
    Str(String),
    /// Bytes.
    Bytes(Vec<u8>),
    /// A date.
    Date(i64),
    /// A naive datetime in microseconds.
    Naive(i128),
    /// An aware datetime in UTC microseconds.
    Aware(i128),
    /// A tuple of keys.
    Tuple(Vec<PyKey>),
}

/// The key of `node`, or `None` when it is unhashable (`list`, `dict`, `set`).
pub fn py_key(node: &Node) -> Option<PyKey> {
    Some(match node {
        Node::Null => PyKey::None,
        Node::Bool(b) => PyKey::Int(BigInt::from(i32::from(*b))),
        Node::Int(i) => PyKey::Int(i.clone()),
        Node::Float(f) => {
            if f.is_nan() {
                // PARITY: `construct_yaml_float` returns one shared `nan_value` object, and a
                // dict treats the same object as the same key, so loaded `.nan` keys collapse.
                PyKey::Nan
            } else if f.is_finite() && f.fract() == 0.0 {
                PyKey::Int(BigInt::from_f64(*f).unwrap_or_default())
            } else {
                PyKey::Float(f.to_bits())
            }
        }
        Node::Str(s) => PyKey::Str(s.clone()),
        Node::Bytes(b) => PyKey::Bytes(b.clone()),
        Node::Date(d) => PyKey::Date(days_from_civil(d.year, d.month, d.day)),
        Node::DateTime(dt) => match dt.utc_offset_us {
            None => PyKey::Naive(dt.local_micros()),
            Some(off) => PyKey::Aware(dt.local_micros() - i128::from(off)),
        },
        Node::Tuple(items) => PyKey::Tuple(
            items
                .iter()
                .map(|i| py_key(i))
                .collect::<Option<Vec<_>>>()?,
        ),
        Node::Seq(_) | Node::Map(_) | Node::Set(_) => return None,
    })
}

/// Build a dict's entries the way `dict[key] = value` does: a repeated key keeps its first
/// position and key object and takes the last value. `None` when a key is unhashable.
pub fn dedup_entries(entries: Vec<(NodeRef, NodeRef)>) -> Option<Vec<(NodeRef, NodeRef)>> {
    let mut index: HashMap<PyKey, usize> = HashMap::new();
    let mut out: Vec<(NodeRef, NodeRef)> = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        let key = py_key(&k)?;
        match index.get(&key) {
            Some(&pos) => out[pos].1 = v,
            None => {
                index.insert(key, out.len());
                out.push((k, v));
            }
        }
    }
    Some(out)
}

/// Build a set's elements (unique under Python equality). `None` when one is unhashable.
pub fn dedup_set(items: Vec<NodeRef>) -> Option<Vec<NodeRef>> {
    let mut seen: HashMap<PyKey, ()> = HashMap::new();
    let mut out = Vec::with_capacity(items.len());
    for k in items {
        let key = py_key(&k)?;
        if seen.insert(key, ()).is_none() {
            out.push(k);
        }
    }
    Some(out)
}

// ---------------------------------------------------------------------------------------------
// Tags and the implicit resolver
// ---------------------------------------------------------------------------------------------

/// `tag:yaml.org,2002:null`.
pub const TAG_NULL: &str = "tag:yaml.org,2002:null";
/// `tag:yaml.org,2002:bool`.
pub const TAG_BOOL: &str = "tag:yaml.org,2002:bool";
/// `tag:yaml.org,2002:int`.
pub const TAG_INT: &str = "tag:yaml.org,2002:int";
/// `tag:yaml.org,2002:float`.
pub const TAG_FLOAT: &str = "tag:yaml.org,2002:float";
/// `tag:yaml.org,2002:str`.
pub const TAG_STR: &str = "tag:yaml.org,2002:str";
/// `tag:yaml.org,2002:binary`.
pub const TAG_BINARY: &str = "tag:yaml.org,2002:binary";
/// `tag:yaml.org,2002:timestamp`.
pub const TAG_TIMESTAMP: &str = "tag:yaml.org,2002:timestamp";
/// `tag:yaml.org,2002:merge`.
pub const TAG_MERGE: &str = "tag:yaml.org,2002:merge";
/// `tag:yaml.org,2002:value`.
pub const TAG_VALUE: &str = "tag:yaml.org,2002:value";
/// `tag:yaml.org,2002:yaml`.
pub const TAG_YAML: &str = "tag:yaml.org,2002:yaml";
/// `tag:yaml.org,2002:seq`.
pub const TAG_SEQ: &str = "tag:yaml.org,2002:seq";
/// `tag:yaml.org,2002:map`.
pub const TAG_MAP: &str = "tag:yaml.org,2002:map";
/// `tag:yaml.org,2002:set`.
pub const TAG_SET: &str = "tag:yaml.org,2002:set";
/// `tag:yaml.org,2002:omap`.
pub const TAG_OMAP: &str = "tag:yaml.org,2002:omap";
/// `tag:yaml.org,2002:pairs`.
pub const TAG_PAIRS: &str = "tag:yaml.org,2002:pairs";
/// `tag:yaml.org,2002:python/tuple`.
pub const TAG_PY_TUPLE: &str = "tag:yaml.org,2002:python/tuple";

macro_rules! re {
    ($name:ident, $pat:expr) => {
        static $name: LazyLock<Regex> = LazyLock::new(|| Regex::new($pat).expect("valid regex"));
    };
}

// Python's `$` also matches before a trailing newline, hence `\n?$`.
re!(
    BOOL_RE,
    r"^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)\n?$"
);
re!(
    FLOAT_RE,
    r"^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+][0-9]+)?|\.[0-9][0-9_]*(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))\n?$"
);
re!(
    INT_RE,
    r"^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)\n?$"
);
re!(MERGE_RE, r"^(?:<<)\n?$");
re!(NULL_RE, r"^(?:~|null|Null|NULL|)\n?$");
re!(
    TIMESTAMP_RE,
    r"^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)\n?$"
);
re!(VALUE_RE, r"^(?:=)\n?$");
re!(YAML_RE, r"^(?:!|&|\*)\n?$");
re!(
    TIMESTAMP_PARTS_RE,
    r"^(?P<year>[0-9][0-9][0-9][0-9])-(?P<month>[0-9][0-9]?)-(?P<day>[0-9][0-9]?)(?:(?:[Tt]|[ \t]+)(?P<hour>[0-9][0-9]?):(?P<minute>[0-9][0-9]):(?P<second>[0-9][0-9])(?:\.(?P<fraction>[0-9]*))?(?:[ \t]*(?P<tz>Z|(?P<tz_sign>[-+])(?P<tz_hour>[0-9][0-9]?)(?::(?P<tz_minute>[0-9][0-9]))?))?)?\n?$"
);

/// `Resolver.resolve(ScalarNode, value, (True, False))`: the tag an unquoted scalar gets.
/// `None` means no implicit resolver matched (the default `str` tag applies).
pub fn implicit_tag(value: &str) -> Option<&'static str> {
    let first = value.chars().next();
    // The resolvers registered for each first character, in registration order.
    let order: &[(&'static str, &LazyLock<Regex>, &str)] = &[
        (TAG_BOOL, &BOOL_RE, "yYnNtTfFoO"),
        (TAG_FLOAT, &FLOAT_RE, "-+0123456789."),
        (TAG_INT, &INT_RE, "-+0123456789"),
        (TAG_MERGE, &MERGE_RE, "<"),
        (TAG_NULL, &NULL_RE, "~nN"),
        (TAG_TIMESTAMP, &TIMESTAMP_RE, "0123456789"),
        (TAG_VALUE, &VALUE_RE, "="),
        (TAG_YAML, &YAML_RE, "!&*"),
    ];
    for (tag, re, firsts) in order {
        let eligible = match first {
            None => *tag == TAG_NULL,
            Some(c) => firsts.contains(c),
        };
        if eligible && re.is_match(value) {
            return Some(tag);
        }
    }
    None
}

/// The parts of a timestamp scalar (`SafeConstructor.timestamp_regexp`).
pub(crate) fn timestamp_parts(value: &str) -> Option<regex::Captures<'_>> {
    TIMESTAMP_PARTS_RE.captures(value)
}

// ---------------------------------------------------------------------------------------------
// A tagged JSON projection (for fixtures and tests)
// ---------------------------------------------------------------------------------------------

/// Project a node to JSON with a type tag on every value, so that the Python generator and the
/// Rust tests can compare typed results exactly: `{"t": "int", "v": "12"}`, `{"t": "dict", "v":
/// [[key, value], ...]}`, ...
pub fn tagged_json(node: &Node) -> serde_json::Value {
    use serde_json::{Value as J, json};
    match node {
        Node::Null => json!({"t": "none"}),
        Node::Bool(b) => json!({"t": "bool", "v": b}),
        Node::Int(i) => json!({"t": "int", "v": i.to_string()}),
        Node::Float(f) => json!({"t": "float", "v": float_repr(*f)}),
        Node::Str(s) => json!({"t": "str", "v": s}),
        Node::Bytes(b) => {
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            json!({"t": "bytes", "v": hex})
        }
        Node::Date(d) => json!({"t": "date", "v": d.isoformat()}),
        Node::DateTime(dt) => json!({"t": "datetime", "v": dt.isoformat('T')}),
        Node::Seq(items) => {
            json!({"t": "list", "v": items.iter().map(|i| tagged_json(i)).collect::<Vec<_>>()})
        }
        Node::Tuple(items) => {
            json!({"t": "tuple", "v": items.iter().map(|i| tagged_json(i)).collect::<Vec<_>>()})
        }
        Node::Map(entries) => json!({
            "t": "dict",
            "v": entries
                .iter()
                .map(|(k, v)| J::Array(vec![tagged_json(k), tagged_json(v)]))
                .collect::<Vec<_>>()
        }),
        Node::Set(items) => {
            let mut v: Vec<J> = items.iter().map(|i| tagged_json(i)).collect();
            v.sort_by_key(|j| j.to_string());
            json!({"t": "set", "v": v})
        }
    }
}

/// An integer node's value as `i64`, when it fits.
pub fn int_to_i64(node: &Node) -> Option<i64> {
    match node {
        Node::Int(i) => i.to_i64(),
        _ => None,
    }
}
