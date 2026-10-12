//! A Python-like scalar value, the unit of the kernel's object (non-native) columns.
//!
//! Much of the v4 dataframe code works per value (`Series.apply`) and inspects the *first valid
//! value* of a column to decide its role (`get_json_cols`, `get_numeric_cols`, ...). `Value`
//! reproduces the Python objects those code paths see, including the distinction between
//! `None`, `pd.NA` and `pd.NaT`, and Python's `str()` of each (used by `value_is_null`,
//! `attempt_cast_to_*` and the `coerce_types` path of `query_df`).

use std::fmt;
use std::hash::{Hash, Hasher};

use chrono::{
    DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, Offset, TimeZone, Timelike,
};

use meerschaum_core::pyfmt::{bytes_repr, float_repr, py_repr_json};

use crate::decimal::Decimal;
use crate::geometry::Geometry;

/// A timezone attached to a datetime value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TzInfo {
    Utc,
    Fixed(FixedOffset),
    Named(chrono_tz::Tz),
}

impl TzInfo {
    /// Parse a timezone name as pandas prints it (`UTC`, `America/New_York`, `UTC-04:00`,
    /// `+05:30`).
    pub fn parse(name: &str) -> Option<Self> {
        let trimmed = name.trim();
        if trimmed.eq_ignore_ascii_case("utc") || trimmed == "Z" || trimmed == "tzutc()" {
            return Some(TzInfo::Utc);
        }
        let offset_str = trimmed
            .strip_prefix("UTC")
            .or_else(|| trimmed.strip_prefix("utc"))
            .unwrap_or(trimmed);
        if let Some(off) = parse_offset(offset_str) {
            return Some(if off.local_minus_utc() == 0 {
                TzInfo::Utc
            } else {
                TzInfo::Fixed(off)
            });
        }
        trimmed.parse::<chrono_tz::Tz>().ok().map(TzInfo::Named)
    }

    /// The UTC offset at a given UTC instant.
    pub fn offset_at(&self, utc: &NaiveDateTime) -> FixedOffset {
        match self {
            TzInfo::Utc => FixedOffset::east_opt(0).expect("zero offset"),
            TzInfo::Fixed(off) => *off,
            TzInfo::Named(tz) => tz.offset_from_utc_datetime(utc).fix(),
        }
    }

    /// The name pandas uses for this timezone in dtype strings.
    pub fn name(&self) -> String {
        match self {
            TzInfo::Utc => "UTC".to_string(),
            TzInfo::Fixed(off) => format!("UTC{}", format_offset(off.local_minus_utc(), true)),
            TzInfo::Named(tz) => tz.name().to_string(),
        }
    }

    /// Resolve a local wall time to UTC (ambiguous times take the earliest).
    pub fn local_to_utc(&self, local: &NaiveDateTime) -> Option<NaiveDateTime> {
        match self {
            TzInfo::Utc => Some(*local),
            TzInfo::Fixed(off) => {
                Some(*local - chrono::Duration::seconds(off.local_minus_utc() as i64))
            }
            TzInfo::Named(tz) => tz
                .from_local_datetime(local)
                .earliest()
                .map(|d| d.naive_utc()),
        }
    }
}

/// A datetime (`datetime.datetime` / `pd.Timestamp`).
///
/// `utc` is the UTC instant when `tz` is set, otherwise the naive wall time.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DateTimeValue {
    pub utc: NaiveDateTime,
    pub tz: Option<TzInfo>,
}

impl DateTimeValue {
    pub fn naive(dt: NaiveDateTime) -> Self {
        Self { utc: dt, tz: None }
    }

    pub fn utc(dt: NaiveDateTime) -> Self {
        Self {
            utc: dt,
            tz: Some(TzInfo::Utc),
        }
    }

    /// The local wall time.
    pub fn local(&self) -> NaiveDateTime {
        match &self.tz {
            None => self.utc,
            Some(tz) => {
                self.utc
                    + chrono::Duration::seconds(tz.offset_at(&self.utc).local_minus_utc() as i64)
            }
        }
    }

    /// `dt.isoformat()` (Python `datetime`).
    pub fn isoformat(&self) -> String {
        self.format_with_sep('T')
    }

    fn format_with_sep(&self, sep: char) -> String {
        let local = self.local();
        let mut out = format!(
            "{:04}-{:02}-{:02}{sep}{:02}:{:02}:{:02}",
            local.year(),
            local.month(),
            local.day(),
            local.hour(),
            local.minute(),
            local.second()
        );
        let nanos = local.nanosecond() % 1_000_000_000;
        if nanos % 1000 != 0 {
            out.push_str(&format!(".{nanos:09}"));
        } else if nanos != 0 {
            out.push_str(&format!(".{:06}", nanos / 1000));
        }
        if let Some(tz) = &self.tz {
            out.push_str(&format_offset(
                tz.offset_at(&self.utc).local_minus_utc(),
                true,
            ));
        }
        out
    }

    /// Nanoseconds since the Unix epoch of `utc` (the naive wall time for naive values).
    pub fn timestamp_nanos(&self) -> Option<i64> {
        self.utc.and_utc().timestamp_nanos_opt()
    }

    /// Microseconds since the Unix epoch of `utc`.
    pub fn timestamp_micros(&self) -> i64 {
        self.utc.and_utc().timestamp_micros()
    }

    pub fn from_timestamp(
        value: i64,
        unit: polars::prelude::TimeUnit,
        tz: Option<TzInfo>,
    ) -> Option<Self> {
        use polars::prelude::TimeUnit;
        let dt = match unit {
            TimeUnit::Nanoseconds => DateTime::from_timestamp_nanos(value),
            TimeUnit::Microseconds => DateTime::from_timestamp_micros(value)?,
            TimeUnit::Milliseconds => DateTime::from_timestamp_millis(value)?,
        };
        Some(Self {
            utc: dt.naive_utc(),
            tz,
        })
    }

    pub fn to_timestamp(&self, unit: polars::prelude::TimeUnit) -> Option<i64> {
        use polars::prelude::TimeUnit;
        let dt = self.utc.and_utc();
        match unit {
            TimeUnit::Nanoseconds => dt.timestamp_nanos_opt(),
            TimeUnit::Microseconds => Some(dt.timestamp_micros()),
            TimeUnit::Milliseconds => Some(dt.timestamp_millis()),
        }
    }
}

/// A Python-like scalar.
#[derive(Clone, Debug, Default)]
pub enum Value {
    /// `None`
    #[default]
    Null,
    /// `pd.NA`
    NA,
    /// `pd.NaT`
    NaT,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Decimal(Decimal),
    Uuid(uuid::Uuid),
    Datetime(DateTimeValue),
    Date(NaiveDate),
    /// A `dict` or `list` (JSON container; scalars inside are plain JSON).
    Json(serde_json::Value),
    /// A shapely geometry.
    Geometry(Geometry),
}

impl Value {
    /// `pd.isna(value)`: `None`, `pd.NA`, `pd.NaT`, float `NaN` and `Decimal('NaN')`.
    pub fn is_na(&self) -> bool {
        match self {
            Value::Null | Value::NA | Value::NaT => true,
            Value::Float(f) => f.is_nan(),
            Value::Decimal(d) => d.is_nan(),
            _ => false,
        }
    }

    /// Python's `str(value)`.
    pub fn py_str(&self) -> String {
        match self {
            Value::Null => "None".to_string(),
            Value::NA => "<NA>".to_string(),
            Value::NaT => "NaT".to_string(),
            Value::Bool(b) => if *b { "True" } else { "False" }.to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => float_repr(*f),
            Value::Str(s) => s.clone(),
            Value::Bytes(b) => bytes_repr(b),
            Value::Decimal(d) => d.to_sci_string(),
            Value::Uuid(u) => u.hyphenated().to_string(),
            Value::Datetime(dt) => dt.format_with_sep(' '),
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::Json(j) => py_repr_json(j),
            Value::Geometry(g) => g.to_wkt_full(),
        }
    }

    /// A short type name for diagnostics.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "None",
            Value::NA => "NAType",
            Value::NaT => "NaTType",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::Str(_) => "str",
            Value::Bytes(_) => "bytes",
            Value::Decimal(_) => "Decimal",
            Value::Uuid(_) => "UUID",
            Value::Datetime(_) => "datetime",
            Value::Date(_) => "date",
            Value::Json(j) => {
                if j.is_array() {
                    "list"
                } else {
                    "dict"
                }
            }
            Value::Geometry(_) => "shapely",
        }
    }

    /// Whether the value is hashable in Python (`dict`/`list` are not).
    pub fn is_hashable(&self) -> bool {
        !matches!(self, Value::Json(_))
    }
}

impl PartialEq for Value {
    /// Equality as pandas `merge` / `isin` treat keys: nulls of any kind match each other,
    /// `NaN` matches `NaN`, numbers compare numerically across `int`/`float`/`bool`.
    fn eq(&self, other: &Self) -> bool {
        key_eq(self, other)
    }
}
impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            v if v.is_na() => 0u8.hash(state),
            Value::Bool(b) => numeric_hash(if *b { 1.0 } else { 0.0 }, state),
            Value::Int(i) => numeric_hash(*i as f64, state),
            Value::Float(f) => numeric_hash(*f, state),
            Value::Str(s) => {
                2u8.hash(state);
                s.hash(state)
            }
            Value::Bytes(b) => {
                3u8.hash(state);
                b.hash(state)
            }
            Value::Decimal(d) => {
                4u8.hash(state);
                d.hash(state)
            }
            Value::Uuid(u) => {
                5u8.hash(state);
                u.hash(state)
            }
            Value::Datetime(d) => {
                6u8.hash(state);
                d.utc.hash(state);
                d.tz.is_some().hash(state)
            }
            Value::Date(d) => {
                7u8.hash(state);
                d.hash(state)
            }
            Value::Json(j) => {
                8u8.hash(state);
                j.to_string().hash(state)
            }
            Value::Geometry(g) => {
                9u8.hash(state);
                g.to_wkb(false).hash(state)
            }
            _ => 0u8.hash(state),
        }
    }
}

fn numeric_hash<H: Hasher>(f: f64, state: &mut H) {
    1u8.hash(state);
    if f == 0.0 {
        0u64.hash(state)
    } else {
        f.to_bits().hash(state)
    }
}

fn as_number(v: &Value) -> Option<f64> {
    match v {
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    }
}

fn key_eq(a: &Value, b: &Value) -> bool {
    if a.is_na() || b.is_na() {
        return a.is_na() && b.is_na();
    }
    if let (Some(x), Some(y)) = (as_number(a), as_number(b)) {
        if let (Value::Int(i), Value::Int(j)) = (a, b) {
            return i == j;
        }
        return x == y;
    }
    match (a, b) {
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::Bytes(x), Value::Bytes(y)) => x == y,
        (Value::Decimal(x), Value::Decimal(y)) => x == y,
        (Value::Uuid(x), Value::Uuid(y)) => x == y,
        (Value::Datetime(x), Value::Datetime(y)) => {
            x.utc == y.utc && x.tz.is_some() == y.tz.is_some()
        }
        (Value::Date(x), Value::Date(y)) => x == y,
        (Value::Json(x), Value::Json(y)) => x == y,
        (Value::Geometry(x), Value::Geometry(y)) => x.to_wkb(false) == y.to_wkb(false),
        _ => false,
    }
}

impl From<&meerschaum_core::Value> for Value {
    /// A parameters / config value as a cell: scalars map one to one, `list` / `dict` become
    /// [`Value::Json`].
    fn from(v: &meerschaum_core::Value) -> Self {
        use meerschaum_core::Value as V;
        match v {
            V::Null => Value::Null,
            V::Bool(b) => Value::Bool(*b),
            V::Int(i) => Value::Int(*i),
            V::Float(f) => Value::Float(*f),
            V::Str(s) => Value::Str(s.clone()),
            V::List(_) | V::Map(_) => {
                Value::Json(serde_json::to_value(v).unwrap_or(serde_json::Value::Null))
            }
        }
    }
}

impl From<meerschaum_core::Value> for Value {
    fn from(v: meerschaum_core::Value) -> Self {
        Value::from(&v)
    }
}

impl Value {
    /// The JSON-compatible [`meerschaum_core::Value`] for this cell: nulls of every kind become
    /// `None`, scalars and JSON containers map one to one. `None` for cells with no JSON form
    /// (bytes, decimals, UUIDs, datetimes, dates and geometries are Python objects).
    pub fn to_core_value(&self) -> Option<meerschaum_core::Value> {
        use meerschaum_core::Value as V;
        Some(match self {
            Value::Null | Value::NA | Value::NaT => V::Null,
            Value::Bool(b) => V::Bool(*b),
            Value::Int(i) => V::Int(*i),
            Value::Float(f) => V::Float(*f),
            Value::Str(s) => V::Str(s.clone()),
            Value::Json(j) => serde_json::from_value(j.clone()).ok()?,
            _ => return None,
        })
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.py_str())
    }
}

/// Format a UTC offset in seconds as `+HH:MM` (with the colon) or `+HHMM`.
pub fn format_offset(seconds: i32, colon: bool) -> String {
    let sign = if seconds < 0 { '-' } else { '+' };
    let s = seconds.abs();
    let (h, m, sec) = (s / 3600, (s % 3600) / 60, s % 60);
    let mut out = if colon {
        format!("{sign}{h:02}:{m:02}")
    } else {
        format!("{sign}{h:02}{m:02}")
    };
    if sec != 0 {
        out.push_str(&format!(":{sec:02}"));
    }
    out
}

fn parse_offset(s: &str) -> Option<FixedOffset> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (sign, rest) = match s.as_bytes()[0] {
        b'+' => (1, &s[1..]),
        b'-' => (-1, &s[1..]),
        _ => return None,
    };
    let digits: String = rest.chars().filter(|c| *c != ':').collect();
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let (h, m) = match digits.len() {
        1 | 2 => (digits.parse::<i32>().ok()?, 0),
        4 => (
            digits[..2].parse::<i32>().ok()?,
            digits[2..].parse::<i32>().ok()?,
        ),
        _ => return None,
    };
    FixedOffset::east_opt(sign * (h * 3600 + m * 60))
}

/// Parse an ISO-8601-like datetime string (the subset accepted by
/// `pd.to_datetime(format='ISO8601')` and commonly by `dateutil.parser.parse`).
///
/// Returns the wall time and an optional offset. Accepts `YYYY-MM-DD`, a `T` or space separator,
/// `HH:MM[:SS[.fffffffff]]`, and `Z` / `±HH[:MM]` / `±HHMM` / ` UTC` suffixes.
pub fn parse_iso_datetime(s: &str) -> Option<(NaiveDateTime, Option<FixedOffset>)> {
    let s = s.trim();
    if s.len() < 8 {
        return None;
    }
    let (date_part, rest) = if s.len() >= 10 && s.as_bytes().get(4) == Some(&b'-') {
        (&s[..10], &s[10..])
    } else if s.len() >= 8 && s[..8].bytes().all(|b| b.is_ascii_digit()) {
        (&s[..8], &s[8..])
    } else {
        return None;
    };
    let date = if date_part.len() == 10 {
        NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()?
    } else {
        NaiveDate::parse_from_str(date_part, "%Y%m%d").ok()?
    };
    if rest.is_empty() {
        return Some((date.and_hms_opt(0, 0, 0)?, None));
    }
    let sep = rest.as_bytes()[0];
    if sep != b'T' && sep != b't' && sep != b' ' {
        return None;
    }
    let rest = rest[1..].trim_start();
    // Split the time from an offset suffix.
    let mut time_end = rest.len();
    let mut offset: Option<FixedOffset> = None;
    if let Some(stripped) = rest.strip_suffix('Z').or_else(|| rest.strip_suffix('z')) {
        time_end = stripped.len();
        offset = FixedOffset::east_opt(0);
    } else if let Some(stripped) = rest
        .strip_suffix("UTC")
        .or_else(|| rest.strip_suffix("utc"))
    {
        time_end = stripped.trim_end().len();
        offset = FixedOffset::east_opt(0);
    } else if let Some(ix) = rest.rfind(['+', '-']) {
        if ix > 0 {
            offset = Some(parse_offset(&rest[ix..])?);
            time_end = ix;
        }
    }
    let time_str = rest[..time_end].trim_end();
    let mut parts = time_str.splitn(3, ':');
    let h: u32 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let (sec, nanos) = match parts.next() {
        Some(p) => {
            let (whole, frac) = match p.split_once(['.', ',']) {
                Some((w, f)) => (w, f),
                None => (p, ""),
            };
            let sec: u32 = whole.parse().ok()?;
            if !frac.bytes().all(|b| b.is_ascii_digit()) || frac.len() > 9 {
                return None;
            }
            let nanos: u32 = if frac.is_empty() {
                0
            } else {
                format!("{frac:0<9}").parse().ok()?
            };
            (sec, nanos)
        }
        None => (0, 0),
    };
    let naive = date.and_hms_nano_opt(h, m, sec, nanos)?;
    Some((naive, offset))
}

/// Parse a datetime string into a [`DateTimeValue`] (aware when an offset is present).
pub fn parse_datetime_value(s: &str) -> Option<DateTimeValue> {
    let (naive, offset) = parse_iso_datetime(s)?;
    Some(match offset {
        None => DateTimeValue::naive(naive),
        Some(off) => {
            let utc = naive - chrono::Duration::seconds(off.local_minus_utc() as i64);
            DateTimeValue {
                utc,
                tz: Some(if off.local_minus_utc() == 0 {
                    TzInfo::Utc
                } else {
                    TzInfo::Fixed(off)
                }),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_repr_matches_python() {
        assert_eq!(float_repr(1.0), "1.0");
        assert_eq!(float_repr(1.5), "1.5");
        assert_eq!(float_repr(1e16), "1e+16");
        assert_eq!(float_repr(1e-5), "1e-05");
        assert_eq!(float_repr(0.0001), "0.0001");
        assert_eq!(float_repr(123_456_789.123_456_79), "123456789.12345679");
        assert_eq!(float_repr(1e20), "1e+20");
        assert_eq!(float_repr(1234567890123456.0), "1234567890123456.0");
        assert_eq!(float_repr(-0.0), "-0.0");
        assert_eq!(float_repr(9007199254740993.0), "9007199254740992.0");
    }

    #[test]
    fn core_value_round_trip() {
        let core = meerschaum_core::Value::from_json_str(r#"{"a": [1, 2.5, null]}"#).unwrap();
        let cell = Value::from(&core);
        assert!(matches!(cell, Value::Json(_)));
        assert_eq!(cell.py_str(), "{'a': [1, 2.5, None]}");
        assert_eq!(cell.to_core_value(), Some(core));
        assert_eq!(
            Value::NA.to_core_value(),
            Some(meerschaum_core::Value::Null)
        );
        assert_eq!(Value::Bytes(vec![1]).to_core_value(), None);
    }

    #[test]
    fn iso_parse() {
        let v = parse_datetime_value("2024-01-01T12:00:00.123456-05:00").unwrap();
        assert_eq!(v.isoformat(), "2024-01-01T12:00:00.123456-05:00");
        assert!(parse_datetime_value("not a date").is_none());
    }
}
