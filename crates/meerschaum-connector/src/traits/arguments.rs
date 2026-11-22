//! The argument types of connector methods: pipe IDs, date bounds, the data filters of
//! [`DataQuery`], CPI samples and the keyword arguments of `sync_pipe` ([`SyncOptions`]).

use std::fmt;

use chrono::{DateTime, Utc};
use meerschaum_core::{Map, Value};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use serde::{Deserialize, Serialize};

/// A pipe's ID on its instance: an integer on SQL and Valkey, a string on custom instances (§5.1).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PipeId {
    /// An integer ID.
    Int(i64),
    /// A string ID.
    Str(String),
}

impl fmt::Display for PipeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipeId::Int(i) => write!(f, "{i}"),
            PipeId::Str(s) => f.write_str(s),
        }
    }
}

impl PipeId {
    /// Parse a key as Python does for the dict form of `fetch_pipes_keys`
    /// (`int(k) if str(k).isdigit() else k`).
    pub fn parse(s: &str) -> PipeId {
        if !s.is_empty()
            && s.chars().all(|c| c.is_ascii_digit())
            && let Ok(i) = s.parse()
        {
            return PipeId::Int(i);
        }
        PipeId::Str(s.to_string())
    }

    /// The ID as a core [`Value`].
    pub fn to_value(&self) -> Value {
        match self {
            PipeId::Int(i) => Value::Int(*i),
            PipeId::Str(s) => Value::Str(s.clone()),
        }
    }
}

/// A bound on a pipe's datetime axis (`begin`, `end`, a sync time): a timestamp or an integer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DateBound {
    /// A timestamp (aware or naive).
    Datetime(DateTimeValue),
    /// An integer axis value.
    Int(i64),
}

impl DateBound {
    /// A UTC timestamp.
    pub fn utc(dt: DateTime<Utc>) -> DateBound {
        DateBound::Datetime(DateTimeValue::utc(dt.naive_utc()))
    }

    /// Python's `str(value)` (`2024-01-01 00:00:00+00:00`), as `requests` encodes query params.
    pub fn py_str(&self) -> String {
        match self {
            DateBound::Datetime(dt) => KValue::Datetime(dt.clone()).py_str(),
            DateBound::Int(i) => i.to_string(),
        }
    }

    /// `value.isoformat()` for timestamps, `str(value)` for integers.
    pub fn isoformat(&self) -> String {
        match self {
            DateBound::Datetime(dt) => dt.isoformat(),
            DateBound::Int(i) => i.to_string(),
        }
    }

    /// The bound as a kernel value.
    pub fn to_value(&self) -> KValue {
        match self {
            DateBound::Datetime(dt) => KValue::Datetime(dt.clone()),
            DateBound::Int(i) => KValue::Int(*i),
        }
    }

    /// A bound from a kernel value (datetimes and integers only).
    pub fn from_value(value: &KValue) -> Option<DateBound> {
        match value {
            KValue::Datetime(dt) => Some(DateBound::Datetime(dt.clone())),
            KValue::Int(i) => Some(DateBound::Int(*i)),
            _ => None,
        }
    }

    /// Parse an integer or an ISO-8601 timestamp.
    pub fn parse(s: &str) -> Option<DateBound> {
        let t = s.trim();
        if let Ok(i) = t.parse::<i64>() {
            return Some(DateBound::Int(i));
        }
        meerschaum_kernel::value::parse_datetime_value(t).map(DateBound::Datetime)
    }
}

impl From<meerschaum_core::success::Bound> for DateBound {
    fn from(b: meerschaum_core::success::Bound) -> Self {
        match b {
            meerschaum_core::success::Bound::Datetime(dt) => DateBound::utc(dt),
            meerschaum_core::success::Bound::Int(i) => DateBound::Int(i),
        }
    }
}

/// The filters shared by `get_pipe_data`, `get_pipe_docs`, `clear_pipe` and
/// `get_pipe_rowcount`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DataQuery {
    /// `select_columns`: only select these columns.
    pub select_columns: Option<Vec<String>>,
    /// `omit_columns`: remove these columns from the selection.
    pub omit_columns: Option<Vec<String>>,
    /// `begin`: the earliest datetime-axis value (inclusive).
    pub begin: Option<DateBound>,
    /// `end`: the latest datetime-axis value (exclusive).
    pub end: Option<DateBound>,
    /// `params`: the `build_where` / `query_df` filter grammar.
    pub params: Option<Map>,
    /// `limit`.
    pub limit: Option<i64>,
    /// `order` (`'asc'` / `'desc'`).
    pub order: Option<String>,
    /// `as_chunks` (sent by the API client; ignored by most instances).
    pub as_chunks: bool,
    /// `remote` (rowcount of the fetch definition instead of the target table).
    pub remote: bool,
}

/// Characteristic-polynomial samples for one (possibly unnamed) immutable series.
#[derive(Debug)]
pub struct CpiSamples {
    pub rows: i64,
    pub distinct_timestamps: i64,
    pub values: Vec<i64>,
}

/// Polynomial samples keyed by series ID.
pub type CpiSeries = std::collections::BTreeMap<Option<String>, CpiSamples>;

/// One missing-timestamp polynomial per series, with coefficients in ascending order.
pub type CpiPolynomials = Vec<(Option<String>, Vec<i64>)>;

/// The modulus used by both the SQL-side CPISync samples and the engine's solver.
pub const CPI_PRIME: i64 = (1 << 61) - 1;

/// The first evaluation point lies outside the range of UTC day offsets in microseconds.
pub const CPI_FIRST_POINT: i64 = 86_400_000_001;

/// Keyword arguments for `sync_pipe`.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncOptions {
    /// `check_existing`: diff against existing rows (default `True`).
    pub check_existing: bool,
    /// `chunksize`: `Some(-1)` (default) uses `system:connectors:sql:chunksize`; `None` syncs
    /// everything in one chunk.
    pub chunksize: Option<i64>,
    /// `begin` / `end` (SQL fetch bounds). With [`SyncOptions::replace`], the window
    /// `[begin, end)` whose rows the frame replaces (`None`: unbounded on that side).
    pub begin: Option<DateBound>,
    /// See `begin`.
    pub end: Option<DateBound>,
    /// `replace`: in one transaction (where the instance has transactions), delete the pipe's
    /// rows in `[begin, end)` that match [`SyncOptions::params`], then write the frame's rows
    /// of that window. Rows of the frame outside the window are not written. A pipe without a
    /// datetime column has one window: the whole table. An empty frame clears the window.
    pub replace: bool,
    /// `params`: with [`SyncOptions::replace`], only the window's rows matching these filters
    /// are replaced (the `params` grammar of `build_where`).
    pub params: Option<Map>,
    /// Any other keyword arguments (the API client forwards them as query parameters).
    pub extra: Map,
}

impl Default for SyncOptions {
    fn default() -> Self {
        SyncOptions {
            check_existing: true,
            chunksize: Some(-1),
            begin: None,
            end: None,
            replace: false,
            params: None,
            extra: Map::new(),
        }
    }
}
