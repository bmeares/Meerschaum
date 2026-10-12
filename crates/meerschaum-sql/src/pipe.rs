//! A local, minimal pipe description for the query builders.
//!
//! TODO(core-merge): replace [`PipeSpec`] with `meerschaum_core::Pipe` once it lands. Only the
//! attributes the SQL query builders read are modeled: keys, the resolved `target`, and the
//! `parameters` dict (columns, dtypes, indices, upsert, static, null_indices, autoincrement,
//! hypertable, hypercore, compress, precision, verify, fetch, schema, index_template).

use chrono::TimeDelta;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::dtypes::{are_dtypes_equal, precision_unit_alias, precision_unit_scalar};
use crate::value::PyValue;

/// `pipes.parameters.verify.chunk_minutes` default (`config/_default.py:276`).
/// TODO(core-merge): read from the config registry.
pub const DEFAULT_CHUNK_MINUTES: i64 = 43200;

/// `pipes.parameters.fetch.backtrack_minutes` default (`config/_default.py:272`).
/// TODO(core-merge): read from the config registry.
pub const DEFAULT_BACKTRACK_MINUTES: i64 = 1440;

/// `STATIC_CONFIG['dtypes']['datetime']['default_precision_unit']`.
pub const DEFAULT_PRECISION_UNIT: &str = "microsecond";

/// `pipes.autotime.column_name_if_datetime_missing` default.
pub const AUTOTIME_COLUMN_NAME: &str = "ts";

/// A chunk or backtrack interval: a `timedelta` on a datetime axis, an `int` on an integer axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interval {
    /// A duration (`datetime.timedelta`).
    Duration(TimeDelta),
    /// Epoch units of an integer axis.
    Int(i64),
}

impl Interval {
    /// `timedelta(minutes=m)`.
    pub fn minutes(m: i64) -> Self {
        Interval::Duration(TimeDelta::minutes(m))
    }

    /// `int(interval.total_seconds() / 60)` for durations, the value itself for ints.
    pub fn whole_minutes(&self) -> i64 {
        match self {
            Interval::Duration(d) => (total_seconds(*d) / 60.0) as i64,
            Interval::Int(i) => *i,
        }
    }
}

/// `timedelta.total_seconds()`.
pub fn total_seconds(d: TimeDelta) -> f64 {
    d.num_microseconds()
        .map(|us| us as f64 / 1e6)
        .unwrap_or_else(|| d.num_seconds() as f64)
}

/// One `indices` entry: a single column or a list of columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IndexColumns {
    /// `'col'`.
    One(String),
    /// `['a', 'b']`.
    Many(Vec<String>),
}

impl IndexColumns {
    /// The columns as a list (`[cols]` for a single column).
    pub fn as_vec(&self) -> Vec<String> {
        match self {
            IndexColumns::One(c) => vec![c.clone()],
            IndexColumns::Many(cs) => cs.clone(),
        }
    }

    /// `'_'.join(cols)` for lists, `str(cols)` otherwise.
    pub fn joined(&self) -> String {
        match self {
            IndexColumns::One(c) => c.clone(),
            IndexColumns::Many(cs) => cs.join("_"),
        }
    }

    fn is_truthy(&self) -> bool {
        match self {
            IndexColumns::One(c) => !c.is_empty(),
            IndexColumns::Many(cs) => !cs.is_empty(),
        }
    }
}

/// The pipe attributes the SQL query builders need.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipeSpec {
    /// `pipe.connector_keys`.
    pub connector_keys: String,
    /// `pipe.metric_key`.
    pub metric_key: String,
    /// `pipe.location_key`.
    #[serde(default)]
    pub location_key: Option<String>,
    /// The resolved (already truncated) `pipe.target`.
    pub target: String,
    /// `pipe.parameters`.
    #[serde(default)]
    pub parameters: IndexMap<String, PyValue>,
}

impl PipeSpec {
    /// A pipe with empty parameters and the legacy default target.
    pub fn new(connector_keys: &str, metric_key: &str, location_key: Option<&str>) -> Self {
        let mut target = format!("{}_{}", connector_keys.replace(':', "_"), metric_key);
        if let Some(lk) = location_key {
            target.push('_');
            target.push_str(lk);
        }
        PipeSpec {
            connector_keys: connector_keys.to_string(),
            metric_key: metric_key.to_string(),
            location_key: location_key.map(str::to_string),
            target,
            parameters: IndexMap::new(),
        }
    }

    /// A top-level parameter.
    pub fn param(&self, key: &str) -> Option<&PyValue> {
        self.parameters.get(key)
    }

    fn bool_param(&self, key: &str, default: bool) -> bool {
        match self.param(key) {
            None => default,
            Some(v) => v.is_truthy(),
        }
    }

    /// `pipe.columns`, keeping only truthy string values (`{role: col}`).
    pub fn columns(&self) -> IndexMap<String, String> {
        let mut out = IndexMap::new();
        if let Some(PyValue::Dict(cols)) = self.param("columns") {
            for (role, col) in cols {
                if let PyValue::Str(c) = col {
                    if !c.is_empty() {
                        out.insert(role.clone(), c.clone());
                    }
                }
            }
        }
        out
    }

    /// `pipe.columns.get(role)`.
    pub fn column(&self, role: &str) -> Option<String> {
        self.columns().get(role).cloned()
    }

    /// The raw `pipe.parameters['dtypes']` (values via `str()`, falsy values dropped).
    pub fn configured_dtypes(&self) -> IndexMap<String, String> {
        let mut out = IndexMap::new();
        if let Some(PyValue::Dict(d)) = self.param("dtypes") {
            for (col, typ) in d {
                if !col.is_empty() && typ.is_truthy() {
                    out.insert(col.clone(), typ.py_str());
                }
            }
        }
        out
    }

    /// `pipe.dtypes` without remote inference (`Pipe.get_dtypes(infer=False)`,
    /// `core/Pipe/_attributes.py:351`): aliases resolved, the datetime column defaults to
    /// `'datetime'`, and an autoincrement primary key defaults to `'int'`.
    ///
    /// TODO(core-merge): Python's default `infer=True` also merges the table's existing dtypes.
    pub fn dtypes(&self) -> IndexMap<String, String> {
        let configured = self.configured_dtypes();
        let mut out: IndexMap<String, String> = configured
            .iter()
            .map(|(c, t)| {
                (
                    c.clone(),
                    crate::dtypes::alias_dtype(t).unwrap_or(t).to_string(),
                )
            })
            .collect();
        let columns = self.columns();
        if let Some(dt) = columns.get("datetime") {
            if !configured.contains_key(dt) {
                out.insert(dt.clone(), "datetime".to_string());
            }
        }
        if let Some(pk) = columns.get("primary") {
            if self.autoincrement() && !out.contains_key(pk) {
                out.insert(pk.clone(), "int".to_string());
            }
        }
        out
    }

    /// `pipe.upsert`.
    pub fn upsert(&self) -> bool {
        self.bool_param("upsert", false)
    }

    /// `pipe.static`.
    pub fn is_static(&self) -> bool {
        self.bool_param("static", false)
    }

    /// `pipe.null_indices`.
    pub fn null_indices(&self) -> bool {
        self.bool_param("null_indices", true)
    }

    /// `pipe.autoincrement`.
    pub fn autoincrement(&self) -> bool {
        self.bool_param("autoincrement", false)
    }

    /// `pipe.autotime`.
    pub fn autotime(&self) -> bool {
        self.bool_param("autotime", false)
    }

    /// `pipe.parameters.get('hypertable', True)`.
    pub fn hypertable(&self) -> bool {
        self.bool_param("hypertable", true)
    }

    /// `pipe.parameters.get('hypercore', True)`.
    pub fn hypercore(&self) -> bool {
        self.bool_param("hypercore", true)
    }

    /// `pipe.parameters.get('schema')`.
    pub fn schema_param(&self) -> Option<String> {
        self.param("schema").and_then(|v| match v {
            PyValue::None => None,
            other => Some(other.py_str()),
        })
    }

    /// `pipe.indices`: `{'unique': [...]}` (when more than one index column), the columns, then
    /// `parameters['indices']` (or `'indexes'`), merged with dict-update semantics.
    ///
    /// PARITY: Python builds the `unique` list with `list(set(...))` (`_attributes.py:258`), so
    /// its order depends on `PYTHONHASHSEED`. This uses a deterministic order: the datetime
    /// column first, then the other columns in `columns` order (the order before the `set`).
    pub fn indices(&self) -> IndexMap<String, IndexColumns> {
        let columns = self.columns();
        let dt_col = columns.get("datetime").cloned();
        let mut unique_cols: Vec<String> = Vec::new();
        if let Some(dt) = &dt_col {
            unique_cols.push(dt.clone());
        }
        for (role, col) in &columns {
            if role != "datetime" && !unique_cols.contains(col) {
                unique_cols.push(col.clone());
            }
        }
        let mut out = IndexMap::new();
        if unique_cols.len() > 1 {
            out.insert("unique".to_string(), IndexColumns::Many(unique_cols));
        }
        for (role, col) in &columns {
            out.insert(role.clone(), IndexColumns::One(col.clone()));
        }
        let key = if self.parameters.contains_key("indexes") {
            "indexes"
        } else {
            "indices"
        };
        if let Some(PyValue::Dict(extra)) = self.param(key) {
            for (ix, cols) in extra {
                let value = match cols {
                    PyValue::Str(s) => IndexColumns::One(s.clone()),
                    PyValue::List(items) => {
                        IndexColumns::Many(items.iter().map(PyValue::py_str).collect())
                    }
                    PyValue::None => IndexColumns::Many(Vec::new()),
                    other => IndexColumns::One(other.py_str()),
                };
                out.insert(ix.clone(), value);
            }
        }
        out
    }

    /// Indices with truthy column values (the `_column_names` filter in `get_pipe_index_names`).
    pub fn truthy_indices(&self) -> IndexMap<String, IndexColumns> {
        self.indices()
            .into_iter()
            .filter(|(_, c)| c.is_truthy())
            .collect()
    }

    /// The datetime axis dtype string (`pipe.dtypes.get(dt_col, 'datetime')`).
    pub fn dt_dtype(&self) -> Option<String> {
        let dt = self.column("datetime")?;
        Some(
            self.dtypes()
                .get(&dt)
                .cloned()
                .unwrap_or_else(|| "datetime".to_string()),
        )
    }

    /// Whether the datetime axis is an integer (`'int' in str(dt_dtype).lower()`).
    pub fn is_int_axis(&self) -> bool {
        self.dt_dtype()
            .is_some_and(|t| t.to_lowercase().contains("int"))
    }

    /// `pipe.precision['unit']` resolved through the aliases (`Pipe.get_precision`).
    pub fn precision_unit(&self) -> String {
        let mut unit: Option<String> = match self.param("precision") {
            Some(PyValue::Str(s)) if !s.is_empty() => Some(s.clone()),
            Some(PyValue::Dict(d)) if !d.is_empty() => Some(
                d.get("unit")
                    .map(PyValue::py_str)
                    .unwrap_or_else(|| DEFAULT_PRECISION_UNIT.to_string()),
            ),
            _ => None,
        };
        if unit.is_none() {
            let mut dt_col = self.column("datetime");
            if dt_col.is_none() && self.autotime() {
                dt_col = Some(AUTOTIME_COLUMN_NAME.to_string());
            }
            let Some(dt_col) = dt_col else {
                return DEFAULT_PRECISION_UNIT.to_string();
            };
            let mut dt_typ = self
                .dtypes()
                .get(&dt_col)
                .cloned()
                .unwrap_or_else(|| "datetime".to_string());
            if are_dtypes_equal(&dt_typ, "datetime") {
                if dt_typ == "datetime" {
                    dt_typ = "datetime64[us, UTC]".to_string();
                }
                let after = dt_typ.split_once('[').map(|(_, r)| r).unwrap_or(&dt_typ);
                let first = after.split(',').next().unwrap_or("");
                let first = first.split(' ').next().unwrap_or("");
                unit = Some(first.trim_end_matches(']').to_string());
            } else if are_dtypes_equal(&dt_typ, "int") {
                unit = Some(if dt_typ.contains("32") {
                    "second".to_string()
                } else {
                    DEFAULT_PRECISION_UNIT.to_string()
                });
            } else if are_dtypes_equal(&dt_typ, "date") {
                unit = Some("day".to_string());
            }
        }
        let unit = unit.unwrap_or_else(|| DEFAULT_PRECISION_UNIT.to_string());
        precision_unit_alias(&unit)
            .map(str::to_string)
            .unwrap_or(unit)
    }

    fn verify_param(&self, key: &str) -> Option<&PyValue> {
        self.param("verify").and_then(|v| v.get(key))
    }

    /// `Pipe.get_chunk_interval()` with no explicit argument (`core/Pipe/_data.py:678`).
    pub fn get_chunk_interval(&self) -> Interval {
        let dt_col = self.column("datetime");
        let is_int_axis = dt_col.is_some() && self.is_int_axis();
        if is_int_axis {
            if let Some(v) = self.verify_param("chunk_range") {
                if let Some(i) = py_value_to_i64(v) {
                    return Interval::Int(i);
                }
            }
        }
        let mut chunk_delta: Option<TimeDelta> = None;
        for suffix in ["minutes", "hours", "days", "weeks", "years", "seconds"] {
            let Some(v) = self.verify_param(&format!("chunk_{suffix}")) else {
                continue;
            };
            if v.is_none() {
                continue;
            }
            let val = py_value_to_f64(v).unwrap_or(0.0);
            let secs = match suffix {
                "minutes" => val * 60.0,
                "hours" => val * 3600.0,
                "days" => val * 86400.0,
                "weeks" => val * 604800.0,
                "years" => val * 365.0 * 86400.0,
                _ => val,
            };
            chunk_delta = Some(TimeDelta::microseconds((secs * 1e6).round() as i64));
            break;
        }
        let chunk_delta = chunk_delta.unwrap_or(TimeDelta::minutes(DEFAULT_CHUNK_MINUTES));
        if dt_col.is_none() {
            return Interval::Duration(chunk_delta);
        }
        if is_int_axis {
            return Interval::Int(self.scale_delta_to_int_axis(chunk_delta));
        }
        Interval::Duration(chunk_delta)
    }

    /// `Pipe.get_chunk_interval(chunk_interval)` with an explicit argument.
    pub fn get_chunk_interval_with(&self, chunk_interval: Interval) -> Interval {
        let is_int_axis = self.column("datetime").is_some() && self.is_int_axis();
        if is_int_axis {
            return match chunk_interval {
                Interval::Int(i) => Interval::Int(i),
                Interval::Duration(d) => Interval::Int(self.scale_delta_to_int_axis(d)),
            };
        }
        Interval::minutes(chunk_interval.whole_minutes())
    }

    /// `Pipe._scale_delta_to_int_axis`.
    pub fn scale_delta_to_int_axis(&self, delta: TimeDelta) -> i64 {
        let legacy = || ((total_seconds(delta) / 60.0) as i64).max(1);
        let has_precision = self.param("precision").is_some_and(PyValue::is_truthy);
        if !has_precision {
            return legacy();
        }
        match precision_unit_scalar(&self.precision_unit()) {
            Some(scalar) => ((total_seconds(delta) * scalar.as_f64()) as i64).max(1),
            None => legacy(),
        }
    }

    /// `Pipe.get_backtrack_interval(check_existing)` (`core/Pipe/_fetch.py:100`).
    pub fn get_backtrack_interval(&self, check_existing: bool) -> Interval {
        let configured = self
            .param("fetch")
            .and_then(|f| f.get("backtrack_minutes"))
            .and_then(py_value_to_i64);
        let backtrack_minutes = if check_existing {
            configured.unwrap_or(DEFAULT_BACKTRACK_MINUTES)
        } else {
            0
        };
        if self.column("datetime").is_none() {
            return Interval::minutes(backtrack_minutes);
        }
        if self.is_int_axis() {
            if !self.param("precision").is_some_and(PyValue::is_truthy) {
                return Interval::Int(backtrack_minutes);
            }
            return match precision_unit_scalar(&self.precision_unit()) {
                Some(scalar) => {
                    Interval::Int((backtrack_minutes as f64 * 60.0 * scalar.as_f64()) as i64)
                }
                None => Interval::Int(backtrack_minutes),
            };
        }
        Interval::minutes(backtrack_minutes)
    }

    /// `Pipe.guess_datetime()` over the configured dtypes.
    ///
    /// PARITY: Python's `if _dtypes['datetime'] is None` guard is dead code because
    /// `pipe.dtypes` already drops `None` values (`_attributes.py:1148`).
    pub fn guess_datetime(&self) -> Option<String> {
        self.dtypes()
            .into_iter()
            .find(|(_, typ)| are_dtypes_equal(typ, "datetime"))
            .map(|(col, _)| col)
    }
}

fn py_value_to_i64(v: &PyValue) -> Option<i64> {
    match v {
        PyValue::Int(i) => Some(*i),
        PyValue::Float(f) => Some(*f as i64),
        PyValue::Bool(b) => Some(*b as i64),
        PyValue::Str(s) => crate::dtypes::py_int(s),
        _ => None,
    }
}

fn py_value_to_f64(v: &PyValue) -> Option<f64> {
    match v {
        PyValue::Int(i) => Some(*i as f64),
        PyValue::Float(f) => Some(*f),
        PyValue::Bool(b) => Some(*b as i64 as f64),
        PyValue::Str(s) => crate::dtypes::py_float(s),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipe(params: &str) -> PipeSpec {
        let mut p = PipeSpec::new("plugin:x", "m", None);
        p.parameters = serde_json::from_str(params).unwrap();
        p
    }

    #[test]
    fn indices_order() {
        let p =
            pipe(r#"{"columns": {"id": "id", "datetime": "ts"}, "indices": {"v": ["a", "b"]}}"#);
        let ix = p.indices();
        assert_eq!(
            ix.keys().collect::<Vec<_>>(),
            vec!["unique", "id", "datetime", "v"]
        );
        assert_eq!(ix["unique"].joined(), "ts_id");
    }

    #[test]
    fn chunk_interval() {
        let p = pipe(r#"{"columns": {"datetime": "ts"}}"#);
        assert_eq!(p.get_chunk_interval(), Interval::minutes(43200));
        let p = pipe(r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}}"#);
        assert_eq!(p.get_chunk_interval(), Interval::Int(43200));
        let p = pipe(
            r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}, "precision": "s", "verify": {"chunk_days": 1}}"#,
        );
        assert_eq!(p.get_chunk_interval(), Interval::Int(86400));
    }
}
