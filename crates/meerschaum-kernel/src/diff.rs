//! `filter_unseen_df` (`meerschaum/utils/dataframe.py`) and the DataFrame part of
//! `Pipe.filter_existing` (`meerschaum/core/Pipe/_sync.py`), including `_get_delta_bounds`.
//!
//! `filter_unseen_df` is a left anti-join of the new frame against the old one on *every*
//! column, after reconciling dtypes and serializing special values (JSON with sorted keys,
//! numerics as `f"{x:f}"` strings) so they compare as v4 compares them. `filter_existing` then
//! splits the delta into `unseen` (index not in the backtrack frame) and `update` (index present)
//! with a left merge on the index columns.
//!
//! Partial rows: `delta`/`update` keep only the columns the incoming frame had, so a partial-row
//! sync updates those columns without nulling the others (design §4.4).

use std::collections::{HashMap, HashSet};

use chrono::Duration;
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use regex::Regex;

use crate::cast::{
    RoundTo, attempt_cast_to_bytes, attempt_cast_to_numeric, attempt_cast_to_uuid, none_if_null,
    round_time,
};
use crate::detect::{
    get_bytes_cols, get_geometry_cols, get_json_cols, get_numeric_cols, get_unhashable_cols,
    get_uuid_cols,
};
use crate::dtypes::{
    are_dtypes_equal, get_numeric_precision_scale, polars_dtype_for_pandas, to_pandas_dtype,
};
use crate::enforce::{
    EnforceError, PipeDtypesContext, PipeInput, apply_result, pandas_astype, pipe_enforce_dtypes,
    series_attempt_cast_to_geometry, series_coerce_timezone,
};
use crate::frame::{ColumnData, Frame, json_to_value};
use crate::serialize::compact_json_sorted;
use crate::value::Value;
use meerschaum_core::pyfmt::float_repr;

/// A target for the per-column casts in `filter_unseen_df`: a pandas dtype string or
/// `attempt_cast_to_numeric` (the mixed int/float rule).
#[derive(Clone, Debug, PartialEq)]
enum CastSpec {
    Pandas(String),
    Numeric,
}

impl CastSpec {
    /// `str(dtype)` as v4 sees it (a function's repr never starts with `datetime64`).
    fn as_str(&self) -> &str {
        match self {
            CastSpec::Pandas(s) => s,
            CastSpec::Numeric => "<function attempt_cast_to_numeric>",
        }
    }
}

/// `add_missing_cols_to_df(df, dtypes)`: append null columns for dtypes missing from `df`.
pub fn add_missing_cols(df: &Frame, dtypes: &IndexMap<String, String>) -> Frame {
    let df_cols: HashSet<String> = df.names().into_iter().collect();
    let dtype_cols: HashSet<String> = dtypes.keys().cloned().collect();
    if df_cols == dtype_cols {
        return df.clone();
    }
    let mut out = df.clone();
    let n = df.height();
    for (col, typ) in dtypes {
        if df_cols.contains(col) {
            continue;
        }
        let pd_type = to_pandas_dtype(typ);
        let data = match polars_dtype_for_pandas(&pd_type) {
            Some(dtype) => ColumnData::Native(polars::prelude::Series::full_null(
                col.as_str().into(),
                n,
                &dtype,
            )),
            None => ColumnData::Object(vec![Value::NA; n]),
        };
        let _ = out.set(col, data);
    }
    out
}

/// `json.dumps(x, sort_keys=True, separators=(',', ':'), default=str)`.
fn json_dumps_sorted(v: &Value) -> String {
    match v {
        Value::Json(j) => compact_json_sorted(j),
        Value::Null => "null".into(),
        Value::Bool(b) => if *b { "true" } else { "false" }.into(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) if f.is_nan() => "NaN".into(),
        Value::Float(f) if f.is_infinite() => {
            if *f > 0.0 { "Infinity" } else { "-Infinity" }.into()
        }
        Value::Float(f) => float_repr(*f),
        Value::Str(s) => compact_json_sorted(&serde_json::Value::String(s.clone())),
        other => compact_json_sorted(&serde_json::Value::String(other.py_str())),
    }
}

static NA_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^(none|nan|na|nat|natz|<na>)$").expect("valid regex"));

/// `normalize_nulls()`: null-like strings in string/object columns become `NA`.
fn normalize_nulls(df: &Frame) -> Frame {
    let mut out = df.clone();
    for col in df.columns() {
        let is_stringish = matches!(&col.data, ColumnData::Object(_))
            || matches!(
                col.data.polars_dtype(),
                Some(polars::prelude::DataType::String)
            );
        if !is_stringish {
            continue;
        }
        let values: Vec<Value> = col
            .data
            .values()
            .into_iter()
            .map(|v| match &v {
                Value::Str(s) if NA_PATTERN.is_match(s) => Value::NA,
                _ => v,
            })
            .collect();
        let _ = out.set(&col.name, ColumnData::Object(values));
    }
    out
}

fn row_keys(df: &Frame, cols: &[String]) -> Vec<Vec<Value>> {
    let datas: Vec<Vec<Value>> = cols
        .iter()
        .map(|c| df.data(c).map(|d| d.values()).unwrap_or_default())
        .collect();
    (0..df.height())
        .map(|i| datas.iter().map(|d| d[i].clone()).collect())
        .collect()
}

/// Options for [`filter_unseen_df`].
#[derive(Clone, Debug)]
pub struct FilterUnseenOptions {
    /// The frames' dtypes (`None`: use the old frame's dtypes).
    pub dtypes: Option<IndexMap<String, String>>,
    /// Cast mixed int/float columns to `numeric` (`Decimal`).
    pub coerce_mixed_numerics: bool,
}

impl Default for FilterUnseenOptions {
    fn default() -> Self {
        Self {
            dtypes: None,
            coerce_mixed_numerics: true,
        }
    }
}

fn cast_column(data: &ColumnData, spec: &CastSpec, name: &str) -> Result<ColumnData, String> {
    match spec {
        CastSpec::Pandas(t) => pandas_astype(name, data, t),
        CastSpec::Numeric => Ok(ColumnData::Object(
            data.values()
                .iter()
                .map(|v| {
                    attempt_cast_to_numeric(v, false, None, None).unwrap_or_else(|_| v.clone())
                })
                .collect(),
        )),
    }
}

/// `filter_unseen_df(old_df, new_df, dtypes=..., coerce_mixed_numerics=...)`: the rows of `new`
/// not present in `old`.
pub fn filter_unseen_df(old: Option<&Frame>, new: &Frame, opts: &FilterUnseenOptions) -> Frame {
    let Some(old) = old else {
        return new.clone();
    };
    let mut old = old.clone();
    let mut new = new.clone();
    let mut new_dtypes = new.dtypes();
    let new_cols = new.names();
    let mut old_dtypes = old.dtypes();

    let new_set: HashSet<String> = new_dtypes.keys().cloned().collect();
    let old_set: HashSet<String> = old_dtypes.keys().cloned().collect();
    if new_set != old_set {
        new = add_missing_cols(&new, &old_dtypes);
        old = add_missing_cols(&old, &new_dtypes);
        let new_missing: Vec<(String, String)> = new_dtypes
            .iter()
            .filter(|(c, _)| !old_dtypes.contains_key(*c))
            .map(|(c, t)| (c.clone(), t.clone()))
            .collect();
        let old_missing: Vec<(String, String)> = old_dtypes
            .iter()
            .filter(|(c, _)| !new_dtypes.contains_key(*c))
            .map(|(c, t)| (c.clone(), t.clone()))
            .collect();
        old_dtypes.extend(new_missing);
        new_dtypes.extend(old_missing);
    } else if new.width() == 0 {
        return new;
    }

    // `new_df = new_df[old_df.columns]` (order matters when checking equality).
    let Ok(reordered) = new.select(&old.names()) else {
        return new
            .select(&new_dtypes.keys().cloned().collect::<Vec<_>>())
            .unwrap_or(new);
    };
    new = reordered;

    let given = opts.dtypes.clone().unwrap_or_else(|| old.dtypes());
    let numeric_ps: HashMap<String, (Option<i64>, Option<i64>)> = given
        .iter()
        .filter(|(c, t)| !c.is_empty() && t.to_lowercase().starts_with("numeric"))
        .map(|(c, t)| (c.clone(), get_numeric_precision_scale(Some(t))))
        .collect();
    let mut dtypes: IndexMap<String, CastSpec> = given
        .iter()
        .filter(|(c, _)| new_dtypes.contains_key(*c) && old_dtypes.contains_key(*c))
        .map(|(c, t)| (c.clone(), CastSpec::Pandas(to_pandas_dtype(t))))
        .collect();
    for (c, t) in &new_dtypes {
        dtypes
            .entry(c.clone())
            .or_insert_with(|| CastSpec::Pandas(t.clone()));
    }
    let dt_cols: Vec<String> = dtypes
        .iter()
        .filter(|(_, s)| are_dtypes_equal(s.as_str(), "datetime"))
        .map(|(c, _)| c.clone())
        .collect();

    // `new_df.astype(non_dt_dtypes)` is all-or-nothing.
    let mut cast_non_dt = true;
    {
        let mut casted = new.clone();
        let mut ok = true;
        for (col, spec) in &dtypes {
            if dt_cols.contains(col) {
                continue;
            }
            let Ok(data) = new.data(col) else { continue };
            match cast_column(data, spec, col) {
                Ok(d) => {
                    let _ = casted.set(col, d);
                }
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if ok {
            new = casted;
            cast_non_dt = false;
        }
    }

    let strip_for = |dtypes: &IndexMap<String, CastSpec>, col: &str| -> bool {
        let s = dtypes.get(col).map(CastSpec::as_str).unwrap_or("datetime");
        s.starts_with("datetime64") && !s.to_lowercase().contains("utc")
    };
    let mut cast_dt = true;
    let dt_result: Result<(), ()> = (|| {
        for col in &dt_cols {
            let strip = strip_for(&dtypes, col);
            let spec = dtypes[col].clone();
            for frame in [&mut old, &mut new] {
                if let Ok(data) = frame.data(col) {
                    let coerced = series_coerce_timezone(data, strip).map_err(|_| ())?;
                    let casted = cast_column(&coerced, &spec, col).map_err(|_| ())?;
                    frame.set(col, casted).map_err(|_| ())?;
                }
            }
        }
        Ok(())
    })();
    if dt_result.is_ok() {
        cast_dt = false;
    }
    let mut cast_cols = cast_dt || cast_non_dt;

    let new_numeric_existing = get_numeric_cols(&new);
    let old_numeric = get_numeric_cols(&old);
    let none = "None".to_string();
    for col in dtypes.keys().cloned().collect::<Vec<_>>() {
        let nt = new_dtypes.get(&col).unwrap_or(&none);
        let ot = old_dtypes.get(&col).unwrap_or(&none);
        if are_dtypes_equal(nt, ot) {
            continue;
        }
        let new_numericish = are_dtypes_equal(nt, "float")
            || are_dtypes_equal(nt, "int")
            || new_numeric_existing.contains(&col);
        let old_numericish = are_dtypes_equal(ot, "float")
            || are_dtypes_equal(ot, "int")
            || old_numeric.contains(&col);
        if opts.coerce_mixed_numerics && new_numericish && old_numericish {
            dtypes.insert(col, CastSpec::Numeric);
        } else {
            // PARITY: "Detected different types ... falling back to 'object'".
            dtypes.insert(col, CastSpec::Pandas("object".into()));
        }
        cast_cols = true;
    }

    if cast_cols {
        for (col, spec) in &dtypes {
            for frame in [&mut new, &mut old] {
                if let Ok(data) = frame.data(col) {
                    if let Ok(d) = cast_column(data, spec, col) {
                        let _ = frame.set(col, d);
                    }
                }
            }
        }
    }

    // Serialize JSON (sorted keys) and numerics (`f"{x:f}"`) so they compare as strings.
    let new_json = get_json_cols(&new);
    let old_json = get_json_cols(&old);
    let json_cols: HashSet<String> = new_json.iter().chain(old_json.iter()).cloned().collect();
    for (frame, cols) in [(&mut old, &old_json), (&mut new, &new_json)] {
        for col in cols {
            let values: Vec<Value> = frame
                .data(col)
                .map(|d| d.values())
                .unwrap_or_default()
                .iter()
                .map(|v| Value::Str(json_dumps_sorted(v)))
                .collect();
            let _ = frame.set(col, ColumnData::Object(values));
        }
    }
    let new_numeric = get_numeric_cols(&new);
    let numeric_cols: HashSet<String> = new_numeric
        .iter()
        .chain(old_numeric.iter())
        .cloned()
        .collect();
    for (frame, cols) in [(&mut old, &old_numeric), (&mut new, &new_numeric)] {
        for col in cols {
            let values: Vec<Value> = frame
                .data(col)
                .map(|d| d.values())
                .unwrap_or_default()
                .into_iter()
                .map(|v| match &v {
                    Value::Decimal(d) if d.is_nan() => Value::Null,
                    Value::Decimal(d) => Value::Str(d.to_plain_string()),
                    _ => v,
                })
                .collect();
            let _ = frame.set(col, ColumnData::Object(values));
        }
    }
    for frame in [&mut old, &mut new] {
        let dt_frame_cols: Vec<String> = frame
            .columns()
            .iter()
            .filter(|c| are_dtypes_equal(&c.data.dtype_str(), "datetime"))
            .map(|c| c.name.clone())
            .collect();
        for col in dt_frame_cols {
            let strip = strip_for(&dtypes, &col);
            if let Ok(data) = frame.data(&col) {
                if let Ok(d) = series_coerce_timezone(data, strip) {
                    let _ = frame.set(&col, d);
                }
            }
        }
    }

    let uuid_cols: HashSet<String> = get_uuid_cols(&new)
        .into_iter()
        .chain(get_uuid_cols(&old))
        .collect();
    let bytes_cols: HashSet<String> = get_bytes_cols(&new)
        .into_iter()
        .chain(get_bytes_cols(&old))
        .collect();
    let geometry_cols: HashSet<String> = get_geometry_cols(&new)
        .into_iter()
        .chain(get_geometry_cols(&old))
        .collect();

    let normalized_new = normalize_nulls(&new);
    let normalized_old = normalize_nulls(&old);
    let all_cols = normalized_new.names();
    let old_keys: HashSet<Vec<Value>> = row_keys(&normalized_old, &all_cols).into_iter().collect();
    let keep: Vec<usize> = row_keys(&normalized_new, &all_cols)
        .into_iter()
        .enumerate()
        .filter(|(_, k)| !old_keys.contains(k))
        .map(|(i, _)| i)
        .collect();
    let mut delta = normalized_new
        .take(&keep)
        .select(&new_cols)
        .unwrap_or_else(|_| normalized_new.take(&keep));

    // Deserialize the special columns in the delta.
    let delta_json = get_json_cols(&delta);
    for col in &json_cols {
        if delta_json.contains(col) || !delta.contains(col) {
            continue;
        }
        let values = delta.data(col).map(|d| d.values()).unwrap_or_default();
        let parsed: Result<Vec<Value>, ()> = values
            .into_iter()
            .map(|v| match &v {
                Value::Str(s) => serde_json::from_str(s).map(json_to_value).map_err(|_| ()),
                _ => Ok(v),
            })
            .collect();
        if let Ok(values) = parsed {
            let _ = delta.set(col, ColumnData::Object(values));
        }
    }
    let delta_numeric = get_numeric_cols(&delta);
    for col in &numeric_cols {
        if delta_numeric.contains(col) || !delta.contains(col) {
            continue;
        }
        let (p, s) = numeric_ps.get(col).cloned().unwrap_or((None, None));
        let values = delta.data(col).map(|d| d.values()).unwrap_or_default();
        let casted: Result<Vec<Value>, String> = values
            .iter()
            .map(|v| attempt_cast_to_numeric(v, true, p, s))
            .collect();
        if let Ok(values) = casted {
            let _ = delta.set(col, ColumnData::Object(values));
        }
    }
    let delta_uuid = get_uuid_cols(&delta);
    for col in &uuid_cols {
        if delta_uuid.contains(col) || !delta.contains(col) {
            continue;
        }
        let values: Vec<Value> = delta
            .data(col)
            .map(|d| d.values())
            .unwrap_or_default()
            .iter()
            .map(attempt_cast_to_uuid)
            .collect();
        let _ = delta.set(col, ColumnData::Object(values));
    }
    let delta_bytes = get_bytes_cols(&delta);
    for col in &bytes_cols {
        if delta_bytes.contains(col) || !delta.contains(col) {
            continue;
        }
        let values: Vec<Value> = delta
            .data(col)
            .map(|d| d.values())
            .unwrap_or_default()
            .iter()
            .map(attempt_cast_to_bytes)
            .collect();
        let _ = delta.set(col, ColumnData::Object(values));
    }
    let delta_geometry = get_geometry_cols(&delta);
    for col in &geometry_cols {
        if delta_geometry.contains(col) || !delta.contains(col) {
            continue;
        }
        if let Some(column) = delta.column(col).cloned() {
            if let Ok(c) = series_attempt_cast_to_geometry(&column) {
                let _ = delta.set_column(c);
            }
        }
    }
    delta
}

// ---------------------------------------------------------------------------------------------
// filter_existing

/// The `get_data()` request `filter_existing` issues for the backtrack frame.
#[derive(Clone, Debug, PartialEq)]
pub struct BacktrackRequest {
    /// `begin` (a datetime, an int, or `Null`).
    pub begin: Value,
    /// `end` (a datetime, an int, or `Null`).
    pub end: Value,
    /// `params`: unique index values per index column (`none_if_null` applied).
    pub params: IndexMap<String, Vec<Value>>,
}

/// The pipe attributes `filter_existing` reads.
#[derive(Clone, Debug)]
pub struct FilterExistingParams {
    /// `pipe.columns` (role → column).
    pub columns: IndexMap<String, String>,
    /// `parameters['dtypes']` (configured dtypes; the datetime axis type comes from here).
    pub configured_dtypes: IndexMap<String, String>,
    /// `Pipe.get_dtypes()` and friends, for enforcement.
    pub dtypes_ctx: PipeDtypesContext,
    pub autoincrement: bool,
    pub autotime: bool,
    /// `Pipe.static` (disables the mixed int/float → numeric rule).
    pub static_: bool,
    pub include_unchanged_columns: bool,
    pub date_bound_only: bool,
    /// Run `Pipe.enforce_dtypes` on the input, backtrack, delta and update frames.
    pub enforce_dtypes: bool,
    /// `pipes:sync:filter_params_index_limit` (default 250).
    pub filter_params_index_limit: usize,
}

/// `(unseen, update, delta)`.
pub type FilterExistingResult = (Frame, Frame, Frame);

/// Min / max of a column's non-null values (datetimes and ints compare natively).
fn column_bound(data: &ColumnData, minimum: bool) -> Value {
    let mut best: Option<Value> = None;
    for v in data.values() {
        if v.is_na() {
            continue;
        }
        best = Some(match best {
            None => v,
            Some(b) => {
                let ord = match (&b, &v) {
                    (Value::Datetime(x), Value::Datetime(y)) => x.utc.cmp(&y.utc),
                    (Value::Int(x), Value::Int(y)) => x.cmp(y),
                    (Value::Float(x), Value::Float(y)) => {
                        x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    (Value::Date(x), Value::Date(y)) => x.cmp(y),
                    (Value::Str(x), Value::Str(y)) => x.cmp(y),
                    _ => std::cmp::Ordering::Equal,
                };
                let take_new = if minimum { ord.is_gt() } else { ord.is_lt() };
                if take_new { v } else { b }
            }
        });
    }
    best.unwrap_or(Value::Null)
}

/// `_get_delta_bounds(min_dt_val, max_dt_val, dt_type, dt_col)`.
pub fn get_delta_bounds(
    min_val: &Value,
    max_val: &Value,
    dt_type: Option<&str>,
    dt_col: Option<&str>,
) -> (Value, Value) {
    let is_dt_type = dt_type.is_some_and(|t| are_dtypes_equal(t, "datetime"));
    let coerce = |v: &Value| -> Value {
        if v.is_na() {
            return Value::Null;
        }
        if is_dt_type {
            return match crate::cast::to_datetime(v, true, None) {
                Ok(Value::Datetime(d)) => Value::Datetime(d),
                Ok(Value::Int(i)) => Value::Int(i),
                Ok(_) => Value::Null,
                Err(_) => {
                    if let Value::Int(i) = v {
                        // `pd.to_datetime(int, utc=True)` reads nanoseconds since the epoch.
                        return crate::value::DateTimeValue::from_timestamp(
                            *i,
                            polars::prelude::TimeUnit::Nanoseconds,
                            Some(crate::value::TzInfo::Utc),
                        )
                        .map(Value::Datetime)
                        .unwrap_or(Value::Null);
                    }
                    Value::Null
                }
            };
        }
        v.clone()
    };
    let keep = |v: Value| -> Value {
        match v {
            Value::Datetime(_) | Value::Int(_) => v,
            _ => Value::Null,
        }
    };
    let min_dt = keep(coerce(min_val));
    let max_dt = keep(coerce(max_val));
    let int_axis = dt_type.is_some_and(|t| t.to_lowercase().contains("int"));
    let minute = 60_000_000;
    let mut begin = match &min_dt {
        Value::Datetime(d) => {
            let rounded = round_time(d, minute, RoundTo::Down);
            let mut b = rounded.clone();
            b.utc = rounded
                .utc
                .checked_sub_signed(Duration::minutes(1))
                .unwrap_or(rounded.utc);
            Value::Datetime(b)
        }
        _ if int_axis => min_dt.clone(),
        _ => Value::Null,
    };
    let _ = dt_col;
    let end = match &max_dt {
        Value::Datetime(d) => {
            let mut e = round_time(d, minute, RoundTo::Down);
            e.utc += Duration::minutes(1);
            Value::Datetime(e)
        }
        Value::Int(i) if int_axis => Value::Int(i + 1),
        _ => Value::Null,
    };
    match (&begin, &end) {
        (Value::Datetime(b), Value::Datetime(e)) if b.utc > e.utc => {
            let mut nb = e.clone();
            nb.utc -= Duration::minutes(1);
            begin = Value::Datetime(nb);
        }
        (Value::Int(b), Value::Int(e)) if b > e => begin = Value::Int(e - 1),
        _ => {}
    }
    (begin, end)
}

fn unique_in_order(values: Vec<Value>) -> Vec<Value> {
    let mut seen: HashSet<Value> = HashSet::new();
    let mut out = Vec::new();
    for v in values {
        if seen.insert(v.clone()) {
            out.push(v);
        }
    }
    out
}

/// `get_empty_df()`: an empty frame with the incoming columns plus the pipe's dtypes.
fn get_empty_df(df: Option<&Frame>, params: &FilterExistingParams) -> Frame {
    let mut dtypes: IndexMap<String, String> = df.map(|d| d.dtypes()).unwrap_or_default();
    if params.dtypes_ctx.enforce {
        for (c, t) in &params.dtypes_ctx.dtypes {
            dtypes.insert(c.clone(), t.clone());
        }
    }
    let pd: IndexMap<String, String> = dtypes
        .iter()
        .map(|(c, t)| (c.clone(), to_pandas_dtype(t)))
        .collect();
    add_missing_cols(&Frame::empty(), &pd)
}

/// Left merge of `left` with `right` on `on` (NA keys match), returning for each output row the
/// left row index and whether it matched (`both`) — one row per matching right row.
fn left_merge_indices(left: &Frame, right: &Frame, on: &[String]) -> Vec<(usize, Option<usize>)> {
    let right_keys = row_keys(right, on);
    let mut index: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
    for (i, k) in right_keys.into_iter().enumerate() {
        index.entry(k).or_default().push(i);
    }
    let mut out = Vec::new();
    for (i, k) in row_keys(left, on).into_iter().enumerate() {
        match index.get(&k) {
            Some(matches) => out.extend(matches.iter().map(|m| (i, Some(*m)))),
            None => out.push((i, None)),
        }
    }
    out
}

/// `Pipe.filter_existing(df, ...)` with the instance access factored out: `get_backtrack`
/// receives the `get_data()` request and returns the backtrack frame (or `None`).
pub fn filter_existing(
    input: PipeInput,
    params: &FilterExistingParams,
    get_backtrack: impl FnOnce(&BacktrackRequest) -> Option<Frame>,
) -> Result<FilterExistingResult, EnforceError> {
    let enforce = |f: Frame| pipe_enforce_dtypes(PipeInput::Frame(f), &params.dtypes_ctx);
    let mut df = match input {
        PipeInput::Frame(f) if !params.enforce_dtypes => f,
        other => pipe_enforce_dtypes(other, &params.dtypes_ctx)?,
    };
    let columns = params.columns.clone();
    let primary = columns.get("primary").filter(|c| !c.is_empty()).cloned();
    let dt_col = columns.get("datetime").filter(|c| !c.is_empty()).cloned();
    let dt_type = dt_col.as_ref().map(|c| {
        params
            .configured_dtypes
            .get(c)
            .cloned()
            .unwrap_or_else(|| "datetime".into())
    });

    // PARITY: v4 drops an all-null autoincrement / autotime column and then pops the column
    // *name* from the role-keyed `columns` dict, which is a no-op
    // (meerschaum/core/Pipe/_sync.py, `filter_existing`).
    if let Some(pk) = &primary {
        if params.autoincrement && df.contains(pk) && df.data(pk)?.is_null_mask().iter().all(|n| *n)
        {
            df.drop(pk);
        }
    }
    if let Some(dt) = &dt_col {
        if params.autotime && df.contains(dt) && df.data(dt)?.is_null_mask().iter().all(|n| *n) {
            df.drop(dt);
        }
    }

    if df.is_empty() {
        return Ok((df.clone(), df.clone(), df));
    }
    if params.autoincrement
        && primary.is_some()
        && primary == dt_col
        && !dt_col.as_ref().is_some_and(|c| df.contains(c))
    {
        let empty = get_empty_df(Some(&df), params);
        let df = if params.enforce_dtypes {
            enforce(df)?
        } else {
            df
        };
        return Ok((df.clone(), empty, df));
    }
    if params.autotime && dt_col.as_ref().is_some_and(|c| !df.contains(c)) {
        let empty = get_empty_df(Some(&df), params);
        let df = if params.enforce_dtypes {
            enforce(df)?
        } else {
            df
        };
        return Ok((df.clone(), empty, df));
    }

    let (min_val, max_val) = match &dt_col {
        Some(c) if df.contains(c) => {
            let data = df.data(c)?;
            (column_bound(data, true), column_bound(data, false))
        }
        _ => (Value::Null, Value::Null),
    };
    let (begin, end) = get_delta_bounds(&min_val, &max_val, dt_type.as_deref(), dt_col.as_deref());

    let mut req_params: IndexMap<String, Vec<Value>> = IndexMap::new();
    if !params.date_bound_only {
        let index_cols: Vec<String> = match &primary {
            Some(pk) => vec![pk.clone()],
            None => columns.values().cloned().collect(),
        };
        for col in index_cols {
            if !df.contains(&col) || Some(&col) == dt_col.as_ref() || req_params.contains_key(&col)
            {
                continue;
            }
            let uniques = unique_in_order(df.data(&col)?.values());
            if uniques.len() <= params.filter_params_index_limit {
                req_params.insert(col, uniques.iter().map(none_if_null).collect());
            }
        }
    }
    let request = BacktrackRequest {
        begin,
        end,
        params: req_params,
    };
    let Some(mut backtrack) = get_backtrack(&request) else {
        let empty = get_empty_df(Some(&df), params);
        return Ok((df.clone(), empty, df));
    };
    if params.enforce_dtypes {
        backtrack = enforce(backtrack)?;
    }

    let on_cols: Vec<String> = match &primary {
        Some(pk) => vec![pk.clone()],
        None => columns
            .iter()
            .filter(|(role, col)| {
                !col.is_empty() && role.as_str() != "value" && backtrack.contains(col)
            })
            .map(|(_, col)| col.clone())
            .collect(),
    };
    let self_dtypes: IndexMap<String, String> = if params.dtypes_ctx.enforce {
        params.dtypes_ctx.dtypes.clone()
    } else {
        IndexMap::new()
    };
    let on_cols_dtypes: IndexMap<String, String> = self_dtypes
        .iter()
        .filter(|(c, _)| on_cols.contains(c))
        .map(|(c, t)| (c.clone(), to_pandas_dtype(t)))
        .collect();
    let unseen_dtypes: IndexMap<String, String> = self_dtypes
        .iter()
        .map(|(c, t)| (c.clone(), to_pandas_dtype(t)))
        .collect();
    let mut delta = add_missing_cols(
        &filter_unseen_df(
            Some(&backtrack),
            &df,
            &FilterUnseenOptions {
                dtypes: Some(unseen_dtypes),
                coerce_mixed_numerics: !params.static_,
            },
        ),
        &on_cols_dtypes,
    );
    if params.enforce_dtypes {
        delta = enforce(delta)?;
    }
    for (col, typ) in &on_cols_dtypes {
        for frame in [&mut delta, &mut backtrack] {
            let data = frame.data(col)?.clone();
            let casted = pandas_astype(col, &data, typ).map_err(EnforceError::Raised)?;
            frame.set(col, casted)?;
        }
    }

    // Serialize unhashable (dict / list) columns so they can be merged.
    let unhashable_delta = get_unhashable_cols(&delta);
    let unhashable_backtrack = get_unhashable_cols(&backtrack);
    for (frame, cols) in [
        (&mut delta, &unhashable_delta),
        (&mut backtrack, &unhashable_backtrack),
    ] {
        for col in cols {
            let values: Vec<Value> = frame
                .data(col)?
                .values()
                .iter()
                .map(|v| Value::Str(json_dumps_sorted(v)))
                .collect();
            frame.set(col, ColumnData::Object(values))?;
        }
    }
    let casted_cols: Vec<String> = unhashable_delta
        .iter()
        .chain(unhashable_backtrack.iter())
        .cloned()
        .map(|c| (c, ()))
        .collect::<IndexMap<String, ()>>()
        .into_keys()
        .collect();
    let deserialize = |frame: &mut Frame| -> Result<(), EnforceError> {
        for col in &casted_cols {
            if let Ok(data) = frame.data(col) {
                let values: Vec<Value> = data
                    .values()
                    .into_iter()
                    .map(|v| match &v {
                        Value::Str(s) => serde_json::from_str(s).map(json_to_value).unwrap_or(v),
                        _ => v,
                    })
                    .collect();
                frame.set(col, apply_result(col, values))?;
            }
        }
        Ok(())
    };

    let cols = delta.names();
    let (unseen, mut update) = if on_cols.is_empty() {
        deserialize(&mut delta)?;
        (delta.clone(), get_empty_df(Some(&df), params))
    } else {
        let merged = left_merge_indices(&delta, &backtrack, &on_cols);
        deserialize(&mut delta)?;
        let unseen_ix: Vec<usize> = merged
            .iter()
            .filter(|(_, m)| m.is_none())
            .map(|(i, _)| *i)
            .collect();
        let update_ix: Vec<usize> = merged
            .iter()
            .filter(|(_, m)| m.is_some())
            .map(|(i, _)| *i)
            .collect();
        (
            delta.take(&unseen_ix).select(&cols)?,
            delta.take(&update_ix).select(&cols)?,
        )
    };

    if params.include_unchanged_columns && !on_cols.is_empty() {
        let update_names = update.names();
        let unchanged: Vec<String> = backtrack
            .names()
            .into_iter()
            .filter(|c| on_cols.contains(c) || !update_names.contains(c))
            .collect();
        if params.enforce_dtypes {
            update = enforce(update)?;
        }
        let left = backtrack.select(&unchanged)?;
        // Inner merge: left (backtrack) order, one row per matching update row.
        let right_keys = row_keys(&update, &on_cols);
        let mut index: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();
        for (i, k) in right_keys.into_iter().enumerate() {
            index.entry(k).or_default().push(i);
        }
        let mut left_ix = Vec::new();
        let mut right_ix = Vec::new();
        for (i, k) in row_keys(&left, &on_cols).into_iter().enumerate() {
            if let Some(ms) = index.get(&k) {
                for m in ms {
                    left_ix.push(i);
                    right_ix.push(*m);
                }
            }
        }
        let mut merged = left.take(&left_ix);
        let right = update.take(&right_ix);
        for c in right.columns() {
            if on_cols.contains(&c.name) {
                continue;
            }
            merged.set_column(c.clone())?;
        }
        update = merged;
    }

    Ok((unseen, update, delta))
}
