//! `query_df` (`meerschaum/utils/dataframe.py`): filter a frame with the `params` grammar and
//! `begin` / `end` bounds.
//!
//! `params` grammar (the same as `build_where` for SQL): a single value, a list (`IN`), a `_`
//! prefix for negation (`NOT IN`), mixed lists, `None` / `'None'` (and other null-like strings)
//! for nulls, and `'_None'`. Any negation switches the whole query to string comparison
//! (`coerce_types`), as v4 does.

use indexmap::IndexMap;
use polars::prelude::DataType;

use crate::cast::{coerce_timezone, value_is_null};
use crate::dtypes::are_dtypes_equal;
use crate::enforce::{EnforceError, EnforceOptions, enforce_dtypes_with_path};
use crate::frame::{ColumnData, Frame};
use crate::value::{DateTimeValue, Value, parse_datetime_value};

/// A `params` value: a scalar or a list (`list`, `tuple` or `set` in Python).
#[derive(Clone, Debug)]
pub enum ParamValue {
    One(Value),
    Many(Vec<Value>),
}

impl From<&meerschaum_core::Value> for ParamValue {
    /// A `params` entry as `build_where` receives it: a list is `IN`, anything else one value.
    fn from(v: &meerschaum_core::Value) -> Self {
        match v {
            meerschaum_core::Value::List(items) => {
                ParamValue::Many(items.iter().map(Value::from).collect())
            }
            other => ParamValue::One(Value::from(other)),
        }
    }
}

/// Convert a `params` map shared with the SQL `build_where` into `query_df` params.
pub fn params_from_core(params: &meerschaum_core::Map) -> IndexMap<String, ParamValue> {
    params
        .iter()
        .map(|(k, v)| (k.clone(), ParamValue::from(v)))
        .collect()
}

/// The keyword arguments of `query_df`.
#[derive(Clone, Debug, Default)]
pub struct QueryOptions {
    pub params: IndexMap<String, ParamValue>,
    /// Inclusive lower bound (a datetime, an int, or an ISO string).
    pub begin: Option<Value>,
    /// Exclusive upper bound.
    pub end: Option<Value>,
    pub datetime_column: Option<String>,
    pub select_columns: Option<Vec<String>>,
    pub omit_columns: Option<Vec<String>>,
    pub reset_index: bool,
    pub coerce_types: bool,
}

/// The filtered frame and the row labels pandas would show (`df.index`).
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub frame: Frame,
    /// Positions of the result rows in the input frame, or `0..n` when v4 resets the index
    /// (`reset_index=True`, or when dtype enforcement rebuilds the frame through Polars).
    pub index: Vec<usize>,
}

/// Python truthiness for the `begin` / `end` checks (`not begin`).
fn truthy(v: &Option<Value>) -> bool {
    match v {
        None => false,
        Some(Value::Null) => false,
        Some(Value::Int(0)) => false,
        Some(Value::Float(f)) => *f != 0.0,
        Some(Value::Str(s)) => !s.is_empty(),
        Some(Value::Bool(b)) => *b,
        Some(_) => true,
    }
}

/// `separate_negation_values()` for one param.
fn split_negation(vals: &[Value]) -> (Vec<Value>, Vec<Value>) {
    let mut inc = Vec::new();
    let mut exc = Vec::new();
    for v in vals {
        let s = v.py_str();
        match s.strip_prefix('_') {
            Some(rest) => exc.push(Value::Str(rest.to_string())),
            None => inc.push(v.clone()),
        }
    }
    (inc, exc)
}

fn as_list(p: &ParamValue) -> Vec<Value> {
    match p {
        ParamValue::One(v) => vec![v.clone()],
        ParamValue::Many(v) => v.clone(),
    }
}

/// `Series.astype(str)` for one cell (`None` for nulls, which pandas 3 keeps as `NaN`).
fn astype_str(v: &Value) -> Option<String> {
    if v.is_na() {
        return None;
    }
    Some(v.py_str())
}

fn select_omit(df: &mut Frame, select: Option<&Vec<String>>, omit: Option<&Vec<String>>) {
    if let Some(sel) = select.filter(|s| !s.is_empty()) {
        for col in df.names() {
            if !sel.contains(&col) {
                df.drop(&col);
            }
        }
    }
    if let Some(om) = omit.filter(|s| !s.is_empty()) {
        for col in df.names() {
            if om.contains(&col) {
                df.drop(&col);
            }
        }
    }
}

fn compare_bound(cell: &Value, bound: &Value) -> Result<Option<std::cmp::Ordering>, EnforceError> {
    if cell.is_na() {
        return Ok(None);
    }
    Ok(match (cell, bound) {
        (Value::Datetime(a), Value::Datetime(b)) => {
            if a.tz.is_some() != b.tz.is_some() {
                return Err(EnforceError::Raised(
                    "TypeError: Invalid comparison between tz-naive and tz-aware datetimes".into(),
                ));
            }
            Some(a.utc.cmp(&b.utc))
        }
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Date(a), Value::Date(b)) => Some(a.cmp(b)),
        (Value::Str(a), Value::Str(b)) => Some(a.cmp(b)),
        _ => {
            return Err(EnforceError::Raised(format!(
                "TypeError: Invalid comparison between {} and {}",
                cell.type_name(),
                bound.type_name()
            )));
        }
    })
}

/// `query_df(df, params, begin, end, datetime_column, select_columns, omit_columns,
/// reset_index=..., coerce_types=...)`.
pub fn query_df(df: &Frame, opts: &QueryOptions) -> Result<QueryResult, EnforceError> {
    if opts.params.is_empty() && !truthy(&opts.begin) && !truthy(&opts.end) {
        let mut out = df.clone();
        // PARITY: on this early path `select_columns=['*']` is not normalized, so it removes
        // every column (meerschaum/utils/dataframe.py, `query_df`).
        select_omit(
            &mut out,
            opts.select_columns.as_ref(),
            opts.omit_columns.as_ref(),
        );
        return Ok(QueryResult {
            index: (0..out.height()).collect(),
            frame: out,
        });
    }

    let mut coerce_types = opts.coerce_types;
    if opts
        .params
        .values()
        .any(|p| !split_negation(&as_list(p)).1.is_empty())
    {
        coerce_types = true;
    }
    let mut params: IndexMap<String, Vec<Value>> = IndexMap::new();
    for (key, val) in &opts.params {
        let vals = match val {
            ParamValue::Many(vals) => {
                let mut v: Vec<Value> = vals.clone();
                if v.iter().any(|x| matches!(x, Value::Null)) {
                    v.retain(|x| !matches!(x, Value::Null));
                    v.push(Value::NA);
                }
                v
            }
            ParamValue::One(v) => vec![if value_is_null(v) {
                Value::NA
            } else {
                v.clone()
            }],
        };
        let vals = if coerce_types {
            vals.iter().map(|v| Value::Str(v.py_str())).collect()
        } else {
            vals
        };
        params.insert(key.clone(), vals);
    }

    let dtypes = df.dtypes();
    let parse_bound = |b: &Option<Value>| -> Option<Value> {
        match b {
            Some(Value::Str(s)) => Some(
                parse_datetime_value(s)
                    .map(Value::Datetime)
                    .unwrap_or(Value::Str(s.clone())),
            ),
            Some(Value::Null) | None => None,
            Some(v) => Some(v.clone()),
        }
    };
    let mut begin = parse_bound(&opts.begin);
    let mut end = parse_bound(&opts.end);
    let dt_col = opts.datetime_column.clone().filter(|c| df.contains(c));
    if (begin.is_some() || end.is_some()) && dt_col.is_none() {
        begin = None;
        end = None;
    }
    if let Some(col) = &dt_col
        && (begin.is_some() || end.is_some())
    {
        let data = df.data(col)?;
        let df_is_dt = are_dtypes_equal(&data.dtype_str(), "datetime");
        if df_is_dt {
            let df_tz = match data.polars_dtype() {
                Some(DataType::Datetime(_, tz)) => tz.clone(),
                _ => None,
            };
            let from_int = |v: Option<Value>| -> Option<Value> {
                match v {
                    Some(Value::Int(i)) => chrono::DateTime::from_timestamp(i, 0)
                        .map(|d| Value::Datetime(DateTimeValue::naive(d.naive_utc()))),
                    other => other,
                }
            };
            begin = from_int(begin);
            end = from_int(end);
            let strip = df_tz.is_none();
            let had_begin = begin.is_some();
            begin = match begin {
                Some(b) => Some(
                    coerce_timezone(&b, strip).map_err(|e| EnforceError::Raised(e.to_string()))?,
                ),
                None => None,
            };
            // PARITY: `end` is coerced only `if begin is not None`, so an `end`-only bound on
            // a datetime column is dropped (meerschaum/utils/dataframe.py, `query_df`).
            end = if had_begin {
                match end {
                    Some(e) => Some(
                        coerce_timezone(&e, strip)
                            .map_err(|e| EnforceError::Raised(e.to_string()))?,
                    ),
                    None => None,
                }
            } else {
                None
            };
        }
    }

    let n = df.height();
    let mut mask: Option<Vec<bool>> = None;
    let mut and_mask = |m: Vec<bool>| {
        mask = Some(match mask.take() {
            None => m,
            Some(prev) => prev.iter().zip(m.iter()).map(|(a, b)| *a && *b).collect(),
        });
    };
    if let Some(col) = &dt_col {
        let values = df.data(col)?.values();
        if let Some(b) = &begin {
            let m: Result<Vec<bool>, EnforceError> = values
                .iter()
                .map(|v| Ok(compare_bound(v, b)?.is_some_and(|o| o.is_ge())))
                .collect();
            and_mask(m?);
        }
        if let Some(e) = &end {
            let m: Result<Vec<bool>, EnforceError> = values
                .iter()
                .map(|v| Ok(compare_bound(v, e)?.is_some_and(|o| o.is_lt())))
                .collect();
            and_mask(m?);
        }
    }
    for (col, vals) in &params {
        let Ok(data) = df.data(col) else { continue };
        let (in_vals, ex_vals) = split_negation(vals);
        let is_float = matches!(
            data.polars_dtype(),
            Some(DataType::Float32 | DataType::Float64)
        );
        let cells = data.values();
        let isin = |cell: &Value, set: &[Value]| -> bool {
            if coerce_types {
                match astype_str(cell) {
                    None => false,
                    Some(s) => set.iter().any(|v| v.py_str() == s),
                }
            } else if cell.is_na() {
                // PARITY: NaN in a float64 column never matches `pd.NA`; other nullable columns
                // (`Int64`, `boolean`, `str`, `object`) do.
                !is_float && set.iter().any(Value::is_na)
            } else {
                set.iter().any(|v| !v.is_na() && *v == *cell)
            }
        };
        if in_vals.is_empty() && ex_vals.is_empty() {
            continue;
        }
        let m: Vec<bool> = cells
            .iter()
            .map(|c| {
                (in_vals.is_empty() || isin(c, &in_vals))
                    && (ex_vals.is_empty() || !isin(c, &ex_vals))
            })
            .collect();
        and_mask(m);
    }

    let (mut result, mut index) = match &mask {
        None => (df.clone(), (0..n).collect::<Vec<usize>>()),
        Some(m) => {
            let ix: Vec<usize> = m
                .iter()
                .enumerate()
                .filter(|(_, b)| **b)
                .map(|(i, _)| i)
                .collect();
            (df.take(&ix), ix)
        }
    };
    if opts.reset_index {
        index = (0..result.height()).collect();
    }
    let (enforced, native) = enforce_dtypes_with_path(
        &result,
        &dtypes,
        &EnforceOptions {
            explicit_dtypes: IndexMap::new(),
            coerce_numeric: false,
            coerce_timezone: false,
            strip_timezone: false,
        },
    )?;
    result = enforced;
    if native {
        index = (0..result.height()).collect();
    }

    let select = opts
        .select_columns
        .clone()
        .filter(|s| !(s.len() == 1 && s[0] == "*"));
    if select.as_ref().is_none_or(|s| s.is_empty())
        && opts.omit_columns.as_ref().is_none_or(|s| s.is_empty())
    {
        let original = df.names();
        result = result.select(&original)?;
    } else {
        select_omit(&mut result, select.as_ref(), opts.omit_columns.as_ref());
    }
    // Object columns of nulls stay object columns, as pandas keeps them.
    for col in result.columns_mut() {
        if let ColumnData::Object(values) = &mut col.data {
            for v in values.iter_mut() {
                if matches!(v, Value::Null) {
                    *v = Value::NA;
                }
            }
        }
    }
    Ok(QueryResult {
        frame: result,
        index,
    })
}
