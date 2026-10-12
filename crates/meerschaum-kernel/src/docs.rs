//! Building frames from documents, as `Pipe.enforce_dtypes` does for `list`/`dict` input:
//! `pd.DataFrame(docs).convert_dtypes()` followed by `parse_df_datetimes()`
//! (`meerschaum/utils/dataframe.py`).

use indexmap::IndexMap;
use once_cell::sync::Lazy;
use polars::prelude::{DataType, TimeUnit, TimeZone};
use regex::Regex;

use crate::frame::{Column, ColumnData, Frame, FrameResult, values_to_series};
use crate::value::{Value, parse_datetime_value};

/// `pd.DataFrame(docs).convert_dtypes()` for a list of documents. Columns appear in order of
/// first appearance; missing keys are null.
pub fn frame_from_docs(docs: &[IndexMap<String, Value>]) -> FrameResult<Frame> {
    let mut keys: Vec<String> = Vec::new();
    for doc in docs {
        for k in doc.keys() {
            if !keys.contains(k) {
                keys.push(k.clone());
            }
        }
    }
    let columns: IndexMap<String, Vec<Value>> = keys
        .into_iter()
        .map(|k| {
            let values = docs
                .iter()
                .map(|d| d.get(&k).cloned().unwrap_or(Value::Null))
                .collect();
            (k, values)
        })
        .collect();
    frame_from_columns(&columns)
}

/// `pd.DataFrame(dict_of_lists).convert_dtypes()`.
pub fn frame_from_columns(columns: &IndexMap<String, Vec<Value>>) -> FrameResult<Frame> {
    let cols = columns
        .iter()
        .map(|(name, values)| Column::new(name.clone(), convert_dtypes(name, values)))
        .collect();
    Frame::new(cols)
}

/// `convert_dtypes(dtype_backend='numpy_nullable')` on a column built from Python values.
///
/// PARITY: integral floats become `Int64` (`[{'val': 1.0}]` → `Int64`), which later makes the
/// mixed int/float rule turn them into numerics against a float column.
pub fn convert_dtypes(name: &str, values: &[Value]) -> ColumnData {
    let non_null: Vec<&Value> = values.iter().filter(|v| !v.is_na()).collect();
    if non_null.is_empty() {
        return ColumnData::Object(values.iter().map(|_| Value::NA).collect());
    }
    let all = |f: fn(&Value) -> bool| non_null.iter().all(|v| f(v));
    let normalized: Vec<Value> = values
        .iter()
        .map(|v| if v.is_na() { Value::Null } else { v.clone() })
        .collect();
    let build =
        |dtype: DataType| values_to_series(name, &normalized, &dtype).map(ColumnData::Native);
    let result = if all(|v| matches!(v, Value::Bool(_))) {
        build(DataType::Boolean)
    } else if all(|v| matches!(v, Value::Int(_))) {
        build(DataType::Int64)
    } else if all(|v| matches!(v, Value::Int(_) | Value::Float(_))) {
        let integral = non_null.iter().all(|v| match v {
            Value::Float(f) => f.fract() == 0.0 && f.is_finite(),
            _ => true,
        });
        if integral {
            let ints: Vec<Value> = normalized
                .iter()
                .map(|v| match v {
                    Value::Float(f) => Value::Int(*f as i64),
                    other => other.clone(),
                })
                .collect();
            values_to_series(name, &ints, &DataType::Int64).map(ColumnData::Native)
        } else {
            build(DataType::Float64)
        }
    } else if all(|v| matches!(v, Value::Str(_))) {
        build(DataType::String)
    } else if all(|v| matches!(v, Value::Datetime(_))) {
        crate::frame::infer_native_series(name, &normalized).map(ColumnData::Native)
    } else {
        None
    };
    result.unwrap_or_else(|| {
        ColumnData::Object(
            values
                .iter()
                .map(|v| if v.is_na() { Value::NA } else { v.clone() })
                .collect(),
        )
    })
}

static ISO_DT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}.\d{2}:\d{2}:\d+").expect("valid regex"));

/// `parse_df_datetimes(df, ignore_cols, strip_timezone, ignore_all)` with the default
/// `precision_unit=None` and `coerce_utc=True`.
///
/// A column is parsed when *every* value's `str()` matches `\d{4}-\d{2}-\d{2}.\d{2}:\d{2}:\d+`
/// (so a column with any null is left as text), then converted to UTC `datetime64[us]`.
pub fn parse_df_datetimes(
    df: &Frame,
    ignore_cols: &[String],
    strip_timezone: bool,
    ignore_all: bool,
) -> Frame {
    if df.height() == 0 || ignore_all {
        return df.clone();
    }
    let mut out = df.clone();
    for col in df.columns() {
        if ignore_cols.contains(&col.name) || col.data.dtype_str().contains("datetime") {
            continue;
        }
        let values = col.data.values();
        let all_match = values
            .iter()
            .all(|v| !v.is_na() && ISO_DT_RE.is_match(&v.py_str()));
        if !all_match {
            continue;
        }
        let parsed: Option<Vec<Value>> = values
            .iter()
            .map(|v| {
                let d = match v {
                    Value::Datetime(d) => d.clone(),
                    other => parse_datetime_value(&other.py_str())?,
                };
                let d = crate::cast::coerce_datetime(&d, strip_timezone);
                Some(Value::Datetime(d))
            })
            .collect();
        let Some(parsed) = parsed else { continue };
        let tz = (!strip_timezone).then_some(TimeZone::UTC);
        if let Some(s) = values_to_series(
            &col.name,
            &parsed,
            &DataType::Datetime(TimeUnit::Microseconds, tz),
        ) {
            let _ = out.set(&col.name, ColumnData::Native(s));
        }
    }
    out
}
