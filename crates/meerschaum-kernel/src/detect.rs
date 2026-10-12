//! Dtype detection on frames, ported from `meerschaum/utils/dataframe.py`
//! (`get_json_cols`, `get_numeric_cols`, `get_uuid_cols`, `get_datetime_cols`, `get_bytes_cols`,
//! `get_geometry_cols`, `get_unhashable_cols`, `get_bool_cols`, `get_date_cols`,
//! `get_special_cols`).
//!
//! Most detectors inspect the *first valid value* of each column, as v4 does.

use indexmap::IndexMap;
use polars::prelude::{DataType, TimeUnit};

use crate::dtypes::{PrecisionUnit, Srid, are_dtypes_equal};
use crate::frame::{ColumnData, Frame};
use crate::value::{TzInfo, Value};

fn first_valid(data: &ColumnData) -> Option<Value> {
    data.first_valid_index().map(|ix| data.get(ix))
}

fn cols_where(df: &Frame, pred: impl Fn(&Value) -> bool) -> Vec<String> {
    if df.height() == 0 {
        return vec![];
    }
    df.columns()
        .iter()
        .filter(|c| first_valid(&c.data).is_some_and(|v| pred(&v)))
        .map(|c| c.name.clone())
        .collect()
}

/// `get_unhashable_cols()`: columns whose value in the *first row* is a `dict`/`list`.
pub fn get_unhashable_cols(df: &Frame) -> Vec<String> {
    if df.height() == 0 {
        return vec![];
    }
    df.columns()
        .iter()
        .filter(|c| !c.data.get(0).is_hashable())
        .map(|c| c.name.clone())
        .collect()
}

/// `get_json_cols()`: the first valid value is a `dict` or `list`.
pub fn get_json_cols(df: &Frame) -> Vec<String> {
    cols_where(df, |v| matches!(v, Value::Json(_)))
}

/// `get_numeric_cols()`: the first valid value is a `Decimal`.
pub fn get_numeric_cols(df: &Frame) -> Vec<String> {
    cols_where(df, |v| matches!(v, Value::Decimal(_)))
}

/// `get_uuid_cols()`: the first valid value is a `UUID`.
pub fn get_uuid_cols(df: &Frame) -> Vec<String> {
    cols_where(df, |v| matches!(v, Value::Uuid(_)))
}

/// `get_bool_cols()`: columns with a boolean dtype (empty frames return nothing).
pub fn get_bool_cols(df: &Frame) -> Vec<String> {
    if df.height() == 0 {
        return vec![];
    }
    df.columns()
        .iter()
        .filter(|c| are_dtypes_equal(&c.data.dtype_str(), "bool"))
        .map(|c| c.name.clone())
        .collect()
}

/// `get_bytes_cols()`: `binary[pyarrow]` columns plus object columns whose first valid value is
/// `bytes`.
pub fn get_bytes_cols(df: &Frame) -> Vec<String> {
    df.columns()
        .iter()
        .filter(|c| {
            matches!(
                c.data.polars_dtype(),
                Some(DataType::Binary | DataType::BinaryOffset)
            ) || (df.height() > 0
                && matches!(&c.data, ColumnData::Object(_))
                && matches!(first_valid(&c.data), Some(Value::Bytes(_))))
        })
        .map(|c| c.name.clone())
        .collect()
}

/// `get_date_cols()`: date dtypes plus columns whose first valid value is a `date`.
///
/// PARITY: `datetime` is a subclass of `date`, so every datetime column (native or object) is
/// also reported as a date column (meerschaum/utils/dataframe.py, `get_date_cols`).
pub fn get_date_cols(df: &Frame) -> Vec<String> {
    df.columns()
        .iter()
        .filter(|c| {
            are_dtypes_equal(&c.data.dtype_str(), "date")
                || (df.height() > 0
                    && matches!(
                        first_valid(&c.data),
                        Some(Value::Date(_) | Value::Datetime(_))
                    ))
        })
        .map(|c| c.name.clone())
        .collect()
}

/// A datetime column's `(timezone name, precision)`.
pub type TzPrecision = (Option<String>, PrecisionUnit);

fn unit_of(tu: TimeUnit) -> PrecisionUnit {
    match tu {
        TimeUnit::Nanoseconds => PrecisionUnit::Nanosecond,
        TimeUnit::Microseconds => PrecisionUnit::Microsecond,
        TimeUnit::Milliseconds => PrecisionUnit::Millisecond,
    }
}

/// Python's `datetime.tzname()`.
fn py_tzname(dt: &crate::value::DateTimeValue) -> Option<String> {
    dt.tz.as_ref().map(|tz| match tz {
        TzInfo::Utc => "UTC".to_string(),
        TzInfo::Fixed(_) => tz.name(),
        TzInfo::Named(named) => {
            use chrono::TimeZone;
            format!("{}", named.offset_from_utc_datetime(&dt.utc))
        }
    })
}

/// `get_datetime_cols(df, timezone_aware, timezone_naive, with_tz_precision=True)`.
///
/// PARITY: native datetime columns are never classified as timezone-aware, because v4 checks
/// `getattr(df[col], 'tz', None)` on the Series (not `.dt.tz`), which is always `None`.
pub fn get_datetime_cols_tz_precision(
    df: &Frame,
    timezone_aware: bool,
    timezone_naive: bool,
) -> Result<IndexMap<String, TzPrecision>, String> {
    if !timezone_aware && !timezone_naive {
        return Err("`timezone_aware` and `timezone_naive` cannot both be `False`.".into());
    }
    let mut known: IndexMap<String, TzPrecision> = IndexMap::new();
    for c in df.columns() {
        if let Some(DataType::Datetime(tu, tz)) = c.data.polars_dtype() {
            known.insert(
                c.name.clone(),
                (tz.as_ref().map(|t| t.as_str().to_string()), unit_of(*tu)),
            );
        }
    }
    if df.height() == 0 {
        return Ok(known);
    }
    let mut pydt: IndexMap<String, (TzPrecision, bool)> = IndexMap::new();
    for c in df.columns() {
        if known.contains_key(&c.name) {
            continue;
        }
        if let Some(Value::Datetime(dt)) = first_valid(&c.data) {
            pydt.insert(
                c.name.clone(),
                (
                    (py_tzname(&dt), PrecisionUnit::Microsecond),
                    dt.tz.is_some(),
                ),
            );
        }
    }
    let mut out = IndexMap::new();
    for c in df.columns() {
        let entry = known
            .get(&c.name)
            .map(|t| (t.clone(), false))
            .or_else(|| pydt.get(&c.name).cloned());
        if let Some((tuple, aware)) = entry {
            let keep = (timezone_aware && timezone_naive)
                || (timezone_aware && aware)
                || (timezone_naive && !timezone_aware && !aware);
            if keep {
                out.insert(c.name.clone(), tuple);
            }
        }
    }
    Ok(out)
}

/// `get_datetime_cols(df, timezone_aware, timezone_naive)`.
pub fn get_datetime_cols(
    df: &Frame,
    timezone_aware: bool,
    timezone_naive: bool,
) -> Result<Vec<String>, String> {
    Ok(
        get_datetime_cols_tz_precision(df, timezone_aware, timezone_naive)?
            .into_keys()
            .collect(),
    )
}

/// `get_datetime_cols_types()`: `datetime64[unit]` / `datetime64[unit, tz]` per column.
pub fn get_datetime_cols_types(df: &Frame) -> IndexMap<String, String> {
    get_datetime_cols_tz_precision(df, true, true)
        .unwrap_or_default()
        .into_iter()
        .map(|(col, (tz, precision))| {
            let abbrev = precision.abbreviation();
            let typ = match tz {
                Some(tz) => format!("datetime64[{abbrev}, {tz}]"),
                None => format!("datetime64[{abbrev}]"),
            };
            (col, typ)
        })
        .collect()
}

/// `get_geometry_cols()`: the first valid value is a geometry.
pub fn get_geometry_cols(df: &Frame) -> Vec<String> {
    cols_where(df, |v| matches!(v, Value::Geometry(_)))
}

/// `get_geometry_cols(df, with_types_srids=True)`: `(geometry type, srid)` per geometry column.
///
/// The type is the single `geom_type` across the column (else `geometry`), suffixed with `Z`
/// when any geometry has Z; the SRID is the column CRS's EPSG code (else `0`).
pub fn get_geometry_cols_types_srids(df: &Frame) -> IndexMap<String, (String, Srid)> {
    let mut out = IndexMap::new();
    for name in get_geometry_cols(df) {
        let col = df.column(&name).expect("column exists");
        let mut types: Vec<&'static str> = Vec::new();
        let mut has_z = false;
        let values = col.data.values();
        for v in &values {
            if let Value::Geometry(g) = v {
                if !types.contains(&g.geom_type()) {
                    types.push(g.geom_type());
                }
                has_z |= g.dims.has_z() && !g.is_empty();
            }
        }
        let srid = col
            .crs
            .as_ref()
            .and_then(Srid::epsg)
            .map(Srid::Int)
            .unwrap_or(Srid::Int(0));
        let mut geometry_type = if types.len() == 1 {
            types[0].to_string()
        } else {
            "geometry".to_string()
        };
        if geometry_type != "geometry" && has_z {
            geometry_type.push('Z');
        }
        out.insert(name, (geometry_type, srid));
    }
    out
}

/// `get_geometry_cols_types()`: `geometry`, `geometry[Point]`, `geometry[Point, 4326]`, ...
pub fn get_geometry_cols_types(df: &Frame) -> IndexMap<String, String> {
    get_geometry_cols_types_srids(df)
        .into_iter()
        .map(|(col, (geometry_type, srid))| {
            if !srid.is_truthy() && geometry_type.eq_ignore_ascii_case("geometry") {
                return (col, "geometry".to_string());
            }
            let mut modifier = String::from("[");
            if !geometry_type.eq_ignore_ascii_case("geometry") {
                modifier.push_str(&geometry_type);
            }
            if srid.is_truthy() {
                if modifier != "[" {
                    modifier.push_str(", ");
                }
                modifier.push_str(&srid.to_string());
            }
            modifier.push(']');
            (col, format!("geometry{modifier}"))
        })
        .collect()
}

/// `get_special_cols()`: Meerschaum dtypes for special columns (later detectors override earlier
/// ones but keep the earlier key position, as Python dict merging does).
pub fn get_special_cols(df: &Frame) -> IndexMap<String, String> {
    let mut out: IndexMap<String, String> = IndexMap::new();
    for c in get_json_cols(df) {
        out.insert(c, "json".into());
    }
    for c in get_uuid_cols(df) {
        out.insert(c, "uuid".into());
    }
    for c in get_bytes_cols(df) {
        out.insert(c, "bytes".into());
    }
    for c in get_bool_cols(df) {
        out.insert(c, "bool".into());
    }
    for c in get_numeric_cols(df) {
        out.insert(c, "numeric".into());
    }
    for c in get_date_cols(df) {
        out.insert(c, "date".into());
    }
    for (c, t) in get_datetime_cols_types(df) {
        out.insert(c, t);
    }
    for (c, t) in get_geometry_cols_types(df) {
        out.insert(c, t);
    }
    out
}
