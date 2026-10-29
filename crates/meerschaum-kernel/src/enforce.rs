//! `enforce_dtypes` (`meerschaum/utils/dataframe.py`) and the `Pipe.enforce_dtypes` call path
//! (`meerschaum/core/Pipe/_dtypes.py`, `Pipe.get_dtypes`, `Pipe.tzinfo`).
//!
//! v4 has two enforcement paths and the kernel keeps both, because they produce different
//! values for the same input:
//!
//! 1. the *native* path (`_enforce_dtypes_with_polars`): strict Polars casts, taken when every
//!    target dtype is Arrow-native and (for the whole frame) every column is typed; when some
//!    columns are untyped or need object handling, it runs on the native subset first;
//! 2. the *object* path (the pandas body of `enforce_dtypes`): per-value casts for JSON, numeric,
//!    UUID, bytes, datetime and geometry columns, then a final `astype` loop with the mixed
//!    int/float → numeric rule.

use indexmap::IndexMap;
use polars::prelude::*;

use crate::cast::{
    attempt_cast_to_bytes, attempt_cast_to_geometry, attempt_cast_to_numeric, attempt_cast_to_uuid,
    coerce_datetime, value_is_null,
};
use crate::detect::get_numeric_cols;
use crate::dtypes::{
    Srid, are_dtypes_equal, are_dtypes_maps_equal, get_geometry_type_srid,
    get_numeric_precision_scale, is_dtype_numeric, polars_dtype_for_pandas, resolve_alias,
    to_pandas_dtype,
};
use crate::frame::{
    Column, ColumnData, Frame, days_since_epoch, infer_native_series, json_to_value,
    values_to_series,
};
use crate::geometry::{Geometry, geometry_is_gpkg, gpkg_wkb_to_wkb};
use crate::serialize::{compact_json, json_dumps_value};
use crate::value::{DateTimeValue, TzInfo, Value, parse_iso_datetime};

/// Errors that v4 raises out of `enforce_dtypes` (most failures are swallowed instead).
#[derive(Debug, thiserror::Error)]
pub enum EnforceError {
    #[error("{0}")]
    Raised(String),
    #[error(transparent)]
    Frame(#[from] crate::frame::FrameError),
}

/// Options for [`enforce_dtypes`] (the keyword arguments of v4's function).
#[derive(Clone, Debug)]
pub struct EnforceOptions {
    /// Explicitly configured dtypes (respected by the mixed int/float → numeric rule).
    pub explicit_dtypes: IndexMap<String, String>,
    /// Convert int/float collisions to `numeric` (`Pipe.mixed_numerics`).
    pub coerce_numeric: bool,
    /// Convert datetimes to UTC.
    pub coerce_timezone: bool,
    /// With `coerce_timezone`, strip the timezone (timezone-naive pipes).
    pub strip_timezone: bool,
}

impl Default for EnforceOptions {
    fn default() -> Self {
        Self {
            explicit_dtypes: IndexMap::new(),
            coerce_numeric: false,
            coerce_timezone: true,
            strip_timezone: false,
        }
    }
}

fn normalized(typ: &str) -> String {
    resolve_alias(typ).to_lowercase()
}

fn is_fallback_dtype(norm: &str) -> bool {
    norm == "uuid"
        || norm == "object"
        || norm.starts_with("geometry")
        || norm.starts_with("geography")
        || (norm.starts_with("numeric") && {
            let (p, s) = get_numeric_precision_scale(Some(norm));
            p.is_none() || s.is_none()
        })
}

/// `enforce_dtypes(df, dtypes, ...)`.
pub fn enforce_dtypes(
    df: &Frame,
    dtypes: &IndexMap<String, String>,
    opts: &EnforceOptions,
) -> Result<Frame, EnforceError> {
    enforce_dtypes_with_path(df, dtypes, opts).map(|(f, _)| f)
}

/// [`enforce_dtypes`], also reporting whether the whole frame went through the native (Polars)
/// path. v4 rebuilds the pandas frame from Polars on that path, which resets the row index.
pub fn enforce_dtypes_with_path(
    df: &Frame,
    dtypes: &IndexMap<String, String>,
    opts: &EnforceOptions,
) -> Result<(Frame, bool), EnforceError> {
    enforce_dtypes_with_report(df, dtypes, opts)
        .map(|(frame, path)| (frame, path == EnforcePath::Native))
}

/// The path [`enforce_dtypes_with_report`] took, which decides the pandas dtypes v4 returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnforcePath {
    /// The whole frame went through Polars (`to_pandas(use_pyarrow_extension_array=True)`).
    Native,
    /// These typed columns (the native subset of a mixed frame) went through Polars first and
    /// were assigned back Arrow-backed; the rest took the object path.
    Subset(Vec<String>),
    /// The pandas (object) path only.
    Object,
}

/// [`enforce_dtypes`], also reporting the [`EnforcePath`].
pub fn enforce_dtypes_with_report(
    df: &Frame,
    dtypes: &IndexMap<String, String>,
    opts: &EnforceOptions,
) -> Result<(Frame, EnforcePath), EnforceError> {
    let norm: IndexMap<&str, String> = dtypes
        .iter()
        .map(|(c, t)| (c.as_str(), normalized(t)))
        .collect();
    let fallback: IndexMap<String, String> = dtypes
        .iter()
        .filter(|(c, _)| is_fallback_dtype(&norm[c.as_str()]))
        .map(|(c, t)| (c.clone(), t.clone()))
        .collect();
    let native: IndexMap<String, String> = dtypes
        .iter()
        .filter(|(c, _)| !fallback.contains_key(*c))
        .map(|(c, t)| (c.clone(), t.clone()))
        .collect();
    let untyped = df
        .names()
        .into_iter()
        .any(|c| !norm.contains_key(c.as_str()));
    let native_strip = if opts.coerce_timezone {
        opts.strip_timezone
    } else {
        false
    };

    if !native.is_empty()
        && fallback.is_empty()
        && !untyped
        && let Some(out) = enforce_native(df, &native, native_strip)
    {
        return Ok((out, EnforcePath::Native));
    }

    let mut df = df.clone();
    let mut dtypes = dtypes.clone();
    let mut path = EnforcePath::Object;
    if !native.is_empty() && (!fallback.is_empty() || untyped) {
        let native_cols: Vec<String> = native.keys().filter(|c| df.contains(c)).cloned().collect();
        if !native_cols.is_empty() {
            let subset: Vec<&Column> = native_cols.iter().filter_map(|c| df.column(c)).collect();
            if let Some(native_df) =
                enforce_native_columns(&subset, df.height(), &native, native_strip)
            {
                for col in native_df.into_columns() {
                    df.set_column(col)?;
                }
                dtypes = fallback.clone();
                path = EnforcePath::Subset(native_cols);
            }
        }
    }
    enforce_object_path(df, &dtypes, opts).map(|f| (f, path))
}

// ---------------------------------------------------------------------------------------------
// Native (Polars) path.

/// `to_polars()` for one column: the Polars series v4 would get, or `None` if Polars could not
/// build a castable column (UUIDs, geometries, mixed objects).
fn to_polars_column(col: &Column) -> Option<Series> {
    let name = col.name.as_str();
    match &col.data {
        ColumnData::Native(s) => Some(s.clone()),
        ColumnData::Object(values) => {
            if let Some(s) = infer_native_series(name, values) {
                return Some(s);
            }
            let non_null: Vec<&Value> = values.iter().filter(|v| !v.is_na()).collect();
            if non_null.is_empty() {
                return Some(Series::full_null(
                    name.into(),
                    values.len(),
                    &DataType::Null,
                ));
            }
            let all = |f: fn(&Value) -> bool| non_null.iter().all(|v| f(v));
            if all(|v| matches!(v, Value::Decimal(_))) {
                let s = crate::frame::object_values_to_series(name, values);
                return matches!(s.dtype(), DataType::Decimal(..)).then_some(s);
            }
            if all(|v| matches!(v, Value::Date(_))) {
                return values_to_series(name, values, &DataType::Date);
            }
            if all(|v| matches!(v, Value::Bytes(_))) {
                return values_to_series(name, values, &DataType::Binary);
            }
            if all(|v| matches!(v, Value::Bool(_))) {
                return values_to_series(name, values, &DataType::Boolean);
            }
            None
        }
    }
}

fn native_target(norm: &str, strip_timezone: bool) -> Option<DataType> {
    Some(match norm {
        "int" | "int64" => DataType::Int64,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float" | "float64" | "double" => DataType::Float64,
        "float32" => DataType::Float32,
        "str" | "string" => DataType::String,
        "bool" => DataType::Boolean,
        "bytes" => DataType::Binary,
        "date" => DataType::Date,
        t if t.starts_with("datetime") => {
            // PARITY: the unit is read as `typ.split('[')[-1].split(',')[0]`, so it is only
            // recognized when a timezone follows (`datetime64[ns, utc]`); `datetime64[ns]` reads
            // `ns]` and falls back to `us`. The timezone is always UTC unless stripping
            // (meerschaum/utils/dataframe.py, `_enforce_dtypes_with_polars`).
            let unit = t
                .rsplit('[')
                .next()
                .unwrap_or("")
                .split(',')
                .next()
                .unwrap_or("");
            let tu = match unit {
                "ns" => TimeUnit::Nanoseconds,
                "ms" => TimeUnit::Milliseconds,
                _ => TimeUnit::Microseconds,
            };
            DataType::Datetime(tu, (!strip_timezone).then_some(TimeZone::UTC))
        }
        t if t.starts_with("numeric") => {
            let (p, s) = get_numeric_precision_scale(Some(t));
            let (p, s) = (p?, s?);
            // PARITY: v4 casts to `polars.Decimal(precision, scale)`; for a precision outside
            // 1..=38 (`numeric[0,0]` included) the cast raises "precision must be between 1 and
            // 38", for a scale above the precision (`numeric[5,10]`) "scale must be less than or
            // equal to precision", and `except Exception: return None` leaves the frame to the
            // pandas path (meerschaum/utils/dataframe.py, `_enforce_dtypes_with_polars`).
            if !(1..=38).contains(&p) || s < 0 || s > p {
                return None;
            }
            DataType::Decimal(p as usize, s as usize)
        }
        _ => return None,
    })
}

/// `_enforce_dtypes_with_polars()`: `None` when any cast fails or a dtype is unsupported.
fn enforce_native(
    df: &Frame,
    dtypes: &IndexMap<String, String>,
    strip_timezone: bool,
) -> Option<Frame> {
    let columns: Vec<&Column> = df.columns().iter().collect();
    enforce_native_columns(&columns, df.height(), dtypes, strip_timezone)
}

/// [`enforce_native`] on `columns` (a frame of `height` rows, or a selection of its columns),
/// reading them without copying: only the columns passed through unchanged are cloned.
fn enforce_native_columns(
    columns: &[&Column],
    height: usize,
    dtypes: &IndexMap<String, String>,
    strip_timezone: bool,
) -> Option<Frame> {
    for typ in dtypes.values() {
        if is_fallback_dtype(&normalized(typ)) {
            return None;
        }
    }
    // PARITY: when any column holds UUIDs, v4 builds the Polars frame with `strict=False`,
    // which leaves Python objects that no cast accepts.
    let uuid_typed = columns
        .iter()
        .any(|c| dtypes.contains_key(&c.name) && crate::detect::is_uuid_column(c, height));
    if uuid_typed {
        return None;
    }
    let mut out = Frame::with_height(height);
    // `out.set(name, data)` on a copy of the frame: the data is replaced and the tags reset
    // (the CRS is kept); untouched columns stay as they are.
    let replace = |out: &mut Frame, col: &Column, data: ColumnData| -> Option<()> {
        out.set_column(Column {
            name: col.name.clone(),
            data,
            crs: col.crs.clone(),
            tags: Default::default(),
        })
        .ok()
    };
    for col in columns.iter().copied() {
        let norm = dtypes.get(&col.name).map(|t| normalized(t));
        if norm.as_deref() == Some("json") {
            // PARITY: `to_polars(json_cols=...)` dumps each value and `to_pandas()` `json.loads`
            // it back; pandas infers the dtype.
            let values = json_roundtrip_values(&col.data);
            replace(&mut out, col, apply_result(&col.name, values))?;
            continue;
        }
        let series = to_polars_column(col)?;
        let Some(norm) = norm else {
            replace(&mut out, col, ColumnData::Native(series))?;
            continue;
        };
        if norm == "object" {
            out.set_column(col.clone()).ok()?;
            continue;
        }
        let target = native_target(&norm, strip_timezone)?;
        let casted = native_cast(&series, &target)?;
        replace(&mut out, col, ColumnData::Native(casted))?;
    }
    Some(out)
}

/// The values of a declared `json` column after v4's `to_polars(json_cols=...)` /
/// `to_pandas()` round trip: each value is dumped to JSON text (strings are `json.loads`ed first
/// when they parse) and loaded back, null-likes become `None`.
///
/// A [`Value::Json`] container round-trips to itself (the dump is valid JSON and Rust's float
/// formatting round-trips exactly), so it is kept without serializing it.
fn json_roundtrip_values(data: &ColumnData) -> Vec<Value> {
    let roundtrip = |v: &Value| -> Value {
        if value_is_null(v) {
            return Value::Null;
        }
        let text = match v {
            Value::Json(j) => return Value::Json(j.clone()),
            Value::Str(s) => match serde_json::from_str::<serde_json::Value>(s) {
                Ok(j) => compact_json(&j),
                Err(_) => compact_json(&serde_json::Value::String(s.clone())),
            },
            other => json_dumps_value(other),
        };
        serde_json::from_str(&text)
            .map(json_to_value)
            .unwrap_or(Value::Null)
    };
    match data {
        ColumnData::Object(values) => values.iter().map(roundtrip).collect(),
        ColumnData::Native(s) => crate::frame::series_to_values(s)
            .iter()
            .map(roundtrip)
            .collect(),
    }
}

/// A strict Polars cast following `_enforce_dtypes_with_polars` (string sources are parsed).
fn native_cast(s: &Series, target: &DataType) -> Option<Series> {
    let name = s.name().clone();
    let src = s.dtype().clone();
    if src == DataType::String {
        let ca = s.str().ok()?;
        return match target {
            DataType::Boolean => {
                let out: Option<Vec<Option<bool>>> = ca
                    .iter()
                    .map(|v| match v {
                        None => Some(None),
                        Some(t) => match t.to_lowercase().as_str() {
                            "true" | "1" => Some(Some(true)),
                            "false" | "0" => Some(Some(false)),
                            _ => None,
                        },
                    })
                    .collect();
                Some(BooleanChunked::from_iter_options(name, out?.into_iter()).into_series())
            }
            DataType::Binary => {
                use base64::Engine as _;
                let out: Option<Vec<Option<Vec<u8>>>> = ca
                    .iter()
                    .map(|v| match v {
                        None => Some(None),
                        Some(t) => base64::engine::general_purpose::STANDARD
                            .decode(t)
                            .ok()
                            .map(Some),
                    })
                    .collect();
                Some(BinaryChunked::from_iter_options(name, out?.into_iter()).into_series())
            }
            DataType::Date => {
                let fmt = infer_date_format(ca.iter().flatten().next()?)?;
                let out: Option<Vec<Option<i32>>> = ca
                    .iter()
                    .map(|v| match v {
                        None => Some(None),
                        Some(t) => chrono::NaiveDate::parse_from_str(t.trim(), fmt)
                            .ok()
                            .map(|d| Some(days_since_epoch(&d))),
                    })
                    .collect();
                Some(
                    Int32Chunked::from_iter_options(name, out?.into_iter())
                        .into_date()
                        .into_series(),
                )
            }
            DataType::Datetime(tu, tz) => str_to_datetime(ca, *tu, tz.clone()),
            DataType::Decimal(p, sc) => {
                let scale = *sc as i64;
                let out: Option<Vec<Option<i128>>> = ca
                    .iter()
                    .map(|v| match v {
                        None => Some(None),
                        Some(t) => {
                            let d = crate::Decimal::parse(t).ok()?;
                            let q = d.quantize(*p as i64, scale).ok()?;
                            q.to_i128_at_scale(scale).map(Some)
                        }
                    })
                    .collect();
                Some(
                    Int128Chunked::from_iter_options(name, out?.into_iter())
                        .into_decimal_unchecked(*p, scale as usize)
                        .into_series(),
                )
            }
            _ => s.strict_cast(target).ok(),
        };
    }
    match (&src, target) {
        (DataType::Datetime(stu, _), DataType::Datetime(ttu, ttz)) => {
            let phys = s.to_physical_repr().into_owned();
            let phys = phys.i64().ok()?;
            if stu == ttu {
                // Same unit: the timestamps are unchanged, only the timezone metadata differs.
                return Some(
                    phys.clone()
                        .with_name(name)
                        .into_datetime(*ttu, ttz.clone())
                        .into_series(),
                );
            }
            let out: Vec<Option<i64>> = phys
                .iter()
                .map(|v| v.map(|v| convert_time_unit(v, *stu, *ttu)))
                .collect();
            Some(
                Int64Chunked::from_iter_options(name, out.into_iter())
                    .into_datetime(*ttu, ttz.clone())
                    .into_series(),
            )
        }
        (DataType::Float32 | DataType::Float64, DataType::Datetime(tu, tz)) => {
            let ints = s.strict_cast(&DataType::Int64).ok()?;
            Some(
                ints.i64()
                    .ok()?
                    .clone()
                    .into_datetime(*tu, tz.clone())
                    .into_series(),
            )
        }
        (DataType::Decimal(..), DataType::Decimal(p, sc))
        | (
            DataType::Float32
            | DataType::Float64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64,
            DataType::Decimal(p, sc),
        ) => {
            let scale = *sc as i64;
            let values = crate::frame::series_to_values(s);
            let out: Option<Vec<Option<i128>>> = values
                .iter()
                .map(|v| {
                    if v.is_na() {
                        return Some(None);
                    }
                    let d = match v {
                        Value::Decimal(d) => d.clone(),
                        other => crate::Decimal::parse(&other.py_str()).ok()?,
                    };
                    let q = d.quantize(*p as i64, scale).ok()?;
                    q.to_i128_at_scale(scale).map(Some)
                })
                .collect();
            Some(
                Int128Chunked::from_iter_options(name, out?.into_iter())
                    .into_decimal_unchecked(*p, scale as usize)
                    .into_series(),
            )
        }
        // PARITY: `float(Decimal)` is correctly rounded; Polars' cast divides the mantissa by
        // `10^scale` in floating point and can land one ulp off.
        (DataType::Decimal(..), DataType::Float64 | DataType::Float32) => {
            let ca = s.decimal().ok()?;
            let scale = ca.scale();
            let floats = Float64Chunked::from_iter_options(
                name,
                ca.physical()
                    .iter()
                    .map(|m| m.map(|m| decimal_mantissa_to_f64(m, scale))),
            )
            .into_series();
            if matches!(target, DataType::Float32) {
                floats.cast(&DataType::Float32).ok()
            } else {
                Some(floats)
            }
        }
        (DataType::Decimal(..), DataType::String) => {
            let values = crate::frame::series_to_values(s);
            Some(
                StringChunked::from_iter_options(
                    name,
                    values.iter().map(|v| match v {
                        Value::Decimal(d) if !d.is_nan() => Some(d.to_plain_string()),
                        _ => None,
                    }),
                )
                .into_series(),
            )
        }
        _ => s.strict_cast(target).ok(),
    }
}

/// The correctly rounded `f64` nearest `mantissa / 10^scale` (`float(Decimal)`). Both operands
/// are exact doubles when `|mantissa| < 2^53` and `scale <= 22`, so one IEEE division is
/// correctly rounded; otherwise parse the decimal string.
fn decimal_mantissa_to_f64(mantissa: i128, scale: usize) -> f64 {
    const EXACT: i128 = 1 << 53;
    if mantissa.abs() < EXACT && scale <= 22 {
        return mantissa as f64 / 10f64.powi(scale as i32);
    }
    let digits = mantissa.unsigned_abs().to_string();
    let sign = if mantissa < 0 { "-" } else { "" };
    format!("{sign}{digits}e-{scale}")
        .parse()
        .unwrap_or(f64::NAN)
}

fn convert_time_unit(v: i64, from: TimeUnit, to: TimeUnit) -> i64 {
    let factor = |tu: TimeUnit| -> i64 {
        match tu {
            TimeUnit::Nanoseconds => 1,
            TimeUnit::Microseconds => 1000,
            TimeUnit::Milliseconds => 1_000_000,
        }
    };
    let (f, t) = (factor(from), factor(to));
    if f >= t {
        v * (f / t)
    } else {
        v.div_euclid(t / f)
    }
}

fn infer_date_format(sample: &str) -> Option<&'static str> {
    let sample = sample.trim();
    [
        "%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y",
    ]
    .into_iter()
    .find(|f| chrono::NaiveDate::parse_from_str(sample, f).is_ok())
}

/// The shape of a datetime string, used to emulate Polars' format inference: every value must
/// share the first value's shape.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
struct DtShape {
    has_time: bool,
    sep: u8,
    has_seconds: bool,
    has_offset: bool,
}

fn dt_shape(s: &str) -> Option<DtShape> {
    dt_shape_parsed(s).map(|(shape, _)| shape)
}

/// [`dt_shape`] with the parsed value (`parse_iso_datetime` trims its input, so parsing the
/// trimmed text gives the same result).
fn dt_shape_parsed(
    s: &str,
) -> Option<(
    DtShape,
    (chrono::NaiveDateTime, Option<chrono::FixedOffset>),
)> {
    let s = s.trim();
    let parsed = parse_iso_datetime(s)?;
    Some((dt_shape_of(s)?, parsed))
}

/// The shape of trimmed text that parses as a datetime.
fn dt_shape_of(s: &str) -> Option<DtShape> {
    if s.len() <= 10 {
        return Some(DtShape {
            has_time: false,
            sep: 0,
            has_seconds: false,
            has_offset: false,
        });
    }
    let sep = s.as_bytes()[10];
    let rest = &s[11..];
    let time_part = rest.split(['+', '-', 'Z']).next().unwrap_or("");
    Some(DtShape {
        has_time: true,
        sep,
        has_seconds: time_part.matches(':').count() >= 2,
        has_offset: rest.contains(['+', '-', 'Z']),
    })
}

/// `f` over `items` in contiguous chunks on scoped threads (large inputs only), concatenated in
/// order: the same result as `f(items)` when `f` maps each item independently. `None` from any
/// chunk is `None` overall.
fn par_map_chunks<T: Sync, R: Send>(
    items: &[T],
    f: impl Fn(&[T]) -> Option<Vec<R>> + Sync,
) -> Option<Vec<R>> {
    const MIN_CHUNK: usize = 65_536;
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(8)
        .min(items.len() / MIN_CHUNK);
    if threads < 2 {
        return f(items);
    }
    let size = items.len().div_ceil(threads);
    std::thread::scope(|scope| {
        let handles: Vec<_> = items
            .chunks(size)
            .map(|chunk| scope.spawn(|| f(chunk)))
            .collect();
        let mut out = Vec::with_capacity(items.len());
        for handle in handles {
            let part = handle
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            out.extend(part?);
        }
        Some(out)
    })
}

/// `Expr.str.to_datetime(time_unit, time_zone, strict=True)`.
fn str_to_datetime(ca: &StringChunked, tu: TimeUnit, tz: Option<TimeZone>) -> Option<Series> {
    let first = ca.iter().flatten().next();
    let shape = match first {
        Some(f) => dt_shape(f)?,
        None => {
            return Some(
                Int64Chunked::full_null(ca.name().clone(), ca.len())
                    .into_datetime(tu, tz)
                    .into_series(),
            );
        }
    };
    let texts: Vec<Option<&str>> = ca.iter().collect();
    let out = par_map_chunks(&texts, |chunk| {
        chunk
            .iter()
            .map(|v| match v {
                None => Some(None),
                Some(t) => {
                    let (t_shape, (naive, offset)) = dt_shape_parsed(t)?;
                    if t_shape != shape {
                        return None;
                    }
                    let utc = match offset {
                        Some(off) => {
                            naive - chrono::Duration::seconds(off.local_minus_utc() as i64)
                        }
                        None => naive,
                    };
                    DateTimeValue::naive(utc).to_timestamp(tu).map(Some)
                }
            })
            .collect()
    });
    Some(
        Int64Chunked::from_iter_options(ca.name().clone(), out?.into_iter())
            .into_datetime(tu, tz)
            .into_series(),
    )
}

// ---------------------------------------------------------------------------------------------
// Object (pandas) path.

/// The column pandas produces from `Series.apply(...)`: homogeneous scalar results become native.
pub fn apply_result(name: &str, values: Vec<Value>) -> ColumnData {
    match infer_native_series(name, &values) {
        Some(s)
            if !matches!(s.dtype(), DataType::Float64)
                || values.iter().all(|v| !matches!(v, Value::Null)) =>
        {
            ColumnData::Native(s)
        }
        _ => ColumnData::Object(values),
    }
}

fn apply_values(data: &ColumnData, f: impl Fn(&Value) -> Value) -> Vec<Value> {
    data.values().iter().map(f).collect()
}

fn enforce_object_path(
    mut df: Frame,
    dtypes: &IndexMap<String, String>,
    opts: &EnforceOptions,
) -> Result<Frame, EnforceError> {
    if df.width() == 0 {
        return Ok(df);
    }
    let pipe_pandas: IndexMap<String, String> = dtypes
        .iter()
        .map(|(c, t)| (c.clone(), to_pandas_dtype(t)))
        .collect();
    // PARITY: these checks use the raw dtype strings (aliases such as `JSON` are not matched).
    let json_cols: Vec<&String> = dtypes
        .iter()
        .filter(|(_, t)| *t == "json")
        .map(|(c, _)| c)
        .collect();
    let numeric_cols: Vec<&String> = dtypes
        .iter()
        .filter(|(_, t)| t.starts_with("numeric"))
        .map(|(c, _)| c)
        .collect();
    let mut geometry_cols: IndexMap<String, (String, Srid)> = IndexMap::new();
    for (c, t) in dtypes {
        if t.starts_with("geometry") || t.starts_with("geography") {
            let ts = get_geometry_type_srid(t, "geometry", Srid::Code(0))
                .map_err(|e| EnforceError::Raised(format!("ValueError: {e}")))?;
            geometry_cols.insert(c.clone(), ts);
        }
    }
    let uuid_cols: Vec<&String> = dtypes
        .iter()
        .filter(|(_, t)| *t == "uuid")
        .map(|(c, _)| c)
        .collect();
    let bytes_cols: Vec<&String> = dtypes
        .iter()
        .filter(|(_, t)| *t == "bytes")
        .map(|(c, _)| c)
        .collect();
    let datetime_cols: Vec<&String> = dtypes
        .iter()
        .filter(|(_, t)| are_dtypes_equal(t, "datetime"))
        .map(|(c, _)| c)
        .collect();
    let df_numeric_cols = get_numeric_cols(&df);

    if !json_cols.is_empty() && df.height() > 0 {
        for col in json_cols {
            let Ok(data) = df.data(col) else { continue };
            // PARITY: one unparseable string aborts the whole column's `apply`; it stays as-is.
            let mut out = Vec::with_capacity(data.len());
            let mut failed = false;
            for v in data.values() {
                match &v {
                    Value::Str(s) => match serde_json::from_str::<serde_json::Value>(s) {
                        Ok(j) => out.push(json_to_value(j)),
                        Err(_) => {
                            failed = true;
                            break;
                        }
                    },
                    _ => out.push(v),
                }
            }
            if !failed {
                df.set(col, apply_result(col, out))?;
            }
        }
    }

    for col in numeric_cols {
        let (p, s) = get_numeric_precision_scale(dtypes.get(col).map(String::as_str));
        let Ok(data) = df.data(col) else { continue };
        let results: Result<Vec<Value>, String> = data
            .values()
            .iter()
            .map(|v| attempt_cast_to_numeric(v, true, p, s))
            .collect();
        // PARITY: a `Decimal` that fails to quantize raises inside `apply`; the column is left
        // unchanged (the exception is only logged in debug mode).
        if let Ok(values) = results {
            df.set(col, ColumnData::Object(values))?;
        }
    }

    for col in uuid_cols {
        let Ok(data) = df.data(col) else { continue };
        let values = apply_values(data, attempt_cast_to_uuid);
        df.set(col, apply_result(col, values))?;
    }

    for col in bytes_cols {
        let Ok(data) = df.data(col) else { continue };
        let values = apply_values(data, attempt_cast_to_bytes);
        df.set(col, apply_result(col, values))?;
    }

    if opts.coerce_timezone {
        for col in datetime_cols {
            let Ok(data) = df.data(col) else { continue };
            let dtype = data.dtype_str();
            if !opts.strip_timezone && dtype.to_lowercase().contains("utc") {
                continue;
            }
            // PARITY: the stripped-timezone skip sits inside `if debug:` in v4, so it only skips
            // when debugging; without debug the column is always coerced.
            let coerced = series_coerce_timezone(data, opts.strip_timezone)?;
            df.set(col, coerced)?;
        }
    }

    if !geometry_cols.is_empty() {
        let mut parsed: Vec<String> = Vec::new();
        for col in geometry_cols.keys() {
            let Some(column) = df.column(col).cloned() else {
                continue;
            };
            if let Ok(new_col) = series_attempt_cast_to_geometry(&column) {
                df.set_column(new_col)?;
                parsed.push(col.clone());
            }
        }
        if let Some(first) = parsed.first() {
            // PARITY: only the first parsed geometry column receives its dtype's CRS; the other
            // columns' `set_crs` results are discarded (`_ = df[col].set_crs(srid)`).
            let (_, srid) = &geometry_cols[first];
            if let Some(c) = df.column_mut(first)
                && srid.truthy()
            {
                c.crs = Some(srid.clone());
            }
        }
    }

    let mut df_dtypes: IndexMap<String, String> = df.dtypes();
    let maps_equal = |a: &IndexMap<String, String>, b: &IndexMap<String, String>| {
        are_dtypes_maps_equal(
            a.iter().map(|(k, v)| (k.as_str(), v.as_str())),
            b.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        )
    };
    if maps_equal(&df_dtypes, &pipe_pandas) {
        return Ok(df);
    }

    let mut common: IndexMap<String, String> = IndexMap::new();
    let mut common_diff: IndexMap<String, String> = IndexMap::new();
    for (col, typ) in &pipe_pandas {
        if let Some(df_typ) = df_dtypes.get(col) {
            common.insert(col.clone(), typ.clone());
            if !are_dtypes_equal(typ, df_typ) {
                common_diff.insert(col.clone(), df_typ.clone());
            }
        }
    }
    let detected_dt: Vec<String> = common_diff
        .iter()
        .filter(|(col, typ)| typ.contains("datetime") && common[col.as_str()].contains("datetime"))
        .map(|(c, _)| c.clone())
        .collect();
    for col in &detected_dt {
        let typ = common_diff.shift_remove(col).unwrap_or_default();
        df_dtypes.insert(col.clone(), typ);
    }
    if maps_equal(&df_dtypes, &pipe_pandas) {
        return Ok(df);
    }

    let mut numeric_casts: Vec<String> = Vec::new();
    for (col, typ) in &common_diff {
        let previous = &common[col];
        let mixed = is_dtype_numeric(typ) && is_dtype_numeric(previous);
        let explicit = opts
            .explicit_dtypes
            .get(col)
            .map(String::as_str)
            .unwrap_or("object");
        let explicitly_float = are_dtypes_equal(explicit, "float");
        let explicitly_int = are_dtypes_equal(explicit, "int");
        let explicitly_numeric = explicit.starts_with("numeric");
        let all_nan = if mixed && opts.coerce_numeric && !(explicitly_float || explicitly_int) {
            df.data(col)?.is_null_mask().iter().all(|n| *n)
        } else {
            false
        };
        let cast_to_numeric = explicitly_numeric
            || df_numeric_cols.contains(col)
            || (mixed && !(explicitly_float || explicitly_int) && !all_nan && opts.coerce_numeric);
        if cast_to_numeric {
            numeric_casts.push(col.clone());
        }
    }

    for col in common_diff.keys() {
        let data = df.data(col)?.clone();
        if numeric_casts.contains(col) {
            let values: Vec<Value> = data
                .values()
                .iter()
                .map(|v| {
                    attempt_cast_to_numeric(v, false, None, None).unwrap_or_else(|_| v.clone())
                })
                .collect();
            df.set(col, ColumnData::Object(values))?;
            continue;
        }
        let target = &common[col];
        match pandas_astype(col, &data, target) {
            Ok(new) => df.set(col, new)?,
            Err(_) => {
                if target.to_lowercase().contains("int")
                    && let Ok(as_float) = pandas_astype(col, &data, "float64")
                    && let Ok(new) = pandas_astype(col, &as_float, target)
                {
                    df.set(col, new)?;
                }
            }
        }
    }
    Ok(df)
}

/// `coerce_timezone()` for a column (`Series`).
pub fn series_coerce_timezone(
    data: &ColumnData,
    strip_utc: bool,
) -> Result<ColumnData, EnforceError> {
    if let Some(DataType::Datetime(tu, tz)) = data.polars_dtype() {
        let ColumnData::Native(s) = data else {
            unreachable!()
        };
        let target = match (tz, strip_utc) {
            (Some(_), true) => DataType::Datetime(*tu, None),
            (Some(_), false) => DataType::Datetime(*tu, Some(TimeZone::UTC)),
            (None, true) => return Ok(data.clone()),
            (None, false) => DataType::Datetime(*tu, Some(TimeZone::UTC)),
        };
        return native_cast(s, &target)
            .map(ColumnData::Native)
            .ok_or_else(|| EnforceError::Raised("failed to convert timezone".into()));
    }
    // `to_datetime(series, coerce_utc=False)`: `astype('datetime64[us, UTC]')`.
    let values = data.values();
    let mut out = Vec::with_capacity(values.len());
    for v in &values {
        let parsed = match v {
            v if v.is_na() => None,
            Value::Datetime(d) => Some(coerce_datetime(d, false)),
            Value::Date(d) => Some(DateTimeValue::utc(
                d.and_hms_opt(0, 0, 0).expect("midnight"),
            )),
            Value::Str(s) => match crate::value::parse_datetime_value(s) {
                Some(d) => Some(coerce_datetime(&d, false)),
                None => {
                    // PARITY: the per-value fallback leaves an object column, and the following
                    // `.dt.tz` access raises out of `enforce_dtypes`.
                    return Err(EnforceError::Raised(format!(
                        "Can only use .dt accessor with datetimelike values (unparseable '{s}')"
                    )));
                }
            },
            // PARITY: `astype('datetime64[us, UTC]')` reads integers as microseconds since
            // the epoch (not nanoseconds, as `pd.to_datetime` would).
            Value::Int(i) => {
                DateTimeValue::from_timestamp(*i, TimeUnit::Microseconds, Some(TzInfo::Utc))
            }
            other => {
                return Err(EnforceError::Raised(format!(
                    "Cannot convert {} to datetime",
                    other.type_name()
                )));
            }
        };
        out.push(match parsed {
            Some(d) => Value::Datetime(if strip_utc {
                coerce_datetime(&d, true)
            } else {
                d
            }),
            None => Value::NaT,
        });
    }
    let tz = (!strip_utc).then_some(TimeZone::UTC);
    values_to_series("", &out, &DataType::Datetime(TimeUnit::Microseconds, tz))
        .map(ColumnData::Native)
        .ok_or_else(|| EnforceError::Raised("failed to build datetime column".into()))
}

/// `attempt_cast_to_geometry()` for a column (the GeoSeries branch).
pub fn series_attempt_cast_to_geometry(col: &Column) -> Result<Column, String> {
    let values = col.data.values();
    let mut out = col.clone();
    let geo_error = || "TypeError: Non geometry data passed to GeoSeries constructor".to_string();
    let per_value = |values: &[Value]| -> Result<Vec<Value>, String> {
        let casted: Vec<Value> = values.iter().map(attempt_cast_to_geometry).collect();
        if casted
            .iter()
            .any(|v| !v.is_na() && !matches!(v, Value::Geometry(_)))
        {
            return Err(geo_error());
        }
        Ok(casted.into_iter().map(na_to_null).collect())
    };
    let first = values.iter().find(|v| !v.is_na()).cloned();
    let new_values = match first {
        None => values.iter().map(|_| Value::Null).collect(),
        Some(Value::Geometry(_)) => {
            if values
                .iter()
                .all(|v| v.is_na() || matches!(v, Value::Geometry(_)))
            {
                values.into_iter().map(na_to_null).collect()
            } else {
                per_value(&values)?
            }
        }
        Some(Value::Bytes(b)) if geometry_is_gpkg(&b) => {
            let stripped: Option<Vec<Value>> = values
                .iter()
                .map(|v| match v {
                    Value::Bytes(b) => gpkg_wkb_to_wkb(b).ok().map(|(w, _, _)| Value::Bytes(w)),
                    _ => None,
                })
                .collect();
            match stripped {
                Some(wkbs) => from_wkb_all(&wkbs).map_or_else(|| per_value(&values), Ok)?,
                None => per_value(&values)?,
            }
        }
        Some(sample) => {
            let is_wkt = matches!(crate::cast::geometry_is_wkt(&sample), Some(true));
            let parsed = if is_wkt {
                values
                    .iter()
                    .map(|v| match v {
                        v if v.is_na() => Some(Value::Null),
                        Value::Str(s) => Geometry::from_wkt(s).ok().map(Value::Geometry),
                        _ => None,
                    })
                    .collect::<Option<Vec<Value>>>()
            } else {
                from_wkb_all(&values)
            };
            match parsed {
                Some(v) => v,
                None => per_value(&values)?,
            }
        }
    };
    out.data = ColumnData::Object(new_values);
    // A successful cast changes the authoritative Arrow field tag as well as the values.
    out.tags.dtype = Some("geometry".into());
    out.tags.db_type = None;
    Ok(out)
}

fn na_to_null(v: Value) -> Value {
    if v.is_na() { Value::Null } else { v }
}

fn from_wkb_all(values: &[Value]) -> Option<Vec<Value>> {
    values
        .iter()
        .map(|v| match v {
            v if v.is_na() => Some(Value::Null),
            Value::Bytes(b) => Geometry::from_wkb(b).ok().map(Value::Geometry),
            Value::Str(s) => Geometry::from_wkb_hex(s).ok().map(Value::Geometry),
            _ => None,
        })
        .collect()
}

/// Whether the value-by-value [`pandas_astype`] of a native column to `dtype` would return the
/// column unchanged, so the conversion can be skipped. Only the cases where every value maps to
/// itself qualify: the same dtype, no `NaN` in float columns (the slow path turns `NaN` into
/// null), no `UInt64` (values above `i64::MAX` fail there), and datetimes that are naive or UTC
/// and inside the range a [`DateTimeValue`] represents.
fn astype_is_identity(s: &Series, dtype: &DataType) -> bool {
    if s.dtype() != dtype {
        return false;
    }
    match dtype {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::Boolean
        | DataType::String
        | DataType::Binary => true,
        DataType::Float32 | DataType::Float64 => s.is_nan().map(|nan| !nan.any()).unwrap_or(false),
        DataType::Datetime(tu, tz) => {
            if tz.as_ref().is_some_and(|tz| *tz != TimeZone::UTC) {
                return false;
            }
            let Ok(phys) = s.to_physical_repr().into_owned().i64().cloned() else {
                return false;
            };
            [phys.min(), phys.max()]
                .into_iter()
                .flatten()
                .all(|v| DateTimeValue::from_timestamp(v, *tu, None).is_some())
        }
        _ => false,
    }
}

/// `Series.astype(target)` for the pandas dtype strings `enforce_dtypes` produces, with Arrow's
/// safe-cast semantics (lossy float → int casts fail).
pub fn pandas_astype(name: &str, data: &ColumnData, target: &str) -> Result<ColumnData, String> {
    if target == "object" {
        return Ok(ColumnData::Object(data.values()));
    }
    let Some(dtype) = polars_dtype_for_pandas(target) else {
        return Err(format!("Cannot cast to '{target}'"));
    };
    if let ColumnData::Native(s) = data
        && astype_is_identity(s, &dtype)
    {
        return Ok(ColumnData::Native(s.clone().with_name(name.into())));
    }
    let from_object = matches!(data, ColumnData::Object(_));
    // pandas' masked integer dtypes (`Int64`, `UInt8`, ...) refuse a fractional float or decimal
    // (`cannot safely cast non-equivalent object to int64`), unlike the Arrow-backed ones.
    let masked_int = target.starts_with(['I', 'U']) && !target.contains("pyarrow");
    let values = data.values();
    let err = |v: &Value| format!("Cannot cast {} '{}' to {target}", v.type_name(), v.py_str());
    let mut out = Vec::with_capacity(values.len());
    for v in &values {
        if v.is_na() {
            out.push(Value::Null);
            continue;
        }
        let converted = match (&dtype, v) {
            (d, Value::Int(i)) if d.is_integer() => Value::Int(*i),
            (d, Value::Bool(b)) if d.is_integer() => Value::Int(*b as i64),
            // Arrow's safe cast: a fractional or out-of-range float is "truncated" and raises
            // (`as i64` would saturate `1e20` to `i64::MAX`).
            // PARITY: an `object` column of floats or decimals cast to an Arrow integer type is
            // converted value by value and truncates toward zero (`Series([2.5], dtype=object)
            // .astype('int64[pyarrow]')` is `[2]`), while a NumPy float64 column raises.
            (d, Value::Float(f)) if d.is_integer() => {
                let f = if from_object && !masked_int {
                    f.trunc()
                } else {
                    *f
                };
                if (masked_int && f.fract() != 0.0) || !crate::cast::float_fits_i64(f) {
                    return Err(err(v));
                }
                Value::Int(f as i64)
            }
            (d, Value::Decimal(dec)) if d.is_integer() => {
                let f = dec.to_f64();
                let f = if masked_int { f } else { f.trunc() };
                if (masked_int && f.fract() != 0.0) || !crate::cast::float_fits_i64(f) {
                    return Err(err(v));
                }
                Value::Int(f as i64)
            }
            (d, Value::Str(s)) if d.is_integer() => {
                Value::Int(s.trim().parse().map_err(|_| err(v))?)
            }
            (d, Value::Int(i)) if d.is_float() => Value::Float(*i as f64),
            (d, Value::Float(f)) if d.is_float() => Value::Float(*f),
            (d, Value::Bool(b)) if d.is_float() => Value::Float(*b as i64 as f64),
            (d, Value::Decimal(dec)) if d.is_float() => Value::Float(dec.to_f64()),
            (d, Value::Str(s)) if d.is_float() => {
                Value::Float(s.trim().parse().map_err(|_| err(v))?)
            }
            (DataType::String, Value::Str(s)) => Value::Str(s.clone()),
            (DataType::String, Value::Bytes(b)) => {
                Value::Str(String::from_utf8(b.clone()).map_err(|_| err(v))?)
            }
            (DataType::String, Value::Decimal(d)) => Value::Str(d.to_plain_string()),
            (DataType::String, other) => Value::Str(other.py_str()),
            (DataType::Boolean, Value::Bool(b)) => Value::Bool(*b),
            (DataType::Boolean, Value::Int(i)) => Value::Bool(*i != 0),
            (DataType::Boolean, Value::Float(f)) => Value::Bool(*f != 0.0),
            (DataType::Boolean, Value::Str(s)) => match s.to_lowercase().as_str() {
                "true" | "1" => Value::Bool(true),
                "false" | "0" => Value::Bool(false),
                _ => return Err(err(v)),
            },
            (DataType::Binary, Value::Bytes(b)) => Value::Bytes(b.clone()),
            (DataType::Binary, Value::Str(s)) => Value::Bytes(s.as_bytes().to_vec()),
            (DataType::Date, Value::Date(d)) => Value::Date(*d),
            (DataType::Date, Value::Datetime(d)) => Value::Date(d.local().date()),
            (DataType::Date, Value::Str(s)) => Value::Date(
                parse_iso_datetime(s)
                    .map(|(n, _)| n.date())
                    .ok_or_else(|| err(v))?,
            ),
            (DataType::Datetime(_, tz), Value::Datetime(d)) => {
                Value::Datetime(coerce_datetime(d, tz.is_none()))
            }
            (DataType::Datetime(_, tz), Value::Str(s)) => {
                let d = crate::value::parse_datetime_value(s).ok_or_else(|| err(v))?;
                Value::Datetime(coerce_datetime(&d, tz.is_none()))
            }
            // `Series([1, 2]).astype('datetime64[us, UTC]')` reads integers (and truncated
            // floats) as ticks of the *target's* unit since the epoch (an integer datetime axis
            // that v4 enforces on the object path, e.g. `enforce: False` pipes).
            (DataType::Datetime(tu, tz), Value::Int(i)) => Value::Datetime(
                DateTimeValue::from_timestamp(*i, *tu, tz.as_ref().map(|_| TzInfo::Utc))
                    .ok_or_else(|| err(v))?,
            ),
            (DataType::Datetime(tu, tz), Value::Float(f))
                if f.is_finite() && crate::cast::float_fits_i64(f.trunc()) =>
            {
                Value::Datetime(
                    DateTimeValue::from_timestamp(
                        f.trunc() as i64,
                        *tu,
                        tz.as_ref().map(|_| TzInfo::Utc),
                    )
                    .ok_or_else(|| err(v))?,
                )
            }
            (DataType::Datetime(_, tz), Value::Date(d)) => {
                let dt = DateTimeValue::naive(d.and_hms_opt(0, 0, 0).expect("midnight"));
                Value::Datetime(coerce_datetime(&dt, tz.is_none()))
            }
            _ => return Err(err(v)),
        };
        out.push(converted);
    }
    values_to_series(name, &out, &dtype)
        .map(ColumnData::Native)
        .ok_or_else(|| format!("Cannot build '{target}' column"))
}

// ---------------------------------------------------------------------------------------------
// Pipe-level call path.

/// `Pipe.get_dtypes()`: merge the inferred (remote) dtypes with the configured ones, resolve
/// aliases, default the datetime axis to `datetime` and an autoincrement primary key to `int`.
///
/// PARITY: the datetime axis is forced to `datetime` unless it is explicitly configured, even
/// when the table reports another type (meerschaum/core/Pipe/_attributes.py, `get_dtypes`).
pub fn pipe_get_dtypes(
    configured: &IndexMap<String, String>,
    remote: &IndexMap<String, String>,
    columns: &IndexMap<String, String>,
    autoincrement: bool,
) -> IndexMap<String, String> {
    let mut patched = remote.clone();
    for (k, v) in configured {
        patched.insert(k.clone(), v.clone());
    }
    let mut out: IndexMap<String, String> = patched
        .into_iter()
        .filter(|(c, t)| !c.is_empty() && !t.is_empty())
        .map(|(c, t)| {
            let t = resolve_alias(&t).to_string();
            (c, t)
        })
        .collect();
    if let Some(dt_col) = columns.get("datetime").filter(|c| !c.is_empty())
        && !configured.contains_key(dt_col)
    {
        out.insert(dt_col.clone(), "datetime".into());
    }
    if let Some(primary) = columns.get("primary").filter(|c| !c.is_empty())
        && autoincrement
        && !out.contains_key(primary)
    {
        out.insert(primary.clone(), "int".into());
    }
    out
}

/// `Pipe.tzinfo is not None`: the datetime axis is UTC-aware.
pub fn pipe_is_tz_aware(
    columns: &IndexMap<String, String>,
    dtypes: &IndexMap<String, String>,
    autotime: bool,
    autotime_column: &str,
) -> bool {
    let dt_col = columns.get("datetime").filter(|c| !c.is_empty());
    let mut dt_typ = dt_col.map(|c| dtypes.get(c).cloned().unwrap_or_else(|| "datetime".into()));
    if autotime {
        dt_typ = Some(
            dtypes
                .get(autotime_column)
                .cloned()
                .unwrap_or_else(|| "datetime".into()),
        );
    }
    match dt_typ {
        Some(t) => t.to_lowercase().contains("utc") || t == "datetime",
        None => false,
    }
}

/// The pipe attributes `Pipe.enforce_dtypes` reads.
#[derive(Clone, Debug)]
pub struct PipeDtypesContext {
    /// `Pipe.get_dtypes(infer=True)` (see [`pipe_get_dtypes`]).
    pub dtypes: IndexMap<String, String>,
    /// `Pipe.get_dtypes(infer=False)`.
    pub explicit_dtypes: IndexMap<String, String>,
    /// `Pipe.enforce`.
    pub enforce: bool,
    /// `Pipe.tzinfo is not None` (see [`pipe_is_tz_aware`]).
    pub tz_aware: bool,
    /// `Pipe.mixed_numerics`.
    pub mixed_numerics: bool,
}

/// The input to [`pipe_enforce_dtypes`].
#[derive(Clone, Debug)]
pub enum PipeInput {
    Frame(Frame),
    /// A list of documents (`[{col: value}, ...]`).
    Docs(Vec<IndexMap<String, Value>>),
    /// A dict of lists (`{col: [values]}`).
    Columns(IndexMap<String, Vec<Value>>),
}

/// `Pipe.enforce_dtypes(df, enforce=True)`.
pub fn pipe_enforce_dtypes(
    input: PipeInput,
    ctx: &PipeDtypesContext,
) -> Result<Frame, EnforceError> {
    let enforce = ctx.enforce;
    let explicit = if enforce {
        ctx.explicit_dtypes.clone()
    } else {
        IndexMap::new()
    };
    let ignore_cols: Vec<String> = ctx
        .dtypes
        .iter()
        .filter(|(_, t)| !enforce || !are_dtypes_equal(t, "datetime"))
        .map(|(c, _)| c.clone())
        .collect();
    let df = match input {
        PipeInput::Frame(f) => f,
        PipeInput::Docs(docs) => crate::docs::parse_df_datetimes(
            &crate::docs::frame_from_docs(&docs)?,
            &ignore_cols,
            !ctx.tz_aware,
            false,
        ),
        PipeInput::Columns(cols) => crate::docs::parse_df_datetimes(
            &crate::docs::frame_from_columns(&cols)?,
            &ignore_cols,
            !ctx.tz_aware,
            false,
        ),
    };
    if ctx.dtypes.is_empty() {
        return Ok(df);
    }
    let (mut out, path) = enforce_dtypes_with_report(
        &df,
        &ctx.dtypes,
        &EnforceOptions {
            explicit_dtypes: explicit,
            coerce_numeric: ctx.mixed_numerics,
            coerce_timezone: enforce,
            strip_timezone: !ctx.tz_aware,
        },
    )?;
    for (name, dtype) in &ctx.dtypes {
        let Some(col) = out.column_mut(name) else {
            continue;
        };
        let norm = normalized(dtype);
        let all_values =
            |matches: fn(&Value) -> bool| col.data.values().iter().all(|v| v.is_na() || matches(v));
        let matches = match norm.as_str() {
            "json" => {
                path == EnforcePath::Native
                    || matches!(&path, EnforcePath::Subset(names) if names.contains(name))
                    || df.data(name).is_ok_and(|source| {
                        source.values().iter().all(|v| match v {
                            Value::Str(s) => serde_json::from_str::<serde_json::Value>(s).is_ok(),
                            _ => true,
                        })
                    })
            }
            t if t.starts_with("geometry") || t.starts_with("geography") => {
                all_values(|v| matches!(v, Value::Geometry(_)))
            }
            t if t.starts_with("numeric") => all_values(|v| matches!(v, Value::Decimal(_))),
            "bytes" => all_values(|v| matches!(v, Value::Bytes(_))),
            "uuid" => all_values(|v| matches!(v, Value::Uuid(_))),
            _ => are_dtypes_equal(&col.data.dtype_str(), dtype),
        };
        // Failed v4 casts leave the source column alone; do not label those as converted.
        if matches {
            col.tags.dtype = Some(dtype.clone());
            col.tags.db_type = None;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_to_float_rounds_like_python() {
        // `float(Decimal('0.78431372549019607843'))` is 0.7843137254901961; the i128 mantissa
        // exceeds 2^53, so the string path decides.
        assert_eq!(
            decimal_mantissa_to_f64(78_431_372_549_019_607_843, 20),
            0.784_313_725_490_196_1
        );
        assert_eq!(decimal_mantissa_to_f64(-15, 1), -1.5);
        assert_eq!(decimal_mantissa_to_f64(45_000_000, 4), 4500.0);
        let s = Int128Chunked::from_iter_options(
            "a".into(),
            [Some(78_431_372_549_019_607_843i128), None].into_iter(),
        )
        .into_decimal_unchecked(38, 20)
        .into_series();
        let out = native_cast(&s, &DataType::Float64).unwrap();
        assert_eq!(out.f64().unwrap().get(0), Some(0.784_313_725_490_196_1));
        assert_eq!(out.f64().unwrap().get(1), None);
    }

    #[test]
    fn numeric_precision_outside_polars_range_has_no_native_target() {
        // Polars decimals need 1 <= precision <= 38; v4's `polars.Decimal(0, 0)` cast raises and
        // leaves `numeric[0,0]` (an unbounded numeric) to the pandas path.
        assert_eq!(native_target("numeric[0,0]", false), None);
        assert_eq!(native_target("numeric[39,2]", false), None);
        // A scale above the precision is not a Polars decimal either ("scale must be less than or
        // equal to precision" in v4's cast).
        assert_eq!(native_target("numeric[5,10]", false), None);
        assert_eq!(
            native_target("numeric[5,5]", false),
            Some(DataType::Decimal(5, 5))
        );
        assert_eq!(
            native_target("numeric[1,0]", false),
            Some(DataType::Decimal(1, 0))
        );
        assert_eq!(
            native_target("numeric[38,10]", false),
            Some(DataType::Decimal(38, 10))
        );
    }

    #[test]
    fn coerce_timezone_reads_integers_as_microseconds() {
        // v4: `coerce_timezone(pd.Series([1791900000001]))[0]` is
        // `1970-01-21 17:45:00.000001+00:00`.
        let data = ColumnData::Object(vec![Value::Int(1_791_900_000_001)]);
        let out = series_coerce_timezone(&data, false).unwrap();
        match &out.values()[0] {
            Value::Datetime(d) => {
                assert_eq!(d.utc.to_string(), "1970-01-21 17:45:00.000001")
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn par_map_chunks_matches_sequential() {
        let items: Vec<u64> = (0..300_000).collect();
        let double = |chunk: &[u64]| Some(chunk.iter().map(|x| x * 2).collect::<Vec<_>>());
        assert_eq!(par_map_chunks(&items, double), double(&items));
        // A failure in any chunk fails the whole map.
        let fail_late = |chunk: &[u64]| {
            chunk
                .iter()
                .map(|x| (*x != 299_999).then_some(*x))
                .collect::<Option<Vec<_>>>()
        };
        assert_eq!(par_map_chunks(&items, fail_late), None);
    }

    #[test]
    fn str_to_datetime_parallel_matches_values() {
        let texts: Vec<String> = (0..200_000)
            .map(|i| format!("2024-01-01 00:{:02}:{:02}", (i / 60) % 60, i % 60))
            .collect();
        let ca = StringChunked::from_iter_values("dt".into(), texts.iter().map(String::as_str));
        let s = str_to_datetime(&ca, TimeUnit::Microseconds, Some(TimeZone::UTC)).unwrap();
        let phys = s.to_physical_repr().into_owned();
        let phys = phys.i64().unwrap();
        for (i, t) in texts.iter().enumerate().step_by(9_973) {
            let (naive, _) = parse_iso_datetime(t).unwrap();
            assert_eq!(phys.get(i), Some(naive.and_utc().timestamp_micros()), "{t}");
        }
        // One differently shaped value (no seconds) fails the whole column, as Polars does.
        let mut mixed = texts.clone();
        mixed[150_000] = "2024-01-01 00:00".to_string();
        let ca = StringChunked::from_iter_values("dt".into(), mixed.iter().map(String::as_str));
        assert!(str_to_datetime(&ca, TimeUnit::Microseconds, None).is_none());
    }
}
