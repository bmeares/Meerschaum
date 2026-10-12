//! Scalar casts and null handling from `meerschaum/utils/dtypes/__init__.py`:
//! `value_is_null`, `none_if_null`, `attempt_cast_to_numeric`, `attempt_cast_to_uuid`,
//! `attempt_cast_to_bytes`, `attempt_cast_to_geometry`, `coerce_timezone`, `to_datetime`,
//! `datetime_to_int` and `round_time`.

use chrono::{Duration, NaiveDate};

use crate::decimal::Decimal;
use crate::dtypes::PrecisionUnit;
use crate::geometry::{Geometry, geometry_is_gpkg, geometry_is_wkt_str, gpkg_wkb_to_wkb};
use crate::serialize::deserialize_bytes_string;
use crate::value::{DateTimeValue, TzInfo, Value, parse_datetime_value};

/// `value_is_null()`: `str(value).lower()` is a null-like string
/// ([`meerschaum_core::dtypes::value_is_null`] on `str(value)`).
pub fn value_is_null(value: &Value) -> bool {
    meerschaum_core::dtypes::value_is_null(&value.py_str())
}

/// `none_if_null()`.
pub fn none_if_null(value: &Value) -> Value {
    if value_is_null(value) {
        Value::Null
    } else {
        value.clone()
    }
}

/// `attempt_cast_to_numeric()`.
///
/// Returns `Err` only where v4 raises: an existing `Decimal` that cannot be quantized
/// (`quantize_decimal` runs outside the `try` for `Decimal` inputs).
///
/// PARITY: quantization requires `precision` and `scale` to be truthy, so `scale=0` disables it
/// (`attempt_cast_to_numeric(1.5, True, 10, 0)` returns `Decimal('1.5')`).
pub fn attempt_cast_to_numeric(
    value: &Value,
    quantize: bool,
    precision: Option<i64>,
    scale: Option<i64>,
) -> Result<Value, String> {
    let do_quantize = quantize && precision.unwrap_or(0) != 0 && scale.unwrap_or(0) != 0;
    if let Value::Decimal(d) = value {
        if do_quantize {
            return d
                .quantize(precision.unwrap_or(0), scale.unwrap_or(0))
                .map(Value::Decimal)
                .map_err(|e| format!("ValueError: {e}"));
        }
        return Ok(value.clone());
    }
    if value_is_null(value) {
        return Ok(Value::Decimal(Decimal::NaN));
    }
    let Ok(dec) = Decimal::parse(&value.py_str()) else {
        return Ok(value.clone());
    };
    if !do_quantize {
        return Ok(Value::Decimal(dec));
    }
    Ok(
        match dec.quantize(precision.unwrap_or(0), scale.unwrap_or(0)) {
            Ok(q) => Value::Decimal(q),
            Err(_) => value.clone(),
        },
    )
}

/// `uuid.UUID(str)` parsing rules.
pub fn parse_py_uuid(text: &str) -> Option<uuid::Uuid> {
    let hex = text.replace("urn:", "").replace("uuid:", "");
    let hex = hex.trim_matches(|c| c == '{' || c == '}').replace('-', "");
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    uuid::Uuid::parse_str(&hex).ok()
}

/// `attempt_cast_to_uuid()`.
pub fn attempt_cast_to_uuid(value: &Value) -> Value {
    if let Value::Uuid(_) = value {
        return value.clone();
    }
    if value_is_null(value) {
        return Value::Null;
    }
    match parse_py_uuid(&value.py_str()) {
        Some(u) => Value::Uuid(u),
        None => value.clone(),
    }
}

/// `attempt_cast_to_bytes()`.
pub fn attempt_cast_to_bytes(value: &Value) -> Value {
    if let Value::Bytes(_) = value {
        return value.clone();
    }
    if value_is_null(value) {
        return Value::Null;
    }
    match deserialize_bytes_string(&value.py_str(), false) {
        Ok(b) => Value::Bytes(b),
        Err(_) => value.clone(),
    }
}

/// `geometry_is_wkt()` for any value: `None` for non-`str`/`bytes`, `Some(false)` for bytes.
pub fn geometry_is_wkt(value: &Value) -> Option<bool> {
    match value {
        Value::Bytes(_) => Some(false),
        Value::Str(s) => geometry_is_wkt_str(s),
        _ => None,
    }
}

/// `attempt_cast_to_geometry()` for a single (non-Series) value.
pub fn attempt_cast_to_geometry(value: &Value) -> Value {
    match value {
        Value::Geometry(_) => return value.clone(),
        Value::Json(j) => {
            return Geometry::from_geojson(j)
                .map(Value::Geometry)
                .unwrap_or_else(|_| value.clone());
        }
        Value::Bytes(b) if geometry_is_gpkg(b) => {
            return gpkg_wkb_to_wkb(b)
                .ok()
                .and_then(|(wkb, _, _)| Geometry::from_wkb(&wkb).ok())
                .map(Value::Geometry)
                .unwrap_or_else(|| value.clone());
        }
        _ => {}
    }
    let parsed = match (value, geometry_is_wkt(value)) {
        (_, None) => return value.clone(),
        (Value::Str(s), Some(true)) => Geometry::from_wkt(s).ok(),
        (Value::Str(s), Some(false)) => Geometry::from_wkb_hex(s).ok(),
        (Value::Bytes(b), _) => Geometry::from_wkb(b).ok(),
        _ => None,
    };
    parsed.map(Value::Geometry).unwrap_or_else(|| value.clone())
}

/// Errors from timezone coercion.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CastError {
    #[error("'{0}' object has no attribute 'tzinfo'")]
    NoTzInfo(&'static str),
    #[error("{0}")]
    Invalid(String),
}

/// `coerce_timezone()` for a scalar: naive → UTC-aware (or left naive with `strip_utc`),
/// aware → converted to UTC (stripped with `strip_utc`). Strings are parsed first and returned
/// unchanged if unparseable; ints and `None` pass through.
pub fn coerce_timezone(value: &Value, strip_utc: bool) -> Result<Value, CastError> {
    let dt = match value {
        Value::Null => return Ok(Value::Null),
        Value::Int(_) => return Ok(value.clone()),
        Value::Str(s) => match parse_datetime_value(s) {
            Some(dt) => dt,
            None => return Ok(value.clone()),
        },
        Value::Datetime(dt) => dt.clone(),
        other => return Err(CastError::NoTzInfo(other.type_name())),
    };
    Ok(Value::Datetime(coerce_datetime(&dt, strip_utc)))
}

/// `coerce_timezone()` on a datetime value.
pub fn coerce_datetime(dt: &DateTimeValue, strip_utc: bool) -> DateTimeValue {
    match dt.tz {
        None => {
            if strip_utc {
                dt.clone()
            } else {
                DateTimeValue::utc(dt.utc)
            }
        }
        Some(_) => {
            if strip_utc {
                DateTimeValue::naive(dt.utc)
            } else {
                DateTimeValue::utc(dt.utc)
            }
        }
    }
}

/// Truncate a datetime to a precision unit (`Timestamp.as_unit`, rounding toward the floor).
pub fn truncate_to_unit(dt: &DateTimeValue, unit: PrecisionUnit) -> DateTimeValue {
    use chrono::Timelike;
    let nanos = dt.utc.nanosecond() % 1_000_000_000;
    let keep = match unit {
        PrecisionUnit::Nanosecond => nanos,
        PrecisionUnit::Microsecond => nanos / 1000 * 1000,
        PrecisionUnit::Millisecond => nanos / 1_000_000 * 1_000_000,
        _ => 0,
    };
    let utc = dt.utc.with_nanosecond(keep).unwrap_or(dt.utc);
    DateTimeValue {
        utc,
        tz: dt.tz.clone(),
    }
}

/// `to_datetime()` for a scalar (`as_pydatetime=True`).
///
/// PARITY: strings and `datetime`s go through `pandas.to_datetime(..., utc=True)`, so the
/// result is UTC-aware even with `coerce_utc=False`; unparseable strings are returned as-is.
pub fn to_datetime(
    value: &Value,
    coerce_utc: bool,
    precision_unit: Option<&str>,
) -> Result<Value, CastError> {
    let unit_str = precision_unit.unwrap_or("microsecond");
    let unit = PrecisionUnit::parse(unit_str)
        .filter(|u| {
            crate::dtypes::precision_time_unit(*u).is_some()
                || matches!(
                    u,
                    PrecisionUnit::Second
                        | PrecisionUnit::Minute
                        | PrecisionUnit::Hour
                        | PrecisionUnit::Day
                )
        })
        .ok_or_else(|| CastError::Invalid(format!("Invalid precision '{unit_str}'.")))?;
    match value {
        Value::Datetime(dt) => Ok(Value::Datetime(truncate_to_unit(
            &coerce_datetime(dt, false),
            unit,
        ))),
        Value::Str(s) => match parse_datetime_value(s) {
            Some(dt) => Ok(Value::Datetime(truncate_to_unit(
                &coerce_datetime(&dt, false),
                unit,
            ))),
            None => {
                if coerce_utc {
                    coerce_timezone(value, false)
                } else {
                    Ok(value.clone())
                }
            }
        },
        Value::Date(d) => Ok(Value::Datetime(DateTimeValue::utc(
            d.and_hms_opt(0, 0, 0).expect("midnight"),
        ))),
        Value::Null | Value::NA | Value::NaT => Ok(Value::NaT),
        other => Err(CastError::Invalid(format!(
            "Cannot convert {} to datetime",
            other.type_name()
        ))),
    }
}

/// `datetime_to_int()`.
///
/// PARITY: computed as `int(dt.timestamp() * scalar)` in floating point, as v4 does.
pub fn datetime_to_int(value: &Value, precision_unit: &str) -> Result<i64, CastError> {
    if let Value::Int(i) = value {
        return Ok(*i);
    }
    let unit = PrecisionUnit::parse(precision_unit)
        .ok_or_else(|| CastError::Invalid(format!("Unknown precision unit '{precision_unit}'.")))?;
    let dt = match coerce_timezone(value, false)? {
        Value::Datetime(dt) => dt,
        other => {
            return Err(CastError::Invalid(format!(
                "'{}' object has no attribute 'timestamp'",
                other.type_name()
            )));
        }
    };
    let micros = dt.timestamp_micros();
    let secs = micros as f64 / 1e6;
    Ok((secs * unit.scalar()).trunc() as i64)
}

/// Rounding direction for [`round_time`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoundTo {
    Down,
    Up,
    Closest,
}

impl RoundTo {
    pub fn parse(s: &str) -> Self {
        match s {
            "down" => RoundTo::Down,
            "up" => RoundTo::Up,
            _ => RoundTo::Closest,
        }
    }
}

/// `round_time()`: round a datetime to a multiple of `delta_micros` measured from
/// `datetime.min` (on the naive wall time; the timezone is kept).
pub fn round_time(dt: &DateTimeValue, delta_micros: i64, to: RoundTo) -> DateTimeValue {
    if delta_micros == 0 {
        return dt.clone();
    }
    let local = dt.local();
    let min = NaiveDate::from_ymd_opt(1, 1, 1)
        .expect("min date")
        .and_hms_opt(0, 0, 0)
        .expect("midnight");
    let delta = local - min;
    let total: i128 = delta.num_microseconds().unwrap_or(i64::MAX) as i128;
    let step = delta_micros as i128;
    let q = total.div_euclid(step);
    let r = total.rem_euclid(step);
    let n = match to {
        RoundTo::Down => q,
        RoundTo::Up => {
            if r == 0 {
                q
            } else {
                q + 1
            }
        }
        RoundTo::Closest => {
            if 2 * r >= step {
                q + 1
            } else {
                q
            }
        }
    };
    let adjustment = (n * step - total) as i64;
    DateTimeValue {
        utc: dt.utc + Duration::microseconds(adjustment),
        tz: dt.tz.clone(),
    }
}

/// Convert an aware datetime to a timezone (`Series.dt.tz_convert`).
pub fn tz_convert(dt: &DateTimeValue, tz: TzInfo) -> DateTimeValue {
    DateTimeValue {
        utc: dt.utc,
        tz: Some(tz),
    }
}
