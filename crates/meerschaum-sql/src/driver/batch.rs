//! Shared conversion from driver cells ([`SqlValue`]) to Arrow arrays.
//!
//! Each driver maps its column type names to a [`ColumnKind`] and extracts one [`SqlValue`] per
//! cell; this module turns a column of cells into an Arrow array, coercing mismatched cells
//! (int → float, anything → text, text → timestamp, …) instead of failing where a lossless
//! conversion exists.
//!
//! Fields are tagged with the shared [`meerschaum_core::dtypes::field_meta`] keys, which the
//! kernel's `Frame::from_record_batches` reads (see [`super::batches_to_frame`]).

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch,
    RecordBatchOptions, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

use meerschaum_core::dtypes::field_meta;

use super::SqlValue;
use crate::error::{Result, SqlError};

/// Field metadata key holding the database type name reported by the driver
/// (`meerschaum:db_type`, owned by [`meerschaum_core::dtypes::field_meta`]).
pub const META_DB_TYPE: &str = field_meta::DB_TYPE;
/// Field metadata key holding the Meerschaum dtype for special columns
/// (`json`, `bytes`, `uuid`, `geometry`, `numeric`).
pub const META_DTYPE: &str = field_meta::DTYPE;
/// Field metadata key holding the geometry encoding (`ewkb`, `mysql`).
pub const META_ENCODING: &str = field_meta::ENCODING;
/// The Arrow canonical extension-name metadata key.
pub const META_EXTENSION_NAME: &str = field_meta::EXTENSION_NAME;

/// Decimal precision used for `NUMERIC` / `DECIMAL` columns.
pub const DECIMAL_PRECISION: u8 = 38;
/// The largest decimal scale (`Decimal128` holds 38 digits).
pub const MAX_DECIMAL_SCALE: u32 = 38;
/// Scale used when the first batch of a decimal column holds no values.
pub const DEFAULT_DECIMAL_SCALE: u32 = 10;

/// How a geometry column's bytes are encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeometryEncoding {
    /// PostGIS binary output: extended WKB (SRID flag + SRID when set).
    Ewkb,
    /// MySQL internal format: 4-byte little-endian SRID followed by WKB.
    Mysql,
}

impl GeometryEncoding {
    fn as_str(self) -> &'static str {
        match self {
            GeometryEncoding::Ewkb => "ewkb",
            GeometryEncoding::Mysql => "mysql",
        }
    }
}

/// The Arrow representation chosen for a result column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnKind {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    /// `Decimal128(38, scale)`.
    Decimal,
    Utf8,
    /// JSON text (`Utf8` with `meerschaum:dtype=json`).
    Json,
    /// `Binary` with `meerschaum:dtype=bytes`.
    Bytes,
    /// `FixedSizeBinary(16)` with the `arrow.uuid` extension name.
    Uuid,
    /// `Binary` geometry with `meerschaum:dtype=geometry`.
    Geometry(GeometryEncoding),
    Date,
    Time,
    /// `Timestamp(Microsecond, None)`.
    Timestamp,
    /// `Timestamp(Microsecond, Some("UTC"))`.
    TimestampTz,
    /// Unknown driver type: the raw value bytes as `Binary`.
    Raw,
}

/// One planned output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPlan {
    pub name: String,
    pub kind: ColumnKind,
    /// The database type name reported by the driver.
    pub db_type: String,
    /// Decimal scale (only meaningful for [`ColumnKind::Decimal`]).
    pub scale: u32,
}

impl ColumnPlan {
    /// A new plan with the default decimal scale.
    pub fn new(name: impl Into<String>, kind: ColumnKind, db_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind,
            db_type: db_type.into(),
            scale: DEFAULT_DECIMAL_SCALE,
        }
    }

    /// The Arrow data type of this column.
    pub fn data_type(&self) -> DataType {
        match self.kind {
            ColumnKind::Boolean => DataType::Boolean,
            ColumnKind::Int8 => DataType::Int8,
            ColumnKind::Int16 => DataType::Int16,
            ColumnKind::Int32 => DataType::Int32,
            ColumnKind::Int64 => DataType::Int64,
            ColumnKind::UInt8 => DataType::UInt8,
            ColumnKind::UInt16 => DataType::UInt16,
            ColumnKind::UInt32 => DataType::UInt32,
            ColumnKind::UInt64 => DataType::UInt64,
            ColumnKind::Float32 => DataType::Float32,
            ColumnKind::Float64 => DataType::Float64,
            ColumnKind::Decimal => DataType::Decimal128(DECIMAL_PRECISION, self.scale as i8),
            ColumnKind::Utf8 | ColumnKind::Json => DataType::Utf8,
            ColumnKind::Bytes | ColumnKind::Geometry(_) | ColumnKind::Raw => DataType::Binary,
            ColumnKind::Uuid => DataType::FixedSizeBinary(16),
            ColumnKind::Date => DataType::Date32,
            ColumnKind::Time => DataType::Time64(TimeUnit::Microsecond),
            ColumnKind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            ColumnKind::TimestampTz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
            }
        }
    }

    /// The Arrow field, with `meerschaum:*` metadata.
    pub fn field(&self) -> Field {
        let mut meta = HashMap::new();
        meta.insert(META_DB_TYPE.to_string(), self.db_type.clone());
        match self.kind {
            ColumnKind::Json => {
                meta.insert(META_DTYPE.to_string(), "json".to_string());
            }
            ColumnKind::Bytes => {
                meta.insert(META_DTYPE.to_string(), "bytes".to_string());
            }
            ColumnKind::Uuid => {
                meta.insert(META_DTYPE.to_string(), "uuid".to_string());
                meta.insert(META_EXTENSION_NAME.to_string(), "arrow.uuid".to_string());
            }
            ColumnKind::Geometry(enc) => {
                meta.insert(META_DTYPE.to_string(), "geometry".to_string());
                meta.insert(META_ENCODING.to_string(), enc.as_str().to_string());
            }
            ColumnKind::Decimal => {
                meta.insert(META_DTYPE.to_string(), "numeric".to_string());
            }
            _ => {}
        }
        Field::new(&self.name, self.data_type(), true).with_metadata(meta)
    }
}

/// Build the schema for a list of planned columns.
pub fn schema_for(plans: &[ColumnPlan]) -> SchemaRef {
    Arc::new(Schema::new(
        plans.iter().map(ColumnPlan::field).collect::<Vec<_>>(),
    ))
}

/// Set each decimal column's scale to the largest scale among its (first-batch) cells.
pub fn resolve_decimal_scales(plans: &mut [ColumnPlan], columns: &[Vec<SqlValue>]) {
    for (plan, cells) in plans.iter_mut().zip(columns) {
        if plan.kind != ColumnKind::Decimal {
            continue;
        }
        let mut scale: Option<u32> = None;
        for cell in cells {
            if let Ok(Some((_, s))) = to_decimal_parts(cell) {
                scale = Some(scale.unwrap_or(0).max(s));
            }
        }
        plan.scale = scale
            .unwrap_or(DEFAULT_DECIMAL_SCALE)
            .min(MAX_DECIMAL_SCALE);
    }
}

/// Build one record batch from column-major cells.
pub fn build_batch(
    schema: &SchemaRef,
    plans: &[ColumnPlan],
    columns: Vec<Vec<SqlValue>>,
    num_rows: usize,
) -> Result<RecordBatch> {
    let arrays = plans
        .iter()
        .zip(columns.iter())
        .map(|(plan, cells)| build_array(plan, cells))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

fn conv_err(plan: &ColumnPlan, cell: &SqlValue, target: &str) -> SqlError {
    SqlError::Conversion {
        column: plan.name.clone(),
        reason: format!("cannot convert {cell:?} to {target}"),
    }
}

fn collect<T>(
    plan: &ColumnPlan,
    cells: &[SqlValue],
    target: &str,
    f: impl Fn(&SqlValue) -> std::result::Result<Option<T>, ()>,
) -> Result<Vec<Option<T>>> {
    cells
        .iter()
        .map(|c| f(c).map_err(|_| conv_err(plan, c, target)))
        .collect()
}

fn narrow<T: TryFrom<i64>>(v: Option<i64>) -> std::result::Result<Option<T>, ()> {
    v.map(|x| T::try_from(x).map_err(|_| ())).transpose()
}

fn narrow_u<T: TryFrom<u64>>(v: Option<u64>) -> std::result::Result<Option<T>, ()> {
    v.map(|x| T::try_from(x).map_err(|_| ())).transpose()
}

/// Convert one column of cells into an Arrow array of the plan's type.
pub fn build_array(plan: &ColumnPlan, cells: &[SqlValue]) -> Result<ArrayRef> {
    Ok(match plan.kind {
        ColumnKind::Boolean => Arc::new(BooleanArray::from(collect(plan, cells, "bool", to_bool)?)),
        ColumnKind::Int8 => Arc::new(Int8Array::from(collect(plan, cells, "i8", |c| {
            narrow(to_i64(c)?)
        })?)),
        ColumnKind::Int16 => Arc::new(Int16Array::from(collect(plan, cells, "i16", |c| {
            narrow(to_i64(c)?)
        })?)),
        ColumnKind::Int32 => Arc::new(Int32Array::from(collect(plan, cells, "i32", |c| {
            narrow(to_i64(c)?)
        })?)),
        ColumnKind::Int64 => Arc::new(Int64Array::from(collect(plan, cells, "i64", to_i64)?)),
        ColumnKind::UInt8 => Arc::new(UInt8Array::from(collect(plan, cells, "u8", |c| {
            narrow_u(to_u64(c)?)
        })?)),
        ColumnKind::UInt16 => Arc::new(UInt16Array::from(collect(plan, cells, "u16", |c| {
            narrow_u(to_u64(c)?)
        })?)),
        ColumnKind::UInt32 => Arc::new(UInt32Array::from(collect(plan, cells, "u32", |c| {
            narrow_u(to_u64(c)?)
        })?)),
        ColumnKind::UInt64 => Arc::new(UInt64Array::from(collect(plan, cells, "u64", to_u64)?)),
        ColumnKind::Float32 => Arc::new(Float32Array::from(collect(plan, cells, "f32", |c| {
            Ok(to_f64(c)?.map(|f| f as f32))
        })?)),
        ColumnKind::Float64 => Arc::new(Float64Array::from(collect(plan, cells, "f64", to_f64)?)),
        ColumnKind::Decimal => {
            let scale = plan.scale;
            let values = collect(plan, cells, "decimal", |c| {
                let Some((m, s)) = to_decimal_parts(c)? else {
                    return Ok(None);
                };
                // Later batches may carry more fractional digits than the first batch that
                // fixed the schema; round half-even to the schema's scale.
                rescale_mantissa(m, s, scale).map(Some).ok_or(())
            })?;
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(DECIMAL_PRECISION, scale as i8)?,
            )
        }
        ColumnKind::Utf8 | ColumnKind::Json => {
            Arc::new(StringArray::from(collect(plan, cells, "text", to_text)?))
        }
        ColumnKind::Bytes | ColumnKind::Geometry(_) | ColumnKind::Raw => {
            let values = collect(plan, cells, "bytes", to_bytes)?;
            Arc::new(BinaryArray::from_iter(values))
        }
        ColumnKind::Uuid => {
            let values = collect(plan, cells, "uuid", to_uuid)?;
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                values.into_iter(),
                16,
            )?)
        }
        ColumnKind::Date => {
            let values = collect(plan, cells, "date", |c| {
                Ok(to_date(c)?.map(|d| {
                    (d - NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch")).num_days() as i32
                }))
            })?;
            Arc::new(Date32Array::from(values))
        }
        ColumnKind::Time => {
            let values = collect(plan, cells, "time", |c| {
                Ok(to_time(c)?.map(|t| {
                    t.num_seconds_from_midnight() as i64 * 1_000_000
                        + (t.nanosecond() % 1_000_000_000) as i64 / 1000
                }))
            })?;
            Arc::new(Time64MicrosecondArray::from(values))
        }
        ColumnKind::Timestamp => {
            let values = collect(plan, cells, "timestamp", |c| {
                Ok(to_naive_utc(c)?.map(|dt| dt.and_utc().timestamp_micros()))
            })?;
            Arc::new(TimestampMicrosecondArray::from(values))
        }
        ColumnKind::TimestampTz => {
            let values = collect(plan, cells, "timestamptz", |c| {
                Ok(to_naive_utc(c)?.map(|dt| dt.and_utc().timestamp_micros()))
            })?;
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"))
        }
    })
}

type Conv<T> = std::result::Result<Option<T>, ()>;

fn to_i64(c: &SqlValue) -> Conv<i64> {
    Ok(Some(match c {
        SqlValue::Null => return Ok(None),
        SqlValue::Int(i) => *i,
        SqlValue::UInt(u) => i64::try_from(*u).map_err(|_| ())?,
        SqlValue::Bool(b) => *b as i64,
        SqlValue::Float(f) if f.fract() == 0.0 && f.is_finite() => *f as i64,
        SqlValue::Decimal(d) if d.fract().is_zero() => d.to_i64().ok_or(())?,
        SqlValue::Text(s) => s.trim().parse().map_err(|_| ())?,
        _ => return Err(()),
    }))
}

fn to_u64(c: &SqlValue) -> Conv<u64> {
    Ok(Some(match c {
        SqlValue::Null => return Ok(None),
        SqlValue::UInt(u) => *u,
        SqlValue::Int(i) => u64::try_from(*i).map_err(|_| ())?,
        SqlValue::Bool(b) => *b as u64,
        SqlValue::Float(f) if f.fract() == 0.0 && *f >= 0.0 && f.is_finite() => *f as u64,
        SqlValue::Decimal(d) if d.fract().is_zero() => d.to_u64().ok_or(())?,
        SqlValue::Text(s) => s.trim().parse().map_err(|_| ())?,
        _ => return Err(()),
    }))
}

fn to_f64(c: &SqlValue) -> Conv<f64> {
    Ok(Some(match c {
        SqlValue::Null => return Ok(None),
        SqlValue::Float(f) => *f,
        SqlValue::Int(i) => *i as f64,
        SqlValue::UInt(u) => *u as f64,
        SqlValue::Bool(b) => *b as i64 as f64,
        SqlValue::Decimal(d) => d.to_f64().ok_or(())?,
        SqlValue::Text(s) => s.trim().parse().map_err(|_| ())?,
        _ => return Err(()),
    }))
}

fn to_bool(c: &SqlValue) -> Conv<bool> {
    Ok(Some(match c {
        SqlValue::Null => return Ok(None),
        SqlValue::Bool(b) => *b,
        SqlValue::Int(i) => *i != 0,
        SqlValue::UInt(u) => *u != 0,
        SqlValue::Float(f) => *f != 0.0,
        SqlValue::Decimal(d) => !d.is_zero(),
        SqlValue::Text(s) => match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "t" | "yes" | "y" => true,
            "0" | "false" | "f" | "no" | "n" => false,
            _ => return Err(()),
        },
        _ => return Err(()),
    }))
}

/// Parse decimal text (`-12.3400`, `1E+3`, `5e-2`) into `(mantissa, scale)` with up to 38
/// significant digits (wider than `rust_decimal`'s 28, matching `Decimal128`).
pub fn parse_decimal_text(s: &str) -> Option<(i128, u32)> {
    let s = s.trim();
    let (neg, rest) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (num, exp) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i32>().ok()?),
        None => (rest, 0),
    };
    let (int_part, frac_part) = match num.split_once('.') {
        Some((a, b)) => (a, b),
        None => (num, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part
        .chars()
        .chain(frac_part.chars())
        .all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    let digits = digits.trim_start_matches('0');
    let mut scale = frac_part.len() as i32 - exp;
    let mut mantissa: i128 = if digits.is_empty() {
        0
    } else {
        if digits.len() > 38 {
            return None;
        }
        digits.parse().ok()?
    };
    if scale < 0 {
        mantissa = mantissa.checked_mul(10i128.checked_pow((-scale) as u32)?)?;
        scale = 0;
    }
    Some((if neg { -mantissa } else { mantissa }, scale as u32))
}

/// Rescale a decimal mantissa from `from` to `to` fractional digits (half-even rounding when
/// dropping digits). `None` on overflow.
pub fn rescale_mantissa(m: i128, from: u32, to: u32) -> Option<i128> {
    if from == to {
        return Some(m);
    }
    if from < to {
        return m.checked_mul(10i128.checked_pow(to - from)?);
    }
    let div = 10i128.checked_pow(from - to)?;
    let q = m / div;
    let r = m % div;
    let twice = r.abs().checked_mul(2)?;
    let round_away = twice > div || (twice == div && q % 2 != 0);
    Some(if round_away { q + m.signum() } else { q })
}

/// A cell as a `(mantissa, scale)` decimal (`NUMERIC` columns).
pub(crate) fn to_decimal_parts(c: &SqlValue) -> Conv<(i128, u32)> {
    Ok(Some(match c {
        SqlValue::Null => return Ok(None),
        SqlValue::Decimal(d) => (d.mantissa(), d.scale()),
        SqlValue::Text(s) => parse_decimal_text(s).ok_or(())?,
        other => {
            let Some(d) = to_decimal(other)? else {
                return Ok(None);
            };
            (d.mantissa(), d.scale())
        }
    }))
}

/// Convert a cell to a `Decimal` (used for `NUMERIC` columns).
pub(crate) fn to_decimal(c: &SqlValue) -> Conv<Decimal> {
    Ok(Some(match c {
        SqlValue::Null => return Ok(None),
        SqlValue::Decimal(d) => *d,
        SqlValue::Int(i) => Decimal::from(*i),
        SqlValue::UInt(u) => Decimal::from(*u),
        SqlValue::Bool(b) => Decimal::from(*b as i64),
        SqlValue::Float(f) => Decimal::from_f64(*f).ok_or(())?,
        SqlValue::Text(s) => {
            let s = s.trim();
            Decimal::from_str_exact(s)
                .or_else(|_| Decimal::from_scientific(s))
                .map_err(|_| ())?
        }
        _ => return Err(()),
    }))
}

fn to_text(c: &SqlValue) -> Conv<String> {
    Ok(match c {
        SqlValue::Null => None,
        SqlValue::Text(s) | SqlValue::Json(s) => Some(s.clone()),
        SqlValue::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        other => Some(other.to_string()),
    })
}

fn to_bytes(c: &SqlValue) -> Conv<Vec<u8>> {
    Ok(match c {
        SqlValue::Null => None,
        SqlValue::Bytes(b) => Some(b.clone()),
        SqlValue::Text(s) | SqlValue::Json(s) => Some(s.as_bytes().to_vec()),
        SqlValue::Uuid(u) => Some(u.as_bytes().to_vec()),
        _ => return Err(()),
    })
}

fn to_uuid(c: &SqlValue) -> Conv<[u8; 16]> {
    Ok(Some(match c {
        SqlValue::Null => return Ok(None),
        SqlValue::Uuid(u) => *u.as_bytes(),
        SqlValue::Text(s) => *uuid::Uuid::from_str(s.trim()).map_err(|_| ())?.as_bytes(),
        SqlValue::Bytes(b) if b.len() == 16 => {
            let mut out = [0u8; 16];
            out.copy_from_slice(b);
            out
        }
        _ => return Err(()),
    }))
}

fn to_date(c: &SqlValue) -> Conv<NaiveDate> {
    Ok(Some(match c {
        SqlValue::Null => return Ok(None),
        SqlValue::Date(d) => *d,
        SqlValue::Timestamp(dt) => dt.date(),
        SqlValue::TimestampTz(dt) => dt.date_naive(),
        SqlValue::Text(s) => {
            let s = s.trim();
            match NaiveDate::parse_from_str(s.get(..10).unwrap_or(s), "%Y-%m-%d") {
                Ok(d) => d,
                Err(_) => parse_datetime_text(s).ok_or(())?.0.date(),
            }
        }
        SqlValue::Int(_) | SqlValue::Float(_) => to_naive_utc(c)?.ok_or(())?.date(),
        _ => return Err(()),
    }))
}

fn to_time(c: &SqlValue) -> Conv<NaiveTime> {
    Ok(Some(match c {
        SqlValue::Null => return Ok(None),
        SqlValue::Time(t) => *t,
        SqlValue::Timestamp(dt) => dt.time(),
        SqlValue::TimestampTz(dt) => dt.time(),
        SqlValue::Text(s) => NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f")
            .or_else(|_| NaiveTime::parse_from_str(s.trim(), "%H:%M"))
            .map_err(|_| ())?,
        _ => return Err(()),
    }))
}

/// Convert a cell to a naive UTC timestamp. Timezone-aware values are converted to UTC;
/// naive values are taken as UTC (Meerschaum stores naive timestamps in UTC).
fn to_naive_utc(c: &SqlValue) -> Conv<NaiveDateTime> {
    Ok(Some(match c {
        SqlValue::Null => return Ok(None),
        SqlValue::Timestamp(dt) => *dt,
        SqlValue::TimestampTz(dt) => dt.naive_utc(),
        SqlValue::Date(d) => d.and_hms_opt(0, 0, 0).ok_or(())?,
        SqlValue::Text(s) => {
            let (naive, offset) = parse_datetime_text(s).ok_or(())?;
            match offset {
                Some(off) => off
                    .from_local_datetime(&naive)
                    .single()
                    .ok_or(())?
                    .naive_utc(),
                None => naive,
            }
        }
        // SQLite stores timestamps as unix seconds (INTEGER) or Julian days (REAL), matching
        // sqlx's SQLite chrono decoding.
        SqlValue::Int(i) => DateTime::<Utc>::from_timestamp(*i, 0)
            .ok_or(())?
            .naive_utc(),
        SqlValue::Float(jd) => {
            let micros = ((jd - 2_440_587.5) * 86_400_000_000.0).round() as i64;
            DateTime::<Utc>::from_timestamp_micros(micros)
                .ok_or(())?
                .naive_utc()
        }
        _ => return Err(()),
    }))
}

/// Parse common SQL timestamp text: `YYYY-MM-DD[ T]HH:MM[:SS[.ffffff]][Z|±HH[:MM]]` or a bare
/// date. Returns the local time and the offset, if one was present.
pub fn parse_datetime_text(s: &str) -> Option<(NaiveDateTime, Option<FixedOffset>)> {
    let s = s.trim();
    let (body, zulu) = match s.strip_suffix('Z').or_else(|| s.strip_suffix('z')) {
        Some(b) => (b, true),
        None => (s, false),
    };
    if zulu {
        let naive = parse_naive(body)?;
        return Some((naive, FixedOffset::east_opt(0)));
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%dT%H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M%#z",
        "%Y-%m-%dT%H:%M%#z",
    ] {
        if let Ok(dt) = DateTime::parse_from_str(body, fmt) {
            return Some((dt.naive_local(), Some(*dt.offset())));
        }
    }
    parse_naive(body).map(|n| (n, None))
}

fn parse_naive(s: &str) -> Option<NaiveDateTime> {
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(dt);
        }
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
}

/// Whether a text cell carries an explicit UTC offset.
pub fn text_has_offset(s: &str) -> bool {
    matches!(parse_datetime_text(s), Some((_, Some(_))))
}

#[cfg(test)]
mod decimal_tests {
    use super::*;

    #[test]
    fn wide_decimals() {
        assert_eq!(parse_decimal_text("-12.3400"), Some((-123400, 4)));
        assert_eq!(parse_decimal_text("1E+3"), Some((1000, 0)));
        assert_eq!(parse_decimal_text("5e-2"), Some((5, 2)));
        assert_eq!(
            parse_decimal_text("123456789012345678.12345678901234567890"),
            Some((12345678901234567812345678901234567890, 20))
        );
        assert_eq!(parse_decimal_text("abc"), None);
        assert_eq!(rescale_mantissa(125, 2, 1), Some(12));
        assert_eq!(rescale_mantissa(135, 2, 1), Some(14));
        assert_eq!(rescale_mantissa(-135, 2, 1), Some(-14));
        assert_eq!(rescale_mantissa(5, 0, 3), Some(5000));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn parses_timestamp_text() {
        let (n, o) = parse_datetime_text("2024-01-02 03:04:05.123456+00:00").unwrap();
        assert_eq!(n.to_string(), "2024-01-02 03:04:05.123456");
        assert_eq!(o.unwrap().local_minus_utc(), 0);
        let (n, o) = parse_datetime_text("2024-01-02T03:04:05").unwrap();
        assert_eq!(n.to_string(), "2024-01-02 03:04:05");
        assert!(o.is_none());
        assert!(parse_datetime_text("2024-01-02").is_some());
        assert!(text_has_offset("2024-01-02 00:00:00Z"));
        assert!(text_has_offset("2024-01-02 00:00:00-0500"));
        assert!(parse_datetime_text("nope").is_none());
    }

    #[test]
    fn builds_decimal_with_scale() {
        let mut plans = vec![ColumnPlan::new("d", ColumnKind::Decimal, "NUMERIC")];
        let cells = vec![
            SqlValue::Decimal(Decimal::from_str("1.5").unwrap()),
            SqlValue::Null,
            SqlValue::Decimal(Decimal::from_str("-2.125").unwrap()),
        ];
        resolve_decimal_scales(&mut plans, std::slice::from_ref(&cells));
        assert_eq!(plans[0].scale, 3);
        let arr = build_array(&plans[0], &cells).unwrap();
        let arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(arr.value_as_string(0), "1.500");
        assert!(arr.is_null(1));
        assert_eq!(arr.value_as_string(2), "-2.125");
    }
}
