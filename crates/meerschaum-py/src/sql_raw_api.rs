//! Raw SQL through the Rust connector (rc15 Q50 / Q53): what `SQLConnector.read`, `exec`,
//! `exec_queries`, `value` and `to_sql` call instead of SQLAlchemy.
//!
//! The façade (`meerschaum/connectors/sql/_native.py`) turns the caller's statement into text
//! with positional placeholders and a list of values; this module binds the values
//! ([`py_to_param`]), runs the statement on the instance's pool ([`SqlConnector::sql_query`],
//! [`SqlConnector::sql_execute`], ... in `sql_api.rs`) or in an open transaction ([`SqlTx`]), and
//! hands rows back as [`SqlFrame`]s: one batch of Arrow columns each.
//!
//! A [`SqlFrame`] keeps what each flavor's Python driver (v4's DBAPI: `sqlite3`, psycopg,
//! PyMySQL, pyodbc, python-oracledb, duckdb) returns ([`normalize_batch`] fixes the few columns
//! where the Rust driver's types differ), so:
//!
//! - [`SqlFrame::rows`] are the tuples a DBAPI cursor's `fetchall()` returns (`int`, `float`,
//!   `Decimal`, `str`, `bytes`, `bool`, `datetime` naive or aware, `date`, `time`, `timedelta`,
//!   `dict` for PostgreSQL JSON, `UUID`, `None`),
//! - [`SqlFrame::to_pandas`] is the frame v4's `pd.read_sql_query()` builds from those values
//!   (pandas' own inference: `int64`, `float64` for integers with nulls or decimals with
//!   `coerce_float`, `bool`, `str`, `datetime64[us(, UTC)]`, `timedelta64[us]`, `object` with
//!   `None` for nulls, every column `object` when there are no rows), and
//! - `__arrow_c_stream__` exports the batch for Polars and other Arrow consumers
//!   (`meerschaum.utils.dataframe.frame_from_arrow`).

use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch, RecordBatchOptions,
    StringArray,
};
use arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, Schema, TimeUnit, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use indexmap::IndexMap;
use meerschaum_connector_sql::driver::batch::{META_DB_TYPE, META_DTYPE, META_EXTENSION_NAME};
use meerschaum_connector_sql::driver::raw::{
    META_SQLITE_CLASSES, RawChunks, error_kind, error_message,
};
use meerschaum_connector_sql::driver::{SqlParam, SqlTransaction};
use meerschaum_connector_sql::facade::IfExists;
use meerschaum_connector_sql::{Flavor, SqlConnector as RsSqlConnector, SqlError};
use meerschaum_core::dtypes::field_meta;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};

use crate::arrow_bridge::ArrowStream;
use crate::convert::{PyCtx, import_optional};
use crate::runtime::block_on;

pyo3::create_exception!(
    meerschaum._core,
    SqlDatabaseError,
    PyException,
    "A database error from raw SQL through the Rust connector: `args` are `(message, kind, \
     sqlstate)`, `kind` one of PEP 249's exception names (`OperationalError`, \
     `ProgrammingError`, `IntegrityError`, `DataError`, `NotSupportedError`, `InternalError`, \
     `DatabaseError`)."
);

/// A raw SQL error as `_core.SqlDatabaseError(message, kind, sqlstate)`.
pub fn sql_err(e: SqlError, flavor: Flavor) -> PyErr {
    let (kind, code) = error_kind(&e, flavor);
    SqlDatabaseError::new_err((error_message(&e), kind, code))
}

/// One bound value from a Python object: `None`, `bool`, `int` (text beyond 64 bits), `float`,
/// `str`, `bytes`, `datetime` (naive, or aware as UTC), `date`, `dict` / `list` (JSON) and
/// NumPy scalars (`.item()`); anything else binds as its `str()` (`Decimal`, `UUID`, `time`).
pub fn py_to_param(ctx: &PyCtx<'_>, obj: &Bound<'_, PyAny>) -> PyResult<SqlParam> {
    let py = ctx.py;
    if obj.is_none() {
        return Ok(SqlParam::Null);
    }
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(SqlParam::Bool(b.is_true()));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(match obj.extract::<i64>() {
            Ok(i) => SqlParam::Int(i),
            Err(_) => SqlParam::Text(obj.str()?.to_string()),
        });
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(SqlParam::Float(obj.extract::<f64>()?));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok(SqlParam::Text(s.to_str()?.to_string()));
    }
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok(SqlParam::Bytes(b.as_bytes().to_vec()));
    }
    if let Ok(b) = obj.cast::<PyByteArray>() {
        return Ok(SqlParam::Bytes(b.to_vec()));
    }
    if obj.is_instance(&ctx.datetime_cls)? {
        let naive = naive_datetime_of(obj)?;
        let offset = obj.call_method0(intern!(py, "utcoffset"))?;
        if offset.is_none() {
            return Ok(SqlParam::Timestamp(naive));
        }
        let seconds: f64 = offset
            .call_method0(intern!(py, "total_seconds"))?
            .extract()?;
        let micros = (seconds * 1e6).round() as i64;
        let utc = naive - chrono::Duration::microseconds(micros);
        return Ok(SqlParam::TimestampTz(utc.and_utc()));
    }
    if obj.is_instance(&ctx.date_cls)? {
        let (y, m, d): (i32, u32, u32) = (
            obj.getattr(intern!(py, "year"))?.extract()?,
            obj.getattr(intern!(py, "month"))?.extract()?,
            obj.getattr(intern!(py, "day"))?.extract()?,
        );
        return NaiveDate::from_ymd_opt(y, m, d)
            .map(SqlParam::Date)
            .ok_or_else(|| PyValueError::new_err(format!("Invalid date: {obj}")));
    }
    if obj.cast::<PyDict>().is_ok() || obj.cast::<PyList>().is_ok() {
        let json = py.import("json")?.call_method1("dumps", (obj,))?;
        let text: String = json.extract()?;
        return Ok(SqlParam::Json(
            serde_json::from_str(&text).map_err(|e| PyValueError::new_err(e.to_string()))?,
        ));
    }
    // NumPy / pandas scalars (`numpy.int64`, `numpy.bool_`, ...): their Python value.
    if obj
        .get_type()
        .getattr(intern!(py, "__module__"))
        .ok()
        .and_then(|m| m.extract::<String>().ok())
        .is_some_and(|m| m.starts_with("numpy"))
        && let Ok(item) = obj.call_method0(intern!(py, "item"))
        && !item.is(obj)
    {
        return py_to_param(ctx, &item);
    }
    Ok(SqlParam::Text(obj.str()?.to_string()))
}

/// The wall-clock fields of a Python `datetime`.
fn naive_datetime_of(obj: &Bound<'_, PyAny>) -> PyResult<NaiveDateTime> {
    let py = obj.py();
    let get = |name: &str| -> PyResult<u32> { obj.getattr(name)?.extract() };
    let year: i32 = obj.getattr(intern!(py, "year"))?.extract()?;
    NaiveDate::from_ymd_opt(year, get("month")?, get("day")?)
        .and_then(|d| {
            d.and_hms_micro_opt(
                get("hour").ok()?,
                get("minute").ok()?,
                get("second").ok()?,
                get("microsecond").ok()?,
            )
        })
        .ok_or_else(|| PyValueError::new_err(format!("Invalid datetime: {obj}")))
}

/// Bound values from a Python sequence (`None` is no values).
pub fn params_from_py(
    ctx: &PyCtx<'_>,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<SqlParam>> {
    let Some(params) = params.filter(|p| !p.is_none()) else {
        return Ok(Vec::new());
    };
    params
        .try_iter()?
        .map(|item| py_to_param(ctx, &item?))
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Normalization: what each flavor's Python driver returns

fn field_meta_get<'a>(field: &'a Field, key: &str) -> Option<&'a str> {
    field.metadata().get(key).map(String::as_str)
}

/// A copy of `field` with a new type and without the given metadata keys.
fn retyped(field: &Field, data_type: DataType, drop: &[&str], add: &[(&str, &str)]) -> Field {
    let mut md = field.metadata().clone();
    for key in drop {
        md.remove(key);
    }
    for (k, v) in add {
        md.insert(k.to_string(), v.to_string());
    }
    Field::new(field.name(), data_type, true).with_metadata(md)
}

/// Python's `repr(float(x))` for a single-precision value: PyMySQL reads `FLOAT` columns from
/// their text (`1.1`), not as the widened `f32` (`1.100000023841858`).
fn f32_shortest(x: f32) -> f64 {
    format!("{x}").parse::<f64>().unwrap_or(x as f64)
}

/// The batch with each column holding what the flavor's Python driver returns where the Rust
/// driver's Arrow type differs:
///
/// - MySQL / MariaDB (PyMySQL): `TINYINT(1)` / `BOOLEAN` are `int`, `FLOAT` is read from its
///   text, `TIMESTAMP` is naive, `TIME` is a `timedelta`, `JSON` is text;
/// - PostgreSQL (psycopg, text results): `real` is read from its text;
/// - Oracle (python-oracledb + SQLAlchemy's output handler): `BINARY_FLOAT` is a `Decimal` of
///   the widened value, `TIMESTAMP WITH TIME ZONE` is naive;
/// - PostGIS (psycopg, no geometry adapter): `geometry` / `geography` are their hex EWKB text;
/// - MSSQL (pyodbc): `UNIQUEIDENTIFIER` is uppercase text;
/// - DuckDB: `JSON` is text.
pub fn normalize_batch(batch: RecordBatch, flavor: Flavor) -> PyResult<RecordBatch> {
    use meerschaum_connector_sql::flavor::DriverFamily;
    let family = flavor.driver_family();
    let schema = batch.schema();
    let mut fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let mut changed = false;
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        let tag = field_meta_get(field, META_DTYPE);
        let out: Option<(Field, ArrayRef)> = match (family, field.data_type()) {
            (DriverFamily::Mysql, DataType::Boolean) => {
                let a = array.as_boolean();
                let ints: Int64Array = a.iter().map(|v| v.map(i64::from)).collect();
                Some((retyped(field, DataType::Int64, &[], &[]), Arc::new(ints)))
            }
            // PARITY: psycopg reads results in text format, so `real` values are their text
            // (`1.1`), as PyMySQL reads `FLOAT`.
            (DriverFamily::Mysql | DriverFamily::Postgres, DataType::Float32) => {
                let a = array.as_primitive::<Float32Type>();
                let floats: Float64Array = a.iter().map(|v| v.map(f32_shortest)).collect();
                Some((
                    retyped(field, DataType::Float64, &[], &[]),
                    Arc::new(floats),
                ))
            }
            (DriverFamily::Mysql | DriverFamily::Oracle, DataType::Timestamp(unit, Some(_))) => {
                let naive = arrow::compute::cast(array, &DataType::Timestamp(*unit, None))
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                Some((
                    retyped(field, DataType::Timestamp(*unit, None), &[], &[]),
                    naive,
                ))
            }
            (DriverFamily::Mysql, DataType::Time64(TimeUnit::Microsecond)) => {
                let a = array.as_primitive::<arrow::datatypes::Time64MicrosecondType>();
                let d: arrow::array::DurationMicrosecondArray = a.iter().collect();
                Some((
                    retyped(field, DataType::Duration(TimeUnit::Microsecond), &[], &[]),
                    Arc::new(d),
                ))
            }
            (DriverFamily::Mysql | DriverFamily::Duckdb, DataType::Utf8) if tag == Some("json") => {
                Some((
                    retyped(
                        field,
                        DataType::Utf8,
                        &[
                            META_DTYPE,
                            META_EXTENSION_NAME,
                            field_meta::EXTENSION_METADATA,
                        ],
                        &[],
                    ),
                    array.clone(),
                ))
            }
            (DriverFamily::Oracle, DataType::Float32) => {
                let a = array.as_primitive::<Float32Type>();
                let text: StringArray = a
                    .iter()
                    .map(|v| v.map(|x| meerschaum_core::pyfmt::float_repr(x as f64)))
                    .collect();
                Some((
                    retyped(field, DataType::Utf8, &[], &[(META_DTYPE, "numeric")]),
                    Arc::new(text),
                ))
            }
            (DriverFamily::Postgres, DataType::Binary) if tag == Some("geometry") => {
                let a = array.as_binary::<i32>();
                let text: StringArray = a
                    .iter()
                    .map(|v| v.map(|b| b.iter().map(|x| format!("{x:02X}")).collect::<String>()))
                    .collect();
                Some((
                    retyped(
                        field,
                        DataType::Utf8,
                        &[
                            META_DTYPE,
                            META_EXTENSION_NAME,
                            field_meta::EXTENSION_METADATA,
                        ],
                        &[],
                    ),
                    Arc::new(text),
                ))
            }
            (DriverFamily::Mssql, DataType::FixedSizeBinary(16)) => {
                let a = array.as_fixed_size_binary();
                let text: StringArray = a
                    .iter()
                    .map(|v| {
                        v.and_then(|b| uuid::Uuid::from_slice(b).ok())
                            .map(|u| u.hyphenated().to_string().to_uppercase())
                    })
                    .collect();
                Some((
                    retyped(
                        field,
                        DataType::Utf8,
                        &[
                            META_DTYPE,
                            META_EXTENSION_NAME,
                            field_meta::EXTENSION_METADATA,
                        ],
                        &[],
                    ),
                    Arc::new(text),
                ))
            }
            _ => None,
        };
        match out {
            Some((f, a)) => {
                changed = true;
                fields.push(f);
                arrays.push(a);
            }
            None => {
                fields.push(field.as_ref().clone());
                arrays.push(array.clone());
            }
        }
    }
    if !changed {
        return Ok(batch);
    }
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| PyValueError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------------------------
// Python values

/// How a normalized column's values cross to Python.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Null,
    Bool,
    Int,
    Float,
    /// `Decimal128` columns (`DECIMAL(p, s)`).
    Decimal,
    /// Decimal text (`meerschaum:dtype=numeric`): PostgreSQL `NUMERIC`, Oracle `BINARY_FLOAT`.
    NumericText,
    /// An unconstrained Oracle `NUMBER` read as decimal text: an `int` when integral, else a
    /// `Decimal` (python-oracledb + SQLAlchemy).
    OracleNumber,
    Text,
    /// JSON text the driver parses (psycopg, python-oracledb): `dict`, `list`, ...
    Json,
    Bytes,
    Uuid,
    Datetime,
    DatetimeTz,
    Date,
    Time,
    Duration,
    /// A SQLite column whose values have different storage classes.
    SqliteMixed,
    /// A list column (PostgreSQL arrays): Python lists of the element values.
    List,
    /// Anything else (lists, structs, intervals): its text.
    Other,
}

fn kind_of(field: &Field, flavor: Flavor) -> Kind {
    let tag = field_meta_get(field, META_DTYPE);
    let ext = field_meta_get(field, META_EXTENSION_NAME);
    match field.data_type() {
        DataType::Null => Kind::Null,
        DataType::Boolean => Kind::Bool,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Kind::Int,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Kind::Float,
        DataType::Decimal128(..) | DataType::Decimal256(..) => Kind::Decimal,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            if field.metadata().contains_key(META_SQLITE_CLASSES) {
                Kind::SqliteMixed
            } else if tag == Some("numeric") {
                if flavor == Flavor::Oracle && field_meta_get(field, META_DB_TYPE) == Some("NUMBER")
                {
                    Kind::OracleNumber
                } else {
                    Kind::NumericText
                }
            } else if tag == Some("json") || ext == Some("arrow.json") {
                Kind::Json
            } else {
                Kind::Text
            }
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Kind::Bytes,
        DataType::FixedSizeBinary(16) if ext == Some("arrow.uuid") || tag == Some("uuid") => {
            Kind::Uuid
        }
        DataType::FixedSizeBinary(_) => Kind::Bytes,
        DataType::Timestamp(_, None) => Kind::Datetime,
        DataType::Timestamp(_, Some(_)) => Kind::DatetimeTz,
        DataType::Date32 | DataType::Date64 => Kind::Date,
        DataType::Time32(_) | DataType::Time64(_) => Kind::Time,
        DataType::Duration(_) => Kind::Duration,
        DataType::List(_) | DataType::LargeList(_) => Kind::List,
        _ => Kind::Other,
    }
}

/// Python classes the conversions need, resolved once per call.
struct Classes<'py> {
    ctx: PyCtx<'py>,
    time_cls: Bound<'py, PyAny>,
}

impl<'py> Classes<'py> {
    fn new(py: Python<'py>) -> PyResult<Self> {
        Ok(Self {
            ctx: PyCtx::scalars(py)?,
            time_cls: py.import("datetime")?.getattr("time")?,
        })
    }
}

/// A timestamp array's values as microseconds since the epoch.
fn timestamp_micros(array: &ArrayRef) -> PyResult<arrow::array::TimestampMicrosecondArray> {
    let cast = arrow::compute::cast(array, &DataType::Timestamp(TimeUnit::Microsecond, None))
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(cast.as_primitive().clone())
}

fn datetime_py<'py>(c: &Classes<'py>, micros: i64, aware: bool) -> PyResult<Bound<'py, PyAny>> {
    let py = c.ctx.py;
    let Some(dt) = DateTime::from_timestamp_micros(micros).map(|d| d.naive_utc()) else {
        return Ok(py.None().into_bound(py));
    };
    let args = (
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
        dt.and_utc().timestamp_subsec_micros(),
    );
    if aware {
        let kw = PyDict::new(py);
        kw.set_item(intern!(py, "tzinfo"), &c.ctx.utc)?;
        c.ctx.datetime_cls.call(args, Some(&kw))
    } else {
        c.ctx.datetime_cls.call1(args)
    }
}

/// The column's values as the flavor's Python driver returns them (`None` for nulls).
fn column_values<'py>(
    c: &Classes<'py>,
    field: &Field,
    array: &ArrayRef,
    flavor: Flavor,
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    let py = c.ctx.py;
    let n = array.len();
    let none = || py.None().into_bound(py);
    let mut out: Vec<Bound<'py, PyAny>> = Vec::with_capacity(n);
    macro_rules! ints {
        ($t:ty) => {{
            let a = array.as_primitive::<$t>();
            for v in a.iter() {
                out.push(match v {
                    Some(x) => x.into_pyobject(py)?.into_any(),
                    None => none(),
                });
            }
        }};
    }
    match kind_of(field, flavor) {
        Kind::Null => out.extend((0..n).map(|_| none())),
        Kind::Bool => {
            for v in array.as_boolean().iter() {
                out.push(match v {
                    Some(b) => PyBool::new(py, b).to_owned().into_any(),
                    None => none(),
                });
            }
        }
        Kind::Int => match field.data_type() {
            DataType::Int8 => ints!(Int8Type),
            DataType::Int16 => ints!(Int16Type),
            DataType::Int32 => ints!(Int32Type),
            DataType::Int64 => ints!(Int64Type),
            DataType::UInt8 => ints!(UInt8Type),
            DataType::UInt16 => ints!(UInt16Type),
            DataType::UInt32 => ints!(UInt32Type),
            _ => ints!(UInt64Type),
        },
        Kind::Float => {
            let cast = arrow::compute::cast(array, &DataType::Float64)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            for v in cast.as_primitive::<Float64Type>().iter() {
                out.push(match v {
                    Some(x) => PyFloat::new(py, x).into_any(),
                    None => none(),
                });
            }
        }
        Kind::Decimal => {
            for i in 0..n {
                out.push(if array.is_null(i) {
                    none()
                } else {
                    let text = arrow::util::display::array_value_to_string(array, i)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    c.ctx.decimal_cls.call1((text,))?
                });
            }
        }
        Kind::NumericText | Kind::OracleNumber | Kind::Text | Kind::Json | Kind::SqliteMixed => {
            let cast = arrow::compute::cast(array, &DataType::Utf8)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            let strings = cast.as_string::<i32>();
            let kind = kind_of(field, flavor);
            let classes = field_meta_get(field, META_SQLITE_CLASSES)
                .map(str::as_bytes)
                .unwrap_or(&[]);
            for (i, v) in strings.iter().enumerate() {
                let Some(s) = v else {
                    out.push(none());
                    continue;
                };
                out.push(match kind {
                    Kind::NumericText => c.ctx.decimal_cls.call1((s,))?,
                    Kind::OracleNumber => match s.parse::<i64>() {
                        Ok(i) => i.into_pyobject(py)?.into_any(),
                        Err(_) => c.ctx.decimal_cls.call1((s,))?,
                    },
                    Kind::Json => match serde_json::from_str::<serde_json::Value>(s) {
                        Ok(j) => crate::convert::json_to_py(py, &j)?,
                        Err(_) => PyString::new(py, s).into_any(),
                    },
                    Kind::SqliteMixed => match classes.get(i).copied() {
                        Some(b'i') => match s.parse::<i64>() {
                            Ok(x) => x.into_pyobject(py)?.into_any(),
                            Err(_) => PyString::new(py, s).into_any(),
                        },
                        Some(b'f') => PyFloat::new(
                            py,
                            match s {
                                "inf" => f64::INFINITY,
                                "-inf" => f64::NEG_INFINITY,
                                _ => s.parse::<f64>().unwrap_or(f64::NAN),
                            },
                        )
                        .into_any(),
                        Some(b'b') => {
                            let bytes = (0..s.len() / 2)
                                .filter_map(|k| u8::from_str_radix(&s[2 * k..2 * k + 2], 16).ok())
                                .collect::<Vec<u8>>();
                            PyBytes::new(py, &bytes).into_any()
                        }
                        Some(b'n') => none(),
                        _ => PyString::new(py, s).into_any(),
                    },
                    _ => PyString::new(py, s).into_any(),
                });
            }
        }
        Kind::Bytes => {
            for i in 0..n {
                out.push(if array.is_null(i) {
                    none()
                } else {
                    let bytes: &[u8] = match field.data_type() {
                        DataType::Binary => array.as_binary::<i32>().value(i),
                        DataType::LargeBinary => array.as_binary::<i64>().value(i),
                        DataType::BinaryView => array.as_binary_view().value(i),
                        _ => array.as_fixed_size_binary().value(i),
                    };
                    PyBytes::new(py, bytes).into_any()
                });
            }
        }
        Kind::Uuid => {
            let a = array.as_fixed_size_binary();
            for v in a.iter() {
                out.push(match v.and_then(|b| uuid::Uuid::from_slice(b).ok()) {
                    Some(u) => c.ctx.uuid_cls.call1((
                        py.None(),
                        py.None(),
                        py.None(),
                        py.None(),
                        u.as_u128(),
                    ))?,
                    None => none(),
                });
            }
        }
        Kind::Datetime | Kind::DatetimeTz => {
            let aware = kind_of(field, flavor) == Kind::DatetimeTz;
            for v in timestamp_micros(array)?.iter() {
                out.push(match v {
                    Some(us) => datetime_py(c, us, aware)?,
                    None => none(),
                });
            }
        }
        Kind::Date => {
            let cast = arrow::compute::cast(array, &DataType::Date32)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            for v in cast.as_primitive::<arrow::datatypes::Date32Type>().iter() {
                out.push(
                    match v.and_then(|d| NaiveDate::from_num_days_from_ce_opt(d + 719_163)) {
                        Some(d) => c.ctx.date_cls.call1((d.year(), d.month(), d.day()))?,
                        None => none(),
                    },
                );
            }
        }
        Kind::Time => {
            let cast = arrow::compute::cast(array, &DataType::Time64(TimeUnit::Microsecond))
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            for v in cast
                .as_primitive::<arrow::datatypes::Time64MicrosecondType>()
                .iter()
            {
                out.push(
                    match v.and_then(|us| {
                        NaiveTime::from_num_seconds_from_midnight_opt(
                            (us / 1_000_000) as u32,
                            ((us % 1_000_000) * 1000) as u32,
                        )
                    }) {
                        Some(t) => c.time_cls.call1((
                            t.hour(),
                            t.minute(),
                            t.second(),
                            t.nanosecond() / 1000,
                        ))?,
                        None => none(),
                    },
                );
            }
        }
        Kind::Duration => {
            let cast = arrow::compute::cast(array, &DataType::Duration(TimeUnit::Microsecond))
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            for v in cast.as_primitive::<DurationMicrosecondType>().iter() {
                out.push(match v {
                    Some(us) => {
                        let kw = PyDict::new(py);
                        kw.set_item(intern!(py, "microseconds"), us)?;
                        c.ctx.timedelta_cls.call((), Some(&kw))?
                    }
                    None => none(),
                });
            }
        }
        Kind::List => {
            let child_field = match field.data_type() {
                DataType::List(f) | DataType::LargeList(f) => f.as_ref().clone(),
                _ => Field::new("item", DataType::Null, true),
            };
            for i in 0..n {
                if array.is_null(i) {
                    out.push(none());
                    continue;
                }
                let child = match field.data_type() {
                    DataType::LargeList(_) => array.as_list::<i64>().value(i),
                    _ => array.as_list::<i32>().value(i),
                };
                let values = column_values(c, &child_field, &child, flavor)?;
                out.push(PyList::new(py, values)?.into_any());
            }
        }
        Kind::Other => {
            for i in 0..n {
                out.push(if array.is_null(i) {
                    none()
                } else {
                    let text = arrow::util::display::array_value_to_string(array, i)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?;
                    PyString::new(py, &text).into_any()
                });
            }
        }
    }
    Ok(out)
}

/// The pandas dtype `pd.read_sql_query()` infers for a column of these driver values
/// (`maybe_convert_objects`): every column of an empty result and every all-null column is
/// `object`; integers are `int64` (`float64` with nulls); floats `float64`; decimals `float64`
/// with `coerce_float`, else `object`; booleans `bool` (`object` with nulls); text `str`
/// (`object` without pandas' string inference); datetimes `datetime64[<unit>]` (with `UTC` when
/// aware); durations `timedelta64[<unit>]`; dates, times, bytes, UUIDs, JSON documents `object`.
fn pandas_hint(
    field: &Field,
    array: &ArrayRef,
    flavor: Flavor,
    coerce_float: bool,
    infer_string: bool,
    unit: &str,
) -> String {
    let n = array.len();
    let nulls = array.null_count();
    if n == 0 || nulls == n {
        return "object".into();
    }
    let has_nulls = nulls > 0;
    let s = |x: &str| x.to_string();
    match kind_of(field, flavor) {
        Kind::Null => s("object"),
        Kind::Int => {
            if has_nulls {
                s("float64")
            } else if field.data_type() == &DataType::UInt64
                && array
                    .as_primitive::<UInt64Type>()
                    .iter()
                    .flatten()
                    .any(|v| v > i64::MAX as u64)
            {
                s("uint64")
            } else {
                s("int64")
            }
        }
        Kind::Float => s("float64"),
        Kind::Decimal | Kind::NumericText => {
            if coerce_float {
                s("float64")
            } else {
                s("object")
            }
        }
        Kind::OracleNumber => {
            // python-oracledb returns an `int` for an integral value: all `int`s infer `int64`.
            let strings = arrow::compute::cast(array, &DataType::Utf8).ok();
            let all_ints = strings.as_ref().is_some_and(|a| {
                a.as_string::<i32>()
                    .iter()
                    .flatten()
                    .all(|v| v.parse::<i64>().is_ok())
            });
            match (all_ints, has_nulls) {
                (true, false) => s("int64"),
                (true, true) => s("float64"),
                _ if coerce_float => s("float64"),
                _ => s("object"),
            }
        }
        Kind::Bool => {
            if has_nulls {
                s("object")
            } else {
                s("bool")
            }
        }
        Kind::Text => {
            if infer_string {
                s("str")
            } else {
                s("object")
            }
        }
        Kind::SqliteMixed => {
            let classes = field_meta_get(field, META_SQLITE_CLASSES).unwrap_or("");
            if classes.bytes().all(|c| matches!(c, b'i' | b'f' | b'n')) {
                s("float64")
            } else {
                s("object")
            }
        }
        Kind::Datetime => format!("datetime64[{unit}]"),
        Kind::DatetimeTz => format!("datetime64[{unit}, UTC]"),
        Kind::Duration => format!("timedelta64[{unit}]"),
        Kind::Json
        | Kind::Bytes
        | Kind::Uuid
        | Kind::Date
        | Kind::Time
        | Kind::List
        | Kind::Other => s("object"),
    }
}

/// A float parsed from decimal text the way Python's `float(Decimal(text))` reads it.
fn text_to_f64(s: &str) -> f64 {
    match s.trim() {
        "NaN" | "nan" | "sNaN" | "-NaN" => f64::NAN,
        "Infinity" | "inf" | "+Infinity" => f64::INFINITY,
        "-Infinity" | "-inf" => f64::NEG_INFINITY,
        t => t.parse::<f64>().unwrap_or(f64::NAN),
    }
}

/// The column as the Arrow array whose `to_pandas()` is `hint` (or that `astype(hint)` turns
/// into it without changing values): integers as `Int64`, every `float64` hint as `Float64`
/// (decimal values parsed as Python's `float()` does), the rest unchanged.
fn pandas_array(field: &Field, array: &ArrayRef, flavor: Flavor, hint: &str) -> PyResult<ArrayRef> {
    let err = |e: arrow::error::ArrowError| PyValueError::new_err(e.to_string());
    match hint {
        "int64" => arrow::compute::cast(array, &DataType::Int64).map_err(err),
        "uint64" => arrow::compute::cast(array, &DataType::UInt64).map_err(err),
        "float64" => match kind_of(field, flavor) {
            Kind::Int | Kind::Float => arrow::compute::cast(array, &DataType::Float64).map_err(err),
            Kind::Decimal => {
                let mut values = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    values.push(if array.is_null(i) {
                        None
                    } else {
                        Some(text_to_f64(
                            &arrow::util::display::array_value_to_string(array, i).map_err(err)?,
                        ))
                    });
                }
                Ok(Arc::new(Float64Array::from(values)))
            }
            _ => {
                let strings = arrow::compute::cast(array, &DataType::Utf8).map_err(err)?;
                let floats: Float64Array = strings
                    .as_string::<i32>()
                    .iter()
                    .map(|v| v.map(text_to_f64))
                    .collect();
                Ok(Arc::new(floats))
            }
        },
        _ => Ok(array.clone()),
    }
}

/// One batch of rows from a raw SQL statement (see the module docs).
#[pyclass(frozen, name = "SqlFrame", module = "meerschaum._core")]
pub struct SqlFrame {
    batch: RecordBatch,
    flavor: Flavor,
}

impl SqlFrame {
    /// A frame of `batch`, normalized for `flavor`.
    pub fn new(batch: RecordBatch, flavor: Flavor) -> PyResult<Self> {
        Ok(Self {
            batch: normalize_batch(batch, flavor)?,
            flavor,
        })
    }

    fn columns_values<'py>(&self, c: &Classes<'py>) -> PyResult<Vec<Vec<Bound<'py, PyAny>>>> {
        let schema = self.batch.schema();
        schema
            .fields()
            .iter()
            .zip(self.batch.columns())
            .map(|(f, a)| column_values(c, f, a, self.flavor))
            .collect()
    }

    fn hints_vec(&self, coerce_float: bool, infer_string: bool, unit: &str) -> Vec<String> {
        let schema = self.batch.schema();
        schema
            .fields()
            .iter()
            .zip(self.batch.columns())
            .map(|(f, a)| pandas_hint(f, a, self.flavor, coerce_float, infer_string, unit))
            .collect()
    }
}

#[pymethods]
impl SqlFrame {
    /// The result's column names (duplicates kept).
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// The number of rows.
    #[getter]
    fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    fn __len__(&self) -> usize {
        self.batch.num_rows()
    }

    fn __repr__(&self) -> String {
        format!(
            "SqlFrame(rows={}, columns={:?})",
            self.batch.num_rows(),
            self.columns()
        )
    }

    /// The rows as tuples of the values the flavor's Python driver returns.
    fn rows<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let c = Classes::new(py)?;
        let columns = self.columns_values(&c)?;
        let out = PyList::empty(py);
        for i in 0..self.batch.num_rows() {
            let row = PyTuple::new(py, columns.iter().map(|col| col[i].clone()))?;
            out.append(row)?;
        }
        Ok(out)
    }

    /// Rows `offset .. offset + length` (a frame sharing these buffers).
    fn slice(&self, offset: usize, length: usize) -> SqlFrame {
        let n = self.batch.num_rows();
        let offset = offset.min(n);
        SqlFrame {
            batch: self.batch.slice(offset, length.min(n - offset)),
            flavor: self.flavor,
        }
    }

    /// One column's values (see [`SqlFrame::rows`]).
    fn column<'py>(&self, py: Python<'py>, index: usize) -> PyResult<Bound<'py, PyList>> {
        let schema = self.batch.schema();
        let (Some(field), Some(array)) =
            (schema.fields().get(index), self.batch.columns().get(index))
        else {
            return Err(pyo3::exceptions::PyIndexError::new_err(index));
        };
        let c = Classes::new(py)?;
        PyList::new(py, column_values(&c, field, array, self.flavor)?)
    }

    /// The first value of the first row (`SQLConnector.value()`), `None` without rows.
    fn first_value<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.batch.num_rows() == 0 || self.batch.num_columns() == 0 {
            return Ok(py.None().into_bound(py));
        }
        let first = self.batch.slice(0, 1);
        let c = Classes::new(py)?;
        let schema = first.schema();
        let values = column_values(&c, schema.field(0), first.column(0), self.flavor)?;
        Ok(values
            .into_iter()
            .next()
            .unwrap_or_else(|| py.None().into_bound(py)))
    }

    /// The pandas dtype `pd.read_sql_query()` infers for each column ([`pandas_hint`]).
    #[pyo3(signature = (coerce_float=true, infer_string=true, unit="us"))]
    fn hints(&self, coerce_float: bool, infer_string: bool, unit: &str) -> Vec<String> {
        self.hints_vec(coerce_float, infer_string, unit)
    }

    /// The pandas DataFrame v4's `pd.read_sql_query(..., coerce_float=coerce_float)` returns for
    /// these rows (default `RangeIndex`, duplicate column names kept). `infer_string` defaults to
    /// pandas' `future.infer_string` option and `unit` to `us` (pandas 3) or `ns` (pandas 2).
    #[pyo3(signature = (coerce_float=true, infer_string=None, unit=None))]
    fn to_pandas<'py>(
        &self,
        py: Python<'py>,
        coerce_float: bool,
        infer_string: Option<bool>,
        unit: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pd = import_optional(py, "pandas")?;
        let infer_string = match infer_string {
            Some(b) => b,
            None => pd
                .call_method1("get_option", ("future.infer_string",))
                .and_then(|v| v.is_truthy())
                .unwrap_or(false),
        };
        let unit = match unit {
            Some(u) => u,
            None => {
                let version: String = pd.getattr("__version__")?.extract()?;
                let major: u32 = version
                    .split('.')
                    .next()
                    .and_then(|m| m.parse().ok())
                    .unwrap_or(3);
                if major >= 3 { "us".into() } else { "ns".into() }
            }
        };
        let hints = self.hints_vec(coerce_float, infer_string, &unit);
        let schema = self.batch.schema();
        let n = self.batch.num_rows();

        // The native columns cross as one Arrow table (no per-value Python objects).
        let mut native_fields = Vec::new();
        let mut native_arrays = Vec::new();
        for ((field, array), hint) in schema.fields().iter().zip(self.batch.columns()).zip(&hints) {
            if hint != "object" {
                let a = pandas_array(field, array, self.flavor, hint)?;
                native_fields.push(Field::new(field.name(), a.data_type().clone(), true));
                native_arrays.push(a);
            }
        }
        let table = if native_arrays.is_empty() {
            None
        } else {
            let sub = RecordBatch::try_new_with_options(
                Arc::new(Schema::new(native_fields)),
                native_arrays,
                &RecordBatchOptions::new().with_row_count(Some(n)),
            )
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
            let stream = ArrowStream::new(sub.schema(), vec![sub]);
            let pa = import_optional(py, "pyarrow")?;
            Some(pa.getattr("table")?.call1((Py::new(py, stream)?,))?)
        };

        let c = Classes::new(py)?;
        let series_cls = pd.getattr("Series")?;
        let data = PyDict::new(py);
        let mut native_ix = 0usize;
        for (i, ((field, array), hint)) in schema
            .fields()
            .iter()
            .zip(self.batch.columns())
            .zip(&hints)
            .enumerate()
        {
            let series = if hint == "object" {
                let values = column_values(&c, field, array, self.flavor)?;
                let kw = PyDict::new(py);
                kw.set_item("dtype", "object")?;
                series_cls.call((PyList::new(py, values)?,), Some(&kw))?
            } else {
                let table = table.as_ref().expect("native table");
                let col = table.call_method1(intern!(py, "column"), (native_ix,))?;
                native_ix += 1;
                let s = col.call_method0(intern!(py, "to_pandas"))?;
                let current: String = s.getattr(intern!(py, "dtype"))?.str()?.extract()?;
                if current == *hint {
                    s
                } else {
                    s.call_method1(intern!(py, "astype"), (hint.as_str(),))?
                }
            };
            data.set_item(i, series)?;
        }
        let df = if schema.fields().is_empty() {
            let kw = PyDict::new(py);
            kw.set_item("index", pd.getattr("RangeIndex")?.call1((n,))?)?;
            pd.getattr("DataFrame")?.call((), Some(&kw))?
        } else {
            let kw = PyDict::new(py);
            kw.set_item("copy", false)?;
            let df = pd.getattr("DataFrame")?.call((data,), Some(&kw))?;
            df.setattr("columns", PyList::new(py, self.columns())?)?;
            df
        };
        Ok(df)
    }

    /// The rows as a Polars DataFrame (the normalized Arrow columns).
    fn to_polars<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pl = import_optional(py, "polars")?;
        let stream = ArrowStream::new(self.batch.schema(), vec![self.batch.clone()]);
        pl.getattr("DataFrame")?.call1((Py::new(py, stream)?,))
    }

    /// The Arrow schema (`__arrow_c_schema__`).
    fn __arrow_c_schema__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, pyo3::types::PyCapsule>> {
        crate::ffi_export::schema_capsule(py, self.batch.schema().as_ref())
    }

    /// The normalized rows as an Arrow C stream (`__arrow_c_stream__`).
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, pyo3::types::PyCapsule>>,
    ) -> PyResult<Bound<'py, pyo3::types::PyCapsule>> {
        let requested = requested_schema
            .as_ref()
            .map(crate::ffi_export::requested_schema)
            .transpose()?;
        let reader = crate::ffi_export::batch_reader(
            self.batch.schema(),
            vec![self.batch.clone()],
            requested.as_ref(),
        );
        crate::ffi_export::stream_capsule(py, reader)
    }
}

/// The batches of a raw read, as an iterator of [`SqlFrame`]s (rows stream from the database
/// while a statement without parameters is read; see `driver::raw`).
#[pyclass(name = "SqlChunks", module = "meerschaum._core")]
pub struct SqlChunks {
    inner: Mutex<Option<RawChunks>>,
    columns: Vec<String>,
    flavor: Flavor,
}

impl SqlChunks {
    pub fn new(chunks: RawChunks, flavor: Flavor) -> Self {
        Self {
            columns: chunks.columns.clone(),
            inner: Mutex::new(Some(chunks)),
            flavor,
        }
    }
}

#[pymethods]
impl SqlChunks {
    /// The result's column names.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<SqlFrame>> {
        let taken = self.inner.lock().unwrap_or_else(|e| e.into_inner()).take();
        let Some(mut chunks) = taken else {
            return Ok(None);
        };
        let (item, chunks) = block_on(py, async move {
            let item = chunks.next().await;
            (item, chunks)
        });
        match item {
            None => Ok(None),
            Some(Ok(batch)) => {
                *self.inner.lock().unwrap_or_else(|e| e.into_inner()) = Some(chunks);
                Ok(Some(SqlFrame::new(batch, self.flavor)?))
            }
            Some(Err(e)) => Err(sql_err(e, self.flavor)),
        }
    }

    /// Stop reading (the connection goes back to the pool).
    fn close(&self) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
}

/// An open transaction for raw SQL (`exec_queries`, protected reads, `exec(commit=False)`).
/// Dropping it without [`SqlTx::commit`] rolls it back.
#[pyclass(name = "SqlTx", module = "meerschaum._core")]
pub struct SqlTx {
    inner: Mutex<Option<SqlTransaction>>,
    flavor: Flavor,
}

impl SqlTx {
    pub fn new(tx: SqlTransaction, flavor: Flavor) -> Self {
        Self {
            inner: Mutex::new(Some(tx)),
            flavor,
        }
    }

    fn take(&self) -> PyResult<SqlTransaction> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| {
                SqlDatabaseError::new_err((
                    "The transaction is closed.".to_string(),
                    "InternalError",
                    None::<String>,
                ))
            })
    }

    fn put(&self, tx: SqlTransaction) {
        *self.inner.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);
    }
}

#[pymethods]
impl SqlTx {
    /// Whether the transaction is still open.
    #[getter]
    fn active(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    /// Run a row-returning statement in the transaction.
    #[pyo3(signature = (sql, params=None))]
    fn query(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<SqlFrame> {
        let ctx = PyCtx::scalars(py)?;
        let params = params_from_py(&ctx, params)?;
        let mut tx = self.take()?;
        let (res, tx) = block_on(py, async move {
            let res = tx.raw_query(&sql, &params).await;
            (res, tx)
        });
        self.put(tx);
        SqlFrame::new(res.map_err(|e| sql_err(e, self.flavor))?, self.flavor)
    }

    /// Execute a statement that returns no rows in the transaction: the rows affected.
    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        let ctx = PyCtx::scalars(py)?;
        let params = params_from_py(&ctx, params)?;
        let mut tx = self.take()?;
        let (res, tx) = block_on(py, async move {
            let res = tx.raw_execute(&sql, &params).await;
            (res, tx)
        });
        self.put(tx);
        res.map_err(|e| sql_err(e, self.flavor))
    }

    /// Commit (the transaction is closed afterwards).
    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let tx = self.take()?;
        block_on(py, async move { tx.commit().await }).map_err(|e| sql_err(e, self.flavor))
    }

    /// Roll back (the transaction is closed afterwards). Closing a closed transaction is a
    /// no-op.
    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let Some(tx) = self.inner.lock().unwrap_or_else(|e| e.into_inner()).take() else {
            return Ok(());
        };
        block_on(py, async move { tx.rollback().await }).map_err(|e| sql_err(e, self.flavor))
    }
}

// ---------------------------------------------------------------------------------------------
// The connector entry points (bound as `_core.SqlConnector` methods in `sql_api.rs`)

/// `SqlConnector.sql_query(sql, params=None, chunksize=None)`.
pub fn sql_query(
    py: Python<'_>,
    conn: Arc<RsSqlConnector>,
    sql: String,
    params: Option<&Bound<'_, PyAny>>,
    chunksize: Option<usize>,
) -> PyResult<SqlChunks> {
    let ctx = PyCtx::scalars(py)?;
    let params = params_from_py(&ctx, params)?;
    let flavor = conn.flavor();
    let chunksize = chunksize
        .filter(|c| *c > 0)
        .unwrap_or(meerschaum_connector_sql::driver::raw::RAW_CHUNKSIZE);
    let chunks = block_on(py, async move {
        conn.driver().raw_query(&sql, &params, chunksize).await
    })
    .map_err(|e| sql_err(e, flavor))?;
    Ok(SqlChunks::new(chunks, flavor))
}

/// `SqlConnector.sql_execute(sql, params=None)`: the rows affected.
pub fn sql_execute(
    py: Python<'_>,
    conn: Arc<RsSqlConnector>,
    sql: String,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<u64> {
    let ctx = PyCtx::scalars(py)?;
    let params = params_from_py(&ctx, params)?;
    let flavor = conn.flavor();
    block_on(
        py,
        async move { conn.driver().raw_execute(&sql, &params).await },
    )
    .map_err(|e| sql_err(e, flavor))
}

/// `SqlConnector.sql_execute_many(sql, param_sets)`: `executemany` in one transaction.
pub fn sql_execute_many(
    py: Python<'_>,
    conn: Arc<RsSqlConnector>,
    sql: String,
    param_sets: &Bound<'_, PyAny>,
) -> PyResult<u64> {
    let ctx = PyCtx::scalars(py)?;
    let sets = param_sets
        .try_iter()?
        .map(|item| params_from_py(&ctx, Some(&item?)))
        .collect::<PyResult<Vec<_>>>()?;
    let flavor = conn.flavor();
    block_on(py, async move {
        conn.driver().raw_execute_many(&sql, &sets).await
    })
    .map_err(|e| sql_err(e, flavor))
}

/// `SqlConnector.sql_begin(read_only=False, write=False)`: an open [`SqlTx`].
pub fn sql_begin(
    py: Python<'_>,
    conn: Arc<RsSqlConnector>,
    read_only: bool,
    write: bool,
) -> PyResult<SqlTx> {
    let flavor = conn.flavor();
    let tx = block_on(py, async move {
        if read_only {
            conn.driver().begin_read_only().await
        } else if write {
            conn.driver().begin_write().await
        } else {
            conn.driver().begin().await
        }
    })
    .map_err(|e| sql_err(e, flavor))?;
    Ok(SqlTx::new(tx, flavor))
}

/// `SqlConnector.sql_to_sql(df, name, schema, if_exists, column_types, index_columns,
/// chunksize)`: `(success, error message)`.
#[allow(clippy::too_many_arguments)]
pub fn sql_to_sql(
    py: Python<'_>,
    conn: Arc<RsSqlConnector>,
    df: &Bound<'_, PyAny>,
    name: String,
    schema: Option<String>,
    if_exists: &str,
    column_types: &Bound<'_, PyDict>,
    index_columns: Vec<String>,
    chunksize: Option<i64>,
) -> PyResult<(bool, String)> {
    let if_exists: IfExists = if_exists.parse().map_err(PyValueError::new_err)?;
    let mut types: IndexMap<String, String> = IndexMap::new();
    for (k, v) in column_types.iter() {
        types.insert(k.str()?.extract()?, v.str()?.extract()?);
    }
    // Polars / Arrow inputs do not need pandas. Pandas inputs already imported it;
    // the scalar context picks up their classes from sys.modules in that case.
    let ctx = PyCtx::scalars(py)?;
    let frame =
        crate::pandas_bridge::frame_from_py(&ctx, df, crate::pandas_bridge::ColumnMode::Full)?
            .frame;
    Ok(block_on(py, async move {
        conn.frame_to_sql(
            &frame,
            &name,
            schema.as_deref(),
            if_exists,
            &types,
            &index_columns,
            chunksize,
        )
        .await
    }))
}

/// `SqlConnector.sql_table_exists(table, schema=None)` (database errors raise).
pub fn sql_table_exists(
    py: Python<'_>,
    conn: Arc<RsSqlConnector>,
    table: String,
    schema: Option<String>,
) -> PyResult<bool> {
    let flavor = conn.flavor();
    let schema = schema.or_else(|| conn.schema().map(str::to_string));
    block_on(py, async move {
        conn.table_exists_checked(&table, schema.as_deref()).await
    })
    .map_err(|e| {
        let kind = if flavor == Flavor::Sqlite {
            "OperationalError"
        } else {
            "DatabaseError"
        };
        SqlDatabaseError::new_err((e.to_string(), kind, None::<String>))
    })
}

/// `_core.sql_returns_rows(sql, flavor)`: whether a statement returns rows
/// (`driver::raw::returns_rows`).
#[pyfunction]
pub fn sql_returns_rows(sql: &str, flavor: &str) -> PyResult<bool> {
    let flavor: Flavor = flavor
        .parse()
        .map_err(|e| PyValueError::new_err(format!("{e:?}")))?;
    Ok(meerschaum_connector_sql::driver::raw::returns_rows(
        sql, flavor,
    ))
}

/// Register the raw SQL classes and functions on `meerschaum._core._native`.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SqlFrame>()?;
    m.add_class::<SqlChunks>()?;
    m.add_class::<SqlTx>()?;
    m.add("SqlDatabaseError", m.py().get_type::<SqlDatabaseError>())?;
    m.add_function(wrap_pyfunction!(sql_returns_rows, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(key: &str, value: &str) -> std::collections::HashMap<String, String> {
        [(key.to_string(), value.to_string())].into_iter().collect()
    }

    #[test]
    fn single_precision_floats_read_like_their_text() {
        assert_eq!(f32_shortest(1.1), 1.1);
        assert_eq!(f32_shortest(2.25), 2.25);
        assert!(f32_shortest(f32::NAN).is_nan());
    }

    #[test]
    fn decimal_text_parses_like_python_float() {
        assert_eq!(text_to_f64("1.50"), 1.5);
        assert!(text_to_f64("NaN").is_nan());
        assert_eq!(text_to_f64("Infinity"), f64::INFINITY);
        assert_eq!(text_to_f64("-Infinity"), f64::NEG_INFINITY);
        assert_eq!(text_to_f64("1e-3"), 0.001);
    }

    #[test]
    fn hints_follow_pandas_inference() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
        let field = Field::new("a", DataType::Int64, true);
        assert_eq!(
            pandas_hint(&field, &ints, Flavor::Postgresql, true, true, "us"),
            "float64"
        );
        let full: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        assert_eq!(
            pandas_hint(&field, &full, Flavor::Postgresql, true, true, "us"),
            "int64"
        );
        let empty: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        assert_eq!(
            pandas_hint(&field, &empty, Flavor::Postgresql, true, true, "us"),
            "object"
        );
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let tfield = Field::new("t", DataType::Utf8, true);
        assert_eq!(
            pandas_hint(&tfield, &text, Flavor::Sqlite, true, true, "us"),
            "str"
        );
        assert_eq!(
            pandas_hint(&tfield, &text, Flavor::Sqlite, true, false, "us"),
            "object"
        );
        let numeric =
            Field::new("n", DataType::Utf8, true).with_metadata(meta(META_DTYPE, "numeric"));
        assert_eq!(
            pandas_hint(&numeric, &text, Flavor::Postgresql, true, true, "us"),
            "float64"
        );
        assert_eq!(
            pandas_hint(&numeric, &text, Flavor::Postgresql, false, true, "us"),
            "object"
        );
        let bools: ArrayRef = Arc::new(arrow::array::BooleanArray::from(vec![Some(true), None]));
        let bfield = Field::new("b", DataType::Boolean, true);
        assert_eq!(
            pandas_hint(&bfield, &bools, Flavor::Postgresql, true, true, "us"),
            "object"
        );
        let mixed =
            Field::new("m", DataType::Utf8, true).with_metadata(meta(META_SQLITE_CLASSES, "if"));
        let mixed_values: ArrayRef = Arc::new(StringArray::from(vec!["1", "2.5"]));
        assert_eq!(
            pandas_hint(&mixed, &mixed_values, Flavor::Sqlite, true, true, "us"),
            "float64"
        );
    }

    #[test]
    fn mysql_columns_hold_what_pymysql_returns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("f", DataType::Float32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::BooleanArray::from(vec![Some(true), None])),
                Arc::new(arrow::array::Float32Array::from(vec![Some(1.1f32), None])),
            ],
        )
        .unwrap();
        let out = normalize_batch(batch.clone(), Flavor::Mysql).unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(out.schema().field(1).data_type(), &DataType::Float64);
        assert_eq!(out.column(1).as_primitive::<Float64Type>().value(0), 1.1);
        // Other flavors keep the driver's types.
        let same = normalize_batch(batch, Flavor::Postgresql).unwrap();
        assert_eq!(same.schema().field(0).data_type(), &DataType::Boolean);
    }
}
