//! Raw SQL for the Python façade's `SQLConnector.read` / `exec` / `value` / `exec_queries`
//! (rc15 Q50 / Q53): one caller-written statement with bound parameters, on the pool or inside
//! an open transaction, whose rows keep what each flavor's Python driver returns.
//!
//! Rows come back as Arrow record batches. Every family but SQLite types its columns from the
//! statement's result metadata, exactly as the instance methods read them (`backend.rs`,
//! `native.rs`). SQLite is dynamically typed and `sqlite3` returns each value in its own storage
//! class (`INTEGER` -> `int`, `REAL` -> `float`, `TEXT` -> `str`, `BLOB` -> `bytes`), whatever
//! the column's declared type: a raw SQLite read types each column of each chunk from the
//! storage classes of its values (`sqlite_batch`), never from the declared type (a
//! `DATETIME` column of text values is text, as v4 returns it). A column whose values have
//! different classes is text tagged with each row's class ([`META_SQLITE_CLASSES`]), so the
//! bindings can rebuild the exact Python values.
//!
//! The statement kind decides the call ([`returns_rows`]): row-returning statements go through
//! the query path (prepared, typed rows), the others through `execute` (rows affected).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, NullArray, RecordBatch,
    RecordBatchOptions, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use futures_util::StreamExt;
use sqlx::sqlite::{SqliteRow, SqliteStatement};
use sqlx::{AssertSqlSafe, Column, Executor, Row, SqlSafeStr, Statement, TypeInfo, ValueRef};
use tokio::sync::mpsc;

use super::batch::META_DB_TYPE;
use super::params::SqlParam;
use super::{Pool, SqlDriver, SqlTransaction, TxInner};
use crate::error::{Result, SqlError};
use crate::flavor::{DriverFamily, Flavor};

/// Field metadata key of a raw SQLite column whose values have different storage classes: one
/// character per row (`i` integer, `f` real, `s` text, `b` blob, `n` null). The column holds each
/// value's text (`CAST(x AS TEXT)`; blobs as lowercase hex) so Arrow consumers still read it.
pub const META_SQLITE_CLASSES: &str = "meerschaum:sqlite_classes";

/// The default number of rows per batch of a raw read.
pub const RAW_CHUNKSIZE: usize = 100_000;

/// Whether a statement returns rows: a query (`SELECT`, `WITH`, `VALUES`, `SHOW`, `PRAGMA`,
/// `EXPLAIN`, `DESCRIBE`, DuckDB's `FROM` / `SUMMARIZE` / `PIVOT`, MSSQL's `EXEC`), or a write
/// with a `RETURNING` (or MSSQL `OUTPUT`) clause.
///
/// The first word after leading whitespace, comments and parentheses decides; for writes the
/// whole text is scanned outside string literals, quoted identifiers and comments. A statement
/// classified as a query that turns out to return no columns (a `WITH ... INSERT` without
/// `RETURNING`, a `PRAGMA` that sets a value) still runs: it just has no rows.
pub fn returns_rows(sql: &str, flavor: Flavor) -> bool {
    let words = significant_words(sql);
    let Some(first) = words.first() else {
        return false;
    };
    match first.as_str() {
        "SELECT" | "WITH" | "VALUES" | "TABLE" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN"
        | "PRAGMA" => true,
        "FROM" | "SUMMARIZE" | "PIVOT" | "UNPIVOT" | "CALL" => {
            flavor.driver_family() == DriverFamily::Duckdb || first == "CALL"
        }
        "EXEC" | "EXECUTE" => flavor.driver_family() == DriverFamily::Mssql,
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "REPLACE" | "UPSERT" => {
            words.iter().any(|w| w == "RETURNING")
                || (flavor.driver_family() == DriverFamily::Mssql
                    && words.iter().any(|w| w == "OUTPUT"))
        }
        _ => false,
    }
}

/// The uppercase words of a statement outside string literals, quoted identifiers and comments
/// (parentheses and punctuation separate words).
fn significant_words(sql: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let flush = |word: &mut String, words: &mut Vec<String>| {
        if !word.is_empty() {
            words.push(word.to_ascii_uppercase());
            word.clear();
        }
    };
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        match c {
            '-' if next == Some('-') => {
                flush(&mut word, &mut words);
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if next == Some('*') => {
                flush(&mut word, &mut words);
                let mut depth = 0usize;
                while i < chars.len() {
                    if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                        depth += 1;
                        i += 2;
                        continue;
                    }
                    if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                        depth = depth.saturating_sub(1);
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                        continue;
                    }
                    i += 1;
                }
                continue;
            }
            '\'' | '"' | '`' => {
                flush(&mut word, &mut words);
                let quote = c;
                i += 1;
                while i < chars.len() {
                    if chars[i] == quote {
                        if chars.get(i + 1) == Some(&quote) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            '[' => {
                flush(&mut word, &mut words);
                while i < chars.len() && chars[i] != ']' {
                    i += 1;
                }
            }
            c if c.is_alphanumeric() || c == '_' => word.push(c),
            _ => flush(&mut word, &mut words),
        }
        i += 1;
    }
    flush(&mut word, &mut words);
    words
}

/// The PEP 249 exception name and SQLSTATE (or driver code) of a raw SQL error.
pub fn error_kind(e: &SqlError, flavor: Flavor) -> (&'static str, Option<String>) {
    match e {
        SqlError::Driver(sqlx::Error::Database(db)) => {
            let code = db.code().map(|c| c.to_string());
            if matches!(
                db.kind(),
                sqlx::error::ErrorKind::UniqueViolation
                    | sqlx::error::ErrorKind::ForeignKeyViolation
                    | sqlx::error::ErrorKind::NotNullViolation
                    | sqlx::error::ErrorKind::CheckViolation
            ) {
                return ("IntegrityError", code);
            }
            // `sqlite3` raises `OperationalError` for its generic errors ("no such table",
            // syntax errors, a locked database).
            if flavor.driver_family() == DriverFamily::Sqlite {
                return ("OperationalError", code);
            }
            let kind = match code.as_deref().map(|c| c.get(..2).unwrap_or(c)) {
                Some("23") => "IntegrityError",
                Some("22") => "DataError",
                Some("42" | "3D" | "3F") => "ProgrammingError",
                Some("08" | "40" | "53" | "55" | "57" | "58" | "HY") => "OperationalError",
                Some("0A") => "NotSupportedError",
                Some("XX" | "25" | "2D" | "2B") => "InternalError",
                _ => "DatabaseError",
            };
            (kind, code)
        }
        SqlError::Driver(_) | SqlError::Connect(_) => ("OperationalError", None),
        SqlError::Database(message) if flavor == Flavor::Oracle => {
            // python-oracledb raises `IntegrityError` for constraint errors and `DatabaseError`
            // for the others (a missing table, a syntax error).
            let m = message.to_ascii_lowercase();
            let integrity = [
                "ora-00001",
                "ora-01400",
                "ora-02290",
                "ora-02291",
                "ora-02292",
            ]
            .iter()
            .any(|code| m.contains(code));
            (
                if integrity {
                    "IntegrityError"
                } else {
                    "DatabaseError"
                },
                None,
            )
        }
        SqlError::Database(message) => {
            let m = message.to_ascii_lowercase();
            let kind = if m.contains("ora-00001")
                || m.contains("constraint")
                || m.contains("duplicate key")
                || m.contains("violation of primary key")
                || m.contains("cannot insert duplicate")
                || m.contains("cannot insert the value null")
            {
                "IntegrityError"
            } else if m.contains("syntax")
                || m.contains("parser error")
                || m.contains("catalog error")
                || m.contains("binder error")
                || m.contains("invalid object name")
                || m.contains("invalid column name")
                || m.contains("ora-00942")
                || m.contains("ora-00904")
                || m.contains("does not exist")
            {
                "ProgrammingError"
            } else if m.contains("conversion") || m.contains("out of range") {
                "DataError"
            } else {
                "DatabaseError"
            };
            (kind, None)
        }
        SqlError::Protected(_) => ("PermissionError", None),
        _ => ("DatabaseError", None),
    }
}

/// The text of a raw SQL error: the database's own message for database errors (without the
/// driver's `error returned from database:` prefix), else the error's display.
pub fn error_message(e: &SqlError) -> String {
    match e {
        SqlError::Driver(sqlx::Error::Database(db)) => db.message().to_string(),
        other => other.to_string(),
    }
}

/// Microseconds between the Unix epoch and PostgreSQL's (2000-01-01).
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
/// Days between the Unix epoch and PostgreSQL's.
const PG_EPOCH_DAYS: i32 = 10_957;

/// psycopg's `interval` (`IntervalBinaryLoader`): `timedelta(days, microseconds)`, a month as 30
/// days and a year as 365.
fn pg_interval_micros(b: &[u8]) -> Option<i64> {
    let micros = i64::from_be_bytes(b.get(0..8)?.try_into().ok()?);
    let mut days = i32::from_be_bytes(b.get(8..12)?.try_into().ok()?) as i64;
    let months = i32::from_be_bytes(b.get(12..16)?.try_into().ok()?) as i64;
    if months != 0 {
        let (years, rest) = (months.abs() / 12, months.abs() % 12);
        let extra = 30 * rest + 365 * years;
        days += if months > 0 { extra } else { -extra };
    }
    days.checked_mul(86_400_000_000)?.checked_add(micros)
}

/// `inet` / `cidr` as `addr/bits` (what `str()` of psycopg's `ipaddress` objects shows).
fn pg_inet_text(b: &[u8]) -> Option<String> {
    let (family, bits, nb) = (*b.first()?, *b.get(1)?, *b.get(3)? as usize);
    let addr = b.get(4..4 + nb)?;
    let ip = match (family, nb) {
        (2, 4) => std::net::IpAddr::from(<[u8; 4]>::try_from(addr).ok()?),
        (3, 16) => std::net::IpAddr::from(<[u8; 16]>::try_from(addr).ok()?),
        _ => return None,
    };
    Some(format!("{ip}/{bits}"))
}

/// The Arrow type of a PostgreSQL array element OID this module decodes.
fn pg_element_type(oid: u32) -> Option<(DataType, Option<(&'static str, &'static str)>)> {
    Some(match oid {
        16 => (DataType::Boolean, None),
        20 | 21 | 23 | 26 => (DataType::Int64, None),
        700 | 701 => (DataType::Float64, None),
        19 | 25 | 1042 | 1043 => (DataType::Utf8, None),
        1700 => (DataType::Utf8, Some((super::batch::META_DTYPE, "numeric"))),
        2950 => (
            DataType::FixedSizeBinary(16),
            Some((super::batch::META_EXTENSION_NAME, "arrow.uuid")),
        ),
        1082 => (DataType::Date32, None),
        1114 => (
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            None,
        ),
        1184 => (
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            None,
        ),
        _ => return None,
    })
}

/// A one-dimensional PostgreSQL array (binary format): its element OID and elements.
fn pg_array_elements(b: &[u8]) -> Option<(u32, Vec<Option<&[u8]>>)> {
    let ndim = i32::from_be_bytes(b.get(0..4)?.try_into().ok()?);
    let oid = u32::from_be_bytes(b.get(8..12)?.try_into().ok()?);
    if ndim == 0 {
        return Some((oid, Vec::new()));
    }
    if ndim != 1 {
        return None;
    }
    let len = i32::from_be_bytes(b.get(12..16)?.try_into().ok()?).max(0) as usize;
    let mut pos = 20;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        let n = i32::from_be_bytes(b.get(pos..pos + 4)?.try_into().ok()?);
        pos += 4;
        if n < 0 {
            out.push(None);
            continue;
        }
        let n = n as usize;
        out.push(Some(b.get(pos..pos + n)?));
        pos += n;
    }
    Some((oid, out))
}

/// One decoded array element as an Arrow scalar array of length one (appended by the caller).
fn pg_element_array(oid: u32, values: &[Option<&[u8]>]) -> Option<ArrayRef> {
    use arrow::array::{
        BooleanBuilder, Date32Builder, FixedSizeBinaryBuilder, TimestampMicrosecondBuilder,
    };
    let be8 = |b: &[u8]| -> Option<[u8; 8]> { b.try_into().ok() };
    Some(match oid {
        16 => {
            let mut a = BooleanBuilder::new();
            for v in values {
                a.append_option(v.map(|b| b.first().copied().unwrap_or(0) != 0));
            }
            Arc::new(a.finish())
        }
        20 | 21 | 23 | 26 => {
            let mut a = Int64Builder::new();
            for v in values {
                a.append_option(match v {
                    None => None,
                    Some(b) => Some(match (oid, b.len()) {
                        (21, 2) => i16::from_be_bytes((*b).try_into().ok()?) as i64,
                        (23, 4) => i32::from_be_bytes((*b).try_into().ok()?) as i64,
                        (26, 4) => u32::from_be_bytes((*b).try_into().ok()?) as i64,
                        (20, 8) => i64::from_be_bytes(be8(b)?),
                        _ => return None,
                    }),
                });
            }
            Arc::new(a.finish())
        }
        700 | 701 => {
            let mut a = Float64Builder::new();
            for v in values {
                a.append_option(match v {
                    None => None,
                    Some(b) if oid == 700 => Some(f32::from_be_bytes((*b).try_into().ok()?) as f64),
                    Some(b) => Some(f64::from_be_bytes(be8(b)?)),
                });
            }
            Arc::new(a.finish())
        }
        19 | 25 | 1042 | 1043 => {
            let mut a = StringBuilder::new();
            for v in values {
                a.append_option(v.map(|b| String::from_utf8_lossy(b).into_owned()));
            }
            Arc::new(a.finish())
        }
        1700 => {
            let mut a = StringBuilder::new();
            for v in values {
                a.append_option(v.and_then(super::postgres::pg_numeric_text));
            }
            Arc::new(a.finish())
        }
        2950 => {
            let mut a = FixedSizeBinaryBuilder::new(16);
            for v in values {
                match v {
                    Some(b) if b.len() == 16 => a.append_value(b).ok()?,
                    _ => a.append_null(),
                }
            }
            Arc::new(a.finish())
        }
        1082 => {
            let mut a = Date32Builder::new();
            for v in values {
                a.append_option(match v {
                    None => None,
                    Some(b) => Some(i32::from_be_bytes((*b).try_into().ok()?) + PG_EPOCH_DAYS),
                });
            }
            Arc::new(a.finish())
        }
        1114 | 1184 => {
            let mut a = TimestampMicrosecondBuilder::new();
            for v in values {
                a.append_option(match v {
                    None => None,
                    Some(b) => Some(i64::from_be_bytes(be8(b)?) + PG_EPOCH_MICROS),
                });
            }
            let a = a.finish();
            Arc::new(if oid == 1184 {
                a.with_timezone("UTC")
            } else {
                a
            })
        }
        _ => return None,
    })
}

/// A PostgreSQL array column (binary values) as an Arrow list column, or `None` when a value is
/// not a one-dimensional array of a decoded element type (the column then stays bytes).
fn pg_array_column(array: &ArrayRef) -> Option<(Field, ArrayRef)> {
    let bin = array.as_any().downcast_ref::<arrow::array::BinaryArray>()?;
    let mut oid: Option<u32> = None;
    let mut rows: Vec<Option<Vec<Option<&[u8]>>>> = Vec::with_capacity(bin.len());
    for i in 0..bin.len() {
        if bin.is_null(i) {
            rows.push(None);
            continue;
        }
        let (elem_oid, values) = pg_array_elements(bin.value(i))?;
        if !values.is_empty() || oid.is_none() {
            match oid {
                Some(o) if o != elem_oid => return None,
                _ => oid = Some(elem_oid),
            }
        }
        rows.push(Some(values));
    }
    let oid = oid.unwrap_or(25);
    let (child_type, child_meta) = pg_element_type(oid)?;
    let flat: Vec<Option<&[u8]>> = rows.iter().flatten().flatten().copied().collect();
    let values = pg_element_array(oid, &flat)?;
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0i32);
    let mut nulls = Vec::with_capacity(rows.len());
    for row in &rows {
        let last = *offsets.last().expect("offsets");
        offsets.push(last + row.as_ref().map(|r| r.len() as i32).unwrap_or(0));
        nulls.push(row.is_some());
    }
    let mut child = Field::new("item", child_type, true);
    if let Some((k, v)) = child_meta {
        let md: std::collections::HashMap<String, String> =
            [(k.to_string(), v.to_string())].into_iter().collect();
        child = child.with_metadata(md);
    }
    let child = Arc::new(child);
    let list = arrow::array::ListArray::try_new(
        child.clone(),
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        values,
        Some(arrow::buffer::NullBuffer::from(nulls)),
    )
    .ok()?;
    Some((
        Field::new("", DataType::List(child), true),
        Arc::new(list) as ArrayRef,
    ))
}

/// A raw PostgreSQL batch with the binary formats the generic reader leaves as bytes decoded as
/// psycopg loads them: `interval` as durations (`timedelta`), `inet` / `cidr` as text, and
/// one-dimensional arrays of scalars as lists. Other unknown types stay bytes.
pub(crate) fn decode_pg_raw(batch: RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    let raw_typed = |f: &Field| {
        f.data_type() == &DataType::Binary
            && !f.metadata().contains_key(super::batch::META_DTYPE)
            && f.metadata().contains_key(META_DB_TYPE)
    };
    if !schema.fields().iter().any(|f| raw_typed(f)) {
        return batch;
    }
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        let db_type = field
            .metadata()
            .get(META_DB_TYPE)
            .map(|t| t.to_ascii_uppercase())
            .unwrap_or_default();
        let decoded: Option<(DataType, ArrayRef)> = if !raw_typed(field) {
            None
        } else if db_type == "INTERVAL" {
            let bin = array.as_any().downcast_ref::<arrow::array::BinaryArray>();
            bin.map(|bin| {
                let d: arrow::array::DurationMicrosecondArray = (0..bin.len())
                    .map(|i| {
                        (!bin.is_null(i))
                            .then(|| pg_interval_micros(bin.value(i)))
                            .flatten()
                    })
                    .collect();
                (
                    DataType::Duration(arrow::datatypes::TimeUnit::Microsecond),
                    Arc::new(d) as ArrayRef,
                )
            })
        } else if db_type == "INET" || db_type == "CIDR" {
            let bin = array.as_any().downcast_ref::<arrow::array::BinaryArray>();
            bin.map(|bin| {
                let t: arrow::array::StringArray = (0..bin.len())
                    .map(|i| {
                        (!bin.is_null(i))
                            .then(|| pg_inet_text(bin.value(i)))
                            .flatten()
                    })
                    .collect();
                (DataType::Utf8, Arc::new(t) as ArrayRef)
            })
        } else if db_type.ends_with("[]") {
            pg_array_column(array).map(|(f, a)| (f.data_type().clone(), a))
        } else {
            None
        };
        match decoded {
            Some((data_type, a)) => {
                fields.push(
                    Field::new(field.name(), data_type, true)
                        .with_metadata(field.metadata().clone()),
                );
                arrays.push(a);
            }
            None => {
                fields.push(field.as_ref().clone());
                arrays.push(array.clone());
            }
        }
    }
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .unwrap_or(batch)
}

/// The batches of a raw read, one at a time. Each batch carries its own schema: a raw SQLite
/// read types every chunk from its own values, as `sqlite3` + pandas do chunk by chunk.
pub struct RawChunks {
    /// The result's column names (also when no rows come back).
    pub columns: Vec<String>,
    rx: mpsc::Receiver<Result<RecordBatch>>,
}

impl RawChunks {
    /// The next batch, `None` at the end.
    pub async fn next(&mut self) -> Option<Result<RecordBatch>> {
        self.rx.recv().await
    }

    /// Every remaining batch.
    pub async fn collect(mut self) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        while let Some(batch) = self.next().await {
            out.push(batch?);
        }
        Ok(out)
    }

    fn from_batches(columns: Vec<String>, batches: Vec<RecordBatch>) -> Self {
        let (tx, rx) = mpsc::channel(batches.len().max(1));
        for batch in batches {
            // The channel holds every batch: `try_send` cannot fail.
            let _ = tx.try_send(Ok(batch));
        }
        Self { columns, rx }
    }
}

/// `batch` cut into batches of at most `chunksize` rows (an empty batch stays one batch).
fn split_batch(batch: RecordBatch, chunksize: usize) -> Vec<RecordBatch> {
    let n = batch.num_rows();
    if n <= chunksize {
        return vec![batch];
    }
    (0..n)
        .step_by(chunksize)
        .map(|start| batch.slice(start, chunksize.min(n - start)))
        .collect()
}

/// The column names of a schema.
fn names(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// One SQLite value's storage class code and decoded cell.
enum Cell {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Cell {
    fn class(&self) -> u8 {
        match self {
            Cell::Null => b'n',
            Cell::Int(_) => b'i',
            Cell::Real(_) => b'f',
            Cell::Text(_) => b's',
            Cell::Blob(_) => b'b',
        }
    }

    /// `CAST(value AS TEXT)` (blobs as lowercase hex).
    fn text(&self) -> Option<String> {
        Some(match self {
            Cell::Null => return None,
            Cell::Int(i) => i.to_string(),
            Cell::Real(f) => meerschaum_core::pyfmt::float_repr(*f),
            Cell::Text(s) => s.clone(),
            Cell::Blob(b) => b.iter().map(|x| format!("{x:02x}")).collect(),
        })
    }
}

fn sqlite_cell(row: &SqliteRow, idx: usize) -> Result<Cell> {
    let raw = row.try_get_raw(idx)?;
    if raw.is_null() {
        return Ok(Cell::Null);
    }
    let storage = raw.type_info().name().to_string();
    let conv = |e: sqlx::Error| SqlError::Conversion {
        column: row.columns()[idx].name().to_string(),
        reason: e.to_string(),
    };
    Ok(match storage.as_str() {
        "INTEGER" => Cell::Int(row.try_get_unchecked::<i64, _>(idx).map_err(conv)?),
        "REAL" => Cell::Real(row.try_get_unchecked::<f64, _>(idx).map_err(conv)?),
        "BLOB" => Cell::Blob(row.try_get_unchecked::<Vec<u8>, _>(idx).map_err(conv)?),
        _ => Cell::Text(row.try_get_unchecked::<String, _>(idx).map_err(conv)?),
    })
}

/// A field with its declared database type (when SQLite reports one).
fn field(name: &str, data_type: DataType, db_type: &str, extra: Option<(&str, String)>) -> Field {
    let mut md = std::collections::HashMap::new();
    if !db_type.is_empty() && db_type != "NULL" {
        md.insert(META_DB_TYPE.to_string(), db_type.to_string());
    }
    if let Some((k, v)) = extra {
        md.insert(k.to_string(), v);
    }
    Field::new(name, data_type, true).with_metadata(md)
}

/// SQLite rows as one record batch, each column typed from its values' storage classes.
pub(crate) fn sqlite_batch(
    rows: &[SqliteRow],
    columns: &[(String, String)],
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (idx, (name, db_type)) in columns.iter().enumerate() {
        let cells = rows
            .iter()
            .map(|r| sqlite_cell(r, idx))
            .collect::<Result<Vec<_>>>()?;
        let mut classes = cells.iter().map(Cell::class).filter(|c| *c != b'n');
        let first = classes.next();
        let uniform = match first {
            None => None,
            Some(c) => classes.all(|x| x == c).then_some(c),
        };
        let (data_type, array, extra): (DataType, ArrayRef, Option<(&str, String)>) =
            match (first, uniform) {
                (None, _) => (DataType::Null, Arc::new(NullArray::new(cells.len())), None),
                (_, Some(b'i')) => {
                    let mut b = Int64Builder::with_capacity(cells.len());
                    for c in &cells {
                        b.append_option(match c {
                            Cell::Int(i) => Some(*i),
                            _ => None,
                        });
                    }
                    (DataType::Int64, Arc::new(b.finish()), None)
                }
                (_, Some(b'f')) => {
                    let mut b = Float64Builder::with_capacity(cells.len());
                    for c in &cells {
                        b.append_option(match c {
                            Cell::Real(f) => Some(*f),
                            _ => None,
                        });
                    }
                    (DataType::Float64, Arc::new(b.finish()), None)
                }
                (_, Some(b'b')) => {
                    let mut b = BinaryBuilder::with_capacity(cells.len(), cells.len() * 16);
                    for c in &cells {
                        match c {
                            Cell::Blob(v) => b.append_value(v),
                            _ => b.append_null(),
                        }
                    }
                    (DataType::Binary, Arc::new(b.finish()), None)
                }
                (_, Some(_)) => {
                    let mut b = StringBuilder::with_capacity(cells.len(), cells.len() * 16);
                    for c in &cells {
                        match c {
                            Cell::Text(s) => b.append_value(s),
                            _ => b.append_null(),
                        }
                    }
                    (DataType::Utf8, Arc::new(b.finish()), None)
                }
                (_, None) => {
                    let mut b = StringBuilder::with_capacity(cells.len(), cells.len() * 16);
                    let mut codes = String::with_capacity(cells.len());
                    for c in &cells {
                        codes.push(c.class() as char);
                        b.append_option(c.text());
                    }
                    (
                        DataType::Utf8,
                        Arc::new(b.finish()),
                        Some((META_SQLITE_CLASSES, codes)),
                    )
                }
            };
        fields.push(field(name, data_type, db_type, extra));
        arrays.push(array);
    }
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}

/// The `(name, declared type)` of each result column of a SQLite row.
fn sqlite_columns(row: &SqliteRow) -> Vec<(String, String)> {
    row.columns()
        .iter()
        .map(|c| (c.name().to_string(), c.type_info().name().to_string()))
        .collect()
}

/// The `(name, declared type)` of each result column of a prepared SQLite statement.
fn sqlite_statement_columns(stmt: &SqliteStatement) -> Vec<(String, String)> {
    stmt.columns()
        .iter()
        .map(|c| (c.name().to_string(), c.type_info().name().to_string()))
        .collect()
}

/// An empty batch with one all-null column per name (the columns of a read without rows).
fn empty_batch(columns: &[(String, String)]) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(n, t)| field(n, DataType::Null, t, None))
        .collect();
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|_| Arc::new(NullArray::new(0)) as ArrayRef)
        .collect();
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(0)),
    )
    .expect("an empty batch of null columns")
}

fn bind_sqlite<'q>(
    sql: &'q str,
    params: &'q [SqlParam],
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments> {
    let mut q = sqlx::query::<sqlx::Sqlite>(AssertSqlSafe(sql)).persistent(false);
    for p in params {
        q = super::params::bind_sqlite(q, p);
    }
    q
}

/// A raw SQLite read on its own pooled connection, `chunksize` rows per batch.
async fn sqlite_chunks(
    pool: sqlx::SqlitePool,
    sql: String,
    params: Vec<SqlParam>,
    chunksize: usize,
) -> Result<RawChunks> {
    let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(2);
    let (columns_tx, columns_rx) = tokio::sync::oneshot::channel::<Result<Vec<String>>>();
    tokio::spawn(async move {
        let mut columns_tx = Some(columns_tx);
        let mut conn = match pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                let _ = columns_tx.take().map(|c| c.send(Err(e.into())));
                return;
            }
        };
        let mut columns: Option<Vec<(String, String)>> = None;
        let mut buf: Vec<SqliteRow> = Vec::with_capacity(chunksize.min(4096));
        {
            let mut rows = bind_sqlite(&sql, &params).fetch(&mut *conn);
            loop {
                match rows.next().await {
                    Some(Ok(row)) => {
                        if columns.is_none() {
                            let cols = sqlite_columns(&row);
                            let names = cols.iter().map(|(n, _)| n.clone()).collect();
                            if let Some(c) = columns_tx.take()
                                && c.send(Ok(names)).is_err()
                            {
                                return;
                            }
                            columns = Some(cols);
                        }
                        buf.push(row);
                        if buf.len() >= chunksize {
                            let item = sqlite_batch(&buf, columns.as_deref().unwrap_or(&[]));
                            buf.clear();
                            let failed = item.is_err();
                            if tx.send(item).await.is_err() || failed {
                                return;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        let err = SqlError::from(e);
                        match columns_tx.take() {
                            Some(c) => {
                                let _ = c.send(Err(err));
                            }
                            None => {
                                let _ = tx.send(Err(err)).await;
                            }
                        }
                        return;
                    }
                    None => break,
                }
            }
        }
        match columns {
            Some(cols) => {
                if !buf.is_empty() {
                    let _ = tx.send(sqlite_batch(&buf, &cols)).await;
                }
            }
            None => {
                // No rows: the prepared statement names the columns.
                let cols = match (&mut *conn)
                    .prepare(AssertSqlSafe(sql.as_str()).into_sql_str())
                    .await
                {
                    Ok(stmt) => sqlite_statement_columns(&stmt),
                    Err(_) => Vec::new(),
                };
                let names = cols.iter().map(|(n, _)| n.clone()).collect();
                if let Some(c) = columns_tx.take()
                    && c.send(Ok(names)).is_err()
                {
                    return;
                }
                let _ = tx.send(Ok(empty_batch(&cols))).await;
            }
        }
    });
    let columns = columns_rx.await.map_err(|_| SqlError::Conversion {
        column: String::new(),
        reason: "the read task ended before naming its columns".to_string(),
    })??;
    Ok(RawChunks { columns, rx })
}

/// A raw SQLite read inside an open transaction (one batch, or an empty one with the columns).
async fn sqlite_tx_rows(
    tx: &mut sqlx::Transaction<'static, sqlx::Sqlite>,
    sql: &str,
    params: &[SqlParam],
) -> Result<RecordBatch> {
    let rows = bind_sqlite(sql, params).fetch_all(&mut **tx).await?;
    match rows.first() {
        Some(first) => sqlite_batch(&rows, &sqlite_columns(first)),
        None => {
            let cols = match (&mut **tx).prepare(AssertSqlSafe(sql).into_sql_str()).await {
                Ok(stmt) => sqlite_statement_columns(&stmt),
                Err(_) => Vec::new(),
            };
            Ok(empty_batch(&cols))
        }
    }
}

impl SqlDriver {
    /// Run a row-returning statement with bound parameters, its rows in batches of at most
    /// `chunksize`. Without parameters the rows stream from the database (one batch in memory
    /// at a time); with parameters they are fetched first and then handed out batch by batch.
    pub async fn raw_query(
        &self,
        sql: &str,
        params: &[SqlParam],
        chunksize: usize,
    ) -> Result<RawChunks> {
        meerschaum_core::dprint!(
            "{}{}",
            meerschaum_core::debug::redact_sql(sql),
            if params.is_empty() {
                String::new()
            } else {
                format!("\n({} bound parameters)", params.len())
            }
        );
        self.ready().await?;
        let chunksize = chunksize.max(1);
        if let Pool::Sqlite(pool) = &self.pool {
            return sqlite_chunks(pool.clone(), sql.to_string(), params.to_vec(), chunksize).await;
        }
        let postgres = matches!(self.pool, Pool::Postgres(_));
        let finish = move |batch: RecordBatch| {
            if postgres {
                decode_pg_raw(batch)
            } else {
                batch
            }
        };
        if params.is_empty() {
            let mut stream = self.read_stream(sql, chunksize).await?;
            let columns = names(&stream.schema());
            let schema = stream.schema();
            let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(2);
            tokio::spawn(async move {
                let mut sent = false;
                while let Some(item) = stream.next().await {
                    let failed = item.is_err();
                    if tx.send(item.map(finish)).await.is_err() || failed {
                        return;
                    }
                    sent = true;
                }
                if !sent {
                    let _ = tx.send(Ok(finish(RecordBatch::new_empty(schema)))).await;
                }
            });
            return Ok(RawChunks { columns, rx });
        }
        let batches: Vec<RecordBatch> = self
            .read_with(sql, params)
            .await?
            .into_iter()
            .map(finish)
            .collect();
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        let columns = names(&schema);
        let mut out = Vec::new();
        for batch in batches {
            out.extend(split_batch(batch, chunksize));
        }
        if out.is_empty() {
            out.push(RecordBatch::new_empty(schema));
        }
        Ok(RawChunks::from_batches(columns, out))
    }

    /// Execute a statement that returns no rows, with bound parameters (one statement), or
    /// without (any number of statements, simple-query protocol). Returns the rows affected.
    pub async fn raw_execute(&self, sql: &str, params: &[SqlParam]) -> Result<u64> {
        if params.is_empty() {
            self.exec(sql).await
        } else {
            self.exec_with(sql, params).await
        }
    }

    /// Execute one statement once per parameter set in one transaction (`executemany`), rolling
    /// back at the first failure (which is returned). Returns the total rows affected.
    pub async fn raw_execute_many(&self, sql: &str, param_sets: &[Vec<SqlParam>]) -> Result<u64> {
        let mut tx = self.begin_write().await?;
        let mut total = 0u64;
        for params in param_sets {
            match tx.raw_execute(sql, params).await {
                Ok(n) => total += n,
                Err(e) => {
                    let _ = tx.rollback().await;
                    return Err(e);
                }
            }
        }
        tx.commit().await?;
        Ok(total)
    }
}

impl SqlTransaction {
    /// [`SqlDriver::raw_query`] inside this transaction (the rows in one batch).
    pub async fn raw_query(&mut self, sql: &str, params: &[SqlParam]) -> Result<RecordBatch> {
        meerschaum_core::dprint!("{}", meerschaum_core::debug::redact_sql(sql));
        let batches = match &mut self.inner {
            TxInner::Sqlite(tx) => return sqlite_tx_rows(tx, sql, params).await,
            TxInner::Postgres(tx) => super::backend::pg_tx::tx_read_with(tx, sql, params)
                .await?
                .into_iter()
                .map(decode_pg_raw)
                .collect(),
            TxInner::Mysql(tx) => super::backend::mysql_tx::tx_read_with(tx, sql, params).await?,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.read_with(sql, params).await?,
        };
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        if batches.len() == 1 {
            return Ok(batches.into_iter().next().expect("one batch"));
        }
        Ok(arrow::compute::concat_batches(&schema, &batches)?)
    }

    /// [`SqlDriver::raw_execute`] inside this transaction.
    pub async fn raw_execute(&mut self, sql: &str, params: &[SqlParam]) -> Result<u64> {
        if params.is_empty() {
            return self.exec(sql).await;
        }
        meerschaum_core::dprint!(
            "{}\n({} bound parameters)",
            meerschaum_core::debug::redact_sql(sql),
            params.len()
        );
        match &mut self.inner {
            TxInner::Sqlite(tx) => Ok(bind_sqlite(sql, params)
                .execute(&mut **tx)
                .await?
                .rows_affected()),
            TxInner::Postgres(tx) => super::backend::pg_tx::tx_exec_with(tx, sql, params).await,
            TxInner::Mysql(tx) => super::backend::mysql_tx::tx_exec_with(tx, sql, params).await,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.exec_with(sql, params).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_that_return_rows() {
        let f = Flavor::Postgresql;
        for sql in [
            "SELECT 1",
            "  select * from t",
            "-- comment\nSELECT 1",
            "/* a /* nested */ b */ (SELECT 1) UNION (SELECT 2)",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "VALUES (1), (2)",
            "INSERT INTO t (a) VALUES (1) RETURNING a",
            "update t set a = 1 returning *",
            "EXPLAIN SELECT 1",
            "SHOW search_path",
        ] {
            assert!(returns_rows(sql, f), "{sql}");
        }
        for sql in [
            "",
            "-- only a comment",
            "CREATE TABLE t (a INT)",
            "INSERT INTO t VALUES ('returning')",
            "INSERT INTO \"returning\" VALUES (1)",
            "DELETE FROM t -- returning",
            "DROP TABLE t",
            "BEGIN",
        ] {
            assert!(!returns_rows(sql, f), "{sql}");
        }
        assert!(returns_rows("PRAGMA table_info(t)", Flavor::Sqlite));
        assert!(returns_rows("FROM t", Flavor::Duckdb));
        assert!(!returns_rows("FROM t", Flavor::Postgresql));
        assert!(returns_rows(
            "INSERT INTO t (a) OUTPUT INSERTED.a VALUES (1)",
            Flavor::Mssql
        ));
        assert!(returns_rows("EXEC sp_who", Flavor::Mssql));
        assert!(!returns_rows("EXEC sp_who", Flavor::Postgresql));
        assert!(!returns_rows(
            "INSERT INTO t (a) OUTPUT INSERTED.a VALUES (1)",
            Flavor::Postgresql
        ));
    }

    #[test]
    fn postgres_binary_formats_decode_like_psycopg() {
        // 1 day, 2 hours; 14 months = 1 year + 2 months = 365 + 60 days.
        let mut b = Vec::new();
        b.extend_from_slice(&(2 * 3_600_000_000i64).to_be_bytes());
        b.extend_from_slice(&1i32.to_be_bytes());
        b.extend_from_slice(&14i32.to_be_bytes());
        assert_eq!(
            pg_interval_micros(&b),
            Some((1 + 365 + 60) * 86_400_000_000 + 2 * 3_600_000_000)
        );
        assert_eq!(
            pg_inet_text(&[2, 32, 0, 4, 127, 0, 0, 1]).as_deref(),
            Some("127.0.0.1/32")
        );
        let mut v6 = vec![3, 64, 1, 16];
        v6.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8]);
        v6.extend_from_slice(&[0; 12]);
        assert_eq!(pg_inet_text(&v6).as_deref(), Some("2001:db8::/64"));
        // ARRAY[1, NULL, 3]::int4[]
        let mut arr = Vec::new();
        for v in [1i32, 1, 23, 3, 1] {
            arr.extend_from_slice(&v.to_be_bytes());
        }
        arr.extend_from_slice(&4i32.to_be_bytes());
        arr.extend_from_slice(&1i32.to_be_bytes());
        arr.extend_from_slice(&(-1i32).to_be_bytes());
        arr.extend_from_slice(&4i32.to_be_bytes());
        arr.extend_from_slice(&3i32.to_be_bytes());
        let column: ArrayRef = Arc::new(arrow::array::BinaryArray::from(vec![
            Some(arr.as_slice()),
            None,
        ]));
        let (field, list) = pg_array_column(&column).unwrap();
        assert!(matches!(field.data_type(), DataType::List(_)));
        let list = list
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert!(list.is_null(1));
        let first = list.value(0);
        let ints = first
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(
            ints.iter().collect::<Vec<_>>(),
            vec![Some(1), None, Some(3)]
        );
    }

    #[test]
    fn batches_split_into_chunks() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let array: ArrayRef = Arc::new(arrow::array::Int64Array::from_iter_values(0..10));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let chunks = split_batch(batch.clone(), 4);
        assert_eq!(
            chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
        assert_eq!(split_batch(batch, 100).len(), 1);
    }

    async fn memory_driver() -> SqlDriver {
        let attrs = crate::uri::ConnectorAttributes {
            flavor: Some(Flavor::Sqlite),
            database: Some(":memory:".into()),
            ..Default::default()
        };
        let spec = crate::uri::ConnectionSpec::from_attributes(&attrs).unwrap();
        SqlDriver::connect(&spec).await.unwrap()
    }

    #[tokio::test]
    async fn sqlite_columns_follow_storage_classes() {
        let driver = memory_driver().await;
        driver
            .exec(
                "CREATE TABLE t (i INTEGER, ts DATETIME, m, b BLOB, n NUMERIC);
                 INSERT INTO t VALUES (1, '2024-01-01 00:00:00', 5, x'00ff', '1.5');
                 INSERT INTO t VALUES (NULL, NULL, 'text', NULL, NULL);
                 INSERT INTO t VALUES (3, '2024-01-03', 2.5, x'01', 2);",
            )
            .await
            .unwrap();
        let chunks = driver
            .raw_query(
                "SELECT * FROM t WHERE i IS NULL OR i > ?",
                &[SqlParam::Int(0)],
                2,
            )
            .await
            .unwrap();
        assert_eq!(chunks.columns, vec!["i", "ts", "m", "b", "n"]);
        let batches = chunks.collect().await.unwrap();
        assert_eq!(batches.len(), 2);
        let first = &batches[0];
        let schema = first.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        // A `DATETIME` column of text values is text, as `sqlite3` returns it.
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            schema
                .field(1)
                .metadata()
                .get(META_DB_TYPE)
                .map(String::as_str),
            Some("DATETIME")
        );
        // Mixed classes: text tagged with each row's class.
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(
            schema
                .field(2)
                .metadata()
                .get(META_SQLITE_CLASSES)
                .map(String::as_str),
            Some("is")
        );
        assert_eq!(schema.field(3).data_type(), &DataType::Binary);
        // `NUMERIC` affinity stored 1.5 as REAL.
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
        // The second chunk is typed from its own values.
        let second = batches[1].schema();
        assert_eq!(second.field(2).data_type(), &DataType::Float64);
        assert_eq!(second.field(4).data_type(), &DataType::Int64);
    }

    #[tokio::test]
    async fn an_empty_sqlite_read_names_its_columns() {
        let driver = memory_driver().await;
        driver
            .exec("CREATE TABLE e (a INTEGER, b TEXT)")
            .await
            .unwrap();
        let chunks = driver
            .raw_query("SELECT a, b AS bee FROM e", &[], 10)
            .await
            .unwrap();
        assert_eq!(chunks.columns, vec!["a", "bee"]);
        let batches = chunks.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
        assert_eq!(batches[0].schema().field(1).name(), "bee");
    }

    #[tokio::test]
    async fn statements_run_in_transactions_with_parameters() {
        let driver = memory_driver().await;
        driver
            .exec("CREATE TABLE x (a INTEGER NOT NULL)")
            .await
            .unwrap();
        let mut tx = driver.begin().await.unwrap();
        assert_eq!(
            tx.raw_execute("INSERT INTO x VALUES (?)", &[SqlParam::Int(7)])
                .await
                .unwrap(),
            1
        );
        let batch = tx
            .raw_query("SELECT a FROM x WHERE a = ?", &[SqlParam::Int(7)])
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        tx.rollback().await.unwrap();
        let after = driver.raw_query("SELECT a FROM x", &[], 10).await.unwrap();
        let batches = after.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 0);
        assert_eq!(
            driver
                .raw_execute_many(
                    "INSERT INTO x VALUES (?)",
                    &[vec![SqlParam::Int(1)], vec![SqlParam::Int(2)]]
                )
                .await
                .unwrap(),
            2
        );
        let err = driver
            .raw_execute_many(
                "INSERT INTO x VALUES (?)",
                &[vec![SqlParam::Int(3)], vec![SqlParam::Null]],
            )
            .await;
        assert!(err.is_err());
        let rows = driver
            .raw_query("SELECT COUNT(*) AS n FROM x", &[], 10)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let n = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2, "the failed executemany rolled back");
    }
}
