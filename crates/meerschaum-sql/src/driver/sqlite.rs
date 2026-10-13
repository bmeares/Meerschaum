//! SQLite family (`sqlite`, `geopackage`) over `sqlx::sqlite`.
//!
//! SQLite is dynamically typed: a column's kind comes from its declared type (as sqlx reports
//! it) and is refined from the first batch's storage classes; mismatched cells are coerced.

use std::str::FromStr;

use sqlx::sqlite::{
    Sqlite, SqliteColumn, SqliteConnectOptions, SqlitePool, SqlitePoolOptions, SqliteRow,
};
use sqlx::{Column, Row, TypeInfo, ValueRef};

use super::SqlValue;
use super::backend::Backend;
use super::batch::{ColumnKind, ColumnPlan, text_has_offset};
use crate::error::{Result, SqlError};
use crate::uri::ConnectionSpec;

pub(crate) struct SqliteBackend;

pub(crate) fn connect_options(spec: &ConnectionSpec) -> Result<SqliteConnectOptions> {
    let resolved = spec.resolved_database()?;
    let database = resolved.as_deref().unwrap_or(":memory:");
    let opts = if database == ":memory:" || database.is_empty() {
        SqliteConnectOptions::from_str("sqlite::memory:")?
    } else {
        SqliteConnectOptions::new()
            .filename(database)
            .create_if_missing(true)
    };
    // Python's `sqlite3` leaves foreign keys off and keeps the rollback journal (`DELETE`);
    // sqlx turns foreign keys on and switches files to WAL by default.
    Ok(opts
        .row_buffer_size(8192)
        .foreign_keys(false)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Delete))
}

/// The pool options: on `geopackage`, every connection gets the GeoPackage SQL functions the
/// R-tree triggers call.
pub(crate) fn pool_options(spec: &ConnectionSpec) -> SqlitePoolOptions {
    let pool = SqlitePoolOptions::new().max_connections(spec.max_connections());
    if spec.flavor == crate::flavor::Flavor::Geopackage {
        pool.after_connect(|conn, _meta| {
            Box::pin(async move { super::gpkg_functions::register(conn).await })
        })
    } else {
        pool
    }
}

pub(crate) async fn connect(spec: &ConnectionSpec) -> Result<SqlitePool> {
    let opts = connect_options(spec)?;
    let mut pool = pool_options(spec);
    if spec.max_connections() == 1 {
        // Keep the single in-memory connection (and therefore the database) alive.
        pool = pool
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None);
    }
    Ok(pool.connect_with(opts).await?)
}

/// The declared kind for a column, or `None` when it must be inferred from the values.
pub(crate) fn declared_kind(type_name: &str) -> Option<ColumnKind> {
    Some(match type_name.to_ascii_uppercase().as_str() {
        "INTEGER" => ColumnKind::Int64,
        "REAL" => ColumnKind::Float64,
        "TEXT" => ColumnKind::Utf8,
        "BLOB" => ColumnKind::Bytes,
        "BOOLEAN" => ColumnKind::Boolean,
        "DATE" => ColumnKind::Date,
        "TIME" => ColumnKind::Time,
        "DATETIME" => ColumnKind::Timestamp,
        "NUMERIC" => ColumnKind::Decimal,
        _ => return None,
    })
}

/// Sentinel kind for "infer from values" during planning.
const INFER: ColumnKind = ColumnKind::Raw;

#[derive(Default)]
struct Classes {
    int: bool,
    float: bool,
    text: bool,
    blob: bool,
}

fn classes(cells: &[SqlValue]) -> Classes {
    let mut c = Classes::default();
    for cell in cells {
        match cell {
            SqlValue::Int(_) => c.int = true,
            SqlValue::Float(_) => c.float = true,
            SqlValue::Text(_) => c.text = true,
            SqlValue::Bytes(_) => c.blob = true,
            _ => {}
        }
    }
    c
}

fn numeric_text(cells: &[SqlValue]) -> bool {
    cells.iter().all(|c| match c {
        SqlValue::Text(s) => s.trim().parse::<f64>().is_ok(),
        _ => true,
    })
}

fn infer_kind(cells: &[SqlValue]) -> ColumnKind {
    let c = classes(cells);
    match (c.int, c.float, c.text, c.blob) {
        (false, false, false, false) => ColumnKind::Utf8,
        (true, false, false, false) => ColumnKind::Int64,
        (_, true, false, false) => ColumnKind::Float64,
        (false, false, false, true) => ColumnKind::Bytes,
        _ => ColumnKind::Utf8,
    }
}

impl Backend for SqliteBackend {
    type DB = Sqlite;
    const DYNAMIC_TYPES: bool = true;

    fn plan(columns: &[SqliteColumn]) -> Vec<ColumnPlan> {
        columns
            .iter()
            .map(|c| {
                let ty = c.type_info().name().to_string();
                ColumnPlan::new(c.name(), declared_kind(&ty).unwrap_or(INFER), ty)
            })
            .collect()
    }

    fn cell(row: &SqliteRow, idx: usize, plan: &ColumnPlan) -> Result<SqlValue> {
        let raw = row.try_get_raw(idx)?;
        if raw.is_null() {
            return Ok(SqlValue::Null);
        }
        let storage = raw.type_info().name().to_string();
        let err = |e: sqlx::Error| SqlError::Conversion {
            column: plan.name.clone(),
            reason: e.to_string(),
        };
        // Decode by storage class; the batch builder coerces to the column's kind.
        Ok(match storage.as_str() {
            "INTEGER" => SqlValue::Int(row.try_get_unchecked::<i64, _>(idx).map_err(err)?),
            "REAL" => SqlValue::Float(row.try_get_unchecked::<f64, _>(idx).map_err(err)?),
            "BLOB" => SqlValue::Bytes(row.try_get_unchecked::<Vec<u8>, _>(idx).map_err(err)?),
            _ => SqlValue::Text(row.try_get_unchecked::<String, _>(idx).map_err(err)?),
        })
    }

    fn refine(plans: &mut [ColumnPlan], columns: &[Vec<SqlValue>]) {
        for (plan, cells) in plans.iter_mut().zip(columns) {
            let c = classes(cells);
            plan.kind = match plan.kind {
                INFER => infer_kind(cells),
                ColumnKind::Int64 if c.blob => ColumnKind::Utf8,
                ColumnKind::Int64 if c.text && !numeric_text(cells) => ColumnKind::Utf8,
                ColumnKind::Int64 if c.float || c.text => ColumnKind::Float64,
                ColumnKind::Float64 if c.blob || (c.text && !numeric_text(cells)) => {
                    ColumnKind::Utf8
                }
                ColumnKind::Timestamp => {
                    let texts: Vec<&str> = cells
                        .iter()
                        .filter_map(|c| match c {
                            SqlValue::Text(s) => Some(s.as_str()),
                            _ => None,
                        })
                        .collect();
                    if texts
                        .iter()
                        .any(|s| super::batch::parse_datetime_text(s).is_none())
                        || c.blob
                    {
                        ColumnKind::Utf8
                    } else if texts.iter().any(|s| text_has_offset(s)) {
                        ColumnKind::TimestampTz
                    } else {
                        ColumnKind::Timestamp
                    }
                }
                ColumnKind::Decimal if c.blob || (c.text && !numeric_text(cells)) => {
                    ColumnKind::Utf8
                }
                other => other,
            };
        }
    }

    fn array(rows: &[SqliteRow], idx: usize, plan: &ColumnPlan) -> Result<arrow::array::ArrayRef> {
        use arrow::array::*;
        use std::sync::Arc;
        macro_rules! primitive {
            ($builder:ty, $ty:ty) => {{
                let mut b = <$builder>::with_capacity(rows.len());
                for row in rows {
                    b.append_option(row.try_get_unchecked::<Option<$ty>, _>(idx)?);
                }
                return Ok(Arc::new(b.finish()));
            }};
        }
        // SQLite affinity is advisory. Preserve coercion when later batches change storage.
        let compatible = rows.iter().all(|row| {
            row.try_get_raw(idx).is_ok_and(|v| {
                v.is_null()
                    || match plan.kind {
                        ColumnKind::Int64 => v.type_info().name() == "INTEGER",
                        ColumnKind::Float64 => matches!(v.type_info().name(), "INTEGER" | "REAL"),
                        ColumnKind::Utf8 | ColumnKind::Timestamp | ColumnKind::TimestampTz => {
                            v.type_info().name() == "TEXT"
                        }
                        ColumnKind::Bytes => v.type_info().name() == "BLOB",
                        _ => false,
                    }
            })
        });
        if compatible {
            match plan.kind {
                ColumnKind::Int64 => primitive!(Int64Builder, i64),
                ColumnKind::Float64 => primitive!(Float64Builder, f64),
                ColumnKind::Timestamp | ColumnKind::TimestampTz => {
                    let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
                    for row in rows {
                        let value = row.try_get::<Option<&str>, _>(idx)?;
                        let dt = value
                            .map(|s| {
                                super::batch::parse_datetime_text(s)
                                    .and_then(|(n, offset)| match offset {
                                        Some(o) => {
                                            use chrono::TimeZone;
                                            o.from_local_datetime(&n)
                                                .single()
                                                .map(|d| d.timestamp_micros())
                                        }
                                        None => Some(n.and_utc().timestamp_micros()),
                                    })
                                    .ok_or_else(|| SqlError::Conversion {
                                        column: plan.name.clone(),
                                        reason: format!("Invalid timestamp: {s}"),
                                    })
                            })
                            .transpose()?;
                        b.append_option(dt);
                    }
                    let arr = b.finish();
                    return Ok(Arc::new(if plan.kind == ColumnKind::TimestampTz {
                        arr.with_timezone("UTC")
                    } else {
                        arr
                    }));
                }
                ColumnKind::Utf8 => {
                    let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                    for row in rows {
                        b.append_option(row.try_get::<Option<&str>, _>(idx)?);
                    }
                    return Ok(Arc::new(b.finish()));
                }
                ColumnKind::Bytes => {
                    let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                    for row in rows {
                        b.append_option(row.try_get::<Option<&[u8]>, _>(idx)?);
                    }
                    return Ok(Arc::new(b.finish()));
                }
                _ => {}
            }
        }
        let cells = rows
            .iter()
            .map(|r| Self::cell(r, idx, plan))
            .collect::<Result<Vec<_>>>()?;
        super::batch::build_array(plan, &cells)
    }

    fn rows_affected(result: &sqlx::sqlite::SqliteQueryResult) -> u64 {
        result.rows_affected()
    }

    fn bind<'q>(
        q: sqlx::query::Query<'q, Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
        p: &super::params::SqlParam,
    ) -> sqlx::query::Query<'q, Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
        super::params::bind_sqlite(q, p)
    }
}

/// sqlite3 mappings preserve each value's storage class, even within a mixed column.
pub(crate) async fn read_docs_frame(
    pool: &SqlitePool,
    sql: &str,
) -> Result<meerschaum_kernel::Frame> {
    use futures_util::TryStreamExt;
    use meerschaum_kernel::value::Value;
    let mut rows = sqlx::query(sql).persistent(false).fetch(pool);
    let mut columns: Vec<(String, Vec<Value>)> = Vec::new();
    while let Some(row) = rows.try_next().await? {
        if columns.is_empty() {
            columns = row
                .columns()
                .iter()
                .map(|c| (c.name().into(), Vec::new()))
                .collect();
        }
        for (i, (_, values)) in columns.iter_mut().enumerate() {
            let raw = row.try_get_raw(i)?;
            values.push(if raw.is_null() {
                Value::Null
            } else {
                match raw.type_info().name() {
                    "INTEGER" => Value::Int(row.try_get_unchecked(i)?),
                    "REAL" => Value::Float(row.try_get_unchecked(i)?),
                    "BLOB" => Value::Bytes(row.try_get_unchecked(i)?),
                    _ => Value::Str(row.try_get_unchecked(i)?),
                }
            });
        }
    }
    Ok(meerschaum_kernel::Frame::from_object_columns(columns)?)
}
