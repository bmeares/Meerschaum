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
    let database = spec.database.as_deref().unwrap_or(":memory:");
    let opts = if database == ":memory:" || database.is_empty() {
        SqliteConnectOptions::from_str("sqlite::memory:")?
    } else {
        // TODO(connector-registry): `{SQLITE_DB_PATH}` is resolved from core's `Paths` when the
        // connector registry builds the spec.
        SqliteConnectOptions::new()
            .filename(database)
            .create_if_missing(true)
    };
    // Python's `sqlite3` leaves foreign keys off and keeps the rollback journal (`DELETE`);
    // sqlx turns foreign keys on and switches files to WAL by default.
    Ok(opts
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
