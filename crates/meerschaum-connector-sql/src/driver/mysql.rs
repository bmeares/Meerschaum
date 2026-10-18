//! MySQL family (`mysql`, `mariadb`) over `sqlx::mysql`.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use sqlx::mysql::MySqlSslMode;
use sqlx::mysql::{MySql, MySqlColumn, MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, Row, TypeInfo, ValueRef};

use super::SqlValue;
use super::backend::Backend;
use super::batch::{ColumnKind, ColumnPlan, GeometryEncoding};
use crate::error::{Result, SqlError};
use crate::uri::ConnectionSpec;

pub(crate) struct MySqlBackend;

/// Prepared statements kept per connection ([`Backend::PERSISTENT_STATEMENTS`]). The server
/// counts them across every client against `max_prepared_stmt_count` (16382 by default), so a
/// full pool of a few dozen processes must stay below it; `sqlx` closes the least recently used
/// statement beyond this.
const STATEMENT_CACHE_CAPACITY: usize = 32;

pub(crate) fn connect_options(spec: &ConnectionSpec) -> Result<MySqlConnectOptions> {
    // PyMySQL does not alter `sql_mode`; sqlx appends PIPES_AS_CONCAT and NO_ENGINE_SUBSTITUTION
    // by default, which would change the meaning of `||`. The session time zone stays at sqlx's
    // `+00:00` so `TIMESTAMP` values decode as UTC (Meerschaum stores naive UTC on MySQL).
    let mut opts = MySqlConnectOptions::new()
        .pipes_as_concat(false)
        .no_engine_substitution(false)
        .statement_cache_capacity(STATEMENT_CACHE_CAPACITY);
    if let Some(host) = &spec.host {
        opts = opts.host(host);
    }
    if let Some(port) = spec.port {
        opts = opts.port(port);
    }
    if let Some(user) = &spec.username {
        opts = opts.username(user);
    }
    if let Some(pw) = &spec.password {
        opts = opts.password(pw);
    }
    if let Some(db) = &spec.database {
        opts = opts.database(db);
    }
    let mut params: Vec<(String, String)> = spec.option_pairs();
    params.extend(spec.params.iter().map(|(k, v)| (k.clone(), v.clone())));
    for (key, value) in params {
        match key.as_str() {
            "charset" => opts = opts.charset(&value),
            "collation" => opts = opts.collation(&value),
            "ssl_mode" | "sslmode" => {
                let mode: MySqlSslMode = value.parse().map_err(|e| {
                    SqlError::InvalidConnection(format!("invalid ssl mode '{value}': {e}"))
                })?;
                opts = opts.ssl_mode(mode);
            }
            "ssl_ca" => opts = opts.ssl_ca(value),
            _ => tracing::debug!("Ignoring MySQL connection parameter '{key}'."),
        }
    }
    Ok(opts)
}

pub(crate) async fn connect(spec: &ConnectionSpec) -> Result<MySqlPool> {
    let opts = connect_options(spec)?;
    Ok(MySqlPoolOptions::new()
        .max_connections(spec.max_connections())
        .connect_with(opts)
        .await?)
}

/// Map a MySQL type name (as reported by sqlx) to an Arrow column kind.
pub(crate) fn kind_for(type_name: &str) -> ColumnKind {
    match type_name.to_ascii_uppercase().as_str() {
        "BOOLEAN" => ColumnKind::Boolean,
        "TINYINT" => ColumnKind::Int8,
        "TINYINT UNSIGNED" => ColumnKind::UInt8,
        "SMALLINT" => ColumnKind::Int16,
        "SMALLINT UNSIGNED" | "YEAR" => ColumnKind::UInt16,
        "INT" | "MEDIUMINT" => ColumnKind::Int32,
        "INT UNSIGNED" | "MEDIUMINT UNSIGNED" => ColumnKind::UInt32,
        "BIGINT" => ColumnKind::Int64,
        "BIGINT UNSIGNED" | "BIT" => ColumnKind::UInt64,
        "FLOAT" => ColumnKind::Float32,
        "DOUBLE" => ColumnKind::Float64,
        "DECIMAL" => ColumnKind::Decimal,
        "TIMESTAMP" => ColumnKind::TimestampTz,
        "DATETIME" => ColumnKind::Timestamp,
        "DATE" => ColumnKind::Date,
        "TIME" => ColumnKind::Time,
        "JSON" => ColumnKind::Json,
        "GEOMETRY" => ColumnKind::Geometry(GeometryEncoding::Mysql),
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET"
        | "NULL" => ColumnKind::Utf8,
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            ColumnKind::Bytes
        }
        _ => ColumnKind::Raw,
    }
}

fn decode_err(plan: &ColumnPlan, e: sqlx::Error) -> SqlError {
    SqlError::Conversion {
        column: plan.name.clone(),
        reason: e.to_string(),
    }
}

impl Backend for MySqlBackend {
    type DB = MySql;
    const PERSISTENT_STATEMENTS: bool = true;

    fn plan(columns: &[MySqlColumn]) -> Vec<ColumnPlan> {
        columns
            .iter()
            .map(|c| {
                let ty = c.type_info().name().to_string();
                ColumnPlan::new(c.name(), kind_for(&ty), ty)
            })
            .collect()
    }

    fn cell(row: &MySqlRow, idx: usize, plan: &ColumnPlan) -> Result<SqlValue> {
        let raw = row.try_get_raw(idx)?;
        if raw.is_null() {
            return Ok(SqlValue::Null);
        }
        let err = |e| decode_err(plan, e);
        Ok(match plan.kind {
            ColumnKind::Boolean => {
                SqlValue::Bool(row.try_get_unchecked::<bool, _>(idx).map_err(err)?)
            }
            // MySQL integers decode from whatever width the server sent.
            ColumnKind::Int8 | ColumnKind::Int16 | ColumnKind::Int32 | ColumnKind::Int64 => {
                SqlValue::Int(row.try_get_unchecked::<i64, _>(idx).map_err(err)?)
            }
            ColumnKind::UInt8 | ColumnKind::UInt16 | ColumnKind::UInt32 => {
                SqlValue::UInt(row.try_get_unchecked::<u64, _>(idx).map_err(err)?)
            }
            ColumnKind::UInt64 => {
                if plan.db_type.eq_ignore_ascii_case("BIT") {
                    // BIT(n) arrives as big-endian bytes.
                    let bytes = row.try_get_unchecked::<Vec<u8>, _>(idx).map_err(err)?;
                    SqlValue::UInt(bytes.iter().fold(0u64, |acc, b| (acc << 8) | *b as u64))
                } else {
                    SqlValue::UInt(row.try_get_unchecked::<u64, _>(idx).map_err(err)?)
                }
            }
            ColumnKind::Float32 => SqlValue::Float(row.try_get::<f32, _>(idx).map_err(err)? as f64),
            ColumnKind::Float64 => SqlValue::Float(row.try_get::<f64, _>(idx).map_err(err)?),
            // DECIMAL arrives as text; `rust_decimal` keeps 28 digits, so wider values stay
            // text and are parsed into the 38-digit `Decimal128` by the batch builder.
            ColumnKind::Decimal => {
                let bytes = row.try_get_unchecked::<Vec<u8>, _>(idx).map_err(err)?;
                let text = String::from_utf8_lossy(&bytes).into_owned();
                match Decimal::from_str_exact(text.trim()) {
                    Ok(d) => SqlValue::Decimal(d),
                    Err(_) => SqlValue::Text(text),
                }
            }
            ColumnKind::Utf8 => {
                let bytes = row.try_get_unchecked::<Vec<u8>, _>(idx).map_err(err)?;
                SqlValue::Text(String::from_utf8_lossy(&bytes).into_owned())
            }
            ColumnKind::Json => {
                let bytes = row.try_get_unchecked::<Vec<u8>, _>(idx).map_err(err)?;
                SqlValue::Json(String::from_utf8_lossy(&bytes).into_owned())
            }
            ColumnKind::TimestampTz => {
                SqlValue::TimestampTz(row.try_get::<DateTime<Utc>, _>(idx).map_err(err)?)
            }
            ColumnKind::Timestamp => {
                SqlValue::Timestamp(row.try_get::<NaiveDateTime, _>(idx).map_err(err)?)
            }
            ColumnKind::Date => SqlValue::Date(row.try_get::<NaiveDate, _>(idx).map_err(err)?),
            ColumnKind::Time => SqlValue::Time(row.try_get::<NaiveTime, _>(idx).map_err(err)?),
            ColumnKind::Bytes | ColumnKind::Geometry(_) | ColumnKind::Raw => {
                SqlValue::Bytes(row.try_get_unchecked::<Vec<u8>, _>(idx).map_err(err)?)
            }
            other => {
                return Err(SqlError::Conversion {
                    column: plan.name.clone(),
                    reason: format!("unexpected MySQL column kind {other:?}"),
                });
            }
        })
    }

    fn array(rows: &[MySqlRow], idx: usize, plan: &ColumnPlan) -> Result<arrow::array::ArrayRef> {
        use arrow::array::*;
        use std::sync::Arc;
        macro_rules! primitive {
            ($builder:ty, $ty:ty) => {{
                let mut b = <$builder>::with_capacity(rows.len());
                for row in rows {
                    b.append_option(row.try_get::<Option<$ty>, _>(idx)?);
                }
                return Ok(Arc::new(b.finish()));
            }};
        }
        match plan.kind {
            ColumnKind::Int64 => primitive!(Int64Builder, i64),
            ColumnKind::Float64 => primitive!(Float64Builder, f64),
            ColumnKind::Int16 => primitive!(Int16Builder, i16),
            ColumnKind::Int32 => primitive!(Int32Builder, i32),
            ColumnKind::Float32 => primitive!(Float32Builder, f32),
            ColumnKind::Boolean => primitive!(BooleanBuilder, bool),
            ColumnKind::Timestamp => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
                for row in rows {
                    b.append_option(
                        row.try_get::<Option<NaiveDateTime>, _>(idx)?
                            .map(|d| d.and_utc().timestamp_micros()),
                    );
                }
                return Ok(Arc::new(b.finish()));
            }
            ColumnKind::TimestampTz => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
                for row in rows {
                    b.append_option(
                        row.try_get::<Option<DateTime<Utc>>, _>(idx)?
                            .map(|d| d.timestamp_micros()),
                    );
                }
                return Ok(Arc::new(b.finish().with_timezone("UTC")));
            }
            ColumnKind::Utf8 => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    let bytes = row.try_get_unchecked::<Option<&[u8]>, _>(idx)?;
                    b.append_option(bytes.map(String::from_utf8_lossy));
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
        let cells = rows
            .iter()
            .map(|r| Self::cell(r, idx, plan))
            .collect::<Result<Vec<_>>>()?;
        super::batch::build_array(plan, &cells)
    }

    fn rows_affected(result: &sqlx::mysql::MySqlQueryResult) -> u64 {
        result.rows_affected()
    }

    fn bind<'q>(
        q: sqlx::query::Query<'q, MySql, sqlx::mysql::MySqlArguments>,
        p: &super::params::SqlParam,
    ) -> sqlx::query::Query<'q, MySql, sqlx::mysql::MySqlArguments> {
        super::params::bind_mysql(q, p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_types() {
        assert_eq!(kind_for("BIGINT UNSIGNED"), ColumnKind::UInt64);
        assert_eq!(kind_for("TIMESTAMP"), ColumnKind::TimestampTz);
        assert_eq!(kind_for("LONGBLOB"), ColumnKind::Bytes);
        assert_eq!(
            kind_for("GEOMETRY"),
            ColumnKind::Geometry(GeometryEncoding::Mysql)
        );
    }
}
