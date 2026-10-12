//! PostgreSQL family (`postgresql`, `timescaledb`, `timescaledb-ha`, `postgis`, `citus`,
//! `cockroachdb`) over `sqlx::postgres`.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use sqlx::postgres::{PgColumn, PgConnectOptions, PgPool, PgPoolOptions, PgRow, PgSslMode};
use sqlx::{Column, Postgres, Row, TypeInfo, ValueRef};

use super::SqlValue;
use super::backend::Backend;
use super::batch::{ColumnKind, ColumnPlan, GeometryEncoding};
use crate::error::{Result, SqlError};
use crate::uri::ConnectionSpec;

pub(crate) struct PgBackend;

/// Parse a libpq `options` string (`-c key=value`, `--key=value`) into runtime parameters.
fn libpq_options(options: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut tokens = options.split_whitespace().peekable();
    while let Some(tok) = tokens.next() {
        let kv = if tok == "-c" {
            tokens.next().map(str::to_string)
        } else if let Some(rest) = tok.strip_prefix("-c") {
            Some(rest.to_string())
        } else {
            tok.strip_prefix("--").map(str::to_string)
        };
        if let Some((k, v)) = kv.as_deref().and_then(|kv| kv.split_once('=')) {
            out.push((k.replace('-', "_"), v.to_string()));
        }
    }
    out
}

pub(crate) fn connect_options(spec: &ConnectionSpec) -> Result<PgConnectOptions> {
    let mut opts = PgConnectOptions::new_without_pgpass();
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
            "sslmode" => {
                let mode: PgSslMode = value.parse().map_err(|e| {
                    SqlError::InvalidConnection(format!("invalid sslmode '{value}': {e}"))
                })?;
                opts = opts.ssl_mode(mode);
            }
            "sslrootcert" => opts = opts.ssl_root_cert(value),
            "application_name" => opts = opts.application_name(&value),
            "options" => opts = opts.options(libpq_options(&value)),
            _ => tracing::debug!("Ignoring PostgreSQL connection parameter '{key}'."),
        }
    }
    Ok(opts)
}

pub(crate) async fn connect(spec: &ConnectionSpec) -> Result<PgPool> {
    let opts = connect_options(spec)?;
    Ok(PgPoolOptions::new()
        .max_connections(spec.max_connections())
        .connect_with(opts)
        .await?)
}

/// Map a PostgreSQL type name to an Arrow column kind.
pub(crate) fn kind_for(type_name: &str) -> ColumnKind {
    match type_name.to_ascii_uppercase().as_str() {
        "BOOL" => ColumnKind::Boolean,
        "INT2" => ColumnKind::Int16,
        "INT4" => ColumnKind::Int32,
        "INT8" => ColumnKind::Int64,
        "OID" => ColumnKind::UInt32,
        "FLOAT4" => ColumnKind::Float32,
        "FLOAT8" => ColumnKind::Float64,
        "NUMERIC" => ColumnKind::Decimal,
        "TEXT" | "VARCHAR" | "BPCHAR" | "NAME" | "CITEXT" | "UNKNOWN" => ColumnKind::Utf8,
        "JSON" | "JSONB" => ColumnKind::Json,
        "BYTEA" => ColumnKind::Bytes,
        "UUID" => ColumnKind::Uuid,
        "TIMESTAMPTZ" => ColumnKind::TimestampTz,
        "TIMESTAMP" => ColumnKind::Timestamp,
        "DATE" => ColumnKind::Date,
        "TIME" => ColumnKind::Time,
        "GEOMETRY" | "GEOGRAPHY" => ColumnKind::Geometry(GeometryEncoding::Ewkb),
        _ => ColumnKind::Raw,
    }
}

/// Decode PostgreSQL's binary `NUMERIC` (`ndigits`, `weight`, `sign`, `dscale`, base-10000
/// digits) into decimal text. `None` for `NaN` / infinities (Python reads `Decimal('NaN')`, a
/// null to pandas).
pub(crate) fn pg_numeric_text(bytes: &[u8]) -> Option<String> {
    let rd = |i: usize| -> Option<i16> {
        Some(i16::from_be_bytes([*bytes.get(i)?, *bytes.get(i + 1)?]))
    };
    let ndigits = rd(0)? as usize;
    let weight = rd(2)? as i32;
    let sign = rd(4)? as u16;
    let dscale = rd(6)?.max(0) as usize;
    if sign != 0x0000 && sign != 0x4000 {
        return None;
    }
    let digits: Vec<u16> = (0..ndigits)
        .map(|k| rd(8 + 2 * k).map(|d| d as u16))
        .collect::<Option<_>>()?;
    let mut int_part = String::new();
    for k in 0..=weight.max(-1) {
        let d = digits.get(k as usize).copied().unwrap_or(0);
        if int_part.is_empty() {
            if d != 0 {
                int_part.push_str(&d.to_string());
            }
        } else {
            int_part.push_str(&format!("{d:04}"));
        }
    }
    if int_part.is_empty() {
        int_part.push('0');
    }
    let mut frac = String::new();
    let mut k = weight + 1;
    while frac.len() < dscale {
        let d = if k < 0 {
            0
        } else {
            digits.get(k as usize).copied().unwrap_or(0)
        };
        frac.push_str(&format!("{d:04}"));
        k += 1;
    }
    frac.truncate(dscale);
    let mut out = String::new();
    if sign == 0x4000 {
        out.push('-');
    }
    out.push_str(&int_part);
    if dscale > 0 {
        out.push('.');
        out.push_str(&frac);
    }
    Some(out)
}

fn decode_err(plan: &ColumnPlan, e: sqlx::Error) -> SqlError {
    SqlError::Conversion {
        column: plan.name.clone(),
        reason: e.to_string(),
    }
}

impl Backend for PgBackend {
    type DB = Postgres;

    fn plan(columns: &[PgColumn]) -> Vec<ColumnPlan> {
        columns
            .iter()
            .map(|c| {
                let ty = c.type_info().name().to_string();
                ColumnPlan::new(c.name(), kind_for(&ty), ty)
            })
            .collect()
    }

    fn cell(row: &PgRow, idx: usize, plan: &ColumnPlan) -> Result<SqlValue> {
        let raw = row.try_get_raw(idx)?;
        if raw.is_null() {
            return Ok(SqlValue::Null);
        }
        let err = |e| decode_err(plan, e);
        Ok(match plan.kind {
            ColumnKind::Boolean => SqlValue::Bool(row.try_get::<bool, _>(idx).map_err(err)?),
            ColumnKind::Int16 => SqlValue::Int(row.try_get::<i16, _>(idx).map_err(err)? as i64),
            ColumnKind::Int32 => SqlValue::Int(row.try_get::<i32, _>(idx).map_err(err)? as i64),
            ColumnKind::Int64 => SqlValue::Int(row.try_get::<i64, _>(idx).map_err(err)?),
            ColumnKind::UInt32 => SqlValue::UInt(
                row.try_get::<sqlx::postgres::types::Oid, _>(idx)
                    .map_err(err)?
                    .0 as u64,
            ),
            ColumnKind::Float32 => SqlValue::Float(row.try_get::<f32, _>(idx).map_err(err)? as f64),
            ColumnKind::Float64 => SqlValue::Float(row.try_get::<f64, _>(idx).map_err(err)?),
            // `rust_decimal` keeps 28 digits; wider values are decoded from the binary format
            // as text and parsed into the 38-digit `Decimal128` by the batch builder.
            ColumnKind::Decimal => match row.try_get::<Decimal, _>(idx) {
                Ok(d) if d.mantissa().unsigned_abs() < 10u128.pow(27) => SqlValue::Decimal(d),
                _ => {
                    let raw = row.try_get_raw(idx)?;
                    let bytes = raw.as_bytes().map_err(|e| SqlError::Conversion {
                        column: plan.name.clone(),
                        reason: e.to_string(),
                    })?;
                    match pg_numeric_text(bytes) {
                        Some(text) => SqlValue::Text(text),
                        None => SqlValue::Null,
                    }
                }
            },
            ColumnKind::Utf8 => {
                SqlValue::Text(row.try_get_unchecked::<String, _>(idx).map_err(err)?)
            }
            ColumnKind::Json => {
                let bytes = row.try_get_unchecked::<Vec<u8>, _>(idx).map_err(err)?;
                // JSONB's binary format is a version byte (1) followed by the JSON text.
                let text =
                    if plan.db_type.eq_ignore_ascii_case("JSONB") && bytes.first() == Some(&1) {
                        &bytes[1..]
                    } else {
                        &bytes[..]
                    };
                SqlValue::Json(String::from_utf8_lossy(text).into_owned())
            }
            ColumnKind::Uuid => SqlValue::Uuid(row.try_get::<uuid::Uuid, _>(idx).map_err(err)?),
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
                    reason: format!("unexpected PostgreSQL column kind {other:?}"),
                });
            }
        })
    }

    fn rows_affected(result: &sqlx::postgres::PgQueryResult) -> u64 {
        result.rows_affected()
    }

    fn bind<'q>(
        q: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
        p: &super::params::SqlParam,
    ) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
        super::params::bind_pg(q, p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_libpq_options() {
        assert_eq!(
            libpq_options("--search_path=foo -c statement_timeout=5000 -cwork_mem=4MB"),
            vec![
                ("search_path".to_string(), "foo".to_string()),
                ("statement_timeout".to_string(), "5000".to_string()),
                ("work_mem".to_string(), "4MB".to_string()),
            ]
        );
    }

    #[test]
    fn numeric_binary() {
        // 12345.678 -> ndigits 3, weight 1, sign 0, dscale 3, digits [1, 2345, 6780]
        let mut b = Vec::new();
        for v in [3i16, 1, 0, 3, 1, 2345, 6780] {
            b.extend_from_slice(&v.to_be_bytes());
        }
        assert_eq!(pg_numeric_text(&b).as_deref(), Some("12345.678"));
        // -0.05 -> ndigits 1, weight -1, sign 0x4000, dscale 2, digits [500]
        let mut b = Vec::new();
        for v in [1i16, -1, 0x4000, 2, 500] {
            b.extend_from_slice(&v.to_be_bytes());
        }
        assert_eq!(pg_numeric_text(&b).as_deref(), Some("-0.05"));
    }

    #[test]
    fn maps_types() {
        assert_eq!(kind_for("TIMESTAMPTZ"), ColumnKind::TimestampTz);
        assert_eq!(
            kind_for("geometry"),
            ColumnKind::Geometry(GeometryEncoding::Ewkb)
        );
        assert_eq!(kind_for("INTERVAL"), ColumnKind::Raw);
    }
}
