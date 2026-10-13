//! DuckDB (`duckdb`) over the `duckdb` crate (bundled DuckDB, cargo feature `duckdb`).
//!
//! DuckDB is embedded and synchronous: every call runs on a blocking thread. One database
//! instance is opened per connector (opening the same file twice in one process conflicts on
//! the file lock), and the pool's connections are clones of it. Bound parameters are `$1`,
//! `$2`, ...

use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta};
use duckdb::Connection;
use duckdb::core::LogicalTypeId;
use duckdb::types::{TimeUnit as DUnit, Value as DValue, ValueRef};
use tokio::sync::mpsc;

use super::SqlValue;
use super::batch::{ColumnKind, ColumnPlan};
use super::native::{NativeConn, NativeConnector, NativeEvent, NativePool, blocking_send_rows};
use super::params::SqlParam;
use crate::error::{Result, SqlError};
use crate::uri::ConnectionSpec;

fn db_err(e: duckdb::Error) -> SqlError {
    SqlError::Database(e.to_string())
}

fn task_err(e: tokio::task::JoinError) -> SqlError {
    SqlError::InvalidArgument(format!("The database task failed: {e}"))
}

/// The database file (`None` for an in-memory database).
fn database_path(spec: &ConnectionSpec) -> Option<String> {
    spec.database
        .as_deref()
        .filter(|d| !d.is_empty() && *d != ":memory:")
        .map(str::to_string)
}

struct DuckdbConnector {
    path: Option<String>,
    /// The database instance, opened on first use; connections are clones of it.
    primary: Mutex<Option<Connection>>,
}

impl DuckdbConnector {
    fn open(&self) -> Result<Connection> {
        let mut primary = self
            .primary
            .lock()
            .map_err(|_| SqlError::Database("The DuckDB connection lock is poisoned.".into()))?;
        if primary.is_none() {
            let conn = match &self.path {
                Some(p) => Connection::open(p),
                None => Connection::open_in_memory(),
            }
            .map_err(db_err)?;
            *primary = Some(conn);
        }
        primary
            .as_ref()
            .expect("opened")
            .try_clone()
            .map_err(db_err)
    }
}

#[async_trait]
impl NativeConnector for DuckdbConnector {
    async fn connect(&self) -> Result<Box<dyn NativeConn>> {
        let conn = self.open()?;
        Ok(Box::new(DuckdbConn {
            conn: Arc::new(Mutex::new(conn)),
        }))
    }
}

/// Open the database, verifying it can be opened.
pub(crate) async fn connect(spec: &ConnectionSpec) -> Result<NativePool> {
    let pool = connect_lazy(spec)?;
    drop(pool.acquire().await?);
    Ok(pool)
}

/// Build the pool without opening the database.
pub(crate) fn connect_lazy(spec: &ConnectionSpec) -> Result<NativePool> {
    Ok(NativePool::new(
        Box::new(DuckdbConnector {
            path: database_path(spec),
            primary: Mutex::new(None),
        }),
        spec.max_connections(),
    ))
}

struct DuckdbConn {
    conn: Arc<Mutex<Connection>>,
}

fn lock(conn: &Mutex<Connection>) -> Result<std::sync::MutexGuard<'_, Connection>> {
    conn.lock()
        .map_err(|_| SqlError::Database("The DuckDB connection lock is poisoned.".into()))
}

/// A bound parameter as a DuckDB value.
fn to_value(p: &SqlParam) -> DValue {
    match p {
        SqlParam::Null => DValue::Null,
        SqlParam::Bool(b) => DValue::Boolean(*b),
        SqlParam::Int(i) => DValue::BigInt(*i),
        SqlParam::Float(f) => DValue::Double(*f),
        SqlParam::Text(s) => DValue::Text(s.clone()),
        SqlParam::Bytes(b) => DValue::Blob(b.clone()),
        SqlParam::Json(j) => DValue::Text(j.to_string()),
        SqlParam::Timestamp(t) => {
            DValue::Timestamp(DUnit::Microsecond, t.and_utc().timestamp_micros())
        }
        SqlParam::TimestampTz(t) => DValue::Timestamp(DUnit::Microsecond, t.timestamp_micros()),
        SqlParam::Date(d) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            DValue::Date32((*d - epoch).num_days() as i32)
        }
    }
}

fn execute_blocking(conn: &Connection, sql: &str, params: &[SqlParam]) -> Result<u64> {
    if params.is_empty() {
        return match conn.prepare(sql) {
            Ok(mut stmt) => stmt.execute([]).map(|n| n as u64).map_err(db_err),
            // Several statements cannot be prepared at once; run them as a batch.
            Err(_) => conn.execute_batch(sql).map(|_| 0).map_err(db_err),
        };
    }
    let values: Vec<DValue> = params.iter().map(to_value).collect();
    conn.execute(sql, duckdb::params_from_iter(values))
        .map(|n| n as u64)
        .map_err(db_err)
}

fn micros(unit: DUnit, v: i64) -> i64 {
    unit.to_micros(v)
}

/// The kind and database type name of a result column.
pub(crate) fn kind_for(dt: &DataType, logical: Option<LogicalTypeId>) -> (ColumnKind, String) {
    if logical == Some(LogicalTypeId::Uuid) {
        return (ColumnKind::Uuid, "UUID".into());
    }
    match dt {
        DataType::Boolean => (ColumnKind::Boolean, "BOOLEAN".into()),
        DataType::Int8 => (ColumnKind::Int8, "TINYINT".into()),
        DataType::Int16 => (ColumnKind::Int16, "SMALLINT".into()),
        DataType::Int32 => (ColumnKind::Int32, "INTEGER".into()),
        DataType::Int64 => (ColumnKind::Int64, "BIGINT".into()),
        DataType::UInt8 => (ColumnKind::UInt8, "UTINYINT".into()),
        DataType::UInt16 => (ColumnKind::UInt16, "USMALLINT".into()),
        DataType::UInt32 => (ColumnKind::UInt32, "UINTEGER".into()),
        DataType::UInt64 => (ColumnKind::UInt64, "UBIGINT".into()),
        DataType::Float16 | DataType::Float32 => (ColumnKind::Float32, "FLOAT".into()),
        DataType::Float64 => (ColumnKind::Float64, "DOUBLE".into()),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            (ColumnKind::Decimal, format!("DECIMAL({p},{s})"))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            (ColumnKind::Utf8, "VARCHAR".into())
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            (ColumnKind::Bytes, "BLOB".into())
        }
        DataType::FixedSizeBinary(16) => (ColumnKind::Uuid, "UUID".into()),
        DataType::Date32 | DataType::Date64 => (ColumnKind::Date, "DATE".into()),
        DataType::Time32(_) | DataType::Time64(_) => (ColumnKind::Time, "TIME".into()),
        DataType::Timestamp(_, None) => (ColumnKind::Timestamp, "TIMESTAMP".into()),
        DataType::Timestamp(_, Some(_)) => {
            (ColumnKind::TimestampTz, "TIMESTAMP WITH TIME ZONE".into())
        }
        DataType::Null => (ColumnKind::Utf8, "NULL".into()),
        other => (ColumnKind::Utf8, format!("{other}")),
    }
}

fn epoch_date(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(TimeDelta::days(days as i64))
}

/// Decode one DuckDB cell.
fn cell(v: ValueRef<'_>, plan: &ColumnPlan) -> Result<SqlValue> {
    let conv = |reason: String| SqlError::Conversion {
        column: plan.name.clone(),
        reason,
    };
    Ok(match v {
        ValueRef::Null => SqlValue::Null,
        ValueRef::Boolean(b) => SqlValue::Bool(b),
        ValueRef::TinyInt(i) => SqlValue::Int(i as i64),
        ValueRef::SmallInt(i) => SqlValue::Int(i as i64),
        ValueRef::Int(i) => SqlValue::Int(i as i64),
        ValueRef::BigInt(i) => SqlValue::Int(i),
        ValueRef::HugeInt(i) => match i64::try_from(i) {
            Ok(i) => SqlValue::Int(i),
            Err(_) => SqlValue::Text(i.to_string()),
        },
        ValueRef::UTinyInt(i) => SqlValue::Int(i as i64),
        ValueRef::USmallInt(i) => SqlValue::Int(i as i64),
        ValueRef::UInt(i) => SqlValue::Int(i as i64),
        ValueRef::UBigInt(i) => SqlValue::UInt(i),
        ValueRef::Float(f) => SqlValue::Float(f as f64),
        ValueRef::Double(f) => SqlValue::Float(f),
        ValueRef::Decimal(d) => SqlValue::Decimal(d),
        ValueRef::Timestamp(unit, v) => {
            let dt = DateTime::from_timestamp_micros(micros(unit, v))
                .ok_or_else(|| conv(format!("timestamp out of range: {v}")))?;
            if plan.kind == ColumnKind::TimestampTz {
                SqlValue::TimestampTz(dt)
            } else {
                SqlValue::Timestamp(dt.naive_utc())
            }
        }
        ValueRef::Text(b) => {
            let s = String::from_utf8_lossy(b).into_owned();
            if plan.kind == ColumnKind::Uuid {
                match uuid::Uuid::parse_str(&s) {
                    Ok(u) => SqlValue::Uuid(u),
                    Err(_) => SqlValue::Text(s),
                }
            } else {
                SqlValue::Text(s)
            }
        }
        ValueRef::Blob(b) => {
            if plan.kind == ColumnKind::Uuid && b.len() == 16 {
                SqlValue::Uuid(uuid::Uuid::from_slice(b).map_err(|e| conv(e.to_string()))?)
            } else {
                SqlValue::Bytes(b.to_vec())
            }
        }
        ValueRef::Date32(d) => {
            SqlValue::Date(epoch_date(d).ok_or_else(|| conv(format!("date out of range: {d}")))?)
        }
        ValueRef::Time64(unit, v) => {
            let us = micros(unit, v);
            let t = NaiveTime::from_num_seconds_from_midnight_opt(
                (us / 1_000_000) as u32,
                ((us % 1_000_000) * 1000) as u32,
            )
            .ok_or_else(|| conv(format!("time out of range: {v}")))?;
            SqlValue::Time(t)
        }
        other => SqlValue::Text(format!("{:?}", other.to_owned())),
    })
}

fn query_blocking(
    conn: &Connection,
    sql: &str,
    params: &[SqlParam],
    chunksize: usize,
    sink: &mpsc::Sender<NativeEvent>,
) -> Result<()> {
    let mut stmt = conn.prepare(sql).map_err(db_err)?;
    let values: Vec<DValue> = params.iter().map(to_value).collect();
    let mut rows = stmt
        .query(duckdb::params_from_iter(values))
        .map_err(db_err)?;
    let plans: Vec<ColumnPlan> = {
        let Some(stmt) = rows.as_ref() else {
            let _ = sink.blocking_send(NativeEvent::Columns(Vec::new()));
            return Ok(());
        };
        (0..stmt.column_count())
            .map(|i| {
                let name = stmt
                    .column_name(i)
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                let logical = Some(stmt.column_logical_type(i).id());
                let (kind, db_type) = kind_for(&stmt.column_type(i), logical);
                let mut plan = ColumnPlan::new(name, kind, db_type);
                if let DataType::Decimal128(_, s) = stmt.column_type(i) {
                    plan.scale = s.max(0) as u32;
                }
                plan
            })
            .collect()
    };
    if sink
        .blocking_send(NativeEvent::Columns(plans.clone()))
        .is_err()
    {
        return Ok(());
    }
    let mut buf: Vec<Vec<SqlValue>> = Vec::new();
    while let Some(row) = rows.next().map_err(db_err)? {
        let mut cells = Vec::with_capacity(plans.len());
        for (i, plan) in plans.iter().enumerate() {
            cells.push(cell(row.get_ref(i).map_err(db_err)?, plan)?);
        }
        buf.push(cells);
        if !blocking_send_rows(sink, &mut buf, chunksize, false) {
            return Ok(());
        }
    }
    blocking_send_rows(sink, &mut buf, chunksize, true);
    Ok(())
}

#[async_trait]
impl NativeConn for DuckdbConn {
    async fn execute(&mut self, sql: &str, params: &[SqlParam]) -> Result<u64> {
        let (conn, sql, params) = (self.conn.clone(), sql.to_string(), params.to_vec());
        tokio::task::spawn_blocking(move || execute_blocking(&*lock(&conn)?, &sql, &params))
            .await
            .map_err(task_err)?
    }

    async fn query(
        &mut self,
        sql: &str,
        params: &[SqlParam],
        chunksize: usize,
        sink: mpsc::Sender<NativeEvent>,
    ) -> Result<()> {
        let (conn, sql, params) = (self.conn.clone(), sql.to_string(), params.to_vec());
        tokio::task::spawn_blocking(move || {
            query_blocking(&*lock(&conn)?, &sql, &params, chunksize.max(1), &sink)
        })
        .await
        .map_err(task_err)?
    }

    async fn begin(&mut self) -> Result<()> {
        self.execute("BEGIN TRANSACTION", &[]).await.map(|_| ())
    }

    async fn commit(&mut self) -> Result<()> {
        self.execute("COMMIT", &[]).await.map(|_| ())
    }

    async fn rollback(&mut self) -> Result<()> {
        self.execute("ROLLBACK", &[]).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;

    #[test]
    fn maps_types() {
        assert_eq!(kind_for(&DataType::Int64, None).0, ColumnKind::Int64);
        assert_eq!(
            kind_for(
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                None
            )
            .0,
            ColumnKind::TimestampTz
        );
        assert_eq!(
            kind_for(&DataType::Utf8, Some(LogicalTypeId::Uuid)).0,
            ColumnKind::Uuid
        );
        assert_eq!(
            kind_for(&DataType::Decimal128(18, 3), None).1,
            "DECIMAL(18,3)"
        );
    }

    #[test]
    fn binds_dates() {
        let d = NaiveDate::from_ymd_opt(1970, 1, 3).unwrap();
        assert_eq!(to_value(&SqlParam::Date(d)), DValue::Date32(2));
    }
}
