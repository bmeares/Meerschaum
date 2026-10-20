//! Oracle (`oracle`) over the `oracle` crate (ODPI-C; cargo feature `oracle`).
//!
//! As v4's thick-mode driver, this loads Oracle Instant Client (`libclntsh`) at runtime; the
//! build itself needs no Oracle libraries. Calls are synchronous and run on blocking threads.
//! Bound parameters are `:1`, `:2`, ... Oracle runs DDL with an implicit commit; outside an
//! explicit transaction every statement is committed (SQLAlchemy's autocommit-per-`exec`).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use oracle::SqlValue as OraValue;
use oracle::sql_type::{OracleType, ToSql};
use oracle::{Connection, Connector};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use super::SqlValue;
use super::batch::{ColumnKind, ColumnPlan};
use super::native::{NativeConn, NativeConnector, NativeEvent, NativePool, blocking_send_rows};
use super::params::SqlParam;
use crate::error::{Result, SqlError};
use crate::uri::ConnectionSpec;

fn db_err(e: oracle::Error) -> SqlError {
    SqlError::Database(e.to_string())
}

fn task_err(e: tokio::task::JoinError) -> SqlError {
    SqlError::InvalidArgument(format!("The database task failed: {e}"))
}

/// The EZConnect string (`host:port/service_name`) for a connection spec.
pub(crate) fn connect_string(spec: &ConnectionSpec) -> String {
    let host = spec.host.as_deref().unwrap_or("localhost");
    let port = spec.port.unwrap_or(1521);
    let service = spec
        .params
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("service_name"))
        .map(|(_, v)| v.clone())
        .or_else(|| spec.database.clone().filter(|d| !d.is_empty()))
        .unwrap_or_default();
    // EZConnect Plus timeouts: the TCP connect and the whole connect are bounded by
    // `connect_timeout`, so an abandoned attempt (the blocking thread outlives a cancelled
    // future) ends with the deadline.
    let limit = super::connect::connect_timeout(spec)
        .as_secs_f64()
        .ceil()
        .max(1.0) as u64;
    let timeouts = format!("?transport_connect_timeout={limit}&connect_timeout={limit}");
    if service.is_empty() {
        format!("//{host}:{port}{timeouts}")
    } else {
        format!("//{host}:{port}/{service}{timeouts}")
    }
}

struct OracleConnector {
    username: String,
    password: String,
    connect_string: String,
}

#[async_trait]
impl NativeConnector for OracleConnector {
    async fn connect(&self) -> Result<Box<dyn NativeConn>> {
        let (u, p, c) = (
            self.username.clone(),
            self.password.clone(),
            self.connect_string.clone(),
        );
        let conn = tokio::task::spawn_blocking(move || Connector::new(u, p, c).connect())
            .await
            .map_err(task_err)?
            .map_err(db_err)?;
        Ok(Box::new(OracleConn {
            conn: Arc::new(Mutex::new(conn)),
            in_tx: false,
        }))
    }
}

/// Open a pool, verifying that one connection can be established.
pub(crate) async fn connect(spec: &ConnectionSpec) -> Result<NativePool> {
    let pool = connect_lazy(spec)?;
    drop(pool.acquire().await?);
    Ok(pool)
}

/// Build the pool without connecting.
pub(crate) fn connect_lazy(spec: &ConnectionSpec) -> Result<NativePool> {
    Ok(NativePool::new(
        Box::new(OracleConnector {
            username: spec.username.clone().unwrap_or_default(),
            password: spec.password.clone().unwrap_or_default(),
            connect_string: connect_string(spec),
        }),
        spec.max_connections(),
    ))
}

struct OracleConn {
    conn: Arc<Mutex<Connection>>,
    in_tx: bool,
}

fn lock(conn: &Mutex<Connection>) -> Result<std::sync::MutexGuard<'_, Connection>> {
    conn.lock()
        .map_err(|_| SqlError::Database("The Oracle connection lock is poisoned.".into()))
}

/// Text binds as `VARCHAR2`, as python-oracledb (v4) binds a `str`. The `oracle` crate binds a `String`
/// as `NVARCHAR2`, and Oracle refuses to mix that with the `VARCHAR2` columns of the instance tables in
/// `coalesce(col, :1)` (`ORA-12704: character set mismatch`, e.g. `fetch_pipes_keys`); a `VARCHAR2` bind
/// converts implicitly to the `NVARCHAR2` columns user data lives in.
struct Varchar2Text(String);

impl ToSql for Varchar2Text {
    fn oratype(&self, _conn: &Connection) -> oracle::Result<OracleType> {
        Ok(OracleType::Varchar2(self.0.len() as u32))
    }

    fn to_sql(&self, val: &mut OraValue) -> oracle::Result<()> {
        self.0.to_sql(val)
    }
}

/// A bound parameter. Booleans bind as `0` / `1` (`NUMBER(1)`, as v4 stores them; `BOOLEAN`
/// columns exist only on 23ai) and `NULL` as a typeless `VARCHAR2`.
pub(crate) fn to_sql_param(p: &SqlParam) -> Box<dyn ToSql + Send> {
    match p {
        SqlParam::Null => Box::new(OracleType::Varchar2(1)),
        SqlParam::Bool(b) => Box::new(*b as i64),
        SqlParam::Int(i) => Box::new(*i),
        SqlParam::Float(f) => Box::new(*f),
        SqlParam::Text(s) => Box::new(Varchar2Text(s.clone())),
        SqlParam::Bytes(b) => Box::new(b.clone()),
        SqlParam::Json(j) => Box::new(Varchar2Text(j.to_string())),
        SqlParam::Timestamp(t) => Box::new(*t),
        SqlParam::TimestampTz(t) => Box::new(*t),
        SqlParam::Date(d) => Box::new(*d),
    }
}

/// The kind and database type name of a result column.
pub(crate) fn kind_for(ty: &OracleType) -> (ColumnKind, String, u32) {
    match ty {
        // `NUMBER(p, 0)`: integers (`INTEGER` is `NUMBER(38, 0)`).
        OracleType::Number(p, 0) if *p > 0 => (ColumnKind::Int64, format!("NUMBER({p},0)"), 0),
        OracleType::Number(p, s) if *p > 0 && *s > 0 => {
            (ColumnKind::Decimal, format!("NUMBER({p},{s})"), *s as u32)
        }
        // Unconstrained `NUMBER` (precision 0: `COUNT(*)`, expressions, a column declared without
        // precision): python-oracledb returns an `int` or a `float` per value; resolved per chunk.
        OracleType::Number(_, _) => (ColumnKind::NumberAuto, "NUMBER".into(), 10),
        OracleType::Float(_) | OracleType::BinaryDouble => {
            (ColumnKind::Float64, "BINARY_DOUBLE".into(), 0)
        }
        OracleType::BinaryFloat => (ColumnKind::Float32, "BINARY_FLOAT".into(), 0),
        OracleType::Varchar2(n) => (ColumnKind::Utf8, format!("VARCHAR2({n})"), 0),
        OracleType::NVarchar2(n) => (ColumnKind::Utf8, format!("NVARCHAR2({n})"), 0),
        OracleType::Char(n) => (ColumnKind::Utf8, format!("CHAR({n})"), 0),
        OracleType::NChar(n) => (ColumnKind::Utf8, format!("NCHAR({n})"), 0),
        OracleType::CLOB => (ColumnKind::Utf8, "CLOB".into(), 0),
        OracleType::NCLOB => (ColumnKind::Utf8, "NCLOB".into(), 0),
        OracleType::Long => (ColumnKind::Utf8, "LONG".into(), 0),
        OracleType::Rowid => (ColumnKind::Utf8, "ROWID".into(), 0),
        OracleType::Raw(n) => (ColumnKind::Bytes, format!("RAW({n})"), 0),
        OracleType::BLOB => (ColumnKind::Bytes, "BLOB".into(), 0),
        OracleType::LongRaw => (ColumnKind::Bytes, "LONG RAW".into(), 0),
        OracleType::Date => (ColumnKind::Timestamp, "DATE".into(), 0),
        OracleType::Timestamp(_) => (ColumnKind::Timestamp, "TIMESTAMP".into(), 0),
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => (
            ColumnKind::TimestampTz,
            "TIMESTAMP WITH TIME ZONE".into(),
            0,
        ),
        OracleType::Boolean => (ColumnKind::Boolean, "BOOLEAN".into(), 0),
        OracleType::Json => (ColumnKind::Json, "JSON".into(), 0),
        other => (ColumnKind::Utf8, other.to_string(), 0),
    }
}

/// Decode one cell according to its column plan.
fn cell(v: &oracle::SqlValue, plan: &ColumnPlan) -> Result<SqlValue> {
    let conv = |e: oracle::Error| SqlError::Conversion {
        column: plan.name.clone(),
        reason: e.to_string(),
    };
    if v.is_null().map_err(conv)? {
        return Ok(SqlValue::Null);
    }
    Ok(match plan.kind {
        ColumnKind::Int64 => match v.get::<i64>() {
            Ok(i) => SqlValue::Int(i),
            Err(_) => SqlValue::Text(v.get::<String>().map_err(conv)?),
        },
        ColumnKind::Decimal | ColumnKind::NumberAuto => {
            let s = v.get::<String>().map_err(conv)?;
            match s
                .parse::<Decimal>()
                .or_else(|_| Decimal::from_scientific(&s))
            {
                Ok(d) => SqlValue::Decimal(d),
                Err(_) => SqlValue::Text(s),
            }
        }
        ColumnKind::Float64 | ColumnKind::Float32 => SqlValue::Float(v.get::<f64>().map_err(conv)?),
        ColumnKind::Bytes => SqlValue::Bytes(v.get::<Vec<u8>>().map_err(conv)?),
        ColumnKind::Timestamp => SqlValue::Timestamp(v.get::<NaiveDateTime>().map_err(conv)?),
        ColumnKind::TimestampTz => SqlValue::TimestampTz(v.get::<DateTime<Utc>>().map_err(conv)?),
        ColumnKind::Date => SqlValue::Date(v.get::<NaiveDate>().map_err(conv)?),
        ColumnKind::Boolean => SqlValue::Bool(v.get::<bool>().map_err(conv)?),
        _ => SqlValue::Text(v.get::<String>().map_err(conv)?),
    })
}

fn execute_blocking(
    conn: &Connection,
    sql: &str,
    params: &[SqlParam],
    commit: bool,
) -> Result<u64> {
    let boxed: Vec<Box<dyn ToSql + Send>> = params.iter().map(to_sql_param).collect();
    let refs: Vec<&dyn ToSql> = boxed.iter().map(|b| b.as_ref() as &dyn ToSql).collect();
    let stmt = conn.execute(sql, &refs).map_err(db_err)?;
    let n = stmt.row_count().map_err(db_err)?;
    if commit {
        conn.commit().map_err(db_err)?;
    }
    Ok(n)
}

fn query_blocking(
    conn: &Connection,
    sql: &str,
    params: &[SqlParam],
    chunksize: usize,
    commit: bool,
    sink: &mpsc::Sender<NativeEvent>,
) -> Result<()> {
    let boxed: Vec<Box<dyn ToSql + Send>> = params.iter().map(to_sql_param).collect();
    let refs: Vec<&dyn ToSql> = boxed.iter().map(|b| b.as_ref() as &dyn ToSql).collect();
    let mut stmt = conn
        .statement(sql)
        .fetch_array_size(chunksize.clamp(1, 10_000) as u32)
        .build()
        .map_err(db_err)?;
    // PARITY: python-oracledb's `cursor.execute(text)` runs any statement, so `read()` of a
    // `CREATE VIEW ...` (`sql <label> "CREATE VIEW ... AS SELECT ..."`, which v4 routes to `read`
    // because the text contains `select`) executes it; ODPI-C's query call refuses a non-query
    // statement ("could not use the `query` method for non-select statements"). It returns no rows.
    if !stmt.is_query() {
        stmt.execute(&refs).map_err(db_err)?;
        if commit {
            conn.commit().map_err(db_err)?;
        }
        let _ = sink.blocking_send(NativeEvent::Columns(Vec::new()));
        return Ok(());
    }
    let rows = stmt.query(&refs).map_err(db_err)?;
    let plans: Vec<ColumnPlan> = rows
        .column_info()
        .iter()
        .map(|c| {
            let (kind, db_type, scale) = kind_for(c.oracle_type());
            // PARITY: SQLAlchemy normalizes Oracle's unquoted uppercase result labels.
            // Quoted mixed-case labels retain their spelling.
            let name = c.name();
            let lower = name.to_lowercase();
            let name = if name == name.to_uppercase()
                && crate::ident::sql_item_name(&lower, crate::flavor::Flavor::Oracle, None) == lower
            {
                lower
            } else {
                name.to_string()
            };
            let mut plan = ColumnPlan::new(name, kind, db_type);
            if kind == ColumnKind::Decimal {
                plan.scale = scale;
            }
            plan
        })
        .collect();
    if sink
        .blocking_send(NativeEvent::Columns(plans.clone()))
        .is_err()
    {
        return Ok(());
    }
    let mut buf: Vec<Vec<SqlValue>> = Vec::new();
    for row in rows {
        let row = row.map_err(db_err)?;
        let cells = row
            .sql_values()
            .iter()
            .zip(&plans)
            .map(|(v, p)| cell(v, p))
            .collect::<Result<Vec<_>>>()?;
        buf.push(cells);
        if !blocking_send_rows(sink, &mut buf, chunksize, false) {
            return Ok(());
        }
    }
    blocking_send_rows(sink, &mut buf, chunksize, true);
    Ok(())
}

#[async_trait]
impl NativeConn for OracleConn {
    async fn execute(&mut self, sql: &str, params: &[SqlParam]) -> Result<u64> {
        let (conn, sql, params) = (self.conn.clone(), sql.to_string(), params.to_vec());
        let commit = !self.in_tx;
        tokio::task::spawn_blocking(move || execute_blocking(&*lock(&conn)?, &sql, &params, commit))
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
        let commit = !self.in_tx;
        tokio::task::spawn_blocking(move || {
            query_blocking(
                &*lock(&conn)?,
                &sql,
                &params,
                chunksize.max(1),
                commit,
                &sink,
            )
        })
        .await
        .map_err(task_err)?
    }

    async fn begin(&mut self) -> Result<()> {
        // Oracle opens transactions implicitly; commit / rollback end them.
        self.in_tx = true;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.in_tx = false;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || lock(&conn)?.commit().map_err(db_err))
            .await
            .map_err(task_err)?
    }

    async fn rollback(&mut self) -> Result<()> {
        self.in_tx = false;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || lock(&conn)?.rollback().map_err(db_err))
            .await
            .map_err(task_err)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_types() {
        assert_eq!(kind_for(&OracleType::Number(38, 0)).0, ColumnKind::Int64);
        assert_eq!(
            kind_for(&OracleType::Number(0, -127)).0,
            ColumnKind::NumberAuto
        );
        assert_eq!(
            kind_for(&OracleType::Number(0, 0)).0,
            ColumnKind::NumberAuto
        );
        assert_eq!(kind_for(&OracleType::Number(10, 2)).0, ColumnKind::Decimal);
        assert_eq!(kind_for(&OracleType::NVarchar2(2000)).0, ColumnKind::Utf8);
        assert_eq!(
            kind_for(&OracleType::TimestampTZ(6)).0,
            ColumnKind::TimestampTz
        );
    }

    #[test]
    fn connect_strings() {
        let spec = ConnectionSpec::from_uri("oracle://system:oracle@localhost:1529/xe", None, None)
            .unwrap();
        assert_eq!(
            connect_string(&spec),
            "//localhost:1529/xe?transport_connect_timeout=5&connect_timeout=5"
        );
    }
}
