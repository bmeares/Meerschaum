//! DuckDB (`duckdb`) over DuckDB's own shared library, loaded at run time.
//!
//! Nothing of DuckDB is compiled into Meerschaum: [`ffi`] binds the library's stable C API and
//! [`crate::duckdb_lib`] finds it (`MRSM_DUCKDB_LIB`, the `<root>/drivers` cache, the system loader
//! path) or downloads it on first use. Results are read from data chunks ([`decode`]).
//!
//! DuckDB is embedded and synchronous: every call runs on a blocking thread. One database
//! instance is shared per file within this process; every pool uses connections to it.
//! Independent DuckDB instances can otherwise maintain incompatible snapshots of the same file.
//! Bound parameters are `$1`, `$2`, ...

mod decode;
pub(crate) mod ffi;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use async_trait::async_trait;
use chrono::NaiveDate;
use tokio::sync::mpsc;

use super::SqlValue;
use super::batch::{ColumnKind, ColumnPlan};
use super::native::{NativeConn, NativeConnector, NativeEvent, NativePool, blocking_send_rows};
use super::params::SqlParam;
use crate::duckdb_lib;
use crate::error::{Result, SqlError};
use crate::uri::ConnectionSpec;

fn db_err(e: String) -> SqlError {
    SqlError::Database(e)
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
    /// The database instance, opened on first use; connections are made on it.
    primary: Arc<Mutex<Option<Arc<ffi::Database>>>>,
}

impl DuckdbConnector {
    fn open(&self, no_auto_install: bool) -> Result<ffi::Connection> {
        let mut primary = self
            .primary
            .lock()
            .map_err(|_| SqlError::Database("The DuckDB connection lock is poisoned.".into()))?;
        if primary.is_none() {
            let db = ffi::Database::open(self.path.as_deref(), no_auto_install).map_err(db_err)?;
            *primary = Some(Arc::new(db));
        }
        let conn = primary
            .as_ref()
            .expect("opened")
            .connect()
            .map_err(db_err)?;
        // PARITY: the bundled DuckDB of earlier builds had no ICU, so every session ran in UTC and
        // a naive timestamp meant UTC wherever the host was. DuckDB's own library bundles ICU, whose
        // default `TimeZone` is the host's, so a naive value cast to `TIMESTAMPTZ` would shift by
        // the host offset. Sessions are pinned to UTC to keep the old, host-independent behavior
        // (a library without ICU rejects the setting, which is fine: it is UTC already).
        let _ = conn.batch("SET TimeZone = 'UTC'");
        Ok(conn)
    }
}

#[async_trait]
impl NativeConnector for DuckdbConnector {
    async fn connect(&self) -> Result<Box<dyn NativeConn>> {
        // Find the library, or download it on first use (not under `MRSM_NO_AUTO_INSTALL`).
        let settings = duckdb_lib::Settings::from_process_env();
        duckdb_lib::ensure_loaded(&settings).await?;
        let conn = self.open(settings.no_auto_install)?;
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
    type Primary = Mutex<Option<Arc<ffi::Database>>>;
    static DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<Primary>>>> = OnceLock::new();
    let path = database_path(spec);
    let primary = match &path {
        Some(path) => {
            let path = PathBuf::from(path);
            let absolute = if path.is_absolute() {
                path
            } else {
                std::env::current_dir()
                    .map_err(|e| SqlError::Database(e.to_string()))?
                    .join(path)
            };
            let key = absolute.canonicalize().unwrap_or_else(|_| {
                let parent = absolute.parent().and_then(|p| p.canonicalize().ok());
                match (parent, absolute.file_name()) {
                    (Some(parent), Some(name)) => parent.join(name),
                    _ => absolute.clone(),
                }
            });
            let mut databases = DATABASES
                .get_or_init(Default::default)
                .lock()
                .map_err(|_| SqlError::Database("The DuckDB registry lock is poisoned.".into()))?;
            databases.retain(|_, primary| primary.strong_count() > 0);
            match databases.get(&key).and_then(Weak::upgrade) {
                Some(primary) => primary,
                None => {
                    let primary = Arc::new(Mutex::new(None));
                    databases.insert(key, Arc::downgrade(&primary));
                    primary
                }
            }
        }
        None => Arc::new(Mutex::new(None)),
    };
    Ok(NativePool::new(
        Box::new(DuckdbConnector { path, primary }),
        spec.max_connections(),
    ))
}

struct DuckdbConn {
    conn: Arc<Mutex<ffi::Connection>>,
}

fn lock(conn: &Mutex<ffi::Connection>) -> Result<std::sync::MutexGuard<'_, ffi::Connection>> {
    conn.lock()
        .map_err(|_| SqlError::Database("The DuckDB connection lock is poisoned.".into()))
}

/// A bound parameter as a DuckDB value.
fn to_param(p: &SqlParam) -> ffi::Param {
    match p {
        SqlParam::Null => ffi::Param::Null,
        SqlParam::Bool(b) => ffi::Param::Bool(*b),
        SqlParam::Int(i) => ffi::Param::Int(*i),
        SqlParam::Float(f) => ffi::Param::Float(*f),
        SqlParam::Text(s) => ffi::Param::Text(s.clone()),
        SqlParam::Bytes(b) => ffi::Param::Bytes(b.clone()),
        SqlParam::Json(j) => ffi::Param::Text(j.to_string()),
        SqlParam::Timestamp(t) => ffi::Param::Timestamp(t.and_utc().timestamp_micros()),
        // PARITY: v4 bound aware timestamps as naive ones, which DuckDB reads in the session
        // `TimeZone` (wrong unless it is UTC); they are bound as `TIMESTAMPTZ` instants here.
        SqlParam::TimestampTz(t) => ffi::Param::TimestampTz(t.timestamp_micros()),
        SqlParam::Date(d) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            ffi::Param::Date((*d - epoch).num_days() as i32)
        }
    }
}

fn execute_blocking(conn: &ffi::Connection, sql: &str, params: &[SqlParam]) -> Result<u64> {
    if params.is_empty() {
        return match conn.prepare(sql) {
            Ok(stmt) => stmt
                .execute(&[])
                .map(|mut r| r.rows_changed())
                .map_err(db_err),
            // Several statements cannot be prepared at once; run them as a batch.
            Err(_) => conn
                .batch(sql)
                .map(|mut r| r.rows_changed())
                .map_err(db_err),
        };
    }
    let values: Vec<ffi::Param> = params.iter().map(to_param).collect();
    conn.prepare(sql)
        .and_then(|stmt| stmt.execute(&values))
        .map(|mut r| r.rows_changed())
        .map_err(db_err)
}

fn query_blocking(
    conn: &ffi::Connection,
    sql: &str,
    params: &[SqlParam],
    chunksize: usize,
    sink: &mpsc::Sender<NativeEvent>,
) -> Result<()> {
    let stmt = conn.prepare(sql).map_err(db_err)?;
    let values: Vec<ffi::Param> = params.iter().map(to_param).collect();
    let mut result = stmt.execute(&values).map_err(db_err)?;
    let count = result.column_count();
    let types: Vec<decode::ColumnType> = (0..count)
        .map(|i| decode::ColumnType::of(&result.column_type(i)))
        .collect();
    let plans: Vec<ColumnPlan> = (0..count)
        .map(|i| {
            let (kind, db_type) = decode::kind_for(&types[i]);
            let mut plan = ColumnPlan::new(result.column_name(i), kind, db_type);
            if kind == ColumnKind::Decimal {
                plan.scale = types[i].decimal.map_or(0, |(_, s, _)| s as u32);
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
    let mut notes = decode::Notes::default();
    while let Some(chunk) = result.next_chunk() {
        let vectors: Vec<_> = (0..count).map(|c| chunk.vector(c)).collect();
        for row in 0..chunk.len() {
            let mut cells = Vec::with_capacity(count);
            for c in 0..count {
                cells.push(
                    decode::cell(vectors[c], &types[c], row, &mut notes).map_err(|reason| {
                        SqlError::Conversion {
                            column: plans[c].name.clone(),
                            reason,
                        }
                    })?,
                );
            }
            buf.push(cells);
            if !blocking_send_rows(sink, &mut buf, chunksize, false) {
                return Ok(());
            }
        }
    }
    blocking_send_rows(sink, &mut buf, chunksize, true);
    if notes.clamped {
        tracing::warn!(
            "A DuckDB result held `infinity` timestamps or dates; they are read as 9999-12-31 / 0001-01-01 (Python's `datetime.max` / `datetime.min`)."
        );
    }
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
    use decode::*;

    fn ty(id: i32) -> ColumnType {
        ColumnType { id, decimal: None }
    }

    #[test]
    fn maps_types() {
        assert_eq!(kind_for(&ty(BIGINT)).0, ColumnKind::Int64);
        assert_eq!(kind_for(&ty(TIMESTAMP_TZ)).0, ColumnKind::TimestampTz);
        assert_eq!(kind_for(&ty(TIMESTAMP_NS)).0, ColumnKind::Timestamp);
        assert_eq!(kind_for(&ty(UUID)).0, ColumnKind::Uuid);
        assert_eq!(kind_for(&ty(LIST)).0, ColumnKind::Utf8);
        let dec = ColumnType {
            id: DECIMAL,
            decimal: Some((18, 3, BIGINT)),
        };
        assert_eq!(
            kind_for(&dec),
            (ColumnKind::Decimal, "DECIMAL(18,3)".into())
        );
        assert_eq!(
            kind_for(&ty(HUGEINT)),
            (ColumnKind::NumericText, "HUGEINT".into())
        );
        assert_eq!(kind_for(&ty(UHUGEINT)).1, "UHUGEINT");
    }

    #[test]
    fn binds_dates_and_aware_timestamps_natively() {
        let d = NaiveDate::from_ymd_opt(1970, 1, 3).unwrap();
        assert!(matches!(to_param(&SqlParam::Date(d)), ffi::Param::Date(2)));
        let t = chrono::DateTime::from_timestamp_micros(1_700_000_000_123_456).unwrap();
        assert!(matches!(
            to_param(&SqlParam::TimestampTz(t)),
            ffi::Param::TimestampTz(1_700_000_000_123_456)
        ));
        assert!(matches!(
            to_param(&SqlParam::Timestamp(t.naive_utc())),
            ffi::Param::Timestamp(1_700_000_000_123_456)
        ));
    }

    #[test]
    fn formats_exact_decimal_text() {
        assert_eq!(decimal_text(123456789, 6), "123.456789");
        assert_eq!(decimal_text(-5, 3), "-0.005");
        assert_eq!(decimal_text(42, 0), "42");
    }

    #[test]
    fn decodes_bignum() {
        // 0x80 header (positive), 3-byte header, then the magnitude big-endian.
        assert_eq!(bignum_text(&[0x80, 0, 1, 10]), "10");
        assert_eq!(bignum_text(&[0x80, 0, 2, 0x01, 0x2c]), "300");
        // Negative numbers are stored inverted.
        assert_eq!(bignum_text(&[!0x80, !0, !2, !0x01, !0x2c]), "-300");
        assert_eq!(bignum_text(&[0x80, 0, 0]), "0");
    }
}
