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
fn database_path(spec: &ConnectionSpec) -> Result<Option<String>> {
    let Some(path) = spec
        .database
        .as_deref()
        .filter(|path| !path.is_empty() && *path != ":memory:")
    else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|error| SqlError::Database(error.to_string()))?
            .join(path)
    };
    let canonical = absolute.canonicalize().unwrap_or_else(|_| {
        let parent = absolute.parent().and_then(|path| path.canonicalize().ok());
        match (parent, absolute.file_name()) {
            (Some(parent), Some(name)) => parent.join(name),
            _ => absolute.clone(),
        }
    });
    let path = canonical.to_str().ok_or_else(|| {
        SqlError::InvalidArgument("DuckDB database path is not valid UTF-8.".into())
    })?;
    Ok(Some(path.to_owned()))
}

struct DuckdbConnector {
    pid: u32,
    path: Option<String>,
    /// The database instance, opened on first use; connections are made on it.
    primary: Arc<Mutex<Option<Arc<ffi::Database>>>>,
}

fn check_pid(pid: u32) -> Result<()> {
    if pid != std::process::id() {
        return Err(SqlError::Database("DuckDB state belongs to the parent process; start a fresh interpreter (spawn or exec).".into()));
    }
    Ok(())
}

impl DuckdbConnector {
    fn open(&self, no_auto_install: bool) -> Result<ffi::Connection> {
        check_pid(self.pid)?;
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
    fn owner_pid(&self) -> Option<u32> {
        Some(self.pid)
    }
    async fn connect(&self) -> Result<Box<dyn NativeConn>> {
        check_pid(self.pid)?;
        // Find the library, or download it on first use (not under `MRSM_NO_AUTO_INSTALL`).
        let settings = duckdb_lib::Settings::from_process_env();
        duckdb_lib::ensure_loaded(&settings).await?;
        let conn = self.open(settings.no_auto_install)?;
        Ok(Box::new(DuckdbConn {
            pid: self.pid,
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

/// Optional host transport. Standalone executables leave this unset and use the C API.
pub type HostFactory = fn(&ConnectionSpec) -> Result<Option<Box<dyn NativeConnector>>>;
static HOST_FACTORY: OnceLock<HostFactory> = OnceLock::new();

/// Read-only facts about the embedding host's DuckDB transport. Metadata is not proof that
/// importing the driver or connecting to a database will succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostDiagnostic {
    /// This host uses the native library, just like a standalone process.
    Native,
    /// The public Python driver is selected, or would be selected on first use.
    Python {
        /// Installed metadata version (or an already imported driver's version).
        version: Option<String>,
        /// The metadata distribution location, when known.
        path: Option<String>,
        /// Whether this process already selected its transport.
        pinned: bool,
    },
    /// Reuse is refused, e.g. after fork or a changed explicit driver override.
    Refused(String),
}

/// A host diagnostic must not import optional drivers, select a policy, open a database or
/// install anything. No provider is registered by standalone executables.
pub type HostDiagnosticProvider = fn() -> HostDiagnostic;
static HOST_DIAGNOSTIC: OnceLock<HostDiagnosticProvider> = OnceLock::new();

/// Register the host's side-effect-free diagnostic provider.
pub fn register_host_diagnostic(provider: HostDiagnosticProvider) {
    let _ = HOST_DIAGNOSTIC.set(provider);
}

/// The embedding host's transport facts; `None` for standalone processes.
pub fn host_diagnostic() -> Option<HostDiagnostic> {
    HOST_DIAGNOSTIC.get().map(|provider| provider())
}

/// Register the Python host's lazy public-driver adapter, without importing a driver.
pub fn register_host_factory(factory: HostFactory) {
    let _ = HOST_FACTORY.set(factory);
}

/// Build the pool without opening the database.
pub(crate) fn connect_lazy(spec: &ConnectionSpec) -> Result<NativePool> {
    if let Some(factory) = HOST_FACTORY.get()
        && let Some(connector) = factory(spec)?
    {
        return Ok(NativePool::new(connector, spec.max_connections()));
    }
    // The C API backend currently supports only its explicit environment settings. Never
    // silently ignore options such as access_mode=READ_ONLY. Host-only identity attributes
    // are not DuckDB options and remain valid after an installed host selects this backend.
    let host = HOST_FACTORY.get().is_some();
    if spec
        .params
        .keys()
        .any(|key| !(host && matches!(key.as_str(), "_duckdb_memory_id" | "_duckdb_database")))
        || spec
            .options
            .as_ref()
            .is_some_and(|options| !options.pairs().is_empty())
    {
        return Err(SqlError::InvalidArgument("The native DuckDB C API does not support connection options; refusing to ignore them. Remove the options or use the public Python driver transport.".into()));
    }
    static PROCESS_PID: OnceLock<u32> = OnceLock::new();
    check_pid(*PROCESS_PID.get_or_init(std::process::id))?;
    type Primary = Mutex<Option<Arc<ffi::Database>>>;
    static DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<Primary>>>> = OnceLock::new();
    let path = database_path(spec)?;
    let primary = match &path {
        Some(path) => {
            let key = PathBuf::from(path);
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
        Box::new(DuckdbConnector {
            pid: std::process::id(),
            path,
            primary,
        }),
        spec.max_connections(),
    ))
}

struct DuckdbConn {
    pid: u32,
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
        check_pid(self.pid)?;
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
        check_pid(self.pid)?;
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
    fn is_broken(&self) -> bool {
        self.pid != std::process::id()
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
    fn connection_options_are_refused_before_native_open() {
        let path =
            std::env::temp_dir().join(format!("w17cduck-options-{}.db", uuid::Uuid::new_v4()));
        for option in [
            "access_mode=READ_ONLY",
            "threads=2",
            "_duckdb_database=ignored.db",
        ] {
            let spec = ConnectionSpec::from_uri(
                &format!("duckdb:///{}?{option}", path.display()),
                None,
                None,
            )
            .unwrap();
            let error = match connect_lazy(&spec) {
                Ok(_) => panic!("ignored DuckDB connection option"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("refusing to ignore"), "{error}");
            assert!(!path.exists());
        }
    }

    #[test]
    fn lazy_native_database_path_is_frozen_to_an_absolute_file() {
        let name = format!("w17cduck-relative-{}.db", uuid::Uuid::new_v4());
        let spec = ConnectionSpec::from_uri(&format!("duckdb:///{name}"), None, None).unwrap();
        let frozen = PathBuf::from(database_path(&spec).unwrap().unwrap());
        assert!(frozen.is_absolute());
        assert_eq!(frozen, std::env::current_dir().unwrap().join(name));
        assert!(!frozen.exists());
        let memory = ConnectionSpec::from_uri("duckdb:///:memory:", None, None).unwrap();
        assert!(database_path(&memory).unwrap().is_none());
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
