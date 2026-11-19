//! The driver layer (D13): connection pools per flavor family, `read` as an Arrow record-batch
//! stream, `exec`, `exec_queries` and `value`.
//!
//! | Family | Flavors | Driver |
//! |---|---|---|
//! | PostgreSQL | `postgresql`, `timescaledb`, `timescaledb-ha`, `postgis`, `citus`, `cockroachdb` | `sqlx` |
//! | MySQL | `mysql`, `mariadb` | `sqlx` |
//! | SQLite | `sqlite`, `geopackage` | `sqlx` |
//! | MSSQL | `mssql` | `tiberius` (cargo feature `mssql`, on by default) |
//! | Oracle | `oracle` | `oracle` (cargo feature `oracle`; Oracle Instant Client at runtime) |
//! | DuckDB | `duckdb` | DuckDB's own shared library, loaded at run time (cargo feature `duckdb`, on by default) |
//!
//! Without its cargo feature a flavor is refused with [`SqlError::DriverUnavailable`](crate::SqlError::DriverUnavailable).
//! The non-`sqlx` drivers share the pool and operation layer in `native.rs`.
//!
//! These methods serve the Rust core, Rust plugins and the Python façade's raw SQL
//! (`SQLConnector.read` / `exec` / `value` / `to_sql`, rc15 Q53, through [`raw`]: statements with
//! bound parameters whose rows keep what each flavor's Python driver returns).

#[cfg(all(test, unix))]
mod authorization_read_tests;
pub(crate) mod authorization_reads;
mod backend;
pub mod batch;
pub mod connect;
pub mod data_commit;
#[cfg(feature = "duckdb")]
pub(crate) mod duckdb;
pub mod gpkg_functions;
#[cfg(feature = "mssql")]
mod mssql;
mod mysql;
pub(crate) mod mysql_append;
pub(crate) mod mysql_authority;
#[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
mod native;
#[cfg(feature = "oracle")]
mod oracle;
pub mod params;
mod postgres;
pub mod raw;
pub(crate) mod registered_source;
#[cfg(test)]
mod scoped_pg;
mod source_snapshot;
mod sqlite;
mod unsupported;
pub(crate) mod write_guard;

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures_util::stream::{BoxStream, Stream, StreamExt, TryStreamExt};
use rust_decimal::Decimal;

use meerschaum_kernel::Frame;

use crate::error::Result;
use crate::flavor::{DriverFamily, Flavor};
use crate::uri::ConnectionSpec;

pub use authorization_reads::Handle as AuthorizationReadScope;
pub use batch::{ColumnKind, ColumnPlan, GeometryEncoding};
#[cfg(feature = "duckdb")]
pub use duckdb::register_host_factory as register_duckdb_host_factory;
pub use duckdb::{
    HostDiagnostic as DuckdbHostDiagnostic, host_diagnostic as duckdb_host_diagnostic,
    register_host_diagnostic as register_duckdb_host_diagnostic,
};
#[cfg(feature = "duckdb")]
pub use native::{NativeConn, NativeConnector, NativeEvent};
pub use params::SqlParam;

/// Python's default `chunksize` (`system.connectors.sql.chunksize`).
pub const DEFAULT_CHUNKSIZE: usize = 100_000;

/// The `sqlx` connect options of a PostgreSQL-family spec, for a dedicated connection outside
/// the pool (`crate::extensions`).
pub(crate) fn pg_connect_options(
    spec: &ConnectionSpec,
) -> Result<sqlx::postgres::PgConnectOptions> {
    postgres::connect_options(spec)
}

/// A single value read from the database.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Decimal(Decimal),
    Text(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    /// JSON document text as returned by the database.
    Json(String),
}

impl SqlValue {
    /// Whether this is `NULL`.
    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }

    /// The value as an `i64`, if it is an integer that fits.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            SqlValue::Int(i) => Some(*i),
            SqlValue::UInt(u) => i64::try_from(*u).ok(),
            SqlValue::Decimal(d) if d.fract().is_zero() => d.to_string().parse().ok(),
            SqlValue::Text(s) => s.trim().parse().ok(),
            _ => None,
        }
    }
}

impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValue::Null => f.write_str("None"),
            SqlValue::Bool(b) => f.write_str(if *b { "True" } else { "False" }),
            SqlValue::Int(i) => write!(f, "{i}"),
            SqlValue::UInt(u) => write!(f, "{u}"),
            SqlValue::Float(x) => write!(f, "{x}"),
            SqlValue::Decimal(d) => write!(f, "{d}"),
            SqlValue::Text(s) | SqlValue::Json(s) => f.write_str(s),
            SqlValue::Bytes(b) => {
                for byte in b {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
            SqlValue::Uuid(u) => write!(f, "{u}"),
            SqlValue::Date(d) => write!(f, "{}", d.format("%Y-%m-%d")),
            SqlValue::Time(t) => write!(f, "{}", t.format("%H:%M:%S%.f")),
            SqlValue::Timestamp(dt) => write!(f, "{}", dt.format("%Y-%m-%d %H:%M:%S%.f")),
            SqlValue::TimestampTz(dt) => {
                write!(f, "{}", dt.format("%Y-%m-%d %H:%M:%S%.f%:z"))
            }
        }
    }
}

/// Convert driver record batches (one schema) into a kernel [`Frame`].
pub fn batches_to_frame(batches: &[RecordBatch]) -> Result<Frame> {
    Ok(Frame::from_record_batches(batches)?)
}

/// A stream of Arrow record batches sharing one schema.
pub struct RecordBatchStream {
    schema: SchemaRef,
    inner: BoxStream<'static, Result<RecordBatch>>,
}

impl RecordBatchStream {
    /// Wrap a schema and a stream of batches.
    pub fn new(schema: SchemaRef, inner: BoxStream<'static, Result<RecordBatch>>) -> Self {
        Self { schema, inner }
    }

    /// The schema of every batch in the stream.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Collect every batch.
    pub async fn collect_batches(self) -> Result<Vec<RecordBatch>> {
        self.inner.try_collect().await
    }
}

impl Stream for RecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

impl fmt::Debug for RecordBatchStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordBatchStream")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// Run a backend operation on its own task with owned arguments.
///
/// The backend functions are generic over the driver family with higher-ranked `Executor`
/// bounds; rustc cannot prove their futures `Send` for every lifetime when they are awaited
/// inside `async_trait` methods. Spawning them with `'static` arguments sidesteps that.
macro_rules! on_pool {
    ($pool:expr, $p:ident, $B:ident => $body:expr) => {
        on_pool!($pool, $p, $B, _tx_mod => $body)
    };
    ($pool:expr, $p:ident, $B:ident, $M:ident => $body:expr) => {{
        let handle = match $pool.clone() {
            Pool::Postgres($p) => {
                #[allow(unused)]
                type $B = postgres::PgBackend;
                #[allow(unused_imports)]
                use backend::pg_tx as $M;
                tokio::spawn(async move { $body })
            }
            Pool::Mysql($p) => {
                #[allow(unused)]
                type $B = mysql::MySqlBackend;
                #[allow(unused_imports)]
                use backend::mysql_tx as $M;
                tokio::spawn(async move { $body })
            }
            Pool::Sqlite($p) => {
                #[allow(unused)]
                type $B = sqlite::SqliteBackend;
                #[allow(unused_imports)]
                use backend::sqlite_tx as $M;
                tokio::spawn(async move { $body })
            }
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            Pool::Native(_) => unreachable!("native pools are dispatched before `on_pool!`"),
        };
        handle.await.map_err(|e| {
            crate::error::SqlError::InvalidArgument(format!("The database task failed: {e}"))
        })?
    }};
}

/// An open transaction (SQLAlchemy `Session`): statements see each other's uncommitted
/// effects until [`SqlTransaction::commit`]. Dropping it without committing rolls back.
pub struct SqlTransaction {
    // Drop the capability before SQLx schedules rollback / returns a connection to its pool.
    authorization_pin: Option<authorization_reads::Owner>,
    authorization_read: Option<authorization_reads::Handle>,
    inner: TxInner,
    mutation_check: Option<std::sync::Arc<dyn meerschaum_connector::mutation::MutationCheck>>,
    write_lock: bool,
    refused: bool,
}

enum TxInner {
    PinnedRead,
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
    Mysql(
        sqlx::Transaction<'static, sqlx::MySql>,
        Option<mysql_authority::Policy>,
    ),
    Sqlite(sqlx::Transaction<'static, sqlx::Sqlite>),
    #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
    Native(native::NativeTx),
}

impl fmt::Debug for SqlTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlTransaction").finish_non_exhaustive()
    }
}

/// Debug output for a statement about to run (`--debug`, see `meerschaum_core::dprint!`): every
/// statement the connector runs goes through one of the driver's entry points below. Expands to
/// one atomic load while debugging is off.
/// The statement is printed through `meerschaum_core::debug::redact_sql` (secret columns' literals
/// and URI passwords hidden); the statement that runs is unchanged.
macro_rules! debug_sql {
    ($sql:expr) => {
        meerschaum_core::dprint!("{}", meerschaum_core::debug::redact_sql($sql))
    };
    ($sql:expr, $params:expr) => {
        meerschaum_core::dprint!(
            "{}\n({} bound parameters)",
            meerschaum_core::debug::redact_sql($sql),
            $params.len()
        )
    };
}

impl SqlTransaction {
    /// The host's frame-source callbacks remain under this exact still-held transaction.
    pub fn authorization_read_scope(&self) -> Option<AuthorizationReadScope> {
        self.authorization_pin
            .as_ref()
            .map(authorization_reads::Owner::handle)
    }

    async fn with_held_authorization_reads<F: std::future::Future>(
        &mut self,
        future: F,
    ) -> Result<F::Output> {
        if matches!(self.inner, TxInner::Mysql(_, Some(_))) {
            let TxInner::Mysql(tx, Some(policy)) =
                std::mem::replace(&mut self.inner, TxInner::PinnedRead)
            else {
                unreachable!("verified InnoDB transaction")
            };
            let held = mysql_authority::Held::new(tx, policy.clone());
            let result = held.scope(future).await;
            self.inner = TxInner::Mysql(held.take().await?, Some(policy));
            return result;
        }
        Ok(match self.authorization_read_scope() {
            Some(scope) => scope.scope(future).await,
            None => future.await,
        })
    }

    async fn check_write(&mut self) -> Result<()> {
        let result = Box::pin(self.check_write_inner()).await;
        if result.is_err() {
            self.refused = true;
            self.authorization_pin.take();
        }
        result
    }

    async fn check_write_inner(&mut self) -> Result<()> {
        let Some(check) = self.mutation_check.clone() else {
            return Ok(());
        };
        if self.refused || !self.write_lock {
            self.refused = true;
            return Err(crate::SqlError::InvalidArgument(
                "This scoped transaction has no admitted write lock.".into(),
            ));
        }
        if self.authorization_pin.is_none() {
            return Box::pin(
                self.with_held_authorization_reads(meerschaum_connector::mutation::recheck(&check)),
            )
            .await?
            .map_err(crate::SqlError::InvalidArgument);
        }
        let TxInner::Sqlite(transaction) = &mut self.inner else {
            self.refused = true;
            return Err(crate::SqlError::InvalidArgument(
                "This write has no native SQLite lock proof.".into(),
            ));
        };
        let identity = Box::pin(authorization_reads::identity(transaction, true)).await?;
        let pin = self.authorization_pin.as_ref().ok_or_else(|| {
            crate::SqlError::InvalidArgument(
                "This write has no lock-backed authorization snapshot.".into(),
            )
        })?;
        pin.verify(&identity)?;
        let handle = pin.handle();
        if let Err(reason) =
            Box::pin(handle.scope(meerschaum_connector::mutation::recheck(&check))).await
        {
            self.refused = true;
            return Err(crate::SqlError::InvalidArgument(reason));
        }
        let identity = Box::pin(authorization_reads::identity(transaction, true)).await?;
        pin.verify(&identity)?;
        pin.freeze();
        Ok(())
    }

    /// Install a host-owned SQLite VM budget for a bounded native read. The caller must retain
    /// this transaction through cancellation, clear the handler, and roll back before pool
    /// reuse. Returning false from SQLx's safe callback interrupts sqlite3_step itself.
    pub async fn set_sqlite_read_budget(
        &mut self,
        deadline: std::time::Instant,
        cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<()> {
        if matches!(self.inner, TxInner::PinnedRead) {
            return Ok(());
        }
        let TxInner::Sqlite(transaction) = &mut self.inner else {
            return Err(crate::SqlError::InvalidArgument(
                "A SQLite read budget requires a SQLite transaction.".into(),
            ));
        };
        transaction
            .lock_handle()
            .await?
            .set_progress_handler(1000, move || {
                !cancelled.load(std::sync::atomic::Ordering::Relaxed)
                    && std::time::Instant::now() < deadline
            });
        Ok(())
    }

    /// Remove a read budget before rollback/pool reuse, including every error and cancellation
    /// path. A handler is never left armed on a shared connector's next operation.
    pub async fn clear_sqlite_read_budget(&mut self) -> Result<()> {
        if matches!(self.inner, TxInner::PinnedRead) {
            return Ok(());
        }
        let TxInner::Sqlite(transaction) = &mut self.inner else {
            return Err(crate::SqlError::InvalidArgument(
                "A SQLite read budget requires a SQLite transaction.".into(),
            ));
        };
        transaction.lock_handle().await?.remove_progress_handler();
        Ok(())
    }

    /// Stream one SQLite read statement through bounded frames while retaining this exact
    /// transaction. The caller owns cancellation, progress-handler cleanup and rollback.
    pub async fn for_each_sqlite_batch<F>(
        &mut self,
        sql: &str,
        chunksize: usize,
        max_bytes: usize,
        accept: F,
    ) -> Result<()>
    where
        F: FnMut(Frame) -> Result<()>,
    {
        if chunksize == 0 || chunksize > 100_000 || max_bytes == 0 || max_bytes > 8 * 1024 * 1024 {
            return Err(crate::SqlError::InvalidArgument(
                "Invalid SQLite batch size.".into(),
            ));
        }
        let TxInner::Sqlite(transaction) = &mut self.inner else {
            return Err(crate::SqlError::InvalidArgument(
                "Snapshot batches require a SQLite transaction.".into(),
            ));
        };
        backend::for_each_sqlite_batch(transaction, sql, chunksize, max_bytes, accept).await
    }

    /// Execute one or more statements, returning the rows affected.
    pub async fn exec(&mut self, sql: &str) -> Result<u64> {
        self.check_write().await?;
        debug_sql!(sql);
        match &mut self.inner {
            TxInner::PinnedRead => Err(crate::SqlError::InvalidArgument(
                "An authorization snapshot cannot write.".into(),
            )),
            TxInner::Postgres(tx) => backend::pg_tx::tx_exec(tx, sql).await,
            TxInner::Mysql(tx, _) => backend::mysql_tx::tx_exec(tx, sql).await,
            TxInner::Sqlite(tx) => backend::sqlite_tx::tx_exec(tx, sql).await,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.exec(sql).await,
        }
    }

    /// Run a query and collect the result as a kernel [`Frame`].
    pub async fn read_frame(&mut self, sql: &str) -> Result<Frame> {
        debug_sql!(sql);
        if let Some(handle) = &self.authorization_read {
            if let Some(authorization_reads::Output::Batches(batches)) =
                handle.get(sql, &[], authorization_reads::Mode::Batches)?
            {
                return batches_to_frame(&batches);
            }
            let TxInner::Sqlite(transaction) = &mut self.inner else {
                return Err(crate::SqlError::InvalidArgument(
                    "The authorization read has no live snapshot.".into(),
                ));
            };
            let (output, bytes) = backend::authorization_read(
                transaction,
                sql,
                &[],
                authorization_reads::Mode::Batches,
            )
            .await?;
            let authorization_reads::Output::Batches(batches) =
                handle.store(sql, &[], authorization_reads::Mode::Batches, output, bytes)?
            else {
                unreachable!()
            };
            return batches_to_frame(&batches);
        }
        let batches = match &mut self.inner {
            TxInner::PinnedRead => {
                return Err(crate::SqlError::InvalidArgument(
                    "Invalid authorization snapshot.".into(),
                ));
            }
            TxInner::Postgres(tx) => backend::pg_tx::tx_read(tx, sql).await?,
            TxInner::Mysql(tx, _) => backend::mysql_tx::tx_read(tx, sql).await?,
            TxInner::Sqlite(tx) => backend::sqlite_tx::tx_read(tx, sql).await?,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.read(sql).await?,
        };
        batches_to_frame(&batches)
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<()> {
        if self.mutation_check.is_some()
            && self.write_lock
            && let Err(error) = self.check_write().await
        {
            Box::pin(self.rollback()).await?;
            return Err(error);
        }
        self.authorization_pin.take();
        match self.inner {
            TxInner::PinnedRead => {
                return Err(crate::SqlError::InvalidArgument(
                    "An authorization snapshot cannot commit.".into(),
                ));
            }
            TxInner::Postgres(tx) => tx.commit().await?,
            TxInner::Mysql(tx, _) => tx.commit().await?,
            TxInner::Sqlite(tx) => tx.commit().await?,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.commit().await?,
        }
        Ok(())
    }

    /// Roll the transaction back.
    pub async fn rollback(mut self) -> Result<()> {
        let check = self
            .write_lock
            .then(|| self.mutation_check.clone())
            .flatten();
        self.authorization_pin.take();
        let result = match self.inner {
            TxInner::PinnedRead => Ok(()),
            TxInner::Postgres(tx) => tx.rollback().await.map_err(crate::SqlError::from),
            TxInner::Mysql(tx, _) => tx.rollback().await.map_err(crate::SqlError::from),
            TxInner::Sqlite(tx) => tx.rollback().await.map_err(crate::SqlError::from),
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.rollback().await,
        };
        if let Some(check) = check {
            check.rollback_completed(result.is_ok());
        }
        result
    }
}

#[derive(Clone)]
enum Pool {
    Postgres(sqlx::PgPool),
    Mysql(sqlx::MySqlPool),
    Sqlite(sqlx::SqlitePool),
    /// `tiberius`, `duckdb` or `oracle` (see `native.rs`).
    #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
    Native(native::NativePool),
}

/// Run a native-pool operation on its own task (isolating driver panics, as `on_pool!` does).
#[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
macro_rules! on_native {
    ($pool:expr, $p:ident => $body:expr) => {
        if let Pool::Native(p) = &$pool {
            let $p = p.clone();
            return tokio::spawn(async move { $body }).await.map_err(|e| {
                crate::error::SqlError::InvalidArgument(format!("The database task failed: {e}"))
            })?;
        }
    };
}
#[cfg(not(any(feature = "mssql", feature = "duckdb", feature = "oracle")))]
macro_rules! on_native {
    ($pool:expr, $p:ident => $body:expr) => {};
}

/// Open a native pool for the MSSQL / Oracle / DuckDB families, verifying one connection.
async fn connect_native(spec: &ConnectionSpec) -> Result<Pool> {
    match spec.flavor.driver_family() {
        #[cfg(feature = "mssql")]
        DriverFamily::Mssql => Ok(Pool::Native(mssql::connect(spec).await?)),
        #[cfg(feature = "duckdb")]
        DriverFamily::Duckdb => Ok(Pool::Native(duckdb::connect(spec).await?)),
        #[cfg(feature = "oracle")]
        DriverFamily::Oracle => Ok(Pool::Native(oracle::connect(spec).await?)),
        _ => Err(unsupported::unavailable(spec.flavor)),
    }
}

/// Build a native pool without connecting (connections open on first use).
fn connect_native_lazy(spec: &ConnectionSpec) -> Result<Pool> {
    match spec.flavor.driver_family() {
        #[cfg(feature = "mssql")]
        DriverFamily::Mssql => Ok(Pool::Native(mssql::connect_lazy(spec)?)),
        #[cfg(feature = "duckdb")]
        DriverFamily::Duckdb => Ok(Pool::Native(duckdb::connect_lazy(spec)?)),
        #[cfg(feature = "oracle")]
        DriverFamily::Oracle => Ok(Pool::Native(oracle::connect_lazy(spec)?)),
        _ => Err(unsupported::unavailable(spec.flavor)),
    }
}

/// A pooled connection to one database.
#[derive(Clone)]
pub struct SqlDriver {
    lineage: std::sync::Arc<()>,
    windowed_pool: std::sync::Arc<tokio::sync::OnceCell<sqlx::SqlitePool>>,
    flavor: Flavor,
    pool: Pool,
    /// One lazy writer slot for scoped SQLite transactions. Canonical authorization reads
    /// retain the ordinary pool, including on installations with only one read slot.
    scoped_sqlite_pool: Option<sqlx::SqlitePool>,
    /// A separate slot lets an administrator's late recheck read the canonical
    /// catalog even when its normal PostgreSQL pool has only one connection.
    admin_postgres_pool: Option<sqlx::PgPool>,
    /// Dedicated read-only source pool with one-row native prefetch.
    snapshot_sqlite_pool: Option<sqlx::SqlitePool>,
    /// The fail-fast gate of the first connection (network flavors only; see [`connect`]).
    gate: Option<std::sync::Arc<ConnectGate>>,
}

/// The first-connection gate shared by the clones of one driver.
struct ConnectGate {
    spec: ConnectionSpec,
    established: tokio::sync::OnceCell<()>,
    failures: connect::FailureCache,
}

impl fmt::Debug for SqlDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlDriver")
            .field("flavor", &self.flavor)
            .finish_non_exhaustive()
    }
}

/// Whether an error says the connection to the server was lost (an I/O error, a server that
/// terminated the session: `57P01` / `57P02`, or a connection exception of class `08`).
fn is_lost_connection(error: &crate::error::SqlError) -> bool {
    match error {
        crate::error::SqlError::Driver(sqlx::Error::Io(_)) => true,
        crate::error::SqlError::Driver(sqlx::Error::Database(db)) => db
            .code()
            .is_some_and(|c| c.starts_with("08") || matches!(&*c, "57P01" | "57P02")),
        _ => false,
    }
}

/// A pooled connection idle for less than this is handed out without a liveness check.
pub(crate) const PING_IDLE_AFTER: std::time::Duration = std::time::Duration::from_secs(1);

/// A cold internal initialization owns a dedicated connection until its metadata and DDL are
/// ready. These connections never enter a pool, so cancellation also releases session locks.
pub(crate) enum InternalInitializationGuard {
    Postgres {
        _connection: Box<sqlx::PgConnection>,
    },
    Mysql {
        _connection: Box<sqlx::MySqlConnection>,
    },
    #[cfg(feature = "mssql")]
    Mssql {
        _connection: Box<dyn native::NativeConn>,
    },
}

/// MySQL names are server-wide (at most 64 characters); MSSQL names belong to a database.
/// Include the selected database and the resolved, flavor-truncated physical table in both.
/// Case folding may serialize distinct case-sensitive names, but cannot split the usual
/// case-insensitive spellings of one internal table into different locks.
fn session_initialization_lock_name(
    database: &str,
    schema: Option<&str>,
    target: &str,
    flavor: Flavor,
) -> String {
    use sha2::{Digest, Sha256};

    let database = database.to_lowercase();
    let table = crate::ident::sql_item_name(target, flavor, schema).to_lowercase();
    let mut digest = Sha256::new();
    digest.update(b"meerschaum:internal-table-initialization:");
    digest.update((database.len() as u64).to_be_bytes());
    digest.update(database.as_bytes());
    digest.update(table.as_bytes());
    hex::encode(digest.finalize())
}

/// sqlx pings every idle connection before handing it out (`test_before_acquire`): one extra
/// protocol round trip per acquire, half of a fresh `Pipe`'s `get_sync_time`. A connection used
/// within [`PING_IDLE_AFTER`] is trusted instead; one idle for longer is still pinged, and a
/// broken one is discarded and replaced as before (an error from the hook closes it and the
/// acquire takes the next idle connection or opens a new one).
fn ping_idle_connections<DB: sqlx::Database>(
    options: sqlx::pool::PoolOptions<DB>,
) -> sqlx::pool::PoolOptions<DB> {
    options
        .test_before_acquire(false)
        .before_acquire(|conn, meta| {
            Box::pin(async move {
                if meta.idle_for >= PING_IDLE_AFTER {
                    sqlx::Connection::ping(conn).await?;
                }
                Ok(true)
            })
        })
}

impl SqlDriver {
    /// Serialize cold internal-table initialization across independent connectors/processes.
    /// The connection is deliberately unpooled: holding a pool slot while initialization
    /// uses the driver would deadlock a one-connection pool. Dropping it (also on cancellation)
    /// closes the socket and releases its transaction/session lock; no lock enters the pool.
    pub(crate) async fn lock_internal_initialization(
        &self,
        target: &str,
        schema: Option<&str>,
    ) -> Result<Option<InternalInitializationGuard>> {
        use sha2::{Digest, Sha256};
        use sqlx::Connection;

        if let Pool::Mysql(pool) = &self.pool {
            self.ready().await?;
            let limit = pool.options().get_acquire_timeout();
            let connect_limit = self
                .gate
                .as_ref()
                .map(|gate| connect::connect_timeout(&gate.spec))
                .unwrap_or_else(|| meerschaum_core::connect::connect_timeout(None));
            let guard = tokio::time::timeout(
                limit,
                Box::pin(async {
                    let mut connection = tokio::time::timeout(
                        connect_limit,
                        sqlx::MySqlConnection::connect_with(&pool.connect_options()),
                    )
                    .await
                    .map_err(|_| sqlx::Error::PoolTimedOut)??;
                    let selected: Option<String> = sqlx::query_scalar("SELECT DATABASE()")
                        .fetch_one(&mut connection)
                        .await?;
                    let database = schema
                        .filter(|schema| !schema.is_empty())
                        .or(selected.as_deref())
                        .ok_or_else(|| {
                            crate::SqlError::Database(
                                "Internal initialization requires a selected database.".into(),
                            )
                        })?;
                    let key = session_initialization_lock_name(database, None, target, self.flavor);
                    let acquired: Option<i64> = sqlx::query_scalar("SELECT GET_LOCK(?, ?)")
                        .bind(key)
                        .bind(limit.as_secs_f64())
                        .fetch_one(&mut connection)
                        .await?;
                    match acquired {
                        Some(1) => Ok(InternalInitializationGuard::Mysql {
                            _connection: Box::new(connection),
                        }),
                        Some(0) => Err(crate::SqlError::Driver(sqlx::Error::PoolTimedOut)),
                        _ => Err(crate::SqlError::Database(
                            "Could not acquire the internal initialization lock.".into(),
                        )),
                    }
                }),
            )
            .await
            .map_err(|_| sqlx::Error::PoolTimedOut)??;
            return Ok(Some(guard));
        }
        #[cfg(feature = "mssql")]
        if let Pool::Native(pool) = &self.pool
            && self.flavor == Flavor::Mssql
        {
            self.ready().await?;
            let limit = self
                .gate
                .as_ref()
                .map(|gate| connect::connect_timeout(&gate.spec))
                .unwrap_or_else(|| meerschaum_core::connect::connect_timeout(None));
            let guard = tokio::time::timeout(
                limit,
                Box::pin(async {
                    let mut connection = pool.connect_unpooled().await?;
                    let (_, identity) = native::collect_rows(
                        connection.as_mut(),
                        "SELECT DB_NAME(), COALESCE(@P1, SCHEMA_NAME())",
                        &[schema
                            .map_or(SqlParam::Null, |schema| SqlParam::Text(schema.to_owned()))],
                        1,
                    )
                    .await?;
                    let identity = identity.first();
                    let database = identity.and_then(|row| row.first());
                    let schema = identity.and_then(|row| row.get(1));
                    let (Some(SqlValue::Text(database)), Some(SqlValue::Text(schema))) =
                        (database, schema)
                    else {
                        return Err(crate::SqlError::Database(
                            "Could not resolve the internal initialization database and schema."
                                .into(),
                        ));
                    };
                    let key = session_initialization_lock_name(
                        database,
                        Some(schema),
                        target,
                        self.flavor,
                    );
                    let timeout_ms = i32::try_from(limit.as_millis()).unwrap_or(i32::MAX);
                    let (_, rows) = native::collect_rows(
                        connection.as_mut(),
                        "DECLARE @result int; \
                     EXEC @result = sys.sp_getapplock @Resource = @P1, \
                     @LockMode = 'Exclusive', @LockOwner = 'Session', \
                     @LockTimeout = @P2, @DbPrincipal = 'public'; SELECT @result;",
                        &[SqlParam::Text(key), SqlParam::Int(i64::from(timeout_ms))],
                        1,
                    )
                    .await?;
                    match rows
                        .first()
                        .and_then(|row| row.first())
                        .and_then(SqlValue::as_i64)
                    {
                        Some(code) if code >= 0 => Ok(InternalInitializationGuard::Mssql {
                            _connection: connection,
                        }),
                        Some(-1) => Err(crate::SqlError::Driver(sqlx::Error::PoolTimedOut)),
                        code => Err(crate::SqlError::Database(format!(
                            "Could not acquire the internal initialization lock ({code:?})."
                        ))),
                    }
                }),
            )
            .await
            .map_err(|_| sqlx::Error::PoolTimedOut)??;
            return Ok(Some(guard));
        }
        let Pool::Postgres(pool) = &self.pool else {
            return Ok(None);
        };
        self.ready().await?;
        // Cockroach speaks the PostgreSQL wire protocol but has no advisory locks.
        if self.flavor == Flavor::Cockroachdb {
            return Ok(None);
        }
        // Bound both establishing the extra connection and waiting for initialization
        // with the pool's acquisition timeout. Server lock/statement timeouts from the
        // original connection options continue to apply and their errors propagate.
        let connection = tokio::time::timeout(pool.options().get_acquire_timeout(), async {
            let mut connection = sqlx::PgConnection::connect_with(&pool.connect_options()).await?;
            let schema = match schema {
                Some(schema) => Some(schema.to_owned()),
                None => {
                    sqlx::query_scalar::<_, Option<String>>("SELECT current_schema()")
                        .fetch_one(&mut connection)
                        .await?
                }
            };
            let table = crate::ident::sql_item_name(target, self.flavor, schema.as_deref());
            let mut digest = Sha256::new();
            digest.update(b"meerschaum:internal-table-initialization:");
            digest.update(table.as_bytes());
            let key = i64::from_be_bytes(digest.finalize()[..8].try_into().expect("eight bytes"));
            sqlx::query("BEGIN").execute(&mut connection).await?;
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(key)
                .execute(&mut connection)
                .await?;
            Ok::<_, sqlx::Error>(connection)
        })
        .await
        .map_err(|_| sqlx::Error::PoolTimedOut)??;
        Ok(Some(InternalInitializationGuard::Postgres {
            _connection: Box::new(connection),
        }))
    }

    /// Open a connection pool for a connection spec. Connections are opened lazily by the pool,
    /// except that this verifies one connection can be established.
    pub async fn connect(spec: &ConnectionSpec) -> Result<Self> {
        match spec.flavor.driver_family() {
            DriverFamily::Sqlite => Ok(Self::new(spec, Pool::Sqlite(sqlite::connect(spec).await?))),
            DriverFamily::Duckdb => Ok(Self::new(spec, connect_native(spec).await?)),
            // Network flavors: build the pool, then establish the first connection with the
            // fail-fast gate (`connect_timeout`, classified errors).
            _ => {
                let driver = Self::connect_lazy(spec)?;
                driver.ready().await?;
                Ok(driver)
            }
        }
    }

    fn new(spec: &ConnectionSpec, pool: Pool) -> Self {
        let networked = matches!(
            spec.flavor.driver_family(),
            DriverFamily::Postgres
                | DriverFamily::Mysql
                | DriverFamily::Mssql
                | DriverFamily::Oracle
        );
        let scoped_sqlite_pool = match &pool {
            Pool::Sqlite(readers) => Some(
                sqlite::pool_options(spec)
                    .max_connections(1)
                    .min_connections(0)
                    .acquire_timeout(readers.options().get_acquire_timeout())
                    .after_release(|connection, _| {
                        Box::pin(async move {
                            // Flush SQLx's queued rollback before checking its transaction
                            // state. A failed rollback can leave the worker alive but its
                            // transaction depth nonzero; discard that connection instead
                            // of returning it as a usable authorized writer.
                            sqlx::Connection::ping(connection).await?;
                            Ok(!sqlx::Connection::is_in_transaction(connection))
                        })
                    })
                    .connect_lazy_with((*readers.connect_options()).clone()),
            ),
            _ => None,
        };
        let admin_postgres_pool = match &pool {
            Pool::Postgres(readers) => Some(
                ping_idle_connections(sqlx::postgres::PgPoolOptions::new().max_connections(1))
                    .acquire_timeout(readers.options().get_acquire_timeout())
                    .connect_lazy_with((*readers.connect_options()).clone()),
            ),
            _ => None,
        };
        let snapshot_sqlite_pool = match &pool {
            Pool::Sqlite(_) => source_snapshot::pool(spec),
            _ => None,
        };
        Self {
            lineage: std::sync::Arc::new(()),
            windowed_pool: std::sync::Arc::new(tokio::sync::OnceCell::new()),
            flavor: spec.flavor,
            pool,
            scoped_sqlite_pool,
            admin_postgres_pool,
            snapshot_sqlite_pool,
            gate: networked.then(|| {
                std::sync::Arc::new(ConnectGate {
                    spec: spec.clone(),
                    established: tokio::sync::OnceCell::new(),
                    failures: connect::FailureCache::default(),
                })
            }),
        }
    }

    /// Make sure the first connection was established (once per driver): bounded by
    /// `connect_timeout`, failing at once with a classified message when the host refuses, does
    /// not resolve, is unreachable or rejects the credentials. Later calls cost one atomic load,
    /// and an established pool keeps the driver's own reconnect behavior.
    pub(crate) async fn ready(&self) -> Result<()> {
        if mysql_authority::active() {
            return Err(mysql_authority::refused());
        }
        let Some(gate) = &self.gate else {
            return Ok(());
        };
        if gate.established.initialized() {
            return Ok(());
        }
        // The slow path is boxed: `ready()` is awaited at the top of every driver method, and
        // an inline `get_or_try_init` + `select!` state machine would be embedded in every one
        // of those futures (and, unoptimized, in every future above them), which overflowed
        // the 2 MiB stack of a runtime worker on a plain `sync pipes` in debug builds.
        Box::pin(self.establish_first(gate)).await
    }

    /// The slow path of [`SqlDriver::ready`]: establish the first connection once.
    async fn establish_first(&self, gate: &ConnectGate) -> Result<()> {
        if let Some(failure) = gate.failures.recent() {
            return Err(crate::error::SqlError::Connect(failure));
        }
        let result = gate
            .established
            .get_or_try_init(|| async {
                let started = std::time::Instant::now();
                let spec = &gate.spec;
                match &self.pool {
                    Pool::Postgres(p) => {
                        connect::establish(spec, async {
                            p.acquire()
                                .await
                                .map(drop)
                                .map_err(|e| connect::map_sqlx(spec, e, started.elapsed()))
                        })
                        .await
                    }
                    Pool::Mysql(p) => {
                        connect::establish(spec, async {
                            p.acquire()
                                .await
                                .map(drop)
                                .map_err(|e| connect::map_sqlx(spec, e, started.elapsed()))
                        })
                        .await
                    }
                    Pool::Sqlite(_) => Ok(()),
                    #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
                    Pool::Native(p) => {
                        connect::establish(spec, async {
                            p.acquire()
                                .await
                                .map(drop)
                                .map_err(|e| connect::map_text(spec, e, started.elapsed()))
                        })
                        .await
                    }
                }
            })
            .await
            .map(|_| ());
        if let Err(e) = &result {
            gate.failures.record(e);
        }
        result
    }

    /// Build the pool without connecting (SQLAlchemy's lazy `create_engine`): the first query
    /// opens the first connection. Must be called within a Tokio runtime.
    pub fn connect_lazy(spec: &ConnectionSpec) -> Result<Self> {
        let max = spec.max_connections();
        let pool = match spec.flavor.driver_family() {
            DriverFamily::Postgres => Pool::Postgres(
                ping_idle_connections(sqlx::postgres::PgPoolOptions::new().max_connections(max))
                    .connect_lazy_with(postgres::connect_options(spec)?),
            ),
            DriverFamily::Mysql => Pool::Mysql(
                ping_idle_connections(sqlx::mysql::MySqlPoolOptions::new().max_connections(max))
                    .connect_lazy_with(mysql::connect_options(spec)?),
            ),
            DriverFamily::Sqlite => Pool::Sqlite(
                sqlite::pool_options(spec).connect_lazy_with(sqlite::connect_options(spec)?),
            ),
            DriverFamily::Mssql | DriverFamily::Oracle | DriverFamily::Duckdb => {
                connect_native_lazy(spec)?
            }
        };
        Ok(Self::new(spec, pool))
    }

    /// A SQLite-family diagnostic pool: never create the file, switch journal mode or write.
    /// Must be called within a Tokio runtime; other driver families are refused.
    pub fn connect_lazy_sqlite_read_only(spec: &ConnectionSpec) -> Result<Self> {
        if spec.flavor.driver_family() != DriverFamily::Sqlite {
            return Err(crate::SqlError::InvalidArgument(
                "A read-only SQLite probe requires sqlite or geopackage".into(),
            ));
        }
        Ok(Self::new(
            spec,
            Pool::Sqlite(
                sqlite::pool_options(spec)
                    .connect_lazy_with(sqlite::read_only_connect_options(spec)?),
            ),
        ))
    }

    /// The flavor this driver serves.
    pub fn flavor(&self) -> Flavor {
        self.flavor
    }

    /// Execute a query and stream its rows as Arrow record batches of at most `chunksize` rows.
    ///
    /// PostgreSQL and MySQL use the binary protocol (an unnamed, non-persistent prepared
    /// statement), so `sql` must be a single statement. The schema is fixed by the first batch
    /// (or by describing the statement when it returns no rows).
    pub async fn read_stream(&self, sql: &str, chunksize: usize) -> Result<RecordBatchStream> {
        self.read_stream_as(sql, chunksize, false, false).await
    }

    /// Whether catalog statements are prepared once per connection on this driver: the
    /// PostgreSQL family, where introspection queries over `information_schema` cost a few
    /// milliseconds of planning each (see [`SqlDriver::read_frame_catalog`]).
    fn catalog_persistent(&self) -> bool {
        matches!(self.pool, Pool::Postgres(_))
    }

    async fn read_stream_as(
        &self,
        sql: &str,
        chunksize: usize,
        persistent: bool,
        collect: bool,
    ) -> Result<RecordBatchStream> {
        debug_sql!(sql);
        if let Some(authorization_reads::Output::Batches(batches)) = Box::pin(
            authorization_reads::read(self, sql, &[], authorization_reads::Mode::Batches),
        )
        .await?
        {
            let schema = batches
                .first()
                .map(RecordBatch::schema)
                .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
            return Ok(RecordBatchStream::new(
                schema,
                futures_util::stream::iter(batches.into_iter().map(Ok)).boxed(),
            ));
        }
        self.ready().await?;
        let chunksize = chunksize.max(1);
        let sql = sql.to_string();
        on_native!(self.pool, p => p.read_stream(sql, chunksize, collect).await);
        on_pool!(self.pool, p, B => backend::read_stream::<B>(p, sql, chunksize, persistent).await)
    }

    /// Execute a query and stream its rows in the batches a collected read
    /// ([`read_frame`](SqlDriver::read_frame)) builds: [`DEFAULT_CHUNKSIZE`] rows, typed from
    /// the first batch, so the batches equal the collected read's. A pooled connection lost
    /// before the first batch is retried once, as a collected read is.
    ///
    /// Unconstrained Oracle `NUMBER` columns are exact decimal text here (see the native
    /// driver's `read_stream`); a caller that needs the collected read's typing reads it whole.
    pub async fn read_stream_batches(&self, sql: &str) -> Result<RecordBatchStream> {
        match self
            .read_stream_as(sql, DEFAULT_CHUNKSIZE, false, false)
            .await
        {
            Err(e) if self.retry_after(&e) => {
                self.evict_dead_connections().await;
                self.read_stream_as(sql, DEFAULT_CHUNKSIZE, false, false)
                    .await
            }
            other => other,
        }
    }

    /// Execute a query and collect all rows (in batches of [`DEFAULT_CHUNKSIZE`]).
    pub async fn read(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        Ok(self.read_collected(sql, false).await?.1)
    }

    /// Run a read to the end (schema and every batch). A pooled connection the server closed in
    /// the moments a connection is trusted without a ping ([`PING_IDLE_AFTER`]) fails the first
    /// attempt with a lost-connection error: dead idle connections are dropped and the read is
    /// tried once more on a live one (a read has no side effects to repeat).
    async fn read_collected(
        &self,
        sql: &str,
        persistent: bool,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<RecordBatch>)> {
        match self.read_collected_once(sql, persistent).await {
            Err(e) if self.retry_after(&e) => {
                self.evict_dead_connections().await;
                self.read_collected_once(sql, persistent).await
            }
            other => other,
        }
    }

    async fn read_collected_once(
        &self,
        sql: &str,
        persistent: bool,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<RecordBatch>)> {
        let stream = self
            .read_stream_as(sql, DEFAULT_CHUNKSIZE, persistent, true)
            .await?;
        let schema = stream.schema();
        Ok((schema, stream.collect_batches().await?))
    }

    /// Whether a failed read should be retried once: the connection was lost, and this pool
    /// trusts recently used connections without pinging them.
    fn retry_after(&self, error: &crate::error::SqlError) -> bool {
        matches!(self.pool, Pool::Postgres(_) | Pool::Mysql(_)) && is_lost_connection(error)
    }

    /// Drop the idle connections the server closed: ping what the pool hands out (the
    /// pool's own hook skips the ping for a connection used a moment ago) until one answers.
    async fn evict_dead_connections(&self) {
        async fn evict<DB: sqlx::Database>(pool: &sqlx::Pool<DB>) {
            for _ in 0..16 {
                let Ok(mut conn) = pool.acquire().await else {
                    return;
                };
                if sqlx::Connection::ping(&mut *conn).await.is_ok() {
                    return;
                }
                // Leaves the pool and closes when dropped.
                drop(conn.detach());
            }
        }
        match &self.pool {
            Pool::Postgres(p) => evict(p).await,
            Pool::Mysql(p) => evict(p).await,
            _ => {}
        }
    }

    fn scoped_write_unavailable(&self) -> crate::SqlError {
        crate::SqlError::InvalidArgument(format!(
            "Authorized native writes after database lock acquisition are not yet supported for '{}'.",
            self.flavor.as_str()
        ))
    }

    async fn scoped_write_transaction(
        &self,
        check: std::sync::Arc<dyn meerschaum_connector::mutation::MutationCheck>,
    ) -> Result<SqlTransaction> {
        Box::pin(self.scoped_write_transaction_inner(check)).await
    }

    async fn scoped_write_transaction_inner(
        &self,
        check: std::sync::Arc<dyn meerschaum_connector::mutation::MutationCheck>,
    ) -> Result<SqlTransaction> {
        if check.direct_administrator()
            && let Some(pool) = &self.admin_postgres_pool
        {
            self.ready().await?;
            // The administrator's native transaction checks the original authority
            // before writing and again after the statements, while their SQL locks
            // are held. Delegated calls still use the explicit backend adapters.
            let mut transaction = SqlTransaction {
                authorization_pin: None,
                authorization_read: None,
                inner: TxInner::Postgres(pool.begin().await?),
                mutation_check: Some(check),
                write_lock: true,
                refused: false,
            };
            if let Err(error) = transaction.check_write().await {
                Box::pin(transaction.rollback()).await?;
                return Err(error);
            }
            return Ok(transaction);
        }
        let pool = self
            .scoped_sqlite_pool
            .as_ref()
            .ok_or_else(|| self.scoped_write_unavailable())?;
        // Acquire the SQLite write lock before reconstructing caller/ACL authority. The
        // separate, bounded writer pool leaves canonical read capacity even with pool_size=1.
        let mut transaction = SqlTransaction {
            authorization_pin: None,
            authorization_read: None,
            inner: TxInner::Sqlite(pool.begin_with("BEGIN IMMEDIATE").await?),
            mutation_check: Some(check),
            write_lock: true,
            refused: false,
        };
        if transaction
            .mutation_check
            .as_ref()
            .is_some_and(|check| check.lock_snapshot_reads())
        {
            let TxInner::Sqlite(locked) = &mut transaction.inner else {
                unreachable!()
            };
            let file = match Box::pin(authorization_reads::identity(locked, true)).await {
                Ok(file) => file,
                Err(error) => {
                    Box::pin(transaction.rollback()).await?;
                    return Err(error);
                }
            };
            transaction.authorization_pin = Some(authorization_reads::Owner::new(file));
        }
        if let Err(error) = transaction.check_write().await {
            Box::pin(transaction.rollback()).await?;
            return Err(error);
        }
        Ok(transaction)
    }

    /// Execute one or more statements (simple-query protocol, like SQLAlchemy `text()`),
    /// returning the number of rows affected.
    pub async fn exec(&self, sql: &str) -> Result<u64> {
        if let Some(check) = meerschaum_connector::mutation::current() {
            let mut transaction = self.scoped_write_transaction(check).await?;
            let result = match transaction.exec(sql).await {
                Ok(result) => result,
                Err(error) => {
                    Box::pin(transaction.rollback()).await?;
                    return Err(error);
                }
            };
            transaction.commit().await?;
            return Ok(result);
        }
        debug_sql!(sql);
        self.ready().await?;
        let sql = sql.to_string();
        on_native!(self.pool, p => p.exec(&sql).await);
        on_pool!(self.pool, p, B => backend::exec::<B>(&p, &sql).await)
    }

    /// Execute a query and collect the result as a kernel [`Frame`] (the in-memory table type
    /// shared by every crate). Special columns are decoded from their field tags, and every
    /// column keeps its `meerschaum:dtype` / `meerschaum:db_type` tags, so
    /// `meerschaum_kernel::enforce::enforce_dtypes` needs no dtype detection on them.
    pub async fn read_docs_frame(&self, sql: &str) -> Result<Frame> {
        if let Some(authorization_reads::Output::Docs(frame)) = Box::pin(authorization_reads::read(
            self,
            sql,
            &[],
            authorization_reads::Mode::Docs,
        ))
        .await?
        {
            return Ok(frame);
        }
        // PARITY: v4 get_docs uses result.mappings(): SQLite timestamps remain raw text.
        if let Pool::Sqlite(pool) = &self.pool {
            debug_sql!(sql);
            return sqlite::read_docs_frame(pool, sql).await;
        }
        self.read_frame(sql).await
    }

    /// Read typed query results as a kernel frame.
    pub async fn read_frame(&self, sql: &str) -> Result<Frame> {
        self.read_frame_as(sql, false).await
    }

    /// [`read_frame`](SqlDriver::read_frame) for a catalog (introspection) query whose text
    /// repeats: on PostgreSQL the statement is prepared once per pooled connection and its
    /// plan reused.
    ///
    /// PARITY: v4's psycopg 3 prepares any statement a connection runs five times
    /// (`prepare_threshold`), which is what keeps its repeated `information_schema` queries
    /// cheap. Only queries with explicit result columns may use this (a cached `SELECT *` plan
    /// fails once the table's columns change).
    pub async fn read_frame_catalog(&self, sql: &str) -> Result<Frame> {
        let persistent = self.catalog_persistent();
        self.read_frame_as(sql, persistent).await
    }

    async fn read_frame_as(&self, sql: &str, persistent: bool) -> Result<Frame> {
        let (schema, batches) = self.read_collected(sql, persistent).await?;
        if batches.is_empty() {
            return batches_to_frame(&[RecordBatch::new_empty(schema)]);
        }
        batches_to_frame(&batches)
    }

    /// Execute a list of queries in a single transaction (`SQLConnector.exec_queries`,
    /// `_sql.py:622`).
    ///
    /// Each entry is `Some(rows_affected)` or `None` when that query failed. With
    /// `break_on_error`, execution stops after the first failure (its `None` is included) and
    /// the transaction is rolled back if `rollback`; otherwise the transaction is committed at
    /// the end. Without `break_on_error`, every query is attempted (on PostgreSQL a failure
    /// aborts the transaction, so later queries fail too, exactly as in v4).
    pub async fn exec_queries(
        &self,
        queries: &[String],
        break_on_error: bool,
        rollback: bool,
    ) -> Result<Vec<Option<u64>>> {
        if let Some(check) = meerschaum_connector::mutation::current() {
            let mut transaction = self.scoped_write_transaction(check).await?;
            let mut results = Vec::with_capacity(queries.len());
            for query in queries {
                match transaction.exec(query).await {
                    Ok(count) => results.push(Some(count)),
                    Err(error) => {
                        if transaction.refused {
                            Box::pin(transaction.rollback()).await?;
                            return Err(error);
                        }
                        results.push(None);
                        if break_on_error {
                            if rollback {
                                Box::pin(transaction.rollback()).await?;
                                return Ok(results);
                            }
                            break;
                        }
                    }
                }
            }
            transaction.commit().await?;
            return Ok(results);
        }
        queries.iter().for_each(|q| debug_sql!(q));
        self.ready().await?;
        let queries = queries.to_vec();
        on_native!(self.pool, p => p.exec_queries(&queries, break_on_error, rollback).await);
        on_pool!(self.pool, p, B, M => M::exec_queries(&p, &queries, break_on_error, rollback).await)
    }

    /// Execute statements outside any transaction block, stopping at the first failure
    /// (`SQLConnector._run_in_autocommit`, `_maintenance.py:54`; required for `VACUUM`).
    /// Returns whether every statement succeeded.
    pub async fn exec_autocommit(&self, queries: &[String]) -> Result<bool> {
        if meerschaum_connector::mutation::current().is_some() {
            return Err(self.scoped_write_unavailable());
        }
        queries.iter().for_each(|q| debug_sql!(q));
        self.ready().await?;
        let queries = queries.to_vec();
        on_native!(self.pool, p => p.exec_autocommit(&queries).await);
        on_pool!(self.pool, p, B, M => M::exec_autocommit(&p, &queries).await)
    }

    /// Return the first column of the first row (`SQLConnector.value`).
    ///
    /// `Ok(None)` when the query returns no rows; `Ok(Some(SqlValue::Null))` for a `NULL`.
    pub async fn value(&self, sql: &str) -> Result<Option<SqlValue>> {
        self.value_as(sql, false).await
    }

    /// [`value`](SqlDriver::value) for a catalog query (see
    /// [`read_frame_catalog`](SqlDriver::read_frame_catalog)).
    pub async fn value_catalog(&self, sql: &str) -> Result<Option<SqlValue>> {
        let persistent = self.catalog_persistent();
        self.value_as(sql, persistent).await
    }

    async fn value_as(&self, sql: &str, persistent: bool) -> Result<Option<SqlValue>> {
        match self.value_once(sql, persistent).await {
            Err(e) if self.retry_after(&e) => {
                self.evict_dead_connections().await;
                self.value_once(sql, persistent).await
            }
            other => other,
        }
    }

    async fn value_once(&self, sql: &str, persistent: bool) -> Result<Option<SqlValue>> {
        debug_sql!(sql);
        if let Some(authorization_reads::Output::Value(value)) = Box::pin(
            authorization_reads::read(self, sql, &[], authorization_reads::Mode::Value),
        )
        .await?
        {
            return Ok(value);
        }
        self.ready().await?;
        let sql = sql.to_string();
        on_native!(self.pool, p => p.value_with(&sql, &[]).await);
        let drain = self.drains_returning(&sql);
        on_pool!(self.pool, p, B => backend::value::<B>(&p, &sql, persistent, drain).await)
    }

    /// Execute one statement with bound parameters, returning the rows affected.
    pub async fn exec_with(&self, sql: &str, params: &[SqlParam]) -> Result<u64> {
        if let Some(check) = meerschaum_connector::mutation::current() {
            let mut transaction = self.scoped_write_transaction(check).await?;
            let result = match transaction.raw_execute(sql, params).await {
                Ok(result) => result,
                Err(error) => {
                    Box::pin(transaction.rollback()).await?;
                    return Err(error);
                }
            };
            transaction.commit().await?;
            return Ok(result);
        }
        debug_sql!(sql, params);
        self.ready().await?;
        let (sql, params) = (sql.to_string(), params.to_vec());
        on_native!(self.pool, p => p.exec_with(&sql, &params).await);
        on_pool!(self.pool, p, B => backend::exec_with::<B>(&p, &sql, &params).await)
    }

    /// Execute bound statements in one transaction, stopping and rolling back at the first
    /// failure (`None` marks the failed statement).
    pub async fn exec_many_with(
        &self,
        statements: &[(String, Vec<SqlParam>)],
    ) -> Result<Vec<Option<u64>>> {
        if let Some(check) = meerschaum_connector::mutation::current() {
            let mut transaction = self.scoped_write_transaction(check).await?;
            let mut results = Vec::with_capacity(statements.len());
            for (query, params) in statements {
                match transaction.raw_execute(query, params).await {
                    Ok(count) => results.push(Some(count)),
                    Err(error) => {
                        Box::pin(transaction.rollback()).await?;
                        return Err(error);
                    }
                }
            }
            transaction.commit().await?;
            return Ok(results);
        }
        statements.iter().for_each(|(q, p)| debug_sql!(q, p));
        self.ready().await?;
        let statements = statements.to_vec();
        on_native!(self.pool, p => p.exec_many_with(&statements).await);
        on_pool!(self.pool, p, B, M => M::exec_many_with(&p, &statements).await)
    }

    /// Run a query with bound parameters and collect the result as a kernel [`Frame`].
    pub async fn read_frame_with(&self, sql: &str, params: &[SqlParam]) -> Result<Frame> {
        let batches = self.read_with_as(sql, params, false).await?;
        batches_to_frame(&batches)
    }

    /// [`read_frame_with`](SqlDriver::read_frame_with) for a catalog query (see
    /// [`read_frame_catalog`](SqlDriver::read_frame_catalog)).
    pub async fn read_frame_with_catalog(&self, sql: &str, params: &[SqlParam]) -> Result<Frame> {
        let persistent = self.catalog_persistent();
        let batches = self.read_with_as(sql, params, persistent).await?;
        batches_to_frame(&batches)
    }

    /// Run a query with bound parameters and collect its rows as record batches.
    pub async fn read_with(&self, sql: &str, params: &[SqlParam]) -> Result<Vec<RecordBatch>> {
        self.read_with_as(sql, params, false).await
    }

    async fn read_with_as(
        &self,
        sql: &str,
        params: &[SqlParam],
        persistent: bool,
    ) -> Result<Vec<RecordBatch>> {
        match self.read_with_once(sql, params, persistent).await {
            Err(e) if self.retry_after(&e) => {
                self.evict_dead_connections().await;
                self.read_with_once(sql, params, persistent).await
            }
            other => other,
        }
    }

    async fn read_with_once(
        &self,
        sql: &str,
        params: &[SqlParam],
        persistent: bool,
    ) -> Result<Vec<RecordBatch>> {
        debug_sql!(sql, params);
        if let Some(authorization_reads::Output::Batches(batches)) = Box::pin(
            authorization_reads::read(self, sql, params, authorization_reads::Mode::Batches),
        )
        .await?
        {
            return Ok(batches);
        }
        self.ready().await?;
        let (sql, params) = (sql.to_string(), params.to_vec());
        on_native!(self.pool, p => p.read_with(&sql, &params).await);
        on_pool!(self.pool, p, B => backend::read_with::<B>(&p, &sql, &params, persistent).await)
    }

    /// Whether `value` reads a statement to its end: SQLite commits a write with `RETURNING`
    /// only when it finishes, and a commit refused there is reported by the last step only.
    fn drains_returning(&self, sql: &str) -> bool {
        matches!(self.pool, Pool::Sqlite(_)) && raw::has_returning(sql)
    }

    /// The first column of the first row of a query with bound parameters.
    pub async fn value_with(&self, sql: &str, params: &[SqlParam]) -> Result<Option<SqlValue>> {
        self.value_with_as(sql, params, false).await
    }

    /// [`value_with`](SqlDriver::value_with) for a catalog query (see
    /// [`read_frame_catalog`](SqlDriver::read_frame_catalog)).
    pub async fn value_with_catalog(
        &self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<Option<SqlValue>> {
        let persistent = self.catalog_persistent();
        self.value_with_as(sql, params, persistent).await
    }

    async fn value_with_as(
        &self,
        sql: &str,
        params: &[SqlParam],
        persistent: bool,
    ) -> Result<Option<SqlValue>> {
        match self.value_with_once(sql, params, persistent).await {
            Err(e) if self.retry_after(&e) => {
                self.evict_dead_connections().await;
                self.value_with_once(sql, params, persistent).await
            }
            other => other,
        }
    }

    async fn value_with_once(
        &self,
        sql: &str,
        params: &[SqlParam],
        persistent: bool,
    ) -> Result<Option<SqlValue>> {
        debug_sql!(sql, params);
        if let Some(authorization_reads::Output::Value(value)) = Box::pin(
            authorization_reads::read(self, sql, params, authorization_reads::Mode::Value),
        )
        .await?
        {
            return Ok(value);
        }
        self.ready().await?;
        let (sql, params) = (sql.to_string(), params.to_vec());
        on_native!(self.pool, p => p.value_with(&sql, &params).await);
        let drain = self.drains_returning(&sql);
        on_pool!(self.pool, p, B => {
            backend::value_with::<B>(&p, &sql, &params, persistent, drain).await
        })
    }

    /// PostgreSQL `COPY ... FROM STDIN` with a pre-encoded payload, in its own transaction
    /// (`psql_insert_copy`, `_sql.py:1109`). Returns the number of rows copied.
    pub async fn copy_in(&self, copy_sql: &str, data: Vec<u8>) -> Result<u64> {
        self.copy_in_after(&[], copy_sql, data)
            .await
            .map(|(_, count)| count)
    }

    /// [`copy_in`](SqlDriver::copy_in) after `prelude` statements, all in one transaction (a
    /// replacing sync deletes its window and copies the new rows atomically). Returns the
    /// prelude statements' row counts and the number of rows copied; any failure rolls back.
    pub async fn copy_in_after(
        &self,
        prelude: &[String],
        copy_sql: &str,
        data: Vec<u8>,
    ) -> Result<(Vec<u64>, u64)> {
        if !matches!(self.pool, Pool::Postgres(_)) {
            return Err(crate::SqlError::InvalidArgument(format!(
                "COPY is only available on PostgreSQL (flavor '{}').",
                self.flavor.as_str()
            )));
        }
        if meerschaum_connector::mutation::current()
            .is_some_and(|check| !check.direct_administrator())
        {
            return Err(self.scoped_write_unavailable());
        }
        meerschaum_core::dprint!(
            "{}\n({} bytes)",
            meerschaum_core::debug::redact_sql(copy_sql),
            data.len()
        );
        let mut transaction = self.begin_write().await?;
        let result = async {
            let mut counts = Vec::with_capacity(prelude.len());
            for sql in prelude {
                counts.push(transaction.exec(sql).await?);
            }
            transaction.check_write().await?;
            let TxInner::Postgres(native) = &mut transaction.inner else {
                unreachable!("PostgreSQL COPY requires a PostgreSQL connection");
            };
            let mut copy = native.copy_in_raw(copy_sql).await?;
            if let Err(error) = copy.send(data).await {
                let _ = copy.abort(error.to_string()).await;
                return Err(crate::SqlError::from(error));
            }
            Ok((counts, copy.finish().await?))
        }
        .await;
        match result {
            Ok(counts) => {
                transaction.commit().await?;
                Ok(counts)
            }
            Err(error) => {
                Box::pin(transaction.rollback()).await?;
                Err(error)
            }
        }
    }

    /// Begin a transaction on a pooled connection.
    pub async fn begin(&self) -> Result<SqlTransaction> {
        if let Some(check) = meerschaum_connector::mutation::current() {
            return self.scoped_write_transaction(check).await;
        }
        self.ready().await?;
        let inner = match &self.pool {
            Pool::Postgres(p) => TxInner::Postgres(p.begin().await?),
            Pool::Mysql(p) => TxInner::Mysql(p.begin().await?, None),
            Pool::Sqlite(p) => TxInner::Sqlite(p.begin().await?),
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            Pool::Native(p) => TxInner::Native(p.begin().await?),
        };
        Ok(SqlTransaction {
            authorization_pin: None,
            authorization_read: None,
            inner,
            mutation_check: None,
            write_lock: false,
            refused: false,
        })
    }

    /// [`SqlDriver::begin`] for a transaction that is going to write. On SQLite the
    /// transaction takes the write lock up front (`BEGIN IMMEDIATE`, waiting for it up to the
    /// busy timeout). A deferred `BEGIN` that reads first and writes later holds a read lock when it
    /// asks for the write lock: if another connection holds it, SQLite fails that request with
    /// `SQLITE_BUSY` at once, without waiting, because waiting could deadlock the two. Every other
    /// driver begins as usual.
    pub async fn begin_write(&self) -> Result<SqlTransaction> {
        if let Some(check) = meerschaum_connector::mutation::current() {
            return self.scoped_write_transaction(check).await;
        }
        match &self.pool {
            Pool::Sqlite(p) => {
                self.ready().await?;
                Ok(SqlTransaction {
                    authorization_pin: None,
                    authorization_read: None,
                    inner: TxInner::Sqlite(p.begin_with("BEGIN IMMEDIATE").await?),
                    mutation_check: None,
                    write_lock: false,
                    refused: false,
                })
            }
            _ => self.begin().await,
        }
    }

    /// [`SqlDriver::begin`] for a transaction that must not write: `BEGIN READ ONLY` on the
    /// PostgreSQL family and `START TRANSACTION READ ONLY` on MySQL / MariaDB refuse its writes.
    /// SQLite, MSSQL and DuckDB have no read-only transactions, and Oracle's (`SET TRANSACTION
    /// READ ONLY`) cannot read a table altered in the last few seconds (`ORA-01466`): the caller
    /// rolls this one back, which undoes their writes (DDL included, except on Oracle, where
    /// [`crate::readonly::is_read_statement`] keeps DDL out). See [`crate::readonly`].
    pub async fn begin_read_only(&self) -> Result<SqlTransaction> {
        self.ready().await?;
        let inner = match &self.pool {
            Pool::Postgres(p) => TxInner::Postgres(p.begin_with("BEGIN READ ONLY").await?),
            Pool::Mysql(p) => {
                TxInner::Mysql(p.begin_with("START TRANSACTION READ ONLY").await?, None)
            }
            Pool::Sqlite(p) => TxInner::Sqlite(p.begin().await?),
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            Pool::Native(p) => TxInner::Native(p.begin().await?),
        };
        Ok(SqlTransaction {
            authorization_pin: None,
            authorization_read: None,
            inner,
            mutation_check: meerschaum_connector::mutation::current(),
            write_lock: false,
            refused: false,
        })
    }

    /// Begin a file-backed SQLite source snapshot on a dedicated read-only pool with
    /// one-row prefetch. Never borrow the ordinary 8192-row query queue for page capture.
    pub async fn begin_source_snapshot(&self) -> Result<SqlTransaction> {
        let read = Box::pin(authorization_reads::route(self)).await?;
        if let Some(handle) = &read
            && handle.frozen()
        {
            return Ok(SqlTransaction {
                authorization_pin: None,
                authorization_read: read,
                inner: TxInner::PinnedRead,
                mutation_check: None,
                write_lock: false,
                refused: false,
            });
        }
        let pool = self.snapshot_sqlite_pool.as_ref().ok_or_else(|| {
            crate::SqlError::InvalidArgument(
                "Bounded source snapshots require a file-backed SQLite source.".into(),
            )
        })?;
        let mut transaction = source_snapshot::begin(pool).await?;
        if let Some(handle) = read {
            let TxInner::Sqlite(connection) = &mut transaction.inner else {
                unreachable!()
            };
            handle.verify_read(connection).await?;
            transaction.authorization_read = Some(handle);
        }
        Ok(transaction)
    }

    /// Retire only the source-read pool when cleanup of one of its VM handlers fails.
    pub async fn close_source_snapshots(&self) {
        if let Some(pool) = &self.snapshot_sqlite_pool {
            pool.close().await;
        }
    }

    /// Close every pooled connection.
    pub async fn close(&self) {
        if let Some(pool) = self.windowed_pool.get() {
            pool.close().await;
        }
        self.close_source_snapshots().await;
        if let Some(pool) = &self.scoped_sqlite_pool {
            pool.close().await;
        }
        if let Some(pool) = &self.admin_postgres_pool {
            pool.close().await;
        }
        match &self.pool {
            Pool::Postgres(p) => p.close().await,
            Pool::Mysql(p) => p.close().await,
            Pool::Sqlite(p) => p.close().await,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            Pool::Native(p) => p.close(),
        }
    }
}

#[cfg(test)]
mod initialization_tests {
    use super::*;

    #[test]
    fn session_lock_names_bind_the_database_schema_and_physical_table() {
        fn name(database: &str, schema: Option<&str>, table: &str, flavor: Flavor) -> String {
            session_initialization_lock_name(database, schema, table, flavor)
        }
        let mysql = name("owned", None, "mrsm_engine_metrics", Flavor::Mysql);
        assert_eq!(mysql.len(), 64);
        assert!(mysql.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(
            mysql,
            name("OWNED", None, "MRSM_ENGINE_METRICS", Flavor::Mariadb)
        );
        assert_ne!(
            mysql,
            name("other", None, "mrsm_engine_metrics", Flavor::Mysql)
        );
        assert_ne!(
            mysql,
            name("owned", None, "mrsm_engine_runs", Flavor::Mysql)
        );
        assert_ne!(
            name("owned", Some("a"), "table", Flavor::Mssql),
            name("owned", Some("b"), "table", Flavor::Mssql)
        );
        for flavor in [Flavor::Mysql, Flavor::Mariadb, Flavor::Mssql] {
            let long = "internal_".repeat(64);
            let physical = crate::ident::truncate_item_name(&long, flavor);
            assert_eq!(
                name("owned", None, &long, flavor),
                name("owned", None, &physical, flavor)
            );
        }
    }

    async fn session_test_driver(spec: &ConnectionSpec) -> SqlDriver {
        let driver = if spec.flavor.driver_family() == DriverFamily::Mysql {
            SqlDriver::new(
                spec,
                Pool::Mysql(
                    sqlx::mysql::MySqlPoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(std::time::Duration::from_secs(1))
                        .connect_lazy_with(mysql::connect_options(spec).unwrap()),
                ),
            )
        } else {
            SqlDriver::connect_lazy(spec).unwrap()
        };
        driver.ready().await.unwrap();
        driver
    }

    async fn session_internal_initialization_locks(variable: &str, flavor: Flavor) {
        let Ok(uri) = std::env::var(variable) else {
            return;
        };
        let mut spec = ConnectionSpec::from_uri(&uri, Some("internal_lock"), None).unwrap();
        spec.flavor = flavor;
        spec.pool_size = 1;
        spec.max_overflow = 0;
        spec.params.insert("connect_timeout".into(), "1".into());
        let driver = session_test_driver(&spec).await;
        let contender = session_test_driver(&spec).await;
        let target = format!("mrsm_init_{}", uuid::Uuid::new_v4().simple());
        let schema_sql = if flavor.driver_family() == DriverFamily::Mysql {
            "SELECT DATABASE()"
        } else {
            "SELECT SCHEMA_NAME()"
        };
        let schema = driver.value(schema_sql).await.unwrap().unwrap();
        let SqlValue::Text(schema) = schema else {
            panic!("expected a selected database/schema");
        };
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let holder_driver = driver.clone();
        let holder_target = target.clone();
        let holder = tokio::spawn(async move {
            let _guard = holder_driver
                .lock_internal_initialization(&holder_target, Some(&schema))
                .await
                .unwrap()
                .unwrap();
            entered_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        entered_rx.await.unwrap();
        // Exercise the driver's own bounded timeout, including equivalent default schema.
        let timed_out = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            contender.lock_internal_initialization(&target, None),
        )
        .await
        .expect("the configured acquisition deadline must be enforced");
        assert!(matches!(
            timed_out,
            Err(crate::SqlError::Driver(sqlx::Error::PoolTimedOut))
        ));
        // An independently cancelled waiter must not leave a session lock in either pool.
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(150),
                contender.lock_internal_initialization(&target, None),
            )
            .await
            .is_err()
        );
        let other = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            contender.lock_internal_initialization(&format!("{target}_other"), None),
        )
        .await
        .expect("different targets must initialize independently")
        .unwrap()
        .unwrap();
        assert_eq!(
            contender
                .value("SELECT 1")
                .await
                .unwrap()
                .and_then(|v| v.as_i64()),
            Some(1),
            "an initialization lock must not hold the only pool slot"
        );
        drop(other);
        holder.abort();
        assert!(holder.await.unwrap_err().is_cancelled());
        let guard = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            contender.lock_internal_initialization(&target, None),
        )
        .await
        .expect("a cancelled holder must close its dedicated session")
        .unwrap()
        .unwrap();
        drop(guard);
        let guard = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            driver.lock_internal_initialization(&target, None),
        )
        .await
        .expect("a dropped guard must not return a locked connection to a pool")
        .unwrap()
        .unwrap();
        drop(guard);
        driver.close().await;
        contender.close().await;
    }

    #[tokio::test]
    async fn mysql_internal_initialization_locks() {
        session_internal_initialization_locks("MRSM_TEST_MYSQL_URI", Flavor::Mysql).await;
    }

    #[tokio::test]
    async fn mariadb_internal_initialization_locks() {
        session_internal_initialization_locks("MRSM_TEST_MARIADB_URI", Flavor::Mariadb).await;
    }

    #[cfg(feature = "mssql")]
    #[tokio::test]
    async fn mssql_internal_initialization_locks() {
        session_internal_initialization_locks("MRSM_TEST_MSSQL_URI", Flavor::Mssql).await;
    }

    #[tokio::test]
    async fn cancelled_initializer_releases_database_lock() {
        let Ok(uri) = std::env::var("MRSM_TEST_PG_URI") else {
            return;
        };
        let spec = ConnectionSpec::from_uri(&uri, Some("w10_lock"), None).unwrap();
        let driver = SqlDriver::connect(&spec).await.unwrap();
        let target = format!("w10_lock_{}", uuid::Uuid::new_v4().simple());
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let holder_driver = driver.clone();
        let holder_target = target.clone();
        let holder = tokio::spawn(async move {
            let _guard = holder_driver
                .lock_internal_initialization(&holder_target, Some("public"))
                .await
                .unwrap()
                .unwrap();
            entered_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        entered_rx.await.unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(150),
                driver.lock_internal_initialization(&target, None),
            )
            .await
            .is_err(),
            "the unqualified target must share the public target's lock"
        );
        // A different physical target must initialize independently.
        let other = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            driver.lock_internal_initialization(&format!("{target}_other"), Some("public")),
        )
        .await
        .unwrap()
        .unwrap();
        drop(other);
        holder.abort();
        assert!(holder.await.unwrap_err().is_cancelled());
        // Cancellation of both a holder and a waiter leaves no pooled session lock.
        let guard = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            driver.lock_internal_initialization(&target, None),
        )
        .await
        .expect("cancellation must close the connection and release its lock")
        .unwrap();
        assert!(guard.is_some());
    }
}

#[cfg(test)]
mod future_size_tests {
    use super::*;

    /// `ready()` is awaited at the top of every driver method, so whatever state it holds is
    /// embedded in each of those futures and in every future above them. Unoptimized, an
    /// inline first-connection state machine took enough of the 2 MiB runtime worker stack to
    /// overflow it on a plain `sync pipes` (wave 18 regression): keep the slow path boxed.
    #[tokio::test]
    async fn ready_future_stays_small() {
        let attrs = crate::uri::ConnectorAttributes {
            flavor: Some(Flavor::Postgresql),
            host: Some("127.0.0.1".into()),
            port: Some(1),
            database: Some("w18fx1".into()),
            username: Some("u".into()),
            password: Some("p".into()),
            ..Default::default()
        };
        let spec = ConnectionSpec::from_attributes(&attrs).unwrap();
        // Lazy: nothing connects until the first operation.
        let driver = SqlDriver::connect_lazy(&spec).unwrap();
        let ready = std::mem::size_of_val(&driver.ready());
        let exec = std::mem::size_of_val(&driver.exec("SELECT 1"));
        eprintln!("ready={ready} exec={exec}");
        assert!(ready <= 128, "ready() future is {ready} bytes");
        assert!(exec <= 1024, "exec() future is {exec} bytes");
    }
}

#[cfg(test)]
mod mutation_authority_tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    struct OriginalAuthority {
        allowed: AtomicBool,
        checks: AtomicUsize,
        reader: SqlDriver,
        rolled_back: AtomicUsize,
        rollback_failed: AtomicUsize,
    }

    #[meerschaum_connector::async_trait]
    impl meerschaum_connector::mutation::MutationCheck for OriginalAuthority {
        fn rollback_completed(&self, succeeded: bool) {
            if succeeded {
                self.rolled_back.fetch_add(1, Ordering::SeqCst);
            } else {
                self.rollback_failed.fetch_add(1, Ordering::SeqCst);
            }
        }
        async fn check(&self) -> std::result::Result<(), String> {
            self.checks.fetch_add(1, Ordering::SeqCst);
            // A one-slot ordinary pool must remain usable for canonical authorization reads.
            self.reader
                .value("SELECT COUNT(*) FROM guarded_rows")
                .await
                .map_err(|error| error.to_string())?;
            if self.allowed.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err("The original caller's write permission was revoked.".into())
            }
        }
    }

    async fn fixture() -> (tempfile::TempDir, SqlDriver, Arc<OriginalAuthority>) {
        let root = tempfile::tempdir().unwrap();
        let mut spec = ConnectionSpec::from_uri(
            &format!("sqlite:///{}", root.path().join("owned.db").display()),
            Some("scoped_mutation"),
            None,
        )
        .unwrap();
        spec.pool_size = 1;
        spec.max_overflow = 0;
        let driver = SqlDriver::connect(&spec).await.unwrap();
        driver
            .exec("CREATE TABLE guarded_rows (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        let authority = Arc::new(OriginalAuthority {
            allowed: AtomicBool::new(true),
            checks: AtomicUsize::new(0),
            reader: driver.clone(),
            rolled_back: AtomicUsize::new(0),
            rollback_failed: AtomicUsize::new(0),
        });
        (root, driver, authority)
    }

    #[tokio::test]
    async fn sqlite_waiting_writer_rechecks_after_lock_and_releases_refused_transaction() {
        let (_root, driver, authority) = fixture().await;
        let held = driver.begin_write().await.unwrap();
        let contender = driver.clone();
        let check = authority.clone();
        let mut pending = tokio::spawn(async move {
            meerschaum_connector::mutation::scope(
                check,
                contender.exec_with("INSERT INTO guarded_rows VALUES (?)", &[SqlParam::Int(1)]),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            let pool = driver.scoped_sqlite_pool.as_ref().unwrap();
            while pool.size() != 1 || pool.num_idle() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut pending)
                .await
                .is_err()
        );
        assert_eq!(
            authority.checks.load(Ordering::SeqCst),
            0,
            "no stale pre-lock admission"
        );
        authority.allowed.store(false, Ordering::SeqCst);
        held.rollback().await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("revoked"), "{error}");
        assert_eq!(authority.rolled_back.load(Ordering::SeqCst), 1);
        assert_eq!(authority.rollback_failed.load(Ordering::SeqCst), 0);
        assert_eq!(
            driver
                .value("SELECT COUNT(*) FROM guarded_rows")
                .await
                .unwrap(),
            Some(SqlValue::Int(0))
        );
        // The refused transaction returned its writer slot and did not strand the lock.
        authority.allowed.store(true, Ordering::SeqCst);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            meerschaum_connector::mutation::scope(
                authority.clone(),
                driver.exec_with("INSERT INTO guarded_rows VALUES (?)", &[SqlParam::Int(2)]),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, 1);
        assert_eq!(
            driver
                .value("SELECT COUNT(*) FROM guarded_rows")
                .await
                .unwrap(),
            Some(SqlValue::Int(1))
        );
        driver.close().await;
    }

    #[tokio::test]
    async fn captured_transaction_authority_survives_scope_and_refusal_always_rolls_back() {
        let (_root, driver, authority) = fixture().await;
        let mut transaction =
            meerschaum_connector::mutation::scope(authority.clone(), driver.begin_write())
                .await
                .unwrap();
        transaction
            .exec("INSERT INTO guarded_rows VALUES (1)")
            .await
            .unwrap();
        authority.allowed.store(false, Ordering::SeqCst);
        let error = transaction
            .raw_execute("INSERT INTO guarded_rows VALUES (?)", &[SqlParam::Int(2)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("revoked"), "{error}");
        authority.allowed.store(true, Ordering::SeqCst);
        assert!(
            transaction.commit().await.is_err(),
            "a refused transaction cannot be revived"
        );
        assert_eq!(authority.rolled_back.load(Ordering::SeqCst), 1);
        assert_eq!(authority.rollback_failed.load(Ordering::SeqCst), 0);
        assert_eq!(
            driver
                .value("SELECT COUNT(*) FROM guarded_rows")
                .await
                .unwrap(),
            Some(SqlValue::Int(0))
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            meerschaum_connector::mutation::scope(
                authority,
                driver.exec("INSERT INTO guarded_rows VALUES (3)"),
            ),
        )
        .await
        .expect("the scoped writer must remain usable after a refused transaction")
        .unwrap();
        assert_eq!(
            driver
                .value("SELECT COUNT(*) FROM guarded_rows")
                .await
                .unwrap(),
            Some(SqlValue::Int(1))
        );
        driver.close().await;
    }

    #[tokio::test]
    async fn native_rollback_failure_is_reported_without_relabeling_it_as_authorization_cleanup() {
        let (_root, driver, authority) = fixture().await;
        let mut transaction =
            meerschaum_connector::mutation::scope(authority.clone(), driver.begin_write())
                .await
                .unwrap();
        transaction
            .exec("INSERT INTO guarded_rows VALUES (1)")
            .await
            .unwrap();
        // Deliberately end this fixture-owned native transaction behind SQLx's transaction
        // object. Its later explicit rollback must fail, providing a real database failure
        // rather than a mocked completion callback or a production fault-injection flag.
        transaction.exec("ROLLBACK").await.unwrap();
        authority.allowed.store(false, Ordering::SeqCst);
        let error = transaction.commit().await.unwrap_err();
        assert!(
            error.to_string().to_ascii_lowercase().contains("rollback"),
            "{error}"
        );
        assert_eq!(authority.rolled_back.load(Ordering::SeqCst), 0);
        assert_eq!(authority.rollback_failed.load(Ordering::SeqCst), 1);
        assert_eq!(
            driver
                .value("SELECT COUNT(*) FROM guarded_rows")
                .await
                .unwrap(),
            Some(SqlValue::Int(0))
        );
        authority.allowed.store(true, Ordering::SeqCst);
        tokio::time::timeout(
            Duration::from_secs(5),
            meerschaum_connector::mutation::scope(
                authority,
                driver.exec("INSERT INTO guarded_rows VALUES (3)"),
            ),
        )
        .await
        .expect("the scoped writer must replace a connection whose rollback failed")
        .unwrap();
        assert_eq!(
            driver
                .value("SELECT COUNT(*) FROM guarded_rows")
                .await
                .unwrap(),
            Some(SqlValue::Int(1))
        );
        driver.close().await;
    }
}
