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
//! Per D16 the Python SDK keeps SQLAlchemy for user SQL; these methods serve the Rust core and
//! Rust plugins.

mod backend;
pub mod batch;
pub mod connect;
#[cfg(feature = "duckdb")]
pub(crate) mod duckdb;
pub mod gpkg_functions;
#[cfg(feature = "mssql")]
mod mssql;
mod mysql;
#[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
mod native;
#[cfg(feature = "oracle")]
mod oracle;
pub mod params;
mod postgres;
mod sqlite;
mod unsupported;

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
    inner: TxInner,
}

enum TxInner {
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
    Mysql(sqlx::Transaction<'static, sqlx::MySql>),
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
    /// Execute one or more statements, returning the rows affected.
    pub async fn exec(&mut self, sql: &str) -> Result<u64> {
        debug_sql!(sql);
        match &mut self.inner {
            TxInner::Postgres(tx) => backend::pg_tx::tx_exec(tx, sql).await,
            TxInner::Mysql(tx) => backend::mysql_tx::tx_exec(tx, sql).await,
            TxInner::Sqlite(tx) => backend::sqlite_tx::tx_exec(tx, sql).await,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.exec(sql).await,
        }
    }

    /// Run a query and collect the result as a kernel [`Frame`].
    pub async fn read_frame(&mut self, sql: &str) -> Result<Frame> {
        debug_sql!(sql);
        let batches = match &mut self.inner {
            TxInner::Postgres(tx) => backend::pg_tx::tx_read(tx, sql).await?,
            TxInner::Mysql(tx) => backend::mysql_tx::tx_read(tx, sql).await?,
            TxInner::Sqlite(tx) => backend::sqlite_tx::tx_read(tx, sql).await?,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.read(sql).await?,
        };
        batches_to_frame(&batches)
    }

    /// Commit the transaction.
    pub async fn commit(self) -> Result<()> {
        match self.inner {
            TxInner::Postgres(tx) => tx.commit().await?,
            TxInner::Mysql(tx) => tx.commit().await?,
            TxInner::Sqlite(tx) => tx.commit().await?,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.commit().await?,
        }
        Ok(())
    }

    /// Roll the transaction back.
    pub async fn rollback(self) -> Result<()> {
        match self.inner {
            TxInner::Postgres(tx) => tx.rollback().await?,
            TxInner::Mysql(tx) => tx.rollback().await?,
            TxInner::Sqlite(tx) => tx.rollback().await?,
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(tx) => tx.rollback().await?,
        }
        Ok(())
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
    flavor: Flavor,
    pool: Pool,
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
    /// closes the socket and releases the transaction lock; no session lock enters the pool.
    pub(crate) async fn lock_internal_initialization(
        &self,
        target: &str,
        schema: Option<&str>,
    ) -> Result<Option<sqlx::PgConnection>> {
        use sha2::{Digest, Sha256};
        use sqlx::Connection;

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
        Ok(Some(connection))
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
        Self {
            flavor: spec.flavor,
            pool,
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
        self.ready().await?;
        let chunksize = chunksize.max(1);
        let sql = sql.to_string();
        on_native!(self.pool, p => p.read_stream(sql, chunksize, collect).await);
        on_pool!(self.pool, p, B => backend::read_stream::<B>(p, sql, chunksize, persistent).await)
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

    /// Execute one or more statements (simple-query protocol, like SQLAlchemy `text()`),
    /// returning the number of rows affected.
    pub async fn exec(&self, sql: &str) -> Result<u64> {
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
        self.ready().await?;
        let sql = sql.to_string();
        on_native!(self.pool, p => p.value_with(&sql, &[]).await);
        on_pool!(self.pool, p, B => backend::value::<B>(&p, &sql, persistent).await)
    }

    /// Execute one statement with bound parameters, returning the rows affected.
    pub async fn exec_with(&self, sql: &str, params: &[SqlParam]) -> Result<u64> {
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
        self.ready().await?;
        let (sql, params) = (sql.to_string(), params.to_vec());
        on_native!(self.pool, p => p.read_with(&sql, &params).await);
        on_pool!(self.pool, p, B => backend::read_with::<B>(&p, &sql, &params, persistent).await)
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
        self.ready().await?;
        let (sql, params) = (sql.to_string(), params.to_vec());
        on_native!(self.pool, p => p.value_with(&sql, &params).await);
        on_pool!(self.pool, p, B => backend::value_with::<B>(&p, &sql, &params, persistent).await)
    }

    /// PostgreSQL `COPY ... FROM STDIN` with a pre-encoded payload, in its own transaction
    /// (`psql_insert_copy`, `_sql.py:1109`). Returns the number of rows copied.
    pub async fn copy_in(&self, copy_sql: &str, data: Vec<u8>) -> Result<u64> {
        meerschaum_core::dprint!(
            "{}\n({} bytes)",
            meerschaum_core::debug::redact_sql(copy_sql),
            data.len()
        );
        self.ready().await?;
        match &self.pool {
            Pool::Postgres(p) => {
                let mut tx = p.begin().await?;
                let mut copy = tx.copy_in_raw(copy_sql).await?;
                if let Err(e) = copy.send(data).await {
                    let _ = copy.abort(e.to_string()).await;
                    return Err(e.into());
                }
                let n = copy.finish().await?;
                tx.commit().await?;
                Ok(n)
            }
            _ => Err(crate::error::SqlError::InvalidArgument(format!(
                "COPY is only available on PostgreSQL (flavor '{}').",
                self.flavor.as_str()
            ))),
        }
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
        prelude.iter().for_each(|q| debug_sql!(q));
        meerschaum_core::dprint!(
            "{}\n({} bytes)",
            meerschaum_core::debug::redact_sql(copy_sql),
            data.len()
        );
        self.ready().await?;
        match &self.pool {
            Pool::Postgres(p) => {
                let mut tx = p.begin().await?;
                let mut counts = Vec::with_capacity(prelude.len());
                for sql in prelude {
                    counts.push(backend::pg_tx::tx_exec(&mut tx, sql).await?);
                }
                let mut copy = tx.copy_in_raw(copy_sql).await?;
                if let Err(e) = copy.send(data).await {
                    let _ = copy.abort(e.to_string()).await;
                    return Err(e.into());
                }
                let n = copy.finish().await?;
                tx.commit().await?;
                Ok((counts, n))
            }
            _ => Err(crate::error::SqlError::InvalidArgument(format!(
                "COPY is only available on PostgreSQL (flavor '{}').",
                self.flavor.as_str()
            ))),
        }
    }

    /// Begin a transaction on a pooled connection.
    pub async fn begin(&self) -> Result<SqlTransaction> {
        self.ready().await?;
        let inner = match &self.pool {
            Pool::Postgres(p) => TxInner::Postgres(p.begin().await?),
            Pool::Mysql(p) => TxInner::Mysql(p.begin().await?),
            Pool::Sqlite(p) => TxInner::Sqlite(p.begin().await?),
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            Pool::Native(p) => TxInner::Native(p.begin().await?),
        };
        Ok(SqlTransaction { inner })
    }

    /// [`SqlDriver::begin`] for a transaction that is going to write. On SQLite the
    /// transaction takes the write lock up front (`BEGIN IMMEDIATE`, waiting for it up to the
    /// busy timeout). A deferred `BEGIN` that reads first and writes later holds a read lock when it
    /// asks for the write lock: if another connection holds it, SQLite fails that request with
    /// `SQLITE_BUSY` at once, without waiting, because waiting could deadlock the two. Every other
    /// driver begins as usual.
    pub async fn begin_write(&self) -> Result<SqlTransaction> {
        match &self.pool {
            Pool::Sqlite(p) => {
                self.ready().await?;
                Ok(SqlTransaction {
                    inner: TxInner::Sqlite(p.begin_with("BEGIN IMMEDIATE").await?),
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
            Pool::Mysql(p) => TxInner::Mysql(p.begin_with("START TRANSACTION READ ONLY").await?),
            Pool::Sqlite(p) => TxInner::Sqlite(p.begin().await?),
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            Pool::Native(p) => TxInner::Native(p.begin().await?),
        };
        Ok(SqlTransaction { inner })
    }

    /// Close every pooled connection.
    pub async fn close(&self) {
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
