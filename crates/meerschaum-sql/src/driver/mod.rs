//! The driver layer (D13): connection pools per flavor family, `read` as an Arrow record-batch
//! stream, `exec`, `exec_queries` and `value`.
//!
//! | Family | Flavors | Driver |
//! |---|---|---|
//! | PostgreSQL | `postgresql`, `timescaledb`, `timescaledb-ha`, `postgis`, `citus`, `cockroachdb` | `sqlx` |
//! | MySQL | `mysql`, `mariadb` | `sqlx` |
//! | SQLite | `sqlite`, `geopackage` | `sqlx` |
//! | MSSQL | `mssql` | `tiberius` — not implemented yet ([`SqlError::DriverUnavailable`]) |
//! | Oracle | `oracle` | `oracle` — not implemented yet |
//! | DuckDB | `duckdb` | `duckdb` — not implemented yet |
//!
//! Per D16 the Python SDK keeps SQLAlchemy for user SQL; these methods serve the Rust core and
//! Rust plugins.

mod backend;
pub mod batch;
mod mysql;
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
pub use params::SqlParam;

/// Python's default `chunksize` (`system.connectors.sql.chunksize`).
pub const DEFAULT_CHUNKSIZE: usize = 100_000;

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
}

impl fmt::Debug for SqlTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlTransaction").finish_non_exhaustive()
    }
}

impl SqlTransaction {
    /// Execute one or more statements, returning the rows affected.
    pub async fn exec(&mut self, sql: &str) -> Result<u64> {
        match &mut self.inner {
            TxInner::Postgres(tx) => backend::pg_tx::tx_exec(tx, sql).await,
            TxInner::Mysql(tx) => backend::mysql_tx::tx_exec(tx, sql).await,
            TxInner::Sqlite(tx) => backend::sqlite_tx::tx_exec(tx, sql).await,
        }
    }

    /// Run a query and collect the result as a kernel [`Frame`].
    pub async fn read_frame(&mut self, sql: &str) -> Result<Frame> {
        let batches = match &mut self.inner {
            TxInner::Postgres(tx) => backend::pg_tx::tx_read(tx, sql).await?,
            TxInner::Mysql(tx) => backend::mysql_tx::tx_read(tx, sql).await?,
            TxInner::Sqlite(tx) => backend::sqlite_tx::tx_read(tx, sql).await?,
        };
        batches_to_frame(&batches)
    }

    /// Commit the transaction.
    pub async fn commit(self) -> Result<()> {
        match self.inner {
            TxInner::Postgres(tx) => tx.commit().await?,
            TxInner::Mysql(tx) => tx.commit().await?,
            TxInner::Sqlite(tx) => tx.commit().await?,
        }
        Ok(())
    }

    /// Roll the transaction back.
    pub async fn rollback(self) -> Result<()> {
        match self.inner {
            TxInner::Postgres(tx) => tx.rollback().await?,
            TxInner::Mysql(tx) => tx.rollback().await?,
            TxInner::Sqlite(tx) => tx.rollback().await?,
        }
        Ok(())
    }
}

#[derive(Clone)]
enum Pool {
    Postgres(sqlx::PgPool),
    Mysql(sqlx::MySqlPool),
    Sqlite(sqlx::SqlitePool),
}

/// A pooled connection to one database.
#[derive(Clone)]
pub struct SqlDriver {
    flavor: Flavor,
    pool: Pool,
}

impl fmt::Debug for SqlDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlDriver")
            .field("flavor", &self.flavor)
            .finish_non_exhaustive()
    }
}

impl SqlDriver {
    /// Open a connection pool for a connection spec. Connections are opened lazily by the pool,
    /// except that this verifies one connection can be established.
    pub async fn connect(spec: &ConnectionSpec) -> Result<Self> {
        let pool = match spec.flavor.driver_family() {
            DriverFamily::Postgres => Pool::Postgres(postgres::connect(spec).await?),
            DriverFamily::Mysql => Pool::Mysql(mysql::connect(spec).await?),
            DriverFamily::Sqlite => Pool::Sqlite(sqlite::connect(spec).await?),
            DriverFamily::Mssql | DriverFamily::Oracle | DriverFamily::Duckdb => {
                return Err(unsupported::unavailable(spec.flavor));
            }
        };
        Ok(Self {
            flavor: spec.flavor,
            pool,
        })
    }

    /// Build the pool without connecting (SQLAlchemy's lazy `create_engine`): the first query
    /// opens the first connection. Must be called within a Tokio runtime.
    pub fn connect_lazy(spec: &ConnectionSpec) -> Result<Self> {
        let max = spec.max_connections();
        let pool = match spec.flavor.driver_family() {
            DriverFamily::Postgres => Pool::Postgres(
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(max)
                    .connect_lazy_with(postgres::connect_options(spec)?),
            ),
            DriverFamily::Mysql => Pool::Mysql(
                sqlx::mysql::MySqlPoolOptions::new()
                    .max_connections(max)
                    .connect_lazy_with(mysql::connect_options(spec)?),
            ),
            DriverFamily::Sqlite => Pool::Sqlite(
                sqlx::sqlite::SqlitePoolOptions::new()
                    .max_connections(max)
                    .connect_lazy_with(sqlite::connect_options(spec)?),
            ),
            DriverFamily::Mssql | DriverFamily::Oracle | DriverFamily::Duckdb => {
                return Err(unsupported::unavailable(spec.flavor));
            }
        };
        Ok(Self {
            flavor: spec.flavor,
            pool,
        })
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
        let chunksize = chunksize.max(1);
        let sql = sql.to_string();
        on_pool!(self.pool, p, B => backend::read_stream::<B>(p, sql, chunksize).await)
    }

    /// Execute a query and collect all rows (in batches of [`DEFAULT_CHUNKSIZE`]).
    pub async fn read(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        self.read_stream(sql, DEFAULT_CHUNKSIZE)
            .await?
            .collect_batches()
            .await
    }

    /// Execute one or more statements (simple-query protocol, like SQLAlchemy `text()`),
    /// returning the number of rows affected.
    pub async fn exec(&self, sql: &str) -> Result<u64> {
        let sql = sql.to_string();
        on_pool!(self.pool, p, B => backend::exec::<B>(&p, &sql).await)
    }

    /// Execute a query and collect the result as a kernel [`Frame`] (the in-memory table type
    /// shared by every crate). Special columns are decoded from their field tags, and every
    /// column keeps its `meerschaum:dtype` / `meerschaum:db_type` tags, so
    /// `meerschaum_kernel::enforce::enforce_dtypes` needs no dtype detection on them.
    pub async fn read_frame(&self, sql: &str) -> Result<Frame> {
        let stream = self.read_stream(sql, DEFAULT_CHUNKSIZE).await?;
        let schema = stream.schema();
        let batches = stream.collect_batches().await?;
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
        let queries = queries.to_vec();
        on_pool!(self.pool, p, B, M => M::exec_queries(&p, &queries, break_on_error, rollback).await)
    }

    /// Execute statements outside any transaction block, stopping at the first failure
    /// (`SQLConnector._run_in_autocommit`, `_maintenance.py:54`; required for `VACUUM`).
    /// Returns whether every statement succeeded.
    pub async fn exec_autocommit(&self, queries: &[String]) -> Result<bool> {
        let queries = queries.to_vec();
        on_pool!(self.pool, p, B, M => M::exec_autocommit(&p, &queries).await)
    }

    /// Return the first column of the first row (`SQLConnector.value`).
    ///
    /// `Ok(None)` when the query returns no rows; `Ok(Some(SqlValue::Null))` for a `NULL`.
    pub async fn value(&self, sql: &str) -> Result<Option<SqlValue>> {
        let sql = sql.to_string();
        on_pool!(self.pool, p, B => backend::value::<B>(&p, &sql).await)
    }

    /// Execute one statement with bound parameters, returning the rows affected.
    pub async fn exec_with(&self, sql: &str, params: &[SqlParam]) -> Result<u64> {
        let (sql, params) = (sql.to_string(), params.to_vec());
        on_pool!(self.pool, p, B => backend::exec_with::<B>(&p, &sql, &params).await)
    }

    /// Execute bound statements in one transaction, stopping and rolling back at the first
    /// failure (`None` marks the failed statement).
    pub async fn exec_many_with(
        &self,
        statements: &[(String, Vec<SqlParam>)],
    ) -> Result<Vec<Option<u64>>> {
        let statements = statements.to_vec();
        on_pool!(self.pool, p, B, M => M::exec_many_with(&p, &statements).await)
    }

    /// Run a query with bound parameters and collect the result as a kernel [`Frame`].
    pub async fn read_frame_with(&self, sql: &str, params: &[SqlParam]) -> Result<Frame> {
        let (sql, params) = (sql.to_string(), params.to_vec());
        let batches =
            on_pool!(self.pool, p, B => backend::read_with::<B>(&p, &sql, &params).await)?;
        batches_to_frame(&batches)
    }

    /// The first column of the first row of a query with bound parameters.
    pub async fn value_with(&self, sql: &str, params: &[SqlParam]) -> Result<Option<SqlValue>> {
        let (sql, params) = (sql.to_string(), params.to_vec());
        on_pool!(self.pool, p, B => backend::value_with::<B>(&p, &sql, &params).await)
    }

    /// PostgreSQL `COPY ... FROM STDIN` with a pre-encoded payload, in its own transaction
    /// (`psql_insert_copy`, `_sql.py:1109`). Returns the number of rows copied.
    pub async fn copy_in(&self, copy_sql: &str, data: Vec<u8>) -> Result<u64> {
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

    /// Begin a transaction on a pooled connection.
    pub async fn begin(&self) -> Result<SqlTransaction> {
        let inner = match &self.pool {
            Pool::Postgres(p) => TxInner::Postgres(p.begin().await?),
            Pool::Mysql(p) => TxInner::Mysql(p.begin().await?),
            Pool::Sqlite(p) => TxInner::Sqlite(p.begin().await?),
        };
        Ok(SqlTransaction { inner })
    }

    /// Close every pooled connection.
    pub async fn close(&self) {
        match &self.pool {
            Pool::Postgres(p) => p.close().await,
            Pool::Mysql(p) => p.close().await,
            Pool::Sqlite(p) => p.close().await,
        }
    }
}
