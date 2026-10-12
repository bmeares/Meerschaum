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
        match &self.pool {
            Pool::Postgres(p) => {
                backend::read_stream::<postgres::PgBackend>(p.clone(), sql.to_string(), chunksize)
                    .await
            }
            Pool::Mysql(p) => {
                backend::read_stream::<mysql::MySqlBackend>(p.clone(), sql.to_string(), chunksize)
                    .await
            }
            Pool::Sqlite(p) => {
                backend::read_stream::<sqlite::SqliteBackend>(p.clone(), sql.to_string(), chunksize)
                    .await
            }
        }
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
        match &self.pool {
            Pool::Postgres(p) => backend::exec::<postgres::PgBackend>(p, sql).await,
            Pool::Mysql(p) => backend::exec::<mysql::MySqlBackend>(p, sql).await,
            Pool::Sqlite(p) => backend::exec::<sqlite::SqliteBackend>(p, sql).await,
        }
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
        match &self.pool {
            Pool::Postgres(p) => {
                backend::exec_queries::<postgres::PgBackend>(p, queries, break_on_error, rollback)
                    .await
            }
            Pool::Mysql(p) => {
                backend::exec_queries::<mysql::MySqlBackend>(p, queries, break_on_error, rollback)
                    .await
            }
            Pool::Sqlite(p) => {
                backend::exec_queries::<sqlite::SqliteBackend>(p, queries, break_on_error, rollback)
                    .await
            }
        }
    }

    /// Execute statements outside any transaction block, stopping at the first failure
    /// (`SQLConnector._run_in_autocommit`, `_maintenance.py:54`; required for `VACUUM`).
    /// Returns whether every statement succeeded.
    pub async fn exec_autocommit(&self, queries: &[String]) -> Result<bool> {
        match &self.pool {
            Pool::Postgres(p) => backend::exec_autocommit::<postgres::PgBackend>(p, queries).await,
            Pool::Mysql(p) => backend::exec_autocommit::<mysql::MySqlBackend>(p, queries).await,
            Pool::Sqlite(p) => backend::exec_autocommit::<sqlite::SqliteBackend>(p, queries).await,
        }
    }

    /// Return the first column of the first row (`SQLConnector.value`).
    ///
    /// `Ok(None)` when the query returns no rows; `Ok(Some(SqlValue::Null))` for a `NULL`.
    pub async fn value(&self, sql: &str) -> Result<Option<SqlValue>> {
        match &self.pool {
            Pool::Postgres(p) => backend::value::<postgres::PgBackend>(p, sql).await,
            Pool::Mysql(p) => backend::value::<mysql::MySqlBackend>(p, sql).await,
            Pool::Sqlite(p) => backend::value::<sqlite::SqliteBackend>(p, sql).await,
        }
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
