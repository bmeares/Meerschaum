//! The driver abstraction for the families that do not go through `sqlx` (D13): `tiberius` for
//! MSSQL, the `duckdb` crate for DuckDB and the `oracle` crate for Oracle.
//!
//! Each family implements [`NativeConn`] (one open connection: execute, query into a channel,
//! transaction control) and [`NativeConnector`] (open a connection). This module supplies the
//! connection pool and the generic implementations of the [`SqlDriver`](super::SqlDriver)
//! operations (`read_stream`, `exec_queries`, `value`, transactions, ...) on top of them, with
//! the same semantics as the `sqlx` backends in `backend.rs`.

use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use futures_util::StreamExt;
use futures_util::stream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use super::batch::{self, ColumnPlan};
use super::params::SqlParam;
use super::{RecordBatchStream, SqlValue};
use crate::error::{Result, SqlError};

/// One event produced by [`NativeConn::query`]: the result's column plans (exactly once, before
/// any rows), then row-major chunks of cells.
#[derive(Debug)]
pub(crate) enum NativeEvent {
    Columns(Vec<ColumnPlan>),
    Rows(Vec<Vec<SqlValue>>),
}

/// One open connection of a native driver family.
#[async_trait]
pub(crate) trait NativeConn: Send + 'static {
    /// Execute one or more statements (several only without parameters), returning the rows
    /// affected.
    async fn execute(&mut self, sql: &str, params: &[SqlParam]) -> Result<u64>;

    /// Run a query, sending [`NativeEvent::Columns`] and then chunks of at most `chunksize`
    /// rows to `sink`. A closed `sink` stops the query early (not an error).
    async fn query(
        &mut self,
        sql: &str,
        params: &[SqlParam],
        chunksize: usize,
        sink: mpsc::Sender<NativeEvent>,
    ) -> Result<()>;

    /// Begin a transaction.
    async fn begin(&mut self) -> Result<()>;

    /// Commit the open transaction.
    async fn commit(&mut self) -> Result<()>;

    /// Roll the open transaction back.
    async fn rollback(&mut self) -> Result<()>;

    /// Whether the connection must be discarded instead of returned to the pool.
    fn is_broken(&self) -> bool {
        false
    }
}

/// Opens connections for a pool.
#[async_trait]
pub(crate) trait NativeConnector: Send + Sync + 'static {
    async fn connect(&self) -> Result<Box<dyn NativeConn>>;
}

struct PoolInner {
    connector: Box<dyn NativeConnector>,
    idle: Mutex<Vec<Box<dyn NativeConn>>>,
    sem: Arc<Semaphore>,
}

/// A connection pool of at most `max` connections, opened on demand.
#[derive(Clone)]
pub(crate) struct NativePool {
    inner: Arc<PoolInner>,
}

/// A connection checked out of a [`NativePool`]; returned on drop unless it is broken or still
/// inside a transaction (closing the connection rolls the transaction back server-side).
pub(crate) struct PooledConn {
    conn: Option<Box<dyn NativeConn>>,
    pool: Arc<PoolInner>,
    in_tx: bool,
    _permit: OwnedSemaphorePermit,
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if !self.in_tx && !conn.is_broken() {
                if let Ok(mut idle) = self.pool.idle.lock() {
                    idle.push(conn);
                }
            }
        }
    }
}

impl PooledConn {
    fn conn(&mut self) -> &mut Box<dyn NativeConn> {
        self.conn.as_mut().expect("pooled connection")
    }
}

fn task_err(e: impl std::fmt::Display) -> SqlError {
    SqlError::InvalidArgument(format!("The database task failed: {e}"))
}

impl NativePool {
    /// A pool over `connector` with at most `max` open connections.
    pub(crate) fn new(connector: Box<dyn NativeConnector>, max: u32) -> Self {
        NativePool {
            inner: Arc::new(PoolInner {
                connector,
                idle: Mutex::new(Vec::new()),
                sem: Arc::new(Semaphore::new(max.max(1) as usize)),
            }),
        }
    }

    /// Check out a connection, opening one when none is idle.
    pub(crate) async fn acquire(&self) -> Result<PooledConn> {
        let permit = self
            .inner
            .sem
            .clone()
            .acquire_owned()
            .await
            .map_err(task_err)?;
        let idle = self.inner.idle.lock().ok().and_then(|mut v| v.pop());
        let conn = match idle {
            Some(c) => c,
            None => self.inner.connector.connect().await?,
        };
        Ok(PooledConn {
            conn: Some(conn),
            pool: self.inner.clone(),
            in_tx: false,
            _permit: permit,
        })
    }

    /// Drop every idle connection.
    pub(crate) fn close(&self) {
        if let Ok(mut idle) = self.inner.idle.lock() {
            idle.clear();
        }
    }

    /// Stream a query's rows as record batches of at most `chunksize` rows.
    pub(crate) async fn read_stream(
        &self,
        sql: String,
        chunksize: usize,
    ) -> Result<RecordBatchStream> {
        let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(2);
        let (schema_tx, schema_rx) = oneshot::channel::<Result<SchemaRef>>();
        let pool = self.clone();
        tokio::spawn(async move {
            let mut schema_tx = Some(schema_tx);
            let mut conn = match pool.acquire().await {
                Ok(c) => c,
                Err(e) => {
                    let _ = schema_tx.take().map(|s| s.send(Err(e)));
                    return;
                }
            };
            let (etx, mut erx) = mpsc::channel::<NativeEvent>(2);
            let query = conn.conn().query(&sql, &[], chunksize, etx);
            let consume = async {
                let mut plans: Option<Vec<ColumnPlan>> = None;
                let mut schema: Option<SchemaRef> = None;
                while let Some(ev) = erx.recv().await {
                    match ev {
                        NativeEvent::Columns(p) => plans = Some(p),
                        NativeEvent::Rows(rows) => {
                            let Some(plans) = plans.as_mut() else {
                                continue;
                            };
                            let n = rows.len();
                            let cells = transpose(rows, plans.len());
                            if schema.is_none() {
                                batch::resolve_decimal_scales(plans, &cells);
                                schema = Some(batch::schema_for(plans));
                            }
                            let s = schema.clone().expect("schema");
                            let item = batch::build_batch(&s, plans, cells, n);
                            if let Some(stx) = schema_tx.take() {
                                let failed = item.is_err();
                                let _ = stx.send(item.as_ref().map(|b| b.schema()).map_err(|e| {
                                    SqlError::Conversion {
                                        column: String::new(),
                                        reason: e.to_string(),
                                    }
                                }));
                                if failed {
                                    return (plans.clone(), schema);
                                }
                            }
                            if tx.send(item).await.is_err() {
                                return (plans.clone(), schema);
                            }
                        }
                    }
                }
                (plans.unwrap_or_default(), schema)
            };
            let (result, (mut plans, schema)) = tokio::join!(query, consume);
            match result {
                Err(e) => match schema_tx.take() {
                    Some(stx) => {
                        let _ = stx.send(Err(e));
                    }
                    None => {
                        let _ = tx.send(Err(e)).await;
                    }
                },
                Ok(()) => {
                    if let Some(stx) = schema_tx.take() {
                        let schema = schema.unwrap_or_else(|| {
                            batch::resolve_decimal_scales(&mut plans, &[]);
                            batch::schema_for(&plans)
                        });
                        let _ = stx.send(Ok(schema));
                    }
                }
            }
        });
        let schema = schema_rx.await.map_err(|_| SqlError::Conversion {
            column: String::new(),
            reason: "the read task ended before producing a schema".to_string(),
        })??;
        let inner = stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })
        .boxed();
        Ok(RecordBatchStream::new(schema, inner))
    }

    /// Execute statements (no parameters).
    pub(crate) async fn exec(&self, sql: &str) -> Result<u64> {
        let mut conn = self.acquire().await?;
        conn.conn().execute(sql, &[]).await
    }

    /// Execute one statement with bound parameters.
    pub(crate) async fn exec_with(&self, sql: &str, params: &[SqlParam]) -> Result<u64> {
        let mut conn = self.acquire().await?;
        conn.conn().execute(sql, params).await
    }

    /// `SQLConnector.exec_queries` (`_sql.py:622`).
    pub(crate) async fn exec_queries(
        &self,
        queries: &[String],
        break_on_error: bool,
        rollback: bool,
    ) -> Result<Vec<Option<u64>>> {
        let mut conn = self.acquire().await?;
        conn.conn().begin().await?;
        conn.in_tx = true;
        let mut results = Vec::with_capacity(queries.len());
        for query in queries {
            match conn.conn().execute(query, &[]).await {
                Ok(n) => results.push(Some(n)),
                Err(e) => {
                    tracing::warn!("Encountered error while executing:\n{e}");
                    results.push(None);
                    if break_on_error {
                        if rollback {
                            conn.conn().rollback().await?;
                            conn.in_tx = false;
                            return Ok(results);
                        }
                        break;
                    }
                }
            }
        }
        conn.conn().commit().await?;
        conn.in_tx = false;
        Ok(results)
    }

    /// `SQLConnector._run_in_autocommit` (`_maintenance.py:54`).
    pub(crate) async fn exec_autocommit(&self, queries: &[String]) -> Result<bool> {
        let mut conn = self.acquire().await?;
        for query in queries {
            if let Err(e) = conn.conn().execute(query, &[]).await {
                tracing::warn!("Encountered error while executing:\n{e}");
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Bound statements in one transaction, stopping (and rolling back) at the first failure.
    pub(crate) async fn exec_many_with(
        &self,
        statements: &[(String, Vec<SqlParam>)],
    ) -> Result<Vec<Option<u64>>> {
        let mut conn = self.acquire().await?;
        conn.conn().begin().await?;
        conn.in_tx = true;
        let mut results = Vec::with_capacity(statements.len());
        for (sql, params) in statements {
            match conn.conn().execute(sql, params).await {
                Ok(n) => results.push(Some(n)),
                Err(e) => {
                    tracing::warn!("Encountered error while executing:\n{e}");
                    results.push(None);
                    conn.conn().rollback().await?;
                    conn.in_tx = false;
                    return Ok(results);
                }
            }
        }
        conn.conn().commit().await?;
        conn.in_tx = false;
        Ok(results)
    }

    /// Run a query with bound parameters and collect its rows as record batches.
    pub(crate) async fn read_with(
        &self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<Vec<RecordBatch>> {
        let mut conn = self.acquire().await?;
        collect_batches(conn.conn().as_mut(), sql, params).await
    }

    /// The first column of the first row.
    pub(crate) async fn value_with(
        &self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<Option<SqlValue>> {
        let mut conn = self.acquire().await?;
        let (_plans, rows) = collect_rows(conn.conn().as_mut(), sql, params, 1).await?;
        Ok(rows.into_iter().next().and_then(|r| r.into_iter().next()))
    }

    /// Begin a transaction on a pooled connection.
    pub(crate) async fn begin(&self) -> Result<NativeTx> {
        let mut conn = self.acquire().await?;
        conn.conn().begin().await?;
        conn.in_tx = true;
        Ok(NativeTx { conn })
    }
}

/// An open transaction on a native connection.
pub(crate) struct NativeTx {
    conn: PooledConn,
}

impl NativeTx {
    pub(crate) async fn exec(&mut self, sql: &str) -> Result<u64> {
        self.conn.conn().execute(sql, &[]).await
    }

    pub(crate) async fn read(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        collect_batches(self.conn.conn().as_mut(), sql, &[]).await
    }

    pub(crate) async fn commit(mut self) -> Result<()> {
        self.conn.conn().commit().await?;
        self.conn.in_tx = false;
        Ok(())
    }

    pub(crate) async fn rollback(mut self) -> Result<()> {
        self.conn.conn().rollback().await?;
        self.conn.in_tx = false;
        Ok(())
    }
}

/// Row-major rows into column-major cells.
pub(crate) fn transpose(rows: Vec<Vec<SqlValue>>, ncols: usize) -> Vec<Vec<SqlValue>> {
    let mut cols: Vec<Vec<SqlValue>> = (0..ncols).map(|_| Vec::with_capacity(rows.len())).collect();
    for row in rows {
        for (j, cell) in row.into_iter().enumerate().take(ncols) {
            cols[j].push(cell);
        }
    }
    cols
}

/// Run a query and collect its plans and (up to `limit`) rows.
pub(crate) async fn collect_rows(
    conn: &mut dyn NativeConn,
    sql: &str,
    params: &[SqlParam],
    limit: usize,
) -> Result<(Vec<ColumnPlan>, Vec<Vec<SqlValue>>)> {
    let (etx, mut erx) = mpsc::channel::<NativeEvent>(2);
    let chunksize = limit.clamp(1, super::DEFAULT_CHUNKSIZE);
    let query = conn.query(sql, params, chunksize, etx);
    let consume = async {
        let mut plans = Vec::new();
        let mut rows = Vec::new();
        while let Some(ev) = erx.recv().await {
            match ev {
                NativeEvent::Columns(p) => plans = p,
                NativeEvent::Rows(r) => {
                    rows.extend(r);
                    if rows.len() >= limit {
                        rows.truncate(limit);
                        break;
                    }
                }
            }
        }
        // Dropping the receiver stops the query early.
        drop(erx);
        (plans, rows)
    };
    let (result, out) = tokio::join!(query, consume);
    result?;
    Ok(out)
}

/// Run a query and collect its rows as one record batch (an empty batch with the result's
/// schema when there are no rows).
pub(crate) async fn collect_batches(
    conn: &mut dyn NativeConn,
    sql: &str,
    params: &[SqlParam],
) -> Result<Vec<RecordBatch>> {
    let (mut plans, rows) = collect_rows(conn, sql, params, usize::MAX).await?;
    if plans.is_empty() && rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(
            SchemaRef::new(Schema::empty()),
        )]);
    }
    let n = rows.len();
    let cells = transpose(rows, plans.len());
    batch::resolve_decimal_scales(&mut plans, &cells);
    let schema = batch::schema_for(&plans);
    Ok(vec![batch::build_batch(&schema, &plans, cells, n)?])
}

/// Send row-major chunks of at most `chunksize` rows from a blocking iterator (DuckDB, Oracle).
/// Returns `false` once the receiver is gone.
#[allow(dead_code)]
pub(crate) fn blocking_send_rows(
    sink: &mpsc::Sender<NativeEvent>,
    buf: &mut Vec<Vec<SqlValue>>,
    chunksize: usize,
    force: bool,
) -> bool {
    if buf.is_empty() || (!force && buf.len() < chunksize) {
        return true;
    }
    let rows = std::mem::take(buf);
    sink.blocking_send(NativeEvent::Rows(rows)).is_ok()
}
