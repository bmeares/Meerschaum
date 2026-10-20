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

use super::batch::{self, ColumnKind, ColumnPlan};
use super::params::SqlParam;
use super::{RecordBatchStream, SqlValue};
use crate::error::{Result, SqlError};

/// One event produced by [`NativeConn::query`]: the result's column plans (exactly once, before
/// any rows), then row-major chunks of cells.
#[derive(Debug)]
pub enum NativeEvent {
    Columns(Vec<ColumnPlan>),
    Rows(Vec<Vec<SqlValue>>),
}

/// One open connection of a native driver family.
#[async_trait]
pub trait NativeConn: Send + 'static {
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
pub trait NativeConnector: Send + Sync + 'static {
    /// Embedded drivers may forbid inherited pool reuse after fork.
    fn owner_pid(&self) -> Option<u32> {
        None
    }

    async fn connect(&self) -> Result<Box<dyn NativeConn>>;
}

struct PoolInner {
    owner_pid: Option<u32>,
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
    _permit: Option<OwnedSemaphorePermit>,
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if self
            .pool
            .owner_pid
            .is_some_and(|pid| pid != std::process::id())
            && let Some(permit) = self._permit.take()
        {
            std::mem::forget(permit);
        }
        if let Some(conn) = self.conn.take()
            && self
                .pool
                .owner_pid
                .is_none_or(|pid| pid == std::process::id())
            && !self.in_tx
            && !conn.is_broken()
            && let Ok(mut idle) = self.pool.idle.lock()
        {
            idle.push(conn);
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

/// How many times a statement or transaction that lost a transaction conflict is run again.
pub(crate) const CONFLICT_RETRIES: u32 = 40;

/// Whether a database message says a concurrent transaction changed the same catalog entry or
/// row first (DuckDB: "Catalog write-write conflict on create with ...", "Conflict on tuple
/// deletion!", ...). DuckDB uses optimistic concurrency: the loser is rolled back whole and is
/// expected to try again on a fresh snapshot.
pub(crate) fn is_conflict_message(message: &str) -> bool {
    message.contains("TransactionContext Error")
        && (message.contains("write-write conflict")
            || message.contains("Conflict on")
            || message.contains("conflict on"))
}

/// [`is_conflict_message`] for a driver error.
pub(crate) fn is_transaction_conflict(error: &SqlError) -> bool {
    matches!(error, SqlError::Database(message) if is_conflict_message(message))
}

/// How long the loser of a conflict waits before attempt `attempt` (1-based): the winner commits
/// within microseconds, so a short, growing pause with jitter keeps several losers from
/// colliding again in lockstep.
pub(crate) fn conflict_backoff_duration(attempt: u32) -> std::time::Duration {
    let jitter = u64::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos())
            % 1000,
    );
    std::time::Duration::from_micros(u64::from(attempt.min(10)) * 500 + jitter)
}

async fn conflict_backoff(attempt: u32) {
    tokio::time::sleep(conflict_backoff_duration(attempt)).await;
}

impl NativePool {
    /// A pool over `connector` with at most `max` open connections.
    pub(crate) fn new(connector: Box<dyn NativeConnector>, max: u32) -> Self {
        NativePool {
            inner: Arc::new(PoolInner {
                owner_pid: connector.owner_pid(),
                connector,
                idle: Mutex::new(Vec::new()),
                sem: Arc::new(Semaphore::new(max.max(1) as usize)),
            }),
        }
    }

    /// Check out a connection, opening one when none is idle.
    pub(crate) async fn acquire(&self) -> Result<PooledConn> {
        if self
            .inner
            .owner_pid
            .is_some_and(|pid| pid != std::process::id())
        {
            return Err(SqlError::Database("DuckDB state belongs to the parent process; start a fresh interpreter (spawn or exec).".into()));
        }
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
            _permit: Some(permit),
        })
    }

    /// Drop every idle connection.
    pub(crate) fn close(&self) {
        if self
            .inner
            .owner_pid
            .is_some_and(|pid| pid != std::process::id())
        {
            return;
        }
        if let Ok(mut idle) = self.inner.idle.lock() {
            idle.clear();
        }
    }

    /// Stream a query's rows as record batches of at most `chunksize` rows, all with the schema
    /// of the stream.
    ///
    /// An unconstrained Oracle `NUMBER` ([`ColumnKind::NumberAuto`]) has no type known up front.
    /// With `collect` (the caller gathers the whole result: [`SqlDriver::read_frame`](super::SqlDriver::read_frame),
    /// `read`) the rows of such a result are held until the end and the column is typed from every
    /// value (`Int64`, `Float64` or an exact decimal), so the batches agree and nothing is cast.
    /// Without it (a stream handed out batch by batch, one schema for the whole stream) the column is
    /// exact decimal text (`NumericText`), whatever later chunks hold.
    pub(crate) async fn read_stream(
        &self,
        sql: String,
        chunksize: usize,
        collect: bool,
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
            // Sends one batch (and the schema before the first); `false` once the consumer is gone
            // or the first batch failed.
            let consume = async {
                let mut plans: Option<Vec<ColumnPlan>> = None;
                let mut schema: Option<SchemaRef> = None;
                let mut held: Vec<(Vec<Vec<SqlValue>>, usize)> = Vec::new();
                macro_rules! emit {
                    ($item:expr, $plans:expr) => {{
                        let item = $item;
                        if let Some(stx) = schema_tx.take() {
                            let failed = item.is_err();
                            let _ = stx.send(item.as_ref().map(|b| b.schema()).map_err(|e| {
                                SqlError::Conversion {
                                    column: String::new(),
                                    reason: e.to_string(),
                                }
                            }));
                            if failed {
                                erx.close();
                                return ($plans.clone(), schema);
                            }
                        }
                        if tx.send(item).await.is_err() {
                            erx.close();
                            return ($plans.clone(), schema);
                        }
                    }};
                }
                while let Some(ev) = erx.recv().await {
                    match ev {
                        NativeEvent::Columns(mut p) => {
                            if !collect {
                                for plan in &mut p {
                                    if plan.kind == ColumnKind::NumberAuto {
                                        plan.kind = ColumnKind::NumericText;
                                    }
                                }
                            }
                            plans = Some(p);
                        }
                        NativeEvent::Rows(rows) => {
                            let Some(plans) = plans.as_mut() else {
                                continue;
                            };
                            let n = rows.len();
                            let cells = transpose(rows, plans.len());
                            if plans.iter().any(|p| p.kind == ColumnKind::NumberAuto) {
                                held.push((cells, n));
                                continue;
                            }
                            if schema.is_none() {
                                batch::resolve_decimal_scales(plans, &cells);
                                schema = Some(batch::schema_for(plans));
                            }
                            let s = schema.clone().expect("schema");
                            emit!(batch::build_batch(&s, plans, cells, n), plans);
                        }
                    }
                }
                if let Some(plans) = plans.as_mut().filter(|_| !held.is_empty()) {
                    // Decimal columns keep the first chunk's scale (as when streaming); the
                    // unconstrained `NUMBER` ones are typed from every chunk.
                    batch::resolve_scales(plans, &held[0].0);
                    batch::resolve_number_auto_with(plans, |i| {
                        held.iter().map(|(cells, _)| cells[i].as_slice()).collect()
                    });
                    let s = batch::schema_for(plans);
                    schema = Some(s.clone());
                    for (cells, n) in std::mem::take(&mut held) {
                        emit!(batch::build_batch(&s, plans, cells, n), plans);
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
    ///
    /// A transaction that loses a catalog / row conflict to a concurrent one (DuckDB's optimistic
    /// concurrency, see [`is_transaction_conflict`]) is rolled back and run again from its first
    /// statement, a bounded number of times.
    pub(crate) async fn exec_queries(
        &self,
        queries: &[String],
        break_on_error: bool,
        rollback: bool,
    ) -> Result<Vec<Option<u64>>> {
        let mut conn = self.acquire().await?;
        let mut attempt = 0u32;
        'attempt: loop {
            // Mark before awaiting BEGIN: cancellation must discard a possibly active transaction.
            conn.in_tx = true;
            conn.conn().begin().await?;
            let mut results = Vec::with_capacity(queries.len());
            for query in queries {
                match conn.conn().execute(query, &[]).await {
                    Ok(n) => results.push(Some(n)),
                    Err(e) => {
                        if attempt < CONFLICT_RETRIES && is_transaction_conflict(&e) {
                            attempt += 1;
                            conn.conn().rollback().await?;
                            conn.in_tx = false;
                            conflict_backoff(attempt).await;
                            continue 'attempt;
                        }
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
            return Ok(results);
        }
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
    /// A lost transaction conflict is retried like in [`NativePool::exec_queries`].
    pub(crate) async fn exec_many_with(
        &self,
        statements: &[(String, Vec<SqlParam>)],
    ) -> Result<Vec<Option<u64>>> {
        let mut conn = self.acquire().await?;
        let mut attempt = 0u32;
        'attempt: loop {
            // Mark before awaiting BEGIN: cancellation must discard a possibly active transaction.
            conn.in_tx = true;
            conn.conn().begin().await?;
            let mut results = Vec::with_capacity(statements.len());
            for (sql, params) in statements {
                match conn.conn().execute(sql, params).await {
                    Ok(n) => results.push(Some(n)),
                    Err(e) => {
                        conn.conn().rollback().await?;
                        conn.in_tx = false;
                        if attempt < CONFLICT_RETRIES && is_transaction_conflict(&e) {
                            attempt += 1;
                            conflict_backoff(attempt).await;
                            continue 'attempt;
                        }
                        tracing::warn!("Encountered error while executing:\n{e}");
                        results.push(None);
                        return Ok(results);
                    }
                }
            }
            conn.conn().commit().await?;
            conn.in_tx = false;
            return Ok(results);
        }
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
        // Mark before awaiting BEGIN: cancellation must discard a possibly active transaction.
        conn.in_tx = true;
        conn.conn().begin().await?;
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

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Connector {
        opens: Arc<AtomicUsize>,
        begun: Arc<tokio::sync::Notify>,
    }
    struct Conn {
        begun: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl NativeConnector for Connector {
        async fn connect(&self) -> Result<Box<dyn NativeConn>> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Conn {
                begun: self.begun.clone(),
            }))
        }
    }
    #[async_trait]
    impl NativeConn for Conn {
        async fn execute(&mut self, _: &str, _: &[SqlParam]) -> Result<u64> {
            Ok(0)
        }
        async fn query(
            &mut self,
            sql: &str,
            _: &[SqlParam],
            _: usize,
            sink: mpsc::Sender<NativeEvent>,
        ) -> Result<()> {
            if sink
                .send(NativeEvent::Columns(vec![ColumnPlan::new(
                    "value",
                    ColumnKind::Int64,
                    "BIGINT",
                )]))
                .await
                .is_err()
            {
                return Ok(());
            }
            let count = if sql == "large" { 10_000 } else { 1 };
            for _ in 0..count {
                let value = if sql == "invalid" {
                    SqlValue::Bytes(vec![0xff])
                } else {
                    SqlValue::Int(1)
                };
                if sink
                    .send(NativeEvent::Rows(vec![vec![value]]))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok(())
        }
        async fn begin(&mut self) -> Result<()> {
            // The server has accepted BEGIN, but its asynchronous acknowledgement may never
            // reach the caller (e.g. cancellation at the Python / blocking-task boundary).
            self.begun.notify_one();
            std::future::pending().await
        }
        async fn commit(&mut self) -> Result<()> {
            Ok(())
        }
        async fn rollback(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn cancelling_begin_discards_the_possibly_active_connection() {
        let opens = Arc::new(AtomicUsize::new(0));
        let begun = Arc::new(tokio::sync::Notify::new());
        let pool = NativePool::new(
            Box::new(Connector {
                opens: opens.clone(),
                begun: begun.clone(),
            }),
            1,
        );
        let pending_pool = pool.clone();
        let task = tokio::spawn(async move { pending_pool.begin().await });
        begun.notified().await;
        task.abort();
        let result = task.await;
        assert!(matches!(result, Err(ref error) if error.is_cancelled()));
        let _connection = pool.acquire().await.unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            2,
            "a cancelled BEGIN must not return its connection to the pool"
        );
    }

    #[tokio::test]
    async fn abandoned_or_invalid_stream_releases_pool_capacity() {
        let pool = NativePool::new(
            Box::new(Connector {
                opens: Arc::new(AtomicUsize::new(0)),
                begun: Arc::new(tokio::sync::Notify::new()),
            }),
            1,
        );
        let mut stream = pool.read_stream("large".into(), 1, false).await.unwrap();
        assert!(stream.next().await.unwrap().is_ok());
        drop(stream);
        let value = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            pool.value_with("SELECT 1", &[]),
        )
        .await
        .expect("abandoning a bounded stream must unblock its producer")
        .unwrap();
        assert_eq!(value, Some(SqlValue::Int(1)));
        assert!(pool.read_stream("invalid".into(), 1, false).await.is_err());
        let value = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            pool.value_with("SELECT 1", &[]),
        )
        .await
        .expect("a conversion failure must release its producer and permit")
        .unwrap();
        assert_eq!(value, Some(SqlValue::Int(1)));
    }
}
