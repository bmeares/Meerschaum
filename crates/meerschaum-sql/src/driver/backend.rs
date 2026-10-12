//! Driver-generic implementations of `read_stream`, `exec`, `exec_queries`, `exec_autocommit`
//! and `value` over a `sqlx` pool. Each family supplies a [`Backend`] that maps its column types
//! to [`ColumnKind`](super::ColumnKind)s and decodes cells.

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use futures_util::StreamExt;
use futures_util::stream;
use sqlx::database::HasStatementCache;
use sqlx::{Database, Executor, IntoArguments, Pool, Row};
use tokio::sync::{mpsc, oneshot};

use super::batch::{self, ColumnPlan};
use super::{RecordBatchStream, SqlValue};
use crate::error::{Result, SqlError};

/// One driver family.
pub(crate) trait Backend: 'static {
    type DB: Database;

    /// Plan the output columns from the statement's column metadata.
    fn plan(columns: &[<Self::DB as Database>::Column]) -> Vec<ColumnPlan>;

    /// Decode one cell.
    fn cell(row: &<Self::DB as Database>::Row, idx: usize, plan: &ColumnPlan) -> Result<SqlValue>;

    /// Adjust the plan after the first batch's cells are known (SQLite's dynamic typing).
    fn refine(_plans: &mut [ColumnPlan], _columns: &[Vec<SqlValue>]) {}

    /// The rows affected by a statement.
    fn rows_affected(result: &<Self::DB as Database>::QueryResult) -> u64;
}

/// Decode a buffer of rows into column-major cells.
fn rows_to_cells<B: Backend>(
    rows: &[<B::DB as Database>::Row],
    plans: &[ColumnPlan],
) -> Result<Vec<Vec<SqlValue>>> {
    plans
        .iter()
        .enumerate()
        .map(|(i, plan)| rows.iter().map(|row| B::cell(row, i, plan)).collect())
        .collect()
}

struct StreamState {
    schema: SchemaRef,
    plans: Vec<ColumnPlan>,
}

fn make_batch<B: Backend>(
    rows: &[<B::DB as Database>::Row],
    state: &mut Option<StreamState>,
) -> Result<RecordBatch> {
    if state.is_none() {
        let columns = rows.first().map(|r| r.columns()).unwrap_or(&[]);
        let mut plans = B::plan(columns);
        let cells = rows_to_cells::<B>(rows, &plans)?;
        B::refine(&mut plans, &cells);
        batch::resolve_decimal_scales(&mut plans, &cells);
        let schema = batch::schema_for(&plans);
        let out = batch::build_batch(&schema, &plans, cells, rows.len())?;
        *state = Some(StreamState { schema, plans });
        return Ok(out);
    }
    let st = state.as_ref().expect("state");
    let cells = rows_to_cells::<B>(rows, &st.plans)?;
    batch::build_batch(&st.schema, &st.plans, cells, rows.len())
}

/// Stream a query's rows as record batches.
pub(crate) async fn read_stream<B: Backend>(
    pool: Pool<B::DB>,
    sql: String,
    chunksize: usize,
) -> Result<RecordBatchStream>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    for<'q> <B::DB as Database>::Arguments<'q>: IntoArguments<'q, B::DB>,
    B::DB: HasStatementCache,
{
    let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(2);
    let (schema_tx, schema_rx) = oneshot::channel::<Result<SchemaRef>>();

    tokio::spawn(async move {
        let mut schema_tx = Some(schema_tx);
        let mut state: Option<StreamState> = None;
        let mut buf: Vec<<B::DB as Database>::Row> = Vec::with_capacity(chunksize.min(4096));

        // Deliver an item: the first batch's schema goes to the caller through `schema_tx`.
        async fn deliver(
            item: Result<RecordBatch>,
            schema_tx: &mut Option<oneshot::Sender<Result<SchemaRef>>>,
            tx: &mpsc::Sender<Result<RecordBatch>>,
        ) -> bool {
            if let Some(stx) = schema_tx.take() {
                match &item {
                    Ok(b) => {
                        if stx.send(Ok(b.schema())).is_err() {
                            return false;
                        }
                    }
                    Err(_) => {
                        let _ = stx.send(item.map(|b| b.schema()));
                        return false;
                    }
                }
            }
            tx.send(item).await.is_ok()
        }

        {
            let mut rows = sqlx::query::<B::DB>(&sql).persistent(false).fetch(&pool);
            loop {
                match rows.next().await {
                    Some(Ok(row)) => {
                        buf.push(row);
                        if buf.len() >= chunksize {
                            let item = make_batch::<B>(&buf, &mut state);
                            buf.clear();
                            let failed = item.is_err();
                            if !deliver(item, &mut schema_tx, &tx).await || failed {
                                return;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        let err = SqlError::from(e);
                        match schema_tx.take() {
                            Some(stx) => {
                                let _ = stx.send(Err(err));
                            }
                            None => {
                                let _ = tx.send(Err(err)).await;
                            }
                        }
                        return;
                    }
                    None => break,
                }
            }
        }
        if !buf.is_empty() {
            let item = make_batch::<B>(&buf, &mut state);
            buf.clear();
            if !deliver(item, &mut schema_tx, &tx).await {
                return;
            }
        }
        if let Some(stx) = schema_tx.take() {
            // No rows: describe the statement for its columns.
            let schema = match (&pool).describe(&sql).await {
                Ok(desc) => {
                    let mut plans = B::plan(desc.columns());
                    let empty = vec![Vec::new(); plans.len()];
                    B::refine(&mut plans, &empty);
                    batch::resolve_decimal_scales(&mut plans, &[]);
                    batch::schema_for(&plans)
                }
                Err(_) => SchemaRef::new(Schema::empty()),
            };
            let _ = stx.send(Ok(schema));
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

/// Execute one or more statements with the simple-query protocol.
pub(crate) async fn exec<B: Backend>(pool: &Pool<B::DB>, sql: &str) -> Result<u64>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
{
    let result = sqlx::raw_sql(sql).execute(pool).await?;
    Ok(B::rows_affected(&result))
}

/// `SQLConnector.exec_queries` (`_sql.py:622`).
pub(crate) async fn exec_queries<B: Backend>(
    pool: &Pool<B::DB>,
    queries: &[String],
    break_on_error: bool,
    rollback: bool,
) -> Result<Vec<Option<u64>>>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
{
    let mut tx = pool.begin().await?;
    let mut results = Vec::with_capacity(queries.len());
    for query in queries {
        match sqlx::raw_sql(query).execute(&mut *tx).await {
            Ok(r) => results.push(Some(B::rows_affected(&r))),
            Err(e) => {
                tracing::warn!("Encountered error while executing:\n{e}");
                results.push(None);
                if break_on_error {
                    if rollback {
                        tx.rollback().await?;
                        return Ok(results);
                    }
                    break;
                }
            }
        }
    }
    tx.commit().await?;
    Ok(results)
}

/// `SQLConnector._run_in_autocommit` (`_maintenance.py:54`).
pub(crate) async fn exec_autocommit<B: Backend>(
    pool: &Pool<B::DB>,
    queries: &[String],
) -> Result<bool>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
{
    let mut conn = pool.acquire().await?;
    for query in queries {
        if let Err(e) = sqlx::raw_sql(query).execute(&mut *conn).await {
            tracing::warn!("Encountered error while executing:\n{e}");
            return Ok(false);
        }
    }
    Ok(true)
}

/// The first column of the first row.
pub(crate) async fn value<B: Backend>(pool: &Pool<B::DB>, sql: &str) -> Result<Option<SqlValue>>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    for<'q> <B::DB as Database>::Arguments<'q>: IntoArguments<'q, B::DB>,
    B::DB: HasStatementCache,
{
    let Some(row) = sqlx::query::<B::DB>(sql)
        .persistent(false)
        .fetch_optional(pool)
        .await?
    else {
        return Ok(None);
    };
    if row.columns().is_empty() {
        return Ok(None);
    }
    let plans = B::plan(row.columns());
    B::cell(&row, 0, &plans[0]).map(Some)
}
