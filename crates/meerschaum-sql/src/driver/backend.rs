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
use super::params::SqlParam;
use super::{RecordBatchStream, SqlValue};
use crate::error::{Result, SqlError};

/// One driver family.
pub(crate) trait Backend: 'static {
    type DB: Database;
    const DYNAMIC_TYPES: bool = false;

    /// Plan the output columns from the statement's column metadata.
    fn plan(columns: &[<Self::DB as Database>::Column]) -> Vec<ColumnPlan>;

    /// Decode one cell.
    fn cell(row: &<Self::DB as Database>::Row, idx: usize, plan: &ColumnPlan) -> Result<SqlValue>;

    /// Decode a column directly into Arrow when its storage layout is known.
    /// Special types retain the lossless conversion path (decimal scale, geometry, etc.).
    fn array(
        rows: &[<Self::DB as Database>::Row],
        idx: usize,
        plan: &ColumnPlan,
    ) -> Result<arrow::array::ArrayRef> {
        let cells = rows
            .iter()
            .map(|r| Self::cell(r, idx, plan))
            .collect::<Result<Vec<_>>>()?;
        batch::build_array(plan, &cells)
    }

    /// Adjust the plan after the first batch's cells are known (SQLite's dynamic typing).
    fn refine(_plans: &mut [ColumnPlan], _columns: &[Vec<SqlValue>]) {}

    /// The rows affected by a statement.
    fn rows_affected(result: &<Self::DB as Database>::QueryResult) -> u64;

    /// Bind one parameter.
    fn bind<'q>(
        q: sqlx::query::Query<'q, Self::DB, <Self::DB as Database>::Arguments<'q>>,
        p: &SqlParam,
    ) -> sqlx::query::Query<'q, Self::DB, <Self::DB as Database>::Arguments<'q>>;
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
        if !B::DYNAMIC_TYPES && plans.iter().all(|p| p.kind != batch::ColumnKind::Decimal) {
            let schema = batch::schema_for(&plans);
            let arrays = plans
                .iter()
                .enumerate()
                .map(|(i, p)| B::array(rows, i, p))
                .collect::<Result<Vec<_>>>()?;
            let out = RecordBatch::try_new(schema.clone(), arrays)?;
            *state = Some(StreamState { schema, plans });
            return Ok(out);
        }
        let cells = rows_to_cells::<B>(rows, &plans)?;
        B::refine(&mut plans, &cells);
        batch::resolve_decimal_scales(&mut plans, &cells);
        let schema = batch::schema_for(&plans);
        let out = batch::build_batch(&schema, &plans, cells, rows.len())?;
        *state = Some(StreamState { schema, plans });
        return Ok(out);
    }
    let st = state.as_ref().expect("state");
    let arrays = st
        .plans
        .iter()
        .enumerate()
        .map(|(i, p)| B::array(rows, i, p))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        st.schema.clone(),
        arrays,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
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

fn bound<'q, B: Backend>(
    sql: &'q str,
    params: &[SqlParam],
) -> sqlx::query::Query<'q, B::DB, <B::DB as Database>::Arguments<'q>>
where
    B::DB: HasStatementCache,
{
    let mut q = sqlx::query::<B::DB>(sql).persistent(false);
    for p in params {
        q = B::bind(q, p);
    }
    q
}

/// Execute one statement with bound parameters.
pub(crate) async fn exec_with<B: Backend>(
    pool: &Pool<B::DB>,
    sql: &str,
    params: &[SqlParam],
) -> Result<u64>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    for<'q> <B::DB as Database>::Arguments<'q>: IntoArguments<'q, B::DB>,
    B::DB: HasStatementCache,
{
    let result = bound::<B>(sql, params).execute(pool).await?;
    Ok(B::rows_affected(&result))
}

/// Run a query with bound parameters and collect its rows as record batches.
pub(crate) async fn read_with<B: Backend>(
    pool: &Pool<B::DB>,
    sql: &str,
    params: &[SqlParam],
) -> Result<Vec<RecordBatch>>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    for<'q> <B::DB as Database>::Arguments<'q>: IntoArguments<'q, B::DB>,
    B::DB: HasStatementCache,
{
    let rows = bound::<B>(sql, params).fetch_all(pool).await?;
    if rows.is_empty() {
        let schema = match pool.describe(sql).await {
            Ok(desc) => {
                let mut plans = B::plan(desc.columns());
                let empty = vec![Vec::new(); plans.len()];
                B::refine(&mut plans, &empty);
                batch::resolve_decimal_scales(&mut plans, &[]);
                batch::schema_for(&plans)
            }
            Err(_) => SchemaRef::new(Schema::empty()),
        };
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }
    let mut state = None;
    Ok(vec![make_batch::<B>(&rows, &mut state)?])
}

/// The first column of the first row of a query with bound parameters.
pub(crate) async fn value_with<B: Backend>(
    pool: &Pool<B::DB>,
    sql: &str,
    params: &[SqlParam],
) -> Result<Option<SqlValue>>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    for<'q> <B::DB as Database>::Arguments<'q>: IntoArguments<'q, B::DB>,
    B::DB: HasStatementCache,
{
    let Some(row) = bound::<B>(sql, params).fetch_optional(pool).await? else {
        return Ok(None);
    };
    if row.columns().is_empty() {
        return Ok(None);
    }
    let plans = B::plan(row.columns());
    B::cell(&row, 0, &plans[0]).map(Some)
}

/// The transaction-scoped operations, generated per driver family with concrete types: their
/// futures must be provably `Send` (generic `&mut Connection` executors are not, see
/// rust-lang/rust#102211).
macro_rules! tx_ops {
    ($m:ident, $B:ty) => {
        pub(crate) mod $m {
            use super::*;

            type B = $B;
            type Db = <$B as Backend>::DB;

            /// `SQLConnector.exec_queries` (`_sql.py:622`).
            pub(crate) async fn exec_queries(
                pool: &Pool<Db>,
                queries: &[String],
                break_on_error: bool,
                rollback: bool,
            ) -> Result<Vec<Option<u64>>> {
                let mut tx = pool.begin().await?;
                let mut results = Vec::with_capacity(queries.len());
                for query in queries {
                    match sqlx::Executor::execute(&mut *tx, sqlx::raw_sql(query)).await {
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
            pub(crate) async fn exec_autocommit(
                pool: &Pool<Db>,
                queries: &[String],
            ) -> Result<bool> {
                let mut conn = pool.acquire().await?;
                for query in queries {
                    if let Err(e) = sqlx::Executor::execute(&mut *conn, sqlx::raw_sql(query)).await
                    {
                        tracing::warn!("Encountered error while executing:\n{e}");
                        return Ok(false);
                    }
                }
                Ok(true)
            }

            /// Execute statements inside an open transaction (simple-query protocol).
            pub(crate) async fn tx_exec(
                tx: &mut sqlx::Transaction<'static, Db>,
                sql: &str,
            ) -> Result<u64> {
                let r = sqlx::Executor::execute(&mut **tx, sqlx::raw_sql(sql)).await?;
                Ok(B::rows_affected(&r))
            }

            /// Run a query inside an open transaction and collect its rows.
            pub(crate) async fn tx_read(
                tx: &mut sqlx::Transaction<'static, Db>,
                sql: &str,
            ) -> Result<Vec<RecordBatch>> {
                let rows =
                    sqlx::Executor::fetch_all(&mut **tx, sqlx::query::<Db>(sql).persistent(false))
                        .await?;
                if rows.is_empty() {
                    let schema = match sqlx::Executor::describe(&mut **tx, sql).await {
                        Ok(desc) => {
                            let mut plans = B::plan(desc.columns());
                            let empty = vec![Vec::new(); plans.len()];
                            B::refine(&mut plans, &empty);
                            batch::resolve_decimal_scales(&mut plans, &[]);
                            batch::schema_for(&plans)
                        }
                        Err(_) => SchemaRef::new(Schema::empty()),
                    };
                    return Ok(vec![RecordBatch::new_empty(schema)]);
                }
                let mut state = None;
                Ok(vec![make_batch::<B>(&rows, &mut state)?])
            }

            /// Bound statements in one transaction, stopping (and rolling back) at the first
            /// failure.
            pub(crate) async fn exec_many_with(
                pool: &Pool<Db>,
                statements: &[(String, Vec<SqlParam>)],
            ) -> Result<Vec<Option<u64>>> {
                let mut tx = pool.begin().await?;
                let mut results = Vec::with_capacity(statements.len());
                for (sql, params) in statements {
                    match sqlx::Executor::execute(&mut *tx, bound::<B>(sql, params)).await {
                        Ok(r) => results.push(Some(B::rows_affected(&r))),
                        Err(e) => {
                            tracing::warn!("Encountered error while executing:\n{e}");
                            results.push(None);
                            tx.rollback().await?;
                            return Ok(results);
                        }
                    }
                }
                tx.commit().await?;
                Ok(results)
            }
        }
    };
}

tx_ops!(pg_tx, crate::driver::postgres::PgBackend);
tx_ops!(mysql_tx, crate::driver::mysql::MySqlBackend);
tx_ops!(sqlite_tx, crate::driver::sqlite::SqliteBackend);
