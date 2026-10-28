//! Driver-generic implementations of `read_stream`, `exec`, `exec_queries`, `exec_autocommit`
//! and `value` over a `sqlx` pool. Each family supplies a [`Backend`] that maps its column types
//! to [`ColumnKind`](super::ColumnKind)s and decodes cells.

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use futures_util::StreamExt;
use futures_util::stream;
use sqlx::database::HasStatementCache;
use sqlx::{AssertSqlSafe, Database, Executor, IntoArguments, Pool, Row, SqlSafeStr, Statement};
use tokio::sync::{mpsc, oneshot};

use super::batch::{self, ColumnPlan};
use super::params::SqlParam;
use super::{RecordBatchStream, SqlValue};
use crate::error::{Result, SqlError};

/// One driver family.
pub(crate) trait Backend: 'static {
    type DB: Database;
    const DYNAMIC_TYPES: bool = false;
    /// A cheap statement run before describing an empty read, for drivers whose connection
    /// keeps a stale schema until a statement touches the database (SQLite).
    const SCHEMA_REFRESH: Option<&'static str> = None;
    /// Prepare every statement once per connection (`persistent(true)`, bounded by the statement
    /// cache) instead of preparing and closing it around each execution. `sqlx` 0.9.0 does not set
    /// `TCP_NODELAY` (fixed after 0.9.0, sqlx #4335), and MySQL's `COM_STMT_CLOSE` has no reply:
    /// the connection's next small write (the pool's ping on release, the next statement, `COMMIT`)
    /// waits for the server's delayed ACK, ~40 ms each, and the busy connection makes the pool open
    /// another. A cached statement is never closed, so none of that happens. See
    /// [`statement_persistent`].
    const PERSISTENT_STATEMENTS: bool = false;

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
        q: sqlx::query::Query<'q, Self::DB, <Self::DB as Database>::Arguments>,
        p: &SqlParam,
    ) -> sqlx::query::Query<'q, Self::DB, <Self::DB as Database>::Arguments>;
}

/// The largest statement text kept in a connection's statement cache.
const MAX_PERSISTENT_SQL_BYTES: usize = 8 * 1024;

/// The most bound parameters of a statement kept in a connection's statement cache.
const MAX_PERSISTENT_PARAMS: usize = 128;

/// Whether to prepare a statement once per connection: what the caller asked for, or every
/// small statement on a backend with [`Backend::PERSISTENT_STATEMENTS`]. Large statements (batch
/// inserts) stay unnamed: they are rare, one extra round trip is nothing next to their cost, and
/// the server should not hold them.
fn statement_persistent<B: Backend>(requested: bool, sql: &str, params: usize) -> bool {
    requested
        || (B::PERSISTENT_STATEMENTS
            && sql.len() <= MAX_PERSISTENT_SQL_BYTES
            && params <= MAX_PERSISTENT_PARAMS)
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

/// The output schema of a statement that returned no rows, from `describe`.
///
/// `sqlx` describes through a named statement it caches on the connection. A table dropped and
/// recreated (or altered) since an earlier describe of the same text leaves that statement
/// stale, and PostgreSQL rejects it ("cached plan must not change result type"), which used to
/// turn the empty read into a frame without columns. On failure the connection's statement
/// cache is cleared and the statement described again on the same connection.
async fn describe_schema<B: Backend>(pool: &Pool<B::DB>, sql: &str) -> SchemaRef
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    B::DB: HasStatementCache,
{
    let schema_of = |desc: &<B::DB as Database>::Statement| {
        let mut plans = B::plan(desc.columns());
        let empty = vec![Vec::new(); plans.len()];
        B::refine(&mut plans, &empty);
        batch::resolve_decimal_scales(&mut plans, &[]);
        batch::schema_for(&plans)
    };
    let Ok(mut conn) = pool.acquire().await else {
        return SchemaRef::new(Schema::empty());
    };
    let marked = format!("{sql}{DESCRIBE_MARKER}");
    // `sqlx` 0.9 has no uncached describe (`Executor::describe` is `offline`-only), and `prepare`
    // hands back the connection's cached statement with its old columns after a table was
    // altered or recreated. An empty read is rare, so the cache is cleared first.
    let _ = sqlx::Connection::clear_cached_statements(&mut *conn).await;
    if let Some(refresh) = B::SCHEMA_REFRESH {
        let _ = sqlx::Executor::execute(&mut *conn, sqlx::raw_sql(refresh)).await;
    }
    match (&mut *conn)
        .prepare(AssertSqlSafe(marked.as_str()).into_sql_str())
        .await
    {
        Ok(desc) => schema_of(&desc),
        Err(_) => SchemaRef::new(Schema::empty()),
    }
}

/// The marker appended to statements prepared only to be described, so `sqlx`'s statement
/// cache never hands their named statement to a real execution of the same text (which reuses a
/// cached statement even when it asked for an unnamed one).
pub(super) const DESCRIBE_MARKER: &str = "\n/* mrsm:describe */";

/// PostgreSQL's `0A000` "cached plan must not change result type": a statement in `sqlx`'s
/// per-connection cache whose result columns changed (`ALTER TABLE ... ADD`, a recreated table).
///
/// Also `26000` "unnamed prepared statement does not exist": `sqlx` 0.9 parses an unnamed
/// (`persistent(false)`) statement and only then resolves custom result types it has not seen on
/// the connection (PostGIS `geometry`, enums, ...) with simple queries, which destroy the unnamed
/// statement before it is bound. The lookups fill the connection's type cache, so a rerun on the
/// same connection succeeds ([`fetch_all_fresh`]).
fn is_stale_statement(e: &sqlx::Error) -> bool {
    matches!(
        e,
        sqlx::Error::Database(db) if matches!(db.code().as_deref(), Some("0A000" | "26000"))
    )
}

/// Fetch every row on one pooled connection after clearing its statement cache: the retry of a
/// statement that failed with [`is_stale_statement`], so the error never reaches the caller.
async fn fetch_all_fresh<B: Backend>(
    pool: &Pool<B::DB>,
    sql: &str,
    params: &[SqlParam],
) -> Result<Vec<<B::DB as Database>::Row>>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    for<'q> <B::DB as Database>::Arguments: IntoArguments<B::DB>,
    B::DB: HasStatementCache,
{
    let mut conn = pool.acquire().await?;
    let _ = sqlx::Connection::clear_cached_statements(&mut *conn).await;
    // A second attempt on the same connection covers `26000` (see `is_stale_statement`): the
    // first attempt resolves the custom types into the connection's cache.
    for _ in 0..2 {
        match bound_as::<B>(sql, params, false)
            .fetch_all(&mut *conn)
            .await
        {
            Err(e) if is_stale_statement(&e) => continue,
            other => return Ok(other?),
        }
    }
    Ok(bound_as::<B>(sql, params, false)
        .fetch_all(&mut *conn)
        .await?)
}

/// Stream a query's rows as record batches.
pub(crate) async fn read_stream<B: Backend>(
    pool: Pool<B::DB>,
    sql: String,
    chunksize: usize,
    persistent: bool,
) -> Result<RecordBatchStream>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    for<'q> <B::DB as Database>::Arguments: IntoArguments<B::DB>,
    B::DB: HasStatementCache,
{
    let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(2);
    let (schema_tx, schema_rx) = oneshot::channel::<Result<SchemaRef>>();
    let persistent = statement_persistent::<B>(persistent, &sql, 0);

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
            let mut rows = sqlx::query::<B::DB>(AssertSqlSafe(sql.as_str()))
                .persistent(persistent)
                .fetch(&pool);
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
                    Some(Err(e))
                        if schema_tx.is_some() && buf.is_empty() && is_stale_statement(&e) =>
                    {
                        // Nothing was delivered yet: rerun the statement on a connection with a
                        // cleared statement cache and deliver its rows below.
                        match fetch_all_fresh::<B>(&pool, &sql, &[]).await {
                            Ok(rows) => {
                                buf = rows;
                                break;
                            }
                            Err(err) => {
                                if let Some(stx) = schema_tx.take() {
                                    let _ = stx.send(Err(err));
                                }
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
            let schema = describe_schema::<B>(&pool, &sql).await;
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
    let result = sqlx::raw_sql(AssertSqlSafe(sql)).execute(pool).await?;
    Ok(B::rows_affected(&result))
}

/// The first column of the first row.
pub(crate) async fn value<B: Backend>(
    pool: &Pool<B::DB>,
    sql: &str,
    persistent: bool,
) -> Result<Option<SqlValue>>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    for<'q> <B::DB as Database>::Arguments: IntoArguments<B::DB>,
    B::DB: HasStatementCache,
{
    let row = match sqlx::query::<B::DB>(AssertSqlSafe(sql))
        .persistent(statement_persistent::<B>(persistent, sql, 0))
        .fetch_optional(pool)
        .await
    {
        Err(e) if is_stale_statement(&e) => fetch_all_fresh::<B>(pool, sql, &[])
            .await?
            .into_iter()
            .next(),
        other => other?,
    };
    let Some(row) = row else {
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
) -> sqlx::query::Query<'q, B::DB, <B::DB as Database>::Arguments>
where
    B::DB: HasStatementCache,
{
    bound_as::<B>(sql, params, false)
}

/// [`bound`], optionally as a statement prepared once per connection (`persistent`).
fn bound_as<'q, B: Backend>(
    sql: &'q str,
    params: &[SqlParam],
    persistent: bool,
) -> sqlx::query::Query<'q, B::DB, <B::DB as Database>::Arguments>
where
    B::DB: HasStatementCache,
{
    let persistent = statement_persistent::<B>(persistent, sql, params.len());
    let mut q = sqlx::query::<B::DB>(AssertSqlSafe(sql)).persistent(persistent);
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
    for<'q> <B::DB as Database>::Arguments: IntoArguments<B::DB>,
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
    persistent: bool,
) -> Result<Vec<RecordBatch>>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    for<'q> <B::DB as Database>::Arguments: IntoArguments<B::DB>,
    B::DB: HasStatementCache,
{
    let rows = match bound_as::<B>(sql, params, persistent).fetch_all(pool).await {
        Err(e) if is_stale_statement(&e) => fetch_all_fresh::<B>(pool, sql, params).await?,
        other => other?,
    };
    if rows.is_empty() {
        let schema = describe_schema::<B>(pool, sql).await;
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
    persistent: bool,
) -> Result<Option<SqlValue>>
where
    for<'c> &'c mut <B::DB as Database>::Connection: Executor<'c, Database = B::DB>,
    for<'q> <B::DB as Database>::Arguments: IntoArguments<B::DB>,
    B::DB: HasStatementCache,
{
    let row = match bound_as::<B>(sql, params, persistent)
        .fetch_optional(pool)
        .await
    {
        Err(e) if is_stale_statement(&e) => fetch_all_fresh::<B>(pool, sql, params)
            .await?
            .into_iter()
            .next(),
        other => other?,
    };
    let Some(row) = row else {
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
                    match sqlx::Executor::execute(
                        &mut *tx,
                        sqlx::raw_sql(AssertSqlSafe(query.as_str())),
                    )
                    .await
                    {
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
                    if let Err(e) = sqlx::Executor::execute(
                        &mut *conn,
                        sqlx::raw_sql(AssertSqlSafe(query.as_str())),
                    )
                    .await
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
                let r =
                    sqlx::Executor::execute(&mut **tx, sqlx::raw_sql(AssertSqlSafe(sql))).await?;
                Ok(B::rows_affected(&r))
            }

            /// Run a query inside an open transaction and collect its rows.
            pub(crate) async fn tx_read(
                tx: &mut sqlx::Transaction<'static, Db>,
                sql: &str,
            ) -> Result<Vec<RecordBatch>> {
                let rows = sqlx::Executor::fetch_all(
                    &mut **tx,
                    sqlx::query::<Db>(AssertSqlSafe(sql))
                        .persistent(super::statement_persistent::<B>(false, sql, 0)),
                )
                .await?;
                if rows.is_empty() {
                    let marked = format!("{sql}{}", super::DESCRIBE_MARKER);
                    let _ = sqlx::Connection::clear_cached_statements(&mut **tx).await;
                    if let Some(refresh) = B::SCHEMA_REFRESH {
                        let _ = sqlx::Executor::execute(&mut **tx, sqlx::raw_sql(refresh)).await;
                    }
                    let schema = match sqlx::Executor::prepare(
                        &mut **tx,
                        AssertSqlSafe(marked.as_str()).into_sql_str(),
                    )
                    .await
                    {
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

            /// Execute one statement with bound parameters inside an open transaction
            /// (`driver::raw`; SQLite has its own there).
            #[allow(dead_code)]
            pub(crate) async fn tx_exec_with(
                tx: &mut sqlx::Transaction<'static, Db>,
                sql: &str,
                params: &[SqlParam],
            ) -> Result<u64> {
                let r = sqlx::Executor::execute(&mut **tx, bound::<B>(sql, params)).await?;
                Ok(B::rows_affected(&r))
            }

            /// Run a query with bound parameters inside an open transaction and collect its rows
            /// (`driver::raw`; SQLite has its own there).
            #[allow(dead_code)]
            pub(crate) async fn tx_read_with(
                tx: &mut sqlx::Transaction<'static, Db>,
                sql: &str,
                params: &[SqlParam],
            ) -> Result<Vec<RecordBatch>> {
                if params.is_empty() {
                    return tx_read(tx, sql).await;
                }
                let rows = sqlx::Executor::fetch_all(&mut **tx, bound::<B>(sql, params)).await?;
                if rows.is_empty() {
                    let marked = format!("{sql}{}", super::DESCRIBE_MARKER);
                    let schema = match sqlx::Executor::prepare(
                        &mut **tx,
                        AssertSqlSafe(marked.as_str()).into_sql_str(),
                    )
                    .await
                    {
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
