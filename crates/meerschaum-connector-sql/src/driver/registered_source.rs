//! Exact relation and registration proof for a bounded, typed PostgreSQL source read.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use indexmap::IndexMap;
use meerschaum_connector::{DataQuery, DateBound};
use meerschaum_core::Value;
use meerschaum_kernel::Frame;
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::{AssertSqlSafe, Connection, Row, ValueRef};

use super::{SqlParam, backend, postgres::PgBackend};
use crate::instance::registered_source::{RegisteredSource, SourceReadBudget, SourceReadError};
use crate::uri::ConnectionSpec;

use SourceReadError::{Changed, TooLarge, Unavailable, Unsupported};

const MAX_COLUMNS: usize = 1024;
const MAX_METADATA_BYTES: usize = 256 * 1024;

impl From<sqlx::Error> for SourceReadError {
    fn from(_: sqlx::Error) -> Self {
        Unavailable
    }
}
impl From<crate::SqlError> for SourceReadError {
    fn from(_: crate::SqlError) -> Self {
        Unavailable
    }
}

fn normal_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn relation(schema: &str, name: &str) -> String {
    format!("{}.{}", identifier(schema), identifier(name))
}

#[derive(Debug, PartialEq)]
struct Relation {
    oid: i64,
    columns: IndexMap<String, i64>,
}

async fn describe(
    connection: &mut PgConnection,
    schema: &str,
    table: &str,
    registry: bool,
) -> Result<Relation, SourceReadError> {
    // Choose ordinary permanent heap storage with no data-reading extensions or rewrite paths.
    // A lock held by the caller serializes these checks with relation and index DDL.
    // Main's own registration catalog is already trusted for metadata access and retains
    // its normal framework indexes, including the JSONB and unique registration indexes.
    let oid: i64 = sqlx::query_scalar(
        "SELECT c.oid::int8
           FROM pg_catalog.pg_class AS c
           JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
          WHERE n.nspname = $1 AND c.relname = $2
            AND c.relkind = 'r' AND c.relpersistence = 'p' AND c.relam = 2
            AND NOT (c.relispartition OR c.relhassubclass OR c.relrowsecurity
                     OR c.relforcerowsecurity OR c.relhasrules)
            AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_inherits AS inheritance
                WHERE inheritance.inhrelid = c.oid
            )
            AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                  AND a.attgenerated <> ''
            )
            AND ($3 OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ix ON ix.oid = i.indexrelid
                WHERE i.indrelid = c.oid
                  AND (i.indexprs IS NOT NULL OR i.indpred IS NOT NULL OR ix.relam <> 403 OR EXISTS (
                        SELECT 1 FROM pg_catalog.unnest(i.indclass::pg_catalog.oid[]) AS id
                        JOIN pg_catalog.pg_opclass AS o ON id = o.oid
                        JOIN pg_catalog.pg_namespace AS ns ON ns.oid = o.opcnamespace
                        WHERE ns.nspname <> 'pg_catalog'
                  ))
            ))"
    )
    .bind(schema)
    .bind(table)
    .bind(registry)
    .persistent(false)
    .fetch_optional(&mut *connection)
    .await?
    .ok_or(Unsupported)?;
    let columns = sqlx::query_as::<_, (String, i64, i64)>(
        "SELECT attname::text, atttypid::int8, attcollation::int8
           FROM pg_catalog.pg_attribute
          WHERE attrelid::int8 = $1 AND attnum > 0 AND NOT attisdropped
          ORDER BY attnum LIMIT 1025",
    )
    .bind(oid)
    .persistent(false)
    .fetch_all(&mut *connection)
    .await?;
    if columns.is_empty() || columns.len() > MAX_COLUMNS {
        return Err(Unsupported);
    }
    // Built-in collation OIDs are stable across supported PostgreSQL versions. This excludes
    // custom collations from comparisons and sorts in a caller-controlled read.
    let mut types = IndexMap::new();
    for (column, kind, collation) in columns {
        if !normal_name(&column) || !matches!(collation, 0 | 100 | 950 | 951) {
            return Err(Unsupported);
        }
        types.insert(column, kind);
    }
    Ok(Relation {
        oid,
        columns: types,
    })
}

async fn lock(
    connection: &mut PgConnection,
    schema: &str,
    table: &str,
    registry: bool,
) -> Result<Relation, SourceReadError> {
    let before = describe(connection, schema, table, registry).await?;
    // Hold the registration row's definition stable through the data SELECT.
    let mode = if registry {
        "SHARE"
    } else {
        "SHARE UPDATE EXCLUSIVE"
    };
    let sql = format!(
        "LOCK TABLE ONLY {} IN {mode} MODE NOWAIT",
        relation(schema, table),
    );
    sqlx::raw_sql(AssertSqlSafe(sql.as_str()))
        .execute(&mut *connection)
        .await?;
    let after = describe(connection, schema, table, registry).await?;
    if before != after {
        return Err(Changed);
    }
    Ok(after)
}

async fn registration(
    connection: &mut PgConnection,
    registry: &str,
    source: RegisteredSource<'_>,
) -> Result<Relation, SourceReadError> {
    let proof = lock(connection, registry, "mrsm_pipes", true).await?;
    let kind = |name| proof.columns.get(name).copied();
    let is_text = |kind| matches!(kind, Some(25 | 1043));
    if !matches!(kind("pipe_id"), Some(21 | 23 | 20))
        || !is_text(kind("connector_keys"))
        || !is_text(kind("metric_key"))
        || !is_text(kind("location_key"))
        || !matches!(kind("parameters"), Some(25 | 1043 | 114 | 3802))
    {
        return Err(Unsupported);
    }
    let statement = format!(
        "SELECT CASE WHEN pg_catalog.octet_length(parameters::text) <= {MAX_METADATA_BYTES}
                      THEN parameters::text END
           FROM ONLY {}
          WHERE pipe_id = $1 AND connector_keys = $2 AND metric_key = $3
            AND location_key IS NOT DISTINCT FROM $4 LIMIT 2",
        relation(registry, "mrsm_pipes")
    );
    let rows = sqlx::query_scalar::<_, Option<String>>(AssertSqlSafe(statement.as_str()))
        .bind(source.registration)
        .bind(source.pipe.connector_keys())
        .bind(source.pipe.metric_key())
        .bind(source.pipe.location_key())
        .persistent(false)
        .fetch_all(connection)
        .await?;
    let [Some(parameters)] = rows.as_slice() else {
        return Err(Changed);
    };
    if Value::from_json_str(parameters)
        .ok()
        .and_then(|v| v.as_map().cloned())
        .as_ref()
        != Some(source.pipe.raw_parameters())
    {
        return Err(Changed);
    }
    Ok(proof)
}

fn selectable(kind: i64) -> bool {
    // Built-in scalar types handled by the existing PostgreSQL Arrow decoder.
    matches!(
        kind,
        16 | 17
            | 20
            | 21
            | 23
            | 25
            | 114
            | 700
            | 701
            | 1043
            | 1082
            | 1083
            | 1114
            | 1184
            | 1700
            | 2950
            | 3802
    )
}

fn filter_param(value: &Value, kind: i64) -> Result<SqlParam, SourceReadError> {
    Ok(match value {
        Value::Int(i) if matches!(kind, 20 | 21 | 23) => SqlParam::Int(*i),
        Value::Str(s) if matches!(kind, 25 | 1043) => SqlParam::Text(s.clone()),
        Value::Bool(b) if kind == 16 => SqlParam::Bool(*b),
        Value::Float(f) if matches!(kind, 700 | 701) && f.is_finite() => SqlParam::Float(*f),
        _ => return Err(Unsupported),
    })
}

fn statement(
    source: RegisteredSource<'_>,
    relation_proof: &Relation,
    query: &DataQuery,
    row_bytes: usize,
) -> Result<(String, Vec<SqlParam>, usize), SourceReadError> {
    if query.as_chunks || query.remote {
        return Err(Unsupported);
    }
    let columns = &relation_proof.columns;
    let mut selected: Vec<&String> = match &query.select_columns {
        Some(names) if !names.is_empty() && !names.iter().any(|name| name == "*") => {
            names.iter().collect()
        }
        _ => columns.keys().collect(),
    };
    if let Some(omit) = &query.omit_columns {
        selected.retain(|column| !omit.contains(column));
    }
    let mut unique = std::collections::BTreeSet::new();
    if selected.is_empty() || selected.iter().any(|column| !unique.insert(*column)) {
        return Err(Unsupported);
    }
    let mut names = Vec::new();
    let mut widths = Vec::new();
    for column in selected {
        let kind = *columns.get(column).ok_or(Unsupported)?;
        if !selectable(kind) {
            return Err(Unsupported);
        }
        let ident = identifier(column);
        let name = format!("src.{ident}");
        let width = match kind {
            // Bound the actual wire bytes even for compressed values.
            17 | 25 | 1043 => format!("coalesce(pg_catalog.octet_length({name}),0)::int8"),
            114 | 1700 | 3802 => {
                format!("coalesce(pg_catalog.octet_length({name}::text),0)::int8 + 32")
            }
            _ => "32".to_string(),
        };
        widths.push(format!("({width} + {})", column.len() + 8));
        names.push(format!("CASE WHEN wire.ok THEN {name} END AS {ident}"));
    }
    let width = names.len();
    names.push("wire.ok".into());
    let mut conditions = Vec::new();
    let mut params = Vec::new();
    if let Some(filters) = &query.params {
        for (column, value) in filters {
            let kind = *columns.get(column).ok_or(Unsupported)?;
            if !selectable(kind) {
                return Err(Unsupported);
            }
            let column = identifier(column);
            if value.is_null() {
                conditions.push(format!("src.{column} IS NULL"));
            } else {
                params.push(filter_param(value, kind)?);
                conditions.push(format!("src.{column} = ${}", params.len()));
            }
        }
    }
    let axis = source
        .pipe
        .column("datetime")
        .or_else(|| source.pipe.column("primary"))
        .or_else(|| source.pipe.column("id"));
    if query.begin.is_some() || query.end.is_some() || query.order.is_some() {
        let axis = axis.as_ref().ok_or(Unsupported)?;
        let kind = *columns.get(axis).ok_or(Unsupported)?;
        if !selectable(kind) {
            return Err(Unsupported);
        }
        let column = identifier(axis);
        for (op, bound) in [(">=", &query.begin), ("<", &query.end)] {
            let Some(bound) = bound else { continue };
            let cast = match bound {
                DateBound::Int(i) if matches!(kind, 20 | 21 | 23) => {
                    params.push(SqlParam::Int(*i));
                    ""
                }
                DateBound::Datetime(value) if matches!(kind, 1114 | 1184) => {
                    params.push(SqlParam::Text(value.isoformat()));
                    if kind == 1114 {
                        "::timestamp"
                    } else {
                        "::timestamptz"
                    }
                }
                _ => return Err(Unsupported),
            };
            conditions.push(format!("src.{column} {op} ${}{cast}", params.len()));
        }
    }
    let mut sql = format!(
        "SELECT {} FROM ONLY {} AS src CROSS JOIN LATERAL (SELECT ({}) <= {row_bytes} AS ok OFFSET 0) AS wire",
        names.join(", "),
        relation(source.schema, source.table),
        widths.join(" + ")
    );
    if !conditions.is_empty() {
        sql.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
    }
    if let Some(order) = &query.order {
        let order = match order.as_str() {
            "asc" => "ASC",
            "desc" => "DESC",
            _ => return Err(Unsupported),
        };
        let column = identifier(axis.as_deref().ok_or(Unsupported)?);
        sql.push_str(&format!(" ORDER BY src.{column} {order}"));
    }
    if let Some(limit) = query.limit {
        if limit < 0 {
            return Err(Unsupported);
        }
        params.push(SqlParam::Int(limit));
        sql.push_str(&format!(" LIMIT ${}", params.len()));
    }
    Ok((sql, params, width))
}

fn emit(
    rows: &[PgRow],
    state: &mut Option<backend::StreamState>,
    width: usize,
    batch: &mut impl FnMut(Frame) -> Result<(), SourceReadError>,
) -> Result<(), SourceReadError> {
    let mut arrow = backend::make_batch::<PgBackend>(rows, state)?;
    arrow.remove_column(width);
    batch(Frame::from_record_batches(&[arrow]).map_err(|_| Unavailable)?)
}

async fn stream(
    connection: &mut PgConnection,
    (sql, params, width): (String, Vec<SqlParam>, usize),
    budget: SourceReadBudget<'_>,
    mut batch: impl FnMut(Frame) -> Result<(), SourceReadError>,
) -> Result<(), SourceReadError> {
    let mut rows = backend::bound::<PgBackend>(&sql, &params).fetch(connection);
    let mut buffer = Vec::new();
    let mut bytes = 0usize;
    let mut state = None;
    while let Some(row) = rows.try_next().await? {
        if budget.cancelled.load(Ordering::Relaxed) || Instant::now() >= budget.deadline {
            return Err(Unavailable);
        }
        if !row.try_get::<bool, _>(width)? {
            return Err(TooLarge);
        }
        let mut size = 0usize;
        for index in 0..width {
            use sqlx::Column;
            let raw = row.try_get_raw(index)?;
            let len = if raw.is_null() {
                0
            } else {
                raw.as_bytes().map_err(|_| Unavailable)?.len()
            };
            size = size.saturating_add(len + row.columns()[index].name().len() + 8);
        }
        if size > budget.batch_bytes {
            return Err(TooLarge);
        }
        if !buffer.is_empty()
            && (bytes.saturating_add(size) > budget.batch_bytes
                || buffer.len() >= budget.batch_rows)
        {
            emit(&buffer, &mut state, width, &mut batch)?;
            buffer.clear();
            bytes = 0;
        }
        buffer.push(row);
        bytes += size;
    }
    if !buffer.is_empty() {
        emit(&buffer, &mut state, width, &mut batch)?;
    }
    Ok(())
}

pub(crate) async fn read<F>(
    spec: &ConnectionSpec,
    registry: &str,
    source: RegisteredSource<'_>,
    query: Option<&DataQuery>,
    budget: SourceReadBudget<'_>,
    mut batch: F,
) -> Result<String, SourceReadError>
where
    F: FnMut(Frame) -> Result<(), SourceReadError> + Send,
{
    if ![registry, source.schema, source.table]
        .into_iter()
        .all(normal_name)
        || ["pg_", "information_schema", "_mrsm_internal"]
            .iter()
            .any(|name| source.schema.to_ascii_lowercase().starts_with(name))
        || budget.batch_rows == 0
        || budget.batch_rows > 2048
        || budget.batch_bytes == 0
        || budget.batch_bytes > 8 * 1024 * 1024
        || budget.deadline.saturating_duration_since(Instant::now()) > Duration::from_secs(20)
    {
        return Err(Unsupported);
    }
    tokio::time::timeout_at(budget.deadline.into(), async {
        if budget.cancelled.load(Ordering::Relaxed) {
            return Err(Unavailable);
        }
        let options = super::pg_connect_options(spec)?;
        // A dedicated connection starts with the operator's role and cannot inherit a pooled
        // caller's temporary objects, prepared statements or session settings.
        let mut connection = PgConnection::connect_with(&options).await?;
        let mut tx = connection.begin_with("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY").await?;
        let work = async {
            let ms = budget.deadline.saturating_duration_since(Instant::now()).as_millis().max(1);
            let settings = format!(
                "SET LOCAL search_path = pg_catalog; SET LOCAL row_security = off; SET LOCAL client_encoding = 'UTF8'; SET LOCAL timezone = 'UTC'; SET LOCAL datestyle = 'ISO, YMD'; SET LOCAL constraint_exclusion = off; SET LOCAL statement_timeout = {ms};"
            );
            sqlx::raw_sql(AssertSqlSafe(settings.as_str())).execute(&mut *tx).await?;
            let encoding: String = sqlx::query_scalar("SHOW server_encoding")
                .persistent(false)
                .fetch_one(&mut *tx)
                .await?;
            if encoding != "UTF8" {
                return Err(Unsupported);
            }
            let registry_proof = registration(&mut tx, registry, source).await?;
            let table = lock(&mut tx, source.schema, source.table, false).await?;
            if let Some(query) = query {
                // The snapshot read starts only after the relation proof. No other relations,
                // definitions, metadata expressions, or caller-supplied statements are executed.
                let statement = statement(source, &table, query, budget.batch_bytes)?;
                stream(&mut tx, statement, budget, &mut batch).await?;
            }
            Ok(format!("{}:{table:?}", registry_proof.oid))
        }.await;
        tx.rollback().await?;
        work
    }).await.map_err(|_| Unavailable)?
}
