//! Additive entry points for the Python façade (`crates/meerschaum-py`, design §4.4).
//!
//! The Python `Pipe` keeps its in-memory parameters and its own caches, so the bindings need a
//! few things the instance methods compute internally: the parameters `sync_pipe` mutates
//! (`autoincrement`, numeric `dtypes`), and the table state a read used (to restore v4's pandas
//! dtypes). These wrappers expose exactly that, without changing any instance method.

use std::time::Instant;

use indexmap::IndexMap;
use meerschaum_connector::{ConnectorResult, DataQuery, DateBound, SyncOptions};
use meerschaum_core::{Map, Pipe, SyncResult};
use meerschaum_kernel::Frame;

use crate::ident::{sql_item_name, truncate_item_name};
use crate::instance::SqlConnector;
use crate::instance::data::{DataQueryOptions, PipeState, pipe_dtypes};
use crate::instance::sync::{InternalSync, ToSqlExtras};
use crate::pipe::SqlPipe;

/// How `SQLConnector.to_sql()` treats a table that already exists (`if_exists`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IfExists {
    /// Refuse (`Table '...' already exists.`).
    Fail,
    /// Drop it and create it again from the frame.
    Replace,
    /// Insert into it as it is.
    Append,
}

impl std::str::FromStr for IfExists {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "fail" => Ok(IfExists::Fail),
            "replace" => Ok(IfExists::Replace),
            "append" => Ok(IfExists::Append),
            other => Err(format!("'{other}' is not valid for if_exists")),
        }
    }
}

/// The `CREATE TABLE` of a `to_sql` table: one column per frame column, typed from
/// `column_types` (`TEXT` for a column without a type), in the frame's order.
pub fn to_sql_create_table_query(
    table: &str,
    flavor: crate::Flavor,
    schema: Option<&str>,
    columns: &[String],
    column_types: &IndexMap<String, String>,
) -> String {
    let cols: Vec<String> = columns
        .iter()
        .map(|c| {
            let typ = column_types
                .get(c)
                .map(String::as_str)
                .filter(|t| !t.trim().is_empty())
                .unwrap_or("TEXT");
            format!("{} {typ}", sql_item_name(c, flavor, None))
        })
        .collect();
    format!(
        "CREATE TABLE {} (\n    {}\n)",
        sql_item_name(table, flavor, schema),
        cols.join(",\n    ")
    )
}

/// `MRSM_CORE_TRACE=1`: per-stage timings on stderr (same format as the bindings' tracer).
pub(crate) fn trace(stage: &str, start: Instant) {
    if std::env::var_os("MRSM_CORE_TRACE").is_some() {
        eprintln!(
            "[trace] {:<50} {:9.2} ms",
            stage,
            start.elapsed().as_secs_f64() * 1e3
        );
    }
}

impl SqlConnector {
    /// The table state of `sp`, from `cached` columns types when the caller has them.
    ///
    /// v4's `Pipe` caches `get_columns_types()` (`pipes:dtypes:columns_types_cache_seconds`,
    /// 60 s by default) and reads its dtypes from that cache, so the façade passes the Python
    /// pipe's cached columns types instead of querying the table's columns on every call
    /// (`pipe_state` runs two catalog queries). An empty cache means "unknown", as in v4.
    pub async fn pipe_state_cached(
        &self,
        sp: &SqlPipe,
        cached: Option<IndexMap<String, String>>,
    ) -> PipeState {
        match cached {
            Some(cols_types) if !cols_types.is_empty() => PipeState {
                exists: true,
                dtypes: pipe_dtypes(sp, &cols_types, true),
                explicit_dtypes: pipe_dtypes(sp, &cols_types, false),
                cols_types,
            },
            _ => {
                let start = Instant::now();
                let state = self.pipe_state(sp).await;
                trace("sql: pipe_state", start);
                state
            }
        }
    }

    /// [`pipe_state_cached`](SqlConnector::pipe_state_cached) for the reads `Pipe` methods
    /// make: without cached columns types, a database that cannot be asked whether the table
    /// exists is an error ([`pipe_state_checked`](SqlConnector::pipe_state_checked)).
    pub async fn pipe_state_cached_checked(
        &self,
        sp: &SqlPipe,
        cached: Option<IndexMap<String, String>>,
    ) -> ConnectorResult<PipeState> {
        match cached.filter(|c| !c.is_empty()) {
            Some(c) => Ok(self.pipe_state_cached(sp, Some(c)).await),
            None => {
                let start = Instant::now();
                let state = self.pipe_state_checked(sp).await;
                trace("sql: pipe_state", start);
                state
            }
        }
    }

    /// [`get_sync_time`](SqlConnector::get_sync_time) with the caller's cached columns types
    /// (see [`pipe_state_cached`](SqlConnector::pipe_state_cached)). Database errors are
    /// returned (`Pipe.get_sync_time()` raises them).
    pub async fn get_sync_time_cached(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
        remote: bool,
        cached: Option<IndexMap<String, String>>,
    ) -> ConnectorResult<Option<DateBound>> {
        if remote {
            return self.get_sync_time(pipe, params, newest, remote).await;
        }
        let pipe = &self.prepare_pipe(pipe).await;
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state_cached_checked(&sp, cached).await?;
        self.sync_time_checked(&sp, &state, params, newest).await
    }

    /// [`get_pipe_rowcount`](SqlConnector::get_pipe_rowcount) with the caller's cached columns
    /// types (see [`pipe_state_cached`](SqlConnector::pipe_state_cached)).
    pub async fn get_pipe_rowcount_cached(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
        cached: Option<IndexMap<String, String>>,
    ) -> ConnectorResult<Option<i64>> {
        if query.remote {
            return self.get_pipe_rowcount(pipe, query).await;
        }
        let pipe = &self.prepare_pipe(pipe).await;
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state_cached_checked(&sp, cached).await?;
        let Some(sql) = self.get_pipe_rowcount_query(&sp, &state, query)? else {
            return Ok(None);
        };
        // Without bounds or `params` the text only depends on the table: prepared once per
        // pooled connection on PostgreSQL (see `SqlConnector::sync_time`).
        let stable = query.begin.is_none()
            && query.end.is_none()
            && query.params.as_ref().is_none_or(|p| p.is_empty());
        let value = if stable {
            self.value_catalog_checked(&sql).await?
        } else {
            self.value_checked(&sql).await?
        };
        Ok(value.and_then(|v| v.as_i64()))
    }

    /// [`sync_pipe_result`](SqlConnector::sync_pipe_result), updating `pipe`'s in-memory
    /// parameters the way v4's `sync_pipe` mutates the Python `Pipe` (`autoincrement`, the
    /// numeric `dtypes` of `get_alter_columns_queries`).
    pub async fn sync_pipe_mut(
        &self,
        pipe: &mut Pipe,
        df: Frame,
        opts: &SyncOptions,
    ) -> SyncResult {
        self.sync_pipe_mut_cached(pipe, df, opts, None).await
    }

    /// Sync using table metadata already cached by the caller. Schema changes refresh it.
    ///
    /// Cross-pipe `{{ Pipe() }}` templates resolve through [`prepare_pipe`](SqlConnector::prepare_pipe)
    /// on every call (never from a snapshot the caller built). The sync runs on the resolved
    /// copy; only the *raw* top-level parameters it changed (`autoincrement`, numeric `dtypes`,
    /// new special `dtypes`) are applied to the caller's `pipe`, so templates are never replaced
    /// by their resolved text.
    pub async fn sync_pipe_mut_cached(
        &self,
        pipe: &mut Pipe,
        df: Frame,
        opts: &SyncOptions,
        cached: Option<IndexMap<String, String>>,
    ) -> SyncResult {
        let mut resolved = self.prepare_pipe(pipe).await;
        let before = resolved.raw_parameters().clone();
        let cached_state = match cached.filter(|c| !c.is_empty()) {
            Some(c) => match self.sql_pipe(&resolved) {
                Ok(sp) => Some(self.pipe_state_cached(&sp, Some(c)).await),
                Err(e) => return SyncResult::new(false, e.to_string()),
            },
            None => None,
        };
        let internal = InternalSync {
            cached_state,
            new_special_cols: opts
                .extra
                .get("new_special_cols")
                .and_then(|v| v.as_map())
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.py_str())).collect()),
            ..Default::default()
        };
        let result = self.sync_pipe_impl(&mut resolved, df, opts, internal).await;
        let patch: Map = resolved
            .raw_parameters()
            .iter()
            .filter(|(k, v)| before.get(*k) != Some(*v))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if !patch.is_empty() {
            pipe.update_parameters(patch);
        }
        result
    }

    /// [`get_pipe_data`](SqlConnector::get_pipe_data), also returning the table state the read
    /// used (its `dtypes` are the pandas dtypes v4's `read(dtype=...)` applies).
    ///
    /// `cached`: the caller's cached columns types (see
    /// [`pipe_state_cached`](SqlConnector::pipe_state_cached)).
    ///
    /// `no_order` is v4's `order=None`: no `ORDER BY` and no sort (the rows come back in the
    /// database's order), where `get_pipe_data` treats a missing order as `'asc'`. The implementation is shared with `get_pipe_data_with_state`.
    pub async fn get_pipe_data_and_state(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
        no_order: bool,
        cached: Option<IndexMap<String, String>>,
    ) -> ConnectorResult<(Option<Frame>, PipeState)> {
        let opts = DataQueryOptions {
            no_order,
            ..Default::default()
        };
        self.get_pipe_data_and_state_with(pipe, query, &opts, cached)
            .await
    }

    /// [`get_pipe_data_and_state`](SqlConnector::get_pipe_data_and_state) with the keywords
    /// `SQLConnector.get_pipe_data()` passes on to `get_pipe_data_query` (`begin=''`,
    /// `begin_add_minutes`, `end_add_minutes`, `replace_nulls`, `skip_existing_cols_check`).
    pub async fn get_pipe_data_and_state_with(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
        opts: &DataQueryOptions,
        cached: Option<IndexMap<String, String>>,
    ) -> ConnectorResult<(Option<Frame>, PipeState)> {
        let pipe = &self.prepare_pipe(pipe).await;
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state_cached_checked(&sp, cached).await?;
        if !state.exists {
            return Ok((None, state));
        }
        let df = self
            .get_pipe_data_with_state(&sp, &state, query, opts)
            .await?;
        Ok((df, state))
    }

    /// `SQLConnector.to_sql(df, name, if_exists=...)` without SQLAlchemy (rc15 Q53): the table is
    /// created from `column_types` (the database type of each column, as pandas' `to_sql` would
    /// create it) when it does not exist or when `if_exists` is [`IfExists::Replace`] (dropped
    /// first), [`IfExists::Fail`] refuses an existing table, and the rows are inserted with
    /// [`to_sql`](SqlConnector::to_sql)'s encodings (PostgreSQL `COPY` / MSSQL `OPENJSON` when
    /// bulk inserts are enabled). Creating, dropping and inserting run in one transaction where
    /// the database allows DDL in transactions. `index_columns` get an index each
    /// (`ix_<table>_<column>`, pandas' `index=True`). Returns `(success, error message)`.
    #[allow(clippy::too_many_arguments)]
    pub async fn frame_to_sql(
        &self,
        df: &Frame,
        name: &str,
        schema: Option<&str>,
        if_exists: IfExists,
        column_types: &IndexMap<String, String>,
        index_columns: &[String],
        chunksize: Option<i64>,
    ) -> (bool, String) {
        let flavor = self.flavor();
        let schema = schema.or(self.schema()).map(str::to_string);
        let schema = if flavor.no_schema() { None } else { schema };
        let truncated = truncate_item_name(name, flavor);
        let exists = match self
            .table_exists_checked(&truncated, schema.as_deref())
            .await
        {
            Ok(exists) => exists,
            Err(e) => return (false, e.to_string()),
        };
        let mut prelude = Vec::new();
        if exists {
            match if_exists {
                IfExists::Fail => return (false, format!("Table '{name}' already exists.")),
                IfExists::Append => {}
                IfExists::Replace => prelude.push(
                    crate::queries::table::get_drop_table_queries(
                        &truncated,
                        flavor,
                        schema.as_deref(),
                    )
                    .remove(0),
                ),
            }
        }
        if !exists || if_exists == IfExists::Replace {
            prelude.push(to_sql_create_table_query(
                &truncated,
                flavor,
                schema.as_deref(),
                &df.names(),
                column_types,
            ));
            for col in index_columns {
                let index_name = truncate_item_name(&format!("ix_{truncated}_{col}"), flavor);
                prelude.push(format!(
                    "CREATE INDEX {} ON {} ({})",
                    sql_item_name(&index_name, flavor, None),
                    sql_item_name(&truncated, flavor, schema.as_deref()),
                    sql_item_name(col, flavor, None),
                ));
            }
        }
        let extras = ToSqlExtras {
            prelude,
            ..Default::default()
        };
        let (ok, msg, _) = self
            .to_sql_inner(
                df,
                &truncated,
                schema.as_deref(),
                column_types,
                chunksize,
                &extras,
            )
            .await;
        (ok, msg)
    }
}

#[cfg(test)]
mod to_sql_tests {
    use super::*;

    #[test]
    fn if_exists_parses_pandas_values() {
        assert_eq!("replace".parse::<IfExists>(), Ok(IfExists::Replace));
        assert_eq!("append".parse::<IfExists>(), Ok(IfExists::Append));
        assert_eq!("fail".parse::<IfExists>(), Ok(IfExists::Fail));
        assert!("truncate".parse::<IfExists>().is_err());
    }

    #[test]
    fn create_table_types_each_column() {
        let types: IndexMap<String, String> = [("a", "BIGINT"), ("b", "TEXT")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let q = to_sql_create_table_query(
            "t",
            crate::Flavor::Postgresql,
            Some("public"),
            &["a".to_string(), "b".to_string(), "c".to_string()],
            &types,
        );
        assert_eq!(
            q,
            "CREATE TABLE \"public\".\"t\" (\n    \"a\" BIGINT,\n    \"b\" TEXT,\n    \"c\" TEXT\n)"
        );
    }
}
