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

use crate::instance::SqlConnector;
use crate::instance::data::{DataQueryOptions, PipeState, pipe_dtypes};
use crate::instance::sync::InternalSync;
use crate::pipe::SqlPipe;

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

    /// [`get_sync_time`](SqlConnector::get_sync_time) with the caller's cached columns types
    /// (see [`pipe_state_cached`](SqlConnector::pipe_state_cached)).
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
        let state = self.pipe_state_cached(&sp, cached).await;
        Ok(self.sync_time(&sp, &state, params, newest).await)
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
        let state = self.pipe_state_cached(&sp, cached).await;
        let Some(sql) = self.get_pipe_rowcount_query(&sp, &state, query)? else {
            return Ok(None);
        };
        // Without bounds or `params` the text only depends on the table: prepared once per
        // pooled connection on PostgreSQL (see `SqlConnector::sync_time`).
        let stable = query.begin.is_none()
            && query.end.is_none()
            && query.params.as_ref().is_none_or(|p| p.is_empty());
        let value = if stable {
            self.value_catalog(&sql).await
        } else {
            self.value(&sql).await
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
        let state = self.pipe_state_cached(&sp, cached).await;
        if !state.exists {
            return Ok((None, state));
        }
        let df = self
            .get_pipe_data_with_state(&sp, &state, query, opts)
            .await?;
        Ok((df, state))
    }
}
