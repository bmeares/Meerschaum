//! The `Pipe` methods that wrap an instance connector (`get_data`, `get_backtrack_data`,
//! `get_sync_time`, `get_rowcount`, `infer_dtypes`, `enforce_dtypes`, `filter_existing`,
//! `deduplicate`, `fetch`; design §4.4, D14), as v4 wrote them in `core/Pipe/_data.py`,
//! `_dtypes.py`, `_fetch.py`, `_deduplicate.py` and `_sync.py`, over the connector traits.
//!
//! The Python `Pipe` keeps signatures, docstrings and argument normalization; the algorithms
//! live here ([`crate::pipe_methods_api`] binds them). A [`Reader`] is everything one call
//! needs: the pipe's instance (a Rust `SqlConnector`, an `api:` / `valkey:` client, or a
//! Python connector behind the worker adapter, D12), the configuration values the v4 bodies read
//! (`pipes:parameters:fetch:backtrack_minutes`, `pipes:sync:filter_params_index_limit`, the
//! pipe defaults), and the Python pipe's cached columns types (v4 caches `get_columns_types()`
//! on the `Pipe`; passing the cache avoids re-reading the table's columns on every call).
//!
//! **SQL instances** run the reads that already carry the whole `Pipe.get_data()` post-processing
//! (column selection, null-filled missing columns, dtype enforcement, sorting, `limit`) in
//! `meerschaum-connector-sql` (`get_pipe_data_and_state`); every other instance goes through the generic
//! port of the v4 body here, which does that post-processing over the `InstanceConnector`
//! trait.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{Duration, TimeDelta};
use indexmap::IndexMap;
use meerschaum_connector::pipe_ops::{self, Doc};
use meerschaum_connector::{ConnectorError, DataQuery, DateBound, FetchedKind, InstanceConnector};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::instance::data::{ChunkBounds, DataQueryOptions, get_chunk_bounds};
use meerschaum_core::pipe::{ChunkInterval, ChunkIntervalArg, PipeDefaults};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_engine::ConnectorRegistry;
use meerschaum_engine::sync::{SyncArgs, SyncBegin, SyncEnv, determine_begin, parse_date_bound};
use meerschaum_kernel::cast::{RoundTo, round_time};
use meerschaum_kernel::diff::{BacktrackRequest, FilterExistingParams, filter_existing};
use meerschaum_kernel::enforce::{
    PipeDtypesContext, PipeInput, pipe_enforce_dtypes, pipe_get_dtypes, pipe_is_tz_aware,
};
use meerschaum_kernel::value::DateTimeValue;
use meerschaum_kernel::{Frame, Value as Cell};

/// What a call needs of the Python process (module docs).
#[derive(Clone)]
pub struct Reader {
    /// The pipe's instance connector.
    pub instance: Arc<dyn InstanceConnector>,
    /// The instance, when it is a Rust SQL connector.
    pub sql: Option<Arc<SqlConnector>>,
    /// The Rust SQL connectors the Python process holds, by full keys (a SQL source answers
    /// `remote=True` reads).
    pub sources: HashMap<String, Arc<SqlConnector>>,
    /// Connector lookup (the instance and, for `fetch`, the source).
    pub registry: Arc<dyn ConnectorRegistry>,
    /// The pipe defaults of the live config.
    pub defaults: PipeDefaults,
    /// `pipes:parameters:fetch:backtrack_minutes`.
    pub backtrack_minutes: i64,
    /// `pipes:sync:filter_params_index_limit`.
    pub filter_limit: usize,
    /// The Python pipe's cached columns types (`None`: ask the instance).
    pub columns_types: Option<IndexMap<String, String>>,
}

/// The arguments of [`Reader::get_data`].
#[derive(Clone, Debug, Default)]
pub struct GetDataArgs {
    /// `select_columns`.
    pub select_columns: Option<Vec<String>>,
    /// `omit_columns`.
    pub omit_columns: Option<Vec<String>>,
    /// `begin` (parsed).
    pub begin: Option<DateBound>,
    /// `end` (parsed).
    pub end: Option<DateBound>,
    /// `params`.
    pub params: Option<Map>,
    /// `as_docs`.
    pub as_docs: bool,
    /// `add_missing_columns`.
    pub add_missing_columns: bool,
    /// `order` (`None`: do not sort).
    pub order: Option<String>,
    /// `limit`.
    pub limit: Option<i64>,
    /// What a SQL instance reads from the keywords (`begin=''`, `begin_add_minutes`,
    /// `end_add_minutes`, `replace_nulls`, `skip_existing_cols_check`).
    pub sql_options: DataQueryOptions,
    /// The keywords `Pipe.get_data(**kw)` forwarded: a custom instance's methods receive them.
    pub extra: Map,
}

/// What [`Reader::get_data`] found.
pub enum Data {
    /// The pipe's table does not exist (`None`, or `[]` for documents).
    Missing,
    /// Documents.
    Docs(Vec<Doc>),
    /// SQL documents retaining original native wide integer columns for Python output.
    TypedDocs(Vec<Doc>, HashSet<String>),
    /// A frame and the pipe dtypes the pandas / Polars conversion needs.
    Frame(Frame, IndexMap<String, String>),
}

/// What [`Reader::fetch`] got from the source.
pub enum Fetched {
    /// Nothing was fetched.
    Nothing,
    /// One frame (a plugin's or an API's `fetch()`).
    Frame(Frame),
    /// A SQL source's rows in chunks of `system:connectors:sql:chunksize` (v4 reads them with
    /// `read(as_iterator=True)`), or the chunks of a generator `fetch()`.
    Chunks(Vec<Frame>),
    /// What a chunk hook returned for each chunk (`fetch(sync_chunks=True)` / `chunk_hook`).
    Hooked(Vec<HookOutcome>),
    /// A source that returned `True` (it synced the data itself, in parallel).
    True,
}

/// What a chunk hook returned.
pub enum HookOutcome {
    /// A `SuccessTuple` (the native sync of the chunk).
    Tuple(SuccessTuple),
    /// What the caller's `chunk_hook` returned.
    Python(pyo3::Py<pyo3::PyAny>),
}

impl HookOutcome {
    /// Whether the hook reported a failure (`isinstance(result, tuple) and result[0] is False`).
    pub fn failed(&self) -> bool {
        use pyo3::prelude::*;
        match self {
            HookOutcome::Tuple(t) => !t.success,
            HookOutcome::Python(object) => Python::attach(|py| {
                object
                    .bind(py)
                    .cast::<pyo3::types::PyTuple>()
                    .ok()
                    .and_then(|t| t.get_item(0).ok())
                    .is_some_and(|first| {
                        first.is_instance_of::<pyo3::types::PyBool>()
                            && !first.is_truthy().unwrap_or(true)
                    })
            }),
        }
    }
}

/// A caller's `chunk_hook`, run on a chunk (it takes the GIL itself).
pub type ChunkHook = Arc<dyn Fn(Frame) -> Result<HookOutcome, String> + Send + Sync>;

/// What `Pipe.fetch()` does with the chunks of its source.
#[derive(Clone)]
pub enum FetchMode {
    /// Return the data.
    Plain,
    /// `sync_chunks=True`: sync every chunk as it arrives (`Pipe.sync` with the chunk's label in
    /// front of the message) and return the results.
    SyncChunks,
    /// `chunk_hook=callable`.
    Hook(ChunkHook),
}

/// How the chunks of an iterated read are laid out.
pub enum ChunkPlan {
    /// The pipe does not exist: no chunks.
    Empty,
    /// One read with the caller's bounds.
    Single,
    /// One read per bounds.
    Bounds(Vec<ChunkBounds>),
}

/// The arguments of [`Reader::filter_existing`].
#[derive(Clone, Debug, Default)]
pub struct FilterOpts {
    /// `date_bound_only`.
    pub date_bound_only: bool,
    /// `include_unchanged_columns`.
    pub include_unchanged_columns: bool,
    /// `enforce_dtypes`.
    pub enforce_dtypes: bool,
}

/// What [`Reader::filter_existing`] computed.
pub struct FilterOut {
    /// `(unseen, update, delta)`.
    pub frames: (Frame, Frame, Frame),
}

/// The arguments of [`Reader::deduplicate`].
#[derive(Clone, Debug, Default)]
pub struct DedupArgs {
    /// `begin`.
    pub begin: Option<DateBound>,
    /// `end`.
    pub end: Option<DateBound>,
    /// `params`.
    pub params: Option<Map>,
    /// `chunk_interval`.
    pub chunk_interval: Option<ChunkIntervalArg>,
    /// `bounded` (`None`: bounded).
    pub bounded: Option<bool>,
    /// `_use_instance_method`.
    pub use_instance_method: bool,
    /// The keywords `Pipe.deduplicate(**kwargs)` forwarded to the instance's own
    /// `deduplicate_pipe()` (v4's chunk loop passes them on to nothing).
    pub extra: Map,
    /// How many chunks are processed at once (`Pipe.get_num_workers()`, at least 1).
    pub workers: usize,
}

/// The message `InstanceConnector::deduplicate_pipe` gives an instance without a native
/// implementation (the trait default and the worker adapter).
const DEDUP_UNSUPPORTED: &str = "Deduplication is not supported for instance connectors of type";

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

/// A pipe whose `dtypes` parameter is the inferred dtypes (v4's `Pipe.dtypes`), which the
/// chunk interval and backtrack interval read to tell an integer axis.
fn with_dtypes(pipe: &Pipe, dtypes: &IndexMap<String, String>) -> Pipe {
    let mut patched = pipe.clone();
    let map: Map = dtypes
        .iter()
        .map(|(c, t)| (c.clone(), Value::Str(t.clone())))
        .collect();
    let mut patch = Map::new();
    patch.insert("dtypes".into(), Value::Map(map));
    patched.update_parameters(patch);
    patched
}

fn dt_col(pipe: &Pipe) -> Option<String> {
    pipe.column("datetime").filter(|c| !c.is_empty())
}

/// Python's `str()` of a bound (`None` for no bound).
fn bound_str(b: &Option<DateBound>) -> String {
    b.as_ref()
        .map(DateBound::py_str)
        .unwrap_or_else(|| "None".into())
}

fn bound_lt(a: &DateBound, b: &DateBound) -> bool {
    match (a, b) {
        (DateBound::Datetime(x), DateBound::Datetime(y)) => x.utc < y.utc,
        (DateBound::Int(x), DateBound::Int(y)) => x < y,
        _ => false,
    }
}

fn add_interval(b: &DateBound, interval: &ChunkInterval) -> Option<DateBound> {
    match (b, interval) {
        (DateBound::Datetime(dt), ChunkInterval::Delta(d)) => {
            Some(DateBound::Datetime(DateTimeValue {
                utc: dt.utc.checked_add_signed(*d)?,
                tz: dt.tz.clone(),
            }))
        }
        (DateBound::Int(i), ChunkInterval::Int(w)) => Some(DateBound::Int(i.checked_add(*w)?)),
        _ => None,
    }
}

fn sub_interval(b: &DateBound, interval: &ChunkInterval) -> Option<DateBound> {
    match (b, interval) {
        (DateBound::Datetime(dt), ChunkInterval::Delta(d)) => {
            Some(DateBound::Datetime(DateTimeValue {
                utc: dt.utc.checked_sub_signed(*d)?,
                tz: dt.tz.clone(),
            }))
        }
        (DateBound::Int(i), ChunkInterval::Int(w)) => Some(DateBound::Int(i.checked_sub(*w)?)),
        _ => None,
    }
}

/// `bound + 1` past the maximum: an integer plus one, a datetime plus a minute.
fn just_past(b: DateBound) -> DateBound {
    match b {
        DateBound::Int(i) => DateBound::Int(i + 1),
        DateBound::Datetime(dt) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc + Duration::minutes(1),
            tz: dt.tz,
        }),
    }
}

impl Reader {
    /// The engine environment (the pieces of `Pipe` logic the engine shares).
    pub fn env(&self) -> SyncEnv<'_> {
        SyncEnv {
            registry: self.registry.as_ref(),
            defaults: &self.defaults,
            default_backtrack_minutes: self.backtrack_minutes,
            filter_params_index_limit: self.filter_limit,
            known: None,
        }
    }

    /// `Pipe.exists()` (without the façade's cache).
    pub async fn exists(&self, pipe: &Pipe) -> Result<bool, String> {
        match self.instance.pipe_exists(pipe).await {
            Ok(b) => Ok(b),
            Err(ConnectorError::NotImplemented(_)) => Ok(false),
            Err(e) => Err(err(e)),
        }
    }

    /// `Pipe.infer_dtypes()`: the columns types mapped to pandas dtypes (`{}` for a missing
    /// table). `refresh` ignores the Python pipe's cached columns types.
    pub async fn infer_dtypes(
        &self,
        pipe: &Pipe,
        refresh: bool,
    ) -> Result<IndexMap<String, String>, String> {
        let cached = self
            .columns_types
            .as_ref()
            .filter(|t| !refresh && !t.is_empty());
        let types = match cached {
            Some(t) => t.clone(),
            None => {
                if !self.exists(pipe).await? {
                    return Ok(IndexMap::new());
                }
                match self.instance.get_pipe_columns_types(pipe).await {
                    Ok(t) => t,
                    Err(ConnectorError::NotImplemented(_)) => IndexMap::new(),
                    Err(e) => return Err(err(e)),
                }
            }
        };
        Ok(types
            .into_iter()
            .map(|(c, t)| {
                let pd = if meerschaum_core::pyfmt::py_isupper(&t) {
                    meerschaum_core::dtypes::get_pd_type_from_db_type(&t, true)
                } else {
                    meerschaum_kernel::dtypes::to_pandas_dtype(&t)
                };
                (c, pd)
            })
            .collect())
    }

    /// `Pipe.get_dtypes(infer, refresh)`.
    pub async fn get_dtypes(
        &self,
        pipe: &Pipe,
        infer: bool,
        refresh: bool,
    ) -> Result<IndexMap<String, String>, String> {
        let remote = if infer {
            self.infer_dtypes(pipe, refresh).await?
        } else {
            IndexMap::new()
        };
        Ok(pipe_get_dtypes(
            &pipe_ops::configured_dtypes(pipe.parameters()),
            &remote,
            &pipe_ops::columns_map(pipe),
            pipe_ops::bool_param(pipe.parameters(), "autoincrement", false),
        ))
    }

    /// The context `Pipe.enforce_dtypes()` reads (`dtypes_override`: the `dtypes` argument).
    pub async fn context(
        &self,
        pipe: &Pipe,
        dtypes_override: Option<IndexMap<String, String>>,
        enforce_arg: bool,
    ) -> Result<PipeDtypesContext, String> {
        let inferred = self.get_dtypes(pipe, true, false).await?;
        let explicit = self.get_dtypes(pipe, false, false).await?;
        let parameters = pipe.parameters();
        // `Pipe.tzinfo` reads `Pipe.dtypes` (the inferred dtypes), whatever `dtypes` was passed.
        let tz_aware = pipe_is_tz_aware(
            &pipe_ops::columns_map(pipe),
            &inferred,
            pipe_ops::bool_param(parameters, "autotime", false),
            &self.defaults.autotime_column,
        );
        let dtypes = match dtypes_override {
            Some(d) if !d.is_empty() => d,
            _ => inferred,
        };
        Ok(PipeDtypesContext {
            dtypes,
            explicit_dtypes: explicit,
            enforce: enforce_arg && pipe_ops::bool_param(parameters, "enforce", true),
            tz_aware,
            mixed_numerics: pipe_ops::bool_param(parameters, "mixed_numerics", true),
        })
    }

    /// `Pipe.enforce_dtypes(df)` over a frame or documents.
    pub async fn enforce_dtypes(
        &self,
        pipe: &Pipe,
        input: PipeInput,
        dtypes: Option<IndexMap<String, String>>,
        enforce: bool,
    ) -> Result<(Frame, PipeDtypesContext), String> {
        let ctx = self.context(pipe, dtypes, enforce).await?;
        let frame = pipe_enforce_dtypes(input, &ctx).map_err(err)?;
        Ok((frame, ctx))
    }

    /// `Pipe.parse_date_bounds(bound)`: a datetime coerced to the datetime axis (an integer
    /// passes through; a datetime on an integer axis becomes epoch units).
    pub async fn parse_bound(
        &self,
        pipe: &Pipe,
        bound: Option<DateBound>,
    ) -> Result<Option<DateBound>, String> {
        match bound {
            None => Ok(None),
            Some(DateBound::Int(i)) => Ok(Some(DateBound::Int(i))),
            Some(b) => {
                let dtypes = self.get_dtypes(pipe, true, false).await?;
                parse_date_bound(&self.env(), pipe, &dtypes, b).map(Some)
            }
        }
    }

    /// The Rust SQL connector of the pipe's source (the Python process holds it), for the
    /// `remote=True` reads.
    pub fn remote_source(&self, pipe: &Pipe) -> Option<Arc<SqlConnector>> {
        self.sources
            .get(&crate::facade::full_keys(pipe.connector_keys()))
            .cloned()
    }

    /// The pipe's source as an instance-shaped connector (`api:`, `valkey:`; a `sql:` source
    /// is [`Reader::remote_source`]): their `get_sync_time` / `get_pipe_rowcount` answer the
    /// remote reads.
    fn native_source(&self, pipe: &Pipe) -> Result<Arc<dyn InstanceConnector>, ConnectorError> {
        let keys = pipe.connector_keys();
        if keys.starts_with("sql:") {
            return Err(ConnectorError::Backend(format!(
                "The SQL source of {pipe} is not a connector of this process."
            )));
        }
        self.registry
            .instance(keys)
            .map_err(ConnectorError::Backend)
    }

    /// The source's sync time (v4: `connector.get_sync_time(pipe, params=, newest=,
    /// remote=)`; the keywords the method does not take are dropped: `api:` and `valkey:`
    /// answer for their own table).
    async fn remote_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> Result<Option<DateBound>, ConnectorError> {
        if let Some(source) = self.remote_source(pipe) {
            return source
                .get_sync_time_cached(pipe, params, newest, true, None)
                .await;
        }
        if crate::facade::is_native(pipe.connector_keys()) {
            return self
                .native_source(pipe)?
                .get_sync_time(pipe, params, newest)
                .await;
        }
        match self.registry.source(pipe.connector_keys()) {
            Ok(Some(source)) => source.get_sync_time_remote(pipe, params, newest).await,
            Ok(None) => Err(ConnectorError::NotImplemented(format!(
                "{pipe} has no source connector."
            ))),
            Err(e) => Err(ConnectorError::Backend(e)),
        }
    }

    /// The source's row count of the fetch definition (v4: `connector.get_pipe_rowcount(pipe,
    /// begin=, end=, params=, remote=True)`; a connector whose method takes no `remote`
    /// keyword counts 0 with a warning).
    async fn remote_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> Result<Option<i64>, ConnectorError> {
        if let Some(source) = self.remote_source(pipe) {
            return source.get_pipe_rowcount_cached(pipe, query, None).await;
        }
        let keys = pipe.connector_keys();
        if keys.starts_with("valkey:") {
            return Err(ConnectorError::NotImplemented(
                "Connectors of type 'valkey' do not support remote rowcounts.".into(),
            ));
        }
        if crate::facade::is_native(keys) {
            return self
                .native_source(pipe)?
                .get_pipe_rowcount(pipe, query)
                .await;
        }
        match self.registry.source(keys) {
            Ok(Some(source)) => source.get_rowcount_remote(pipe, query).await,
            Ok(None) => Err(ConnectorError::NotImplemented(format!(
                "{pipe} has no source connector."
            ))),
            Err(e) => Err(ConnectorError::Backend(e)),
        }
    }

    /// `Pipe.get_sync_time()` (`remote`: the source's sync time, see
    /// [`Reader::remote_sync_time`]).
    pub async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
        remote: bool,
        apply_backtrack_interval: bool,
        round_down: bool,
    ) -> Result<Option<DateBound>, String> {
        if dt_col(pipe).is_none() {
            return Ok(None);
        }
        let read = if remote {
            self.remote_sync_time(pipe, params, newest).await
        } else {
            match &self.sql {
                Some(sql) => {
                    sql.get_sync_time_cached(
                        pipe,
                        params,
                        newest,
                        false,
                        self.columns_types.clone(),
                    )
                    .await
                }
                None => self.instance.get_sync_time(pipe, params, newest).await,
            }
        };
        let mut sync_time = match read {
            Ok(t) => t,
            Err(ConnectorError::NotImplemented(message)) => {
                tracing::warn!("{message}");
                return Ok(None);
            }
            Err(e) => return Err(err(e)),
        };
        if round_down && let Some(DateBound::Datetime(dt)) = &sync_time {
            sync_time = Some(DateBound::Datetime(round_time(
                dt,
                60_000_000,
                RoundTo::Down,
            )));
        }
        if apply_backtrack_interval && let Some(st) = sync_time.clone() {
            let dtypes = self.get_dtypes(pipe, true, false).await?;
            let interval = with_dtypes(pipe, &dtypes)
                .get_backtrack_interval(true, self.backtrack_minutes, &self.defaults)
                .map_err(err)?;
            match sub_interval(&st, &interval) {
                Some(b) => sync_time = Some(b),
                None => {
                    let (left, right) = match (&st, &interval) {
                        (DateBound::Datetime(_), _) => ("datetime.datetime", "int"),
                        _ => ("int", "datetime.timedelta"),
                    };
                    tracing::warn!(
                        "Failed to apply backtrack interval:\nunsupported operand type(s) \
                         for -=: '{left}' and '{right}'"
                    );
                }
            }
        }
        self.parse_bound(pipe, sync_time).await
    }

    /// `Pipe.get_rowcount()` (`0` for a missing count; `remote`: the rowcount of the fetch
    /// definition on the source, which must be a Rust SQL connector).
    pub async fn get_rowcount(
        &self,
        pipe: &Pipe,
        begin: Option<DateBound>,
        end: Option<DateBound>,
        params: Option<Map>,
        remote: bool,
    ) -> Result<i64, String> {
        let query = DataQuery {
            begin,
            end,
            params,
            remote,
            ..Default::default()
        };
        let count = if remote {
            self.remote_rowcount(pipe, &query).await
        } else {
            match &self.sql {
                Some(sql) => {
                    sql.get_pipe_rowcount_cached(pipe, &query, self.columns_types.clone())
                        .await
                }
                None => self.instance.get_pipe_rowcount(pipe, &query).await,
            }
        };
        match count {
            Ok(n) => Ok(n.unwrap_or(0)),
            Err(ConnectorError::NotImplemented(message)) => {
                tracing::warn!("{message}");
                Ok(0)
            }
            Err(e) => Err(err(e)),
        }
    }

    /// `Pipe.get_rowcount(estimate=True)` (contract C1): the instance's row estimate from
    /// catalog statistics, exact where there is none or counting is cheap; bounds, `params` and
    /// `remote` make it exact ([`Reader::get_rowcount`]).
    pub async fn get_rowcount_estimate(
        &self,
        pipe: &Pipe,
        begin: Option<DateBound>,
        end: Option<DateBound>,
        params: Option<Map>,
        remote: bool,
    ) -> Result<i64, String> {
        if remote
            || begin.is_some()
            || end.is_some()
            || params.as_ref().is_some_and(|p| !p.is_empty())
        {
            return self.get_rowcount(pipe, begin, end, params, remote).await;
        }
        let query = DataQuery::default();
        let count = match &self.sql {
            Some(sql) => {
                sql.get_pipe_rowcount_estimate(pipe, &query, self.columns_types.clone())
                    .await
            }
            None => self.instance.get_pipe_rowcount_estimate(pipe, &query).await,
        };
        match count {
            Ok(n) => Ok(n.unwrap_or(0)),
            Err(ConnectorError::NotImplemented(message)) => {
                tracing::warn!("{message}");
                Ok(0)
            }
            Err(e) => Err(err(e)),
        }
    }

    /// A PostgreSQL-family instance's documents hold a geometry the way psycopg returns the
    /// column, the hex EWKB text (`result.mappings()`), not a shapely object.
    fn raw_geometries(&self, docs: Vec<Doc>) -> Vec<Doc> {
        use meerschaum_connector_sql::Flavor::*;
        let Some(sql) = &self.sql else {
            return docs;
        };
        if !matches!(
            sql.flavor(),
            Postgresql | Postgis | Timescaledb | TimescaledbHa | Citus | Cockroachdb
        ) {
            return docs;
        }
        docs.into_iter()
            .map(|doc| {
                doc.into_iter()
                    .map(|(key, value)| match value {
                        Cell::Geometry(g) => (key, Cell::Str(g.to_wkb_hex(true))),
                        other => (key, other),
                    })
                    .collect()
            })
            .collect()
    }

    /// The instance as the `Pipe` methods call it with `extra` keywords (`Pipe.get_data(**kw)`).
    fn call_instance(&self, extra: &Map) -> Arc<dyn InstanceConnector> {
        self.instance
            .with_call_kwargs(extra)
            .unwrap_or_else(|| self.instance.clone())
    }

    /// `Pipe.get_data()`.
    pub async fn get_data(&self, pipe: &Pipe, args: &GetDataArgs) -> Result<Data, String> {
        self.read_data(pipe, args).await
    }

    async fn read_data(&self, pipe: &Pipe, args: &GetDataArgs) -> Result<Data, String> {
        let query = DataQuery {
            select_columns: args.select_columns.clone(),
            omit_columns: args.omit_columns.clone(),
            begin: args.begin.clone(),
            end: args.end.clone(),
            params: args.params.clone(),
            limit: args.limit,
            order: args.order.clone(),
            ..Default::default()
        };
        if args.as_docs {
            // A SQL instance answers a missing table with no documents itself, and an
            // unreachable database with an error (rc15 Q17), so no `exists` check first.
            if self.sql.is_none() && !self.exists(pipe).await? {
                return Ok(Data::Docs(Vec::new()));
            }
            if let Some(sql) = &self.sql {
                let frame = sql.get_pipe_docs_frame(pipe, &query).await.map_err(err)?;
                let integers = frame.columns().iter().filter(|column| {
                    matches!(&column.data, meerschaum_kernel::frame::ColumnData::Native(series)
                        if matches!(series.dtype(), polars::prelude::DataType::UInt64 | polars::prelude::DataType::Int128))
                }).map(|column| column.name.clone()).collect::<HashSet<_>>();
                let docs = self.raw_geometries(pipe_ops::frame_to_docs(&frame));
                return Ok(if integers.is_empty() {
                    Data::Docs(docs)
                } else {
                    Data::TypedDocs(docs, integers)
                });
            }
            // PARITY: `get_pipe_docs()` of an instance without its own (Valkey, custom
            // connectors) is `get_pipe_data()` as documents: `limit` and `order` are not applied
            // to documents (`connectors/instance/_pipes.py`), as in v4.
            let docs = self
                .call_instance(&args.extra)
                .get_pipe_docs(pipe, &query)
                .await
                .map_err(err)?;
            return Ok(Data::Docs(self.raw_geometries(docs)));
        }
        if let Some(sql) = &self.sql {
            // The SQL driver reads, selects, adds the missing columns, enforces the dtypes,
            // sorts and limits (`get_pipe_data_with_state`); `order=None` is "no sort".
            let options = DataQueryOptions {
                no_order: args.order.is_none(),
                ..args.sql_options.clone()
            };
            let (frame, state) = sql
                .get_pipe_data_and_state_with(pipe, &query, &options, self.columns_types.clone())
                .await
                .map_err(err)?;
            let Some(mut frame) = frame else {
                return Ok(Data::Missing);
            };
            if args.add_missing_columns {
                // The driver adds the columns `select_columns` named; `add_missing_columns`
                // adds every column of the pipe's dtypes.
                let missing: IndexMap<String, String> = state
                    .dtypes
                    .iter()
                    .filter(|(c, _)| !frame.contains(c))
                    .map(|(c, t)| (c.clone(), t.clone()))
                    .collect();
                if !missing.is_empty() {
                    let mut all = frame.dtypes();
                    all.extend(missing);
                    frame = meerschaum_kernel::diff::add_missing_cols(&frame, &all);
                }
            }
            return Ok(Data::Frame(frame, state.dtypes));
        }
        if !self.exists(pipe).await? {
            return Ok(Data::Missing);
        }
        let frame = match self
            .call_instance(&args.extra)
            .get_pipe_data(pipe, &query)
            .await
        {
            Ok(Some(f)) => f,
            Ok(None) => return Ok(Data::Missing),
            Err(e) => return Err(err(e)),
        };
        self.postprocess(pipe, frame, args).await
    }

    /// The part of `Pipe.get_data()` after the instance returned a frame: drop the columns the
    /// instance did not omit, add the missing ones as nulls, enforce the pipe's dtypes, sort and
    /// limit.
    async fn postprocess(
        &self,
        pipe: &Pipe,
        mut df: Frame,
        args: &GetDataArgs,
    ) -> Result<Data, String> {
        let names = df.names();
        let select: Vec<String> = match &args.select_columns {
            Some(s) if !s.is_empty() => s.clone(),
            _ => names.clone(),
        };
        let omit = args.omit_columns.clone().unwrap_or_default();
        let pipe_dtypes = self.get_dtypes(pipe, true, false).await?;
        let cols_to_omit: Vec<&String> = names
            .iter()
            .filter(|c| omit.contains(c) || !select.contains(c))
            .collect();
        let mut cols_to_add: IndexMap<String, String> = IndexMap::new();
        for col in select.iter().filter(|c| !names.contains(c)) {
            cols_to_add.insert(
                col.clone(),
                pipe_dtypes
                    .get(col)
                    .cloned()
                    .unwrap_or_else(|| "string".into()),
            );
        }
        if args.add_missing_columns {
            for (col, typ) in pipe_dtypes.iter().filter(|(c, _)| !names.contains(c)) {
                cols_to_add
                    .entry(col.clone())
                    .or_insert_with(|| typ.clone());
            }
        }
        if !cols_to_omit.is_empty() {
            tracing::warn!(
                "Received {} omitted column{} for {pipe}. Consider adding `select_columns` and \
                 `omit_columns` support to '{}' connectors to improve performance.",
                cols_to_omit.len(),
                if cols_to_omit.len() != 1 { "s" } else { "" },
                self.instance.connector_type()
            );
            let keep: Vec<String> = names
                .iter()
                .filter(|c| !cols_to_omit.contains(c))
                .cloned()
                .collect();
            df = df.select(&keep).map_err(err)?;
        }
        if !cols_to_add.is_empty() {
            if !args.add_missing_columns {
                let items: Vec<String> = cols_to_add.keys().cloned().collect();
                tracing::warn!(
                    "Will add columns {} as nulls to dataframe.",
                    meerschaum_connector_sql::instance::values::items_str(&items)
                );
            }
            let mut all = df.dtypes();
            all.extend(cols_to_add);
            df = meerschaum_kernel::diff::add_missing_cols(&df, &all);
        }
        let ctx = self.context(pipe, Some(pipe_dtypes.clone()), true).await?;
        let mut enforced = pipe_enforce_dtypes(PipeInput::Frame(df), &ctx).map_err(err)?;
        if args.order.as_deref().is_some_and(|o| !o.is_empty()) {
            let query = DataQuery {
                order: args.order.clone(),
                limit: args.limit,
                ..Default::default()
            };
            enforced = pipe_ops::pipe_postprocess(pipe, enforced, &query);
        }
        Ok(Data::Frame(enforced, pipe_dtypes))
    }

    /// `Pipe.get_backtrack_data()`: the rows newer than `base.begin` (default: the sync time)
    /// minus the backtrack window, newest first (`base` carries the other `get_data()`
    /// arguments; an unset `order` is `'desc'`).
    pub async fn get_backtrack_data(
        &self,
        pipe: &Pipe,
        backtrack_minutes: Option<f64>,
        base: GetDataArgs,
    ) -> Result<Data, String> {
        if !self.exists(pipe).await? {
            return Ok(Data::Missing);
        }
        let dtypes = self.get_dtypes(pipe, true, false).await?;
        let backtrack_interval = with_dtypes(pipe, &dtypes)
            .get_backtrack_interval(true, self.backtrack_minutes, &self.defaults)
            .map_err(err)?;
        let minutes = backtrack_minutes.unwrap_or(match backtrack_interval {
            ChunkInterval::Delta(d) => meerschaum_core::pipe::chunk::total_seconds(d) / 60.0,
            ChunkInterval::Int(i) => i as f64,
        });
        if self.instance.has_backtrack_data() {
            // The instance connector defines its own `get_backtrack_data`: v4 calls it and
            // enforces the pipe's dtypes on what it returns.
            let query = DataQuery {
                select_columns: base.select_columns.clone(),
                omit_columns: base.omit_columns.clone(),
                begin: base.begin.clone(),
                end: base.end.clone(),
                params: base.params.clone(),
                limit: base.limit,
                ..Default::default()
            };
            let frame = self
                .call_instance(&base.extra)
                .get_backtrack_data(pipe, minutes.round() as i64, &query)
                .await
                .map_err(err)?;
            let Some(frame) = frame else {
                return Ok(Data::Missing);
            };
            let (frame, ctx) = self
                .enforce_dtypes(pipe, PipeInput::Frame(frame), None, true)
                .await?;
            return Ok(Data::Frame(frame, ctx.dtypes));
        }
        let mut begin = base.begin.clone();
        if begin.is_none() {
            begin = self
                .get_sync_time(pipe, base.params.as_ref(), true, false, false, false)
                .await?;
        }
        let begin = match begin {
            Some(DateBound::Datetime(dt)) => {
                let delta = TimeDelta::microseconds((minutes * 60_000_000.0).round() as i64);
                Some(DateBound::Datetime(DateTimeValue {
                    utc: dt.utc - delta,
                    tz: dt.tz,
                }))
            }
            Some(DateBound::Int(i)) => Some(DateBound::Int(i - minutes as i64)),
            None => None,
        };
        let order = match &base.order {
            Some(o) if !o.is_empty() => o.clone(),
            _ => "desc".to_string(),
        };
        self.get_data(
            pipe,
            &GetDataArgs {
                begin,
                order: Some(order),
                ..base
            },
        )
        .await
    }

    /// The bounds `Pipe.get_data(as_iterator=True)` reads in: one chunk when the data span less
    /// than a chunk interval, else the interior chunks of `get_chunk_bounds`.
    pub async fn plan_chunks(
        &self,
        pipe: &Pipe,
        begin: Option<&DateBound>,
        end: Option<&DateBound>,
        params: Option<&Map>,
        chunk_interval: Option<ChunkIntervalArg>,
    ) -> Result<ChunkPlan, String> {
        if !self.exists(pipe).await? {
            return Ok(ChunkPlan::Empty);
        }
        let has_dt = dt_col(pipe).is_some();
        let min_dt = match (has_dt, begin) {
            (false, _) => None,
            (true, Some(b)) => Some(b.clone()),
            (true, None) => {
                self.get_sync_time(pipe, params, false, false, false, false)
                    .await?
            }
        };
        let mut max_dt = match (has_dt, end) {
            (false, _) => None,
            (true, Some(b)) => Some(b.clone()),
            (true, None) => {
                self.get_sync_time(pipe, params, true, false, false, false)
                    .await?
            }
        };
        // We want to search just past the maximum value.
        if end.is_none() {
            max_dt = max_dt.map(|m| match m {
                DateBound::Int(i) => DateBound::Int(i + 1),
                DateBound::Datetime(dt) => DateBound::Datetime(round_time(
                    &DateTimeValue {
                        utc: dt.utc + Duration::minutes(1),
                        tz: dt.tz,
                    },
                    60_000_000,
                    RoundTo::Down,
                )),
            });
        }
        let dtypes = self.get_dtypes(pipe, true, false).await?;
        let interval = with_dtypes(pipe, &dtypes)
            .get_chunk_interval(chunk_interval, &self.defaults)
            .map_err(err)?;
        // If we can't determine bounds or the chunk interval exceeds the span, one chunk.
        let single = match (&min_dt, &max_dt) {
            (None, None) => true,
            // `(min + chunk_interval) > max`
            (Some(mn), Some(mx)) => add_interval(mn, &interval)
                .map(|edge| bound_lt(mx, &edge))
                .unwrap_or(true),
            _ => true,
        };
        if single {
            return Ok(ChunkPlan::Single);
        }
        let bounds = get_chunk_bounds(min_dt, max_dt, false, interval, false, None, None)?;
        Ok(ChunkPlan::Bounds(bounds))
    }

    /// `Pipe.filter_existing(df)`: `(unseen, update, delta)`.
    pub async fn filter_existing(
        &self,
        pipe: &Pipe,
        input: PipeInput,
        opts: &FilterOpts,
    ) -> Result<FilterOut, String> {
        let ctx = self.context(pipe, None, true).await?;
        // Declared text / index columns keep their null-like strings (rc15 Q18).
        let literal_null_markers =
            meerschaum_kernel::diff::literal_null_markers(&pipe_ops::columns_map(pipe), &ctx);
        let params = FilterExistingParams {
            columns: pipe_ops::columns_map(pipe),
            configured_dtypes: ctx.explicit_dtypes.clone(),
            dtypes_ctx: ctx,
            autoincrement: pipe.autoincrement(),
            autotime: pipe.autotime(),
            static_: pipe.is_static(),
            include_unchanged_columns: opts.include_unchanged_columns,
            date_bound_only: opts.date_bound_only,
            enforce_dtypes: opts.enforce_dtypes,
            filter_params_index_limit: self.filter_limit,
            literal_null_markers,
        };
        // The kernel's callback is synchronous: capture the `get_data()` request, read it, then
        // diff against the frame it returns.
        let mut request: Option<BacktrackRequest> = None;
        let _ = filter_existing(input.clone(), &params, |req| {
            request = Some(req.clone());
            None
        });
        let backtrack = match &request {
            Some(req) => {
                let args = GetDataArgs {
                    begin: DateBound::from_value(&req.begin),
                    end: DateBound::from_value(&req.end),
                    params: Some(backtrack_params(req)),
                    order: Some("asc".into()),
                    ..Default::default()
                };
                match self.get_data(pipe, &args).await? {
                    Data::Frame(frame, _) => Some(frame),
                    _ => None,
                }
            }
            None => None,
        };
        let frames = filter_existing(input, &params, move |_| backtrack).map_err(err)?;
        Ok(FilterOut { frames })
    }

    /// The `Pipe.deduplicate()` of an instance without a native implementation, or the
    /// instance's own.
    pub async fn deduplicate(&self, pipe: &Pipe, args: &DedupArgs) -> SuccessTuple {
        if args.use_instance_method {
            let q = DataQuery {
                begin: args.begin.clone(),
                end: args.end.clone(),
                params: args.params.clone(),
                ..Default::default()
            };
            let result = self
                .call_instance(&args.extra)
                .deduplicate_pipe(pipe, &q)
                .await;
            if result.success || !result.message.starts_with(DEDUP_UNSUPPORTED) {
                return result;
            }
        }
        match self.deduplicate_chunks(pipe, args).await {
            Ok(t) => t,
            Err(e) => SuccessTuple::fail(e),
        }
    }

    /// `Pipe.get_bound_interval()`: the `verify.bound_*` window (`None` if unset).
    fn bound_interval(&self, pipe: &Pipe, int_axis: bool) -> Option<ChunkInterval> {
        let verify = pipe.parameters().get("verify")?.as_map()?.clone();
        let mut found: Option<(&str, &Value)> = None;
        for (key, value) in &verify {
            if let Some(suffix) = key.strip_prefix("bound_")
                && ["minutes", "hours", "days", "weeks", "years", "seconds"].contains(&suffix)
            {
                found = Some((suffix, value));
                break;
            }
        }
        let (suffix, value) = found?;
        let amount = meerschaum_core::pyfmt::py_int(value)?;
        if dt_col(pipe).is_none() || int_axis {
            return Some(ChunkInterval::Int(amount));
        }
        let delta = match suffix {
            "minutes" => TimeDelta::minutes(amount),
            "hours" => TimeDelta::hours(amount),
            "days" => TimeDelta::days(amount),
            "weeks" => TimeDelta::weeks(amount),
            "years" => TimeDelta::days(amount * 365),
            _ => TimeDelta::seconds(amount),
        };
        Some(ChunkInterval::Delta(delta))
    }

    /// `Pipe.get_bound_time()`.
    async fn bound_time(&self, pipe: &Pipe) -> Result<Option<DateBound>, String> {
        let dtypes = self.get_dtypes(pipe, true, false).await?;
        let int_axis = with_dtypes(pipe, &dtypes).is_int_axis();
        let Some(interval) = self.bound_interval(pipe, int_axis) else {
            return Ok(None);
        };
        let Some(newest) = self
            .get_sync_time(pipe, None, true, false, false, false)
            .await?
        else {
            return Ok(None);
        };
        let Some(bound_time) = sub_interval(&newest, &interval) else {
            return Ok(None);
        };
        let Some(oldest) = self
            .get_sync_time(pipe, None, false, false, false, false)
            .await?
        else {
            return Ok(None);
        };
        let max_days = meerschaum_core::static_config::static_config()
            .pipes
            .max_bound_time_days;
        let extreme = match (&newest, &oldest) {
            (DateBound::Datetime(n), DateBound::Datetime(o)) => {
                n.utc - o.utc >= Duration::days(max_days)
            }
            _ => false,
        };
        Ok((bound_lt(&oldest, &bound_time) || extreme).then_some(bound_time))
    }

    /// The generic chunked implementation of `Pipe.deduplicate()` (`_deduplicate.py`).
    async fn deduplicate_chunks(
        &self,
        pipe: &Pipe,
        args: &DedupArgs,
    ) -> Result<SuccessTuple, String> {
        // Only unbound if explicitly `False`.
        let bounded = args.bounded.unwrap_or(true);
        let dtypes = self.get_dtypes(pipe, true, false).await?;
        let interval = with_dtypes(pipe, &dtypes)
            .get_chunk_interval(args.chunk_interval, &self.defaults)
            .map_err(err)?;
        let bound_time = self.bound_time(pipe).await?;
        let mut begin = args.begin.clone();
        let mut end = args.end.clone();
        if bounded && begin.is_none() {
            begin = match bound_time {
                Some(b) => Some(b),
                None => {
                    self.get_sync_time(pipe, None, false, false, false, false)
                        .await?
                }
            };
        }
        if bounded && end.is_none() {
            end = self
                .get_sync_time(pipe, None, true, false, false, false)
                .await?
                .map(just_past);
        }
        // `get_chunk_bounds(bounded=..., begin=..., end=...)`: the sync times stand in for an
        // unset bound.
        let oldest = if begin.is_none() {
            self.get_sync_time(pipe, None, false, false, false, false)
                .await?
        } else {
            None
        };
        let newest = if end.is_none() {
            self.get_sync_time(pipe, None, true, false, false, false)
                .await?
        } else {
            None
        };
        let chunk_bounds = get_chunk_bounds(begin, end, bounded, interval, false, oldest, newest)?;

        let indices: Vec<String> = pipe_ops::columns_map(pipe)
            .into_values()
            .filter(|c| !c.is_empty())
            .collect();
        if indices.is_empty() {
            return Ok(SuccessTuple::fail(
                "Cannot deduplicate without index columns.",
            ));
        }

        let n = chunk_bounds.len();
        py_info(&format!(
            "Deduplicating {n} chunk{}{} of size '{}' on {pipe}.",
            if n != 1 { "s" } else { "" },
            format_args!(" ({}bounded)", if bounded { "" } else { "un" }),
            interval_str(&interval)
        ));

        let results = self
            .dedup_chunks(
                pipe,
                &indices,
                &chunk_bounds,
                args.params.as_ref(),
                args.workers,
            )
            .await?;
        let (successes, failures): (Vec<_>, Vec<_>) = results.iter().partition(|(_, t)| t.success);
        let plural = |k: usize| if k != 1 { "s" } else { "" };
        let messages = |items: &[&(ChunkBounds, SuccessTuple)]| -> String {
            items
                .iter()
                .filter(|(_, t)| !t.message.is_empty())
                .map(|(_, t)| t.message.clone())
                .collect::<Vec<_>>()
                .join("\n")
        };
        // No need to retry if everything failed.
        if !failures.is_empty() && successes.is_empty() {
            return Ok(SuccessTuple::fail(format!(
                "Failed to deduplicate {} chunk{}.\n{}",
                failures.len(),
                plural(failures.len()),
                messages(&failures)
            )));
        }
        if failures.is_empty() {
            return Ok(SuccessTuple::ok(
                format!(
                    "Successfully deduplicated {} chunk{}.\n{}",
                    successes.len(),
                    plural(successes.len()),
                    messages(&successes)
                )
                .trim_end_matches('\n')
                .to_string(),
            ));
        }
        let retry_bounds: Vec<ChunkBounds> = failures.iter().map(|(b, _)| b.clone()).collect();
        py_info(&format!(
            "Retrying {} chunks for {pipe}...",
            retry_bounds.len()
        ));
        let retried = self
            .dedup_chunks(
                pipe,
                &indices,
                &retry_bounds,
                args.params.as_ref(),
                args.workers,
            )
            .await?;
        let (retry_successes, retry_failures): (Vec<_>, Vec<_>) =
            retried.iter().partition(|(_, t)| t.success);
        if retry_failures.is_empty() {
            let mut all: Vec<&(ChunkBounds, SuccessTuple)> = successes.clone();
            all.extend(retry_successes.iter().copied());
            return Ok(SuccessTuple::ok(
                format!(
                    "Successfully deduplicated {} chunk{}({} retried):\n{}",
                    all.len(),
                    plural(all.len()),
                    retry_successes.len(),
                    messages(&all)
                )
                .trim_end_matches('\n')
                .to_string(),
            ));
        }
        // PARITY: v4 reports `len(bounds_failures)` chunks but pluralizes with
        // `len(retry_bounds_failures)` (`_deduplicate.py`, the last return).
        Ok(SuccessTuple::fail(
            format!(
                "Failed to deduplicate {} chunk{}.\n{}",
                failures.len(),
                plural(retry_failures.len()),
                messages(&retry_failures)
            )
            .trim_end_matches('\n')
            .to_string(),
        ))
    }

    /// `pool.map(process_chunk_bounds, chunk_bounds)`: the chunks run up to `workers` at a
    /// time and come back in chunk order.
    async fn dedup_chunks(
        &self,
        pipe: &Pipe,
        indices: &[String],
        bounds: &[ChunkBounds],
        params: Option<&Map>,
        workers: usize,
    ) -> Result<Vec<(ChunkBounds, SuccessTuple)>, String> {
        let mut out = Vec::with_capacity(bounds.len());
        if workers <= 1 || bounds.len() <= 1 {
            for b in bounds {
                out.push((b.clone(), self.dedup_chunk(pipe, indices, b, params).await?));
            }
            return Ok(out);
        }
        let limit = Arc::new(tokio::sync::Semaphore::new(workers));
        let mut tasks = tokio::task::JoinSet::new();
        for (position, b) in bounds.iter().enumerate() {
            let (reader, pipe, indices, b, params) = (
                self.clone(),
                pipe.clone(),
                indices.to_vec(),
                b.clone(),
                params.cloned(),
            );
            let limit = limit.clone();
            tasks.spawn(async move {
                let _permit = limit.acquire_owned().await;
                let result = reader
                    .dedup_chunk(&pipe, &indices, &b, params.as_ref())
                    .await;
                (position, b, result)
            });
        }
        let mut done: Vec<Option<(ChunkBounds, SuccessTuple)>> = vec![None; bounds.len()];
        while let Some(joined) = tasks.join_next().await {
            let (position, b, result) = joined.map_err(err)?;
            done[position] = Some((b, result?));
        }
        out.extend(done.into_iter().flatten());
        Ok(out)
    }

    /// `process_chunk_bounds` of v4's `deduplicate`.
    async fn dedup_chunk(
        &self,
        pipe: &Pipe,
        indices: &[String],
        bounds: &ChunkBounds,
        params: Option<&Map>,
    ) -> Result<SuccessTuple, String> {
        let (chunk_begin, chunk_end) = bounds;
        // Only selecting the index values here to keep bandwidth down.
        let index_args = GetDataArgs {
            select_columns: Some(indices.to_vec()),
            begin: chunk_begin.clone(),
            end: chunk_end.clone(),
            params: params.cloned(),
            order: Some("asc".into()),
            ..Default::default()
        };
        let Data::Frame(chunk_df, _) = self.get_data(pipe, &index_args).await? else {
            return Ok(SuccessTuple::ok(""));
        };
        let existing_chunk_len = chunk_df.height();
        let keys: Vec<Vec<String>> = (0..chunk_df.height())
            .map(|i| chunk_df.row(i).iter().map(Cell::py_str).collect())
            .collect();
        let deduped_chunk_len = distinct_len(&keys);
        if existing_chunk_len == deduped_chunk_len {
            return Ok(SuccessTuple::ok(""));
        }
        let header = format!("\n{} - {}", bound_str(chunk_begin), bound_str(chunk_end));
        let mut body = String::new();
        let full_args = GetDataArgs {
            select_columns: None,
            begin: chunk_begin.clone(),
            end: chunk_end.clone(),
            params: params.cloned(),
            order: Some("asc".into()),
            ..Default::default()
        };
        let full_chunk = match self.get_data(pipe, &full_args).await? {
            Data::Frame(f, _) if f.height() > 0 => f,
            _ => {
                return Ok(SuccessTuple::ok(format!(
                    "{header}\nChunk is empty, skipping..."
                )));
            }
        };
        let chunk_indices: Vec<String> = indices
            .iter()
            .filter(|c| full_chunk.contains(c))
            .cloned()
            .collect();
        if chunk_indices.is_empty() {
            return Ok(SuccessTuple::fail(format!(
                "None of {} were present in chunk.",
                meerschaum_connector_sql::instance::values::items_str(indices)
            )));
        }
        let deduped = drop_duplicates_last(&full_chunk, &chunk_indices);

        let clear = DataQuery {
            begin: chunk_begin.clone(),
            end: chunk_end.clone(),
            params: params.cloned(),
            ..Default::default()
        };
        let cleared = self.instance.clear_pipe(pipe, &clear).await;
        if !cleared.success {
            body.push_str(&format!(
                "Failed to clear chunk while deduplicating:\n{}\n",
                cleared.message
            ));
            tracing::warn!("{body}");
        }
        let env = self.env();
        let synced = meerschaum_engine::sync::sync_pipe(
            &env,
            pipe,
            Some(PipeInput::Frame(deduped)),
            &SyncArgs::default(),
            None,
        )
        .await;
        if !synced.success {
            body.push_str(&format!(
                "Failed to sync chunk while deduplicating:\n{}\n",
                synced.message
            ));
        }
        // Finally check if the deduplication worked.
        let chunk_rowcount = self
            .get_rowcount(
                pipe,
                chunk_begin.clone(),
                chunk_end.clone(),
                params.cloned(),
                false,
            )
            .await?;
        let sep = if body.is_empty() { "" } else { "\n" };
        if chunk_rowcount != deduped_chunk_len as i64 {
            return Ok(SuccessTuple::fail(format!(
                "{header}\n{body}{sep}Chunk rowcounts still differ ({chunk_rowcount} rowcount vs \
                 {deduped_chunk_len} chunk length)."
            )));
        }
        Ok(SuccessTuple::ok(format!(
            "{header}\n{body}{sep}Deduplicated chunk from {existing_chunk_len} to \
             {chunk_rowcount} rows."
        )))
    }

    /// `Pipe.fetch()`: the source's newest data (`mode`: v4's `sync_chunks` / `chunk_hook`).
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch(
        &self,
        pipe: &Pipe,
        begin: SyncBegin,
        end: Option<DateBound>,
        check_existing: bool,
        params: Option<Map>,
        extra: Map,
        mode: FetchMode,
    ) -> Result<Fetched, String> {
        let args = SyncArgs {
            begin: begin.clone(),
            end: end.clone(),
            params: params.clone(),
            check_existing,
            extra: extra.clone(),
            ..Default::default()
        };
        let source = match self.registry.source_for_sync(pipe.connector_keys(), &args) {
            Ok(Some(s)) => s,
            Ok(None) | Err(_) => {
                tracing::warn!(
                    "No `fetch()` function defined for connector '{}'",
                    pipe.connector_keys()
                );
                return Ok(Fetched::Nothing);
            }
        };
        let dtypes = self.get_dtypes(pipe, true, false).await?;
        let env = self.env();
        let begin = determine_begin(
            &env,
            self.instance.as_ref(),
            pipe,
            &dtypes,
            &begin,
            end.as_ref(),
            check_existing,
        )
        .await
        .map_err(err)?;
        let query = DataQuery {
            begin,
            end,
            params,
            ..Default::default()
        };
        let is_sql = source.connector_type() == "sql";
        // A Python source gets the chunk hook (`sync_chunks`: synced on the host as the worker
        // produces each chunk); `SQLConnector.fetch()`, `APIConnector.fetch()` and
        // `ValkeyConnector.fetch()` take no `chunk_hook`, so v4 returns their data as it is.
        if !matches!(mode, FetchMode::Plain)
            && !crate::facade::is_native(pipe.connector_keys())
            && let Some(out) = self.fetch_streamed(pipe, &source, &query, &mode).await?
        {
            return Ok(out);
        }
        match source.fetch(pipe, &query).await {
            Ok(Some(frame)) if is_sql => Ok(Fetched::Chunks(self.sql_chunks(pipe, frame))),
            Ok(Some(frame)) => Ok(Fetched::Frame(frame)),
            Ok(None) => Ok(Fetched::Nothing),
            Err(ConnectorError::NotImplemented(_)) => {
                tracing::warn!(
                    "No `fetch()` function defined for connector '{}'",
                    pipe.connector_keys()
                );
                Ok(Fetched::Nothing)
            }
            Err(e) => Err(err(e)),
        }
    }

    /// A SQL source's rows in chunks of `system:connectors:sql:chunksize` rows (one empty chunk
    /// for an empty result).
    fn sql_chunks(&self, pipe: &Pipe, frame: Frame) -> Vec<Frame> {
        let size = self.registry.fetch_chunksize(pipe.connector_keys()).max(1);
        let height = frame.height();
        let mut chunks = Vec::new();
        let mut start = 0;
        while start < height {
            let stop = (start + size).min(height);
            chunks.push(frame.take(&(start..stop).collect::<Vec<_>>()));
            start = stop;
        }
        if chunks.is_empty() {
            chunks.push(frame);
        }
        chunks
    }

    /// A Python source's `fetch()` with the chunks it yields (or the hook results it
    /// returns) consumed as they arrive. `None` when the source cannot stream.
    async fn fetch_streamed(
        &self,
        pipe: &Pipe,
        source: &Arc<dyn meerschaum_connector::Connector>,
        query: &DataQuery,
        mode: &FetchMode,
    ) -> Result<Option<Fetched>, String> {
        // `sync_chunks`: the worker hands the source a `chunk_hook` that syncs each chunk on
        // the host. A caller's `chunk_hook` cannot cross to the worker: it runs here, on every
        // chunk a generator yields.
        let sync_chunks = matches!(mode, FetchMode::SyncChunks);
        let Some(mut chunked) = source
            .fetch_chunked(pipe, query, sync_chunks)
            .await
            .map_err(err)?
        else {
            return Ok(None);
        };
        let mut frames: Vec<Frame> = Vec::new();
        while let Some(chunk) = chunked.chunks.next_chunk().await.map_err(err)? {
            frames.push(input_frame(chunk)?);
        }
        let kind = chunked.chunks.finish().await.map_err(err)?;
        Ok(Some(match kind {
            FetchedKind::HookResults(results) => {
                Fetched::Hooked(results.into_iter().map(HookOutcome::Tuple).collect())
            }
            FetchedKind::InParallel => Fetched::True,
            FetchedKind::Nothing => Fetched::Nothing,
            FetchedKind::Data { generator } => match mode {
                FetchMode::Hook(hook) if generator => Fetched::Hooked(
                    frames
                        .into_iter()
                        .map(|f| {
                            hook(f).unwrap_or_else(|e| HookOutcome::Tuple(SuccessTuple::fail(e)))
                        })
                        .collect(),
                ),
                _ if generator => Fetched::Chunks(frames),
                _ => match frames.len() {
                    0 => Fetched::Nothing,
                    1 => Fetched::Frame(frames.remove(0)),
                    _ => Fetched::Chunks(frames),
                },
            },
        }))
    }
}

/// A fetched chunk as a frame.
fn input_frame(input: PipeInput) -> Result<Frame, String> {
    match input {
        PipeInput::Frame(f) => Ok(f),
        PipeInput::Docs(docs) => meerschaum_kernel::docs::frame_from_docs(&docs).map_err(err),
        PipeInput::Columns(cols) => meerschaum_kernel::docs::frame_from_columns(&cols).map_err(err),
    }
}

/// A backtrack request's `params` as `build_where` params (as the SQL instance builds them).
fn backtrack_params(req: &BacktrackRequest) -> Map {
    req.params
        .iter()
        .map(|(k, vals)| {
            let items = vals
                .iter()
                .map(|v| match v {
                    v if v.is_na() => Value::Null,
                    Cell::Int(i) => Value::Int(*i),
                    Cell::Float(f) => Value::Float(*f),
                    Cell::Bool(b) => Value::Bool(*b),
                    other => Value::Str(other.py_str()),
                })
                .collect();
            (k.clone(), Value::List(items))
        })
        .collect()
}

/// The number of distinct rows (`DataFrame.drop_duplicates()`).
fn distinct_len(rows: &[Vec<String>]) -> usize {
    let mut seen = std::collections::HashSet::with_capacity(rows.len());
    rows.iter().filter(|r| seen.insert((*r).clone())).count()
}

/// `df.drop_duplicates(subset=cols, keep='last').reset_index(drop=True)`.
fn drop_duplicates_last(df: &Frame, cols: &[String]) -> Frame {
    let keys: Vec<Vec<String>> = (0..df.height())
        .map(|i| {
            cols.iter()
                .map(|c| {
                    df.column(c)
                        .map(|col| col.data.get(i).py_str())
                        .unwrap_or_default()
                })
                .collect()
        })
        .collect();
    let mut last: HashMap<&Vec<String>, usize> = HashMap::new();
    for (i, key) in keys.iter().enumerate() {
        last.insert(key, i);
    }
    let mut keep: Vec<usize> = last.into_values().collect();
    keep.sort_unstable();
    df.take(&keep)
}

/// `interval_str(interval)` (`meerschaum.utils.misc`).
pub fn interval_str(interval: &ChunkInterval) -> String {
    match interval {
        ChunkInterval::Int(i) => i.to_string(),
        ChunkInterval::Delta(d) => meerschaum_cli::native::sync::interval_str(
            meerschaum_core::pipe::chunk::total_seconds(*d),
        ),
    }
}

/// `info(message)` through the Python process's `meerschaum.utils.warnings.info`.
pub fn py_info(message: &str) {
    use pyo3::prelude::*;
    // Dropped once the interpreter is finalizing (`attach` would panic there).
    let _ = Python::try_attach(|py| -> PyResult<()> {
        py.import("meerschaum.utils.warnings")?
            .getattr("info")?
            .call1((message,))?;
        Ok(())
    });
}

#[cfg(test)]
mod tests {
    //! The generic port of the v4 bodies (a [`Reader`] with no SQL fast path) against the SQL
    //! driver's own `Pipe.get_data()` pipeline, on a SQLite file.

    use super::*;
    use indexmap::IndexMap;
    use meerschaum_engine::registry::StaticRegistry;
    use meerschaum_kernel::value::DateTimeValue;

    const INSTANCE: &str = "sql:w15l2_unit";

    fn run<F: std::future::Future>(fut: F) -> F::Output {
        crate::runtime::runtime().block_on(fut)
    }

    fn start() -> chrono::NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }

    fn doc(i: i64) -> IndexMap<String, Cell> {
        let mut d = IndexMap::new();
        d.insert(
            "dt".to_string(),
            Cell::Datetime(DateTimeValue::utc(start() + Duration::hours(i))),
        );
        d.insert("id".to_string(), Cell::Int(i % 3));
        d.insert("val".to_string(), Cell::Float(i as f64 / 2.0));
        d.insert("name".to_string(), Cell::Str(format!("n{i}")));
        d
    }

    fn pipe(metric: &str) -> Pipe {
        let json = serde_json::json!({
            "columns": {"datetime": "dt", "id": "id"},
            "dtypes": {"val": "float", "name": "str"},
        });
        let Value::Map(parameters) = pipe_ops::json_to_core(&json) else {
            unreachable!()
        };
        Pipe::builder("w15l2", metric)
            .instance(INSTANCE)
            .parameters(parameters)
            .build()
            .unwrap()
    }

    struct Fixture {
        generic: Reader,
        sql: Reader,
        path: std::path::PathBuf,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    /// A SQLite file with `n` rows synced, read through the SQL fast path and the generic path.
    fn fixture(tag: &str, n: i64) -> (Fixture, Pipe) {
        let path = std::env::temp_dir().join(format!(
            "w15l2-{tag}-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let conn = run(SqlConnector::from_uri(
            "w15l2_unit",
            &format!("sqlite:///{}", path.display()),
        ))
        .unwrap();
        let conn = Arc::new(conn);
        let registry: Arc<dyn ConnectorRegistry> = Arc::new(
            StaticRegistry::new()
                .with_instance(INSTANCE, conn.clone() as Arc<dyn InstanceConnector>),
        );
        let reader = |sql: Option<Arc<SqlConnector>>| Reader {
            instance: conn.clone(),
            sql,
            sources: HashMap::new(),
            registry: registry.clone(),
            defaults: PipeDefaults::default(),
            backtrack_minutes: 1440,
            filter_limit: 250,
            columns_types: None,
        };
        let generic = reader(None);
        let sql = reader(Some(conn.clone()));
        let pipe = pipe(tag);
        let docs: Vec<_> = (0..n).map(doc).collect();
        let env = sql.env();
        let result = run(meerschaum_engine::sync::sync_pipe(
            &env,
            &pipe,
            Some(PipeInput::Docs(docs)),
            &SyncArgs::default(),
            None,
        ));
        assert!(result.success, "{}", result.message);
        (Fixture { generic, sql, path }, pipe)
    }

    fn rows(data: &Data) -> Vec<Vec<String>> {
        match data {
            Data::Missing => vec![],
            Data::Docs(docs) | Data::TypedDocs(docs, _) => docs
                .iter()
                .map(|d| d.values().map(Cell::py_str).collect())
                .collect(),
            Data::Frame(frame, _) => (0..frame.height())
                .map(|i| frame.row(i).iter().map(Cell::py_str).collect())
                .collect(),
        }
    }

    fn names(data: &Data) -> Vec<String> {
        match data {
            Data::Frame(frame, _) => frame.names(),
            _ => vec![],
        }
    }

    fn at(hours: i64) -> Option<DateBound> {
        Some(DateBound::Datetime(DateTimeValue::utc(
            start() + Duration::hours(hours),
        )))
    }

    fn params(json: serde_json::Value) -> Option<Map> {
        match pipe_ops::json_to_core(&json) {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    #[test]
    fn the_generic_get_data_matches_the_sql_pipeline() {
        let (fx, pipe) = fixture("getdata", 12);
        let cols = |c: &[&str]| Some(c.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let order = |o: &str| Some(o.to_string());
        let cases = [
            GetDataArgs {
                order: order("asc"),
                ..Default::default()
            },
            GetDataArgs {
                select_columns: cols(&["dt", "val"]),
                order: order("asc"),
                ..Default::default()
            },
            GetDataArgs {
                omit_columns: cols(&["name"]),
                order: order("asc"),
                ..Default::default()
            },
            GetDataArgs {
                begin: at(2),
                end: at(9),
                order: order("asc"),
                ..Default::default()
            },
            GetDataArgs {
                params: params(serde_json::json!({"id": 1})),
                order: order("desc"),
                limit: Some(3),
                ..Default::default()
            },
            GetDataArgs {
                select_columns: cols(&["dt", "nope"]),
                order: order("asc"),
                ..Default::default()
            },
            GetDataArgs {
                select_columns: cols(&["dt", "id"]),
                add_missing_columns: true,
                order: order("asc"),
                ..Default::default()
            },
        ];
        for (i, args) in cases.iter().enumerate() {
            let want = run(fx.sql.get_data(&pipe, args)).unwrap();
            let got = run(fx.generic.get_data(&pipe, args)).unwrap();
            assert_eq!(names(&got), names(&want), "case {i} columns");
            assert_eq!(rows(&got), rows(&want), "case {i} rows");
        }
    }

    #[test]
    fn a_missing_pipe_has_no_data_and_no_dtypes() {
        let (fx, _) = fixture("missing", 3);
        let ghost = pipe("never_synced");
        for reader in [&fx.sql, &fx.generic] {
            let data = run(reader.get_data(&ghost, &GetDataArgs::default())).unwrap();
            assert!(matches!(data, Data::Missing));
            let docs = run(reader.get_data(
                &ghost,
                &GetDataArgs {
                    as_docs: true,
                    ..Default::default()
                },
            ))
            .unwrap();
            assert!(matches!(docs, Data::Docs(d) if d.is_empty()));
            assert!(run(reader.infer_dtypes(&ghost, false)).unwrap().is_empty());
            assert_eq!(
                run(reader.get_rowcount(&ghost, None, None, None, false)).unwrap(),
                0
            );
            assert!(
                run(reader.get_sync_time(&ghost, None, true, false, false, false))
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn sync_times_and_rowcounts() {
        let (fx, pipe) = fixture("times", 12);
        for reader in [&fx.sql, &fx.generic] {
            let newest = run(reader.get_sync_time(&pipe, None, true, false, false, false)).unwrap();
            assert_eq!(newest, at(11));
            let oldest =
                run(reader.get_sync_time(&pipe, None, false, false, false, false)).unwrap();
            assert_eq!(oldest, at(0));
            // The default backtrack is a day: the sync time minus 1440 minutes.
            let back = run(reader.get_sync_time(&pipe, None, true, false, true, false)).unwrap();
            assert_eq!(
                back,
                Some(DateBound::Datetime(DateTimeValue::utc(
                    start() + Duration::hours(11) - Duration::minutes(1440)
                )))
            );
            let by_id = run(reader.get_sync_time(
                &pipe,
                params(serde_json::json!({"id": 1})).as_ref(),
                true,
                false,
                false,
                false,
            ))
            .unwrap();
            assert_eq!(by_id, at(10));
            assert_eq!(
                run(reader.get_rowcount(&pipe, None, None, None, false)).unwrap(),
                12
            );
            assert_eq!(
                run(reader.get_rowcount(&pipe, at(3), at(8), None, false)).unwrap(),
                5
            );
            assert_eq!(
                run(reader.get_rowcount(
                    &pipe,
                    None,
                    None,
                    params(serde_json::json!({"id": 0})),
                    false
                ))
                .unwrap(),
                4
            );
        }
    }

    #[test]
    fn chunk_plans() {
        let (fx, pipe) = fixture("chunks", 12);
        for reader in [&fx.sql, &fx.generic] {
            let plan = |interval| {
                run(reader.plan_chunks(
                    &pipe,
                    None,
                    None,
                    None,
                    Some(ChunkIntervalArg::Delta(TimeDelta::hours(interval))),
                ))
                .unwrap()
            };
            // Twelve hourly rows span 12 hours: chunks of 5 hours make three, 30 days one.
            assert!(matches!(plan(5), ChunkPlan::Bounds(b) if b.len() == 3));
            assert!(matches!(plan(24 * 30), ChunkPlan::Single));
            let ghost = pipe_without_table();
            assert!(matches!(
                run(reader.plan_chunks(&ghost, None, None, None, None)).unwrap(),
                ChunkPlan::Empty
            ));
        }
    }

    fn pipe_without_table() -> Pipe {
        pipe("no_table_yet")
    }

    #[test]
    fn backtrack_data_is_newest_first() {
        let (fx, pipe) = fixture("backtrack", 12);
        for reader in [&fx.sql, &fx.generic] {
            let data =
                run(reader.get_backtrack_data(&pipe, Some(180.0), GetDataArgs::default())).unwrap();
            let got = rows(&data);
            // The sync time (hour 11) minus 3 hours is hour 8: hours 8..=11, newest first.
            assert_eq!(got.len(), 4);
            assert!(got[0][0].contains("2026-01-01 11:00:00"), "{:?}", got[0]);
            assert!(got[3][0].contains("2026-01-01 08:00:00"), "{:?}", got[3]);
        }
    }

    #[test]
    fn filter_existing_splits_new_and_changed_rows() {
        let (fx, pipe) = fixture("filter", 12);
        let mut incoming: Vec<_> = (3..9).map(doc).collect();
        for d in incoming.iter_mut().skip(3) {
            d.insert("val".to_string(), Cell::Float(100.0));
        }
        incoming.extend((12..16).map(doc));
        for reader in [&fx.sql, &fx.generic] {
            let (unseen, update, delta) = run(reader.filter_existing(
                &pipe,
                PipeInput::Docs(incoming.clone()),
                &FilterOpts::default(),
            ))
            .unwrap()
            .frames;
            assert_eq!(unseen.height(), 4);
            assert_eq!(update.height(), 3);
            assert_eq!(delta.height(), 7);
        }
    }

    #[test]
    fn the_generic_deduplicate_removes_duplicates() {
        pyo3::Python::initialize();
        let (fx, pipe) = fixture("dedup", 6);
        // Duplicates around the sync's diff: write the rows again straight to the table.
        let conn = fx.sql.sql.clone().unwrap();
        let table = conn
            .sql_pipe(&pipe)
            .map(|sp| conn.pipe_table_name(&sp))
            .unwrap();
        run(conn.exec(&format!("INSERT INTO {table} SELECT * FROM {table}")));
        // A generic pass must see the duplicates and leave one of each.
        let before = run(fx.generic.get_rowcount(&pipe, None, None, None, false)).unwrap();
        let result = run(fx.generic.deduplicate(
            &pipe,
            &DedupArgs {
                use_instance_method: false,
                ..Default::default()
            },
        ));
        assert!(result.success, "{}", result.message);
        let after = run(fx.generic.get_rowcount(&pipe, None, None, None, false)).unwrap();
        assert!(after <= before);
        assert_eq!(after, 6, "{}", result.message);
    }

    #[test]
    fn helpers() {
        assert_eq!(
            distinct_len(&[vec!["a".into()], vec!["a".into()], vec!["b".into()]]),
            2
        );
        let frame = meerschaum_kernel::docs::frame_from_docs(&[doc(1), doc(1), doc(2)]).unwrap();
        let out = drop_duplicates_last(&frame, &["dt".to_string(), "id".to_string()]);
        assert_eq!(out.height(), 2);
        let a = DateBound::Int(5);
        assert_eq!(
            add_interval(&a, &ChunkInterval::Int(3)),
            Some(DateBound::Int(8))
        );
        assert_eq!(add_interval(&a, &ChunkInterval::minutes(3)), None);
        assert_eq!(
            sub_interval(&a, &ChunkInterval::Int(3)),
            Some(DateBound::Int(2))
        );
        assert!(bound_lt(&DateBound::Int(1), &DateBound::Int(2)));
    }
}
