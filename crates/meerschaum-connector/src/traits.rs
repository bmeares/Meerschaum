//! The `Connector` and `InstanceConnector` traits (port of `meerschaum/connectors/_Connector.py`
//! and `meerschaum/connectors/instance/_InstanceConnector.py`).
//!
//! These traits are shared by every instance connector (`meerschaum-connector-sql`, `meerschaum-connector-valkey`,
//! the `APIConnector` in `meerschaum-connector-api`), the engine and the server. They need
//! `meerschaum-core` (keys, parameters, users, tokens, `SuccessTuple`) and `meerschaum-kernel`
//! (`Frame`).
//!
//! Every method mirrors one Python method; its doc comment carries the Python docstring's
//! contract. Methods that return a `SuccessTuple` in Python return [`SuccessTuple`] here (failures
//! are values, never errors). Methods that return data return [`ConnectorResult`], where `Err`
//! replaces a Python exception (or a `NotImplementedError` for an unimplemented optional method).
//!
//! Default implementations follow the Python base class: the optional maintenance methods return
//! the same failure messages, and users, plugins and tokens are implemented generically on top of
//! the internal pipes (`get_users_pipe()`, `get_plugins_pipe()`, `get_tokens_pipe()`) exactly as
//! `meerschaum/connectors/instance/_users.py`, `_plugins.py` and `_tokens.py` do.

use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_core::users::{Token, User};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use async_trait::async_trait;

use crate::models::TokenModel;
use crate::pipe_ops;
use crate::schema::SchemaPlan;

/// The error type for connector methods that return data.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// The method is not implemented for this connector type (Python `NotImplementedError`).
    #[error("{0}")]
    NotImplemented(String),
    /// The backend (database, Valkey server, HTTP transport) failed.
    #[error("{0}")]
    Backend(String),
    /// An HTTP response with an error status.
    #[error("HTTP {status}: {body}")]
    Http {
        /// The status code.
        status: u16,
        /// The response body.
        body: String,
    },
    /// A FastAPI-style `{"detail": ...}` error response.
    #[error("{0}")]
    Detail(String),
    /// Invalid input or an unparseable response.
    #[error("{0}")]
    Invalid(String),
    /// JSON (de)serialization failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// The result type for connector methods that return data.
pub type ConnectorResult<T> = Result<T, ConnectorError>;

/// A pipe's ID on its instance: an integer on SQL and Valkey, a string on custom instances (§5.1).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PipeId {
    /// An integer ID.
    Int(i64),
    /// A string ID.
    Str(String),
}

impl fmt::Display for PipeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipeId::Int(i) => write!(f, "{i}"),
            PipeId::Str(s) => f.write_str(s),
        }
    }
}

impl PipeId {
    /// Parse a key as Python does for the dict form of `fetch_pipes_keys`
    /// (`int(k) if str(k).isdigit() else k`).
    pub fn parse(s: &str) -> PipeId {
        if !s.is_empty()
            && s.chars().all(|c| c.is_ascii_digit())
            && let Ok(i) = s.parse()
        {
            return PipeId::Int(i);
        }
        PipeId::Str(s.to_string())
    }

    /// The ID as a core [`Value`].
    pub fn to_value(&self) -> Value {
        match self {
            PipeId::Int(i) => Value::Int(*i),
            PipeId::Str(s) => Value::Str(s.clone()),
        }
    }
}

/// A bound on a pipe's datetime axis (`begin`, `end`, a sync time): a timestamp or an integer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DateBound {
    /// A timestamp (aware or naive).
    Datetime(DateTimeValue),
    /// An integer axis value.
    Int(i64),
}

impl DateBound {
    /// A UTC timestamp.
    pub fn utc(dt: DateTime<Utc>) -> DateBound {
        DateBound::Datetime(DateTimeValue::utc(dt.naive_utc()))
    }

    /// Python's `str(value)` (`2024-01-01 00:00:00+00:00`), as `requests` encodes query params.
    pub fn py_str(&self) -> String {
        match self {
            DateBound::Datetime(dt) => KValue::Datetime(dt.clone()).py_str(),
            DateBound::Int(i) => i.to_string(),
        }
    }

    /// `value.isoformat()` for timestamps, `str(value)` for integers.
    pub fn isoformat(&self) -> String {
        match self {
            DateBound::Datetime(dt) => dt.isoformat(),
            DateBound::Int(i) => i.to_string(),
        }
    }

    /// The bound as a kernel value.
    pub fn to_value(&self) -> KValue {
        match self {
            DateBound::Datetime(dt) => KValue::Datetime(dt.clone()),
            DateBound::Int(i) => KValue::Int(*i),
        }
    }

    /// A bound from a kernel value (datetimes and integers only).
    pub fn from_value(value: &KValue) -> Option<DateBound> {
        match value {
            KValue::Datetime(dt) => Some(DateBound::Datetime(dt.clone())),
            KValue::Int(i) => Some(DateBound::Int(*i)),
            _ => None,
        }
    }

    /// Parse an integer or an ISO-8601 timestamp.
    pub fn parse(s: &str) -> Option<DateBound> {
        let t = s.trim();
        if let Ok(i) = t.parse::<i64>() {
            return Some(DateBound::Int(i));
        }
        meerschaum_kernel::value::parse_datetime_value(t).map(DateBound::Datetime)
    }
}

impl From<meerschaum_core::success::Bound> for DateBound {
    fn from(b: meerschaum_core::success::Bound) -> Self {
        match b {
            meerschaum_core::success::Bound::Datetime(dt) => DateBound::utc(dt),
            meerschaum_core::success::Bound::Int(i) => DateBound::Int(i),
        }
    }
}

/// The filters shared by `get_pipe_data`, `get_pipe_docs`, `clear_pipe` and
/// `get_pipe_rowcount`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DataQuery {
    /// `select_columns`: only select these columns.
    pub select_columns: Option<Vec<String>>,
    /// `omit_columns`: remove these columns from the selection.
    pub omit_columns: Option<Vec<String>>,
    /// `begin`: the earliest datetime-axis value (inclusive).
    pub begin: Option<DateBound>,
    /// `end`: the latest datetime-axis value (exclusive).
    pub end: Option<DateBound>,
    /// `params`: the `build_where` / `query_df` filter grammar.
    pub params: Option<Map>,
    /// `limit`.
    pub limit: Option<i64>,
    /// `order` (`'asc'` / `'desc'`).
    pub order: Option<String>,
    /// `as_chunks` (sent by the API client; ignored by most instances).
    pub as_chunks: bool,
    /// `remote` (rowcount of the fetch definition instead of the target table).
    pub remote: bool,
}

/// Keyword arguments for `sync_pipe`.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncOptions {
    /// `check_existing`: diff against existing rows (default `True`).
    pub check_existing: bool,
    /// `chunksize`: `Some(-1)` (default) uses `system:connectors:sql:chunksize`; `None` syncs
    /// everything in one chunk.
    pub chunksize: Option<i64>,
    /// `begin` / `end` (SQL fetch bounds). With [`SyncOptions::replace`], the window
    /// `[begin, end)` whose rows the frame replaces (`None`: unbounded on that side).
    pub begin: Option<DateBound>,
    /// See `begin`.
    pub end: Option<DateBound>,
    /// `replace`: in one transaction (where the instance has transactions), delete the pipe's
    /// rows in `[begin, end)` that match [`SyncOptions::params`], then write the frame's rows
    /// of that window. Rows of the frame outside the window are not written. A pipe without a
    /// datetime column has one window: the whole table. An empty frame clears the window.
    pub replace: bool,
    /// `params`: with [`SyncOptions::replace`], only the window's rows matching these filters
    /// are replaced (the `params` grammar of `build_where`).
    pub params: Option<Map>,
    /// Any other keyword arguments (the API client forwards them as query parameters).
    pub extra: Map,
}

impl Default for SyncOptions {
    fn default() -> Self {
        SyncOptions {
            check_existing: true,
            chunksize: Some(-1),
            begin: None,
            end: None,
            replace: false,
            params: None,
            extra: Map::new(),
        }
    }
}

/// The filters of `fetch_pipes_keys` (`-c`, `-m`, `-l`, `--tags`, `params`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PipesKeysQuery {
    /// `connector_keys` (`-c`); a `_` prefix negates.
    pub connector_keys: Vec<String>,
    /// `metric_keys` (`-m`).
    pub metric_keys: Vec<String>,
    /// `location_keys` (`-l`); `None` matches null locations.
    pub location_keys: Vec<Option<String>>,
    /// `tags` (`--tags`): comma-separated groups are ANDed, groups are ORed.
    pub tags: Vec<String>,
    /// `params`: extra filters against the pipes table.
    pub params: Option<Map>,
}

/// One entry of `fetch_pipes_keys`: `(connector_keys, metric_key, location_key[, extra])`.
#[derive(Debug, Clone, PartialEq)]
pub struct PipeKeysEntry {
    /// The connector keys.
    pub connector_keys: String,
    /// The metric key.
    pub metric_key: String,
    /// The location key (`None` or the string `"None"` for null locations).
    pub location_key: Option<String>,
    /// The optional fourth element: the parameters dict or the tags list.
    pub extra: Option<Value>,
}

impl PipeKeysEntry {
    /// Build an entry from a JSON list `[ck, mk, lk, extra?]`.
    pub fn from_list(items: &[Value]) -> Option<PipeKeysEntry> {
        let s = |v: &Value| match v {
            Value::Null => None,
            other => Some(meerschaum_core::pyfmt::py_str(other)),
        };
        Some(PipeKeysEntry {
            connector_keys: s(items.first()?)?,
            metric_key: s(items.get(1)?)?,
            location_key: items.get(2).and_then(s),
            extra: items.get(3).cloned(),
        })
    }

    /// The entry as a JSON-like list.
    pub fn to_list(&self) -> Vec<Value> {
        let mut out = vec![
            Value::Str(self.connector_keys.clone()),
            Value::Str(self.metric_key.clone()),
            self.location_key.clone().into(),
        ];
        if let Some(extra) = &self.extra {
            out.push(extra.clone());
        }
        out
    }
}

/// The return value of `fetch_pipes_keys`: a list of key tuples, or a dict mapping pipe IDs to
/// key tuples (the dict form returns parameters in one round trip, §5.1).
#[derive(Debug, Clone, PartialEq)]
pub enum PipesKeys {
    /// `[(ck, mk, lk[, extra]), ...]`.
    List(Vec<PipeKeysEntry>),
    /// `{pipe_id: (ck, mk, lk[, extra])}`.
    Dict(IndexMap<PipeId, PipeKeysEntry>),
}

impl PipesKeys {
    /// The entries regardless of form, in order.
    pub fn entries(&self) -> Vec<&PipeKeysEntry> {
        match self {
            PipesKeys::List(v) => v.iter().collect(),
            PipesKeys::Dict(m) => m.values().collect(),
        }
    }

    /// The number of entries.
    pub fn len(&self) -> usize {
        match self {
            PipesKeys::List(v) => v.len(),
            PipesKeys::Dict(m) => m.len(),
        }
    }

    /// Whether there are no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One index on a column (`get_pipe_columns_indices`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnIndexInfo {
    /// The index name.
    pub name: String,
    /// The index type (`'INDEX'`, `'PRIMARY KEY'`, ...).
    #[serde(rename = "type")]
    pub index_type: String,
}

/// A plugin's registration (`meerschaum.core.Plugin` as instances see it). Plugins themselves
/// stay in Python (§6.1); instances only store these fields.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PluginRecord {
    /// The plugin name.
    pub name: String,
    /// The plugin version.
    pub version: Option<String>,
    /// The owner's user ID.
    pub user_id: Option<Value>,
    /// `plugin.attributes`.
    pub attributes: Map,
}

impl fmt::Display for PluginRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

/// A base connector (`meerschaum.connectors.Connector`): `type:label` keys and a source of data.
#[async_trait]
pub trait Connector: Send + Sync {
    /// The connector type (`'sql'`, `'api'`, `'valkey'`, ...).
    fn connector_type(&self) -> &str;

    /// The connector label.
    fn label(&self) -> &str;

    /// `str(connector)`: `'type:label'`.
    fn connector_keys(&self) -> String {
        format!("{}:{}", self.connector_type(), self.label())
    }

    /// `IS_THREAD_SAFE`: whether concurrent reads may share this connector.
    fn is_thread_safe(&self) -> bool {
        false
    }

    /// `connector.protected`: whether the connector's attributes mark it protected
    /// (`protected: true`). Every mutation the core makes through a protected connector is
    /// refused unless writes are allowed (`--allow-protected`, `MRSM_ALLOW_PROTECTED=1`; see
    /// [`meerschaum_core::protected`] and [`crate::policy::protected_refusal`]). Reads are
    /// never blocked. The default is `false`.
    fn is_protected(&self) -> bool {
        false
    }

    /// Return whether a connection may be established.
    async fn test_connection(&self) -> ConnectorResult<bool>;

    /// Return new data for `pipe` between `begin` and `end` (`fetch(pipe, begin, end, params)`).
    ///
    /// Source connectors return a frame of new rows; the default raises `NotImplementedError`.
    async fn fetch(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Option<Frame>> {
        let _ = (pipe, query);
        Err(ConnectorError::NotImplemented(format!(
            "`fetch()` is not implemented for connectors of type '{}'.",
            self.connector_type()
        )))
    }

    /// The source's own `get_sync_time(pipe, params, newest, remote=True)`
    /// (`Pipe.get_sync_time(remote=True)`): the newest (oldest) datetime-axis value of the
    /// remote table. The default is `NotImplemented` with v4's warning text.
    async fn get_sync_time_remote(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        let _ = (pipe, params, newest);
        Err(ConnectorError::NotImplemented(format!(
            "Connectors of type '{}' do not implement `get_sync_time().",
            self.connector_type()
        )))
    }

    /// The source's own `get_pipe_rowcount(pipe, begin, end, params, remote=True)`
    /// (`Pipe.get_rowcount(remote=True)`). The default is `NotImplemented` with v4's warning
    /// text.
    async fn get_rowcount_remote(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        let _ = (pipe, query);
        Err(ConnectorError::NotImplemented(format!(
            "Connectors of type '{}' do not implement `get_pipe_rowcount()`.",
            self.connector_type()
        )))
    }

    /// [`Connector::fetch`] for sources whose result is not always one frame: Python plugins
    /// and custom connectors, whose `fetch()` may return a generator of chunks, the results of
    /// the `chunk_hook` calls it made, `True` or `None`. The default returns `Ok(None)`: the
    /// native flow then calls [`Connector::fetch`] and syncs its frame.
    ///
    /// `sync_chunks` is `Pipe.sync(sync_chunks=...)`: whether the source is given a
    /// `chunk_hook` that syncs a chunk as soon as the source has produced it.
    async fn fetch_chunked(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
        sync_chunks: bool,
    ) -> ConnectorResult<Option<ChunkedFetch>> {
        let _ = (pipe, query, sync_chunks);
        Ok(None)
    }
}

/// A lazily produced sequence of fetched chunks (a Python generator, consumed as the source
/// produces it).
#[async_trait]
pub trait ChunkStream: Send {
    /// The next chunk, or `None` when the sequence has ended. A source that fails while
    /// producing chunks returns `Err` (v4: the exception propagates out of the generator).
    async fn next_chunk(
        &mut self,
    ) -> ConnectorResult<Option<meerschaum_kernel::enforce::PipeInput>>;

    /// What `fetch()` returned besides chunks, once the sequence has ended:
    /// the source's own [`FetchedKind`] (hook results, "synced in parallel", `None`).
    async fn finish(&mut self) -> ConnectorResult<FetchedKind>;
}

/// How a source's `fetch()` result is shaped when it is consumed in pieces (v4's `_sync`
/// branches on exactly these).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchedKind {
    /// Data: a DataFrame, a list of documents, a dictionary of lists or a generator of those.
    Data {
        /// `fetch()` returned a generator (v4: `df_is_chunk_generator`), so every chunk is
        /// synced on its own and the messages are combined.
        generator: bool,
    },
    /// `fetch()` returned the `(success, message)` results of the `chunk_hook` calls it made
    /// itself (a list of tuples).
    HookResults(Vec<SuccessTuple>),
    /// `fetch()` returned `True`: the source syncs in parallel itself.
    InParallel,
    /// `fetch()` returned `None`.
    Nothing,
}

/// The result of [`Connector::fetch_chunked`].
pub struct ChunkedFetch {
    /// The chunks, in order (possibly none).
    pub chunks: Box<dyn ChunkStream>,
}

/// Instance connectors manage pipes and provide users, plugins, tokens and engine state built
/// atop pipes (`meerschaum.connectors.InstanceConnector`).
#[async_trait]
pub trait InstanceConnector: Connector {
    // ----------------------------------------------------------------------------------------
    // Pipes
    // ----------------------------------------------------------------------------------------

    /// Insert the pipe's attributes into the internal `pipes` table.
    ///
    /// Returns a `SuccessTuple` of the result.
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple;

    /// Return the pipe's document from the internal `pipes` table: the keys, `parameters` and
    /// `pipe_id`. Return an empty map if the pipe is not registered.
    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map>;

    /// Return the `id` for the pipe if it exists, else `None`.
    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>>;

    /// Edit the attributes of the pipe: persist its in-memory parameters.
    ///
    /// `patch`: if `true`, patch the existing parameters instead of replacing them (API only).
    async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        let _ = (pipe, patch);
        SuccessTuple::fail(format!(
            "`edit_pipe()` is not implemented for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// Delete a pipe's registration from the `pipes` collection (and drop its table).
    async fn delete_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let _ = pipe;
        SuccessTuple::fail(format!(
            "`delete_pipe()` is not implemented for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// Return registered pipes' keys according to the provided filters.
    ///
    /// May return either a list of key tuples or a dictionary mapping pipe IDs to key tuples;
    /// tuples may carry the parameters (or tags) as a fourth element. Location keys may be
    /// returned as the string `"None"` in place of nulls.
    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys>;

    /// Fresh registration identities and parameters for authorization and metrics.
    ///
    /// Dictionary backends already return metadata in bulk. List backends such as Valkey
    /// store parameters separately; read those attributes rather than treating their empty
    /// fourth tuple element as a complete access policy. The public key-list wire shape is
    /// unchanged.
    async fn registered_pipes(
        &self,
        query: &PipesKeysQuery,
    ) -> ConnectorResult<Vec<(Option<PipeId>, PipeKeysEntry)>> {
        match self.fetch_pipes_keys(query).await? {
            PipesKeys::Dict(entries) => Ok(entries
                .into_iter()
                .map(|(identity, entry)| (Some(identity), entry))
                .collect()),
            PipesKeys::List(entries) => {
                let mut registered = Vec::with_capacity(entries.len());
                for mut entry in entries {
                    if entry.location_key.as_deref() == Some("None") {
                        entry.location_key = None;
                    }
                    let pipe = Pipe::builder(&entry.connector_keys, &entry.metric_key)
                        .location(entry.location_key.as_deref().filter(|key| *key != "None"))
                        .instance(&self.connector_keys())
                        .build()
                        .map_err(|error| ConnectorError::Invalid(error.to_string()))?;
                    let attributes = self.get_pipe_attributes(&pipe).await?;
                    if let Some(parameters) = attributes.get("parameters") {
                        entry.extra = Some(parameters.clone());
                    }
                    let identity = attributes.get("pipe_id").and_then(|value| match value {
                        Value::Int(identity) => Some(PipeId::Int(*identity)),
                        Value::Str(identity) => Some(PipeId::parse(identity)),
                        _ => None,
                    });
                    registered.push((identity, entry));
                }
                Ok(registered)
            }
        }
    }

    /// Bounded internal metrics/run/node candidates and whether the backend scan was capped.
    /// SQL pushes bounds and limits into its query. Backends which filter after reading
    /// must override this method to bound both transferred candidates and document fetches.
    async fn read_engine_metric_rows(
        &self,
        metric: &str,
        query: &DataQuery,
    ) -> ConnectorResult<(Vec<crate::pipe_ops::Doc>, bool)> {
        let Some(pipe) = self
            .engine_state_pipe(&self.connector_keys(), metric)
            .await?
        else {
            return Ok((Vec::new(), false));
        };
        let docs = self.get_pipe_docs(&pipe, query).await?;
        let truncated = query
            .limit
            .is_some_and(|limit| docs.len() >= limit.max(0) as usize);
        Ok((docs, truncated))
    }

    /// `{{ Pipe(...) }}` without an attribute chain: the table a template for this pipe expands to
    /// on this instance (`sql_item_name` on SQL instances). `None` means `pipe.target`.
    fn pipe_table_reference(&self, pipe: &Pipe) -> Option<String> {
        let _ = pipe;
        None
    }

    /// Physical target aliases used to authorize a pipe before accessing its data. SQL
    /// instances include identifier truncation and any database byte limit; this must not
    /// require a connection or change the pipe's parameters.
    fn pipe_target_aliases(&self, pipe: &Pipe) -> Vec<String> {
        self.pipe_table_reference(pipe).into_iter().collect()
    }

    /// The database's byte limit for SQL identifiers, when it silently clips longer
    /// identifiers supplied in queries (`None`: no such normalization).
    fn sql_identifier_byte_limit(&self) -> Option<usize> {
        None
    }

    /// Check whether a pipe's target table exists.
    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool>;

    /// Drop a pipe's target table (keep its registration).
    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple;

    /// Drop a pipe's indices.
    async fn drop_pipe_indices(&self, pipe: &Pipe) -> SuccessTuple {
        let _ = pipe;
        SuccessTuple::fail(format!(
            "Cannot drop indices for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// Create a pipe's indices.
    async fn create_pipe_indices(&self, pipe: &Pipe) -> SuccessTuple {
        let _ = pipe;
        SuccessTuple::fail(format!(
            "Cannot create indices for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// Sync a DataFrame into a pipe.
    ///
    /// `df` is the data to sync. With `check_existing`, existing rows are pulled and diffed
    /// (inserts vs updates); otherwise rows are inserted directly. `chunksize` `-1` resorts to
    /// `system:connectors:sql:chunksize` (900); `None` syncs all rows in one transaction.
    async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple;

    /// [`InstanceConnector::sync_pipe`] with the row counts of the write, for the callers that
    /// keep them (the engine's sync flow: run totals, sync statuses, changed ranges). The
    /// message is `sync_pipe`'s. The default reads the counts from it
    /// ([`pipe_ops::sync_result_from_message`]); an instance whose message does not carry them
    /// (the `api:` client's `It took ... to sync 3 rows across 1 chunk ...`) overrides this.
    async fn sync_pipe_with_counts(
        &self,
        pipe: &Pipe,
        df: Frame,
        opts: &SyncOptions,
    ) -> meerschaum_core::success::SyncResult {
        let res = self.sync_pipe(pipe, df, opts).await;
        pipe_ops::sync_result_from_message(res.success, res.message)
    }

    /// Delete rows within `begin`, `end` and `params`.
    ///
    /// `begin`: remove rows `>= begin`; `end`: remove rows `< end`; `params`: only remove rows
    /// which match the filter.
    async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        let _ = (pipe, query);
        SuccessTuple::fail(format!(
            "`clear_pipe()` is not implemented for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// Query a pipe's target table and return the DataFrame (`None` if the table does not exist).
    ///
    /// `select_columns`, `omit_columns`, `begin` (inclusive), `end` (exclusive) and `params`
    /// filter the result.
    async fn get_pipe_data(&self, pipe: &Pipe, query: &DataQuery)
    -> ConnectorResult<Option<Frame>>;

    /// Return a pipe's data as a list of documents. Defaults to
    /// `get_pipe_data().to_dict(orient='records')`.
    async fn get_pipe_docs(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Vec<IndexMap<String, KValue>>> {
        Ok(match self.get_pipe_data(pipe, query).await? {
            Some(df) => pipe_ops::frame_to_docs(&df),
            None => Vec::new(),
        })
    }

    /// The connector as the `Pipe` methods call it with extra keyword arguments
    /// (`Pipe.get_data(**kw)`, `deduplicate(**kwargs)`: v4 forwards them to the instance's
    /// methods, which a custom connector may read). `None` (the default): the connector takes
    /// no extra keywords.
    fn with_call_kwargs(&self, extra: &Map) -> Option<std::sync::Arc<dyn InstanceConnector>> {
        let _ = extra;
        None
    }

    /// Whether the connector defines its own `get_backtrack_data` (v4's
    /// `hasattr(instance_connector, 'get_backtrack_data')` in `Pipe.get_backtrack_data`): the
    /// `Pipe` calls it instead of reading the window itself. Only Python connectors that
    /// define the method say `true`.
    fn has_backtrack_data(&self) -> bool {
        false
    }

    /// Return the backtrack window: rows newer than the sync time minus `backtrack_minutes`
    /// (`Pipe.get_backtrack_data`). Defaults to `get_sync_time` plus `get_pipe_data`.
    async fn get_backtrack_data(
        &self,
        pipe: &Pipe,
        backtrack_minutes: i64,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        let sync_time = self
            .get_sync_time(pipe, query.params.as_ref(), true)
            .await?;
        let begin = match (&query.begin, sync_time) {
            (Some(b), _) => Some(b.clone()),
            (None, Some(DateBound::Int(i))) => Some(DateBound::Int(i - backtrack_minutes)),
            (None, Some(DateBound::Datetime(dt))) => Some(DateBound::Datetime(DateTimeValue {
                utc: dt.utc - chrono::Duration::minutes(backtrack_minutes),
                tz: dt.tz,
            })),
            (None, None) => None,
        };
        let q = DataQuery {
            begin,
            ..query.clone()
        };
        self.get_pipe_data(pipe, &q).await
    }

    /// Return the most recent (or with `newest = false`, the oldest) value of the datetime axis,
    /// filtered by `params`.
    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>>;

    /// Return the number of rows in the pipe's target table within `begin`, `end` and `params`
    /// (`remote`: count the fetch definition instead). `None` if it cannot be determined.
    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>>;

    /// `Pipe.get_rowcount(estimate=True)`: the database's estimate of the number of rows in the
    /// pipe's table (catalog statistics, no table scan), falling back to the exact count where
    /// no estimate exists or counting is cheap (small tables, file databases). `None` for a
    /// missing table. With `begin`, `end`, `params` or `remote` the count is exact. The default
    /// is the exact count ([`Self::get_pipe_rowcount`]).
    async fn get_pipe_rowcount_estimate(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        self.get_pipe_rowcount(pipe, query).await
    }

    /// Return the data types for the columns in the target table for data type enforcement.
    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>>;

    /// Return a dictionary mapping columns to metadata about related indices
    /// (`{"col": [{"name": ..., "type": ...}]}`). Defaults to `{}`.
    async fn get_pipe_columns_indices(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, Vec<ColumnIndexInfo>>> {
        let _ = pipe;
        Ok(IndexMap::new())
    }

    /// Return the templated index names (`Pipe.get_indices()`), keyed by index role.
    async fn get_pipe_index_names(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        let _ = pipe;
        Err(ConnectorError::NotImplemented(format!(
            "`get_pipe_index_names()` is not implemented for instance connectors of type '{}'.",
            self.connector_type()
        )))
    }

    /// Delete duplicate rows (`Pipe.deduplicate`). Instances without a native implementation
    /// fall back to the generic chunked implementation in the engine.
    async fn deduplicate_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        let _ = (pipe, query);
        SuccessTuple::fail(format!(
            "Deduplication is not supported for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// Return the on-disk size of a pipe's target table in bytes, or `None` if it cannot be
    /// determined.
    async fn get_pipe_size(&self, pipe: &Pipe) -> ConnectorResult<Option<i64>> {
        let _ = pipe;
        Err(ConnectorError::NotImplemented(format!(
            "`get_pipe_size()` is not implemented for instance connectors of type '{}'.",
            self.connector_type()
        )))
    }

    /// Compress a pipe's target table to reduce disk usage.
    async fn compress_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let _ = pipe;
        SuccessTuple::fail(format!(
            "Compression is not supported for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// `apply_compression_policy(pipe)`: idempotently install the pipe's compression policy.
    /// `Pipe.sync()` calls it after a successful sync when `compress` is set; instances without
    /// one (v4: no such method) do nothing.
    async fn apply_compression_policy(&self, pipe: &Pipe) -> SuccessTuple {
        let _ = pipe;
        SuccessTuple::success()
    }

    /// `get_tables(mrsm_instance=self, create=True)`: create the internal instance tables
    /// (`POST /metadata`). Instances without tables of their own answer `true`.
    async fn create_instance_tables(&self) -> ConnectorResult<bool> {
        Ok(true)
    }

    /// Decompress a pipe's target table, the inverse of `compress_pipe()`. `no_policy` keeps the
    /// compression policy in place.
    async fn decompress_pipe(&self, pipe: &Pipe, no_policy: bool) -> SuccessTuple {
        let _ = (pipe, no_policy);
        SuccessTuple::fail(format!(
            "Decompression is not supported for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// Reclaim disk space from a pipe's target table (`full`: `VACUUM FULL`).
    async fn vacuum_pipe(&self, pipe: &Pipe, full: bool) -> SuccessTuple {
        let _ = (pipe, full);
        SuccessTuple::fail(format!(
            "Vacuuming is not supported for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// Refresh the planner statistics for a pipe's target table.
    async fn analyze_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let _ = pipe;
        SuccessTuple::fail(format!(
            "Analyzing is not supported for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// Rebuild a pipe's target table to a new partition (chunk) width (`chunk_minutes`, default
    /// the pipe's `verify.chunk_minutes`).
    async fn partition_pipe(&self, pipe: &Pipe, chunk_minutes: Option<i64>) -> SuccessTuple {
        let _ = (pipe, chunk_minutes);
        SuccessTuple::fail(format!(
            "Repartitioning is not supported for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    /// `verify schema` / `Pipe.verify_schema()`: how the pipe's table differs from its declared
    /// dtypes (`dtypes`, the `columns` roles, geometry notations; `static` pipes included).
    /// Nothing is written. The default is unsupported.
    async fn get_pipe_schema_plan(&self, pipe: &Pipe) -> ConnectorResult<SchemaPlan> {
        let _ = pipe;
        Err(ConnectorError::NotImplemented(format!(
            "Verifying table schemas is not supported for instance connectors of type '{}'.",
            self.connector_type()
        )))
    }

    /// Apply a plan made by [`Self::get_pipe_schema_plan`]: every change, or with `safe_only`
    /// only the safe ones ([`crate::schema::SchemaChange::is_safe`]). The instance plans again
    /// first and refuses when the table no longer matches `plan`; it never drops a column.
    async fn apply_pipe_schema(
        &self,
        pipe: &Pipe,
        plan: &SchemaPlan,
        safe_only: bool,
    ) -> SuccessTuple {
        let _ = (pipe, plan, safe_only);
        SuccessTuple::fail(format!(
            "Verifying table schemas is not supported for instance connectors of type '{}'.",
            self.connector_type()
        ))
    }

    // ----------------------------------------------------------------------------------------
    // Leases (§5.8)
    // ----------------------------------------------------------------------------------------

    /// Atomically acquire (or, for the current holder, extend) the lease on `pipe` for `ttl`.
    /// Returns `true` if `holder` now holds the lease.
    ///
    /// The default is `NotImplemented`: the engine then falls back to interprocess file locks
    /// with a one-time warning that the lease is machine-local.
    async fn acquire_pipe_lease(
        &self,
        pipe: &Pipe,
        holder: &str,
        ttl: Duration,
    ) -> ConnectorResult<bool> {
        let _ = (pipe, holder, ttl);
        Err(ConnectorError::NotImplemented(format!(
            "Leases are not implemented for instance connectors of type '{}'.",
            self.connector_type()
        )))
    }

    /// Release the lease on `pipe` if `holder` holds it. Returns `true` if a lease was released.
    async fn release_pipe_lease(&self, pipe: &Pipe, holder: &str) -> ConnectorResult<bool> {
        let _ = (pipe, holder);
        Err(ConnectorError::NotImplemented(format!(
            "Leases are not implemented for instance connectors of type '{}'.",
            self.connector_type()
        )))
    }

    /// The Syncing Engine's state pipe `mrsm:<metric_key>` on this instance (§5.8), keyed under
    /// `instance_keys`; `None` for a metric that is not engine state. The default keeps it
    /// beside the instance's other tables (target `mrsm_<metric_key>`,
    /// [`pipe_ops::engine_state_pipe`]); the SQL connector places it in the internal schema,
    /// namespaced by the instance schema.
    async fn engine_state_pipe(
        &self,
        instance_keys: &str,
        metric_key: &str,
    ) -> ConnectorResult<Option<Pipe>> {
        Ok(pipe_ops::engine_state_pipe(instance_keys, metric_key))
    }

    // ----------------------------------------------------------------------------------------
    // Per-pipe sync status (rc15 contract C2, [`crate::sync_status`])
    // ----------------------------------------------------------------------------------------

    /// Merge the outcomes of syncs of `pipe` (oldest first) into its sync status on this
    /// instance and return the stored status. The default reads the pipe's row of the internal
    /// pipe `mrsm:engine_sync_status`, applies the outcomes and upserts it (one read, one
    /// write); the `api:` client sends them to its server instead. Sync paths call
    /// [`crate::sync_status::record`], which debounces and never fails a sync, not this.
    async fn record_sync_status(
        &self,
        pipe: &Pipe,
        outcomes: &[crate::sync_status::SyncOutcome],
    ) -> ConnectorResult<Option<crate::sync_status::SyncStatus>> {
        crate::sync_status::merge_and_store(self, pipe, outcomes).await
    }

    /// The sync status of `pipe`: its latest recorded sync (`None` if none was recorded).
    async fn get_sync_status(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<Option<crate::sync_status::SyncStatus>> {
        crate::sync_status::read_status(self, pipe).await
    }

    /// The sync statuses of every pipe recorded on this instance, keyed under `instance_keys`
    /// (one per pipe, ordered by `pipe_ref`).
    async fn get_sync_statuses(
        &self,
        instance_keys: &str,
    ) -> ConnectorResult<Vec<crate::sync_status::SyncStatus>> {
        crate::sync_status::read_statuses(self, instance_keys).await
    }

    /// Persist numeric sync history. API clients forward it to the served instance.
    async fn record_sync_metrics(
        &self,
        metrics: &[crate::sync_metrics::SyncMetric],
    ) -> ConnectorResult<()> {
        crate::sync_metrics::store(self, metrics).await
    }

    /// Bounded sync and Pipeline performance history, filtered before aggregation.
    async fn get_engine_metrics(
        &self,
        query: &crate::sync_metrics::MetricsQuery,
    ) -> ConnectorResult<crate::sync_metrics::MetricsResponse> {
        crate::sync_metrics::read(self, query).await
    }

    /// Create empty native metrics storage and indexes for a new instance.
    async fn initialize_engine_metrics(&self) -> ConnectorResult<()> {
        crate::sync_metrics::initialize(self).await
    }

    /// Expire old metric outcomes. SQL deletes through its timestamp index; stores with
    /// per-document keys override this to delete a bounded batch without scanning history.
    async fn expire_engine_metrics(&self, before_micros: i64) -> ConnectorResult<()> {
        let pipe = crate::sync_metrics::metrics_pipe(self).await?;
        let result = self
            .clear_pipe(
                &pipe,
                &DataQuery {
                    end: Some(DateBound::Int(before_micros)),
                    ..Default::default()
                },
            )
            .await;
        if result.success {
            Ok(())
        } else {
            Err(ConnectorError::Backend(result.message))
        }
    }

    // ----------------------------------------------------------------------------------------
    // Users (`instance/_users.py`)
    // ----------------------------------------------------------------------------------------

    /// Return the pipe used for users registration (`mrsm:users`, target `mrsm_users`).
    fn get_users_pipe(&self) -> Pipe {
        pipe_ops::users_pipe(&self.connector_keys())
    }

    /// Register a new user to the users pipe (assigns a new UUID `user_id`).
    async fn register_user(&self, user: &mut User) -> SuccessTuple {
        pipe_ops::default_register_user(self, user).await
    }

    /// Return a user's ID from the username.
    ///
    /// The username is matched exactly ([`pipe_ops::user_id_of_username`]): a `params` filter
    /// would read a leading `_` as a negation and match another user's row.
    async fn get_user_id(&self, user: &User) -> ConnectorResult<Option<Value>> {
        pipe_ops::user_id_of_username(self, &user.username).await
    }

    /// Return the username from the given ID (matched exactly).
    async fn get_username(&self, user_id: &Value) -> ConnectorResult<Option<Value>> {
        pipe_ops::username_of_user_id(self, user_id).await
    }

    /// Get the registered usernames.
    async fn get_users(&self) -> ConnectorResult<Vec<String>> {
        let pipe = self.get_users_pipe();
        let docs = pipe_ops::get_docs(self, &pipe, &DataQuery::default()).await?;
        Ok(docs
            .iter()
            .filter_map(|d| d.get("username"))
            .map(|v| v.py_str())
            .collect())
    }

    /// Edit the attributes for an existing user.
    async fn edit_user(&self, user: &User) -> SuccessTuple {
        pipe_ops::default_edit_user(self, user).await
    }

    /// Delete a user from the users table, then remove them from every team and clear their
    /// ownership of pipes ([`crate::teams::forget_user`]; the pipes stay with their team).
    async fn delete_user(&self, user: &User) -> SuccessTuple {
        let user_id = match pipe_ops::resolve_user_id(self, user).await {
            Ok(Some(id)) if !id.is_null() => id,
            // FX1: an unknown user is not deleted (a `user_id IS NULL` filter is nobody's).
            Ok(_) => {
                return SuccessTuple::fail(format!(
                    "User '{}' is not registered and cannot be deleted.",
                    user.username
                ));
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        if !pipe_ops::literal_param_safe(&meerschaum_core::pyfmt::py_str(&user_id)) {
            return SuccessTuple::fail(format!(
                "Cannot delete user '{}': its ID cannot be matched exactly.",
                user.username
            ));
        }
        let pipe = self.get_users_pipe();
        let mut params = Map::new();
        params.insert("user_id".into(), user_id);
        let res = self
            .clear_pipe(
                &pipe,
                &DataQuery {
                    params: Some(params),
                    ..Default::default()
                },
            )
            .await;
        if !res.success {
            return SuccessTuple::fail(format!(
                "Failed to delete user '{}':\n{}",
                user.username, res.message
            ));
        }
        let forgotten = crate::teams::forget_user(self, &user.username).await;
        if !forgotten.success {
            tracing::warn!("{}", forgotten.message);
        }
        SuccessTuple::success()
    }

    /// Replace a user's attributes and nothing else (their type, email and password stay). The
    /// default writes the users pipe's `attributes` for the user's exact `user_id`.
    async fn set_user_attributes(&self, user: &User, attributes: &Map) -> SuccessTuple {
        crate::teams::default_set_user_attributes(self, user, attributes).await
    }

    /// A user's effective permissions ([`meerschaum_core::access::user_permissions`]): from
    /// their type and attributes on this instance, with this host's default role
    /// (`api:permissions:default_role`). An `api:` instance answers with its server's.
    async fn get_user_permissions(&self, user: &User) -> ConnectorResult<Vec<String>> {
        let (user_type, attributes) = pipe_ops::user_type_and_attributes(self, user).await?;
        let role = meerschaum_core::config::Config::current()
            .map(|c| meerschaum_core::access::default_role_from_config(&c))
            .unwrap_or(meerschaum_core::access::DEFAULT_ROLE);
        Ok(meerschaum_core::access::ordered_permissions(
            &meerschaum_core::access::user_permissions(
                user_type.as_deref(),
                attributes.as_ref(),
                role,
            ),
        ))
    }

    /// A user's role ([`meerschaum_core::access::user_role`]): `admin` for administrators, the
    /// preset their permissions equal, else `custom`.
    async fn get_user_role(&self, user: &User) -> ConnectorResult<meerschaum_core::access::Role> {
        let (user_type, attributes) = pipe_ops::user_type_and_attributes(self, user).await?;
        let role = meerschaum_core::config::Config::current()
            .map(|c| meerschaum_core::access::default_role_from_config(&c))
            .unwrap_or(meerschaum_core::access::DEFAULT_ROLE);
        Ok(meerschaum_core::access::user_role(
            user_type.as_deref(),
            attributes.as_ref(),
            role,
        ))
    }

    /// Get a user's password hash from the users table.
    async fn get_user_password_hash(&self, user: &User) -> ConnectorResult<Option<String>> {
        pipe_ops::user_field(self, user, "password_hash")
            .await
            .map(|v| v.map(|v| meerschaum_core::pyfmt::py_str(&v)))
    }

    /// Get a user's type from the users table.
    async fn get_user_type(&self, user: &User) -> ConnectorResult<Option<String>> {
        pipe_ops::user_field(self, user, "user_type")
            .await
            .map(|v| v.map(|v| meerschaum_core::pyfmt::py_str(&v)))
    }

    /// Get a user's attributes from the users table.
    async fn get_user_attributes(&self, user: &User) -> ConnectorResult<Option<Map>> {
        pipe_ops::user_field(self, user, "attributes")
            .await
            .map(|v| v.and_then(|v| v.as_map().cloned()))
    }

    /// Get a user's email address from the users table (`None` when unset). An `api:` instance
    /// answers per the server's privacy rules (AC13): other users' addresses only for
    /// administrators and the managers of a team they are in.
    async fn get_user_email(&self, user: &User) -> ConnectorResult<Option<String>> {
        pipe_ops::user_field(self, user, "email").await.map(|v| {
            v.filter(|v| !v.is_null())
                .map(|v| meerschaum_core::pyfmt::py_str(&v))
                .filter(|e| !e.trim().is_empty())
        })
    }

    // ----------------------------------------------------------------------------------------
    // Teams ([`crate::teams`], [`meerschaum_core::access`])
    // ----------------------------------------------------------------------------------------

    /// Return the internal pipe teams are stored in (`mrsm:teams`, target `mrsm_teams`).
    fn get_teams_pipe(&self) -> Pipe {
        crate::teams::teams_pipe(&self.connector_keys())
    }

    /// Every team: the built-in team Everyone first, then the others by name.
    async fn get_teams(&self) -> ConnectorResult<Vec<meerschaum_core::access::Team>> {
        crate::teams::default_get_teams(self).await
    }

    /// The team named `name` (names compare without case; `Everyone` always exists).
    async fn get_team(&self, name: &str) -> ConnectorResult<Option<meerschaum_core::access::Team>> {
        crate::teams::default_get_team(self, name).await
    }

    /// Register a new team (assigns its id to `team.id`). Its name must be valid and unused and
    /// its members registered users.
    async fn register_team(&self, team: &mut meerschaum_core::access::Team) -> SuccessTuple {
        crate::teams::default_register_team(self, team).await
    }

    /// Store a team's members and attributes (Everyone's managers and attributes too).
    async fn edit_team(&self, team: &meerschaum_core::access::Team) -> SuccessTuple {
        crate::teams::default_edit_team(self, team).await
    }

    /// Delete a team: its pipes and saved Pipelines move to Everyone. Everyone cannot be
    /// deleted.
    async fn delete_team(&self, name: &str) -> SuccessTuple {
        crate::teams::default_delete_team(self, name).await
    }

    /// Rename a team, in the pipes, saved Pipelines and primary teams that name it too.
    async fn rename_team(&self, old_name: &str, new_name: &str) -> SuccessTuple {
        crate::teams::default_rename_team(self, old_name, new_name).await
    }

    /// A user's teams with their roles (Everyone first: every user is a member).
    async fn get_user_teams(
        &self,
        username: &str,
    ) -> ConnectorResult<Vec<(String, meerschaum_core::access::TeamRole)>> {
        crate::teams::default_get_user_teams(self, username).await
    }

    // ----------------------------------------------------------------------------------------
    // Pipe access and locks ([`crate::locks`], [`meerschaum_core::access`])
    // ----------------------------------------------------------------------------------------

    /// Add `locks` to a registered pipe's locks (`parameters['locks']`). Changing `locks` is
    /// the one edit a `parameters` lock allows. The `api:` client asks its server, which checks
    /// who may lock the pipe.
    async fn lock_pipe(&self, pipe: &Pipe, locks: &meerschaum_core::access::Locks) -> SuccessTuple {
        crate::locks::default_lock_pipe(self, pipe, locks).await
    }

    /// Remove `locks` from a registered pipe's locks (removing `all` removes every lock).
    async fn unlock_pipe(
        &self,
        pipe: &Pipe,
        locks: &meerschaum_core::access::Locks,
    ) -> SuccessTuple {
        crate::locks::default_unlock_pipe(self, pipe, locks).await
    }

    /// Store a registered pipe's access (`parameters['access']`: owner, team, visibility,
    /// edit). A parameter edit: a `parameters` lock refuses it. The `api:` client asks its
    /// server, which checks who may change the pipe's access.
    async fn set_pipe_access(
        &self,
        pipe: &Pipe,
        access: &meerschaum_core::access::PipeAccess,
    ) -> SuccessTuple {
        crate::locks::default_set_pipe_access(self, pipe, access).await
    }

    // ----------------------------------------------------------------------------------------
    // Plugins (`instance/_plugins.py`)
    // ----------------------------------------------------------------------------------------

    /// Return the internal pipe for syncing plugins metadata (`mrsm:plugins`).
    fn get_plugins_pipe(&self) -> Pipe {
        pipe_ops::plugins_pipe(&self.connector_keys(), "uuid")
    }

    /// Register a new plugin to the plugins table (`force` is accepted for API parity).
    async fn register_plugin(&self, plugin: &PluginRecord, force: bool) -> SuccessTuple {
        let _ = force;
        pipe_ops::default_register_plugin(self, plugin).await
    }

    /// New in 5.0: [`Self::register_plugin`] for a caller which verified that `plugin.user_id`
    /// may edit the plugin (the API's plugin upload route): an existing registration owned by
    /// another user (or `plugin.user_id` being `None` while it has an owner) is refused with
    /// [`pipe_ops::plugin_owner_conflict`] instead of moving to `plugin.user_id`.
    ///
    /// This default checks the owner and then registers, which is not atomic; instances whose
    /// `register_plugin` can change an owner (SQL, Valkey) override it to check while writing.
    /// It is a separate method because v4's `register_plugin` changes owners on purpose.
    async fn register_plugin_as_owner(&self, plugin: &PluginRecord, force: bool) -> SuccessTuple {
        match self.get_plugin_user_id(plugin).await {
            Ok(owner)
                if owner.as_ref().is_some_and(|v| !v.is_null())
                    && !pipe_ops::same_plugin_owner(owner.as_ref(), plugin.user_id.as_ref()) =>
            {
                return pipe_ops::plugin_owner_conflict(plugin);
            }
            Ok(_) => {}
            Err(e) => return SuccessTuple::fail(e.to_string()),
        }
        self.register_plugin(plugin, force).await
    }

    /// Return a plugin's ID (its name if registered).
    async fn get_plugin_id(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        Ok(self
            .get_plugin_user_id(plugin)
            .await?
            .map(|_| plugin.name.clone()))
    }

    /// Return the version for a plugin.
    async fn get_plugin_version(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        pipe_ops::plugin_field(self, plugin, "version")
            .await
            .map(|v| v.map(|v| meerschaum_core::pyfmt::py_str(&v)))
    }

    /// Return the user ID for the plugin's owner.
    async fn get_plugin_user_id(&self, plugin: &PluginRecord) -> ConnectorResult<Option<Value>> {
        pipe_ops::plugin_field(self, plugin, "user_id").await
    }

    /// Return the username for the plugin's owner.
    async fn get_plugin_username(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        let Some(user_id) = self.get_plugin_user_id(plugin).await? else {
            return Ok(None);
        };
        Ok(self
            .get_username(&user_id)
            .await?
            .map(|v| meerschaum_core::pyfmt::py_str(&v)))
    }

    /// Return the attributes for a plugin (`{}` if unset).
    async fn get_plugin_attributes(&self, plugin: &PluginRecord) -> ConnectorResult<Map> {
        Ok(pipe_ops::plugin_field(self, plugin, "attributes")
            .await?
            .and_then(|v| v.as_map().cloned())
            .unwrap_or_default())
    }

    /// Return a list of plugin names, optionally owned by `user_id` and starting with
    /// `search_term`.
    async fn get_plugins(
        &self,
        user_id: Option<&Value>,
        search_term: Option<&str>,
    ) -> ConnectorResult<Vec<String>> {
        let pipe = self.get_plugins_pipe();
        let mut params = Map::new();
        if let Some(uid) = user_id.filter(|u| u.truthy()) {
            params.insert("user_id".into(), uid.clone());
        }
        let docs = pipe_ops::get_docs(
            self,
            &pipe,
            &DataQuery {
                select_columns: Some(vec!["plugin_name".into()]),
                params: Some(params),
                ..Default::default()
            },
        )
        .await?;
        let prefix = search_term.unwrap_or("");
        Ok(docs
            .iter()
            .filter_map(|d| d.get("plugin_name"))
            .map(|v| v.py_str())
            .filter(|n| n.starts_with(prefix))
            .collect())
    }

    /// Delete a plugin's registration.
    async fn delete_plugin(&self, plugin: &PluginRecord) -> SuccessTuple {
        match self.get_plugin_id(plugin).await {
            Ok(Some(_)) => {}
            Ok(None) => return SuccessTuple::fail(format!("{plugin} is not registered.")),
            Err(e) => return SuccessTuple::fail(e.to_string()),
        }
        let pipe = self.get_plugins_pipe();
        let mut params = Map::new();
        params.insert("plugin_name".into(), Value::Str(plugin.name.clone()));
        let res = self
            .clear_pipe(
                &pipe,
                &DataQuery {
                    params: Some(params),
                    ..Default::default()
                },
            )
            .await;
        if !res.success {
            return SuccessTuple::fail(format!("Failed to delete {plugin}:\n{}", res.message));
        }
        SuccessTuple::success()
    }

    // ----------------------------------------------------------------------------------------
    // Tokens (`instance/_tokens.py`)
    // ----------------------------------------------------------------------------------------

    /// Return the internal pipe for tokens management (`mrsm:tokens`).
    fn get_tokens_pipe(&self) -> Pipe {
        pipe_ops::tokens_pipe(&self.connector_keys(), "uuid")
    }

    /// Register the new token to the tokens table (generates its credentials).
    async fn register_token(&self, token: &mut Token) -> SuccessTuple {
        pipe_ops::default_register_token(self, token).await
    }

    /// Persist the token's in-memory state to the tokens pipe.
    async fn edit_token(&self, token: &Token) -> SuccessTuple {
        pipe_ops::default_edit_token(self, token, false).await
    }

    /// Set `is_valid` to `false` for the given token.
    async fn invalidate_token(&self, token: &Token) -> SuccessTuple {
        pipe_ops::default_edit_token(self, token, true).await
    }

    /// Delete the given token from the tokens table.
    async fn delete_token(&self, token: &Token) -> SuccessTuple {
        pipe_ops::default_delete_token(self, token).await
    }

    /// Return the tokens, optionally for `user` (admins also see unowned tokens), with
    /// `labels` or `ids`. Newest first.
    async fn get_tokens(
        &self,
        user: Option<&User>,
        labels: Option<&[String]>,
        ids: Option<&[Uuid]>,
    ) -> ConnectorResult<Vec<TokenModel>> {
        pipe_ops::default_get_tokens(self, user, labels, ids).await
    }

    /// Return a token's model from its ID.
    async fn get_token_model(&self, token_id: &Uuid) -> ConnectorResult<Option<TokenModel>> {
        pipe_ops::default_get_token_model(self, token_id).await
    }

    /// Return the secret hash for a given token.
    async fn get_token_secret_hash(&self, token_id: &Uuid) -> ConnectorResult<Option<String>> {
        Ok(pipe_ops::token_field(self, token_id, "secret_hash")
            .await?
            .map(|v| meerschaum_core::pyfmt::py_str(&v)))
    }

    /// Return a token's `user_id`.
    async fn get_token_user_id(&self, token_id: &Uuid) -> ConnectorResult<Option<Value>> {
        pipe_ops::token_field(self, token_id, "user_id").await
    }

    /// Return the scopes for a token (`[]` if unset).
    async fn get_token_scopes(&self, token_id: &Uuid) -> ConnectorResult<Vec<String>> {
        Ok(
            match pipe_ops::token_field(self, token_id, "scopes").await? {
                Some(Value::List(items)) => {
                    items.iter().map(meerschaum_core::pyfmt::py_str).collect()
                }
                // FIX (intentional v4 change): an instance that returns the `json` column as
                // text (SQLite) used to give the raw string, which v4 compared by substring.
                Some(Value::Str(text)) => match Value::from_json_str(&text) {
                    Ok(Value::List(items)) => {
                        items.iter().map(meerschaum_core::pyfmt::py_str).collect()
                    }
                    _ => Vec::new(),
                },
                _ => Vec::new(),
            },
        )
    }

    /// Return `true` if a token exists in the tokens pipe.
    async fn token_exists(&self, token_id: &Uuid) -> ConnectorResult<bool> {
        Ok(pipe_ops::token_field(self, token_id, "creation")
            .await?
            .is_some())
    }
}
