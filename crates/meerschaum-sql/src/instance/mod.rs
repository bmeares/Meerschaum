//! The SQL instance connector: pipes, users, plugins and maintenance on a SQL database
//! (port of `meerschaum/connectors/sql/_pipes.py`, `_instance.py`, `tables/`, `_users.py`,
//! `_plugins.py`, `_fetch.py`, `_partition.py`, `_compress.py` and `_maintenance.py`).
//!
//! Every Python method is an async inherent method on [`SqlConnector`] with the same name and
//! semantics; [`meerschaum_connector::InstanceConnector`] is implemented on top of them
//! (`trait_impl`). Query text comes from the builders in [`crate::queries`] (golden-tested
//! against v4); user values reach the database either through Python's own escaping
//! (`build_where`, `dateadd_str`, quote doubling) or as bound parameters.
//!
//! The `_mrsm_internal` schema and the `mrsm_pipes` / `mrsm_users` / `mrsm_plugins` /
//! `mrsm_temp_tables` tables keep v4's names and column layout, so a v4.1 instance is read as is.

pub mod data;
pub mod env_provider;
pub mod fetch;
pub mod geopackage;
pub mod indices;
pub mod inplace;
mod leases;
pub mod maintenance;
pub mod partition;
pub mod pipes;
pub mod plugins;
pub mod reflect;
pub mod sync;
pub mod tables;
mod trait_impl;
pub mod users;
pub mod values;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use indexmap::IndexMap;
use meerschaum_connector::{ConnectorError, ConnectorResult, InstanceProvider};
use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::{Config, Pipe, static_config};
use meerschaum_kernel::Frame;
use meerschaum_kernel::value::Value as KValue;

use crate::driver::{SqlDriver, SqlParam, SqlValue};
use crate::error::{Result, SqlError};
use crate::flavor::Flavor;
use crate::ident::sql_item_name;
use crate::pipe::SqlPipe;
use crate::queries::ConnectorCtx;
use crate::queries::table::get_table_exists_query;
use crate::uri::ConnectionSpec;

/// `system:connectors:sql:instance:temporary_target`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryTargetConfig {
    /// `prefix` (default `_`).
    pub prefix: String,
    /// `transaction_id_length` (default 4).
    pub transaction_id_length: usize,
    /// `separator` (default `_`).
    pub separator: String,
}

impl Default for TemporaryTargetConfig {
    fn default() -> Self {
        TemporaryTargetConfig {
            prefix: "_".into(),
            transaction_id_length: 4,
            separator: "_".into(),
        }
    }
}

/// The configuration values the SQL instance methods read with `get_config`.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceSettings {
    /// `system:connectors:sql:chunksize` (default 100,000).
    pub chunksize: i64,
    /// `system:connectors:sql:bulk_insert:<flavor>` (PostgreSQL family and MSSQL by default).
    pub bulk_insert: bool,
    /// `system:connectors:sql:instance:temporary_target`.
    pub temporary_target: TemporaryTargetConfig,
    /// `system:connectors:sql:instance:max_partitions_per_sync` (default 10,000).
    pub max_partitions_per_sync: usize,
    /// `system:connectors:sql:instance:stale_temporary_tables_minutes` (default 1440).
    pub stale_temporary_tables_minutes: i64,
    /// `pipes:sync:filter_params_index_limit` (default 250).
    pub filter_params_index_limit: usize,
    /// `pipes:parameters:fetch:backtrack_minutes` (default 1440).
    pub backtrack_minutes: i64,
    /// The pipe defaults (`verify:chunk_minutes`, the default instance, autotime column).
    pub pipe_defaults: PipeDefaults,
}

impl InstanceSettings {
    /// The built-in defaults (`meerschaum/config/_default.py`).
    pub fn defaults(flavor: Flavor) -> Self {
        InstanceSettings {
            chunksize: 100_000,
            bulk_insert: matches!(
                flavor,
                Flavor::Postgresql
                    | Flavor::Postgis
                    | Flavor::Citus
                    | Flavor::Timescaledb
                    | Flavor::TimescaledbHa
                    | Flavor::Mssql
            ),
            temporary_target: TemporaryTargetConfig::default(),
            max_partitions_per_sync: crate::queries::partition::MAX_PARTITIONS_PER_SYNC,
            stale_temporary_tables_minutes: 1440,
            filter_params_index_limit: 250,
            backtrack_minutes: crate::pipe::DEFAULT_BACKTRACK_MINUTES,
            pipe_defaults: PipeDefaults::default(),
        }
    }

    /// Read the settings from a config registry, falling back to the defaults.
    pub fn from_config(flavor: Flavor, config: &Config) -> Self {
        let d = Self::defaults(flavor);
        let opts = meerschaum_core::config::GetOptions {
            warn: false,
            ..Default::default()
        };
        let get = |keys: &[&str]| -> Option<meerschaum_core::Value> {
            let (valid, v) = config.get_with(keys, opts);
            (valid && !v.is_null()).then_some(v)
        };
        let int = |keys: &[&str]| get(keys).and_then(|v| meerschaum_core::pyfmt::py_int(&v));
        let tt = |key: &str| {
            get(&[
                "system",
                "connectors",
                "sql",
                "instance",
                "temporary_target",
                key,
            ])
        };
        InstanceSettings {
            chunksize: int(&["system", "connectors", "sql", "chunksize"]).unwrap_or(d.chunksize),
            bulk_insert: get(&[
                "system",
                "connectors",
                "sql",
                "bulk_insert",
                flavor.as_str(),
            ])
            .map(|v| v.truthy())
            .unwrap_or(false),
            temporary_target: TemporaryTargetConfig {
                prefix: tt("prefix")
                    .map(|v| v.py_str())
                    .unwrap_or(d.temporary_target.prefix),
                transaction_id_length: tt("transaction_id_length")
                    .and_then(|v| meerschaum_core::pyfmt::py_int(&v))
                    .map(|i| i.max(0) as usize)
                    .unwrap_or(3),
                separator: tt("separator")
                    .map(|v| v.py_str())
                    .unwrap_or(d.temporary_target.separator),
            },
            max_partitions_per_sync: crate::queries::partition::max_partitions_per_sync(config),
            stale_temporary_tables_minutes: int(&[
                "system",
                "connectors",
                "sql",
                "instance",
                "stale_temporary_tables_minutes",
            ])
            .unwrap_or(d.stale_temporary_tables_minutes),
            filter_params_index_limit: int(&["pipes", "sync", "filter_params_index_limit"])
                .map(|i| i.max(0) as usize)
                .unwrap_or(d.filter_params_index_limit),
            backtrack_minutes: int(&["pipes", "parameters", "fetch", "backtrack_minutes"])
                .unwrap_or(d.backtrack_minutes),
            pipe_defaults: PipeDefaults::from_config(config),
        }
    }
}

#[derive(Debug, Default)]
struct State {
    /// `_in_memory_temp_tables`: temporary table → ready to drop.
    temp_tables: IndexMap<String, bool>,
    /// `_stale_temporary_tables_check_timestamp`.
    stale_check: Option<Instant>,
    /// Whether the instance tables were created (or found) in this process.
    tables_ready: bool,
    leases_ready: bool,
    internal_pipe_states: HashMap<String, data::PipeState>,
    /// `db_version`.
    db_version: Option<String>,
    /// Executed statements, when the query log is enabled (tests record emitted DDL).
    query_log: Option<Vec<String>>,
    /// `_compress_upsert_warned_pipes`.
    compress_upsert_warned: HashMap<String, ()>,
}

/// A `sql:<label>` instance connector.
pub struct SqlConnector {
    label: String,
    flavor: Flavor,
    spec: ConnectionSpec,
    driver: SqlDriver,
    schema: Option<String>,
    internal_schema: Option<String>,
    /// The configuration the instance methods read.
    pub settings: InstanceSettings,
    state: Mutex<State>,
    /// Connectors for `{{ Pipe(..., instance='sql:other') }}` references to other instances.
    provider: Mutex<Option<Arc<dyn InstanceProvider>>>,
}

impl std::fmt::Debug for SqlConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlConnector")
            .field("label", &self.label)
            .field("flavor", &self.flavor)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for SqlConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sql:{}", self.label)
    }
}

/// Convert a crate error into a connector error.
pub(crate) fn be(e: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::Backend(e.to_string())
}

impl SqlConnector {
    /// Connect to the database described by `spec` with the default settings.
    pub async fn connect(label: &str, spec: ConnectionSpec) -> Result<Self> {
        let settings = InstanceSettings::defaults(spec.flavor);
        Self::connect_with(label, spec, settings).await
    }

    /// Connect with explicit settings.
    pub async fn connect_with(
        label: &str,
        spec: ConnectionSpec,
        settings: InstanceSettings,
    ) -> Result<Self> {
        let driver = SqlDriver::connect(&spec).await?;
        let flavor = spec.flavor;
        // `SQLConnector.schema`: the configured schema, else the database's default schema
        // (`sqlalchemy.inspect(engine).default_schema_name`), `None` on schema-less flavors.
        let schema = if flavor.no_schema() {
            None
        } else if let Some(s) = spec.schema.clone().filter(|s| !s.is_empty()) {
            Some(s)
        } else {
            let query = match flavor {
                Flavor::Mssql => "SELECT SCHEMA_NAME()",
                _ => "SELECT current_schema()",
            };
            match driver.value(query).await {
                Ok(Some(SqlValue::Text(s))) => Some(s),
                _ => flavor.default_schema().map(str::to_string),
            }
        };
        // `SQLConnector.internal_schema`.
        let internal_schema = if flavor.no_schema() {
            schema.clone()
        } else {
            Some(static_config().sql.internal_schema.clone())
        };
        Ok(SqlConnector {
            label: label.to_string(),
            flavor,
            spec,
            driver,
            schema,
            internal_schema,
            settings,
            state: Mutex::new(State::default()),
            provider: Mutex::new(None),
        })
    }

    /// A connector whose pool connects lazily and which resolves `self.schema` without a
    /// round trip (the configured schema, else the flavor's default). Query building works
    /// offline; the first database call opens a connection. Must be called within a Tokio
    /// runtime.
    pub fn connect_lazy(
        label: &str,
        spec: ConnectionSpec,
        settings: InstanceSettings,
    ) -> Result<Self> {
        let driver = SqlDriver::connect_lazy(&spec)?;
        let flavor = spec.flavor;
        let schema = if flavor.no_schema() {
            None
        } else {
            spec.schema
                .clone()
                .filter(|s| !s.is_empty())
                .or_else(|| flavor.default_schema().map(str::to_string))
        };
        let internal_schema = if flavor.no_schema() {
            schema.clone()
        } else {
            Some(static_config().sql.internal_schema.clone())
        };
        Ok(SqlConnector {
            label: label.to_string(),
            flavor,
            spec,
            driver,
            schema,
            internal_schema,
            settings,
            state: Mutex::new(State::default()),
            provider: Mutex::new(None),
        })
    }

    /// `SQLConnector(label, uri=uri)`.
    pub async fn from_uri(label: &str, uri: &str) -> Result<Self> {
        let spec = ConnectionSpec::from_uri(uri, Some(label), None)?;
        Self::connect(label, spec).await
    }

    /// The connector label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// `str(connector)`: `sql:<label>`.
    pub fn keys(&self) -> String {
        format!("sql:{}", self.label)
    }

    /// `self.flavor`.
    pub fn flavor(&self) -> Flavor {
        self.flavor
    }

    /// `self.schema`.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// `self.instance_schema` (the schema of the `mrsm_*` tables).
    pub fn instance_schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// `self.internal_schema` (`_mrsm_internal` on schema flavors).
    pub fn internal_schema(&self) -> Option<&str> {
        self.internal_schema.as_deref()
    }

    /// `self.database`.
    pub fn database(&self) -> Option<&str> {
        self.spec.database.as_deref()
    }

    /// The connection spec.
    pub fn spec(&self) -> &ConnectionSpec {
        &self.spec
    }

    /// The underlying driver (`read`, `exec`, `value` for the Rust core and Rust plugins).
    pub fn driver(&self) -> &SqlDriver {
        &self.driver
    }

    /// The query-builder context (`self.flavor`, `self.schema`).
    pub fn ctx(&self) -> ConnectorCtx {
        ConnectorCtx::with_schema(self.flavor, self.schema.as_deref())
    }

    /// Bind a pipe to this instance (its flavor-truncated `target` and the configured defaults).
    pub fn sql_pipe(&self, pipe: &Pipe) -> Result<SqlPipe> {
        Ok(SqlPipe::new(pipe.clone(), self.flavor)?.with_defaults(
            self.settings.pipe_defaults.clone(),
            self.settings.backtrack_minutes,
        ))
    }

    /// Set the connectors cross-instance `{{ Pipe() }}` references resolve against.
    pub fn set_instance_provider(&self, provider: Arc<dyn InstanceProvider>) {
        *self.provider.lock().unwrap_or_else(|e| e.into_inner()) = Some(provider);
    }

    /// `pipe` with its parameters resolved against the registered state of every pipe it
    /// references (`{{ Pipe() }}` templates, `reference(s)`), on this instance or the one the
    /// reference names. The raw parameters (what persistence writes) are untouched.
    ///
    /// Called at the entry points the engine and the façade use (`InstanceConnector` methods,
    /// `fetch`, `get_pipe_metadef`, the `facade` wrappers); a pipe without cross-pipe templates
    /// is returned as is. Warnings (unreachable instance, failed template) go to `tracing`.
    pub async fn prepare_pipe(&self, pipe: &Pipe) -> Pipe {
        if !meerschaum_connector::template::needs_instance_resolution(pipe) {
            return pipe.clone();
        }
        let provider = self
            .provider
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let fallback = crate::instance::env_provider::EnvSqlProvider::shared();
        let provider: &dyn InstanceProvider = match &provider {
            Some(p) => p.as_ref(),
            None => fallback.as_ref(),
        };
        let (out, warnings) = meerschaum_connector::resolve_pipe(
            pipe,
            self,
            Some(provider),
            &self.settings.pipe_defaults,
        )
        .await;
        for w in warnings {
            tracing::warn!("{w}");
        }
        out
    }

    /// `pipe` carrying the parameters registered on this instance, so a table reference for a
    /// pipe built from keys only (`{{ Pipe('a', 'b') }}`) names its explicit `target`, as
    /// `Pipe.target` does in Python (which reads the registered parameters).
    ///
    /// `InstanceConnector::pipe_table_reference` is synchronous; the lookup runs on the current
    /// multi-threaded Tokio runtime (every runtime the CLI, bindings and server build). On any
    /// other context, or when the pipe is unregistered or names its own target, the pipe is
    /// returned as is.
    pub(crate) fn with_registered_parameters(&self, pipe: &Pipe) -> Pipe {
        let raw = pipe.raw_parameters();
        if ["target", "target_name", "target_table"]
            .iter()
            .any(|k| raw.contains_key(*k))
        {
            return pipe.clone();
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return pipe.clone();
        };
        if handle.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
            return pipe.clone();
        }
        let attrs = tokio::task::block_in_place(|| handle.block_on(self.get_pipe_attributes(pipe)));
        match attrs
            .ok()
            .and_then(|a| a.get("parameters").map(meerschaum_core::Value::to_map))
        {
            Some(registered) => {
                let mut out = pipe.clone();
                out.set_parameters(meerschaum_core::value::apply_patch_maps(&registered, raw));
                out
            }
            None => pipe.clone(),
        }
    }

    /// `self.get_pipe_schema(pipe)`.
    pub fn get_pipe_schema(&self, pipe: &SqlPipe) -> Option<String> {
        self.ctx().get_pipe_schema(pipe)
    }

    /// `sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))`.
    pub fn pipe_table_name(&self, pipe: &SqlPipe) -> String {
        sql_item_name(
            &pipe.target,
            self.flavor,
            self.get_pipe_schema(pipe).as_deref(),
        )
    }

    /// `self.IS_THREAD_SAFE`.
    pub fn is_thread_safe(&self) -> bool {
        match self.flavor {
            Flavor::Duckdb | Flavor::Oracle => false,
            Flavor::Sqlite | Flavor::Geopackage => !self.spec.is_memory(),
            _ => true,
        }
    }

    // ------------------------------------------------------------------------------------------
    // Query log (tests record the exact statements emitted).
    // ------------------------------------------------------------------------------------------

    /// Start recording every executed statement.
    pub fn enable_query_log(&self) {
        self.state.lock().expect("state").query_log = Some(Vec::new());
    }

    /// Return and clear the recorded statements.
    pub fn take_query_log(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("state")
            .query_log
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    fn log(&self, sql: &str) {
        if let Some(log) = self.state.lock().expect("state").query_log.as_mut() {
            log.push(sql.to_string());
        }
    }

    // ------------------------------------------------------------------------------------------
    // `_sql.py` essentials: `exec`, `exec_queries`, `value`, `read` with Python's failure modes.
    // ------------------------------------------------------------------------------------------

    /// `self.exec(query, silent=True)`: the rows affected, or `None` on failure.
    pub async fn exec(&self, sql: &str) -> Option<u64> {
        self.log(sql);
        match self.driver.exec(sql).await {
            Ok(n) => Some(n),
            Err(e) => {
                tracing::debug!("Failed to execute query:\n{sql}\n{e}");
                None
            }
        }
    }

    /// `self.exec(query)` with bound parameters.
    pub async fn exec_params(&self, sql: &str, params: &[SqlParam]) -> Option<u64> {
        self.log(sql);
        match self.driver.exec_with(sql, params).await {
            Ok(n) => Some(n),
            Err(e) => {
                tracing::warn!("Failed to execute query:\n{sql}\n{e}");
                None
            }
        }
    }

    /// `self.exec_queries(queries, break_on_error, rollback)`.
    pub async fn exec_queries(
        &self,
        queries: &[String],
        break_on_error: bool,
        rollback: bool,
    ) -> Vec<Option<u64>> {
        for q in queries {
            self.log(q);
        }
        if queries.is_empty() {
            return Vec::new();
        }
        match self
            .driver
            .exec_queries(queries, break_on_error, rollback)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Failed to execute queries:\n{e}");
                vec![None]
            }
        }
    }

    /// `all(self.exec_queries(...))`.
    pub async fn exec_queries_all(
        &self,
        queries: &[String],
        break_on_error: bool,
        rollback: bool,
    ) -> bool {
        self.exec_queries(queries, break_on_error, rollback)
            .await
            .iter()
            .all(Option::is_some)
    }

    /// `self.value(query, silent=True)`: errors become `None`.
    pub async fn value(&self, sql: &str) -> Option<SqlValue> {
        self.log(sql);
        match self.driver.value(sql).await {
            Ok(v) => v.filter(|v| !v.is_null()),
            Err(e) => {
                tracing::debug!("Failed to fetch value:\n{sql}\n{e}");
                None
            }
        }
    }

    /// `self.value(query)` with bound parameters.
    pub async fn value_params(
        &self,
        sql: &str,
        params: &[SqlParam],
    ) -> ConnectorResult<Option<SqlValue>> {
        self.log(sql);
        Ok(self
            .driver
            .value_with(sql, params)
            .await
            .map_err(be)?
            .filter(|v| !v.is_null()))
    }

    /// `self.read(query)` as a kernel frame.
    pub async fn read(&self, sql: &str) -> Result<Frame> {
        self.log(sql);
        self.driver.read_frame(sql).await
    }

    /// `self.read(query)` with bound parameters.
    pub async fn read_params(&self, sql: &str, params: &[SqlParam]) -> Result<Frame> {
        self.log(sql);
        self.driver.read_frame_with(sql, params).await
    }

    /// `table_exists(table, self, schema)` (`utils/sql.py`).
    pub async fn table_exists(&self, table: &str, schema: Option<&str>) -> bool {
        let schema = if self.flavor.no_schema() {
            None
        } else {
            schema
        };
        let query = get_table_exists_query(table, self.flavor, schema);
        self.value(&query).await.is_some()
    }

    /// `self.db_version`.
    pub async fn db_version(&self) -> Option<String> {
        if let Some(v) = self.state.lock().expect("state").db_version.clone() {
            return Some(v);
        }
        let query = crate::queries::table::get_db_version_query(self.flavor);
        let v = self.value(&query).await.map(|v| v.to_string())?;
        // PARITY: `get_db_version` returns the server's version string as is.
        self.state.lock().expect("state").db_version = Some(v.clone());
        Some(v)
    }

    /// `self.get_temporary_target(target, transact_id, label)`.
    pub fn get_temporary_target(
        &self,
        target: &str,
        transact_id: Option<&str>,
        label: Option<&str>,
    ) -> String {
        let cfg = &self.settings.temporary_target;
        let transact_id = transact_id.map(str::to_string).unwrap_or_else(|| {
            meerschaum_core::users::generate_password(cfg.transaction_id_length)
                .unwrap_or_else(|_| "0000".into())
        });
        let mut out = format!("{}{}{}{}", cfg.prefix, target, cfg.separator, transact_id);
        if let Some(l) = label.filter(|l| !l.is_empty()) {
            out.push_str(&cfg.separator);
            out.push_str(l);
        }
        out
    }

    pub(crate) fn mark_tables_ready(&self) {
        self.state.lock().expect("state").tables_ready = true;
    }

    pub(crate) fn tables_ready(&self) -> bool {
        self.state.lock().expect("state").tables_ready
    }

    pub(crate) fn record_temp_tables(&self, tables: &[String], ready_to_drop: bool) {
        let mut st = self.state.lock().expect("state");
        for t in tables {
            st.temp_tables.insert(t.clone(), ready_to_drop);
        }
    }

    pub(crate) fn ready_temp_tables(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("state")
            .temp_tables
            .iter()
            .filter(|(_, ready)| **ready)
            .map(|(t, _)| t.clone())
            .collect()
    }

    pub(crate) fn forget_temp_table(&self, table: &str) {
        self.state
            .lock()
            .expect("state")
            .temp_tables
            .shift_remove(table);
    }

    pub(crate) fn stale_check(&self) -> Option<Instant> {
        self.state.lock().expect("state").stale_check
    }

    pub(crate) fn set_stale_check(&self, at: Instant) {
        self.state.lock().expect("state").stale_check = Some(at);
    }

    pub(crate) fn warn_compress_upsert_once(&self, key: &str) -> bool {
        self.state
            .lock()
            .expect("state")
            .compress_upsert_warned
            .insert(key.to_string(), ())
            .is_none()
    }
}

/// Read a frame's rows as documents.
pub(crate) fn frame_docs(df: &Frame) -> Vec<IndexMap<String, KValue>> {
    meerschaum_connector::pipe_ops::frame_to_docs(df)
}

/// A cell as an optional string (`None` for nulls).
pub(crate) fn kv_str(v: Option<&KValue>) -> Option<String> {
    match v {
        None => None,
        Some(v) if v.is_na() => None,
        Some(KValue::Str(s)) => Some(s.clone()),
        Some(KValue::Json(j)) => Some(serde_json::to_string(j).unwrap_or_default()),
        Some(other) => Some(other.py_str()),
    }
}

/// A cell as an optional integer.
pub(crate) fn kv_int(v: Option<&KValue>) -> Option<i64> {
    match v? {
        KValue::Int(i) => Some(*i),
        KValue::Float(f) if f.fract() == 0.0 => Some(*f as i64),
        KValue::Bool(b) => Some(*b as i64),
        KValue::Decimal(d) => d.to_plain_string().parse().ok(),
        KValue::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// Map a crate error to a connector error (`?` helper).
impl From<SqlError> for ConnectorError {
    fn from(e: SqlError) -> Self {
        ConnectorError::Backend(e.to_string())
    }
}
