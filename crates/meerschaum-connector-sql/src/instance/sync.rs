//! Syncing frames into pipes (`_pipes.py`: `sync_pipe`, `create_pipe_table_from_df`,
//! `get_add_columns_queries`, `get_alter_columns_queries`, `get_to_sql_dtype`; `_sql.py`:
//! `to_sql` and `psql_insert_copy`), plus the essentials of `Pipe.sync()` (dtype enforcement,
//! autotime, capturing new special columns) for callers that hold raw rows.

use std::time::Instant;

use indexmap::IndexMap;
use meerschaum_connector::{ConnectorResult, SyncOptions};
use meerschaum_core::dtypes::{are_dtypes_equal, get_pd_type_from_db_type, is_dtype_special};
use meerschaum_core::{Map, Pipe, SuccessTuple, SyncResult, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::detect::{get_numeric_cols, get_special_cols};
use meerschaum_kernel::diff::{BacktrackRequest, FilterExistingParams, filter_existing};
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::Value as KValue;

use crate::driver::SqlParam;
use crate::driver::params::{max_bind_params, typed_placeholder};
use crate::dtypes::get_db_type_from_pd_type;
use crate::flavor::{DriverFamily, Flavor};
use crate::ident::{sql_item_name, truncate_item_name};
use crate::pipe::{ChunkInterval, SqlPipe};
use crate::queries::compress::get_compress_settings;
use crate::queries::table::{
    CreateTableOptions, QueryOrDtypes, get_create_schema_if_not_exists_queries,
    get_create_table_queries, get_max_id_query, get_reset_autoincrement_queries,
};
use crate::queries::templates::has_update_query;
use crate::queries::update::{UpdateQueriesArgs, get_update_queries};

use super::data::{PipeState, columns_map};
use super::values::{
    ColumnEncoding, csv_row, encode_copy_field, encode_json_cell, encode_param,
    numeric_precision_scale,
};
use super::{SqlConnector, be};

/// The inputs `get_alter_columns_queries` reads from the frame, the table and the pipe.
#[derive(Debug, Clone, Default)]
pub struct AlterColumnsInput {
    /// `df.dtypes` (pandas dtype strings).
    pub df_cols_types: IndexMap<String, String>,
    /// `get_numeric_cols(df)`.
    pub numeric_cols: Vec<String>,
    /// `get_table_cols_types(target)` (database types).
    pub db_cols_types: IndexMap<String, String>,
    /// `pipe.get_dtypes()`.
    pub pipe_dtypes: IndexMap<String, String>,
    /// `pipe.get_dtypes(infer=False)`.
    pub explicit_dtypes: IndexMap<String, String>,
    /// `pipe._get_cached_value('new_special_cols')`.
    pub new_special_cols: IndexMap<String, String>,
    /// `generate_password(3)` (the SQLite rebuild's temporary table suffix).
    pub session_id: String,
}

/// `_max_chunks_flavors` (`_sql.py:30`).
fn max_chunks(flavor: Flavor) -> Option<i64> {
    match flavor {
        Flavor::Sqlite | Flavor::Geopackage => Some(1000),
        _ => None,
    }
}

/// The internal options `sync_pipe` threads through recursive temporary syncs.
#[derive(Debug, Clone, Default)]
pub(crate) struct InternalSync {
    pub cached_state: Option<PipeState>,
    /// `temp_pipe._cache_value('_columns_types', ...)`.
    pub columns_types: Option<IndexMap<String, String>>,
    /// `temp_pipe._cache_value('_skip_check_indices', True)`.
    pub skip_check_indices: bool,
    /// `pipe._get_cached_value('new_special_cols')` (set by `Pipe.sync()`).
    pub new_special_cols: Option<IndexMap<String, String>>,
    /// `_check_temporary_tables`.
    pub skip_temporary_tables_check: bool,
}

/// A backtrack request's `params` as `build_where` params (values via `str()`).
fn backtrack_params(req: &BacktrackRequest) -> Map {
    req.params
        .iter()
        .map(|(k, vals)| {
            let items = vals
                .iter()
                .map(|v| match v {
                    v if v.is_na() => Value::Null,
                    KValue::Int(i) => Value::Int(*i),
                    KValue::Float(f) => Value::Float(*f),
                    KValue::Bool(b) => Value::Bool(*b),
                    other => Value::Str(other.py_str()),
                })
                .collect();
            (k.clone(), Value::List(items))
        })
        .collect()
}

/// `get_current_timestamp(precision_unit, as_int)` for autotime (rounded down to the unit).
fn current_timestamp(unit: meerschaum_core::dtypes::PrecisionUnit, as_int: bool) -> KValue {
    let now = chrono::Utc::now();
    let micros = now.timestamp_micros();
    let unit_micros: i64 = match unit.name() {
        "nanosecond" | "microsecond" => 1,
        "millisecond" => 1_000,
        "second" => 1_000_000,
        "minute" => 60_000_000,
        "hour" => 3_600_000_000,
        "day" => 86_400_000_000,
        _ => 1,
    };
    let rounded = micros.div_euclid(unit_micros) * unit_micros;
    if as_int {
        let v = match unit.name() {
            "nanosecond" => rounded.saturating_mul(1000),
            _ => rounded / unit_micros,
        };
        return KValue::Int(v);
    }
    KValue::Datetime(meerschaum_kernel::value::DateTimeValue::utc(
        chrono::DateTime::from_timestamp_micros(rounded)
            .map(|d| d.naive_utc())
            .unwrap_or_else(|| now.naive_utc()),
    ))
}

/// The longest internal key value (characters) MySQL / MariaDB / MSSQL / Oracle can index:
/// internal (`mrsm`) string keys are stored as `VARCHAR(255)` / `NVARCHAR(255)` / `NVARCHAR2(255)`.
/// (Oracle bounds an index key to about 6398 bytes at the default 8 KB block, and a unique key over
/// two `NVARCHAR2(2000)` columns is 8000 bytes: `ORA-01450`.)
pub const INTERNAL_KEY_MAX_CHARS: usize = 255;

/// A failure message when an internal pipe's key column holds a string longer than
/// [`INTERNAL_KEY_MAX_CHARS`] on a flavor that bounds internal keys (the database would
/// otherwise reject or silently truncate it).
fn internal_key_length_error(sp: &SqlPipe, flavor: Flavor, df: &Frame) -> Option<String> {
    if sp.pipe().connector_keys() != "mrsm"
        || !matches!(
            flavor,
            Flavor::Mysql | Flavor::Mariadb | Flavor::Mssql | Flavor::Oracle
        )
    {
        return None;
    }
    let limit = |v: &KValue| -> usize {
        match flavor {
            // NVARCHAR / NVARCHAR2 count UTF-16 code units.
            Flavor::Mssql | Flavor::Oracle => v.py_str().encode_utf16().count(),
            _ => v.py_str().chars().count(),
        }
    };
    for column in columns_map(sp).values() {
        let Ok(data) = df.data(column) else { continue };
        for v in data.values() {
            if matches!(v, KValue::Str(_)) && limit(&v) > INTERNAL_KEY_MAX_CHARS {
                return Some(format!(
                    "The internal key '{column}' of {} exceeds {INTERNAL_KEY_MAX_CHARS} characters, which {flavor} cannot index.",
                    sp.pipe()
                ));
            }
        }
    }
    None
}

impl SqlConnector {
    /// `Pipe.sync(df)` essentials over this instance: enforce the pipe's dtypes on the input
    /// (a frame, documents or a dict of lists), add the autotime column, persist newly detected
    /// special columns, then `sync_pipe`. Applies the compression policy afterwards when
    /// `compress` is set.
    pub async fn sync(&self, pipe: &Pipe, input: PipeInput, opts: &SyncOptions) -> SuccessTuple {
        let mut pipe = pipe.clone();
        self.sync_mut(&mut pipe, input, opts).await
    }

    /// [`sync`](Self::sync), updating the caller's in-memory parameters the way `Pipe.sync()`
    /// mutates the Python `Pipe` (newly detected special `dtypes`, `autoincrement`, numeric
    /// columns from `get_alter_columns_queries`).
    pub async fn sync_mut(
        &self,
        pipe: &mut Pipe,
        input: PipeInput,
        opts: &SyncOptions,
    ) -> SuccessTuple {
        match self.sync_inner(pipe, input, opts).await {
            Ok(t) => t,
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    async fn sync_inner(
        &self,
        pipe: &mut Pipe,
        input: PipeInput,
        opts: &SyncOptions,
    ) -> ConnectorResult<SuccessTuple> {
        if let Some(params) = self.get_pipe_parameters(pipe).await? {
            // `Pipe.parameters` reads the registered parameters unless set in memory.
            if pipe.parameters().is_empty() {
                pipe.set_parameters(params);
            }
        }
        // Cross-pipe `{{ Pipe() }}` templates resolve against registered state (raw parameters,
        // what `edit_pipe` persists, keep the templates).
        *pipe = self.prepare_pipe(pipe).await;
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state(&sp).await;
        let ctx = self.dtypes_ctx(&sp, &state);
        let mut df = pipe_enforce_dtypes(input, &ctx).map_err(be)?;
        let mut opts = opts.clone();
        if sp.autotime() {
            let dt_col = sp.column("datetime");
            let ts_col = dt_col
                .clone()
                .unwrap_or_else(|| self.settings.pipe_defaults.autotime_column.clone());
            let ts_typ = state
                .dtypes
                .get(&ts_col)
                .cloned()
                .unwrap_or_else(|| "datetime".into());
            if !df.contains(&ts_col) {
                let unit = sp.precision_unit().map_err(be)?;
                let now = current_timestamp(unit, are_dtypes_equal(&ts_typ, "int"));
                let n = df.height();
                df.set_column(meerschaum_kernel::Column::object(&ts_col, vec![now; n]))
                    .map_err(be)?;
                df = pipe_enforce_dtypes(PipeInput::Frame(df), &ctx).map_err(be)?;
                opts.check_existing = dt_col.is_some();
            }
        }
        // `_persist_new_special_columns`.
        let special_cols = get_special_cols(&df);
        let existing_special: Vec<&String> = state
            .dtypes
            .iter()
            .filter(|(_, t)| is_dtype_special(t))
            .map(|(c, _)| c)
            .collect();
        let new_special_cols: IndexMap<String, String> = special_cols
            .into_iter()
            .filter(|(c, _)| !existing_special.contains(&c))
            .collect();
        if !new_special_cols.is_empty() {
            let mut dtypes: Map = pipe
                .param("dtypes")
                .and_then(|d| d.as_map().cloned())
                .unwrap_or_default();
            for (c, t) in &new_special_cols {
                dtypes.insert(c.clone(), Value::Str(t.clone()));
            }
            let mut patch = Map::new();
            patch.insert("dtypes".into(), Value::Map(dtypes));
            pipe.update_parameters(patch);
            if !pipe.temporary && self.get_pipe_id(pipe).await?.is_some() {
                let res = self.edit_pipe(pipe, false).await;
                if !res.success {
                    tracing::warn!(
                        "Failed to capture new special columns for {pipe}:\n{}",
                        res.message
                    );
                }
            }
        }
        let internal = InternalSync {
            new_special_cols: Some(new_special_cols),
            ..Default::default()
        };
        let result = self.sync_pipe_impl(pipe, df, &opts, internal).await;
        if result.success && pipe.param("compress").is_some_and(|v| v.truthy()) {
            let res = self.apply_compression_policy(pipe).await;
            if !res.success {
                tracing::debug!(
                    "Could not apply compression policy to {pipe}:\n{}",
                    res.message
                );
            }
        }
        Ok(result.as_success_tuple())
    }

    /// `sync_pipe(pipe, df, begin, end, chunksize, check_existing)`: write a (dtype-enforced)
    /// frame into the pipe's table, creating the table, columns, partitions and indices as
    /// needed.
    pub async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple {
        self.sync_pipe_result(pipe, df, opts)
            .await
            .as_success_tuple()
    }

    /// [`sync_pipe`](Self::sync_pipe) with the structured result (row counts).
    pub async fn sync_pipe_result(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SyncResult {
        let internal = InternalSync {
            new_special_cols: opts
                .extra
                .get("new_special_cols")
                .and_then(|v| v.as_map())
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.py_str())).collect()),
            ..Default::default()
        };
        let mut pipe = self.prepare_pipe(pipe).await;
        self.sync_pipe_impl(&mut pipe, df, opts, internal).await
    }

    pub(crate) fn sync_pipe_impl<'a>(
        &'a self,
        pipe: &'a mut Pipe,
        df: Frame,
        opts: &'a SyncOptions,
        internal: InternalSync,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SyncResult> + Send + 'a>> {
        Box::pin(async move {
            let start = Instant::now();
            let mut result = match self.sync_pipe_inner(pipe, df, opts, internal).await {
                Ok(r) => r,
                Err(e) => SyncResult::new(false, e.to_string()),
            };
            if !result.success
                && let Ok(sp) = self.sql_pipe(pipe)
            {
                self.state
                    .lock()
                    .expect("state")
                    .internal_pipe_states
                    .remove(&self.pipe_table_name(&sp));
            }
            result.duration = Some(start.elapsed());
            result
        })
    }

    async fn sync_pipe_inner(
        &self,
        pipe: &mut Pipe,
        df: Frame,
        opts: &SyncOptions,
        internal: InternalSync,
    ) -> ConnectorResult<SyncResult> {
        let flavor = self.flavor;
        let sp0 = self.sql_pipe(pipe)?;
        let schema = self.get_pipe_schema(&sp0);
        if let Some(message) = internal_key_length_error(&sp0, flavor, &df) {
            return Ok(SyncResult::new(false, message));
        }

        if !pipe.temporary && self.get_pipe_id(pipe).await?.is_none() {
            let reg = self.register_pipe(pipe).await;
            if !reg.success {
                return Ok(reg.into());
            }
        }

        let mut check_existing = opts.check_existing;
        let cache_internal = pipe.temporary && sp0.is_static() && pipe.connector_keys() == "mrsm";
        let cache_key = self.pipe_table_name(&sp0);
        let cached = internal.cached_state.clone().or_else(|| {
            if cache_internal {
                self.state
                    .lock()
                    .expect("state")
                    .internal_pipe_states
                    .get(&cache_key)
                    .cloned()
            } else {
                None
            }
        });
        // Table existence alone does not mean an upsert is ready: another process may
        // still be creating its unique constraint. Inspect metadata only after taking
        // the database lock, and retain it through index creation and the first write.
        // Warm internal writes use the cached readiness and retain their one-DML cost.
        let _initialization_guard = if cache_internal && cached.is_none() {
            self.driver
                .lock_internal_initialization(&sp0.target, schema.as_deref())
                .await
                .map_err(be)?
        } else {
            None
        };
        let initializing_internal = cache_internal && cached.is_none();
        let mut state = match cached {
            Some(state) => state,
            None => self.pipe_state(&sp0).await,
        };
        let exists = state.exists;
        let is_new = !exists;
        if is_new {
            check_existing = false;
        } else {
            let add_cols_queries = self.add_columns_queries_for(
                &sp0,
                &Self::df_cols_types_refined(&df),
                &state.cols_types,
            );
            if !add_cols_queries.is_empty()
                && !self.exec_queries_all(&add_cols_queries, false, true).await
            {
                tracing::warn!("Failed to add new columns to {pipe}.");
            }
            if !add_cols_queries.is_empty() {
                state = self.pipe_state(&sp0).await;
            }
            let alter_cols_queries = self
                .alter_columns_with_state(pipe, &df, internal.new_special_cols.as_ref(), &state)
                .await;
            if !alter_cols_queries.is_empty()
                && !self
                    .exec_queries_all(&alter_cols_queries, false, true)
                    .await
            {
                tracing::warn!("Failed to alter columns for {pipe}.");
            }
            if !alter_cols_queries.is_empty() {
                state = self.pipe_state(&self.sql_pipe(pipe)?).await;
            }
        }
        let sp = self.sql_pipe(pipe)?;
        state.dtypes = super::data::pipe_dtypes(&sp, &state.cols_types, true);
        state.explicit_dtypes = super::data::pipe_dtypes(&sp, &state.cols_types, false);
        if let Some(ct) = &internal.columns_types {
            state.cols_types = ct.clone();
        }
        let dtypes = state.dtypes.clone();

        let internal_duckdb = flavor == Flavor::Duckdb && pipe.connector_keys() == "mrsm";
        let upsert = sp.upsert()
            && (has_update_query(&format!("{}-upsert", flavor.as_str())) || internal_duckdb);
        if upsert {
            check_existing = false;
        }

        let filter_start = std::time::Instant::now();
        let (mut unseen_df, mut update_df, mut delta_df) = if check_existing {
            let (u, up, d) = self.filter_existing_frame(&sp, &state, df).await?;
            (u, Some(up), d)
        } else {
            (df.clone(), None, df)
        };
        crate::facade::trace("sql: filter existing", filter_start);
        if upsert {
            unseen_df = delta_df.clear();
            update_df = Some(delta_df.clone());
        }

        let dt_col = sp.column("datetime");
        let primary_key = sp.column("primary");
        let autoincrement = sp.autoincrement()
            || (is_new
                && primary_key
                    .as_ref()
                    .is_some_and(|pk| !dtypes.contains_key(pk) && !unseen_df.contains(pk)));
        // PARITY: `autoincrement not in pipe.parameters` tests the boolean against the keys, so
        // the parameter is always (re)persisted when truthy (`_pipes.py:2075`).
        if autoincrement {
            let mut patch = Map::new();
            patch.insert("autoincrement".into(), Value::Bool(true));
            pipe.update_parameters(patch);
            if !pipe.temporary {
                let res = self.edit_pipe(pipe, false).await;
                if !res.success {
                    return Ok(res.into());
                }
            }
        }
        let sp = self.sql_pipe(pipe)?;

        let pk_has_values = |f: &Frame| -> bool {
            primary_key.as_ref().is_some_and(|pk| {
                f.data(pk)
                    .map(|d| d.is_null_mask().iter().any(|n| !n))
                    .unwrap_or(false)
            })
        };
        let autoincrement_needs_reset = autoincrement
            && primary_key
                .as_ref()
                .is_some_and(|pk| unseen_df.contains(pk))
            && pk_has_values(&unseen_df);
        if autoincrement && let Some(pk) = &primary_key {
            for f in [
                Some(&mut unseen_df),
                update_df.as_mut(),
                Some(&mut delta_df),
            ]
            .into_iter()
            .flatten()
            {
                if f.contains(pk)
                    && !f
                        .data(pk)
                        .map(|d| d.is_null_mask().iter().any(|n| !n))
                        .unwrap_or(false)
                {
                    f.drop(pk);
                }
            }
        }

        if is_new {
            let res = self
                .create_pipe_table_from_df(&sp, &unseen_df, &state)
                .await;
            if !res.success {
                return Ok(res.into());
            }
        }

        // Pre-create native range partitions so the rows land in an existing partition.
        if self.should_partition(&sp) {
            for part_df in [Some(&unseen_df), update_df.as_ref()].into_iter().flatten() {
                if part_df.height() > 0 {
                    let res = self.create_missing_partitions(&sp, part_df).await;
                    if !res.success {
                        return Ok(res.into());
                    }
                }
            }
        }

        let mut success = true;
        let mut msg = String::new();
        if unseen_df.height() > 0 {
            let dtype = self.get_to_sql_dtype(&sp, &unseen_df, &state, true);
            let identity_insert = flavor == Flavor::Mssql
                && autoincrement
                && primary_key
                    .as_ref()
                    .is_some_and(|pk| unseen_df.contains(pk));
            let (ok, m) = self
                .to_sql_inner(
                    &unseen_df,
                    &sp.target,
                    schema.as_deref(),
                    &dtype,
                    opts.chunksize,
                    identity_insert,
                )
                .await;
            success = ok;
            if !ok {
                msg = m;
            }
        }

        if (is_new || initializing_internal) && !internal.skip_check_indices {
            let cols_types = internal.columns_types.clone();
            if !self.create_indices_sp(&sp, None, None, cols_types).await {
                if cache_internal {
                    return Ok(SyncResult::new(
                        false,
                        format!("Failed to initialize indices for {pipe}."),
                    ));
                }
                tracing::warn!("Failed to create indices for {pipe}. Continuing...");
            }
        }

        if autoincrement_needs_reset && let Some(pk) = &primary_key {
            let max_id_query = get_max_id_query(&sp.target, pk, flavor, schema.as_deref());
            let max_id = self.value(&max_id_query).await.and_then(|v| v.as_i64());
            let queries =
                get_reset_autoincrement_queries(&sp.target, pk, flavor, schema.as_deref(), max_id);
            for r in self.exec_queries(&queries, false, true).await {
                if r.is_none() {
                    tracing::warn!("Could not reset auto-incrementing primary key for {pipe}.");
                }
            }
        }

        if let Some(update) = update_df.as_ref().filter(|u| u.height() > 0) {
            let (ok, m) = self
                .apply_update(&sp, &state, update, upsert, autoincrement)
                .await?;
            if !ok {
                success = false;
                msg = format!("{msg}\n{m}").trim_start().to_string();
            }
        }

        if !success {
            return Ok(SyncResult::new(false, msg));
        }
        let unseen_count = unseen_df.height() as u64;
        let update_count = update_df.as_ref().map_or(0, |u| u.height()) as u64;
        let message = if !upsert {
            format!(
                "Inserted {}, updated {} rows.",
                super::data::fmt_thousands(unseen_count as i64),
                super::data::fmt_thousands(update_count as i64)
            )
        } else {
            format!(
                "Upserted {} row{}.",
                super::data::fmt_thousands(update_count as i64),
                if update_count != 1 { "s" } else { "" }
            )
        };
        if !internal.skip_temporary_tables_check && !cache_internal {
            let (ok, m) = self.drop_old_temporary_tables(false).await;
            if !ok {
                tracing::warn!("{m}");
            }
        }
        let mut result = SyncResult::new(true, message);
        if upsert {
            result.upserted = update_count;
        } else {
            result.inserted = unseen_count;
            result.updated = update_count;
        }
        if cache_internal {
            if is_new {
                state = self.pipe_state(&sp).await;
            }
            self.state
                .lock()
                .expect("state")
                .internal_pipe_states
                .insert(cache_key, state);
        }
        let _ = dt_col;
        Ok(result)
    }

    /// `pipe.filter_existing(df)`: split a frame into `(unseen, update, delta)` against the
    /// rows already in the table (the kernel diff with the backtrack read from this instance).
    pub(crate) async fn filter_existing_frame(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        df: Frame,
    ) -> ConnectorResult<(Frame, Frame, Frame)> {
        let fe_params = FilterExistingParams {
            columns: columns_map(sp),
            configured_dtypes: state.dtypes.clone(),
            dtypes_ctx: self.dtypes_ctx(sp, state),
            autoincrement: sp.autoincrement(),
            autotime: sp.autotime(),
            static_: sp.is_static(),
            include_unchanged_columns: false,
            date_bound_only: false,
            enforce_dtypes: false,
            filter_params_index_limit: self.settings.filter_params_index_limit,
        };
        // The kernel's callback is synchronous: capture the backtrack request first, fetch it,
        // then run the diff with the fetched frame.
        let mut request: Option<BacktrackRequest> = None;
        let _ = filter_existing(PipeInput::Frame(df.clone()), &fe_params, |req| {
            request = Some(req.clone());
            None
        });
        let backtrack = match &request {
            Some(req) if state.exists => {
                let q = meerschaum_connector::DataQuery {
                    begin: meerschaum_connector::DateBound::from_value(&req.begin),
                    end: meerschaum_connector::DateBound::from_value(&req.end),
                    params: Some(backtrack_params(req)),
                    ..Default::default()
                };
                self.get_pipe_data_with_state(
                    sp,
                    state,
                    &q,
                    &super::data::DataQueryOptions::default(),
                )
                .await?
            }
            _ => None,
        };
        filter_existing(PipeInput::Frame(df), &fe_params, move |_| backtrack).map_err(be)
    }

    /// Internal state updates need no staging table. Null/duplicate keys keep the general path.
    async fn upsert_small_internal(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        df: &Frame,
    ) -> ConnectorResult<Option<(bool, String)>> {
        let cols = df.names();
        let mut keys = sp
            .column("primary")
            .map(|k| vec![k])
            .unwrap_or_else(|| columns_map(sp).into_values().collect());
        if (self.should_partition(sp) || self.flavor.is_timescaledb())
            && let Some(dt) = sp.column("datetime")
            && !keys.contains(&dt)
        {
            keys.push(dt);
        }
        let mut unique_keys = std::collections::HashSet::new();
        keys.retain(|key| unique_keys.insert(key.clone()));
        if keys.is_empty() || keys.iter().any(|k| !df.contains(k)) {
            return Ok(None);
        }
        let key_names = keys.clone();
        // DuckDB has no staged upsert template, so a batch repeating a key is collapsed to its
        // last row (applying the rows in order would leave the last one) instead of being
        // rejected. Null keys still cannot be upserted.
        let deduped;
        let df = if self.flavor == Flavor::Duckdb && df.height() > 1 {
            let key_values: Vec<Vec<KValue>> = key_names
                .iter()
                .map(|k| df.data(k).expect("key column").values())
                .collect();
            if key_values.iter().any(|v| v.iter().any(|x| x.is_na())) {
                return Ok(None);
            }
            let mut last: IndexMap<Vec<String>, usize> = IndexMap::new();
            for row in 0..df.height() {
                let key: Vec<String> = key_values.iter().map(|v| v[row].py_str()).collect();
                last.insert(key, row);
            }
            if last.len() == df.height() {
                df
            } else {
                let mut rows: Vec<usize> = last.into_values().collect();
                rows.sort_unstable();
                deduped = df.take(&rows);
                &deduped
            }
        } else {
            df
        };
        let columns: Vec<Vec<KValue>> = cols
            .iter()
            .map(|c| df.data(c).expect("column").values())
            .collect();
        let key_indices: Vec<usize> = keys
            .iter()
            .map(|k| cols.iter().position(|c| c == k).expect("key"))
            .collect();
        let mut seen = std::collections::HashSet::new();
        for (row, _) in columns[0].iter().enumerate() {
            let values: Vec<String> = key_indices
                .iter()
                .map(|&i| columns[i][row].py_str())
                .collect();
            if key_indices.iter().any(|&i| columns[i][row].is_na()) || !seen.insert(values) {
                return Ok(None);
            }
        }
        let f = self.flavor;
        // The column types type the bound text cells on PostgreSQL (`typed_placeholder`). The
        // state predates a table this sync just created, so read the table when it lacks any.
        let fresh_types;
        let cols_types = if f.driver_family() == DriverFamily::Postgres
            && cols.iter().any(|c| !state.cols_types.contains_key(c))
        {
            fresh_types = self
                .get_table_cols_types(&sp.target, self.get_pipe_schema(sp).as_deref())
                .await;
            &fresh_types
        } else {
            &state.cols_types
        };
        let q = |c: &str| sql_item_name(c, f, None);
        let quoted: Vec<String> = cols.iter().map(|c| q(c)).collect();
        let key_sql = keys.iter().map(|k| q(k)).collect::<Vec<_>>().join(", ");
        let updates: Vec<&String> = cols.iter().filter(|c| !keys.contains(c)).collect();
        let mut params = Vec::new();
        let mut values = Vec::new();
        let mut selects = Vec::new();
        let encoding = |i: usize| ColumnEncoding {
            name: cols[i].clone(),
            numeric: state
                .cols_types
                .get(&cols[i])
                .and_then(|t| numeric_precision_scale(t))
                .map(|(p, s)| (Some(p), Some(s))),
        };
        // Oracle's `UNION ALL` branches must agree on every column's datatype, and a bound `NULL`
        // is a typeless `VARCHAR2` (ORA-01790 next to a bound timestamp or number): a `NULL` cell
        // of a column that has typed cells in this batch is written as `CAST(NULL AS <type>)`.
        let oracle_null_types: Vec<Option<&'static str>> = if f == Flavor::Oracle {
            (0..cols.len())
                .map(|i| {
                    let enc = encoding(i);
                    columns[i]
                        .iter()
                        .find_map(|v| match encode_param(v, f, &enc) {
                            SqlParam::Bool(_) | SqlParam::Int(_) => Some("NUMBER"),
                            SqlParam::Float(_) => Some("BINARY_DOUBLE"),
                            SqlParam::Text(_) | SqlParam::Json(_) => Some("VARCHAR2(4000)"),
                            SqlParam::Timestamp(_) => Some("TIMESTAMP"),
                            SqlParam::TimestampTz(_) => Some("TIMESTAMP WITH TIME ZONE"),
                            SqlParam::Date(_) => Some("DATE"),
                            SqlParam::Bytes(_) => Some("RAW(2000)"),
                            SqlParam::Null => None,
                        })
                })
                .collect()
        } else {
            Vec::new()
        };
        for (row, _) in columns[0].iter().enumerate() {
            let mut cells = Vec::new();
            for (i, col) in cols.iter().enumerate() {
                let enc = encoding(i);
                let param = encode_param(&columns[i][row], f, &enc);
                if let (SqlParam::Null, Some(Some(typ))) = (&param, oracle_null_types.get(i)) {
                    cells.push(format!("CAST(NULL AS {typ})"));
                    continue;
                }
                cells.push(crate::driver::params::typed_placeholder(
                    f,
                    params.len() + 1,
                    &param,
                    cols_types.get(col).map(String::as_str).unwrap_or(""),
                ));
                params.push(param);
            }
            values.push(format!("({})", cells.join(", ")));
            selects.push(format!(
                "SELECT {}{}",
                cells
                    .iter()
                    .zip(&quoted)
                    .map(|(v, c)| format!("{v} AS {c}"))
                    .collect::<Vec<_>>()
                    .join(", "),
                if f == Flavor::Oracle {
                    " FROM dual"
                } else {
                    ""
                }
            ));
        }
        let table = self.pipe_table_name(sp);
        let insert = format!(
            "INSERT INTO {table} ({}) VALUES {}",
            quoted.join(", "),
            values.join(", ")
        );
        let sql = match f.driver_family() {
            DriverFamily::Postgres | DriverFamily::Sqlite => {
                let action = if updates.is_empty() {
                    "NOTHING".into()
                } else {
                    format!(
                        "UPDATE SET {}",
                        updates
                            .iter()
                            .map(|c| format!("{}=excluded.{}", q(c), q(c)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                let target = if f.driver_family() == DriverFamily::Sqlite {
                    // SQLite can retain a pre-DDL schema on another pooled connection while
                    // preparing an explicit conflict target. Internal pipes have only their
                    // declared key constraints, so the targetless form is equivalent.
                    String::new()
                } else {
                    format!(" ({key_sql})")
                };
                format!("{insert} ON CONFLICT{target} DO {action}")
            }
            DriverFamily::Mysql => {
                let update = if updates.is_empty() {
                    vec![format!("{}={}", q(&keys[0]), q(&keys[0]))]
                } else {
                    updates
                        .iter()
                        .map(|c| format!("{}=VALUES({})", q(c), q(c)))
                        .collect()
                };
                format!("{insert} ON DUPLICATE KEY UPDATE {}", update.join(", "))
            }
            DriverFamily::Mssql | DriverFamily::Oracle | DriverFamily::Duckdb => {
                let on = keys
                    .iter()
                    .map(|k| format!("t.{}=s.{}", q(k), q(k)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let update = if updates.is_empty() {
                    String::new()
                } else {
                    format!(
                        " WHEN MATCHED THEN UPDATE SET {}",
                        updates
                            .iter()
                            .map(|c| format!("{}=s.{}", q(c), q(c)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                let lock = if f == Flavor::Mssql {
                    " WITH (HOLDLOCK)"
                } else {
                    ""
                };
                let terminator = if f == Flavor::Mssql { ";" } else { "" };
                format!(
                    "MERGE INTO {table}{lock} t USING ({}) s ON ({on}){update} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({}){terminator}",
                    selects.join(" UNION ALL "),
                    quoted.join(", "),
                    quoted
                        .iter()
                        .map(|c| format!("s.{c}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        };
        self.log(&sql);
        self.driver.exec_with(&sql, &params).await.map_err(be)?;
        Ok(Some((true, String::new())))
    }

    /// The update / upsert path of `sync_pipe`: stage the rows in a temporary table, then
    /// `get_update_queries`.
    async fn apply_update(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        update_df: &Frame,
        upsert: bool,
        autoincrement: bool,
    ) -> ConnectorResult<(bool, String)> {
        let flavor = self.flavor;
        let pipe = sp.pipe();
        if upsert
            && !autoincrement
            && pipe.connector_keys() == "mrsm"
            && (update_df.height() <= 64 || flavor == Flavor::Duckdb)
        {
            if let Some(result) = self.upsert_small_internal(sp, state, update_df).await? {
                return Ok(result);
            }
            if flavor == Flavor::Duckdb {
                // v4 skips DuckDB indices and has no staged upsert template. Do not silently
                // turn an invalid internal upsert into a plain insert or an empty update.
                return Err(be("Internal DuckDB upserts require non-null keys."));
            }
        }
        let temp_target = self.get_temporary_target(
            &sp.target,
            None,
            Some(if upsert { "upsert" } else { "update" }),
        );
        let _ = self
            .log_temporary_tables_creation(
                std::slice::from_ref(&temp_target),
                false,
                !pipe.temporary,
            )
            .await;
        let pipe_dtypes = &state.dtypes;
        let mut update_dtypes: IndexMap<String, String> = update_df.dtypes();
        for (c, t) in get_special_cols(update_df) {
            update_dtypes.insert(c, t);
        }
        for col in update_df.names() {
            if let Some(t) = pipe_dtypes.get(&col) {
                let all_null = update_df
                    .data(&col)
                    .map(|d| d.is_null_mask().iter().all(|n| *n))
                    .unwrap_or(false);
                if all_null {
                    update_dtypes.insert(col.clone(), t.clone());
                }
            }
        }
        let temp_columns: Map = columns_map(sp)
            .into_iter()
            .filter(|(_, c)| update_df.contains(c))
            .map(|(k, c)| {
                (
                    if k == "primary" {
                        "primary_".to_string()
                    } else {
                        k
                    },
                    Value::Str(c),
                )
            })
            .collect();
        let mut parameters = Map::new();
        parameters.insert(
            "schema".into(),
            self.internal_schema()
                .map(|s| Value::Str(s.into()))
                .unwrap_or(Value::Null),
        );
        parameters.insert("hypertable".into(), Value::Bool(false));
        let mut builder = Pipe::builder(
            &format!("{}_", pipe.connector_keys().replace(':', "_")),
            pipe.metric_key(),
        )
        .location(pipe.location_key())
        .instance(pipe.instance_keys())
        .parameters(parameters)
        .kwarg("columns", Value::Map(temp_columns))
        .kwarg(
            "dtypes",
            Value::Map(
                update_dtypes
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                    .collect(),
            ),
        )
        .kwarg("target", Value::Str(temp_target.clone()))
        .kwarg("temporary", true)
        .kwarg("enforce", false)
        .kwarg("static", true)
        .kwarg("autoincrement", false)
        .kwarg("cache", false);
        builder = builder.defaults(self.settings.pipe_defaults.clone());
        let mut temp_pipe = builder.build().map_err(be)?;
        let temp_columns_types: IndexMap<String, String> = update_dtypes
            .iter()
            .map(|(c, t)| (c.clone(), get_db_type_from_pd_type(t, Some(flavor))))
            .collect();
        let temp_opts = SyncOptions {
            check_existing: false,
            ..Default::default()
        };
        let temp_result = self
            .sync_pipe_impl(
                &mut temp_pipe,
                update_df.clone(),
                &temp_opts,
                InternalSync {
                    cached_state: None,
                    columns_types: Some(temp_columns_types.clone()),
                    skip_check_indices: true,
                    new_special_cols: None,
                    skip_temporary_tables_check: false,
                },
            )
            .await;
        if !temp_result.success {
            return Ok((false, temp_result.message));
        }

        let existing_cols = if state.cols_types.is_empty() {
            self.columns_types(sp).await
        } else {
            state.cols_types.clone()
        };
        let dt_col = sp.column("datetime");
        let primary_key = sp.column("primary");
        // A partitioned table folds the datetime column into its composite primary key, so the
        // conflict target must include it too.
        let partition_upsert = dt_col.as_ref().is_some_and(|c| update_df.contains(c))
            && (flavor.is_timescaledb() || self.should_partition(sp));
        let join_cols: Vec<String> = match &primary_key {
            Some(pk) if flavor != Flavor::Oracle => {
                if partition_upsert {
                    vec![dt_col.clone().unwrap_or_default(), pk.clone()]
                } else {
                    vec![pk.clone()]
                }
            }
            _ => columns_map(sp)
                .into_iter()
                .map(|(_, c)| c)
                .filter(|c| existing_cols.contains_key(c))
                .collect(),
        };
        let schema = self.get_pipe_schema(sp);
        let internal_schema = self.internal_schema().map(str::to_string);
        let db_version = if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) {
            self.db_version().await
        } else {
            None
        };
        let datetime_col = dt_col.as_deref().filter(|c| update_df.contains(c));
        let mut args = UpdateQueriesArgs::new(&sp.target, &temp_target, join_cols, flavor);
        args.upsert = upsert;
        args.schema = schema.as_deref();
        args.patch_schema = internal_schema.as_deref();
        args.target_cols_types = existing_cols;
        args.patch_cols_types = temp_columns_types;
        args.datetime_col = datetime_col;
        args.identity_insert = autoincrement
            && primary_key
                .as_ref()
                .is_some_and(|pk| update_df.contains(pk));
        args.null_indices = sp.null_indices();
        args.cast_columns = sp.enforce();
        args.db_version = db_version.as_deref();
        // SQLite cannot use a plain key index for the COALESCE equality used by v4.
        // Index the small staging table's exact expressions so a scattered update does not
        // scan that table once per row in the (potentially very large) target date range.
        // PARITY: retain v4's null-sentinel matching, including sentinel collisions.
        if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) && args.null_indices && !upsert {
            let expressions = args
                .join_cols
                .iter()
                .filter_map(|column| {
                    args.target_cols_types.get(column).map(|dtype| {
                        format!(
                            "COALESCE({}, {})",
                            sql_item_name(column, flavor, None),
                            crate::queries::update::get_null_replacement(dtype, Some(flavor))
                        )
                    })
                })
                .collect::<Vec<_>>();
            if !expressions.is_empty() {
                let index = sql_item_name(&format!("IX_{temp_target}_join"), flavor, None);
                let table = sql_item_name(&temp_target, flavor, internal_schema.as_deref());
                let _ = self
                    .exec(&format!(
                        "CREATE INDEX {index} ON {table} ({})",
                        expressions.join(", ")
                    ))
                    .await;
            }
        }
        let update_queries = get_update_queries(&args);
        let update_start = std::time::Instant::now();
        let update_success = self.exec_queries_all(&update_queries, true, true).await;
        crate::facade::trace("sql: execute update", update_start);
        let _ = self
            .log_temporary_tables_creation(&[temp_target], true, !pipe.temporary)
            .await;
        if !update_success {
            tracing::warn!("Failed to apply update to {pipe}.");
            return Ok((false, format!("Failed to apply update to {pipe}.")));
        }
        Ok((true, String::new()))
    }

    /// `create_pipe_table_from_df(pipe, df)`: create the target table from the pipe's dtypes
    /// and the incoming frame (hypertables, Hypercore, native partitions included).
    pub(crate) async fn create_pipe_table_from_df(
        &self,
        sp: &SqlPipe,
        df: &Frame,
        state: &PipeState,
    ) -> SuccessTuple {
        let flavor = self.flavor;
        let pipe_dtypes = &state.dtypes;
        let df_dtypes = df.dtypes();
        let primary_key = sp.column("primary");
        let primary_key_typ = primary_key.as_ref().map(|pk| {
            pipe_dtypes
                .get(pk)
                .cloned()
                .unwrap_or_else(|| df_dtypes.get(pk).cloned().unwrap_or_else(|| "int".into()))
        });
        let mut primary_key_db_type = primary_key_typ
            .as_ref()
            .map(|t| get_db_type_from_pd_type(t, Some(flavor)));
        let dt_col = sp.column("datetime");
        let mut new_dtypes: IndexMap<String, String> = df_dtypes.clone();
        for (ix, col) in columns_map(sp) {
            if ix != "primary" {
                new_dtypes.insert(
                    col.clone(),
                    df_dtypes.get(&col).cloned().unwrap_or_else(|| "int".into()),
                );
            }
        }
        for (c, t) in get_special_cols(df) {
            new_dtypes.insert(c, t);
        }
        for (c, t) in pipe_dtypes {
            new_dtypes.insert(c.clone(), t.clone());
        }
        let autoincrement = sp.autoincrement()
            || primary_key
                .as_ref()
                .is_some_and(|pk| !new_dtypes.contains_key(pk));
        if autoincrement && let Some(pk) = &primary_key {
            new_dtypes.shift_remove(pk);
        }
        // Internal state keys must be indexable on engines which disallow TEXT keys.
        // Keep the public pipe dtype unchanged; only the physical key type is bounded.
        let internal_key_types = sp.pipe().connector_keys() == "mrsm"
            && matches!(
                flavor,
                Flavor::Mysql | Flavor::Mariadb | Flavor::Mssql | Flavor::Oracle
            );
        if internal_key_types {
            let keys = columns_map(sp);
            for (column, dtype) in &mut new_dtypes {
                let db_type = get_db_type_from_pd_type(dtype, Some(flavor));
                *dtype = if keys.values().any(|key| key == column)
                    && matches!(
                        db_type.as_str(),
                        "TEXT" | "NVARCHAR(MAX)" | "VARCHAR(MAX)" | "NVARCHAR2(2000)"
                    ) {
                    match flavor {
                        Flavor::Mssql => "NVARCHAR(255)",
                        Flavor::Oracle => "NVARCHAR2(255)",
                        _ => "VARCHAR(255)",
                    }
                    .into()
                } else {
                    db_type
                };
            }
            // The Syncing Engine's state tables (new in v5, no v4 layout to keep) hold free text
            // (planner reasons, sync messages): declare utf8mb4 so a database whose default
            // charset is not (MySQL 5.7 defaults to latin1) still records every row.
            if matches!(flavor, Flavor::Mysql | Flavor::Mariadb)
                && meerschaum_connector::pipe_ops::engine_state_parameters(sp.pipe().metric_key())
                    .is_some()
            {
                for dtype in new_dtypes.values_mut() {
                    let upper = dtype.to_ascii_uppercase();
                    let textual = [
                        "TEXT",
                        "TINYTEXT",
                        "MEDIUMTEXT",
                        "LONGTEXT",
                        "VARCHAR",
                        "CHAR",
                    ]
                    .iter()
                    .any(|t| upper == *t || upper.starts_with(&format!("{t}(")));
                    if textual {
                        dtype.push_str(" CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
                    }
                }
            }
            primary_key_db_type = primary_key
                .as_ref()
                .and_then(|key| new_dtypes.get(key).cloned());
        }
        let schema = self.get_pipe_schema(sp);

        let hypertable = flavor.is_timescaledb() && sp.hypertable() && dt_col.is_some();
        let hypercore = hypertable && sp.hypercore();
        let (mut chunk_str, mut segmentby, mut orderby) = (None, None, None);
        if hypercore {
            if let Ok(ci) = sp.chunk_interval() {
                chunk_str = Some(match ci {
                    ChunkInterval::Int(i) => i.to_string(),
                    ChunkInterval::Delta(d) => {
                        format!("{} minutes", (d.num_seconds() as f64 / 60.0) as i64)
                    }
                });
            }
            let settings = get_compress_settings(flavor, sp);
            segmentby = Some(settings.segmentby).filter(|s| !s.is_empty());
            orderby = Some(settings.orderby).filter(|s| !s.is_empty());
        }

        let mut partition_by_column = crate::queries::partition::get_partition_column(flavor, sp);
        let partition_bounds = if partition_by_column.is_some() && self.inline_partitions() {
            Some(self.initial_partition_bounds(sp, df))
        } else {
            None
        };
        if partition_by_column.is_some()
            && self.inline_partitions()
            && partition_bounds.as_ref().is_none_or(|b| b.is_empty())
        {
            partition_by_column = None;
        }

        // MSSQL partitions through a function + scheme created before the table; its clustered
        // index is placed on the scheme.
        let (partition_scheme_name, partition_creation_queries) =
            if partition_by_column.is_some() && flavor == Flavor::Mssql {
                let (name, queries) = self.mssql_partition_creation(sp, df);
                (Some(name), queries)
            } else {
                (None, Vec::new())
            };

        let build = |chunk: Option<String>| -> crate::Result<Vec<String>> {
            let opts = CreateTableOptions {
                schema: schema.clone(),
                primary_key: primary_key.clone(),
                primary_key_db_type: primary_key_db_type.clone(),
                autoincrement: false,
                datetime_column: dt_col.clone(),
                hypertable_segmentby: if chunk.is_some() {
                    segmentby.clone()
                } else {
                    None
                },
                hypertable_orderby: if chunk.is_some() {
                    orderby.clone()
                } else {
                    None
                },
                hypertable_chunk_interval: chunk,
                partition_by_column: partition_by_column.clone(),
                partition_bounds: partition_bounds.clone(),
                partition_scheme_name: partition_scheme_name.clone(),
                parse_dtypes: !internal_key_types,
            };
            let mut queries = get_create_table_queries(
                &QueryOrDtypes::Dtypes(new_dtypes.clone()),
                &sp.target,
                flavor,
                &opts,
            )?;
            if !partition_creation_queries.is_empty() {
                let mut q = partition_creation_queries.clone();
                q.extend(queries);
                queries = q;
            }
            if let Some(s) = schema.as_deref().filter(|s| !s.is_empty()) {
                let mut q = get_create_schema_if_not_exists_queries(Some(s), flavor)?;
                if sp.pipe().temporary
                    && sp.is_static()
                    && sp.pipe().connector_keys() == "mrsm"
                    && flavor.driver_family() == DriverFamily::Postgres
                    && flavor != Flavor::Cockroachdb
                {
                    use sha2::{Digest, Sha256};

                    // Distinct target locks do not protect creation of their common
                    // schema. Lock only while an absent schema is created, in the same
                    // transaction as its DDL. Existing schemas take no schema lock.
                    let schema = truncate_item_name(s, flavor);
                    let mut digest = Sha256::new();
                    digest.update(b"meerschaum:internal-schema-initialization:");
                    digest.update(schema.as_bytes());
                    let key =
                        i64::from_be_bytes(digest.finalize()[..8].try_into().expect("eight bytes"));
                    q.insert(0, format!(
                        "SELECT pg_advisory_xact_lock({key}) WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '{}')",
                        schema.replace('\'', "''"),
                    ));
                }
                q.extend(queries);
                queries = q;
            }
            Ok(queries)
        };
        let queries = match build(chunk_str.clone()) {
            Ok(q) => q,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let mut success = self.exec_queries_all(&queries, true, true).await;
        if !success && hypercore {
            // Declarative hypertable syntax unsupported; retry as a plain table.
            if let Ok(queries) = build(None) {
                success = self.exec_queries_all(&queries, true, true).await;
            }
        }
        if success && hypercore && !self.set_integer_now_func(sp).await {
            tracing::warn!(
                "Could not register an `integer_now` function for {}; its columnstore policy will fail on every run.",
                sp.pipe()
            );
        }
        if success && flavor == Flavor::Geopackage {
            return self.init_geopackage_pipe(sp, &new_dtypes, df).await;
        }
        if success {
            SuccessTuple::ok("Success")
        } else {
            SuccessTuple::fail(format!("Failed to create {}.", self.pipe_table_name(sp)))
        }
    }

    /// `df.dtypes` with object columns refined from their first value (`get_add_columns_queries`).
    fn df_cols_types_refined(df: &Frame) -> IndexMap<String, String> {
        let mut out = df.dtypes();
        if df.height() > 0 {
            for (col, typ) in out.iter_mut() {
                if typ != "object" {
                    continue;
                }
                let Ok(data) = df.data(col) else { continue };
                match data.get(0) {
                    KValue::Json(_) => *typ = "json".into(),
                    KValue::Decimal(_) => *typ = "numeric".into(),
                    KValue::Str(_) => *typ = "str".into(),
                    _ => {}
                }
            }
        }
        out
    }

    /// `get_add_columns_queries(pipe, df)`: `ALTER TABLE ... ADD` for columns new to the table.
    ///
    /// PARITY: Python iterates a `set` of new columns (hash order); Rust keeps frame order.
    pub async fn get_add_columns_queries(&self, sp: &SqlPipe, df: &Frame) -> Vec<String> {
        let schema = self.get_pipe_schema(sp);
        if !self.table_exists(&sp.target, schema.as_deref()).await || sp.is_static() {
            return Vec::new();
        }
        let df_cols_types = Self::df_cols_types_refined(df);
        let db_cols_types = self
            .get_table_cols_types(&sp.target, schema.as_deref())
            .await;
        self.add_columns_queries_for(sp, &df_cols_types, &db_cols_types)
    }

    /// The pure part of `get_add_columns_queries`: the incoming pandas dtypes against the
    /// table's database types.
    pub fn add_columns_queries_for(
        &self,
        sp: &SqlPipe,
        df_cols_types: &IndexMap<String, String>,
        db_cols_types: &IndexMap<String, String>,
    ) -> Vec<String> {
        if sp.is_static() {
            return Vec::new();
        }
        let flavor = self.flavor;
        let new_cols: Vec<(String, String)> = df_cols_types
            .iter()
            .filter(|(c, t)| !db_cols_types.contains_key(*c) && !c.is_empty() && !t.is_empty())
            .map(|(c, t)| (c.clone(), get_db_type_from_pd_type(t, Some(flavor))))
            .collect();
        if new_cols.is_empty() {
            return Vec::new();
        }
        let mut alter = format!("ALTER TABLE {}", self.pipe_table_name(sp));
        let mut queries = Vec::new();
        for (col, typ) in &new_cols {
            let add = format!("\nADD {} {typ},", sql_item_name(col, flavor, None));
            if flavor.single_alter_table() {
                queries.push(format!("{alter}{}", &add[..add.len() - 1]));
            } else {
                alter.push_str(&add);
            }
        }
        if queries.is_empty() {
            alter.pop();
            queries.push(alter);
        }
        queries
    }

    /// `get_alter_columns_queries(pipe, df)`: retype columns whose incoming dtype conflicts with
    /// the table (to `TEXT`, or `NUMERIC` for numeric columns). May persist numeric dtypes.
    pub async fn get_alter_columns_queries(
        &self,
        pipe: &mut Pipe,
        df: &Frame,
        new_special_cols: Option<&IndexMap<String, String>>,
    ) -> Vec<String> {
        let Ok(sp) = self.sql_pipe(pipe) else {
            return Vec::new();
        };
        let schema = self.get_pipe_schema(&sp);
        if !self.table_exists(&sp.target, schema.as_deref()).await || sp.is_static() {
            return Vec::new();
        }
        let state = self.pipe_state(&sp).await;
        self.alter_columns_with_state(pipe, df, new_special_cols, &state)
            .await
    }

    async fn alter_columns_with_state(
        &self,
        pipe: &mut Pipe,
        df: &Frame,
        new_special_cols: Option<&IndexMap<String, String>>,
        state: &PipeState,
    ) -> Vec<String> {
        let Ok(sp) = self.sql_pipe(pipe) else {
            return Vec::new();
        };
        if !state.exists || sp.is_static() {
            return Vec::new();
        }
        let input = AlterColumnsInput {
            df_cols_types: df.dtypes(),
            numeric_cols: get_numeric_cols(df),
            db_cols_types: state.cols_types.clone(),
            pipe_dtypes: state.dtypes.clone(),
            explicit_dtypes: state.explicit_dtypes.clone(),
            new_special_cols: new_special_cols.cloned().unwrap_or_default(),
            session_id: meerschaum_core::users::generate_password(3)
                .unwrap_or_else(|_| "000".into()),
        };
        let (queries, new_dtypes) = self.alter_columns_queries_for(&sp, &input);
        if let Some(explicit) = new_dtypes {
            pipe.set_dtypes(
                explicit
                    .into_iter()
                    .map(|(k, v)| (k, Value::Str(v)))
                    .collect(),
            );
            if !pipe.temporary {
                let res = self.edit_pipe(pipe, false).await;
                if !res.success {
                    tracing::warn!(
                        "Failed to update dtypes for numeric columns {}:\n{}",
                        super::values::items_str(&input.numeric_cols),
                        res.message
                    );
                }
            }
        }
        queries
    }

    /// The pure part of `get_alter_columns_queries`. Returns the queries and, when numeric
    /// columns were detected, the explicit dtypes Python persists with `pipe.edit()`.
    pub fn alter_columns_queries_for(
        &self,
        sp: &SqlPipe,
        input: &AlterColumnsInput,
    ) -> (Vec<String>, Option<IndexMap<String, String>>) {
        if sp.is_static() {
            return (Vec::new(), None);
        }
        let flavor = self.flavor;
        let schema = self.get_pipe_schema(sp);
        let target = sp.target.clone();
        let session_id = input.session_id.clone();
        let mut numeric_cols = input.numeric_cols.clone();
        let df_cols_types = &input.df_cols_types;
        let db_cols_types: IndexMap<String, String> = input
            .db_cols_types
            .iter()
            .map(|(c, t)| (c.clone(), get_pd_type_from_db_type(t, true)))
            .collect();
        let pipe_dtypes = &input.pipe_dtypes;
        let pipe_bool_cols: Vec<&String> = pipe_dtypes
            .iter()
            .filter(|(_, t)| are_dtypes_equal(t, "bool"))
            .map(|(c, _)| c)
            .collect();
        let mut aliases: Vec<(&str, &str)> = vec![
            ("int", "bool"),
            ("float", "bool"),
            ("numeric", "bool"),
            ("guid", "object"),
        ];
        match flavor {
            Flavor::Oracle => {
                aliases.retain(|(k, _)| !matches!(*k, "int" | "numeric"));
                aliases.extend([("int", "numeric"), ("date", "datetime"), ("numeric", "int")]);
            }
            Flavor::Geopackage => aliases.extend([("geometry", "bytes"), ("bytes", "geometry")]),
            _ => {}
        }
        let db_typ_of = |c: &str| {
            db_cols_types
                .get(c)
                .cloned()
                .unwrap_or_else(|| "object".into())
        };
        let mut altered_cols: IndexMap<String, (String, String)> = df_cols_types
            .iter()
            .filter(|(c, t)| {
                let db = db_typ_of(c);
                !are_dtypes_equal(t, &db.to_lowercase()) && !are_dtypes_equal(&db, "string")
            })
            .map(|(c, t)| (c.clone(), (db_typ_of(c), t.clone())))
            .collect();
        let new_special = input.new_special_cols.clone();
        for (c, t) in &new_special {
            altered_cols.insert(c.clone(), (db_typ_of(c), t.clone()));
        }
        // NOTE: Sometimes bools are coerced into ints or floats.
        let mut ignore: Vec<String> = Vec::new();
        for (col, (db_typ, df_typ)) in &altered_cols {
            for (db_alias, df_alias) in &aliases {
                if db_typ.to_lowercase().contains(db_alias)
                    && df_typ.to_lowercase().contains(df_alias)
                    && !new_special.contains_key(col)
                {
                    ignore.push(col.clone());
                }
            }
        }
        let bool_compat = |t: &str| {
            are_dtypes_equal("int", t)
                || are_dtypes_equal("float", t)
                || are_dtypes_equal("numeric", t)
                || are_dtypes_equal("bool", t)
        };
        for bool_col in pipe_bool_cols {
            if let Some((db, dft)) = altered_cols.get(bool_col)
                && bool_compat(db)
                && bool_compat(dft)
            {
                ignore.push(bool_col.clone());
            }
        }
        for c in &ignore {
            altered_cols.shift_remove(c);
        }
        if altered_cols.is_empty() {
            return (Vec::new(), None);
        }
        let mut new_dtypes = None;
        if !numeric_cols.is_empty() {
            let mut explicit = input.explicit_dtypes.clone();
            for c in &numeric_cols {
                explicit.insert(c.clone(), "numeric".into());
            }
            new_dtypes = Some(explicit);
        } else {
            numeric_cols.extend(
                pipe_dtypes
                    .iter()
                    .filter(|(_, t)| t.starts_with("numeric"))
                    .map(|(c, _)| c.clone()),
            );
        }
        let numeric_type = get_db_type_from_pd_type("numeric", Some(flavor));
        let text_type = get_db_type_from_pd_type("str", Some(flavor));
        let altered_types: IndexMap<String, String> = altered_cols
            .keys()
            .map(|c| {
                (
                    c.clone(),
                    if numeric_cols.contains(c) {
                        numeric_type.clone()
                    } else {
                        text_type.clone()
                    },
                )
            })
            .collect();
        let q = |c: &str| sql_item_name(c, flavor, None);
        let target_name = sql_item_name(&target, flavor, schema.as_deref());

        if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) {
            let temp_table_name = format!("-{session_id}_{target}");
            let rename_query = format!(
                "ALTER TABLE {target_name} RENAME TO {}",
                q(&temp_table_name)
            );
            // PARITY: the rebuilt table declares the *pandas* types `get_pd_type_from_db_type`
            // returns for unaltered columns (`_pipes.py:3793`).
            let mut create_query = format!("CREATE TABLE {target_name} (\n");
            for (col, typ) in &db_cols_types {
                let t = altered_types.get(col).unwrap_or(typ);
                create_query.push_str(&format!("{} {t},\n", q(col)));
            }
            create_query.truncate(create_query.len() - 2);
            create_query.push_str("\n)");
            let cols: Vec<String> = db_cols_types.keys().map(|c| q(c)).collect();
            let mut insert_query =
                format!("INSERT INTO {target_name} ({})\nSELECT\n", cols.join(", "));
            for col in db_cols_types.keys() {
                let s = match altered_types.get(col) {
                    Some(t) => format!("CAST({} AS {t})", q(col)),
                    None => q(col),
                };
                insert_query.push_str(&format!("{s},\n"));
            }
            insert_query.truncate(insert_query.len() - 2);
            insert_query.push_str(&format!(
                "\nFROM {}",
                sql_item_name(&temp_table_name, flavor, schema.as_deref())
            ));
            let if_exists = if flavor.drop_if_exists() {
                "IF EXISTS"
            } else {
                ""
            };
            let drop_query = format!(
                "DROP TABLE {if_exists}{}",
                sql_item_name(&temp_table_name, flavor, schema.as_deref())
            );
            return (
                vec![rename_query, create_query, insert_query, drop_query],
                new_dtypes,
            );
        }

        if flavor == Flavor::Oracle {
            // `_pipes.py:3843`: Oracle cannot change a populated column's type in place (and has
            // no `TYPE` clause), so each altered column goes through a temporary column.
            let mut queries = Vec::new();
            let temp = |c: &str| q(&format!("{c}_temp"));
            for (col, typ) in &altered_types {
                queries.push(format!(
                    "ALTER TABLE {target_name}\nADD {} {typ}",
                    temp(col)
                ));
            }
            for col in altered_types.keys() {
                queries.push(format!(
                    "UPDATE {target_name}\nSET {} = {}",
                    temp(col),
                    q(col)
                ));
            }
            for col in altered_types.keys() {
                queries.push(format!("UPDATE {target_name}\nSET {} = NULL", q(col)));
            }
            for (col, typ) in &altered_types {
                queries.push(format!(
                    "ALTER TABLE {target_name}\nMODIFY {} {typ}",
                    q(col)
                ));
            }
            for col in altered_types.keys() {
                queries.push(format!(
                    "UPDATE {target_name}\nSET {} = {}",
                    q(col),
                    temp(col)
                ));
            }
            for col in altered_types.keys() {
                queries.push(format!(
                    "ALTER TABLE {target_name}\nDROP COLUMN {}",
                    temp(col)
                ));
            }
            return (queries, new_dtypes);
        }

        let mut queries = Vec::new();
        let mut query = format!("ALTER TABLE {target_name}");
        for (col, typ) in &altered_types {
            let prefix = if flavor.mysql_partition() || flavor == Flavor::Oracle {
                "MODIFY"
            } else {
                "ALTER"
            };
            let type_prefix = if matches!(flavor, Flavor::Mssql | Flavor::Mariadb | Flavor::Mysql) {
                ""
            } else {
                "TYPE "
            };
            let column_str = if flavor != Flavor::Oracle {
                "COLUMN"
            } else {
                ""
            };
            let suffix = format!("\n{prefix} {column_str} {} {type_prefix}{typ},", q(col));
            if !flavor.single_alter_table() {
                query.push_str(&suffix);
            } else {
                queries.push(format!("{query}{}", &suffix[..suffix.len() - 1]));
            }
        }
        if !flavor.single_alter_table() {
            query.pop();
            queries.push(query);
        }
        (queries, new_dtypes)
    }

    /// `get_to_sql_dtype(pipe, df, update_dtypes)`: column → database type for `to_sql`.
    pub fn get_to_sql_dtype(
        &self,
        sp: &SqlPipe,
        df: &Frame,
        state: &PipeState,
        update_dtypes: bool,
    ) -> IndexMap<String, String> {
        let _ = sp;
        let mut df_dtypes = df.dtypes();
        for (c, t) in get_special_cols(df) {
            df_dtypes.insert(c, t);
        }
        if update_dtypes {
            for (c, t) in &state.dtypes {
                df_dtypes.insert(c.clone(), t.clone());
            }
        }
        df_dtypes
            .into_iter()
            .filter(|(c, t)| !c.is_empty() && !t.is_empty())
            .map(|(c, t)| (c, get_db_type_from_pd_type(&t, Some(self.flavor))))
            .collect()
    }

    /// `to_sql(df, name, if_exists='append', schema, chunksize, dtype)`: insert a frame's rows
    /// (PostgreSQL `COPY` when bulk inserts are enabled, multi-row bound `INSERT`s otherwise),
    /// in one transaction. Returns `(success, error message)`.
    pub async fn to_sql(
        &self,
        df: &Frame,
        name: &str,
        schema: Option<&str>,
        dtype: &IndexMap<String, String>,
        chunksize: Option<i64>,
    ) -> (bool, String) {
        self.to_sql_inner(df, name, schema, dtype, chunksize, false)
            .await
    }

    /// [`Self::to_sql`], optionally wrapped in `SET IDENTITY_INSERT ... ON` / `OFF` on the same
    /// connection and transaction (MSSQL autoincrement pipes given explicit keys,
    /// `_pipes.py:2201`).
    pub(crate) async fn to_sql_inner(
        &self,
        df: &Frame,
        name: &str,
        schema: Option<&str>,
        dtype: &IndexMap<String, String>,
        chunksize: Option<i64>,
        identity_insert: bool,
    ) -> (bool, String) {
        let flavor = self.flavor;
        let schema = schema.or(self.schema());
        let truncated = truncate_item_name(name, flavor);
        let cols = df.names();
        if df.height() == 0 || cols.is_empty() {
            return (true, String::new());
        }
        let numeric_cols = get_numeric_cols(df);
        let encodings: Vec<ColumnEncoding> = cols
            .iter()
            .map(|c| {
                let db_type = dtype.get(c).cloned().unwrap_or_default();
                let is_numeric_dtype = db_type.to_lowercase().contains("numeric");
                let numeric = if is_numeric_dtype {
                    Some(
                        numeric_precision_scale(&db_type)
                            .map(|(p, s)| (Some(p), Some(s)))
                            .unwrap_or((None, None)),
                    )
                } else if numeric_cols.contains(c) {
                    Some(crate::dtypes::get_numeric_precision_scale(
                        Some(flavor),
                        None,
                    ))
                } else {
                    None
                };
                ColumnEncoding {
                    name: c.clone(),
                    numeric,
                }
            })
            .collect();
        let columns: Vec<Vec<KValue>> = cols
            .iter()
            .map(|c| df.data(c).map(|d| d.values()).unwrap_or_default())
            .collect();
        let n = df.height();
        let table_name = sql_item_name(&truncated, flavor, schema);

        if flavor.driver_family() == DriverFamily::Postgres && self.settings.bulk_insert {
            let col_list: Vec<String> = cols.iter().map(|c| format!("\"{c}\"")).collect();
            let copy_sql = format!(
                "COPY {} ({}) FROM STDIN WITH CSV NULL '\\N'",
                sql_item_name(&truncated, Flavor::Postgresql, schema),
                col_list.join(", ")
            );
            let mut buf = String::new();
            let mut fields: Vec<Option<String>> = Vec::with_capacity(cols.len());
            // Rows are read column-major.
            #[allow(clippy::needless_range_loop)]
            for i in 0..n {
                fields.clear();
                for (j, enc) in encodings.iter().enumerate() {
                    fields.push(encode_copy_field(&columns[j][i], enc));
                }
                csv_row(&fields, &mut buf);
            }
            self.log(&copy_sql);
            return match self.driver().copy_in(&copy_sql, buf.into_bytes()).await {
                Ok(_) => (true, String::new()),
                Err(e) => {
                    tracing::warn!("Failed to insert into {table_name}:\n{e}");
                    (false, e.to_string())
                }
            };
        }

        if flavor == Flavor::Mssql
            && self.settings.bulk_insert
            && let Some(statements) = mssql_insert_json_statements(
                &table_name,
                &cols,
                &columns,
                &encodings,
                dtype,
                &numeric_cols,
                n,
                match chunksize {
                    Some(-1) => Some(self.settings.chunksize),
                    other => other,
                },
            )
        {
            return self
                .exec_insert_statements(statements, &table_name, identity_insert)
                .await;
        }

        let default_chunksize = self.settings.chunksize;
        let mut chunk = match chunksize {
            Some(-1) => Some(default_chunksize),
            other => other,
        };
        if let (Some(c), Some(max)) = (chunk, max_chunks(flavor))
            && c > max
        {
            chunk = Some(max);
        }
        let max_rows_per_stmt = (max_bind_params(flavor) / cols.len()).max(1);
        let rows_per_stmt = if flavor == Flavor::Oracle {
            // PARITY: SQLAlchemy uses executemany on Oracle. Multi-row VALUES is unavailable
            // before 23c, and INSERT ALL evaluates an identity sequence only once per statement.
            1
        } else {
            chunk
                .map(|c| (c.max(1) as usize).min(max_rows_per_stmt))
                .unwrap_or(max_rows_per_stmt)
        };
        let col_names: Vec<String> = cols
            .iter()
            .map(|c| sql_item_name(c, flavor, None))
            .collect();
        let mut statements: Vec<(String, Vec<SqlParam>)> = Vec::new();
        let mut start = 0;
        while start < n {
            let end = (start + rows_per_stmt).min(n);
            let mut params = Vec::with_capacity((end - start) * cols.len());
            let mut groups = Vec::with_capacity(end - start);
            #[allow(clippy::needless_range_loop)]
            for i in start..end {
                let mut cells = Vec::with_capacity(cols.len());
                for (j, enc) in encodings.iter().enumerate() {
                    let param = encode_param(&columns[j][i], flavor, enc);
                    cells.push(typed_placeholder(
                        flavor,
                        params.len() + 1,
                        &param,
                        dtype.get(&cols[j]).map(String::as_str).unwrap_or(""),
                    ));
                    params.push(param);
                }
                groups.push(format!("({})", cells.join(", ")));
            }
            let sql = format!(
                "INSERT INTO {table_name} ({}) VALUES {}",
                col_names.join(", "),
                groups.join(", ")
            );
            statements.push((sql, params));
            start = end;
        }
        self.exec_insert_statements(statements, &table_name, identity_insert)
            .await
    }

    /// Run `to_sql`'s insert statements in one transaction, optionally between
    /// `SET IDENTITY_INSERT ... ON` and `OFF`.
    async fn exec_insert_statements(
        &self,
        mut statements: Vec<(String, Vec<SqlParam>)>,
        table_name: &str,
        identity_insert: bool,
    ) -> (bool, String) {
        if let Some((sql, _)) = statements.first() {
            self.log(sql);
        }
        if identity_insert {
            statements.insert(
                0,
                (format!("SET IDENTITY_INSERT {table_name} ON"), Vec::new()),
            );
            statements.push((format!("SET IDENTITY_INSERT {table_name} OFF"), Vec::new()));
        }
        match self.driver().exec_many_with(&statements).await {
            Ok(results) if results.iter().all(Option::is_some) => (true, String::new()),
            Ok(_) => (false, format!("Failed to insert rows into {table_name}.")),
            Err(e) => (false, e.to_string()),
        }
    }
}

/// `mssql_insert_json` (`_sql.py:1192`), the MSSQL bulk path: one
/// `INSERT ... SELECT ... FROM OPENJSON(@P1) WITH (...)` per chunk of rows, the rows bound as a
/// single JSON document. `None` when a frame column has no declared type (the row-batch
/// `INSERT` path handles it instead).
#[allow(clippy::too_many_arguments)]
fn mssql_insert_json_statements(
    table_name: &str,
    cols: &[String],
    columns: &[Vec<KValue>],
    encodings: &[ColumnEncoding],
    dtype: &IndexMap<String, String>,
    numeric_cols: &[String],
    n: usize,
    chunksize: Option<i64>,
) -> Option<Vec<(String, Vec<SqlParam>)>> {
    let flavor = Flavor::Mssql;
    // `cols_pd_types` -> `cols_db_types` (`_sql.py:885-897`).
    let cols_types: IndexMap<String, String> = dtype
        .iter()
        .map(|(col, typ)| {
            let mut pd = get_pd_type_from_db_type(typ, true);
            if cols.contains(col)
                && (typ.to_lowercase().contains("numeric") || numeric_cols.contains(col))
                && let Some((p, s)) =
                    numeric_precision_scale(typ).filter(|(p, s)| *p != 0 && *s != 0)
            {
                pd = format!("numeric[{p},{s}]");
            }
            (col.clone(), get_db_type_from_pd_type(&pd, Some(flavor)))
        })
        .collect();
    if cols.iter().any(|c| !cols_types.contains_key(c)) {
        return None;
    }
    let col_list = cols
        .iter()
        .map(|c| format!("[{c}]"))
        .collect::<Vec<_>>()
        .join(",\n    ");
    let with_clause = cols_types
        .iter()
        .map(|(c, t)| format!("[{c}] {t} '$.\"{c}\"'"))
        .collect::<Vec<_>>()
        .join(",\n    ");
    let sql = format!(
        "INSERT INTO {table_name} (\n    {col_list}\n)\nSELECT\n    {col_list}\nFROM OPENJSON({})\nWITH (\n    {with_clause}\n);",
        crate::driver::params::placeholder(flavor, 1)
    );
    let rows_per_chunk = chunksize
        .filter(|c| *c > 0)
        .map(|c| c as usize)
        .unwrap_or(n)
        .max(1);
    let mut statements = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + rows_per_chunk).min(n);
        let docs: Vec<serde_json::Value> = (start..end)
            .map(|i| {
                let mut doc = serde_json::Map::with_capacity(cols.len());
                for (j, enc) in encodings.iter().enumerate() {
                    doc.insert(
                        cols[j].clone(),
                        encode_json_cell(&columns[j][i], flavor, enc),
                    );
                }
                serde_json::Value::Object(doc)
            })
            .collect();
        let json = serde_json::to_string(&docs).ok()?;
        statements.push((sql.clone(), vec![SqlParam::Text(json)]));
        start = end;
    }
    Some(statements)
}
