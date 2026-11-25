//! Syncing frames into pipes (`_pipes.py`: `sync_pipe`, `create_pipe_table_from_df`,
//! `get_add_columns_queries`, `get_alter_columns_queries`, `get_to_sql_dtype`; `_sql.py`:
//! `to_sql` and `psql_insert_copy`), plus the essentials of `Pipe.sync()` (dtype enforcement,
//! autotime, capturing new special columns) for callers that hold raw rows.

mod postgres_patch;
mod schema;
pub mod snapshot;
mod to_sql;
mod upsert;
mod window_guard;
use upsert::last_row_per_key;

use std::time::Instant;

use indexmap::IndexMap;
use meerschaum_connector::{ConnectorResult, SyncOptions};
use meerschaum_core::dtypes::{are_dtypes_equal, get_pd_type_from_db_type, is_dtype_special};
use meerschaum_core::{Map, Pipe, SuccessTuple, SyncResult, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::detect::{get_numeric_cols, get_special_cols};
use meerschaum_kernel::diff::{
    BacktrackRequest, FilterExistingParams, backtrack_params, filter_existing,
};
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

fn native_binary_dtype(dtype: &str) -> bool {
    matches!(
        dtype.split('[').next(),
        Some("bytes" | "binary" | "large_binary" | "binary_view")
    )
}

/// A new SQLite BLOB column already accepts the binary frame. Applying the
/// legacy special-value conversion would cast its bytes to TEXT.
fn retain_special_conversions(
    special: &mut IndexMap<String, String>,
    frame_types: &IndexMap<String, String>,
    before: &IndexMap<String, String>,
    after: &IndexMap<String, String>,
) {
    special.retain(|column, inferred| {
        !(inferred == "bytes"
            && !before.contains_key(column)
            && after.get(column).is_some_and(|dtype| dtype == "BLOB")
            && frame_types
                .get(column)
                .is_some_and(|dtype| native_binary_dtype(dtype)))
    });
}

/// `_max_chunks_flavors` (`_sql.py:30`).
fn max_chunks(flavor: Flavor) -> Option<i64> {
    match flavor {
        Flavor::Sqlite | Flavor::Geopackage => Some(1000),
        _ => None,
    }
}

/// What a pipe sync adds to `to_sql` ([`SqlConnector::to_sql_inner`]); the default is plain
/// `to_sql`.
#[derive(Debug, Clone, Default)]
pub(crate) struct ToSqlExtras<'a> {
    /// Wrap the inserts in `SET IDENTITY_INSERT ... ON` / `OFF` (MSSQL).
    pub identity_insert: bool,
    /// The columns whose null-like strings are text (`null_marker_text_columns`).
    pub keep_null_markers: &'a [String],
    /// Statements run first, in the same transaction (a replacing sync's `DELETE`).
    pub prelude: Vec<String>,
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
    /// An engine-history staging table must retain the target's subsecond timestamps too.
    pub history_datetime_precision: bool,
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

/// The columns of the pipe whose null-like strings (`'None'`, `'NA'`, `''`, ...) are text:
/// declared text columns and text index columns
/// ([`meerschaum_core::dtypes::null_marker_text_columns`]).
pub(crate) fn null_marker_columns(sp: &SqlPipe, state: &PipeState) -> Vec<String> {
    let index: Vec<String> = columns_map(sp).into_values().collect();
    meerschaum_core::dtypes::null_marker_text_columns(
        state.explicit_dtypes.iter(),
        index.iter(),
        |c| state.dtypes.get(c).cloned(),
    )
}

pub(crate) use meerschaum_connector::replace::replace_result;

fn engine_history(sp: &SqlPipe) -> bool {
    sp.pipe().temporary
        && sp.pipe().connector_keys() == "mrsm"
        && matches!(
            sp.pipe().metric_key(),
            "engine_runs" | "engine_nodes" | "engine_sync_status"
        )
}

fn mysql_engine_history(sp: &SqlPipe, flavor: Flavor) -> bool {
    matches!(flavor, Flavor::Mysql | Flavor::Mariadb) && engine_history(sp)
}

fn history_db_type(sp: &SqlPipe, flavor: Flavor, dtype: &str) -> String {
    let db_type = get_db_type_from_pd_type(dtype, Some(flavor));
    if mysql_engine_history(sp, flavor) && db_type == "DATETIME" {
        "DATETIME(6)".into()
    } else {
        db_type
    }
}

/// `params` with the empty strings of the columns `literal` (see [`null_marker_columns`]) as
/// nulls (`''` → `None`, `'_'` → `'_None'`, list items too), for Oracle, which stores `''` as
/// `NULL`: there `''` keeps v4's meaning (`IS NULL`) on every column. `None` when nothing
/// changes.
pub(crate) fn oracle_empty_string_params(params: Option<&Map>, literal: &[String]) -> Option<Map> {
    let params = params?;
    let negation = meerschaum_core::static_config::negation_prefix();
    let fix = |v: &Value| -> Option<Value> {
        match v {
            Value::Str(s) if s.is_empty() => Some(Value::Null),
            Value::Str(s) if s == negation => Some(Value::Str(format!("{negation}None"))),
            _ => None,
        }
    };
    let mut changed = false;
    let out: Map = params
        .iter()
        .map(|(k, v)| {
            if !literal.contains(k) {
                return (k.clone(), v.clone());
            }
            let fixed = match v {
                Value::List(items) => {
                    let items: Vec<Value> = items
                        .iter()
                        .map(|i| match fix(i) {
                            Some(f) => {
                                changed = true;
                                f
                            }
                            None => i.clone(),
                        })
                        .collect();
                    Value::List(items)
                }
                other => match fix(other) {
                    Some(f) => {
                        changed = true;
                        f
                    }
                    None => other.clone(),
                },
            };
            (k.clone(), fixed)
        })
        .collect();
    changed.then_some(out)
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
        self.ensure_schema().await?;
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
        if let Some(window) = meerschaum_connector::mutation::admit_frame(pipe, &df, &opts)
            .await
            .map_err(be)?
        {
            self.preflight_window_frame(pipe, &df, &opts, &window)
                .await?;
        }
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
                // A `parameters` lock keeps the parameters as they are; the sync goes on.
                if !res.success && !meerschaum_connector::locks::refused_by_lock(&res) {
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
        if result.success
            && pipe.connector_keys() == "mrsm"
            && pipe.param("compress").is_some_and(|v| v.truthy())
        {
            let policy = self.apply_compression_policy(pipe).await;
            if !policy.success {
                tracing::warn!(
                    "Could not install the internal compression policy: {}",
                    policy.message
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
        if let Err(e) = self.ensure_schema().await {
            return SyncResult::new(false, e.to_string());
        }
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
            // Every sync into this instance converges here (CLI, jobs, engine, API, façade).
            if let Err(e) = self.ensure_schema().await {
                return SyncResult::new(false, e.to_string());
            }
            if let Some(refused) = self.protected_refusal(pipe, "sync") {
                return SyncResult::new(false, refused.message);
            }
            // Locks too: `sync` refuses every write, and a replacing sync deletes rows (`clear`).
            if let Some(refused) = self
                .lock_refusal(pipe, meerschaum_connector::locks::sync_op(opts.replace))
                .await
            {
                return SyncResult::new(false, refused.message);
            }
            let start = Instant::now();
            let mut result = match self.sync_pipe_inner(pipe, df, opts, internal).await {
                Ok(r) => r,
                Err(e) => SyncResult::new(false, e.to_string()),
            };
            if !result.success
                && let Ok(sp) = self.sql_pipe(pipe)
            {
                // The table may have changed elsewhere: forget all cached catalog & readiness
                // metadata so the next sync asks the database again.
                self.forget_catalog_table(&sp.target);
            }
            if result.success {
                self.check_declared_hypertable(pipe, &mut result.message)
                    .await;
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
        let sp0 = self.sql_pipe(pipe)?;
        let window = meerschaum_connector::mutation::admit_frame(pipe, &df, opts)
            .await
            .map_err(be)?;
        // Admission may re-enter Main's graph planner. Enter the larger SQL writer
        // after authorization returns, so its poll frame stays off that call stack.
        Box::pin(self.write_admitted_frame(pipe, sp0, df, opts, internal, window)).await
    }

    async fn write_admitted_frame(
        &self,
        pipe: &mut Pipe,
        sp0: SqlPipe,
        df: Frame,
        opts: &SyncOptions,
        internal: InternalSync,
        window: Option<meerschaum_connector::mutation::DataWindow>,
    ) -> ConnectorResult<SyncResult> {
        let flavor = self.flavor;
        if let Some(window) = &window {
            if matches!(flavor, Flavor::Mysql | Flavor::Mariadb) {
                let append = self.innodb_append_plan(pipe, &df, opts, window)?;
                return self.execute_innodb_append(append).await;
            }
            self.preflight_window_frame(pipe, &df, opts, window).await?;
            if df.height() == 0 && df.width() == 0 && !opts.replace {
                if meerschaum_connector::mutation::current()
                    .is_some_and(|check| check.native_batch().is_some())
                {
                    // A captured empty batch still needs a receipt for an actual,
                    // guarded native commit, including when the table is absent.
                    let state = self.pipe_state(&sp0).await;
                    self.exec_window_batch(window_guard::WindowBatch {
                        pipe: &sp0,
                        state: &state,
                        window,
                        unseen: &df,
                        update: None,
                        options: opts,
                        upsert: false,
                        add_columns: None,
                    })
                    .await?;
                }
                return Ok(SyncResult::new(
                    true,
                    "Inserted 0, updated 0 rows; the admitted empty batch made no target changes.",
                ));
            }
        }
        let schema = self.get_pipe_schema(&sp0);
        if let Some(message) = internal_key_length_error(&sp0, flavor, &df) {
            return Ok(SyncResult::new(false, message));
        }

        if !pipe.temporary && self.get_pipe_id(pipe).await?.is_none() {
            if window.is_some() {
                return Err(be(
                    "The Main-admitted window requires its original registered target; registration cannot be recreated by a data batch.",
                ));
            }
            let reg = self.register_pipe(pipe).await;
            // A concurrent first sync of the same pipe may have registered it in between.
            if !reg.success && self.get_pipe_id(pipe).await?.is_none() {
                return Ok(reg.into());
            }
        }

        let mut check_existing = opts.check_existing;
        let internal_table = pipe.temporary && pipe.connector_keys() == "mrsm";
        let cache_internal = internal_table && sp0.is_static();
        let precise_history = mysql_engine_history(&sp0, flavor);
        let ready = (internal_table || precise_history)
            .then(|| {
                self.catalog()
                    .internal_ready(&sp0.target, schema.as_deref())
            })
            .flatten();
        // Non-static node writers still refresh their schema, but successful readiness also
        // marks their timestamp precision so warm progress updates need no extra catalog read.
        let precision_ready = precise_history && ready.is_some();
        // A sync that does not finish leaves no columns types behind.
        let synced_key = super::catalog::synced_key(pipe);
        self.catalog().forget_synced(&synced_key);
        let mut cached =
            internal
                .cached_state
                .clone()
                .or(if internal_table { ready } else { None });
        // A non-static internal table (engine nodes and cursors: newer releases add progress
        // and dependency columns) is ready as this process left it only while the rows fit its
        // layout: rows that need a column added or altered take the cold path below (the lock
        // and a fresh look at the table) instead.
        if internal_table
            && !sp0.is_static()
            && internal.cached_state.is_none()
            && let Some(ready) = &cached
        {
            let incoming_types = self.df_cols_types_refined(&df);
            let alter = AlterColumnsInput {
                df_cols_types: df.dtypes(),
                numeric_cols: get_numeric_cols(&df),
                db_cols_types: ready.cols_types.clone(),
                pipe_dtypes: ready.dtypes.clone(),
                explicit_dtypes: ready.explicit_dtypes.clone(),
                new_special_cols: internal.new_special_cols.clone().unwrap_or_default(),
                session_id: String::new(),
            };
            if !self
                .add_columns_queries_for(&sp0, &incoming_types, &ready.cols_types)
                .is_empty()
                || !self.alter_columns_queries_for(&sp0, &alter).0.is_empty()
            {
                cached = None;
            }
        }
        // Table existence alone does not mean an upsert is ready: another process may
        // still be creating its unique constraint. Inspect metadata only after taking
        // the database lock, and retain it through index creation and the first write.
        // Warm internal writes use the cached readiness and retain their one-DML cost.
        let _initialization_guard = if internal_table && cached.is_none() {
            self.driver
                .lock_internal_initialization(&sp0.target, schema.as_deref())
                .await
                .map_err(be)?
        } else {
            None
        };
        let initializing_internal = internal_table && cached.is_none();
        let mut state = match cached {
            Some(state) => state,
            None => self.pipe_state(&sp0).await,
        };
        if let Some(window) = &window
            && !state.exists
        {
            // The first target is created by the same bounded native writer that
            // inserts its admitted rows and persists its actual commit intent.
            return self
                .exec_window_create(&sp0, &state, &df, opts, window)
                .await;
        }
        if window.is_some() && (initializing_internal || precise_history) {
            return Err(be(
                "The native window target is no longer prepared; initialization and history upgrades require a separate Main-admitted schema transition.",
            ));
        }
        if precise_history && state.exists && !precision_ready {
            self.upgrade_history_datetime_precision(&sp0).await?;
        }
        let exists = state.exists;
        let is_new = !exists;
        let mut add_columns = None;
        if is_new {
            check_existing = false;
        } else {
            let incoming_types = self.df_cols_types_refined(&df);
            let mut special_conversions = internal.new_special_cols.clone().unwrap_or_default();
            let add_cols_queries =
                self.add_columns_queries_for(&sp0, &incoming_types, &state.cols_types);
            if window.is_some() && !add_cols_queries.is_empty() {
                add_columns = Some(window_guard::AddColumns::new(
                    self,
                    &sp0,
                    &state,
                    &incoming_types,
                )?);
            } else if !add_cols_queries.is_empty() {
                let added = self.exec_queries_all(&add_cols_queries, false, true).await;
                let before = state.cols_types;
                state = self.pipe_state(&sp0).await;
                if flavor == Flavor::Sqlite && !special_conversions.is_empty() {
                    retain_special_conversions(
                        &mut special_conversions,
                        &incoming_types,
                        &before,
                        &state.cols_types,
                    );
                }
                if !added {
                    // A concurrent upgrader may have added the same declared columns. Ignore
                    // that race only after fresh metadata proves every planned addition exists.
                    if internal_table
                        && !self
                            .add_columns_queries_for(&sp0, &incoming_types, &state.cols_types)
                            .is_empty()
                    {
                        return Err(be(format!("Failed to extend internal schema for {pipe}.")));
                    }
                    if !internal_table {
                        tracing::warn!("Failed to add new columns to {pipe}.");
                    }
                }
            }
            let alter_cols_queries = if window.is_some() {
                // The ordinary helper may persist numeric dtypes even before executing
                // its returned DDL. Compute its plan purely and refuse before calling it.
                let mut input = AlterColumnsInput {
                    df_cols_types: df.dtypes(),
                    numeric_cols: get_numeric_cols(&df),
                    db_cols_types: state.cols_types.clone(),
                    pipe_dtypes: state.dtypes.clone(),
                    explicit_dtypes: state.explicit_dtypes.clone(),
                    new_special_cols: special_conversions,
                    session_id: "window-read-only-plan".into(),
                };
                if let Some(add) = &add_columns {
                    add.project_alter(&mut input, &incoming_types);
                }
                let (queries, new_dtypes) = self.alter_columns_queries_for(&sp0, &input);
                if !queries.is_empty()
                    || new_dtypes
                        .as_ref()
                        .is_some_and(|dtypes| dtypes != &state.explicit_dtypes)
                {
                    return Err(be(
                        "This native window needs an explicit Main schema/inference transition; no legacy dtype mutation was admitted.",
                    ));
                }
                Vec::new()
            } else {
                self.alter_columns_with_state(pipe, &df, Some(&special_conversions), &state)
                    .await
            };
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
        // Declared text columns and text index columns keep their null-like strings.
        let literal_cols = null_marker_columns(&sp, &state);
        // Oracle stores `''` as `NULL` (it has no empty strings): there `''` is a null in those
        // columns too, so that a re-sync matches the row it stored (the other null-like strings
        // stay text).
        let mut df = df;
        let oracle_opts;
        let opts = if flavor == Flavor::Oracle {
            df.null_empty_strings(&literal_cols);
            match oracle_empty_string_params(opts.params.as_ref(), &literal_cols) {
                Some(params) => {
                    oracle_opts = SyncOptions {
                        params: Some(params),
                        ..opts.clone()
                    };
                    &oracle_opts
                }
                None => opts,
            }
        } else {
            opts
        };

        // `replace`: the rows of the window `[begin, end)` (matching `params`) are deleted and
        // the frame's rows of that window written, in one transaction. Nothing is diffed.
        let replace = opts.replace;
        let mut replace_skipped = 0usize;
        let mut replace_delete: Vec<String> = Vec::new();
        if replace {
            check_existing = false;
            if sp.column("datetime").is_none() && (opts.begin.is_some() || opts.end.is_some()) {
                return Ok(SyncResult::new(
                    false,
                    meerschaum_connector::replace::no_datetime_window(pipe),
                ));
            }
            if exists {
                let query = meerschaum_connector::DataQuery {
                    begin: opts.begin.clone(),
                    end: opts.end.clone(),
                    params: opts.params.clone(),
                    ..Default::default()
                };
                replace_delete.push(self.replace_delete_query(&sp, &state, &query)?);
            }
        }

        let filter_start = std::time::Instant::now();
        let (mut unseen_df, mut update_df) = if replace {
            let (inside, skipped) = meerschaum_connector::replace::rows_in_window(
                sp.column("datetime").as_deref(),
                &df,
                opts,
                &literal_cols,
            )
            .map_err(be)?;
            replace_skipped = skipped;
            if is_new && inside.height() == 0 {
                // No table and no rows of the window: nothing to replace (the first sync of a
                // `fetch: {replace: true}` pipe whose fetch is empty succeeds like a merge).
                return Ok(replace_result(0, 0, skipped, true));
            }
            (inside, None)
        } else if check_existing {
            let (u, up, d) = self.filter_existing_frame(&sp, &state, df).await?;
            if upsert {
                (d.clear(), Some(d))
            } else {
                (u, Some(up))
            }
        } else if upsert {
            (df.clear(), Some(df))
        } else {
            (df, None)
        };
        crate::facade::trace("sql: filter existing", filter_start);
        meerschaum_core::dprint!(
            "Filtered {} in {:?}: {} unseen, {} updated rows (check_existing={check_existing}, upsert={upsert}).",
            sp.target,
            filter_start.elapsed(),
            unseen_df.height(),
            update_df.as_ref().map_or(0, |u| u.height())
        );

        let dt_col = sp.column("datetime");
        let primary_key = sp.column("primary");
        let autoincrement = sp.autoincrement()
            || (is_new
                && primary_key
                    .as_ref()
                    .is_some_and(|pk| !dtypes.contains_key(pk) && !unseen_df.contains(pk)));
        // PARITY: `autoincrement not in pipe.parameters` tests the boolean against the keys, so
        // the parameter is always (re)persisted when truthy (`_pipes.py:2075`).
        if autoincrement && window.is_none() {
            let mut patch = Map::new();
            patch.insert("autoincrement".into(), Value::Bool(true));
            pipe.update_parameters(patch);
            if !pipe.temporary {
                let res = self.edit_pipe(pipe, false).await;
                // Under a `parameters` lock the sync goes on without recording it.
                if !res.success && !meerschaum_connector::locks::refused_by_lock(&res) {
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
        if window.is_some() && (autoincrement_needs_reset || self.should_partition(&sp)) {
            return Err(be(
                "This native window requires a controlled sequence/partition transition; no target DML was admitted.",
            ));
        }
        if autoincrement && let Some(pk) = &primary_key {
            for f in [Some(&mut unseen_df), update_df.as_mut()]
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
            if window.is_some() {
                return Err(be(
                    "A window-bound data batch cannot create or replace its native target.",
                ));
            }
            let res = self
                .create_pipe_table_from_df(
                    &sp,
                    &unseen_df,
                    &state,
                    internal.history_datetime_precision,
                )
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
        let mut replaced_rows = 0u64;
        let mut window_update_count = None;
        let write_start = std::time::Instant::now();
        let extended_schema = add_columns.is_some();
        if let Some(window) = &window {
            let (deleted, updated) = self
                .exec_window_batch(window_guard::WindowBatch {
                    pipe: &sp,
                    state: &state,
                    window,
                    unseen: &unseen_df,
                    update: update_df.as_ref(),
                    options: opts,
                    upsert,
                    add_columns,
                })
                .await?;
            if extended_schema {
                self.forget_catalog_table(&sp.target);
                state = self.pipe_state(&sp).await;
            }
            replaced_rows = deleted.unwrap_or(0);
            window_update_count = Some(updated);
        }
        if window.is_none() && (unseen_df.height() > 0 || !replace_delete.is_empty()) {
            let dtype = self.get_to_sql_dtype(&sp, &unseen_df, &state, true);
            let identity_insert = flavor == Flavor::Mssql
                && autoincrement
                && primary_key
                    .as_ref()
                    .is_some_and(|pk| unseen_df.contains(pk));
            let (ok, m, deleted) = self
                .to_sql_inner(
                    &unseen_df,
                    &sp.target,
                    schema.as_deref(),
                    &dtype,
                    opts.chunksize,
                    &ToSqlExtras {
                        identity_insert,
                        keep_null_markers: &literal_cols,
                        prelude: std::mem::take(&mut replace_delete),
                    },
                )
                .await;
            success = ok;
            replaced_rows = deleted.unwrap_or(0);
            if !ok {
                msg = m;
            }
        }

        if (is_new || initializing_internal) && !internal.skip_check_indices {
            let cols_types = internal.columns_types.clone();
            if !self.create_indices_sp(&sp, None, None, cols_types).await {
                if internal_table {
                    return Ok(SyncResult::new(
                        false,
                        format!("Failed to initialize indices for {pipe}."),
                    ));
                }
                tracing::warn!("Failed to create indices for {pipe}. Continuing...");
            }
        }
        if initializing_internal && flavor.is_timescaledb() && dt_col.is_some() {
            self.upgrade_engine_hypertable_policy(&sp).await?;
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

        let mut update_count =
            window_update_count.unwrap_or(update_df.as_ref().map_or(0, |u| u.height()) as u64);
        if window.is_none()
            && let Some(update) = update_df.as_ref().filter(|u| u.height() > 0)
        {
            let (ok, m, written) = self
                .apply_update(&sp, &state, update, upsert, autoincrement)
                .await?;
            update_count = written;
            if !ok {
                success = false;
                msg = format!("{msg}\n{m}").trim_start().to_string();
            }
        }

        meerschaum_core::dprint!(
            "Wrote {} in {:?} (success={success}).",
            sp.target,
            write_start.elapsed()
        );
        if !success {
            return Ok(SyncResult::new(false, msg));
        }
        let unseen_count = unseen_df.height() as u64;
        if replace {
            if window.is_none() && !internal.skip_temporary_tables_check && !cache_internal {
                let (ok, m) = self.drop_old_temporary_tables(false).await;
                if !ok {
                    tracing::warn!("{m}");
                }
            }
            if !is_new && internal.columns_types.is_none() {
                self.catalog().put_synced(
                    &synced_key,
                    &sp.target,
                    schema.as_deref(),
                    &state.cols_types,
                );
            }
            return Ok(replace_result(
                replaced_rows,
                unseen_count,
                replace_skipped,
                true,
            ));
        }
        let message = if !upsert {
            format!(
                "Inserted {}, updated {} rows.",
                meerschaum_core::pyfmt::thousands(unseen_count as i64),
                meerschaum_core::pyfmt::thousands(update_count as i64)
            )
        } else {
            format!(
                "Upserted {} row{}.",
                meerschaum_core::pyfmt::thousands(update_count as i64),
                if update_count != 1 { "s" } else { "" }
            )
        };
        if window.is_none() && !internal.skip_temporary_tables_check && !cache_internal {
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
        if !is_new && !cache_internal && internal.columns_types.is_none() {
            // The layout the sync worked against (reflected again after any `ADD COLUMN` /
            // `ALTER COLUMN` it ran; inserts and updates do not change it).
            self.catalog().put_synced(
                &synced_key,
                &sp.target,
                schema.as_deref(),
                &state.cols_types,
            );
        }
        // Internal tables (static or not) are ready for this process once a write succeeded:
        // later writes skip the initialization lock and the catalog.
        if internal_table || precise_history {
            if is_new {
                state = self.pipe_state(&sp).await;
            }
            self.catalog()
                .put_internal_ready(&sp.target, schema.as_deref(), &state);
        }
        let _ = dt_col;
        Ok(result)
    }

    /// Reconcile internal engine hypertables on TimescaleDB once per connection:
    /// - rc17 `engine_metrics` omitted the integer axis unit (`43200` µs chunks).
    /// - 5.0 internal engine tables (`engine_metrics`, `engine_events`, `engine_nodes`,
    ///   `engine_runs`, `engine_ledger`) used 30-day chunks and 30-day `compress_after`,
    ///   leaving history uncompressed until retention deleted it.
    ///
    /// Only known legacy Meerschaum defaults (`1..=43200` µs or 30 days) are migrated to
    /// the current chunk interval and 7-day compression window; custom admin intervals and
    /// retained history stay untouched.
    async fn upgrade_engine_hypertable_policy(&self, sp: &SqlPipe) -> ConnectorResult<()> {
        let Some(dt_col) = sp.column("datetime") else {
            return Ok(());
        };
        let expected = sp.chunk_interval().map_err(be)?;
        let schema = self.get_pipe_schema(sp);
        let schema = schema.as_deref().unwrap_or("public");
        let schema_lit = schema.replace('\'', "''");
        let table_lit = sp.target.replace('\'', "''");
        let dt_lit = dt_col.replace('\'', "''");
        let table = sql_item_name(&sp.target, self.flavor, Some(schema));
        let table_reg = table.replace('\'', "''");

        const LEGACY_MONTH_DAYS: i64 = 30;
        const LEGACY_MONTH_SECONDS: i64 = LEGACY_MONTH_DAYS * 86_400;
        const LEGACY_MONTH_US: i64 = LEGACY_MONTH_SECONDS * 1_000_000;

        match expected {
            ChunkInterval::Int(expected_int) => {
                let query = format!(
                    "SELECT integer_interval FROM timescaledb_information.dimensions \
                     WHERE hypertable_schema = '{schema_lit}' AND hypertable_name = '{table_lit}' \
                     AND column_name = '{dt_lit}'"
                );
                let current = self.driver.value(&query).await.map_err(be)?;
                let Some(current_val) = current else {
                    return Ok(());
                };
                if current_val.as_i64().is_some_and(|n| {
                    n != expected_int && ((1..=43_200).contains(&n) || n == LEGACY_MONTH_US)
                }) {
                    let query = format!(
                        "SELECT public.set_chunk_time_interval('{table_reg}', {expected_int})"
                    );
                    self.driver.exec(&query).await.map_err(be)?;
                }
                if sp.pipe().param("compress").is_some_and(|v| v.truthy()) {
                    let has_policy = self
                        .driver
                        .value(&format!(
                            "SELECT 1 FROM timescaledb_information.jobs \
                             WHERE hypertable_schema = '{schema_lit}' AND hypertable_name = '{table_lit}' \
                             AND proc_name = 'policy_compression' LIMIT 1"
                        ))
                        .await
                        .map_err(be)?
                        .is_some();
                    if has_policy {
                        let expected_after = crate::queries::compress::get_integer_after(
                            sp,
                            sp.pipe().param("compress").and_then(|v| v.get("after")),
                        );
                        let query = format!(
                            "SELECT public.alter_job(job_id, config => jsonb_set(config, '{{compress_after}}', \
                             to_jsonb({expected_after}::bigint))) FROM timescaledb_information.jobs \
                             WHERE hypertable_schema = '{schema_lit}' AND hypertable_name = '{table_lit}' \
                             AND proc_name = 'policy_compression' \
                             AND CASE WHEN jsonb_typeof(config->'compress_after') = 'number' \
                             THEN ((config->>'compress_after')::numeric BETWEEN 1 AND 43200 \
                             OR (config->>'compress_after')::numeric = {LEGACY_MONTH_US}) \
                             AND (config->>'compress_after')::numeric <> {expected_after} \
                             ELSE false END"
                        );
                        self.driver.exec(&query).await.map_err(be)?;
                    } else {
                        let res = self.apply_compression_policy(sp.pipe()).await;
                        if !res.success {
                            tracing::debug!(
                                "Could not install compression policy on {}: {}",
                                sp.pipe(),
                                res.message
                            );
                        }
                    }
                }
            }
            ChunkInterval::Delta(delta) => {
                let expected_minutes = delta.num_minutes();
                let expected_seconds = delta.num_seconds();
                if expected_minutes <= 0 {
                    return Ok(());
                }
                let query = format!(
                    "SELECT EXTRACT(EPOCH FROM time_interval)::bigint \
                     FROM timescaledb_information.dimensions \
                     WHERE hypertable_schema = '{schema_lit}' AND hypertable_name = '{table_lit}' \
                     AND column_name = '{dt_lit}'"
                );
                let current = self.driver.value(&query).await.map_err(be)?;
                let Some(current_val) = current else {
                    return Ok(());
                };
                if current_val
                    .as_i64()
                    .is_some_and(|secs| secs == LEGACY_MONTH_SECONDS && secs != expected_seconds)
                {
                    let query = format!(
                        "SELECT public.set_chunk_time_interval('{table_reg}', INTERVAL '{expected_minutes} MINUTES')"
                    );
                    self.driver.exec(&query).await.map_err(be)?;
                }
                if sp.pipe().param("compress").is_some_and(|v| v.truthy()) {
                    let has_policy = self
                        .driver
                        .value(&format!(
                            "SELECT 1 FROM timescaledb_information.jobs \
                             WHERE hypertable_schema = '{schema_lit}' AND hypertable_name = '{table_lit}' \
                             AND proc_name = 'policy_compression' LIMIT 1"
                        ))
                        .await
                        .map_err(be)?
                        .is_some();
                    if has_policy {
                        let compress_after_str = sp
                            .pipe()
                            .param("compress")
                            .and_then(|v| v.get("after"))
                            .filter(|v| v.truthy())
                            .map(|v| v.py_str())
                            .unwrap_or_else(|| format!("{expected_minutes} minutes"));
                        let after_lit = compress_after_str.replace('\'', "''");
                        let query = format!(
                            "SELECT public.alter_job(job_id, config => jsonb_set(config, '{{compress_after}}', \
                             to_jsonb('{after_lit}'::text))) FROM timescaledb_information.jobs \
                             WHERE hypertable_schema = '{schema_lit}' AND hypertable_name = '{table_lit}' \
                             AND proc_name = 'policy_compression' \
                             AND CASE WHEN jsonb_typeof(config->'compress_after') = 'string' \
                             THEN (config->>'compress_after')::interval = INTERVAL '{LEGACY_MONTH_DAYS} days' \
                             AND (config->>'compress_after')::interval <> INTERVAL '{after_lit}' \
                             ELSE false END"
                        );
                        self.driver.exec(&query).await.map_err(be)?;
                    } else {
                        let res = self.apply_compression_policy(sp.pipe()).await;
                        if !res.success {
                            tracing::debug!(
                                "Could not install compression policy on {}: {}",
                                sp.pipe(),
                                res.message
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Old internal MySQL/MariaDB history used DATETIME(0), rounding subsecond runs and
    /// moving them across time filters. Upgrade only these managed columns; existing whole-
    /// second values stay unchanged, and the public v4 dtype mapping remains DATETIME.
    async fn upgrade_history_datetime_precision(&self, sp: &SqlPipe) -> ConnectorResult<()> {
        let schema = self.get_pipe_schema(sp);
        let query = "SELECT COLUMN_NAME AS `column`, IS_NULLABLE AS `nullable` \
            FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = COALESCE(?, DATABASE()) \
            AND TABLE_NAME = ? AND DATA_TYPE = 'datetime' AND DATETIME_PRECISION < 6";
        let frame = self
            .read_params_catalog(
                query,
                &[
                    schema.map(SqlParam::Text).unwrap_or(SqlParam::Null),
                    SqlParam::Text(sp.target.clone()),
                ],
            )
            .await
            .map_err(be)?;
        let configured = super::data::configured_dtypes(sp);
        let mut changes = Vec::new();
        for row in meerschaum_connector::pipe_ops::frame_to_docs(&frame) {
            let Some(column) = row.get("column").map(KValue::py_str) else {
                continue;
            };
            if !configured
                .get(&column)
                .is_some_and(|dtype| history_db_type(sp, self.flavor, dtype) == "DATETIME(6)")
            {
                continue;
            }
            let nullable = if row.get("nullable").map(KValue::py_str).as_deref() == Some("YES") {
                "NULL"
            } else {
                "NOT NULL"
            };
            changes.push(format!(
                "MODIFY COLUMN {} DATETIME(6) {nullable}",
                sql_item_name(&column, self.flavor, None)
            ));
        }
        if !changes.is_empty() {
            let query = format!(
                "ALTER TABLE {} {}",
                self.pipe_table_name(sp),
                changes.join(", ")
            );
            self.log(&query);
            self.driver.exec(&query).await.map_err(be)?;
        }
        Ok(())
    }
}
