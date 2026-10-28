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

/// The columns of the pipe whose null-like strings (`'None'`, `'NA'`, `''`, ...) are text
/// (rc15 Q18): declared text columns and text index columns
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
    let negation = crate::where_clause::negation_prefix();
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

/// The most pipes whose post-sync columns types are held at once (consumed by the first
/// [`take_synced_columns_types`](SqlConnector::take_synced_columns_types); a caller that never
/// takes them, such as the CLI, must not grow the map without bound).
const MAX_SYNCED_COLUMNS: usize = 512;

/// The key of a pipe's post-sync columns types: its keys, which a caller has without resolving
/// the pipe's parameters.
fn synced_columns_key(pipe: &Pipe) -> String {
    format!(
        "{:?}",
        (
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key()
        )
    )
}

impl SqlConnector {
    fn forget_synced_columns(&self, key: &str) {
        self.state.lock().expect("state").synced_columns.remove(key);
    }

    fn record_synced_columns(&self, key: &str, cols_types: &IndexMap<String, String>) {
        if cols_types.is_empty() {
            return;
        }
        let mut state = self.state.lock().expect("state");
        if state.synced_columns.len() >= MAX_SYNCED_COLUMNS {
            state.synced_columns.clear();
        }
        state
            .synced_columns
            .insert(key.to_string(), cols_types.clone());
    }

    /// The columns types `{column: DB TYPE}` of the pipe's table as the last successful
    /// `sync_pipe` of this connector left them, handed out once (`None` when that sync created
    /// the table, did not finish, or was already taken).
    ///
    /// A sync reflects the table's columns before it writes, and again after any DDL of its own,
    /// so what it holds at the end is the table's layout. The Python façade adopts it as the
    /// pipe's cached `get_columns_types()` instead of reflecting the table a second time on the
    /// next read.
    pub fn take_synced_columns_types(&self, pipe: &Pipe) -> Option<IndexMap<String, String>> {
        self.state
            .lock()
            .expect("state")
            .synced_columns
            .remove(&synced_columns_key(pipe))
    }
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
            // Every sync into this instance converges here (CLI, jobs, engine, API, façade).
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
            // A concurrent first sync of the same pipe may have registered it in between.
            if !reg.success && self.get_pipe_id(pipe).await?.is_none() {
                return Ok(reg.into());
            }
        }

        let mut check_existing = opts.check_existing;
        let internal_table = pipe.temporary && pipe.connector_keys() == "mrsm";
        let cache_internal = internal_table && sp0.is_static();
        let cache_key = self.pipe_table_name(&sp0);
        let precise_history = mysql_engine_history(&sp0, flavor);
        // Non-static node writers still refresh their schema, but successful readiness also
        // marks their timestamp precision so warm progress updates need no extra catalog read.
        let precision_ready = precise_history
            && self
                .state
                .lock()
                .expect("state")
                .internal_pipe_states
                .contains_key(&cache_key);
        // A sync that does not finish leaves no columns types behind.
        let synced_key = synced_columns_key(pipe);
        self.forget_synced_columns(&synced_key);
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
        if precise_history && state.exists && !precision_ready {
            self.upgrade_history_datetime_precision(&sp0).await?;
        }
        let exists = state.exists;
        let is_new = !exists;
        if is_new {
            check_existing = false;
        } else {
            let incoming_types = Self::df_cols_types_refined(&df);
            let add_cols_queries =
                self.add_columns_queries_for(&sp0, &incoming_types, &state.cols_types);
            if !add_cols_queries.is_empty() {
                let added = self.exec_queries_all(&add_cols_queries, false, true).await;
                state = self.pipe_state(&sp0).await;
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
        let (mut unseen_df, mut update_df, mut delta_df) = if replace {
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
            (inside.clone(), None, inside)
        } else if check_existing {
            let (u, up, d) = self.filter_existing_frame(&sp, &state, df).await?;
            (u, Some(up), d)
        } else {
            (df.clone(), None, df)
        };
        crate::facade::trace("sql: filter existing", filter_start);
        meerschaum_core::dprint!(
            "Filtered {} in {:?}: {} unseen, {} updated rows (check_existing={check_existing}, upsert={upsert}).",
            sp.target,
            filter_start.elapsed(),
            unseen_df.height(),
            update_df.as_ref().map_or(0, |u| u.height())
        );
        if upsert && !replace {
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
        let write_start = std::time::Instant::now();
        if unseen_df.height() > 0 || !replace_delete.is_empty() {
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

        meerschaum_core::dprint!(
            "Wrote {} in {:?} (success={success}).",
            sp.target,
            write_start.elapsed()
        );
        if !success {
            return Ok(SyncResult::new(false, msg));
        }
        let unseen_count = unseen_df.height() as u64;
        let update_count = update_df.as_ref().map_or(0, |u| u.height()) as u64;
        if replace {
            if !internal.skip_temporary_tables_check && !cache_internal {
                let (ok, m) = self.drop_old_temporary_tables(false).await;
                if !ok {
                    tracing::warn!("{m}");
                }
            }
            if !is_new && internal.columns_types.is_none() {
                self.record_synced_columns(&synced_key, &state.cols_types);
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
        if !is_new && !cache_internal && internal.columns_types.is_none() {
            // The layout the sync worked against (reflected again after any `ADD COLUMN` /
            // `ALTER COLUMN` it ran; inserts and updates do not change it).
            self.record_synced_columns(&synced_key, &state.cols_types);
        }
        if cache_internal || precise_history {
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

    /// The `DELETE` of a replacing sync's window (`[begin, end)` matching `params`): the
    /// `clear_pipe` query, except on SQLite, whose `datetime(...)` bounds drop fractions of a
    /// second (a window ending just after `00:00:00.5` would end at `00:00:00`): there the
    /// datetime bounds are the exact canonical text the rows are stored as.
    pub(crate) fn replace_delete_query(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &meerschaum_connector::DataQuery,
    ) -> ConnectorResult<String> {
        Ok(format!(
            "DELETE FROM {}\n{}",
            self.pipe_table_name(sp),
            self.replace_window_where(sp, state, query)?
        ))
    }

    /// The `WHERE` clause of a replacing sync's window ([`replace_delete_query`]'s): the
    /// in-place sync applies it to the staged rows too, so that it inserts exactly the rows of
    /// the window it deletes. Its column references are unqualified.
    ///
    /// [`replace_delete_query`]: Self::replace_delete_query
    pub(crate) fn replace_window_where(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &meerschaum_connector::DataQuery,
    ) -> ConnectorResult<String> {
        let delete = self.replace_window_delete(sp, state, query)?;
        let prefix = format!("DELETE FROM {}\n", self.pipe_table_name(sp));
        delete
            .strip_prefix(&prefix)
            .map(str::to_string)
            .ok_or_else(|| {
                be(format!(
                    "Unexpected clear query for {}:\n{delete}",
                    sp.pipe()
                ))
            })
    }

    fn replace_window_delete(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &meerschaum_connector::DataQuery,
    ) -> ConnectorResult<String> {
        let flavor = self.flavor;
        let dt_col = sp.column("datetime");
        let text_datetimes = matches!(flavor, Flavor::Sqlite | Flavor::Geopackage)
            && dt_col.as_ref().is_some_and(|c| {
                state
                    .dtypes
                    .get(c)
                    .is_none_or(|t| !are_dtypes_equal(t, "int"))
            })
            && [&query.begin, &query.end]
                .iter()
                .any(|b| matches!(b, Some(meerschaum_connector::DateBound::Datetime(_))));
        if !text_datetimes {
            return self.get_clear_pipe_query(sp, state, query);
        }
        let without_bounds = meerschaum_connector::DataQuery {
            begin: None,
            end: None,
            ..query.clone()
        };
        let mut sql = self.get_clear_pipe_query(sp, state, &without_bounds)?;
        let dt = sql_item_name(&dt_col.unwrap_or_default(), flavor, None);
        let literal = |b: &meerschaum_connector::DateBound| match b {
            meerschaum_connector::DateBound::Datetime(d) => {
                format!("'{}'", d.utc.format("%Y-%m-%d %H:%M:%S%.6f"))
            }
            meerschaum_connector::DateBound::Int(i) => i.to_string(),
        };
        if let Some(b) = &query.begin {
            sql.push_str(&format!("\n    AND {dt} >= {}", literal(b)));
        }
        if let Some(e) = &query.end {
            sql.push_str(&format!("\n    AND {dt} <  {}", literal(e)));
        }
        Ok(sql)
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
            // Declared text / index columns keep their null-like strings (rc15 Q18).
            literal_null_markers: null_marker_columns(sp, state),
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
        let literal_cols = null_marker_columns(sp, state);
        let encoding = |i: usize| ColumnEncoding {
            name: cols[i].clone(),
            numeric: state
                .cols_types
                .get(&cols[i])
                .and_then(|t| numeric_precision_scale(t))
                .map(|(p, s)| (Some(p), Some(s))),
            keep_null_markers: literal_cols.contains(&cols[i]),
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

    /// The `ON CONFLICT (...)` target of an upsert into the pipe's table (PostgreSQL family,
    /// SQLite, GeoPackage): the key columns and expressions of the table's unique index over
    /// `join_cols`, exactly as the database lists them, so the conflict target always matches
    /// the index (v4's plain unique constraint, one declared `NULLS NOT DISTINCT`, or an older
    /// table's `COALESCE(...)` index), and whether that key treats NULL keys as equal (else the
    /// upsert compares NULL keys itself: `UpdateQueriesArgs::null_safe_upsert`). `None` (render
    /// the join columns, as v4) when no unique index covers exactly `join_cols` or on other
    /// flavors.
    pub(crate) async fn upsert_conflict_target(
        &self,
        sp: &SqlPipe,
        join_cols: &[String],
    ) -> Option<(String, bool)> {
        if join_cols.is_empty() {
            return None;
        }
        let flavor = self.flavor;
        let table = truncate_item_name(&sp.target, flavor);
        // (index name, key expressions in order)
        let mut indices: Vec<(String, Vec<String>)> = Vec::new();
        // The indices declared `NULLS NOT DISTINCT` (PostgreSQL 15+).
        let mut nulls_not_distinct: Vec<String> = Vec::new();
        match flavor.driver_family() {
            DriverFamily::Postgres => {
                let schema = self.get_pipe_schema(sp);
                let nnd = if self.supports_nulls_not_distinct().await {
                    "ix.indnullsnotdistinct"
                } else {
                    "false"
                };
                let sql = format!(
                    "SELECT i.relname AS \"index\", k.k AS \"position\", \
                    pg_catalog.pg_get_indexdef(ix.indexrelid, k.k, true) AS \"expr\", \
                    {nnd} AS \"nnd\" \
                    FROM pg_catalog.pg_index ix \
                    JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid \
                    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid \
                    JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace \
                    CROSS JOIN LATERAL generate_series(1, ix.indnkeyatts::int) AS k(k) \
                    WHERE ix.indisunique AND ix.indpred IS NULL AND t.relname = $1 \
                    AND n.nspname = COALESCE($2, current_schema()) \
                    ORDER BY i.relname, k.k"
                );
                let params = [
                    SqlParam::Text(table.clone()),
                    match schema {
                        Some(s) => SqlParam::Text(truncate_item_name(&s, flavor)),
                        None => SqlParam::Null,
                    },
                ];
                let df = self.driver().read_frame_with(&sql, &params).await.ok()?;
                for doc in super::frame_docs(&df) {
                    let (Some(name), Some(expr)) = (
                        super::kv_str(doc.get("index")),
                        super::kv_str(doc.get("expr")),
                    ) else {
                        continue;
                    };
                    if matches!(doc.get("nnd"), Some(KValue::Bool(true)))
                        && !nulls_not_distinct.contains(&name)
                    {
                        nulls_not_distinct.push(name.clone());
                    }
                    match indices.iter_mut().find(|(n, _)| *n == name) {
                        Some((_, exprs)) => exprs.push(expr),
                        None => indices.push((name, vec![expr])),
                    }
                }
            }
            DriverFamily::Sqlite => {
                let sql = "SELECT name, sql FROM sqlite_master WHERE type = 'index' \
                    AND tbl_name = ? AND sql IS NOT NULL";
                let df = self
                    .driver()
                    .read_frame_with(sql, &[SqlParam::Text(table.clone())])
                    .await
                    .ok()?;
                for doc in super::frame_docs(&df) {
                    let (Some(name), Some(sql)) = (
                        super::kv_str(doc.get("name")),
                        super::kv_str(doc.get("sql")),
                    ) else {
                        continue;
                    };
                    if !sql
                        .trim_start()
                        .to_uppercase()
                        .starts_with("CREATE UNIQUE INDEX")
                    {
                        continue;
                    }
                    if let Some(exprs) = sqlite_index_key_list(&sql) {
                        indices.push((name, exprs));
                    }
                }
            }
            _ => return None,
        }
        let wanted: std::collections::BTreeSet<String> =
            join_cols.iter().map(|c| c.to_lowercase()).collect();
        let matching: Vec<&(String, Vec<String>)> = indices
            .iter()
            .filter(|(_, exprs)| {
                let cols: Option<std::collections::BTreeSet<String>> = exprs
                    .iter()
                    .map(|e| key_expression_column(e).map(|c| c.to_lowercase()))
                    .collect();
                cols.is_some_and(|c| c == wanted && c.len() == exprs.len())
            })
            .collect();
        // Prefer the index that makes NULL keys equal.
        let null_safe = |(name, exprs): &&(String, Vec<String>)| {
            nulls_not_distinct.contains(name) || exprs.iter().any(|e| !is_plain_identifier(e))
        };
        let chosen = matching
            .iter()
            .find(|i| null_safe(i))
            .or_else(|| matching.first())?;
        Some((
            chosen
                .1
                .iter()
                .map(|e| {
                    if is_plain_identifier(e) || flavor.driver_family() == DriverFamily::Sqlite {
                        e.clone()
                    } else {
                        format!("({e})")
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
            null_safe(chosen),
        ))
    }

    /// Whether the server accepts `UNIQUE NULLS NOT DISTINCT` (PostgreSQL 15 and later, Citus
    /// aside): the NULL-safe unique key of an upsert pipe is then v4's own constraint
    /// ([`crate::queries::index::IndexState::nulls_not_distinct`]).
    pub(crate) async fn supports_nulls_not_distinct(&self) -> bool {
        if self.flavor.driver_family() != DriverFamily::Postgres || self.flavor == Flavor::Citus {
            return false;
        }
        self.db_version()
            .await
            .and_then(|v| postgres_major_version(&v))
            .is_some_and(|major| major >= 15)
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
        // NULL keys must be equal (rc15 S3). The conflict target follows the table's own unique
        // key; when that key treats NULLs as distinct (MySQL / MariaDB, SQLite, PostgreSQL before
        // 15, a table made before 5.0.0rc15) and the batch has a NULL key, the upsert compares
        // the keys itself (`UpdateQueriesArgs::null_safe_upsert`).
        let (conflict_target, key_null_safe) = if upsert {
            match self.upsert_conflict_target(sp, &join_cols).await {
                Some((target, null_safe)) => (Some(target), null_safe),
                None => (None, false),
            }
        } else {
            (None, false)
        };
        let null_safe_upsert = upsert
            && sp.null_indices()
            && pipe.connector_keys() != "mrsm"
            && match flavor {
                Flavor::Mysql | Flavor::Mariadb => true,
                Flavor::Postgresql
                | Flavor::Postgis
                | Flavor::Timescaledb
                | Flavor::TimescaledbHa
                | Flavor::Sqlite
                | Flavor::Geopackage => {
                    !key_null_safe
                        && join_cols
                            .iter()
                            .filter(|c| Some(*c) != dt_col.as_ref())
                            .any(|c| {
                                update_df
                                    .data(c)
                                    .map(|d| d.is_null_mask().iter().any(|n| *n))
                                    .unwrap_or(false)
                            })
                }
                _ => false,
            };
        // These statements need one row per key: a batch that repeats a key keeps its last row,
        // as `INSERT ... ON DUPLICATE KEY UPDATE` / `ON CONFLICT DO UPDATE` resolve it (NULL keys
        // are equal here).
        let deduplicated;
        let update_df = if null_safe_upsert {
            deduplicated = last_row_per_key(update_df, &join_cols);
            &deduplicated
        } else {
            update_df
        };
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
            .map(|(c, t)| (c.clone(), history_db_type(sp, flavor, t)))
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
                    history_datetime_precision: mysql_engine_history(sp, flavor),
                },
            )
            .await;
        if !temp_result.success {
            return Ok((false, temp_result.message));
        }

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
        if upsert {
            // Name the unique key's own columns / expressions; compare NULL keys in the
            // statements where that key does not.
            args.conflict_target = conflict_target;
            args.null_safe_upsert = null_safe_upsert;
        }
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
        history_datetime_precision: bool,
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
        let precise_history = matches!(flavor, Flavor::Mysql | Flavor::Mariadb)
            && (mysql_engine_history(sp, flavor) || history_datetime_precision);
        let physical_types = internal_key_types || precise_history;
        if physical_types {
            let keys = columns_map(sp);
            for (column, dtype) in &mut new_dtypes {
                let db_type = history_db_type(sp, flavor, dtype);
                *dtype = if precise_history && db_type == "DATETIME" {
                    "DATETIME(6)".into()
                } else if internal_key_types
                    && keys.values().any(|key| key == column)
                    && matches!(
                        db_type.as_str(),
                        "TEXT" | "NVARCHAR(MAX)" | "VARCHAR(MAX)" | "NVARCHAR2(2000)"
                    )
                {
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
                parse_dtypes: !physical_types,
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
                    && sp.pipe().connector_keys() == "mrsm"
                    && flavor == Flavor::Mssql
                    && !q.is_empty()
                {
                    // Different internal targets have different initialization locks but
                    // may create their shared schema together. Lock only while it is absent,
                    // using the existing create-table transaction, then check again. MSSQL's
                    // generic CREATE SCHEMA error 2759 cannot distinguish a duplicate from a
                    // permission failure, so no schema/DDL errors are suppressed.
                    use sha2::{Digest, Sha256};

                    let schema = truncate_item_name(s, flavor);
                    let name = schema.replace('\'', "''");
                    let create = format!("CREATE SCHEMA {}", sql_item_name(&schema, flavor, None))
                        .replace('\'', "''");
                    let mut digest = Sha256::new();
                    digest.update(b"meerschaum:internal-schema-initialization:");
                    digest.update(schema.to_lowercase().as_bytes());
                    let key = hex::encode(digest.finalize());
                    let limit = crate::driver::connect::connect_timeout(self.spec());
                    let timeout_ms = i32::try_from(limit.as_millis()).unwrap_or(i32::MAX);
                    q = vec![format!(
                        "IF SCHEMA_ID(N'{name}') IS NULL\n\
                         BEGIN\n\
                             DECLARE @mrsm_schema_lock int;\n\
                             EXEC @mrsm_schema_lock = sys.sp_getapplock \
                                 @Resource = N'{key}', @LockMode = 'Exclusive', \
                                 @LockOwner = 'Transaction', @LockTimeout = {timeout_ms}, \
                                 @DbPrincipal = 'public';\n\
                             IF @mrsm_schema_lock < 0\n\
                                 THROW 50000, 'Could not acquire internal schema initialization lock.', 1;\n\
                             IF SCHEMA_ID(N'{name}') IS NULL\n\
                                 EXEC(N'{create}');\n\
                         END;"
                    )];
                }
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
        if !success && self.table_exists(&sp.target, schema.as_deref()).await {
            // Another connection created the table between our existence check and the
            // CREATE (two writers' first sync of a new pipe, e.g. the engine's state pipes):
            // the table is there, so the rows can be written to it.
            tracing::debug!("{} was created concurrently.", self.pipe_table_name(sp));
            return SuccessTuple::ok("Success");
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
        if !self.table_exists(&sp.target, schema.as_deref()).await
            || (sp.is_static() && !engine_history(sp))
        {
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
        if sp.is_static() && !engine_history(sp) {
            return Vec::new();
        }
        let flavor = self.flavor;
        let declared = sp.dtypes();
        let declared_history;
        let incoming = if engine_history(sp) {
            // An empty initializer has no incoming Arrow columns. Its declared schema still
            // has to be made ready, without fabricating values for existing legacy rows.
            let mut types = df_cols_types.clone();
            for (column, dtype) in &declared {
                types.insert(column.clone(), dtype.py_str());
            }
            declared_history = types;
            &declared_history
        } else {
            df_cols_types
        };
        let new_cols: Vec<(String, String)> = incoming
            .iter()
            .filter(|(c, t)| !db_cols_types.contains_key(*c) && !c.is_empty() && !t.is_empty())
            // Managed static history may gain new declared fields, such as selection or
            // registration provenance. Unrecognized incoming fields retain static semantics.
            .filter(|(column, _)| !sp.is_static() || declared.contains_key(*column))
            .map(|(c, t)| (c.clone(), history_db_type(sp, flavor, t)))
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
                // A `parameters` lock keeps the parameters as they are; the sync goes on.
                if !res.success && !meerschaum_connector::locks::refused_by_lock(&res) {
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
            .map(|(c, t)| (c, history_db_type(sp, self.flavor, &t)))
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
        let (ok, msg, _) = self
            .to_sql_inner(df, name, schema, dtype, chunksize, &ToSqlExtras::default())
            .await;
        (ok, msg)
    }

    /// [`Self::to_sql`] with what a pipe sync adds ([`ToSqlExtras`]): `SET IDENTITY_INSERT ...
    /// ON` / `OFF` on the same connection and transaction (MSSQL autoincrement pipes given
    /// explicit keys, `_pipes.py:2201`), the text columns that keep null-like strings, and
    /// statements run first in the same transaction (a replacing sync's `DELETE`). Returns
    /// `(success, error message, rows affected by the first prelude statement)`.
    pub(crate) async fn to_sql_inner(
        &self,
        df: &Frame,
        name: &str,
        schema: Option<&str>,
        dtype: &IndexMap<String, String>,
        chunksize: Option<i64>,
        extras: &ToSqlExtras<'_>,
    ) -> (bool, String, Option<u64>) {
        let identity_insert = extras.identity_insert;
        let flavor = self.flavor;
        let schema = schema.or(self.schema());
        let truncated = truncate_item_name(name, flavor);
        let cols = df.names();
        if df.height() == 0 || cols.is_empty() {
            if extras.prelude.is_empty() {
                return (true, String::new(), None);
            }
            // A replacing sync with no rows still clears its window.
            let statements: Vec<(String, Vec<SqlParam>)> = extras
                .prelude
                .iter()
                .map(|q| (q.clone(), Vec::new()))
                .collect();
            return match self.driver().exec_many_with(&statements).await {
                Ok(results) if results.iter().all(Option::is_some) => {
                    (true, String::new(), results.first().copied().flatten())
                }
                Ok(_) => (false, format!("Failed to replace rows of {name}."), None),
                Err(e) => (false, e.to_string(), None),
            };
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
                    keep_null_markers: extras.keep_null_markers.contains(c),
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
            if !extras.prelude.is_empty() {
                return match self
                    .driver()
                    .copy_in_after(&extras.prelude, &copy_sql, buf.into_bytes())
                    .await
                {
                    Ok((counts, _)) => (true, String::new(), counts.first().copied()),
                    Err(e) => {
                        tracing::warn!("Failed to insert into {table_name}:\n{e}");
                        (false, e.to_string(), None)
                    }
                };
            }
            return match self.driver().copy_in(&copy_sql, buf.into_bytes()).await {
                Ok(_) => (true, String::new(), None),
                Err(e) => {
                    tracing::warn!("Failed to insert into {table_name}:\n{e}");
                    (false, e.to_string(), None)
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
                .exec_insert_statements(statements, &table_name, identity_insert, &extras.prelude)
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
        self.exec_insert_statements(statements, &table_name, identity_insert, &extras.prelude)
            .await
    }

    /// Run `to_sql`'s insert statements in one transaction, optionally between
    /// `SET IDENTITY_INSERT ... ON` and `OFF`, after `prelude` (in the same transaction).
    async fn exec_insert_statements(
        &self,
        mut statements: Vec<(String, Vec<SqlParam>)>,
        table_name: &str,
        identity_insert: bool,
        prelude: &[String],
    ) -> (bool, String, Option<u64>) {
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
        for (i, q) in prelude.iter().enumerate() {
            self.log(q);
            statements.insert(i, (q.clone(), Vec::new()));
        }
        match self.driver().exec_many_with(&statements).await {
            Ok(results) if results.iter().all(Option::is_some) => (
                true,
                String::new(),
                if prelude.is_empty() {
                    None
                } else {
                    results.first().copied().flatten()
                },
            ),
            Ok(_) => (
                false,
                format!("Failed to insert rows into {table_name}."),
                None,
            ),
            Err(e) => (false, e.to_string(), None),
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

/// Whether a key expression is a bare (possibly quoted) column name.
/// The major version in PostgreSQL's `version()` text (`PostgreSQL 17.5 (Debian ...) on ...`),
/// or in a bare version (`15.4`).
pub(crate) fn postgres_major_version(version: &str) -> Option<u32> {
    let v = version.trim();
    let v = v.strip_prefix("PostgreSQL").unwrap_or(v).trim_start();
    let digits: String = v.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// The rows of `df` left when a key (`keys`, NULL equal to NULL) that repeats keeps only its
/// last row, in their original order: how `INSERT ... ON DUPLICATE KEY UPDATE` resolves a batch
/// that repeats a key (the last row wins). `df` itself when no key repeats or a key column is
/// missing.
pub(crate) fn last_row_per_key(df: &Frame, keys: &[String]) -> Frame {
    if df.height() < 2 || keys.is_empty() {
        return df.clone();
    }
    let Ok(columns) = keys
        .iter()
        .map(|k| df.data(k).map(|d| d.values()))
        .collect::<Result<Vec<Vec<KValue>>, _>>()
    else {
        return df.clone();
    };
    let mut last: std::collections::HashMap<Vec<KValue>, usize> =
        std::collections::HashMap::with_capacity(df.height());
    for i in 0..df.height() {
        last.insert(columns.iter().map(|c| c[i].clone()).collect(), i);
    }
    if last.len() == df.height() {
        return df.clone();
    }
    let mut keep: Vec<usize> = last.into_values().collect();
    keep.sort_unstable();
    df.take(&keep)
}

fn is_plain_identifier(expr: &str) -> bool {
    let e = expr.trim();
    if let Some(inner) = e.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        return !inner.is_empty() && !inner.replace("\"\"", "").contains('"');
    }
    if let Some(inner) = e.strip_prefix('`').and_then(|r| r.strip_suffix('`')) {
        return !inner.is_empty() && !inner.contains('`');
    }
    if let Some(inner) = e.strip_prefix('[').and_then(|r| r.strip_suffix(']')) {
        return !inner.is_empty() && !inner.contains(']');
    }
    !e.is_empty()
        && e.chars()
            .next()
            .is_some_and(|c| c.is_alphabetic() || c == '_')
        && e.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

/// A quoted or bare identifier's name.
fn unquote_identifier(expr: &str) -> String {
    let e = expr.trim();
    for (open, close) in [('"', '"'), ('`', '`'), ('[', ']')] {
        if let Some(inner) = e.strip_prefix(open).and_then(|r| r.strip_suffix(close)) {
            return inner.replace("\"\"", "\"");
        }
    }
    e.to_string()
}

/// The column a unique-index key expression is over: the column itself, or the first argument
/// of `COALESCE(col, ...)` (Meerschaum's NULL-safe key). `None` for any other expression.
fn key_expression_column(expr: &str) -> Option<String> {
    let e = expr.trim();
    if is_plain_identifier(e) {
        return Some(unquote_identifier(e));
    }
    let upper = e.to_uppercase();
    let rest = upper
        .strip_prefix("COALESCE")
        .map(|_| e["COALESCE".len()..].trim_start())?;
    let inner = rest.strip_prefix('(')?;
    let args = split_top_level(inner.strip_suffix(')').unwrap_or(inner));
    let first = args.first()?.trim();
    // `COALESCE(("id")::text, ...)` or a cast: take the identifier inside.
    let first = first.trim_start_matches('(');
    let first = first.split(')').next().unwrap_or(first);
    let first = first.split("::").next().unwrap_or(first).trim();
    is_plain_identifier(first).then(|| unquote_identifier(first))
}

/// Split on the commas outside parentheses and quotes.
fn split_top_level(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut cur = String::new();
    for c in s.chars() {
        match quote {
            Some(q) => {
                cur.push(c);
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' | '`' => {
                    quote = Some(c);
                    cur.push(c);
                }
                '(' => {
                    depth += 1;
                    cur.push(c);
                }
                ')' => {
                    depth -= 1;
                    cur.push(c);
                }
                ',' if depth == 0 => {
                    out.push(cur.trim().to_string());
                    cur.clear();
                }
                _ => cur.push(c),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// The key list of a SQLite `CREATE [UNIQUE] INDEX ... ON table (keys)` statement.
fn sqlite_index_key_list(sql: &str) -> Option<Vec<String>> {
    let upper = sql.to_uppercase();
    let on = upper.find(" ON ")?;
    let open = on + sql[on..].find('(')?;
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    for (i, c) in sql[open..].char_indices() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' | '`' => quote = Some(c),
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let keys = split_top_level(&sql[open + 1..open + i]);
                        return (!keys.is_empty()).then_some(keys);
                    }
                }
                _ => {}
            },
        }
    }
    None
}

#[cfg(test)]
mod conflict_target_tests {
    use super::*;

    #[test]
    fn key_expressions_name_their_columns() {
        assert_eq!(key_expression_column("ts").as_deref(), Some("ts"));
        assert_eq!(key_expression_column("\"Ts\"").as_deref(), Some("Ts"));
        assert_eq!(
            key_expression_column("COALESCE(id, '-987654321'::integer)").as_deref(),
            Some("id")
        );
        assert_eq!(
            key_expression_column("COALESCE(\"id\", -987654321) ").as_deref(),
            Some("id")
        );
        assert_eq!(
            key_expression_column("COALESCE((station)::text, '-987654321'::text)").as_deref(),
            Some("station")
        );
        assert_eq!(key_expression_column("lower(name)"), None);
        assert!(is_plain_identifier("\"a b\"") && !is_plain_identifier("COALESCE(a, 1)"));
    }

    #[test]
    fn repeated_keys_keep_their_last_row() {
        use meerschaum_kernel::frame::Column;
        let df = Frame::new(vec![
            Column::object(
                "id",
                vec![
                    KValue::Int(1),
                    KValue::Int(2),
                    KValue::Null,
                    KValue::Int(1),
                    KValue::Null,
                    KValue::Int(3),
                ],
            ),
            Column::object("v", (1..=6).map(KValue::Int).collect::<Vec<_>>()),
        ])
        .unwrap();
        let out = last_row_per_key(&df, &["id".to_string()]);
        let values = |c: &str| out.data(c).unwrap().values();
        // Key 1 keeps v=4, NULL keeps v=5 (NULL keys are equal), order kept.
        assert_eq!(
            values("v"),
            vec![
                KValue::Int(2),
                KValue::Int(4),
                KValue::Int(5),
                KValue::Int(6)
            ]
        );
        // No repeats (or a missing key column): unchanged.
        let unique = df.take(&[0, 1, 2]);
        assert_eq!(last_row_per_key(&unique, &["id".to_string()]).height(), 3);
        assert_eq!(last_row_per_key(&df, &["missing".to_string()]).height(), 6);
    }

    #[test]
    fn postgres_versions_parse() {
        assert_eq!(
            postgres_major_version(
                "PostgreSQL 17.5 (Debian 17.5-1.pgdg110+1) on x86_64-pc-linux-gnu, compiled by gcc"
            ),
            Some(17)
        );
        assert_eq!(
            postgres_major_version("PostgreSQL 9.6.24 on x86_64"),
            Some(9)
        );
        assert_eq!(postgres_major_version("15.4"), Some(15));
        assert_eq!(postgres_major_version("CockroachDB CCL v23.1"), None);
    }

    #[test]
    fn sqlite_key_lists_parse() {
        assert_eq!(
            sqlite_index_key_list(
                "CREATE UNIQUE INDEX \"IX_t_ts_id\" ON \"t\" (\"ts\", COALESCE(\"id\", -987654321) )"
            ),
            Some(vec![
                "\"ts\"".to_string(),
                "COALESCE(\"id\", -987654321)".to_string()
            ])
        );
        assert_eq!(
            sqlite_index_key_list("CREATE UNIQUE INDEX ix ON t (a, b)"),
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }
}
