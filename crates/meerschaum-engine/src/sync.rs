//! The native `Pipe.sync()` flow (`meerschaum/core/Pipe/_sync.py`, `_fetch.py`), written
//! against the [`InstanceConnector`] / [`Connector`](meerschaum_connector::Connector) traits so that SQL, Valkey, API and the
//! Python adapter's connectors all run through it.
//!
//! The observable flow is v4's:
//!
//! 1. register the pipe if it is not registered (and not `temporary`);
//! 2. with no data given: a plugin's own `sync()` if it has one, else `fetch()` from the source
//!    connector with `begin=''` resolved as `_determine_begin` does (the sync time minus the
//!    backtrack interval, `None` when `end` is set);
//! 3. `enforce_dtypes`, the `autotime` column, persisting newly detected special columns;
//! 4. `instance.sync_pipe()` (retried under `force`).
//!
//! The engine adds one step for the dirty-range ledger (§5.4): when a [`DeltaObserver`] is
//! given, the delta `filter_existing` computes is handed to it **before** the data commit
//! (the ledger write), and only that delta is passed to `sync_pipe`. Without an observer the
//! flow is exactly v4's (a bare `pipe.sync()`, R1).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_connector::pipe_ops::{self, columns_map};
use meerschaum_connector::{
    ChunkedFetch, ConnectorError, DataQuery, DateBound, FetchedKind, InstanceConnector,
    SyncOptions, async_trait,
};
use meerschaum_core::pipe::{ChunkInterval, PipeDefaults};
use meerschaum_core::success::SyncResult;
use meerschaum_core::template::TemplateResolver;
use meerschaum_core::value::apply_patch_maps;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::cast::{coerce_datetime, datetime_to_int};
use meerschaum_kernel::diff::{BacktrackRequest, FilterExistingParams, filter_existing};
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

use crate::node::PipeRef;
use crate::registry::ConnectorRegistry;

/// v4's `begin` argument: `''` (the default), `None`, or an explicit bound.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SyncBegin {
    /// `begin=''`: the sync time minus the backtrack interval.
    #[default]
    Default,
    /// `begin=None`: no lower bound.
    Unbounded,
    /// An explicit lower bound.
    At(DateBound),
}

/// The keyword arguments of `Pipe.sync()` the native flow honors.
#[derive(Debug, Clone)]
pub struct SyncArgs {
    /// Additional CLI and plugin-specific keyword arguments passed to Python syncs and hooks.
    pub extra: Map,
    /// `begin`.
    pub begin: SyncBegin,
    /// `end` (exclusive).
    pub end: Option<DateBound>,
    /// `params` (passed to `fetch`).
    pub params: Option<Map>,
    /// `check_existing`.
    pub check_existing: bool,
    /// `enforce_dtypes`.
    pub enforce_dtypes: bool,
    /// `chunksize` (`Some(-1)`: the configured default; `None`: one transaction).
    pub chunksize: Option<i64>,
    /// `force`: retry `sync_pipe` until it succeeds, up to `retries` attempts.
    pub force: bool,
    /// `retries` (default 10).
    pub retries: u32,
    /// `min_seconds` between forced retries (default 1).
    pub min_seconds: f64,
    /// `workers`.
    pub workers: Option<i64>,
    /// The parameters on the pipe are the registered ones (the engine's graph read them in
    /// one batch): skip re-reading them and the registration check.
    pub parameters_loaded: bool,
    /// When the sync began (`sync_timestamp` for hooks).
    pub sync_timestamp: DateTime<Utc>,
}

impl Default for SyncArgs {
    fn default() -> Self {
        SyncArgs {
            extra: Map::new(),
            begin: SyncBegin::Default,
            end: None,
            params: None,
            check_existing: true,
            enforce_dtypes: true,
            chunksize: Some(-1),
            force: false,
            retries: 10,
            min_seconds: 1.0,
            workers: None,
            parameters_loaded: false,
            sync_timestamp: Utc::now(),
        }
    }
}

/// Sees the delta of a sync before and after its data commit (the ledger writer, crash
/// injection in tests).
#[async_trait]
pub trait DeltaObserver: Send + Sync {
    /// Called with the rows about to be inserted, updated or upserted, before the data commit.
    /// An error aborts the sync with nothing committed.
    async fn before_commit(&self, pipe: &Pipe, delta: &Frame) -> Result<(), String>;

    /// Called after a successful data commit. An error turns the result into a failure (the
    /// data stays committed).
    async fn after_commit(&self, pipe: &Pipe, result: &SyncResult) -> Result<(), String> {
        let _ = (pipe, result);
        Ok(())
    }

    /// Whether this observer records the delta of a commit (a ledger writer, crash injection).
    /// A progress-only observer returns `false`, so that a sync which would otherwise bypass the
    /// observer entirely (the SQL in-place `INSERT ... SELECT`, which has no delta frame) keeps
    /// its direct path and v4's messages and table layout.
    fn records_deltas(&self) -> bool {
        true
    }

    /// A fetched chunk is about to sync (in fetch order). Progress only: must not block.
    fn chunk_started(&self, chunk: &FetchChunk) {
        let _ = chunk;
    }

    /// A fetched chunk finished syncing (successfully or not).
    fn chunk_finished(&self, chunk: &FetchChunk, result: &SyncResult, elapsed: Duration) {
        let _ = (chunk, result, elapsed);
    }
}

/// One piece of a fetched frame that is synced separately (SQL fetch chunks, `--chunksize`).
#[derive(Debug, Clone, PartialEq)]
pub struct FetchChunk {
    /// Position within this fetch, starting at 0.
    pub index: usize,
    /// Rows in the chunk.
    pub rows: u64,
    /// The smallest datetime-axis value in the chunk, if the pipe has a datetime column.
    pub begin: Option<meerschaum_core::success::Bound>,
    /// The largest datetime-axis value in the chunk.
    pub end: Option<meerschaum_core::success::Bound>,
}

/// What the native flow needs besides the pipe: connectors and configuration.
#[derive(Clone)]
pub struct SyncEnv<'a> {
    /// Connector lookup.
    pub registry: &'a dyn ConnectorRegistry,
    /// Configuration defaults for pipes.
    pub defaults: &'a PipeDefaults,
    /// `pipes:parameters:fetch:backtrack_minutes` (default 1440).
    pub default_backtrack_minutes: i64,
    /// `pipes:sync:filter_params_index_limit` (default 250).
    pub filter_params_index_limit: usize,
    /// Registered parameters of known pipes, for resolving `{{ Pipe(...) }}` without a query.
    pub known: Option<&'a HashMap<PipeRef, Map>>,
}

impl std::fmt::Debug for SyncEnv<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncEnv")
            .field("default_backtrack_minutes", &self.default_backtrack_minutes)
            .finish_non_exhaustive()
    }
}

/// The registry's instances as the provider of cross-instance references.
struct RegistryProvider<'a>(&'a dyn ConnectorRegistry);

impl meerschaum_connector::InstanceProvider for RegistryProvider<'_> {
    fn instance(&self, keys: &str) -> Option<std::sync::Arc<dyn InstanceConnector>> {
        self.0.instance(keys).ok()
    }
}

/// Resolves `{{ Pipe(...) }}` from the engine's known pipes and the registry.
struct EngineResolver<'a> {
    env: &'a SyncEnv<'a>,
}

impl TemplateResolver for EngineResolver<'_> {
    fn defaults(&self) -> PipeDefaults {
        self.env.defaults.clone()
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        let loaded = self.load_pipe(pipe);
        self.env.registry.table_reference(&loaded)
    }

    fn load_pipe(&self, pipe: &Pipe) -> Pipe {
        let mut out = pipe.clone();
        if let Some(params) = self
            .env
            .known
            .and_then(|k| k.get(&PipeRef::from_pipe(pipe)))
        {
            out.set_parameters(apply_patch_maps(params, pipe.raw_parameters()));
        }
        out
    }
}

fn fail(message: impl Into<String>) -> SyncResult {
    SyncResult::new(false, message)
}

fn fetch_failed(pipe: &Pipe, connector_keys: &str, error: &ConnectorError) -> SyncResult {
    let detail = format!("Failed to fetch data from {connector_keys}:\n    {error}");
    tracing::warn!("{detail}");
    // Diagnostic improvement over v4: embedded callers may not receive tracing output.
    // Keep the familiar failure prefix and return the source's explanation as well.
    fail(format!("No data were fetched for {pipe}.\n{detail}"))
}

/// Whether any string in a parameters tree contains a template opener.
fn has_templates(v: &Value) -> bool {
    match v {
        Value::Str(s) => s.contains("{{"),
        Value::Map(m) => m.values().any(has_templates),
        Value::List(l) => l.iter().any(has_templates),
        _ => false,
    }
}

/// `pipe.parameters` (the property): `{{ Pipe(...) }}` templates resolved.
///
/// The raw (stored) parameters stay untouched so later persistence (`edit_pipe`) never writes
/// resolved text; the resolved snapshot is cached with `resolve_parameters_with` using the
/// engine's registry-backed resolver, so lazy fallback cannot consume a reference first.
pub fn resolve_parameters(env: &SyncEnv<'_>, pipe: &Pipe) -> (Pipe, Vec<String>) {
    let raw = Value::Map(pipe.raw_parameters().clone());
    if !has_templates(&raw) && !raw.to_json_string().contains("MRSM{") {
        return (pipe.clone(), Vec::new());
    }
    let resolver = EngineResolver { env };
    let mut out = pipe.clone();
    let config = meerschaum_core::config::Config::current().ok();
    let warnings = out
        .resolve_parameters_with(&resolver, config.as_deref())
        .unwrap_or_else(|e| vec![e.to_string()]);
    (out, warnings)
}

/// `Pipe.parse_date_bounds(dt_val)` (`core/Pipe/_data.py:991`) for one bound: integers pass
/// through; a datetime on an integer axis becomes an epoch integer (with `precision`, else an
/// error); otherwise the datetime is coerced to the timezone of the datetime column's dtype
/// (`coerce_timezone(dt, strip_utc='utc' not in dt_typ)`), so a naive value read from a
/// tz-aware column (SQLite) becomes UTC-aware and an aware value on a naive column is stripped.
/// `dtypes` is `Pipe.get_dtypes()`.
pub fn parse_date_bound(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    dtypes: &IndexMap<String, String>,
    bound: DateBound,
) -> Result<DateBound, String> {
    let DateBound::Datetime(dt) = bound else {
        return Ok(bound);
    };
    // `_dtypes.get(dt_col, 'datetime')`: also when the pipe has no datetime column.
    let dt_typ = pipe
        .column("datetime")
        .and_then(|c| dtypes.get(&c).cloned())
        .unwrap_or_else(|| "datetime".to_string());
    if meerschaum_core::dtypes::are_dtypes_equal(&dt_typ, "int") {
        if pipe.param("precision").is_some_and(|p| p.truthy()) {
            let unit = pipe
                .precision(env.defaults)
                .map_err(|e| e.to_string())?
                .unit
                .name()
                .to_string();
            return datetime_to_int(&KValue::Datetime(dt), &unit)
                .map(DateBound::Int)
                .map_err(|e| e.to_string());
        }
        return Err(format!(
            "Cannot use datetime bound '{}' on the non-epoch integer axis of {pipe}.\n    \
             Pass an integer instead, or set the `precision` parameter.",
            DateBound::Datetime(dt).py_str()
        ));
    }
    let dt_typ = if dt_typ == "datetime" {
        meerschaum_core::dtypes::pd_dtype("datetime")
            .unwrap_or("datetime64[ns, UTC]")
            .to_string()
    } else {
        dt_typ
    };
    let strip_utc = !dt_typ.to_lowercase().contains("utc");
    Ok(DateBound::Datetime(coerce_datetime(&dt, strip_utc)))
}

/// `_determine_begin(pipe, begin, end, check_existing)` (`core/Pipe/_fetch.py:144`). The sync
/// time goes through [`parse_date_bound`] first, as `Pipe.get_sync_time()` returns
/// `parse_date_bounds(sync_time)` (`core/Pipe/_sync.py:624`).
pub async fn determine_begin(
    env: &SyncEnv<'_>,
    instance: &dyn InstanceConnector,
    pipe: &Pipe,
    dtypes: &IndexMap<String, String>,
    begin: &SyncBegin,
    end: Option<&DateBound>,
    check_existing: bool,
) -> Result<Option<DateBound>, ConnectorError> {
    match begin {
        SyncBegin::At(b) => return Ok(Some(b.clone())),
        SyncBegin::Unbounded => return Ok(None),
        SyncBegin::Default => {}
    }
    // PARITY: with `begin=''` and an `end`, v4 fetches with no lower bound
    // (`_fetch.py:_determine_begin`, `if end is not None: return None`).
    if end.is_some() {
        return Ok(None);
    }
    let Some(sync_time) = instance.get_sync_time(pipe, None, true).await? else {
        return Ok(None);
    };
    let sync_time =
        parse_date_bound(env, pipe, dtypes, sync_time).map_err(ConnectorError::Invalid)?;
    let backtrack = pipe
        .get_backtrack_interval(check_existing, env.default_backtrack_minutes, env.defaults)
        .map_err(|e| ConnectorError::Invalid(e.to_string()))?;
    Ok(Some(match (sync_time, backtrack) {
        (DateBound::Datetime(dt), ChunkInterval::Delta(d)) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc - d,
            tz: dt.tz,
        }),
        // `isinstance(sync_time, datetime) and isinstance(backtrack_interval, int)`: minutes.
        (DateBound::Datetime(dt), ChunkInterval::Int(m)) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc - chrono::TimeDelta::minutes(m),
            tz: dt.tz,
        }),
        (DateBound::Int(i), ChunkInterval::Int(b)) => DateBound::Int(i - b),
        (st @ DateBound::Int(_), ChunkInterval::Delta(d)) => {
            // PARITY: `sync_time - backtrack_interval` raises for an int minus a timedelta;
            // v4 warns and returns the sync time (`_fetch.py:_determine_begin`).
            tracing::warn!(
                "Unable to substract backtrack interval {d} from {}.",
                st.py_str()
            );
            st
        }
    }))
}

/// Parse row counts out of v4's `sync_pipe` messages (`"Inserted 1,234, updated 5 rows."`,
/// `"Upserted 3 rows."`) into a [`SyncResult`]. The trait's `sync_pipe` returns a
/// `SuccessTuple`; every built-in instance writes these messages.
pub fn result_from_message(success: bool, message: String) -> SyncResult {
    fn number_after(text: &str, word: &str) -> Option<u64> {
        let i = text.find(word)? + word.len();
        let digits: String = text[i..]
            .trim_start()
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == ',')
            .filter(|c| *c != ',')
            .collect();
        digits.parse().ok()
    }
    let mut r = SyncResult::new(success, message);
    if success {
        if let Some(n) = number_after(&r.message, "Upserted ") {
            r.upserted = n;
        } else {
            r.inserted = number_after(&r.message, "Inserted ").unwrap_or(0);
            r.updated = number_after(&r.message, "updated ").unwrap_or(0);
        }
    }
    r
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

/// `pipe.filter_existing(df)` over the trait: `(unseen, update, delta)`.
pub async fn filter_existing_frame(
    env: &SyncEnv<'_>,
    instance: &dyn InstanceConnector,
    pipe: &Pipe,
    ctx: &meerschaum_kernel::enforce::PipeDtypesContext,
    df: Frame,
) -> Result<(Frame, Frame, Frame), String> {
    let params = FilterExistingParams {
        columns: columns_map(pipe),
        configured_dtypes: ctx.dtypes.clone(),
        dtypes_ctx: ctx.clone(),
        autoincrement: pipe.autoincrement(),
        autotime: pipe.autotime(),
        static_: pipe.is_static(),
        include_unchanged_columns: false,
        date_bound_only: false,
        enforce_dtypes: false,
        filter_params_index_limit: env.filter_params_index_limit,
    };
    // The kernel's callback is synchronous: capture the request, read it, then diff.
    let mut request: Option<BacktrackRequest> = None;
    let _ = filter_existing(PipeInput::Frame(df.clone()), &params, |req| {
        request = Some(req.clone());
        None
    });
    let backtrack = match &request {
        Some(req) => {
            let q = DataQuery {
                begin: DateBound::from_value(&req.begin),
                end: DateBound::from_value(&req.end),
                params: Some(backtrack_params(req)),
                ..Default::default()
            };
            instance
                .get_pipe_data(pipe, &q)
                .await
                .map_err(|e| e.to_string())?
        }
        None => None,
    };
    filter_existing(PipeInput::Frame(df), &params, move |_| backtrack).map_err(|e| e.to_string())
}

/// `get_current_timestamp(precision_unit, as_int)` for `autotime` (rounded down to the unit).
fn current_timestamp(unit_name: &str, as_int: bool) -> KValue {
    let now = Utc::now();
    let micros = now.timestamp_micros();
    let unit_micros: i64 = match unit_name {
        "millisecond" => 1_000,
        "second" => 1_000_000,
        "minute" => 60_000_000,
        "hour" => 3_600_000_000,
        "day" => 86_400_000_000,
        _ => 1,
    };
    let rounded = micros.div_euclid(unit_micros) * unit_micros;
    if as_int {
        return KValue::Int(match unit_name {
            "nanosecond" => rounded.saturating_mul(1000),
            _ => rounded / unit_micros,
        });
    }
    KValue::Datetime(DateTimeValue::utc(
        DateTime::from_timestamp_micros(rounded)
            .map(|d| d.naive_utc())
            .unwrap_or_else(|| now.naive_utc()),
    ))
}

/// `Pipe.sync(df, **args)`: the native flow (see the module docs). `input = None` fetches.
pub async fn sync_pipe(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    input: Option<PipeInput>,
    args: &SyncArgs,
    observer: Option<&dyn DeltaObserver>,
) -> SyncResult {
    let start = Instant::now();
    let mut result = sync_inner(env, pipe, input, args, observer).await;
    result.duration.get_or_insert(start.elapsed());
    result
}

async fn sync_inner(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    input: Option<PipeInput>,
    args: &SyncArgs,
    observer: Option<&dyn DeltaObserver>,
) -> SyncResult {
    let instance = match env.registry.instance(pipe.instance_keys()) {
        Ok(i) => i,
        Err(e) => return fail(e),
    };
    let instance = instance.as_ref();

    // `pipe.parameters`: the registered parameters patched with the in-memory ones.
    let mut pipe = pipe.clone();
    if !args.parameters_loaded
        && !pipe.temporary
        && let Ok(attrs) = instance.get_pipe_attributes(&pipe).await
        && let Some(Value::Map(registered)) = attrs.get("parameters")
    {
        let merged = apply_patch_maps(registered, pipe.raw_parameters());
        pipe.set_parameters(merged);
    }
    let (mut pipe, warnings) = if env.known.is_none()
        && meerschaum_connector::template::needs_instance_resolution(&pipe)
    {
        // A one-pipe sync (no graph snapshot): `reference`s and `{{ Pipe() }}` templates are
        // resolved against the registered state of the pipes they name, fetched from their
        // instances (what `Pipe.parameters` does lazily in v4).
        meerschaum_connector::resolve_pipe(
            &pipe,
            instance,
            Some(&RegistryProvider(env.registry)),
            env.defaults,
        )
        .await
    } else {
        resolve_parameters(env, &pipe)
    };
    for w in warnings {
        tracing::warn!("{w}");
    }

    // Ensure that the pipe is registered.
    if !pipe.temporary && !args.parameters_loaded {
        match instance.get_pipe_id(&pipe).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                // `Pipe.register()`: a plugin's `register(pipe)` supplies the base parameters
                // under the pipe's own (`apply_patch_to_config(params, self.parameters)`).
                if let Some(hook) = env.registry.register_hook(pipe.connector_keys()) {
                    match hook.register_parameters(&pipe).await {
                        crate::ConnectorRegister::Parameters(params) => {
                            let merged = apply_patch_maps(&params, pipe.raw_parameters());
                            pipe.set_parameters(merged);
                        }
                        crate::ConnectorRegister::Invalid(repr) => tracing::warn!(
                            "Invalid parameters returned from `register()` in connector {}:\n{repr}",
                            pipe.connector_keys()
                        ),
                        crate::ConnectorRegister::Undefined => {}
                    }
                }
                let reg = instance.register_pipe(&pipe).await;
                if !reg.success && !reg.message.contains("already") {
                    return reg.into();
                }
            }
            Err(e) => return fail(e.to_string()),
        }
    }

    let input = match input {
        Some(i) => i,
        None => {
            if let Some(ps) = env.registry.plugin_sync(pipe.connector_keys()) {
                return ps.sync_native(&pipe, args, env, observer).await;
            }
            let source = match env.registry.source_for_sync(pipe.connector_keys(), args) {
                Ok(Some(s)) => s,
                Ok(None) => {
                    return SyncResult::new(
                        true,
                        format!("{pipe} does not support fetching; nothing to do."),
                    );
                }
                Err(e) => {
                    tracing::debug!("{e}");
                    let mut msg = format!("{pipe} does not have a valid connector.");
                    if pipe.connector_keys().starts_with("plugin:") {
                        msg.push_str(&format!(
                            "\n    Perhaps {} has a syntax error?",
                            pipe.connector_keys()
                        ));
                    }
                    return fail(msg);
                }
            };
            // `begin, end = self.parse_date_bounds(begin, end)` (`core/Pipe/_fetch.py:78`).
            let dtypes = match pipe_ops::get_dtypes(instance, &pipe, pipe.parameters(), true).await
            {
                Ok(d) => d,
                Err(e) => return fail(e.to_string()),
            };
            let parse = |b: DateBound| parse_date_bound(env, &pipe, &dtypes, b);
            let end = match args.end.clone().map(parse).transpose() {
                Ok(e) => e,
                Err(e) => return fail(e),
            };
            let begin_arg = match &args.begin {
                SyncBegin::At(b) => match parse(b.clone()) {
                    Ok(b) => SyncBegin::At(b),
                    Err(e) => return fail(e),
                },
                other => other.clone(),
            };
            let begin = match determine_begin(
                env,
                instance,
                &pipe,
                &dtypes,
                &begin_arg,
                end.as_ref(),
                args.check_existing,
            )
            .await
            {
                Ok(b) => b,
                Err(e) => {
                    return fetch_failed(&pipe, &source.connector_keys(), &e);
                }
            };
            let query = DataQuery {
                begin,
                end,
                params: args.params.clone(),
                ..Default::default()
            };
            // Sources whose `fetch()` may return a generator, hook results, `True` or `None`
            // (Python plugins and custom connectors) are consumed the way v4's `_sync` does.
            let sync_chunks = args.extra.get("sync_chunks").is_none_or(|v| v.truthy());
            let fetch_started = std::time::Instant::now();
            meerschaum_core::dprint!(
                "Fetching {pipe} from {} (begin={:?}, end={:?}).",
                source.connector_keys(),
                query.begin,
                query.end
            );
            match source.fetch_chunked(&pipe, &query, sync_chunks).await {
                Ok(Some(chunked)) => {
                    return sync_chunked_source(env, &pipe, chunked, args, observer).await;
                }
                Ok(None) => {}
                Err(e) => {
                    return fetch_failed(&pipe, &source.connector_keys(), &e);
                }
            }
            match source.fetch(&pipe, &query).await {
                Ok(Some(df)) if source.connector_type() == "sql" && args.chunksize.is_some() => {
                    let size = args
                        .chunksize
                        .filter(|n| *n > 0)
                        .map(|n| n as usize)
                        .unwrap_or_else(|| env.registry.fetch_chunksize(pipe.connector_keys()));
                    return sync_fetched_chunks(env, &pipe, df, args, observer, size.max(1)).await;
                }
                Ok(Some(df)) => {
                    meerschaum_core::dprint!(
                        "Fetched {} rows for {pipe} in {:?}.",
                        df.height(),
                        fetch_started.elapsed()
                    );
                    PipeInput::Frame(df)
                }
                Ok(None) => return fail(format!("No data were fetched for {pipe}.")),
                Err(e) => {
                    return fetch_failed(&pipe, &source.connector_keys(), &e);
                }
            }
        }
    };

    // Cast to a frame and enforce the pipe's dtypes.
    let mut ctx = match pipe_ops::dtypes_context(instance, &pipe, pipe.parameters()).await {
        Ok(c) => c,
        Err(e) => return fail(e.to_string()),
    };
    ctx.enforce = ctx.enforce && args.enforce_dtypes;
    let enforce_started = std::time::Instant::now();
    let mut df = match pipe_enforce_dtypes(input, &ctx) {
        Ok(df) => df,
        Err(e) => return fail(format!("Failed to enforce dtypes:\n{e}")),
    };
    meerschaum_core::dprint!(
        "Enforced dtypes on {} rows for {pipe} in {:?} (enforce={}).",
        df.height(),
        enforce_started.elapsed(),
        ctx.enforce
    );
    let mut check_existing = args.check_existing;
    if pipe.autotime() {
        let dt_col = pipe.column("datetime");
        let ts_col = dt_col
            .clone()
            .unwrap_or_else(|| env.defaults.autotime_column.clone());
        if !df.contains(&ts_col) {
            let ts_typ = ctx
                .dtypes
                .get(&ts_col)
                .cloned()
                .unwrap_or_else(|| "datetime".into());
            let unit = pipe
                .precision(env.defaults)
                .map(|p| p.unit.name().to_string())
                .unwrap_or_else(|_| "microsecond".into());
            let now = current_timestamp(
                &unit,
                meerschaum_core::dtypes::are_dtypes_equal(&ts_typ, "int"),
            );
            let n = df.height();
            if let Err(e) = df.set_column(meerschaum_kernel::Column::object(&ts_col, vec![now; n]))
            {
                return fail(e.to_string());
            }
            check_existing = dt_col.is_some();
        }
    }

    // `_persist_new_special_columns`.
    let mut extra = Map::new();
    pipe_ops::persist_new_special_columns(instance, &mut pipe, &df, &ctx.dtypes, true, &mut extra)
        .await;
    if let Some(w) = args.workers {
        extra.insert("workers".into(), Value::Int(w));
    }

    // The ledger hook: compute the delta and hand it over before the data commit.
    if let Some(obs) = observer {
        let delta = if check_existing {
            match filter_existing_frame(env, instance, &pipe, &ctx, df).await {
                Ok((_, _, delta)) => delta,
                Err(e) => return fail(e),
            }
        } else {
            df
        };
        meerschaum_core::dprint!("Filtered {pipe}: {} new or changed rows.", delta.height());
        if let Err(e) = obs.before_commit(&pipe, &delta).await {
            return fail(e);
        }
        df = delta;
    }

    let opts = SyncOptions {
        check_existing,
        chunksize: args.chunksize,
        begin: None,
        end: None,
        extra,
    };
    // Without a ledger observer the changed intervals are the grid cells of the rows handed to
    // the instance (a superset of the rows that changed; the ledger's exact delta needs the
    // observer's second `filter_existing`, which a bare `Pipe.sync()` does not pay for).
    let written_cells = if observer.is_none() {
        change_cells(env, &pipe, &df)
    } else {
        None
    };
    let mut attempt = 1;
    let result = loop {
        let write_started = std::time::Instant::now();
        meerschaum_core::dprint!(
            "Writing {} rows to {pipe} (attempt {attempt}, check_existing={}).",
            df.height(),
            opts.check_existing
        );
        let res = instance.sync_pipe(&pipe, df.clone(), &opts).await;
        meerschaum_core::dprint!(
            "Wrote {pipe} in {:?}: {}",
            write_started.elapsed(),
            res.message
        );
        attempt += 1;
        if res.success || !args.force || attempt > args.retries {
            if !res.success && args.force && attempt > args.retries {
                tracing::warn!(
                    "Unable to sync {pipe} within {} attempt{}!",
                    args.retries,
                    if args.retries != 1 { "s" } else { "" }
                );
            }
            break res;
        }
        tokio::time::sleep(Duration::from_secs_f64(args.min_seconds.max(0.0))).await;
    };
    let mut result = result_from_message(result.success, result.message);
    if result.success
        && let Some(obs) = observer
        && let Err(e) = obs.after_commit(&pipe, &result).await
    {
        return fail(e);
    }
    result.end = args.end.as_ref().and_then(date_bound_to_bound);
    if let SyncBegin::At(b) = &args.begin {
        result.begin = date_bound_to_bound(b);
    }
    if result.success
        && result.rows_written() + result.deleted > 0
        && let Some((axis, positions)) = written_cells
    {
        result.changed_intervals = crate::ledger::ledger_intervals(positions, &axis)
            .iter()
            .map(|iv| meerschaum_core::success::ChangedInterval {
                begin: iv.begin.and_then(|b| axis.bound(b)),
                end: iv.end.and_then(|e| axis.bound(e)),
            })
            .collect();
    }
    result
}

/// The datetime-axis positions of a frame about to be synced and the pipe's axis (grid width =
/// the chunk interval), for [`SyncResult::changed_intervals`]. `None` when the frame is empty.
fn change_cells(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    df: &Frame,
) -> Option<(crate::axis::Axis, Vec<i64>)> {
    if df.height() == 0 {
        return None;
    }
    let options = crate::graph::GraphOptions {
        defaults: env.defaults.clone(),
        implicit_edges: false,
        default_backtrack_minutes: env.default_backtrack_minutes as f64,
    };
    let axis = crate::graph::node_axis(pipe, &options).ok()?;
    let positions = crate::native::frame_positions(df, &axis);
    Some((axis, positions))
}

/// The keyword arguments a plugin receives for a sync, exactly as `Pipe.sync(**kw)` passes them
/// to a Python plugin's `fetch(pipe, **kw)` / `sync(pipe, **kw)`: the sync's `extra` (CLI and
/// plugin-specific flags) plus `begin`, `end`, `params`, `check_existing`, `enforce`, `force`,
/// `workers`, `chunksize`, `retries` and `min_seconds`. Registries that hand a native (C ABI)
/// plugin source its fetch context build it from this map in
/// [`ConnectorRegistry::source_for_sync`], so native and Python plugins see the same kwargs.
pub fn sync_kwargs(args: &SyncArgs) -> Map {
    let mut map = args.extra.clone();
    map.insert(
        "begin".into(),
        match &args.begin {
            SyncBegin::Default => Value::Str(String::new()),
            SyncBegin::Unbounded => Value::Null,
            SyncBegin::At(b) => bound_value(b),
        },
    );
    map.insert(
        "end".into(),
        args.end.as_ref().map(bound_value).unwrap_or(Value::Null),
    );
    map.insert(
        "params".into(),
        args.params.clone().map(Value::Map).unwrap_or(Value::Null),
    );
    map.insert("check_existing".into(), Value::Bool(args.check_existing));
    map.insert("enforce".into(), Value::Bool(args.enforce_dtypes));
    map.insert("force".into(), Value::Bool(args.force));
    if args.workers.is_some() || !map.contains_key("workers") {
        map.insert(
            "workers".into(),
            args.workers.map(Value::Int).unwrap_or(Value::Null),
        );
    }
    map.insert(
        "chunksize".into(),
        args.chunksize.map(Value::Int).unwrap_or(Value::Null),
    );
    map.insert("retries".into(), Value::Int(args.retries.into()));
    map.insert("min_seconds".into(), Value::Float(args.min_seconds));
    map
}

/// A bound as a plugin kwarg: an integer, or an ISO 8601 string.
pub fn bound_value(bound: &DateBound) -> Value {
    match bound {
        DateBound::Int(i) => Value::Int(*i),
        DateBound::Datetime(_) => Value::Str(bound.isoformat()),
    }
}

/// A connector bound as an engine bound (UTC).
pub fn date_bound_to_bound(b: &DateBound) -> Option<meerschaum_core::success::Bound> {
    use meerschaum_core::success::Bound;
    match b {
        DateBound::Int(i) => Some(Bound::Int(*i)),
        DateBound::Datetime(dt) => Some(Bound::Datetime(DateTime::from_naive_utc_and_offset(
            dt.utc, Utc,
        ))),
    }
}

/// SQL `fetch` yields chunks in v4, including one empty chunk for an empty result.
async fn sync_fetched_chunks(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    df: Frame,
    args: &SyncArgs,
    observer: Option<&dyn DeltaObserver>,
    size: usize,
) -> SyncResult {
    let mut results = Vec::new();
    let mut aggregate = SyncResult::new(true, String::new());
    for start in (0..df.height().max(1)).step_by(size) {
        let chunk = df.take(&(start..(start + size).min(df.height())).collect::<Vec<_>>());
        let label = fetched_chunk_label(&chunk, pipe.column("datetime").as_deref());
        let rows = chunk.height();
        meerschaum_core::dprint!(
            "Fetched chunk {} of {pipe}: {rows} rows ({}).",
            results.len(),
            label.trim()
        );
        let info = {
            let (begin, end) = fetched_chunk_bounds(&chunk, pipe.column("datetime").as_deref());
            FetchChunk {
                index: results.len(),
                rows: rows as u64,
                begin,
                end,
            }
        };
        if let Some(o) = observer {
            o.chunk_started(&info);
        }
        let chunk_start = Instant::now();
        let result = Box::pin(sync_inner(
            env,
            pipe,
            Some(PipeInput::Frame(chunk)),
            args,
            observer,
        ))
        .await;
        if let Some(o) = observer {
            o.chunk_finished(&info, &result, chunk_start.elapsed());
        }
        let message = if results.is_empty() {
            let msg = format!("\n{label}\n{}", result.message);
            if !result.success {
                return fail(format!("Unable to sync initial chunk for {pipe}:\n{msg}"));
            }
            msg
        } else {
            format!(
                "{} a chunk ({} row{}) to {pipe}:\n{label}\n{}",
                if result.success {
                    "Synced"
                } else {
                    "Failed to sync"
                },
                comma_count(rows),
                if rows == 1 { "" } else { "s" },
                result.message
            )
        };
        aggregate.success &= result.success;
        aggregate.inserted += result.inserted;
        aggregate.updated += result.updated;
        aggregate.upserted += result.upserted;
        aggregate.deleted += result.deleted;
        aggregate.begin = aggregate.begin.or(result.begin);
        aggregate.end = result.end;
        aggregate.changed_intervals.extend(result.changed_intervals);
        results.push((result.success, message));
    }
    // PARITY: v4 sorts the `(success, message)` tuples, putting failed chunks first.
    summarize_chunk_results(pipe, results, aggregate)
}

/// A raw chunk as a frame, for its label and progress bounds only (never synced from this).
fn input_frame(input: &PipeInput) -> Option<Frame> {
    match input {
        PipeInput::Frame(f) => Some(f.clone()),
        PipeInput::Docs(d) => meerschaum_kernel::docs::frame_from_docs(d).ok(),
        PipeInput::Columns(c) => meerschaum_kernel::docs::frame_from_columns(c).ok(),
    }
}

fn input_rows(input: &PipeInput) -> usize {
    match input {
        PipeInput::Frame(f) => f.height(),
        PipeInput::Docs(d) => d.len(),
        PipeInput::Columns(c) => c.values().map(Vec::len).max().unwrap_or(0),
    }
}

/// v4's `_sync` for a `fetch()` result that is not one DataFrame (`core/Pipe/_sync.py`, "If
/// connector is a plugin with a `sync()` method ... Fetch the dataframe from the connector's
/// `fetch()` method"): `None`, `True`, the results of the `chunk_hook` calls, a list that came
/// back empty, a generator (every chunk synced on its own), or a single frame.
async fn sync_chunked_source(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    chunked: ChunkedFetch,
    args: &SyncArgs,
    observer: Option<&dyn DeltaObserver>,
) -> SyncResult {
    let mut chunks = chunked.chunks;
    let fetch_failed = |e: ConnectorError| {
        tracing::warn!(
            "Failed to fetch data from {}:\n    {e}",
            pipe.connector_keys()
        );
        fail(format!("No data were fetched for {pipe}."))
    };
    let first = match chunks.next_chunk().await {
        Ok(first) => first,
        Err(e) => return fetch_failed(e),
    };
    let Some(first) = first else {
        return match chunks.finish().await {
            Err(e) => fetch_failed(e),
            Ok(FetchedKind::HookResults(results)) => hook_results_summary(&results),
            Ok(FetchedKind::InParallel) => {
                SyncResult::new(true, format!("{pipe} is being synced in parallel."))
            }
            Ok(FetchedKind::Data { generator: true }) => {
                SyncResult::new(true, "Received an empty generator; nothing to do.")
            }
            Ok(_) => fail(format!("No data were fetched for {pipe}.")),
        };
    };
    let second = match chunks.next_chunk().await {
        Ok(second) => second,
        Err(e) => return fetch_failed(e),
    };
    if second.is_none() {
        match chunks.finish().await {
            Err(e) => return fetch_failed(e),
            Ok(FetchedKind::Data { generator: false }) => {
                // One DataFrame (or one list of documents): the ordinary flow.
                meerschaum_core::dprint!("Fetched {} rows for {pipe}.", input_rows(&first));
                if matches!(&first, PipeInput::Docs(docs) if docs.is_empty()) {
                    return SyncResult::new(true, format!("No new rows were returned for {pipe}."));
                }
                return Box::pin(sync_inner(env, pipe, Some(first), args, observer)).await;
            }
            Ok(_) => {}
        }
    }
    sync_generator_chunks(env, pipe, first, second, chunks, args, observer).await
}

/// The `(success, message)` results a plugin's `chunk_hook` calls returned: all must have
/// succeeded, the messages are joined (`'\n'.join(...)`).
fn hook_results_summary(results: &[meerschaum_core::SuccessTuple]) -> SyncResult {
    if results.is_empty() {
        return SyncResult::new(true, "No new rows were returned.");
    }
    SyncResult::new(
        results.iter().all(|r| r.success),
        results
            .iter()
            .map(|r| r.message.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// v4's chunk-generator branch of `_sync`: the first chunk is synced alone (a failure ends the
/// sync), every other chunk gets three attempts with `attempt ** 2` seconds of sleep after each
/// failure, and the messages are sorted and summarized.
async fn sync_generator_chunks(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    first: PipeInput,
    second: Option<PipeInput>,
    mut chunks: Box<dyn meerschaum_connector::ChunkStream>,
    args: &SyncArgs,
    observer: Option<&dyn DeltaObserver>,
) -> SyncResult {
    const MAX_CHUNK_ATTEMPTS: u32 = 3;
    let dt = pipe.column("datetime");
    let mut results: Vec<(bool, String)> = Vec::new();
    let mut aggregate = SyncResult::new(true, String::new());
    let mut upcoming = second;
    let mut current = Some(first);
    while let Some(input) = current.take() {
        let index = results.len();
        let rows = input_rows(&input);
        let (label, info) = match input_frame(&input) {
            Some(frame) => {
                let (begin, end) = fetched_chunk_bounds(&frame, dt.as_deref());
                (
                    fetched_chunk_label(&frame, dt.as_deref()),
                    FetchChunk {
                        index,
                        rows: rows as u64,
                        begin,
                        end,
                    },
                )
            }
            None => (
                String::new(),
                FetchChunk {
                    index,
                    rows: rows as u64,
                    begin: None,
                    end: None,
                },
            ),
        };
        meerschaum_core::dprint!(
            "Fetched chunk {index} of {pipe}: {rows} rows ({}).",
            label.trim()
        );
        if let Some(o) = observer {
            o.chunk_started(&info);
        }
        let started = Instant::now();
        let result = if index == 0 {
            Box::pin(sync_inner(env, pipe, Some(input), args, observer)).await
        } else {
            let mut attempts = 0u32;
            loop {
                let result =
                    Box::pin(sync_inner(env, pipe, Some(input.clone()), args, observer)).await;
                if result.success {
                    break result;
                }
                attempts += 1;
                let sleep_seconds = u64::from(attempts * attempts);
                tracing::warn!(
                    "Failed to sync chunk to {pipe} (attempt {attempts} / {MAX_CHUNK_ATTEMPTS}).\n\
                     Sleeping for {sleep_seconds} second{}:\n{}",
                    if sleep_seconds == 1 { "" } else { "s" },
                    result.message
                );
                tokio::time::sleep(Duration::from_secs(sleep_seconds)).await;
                if attempts >= MAX_CHUNK_ATTEMPTS {
                    break result;
                }
            }
        };
        if let Some(o) = observer {
            o.chunk_finished(&info, &result, started.elapsed());
        }
        let message = if index == 0 {
            let msg = format!("\n{label}\n{}", result.message);
            if !result.success {
                return fail(format!("Unable to sync initial chunk for {pipe}:\n{msg}"));
            }
            msg
        } else {
            format!(
                "{} a chunk ({} row{}) to {pipe}:\n{label}\n{}",
                if result.success {
                    "Synced"
                } else {
                    "Failed to sync"
                },
                comma_count(rows),
                if rows == 1 { "" } else { "s" },
                result.message
            )
        };
        aggregate.success &= result.success;
        aggregate.inserted += result.inserted;
        aggregate.updated += result.updated;
        aggregate.upserted += result.upserted;
        aggregate.deleted += result.deleted;
        aggregate.begin = aggregate.begin.or(result.begin);
        aggregate.end = result.end;
        aggregate.changed_intervals.extend(result.changed_intervals);
        results.push((result.success, message));
        current = match upcoming.take() {
            Some(next) => Some(next),
            None => match chunks.next_chunk().await {
                Ok(next) => next,
                Err(e) => {
                    tracing::warn!(
                        "Failed to fetch data from {}:\n    {e}",
                        pipe.connector_keys()
                    );
                    aggregate.success = false;
                    results.push((false, format!("Failed to fetch the next chunk:\n{e}")));
                    None
                }
            },
        };
    }
    summarize_chunk_results(pipe, results, aggregate)
}

/// `Synced N chunks to <pipe> ...`: v4 sorts the `(success, message)` tuples, which puts failed
/// chunks first.
fn summarize_chunk_results(
    pipe: &Pipe,
    mut results: Vec<(bool, String)>,
    mut aggregate: SyncResult,
) -> SyncResult {
    results.sort();
    let succeeded = results.iter().filter(|r| r.0).count();
    let failed = results.len() - succeeded;
    let messages = results
        .iter()
        .map(|r| r.1.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    aggregate.message = format!(
        "Synced {} chunk{} to {pipe}\n({succeeded} succeeded, {failed} failed):\n\n{}",
        comma_count(results.len()),
        if results.len() == 1 { "" } else { "s" },
        messages.trim()
    );
    aggregate
}

/// The smallest and largest datetime-axis values of a chunk, as engine bounds.
fn fetched_chunk_bounds(
    df: &Frame,
    dt: Option<&str>,
) -> (
    Option<meerschaum_core::success::Bound>,
    Option<meerschaum_core::success::Bound>,
) {
    use meerschaum_core::success::Bound;
    let Some(column) = dt.and_then(|c| df.column(c)) else {
        return (None, None);
    };
    let values = column.data.values();
    let mut present = values.iter().filter(|v| !v.is_na()).collect::<Vec<_>>();
    present.sort_by(|a, b| pipe_ops::compare_values(a, b));
    let to_bound = |v: &KValue| match v {
        KValue::Datetime(d) => Some(Bound::Datetime(DateTime::from_naive_utc_and_offset(
            d.utc, Utc,
        ))),
        KValue::Date(d) => d
            .and_hms_opt(0, 0, 0)
            .map(|n| Bound::Datetime(DateTime::from_naive_utc_and_offset(n, Utc))),
        KValue::Int(i) => Some(Bound::Int(*i)),
        // A fetch result before dtype enforcement (SQLite text, ISO strings from a plugin).
        KValue::Str(text) => DateBound::parse(text).and_then(|b| date_bound_to_bound(&b)),
        _ => None,
    };
    (
        present.first().and_then(|v| to_bound(v)),
        present.last().and_then(|v| to_bound(v)),
    )
}

fn fetched_chunk_label(df: &Frame, dt: Option<&str>) -> String {
    let Some(column) = dt.and_then(|c| df.column(c)) else {
        return String::new();
    };
    let values = column.data.values();
    let mut present = values.iter().filter(|v| !v.is_na()).collect::<Vec<_>>();
    present.sort_by(|a, b| pipe_ops::compare_values(a, b));
    match (present.first(), present.last()) {
        (Some(a), Some(b)) => format!("{} - {}", a.py_str(), b.py_str()),
        _ => String::new(),
    }
}

fn comma_count(count: usize) -> String {
    let digits = count.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_counts() {
        let r = result_from_message(true, "Inserted 1,234, updated 5 rows.".into());
        assert_eq!((r.inserted, r.updated, r.upserted), (1234, 5, 0));
        let r = result_from_message(true, "Upserted 3 rows.".into());
        assert_eq!((r.inserted, r.upserted), (0, 3));
        let r = result_from_message(false, "Inserted 1, updated 0 rows.".into());
        assert_eq!(r.inserted, 0);
        let r = result_from_message(true, "Success".into());
        assert_eq!(r.rows_written(), 0);
    }
}
