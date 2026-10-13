//! The native `Pipe.sync()` flow (`meerschaum/core/Pipe/_sync.py`, `_fetch.py`), written
//! against the [`InstanceConnector`] / [`Connector`] traits so that SQL, Valkey, API and the
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
    ConnectorError, DataQuery, DateBound, InstanceConnector, SyncOptions, async_trait,
};
use meerschaum_core::pipe::{ChunkInterval, PipeDefaults};
use meerschaum_core::success::SyncResult;
use meerschaum_core::template::{TemplateResolver, replace_in_parameters};
use meerschaum_core::value::apply_patch_maps;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::cast::{coerce_datetime, datetime_to_int};
use meerschaum_kernel::detect::get_special_cols;
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
            out.set_parameters(apply_patch_maps(params, pipe.parameters()));
        }
        out
    }
}

fn fail(message: impl Into<String>) -> SyncResult {
    SyncResult::new(false, message)
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
pub fn resolve_parameters(env: &SyncEnv<'_>, pipe: &Pipe) -> (Pipe, Vec<String>) {
    let mut warnings = Vec::new();
    if !has_templates(&Value::Map(pipe.parameters().clone())) {
        return (pipe.clone(), warnings);
    }
    let resolver = EngineResolver { env };
    let resolved = replace_in_parameters(pipe.parameters(), Some(pipe), &resolver, &mut warnings);
    let mut out = pipe.clone();
    out.set_parameters(resolved);
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
    if !args.parameters_loaded && !pipe.temporary {
        if let Ok(attrs) = instance.get_pipe_attributes(&pipe).await {
            if let Some(Value::Map(registered)) = attrs.get("parameters") {
                let merged = apply_patch_maps(registered, pipe.parameters());
                pipe.set_parameters(merged);
            }
        }
    }
    let (mut pipe, warnings) = resolve_parameters(env, &pipe);
    for w in warnings {
        tracing::warn!("{w}");
    }

    // Ensure that the pipe is registered.
    if !pipe.temporary && !args.parameters_loaded {
        match instance.get_pipe_id(&pipe).await {
            Ok(Some(_)) => {}
            Ok(None) => {
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
                return ps.sync(&pipe, args).await;
            }
            let source = match env.registry.source(pipe.connector_keys()) {
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
                    tracing::warn!(
                        "Failed to fetch data from {}:\n    {e}",
                        source.connector_keys()
                    );
                    return fail(format!("No data were fetched for {pipe}."));
                }
            };
            let query = DataQuery {
                begin,
                end,
                params: args.params.clone(),
                ..Default::default()
            };
            match source.fetch(&pipe, &query).await {
                Ok(Some(df)) => PipeInput::Frame(df),
                Ok(None) => return fail(format!("No data were fetched for {pipe}.")),
                Err(ConnectorError::NotImplemented(_)) => {
                    tracing::warn!(
                        "No `fetch()` function defined for connector '{}'",
                        source.connector_keys()
                    );
                    return fail(format!("No data were fetched for {pipe}."));
                }
                Err(e) => {
                    // PARITY: v4 prints the exception and returns the generic message
                    // (`core/Pipe/_sync.py`, `msg` is overwritten by the `df is None` branch).
                    tracing::warn!(
                        "Failed to fetch data from {}:\n    {e}",
                        source.connector_keys()
                    );
                    return fail(format!("No data were fetched for {pipe}."));
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
    let mut df = match pipe_enforce_dtypes(input, &ctx) {
        Ok(df) => df,
        Err(e) => return fail(format!("Failed to enforce dtypes:\n{e}")),
    };
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
    let new_special: IndexMap<String, String> = get_special_cols(&df)
        .into_iter()
        .filter(|(c, _)| {
            !ctx.dtypes
                .get(c)
                .is_some_and(|t| meerschaum_core::dtypes::is_dtype_special(t))
        })
        .collect();
    let mut extra = Map::new();
    if !new_special.is_empty() {
        let mut dtypes = pipe
            .param("dtypes")
            .and_then(|d| d.as_map().cloned())
            .unwrap_or_default();
        for (c, t) in &new_special {
            dtypes.insert(c.clone(), Value::Str(t.clone()));
        }
        let mut patch = Map::new();
        patch.insert("dtypes".into(), Value::Map(dtypes));
        pipe.update_parameters(patch);
        if !pipe.temporary {
            let res = instance.edit_pipe(&pipe, false).await;
            if !res.success {
                tracing::warn!(
                    "Failed to capture new special columns for {pipe}:\n{}",
                    res.message
                );
            }
        }
        extra.insert(
            "new_special_cols".into(),
            Value::Map(
                new_special
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                    .collect(),
            ),
        );
    }
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
    let mut attempt = 1;
    let result = loop {
        let res = instance.sync_pipe(&pipe, df.clone(), &opts).await;
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
    if result.success {
        if let Some(obs) = observer {
            if let Err(e) = obs.after_commit(&pipe, &result).await {
                return fail(e);
            }
        }
    }
    result.end = args.end.as_ref().and_then(date_bound_to_bound);
    if let SyncBegin::At(b) = &args.begin {
        result.begin = date_bound_to_bound(b);
    }
    result
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
