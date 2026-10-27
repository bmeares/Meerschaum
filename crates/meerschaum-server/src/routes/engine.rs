//! The `/engine/*` route family (§5.8, new in v5): engine state for API instances, backed by an
//! [`EngineBackend`](crate::backends::EngineBackend) (`start api`: the server's instance,
//! [`crate::engine_store::InstanceEngine`]).
//!
//! Every route answers in the v5 shape (`{"success": bool, "message": str, ...}` with real
//! status codes, [`crate::envelope`]). Reads (`GET`, and `POST .../read`) need `pipes:read`,
//! writes `pipes:write`.
//!
//! `/engine/pipes/{metric}/{read,write,clear,exists}` expose the engine's six state pipes
//! (`mrsm:engine_*`) to an `APIConnector`, whose engine keeps its state through them; any other
//! metric is a `404` (no generic access to `mrsm` keys). Rows travel as `orient='records'` JSON
//! under `rows`.
//!
//! `GET /engine/runs` lists runs with filters (`pipeline`, `pipe`, `status`, `since`, `limit`,
//! `run_id`, `instance_keys`), `GET /engine/runs/{run_id}` returns one run and its nodes, and
//! `GET /engine/runs/{run_id}/events/stream` follows a run's events as Server-Sent Events
//! ([`crate::engine_stream`]).

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::routing::{get, post};
use meerschaum_connector_api::engine_pipes::{self, EnginePipeQuery};
use meerschaum_connector_api::models::{
    EdgeCursor, EngineEvent, EngineState, LeaseRequest, LedgerEntry, RunRecord,
};
use serde::de::DeserializeOwned;
use serde_json::{Map as JsonMap, Value as Json, json};

use super::Ctx;
use crate::auth::scoped_auth;
use crate::backends::{EngineStoreError, RunStatusFilter, RunsFilter};
use crate::config::ServerConfig;
use crate::envelope;
use crate::error::{ApiError, ApiResult};
use crate::routes::pipes::parse_json_body;
use crate::state::AppState;

/// The scopes of the write routes.
const SCOPES: &[&str] = &["pipes:write"];

/// The scopes of the read routes.
const READ_SCOPES: &[&str] = &["pipes:read"];

/// The most runs one `GET /engine/runs` returns.
pub const MAX_RUNS: usize = 1000;

/// The runs `GET /engine/runs` returns without `limit`.
pub const DEFAULT_RUNS: usize = 100;

/// Register the `engine` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/engine/ledger", post(post_ledger).get(get_ledger))
        .route("/engine/cursors", axum::routing::put(put_cursors))
        .route("/engine/state", get(get_state).put(put_state))
        .route("/engine/runs", post(post_runs).get(get_runs))
        .route("/engine/runs/{run_id}", get(get_run))
        .route(
            "/engine/runs/{run_id}/events/stream",
            get(stream_run_events),
        )
        .route("/engine/events", post(post_events))
        .route("/engine/leases", post(acquire_lease).delete(release_lease))
        // The bodies are read by `body_json` with `ENGINE_BODY_LIMIT` (raw-body handlers are not
        // covered by `DefaultBodyLimit`).
        .route("/engine/pipes/{metric}/read", post(read_pipe))
        .route("/engine/pipes/{metric}/write", post(write_pipe))
        .route("/engine/pipes/{metric}/clear", post(clear_pipe))
        .route("/engine/pipes/{metric}/exists", get(pipe_exists))
}

/// Authorize, then refuse anything but the six engine state pipes.
async fn engine_metric(ctx: &Ctx, metric: &str, scopes: &[&str]) -> Result<(), ApiError> {
    scoped_auth(&ctx.state, &ctx.headers, scopes).await?;
    if !engine_pipes::is_engine_metric(metric) {
        return Err(ApiError::new(
            404,
            format!("'{metric}' is not an engine state pipe."),
        ));
    }
    Ok(())
}

fn store_error(e: EngineStoreError) -> ApiError {
    crate::engine_stream::api_error(e)
}

/// The largest `/engine/*` request body (64 MiB). The engine's biggest batch is one run's
/// node rows (about 1 KB per node) or a flush of buffered events, a few MB even for thousands of
/// nodes; the cap leaves an order of magnitude of headroom and bounds what an authorized client
/// can make the server buffer.
pub const ENGINE_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// The body as JSON (`null` for an empty body), read up to [`ENGINE_BODY_LIMIT`] (`413` beyond).
/// Callers authorize first.
async fn body_json(body: Body) -> Result<Json, ApiError> {
    let bytes = axum::body::to_bytes(body, ENGINE_BODY_LIMIT)
        .await
        .map_err(|_| {
            ApiError::new(
                413,
                format!(
                    "The request body exceeds the engine routes' limit of {} MiB.",
                    ENGINE_BODY_LIMIT >> 20
                ),
            )
        })?;
    Ok(parse_json_body(&bytes)?.unwrap_or(Json::Null))
}

fn query_from(j: Json) -> Result<EnginePipeQuery, ApiError> {
    if j.is_null() {
        return Ok(EnginePipeQuery::default());
    }
    serde_json::from_value(j.clone()).map_err(|e| {
        ApiError::validation(vec![ApiError::field_error(
            "model_type",
            &["body"],
            &e.to_string(),
            j,
            None,
        )])
    })
}

/// `"1 row"` / `"2 rows"`.
fn count(n: usize, singular: &str, plural: &str) -> String {
    format!("{n} {}", if n == 1 { singular } else { plural })
}

async fn read_pipe(ctx: Ctx, Path(metric): Path<String>, body: Body) -> ApiResult {
    engine_metric(&ctx, &metric, READ_SCOPES).await?;
    let query = query_from(body_json(body).await?)?;
    let df = ctx
        .state
        .engine
        .read_pipe(&metric, &query)
        .await
        .map_err(store_error)?;
    Ok(match df {
        Some(df) => {
            let message = format!("Read {}.", count(df.height(), "row", "rows"));
            envelope::ok_with_raw(&message, "rows", &engine_pipes::records_json(&df))
        }
        None => envelope::ok_with("Read 0 rows.", "rows", json!([])),
    })
}

async fn write_pipe(ctx: Ctx, Path(metric): Path<String>, body: Body) -> ApiResult {
    engine_metric(&ctx, &metric, SCOPES).await?;
    let j = body_json(body).await?;
    let Some(items) = j.as_array() else {
        return Err(ApiError::missing(&["body"]));
    };
    let docs = engine_pipes::docs_from_records(items);
    // An engine on an `api:` instance flushes its events here: its runs' streams get them live.
    let events = (metric == "engine_events").then(|| docs.clone());
    let res = ctx
        .state
        .engine
        .write_pipe(&metric, docs)
        .await
        .map_err(store_error)?;
    if let Some(docs) = events
        && res.success
    {
        // `/engine/pipes/*` keep their rows on the server's own instance.
        let instance = ctx.state.instance().connector_keys();
        ctx.state.engine_streams.publish_docs(&instance, &docs);
    }
    envelope::tuple(&res, 500)
}

/// `GET /engine/runs/{run_id}/events/stream` ([`crate::engine_stream`]).
async fn stream_run_events(ctx: Ctx, Path(run_id): Path<String>) -> ApiResult {
    crate::engine_stream::serve(ctx, run_id).await
}

async fn clear_pipe(ctx: Ctx, Path(metric): Path<String>, body: Body) -> ApiResult {
    engine_metric(&ctx, &metric, SCOPES).await?;
    let query = query_from(body_json(body).await?)?;
    bounded_clear(&metric, &query)?;
    let res = ctx
        .state
        .engine
        .clear_pipe(&metric, &query)
        .await
        .map_err(store_error)?;
    envelope::tuple(&res, 500)
}

/// The state pipes whose rows age out by time: an `end`-only clear is their retention
/// (`InstanceState::prune_history`). Each has a datetime column for `end` to bound.
const RETENTION_METRICS: &[&str] = &["engine_runs", "engine_nodes", "engine_events"];

/// Whether a `params` value selects rows positively: a scalar (not `null`, `"None"` or a
/// `_`-prefixed negation) or a non-empty list of them. The engine only ever sends pipe refs,
/// run IDs and sequence numbers.
fn positive_selector(v: &meerschaum_core::Value) -> bool {
    use meerschaum_core::Value;
    let scalar = |v: &Value| match v {
        Value::Str(s) => !s.is_empty() && !s.starts_with('_') && s != "None",
        Value::Int(_) | Value::Float(_) | Value::Bool(_) => true,
        _ => false,
    };
    match v {
        Value::List(items) => !items.is_empty() && items.iter().all(scalar),
        other => scalar(other),
    }
}

/// Never wipe a state table: a clear must name its rows with `params` over the metric's own
/// columns (a pipe's ledger, cursors or hash; an unknown column would be dropped by the
/// connector and the `WHERE` would match everything), or, for the retention metrics only,
/// bound them with `end`.
fn bounded_clear(metric: &str, query: &EnginePipeQuery) -> Result<(), ApiError> {
    let refuse = |why: &str| {
        Err(ApiError::new(
            400,
            format!("Refusing to clear '{metric}': {why}"),
        ))
    };
    let columns = engine_pipes::engine_dtypes_context(metric)
        .map(|c| c.dtypes)
        .unwrap_or_default();
    match query.params_map().filter(|p| !p.is_empty()) {
        Some(params) => {
            if let Some(k) = params.keys().find(|k| !columns.contains_key(*k)) {
                return refuse(&format!("`{k}` is not a column of this state pipe."));
            }
            if let Some(k) = params
                .iter()
                .find(|(_, v)| !positive_selector(v))
                .map(|(k, _)| k)
            {
                return refuse(&format!(
                    "`{k}` must select rows by value (no null, `None` or `_` negation)."
                ));
            }
            Ok(())
        }
        None if query.end.is_some() && RETENTION_METRICS.contains(&metric) => Ok(()),
        None if query.end.is_some() => {
            refuse("an `end`-only clear applies to runs, nodes and events only.")
        }
        None => refuse("`params` (or, for runs, nodes and events, `end`) is required."),
    }
}

async fn pipe_exists(ctx: Ctx, Path(metric): Path<String>) -> ApiResult {
    engine_metric(&ctx, &metric, READ_SCOPES).await?;
    let exists = ctx
        .state
        .engine
        .pipe_exists(&metric)
        .await
        .map_err(store_error)?;
    let message = if exists {
        format!("The table of '{metric}' exists.")
    } else {
        format!("The table of '{metric}' does not exist.")
    };
    Ok(envelope::ok_with(message, "exists", json!(exists)))
}

async fn body_as<T: DeserializeOwned>(ctx: &Ctx, body: Body) -> Result<T, ApiError> {
    scoped_auth(&ctx.state, &ctx.headers, SCOPES).await?;
    let j = body_json(body).await?;
    if j.is_null() {
        return Err(ApiError::missing(&["body"]));
    }
    serde_json::from_value(j.clone()).map_err(|e| {
        ApiError::validation(vec![ApiError::field_error(
            "model_type",
            &["body"],
            &e.to_string(),
            j,
            None,
        )])
    })
}

async fn post_ledger(ctx: Ctx, body: Body) -> ApiResult {
    let entries: Vec<LedgerEntry> = body_as(&ctx, body).await?;
    envelope::tuple(&ctx.state.engine.append_ledger(entries).await, 500)
}

async fn get_ledger(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, READ_SCOPES).await?;
    let Some(child_ref) = ctx.query.opt_str("child_ref") else {
        return Err(ApiError::missing(&["query", "child_ref"]));
    };
    let after_seq = ctx.query.opt_int("after_seq")?;
    let entries = ctx.state.engine.ledger_after(&child_ref, after_seq).await;
    Ok(envelope::ok_with(
        format!(
            "Found {}.",
            count(entries.len(), "ledger entry", "ledger entries")
        ),
        "entries",
        serde_json::to_value(entries).unwrap_or(Json::Null),
    ))
}

async fn put_cursors(ctx: Ctx, body: Body) -> ApiResult {
    let cursors: Vec<EdgeCursor> = body_as(&ctx, body).await?;
    envelope::tuple(&ctx.state.engine.put_cursors(cursors).await, 500)
}

async fn get_state(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, READ_SCOPES).await?;
    let Some(pipe_ref) = ctx.query.opt_str("pipe_ref") else {
        return Err(ApiError::missing(&["query", "pipe_ref"]));
    };
    match ctx.state.engine.get_state(&pipe_ref).await {
        Some(state) => Ok(envelope::ok_with(
            format!("Found the engine state of '{pipe_ref}'."),
            "state",
            serde_json::to_value(state).unwrap_or(Json::Null),
        )),
        None => Err(ApiError::new(
            404,
            format!("No engine state for '{pipe_ref}'."),
        )),
    }
}

async fn put_state(ctx: Ctx, body: Body) -> ApiResult {
    let states: Vec<EngineState> = body_as(&ctx, body).await?;
    envelope::tuple(&ctx.state.engine.put_states(states).await, 500)
}

async fn post_runs(ctx: Ctx, body: Body) -> ApiResult {
    let runs: Vec<RunRecord> = body_as(&ctx, body).await?;
    envelope::tuple(&ctx.state.engine.record_runs(runs).await, 500)
}

/// Parse an ISO 8601 timestamp (naive means UTC), or `400`.
fn parse_since(text: &str) -> Result<chrono::DateTime<chrono::Utc>, ApiError> {
    let invalid = || {
        ApiError::new(
            400,
            format!("`since` must be an ISO 8601 datetime, got '{text}'."),
        )
    };
    let value = meerschaum_kernel::value::parse_datetime_value(text.trim()).ok_or_else(invalid)?;
    Ok(chrono::DateTime::from_naive_utc_and_offset(
        value.utc,
        chrono::Utc,
    ))
}

/// The [`RunsFilter`] of a request's query (`pipeline`, `pipe`, `status`, `since`, `limit`,
/// `run_id`).
pub fn runs_filter(ctx: &Ctx) -> Result<RunsFilter, ApiError> {
    let status = match ctx.query.opt_str("status") {
        None => None,
        Some(s) if s.trim().is_empty() || s.trim().eq_ignore_ascii_case("any") => None,
        Some(s) => Some(RunStatusFilter::parse(&s).ok_or_else(|| {
            ApiError::new(
                400,
                format!("`status` must be 'success', 'failed' or 'running', got '{s}'."),
            )
        })?),
    };
    let limit = match ctx
        .query
        .opt_int("limit")
        .map_err(|_| ApiError::new(400, "`limit` must be a whole number."))?
    {
        None => DEFAULT_RUNS,
        Some(n) if n < 1 => return Err(ApiError::new(400, "`limit` must be at least 1.")),
        Some(n) => usize::try_from(n).unwrap_or(MAX_RUNS).min(MAX_RUNS),
    };
    Ok(RunsFilter {
        run_id: ctx.query.opt_str("run_id").filter(|s| !s.is_empty()),
        pipeline: ctx.query.opt_str("pipeline").filter(|s| !s.is_empty()),
        pipe_ref: ctx.query.opt_str("pipe").filter(|s| !s.is_empty()),
        status,
        since: ctx
            .query
            .opt_str("since")
            .filter(|s| !s.trim().is_empty())
            .map(|s| parse_since(&s))
            .transpose()?,
        limit: Some(limit),
    })
}

async fn get_runs(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, READ_SCOPES).await?;
    let filter = runs_filter(&ctx)?;
    let (store, _) = crate::engine_stream::store_for(&ctx)?;
    let runs = store.get_runs(&filter).await.map_err(store_error)?;
    let n = runs.iter().filter(|r| r.pipe_ref.is_none()).count();
    Ok(envelope::ok_with(
        format!("Found {}.", count(n, "run", "runs")),
        "runs",
        serde_json::to_value(runs).unwrap_or(Json::Null),
    ))
}

/// One run and its nodes, `None` if the store does not know it.
pub async fn run_detail(
    store: &dyn crate::backends::EngineBackend,
    run_id: &str,
) -> Result<Option<(RunRecord, Vec<RunRecord>)>, EngineStoreError> {
    let filter = RunsFilter {
        run_id: Some(run_id.to_string()),
        limit: Some(1),
        ..Default::default()
    };
    let rows = store.get_runs(&filter).await?;
    let (nodes, runs): (Vec<RunRecord>, Vec<RunRecord>) =
        rows.into_iter().partition(|r| r.pipe_ref.is_some());
    Ok(runs.into_iter().next().map(|run| (run, nodes)))
}

async fn get_run(ctx: Ctx, Path(run_id): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, READ_SCOPES).await?;
    let (store, _) = crate::engine_stream::store_for(&ctx)?;
    let Some((run, nodes)) = run_detail(store.as_ref(), &run_id)
        .await
        .map_err(store_error)?
    else {
        return Err(ApiError::new(404, format!("Run '{run_id}' was not found.")));
    };
    let mut fields = JsonMap::new();
    let message = format!(
        "Run '{run_id}' is {} ({}).",
        match run.status.as_str() {
            "success" => "a success",
            "failed" => "a failure",
            _ => "running",
        },
        count(nodes.len(), "pipe", "pipes")
    );
    fields.insert(
        "run".into(),
        serde_json::to_value(&run).unwrap_or(Json::Null),
    );
    fields.insert(
        "nodes".into(),
        serde_json::to_value(&nodes).unwrap_or(Json::Null),
    );
    Ok(envelope::ok(message, fields))
}

async fn post_events(ctx: Ctx, body: Body) -> ApiResult {
    let events: Vec<EngineEvent> = body_as(&ctx, body).await?;
    envelope::tuple(&ctx.state.engine.record_events(events).await, 500)
}

/// A lease answer: `200` when done, `409` when the holder does not (or may not) hold it.
fn lease_response(ok: bool, done: String, refused: String) -> ApiResult {
    if ok {
        Ok(envelope::ok(done, JsonMap::new()))
    } else {
        Err(ApiError::new(409, refused))
    }
}

async fn acquire_lease(ctx: Ctx, body: Body) -> ApiResult {
    let req: LeaseRequest = body_as(&ctx, body).await?;
    let ok = ctx
        .state
        .engine
        .acquire_lease(
            &req.pipe_ref,
            &req.holder,
            Duration::from_millis(req.ttl_ms),
        )
        .await;
    lease_response(
        ok,
        format!("Acquired the lease on '{}'.", req.pipe_ref),
        format!(
            "Could not acquire the lease on '{}': another process holds it.",
            req.pipe_ref
        ),
    )
}

async fn release_lease(ctx: Ctx, body: Body) -> ApiResult {
    let req: LeaseRequest = body_as(&ctx, body).await?;
    let ok = ctx
        .state
        .engine
        .release_lease(&req.pipe_ref, &req.holder)
        .await;
    lease_response(
        ok,
        format!("Released the lease on '{}'.", req.pipe_ref),
        format!(
            "Could not release the lease on '{}': '{}' does not hold it.",
            req.pipe_ref, req.holder
        ),
    )
}
