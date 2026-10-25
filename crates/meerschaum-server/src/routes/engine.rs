//! The `/engine/*` route family (§5.8, new in v5): engine state for API instances, backed by an
//! [`EngineBackend`](crate::backends::EngineBackend) (`start api`: the server's instance,
//! [`crate::engine_store::InstanceEngine`]). The per-shape routes require `pipes:write`.
//! Write routes answer with a `SuccessTuple`, the lease routes with `{"success": bool}`.
//!
//! `/engine/pipes/{metric}/{read,write,clear,exists}` expose the engine's six state pipes
//! (`mrsm:engine_*`) to an `APIConnector`, whose engine keeps its state through them: reads
//! need `pipes:read`, writes `pipes:write`; any other metric is a `404` (no generic access to
//! `mrsm` keys). Rows travel as `orient='records'` JSON.
//!
//! `GET /engine/runs/{run_id}/events/stream` follows a run's events as Server-Sent Events
//! ([`crate::engine_stream`]).

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing::{get, post};
use meerschaum_connector_api::engine_pipes::{self, EnginePipeQuery};
use meerschaum_connector_api::models::{
    EdgeCursor, EngineEvent, EngineState, LeaseRequest, LedgerEntry, RunRecord,
};
use serde::de::DeserializeOwned;
use serde_json::{Value as Json, json};

use super::Ctx;
use crate::auth::scoped_auth;
use crate::backends::EngineStoreError;
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{ok_json, raw_json, tuple_response};
use crate::routes::pipes::parse_json_body;
use crate::state::AppState;

const SCOPES: &[&str] = &["pipes:write"];

/// Register the `engine` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/engine/ledger", post(post_ledger).get(get_ledger))
        .route("/engine/cursors", axum::routing::put(put_cursors))
        .route("/engine/state", get(get_state).put(put_state))
        .route("/engine/runs", post(post_runs).get(get_runs))
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

const READ_SCOPES: &[&str] = &["pipes:read"];

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
    match e {
        EngineStoreError::Unavailable => ApiError::new(501, e.to_string()),
        EngineStoreError::Failed(m) => ApiError::new(500, m),
    }
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
        Some(df) => raw_json(StatusCode::OK, engine_pipes::records_json(&df)),
        None => ok_json(&json!([])),
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
        ctx.state.engine_streams.publish_docs(&docs);
    }
    Ok(tuple_response(&res))
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
    Ok(tuple_response(&res))
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
    Ok(ok_json(&json!(exists)))
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
    Ok(tuple_response(
        &ctx.state.engine.append_ledger(entries).await,
    ))
}

async fn get_ledger(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, SCOPES).await?;
    let Some(child_ref) = ctx.query.opt_str("child_ref") else {
        return Err(ApiError::missing(&["query", "child_ref"]));
    };
    let after_seq = ctx.query.opt_int("after_seq")?;
    let entries = ctx.state.engine.ledger_after(&child_ref, after_seq).await;
    Ok(ok_json(
        &serde_json::to_value(entries).unwrap_or(Json::Null),
    ))
}

async fn put_cursors(ctx: Ctx, body: Body) -> ApiResult {
    let cursors: Vec<EdgeCursor> = body_as(&ctx, body).await?;
    Ok(tuple_response(&ctx.state.engine.put_cursors(cursors).await))
}

async fn get_state(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, SCOPES).await?;
    let Some(pipe_ref) = ctx.query.opt_str("pipe_ref") else {
        return Err(ApiError::missing(&["query", "pipe_ref"]));
    };
    let state = ctx.state.engine.get_state(&pipe_ref).await;
    Ok(ok_json(&serde_json::to_value(state).unwrap_or(Json::Null)))
}

async fn put_state(ctx: Ctx, body: Body) -> ApiResult {
    let states: Vec<EngineState> = body_as(&ctx, body).await?;
    Ok(tuple_response(&ctx.state.engine.put_states(states).await))
}

async fn post_runs(ctx: Ctx, body: Body) -> ApiResult {
    let runs: Vec<RunRecord> = body_as(&ctx, body).await?;
    Ok(tuple_response(&ctx.state.engine.record_runs(runs).await))
}

async fn get_runs(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, SCOPES).await?;
    let run_id = ctx.query.opt_str("run_id");
    let limit = ctx.query.opt_int("limit")?;
    let runs = ctx.state.engine.get_runs(run_id.as_deref(), limit).await;
    Ok(ok_json(&serde_json::to_value(runs).unwrap_or(Json::Null)))
}

async fn post_events(ctx: Ctx, body: Body) -> ApiResult {
    let events: Vec<EngineEvent> = body_as(&ctx, body).await?;
    Ok(tuple_response(
        &ctx.state.engine.record_events(events).await,
    ))
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
    Ok(ok_json(&json!({"success": ok})))
}

async fn release_lease(ctx: Ctx, body: Body) -> ApiResult {
    let req: LeaseRequest = body_as(&ctx, body).await?;
    let ok = ctx
        .state
        .engine
        .release_lease(&req.pipe_ref, &req.holder)
        .await;
    Ok(ok_json(&json!({"success": ok})))
}
