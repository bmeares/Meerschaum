//! The `/engine/*` route family (§5.8, new in v5): engine state for API instances, backed by an
//! [`EngineBackend`](crate::backends::EngineBackend). Every route requires `pipes:write`.
//! Write routes answer with a `SuccessTuple`, the lease routes with `{"success": bool}`.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::routing::{get, post};
use meerschaum_connector_api::models::{
    EdgeCursor, EngineEvent, EngineState, LeaseRequest, LedgerEntry, RunRecord,
};
use serde::de::DeserializeOwned;
use serde_json::{Value as Json, json};

use super::{Ctx, body_bytes};
use crate::auth::scoped_auth;
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{ok_json, tuple_response};
use crate::routes::pipes::parse_json_body;
use crate::state::AppState;

const SCOPES: &[&str] = &["pipes:write"];

/// Register the `engine` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/engine/ledger", post(post_ledger).get(get_ledger))
        .route("/engine/cursors", axum::routing::put(put_cursors))
        .route("/engine/state", get(get_state).put(put_state))
        .route("/engine/runs", post(post_runs).get(get_runs))
        .route("/engine/events", post(post_events))
        .route("/engine/leases", post(acquire_lease).delete(release_lease))
}

async fn body_as<T: DeserializeOwned>(ctx: &Ctx, body: Body) -> Result<T, ApiError> {
    let parsed = parse_json_body(&body_bytes(body).await?)?;
    scoped_auth(&ctx.state, &ctx.headers, SCOPES).await?;
    let Some(j) = parsed else {
        return Err(ApiError::missing(&["body"]));
    };
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
