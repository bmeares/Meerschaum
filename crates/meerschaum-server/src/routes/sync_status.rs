//! The per-pipe sync status routes (rc15 contract C2), in the `engine` route group:
//!
//! - `GET /engine/sync-status` (`pipes:read`): the statuses recorded on the server's instance
//!   (`?instance_keys=` like the pipe routes), or one pipe's with `?pipe_ref=` (either the
//!   server's or a client's `api:<label>` pipe ref: only its connector, metric and location
//!   keys are read). `{"success": true, "message": "...", "statuses": [...]}`.
//! - `POST /engine/sync-status` (`pipes:write`): an `api:` client's sync outcomes for one pipe
//!   (`{"connector_keys", "metric_key", "location_key", "outcomes": [...]}`, oldest first),
//!   merged into that pipe's status on the server's instance. `{"success": true, "message":
//!   "...", "status": {...}}`.
//!
//! **Access (rc15).** A caller who is not an administrator reads the statuses of the pipes it can
//! see, and records outcomes only for pipes it may change.
//!
//! Statuses are keyed with the server's instance keys; the client re-keys them with its own
//! (`meerschaum_connector::sync_status::rekey`). The server also records the syncs posted to
//! `POST /pipes/{connector}/{metric}/{location}/data` ([`record_data_post`]) unless the client
//! marks the request as one it records itself
//! ([`meerschaum_connector::sync_status::CLIENT_RECORDS_HEADER`]: v5 clients do, so client-side
//! failures such as a source that cannot be fetched are recorded too, and nothing twice).
//! Errors answer in the v5 shape (`crate::envelope`).

use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector::sync_status::{
    self, CLIENT_RECORDS_HEADER, SyncOutcome, SyncSource, SyncStatus,
};
use meerschaum_core::{Pipe, SuccessTuple};
use serde::Deserialize;
use serde_json::Value as Json;

use super::Ctx;
use crate::access::{self, RefFilter};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::json_response;
use crate::state::AppState;

/// The route path.
pub const PATH: &str = "/engine/sync-status";

const READ_SCOPES: &[&str] = &["pipes:read"];
const WRITE_SCOPES: &[&str] = &["pipes:write"];

/// The largest `POST /engine/sync-status` body (a few outcomes of one pipe).
const BODY_LIMIT: usize = 1 << 20;

/// Register the sync status routes (the `engine` route group).
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route(PATH, get(get_statuses).post(post_outcomes))
}

fn ok_body(message: String, key: &str, value: Json) -> axum::response::Response {
    let mut body = serde_json::Map::new();
    body.insert("success".into(), Json::Bool(true));
    body.insert("message".into(), Json::String(message));
    body.insert(key.to_string(), value);
    json_response(StatusCode::OK, &Json::Object(body))
}

/// The pipe a `pipe_ref` names on the server's instance `instance_keys`: `ck|mk|lk` with or
/// without a leading instance (`None` for a location means no location).
fn pipe_from_ref(pipe_ref: &str, instance_keys: &str) -> Result<Pipe, ApiError> {
    let parts: Vec<&str> = pipe_ref.split('|').collect();
    let (ck, mk, lk) = match parts.as_slice() {
        [_, ck, mk, lk] => (*ck, *mk, Some(*lk)),
        [ck, mk, lk] => (*ck, *mk, Some(*lk)),
        [ck, mk] => (*ck, *mk, None),
        _ => {
            return Err(ApiError::new(
                400,
                format!(
                    "Invalid pipe_ref '{pipe_ref}': expected \
                     'instance|connector|metric|location'."
                ),
            ));
        }
    };
    keys_pipe(
        ck,
        mk,
        lk.filter(|l| !matches!(*l, "None" | "")),
        instance_keys,
    )
}

fn keys_pipe(ck: &str, mk: &str, lk: Option<&str>, instance_keys: &str) -> Result<Pipe, ApiError> {
    if ck.is_empty() || mk.is_empty() {
        return Err(ApiError::new(
            400,
            "Connector and metric keys are required.",
        ));
    }
    Pipe::builder(ck, mk)
        .location(lk)
        .instance(instance_keys)
        .build()
        .map_err(|e| ApiError::new(400, e.to_string()))
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "es" }
}

/// `GET /engine/sync-status[?pipe_ref=]`.
async fn get_statuses(ctx: Ctx) -> ApiResult {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, READ_SCOPES).await?;
    let ik = ctx.instance_keys();
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    access::guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let instance_keys = conn.connector_keys();
    let statuses: Vec<SyncStatus> = match ctx.query.opt_str("pipe_ref") {
        Some(pipe_ref) => {
            let pipe = pipe_from_ref(&pipe_ref, &instance_keys)?;
            conn.get_sync_status(&pipe)
                .await
                .map_err(|e| ApiError::exception("get_sync_status", e))?
                .into_iter()
                .collect()
        }
        None => conn
            .get_sync_statuses(&instance_keys)
            .await
            .map_err(|e| ApiError::exception("get_sync_statuses", e))?,
    };
    let statuses = if caller.is_admin() {
        statuses
    } else {
        let mut filter = RefFilter::new(&ctx.state, &caller, &instance_keys);
        let mut kept = Vec::with_capacity(statuses.len());
        for status in statuses {
            if access::guard::may_use_connector(&ctx.state, &caller, &status.connector_keys)
                && filter.visible(&status.pipe_ref).await
            {
                kept.push(status);
            }
        }
        kept
    };
    let n = statuses.len();
    Ok(ok_body(
        format!("Found {n} sync status{} on '{instance_keys}'.", plural(n)),
        "statuses",
        Json::Array(statuses.iter().map(SyncStatus::to_json).collect()),
    ))
}

/// The body of `POST /engine/sync-status`.
#[derive(Debug, Deserialize)]
struct PostBody {
    connector_keys: String,
    metric_key: String,
    #[serde(default)]
    location_key: Option<String>,
    outcomes: Vec<SyncOutcome>,
}

/// `POST /engine/sync-status`: record an `api:` client's sync outcomes.
async fn post_outcomes(ctx: Ctx, body: Body) -> ApiResult {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, WRITE_SCOPES).await?;
    let bytes = axum::body::to_bytes(body, BODY_LIMIT)
        .await
        .map_err(|_| ApiError::new(413, "The request body is too large."))?;
    let mut body: PostBody = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::new(400, format!("Invalid sync status body: {e}")))?;
    if body.outcomes.is_empty() {
        return Err(ApiError::new(400, "No sync outcomes were given."));
    }
    let ik = ctx.instance_keys();
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    access::guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let pipe = keys_pipe(
        &body.connector_keys,
        &body.metric_key,
        body.location_key.as_deref().filter(|l| *l != "None"),
        &conn.connector_keys(),
    )?;
    if !sync_status::is_recorded(&pipe) {
        return Err(ApiError::new(
            400,
            format!("{pipe} is an internal pipe and has no sync status."),
        ));
    }
    access::guard::check_connector_teams(&ctx.state, &caller, pipe.connector_keys())?;
    let mut filter = RefFilter::new(&ctx.state, &caller, &conn.connector_keys());
    match filter.editable(&pipe.pipe_ref()).await {
        (true, _) => {}
        _ if !filter.visible(&pipe.pipe_ref()).await => {
            return Err(ApiError::new(404, format!("{pipe} was not found.")));
        }
        _ => return Err(access::not_editable(&pipe)),
    }
    let attributes = conn
        .get_pipe_attributes(&pipe)
        .await
        .map_err(|_| ApiError::new(503, "Cannot read Pipe registration."))?;
    let current = sync_status::registration_from_attributes(&attributes);
    for outcome in &mut body.outcomes {
        // An old wire shape remains accepted, but never receives freshly minted proof.
        outcome.registration_captured = true;
        if outcome.registration_id.is_some() && outcome.registration_id != current {
            return Err(ApiError::new(
                409,
                "The Pipe registration or privacy segment changed since capture.",
            ));
        }
    }
    let status = sync_status::record_now(conn.as_ref(), &pipe, &body.outcomes)
        .await
        .map_err(|e| ApiError::exception("record_sync_status", e))?;
    let n = body.outcomes.len();
    Ok(ok_body(
        if status.is_none() {
            format!("Accepted {n} sync outcomes of {pipe}; no proven status was recorded.")
        } else {
            format!(
                "Recorded {n} sync{} of {pipe}.",
                if n == 1 { "" } else { "s" }
            )
        },
        "status",
        status.as_ref().map_or(Json::Null, SyncStatus::to_json),
    ))
}

/// Record a sync posted to `POST /pipes/{connector}/{metric}/{location}/data` in its pipe's
/// status (source `api`), unless the client records it itself (it sent
/// [`CLIENT_RECORDS_HEADER`]). Best effort and written in the background: it never changes or
/// delays the response.
pub fn record_data_post(
    headers: &HeaderMap,
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    result: &SuccessTuple,
    started: Instant,
    registration: Option<String>,
) {
    if headers.contains_key(CLIENT_RECORDS_HEADER) {
        return;
    }
    record_server_sync(conn, pipe, result, started, registration);
}

/// Record a sync the server ran itself on `conn` (a data post, the MCP tool `sync_documents`)
/// in its pipe's status, with source `api`: best effort and written in the background.
pub fn record_server_sync(
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    result: &SuccessTuple,
    started: Instant,
    registration: Option<String>,
) {
    let outcome = SyncOutcome::new(
        result.success,
        result.message.clone(),
        SyncSource::Api,
        None,
    )
    .with_message_counts()
    .with_duration(Some(started.elapsed()))
    .with_registration(registration);
    sync_status::record(conn.clone(), pipe, outcome);
}

/// `true` when a request's headers say its client records its own syncs.
pub fn client_records(headers: &HeaderMap) -> bool {
    headers.contains_key(CLIENT_RECORDS_HEADER)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn pipe_refs_in_either_form_name_the_server_pipe() {
        let a = pipe_from_ref("api:main|sql:src|weather|home", "sql:main").unwrap();
        assert_eq!(a.pipe_ref(), "sql:main|sql:src|weather|home");
        let b = pipe_from_ref("sql:src|weather|None", "sql:main").unwrap();
        assert_eq!(b.pipe_ref(), "sql:main|sql:src|weather|None");
        let c = pipe_from_ref("sql:src|weather", "sql:main").unwrap();
        assert_eq!(c.location_key(), None);
        assert!(pipe_from_ref("weather", "sql:main").is_err());
        assert!(pipe_from_ref("|weather|None", "sql:main").is_err());
    }

    #[test]
    fn the_post_body_takes_outcomes() {
        let body: PostBody = serde_json::from_value(json!({
            "connector_keys": "sql:src",
            "metric_key": "weather",
            "location_key": null,
            "outcomes": [{
                "success": false,
                "message": "boom",
                "finished": "2026-10-27T12:00:00.000000+00:00",
                "source": "job",
                "job": "weather-sync",
            }],
        }))
        .unwrap();
        assert_eq!(body.outcomes[0].source, SyncSource::Job);
        assert_eq!(body.outcomes[0].inserted, 0);
        assert_eq!(body.outcomes[0].job.as_deref(), Some("weather-sync"));
    }
}
