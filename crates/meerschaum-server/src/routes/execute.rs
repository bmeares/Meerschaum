//! `POST /execute` (v5, D21): run an action line as an ephemeral job. The Dash console's Execute
//! box calls it (server side, with the user's bearer) and the browser then follows the job's log
//! over Server-Sent Events (`GET /logs/{name}/stream`, served by `meerschaum-web`).
//!
//! The body is `{"sysargs": ["sync", "pipes", ...]}` (an optional `"name"` picks the job name,
//! else `execute-<8 hex>`). The job is an ordinary v5 job (`show jobs` lists it) started through
//! the same [`JobsBackend`](crate::backends::JobsBackend) as `POST /jobs/{name}`, with the
//! server's environment and `NONINTERACTIVE=1` (jobs never ask a question nobody can answer).
//!
//! A finished job is **kept** for [`KEEP_FINISHED`], not deleted on completion: the browser opens
//! the log stream after the response, and a quick action would be gone (with its output) before
//! it connects. Each call removes the `execute-*` jobs that stopped longer ago than that.
//!
//! Authorization is that of creating a job to run an action: `actions:execute`, `jobs:execute`
//! and `jobs:write`, plus the rule of who may run actions at all:
//! administrators always; other users only when `api:permissions:actions:non_admin` is true (its
//! default; `start api --secure` sets it false), the same key the console's own session check and
//! the terminal read. (v4's `/actions/{action}` and job routes read the unset
//! `system:api:permissions:actions:non_admin` instead; see PARITY.md.)
//!
//! What a non-administrator may run is limited to what the console offers
//! ([`execute_policy`](crate::execute_policy), published at `GET /execute/policy`); an
//! administrator keeps v4's `/actions` latitude. A job started here records its creator
//! (`properties.mrsm_owner`) and an `execute-*` job is visible to its creator and to
//! administrators only (`GET /jobs*`, `/logs/*`, the job actions; see [`job_visible_to`]).

use axum::Router;
use axum::body::Body;
use axum::routing::{get, post};
use meerschaum_core::{Map, Value};
use serde_json::{Value as Json, json};

use super::jobs::clean_sysargs;
use super::{Ctx, body_bytes};
use crate::auth::{
    Principal, is_admin, is_user_allowed_to_execute_console, scoped_auth, scoped_auth_if_private,
};
use crate::backends::CreateJob;
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{ok_json, tuple_response};
use crate::routes::pipes::parse_json_body;
use crate::state::AppState;

/// Register `POST /execute` (part of the `actions` route group).
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/execute", post(execute))
        .route("/execute/policy", get(policy))
}

/// The job property that records who started an `execute-*` job.
pub const OWNER_KEY: &str = "mrsm_owner";

/// The name prefix of the jobs `POST /execute` starts.
pub const JOB_PREFIX: &str = "execute-";

/// How a caller is recorded as the owner of a job: the user name, `token:<id>` for a token (none
/// for `--no-auth`, where everyone is an administrator).
pub fn owner_label(principal: Option<&Principal>) -> Option<String> {
    match principal? {
        Principal::User(user) => Some(user.username.clone()),
        Principal::Token(token) => Some(format!(
            "token:{}",
            token.id.map(|id| id.to_string()).unwrap_or_default()
        )),
    }
}

/// Whether the caller may see the job `name` (whose metadata is `meta`): an `execute-*` job is its
/// creator's and the administrators'; any other job (one nobody started through the console) is
/// visible as before. A job created before the owner was recorded has no owner and stays with the
/// administrators.
pub fn job_visible_to(admin: bool, caller: Option<&str>, name: &str, meta: Option<&Json>) -> bool {
    if admin || !name.starts_with(JOB_PREFIX) {
        return true;
    }
    let owner = meta
        .and_then(|m| m.pointer(&format!("/daemon/properties/{OWNER_KEY}")))
        .and_then(Json::as_str);
    owner.is_some() && caller == owner
}

/// `GET /execute/policy`: what the console may offer and what non-administrators may run
/// ([`crate::execute_policy`]). Not secret; behind the bearer only when the server is `--private`.
async fn policy(ctx: Ctx) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &[]).await?;
    Ok(ok_json(&crate::execute_policy::policy_json()))
}

/// How long a finished `execute-*` job (and its log) is kept.
pub const KEEP_FINISHED: std::time::Duration = std::time::Duration::from_secs(3600);

/// The longest job name accepted from a client.
const MAX_NAME: usize = 64;

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn invalid(loc: &[&str], kind: &str, message: &str, input: Json) -> ApiError {
    ApiError::validation(vec![ApiError::field_error(kind, loc, message, input, None)])
}

/// Delete the `execute-*` jobs that stopped more than [`KEEP_FINISHED`] ago.
async fn prune_finished(state: &AppState) {
    let cutoff = chrono::Utc::now() - KEEP_FINISHED;
    for (name, meta) in state.jobs.list().await {
        if !name.starts_with("execute-") || meta["status"] != "stopped" {
            continue;
        }
        if state
            .jobs
            .stop_time(&name)
            .await
            .is_some_and(|stopped| stopped < cutoff)
        {
            let _ = state.jobs.delete(&name).await;
        }
    }
}

async fn execute(ctx: Ctx, body: Body) -> ApiResult {
    let body = parse_json_body(&body_bytes(body).await?)?;
    let principal = scoped_auth(
        &ctx.state,
        &ctx.headers,
        &["actions:execute", "jobs:execute", "jobs:write"],
    )
    .await?;
    let object = match body {
        None => return Err(ApiError::missing(&["body"])),
        Some(Json::Object(object)) => object,
        Some(other) => {
            return Err(invalid(
                &["body"],
                "dict_type",
                "Input should be a valid dictionary",
                other,
            ));
        }
    };
    let sysargs: Vec<String> = match object.get("sysargs") {
        Some(Json::Array(items)) if items.iter().all(Json::is_string) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .collect(),
        Some(other) => {
            return Err(invalid(
                &["body", "sysargs"],
                "list_type",
                "Input should be a valid list of strings",
                other.clone(),
            ));
        }
        None => return Err(ApiError::missing(&["body", "sysargs"])),
    };
    let sysargs = clean_sysargs(&sysargs);
    if sysargs.is_empty() {
        return Err(invalid(
            &["body", "sysargs"],
            "too_short",
            "List should have at least 1 item after validation",
            json!([]),
        ));
    }
    let name = match object.get("name") {
        None | Some(Json::Null) => {
            let id = uuid::Uuid::new_v4().simple().to_string();
            format!("{JOB_PREFIX}{}", &id[..8])
        }
        Some(Json::String(name)) if valid_name(name) => name.clone(),
        Some(other) => {
            return Err(invalid(
                &["body", "name"],
                "string_pattern_mismatch",
                "Job names are 1 to 64 letters, digits, '-', '_' or '.'",
                other.clone(),
            ));
        }
    };
    let allowed = is_user_allowed_to_execute_console(&ctx.state, principal.as_ref()).await;
    if !allowed.success {
        return Ok(tuple_response(&allowed));
    }
    if !is_admin(&ctx.state, principal.as_ref()).await {
        // A non-administrator runs what the console offers, and only in `execute-*` jobs (the
        // family whose visibility is the creator's).
        if let Err(message) = crate::execute_policy::check_non_admin(&sysargs) {
            return Err(ApiError::new(403, message));
        }
        if !name.starts_with(JOB_PREFIX) {
            return Err(invalid(
                &["body", "name"],
                "string_pattern_mismatch",
                "A job name chosen by a non-administrator starts with 'execute-'",
                json!(name),
            ));
        }
    }
    prune_finished(&ctx.state).await;
    let properties = owner_label(principal.as_ref()).map(|owner| {
        let mut properties = Map::new();
        properties.insert(OWNER_KEY.into(), Value::Str(owner));
        properties
    });
    match ctx
        .state
        .jobs
        .create(&name, sysargs.clone(), properties)
        .await
    {
        CreateJob::Exists => Err(ApiError::new(
            409,
            format!("{} already exists.", ctx.state.jobs.job_str(&name)),
        )),
        CreateJob::Started(result) => Ok(ok_json(&json!({
            "success": result.success,
            "msg": result.message,
            "job": name,
            "sysargs": sysargs,
            "stream": format!("/logs/{name}/stream"),
        }))),
    }
}
