//! Automatic execution workers (`POST /execute`), terminal Exec validation, and retained runs.
//!
//! Pipe management and Pipeline runs create jobs through the shared supervisor and follow SSE
//! logs. Dashboard Exec instead validates policy at `/execute/validate` and types into its
//! existing terminal. It never starts a second worker.
//!
//! Every route answers in the v5 shape (`{"success": bool, "message": str, ...}` with real
//! status codes, [`crate::envelope`]). `POST /execute` names its job's log stream
//! (`/logs/{name}/stream`) only when this server serves it, and otherwise `null` with the
//! log routes that remain (`logs`: `/logs/{name}`, when the `jobs` route group is served).
//!
//! Explicit `mrsm_execution` metadata identifies temporary workers independently of their names.
//! Creator/administrator visibility applies to metadata and every log transport. Legacy
//! `execute-*` records retain that visibility rule. Successful output lasts 24 hours, failed
//! output 7 days, and archived summaries 30 days by default. Startup and periodic cleanup leave
//! active work untouched; a completed worker may be promoted into a persistent Job.

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::routing::{get, post};
use meerschaum_core::{Map, Value};
use serde_json::{Value as Json, json};

use super::jobs::clean_sysargs;
use super::{Ctx, body_bytes};
use crate::auth::{Principal, is_user_allowed_to_execute_console, scoped_auth_if_private};
use crate::backends::CreateJob;
use crate::config::ServerConfig;
use crate::envelope;
use crate::error::{ApiError, ApiResult};
use crate::routes::pipes::parse_json_body;
use crate::state::AppState;

/// Register `POST /execute` (part of the `actions` route group).
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/execute", post(execute))
        .route("/execute/policy", get(policy))
        .route("/execute/validate", post(validate_terminal_command))
        .route("/execute/runs", get(run_summaries))
        .route("/execute/runs/{name}/retain", post(retain_run))
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
    let temporary = meta
        .and_then(|m| m.pointer("/daemon/properties/mrsm_execution/kind"))
        .and_then(Json::as_str)
        .map(|k| k == "temporary_execution")
        .unwrap_or_else(|| name.starts_with(JOB_PREFIX));
    if admin || !temporary {
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
    let policy = crate::execute_policy::policy_json();
    let fields = policy.as_object().cloned().unwrap_or_default();
    Ok(envelope::ok("The console's execute policy.", fields))
}

/// Authorize terminal Exec without starting another execution worker.
async fn validate_terminal_command(ctx: Ctx, body: Body) -> ApiResult {
    let caller = crate::access::require(
        &ctx.state,
        &ctx.headers,
        &["actions:execute", "jobs:execute", "jobs:write"],
    )
    .await?;
    let principal = caller.auth.clone();
    if !caller.no_auth && !caller.full_admin() {
        let allowed = is_user_allowed_to_execute_console(&ctx.state, principal.as_ref()).await;
        if !allowed.success {
            return Err(ApiError::new(403, allowed.message));
        }
    }
    let body = parse_json_body(&body_bytes(body).await?)?.unwrap_or(Json::Null);
    let items = body
        .get("sysargs")
        .and_then(Json::as_array)
        .ok_or_else(|| ApiError::missing(&["body", "sysargs"]))?;
    let args = items
        .iter()
        .map(|value| value.as_str().map(str::to_string))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| ApiError::new(422, "sysargs must contain strings"))?;
    if args.is_empty() {
        return Err(ApiError::new(422, "sysargs is empty"));
    }
    if !caller.no_auth && !caller.full_admin() {
        crate::execute_policy::check_non_admin(&args)
            .map_err(|message| ApiError::new(403, message))?;
        // The pipes the command selects (rc15 access model).
        crate::execute_access::check(&ctx.state, &caller, &args).await?;
    }
    Ok(envelope::ok(
        "You may run this command.",
        serde_json::Map::new(),
    ))
}

/// Retained summaries are owner filtered and redact the same secrets as Job metadata.
async fn run_summaries(ctx: Ctx) -> ApiResult {
    let access = crate::access::require(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let mut records = Vec::new();
    if let Ok(files) = std::fs::read_dir(ctx.state.config.root_dir.join("runs")) {
        for file in files.flatten() {
            if file.path().extension().and_then(|v| v.to_str()) != Some("json") {
                continue;
            }
            let Some(mut record) = std::fs::read(file.path())
                .ok()
                .and_then(|v| serde_json::from_slice::<Json>(&v).ok())
            else {
                continue;
            };
            let Some(name) = record["name"].as_str() else {
                continue;
            };
            if !super::jobs::can_see(&access, name, record.get("metadata")) {
                continue;
            }
            if let Some(meta) = record.get_mut("metadata") {
                crate::redact::redact_job_meta(meta);
            }
            records.push(record);
        }
    }
    records.sort_by(|a, b| b["ended"].as_str().cmp(&a["ended"].as_str()));
    let message = format!(
        "Found {} retained execution{}.",
        records.len(),
        if records.len() == 1 { "" } else { "s" }
    );
    Ok(envelope::ok_with(message, "runs", json!(records)))
}

async fn retain_run(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let _maintenance = ctx.state.execution_maintenance.lock().await;
    let access = crate::access::require(&ctx.state, &ctx.headers, &["jobs:write"]).await?;
    let meta = ctx
        .state
        .jobs
        .get(&name)
        .await
        .ok_or_else(|| ApiError::new(404, format!("Execution '{name}' was not found.")))?;
    if !super::jobs::can_see(&access, &name, Some(&meta)) {
        return Err(ApiError::new(
            404,
            format!("Execution '{name}' was not found."),
        ));
    }
    if meta["status"] != "stopped" {
        return Err(ApiError::new(
            409,
            "Wait until the execution finishes before promoting it.",
        ));
    }
    envelope::tuple(&ctx.state.jobs.promote(&name).await, 500)
}

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

/// Explicit execution metadata, with compatibility for legacy console workers.
pub fn is_temporary(name: &str, meta: &Json) -> bool {
    meta.pointer("/daemon/properties/mrsm_execution/kind")
        .and_then(Json::as_str)
        .map(|kind| kind == "temporary_execution")
        .unwrap_or_else(|| name.starts_with(JOB_PREFIX))
}

/// Archive output before removing a completed worker. Active and paused work never expires.
pub async fn prune_finished(state: &AppState) {
    let _maintenance = state.execution_maintenance.lock().await;
    let now = chrono::Utc::now();
    let archive = state.config.root_dir.join("runs");
    if let Err(error) = std::fs::create_dir_all(&archive) {
        tracing::warn!(%error, "Unable to create execution archive");
        return;
    }
    for (name, meta) in state.jobs.list().await {
        if !is_temporary(&name, &meta) || meta["status"] != "stopped" || !valid_name(&name) {
            continue;
        }
        // The listing may precede a restart or promotion. Re-read under maintenance lock.
        let Some(meta) = state.jobs.get(&name).await else {
            continue;
        };
        if meta["status"] != "stopped" || !is_temporary(&name, &meta) {
            continue;
        }
        let Some(stopped) = state.jobs.completion_time(&name).await else {
            continue;
        };
        // Keep the worker until its output expires. Summaries outlive worker deletion.
        let success = meta
            .pointer("/result/0")
            .and_then(Json::as_bool)
            .or_else(|| meta.pointer("/result/success").and_then(Json::as_bool))
            .unwrap_or(false);
        let keep = state.config.execution_retention[usize::from(!success)];
        let age = (now - stopped).num_seconds().max(0) as u64;
        if age < keep {
            continue;
        }
        let record = json!({"name": name, "metadata": meta, "ended": stopped,
            "summary_expires": stopped.timestamp().saturating_add(state.config.execution_retention[2] as i64)});
        let path = archive.join(format!("{name}.json"));
        let temporary = archive.join(format!("{name}.json.tmp"));
        if std::fs::write(&temporary, record.to_string()).is_ok()
            && std::fs::rename(&temporary, &path).is_ok()
        {
            let _ = state.jobs.delete(&name).await;
        }
    }
    if let Ok(files) = std::fs::read_dir(&archive) {
        for file in files.flatten() {
            if file.path().extension().and_then(|v| v.to_str()) != Some("json") {
                continue;
            }
            let record = std::fs::read(file.path())
                .ok()
                .and_then(|v| serde_json::from_slice::<Json>(&v).ok());
            if record
                .as_ref()
                .and_then(|v| v["summary_expires"].as_i64())
                .is_some_and(|v| v <= now.timestamp())
            {
                let _ = std::fs::remove_file(file.path());
            }
        }
    }
}

/// Run immediately at server startup and periodically, independently of new executions.
pub fn start_cleanup(state: &AppState) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let upgrade = state.weak_upgrade();
        runtime.spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                let Some(state) = upgrade() else { break };
                prune_finished(&state).await;
            }
        });
    }
}

/// `POST /execute`. The console's pipe operations (Sync, Verify, Clear, Drop, Delete of pipes
/// and Pipelines) need `jobs:execute`, the operation's permission and edit rights on the pipes
/// they select; any other action also needs `actions:execute` and `jobs:write` (rc15,
/// [`crate::execute_access`]).
async fn execute(ctx: Ctx, body: Body) -> ApiResult {
    let body = parse_json_body(&body_bytes(body).await?)?;
    let caller = crate::access::require(&ctx.state, &ctx.headers, &["jobs:execute"]).await?;
    let principal = caller.auth.clone();
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
        None | Some(Json::Null) => new_execution_name(),
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
    let plan = crate::execute_access::check(&ctx.state, &caller, &sysargs).await?;
    if plan.needs_actions {
        caller.require(&["actions:execute", "jobs:write"], &[])?;
        if !caller.no_auth && !caller.full_admin() {
            let allowed = is_user_allowed_to_execute_console(&ctx.state, principal.as_ref()).await;
            if !allowed.success {
                return Err(ApiError::new(403, allowed.message));
            }
        }
    } else if !caller.no_auth && !caller.is_admin() && !ctx.state.config.actions_non_admin {
        return Err(ApiError::new(
            403,
            "The administrator for this server has not allowed users to perform actions.",
        ));
    }
    if !caller.no_auth && !caller.full_admin() {
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
    let execution_id = uuid::Uuid::new_v4().to_string();
    let metadata = json!({
        "origin": "api_execute",
        "pipeline": object.get("pipeline"),
        "run_id": object.get("run_id").cloned().unwrap_or_else(|| json!(execution_id)),
        "parent_job": object.get("parent_job"),
    });
    let result = start_execution(&ctx.state, principal.as_ref(), &name, &sysargs, metadata).await?;
    let mut fields = log_routes(&ctx.state, &name);
    fields.insert("job".into(), json!(name));
    fields.insert("sysargs".into(), json!(sysargs));
    if result.success {
        Ok(envelope::ok(result.message, fields))
    } else {
        Err(ApiError::new(500, result.message))
    }
}

/// Start `sysargs` as a temporary execution job `name` owned by the caller (the jobs
/// `POST /execute` and `POST /pipelines/{name}/sync` start): retention as configured, `metadata`
/// merged into the job's `mrsm_execution` property. `409` when the job exists.
pub async fn start_execution(
    state: &AppState,
    principal: Option<&Principal>,
    name: &str,
    sysargs: &[String],
    metadata: Json,
) -> Result<meerschaum_core::SuccessTuple, ApiError> {
    prune_finished(state).await;
    let mut properties = Map::new();
    let owner = owner_label(principal);
    // The run is the caller's (`owner`, `mrsm_owner`), in the caller's primary team (`team`).
    let access = super::jobs::access_of_principal(state, principal).await?;
    super::jobs::stamp_caller(&mut properties, &access);
    if let Some(owner) = owner.as_ref().filter(|_| access.username().is_none()) {
        // A token without an owner keeps its `token:<id>` label.
        properties.insert(OWNER_KEY.into(), Value::Str(owner.clone()));
    }
    let mut execution = json!({
        "kind": "temporary_execution",
        "origin": "api_execute",
        "owner": owner,
        "retention": {"success_seconds": state.config.execution_retention[0],
            "failure_seconds": state.config.execution_retention[1],
            "summary_seconds": state.config.execution_retention[2]}
    });
    if let (Some(out), Json::Object(extra)) = (execution.as_object_mut(), metadata) {
        for (key, value) in extra {
            if key != "kind" && key != "owner" && key != "retention" {
                out.insert(key, value);
            }
        }
    }
    properties.insert(
        "mrsm_execution".into(),
        Value::from_json_str(&execution.to_string()).unwrap_or(Value::Null),
    );
    match state
        .jobs
        .create(name, sysargs.to_vec(), Some(properties))
        .await
    {
        CreateJob::Exists => Err(ApiError::new(
            409,
            format!("{} already exists.", state.jobs.job_str(name)),
        )),
        CreateJob::Started(result) => Ok(result),
    }
}

/// The routes that follow job `name`'s output: `stream` (`/logs/{name}/stream`, Server-Sent
/// Events) when this server serves it, else `null`; `logs` (`/logs/{name}`) when the `jobs` route
/// group is served, else `null`.
pub fn log_routes(state: &AppState, name: &str) -> serde_json::Map<String, Json> {
    let mut fields = serde_json::Map::new();
    let quoted = crate::json::quote_segment(name);
    fields.insert(
        "stream".into(),
        if state.log_stream_served() {
            json!(format!("/logs/{quoted}/stream"))
        } else {
            Json::Null
        },
    );
    fields.insert(
        "logs".into(),
        if state.config.route_group_is_allowed("jobs") {
            json!(format!("/logs/{quoted}"))
        } else {
            Json::Null
        },
    );
    fields
}

/// A new temporary execution job's name (`execute-<8 hex>`).
pub fn new_execution_name() -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{JOB_PREFIX}{}", &id[..8])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_kind_and_owner_do_not_depend_on_name() {
        let temporary = json!({"daemon": {"properties": {
            "mrsm_owner": "alice", "mrsm_execution": {"kind": "temporary_execution"}
        }}});
        assert!(is_temporary("custom-name", &temporary));
        assert!(!job_visible_to(
            false,
            Some("bob"),
            "custom-name",
            Some(&temporary)
        ));
        assert!(job_visible_to(
            false,
            Some("alice"),
            "custom-name",
            Some(&temporary)
        ));
        assert!(job_visible_to(true, None, "custom-name", Some(&temporary)));
        let kept =
            json!({"daemon": {"properties": {"mrsm_execution": {"kind": "persistent_job"}}}});
        assert!(!is_temporary("execute-kept", &kept));
        assert!(is_temporary("execute-legacy", &json!({})));
        assert!(!is_temporary("user-job", &json!({})));
    }
}
