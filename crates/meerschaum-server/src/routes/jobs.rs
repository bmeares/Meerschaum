//! `/jobs/*`, `/logs/{name}` and the logs websocket `/logs/{name}/ws` (`api/routes/_jobs.py`),
//! delegated to a [`JobsBackend`](crate::backends::JobsBackend).
//!
//! **Who sees and controls a job** (the rule is
//! [`meerschaum_core::job_access`]): a job started through the API (this route group, `POST
//! /execute`, `POST /pipelines/{name}/sync`) or the console records its `owner` (the caller) and
//! `team` (the caller's primary team). Its team's members and administrators see it (and its
//! logs); jobs created locally (the CLI, Compose, systemd) are Everyone's; console runs are
//! private to their owner, their team's managers and administrators. A job the caller cannot see
//! answers as one that does not exist (`404`).
//!
//! Controlling a job needs the job permission of the operation on a job the caller can see:
//! `jobs:execute` to start (the role `editor` may start existing jobs), `jobs:stop`,
//! `jobs:pause`, `jobs:delete` (the role `operator`). Creating one runs an action on the server,
//! so it also follows the rule of `/actions` ([`is_user_allowed_to_execute`]: `actions:execute`,
//! administrators only under `--secure`), as does answering a job's prompt over the websocket.
//! Under `--secure` (`api:permissions:actions:non_admin: false`) only administrators control
//! jobs at all, as in the web console; everyone keeps reading what they may see.

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use meerschaum_core::job_access::{self, JobAccess};
use meerschaum_core::{Map, SuccessTuple};
use serde_json::{Value as Json, json};

use super::{Ctx, body_bytes};
use crate::access::{self, Access};
use crate::auth::is_user_allowed_to_execute;
use crate::backends::{CreateJob, LogEvent};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{from_core, ok_json, py_isoformat_utc, to_core, tuple_response};
use crate::redact::redact_job_meta;
use crate::routes::pipes::parse_json_body;
use crate::state::AppState;

/// Register the `jobs` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/jobs", get(get_jobs))
        .route(
            "/jobs/{name}",
            get(get_job).post(create_job).delete(delete_job),
        )
        .route("/jobs/{name}/exists", get(get_job_exists))
        .route("/jobs/{name}/start", post(start_job))
        .route("/jobs/{name}/stop", post(stop_job))
        .route("/jobs/{name}/pause", post(pause_job))
        .route("/jobs/{name}/stop_time", get(get_stop_time))
        .route(
            "/jobs/{name}/is_blocking_on_stdin",
            get(get_is_blocking_on_stdin),
        )
        // PARITY: `endpoints['jobs'] + '{name}/prompt_kwargs'` is missing a slash
        // (`api/routes/_jobs.py:281`), so the route is `/jobs{name}/prompt_kwargs`.
        .route("/jobs{name}/prompt_kwargs", get(get_prompt_kwargs))
        .route("/logs/{name}", get(get_logs))
        .route("/logs/{name}/ws", get(logs_websocket))
}

/// `clean_sysargs(sysargs)`: drop `-e` / `--executor-keys` / `api` and the argument after it.
pub fn clean_sysargs(sysargs: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut skip = false;
    for arg in sysargs {
        if matches!(arg.as_str(), "-e" | "--executor-keys" | "api") {
            skip = true;
            continue;
        }
        if skip {
            skip = false;
            continue;
        }
        out.push(arg.clone());
    }
    out
}

/// Main has already admitted the exact canonical command. Compatibility cleaning of an
/// ordinary API executor command would corrupt legitimate plugin words or data named `api`.
/// A client-supplied marker is insufficient: the request must also be gateway verified, and
/// the launch resolver still requires this job's active, exact signed context before spawn.
fn creation_sysargs(name: &str, sysargs: &[String], properties: Option<&Map>) -> Vec<String> {
    let canonical = name
        .strip_prefix("mrsm-")
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .filter(|id| !id.is_nil() && name == format!("mrsm-{}", id.simple()));
    let marker = properties
        .and_then(|properties| properties.get(crate::executor::environment::JOB_CONTEXT_PROPERTY))
        .and_then(meerschaum_core::Value::as_str)
        .and_then(|value| uuid::Uuid::parse_str(value).ok());
    if crate::executor::gateway::current().is_some() && canonical.is_some() && marker == canonical {
        return sysargs.to_vec();
    }
    clean_sysargs(sysargs)
}

/// What a non-administrator who tries to control a job hears under `--secure` (the text of
/// [`is_user_allowed_to_execute`]).
const SECURE_REFUSAL: &str =
    "The administrator for this server has not allowed users to perform actions.";

/// Record who starts a job through the API in its `properties`: the caller (`owner`) and the
/// caller's primary team (`team`). Whatever the request carried is replaced; under `--no-auth`
/// (and for a token without an owner) nobody is recorded and the job is Everyone's.
pub fn stamp_caller(properties: &mut Map, access: &Access) {
    // An executor's actor is verified for its own runtime, not a main registration identity.
    // Never adopt an identity marker supplied in client or executor properties.
    stamp_registration(properties, access);
    if let Some(verified) = crate::executor::gateway::current() {
        properties.shift_remove(job_access::OWNER_BINDING_KEY);
        properties.shift_remove(job_access::TEAM_ID_KEY);
        JobAccess::stamp(
            properties,
            Some(&verified.claims.actor.owner),
            Some(&verified.team_name),
        );
        return;
    }
    if access.no_auth {
        JobAccess::stamp(properties, None, None);
        return;
    }
    JobAccess::stamp(properties, access.username(), Some(&access.primary_team));
}

/// Private registered-owner evidence. Call before crossing blocking or spawned task boundaries.
/// It is independent of the password and is removed from every public metadata response.
pub fn stamp_registration(properties: &mut Map, access: &Access) {
    properties.shift_remove(job_access::OWNER_BINDING_KEY);
    properties.shift_remove(job_access::TEAM_ID_KEY);
    if let Some(binding) = crate::executor::callbacks::OwnerBinding::capture(access)
        && let Ok(json) = serde_json::to_value(binding)
    {
        properties.insert(job_access::OWNER_BINDING_KEY.into(), to_core(&json));
    }
    if access.no_auth {
        return;
    }
    if meerschaum_core::access::is_everyone(Some(&access.primary_team)) {
        properties.insert(job_access::TEAM_ID_KEY.into(), meerschaum_core::Value::Null);
    } else if let Some(id) = access
        .team_records
        .iter()
        .find(|team| {
            meerschaum_core::access::same_team(Some(&team.name), Some(&access.primary_team))
        })
        .and_then(|team| team.id.as_deref())
        .and_then(|id| uuid::Uuid::parse_str(id).ok())
        .filter(|id| !id.is_nil())
    {
        properties.insert(job_access::TEAM_ID_KEY.into(), id.to_string().into());
    }
}

/// Mark a job `access` starts as team execution (5.1,
/// [`meerschaum_core::job_access::TEAM_EXECUTION_KEY`]) when that is what lets them run it
/// ([`crate::execute_policy::SystemCommands::TeamOnly`]): its launch then requires the team's
/// sandbox and never runs it without the team's scope. The job's team is the caller's primary
/// team ([`stamp_caller`]), the team they act for.
pub async fn mark_team_execution(state: &AppState, properties: &mut Map, access: &Access) {
    if let crate::execute_policy::SystemCommands::TeamOnly { .. } =
        crate::execute_policy::may_run_system_commands(state, access).await
    {
        properties.insert(
            meerschaum_core::job_access::TEAM_EXECUTION_KEY.into(),
            meerschaum_core::Value::Bool(true),
        );
    }
}

/// Whether `access` may see the job `name` whose metadata (as the jobs routes serve it) is
/// `meta` (see [`meerschaum_core::job_access`]).
pub fn can_see(access: &Access, name: &str, meta: Option<&Json>) -> bool {
    if access.no_auth {
        return true;
    }
    let job = JobAccess::from_metadata(name, meta);
    let properties = meta
        .and_then(|m| m.get("daemon"))
        .and_then(|m| m.get("properties"));
    let team_id = properties
        .and_then(|p| p.get(job_access::TEAM_ID_KEY))
        .and_then(|id| {
            if id.is_null() {
                Some(None)
            } else {
                id.as_str()
                    .and_then(|id| uuid::Uuid::parse_str(id).ok())
                    .filter(|id| !id.is_nil())
                    .map(Some)
            }
        });
    let current_team = team_id.flatten().and_then(|id| {
        access.team_records.iter().find(|team| {
            team.id
                .as_deref()
                .and_then(|id| uuid::Uuid::parse_str(id).ok())
                == Some(id)
        })
    });
    // A derived execution token's effective team boundary precedes owner/admin shortcuts.
    if access.principal.team_boundary.is_some() {
        match team_id {
            Some(None) if access.principal.permits_team(None) => {}
            Some(Some(_))
                if current_team.is_some_and(|team| {
                    access.principal.permits_team(Some(&team.name))
                        && access.principal.is_member(Some(&team.name))
                }) => {}
            _ => return false,
        }
    }
    if access.is_admin() {
        return true;
    }
    let binding = properties
        .and_then(|p| p.get(job_access::OWNER_BINDING_KEY))
        .and_then(|value| {
            serde_json::from_value::<crate::executor::callbacks::OwnerBinding>(value.clone()).ok()
        });
    let owner = access
        .username()
        .map(str::to_string)
        .or_else(|| crate::routes::execute::owner_label(access.auth.as_ref()));
    if job.owner.is_some()
        && job.owner == owner
        && binding.is_some_and(|binding| binding.matches(access))
    {
        return true;
    }
    // Names on a legacy record cannot establish registration or team identity. Private
    // histories without team proof stay with administrators. A captured UUID that was deleted
    // cannot become Everyone's or be adopted by a replacement team reusing its display name.
    let mut principal = access.principal.clone();
    principal.username.clear();
    let mut projected = job;
    match team_id {
        Some(None) => projected.team = None,
        Some(Some(_)) => match current_team {
            Some(team) => projected.team = Some(team.name.clone()),
            None => return false,
        },
        None if projected.private => return false,
        None => {}
    }
    job_access::can_see_job(&principal, &projected)
}

/// Render a captured team UUID with its current display name. A missing registration retains
/// its historical label; deletion never turns retained history into Everyone's history.
pub fn project_team_names(meta: &mut Json, teams: &[meerschaum_core::access::Team]) {
    let Some(properties) = meta
        .get_mut("daemon")
        .and_then(|m| m.get_mut("properties"))
        .and_then(Json::as_object_mut)
    else {
        return;
    };
    let Some(id) = properties
        .get(job_access::TEAM_ID_KEY)
        .and_then(Json::as_str)
        .and_then(|id| uuid::Uuid::parse_str(id).ok())
    else {
        return;
    };
    if let Some(team) = teams.iter().find(|team| {
        team.id
            .as_deref()
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            == Some(id)
    }) {
        properties.insert(job_access::TEAM_KEY.into(), Json::String(team.name.clone()));
    }
}

pub(super) fn registered_team(meta: &Json) -> bool {
    meta.get("daemon")
        .and_then(|m| m.get("properties"))
        .and_then(|p| p.get(job_access::TEAM_ID_KEY))
        .and_then(Json::as_str)
        .and_then(|id| uuid::Uuid::parse_str(id).ok())
        .is_some_and(|id| !id.is_nil())
}

/// Who an authenticated principal is for job visibility and ownership (its name, teams and
/// primary team; `None`, as under `--no-auth`: everything), for the callers that authenticated
/// it already (MCP's job tools, `POST /execute` and `POST /pipelines/{name}/sync` runs).
pub async fn access_of_principal(
    state: &AppState,
    principal: Option<&crate::auth::Principal>,
) -> Result<Access, ApiError> {
    match principal {
        None => Ok(Access::everything()),
        Some(principal) => {
            let granted = crate::auth::Scopes::List(vec!["*".into()]);
            access::access_of(state, principal.clone(), &granted).await
        }
    }
}

/// Whether the caller may see the job `name`. A job that does not exist is not hidden: the route
/// answers as it always does.
pub async fn may_see(state: &AppState, access: &Access, name: &str) -> bool {
    if access.no_auth || access.is_admin() {
        return true;
    }
    match state.jobs.try_get(name).await {
        Ok(None) => true,
        Ok(Some(meta)) => can_see(access, name, Some(&meta)),
        Err(_) => false,
    }
}

/// The 404 of a job that is hidden from the caller (the same as one that does not exist).
fn hidden(state: &AppState, name: &str) -> ApiError {
    ApiError::new(404, format!("{} does not exist.", state.jobs.job_str(name)))
}

async fn get_jobs(ctx: Ctx) -> ApiResult {
    let access = access::require(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let mut jobs = ctx
        .state
        .jobs
        .try_list()
        .await
        .map_err(ApiError::unavailable)?;
    jobs.retain(|name, meta| can_see(&access, name, Some(meta)));
    let teams =
        if jobs.values().any(registered_team) {
            ctx.state.instance().get_teams().await.map_err(|_| {
                ApiError::unavailable("Could not resolve retained job team identities.")
            })?
        } else {
            Vec::new()
        };
    // PARITY: v4 returns every job's environment (connector URIs with their passwords and the
    // server's other secrets) to any `jobs:read` caller; v5 masks the credentials.
    for meta in jobs.values_mut() {
        project_team_names(meta, &teams);
        redact_job_meta(meta);
    }
    Ok(ok_json(&Json::Object(jobs.into_iter().collect())))
}

async fn get_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let access = access::require(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    match ctx
        .state
        .jobs
        .try_get(&name)
        .await
        .map_err(ApiError::unavailable)?
    {
        Some(mut meta) if can_see(&access, &name, Some(&meta)) => {
            let teams = if registered_team(&meta) {
                ctx.state.instance().get_teams().await.map_err(|_| {
                    ApiError::unavailable("Could not resolve retained job team identities.")
                })?
            } else {
                Vec::new()
            };
            project_team_names(&mut meta, &teams);
            redact_job_meta(&mut meta);
            Ok(ok_json(&meta))
        }
        _ => Err(ApiError::new(
            404,
            format!("{} doesn't exist.", ctx.state.jobs.job_str(&name)),
        )),
    }
}

async fn create_job(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let body = parse_json_body(&body_bytes(body, ctx.state.config.limits.body).await?)?;
    let access = access::require(&ctx.state, &ctx.headers, &["jobs:execute", "jobs:write"]).await?;
    let Some(metadata) = body else {
        return Err(ApiError::missing(&["body"]));
    };
    // A new job runs an action on the server: the rule of `/actions`.
    if !access.no_auth && !access.full_admin() {
        let allowed = is_user_allowed_to_execute(&ctx.state, access.auth.as_ref()).await;
        if !allowed.success {
            return Ok(tuple_response(&allowed));
        }
    }
    let strs = |v: &Json| -> Option<Vec<String>> {
        v.as_array().map(|a| {
            a.iter()
                .map(|i| {
                    i.as_str()
                        .map(String::from)
                        .unwrap_or_else(|| i.to_string())
                })
                .collect()
        })
    };
    let (sysargs, properties) = match &metadata {
        Json::Array(_) => (strs(&metadata).unwrap_or_default(), None),
        Json::Object(o) => {
            // PARITY: `metadata['sysargs']` and `metadata['properties']` raise `KeyError`
            // (a 500) when missing (`api/routes/_jobs.py:139`).
            let Some(sysargs) = o.get("sysargs").and_then(strs) else {
                return Err(ApiError::internal());
            };
            let Some(props) = o.get("properties") else {
                return Err(ApiError::internal());
            };
            let props: Option<Map> = match to_core(props) {
                meerschaum_core::Value::Map(m) => Some(m),
                _ => None,
            };
            (sysargs, props)
        }
        _ => {
            return Err(ApiError::validation(vec![ApiError::field_error(
                "list_type",
                &["body"],
                "Input should be a valid list",
                metadata.clone(),
                None,
            )]));
        }
    };
    // The ordinary Job client sends a display label and its user-job history marker.
    // Compare their exact generated values; neither supplies execution options or identity.
    let plain_properties = properties.as_ref().is_none_or(|props| {
        props.iter().all(|(key, value)| match key.as_str() {
            "label" => value
                .as_str()
                .is_some_and(|label| label == meerschaum_core::shlex::join(&sysargs)),
            "mrsm_execution" => value.as_map().is_some_and(|meta| {
                meta.len() == 2
                    && meta.get("kind").and_then(meerschaum_core::Value::as_str)
                        == Some("persistent_job")
                    && meta.get("origin").and_then(meerschaum_core::Value::as_str)
                        == Some("user_job")
            }),
            _ => false,
        })
    });
    let stored_request = crate::executor::context::JobRequestKind::Create {
        words: sysargs.clone(),
        plain: plain_properties,
    };
    let sysargs = creation_sysargs(&name, &sysargs, properties.as_ref());
    // A caller who is not an administrator runs what the console offers, on the pipes it may
    // change (the checks of `POST /execute`, `crate::execute_access`).
    if !access.no_auth
        && !access.full_admin()
        && !crate::execute_policy::check_bound_job_command(&ctx.state, &access, &sysargs).await?
    {
        if let Err(message) = crate::execute_policy::check_command(
            &ctx.state,
            &access,
            &sysargs,
            crate::execute_policy::Place::Job,
        )
        .await
        {
            return Ok(tuple_response(&SuccessTuple::fail(message)));
        }
        crate::execute_access::check(&ctx.state, &access, &sysargs).await?;
    }
    // The job is the caller's, in the caller's primary team (never what the request says).
    let mut properties = properties.unwrap_or_default();
    // 5.1: where a job runs and writes is the server's to choose for a caller who may not run
    // system commands: a log path or working directory of theirs would let a job write files
    // (a plugin, a shell profile) anywhere the server's user may.
    if !crate::execute_policy::may_run_system_commands(&ctx.state, &access)
        .await
        .anywhere()
    {
        for key in SERVER_CHOSEN_PROPERTIES {
            properties.shift_remove(*key);
        }
    }
    stamp_caller(&mut properties, &access);
    mark_team_execution(&ctx.state, &mut properties, &access).await;
    let create = ctx.state.jobs.create(&name, sysargs, Some(properties));
    match crate::executor::context::stored_request_scope(
        &name,
        stored_request,
        &ctx.headers,
        &access,
        create,
    )
    .await
    {
        CreateJob::Exists => Err(ApiError::new(
            409,
            format!("{} already exists.", ctx.state.jobs.job_str(&name)),
        )),
        CreateJob::Started(res) => Ok(tuple_response(&res)),
        CreateJob::Unavailable(error) => Err(ApiError::unavailable(error)),
    }
}

/// Run `op` on the job `name` for a caller who holds `scopes` (each also satisfied by its
/// legacy spelling in `aliases`) and can see the job.
async fn guarded<F, Fut>(
    ctx: Ctx,
    scopes: &[&str],
    aliases: &[(&str, &str)],
    name: String,
    check_exists: bool,
    op: F,
) -> ApiResult
where
    F: FnOnce(AppState, String, Access) -> Fut,
    Fut: std::future::Future<Output = SuccessTuple>,
{
    let access = access::require_with_aliases(&ctx.state, &ctx.headers, scopes, aliases).await?;
    // `--secure`: only administrators act on jobs (v4's shape: `[false, "..."]`).
    if !ctx.state.config.actions_non_admin && !access.no_auth && !access.is_admin() {
        return Ok(tuple_response(&SuccessTuple::fail(SECURE_REFUSAL)));
    }
    if !may_see(&ctx.state, &access, &name).await {
        return Err(hidden(&ctx.state, &name));
    }
    if check_exists && !ctx.state.jobs.exists(&name).await {
        return Err(ApiError::new(
            404,
            format!("{} does not exist.", ctx.state.jobs.job_str(&name)),
        ));
    }
    if check_exists {
        ctx.state
            .jobs
            .ready(&name)
            .await
            .map_err(ApiError::unavailable)?;
    }
    Ok(tuple_response(&op(ctx.state.clone(), name, access).await))
}

async fn delete_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    guarded(
        ctx,
        &["jobs:delete"],
        &[],
        name,
        false,
        |s, n, _| async move { s.jobs.delete(&n).await },
    )
    .await
}

async fn start_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let state = ctx.state.clone();
    let _maintenance = state.execution_maintenance.lock().await;
    let headers = ctx.headers.clone();
    guarded(
        ctx,
        &["jobs:execute"],
        &[],
        name,
        true,
        |s, n, access| async move {
            crate::executor::context::stored_request_scope(
                &n,
                crate::executor::context::JobRequestKind::Start,
                &headers,
                &access,
                s.jobs.start(&n),
            )
            .await
        },
    )
    .await
}

async fn stop_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    // FIX (intentional v4 change): v4 required the misspelled scope `josb:stop`
    // (`api/routes/_jobs.py:213`), which no token can hold, so only `*` could stop a job over
    // the API. The route requires `jobs:stop`; a token that explicitly holds the legacy
    // `josb:stop` is still accepted in its place.
    guarded(
        ctx,
        &["jobs:execute", "jobs:stop"],
        &[("jobs:stop", "josb:stop")],
        name,
        true,
        |s, n, _| async move { s.jobs.stop(&n).await },
    )
    .await
}

async fn pause_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    guarded(
        ctx,
        &["jobs:execute", "jobs:pause"],
        &[],
        name,
        true,
        |s, n, _| async move { s.jobs.pause(&n).await },
    )
    .await
}

async fn get_job_exists(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let access = access::require(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let exists = may_see(&ctx.state, &access, &name).await && ctx.state.jobs.exists(&name).await;
    Ok(ok_json(&json!(exists)))
}

async fn get_logs(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let access = access::require(&ctx.state, &ctx.headers, &["jobs:read", "logs:read"]).await?;
    if !may_see(&ctx.state, &access, &name).await || !ctx.state.jobs.exists(&name).await {
        return Err(ApiError::new(
            404,
            format!("{} does not exist.", ctx.state.jobs.job_str(&name)),
        ));
    }
    Ok(ok_json(&json!(ctx.state.jobs.logs(&name).await)))
}

async fn get_stop_time(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let access = access::require(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let t = if may_see(&ctx.state, &access, &name).await {
        ctx.state.jobs.stop_time(&name).await
    } else {
        None
    };
    Ok(ok_json(&json!(t.map(|d| py_isoformat_utc(&d)))))
}

async fn get_is_blocking_on_stdin(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let access = access::require(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let blocking = may_see(&ctx.state, &access, &name).await
        && ctx.state.jobs.is_blocking_on_stdin(&name).await;
    Ok(ok_json(&json!(blocking)))
}

async fn get_prompt_kwargs(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let access = access::require(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let kwargs = if may_see(&ctx.state, &access, &name).await {
        ctx.state.jobs.prompt_kwargs(&name).await
    } else {
        Map::new()
    };
    Ok(ok_json(&from_core(&meerschaum_core::Value::Map(kwargs))))
}

async fn logs_websocket(
    ctx: Ctx,
    Path(name): Path<String>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    let state = ctx.state.clone();
    ws.on_upgrade(move |socket| monitor_socket(state, name, socket))
        .into_response()
}

/// The websocket protocol of `logs_websocket`: the first client message is a JWT or API key; then the
/// server streams log text, sends `MRSM_STDIN` when the job wants input (the next client
/// message is forwarded), and `MRSM_STOP` followed by the JSON result when the job stops.
async fn monitor_socket(state: AppState, name: String, mut socket: WebSocket) {
    let token = match socket.recv().await {
        Some(Ok(Message::Text(t))) => t.to_string(),
        _ => return,
    };
    let mut headers = HeaderMap::new();
    if let Ok(value) = format!("Bearer {token}").parse() {
        headers.insert(header::AUTHORIZATION, value);
    }
    // The websocket exposes the same logs as GET /logs/{name}, so it must enforce the
    // same permissions. Reuse the HTTP authenticator to support API keys and JWTs alike.
    let access = match access::require(&state, &headers, &["jobs:read", "logs:read"]).await {
        Ok(access) => access,
        Err(error) => {
            socket_error(&mut socket, error).await;
            return;
        }
    };
    // A job the caller cannot see (another team's, someone's console run) is absent.
    if !may_see(&state, &access, &name).await {
        let _ = socket.send(Message::Close(None)).await;
        return;
    }
    let Some(mut monitor) = crate::executor::context::scope(
        Some(std::sync::Arc::new(access.clone())),
        state.jobs.monitor_logs(&name),
    )
    .await
    else {
        let _ = socket.send(Message::Close(None)).await;
        return;
    };
    let drain = state.lifecycle.signal().wait();
    tokio::pin!(drain);
    let mut reauthorize = tokio::time::interval(access::IDENTITY_TTL);
    reauthorize.tick().await;
    loop {
        tokio::select! {
            _ = reauthorize.tick() => {
                let current = match access::require_fresh(&state, &headers, &["jobs:read", "logs:read"]).await {
                    Ok(current) => current,
                    Err(error) => { socket_error(&mut socket, error).await; return; }
                };
                if !may_see(&state, &current, &name).await { let _ = socket.send(Message::Close(None)).await; return; }
            }
            // A draining server closes the socket at once (`1001 Going Away`): the client
            // reconnects to another server.
            () = &mut drain => {
                let _ = socket
                    .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1001,
                        reason: "The server is shutting down.".into(),
                    })))
                    .await;
                return;
            }
            event = monitor.events.recv() => match event {
                Some(LogEvent::Line(text)) => {
                    if socket.send(Message::Text(text.into())).await.is_err() {
                        return;
                    }
                }
                Some(LogEvent::StdinRequested) => {
                    if socket
                        .send(Message::Text(meerschaum_connector_api::routes::JOBS_STDIN_MESSAGE.into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Some(LogEvent::Stopped(result)) => {
                    let _ = socket
                        .send(Message::Text(meerschaum_connector_api::routes::JOBS_STOP_MESSAGE.into()))
                        .await;
                    let body = crate::json::fastapi_json(&json!([result.success, result.message]));
                    let _ = socket.send(Message::Text(body.into())).await;
                    let _ = socket.send(Message::Close(None)).await;
                    return;
                }
                // The follower ended without a result (a finished job with no output): close
                // the socket so that the client stops waiting.
                None => {
                    let _ = socket.send(Message::Close(None)).await;
                    return;
                }
            },
            msg = socket.recv() => match msg {
                Some(Ok(Message::Text(t))) => {
                    // Reading a job's output must not grant control of its prompts. Check
                    // the submitted credential again, including expiry and current API-key
                    // scopes, before allowing input to change a running job's behavior.
                    let access = match access::require(
                        &state, &headers, &["actions:execute", "jobs:execute"],
                    ).await {
                        Ok(access) => access,
                        Err(error) => {
                            socket_error(&mut socket, error).await;
                            return;
                        }
                    };
                    let allowed = is_user_allowed_to_execute(&state, access.auth.as_ref()).await;
                    if !allowed.success {
                        socket_error(&mut socket, ApiError::new(403, allowed.message)).await;
                        return;
                    }
                    if !may_see(&state, &access, &name).await {
                        let _ = socket.send(Message::Close(None)).await;
                        return;
                    }
                    // A job's input is its program's (a shell's, `python`'s): a caller who may
                    // not run system commands answers only the prompts of a job they could have
                    // started themselves (5.1, `crate::execute_policy`).
                    if let Err(message) = job_input_allowed(&state, &access, &name).await {
                        socket_error(&mut socket, ApiError::new(403, message)).await;
                        return;
                    }
                    let _ = monitor.input.send(t.to_string()).await;
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            },
        }
    }
}

/// The job properties `POST /jobs/{name}` ignores from a caller who may not run system commands
/// (5.1): the working directory, the log settings (their `path` is any file the server's user
/// may write) and the command (`target`, which the job writes from its `sysargs` anyway).
pub const SERVER_CHOSEN_PROPERTIES: &[&str] = &["cwd", "logs", "target"];

/// Whether `access` may type into job `name`'s input: always when the caller may run system
/// commands, otherwise only when the job's own command line passes
/// [`crate::execute_policy::check_command`] for them (an administrator's `python` or shell job
/// does not). `Err` says why not.
pub async fn job_input_allowed(
    state: &AppState,
    access: &Access,
    name: &str,
) -> Result<(), String> {
    match crate::execute_policy::may_run_system_commands(state, access).await {
        crate::execute_policy::SystemCommands::Anywhere => return Ok(()),
        // 5.1: team execution answers its own team's jobs only (they run in its sandbox).
        crate::execute_policy::SystemCommands::TeamOnly { team } => {
            let meta = state.jobs.try_get(name).await?;
            let job = JobAccess::from_metadata(name, meta.as_ref());
            if job.is_team(Some(&team)) {
                return Ok(());
            }
        }
        crate::execute_policy::SystemCommands::No { .. } => {}
    }
    let sysargs: Option<Vec<String>> = state.jobs.try_get(name).await?.and_then(|meta| {
        meta.get("sysargs")?
            .as_array()?
            .iter()
            .map(|word| word.as_str().map(str::to_string))
            .collect()
    });
    let Some(sysargs) = sysargs.filter(|words| !words.is_empty()) else {
        return Err(format!(
            "{} does not say what it runs, so only an administrator may answer it.",
            state.jobs.job_str(name)
        ));
    };
    // The job runs as its own process: its command must be one the caller could run.
    crate::execute_policy::check_non_admin_resolved(
        state.actions.as_ref(),
        access,
        &clean_sysargs(&sysargs),
    )
    .await
    .map_err(|message| {
        format!(
            "Only an administrator may answer the prompts of {}: {message}",
            state.jobs.job_str(name)
        )
    })
}

/// Keep the existing invalid-credentials handshake text; explain missing scopes to callers.
async fn socket_error(socket: &mut WebSocket, error: ApiError) {
    let message = if error.status == axum::http::StatusCode::FORBIDDEN {
        error
            .detail
            .and_then(|detail| detail.as_str().map(String::from))
    } else {
        None
    }
    .unwrap_or_else(|| "Invalid credentials.".into());
    let _ = socket.send(Message::Text(message.into())).await;
    let _ = socket.send(Message::Close(None)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sysargs_cleaning() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(
            clean_sysargs(&s(&["api", "main", "show", "pipes", "-e", "x", "-y"])),
            s(&["show", "pipes", "-y"])
        );
    }

    #[tokio::test]
    async fn only_verified_exact_canonical_jobs_transport_the_original_command() {
        use crate::executor::control::{Actor, Claims};
        use crate::executor::gateway::{VerifiedRequest, scope};
        let id = uuid::Uuid::parse_str("abcdefab-cdef-4abc-8def-abcdefabcdef").unwrap();
        let name = format!("mrsm-{}", id.simple());
        let command: Vec<String> = ["api", "probe", "--metric", "api", "value"]
            .into_iter()
            .map(str::to_string)
            .collect();
        let properties = Map::from_iter([(
            crate::executor::environment::JOB_CONTEXT_PROPERTY.into(),
            id.to_string().into(),
        )]);
        // Neither a public caller nor a direct backend's spoofed property gains transport.
        assert_eq!(
            creation_sysargs(&name, &command, Some(&properties)),
            clean_sysargs(&command)
        );
        let verified = VerifiedRequest {
            team_name: "TransportFixture".into(),
            claims: Claims {
                protocol: crate::executor::control::PROTOCOL,
                version: "5.1.0".into(),
                issuer: uuid::Uuid::new_v4(),
                team_id: uuid::Uuid::new_v4(),
                generation: uuid::Uuid::new_v4(),
                issued_at: 1,
                expires_at: 2,
                nonce: uuid::Uuid::new_v4(),
                method: "POST".into(),
                path: format!("/jobs/{name}"),
                body_sha256: String::new(),
                configuration_sha256: String::new(),
                actor: Actor {
                    owner: "operator".into(),
                    username: Some("operator".into()),
                    permissions: Default::default(),
                    administrator: false,
                },
            },
        };
        scope(Some(verified), async {
            assert_eq!(
                creation_sysargs(&name, &command, Some(&properties)),
                command
            );
            assert_eq!(
                creation_sysargs(&name, &command, None),
                clean_sysargs(&command)
            );
            let wrong = Map::from_iter([(
                crate::executor::environment::JOB_CONTEXT_PROPERTY.into(),
                uuid::Uuid::new_v4().to_string().into(),
            )]);
            assert_eq!(
                creation_sysargs(&name, &command, Some(&wrong)),
                clean_sysargs(&command)
            );
            for invalid in [
                format!("mrsm-{id}"),
                format!("mrsm-{}", id.simple().to_string().to_uppercase()),
                "ordinary".into(),
                format!("mrsm-{}", uuid::Uuid::nil().simple()),
            ] {
                assert_eq!(
                    creation_sysargs(&invalid, &command, Some(&properties)),
                    clean_sysargs(&command)
                );
            }
        })
        .await;
    }

    #[test]
    fn the_caller_is_recorded_whatever_the_request_says() {
        let mut access = Access::everything();
        let mut props = Map::new();
        props.insert("owner".into(), meerschaum_core::Value::Str("admin".into()));
        stamp_caller(&mut props, &access);
        assert_eq!(props.get("owner"), Some(&meerschaum_core::Value::Null));
        access.no_auth = false;
        access.principal.username = "bob".into();
        access.principal.is_admin = false;
        access.primary_team = "Data".into();
        let mut user = meerschaum_core::users::User::new("bob", None);
        user.user_id = Some(meerschaum_core::Value::Int(1));
        access.auth = Some(crate::auth::Principal::User(user));
        stamp_caller(&mut props, &access);
        let meta =
            json!({"daemon": {"properties": from_core(&meerschaum_core::Value::Map(props))}});
        let recorded = JobAccess::from_metadata("x", Some(&meta));
        assert_eq!(recorded.owner.as_deref(), Some("bob"));
        assert_eq!(recorded.team.as_deref(), Some("Data"));
        assert!(can_see(&access, "x", Some(&meta)));
        access.principal.username = "carol".into();
        assert!(!can_see(&access, "x", Some(&meta)));
    }

    #[test]
    fn registered_history_respects_team_ids_effective_boundaries_and_legacy_recovery() {
        use meerschaum_core::Value;
        use meerschaum_core::access::{Principal, Role, Team, TeamRole};
        let mut team = Team::new("Wetlands");
        team.id = Some(uuid::Uuid::new_v4().to_string());
        let caller = |username: &str, id: i64, role| {
            let mut caller = Access::everything();
            caller.no_auth = false;
            let mut user = meerschaum_core::users::User::new(username, None);
            user.user_id = Some(Value::Int(id));
            caller.auth = Some(crate::auth::Principal::User(user));
            caller.principal = Principal::new(
                username,
                false,
                Role::Operator.permissions(),
                vec![(team.name.clone(), role)],
            );
            caller.primary_team = team.name.clone();
            caller.team_records = vec![team.clone()];
            caller
        };
        let mut owner = caller("alice", 1, TeamRole::Member);
        let manager = caller("manager", 2, TeamRole::Manager);
        let mut props = Map::new();
        stamp_caller(&mut props, &owner);
        props.insert(
            "mrsm_execution".into(),
            serde_json::from_value::<Value>(json!({"kind":"temporary_execution"})).unwrap(),
        );
        let metadata = json!({"daemon":{"properties":props}});
        assert!(can_see(&manager, "private", Some(&metadata)));
        let mut reused_team = manager.clone();
        reused_team.team_records[0].id = Some(uuid::Uuid::new_v4().to_string());
        assert!(!can_see(&reused_team, "private", Some(&metadata)));
        owner.team_records.clear();
        owner.principal.teams.clear();
        assert!(can_see(&owner, "private", Some(&metadata)));
        owner.principal = owner.principal.within_team("Wetlands");
        assert!(
            !can_see(&owner, "private", Some(&metadata)),
            "A bounded capability cannot use historical ownership outside its current team identity."
        );
        let legacy = json!({"daemon":{"properties":{"owner":"manager","team":"Wetlands","mrsm_execution":{"kind":"temporary_execution"}}}});
        assert!(
            !can_see(&manager, "private", Some(&legacy)),
            "A reused display name is not proof of legacy private ownership or team oversight."
        );
        assert!(can_see(&Access::everything(), "private", Some(&legacy)));
    }
}
