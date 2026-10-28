//! `/jobs/*`, `/logs/{name}` and the logs websocket `/logs/{name}/ws` (`api/routes/_jobs.py`),
//! delegated to a [`JobsBackend`](crate::backends::JobsBackend).
//!
//! **Who sees and controls a job** (decision AC9; the rule is
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

/// What a non-administrator who tries to control a job hears under `--secure` (the text of
/// [`is_user_allowed_to_execute`]).
const SECURE_REFUSAL: &str =
    "The administrator for this server has not allowed users to perform actions.";

/// Record who starts a job through the API in its `properties`: the caller (`owner`) and the
/// caller's primary team (`team`). Whatever the request carried is replaced; under `--no-auth`
/// (and for a token without an owner) nobody is recorded and the job is Everyone's.
pub fn stamp_caller(properties: &mut Map, access: &Access) {
    if access.no_auth {
        JobAccess::stamp(properties, None, None);
        return;
    }
    JobAccess::stamp(properties, access.username(), Some(&access.primary_team));
}

/// Whether `access` may see the job `name` whose metadata (as the jobs routes serve it) is
/// `meta` (see [`meerschaum_core::job_access`]).
pub fn can_see(access: &Access, name: &str, meta: Option<&Json>) -> bool {
    if access.no_auth {
        return true;
    }
    let job = JobAccess::from_metadata(name, meta);
    job_access::can_see_job(&access.principal, &job) || token_owns(access, &job)
}

/// A token without an owner (made locally) records itself as `token:<id>`: it sees its own runs.
fn token_owns(access: &Access, job: &JobAccess) -> bool {
    match (&access.auth, job.owner.as_deref()) {
        (Some(crate::auth::Principal::Token(token)), Some(owner))
            if access.username().is_none() =>
        {
            token.id.is_some_and(|id| owner == format!("token:{id}"))
        }
        _ => false,
    }
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
    match state.jobs.get(name).await {
        None => true,
        Some(meta) => can_see(access, name, Some(&meta)),
    }
}

/// The 404 of a job that is hidden from the caller (the same as one that does not exist).
fn hidden(state: &AppState, name: &str) -> ApiError {
    ApiError::new(404, format!("{} does not exist.", state.jobs.job_str(name)))
}

async fn get_jobs(ctx: Ctx) -> ApiResult {
    let access = access::require(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let mut jobs = ctx.state.jobs.list().await;
    jobs.retain(|name, meta| can_see(&access, name, Some(meta)));
    // PARITY: v4 returns every job's environment (connector URIs with their passwords and the
    // server's other secrets) to any `jobs:read` caller; v5 masks the credentials.
    for meta in jobs.values_mut() {
        redact_job_meta(meta);
    }
    Ok(ok_json(&Json::Object(jobs.into_iter().collect())))
}

async fn get_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let access = access::require(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    match ctx.state.jobs.get(&name).await {
        Some(mut meta) if can_see(&access, &name, Some(&meta)) => {
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
    let body = parse_json_body(&body_bytes(body).await?)?;
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
    // A caller who is not an administrator runs what the console offers, on the pipes it may
    // change (rc15, P3: the checks of `POST /execute`, `crate::execute_access`).
    if !access.no_auth && !access.full_admin() {
        let cleaned = clean_sysargs(&sysargs);
        if let Err(message) = crate::execute_policy::check_non_admin(&cleaned) {
            return Ok(tuple_response(&SuccessTuple::fail(message)));
        }
        crate::execute_access::check(&ctx.state, &access, &cleaned).await?;
    }
    // The job is the caller's, in the caller's primary team (never what the request says).
    let mut properties = properties.unwrap_or_default();
    stamp_caller(&mut properties, &access);
    match ctx
        .state
        .jobs
        .create(&name, clean_sysargs(&sysargs), Some(properties))
        .await
    {
        CreateJob::Exists => Err(ApiError::new(
            409,
            format!("{} already exists.", ctx.state.jobs.job_str(&name)),
        )),
        CreateJob::Started(res) => Ok(tuple_response(&res)),
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
    F: FnOnce(AppState, String) -> Fut,
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
    Ok(tuple_response(&op(ctx.state.clone(), name).await))
}

async fn delete_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    guarded(ctx, &["jobs:delete"], &[], name, false, |s, n| async move {
        s.jobs.delete(&n).await
    })
    .await
}

async fn start_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let state = ctx.state.clone();
    let _maintenance = state.execution_maintenance.lock().await;
    guarded(ctx, &["jobs:execute"], &[], name, true, |s, n| async move {
        s.jobs.start(&n).await
    })
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
        |s, n| async move { s.jobs.stop(&n).await },
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
        |s, n| async move { s.jobs.pause(&n).await },
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
    let Some(mut monitor) = state.jobs.monitor_logs(&name).await else {
        let _ = socket.send(Message::Close(None)).await;
        return;
    };
    loop {
        tokio::select! {
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
                    let _ = monitor.input.send(t.to_string()).await;
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            },
        }
    }
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
}
