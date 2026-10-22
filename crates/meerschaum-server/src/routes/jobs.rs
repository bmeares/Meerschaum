//! `/jobs/*`, `/logs/{name}` and the logs websocket `/logs/{name}/ws` (`api/routes/_jobs.py`),
//! delegated to a [`JobsBackend`](crate::backends::JobsBackend).

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use meerschaum_core::{Map, SuccessTuple};
use serde_json::{Value as Json, json};

use super::{Ctx, body_bytes};
use crate::auth::{
    Principal, is_admin, is_user_allowed_to_execute, scoped_auth, scoped_auth_with_aliases,
};
use crate::backends::{CreateJob, LogEvent};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{from_core, ok_json, py_isoformat_utc, to_core, tuple_response};
use crate::redact::redact_job_meta;
use crate::routes::execute::{job_visible_to, owner_label};
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

/// Whether the caller may see the job `name`: an `execute-*` job (the console's) is its creator's
/// and the administrators' (see [`job_visible_to`]); every other job is visible as before.
async fn may_see(state: &AppState, principal: Option<&Principal>, name: &str) -> bool {
    if is_admin(state, principal).await {
        return true;
    }
    match state.jobs.get(name).await {
        // A job that does not exist is not hidden: the route answers as it always does.
        None => true,
        Some(meta) => job_visible_to(false, owner_label(principal).as_deref(), name, Some(&meta)),
    }
}

/// The 404 of a job that is hidden from the caller (the same as one that does not exist).
fn hidden(state: &AppState, name: &str) -> ApiError {
    ApiError::new(404, format!("{} does not exist.", state.jobs.job_str(name)))
}

async fn get_jobs(ctx: Ctx) -> ApiResult {
    let principal = scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let admin = is_admin(&ctx.state, principal.as_ref()).await;
    let caller = owner_label(principal.as_ref());
    let mut jobs = ctx.state.jobs.list().await;
    jobs.retain(|name, meta| job_visible_to(admin, caller.as_deref(), name, Some(meta)));
    // PARITY: v4 returns every job's environment (connector URIs with their passwords and the
    // server's other secrets) to any `jobs:read` caller; v5 masks the credentials.
    for meta in jobs.values_mut() {
        redact_job_meta(meta);
    }
    Ok(ok_json(&Json::Object(jobs.into_iter().collect())))
}

async fn get_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let principal = scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let admin = is_admin(&ctx.state, principal.as_ref()).await;
    match ctx.state.jobs.get(&name).await {
        Some(mut meta)
            if job_visible_to(
                admin,
                owner_label(principal.as_ref()).as_deref(),
                &name,
                Some(&meta),
            ) =>
        {
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
    let principal = scoped_auth(&ctx.state, &ctx.headers, &["jobs:execute", "jobs:write"]).await?;
    let Some(metadata) = body else {
        return Err(ApiError::missing(&["body"]));
    };
    let allowed = is_user_allowed_to_execute(&ctx.state, principal.as_ref()).await;
    if !allowed.success {
        return Ok(tuple_response(&allowed));
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
    match ctx
        .state
        .jobs
        .create(&name, clean_sysargs(&sysargs), properties)
        .await
    {
        CreateJob::Exists => Err(ApiError::new(
            409,
            format!("{} already exists.", ctx.state.jobs.job_str(&name)),
        )),
        CreateJob::Started(res) => Ok(tuple_response(&res)),
    }
}

async fn guarded<F, Fut>(
    ctx: Ctx,
    scopes: &[&str],
    name: String,
    check_exists: bool,
    op: F,
) -> ApiResult
where
    F: FnOnce(AppState, String) -> Fut,
    Fut: std::future::Future<Output = SuccessTuple>,
{
    guarded_with_aliases(ctx, scopes, &[], name, check_exists, op).await
}

async fn guarded_with_aliases<F, Fut>(
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
    let principal = scoped_auth_with_aliases(&ctx.state, &ctx.headers, scopes, aliases).await?;
    let allowed = is_user_allowed_to_execute(&ctx.state, principal.as_ref()).await;
    if !allowed.success {
        return Ok(tuple_response(&allowed));
    }
    if !may_see(&ctx.state, principal.as_ref(), &name).await {
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
    guarded(ctx, &["jobs:delete"], name, false, |s, n| async move {
        s.jobs.delete(&n).await
    })
    .await
}

async fn start_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let state = ctx.state.clone();
    let _maintenance = state.execution_maintenance.lock().await;
    guarded(ctx, &["jobs:execute"], name, true, |s, n| async move {
        s.jobs.start(&n).await
    })
    .await
}

async fn stop_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    // FIX (intentional v4 change): v4 required the misspelled scope `josb:stop`
    // (`api/routes/_jobs.py:213`), which no token can hold, so only `*` could stop a job over
    // the API. The route requires `jobs:stop`; a token that explicitly holds the legacy
    // `josb:stop` is still accepted in its place.
    guarded_with_aliases(
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
        name,
        true,
        |s, n| async move { s.jobs.pause(&n).await },
    )
    .await
}

async fn get_job_exists(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let principal = scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let exists =
        may_see(&ctx.state, principal.as_ref(), &name).await && ctx.state.jobs.exists(&name).await;
    Ok(ok_json(&json!(exists)))
}

async fn get_logs(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let principal = scoped_auth(&ctx.state, &ctx.headers, &["jobs:read", "logs:read"]).await?;
    if !may_see(&ctx.state, principal.as_ref(), &name).await || !ctx.state.jobs.exists(&name).await
    {
        return Err(ApiError::new(
            404,
            format!("{} does not exist.", ctx.state.jobs.job_str(&name)),
        ));
    }
    Ok(ok_json(&json!(ctx.state.jobs.logs(&name).await)))
}

async fn get_stop_time(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let principal = scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let t = if may_see(&ctx.state, principal.as_ref(), &name).await {
        ctx.state.jobs.stop_time(&name).await
    } else {
        None
    };
    Ok(ok_json(&json!(t.map(|d| py_isoformat_utc(&d)))))
}

async fn get_is_blocking_on_stdin(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let principal = scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let blocking = may_see(&ctx.state, principal.as_ref(), &name).await
        && ctx.state.jobs.is_blocking_on_stdin(&name).await;
    Ok(ok_json(&json!(blocking)))
}

async fn get_prompt_kwargs(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let principal = scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let kwargs = if may_see(&ctx.state, principal.as_ref(), &name).await {
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
    // same scopes. Reuse the HTTP authenticator to support API keys and JWTs alike.
    let principal = match scoped_auth(&state, &headers, &["jobs:read", "logs:read"]).await {
        Ok(principal) => principal,
        Err(error) => {
            socket_error(&mut socket, error).await;
            return;
        }
    };
    // An `execute-*` job's output is its creator's and the administrators'.
    if !may_see(&state, principal.as_ref(), &name).await {
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
                    let principal = match scoped_auth(
                        &state, &headers, &["actions:execute", "jobs:execute"],
                    ).await {
                        Ok(principal) => principal,
                        Err(error) => {
                            socket_error(&mut socket, error).await;
                            return;
                        }
                    };
                    let allowed = is_user_allowed_to_execute(&state, principal.as_ref()).await;
                    if !allowed.success {
                        socket_error(&mut socket, ApiError::new(403, allowed.message)).await;
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
}
