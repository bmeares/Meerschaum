//! `/jobs/*`, `/logs/{name}` and the logs websocket `/logs/{name}/ws` (`api/routes/_jobs.py`),
//! delegated to a [`JobsBackend`](crate::backends::JobsBackend).

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use chrono::Utc;
use meerschaum_core::{Map, SuccessTuple};
use serde_json::{Value as Json, json};

use super::{Ctx, body_bytes};
use crate::auth::{is_user_allowed_to_execute, jwt_decode, load_from_subject, scoped_auth};
use crate::backends::{CreateJob, LogEvent};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{from_core, ok_json, py_isoformat_utc, to_core, tuple_response};
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

async fn get_jobs(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let jobs = ctx.state.jobs.list().await;
    Ok(ok_json(&Json::Object(jobs.into_iter().collect())))
}

async fn get_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    match ctx.state.jobs.get(&name).await {
        Some(meta) => Ok(ok_json(&meta)),
        None => Err(ApiError::new(
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
    let principal = scoped_auth(&ctx.state, &ctx.headers, scopes).await?;
    let allowed = is_user_allowed_to_execute(&ctx.state, principal.as_ref()).await;
    if !allowed.success {
        return Ok(tuple_response(&allowed));
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
    guarded(ctx, &["jobs:execute"], name, true, |s, n| async move {
        s.jobs.start(&n).await
    })
    .await
}

async fn stop_job(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    // PARITY: the scope typo `josb:stop` is v4's (`api/routes/_jobs.py:213`).
    guarded(
        ctx,
        &["jobs:execute", "josb:stop"],
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
    scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    Ok(ok_json(&json!(ctx.state.jobs.exists(&name).await)))
}

async fn get_logs(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["jobs:read", "logs:read"]).await?;
    if !ctx.state.jobs.exists(&name).await {
        return Err(ApiError::new(
            404,
            format!("{} does not exist.", ctx.state.jobs.job_str(&name)),
        ));
    }
    Ok(ok_json(&json!(ctx.state.jobs.logs(&name).await)))
}

async fn get_stop_time(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let t = ctx.state.jobs.stop_time(&name).await;
    Ok(ok_json(&json!(t.map(|d| py_isoformat_utc(&d)))))
}

async fn get_is_blocking_on_stdin(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    Ok(ok_json(&json!(
        ctx.state.jobs.is_blocking_on_stdin(&name).await
    )))
}

async fn get_prompt_kwargs(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["jobs:read"]).await?;
    let kwargs = ctx.state.jobs.prompt_kwargs(&name).await;
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

/// The websocket protocol of `logs_websocket`: the first client message is a JWT; then the
/// server streams log text, sends `MRSM_STDIN` when the job wants input (the next client
/// message is forwarded), and `MRSM_STOP` followed by the JSON result when the job stops.
async fn monitor_socket(state: AppState, name: String, mut socket: WebSocket) {
    let token = match socket.recv().await {
        Some(Ok(Message::Text(t))) => t.to_string(),
        _ => return,
    };
    if !state.config.no_auth {
        let user = match jwt_decode(&token, state.secret(), Utc::now()) {
            Ok(payload) => match payload.get("sub").and_then(Json::as_str) {
                Some(sub) => load_from_subject(&state, sub).await,
                None => None,
            },
            Err(_) => None,
        };
        if user.is_none() {
            let _ = socket
                .send(Message::Text("Invalid credentials.".into()))
                .await;
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
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
                        .send(Message::Text(meerschaum_api::routes::JOBS_STDIN_MESSAGE.into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Some(LogEvent::Stopped(result)) => {
                    let _ = socket
                        .send(Message::Text(meerschaum_api::routes::JOBS_STOP_MESSAGE.into()))
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
                    let _ = monitor.input.send(t.to_string()).await;
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            },
        }
    }
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
