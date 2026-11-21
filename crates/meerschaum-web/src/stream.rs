//! `GET /logs/{name}/stream`: a job's log as Server-Sent Events (the Dash console's Execute box
//! follows the jobs `POST /execute` starts; it replaces v4's websocket). Authenticated by a bearer
//! token (`Authorization`) or, for the browser's `EventSource`, by the console session cookie.
//!
//! One `data:` event per log line; `event: stdin` when the job waits for input (the stream cannot
//! answer; jobs started by `POST /execute` run noninteractive); `event: stopped` with the job's
//! final message, after which the stream ends.
//!
//! Authorization is the rule of `GET /logs/{name}` (`jobs:read` and `logs:read` on a job the
//! caller can see: its team's, a console run its owner's and its team's managers', every job the
//! administrators'; `meerschaum_core::job_access`); streams are rate limited like the rest of the
//! `jobs` route group and bounded in number (`api:permissions:log_streams`) through
//! [`meerschaum_server::sse::StreamGuard`].

use std::convert::Infallible;
use std::time::Duration;

use axum::{
    extract::{Path, State},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive},
    },
};
use meerschaum_server::backends::LogEvent;
use meerschaum_server::sse::{self, StreamAuth};

use super::*;

/// How often an idle stream re-authorizes its session/bearer and job visibility, re-minting
/// short-lived console bearers so a live console session never expires mid-stream.
const SESSION_CHECK: Duration = Duration::from_secs(10);

/// Scopes required to read a job's log stream.
const SCOPES: &[&str] = &["jobs:read", "logs:read"];

pub(crate) async fn logs(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    let auth =
        match StreamAuth::open(&state.server, &headers, Some(state.browser_auth.clone())).await {
            Ok(auth) => auth,
            Err(rejected) => return rejected.into_response(),
        };
    // The rule of `GET /logs/{name}` (the same log): the permissions to read jobs and their logs,
    // on a job the caller can see.
    let access =
        match meerschaum_server::access::require_fresh(&state.server, auth.headers(), SCOPES).await
        {
            Ok(access) => access,
            Err(e) => return e.into_response(),
        };
    if !state.server.jobs.exists(&name).await {
        return StatusCode::NOT_FOUND.into_response();
    }
    // A job's output is its team's (a console run: its creator's, its team's managers'), and the
    // administrators' (the job metadata routes apply the same rule); anyone else sees it as
    // absent.
    let meta = match state.server.jobs.try_get(&name).await {
        Ok(meta) => meta,
        Err(error) => {
            return meerschaum_server::error::ApiError::unavailable(error).into_response();
        }
    };
    if !meerschaum_server::routes::jobs::can_see(&access, &name, meta.as_ref()) {
        return StatusCode::NOT_FOUND.into_response();
    }
    // Each stream follows its job on a thread: bounded in total and per user.
    let guard = match auth.into_guard(
        &state.streams,
        state.server.config.log_streams_max,
        state.server.config.log_streams_per_user,
        access.auth.as_ref(),
    ) {
        Ok(guard) => guard,
        Err(rejected) => return rejected.into_response(),
    };
    let Some(monitor) = meerschaum_server::executor::context::scope(
        Some(std::sync::Arc::new(access)),
        state.server.jobs.monitor_logs(&name),
    )
    .await
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut ticker =
        tokio::time::interval_at(tokio::time::Instant::now() + SESSION_CHECK, SESSION_CHECK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let drain = state.server.lifecycle.signal();
    let stream = futures_util::stream::unfold(
        (monitor, state, guard, name, false, ticker),
        |(mut monitor, state, mut guard, name, done, mut ticker)| async move {
            if done {
                return None;
            }
            let event = loop {
                tokio::select! {
                    event = monitor.events.recv() => {
                        let event = event?;
                        // A console session that ended (logout, credential rotation) ends its
                        // streams immediately on the next log event too.
                        if guard.has_session()
                            && guard.refresh_headers(&state.server).await.is_err()
                        {
                            return None;
                        }
                        break event;
                    }
                    // ... also while the job is silent, re-minting fresh short-lived bearers
                    // for live console sessions and re-verifying fresh permissions & visibility.
                    _ = ticker.tick() => {
                        let caller = guard.reauthorize(&state.server, SCOPES).await?;
                        if !meerschaum_server::routes::jobs::may_see(&state.server, &caller, &name).await {
                            return None;
                        }
                    }
                }
            };
            let (event, done) = match event {
                LogEvent::Line(line) => (Event::default().data(line), false),
                LogEvent::Stopped(result) => {
                    (Event::default().event("stopped").data(result.message), true)
                }
                LogEvent::StdinRequested => (
                    Event::default().event("stdin").data("Input requested"),
                    false,
                ),
            };
            Some((
                Ok::<_, Infallible>(event),
                (monitor, state, guard, name, done, ticker),
            ))
        },
    );
    // A draining server ends the stream at once (the browser reconnects to another server).
    sse::response(stream, drain, KeepAlive::default(), sse::CACHE_NO_STORE)
}
