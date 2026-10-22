//! `GET /logs/{name}/stream`: a job's log as Server-Sent Events (the Dash console's Execute box
//! follows the jobs `POST /execute` starts; it replaces v4's websocket). Authenticated by a bearer
//! token (`Authorization`) or, for the browser's `EventSource`, by the console session cookie.
//!
//! One `data:` event per log line; `event: stdin` when the job waits for input (the stream cannot
//! answer; jobs started by `POST /execute` run noninteractive); `event: stopped` with the job's
//! final message, after which the stream ends.
//!
//! Authorization is the rule of `POST /execute`; streams are rate limited like the rest of the
//! `jobs` route group and bounded in number (`api:permissions:log_streams`).
use super::*;
use axum::{
    extract::{Path, State},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use meerschaum_server::backends::LogEvent;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How often an idle stream re-checks that its console session still exists, so a logout ends a
/// stream of a job that is silent.
const SESSION_CHECK: Duration = Duration::from_secs(10);

/// The log streams open on this server, in total and per user or token: each follows its job on
/// a thread of its own, so they are bounded (`api:permissions:log_streams`).
#[derive(Debug, Default)]
pub(crate) struct StreamSlots {
    open: Mutex<(usize, HashMap<String, usize>)>,
}

/// One open stream; the slot is free again when it drops (however the stream ends).
#[derive(Debug)]
pub(crate) struct Slot {
    slots: Arc<StreamSlots>,
    key: String,
}

impl StreamSlots {
    /// Take a slot for `key`, or say which limit is reached.
    pub(crate) fn take(
        self: &Arc<Self>,
        key: &str,
        max: usize,
        per_user: usize,
    ) -> Result<Slot, &'static str> {
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        if open.0 >= max {
            return Err("Too many log streams are open on this server.");
        }
        if open.1.get(key).copied().unwrap_or(0) >= per_user {
            return Err("Too many log streams are open for this user.");
        }
        open.0 += 1;
        *open.1.entry(key.to_string()).or_insert(0) += 1;
        Ok(Slot {
            slots: self.clone(),
            key: key.to_string(),
        })
    }

    #[cfg(test)]
    fn total(&self) -> usize {
        self.open.lock().unwrap_or_else(|e| e.into_inner()).0
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut open = self.slots.open.lock().unwrap_or_else(|e| e.into_inner());
        open.0 = open.0.saturating_sub(1);
        if let Some(n) = open.1.get_mut(&self.key) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                open.1.remove(&self.key);
            }
        }
    }
}

/// Who a stream counts against: the user, the token, or the session of a `--no-auth` server.
fn slot_key(principal: Option<&auth::Principal>, session_id: Option<&str>) -> String {
    match principal {
        Some(auth::Principal::User(user)) => format!("user:{}", user.username),
        Some(auth::Principal::Token(token)) => match token.id {
            Some(id) => format!("token:{id}"),
            None => "token:".to_string(),
        },
        None => format!("session:{}", session_id.unwrap_or("")),
    }
}

/// Whether a console session that was following has ended (`None`: a bearer, nothing to check).
async fn session_ended(state: &WebState, id: Option<&str>) -> bool {
    match id {
        Some(id) if !state.server.config.no_auth => {
            !matches!(state.config.sessions.get(id).await, Ok(Some(_)))
        }
        _ => false,
    }
}

pub(crate) async fn logs(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    // A bearer wins; otherwise the cookie's session stands in for the user.
    let (auth_headers, session_id) = if headers.contains_key(header::AUTHORIZATION) {
        (headers.clone(), None)
    } else if state.server.config.no_auth {
        (HeaderMap::new(), cookie(&headers))
    } else {
        let data = match session(&state, &headers).await {
            Ok(data) => data,
            Err(status) => return status.into_response(),
        };
        match api_headers(&state, &data).await {
            Ok(h) => (h, cookie(&headers)),
            Err(status) => return status.into_response(),
        }
    };
    let principal = match auth::scoped_auth(&state.server, &auth_headers, &["jobs:read"]).await {
        Ok(principal) => principal,
        Err(e) => return e.into_response(),
    };
    // The rule of running the action from the console (`POST /execute`, one shared function):
    // administrators, and other users only when `api:permissions:actions:non_admin` allows them,
    // so a user who could not have started a job cannot read its output.
    let allowed = auth::is_user_allowed_to_execute_console(&state.server, principal.as_ref()).await;
    if !allowed.success {
        return (StatusCode::FORBIDDEN, allowed.message).into_response();
    }
    if !state.server.jobs.exists(&name).await {
        return StatusCode::NOT_FOUND.into_response();
    }
    // An `execute-*` job's output is its creator's and the administrators' (the job metadata
    // routes apply the same rule); anyone else sees it as absent.
    {
        let admin = auth::is_admin(&state.server, principal.as_ref()).await;
        let meta = state.server.jobs.get(&name).await;
        let caller = meerschaum_server::routes::execute::owner_label(principal.as_ref());
        if !meerschaum_server::routes::execute::job_visible_to(
            admin,
            caller.as_deref(),
            &name,
            meta.as_ref(),
        ) {
            return StatusCode::NOT_FOUND.into_response();
        }
    }
    // Each stream follows its job on a thread: bounded in total and per user.
    let slot = match state.streams.take(
        &slot_key(principal.as_ref(), session_id.as_deref()),
        state.server.config.log_streams_max,
        state.server.config.log_streams_per_user,
    ) {
        Ok(slot) => slot,
        Err(why) => {
            let mut response = (StatusCode::TOO_MANY_REQUESTS, why).into_response();
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, "5".parse().unwrap());
            return response;
        }
    };
    let Some(monitor) = state.server.jobs.monitor_logs(&name).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut ticker =
        tokio::time::interval_at(tokio::time::Instant::now() + SESSION_CHECK, SESSION_CHECK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let stream = futures_util::stream::unfold(
        (monitor, state, session_id, false, ticker, slot),
        |(mut monitor, state, id, done, mut ticker, slot)| async move {
            if done {
                return None;
            }
            let event = loop {
                tokio::select! {
                    event = monitor.events.recv() => {
                        let event = event?;
                        // A session that ended (logout, expiry) ends its streams.
                        if session_ended(&state, id.as_deref()).await {
                            return None;
                        }
                        break event;
                    }
                    // ... also while the job is silent.
                    _ = ticker.tick() => {
                        if session_ended(&state, id.as_deref()).await {
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
                (monitor, state, id, done, ticker, slot),
            ))
        },
    );
    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    // A reverse proxy in front of the server must not buffer the stream.
    response
        .headers_mut()
        .insert("x-accel-buffering", "no".parse().unwrap());
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_are_taken_per_key_and_freed_on_drop() {
        let slots = Arc::new(StreamSlots::default());
        let a1 = slots.take("user:a", 3, 2).unwrap();
        let _a2 = slots.take("user:a", 3, 2).unwrap();
        assert!(slots.take("user:a", 3, 2).unwrap_err().contains("user"));
        let _b = slots.take("user:b", 3, 2).unwrap();
        assert!(slots.take("user:c", 3, 2).unwrap_err().contains("server"));
        assert_eq!(slots.total(), 3);
        drop(a1);
        assert_eq!(slots.total(), 2);
        assert!(slots.take("user:a", 3, 2).is_ok());
    }
}
