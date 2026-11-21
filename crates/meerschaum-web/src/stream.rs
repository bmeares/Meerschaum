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

/// How often a stream re-authorizes its caller and the job's visibility, re-minting a console
/// session's short-lived bearer (it lives `CONSOLE_BEARER_SECONDS`), so a live console session
/// never expires mid-stream.
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
                    event = monitor.events.recv() => break event?,
                    // Re-authorize at an interval, never per log line: a console session is
                    // checked again (an ended session or a rotated credential ends the stream)
                    // and its next short-lived bearer minted, then the caller's permissions and
                    // the job's visibility are checked with it.
                    _ = ticker.tick() => {
                        let headers = guard.reauthorize(&state.server).await?;
                        let caller = meerschaum_server::access::require_fresh(
                            &state.server,
                            headers,
                            SCOPES,
                        )
                        .await
                        .ok()?;
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use futures_util::StreamExt;
    use indexmap::IndexMap;
    use meerschaum_core::users::User;
    use meerschaum_core::{Map, SuccessTuple};
    use meerschaum_server::backends::{CreateJob, JobsBackend, LogMonitor};
    use meerschaum_server::sse::StreamSlots;
    use meerschaum_server::{ServerConfig, auth};
    use tokio::sync::mpsc;

    use super::*;
    use crate::engine_auth::ConsoleSessionAuth;
    use crate::sessions::{COOKIE, SessionStore};
    use crate::test_memory as memory;

    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef0123456789abcdef";
    const JOB: &str = "execute-held";
    const SESSION: &str = "6f1d3c2a-5b4e-4f7a-9c8d-0e1f2a3b4c5d";
    /// The test's console bearers live this long (the console's live a minute).
    const BEARER_SECONDS: i64 = 2;

    /// Console sessions in memory, counting the reads of the session store.
    #[derive(Default)]
    struct CountingSessions {
        rows: Mutex<HashMap<String, Json>>,
        reads: AtomicUsize,
    }

    impl CountingSessions {
        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl SessionStore for CountingSessions {
        async fn get(&self, id: &str) -> Result<Option<Json>, String> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.rows.lock().unwrap().get(id).cloned())
        }
        async fn set(&self, id: &str, payload: Json, _ttl: i64) -> Result<(), String> {
            self.rows.lock().unwrap().insert(id.into(), payload);
            Ok(())
        }
        async fn delete(&self, id: &str) -> Result<(), String> {
            self.rows.lock().unwrap().remove(id);
            Ok(())
        }
    }

    /// One running job whose log lines the test writes.
    #[derive(Default)]
    struct HeldJob {
        lines: Mutex<Option<mpsc::Sender<LogEvent>>>,
    }

    #[async_trait]
    impl JobsBackend for HeldJob {
        async fn list(&self) -> IndexMap<String, Json> {
            IndexMap::new()
        }
        async fn get(&self, name: &str) -> Option<Json> {
            (name == JOB).then(|| json!({"daemon": {"properties": {}}}))
        }
        async fn exists(&self, name: &str) -> bool {
            name == JOB
        }
        async fn create(&self, _: &str, _: Vec<String>, _: Option<Map>) -> CreateJob {
            CreateJob::Started(SuccessTuple::success())
        }
        async fn delete(&self, _: &str) -> SuccessTuple {
            SuccessTuple::success()
        }
        async fn start(&self, _: &str) -> SuccessTuple {
            SuccessTuple::success()
        }
        async fn stop(&self, _: &str) -> SuccessTuple {
            SuccessTuple::success()
        }
        async fn pause(&self, _: &str) -> SuccessTuple {
            SuccessTuple::success()
        }
        async fn logs(&self, _: &str) -> Option<String> {
            None
        }
        async fn stop_time(&self, _: &str) -> Option<DateTime<Utc>> {
            None
        }
        async fn is_blocking_on_stdin(&self, _: &str) -> bool {
            false
        }
        async fn prompt_kwargs(&self, _: &str) -> Map {
            Map::new()
        }
        async fn monitor_logs(&self, name: &str) -> Option<LogMonitor> {
            if name != JOB {
                return None;
            }
            let (tx, events) = mpsc::channel(64);
            let (input, _) = mpsc::channel(1);
            *self.lines.lock().unwrap() = Some(tx);
            Some(LogMonitor { events, input })
        }
    }

    /// A console job-log stream re-authorizes its session at its interval, never per log line;
    /// each re-authorization mints the next short-lived bearer, so the stream outlives the
    /// bearer it opened with, and ends once the session's credential rotates.
    #[tokio::test(start_paused = true)]
    async fn a_console_log_stream_outlives_its_bearer_and_ends_when_its_credential_rotates() {
        let root = tempfile::tempdir().unwrap();
        let instance = Arc::new(memory::MemoryInstance::new("fixture"));
        instance.seed_user("admin", "pass", Some("admin"));
        let jobs = Arc::new(HeldJob::default());
        let server =
            AppState::builder(ServerConfig::for_root("sql:fixture", root.path()), instance)
                .secret(SECRET.to_vec())
                .jobs(jobs.clone())
                .build()
                .unwrap();
        let sessions = Arc::new(CountingSessions::default());
        let credential = auth::user_credential(&server, &User::new("admin", None))
            .await
            .unwrap();
        sessions
            .set(
                SESSION,
                json!({"username": "admin", "credential": credential}),
                3600,
            )
            .await
            .unwrap();
        let state = WebState {
            server,
            config: WebConfig {
                sessions: sessions.clone(),
                webterm: None,
                pages: vec![],
            },
            streams: StreamSlots::new("log streams"),
            browser_auth: Arc::new(ConsoleSessionAuth::with_bearer_lifetime(
                sessions.clone(),
                BEARER_SECONDS,
            )),
            grafana: None,
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            format!("{COOKIE}={SESSION}").parse().unwrap(),
        );
        let response = logs(State(state), headers, Path(JOB.into())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();
        let lines = jobs.lines.lock().unwrap().clone().unwrap();

        // Log lines never re-authorize the session.
        let opened = sessions.reads();
        for i in 0..50 {
            lines
                .send(LogEvent::Line(format!("line {i}")))
                .await
                .unwrap();
            let frame = body.next().await.unwrap().unwrap();
            assert!(String::from_utf8_lossy(&frame).contains(&format!("line {i}")));
        }
        assert_eq!(
            sessions.reads(),
            opened,
            "a log line re-authorized the session"
        );

        // The bearer minted at open expires (bearers expire by the wall clock) ...
        std::thread::sleep(Duration::from_millis(
            u64::try_from(BEARER_SECONDS).unwrap() * 1000 + 1500,
        ));
        // ... and the stream outlives it: its next re-authorization mints another.
        let idle = tokio::time::timeout(SESSION_CHECK + Duration::from_secs(2), body.next()).await;
        assert!(
            idle.is_err(),
            "the stream ended with its first bearer: {idle:?}"
        );
        assert_eq!(sessions.reads(), opened + 1);
        lines
            .send(LogEvent::Line("still here".into()))
            .await
            .unwrap();
        let frame = body.next().await.unwrap().unwrap();
        assert!(String::from_utf8_lossy(&frame).contains("still here"));

        // A rotated credential (a password change) ends the stream at its next check.
        sessions
            .set(
                SESSION,
                json!({"username": "admin", "credential": "rotated-stale-hash"}),
                3600,
            )
            .await
            .unwrap();
        let ended = tokio::time::timeout(SESSION_CHECK * 3, async {
            while let Some(frame) = body.next().await {
                frame.unwrap();
            }
        })
        .await;
        assert!(ended.is_ok(), "the stream outlived its rotated credential");
    }
}
