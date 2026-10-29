//! `GET /engine/runs/{run_id}/events/stream`: a run's typed engine events as Server-Sent Events
//! (§5.7, §5.8). One SSE event per engine event: `event:` is its kind (`node_started`,
//! `chunk_finished`, ...), `id:` its per-run `seq` and `data:` its JSON
//! ([`EngineRunEvent`], the shape of `meerschaum_engine::Event`). The stream ends after
//! `run_finished`; a client that reconnects with `Last-Event-ID: <seq>` (or `?after_seq=`)
//! resumes after that event.
//!
//! Where the events come from:
//!
//! - **Live**: events this process sees as they happen go through [`EngineStreams::publish`]:
//!   the batches an engine on an `api:` instance writes to this server
//!   (`POST /engine/pipes/engine_events/write`), and the engine runs inside the server
//!   process (`POST /actions/sync`, MCP action tools), which `mrsm start api` forwards with
//!   `meerschaum_engine::add_process_event_sink` and [`EngineStreams::publish_json`]. Each
//!   carries the instance that keeps its run, and a stream takes only its own instance's events
//!   (run IDs are unique per instance, not across instances).
//! - **Persisted**: otherwise (a run of a job, or of a `mrsm` process on another machine that
//!   shares the instance) the stream tails the instance's `mrsm_engine_events` rows after the
//!   last delivered `seq` every [`POLL`] until `run_finished`.
//!
//! Both feed one ordered buffer, so an event is delivered once and in `seq` order. A gap in the
//! sequence (an event not flushed yet, or one `system.engine.events: nodes` does not keep) is
//! waited for at most [`GAP_GRACE`], once for all the events received by then (so the delay
//! does not add up gap by gap). Comments (`: keep-alive`) every [`HEARTBEAT`] keep proxies from
//! closing an idle stream.
//!
//! Every stream ends with a terminal event: `run_finished`, or a synthetic `run_closed` (no
//! `id:`; `seq` repeats the last delivered one) when the run will not send `run_finished`. Its
//! `reason` is `interrupted` (the run's record closed without `run_finished`), `holder_gone`
//! (the run's holder `mrsm:<host>:<pid>` names the server's host and a process that is gone, and
//! the run has been quiet for [`HOLDER_GRACE`]: a killed process; a holder of another host, or a
//! release candidate's `mrsm:<pid>`, is left to the idle timeout) or `idle` (no event for `?idle_timeout=`
//! seconds, default [`IDLE_TIMEOUT`]; the run may still be going). It also carries `finished`
//! (whether the record closed), `success` and `message` from the run's record.
//!
//! Authorization is that of `GET /engine/runs` (`pipes:read`), by bearer token or, for the
//! console's `EventSource`, by the console session cookie when `meerschaum-web` installs a
//! [`BrowserAuth`]. Open streams are bounded like the job-log streams
//! (`api:permissions:log_streams`, a separate count). A run the store cannot be read for is an
//! error (`500`, or `501` without a store), never "not found".

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use meerschaum_connector::pipe_ops::Doc;
use meerschaum_connector_api::models::EngineRunEvent;
use meerschaum_kernel::value::Value as KValue;
use serde_json::Value as Json;
use tokio::sync::broadcast;
use tokio::time::Instant;

use crate::backends::{EngineBackend, EngineStoreError, RunsFilter};
use crate::error::ApiError;
use crate::routes::Ctx;
use crate::state::AppState;
use meerschaum_connector_api::models::RunRecord;

/// How often a stream reads the persisted events.
pub const POLL: Duration = Duration::from_millis(500);
/// How long a gap in the sequence is waited for before the events after it are delivered.
pub const GAP_GRACE: Duration = Duration::from_secs(1);
/// How often an idle stream sends a comment.
pub const HEARTBEAT: Duration = Duration::from_secs(15);
/// How often a stream checks the run's record (an interrupted run never sends `run_finished`).
const STATUS_CHECK: Duration = Duration::from_secs(5);
/// How long a stream keeps reading after the run's record says it finished.
const CLOSE_GRACE: Duration = Duration::from_secs(3);
/// How often a console stream re-checks that its session still exists.
const SESSION_CHECK: Duration = Duration::from_secs(10);
/// The most a request may wait (`?wait=`) for a run that has not started yet.
pub const MAX_WAIT: Duration = Duration::from_secs(60);
/// How long a run may go without any event before its stream ends (`run_closed`, `idle`);
/// `?idle_timeout=` (seconds, [`MIN_IDLE_TIMEOUT`] to [`MAX_IDLE_TIMEOUT`]) changes it.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(15 * 60);
/// The shortest `?idle_timeout=`.
pub const MIN_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest `?idle_timeout=`.
pub const MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
/// How long a run whose holder process is gone may stay quiet before its stream ends
/// (`run_closed`, `holder_gone`): twice the engine's default lease TTL.
pub const HOLDER_GRACE: Duration = Duration::from_secs(120);
/// Persisted reads look back this far from the newest event seen (event timestamps are taken
/// after their sequence numbers, so they are only nearly ordered).
const TS_SLACK: chrono::TimeDelta = chrono::TimeDelta::seconds(60);

/// The scopes of the stream (those of `GET /engine/runs`).
const SCOPES: &[&str] = &["pipes:read"];

/// Authenticates a browser request that carries no `Authorization` header (the console's
/// `EventSource` cannot set one) by its console session. `meerschaum-web` installs one with
/// [`EngineStreams::set_browser_auth`].
#[async_trait]
pub trait BrowserAuth: Send + Sync {
    /// `None` when the request has no console session; otherwise the headers to authorize it
    /// with (a short-lived bearer for the session's user) and the session ID, or the status to
    /// answer (`401`, `503`).
    async fn authorize(
        &self,
        state: &AppState,
        headers: &HeaderMap,
    ) -> Option<Result<(HeaderMap, String), StatusCode>>;

    /// Whether the session still exists (a logout ends its streams).
    async fn session_active(&self, state: &AppState, session_id: &str) -> bool;

    /// On a `--no-auth` server: who an unauthenticated request counts against for the
    /// per-user stream limit (the console cookie). `None`: only the server-wide limit applies.
    fn anonymous_key(&self, _headers: &HeaderMap) -> Option<String> {
        None
    }
}

/// An event published live, with the keys of the instance that keeps its run.
#[derive(Debug, Clone)]
pub struct LiveRunEvent {
    /// The instance that keeps the run (`sql:main`).
    pub instance: Arc<str>,
    /// The event.
    pub event: EngineRunEvent,
}

/// The live side of the run event streams, one per server
/// ([`Inner::engine_streams`](crate::state::Inner::engine_streams)).
pub struct EngineStreams {
    tx: broadcast::Sender<LiveRunEvent>,
    browser_auth: OnceLock<Arc<dyn BrowserAuth>>,
    open: Arc<Mutex<(usize, HashMap<String, usize>)>>,
}

impl Default for EngineStreams {
    fn default() -> Self {
        EngineStreams {
            tx: broadcast::channel(4096).0,
            browser_auth: OnceLock::new(),
            open: Arc::default(),
        }
    }
}

impl std::fmt::Debug for EngineStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineStreams")
            .field("receivers", &self.tx.receiver_count())
            .finish_non_exhaustive()
    }
}

impl EngineStreams {
    /// Deliver an event of a run kept on `instance` (its keys) to the open streams of that run
    /// now (it is persisted separately).
    pub fn publish(&self, instance: &str, event: EngineRunEvent) {
        let _ = self.tx.send(LiveRunEvent {
            instance: instance.into(),
            event,
        });
    }

    /// [`publish`](Self::publish) an engine event given as its JSON (`serde_json::to_value` of a
    /// `meerschaum_engine::Event`): how a host that runs an engine in the server process forwards
    /// its process events (`instance` is the engine's home instance, which keeps its runs).
    /// `false` if the JSON is not an event.
    pub fn publish_json(&self, instance: &str, event: &Json) -> bool {
        match serde_json::from_value::<EngineRunEvent>(event.clone()) {
            Ok(e) => {
                self.publish(instance, e);
                true
            }
            Err(_) => false,
        }
    }

    /// [`publish`](Self::publish) the rows of a `mrsm_engine_events` write to `instance`.
    pub fn publish_docs(&self, instance: &str, docs: &[Doc]) {
        if self.tx.receiver_count() == 0 {
            return;
        }
        let instance: Arc<str> = instance.into();
        let mut events: Vec<EngineRunEvent> = docs.iter().filter_map(event_from_doc).collect();
        events.sort_by_key(|e| e.seq);
        for event in events {
            let _ = self.tx.send(LiveRunEvent {
                instance: instance.clone(),
                event,
            });
        }
    }

    /// Whether any stream is open (publishing is wasted otherwise).
    pub fn is_followed(&self) -> bool {
        self.tx.receiver_count() > 0
    }

    /// A receiver of the published events.
    pub fn subscribe(&self) -> broadcast::Receiver<LiveRunEvent> {
        self.tx.subscribe()
    }

    /// Install the console session authentication (once; `false` if one is installed).
    pub fn set_browser_auth(&self, auth: Arc<dyn BrowserAuth>) -> bool {
        self.browser_auth.set(auth).is_ok()
    }

    /// The installed console session authentication.
    pub fn browser_auth(&self) -> Option<Arc<dyn BrowserAuth>> {
        self.browser_auth.get().cloned()
    }

    fn take_slot(&self, key: &str, max: usize, per_user: usize) -> Result<Slot, &'static str> {
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        if open.0 >= max {
            return Err("Too many event streams are open on this server.");
        }
        if open.1.get(key).copied().unwrap_or(0) >= per_user {
            return Err("Too many event streams are open for this user.");
        }
        open.0 += 1;
        *open.1.entry(key.to_string()).or_insert(0) += 1;
        Ok(Slot {
            open: self.open.clone(),
            key: key.to_string(),
        })
    }

    /// The number of open streams (for tests).
    pub fn open_streams(&self) -> usize {
        self.open.lock().unwrap_or_else(|e| e.into_inner()).0
    }
}

/// One open stream; freed when the stream drops.
struct Slot {
    open: Arc<Mutex<(usize, HashMap<String, usize>)>>,
    key: String,
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        open.0 = open.0.saturating_sub(1);
        if let Some(n) = open.1.get_mut(&self.key) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                open.1.remove(&self.key);
            }
        }
    }
}

fn doc_json(v: &KValue) -> Json {
    match v {
        v if v.is_na() => Json::Null,
        KValue::Json(j) => j.clone(),
        KValue::Str(s) => serde_json::from_str(s).unwrap_or_else(|_| Json::String(s.clone())),
        KValue::Bool(b) => Json::Bool(*b),
        KValue::Int(i) => Json::from(*i),
        other => Json::String(other.py_str()),
    }
}

fn doc_string(doc: &Doc, key: &str) -> Option<String> {
    match doc.get(key)? {
        v if v.is_na() => None,
        KValue::Str(s) => Some(s.clone()),
        other => Some(other.py_str()),
    }
}

/// A `mrsm_engine_events` row as an event (`None` if it lacks a run, `seq` or event name).
pub fn event_from_doc(doc: &Doc) -> Option<EngineRunEvent> {
    let seq = match doc.get("seq")? {
        KValue::Int(i) => *i,
        KValue::Float(f) if f.is_finite() => *f as i64,
        KValue::Str(s) => s.trim().parse().ok()?,
        _ => return None,
    };
    let ts = match doc.get("ts") {
        Some(KValue::Datetime(d)) => DateTime::from_naive_utc_and_offset(d.utc, Utc),
        Some(KValue::Str(s)) => meerschaum_kernel::value::parse_datetime_value(s)
            .map(|d| DateTime::from_naive_utc_and_offset(d.utc, Utc))?,
        _ => return None,
    };
    Some(EngineRunEvent::from_row(
        &doc_string(doc, "run_id")?,
        seq,
        ts,
        doc_string(doc, "pipe_ref").filter(|r| !r.is_empty() && r != "None"),
        &doc_string(doc, "event")?,
        doc.get("payload").map(doc_json).unwrap_or(Json::Null),
    ))
}

/// Whether `run_id` can name a run (the engine's run IDs, `--run-id` values). A leading `_`
/// is refused: the `params` grammar would read it as a negation (`run_id != ...`).
pub fn valid_run_id(run_id: &str) -> bool {
    run_id.len() <= 200
        && run_id
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && run_id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.:".contains(&c))
}

/// A store error as a response: `501` without a store, else `500` (its message is for
/// administrators, [`crate::envelope`]).
pub fn api_error(e: EngineStoreError) -> ApiError {
    match e {
        EngineStoreError::Unavailable => ApiError::new(501, e.to_string()),
        EngineStoreError::Failed(m) => {
            tracing::warn!("Failed to read the engine state: {m}");
            ApiError::new(500, "Failed to read the engine state.").with_internal(m)
        }
    }
}

/// Who a stream counts against.
fn slot_key(principal: Option<&crate::auth::Principal>, session: Option<&str>) -> String {
    match principal {
        Some(crate::auth::Principal::User(user)) => format!("user:{}", user.username),
        Some(crate::auth::Principal::Token(token)) => {
            format!(
                "token:{}",
                token.id.map(|id| id.to_string()).unwrap_or_default()
            )
        }
        None => format!("session:{}", session.unwrap_or("")),
    }
}

/// The store that keeps the run's history: the server's engine store, or (`instance_keys`
/// naming another instance the server may use) that instance's state pipes. Also the keys
/// under which that instance's runs are published live ([`own_instance_names`] for the server's).
pub fn store_for(
    ctx: &Ctx,
    caller: &crate::access::Access,
) -> Result<(Arc<dyn EngineBackend>, Vec<String>), ApiError> {
    let conn = ctx
        .state
        .get_api_connector(ctx.instance_keys().as_deref())?;
    crate::access::guard::check_connector_teams(&ctx.state, caller, &conn.connector_keys())?;
    match ctx.instance_keys() {
        Some(keys)
            if keys != ctx.state.config.instance_keys
                && keys != ctx.state.instance().connector_keys() =>
        {
            let conn = ctx.state.get_api_connector(Some(&keys))?;
            Ok((
                Arc::new(crate::engine_store::InstanceEngine::new(conn, &keys)),
                vec![keys],
            ))
        }
        _ => Ok((ctx.state.engine.clone(), own_instance_names(&ctx.state))),
    }
}

/// The names of the server's own instance (the configured keys and the connector's keys).
pub fn own_instance_names(state: &AppState) -> Vec<String> {
    let mut names = vec![state.config.instance_keys.clone()];
    let keys = state.instance().connector_keys();
    if !names.contains(&keys) {
        names.push(keys);
    }
    names
}

async fn run_header_exists(store: &dyn EngineBackend, run_id: &str) -> Result<bool, ApiError> {
    let filter = RunsFilter {
        run_id: Some(run_id.into()),
        limit: Some(1),
        ..Default::default()
    };
    Ok(store
        .get_runs(&filter)
        .await
        .map_err(api_error)?
        .iter()
        .any(|row| row.pipe_ref.is_none()))
}

/// Whether the store knows the run (a run record or any event). A failed read is an error.
async fn run_known(store: &dyn EngineBackend, run_id: &str) -> Result<bool, ApiError> {
    let filter = RunsFilter {
        run_id: Some(run_id.to_string()),
        limit: Some(1),
        ..Default::default()
    };
    if !store.get_runs(&filter).await.map_err(api_error)?.is_empty() {
        return Ok(true);
    }
    Ok(!store
        .run_events(run_id, None, None)
        .await
        .map_err(api_error)?
        .is_empty())
}

/// The handler of `GET /engine/runs/{run_id}/events/stream`.
pub async fn serve(ctx: Ctx, run_id: String) -> Result<Response, ApiError> {
    let state = ctx.state.clone();
    // A bearer wins; otherwise the console session (if `meerschaum-web` installed one).
    let mut session = None;
    let auth_headers = match state.engine_streams.browser_auth() {
        Some(auth) if !ctx.headers.contains_key(header::AUTHORIZATION) => {
            match auth.authorize(&state, &ctx.headers).await {
                Some(Ok((headers, id))) => {
                    session = Some((auth, id));
                    headers
                }
                Some(Err(status)) => return Ok(status.into_response()),
                None => ctx.headers.clone(),
            }
        }
        _ => ctx.headers.clone(),
    };
    let caller = crate::access::require_fresh(&state, &auth_headers, SCOPES).await?;
    let principal = caller.auth.clone();
    if !valid_run_id(&run_id) {
        return Err(ApiError::new(422, "Invalid run ID."));
    }
    let after = match ctx
        .headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        Some(id) => Some(
            id.parse::<i64>()
                .map_err(|_| ApiError::new(422, "`Last-Event-ID` must be an event's `seq`."))?,
        ),
        None => ctx.query.opt_int("after_seq")?,
    };
    let wait = ctx
        .query
        .get("wait")
        .and_then(|w| w.trim().parse::<f64>().ok())
        .filter(|w| w.is_finite() && *w > 0.0)
        // Clamped before converting: `from_secs_f64` panics past `Duration::MAX`.
        .map_or(Duration::ZERO, |w| {
            Duration::from_secs_f64(w.min(MAX_WAIT.as_secs_f64()))
        });
    let idle_timeout = ctx
        .query
        .get("idle_timeout")
        .and_then(|w| w.trim().parse::<f64>().ok())
        .filter(|w| w.is_finite() && *w > 0.0)
        .map_or(IDLE_TIMEOUT, |w| {
            Duration::from_secs_f64(w.min(MAX_IDLE_TIMEOUT.as_secs_f64()))
                .clamp(MIN_IDLE_TIMEOUT, MAX_IDLE_TIMEOUT)
        });
    let (store, instances) = store_for(&ctx, &caller)?;
    // Subscribe before the first read, so no event falls between the two.
    let hub = state.engine_streams.subscribe();
    // Waiting for a run to appear holds no stream slot (it is bounded by `MAX_WAIT`).
    let deadline = Instant::now() + wait;
    while !run_known(store.as_ref(), &run_id).await? {
        if Instant::now() >= deadline {
            return Err(ApiError::new(404, format!("Run '{run_id}' was not found.")));
        }
        tokio::time::sleep(POLL).await;
    }
    // A caller who is not an administrator follows the runs of pipes it can see (rc15); the
    // events of hidden pipes reach it anonymized.
    let caller = crate::access::require_fresh(&state, &auth_headers, SCOPES).await?;
    let filter = if caller.is_admin() {
        None
    } else {
        let default = instances.first().cloned().unwrap_or_default();
        loop {
            let fresh = crate::access::require_fresh(&state, &auth_headers, SCOPES).await?;
            let mut filter = crate::access::RefFilter::new(&state, &fresh, &default).for_engine();
            let visible =
                crate::routes::engine::visible_run_detail(store.as_ref(), &run_id, &mut filter)
                    .await
                    .map_err(api_error)?;
            if visible.is_some() {
                break Some(filter);
            }
            // An event alone cannot prove Pipeline visibility. Honor an early-run wait,
            // but a known inaccessible header is refused immediately.
            if Instant::now() >= deadline || run_header_exists(store.as_ref(), &run_id).await? {
                return Err(ApiError::new(404, format!("Run '{run_id}' was not found.")));
            }
            tokio::time::sleep(POLL).await;
        }
    };
    // Without authentication there is no user: count the console cookie's streams, or else
    // only the server-wide total.
    let (key, per_user) = match (&principal, &session) {
        (None, None) => match state
            .engine_streams
            .browser_auth()
            .and_then(|auth| auth.anonymous_key(&ctx.headers))
        {
            Some(key) => (key, state.config.log_streams_per_user),
            None => ("anonymous".to_string(), usize::MAX),
        },
        _ => (
            slot_key(
                principal.as_ref(),
                session.as_ref().map(|(_, id)| id.as_str()),
            ),
            state.config.log_streams_per_user,
        ),
    };
    let slot = match state
        .engine_streams
        .take_slot(&key, state.config.log_streams_max, per_user)
    {
        Ok(slot) => slot,
        Err(why) => {
            let mut response = (StatusCode::TOO_MANY_REQUESTS, why).into_response();
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
            return Ok(response);
        }
    };
    let mut tail = Tail {
        state,
        store,
        instances,
        run_id,
        last: after,
        since: None,
        pending: BTreeMap::new(),
        gap_since: None,
        hub: Some(hub),
        poll: tokio::time::interval_at(Instant::now() + POLL, POLL),
        next_status_check: Instant::now(),
        closed_at: None,
        record: None,
        holder_gone: false,
        opened: Utc::now(),
        newest_ts: None,
        idle_timeout,
        gap_through: None,
        terminal: None,
        session,
        next_session_check: Instant::now() + SESSION_CHECK,
        done: false,
        filter,
        auth_headers,
        next_access_check: Instant::now() + STATUS_CHECK,
        _slot: slot,
    };
    tail.poll
        .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tail.read_persisted().await;
    let stream = futures_util::stream::unfold(tail, |mut tail| async move {
        let event = tail.next().await?;
        Some((Ok::<_, Infallible>(event), tail))
    });
    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(HEARTBEAT).text("keep-alive"))
        .into_response();
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    // nginx buffers proxied responses unless told not to.
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    Ok(response)
}

/// One stream's state.
struct Tail {
    state: AppState,
    store: Arc<dyn EngineBackend>,
    /// The keys of the instance that keeps the run (live events of other instances are not
    /// this run's, even under the same run ID).
    instances: Vec<String>,
    run_id: String,
    /// The last delivered `seq`.
    last: Option<i64>,
    /// The newest persisted timestamp seen.
    since: Option<DateTime<Utc>>,
    /// Received, not yet delivered (by `seq`).
    pending: BTreeMap<i64, EngineRunEvent>,
    /// Since when the next event has waited for a gap.
    gap_since: Option<Instant>,
    hub: Option<broadcast::Receiver<LiveRunEvent>>,
    poll: tokio::time::Interval,
    next_status_check: Instant,
    /// When the run's record was first seen finished.
    closed_at: Option<Instant>,
    /// The run's record, as of the last status check.
    record: Option<RunRecord>,
    /// Whether the run's holder (`mrsm:<host>:<pid>`) is a gone process of this host.
    holder_gone: bool,
    /// When the stream opened.
    opened: DateTime<Utc>,
    /// The newest event timestamp received (from either side).
    newest_ts: Option<DateTime<Utc>>,
    /// How long the run may stay quiet.
    idle_timeout: Duration,
    /// Events up to this `seq` are delivered without waiting for gaps (a grace expired).
    gap_through: Option<i64>,
    /// The synthetic `run_closed` to send before ending.
    terminal: Option<EngineRunEvent>,
    session: Option<(Arc<dyn BrowserAuth>, String)>,
    next_session_check: Instant,
    done: bool,
    /// For a caller who is not an administrator: anonymizes the events of hidden pipes.
    filter: Option<crate::access::RefFilter>,
    auth_headers: axum::http::HeaderMap,
    next_access_check: Instant,
    _slot: Slot,
}

impl Tail {
    fn insert(&mut self, event: EngineRunEvent) {
        if event.run_id != self.run_id {
            return;
        }
        self.newest_ts = Some(self.newest_ts.map_or(event.ts, |t| t.max(event.ts)));
        if self.last.is_none_or(|last| event.seq > last) {
            self.pending.entry(event.seq).or_insert(event);
        }
    }

    async fn read_persisted(&mut self) {
        let since = self.since.map(|t| t - TS_SLACK);
        // Unfiltered by `seq`, so `since` advances on what was already delivered too and the
        // next read stays a short window instead of the run's whole history.
        match self.store.run_events(&self.run_id, None, since).await {
            Ok(events) => {
                for event in events {
                    self.since = Some(self.since.map_or(event.ts, |t| t.max(event.ts)));
                    self.insert(event);
                }
            }
            Err(e) => tracing::debug!("Cannot read the events of run '{}': {e}", self.run_id),
        }
    }

    /// The next event in order, if it may be delivered now.
    fn release(&mut self) -> Option<EngineRunEvent> {
        let (&seq, _) = self.pending.first_key_value()?;
        let contiguous = self.last.is_none_or(|last| seq == last + 1)
            || self.gap_through.is_some_and(|through| seq <= through);
        if !contiguous && self.closed_at.is_none() {
            let since = *self.gap_since.get_or_insert_with(Instant::now);
            if since.elapsed() < GAP_GRACE {
                return None;
            }
            // One grace for every event received by now, not one per gap.
            self.gap_through = self.pending.last_key_value().map(|(&s, _)| s);
        }
        self.gap_since = None;
        let event = self.pending.remove(&seq)?;
        self.last = Some(seq);
        Some(event)
    }

    async fn next(&mut self) -> Option<SseEvent> {
        loop {
            if self.done {
                return None;
            }
            if Instant::now() >= self.next_access_check {
                self.next_access_check = Instant::now() + STATUS_CHECK;
                self.refresh_access().await?;
            }
            if let Some(mut event) = self.release() {
                self.done = event.is_run_finished();
                crate::access::runs::strip_event_proofs(&mut event);
                let event = match self.filter.as_mut() {
                    Some(filter) => crate::access::runs::visible_event(filter, event).await,
                    None => event,
                };
                let data = serde_json::to_string(&event).unwrap_or_default();
                return Some(
                    SseEvent::default()
                        .event(&event.event)
                        .id(event.seq.to_string())
                        .data(data),
                );
            }
            if self.pending.is_empty()
                && let Some(mut event) = self.terminal.take()
            {
                self.done = true;
                crate::access::runs::strip_event_proofs(&mut event);
                let event = match self.filter.as_mut() {
                    Some(filter) => crate::access::runs::visible_event(filter, event).await,
                    None => event,
                };
                // No `id:`: a reconnect resumes after the last real event.
                return Some(
                    SseEvent::default()
                        .event(&event.event)
                        .data(serde_json::to_string(&event).unwrap_or_default()),
                );
            }
            let received = async {
                match self.hub.as_mut() {
                    Some(hub) => Some(hub.recv().await),
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                received = received => match received {
                    Some(Ok(live)) => {
                        if self.instances.iter().any(|i| **i == *live.instance) {
                            self.insert(live.event);
                        }
                    }
                    // Missed live events are read back from the instance on the next poll.
                    Some(Err(broadcast::error::RecvError::Lagged(_))) => {}
                    Some(Err(broadcast::error::RecvError::Closed)) | None => self.hub = None,
                },
                _ = self.poll.tick() => self.tick().await?,
            }
        }
    }

    /// A long-lived stream must stop when its credential or saved Pipeline access changes.
    async fn refresh_access(&mut self) -> Option<()> {
        let caller = crate::access::require_fresh(&self.state, &self.auth_headers, SCOPES)
            .await
            .ok()?;
        let default = self.instances.first().cloned().unwrap_or_default();
        let conn = self.state.get_api_connector(Some(&default)).ok()?;
        crate::access::guard::check_connector_teams(&self.state, &caller, &conn.connector_keys())
            .ok()?;
        if caller.is_admin() {
            self.filter = None;
            return Some(());
        }
        let mut filter = crate::access::RefFilter::new(&self.state, &caller, &default).for_engine();
        crate::routes::engine::visible_run_detail(self.store.as_ref(), &self.run_id, &mut filter)
            .await
            .ok()??;
        self.filter = Some(filter);
        Some(())
    }

    /// A poll: read the persisted events, check the run's record and the session. `None` ends
    /// the stream.
    async fn tick(&mut self) -> Option<()> {
        self.read_persisted().await;
        let now = Instant::now();
        if let Some((auth, id)) = &self.session
            && now >= self.next_session_check
        {
            if !auth.session_active(&self.state, id).await {
                return None;
            }
            self.next_session_check = now + SESSION_CHECK;
        }
        if self.closed_at.is_none() && now >= self.next_status_check {
            self.next_status_check = now + STATUS_CHECK;
            let filter = RunsFilter {
                run_id: Some(self.run_id.clone()),
                limit: Some(1),
                ..Default::default()
            };
            // A failed read keeps the last record (the next check tries again).
            match self.store.get_runs(&filter).await {
                Ok(runs) => self.record = runs.into_iter().find(|r| r.pipe_ref.is_none()),
                Err(e) => tracing::debug!("Cannot read the record of run '{}': {e}", self.run_id),
            }
            if self.record.as_ref().is_some_and(|r| r.finished.is_some()) {
                self.closed_at = Some(now);
            }
            self.holder_gone = self
                .record
                .as_ref()
                .and_then(|r| r.extra.get("holder"))
                .and_then(|h| h.as_str())
                .is_some_and(holder_gone);
        }
        if self.terminal.is_some() || !self.pending.is_empty() {
            return Some(());
        }
        // A run that will not send `run_finished`: deliver what is left, then say why.
        let quiet = (Utc::now()
            - self.newest_ts.unwrap_or_else(|| {
                self.record
                    .as_ref()
                    .map_or(self.opened, |r| r.started.max(self.opened))
            }))
        .to_std()
        .unwrap_or_default();
        let reason = if self.closed_at.is_some_and(|t| t.elapsed() >= CLOSE_GRACE) {
            Some("interrupted")
        } else if self.closed_at.is_none() && self.holder_gone && quiet >= HOLDER_GRACE {
            Some("holder_gone")
        } else if self.closed_at.is_none() && quiet >= self.idle_timeout {
            Some("idle")
        } else {
            None
        };
        if let Some(reason) = reason {
            self.terminal = Some(self.run_closed(reason));
        }
        Some(())
    }

    /// The synthetic terminal event of a run that will not send `run_finished`.
    fn run_closed(&self, reason: &str) -> EngineRunEvent {
        let record = self.record.as_ref();
        let success = record.and_then(|r| match r.status.as_str() {
            "success" => Some(true),
            "failed" => Some(false),
            _ => None,
        });
        let message = record
            .and_then(|r| r.extra.get("message"))
            .and_then(|m| m.as_str())
            .map_or(Json::Null, |m| Json::String(m.to_string()));
        EngineRunEvent::from_row(
            &self.run_id,
            self.last.unwrap_or(0),
            Utc::now(),
            None,
            RUN_CLOSED,
            serde_json::json!({
                "reason": reason,
                "finished": self.closed_at.is_some(),
                "success": success,
                "message": message,
            }),
        )
    }
}

/// The name of the synthetic terminal event.
pub const RUN_CLOSED: &str = "run_closed";

/// Whether a run holder names a process of this host that is gone (`mrsm:<host>:<pid>` with
/// this host's name). Any other holder (a job name, another host, a release candidate's
/// `mrsm:<pid>` whose host is unknown), or a platform without the check, is presumed alive.
fn holder_gone(holder: &str) -> bool {
    let meerschaum_core::holder::HolderProcess::Local(pid) =
        meerschaum_core::holder::holder_process(holder)
    else {
        return false;
    };
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    if pid <= 0 || pid as u32 == std::process::id() {
        return false;
    }
    #[cfg(unix)]
    {
        match rustix::process::Pid::from_raw(pid) {
            Some(pid) => matches!(
                rustix::process::test_kill_process(pid),
                Err(rustix::io::Errno::SRCH)
            ),
            None => false,
        }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(seq: i64, event: &str) -> Doc {
        [
            ("ts", KValue::Str("2026-01-01T00:00:00+00:00".into())),
            ("run_id", KValue::Str("r1".into())),
            ("seq", KValue::Int(seq)),
            ("pipe_ref", KValue::Str("sql:main|sql:main|m|None".into())),
            ("event", KValue::Str(event.into())),
            (
                "payload",
                KValue::Json(serde_json::json!({"event": event, "attempt": 1})),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }

    #[test]
    fn rows_become_typed_events() {
        let e = event_from_doc(&doc(3, "node_started")).unwrap();
        assert_eq!(e.seq, 3);
        assert_eq!(e.event, "node_started");
        let json = serde_json::to_value(&e).unwrap();
        assert_eq!(json["attempt"], 1);
        assert_eq!(json["event"], "node_started");
        assert_eq!(json["pipe_ref"], "sql:main|sql:main|m|None");
        assert!(
            json["ts"]
                .as_str()
                .unwrap()
                .starts_with("2026-01-01T00:00:00")
        );
    }

    #[test]
    fn run_ids_are_validated() {
        assert!(valid_run_id("canvas-0123abcd"));
        assert!(valid_run_id("20261025_120000.123"));
        assert!(!valid_run_id(""));
        assert!(!valid_run_id("_negated"));
        assert!(!valid_run_id("a/b"));
        assert!(!valid_run_id(&"x".repeat(201)));
    }

    #[test]
    fn slots_are_bounded_and_freed() {
        let streams = EngineStreams::default();
        let a = streams.take_slot("user:a", 2, 1).unwrap();
        assert!(streams.take_slot("user:a", 2, 1).is_err());
        let _b = streams.take_slot("user:b", 2, 1).unwrap();
        assert!(streams.take_slot("user:c", 2, 1).is_err());
        drop(a);
        assert_eq!(streams.open_streams(), 1);
        assert!(streams.take_slot("user:a", 2, 1).is_ok());
    }
}
