//! Shared console-backed Server-Sent Events (SSE) stream policy and guard.
//!
//! Both `GET /engine/runs/{run_id}/events/stream` ([`crate::engine_stream`]) and
//! `GET /logs/{name}/stream` (`meerschaum-web`) follow the same browser/bearer lifecycle:
//!
//! - **Authentication ([`BrowserAuth`], [`StreamAuth`])**: a request with an `Authorization`
//!   header uses that credential; otherwise a console session cookie is validated through the
//!   installed [`BrowserAuth`] implementation, which verifies the session and its user's
//!   credential binding and mints a fresh short-lived bearer token.
//! - **Periodic reauthorization ([`StreamGuard::refresh_headers`], [`StreamGuard::reauthorize`])**:
//!   each refresh tick re-validates the console session via [`BrowserAuth::authorize`] and mints a
//!   fresh short-lived bearer, so a long-lived console stream outlives individual bearer lifetimes
//!   while still ending promptly on logout, credential rotation, or permission revocation.
//! - **Concurrency limits ([`StreamSlots`], [`Slot`])**: open streams are bounded globally and
//!   per user/token/session (`api:permissions:log_streams`), returning `429 Too Many Requests`
//!   with `Retry-After: 5` when exhausted and releasing the slot automatically on drop.
//! - **Graceful shutdown and proxy-safe headers ([`response`])**: streams terminate immediately
//!   when the server begins draining ([`crate::lifecycle::until_drain`]) and always send
//!   `x-accel-buffering: no` alongside `Cache-Control`.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};

use crate::access::Access;
use crate::auth::Principal;
use crate::lifecycle::DrainSignal;
use crate::state::AppState;

/// `Cache-Control: no-store` (for sensitive log streams).
pub const CACHE_NO_STORE: &str = "no-store";
/// `Cache-Control: no-cache` (for replayable event streams).
pub const CACHE_NO_CACHE: &str = "no-cache";
/// Default `Retry-After` header value on `429 Too Many Requests`.
pub const RETRY_AFTER_SECONDS: &str = "5";

/// Authenticates a browser request that carries no `Authorization` header (a browser's
/// `EventSource` cannot set one) by its console session. `meerschaum-web` installs one with
/// [`crate::engine_stream::EngineStreams::set_browser_auth`].
#[async_trait]
pub trait BrowserAuth: Send + Sync {
    /// `None` when the request has no console session; otherwise the headers to authorize it
    /// with (a fresh short-lived bearer for the session's user) and the session ID, or the
    /// status to answer (`401`, `503`).
    ///
    /// An open stream calls this again (with the request's cookies) at each refresh tick, so
    /// implementations must verify session existence, user existence, and credential binding
    /// every time and mint a fresh bearer.
    async fn authorize(
        &self,
        state: &AppState,
        headers: &HeaderMap,
    ) -> Option<Result<(HeaderMap, String), StatusCode>>;

    /// On a `--no-auth` server: who an unauthenticated request counts against for the
    /// per-user stream limit (the console cookie). `None`: only the server-wide limit applies.
    fn anonymous_key(&self, _headers: &HeaderMap) -> Option<String> {
        None
    }
}

/// Server-wide and per-caller concurrency tracker for open SSE streams.
#[derive(Debug, Clone)]
pub struct StreamSlots {
    open: Arc<Mutex<(usize, HashMap<String, usize>)>>,
    label: &'static str,
}

impl Default for StreamSlots {
    fn default() -> Self {
        Self::new("streams")
    }
}

impl StreamSlots {
    /// Create a slot tracker whose `429` error messages name `label` (e.g. `"event streams"`
    /// or `"log streams"`).
    pub fn new(label: &'static str) -> Self {
        Self {
            open: Arc::default(),
            label,
        }
    }

    /// Take a slot for `key`, or return the reason the server-wide or per-user limit was reached.
    pub fn take(&self, key: &str, max: usize, per_user: usize) -> Result<Slot, String> {
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        if open.0 >= max {
            return Err(format!("Too many {} are open on this server.", self.label));
        }
        if open.1.get(key).copied().unwrap_or(0) >= per_user {
            return Err(format!("Too many {} are open for this user.", self.label));
        }
        open.0 += 1;
        *open.1.entry(key.to_string()).or_insert(0) += 1;
        Ok(Slot {
            open: self.open.clone(),
            key: key.to_string(),
        })
    }

    /// The number of currently open streams.
    pub fn open(&self) -> usize {
        self.open.lock().unwrap_or_else(|e| e.into_inner()).0
    }
}

/// RAII handle for one open stream; releases the slot automatically on drop.
#[derive(Debug)]
pub struct Slot {
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

/// Who a stream counts against for per-user stream limits.
pub fn slot_key(principal: Option<&Principal>, session_id: Option<&str>) -> String {
    match principal {
        Some(Principal::Execution(identity)) => identity.owner_key(),
        Some(Principal::User(user)) => format!("user:{}", user.username),
        Some(Principal::Token(token)) => {
            format!(
                "token:{}",
                token.id.map(|id| id.to_string()).unwrap_or_default()
            )
        }
        None => format!("session:{}", session_id.unwrap_or("")),
    }
}

/// Build a `429 Too Many Requests` response with `Retry-After: 5`.
pub fn too_many_response(reason: impl Into<String>) -> Response {
    let mut response = (StatusCode::TOO_MANY_REQUESTS, reason.into()).into_response();
    response.headers_mut().insert(
        header::RETRY_AFTER,
        HeaderValue::from_static(RETRY_AFTER_SECONDS),
    );
    response
}

/// Why an SSE stream could not be opened or acquire a concurrency slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamRejection {
    /// Console session authentication failed (`401`, `503`, ...).
    Status(StatusCode),
    /// Server-wide or per-user stream concurrency limit reached (`429`, `Retry-After: 5`).
    TooMany(String),
}

impl IntoResponse for StreamRejection {
    fn into_response(self) -> Response {
        match self {
            Self::Status(status) => status.into_response(),
            Self::TooMany(reason) => too_many_response(reason),
        }
    }
}

/// Extract only the `Cookie` headers of a request.
pub fn cookies_of(headers: &HeaderMap) -> HeaderMap {
    let mut cookies = HeaderMap::new();
    for value in headers.get_all(header::COOKIE) {
        cookies.append(header::COOKIE, value.clone());
    }
    cookies
}

struct ConsoleSession {
    auth: Arc<dyn BrowserAuth>,
    id: String,
    cookies: HeaderMap,
}

/// Initial bearer-or-console-session authentication state for an SSE request before a stream
/// slot is acquired (so 404/visibility checks and `?wait=` loops never hold a slot).
pub struct StreamAuth {
    auth_headers: HeaderMap,
    session: Option<ConsoleSession>,
    anonymous_key: Option<String>,
}

impl StreamAuth {
    /// Authenticate an incoming SSE request: an explicit `Authorization` header wins; otherwise
    /// `browser_auth` (when installed) validates the console session cookie and mints a
    /// short-lived bearer.
    pub async fn open(
        state: &AppState,
        headers: &HeaderMap,
        browser_auth: Option<Arc<dyn BrowserAuth>>,
    ) -> Result<Self, StreamRejection> {
        if !headers.contains_key(header::AUTHORIZATION)
            && let Some(auth) = browser_auth
        {
            match auth.authorize(state, headers).await {
                Some(Ok((minted, id))) => {
                    return Ok(Self {
                        auth_headers: minted,
                        session: Some(ConsoleSession {
                            auth,
                            id,
                            cookies: cookies_of(headers),
                        }),
                        anonymous_key: None,
                    });
                }
                Some(Err(status)) => return Err(StreamRejection::Status(status)),
                None => {
                    return Ok(Self {
                        auth_headers: headers.clone(),
                        session: None,
                        anonymous_key: auth.anonymous_key(headers),
                    });
                }
            }
        }
        Ok(Self {
            auth_headers: headers.clone(),
            session: None,
            anonymous_key: None,
        })
    }

    /// The current authorization headers (the caller's bearer, or the latest bearer minted from
    /// the console session).
    pub fn headers(&self) -> &HeaderMap {
        &self.auth_headers
    }

    /// Whether this request is backed by a console session cookie.
    pub fn has_session(&self) -> bool {
        self.session.is_some()
    }

    /// The console session ID, if backed by a console session.
    pub fn session_id(&self) -> Option<&str> {
        self.session.as_ref().map(|s| s.id.as_str())
    }

    /// Refresh the console session and re-mint its short-lived bearer (for bearer-backed
    /// streams, returns the existing headers unchanged).
    pub async fn refresh_headers(&mut self, state: &AppState) -> Result<&HeaderMap, StatusCode> {
        if let Some(session) = &self.session {
            match session.auth.authorize(state, &session.cookies).await {
                Some(Ok((headers, id))) if id == session.id => {
                    self.auth_headers = headers;
                }
                Some(Err(status)) => return Err(status),
                _ => return Err(StatusCode::UNAUTHORIZED),
            }
        }
        Ok(&self.auth_headers)
    }

    /// Acquire a bounded stream slot and promote this authentication state into a live
    /// [`StreamGuard`]. Returns [`StreamRejection::TooMany`] (`429 Too Many Requests` with
    /// `Retry-After: 5`) when exhausted.
    pub fn into_guard(
        self,
        slots: &StreamSlots,
        max: usize,
        per_user: usize,
        principal: Option<&Principal>,
    ) -> Result<StreamGuard, StreamRejection> {
        let (key, limit) = match (principal, &self.session) {
            (None, None) => match &self.anonymous_key {
                Some(key) => (key.clone(), per_user),
                None => ("anonymous".to_string(), usize::MAX),
            },
            _ => (
                slot_key(principal, self.session.as_ref().map(|s| s.id.as_str())),
                per_user,
            ),
        };
        let slot = slots
            .take(&key, max, limit)
            .map_err(StreamRejection::TooMany)?;
        Ok(StreamGuard {
            auth: self,
            _slot: slot,
        })
    }
}

/// Live guard held by an open SSE stream: owns the RAII [`Slot`] and refreshes console-session
/// bearers + server authorization on every refresh tick.
pub struct StreamGuard {
    auth: StreamAuth,
    _slot: Slot,
}

impl StreamGuard {
    /// The current authorization headers.
    pub fn headers(&self) -> &HeaderMap {
        self.auth.headers()
    }

    /// Whether this stream is backed by a console session cookie.
    pub fn has_session(&self) -> bool {
        self.auth.has_session()
    }

    /// Re-validate a console session and mint a fresh short-lived bearer. For bearer-backed
    /// streams, returns the original headers unchanged.
    pub async fn refresh_headers(&mut self, state: &AppState) -> Result<&HeaderMap, StatusCode> {
        self.auth.refresh_headers(state).await
    }

    /// Re-mint a console session's short-lived bearer (if session-backed) and re-verify fresh
    /// access with `scopes`. Returns `None` when the session ended, the user's credential rotated,
    /// or permissions were revoked.
    pub async fn reauthorize(&mut self, state: &AppState, scopes: &[&str]) -> Option<Access> {
        let headers = self.auth.refresh_headers(state).await.ok()?;
        crate::access::require_fresh(state, headers, scopes)
            .await
            .ok()
    }
}

/// Wrap an SSE event stream with graceful-drain termination and proxy-safe headers
/// (`x-accel-buffering: no`, `Cache-Control`).
pub fn response<S>(
    stream: S,
    drain: DrainSignal,
    keep_alive: KeepAlive,
    cache_control: &'static str,
) -> Response
where
    S: futures_util::Stream<Item = Result<SseEvent, Infallible>> + Send + 'static,
{
    let stream = crate::lifecycle::until_drain(stream, drain);
    let mut response = Sse::new(stream).keep_alive(keep_alive).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(cache_control),
    );
    // Reverse proxies (e.g. nginx) buffer SSE responses unless told not to.
    headers.insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use futures_util::StreamExt;
    use meerschaum_core::Map;
    use meerschaum_core::users::User;
    use serde_json::json;

    const SECRET: &[u8] = b"sse-guard-test-secret-key-0123456789";

    struct RotatingBrowserAuth {
        active: AtomicBool,
        minted: AtomicUsize,
        lifetime_secs: i64,
        /// Optional override of the credential bound into the session; when `None`, uses the
        /// user's current credential from the instance.
        pinned_credential: Mutex<Option<String>>,
    }

    impl RotatingBrowserAuth {
        fn new(lifetime_secs: i64) -> Self {
            Self {
                active: AtomicBool::new(true),
                minted: AtomicUsize::new(0),
                lifetime_secs,
                pinned_credential: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl BrowserAuth for RotatingBrowserAuth {
        async fn authorize(
            &self,
            state: &AppState,
            headers: &HeaderMap,
        ) -> Option<Result<(HeaderMap, String), StatusCode>> {
            let cookie = headers.get(header::COOKIE)?.to_str().ok()?;
            if cookie != "mrsm-session-id=sess-1" || !self.active.load(Ordering::SeqCst) {
                return Some(Err(StatusCode::UNAUTHORIZED));
            }
            let pinned = self.pinned_credential.lock().unwrap().clone();
            let credential = match pinned {
                Some(pinned) => pinned,
                None => crate::auth::user_credential(state, &User::new("alice", None))
                    .await
                    .ok()
                    .flatten()?,
            };
            // Verify credential binding against the user's current credential, just like
            // `meerschaum-web`'s `ConsoleSessionAuth`.
            let bound = crate::auth::load_from_bound_subject(
                state,
                "alice",
                Some("user"),
                Some(&credential),
            )
            .await
            .ok()?;
            if bound.is_none() {
                return Some(Err(StatusCode::UNAUTHORIZED));
            }
            self.minted.fetch_add(1, Ordering::SeqCst);
            let claims: Map = serde_json::from_value(json!({
                "sub": "alice",
                "scopes": ["jobs:read", "logs:read"],
                "exp": chrono::Utc::now().timestamp() + self.lifetime_secs,
                (crate::auth::PRINCIPAL_CLAIM): "user",
                (crate::auth::CREDENTIAL_CLAIM): credential,
            }))
            .unwrap();
            let mut h = HeaderMap::new();
            h.insert(
                header::AUTHORIZATION,
                format!("Bearer {}", crate::auth::jwt_encode(&claims, SECRET))
                    .parse()
                    .unwrap(),
            );
            Some(Ok((h, "sess-1".into())))
        }

        fn anonymous_key(&self, headers: &HeaderMap) -> Option<String> {
            headers
                .get(header::COOKIE)
                .and_then(|v| v.to_str().ok())
                .map(|c| format!("session:{c}"))
        }
    }

    async fn test_state(no_auth: bool) -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = crate::ServerConfig::for_root("sql:sse_test", dir.path());
        cfg.no_auth = no_auth;
        let instance = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "sse_test",
                &format!("sqlite:///{}", dir.path().join("test.db").display()),
            )
            .await
            .unwrap(),
        );
        let mut user = User::new("alice", Some("pass-1"));
        user.user_type = Some("admin".into());
        let reg = instance.register_user(&mut user).await;
        assert!(reg.success, "{reg}");
        let state = AppState::builder(cfg, instance)
            .secret(SECRET.to_vec())
            .build()
            .unwrap();
        (dir, state)
    }

    #[test]
    fn slot_exhaustion_returns_429_with_retry_after_5_and_frees_on_drop() {
        let slots = StreamSlots::new("event streams");
        let a1 = slots.take("user:a", 2, 1).unwrap();
        let err = slots.take("user:a", 2, 1).unwrap_err();
        assert!(err.contains("user"), "{err}");
        let resp = too_many_response(err);
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers()[header::RETRY_AFTER], "5");

        let _b1 = slots.take("user:b", 2, 1).unwrap();
        let err = slots.take("user:c", 2, 1).unwrap_err();
        assert!(err.contains("server"), "{err}");
        assert_eq!(slots.open(), 2);

        drop(a1);
        assert_eq!(slots.open(), 1);
        assert!(slots.take("user:a", 2, 1).is_ok());
    }

    #[tokio::test]
    async fn bearer_refresh_prevents_expiry_and_logout_or_credential_rotation_ends_stream() {
        let (_dir, state) = test_state(false).await;
        let auth = Arc::new(RotatingBrowserAuth::new(1));
        let mut req_headers = HeaderMap::new();
        req_headers.insert(
            header::COOKIE,
            HeaderValue::from_static("mrsm-session-id=sess-1"),
        );

        let stream_auth = StreamAuth::open(&state, &req_headers, Some(auth.clone()))
            .await
            .unwrap();
        let initial_access = crate::access::require_fresh(
            &state,
            stream_auth.headers(),
            &["jobs:read", "logs:read"],
        )
        .await
        .unwrap();
        let slots = StreamSlots::new("log streams");
        let mut guard = stream_auth
            .into_guard(&slots, 10, 5, initial_access.auth.as_ref())
            .unwrap();
        assert_eq!(slots.open(), 1);
        assert_eq!(auth.minted.load(Ordering::SeqCst), 1);

        // Wait past the initial 1s bearer's expiration: reauthorize re-mints a fresh bearer.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(
            guard
                .reauthorize(&state, &["jobs:read", "logs:read"])
                .await
                .is_some(),
            "bearer refresh must keep a live console session open past bearer expiry"
        );
        assert_eq!(auth.minted.load(Ordering::SeqCst), 2);

        // Credential rotation (e.g. password change): pinned old credential no longer binds.
        *auth.pinned_credential.lock().unwrap() = Some("stale-credential-hash".into());
        assert!(
            guard
                .reauthorize(&state, &["jobs:read", "logs:read"])
                .await
                .is_none(),
            "credential rotation must terminate the stream"
        );

        // Restore valid credential, then test logout (session deleted/inactive).
        *auth.pinned_credential.lock().unwrap() = None;
        assert!(
            guard
                .reauthorize(&state, &["jobs:read", "logs:read"])
                .await
                .is_some()
        );
        auth.active.store(false, Ordering::SeqCst);
        assert!(
            guard
                .reauthorize(&state, &["jobs:read", "logs:read"])
                .await
                .is_none(),
            "logout must terminate the stream"
        );

        drop(guard);
        assert_eq!(slots.open(), 0);
    }

    #[tokio::test]
    async fn sse_response_sets_proxy_safe_headers_and_closes_cleanly_on_drain() {
        let lifecycle = crate::lifecycle::Lifecycle::default();
        let drain = lifecycle.signal();
        let stream = futures_util::stream::pending::<Result<SseEvent, Infallible>>();
        let resp = response(stream, drain, KeepAlive::default(), CACHE_NO_STORE);

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(resp.headers()["x-accel-buffering"], "no");

        let mut body = resp.into_body().into_data_stream();
        lifecycle.begin_drain();
        let next = tokio::time::timeout(Duration::from_secs(2), body.next())
            .await
            .expect("draining SSE stream must close promptly");
        assert!(next.is_none(), "draining stream should yield EOF");
    }
}
