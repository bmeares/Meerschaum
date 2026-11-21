//! Console-session authentication for console-backed SSE streams
//! (`GET /engine/runs/{run_id}/events/stream` and `GET /logs/{name}/stream`): the browser's
//! `EventSource` sends the console's session cookie but cannot set `Authorization`. Each
//! authorization (initial open and every periodic refresh tick) verifies the session store entry
//! and its user's credential binding ([`bound_user_principal`]) before minting a fresh
//! short-lived bearer.

use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use meerschaum_server::AppState;
use meerschaum_server::sse::BrowserAuth;

use crate::sessions::SessionStore;
use crate::{CONSOLE_BEARER_SECONDS, api_headers_for, cookie, session_from};

/// [`BrowserAuth`] over the console's session store.
pub(crate) struct ConsoleSessionAuth {
    sessions: Arc<dyn SessionStore>,
    /// How long each minted bearer lives, in seconds.
    bearer_lifetime: i64,
}

impl ConsoleSessionAuth {
    pub(crate) fn new(sessions: Arc<dyn SessionStore>) -> Self {
        Self::with_bearer_lifetime(sessions, CONSOLE_BEARER_SECONDS)
    }

    pub(crate) fn with_bearer_lifetime(sessions: Arc<dyn SessionStore>, seconds: i64) -> Self {
        Self {
            sessions,
            bearer_lifetime: seconds,
        }
    }
}

#[async_trait::async_trait]
impl BrowserAuth for ConsoleSessionAuth {
    async fn authorize(
        &self,
        state: &AppState,
        headers: &HeaderMap,
    ) -> Option<Result<(HeaderMap, String), StatusCode>> {
        // Without authentication every request is authorized as is.
        if state.config.no_auth {
            return None;
        }
        let id = cookie(headers)?;
        let data = match session_from(self.sessions.as_ref(), headers).await {
            Ok(data) => data,
            Err(status) => return Some(Err(status)),
        };
        Some(
            api_headers_for(state, &data, self.bearer_lifetime)
                .await
                .map(|h| (h, id)),
        )
    }

    /// A `--no-auth` server counts a browser's streams by its console cookie.
    fn anonymous_key(&self, headers: &HeaderMap) -> Option<String> {
        cookie(headers).map(|id| format!("session:{id}"))
    }
}
