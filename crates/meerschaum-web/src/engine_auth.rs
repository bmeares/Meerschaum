//! Console-session authentication for `GET /engine/runs/{run_id}/events/stream`
//! (`meerschaum_server::engine_stream`): the Dash Pipelines page follows a live run with an
//! `EventSource`, which sends the console's session cookie but cannot set `Authorization`. The
//! session's user is authorized with a short-lived bearer, exactly as for the job-log stream.
use super::*;
use meerschaum_server::engine_stream::BrowserAuth;

/// [`BrowserAuth`] over the console's session store.
pub(crate) struct ConsoleSessionAuth {
    pub config: WebConfig,
}

impl ConsoleSessionAuth {
    fn web_state(&self, state: &AppState) -> WebState {
        WebState {
            server: state.clone(),
            config: self.config.clone(),
            streams: Arc::default(),
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
        let web = self.web_state(state);
        let data = match session(&web, headers).await {
            Ok(data) => data,
            Err(status) => return Some(Err(status)),
        };
        Some(api_headers(&web, &data).await.map(|h| (h, id)))
    }

    async fn session_active(&self, _state: &AppState, session_id: &str) -> bool {
        matches!(self.config.sessions.get(session_id).await, Ok(Some(_)))
    }

    /// A `--no-auth` server counts a browser's streams by its console cookie, like the job-log
    /// stream does.
    fn anonymous_key(&self, headers: &HeaderMap) -> Option<String> {
        cookie(headers).map(|id| format!("session:{id}"))
    }
}
