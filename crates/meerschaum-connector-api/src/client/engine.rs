//! The `/engine/*` route family (§5.8, new in v5): engine state for API instances. Every route
//! requires `pipes:write`. Write routes answer with a `SuccessTuple`; the lease routes with a
//! [`LeaseResponse`].

use meerschaum_core::SuccessTuple;
use meerschaum_kernel::Frame;
use serde::Serialize;

use crate::engine_pipes::{self, EnginePipeQuery};
use crate::models::{
    EdgeCursor, EngineEvent, EngineRunEvent, EngineState, LeaseRequest, LeaseResponse, LedgerEntry,
    RunRecord,
};
use crate::routes;
use crate::traits::{ConnectorError, ConnectorResult};
use crate::wire::QueryValue;

use super::{APIConnector, Body, Method, RequestOptions};

impl APIConnector {
    async fn engine_write<T: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: &T,
    ) -> SuccessTuple {
        let body = match serde_json::to_string(body) {
            Ok(b) => b,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match self
            .make_request(
                method,
                path,
                RequestOptions {
                    body: Body::Json(body),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => crate::models::parse_success_tuple(r.ok(), &r.text()),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `POST /engine/ledger`: append dirty-range ledger entries.
    pub async fn post_engine_ledger(&self, entries: &[LedgerEntry]) -> SuccessTuple {
        self.engine_write(Method::Post, routes::ENGINE_LEDGER, entries)
            .await
    }

    /// `GET /engine/ledger?child_ref=&after_seq=`: the parents' entries for a child after a
    /// sequence number.
    pub async fn get_engine_ledger(
        &self,
        child_ref: &str,
        after_seq: Option<i64>,
    ) -> ConnectorResult<Vec<LedgerEntry>> {
        let opts = RequestOptions::query(vec![
            ("child_ref", child_ref.into()),
            (
                "after_seq",
                after_seq.map(QueryValue::from).unwrap_or(QueryValue::None),
            ),
        ]);
        let j = self
            .get(routes::ENGINE_LEDGER, opts)
            .await?
            .json_or_detail()?;
        Ok(serde_json::from_value(j)?)
    }

    /// `PUT /engine/cursors`: upsert edge cursors.
    pub async fn put_engine_cursors(&self, cursors: &[EdgeCursor]) -> SuccessTuple {
        self.engine_write(Method::Put, routes::ENGINE_CURSORS, cursors)
            .await
    }

    /// `GET /engine/state?pipe_ref=`: a pipe's persisted engine state.
    pub async fn get_engine_state(&self, pipe_ref: &str) -> ConnectorResult<Option<EngineState>> {
        let r = self
            .get(
                routes::ENGINE_STATE,
                RequestOptions::query(vec![("pipe_ref", pipe_ref.into())]),
            )
            .await?;
        if r.status == 404 {
            return Ok(None);
        }
        let j = r.json_or_detail()?;
        if j.is_null() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_value(j)?))
    }

    /// `PUT /engine/state`: upsert pipes' engine state.
    pub async fn put_engine_state(&self, states: &[EngineState]) -> SuccessTuple {
        self.engine_write(Method::Put, routes::ENGINE_STATE, states)
            .await
    }

    /// `POST /engine/runs`: record runs and per-node results.
    pub async fn post_engine_runs(&self, records: &[RunRecord]) -> SuccessTuple {
        self.engine_write(Method::Post, routes::ENGINE_RUNS, records)
            .await
    }

    /// `GET /engine/runs?run_id=&limit=`: run history.
    pub async fn get_engine_runs(
        &self,
        run_id: Option<&str>,
        limit: Option<i64>,
    ) -> ConnectorResult<Vec<RunRecord>> {
        let opts = RequestOptions::query(vec![
            ("run_id", QueryValue::opt(run_id.map(String::from))),
            (
                "limit",
                limit.map(QueryValue::from).unwrap_or(QueryValue::None),
            ),
        ]);
        let j = self
            .get(routes::ENGINE_RUNS, opts)
            .await?
            .json_or_detail()?;
        Ok(serde_json::from_value(j)?)
    }

    /// `POST /engine/events`: append a batch of run events.
    pub async fn post_engine_events(&self, events: &[EngineEvent]) -> SuccessTuple {
        self.engine_write(Method::Post, routes::ENGINE_EVENTS, events)
            .await
    }

    async fn lease_request(&self, method: Method, req: &LeaseRequest) -> ConnectorResult<bool> {
        let parts: Vec<_> = req.pipe_ref.splitn(4, '|').collect();
        let pipeline_metadata = parts.len() == 4
            && parts[1] == "mrsm"
            && ["pipeline", "pipeline_registry"].contains(&parts[2]);
        let route = if pipeline_metadata {
            "/pipelines/leases"
        } else {
            routes::ENGINE_LEASES
        };
        let r = self
            .make_request(
                method,
                route,
                RequestOptions {
                    body: Body::Json(serde_json::to_string(req)?),
                    query: if pipeline_metadata {
                        vec![(
                            "instance_keys".into(),
                            QueryValue::opt(self.instance_keys()),
                        )]
                    } else {
                        vec![]
                    },
                    ..Default::default()
                },
            )
            .await?;
        if !r.ok() {
            return Err(ConnectorError::Http {
                status: r.status,
                body: r.text(),
            });
        }
        let resp: LeaseResponse = serde_json::from_value(r.json()?)?;
        Ok(resp.success)
    }

    /// `POST /engine/leases`: acquire (or extend) a pipe's lease.
    pub async fn acquire_engine_lease(&self, req: &LeaseRequest) -> ConnectorResult<bool> {
        self.lease_request(Method::Post, req).await
    }

    /// `DELETE /engine/leases`: release a pipe's lease.
    pub async fn release_engine_lease(&self, req: &LeaseRequest) -> ConnectorResult<bool> {
        self.lease_request(Method::Delete, req).await
    }

    async fn engine_pipe_request(
        &self,
        method: Method,
        metric_key: &str,
        op: &str,
        body: Option<String>,
    ) -> ConnectorResult<serde_json::Value> {
        let r = self
            .make_request(
                method,
                &routes::engine_pipe_path(metric_key, op),
                RequestOptions {
                    body: body.map(Body::Json).unwrap_or_default(),
                    ..Default::default()
                },
            )
            .await?;
        if r.status == 501 {
            return Err(ConnectorError::NotImplemented(r.text()));
        }
        if !r.ok() {
            return Err(ConnectorError::Http {
                status: r.status,
                body: r.text(),
            });
        }
        r.json()
    }

    /// `POST /engine/pipes/{metric}/read`: rows of an engine state pipe, kept on the server's
    /// instance, as a frame with the pipe's static dtypes (`None` when there are none).
    pub async fn read_engine_pipe(
        &self,
        metric_key: &str,
        query: &EnginePipeQuery,
    ) -> ConnectorResult<Option<Frame>> {
        let j = self
            .engine_pipe_request(
                Method::Post,
                metric_key,
                "read",
                Some(serde_json::to_string(query)?),
            )
            .await?;
        let Some(items) = j.as_array() else {
            return Err(ConnectorError::Invalid(format!(
                "Unexpected engine state response: {j}"
            )));
        };
        if items.is_empty() {
            return Ok(None);
        }
        engine_pipes::engine_frame(metric_key, engine_pipes::docs_from_records(items))
            .map(Some)
            .map_err(ConnectorError::Invalid)
    }

    /// `POST /engine/pipes/{metric}/write`: append (or, for the upsert pipes, upsert) rows.
    pub async fn write_engine_pipe(&self, metric_key: &str, df: &Frame) -> SuccessTuple {
        let body = engine_pipes::records_json(df);
        match self
            .engine_pipe_request(Method::Post, metric_key, "write", Some(body))
            .await
        {
            Ok(j) => crate::models::parse_success_tuple(true, &j.to_string()),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `POST /engine/pipes/{metric}/clear`: delete the rows matching `params` (and `end`).
    pub async fn clear_engine_pipe(
        &self,
        metric_key: &str,
        query: &EnginePipeQuery,
    ) -> SuccessTuple {
        let body = match serde_json::to_string(query) {
            Ok(b) => b,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match self
            .engine_pipe_request(Method::Post, metric_key, "clear", Some(body))
            .await
        {
            Ok(j) => crate::models::parse_success_tuple(true, &j.to_string()),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `GET /engine/pipes/{metric}/exists`: whether the state pipe's table exists.
    pub async fn engine_pipe_exists(&self, metric_key: &str) -> ConnectorResult<bool> {
        let j = self
            .engine_pipe_request(Method::Get, metric_key, "exists", None)
            .await?;
        Ok(j.as_bool().unwrap_or(false))
    }
}

/// The events of [`APIConnector::follow_engine_run`], oldest first; the stream ends after a
/// terminal event (`run_finished`, or the server's `run_closed`; see
/// [`EngineRunEvent::is_terminal`]) or with the error that stopped it.
pub type EngineRunEventStream =
    futures_util::stream::BoxStream<'static, ConnectorResult<EngineRunEvent>>;

/// How many times in a row [`APIConnector::follow_engine_run`] tries to resume a dropped
/// stream (with a growing pause) before it gives up.
const FOLLOW_RECONNECTS: u32 = 5;

/// How long a followed stream may send nothing (the server sends a comment every 15 s) before
/// it is treated as dropped and resumed.
const FOLLOW_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);

/// Options of [`APIConnector::follow_engine_run_with`].
#[derive(Debug, Clone, Default)]
pub struct FollowRunOptions {
    /// Start after this `seq` (`None`: from the run's first event).
    pub after_seq: Option<i64>,
    /// The instance that keeps the run's history (default: the server's).
    pub instance_keys: Option<String>,
    /// Wait this long (the server allows up to 60 s) for a run that has not started yet,
    /// instead of failing with `404` (`?wait=`).
    pub wait: Option<std::time::Duration>,
    /// End with `run_closed` (`reason: idle`) after this long without an event (`?idle_timeout=`;
    /// the server's default is 15 minutes).
    pub idle_timeout: Option<std::time::Duration>,
}

type ByteStream = futures_util::stream::BoxStream<'static, reqwest::Result<Vec<u8>>>;

/// The state of one [`APIConnector::follow_engine_run`] stream.
struct Follow {
    conn: APIConnector,
    run_id: String,
    options: FollowRunOptions,
    /// The last delivered `seq` (sent as `Last-Event-ID` on a reconnect).
    last: Option<i64>,
    body: Option<ByteStream>,
    buf: Vec<u8>,
    frame: SseFrame,
    failures: u32,
    done: bool,
}

/// The fields of the SSE event being read.
#[derive(Default)]
struct SseFrame {
    event: Option<String>,
    data: Vec<String>,
}

/// Whether a failed (re)connect is final: the run or the route is not there, or the client
/// may not read it. Anything else (a refused connection, `429`, a `5xx`) is retried.
fn connect_error_is_final(error: &ConnectorError) -> bool {
    matches!(error, ConnectorError::Http { status, .. } if matches!(status, 400..=428 | 430..=499))
}

impl Follow {
    async fn connect(&mut self) -> ConnectorResult<()> {
        let mut query = Vec::new();
        if let Some(keys) = &self.options.instance_keys {
            query.push((
                "instance_keys".to_string(),
                QueryValue::opt(Some(keys.clone())),
            ));
        }
        for (key, value) in [
            ("wait", self.options.wait),
            ("idle_timeout", self.options.idle_timeout),
        ] {
            if let Some(value) = value {
                query.push((
                    key.to_string(),
                    QueryValue::opt(Some(value.as_secs_f64().to_string())),
                ));
            }
        }
        let url = self
            .conn
            .request_url(&routes::engine_run_events_stream_path(&self.run_id), &query);
        let mut req = self
            .conn
            .http
            .get(&url)
            .header("Accept", "text/event-stream");
        if let Some(seq) = self.last {
            req = req.header("Last-Event-ID", seq.to_string());
        }
        // Boxed: `token()` may log in, which posts through `make_request`.
        let token = Box::pin(self.conn.token())
            .await
            .unwrap_or_else(|| "None".into());
        req = req.header("Authorization", format!("Bearer {token}"));
        let response = req
            .send()
            .await
            .map_err(|e| ConnectorError::Backend(e.to_string()))?;
        let status = response.status().as_u16();
        if status != 200 {
            let body = response.text().await.unwrap_or_default();
            return Err(ConnectorError::Http { status, body });
        }
        use futures_util::StreamExt;
        self.body = Some(
            response
                .bytes_stream()
                .map(|chunk| chunk.map(|b| b.to_vec()))
                .boxed(),
        );
        self.buf.clear();
        self.frame = SseFrame::default();
        Ok(())
    }

    /// Feed one line of the body; an event when the line ends one.
    fn line(&mut self, line: &str) -> Option<ConnectorResult<EngineRunEvent>> {
        if line.is_empty() {
            let frame = std::mem::take(&mut self.frame);
            if frame.data.is_empty() {
                return None;
            }
            let data = frame.data.join("\n");
            return Some(
                serde_json::from_str::<EngineRunEvent>(&data).map_err(ConnectorError::from),
            );
        }
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => self.frame.event = Some(value.to_string()),
            "data" => self.frame.data.push(value.to_string()),
            _ => {}
        }
        None
    }

    /// The next event, resuming a dropped stream.
    async fn next_event(&mut self) -> Option<ConnectorResult<EngineRunEvent>> {
        use futures_util::StreamExt;
        loop {
            if self.done {
                return None;
            }
            // A complete line already buffered.
            if let Some(pos) = self.buf.iter().position(|b| *b == b'\n') {
                let raw: Vec<u8> = self.buf.drain(..=pos).collect();
                let text = String::from_utf8_lossy(&raw);
                let text = text.trim_end_matches(['\n', '\r']).to_string();
                match self.line(&text) {
                    // `run_closed` repeats the last `seq`; it ends the stream.
                    Some(Ok(event)) if event.is_run_closed() => {
                        self.done = true;
                        return Some(Ok(event));
                    }
                    Some(Ok(event)) => {
                        if self.last.is_some_and(|last| event.seq <= last) {
                            continue;
                        }
                        self.failures = 0;
                        self.last = Some(event.seq);
                        self.done = event.is_terminal();
                        return Some(Ok(event));
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => continue,
                }
            }
            let chunk = match self.body.as_mut() {
                // Nothing at all (not even a keep-alive) for too long: a dead connection.
                Some(body) => tokio::time::timeout(FOLLOW_READ_TIMEOUT, body.next())
                    .await
                    .unwrap_or(None),
                None => None,
            };
            match chunk {
                Some(Ok(bytes)) => self.buf.extend_from_slice(&bytes),
                // The connection dropped (or the server ended the stream early): resume after
                // the last delivered event, pausing longer after each failure in a row.
                Some(Err(_)) | None => {
                    self.body = None;
                    loop {
                        if self.failures >= FOLLOW_RECONNECTS {
                            self.done = true;
                            return Some(Err(ConnectorError::Backend(format!(
                                "The event stream of run '{}' ended before `run_finished`.",
                                self.run_id
                            ))));
                        }
                        if self.failures > 0 {
                            let pause = 1u64 << (self.failures - 1).min(4);
                            tokio::time::sleep(std::time::Duration::from_secs(pause)).await;
                        }
                        self.failures += 1;
                        match self.connect().await {
                            Ok(()) => break,
                            Err(e) if connect_error_is_final(&e) => {
                                self.done = true;
                                return Some(Err(e));
                            }
                            Err(_) => continue,
                        }
                    }
                }
            }
        }
    }
}

impl APIConnector {
    /// `GET /engine/runs/{run_id}/events/stream`: follow a run's typed events live (a run of
    /// an engine whose instance is this server, of the server's own actions, or of a job on
    /// the server's machine), oldest first, from after `after_seq` (`None`: from the start).
    /// The stream ends after `run_finished`, or after the server's `run_closed` when the run
    /// will not finish (see [`EngineRunEvent::is_terminal`]). A dropped connection is resumed
    /// with `Last-Event-ID` (up to 5 failures in a row). `instance_keys` names the instance
    /// that keeps the run's history (default: the server's).
    ///
    /// The route requires `pipes:write`; a server without it answers `404`, which is an
    /// [`ConnectorError::Http`] here, as is a run the server does not know (see
    /// [`follow_engine_run_with`](Self::follow_engine_run_with) to wait for one).
    pub async fn follow_engine_run(
        &self,
        run_id: &str,
        after_seq: Option<i64>,
        instance_keys: Option<&str>,
    ) -> ConnectorResult<EngineRunEventStream> {
        self.follow_engine_run_with(
            run_id,
            FollowRunOptions {
                after_seq,
                instance_keys: instance_keys.map(String::from),
                ..Default::default()
            },
        )
        .await
    }

    /// [`follow_engine_run`](Self::follow_engine_run) with every option (`wait` for a run
    /// launched a moment ago, `idle_timeout`).
    pub async fn follow_engine_run_with(
        &self,
        run_id: &str,
        options: FollowRunOptions,
    ) -> ConnectorResult<EngineRunEventStream> {
        let mut follow = Follow {
            conn: self.clone(),
            run_id: run_id.to_string(),
            last: options.after_seq,
            options,
            body: None,
            buf: Vec::new(),
            frame: SseFrame::default(),
            failures: 0,
            done: false,
        };
        follow.connect().await?;
        Ok(Box::pin(futures_util::stream::unfold(
            follow,
            |mut follow| async move {
                let item = follow.next_event().await?;
                Some((item, follow))
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_client_errors_end_a_follow() {
        let http = |status| ConnectorError::Http {
            status,
            body: String::new(),
        };
        for status in [401, 403, 404, 422] {
            assert!(connect_error_is_final(&http(status)), "{status}");
        }
        for status in [429, 500, 502, 503] {
            assert!(!connect_error_is_final(&http(status)), "{status}");
        }
        assert!(!connect_error_is_final(&ConnectorError::Backend(
            "connection refused".into()
        )));
    }
}
