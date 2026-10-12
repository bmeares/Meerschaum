//! Job routes and the logs websocket (`meerschaum/connectors/api/_jobs.py`).

use std::time::Instant;

use chrono::NaiveDateTime;
use futures_util::{SinkExt, StreamExt};
use indexmap::IndexMap;
use meerschaum_core::{Map, SuccessTuple, Value};
use tokio_tungstenite::tungstenite::Message;

use crate::models::{CreateJobRequest, JobMetadata};
use crate::pipe_ops;
use crate::routes::{self, JOB_METADATA_CACHE_SECONDS, JOBS_STDIN_MESSAGE, JOBS_STOP_MESSAGE};
use crate::traits::{ConnectorError, ConnectorResult};

use super::pipes::py_json;
use super::{APIConnector, ApiResponse, Body, RequestOptions};

/// Callbacks for [`APIConnector::monitor_logs`] (`callback_function`,
/// `input_callback_function`, `stop_callback_function`).
pub struct LogsMonitor<'a> {
    /// Called with each log line.
    pub on_line: Box<dyn FnMut(String) + Send + 'a>,
    /// Called when the job blocks on stdin; the returned text is sent to the job.
    pub on_input: Option<Box<dyn FnMut() -> String + Send + 'a>>,
    /// Called with the job's result when it stops.
    pub on_stop: Option<Box<dyn FnMut(SuccessTuple) + Send + 'a>>,
    /// Stop monitoring when the job stops.
    pub stop_on_exit: bool,
    /// Remove the `%Y-%m-%d %H:%M | ` prefix from lines.
    pub strip_timestamps: bool,
}

/// `strip_timestamp_from_line(line)` with the default `jobs:logs:timestamps:format`
/// (`%Y-%m-%d %H:%M`).
pub fn strip_timestamp_from_line(line: &str) -> String {
    const LEN: usize = 16; // len(now.strftime('%Y-%m-%d %H:%M'))
    if line.len() >= LEN && line.is_char_boundary(LEN) {
        let prefix = &line[..LEN];
        if NaiveDateTime::parse_from_str(prefix, "%Y-%m-%d %H:%M").is_ok() {
            return line.get(LEN + 3..).unwrap_or("").to_string();
        }
    }
    line.to_string()
}

fn job_tuple(r: &ApiResponse) -> SuccessTuple {
    if !r.ok() {
        let text = r.text();
        if text.contains("detail") {
            if let Ok(serde_json::Value::Object(o)) = r.json() {
                if let Some(d) = o.get("detail") {
                    return SuccessTuple::fail(match d {
                        serde_json::Value::String(s) => s.clone(),
                        other => crate::models::detail_repr(other),
                    });
                }
            }
        }
        return SuccessTuple::fail(text);
    }
    match r.json() {
        Ok(serde_json::Value::Array(a)) if a.len() >= 2 => SuccessTuple {
            success: a[0].as_bool().unwrap_or(false),
            message: a[1].as_str().map(String::from).unwrap_or_default(),
        },
        _ => SuccessTuple::fail(r.text()),
    }
}

impl APIConnector {
    /// `get_jobs()`: `GET /jobs`, the remote jobs' metadata by name (`{}` on errors).
    pub async fn get_jobs(&self) -> ConnectorResult<IndexMap<String, JobMetadata>> {
        let r = self.get(routes::JOBS, RequestOptions::default()).await?;
        if !r.ok() {
            tracing::warn!("Failed to get remote jobs from {self}.");
            return Ok(IndexMap::new());
        }
        Ok(serde_json::from_value(r.json()?)?)
    }

    /// `get_job_metadata(name)`: `GET /jobs/{name}`, cached for 5 seconds (`{}` if missing).
    pub async fn get_job_metadata(&self, name: &str) -> ConnectorResult<Map> {
        if let Ok(cache) = self.job_metadata_cache.lock() {
            if let Some((at, meta)) = cache.get(name) {
                if at.elapsed().as_secs() < JOB_METADATA_CACHE_SECONDS {
                    return Ok(meta.clone());
                }
            }
        }
        let r = self
            .get(
                &format!("{}/{name}", routes::JOBS),
                RequestOptions::default(),
            )
            .await?;
        if !r.ok() {
            return Ok(Map::new());
        }
        let meta = pipe_ops::json_to_core(&r.json()?).to_map();
        if let Ok(mut cache) = self.job_metadata_cache.lock() {
            cache.insert(name.to_string(), (Instant::now(), meta.clone()));
        }
        Ok(meta)
    }

    /// Forget the cached job metadata (v4 keeps it for 5 seconds).
    pub fn clear_job_metadata_cache(&self) {
        if let Ok(mut cache) = self.job_metadata_cache.lock() {
            cache.clear();
        }
    }

    /// `get_job(name)`: the metadata, or an error if the job does not exist.
    pub async fn get_job(&self, name: &str) -> ConnectorResult<JobMetadata> {
        let meta = self.get_job_metadata(name).await?;
        if meta.is_empty() {
            return Err(ConnectorError::Invalid(format!(
                "Job '{name}' does not exist."
            )));
        }
        Ok(serde_json::from_value(serde_json::to_value(Value::Map(
            meta,
        ))?)?)
    }

    /// `get_job_properties(name)`: `metadata['daemon']['properties']`.
    pub async fn get_job_properties(&self, name: &str) -> ConnectorResult<Map> {
        let meta = self.get_job_metadata(name).await?;
        Ok(meta
            .get("daemon")
            .and_then(|d| d.get("properties"))
            .map(|p| p.to_map())
            .unwrap_or_default())
    }

    /// `get_job_status(name)`: `metadata['status']`, default `stopped`.
    pub async fn get_job_status(&self, name: &str) -> ConnectorResult<String> {
        let meta = self.get_job_metadata(name).await?;
        Ok(meta
            .get("status")
            .map(meerschaum_core::pyfmt::py_str)
            .unwrap_or_else(|| "stopped".into()))
    }

    async fn daemon_property(&self, name: &str, key: &str) -> ConnectorResult<Option<String>> {
        let props = self.get_job_properties(name).await?;
        Ok(props
            .get("daemon")
            .and_then(|d| d.get(key))
            .filter(|v| !v.is_null())
            .map(meerschaum_core::pyfmt::py_str))
    }

    /// `get_job_began(name)`.
    pub async fn get_job_began(&self, name: &str) -> ConnectorResult<Option<String>> {
        self.daemon_property(name, "began").await
    }

    /// `get_job_ended(name)`.
    pub async fn get_job_ended(&self, name: &str) -> ConnectorResult<Option<String>> {
        self.daemon_property(name, "ended").await
    }

    /// `get_job_paused(name)`.
    pub async fn get_job_paused(&self, name: &str) -> ConnectorResult<Option<String>> {
        self.daemon_property(name, "paused").await
    }

    /// `get_job_exists(name)`: `GET /jobs/{name}/exists`.
    pub async fn get_job_exists(&self, name: &str) -> ConnectorResult<bool> {
        let r = self
            .get(
                &format!("{}/{name}/exists", routes::JOBS),
                RequestOptions::default(),
            )
            .await?;
        if !r.ok() {
            tracing::warn!("Failed to determine whether job '{name}' exists.");
            return Ok(false);
        }
        Ok(r.json()?.as_bool().unwrap_or(false))
    }

    /// `delete_job(name)`: `DELETE /jobs/{name}`.
    pub async fn delete_job(&self, name: &str) -> SuccessTuple {
        match self
            .delete(
                &format!("{}/{name}", routes::JOBS),
                RequestOptions::default(),
            )
            .await
        {
            Ok(r) => job_tuple(&r),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `start_job(name)`: `POST /jobs/{name}/start`.
    pub async fn start_job(&self, name: &str) -> SuccessTuple {
        self.job_post(name, "/start").await
    }

    /// `stop_job(name)`: `POST /jobs/{name}/stop`.
    pub async fn stop_job(&self, name: &str) -> SuccessTuple {
        self.job_post(name, "/stop").await
    }

    /// `pause_job(name)`: `POST /jobs/{name}/pause`.
    pub async fn pause_job(&self, name: &str) -> SuccessTuple {
        self.job_post(name, "/pause").await
    }

    async fn job_post(&self, name: &str, suffix: &str) -> SuccessTuple {
        match self
            .post(
                &format!("{}/{name}{suffix}", routes::JOBS),
                RequestOptions::default(),
            )
            .await
        {
            Ok(r) => job_tuple(&r),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `create_job(name, sysargs, properties)`: `POST /jobs/{name}`.
    pub async fn create_job(
        &self,
        name: &str,
        sysargs: &[String],
        properties: Option<Map>,
    ) -> SuccessTuple {
        let body = CreateJobRequest {
            sysargs: sysargs.to_vec(),
            properties,
        };
        let body = match serde_json::to_value(&body) {
            Ok(v) => py_json(&pipe_ops::json_to_core(&v)),
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match self
            .post(
                &format!("{}/{name}", routes::JOBS),
                RequestOptions {
                    body: Body::Json(body),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => job_tuple(&r),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `get_logs(name)`: `GET /logs/{name}` (errors raise).
    pub async fn get_logs(&self, name: &str) -> ConnectorResult<String> {
        let r = self
            .get(
                &format!("{}/{name}", routes::LOGS),
                RequestOptions::default(),
            )
            .await?;
        if !r.ok() {
            return Err(ConnectorError::Invalid(format!(
                "Cannot fetch logs for job '{name}':\n{}",
                r.text()
            )));
        }
        Ok(match r.json()? {
            serde_json::Value::String(s) => s,
            other => other.to_string(),
        })
    }

    /// `get_job_stop_time(name)`: `GET /jobs/{name}/stop_time`.
    pub async fn get_job_stop_time(&self, name: &str) -> ConnectorResult<Option<String>> {
        let r = self
            .get(
                &format!("{}/{name}/stop_time", routes::JOBS),
                RequestOptions::default(),
            )
            .await?;
        if !r.ok() {
            tracing::warn!("Failed to get stop time for job '{name}':\n{}", r.text());
            return Ok(None);
        }
        Ok(r.json()?.as_str().map(String::from))
    }

    /// `get_job_is_blocking_on_stdin(name)`: `GET /jobs/{name}/is_blocking_on_stdin`.
    pub async fn get_job_is_blocking_on_stdin(&self, name: &str) -> ConnectorResult<bool> {
        let r = self
            .get(
                &format!("{}/{name}/is_blocking_on_stdin", routes::JOBS),
                RequestOptions::default(),
            )
            .await?;
        if !r.ok() {
            return Ok(false);
        }
        Ok(r.json()?.as_bool().unwrap_or(false))
    }

    /// `get_job_prompt_kwargs(name)`: `GET /jobs/{name}/prompt_kwargs`.
    ///
    /// PARITY: the v4 route is registered as `/jobs{name}/prompt_kwargs` (missing slash), so
    /// this request 404s and returns `{}` against a v4 server (`api/routes/_jobs.py:300`).
    pub async fn get_job_prompt_kwargs(&self, name: &str) -> ConnectorResult<Map> {
        let r = self
            .get(
                &format!("{}/{name}/prompt_kwargs", routes::JOBS),
                RequestOptions::default(),
            )
            .await?;
        if !r.ok() {
            return Ok(Map::new());
        }
        Ok(pipe_ops::json_to_core(&r.json()?).to_map())
    }

    /// The logs websocket URL: `ws[s]://{host}:{port}/logs/{name}/ws`.
    ///
    /// PARITY: without a configured port the URL keeps the empty port (`host:/logs/...`).
    pub fn logs_websocket_url(&self, name: &str) -> String {
        let protocol = if self.uri().starts_with("http://") {
            "ws"
        } else {
            "wss"
        };
        let port = self.attr("port").unwrap_or_default();
        format!(
            "{protocol}://{}:{port}{}/{name}/ws",
            self.attr("host").unwrap_or_default(),
            routes::LOGS
        )
    }

    /// `monitor_logs_async(name, ...)`: stream a job's logs over the websocket. The token (or
    /// `no-login`) is sent first; `MRSM_STDIN` asks for input, `MRSM_STOP` is followed by the
    /// job's result as JSON.
    pub async fn monitor_logs(
        &self,
        name: &str,
        mut monitor: LogsMonitor<'_>,
    ) -> ConnectorResult<()> {
        let url = self.logs_websocket_url(name);
        let (ws, _) = tokio_tungstenite::connect_async(url.as_str())
            .await
            .map_err(|e| ConnectorError::Backend(e.to_string()))?;
        let (mut tx, mut rx) = ws.split();
        let token = self.token().await.unwrap_or_else(|| "no-login".into());
        let _ = tx.send(Message::text(token)).await;

        async fn recv_text(
            rx: &mut (
                     impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
                     + Unpin
                 ),
        ) -> Option<Result<String, ConnectorError>> {
            loop {
                match rx.next().await? {
                    Ok(Message::Text(t)) => return Some(Ok(t.to_string())),
                    Ok(Message::Binary(b)) => {
                        return Some(Ok(String::from_utf8_lossy(&b).into_owned()));
                    }
                    Ok(Message::Close(_)) => return None,
                    Ok(_) => continue,
                    Err(e) => return Some(Err(ConnectorError::Backend(e.to_string()))),
                }
            }
        }

        while let Some(msg) = recv_text(&mut rx).await {
            let msg = msg?;
            if msg == JOBS_STDIN_MESSAGE {
                if let Some(f) = monitor.on_input.as_mut() {
                    let data = f();
                    tx.send(Message::text(data))
                        .await
                        .map_err(|e| ConnectorError::Backend(e.to_string()))?;
                }
                continue;
            }
            if msg == JOBS_STOP_MESSAGE {
                let result = match recv_text(&mut rx).await {
                    Some(Ok(text)) => crate::models::parse_success_tuple(false, &text),
                    Some(Err(e)) => SuccessTuple::fail(e.to_string()),
                    None => SuccessTuple::fail("Connection closed."),
                };
                if let Some(f) = monitor.on_stop.as_mut() {
                    f(result);
                }
                if monitor.stop_on_exit {
                    let _ = tx.send(Message::Close(None)).await;
                    break;
                }
                continue;
            }
            let line = if monitor.strip_timestamps {
                strip_timestamp_from_line(&msg)
            } else {
                msg
            };
            (monitor.on_line)(line);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_timestamps() {
        assert_eq!(
            strip_timestamp_from_line("2026-10-12 17:09 | hello"),
            "hello"
        );
        assert_eq!(strip_timestamp_from_line("hello"), "hello");
        assert_eq!(strip_timestamp_from_line("2026-10-12 17:09 | "), "");
    }
}
