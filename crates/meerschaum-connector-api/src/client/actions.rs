//! `/actions` (`meerschaum/connectors/api/_actions.py`).

use std::sync::{Arc, Mutex};

use meerschaum_core::{Map, SuccessTuple, Value};

use crate::routes::{self, JOBS_TEMP_PREFIX};
use crate::traits::ConnectorResult;

use super::pipes::py_json;
use super::{APIConnector, Body, LogsMonitor, RequestOptions};

/// How many times `do_action` reads a job's result from its metadata (100 ms apart).
const RESULT_READ_ATTEMPTS: usize = 10;

impl APIConnector {
    /// `get_actions()`: `GET /actions`, the available action names.
    pub async fn get_actions(&self) -> ConnectorResult<Vec<String>> {
        let j = self
            .get(routes::ACTIONS, RequestOptions::default())
            .await?
            .json_or_detail()?;
        Ok(serde_json::from_value(j)?)
    }

    /// `do_action_legacy(action, **kw)`: `POST /actions/{action[0]}` with the keyword arguments
    /// (`action` without its first word, `noask` and `yes` defaulting to `True`).
    ///
    /// PARITY: the body is a JSON string sent without a `Content-Type` (see `register_token`).
    pub async fn do_action_legacy(&self, action: &[String], kwargs: Map) -> SuccessTuple {
        let mut json_dict = kwargs;
        let root_action = action.first().cloned().unwrap_or_default();
        json_dict.insert(
            "action".into(),
            Value::List(
                action
                    .iter()
                    .skip(1)
                    .map(|a| Value::Str(a.clone()))
                    .collect(),
            ),
        );
        if !json_dict.contains_key("noask") {
            json_dict.insert("noask".into(), Value::Bool(true));
        }
        if !json_dict.contains_key("yes") {
            json_dict.insert("yes".into(), Value::Bool(true));
        }
        let r = match self
            .post(
                &format!("{}/{root_action}", routes::ACTIONS),
                RequestOptions {
                    body: Body::Text(py_json(&Value::Map(json_dict))),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match r.json() {
            Ok(serde_json::Value::Object(o)) if o.contains_key("detail") => {
                SuccessTuple::fail(match &o["detail"] {
                    serde_json::Value::String(s) => s.clone(),
                    other => crate::models::detail_repr(other),
                })
            }
            Ok(serde_json::Value::Array(a)) if a.len() >= 2 => SuccessTuple {
                success: a[0].as_bool().unwrap_or(false),
                message: a[1].as_str().map(String::from).unwrap_or_default(),
            },
            Ok(_) => SuccessTuple::fail(format!(
                "Failed to parse result from action '{root_action}'"
            )),
            Err(_) => SuccessTuple::fail(r.text()),
        }
    }

    /// `do_action_async(sysargs)`: run the action as a temporary remote job
    /// (`.api-temp-{random}` unless `job_name` is given), stream its output to `on_line`, and
    /// return its result after deleting the job.
    pub async fn do_action(
        &self,
        sysargs: &[String],
        job_name: Option<&str>,
        on_line: Box<dyn FnMut(String) + Send + '_>,
    ) -> SuccessTuple {
        let name = match job_name {
            Some(n) => n.to_string(),
            None => format!(
                "{JOBS_TEMP_PREFIX}{}",
                meerschaum_core::users::generate_password(12).unwrap_or_default()
            ),
        };
        let mut logs = Map::new();
        logs.insert("lines_to_show".into(), Value::Int(0));
        let mut properties = Map::new();
        properties.insert("logs".into(), Value::Map(logs));
        let created = self.create_job(&name, sysargs, Some(properties)).await;
        if !created.success {
            return created;
        }
        let result: Arc<Mutex<Option<SuccessTuple>>> = Arc::new(Mutex::new(None));
        let sink = result.clone();
        let monitor = LogsMonitor {
            on_line,
            on_input: None,
            on_stop: Some(Box::new(move |r| {
                if let Ok(mut s) = sink.lock() {
                    *s = Some(r);
                }
            })),
            stop_on_exit: true,
            strip_timestamps: true,
        };
        if let Err(e) = self.monitor_logs(&name, monitor).await {
            let _ = self.delete_job(&name).await;
            return SuccessTuple::fail(e.to_string());
        }
        // The websocket reports the result when the job produced output; a job which finished
        // without any (or before the monitor connected) closes it silently, so read the result
        // from the job like v4 does (`job.result`).
        let streamed = result.lock().ok().and_then(|r| r.clone());
        let fetched = match streamed {
            Some(r) => Some(r),
            None => {
                // The supervisor saves the result a moment after the job's last output, so a
                // missing result is read again a few times (about a second in all) before giving
                // up.
                let mut found = None;
                for attempt in 0..RESULT_READ_ATTEMPTS {
                    if attempt > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    self.clear_job_metadata_cache();
                    found = self.get_job_metadata(&name).await.ok().and_then(|meta| {
                        match meta.get("result")?.as_list()?.as_slice() {
                            [success, message, ..] => Some(SuccessTuple {
                                success: success.truthy(),
                                message: message.py_str(),
                            }),
                            _ => None,
                        }
                    });
                    if found.is_some() {
                        break;
                    }
                }
                found
            }
        };
        let _ = self.delete_job(&name).await;
        fetched
            .unwrap_or_else(|| SuccessTuple::fail(format!("Job '{name}' did not report a result.")))
    }
}
