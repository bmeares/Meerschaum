//! The `api:<label>` executor over an injected client.
//!
//! `meerschaum-jobs` sits on `meerschaum-core` only (`crates/README.md`), so it cannot depend on
//! the HTTP client in `meerschaum-api`. Instead it owns the boundary:
//!
//! - [`RemoteJobs`]: a *blocking* client of one remote jobs endpoint, every read fallible;
//! - [`RemoteJobsResolver`]: turns executor keys (`api:main`) into a shared [`RemoteJobs`];
//! - [`crate::JobsContext::with_remote`] installs a resolver, and
//!   [`crate::executor::make_executor`] builds an [`ApiExecutor`] over what it resolves.
//!
//! The host (`meerschaum-cli`) implements both over `meerschaum_api::APIConnector` and owns the
//! Tokio runtime the client blocks on, so there is one runtime per session and none per method
//! call. A context without a resolver answers every operation with an explicit error.
//!
//! Failures are never an empty success: reads which decide something (existence, listings, logs)
//! return `Err`, and the `JobExecutor` getters which cannot (`get_job_status`, ...) report
//! `stopped` / `None` for display only while `Job::start`, listings and logs use the fallible
//! variants.

use std::sync::Arc;

use indexmap::IndexMap;
use meerschaum_core::pyfmt::{PyDateTime, py_str};
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};

use crate::executor::JobExecutor;
use crate::job::{MonitorControl, MonitorOptions};

/// Receives a remote job's log stream.
pub trait RemoteLogSink {
    /// A log line; return `false` to stop monitoring.
    fn on_line(&mut self, line: &str) -> bool;
    /// The job blocks on stdin: the text to send (`None` sends nothing).
    fn on_input(&mut self) -> Option<String>;
    /// The job stopped with this result.
    fn on_stop(&mut self, result: SuccessTuple);
    /// Whether the caller wants to stop (a stop flag was raised).
    fn should_stop(&self) -> bool;
}

/// The options of [`RemoteJobs::monitor_logs`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RemoteMonitor {
    /// Stop when the job reports it has stopped.
    pub stop_on_exit: bool,
    /// Remove the leading timestamps from lines.
    pub strip_timestamps: bool,
    /// Answer the job's stdin requests.
    pub accept_input: bool,
}

/// A blocking client of one remote jobs endpoint (`api:<label>`).
pub trait RemoteJobs: Send + Sync + std::fmt::Debug {
    /// The executor keys (`api:main`).
    fn keys(&self) -> String;
    /// `GET /jobs`: every visible job's metadata by name.
    fn get_jobs(&self) -> Result<IndexMap<String, Map>, String>;
    /// `GET /jobs/{name}`: the job's metadata, or `None` when it does not exist.
    fn get_job_metadata(&self, name: &str) -> Result<Option<Map>, String>;
    /// `GET /jobs/{name}/exists`.
    fn get_job_exists(&self, name: &str) -> Result<bool, String>;
    /// `POST /jobs/{name}`.
    fn create_job(&self, name: &str, sysargs: &[String], properties: Option<&Map>) -> SuccessTuple;
    /// `POST /jobs/{name}/start`.
    fn start_job(&self, name: &str) -> SuccessTuple;
    /// `POST /jobs/{name}/stop`.
    fn stop_job(&self, name: &str) -> SuccessTuple;
    /// `POST /jobs/{name}/pause`.
    fn pause_job(&self, name: &str) -> SuccessTuple;
    /// `DELETE /jobs/{name}`.
    fn delete_job(&self, name: &str) -> SuccessTuple;
    /// `GET /logs/{name}`.
    fn get_logs(&self, name: &str) -> Result<String, String>;
    /// `GET /jobs/{name}/stop_time` (an ISO timestamp).
    fn get_job_stop_time(&self, name: &str) -> Result<Option<String>, String>;
    /// `GET /jobs/{name}/is_blocking_on_stdin`.
    fn get_job_is_blocking_on_stdin(&self, name: &str) -> Result<bool, String>;
    /// `GET /jobs/{name}/prompt_kwargs`.
    fn get_job_prompt_kwargs(&self, name: &str) -> Result<Map, String>;
    /// Stream the job's logs over the websocket until the sink stops, the job stops (with
    /// `stop_on_exit`) or the connection closes.
    fn monitor_logs(
        &self,
        name: &str,
        sink: &mut dyn RemoteLogSink,
        opts: RemoteMonitor,
    ) -> Result<(), String>;
}

/// Resolves executor keys to a shared client.
pub trait RemoteJobsResolver: Send + Sync + std::fmt::Debug {
    /// The client for `api:<label>` (the same client for the same keys, so its login token and
    /// metadata cache are reused).
    fn resolve(&self, executor_keys: &str) -> Result<Arc<dyn RemoteJobs>, String>;
}

/// The `api:<label>` executor.
#[derive(Debug, Clone)]
pub struct ApiExecutor {
    keys: String,
    resolver: Option<Arc<dyn RemoteJobsResolver>>,
}

impl ApiExecutor {
    /// An executor for `api:<label>` over the context's resolver (if any).
    pub fn new(keys: &str, resolver: Option<Arc<dyn RemoteJobsResolver>>) -> ApiExecutor {
        ApiExecutor {
            keys: keys.into(),
            resolver,
        }
    }

    /// The client, or why there is none.
    pub fn client(&self) -> Result<Arc<dyn RemoteJobs>, String> {
        match &self.resolver {
            Some(resolver) => resolver.resolve(&self.keys),
            None => Err(format!(
                "No jobs client is available for the '{}' executor.",
                self.keys
            )),
        }
    }

    fn tuple(&self, f: impl FnOnce(&dyn RemoteJobs) -> SuccessTuple) -> SuccessTuple {
        match self.client() {
            Ok(client) => f(client.as_ref()),
            Err(e) => SuccessTuple::fail(e),
        }
    }

    fn timestamp(&self, name: &str, key: &str) -> Option<String> {
        let props = self.get_job_properties(name);
        props
            .get("process")
            .and_then(|p| p.get(key))
            .filter(|v| !v.is_null())
            .map(py_str)
    }
}

fn metadata_pid(meta: &Map) -> Option<i64> {
    meta.get("daemon")
        .and_then(|d| d.get("pid"))
        .and_then(Value::as_i64)
}

impl JobExecutor for ApiExecutor {
    fn executor_keys(&self) -> String {
        self.keys.clone()
    }

    fn get_job_names(&self) -> Vec<String> {
        self.try_get_job_names().unwrap_or_default()
    }

    fn get_job_exists(&self, name: &str) -> bool {
        self.try_get_job_exists(name).unwrap_or(false)
    }

    fn get_job_metadata(&self, name: &str) -> Map {
        self.try_get_job_metadata(name)
            .ok()
            .flatten()
            .unwrap_or_default()
    }

    fn get_job_properties(&self, name: &str) -> Map {
        self.get_job_metadata(name)
            .get("daemon")
            .and_then(|d| d.get("properties"))
            .map(Value::to_map)
            .unwrap_or_default()
    }

    fn get_job_status(&self, name: &str) -> String {
        self.get_job_metadata(name)
            .get("status")
            .map(py_str)
            .unwrap_or_else(|| "stopped".into())
    }

    fn get_job_began(&self, name: &str) -> Option<String> {
        self.timestamp(name, "began")
    }

    fn get_job_ended(&self, name: &str) -> Option<String> {
        self.timestamp(name, "ended")
    }

    fn get_job_paused(&self, name: &str) -> Option<String> {
        self.timestamp(name, "paused")
    }

    fn create_job(&self, name: &str, sysargs: &[String], properties: Option<&Map>) -> SuccessTuple {
        self.tuple(|c| c.create_job(name, sysargs, properties))
    }

    fn start_job(&self, name: &str) -> SuccessTuple {
        self.tuple(|c| c.start_job(name))
    }

    fn stop_job(&self, name: &str) -> SuccessTuple {
        self.tuple(|c| c.stop_job(name))
    }

    fn pause_job(&self, name: &str) -> SuccessTuple {
        self.tuple(|c| c.pause_job(name))
    }

    fn delete_job(&self, name: &str) -> SuccessTuple {
        self.tuple(|c| c.delete_job(name))
    }

    fn get_logs(&self, name: &str) -> Option<String> {
        self.try_get_logs(name).ok().flatten()
    }

    fn get_job_stop_time(&self, name: &str) -> Option<PyDateTime> {
        let text = self.client().ok()?.get_job_stop_time(name).ok()??;
        crate::pytime::fromisoformat(&text)
    }

    fn monitor_logs(
        &self,
        name: &str,
        callback: &mut dyn FnMut(&str) -> MonitorControl,
        opts: MonitorOptions<'_>,
    ) -> SuccessTuple {
        let client = match self.client() {
            Ok(c) => c,
            Err(e) => return SuccessTuple::fail(e),
        };
        struct Sink<'a, 'b> {
            callback: &'a mut dyn FnMut(&str) -> MonitorControl,
            opts: MonitorOptions<'b>,
        }
        impl RemoteLogSink for Sink<'_, '_> {
            fn on_line(&mut self, line: &str) -> bool {
                (self.callback)(line) == MonitorControl::Continue
            }
            fn on_input(&mut self) -> Option<String> {
                match self.opts.input.as_mut() {
                    Some(input) => input(),
                    None => {
                        let mut line = String::new();
                        std::io::stdin().read_line(&mut line).ok().map(|_| line)
                    }
                }
            }
            fn on_stop(&mut self, result: SuccessTuple) {
                if let Some(on_stop) = self.opts.on_stop.as_mut() {
                    on_stop(&result);
                }
            }
            fn should_stop(&self) -> bool {
                self.opts
                    .stop
                    .as_ref()
                    .is_some_and(|s| s.load(std::sync::atomic::Ordering::Relaxed))
            }
        }
        let remote_opts = RemoteMonitor {
            stop_on_exit: opts.stop_on_exit,
            strip_timestamps: opts.strip_timestamps,
            accept_input: opts.accept_input,
        };
        let mut sink = Sink { callback, opts };
        match client.monitor_logs(name, &mut sink, remote_opts) {
            Ok(()) => SuccessTuple::success(),
            Err(e) => SuccessTuple::fail(e),
        }
    }

    fn get_job_is_blocking_on_stdin(&self, name: &str) -> bool {
        self.client()
            .and_then(|c| c.get_job_is_blocking_on_stdin(name))
            .unwrap_or(false)
    }

    fn get_job_prompt_kwargs(&self, name: &str) -> Map {
        self.client()
            .and_then(|c| c.get_job_prompt_kwargs(name))
            .unwrap_or_default()
    }

    fn try_get_job_names(&self) -> Result<Vec<String>, String> {
        Ok(self.client()?.get_jobs()?.into_keys().collect())
    }

    fn try_get_job_exists(&self, name: &str) -> Result<bool, String> {
        self.client()?.get_job_exists(name)
    }

    fn try_get_job_metadata(&self, name: &str) -> Result<Option<Map>, String> {
        self.client()?.get_job_metadata(name)
    }

    fn try_get_jobs(&self) -> Result<Option<IndexMap<String, Map>>, String> {
        Ok(Some(self.client()?.get_jobs()?))
    }

    fn try_get_logs(&self, name: &str) -> Result<Option<String>, String> {
        self.client()?.get_logs(name).map(Some)
    }

    fn get_job_pid(&self, name: &str) -> Option<i32> {
        metadata_pid(&self.get_job_metadata(name)).map(|p| p as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_context_without_a_client_reports_errors_not_emptiness() {
        let ex = ApiExecutor::new("api:nowhere", None);
        assert!(!ex.create_job("a", &[], None).success);
        assert!(
            ex.start_job("a")
                .message
                .contains("No jobs client is available")
        );
        assert!(ex.try_get_job_exists("a").is_err());
        assert!(ex.try_get_job_names().is_err());
        assert!(ex.try_get_logs("a").is_err());
        assert!(
            !ex.monitor_logs(
                "a",
                &mut |_| MonitorControl::Continue,
                MonitorOptions::default()
            )
            .success
        );
    }
}
