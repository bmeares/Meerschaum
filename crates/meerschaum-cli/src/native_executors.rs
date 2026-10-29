//! Job executors of native plugins (`<type>:<label>` executor keys): the
//! [`meerschaum_jobs::executor::CustomExecutors`] the CLI installs in every jobs context, and the
//! [`JobExecutor`] adapter over [`meerschaum_plugin::executor::NativeExecutor`].
//!
//! A method the plugin does not implement is ``(False, "`<method>` is not supported by the
//! '<keys>' executor.")`` for actions and a neutral value (`stopped`, `None`, `False`, empty) for
//! the display getters; the fallible listing reads report it as an error. Any other failure of a
//! display getter (a panic or an I/O error in the plugin) is printed as a warning before the
//! neutral value is used, so a broken executor is never silently shown as "stopped". See
//! `crates/meerschaum-plugin/src/executor.rs` for the wire contract.

use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_core::value::{Map, Value};
use meerschaum_jobs::executor::{CustomExecutors, JobExecutor};
use meerschaum_jobs::{MonitorControl, MonitorOptions};
use meerschaum_plugin::NativePlugins;
use meerschaum_plugin::executor::{LogEvent, MonitorLogs, NativeExecutor};
use serde_json::{Value as Json, json};

/// Resolves the executor types the native plugins declare.
pub struct NativeExecutors {
    natives: Arc<NativePlugins>,
    /// `meerschaum:connectors` (the executor's configuration by type and label).
    connectors: Map,
    /// The root the jobs belong to (sent as `context.root_dir`).
    root_dir: String,
    /// What the executors' host calls reach (`host::plugin_config()`, `host::pipe(...)`).
    host: Option<crate::native_plugins::HostEnv>,
}

impl std::fmt::Debug for NativeExecutors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeExecutors")
            .field("types", &self.natives.executor_types())
            .finish()
    }
}

impl NativeExecutors {
    /// A resolver over the native plugins and the config's `meerschaum:connectors` section.
    pub fn new(natives: Arc<NativePlugins>, config: &meerschaum_core::Config) -> NativeExecutors {
        NativeExecutors {
            natives,
            connectors: config
                .get(&["meerschaum", "connectors"])
                .map(|v| v.to_map())
                .unwrap_or_default(),
            root_dir: config.paths().root_dir.to_string_lossy().into_owned(),
            host: None,
        }
    }

    /// Serve the executors' host calls over `session`'s connectors and (a copy of) `config`,
    /// with the trust of this process.
    pub fn with_host(
        mut self,
        session: crate::native::Session,
        config: &meerschaum_core::Config,
    ) -> NativeExecutors {
        self.host = Some(crate::native_plugins::HostEnv::new(
            session,
            crate::native_plugins::host_config(config),
            self.natives.clone(),
            None,
        ));
        self
    }
}

impl CustomExecutors for NativeExecutors {
    fn resolve(&self, executor_keys: &str) -> Option<Result<Box<dyn JobExecutor>, String>> {
        let (kind, label) = executor_keys.split_once(':').unwrap_or((executor_keys, ""));
        let attributes = crate::native_plugins::attributes(&self.connectors, kind, label);
        let config = serde_json::to_value(&attributes).unwrap_or(Json::Null);
        let label = if label.is_empty() { "main" } else { label };
        let executor = self.natives.executor(kind, label, config)?;
        Some(
            executor
                .map(|executor| {
                    let mut executor = executor.with_root_dir(self.root_dir.clone());
                    if let Some(env) = &self.host {
                        executor = executor
                            .with_host_services(crate::native_plugins::local_host(env.clone()));
                    }
                    Box::new(NativeJobExecutor::new(executor)) as Box<dyn JobExecutor>
                })
                .map_err(|e| format!("Cannot use the executor '{executor_keys}': {e}")),
        )
    }
}

/// The resolver over the native plugins of `config`'s plugin directories (through the manifest
/// cache, loading no code on a hit), or `None` when none declares an executor type. For hosts
/// without a CLI session (the Python extension's jobs API).
pub fn from_config(config: &meerschaum_core::Config) -> Option<Arc<NativeExecutors>> {
    let paths = config.paths();
    let natives = NativePlugins::open(
        &paths.plugins_dirs,
        &crate::native_plugins::cache_path(&paths.root_dir),
    )
    .ok()?;
    (!natives.executor_types().is_empty())
        .then(|| Arc::new(NativeExecutors::new(Arc::new(natives), config)))
}

/// [`from_config`] whose executors make their host calls over `session`'s connectors.
pub fn from_config_with_host(
    config: &meerschaum_core::Config,
    session: crate::native::Session,
) -> Option<Arc<NativeExecutors>> {
    let paths = config.paths();
    let natives = NativePlugins::open(
        &paths.plugins_dirs,
        &crate::native_plugins::cache_path(&paths.root_dir),
    )
    .ok()?;
    (!natives.executor_types().is_empty()).then(|| {
        Arc::new(NativeExecutors::new(Arc::new(natives), config).with_host(session, config))
    })
}

/// Whether `executor_type` is declared by a native plugin of this environment (for
/// `parse_executor_keys`, before any session exists): the native plugins of the configured
/// directories, read through the manifest cache.
pub fn is_native_executor_type(executor_type: &str) -> bool {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    static KNOWN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let known = KNOWN.get_or_init(Default::default);
    if known
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(executor_type)
    {
        return true;
    }
    let Ok(config) = meerschaum_core::config::Config::from_env() else {
        return false;
    };
    let paths = config.paths();
    let Ok(natives) = NativePlugins::open(
        &paths.plugins_dirs,
        &crate::native_plugins::cache_path(&paths.root_dir),
    ) else {
        return false;
    };
    let types = natives.executor_types();
    let found = types.iter().any(|t| t == executor_type);
    known
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .extend(types);
    found
}

/// Remove `-e` / `--executor-keys <keys>` from a job's sysargs (the executor runs the job; the
/// keys would only select it again).
pub fn strip_executor_keys(sysargs: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(sysargs.len());
    let mut skip = false;
    for arg in sysargs {
        if std::mem::take(&mut skip) {
            continue;
        }
        if arg == "-e" || arg == "--executor-keys" {
            skip = true;
            continue;
        }
        if arg.starts_with("--executor-keys=") {
            continue;
        }
        out.push(arg.clone());
    }
    out
}

/// A native plugin's executor as a [`JobExecutor`].
#[derive(Debug, Clone)]
pub struct NativeJobExecutor {
    inner: NativeExecutor,
    keys: String,
}

impl NativeJobExecutor {
    /// Wrap a native executor.
    pub fn new(inner: NativeExecutor) -> NativeJobExecutor {
        let keys = inner.keys();
        NativeJobExecutor { inner, keys }
    }

    fn unsupported(&self, method: &str) -> String {
        format!(
            "`{method}` is not supported by the '{}' executor.",
            self.keys
        )
    }

    /// `executor.<method>`; an unimplemented method is the "not supported" message.
    fn call(&self, method: &str, context: Json) -> Result<Json, String> {
        self.inner.call(method, context).map_err(|e| match e {
            meerschaum_plugin::Error::Unsupported(_) => self.unsupported(method),
            other => format!("The '{}' executor failed in `{method}`: {other}", self.keys),
        })
    }

    /// A display getter: the plugin's answer, or `neutral` when the method is not implemented.
    /// Other failures are warned about (the getters cannot return an error) and also give
    /// `neutral`.
    fn shown<T: serde::de::DeserializeOwned>(&self, method: &str, name: &str, neutral: T) -> T {
        match self.named(method, name) {
            Ok(value) => value,
            Err(error) => {
                if error != self.unsupported(method) {
                    eprintln!("Warning: {error}");
                }
                neutral
            }
        }
    }

    fn named<T: serde::de::DeserializeOwned>(&self, method: &str, name: &str) -> Result<T, String> {
        let value = self.call(method, json!({ "name": name }))?;
        serde_json::from_value(value).map_err(|e| {
            format!(
                "The '{}' executor returned an invalid `{method}`: {e}",
                self.keys
            )
        })
    }

    /// When the job blocks on stdin, read the answer from `opts.input` and send it with
    /// `executor.write_stdin`. Returns whether to keep forwarding input (`false` once the
    /// executor does not support it).
    fn answer_prompt(&self, name: &str, opts: &mut MonitorOptions<'_>) -> bool {
        let blocking = match self.named::<bool>("get_job_is_blocking_on_stdin", name) {
            Ok(blocking) => blocking,
            Err(error) => return error != self.unsupported("get_job_is_blocking_on_stdin"),
        };
        if !blocking {
            return true;
        }
        let Some(data) = opts.input.as_mut().and_then(|input| input()) else {
            return true;
        };
        let data = if data.ends_with('\n') {
            data
        } else {
            format!("{data}\n")
        };
        match self.call("write_stdin", json!({ "name": name, "data": data })) {
            Ok(_) => true,
            Err(error) if error == self.unsupported("write_stdin") => false,
            Err(error) => {
                eprintln!("Warning: {error}");
                true
            }
        }
    }

    fn tuple(&self, method: &str, context: Json) -> SuccessTuple {
        match self.call(method, context) {
            Ok(value) => serde_json::from_value(value).unwrap_or_else(|e| {
                SuccessTuple::fail(format!(
                    "The '{}' executor returned an invalid `{method}`: {e}",
                    self.keys
                ))
            }),
            Err(e) => SuccessTuple::fail(e),
        }
    }
}

fn to_map(value: Json) -> Map {
    serde_json::from_value::<Value>(value)
        .map(|v| v.to_map())
        .unwrap_or_default()
}

impl JobExecutor for NativeJobExecutor {
    fn executor_keys(&self) -> String {
        self.keys.clone()
    }

    fn get_job_names(&self) -> Vec<String> {
        self.try_get_job_names().unwrap_or_default()
    }

    fn get_job_exists(&self, name: &str) -> bool {
        self.shown("get_job_exists", name, false)
    }

    fn get_job_metadata(&self, name: &str) -> Map {
        to_map(self.shown("get_job_metadata", name, Json::Null))
    }

    fn get_job_properties(&self, name: &str) -> Map {
        to_map(self.shown("get_job_properties", name, Json::Null))
    }

    fn get_job_status(&self, name: &str) -> String {
        self.shown("get_job_status", name, "stopped".to_string())
    }

    fn get_job_began(&self, name: &str) -> Option<String> {
        self.shown("get_job_began", name, None)
    }

    fn get_job_ended(&self, name: &str) -> Option<String> {
        self.shown("get_job_ended", name, None)
    }

    fn get_job_paused(&self, name: &str) -> Option<String> {
        self.shown("get_job_paused", name, None)
    }

    fn create_job(&self, name: &str, sysargs: &[String], properties: Option<&Map>) -> SuccessTuple {
        let properties = properties
            .map(|p| serde_json::to_value(p).unwrap_or(Json::Null))
            .unwrap_or_else(|| json!({}));
        self.tuple(
            "create_job",
            json!({
                "name": name,
                "sysargs": strip_executor_keys(sysargs),
                "properties": properties,
            }),
        )
    }

    fn start_job(&self, name: &str) -> SuccessTuple {
        self.tuple("start_job", json!({ "name": name }))
    }

    fn stop_job(&self, name: &str) -> SuccessTuple {
        self.tuple("stop_job", json!({ "name": name }))
    }

    fn pause_job(&self, name: &str) -> SuccessTuple {
        self.tuple("pause_job", json!({ "name": name }))
    }

    fn delete_job(&self, name: &str) -> SuccessTuple {
        self.tuple("delete_job", json!({ "name": name }))
    }

    fn get_logs(&self, name: &str) -> Option<String> {
        self.shown("get_logs", name, None)
    }

    fn get_job_stop_time(&self, name: &str) -> Option<PyDateTime> {
        let text: Option<String> = self.shown("get_job_stop_time", name, None);
        meerschaum_jobs::pytime::fromisoformat(&text?)
    }

    fn monitor_logs(
        &self,
        name: &str,
        callback: &mut dyn FnMut(&str) -> MonitorControl,
        mut opts: MonitorOptions<'_>,
    ) -> SuccessTuple {
        let options = MonitorLogs {
            strip_timestamps: opts.strip_timestamps,
            stop_on_exit: opts.stop_on_exit,
        };
        let mut stream = match self.inner.monitor_logs(name, options) {
            Ok(stream) => stream,
            Err(meerschaum_plugin::Error::Unsupported(_)) => {
                return SuccessTuple::fail(self.unsupported("monitor_logs"));
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let poll = opts
            .poll_interval
            .unwrap_or(Duration::from_millis(250))
            .clamp(Duration::from_millis(10), Duration::from_secs(5));
        let stopped = |opts: &MonitorOptions<'_>| {
            opts.stop
                .as_ref()
                .is_some_and(|s| s.load(std::sync::atomic::Ordering::Relaxed))
        };
        // Answer prompts while no lines arrive (as the local executor does with `accept_input`),
        // until the executor reports it cannot.
        let mut forward_input = opts.accept_input && opts.input.is_some();
        loop {
            if stopped(&opts) {
                break;
            }
            match stream.next_lines(poll) {
                Ok(LogEvent::Lines(lines)) => {
                    for line in lines {
                        if callback(&line) == MonitorControl::Stop {
                            return SuccessTuple::success();
                        }
                    }
                }
                Ok(LogEvent::Timeout) => {
                    if forward_input {
                        forward_input = self.answer_prompt(name, &mut opts);
                    }
                }
                Ok(LogEvent::End(result)) => {
                    if let (Some(result), Some(on_stop)) = (result, opts.on_stop.as_mut()) {
                        on_stop(&result);
                    }
                    break;
                }
                Err(e) => return SuccessTuple::fail(e.to_string()),
            }
        }
        SuccessTuple::success()
    }

    fn get_job_is_blocking_on_stdin(&self, name: &str) -> bool {
        self.shown("get_job_is_blocking_on_stdin", name, false)
    }

    fn get_job_prompt_kwargs(&self, name: &str) -> Map {
        to_map(self.shown("get_job_prompt_kwargs", name, Json::Null))
    }

    fn try_get_job_names(&self) -> Result<Vec<String>, String> {
        let value = self.call("get_job_names", json!({}))?;
        serde_json::from_value(value).map_err(|e| e.to_string())
    }

    fn try_get_job_exists(&self, name: &str) -> Result<bool, String> {
        self.named("get_job_exists", name)
    }

    fn try_get_job_metadata(&self, name: &str) -> Result<Option<Map>, String> {
        let value = self.call("get_job_metadata", json!({ "name": name }))?;
        Ok((!value.is_null()).then(|| to_map(value)))
    }

    fn try_get_jobs(&self) -> Result<Option<IndexMap<String, Map>>, String> {
        match self.inner.call("get_jobs", json!({})) {
            Ok(Json::Object(jobs)) => Ok(Some(
                jobs.into_iter()
                    .map(|(name, meta)| (name, to_map(meta)))
                    .collect(),
            )),
            Ok(_) => Err(format!(
                "The '{}' executor returned an invalid `get_jobs`.",
                self.keys
            )),
            // Listing by name instead.
            Err(meerschaum_plugin::Error::Unsupported(_)) => Ok(None),
            Err(e) => Err(format!(
                "The '{}' executor failed in `get_jobs`: {e}",
                self.keys
            )),
        }
    }

    fn try_get_logs(&self, name: &str) -> Result<Option<String>, String> {
        self.named("get_logs", name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_keys_are_removed_from_job_sysargs() {
        let args: Vec<String> = ["sync", "pipes", "-e", "procs:a", "-i", "sql:local"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            strip_executor_keys(&args),
            vec!["sync", "pipes", "-i", "sql:local"]
        );
        let args: Vec<String> = ["show", "version", "--executor-keys=procs:a"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(strip_executor_keys(&args), vec!["show", "version"]);
    }
}
