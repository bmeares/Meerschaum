//! Job executors: the `meerschaum.jobs.Executor` interface over the plugin ABI.
//!
//! A plugin declares executor types in its manifest's top-level `executor_types`
//! (`{"<type>": {"type": "<type>", "plugin": "<plugin>", "class_name": ..., "doc": ...}}`).
//! `<type>:<label>` then works wherever executor keys are accepted: `-e` / `--executor-keys`,
//! the shell's `executor` command and compose files.
//!
//! Every method is the JSON method `executor.<name>`. The request's `connector_type` and `label`
//! are the executor keys and `config` is the connector configuration of `<type>:<label>`
//! (`meerschaum:connectors:<type>:<label>` over `...:default`, including `MRSM_<TYPE>_<LABEL>`).
//!
//! | Method | Context | Reply value |
//! |---|---|---|
//! | `executor.get_job_names` | `{}` | `[name]` |
//! | `executor.get_job_exists` | `{name}` | `bool` |
//! | `executor.get_jobs` | `{}` | `{name: metadata}` |
//! | `executor.get_job_metadata` | `{name}` | metadata or `null` |
//! | `executor.get_job_properties` | `{name}` | `{...}` |
//! | `executor.get_job_status` | `{name}` | `"running"` \| `"paused"` \| `"stopped"` |
//! | `executor.get_job_began` / `_ended` / `_paused` | `{name}` | ISO string or `null` |
//! | `executor.create_job` | `{name, sysargs, properties}` | `[bool, message]` |
//! | `executor.start_job` / `stop_job` / `pause_job` / `delete_job` | `{name}` | `[bool, message]` |
//! | `executor.get_logs` | `{name}` | string or `null` |
//! | `executor.get_job_stop_time` | `{name}` | ISO string or `null` |
//! | `executor.get_job_is_blocking_on_stdin` | `{name}` | `bool` |
//! | `executor.get_job_prompt_kwargs` | `{name}` | `{...}` |
//! | `executor.write_stdin` | `{name, data}` | `null` |
//! | `executor.monitor_logs` | `{name, strip_timestamps, stop_on_exit}` | `{"handle": u64}` |
//! | `executor.next_logs` | `{handle, timeout_ms}` | `{"status": "lines" \| "timeout" \| "end", "lines": [str], "result": [bool, str] \| null}` |
//! | `executor.close_logs` | `{handle}` | `null` |
//!
//! Every context also carries `mrsm`: the command that runs the host's Meerschaum (`["/path/mrsm"]`
//! or `["/path/python", "-P", "-m", "meerschaum"]`); in the SDK, [`mrsm_command`] returns it.
//! When the host knows it, the context also carries `root_dir`: the Meerschaum root the jobs
//! belong to (a compose project's root, which differs from the host process's `MRSM_ROOT_DIR`).
//! An executor that keeps state per root reads it from `request.context["root_dir"]` in
//! [`crate::Plugin::executor`].
//!
//! Metadata is `{"sysargs": [...], "result": [bool, str], "restart": bool, "daemon": {"status",
//! "pid", "properties"}}`, as the local and `api:` executors report it; the properties follow the
//! local job layout (`process.began` / `ended` / `paused`, `stop_time`, ...). `sysargs` never
//! contain `-e` / `--executor-keys`: the host removes them before `create_job`.
//!
//! A method a plugin does not implement answers status 3 ("unsupported"); the host turns it into
//! ``(False, "`<method>` is not supported by the '<type>:<label>' executor.")`` for actions and a
//! neutral value (`stopped`, `None`, `False`) for display getters. `executor.next_logs` and
//! `executor.close_logs` are not serialized with the plugin's other calls (like `next_batch`).
//! Every method may make host calls (`host::plugin_config()`, `host::pipe(...)`, ...) when the
//! host attaches its services ([`NativeExecutor::with_host_services`]; the CLI does).
use crate::{
    Result, SuccessTuple, error,
    handles::{Handles, handle_of, timeout_of},
    sdk::{Reply, Request},
};
use serde_json::{Map, Value, json};
use std::{sync::LazyLock, time::Duration};

/// The prefix of the executor JSON methods.
pub const METHOD_PREFIX: &str = "executor.";

/// The "not implemented" error of a default [`Executor`] method.
pub fn unsupported(method: &str) -> crate::Error {
    crate::Error::Unsupported(format!("`{method}` is not supported by this executor."))
}

/// Options of [`Executor::monitor_logs`].
#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct MonitorLogs {
    /// Remove the leading timestamps from lines.
    #[serde(default)]
    pub strip_timestamps: bool,
    /// End the stream once the job stops.
    #[serde(default)]
    pub stop_on_exit: bool,
}

/// One step of a [`LogStream`].
#[derive(Debug, Clone, PartialEq)]
pub enum LogEvent {
    /// New log lines (without trailing newlines).
    Lines(Vec<String>),
    /// Nothing new within the timeout.
    Timeout,
    /// The stream ended (`stop_on_exit` and the job stopped), with the job's result if known.
    End(Option<SuccessTuple>),
}

/// A followed job log. Blocking up to `timeout` in `next_lines` is expected.
pub trait LogStream: Send {
    /// New lines, waiting at most `timeout`.
    fn next_lines(&mut self, timeout: Duration) -> Result<LogEvent>;
    /// Stop following (called once).
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// The executor interface (`meerschaum/jobs/_Executor.py`). Every method has a default: the
/// getters derived from others fall back on them, and the rest are unsupported. The SDK builds
/// a new executor for each call through [`crate::Plugin::executor`], so keep job state outside
/// the value (in files, a database, a static map keyed by label).
#[allow(unused_variables)]
pub trait Executor: Send {
    /// Every job, including hidden ones.
    fn get_job_names(&self) -> Result<Vec<String>> {
        Err(unsupported("get_job_names"))
    }
    /// Whether the job exists (default: in [`Executor::get_job_names`]).
    fn get_job_exists(&self, name: &str) -> Result<bool> {
        Ok(self.get_job_names()?.iter().any(|n| n == name))
    }
    /// Every job's metadata by name (default: names, then each job's metadata).
    fn get_jobs(&self) -> Result<Map<String, Value>> {
        let mut jobs = Map::new();
        for name in self.get_job_names()? {
            let metadata = self.get_job_metadata(&name)?;
            if !metadata.is_null() {
                jobs.insert(name, metadata);
            }
        }
        Ok(jobs)
    }
    /// The job's metadata (`null` when it does not exist).
    fn get_job_metadata(&self, name: &str) -> Result<Value> {
        Err(unsupported("get_job_metadata"))
    }
    /// The job's properties (default: `metadata.daemon.properties`).
    fn get_job_properties(&self, name: &str) -> Result<Value> {
        let metadata = self.get_job_metadata(name)?;
        Ok(match &metadata["daemon"]["properties"] {
            Value::Object(properties) => Value::Object(properties.clone()),
            _ => json!({}),
        })
    }
    /// `running`, `paused` or `stopped` (default: `metadata.daemon.status`).
    fn get_job_status(&self, name: &str) -> Result<String> {
        Ok(self.get_job_metadata(name)?["daemon"]["status"]
            .as_str()
            .unwrap_or("stopped")
            .to_string())
    }
    /// When the job began (default: `properties.process.began`).
    fn get_job_began(&self, name: &str) -> Result<Option<String>> {
        process_timestamp(self.get_job_properties(name)?, "began")
    }
    /// When the job ended (default: `properties.process.ended`).
    fn get_job_ended(&self, name: &str) -> Result<Option<String>> {
        process_timestamp(self.get_job_properties(name)?, "ended")
    }
    /// When the job was paused (default: `properties.process.paused`).
    fn get_job_paused(&self, name: &str) -> Result<Option<String>> {
        process_timestamp(self.get_job_properties(name)?, "paused")
    }
    /// Create and start a job running `mrsm <sysargs>`.
    fn create_job(
        &self,
        name: &str,
        sysargs: &[String],
        properties: &Value,
    ) -> Result<SuccessTuple> {
        Err(unsupported("create_job"))
    }
    /// Start (or resume) a job.
    fn start_job(&self, name: &str) -> Result<SuccessTuple> {
        Err(unsupported("start_job"))
    }
    /// Stop a job.
    fn stop_job(&self, name: &str) -> Result<SuccessTuple> {
        Err(unsupported("stop_job"))
    }
    /// Pause a job.
    fn pause_job(&self, name: &str) -> Result<SuccessTuple> {
        Err(unsupported("pause_job"))
    }
    /// Delete a job.
    fn delete_job(&self, name: &str) -> Result<SuccessTuple> {
        Err(unsupported("delete_job"))
    }
    /// The job's log text.
    fn get_logs(&self, name: &str) -> Result<Option<String>> {
        Err(unsupported("get_logs"))
    }
    /// When the job was manually stopped (ISO timestamp).
    fn get_job_stop_time(&self, name: &str) -> Result<Option<String>> {
        Err(unsupported("get_job_stop_time"))
    }
    /// Follow the job's log.
    fn monitor_logs(&self, name: &str, options: MonitorLogs) -> Result<Box<dyn LogStream>> {
        Err(unsupported("monitor_logs"))
    }
    /// Whether the job waits for input.
    fn get_job_is_blocking_on_stdin(&self, name: &str) -> Result<bool> {
        Err(unsupported("get_job_is_blocking_on_stdin"))
    }
    /// The keyword arguments of the job's pending prompt.
    fn get_job_prompt_kwargs(&self, name: &str) -> Result<Value> {
        Err(unsupported("get_job_prompt_kwargs"))
    }
    /// Answer the job's pending prompt: write `data` (one line, with its newline) to its stdin.
    /// While the host follows a job's log with input accepted (`show logs`, `start job` in the
    /// foreground), it asks [`Executor::get_job_is_blocking_on_stdin`] when no lines arrive and,
    /// if the job is blocking, sends the user's answer here.
    fn write_stdin(&self, name: &str, data: &str) -> Result<()> {
        Err(unsupported("write_stdin"))
    }
}

fn process_timestamp(properties: Value, key: &str) -> Result<Option<String>> {
    Ok(properties["process"][key].as_str().map(str::to_string))
}

static MONITORS: LazyLock<Handles<dyn LogStream>> = LazyLock::new(Handles::new);

thread_local! {
    static MRSM_COMMAND: std::cell::RefCell<Option<Vec<String>>> = const { std::cell::RefCell::new(None) };
}

/// The command that runs the host's Meerschaum, for starting a job's `mrsm <sysargs>`:
/// `[mrsm]` for the standalone binary or `[python, -P, -m, meerschaum]` inside the Python
/// package (the host sends it as `context.mrsm` with every executor call). Outside an executor
/// call it is this process's executable.
pub fn mrsm_command() -> Vec<String> {
    MRSM_COMMAND
        .with(|command| command.borrow().clone())
        .filter(|command| !command.is_empty())
        .unwrap_or_else(|| {
            vec![
                std::env::current_exe()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "mrsm".into()),
            ]
        })
}

struct CommandScope(Option<Vec<String>>);
impl CommandScope {
    fn enter(command: &Value) -> Self {
        let command: Option<Vec<String>> = serde_json::from_value(command.clone()).ok();
        Self(MRSM_COMMAND.with(|slot| slot.replace(command)))
    }
}
impl Drop for CommandScope {
    fn drop(&mut self) {
        let previous = self.0.take();
        MRSM_COMMAND.with(|slot| *slot.borrow_mut() = previous);
    }
}

fn name_of(context: &Value) -> Result<&str> {
    context["name"]
        .as_str()
        .ok_or_else(|| error("missing job name"))
}

fn tuple(result: Result<SuccessTuple>) -> Result<Value> {
    result.and_then(|t| serde_json::to_value(t).map_err(error))
}

/// SDK routing of the `executor.*` methods (`None`: not an executor method).
pub(crate) fn dispatch<P: crate::Plugin>(request: &Request) -> Option<Result<Reply>> {
    let method = request.method.strip_prefix(METHOD_PREFIX)?;
    let context = &request.context;
    // The log-stream methods need no executor value.
    match method {
        "next_logs" => return Some(next_logs(context)),
        "close_logs" => {
            return Some((|| {
                if let Some(entry) = MONITORS.remove(handle_of(context)?) {
                    entry.lock().unwrap_or_else(|e| e.into_inner()).close()?;
                }
                Ok(Reply::json(Value::Null))
            })());
        }
        _ => {}
    }
    let _command = CommandScope::enter(&context["mrsm"]);
    let Some(executor) = P::default().executor(request) else {
        return Some(Err(crate::Error::Unsupported(format!(
            "unknown executor type: {}",
            request.connector_type
        ))));
    };
    let value = (|| -> Result<Value> {
        Ok(match method {
            "get_job_names" => json!(executor.get_job_names()?),
            "get_job_exists" => json!(executor.get_job_exists(name_of(context)?)?),
            "get_jobs" => Value::Object(executor.get_jobs()?),
            "get_job_metadata" => executor.get_job_metadata(name_of(context)?)?,
            "get_job_properties" => executor.get_job_properties(name_of(context)?)?,
            "get_job_status" => json!(executor.get_job_status(name_of(context)?)?),
            "get_job_began" => json!(executor.get_job_began(name_of(context)?)?),
            "get_job_ended" => json!(executor.get_job_ended(name_of(context)?)?),
            "get_job_paused" => json!(executor.get_job_paused(name_of(context)?)?),
            "create_job" => {
                let sysargs: Vec<String> =
                    serde_json::from_value(context["sysargs"].clone()).map_err(error)?;
                let properties = match &context["properties"] {
                    Value::Null => json!({}),
                    other => other.clone(),
                };
                tuple(executor.create_job(name_of(context)?, &sysargs, &properties))?
            }
            "start_job" => tuple(executor.start_job(name_of(context)?))?,
            "stop_job" => tuple(executor.stop_job(name_of(context)?))?,
            "pause_job" => tuple(executor.pause_job(name_of(context)?))?,
            "delete_job" => tuple(executor.delete_job(name_of(context)?))?,
            "get_logs" => json!(executor.get_logs(name_of(context)?)?),
            "get_job_stop_time" => json!(executor.get_job_stop_time(name_of(context)?)?),
            "get_job_is_blocking_on_stdin" => {
                json!(executor.get_job_is_blocking_on_stdin(name_of(context)?)?)
            }
            "get_job_prompt_kwargs" => executor.get_job_prompt_kwargs(name_of(context)?)?,
            "write_stdin" => {
                let data = context["data"]
                    .as_str()
                    .ok_or_else(|| error("missing stdin data"))?;
                executor.write_stdin(name_of(context)?, data)?;
                Value::Null
            }
            "monitor_logs" => {
                let options: MonitorLogs =
                    serde_json::from_value(context.clone()).unwrap_or_default();
                let stream = executor.monitor_logs(name_of(context)?, options)?;
                json!({"handle": MONITORS.insert(stream)})
            }
            other => return Err(unsupported(other)),
        })
    })();
    Some(value.map(Reply::json))
}

fn next_logs(context: &Value) -> Result<Reply> {
    let entry = MONITORS.get(handle_of(context)?)?;
    let mut stream = entry.lock().unwrap_or_else(|e| e.into_inner());
    Ok(Reply::json(
        match stream.next_lines(timeout_of(context))? {
            LogEvent::Lines(lines) => json!({"status": "lines", "lines": lines, "result": null}),
            LogEvent::Timeout => json!({"status": "timeout", "lines": [], "result": null}),
            LogEvent::End(result) => json!({"status": "end", "lines": [], "result": result}),
        },
    ))
}

// ---------------------------------------------------------------------------------------------
// Host side.
// ---------------------------------------------------------------------------------------------

/// An executor `<type>:<label>` of a loaded native plugin.
#[derive(Clone)]
pub struct NativeExecutor {
    plugin: std::sync::Arc<crate::LoadedPlugin>,
    executor_type: String,
    label: String,
    config: Value,
    /// The Meerschaum root the jobs belong to (`context.root_dir`).
    root_dir: Option<String>,
    /// What the executor's host calls reach (`host::plugin_config()`, `host::pipe(...)`).
    host: Option<std::sync::Arc<dyn crate::HostServices>>,
}

impl std::fmt::Debug for NativeExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeExecutor")
            .field("keys", &self.keys())
            .finish()
    }
}

impl NativeExecutor {
    pub(crate) fn new(
        plugin: std::sync::Arc<crate::LoadedPlugin>,
        executor_type: &str,
        label: &str,
        config: Value,
    ) -> Self {
        Self {
            plugin,
            executor_type: executor_type.into(),
            label: label.into(),
            config,
            root_dir: None,
            host: None,
        }
    }

    /// Send `root_dir` (the Meerschaum root the jobs belong to) with every call.
    pub fn with_root_dir(mut self, root_dir: impl Into<String>) -> Self {
        self.root_dir = Some(root_dir.into());
        self
    }

    /// Serve the executor's host calls (every method and the log follow) with `host`.
    pub fn with_host_services(mut self, host: std::sync::Arc<dyn crate::HostServices>) -> Self {
        self.host = Some(host);
        self
    }

    /// `<type>:<label>`.
    pub fn keys(&self) -> String {
        format!("{}:{}", self.executor_type, self.label)
    }

    fn request(&self, method: &str, mut context: Value) -> Request {
        if let Some(object) = context.as_object_mut() {
            object.insert(
                "mrsm".into(),
                json!(meerschaum_core::self_exe::self_command()),
            );
            if let Some(root_dir) = &self.root_dir {
                object.insert("root_dir".into(), json!(root_dir));
            }
        }
        Request {
            method: format!("{METHOD_PREFIX}{method}"),
            connector_type: self.executor_type.clone(),
            label: self.label.clone(),
            config: self.config.clone(),
            context,
        }
    }

    /// Call `executor.<method>` (serialized with the plugin's other calls). An unimplemented
    /// method is [`crate::Error::Unsupported`].
    pub fn call(&self, method: &str, context: Value) -> Result<Value> {
        self.plugin
            .invoke_with_host(&self.request(method, context), None, self.host.as_deref())
            .map(|reply| reply.value)
    }

    /// A stream method of a log follow (`next_logs`, `close_logs`): not serialized.
    fn stream_call(&self, request: &Request) -> Result<crate::Reply> {
        self.plugin
            .invoke_stream_with_host(request, self.host.as_deref())
    }

    /// Follow a job's log.
    pub fn monitor_logs(&self, name: &str, options: MonitorLogs) -> Result<NativeLogStream> {
        let mut context = serde_json::to_value(options).map_err(error)?;
        context["name"] = json!(name);
        let handle = self
            .call("monitor_logs", context)?
            .get("handle")
            .and_then(Value::as_u64)
            .ok_or_else(|| error("monitor_logs returned no handle"))?;
        Ok(NativeLogStream {
            executor: self.clone(),
            handle,
            closed: false,
        })
    }
}

/// A followed log of a [`NativeExecutor`] job. Dropping it closes the handle.
pub struct NativeLogStream {
    executor: NativeExecutor,
    handle: u64,
    closed: bool,
}

impl NativeLogStream {
    /// New lines, waiting at most `timeout` inside the plugin.
    pub fn next_lines(&mut self, timeout: Duration) -> Result<LogEvent> {
        let request = self.executor.request(
            "next_logs",
            json!({"handle": self.handle, "timeout_ms": timeout.as_millis() as u64}),
        );
        let reply = self.executor.stream_call(&request)?.value;
        match reply["status"].as_str() {
            Some("lines") => Ok(LogEvent::Lines(
                serde_json::from_value(reply["lines"].clone()).map_err(error)?,
            )),
            Some("timeout") => Ok(LogEvent::Timeout),
            Some("end") => Ok(LogEvent::End(
                serde_json::from_value(reply["result"].clone()).ok(),
            )),
            other => Err(error(format!(
                "invalid next_logs status: {}",
                other.unwrap_or("null")
            ))),
        }
    }

    /// Stop following (idempotent; also done on drop).
    pub fn close(&mut self) -> Result<()> {
        if std::mem::replace(&mut self.closed, true) {
            return Ok(());
        }
        let request = self
            .executor
            .request("close_logs", json!({"handle": self.handle}));
        self.executor.stream_call(&request).map(|_| ())
    }
}

impl Drop for NativeLogStream {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Plugin;
    use std::sync::Mutex;

    static JOBS: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static STDIN: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

    #[derive(Default)]
    struct Memory;
    struct MemoryExecutor;
    impl Executor for MemoryExecutor {
        fn get_job_names(&self) -> Result<Vec<String>> {
            Ok(JOBS.lock().unwrap().clone())
        }
        fn get_job_metadata(&self, name: &str) -> Result<Value> {
            Ok(if JOBS.lock().unwrap().iter().any(|n| n == name) {
                json!({"sysargs": ["show", "version"], "result": [true, "ok"], "restart": false,
                    "daemon": {"status": "running", "pid": null,
                        "properties": {"process": {"began": "2026-01-01T00:00:00+00:00"}}}})
            } else {
                Value::Null
            })
        }
        fn create_job(&self, name: &str, _: &[String], _: &Value) -> Result<SuccessTuple> {
            JOBS.lock().unwrap().push(name.into());
            Ok(SuccessTuple::ok(format!("Created '{name}'.")))
        }
        fn write_stdin(&self, name: &str, data: &str) -> Result<()> {
            STDIN.lock().unwrap().push((name.into(), data.into()));
            Ok(())
        }
    }
    impl Plugin for Memory {
        const NAME: &'static str = "memory";
        const VERSION: &'static str = "1";
        fn manifest(&self) -> Value {
            Value::Null
        }
        fn executor(&self, request: &Request) -> Option<Box<dyn Executor>> {
            (request.connector_type == "memory").then(|| Box::new(MemoryExecutor) as _)
        }
    }

    fn call(method: &str, context: Value) -> Result<Value> {
        dispatch::<Memory>(&Request {
            method: format!("{METHOD_PREFIX}{method}"),
            connector_type: "memory".into(),
            label: "test".into(),
            config: Value::Null,
            context,
        })
        .unwrap()
        .map(|r| r.value)
    }

    #[test]
    fn executor_methods_defaults_and_unsupported() {
        assert_eq!(
            call(
                "create_job",
                json!({"name": "a", "sysargs": ["show", "version"]})
            )
            .unwrap(),
            json!([true, "Created 'a'."])
        );
        assert_eq!(
            call("get_job_exists", json!({"name": "a"})).unwrap(),
            json!(true)
        );
        assert_eq!(
            call("get_job_exists", json!({"name": "b"})).unwrap(),
            json!(false)
        );
        assert_eq!(
            call("get_job_status", json!({"name": "a"})).unwrap(),
            json!("running")
        );
        assert_eq!(
            call("get_job_began", json!({"name": "a"})).unwrap(),
            json!("2026-01-01T00:00:00+00:00")
        );
        assert_eq!(
            call("get_job_paused", json!({"name": "a"})).unwrap(),
            Value::Null
        );
        assert!(call("get_jobs", json!({})).unwrap()["a"]["daemon"].is_object());
        assert!(matches!(
            call("stop_job", json!({"name": "a"})),
            Err(crate::Error::Unsupported(m)) if m.contains("stop_job")
        ));
        assert!(matches!(
            call("monitor_logs", json!({"name": "a"})),
            Err(crate::Error::Unsupported(_))
        ));
        assert_eq!(
            call("write_stdin", json!({"name": "a", "data": "y\n"})).unwrap(),
            Value::Null
        );
        assert_eq!(
            STDIN.lock().unwrap().as_slice(),
            [("a".to_string(), "y\n".to_string())]
        );
        assert!(call("write_stdin", json!({"name": "a"})).is_err());
        assert!(
            dispatch::<Memory>(&Request {
                method: "fetch".into(),
                connector_type: String::new(),
                label: String::new(),
                config: Value::Null,
                context: Value::Null,
            })
            .is_none()
        );
        let unknown = dispatch::<Memory>(&Request {
            method: "executor.get_job_names".into(),
            connector_type: "other".into(),
            label: "x".into(),
            config: Value::Null,
            context: json!({}),
        })
        .unwrap();
        assert!(matches!(unknown, Err(crate::Error::Unsupported(_))));
    }

    #[test]
    fn mrsm_command_is_scoped_to_the_call() {
        {
            let _scope = CommandScope::enter(&json!(["/opt/python", "-P", "-m", "meerschaum"]));
            assert_eq!(
                mrsm_command(),
                vec!["/opt/python", "-P", "-m", "meerschaum"]
            );
        }
        let fallback = mrsm_command();
        assert_eq!(fallback.len(), 1);
        assert_ne!(fallback[0], "/opt/python");
    }
}
