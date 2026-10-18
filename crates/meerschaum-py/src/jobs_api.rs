//! `meerschaum-jobs` bound into `meerschaum._core`: the Rust side of `meerschaum.jobs.Job`, the
//! executors, `meerschaum.utils.daemon.Daemon` (a read/control façade) and the job listings
//! (wave 16, task S).
//!
//! The Python classes are façades: every operation is one call into the Rust types of
//! `meerschaum-jobs` (the same ones the `mrsm` binary and the API server use), so the on-disk
//! layout, the status rules and the job-scan fix of wave 16 (M2: no process-table scan for a job
//! which has ended) are not re-implemented in Python.
//!
//! **Launching.** A job runs `<mrsm> <sysargs>` under a detached supervisor
//! (`<mrsm> __mrsm_job_supervisor__ ...`), where `<mrsm>` is the process's own command
//! (`meerschaum_core::self_exe::self_command`). Inside the extension that is
//! `<sys.executable> -m meerschaum` (the `mrsm` console script and `python -m meerschaum` run the
//! CLI from this same extension), or the standalone binary named by `MRSM_BIN` when it is set to an
//! executable. `MRSM_JOB_SUPERVISOR` / `MRSM_JOB_PROGRAM` (shell-split) override either part.
//!
//! **`api:<label>` executors** resolve through the CLI's `CliRemoteResolver`, built once per
//! process and per snapshot of the `meerschaum:connectors:api` configuration (so the login token
//! and the job metadata cache are shared by every `Job` on that endpoint), over the extension's
//! runtime. Connectors defined ad hoc in Python (`api_attributes`) patch their label's section.
//!
//! **Monitoring** runs the Rust log follower on a helper thread. Callbacks take the GIL for each
//! line; the calling thread waits with the GIL released, polls for signals (`Ctrl-C` raises
//! `KeyboardInterrupt` in the caller) and for the caller's `stop_event`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indexmap::IndexMap;
use meerschaum_cli::native::remote_jobs::CliRemoteResolver;
use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_core::value::{Map, Value};
use meerschaum_core::{SuccessTuple, config::Config};
use meerschaum_jobs::config::{JobPaths, JobsConfig, Seconds};
use meerschaum_jobs::daemon::{Daemon, JobsContext, Status};
use meerschaum_jobs::executor::{self, JobExecutor, make_executor};
use meerschaum_jobs::job::{Job, JobOptions, MonitorControl, MonitorOptions};
use meerschaum_jobs::rotating::{RotatingFile, RotatingFileOptions};
use meerschaum_jobs::stdin::StdinFile;
use meerschaum_jobs::supervisor::{Launcher, SUPERVISOR_ENV};
use meerschaum_jobs::systemd::SystemdExecutor;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

use crate::convert::{core_to_py, py_to_core};
use crate::pipe_bridge::success_to_py;

fn runtime_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ---------------------------------------------------------------------------------------------
// Context

/// The launcher of this process's jobs: its own command (see [`crate::main_api`]) unless
/// `MRSM_JOB_SUPERVISOR` / `MRSM_JOB_PROGRAM` say otherwise.
fn resolve_launcher(py: Python<'_>) -> PyResult<Launcher> {
    crate::main_api::ensure_self_command(py)?;
    Ok(Launcher::from_env())
}

/// One `CliRemoteResolver` per process and API configuration (a fork builds its own, like the
/// runtime does).
fn resolver_for(section: Map) -> Arc<CliRemoteResolver> {
    type Resolvers = (u32, HashMap<String, Arc<CliRemoteResolver>>);
    static RESOLVERS: Mutex<Option<Resolvers>> = Mutex::new(None);
    let pid = std::process::id();
    let fingerprint = Value::Map(section.clone()).py_str();
    let mut slot = RESOLVERS.lock().unwrap_or_else(|e| e.into_inner());
    match slot.as_mut() {
        Some((owner, _)) if *owner != pid => *slot = None,
        _ => {}
    }
    let (_, map) = slot.get_or_insert_with(|| (pid, HashMap::new()));
    map.entry(fingerprint)
        .or_insert_with(|| {
            Arc::new(CliRemoteResolver::new(
                crate::facade::worker_session(),
                section,
            ))
        })
        .clone()
}

/// A jobs context for the live Python config.
#[derive(Clone)]
struct Ctx {
    jobs: JobsContext,
}

fn api_overrides(attributes: Option<&Bound<'_, PyDict>>) -> PyResult<Map> {
    let mut out = Map::new();
    if let Some(attributes) = attributes {
        for (keys, attrs) in attributes.iter() {
            let keys: String = keys.str()?.extract()?;
            let label = keys.strip_prefix("api:").unwrap_or(&keys).to_string();
            out.insert(label, py_to_core(&attrs)?);
        }
    }
    Ok(out)
}

/// What a [`Ctx`] depends on: the sections of the live config the jobs read, the root, the
/// launcher inputs and the ad hoc API connectors. Cheap to compute (a few small sections),
/// unlike [`crate::config_api::live_config`], which converts the whole dictionary.
fn ctx_fingerprint(py: Python<'_>, api: Option<&Bound<'_, PyDict>>) -> Option<String> {
    let config = py
        .import("meerschaum.config")
        .and_then(|m| m.getattr("config"))
        .ok()?;
    let section = |keys: &[&str]| -> String {
        let mut cur = config.clone();
        for key in keys {
            cur = match cur.get_item(*key) {
                Ok(next) => next,
                Err(_) => return String::new(),
            };
        }
        py_to_core(&cur).map(|v| v.py_str()).unwrap_or_default()
    };
    let root = py
        .import("meerschaum.config.paths")
        .and_then(|m| m.getattr("ROOT_DIR_PATH"))
        .and_then(|p| p.str())
        .map(|p| p.to_string())
        .unwrap_or_default();
    let env = |name: &str| std::env::var(name).unwrap_or_default();
    let parts = [
        std::process::id().to_string(),
        root,
        section(&["jobs"]),
        section(&["system", "cli"]),
        section(&["meerschaum", "executor"]),
        section(&["meerschaum", "connectors", "api"]),
        env("MRSM_BIN"),
        env(SUPERVISOR_ENV),
        env(meerschaum_jobs::supervisor::PROGRAM_ENV),
        env("PATH"),
        env("XDG_CONFIG_HOME"),
        env("HOME"),
        api.map(|a| a.to_string()).unwrap_or_default(),
    ];
    Some(parts.join("\u{1f}"))
}

fn build_ctx(py: Python<'_>, api_attributes: Option<&Bound<'_, PyDict>>) -> PyResult<Ctx> {
    static CACHE: Mutex<Option<(String, Ctx)>> = Mutex::new(None);
    let fingerprint = ctx_fingerprint(py, api_attributes);
    if let Some(fingerprint) = &fingerprint
        && let Some((cached, ctx)) = CACHE.lock().unwrap_or_else(|e| e.into_inner()).as_ref()
        && cached == fingerprint
    {
        return Ok(ctx.clone());
    }
    let config: Config = crate::config_api::live_config(py)?;
    let launcher = resolve_launcher(py)?;
    let mut section = CliRemoteResolver::section(&config);
    for (label, attrs) in api_overrides(api_attributes)? {
        let base = section.get(&label).map(Value::to_map).unwrap_or_default();
        let patched = meerschaum_core::value::apply_patch_maps(&base, &attrs.to_map());
        section.insert(label, Value::Map(patched));
    }
    let resolver = resolver_for(section);
    let jobs = JobsContext::new(
        JobPaths::from_config(&config),
        JobsConfig::from_config(&config),
        launcher,
    )
    .with_remote(resolver);
    let ctx = Ctx { jobs };
    if let Some(fingerprint) = fingerprint {
        *CACHE.lock().unwrap_or_else(|e| e.into_inner()) = Some((fingerprint, ctx.clone()));
    }
    Ok(ctx)
}

// ---------------------------------------------------------------------------------------------
// Conversions

fn seconds(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Seconds>> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else {
        return Ok(None);
    };
    if obj.is_instance_of::<PyInt>() {
        return Ok(Some(Seconds::Int(obj.extract()?)));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(Some(Seconds::Float(obj.extract()?)));
    }
    Err(PyValueError::new_err(format!(
        "Invalid number of seconds: {}",
        obj.repr()?
    )))
}

fn tuple<'py>(py: Python<'py>, t: &SuccessTuple) -> PyResult<Bound<'py, PyTuple>> {
    success_to_py(py, t)
}

fn iso(dt: Option<PyDateTime>) -> Option<String> {
    dt.map(|d| d.isoformat())
}

fn map_to_py<'py>(py: Python<'py>, map: Map) -> PyResult<Bound<'py, PyAny>> {
    core_to_py(py, &Value::Map(map))
}

fn string_map(obj: &Bound<'_, PyDict>) -> PyResult<IndexMap<String, String>> {
    let mut out = IndexMap::new();
    for (k, v) in obj.iter() {
        out.insert(k.str()?.extract()?, v.str()?.extract()?);
    }
    Ok(out)
}

fn status_str(status: Status) -> &'static str {
    status.as_str()
}

fn sysargs_arg(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Vec<String>>> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else {
        return Ok(None);
    };
    if let Ok(text) = obj.downcast::<PyString>() {
        return meerschaum_jobs::shlex::split(text.to_str()?)
            .map(Some)
            .map_err(|e| PyValueError::new_err(e.to_string()));
    }
    let mut out = Vec::new();
    for item in obj.try_iter()? {
        out.push(item?.str()?.extract()?);
    }
    Ok(Some(out))
}

// ---------------------------------------------------------------------------------------------
// Job

#[derive(Clone)]
struct Spec {
    name: String,
    sysargs: Option<Vec<String>>,
    opts: JobOptions,
}

fn new_job(spec: &Spec, ctx: &Ctx) -> PyResult<Job> {
    Job::new(
        &spec.name,
        spec.sysargs.clone(),
        spec.opts.clone(),
        &ctx.jobs,
    )
    .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Lock `mutex` without ever waiting for it while holding the interpreter lock. Another thread
/// may hold the mutex in a long operation (`start()`, a remote read) whose warnings take the
/// interpreter lock on a runtime thread: a caller blocked on the mutex with the GIL held would
/// deadlock with it. A contended lock is polled with the GIL released.
fn lock_without_the_gil<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    loop {
        match mutex.try_lock() {
            Ok(guard) => return guard,
            Err(std::sync::TryLockError::Poisoned(e)) => return e.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                Python::with_gil(|py| {
                    py.allow_threads(|| std::thread::sleep(Duration::from_millis(1)))
                });
            }
        }
    }
}

/// `meerschaum.jobs.Job`'s native half.
#[pyclass(frozen, name = "Job", module = "meerschaum._core")]
pub struct PyJob {
    inner: Mutex<Job>,
    spec: Spec,
    ctx: Ctx,
}

impl PyJob {
    fn lock(&self) -> std::sync::MutexGuard<'_, Job> {
        lock_without_the_gil(&self.inner)
    }
}

#[pymethods]
impl PyJob {
    #[new]
    #[pyo3(signature = (
        name, sysargs=None, env=None, executor_keys=None, delete_after_completion=false,
        refresh_seconds=None, properties=None, externally_managed=false, api_attributes=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        name: &str,
        sysargs: Option<&Bound<'_, PyAny>>,
        env: Option<&Bound<'_, PyDict>>,
        executor_keys: Option<String>,
        delete_after_completion: bool,
        refresh_seconds: Option<&Bound<'_, PyAny>>,
        properties: Option<&Bound<'_, PyAny>>,
        externally_managed: bool,
        api_attributes: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let ctx = build_ctx(py, api_attributes)?;
        let properties = match properties.filter(|p| !p.is_none()) {
            Some(p) => Some(py_to_core(p)?.to_map()),
            None => None,
        };
        let spec = Spec {
            name: name.to_string(),
            sysargs: sysargs_arg(sysargs)?,
            opts: JobOptions {
                env: match env {
                    Some(e) => Some(string_map(e)?),
                    None => None,
                },
                executor_keys,
                delete_after_completion,
                refresh_seconds: seconds(refresh_seconds)?,
                properties,
                externally_managed,
            },
        };
        let job = py.allow_threads(|| new_job(&spec, &ctx))?;
        Ok(PyJob {
            inner: Mutex::new(job),
            spec,
            ctx,
        })
    }

    #[getter]
    fn name(&self) -> String {
        self.lock().name.clone()
    }

    #[getter]
    fn executor_keys(&self) -> String {
        self.lock().executor_keys.clone()
    }

    /// The warnings the operations so far produced (drained): v4 emitted them with `warn()`.
    fn take_warnings(&self) -> Vec<String> {
        std::mem::take(&mut self.lock().warnings)
    }

    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let r = py.allow_threads(|| self.lock().start());
        tuple(py, &r)
    }

    #[pyo3(signature = (timeout_seconds=None))]
    fn stop<'py>(
        &self,
        py: Python<'py>,
        timeout_seconds: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let timeout = seconds(timeout_seconds)?;
        let r = py.allow_threads(|| self.lock().stop(timeout));
        tuple(py, &r)
    }

    #[pyo3(signature = (timeout_seconds=None))]
    fn pause<'py>(
        &self,
        py: Python<'py>,
        timeout_seconds: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let timeout = seconds(timeout_seconds)?;
        let r = py.allow_threads(|| self.lock().pause(timeout));
        tuple(py, &r)
    }

    #[pyo3(signature = (timeout_seconds=None))]
    fn resume<'py>(
        &self,
        py: Python<'py>,
        timeout_seconds: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let timeout = seconds(timeout_seconds)?;
        let r = py.allow_threads(|| self.lock().resume(timeout));
        tuple(py, &r)
    }

    /// Stop the job if it is running or paused, then start it.
    #[pyo3(signature = (timeout_seconds=None))]
    fn restart_now<'py>(
        &self,
        py: Python<'py>,
        timeout_seconds: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let timeout = seconds(timeout_seconds)?;
        let r = py.allow_threads(|| self.lock().restart(timeout));
        tuple(py, &r)
    }

    fn delete<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let r = py.allow_threads(|| self.lock().delete());
        tuple(py, &r)
    }

    fn is_running(&self, py: Python<'_>) -> bool {
        py.allow_threads(|| self.lock().is_running())
    }

    /// `exists()`; a failed remote read raises `RuntimeError` when `strict`.
    #[pyo3(signature = (strict=false))]
    fn exists(&self, py: Python<'_>, strict: bool) -> PyResult<bool> {
        py.allow_threads(|| {
            let mut job = self.lock();
            if strict {
                job.try_exists().map_err(runtime_err)
            } else {
                Ok(job.exists())
            }
        })
    }

    #[pyo3(signature = (strict=false))]
    fn get_logs(&self, py: Python<'_>, strict: bool) -> PyResult<Option<String>> {
        py.allow_threads(|| {
            let mut job = self.lock();
            if strict {
                job.try_get_logs().map_err(runtime_err)
            } else {
                Ok(job.get_logs())
            }
        })
    }

    #[getter]
    fn status(&self, py: Python<'_>) -> &'static str {
        py.allow_threads(|| status_str(self.lock().status()))
    }

    #[getter]
    fn pid(&self, py: Python<'_>) -> Option<i32> {
        py.allow_threads(|| self.lock().pid())
    }

    /// The `restart` property: whether a stopped job is started again by `check_restart`.
    #[getter]
    fn restarts(&self, py: Python<'_>) -> bool {
        py.allow_threads(|| self.lock().restarts())
    }

    #[getter]
    fn result<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let r = py.allow_threads(|| self.lock().result());
        tuple(py, &r)
    }

    #[getter]
    fn sysargs(&self, py: Python<'_>) -> Vec<String> {
        py.allow_threads(|| self.lock().sysargs())
    }

    fn get_daemon_properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let props = py.allow_threads(|| self.lock().get_daemon_properties());
        map_to_py(py, props)
    }

    /// The daemon's properties (a local job's `properties.json` merged with the in-memory ones;
    /// for other executors the executor's, patched with this job's).
    fn properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let props = py.allow_threads(|| self.lock().properties());
        map_to_py(py, props)
    }

    #[getter]
    fn began(&self, py: Python<'_>) -> Option<String> {
        py.allow_threads(|| iso(self.lock().began()))
    }

    #[getter]
    fn ended(&self, py: Python<'_>) -> Option<String> {
        py.allow_threads(|| iso(self.lock().ended()))
    }

    #[getter]
    fn paused(&self, py: Python<'_>) -> Option<String> {
        py.allow_threads(|| iso(self.lock().paused()))
    }

    #[getter]
    fn stop_time(&self, py: Python<'_>) -> Option<String> {
        py.allow_threads(|| iso(self.lock().stop_time()))
    }

    #[getter]
    fn hidden(&self, py: Python<'_>) -> bool {
        py.allow_threads(|| self.lock().hidden())
    }

    #[getter]
    fn is_externally_managed(&self, py: Python<'_>) -> bool {
        py.allow_threads(|| self.lock().is_externally_managed())
    }

    fn set_externally_managed(&self) {
        self.lock().set_externally_managed();
    }

    fn check_restart<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let r = py.allow_threads(|| self.lock().check_restart());
        tuple(py, &r)
    }

    #[getter]
    fn label(&self, py: Python<'_>) -> String {
        py.allow_threads(|| self.lock().label())
    }

    #[getter]
    fn env<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let env = py.allow_threads(|| self.lock().env());
        let out = PyDict::new(py);
        for (k, v) in env {
            out.set_item(k, v)?;
        }
        Ok(out)
    }

    #[getter]
    fn delete_after_completion(&self, py: Python<'_>) -> bool {
        py.allow_threads(|| self.lock().delete_after_completion())
    }

    #[pyo3(signature = (strict=false))]
    fn is_blocking_on_stdin(&self, py: Python<'_>, strict: bool) -> PyResult<bool> {
        let _ = strict;
        Ok(py.allow_threads(|| self.lock().is_blocking_on_stdin()))
    }

    fn get_prompt_kwargs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let kw = py.allow_threads(|| self.lock().get_prompt_kwargs());
        map_to_py(py, kw)
    }

    fn write_stdin(&self, py: Python<'_>, data: &str) -> PyResult<()> {
        py.allow_threads(|| self.lock().write_stdin(data))
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    /// The files of the job's daemon (local jobs; the paths exist for other executors too).
    fn paths<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let mut job = self.lock();
        let paths = job.daemon().paths().clone();
        let log = job.daemon().log_path();
        daemon_paths_dict(py, &paths, log)
    }

    /// `Daemon.read_stop_file()` for a local job.
    fn read_stop_file<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let data = self.lock().daemon().read_stop_file();
        match data {
            Some(m) => map_to_py(py, m),
            None => Ok(py.None().into_bound(py)),
        }
    }

    /// The text of `Job.__str__`.
    fn describe(&self) -> String {
        self.lock().to_string()
    }

    /// Follow the job's logs. `callback(line)` (and the optional callables) run on a helper
    /// thread with the GIL; this call returns when monitoring ends. A `stop_exception` raised by
    /// a callback ends it quietly, any other `Exception` is reported with `warn` and monitoring
    /// goes on, a `BaseException` (`KeyboardInterrupt`) ends it and is raised here.
    #[pyo3(signature = (
        callback, input_callback=None, stop_callback=None, stop_event=None, stop_flag=None,
        stop_on_exit=false, strip_timestamps=false, accept_input=true, wait_if_stopped=true,
        stop_exception=None, clear_callback=None, warn=None, poll_seconds=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn monitor_logs(
        &self,
        py: Python<'_>,
        callback: PyObject,
        input_callback: Option<PyObject>,
        stop_callback: Option<PyObject>,
        stop_event: Option<PyObject>,
        stop_flag: Option<&PyStopFlag>,
        stop_on_exit: bool,
        strip_timestamps: bool,
        accept_input: bool,
        wait_if_stopped: bool,
        stop_exception: Option<PyObject>,
        clear_callback: Option<PyObject>,
        warn: Option<PyObject>,
        poll_seconds: Option<f64>,
    ) -> PyResult<()> {
        let spec = self.spec.clone();
        let ctx = self.ctx.clone();
        let flag = stop_flag
            .map(|f| f.flag.clone())
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        let fatal: Arc<Mutex<Option<PyErr>>> = Arc::new(Mutex::new(None));
        let (tx, rx) = channel::<Result<SuccessTuple, String>>();
        let worker_flag = flag.clone();
        let worker_fatal = fatal.clone();
        std::thread::spawn(move || {
            let outcome = (|| -> Result<SuccessTuple, String> {
                let mut job = Job::new(
                    &spec.name,
                    spec.sysargs.clone(),
                    spec.opts.clone(),
                    &ctx.jobs,
                )
                .map_err(|e| e.to_string())?;
                let report = |py: Python<'_>, err: PyErr| -> bool {
                    // Returns whether monitoring stops.
                    if let Some(cls) = &stop_exception
                        && err.matches(py, cls.bind(py)).unwrap_or(false)
                    {
                        return true;
                    }
                    if err.is_instance_of::<PyException>(py) {
                        let text = traceback_text(py, &err);
                        if let Some(warn) = &warn {
                            let _ = warn.call1(py, (format!("Error in logs callback:\n{text}"),));
                        } else {
                            eprintln!("Error in logs callback:\n{text}");
                        }
                        return false;
                    }
                    *worker_fatal.lock().unwrap_or_else(|e| e.into_inner()) = Some(err);
                    true
                };
                let line_cb = |line: &str| -> MonitorControl {
                    Python::with_gil(|py| match callback.call1(py, (line,)) {
                        Ok(_) => MonitorControl::Continue,
                        Err(e) => {
                            if report(py, e) {
                                MonitorControl::Stop
                            } else {
                                MonitorControl::Continue
                            }
                        }
                    })
                };
                let opts = MonitorOptions {
                    stop_on_exit,
                    strip_timestamps,
                    accept_input,
                    stop: Some(worker_flag.clone()),
                    input: input_callback.as_ref().map(|cb| {
                        Box::new(move || -> Option<String> {
                            Python::with_gil(|py| match cb.call0(py) {
                                Ok(v) => v.extract::<String>(py).ok(),
                                Err(e) => {
                                    // KeyboardInterrupt while waiting for input ends the wait.
                                    let _ = report(py, e);
                                    None
                                }
                            })
                        }) as Box<dyn FnMut() -> Option<String>>
                    }),
                    on_stop: stop_callback.as_ref().map(|cb| {
                        Box::new(move |result: &SuccessTuple| {
                            Python::with_gil(|py| {
                                let t = match success_to_py(py, result) {
                                    Ok(t) => t,
                                    Err(_) => return,
                                };
                                if let Err(e) = cb.call1(py, (t,)) {
                                    let _ = report(py, e);
                                }
                            })
                        }) as Box<dyn FnMut(&SuccessTuple)>
                    }),
                    on_clear: clear_callback.as_ref().map(|cb| {
                        Box::new(move || {
                            Python::with_gil(|py| {
                                let _ = cb.call0(py);
                            })
                        }) as Box<dyn FnMut()>
                    }),
                    poll_interval: poll_seconds.map(Duration::from_secs_f64),
                    wait_if_stopped,
                };
                Ok(job.monitor_logs(line_cb, opts))
            })();
            let _ = tx.send(outcome);
        });
        let rx = Mutex::new(rx);
        let wait = |timeout: Duration| {
            py.allow_threads(|| {
                rx.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .recv_timeout(timeout)
            })
        };
        let outcome = loop {
            let received = wait(Duration::from_millis(50));
            match received {
                Ok(outcome) => break outcome,
                Err(RecvTimeoutError::Timeout) => {
                    if let Err(e) = py.check_signals() {
                        flag.store(true, Ordering::SeqCst);
                        let _ = wait(Duration::from_secs(5));
                        return Err(e);
                    }
                    if let Some(event) = &stop_event
                        && event
                            .call_method0(py, "is_set")
                            .and_then(|v| v.is_truthy(py))
                            .unwrap_or(false)
                    {
                        flag.store(true, Ordering::SeqCst);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(runtime_err("The log monitor stopped unexpectedly."));
                }
            }
        };
        if let Some(err) = fatal.lock().unwrap_or_else(|e| e.into_inner()).take() {
            return Err(err);
        }
        match outcome {
            Ok(result) if result.success => Ok(()),
            Ok(result) => {
                // A monitor which could not start (no such job, no log) reports why.
                if result.message.is_empty() {
                    Ok(())
                } else {
                    Err(runtime_err(result.message))
                }
            }
            Err(e) => Err(PyValueError::new_err(e)),
        }
    }

    fn __repr__(&self) -> String {
        self.lock().to_string()
    }
}

fn traceback_text(py: Python<'_>, err: &PyErr) -> String {
    py.import("traceback")
        .and_then(|tb| {
            let value = err.value(py);
            tb.call_method1(
                "format_exception",
                (value.get_type(), value, err.traceback(py)),
            )
        })
        .and_then(|lines| lines.extract::<Vec<String>>())
        .map(|lines| lines.concat())
        .unwrap_or_else(|_| err.to_string())
}

/// A flag a caller raises to end a running `monitor_logs` (asyncio cancellation).
#[pyclass(frozen, name = "StopFlag", module = "meerschaum._core")]
pub struct PyStopFlag {
    flag: Arc<AtomicBool>,
}

#[pymethods]
impl PyStopFlag {
    #[new]
    fn new() -> Self {
        PyStopFlag {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }

    fn is_set(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }
}

fn daemon_paths_dict<'py>(
    py: Python<'py>,
    paths: &meerschaum_jobs::config::DaemonPaths,
    log: PathBuf,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    let text = |p: &PathBuf| p.to_string_lossy().to_string();
    out.set_item("dir", text(&paths.dir))?;
    out.set_item("properties", text(&paths.properties))?;
    out.set_item("pid", text(&paths.pid))?;
    out.set_item("pickle", text(&paths.pickle))?;
    out.set_item("stdin", text(&paths.stdin))?;
    out.set_item("stdin_block", text(&paths.stdin_block))?;
    out.set_item("stop", text(&paths.stop))?;
    out.set_item("prompt_kwargs", text(&paths.prompt_kwargs))?;
    out.set_item("externally_managed", text(&paths.externally_managed))?;
    out.set_item("log", text(&log))?;
    out.set_item("log_offset", text(&paths.log_offset))?;
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// Daemon (the read / control façade)

/// `meerschaum.utils.daemon.Daemon`'s native half: a job's directory.
#[pyclass(frozen, name = "Daemon", module = "meerschaum._core")]
pub struct PyDaemon {
    inner: Mutex<Daemon>,
}

impl PyDaemon {
    fn lock(&self) -> std::sync::MutexGuard<'_, Daemon> {
        lock_without_the_gil(&self.inner)
    }
}

#[pymethods]
impl PyDaemon {
    /// `Daemon(daemon_id=...)` of an existing daemon, or a new one when `sysargs` is given.
    #[new]
    #[pyo3(signature = (daemon_id, sysargs=None, label=None, properties=None))]
    fn new(
        py: Python<'_>,
        daemon_id: &str,
        sysargs: Option<&Bound<'_, PyAny>>,
        label: Option<String>,
        properties: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let ctx = build_ctx(py, None)?;
        if let Some(reason) = meerschaum_jobs::names::invalid_job_name_reason(daemon_id) {
            return Err(PyValueError::new_err(format!("Invalid name: {reason}.")));
        }
        let daemon = match sysargs_arg(sysargs)? {
            Some(sysargs) => {
                let props = match properties.filter(|p| !p.is_none()) {
                    Some(p) => py_to_core(p)?.to_map(),
                    None => Map::new(),
                };
                Daemon::new(daemon_id, sysargs, label, props, &ctx.jobs)
            }
            None => Daemon::open(daemon_id, &ctx.jobs)
                .map_err(|e| PyException::new_err(e.to_string()))?,
        };
        Ok(PyDaemon {
            inner: Mutex::new(daemon),
        })
    }

    #[getter]
    fn daemon_id(&self) -> String {
        self.lock().daemon_id.clone()
    }

    #[getter]
    fn label(&self) -> String {
        self.lock().label().to_string()
    }

    #[getter]
    fn sysargs(&self) -> Vec<String> {
        self.lock().sysargs().to_vec()
    }

    #[getter]
    fn hidden(&self) -> bool {
        self.lock().hidden()
    }

    #[getter]
    fn status(&self, py: Python<'_>) -> &'static str {
        py.allow_threads(|| status_str(self.lock().status()))
    }

    #[getter]
    fn pid(&self, py: Python<'_>) -> Option<i32> {
        py.allow_threads(|| self.lock().pid())
    }

    /// `process`: the PID if the process exists.
    #[getter]
    fn process(&self, py: Python<'_>) -> Option<i32> {
        py.allow_threads(|| self.lock().process())
    }

    fn properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let props = py.allow_threads(|| self.lock().properties());
        map_to_py(py, props)
    }

    fn read_properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self.lock().read_properties() {
            Some(m) => map_to_py(py, m),
            None => Ok(py.None().into_bound(py)),
        }
    }

    fn update_properties(&self, patch: &Bound<'_, PyAny>) -> PyResult<()> {
        let patch = py_to_core(patch)?.to_map();
        self.lock().update_properties(&patch);
        Ok(())
    }

    fn pop_property<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Bound<'py, PyAny>> {
        match self.lock().pop_property(key) {
            Some(v) => core_to_py(py, &v),
            None => Ok(py.None().into_bound(py)),
        }
    }

    fn write_properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let r = self.lock().write_properties();
        tuple(py, &r)
    }

    fn paths<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let mut d = self.lock();
        let paths = d.paths().clone();
        let log = d.log_path();
        daemon_paths_dict(py, &paths, log)
    }

    fn log_text(&self, py: Python<'_>) -> Option<String> {
        py.allow_threads(|| self.lock().log_text())
    }

    fn readlines(&self, py: Python<'_>) -> Vec<String> {
        py.allow_threads(|| self.lock().readlines())
    }

    fn read_stop_file<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self.lock().read_stop_file() {
            Some(m) => map_to_py(py, m),
            None => Ok(py.None().into_bound(py)),
        }
    }

    fn write_stop_file<'py>(&self, py: Python<'py>, action: &str) -> PyResult<Bound<'py, PyTuple>> {
        tuple(py, &self.lock().write_stop_file(action))
    }

    fn remove_stop_file<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        tuple(py, &self.lock().remove_stop_file())
    }

    fn mkdir_if_not_exists<'py>(
        &self,
        py: Python<'py>,
        allow_dirty_run: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        tuple(py, &self.lock().mkdir_if_not_exists(allow_dirty_run))
    }

    #[pyo3(signature = (keep_daemon_output=true, allow_dirty_run=false))]
    fn run<'py>(
        &self,
        py: Python<'py>,
        keep_daemon_output: bool,
        allow_dirty_run: bool,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let r = py.allow_threads(|| self.lock().run(keep_daemon_output, allow_dirty_run));
        tuple(py, &r)
    }

    #[pyo3(signature = (timeout=None))]
    fn kill<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let timeout = seconds(timeout)?;
        let r = py.allow_threads(|| self.lock().kill(timeout));
        tuple(py, &r)
    }

    #[pyo3(signature = (timeout=None))]
    fn quit<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let timeout = seconds(timeout)?;
        let r = py.allow_threads(|| self.lock().quit(timeout));
        tuple(py, &r)
    }

    #[pyo3(signature = (timeout=None, check_timeout_interval=None))]
    fn pause<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<&Bound<'py, PyAny>>,
        check_timeout_interval: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let timeout = seconds(timeout)?;
        let interval = seconds(check_timeout_interval)?;
        let r = py.allow_threads(|| self.lock().pause(timeout, interval));
        tuple(py, &r)
    }

    #[pyo3(signature = (timeout=None, check_timeout_interval=None))]
    fn resume<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<&Bound<'py, PyAny>>,
        check_timeout_interval: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let timeout = seconds(timeout)?;
        let interval = seconds(check_timeout_interval)?;
        let r = py.allow_threads(|| self.lock().resume(timeout, interval));
        tuple(py, &r)
    }

    #[pyo3(signature = (keep_logs=false))]
    fn cleanup<'py>(&self, py: Python<'py>, keep_logs: bool) -> PyResult<Bound<'py, PyTuple>> {
        let r = py.allow_threads(|| self.lock().cleanup(keep_logs));
        tuple(py, &r)
    }

    #[pyo3(signature = (timeout=None))]
    fn get_timeout_seconds(
        &self,
        py: Python<'_>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        let t = self.lock().get_timeout_seconds(seconds(timeout)?);
        seconds_to_py(py, t)
    }

    #[pyo3(signature = (interval=None))]
    fn get_check_timeout_interval_seconds(
        &self,
        py: Python<'_>,
        interval: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        let t = self
            .lock()
            .get_check_timeout_interval_seconds(seconds(interval)?);
        seconds_to_py(py, t)
    }
}

fn seconds_to_py(py: Python<'_>, s: Seconds) -> PyResult<PyObject> {
    Ok(match s {
        Seconds::Int(i) => i.into_pyobject(py)?.into_any().unbind(),
        Seconds::Float(f) => f.into_pyobject(py)?.into_any().unbind(),
    })
}

// ---------------------------------------------------------------------------------------------
// Executors

/// `Executor` / `SystemdExecutor`'s native half: the operations by job name over `local`,
/// `systemd` or `api:<label>`.
#[pyclass(frozen, name = "Executor", module = "meerschaum._core")]
pub struct PyExecutor {
    keys: String,
    ctx: Ctx,
    warnings: Mutex<Vec<String>>,
}

impl PyExecutor {
    fn with<R>(&self, f: impl FnOnce(&dyn JobExecutor) -> R) -> R {
        let executor = make_executor(&self.keys, &self.ctx.jobs);
        let out = f(executor.as_ref());
        self.warnings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(executor.take_warnings());
        out
    }

    fn systemd(&self) -> SystemdExecutor {
        SystemdExecutor::new(self.ctx.jobs.clone())
    }
}

#[pymethods]
impl PyExecutor {
    #[new]
    #[pyo3(signature = (executor_keys, api_attributes=None))]
    fn new(
        py: Python<'_>,
        executor_keys: &str,
        api_attributes: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        Ok(PyExecutor {
            keys: executor_keys.to_string(),
            ctx: build_ctx(py, api_attributes)?,
            warnings: Mutex::new(Vec::new()),
        })
    }

    #[getter]
    fn executor_keys(&self) -> String {
        self.keys.clone()
    }

    fn take_warnings(&self) -> Vec<String> {
        std::mem::take(&mut self.warnings.lock().unwrap_or_else(|e| e.into_inner()))
    }

    #[pyo3(signature = (strict=false))]
    fn get_job_names(&self, py: Python<'_>, strict: bool) -> PyResult<Vec<String>> {
        py.allow_threads(|| {
            self.with(|ex| {
                if strict {
                    ex.try_get_job_names().map_err(runtime_err)
                } else {
                    Ok(ex.get_job_names())
                }
            })
        })
    }

    #[pyo3(signature = (name, strict=false))]
    fn get_job_exists(&self, py: Python<'_>, name: &str, strict: bool) -> PyResult<bool> {
        py.allow_threads(|| {
            self.with(|ex| {
                if strict {
                    ex.try_get_job_exists(name).map_err(runtime_err)
                } else {
                    Ok(ex.get_job_exists(name))
                }
            })
        })
    }

    fn get_job_metadata<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
        let meta = py.allow_threads(|| self.with(|ex| ex.get_job_metadata(name)));
        map_to_py(py, meta)
    }

    fn get_job_properties<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
        let props = py.allow_threads(|| self.with(|ex| ex.get_job_properties(name)));
        map_to_py(py, props)
    }

    fn get_job_status(&self, py: Python<'_>, name: &str) -> String {
        py.allow_threads(|| self.with(|ex| ex.get_job_status(name)))
    }

    fn get_job_began(&self, py: Python<'_>, name: &str) -> Option<String> {
        py.allow_threads(|| self.with(|ex| ex.get_job_began(name)))
    }

    fn get_job_ended(&self, py: Python<'_>, name: &str) -> Option<String> {
        py.allow_threads(|| self.with(|ex| ex.get_job_ended(name)))
    }

    fn get_job_paused(&self, py: Python<'_>, name: &str) -> Option<String> {
        py.allow_threads(|| self.with(|ex| ex.get_job_paused(name)))
    }

    fn get_job_pid(&self, py: Python<'_>, name: &str) -> Option<i32> {
        py.allow_threads(|| self.with(|ex| ex.get_job_pid(name)))
    }

    fn get_job_result<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
        match py.allow_threads(|| self.with(|ex| ex.get_job_result(name))) {
            Some(t) => Ok(tuple(py, &t)?.into_any()),
            None => Ok(py.None().into_bound(py)),
        }
    }

    #[pyo3(signature = (name, sysargs, properties=None))]
    fn create_job<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        sysargs: Vec<String>,
        properties: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let props = match properties.filter(|p| !p.is_none()) {
            Some(p) => Some(py_to_core(p)?.to_map()),
            None => None,
        };
        let r = py.allow_threads(|| self.with(|ex| ex.create_job(name, &sysargs, props.as_ref())));
        tuple(py, &r)
    }

    fn start_job<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyTuple>> {
        let r = py.allow_threads(|| self.with(|ex| ex.start_job(name)));
        tuple(py, &r)
    }

    fn stop_job<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyTuple>> {
        let r = py.allow_threads(|| self.with(|ex| ex.stop_job(name)));
        tuple(py, &r)
    }

    fn pause_job<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyTuple>> {
        let r = py.allow_threads(|| self.with(|ex| ex.pause_job(name)));
        tuple(py, &r)
    }

    fn delete_job<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyTuple>> {
        let r = py.allow_threads(|| self.with(|ex| ex.delete_job(name)));
        tuple(py, &r)
    }

    #[pyo3(signature = (name, strict=false))]
    fn get_logs(&self, py: Python<'_>, name: &str, strict: bool) -> PyResult<Option<String>> {
        py.allow_threads(|| {
            self.with(|ex| {
                if strict {
                    ex.try_get_logs(name).map_err(runtime_err)
                } else {
                    Ok(ex.get_logs(name))
                }
            })
        })
    }

    fn get_job_stop_time(&self, py: Python<'_>, name: &str) -> Option<String> {
        py.allow_threads(|| self.with(|ex| iso(ex.get_job_stop_time(name))))
    }

    fn get_job_is_blocking_on_stdin(&self, py: Python<'_>, name: &str) -> bool {
        py.allow_threads(|| self.with(|ex| ex.get_job_is_blocking_on_stdin(name)))
    }

    fn get_job_prompt_kwargs<'py>(
        &self,
        py: Python<'py>,
        name: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let kw = py.allow_threads(|| self.with(|ex| ex.get_job_prompt_kwargs(name)));
        map_to_py(py, kw)
    }

    // The `systemd` extras (`SystemdExecutor`).

    fn get_service_name(&self, name: &str) -> String {
        self.systemd().get_service_name(name)
    }

    fn get_service_job_path(&self, name: &str) -> String {
        path_text(self.systemd().get_service_job_path(name))
    }

    fn get_service_symlink_file_path(&self, name: &str) -> String {
        path_text(self.systemd().get_service_symlink_file_path(name))
    }

    fn get_service_file_path(&self, name: &str) -> String {
        path_text(self.systemd().get_service_file_path(name))
    }

    fn get_service_logs_path(&self, name: &str) -> String {
        path_text(self.systemd().get_service_logs_path(name))
    }

    fn get_socket_path(&self, name: &str) -> String {
        path_text(self.systemd().get_socket_path(name))
    }

    fn get_result_path(&self, name: &str) -> String {
        path_text(self.systemd().get_result_path(name))
    }

    /// The program `ExecStart=` runs before ` -m meerschaum <sysargs>`.
    fn exec_program(&self) -> String {
        self.systemd().exec_program()
    }

    #[pyo3(signature = (name, sysargs, python=None))]
    fn get_service_file_text(
        &self,
        name: &str,
        sysargs: Vec<String>,
        python: Option<String>,
    ) -> String {
        let ex = self.systemd();
        let python = python.unwrap_or_else(|| ex.exec_program());
        let env: Vec<(String, String)> = meerschaum_cli::dispatch::process_vars();
        ex.get_service_file_text(name, &sysargs, &python, &env)
    }

    fn get_socket_file_text(&self, name: &str) -> String {
        self.systemd().get_socket_file_text(name)
    }

    fn get_job_sysargs(&self, name: &str) -> Vec<String> {
        self.systemd().get_job_sysargs(name)
    }

    fn get_job_restart(&self, name: &str) -> bool {
        self.systemd().get_job_restart(name)
    }

    /// `systemctl --user <args>`: a `SuccessTuple`, or the stripped output with `as_output`.
    #[pyo3(signature = (args, as_output=false))]
    fn run_command<'py>(
        &self,
        py: Python<'py>,
        args: Vec<String>,
        as_output: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let ex = self.systemd();
        if as_output {
            let out = py.allow_threads(|| ex.run_command_output(&refs));
            return Ok(PyString::new(py, &out).into_any());
        }
        let r = py.allow_threads(|| ex.run_command(&refs));
        Ok(tuple(py, &r)?.into_any())
    }
}

fn path_text(path: PathBuf) -> String {
    path.to_string_lossy().to_string()
}

// ---------------------------------------------------------------------------------------------
// Listings and module functions

fn listing_rows<'py>(
    py: Python<'py>,
    mut jobs: IndexMap<String, Job>,
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for (name, job) in jobs.iter_mut() {
        let row = PyDict::new(py);
        row.set_item("name", name)?;
        row.set_item("executor_keys", job.executor_keys.clone())?;
        row.set_item("sysargs", job.sysargs())?;
        out.append(row)?;
    }
    Ok(out)
}

/// `get_jobs(...)`: `(jobs, warnings)`; each job is `{name, executor_keys, sysargs}`. A failed
/// read of a remote or `systemd` listing raises `RuntimeError` (v4 answered `{}`).
#[pyfunction]
#[pyo3(signature = (executor_keys=None, include_hidden=false, combine_local_and_systemd=true, api_attributes=None))]
pub fn jobs_get_jobs<'py>(
    py: Python<'py>,
    executor_keys: Option<String>,
    include_hidden: bool,
    combine_local_and_systemd: bool,
    api_attributes: Option<&Bound<'py, PyDict>>,
) -> PyResult<(Bound<'py, PyList>, Vec<String>)> {
    let ctx = build_ctx(py, api_attributes)?;
    let listing = py
        .allow_threads(|| {
            executor::get_jobs_for(
                &ctx.jobs,
                executor_keys.as_deref(),
                include_hidden,
                combine_local_and_systemd,
            )
        })
        .map_err(runtime_err)?;
    Ok((listing_rows(py, listing.jobs)?, listing.warnings))
}

/// `get_filtered_jobs(...)`: `(jobs, listing_warnings, missing)`.
#[pyfunction]
#[pyo3(signature = (executor_keys=None, filter_list=None, include_hidden=false, combine_local_and_systemd=true, api_attributes=None))]
pub fn jobs_get_filtered_jobs<'py>(
    py: Python<'py>,
    executor_keys: Option<String>,
    filter_list: Option<Vec<String>>,
    include_hidden: bool,
    combine_local_and_systemd: bool,
    api_attributes: Option<&Bound<'py, PyDict>>,
) -> PyResult<(Bound<'py, PyList>, Vec<String>, Vec<String>)> {
    let ctx = build_ctx(py, api_attributes)?;
    let filter = filter_list.unwrap_or_default();
    let filtered = py
        .allow_threads(|| {
            executor::get_filtered_jobs_for(
                &ctx.jobs,
                executor_keys.as_deref(),
                &filter,
                include_hidden,
                combine_local_and_systemd,
            )
        })
        .map_err(runtime_err)?;
    Ok((
        listing_rows(py, filtered.jobs)?,
        filtered.listing_warnings,
        filtered.missing,
    ))
}

/// `get_executor_keys_from_context()`.
#[pyfunction]
pub fn jobs_executor_keys_from_context(py: Python<'_>) -> PyResult<String> {
    let ctx = build_ctx(py, None)?;
    Ok(meerschaum_jobs::job::get_executor_keys_from_context(
        &ctx.jobs,
    ))
}

/// `get_daemon_ids()`.
#[pyfunction]
pub fn jobs_daemon_ids(py: Python<'_>) -> PyResult<Vec<String>> {
    let ctx = build_ctx(py, None)?;
    Ok(meerschaum_jobs::daemon::get_daemon_ids(&ctx.jobs.paths))
}

/// `get_new_daemon_name()`.
#[pyfunction]
pub fn jobs_new_daemon_name(py: Python<'_>) -> PyResult<String> {
    let ctx = build_ctx(py, None)?;
    Ok(meerschaum_jobs::names::get_new_daemon_name(&ctx.jobs.paths))
}

/// How many times this process has enumerated the process table looking for a job.
#[pyfunction]
pub fn jobs_cmdline_scans() -> usize {
    meerschaum_jobs::process::cmdline_scans()
}

/// The program that runs jobs, `(supervisor, program)` (diagnostics and tests).
#[pyfunction]
pub fn jobs_launcher(py: Python<'_>) -> PyResult<(Vec<String>, Vec<String>)> {
    let launcher = resolve_launcher(py)?;
    Ok((launcher.supervisor, launcher.program))
}

// ---------------------------------------------------------------------------------------------
// RotatingFile and StdinFile

/// A rotating log (`meerschaum.utils.daemon.RotatingFile`).
#[pyclass(name = "RotatingFile", module = "meerschaum._core")]
pub struct PyRotatingFile {
    inner: Mutex<RotatingFile>,
}

impl PyRotatingFile {
    fn lock(&self) -> std::sync::MutexGuard<'_, RotatingFile> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[pymethods]
impl PyRotatingFile {
    #[new]
    #[pyo3(signature = (file_path, num_files_to_keep=5, max_file_size=100000, write_timestamps=false, timestamp_format="%Y-%m-%d %H:%M".to_string()))]
    fn new(
        file_path: &str,
        num_files_to_keep: i64,
        max_file_size: i64,
        write_timestamps: bool,
        timestamp_format: String,
    ) -> PyResult<Self> {
        let file = RotatingFile::new(
            file_path,
            RotatingFileOptions {
                num_files_to_keep,
                max_file_size,
                write_timestamps,
                timestamp_format,
            },
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRotatingFile {
            inner: Mutex::new(file),
        })
    }

    #[getter]
    fn file_path(&self) -> String {
        path_text(self.lock().file_path().to_path_buf())
    }

    fn read(&self) -> String {
        self.lock().read()
    }

    fn readlines(&self) -> Vec<String> {
        self.lock().readlines()
    }

    fn write(&self, data: &str) -> PyResult<usize> {
        self.lock()
            .write(data)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(data.len())
    }

    #[pyo3(signature = (unused_only=false))]
    fn delete(&self, unused_only: bool) {
        self.lock().delete(unused_only);
    }

    fn close(&self) {
        self.lock().close();
    }

    fn touch(&self) -> PyResult<()> {
        self.lock()
            .touch()
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn seek(&self, position: u64) {
        self.lock().seek(position);
    }

    fn refresh_cursor(&self) {
        self.lock().refresh_cursor();
    }

    fn get_latest_subfile_index(&self) -> i64 {
        self.lock().get_latest_subfile_index()
    }

    fn get_latest_subfile_path(&self) -> String {
        path_text(self.lock().get_latest_subfile_path())
    }

    fn get_existing_subfile_indices(&self) -> Vec<i64> {
        self.lock().get_existing_subfile_indices()
    }

    fn get_existing_subfile_paths(&self) -> Vec<String> {
        self.lock()
            .get_existing_subfile_paths()
            .into_iter()
            .map(path_text)
            .collect()
    }

    fn get_subfile_path_from_index(&self, index: i64) -> String {
        path_text(self.lock().get_subfile_path_from_index(index))
    }

    fn get_subfile_name_from_index(&self, index: i64) -> String {
        self.lock().get_subfile_name_from_index(index)
    }

    fn get_index_from_subfile_name(&self, name: &str) -> i64 {
        self.lock().get_index_from_subfile_name(name)
    }

    fn get_timestamp_prefix_str(&self) -> String {
        self.lock().get_timestamp_prefix_str()
    }
}

/// A job's stdin FIFO (`meerschaum.utils.daemon.StdinFile`).
#[pyclass(name = "StdinFile", module = "meerschaum._core")]
pub struct PyStdinFile {
    inner: Mutex<StdinFile>,
}

impl PyStdinFile {
    fn lock(&self) -> std::sync::MutexGuard<'_, StdinFile> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[pymethods]
impl PyStdinFile {
    #[new]
    #[pyo3(signature = (file_path, lock_file_path=None))]
    fn new(file_path: &str, lock_file_path: Option<&str>) -> Self {
        PyStdinFile {
            inner: Mutex::new(StdinFile::new(file_path, lock_file_path.map(PathBuf::from))),
        }
    }

    #[getter]
    fn file_path(&self) -> String {
        path_text(self.lock().file_path.clone())
    }

    #[getter]
    fn blocking_file_path(&self) -> String {
        path_text(self.lock().blocking_file_path.clone())
    }

    fn write(&self, py: Python<'_>, data: &str) -> PyResult<usize> {
        py.allow_threads(|| self.lock().write(data.as_bytes()))
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(data.len())
    }

    fn readline(&self, py: Python<'_>) -> PyResult<String> {
        py.allow_threads(|| self.lock().readline())
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn close(&self) {
        self.lock().close();
    }

    fn is_open(&self) -> bool {
        self.lock().is_open()
    }
}

// ---------------------------------------------------------------------------------------------

/// Register the jobs bindings.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyJob>()?;
    m.add_class::<PyDaemon>()?;
    m.add_class::<PyExecutor>()?;
    m.add_class::<PyStopFlag>()?;
    m.add_class::<PyRotatingFile>()?;
    m.add_class::<PyStdinFile>()?;
    m.add_function(wrap_pyfunction!(jobs_get_jobs, m)?)?;
    m.add_function(wrap_pyfunction!(jobs_get_filtered_jobs, m)?)?;
    m.add_function(wrap_pyfunction!(jobs_executor_keys_from_context, m)?)?;
    m.add_function(wrap_pyfunction!(jobs_daemon_ids, m)?)?;
    m.add_function(wrap_pyfunction!(jobs_new_daemon_name, m)?)?;
    m.add_function(wrap_pyfunction!(jobs_cmdline_scans, m)?)?;
    m.add_function(wrap_pyfunction!(jobs_launcher, m)?)?;
    Ok(())
}
