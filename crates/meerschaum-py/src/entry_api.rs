//! The native action dispatcher from Python (design §4.4, D14): `_core.entry`,
//! `_core.run_action` and `_core.core_actions`.
//!
//! `mrsm.entry([...])`, `meerschaum.actions.actions['sync'](['pipes'], ...)` and every façade
//! that used to run the v4 action modules call these. They run the CLI's own dispatcher
//! (`meerschaum_cli::dispatch`) in the Python process on the façade session
//! ([`crate::facade::worker_session`]): output goes to `sys.stdout` / `sys.stderr` (so `capsys`
//! and redirected streams see it), the config is a snapshot of the live Python config
//! ([`crate::facade::publish_live_config`]), and Python plugin actions run in the worker the
//! session starts (the interpreter running this extension unless `MRSM_PYTHON` is set).

use meerschaum_cli::actions::{ActionDef, CORE_ACTIONS};
use meerschaum_cli::dispatch::{
    self, CommandOutput, CommandSpec, Host, Runtime, SuccessTuple, SystemHost,
};
use meerschaum_cli::value::{ArgsMap, PyTimeDelta, PyValue};
use pyo3::exceptions::{PyEOFError, PyKeyboardInterrupt, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyBytes, PyDict, PyFloat, PyFrozenSet, PyInt, PyList, PySet, PyString, PyTuple,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

use crate::pipe_bridge::success_to_py;
use crate::sql_api::PySqlConnector;
use crate::verify_api::PyStream;

/// A Python value as the dispatcher's argument value. Types the argument parser never produces
/// (arbitrary objects) become their `str()`.
fn to_py_value(obj: &Bound<'_, PyAny>) -> PyResult<PyValue> {
    if obj.is_none() {
        return Ok(PyValue::None);
    }
    if let Ok(b) = obj.downcast::<PyBool>() {
        return Ok(PyValue::Bool(b.is_true()));
    }
    if obj.is_instance_of::<PyInt>() {
        return Ok(match obj.extract::<i128>() {
            Ok(i) => PyValue::Int(i),
            Err(_) => PyValue::BigInt(obj.str()?.to_string()),
        });
    }
    if let Ok(f) = obj.downcast::<PyFloat>() {
        return Ok(PyValue::Float(f.value()));
    }
    if let Ok(s) = obj.downcast::<PyString>() {
        return Ok(PyValue::Str(s.to_string_lossy().into_owned()));
    }
    if let Ok(b) = obj.downcast::<PyBytes>() {
        return Ok(PyValue::Bytes(b.as_bytes().to_vec()));
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        return Ok(PyValue::List(
            list.iter()
                .map(|item| to_py_value(&item))
                .collect::<PyResult<_>>()?,
        ));
    }
    if let Ok(tuple) = obj.downcast::<PyTuple>() {
        return Ok(PyValue::Tuple(
            tuple
                .iter()
                .map(|item| to_py_value(&item))
                .collect::<PyResult<_>>()?,
        ));
    }
    if obj.is_instance_of::<PySet>() || obj.is_instance_of::<PyFrozenSet>() {
        return Ok(PyValue::Set(
            obj.try_iter()?
                .map(|item| to_py_value(&item?))
                .collect::<PyResult<_>>()?,
        ));
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        return Ok(PyValue::Dict(
            dict.iter()
                .map(|(k, v)| Ok((to_py_value(&k)?, to_py_value(&v)?)))
                .collect::<PyResult<_>>()?,
        ));
    }
    let py = obj.py();
    let datetime = py.import("datetime")?;
    if obj.is_instance(&datetime.getattr("datetime")?)? {
        let text: String = obj.call_method0("isoformat")?.extract()?;
        let parsed = chrono::DateTime::parse_from_rfc3339(&text)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%dT%H:%M:%S%.f")
                    .map(|dt| dt.and_utc())
            });
        return Ok(match parsed {
            Ok(dt) => PyValue::DateTime(dt),
            Err(_) => PyValue::Str(text),
        });
    }
    if obj.is_instance(&datetime.getattr("timedelta")?)? {
        return Ok(PyValue::TimeDelta(PyTimeDelta {
            days: obj.getattr("days")?.extract()?,
            seconds: obj.getattr("seconds")?.extract()?,
            microseconds: obj.getattr("microseconds")?.extract()?,
        }));
    }
    if obj.is_instance(&py.import("pathlib")?.getattr("PurePath")?)? {
        return Ok(PyValue::Path(obj.str()?.to_string()));
    }
    Ok(PyValue::Str(obj.str()?.to_string()))
}

fn args_map(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<ArgsMap> {
    let mut out = ArgsMap::new();
    if let Some(kwargs) = kwargs {
        for (key, value) in kwargs.iter() {
            let key: String = key
                .extract()
                .map_err(|_| PyValueError::new_err("Argument names must be strings."))?;
            out.insert(key, to_py_value(&value)?);
        }
    }
    Ok(out)
}

/// What a run needs from the Python process, captured with the interpreter lock held.
struct Prepared {
    session: meerschaum_cli::native::Session,
    config: meerschaum_core::config::Config,
}

/// `context` is what the façade holds that the config registry does not know: `connectors`
/// maps connector keys to the `_core.SqlConnector`s of SQL connectors the Python process defined
/// ad hoc, `attributes` maps the keys of every ad hoc connector to the attributes that rebuild it
/// (`api:` and `valkey:` are built natively, custom types are told to the Python workers).
fn prepare(py: Python<'_>, context: Option<&Bound<'_, PyDict>>) -> PyResult<Prepared> {
    let session = crate::facade::worker_session();
    crate::facade::suggest_worker_python(py, &session);
    // Native code that reads `Config::current()` (the engine, `MRSM{}` symlinks) sees the same
    // tree as the runtime's own copy.
    crate::facade::publish_live_config(py)?;
    let config = crate::config_api::live_config(py)?;
    if let Some(context) = context {
        let mut held = Vec::new();
        if let Some(connectors) = context.get_item("connectors")? {
            for (keys, conn) in connectors.downcast::<PyDict>()?.iter() {
                let keys: String = keys.extract()?;
                let conn = conn
                    .downcast::<PySqlConnector>()
                    .map_err(|_| PyValueError::new_err("Expected a `_core.SqlConnector`."))?;
                session.register_sql(&keys, conn.get().inner.clone());
                held.push((keys, conn.get().inner.clone()));
            }
        }
        crate::facade::install_instance_provider(&held);
        let attributes = context
            .get_item("attributes")?
            .map(|a| a.downcast_into::<PyDict>())
            .transpose()?;
        let attribute_maps = crate::sync_api::attribute_maps(attributes.as_ref())?;
        // The pool is created lazily and workers start on first use: telling it about the
        // connectors costs nothing, and a plugin action (or a plugin source) builds the same
        // connectors by keys in its worker.
        // A connector this process holds that cannot be reached (a stale `valkey:` fixture) must
        // not fail an action that never uses it: each is registered on its own.
        for one in attribute_maps.chunks(1) {
            if let Err(e) = crate::facade::register_adhoc(py, &session, &config, one, true) {
                tracing::warn!("Skipping the connector `{}`: {e}", one[0].0);
            }
        }
    }
    Ok(Prepared { session, config })
}

/// The host of the extension's runtime: processes run as in the binary (`SystemHost`), but
/// prompts read Python's stdin through `input()` / `getpass.getpass()`, so a patched `input`,
/// a replaced `sys.stdin` and Jupyter's stdin all work (a terminal reader on fd 0 would not see
/// them). End of input and `KeyboardInterrupt` end the prompt like the end of input does.
#[derive(Debug, Default)]
struct PyHost {
    system: SystemHost,
}

fn py_input(prompt: &str) -> Option<String> {
    // A call that was stopped (Ctrl-C) asks nothing more: a prompt that nobody waits for would
    // swallow the next line typed at the REPL.
    if meerschaum_cli::native::jobs::stop_requested_flag().load(Ordering::SeqCst) {
        return None;
    }
    Python::with_gil(|py| -> PyResult<Option<String>> {
        let input = py.import("builtins")?.getattr("input")?;
        match input.call1((prompt,)) {
            Ok(line) => Ok(Some(line.extract::<String>()?)),
            Err(e) if e.is_instance_of::<PyEOFError>(py) => Ok(None),
            Err(e) if e.is_instance_of::<PyKeyboardInterrupt>(py) => {
                // The interrupt belongs to the main thread's `check_signals`; the prompt ends.
                Ok(None)
            }
            Err(e) => Err(e),
        }
    })
    .ok()
    .flatten()
}

impl Host for PyHost {
    fn run(&mut self, spec: &CommandSpec) -> std::io::Result<i32> {
        self.system.run(spec)
    }

    fn output(
        &mut self,
        spec: &CommandSpec,
        timeout: Option<std::time::Duration>,
    ) -> std::io::Result<CommandOutput> {
        self.system.output(spec, timeout)
    }

    fn read_line(&mut self, prompt: &str) -> Option<String> {
        py_input(prompt)
    }

    fn read_multiline(&mut self, prompt: &str) -> Option<String> {
        py_input(prompt)
    }

    fn read_line_timeout(&mut self, timeout: std::time::Duration) -> Option<String> {
        self.system.read_line_timeout(timeout)
    }

    fn read_line_interruptible(&mut self, prompt: &str) -> Option<String> {
        py_input(prompt)
    }

    fn read_secret(&mut self, prompt: &str) -> Option<String> {
        Python::with_gil(|py| -> PyResult<Option<String>> {
            let getpass = py.import("getpass")?.getattr("getpass")?;
            match getpass.call1((prompt,)) {
                Ok(line) => Ok(Some(line.extract::<String>()?)),
                Err(e)
                    if e.is_instance_of::<PyEOFError>(py)
                        || e.is_instance_of::<PyKeyboardInterrupt>(py) =>
                {
                    Ok(None)
                }
                Err(e) => Err(e),
            }
        })
        .ok()
        .flatten()
    }

    fn read_line_prefilled(&mut self, prompt: &str, prefill: &str) -> Option<String> {
        // Like `prompt_toolkit` without a terminal: the typed line is appended to the prefill.
        py_input(prompt).map(|line| format!("{prefill}{line}"))
    }
}

/// Run `f` on a runtime on the façade session printing to Python's streams, with the live config
/// and Python's stdin for prompts.
fn with_runtime<R>(prepared: Prepared, f: impl FnOnce(&mut Runtime<'_>) -> R) -> R {
    let mut out = PyStream("stdout");
    let mut rt = Runtime::system(&mut out);
    rt.err = Box::new(PyStream("stderr"));
    rt.host = Box::new(PyHost::default());
    rt.config = Some(prepared.config);
    rt.native = Some(prepared.session);
    // Colors and live progress follow Python's streams, not the process's file descriptors:
    // `sys.stdout` may be redirected while fd 1 is a terminal.
    rt.live = py_isatty("stdout");
    rt.progress = python_progress_sink();
    f(&mut rt)
}

thread_local! {
    /// Set on a call's thread by `run_interruptible`: raised when the call was abandoned (a
    /// second Ctrl-C), which turns that call's spinner off.
    static ABANDONED: std::cell::RefCell<Option<Arc<AtomicBool>>> =
        const { std::cell::RefCell::new(None) };
}

/// `sys.<name>.isatty()`; false when the stream is missing, closed or has no `isatty`.
fn py_isatty(name: &str) -> bool {
    Python::with_gil(|py| -> PyResult<bool> {
        let stream = py.import("sys")?.getattr(name)?;
        if stream.is_none() || !stream.hasattr("isatty")? {
            return Ok(false);
        }
        stream.call_method0("isatty")?.extract()
    })
    .unwrap_or(false)
}

/// Waiting feedback (spinners) is drawn through Python's `sys.stderr`, and only when it is a
/// terminal: a notebook, a pipe or a captured stream gets none.
fn python_progress_sink() -> meerschaum_cli::format::progress::ProgressSink {
    use meerschaum_cli::format::progress::ProgressSink;
    if !py_isatty("stderr") {
        return ProgressSink::Off;
    }
    let disabled = ABANDONED
        .with(|slot| slot.borrow().clone())
        .unwrap_or_default();
    ProgressSink::custom(
        Arc::new(|text: &str| {
            use std::io::Write as _;
            let mut stream = PyStream("stderr");
            let _ = stream.write_all(text.as_bytes());
        }),
        disabled,
    )
}

/// Printed (stderr) by a call made while a plugin module is being imported.
const NESTED_IMPORT_NOTICE: &str = "Plugins are skipped for this call: it was made while a plugin was being imported in a worker (a nested `mrsm.entry()` in a module's import), so it runs natively without plugins.\n";

/// How long an action may keep running after the first Ctrl-C before the call gives up on it.
const INTERRUPT_GRACE: Duration = Duration::from_secs(10);

/// Run the dispatcher on its own thread while this one (the interpreter's) polls Python's
/// signals every 50 ms with the GIL.
///
/// Every call has its own stop flag (`jobs::with_call_stop`), which the loops of that call poll.
/// The first Ctrl-C sets it and waits for the action to wind down; the call then raises
/// `KeyboardInterrupt`. A second Ctrl-C, or an action still running `INTERRUPT_GRACE` after the
/// first, raises at once and leaves the thread to finish: its flag stays set, so it stops at its
/// next checkpoint and a later call cannot revive it. The extension installs no process-wide
/// signal handler and never ends the process.
///
/// Python delivers signals to the main thread only: a call made on another thread cannot be
/// interrupted (it runs to the end).
fn run_interruptible(
    py: Python<'_>,
    f: impl FnOnce() -> SuccessTuple + Send + 'static,
) -> PyResult<SuccessTuple> {
    let stop = Arc::new(AtomicBool::new(false));
    let call_stop = stop.clone();
    // Raised when this call is given up on: its spinner must not outlive the call.
    let abandoned = Arc::new(AtomicBool::new(false));
    let call_abandoned = abandoned.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let rx = std::sync::Mutex::new(rx);
    std::thread::Builder::new()
        .name("mrsm-action".into())
        .spawn(move || {
            ABANDONED.with(|slot| *slot.borrow_mut() = Some(call_abandoned));
            let _ = tx.send(meerschaum_cli::native::jobs::with_call_stop(call_stop, f));
        })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let mut interrupted: Option<(PyErr, Instant)> = None;
    loop {
        match py.allow_threads(|| {
            rx.lock()
                .unwrap_or_else(|e| e.into_inner())
                .recv_timeout(Duration::from_millis(50))
        }) {
            Ok(result) => {
                return match interrupted {
                    Some((e, _)) => Err(e),
                    None => Ok(result),
                };
            }
            Err(RecvTimeoutError::Timeout) => {
                if let Err(e) = py.check_signals() {
                    stop.store(true, Ordering::SeqCst);
                    if interrupted.is_some() {
                        abandon_progress(py, &abandoned);
                        return Err(e);
                    }
                    interrupted = Some((e, Instant::now()));
                } else if let Some((_, at)) = &interrupted
                    && at.elapsed() > INTERRUPT_GRACE
                {
                    abandon_progress(py, &abandoned);
                    return Err(interrupted.take().map(|(e, _)| e).expect("interrupted"));
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(PyRuntimeError::new_err(
                    "The action ended without a result.",
                ));
            }
        }
    }
}

/// Erase the abandoned call's spinner and keep it off. Done without the GIL: the spinner writes
/// through Python's `sys.stderr`, which its thread may be waiting for while it holds the
/// progress lock this takes.
fn abandon_progress(py: Python<'_>, abandoned: &Arc<AtomicBool>) {
    py.allow_threads(|| {
        meerschaum_cli::format::progress::abandon();
        abandoned.store(true, Ordering::SeqCst);
    });
}

/// `_core.entry(sysargs, patch=None, context=None)`: run a (possibly chained) command line like
/// `meerschaum.entry()` and return the `SuccessTuple`. `patch` holds keyword arguments that
/// override the parsed ones (`_patch_args`).
#[pyfunction]
#[pyo3(signature = (sysargs, patch=None, context=None))]
pub fn entry<'py>(
    py: Python<'py>,
    sysargs: Vec<String>,
    patch: Option<&Bound<'py, PyDict>>,
    context: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyTuple>> {
    let patch = patch.map(|p| args_map(Some(p))).transpose()?;
    let prepared = prepare(py, context)?;
    let tuple = run_interruptible(py, move || {
        with_runtime(prepared, |rt| {
            // The plugin manifest decides which words are plugin actions (the binary does this
            // before it dispatches). A call made while a plugin is being imported must not start
            // a worker (the import would recurse): it runs natively, without plugins.
            if meerschaum_worker::worker::spawn_refused() {
                rt.eprint(NESTED_IMPORT_NOTICE);
            } else if let Err(e) = meerschaum_cli::python::manifest(rt) {
                rt.eprint(&format!("Unable to load plugin manifest: {}\n", e.message));
            }
            match &patch {
                Some(patch) if !patch.is_empty() => dispatch::entry_with_patch(&sysargs, patch, rt),
                _ => dispatch::entry(&sysargs, rt),
            }
        })
    })?;
    success_to_py(py, &tuple)
}

/// `_core.run_action(action, kwargs=None, context=None)`: run one action with keyword arguments like
/// `entry_with_args(action=[...], **kwargs)` (no argument parsing) and return the `SuccessTuple`.
#[pyfunction]
#[pyo3(signature = (action, kwargs=None, context=None))]
pub fn run_action<'py>(
    py: Python<'py>,
    action: Vec<String>,
    kwargs: Option<&Bound<'py, PyDict>>,
    context: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyTuple>> {
    let mut args = args_map(kwargs)?;
    args.insert("action".into(), PyValue::str_list(&action));
    let prepared = prepare(py, context)?;
    let tuple = run_interruptible(py, move || {
        with_runtime(prepared, |rt| dispatch::entry_with_args(args, rt, 0))
    })?;
    success_to_py(py, &tuple)
}

/// `_core.core_actions()`: the registry the façade builds `meerschaum.actions.actions` from, in
/// v4's order: `[(name, doc, accepts_executor_keys, [(sub, function, doc, accepts_executor_keys)])]`.
#[pyfunction]
pub fn core_actions(py: Python<'_>) -> PyResult<Bound<'_, PyList>> {
    let out = PyList::empty(py);
    for def in CORE_ACTIONS.iter() {
        out.append(action_entry(py, def)?)?;
    }
    Ok(out)
}

fn action_entry<'py>(py: Python<'py>, def: &ActionDef) -> PyResult<Bound<'py, PyTuple>> {
    let subs = PyList::empty(py);
    for sub in def.subactions {
        subs.append((
            sub.name,
            sub.function,
            sub.doc().unwrap_or_default(),
            sub.accepts_executor_keys,
        ))?;
    }
    PyTuple::new(
        py,
        [
            def.name.into_pyobject(py)?.into_any(),
            def.doc().unwrap_or_default().into_pyobject(py)?.into_any(),
            def.accepts_executor_keys
                .into_pyobject(py)?
                .to_owned()
                .into_any(),
            subs.into_any(),
        ],
    )
}

/// `_core.set_in_worker(in_worker=True)`: mark this interpreter as a Python worker's own (the
/// worker program does it at startup).
#[pyfunction]
#[pyo3(signature = (in_worker=true))]
pub fn set_in_worker(in_worker: bool) {
    meerschaum_worker::worker::set_in_worker(in_worker);
}

/// `_core.plugin_import_begin()`: a plugin module starts importing in this process. Called by
/// the code that imports plugins (`meerschaum.plugins`), paired with `plugin_import_end()`. In
/// a worker's interpreter no worker is started meanwhile (see `Worker::spawn`).
#[pyfunction]
pub fn plugin_import_begin() {
    meerschaum_worker::worker::plugin_import_begin();
}

/// `_core.plugin_import_end()`: the import started with `plugin_import_begin()` ended.
#[pyfunction]
pub fn plugin_import_end() {
    meerschaum_worker::worker::plugin_import_end();
}
