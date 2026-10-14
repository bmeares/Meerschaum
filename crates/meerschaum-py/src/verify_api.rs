//! `Pipe.verify()` in Rust (design §4.4, D14): `_core.verify_pipe` runs the CLI's
//! `meerschaum_cli::native::verify::verify_pipe_with` on connectors the Python process built.
//!
//! **The façade session.** [`facade_session`] is one [`Session`] per process (v4 job daemons
//! fork; a child builds its own) on the extension's Tokio runtime ([`crate::runtime::runtime`]),
//! which the `_core.SqlConnector` pools were created on. It is *registered-only* and has *no
//! Python worker*: lookups return only the connectors the façade registered
//! ([`Session::register_sql`]) and never read the config registry, connect to configured
//! connectors, or start a worker process. The engine registry built for each chunk sync
//! therefore sees exactly the instance and source connectors the caller passed.
//!
//! Output (`info`, the per-chunk `pprint`, warnings) goes to Python's `sys.stdout` /
//! `sys.stderr` as it is produced (the GIL is taken per write), so `capsys` and redirected
//! streams see it like v4's prints.
//!
//! The Python side (`meerschaum/core/Pipe/_verify.py`) decides which calls may come here and
//! falls back to the Python orchestration for the rest (see `PARITY.md`, the `_verify.py` row).

use std::io::Write;
use std::sync::Mutex;

use chrono::Duration;
use meerschaum_cli::dispatch::Runtime;
use meerschaum_cli::native::Session;
use meerschaum_cli::native::pipes::SelectedPipe;
use meerschaum_cli::native::sync::SyncArgs;
use meerschaum_cli::native::verify::{VerifyArgs, verify_pipe_with};
use meerschaum_cli::types::DateContext;
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::pipe::ChunkIntervalArg;
use meerschaum_core::{Map, SuccessTuple, Value};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyInt, PyString, PyTuple};

use crate::convert::{PyCtx, py_to_core};
use crate::pipe_bridge::{ParamsView, core_pipe, date_bound, params_map, success_to_py};
use crate::sql_api::PySqlConnector;

/// The façade's process session: registered connectors only, no Python worker, on the
/// extension's runtime. Rebuilt in a forked child.
pub fn facade_session() -> Session {
    static SESSION: Mutex<Option<(u32, Session)>> = Mutex::new(None);
    let pid = std::process::id();
    let mut slot = SESSION.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((owner, session)) = slot.as_ref()
        && *owner == pid
    {
        return session.clone();
    }
    let session = Session::with_runtime(crate::runtime::runtime().handle().clone());
    session.set_registered_only(true);
    session.set_python_worker(false);
    *slot = Some((pid, session.clone()));
    session
}

/// `_core.register_connector(keys, connector)`: make `connector` (a `_core.SqlConnector`) the
/// façade session's connector for `keys`. Returns whether it replaced another connector.
#[pyfunction]
pub fn register_connector(keys: &str, connector: &Bound<'_, PyAny>) -> PyResult<bool> {
    let conn = connector
        .downcast::<PySqlConnector>()
        .map_err(|_| PyValueError::new_err("Expected a `_core.SqlConnector`."))?;
    Ok(facade_session()
        .register_sql(keys, conn.get().inner.clone())
        .is_some())
}

/// `_core.unregister_connector(keys)`: forget the façade session's connector for `keys`.
#[pyfunction]
pub fn unregister_connector(keys: &str) -> bool {
    facade_session().unregister(keys).is_some()
}

/// Writes to `sys.<name>` (taking the GIL for each write).
struct PyStream(&'static str);

impl Write for PyStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let text = String::from_utf8_lossy(buf).into_owned();
        Python::with_gil(|py| -> PyResult<()> {
            let stream = py.import("sys")?.getattr(self.0)?;
            if !stream.is_none() {
                stream.call_method1("write", (text,))?;
            }
            Ok(())
        })
        .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Python::with_gil(|py| -> PyResult<()> {
            let stream = py.import("sys")?.getattr(self.0)?;
            if !stream.is_none() && stream.hasattr("flush")? {
                stream.call_method0("flush")?;
            }
            Ok(())
        })
        .map_err(|e| std::io::Error::other(e.to_string()))
    }
}

/// `chunk_interval`: a `timedelta`, an `int`, or `None`.
fn chunk_interval_arg(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<ChunkIntervalArg>> {
    let Some(obj) = obj.filter(|o| !o.is_none()) else {
        return Ok(None);
    };
    if obj.is_instance_of::<PyInt>() {
        return Ok(Some(ChunkIntervalArg::Int(obj.extract()?)));
    }
    if obj.hasattr("total_seconds")? && obj.hasattr("microseconds")? {
        let days: i64 = obj.getattr("days")?.extract()?;
        let seconds: i64 = obj.getattr("seconds")?.extract()?;
        let micros: i64 = obj.getattr("microseconds")?.extract()?;
        return Ok(Some(ChunkIntervalArg::Delta(
            Duration::days(days) + Duration::seconds(seconds) + Duration::microseconds(micros),
        )));
    }
    Err(PyValueError::new_err(format!(
        "Invalid chunk_interval '{}'.",
        obj.str()?
    )))
}

/// The config the verify runtime reads (`pipes:verify:max_chunks_syncs`, the engine settings,
/// formatting): the files under `MRSM_ROOT_DIR` overlaid with the Python process's in-memory
/// `meerschaum.config.config` (so runtime patches are honored). Never writes files.
fn verify_config(py: Python<'_>) -> PyResult<Config> {
    let mut options = ConfigOptions::from_process();
    options.allow_writes = false;
    let config = Config::new(options).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let py_config = py.import("meerschaum.config")?.getattr("config")?;
    if let Ok(Value::Map(map)) = py_to_core(&py_config) {
        let mut merged = config.snapshot();
        for (k, v) in map {
            merged.insert(k, v);
        }
        config.set_config(merged);
    }
    Ok(config)
}

/// `_core.verify_pipe(pipe, connectors, begin=None, end=None, params=None, chunk_interval=None,
/// bounded=None, deduplicate=False, workers=None, batchsize=None,
/// skip_chunks_with_greater_rowcounts=False, check_rowcounts_only=False, chunksize=-1,
/// check_existing=True)`.
///
/// `connectors` maps connector keys to the `_core.SqlConnector`s to use (the pipe's instance
/// and, for SQL sources, its source); they are registered in the façade session first.
/// Returns the `SuccessTuple`.
#[pyfunction]
#[pyo3(signature = (
    pipe, connectors, begin=None, end=None, params=None, chunk_interval=None, bounded=None,
    deduplicate=false, workers=None, batchsize=None, skip_chunks_with_greater_rowcounts=false,
    check_rowcounts_only=false, chunksize=-1, check_existing=true
))]
#[allow(clippy::too_many_arguments)]
pub fn verify_pipe<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    connectors: &Bound<'py, PyDict>,
    begin: Option<&Bound<'py, PyAny>>,
    end: Option<&Bound<'py, PyAny>>,
    params: Option<&Bound<'py, PyAny>>,
    chunk_interval: Option<&Bound<'py, PyAny>>,
    bounded: Option<bool>,
    deduplicate: bool,
    workers: Option<i64>,
    batchsize: Option<i64>,
    skip_chunks_with_greater_rowcounts: bool,
    check_rowcounts_only: bool,
    chunksize: Option<i64>,
    check_existing: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    let session = facade_session();
    for (keys, conn) in connectors.iter() {
        let keys = keys.downcast::<PyString>()?.to_str()?.to_string();
        let conn = conn
            .downcast::<PySqlConnector>()
            .map_err(|_| PyValueError::new_err("Expected a `_core.SqlConnector`."))?;
        session.register_sql(&keys, conn.get().inner.clone());
    }
    let core = core_pipe(pipe, ParamsView::Raw)?;
    let instance_keys = core.instance_keys().to_string();
    if !session.is_registered(&instance_keys) {
        return Err(PyValueError::new_err(format!(
            "No connector was passed for the instance '{instance_keys}'."
        )));
    }
    let ctx = PyCtx::new(py)?;
    let va = VerifyArgs {
        bounded,
        chunk_interval: chunk_interval_arg(chunk_interval)?,
        workers,
        batchsize,
        deduplicate,
        check_rowcounts_only,
        skip_chunks_with_greater_rowcounts,
    };
    let sync_args = SyncArgs {
        begin: date_bound(&ctx, begin)?,
        end: date_bound(&ctx, end)?,
        params: params_map(params)?,
        check_existing,
        chunksize,
        extra: Map::new(),
    };
    let config = verify_config(py)?;
    let result: Result<SuccessTuple, String> = py.allow_threads(move || {
        let inst = session
            .instance(&instance_keys, &config)
            .map_err(|e| e.message)?;
        let mut out = PyStream("stdout");
        let mut rt = Runtime::new(
            DateContext::system(),
            meerschaum_cli::help::terminal_columns(),
            &mut out,
            meerschaum_cli::env::process_env,
        );
        rt.err = Box::new(PyStream("stderr"));
        rt.vars = || std::env::vars().collect();
        rt.config = Some(config);
        rt.native = Some(session.clone());
        let mut sp = SelectedPipe {
            pipe: core,
            id: None,
            hydrated: false,
        };
        Ok(verify_pipe_with(
            &mut rt, &session, &inst, &mut sp, &va, &sync_args,
        ))
    });
    match result {
        Ok(tuple) => success_to_py(py, &tuple),
        Err(message) => Err(PyRuntimeError::new_err(message)),
    }
}
