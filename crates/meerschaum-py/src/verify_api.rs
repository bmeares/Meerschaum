//! `Pipe.verify()` in Rust (design §4.4, D14): `_core.verify_pipe` runs the CLI's
//! `meerschaum_cli::native::verify::verify_pipe_with` on connectors the Python process built.
//!
//! **The façade session.** [`facade_session`] is [`crate::facade::worker_session`]: one
//! [`Session`] per process (v4 job daemons fork; a child builds its own) on the extension's Tokio
//! runtime ([`crate::runtime::runtime`]), which the `_core.SqlConnector` pools were created on.
//! The `_core.SqlConnector`s the façade registers ([`Session::register_sql`]) and the ad hoc
//! `api:` / `valkey:` connectors of the call ([`crate::facade::register_adhoc`]) are used as
//! given; every other connector is built from the config registry (the files under
//! `MRSM_ROOT_DIR` overlaid with the Python process's in-memory config), and a Python source or
//! instance (`plugin:`, `@make_connector`) runs in the Python worker the session starts on first
//! use (`MRSM_PYTHON`, the façade defaults it to its own interpreter).
//!
//! Output (`info`, the per-chunk `pprint`, warnings) goes to Python's `sys.stdout` /
//! `sys.stderr` as it is produced (the GIL is taken per write), so `capsys` and redirected
//! streams see it like v4's prints. `debug=True` turns on the `tracing` sink's `debug` events
//! ([`crate::logging`]).
//!
//! The Python side (`meerschaum/core/Pipe/_verify.py`) hands over the connectors; there is no
//! Python orchestration left for the cases of the earlier waves (see `PARITY.md`, Wave 15, L).

use chrono::Duration;
use meerschaum_cli::dispatch::Runtime;
use meerschaum_cli::native::Session;
use meerschaum_cli::native::pipes::SelectedPipe;
use meerschaum_cli::native::sync::SyncArgs;
use meerschaum_cli::native::verify::{VerifyArgs, verify_pipe_with};
use meerschaum_cli::types::DateContext;
use meerschaum_core::SuccessTuple;
use meerschaum_core::config::Config;
use meerschaum_core::pipe::ChunkIntervalArg;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyInt, PyString, PyTuple};
use std::io::Write;

use crate::convert::PyCtx;
use crate::pipe_bridge::{ParamsView, core_pipe, date_bound, params_map, success_to_py};
use crate::sql_api::PySqlConnector;

/// The façade's process session (see the module docs). Rebuilt in a forked child.
pub fn facade_session() -> Session {
    crate::facade::worker_session()
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
pub(crate) struct PyStream(pub(crate) &'static str);

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
/// formatting): a snapshot of the live config dictionary, taken now ([`crate::config_api::live_config`]).
pub(crate) fn verify_config(py: Python<'_>) -> PyResult<Config> {
    crate::config_api::live_config(py)
}

/// `_core.verify_pipe(pipe, connectors, begin=None, end=None, params=None, chunk_interval=None,
/// bounded=None, deduplicate=False, workers=None, batchsize=None,
/// skip_chunks_with_greater_rowcounts=False, check_rowcounts_only=False, chunksize=-1,
/// check_existing=True, kwargs=None, attributes=None, debug=False)`.
///
/// `connectors` maps connector keys to the `_core.SqlConnector`s to use (the pipe's instance
/// and, for SQL sources, its source); they are registered in the façade session first.
/// `attributes` maps the keys of connectors the Python process defined ad hoc to their
/// attributes, and `kwargs` holds the keyword arguments `Pipe.verify()` forwards to every chunk's
/// `pipe.sync()` (`force`, `retries`, plugin arguments). Returns the `SuccessTuple`.
#[pyfunction]
#[pyo3(signature = (
    pipe, connectors, begin=None, end=None, params=None, chunk_interval=None, bounded=None,
    deduplicate=false, workers=None, batchsize=None, skip_chunks_with_greater_rowcounts=false,
    check_rowcounts_only=false, chunksize=-1, check_existing=true, kwargs=None, attributes=None,
    debug=false
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
    kwargs: Option<&Bound<'py, PyDict>>,
    attributes: Option<&Bound<'py, PyDict>>,
    debug: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    let _debug = crate::sync_api::DebugGuard::new(debug);
    let session = facade_session();
    crate::facade::suggest_worker_python(py, &session);
    let mut held = Vec::new();
    for (keys, conn) in connectors.iter() {
        let keys = keys.downcast::<PyString>()?.to_str()?.to_string();
        let conn = conn
            .downcast::<PySqlConnector>()
            .map_err(|_| PyValueError::new_err("Expected a `_core.SqlConnector`."))?;
        session.register_sql(&keys, conn.get().inner.clone());
        held.push((keys, conn.get().inner.clone()));
    }
    crate::facade::install_instance_provider(&held);
    let core = core_pipe(pipe, ParamsView::Raw)?;
    let instance_keys = core.instance_keys().to_string();
    let connector_keys = core.connector_keys().to_string();
    crate::facade::publish_live_config(py)?;
    let config = verify_config(py)?;
    let attribute_maps = crate::sync_api::attribute_maps(attributes)?;
    let need_python = !crate::facade::is_native(&instance_keys)
        || (connector_keys.contains(':') && !crate::facade::is_native(&connector_keys));
    crate::facade::register_adhoc(py, &session, &config, &attribute_maps, need_python)
        .map_err(PyRuntimeError::new_err)?;
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
        extra: crate::sync_api::kwargs_map(kwargs)?,
    };
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
        rt.vars = meerschaum_cli::dispatch::process_vars;
        rt.config = Some(config);
        rt.native = Some(session.clone());
        let mut sp = SelectedPipe {
            pipe: core,
            id: None,
            hydrated: false,
        };
        // The verification reads `columns`, `dtypes` and `verify` of the pipe before the engine
        // resolves anything: resolve `reference`s, `{{ }}` templates and `MRSM{}` symlinks against
        // the instance first (the raw, persisted parameters keep their templates).
        meerschaum_cli::native::pipes::hydrate(&mut rt, &inst, &mut sp);
        let provider = meerschaum_cli::native::session::ConfiguredSqlProvider::new(
            rt.config.as_ref().expect("config"),
        );
        let defaults =
            meerschaum_core::pipe::PipeDefaults::from_config(rt.config.as_ref().expect("config"));
        let (resolved, warnings) = session.block_on(meerschaum_connector::resolve_pipe(
            &sp.pipe,
            inst.conn().as_ref(),
            Some(&provider),
            &defaults,
        ));
        for warning in warnings {
            tracing::warn!("{warning}");
        }
        sp.pipe = resolved;
        Ok(verify_pipe_with(
            &mut rt, &session, &inst, &mut sp, &va, &sync_args,
        ))
    });
    match result {
        Ok(tuple) => success_to_py(py, &tuple),
        Err(message) => Err(PyRuntimeError::new_err(message)),
    }
}
