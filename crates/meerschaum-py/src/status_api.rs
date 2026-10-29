//! Per-pipe sync status (rc15 contract C2) for the Python façade: `pipe.get_sync_status()` and
//! `mrsm.get_sync_statuses(instance)` read the `mrsm:engine_sync_status` rows every sync path
//! records ([`meerschaum_connector::sync_status`]) through the instance connector's trait
//! methods, so an `api:` instance asks its server (`GET /engine/sync-status`).
//!
//! The façade passes the pipe (only its keys are read) and the `context` the other `_core.pipe_*`
//! bindings take (`meerschaum.core.Pipe._data._rust_context`): a SQL instance is reached through
//! the `_core.SqlConnector` the Python connector holds, and any other instance is built by keys
//! (the Rust clients for `api:` and `valkey:`, the worker for a custom instance connector).
//!
//! Loading this module also says that this process is Python (the `source` of the syncs it
//! records), unless the command line runs in it ([`register`]).

use std::sync::Arc;

use meerschaum_connector::InstanceConnector;
use meerschaum_connector::sync_status::{SyncSource, SyncStatus};
use meerschaum_engine::ConnectorRegistry;
use meerschaum_engine::registry::StaticRegistry;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::convert::json_to_py;
use crate::facade::{self, RegistryInput};
use crate::pipe_bridge::{ParamsView, core_pipe};
use crate::sql_api::PySqlConnector;
use crate::sync_api::{DebugGuard, attribute_maps};

fn context_item<'py>(
    context: &Bound<'py, PyDict>,
    key: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(context.get_item(key)?.filter(|v| !v.is_none()))
}

/// The instance connector of `pipe` for this call.
pub(crate) fn instance_of(
    py: Python<'_>,
    pipe: &meerschaum_core::Pipe,
    context: &Bound<'_, PyDict>,
) -> PyResult<Arc<dyn InstanceConnector>> {
    let instance_keys = pipe.instance_keys().to_string();
    if let Some(conn) = context_item(context, "connector")? {
        let sql = conn
            .cast::<PySqlConnector>()
            .map_err(|_| PyValueError::new_err("Expected a `_core.SqlConnector`."))?
            .get()
            .inner
            .clone();
        facade::install_instance_provider(&[(instance_keys.clone(), sql.clone())]);
        let registry = StaticRegistry::new().with_instance(&instance_keys, sql as Arc<_>);
        return registry
            .instance(&instance_keys)
            .map_err(PyRuntimeError::new_err);
    }
    let attributes = match context_item(context, "attributes")? {
        Some(a) => attribute_maps(Some(a.cast::<PyDict>()?))?,
        None => Vec::new(),
    };
    let built = facade::build_registry(
        py,
        RegistryInput {
            sql: Vec::new(),
            attributes,
            instance_keys: instance_keys.clone(),
            connector_keys: instance_keys.clone(),
            inplace: false,
        },
        !facade::is_native(&instance_keys),
    )
    .map_err(PyRuntimeError::new_err)?;
    built
        .registry
        .instance(&instance_keys)
        .map_err(PyRuntimeError::new_err)
}

fn status_to_py<'py>(py: Python<'py>, status: &SyncStatus) -> PyResult<Bound<'py, PyAny>> {
    json_to_py(py, &status.to_json())
}

/// `_core.pipe_get_sync_status(pipe, context, debug=False)`: the C2 record of the pipe's latest
/// recorded sync as a dict, or `None` when no sync of it was recorded. A failed read raises
/// `RuntimeError`.
#[pyfunction]
#[pyo3(signature = (pipe, context, debug=false))]
pub fn pipe_get_sync_status<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _debug = DebugGuard::new(debug);
    let core = core_pipe(pipe, ParamsView::Keys)?;
    let conn = instance_of(py, &core, context)?;
    let found = facade::spawn_block_on(py, async move { conn.get_sync_status(&core).await })
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    match found {
        Some(status) => status_to_py(py, &status),
        None => Ok(py.None().into_bound(py)),
    }
}

/// `_core.get_sync_statuses(pipe, context, debug=False)`: every C2 record on the instance of
/// `pipe` (any pipe of that instance; only its instance is read), as a list of dicts ordered by
/// `pipe_ref`. A failed read raises `RuntimeError`.
#[pyfunction]
#[pyo3(signature = (pipe, context, debug=false))]
pub fn get_sync_statuses<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    debug: bool,
) -> PyResult<Bound<'py, PyList>> {
    let _debug = DebugGuard::new(debug);
    let core = core_pipe(pipe, ParamsView::Keys)?;
    let instance_keys = core.instance_keys().to_string();
    let conn = instance_of(py, &core, context)?;
    let statuses =
        facade::spawn_block_on(
            py,
            async move { conn.get_sync_statuses(&instance_keys).await },
        )
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let out = PyList::empty(py);
    for status in &statuses {
        out.append(status_to_py(py, status)?)?;
    }
    Ok(out)
}

/// `_core.flush_sync_statuses(timeout=3.0)`: write the sync statuses this process still holds
/// (written in the background; a success within a few seconds of the last one is held) and
/// wait up to `timeout` seconds. Registered with `atexit`, so a script's last syncs are kept.
#[pyfunction]
#[pyo3(signature = (timeout=3.0))]
pub fn flush_sync_statuses(py: Python<'_>, timeout: f64) {
    if !meerschaum_connector::sync_status::has_unwritten() {
        return;
    }
    let timeout = std::time::Duration::from_secs_f64(timeout.clamp(0.0, 60.0));
    facade::spawn_block_on(py, meerschaum_connector::sync_status::flush(timeout));
}

/// Register the bindings, and say that this process is Python unless the command line set
/// its own role first ([`meerschaum_connector::sync_status::set_default_process_role`]).
/// The process writes the sync statuses it still holds when it exits ([`flush_sync_statuses`]
/// with `atexit`).
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    meerschaum_connector::sync_status::set_default_process_role(SyncSource::Python);
    m.add_function(wrap_pyfunction!(pipe_get_sync_status, m)?)?;
    m.add_function(wrap_pyfunction!(get_sync_statuses, m)?)?;
    m.add_function(wrap_pyfunction!(flush_sync_statuses, m)?)?;
    let flush = m.getattr("flush_sync_statuses")?;
    m.py()
        .import("atexit")?
        .call_method1("register", (flush,))?;
    Ok(())
}
