//! Native (Rust) plugins from the Python façade: `_core.native_plugin_names` and
//! `_core.native_connector_register`.
//!
//! A native plugin (a single `.rs` file or a native directory in a plugins directory) is not a
//! Python module, so `PluginConnector` cannot import it. The façade asks these bindings which
//! plugins are native and runs their `register(pipe)` hook in-process, exactly as the CLI's
//! `register pipes` does (`meerschaum_cli::native::manage::native_connector_register`).

use meerschaum_plugin::NativePlugins;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::pipe_bridge::{ParamsView, core_pipe, value_to_py};

/// `native_plugin_names()`: the names of the native plugins in the configured plugin
/// directories, in priority order. A scan only: nothing is built or loaded.
#[pyfunction]
pub fn native_plugin_names(py: Python<'_>) -> PyResult<Vec<String>> {
    let config = crate::facade::merged_config(py)?;
    let dirs = config.paths().plugins_dirs.clone();
    Ok(py.detach(move || {
        meerschaum_plugin::discovery::discover_report(&dirs)
            .candidates
            .into_iter()
            .map(|candidate| candidate.name)
            .collect()
    }))
}

/// `native_connector_register(pipe)`: run the `register(pipe)` hook of the native plugin that
/// owns the pipe's connector (`plugin:<name>` or a native `#[make_connector]` type).
///
/// Returns `None` when no native plugin owns the connector or it defines no `register`, else
/// `(valid, parameters, repr)`: `valid` is whether the hook returned a dict (or nothing),
/// `repr` the returned value as text for the façade's warning. A failing hook raises
/// `RuntimeError`.
#[pyfunction]
pub fn native_connector_register<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyTuple>>> {
    let core = core_pipe(pipe, ParamsView::Raw)?;
    let config = crate::facade::publish_live_config(py)?;
    let outcome = py.detach(move || {
        let paths = config.paths();
        let natives = NativePlugins::open(
            &paths.plugins_dirs,
            &meerschaum_cli::native_plugins::cache_path(&paths.root_dir),
        )
        .map_err(|e| e.to_string())?;
        meerschaum_cli::native::manage::native_connector_register(&natives, &config, &core)
            .transpose()
    });
    match outcome.map_err(PyRuntimeError::new_err)? {
        None => Ok(None),
        Some(outcome) => {
            let parameters =
                value_to_py(py, &meerschaum_core::Value::Map(outcome.parameters.clone()))?;
            Ok(Some(PyTuple::new(
                py,
                [
                    outcome.valid.into_pyobject(py)?.to_owned().into_any(),
                    parameters,
                    outcome.repr.into_pyobject(py)?.into_any(),
                ],
            )?))
        }
    }
}
