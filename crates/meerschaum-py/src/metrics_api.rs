//! The shared native numeric metrics reader; Python only supplies instance and visibility.

use meerschaum_connector::{InstanceConnector, sync_metrics::MetricsQuery};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::PyDict,
};
use std::sync::Arc;

fn instance(
    py: Python<'_>,
    request: &serde_json::Value,
    pipe: Option<&Bound<'_, PyAny>>,
    context: Option<&Bound<'_, PyDict>>,
) -> PyResult<Arc<dyn InstanceConnector>> {
    match (pipe, context) {
        (Some(pipe), Some(context)) => {
            let pipe = crate::pipe_bridge::core_pipe(pipe, crate::pipe_bridge::ParamsView::Keys)?;
            crate::status_api::instance_of(py, &pipe, context)
        }
        (None, None) => crate::engine_api::metrics_instance(py, request),
        _ => Err(PyValueError::new_err(
            "Supply both Pipe and native instance context.",
        )),
    }
}

/// `engine_metrics(request_json, pipe=None, context=None)`: bounded numeric aggregates.
#[pyfunction]
#[pyo3(signature = (request, pipe=None, context=None))]
pub fn engine_metrics(
    py: Python<'_>,
    request: &str,
    pipe: Option<&Bound<'_, PyAny>>,
    context: Option<&Bound<'_, PyDict>>,
) -> PyResult<String> {
    let request: serde_json::Value =
        serde_json::from_str(request).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let query: MetricsQuery = serde_json::from_value(request.clone())
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    query
        .validate()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let conn = instance(py, &request, pipe, context)?;
    let result =
        crate::facade::spawn_block_on(py, async move { conn.get_engine_metrics(&query).await })
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    serde_json::to_string(&result).map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

/// Create the canonical empty metrics, runs and nodes tables and indexes.
#[pyfunction]
#[pyo3(signature = (request, pipe=None, context=None))]
pub fn initialize_engine_metrics(
    py: Python<'_>,
    request: &str,
    pipe: Option<&Bound<'_, PyAny>>,
    context: Option<&Bound<'_, PyDict>>,
) -> PyResult<()> {
    let request: serde_json::Value =
        serde_json::from_str(request).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let conn = instance(py, &request, pipe, context)?;
    crate::facade::spawn_block_on(py, async move { conn.initialize_engine_metrics().await })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
}
