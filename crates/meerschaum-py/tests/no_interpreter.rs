//! Callers that reach Python from Rust threads (the `tracing` sink, `info()` from the engine, job
//! monitors and API callbacks) must skip quietly when no interpreter can be attached. pyo3 0.30's
//! `Python::attach` panics when the interpreter is not initialized or is finalizing (Python 3.13+,
//! e.g. a background thread still logging at exit); these paths use `Python::try_attach`. This
//! test binary never initializes Python, which takes the same branch as finalization.

#[test]
fn background_python_sinks_skip_without_an_interpreter() {
    _native::logging::install();
    tracing::warn!("dropped: no interpreter is attached");
    tracing::debug!("dropped: no interpreter is attached");
    _native::pipe_methods::py_info("dropped: no interpreter is attached");
}
