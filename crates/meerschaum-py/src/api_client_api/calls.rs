//! Running client calls from Python: interruptible waits on the runtime with signal checks, and
//! the sink that sends the client's warnings to `meerschaum.utils.warnings.warn`.

use super::*;

/// Run `fut` on the runtime until it ends, checking for Python signals every 100 ms (a
/// `KeyboardInterrupt` aborts the task and propagates) and for `stop`, which a callback notifies
/// to end the task early. `Ok(None)` is an early stop.
pub(crate) fn run_interruptible<T: Send + 'static>(
    py: Python<'_>,
    fut: impl std::future::Future<Output = T> + Send + 'static,
    stop: Option<Arc<tokio::sync::Notify>>,
    stop_event: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<T>> {
    let mut handle = runtime().spawn(fut);
    loop {
        if let Err(e) = py.check_signals() {
            handle.abort();
            return Err(e);
        }
        // A `threading.Event`-like object (`is_set()`) ends the task early.
        if let Some(event) = stop_event
            && event
                .call_method0("is_set")
                .and_then(|v| v.is_truthy())
                .unwrap_or(false)
        {
            handle.abort();
            return Ok(None);
        }
        let stop = stop.clone();
        let step = py.detach(|| {
            runtime().block_on(async {
                tokio::select! {
                    r = &mut handle => Some(Some(r)),
                    _ = async {
                        match &stop {
                            Some(s) => s.notified().await,
                            None => std::future::pending::<()>().await,
                        }
                    } => Some(None),
                    _ = tokio::time::sleep(Duration::from_millis(100)) => None,
                }
            })
        });
        match step {
            None => continue,
            Some(None) => {
                handle.abort();
                return Ok(None);
            }
            Some(Some(Ok(v))) => return Ok(Some(v)),
            Some(Some(Err(e))) => {
                return Err(PyRuntimeError::new_err(format!(
                    "The request task failed: {e}"
                )));
            }
        }
    }
}

/// The client's `warn(msg, stack=False)` calls (called from whichever runtime thread made the
/// request) reach `meerschaum.utils.warnings.warn`, with v4's text.
pub(crate) fn install_warning_sink() {
    meerschaum_connector_api::client::warnings::set_warning_sink(Some(Arc::new(
        |message: &str| {
            // Skipped once the interpreter is finalizing (`attach` would panic there).
            let _ = Python::try_attach(|py| {
                let _ = (|| -> PyResult<()> {
                    let warn = py.import("meerschaum.utils.warnings")?.getattr("warn")?;
                    let kwargs = PyDict::new(py);
                    kwargs.set_item("stack", false)?;
                    warn.call((message,), Some(&kwargs))?;
                    Ok(())
                })();
            });
        },
    )));
}
