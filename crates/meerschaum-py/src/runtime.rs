//! The Tokio runtime owned by the extension (design §4.2): every async instance method runs on
//! it with the GIL released.
//!
//! The runtime is per process. v4 jobs daemonize with `fork()`, and a forked child inherits the
//! parent's runtime object but none of its worker threads, so `block_on` would wait forever. The
//! runtime is therefore keyed by the process ID: a child builds its own on first use, and the
//! parent's copy is leaked (dropping it would try to join threads that do not exist in the child).
//! Connection pools have the same problem, so the Python façade rebuilds its `_core.SqlConnector`
//! after a fork (see `SQLConnector._core_connector`).

use std::future::Future;
use std::sync::Mutex;

use pyo3::prelude::*;
use tokio::runtime::Runtime;

/// The current process's multi-threaded runtime (built on first use in each process).
pub fn runtime() -> &'static Runtime {
    static RUNTIME: Mutex<Option<(u32, &'static Runtime)>> = Mutex::new(None);
    let pid = std::process::id();
    let mut slot = RUNTIME.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((owner, rt)) = *slot {
        if owner == pid {
            return rt;
        }
    }
    let rt: &'static Runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("mrsm-core")
            .worker_threads(
                std::thread::available_parallelism()
                    .map(|n| n.get().clamp(2, 8))
                    .unwrap_or(4),
            )
            .build()
            .expect("failed to build the Tokio runtime"),
    ));
    *slot = Some((pid, rt));
    rt
}

/// Run `fut` to completion on the extension's runtime with the GIL released.
///
/// The future must not touch Python objects (convert arguments before, results after).
pub fn block_on<F>(py: Python<'_>, fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    py.allow_threads(|| runtime().block_on(fut))
}
