//! Where the client's `warn(msg, stack=False)` calls go.
//!
//! The Python client prints these through `meerschaum.utils.warnings.warn`. This crate cannot
//! format output (the formatter lives in the host), so a host installs a sink with
//! [`set_warning_sink`] and prints the messages itself, in the host's style and on its stderr.
//! Without a sink the messages go to `tracing`.

use std::sync::{Arc, RwLock};

/// A host's receiver of the client's warnings (called from whichever thread made the request).
pub type WarningSink = Arc<dyn Fn(&str) + Send + Sync>;

static SINK: RwLock<Option<WarningSink>> = RwLock::new(None);

/// Install (or, with `None`, remove) the process-wide warning sink.
pub fn set_warning_sink(sink: Option<WarningSink>) {
    if let Ok(mut slot) = SINK.write() {
        *slot = sink;
    }
}

/// `warn(msg, stack=False)`.
pub(crate) fn warn(message: &str) {
    let sink = SINK.read().ok().and_then(|slot| slot.clone());
    match sink {
        Some(sink) => sink(message),
        None => tracing::warn!("{message}"),
    }
}

/// `warn(f"...")` with `format!` arguments.
macro_rules! warn_fmt {
    ($($arg:tt)*) => {
        $crate::client::warnings::warn(&format!($($arg)*))
    };
}
pub(crate) use warn_fmt;

/// `str(value)` of a JSON value the server sent as a `detail` (a string prints bare).
pub(crate) fn detail_text(value: &serde_json::Value) -> String {
    meerschaum_core::pyfmt::py_str(&crate::pipe_ops::json_to_core(value))
}

/// The `tracing` target of the warnings about the engine's own state pipes: hosts that print
/// `tracing` warnings to users (the Python extension) skip it.
pub const STATE_PIPE_TARGET: &str = "meerschaum_api::state_pipes";

/// `warn(...)` about a pipe. The engine's own state pipes (`mrsm:` connector keys, which the API
/// refuses to serve) are probed on API instances with no v4 counterpart: their failures stay on
/// `tracing`.
pub(crate) fn warn_pipe(pipe: &meerschaum_core::Pipe, message: &str) {
    if pipe.connector_keys() == "mrsm" {
        tracing::warn!(target: STATE_PIPE_TARGET, "{message}");
    } else {
        warn(message);
    }
}

/// [`warn_pipe`] with `format!` arguments.
macro_rules! warn_pipe_fmt {
    ($pipe:expr, $($arg:tt)*) => {
        $crate::client::warnings::warn_pipe($pipe, &format!($($arg)*))
    };
}
pub(crate) use warn_pipe_fmt;
