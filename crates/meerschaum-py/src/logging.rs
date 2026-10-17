//! The extension's `tracing` sink: the engine, the worker adapter and the SQL driver report
//! warnings through `tracing` (the `mrsm` binary prints them with its subscriber); in the Python
//! process they reach `meerschaum.utils.warnings.warn()` (what v4's `warn()` calls print), and
//! the `debug` events of a `Pipe.sync(debug=True)` reach `meerschaum.utils.debug.dprint()`.
//!
//! Only events from the Meerschaum crates are forwarded (`meerschaum_*` targets); dependency
//! chatter (`sqlx`, `hyper`, ...) never reaches the user. The sink installs itself once, as the
//! global default, unless another subscriber already is one (`cargo test` harnesses).

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::prelude::*;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

use crate::sync_api::DEBUG_SYNCS;

struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.message, " {}={value:?}", field.name());
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            let _ = write!(self.message, " {}={value}", field.name());
        }
    }
}

struct PythonSink {
    next_span: AtomicU64,
}

impl Subscriber for PythonSink {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        if !metadata.target().starts_with("meerschaum_") {
            return false;
        }
        // Probes of the engine's own state pipes (`mrsm:` keys) on an API instance have no v4
        // counterpart; like in the CLI, Python users never see their failures.
        if metadata.target() == meerschaum_api::client::warnings::STATE_PIPE_TARGET {
            return false;
        }
        match *metadata.level() {
            Level::ERROR | Level::WARN => true,
            Level::INFO | Level::DEBUG => DEBUG_SYNCS.load(Ordering::SeqCst) > 0,
            Level::TRACE => false,
        }
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = MessageVisitor {
            message: String::new(),
        };
        event.record(&mut visitor);
        let level = *event.metadata().level();
        let message = visitor.message;
        let _ = Python::with_gil(|py| -> PyResult<()> {
            if level <= Level::WARN {
                let warn = py.import("meerschaum.utils.warnings")?.getattr("warn")?;
                let kwargs = pyo3::types::PyDict::new(py);
                kwargs.set_item("stack", false)?;
                warn.call((message,), Some(&kwargs))?;
            } else {
                let dprint = py.import("meerschaum.utils.debug")?.getattr("dprint")?;
                dprint.call1((message,))?;
            }
            Ok(())
        });
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

/// Install the sink as the global `tracing` subscriber (once; a failure means another
/// subscriber is installed and is left alone).
pub fn install() {
    let _ = tracing::subscriber::set_global_default(PythonSink {
        next_span: AtomicU64::new(1),
    });
}
