//! The extension's `tracing` sink: the engine, the worker adapter and the SQL driver report
//! warnings through `tracing` (the `mrsm` binary prints them with its subscriber); in the Python
//! process they reach `meerschaum.utils.warnings.warn()` (what v4's `warn()` calls print), and
//! the `debug` events of a `Pipe.sync(debug=True)` reach `meerschaum.utils.debug.dprint()`.
//!
//! Only events from the Meerschaum crates are forwarded (`meerschaum_*` targets); dependency
//! chatter (`sqlx`, `hyper`, ...) never reaches the user. The sink installs itself once, as the
//! global default, unless another subscriber already is one (`cargo test` harnesses).

use std::fmt::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pyo3::prelude::*;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
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

/// Span ids this sink hands out itself carry this bit; the CLI subscriber's (a `tracing`
/// registry's slab indexes) never do. A span's later calls (`enter`, `exit`, `clone_span`,
/// `try_close`, ...) are routed by its id, not by the current mode, so a span opened while the CLI
/// sink was in charge still closes there after `_native.main` returned to a Python caller.
const OWN_SPAN: u64 = 1 << 63;

/// The CLI subscriber of a span id which came from it, if there is one.
fn cli_span(id: &Id) -> Option<&'static (dyn Subscriber + Send + Sync)> {
    if id.into_u64() & OWN_SPAN != 0 {
        return None;
    }
    CLI_SINK.get()?.as_deref()
}

/// What `_native.main` (the `mrsm` console script) runs on instead of Python's `warn()`: the
/// binary's own behavior, which prints `tracing` events (to stderr) only when `RUST_LOG` asks for
/// them. `None` once CLI mode is on and `RUST_LOG` is unset: every event is dropped.
type CliSink = Option<Box<dyn Subscriber + Send + Sync>>;

static CLI_SINK: OnceLock<CliSink> = OnceLock::new();
static CLI_ACTIVE: AtomicBool = AtomicBool::new(false);

/// The CLI sink while `_native.main` is running, else `None` (the Python sink is in charge).
fn cli_mode() -> Option<&'static CliSink> {
    CLI_ACTIVE
        .load(Ordering::SeqCst)
        .then(|| CLI_SINK.get())
        .flatten()
}

/// Switch the sink to the standalone binary's behavior (`_native.main`, the `mrsm` console
/// script) until [`leave_cli_mode`]. Idempotent.
pub fn enter_cli_mode() {
    CLI_SINK.get_or_init(|| {
        std::env::var_os("RUST_LOG").map(|_| {
            Box::new(
                tracing_subscriber::fmt()
                    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                    .with_writer(std::io::stderr)
                    .finish(),
            ) as Box<dyn Subscriber + Send + Sync>
        })
    });
    CLI_ACTIVE.store(true, Ordering::SeqCst);
}

/// Put the Python sink back (`_native.main` returned to a Python caller).
pub fn leave_cli_mode() {
    CLI_ACTIVE.store(false, Ordering::SeqCst);
}

impl Subscriber for PythonSink {
    /// Whether a call site is enabled changes at run time (`DEBUG_SYNCS`, CLI mode, `RUST_LOG`), so
    /// the default (`always` / `never`, remembered per call site) would keep an answer given the
    /// first time a site was hit: a site first reached inside `_native.main` without `RUST_LOG`
    /// is "never" for good, and its warnings would no longer reach `warn()` in the REPL afterwards.
    fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        if let Some(cli) = cli_mode() {
            return cli.as_ref().is_some_and(|sub| sub.enabled(metadata));
        }
        if !metadata.target().starts_with("meerschaum_") {
            return false;
        }
        // Probes of the engine's own state pipes (`mrsm:` keys) on an API instance have no v4
        // counterpart; like in the CLI, Python users never see their failures.
        if metadata.target() == meerschaum_connector_api::client::warnings::STATE_PIPE_TARGET {
            return false;
        }
        match *metadata.level() {
            Level::ERROR | Level::WARN => true,
            Level::INFO | Level::DEBUG => DEBUG_SYNCS.load(Ordering::SeqCst) > 0,
            Level::TRACE => false,
        }
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        if let Some(Some(cli)) = cli_mode() {
            return cli.new_span(attributes);
        }
        Id::from_u64(OWN_SPAN | self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        if let Some(cli) = cli_span(span) {
            cli.record(span, values);
        }
    }

    fn record_follows_from(&self, span: &Id, follows: &Id) {
        if let Some(cli) = cli_span(span) {
            cli.record_follows_from(span, follows);
        }
    }

    fn event(&self, event: &Event<'_>) {
        if let Some(cli) = cli_mode() {
            if let Some(cli) = cli {
                cli.event(event);
            }
            return;
        }
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

    fn enter(&self, span: &Id) {
        if let Some(cli) = cli_span(span) {
            cli.enter(span);
        }
    }

    fn exit(&self, span: &Id) {
        if let Some(cli) = cli_span(span) {
            cli.exit(span);
        }
    }

    fn clone_span(&self, id: &Id) -> Id {
        match cli_span(id) {
            Some(cli) => cli.clone_span(id),
            None => id.clone(),
        }
    }

    /// Forwarded, so the CLI subscriber's registry releases the spans of `start api` with
    /// `RUST_LOG` set. The sink's own spans are not tracked: they are closed as soon as the last
    /// handle is dropped.
    fn try_close(&self, id: Id) -> bool {
        match cli_span(&id) {
            Some(cli) => cli.try_close(id),
            None => true,
        }
    }
}

/// Install the sink as the global `tracing` subscriber (once; a failure means another
/// subscriber is installed and is left alone).
pub fn install() {
    let _ = tracing::subscriber::set_global_default(PythonSink {
        next_span: AtomicU64::new(1),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::callsite::Callsite;

    #[test]
    fn a_callsite_is_asked_every_time() {
        // Answers change with `DEBUG_SYNCS` and CLI mode: `always` / `never` would be cached.
        static CALLSITE: tracing::callsite::DefaultCallsite =
            tracing::callsite::DefaultCallsite::new(&tracing::metadata::Metadata::new(
                "test",
                "meerschaum_py::logging::tests",
                Level::WARN,
                None,
                None,
                None,
                tracing::field::FieldSet::new(&[], tracing::callsite::Identifier(&CALLSITE)),
                tracing::metadata::Kind::EVENT,
            ));
        let sink = PythonSink {
            next_span: AtomicU64::new(1),
        };
        assert!(sink.register_callsite(CALLSITE.metadata()).is_sometimes());
    }

    #[test]
    fn the_sinks_own_spans_never_reach_the_cli_subscriber() {
        let sink = PythonSink {
            next_span: AtomicU64::new(1),
        };
        let own = Id::from_u64(OWN_SPAN | 7);
        assert!(cli_span(&own).is_none());
        assert_eq!(sink.clone_span(&own), own);
        assert!(sink.try_close(own.clone()));
        // Without a CLI sink installed there is nothing to forward a registry id to either.
        let theirs = Id::from_u64(3);
        assert_eq!(sink.clone_span(&theirs), theirs);
        sink.enter(&theirs);
        sink.exit(&theirs);
    }
}
