//! The extension's `tracing` sink: the engine, the worker adapter and the SQL driver report
//! warnings through `tracing` (the `mrsm` binary prints them with its subscriber); in the Python
//! process they reach `meerschaum.utils.warnings.warn()` (what v4's `warn()` calls print), and
//! the debug output (`dprint!` and the other `DEBUG` / `INFO` events, see
//! `meerschaum_core::debug`) of any binding called with `debug=True` (or inside
//! `_core.debugging(True)`) reaches `meerschaum.utils.debug.dprint()` with the Rust module and
//! line as its header.
//!
//! Only events from the Meerschaum crates are forwarded (`meerschaum_*` targets); dependency
//! chatter (`sqlx`, `hyper`, ...) never reaches the user. The sink installs itself once, as the
//! global default, unless another subscriber already is one (`cargo test` harnesses).

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pyo3::prelude::*;
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};

struct PythonSink {
    next_span: AtomicU64,
    /// The CLI subscriber when `RUST_LOG` asked for one (see [`CLI_SINK`]). **Span routing rule:**
    /// when it exists it owns *every* span of the process (`new_span`, `enter`, `exit`,
    /// `clone_span`, `try_close` ... all go to it, in Python mode too), so an id is only ever
    /// seen by the subscriber which made it; without it the sink makes counter ids and no span
    /// call goes anywhere. It exists from [`install`] on, before any span can be created, so the
    /// rule cannot change during the process's life (no mode, no bit of the id decides anything).
    /// No Meerschaum crate opens spans in Python mode; this keeps the ones in `RUST_LOG` runs
    /// (`start api`) balanced.
    spans: Option<&'static (dyn Subscriber + Send + Sync)>,
}

impl PythonSink {
    fn new(spans: Option<&'static (dyn Subscriber + Send + Sync)>) -> PythonSink {
        PythonSink {
            next_span: AtomicU64::new(1),
            spans,
        }
    }
}

/// What `_native.main` (the `mrsm` console script) runs on instead of Python's `warn()`: the
/// binary's own subscriber (`meerschaum_cli::debug_log`), which prints debug output while
/// `--debug` is on and `tracing` events when `RUST_LOG` asks for them, both on stderr.
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

/// The CLI sink (built once, from `RUST_LOG` as it is when the sink is installed or first needed).
fn cli_sink() -> &'static CliSink {
    CLI_SINK.get_or_init(|| Some(meerschaum_cli::debug_log::subscriber()))
}

/// Switch the sink to the standalone binary's behavior (`_native.main`, the `mrsm` console
/// script) until [`leave_cli_mode`]. Idempotent.
pub fn enter_cli_mode() {
    cli_sink();
    CLI_ACTIVE.store(true, Ordering::SeqCst);
}

/// Put the Python sink back (`_native.main` returned to a Python caller).
pub fn leave_cli_mode() {
    CLI_ACTIVE.store(false, Ordering::SeqCst);
}

impl Subscriber for PythonSink {
    /// Whether a call site is enabled changes at run time (the debug switch, CLI mode), so
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
        if !meerschaum_core::debug::is_meerschaum_target(metadata.target()) {
            return false;
        }
        // Probes of the engine's own state pipes (`mrsm:` keys) on an API instance have no v4
        // counterpart; like in the CLI, Python users never see their failures.
        if metadata.target() == meerschaum_connector_api::client::warnings::STATE_PIPE_TARGET {
            return false;
        }
        match *metadata.level() {
            Level::ERROR | Level::WARN => true,
            Level::INFO | Level::DEBUG => meerschaum_core::debug::enabled(),
            Level::TRACE => false,
        }
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        match self.spans {
            Some(spans) => spans.new_span(attributes),
            None => Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed)),
        }
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        if let Some(spans) = self.spans {
            spans.record(span, values);
        }
    }

    fn record_follows_from(&self, span: &Id, follows: &Id) {
        if let Some(spans) = self.spans {
            spans.record_follows_from(span, follows);
        }
    }

    fn event(&self, event: &Event<'_>) {
        if let Some(cli) = cli_mode() {
            if let Some(cli) = cli {
                cli.event(event);
            }
            return;
        }
        let metadata = event.metadata();
        let level = *metadata.level();
        let message = meerschaum_cli::debug_log::event_message(event);
        let module = metadata.module_path().unwrap_or(metadata.target());
        // `try_attach`: an event from a runtime thread while the interpreter shuts down is
        // dropped (pyo3 0.30's `attach` panics during finalization on Python 3.13+).
        let _ = Python::try_attach(|py| -> PyResult<()> {
            if level <= Level::WARN {
                let warn = py.import("meerschaum.utils.warnings")?.getattr("warn")?;
                let kwargs = pyo3::types::PyDict::new(py);
                kwargs.set_item("stack", false)?;
                warn.call((message,), Some(&kwargs))?;
            } else {
                let dprint = py.import("meerschaum.utils.debug")?.getattr("dprint")?;
                let kwargs = pyo3::types::PyDict::new(py);
                kwargs.set_item("_package", module)?;
                kwargs.set_item("_lineno", metadata.line())?;
                dprint.call((message,), Some(&kwargs))?;
            }
            Ok(())
        });
    }

    fn enter(&self, span: &Id) {
        if let Some(spans) = self.spans {
            spans.enter(span);
        }
    }

    fn exit(&self, span: &Id) {
        if let Some(spans) = self.spans {
            spans.exit(span);
        }
    }

    fn clone_span(&self, id: &Id) -> Id {
        match self.spans {
            Some(spans) => spans.clone_span(id),
            None => id.clone(),
        }
    }

    /// Forwarded, so the CLI subscriber's registry releases the spans of `start api` with
    /// `RUST_LOG` set.
    fn try_close(&self, id: Id) -> bool {
        match self.spans {
            Some(spans) => spans.try_close(id),
            None => true,
        }
    }
}

/// `_core.debugging(on=True)`: a context manager turning the extension's debug output on (when
/// `on`) for the bindings called inside it, as if each had been given `debug=True`. For façade
/// code whose bindings take no `debug` argument.
#[pyclass(module = "meerschaum._core", name = "debugging", frozen)]
pub struct Debugging {
    on: bool,
    /// The active guards (one per `__enter__`, so a re-entered manager stays balanced).
    guards: std::sync::Mutex<Vec<meerschaum_core::debug::DebugGuard>>,
}

#[pymethods]
impl Debugging {
    #[new]
    #[pyo3(signature = (on=true))]
    fn new(on: bool) -> Self {
        Debugging {
            on,
            guards: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        let guard = meerschaum_core::debug::guard(slf.get().on);
        slf.get()
            .guards
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(guard);
        slf
    }

    #[pyo3(signature = (*_args))]
    fn __exit__(&self, _args: &Bound<'_, pyo3::types::PyTuple>) -> bool {
        self.guards.lock().unwrap_or_else(|e| e.into_inner()).pop();
        false
    }
}

/// Install the sink as the global `tracing` subscriber (once; a failure means another
/// subscriber is installed and is left alone).
pub fn install() {
    let _ = tracing::subscriber::set_global_default(PythonSink::new(cli_sink().as_deref()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::callsite::Callsite;

    #[test]
    fn a_callsite_is_asked_every_time() {
        // Answers change with the debug switch and CLI mode: `always` / `never` would be cached.
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
        let sink = PythonSink::new(None);
        assert!(sink.register_callsite(CALLSITE.metadata()).is_sometimes());
    }

    use std::sync::atomic::AtomicUsize;

    /// A subscriber which hands out ids with every bit pattern (the top one included) and counts
    /// the calls it gets.
    struct Recorder {
        calls: AtomicUsize,
    }

    const PACKED: u64 = (1 << 63) | 42;

    impl Subscriber for Recorder {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Id::from_u64(PACKED)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, _: &Event<'_>) {}
        fn enter(&self, id: &Id) {
            assert_eq!(id.into_u64(), PACKED);
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        fn exit(&self, id: &Id) {
            assert_eq!(id.into_u64(), PACKED);
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        fn clone_span(&self, id: &Id) -> Id {
            self.calls.fetch_add(1, Ordering::SeqCst);
            id.clone()
        }
        fn try_close(&self, id: Id) -> bool {
            assert_eq!(id.into_u64(), PACKED);
            self.calls.fetch_add(1, Ordering::SeqCst);
            true
        }
    }

    #[test]
    fn without_a_cli_subscriber_no_span_call_goes_anywhere() {
        let sink = PythonSink::new(None);
        let id = Id::from_u64(7);
        assert_eq!(sink.clone_span(&id), id);
        assert!(sink.try_close(id.clone()));
        sink.enter(&id);
        sink.exit(&id);
    }

    #[test]
    fn a_cli_subscriber_owns_every_span_whatever_the_bits_of_its_ids() {
        // The CLI subscriber's ids may use any bit: nothing here decides by the id.
        let recorder: &'static Recorder = Box::leak(Box::new(Recorder {
            calls: AtomicUsize::new(0),
        }));
        let sink = PythonSink::new(Some(recorder));
        let id = Id::from_u64(PACKED);
        let id = sink.clone_span(&id);
        sink.enter(&id);
        sink.exit(&id);
        assert!(sink.try_close(id));
        assert_eq!(recorder.calls.load(Ordering::SeqCst), 4);
    }
}
