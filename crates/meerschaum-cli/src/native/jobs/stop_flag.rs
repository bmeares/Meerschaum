//! The process-wide stop flag (`utils/threading.py` `stop_requested`): who owns the process's
//! signals, the per-call flag of an embedded host, and sleeping until a stop is requested.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

static STOP: OnceLock<Arc<AtomicBool>> = OnceLock::new();
static SIGNALS: AtomicUsize = AtomicUsize::new(0);
static HANDLER: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

static EMBEDDED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Whether [`install_stop_handler`] turned SIGINT / SIGTERM into stop requests in this process.
pub fn stop_handler_installed() -> bool {
    HANDLER.load(Ordering::SeqCst) || crate::shell::interrupt::watching()
}

static OWN_SIGNALS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Whether this process embeds the dispatcher (the Python extension, outside `mrsm`'s own run).
pub fn is_embedded() -> bool {
    EMBEDDED.load(Ordering::SeqCst)
}

/// The running action handles SIGINT / SIGTERM itself (the servers: `start api` / `start mcp`
/// wait on them for a graceful shutdown), so nothing else may take them, the progress spinner's
/// guard included.
pub fn own_signals() {
    OWN_SIGNALS.store(true, Ordering::SeqCst);
}

/// Whether the running action handles the signals itself (see [`own_signals`]).
pub fn signals_owned() -> bool {
    OWN_SIGNALS.load(Ordering::SeqCst)
}

/// Give the signals back after [`own_signals`], for an action that owns them only while it
/// runs (`upgrade postgres` and friends: a shell goes on afterwards).
pub fn disown_signals() {
    OWN_SIGNALS.store(false, Ordering::SeqCst);
}

/// Mark this process as embedding the dispatcher (the Python extension). The process-wide
/// SIGINT / SIGTERM handler of [`install_stop_handler`], which ends the process on a second
/// signal, belongs to the `mrsm` binary: an embedding host is never given one, and sets
/// [`stop_requested_flag`] for the call it runs instead.
pub fn set_embedded(embedded: bool) {
    EMBEDDED.store(embedded, Ordering::SeqCst);
}

/// `stop_requested()`: set when the process receives SIGINT / SIGTERM (see
/// [`install_stop_handler`]).
///
/// An embedding host runs each call under [`with_call_stop`]: the loops of that call (which read
/// this on the thread that runs it) then see the call's own flag, so stopping one call neither
/// reaches nor is reset by another. Without one (the `mrsm` binary) it is the process-wide flag.
pub fn stop_requested_flag() -> Arc<AtomicBool> {
    CALL_STOP
        .with(|slot| slot.borrow().clone())
        .unwrap_or_else(process_stop_flag)
}

fn process_stop_flag() -> Arc<AtomicBool> {
    STOP.get_or_init(|| Arc::new(AtomicBool::new(false)))
        .clone()
}

thread_local! {
    static CALL_STOP: std::cell::RefCell<Option<Arc<AtomicBool>>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `f` with `flag` as this thread's stop flag (see [`stop_requested_flag`]).
pub fn with_call_stop<R>(flag: Arc<AtomicBool>, f: impl FnOnce() -> R) -> R {
    let previous = CALL_STOP.with(|slot| slot.borrow_mut().replace(flag));
    let result = f();
    CALL_STOP.with(|slot| *slot.borrow_mut() = previous);
    result
}

/// Forget that [`install_stop_handler`] ran, and any signal it counted. The handler is a task of the
/// session's runtime, which ends with the session: a host which runs the CLI more than once in
/// one process (the Python extension's `_native.main`) calls this after each run so the next run
/// installs its own.
pub fn release_stop_handler() {
    HANDLER.store(false, Ordering::SeqCst);
    reset_stop_request();
}

/// Begin a new interactive command after any previous command has finished stopping.
pub fn reset_stop_request() {
    SIGNALS.store(0, Ordering::SeqCst);
    process_stop_flag().store(false, Ordering::SeqCst);
}

/// Turn the first SIGINT / SIGTERM into a stop request for long-running actions (laps,
/// schedules, following logs); a second signal exits immediately (v4 raises `KeyboardInterrupt`
/// at the next checkpoint). In a shell the second signal cancels the action instead of ending
/// the shell.
///
/// The signals are handled by the process-wide watcher of [`crate::shell::interrupt`]: this marks
/// the running action as one that reads [`stop_requested_flag`] itself, so that the first Ctrl-C
/// sets the flag and drops nothing (an action which never calls this is cancelled outright by the
/// first Ctrl-C).
pub fn install_stop_handler(session: &super::Session) {
    // An embedding host (the Python extension) owns the process's signals: it polls them
    // itself and sets the stop flag for the call it runs (see [`set_embedded`]).
    if EMBEDDED.load(Ordering::SeqCst) {
        return;
    }
    HANDLER.store(true, Ordering::SeqCst);
    crate::shell::interrupt::install(Some(session));
    crate::shell::interrupt::mark_cooperative();
}

/// Sleep up to `seconds`, waking early on a stop request. Returns whether a stop was requested.
pub fn sleep_unless_stopped(seconds: f64) -> bool {
    let stop = stop_requested_flag();
    let deadline = Instant::now() + Duration::from_secs_f64(seconds.max(0.0));
    while Instant::now() < deadline {
        if stop.load(Ordering::SeqCst) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50).min(deadline - Instant::now()));
    }
    stop.load(Ordering::SeqCst)
}
