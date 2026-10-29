//! Ctrl-C while an action runs (wave 18, decision 1).
//!
//! The terminal sends SIGINT to the whole foreground process group: `mrsm`, its Python workers
//! and every program it started. One watcher thread (`signal-hook`, a safe API) turns it into
//! what the running action can use, so that Ctrl-C cancels *the action* and never the process
//! (the shell goes back to its prompt; `mrsm <action>` exits 130):
//!
//! | Regime (decided per signal) | First Ctrl-C | Second Ctrl-C |
//! |---|---|---|
//! | idle at the shell prompt | ignored (the editor reads the key) | |
//! | idle, not in a shell | the default action (the process dies) | |
//! | an action runs | cooperative cancellation: [`Session::block_on`](crate::native::session::Session::block_on) drops its future and unwinds with [`Interrupted`]; [`check`] does the same between awaits; the dispatcher ([`guarded`]) turns the unwind into `Interrupted.` | `mrsm <action>`: exit 130 at once; in a shell, only when the action is still running two seconds later |
//! | the action handles the stop flag itself (laps, `--loop`, `sync --loop`, followed logs: [`mark_cooperative`]) | sets the stop flag, nothing is dropped | exit 130 (`mrsm <action>`) or forced cancellation as above (shell) |
//! | a foreground program runs (`sh`, `python`: [`ChildScope`]) | nothing: the terminal already delivered it to the program's own process group | within two seconds: SIGKILL the program (not an interactive REPL) |
//! | a Python worker runs the action (`@make_action`) | nothing (its call raises `KeyboardInterrupt` and returns) | within two seconds: the call is dropped, which kills the worker |
//!
//! Hosts which own their signals keep them: the Python extension's `entry()` (it is *embedded*),
//! servers (`own_signals`), and job workers (no unwinding: their stop handler is the whole story, as
//! before). SIGTERM keeps its old meaning (a graceful stop where an action handles it, else the
//! default action).
//!
//! The unwind is [`std::panic::resume_unwind`] (no panic message, no traceback) and is only
//! ever caught by [`guarded`]; the panic hook stays silent for panics that follow an interrupt
//! (a scoped thread that propagates ours), and delegates for everything else.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use meerschaum_core::SuccessTuple;
use tokio::sync::Notify;

use crate::native::jobs;

/// The window in which a second Ctrl-C is "the second".
pub const SECOND_PRESS_WINDOW: Duration = Duration::from_secs(2);

/// The exit status of a process ended by Ctrl-C (`128 + SIGINT`).
pub const EXIT_CODE: i32 = 130;

/// The text printed when an action was interrupted.
pub const MESSAGE: &str = "Interrupted.";

/// The payload [`Session::block_on`](crate::native::Session::block_on) and [`check`] unwind with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interrupted;

/// Actions nested in the running one (`0`: none is running).
static DEPTH: AtomicUsize = AtomicUsize::new(0);
/// A shell waits at its prompt between actions: an idle SIGINT is the editor's, not ours.
static IDLE_IGNORES: AtomicBool = AtomicBool::new(false);
/// Whether Ctrl-C may unwind the outermost running action (not in a job worker).
static UNWINDING: AtomicBool = AtomicBool::new(false);
/// The running action reads the stop flag itself.
static COOPERATIVE: AtomicBool = AtomicBool::new(false);
/// Foreground programs / worker calls the action waits for.
static CHILDREN: AtomicUsize = AtomicUsize::new(0);
/// The pid of the program [`CHILDREN`] counts (`0`: a worker call or several).
static CHILD_PID: AtomicU32 = AtomicU32::new(0);
/// The program may be killed by a second Ctrl-C (an interactive REPL may not).
static CHILD_KILLABLE: AtomicBool = AtomicBool::new(false);
/// A Ctrl-C asked the running action to stop (it unwinds at its next await point).
static REQUESTED: AtomicBool = AtomicBool::new(false);
/// A second Ctrl-C forces the running action to unwind although it handles the signal itself.
static FORCED: AtomicBool = AtomicBool::new(false);
/// A Ctrl-C reached the action (in any regime that is not the cooperative one).
static SEEN: AtomicBool = AtomicBool::new(false);
/// An unwind was turned into `Interrupted.`.
static UNWOUND: AtomicBool = AtomicBool::new(false);
/// Ctrl-Cs counted in the current window.
static PRESSES: AtomicUsize = AtomicUsize::new(0);
static FIRST_PRESS: Mutex<Option<Instant>> = Mutex::new(None);
/// The watcher is running.
static WATCHING: AtomicBool = AtomicBool::new(false);
/// Wakes the futures [`interruptible`] wraps.
static WAKE: Notify = Notify::const_new();

fn lock_first() -> std::sync::MutexGuard<'static, Option<Instant>> {
    FIRST_PRESS.lock().unwrap_or_else(|e| e.into_inner())
}

/// Whether the watcher is running (SIGINT / SIGTERM have a handler in this process).
pub fn watching() -> bool {
    WATCHING.load(Ordering::SeqCst)
}

/// An action runs under [`guarded`] and Ctrl-C may unwind it.
pub fn cancellable() -> bool {
    owns_the_state()
        && DEPTH.load(Ordering::SeqCst) > 0
        && UNWINDING.load(Ordering::SeqCst)
        && !jobs::is_embedded()
        && !std::thread::panicking()
}

/// The state above is process-wide, which is right for a process that runs one action at a time.
/// The unit tests of this crate run in parallel threads of one process: there only the thread that
/// holds [`test_serial`] (an interrupt test, or an action under [`guarded`]) sees the state, so a
/// test that sets a request cannot unwind the print of an unrelated test.
#[cfg(not(test))]
fn owns_the_state() -> bool {
    true
}

#[cfg(test)]
fn owns_the_state() -> bool {
    test_serial::held()
}

/// One turn at the process-wide interrupt state, for the unit tests (see [`owns_the_state`]).
#[cfg(test)]
pub(crate) mod test_serial {
    use std::cell::Cell;
    use std::sync::{Mutex, MutexGuard};

    static LOCK: Mutex<()> = Mutex::new(());

    thread_local! {
        static HELD: Cell<bool> = const { Cell::new(false) };
    }

    /// Whether this thread has its turn.
    pub(crate) fn held() -> bool {
        HELD.with(Cell::get)
    }

    /// The turn, until dropped (a thread that already has it takes no second one).
    pub(crate) struct Turn(Option<MutexGuard<'static, ()>>);

    pub(crate) fn take() -> Turn {
        if held() {
            return Turn(None);
        }
        let guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        HELD.with(|held| held.set(true));
        Turn(Some(guard))
    }

    impl Drop for Turn {
        fn drop(&mut self) {
            if self.0.is_some() {
                HELD.with(|held| held.set(false));
            }
        }
    }
}

/// A cancellation was asked for and the running action should unwind now.
fn wanted() -> bool {
    cancellable() && (REQUESTED.load(Ordering::SeqCst) || FORCED.load(Ordering::SeqCst))
}

/// Whether Ctrl-C asked the running action to stop (any regime).
pub fn requested() -> bool {
    REQUESTED.load(Ordering::SeqCst) || FORCED.load(Ordering::SeqCst)
}

/// The running action reads [`jobs::stop_requested_flag`] itself: the first Ctrl-C sets that flag
/// and drops nothing (called by `install_stop_handler`).
pub fn mark_cooperative() {
    COOPERATIVE.store(true, Ordering::SeqCst);
}

/// Unwind with [`Interrupted`] when Ctrl-C asked the running action to stop. Call it between
/// the steps of a long loop; it does nothing off the action's own work (not running, embedded,
/// a job worker, already unwinding).
pub fn check() {
    if wanted() {
        resume_unwind(Box::new(Interrupted));
    }
}

/// `fut`, cancelled (dropped) at its next await point once Ctrl-C asked the running action to stop.
///
/// A hand-written future over the *boxed* `fut` (not an `async fn`): the layout of `fut` is then
/// never nested inside the wrapper's, so the release build's type-layout queries, which already
/// reach the default recursion limit for the deepest futures of the engine views, do not get any
/// deeper (an `async fn` wrapper tipped `meerschaum-cli` and `meerschaum-py` over it).
pub fn interruptible<F: Future>(fut: F) -> Interruptible<F> {
    Interruptible {
        fut: Box::pin(fut),
        woken: Box::pin(WAKE.notified()),
    }
}

/// The future of [`interruptible`]: `Ok(output)`, or `Err(Interrupted)` once a cancellation is asked.
pub struct Interruptible<F: Future> {
    fut: std::pin::Pin<Box<F>>,
    woken: std::pin::Pin<Box<tokio::sync::futures::Notified<'static>>>,
}

impl<F: Future> Future for Interruptible<F> {
    type Output = Result<F::Output, Interrupted>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll;
        // Both fields are `Pin<Box<_>>`: `Interruptible` is `Unpin`.
        let this = &mut *self;
        if wanted() {
            return Poll::Ready(Err(Interrupted));
        }
        if let Poll::Ready(output) = this.fut.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        loop {
            match this.woken.as_mut().poll(cx) {
                Poll::Pending => {
                    // Registered for the next wake-up. A Ctrl-C between the check above and the
                    // registration set the flag first and notified nobody: read it again.
                    return if wanted() {
                        Poll::Ready(Err(Interrupted))
                    } else {
                        Poll::Pending
                    };
                }
                Poll::Ready(()) => {
                    this.woken = Box::pin(WAKE.notified());
                    if wanted() {
                        return Poll::Ready(Err(Interrupted));
                    }
                }
            }
        }
    }
}

/// Start the watcher thread (once per process; it stays: unregistering a `signal-hook` handler
/// would leave it installed without restoring the default action). Does nothing in an embedding
/// host, which owns the signals.
#[cfg(unix)]
pub fn install(_session: Option<&crate::native::Session>) {
    use signal_hook::consts::{SIGINT, SIGTERM};
    use signal_hook::iterator::SignalsInfo;
    use signal_hook::iterator::exfiltrator::WithOrigin;
    static START: std::sync::Once = std::sync::Once::new();

    if jobs::is_embedded() {
        return;
    }
    START.call_once(|| {
        let Ok(mut signals) = SignalsInfo::<WithOrigin>::new([SIGINT, SIGTERM]) else {
            return;
        };
        install_panic_hook();
        WATCHING.store(true, Ordering::SeqCst);
        std::thread::spawn(move || {
            for origin in signals.forever() {
                if origin.signal == SIGINT {
                    // Sent by a process (`kill -INT`): the terminal did not give it to the
                    // programs of the foreground process group.
                    on_sigint(origin.process.is_none());
                } else {
                    on_sigterm(origin.signal);
                }
            }
        });
    });
}

/// Start the Ctrl-C task on the session's runtime (once per process).
#[cfg(not(unix))]
pub fn install(session: Option<&crate::native::Session>) {
    static START: std::sync::Once = std::sync::Once::new();
    let Some(session) = session else {
        return;
    };
    if jobs::is_embedded() {
        return;
    }
    START.call_once(|| {
        install_panic_hook();
        WATCHING.store(true, Ordering::SeqCst);
        session.spawn(async {
            while tokio::signal::ctrl_c().await.is_ok() {
                on_sigint(true);
            }
        });
    });
}

/// The panic hook: silent for the panics an interrupt causes (a scoped thread re-panicking with
/// its own message), the previous hook for every other panic.
fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if info.payload().is::<Interrupted>() || (cancellable() && requested()) {
            return;
        }
        previous(info);
    }));
}

/// Print `Interrupted.` and end the process with status 130 (the cancellation did not work).
fn exit_now() -> ! {
    crate::format::progress::abandon();
    crate::tui::app::restore_for_exit();
    {
        use std::io::Write;
        let _ = writeln!(std::io::stderr(), "\n{MESSAGE}");
    }
    std::process::exit(EXIT_CODE);
}

/// The default action of a signal (the process ends "killed by the signal"), after erasing the
/// spinner line.
#[cfg(unix)]
fn die_by_default(signal: i32) {
    crate::format::progress::clear_for_signal();
    crate::tui::app::restore_for_exit();
    let _ = signal_hook::low_level::emulate_default_handler(signal);
}

/// What an unhandled SIGINT does: the process ends (killed by the signal on Unix, status 130
/// elsewhere), after the spinner line is erased.
fn end_like_default_sigint() {
    #[cfg(unix)]
    die_by_default(signal_hook::consts::SIGINT);
    #[cfg(not(unix))]
    exit_now()
}

/// One more Ctrl-C: the count inside the current window (a press after the window starts a new one)
/// and the time since the window's first press.
fn press(window: Option<Duration>) -> (usize, Duration) {
    let now = Instant::now();
    let mut first = lock_first();
    if let (Some(started), Some(window)) = (*first, window)
        && now.duration_since(started) > window
    {
        *first = None;
        PRESSES.store(0, Ordering::SeqCst);
    }
    let started = *first.get_or_insert(now);
    let count = PRESSES.fetch_add(1, Ordering::SeqCst) + 1;
    (count, now.duration_since(started))
}

fn in_shell() -> bool {
    IDLE_IGNORES.load(Ordering::SeqCst)
}

/// SIGINT, from the watcher. `from_terminal`: the terminal sent it (Ctrl-C), so every program of the
/// foreground process group has it too; otherwise a process sent it to this one only.
fn on_sigint(from_terminal: bool) {
    // The owner of the signals (the extension's host, a server waiting on them) handles it.
    if jobs::is_embedded() || jobs::signals_owned() {
        return;
    }
    if DEPTH.load(Ordering::SeqCst) == 0 {
        // Between actions: a shell's editor reads Ctrl-C as a key (the terminal is raw), so a
        // signal here is a stray one; anywhere else the process ends as it always did.
        if in_shell() {
            crate::format::progress::clear_for_signal();
            return;
        }
        end_like_default_sigint();
        return;
    }
    if COOPERATIVE.load(Ordering::SeqCst) {
        jobs::stop_requested_flag().store(true, Ordering::SeqCst);
        let (count, since_first) = press(None);
        if count >= 2 {
            if in_shell() && since_first < SECOND_PRESS_WINDOW {
                // The action is already stopping on the flag: give it the window, then force.
                return;
            }
            if in_shell() {
                FORCED.store(true, Ordering::SeqCst);
                WAKE.notify_waiters();
            } else {
                exit_now();
            }
        }
        return;
    }
    if CHILDREN.load(Ordering::SeqCst) > 0 {
        // The terminal gave the program (or the worker) the signal; this process waits. A signal
        // sent to this process alone is forwarded (a worker call is dropped, which kills it).
        SEEN.store(true, Ordering::SeqCst);
        let pid = CHILD_PID.load(Ordering::SeqCst);
        if !from_terminal {
            if pid == 0 {
                FORCED.store(true, Ordering::SeqCst);
                WAKE.notify_waiters();
            } else {
                forward_sigint(pid);
            }
        }
        let (count, _) = press(Some(SECOND_PRESS_WINDOW));
        if count >= 2 {
            if pid == 0 {
                // A worker call: dropping the call kills its worker.
                FORCED.store(true, Ordering::SeqCst);
                WAKE.notify_waiters();
            } else if CHILD_KILLABLE.load(Ordering::SeqCst) {
                kill_program(pid);
            }
            PRESSES.store(0, Ordering::SeqCst);
            *lock_first() = None;
        }
        return;
    }
    SEEN.store(true, Ordering::SeqCst);
    jobs::stop_requested_flag().store(true, Ordering::SeqCst);
    if !UNWINDING.load(Ordering::SeqCst) {
        // A job worker: the stop flag is the stop (as before), nothing else handles it.
        end_like_default_sigint();
        return;
    }
    REQUESTED.store(true, Ordering::SeqCst);
    WAKE.notify_waiters();
    let (count, since_first) = press(None);
    if count >= 2 && (!in_shell() || since_first >= SECOND_PRESS_WINDOW) {
        // Not even the cancellation got the action out (a blocking call): end the process.
        exit_now();
    }
}

#[cfg(unix)]
fn signal_program(pid: u32, signal: rustix::process::Signal) {
    if let Some(pid) = i32::try_from(pid)
        .ok()
        .and_then(rustix::process::Pid::from_raw)
    {
        let _ = rustix::process::kill_process(pid, signal);
    }
}

#[cfg(unix)]
fn kill_program(pid: u32) {
    signal_program(pid, rustix::process::Signal::KILL);
}

#[cfg(unix)]
fn forward_sigint(pid: u32) {
    signal_program(pid, rustix::process::Signal::INT);
}

#[cfg(not(unix))]
fn kill_program(_pid: u32) {}

#[cfg(not(unix))]
fn forward_sigint(_pid: u32) {}

/// SIGTERM, from the watcher: a graceful stop where the action handles it, else the default
/// action; a second one ends the process.
#[cfg(unix)]
fn on_sigterm(signal: i32) {
    if jobs::is_embedded() || jobs::signals_owned() {
        return;
    }
    if DEPTH.load(Ordering::SeqCst) > 0 && COOPERATIVE.load(Ordering::SeqCst) {
        crate::format::progress::clear_for_signal();
        jobs::stop_requested_flag().store(true, Ordering::SeqCst);
        if PRESSES.fetch_add(1, Ordering::SeqCst) >= 1 {
            crate::format::progress::abandon();
            crate::tui::app::restore_for_exit();
            std::process::exit(EXIT_CODE);
        }
        return;
    }
    die_by_default(signal);
}

/// What one top-level action changes in the process state, undone when it ends.
struct Running {
    outermost: bool,
    owned_before: bool,
}

impl Running {
    fn begin(unwinding: bool) -> Running {
        let outermost = DEPTH.fetch_add(1, Ordering::SeqCst) == 0;
        let owned_before = jobs::signals_owned();
        if outermost {
            UNWINDING.store(unwinding, Ordering::SeqCst);
            COOPERATIVE.store(false, Ordering::SeqCst);
            CHILDREN.store(0, Ordering::SeqCst);
            CHILD_PID.store(0, Ordering::SeqCst);
            REQUESTED.store(false, Ordering::SeqCst);
            FORCED.store(false, Ordering::SeqCst);
            SEEN.store(false, Ordering::SeqCst);
            UNWOUND.store(false, Ordering::SeqCst);
            PRESSES.store(0, Ordering::SeqCst);
            *lock_first() = None;
        }
        Running {
            outermost,
            owned_before,
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        DEPTH.fetch_sub(1, Ordering::SeqCst);
        if self.outermost {
            // A server marks the signals as its own for good; the next action starts afresh.
            if !self.owned_before {
                jobs::disown_signals();
            }
            COOPERATIVE.store(false, Ordering::SeqCst);
        }
    }
}

/// Run one action: Ctrl-C cancels it (see the module documentation). `job` is a job worker, whose
/// first Ctrl-C only sets the stop flag.
///
/// An unwind caused by an interrupt ([`Interrupted`], or any panic after the interrupt asked for
/// the cancellation: a scoped thread's) ends here as a failed `Interrupted.` result; every other
/// panic continues.
pub fn guarded(
    session: Option<&crate::native::Session>,
    job: bool,
    run: impl FnOnce() -> SuccessTuple,
) -> SuccessTuple {
    if jobs::is_embedded() {
        return run();
    }
    #[cfg(test)]
    let _turn = test_serial::take();
    install(session);
    let running = Running::begin(!job);
    let result = catch_unwind(AssertUnwindSafe(run));
    let interrupted = match &result {
        Err(payload) => {
            payload.is::<Interrupted>()
                || (UNWINDING.load(Ordering::SeqCst)
                    && (REQUESTED.load(Ordering::SeqCst) || FORCED.load(Ordering::SeqCst)))
        }
        Ok(_) => false,
    };
    drop(running);
    match result {
        Ok(tuple) => tuple,
        Err(_) if interrupted => {
            UNWOUND.store(true, Ordering::SeqCst);
            SuccessTuple::fail(MESSAGE)
        }
        Err(payload) => resume_unwind(payload),
    }
}

/// Whether the command that returned `result` ended because of Ctrl-C: an unwind became
/// `Interrupted.`, or a program / worker the command waited for failed after Ctrl-C reached it.
/// The caller prints [`MESSAGE`] instead of the result (and `mrsm <action>` exits 130). Resets
/// the state: ask once per command.
pub fn take_interrupted(result: &SuccessTuple) -> bool {
    #[cfg(test)]
    let _turn = test_serial::take();
    let unwound = UNWOUND.swap(false, Ordering::SeqCst);
    let seen = SEEN.swap(false, Ordering::SeqCst);
    REQUESTED.store(false, Ordering::SeqCst);
    FORCED.store(false, Ordering::SeqCst);
    unwound || (seen && !result.success)
}

/// The shell between its commands: an idle Ctrl-C is the editor's. Dropping it restores the
/// state of the enclosing action (the shell can run inside `mrsm`'s own action).
pub struct ShellScope {
    depth: usize,
    idle_ignores: bool,
    #[cfg(test)]
    _turn: test_serial::Turn,
}

impl ShellScope {
    /// Enter the shell loop; `interactive` is whether a terminal editor reads the keys.
    pub fn enter(session: Option<&crate::native::Session>, interactive: bool) -> ShellScope {
        #[cfg(test)]
        let turn = test_serial::take();
        install(session);
        ShellScope {
            depth: DEPTH.swap(0, Ordering::SeqCst),
            idle_ignores: IDLE_IGNORES.swap(interactive, Ordering::SeqCst),
            #[cfg(test)]
            _turn: turn,
        }
    }
}

impl Drop for ShellScope {
    fn drop(&mut self) {
        DEPTH.store(self.depth, Ordering::SeqCst);
        IDLE_IGNORES.store(self.idle_ignores, Ordering::SeqCst);
    }
}

/// A program (`sh`, `python`) or a worker call the action waits for: while it lives, Ctrl-C is the
/// program's (the terminal delivered it), and a second one within two seconds kills it.
pub struct ChildScope {
    active: bool,
}

impl ChildScope {
    /// `pid` is the program's (`None` for a worker call); `killable` is `false` for an
    /// interactive REPL, which keeps running however often Ctrl-C is pressed.
    pub fn enter(pid: Option<u32>, killable: bool) -> ChildScope {
        let active = owns_the_state() && DEPTH.load(Ordering::SeqCst) > 0 && !jobs::is_embedded();
        if active {
            CHILD_PID.store(pid.unwrap_or(0), Ordering::SeqCst);
            CHILD_KILLABLE.store(killable, Ordering::SeqCst);
            PRESSES.store(0, Ordering::SeqCst);
            *lock_first() = None;
            CHILDREN.fetch_add(1, Ordering::SeqCst);
        }
        ChildScope { active }
    }
}

impl Drop for ChildScope {
    fn drop(&mut self) {
        if self.active {
            CHILDREN.fetch_sub(1, Ordering::SeqCst);
            CHILD_PID.store(0, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> crate::native::Session {
        let s = crate::native::Session::new();
        s.set_python_worker(false);
        s
    }

    fn reset() {
        DEPTH.store(0, Ordering::SeqCst);
        UNWINDING.store(false, Ordering::SeqCst);
        COOPERATIVE.store(false, Ordering::SeqCst);
        CHILDREN.store(0, Ordering::SeqCst);
        REQUESTED.store(false, Ordering::SeqCst);
        FORCED.store(false, Ordering::SeqCst);
        SEEN.store(false, Ordering::SeqCst);
        UNWOUND.store(false, Ordering::SeqCst);
        PRESSES.store(0, Ordering::SeqCst);
        *lock_first() = None;
    }

    #[test]
    fn nothing_unwinds_outside_a_running_action() {
        let _serial = test_serial::take();
        reset();
        REQUESTED.store(true, Ordering::SeqCst);
        assert!(!cancellable());
        check();
        reset();
    }

    #[test]
    fn a_request_unwinds_the_action_into_an_interrupted_result() {
        let _serial = test_serial::take();
        reset();
        let s = session();
        let result = guarded(Some(&s), false, || {
            REQUESTED.store(true, Ordering::SeqCst);
            check();
            SuccessTuple::success()
        });
        assert_eq!(result, SuccessTuple::fail(MESSAGE));
        assert!(take_interrupted(&result));
        assert!(!take_interrupted(&SuccessTuple::success()), "asked once");
        reset();
    }

    #[test]
    fn block_on_drops_a_pending_future_when_asked_to_stop() {
        let _serial = test_serial::take();
        reset();
        let s = session();
        let dropped = std::sync::Arc::new(AtomicBool::new(false));
        struct Flag(std::sync::Arc<AtomicBool>);
        impl Drop for Flag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let flag = Flag(dropped.clone());
        let started = Instant::now();
        let session = s.clone();
        let result = guarded(Some(&s), false, move || {
            std::thread::spawn(|| {
                std::thread::sleep(Duration::from_millis(150));
                REQUESTED.store(true, Ordering::SeqCst);
                WAKE.notify_waiters();
            });
            session.block_on(async move {
                let _held = flag;
                tokio::time::sleep(Duration::from_secs(60)).await;
            });
            SuccessTuple::success()
        });
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(result, SuccessTuple::fail(MESSAGE));
        assert!(dropped.load(Ordering::SeqCst), "the future was dropped");
        reset();
    }

    #[test]
    fn a_job_worker_is_not_unwound() {
        let _serial = test_serial::take();
        reset();
        let s = session();
        let result = guarded(Some(&s), true, || {
            REQUESTED.store(true, Ordering::SeqCst);
            check();
            SuccessTuple::success()
        });
        assert_eq!(result, SuccessTuple::success());
        reset();
    }

    #[test]
    fn other_panics_continue() {
        let _serial = test_serial::take();
        reset();
        let s = session();
        let caught = catch_unwind(AssertUnwindSafe(|| {
            guarded(Some(&s), false, || std::panic::panic_any("not ours"))
        }));
        assert!(caught.is_err());
        assert_eq!(DEPTH.load(Ordering::SeqCst), 0, "the depth is restored");
        reset();
    }

    #[test]
    fn a_failed_program_after_ctrl_c_is_an_interruption() {
        let _serial = test_serial::take();
        reset();
        SEEN.store(true, Ordering::SeqCst);
        assert!(take_interrupted(&SuccessTuple::fail(
            "Returned exit code: 130"
        )));
        SEEN.store(true, Ordering::SeqCst);
        assert!(
            !take_interrupted(&SuccessTuple::success()),
            "a REPL that ended normally is not an interruption"
        );
        reset();
    }

    #[test]
    fn the_shell_scope_hides_the_running_action_while_idle() {
        let _serial = test_serial::take();
        reset();
        let s = session();
        DEPTH.store(1, Ordering::SeqCst);
        {
            let _scope = ShellScope::enter(Some(&s), true);
            assert_eq!(DEPTH.load(Ordering::SeqCst), 0);
            assert!(in_shell());
        }
        assert_eq!(DEPTH.load(Ordering::SeqCst), 1);
        assert!(!in_shell());
        reset();
    }

    #[test]
    fn presses_outside_the_window_start_over() {
        let _serial = test_serial::take();
        reset();
        let (count, _) = press(Some(Duration::from_millis(50)));
        assert_eq!(count, 1);
        let (count, _) = press(Some(Duration::from_millis(50)));
        assert_eq!(count, 2);
        std::thread::sleep(Duration::from_millis(80));
        let (count, _) = press(Some(Duration::from_millis(50)));
        assert_eq!(count, 1, "a press after the window is a first press");
        reset();
    }
}
