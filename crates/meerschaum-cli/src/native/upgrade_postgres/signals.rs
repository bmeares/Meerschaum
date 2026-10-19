//! Signals while `upgrade postgres`, `backup postgres` and `restore postgres` run.
//!
//! Every `docker` child of these commands runs in its own process group, so the terminal's
//! Ctrl-C (SIGINT), a closed terminal (SIGHUP) or a `kill` (SIGTERM) reach only `mrsm`. What a
//! signal does depends on the phase of the command:
//!
//! - [`Phase::Armed`] (a step runs): a stop request. The running step kills its child and the
//!   command puts things back; further signals change nothing.
//! - [`Phase::Shielded`] (things are being put back): ignored, with a notice (stopping then would
//!   leave them half-done).
//! - [`Phase::Prompt`] (a question waits for its answer; nothing runs then): the default action,
//!   which ends the process.
//! - [`Phase::Idle`] (none of these commands runs): the default action, unless another action
//!   owns the signals (as before these commands ran).
//!
//! This is Unix: on Windows the jobs stop handler turns the first Ctrl-C into a stop request and
//! a second one ends the process. An embedding host (the Python extension) owns its signals and
//! sets the call's stop flag itself.

use std::sync::atomic::{AtomicI32, AtomicU8, Ordering};

use crate::dispatch::Runtime;
use crate::native::jobs;

/// What a signal does now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Idle,
    Prompt,
    Armed,
    Shielded,
}

impl Phase {
    fn code(self) -> u8 {
        match self {
            Phase::Idle => 0,
            Phase::Prompt => 1,
            Phase::Armed => 2,
            Phase::Shielded => 3,
        }
    }

    fn from_code(code: u8) -> Phase {
        match code {
            1 => Phase::Prompt,
            2 => Phase::Armed,
            3 => Phase::Shielded,
            _ => Phase::Idle,
        }
    }
}

static PHASE: AtomicU8 = AtomicU8::new(0);

/// The last signal that asked to stop (0: none, or not known on this platform).
static LAST_SIGNAL: AtomicI32 = AtomicI32::new(0);

pub(crate) fn phase() -> Phase {
    Phase::from_code(PHASE.load(Ordering::SeqCst))
}

fn set_phase(phase: Phase) -> Phase {
    Phase::from_code(PHASE.swap(phase.code(), Ordering::SeqCst))
}

/// Whether a stop was requested while a step runs.
pub(crate) fn stop_requested() -> bool {
    phase() == Phase::Armed && jobs::stop_requested_flag().load(Ordering::SeqCst)
}

/// How the stop was asked for, for the messages.
pub(crate) fn stop_reason() -> &'static str {
    #[cfg(unix)]
    {
        match LAST_SIGNAL.load(Ordering::SeqCst) {
            signal_hook::consts::SIGHUP => "the terminal was closed (SIGHUP)",
            signal_hook::consts::SIGTERM => "SIGTERM",
            _ => "Ctrl-C",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = LAST_SIGNAL.load(Ordering::SeqCst);
        "Ctrl-C"
    }
}

/// Run `f` in `phase`, then go back to the phase before.
pub(crate) fn with_phase<T>(phase: Phase, f: impl FnOnce() -> T) -> T {
    let previous = set_phase(phase);
    let result = f();
    set_phase(previous);
    result
}

/// Run `f` (a question) with the default action of the signals: nothing runs at a question, so
/// nothing needs putting back when it ends the process.
pub(crate) fn prompt<T>(f: impl FnOnce() -> T) -> T {
    with_phase(Phase::Prompt, f)
}

/// Run `f` (putting things back) with the signals ignored.
pub(crate) fn shielded<T>(f: impl FnOnce() -> T) -> T {
    with_phase(Phase::Shielded, f)
}

/// One of these commands, from its first step to its end: the signals are this module's.
pub(crate) struct Session {
    previous: Phase,
    owned_before: bool,
    embedded: bool,
}

impl Session {
    pub fn begin(rt: &mut Runtime<'_>) -> Session {
        let embedded = jobs::is_embedded();
        let owned_before = jobs::signals_owned();
        if !embedded {
            #[cfg(unix)]
            {
                let _ = rt;
                install_watcher();
            }
            #[cfg(not(unix))]
            jobs::install_stop_handler(&crate::native::session(rt));
            // The progress spinner's guard must not end the process on a signal meanwhile.
            jobs::own_signals();
        }
        let previous = set_phase(Phase::Armed);
        Session {
            previous,
            owned_before,
            embedded,
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        set_phase(self.previous);
        if !self.embedded && !self.owned_before {
            jobs::disown_signals();
        }
    }
}

/// Write a notice to stderr without failing when it is gone (a closed terminal).
fn notice(text: &str) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{text}");
}

/// The watcher thread (once per process, for its whole life: unregistering would leave the
/// library's handler in place without the default action).
#[cfg(unix)]
fn install_watcher() {
    use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
    use signal_hook::iterator::Signals;

    static START: std::sync::Once = std::sync::Once::new();
    START.call_once(|| {
        let Ok(mut signals) = Signals::new([SIGINT, SIGTERM, SIGHUP]) else {
            return;
        };
        std::thread::spawn(move || {
            for signal in signals.forever() {
                match phase() {
                    Phase::Armed => {
                        LAST_SIGNAL.store(signal, Ordering::SeqCst);
                        if jobs::stop_requested_flag().swap(true, Ordering::SeqCst) {
                            notice("Stopping; please wait while things are put back as they were.");
                        }
                    }
                    Phase::Shielded => notice(
                        "Putting things back as they were; please wait (stopping now would leave \
                         them half-done).",
                    ),
                    Phase::Prompt => {
                        let _ = signal_hook::low_level::emulate_default_handler(signal);
                    }
                    Phase::Idle => {
                        // As before these commands ran: the default action, unless another
                        // action handles SIGINT / SIGTERM (nothing else handles SIGHUP).
                        let handled = signal != SIGHUP
                            && (jobs::stop_handler_installed()
                                || jobs::is_embedded()
                                || jobs::signals_owned());
                        if !handled {
                            let _ = signal_hook::low_level::emulate_default_handler(signal);
                        }
                    }
                }
            }
        });
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phases_nest_and_come_back() {
        assert_eq!(phase(), Phase::Idle);
        with_phase(Phase::Armed, || {
            assert_eq!(phase(), Phase::Armed);
            shielded(|| assert_eq!(phase(), Phase::Shielded));
            prompt(|| assert_eq!(phase(), Phase::Prompt));
            assert_eq!(phase(), Phase::Armed);
        });
        assert_eq!(phase(), Phase::Idle);
        // A stop request counts only while a step runs.
        assert!(!stop_requested());
    }
}
