//! The process entry point shared by every host of the CLI: the standalone `mrsm` binary
//! (`crates/meerschaum/src/main.rs` and this crate's own `src/main.rs`) and the Python extension
//! (`meerschaum._core._native.main`, what the `mrsm` console script of `pip install meerschaum`
//! calls).
//!
//! The host passes the command line explicitly: `std::env::args()` is the interpreter's argv
//! inside the extension.

use std::process::ExitCode;

use crate::dispatch::{self, Runtime};

/// Run `mrsm <sysargs>` to completion and return its exit code.
///
/// A job's supervisor is this program started with the supervisor argument, and a `systemd` job
/// unit starts it with the `MRSM_SYSTEMD_*` variables; both are handled first (they exit the
/// process when they apply). Nothing is stripped from `sysargs`: `-m` is the metric filter of
/// `mrsm -m ...`, and the leading `-m meerschaum` of a systemd unit is removed inside
/// `systemd_job::run_if_requested_with`.
pub fn run(sysargs: &[String]) -> i32 {
    // A shell completion request (`_MRSM_COMPLETE`, see `completions`) is answered before anything
    // else: no logging, no config writes, no plugins, no runtime.
    if let Some(rc) = crate::completions::answer_if_requested() {
        return rc;
    }
    if let Some(rc) = crate::completions::print_script_if_requested(sysargs) {
        return rc;
    }
    meerschaum_jobs::supervisor::run_if_requested_with(sysargs);
    meerschaum_jobs::systemd_job::run_if_requested_with(sysargs);
    // Library diagnostics (`tracing`) go to stderr only when `RUST_LOG` asks for them.
    if std::env::var_os("RUST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .try_init();
    }
    // The client's warnings are printed by this process's runtime.
    crate::native::api_warnings::install();
    let mut out = std::io::stdout();
    let mut rt = Runtime::system(&mut out);
    let rc = if sysargs.first().map(String::as_str) == Some(dispatch::PIPELINE_CHILD_ARG) {
        dispatch::pipeline_child(&sysargs[1..], &mut rt)
    } else if sysargs.first().map(String::as_str) == Some(dispatch::ENTRY_CHILD_ARG)
        && let Some(path) = sysargs.get(1)
        && take_entry_child_marker().is_some_and(|m| m.as_os_str() == std::ffi::OsStr::new(path))
    {
        dispatch::entry_child(&sysargs[1..], &mut rt)
    } else {
        dispatch::main(sysargs, &mut rt)
    };
    if let Some(session) = &rt.native {
        session.shutdown();
    }
    rc
}

/// The exit status of `rc` as a process exit code (anything outside `0..=255` is `1`).
pub fn exit_code(rc: i32) -> ExitCode {
    ExitCode::from(u8::try_from(rc).unwrap_or(1))
}

/// The command line of this process without the program name (lossy for non-UTF-8 arguments).
pub fn process_args() -> Vec<String> {
    std::env::args_os()
        .skip(1)
        .map(|a| a.to_string_lossy().into_owned())
        .collect()
}

/// Forget that a Python launcher handed this process over (`MRSM_BIN_EXEC`, see
/// [`meerschaum_core::self_exe::BIN_GUARD_ENV`]).
///
/// The guard only blocks the immediate re-entry of a launcher which is not a real binary. This
/// process *is* a real standalone binary, so it must not reach its children (Python workers, the
/// API host, jobs), where it would make a Python process ignore `MRSM_BIN` and warn that it was
/// "started again". Only the standalone entry does this: inside the extension the guard stays,
/// because there it means "this Python process was started by a launcher".
#[allow(unsafe_code)]
fn drop_handover_guard() {
    // SAFETY: called first thing in `fn main` of the standalone binary (`run_main`), before this
    // program starts any thread or reads the environment elsewhere, so nothing can observe the
    // environment while it changes.
    unsafe { std::env::remove_var(meerschaum_core::self_exe::BIN_GUARD_ENV) };
}

/// The [`dispatch::ENTRY_CHILD_ENV`] marker of this process, taken out of the environment
/// before anything runs (`None` once taken, or when absent).
static ENTRY_CHILD_MARKER: std::sync::Mutex<Option<std::ffi::OsString>> =
    std::sync::Mutex::new(None);

/// Record this process's [`dispatch::ENTRY_CHILD_ENV`] marker. The host removes the variable from
/// the environment first (the standalone binary in [`run_main`], the extension through Python's
/// `os.environ`), so nothing the command starts inherits it. It is compared with the result path
/// on the command line as an OS string (the compose runner only uses a UTF-8 path).
pub fn set_entry_child_marker(marker: Option<std::ffi::OsString>) {
    *ENTRY_CHILD_MARKER.lock().unwrap_or_else(|e| e.into_inner()) = marker;
}

fn take_entry_child_marker() -> Option<std::ffi::OsString> {
    ENTRY_CHILD_MARKER
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
}

/// Take the [`dispatch::ENTRY_CHILD_ENV`] marker out of the standalone binary's environment.
#[allow(unsafe_code)]
fn take_entry_child_env() {
    let marker = std::env::var_os(dispatch::ENTRY_CHILD_ENV);
    if marker.is_some() {
        // SAFETY: called first thing in `fn main` of the standalone binary (`run_main`), before
        // this program starts any thread, as `drop_handover_guard` is.
        unsafe { std::env::remove_var(dispatch::ENTRY_CHILD_ENV) };
    }
    set_entry_child_marker(marker);
}

/// [`run`] on this process's own arguments, as an [`ExitCode`]: the whole of `fn main` for a
/// standalone `mrsm`.
pub fn run_main() -> ExitCode {
    drop_handover_guard();
    take_entry_child_env();
    exit_code(run(&process_args()))
}
