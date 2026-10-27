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
    // The syncs this process records in pipes' sync statuses come from the command line
    // (`mrsm start api` switches to `api` while it serves). The previous role comes back when
    // the command line returns: Python's `_native.main()` runs it inside a Python process, whose
    // later `pipe.sync()` calls are `python` again.
    let _sync_source = meerschaum_connector::sync_status::role_scope(
        meerschaum_connector::sync_status::SyncSource::Cli,
    );
    // Debug output (`--debug`, the shell's `debug` toggle) and `RUST_LOG` diagnostics go to
    // stderr through this subscriber (see `debug_log`).
    crate::debug_log::install();
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
        // Sync statuses are written in the background: keep the latest ones, debounced ones
        // included, before the runtime goes away.
        session.block_on_uninterruptible(meerschaum_connector::sync_status::flush(
            meerschaum_connector::sync_status::RECORD_TIMEOUT,
        ));
        session.shutdown();
    }
    if rt.restart_requested {
        RESTART_SHELL
    } else if rc == RESTART_SHELL {
        // Reserved for the restart request: a passed-through status never reopens a shell.
        1
    } else {
        rc
    }
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
/// Internal launcher result: start a fresh shell after replacing the engine.
pub const RESTART_SHELL: i32 = 75;

pub fn run_main() -> ExitCode {
    drop_handover_guard();
    take_entry_child_env();
    let code = run(&process_args());
    if code == RESTART_SHELL {
        let (program, leading) = meerschaum_core::self_exe::self_program();
        let mut command = std::process::Command::new(program);
        command.args(leading).arg("--shell");
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            eprintln!("Unable to restart Meerschaum: {}", command.exec());
            return exit_code(1);
        }
        #[cfg(not(unix))]
        {
            return exit_code(command.status().ok().and_then(|s| s.code()).unwrap_or(1));
        }
    }
    exit_code(code)
}
