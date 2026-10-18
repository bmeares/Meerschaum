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

/// [`run`] on this process's own arguments, as an [`ExitCode`]: the whole of `fn main` for a
/// standalone `mrsm`.
pub fn run_main() -> ExitCode {
    exit_code(run(&process_args()))
}
