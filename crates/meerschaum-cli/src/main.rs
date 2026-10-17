//! `mrsm`: the Meerschaum v5 binary. See `docs/design/v5.0.0.md` §4.3.
#![deny(unsafe_code)]

use std::process::ExitCode;

use meerschaum_cli::dispatch::{self, Runtime};

fn main() -> ExitCode {
    // A job's supervisor is this binary started with the supervisor argument.
    meerschaum_jobs::supervisor::run_if_requested();
    // A `systemd` job unit starts this binary as `mrsm -m meerschaum <sysargs>` with the
    // `MRSM_SYSTEMD_*` variables: wrap the run (log, stdin, result) like `entry.py` does.
    meerschaum_jobs::systemd_job::run_if_requested();
    // Library diagnostics (`tracing`) go to stderr only when `RUST_LOG` asks for them.
    if std::env::var_os("RUST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .try_init();
    }
    // Nothing is stripped here: `-m` is the metric filter of `mrsm -m ...`. The leading
    // `-m meerschaum` of a systemd unit is removed inside `systemd_job::run_if_requested`.
    let sysargs: Vec<String> = std::env::args_os()
        .skip(1)
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    // The client's warnings are printed by this process's runtime.
    meerschaum_cli::native::api_warnings::install();
    let mut out = std::io::stdout();
    let mut rt = Runtime::system(&mut out);
    let rc = if sysargs.first().map(String::as_str) == Some(dispatch::PIPELINE_CHILD_ARG) {
        dispatch::pipeline_child(&sysargs[1..], &mut rt)
    } else {
        dispatch::main(&sysargs, &mut rt)
    };
    if let Some(session) = &rt.native {
        session.shutdown();
    }
    ExitCode::from(u8::try_from(rc).unwrap_or(1))
}
