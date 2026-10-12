//! `mrsm`: the Meerschaum v5 binary. See `docs/design/v5.0.0.md` §4.3.
#![deny(unsafe_code)]

use std::process::ExitCode;

use meerschaum_cli::dispatch::{self, Runtime};

fn main() -> ExitCode {
    let sysargs: Vec<String> = std::env::args_os()
        .skip(1)
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let mut rt = Runtime::system(&mut out);
    let rc = dispatch::main(&sysargs, &mut rt);
    ExitCode::from(u8::try_from(rc).unwrap_or(1))
}
