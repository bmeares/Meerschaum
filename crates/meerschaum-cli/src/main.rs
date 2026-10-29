//! `mrsm`: the Meerschaum v5 binary. See `docs/design/v5.0.0.md` §4.3.
//!
//! This is the internal binary of the `meerschaum-cli` crate (its integration tests run it). The
//! `mrsm` that users install is built from the `meerschaum` front crate (`cargo install
//! meerschaum`) or comes from the Python wheel; both call [`meerschaum_cli::run_main`] too.
#![deny(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    meerschaum_cli::run_main()
}
