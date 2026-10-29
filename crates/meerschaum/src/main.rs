//! `mrsm`: the Meerschaum command line, installed by `cargo install meerschaum`.
//!
//! The same program is the `mrsm` console script of `pip install meerschaum` (there it runs from
//! the Python extension) and the binary in the Docker image. Everything lives in the
//! `meerschaum-cli` crate; this binary only gives the `meerschaum` crate the name `mrsm`.
#![deny(unsafe_code)]

fn main() -> std::process::ExitCode {
    meerschaum_cli::run_main()
}
