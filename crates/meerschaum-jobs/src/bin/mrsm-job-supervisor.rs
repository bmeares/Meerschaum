//! The standalone job supervisor (see `meerschaum_jobs::supervisor`).

fn main() {
    std::process::exit(meerschaum_jobs::supervisor::main_with_args(
        std::env::args().skip(1),
    ));
}
