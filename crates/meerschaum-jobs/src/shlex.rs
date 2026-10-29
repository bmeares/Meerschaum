//! Python's `shlex.split`, `shlex.quote` and `shlex.join`: the one implementation lives in
//! `meerschaum_core::shlex` (job labels are `shlex.join(sysargs)`; the systemd `ExecStart=`
//! line is parsed with `shlex.split`).

pub use meerschaum_core::shlex::{ShlexError, join, quote, split};
