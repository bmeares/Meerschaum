//! Python's `shlex.split`, `shlex.quote` and `shlex.join` (the one implementation lives in
//! `meerschaum_core::shlex`); the CLI's errors carry the Python `ValueError` message as text.

pub use meerschaum_core::shlex::{join, quote};

/// `shlex.split(s)`. Errors carry the Python `ValueError` message.
pub fn split(s: &str) -> Result<Vec<String>, String> {
    meerschaum_core::shlex::split(s).map_err(|e| e.to_string())
}
