//! Environment maps.
//!
//! Python compose reads and mutates `os.environ` (loading `.env` files, blanking undefined
//! variables) inside `replace_env({})`, so nothing leaks into the host process. The Rust port
//! never touches the real process environment: every function takes the environment as an
//! explicit, insertion-ordered map (Python dicts and `os.environ` iterate in insertion order).

use indexmap::IndexMap;
use meerschaum_core::Value;

/// A process environment (`os.environ`): string keys and values, in insertion order.
pub type ProcessEnv = IndexMap<String, String>;

/// A project environment as returned by `get_env_dict()`: values from the compose file's
/// `environment:` block keep their YAML types (Python passes them through unconverted).
pub type EnvDict = IndexMap<String, Value>;

/// Build a [`ProcessEnv`] from the process environment (variables that are not valid UTF-8 are
/// skipped).
pub fn current_process_env() -> ProcessEnv {
    meerschaum_core::process_env::vars().into_iter().collect()
}

/// Convert an [`EnvDict`] into string pairs for spawning a process (`str(value)`, `None` as an
/// empty string).
pub fn env_dict_to_strings(env: &EnvDict) -> Vec<(String, String)> {
    env.iter()
        .map(|(k, v)| {
            let s = match v {
                Value::Null => String::new(),
                other => meerschaum_core::pyfmt::py_str(other),
            };
            (k.clone(), s)
        })
        .collect()
}
