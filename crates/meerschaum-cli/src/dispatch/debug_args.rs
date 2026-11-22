//! `--debug` support: the debug scope and the redacted rendering of an action's arguments.

use super::Runtime;
use crate::value::{ArgsMap, PyValue};

/// Turn the debug switch on for `--debug` (see `meerschaum_core::debug`) until the returned scope
/// is dropped, with the debug header drawn from this runtime's formatting settings. Never turns
/// it off: a nested action without `--debug` keeps an enclosing `--debug` or the shell toggle.
pub fn debug_scope(rt: &mut Runtime<'_>, on: bool) -> Option<meerschaum_core::debug::DebugScope> {
    if !on {
        return None;
    }
    rt.with_err_formatter(crate::debug_log::configure);
    Some(meerschaum_core::debug::scope(true))
}

/// [`format_args_map`] for debug output: secret values (`--password`, tokens, ...), at any depth
/// of dicts and lists (`--params`), and URI passwords, secret query parameters and secrets inside
/// JSON text or one word (`--sub-args '--password x'`) are replaced with `***`.
pub fn redacted_args(args: &ArgsMap) -> String {
    use meerschaum_core::debug::{is_secret_key, redact_text, redact_uris};
    fn hide(value: &mut PyValue) {
        if !matches!(value, PyValue::None) {
            *value = PyValue::Str("***".into());
        }
    }
    fn walk(value: &mut PyValue) {
        match value {
            PyValue::Dict(items) => {
                for (key, value) in items.iter_mut() {
                    if matches!(key, PyValue::Str(k) if is_secret_key(k)) {
                        hide(value);
                    } else {
                        walk(value);
                    }
                }
            }
            PyValue::List(words) | PyValue::Tuple(words) | PyValue::Set(words) => {
                // `sysargs`, `filtered_sysargs`, `sub_args`: the word after a secret flag.
                let mut hide_next = false;
                for word in words.iter_mut() {
                    if std::mem::take(&mut hide_next) {
                        hide(word);
                        continue;
                    }
                    let PyValue::Str(text) = word else {
                        walk(word);
                        continue;
                    };
                    let Some(flag) = text.strip_prefix("--") else {
                        // `--params '{"token": 1}'`, `-P token:1`, a URI with a password.
                        walk(word);
                        continue;
                    };
                    let (name, inline) = match flag.split_once('=') {
                        Some((name, _)) => (name, true),
                        None => (flag, false),
                    };
                    if is_secret_key(name) && !name.contains(char::is_whitespace) {
                        if inline {
                            *word = PyValue::Str(format!("--{name}=***"));
                        } else {
                            hide_next = true;
                        }
                    }
                }
            }
            PyValue::Str(text) => {
                let redacted = redact_text(text);
                if redacted != *text {
                    *text = redacted;
                }
            }
            _ => {}
        }
    }
    let mut args = args.clone();
    for (key, value) in args.iter_mut() {
        if is_secret_key(key) {
            hide(value);
        } else {
            walk(value);
        }
    }
    redact_uris(&format_args_map(&args))
}

/// Render the arguments map as a Python `dict` literal, one key per line.
pub fn format_args_map(args: &ArgsMap) -> String {
    if args.is_empty() {
        return "{}".into();
    }
    let lines: Vec<String> = args
        .iter()
        .map(|(k, v)| format!("    {}: {},", crate::value::repr_str(k), v.py_repr()))
        .collect();
    format!("{{\n{}\n}}", lines.join("\n"))
}
