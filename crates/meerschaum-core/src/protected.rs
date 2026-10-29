//! Protected connectors (`protected: true` on a connector's attributes).
//!
//! A connector marked `protected` refuses every mutation the core makes through it (sync, clear,
//! drop, delete, register, edit, schema changes, raw SQL execution through the façade) unless
//! writes to protected connectors are allowed for the process:
//!
//! - `MRSM_ALLOW_PROTECTED` is set to a truthy value (`1`, `true`, `yes`, `on`) in the
//!   environment ([`ALLOW_PROTECTED_ENV`]), which child processes inherit; or
//! - the CLI got `--allow-protected` (the dispatcher holds an [`allow`] scope for the action and
//!   forwards the flag to the jobs it starts), or Python code runs inside
//!   `meerschaum.connectors.allow_protected()` (also an [`allow`] scope).
//!
//! `-y` / `--yes` never counts: confirming a prompt is not permission to write to a protected
//! connector. Reads are never blocked.
//!
//! Every connector type checks its own attribute ([`is_protected_attribute`]) and refuses with
//! [`refusal_message`] (the text is part of the rc15 contract C6).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::value::Value;

/// The variable that allows writes to protected connectors for a process and its children.
pub const ALLOW_PROTECTED_ENV: &str = "MRSM_ALLOW_PROTECTED";

/// The connector attribute that marks a connector protected.
pub const PROTECTED_ATTRIBUTE: &str = "protected";

/// How many [`allow`] scopes are alive (overlapping actions or Python contexts).
static SCOPES: AtomicUsize = AtomicUsize::new(0);
/// [`allow_for_process`] was called.
static PROCESS: AtomicBool = AtomicBool::new(false);

/// Whether a text value means "on" (`1`, `true`, `yes`, `on`, `y`, `t`; any case).
pub fn truthy_text(text: &str) -> bool {
    matches!(
        text.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | "y" | "t"
    )
}

/// Whether a connector attribute value marks the connector protected (`true`, a non-zero
/// number, or a truthy string such as `"true"` from a URI query or an environment variable).
pub fn is_protected_attribute(value: &Value) -> bool {
    match value {
        Value::Bool(b) => *b,
        Value::Int(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        Value::Str(s) => truthy_text(s),
        _ => false,
    }
}

/// Whether a connector URI marks the connector protected: a `protected` query parameter with a
/// truthy value (`valkey://host:6379/0?protected=true`, `MRSM_VALKEY_PROD=...`). The parameter
/// is Meerschaum's: connectors never pass it to their database client.
pub fn uri_marks_protected(uri: &str) -> bool {
    let without_fragment = uri.split('#').next().unwrap_or(uri);
    let Some((_, query)) = without_fragment.split_once('?') else {
        return false;
    };
    query.split('&').any(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        key.trim() == PROTECTED_ATTRIBUTE && truthy_text(value)
    })
}

/// Whether `MRSM_ALLOW_PROTECTED` is set to a truthy value in this process's environment.
pub fn allowed_by_env() -> bool {
    std::env::var(ALLOW_PROTECTED_ENV).is_ok_and(|v| truthy_text(&v))
}

/// Whether this process allows writes to protected connectors through a flag or a scope
/// (not counting the environment).
pub fn allowed_by_flag() -> bool {
    PROCESS.load(Ordering::SeqCst) || SCOPES.load(Ordering::SeqCst) > 0
}

/// Whether writes to protected connectors are allowed right now (the environment, the
/// process flag or a live [`allow`] scope).
pub fn allowed() -> bool {
    allowed_by_flag() || allowed_by_env()
}

/// Allow writes to protected connectors for the rest of the process (a one-shot command run
/// with `--allow-protected`). Prefer [`allow`] where the allowance has an end.
pub fn allow_for_process() {
    PROCESS.store(true, Ordering::SeqCst);
}

/// An allowance that ends when it is dropped (see [`allow`]).
#[must_use = "the allowance ends when the scope is dropped"]
#[derive(Debug)]
pub struct AllowScope {
    active: bool,
}

impl Drop for AllowScope {
    fn drop(&mut self) {
        if self.active {
            SCOPES.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Allow writes to protected connectors until the returned scope is dropped when `on` (one
/// dispatched action with `--allow-protected`, one `with allow_protected():` block). Scopes are
/// counted, so overlapping ones end in any order; `allow(false)` does nothing. The allowance is
/// process-wide: other threads of the process see it while the scope lives.
pub fn allow(on: bool) -> AllowScope {
    if on {
        SCOPES.fetch_add(1, Ordering::SeqCst);
    }
    AllowScope { active: on }
}

/// Enter one allowance without a guard (the Python context manager's `__enter__`); pair it
/// with [`leave`].
pub fn enter() {
    SCOPES.fetch_add(1, Ordering::SeqCst);
}

/// Leave an allowance entered with [`enter`] (never below zero).
pub fn leave() {
    let _ = SCOPES.try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
}

/// The environment a child process needs to keep this process's allowance (empty unless a
/// flag or scope allows protected writes; an inherited `MRSM_ALLOW_PROTECTED` reaches children
/// on its own).
pub fn child_env() -> Vec<(String, String)> {
    if allowed_by_flag() {
        vec![(ALLOW_PROTECTED_ENV.to_string(), "1".to_string())]
    } else {
        Vec::new()
    }
}

/// The refusal text (contract C6): `Refusing to <op> on protected instance '<keys>'. Pass
/// --allow-protected (or set MRSM_ALLOW_PROTECTED=1).`
pub fn refusal_message(op: &str, connector_keys: &str) -> String {
    format!(
        "Refusing to {op} on protected instance '{connector_keys}'. \
         Pass --allow-protected (or set {ALLOW_PROTECTED_ENV}=1)."
    )
}

/// `Err(refusal_message(...))` when `protected` and writes are not allowed, else `Ok(())`.
pub fn check(protected: bool, op: &str, connector_keys: &str) -> Result<(), String> {
    if protected && !allowed() {
        return Err(refusal_message(op, connector_keys));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attribute_values() {
        assert!(is_protected_attribute(&Value::Bool(true)));
        assert!(!is_protected_attribute(&Value::Bool(false)));
        assert!(is_protected_attribute(&Value::Str("True".into())));
        assert!(is_protected_attribute(&Value::Str("yes".into())));
        assert!(is_protected_attribute(&Value::Int(1)));
        assert!(!is_protected_attribute(&Value::Str("false".into())));
        assert!(!is_protected_attribute(&Value::Str("".into())));
        assert!(!is_protected_attribute(&Value::Null));
    }

    #[test]
    fn uri_query_marks() {
        assert!(uri_marks_protected(
            "redis://localhost:6379/0?protected=true"
        ));
        assert!(uri_marks_protected("valkey://h/1?timeout=5s&protected=1"));
        assert!(uri_marks_protected("http://u:p@h:8000?protected=True#x"));
        assert!(!uri_marks_protected("redis://localhost:6379/0"));
        assert!(!uri_marks_protected("redis://h/0?protected=false"));
        assert!(!uri_marks_protected("redis://h/0?protected"));
        assert!(!uri_marks_protected("redis://h/0?unprotected=true"));
        assert!(!uri_marks_protected("redis://h/0#?protected=true"));
    }

    #[test]
    fn message_and_scopes() {
        let msg = refusal_message("sync Pipe('a', 'b')", "sql:prod");
        assert_eq!(
            msg,
            "Refusing to sync Pipe('a', 'b') on protected instance 'sql:prod'. \
             Pass --allow-protected (or set MRSM_ALLOW_PROTECTED=1)."
        );
        // Other tests in this process may hold scopes; only check what a scope adds.
        {
            let _scope = allow(true);
            assert!(allowed_by_flag());
            assert!(check(true, "clear", "sql:prod").is_ok());
            assert_eq!(child_env().len(), 1);
        }
        assert!(check(false, "clear", "sql:prod").is_ok());
        let _off = allow(false);
        enter();
        assert!(allowed_by_flag());
        leave();
        leave(); // Never below zero.
    }
}
