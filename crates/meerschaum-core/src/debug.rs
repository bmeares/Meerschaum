//! Debug tracing: the [`dprint!`](crate::dprint) macro, the process-wide debug switch and the
//! helpers that keep secrets out of debug output.
//!
//! `dprint!` is a `tracing` `DEBUG` event whose target is the caller's module
//! (`meerschaum_connector_sql::instance`), with the caller's file and line in its metadata. It is
//! emitted only while the switch is on, so a `dprint!` costs one atomic load when debugging is
//! off (the arguments are never formatted):
//!
//! - the CLI turns the switch on for `--debug` (any action) and for the shell's `debug` toggle;
//!   its subscriber prints the events like v4's `dprint()` (see `meerschaum-cli`'s `debug_log`);
//! - the Python extension turns it on for the duration of a binding called with `debug=True`
//!   ([`guard`]) and hands the events to `meerschaum.utils.debug.dprint()`.
//!
//! `RUST_LOG` is independent of the switch: it keeps printing what its filter selects.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[doc(hidden)]
pub use tracing as __tracing;

/// How many [`scope`]s are alive (`--debug` actions, which may overlap in one process).
static SCOPES: AtomicUsize = AtomicUsize::new(0);
/// The shell's `debug` toggle ([`set_enabled`]).
static TOGGLE: AtomicBool = AtomicBool::new(false);
/// How many [`guard`]s are alive (the extension's `debug=True` calls, which may overlap).
static GUARDS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// Set by [`without_scopes`]: [`scope`] does nothing on this thread.
    static NO_SCOPES: Cell<bool> = const { Cell::new(false) };
}

/// Whether debug output is on.
#[inline]
pub fn enabled() -> bool {
    SCOPES.load(Ordering::Relaxed) > 0
        || TOGGLE.load(Ordering::Relaxed)
        || GUARDS.load(Ordering::Relaxed) > 0
}

/// Whether the switch itself is on (a `--debug` [`scope`] or the shell toggle; [`guard`]s are
/// not counted).
pub fn forced() -> bool {
    SCOPES.load(Ordering::SeqCst) > 0 || TOGGLE.load(Ordering::SeqCst)
}

/// Turn the shell's toggle on or off. A live `--debug` [`scope`] or [`guard`] still counts while
/// it is off (see [`take_over`] for the shell that was started with `--debug`).
pub fn set_enabled(on: bool) {
    TOGGLE.store(on, Ordering::SeqCst);
}

/// Turn debug output on until the returned value is dropped when `on` (one dispatched action
/// with `--debug`). Scopes are counted, so overlapping actions in one process (threads calling
/// `mrsm.entry`) leave the switch off once the last one ends, whatever the order. `scope(false)`
/// does nothing: an action without `--debug` never turns off an enclosing one. On a thread
/// inside [`without_scopes`] nothing happens either.
#[must_use = "the switch is restored when the scope is dropped"]
pub fn scope(on: bool) -> DebugScope {
    let counted = on && !NO_SCOPES.with(Cell::get);
    if counted {
        SCOPES.fetch_add(1, Ordering::SeqCst);
    }
    DebugScope { counted }
}

/// See [`scope`].
#[derive(Debug)]
pub struct DebugScope {
    counted: bool,
}

impl Drop for DebugScope {
    fn drop(&mut self) {
        if self.counted {
            sub_saturating(&SCOPES, 1);
        }
    }
}

fn sub_saturating(counter: &AtomicUsize, n: usize) {
    let _ = counter.try_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
        Some(v.saturating_sub(n))
    });
}

/// Run `f` with [`scope`] turned off on this thread: a `--debug` in an action an API client
/// asked for must not switch on debug output (and `debug=True` for Python worker calls) for
/// every other request the server is handling. The action still receives its `debug` argument.
pub fn without_scopes<R>(f: impl FnOnce() -> R) -> R {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            NO_SCOPES.with(|c| c.set(self.0));
        }
    }
    let _reset = Reset(NO_SCOPES.with(|c| c.replace(true)));
    f()
}

/// Hand the live `--debug` scopes over to the shell's toggle until the returned value is
/// dropped: `mrsm --shell --debug` starts the shell with debug on, and its `debug` toggle can
/// then turn it off. On drop the scopes count again and the toggle is put back.
#[must_use = "the scopes come back when this is dropped"]
pub fn take_over() -> TakeOver {
    let scopes = SCOPES.swap(0, Ordering::SeqCst);
    let toggle = TOGGLE.load(Ordering::SeqCst);
    if scopes > 0 {
        TOGGLE.store(true, Ordering::SeqCst);
    }
    TakeOver { scopes, toggle }
}

/// See [`take_over`].
#[derive(Debug)]
pub struct TakeOver {
    scopes: usize,
    toggle: bool,
}

impl Drop for TakeOver {
    fn drop(&mut self) {
        SCOPES.fetch_add(self.scopes, Ordering::SeqCst);
        TOGGLE.store(self.toggle, Ordering::SeqCst);
    }
}

/// Turn debug output on while the returned value lives when `on` (a call with `debug=True`;
/// concurrent guards stack). `guard(false)` does nothing.
#[must_use = "debug output stops when the guard is dropped"]
pub fn guard(on: bool) -> DebugGuard {
    if on {
        GUARDS.fetch_add(1, Ordering::SeqCst);
    }
    DebugGuard(on)
}

/// See [`guard`].
#[derive(Debug)]
pub struct DebugGuard(bool);

impl Drop for DebugGuard {
    fn drop(&mut self) {
        if self.0 {
            sub_saturating(&GUARDS, 1);
        }
    }
}

/// Whether a `tracing` target belongs to a Meerschaum crate (`meerschaum`, `meerschaum_*`):
/// only these events are printed as debug output, never dependency chatter (`sqlx`, `hyper`).
pub fn is_meerschaum_target(target: &str) -> bool {
    target == "meerschaum"
        || target.starts_with("meerschaum_")
        || target.starts_with("meerschaum::")
}

/// The header line of v4's `dprint()` without its icon and colors:
/// `HH:MM:SS.mmm | <module>:<line>` (the time is UTC, as `get_current_timestamp('ms')`).
/// `module` is the Rust module path (`meerschaum_connector_sql::instance`) where v4 printed the
/// Python module's `__name__`.
pub fn header(time: chrono::NaiveTime, module: &str, line: Option<u32>) -> String {
    let mut out = time.format("%H:%M:%S%.3f").to_string();
    out.push_str(" | ");
    out.push_str(module);
    if let Some(line) = line {
        out.push(':');
        out.push_str(&line.to_string());
    }
    out
}

/// Print a debug message: a `tracing` `DEBUG` event from the caller's module, file and line,
/// emitted only while [`enabled`](crate::debug::enabled) (the arguments are not evaluated
/// otherwise). Takes what `tracing::debug!` takes.
///
/// ```
/// meerschaum_core::dprint!("Fetched {} rows.", 10);
/// ```
#[macro_export]
macro_rules! dprint {
    ($($arg:tt)+) => {
        if $crate::debug::enabled() {
            $crate::debug::__tracing::debug!($($arg)+);
        }
    };
}

/// `text` with the secrets it shows replaced with `***`, wherever they appear (the rest is
/// unchanged):
///
/// - a URI's password (`postgresql://user:***@host/db`);
/// - a secret query parameter (`?password=***`, `&api_key=***`);
/// - a quoted secret key's quoted value, as JSON or a Python `repr` prints it
///   (`"password": "***"`, `'token': '***'`);
/// - the value after a secret flag inside one word (`'--password ***'`, `--api-key=***`).
pub fn redact_uris(text: &str) -> String {
    use std::sync::LazyLock;
    const SECRET: &str = r"[A-Za-z0-9_\-]*(?:password|passwd|secret|token|api[_\-]?key|private[_\-]?key)[A-Za-z0-9_\-]*";
    static URI_PASSWORD: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(?P<lead>[A-Za-z][A-Za-z0-9+.\-]*://[^:/@\s]*:)\S+@")
            .expect("valid regex")
    });
    static QUERY: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(&format!(r#"(?i)(?P<lead>[?&;]{SECRET}=)[^&#\s'"]+"#))
            .expect("valid regex")
    });
    static QUOTED: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(&format!(
            r#"(?i)(?P<lead>["']{SECRET}["']\s*:\s*)(?:"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')"#
        ))
        .expect("valid regex")
    });
    static FLAG: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(&format!(r#"(?i)(?P<lead>--{SECRET}(?:=|[ \t]+))[^\s'",]+"#))
            .expect("valid regex")
    });
    let text = URI_PASSWORD.replace_all(text, "${lead}***@");
    let text = QUERY.replace_all(&text, "${lead}***");
    let text = QUOTED.replace_all(&text, "${lead}'***'");
    FLAG.replace_all(&text, "${lead}***").into_owned()
}

/// Whether a key or flag names a secret (`password`, `api_key`, `--client-secret`, `token`):
/// debug output prints its value as `***`. URI credentials are handled by [`redact_uris`].
pub fn is_secret_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase().replace('-', "_");
    [
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "private_key",
    ]
    .iter()
    .any(|s| key.contains(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The switch is process-wide: the tests touching it run in one function.
    #[test]
    fn the_switch_follows_scopes_and_guards() {
        assert!(!enabled());
        // Off: the arguments are not even evaluated.
        let mut evaluated = false;
        crate::dprint!("{}", {
            evaluated = true;
            1
        });
        assert!(!evaluated);
        {
            let _on = scope(true);
            assert!(enabled());
            {
                // A nested action without `--debug` keeps the enclosing one.
                let _off = scope(false);
                assert!(enabled());
            }
            assert!(enabled());
        }
        assert!(!enabled());

        // Overlapping scopes that do not end in reverse order (two threads' actions).
        let a = scope(true);
        let b = scope(true);
        drop(a);
        assert!(enabled());
        drop(b);
        assert!(!enabled(), "the switch must not stay on");

        // A server request's `--debug` does not switch on debug output for the process.
        without_scopes(|| {
            let _on = scope(true);
            assert!(!enabled());
        });
        let _on = scope(true);
        assert!(enabled(), "without_scopes is reset afterwards");
        drop(_on);

        let a = guard(true);
        let b = guard(true);
        let none = guard(false);
        assert!(enabled());
        drop(a);
        assert!(enabled());
        drop(b);
        drop(none);
        assert!(!enabled());
        set_enabled(true);
        assert!(enabled() && forced());
        set_enabled(false);
        assert!(!enabled());

        // `mrsm --shell --debug`: the shell's toggle takes over the outer scope.
        let outer = scope(true);
        {
            let _shell = take_over();
            assert!(forced());
            set_enabled(false);
            assert!(!enabled(), "the shell's `debug off` wins");
            set_enabled(true);
            assert!(enabled());
        }
        assert!(enabled(), "the outer scope counts again");
        drop(outer);
        assert!(!enabled());
    }

    #[test]
    fn the_header_matches_v4() {
        let t = chrono::NaiveTime::from_hms_milli_opt(9, 5, 3, 7).unwrap();
        assert_eq!(
            header(t, "meerschaum_cli::dispatch", Some(42)),
            "09:05:03.007 | meerschaum_cli::dispatch:42"
        );
        assert_eq!(header(t, "x", None), "09:05:03.007 | x");
    }

    #[test]
    fn uri_passwords_are_redacted() {
        assert_eq!(
            redact_uris("uri=postgresql://user:p@ss:w0rd@localhost:5432/db x"),
            "uri=postgresql://user:***@localhost:5432/db x"
        );
        assert_eq!(
            redact_uris("sqlite:////tmp/a.db and http://host:8000/x"),
            "sqlite:////tmp/a.db and http://host:8000/x"
        );
        assert_eq!(
            redact_uris("mssql://sa:secret!@localhost:1439/master"),
            "mssql://sa:***@localhost:1439/master"
        );
        assert_eq!(
            redact_uris("postgresql://h/db?sslmode=require&password=hunter2&x=1"),
            "postgresql://h/db?sslmode=require&password=***&x=1"
        );
        assert_eq!(
            redact_uris(r#"'{"password": "hun\"ter2", "fetch": 1}' {'api_key': 'k3y'}"#),
            r#"'{"password": '***', "fetch": 1}' {'api_key': '***'}"#
        );
        assert_eq!(
            redact_uris("['--sub-args', '--password hunter5 -x', '--token=abc']"),
            "['--sub-args', '--password *** -x', '--token=***']"
        );
    }

    #[test]
    fn secret_keys() {
        for key in ["password", "PASSWORD", "client_secret", "api-key", "token"] {
            assert!(is_secret_key(key), "{key}");
        }
        for key in ["username", "host", "action", "debug"] {
            assert!(!is_secret_key(key), "{key}");
        }
    }
}
