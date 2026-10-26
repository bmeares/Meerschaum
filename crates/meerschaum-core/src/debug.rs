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

/// A key or flag name that holds a secret (see [`is_secret_key`]), as a regex fragment.
const SECRET: &str =
    r"[A-Za-z0-9_\-]*(?:password|passwd|secret|token|api[_\-]?key|private[_\-]?key)[A-Za-z0-9_\-]*";

/// A value after a secret key in text that is not parsed: a quoted string, a flat list or
/// object, or a bare scalar (a number, `true`).
const ANY_VALUE: &str =
    r#"(?:"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\[[^\]]*\]|\{[^}]*\}|[^\s,}\])]+)"#;

/// What replaces a secret in debug output.
pub const MASK: &str = "***";

/// `text` with the secrets it shows replaced with `***`, wherever they appear (the rest is
/// unchanged):
///
/// - a URI's password (`postgresql://user:***@host/db`);
/// - a secret query parameter (`?password=***`, `&api_key=***`);
/// - a quoted secret key's value, as JSON or a Python `repr` prints it, whatever its type
///   (`"password": "***"`, `'token': '***'`, `"api_key": '***'` for `"api_key": ["k"]`);
/// - the value after a secret flag inside one word (`'--password ***'`, `--api-key=***`).
pub fn redact_uris(text: &str) -> String {
    use std::sync::LazyLock;
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
            r#"(?i)(?P<lead>["']{SECRET}["']\s*:\s*)(?P<value>{ANY_VALUE})"#
        ))
        .expect("valid regex")
    });
    static FLAG: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(&format!(r#"(?i)(?P<lead>--{SECRET}(?:=|[ \t]+))[^\s'",]+"#))
            .expect("valid regex")
    });
    let text = URI_PASSWORD.replace_all(text, "${lead}***@");
    let text = QUERY.replace_all(&text, "${lead}***");
    let text = QUOTED.replace_all(&text, |caps: &regex::Captures<'_>| {
        // An already masked value keeps its quotes (`{"token":"***"}` from `redact_json`).
        match &caps["value"] {
            v @ ("\"***\"" | "'***'") => format!("{}{v}", &caps["lead"]),
            _ => format!("{}'***'", &caps["lead"]),
        }
    });
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

/// Redact a parsed JSON value in place: the value of every secret key (see [`is_secret_key`]) at
/// any depth, whatever its type (a number, a list, an object), becomes `"***"` (`null` stays);
/// every string goes through [`redact_text`]. Returns whether anything changed.
pub fn redact_json(value: &mut serde_json::Value) -> bool {
    use serde_json::Value as Json;
    match value {
        Json::Object(map) => {
            let mut changed = false;
            for (key, value) in map.iter_mut() {
                if is_secret_key(key) {
                    if !value.is_null() && value.as_str() != Some(MASK) {
                        *value = Json::String(MASK.into());
                        changed = true;
                    }
                } else {
                    changed |= redact_json(value);
                }
            }
            changed
        }
        Json::Array(items) => items
            .iter_mut()
            .fold(false, |changed, item| redact_json(item) | changed),
        Json::String(text) => {
            let redacted = redact_text(text);
            if redacted == *text {
                false
            } else {
                *text = redacted;
                true
            }
        }
        _ => false,
    }
}

/// One command-line word (or any string an argument holds) with its secrets replaced, for
/// debug output:
///
/// - a JSON object or list (`--params '{"token": 123, "api_key": ["k"]}'`) is parsed, redacted
///   with [`redact_json`] and written back compactly;
/// - `key:value` / `key=value` pairs (`--params token:123,id:1`, a Python `dict` literal) hide the
///   value of a secret key;
/// - then everything [`redact_uris`] hides (URI passwords, secret query parameters, ...).
pub fn redact_text(text: &str) -> String {
    use std::sync::LazyLock;
    static PAIRS: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(&format!(
            r#"(?i)(?P<lead>(?:^|[,\s{{])["']?{SECRET}["']?\s*[:=]\s*){ANY_VALUE}"#
        ))
        .expect("valid regex")
    });
    let trimmed = text.trim();
    if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && let Ok(mut parsed) = serde_json::from_str::<serde_json::Value>(trimmed)
    {
        return if redact_json(&mut parsed) {
            parsed.to_string()
        } else {
            text.to_string()
        };
    }
    redact_uris(&PAIRS.replace_all(text, format!("${{lead}}{MASK}")))
}

/// A SQL statement as debug output prints it (the statement that runs is unchanged): the
/// literal compared to (or assigned to) a column whose name is a secret (see [`is_secret_key`])
/// becomes `'***'`, in every flavor's identifier quoting:
///
/// - `"password" = 'x'`, `` `token` <> 'x' ``, `[api_key] != N'x'`, `secret = 123`;
/// - `"token" IN ('a', 'b')`, `"token" NOT IN (...)`, `"token" LIKE 'x%'`;
/// - `CAST("password" AS TEXT) = '...'`, MySQL's `_utf8mb4 X'...'` literals;
///
/// and everything [`redact_uris`] hides (URI passwords anywhere, quoted secret keys in JSON text).
pub fn redact_sql(sql: &str) -> String {
    use std::sync::LazyLock;
    static COMPARISON: LazyLock<regex::Regex> = LazyLock::new(|| {
        let column = format!(r#"(?:"{SECRET}"|`{SECRET}`|\[{SECRET}\]|\b{SECRET}\b)"#);
        let cast = r"(?:\s+AS\s+\w+(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?\s*\))?";
        let op = r"(?:=|!=|<>|<=|>=|<|>|(?:NOT\s+)?(?:I?LIKE|IN)\b)";
        let literal = r"(?:_utf8mb4\s+)?[NnXx]?'(?:[^']|'')*'";
        let value = format!(r"(?:\((?:[^()']|{literal})*\)|{literal}|-?\d+(?:\.\d+)?)");
        regex::Regex::new(&format!(
            r"(?i)(?P<lead>{column}{cast}\s*{op}\s*)(?P<value>{value})"
        ))
        .expect("valid regex")
    });
    let sql = COMPARISON.replace_all(sql, |caps: &regex::Captures<'_>| {
        let mask = if caps["value"].starts_with('(') {
            format!("('{MASK}')")
        } else {
            format!("'{MASK}'")
        };
        format!("{}{mask}", &caps["lead"])
    });
    redact_uris(&sql)
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

    #[test]
    fn json_words_are_redacted_structurally() {
        let word = r#"{"token":123456789,"api_key":["live-secret"],"fetch":{"client_secret":{"a":1},"uri":"postgresql://u:hunter2@h/db"},"id":5,"password":null}"#;
        let out = redact_text(word);
        for secret in ["123456789", "live-secret", "hunter2", r#""a""#] {
            assert!(!out.contains(secret), "{out}");
        }
        assert!(out.contains(r#""id":5"#), "{out}");
        assert!(out.contains(r#""password":null"#), "{out}");
        assert!(out.contains("postgresql://u:***@h/db"), "{out}");
        let list = r#"[{"secret": true}, 1]"#;
        assert_eq!(redact_text(list), r#"[{"secret":"***"},1]"#);
        // Nothing to hide: the word is unchanged (not re-serialized).
        assert_eq!(redact_text(r#"{"id": 1}"#), r#"{"id": 1}"#);
    }

    #[test]
    fn pairs_and_unparsed_json_are_redacted() {
        assert_eq!(redact_text("token:123,id:1"), "token:***,id:1");
        assert_eq!(redact_text("id:1,api_key:abc"), "id:1,api_key:***");
        assert_eq!(redact_text("password=hunter2"), "password=***");
        assert_eq!(
            redact_text("{'token': 123, 'id': 1}"),
            "{'token': '***', 'id': 1}"
        );
        // Text that is not JSON (`redact_uris`): any value type after a quoted secret key.
        let out = redact_uris(r#"x {"token": 123, "api_key": ["live-secret"], "id": 2"#);
        assert!(
            !out.contains("123") && !out.contains("live-secret"),
            "{out}"
        );
        assert!(out.contains(r#""id": 2"#), "{out}");
        // Pipe keys and connector keys are not pairs.
        assert_eq!(redact_text("sql:main"), "sql:main");
        assert_eq!(redact_text("api:token_service"), "api:token_service");
        assert_eq!(
            redact_text("postgresql://u:hunter2@h/db"),
            "postgresql://u:***@h/db"
        );
    }

    #[test]
    fn sql_literals_of_secret_columns_are_redacted() {
        let cases = [
            (
                r#"WHERE "password" = 'hunter2'"#,
                r#"WHERE "password" = '***'"#,
            ),
            ("WHERE `token` != 'hunter2'", "WHERE `token` != '***'"),
            ("WHERE [api_key] <> N'hunter2'", "WHERE [api_key] <> '***'"),
            ("WHERE secret = 123", "WHERE secret = '***'"),
            (
                r#"WHERE t."client_secret" IN ('hunter2', 'it''s')"#,
                r#"WHERE t."client_secret" IN ('***')"#,
            ),
            (
                r#"AND ("token" NOT IN ('hunter2')) = 1"#,
                r#"AND ("token" NOT IN ('***')) = 1"#,
            ),
            (
                r#"CAST("password" AS TEXT) = '{"a": "hunter2"}'"#,
                r#"CAST("password" AS TEXT) = '***'"#,
            ),
            (
                "WHERE `password` = _utf8mb4 X'68756e74657232'",
                "WHERE `password` = '***'",
            ),
            (
                r#"UPDATE "users" SET "password_hash" = 'hunter2' WHERE "username" = 'bob'"#,
                r#"UPDATE "users" SET "password_hash" = '***' WHERE "username" = 'bob'"#,
            ),
            (
                r#"WHERE "token" LIKE 'hun%'"#,
                r#"WHERE "token" LIKE '***'"#,
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(redact_sql(sql), expected);
        }
        // Other columns, NULL checks and the rest of the statement are untouched.
        let sql = r#"SELECT "id", "password" FROM "t" WHERE "id" = 'x' AND "password" IS NULL"#;
        assert_eq!(redact_sql(sql), sql);
        assert_eq!(
            redact_sql("ATTACH 'postgresql://u:hunter2@h/db' AS x"),
            "ATTACH 'postgresql://u:***@h/db' AS x"
        );
        assert_eq!(
            redact_sql(r#"WHERE CAST("params" AS TEXT) = '{"token": 42}'"#),
            r#"WHERE CAST("params" AS TEXT) = '{"token": '***'}'"#
        );
    }
}
