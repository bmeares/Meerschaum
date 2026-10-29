//! The shell's key bindings (`shell:keybindings`): a key runs a command line without touching
//! the line being typed. By default F2 runs `watch run` and F3 `watch jobs` (from
//! [`super::VIEWS`]); v4 bound neither key, so the recorded v4 screens are unchanged.
//!
//! ```yaml
//! shell:
//!   keybindings:
//!     f2: watch run          # the default
//!     f3: null               # unbind a default
//!     ctrl-g: watch jobs -e systemd
//!     f5: show pipes
//! ```
//!
//! Keys are `f1` to `f24` or a character, optionally prefixed with `ctrl-` / `alt-` /
//! `shift-` (`c-`, `a-` / `m-`, `s-`). An empty or `null` command removes the binding.

use crossterm::event::{KeyCode, KeyModifiers};
use meerschaum_core::config::{Config, GetOptions};
use meerschaum_core::value::Value;

/// One binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    /// The key.
    pub code: KeyCode,
    /// Its modifiers.
    pub modifiers: KeyModifiers,
    /// The spelling it was configured with (`f2`).
    pub spec: String,
    /// The command line it runs.
    pub command: String,
}

/// Parse a key spec (`f2`, `ctrl-g`, `alt-w`, `C-x`).
pub fn parse_key(spec: &str) -> Option<(KeyCode, KeyModifiers)> {
    let mut modifiers = KeyModifiers::NONE;
    let mut rest = spec.trim().to_lowercase();
    loop {
        let stripped = [
            ("ctrl-", KeyModifiers::CONTROL),
            ("control-", KeyModifiers::CONTROL),
            ("c-", KeyModifiers::CONTROL),
            ("alt-", KeyModifiers::ALT),
            ("meta-", KeyModifiers::ALT),
            ("a-", KeyModifiers::ALT),
            ("m-", KeyModifiers::ALT),
            ("shift-", KeyModifiers::SHIFT),
            ("s-", KeyModifiers::SHIFT),
        ]
        .into_iter()
        .find_map(|(prefix, m)| {
            // `c-` and friends are prefixes only when something follows.
            (rest.len() > prefix.len() && rest.starts_with(prefix))
                .then(|| (rest[prefix.len()..].to_string(), m))
        });
        match stripped {
            Some((r, m)) => {
                modifiers |= m;
                rest = r;
            }
            None => break,
        }
    }
    let code = if let Some(n) = rest.strip_prefix('f').and_then(|n| n.parse::<u8>().ok()) {
        if !(1..=24).contains(&n) {
            return None;
        }
        KeyCode::F(n)
    } else {
        let mut chars = rest.chars();
        let c = chars.next()?;
        if chars.next().is_some() {
            return None;
        }
        // A bare character would only replace typing: a binding needs Ctrl or Alt.
        if !modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) {
            return None;
        }
        KeyCode::Char(c)
    };
    Some((code, modifiers))
}

/// The default bindings: each view's shell key.
pub fn defaults() -> Vec<(String, String)> {
    super::VIEWS
        .iter()
        .filter_map(|v| v.shell_key)
        .map(|(key, command)| (key.to_string(), command.to_string()))
        .collect()
}

/// The bindings of `shell:keybindings` over the defaults. Invalid keys are returned as
/// warnings and skipped.
pub fn bindings_from(overrides: Option<&Value>) -> (Vec<Binding>, Vec<String>) {
    let mut table: Vec<(String, Option<String>)> =
        defaults().into_iter().map(|(k, c)| (k, Some(c))).collect();
    let mut warnings = Vec::new();
    if let Some(Value::Map(map)) = overrides {
        for (key, command) in map {
            let command = match command {
                Value::Null => None,
                Value::Str(s) if s.trim().is_empty() => None,
                Value::Str(s) => Some(s.trim().to_string()),
                Value::Bool(false) => None,
                other => {
                    warnings.push(format!(
                        "shell:keybindings:{key} must be a command line, not {other:?}."
                    ));
                    continue;
                }
            };
            let Some(parsed) = parse_key(key) else {
                warnings.push(format!("shell:keybindings: unknown key '{key}'."));
                continue;
            };
            table.retain(|(k, _)| parse_key(k) != Some(parsed));
            table.push((key.clone(), command));
        }
    }
    let bindings = table
        .into_iter()
        .filter_map(|(spec, command)| {
            let command = command?;
            let (code, modifiers) = parse_key(&spec)?;
            Some(Binding {
                code,
                modifiers,
                spec,
                command,
            })
        })
        .collect();
    (bindings, warnings)
}

/// The bindings configured for the shell (no warning for a missing `shell:keybindings`).
pub fn bindings(config: &Config) -> (Vec<Binding>, Vec<String>) {
    let (valid, value) = config.get_with(
        &["shell", "keybindings"],
        GetOptions {
            patch: false,
            substitute: true,
            write_missing: false,
            warn: false,
        },
    );
    bindings_from(valid.then_some(&value))
}

/// The command bound to a key press.
pub fn lookup(bindings: &[Binding], code: KeyCode, modifiers: KeyModifiers) -> Option<&str> {
    // Terminals report Shift with F-keys inconsistently: compare without it unless bound.
    bindings
        .iter()
        .find(|b| b.code == code && b.modifiers == modifiers)
        .or_else(|| {
            bindings.iter().find(|b| {
                b.code == code
                    && matches!(code, KeyCode::F(_))
                    && b.modifiers == (modifiers - KeyModifiers::SHIFT)
            })
        })
        .map(|b| b.command.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_key_specs() {
        assert_eq!(parse_key("F2"), Some((KeyCode::F(2), KeyModifiers::NONE)));
        assert_eq!(
            parse_key(" f12 "),
            Some((KeyCode::F(12), KeyModifiers::NONE))
        );
        assert_eq!(
            parse_key("ctrl-g"),
            Some((KeyCode::Char('g'), KeyModifiers::CONTROL))
        );
        assert_eq!(
            parse_key("C-A-w"),
            Some((
                KeyCode::Char('w'),
                KeyModifiers::CONTROL | KeyModifiers::ALT
            ))
        );
        assert_eq!(
            parse_key("shift-f3"),
            Some((KeyCode::F(3), KeyModifiers::SHIFT))
        );
        assert_eq!(parse_key("f0"), None);
        assert_eq!(parse_key("f25"), None);
        assert_eq!(parse_key("x"), None, "bare characters stay typing");
        assert_eq!(parse_key("ctrl-xy"), None);
        assert_eq!(parse_key(""), None);
    }

    #[test]
    fn defaults_and_overrides() {
        let (b, w) = bindings_from(None);
        assert!(w.is_empty());
        assert_eq!(
            lookup(&b, KeyCode::F(2), KeyModifiers::NONE),
            Some("watch run")
        );
        assert_eq!(
            lookup(&b, KeyCode::F(3), KeyModifiers::NONE),
            Some("watch jobs")
        );
        assert_eq!(
            lookup(&b, KeyCode::F(3), KeyModifiers::SHIFT),
            Some("watch jobs")
        );
        assert_eq!(lookup(&b, KeyCode::F(4), KeyModifiers::NONE), None);
        let overrides = Value::from_json_str(
            r#"{"F3": null, "ctrl-g": "watch jobs -e systemd", "f2": "watch run --nopretty", "nope": "x", "f5": 3}"#,
        )
        .unwrap();
        let (b, w) = bindings_from(Some(&overrides));
        assert_eq!(lookup(&b, KeyCode::F(3), KeyModifiers::NONE), None);
        assert_eq!(
            lookup(&b, KeyCode::Char('g'), KeyModifiers::CONTROL),
            Some("watch jobs -e systemd")
        );
        assert_eq!(
            lookup(&b, KeyCode::F(2), KeyModifiers::NONE),
            Some("watch run --nopretty")
        );
        assert_eq!(w.len(), 2, "{w:?}");
    }
}
