//! Ports of Python's `shlex.split`, `shlex.quote` and `shlex.join`.
//!
//! Job labels are `shlex.join(sysargs)` and the systemd unit's `ExecStart=` line is parsed with
//! `shlex.split`, so the exact quoting rules matter for on-disk parity.
// TODO(core-merge): `meerschaum-cli` and `meerschaum-compose` carry their own copies of `split`;
// fold the three into `meerschaum-core::pyfmt` once the crates settle.

/// Errors raised by [`split`] (Python raises `ValueError`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ShlexError {
    /// An opening quote without its closing quote.
    #[error("No closing quotation")]
    NoClosingQuotation,
    /// A trailing backslash.
    #[error("No escaped character")]
    NoEscapedCharacter,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Whitespace,
    Word,
    Quote(char),
    Escape(char),
}

/// `shlex.split(s)` (POSIX mode, no comments).
pub fn split(s: &str) -> Result<Vec<String>, ShlexError> {
    let mut out = Vec::new();
    let mut token = String::new();
    let mut has_token = false;
    let mut state = State::Whitespace;
    for c in s.chars() {
        match state {
            State::Whitespace | State::Word => {
                if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
                    if state == State::Word {
                        out.push(std::mem::take(&mut token));
                        has_token = false;
                    }
                    state = State::Whitespace;
                } else if c == '\\' {
                    has_token = true;
                    state = State::Escape('a');
                } else if c == '\'' || c == '"' {
                    has_token = true;
                    state = State::Quote(c);
                } else {
                    has_token = true;
                    token.push(c);
                    state = State::Word;
                }
            }
            State::Quote(q) => {
                if c == q {
                    state = State::Word;
                } else if q == '"' && c == '\\' {
                    state = State::Escape('"');
                } else {
                    token.push(c);
                }
            }
            State::Escape(prev) => {
                // PARITY: inside double quotes `shlex` drops the backslash only before `"` or `\`.
                if prev == '"' && c != '"' && c != '\\' {
                    token.push('\\');
                }
                token.push(c);
                state = if prev == '"' {
                    State::Quote('"')
                } else {
                    State::Word
                };
            }
        }
    }
    match state {
        State::Quote(_) => return Err(ShlexError::NoClosingQuotation),
        State::Escape(_) => return Err(ShlexError::NoEscapedCharacter),
        _ => {}
    }
    if has_token {
        out.push(token);
    }
    Ok(out)
}

/// `shlex.quote(s)`: `'...'`-quote anything outside `[\w@%+=:,./-]` (ASCII `\w`).
pub fn quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    let safe = s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "_@%+=:,./-".contains(c));
    if safe {
        return s.to_string();
    }
    format!("'{}'", s.replace('\'', "'\"'\"'"))
}

/// `shlex.join(args)`.
pub fn join<S: AsRef<str>>(args: &[S]) -> String {
    args.iter()
        .map(|a| quote(a.as_ref()))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_matches_python() {
        assert_eq!(quote(""), "''");
        assert_eq!(quote("plugin:noaa"), "plugin:noaa");
        assert_eq!(quote("my job"), "'my job'");
        assert_eq!(quote("it's"), "'it'\"'\"'s'");
        assert_eq!(quote("é"), "'é'");
    }

    #[test]
    fn split_join_round_trip() {
        let args = vec!["sql", "main", "SELECT 'x' AS y", "", "a\"b"];
        let joined = join(&args);
        assert_eq!(split(&joined).unwrap(), args);
        assert_eq!(split("a 'b").unwrap_err(), ShlexError::NoClosingQuotation);
    }
}
