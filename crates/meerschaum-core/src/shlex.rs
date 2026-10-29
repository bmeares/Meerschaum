//! Ports of Python's `shlex.split` (POSIX mode, `whitespace_split=True`, no comments),
//! `shlex.quote` and `shlex.join`.
//!
//! Job labels are `shlex.join(sysargs)`, the systemd unit's `ExecStart=` line, compose job
//! commands and the shell's command line are parsed with `shlex.split`, so the exact quoting
//! rules matter for on-disk and transcript parity. One implementation for every crate.

/// Errors raised by [`split`]: Python raises `ValueError` with the same message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ShlexError {
    /// An opening quote without its closing quote.
    #[error("No closing quotation")]
    NoClosingQuotation,
    /// A trailing backslash.
    #[error("No escaped character")]
    NoEscapedCharacter,
}

/// `shlex.split(s)` (POSIX mode, `whitespace_split=True`, no comments): a line-for-line port of
/// the `shlex.shlex.read_token` state machine.
pub fn split(s: &str) -> Result<Vec<String>, ShlexError> {
    const WHITESPACE: &[char] = &[' ', '\t', '\r', '\n'];
    const QUOTES: &[char] = &['\'', '"'];
    const ESCAPE: char = '\\';
    const ESCAPED_QUOTES: &[char] = &['"'];

    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Whitespace,
        Word,
        Quote(char),
        Escape(char),
        Eof,
    }

    let mut chars = s.chars();
    let mut tokens = Vec::new();
    let mut state = State::Whitespace;
    loop {
        // `read_token`
        let mut token = String::new();
        let mut quoted = false;
        let mut escaped_state = State::Word;
        loop {
            let next = chars.next();
            match state {
                State::Eof => break,
                State::Whitespace => match next {
                    None => {
                        state = State::Eof;
                        break;
                    }
                    Some(c) if WHITESPACE.contains(&c) => {
                        if !token.is_empty() || quoted {
                            break;
                        }
                    }
                    Some(ESCAPE) => {
                        escaped_state = State::Word;
                        state = State::Escape(ESCAPE);
                    }
                    Some(c) if QUOTES.contains(&c) => state = State::Quote(c),
                    Some(c) => {
                        token.push(c);
                        state = State::Word;
                    }
                },
                State::Quote(q) => {
                    quoted = true;
                    match next {
                        None => return Err(ShlexError::NoClosingQuotation),
                        Some(c) if c == q => state = State::Word,
                        Some(ESCAPE) if ESCAPED_QUOTES.contains(&q) => {
                            escaped_state = State::Quote(q);
                            state = State::Escape(ESCAPE);
                        }
                        Some(c) => token.push(c),
                    }
                }
                State::Escape(e) => match next {
                    None => return Err(ShlexError::NoEscapedCharacter),
                    Some(c) => {
                        // In quotes, only the quote itself or the escape character may be escaped.
                        if let State::Quote(q) = escaped_state
                            && c != e
                            && c != q
                        {
                            token.push(e);
                        }
                        token.push(c);
                        state = escaped_state;
                    }
                },
                State::Word => match next {
                    None => {
                        state = State::Eof;
                        break;
                    }
                    Some(c) if WHITESPACE.contains(&c) => {
                        state = State::Whitespace;
                        if !token.is_empty() || quoted {
                            break;
                        }
                    }
                    Some(c) if QUOTES.contains(&c) => state = State::Quote(c),
                    Some(ESCAPE) => {
                        escaped_state = State::Word;
                        state = State::Escape(ESCAPE);
                    }
                    Some(c) => token.push(c),
                },
            }
        }
        if token.is_empty() && !quoted {
            // `None` in POSIX mode: end of input.
            break;
        }
        tokens.push(token);
        if state == State::Eof {
            break;
        }
    }
    Ok(tokens)
}

/// `shlex.quote(s)`.
pub fn quote(s: &str) -> String {
    if s.is_empty() {
        return "''".into();
    }
    let safe = |c: char| c.is_ascii_alphanumeric() || "%+,-./:=@_".contains(c);
    if s.chars().all(safe) {
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

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn splits_like_python() {
        assert_eq!(
            split("sql local exec 'SELECT 1' -d").unwrap(),
            s(&["sql", "local", "exec", "SELECT 1", "-d"])
        );
        assert_eq!(
            split(r#"a "b \"c\" \d" e\ f '' "#).unwrap(),
            s(&["a", r#"b "c" \d"#, "e f", ""])
        );
        assert_eq!(
            split("a 'b c' \"d\\\"e\"").unwrap(),
            s(&["a", "b c", "d\"e"])
        );
        assert_eq!(split("echo a\\ b").unwrap(), s(&["echo", "a b"]));
        assert_eq!(split("show \"\" pipes").unwrap(), s(&["show", "", "pipes"]));
        assert_eq!(split("  ").unwrap(), Vec::<String>::new());
        assert_eq!(split("a#b # c").unwrap(), s(&["a#b", "#", "c"]));
        assert_eq!(
            split("echo \"a\\$b\" \"\\x\"").unwrap(),
            s(&["echo", "a\\$b", "\\x"])
        );
        assert_eq!(split("'a").unwrap_err(), ShlexError::NoClosingQuotation);
        assert_eq!(split("a\\").unwrap_err(), ShlexError::NoEscapedCharacter);
        assert_eq!(
            split("a 'b").unwrap_err().to_string(),
            "No closing quotation"
        );
    }

    #[test]
    fn quote_matches_python() {
        assert_eq!(quote(""), "''");
        assert_eq!(quote("plugin:noaa"), "plugin:noaa");
        assert_eq!(quote("sql:main"), "sql:main");
        assert_eq!(quote("my job"), "'my job'");
        assert_eq!(quote("it's"), "'it'\"'\"'s'");
        assert_eq!(quote("\u{e9}"), "'\u{e9}'");
    }

    #[test]
    fn split_join_round_trip() {
        let args = vec!["sql", "main", "SELECT 'x' AS y", "", "a\"b"];
        assert_eq!(split(&join(&args)).unwrap(), args);
    }
}
