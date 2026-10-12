//! A port of Python's `shlex.split(s)` (POSIX mode, no comments), used to split compose job
//! commands and `sync.args` exactly the way the Python implementation does.

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

/// `shlex.split(s)`: POSIX shell-like splitting with `'` / `"` quotes and `\` escapes.
///
/// Like Python's `shlex` in POSIX mode with `whitespace_split=True` and no comment characters:
/// inside double quotes a backslash only escapes `"` and `\`; inside single quotes it is literal.
pub fn split(s: &str) -> Result<Vec<String>, ShlexError> {
    let mut out = Vec::new();
    let mut token = String::new();
    // Whether the current token exists even if empty (`''` yields an empty argument).
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
                // PARITY: `shlex` drops the backslash only when it escapes the quote character or
                // itself inside double quotes; outside quotes, any character is escaped.
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
    if has_token || !token.is_empty() {
        out.push(token);
    }
    Ok(out)
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
        assert_eq!(split("  ").unwrap(), Vec::<String>::new());
        assert_eq!(split("a#b # c").unwrap(), s(&["a#b", "#", "c"]));
        assert_eq!(split("'a").unwrap_err(), ShlexError::NoClosingQuotation);
        assert_eq!(split("a\\").unwrap_err(), ShlexError::NoEscapedCharacter);
    }
}
