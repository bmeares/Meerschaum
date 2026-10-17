//! Ports of Python's `shlex.split` (POSIX mode, `whitespace_split=True`, no comments),
//! `shlex.quote` and `shlex.join`.

/// `shlex.split(s)`. Errors carry the Python `ValueError` message.
pub fn split(s: &str) -> Result<Vec<String>, String> {
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
                        None => return Err("No closing quotation".into()),
                        Some(c) if c == q => state = State::Word,
                        Some(ESCAPE) if ESCAPED_QUOTES.contains(&q) => {
                            escaped_state = State::Quote(q);
                            state = State::Escape(ESCAPE);
                        }
                        Some(c) => token.push(c),
                    }
                }
                State::Escape(e) => match next {
                    None => return Err("No escaped character".into()),
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

    #[test]
    fn split_like_python() {
        assert_eq!(
            split("a 'b c' \"d\\\"e\"").unwrap(),
            vec!["a", "b c", "d\"e"]
        );
        assert_eq!(split("echo a\\ b").unwrap(), vec!["echo", "a b"]);
        assert_eq!(split("show \"\" pipes").unwrap(), vec!["show", "", "pipes"]);
        assert_eq!(split("   ").unwrap(), Vec::<String>::new());
        assert!(split("show \"unclosed").is_err());
        assert!(split("show \\").is_err());
        assert_eq!(
            split("echo \"a\\$b\" \"\\x\"").unwrap(),
            vec!["echo", "a\\$b", "\\x"]
        );
    }

    #[test]
    fn quote_like_python() {
        assert_eq!(quote(""), "''");
        assert_eq!(quote("sql:main"), "sql:main");
        assert_eq!(quote("it's"), "'it'\"'\"'s'");
        assert_eq!(quote("a b"), "'a b'");
    }
}
