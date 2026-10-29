//! A port of `python-dotenv`'s parser and `load_dotenv(path, override=False, interpolate=True)`
//! (`dotenv/parser.py`, `dotenv/variables.py`, `dotenv/main.py`, v1.x), which Python compose
//! calls in `init_env()` to load the project's `.env` file into the process environment.

use std::sync::LazyLock;

use regex::Regex;

use crate::env::ProcessEnv;

fn re(pattern: &str) -> Regex {
    // Every parser regex is applied with `regex.match(string, pos)`: anchor it.
    Regex::new(&format!("^(?:{pattern})")).expect("valid dotenv regex")
}

static MULTILINE_WHITESPACE: LazyLock<Regex> = LazyLock::new(|| re(r"\s*"));
static WHITESPACE: LazyLock<Regex> = LazyLock::new(|| re(r"[^\S\r\n]*"));
static EXPORT: LazyLock<Regex> = LazyLock::new(|| re(r"(?:export[^\S\r\n]+)?"));
static SINGLE_QUOTED_KEY: LazyLock<Regex> = LazyLock::new(|| re(r"'([^']+)'"));
static UNQUOTED_KEY: LazyLock<Regex> = LazyLock::new(|| re(r"([^=\#\s]+)"));
static EQUAL_SIGN: LazyLock<Regex> = LazyLock::new(|| re(r"(=[^\S\r\n]*)"));
static SINGLE_QUOTED_VALUE: LazyLock<Regex> = LazyLock::new(|| re(r"'((?:\\'|[^'])*)'"));
static DOUBLE_QUOTED_VALUE: LazyLock<Regex> = LazyLock::new(|| re(r#""((?:\\"|[^"])*)""#));
static UNQUOTED_VALUE: LazyLock<Regex> = LazyLock::new(|| re(r"([^\r\n]*)"));
static COMMENT: LazyLock<Regex> = LazyLock::new(|| re(r"(?:[^\S\r\n]*#[^\r\n]*)?"));
static END_OF_LINE: LazyLock<Regex> = LazyLock::new(|| re(r"[^\S\r\n]*(?:\r\n|\n|\r|$)"));
static REST_OF_LINE: LazyLock<Regex> = LazyLock::new(|| re(r"[^\r\n]*(?:\r|\n|\r\n)?"));
static DOUBLE_QUOTE_ESCAPES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"\\[\\'"abfnrtv]"#).expect("valid regex"));
static SINGLE_QUOTE_ESCAPES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\\[\\']").expect("valid regex"));
static UNQUOTED_COMMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s+#.*").expect("valid regex"));
static POSIX_VARIABLE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{(?P<name>[^\}:]*)(?::-(?P<default>[^\}]*))?\}").expect("valid regex")
});

struct ParseError;

struct Reader<'a> {
    text: &'a str,
    pos: usize,
}

impl<'a> Reader<'a> {
    fn has_next(&self) -> bool {
        self.pos < self.text.len()
    }

    fn peek(&self) -> &'a str {
        let rest = &self.text[self.pos..];
        rest.chars().next().map_or("", |c| &rest[..c.len_utf8()])
    }

    fn read_regex(&mut self, regex: &Regex) -> Result<Vec<Option<String>>, ParseError> {
        let rest = &self.text[self.pos..];
        let caps = regex.captures(rest).ok_or(ParseError)?;
        let whole = caps.get(0).ok_or(ParseError)?;
        self.pos += whole.end();
        Ok(caps
            .iter()
            .skip(1)
            .map(|m| m.map(|m| m.as_str().to_string()))
            .collect())
    }
}

fn decode_escapes(regex: &Regex, s: &str) -> String {
    regex
        .replace_all(s, |caps: &regex::Captures| {
            let esc = &caps[0];
            match &esc[1..] {
                "\\" => "\\".to_string(),
                "'" => "'".to_string(),
                "\"" => "\"".to_string(),
                "a" => "\x07".to_string(),
                "b" => "\x08".to_string(),
                "f" => "\x0c".to_string(),
                "n" => "\n".to_string(),
                "r" => "\r".to_string(),
                "t" => "\t".to_string(),
                "v" => "\x0b".to_string(),
                other => format!("\\{other}"),
            }
        })
        .into_owned()
}

fn parse_key(reader: &mut Reader) -> Result<Option<String>, ParseError> {
    match reader.peek() {
        "#" => Ok(None),
        "'" => Ok(reader.read_regex(&SINGLE_QUOTED_KEY)?.remove(0)),
        _ => Ok(reader.read_regex(&UNQUOTED_KEY)?.remove(0)),
    }
}

fn parse_value(reader: &mut Reader) -> Result<String, ParseError> {
    match reader.peek() {
        "'" => {
            let v = reader.read_regex(&SINGLE_QUOTED_VALUE)?.remove(0);
            Ok(decode_escapes(
                &SINGLE_QUOTE_ESCAPES,
                &v.unwrap_or_default(),
            ))
        }
        "\"" => {
            let v = reader.read_regex(&DOUBLE_QUOTED_VALUE)?.remove(0);
            Ok(decode_escapes(
                &DOUBLE_QUOTE_ESCAPES,
                &v.unwrap_or_default(),
            ))
        }
        "" | "\n" | "\r" => Ok(String::new()),
        _ => {
            let part = reader
                .read_regex(&UNQUOTED_VALUE)?
                .remove(0)
                .unwrap_or_default();
            Ok(UNQUOTED_COMMENT
                .replace_all(&part, "")
                .trim_end_matches(char::is_whitespace)
                .to_string())
        }
    }
}

/// One `KEY=value` binding (`value` is `None` for a bare `KEY`).
type Binding = (Option<String>, Option<String>);

fn parse_binding(reader: &mut Reader) -> Binding {
    let start = reader.pos;
    let attempt = (|| -> Result<Binding, ParseError> {
        reader.read_regex(&MULTILINE_WHITESPACE)?;
        if !reader.has_next() {
            return Ok((None, None));
        }
        reader.read_regex(&EXPORT)?;
        let key = parse_key(reader)?;
        reader.read_regex(&WHITESPACE)?;
        let value = if reader.peek() == "=" {
            reader.read_regex(&EQUAL_SIGN)?;
            Some(parse_value(reader)?)
        } else {
            None
        };
        reader.read_regex(&COMMENT)?;
        reader.read_regex(&END_OF_LINE)?;
        Ok((key, value))
    })();
    match attempt {
        Ok(binding) => binding,
        Err(_) => {
            // PARITY: an invalid line is skipped (python-dotenv logs a warning).
            let _ = start;
            let _ = reader.read_regex(&REST_OF_LINE);
            (None, None)
        }
    }
}

/// Parse a `.env` text into its bindings, in order (`parse_stream`).
pub fn parse(text: &str) -> Vec<(String, Option<String>)> {
    let mut reader = Reader { text, pos: 0 };
    let mut out = Vec::new();
    while reader.has_next() {
        let before = reader.pos;
        let (key, value) = parse_binding(&mut reader);
        if let Some(key) = key {
            out.push((key, value));
        }
        if reader.pos == before {
            // Defensive: never loop forever (Python always consumes at least the line).
            break;
        }
    }
    out
}

/// `resolve_variables(values, override=False)`: interpolate `${NAME}` / `${NAME:-default}`, where
/// the process environment takes precedence over values defined earlier in the file.
pub fn resolve_variables(
    values: Vec<(String, Option<String>)>,
    env: &ProcessEnv,
) -> Vec<(String, Option<String>)> {
    let mut new_values: Vec<(String, Option<String>)> = Vec::new();
    for (name, value) in values {
        let result = value.map(|value| {
            let lookup = |var: &str| -> Option<String> {
                if let Some(v) = env.get(var) {
                    return Some(v.to_string());
                }
                new_values
                    .iter()
                    .rev()
                    .find(|(k, _)| k == var)
                    .map(|(_, v)| v.clone().unwrap_or_default())
            };
            let mut out = String::new();
            let mut cursor = 0;
            for caps in POSIX_VARIABLE.captures_iter(&value) {
                let m = caps.get(0).expect("group 0");
                out.push_str(&value[cursor..m.start()]);
                let name = caps.name("name").map_or("", |g| g.as_str());
                let default = caps.name("default").map(|g| g.as_str().to_string());
                out.push_str(&lookup(name).unwrap_or_else(|| default.unwrap_or_default()));
                cursor = m.end();
            }
            out.push_str(&value[cursor..]);
            out
        });
        if let Some(existing) = new_values.iter_mut().find(|(k, _)| *k == name) {
            existing.1 = result;
        } else {
            new_values.push((name, result));
        }
    }
    new_values
}

/// `load_dotenv(path)` with `override=False`: set every variable from the file that is not
/// already in `env`. Returns whether the file defined any variable (Python's return value).
///
/// Honors `PYTHON_DOTENV_DISABLED` like python-dotenv.
pub fn load_into(text: &str, env: &mut ProcessEnv) -> bool {
    if let Some(v) = env.get("PYTHON_DOTENV_DISABLED") {
        let v = v.to_ascii_lowercase();
        if matches!(v.as_str(), "1" | "true" | "t" | "yes" | "y") {
            return false;
        }
    }
    let values = resolve_variables(parse(text), env);
    if values.is_empty() {
        return false;
    }
    for (k, v) in values {
        if env.contains_key(&k) {
            continue;
        }
        if let Some(v) = v {
            env.insert(k, v);
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_like_python_dotenv() {
        let text = "# c\nA=1\nexport B='two words'\nC=\"x\\ny\" # comment\nD=val # trailing\nE\n'F'=f\nbad line here\nG=${A}-${MISSING:-def}\n";
        let parsed = parse(text);
        assert_eq!(
            parsed,
            vec![
                ("A".into(), Some("1".into())),
                ("B".into(), Some("two words".into())),
                ("C".into(), Some("x\ny".into())),
                ("D".into(), Some("val".into())),
                ("E".into(), None),
                ("F".into(), Some("f".into())),
                ("G".into(), Some("${A}-${MISSING:-def}".into())),
            ]
        );
        let mut env = ProcessEnv::new();
        env.insert("A".into(), "process".into());
        assert!(load_into(text, &mut env));
        assert_eq!(env.get("A").map(String::as_str), Some("process"));
        assert_eq!(env.get("G").map(String::as_str), Some("process-def"));
        assert!(!env.contains_key("E"));
    }
}
