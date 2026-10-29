//! Redaction of the job metadata that `GET /jobs` and `GET /jobs/{name}` return (wave 18, FX7).
//!
//! A job's `daemon.properties.env` is the environment of the server's process when the job was
//! created: every `MRSM_SQL_*` / `MRSM_API_*` connector URI (`postgresql://user:password@host/db`)
//! and whatever other secrets the process held. v4 returns it as it is to anyone with `jobs:read`;
//! v5 masks the credentials in every response:
//!
//! - the password of a URI (`scheme://user:PASSWORD@host`) and the value of a `password=` / `token=` /
//!   ... query parameter, wherever a string holds one;
//! - the value of any key whose name says it is a credential (`DB_PASSWORD`, `GITHUB_TOKEN`,
//!   `AWS_SECRET_ACCESS_KEY`, `SOME_SERVICE_KEY`, `password` inside the JSON of `MRSM_CONFIG`);
//! - strings that hold a JSON object or list (`MRSM_CONFIG`, `-P '{...}'`) are parsed, redacted
//!   the same way and written back compactly.
//!
//! The rest of the metadata is untouched. See PARITY.md ("`GET /jobs` redacts credentials").

use serde_json::Value as Json;

/// What replaces a credential.
pub const MASK: &str = "****";

/// Substrings of a (lower-cased) key that make its value a credential.
const SECRET_SUBSTRINGS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "private",
    "apikey",
    "cookie",
];

/// Whole `_`-separated words of a key that make its value a credential (`SOME_SERVICE_KEY`, but not
/// `executor_keys`).
const SECRET_WORDS: &[&str] = &["key", "pass", "auth", "pat", "pw", "pwd_hash"];

/// Query parameters of a URI whose value is masked.
const SECRET_PARAMS: &[&str] = &[
    "password",
    "passwd",
    "pwd",
    "pass",
    "secret",
    "token",
    "apikey",
    "api_key",
    "key",
    "access_token",
    "sas_token",
];

/// Whether a key names a credential.
pub fn is_secret_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    SECRET_SUBSTRINGS.iter().any(|s| lower.contains(s))
        || lower
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|word| SECRET_WORDS.contains(&word))
}

/// Mask the password of every `scheme://user:password@host` in `text`, and the value of the
/// secret query parameters of every URI.
pub fn mask_uri_passwords(text: &str) -> String {
    if !text.contains("://") {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find("://") {
        let (head, tail) = rest.split_at(at + 3);
        out.push_str(head);
        // The URI ends at whitespace or a quote.
        let end = tail
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>'))
            .unwrap_or(tail.len());
        out.push_str(&mask_one_uri(&tail[..end]));
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

/// Mask what follows `scheme://` up to the end of the URI.
fn mask_one_uri(uri: &str) -> String {
    let (before_query, query) = match uri.split_once('?') {
        Some((a, b)) => (a, Some(b)),
        None => (uri, None),
    };
    // The authority is up to the first `/`; the userinfo is before the LAST `@` in it.
    let authority_end = before_query.find('/').unwrap_or(before_query.len());
    let (authority, path) = before_query.split_at(authority_end);
    let authority = match authority.rfind('@') {
        Some(at) => {
            let (userinfo, host) = authority.split_at(at);
            match userinfo.split_once(':') {
                Some((user, _password)) => format!("{user}:{MASK}{host}"),
                None => authority.to_string(),
            }
        }
        None => authority.to_string(),
    };
    let mut out = format!("{authority}{path}");
    if let Some(query) = query {
        out.push('?');
        let masked: Vec<String> = query
            .split('&')
            .map(|pair| match pair.split_once('=') {
                Some((name, _)) if SECRET_PARAMS.contains(&name.to_ascii_lowercase().as_str()) => {
                    format!("{name}={MASK}")
                }
                _ => pair.to_string(),
            })
            .collect();
        out.push_str(&masked.join("&"));
    }
    out
}

/// Redact one value in place (`key` is the name it is stored under, if any).
pub fn redact_value(key: Option<&str>, value: &mut Json) {
    match value {
        Json::Object(map) => {
            for (k, v) in map.iter_mut() {
                redact_value(Some(k), v);
            }
        }
        Json::Array(items) => {
            for item in items {
                redact_value(None, item);
            }
        }
        Json::String(text) => {
            if key.is_some_and(is_secret_key) && !text.is_empty() {
                *text = MASK.to_string();
                return;
            }
            let trimmed = text.trim_start();
            if (trimmed.starts_with('{') || trimmed.starts_with('['))
                && let Ok(mut parsed) = serde_json::from_str::<Json>(text)
            {
                let before = parsed.clone();
                redact_value(None, &mut parsed);
                if parsed != before {
                    *text = parsed.to_string();
                }
                return;
            }
            let masked = mask_uri_passwords(text);
            if masked != *text {
                *text = masked;
            }
        }
        _ => {}
    }
}

/// Redact a job's metadata (the object `GET /jobs` returns per job).
pub fn redact_job_meta(meta: &mut Json) {
    redact_value(None, meta);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn uri_passwords_are_masked() {
        assert_eq!(
            mask_uri_passwords("postgresql://mrsm:hunter2@db:5432/x"),
            "postgresql://mrsm:****@db:5432/x"
        );
        // A `@` in the password and a path with a colon.
        assert_eq!(
            mask_uri_passwords("mysql://u:p@ss:w@h:3306/db"),
            "mysql://u:****@h:3306/db"
        );
        assert_eq!(
            mask_uri_passwords("a https://svc:tok@api.example.com:8000/v1:x b"),
            "a https://svc:****@api.example.com:8000/v1:x b"
        );
        assert_eq!(
            mask_uri_passwords("mssql://u@h/db?driver=x&password=abc&Token=z"),
            "mssql://u@h/db?driver=x&password=****&Token=****"
        );
        for plain in [
            "sqlite:////data/x.db",
            "https://api.example.com:8000/path",
            "no uri here",
            "user:pass@host without a scheme",
        ] {
            assert_eq!(mask_uri_passwords(plain), plain);
        }
    }

    #[test]
    fn secret_keys() {
        for key in [
            "DB_PASSWORD",
            "GITHUB_TOKEN",
            "AWS_SECRET_ACCESS_KEY",
            "SOME_SERVICE_KEY",
            "api-key",
            "password",
            "MRSM_API_SECRET",
            "SSH_AUTH_SOCK",
        ] {
            assert!(is_secret_key(key), "{key}");
        }
        for key in [
            "HOME",
            "PATH",
            "executor_keys",
            "MRSM_SQL_MAIN",
            "pid",
            "keyboard",
        ] {
            assert!(!is_secret_key(key), "{key}");
        }
    }

    #[test]
    fn json_strings_are_redacted_inside() {
        let mut meta = json!({
            "sysargs": ["sync", "pipes", "-P", "{\"password\": \"x\", \"a\": 1}"],
            "env": {"MRSM_CONFIG": "{\"c\": {\"password\": \"p\", \"host\": \"h\"}}", "HOME": "/h"},
        });
        redact_job_meta(&mut meta);
        let text = meta.to_string();
        assert!(
            !text.contains("\\\"x\\\"") && !text.contains("\\\"p\\\""),
            "{text}"
        );
        assert_eq!(meta["env"]["HOME"], "/h");
        assert!(
            meta["env"]["MRSM_CONFIG"]
                .as_str()
                .unwrap()
                .contains("\"host\":\"h\"")
        );
    }
}
