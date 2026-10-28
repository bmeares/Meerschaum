//! Connector attributes with every secret masked, by the rules of the web console's Connectors
//! page (`meerschaum/api/dash/connector_listing.py`, ported here function by function):
//! `show connectors` prints what the console shows, so the CLI and `POST /execute` (which runs
//! actions for remote users) never print a password, token, key or other secret.
//!
//! - The values of a connector's connection settings are shown ([`PUBLIC_ATTRIBUTES`]: flavor,
//!   host, port, database, username, driver, timeouts, ...); every other attribute's value is
//!   masked whatever its name, so a secret under a name nobody listed (`pass`, `pw`,
//!   `ssh_passphrase`, `cookie`) is never printed.
//! - A URI or a connection string ([`URI_NAMES`]: `uri`, `dsn`, `connection_string`, ...) keeps
//!   its scheme, user, host, port, database (for `sql:` and `valkey:` connectors) and public
//!   settings; its password, a token and everything else are masked.
//! - The values of secret-looking names ([`SECRET_WORDS`]) and the passwords of URIs are then
//!   scrubbed from every field shown.
//!
//! The mask is ASCII ([`MASK`], `******`) so plain and `--nopretty` output stay ASCII; the
//! console draws `••••••`. A pytest (`tests/api_host/test_show_connectors_masking.py`) compares
//! the two implementations on a corpus.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use meerschaum_core::pyfmt;
use meerschaum_core::value::{Map, Value};

/// What a masked value shows.
pub const MASK: &str = "******";

/// A name as the lists below compare it: lowercase letters and digits only (`_norm`).
fn norm(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        .collect()
}

/// The attributes whose values are shown: where and how a connector connects. Every other
/// attribute's value is masked whatever its name. Compared by `norm`
/// (`TrustServerCertificate` is `trust_server_certificate`), also for the settings inside URIs and
/// connection strings (`Server=h;Uid=u;Pwd=p`).
pub const PUBLIC_ATTRIBUTES: &[&str] = &[
    // Where.
    "flavor",
    "protocol",
    "host",
    "hostname",
    "hostaddr",
    "server",
    "data_source",
    "port",
    "database",
    "dbname",
    "db",
    "initial_catalog",
    "schema",
    "service_name",
    "sid",
    "region",
    // Who (a user's name; never its password).
    "username",
    "user",
    "uid",
    // How.
    "driver",
    "dialect",
    "sslmode",
    "ssl",
    "tls",
    "encrypt",
    "verify",
    "trust_server_certificate",
    "use_fmtonly",
    "mars_connection",
    "application_name",
    "charset",
    "encoding",
    "timezone",
    "isolation_level",
    "autocommit",
    "echo",
    "connect_timeout",
    "timeout",
    "socket_timeout",
    "command_timeout",
    "pool_size",
    "max_overflow",
    "pool_recycle",
    "pool_pre_ping",
    "pool_timeout",
    "fast_executemany",
    "use_insertmanyvalues",
    "use_setinputsizes",
    "ignore_no_transaction_on_rollback",
    "protected",
    // A connector's own keys.
    "label",
    "type",
];

/// Attributes that hold a URI or a connection string (masked by [`mask_connection_string`]).
pub const URI_NAMES: &[&str] = &[
    "uri",
    "dsn",
    "connection_string",
    "conn_str",
    "odbc_connect",
    "options",
];

/// The connector types whose URI path is their database (`sqlite:////path/to.db`,
/// `redis://host:6379/0`). Any other type's path is masked: a webhook URL's path is its secret.
const DATABASE_PATH_TYPES: &[&str] = &["sql", "valkey"];

/// Words that make a name look like a secret's (`password`, `api_key`, `ssh_passphrase`): its
/// value is masked in any text shown (`Pwd=...` in a connection string), it is scrubbed from the
/// other fields, and a group of settings under such a name is masked whole.
pub const SECRET_WORDS: &[&str] = &[
    "password",
    "passwd",
    "pass",
    "pwd",
    "pw",
    "secret",
    "token",
    "key",
    "credential",
    "private",
    "auth",
    "phrase",
    "cookie",
    "session",
    "sig",
    "salt",
    "sas",
    "jwt",
    "bearer",
    "otp",
];

/// Schemes whose bare user information (`https://<token>@host`) is usually a token.
const TOKEN_USERINFO_SCHEMES: &[&str] = &["http", "https", "ws", "wss", "git+https"];

/// Values too common to scrub from the text around them (`verify: 'true'` would mask every
/// "true").
const COMMON_VALUES: &[&str] = &["true", "false", "none", "null", "yes", "off"];

static NORMED_PUBLIC: LazyLock<BTreeSet<String>> =
    LazyLock::new(|| PUBLIC_ATTRIBUTES.iter().map(|n| norm(n)).collect());
static NORMED_URI_NAMES: LazyLock<BTreeSet<String>> =
    LazyLock::new(|| URI_NAMES.iter().map(|n| norm(n)).collect());
static PERCENT_ESCAPE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"%[0-9a-fA-F]{2}").expect("valid regex"));

/// `scheme://user:password@` (the password runs to the last `@` of the address, so one holding an
/// unencoded `@`, `/` or `#` is masked whole) and `https://<token>@`.
static URI_CREDENTIALS: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        r"(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*)://(?P<user>[^:/@\s?#]*)(?::(?P<password>\S*))?@",
    )
    .expect("valid regex")
});

/// A secret-looking setting in any text, at any position: `password=...`, `;Pwd=...`,
/// `?sig=...`, `"api_key": "..."`, `token: ...`.
static TEXT_SECRETS: LazyLock<fancy_regex::Regex> = LazyLock::new(|| {
    let secret_name = format!(r"[\w.\-]*(?:{})[\w.\-]*", SECRET_WORDS.join("|"));
    fancy_regex::Regex::new(&format!(
        r#"(?i)(?P<name>(?<![\w.\-])["']?{secret_name}["']?\s*(?:=|:\s*))(?P<value>\{{[^}}]*\}}?|"[^"]*"?|'[^']*'?|[^\s&;,)\]}}]+)"#
    ))
    .expect("valid regex")
});

/// `Bearer <credential>` / `Basic <credential>`.
static HTTP_CREDENTIALS: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?i)\b(?P<scheme>Bearer|Basic)\s+[A-Za-z0-9._~+/=\-]{6,}")
        .expect("valid regex")
});

/// A URI at the start of a value (`re.match`, `DOTALL`).
static URI_START: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?s)\A\s*(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*)://(?P<rest>.*)\z")
        .expect("valid regex")
});

/// A URI's address after its user information (`re.match`, `DOTALL`).
static URI_ADDRESS: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(
        r"(?s)\A(?P<hostport>[^/?#]*)(?P<path>/[^?#]*)?(?:\?(?P<query>[^#]*))?(?:#(?P<fragment>.*))?\z",
    )
    .expect("valid regex")
});

/// Whether a name looks like a secret's (`password`, `api_key`, `client_secret`, `pass`, `pw`,
/// `ssh_passphrase`, `cookie`, ...). More than these is masked: see [`is_public_name`].
pub fn is_secret_name(name: &str) -> bool {
    let lowered = name.to_lowercase();
    SECRET_WORDS.iter().any(|word| lowered.contains(word))
}

/// Whether the value of an attribute (or setting) of this name is shown.
pub fn is_public_name(name: &str) -> bool {
    NORMED_PUBLIC.contains(&norm(name))
}

/// Any text with what looks like a secret masked: URI credentials (`scheme://user:******@host`,
/// a bare `https://******@host`), secret-looking settings anywhere (`password=******`,
/// `Pwd=******;`, `?sig=******`, `"api_key": ******`) and `Bearer` / `Basic` credentials.
pub fn mask_text(text: &str) -> String {
    let masked = HTTP_CREDENTIALS.replace_all(text, |caps: &regex::Captures<'_>| {
        format!("{} {MASK}", &caps["scheme"])
    });
    let masked = URI_CREDENTIALS.replace_all(&masked, |caps: &regex::Captures<'_>| {
        let scheme = &caps["scheme"];
        let user = &caps["user"];
        if caps.name("password").is_some() {
            return format!("{scheme}://{user}:{MASK}@");
        }
        if !user.is_empty() && TOKEN_USERINFO_SCHEMES.contains(&scheme.to_lowercase().as_str()) {
            return format!("{scheme}://{MASK}@");
        }
        caps[0].to_string()
    });
    TEXT_SECRETS
        .replace_all(&masked, |caps: &fancy_regex::Captures<'_, str>| {
            let name = caps.name("name").map(|m| m.as_str()).unwrap_or_default();
            format!("{name}{MASK}")
        })
        .into_owned()
}

/// `text` split on `separator`: `'&'` (a URL query), `';'` outside braces (ODBC,
/// `Driver={a;b};...`) or `' '`, any whitespace outside quotes (libpq, `password='a b'`).
fn split_settings(text: &str, separator: char) -> Vec<String> {
    if separator == '&' {
        return text.split('&').map(str::to_string).collect();
    }
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut depth = 0usize;
    for ch in text.chars() {
        if let Some(q) = quote {
            current.push(ch);
            if ch == q {
                quote = None;
            }
            continue;
        }
        if separator == ' ' && (ch == '"' || ch == '\'') {
            quote = Some(ch);
        } else if separator == ';' && ch == '{' {
            depth += 1;
        } else if separator == ';' && ch == '}' && depth > 0 {
            depth -= 1;
        } else if depth == 0
            && (if separator == ' ' {
                ch.is_whitespace()
            } else {
                ch == separator
            })
        {
            parts.push(std::mem::take(&mut current));
            continue;
        }
        current.push(ch);
    }
    parts.push(current);
    parts
}

/// `key=value` settings (`a=1&b=2`, `Driver={x};Pwd=p;`, `host=h password='p q'`) with the value
/// of every key that is not public masked, and any text that is not a setting masked too:
/// `(masked text, hidden values)`.
fn mask_settings(text: &str, separator: Option<char>) -> (String, Vec<String>) {
    let separator = separator.unwrap_or(if text.contains('&') {
        '&'
    } else if text.contains(';') {
        ';'
    } else {
        ' '
    });
    let mut masked: Vec<String> = Vec::new();
    let mut hidden: Vec<String> = Vec::new();
    for part in split_settings(text, separator) {
        if part.trim().is_empty() {
            masked.push(part);
            continue;
        }
        match part.split_once('=') {
            Some((key, _)) if is_public_name(key) => masked.push(part.clone()),
            Some((key, value)) => {
                if value.trim().is_empty() {
                    masked.push(part.clone());
                } else {
                    masked.push(format!("{key}={MASK}"));
                    hidden.push(value.trim().to_string());
                }
            }
            None => {
                masked.push(MASK.to_string());
                hidden.push(part.trim().to_string());
            }
        }
    }
    let joiner = if separator == ' ' {
        " ".to_string()
    } else {
        separator.to_string()
    };
    (masked.join(&joiner), hidden)
}

/// [`mask_connection_string`] and the values it hid.
fn mask_connection(text: &str, typ: Option<&str>) -> (String, Vec<String>) {
    if text.trim().is_empty() {
        return (text.to_string(), Vec::new());
    }
    let Some(start) = URI_START.captures(text) else {
        if text.contains('=') {
            return mask_settings(text, None);
        }
        return (MASK.to_string(), vec![text.trim().to_string()]);
    };
    let scheme = start["scheme"].to_string();
    let mut rest = start["rest"].to_string();
    let mut hidden: Vec<String> = Vec::new();
    let mut userinfo = String::new();
    // The address starts after the last `@`: a password with an unencoded `@` is hidden whole.
    if let Some(at) = rest.rfind('@') {
        let credentials = rest[..at].to_string();
        rest = rest[at + 1..].to_string();
        let (user, password) = match credentials.split_once(':') {
            Some((u, p)) => (u.to_string(), Some(p.to_string())),
            None => (credentials.clone(), None),
        };
        if user.chars().any(|c| "/?#".contains(c)) {
            userinfo = MASK.to_string();
            hidden.push(credentials);
        } else if let Some(password) = password {
            userinfo = format!("{user}:{MASK}");
            hidden.push(password);
        } else if !user.is_empty()
            && TOKEN_USERINFO_SCHEMES.contains(&scheme.to_lowercase().as_str())
        {
            userinfo = MASK.to_string();
            hidden.push(user);
        } else {
            userinfo = user;
        }
        userinfo.push('@');
    }
    let Some(address) = URI_ADDRESS.captures(&rest) else {
        hidden.push(rest.clone());
        return (format!("{scheme}://{userinfo}{MASK}"), hidden);
    };
    let mut hostport = address
        .name("hostport")
        .map(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    if let Some((host, port)) = hostport.rsplit_once(':')
        && !port.is_empty()
        && !port.chars().all(char::is_numeric)
        && !hostport.ends_with(']')
    {
        let host = host.to_string();
        hidden.push(port.to_string());
        hostport = format!("{host}:{MASK}");
    }
    let mut path = address
        .name("path")
        .map(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    if !path.trim_matches('/').is_empty() && !typ.is_some_and(|t| DATABASE_PATH_TYPES.contains(&t))
    {
        hidden.push(path.trim_matches('/').to_string());
        path = format!("/{MASK}");
    }
    let mut shown = format!("{scheme}://{userinfo}{hostport}{path}");
    if let Some(query) = address.name("query") {
        let (masked_query, query_hidden) = mask_settings(query.as_str(), Some('&'));
        shown.push('?');
        shown.push_str(&masked_query);
        hidden.extend(query_hidden);
    }
    if let Some(fragment) = address.name("fragment")
        && !fragment.as_str().is_empty()
    {
        shown.push('#');
        shown.push_str(MASK);
        hidden.push(fragment.as_str().to_string());
    }
    (shown, hidden)
}

/// A URI or connection string as it is shown. A URI keeps its scheme, its user, its host and
/// port, its path when that is the database (`sql:` and `valkey:` connectors) and its public
/// settings (`?sslmode=require`); a `key=value` string (`host=h user=u password=p`,
/// `Driver={...};Server=h;Pwd=p`) keeps its public settings. Everything else is masked.
pub fn mask_connection_string(text: &str, typ: Option<&str>) -> String {
    mask_connection(text, typ).0
}

/// The passwords and tokens of the URIs in `text`, and its secret-looking settings' values.
fn uri_secrets(text: &str) -> Vec<String> {
    let mut secrets = Vec::new();
    for view in decoded_views(text) {
        for caps in URI_CREDENTIALS.captures_iter(&view) {
            let password = caps
                .name("password")
                .map(|m| m.as_str())
                .unwrap_or_default();
            let user = &caps["user"];
            if !password.is_empty() {
                secrets.push(password.to_string());
            } else if !user.is_empty()
                && TOKEN_USERINFO_SCHEMES.contains(&caps["scheme"].to_lowercase().as_str())
            {
                secrets.push(user.to_string());
            }
        }
        for caps in TEXT_SECRETS.captures_iter(&view).flatten() {
            let value = caps
                .name("value")
                .map(|m| m.as_str())
                .unwrap_or_default()
                .trim_matches(|c| "'\"{}".contains(c));
            if !value.is_empty() {
                secrets.push(value.to_string());
            }
        }
    }
    secrets
}

/// `str(value)` for a scalar, as Python prints it.
fn text_of(value: &Value) -> String {
    pyfmt::py_str(value)
}

/// The texts of the plain values inside `value` (a dict's values, a list's items).
fn leaves(value: &Value) -> Vec<String> {
    match value {
        Value::Map(map) => map.values().flat_map(leaves).collect(),
        Value::List(items) => items.iter().flat_map(leaves).collect(),
        Value::Null | Value::Bool(_) => Vec::new(),
        other => {
            let text = text_of(other).trim().to_string();
            if text.is_empty() { vec![] } else { vec![text] }
        }
    }
}

/// The secrets in a connector's attributes (nested values included): the values of
/// secret-looking names ([`is_secret_name`]), the passwords and tokens of URIs and the values of
/// secret-looking settings in strings. [`scrub`] masks them in any other text.
pub fn collect_secrets(attributes: &Value, name: &str) -> Vec<String> {
    let mut secrets = Vec::new();
    match attributes {
        Value::Map(map) => {
            for (key, value) in map {
                if is_secret_name(key) && matches!(value, Value::Map(_) | Value::List(_)) {
                    // A group of settings under a secret's name is a secret whole.
                    secrets.extend(leaves(value));
                    continue;
                }
                secrets.extend(collect_secrets(value, key));
            }
        }
        Value::List(items) => {
            for item in items {
                secrets.extend(collect_secrets(item, name));
            }
        }
        Value::Null | Value::Bool(_) => {}
        other => {
            let text = text_of(other);
            if is_secret_name(name) && !text.is_empty() {
                secrets.push(text.clone());
            }
            secrets.extend(uri_secrets(&text));
        }
    }
    secrets
}

/// `urllib.parse.unquote(text)`: percent-decoding as UTF-8, invalid bytes replaced.
fn unquote(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (
                (bytes[i + 1] as char).to_digit(16),
                (bytes[i + 2] as char).to_digit(16),
            )
        {
            out.push((h * 16 + l) as u8);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Bounded URL / form decoding, including encoded names and mixed-case percent escapes.
fn decoded_views(text: &str) -> BTreeSet<String> {
    let mut views = BTreeSet::from([text.to_string()]);
    for form in [false, true] {
        let mut view = text.to_string();
        for _ in 0..4 {
            let decoded = unquote(&if form {
                view.replace('+', " ")
            } else {
                view.clone()
            });
            if decoded == view {
                break;
            }
            views.insert(decoded.clone());
            view = decoded;
        }
    }
    views
}

/// `text` with what looks like a secret ([`mask_text`]) and every known secret value masked.
pub fn scrub(text: &str, secrets: &[String]) -> String {
    let mut text = text.to_string();
    let mut candidates: BTreeSet<String> = BTreeSet::new();
    let mut short: BTreeSet<String> = BTreeSet::new();
    for secret in secrets {
        let secret = secret.trim().to_string();
        // Also the decoded form of a percent-encoded password and the parts of one holding an
        // `@` (a URI parser may split it there): an error may quote any of them.
        let forms = decoded_views(&secret);
        let mut all: BTreeSet<String> = forms.clone();
        for form in &forms {
            all.extend(form.split('@').map(str::to_string));
        }
        for candidate in all {
            let candidate = candidate.trim().to_string();
            if candidate.is_empty() || candidate == MASK {
                continue;
            }
            if candidate.chars().count() >= 3
                && !COMMON_VALUES.contains(&candidate.to_lowercase().as_str())
            {
                candidates.insert(candidate.clone());
            } else {
                short.insert(candidate.clone());
            }
            // Mask known representations before heuristic masking can split their escapes.
            for quoted in [
                pyfmt::str_repr(&candidate),
                pyfmt::json_dumps_str(&candidate, true),
            ] {
                let inner = &quoted[1..quoted.len() - 1];
                if inner != candidate.as_str() {
                    candidates.insert(inner.to_string());
                }
                candidates.insert(quoted);
            }
        }
    }
    // Longest first, so a secret containing another is masked whole.
    let mut ordered: Vec<&String> = candidates.iter().collect();
    ordered.sort_by_key(|c| std::cmp::Reverse(c.chars().count()));
    for secret in ordered {
        if secret.chars().count() >= 3 {
            text = text.replace(secret.as_str(), MASK);
        }
    }
    let short_patterns: Vec<fancy_regex::Regex> = short
        .iter()
        .map(|secret| {
            fancy_regex::Regex::new(&format!(r"(?<!\w){}(?!\w)", regex::escape(secret)))
                .expect("escaped credential pattern")
        })
        .collect();
    for pattern in &short_patterns {
        text = pattern.replace_all(&text, MASK).into_owned();
    }
    // Decode only for comparison. Conceal the whole value when its original span is obscured.
    let views = decoded_views(&text);
    // A valid escape after the comparison budget may conceal further layers. Fail closed.
    if (!candidates.is_empty() || !short.is_empty())
        && views
            .iter()
            .min_by_key(|view| view.chars().count())
            .is_some_and(|view| PERCENT_ESCAPE.is_match(view))
    {
        return MASK.to_string();
    }
    if views.iter().any(|view| {
        candidates
            .iter()
            .any(|secret| secret.chars().count() >= 3 && view.contains(secret.as_str()))
            || short_patterns
                .iter()
                .any(|pattern| pattern.is_match(view).unwrap_or(true))
    }) {
        return MASK.to_string();
    }
    mask_text(&text)
}

/// `value` (of attribute `name`) as it is shown; each value masked is added to `hidden`. Shown
/// text is scrubbed of `secrets` (unless `None`).
fn mask_tree(
    name: &str,
    value: &Value,
    typ: Option<&str>,
    secrets: Option<&[String]>,
    hidden: &mut Vec<String>,
) -> Value {
    match value {
        Value::Map(map) => {
            if map.is_empty() {
                return Value::Map(Map::new());
            }
            if is_secret_name(name) && !is_public_name(name) {
                hidden.extend(leaves(value));
                return Value::Str(MASK.into());
            }
            Value::Map(
                map.iter()
                    .filter(|(key, _)| !key.starts_with('_'))
                    .map(|(key, val)| (key.clone(), mask_tree(key, val, typ, secrets, hidden)))
                    .collect(),
            )
        }
        Value::Null | Value::Bool(_) => value.clone(),
        Value::Str(s) if s.is_empty() => value.clone(),
        Value::Str(s) if NORMED_URI_NAMES.contains(&norm(name)) => {
            let (shown, values) = mask_connection(s, typ);
            hidden.extend(values);
            Value::Str(match secrets {
                Some(secrets) => scrub(&shown, secrets),
                None => shown,
            })
        }
        _ if !is_public_name(name) => {
            hidden.extend(leaves(value));
            Value::Str(MASK.into())
        }
        Value::List(items) => Value::List(
            items
                .iter()
                .map(|item| mask_tree(name, item, typ, secrets, hidden))
                .collect(),
        ),
        Value::Int(_) | Value::Float(_) => {
            let text = text_of(value);
            if secrets.is_some_and(|secrets| scrub(&text, secrets) != text) {
                Value::Str(MASK.into())
            } else {
                value.clone()
            }
        }
        Value::Str(s) => Value::Str(match secrets {
            Some(secrets) => scrub(s, secrets),
            None => s.clone(),
        }),
    }
}

/// A copy of attribute `name`'s value as it is shown: the value of a public name
/// ([`PUBLIC_ATTRIBUTES`]) scrubbed of `secrets`, a URI or connection string masked by
/// [`mask_connection_string`], a group of settings with each setting judged by its own name, and
/// anything else masked.
pub fn mask_value(name: &str, value: &Value, secrets: &[String], typ: Option<&str>) -> Value {
    mask_tree(name, value, typ, Some(secrets), &mut Vec::new())
}

/// A connector's attributes with every value that is not shown masked (keys starting with `_`
/// are left out). `secrets` defaults to [`hidden_values`] of the attributes.
pub fn mask_attributes(attributes: &Map, typ: Option<&str>, secrets: Option<&[String]>) -> Map {
    let collected;
    let secrets = match secrets {
        Some(s) => s,
        None => {
            collected = hidden_values(&Value::Map(attributes.clone()), "", typ);
            &collected
        }
    };
    attributes
        .iter()
        .filter(|(key, _)| !key.starts_with('_'))
        .map(|(key, value)| (key.clone(), mask_value(key, value, secrets, typ)))
        .collect()
}

/// Every value masked in a connector's attributes ([`mask_attributes`]), its secrets
/// ([`collect_secrets`]) included.
pub fn hidden_values(attributes: &Value, name: &str, typ: Option<&str>) -> Vec<String> {
    let mut hidden = Vec::new();
    mask_tree(name, attributes, typ, None, &mut hidden);
    hidden.extend(collect_secrets(attributes, name));
    hidden
}

/// The connectors section of the configuration (`meerschaum:connectors`, or one type's labels
/// when `typ` is given) with every connector's attributes masked: each label (and the type's
/// `default`) by [`mask_attributes`], scrubbed of the secrets of the attributes it inherits
/// from `default` too. A value that is not a connector's attributes is masked whole.
pub fn mask_connectors_config(config: &Value, typ: Option<&str>) -> Value {
    match typ {
        Some(typ) => mask_labels(config, typ),
        None => match config {
            Value::Map(types) => Value::Map(
                types
                    .iter()
                    .map(|(typ, labels)| (typ.clone(), mask_labels(labels, typ)))
                    .collect(),
            ),
            other => mask_loose(other),
        },
    }
}

/// One connector type's labels, masked.
fn mask_labels(labels: &Value, typ: &str) -> Value {
    let Value::Map(labels) = labels else {
        return mask_loose(labels);
    };
    let inherited: Map = labels
        .get("default")
        .and_then(Value::as_map)
        .cloned()
        .unwrap_or_default();
    Value::Map(
        labels
            .iter()
            .map(|(label, attributes)| {
                let Value::Map(attributes) = attributes else {
                    return (label.clone(), mask_loose(attributes));
                };
                let merged = if label == "default" {
                    attributes.clone()
                } else {
                    let mut merged = inherited.clone();
                    merged.extend(attributes.iter().map(|(k, v)| (k.clone(), v.clone())));
                    merged
                };
                let secrets = hidden_values(&Value::Map(merged), "", Some(typ));
                (
                    label.clone(),
                    Value::Map(mask_attributes(attributes, Some(typ), Some(&secrets))),
                )
            })
            .collect(),
    )
}

/// A value where a connector's attributes were expected: shown only when it holds nothing.
fn mask_loose(value: &Value) -> Value {
    match value {
        Value::Null | Value::Bool(_) => value.clone(),
        Value::Str(s) if s.is_empty() => value.clone(),
        Value::Map(m) if m.is_empty() => value.clone(),
        _ => Value::Str(MASK.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, Value)]) -> Map {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    fn s(text: &str) -> Value {
        Value::Str(text.into())
    }

    #[test]
    fn public_settings_are_shown_and_everything_else_is_masked() {
        let attrs = map(&[
            ("flavor", s("postgresql")),
            ("host", s("db.example.com")),
            ("port", Value::Int(5432)),
            ("username", s("mrsm")),
            ("password", s("hunter2!")),
            ("database", s("meerschaum")),
            ("api_key", s("abc123")),
            ("passphrase", s("open sesame")),
            ("pw", s("x1y2z3")),
            ("stations", Value::List(vec![s("a"), s("b")])),
            ("verify", Value::Bool(false)),
            ("_private", s("never shown")),
            ("empty", s("")),
            ("nothing", Value::Null),
        ]);
        let shown = mask_attributes(&attrs, Some("sql"), None);
        assert_eq!(shown["flavor"], s("postgresql"));
        assert_eq!(shown["host"], s("db.example.com"));
        assert_eq!(shown["port"], Value::Int(5432));
        assert_eq!(shown["username"], s("mrsm"));
        assert_eq!(shown["database"], s("meerschaum"));
        for key in ["password", "api_key", "passphrase", "pw", "stations"] {
            assert_eq!(shown[key], s(MASK), "{key}");
        }
        // Booleans, empty strings and nulls hold nothing.
        assert_eq!(shown["verify"], Value::Bool(false));
        assert_eq!(shown["empty"], s(""));
        assert_eq!(shown["nothing"], Value::Null);
        assert!(!shown.contains_key("_private"));
    }

    #[test]
    fn uris_keep_their_address_and_lose_their_credentials() {
        let shown = |uri: &str, typ: &str| mask_connection_string(uri, Some(typ));
        assert_eq!(
            shown(
                "postgresql://mrsm:hunter2@db:5432/meerschaum?sslmode=require",
                "sql"
            ),
            "postgresql://mrsm:******@db:5432/meerschaum?sslmode=require"
        );
        // A password holding an unencoded `@` is hidden whole.
        assert_eq!(
            shown("mysql://u:p@ss@h:3306/db", "sql"),
            "mysql://u:******@h:3306/db"
        );
        // A non-public query setting and the fragment are masked.
        assert_eq!(
            shown("mssql://u:p@h/db?driver=x&password=abc#frag", "sql"),
            "mssql://u:******@h/db?driver=x&password=******#******"
        );
        // A bare token as user information, and a webhook's path.
        assert_eq!(
            shown("https://tok3n@hooks.example.com/services/T0/B1", "api"),
            "https://******@hooks.example.com/******"
        );
        // A database path stays for `sql:` and `valkey:` connectors.
        assert_eq!(
            shown("sqlite:////var/data/x.db", "sql"),
            "sqlite:////var/data/x.db"
        );
        assert_eq!(
            shown("redis://:secret@cache:6379/0", "valkey"),
            "redis://:******@cache:6379/0"
        );
        // ODBC and libpq connection strings keep their public settings.
        assert_eq!(
            shown("Driver={ODBC 18};Server=h;Uid=u;Pwd=p@ss;", "sql"),
            "Driver={ODBC 18};Server=h;Uid=u;Pwd=******;"
        );
        assert_eq!(
            shown("host=h user=u password='a b' dbname=d", "sql"),
            "host=h user=u password=****** dbname=d"
        );
        // Text that is neither is masked whole.
        assert_eq!(shown("opaque-thing", "sql"), MASK);
    }

    #[test]
    fn secrets_are_scrubbed_from_the_fields_shown() {
        let attrs = map(&[
            ("password", s("s3cretpw")),
            ("host", s("s3cretpw.example.com")),
            ("uri", s("postgresql://u:s3cretpw@h/db")),
        ]);
        let shown = mask_attributes(&attrs, Some("sql"), None);
        assert_eq!(shown["host"], s("******.example.com"));
        assert_eq!(shown["uri"], s("postgresql://u:******@h/db"));
        // Common words and very short values are scrubbed as tokens, not inside other words.
        let secrets = vec!["true".to_string(), "ab".to_string()];
        assert_eq!(
            scrub("true ab truehost table", &secrets),
            "****** ****** truehost table"
        );
        // Percent-encoded passwords are scrubbed in both forms.
        let secrets = collect_secrets(&s("postgresql://u:p%40ss%21@h/db"), "uri");
        assert_eq!(scrub("p@ss! p%40ss%21", &secrets), "****** ******");
    }

    #[test]
    fn secret_settings_are_masked_in_any_text() {
        assert_eq!(
            mask_text("connect password=abc token: xyz ok"),
            "connect password=****** token: ****** ok"
        );
        assert_eq!(
            mask_text(r#"{"api_key": "k", "host": "h"}"#),
            r#"{"api_key": ******, "host": "h"}"#
        );
        assert_eq!(
            mask_text("sent Bearer abcdef123456 and Basic dXNlcjpwYXNz"),
            "sent Bearer ****** and Basic ******"
        );
        // `Authorization` is a secret's name itself (it holds `auth`).
        assert_eq!(
            mask_text("Authorization: Bearer abcdef123456"),
            "Authorization: ****** ******"
        );
        // A name holding a secret word anywhere is a secret's name, dotted names included.
        assert_eq!(mask_text("donkey=1"), "donkey=******");
        assert_eq!(mask_text("x.apikey=v"), "x.apikey=******");
        // An empty password leaves an `https` user that is a token.
        assert_eq!(uri_secrets("https://tok3n:@h/x"), vec!["tok3n".to_string()]);
    }

    #[test]
    fn groups_under_a_secret_name_are_masked_whole() {
        let attrs = map(&[(
            "credentials",
            Value::Map(map(&[("user", s("u")), ("secret", s("zzz999"))])),
        )]);
        let shown = mask_attributes(&attrs, Some("plugin"), None);
        assert_eq!(shown["credentials"], s(MASK));
        // A group under a public-looking name is judged setting by setting.
        let attrs = map(&[(
            "options",
            Value::Map(map(&[("sslmode", s("require")), ("sslkey", s("/k"))])),
        )]);
        let shown = mask_attributes(&attrs, Some("sql"), None);
        assert_eq!(
            shown["options"],
            Value::Map(map(&[("sslmode", s("require")), ("sslkey", s(MASK))]))
        );
    }

    #[test]
    fn the_connectors_config_masks_every_label_with_its_inherited_secrets() {
        let config = Value::Map(map(&[
            (
                "sql",
                Value::Map(map(&[
                    (
                        "default",
                        Value::Map(map(&[("password", s("inherited-pw"))])),
                    ),
                    (
                        "main",
                        Value::Map(map(&[
                            ("host", s("inherited-pw.local")),
                            ("username", s("mrsm")),
                        ])),
                    ),
                ])),
            ),
            ("weird", s("a value where labels belong")),
        ]));
        let shown = mask_connectors_config(&config, None);
        let sql = shown.get("sql").unwrap();
        assert_eq!(sql.get_path(&["default", "password"]), Some(&s(MASK)));
        assert_eq!(sql.get_path(&["main", "host"]), Some(&s("******.local")));
        assert_eq!(sql.get_path(&["main", "username"]), Some(&s("mrsm")));
        assert_eq!(shown.get("weird"), Some(&s(MASK)));
    }

    #[test]
    fn unquote_decodes_percent_escapes() {
        assert_eq!(unquote("p%40ss%21"), "p@ss!");
        assert_eq!(unquote("100%"), "100%");
        assert_eq!(unquote("%zz%4"), "%zz%4");
        assert_eq!(unquote("%E2%80%A2"), "•");
    }

    #[test]
    fn encoded_credentials_cannot_reappear_in_public_fields_or_errors() {
        let secret = "review secret+/=encoded";
        let forms = [
            secret,
            "review%20secret%2B%2F%3Dencoded",
            "review+secret%2B%2F%3Dencoded",
            "review%2520secret%252B%252F%253Dencoded",
            "review%20secret%2b%2f%3dencoded",
            "%72%65%76%69%65%77%20%73%65%63%72%65%74%2b%2f%3d%65%6e%63%6f%64%65%64",
        ];
        for form in forms {
            let attrs = map(&[
                ("password", s(secret)),
                ("database", s(form)),
                ("host", s("db.invalid")),
            ]);
            let shown = mask_attributes(&attrs, Some("sql"), None);
            assert_eq!(shown["database"], s(MASK), "{form}");
            assert_eq!(shown["host"], s("db.invalid"));
            let hidden = hidden_values(&Value::Map(attrs), "", Some("sql"));
            let error = scrub(&format!("Driver echoed {form}"), &hidden);
            assert!(!error.contains(secret) && !error.contains(form), "{error}");
        }
        assert_eq!(
            scrub(
                "Driver echoed p%C3%A1ssword%2F%CE%BA",
                &["pássword/κ".into()]
            ),
            MASK
        );
        assert_eq!(
            scrub("A harmless host db.invalid", &[secret.into()]),
            "A harmless host db.invalid"
        );
        // Decoding is bounded; a still-encoded value beyond the budget is concealed whole.
        let deep = "%2525252541";
        assert!(!decoded_views(deep).contains("A"));
        assert!(decoded_views(deep).len() <= 9);
        assert_eq!(scrub(deep, &["A".into()]), MASK);
    }

    #[test]
    fn encoded_query_keys_repr_and_numeric_copies_are_scrubbed() {
        let attrs = map(&[
            (
                "uri",
                s(
                    "postgresql://reader:password-value@db.invalid/readings?%70aSsWoRd=query-private",
                ),
            ),
            ("database", s("query-private")),
        ]);
        assert_eq!(
            mask_attributes(&attrs, Some("sql"), None)["database"],
            s(MASK)
        );
        for name in ["password", "PASSWORD", "pass%77ord", "%70aSsWoRd"] {
            assert!(
                !mask_connection_string(
                    &format!("https://db.invalid/?{name}=query-private"),
                    Some("api")
                )
                .contains("query-private")
            );
        }
        let secret = "part\"one\\two\nlast";
        for quoted in [pyfmt::str_repr(secret), pyfmt::json_dumps_str(secret, true)] {
            let shown = scrub(&format!("Driver echoed {quoted}"), &[secret.into()]);
            assert!(
                !shown.contains("part") && !shown.contains("last"),
                "{shown}"
            );
        }
        let attrs = map(&[("api_key", Value::Int(4321)), ("port", Value::Int(4321))]);
        assert_eq!(mask_attributes(&attrs, Some("sql"), None)["port"], s(MASK));
        let attrs = map(&[("password", s("xy")), ("host", s("xy"))]);
        assert_eq!(mask_attributes(&attrs, Some("sql"), None)["host"], s(MASK));
        // Inherited opaque settings also remain private if repeated as a public value.
        let config = Value::Map(map(&[
            (
                "default",
                Value::Map(map(&[("opaque", s("private-opaque-value"))])),
            ),
            (
                "review",
                Value::Map(map(&[("host", s("private-opaque-value"))])),
            ),
        ]));
        assert_eq!(
            mask_connectors_config(&config, Some("sql")).get_path(&["review", "host"]),
            Some(&s(MASK))
        );
    }
}
