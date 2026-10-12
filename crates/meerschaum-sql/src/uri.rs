//! Connection specs: parse SQLAlchemy URIs and `connectors.yaml` attributes into driver options.
//!
//! Ported from `meerschaum/connectors/sql/_SQLConnector.py` (`__init__`),
//! `meerschaum/connectors/sql/_uri.py` (`parse_uri`, `from_uri`) and
//! `meerschaum/connectors/sql/_create_engine.py` (`create_engine`'s engine string), plus the parts
//! of SQLAlchemy 2.0 (`sqlalchemy/engine/url.py::_parse_url`, `URL.translate_connect_args`) and
//! `urllib.parse` (`parse_qs`, `unquote`, `quote_plus`, `urlencode`) they rely on.
//!
//! The config file format does not change (§6.2): the same URI or attribute set yields the same
//! host, port, credentials, database, schema and label as v4, and [`ConnectionSpec::engine_str`]
//! reproduces the SQLAlchemy engine string byte for byte (golden fixtures in
//! `tests/fixtures/uris.json`).

use std::sync::LazyLock;

use indexmap::IndexMap;
use regex::Regex;

use crate::error::{Result, SqlError};
use crate::flavor::Flavor;

/// SQLAlchemy `QueuePool` size from `system.connectors.sql.create_engine.pool_size`
/// (`meerschaum/config/_default.py`), which overrides the static `os.cpu_count()` default.
pub const DEFAULT_POOL_SIZE: u32 = 8;

/// SQLAlchemy `max_overflow` from `system.connectors.sql.create_engine.max_overflow`.
pub const DEFAULT_MAX_OVERFLOW: u32 = 12;

/// The `options` attribute of a SQL connector: a query string or a mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionOptions {
    /// A query string, e.g. `"charset=utf8mb4&ssl=true"` (parsed with `parse_qsl`).
    Str(String),
    /// An ordered mapping, as written in YAML.
    Pairs(Vec<(String, String)>),
}

impl ConnectionOptions {
    /// The options as ordered pairs: `dict(urllib.parse.parse_qsl(options))` for a string
    /// (`_create_engine.py:87`): blank values dropped, later duplicates overwrite in place.
    pub fn pairs(&self) -> Vec<(String, String)> {
        match self {
            ConnectionOptions::Str(s) => {
                let mut out: IndexMap<String, String> = IndexMap::new();
                for (k, v) in parse_qsl(s) {
                    out.insert(k, v);
                }
                out.into_iter().collect()
            }
            ConnectionOptions::Pairs(p) => {
                let mut out: IndexMap<String, String> = IndexMap::new();
                for (k, v) in p {
                    out.insert(k.clone(), v.clone());
                }
                out.into_iter().collect()
            }
        }
    }
}

/// The attributes of a `sql:` connector as found in `connectors.yaml`, keyword arguments or an
/// `MRSM_SQL_<LABEL>` URI. Every field is optional; [`ConnectionSpec::from_attributes`] applies
/// the flavor defaults and requirements.
// TODO(core-merge): built by `meerschaum-core`'s connector registry from the merged config.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorAttributes {
    /// The connector label (`sql:<label>`).
    pub label: Option<String>,
    /// The explicit flavor (overrides the URI scheme).
    pub flavor: Option<Flavor>,
    /// A SQLAlchemy URI.
    pub uri: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub options: Option<ConnectionOptions>,
    /// Any other string attributes (e.g. `driver`, `sslmode`), in insertion order.
    pub extra: Vec<(String, String)>,
    /// `create_engine.pool_size`.
    pub pool_size: Option<u32>,
    /// `create_engine.max_overflow`.
    pub max_overflow: Option<u32>,
}

/// A fully resolved connection: what `SQLConnector.__dict__` holds after `__init__` and
/// `create_engine()` (flavor defaults applied).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionSpec {
    pub flavor: Flavor,
    pub label: Option<String>,
    /// The (rewritten) URI, if the connector was built from one.
    pub uri: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub options: Option<ConnectionOptions>,
    /// Remaining attributes: URI query parameters (`driver`, `sslmode`, `charset`, …) and any
    /// extra configured keys, in insertion order.
    pub params: IndexMap<String, String>,
    /// SQLAlchemy `pool_size`.
    pub pool_size: u32,
    /// SQLAlchemy `max_overflow`.
    pub max_overflow: u32,
}

/// A value in the dictionaries returned by [`parse_uri`] / [`from_uri_params`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UriValue {
    Str(String),
    Int(i64),
}

impl UriValue {
    /// The value as text (`str(value)`).
    pub fn as_string(&self) -> String {
        match self {
            UriValue::Str(s) => s.clone(),
            UriValue::Int(i) => i.to_string(),
        }
    }
}

/// SQLAlchemy 2.0's URL pattern (`sqlalchemy/engine/url.py:868`), unchanged.
static SQLALCHEMY_URL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
        ^(?P<name>[\w\+]+)://
        (?:
            (?P<username>[^:/]*)
            (?::(?P<password>[^@]*))?
        @)?
        (?:
            (?:
                \[(?P<ipv6host>[^/\?]+)\] |
                (?P<ipv4host>[^/:\?]+)
            )?
            (?::(?P<port>[^/\?]*))?
        )?
        (?:/(?P<database>[^\?]*))?
        (?:\?(?P<query>.*))?
        ",
    )
    .expect("valid SQLAlchemy URL regex")
});

/// The components of a SQLAlchemy URL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SqlalchemyUrl {
    pub drivername: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub host: Option<String>,
    pub port: Option<i64>,
    pub database: Option<String>,
}

/// `sqlalchemy.engine.url.make_url` for a string (`url.py:867`).
pub fn make_url(uri: &str) -> Result<SqlalchemyUrl> {
    let caps = SQLALCHEMY_URL_RE.captures(uri).ok_or_else(|| {
        SqlError::InvalidConnection(
            "Could not parse SQLAlchemy URL from given URL string".to_string(),
        )
    })?;
    let get = |name: &str| caps.name(name).map(|m| m.as_str().to_string());
    let host = get("ipv4host")
        .filter(|h| !h.is_empty())
        .or_else(|| get("ipv6host"));
    let port = match get("port") {
        // PARITY: `if components["port"]: int(...)` (`url.py:916`); an empty port is passed to
        // `URL.create`, which rejects a non-integer port.
        Some(p) if !p.is_empty() => Some(p.parse::<i64>().map_err(|_| {
            SqlError::InvalidConnection(format!("invalid literal for int() with base 10: '{p}'"))
        })?),
        Some(_) => {
            return Err(SqlError::InvalidConnection(
                "Port argument must be an integer or None".to_string(),
            ));
        }
        None => None,
    };
    Ok(SqlalchemyUrl {
        drivername: get("name").unwrap_or_default(),
        username: get("username").map(|u| unquote(&u)),
        password: get("password").map(|p| unquote(&p)),
        host,
        port,
        database: get("database"),
    })
}

impl SqlalchemyUrl {
    /// `URL.translate_connect_args()`: `host`, `database`, `username`, `password`, `port`,
    /// omitting unset or falsy values (`url.py:797`).
    pub fn translate_connect_args(&self) -> IndexMap<String, UriValue> {
        let mut out = IndexMap::new();
        let mut put = |k: &str, v: &Option<String>| {
            if let Some(v) = v.as_ref().filter(|v| !v.is_empty()) {
                out.insert(k.to_string(), UriValue::Str(v.clone()));
            }
        };
        put("host", &self.host);
        put("database", &self.database);
        put("username", &self.username);
        put("password", &self.password);
        if let Some(port) = self.port.filter(|p| *p != 0) {
            out.insert("port".to_string(), UriValue::Int(port));
        }
        out
    }
}

/// `SQLConnector.parse_uri(uri)` (`_uri.py:72`).
pub fn parse_uri(uri: &str) -> Result<IndexMap<String, UriValue>> {
    let mut params = make_url(uri)?.translate_connect_args();
    let scheme = uri.split(':').next().unwrap_or_default();
    let mut flavor = scheme.split('+').next().unwrap_or_default().to_string();
    if flavor == "postgres" {
        flavor = "postgresql".to_string();
    }
    params.insert("flavor".to_string(), UriValue::Str(flavor));
    if uri.contains('?') {
        // PARITY: `urlparse(uri).query` drops the `#fragment` before splitting the query
        // (`_uri.py:106`), while SQLAlchemy's regex keeps `#` in the database name.
        let without_fragment = uri.split('#').next().unwrap_or_default();
        if let Some((_, query)) = without_fragment.split_once('?') {
            for (key, values) in parse_qs(query) {
                if let Some(first) = values.into_iter().next() {
                    params.insert(key, UriValue::Str(first));
                }
            }
        }
        let options = params
            .get("options")
            .map(UriValue::as_string)
            .unwrap_or_default();
        if options.contains("--search_path") {
            params.insert(
                "schema".to_string(),
                UriValue::Str(options.replacen("--search_path=", "", 1)),
            );
        }
    }
    Ok(params)
}

/// `SQLConnector.from_uri(uri, label=label, as_dict=True)` (`_uri.py:15`).
pub fn from_uri_params(uri: &str, label: Option<&str>) -> Result<IndexMap<String, UriValue>> {
    let mut params = parse_uri(uri)?;
    params.insert("uri".to_string(), UriValue::Str(uri.to_string()));
    let flavor = params
        .get("flavor")
        .map(UriValue::as_string)
        .unwrap_or_default();
    if flavor.is_empty() || flavor.parse::<Flavor>().is_err() || flavor == "postgres" {
        return Err(SqlError::InvalidConnection(format!(
            "Invalid flavor '{flavor}' detected from the provided URI."
        )));
    }
    let Some(database) = params.get("database").map(UriValue::as_string) else {
        return Err(SqlError::InvalidConnection(
            "Unable to determine the database from the provided URI.".to_string(),
        ));
    };
    let derived_label = if matches!(flavor.as_str(), "sqlite" | "duckdb" | "geopackage") {
        if database == ":memory:" {
            format!("memory_{flavor}")
        } else {
            // PARITY: `params['database'].split(os.path.sep)[-1].lower()` (`_uri.py:58`).
            database
                .rsplit(std::path::MAIN_SEPARATOR)
                .next()
                .unwrap_or_default()
                .to_lowercase()
        }
    } else {
        let username = params.get("username").map(UriValue::as_string);
        let host = params.get("host").map(UriValue::as_string);
        format!(
            "{}{}{}{}",
            username.map(|u| format!("{u}@")).unwrap_or_default(),
            host.clone().unwrap_or_default(),
            if host.is_some() { "/" } else { "" },
            database,
        )
        .to_lowercase()
    };
    let label = label
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .unwrap_or(derived_label);
    params.insert("label".to_string(), UriValue::Str(label));
    Ok(params)
}

/// The URI rewriting at the top of `SQLConnector.__init__` (`_SQLConnector.py:203-219`).
/// Returns the rewritten URI and the flavor it implies, if any.
pub fn rewrite_uri(uri: &str) -> (String, Option<Flavor>) {
    let mut uri = uri.to_string();
    let mut flavor = None;
    if uri.starts_with("postgres") && !uri.starts_with("postgresql") {
        uri = uri.replacen("postgres", "postgresql", 1);
    }
    if uri.starts_with("postgresql") && !uri.starts_with("postgresql+") {
        uri = uri.replacen("postgresql://", "postgresql+psycopg://", 1);
    }
    if uri.starts_with("timescaledb://") {
        uri = uri.replacen("timescaledb://", "postgresql+psycopg://", 1);
        flavor = Some(Flavor::Timescaledb);
    }
    if uri.starts_with("timescaledb-ha://") {
        uri = uri.replacen("timescaledb-ha://", "postgresql+psycopg://", 1);
        flavor = Some(Flavor::TimescaledbHa);
    }
    if uri.starts_with("postgis://") {
        uri = uri.replacen("postgis://", "postgresql+psycopg://", 1);
        flavor = Some(Flavor::Postgis);
    }
    (uri, flavor)
}

impl ConnectionSpec {
    /// Build a spec from a URI (`SQLConnector(uri=uri, label=label, flavor=flavor)`).
    pub fn from_uri(uri: &str, label: Option<&str>, flavor: Option<Flavor>) -> Result<Self> {
        Self::from_attributes(&ConnectorAttributes {
            label: label.map(str::to_string),
            flavor,
            uri: Some(uri.to_string()),
            ..Default::default()
        })
    }

    /// Build a spec the way `SQLConnector.__init__` followed by `create_engine()` resolves its
    /// attributes: URI rewriting and parsing, explicit flavor override, requirement checks and
    /// flavor defaults.
    pub fn from_attributes(attrs: &ConnectorAttributes) -> Result<Self> {
        let mut flavor = attrs.flavor;
        let mut label = attrs.label.clone();
        let mut uri = attrs.uri.clone();
        let mut host = attrs.host.clone();
        let mut port = attrs.port;
        let mut username = attrs.username.clone();
        let mut password = attrs.password.clone();
        let mut database = attrs.database.clone();
        let mut schema = attrs.schema.clone();
        let mut options = attrs.options.clone();
        let mut params: IndexMap<String, String> = IndexMap::new();
        for (k, v) in &attrs.extra {
            params.insert(k.clone(), v.clone());
        }

        if let Some(raw_uri) = attrs.uri.as_deref() {
            let (rewritten, uri_flavor) = rewrite_uri(raw_uri);
            if uri_flavor.is_some() {
                // PARITY: the scheme rewrite sets the local `flavor`, overriding an explicit one
                // (`_SQLConnector.py:211-219`).
                flavor = uri_flavor;
            }
            let parsed = from_uri_params(&rewritten, None)?;
            if label.as_deref().is_none_or(str::is_empty) {
                label = parsed.get("label").map(UriValue::as_string);
            }
            let mut parsed_flavor = None;
            for (key, value) in parsed {
                let text = value.as_string();
                match key.as_str() {
                    "label" => {}
                    "uri" => uri = Some(text),
                    "host" => host = Some(text),
                    "port" => {
                        port = Some(text.parse::<u16>().map_err(|_| {
                            SqlError::InvalidConnection(format!("Invalid port '{text}'."))
                        })?)
                    }
                    "username" => username = Some(text),
                    "password" => password = Some(text),
                    "database" => database = Some(text),
                    "schema" => schema = Some(text),
                    "flavor" => parsed_flavor = Some(text),
                    "options" => options = Some(ConnectionOptions::Str(text)),
                    _ => {
                        params.insert(key, text);
                    }
                }
            }
            if flavor.is_none() {
                flavor = parsed_flavor.and_then(|f| f.parse().ok());
            }
        }

        let flavor = flavor.ok_or_else(|| {
            SqlError::InvalidConnection(format!(
                "Missing flavor. Provide flavor as a key for 'sql:{}'.",
                label.clone().unwrap_or_default()
            ))
        })?;

        // PARITY: requirements are only verified without a `uri` (`_SQLConnector.py:265`).
        if uri.is_none() {
            let mut missing = Vec::new();
            for req in flavor.requirements() {
                let present = match *req {
                    "username" => username.is_some(),
                    "password" => password.is_some(),
                    "host" => host.is_some(),
                    "database" => database.is_some(),
                    _ => true,
                };
                if !present {
                    missing.push(format!("'{req}'"));
                }
            }
            if !missing.is_empty() {
                return Err(SqlError::InvalidConnection(format!(
                    "Missing {} for connector 'sql:{}'.",
                    missing.join(", "),
                    label.clone().unwrap_or_default()
                )));
            }
        }

        // Flavor defaults fill missing attributes (`_create_engine.py:69-71`).
        for (key, value) in flavor.defaults() {
            match *key {
                "port" if port.is_none() => port = value.parse().ok(),
                "database" if database.is_none() => database = Some(value.to_string()),
                "username" if username.is_none() => username = Some(value.to_string()),
                "password" if password.is_none() => password = Some(value.to_string()),
                "options" if options.is_none() => {
                    options = Some(ConnectionOptions::Str(value.to_string()))
                }
                _ => {}
            }
        }

        Ok(ConnectionSpec {
            flavor,
            label,
            uri,
            host,
            port,
            username,
            password,
            database,
            schema,
            options,
            params,
            pool_size: attrs.pool_size.unwrap_or(DEFAULT_POOL_SIZE),
            max_overflow: attrs.max_overflow.unwrap_or(DEFAULT_MAX_OVERFLOW),
        })
    }

    /// Whether this is an in-memory SQLite / DuckDB database.
    pub fn is_memory(&self) -> bool {
        self.database.as_deref() == Some(":memory:")
            || self.uri.as_deref().is_some_and(|u| u.contains(":memory:"))
    }

    /// The pool size for the driver: SQLAlchemy's `pool_size + max_overflow`, and one connection
    /// for in-memory SQLite (each connection would otherwise open its own empty database).
    pub fn max_connections(&self) -> u32 {
        if self.is_memory() {
            return 1;
        }
        (self.pool_size + self.max_overflow).max(1)
    }

    /// The options as ordered pairs (`dict(parse_qsl(options))` for a string).
    pub fn option_pairs(&self) -> Vec<(String, String)> {
        self.options
            .as_ref()
            .map(ConnectionOptions::pairs)
            .unwrap_or_default()
    }

    /// The SQLAlchemy engine string `create_engine(include_uri=True)` builds
    /// (`_create_engine.py:93-120`).
    pub fn engine_str(&self) -> Result<String> {
        let engine = self.flavor.engine();
        if matches!(
            self.flavor,
            Flavor::Sqlite | Flavor::Duckdb | Flavor::Geopackage
        ) {
            if let Some(uri) = &self.uri {
                return Ok(uri.clone());
            }
            // TODO(core-merge): `{SQLITE_DB_PATH}` resolves to `paths.SQLITE_DB_PATH`, which
            // lives in core's path registry.
            // PARITY: a missing database renders as `None` (`f"{_engine}:///{_database}"`).
            let database = self.database.clone().unwrap_or_else(|| "None".to_string());
            return Ok(format!("{engine}:///{database}"));
        }

        let Some(uri) = &self.uri else {
            let host = self.host.as_deref().ok_or_else(|| {
                SqlError::InvalidConnection(
                    "can only concatenate str (not \"NoneType\") to str".to_string(),
                )
            })?;
            let mut s = format!("{engine}://");
            // PARITY: the username is not quoted, only the password (`_create_engine.py:101`).
            s.push_str(self.username.as_deref().unwrap_or(""));
            if let Some(password) = &self.password {
                s.push(':');
                s.push_str(&quote_plus(password));
            }
            s.push('@');
            s.push_str(host);
            if let Some(port) = self.port {
                s.push(':');
                s.push_str(&port.to_string());
            }
            if let Some(database) = &self.database {
                s.push('/');
                s.push_str(database);
            }
            let options = self.option_pairs();
            if !options.is_empty() {
                s.push('?');
                s.push_str(&urlencode(&options));
            }
            return Ok(s);
        };

        let mut engine_str = uri.clone();
        // PARITY: "Sometimes the timescaledb:// flavor can slip in." The flavor name is replaced
        // wherever it first occurs in the URI, including inside a database named after the
        // flavor (`_create_engine.py:113-117`).
        if uri.contains(self.flavor.as_str()) {
            if matches!(
                self.flavor,
                Flavor::Timescaledb | Flavor::TimescaledbHa | Flavor::Postgis
            ) {
                engine_str = engine_str.replacen(self.flavor.as_str(), "postgresql", 1);
            } else if uri.starts_with("postgresql://") {
                engine_str = engine_str.replace("postgresql://", "postgresql+psycopg2://");
            }
        }
        Ok(engine_str)
    }

    /// The engine string with the password masked, for logging.
    pub fn display_str(&self) -> String {
        let s = self.engine_str().unwrap_or_default();
        match &self.password {
            Some(pw) if !pw.is_empty() => {
                let quoted = quote_plus(pw);
                s.replace(
                    &format!(":{quoted}@"),
                    &format!(":{}@", "*".repeat(pw.len())),
                )
                .replace(&format!(":{pw}@"), &format!(":{}@", "*".repeat(pw.len())))
            }
            _ => s,
        }
    }

    /// The flattened attributes, comparable to `SQLConnector.__dict__` (public keys only).
    pub fn attributes(&self) -> IndexMap<String, String> {
        let mut out = IndexMap::new();
        let mut put = |k: &str, v: Option<String>| {
            if let Some(v) = v {
                out.insert(k.to_string(), v);
            }
        };
        put("label", self.label.clone());
        put("uri", self.uri.clone());
        put("flavor", Some(self.flavor.as_str().to_string()));
        put("host", self.host.clone());
        put("port", self.port.map(|p| p.to_string()));
        put("username", self.username.clone());
        put("password", self.password.clone());
        put("database", self.database.clone());
        put("schema", self.schema.clone());
        put(
            "options",
            self.options.as_ref().map(|o| match o {
                ConnectionOptions::Str(s) => s.clone(),
                ConnectionOptions::Pairs(p) => urlencode(p),
            }),
        );
        for (k, v) in &self.params {
            out.insert(k.clone(), v.clone());
        }
        out
    }
}

// ----------------------------------------------------------------------------------------------
// `urllib.parse` equivalents
// ----------------------------------------------------------------------------------------------

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// `urllib.parse.unquote(s)`: decode `%XX` escapes (UTF-8, invalid sequences replaced);
/// malformed escapes are kept verbatim and `+` is not translated.
pub fn unquote(s: &str) -> String {
    if !s.contains('%') {
        return s.to_string();
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = bytes.get(i + 1).copied().and_then(hex_val);
            let lo = bytes.get(i + 2).copied().and_then(hex_val);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `urllib.parse.quote_plus(s)` with the default `safe=''`.
pub fn quote_plus(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `urllib.parse.urlencode(pairs)` (`quote_via=quote_plus`, no `doseq`).
pub fn urlencode(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", quote_plus(k), quote_plus(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// `urllib.parse.parse_qsl(qs)` with `keep_blank_values=False`.
pub fn parse_qsl(qs: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for name_value in qs.split('&') {
        if name_value.is_empty() {
            continue;
        }
        let Some((name, value)) = name_value.split_once('=') else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        out.push((
            unquote(&name.replace('+', " ")),
            unquote(&value.replace('+', " ")),
        ));
    }
    out
}

/// `urllib.parse.parse_qs(qs)`: values grouped by key in first-occurrence order.
pub fn parse_qs(qs: &str) -> IndexMap<String, Vec<String>> {
    let mut out: IndexMap<String, Vec<String>> = IndexMap::new();
    for (k, v) in parse_qsl(qs) {
        out.entry(k).or_default().push(v);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_helpers() {
        assert_eq!(quote_plus("p@ss w/rd!"), "p%40ss+w%2Frd%21");
        assert_eq!(unquote("p%40ss%3Aw%2Frd"), "p@ss:w/rd");
        assert_eq!(unquote("100%"), "100%");
        assert_eq!(unquote("a%zz"), "a%zz");
        assert_eq!(
            parse_qsl("a=1&b=&c&d=x+y%21"),
            vec![
                ("a".to_string(), "1".to_string()),
                ("d".to_string(), "x y!".to_string())
            ]
        );
    }

    #[test]
    fn memory_pool_is_single() {
        let spec = ConnectionSpec::from_uri("sqlite:///:memory:", None, None).unwrap();
        assert_eq!(spec.max_connections(), 1);
        let spec = ConnectionSpec::from_uri("sqlite:////tmp/x.db", None, None).unwrap();
        assert_eq!(
            spec.max_connections(),
            DEFAULT_POOL_SIZE + DEFAULT_MAX_OVERFLOW
        );
    }
}
