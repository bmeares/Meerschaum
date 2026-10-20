//! The `APIConnector` (`meerschaum/connectors/api/`): an HTTP client for a Meerschaum API
//! instance, speaking every route of §7.1 with the same paths, query parameters and bodies as the
//! v4 Python client.
//!
//! Requests are encoded the way `requests` encodes them (`params=` dicts with `None` dropped and
//! `str()` values, `json=` bodies with `json.dumps` default separators, `data=` strings without a
//! `Content-Type`), so a v4 server and the Rust server see identical requests.
//!
//! The client also keeps the per-pipe caches the Python `Pipe` object keeps between calls
//! (attributes for 600 s, `exists` for 60 s, columns types for 60 s), because the v4 client reads
//! `pipe.columns` / `pipe.dtypes` inside `sync_pipe` and `get_pipe_data` and those reads issue the
//! `/attributes`, `/exists` and `/columns/types` requests on a cache miss.

mod actions;
mod engine;
mod instance;
mod jobs;
mod login;
mod misc;
mod pipes;
mod plugins;
mod request;
mod tokens;
pub mod uri;
mod users;
pub mod warnings;
mod wss;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::NaiveDateTime;
use indexmap::IndexMap;
use meerschaum_core::{Map, Value};

use crate::traits::{ConnectorError, ConnectorResult};

pub use jobs::LogsMonitor;
pub use login::parse_expires;
pub use request::{ApiResponse, Body, Method, RequestOptions};

/// `pipes:attributes:local_cache_timeout_seconds`.
pub const ATTRIBUTES_CACHE_SECONDS: f64 = 600.0;
/// `pipes:sync:exists_cache_seconds`.
pub const EXISTS_CACHE_SECONDS: f64 = 60.0;
/// `pipes:dtypes:columns_types_cache_seconds`.
pub const COLUMNS_TYPES_CACHE_SECONDS: f64 = 60.0;
/// `pipes:static:static_schema_cache_seconds`.
pub const STATIC_SCHEMA_CACHE_SECONDS: f64 = 3600.0;
/// `system:connectors:sql:chunksize` (the default for `chunksize=-1`).
pub const DEFAULT_SYNC_CHUNKSIZE: i64 = 100_000;

#[derive(Debug, Default)]
pub(crate) struct AuthState {
    pub(crate) token: Option<String>,
    pub(crate) expires: Option<NaiveDateTime>,
    pub(crate) emitted_warning: bool,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct PipeCacheEntry {
    /// The instance's parameters and when they were fetched.
    pub(crate) attributes: Option<(Instant, Map)>,
    /// When `exists()` last returned `true`.
    pub(crate) exists: Option<Instant>,
    /// The columns types and when they were fetched (only non-empty results are cached).
    pub(crate) columns_types: Option<(Instant, IndexMap<String, String>)>,
}

/// A connection to a Meerschaum API instance.
#[derive(Clone)]
pub struct APIConnector {
    pub(crate) label: String,
    pub(crate) attributes: Map,
    /// `protocol://host[:port]`.
    pub url: String,
    pub(crate) http: reqwest::Client,
    /// `false` when the `verify` attribute is `false` (HTTP and websocket TLS accept any certificate).
    pub(crate) verify: bool,
    pub(crate) auth: Arc<tokio::sync::Mutex<AuthState>>,
    pub(crate) pipe_cache: Arc<Mutex<HashMap<String, PipeCacheEntry>>>,
    pub(crate) job_metadata_cache: Arc<Mutex<HashMap<String, (Instant, Map)>>>,
    pub(crate) timeout: Duration,
    /// `system:connectors:sql:chunksize` for `chunksize=-1`.
    pub sync_chunksize: i64,
}

impl std::fmt::Debug for APIConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("APIConnector")
            .field("label", &self.label)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for APIConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "api:{}", self.label)
    }
}

fn attr_str(v: &Value) -> String {
    meerschaum_core::pyfmt::py_str(v)
}

impl APIConnector {
    /// `OPTIONAL_ATTRIBUTES`.
    pub const OPTIONAL_ATTRIBUTES: &'static [&'static str] =
        &["port", "client_secret", "client_id", "api_key"];

    /// `APIConnector(label, **attributes)`: `uri`, or `host` (required) with `port`,
    /// `protocol`, `username` / `password`, `client_id` / `client_secret`, `api_key`,
    /// `instance_keys` and `verify`.
    pub fn new(label: Option<&str>, attributes: Map) -> ConnectorResult<APIConnector> {
        let mut attributes = attributes;
        let mut label = label.map(String::from);
        if let Some(uri) = attributes.get("uri").map(attr_str) {
            let parsed = uri::from_uri_params(&uri, label.as_deref())?;
            if label.is_none() {
                label = parsed.get("label").map(attr_str);
            }
            for (k, v) in parsed {
                if k != "label" {
                    attributes.insert(k, v);
                }
            }
        }
        let label = label.unwrap_or_else(|| "main".into());
        if !attributes.contains_key("protocol") {
            let uri = attributes.get("uri").map(attr_str).unwrap_or_default();
            let protocol = if uri.starts_with("https") {
                "https"
            } else {
                "http"
            };
            attributes.insert("protocol".into(), Value::Str(protocol.into()));
        }
        if let Some(uri) = attributes.get("uri").map(attr_str) {
            let conn_attrs = uri::parse_uri(&uri)?;
            if !conn_attrs.contains_key("host") {
                return Err(ConnectorError::Invalid(format!(
                    "Invalid URI for 'api:{label}'."
                )));
            }
            for (k, v) in conn_attrs {
                attributes.insert(k, v);
            }
        } else if !attributes.contains_key("host") {
            return Err(ConnectorError::Invalid(format!(
                "Missing required attribute 'host' for connector 'api:{label}'."
            )));
        }
        let protocol = attributes.get("protocol").map(attr_str).unwrap_or_default();
        let host = attributes.get("host").map(attr_str).unwrap_or_default();
        let port = attributes
            .get("port")
            .filter(|p| p.truthy())
            .map(|p| format!(":{}", attr_str(p)))
            .unwrap_or_default();
        let url = format!("{protocol}://{host}{port}");

        let verify = !matches!(attributes.get("verify"), Some(Value::Bool(false)));
        // A new connection fails after `connect_timeout` (the `connect_timeout` attribute,
        // `MRSM_CONNECT_TIMEOUT`, else 5 s) instead of waiting for the operating system; a refused
        // connection already fails at once (see `ApiConnector::connect_failure`).
        let connect_timeout = meerschaum_core::connect::connect_timeout(
            attributes.get("connect_timeout").map(attr_str).as_deref(),
        );
        let mut builder = reqwest::Client::builder().connect_timeout(connect_timeout);
        if !verify {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let http = builder
            .build()
            .map_err(|e| ConnectorError::Backend(e.to_string()))?;
        Ok(APIConnector {
            label,
            attributes,
            url,
            http,
            verify,
            auth: Arc::new(tokio::sync::Mutex::new(AuthState::default())),
            pipe_cache: Arc::new(Mutex::new(HashMap::new())),
            job_metadata_cache: Arc::new(Mutex::new(HashMap::new())),
            timeout: Duration::from_secs(crate::routes::DEFAULT_TIMEOUT_SECONDS),
            sync_chunksize: DEFAULT_SYNC_CHUNKSIZE,
        })
    }

    /// `APIConnector.from_uri(uri, label)`.
    pub fn from_uri(uri: &str, label: Option<&str>) -> ConnectorResult<APIConnector> {
        let mut attributes = Map::new();
        attributes.insert("uri".into(), Value::Str(uri.to_string()));
        APIConnector::new(label, attributes)
    }

    /// The attributes for `api:{label}` from the config registry (the `default` section patched
    /// with the label's section, as `Connector.__init__` does).
    pub fn from_config(
        config: &meerschaum_core::Config,
        label: &str,
    ) -> ConnectorResult<APIConnector> {
        let base = config
            .get(&["meerschaum", "connectors", "api", "default"])
            .map(|v| v.to_map())
            .unwrap_or_default();
        let specific = config
            .get(&["meerschaum", "connectors", "api", label])
            .map(|v| v.to_map())
            .unwrap_or_default();
        APIConnector::new(
            Some(label),
            meerschaum_core::value::apply_patch_maps(&base, &specific),
        )
    }

    /// The label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The attributes.
    pub fn attributes(&self) -> &Map {
        &self.attributes
    }

    pub(crate) fn attr(&self, key: &str) -> Option<String> {
        self.attributes.get(key).map(attr_str)
    }

    /// The `URI` property: `protocol://[username:password@]host[:port]`.
    pub fn uri(&self) -> String {
        let creds = match (self.attr("username"), self.attr("password")) {
            (Some(u), Some(p)) if !u.is_empty() && !p.is_empty() => format!("{u}:{p}@"),
            _ => String::new(),
        };
        let port = self
            .attributes
            .get("port")
            .filter(|p| p.truthy())
            .map(|p| format!(":{}", attr_str(p)))
            .unwrap_or_default();
        format!(
            "{}://{creds}{}{port}",
            self.attr("protocol").unwrap_or_default(),
            self.attr("host").unwrap_or_default()
        )
    }

    /// `instance_keys`: sent with pipe requests (`None` when unset).
    pub fn instance_keys(&self) -> Option<String> {
        self.attr("instance_keys")
    }

    /// `login_scheme`: `password` (with a username), `client_credentials` (with a client ID),
    /// `api_key`, else `password`.
    pub fn login_scheme(&self) -> &'static str {
        if self.attributes.contains_key("username") {
            "password"
        } else if self.attributes.contains_key("client_id") {
            "client_credentials"
        } else if self.attributes.contains_key("api_key") {
            "api_key"
        } else {
            "password"
        }
    }

    /// The connection timeout of this connector's HTTP client.
    pub fn connect_timeout(&self) -> Duration {
        meerschaum_core::connect::connect_timeout(
            self.attributes
                .get("connect_timeout")
                .map(attr_str)
                .as_deref(),
        )
    }

    /// Describe a failed request when it failed while connecting: `api:main: cannot connect to
    /// localhost:8000 (connection refused). Is the API server running?`. `None` for any other
    /// failure (the server answered, or the request itself was malformed).
    pub(crate) fn connect_failure(&self, error: &reqwest::Error) -> Option<String> {
        use meerschaum_core::connect::{
            ConnectErrorKind, ConnectTarget, classify_text, connect_error,
        };
        if !error.is_connect() {
            return None;
        }
        let mut text = error.to_string();
        let mut source = std::error::Error::source(error);
        while let Some(s) = source {
            text.push_str(&format!(": {s}"));
            source = s.source();
        }
        let kind = if error.is_timeout() {
            ConnectErrorKind::Timeout
        } else {
            classify_text(&text).unwrap_or(ConnectErrorKind::Other)
        };
        let host = self.attr("host").unwrap_or_default();
        let address = match self.attributes.get("port").filter(|p| p.truthy()) {
            Some(port) => format!("{host}:{}", attr_str(port)),
            None => host,
        };
        let target = ConnectTarget {
            connector: format!("api:{}", self.label),
            address,
            user: self
                .attr("username")
                .or_else(|| self.attr("client_id"))
                .unwrap_or_default(),
            noun: "API server",
        };
        Some(connect_error(&target, kind, &text, self.connect_timeout()).to_string())
    }

    /// Set the request timeout (default `STATIC_CONFIG['api']['default_timeout']`, 600 s).
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    /// `Pipe._invalidate_cache()` for the client-side pipe caches.
    pub fn invalidate_pipe_cache(&self, pipe: &meerschaum_core::Pipe) {
        if let Ok(mut cache) = self.pipe_cache.lock() {
            cache.remove(&pipe.pipe_ref());
        }
    }
}
