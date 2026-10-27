//! Server configuration: the `api:*` config keys, the `start api` flags, and the v5 renames.
//!
//! `api:uvicorn:*` keys are honored with a deprecation notice and mapped to `api:server:*`
//! (§11.2); `api:permissions:rate_limits` is new in v5 (§7.1).

use std::path::PathBuf;
use std::time::Duration;

use indexmap::IndexMap;
use meerschaum_core::config::paths::PathValue;
use meerschaum_core::{Config, Map, Value};

/// The `start api` flags (`meerschaum/actions/api.py`).
#[derive(Debug, Clone, Default)]
pub struct ServerFlags {
    /// `-i` / `--instance`: the instance keys (default `meerschaum:api_instance`).
    pub instance: Option<String>,
    /// `--host`.
    pub host: Option<String>,
    /// `-p` / `--port`.
    pub port: Option<u16>,
    /// `--production`.
    pub production: bool,
    /// `--secure`: only admins may execute actions (`api:permissions:actions:non_admin = false`).
    pub secure: bool,
    /// `--private`: require authentication on every endpoint.
    pub private: bool,
    /// `--no-auth` / `--insecure`.
    pub no_auth: bool,
    /// `--debug`.
    pub debug: bool,
    /// `--no-dash`.
    pub no_dash: bool,
    /// `--no-webterm`.
    pub no_webterm: bool,
    /// `--public`: the public preset (`api:permissions:preset: public`).
    pub public: bool,
}

/// A rate limit for one route group (`api:permissions:rate_limits:<group>`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateLimit {
    /// The number of requests allowed per window (the bucket size).
    pub requests: u32,
    /// The window.
    pub per: Duration,
}

/// The name of the public preset (`api:permissions:preset: public`, `start api --public`).
pub const PUBLIC_PRESET: &str = "public";

/// The core route groups the public preset keeps: `/version` and, of the `misc` group, only
/// `/healthcheck` and `/favicon.ico` (see [`ServerConfig::serves_full_misc`]). `/login` is served
/// unless explicitly disabled, as always; plugin routes and pages are not core groups.
pub const PUBLIC_ROUTE_GROUPS: &[&str] = &["version", "misc"];

/// The route groups a rate limit may name (`api:permissions:rate_limits:<group>`), besides glob
/// patterns over them and path prefixes (`/my-plugin`): the core groups and the groups of the
/// routes outside them ([`crate::routes::route_group_of`]).
pub const RATE_LIMIT_GROUPS: &[&str] = &[
    "actions",
    "connectors",
    "index",
    "jobs",
    "misc",
    "pipes",
    "pipelines",
    "plugins",
    "tokens",
    "users",
    "version",
    "engine",
    "login",
    "docs",
    "mcp",
    "webterm",
    "internal",
    "other",
];

/// The resolved server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// The instance keys served (`mrsm_instance`).
    pub instance_keys: String,
    /// The bind host (`api:server:host`, formerly `api:uvicorn:host`).
    pub host: String,
    /// The bind port.
    pub port: u16,
    /// `--production`.
    pub production: bool,
    /// `--private`.
    pub private: bool,
    /// `--no-auth`.
    pub no_auth: bool,
    /// `--debug` (registers `/id`).
    pub debug: bool,
    /// `--no-dash`.
    pub no_dash: bool,
    /// `--no-webterm` (also implied by `--no-auth`).
    pub no_webterm: bool,
    /// The Meerschaum version reported by `/version`, `/version/mrsm` and `/info`.
    pub version: String,
    /// `MRSM_SERVER_ID`.
    pub server_id: String,
    /// `api:permissions:registration:users`.
    pub registration_users: bool,
    /// `api:permissions:registration:pipes`.
    pub registration_pipes: bool,
    /// `api:permissions:registration:plugins`.
    pub registration_plugins: bool,
    /// `api:permissions:actions:non_admin` (forced `false` by `--secure`).
    pub actions_non_admin: bool,
    /// `system:api:permissions:actions:non_admin`, which `is_user_allowed_to_execute()` reads.
    ///
    /// PARITY: `core/User/__init__.py:44` reads a `system:` key that the default config does not
    /// define, so non-admin users and tokens are refused by the actions and jobs routes.
    pub system_actions_non_admin: bool,
    /// `api:permissions:chaining:child_apis`.
    pub chaining_child_apis: bool,
    /// `api:permissions:instances:allow_multiple_instances`.
    pub allow_multiple_instances: bool,
    /// `api:permissions:instances:allowed_instance_keys` (fnmatch patterns).
    pub allowed_instance_keys: Vec<String>,
    /// `api:permissions:routes:allowlist` (fnmatch patterns over route groups). A missing key
    /// (or `null`) means `['*']`; an explicitly empty list serves no core group.
    pub routes_allowlist: Vec<String>,
    /// Explicitly disabled route groups; overrides the allowlist, including login.
    pub routes_disabled: Vec<String>,
    /// The public preset (`api:permissions:preset: public` or `start api --public`): only
    /// `/login`, `/healthcheck`, `/version`, plugin routes and plugin pages are served. The
    /// allowlist's groups other than a bare `*` are served too, and `routes_disabled` still
    /// removes groups (see [`ServerConfig::route_group_is_allowed`]).
    pub public: bool,
    /// Whether `/openapi.json` is served (always, except under the public preset unless the
    /// `docs` group is allowlisted).
    pub openapi_enabled: bool,
    /// Automatic execution retention (success output, failure output, summaries), seconds.
    pub execution_retention: [u64; 3],
    /// `api:permissions:rate_limits` by route-group pattern or path prefix (keys starting with
    /// `/`), in config order (new in v5).
    pub rate_limits: IndexMap<String, RateLimit>,
    /// `api:permissions:log_streams:max`: log streams (`GET /logs/{name}/stream`) one server
    /// follows at once (new in v5; default 32). Each follows a job on a thread of its own.
    pub log_streams_max: usize,
    /// `api:permissions:log_streams:per_user`: the same, per user or token (default 4).
    pub log_streams_per_user: usize,
    /// `api:server:forwarded_allow_ips` (formerly `api:uvicorn:forwarded_allow_ips`).
    pub forwarded_allow_ips: Vec<String>,
    /// `api:server:proxy_headers`.
    pub proxy_headers: bool,
    /// `api:data:max_response_row_limit`.
    pub max_response_row_limit: i64,
    /// `api:data:chunks:ttl_seconds`.
    pub chunks_ttl_seconds: i64,
    /// Whether `/docs` and `/redoc` are served (`not production or docs_in_production`).
    pub docs_enabled: bool,
    /// `api:cache:session_expires_minutes`.
    pub session_expires_minutes: i64,
    /// `api:tokens:default_expiration_days`.
    pub token_default_expiration_days: i64,
    /// `pipes:attributes:local_cache_timeout_seconds`.
    pub attributes_cache_seconds: f64,
    /// `pipes:dtypes:columns_types_cache_seconds`.
    pub columns_types_cache_seconds: f64,
    /// `pipes:verify:chunk_minutes` defaults etc. for core `Pipe` construction.
    pub pipe_defaults: meerschaum_core::pipe::PipeDefaults,
    /// `meerschaum:connectors` (for `/connectors`).
    pub connectors: Map,
    /// `API_SECRET_KEY_PATH`.
    pub secret_key_path: PathBuf,
    /// `PLUGINS_ARCHIVES_RESOURCES_PATH`.
    pub plugins_archives_path: PathBuf,
    /// `API_STATIC_PATH` (the Python package's static files), if known.
    pub static_path: Option<PathBuf>,
    /// The root directory (`MRSM_ROOT_DIR`).
    pub root_dir: PathBuf,
    /// Deprecation notices collected while resolving the config (logged at startup).
    pub notices: Vec<String>,
    /// `api:mcp` (the MCP server at `/mcp`, §7.2).
    pub mcp: crate::mcp::McpConfig,
    /// `api:webterm:tmux:enabled`: keep each user's terminal in a tmux session when tmux exists.
    pub webterm_tmux_enabled: bool,
    /// `api:webterm:tmux:session_suffix`: appended to the user name to name the tmux session.
    pub webterm_tmux_session_suffix: String,
    /// `api:webterm:tmux:config`: the tmux configuration file the terminal's tmux server starts
    /// with. `None` (the default) writes and uses Meerschaum's own file in the webterm resources
    /// directory (no status bar, mouse on, no plugins).
    pub webterm_tmux_config: Option<PathBuf>,
    /// `WEBTERM_INTERNAL_RESOURCES_PATH`: where the tmux path keeps the shell's environment file.
    pub webterm_resources_path: PathBuf,
}

fn get<'a>(map: &'a Map, keys: &[&str]) -> Option<&'a Value> {
    let mut cur = map.get(*keys.first()?)?;
    for k in &keys[1..] {
        cur = cur.get(k)?;
    }
    Some(cur)
}

fn bool_at(map: &Map, keys: &[&str], default: bool) -> bool {
    match get(map, keys) {
        Some(Value::Null) | None => default,
        Some(v) => v.truthy(),
    }
}

fn int_at(map: &Map, keys: &[&str], default: i64) -> i64 {
    get(map, keys)
        .and_then(meerschaum_core::pyfmt::py_int)
        .unwrap_or(default)
}

fn float_at(map: &Map, keys: &[&str], default: f64) -> f64 {
    get(map, keys).and_then(Value::as_f64).unwrap_or(default)
}

fn str_list(v: Option<&Value>) -> Option<Vec<String>> {
    match v {
        Some(Value::List(items)) => Some(items.iter().map(Value::py_str).collect()),
        Some(Value::Str(s)) => Some(s.split(',').map(|p| p.trim().to_string()).collect()),
        _ => None,
    }
}

fn path_of(config: &Config, name: &str) -> Option<PathBuf> {
    match config.paths().get(name)? {
        PathValue::Path(p) => Some(p),
        PathValue::Name(n) => Some(PathBuf::from(n)),
    }
}

/// The spellings of a rate limit's two numbers: `requests` / `seconds`, or the
/// `max` / `window_seconds` some guides used.
const RATE_LIMIT_KEYS: &[&str] = &["requests", "seconds", "max", "window_seconds"];

/// A rate limit's number, if `v` is one (`10`, `10.0`, `"10"`).
fn rate_number(v: &Value) -> Option<f64> {
    match v {
        Value::Bool(_) => None,
        Value::Str(s) => s.trim().parse::<f64>().ok(),
        other => other.as_f64(),
    }
    .filter(|n| n.is_finite())
}

/// Whether a rate-limit key names something requests can be in: a path prefix (`/my-plugin`), a
/// glob pattern, or one of [`RATE_LIMIT_GROUPS`].
fn known_rate_limit_key(key: &str) -> bool {
    key.starts_with('/') || key.contains(['*', '?', '[']) || RATE_LIMIT_GROUPS.contains(&key)
}

/// `api:permissions:rate_limits`: `{<group pattern or /path>: {requests, seconds}}`, also spelled
/// `{max, window_seconds}`. Entries that cannot be used are skipped with one notice each that
/// names the entry, and so are unknown keys inside an entry.
pub fn parse_rate_limits(
    limits: Option<&Value>,
    notices: &mut Vec<String>,
) -> IndexMap<String, RateLimit> {
    let mut out = IndexMap::new();
    let entries = match limits {
        None | Some(Value::Null) => return out,
        Some(Value::Map(m)) => m,
        Some(other) => {
            notices.push(format!(
                "Ignoring `api:permissions:rate_limits`: expected a mapping of route groups to \
                 {{requests, seconds}}, got {}.",
                other.type_name()
            ));
            return out;
        }
    };
    let groups = RATE_LIMIT_GROUPS.join(", ");
    for (group, spec) in entries {
        let Value::Map(spec) = spec else {
            notices.push(format!(
                "Ignoring the rate limit for '{group}': expected {{requests: N, seconds: S}}, \
                 got {}.",
                spec.type_name()
            ));
            continue;
        };
        let unknown: Vec<&str> = spec
            .keys()
            .map(String::as_str)
            .filter(|k| !RATE_LIMIT_KEYS.contains(k))
            .collect();
        if !unknown.is_empty() {
            notices.push(format!(
                "Unknown key(s) {} in the rate limit for '{group}' (expected `requests` and \
                 `seconds`; `max` and `window_seconds` also work).",
                unknown
                    .iter()
                    .map(|k| format!("`{k}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        // `requests` / `seconds` win when both spellings are given.
        let pick = |a: &str, b: &str| spec.get(a).or_else(|| spec.get(b));
        let requests = pick("requests", "max");
        let seconds = pick("seconds", "window_seconds");
        let Some(requests_value) = requests else {
            notices.push(format!(
                "Ignoring the rate limit for '{group}': it has no `requests` (or `max`)."
            ));
            continue;
        };
        let requests = rate_number(requests_value).filter(|n| *n >= 1.0 && n.fract() == 0.0);
        let Some(requests) = requests else {
            notices.push(format!(
                "Ignoring the rate limit for '{group}': `requests` must be a whole number of at \
                 least 1, got {}.",
                requests_value.py_str()
            ));
            continue;
        };
        let seconds = match seconds {
            // A window of one minute when only the count is given (v5's default).
            None => 60.0,
            Some(v) => match rate_number(v).filter(|n| *n > 0.0) {
                Some(n) => n,
                None => {
                    notices.push(format!(
                        "Ignoring the rate limit for '{group}': `seconds` must be a positive \
                         number, got {}.",
                        v.py_str()
                    ));
                    continue;
                }
            },
        };
        if !known_rate_limit_key(group) {
            notices.push(format!(
                "The rate limit for '{group}' matches no route group (groups: {groups}; use a \
                 path such as '/my-plugin' for plugin routes)."
            ));
        }
        out.insert(
            group.clone(),
            RateLimit {
                requests: if requests >= f64::from(u32::MAX) {
                    u32::MAX
                } else {
                    requests as u32
                },
                // Bounded so `Duration::from_secs_f64` cannot overflow.
                per: Duration::from_secs_f64(seconds.min(1e9)),
            },
        );
    }
    out
}

/// Expand a leading `~` of a configured path.
fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME").filter(|h| !h.is_empty())
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(path)
}

/// The v4 defaults of the `api:uvicorn` keys that moved to `api:server`.
const UVICORN_DEFAULTS: &[(&str, &str)] = &[
    ("host", "0.0.0.0"),
    ("port", "8000"),
    ("forwarded_allow_ips", "*"),
    ("proxy_headers", "True"),
];

impl ServerConfig {
    /// Resolve the configuration from the config registry and the `start api` flags.
    pub fn from_config(config: &Config, flags: &ServerFlags) -> ServerConfig {
        let mut cfg = Map::new();
        for top in ["api", "meerschaum", "system", "pipes"] {
            if let Some(v) = config.get(&[top]) {
                cfg.insert(top.to_string(), v);
            }
        }
        let mut sc = ServerConfig::from_map(&cfg, flags);
        if let Some(p) = path_of(config, "API_SECRET_KEY_PATH") {
            sc.secret_key_path = p;
        }
        if let Some(p) = path_of(config, "PLUGINS_ARCHIVES_RESOURCES_PATH") {
            sc.plugins_archives_path = p;
        }
        if let Some(p) = path_of(config, "WEBTERM_INTERNAL_RESOURCES_PATH") {
            sc.webterm_resources_path = p;
        }
        sc.static_path = path_of(config, "API_STATIC_PATH");
        sc.root_dir = config.paths().root_dir.clone();
        sc.pipe_defaults = meerschaum_core::pipe::PipeDefaults::from_config(config);
        sc
    }

    /// Resolve the configuration from a config map (`{'api': ..., 'meerschaum': ..., ...}`).
    pub fn from_map(cfg: &Map, flags: &ServerFlags) -> ServerConfig {
        let mut notices = Vec::new();
        // `api:server:*` wins; `api:uvicorn:*` is honored with a deprecation notice when it
        // differs from the v4 default.
        let server_or_uvicorn = |key: &str, notices: &mut Vec<String>| -> Option<Value> {
            if let Some(v) = get(cfg, &["api", "server", key]) {
                return Some(v.clone());
            }
            let v = get(cfg, &["api", "uvicorn", key])?.clone();
            let default = UVICORN_DEFAULTS
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, d)| *d);
            if default.is_some_and(|d| d != v.py_str()) {
                notices.push(format!(
                    "The config key 'api:uvicorn:{key}' is deprecated; use 'api:server:{key}'."
                ));
            }
            Some(v)
        };
        let host = flags
            .host
            .clone()
            .or_else(|| server_or_uvicorn("host", &mut notices).map(|v| v.py_str()))
            .unwrap_or_else(|| "0.0.0.0".into());
        let port = flags
            .port
            .or_else(|| {
                server_or_uvicorn("port", &mut notices)
                    .and_then(|v| meerschaum_core::pyfmt::py_int(&v))
                    .and_then(|p| u16::try_from(p).ok())
            })
            .unwrap_or(8000);
        let forwarded_allow_ips = server_or_uvicorn("forwarded_allow_ips", &mut notices)
            .and_then(|v| str_list(Some(&v)))
            .unwrap_or_else(|| vec!["*".into()]);
        let proxy_headers = server_or_uvicorn("proxy_headers", &mut notices)
            .map(|v| v.truthy())
            .unwrap_or(true);

        let instance_keys = flags
            .instance
            .clone()
            .or_else(|| get(cfg, &["meerschaum", "api_instance"]).map(Value::py_str))
            .or_else(|| get(cfg, &["meerschaum", "instance"]).map(Value::py_str))
            .unwrap_or_else(|| "sql:main".into());

        let routes_allowlist = match get(cfg, &["api", "permissions", "routes", "allowlist"]) {
            None | Some(Value::Null) => vec!["*".to_string()],
            v => str_list(v).unwrap_or_default(),
        };

        let routes_disabled =
            str_list(get(cfg, &["api", "permissions", "routes", "disabled"])).unwrap_or_default();

        let rate_limits = parse_rate_limits(
            get(cfg, &["api", "permissions", "rate_limits"]),
            &mut notices,
        );

        // The public preset: `api:permissions:preset: public` or `start api --public`.
        let public = flags.public
            || match get(cfg, &["api", "permissions", "preset"]) {
                None | Some(Value::Null) => false,
                Some(v) => {
                    let name = v.py_str();
                    let name = name.trim();
                    if name.eq_ignore_ascii_case(PUBLIC_PRESET) {
                        true
                    } else {
                        if !name.is_empty() {
                            notices.push(format!(
                                "Ignoring unknown `api:permissions:preset` '{name}' (the only \
                                 preset is '{PUBLIC_PRESET}')."
                            ));
                        }
                        false
                    }
                }
            };
        // Under the preset, an allowlisted group other than a bare `*` (the default) is
        // served in addition to the public ones.
        let preset_allows = |group: &str| {
            routes_allowlist
                .iter()
                .any(|p| p != "*" && fnmatch(group, p))
                && !routes_disabled.iter().any(|p| fnmatch(group, p))
        };

        let production = flags.production;
        let docs_enabled = (!production
            || bool_at(cfg, &["api", "endpoints", "docs_in_production"], true))
            && (!public || preset_allows("docs"));
        let openapi_enabled = !public || preset_allows("docs");
        let connectors = match get(cfg, &["meerschaum", "connectors"]) {
            Some(Value::Map(m)) => m.clone(),
            _ => Map::new(),
        };
        let server_id = std::env::var("MRSM_SERVER_ID").unwrap_or_else(|_| {
            meerschaum_core::users::generate_password(6).unwrap_or_else(|_| "server".into())
        });

        ServerConfig {
            instance_keys,
            host,
            port,
            production,
            private: flags.private,
            no_auth: flags.no_auth,
            debug: flags.debug,
            no_dash: flags.no_dash,
            // The public preset serves no terminal.
            no_webterm: flags.no_webterm || flags.no_auth || public,
            version: crate::VERSION.to_string(),
            server_id,
            // The public preset turns user registration off.
            registration_users: !public
                && bool_at(cfg, &["api", "permissions", "registration", "users"], true),
            registration_pipes: bool_at(
                cfg,
                &["api", "permissions", "registration", "pipes"],
                true,
            ),
            registration_plugins: bool_at(
                cfg,
                &["api", "permissions", "registration", "plugins"],
                true,
            ),
            // PARITY: v4 has two keys. `api:permissions:actions:non_admin` (default true;
            // `start api --secure` sets it to false) gates `edit_pipe` (`api/routes/_pipes.py`) and
            // the terminal (`is_session_authenticated`, `api/dash/sessions.py`).
            actions_non_admin: !flags.secure
                && bool_at(cfg, &["api", "permissions", "actions", "non_admin"], true),
            // PARITY: `is_user_allowed_to_execute` (`core/User/__init__.py:44`, the `/actions` and
            // job routes) reads `system:api:permissions:actions:non_admin`, which v4's default
            // config does not define (`get_config` warns "Invalid keys" and returns `None`), so
            // non-admins are refused unless the key is set by hand. Same default here.
            system_actions_non_admin: bool_at(
                cfg,
                &["system", "api", "permissions", "actions", "non_admin"],
                false,
            ),
            chaining_child_apis: bool_at(
                cfg,
                &["api", "permissions", "chaining", "child_apis"],
                false,
            ),
            allow_multiple_instances: bool_at(
                cfg,
                &[
                    "api",
                    "permissions",
                    "instances",
                    "allow_multiple_instances",
                ],
                true,
            ),
            allowed_instance_keys: str_list(get(
                cfg,
                &["api", "permissions", "instances", "allowed_instance_keys"],
            ))
            .unwrap_or_else(|| vec!["*".into()]),
            routes_allowlist,
            routes_disabled,
            public,
            openapi_enabled,
            execution_retention: [
                ("success_seconds", 86400),
                ("failure_seconds", 604800),
                ("summary_seconds", 2592000),
            ]
            .map(|(key, default)| {
                get(cfg, &["api", "execute", "retention", key])
                    .and_then(meerschaum_core::pyfmt::py_int)
                    .and_then(|v| u64::try_from(v).ok())
                    .unwrap_or(default)
            }),
            rate_limits,
            log_streams_max: usize::try_from(int_at(
                cfg,
                &["api", "permissions", "log_streams", "max"],
                32,
            ))
            .unwrap_or(32)
            .max(1),
            log_streams_per_user: usize::try_from(int_at(
                cfg,
                &["api", "permissions", "log_streams", "per_user"],
                4,
            ))
            .unwrap_or(4)
            .max(1),
            forwarded_allow_ips,
            proxy_headers,
            max_response_row_limit: int_at(
                cfg,
                &["api", "data", "max_response_row_limit"],
                100_000,
            ),
            chunks_ttl_seconds: int_at(cfg, &["api", "data", "chunks", "ttl_seconds"], 1800),
            docs_enabled,
            session_expires_minutes: int_at(
                cfg,
                &["api", "cache", "session_expires_minutes"],
                43200,
            ),
            token_default_expiration_days: int_at(
                cfg,
                &["api", "tokens", "default_expiration_days"],
                366,
            ),
            attributes_cache_seconds: float_at(
                cfg,
                &["pipes", "attributes", "local_cache_timeout_seconds"],
                600.0,
            ),
            columns_types_cache_seconds: float_at(
                cfg,
                &["pipes", "dtypes", "columns_types_cache_seconds"],
                60.0,
            ),
            pipe_defaults: meerschaum_core::pipe::PipeDefaults::default(),
            connectors,
            secret_key_path: PathBuf::from("api/.api_secret_key"),
            plugins_archives_path: PathBuf::from("plugins/.archives"),
            static_path: None,
            root_dir: PathBuf::from("."),
            notices,
            mcp: crate::mcp::McpConfig::from_map(cfg),
            webterm_tmux_enabled: bool_at(cfg, &["api", "webterm", "tmux", "enabled"], true),
            webterm_tmux_session_suffix: match get(
                cfg,
                &["api", "webterm", "tmux", "session_suffix"],
            ) {
                Some(Value::Null) | None => "_mrsm".into(),
                Some(v) => v.py_str(),
            },
            webterm_tmux_config: match get(cfg, &["api", "webterm", "tmux", "config"]) {
                Some(Value::Str(s)) if !s.trim().is_empty() => Some(expand_home(s.trim())),
                _ => None,
            },
            webterm_resources_path: PathBuf::from("internal/webterm"),
        }
    }

    /// A configuration for tests and embedding: defaults with the given instance keys and
    /// paths under `root`.
    pub fn for_root(instance_keys: &str, root: &std::path::Path) -> ServerConfig {
        let mut sc = ServerConfig::from_map(
            &Map::new(),
            &ServerFlags {
                instance: Some(instance_keys.to_string()),
                ..Default::default()
            },
        );
        let paths = meerschaum_core::config::paths::Paths::for_root(root);
        sc.secret_key_path = paths.path("API_SECRET_KEY_PATH");
        sc.plugins_archives_path = paths.path("PLUGINS_ARCHIVES_RESOURCES_PATH");
        sc.webterm_resources_path = paths.path("WEBTERM_INTERNAL_RESOURCES_PATH");
        sc.root_dir = root.to_path_buf();
        sc
    }

    /// The startup warning for an API that gives a shell to anyone who can reach it, or `None`.
    ///
    /// With `api:permissions:registration:users` on (the default) anyone can register an account,
    /// and with `api:permissions:actions:non_admin` on (the default) every logged-in user may open
    /// the web terminal (v4's behavior, kept). On an address other than loopback that is a shell
    /// for whoever can connect. `--secure` and `--private` close it, as do the two settings; with
    /// no console (`--no-dash`), `--no-webterm` or `--no-auth` there is no terminal at all.
    pub fn open_terminal_warning(&self) -> Option<String> {
        let terminal = !self.no_dash && !self.no_webterm && !self.no_auth;
        if !(terminal && self.registration_users && self.actions_non_admin && !self.private)
            || self.is_loopback()
        {
            return None;
        }
        Some(format!(
            "Anyone who can reach {}:{} can register an account and open a shell: user registration (`api:permissions:registration:users`) and terminals for non-admins (`api:permissions:actions:non_admin`) are both on. Start with `--secure` or `--private`, or set `api:permissions:registration:users` or `api:permissions:actions:non_admin` to false.",
            self.host, self.port
        ))
    }

    /// The startup warning for `--no-auth` on an address other than loopback, or `None`: every
    /// route, `/actions` included, is then open to anyone who can connect (v4 behaves the same).
    pub fn open_no_auth_warning(&self) -> Option<String> {
        if !self.no_auth || self.is_loopback() {
            return None;
        }
        Some(format!(
            "`--no-auth` on {}:{}: authentication is off, so anyone who can reach this address can run actions and read and change data. Bind to 127.0.0.1 or drop `--no-auth`.",
            self.host, self.port
        ))
    }

    fn is_loopback(&self) -> bool {
        matches!(self.host.as_str(), "localhost" | "::1" | "[::1]") || self.host.starts_with("127.")
    }

    /// Whether a core route group is served (`route_group_is_allowed`).
    ///
    /// Under the public preset only [`PUBLIC_ROUTE_GROUPS`] are, plus the groups the allowlist
    /// names explicitly (any pattern but a bare `*`, the default); `routes_disabled` always wins.
    pub fn route_group_is_allowed(&self, group: &str) -> bool {
        if self.routes_disabled.iter().any(|p| fnmatch(group, p)) {
            return false;
        }
        if self.public {
            return PUBLIC_ROUTE_GROUPS.contains(&group) || self.preset_allowlisted(group);
        }
        self.routes_allowlist.iter().any(|p| fnmatch(group, p))
    }

    /// Under the public preset: whether the allowlist names `group` explicitly (a pattern other
    /// than a bare `*`) and it is not disabled.
    pub fn preset_allowlisted(&self, group: &str) -> bool {
        self.routes_allowlist
            .iter()
            .any(|p| p != "*" && fnmatch(group, p))
            && !self.routes_disabled.iter().any(|p| fnmatch(group, p))
    }

    /// Whether every route of the `misc` group is served: always, except under the public preset,
    /// which keeps only `/healthcheck` and `/favicon.ico` unless the allowlist names `misc`.
    pub fn serves_full_misc(&self) -> bool {
        !self.public || self.preset_allowlisted("misc")
    }

    /// The startup line of the public preset (what it serves), or `None` without it.
    pub fn preset_notice(&self) -> Option<String> {
        if !self.public {
            return None;
        }
        let extra: Vec<&str> = crate::routes::CORE_ROUTE_GROUPS
            .iter()
            .chain(["mcp", "docs"].iter())
            .copied()
            .filter(|g| !PUBLIC_ROUTE_GROUPS.contains(g) && self.preset_allowlisted(g))
            .collect();
        let mut line =
            "Public preset: serving only /login, /healthcheck, /version, plugin routes and \
             plugin pages; the web console, web terminal, actions, user registration and API docs \
             are off."
                .to_string();
        if !extra.is_empty() {
            line.push_str(&format!(
                " Also serving the allowlisted route groups: {}.",
                extra.join(", ")
            ));
        }
        Some(line)
    }

    /// The rate limit for a route group (the first matching pattern).
    pub fn rate_limit_for(&self, group: &str) -> Option<RateLimit> {
        self.rate_limits
            .iter()
            .find(|(pattern, _)| !pattern.starts_with('/') && fnmatch(group, pattern))
            .map(|(_, l)| *l)
    }

    /// The rate limit of a request and the bucket it counts against: the longest path-prefix key
    /// (`/my-plugin` matches `/my-plugin` and `/my-plugin/...`) wins, else the first route-group
    /// pattern matching `group`.
    pub fn rate_limit_for_request(&self, path: &str, group: &str) -> Option<(String, RateLimit)> {
        let by_path = self
            .rate_limits
            .iter()
            .filter(|(key, _)| key.starts_with('/'))
            .filter(|(key, _)| {
                let prefix = key.trim_end_matches('/');
                prefix.is_empty()
                    || path == prefix
                    || path
                        .strip_prefix(prefix)
                        .is_some_and(|rest| rest.starts_with('/'))
            })
            .max_by_key(|(key, _)| key.trim_end_matches('/').len());
        if let Some((key, limit)) = by_path {
            return Some((key.clone(), *limit));
        }
        self.rate_limits
            .iter()
            .find(|(pattern, _)| !pattern.starts_with('/') && fnmatch(group, pattern))
            .map(|(_, l)| (group.to_string(), *l))
    }
}

/// Python's `fnmatch.fnmatchcase(name, pattern)`: `*`, `?`, `[seq]`, `[!seq]`.
pub fn fnmatch(name: &str, pattern: &str) -> bool {
    let n: Vec<char> = name.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    fn class_match(p: &[char], mut i: usize, c: char) -> Option<(bool, usize)> {
        // `i` points just past '['.
        let negate = p.get(i) == Some(&'!');
        if negate {
            i += 1;
        }
        let start = i;
        let mut matched = false;
        while i < p.len() {
            if p[i] == ']' && i > start {
                return Some((matched != negate, i + 1));
            }
            if i + 2 < p.len() && p[i + 1] == '-' && p[i + 2] != ']' {
                if p[i] <= c && c <= p[i + 2] {
                    matched = true;
                }
                i += 3;
            } else {
                if p[i] == c {
                    matched = true;
                }
                i += 1;
            }
        }
        None
    }
    fn rec(n: &[char], p: &[char]) -> bool {
        match p.first() {
            None => n.is_empty(),
            Some('*') => (0..=n.len()).any(|k| rec(&n[k..], &p[1..])),
            Some('?') => !n.is_empty() && rec(&n[1..], &p[1..]),
            Some('[') => match (n.first(), class_match(p, 1, *n.first().unwrap_or(&'\0'))) {
                (Some(_), Some((true, next))) => rec(&n[1..], &p[next..]),
                (Some(_), Some((false, _))) => false,
                (Some(c), None) => *c == '[' && rec(&n[1..], &p[1..]),
                (None, _) => false,
            },
            Some(c) => n.first() == Some(c) && rec(&n[1..], &p[1..]),
        }
    }
    rec(&n, &p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_registration_with_non_admin_terminals_warns_off_loopback() {
        let warn = |host: &str, secure: bool, private: bool, patch: &str| {
            let Value::Map(map) = Value::from_json_str(patch).unwrap() else {
                panic!("a map")
            };
            ServerConfig::from_map(
                &map,
                &ServerFlags {
                    host: Some(host.into()),
                    secure,
                    private,
                    ..Default::default()
                },
            )
            .open_terminal_warning()
        };
        // The defaults: open registration, terminals for everyone.
        for host in ["0.0.0.0", "::", "192.168.1.10", "example.test"] {
            let text = warn(host, false, false, "{}").unwrap_or_else(|| panic!("{host}"));
            assert!(text.contains(host) && text.contains("--secure"), "{text}");
        }
        // Loopback binds, `--secure`, `--private` and either setting turned off are quiet.
        for host in ["127.0.0.1", "127.0.1.1", "localhost", "::1"] {
            assert!(warn(host, false, false, "{}").is_none(), "{host}");
        }
        assert!(warn("0.0.0.0", true, false, "{}").is_none());
        assert!(warn("0.0.0.0", false, true, "{}").is_none());
        let registration = r#"{"api": {"permissions": {"registration": {"users": false}}}}"#;
        assert!(warn("0.0.0.0", false, false, registration).is_none());
        let terminals = r#"{"api": {"permissions": {"actions": {"non_admin": false}}}}"#;
        assert!(warn("0.0.0.0", false, false, terminals).is_none());
    }

    #[test]
    fn no_auth_off_loopback_warns() {
        let warn = |host: &str, no_auth: bool| {
            ServerConfig::from_map(
                &Map::new(),
                &ServerFlags {
                    host: Some(host.into()),
                    no_auth,
                    ..Default::default()
                },
            )
            .open_no_auth_warning()
        };
        let text = warn("0.0.0.0", true).unwrap();
        assert!(
            text.contains("--no-auth") && text.contains("0.0.0.0"),
            "{text}"
        );
        assert!(warn("127.0.0.1", true).is_none());
        assert!(warn("localhost", true).is_none());
        assert!(warn("0.0.0.0", false).is_none());
    }

    #[test]
    fn fnmatch_semantics() {
        assert!(fnmatch("pipes", "*"));
        assert!(fnmatch("pipes", "pi*"));
        assert!(!fnmatch("pipes", "jobs"));
        assert!(fnmatch("sql:main", "sql:*"));
        assert!(fnmatch("a", "[abc]"));
        assert!(!fnmatch("a", "[!abc]"));
        assert!(fnmatch("users", "u?ers"));
    }

    #[test]
    fn allowlist_and_renames() {
        let mut cfg = Map::new();
        let api: Value = serde_json::from_str(
            r#"{"permissions": {"routes": {"allowlist": []},
                "rate_limits": {"login": {"requests": 2, "seconds": 1}}},
               "uvicorn": {"forwarded_allow_ips": "10.0.0.1", "port": 8000}}"#,
        )
        .unwrap();
        cfg.insert("api".into(), api);
        let sc = ServerConfig::from_map(&cfg, &ServerFlags::default());
        assert!(!sc.route_group_is_allowed("pipes"));
        assert_eq!(sc.forwarded_allow_ips, vec!["10.0.0.1".to_string()]);
        assert_eq!(sc.notices.len(), 1);
        assert_eq!(sc.rate_limit_for("login").unwrap().requests, 2);
        assert!(sc.rate_limit_for("pipes").is_none());
        let sc = ServerConfig::from_map(&Map::new(), &ServerFlags::default());
        assert!(sc.route_group_is_allowed("pipes"));
        assert!(sc.notices.is_empty());
    }

    fn map(json: &str) -> Map {
        let Value::Map(m) = Value::from_json_str(json).unwrap() else {
            panic!("a map")
        };
        m
    }

    #[test]
    fn rate_limits_accept_both_spellings_and_warn_on_the_rest() {
        let sc = ServerConfig::from_map(
            &map(r#"{"api": {"permissions": {"rate_limits": {
                    "login": {"requests": 5, "seconds": 60},
                    "pipes": {"max": 100, "window_seconds": 10},
                    "jobs": {"requests": 3},
                    "/my-plugin": {"max": 2, "window_seconds": 1},
                    "users": {"requests": 4, "seconds": 2, "burst": 9},
                    "tokens": {"requests": 0, "seconds": 1},
                    "plugins": {"requests": "x", "seconds": 1},
                    "engine": {"requests": 2, "seconds": -1},
                    "connectors": 7,
                    "logins": {"requests": 1, "seconds": 1}
                }}}}"#),
            &ServerFlags::default(),
        );
        let login = sc.rate_limit_for("login").unwrap();
        assert_eq!((login.requests, login.per), (5, Duration::from_secs(60)));
        let pipes = sc.rate_limit_for("pipes").unwrap();
        assert_eq!((pipes.requests, pipes.per), (100, Duration::from_secs(10)));
        // A count without a window: one minute.
        assert_eq!(
            sc.rate_limit_for("jobs").unwrap().per,
            Duration::from_secs(60)
        );
        // An unknown key is a notice, the entry still applies.
        assert_eq!(sc.rate_limit_for("users").unwrap().requests, 4);
        for bad in ["tokens", "plugins", "engine", "connectors"] {
            assert!(sc.rate_limit_for(bad).is_none(), "{bad}");
        }
        let notices = sc.notices.join("\n");
        for needle in [
            "`burst` in the rate limit for 'users'",
            "'tokens'",
            "'plugins'",
            "'engine'",
            "'connectors'",
            "The rate limit for 'logins' matches no route group",
        ] {
            assert!(notices.contains(needle), "{needle}:\n{notices}");
        }
        assert!(!notices.contains("for 'pipes'"), "{notices}");
        assert!(!notices.contains("for '/my-plugin'"), "{notices}");
        assert!(!notices.contains("for 'jobs'"), "{notices}");
        // Path prefixes win over groups and match whole segments only.
        let (bucket, limit) = sc
            .rate_limit_for_request("/my-plugin/data", "other")
            .unwrap();
        assert_eq!((bucket.as_str(), limit.requests), ("/my-plugin", 2));
        assert!(sc.rate_limit_for_request("/my-plugins", "other").is_none());
        let (bucket, _) = sc.rate_limit_for_request("/pipes/a/b/c", "pipes").unwrap();
        assert_eq!(bucket, "pipes");
    }

    #[test]
    fn public_preset_serves_only_public_routes() {
        let public = |patch: &str, flag: bool| {
            ServerConfig::from_map(
                &map(patch),
                &ServerFlags {
                    public: flag,
                    ..Default::default()
                },
            )
        };
        for sc in [
            public(r#"{"api": {"permissions": {"preset": "public"}}}"#, false),
            public("{}", true),
            // The default allowlist (`['*']`) adds nothing.
            public(
                r#"{"api": {"permissions": {"routes": {"allowlist": ["*"]}}}}"#,
                true,
            ),
        ] {
            assert!(sc.public);
            for group in ["version", "misc"] {
                assert!(sc.route_group_is_allowed(group), "{group}");
            }
            for group in [
                "pipes",
                "actions",
                "jobs",
                "users",
                "tokens",
                "engine",
                "pipelines",
                "mcp",
                "index",
                "connectors",
                "plugins",
            ] {
                assert!(!sc.route_group_is_allowed(group), "{group}");
            }
            assert!(!sc.serves_full_misc());
            assert!(sc.no_webterm && !sc.registration_users);
            assert!(!sc.docs_enabled && !sc.openapi_enabled);
            assert!(sc.preset_notice().unwrap().contains("/healthcheck"));
        }
        // An explicit allowlist adds groups on top; `disabled` still removes them.
        let sc = public(
            r#"{"api": {"permissions": {"routes": {"allowlist": ["pipes", "docs", "misc"],
                "disabled": ["version"]}}}}"#,
            true,
        );
        assert!(sc.route_group_is_allowed("pipes"));
        assert!(!sc.route_group_is_allowed("version"));
        assert!(!sc.route_group_is_allowed("jobs"));
        assert!(sc.serves_full_misc() && sc.docs_enabled && sc.openapi_enabled);
        assert!(sc.preset_notice().unwrap().contains("pipes"));
        // An unknown preset is a notice; no preset is the usual server.
        let sc = public(r#"{"api": {"permissions": {"preset": "kiosk"}}}"#, false);
        assert!(!sc.public && sc.route_group_is_allowed("pipes"));
        assert!(sc.notices.iter().any(|n| n.contains("kiosk")));
        let sc = public("{}", false);
        assert!(!sc.public && sc.preset_notice().is_none() && sc.openapi_enabled);
    }

    #[test]
    fn tmux_config_path_is_optional() {
        let sc = ServerConfig::from_map(&Map::new(), &ServerFlags::default());
        assert!(sc.webterm_tmux_config.is_none());
        let sc = ServerConfig::from_map(
            &map(r#"{"api": {"webterm": {"tmux": {"config": "/etc/mrsm-tmux.conf"}}}}"#),
            &ServerFlags::default(),
        );
        assert_eq!(
            sc.webterm_tmux_config.as_deref(),
            Some(std::path::Path::new("/etc/mrsm-tmux.conf"))
        );
    }
}
