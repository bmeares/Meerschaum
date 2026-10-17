//! The shared static configuration (D15): `static.toml`, the port of
//! `meerschaum._internal.static.STATIC_CONFIG`.
//!
//! The TOML file is embedded at compile time. [`static_config`] returns the typed
//! [`StaticConfig`]; [`StaticConfig::get`] indexes the generic [`Value`] tree with the same key
//! paths as the Python dictionary (`STATIC_CONFIG['users']['password_hash']['salt_bytes']`).

use std::sync::OnceLock;

use indexmap::IndexMap;
use serde::Deserialize;

use crate::value::{Map, Value};

/// The raw contents of `static.toml`.
pub const STATIC_TOML: &str = include_str!("../static.toml");

/// Marker key for Python `None` in `static.toml`: `{ __null__ = true }`.
pub const NULL_MARKER: &str = "__null__";
/// Marker key for `(os.cpu_count() or N)` in `static.toml`: `{ __cpu_count_or__ = N }`.
pub const CPU_COUNT_MARKER: &str = "__cpu_count_or__";

/// Parse `static.toml` into a [`Value`], resolving the markers with the given CPU count
/// (`None` mimics `os.cpu_count()` returning `None`).
pub fn parse_static_toml(text: &str, cpu_count: Option<i64>) -> Result<Value, toml::de::Error> {
    let raw: Value = toml::from_str(text)?;
    Ok(resolve_markers(raw, cpu_count))
}

/// Replace `{ __null__ = true }` and `{ __cpu_count_or__ = N }` markers.
pub fn resolve_markers(value: Value, cpu_count: Option<i64>) -> Value {
    match value {
        Value::Map(m) => {
            if m.len() == 1 {
                if let Some(Value::Bool(true)) = m.get(NULL_MARKER) {
                    return Value::Null;
                }
                if let Some(fallback) = m.get(CPU_COUNT_MARKER) {
                    return match cpu_count {
                        Some(n) if n > 0 => Value::Int(n),
                        _ => fallback.clone(),
                    };
                }
            }
            Value::Map(
                m.into_iter()
                    .map(|(k, v)| (k, resolve_markers(v, cpu_count)))
                    .collect(),
            )
        }
        Value::List(l) => Value::List(
            l.into_iter()
                .map(|v| resolve_markers(v, cpu_count))
                .collect(),
        ),
        other => other,
    }
}

/// The number of logical CPUs, like Python's `os.cpu_count()` (online CPUs, ignoring affinity).
pub fn os_cpu_count() -> Option<i64> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(text) = std::fs::read_to_string("/sys/devices/system/cpu/online") {
            let mut total = 0i64;
            for range in text.trim().split(',') {
                let mut parts = range.split('-');
                let start: Option<i64> = parts.next().and_then(|s| s.trim().parse().ok());
                let end: Option<i64> = parts.next().and_then(|s| s.trim().parse().ok());
                match (start, end) {
                    (Some(s), Some(e)) => total += e - s + 1,
                    (Some(_), None) => total += 1,
                    _ => {}
                }
            }
            if total > 0 {
                return Some(total);
            }
        }
    }
    std::thread::available_parallelism()
        .ok()
        .and_then(|n| i64::try_from(n.get()).ok())
}

/// The typed static configuration. Field names match the Python keys.
#[derive(Debug, Clone, Deserialize)]
pub struct StaticConfig {
    /// `STATIC_CONFIG['api']`.
    pub api: ApiStatic,
    /// `STATIC_CONFIG['sql']`.
    pub sql: SqlStatic,
    /// `STATIC_CONFIG['valkey']`.
    pub valkey: ValkeyStatic,
    /// `STATIC_CONFIG['environment']`: names of the `MRSM_*` environment variables.
    pub environment: EnvironmentStatic,
    /// `STATIC_CONFIG['config']`.
    pub config: ConfigStatic,
    /// `STATIC_CONFIG['system']`.
    pub system: SystemStatic,
    /// `STATIC_CONFIG['connectors']`.
    pub connectors: ConnectorsStatic,
    /// `STATIC_CONFIG['dtypes']`.
    pub dtypes: DtypesStatic,
    /// `STATIC_CONFIG['stack']`.
    pub stack: StackStatic,
    /// `STATIC_CONFIG['users']`.
    pub users: UsersStatic,
    /// `STATIC_CONFIG['plugins']`.
    pub plugins: PluginsStatic,
    /// `STATIC_CONFIG['pipes']`.
    pub pipes: PipesStatic,
    /// `STATIC_CONFIG['jobs']`.
    pub jobs: JobsStatic,
    /// `STATIC_CONFIG['tokens']`.
    pub tokens: TokensStatic,
    /// The full tree as a [`Value`], for [`StaticConfig::get`].
    #[serde(skip)]
    pub tree: Value,
}

/// `STATIC_CONFIG['api']`.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiStatic {
    /// Route paths by name.
    pub endpoints: IndexMap<String, String>,
    /// OAuth settings.
    pub oauth: ApiOauthStatic,
    /// Name of the webterm job.
    pub webterm_job_name: String,
    /// Default request timeout in seconds.
    pub default_timeout: i64,
    /// Job-related API constants.
    pub jobs: ApiJobsStatic,
}

/// `STATIC_CONFIG['api']['oauth']`.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiOauthStatic {
    /// Lifetime of issued access tokens.
    pub token_expires_minutes: i64,
}

/// `STATIC_CONFIG['api']['jobs']`.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiJobsStatic {
    /// Websocket message requesting stdin.
    pub stdin_message: String,
    /// Websocket message signaling a stop.
    pub stop_message: String,
    /// Job metadata cache lifetime.
    pub metadata_cache_seconds: i64,
    /// Prefix for temporary API jobs.
    pub temp_prefix: String,
}

/// `STATIC_CONFIG['sql']`.
#[derive(Debug, Clone, Deserialize)]
pub struct SqlStatic {
    /// Schema for internal tables.
    pub internal_schema: String,
    /// Schema for instance tables.
    pub instance_schema: String,
    /// Default SQLAlchemy `create_engine` arguments.
    pub default_create_engine_args: Map,
    /// Per-flavor engine settings.
    pub create_engine_flavors: IndexMap<String, FlavorStatic>,
}

/// One entry of `STATIC_CONFIG['sql']['create_engine_flavors']`.
#[derive(Debug, Clone, Deserialize)]
pub struct FlavorStatic {
    /// SQLAlchemy driver name.
    pub engine: String,
    /// `create_engine` keyword arguments.
    pub create_engine: Map,
    /// Arguments omitted from `create_engine` (a Python `set`).
    pub omit_create_engine: Vec<String>,
    /// `to_sql` keyword arguments.
    pub to_sql: Map,
    /// Required connector attributes (a Python `set`, or `''` for DuckDB).
    pub requirements: Value,
    /// Default connector attributes.
    pub defaults: Map,
}

/// `STATIC_CONFIG['valkey']`.
#[derive(Debug, Clone, Deserialize)]
pub struct ValkeyStatic {
    /// Replacement for `:` in Valkey keys.
    pub colon: String,
}

/// `STATIC_CONFIG['environment']`.
#[derive(Debug, Clone, Deserialize)]
pub struct EnvironmentStatic {
    /// `MRSM_CONFIG`.
    pub config: String,
    /// `MRSM_CONFIG_DIR`.
    pub config_dir: String,
    /// `MRSM_PATCH`.
    pub patch: String,
    /// `MRSM_ROOT_DIR`.
    pub root: String,
    /// `MRSM_PLUGINS_DIR`.
    pub plugins: String,
    /// `MRSM_VENVS_DIR`.
    pub venvs: String,
    /// `MRSM_RUNTIME`.
    pub runtime: String,
    /// `MRSM_WORK_DIR`.
    pub work_dir: String,
    /// `MRSM_USER`.
    pub user: String,
    /// `MRSM_DEP_GROUP`.
    pub dep_group: String,
    /// `MRSM_HOME`.
    pub home: String,
    /// `MRSM_SRC`.
    pub src: String,
    /// `MRSM_UID`.
    pub uid: String,
    /// `MRSM_GID`.
    pub gid: String,
    /// `MRSM_NOASK`.
    pub noask: String,
    /// `MRSM_NONINTERACTIVE`.
    pub noninteractive: String,
    /// `MRSM_NO_AUTO_INSTALL`.
    pub no_auto_install: String,
    /// `MRSM_SERVER_ID`.
    pub id: String,
    /// `MRSM_TEST_FLAVORS`.
    pub test_flavors: String,
    /// `MRSM_DAEMON_ID`.
    pub daemon_id: String,
    /// `MRSM__COMPOSE_PROJECT`.
    pub compose_project: String,
    /// `MRSM_SYSTEMD_LOG_PATH`.
    pub systemd_log_path: String,
    /// `MRSM_SYSTEMD_STDIN_PATH`.
    pub systemd_stdin_path: String,
    /// `MRSM_SYSTEMD_RESULT_PATH`.
    pub systemd_result_path: String,
    /// `MRSM_SYSTEMD_DELETE_JOB`.
    pub systemd_delete_job: String,
    /// Regex for `MRSM_<TYPE>_<LABEL>` connector variables.
    pub uri_regex: String,
    /// `MRSM_`.
    pub prefix: String,
}

/// `STATIC_CONFIG['config']`.
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigStatic {
    /// Filetype for newly written config files (`json`).
    pub default_filetype: String,
    /// The in-memory key holding `MRSM{}` originals (`_symlinks`).
    pub symlinks_key: String,
    /// Config backups kept per key.
    pub max_backups_per_key: i64,
}

/// `STATIC_CONFIG['system']`.
#[derive(Debug, Clone, Deserialize)]
pub struct SystemStatic {
    /// Argument-parsing constants.
    pub arguments: ArgumentsStatic,
    /// External URLs.
    pub urls: IndexMap<String, String>,
    /// Success-message handling.
    pub success: SuccessStatic,
    /// Prompt settings.
    pub prompt: PromptStatic,
    /// `fetch_pipes_keys` settings.
    pub fetch_pipes_keys: FetchPipesKeysStatic,
}

/// `STATIC_CONFIG['system']['arguments']`.
#[derive(Debug, Clone, Deserialize)]
pub struct ArgumentsStatic {
    /// Sub-argument delimiters (a Python tuple).
    pub sub_decorators: Vec<String>,
    /// Temporary stand-in for underscores while parsing.
    pub underscore_standin: String,
    /// Key under which argparse exceptions are stored.
    pub failure_key: String,
    /// Action chaining token (`+`).
    pub and_key: String,
    /// Escaped chaining token (`++`).
    pub escaped_and_key: String,
    /// Pipeline flag token (`:`).
    pub pipeline_key: String,
    /// Escaped pipeline token (`::`).
    pub escaped_pipeline_key: String,
}

/// `STATIC_CONFIG['system']['success']`.
#[derive(Debug, Clone, Deserialize)]
pub struct SuccessStatic {
    /// Messages not printed after a successful action (a Python tuple containing `None`).
    pub ignore: Vec<Option<String>>,
}

/// `STATIC_CONFIG['system']['prompt']`.
#[derive(Debug, Clone, Deserialize)]
pub struct PromptStatic {
    /// Whether prompts are served over the web.
    pub web: bool,
}

/// `STATIC_CONFIG['system']['fetch_pipes_keys']`.
#[derive(Debug, Clone, Deserialize)]
pub struct FetchPipesKeysStatic {
    /// Prefix negating a key filter (`_`).
    pub negation_prefix: String,
}

/// `STATIC_CONFIG['connectors']`.
#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorsStatic {
    /// Default connector label (`main`).
    pub default_label: String,
}

/// `STATIC_CONFIG['dtypes']`.
#[derive(Debug, Clone, Deserialize)]
pub struct DtypesStatic {
    /// Datetime settings.
    pub datetime: DatetimeDtypeStatic,
}

/// `STATIC_CONFIG['dtypes']['datetime']`.
#[derive(Debug, Clone, Deserialize)]
pub struct DatetimeDtypeStatic {
    /// Default precision unit (`microsecond`).
    pub default_precision_unit: String,
}

/// `STATIC_CONFIG['stack']`.
#[derive(Debug, Clone, Deserialize)]
pub struct StackStatic {
    /// Stand-in for `$` in the generated compose file.
    pub dollar_standin: String,
}

/// `STATIC_CONFIG['users']`.
#[derive(Debug, Clone, Deserialize)]
pub struct UsersStatic {
    /// Password hashing parameters.
    pub password_hash: PasswordHashStatic,
    /// Minimum username length.
    pub min_username_length: i64,
    /// Maximum username length.
    pub max_username_length: i64,
    /// Minimum password length.
    pub min_password_length: i64,
}

/// `STATIC_CONFIG['users']['password_hash']`.
#[derive(Debug, Clone, Deserialize)]
pub struct PasswordHashStatic {
    /// Digest name (`sha256`).
    pub algorithm_name: String,
    /// Salt length in bytes.
    pub salt_bytes: usize,
    /// Accepted schemes.
    pub schemes: Vec<String>,
    /// Default scheme.
    pub default: String,
    /// Default PBKDF2 rounds (key `pbkdf2_sha256__default_rounds`).
    #[serde(rename = "pbkdf2_sha256__default_rounds")]
    pub pbkdf2_sha256_default_rounds: u32,
}

/// `STATIC_CONFIG['plugins']`.
#[derive(Debug, Clone, Deserialize)]
pub struct PluginsStatic {
    /// Separator between a plugin name and its repository (`@`).
    pub repo_separator: String,
    /// Total lock wait.
    pub lock_sleep_total: f64,
    /// Lock polling interval.
    pub lock_sleep_increment: f64,
    /// Filename recording plugin origins.
    pub origins_filename: String,
}

/// `STATIC_CONFIG['pipes']`.
#[derive(Debug, Clone, Deserialize)]
pub struct PipesStatic {
    /// Dtype settings.
    pub dtypes: PipesDtypesStatic,
    /// Maximum bound time span in days.
    pub max_bound_time_days: i64,
}

/// `STATIC_CONFIG['pipes']['dtypes']`.
#[derive(Debug, Clone, Deserialize)]
pub struct PipesDtypesStatic {
    /// Ratio of changed columns above which a full `astype` is used.
    pub min_ratio_columns_changed_for_full_astype: f64,
}

/// `STATIC_CONFIG['jobs']`.
#[derive(Debug, Clone, Deserialize)]
pub struct JobsStatic {
    /// Restart check interval.
    pub check_restart_seconds: f64,
    /// Log token signaling a stop.
    pub stop_token: String,
    /// Log token signaling a clear.
    pub clear_token: String,
    /// Log token signaling a flush.
    pub flush_token: String,
}

/// `STATIC_CONFIG['tokens']`.
#[derive(Debug, Clone, Deserialize)]
pub struct TokensStatic {
    /// Minimum generated secret length.
    pub minimum_length: i64,
    /// Maximum generated secret length.
    pub maximum_length: i64,
    /// Hash rounds for token secrets.
    pub hash_rounds: u32,
    /// Scope names and descriptions, in order.
    pub scopes: IndexMap<String, String>,
}

impl StaticConfig {
    /// Parse a `static.toml` document with an explicit CPU count (for tests).
    pub fn from_toml(text: &str, cpu_count: Option<i64>) -> Result<StaticConfig, String> {
        let tree = parse_static_toml(text, cpu_count).map_err(|e| e.to_string())?;
        let json = serde_json::to_string(&tree).map_err(|e| e.to_string())?;
        let mut cfg: StaticConfig = serde_json::from_str(&json).map_err(|e| e.to_string())?;
        cfg.tree = tree;
        Ok(cfg)
    }

    /// Index the static config by keys, like `STATIC_CONFIG[k1][k2]...`.
    ///
    /// ```
    /// let cfg = meerschaum_core::static_config();
    /// let v = cfg.get(&["users", "password_hash", "salt_bytes"]).unwrap();
    /// assert_eq!(v.as_i64(), Some(16));
    /// ```
    pub fn get(&self, keys: &[&str]) -> Option<&Value> {
        self.tree.get_path(keys)
    }

    /// The whole static config as a [`Value`] tree.
    pub fn as_value(&self) -> &Value {
        &self.tree
    }

    /// The token scope names in definition order (`list(STATIC_CONFIG['tokens']['scopes'])`).
    pub fn scope_names(&self) -> Vec<String> {
        self.tokens.scopes.keys().cloned().collect()
    }
}

/// The process-wide static config, parsed once from the embedded `static.toml`.
pub fn static_config() -> &'static StaticConfig {
    static CELL: OnceLock<StaticConfig> = OnceLock::new();
    CELL.get_or_init(|| {
        StaticConfig::from_toml(STATIC_TOML, os_cpu_count()).expect("embedded static.toml is valid")
    })
}
