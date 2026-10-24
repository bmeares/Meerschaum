//! The config registry: a port of `meerschaum.config` (`get_config`, `read_config`,
//! `write_config`, `search_and_substitute_config`, the environment overrides and the paths).
//!
//! The registry mirrors Python's process-global, lazily-loaded `config` dictionary:
//!
//! - Each top-level key is one file under `CONFIG_DIR_PATH` (`<key>.yaml`, `.yml` or `.json`),
//!   read the first time the key is requested.
//! - Missing keys fall back to the defaults (`meerschaum/config/_default.py`, embedded as
//!   `default_config.json`); falling back merges the defaults into the loaded key and, when
//!   `patch` is set and the key's file is absent, writes the loaded config to disk.
//! - `MRSM{a:b:c}` symlinks are substituted from other config values.
//! - `MRSM_SQL_<LABEL>` / `MRSM_API_<LABEL>` (any `MRSM_<TYPE>_<LABEL>` of an existing connector
//!   type, see [`connector_types`]) variables add connectors, and `MRSM_CONFIG` / `MRSM_PATCH` patch the config, when the registry is created.
//! - `patch_config/` and `permanent_patch_config/` directories under the root are applied.
//!
//! Lookups that load files or fall back to defaults mutate the in-memory state exactly as the
//! Python implementation does, so the order of lookups is observable (as in Python).
//!
//! **The state is lossless.** The registry stores [`ConfigValue`]s: dates, bytes, integers of any
//! size, tuples, sets and non-string mapping keys survive `read_config`, `get_config` and
//! `write_config` as PyYAML builds them. The `*_lossless` methods are the real implementation;
//! [`Config::get`], [`ConfigState::read_config`] and friends are the JSON-like projection
//! ([`crate::Value`], lossy for those leaves) the other crates use.
//!
//! **Messages.** v4 reports through `print()` (stdout) and `warn()` (stderr). The registry
//! records each as a [`Notice`] of the matching kind, in order, so the Python boundary can replay
//! them on the right stream; Rust callers read [`Config::take_warnings`].

pub mod backup;
pub mod connector_types;
pub mod lossless;
pub mod paths;
pub mod sync;

use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard};

use regex::Regex;

use crate::pyfmt::{JsonStyle, json_dumps_str, py_repr, str_repr, string_to_dict};
use crate::static_config::static_config;
use crate::value::{Map, Value};
use crate::yaml::dumper::{DumpOptions, dump_node};
use crate::yaml::loader::{LoadOptions, load_text};
use crate::yaml::scanner::PyYamlError;

pub use lossless::{ConfigMap, ConfigValue, JsonDecodeError};
pub use paths::{PathValue, Paths};

use lossless::{
    apply_patch, apply_patch_maps, json_dumps, json_dumps_lossless, json_dumps_map_lossless,
    json_loads, map_contains_text, py_str, restore_placeholders,
};

/// The embedded default configuration with machine-dependent markers (see
/// `tests/fixtures/gen_default_config.py`).
pub const DEFAULT_CONFIG_JSON: &str = include_str!("default_config.json");

/// The marker substring replaced by the root directory in [`DEFAULT_CONFIG_JSON`].
pub const ROOT_DIR_MARKER: &str = "{__MRSM_ROOT_DIR__}";

/// The marker substring replaced by this release's version, as the Python package writes it
/// (PEP 440, `5.0.0rc11`), in [`DEFAULT_CONFIG_JSON`]: the stack's `api` image tag
/// (`bmeares/meerschaum:{__MRSM_VERSION__}-full`), so a root without a `stack.yaml` always runs the
/// API server of the version that wrote the compose file (`PARITY.md` W18PK-001).
pub const VERSION_MARKER: &str = "{__MRSM_VERSION__}";

/// The header written at the top of `meerschaum.yaml` (`default_header_comment`).
pub const DEFAULT_HEADER_COMMENT: &str = "
#####################################################################
#                                                                   #
#  Edit or add credentials for connectors.                          #
#  You can read more about connectors at https://meerschaum.io.     #
#                                                                   #
#  Connectors inherit from `default`, and flavor-dependent defaults #
#  are defined for SQL connectors (e.g. port 5432 for PostgreSQL).  #
#                                                                   #
#####################################################################

";

/// The supported config file types, in Python's loader order.
const FILETYPES: [&str; 3] = ["yml", "yaml", "json"];

/// Why a config file could not be loaded: the exception `read_config` would raise.
#[derive(Debug, Clone, PartialEq)]
pub enum LoadError {
    /// A PyYAML error (its class and `str()`).
    Yaml(PyYamlError),
    /// A `json.JSONDecodeError`.
    Json(JsonDecodeError),
    /// A `KeyError` (a file type without a loader): the missing key.
    Key(String),
    /// A file that is not valid UTF-8 (`UnicodeDecodeError`); the text is `str(exception)`.
    Decode(String),
}

impl LoadError {
    /// `str(exception)`.
    pub fn message(&self) -> String {
        match self {
            LoadError::Yaml(e) => e.message.clone(),
            LoadError::Json(e) => e.message.clone(),
            LoadError::Key(key) => key_error_text(key),
            LoadError::Decode(m) => m.clone(),
        }
    }
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

/// Errors raised while building the config registry.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// An `MRSM_*` path variable points to a missing or invalid path.
    #[error("Invalid path '{path}' set for environment variable '{var}'.")]
    InvalidPath {
        /// The environment variable.
        var: String,
        /// The offending value.
        path: String,
    },
    /// A config file could not be read or parsed.
    #[error("Unable to parse '{file}': {message}")]
    Parse {
        /// The file name.
        file: String,
        /// The parser's message.
        message: String,
    },
    /// A config file could not be parsed (`raise_parsing_errors`): the exception, with its class.
    #[error("{error}")]
    Load {
        /// The file name.
        file: String,
        /// The exception.
        error: LoadError,
    },
    /// An I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// The default-config paths v5 adds to v4.1.4's `default_config` (see [`default_config`]).
pub const V5_ONLY_DEFAULT_PATHS: &[&[&str]] = &[&["system", "engine"]];

/// Whether `keys` names a v5-only default (a path under [`V5_ONLY_DEFAULT_PATHS`]).
pub fn is_v5_only_default(keys: &[&str]) -> bool {
    V5_ONLY_DEFAULT_PATHS
        .iter()
        .any(|path| keys.len() >= path.len() && keys[..path.len()] == **path)
}

/// Build the default config for a root directory (`meerschaum.config._default.default_config`).
///
/// `cpu_count` is `multiprocessing.cpu_count()`; `windows` is `platform.system() == 'Windows'`.
pub fn default_config(root_dir: &str, cpu_count: i64, windows: bool) -> Map {
    let raw = Value::from_json_str(DEFAULT_CONFIG_JSON).expect("embedded default_config.json");
    match resolve_default_markers(raw, root_dir, cpu_count, windows) {
        Value::Map(mut m) => {
            if let Some(Value::Map(system)) = m.get_mut("system") {
                system.insert(
                    "engine".into(),
                    Value::Map(Map::from([
                        ("python_workers".into(), Value::Int(cpu_count.clamp(1, 4))),
                        ("inclusive_cap".into(), Value::Bool(true)),
                        ("flat_ledger".into(), Value::Bool(true)),
                    ])),
                );
            }
            m
        }
        _ => Map::new(),
    }
}

fn resolve_default_markers(value: Value, root: &str, cpu: i64, windows: bool) -> Value {
    match value {
        Value::Map(m) => {
            if m.len() == 1
                && let Some(Value::Str(marker)) = m.get("__mrsm_dynamic__")
            {
                return match marker.as_str() {
                    "cpu_count*3" => Value::Int(cpu * 3),
                    "max(cpu_count/2,1)" => Value::Int(std::cmp::max(cpu / 2, 1)),
                    "not_windows" => Value::Bool(!windows),
                    _ => Value::Null,
                };
            }
            Value::Map(
                m.into_iter()
                    .map(|(k, v)| (k, resolve_default_markers(v, root, cpu, windows)))
                    .collect(),
            )
        }
        Value::List(l) => Value::List(
            l.into_iter()
                .map(|v| resolve_default_markers(v, root, cpu, windows))
                .collect(),
        ),
        Value::Str(s) if s.contains(ROOT_DIR_MARKER) || s.contains(VERSION_MARKER) => Value::Str(
            s.replace(ROOT_DIR_MARKER, root)
                .replace(VERSION_MARKER, &crate::python_guard::expected_version()),
        ),
        other => other,
    }
}

/// Port of `revert_symlinks_config` over lossless maps: re-apply the `_symlinks` originals onto
/// a copy of the config and drop the `_symlinks` key.
pub fn revert_symlinks_lossless(config: &ConfigMap) -> ConfigMap {
    let symlinks_key = &static_config().config.symlinks_key;
    let Some(symlinks) = config.get(symlinks_key) else {
        return config.clone();
    };
    let mut reverted = config.clone();
    reverted.shift_remove(symlinks_key);
    fn deep_patch(target: &mut ConfigMap, patch: &ConfigValue) {
        let ConfigValue::Map(patch) = patch else {
            return;
        };
        for (key, value) in patch {
            match (value, target.get_mut(key)) {
                (ConfigValue::Map(_), Some(ConfigValue::Map(inner))) => deep_patch(inner, value),
                _ => {
                    target.insert(key.clone(), value.clone());
                }
            }
        }
    }
    deep_patch(&mut reverted, symlinks);
    reverted
}

/// Port of `revert_symlinks_config`: re-apply the `_symlinks` originals onto a copy of the config
/// and drop the `_symlinks` key.
pub fn revert_symlinks_config(config: &Map) -> Map {
    ConfigValue::map_to_value(&revert_symlinks_lossless(&ConfigValue::map_from_value(
        config,
    )))
}

/// Options for [`Config::get_with`], matching `get_config`'s keyword arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GetOptions {
    /// `patch`: persist defaults when the key's file is missing.
    pub patch: bool,
    /// `substitute`: resolve `MRSM{}` symlinks.
    pub substitute: bool,
    /// `write_missing`: write default files for missing keys.
    pub write_missing: bool,
    /// `warn`: record a warning for invalid keys.
    pub warn: bool,
}

impl Default for GetOptions {
    fn default() -> Self {
        GetOptions {
            patch: true,
            substitute: true,
            write_missing: true,
            warn: true,
        }
    }
}

/// How to construct a [`Config`].
#[derive(Debug, Clone)]
pub struct ConfigOptions {
    /// The environment variables, in process order (`os.environ`).
    pub env: Vec<(String, String)>,
    /// The user's home directory (`Path.home()`).
    pub home: PathBuf,
    /// Whether to use the Windows defaults.
    pub windows: bool,
    /// `PACKAGE_ROOT_PATH`, if known.
    pub package_root: Option<PathBuf>,
    /// `multiprocessing.cpu_count()`.
    pub cpu_count: i64,
    /// Whether the registry may write files and delete patch directories
    /// (Python's `_allow_write_missing`).
    pub allow_writes: bool,
}

impl ConfigOptions {
    /// Options from the current process (environment, home directory, CPU count).
    pub fn from_process() -> ConfigOptions {
        let env: Vec<(String, String)> = crate::process_env::vars();
        let home = env
            .iter()
            .find(|(k, _)| k == "HOME" || k == "USERPROFILE")
            .map(|(_, v)| PathBuf::from(v))
            .unwrap_or_else(|| PathBuf::from("/"));
        ConfigOptions {
            env,
            home,
            windows: cfg!(windows),
            package_root: None,
            cpu_count: crate::static_config::os_cpu_count().unwrap_or(1),
            allow_writes: true,
        }
    }

    /// Options with an explicit environment (other fields from the current process).
    pub fn with_env(env: Vec<(String, String)>) -> ConfigOptions {
        let mut opts = ConfigOptions::from_process();
        opts.env = env;
        opts
    }

    fn lookup(&self, key: &str) -> Option<String> {
        self.env
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    }
}

/// One message the registry would have printed or warned, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Notice {
    /// v4 `print(text)` (stdout). Rust callers also see it in [`Config::take_warnings`].
    Print(String),
    /// v4 `print(text)` that Rust callers never saw (they are not in `take_warnings`).
    PrintOnly(String),
    /// v4 `warn(text, stack=False)`.
    Warn(String),
    /// v4 `warn(text, stacklevel=3, color=False)` (the `get_config` invalid-keys warning).
    WarnPlain(String),
    /// v4 `if debug: dprint(text)`. Recorded always; the Python boundary prints it only when the
    /// caller passed `debug=True`, Rust callers never see it in [`Config::take_warnings`].
    Debug(String),
}

impl Notice {
    /// The message text.
    pub fn text(&self) -> &str {
        match self {
            Notice::Print(t)
            | Notice::PrintOnly(t)
            | Notice::Warn(t)
            | Notice::WarnPlain(t)
            | Notice::Debug(t) => t,
        }
    }
}

/// The mutable registry state (Python's module-level `config` dictionary and friends).
#[derive(Debug, Clone)]
pub struct ConfigState {
    paths: Paths,
    defaults: ConfigMap,
    config: ConfigMap,
    allow_writes: bool,
    notices: Vec<Notice>,
    write_error: Option<(std::io::ErrorKind, String)>,
}

/// The keyword arguments of `search_and_substitute_config`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubstituteOptions {
    /// `leading_key`: the string with which a symlink starts (`MRSM`).
    pub leading_key: String,
    /// `delimiter`: separates the keys of a symlink (`:`).
    pub delimiter: String,
    /// `begin_key`: starts the keys list (`{`).
    pub begin_key: String,
    /// `end_key`: ends the keys list (`}`).
    pub end_key: String,
    /// `literal_key`: forces a literal interpretation of a value (`!`).
    pub literal_key: String,
    /// `keep_symlinks`: record the symlinks under `_symlinks`.
    pub keep_symlinks: bool,
}

impl Default for SubstituteOptions {
    fn default() -> Self {
        SubstituteOptions {
            leading_key: "MRSM".to_string(),
            delimiter: ":".to_string(),
            begin_key: "{".to_string(),
            end_key: "}".to_string(),
            literal_key: "!".to_string(),
            keep_symlinks: true,
        }
    }
}

impl SubstituteOptions {
    /// The text that starts every symlink (`MRSM{`).
    pub fn needle(&self) -> String {
        format!("{}{}", self.leading_key, self.begin_key)
    }
}

/// The thread-safe config registry.
#[derive(Debug)]
pub struct Config {
    state: Mutex<ConfigState>,
}

/// The snapshot of the host process's live config tree (the Python extension: the dictionary
/// behind `meerschaum.config.config`, which `set_config()` and callers mutate in place), taken by
/// the host *with its interpreter lock held* at the start of each native call. See
/// [`Config::current`].
static LIVE_SNAPSHOT: std::sync::RwLock<Option<std::sync::Arc<Config>>> =
    std::sync::RwLock::new(None);

/// Publish the host's snapshot for the native calls that follow (the latest wins).
pub fn set_live_snapshot(config: std::sync::Arc<Config>) {
    *LIVE_SNAPSHOT.write().unwrap_or_else(|e| e.into_inner()) = Some(config);
}

impl Config {
    /// Build the registry from the current process environment.
    pub fn from_env() -> Result<Config, ConfigError> {
        Config::new(ConfigOptions::from_process())
    }

    /// The config a running sync reads (`MRSM{}` symlinks, defaults): the host's published
    /// snapshot of its live config tree ([`set_live_snapshot`], so a `set_config()` /
    /// `MRSM_CONFIG` patch / `write_config()` made in the process is seen by the next call), else
    /// the config the environment describes ([`Config::from_env`]). Never calls into the host:
    /// it is safe from any thread, including a runtime thread while the host's lock is released.
    pub fn current() -> Result<std::sync::Arc<Config>, ConfigError> {
        if let Some(config) = LIVE_SNAPSHOT
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            return Ok(config.clone());
        }
        Ok(std::sync::Arc::new(Config::from_env()?))
    }

    /// Build the registry, applying the environment connectors, `MRSM_CONFIG` / `MRSM_PATCH`,
    /// and the patch directories, like importing `meerschaum.config`.
    pub fn new(opts: ConfigOptions) -> Result<Config, ConfigError> {
        let lookup = |k: &str| opts.lookup(k);
        let paths = Paths::from_env_with_writes(
            &lookup,
            &opts.home,
            opts.windows,
            opts.package_root.clone(),
            opts.allow_writes,
        )?;
        let defaults = ConfigValue::map_from_value(&default_config(
            &paths::posix(&paths.root_dir),
            opts.cpu_count,
            opts.windows,
        ));
        let mut state = ConfigState {
            paths,
            defaults,
            config: ConfigMap::new(),
            allow_writes: opts.allow_writes,
            notices: Vec::new(),
            write_error: None,
        };
        state.apply_environment_uris(&opts.env, false);
        state.apply_environment_patches(&opts.env);
        state.apply_patch_dirs();
        Ok(Config {
            state: Mutex::new(state),
        })
    }

    /// A registry for a host that supplies its own paths and keeps the config itself (the
    /// Python boundary): nothing is applied at construction (no environment layers, no patch
    /// directories) and the config starts empty.
    pub fn bare(paths: Paths, cpu_count: i64, windows: bool, allow_writes: bool) -> Config {
        let defaults = ConfigValue::map_from_value(&default_config(
            &paths::posix(&paths.root_dir),
            cpu_count,
            windows,
        ));
        Config {
            state: Mutex::new(ConfigState {
                paths,
                defaults,
                config: ConfigMap::new(),
                allow_writes,
                notices: Vec::new(),
                write_error: None,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, ConfigState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `get_config(*keys)`: the value at the key path, or `None` if the keys are invalid.
    pub fn get(&self, keys: &[&str]) -> Option<Value> {
        let (valid, value) = self.get_with(keys, GetOptions::default());
        valid.then_some(value)
    }

    /// `get_config(*keys, as_tuple=True, ...)`: `(valid, value)`.
    pub fn get_with(&self, keys: &[&str], opts: GetOptions) -> (bool, Value) {
        let (valid, value) = self.get_with_lossless(keys, opts);
        (valid, value.to_value())
    }

    /// [`get_with`](Self::get_with) without the JSON projection.
    pub fn get_with_lossless(&self, keys: &[&str], opts: GetOptions) -> (bool, ConfigValue) {
        let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        self.lock().get_config_lossless(&keys, opts)
    }

    /// `get_plugin_config(*keys)` for a named plugin (`plugins:<name>:...`, without warnings).
    pub fn get_plugin_config(&self, plugin: &str, keys: &[&str]) -> Option<Value> {
        let mut full = vec!["plugins", plugin];
        full.extend_from_slice(keys);
        let opts = GetOptions {
            warn: false,
            ..GetOptions::default()
        };
        let (valid, value) = self.get_with(&full, opts);
        valid.then_some(value)
    }

    /// `set_config(cf)`: replace the in-memory config.
    pub fn set_config(&self, config: Map) {
        self.lock().config = ConfigValue::map_from_value(&config);
    }

    /// [`set_config`](Self::set_config) with a lossless config.
    pub fn set_config_lossless(&self, config: ConfigMap) {
        self.lock().config = config;
    }

    /// Apply connector variables from an explicit environment, returning the number applied.
    ///
    /// Existing connector attributes are patched by the environment. Other top-level keys
    /// remain available as `MRSM{}` symlink targets. Unlike registry initialization (which
    /// mirrors Python's whole-config replacement), this supports an already loaded config.
    /// Invalid connector JSON records a warning and falls back to a literal `uri` attribute.
    pub fn apply_environment_uris(&self, env: &[(String, String)]) -> usize {
        self.lock().apply_environment_uris(env, true)
    }

    /// Apply `MRSM_CONFIG`, then `MRSM_PATCH`, from an explicit environment.
    ///
    /// Invalid patches record warnings and are skipped. Callers replacing `MRSM_CONFIG`
    /// with their own config block may omit that variable from `env`. Paths and the stored
    /// registry options are unaffected; file loading still follows this registry's paths.
    pub fn apply_environment_patches(&self, env: &[(String, String)]) {
        self.lock().apply_environment_patches(env);
    }

    /// A copy of the in-memory config (`meerschaum.config.config`).
    pub fn snapshot(&self) -> Map {
        ConfigValue::map_to_value(&self.lock().config)
    }

    /// A lossless copy of the in-memory config.
    pub fn snapshot_lossless(&self) -> ConfigMap {
        self.lock().config.clone()
    }

    /// The resolved paths.
    pub fn paths(&self) -> Paths {
        self.lock().paths.clone()
    }

    /// The default config for this root.
    pub fn defaults(&self) -> Map {
        ConfigValue::map_to_value(&self.lock().defaults)
    }

    /// `search_and_substitute_config(config)`.
    pub fn search_and_substitute(&self, config: &Map) -> Result<Map, String> {
        self.lock().search_and_substitute(config)
    }

    /// `search_and_substitute_config(config)` over a lossless config.
    pub fn search_and_substitute_lossless(&self, config: &ConfigMap) -> Result<ConfigMap, String> {
        self.lock().search_and_substitute_lossless(config)
    }

    /// `write_config(config_dict, directory)`.
    pub fn write_config(&self, config: Option<&Map>, directory: Option<&Path>) -> bool {
        self.lock().write_config(config, directory)
    }

    /// Drain the warnings recorded so far (Python emits them with `warn()`).
    ///
    /// The messages v4 `print()`s are included, except those Rust callers never saw
    /// ([`Notice::PrintOnly`]).
    pub fn take_warnings(&self) -> Vec<String> {
        self.lock().take_warnings()
    }

    /// Drain every [`Notice`] recorded so far, in order.
    pub fn take_notices(&self) -> Vec<Notice> {
        self.lock().take_notices()
    }

    /// Run a closure with exclusive access to the state.
    pub fn with_state<R>(&self, f: impl FnOnce(&mut ConfigState) -> R) -> R {
        f(&mut self.lock())
    }
}

/// The Rust form of `STATIC_CONFIG['environment']['uri_regex']`, anchored like `re.match`.
static URI_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    let py = &static_config().environment.uri_regex;
    // Python treats the `-` after the `0-9` range literally; Rust needs it escaped.
    let rust = format!("^{}", py.replace("0-9-_", "0-9\\-_"));
    Regex::new(&rust).expect("valid connector URI regex")
});

/// Port of `get_connector_env_vars`: the `MRSM_<TYPE>_<LABEL>` variable names in `env`.
pub fn get_connector_env_vars(env: &[(String, String)]) -> Vec<String> {
    let reserved: Vec<String> = static_config()
        .tree
        .get("environment")
        .and_then(Value::as_map)
        .map(|m| {
            m.values()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    env.iter()
        .filter(|(k, _)| URI_REGEX.is_match(k) && !reserved.contains(k))
        .map(|(k, _)| k.clone())
        .collect()
}

/// Split a connector variable into `(type, label)` (lowercased), as `apply_connector_uri` does.
pub fn parse_connector_env_var(var: &str) -> Option<(String, String)> {
    let caps = URI_REGEX.captures(var)?;
    Some((caps[1].to_lowercase(), caps[2].to_lowercase()))
}

/// The file names `read_config` chose to parse for a request, with what v4 would have printed.
struct FileSelection {
    /// The file to parse for each key, in order.
    filekeys: Vec<String>,
    /// Requested keys with no file.
    missing: Vec<String>,
    /// `(stem, chosen file)` for every stem with more than one candidate.
    duplicates: Vec<(String, String)>,
}

/// `os.path.splitext(name)[0]`.
/// Replace `path` with `bytes` without ever leaving it truncated, partial or deleted: the text
/// goes to a temporary file in the same directory (created with the replaced file's mode, owner
/// and group where permitted), is synced, and is renamed over the target. A failure removes only
/// the temporary file. Extended attributes, ACLs and SELinux labels are not copied (the new
/// file gets the directory's defaults; PARITY.md W15O-2). Where a temporary file cannot be used
/// (a bind-mounted single file, a writable file in a read-only directory) the file is rewritten
/// in place, as v4 does.
///
/// PARITY: v4 opens the file with `open(path, 'w+')`, so a read-only file raises
/// `PermissionError` and is left intact; the same check is made here (a write-open without
/// truncation) because a rename would otherwise replace a read-only file.
fn write_file_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    write_file_with(path, bytes, &|from, to| std::fs::rename(from, to))
}

/// Whether a failure to create the temporary file or to rename it over the target is one the
/// in-place rewrite can get around: `EPERM`, `EACCES` (a writable file in a directory the
/// process cannot write), `EBUSY` (a bind-mounted single file cannot be replaced) and `EXDEV`.
fn needs_in_place_write(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(1 | 13 | 16 | 18))
        || error.kind() == std::io::ErrorKind::PermissionDenied
}

/// Rewrite an existing, writable file in place (`open(path, 'w+')`: the inode, mode, owner and
/// attributes stay; not atomic).
fn write_in_place(target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new().write(true).open(target)?;
    file.set_len(0)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// [`write_file_atomically`] with the rename injectable (tests simulate `EBUSY`).
///
/// When the temporary file cannot be created or renamed for one of the reasons in
/// [`needs_in_place_write`] and the target exists (the write-probe above proved it writable), the
/// file is rewritten in place, as v4 does, and any temporary file is removed.
fn write_file_with(
    path: &Path,
    bytes: &[u8],
    rename: &dyn Fn(&Path, &Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    // Write through a symlink to the file it points at, as `open()` does.
    let target = if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        std::fs::canonicalize(path)?
    } else {
        path.to_path_buf()
    };
    let existing = std::fs::metadata(&target).ok();
    if existing.is_some() {
        std::fs::OpenOptions::new().write(true).open(&target)?;
    }
    let dir = target.parent().map(Path::to_path_buf).unwrap_or_default();
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let temp = dir.join(format!(
        ".{name}.{}-{}.tmp",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        // The temporary file is created with the target's mode (0600 when there is no target),
        // before a byte of the config is written: it is never readable by more than the file it
        // replaces. The umask can only clear bits, so the exact mode is restored right after.
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
            options.mode(existing.as_ref().map_or(0o600, |m| m.mode() & 0o7777));
        }
        let mut file = options.open(&temp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if let Some(meta) = &existing {
                file.set_permissions(std::fs::Permissions::from_mode(meta.mode() & 0o7777))?;
                // Keep the owner and group where the process may (best effort: only root can
                // give a file away, and a member can keep a group it belongs to).
                let _ = std::os::unix::fs::fchown(&file, Some(meta.uid()), Some(meta.gid()));
            } else {
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
        }
        #[cfg(not(unix))]
        if let Some(meta) = &existing {
            file.set_permissions(meta.permissions())?;
        }
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        rename(&temp, &target)
    })();
    if let Err(error) = result {
        let _ = std::fs::remove_file(&temp);
        if existing.is_some() && needs_in_place_write(&error) {
            return write_in_place(&target, bytes);
        }
        return Err(error);
    }
    Ok(())
}

fn stem_of(name: &str) -> String {
    match name.rfind('.') {
        Some(i) if i > 0 => name[..i].to_string(),
        _ => name.to_string(),
    }
}

/// The text of a `KeyError` for `key` (`str(KeyError('txt'))`).
fn key_error_text(key: &str) -> String {
    str_repr(key)
}

/// `UnicodeDecodeError`'s text for a config file that is not UTF-8.
fn decode_error_text(bytes: &[u8], err: &std::str::Utf8Error) -> String {
    let pos = err.valid_up_to();
    let byte = bytes[pos];
    let reason = if err.error_len().is_none() {
        "unexpected end of data"
    } else if (0x80..=0xBF).contains(&byte) || byte >= 0xF5 || byte == 0xC0 || byte == 0xC1 {
        "invalid start byte"
    } else {
        "invalid continuation byte"
    };
    format!("'utf-8' codec can't decode byte 0x{byte:02x} in position {pos}: {reason}")
}

impl ConfigState {
    fn symlinks_key() -> &'static str {
        &static_config().config.symlinks_key
    }

    fn notice(&mut self, notice: Notice) {
        self.notices.push(notice);
    }

    fn warn(&mut self, msg: impl Into<String>) {
        self.notice(Notice::Warn(msg.into()));
    }

    fn debug(&mut self, msg: impl Into<String>) {
        self.notice(Notice::Debug(msg.into()));
    }

    fn print(&mut self, msg: impl Into<String>) {
        self.notice(Notice::Print(msg.into()));
    }

    /// Drain the warnings (see [`Config::take_warnings`]).
    pub fn take_warnings(&mut self) -> Vec<String> {
        std::mem::take(&mut self.notices)
            .into_iter()
            .filter_map(|n| match n {
                Notice::PrintOnly(_) | Notice::Debug(_) => None,
                Notice::Print(t) | Notice::Warn(t) | Notice::WarnPlain(t) => Some(t),
            })
            .collect()
    }

    /// Drain every notice, in order.
    pub fn take_notices(&mut self) -> Vec<Notice> {
        std::mem::take(&mut self.notices)
    }

    /// The resolved paths.
    pub fn paths(&self) -> &Paths {
        &self.paths
    }

    /// The resolved paths, mutably (a host whose root moves after the registry was built).
    pub fn paths_mut(&mut self) -> &mut Paths {
        &mut self.paths
    }

    /// Whether the registry may write files (`_allow_write_missing`).
    pub fn allow_writes(&self) -> bool {
        self.allow_writes
    }

    /// Set whether the registry may write files (`_allow_write_missing`).
    pub fn set_allow_writes(&mut self, allow: bool) {
        self.allow_writes = allow;
    }

    /// The in-memory config, projected into the JSON-like [`Map`].
    pub fn config(&self) -> Map {
        ConfigValue::map_to_value(&self.config)
    }

    /// The in-memory config.
    pub fn config_lossless(&self) -> &ConfigMap {
        &self.config
    }

    /// Replace the in-memory config (`set_config`).
    pub fn set_config_lossless(&mut self, config: ConfigMap) {
        self.config = config;
    }

    /// Take the in-memory config out of the state, leaving it empty.
    pub fn take_config_lossless(&mut self) -> ConfigMap {
        std::mem::take(&mut self.config)
    }

    /// The default config.
    pub fn defaults_lossless(&self) -> &ConfigMap {
        &self.defaults
    }

    /// Port of `get_config(*keys, ...)` returning `(valid, value)` as the JSON-like projection.
    pub fn get_config(&mut self, keys: &[String], opts: GetOptions) -> (bool, Value) {
        let (valid, value) = self.get_config_lossless(keys, opts);
        (valid, value.to_value())
    }

    /// Port of `get_config(*keys, ...)` returning `(valid, value)`.
    pub fn get_config_lossless(
        &mut self,
        keys: &[String],
        opts: GetOptions,
    ) -> (bool, ConfigValue) {
        let symlinks_key = Self::symlinks_key();
        if keys.is_empty() {
            return (true, ConfigValue::Map(self.config.clone()));
        }
        let k0 = keys[0].clone();

        // Re-substitute a key that was loaded earlier without substitution.
        if opts.substitute
            && k0 != symlinks_key
            && self
                .config
                .get(&k0)
                .is_some_and(|c| c.contains_text("MRSM{"))
        {
            let current = self.config.get(&k0).cloned().unwrap_or_default();
            let mut single = ConfigMap::new();
            single.insert(k0.clone(), current);
            match self.search_and_substitute_lossless(&single) {
                Ok(subbed) => {
                    self.config.insert(
                        k0.clone(),
                        subbed.get(&k0).cloned().unwrap_or(ConfigValue::Null),
                    );
                    if let Some(sub_links) = subbed.get(symlinks_key) {
                        let existing = self
                            .config
                            .get(symlinks_key)
                            .cloned()
                            .unwrap_or_else(ConfigValue::new_map);
                        let merged = apply_patch(sub_links, &existing);
                        self.config.insert(symlinks_key.to_string(), merged);
                    }
                }
                Err(e) => self.warn(e),
            }
        }

        let mut invalid_keys = false;
        if !self.config.contains_key(&k0) && k0 != symlinks_key {
            let single = self
                .read_config_lossless(
                    None,
                    Some(std::slice::from_ref(&k0)),
                    opts.write_missing,
                    opts.substitute,
                    false,
                )
                .map(|(c, _)| c)
                .unwrap_or_default();
            match single.get(&k0) {
                None => invalid_keys = true,
                Some(v) => {
                    self.config.insert(k0.clone(), v.clone());
                    if let Some(link) = single.get(symlinks_key).and_then(|s| s.get(&k0)) {
                        let links = self
                            .config
                            .entry(symlinks_key.to_string())
                            .or_insert_with(ConfigValue::new_map);
                        if let Some(m) = links.as_map_mut() {
                            m.insert(k0.clone(), link.clone());
                        }
                    }
                }
            }
        }

        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let mut found: Option<ConfigValue> = None;
        if !invalid_keys {
            let mut current: Option<&ConfigValue> = None;
            for (i, k) in key_refs.iter().enumerate() {
                current = if i == 0 {
                    self.config.get(*k)
                } else {
                    current.and_then(|c| c.get(k))
                };
                if current.is_none() {
                    break;
                }
            }
            found = current.cloned();
        }
        if let Some(value) = found {
            return (true, value);
        }

        // Check whether the keys are in the default configuration.
        let patched_default = if opts.substitute {
            let defaults = self.defaults.clone();
            match self.search_and_substitute_lossless(&defaults) {
                Ok(m) => m,
                Err(e) => {
                    self.warn(e);
                    defaults
                }
            }
        } else {
            self.defaults.clone()
        };
        let in_default = patched_default
            .get(key_refs[0])
            .and_then(|v| v.get_path(&key_refs[1..]))
            .cloned();
        let Some(default_value) = in_default else {
            if opts.warn {
                let tuple = Value::List(keys.iter().map(|k| Value::Str(k.clone())).collect());
                let repr = py_repr(&tuple);
                let repr = format!(
                    "({}{})",
                    &repr[1..repr.len() - 1],
                    if keys.len() == 1 { "," } else { "" }
                );
                self.notice(Notice::WarnPlain(format!("Invalid keys in config: {repr}")));
            }
            return (false, ConfigValue::Null);
        };

        // v5-only defaults (`system:engine`, which v4.1.4's `default_config` lacks): a v4-written
        // `system.json` never has them, so v5's own lookups of them (the engine and worker
        // settings, read by most commands) always fall back. v4 never looks them up, so its
        // fallback below (deep-merging EVERY default into every loaded key) never runs for them:
        // answer with the default and leave the loaded config alone, or an upgraded v4 root
        // would get defaults filled into keys that are present (e.g. `start_period` and a
        // deliberately removed `retries` in `stack.yaml`'s compose section; PARITY.md W17K1-006).
        if is_v5_only_default(&key_refs) {
            return (true, default_value);
        }

        // Don't write keys that we haven't yet loaded into memory.
        let mut trimmed = patched_default;
        trimmed.retain(|k, _| self.config.contains_key(k));
        self.config = apply_patch_maps(&trimmed, &self.config);

        if opts.patch && k0 != symlinks_key && opts.write_missing {
            let keyfile_exists = self.get_keyfile_path(&k0, false, None).is_some();
            if !keyfile_exists {
                let cfg = self.config.clone();
                self.write_config_lossless(Some(&cfg), None);
            }
        }

        (true, default_value)
    }

    /// The file names in a config directory (sorted, for determinism).
    fn list_dir(&self, directory: &Path) -> Vec<String> {
        if !directory.exists() && self.allow_writes {
            let _ = std::fs::create_dir_all(directory);
        }
        let mut names: Vec<String> = std::fs::read_dir(directory)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    /// Choose which files to parse for the requested keys (`read_config`'s filekey logic).
    fn select_filekeys(filenames: &[String], keys: Option<&[String]>) -> FileSelection {
        let mut missing: Vec<String> = Vec::new();
        let mut found: Vec<String> = Vec::new();
        let candidates: Vec<String> = match keys {
            None => filenames.to_vec(),
            Some(keys) => {
                let mut out = Vec::new();
                for k in keys {
                    for ft in FILETYPES {
                        let name = format!("{k}.{ft}");
                        if filenames.contains(&name) {
                            out.push(name);
                            if !found.contains(k) {
                                found.push(k.clone());
                            }
                            missing.retain(|m| m != k);
                        } else if !found.contains(k) && !missing.contains(k) {
                            missing.push(k.clone());
                        }
                    }
                }
                out
            }
        };

        // Group by stem (stable sort), preferring the default filetype when duplicated.
        let mut sorted = candidates;
        sorted.sort_by_key(|f| stem_of(f));
        let default_ft = &static_config().config.default_filetype;
        let mut filekeys = Vec::new();
        let mut duplicates = Vec::new();
        let mut i = 0;
        while i < sorted.len() {
            let s = stem_of(&sorted[i]);
            let mut group = vec![sorted[i].clone()];
            let mut j = i + 1;
            while j < sorted.len() && stem_of(&sorted[j]) == s {
                group.push(sorted[j].clone());
                j += 1;
            }
            let preferred = format!("{s}.{default_ft}");
            let chosen = if group.len() > 1 && group.contains(&preferred) {
                preferred
            } else {
                group[0].clone()
            };
            if group.len() > 1 {
                duplicates.push((s.clone(), chosen.clone()));
            }
            filekeys.push(chosen);
            i = j;
        }
        FileSelection {
            filekeys,
            missing,
            duplicates,
        }
    }

    /// Port of `read_config` as the JSON-like projection (see [`read_config_lossless`]).
    ///
    /// [`read_config_lossless`]: Self::read_config_lossless
    pub fn read_config(
        &mut self,
        directory: Option<&Path>,
        keys: Option<&[String]>,
        write_missing: bool,
        substitute: bool,
        raise_parsing_errors: bool,
    ) -> Result<(Map, Vec<String>), ConfigError> {
        let (config, names) = self.read_config_lossless(
            directory,
            keys,
            write_missing,
            substitute,
            raise_parsing_errors,
        )?;
        Ok((ConfigValue::map_to_value(&config), names))
    }

    /// Port of `read_config`. Returns the config read from `directory` (default
    /// `CONFIG_DIR_PATH`) and the file names that were parsed.
    ///
    /// PARITY: a file whose extension has no loader (`foo.txt`) is announced as unknown and then
    /// still "parsed": v4 does not skip it, the missing loader is a `KeyError` that becomes the
    /// parse warning, and the key gets its defaults (`_read_config.py:178-194`).
    pub fn read_config_lossless(
        &mut self,
        directory: Option<&Path>,
        keys: Option<&[String]>,
        write_missing: bool,
        substitute: bool,
        raise_parsing_errors: bool,
    ) -> Result<(ConfigMap, Vec<String>), ConfigError> {
        let symlinks_key = Self::symlinks_key();
        let directory: PathBuf = directory
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.paths.config_dir.clone());
        let filenames = self.list_dir(&directory);
        let FileSelection {
            filekeys,
            missing: missing_keys,
            duplicates,
        } = Self::select_filekeys(&filenames, keys);

        let mut config = ConfigMap::new();
        let mut config_to_write = ConfigMap::new();

        for mk in &missing_keys {
            if !self.defaults.contains_key(mk) {
                continue;
            }
            let default_dict = if substitute {
                let defaults = self.defaults.clone();
                match self.search_and_substitute_lossless(&defaults) {
                    Ok(m) => m,
                    Err(e) => {
                        self.warn(e);
                        defaults
                    }
                }
            } else {
                self.defaults.clone()
            };
            let default_symlinks = default_dict
                .get(symlinks_key)
                .and_then(|s| s.get(mk))
                .cloned()
                .unwrap_or_else(ConfigValue::new_map);
            let value = default_dict.get(mk).cloned().unwrap_or(ConfigValue::Null);
            config.insert(mk.clone(), value.clone());
            if default_symlinks.truthy() {
                for target in [&mut config, &mut config_to_write] {
                    let links = target
                        .entry(symlinks_key.to_string())
                        .or_insert_with(ConfigValue::new_map);
                    if let Some(m) = links.as_map_mut() {
                        let existing = m.get(mk).cloned().unwrap_or_else(ConfigValue::new_map);
                        m.insert(mk.clone(), apply_patch(&existing, &default_symlinks));
                    }
                }
            }
            config_to_write.insert(mk.clone(), value);
        }

        if !config_to_write.is_empty() && write_missing {
            self.write_config_lossless(Some(&config_to_write), Some(&directory));
        }

        for (stem, chosen) in &duplicates {
            self.notice(Notice::PrintOnly(format!(
                "Found multiple config files named '{stem}'. Will attempt to parse '{chosen}' for key '{stem}'."
            )));
        }

        for filename in &filekeys {
            let filepath = directory.join(filename);
            let parts: Vec<&str> = filename.split('.').collect();
            let filetype = parts[parts.len() - 1].to_string();
            let key = parts[..parts.len() - 1].join(".");
            if parts.len() < 2 || !FILETYPES.contains(&filetype.as_str()) {
                self.print(format!(
                    "Unknown file '{filename}' in '{}'. Skipping...",
                    paths::posix(&directory)
                ));
            }
            let bytes = std::fs::read(&filepath)?;
            let parsed: Result<ConfigValue, LoadError> = if !FILETYPES.contains(&filetype.as_str())
            {
                Err(LoadError::Key(filetype.clone()))
            } else {
                match std::str::from_utf8(&bytes) {
                    Err(e) => Err(LoadError::Decode(decode_error_text(&bytes, &e))),
                    Ok(text) if filetype == "json" => json_loads(text).map_err(LoadError::Json),
                    Ok(text) => load_text(
                        text,
                        &paths::posix(&filepath),
                        false,
                        LoadOptions {
                            safe: false,
                            project: false,
                        },
                    )
                    .map(|node| ConfigValue::from_node(&node))
                    .map_err(LoadError::Yaml),
                }
            };
            let config_key = match parsed {
                Ok(v) => v,
                Err(error) => {
                    if raise_parsing_errors {
                        return Err(ConfigError::Load {
                            file: filename.clone(),
                            error,
                        });
                    }
                    self.warn(format!(
                        "Could not parse '{filename}':\n    {}\nUsing default values in memory; the file on disk is unchanged.\nFix it manually or run `edit config {key} --rollback` to restore a backup.",
                        error.message()
                    ));
                    self.defaults
                        .get(&key)
                        .cloned()
                        .unwrap_or_else(ConfigValue::new_map)
                }
            };
            let mut single = ConfigMap::new();
            single.insert(key.clone(), config_key);
            let single = if substitute {
                match self.search_and_substitute_lossless(&single) {
                    Ok(m) => m,
                    Err(e) => {
                        self.warn(e);
                        single
                    }
                }
            } else {
                single
            };
            config.insert(
                key.clone(),
                single.get(&key).cloned().unwrap_or(ConfigValue::Null),
            );
            if let Some(link) = single.get(symlinks_key).and_then(|s| s.get(&key)) {
                let links = config
                    .entry(symlinks_key.to_string())
                    .or_insert_with(ConfigValue::new_map);
                if let Some(m) = links.as_map_mut() {
                    m.insert(key.clone(), link.clone());
                }
            }
        }

        Ok((config, filekeys))
    }

    /// Port of `get_keyfile_path`.
    ///
    /// PARITY: the file is looked up in the **config directory** and only its name is joined
    /// onto `directory` (v4 calls `read_config(keys=[key], ...)` without a directory,
    /// `_read_config.py:446`), so a write into another directory keeps the filetype the key
    /// already has in the config directory. Unlike v4 it does not parse the file (no warnings
    /// for a malformed one).
    pub fn get_keyfile_path(
        &self,
        key: &str,
        create_new: bool,
        directory: Option<&Path>,
    ) -> Option<PathBuf> {
        let directory: PathBuf = directory
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.paths.config_dir.clone());
        let filenames = self.list_dir(&self.paths.config_dir);
        let selection = Self::select_filekeys(&filenames, Some(&[key.to_string()]));
        match selection.filekeys.first() {
            Some(f) => Some(directory.join(f)),
            None if create_new => {
                Some(directory.join(format!("{key}.{}", static_config().config.default_filetype)))
            }
            None => None,
        }
    }

    /// Port of `write_config` for a JSON-like config. Returns `false` when writes are disabled
    /// or a write failed.
    pub fn write_config(&mut self, config: Option<&Map>, directory: Option<&Path>) -> bool {
        let lossless = config.map(ConfigValue::map_from_value);
        self.write_config_lossless(lossless.as_ref(), directory)
    }

    /// Port of `write_config`: one file per top-level key. Returns `false` when writes are
    /// disabled or a write failed.
    pub fn write_config_lossless(
        &mut self,
        config: Option<&ConfigMap>,
        directory: Option<&Path>,
    ) -> bool {
        self.write_config_with(config, directory, &|v: &ConfigValue| {
            dump_node(
                &v.to_node(),
                &DumpOptions {
                    sort_keys: false,
                    ..DumpOptions::default()
                },
            )
            .map_err(|e| e.message)
        })
    }

    /// `write_config` with the YAML emitter injectable. When it fails for a key, that key's
    /// file is **not touched** (an emitter error must never truncate a user's config) and the
    /// write returns `false` after the messages v4 prints.
    ///
    /// PARITY: v4 opens the file for writing before dumping and removes it when the dump fails;
    /// the file is left as it was here (also for JSON, which cannot hold a date or `bytes`).
    pub fn write_config_with(
        &mut self,
        config: Option<&ConfigMap>,
        directory: Option<&Path>,
        dump_yaml: &dyn Fn(&ConfigValue) -> Result<String, String>,
    ) -> bool {
        self.write_error = None;
        if !self.allow_writes {
            return false;
        }
        let directory: PathBuf = directory
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.paths.config_dir.clone());
        let config_dict = match config {
            Some(c) => c.clone(),
            None => self.config.clone(),
        };
        let config_dict = revert_symlinks_lossless(&config_dict);
        let default_ft = static_config().config.default_filetype.clone();
        for (k, v) in &config_dict {
            let filetype = if k == "meerschaum" {
                "yaml".to_string()
            } else if let Some(ft) = v.get("filetype") {
                py_str(ft)
            } else {
                match self.get_keyfile_path(k, false, Some(&directory)) {
                    None => default_ft.clone(),
                    Some(path) => {
                        let ft = path
                            .extension()
                            .map(|e| e.to_string_lossy().to_string())
                            .unwrap_or_default();
                        if FILETYPES.contains(&ft.as_str()) {
                            ft
                        } else {
                            self.print(format!(
                                "Invalid filetype '{ft}' for '{k}'. Assuming {default_ft}..."
                            ));
                            default_ft.clone()
                        }
                    }
                }
            };
            let filepath = directory.join(format!("{k}.{filetype}"));
            if let Some(parent) = filepath.parent() {
                let _ = std::fs::create_dir(parent);
            }
            let mut text = String::new();
            if k == "meerschaum" {
                text.push_str(DEFAULT_HEADER_COMMENT);
            }
            match filetype.as_str() {
                "json" => match json_dumps(v, JsonStyle::Indent(2), true) {
                    Ok(dumped) => text.push_str(&dumped),
                    Err(e) => {
                        self.print(format!(
                            "FAILED TO WRITE!\n{e}\n{{'sort_keys': False, 'indent': 2}}"
                        ));
                        return false;
                    }
                },
                "yaml" | "yml" => match dump_yaml(v) {
                    Ok(dumped) => text.push_str(&dumped),
                    Err(e) => {
                        self.print(format!(
                            "FAILED TO WRITE! Could not dump '{k}':\n{e}\n{{'sort_keys': False, 'indent': 2}}"
                        ));
                        return false;
                    }
                },
                other => {
                    self.print(format!(
                        "FAILED TO WRITE!\n{}\n{{'sort_keys': False, 'indent': 2}}",
                        str_repr(other)
                    ));
                    // PARITY: v4 removes the file here; a config file holds credentials, so it is
                    // never deleted on a failed write.
                    return false;
                }
            }
            if let Err(e) = write_file_atomically(&filepath, text.as_bytes()) {
                self.write_error = Some((e.kind(), e.to_string()));
                return false;
            }
        }
        true
    }

    /// The I/O error the last failed [`write_config_with`](Self::write_config_with) hit, if any
    /// (v4 raises it, e.g. `PermissionError` for a read-only file). Cleared by this call.
    pub fn take_write_error(&mut self) -> Option<(std::io::ErrorKind, String)> {
        self.write_error.take()
    }

    /// Port of `search_and_substitute_config` for a JSON-like config.
    pub fn search_and_substitute(&mut self, config: &Map) -> Result<Map, String> {
        let substituted =
            self.search_and_substitute_lossless(&ConfigValue::map_from_value(config))?;
        Ok(ConfigValue::map_to_value(&substituted))
    }

    /// Port of `search_and_substitute_config` with the default arguments
    /// (`MRSM{` ... `}`, `:` delimiter, `!` literal prefix, `keep_symlinks=True`).
    pub fn search_and_substitute_lossless(
        &mut self,
        config: &ConfigMap,
    ) -> Result<ConfigMap, String> {
        self.search_and_substitute_with(config, &SubstituteOptions::default())
    }

    /// Port of `search_and_substitute_config`: replace every `MRSM{a:b:c}` in `config` with the
    /// value at those keys of the current config.
    ///
    /// Substitution rewrites a `json.dumps` text, as v4 does; a date, `bytes` or `set` that the
    /// text cannot hold rides through as a placeholder and comes back as itself (v4 raises a
    /// `TypeError` there, PARITY.md G7). Tuples become lists and non-string keys strings, as in
    /// v4, wherever a substitution happened.
    pub fn search_and_substitute_with(
        &mut self,
        config: &ConfigMap,
        options: &SubstituteOptions,
    ) -> Result<ConfigMap, String> {
        let needle = options.needle();
        let mut links: Vec<(Vec<String>, String)> = Vec::new();
        fn find_symlinks(
            d: &ConfigMap,
            prefix: &[String],
            needle: &str,
            out: &mut Vec<(Vec<String>, String)>,
        ) {
            for (k, v) in d {
                let mut path = prefix.to_vec();
                path.push(k.clone());
                match v {
                    ConfigValue::Map(m) => find_symlinks(m, &path, needle, out),
                    ConfigValue::Str(s) if s.contains(needle) => out.push((path, s.clone())),
                    _ => {}
                }
            }
        }
        if options.keep_symlinks {
            find_symlinks(config, &[], &needle, &mut links);
        }

        let mut parsed_config = if !map_contains_text(config, &needle) {
            config.clone()
        } else {
            let mut table: Vec<ConfigValue> = Vec::new();
            let mut haystack =
                json_dumps_map_lossless(config, JsonStyle::Compact, true, &mut table)?;
            let pattern_re = Regex::new(&format!(
                "{}(?P<pattern_keys>.*?){}",
                regex::escape(&needle),
                regex::escape(&options.end_key)
            ))
            .map_err(|e| e.to_string())?;
            let mut order: Vec<String> = Vec::new();
            let mut patterns: std::collections::HashMap<String, (ConfigValue, bool, bool)> =
                std::collections::HashMap::new();
            let mut memo: std::collections::HashMap<Vec<String>, (bool, ConfigValue)> =
                std::collections::HashMap::new();
            let matches: Vec<(String, String, usize, usize)> = pattern_re
                .captures_iter(&haystack)
                .filter_map(|caps| {
                    let m = caps.get(0)?;
                    Some((
                        m.as_str().to_string(),
                        caps["pattern_keys"].to_string(),
                        m.start(),
                        m.end(),
                    ))
                })
                .collect();
            for (pattern, raw_keys, start, end) in matches {
                let prior = haystack[..start].chars().next_back();
                let after = haystack[end..].chars().next();
                let mut keys: Vec<String> = raw_keys
                    .split(options.delimiter.as_str())
                    .map(String::from)
                    .collect();
                let mut force_literal = false;
                if let Some(first) = keys.first_mut()
                    && let Some(stripped) = first.strip_prefix(options.literal_key.as_str())
                {
                    *first = stripped.to_string();
                    force_literal = true;
                }
                if keys.len() == 1 && keys[0].is_empty() {
                    keys.clear();
                }
                let (valid, value) = match memo.get(&keys) {
                    Some(v) => v.clone(),
                    None => {
                        let opts = GetOptions {
                            patch: true,
                            substitute: false,
                            write_missing: false,
                            warn: true,
                        };
                        let result = self.get_config_lossless(&keys, opts);
                        memo.insert(keys.clone(), result.clone());
                        result
                    }
                };
                if !valid {
                    continue;
                }
                if !patterns.contains_key(&pattern) {
                    order.push(pattern.clone());
                }
                // PARITY: a pattern seen both quoted and inside a longer string keeps the status
                // of its last occurrence (`_read_config.py:265`).
                patterns.insert(
                    pattern,
                    (
                        value,
                        prior == Some('"') && after == Some('"'),
                        force_literal,
                    ),
                );
            }
            for pattern in order {
                let (value, isolated, literal) = &patterns[&pattern];
                if *isolated {
                    let dumped = json_dumps_lossless(value, JsonStyle::Default, true, &mut table)?;
                    haystack = haystack.replace(&json_dumps_str(&pattern, true), &dumped);
                } else if *literal {
                    // PARITY: escaping `'` as `\'` produces invalid JSON (`_read_config.py:283`).
                    let dumped = json_dumps(value, JsonStyle::Default, true)?
                        .replace('\\', "\\\\")
                        .replace('"', "\\\"")
                        .replace('\'', "\\'");
                    haystack = haystack.replace(&pattern, &dumped);
                } else {
                    haystack = haystack.replace(&pattern, &py_str(value));
                }
            }
            match restore_placeholders(json_loads(&haystack).map_err(|e| e.message)?, &table) {
                ConfigValue::Map(m) => m,
                v if !v.truthy() => ConfigMap::new(),
                _ => return Err("substituted config is not a dictionary".to_string()),
            }
        };

        if !links.is_empty() {
            let mut symlinks = ConfigMap::new();
            for (keys, pattern) in links {
                let mut s = &mut symlinks;
                for k in &keys[..keys.len() - 1] {
                    let entry = s.entry(k.clone()).or_insert_with(ConfigValue::new_map);
                    if !entry.is_dict() {
                        *entry = ConfigValue::new_map();
                    }
                    s = entry.as_map_mut().expect("map");
                }
                s.insert(keys[keys.len() - 1].clone(), ConfigValue::Str(pattern));
            }
            let symlinks_key = Self::symlinks_key();
            let merged = match parsed_config.get(symlinks_key) {
                None => ConfigValue::Map(symlinks),
                Some(existing) => apply_patch(existing, &ConfigValue::Map(symlinks)),
            };
            parsed_config.insert(symlinks_key.to_string(), merged);
        }
        Ok(parsed_config)
    }

    /// Port of `apply_environment_uris` / `apply_connector_uri` (the variables of `env`).
    ///
    /// With `preserve_keys` false the config is replaced by one holding only `meerschaum`
    /// (PARITY: `config/environment.py:166`, what importing `meerschaum.config` does); with it
    /// true the other loaded keys stay. Returns the number of variables applied.
    pub fn apply_environment_uris(
        &mut self,
        env: &[(String, String)],
        preserve_keys: bool,
    ) -> usize {
        let mut applied = 0;
        // Plugin connector types, read lazily: only a variable of a non-built-in type needs them.
        let mut plugin_types: Option<std::collections::BTreeSet<String>> = None;
        for var in get_connector_env_vars(env) {
            let Some((typ, label)) = parse_connector_env_var(&var) else {
                continue;
            };
            if typ.is_empty() || label.is_empty() {
                continue;
            }
            // PARITY: v4 read every `MRSM_<TYPE>_<LABEL>` as a connector (`MRSM_DUCKDB_LIB`
            // became `duckdb:lib`). v5 ignores the variables of a type that does not exist.
            if !connector_types::is_builtin_connector_type(&typ) {
                let known = plugin_types.get_or_insert_with(|| {
                    connector_types::plugin_connector_types(
                        &self.paths.root_dir,
                        &self.paths.plugins_dirs,
                    )
                });
                if !known.contains(&typ) {
                    self.debug(format!(
                        "Ignoring environment variable {var}: '{typ}' is not a connector type."
                    ));
                    continue;
                }
            }
            let uri = env
                .iter()
                .find(|(k, _)| k == &var)
                .map(|(_, v)| v.as_str())
                .unwrap_or_default();
            let conn_attrs = if uri.trim_start().starts_with('{') && uri.trim_end().ends_with('}') {
                match json_loads(uri) {
                    Ok(v) => v,
                    Err(_) => {
                        self.warn(format!(
                            "Unable to parse JSON for environment connector '{typ}:{label}'."
                        ));
                        uri_attrs(uri)
                    }
                }
            } else {
                uri_attrs(uri)
            };
            let (_, meerschaum) =
                self.get_config_lossless(&["meerschaum".to_string()], GetOptions::default());
            let mut base = if preserve_keys {
                self.config.clone()
            } else {
                ConfigMap::new()
            };
            base.insert("meerschaum".to_string(), meerschaum);
            let mut label_map = ConfigMap::new();
            label_map.insert(label, conn_attrs);
            let mut typ_map = ConfigMap::new();
            typ_map.insert(typ, ConfigValue::Map(label_map));
            let mut connectors = ConfigMap::new();
            connectors.insert("connectors".to_string(), ConfigValue::Map(typ_map));
            let mut patch = ConfigMap::new();
            patch.insert("meerschaum".to_string(), ConfigValue::Map(connectors));
            self.config = apply_patch_maps(&base, &patch);
            applied += 1;
        }
        applied
    }

    /// Port of `apply_environment_patches` (`MRSM_CONFIG`, then `MRSM_PATCH`).
    pub fn apply_environment_patches(&mut self, env: &[(String, String)]) {
        let env_names = &static_config().environment;
        for var in [env_names.config.clone(), env_names.patch.clone()] {
            let Some((_, raw)) = env.iter().find(|(k, _)| k == &var) else {
                continue;
            };
            let patch = match string_to_dict(raw.trim_start()) {
                Ok(p) => ConfigValue::map_from_value(&p),
                Err(_) => {
                    self.print(format!(
                        "Environment variable {var} is set but cannot be parsed.\nUnset {var} or change to JSON or simplified dictionary format (see --help, under params for formatting)\n{var} is set to:\n{raw}\nSkipping patching os environment into config..."
                    ));
                    continue;
                }
            };
            for key in patch.keys() {
                let opts = GetOptions {
                    warn: false,
                    ..GetOptions::default()
                };
                let _ = self.get_config_lossless(std::slice::from_ref(key), opts);
            }
            self.config = apply_patch_maps(&self.config, &patch);
        }
    }

    /// Apply `patch_config/` and `permanent_patch_config/` under the root directory.
    pub fn apply_patch_dirs(&mut self) {
        let patch_dir = self.paths.path("PATCH_DIR_PATH");
        let permanent_dir = self.paths.path("PERMANENT_PATCH_DIR_PATH");
        let patch_config = if patch_dir.exists() {
            self.read_config_lossless(Some(&patch_dir), None, true, true, false)
                .ok()
                .map(|(c, _)| c)
        } else {
            None
        };
        let permanent_config = if permanent_dir.exists() {
            self.read_config_lossless(Some(&permanent_dir), None, true, true, false)
                .ok()
                .map(|(c, _)| c)
        } else {
            None
        };
        if let Some(patch) = patch_config {
            self.config = apply_patch_maps(&self.config, &patch);
            if self.allow_writes {
                let _ = std::fs::remove_dir_all(&patch_dir);
            }
        }
        if let Some(permanent) = permanent_config {
            self.config = apply_patch_maps(&self.config, &permanent);
            let cfg = self.config.clone();
            self.write_config_lossless(Some(&cfg), None);
            if self.allow_writes {
                let _ = std::fs::remove_dir_all(&permanent_dir);
                let _ = std::fs::remove_dir_all(self.paths.path("DEFAULT_CONFIG_DIR_PATH"));
            }
        }
    }
}

fn uri_attrs(uri: &str) -> ConfigValue {
    let mut m = ConfigMap::new();
    m.insert("uri".to_string(), ConfigValue::Str(uri.to_string()));
    ConfigValue::Map(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_env_vars() {
        let env: Vec<(String, String)> = [
            ("MRSM_SQL_FOO", "sqlite://"),
            ("MRSM_ROOT_DIR", "/x"),
            ("MRSM_SQL_A.B", "x"),
            ("MRSM_API_MY-LABEL", "http://x"),
            ("HOME", "/home"),
        ]
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
        assert_eq!(
            get_connector_env_vars(&env),
            vec!["MRSM_SQL_FOO".to_string(), "MRSM_API_MY-LABEL".to_string()]
        );
        assert_eq!(
            parse_connector_env_var("MRSM_SQL_MY_DB"),
            Some(("sql".to_string(), "my_db".to_string()))
        );
    }

    #[test]
    fn defaults_have_markers_resolved() {
        let d = default_config("/root", 8, false);
        assert_eq!(
            Value::Map(d.clone()).get_path(&["system", "cli", "max_daemons"]),
            Some(&Value::Int(24))
        );
        assert_eq!(
            Value::Map(d).get_path(&["api", "uvicorn", "workers"]),
            Some(&Value::Int(4))
        );
    }

    fn scratch(env: &[(&str, &str)]) -> (tempfile::TempDir, Config) {
        let root = tempfile::tempdir().unwrap();
        let mut vars = vec![(
            "MRSM_ROOT_DIR".to_string(),
            root.path().to_string_lossy().into_owned(),
        )];
        vars.extend(env.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        let config = Config::new(ConfigOptions::with_env(vars)).unwrap();
        (root, config)
    }

    #[test]
    fn a_failing_yaml_dump_leaves_the_file_alone() {
        let (root, config) = scratch(&[]);
        let directory = root.path().join("config");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("meerschaum.yaml");
        std::fs::write(&path, "instance: sql:keep\n").unwrap();
        let new = ConfigValue::map_from_value(
            Value::from_json_str(r#"{"meerschaum": {"instance": "sql:new"}}"#)
                .unwrap()
                .as_map()
                .unwrap(),
        );

        let failing = |_: &ConfigValue| Err("cannot represent an object".to_string());
        let wrote =
            config.with_state(|s| s.write_config_with(Some(&new), Some(&directory), &failing));
        assert!(!wrote);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "instance: sql:keep\n"
        );
        let warnings = config.take_warnings();
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("FAILED TO WRITE") && w.contains("cannot represent")),
            "{warnings:?}"
        );

        assert!(config.with_state(|s| s.write_config_lossless(Some(&new), Some(&directory))));
        assert!(
            std::fs::read_to_string(&path)
                .unwrap()
                .contains("instance: sql:new")
        );
    }

    #[test]
    fn a_date_in_a_config_file_survives_get_and_write() {
        let (root, config) = scratch(&[]);
        let directory = root.path().join("config");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("typed.yaml"),
            "when: 2024-01-01\nstamp: 2024-01-01T12:30:00Z\nraw: !!binary eHl6\nbig: 123456789012345678901234567890\nkeys:\n  1: number\n  b: text\nlink: MRSM{typed:when}\ntext: 'at MRSM{typed:stamp}'\n",
        )
        .unwrap();
        let (valid, value) = config.get_with_lossless(&["typed"], GetOptions::default());
        assert!(valid);
        let map = value.as_map().unwrap();
        assert!(matches!(map["when"], ConfigValue::Date(_)));
        assert!(matches!(map["stamp"], ConfigValue::DateTime(_)));
        assert_eq!(map["raw"], ConfigValue::Bytes(b"xyz".to_vec()));
        assert!(matches!(map["big"], ConfigValue::BigInt(_)));
        // The symlink to a date is that date; embedded in text it is `str(datetime)`.
        assert_eq!(map["link"], map["when"]);
        assert_eq!(
            map["text"],
            ConfigValue::Str("at 2024-01-01 12:30:00+00:00".into())
        );
        // Substitution went through a JSON text: the typed keys are strings now, as in v4.
        assert!(map["keys"].as_map().is_some());

        let out = tempfile::tempdir().unwrap();
        let mut one = ConfigMap::new();
        one.insert("typed".into(), value);
        assert!(config.with_state(|s| s.write_config_lossless(Some(&one), Some(out.path()))));
        let text = std::fs::read_to_string(out.path().join("typed.yaml")).unwrap();
        assert!(text.contains("when: 2024-01-01"), "{text}");
        assert!(text.contains("raw: !!binary |"), "{text}");
    }

    #[test]
    fn unreadable_json_values_are_reported_like_python() {
        let (root, config) = scratch(&[]);
        let directory = root.path().join("config");
        std::fs::create_dir_all(&directory).unwrap();
        let mut one = ConfigMap::new();
        one.insert(
            "plain".into(),
            ConfigValue::from_node(&crate::yaml::node::Node::Date(crate::yaml::node::Date {
                year: 2024,
                month: 1,
                day: 1,
            })),
        );
        let mut wrapper = ConfigMap::new();
        let mut inner = ConfigMap::new();
        inner.insert("filetype".into(), ConfigValue::str("json"));
        inner.insert("when".into(), one["plain"].clone());
        wrapper.insert("plain".into(), ConfigValue::Map(inner));
        assert!(!config.with_state(|s| s.write_config_lossless(Some(&wrapper), Some(&directory))));
        assert!(!directory.join("plain.json").exists());
        let notices = config.take_notices();
        assert_eq!(
            notices,
            vec![Notice::Print(
                "FAILED TO WRITE!\nObject of type date is not JSON serializable\n{'sort_keys': False, 'indent': 2}"
                    .to_string()
            )]
        );
    }
}

#[cfg(test)]
mod write_fallback_tests {
    use super::*;

    fn leftovers(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with('.'))
            .collect()
    }

    /// A single bind-mounted file cannot be renamed over (`EBUSY`): the file is rewritten in place.
    #[test]
    fn a_busy_rename_falls_back_to_rewriting_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("plugins.json");
        std::fs::write(&target, "old contents, longer than the new ones\n").unwrap();
        #[cfg(unix)]
        let inode = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&target).unwrap());
        let busy = |_: &Path, _: &Path| Err(std::io::Error::from_raw_os_error(16));
        write_file_with(&target, b"new\n", &busy).unwrap();
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "new\n");
        assert!(
            leftovers(dir.path()).is_empty(),
            "the temporary file is removed"
        );
        #[cfg(unix)]
        assert_eq!(
            std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&target).unwrap()),
            inode,
            "same inode: the bind mount still sees the new contents"
        );
    }

    #[test]
    fn a_cross_device_rename_falls_back_too() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("a.json");
        std::fs::write(&target, "{}\n").unwrap();
        let exdev = |_: &Path, _: &Path| Err(std::io::Error::from_raw_os_error(18));
        write_file_with(&target, b"[1]\n", &exdev).unwrap();
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "[1]\n");
    }

    /// Any other rename failure is reported and leaves the file as it was.
    #[test]
    fn another_rename_failure_is_an_error_and_the_file_stays() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("a.json");
        std::fs::write(&target, "{\"keep\": 1}\n").unwrap();
        let full = |_: &Path, _: &Path| Err(std::io::Error::from_raw_os_error(28)); // ENOSPC
        assert!(write_file_with(&target, b"x", &full).is_err());
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "{\"keep\": 1}\n");
        assert!(leftovers(dir.path()).is_empty());
    }

    /// A writable file in a directory the process cannot write (`EACCES` creating the temporary
    /// file): rewritten in place, mode kept.
    #[cfg(unix)]
    #[test]
    fn a_writable_file_in_a_read_only_directory_is_rewritten_in_place() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("plugins.json");
        std::fs::write(&target, "{\"old\": true}\n").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640)).unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        let probe = dir.path().join(".probe");
        if std::fs::write(&probe, "").is_ok() {
            // Running as root: directory modes do not apply.
            let _ = std::fs::remove_file(&probe);
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        let result = write_file_atomically(&target, b"{\"new\": true}\n");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        result.unwrap();
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "{\"new\": true}\n"
        );
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    /// A new file in a directory the process cannot write is an error (nothing to rewrite).
    #[cfg(unix)]
    #[test]
    fn a_new_file_in_a_read_only_directory_is_an_error() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        let probe = dir.path().join(".probe");
        if std::fs::write(&probe, "").is_ok() {
            let _ = std::fs::remove_file(&probe);
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        let result = write_file_atomically(&dir.path().join("new.json"), b"{}");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }
}
