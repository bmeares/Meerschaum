//! The config registry: a port of `meerschaum.config` (`get_config`, `read_config`,
//! `write_config`, `search_and_substitute_config`, the environment overrides and the paths).
//!
//! The registry mirrors Python's process-global, lazily-loaded `config` dictionary:
//!
//! - Each top-level key is one file under `CONFIG_DIR_PATH` (`<key>.yaml`, `.yml` or `.json`),
//!   read the first time the key is requested.
//! - Missing keys fall back to the defaults (v4's `meerschaum/config/_default.py`, embedded as
//!   `default_config.json`, which is edited by hand where v5 changes a default; the
//!   `tests/fixtures/default_config.json` parity fixture keeps v4's values); falling back merges the defaults into the loaded key and, when
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
pub mod dataframe;
pub mod environment;
pub mod grafana_readonly;
pub mod lossless;
pub mod paths;
pub mod provenance;
pub mod secrets;
pub mod sync;

mod files;
mod overrides;
mod substitute;

use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard};

use regex::Regex;

use crate::pyfmt::{JsonStyle, json_dumps_str, py_repr, str_repr};
use crate::static_config::static_config;
use crate::value::{Map, Value};
use crate::yaml::dumper::{DumpOptions, dump_node};
use crate::yaml::loader::{LoadOptions, load_text};
use crate::yaml::scanner::PyYamlError;

pub use lossless::{ConfigMap, ConfigValue, JsonDecodeError};
pub use overrides::{NATIVE_EXECUTION_ENV_VARS, get_connector_env_vars, parse_connector_env_var};
pub use paths::{PathValue, Paths};
pub use provenance::{EnvLayer, Origin};

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
/// (PEP 440, `5.1.0rc1`), in [`DEFAULT_CONFIG_JSON`]: the stack's `api` image tag
/// (`bmeares/meerschaum:{__MRSM_VERSION__}-full`), so a root without a `stack.yaml` always runs the
/// API server of the version that wrote the compose file (see `PARITY.md`, the stack's `api` image).
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
    /// The environment configures the same thing twice (`MRSM_CONFIG` and `MRSM_CONFIG_FILE`),
    /// or a `*_FILE` variable names a file that cannot be read.
    #[error("{0}")]
    Environment(String),
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
    /// The layers applied over the files, in order (`MRSM_<TYPE>_<LABEL>` connectors,
    /// `MRSM_CONFIG` / `MRSM_PATCH`, `patch_config/`): what [`provenance`] replays to know
    /// where a value came from, so a writer never persists what only they hold.
    layers: Vec<EnvLayer>,
    /// 5.1: a team-scoped process's registry (`MRSM_TEAM_SCOPE` is set). Its configuration sets
    /// the connectors the team was not given to `null`: a key path through such a `null` is
    /// withheld, not missing, and never falls back to the defaults (which would resurrect a
    /// default connector's attributes, and merge every default into the configuration on each
    /// such lookup).
    withholds: bool,
}

/// Whether a registry built from `lookup` is a team-scoped process's ([`ConfigState::withholds`]).
fn team_scoped(lookup: impl Fn(&str) -> Option<String>) -> bool {
    lookup(crate::team_scope::TEAM_SCOPE_VAR).is_some_and(|v| !v.trim().is_empty())
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
        environment::environment_patch_texts(&opts.env).map_err(ConfigError::Environment)?;
        let mut state = ConfigState {
            paths,
            defaults,
            config: ConfigMap::new(),
            allow_writes: opts.allow_writes,
            notices: Vec::new(),
            write_error: None,
            layers: Vec::new(),
            withholds: team_scoped(lookup),
        };
        state.apply_environment_uris(&opts.env, false);
        state.apply_environment_patches(&opts.env);
        // A team-scoped process (5.1) never reads or consumes the root's patch directories:
        // what they hold is the server's.
        if lookup(crate::team_scope::TEAM_SCOPE_VAR).is_none_or(|v| v.is_empty()) {
            state.apply_patch_dirs();
        }
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
                layers: Vec::new(),
                // The Python boundary's registry: the process's own environment.
                withholds: team_scoped(|k| std::env::var(k).ok()),
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

    /// Re-read `meerschaum:connectors` from the config files and patch it over the section in
    /// memory: the connectors another process registered since this one read its config (a team's
    /// database created from the console while the server runs). Connectors that exist only in
    /// memory (environment connectors, patches) are kept. `true` when the section changed.
    pub fn refresh_connectors(&self) -> bool {
        self.with_state(|s| s.refresh_connectors())
    }

    /// The file `meerschaum` is read from (to notice that another process wrote it).
    pub fn connectors_file(&self) -> Option<PathBuf> {
        self.with_state(|s| s.get_keyfile_path("meerschaum", false, None))
    }

    /// `write_config(config_dict, directory)`.
    pub fn write_config(&self, config: Option<&Map>, directory: Option<&Path>) -> bool {
        self.lock().write_config(config, directory)
    }

    /// Add the connector `typ:label` with `attributes` to the configuration file, as written, and
    /// to memory (5.1: a team's connector). Nothing else in the file changes: `MRSM{...}`
    /// references stay references, and what only the environment holds (`MRSM_<TYPE>_<LABEL>`
    /// connectors, `MRSM_CONFIG` / `MRSM_PATCH` patches) is never written. The attributes hold a
    /// generated password, so the file is left readable by its owner only. `Err` when the
    /// connector is already in the file, or the file could not be read or written.
    pub fn add_connector(&self, typ: &str, label: &str, attributes: &Map) -> Result<(), String> {
        let keys = format!("{typ}:{label}");
        self.with_state(|s| {
            let key = "meerschaum".to_string();
            let (disk, _) = s
                .read_config_lossless(None, Some(std::slice::from_ref(&key)), false, false, true)
                .map_err(|e| format!("The configuration could not be read: {e:?}"))?;
            let mut section = disk
                .get(&key)
                .and_then(ConfigValue::as_map)
                .cloned()
                .unwrap_or_default();
            let value = ConfigValue::from_value(&Value::Map(attributes.clone()));
            let connectors = section
                .entry("connectors".to_string())
                .or_insert_with(ConfigValue::new_map);
            if connectors.as_map().is_none() {
                *connectors = ConfigValue::new_map();
            }
            let labels = connectors
                .as_map_mut()
                .expect("a map")
                .entry(typ.to_string())
                .or_insert_with(ConfigValue::new_map);
            if labels.as_map().is_none() {
                *labels = ConfigValue::new_map();
            }
            let labels = labels.as_map_mut().expect("a map");
            if labels.get(label).is_some_and(ConfigValue::truthy) {
                return Err(format!("The connector '{keys}' already exists."));
            }
            labels.insert(label.to_string(), value.clone());
            let mut config = ConfigMap::new();
            config.insert(key.clone(), ConfigValue::Map(section));
            if !s.write_config_lossless(Some(&config), None) {
                let reason = s
                    .take_write_error()
                    .map(|(_, message)| format!(": {message}"))
                    .unwrap_or_else(|| ".".into());
                return Err(format!(
                    "The connector '{keys}' could not be written to the configuration{reason}"
                ));
            }
            #[cfg(unix)]
            if let Some(path) = s.get_keyfile_path(&key, false, None) {
                use std::os::unix::fs::PermissionsExt;
                if let Ok(meta) = std::fs::metadata(&path) {
                    let mode = meta.permissions().mode() & 0o7777;
                    if mode & 0o077 != 0 {
                        std::fs::set_permissions(
                            &path,
                            std::fs::Permissions::from_mode(mode & 0o7700),
                        )
                        .map_err(|e| {
                            format!(
                                "The connector '{keys}' was written, but '{}' could not be made \
                                 readable by its owner only: {e}",
                                path.display()
                            )
                        })?;
                    }
                }
            }
            // In memory (when the section is loaded; else its first read loads the file).
            if let Some(section) = s.config.get_mut(&key).and_then(|m| m.as_map_mut()) {
                let connectors = section
                    .entry("connectors".to_string())
                    .or_insert_with(ConfigValue::new_map);
                if let Some(connectors) = connectors.as_map_mut() {
                    let labels = connectors
                        .entry(typ.to_string())
                        .or_insert_with(ConfigValue::new_map);
                    if let Some(labels) = labels.as_map_mut() {
                        labels.insert(label.to_string(), value);
                    }
                }
            }
            Ok(())
        })
    }

    /// Move the connector reservations of team `from` to `to` (5.1,
    /// [`crate::access::move_reservations`]) in the configuration file, as written (references
    /// are kept), and in memory. The connectors changed (`type:label`); `Err` when the file could
    /// not be read or written. Connectors that exist only in memory (environment connectors) are
    /// rewritten in memory only.
    pub fn move_team_reservations(&self, from: &str, to: &str) -> Result<Vec<String>, String> {
        self.with_state(|s| {
            let key = "meerschaum".to_string();
            let (disk, _) = s
                .read_config(None, Some(std::slice::from_ref(&key)), false, false, true)
                .map_err(|e| format!("The configuration could not be read: {e:?}"))?;
            let mut section = disk.get(&key).map(|v| v.to_map()).unwrap_or_default();
            let mut changed = Vec::new();
            if let Some(Value::Map(connectors)) = section.get_mut("connectors") {
                changed = crate::access::move_reservations(connectors, from, to);
            }
            if !changed.is_empty() {
                let mut config = Map::new();
                config.insert(key.clone(), Value::Map(section));
                if !s.write_config(Some(&config), None) {
                    return Err(format!(
                        "The connectors {} could not be written to the configuration.",
                        changed.join(", ")
                    ));
                }
                s.refresh_connectors();
            }
            // What is only in memory.
            let mut live = ConfigValue::map_to_value(&s.config);
            if let Some(Value::Map(meerschaum)) = live.get_mut(&key)
                && let Some(Value::Map(connectors)) = meerschaum.get_mut("connectors")
            {
                let more = crate::access::move_reservations(connectors, from, to);
                if !more.is_empty() {
                    let connectors = ConfigValue::from_value(&Value::Map(connectors.clone()));
                    if let Some(section) = s.config.get_mut(&key).and_then(|m| m.as_map_mut()) {
                        section.insert("connectors".to_string(), connectors);
                    }
                    for keys in more {
                        if !changed.contains(&keys) {
                            changed.push(keys);
                        }
                    }
                }
            }
            Ok(changed)
        })
    }

    /// Where each value of the top-level key `key` comes from (5.1, see [`provenance`]).
    pub fn origins(&self, key: &str) -> provenance::KeyOrigins {
        self.with_state(|s| s.origins(key))
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

/// Replace `path` with `bytes` without ever leaving it truncated, partial or deleted: the text
/// goes to a temporary file in the same directory (created with the replaced file's mode, owner
/// and group where permitted), is synced, and is renamed over the target. A failure removes only
/// the temporary file. Extended attributes, ACLs and SELinux labels are not copied (the new
/// file gets the directory's defaults; see PARITY.md, `write_config`). Where a temporary file cannot be used
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

/// The text of a `KeyError` for `key` (`str(KeyError('txt'))`).
fn key_error_text(key: &str) -> String {
    str_repr(key)
}

// Execution aliases are complete connector sections, even when an unrelated missing legacy
// setting makes the parity fallback merge all defaults. Preserve malformed explicit fields
// too: the alias validator must reject them instead of silently repairing their input.
fn connector_alias_sections(connectors: &ConfigValue) -> Vec<(String, String, ConfigValue)> {
    let mut aliases = Vec::new();
    if let Some(types) = connectors.as_map() {
        for (kind, labels) in types {
            if let Some(labels) = labels.as_map() {
                for (label, attributes) in labels {
                    if attributes
                        .get(crate::team_scope::api_execution::ALIAS_ATTRIBUTE)
                        .is_some()
                    {
                        aliases.push((kind.clone(), label.clone(), attributes.clone()));
                    }
                }
            }
        }
    }
    aliases
}

fn restore_connector_alias_sections(
    connectors: &mut ConfigValue,
    aliases: Vec<(String, String, ConfigValue)>,
) {
    let Some(types) = connectors.as_map_mut() else {
        return;
    };
    for (kind, label, attributes) in aliases {
        let Some(labels) = types.get_mut(&kind).and_then(ConfigValue::as_map_mut) else {
            continue;
        };
        // An explicit null still revokes a section; only an alias surviving the merge is
        // replaced with its exact original descriptor map.
        if labels.get(&label).is_some_and(|value| {
            value
                .get(crate::team_scope::api_execution::ALIAS_ATTRIBUTE)
                .is_some()
        }) {
            labels.insert(label, attributes);
        }
    }
}

// Incoming complete aliases override native defaults atomically; an existing alias is also
// protected from a later partial native-attribute patch. Explicit null sections still revoke.
fn apply_config_patch(base: &ConfigMap, patch: &ConfigMap) -> ConfigMap {
    let aliases_of = |config: &ConfigMap| {
        config
            .get("meerschaum")
            .and_then(|section| section.get("connectors"))
            .map(connector_alias_sections)
            .unwrap_or_default()
    };
    let mut aliases = aliases_of(base);
    aliases.extend(aliases_of(patch));
    let mut merged = apply_patch_maps(base, patch);
    if let Some(connectors) = merged
        .get_mut("meerschaum")
        .and_then(ConfigValue::as_map_mut)
        .and_then(|section| section.get_mut("connectors"))
    {
        restore_connector_alias_sections(connectors, aliases);
    }
    merged
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
        let mut withheld = false;
        let mut alias_withheld = false;
        if !invalid_keys {
            let mut current: Option<&ConfigValue> = None;
            for (i, k) in key_refs.iter().enumerate() {
                current = if i == 0 {
                    self.config.get(*k)
                } else {
                    let parent = current;
                    let child = parent.and_then(|c| c.get(k));
                    withheld = child.is_none() && matches!(parent, Some(ConfigValue::Null));
                    alias_withheld |= child.is_none()
                        && parent.is_some_and(|value| {
                            value
                                .get(crate::team_scope::api_execution::ALIAS_ATTRIBUTE)
                                .is_some()
                        });
                    child
                };
                if current.is_none() {
                    break;
                }
            }
            found = current.cloned();
        }
        // 5.1: below a connector a team-scoped process was not given (`null`): withheld. No
        // default stands in for it and nothing is merged (see `ConfigState::withholds`).
        if alias_withheld || withheld && self.withholds {
            return (false, ConfigValue::Null);
        }
        if let Some(mut value) = found {
            // 5.1: a path holding a secret reference re-reads it (a rotated file is seen by the
            // next connector built); a process that never met one pays an atomic load.
            if opts.substitute && k0 != symlinks_key && secrets::seen() {
                if let Some(fresh) = self.refresh_secrets(keys) {
                    value = fresh;
                }
                self.warn_unresolved_secret(keys, &value);
            }
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
        let aliases = self
            .config
            .get("meerschaum")
            .and_then(|section| section.get("connectors"))
            .map(connector_alias_sections)
            .unwrap_or_default();
        self.config = apply_patch_maps(&trimmed, &self.config);
        if let Some(connectors) = self
            .config
            .get_mut("meerschaum")
            .and_then(ConfigValue::as_map_mut)
            .and_then(|section| section.get_mut("connectors"))
        {
            restore_connector_alias_sections(connectors, aliases);
        }

        if opts.patch && k0 != symlinks_key && opts.write_missing {
            let keyfile_exists = self.get_keyfile_path(&k0, false, None).is_some();
            if !keyfile_exists {
                let cfg = self.config.clone();
                self.write_config_lossless(Some(&cfg), None);
            }
        }

        (true, default_value)
    }

    /// Re-resolve the secret references (5.1) recorded under `_symlinks` at `keys` (or below), and
    /// update the in-memory values that changed. Returns the value at `keys` when one did.
    fn refresh_secrets(&mut self, keys: &[String]) -> Option<ConfigValue> {
        let symlinks_key = Self::symlinks_key();
        let mut node = self.config.get(symlinks_key)?;
        for k in keys {
            node = node.get(k)?;
        }
        let mut found: Vec<(Vec<String>, String)> = Vec::new();
        fn collect(v: &ConfigValue, path: &mut Vec<String>, out: &mut Vec<(Vec<String>, String)>) {
            match v {
                ConfigValue::Str(s) if secrets::contains_reference(s) => {
                    out.push((path.clone(), s.clone()))
                }
                ConfigValue::Map(m) => {
                    for (k, v) in m {
                        path.push(k.clone());
                        collect(v, path, out);
                        path.pop();
                    }
                }
                _ => {}
            }
        }
        collect(node, &mut Vec::new(), &mut found);
        if found.is_empty() {
            return None;
        }
        let options = SubstituteOptions {
            keep_symlinks: false,
            ..SubstituteOptions::default()
        };
        let mut changed = false;
        for (rel, original) in found {
            let resolved = match secrets::substitute_references(&original, &self.paths.root_dir) {
                Some(text) => ConfigValue::Str(text),
                // Also an ordinary symlink: the general substitution.
                None => {
                    let mut single = ConfigMap::new();
                    single.insert("v".into(), ConfigValue::Str(original));
                    let Ok(subbed) = self.search_and_substitute_with(&single, &options) else {
                        continue;
                    };
                    let Some(resolved) = subbed.get("v").cloned() else {
                        continue;
                    };
                    resolved
                }
            };
            let full: Vec<&str> = keys.iter().chain(rel.iter()).map(String::as_str).collect();
            if let Some(slot) = value_at_mut(&mut self.config, &full)
                && *slot != resolved
            {
                *slot = resolved;
                changed = true;
            }
        }
        if !changed {
            return None;
        }
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        self.config
            .get(refs[0])
            .and_then(|v| v.get_path(&refs[1..]))
            .cloned()
    }

    /// Warn once per process when a setting read through `get_config` is a secret reference that
    /// could not be resolved (its text is what the caller gets).
    fn warn_unresolved_secret(&mut self, keys: &[String], value: &ConfigValue) {
        let ConfigValue::Str(text) = value else {
            return;
        };
        for reference in secrets::references_in(text) {
            let key = keys.join(":");
            if secrets::first_time(&format!("unresolved\u{0}{key}\u{0}{reference}")) {
                self.warn(secrets::unresolved_message(&key, &reference));
            }
        }
    }
}

/// The value at a key path of a config map, mutably.
fn value_at_mut<'a>(map: &'a mut ConfigMap, path: &[&str]) -> Option<&'a mut ConfigValue> {
    let (first, rest) = path.split_first()?;
    let mut current = map.get_mut(*first)?;
    for k in rest {
        current = current.as_map_mut()?.get_mut(*k)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests;

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
