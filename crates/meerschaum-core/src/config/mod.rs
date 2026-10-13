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
//! - `MRSM_SQL_<LABEL>` / `MRSM_API_<LABEL>` (any `MRSM_<TYPE>_<LABEL>`) variables add connectors,
//!   and `MRSM_CONFIG` / `MRSM_PATCH` patch the config, when the registry is created.
//! - `patch_config/` and `permanent_patch_config/` directories under the root are applied.
//!
//! Lookups that load files or fall back to defaults mutate the in-memory state exactly as the
//! Python implementation does, so the order of lookups is observable (as in Python).

pub mod paths;

use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard};

use regex::Regex;

use crate::pyfmt::{JsonStyle, json_dumps, json_dumps_str, py_repr, py_str, string_to_dict};
use crate::static_config::static_config;
use crate::value::{Map, Value, apply_patch, apply_patch_maps};

pub use paths::{PathValue, Paths};

/// The embedded default configuration with machine-dependent markers (see
/// `tests/fixtures/gen_default_config.py`).
pub const DEFAULT_CONFIG_JSON: &str = include_str!("default_config.json");

/// The marker substring replaced by the root directory in [`DEFAULT_CONFIG_JSON`].
pub const ROOT_DIR_MARKER: &str = "{__MRSM_ROOT_DIR__}";

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
    /// A development build refused to fall back to the real default root.
    #[error(
        "Refusing to use the default root '{path}' from a development build. \
         Set MRSM_ROOT_DIR to a scratch directory, or set MRSM_ALLOW_DEFAULT_ROOT=1 to use the real root."
    )]
    DefaultRootRefused {
        /// The default root that would have been used.
        path: String,
    },
    /// An I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
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
            if m.len() == 1 {
                if let Some(Value::Str(marker)) = m.get("__mrsm_dynamic__") {
                    return match marker.as_str() {
                        "cpu_count*3" => Value::Int(cpu * 3),
                        "max(cpu_count/2,1)" => Value::Int(std::cmp::max(cpu / 2, 1)),
                        "not_windows" => Value::Bool(!windows),
                        _ => Value::Null,
                    };
                }
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
        Value::Str(s) if s.contains(ROOT_DIR_MARKER) => {
            Value::Str(s.replace(ROOT_DIR_MARKER, root))
        }
        other => other,
    }
}

/// Port of `revert_symlinks_config`: re-apply the `_symlinks` originals onto a copy of the config
/// and drop the `_symlinks` key.
pub fn revert_symlinks_config(config: &Map) -> Map {
    let symlinks_key = &static_config().config.symlinks_key;
    let Some(symlinks) = config.get(symlinks_key) else {
        return config.clone();
    };
    let mut reverted = config.clone();
    reverted.shift_remove(symlinks_key);
    fn deep_patch(target: &mut Map, patch: &Value) {
        let Value::Map(patch) = patch else {
            return;
        };
        for (key, value) in patch {
            match (value, target.get_mut(key)) {
                (Value::Map(_), Some(Value::Map(inner))) => deep_patch(inner, value),
                _ => {
                    target.insert(key.clone(), value.clone());
                }
            }
        }
    }
    deep_patch(&mut reverted, symlinks);
    reverted
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
        let env: Vec<(String, String)> = std::env::vars().collect();
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

/// The mutable registry state (Python's module-level `config` dictionary and friends).
#[derive(Debug, Clone)]
pub struct ConfigState {
    paths: Paths,
    defaults: Map,
    config: Map,
    env: Vec<(String, String)>,
    allow_writes: bool,
    warnings: Vec<String>,
}

/// The thread-safe config registry.
#[derive(Debug)]
pub struct Config {
    state: Mutex<ConfigState>,
}

impl Config {
    /// Build the registry from the current process environment.
    pub fn from_env() -> Result<Config, ConfigError> {
        Config::new(ConfigOptions::from_process())
    }

    /// Build the registry, applying the environment connectors, `MRSM_CONFIG` / `MRSM_PATCH`,
    /// and the patch directories, like importing `meerschaum.config`.
    pub fn new(opts: ConfigOptions) -> Result<Config, ConfigError> {
        let lookup = |k: &str| opts.lookup(k);
        let paths = Paths::from_env(&lookup, &opts.home, opts.windows, opts.package_root.clone())?;
        let defaults = default_config(&paths::posix(&paths.root_dir), opts.cpu_count, opts.windows);
        let mut state = ConfigState {
            paths,
            defaults,
            config: Map::new(),
            env: opts.env.clone(),
            allow_writes: opts.allow_writes,
            warnings: Vec::new(),
        };
        state.apply_environment_uris();
        state.apply_environment_patches();
        state.apply_patch_dirs();
        Ok(Config {
            state: Mutex::new(state),
        })
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
        let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        self.lock().get_config(&keys, opts)
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
        self.lock().config = config;
    }

    /// A copy of the in-memory config (`meerschaum.config.config`).
    pub fn snapshot(&self) -> Map {
        self.lock().config.clone()
    }

    /// The resolved paths.
    pub fn paths(&self) -> Paths {
        self.lock().paths.clone()
    }

    /// The default config for this root.
    pub fn defaults(&self) -> Map {
        self.lock().defaults.clone()
    }

    /// `search_and_substitute_config(config)`.
    pub fn search_and_substitute(&self, config: &Map) -> Result<Map, String> {
        self.lock().search_and_substitute(config)
    }

    /// `write_config(config_dict, directory)`.
    pub fn write_config(&self, config: Option<&Map>, directory: Option<&Path>) -> bool {
        self.lock().write_config(config, directory)
    }

    /// Drain the warnings recorded so far (Python emits them with `warn()`).
    pub fn take_warnings(&self) -> Vec<String> {
        std::mem::take(&mut self.lock().warnings)
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

impl ConfigState {
    fn symlinks_key() -> &'static str {
        &static_config().config.symlinks_key
    }

    fn warn(&mut self, msg: impl Into<String>) {
        self.warnings.push(msg.into());
    }

    fn env_get(&self, key: &str) -> Option<String> {
        self.env
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    }

    /// The resolved paths.
    pub fn paths(&self) -> &Paths {
        &self.paths
    }

    /// The in-memory config.
    pub fn config(&self) -> &Map {
        &self.config
    }

    /// Port of `get_config(*keys, ...)` returning `(valid, value)`.
    pub fn get_config(&mut self, keys: &[String], opts: GetOptions) -> (bool, Value) {
        let symlinks_key = Self::symlinks_key();
        if keys.is_empty() {
            return (true, Value::Map(self.config.clone()));
        }
        let k0 = keys[0].clone();

        // Re-substitute a key that was loaded earlier without substitution.
        if opts.substitute && k0 != symlinks_key {
            let current = self.config.get(&k0).cloned().unwrap_or(Value::Null);
            if json_dumps(&current, JsonStyle::Default, true).contains("MRSM{") {
                let mut single = Map::new();
                single.insert(k0.clone(), current.clone());
                match self.search_and_substitute(&single) {
                    Ok(subbed) => {
                        self.config
                            .insert(k0.clone(), subbed.get(&k0).cloned().unwrap_or(Value::Null));
                        if let Some(sub_links) = subbed.get(symlinks_key) {
                            let existing = self
                                .config
                                .get(symlinks_key)
                                .cloned()
                                .unwrap_or_else(Value::new_map);
                            let merged = apply_patch(sub_links, &existing);
                            self.config.insert(symlinks_key.to_string(), merged);
                        }
                    }
                    Err(e) => self.warn(e),
                }
            }
        }

        let mut invalid_keys = false;
        if !self.config.contains_key(&k0) && k0 != symlinks_key {
            let single = self
                .read_config(
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
                            .or_insert_with(Value::new_map);
                        if let Some(m) = links.as_map_mut() {
                            m.insert(k0.clone(), link.clone());
                        }
                    }
                }
            }
        }

        let mut c = Value::Map(self.config.clone());
        for k in keys {
            match c.get(k) {
                Some(v) => c = v.clone(),
                None => {
                    invalid_keys = true;
                    break;
                }
            }
        }

        if invalid_keys {
            let patched_default = if opts.substitute {
                let defaults = self.defaults.clone();
                match self.search_and_substitute(&defaults) {
                    Ok(m) => m,
                    Err(e) => {
                        self.warn(e);
                        defaults
                    }
                }
            } else {
                self.defaults.clone()
            };
            let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
            let in_default = Value::Map(patched_default.clone())
                .get_path(&key_refs)
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
                    self.warn(format!("Invalid keys in config: {repr}"));
                }
                return (false, Value::Null);
            };
            c = default_value;
            invalid_keys = false;

            // Don't write keys that we haven't yet loaded into memory.
            let mut trimmed = patched_default;
            trimmed.retain(|k, _| self.config.contains_key(k));
            self.config = apply_patch_maps(&trimmed, &self.config);

            if opts.patch && k0 != symlinks_key && opts.write_missing {
                let keyfile_exists = self.get_keyfile_path(&k0, false, None).is_some();
                if !keyfile_exists {
                    let cfg = self.config.clone();
                    self.write_config(Some(&cfg), None);
                }
            }
        }

        (!invalid_keys, c)
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
    /// Returns `(filekeys, missing_keys)`.
    fn select_filekeys(
        filenames: &[String],
        keys: Option<&[String]>,
    ) -> (Vec<String>, Vec<String>) {
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
        let stem = |f: &str| -> String {
            match f.rfind('.') {
                Some(i) if i > 0 => f[..i].to_string(),
                _ => f.to_string(),
            }
        };
        let mut sorted = candidates.clone();
        sorted.sort_by_key(|f| stem(f));
        let default_ft = &static_config().config.default_filetype;
        let mut filekeys = Vec::new();
        let mut i = 0;
        while i < sorted.len() {
            let s = stem(&sorted[i]);
            let mut group = vec![sorted[i].clone()];
            let mut j = i + 1;
            while j < sorted.len() && stem(&sorted[j]) == s {
                group.push(sorted[j].clone());
                j += 1;
            }
            let preferred = format!("{s}.{default_ft}");
            let chosen = if group.len() > 1 && group.contains(&preferred) {
                preferred
            } else {
                group[0].clone()
            };
            filekeys.push(chosen);
            i = j;
        }
        (filekeys, missing)
    }

    /// Port of `read_config`. Returns the config read from `directory` (default
    /// `CONFIG_DIR_PATH`) and the file names that were parsed.
    pub fn read_config(
        &mut self,
        directory: Option<&Path>,
        keys: Option<&[String]>,
        write_missing: bool,
        substitute: bool,
        raise_parsing_errors: bool,
    ) -> Result<(Map, Vec<String>), ConfigError> {
        let symlinks_key = Self::symlinks_key();
        let directory: PathBuf = directory
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.paths.config_dir.clone());
        let filenames = self.list_dir(&directory);
        let (filekeys, missing_keys) = Self::select_filekeys(&filenames, keys);

        let mut config = Map::new();
        let mut config_to_write = Map::new();

        if !missing_keys.is_empty() {
            for mk in &missing_keys {
                if !self.defaults.contains_key(mk) {
                    continue;
                }
                let default_dict = if substitute {
                    let defaults = self.defaults.clone();
                    match self.search_and_substitute(&defaults) {
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
                    .unwrap_or_else(Value::new_map);
                let value = default_dict.get(mk).cloned().unwrap_or(Value::Null);
                config.insert(mk.clone(), value.clone());
                if default_symlinks.truthy() {
                    for target in [&mut config, &mut config_to_write] {
                        let links = target
                            .entry(symlinks_key.to_string())
                            .or_insert_with(Value::new_map);
                        if let Some(m) = links.as_map_mut() {
                            let existing = m.get(mk).cloned().unwrap_or_else(Value::new_map);
                            m.insert(mk.clone(), apply_patch(&existing, &default_symlinks));
                        }
                    }
                }
                config_to_write.insert(mk.clone(), value);
            }
        }

        if !config_to_write.is_empty() && write_missing {
            self.write_config(Some(&config_to_write), Some(&directory));
        }

        for filename in &filekeys {
            let filepath = directory.join(filename);
            let (key, filetype) = match filename.rsplit_once('.') {
                Some((k, t)) => (k.to_string(), t.to_string()),
                None => continue,
            };
            if !FILETYPES.contains(&filetype.as_str()) {
                self.warn(format!(
                    "Unknown file '{filename}' in '{}'. Skipping...",
                    paths::posix(&directory)
                ));
                continue;
            }
            let text = std::fs::read_to_string(&filepath)?;
            let parsed = if filetype == "json" {
                Value::from_json_str(&text).map_err(|e| e.to_string())
            } else {
                crate::yaml::load(&text).map_err(|e| e.to_string())
            };
            let config_key = match parsed {
                Ok(v) => v,
                Err(e) => {
                    if raise_parsing_errors {
                        return Err(ConfigError::Parse {
                            file: filename.clone(),
                            message: e,
                        });
                    }
                    self.warn(format!(
                        "Could not parse '{filename}':\n    {e}\nUsing default values in memory; the file on disk is unchanged.\nFix it manually or run `edit config {key} --rollback` to restore a backup."
                    ));
                    self.defaults
                        .get(&key)
                        .cloned()
                        .unwrap_or_else(Value::new_map)
                }
            };
            let mut single = Map::new();
            single.insert(key.clone(), config_key);
            let single = if substitute {
                match self.search_and_substitute(&single) {
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
                single.get(&key).cloned().unwrap_or(Value::Null),
            );
            if let Some(link) = single.get(symlinks_key).and_then(|s| s.get(&key)) {
                let links = config
                    .entry(symlinks_key.to_string())
                    .or_insert_with(Value::new_map);
                if let Some(m) = links.as_map_mut() {
                    m.insert(key.clone(), link.clone());
                }
            }
        }

        Ok((config, filekeys))
    }

    /// Port of `get_keyfile_path`.
    pub fn get_keyfile_path(
        &self,
        key: &str,
        create_new: bool,
        directory: Option<&Path>,
    ) -> Option<PathBuf> {
        let directory: PathBuf = directory
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.paths.config_dir.clone());
        let filenames = self.list_dir(&directory);
        let (filekeys, _) = Self::select_filekeys(&filenames, Some(&[key.to_string()]));
        match filekeys.first() {
            Some(f) => Some(directory.join(f)),
            None if create_new => {
                Some(directory.join(format!("{key}.{}", static_config().config.default_filetype)))
            }
            None => None,
        }
    }

    /// Port of `write_config`. Returns `false` when writes are disabled or a write failed.
    pub fn write_config(&mut self, config: Option<&Map>, directory: Option<&Path>) -> bool {
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
        let config_dict = revert_symlinks_config(&config_dict);
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
                            self.warn(format!(
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
                "json" => text.push_str(&json_dumps(v, JsonStyle::Indent(2), true)),
                "yaml" | "yml" => text.push_str(&crate::yaml::dump(v)),
                other => {
                    self.warn(format!("FAILED TO WRITE! Unknown filetype '{other}'."));
                    let _ = std::fs::remove_file(&filepath);
                    return false;
                }
            }
            if std::fs::write(&filepath, text).is_err() {
                let _ = std::fs::remove_file(&filepath);
                return false;
            }
        }
        true
    }

    /// Port of `search_and_substitute_config` with the default arguments
    /// (`MRSM{` ... `}`, `:` delimiter, `!` literal prefix, `keep_symlinks=True`).
    pub fn search_and_substitute(&mut self, config: &Map) -> Result<Map, String> {
        let needle = "MRSM{";
        let mut links: Vec<(Vec<String>, String)> = Vec::new();
        fn find_symlinks(
            d: &Map,
            prefix: &[String],
            needle: &str,
            out: &mut Vec<(Vec<String>, String)>,
        ) {
            for (k, v) in d {
                let mut path = prefix.to_vec();
                path.push(k.clone());
                match v {
                    Value::Map(m) => find_symlinks(m, &path, needle, out),
                    Value::Str(s) if s.contains(needle) => out.push((path, s.clone())),
                    _ => {}
                }
            }
        }
        find_symlinks(config, &[], needle, &mut links);

        let mut haystack = json_dumps(&Value::Map(config.clone()), JsonStyle::Compact, true);
        let mut parsed_config = if !haystack.contains(needle) {
            config.clone()
        } else {
            static PATTERN_RE: LazyLock<Regex> = LazyLock::new(|| {
                Regex::new(r"MRSM\{(?P<pattern_keys>.*?)\}").expect("valid regex")
            });
            let mut order: Vec<String> = Vec::new();
            let mut patterns: std::collections::HashMap<String, (Value, bool, bool)> =
                std::collections::HashMap::new();
            let mut memo: std::collections::HashMap<Vec<String>, (bool, Value)> =
                std::collections::HashMap::new();
            let matches: Vec<(String, String, usize, usize)> = PATTERN_RE
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
                let mut keys: Vec<String> = raw_keys.split(':').map(String::from).collect();
                let mut force_literal = false;
                if let Some(first) = keys.first_mut() {
                    if let Some(stripped) = first.strip_prefix('!') {
                        *first = stripped.to_string();
                        force_literal = true;
                    }
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
                        let result = self.get_config(&keys, opts);
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
                    haystack = haystack.replace(
                        &json_dumps_str(&pattern, true),
                        &json_dumps(value, JsonStyle::Default, true),
                    );
                } else if *literal {
                    // PARITY: escaping `'` as `\'` produces invalid JSON (`_read_config.py:283`).
                    let dumped = json_dumps(value, JsonStyle::Default, true)
                        .replace('\\', "\\\\")
                        .replace('"', "\\\"")
                        .replace('\'', "\\'");
                    haystack = haystack.replace(&pattern, &dumped);
                } else {
                    haystack = haystack.replace(&pattern, &py_str(value));
                }
            }
            match Value::from_json_str(&haystack).map_err(|e| e.to_string())? {
                Value::Map(m) => m,
                v if !v.truthy() => Map::new(),
                _ => return Err("substituted config is not a dictionary".to_string()),
            }
        };

        if !links.is_empty() {
            let mut symlinks = Map::new();
            for (keys, pattern) in links {
                let mut s = &mut symlinks;
                for k in &keys[..keys.len() - 1] {
                    let entry = s.entry(k.clone()).or_insert_with(Value::new_map);
                    if !entry.is_map() {
                        *entry = Value::new_map();
                    }
                    s = entry.as_map_mut().expect("map");
                }
                s.insert(keys[keys.len() - 1].clone(), Value::Str(pattern));
            }
            let symlinks_key = Self::symlinks_key();
            let merged = match parsed_config.get(symlinks_key) {
                None => Value::Map(symlinks),
                Some(existing) => apply_patch(existing, &Value::Map(symlinks)),
            };
            parsed_config.insert(symlinks_key.to_string(), merged);
        }
        Ok(parsed_config)
    }

    /// Port of `apply_environment_uris` / `apply_connector_uri`.
    fn apply_environment_uris(&mut self) {
        for var in get_connector_env_vars(&self.env) {
            let Some((typ, label)) = parse_connector_env_var(&var) else {
                continue;
            };
            if typ.is_empty() || label.is_empty() {
                continue;
            }
            let uri = self.env_get(&var).unwrap_or_default();
            let conn_attrs = if uri.trim_start().starts_with('{') && uri.trim_end().ends_with('}') {
                match Value::from_json_str(&uri) {
                    Ok(v) => v,
                    Err(_) => {
                        self.warn(format!(
                            "Unable to parse JSON for environment connector '{typ}:{label}'."
                        ));
                        uri_attrs(&uri)
                    }
                }
            } else {
                uri_attrs(&uri)
            };
            let (_, meerschaum) =
                self.get_config(&["meerschaum".to_string()], GetOptions::default());
            let mut base = Map::new();
            base.insert("meerschaum".to_string(), meerschaum);
            let mut label_map = Map::new();
            label_map.insert(label, conn_attrs);
            let mut typ_map = Map::new();
            typ_map.insert(typ, Value::Map(label_map));
            let mut connectors = Map::new();
            connectors.insert("connectors".to_string(), Value::Map(typ_map));
            let mut patch = Map::new();
            patch.insert("meerschaum".to_string(), Value::Map(connectors));
            // PARITY: `set_config` replaces the whole config with only the `meerschaum` key
            // (`config/environment.py:166`).
            self.config = apply_patch_maps(&base, &patch);
        }
    }

    /// Port of `apply_environment_patches` (`MRSM_CONFIG`, then `MRSM_PATCH`).
    fn apply_environment_patches(&mut self) {
        let env_names = &static_config().environment;
        for var in [env_names.config.clone(), env_names.patch.clone()] {
            let Some(raw) = self.env_get(&var) else {
                continue;
            };
            let patch = match string_to_dict(raw.trim_start()) {
                Ok(p) => p,
                Err(_) => {
                    self.warn(format!(
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
                let _ = self.get_config(std::slice::from_ref(key), opts);
            }
            self.config = apply_patch_maps(&self.config, &patch);
        }
    }

    /// Apply `patch_config/` and `permanent_patch_config/` under the root directory.
    fn apply_patch_dirs(&mut self) {
        let patch_dir = self.paths.path("PATCH_DIR_PATH");
        let permanent_dir = self.paths.path("PERMANENT_PATCH_DIR_PATH");
        let patch_config = if patch_dir.exists() {
            self.read_config(Some(&patch_dir), None, true, true, false)
                .ok()
                .map(|(c, _)| c)
        } else {
            None
        };
        let permanent_config = if permanent_dir.exists() {
            self.read_config(Some(&permanent_dir), None, true, true, false)
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
            self.write_config(Some(&cfg), None);
            if self.allow_writes {
                let _ = std::fs::remove_dir_all(&permanent_dir);
                let _ = std::fs::remove_dir_all(self.paths.path("DEFAULT_CONFIG_DIR_PATH"));
            }
        }
    }
}

fn uri_attrs(uri: &str) -> Value {
    let mut m = Map::new();
    m.insert("uri".to_string(), Value::Str(uri.to_string()));
    Value::Map(m)
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
}
