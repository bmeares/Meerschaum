//! What the environment adds to the config: the connectors of `MRSM_<TYPE>_<LABEL>` variables,
//! the `MRSM_CONFIG` / `MRSM_PATCH` patches and the `patch_config/` directories.

use super::*;

/// The Rust form of `STATIC_CONFIG['environment']['uri_regex']`, anchored like `re.match`.
static URI_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    let py = &static_config().environment.uri_regex;
    // Python treats the `-` after the `0-9` range literally; Rust needs it escaped.
    let rust = format!("^{}", py.replace("0-9-_", "0-9\\-_"));
    Regex::new(&rust).expect("valid connector URI regex")
});

/// Native launch controls are never connector URI declarations, including when a plugin
/// registers a connector type whose name happens to match one of these exact markers.
/// Keep in sync with config/environment.py; do not reserve an entire connector prefix.
pub const NATIVE_EXECUTION_ENV_VARS: &[&str] = &[
    "MRSM_API_HOST",
    "MRSM_API_HOST_CONFIG",
    "MRSM_API_HOST_PARENT_PIPE",
    "MRSM_API_HOST_PROXY_TOKEN",
    "MRSM_API_HOST_TRANSPORT",
    "MRSM_API_EXECUTION",
    "MRSM_METADATA_EXECUTION",
    "MRSM_NATIVE_JOB_LIFECYCLE",
    "MRSM_TEAM_SCOPE",
    "MRSM_TEAM_SANDBOX",
    "MRSM_TEAM_FINGERPRINT",
    "MRSM_TEAM_PROBE",
    "MRSM_EXECUTOR_PLUGINS",
    "MRSM_TERMINAL_LAUNCH",
    "MRSM_WEBTERM_STATE_DIR",
    "MRSM_BIN_EXEC",
    "MRSM_JOB_RESULT_PATH",
    "MRSM_JOB_SUPERVISOR",
    "MRSM_JOB_PROGRAM",
    "MRSM_JOB_STARTUP_RELEASE",
    "MRSM_JOB_START_KIND",
    "MRSM_JOB_LAPS_PATH",
    "MRSM_JOB_LAPS_PARENT",
    "MRSM_SYSTEMD_JOB_CHILD",
];

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
        .filter(|(k, _)| {
            URI_REGEX.is_match(k)
                && !reserved.contains(k)
                && !NATIVE_EXECUTION_ENV_VARS.contains(&k.as_str())
        })
        .map(|(k, _)| k.clone())
        .collect()
}

/// Split a connector variable into `(type, label)` (lowercased), as `apply_connector_uri` does.
pub fn parse_connector_env_var(var: &str) -> Option<(String, String)> {
    let caps = URI_REGEX.captures(var)?;
    Some((caps[1].to_lowercase(), caps[2].to_lowercase()))
}

impl ConfigState {
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
        let mut used: Vec<(String, String)> = Vec::new();
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
            self.config = apply_config_patch(&base, &patch);
            applied += 1;
            used.push((var.clone(), uri.to_string()));
        }
        if !used.is_empty() {
            self.record_layer(EnvLayer::Uris(used));
        }
        applied
    }

    /// Port of `apply_environment_patches` (`MRSM_CONFIG`, then `MRSM_PATCH`; 5.1: or the file
    /// `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE` names). Both forms of one set, or an unreadable
    /// file, is reported and nothing is applied ([`Config::new`] refuses it).
    pub fn apply_environment_patches(&mut self, env: &[(String, String)]) {
        let texts = match environment::environment_patch_texts(env) {
            Ok(texts) => texts,
            Err(message) => {
                self.warn(message);
                return;
            }
        };
        if texts.is_empty() {
            return;
        }
        self.record_layer(EnvLayer::Patches(texts.clone()));
        self.apply_patch_texts(&texts);
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
            self.record_layer(EnvLayer::PatchDir(patch.clone()));
            self.config = apply_config_patch(&self.config, &patch);
            if self.allow_writes {
                let _ = std::fs::remove_dir_all(&patch_dir);
            }
        }
        if let Some(permanent) = permanent_config {
            self.config = apply_config_patch(&self.config, &permanent);
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
