//! Derivative files of the `stack` config key: a port of `meerschaum/config/_sync.py`
//! (`sync_yaml_configs`, `sync_files`) and `config/stack/__init__.py::_write_initdb`.
//!
//! The first time the `stack` key is loaded (and whenever the stack actions ask), the Docker
//! Compose file and the two Grafana files are rewritten from the config, keeping the comment
//! header of the file already on disk, and the PostgreSQL extensions script is created once.
//! The built-in Grafana dashboards are installed next to the provider file, and the ones an
//! earlier release shipped are removed when Meerschaum still manages them.

use std::path::Path;

use super::{ConfigState, GetOptions};
use crate::yaml::dumper::{DumpOptions, dump_node};
use crate::yaml::loader::{LoadOptions, load_text};

/// The `(old, new)` replacements applied to the dumped Compose text: `$` is Compose's
/// interpolation marker, `<DOLLAR>` the config's spelling of a literal one.
const COMPOSE_REPLACEMENTS: &[(&str, &str)] = &[("$", "$$"), ("<DOLLAR>", "$")];

/// The stack's database setup script (`<root>/db/initdb/create-extensions.sql`), which
/// `write_initdb` creates when it is absent (a customized file is never touched). The one copy of
/// the text: `mrsm stack` (`meerschaum-cli`) writes it through this constant, and the Python
/// façade's `meerschaum.config.stack.CREATE_EXTENSIONS_TEXT` must stay byte-identical
/// (`tests/test_stack_setup.py`).
///
/// v5 differs from v4.1.4 (PARITY.md W17K1-001): v4 wrote four bare `CREATE EXTENSION` lines,
/// which abort the first start of an image without one of them; this script creates each
/// extension only when the server offers it and preloads `pg_stat_statements`.
pub const CREATE_EXTENSIONS_TEXT: &str = include_str!("create-extensions.sql");

/// Filled with the configured default instance when a built-in dashboard is written.
pub const GRAFANA_DEFAULT_INSTANCE: &str = "__MRSM_DEFAULT_INSTANCE__";

/// Managed Grafana resources shipped in both the standalone CLI and the Python extension.
pub const GRAFANA_DASHBOARDS: &[(&str, &str)] = &[
    (
        "meerschaum-syncs.json",
        include_str!("grafana/meerschaum-syncs.json"),
    ),
    (
        "meerschaum-engine.json",
        include_str!("grafana/meerschaum-engine.json"),
    ),
];

/// Built-in dashboards an earlier release wrote and this one no longer ships (5.0.0rc16 through
/// 5.0.0rc34 wrote "Meerschaum Performance", "Pipe Performance" and "Pipeline Performance"; the
/// Syncs dashboard replaces them): `(file name, UID, SHA-256 of every version Meerschaum wrote)`.
/// A version that fills in the default instance is hashed with the placeholder restored
/// ([`managed_dashboard_hash`]). A file matching none of them is the user's and is kept.
pub const RETIRED_GRAFANA_DASHBOARDS: &[(&str, &str, &[&str])] = &[
    (
        "meerschaum-performance.json",
        "mrsm-performance",
        &[
            "c4ffaa1b88d6134c1586482efd41bd6723881dea4248c3c7cf470370414606b3",
            "cc9124a8c2202c06ae13d64811aca72a156fb4f9a69164b0caf24cbb414febb5",
        ],
    ),
    (
        "meerschaum-pipes.json",
        "mrsm-pipe-performance",
        &[
            "f8367dc580e2d7911be790bc02a2e4a1860d2aa9976fb393c511ace1a1c414cc",
            "331adb9a244cf0b25848a584a90cc36ea780ac879baacc9161bcdc06153ef4c9",
        ],
    ),
    (
        "meerschaum-pipelines.json",
        "mrsm-pipeline-performance",
        &[
            "b3f9aad150598de76f025c81ceab0ea146d1620ecca8c5b8a9947e209e1b2c0f",
            "76edfcdfbfa921e86d62861d8312d93968a9ed2eb9e98c95bacb94c1304c254e",
        ],
    ),
];

/// The SHA-256 (lowercase hex) of a dashboard file as Meerschaum's template had it: a filled-in
/// default instance (the `default_instance` variable's value, written as a whole JSON string) is
/// put back to [`GRAFANA_DEFAULT_INSTANCE`] first. A file that does not parse is hashed as it is.
pub fn managed_dashboard_hash(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let filled = serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|dashboard| {
            dashboard["templating"]["list"]
                .as_array()?
                .iter()
                .find(|variable| variable["name"] == "default_instance")?["query"]
                .as_str()
                .map(str::to_string)
        })
        .filter(|instance| !instance.is_empty() && instance != GRAFANA_DEFAULT_INSTANCE);
    let template = match filled {
        Some(instance) => text.replace(
            &serde_json::Value::String(instance).to_string(),
            &format!("\"{GRAFANA_DEFAULT_INSTANCE}\""),
        ),
        None => text.to_string(),
    };
    Sha256::digest(template.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl ConfigState {
    /// `_read_yaml_config(path)`: the leading comment / blank-line header of a YAML file. A file
    /// that does not parse is reported (`Could not parse '<path>':`) and left as it is.
    fn read_yaml_header(&mut self, path: &Path) -> String {
        let Ok(bytes) = std::fs::read(path) else {
            return String::new();
        };
        let text = String::from_utf8_lossy(&bytes).into_owned();
        if let Err(e) = load_text(
            &text,
            &super::paths::posix(path),
            false,
            LoadOptions {
                safe: false,
                project: false,
            },
        ) {
            self.warn(format!(
                "Could not parse '{}':\n    {}",
                super::paths::posix(path),
                e.message
            ));
        }
        let mut header = String::new();
        for line in text.split_inclusive('\n') {
            if !line.starts_with('#') && line != "\n" {
                break;
            }
            header.push_str(line);
        }
        header
    }

    /// `sync_yaml_configs(keys, sub_path, substitute=True, replace_tuples=...)`: write the config
    /// at `keys` (symlinks substituted) to `sub_path` as YAML, under the header already there.
    pub fn sync_yaml_config(
        &mut self,
        keys: &[&str],
        sub_path: &Path,
        replace: &[(&str, &str)],
    ) -> Result<(), String> {
        // `get_keyfile_path(keys[0], create_new=True)` and the key file's own parse check.
        if let Some(key_path) = self.get_keyfile_path(keys[0], true, None) {
            self.read_yaml_header(&key_path);
        }
        let owned: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        let (_, value) = self.get_config_lossless(
            &owned,
            GetOptions {
                patch: true,
                substitute: true,
                write_missing: true,
                warn: true,
            },
        );
        let header = self.read_yaml_header(sub_path);
        let mut text = dump_node(
            &value.to_node(),
            &DumpOptions {
                sort_keys: false,
                ..DumpOptions::default()
            },
        )
        .map_err(|e| e.message)?;
        for (old, new) in replace {
            text = text.replace(old, new);
        }
        if let Some(parent) = sub_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(sub_path, format!("{header}{text}")).map_err(|e| e.to_string())
    }

    /// `_write_initdb()`: the PostgreSQL extensions script, created only when absent.
    pub fn write_initdb(&self) -> Result<(), String> {
        let path = self.paths().path("DB_CREATE_EXTENSIONS_PATH");
        if path.exists() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(&path, CREATE_EXTENSIONS_TEXT).map_err(|e| e.to_string())
    }

    /// The configured default instance (`meerschaum:instance`), as the built-in dashboards
    /// compare it with each pipe's instance.
    fn grafana_default_instance(&mut self) -> String {
        let keys = ["meerschaum".to_string(), "instance".to_string()];
        let (valid, value) = self.get_config_lossless(
            &keys,
            GetOptions {
                patch: true,
                substitute: true,
                write_missing: false,
                warn: false,
            },
        );
        let instance = valid
            .then(|| crate::pyfmt::py_str(&value.to_value()))
            .filter(|instance| !instance.is_empty() && instance != "None")
            .unwrap_or_else(|| "sql:main".to_string());
        // A JSON string body: the keys are plain connector keys, but escape defensively.
        serde_json::to_string(&instance)
            .map(|quoted| quoted[1..quoted.len() - 1].to_string())
            .unwrap_or(instance)
    }

    /// Install/update built-in dashboard files in the existing provisioner's mounted folder and
    /// remove the retired built-ins Meerschaum still manages. Other files are untouched; a
    /// filename collision with a different dashboard UID is kept.
    pub fn write_grafana_dashboards(&mut self) -> Result<(), String> {
        let provider = self.paths().path("GRAFANA_DASHBOARD_PATH");
        let directory = provider
            .parent()
            .ok_or("Grafana dashboard directory is missing")?;
        std::fs::create_dir_all(directory).map_err(|error| error.to_string())?;
        // Pipes are labeled as `Pipe(...)`, which omits the configured default instance.
        let default_instance = self.grafana_default_instance();
        for (name, template) in GRAFANA_DASHBOARDS {
            let filled = template.replace(GRAFANA_DEFAULT_INSTANCE, &default_instance);
            let source: &str = &filled;
            let path = directory.join(name);
            if path.is_symlink() {
                self.warn(format!(
                    "Keeping customized Grafana dashboard '{}': it is a symlink.",
                    path.display()
                ));
                continue;
            }
            match std::fs::read_to_string(&path) {
                Ok(existing) => {
                    if existing == source {
                        continue;
                    }
                    let old = serde_json::from_str::<serde_json::Value>(&existing);
                    let new: serde_json::Value =
                        serde_json::from_str(source).map_err(|error| error.to_string())?;
                    if old.as_ref().ok().and_then(|value| value.get("uid")) != new.get("uid") {
                        self.warn(format!("Keeping customized Grafana dashboard '{}': its UID differs from the built-in.", path.display()));
                        continue;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    self.warn(format!(
                        "Keeping unreadable Grafana dashboard '{}': {error}.",
                        path.display()
                    ));
                    continue;
                }
            }
            super::write_file_atomically(&path, source.as_bytes())
                .map_err(|error| error.to_string())?;
        }
        self.remove_retired_grafana_dashboards(directory);
        Ok(())
    }

    /// Remove the dashboards an earlier release installed ([`RETIRED_GRAFANA_DASHBOARDS`]) when
    /// the file is exactly what Meerschaum wrote; Grafana's provisioner then drops them. A
    /// symlink, an unreadable file, another UID or a customized copy is kept with a warning.
    fn remove_retired_grafana_dashboards(&mut self, directory: &Path) {
        for (name, uid, hashes) in RETIRED_GRAFANA_DASHBOARDS {
            let path = directory.join(name);
            if path.is_symlink() {
                self.warn(format!(
                    "Keeping retired Grafana dashboard '{}': it is a symlink.",
                    path.display()
                ));
                continue;
            }
            let existing = match std::fs::read_to_string(&path) {
                Ok(existing) => existing,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    self.warn(format!(
                        "Keeping unreadable Grafana dashboard '{}': {error}.",
                        path.display()
                    ));
                    continue;
                }
            };
            if hashes.contains(&managed_dashboard_hash(&existing).as_str()) {
                if let Err(error) = std::fs::remove_file(&path) {
                    self.warn(format!(
                        "Could not remove the retired Grafana dashboard '{}': {error}.",
                        path.display()
                    ));
                }
                continue;
            }
            let same_uid = serde_json::from_str::<serde_json::Value>(&existing)
                .is_ok_and(|dashboard| dashboard["uid"] == *uid);
            self.warn(if same_uid {
                format!(
                    "Keeping customized Grafana dashboard '{}': Meerschaum no longer ships it \
                     (see the Syncs and Engine dashboards); delete it when you no longer need it.",
                    path.display()
                )
            } else {
                format!(
                    "Keeping customized Grafana dashboard '{}': its UID differs from the retired built-in.",
                    path.display()
                )
            });
        }
    }

    /// `sync_files(keys)`: only the `stack` key has derivative files. Does nothing when writes
    /// are disabled (`_allow_write_missing`), as v4's callers check before calling.
    pub fn sync_files(&mut self, keys: &[&str]) -> Result<(), String> {
        if !self.allow_writes() || !keys.contains(&"stack") {
            return Ok(());
        }
        let paths = self.paths().clone();
        let compose_name = paths.path("STACK_COMPOSE_FILENAME");
        let compose_name = compose_name.to_string_lossy().into_owned();
        self.sync_yaml_config(
            &["stack", compose_name.as_str()],
            &paths.path("STACK_COMPOSE_PATH"),
            COMPOSE_REPLACEMENTS,
        )?;
        self.sync_yaml_config(
            &["stack", "grafana", "datasource"],
            &paths.path("GRAFANA_DATASOURCE_PATH"),
            &[],
        )?;
        self.sync_yaml_config(
            &["stack", "grafana", "dashboard"],
            &paths.path("GRAFANA_DASHBOARD_PATH"),
            &[],
        )?;
        self.write_initdb()?;
        self.write_grafana_dashboards()
    }
}
