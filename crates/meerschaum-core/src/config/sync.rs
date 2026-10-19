//! Derivative files of the `stack` config key: a port of `meerschaum/config/_sync.py`
//! (`sync_yaml_configs`, `sync_files`) and `config/stack/__init__.py::_write_initdb`.
//!
//! The first time the `stack` key is loaded (and whenever the stack actions ask), the Docker
//! Compose file and the two Grafana files are rewritten from the config, keeping the comment
//! header of the file already on disk, and the PostgreSQL extensions script is created once.

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
        self.write_initdb()
    }
}
