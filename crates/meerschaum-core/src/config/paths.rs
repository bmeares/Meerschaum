//! Port of `meerschaum.config._paths` / `meerschaum.config.paths`: every path name derived from
//! `MRSM_ROOT_DIR`, `MRSM_CONFIG_DIR`, `MRSM_PLUGINS_DIR` and `MRSM_VENVS_DIR`.
//!
//! [`Paths::get`] is pure. Python's `paths.<NAME>` attribute access also creates the directory
//! for names ending in `RESOURCES_PATH` (and `CONFIG_DIR_PATH`); call [`Paths::ensure`] for that.

use std::path::{Component, Path, PathBuf};

use super::ConfigError;

/// A path template part: a literal segment or a reference to another path name.
#[derive(Debug, Clone, Copy)]
enum Part {
    Lit(&'static str),
    Ref(&'static str),
}

use Part::{Lit, Ref};

/// The `paths` dictionary from `meerschaum/config/_paths.py`, in definition order. The fixed
/// entries (`PACKAGE_ROOT_PATH`, `ROOT_DIR_PATH`, `CONFIG_DIR_PATH`, and `VIRTENV_RESOURCES_PATH`
/// when `MRSM_VENVS_DIR` is set) are computed in [`Paths::base`].
const TEMPLATES: &[(&str, &[Part])] = &[
    ("PACKAGE_ROOT_PATH", &[]),
    ("ROOT_DIR_PATH", &[]),
    (
        "VIRTENV_RESOURCES_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("venvs")],
    ),
    ("CONFIG_DIR_PATH", &[]),
    (
        "DEFAULT_CONFIG_DIR_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("default_config")],
    ),
    (
        "PATCH_DIR_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("patch_config")],
    ),
    (
        "PERMANENT_PATCH_DIR_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("permanent_patch_config")],
    ),
    (
        "INTERNAL_RESOURCES_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit(".internal")],
    ),
    (
        "CONFIG_BACKUPS_RESOURCES_PATH",
        &[Ref("INTERNAL_RESOURCES_PATH"), Lit("config_backups")],
    ),
    (
        "STACK_RESOURCES_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("stack")],
    ),
    ("STACK_COMPOSE_FILENAME", &[Lit("docker-compose.yaml")]),
    (
        "STACK_COMPOSE_PATH",
        &[Ref("STACK_RESOURCES_PATH"), Ref("STACK_COMPOSE_FILENAME")],
    ),
    ("STACK_ENV_FILENAME", &[Lit(".env")]),
    (
        "STACK_ENV_PATH",
        &[Ref("STACK_RESOURCES_PATH"), Ref("STACK_ENV_FILENAME")],
    ),
    ("SHELL_RESOURCES_PATH", &[Ref("ROOT_DIR_PATH")]),
    (
        "SHELL_HISTORY_PATH",
        &[Ref("SHELL_RESOURCES_PATH"), Lit(".mrsm_history")],
    ),
    (
        "PYTHON_RESOURCES_PATH",
        &[Ref("INTERNAL_RESOURCES_PATH"), Lit("python")],
    ),
    (
        "API_RESOURCES_PATH",
        &[Ref("PACKAGE_ROOT_PATH"), Lit("api"), Lit("resources")],
    ),
    (
        "API_STATIC_PATH",
        &[Ref("API_RESOURCES_PATH"), Lit("static")],
    ),
    (
        "API_TEMPLATES_PATH",
        &[Ref("API_RESOURCES_PATH"), Lit("templates")],
    ),
    (
        "API_CONFIG_RESOURCES_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("api")],
    ),
    (
        "API_SECRET_KEY_PATH",
        &[Ref("API_CONFIG_RESOURCES_PATH"), Lit(".api_secret_key")],
    ),
    (
        "API_UVICORN_RESOURCES_PATH",
        &[Ref("API_CONFIG_RESOURCES_PATH"), Lit("uvicorn")],
    ),
    (
        "API_UVICORN_CONFIG_PATH",
        &[
            Ref("API_UVICORN_RESOURCES_PATH"),
            Lit(".thread_config.json"),
        ],
    ),
    (
        "WEBTERM_INTERNAL_RESOURCES_PATH",
        &[Ref("INTERNAL_RESOURCES_PATH"), Lit("webterm")],
    ),
    (
        "CACHE_RESOURCES_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit(".cache")],
    ),
    (
        "PIPES_CACHE_RESOURCES_PATH",
        &[Ref("CACHE_RESOURCES_PATH"), Lit("pipes")],
    ),
    (
        "USERS_CACHE_RESOURCES_PATH",
        &[Ref("CACHE_RESOURCES_PATH"), Lit("users")],
    ),
    (
        "VENVS_CACHE_RESOURCES_PATH",
        &[Ref("CACHE_RESOURCES_PATH"), Lit("venvs")],
    ),
    (
        "SQL_CONN_CACHE_RESOURCES_PATH",
        &[Ref("CACHE_RESOURCES_PATH"), Lit("sql")],
    ),
    (
        "UPDATES_RESOURCES_PATH",
        &[Ref("CACHE_RESOURCES_PATH"), Lit("updates")],
    ),
    (
        "UPDATES_CACHE_PATH",
        &[Ref("UPDATES_RESOURCES_PATH"), Lit("cache.json")],
    ),
    (
        "PLUGIN_UPDATES_CACHE_PATH",
        &[Ref("UPDATES_RESOURCES_PATH"), Lit("plugins.json")],
    ),
    (
        "UPDATES_LOCK_PATH",
        &[Ref("UPDATES_RESOURCES_PATH"), Lit(".updates.lock")],
    ),
    (
        "CLI_RESOURCES_PATH",
        &[Ref("INTERNAL_RESOURCES_PATH"), Lit("cli")],
    ),
    (
        "CLI_LOGS_RESOURCES_PATH",
        &[Ref("CLI_RESOURCES_PATH"), Lit("logs")],
    ),
    (
        "PLUGINS_RESOURCES_PATH",
        &[Ref("INTERNAL_RESOURCES_PATH"), Lit("plugins")],
    ),
    (
        "PLUGINS_INTERNAL_LOCK_PATH",
        &[Ref("INTERNAL_RESOURCES_PATH"), Lit("plugins.lock")],
    ),
    (
        "PLUGINS_PACKAGES_INTERNAL_PATH",
        &[Ref("INTERNAL_RESOURCES_PATH"), Lit("packaged_plugins")],
    ),
    (
        "PLUGINS_ARCHIVES_RESOURCES_PATH",
        &[Ref("PLUGINS_RESOURCES_PATH"), Lit(".archives")],
    ),
    (
        "PLUGINS_TEMP_RESOURCES_PATH",
        &[Ref("PLUGINS_RESOURCES_PATH"), Lit(".tmp")],
    ),
    (
        "PLUGINS_INIT_PATH",
        &[Ref("PLUGINS_RESOURCES_PATH"), Lit("__init__.py")],
    ),
    (
        "PLUGINS_INJECTED_RESOURCES_PATH",
        &[Ref("PLUGINS_RESOURCES_PATH"), Lit(".injected")],
    ),
    ("DB_RESOURCES_PATH", &[Ref("ROOT_DIR_PATH"), Lit("db")]),
    (
        "DB_INIT_RESOURCES_PATH",
        &[Ref("DB_RESOURCES_PATH"), Lit("initdb")],
    ),
    (
        "DB_CREATE_EXTENSIONS_PATH",
        &[Ref("DB_INIT_RESOURCES_PATH"), Lit("create-extensions.sql")],
    ),
    (
        "SQLITE_RESOURCES_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("sqlite")],
    ),
    (
        "SQLITE_DB_PATH",
        &[Ref("SQLITE_RESOURCES_PATH"), Lit("mrsm_local.db")],
    ),
    (
        "BACKUP_RESOURCES_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("backup")],
    ),
    (
        "BACKUP_DATABASE_PATH",
        &[Ref("BACKUP_RESOURCES_PATH"), Lit("backup.sql")],
    ),
    (
        "DUCKDB_RESOURCES_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("duckdb")],
    ),
    (
        "DUCKDB_PATH",
        &[Ref("DUCKDB_RESOURCES_PATH"), Lit("duck.db")],
    ),
    (
        "GRAFANA_RESOURCES_PATH",
        &[
            Ref("STACK_RESOURCES_PATH"),
            Lit("grafana"),
            Lit("resources"),
        ],
    ),
    (
        "GRAFANA_DATASOURCE_PATH",
        &[
            Ref("GRAFANA_RESOURCES_PATH"),
            Lit("provisioning"),
            Lit("datasources"),
            Lit("datasource.yaml"),
        ],
    ),
    (
        "GRAFANA_DASHBOARD_PATH",
        &[
            Ref("GRAFANA_RESOURCES_PATH"),
            Lit("provisioning"),
            Lit("dashboards"),
            Lit("dashboard.yaml"),
        ],
    ),
    (
        "MOSQUITTO_RESOURCES_PATH",
        &[
            Ref("STACK_RESOURCES_PATH"),
            Lit("mosquitto"),
            Lit("resources"),
        ],
    ),
    (
        "MOSQUITTO_CONFIG_PATH",
        &[Ref("MOSQUITTO_RESOURCES_PATH"), Lit("mosquitto.conf")],
    ),
    (
        "PORTABLE_CHECK_READLINE_PATH",
        &[
            Ref("SHELL_RESOURCES_PATH"),
            Lit(".readline_attempted_install"),
        ],
    ),
    (
        "DAEMON_RESOURCES_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("jobs")],
    ),
    ("LOGS_RESOURCES_PATH", &[Ref("ROOT_DIR_PATH"), Lit("logs")]),
    (
        "DAEMON_ERROR_LOG_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("daemon_errors.log")],
    ),
    (
        "CHECK_JOBS_LOCK_PATH",
        &[Ref("INTERNAL_RESOURCES_PATH"), Lit("check-jobs.lock")],
    ),
    (
        "SYSTEMD_RESOURCES_PATH",
        &[Ref("DOT_CONFIG_DIR_PATH"), Lit("systemd")],
    ),
    (
        "SYSTEMD_USER_RESOURCES_PATH",
        &[Ref("SYSTEMD_RESOURCES_PATH"), Lit("user")],
    ),
    (
        "SYSTEMD_ROOT_RESOURCES_PATH",
        &[Ref("ROOT_DIR_PATH"), Lit("systemd")],
    ),
    (
        "SYSTEMD_JOBS_RESOURCES_PATH",
        &[Ref("SYSTEMD_ROOT_RESOURCES_PATH"), Lit("services")],
    ),
    (
        "SYSTEMD_LOGS_RESOURCES_PATH",
        &[Ref("SYSTEMD_ROOT_RESOURCES_PATH"), Lit("logs")],
    ),
];

/// The resolved Meerschaum paths for one root directory.
#[derive(Debug, Clone, PartialEq)]
pub struct Paths {
    /// `PACKAGE_ROOT_PATH`: the installed Python package directory, if known.
    pub package_root: Option<PathBuf>,
    /// `DOT_CONFIG_DIR_PATH`: `$XDG_CONFIG_HOME` or `~/.config` (`%AppData%` on Windows).
    pub dot_config_dir: PathBuf,
    /// `ROOT_DIR_PATH`.
    pub root_dir: PathBuf,
    /// `CONFIG_DIR_PATH`.
    pub config_dir: PathBuf,
    /// `PLUGINS_DIR_PATHS`.
    pub plugins_dirs: Vec<PathBuf>,
    /// `VIRTENV_RESOURCES_PATH` when `MRSM_VENVS_DIR` is set; otherwise derived from the root.
    pub venvs_dir: Option<PathBuf>,
}

/// The value of one path name: most are paths, `*_FILENAME` names are plain strings.
#[derive(Debug, Clone, PartialEq)]
pub enum PathValue {
    /// A filesystem path.
    Path(PathBuf),
    /// A file name (`STACK_COMPOSE_FILENAME`, `STACK_ENV_FILENAME`).
    Name(String),
}

impl PathValue {
    /// The value as a string with forward slashes (`Path.as_posix()`).
    pub fn as_posix(&self) -> String {
        match self {
            PathValue::Path(p) => posix(p),
            PathValue::Name(n) => n.clone(),
        }
    }
}

/// `Path.as_posix()`.
pub fn posix(p: &Path) -> String {
    p.to_string_lossy().replace('\\', "/")
}

/// Python's `Path(p).resolve()` (non-strict): make absolute, resolve symlinks in the existing
/// prefix, and normalize `.` / `..` lexically for the rest.
pub fn resolve_path(p: &Path) -> PathBuf {
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(p)
    };
    let mut resolved = PathBuf::new();
    for comp in abs.components() {
        match comp {
            Component::Prefix(_) | Component::RootDir => resolved.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Normal(seg) => {
                resolved.push(seg);
                if let Ok(canon) = std::fs::canonicalize(&resolved) {
                    resolved = canon;
                }
            }
        }
    }
    resolved
}

/// Parse a plugins-directories string (`MRSM_PLUGINS_DIR`): a JSON list or a `:`-separated list.
pub fn parse_plugins_dirs(value: &str) -> Result<Vec<PathBuf>, ConfigError> {
    let paths: Vec<PathBuf> = if value.trim_start().starts_with('[') {
        match serde_json::from_str::<Vec<String>>(value) {
            Ok(items) => items.iter().map(|s| resolve_path(Path::new(s))).collect(),
            Err(_) => Vec::new(),
        }
    } else {
        value
            .split(':')
            .filter(|s| !s.is_empty())
            .map(|s| resolve_path(Path::new(s)))
            .collect()
    };
    if paths.is_empty() {
        return Err(ConfigError::InvalidPath {
            var: "MRSM_PLUGINS_DIR".to_string(),
            path: value.to_string(),
        });
    }
    Ok(dedupe_plugins_dirs(paths))
}

/// Port of `_process_plugins_dir_paths`.
///
/// PARITY: for each path that appears more than once, only its *first* occurrence is removed
/// (`list.remove`), so `[a, b, a]` becomes `[b, a]` and a path listed three times stays
/// duplicated (`config/_paths.py:105`).
pub fn dedupe_plugins_dirs(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen: Vec<PathBuf> = Vec::new();
    let mut to_remove: Vec<PathBuf> = Vec::new();
    for p in &paths {
        if seen.contains(p) && !to_remove.contains(p) {
            to_remove.push(p.clone());
        }
        seen.push(p.clone());
    }
    for p in to_remove {
        if let Some(i) = paths.iter().position(|x| *x == p) {
            paths.remove(i);
        }
    }
    paths
}

impl Paths {
    /// Derive the paths from environment variables, like importing `meerschaum.config._paths`.
    ///
    /// `env` looks up a variable; `home` is the user's home directory; `windows` selects the
    /// Windows defaults. `MRSM_ROOT_DIR` and `MRSM_CONFIG_DIR` must exist (Python exits
    /// otherwise). A missing `MRSM_VENVS_DIR` is created, as in Python.
    pub fn from_env(
        env: &dyn Fn(&str) -> Option<String>,
        home: &Path,
        windows: bool,
        package_root: Option<PathBuf>,
    ) -> Result<Paths, ConfigError> {
        let dot_config_dir = if windows {
            env("AppData")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join("AppData").join("Roaming"))
        } else {
            env("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".config"))
        };
        let default_root = dot_config_dir.join(if windows { "Meerschaum" } else { "meerschaum" });

        let root_dir = match env("MRSM_ROOT_DIR") {
            Some(r) => {
                let p = resolve_path(Path::new(&r));
                if !p.exists() {
                    return Err(ConfigError::InvalidPath {
                        var: "MRSM_ROOT_DIR".to_string(),
                        path: posix(&p),
                    });
                }
                p
            }
            None => default_root,
        };

        let config_dir = match env("MRSM_CONFIG_DIR") {
            Some(c) => {
                let p = resolve_path(Path::new(&c));
                if !p.exists() {
                    return Err(ConfigError::InvalidPath {
                        var: "MRSM_CONFIG_DIR".to_string(),
                        path: posix(&p),
                    });
                }
                p
            }
            None => root_dir.join("config"),
        };

        let plugins_dirs = match env("MRSM_PLUGINS_DIR") {
            Some(v) => parse_plugins_dirs(&v)?,
            None => vec![root_dir.join("plugins")],
        };

        let venvs_dir = match env("MRSM_VENVS_DIR") {
            Some(v) => {
                let p = resolve_path(Path::new(&v));
                if !p.exists() && std::fs::create_dir_all(&p).is_err() {
                    Some(resolve_path(&root_dir.join("venvs")))
                } else {
                    Some(p)
                }
            }
            None => None,
        };

        Ok(Paths {
            package_root,
            dot_config_dir,
            root_dir,
            config_dir,
            plugins_dirs,
            venvs_dir,
        })
    }

    /// Paths for a root directory with no other overrides (as if only `MRSM_ROOT_DIR` were set,
    /// without resolving or validating it).
    pub fn for_root(root: impl Into<PathBuf>) -> Paths {
        let root_dir = root.into();
        Paths {
            package_root: None,
            dot_config_dir: PathBuf::from("~/.config"),
            config_dir: root_dir.join("config"),
            plugins_dirs: vec![root_dir.join("plugins")],
            root_dir,
            venvs_dir: None,
        }
    }

    /// Port of `set_root`: change `ROOT_DIR_PATH` only.
    ///
    /// PARITY: `CONFIG_DIR_PATH` and `PLUGINS_DIR_PATHS` keep their old values
    /// (`config/_paths.py:205`).
    pub fn set_root(&mut self, root: &Path) {
        self.root_dir = resolve_path(root);
    }

    /// Every path name, in the order of the Python `paths` dictionary.
    pub fn names() -> Vec<&'static str> {
        TEMPLATES.iter().map(|(name, _)| *name).collect()
    }

    fn base(&self, name: &str) -> Option<PathValue> {
        match name {
            "PACKAGE_ROOT_PATH" => self.package_root.clone().map(PathValue::Path),
            "ROOT_DIR_PATH" => Some(PathValue::Path(self.root_dir.clone())),
            "CONFIG_DIR_PATH" => Some(PathValue::Path(self.config_dir.clone())),
            "DOT_CONFIG_DIR_PATH" => Some(PathValue::Path(self.dot_config_dir.clone())),
            "VIRTENV_RESOURCES_PATH" => self.venvs_dir.clone().map(PathValue::Path),
            _ => None,
        }
    }

    /// The value of a path name (`paths.<NAME>`), or `None` for unknown names (and for names
    /// derived from an unknown `PACKAGE_ROOT_PATH`).
    pub fn get(&self, name: &str) -> Option<PathValue> {
        if let Some(v) = self.base(name) {
            return Some(v);
        }
        if name == "PLUGINS_DIR_PATHS" {
            return None;
        }
        let (_, parts) = TEMPLATES.iter().find(|(n, _)| *n == name)?;
        if parts.is_empty() {
            return None;
        }
        let mut path = PathBuf::new();
        for part in parts.iter() {
            match part {
                Lit(s) => path.push(s),
                Ref(r) => path.push(self.get(r)?.as_posix()),
            }
        }
        if name.contains("FILENAME") {
            return Some(PathValue::Name(posix(&path)));
        }
        Some(PathValue::Path(path))
    }

    /// The path for a name, panicking on unknown names. Convenience for known-good constants.
    pub fn path(&self, name: &str) -> PathBuf {
        match self.get(name) {
            Some(PathValue::Path(p)) => p,
            Some(PathValue::Name(n)) => PathBuf::from(n),
            None => panic!("unknown or unavailable path name '{name}'"),
        }
    }

    /// Resolve a path name and create its directory like Python's attribute access does for
    /// `*RESOURCES_PATH` names and `CONFIG_DIR_PATH`.
    pub fn ensure(&self, name: &str) -> std::io::Result<Option<PathValue>> {
        let value = self.get(name);
        if let Some(PathValue::Path(p)) = &value {
            if name.ends_with("RESOURCES_PATH") || name == "CONFIG_DIR_PATH" {
                std::fs::create_dir_all(p)?;
            }
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_paths() {
        let p = Paths::for_root("/r");
        assert_eq!(p.get("SHELL_RESOURCES_PATH").unwrap().as_posix(), "/r");
        assert_eq!(
            p.get("STACK_COMPOSE_PATH").unwrap().as_posix(),
            "/r/stack/docker-compose.yaml"
        );
        assert_eq!(
            p.get("STACK_COMPOSE_FILENAME").unwrap(),
            PathValue::Name("docker-compose.yaml".into())
        );
        assert_eq!(
            p.get("VIRTENV_RESOURCES_PATH").unwrap().as_posix(),
            "/r/venvs"
        );
        assert!(p.get("API_STATIC_PATH").is_none());
    }

    #[test]
    fn plugins_dedupe_quirk() {
        let a = PathBuf::from("/a");
        let b = PathBuf::from("/b");
        assert_eq!(
            dedupe_plugins_dirs(vec![a.clone(), b.clone(), a.clone()]),
            vec![b.clone(), a.clone()]
        );
        assert_eq!(
            dedupe_plugins_dirs(vec![a.clone(), a.clone(), a.clone()]),
            vec![a.clone(), a.clone()]
        );
    }
}
