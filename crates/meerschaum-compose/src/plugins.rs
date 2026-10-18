//! Plugins in a compose project: a port of `meerschaum/compose/utils/plugins.py` and of the
//! obsolete-plugin detection in `meerschaum/compose/__init__.py`.

use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use meerschaum_core::pyfmt::py_str;
use meerschaum_core::static_config::static_config;
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::Value;

use crate::command::{CommandRunner, RunOptions, run_mrsm_command};
use crate::env::EnvDict;
use crate::error::{ComposeError, Messages};
use crate::pipes::ProjectConfig;
use crate::project::{Host, Project, Session, env_dict_as_process_env};

/// The warning printed once per process when the superseded `compose` plugin is installed.
pub const COMPOSE_PLUGIN_WARNING: &str = "The 'compose' plugin is superseded by Meerschaum's \
built-in compose action. The built-in action is being used; uninstall the plugin with \
`mrsm uninstall plugin compose`.";

/// The notice for `mrsm install plugin compose`, which is a no-op in v5 (§11.2).
pub const INSTALL_COMPOSE_NOTICE: &str = "Compose is built into Meerschaum; the 'compose' plugin \
is not installed. Remove `mrsm install plugin compose` from your bootstrap scripts.";

/// The plugin names found in plugins directories, like `get_plugins_names()` after
/// `sync_plugins_symlinks()`: `*.py` modules and package directories **with an `__init__.py`**,
/// skipping hidden entries, `__pycache__` and `__init__.py`.
///
/// Failed imports (decided against released v4.1.4): `get_plugins_names()` keeps every plugin
/// `Plugin.is_installed()` accepts, and that is a file-system test (`__file__ is not None`: a
/// `<name>/__init__.py` or `<name>.py` in the plugins directory), never an import. So a plugin
/// whose import raises is listed (`mrsm show plugins` prints it, `install required` and
/// `setup plugins` count it, and the import error is only warned about when something imports
/// it), and a directory without `__init__.py` is not a plugin. This function therefore needs no
/// plugin runtime. Names are unique, in directory order with each directory's entries sorted.
///
/// v5 adds native (C-ABI) plugins: a directory holding a `plugin.toml` counts as installed.
/// A native plugin shadows a Python plugin of the same name, so the name list is the same either
/// way.
pub fn plugin_names_in_dirs(dirs: &[PathBuf]) -> Vec<String> {
    let mut names = Vec::new();
    for dir in dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        let mut items: Vec<(String, PathBuf)> = entries
            .filter_map(Result::ok)
            .map(|e| (e.file_name().to_string_lossy().to_string(), e.path()))
            .collect();
        items.sort();
        for (name, path) in items {
            if name.starts_with('.') || name == "__pycache__" || name == "__init__.py" {
                continue;
            }
            let plugin = if path.is_dir() {
                (path.join("__init__.py").is_file() || path.join("plugin.toml").is_file())
                    .then_some(name)
            } else {
                name.strip_suffix(".py").map(String::from)
            };
            if let Some(plugin) = plugin
                && !names.contains(&plugin)
            {
                names.push(plugin);
            }
        }
    }
    names
}

/// The plugins directories an environment points at (`MRSM_PLUGINS_DIR`, a path or a JSON
/// list; `<MRSM_ROOT_DIR>/plugins` by default).
pub fn plugins_dirs_from_env(env: &EnvDict) -> Vec<PathBuf> {
    match env.get("MRSM_PLUGINS_DIR") {
        Some(Value::Str(s)) if !s.is_empty() => {
            meerschaum_core::config::paths::parse_plugins_dirs(s).unwrap_or_default()
        }
        _ => env
            .get("MRSM_ROOT_DIR")
            .map(|v| PathBuf::from(py_str(v)).join("plugins"))
            .into_iter()
            .collect(),
    }
}

/// `get_installed_plugins(compose_config)`: the plugins visible in the project environment.
pub fn get_installed_plugins(project: &Project) -> Result<Vec<String>, ComposeError> {
    let env = project.env_dict()?;
    // PARITY: Python applies the environment with `replace_env()`, which raises `TypeError`
    // for a non-string value from `environment:`.
    env_dict_as_process_env(&env)?;
    Ok(plugin_names_in_dirs(&plugins_dirs_from_env(&env)))
}

/// `check_and_install_plugins(compose_config)`: create the plugins directories and install the
/// missing plugins (every plugin of a repository is reinstalled when any of them is missing).
pub fn check_and_install_plugins(
    project: &Project,
    project_config: &ProjectConfig,
    existing: Option<Vec<String>>,
    debug: bool,
    runner: &mut dyn CommandRunner,
    msgs: &mut Messages,
) -> Result<SuccessTuple, ComposeError> {
    let configured = match project.config.get("plugins") {
        None => return Ok(SuccessTuple::success()),
        Some(v) if !v.truthy() => return Ok(SuccessTuple::success()),
        Some(Value::List(items)) => items.iter().map(py_str).collect::<Vec<_>>(),
        Some(_) => return Ok(SuccessTuple::fail("Required plugins must be a list.")),
    };

    let default_repository = project
        .config_block()
        .get("meerschaum")
        .and_then(Value::as_map)
        .and_then(|m| m.get("repository"))
        .cloned()
        .or_else(|| project_config.get(&["meerschaum", "repository"]))
        .map(|v| py_str(&v))
        .unwrap_or_default();

    for path in project.plugins_dirs() {
        if !path.exists()
            && let Err(e) = std::fs::create_dir_all(&path)
        {
            return Ok(SuccessTuple::fail(format!(
                "Failed to create plugins path '{}': {e}",
                path.display()
            )));
        }
    }

    let separator = static_config()
        .get(&["plugins", "repo_separator"])
        .and_then(Value::as_str)
        .unwrap_or("@")
        .to_string();
    let parts: Vec<Vec<String>> = configured
        .iter()
        .map(|name| name.split(separator.as_str()).map(String::from).collect())
        .collect();
    let mut required: IndexMap<String, Vec<String>> = IndexMap::new();
    for p in &parts {
        let repo = p
            .get(1)
            .cloned()
            .unwrap_or_else(|| default_repository.clone());
        required.entry(repo).or_default().push(p[0].clone());
    }

    let existing = match existing.filter(|e| !e.is_empty()) {
        Some(e) => e,
        None => get_installed_plugins(project)?,
    };
    let to_install: Vec<&String> = parts
        .iter()
        .map(|p| &p[0])
        .filter(|name| !existing.contains(name))
        .collect();
    if to_install.is_empty() {
        return Ok(SuccessTuple::ok("Required plugins are already installed."));
    }

    let mut success = true;
    let mut msg = String::new();
    // PARITY: when any configured plugin is missing, every plugin of its repository group is
    // installed again, not only the missing member (`compose/utils/plugins.py`
    // `check_and_install_plugins`: `install plugins <all names of the repo> -r <repo>`). The
    // command runs the native `install plugins` of this binary under the project environment.
    for (repo, names) in &required {
        let mut args = vec!["install".to_string(), "plugins".to_string()];
        args.extend(names.iter().cloned());
        if !repo.is_empty() {
            args.push("-r".into());
            args.push(repo.clone());
        }
        let result = run_mrsm_command(
            &args,
            project,
            RunOptions {
                debug,
                subprocess: Some(true),
                ..RunOptions::default()
            },
            runner,
            msgs,
        )?;
        if !result.success {
            msgs.warn(result.message.clone());
        }
        success = success && result.success;
        msg.push_str(&result.message);
    }
    let msg = if success {
        "Success".to_string()
    } else {
        msg.trim_end().to_string()
    };
    Ok(SuccessTuple {
        success,
        message: msg,
    })
}

/// `has_compose_plugin()`: whether the superseded `compose` plugin is installed in the host's
/// plugins directories.
pub fn has_compose_plugin(host: &Host) -> bool {
    host.config
        .paths()
        .plugins_dirs
        .iter()
        .any(|dir| is_plugin_in_dir(dir, "compose"))
}

fn is_plugin_in_dir(dir: &Path, name: &str) -> bool {
    dir.join(format!("{name}.py")).is_file() || dir.join(name).join("__init__.py").is_file()
}

/// `warn_if_compose_plugin_installed()`: warn once per process.
pub fn warn_if_compose_plugin_installed(host: &Host, session: &mut Session, msgs: &mut Messages) {
    if session.warned_about_plugin || !has_compose_plugin(host) {
        return;
    }
    msgs.warn(COMPOSE_PLUGIN_WARNING);
    session.warned_about_plugin = true;
}

/// `mrsm install plugin(s) ...`: drop `compose` (a no-op with a notice in v5, §11.2) and return
/// the remaining plugin names with the notice, if any.
pub fn filter_install_plugins(names: &[String]) -> (Vec<String>, Option<&'static str>) {
    let separator = static_config()
        .get(&["plugins", "repo_separator"])
        .and_then(Value::as_str)
        .unwrap_or("@");
    let is_compose = |n: &String| n.split(separator).next() == Some("compose");
    let notice = names
        .iter()
        .any(is_compose)
        .then_some(INSTALL_COMPOSE_NOTICE);
    (
        names.iter().filter(|n| !is_compose(n)).cloned().collect(),
        notice,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(path: &Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "").unwrap();
    }

    #[test]
    fn discovery_never_imports_so_failed_imports_stay_listed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(
            root.join("broken.py"),
            "import definitely_not_a_module_xyz\n",
        )
        .unwrap();
        touch(&root.join("ok.py"));
        touch(&root.join("pkg/__init__.py"));
        // v4 drops a directory without `__init__.py` (`Plugin.__file__` is `None`).
        touch(&root.join("no_init/mod.py"));
        touch(&root.join("_private.py"));
        touch(&root.join(".hidden.py"));
        touch(&root.join("README.md"));
        touch(&root.join("__init__.py"));
        touch(&root.join("__pycache__/x.pyc"));
        // A native plugin descriptor counts; it shadows a Python plugin of the same name.
        touch(&root.join("native_one/plugin.toml"));
        touch(&root.join("native_one.py"));
        assert_eq!(
            plugin_names_in_dirs(&[root.to_path_buf()]),
            vec!["_private", "broken", "native_one", "ok", "pkg"]
        );
    }

    #[test]
    fn the_first_directory_orders_the_names() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        touch(&a.path().join("same.py"));
        touch(&b.path().join("same.py"));
        touch(&b.path().join("a_only.py"));
        assert_eq!(
            plugin_names_in_dirs(&[a.path().to_path_buf(), b.path().to_path_buf()]),
            vec!["same", "a_only"]
        );
    }

    #[test]
    fn install_compose_is_a_noop() {
        let names = vec!["compose".to_string(), "noaa@api:custom".to_string()];
        let (rest, notice) = filter_install_plugins(&names);
        assert_eq!(rest, vec!["noaa@api:custom".to_string()]);
        assert!(notice.is_some());
        let (rest, notice) = filter_install_plugins(&rest);
        assert_eq!(rest.len(), 1);
        assert!(notice.is_none());
    }
}
