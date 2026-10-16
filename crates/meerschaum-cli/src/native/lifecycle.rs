//! The plugin lifecycle actions, native: `setup plugins`, `install required`, `install packages`
//! and `install plugins`, ported from `meerschaum/actions/setup.py` and `install.py`, with the
//! `Plugin` methods they call (`setup`, `install`, `_install`, `install_dependencies`,
//! `get_required_plugins`) and `APIConnector.install_plugin`.
//!
//! Python only runs a plugin's own code: `setup()` goes to the worker's `setup` operation (so
//! it runs in the plugin's venv, as in v4); a native (C-ABI) plugin's `setup` goes through the
//! plugin SDK's `call` and starts no Python at all. Everything else (selection, the repository
//! download, archive extraction, version rules, `.mrsm_origins.json`, the venv and package
//! installation, rollback, the manifest refresh and every message) is Rust.
//!
//! # Differences from v4
//!
//! - `reload` (v4 imports every plugin again after an install) is the manifest refresh: only
//!   changed plugin trees are imported again and the worker pool is recycled when they were.
//!   The reload step prints nothing and returns v4's `(True, "Success")`.
//! - A plugin's `required` list comes from the manifest when its import succeeded, else from its
//!   source text (see [`plugin_files::static_required`]); v4 always reads the text.
//! - `--debug` prints the installer's command line (as v4) but not v4's `dprint` traces.
//! - A Python plugin's `print()` in `setup()` goes to the worker's stderr, not stdout.
//! - The version of an installed plugin is read from its source (`__version__ = '...'`) and not
//!   by importing it.
//! - `install plugins` keeps v4's rule that a failed install still ends in `(True, "Success")`
//!   (the per-plugin result is printed).

use std::path::PathBuf;

use meerschaum_api::APIConnector;
use meerschaum_api::client::RequestOptions;
use meerschaum_core::SuccessTuple;
use meerschaum_core::static_config::static_config;
use meerschaum_worker::manifest::Manifest;
use serde_json::{Value as Json, json};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::value::ArgsMap;

use super::pip::{PipEnv, PipInstall, pip_install};
use super::plugin_files::{self, Rollback};
use super::{flag, info, items_str, plural, print_tuple, session, string, warn, words};

/// `error(msg)`: the exception v4 raises (`Exception` with the icon-prefixed text).
fn exception(message: impl AsRef<str>) -> ActionError {
    ActionError::new("Exception", format!("\n 🛑 {}", message.as_ref()))
}

/// `info(msg, icon=False)`.
fn info_plain(rt: &mut Runtime<'_>, message: &str) {
    let text = rt.with_formatter(|f| f.info(message, false));
    rt.print(&text);
}

fn repo_separator() -> String {
    static_config().plugins.repo_separator.clone()
}

/// Everything the lifecycle actions read from the runtime once.
struct Context {
    env: PipEnv,
    plugins_dirs: Vec<PathBuf>,
    temp_dir: PathBuf,
    updates_cache: Option<PathBuf>,
    origins_filename: String,
    debug: bool,
    force: bool,
    separator: String,
}

impl Context {
    fn new(rt: &mut Runtime<'_>, args: &ArgsMap) -> Context {
        let env = PipEnv::new(rt);
        let paths = rt.config().paths();
        let temp_dir = paths
            .get("PLUGINS_TEMP_RESOURCES_PATH")
            .map(|p| PathBuf::from(p.as_posix()))
            .unwrap_or_else(|| {
                paths
                    .root_dir
                    .join(".internal")
                    .join("plugins")
                    .join(".tmp")
            });
        let updates_cache = paths
            .get("PLUGIN_UPDATES_CACHE_PATH")
            .map(|p| PathBuf::from(p.as_posix()));
        Context {
            env,
            plugins_dirs: paths.plugins_dirs.clone(),
            temp_dir,
            updates_cache,
            origins_filename: static_config().plugins.origins_filename.clone(),
            debug: flag(args, "debug"),
            force: flag(args, "force"),
            separator: repo_separator(),
        }
    }
}

/// `parse_repo_keys(repository)` followed by the connector's construction (which fails for a
/// label nobody configured).
fn repo_connector(
    rt: &mut Runtime<'_>,
    args: &ArgsMap,
) -> Result<(String, APIConnector), ActionError> {
    let keys = super::show::repo_keys(args, rt)?;
    connector_for(rt, &keys).map(|c| (keys, c))
}

fn connector_for(rt: &mut Runtime<'_>, keys: &str) -> Result<APIConnector, ActionError> {
    let label = keys.split(':').nth(1).unwrap_or("mrsm");
    APIConnector::from_config(rt.config(), label)
        .map_err(|_| exception(format!("Unable to parse connector keys '{keys}'")))
}

/// `is_valid_connector_keys(keys)`: the type and label are in `meerschaum:connectors`.
fn valid_connector_keys(rt: &mut Runtime<'_>, keys: &str) -> bool {
    let mut parts = keys.split(':');
    let kind = parts.next().unwrap_or_default();
    let label = parts.next().unwrap_or("main");
    rt.config()
        .get(&["meerschaum", "connectors", kind, label])
        .is_some()
}

/// The installed plugin names and which of them are native.
fn is_installed(rt: &mut Runtime<'_>, ctx: &Context, name: &str) -> bool {
    is_native(rt, name) || plugin_files::plugin_file(&ctx.plugins_dirs, name).is_some()
}

fn is_native(rt: &mut Runtime<'_>, name: &str) -> bool {
    let session = session(rt);
    session
        .native_plugins(rt.config())
        .map(|natives| natives.contains(name))
        .unwrap_or(false)
}

/// The names a lifecycle action works on: the ones asked for, else every installed plugin.
fn selected_plugins(rt: &mut Runtime<'_>, ctx: &Context, args: &ArgsMap) -> Vec<String> {
    let action = words(args, "action");
    if !action.is_empty() {
        return action;
    }
    let mut names = plugin_files::installed_plugin_names(&ctx.plugins_dirs);
    // Plugins Python found elsewhere (entry points) are in the manifest.
    if let Ok(manifest) = crate::python::manifest(rt) {
        for name in manifest.plugins.keys() {
            if !names.contains(name) {
                names.push(name.clone());
            }
        }
    }
    names
}

/// Validate the plugin trees now (what `reload` does in v4 after an install).
fn refresh(rt: &mut Runtime<'_>) -> Option<std::sync::Arc<Manifest>> {
    match crate::python::refresh_manifest(rt) {
        Ok(manifest) => Some(manifest),
        Err(error) => {
            rt.eprint(&format!(
                "Unable to refresh plugin manifest: {}\n",
                error.message
            ));
            None
        }
    }
}

// ---------------------------------------------------------------------------------------------
// setup plugins
// ---------------------------------------------------------------------------------------------

/// The keyword arguments `setup()` receives: every parsed argument except the ones the action
/// itself names (`action`, `repository`), as `Plugin.setup(**kw)` gets them.
fn setup_kwargs(args: &ArgsMap) -> meerschaum_core::Map {
    args.iter()
        .filter(|(key, _)| !matches!(key.as_str(), "action" | "repository"))
        .map(|(key, value)| (key.clone(), super::py_to_value(value)))
        .collect()
}

/// A native plugin's `setup(**kw)`: the SDK's `setup` method receives
/// `{"args": [], "kwargs": {...}}` and answers `true`, `false` or `[success, message]`. A plugin
/// that does not implement it is "assume success" (v4: no `setup()` function).
fn native_setup(
    rt: &mut Runtime<'_>,
    name: &str,
    kwargs: &meerschaum_core::Map,
) -> Result<SuccessTuple, ActionError> {
    let session = session(rt);
    let natives = session.native_plugins(rt.config())?;
    let plugin = natives
        .plugin(name)
        .map_err(|e| ActionError::new("ImportError", e.to_string()))?;
    let context = json!({"args": [], "kwargs": serde_json::to_value(kwargs).unwrap_or(Json::Null)});
    Ok(interpret_native_setup(
        name,
        plugin.call_json("setup", context),
    ))
}

/// `Plugin.setup`'s handling of the plugin's return value.
fn interpret_native_setup(name: &str, reply: meerschaum_plugin::Result<Json>) -> SuccessTuple {
    match reply {
        Err(meerschaum_plugin::Error::Unsupported(_)) => SuccessTuple::success(),
        // v4: `except Exception as e: return False, str(e)`.
        Err(error) => SuccessTuple::fail(error.to_string()),
        Ok(Json::Array(items)) if items.len() == 2 => SuccessTuple {
            success: items[0].as_bool().unwrap_or(false),
            message: items[1].as_str().unwrap_or_default().to_string(),
        },
        Ok(Json::Bool(value)) => SuccessTuple {
            success: value,
            message: format!("Setup for Plugin '{name}' did not return a message."),
        },
        Ok(Json::Null) => SuccessTuple::fail(format!("Setup for Plugin '{name}' returned None.")),
        Ok(other) => SuccessTuple::fail(format!(
            "Unknown return value from setup for Plugin '{name}': {other}"
        )),
    }
}

/// `Plugin(name).setup(**kw)` in the worker.
fn python_setup(
    rt: &mut Runtime<'_>,
    name: &str,
    kwargs: &meerschaum_core::Map,
) -> Result<SuccessTuple, ActionError> {
    let session = session(rt);
    let pool = session.pool(rt.config());
    session
        .block_on(meerschaum_worker::plugins::setup_plugin(
            &pool,
            name,
            &[],
            kwargs,
        ))
        .map_err(|e| ActionError::new("WorkerError", e.to_string()))
}

/// `setup plugins [names]` (`actions/setup.py::_setup_plugins`).
pub fn setup_plugins(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let ctx = Context::new(rt, args);
    repo_connector(rt, args)?;
    let kwargs = setup_kwargs(args);
    let names = selected_plugins(rt, &ctx, args);

    let mut success_count = 0usize;
    let mut fail_count = 0usize;
    for plugin_name in names {
        let (name, _) =
            plugin_files::split_repo(&plugin_name, &ctx.separator).map_err(exception)?;
        if !is_installed(rt, &ctx, &name) {
            warn(
                rt,
                &format!("Plugin '{name}' is not installed. Skipping..."),
            );
            continue;
        }
        let result = if is_native(rt, &name) {
            native_setup(rt, &name, &kwargs)?
        } else {
            python_setup(rt, &name, &kwargs)?
        };
        if !result.success {
            print_tuple(rt, &result, TupleArgs::default());
            warn(rt, &format!("Failed to set up plugin '{name}'."));
            fail_count += 1;
        } else {
            success_count += 1;
        }
    }
    refresh(rt);

    let total = success_count + fail_count;
    Ok(SuccessTuple {
        success: success_count > 0,
        message: format!(
            "Completed setting up {total} plugins\n    ({success_count} succeeded, {fail_count} failed)."
        ),
    })
}

// ---------------------------------------------------------------------------------------------
// install required
// ---------------------------------------------------------------------------------------------

/// A plugin's `required` list. The manifest knows it for a plugin that imported; a plugin whose
/// import failed (or that has no module) is read from its source text.
fn plugin_requirements(rt: &mut Runtime<'_>, ctx: &Context, name: &str) -> Vec<String> {
    if let Ok(manifest) = crate::python::manifest(rt)
        && let Some(entry) = manifest.plugins.get(name)
        && entry.import_error.is_none()
        && entry.path.is_some()
    {
        return entry.required.clone();
    }
    let Some(file) = plugin_files::plugin_file(&ctx.plugins_dirs, name) else {
        return Vec::new();
    };
    let Ok(text) = std::fs::read_to_string(&file) else {
        return Vec::new();
    };
    match plugin_files::static_required(&text) {
        Ok(required) => required,
        Err(error) => {
            warn(
                rt,
                &format!(
                    "Unable to determine requirements for plugin '{name}' without importing the module.\n    \
                     This may be due to dynamically setting the global `required` list.\n    {error}"
                ),
            );
            Vec::new()
        }
    }
}

/// `Plugin.requirements_file_path`: a `requirements.txt` next to the plugin's source.
fn requirements_file(ctx: &Context, name: &str) -> Option<PathBuf> {
    let file = plugin_files::plugin_file(&ctx.plugins_dirs, name)?;
    let path = file.parent()?.join("requirements.txt");
    path.exists().then_some(path)
}

/// `Plugin.get_required_plugins()`: `(name, repository keys)` of every `plugin:` requirement.
fn required_plugins(
    rt: &mut Runtime<'_>,
    ctx: &Context,
    requirements: &[String],
) -> Vec<(String, String)> {
    let default_keys = rt
        .config()
        .get(&["meerschaum", "repository"])
        .map(|v| v.py_str())
        .unwrap_or_else(|| "api:mrsm".into());
    let mut plugins = Vec::new();
    for requirement in requirements {
        let Some(spec) = requirement
            .strip_prefix("plugin:")
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        let (name, keys) = if spec.contains(&ctx.separator) {
            match plugin_files::split_repo(spec, &ctx.separator) {
                Ok((name, Some(keys))) => (name, keys),
                _ => {
                    let name = spec
                        .split(&ctx.separator)
                        .next()
                        .unwrap_or(spec)
                        .to_string();
                    warn(
                        rt,
                        &format!(
                            "Invalid repo keys for required plugin '{name}'.\n    \
                             Will try to use '{default_keys}' instead."
                        ),
                    );
                    (name, default_keys.clone())
                }
            }
        } else {
            (spec.to_string(), default_keys.clone())
        };
        if !valid_connector_keys(rt, &keys) {
            warn(
                rt,
                &format!(
                    "Invalid connector '{keys}'.\n    \
                     Skipping required plugins from repository '{keys}'"
                ),
            );
            continue;
        }
        plugins.push((name, keys));
    }
    plugins
}

/// `Plugin.install_dependencies(force, debug)`.
fn install_dependencies(
    rt: &mut Runtime<'_>,
    ctx: &Context,
    name: &str,
    force: bool,
) -> Result<bool, ActionError> {
    let requirements = plugin_requirements(rt, ctx, name);
    let requirements_path = requirements_file(ctx, name);
    if requirements.is_empty() && requirements_path.is_none() {
        return Ok(true);
    }

    let plugins = required_plugins(rt, ctx, &requirements);
    for (required, keys) in &plugins {
        if required == name {
            warn(
                rt,
                &format!("Plugin '{name}' cannot depend on itself! Skipping..."),
            );
            continue;
        }
        let result = install_from_repository(rt, ctx, keys, required, force, false)?;
        if !result.success {
            warn(
                rt,
                &format!(
                    "Failed to install required plugin '{required}' from '{keys}' for plugin '{name}':\n{}",
                    result.message
                ),
            );
            if !force {
                warn(
                    rt,
                    "Try installing with the `--force` flag to continue anyway.",
                );
                return Ok(false);
            }
            info_plain(
                rt,
                "Continuing with installation despite the failure (careful, things might be broken!)...",
            );
        }
    }

    // First step: `requirements.txt`.
    if let Some(path) = requirements_path {
        let installed = pip_install(
            &ctx.env,
            rt,
            &PipInstall {
                requirements_file: Some(path),
                venv: name.to_string(),
                debug: ctx.debug,
                ..PipInstall::default()
            },
        );
        if !installed {
            warn(
                rt,
                &format!("Failed to resolve 'requirements.txt' for plugin '{name}'."),
            );
            if !force {
                warn(rt, "Try installing with `--force` to continue anyway.");
                return Ok(false);
            }
            info_plain(
                rt,
                "Continuing with installation despite the failure (careful, things might be broken!)...",
            );
        }
    }

    // Packages a required plugin's environment already holds are not installed again.
    let packages: Vec<&String> = requirements
        .iter()
        .filter(|r| !r.starts_with("plugin:"))
        .filter(|package| {
            !plugins
                .iter()
                .any(|(plugin, _)| ctx.env.venv_contains_package(package, plugin))
        })
        .collect();
    for package in packages {
        let installed = pip_install(
            &ctx.env,
            rt,
            &PipInstall {
                names: vec![package.clone()],
                venv: name.to_string(),
                debug: ctx.debug,
                ..PipInstall::default()
            },
        );
        if !installed {
            warn(
                rt,
                &format!("Failed to install required package '{package}' for plugin '{name}'."),
            );
            if !force {
                warn(rt, "Try installing with `--force` to continue anyway.");
                return Ok(false);
            }
            info_plain(
                rt,
                "Continuing with installation despite the failure (careful, things might be broken!)...",
            );
        }
    }
    Ok(true)
}

/// `install required [names]` (`actions/install.py::_install_required`).
pub fn install_required(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let ctx = Context::new(rt, args);
    repo_connector(rt, args)?;
    let names = selected_plugins(rt, &ctx, args);

    let mut success_count = 0usize;
    let mut fail_count = 0usize;
    for plugin_name in names {
        let (name, _) =
            plugin_files::split_repo(&plugin_name, &ctx.separator).map_err(exception)?;
        if !is_installed(rt, &ctx, &name) {
            warn(
                rt,
                &format!("Plugin '{name}' is not installed. Skipping..."),
            );
            continue;
        }
        info(
            rt,
            &format!("Installing required packages for plugin '{name}'..."),
        );
        if install_dependencies(rt, &ctx, &name, ctx.force)? {
            success_count += 1;
        } else {
            warn(
                rt,
                &format!("Failed to install required packages for plugin '{name}'."),
            );
            fail_count += 1;
        }
    }
    refresh(rt);

    Ok(SuccessTuple {
        success: fail_count == 0,
        message: format!(
            "Installed packages for {} plugins\n    ({success_count} succeeded, {fail_count} failed).",
            success_count + fail_count
        ),
    })
}

// ---------------------------------------------------------------------------------------------
// install packages
// ---------------------------------------------------------------------------------------------

/// `install packages <names> [--venv name] [--dry-run]` (`actions/install.py::_install_packages`).
pub fn install_packages(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    if action.is_empty() {
        return Ok(SuccessTuple::fail("No packages to install."));
    }
    let ctx = Context::new(rt, args);
    let venv = string(args, "venv").unwrap_or_else(|| "mrsm".into());
    let mut installer_args = vec!["--upgrade".to_string()];
    installer_args.extend(words(args, "sub_args"));
    let dry_run = flag(args, "dry_run");

    let installed = pip_install(
        &ctx.env,
        rt,
        &PipInstall {
            names: action.clone(),
            args: Some(installer_args),
            venv: venv.clone(),
            dry_run,
            debug: ctx.debug,
            ..PipInstall::default()
        },
    );
    let s = plural(action.len());
    if installed {
        if dry_run {
            return Ok(SuccessTuple::ok(
                "Dependency preflight completed without changing the environment.",
            ));
        }
        refresh(rt);
        return Ok(SuccessTuple::ok(format!(
            "Successfully installed package{s} {} into the virtual environment '{venv}'.",
            items_str(&action)
        )));
    }
    Ok(SuccessTuple::fail(format!(
        "Failed to install package{s} {}.",
        items_str(&action)
    )))
}

// ---------------------------------------------------------------------------------------------
// install plugins
// ---------------------------------------------------------------------------------------------

/// `binaryornot.check.is_binary`: a gzip archive is binary, JSON error text is not.
fn is_binary(bytes: &[u8]) -> bool {
    let chunk = &bytes[..bytes.len().min(1024)];
    if chunk.is_empty() {
        return false;
    }
    if chunk.starts_with(&[0x1f, 0x8b]) || chunk.contains(&0) {
        return true;
    }
    // Text that decodes (a multi-byte character may be cut by the chunk boundary).
    let decodes = match std::str::from_utf8(chunk) {
        Ok(_) => true,
        Err(e) => e.error_len().is_none() && e.valid_up_to() + 4 > chunk.len(),
    };
    !decodes && chunk.contains(&0xff)
}

/// The reason phrase urllib prints (`HTTP Error 404: Not Found`).
fn http_reason(status: u16) -> &'static str {
    match status {
        301 => "Moved Permanently",
        302 => "Found",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        422 => "Unprocessable Entity",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "",
    }
}

/// What a download produced.
enum Download {
    /// The archive.
    Archive(Vec<u8>),
    /// The server answered with a message instead (`install_plugin` returns it as is).
    Message(SuccessTuple),
}

/// `APIConnector.install_plugin`'s first half: `wget` the archive to
/// `PLUGINS_TEMP_RESOURCES_PATH/<name>.tar.gz` and tell an archive from a JSON answer.
fn download_plugin(
    rt: &mut Runtime<'_>,
    ctx: &Context,
    connector: &APIConnector,
    name: &str,
) -> Result<Download, ActionError> {
    let r_url = meerschaum_api::routes::plugin_r_url(name);
    let attributes = connector.attributes();
    let attr = |key: &str| {
        attributes
            .get(key)
            .map(meerschaum_core::pyfmt::py_str)
            .unwrap_or_default()
    };
    let port = attributes
        .get("port")
        .filter(|p| p.truthy())
        .map(|p| format!(":{}", meerschaum_core::pyfmt::py_str(p)))
        .unwrap_or_default();
    let url = format!("{}://{}{port}{r_url}", attr("protocol"), attr("host"));
    let session = session(rt);
    let response = session.block_on(connector.get(&r_url, RequestOptions::default()));
    let response = match response {
        Ok(response) if response.status == 200 => response,
        other => {
            // `wget` prints the exception, then raises `Failed to download from '<url>'.`
            let detail = match other {
                Ok(response) => format!(
                    "HTTP Error {}: {}",
                    response.status,
                    http_reason(response.status)
                ),
                Err(error) => format!("<urlopen error {error}>"),
            };
            rt.print(&format!("{detail}\n"));
            return Err(exception(format!("Failed to download from '{url}'.")));
        }
    };
    let destination = ctx.temp_dir.join(format!("{name}.tar.gz"));
    let _ = std::fs::create_dir_all(&ctx.temp_dir);
    let _ = std::fs::write(&destination, &response.bytes);
    if is_binary(&response.bytes) {
        return Ok(Download::Archive(response.bytes));
    }
    let fail = format!("Failed to download binary for plugin '{name}'.");
    let message = match response.json() {
        Ok(Json::Array(items)) if items.len() == 2 => SuccessTuple {
            success: match &items[0] {
                Json::Bool(b) => *b,
                Json::Null => false,
                other => !matches!(other.as_i64(), Some(0)),
            },
            message: match &items[1] {
                Json::String(s) => s.clone(),
                other => other.to_string(),
            },
        },
        _ => SuccessTuple::fail(fail),
    };
    Ok(Download::Message(message))
}

/// `repo_connector.install_plugin(name, force=, skip_deps=)` for the repository `keys`.
fn install_from_repository(
    rt: &mut Runtime<'_>,
    ctx: &Context,
    keys: &str,
    name: &str,
    force: bool,
    skip_deps: bool,
) -> Result<SuccessTuple, ActionError> {
    let connector = connector_for(rt, keys)?;
    match download_plugin(rt, ctx, &connector, name)? {
        Download::Message(message) => Ok(message),
        Download::Archive(bytes) => install_archive(rt, ctx, keys, name, &bytes, force, skip_deps),
    }
}

/// `Plugin.install()`: extract an archive into the plugins directory, replacing what was
/// installed, with the venv, dependencies, setup and rollback v4 does.
fn install_archive(
    rt: &mut Runtime<'_>,
    ctx: &Context,
    keys: &str,
    plugin_name: &str,
    archive: &[u8],
    force: bool,
    skip_deps: bool,
) -> Result<SuccessTuple, ActionError> {
    let (name, _) = plugin_files::split_repo(plugin_name, &ctx.separator).map_err(exception)?;
    let name = name.as_str();
    let venvs_dir = ctx.env.venvs_dir.clone();
    let mut tracked: Vec<PathBuf> = Vec::new();
    for dir in &ctx.plugins_dirs {
        tracked.push(dir.join(name));
        tracked.push(dir.join(format!("{name}.py")));
    }
    tracked.push(venvs_dir.join(name));
    let mut rollback = Rollback::new(name, &ctx.temp_dir, tracked)
        .map_err(|e| ActionError::new("OSError", e.to_string()))?;

    let lock = super::pip::InstallLock::acquire(&ctx.env, name);
    let placed = place_archive(
        rt,
        ctx,
        keys,
        name,
        archive,
        force,
        skip_deps,
        &mut rollback,
    );
    drop(lock);
    let placed = match placed {
        Ok(placed) => placed,
        Err(error) => {
            let _ = rollback.restore();
            refresh(rt);
            return Err(error);
        }
    };
    let (result, install_dir) = match placed {
        Placed::Done(result) if result.success => return Ok(result),
        Placed::Done(result) => return Ok(rollback_failed(rt, &mut rollback, name, result)),
        Placed::Installed {
            result,
            install_dir,
        } => (result, install_dir),
    };

    // Post-install `setup()`, outside the package lock (a worker installing packages takes it).
    refresh(rt);
    let success_msg = result.message.clone();
    let kwargs: meerschaum_core::Map =
        [("debug".to_string(), meerschaum_core::Value::Bool(ctx.debug))]
            .into_iter()
            .collect();
    let setup = if is_native(rt, name) {
        native_setup(rt, name, &kwargs)?
    } else {
        python_setup(rt, name, &kwargs)?
    };
    let result = if setup.success {
        SuccessTuple::ok(success_msg)
    } else {
        setup
    };
    if result.success {
        plugin_files::write_origin(&install_dir, &ctx.origins_filename, name, keys);
        Ok(result)
    } else {
        Ok(rollback_failed(rt, &mut rollback, name, result))
    }
}

/// Restore the previous plugin after a failed installation (`rollback()`); a failure to do so
/// is appended to the message.
fn rollback_failed(
    rt: &mut Runtime<'_>,
    rollback: &mut Rollback,
    name: &str,
    result: SuccessTuple,
) -> SuccessTuple {
    let restored = rollback.restore();
    refresh(rt);
    match restored {
        Ok(()) => result,
        Err(error) => SuccessTuple::fail(format!(
            "{}\nFailed to roll back plugin '{name}': {error}",
            result.message
        )),
    }
}

/// Removes a directory when dropped, on every exit path.
struct RemoveOnDrop(PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// What the file part of an installation decided.
enum Placed {
    /// The installation ended (up to date, a higher version installed, extraction failed, or
    /// dependencies failed).
    Done(SuccessTuple),
    /// The files are in place and the dependencies installed; `setup()` is next.
    Installed {
        result: SuccessTuple,
        install_dir: PathBuf,
    },
}

/// `Plugin._install` up to the post-install `setup()`.
#[allow(clippy::too_many_arguments)]
fn place_archive(
    rt: &mut Runtime<'_>,
    ctx: &Context,
    keys: &str,
    name: &str,
    archive: &[u8],
    force: bool,
    skip_deps: bool,
    rollback: &mut Rollback,
) -> Result<Placed, ActionError> {
    // The name becomes a path component below (temp dir, plugins dir); a repository answer or a
    // `required` entry must not be able to smuggle a path (`a/../..`, an absolute path, `..`).
    if !is_safe_plugin_name(name) {
        return Err(ActionError::new(
            "ValueError",
            format!("Invalid plugin name '{name}'."),
        ));
    }
    let temp_dir = ctx.temp_dir.join(name);
    // A stale directory from an earlier run must not feed the extraction (or the file listing).
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).map_err(|e| ActionError::new("OSError", e.to_string()))?;
    let _cleanup = RemoveOnDrop(temp_dir.clone());

    // The version of what is installed now.
    let old_version = plugin_files::plugin_file(&ctx.plugins_dirs, name)
        .and_then(|file| std::fs::read_to_string(file).ok())
        .and_then(|text| plugin_files::declared_version(&text).ok().flatten())
        .unwrap_or_default();

    if let Err(error) = plugin_files::extract_archive(archive, &temp_dir) {
        warn(rt, &error);
        return Ok(Placed::Done(SuccessTuple::fail(format!(
            "Failed to extract plugin '{name}'."
        ))));
    }

    // Search for version information.
    let mut files: Vec<String> = std::fs::read_dir(&temp_dir)
        .map_err(|e| ActionError::new("OSError", e.to_string()))?
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    files.sort();
    let Some(first) = files.first().cloned() else {
        return Err(ActionError::new("IndexError", "list index out of range"));
    };
    let is_dir = if first == name {
        true
    } else if first == format!("{name}.py") {
        false
    } else {
        return Err(exception(format!(
            "Unknown format encountered for plugin '{name}'."
        )));
    };
    let mut version_file = temp_dir.join(&first);
    if is_dir {
        version_file = version_file.join("__init__.py");
    }
    let init_text = std::fs::read_to_string(&version_file).map_err(|_| {
        ActionError::new(
            "FileNotFoundError",
            format!(
                "[Errno 2] No such file or directory: '{}'",
                version_file.display()
            ),
        )
    })?;
    let new_version = plugin_files::declared_version(&init_text)
        .map_err(|e| ActionError::new("ValueError", e))?
        .filter(|v| !v.is_empty());
    if new_version.is_none() {
        warn(
            rt,
            &format!("No `__version__` defined for plugin '{name}'. Assuming new version..."),
        );
    }

    let (is_new_version, is_same_version) = compare_versions(&old_version, new_version.as_deref());

    // Where to store the new plugin permanently.
    let mut install_dir = ctx
        .plugins_dirs
        .first()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("."));
    for dir in &ctx.plugins_dirs {
        if !dir.exists() {
            warn(
                rt,
                &format!("Plugins path does not exist: {}", dir.display()),
            );
            continue;
        }
        if dir.join(name).exists() || dir.join(format!("{name}.py")).exists() {
            install_dir = dir.clone();
            break;
        }
    }

    let success_msg = format!(
        "Successfully installed plugin '{name}'{}.",
        if skip_deps {
            "\n    (skipped dependencies)"
        } else {
            ""
        }
    );
    let mut success = false;
    let mut abort = false;
    let message: String;
    if is_same_version && !force {
        success = true;
        message = format!(
            "Plugin '{name}' is up-to-date (version {old_version}).\n    \
             Install again with `-f` or `--force` to reinstall."
        );
        abort = true;
    } else if is_new_version || force {
        if let Err(error) = rollback.prepare() {
            return Err(ActionError::new("OSError", error.to_string()));
        }
        let source = temp_dir.join(&first);
        let destination = install_dir.join(&first);
        let moved = (|| -> std::io::Result<()> {
            std::fs::create_dir_all(&install_dir)?;
            for existing in [
                install_dir.join(name),
                install_dir.join(format!("{name}.py")),
            ] {
                plugin_files::remove_path(&existing)?;
            }
            if std::fs::rename(&source, &destination).is_err() {
                plugin_files::copy_path(&source, &destination)?;
                plugin_files::remove_path(&source)?;
            }
            Ok(())
        })();
        match moved {
            Ok(()) => {
                success = true;
                message = success_msg.clone();
            }
            Err(error) => {
                message = format!("Failed to install plugin '{name}': {error}");
            }
        }
    } else {
        message = format!(
            "Your installed version of plugin '{name}' ({old_version}) is higher than \
             attempted version {}.",
            new_version.as_deref().unwrap_or_default()
        );
    }
    let _ = std::fs::remove_dir_all(&temp_dir);

    // Reload the plugin: the venv, then the plugin trees.
    ctx.env.init_venv(rt, name, true, ctx.debug);
    refresh(rt);

    if abort {
        plugin_files::write_origin(&install_dir, &ctx.origins_filename, name, keys);
    }
    if !success || abort {
        return Ok(Placed::Done(SuccessTuple { success, message }));
    }

    // Dependencies.
    let installed = skip_deps || install_dependencies(rt, ctx, name, force)?;
    if !installed {
        return Ok(Placed::Done(SuccessTuple::fail(format!(
            "Failed to install dependencies for plugin '{name}'."
        ))));
    }
    Ok(Placed::Installed {
        result: SuccessTuple::ok(success_msg),
        install_dir,
    })
}

/// `(is_new_version, is_same_version)` of `Plugin._install`: `packaging.version` comparison
/// where `parse` of a missing or invalid version raises (and means "new").
fn compare_versions(old: &str, new: Option<&str>) -> (bool, bool) {
    if new.is_none() && old.is_empty() {
        return (true, false);
    }
    let (Some(new), false) = (new, old.is_empty()) else {
        return (true, false);
    };
    match (
        meerschaum_core::version::parse_version(old),
        meerschaum_core::version::parse_version(new),
    ) {
        (Ok(old), Ok(new)) => (old < new, old == new),
        _ => (true, false),
    }
}

/// `install plugins <names> [-r repo] [--force] [--skip-deps]` (`actions/install.py::_install_plugins`).
pub fn install_plugins(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    if action.is_empty() {
        return Ok(SuccessTuple::fail("No plugins to install."));
    }
    let ctx = Context::new(rt, args);
    let (keys, _connector) = repo_connector(rt, args)?;
    // Compose has been built in since 4.0: installing the plugin is a no-op (§11.2).
    let (names, notice) = meerschaum_compose::plugins::filter_install_plugins(&action);
    if let Some(notice) = notice {
        info(rt, notice);
    }
    let skip_deps = flag(args, "skip_deps");

    for name in names {
        info(
            rt,
            &format!("Installing plugin '{name}' from Meerschaum repository '{keys}'..."),
        );
        let result = install_from_repository(rt, &ctx, &keys, &name, ctx.force, skip_deps)?;
        print_tuple(rt, &result, TupleArgs::default());
        if result.success
            && let Some(cache) = &ctx.updates_cache
        {
            let _ = std::fs::remove_file(cache);
        }
    }
    refresh(rt);
    // `return actions['reload'](debug=debug)`: the per-plugin results were printed above.
    Ok(SuccessTuple::success())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archives_and_json_answers_are_told_apart() {
        assert!(is_binary(&[0x1f, 0x8b, 8, 0, 0, 0]));
        assert!(!is_binary(
            br#"[false, "Archive for plugin 'x' could not be found."]"#
        ));
        assert!(!is_binary(b""));
        assert!(!is_binary("[true, \"é\"]".as_bytes()));
        assert!(is_binary(&[0x00, 0x01, 0x02]));
    }

    #[test]
    fn version_comparison_follows_packaging() {
        // Nothing declared anywhere: new.
        assert_eq!(compare_versions("", None), (true, false));
        // Nothing installed: new (parse of '' raises in v4).
        assert_eq!(compare_versions("", Some("1.0")), (true, false));
        // Installed but the new one declares nothing: new (parse(None) raises).
        assert_eq!(compare_versions("1.0", None), (true, false));
        assert_eq!(compare_versions("1.0.0", Some("1.0")), (false, true));
        assert_eq!(compare_versions("1.0", Some("1.1")), (true, false));
        assert_eq!(compare_versions("2.0", Some("1.1")), (false, false));
        // Unparsable versions mean new.
        assert_eq!(compare_versions("x y", Some("1.1")), (true, false));
        assert_eq!(compare_versions("1.0rc1", Some("1.0")), (true, false));
    }

    #[test]
    fn native_setup_replies_follow_plugin_setup_rules() {
        let name = "p";
        let ok = |v: Json| interpret_native_setup(name, Ok(v));
        assert_eq!(ok(json!([true, "fine"])), SuccessTuple::ok("fine"));
        assert_eq!(ok(json!([false, "bad"])), SuccessTuple::fail("bad"));
        assert_eq!(
            ok(json!(false)),
            SuccessTuple::fail("Setup for Plugin 'p' did not return a message.")
        );
        assert!(ok(json!(true)).success);
        assert_eq!(
            ok(Json::Null),
            SuccessTuple::fail("Setup for Plugin 'p' returned None.")
        );
        assert!(
            ok(json!("what"))
                .message
                .starts_with("Unknown return value")
        );
        // No `setup()` is "assume success", like a Python plugin without one.
        assert!(
            interpret_native_setup(
                name,
                Err(meerschaum_plugin::Error::Unsupported("setup".into()))
            )
            .success
        );
        assert_eq!(
            interpret_native_setup(name, Err(meerschaum_plugin::Error::Message("boom".into()))),
            SuccessTuple::fail("boom")
        );
    }
}

/// A plugin name is a single path component: letters, digits, `_` and `-`, never empty, never
/// `.`/`..`, no separators and no absolute path.
pub fn is_safe_plugin_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

#[cfg(test)]
mod name_tests {
    use super::is_safe_plugin_name;

    #[test]
    fn rejects_path_components() {
        for bad in [
            "",
            ".",
            "..",
            "a/../../..",
            "/tmp/x",
            "a\\b",
            "a/b",
            "../x",
            "name with space",
        ] {
            assert!(!is_safe_plugin_name(bad), "{bad:?}");
        }
        for good in ["noaa", "my_plugin", "my-plugin", "p1"] {
            assert!(is_safe_plugin_name(good), "{good:?}");
        }
    }
}
