//! Native (C-ABI) plugins in the CLI: the manifest merge, action dispatch, plugin flags, and
//! the engine registry that resolves native connectors, `sync()` functions and sync hooks.
//!
//! Native plugins are discovered in the same `MRSM_PLUGINS_DIR` directories as Python plugins
//! (a directory with a `plugin.toml` and `lib<name>.so`). A native plugin shadows a Python
//! plugin of the same name. See `crates/meerschaum-plugin/README.md`.
//!
//! The children: `registry` (hooks, sync wrappers, `NativeRegistry`), `host`, `host_env` and
//! `host_sql` (the `PluginHost` callback services), `routes` and `extensions` (HTTP routes, MCP
//! tools and console pages), and `tests`.

mod extensions;
mod host;
mod host_env;
mod host_sql;
mod registry;
mod routes;
pub use extensions::{mcp, pages};
pub(crate) use host::PluginHost;
pub use host::local_host;
use host_env::*;
pub use host_env::{HostEnv, has_written_plugin_configs, host_config, take_written_plugin_configs};
pub(crate) use registry::attributes;
use registry::*;
pub use registry::{NativeRegistry, instance_connector};
pub use routes::routes;
use routes::*;

use std::collections::HashSet;
use std::sync::Arc;

use meerschaum_connector::{Connector, InstanceConnector, async_trait};
use meerschaum_core::success::SyncResult;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_engine::{
    ConnectorRegistry, PluginSync, SyncArgs, SyncBegin, SyncHooks,
    stream::{LegacyStream, StreamSource},
    sync::{DeltaObserver, SyncEnv},
};
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_plugin::NativePlugins;
use meerschaum_worker::manifest::{ArgumentManifest, Manifest, PluginManifest};

use crate::builtin::{ActionError, ActionResult};
use crate::native;
use crate::value::{ArgsMap, PyValue};

/// Where the native manifest is cached (separate from the Python worker manifest).
pub fn cache_path(root_dir: &std::path::Path) -> std::path::PathBuf {
    if let Some(namespace) = meerschaum_core::team_scope::source_namespace::for_root(root_dir) {
        return namespace.cache.join("native_plugins_manifest.json");
    }
    // A sandboxed team process keeps its own.
    if let Some(dir) = meerschaum_core::team_scope::sandbox::private_cache_dir() {
        return dir.join("native_plugins_manifest.json");
    }
    root_dir.join("cache").join("native_plugins_manifest.json")
}

/// Merge the native plugins into the Python manifest. A native plugin replaces a Python plugin
/// of the same name (and that plugin's custom connector types).
pub fn merge(python: &Manifest, native: &NativePlugins) -> Manifest {
    let mut merged = python.clone();
    if native.is_empty() {
        return merged;
    }
    let mut raw = native.manifest().clone();
    // Native manifests may omit argparse fields the worker manifest always lists.
    if let Some(plugins) = raw["plugins"].as_object_mut() {
        for plugin in plugins.values_mut() {
            for argument in plugin
                .get_mut("arguments")
                .and_then(serde_json::Value::as_array_mut)
                .into_iter()
                .flatten()
            {
                for key in ["nargs", "const", "default", "choices", "metavar"] {
                    if argument.get(key).is_none() {
                        argument[key] = serde_json::Value::Null;
                    }
                }
            }
        }
    }
    let parsed: Manifest = match serde_json::from_value(raw) {
        Ok(manifest) => manifest,
        Err(error) => {
            eprintln!(
                "Warning: Skipping native plugins: their manifest could not be read: {error}"
            );
            merged
                .load_errors
                .insert("native plugins".into(), error.to_string());
            return merged;
        }
    };
    for (name, entry) in parsed.plugins {
        if !native.contains(&name) {
            continue;
        }
        merged.connector_types.retain(|_, c| c.plugin != name);
        for list in [
            &mut merged.api_plugins,
            &mut merged.dash_plugins,
            &mut merged.web_pages,
            &mut merged.mcp_tools,
            &mut merged.pre_sync_hooks,
            &mut merged.post_sync_hooks,
        ] {
            list.retain(|p| *p != name);
        }
        merged.plugins.insert(name, entry);
    }
    for (kind, connector) in parsed.connector_types {
        if native.contains(&connector.plugin) {
            merged.connector_types.insert(kind, connector);
        }
    }
    for (kind, executor) in parsed.executor_types {
        if native.contains(&executor.plugin) {
            merged.executor_types.insert(kind, executor);
        }
    }
    merged
}

/// The native plugin and action name that handle `words`, if any.
pub fn action_owner(
    words: &[String],
    manifest: &Manifest,
    native: &NativePlugins,
) -> Option<(String, String)> {
    let owned: Vec<(&str, &str)> = manifest
        .plugins
        .values()
        .filter(|p| native.contains(&p.name))
        .flat_map(|p| {
            p.actions
                .iter()
                .map(move |a| (p.name.as_str(), a.name.as_str()))
        })
        .collect();
    // v4's `get_action` over the native plugins' actions (see `python::resolve_action`).
    let name = crate::python::resolve_action(words, owned.iter().map(|(_, action)| *action))?;
    owned
        .iter()
        .find(|(_, action)| *action == name)
        .map(|(plugin, action)| (plugin.to_string(), action.to_string()))
}

fn kwargs_json(args: &ArgsMap) -> serde_json::Value {
    let map: Map = args
        .iter()
        .map(|(key, value)| (key.clone(), native::py_to_value(value)))
        .collect();
    serde_json::to_value(map).unwrap_or(serde_json::Value::Null)
}

/// Run a native plugin action with host calls over the CLI's session and config
/// (`host::pipe(...)`, `host::read_sql(...)`, ...). `action` is the dispatched action name
/// (`words` consumed).
pub fn run_action_in(
    rt: &mut crate::dispatch::Runtime<'_>,
    plugin: &str,
    action: &str,
    args: &ArgsMap,
    natives: &Arc<NativePlugins>,
) -> ActionResult {
    let instance = native::instance_keys(args, rt);
    with_command_host(rt, natives, Some(instance), |host| {
        run_action_with(plugin, action, args, natives, Some(host))
    })
}

/// Run `f` with host services for a command's plugin call outside an engine run (an action,
/// `setup`), over the CLI's session and the runtime's own config (a compose project's, a test's
/// explicit one, never the process's default root behind its back). The config is shared with
/// the host calls for the call, so a `write_plugin_config` updates it as Python's does, and it is
/// handed back to the runtime after. `instance` is the default instance of `host::pipe(...)`.
pub(crate) fn with_command_host<R>(
    rt: &mut crate::dispatch::Runtime<'_>,
    natives: &Arc<NativePlugins>,
    instance: Option<String>,
    f: impl FnOnce(&PluginHost) -> R,
) -> R {
    let session = native::session(rt);
    rt.config();
    let config = Arc::new(
        rt.config
            .take()
            .expect("the runtime's config was just built"),
    );
    let host = PluginHost::local(HostEnv::new(
        session,
        config.clone(),
        natives.clone(),
        instance,
    ));
    let result = f(&host);
    drop(host);
    // If anything still holds the config (nothing does once the call returns), keep a copy.
    rt.config = Some(Arc::try_unwrap(config).unwrap_or_else(|shared| duplicate_config(&shared)));
    result
}

/// A native plugin's `setup` (`mrsm setup plugins`, after `mrsm install plugins`), with host
/// calls (a `setup` often writes the plugin's default config).
pub fn call_setup(
    rt: &mut crate::dispatch::Runtime<'_>,
    natives: &Arc<NativePlugins>,
    plugin: &meerschaum_plugin::LoadedPlugin,
    context: serde_json::Value,
) -> meerschaum_plugin::Result<serde_json::Value> {
    with_command_host(rt, natives, None, |host| {
        plugin.call_json_with_host("setup", context, Some(host))
    })
}

/// Run a native plugin action. `action` is the dispatched action name (`words` consumed).
pub fn run_action(
    plugin: &str,
    action: &str,
    args: &ArgsMap,
    natives: &NativePlugins,
) -> ActionResult {
    run_action_with(plugin, action, args, natives, None)
}

fn run_action_with(
    plugin: &str,
    action: &str,
    args: &ArgsMap,
    natives: &NativePlugins,
    host: Option<&PluginHost>,
) -> ActionResult {
    let plugin = natives
        .plugin(plugin)
        .map_err(|e| ActionError::new("ImportError", e.to_string()))?;
    let mut args = args.clone();
    // The words after the action name are its subaction arguments, like v4's `action` list.
    let words = native::words(&args, "action");
    let consumed = (1..=words.len())
        .find(|&count| words[..count].join("_") == action)
        .unwrap_or_else(|| action.split('_').count());
    args.insert(
        "action".into(),
        PyValue::str_list(words.get(consumed..).unwrap_or_default()),
    );
    // PARITY: a plugin action's exceptions are returned as `(False, message)`.
    let host = host.map(|h| h as &dyn meerschaum_plugin::HostServices);
    Ok(plugin
        .action_with_host(action, kwargs_json(&args), host)
        .unwrap_or_else(|e| SuccessTuple::fail(e.to_string())))
}

/// `_StoreTrueAction` / `store_true` -> `store_true`.
fn normalize_action(action: &str) -> String {
    let name = action.trim_start_matches('_');
    let name = name.strip_suffix("Action").unwrap_or(name);
    let mut out = String::new();
    for (index, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// The argparse `dest` of a manifest argument.
fn destination(argument: &ArgumentManifest) -> Option<String> {
    argument.dest.clone().or_else(|| {
        let option = argument
            .option_strings
            .iter()
            .find(|o| o.starts_with("--"))
            .or_else(|| argument.option_strings.first())?;
        Some(option.trim_start_matches('-').replace('-', "_"))
    })
}

/// argparse's name for an argument in its errors: its option strings joined by `/`.
fn argument_name(argument: &ArgumentManifest) -> String {
    argument.option_strings.join("/")
}

/// Convert a given value with the argument's `type` and check its `choices`, with argparse's
/// error messages.
fn convert_argument(argument: &ArgumentManifest, text: &str) -> Result<PyValue, String> {
    let invalid = |kind: &str| {
        format!(
            "argument {}: invalid {kind} value: '{text}'",
            argument_name(argument)
        )
    };
    let value = match argument.type_name.as_deref() {
        Some("int") => PyValue::Int(text.trim().parse::<i128>().map_err(|_| invalid("int"))?),
        Some("float") => PyValue::Float(text.trim().parse::<f64>().map_err(|_| invalid("float"))?),
        _ => PyValue::Str(text.into()),
    };
    if let Some(choices) = argument.choices.as_array()
        && !choices.iter().any(|choice| json_to_py(choice) == value)
    {
        return Err(format!(
            "argument {}: invalid choice: {} (choose from {})",
            argument_name(argument),
            value.py_repr(),
            choices
                .iter()
                .map(|c| json_to_py(c).py_repr())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(value)
}

/// Populate the flags native plugins declared (`add_plugin_argument` equivalents) from the
/// raw sysargs. Supports the common argparse actions and a single value (`nargs` unset), and
/// enforces `type` and `choices` with argparse's messages. A `required` flag is required when
/// one of its plugin's actions runs. PARITY: as v4's `add_plugin_argument`, a flag whose option
/// string is already taken (by the built-in CLI or another plugin) is a warning and is skipped.
pub fn arguments(
    args: &mut ArgsMap,
    plugins: &[&PluginManifest],
    sysargs: &[String],
) -> Result<(), String> {
    let action_name = crate::native::words(args, "action").into_iter().next();
    let mut taken: HashSet<&str> = crate::spec::ARGUMENTS
        .iter()
        .flat_map(|a| a.option_strings.iter().copied())
        .collect();
    let mut missing = Vec::new();
    for plugin in plugins {
        let runs_action = action_name
            .as_deref()
            .is_some_and(|name| plugin.actions.iter().any(|a| a.name == name));
        for argument in &plugin.arguments {
            if let Some(option) = argument
                .option_strings
                .iter()
                .find(|o| taken.contains(o.as_str()))
            {
                tracing_warn(&format!(
                    "argument {}: conflicting option string: {option} (plugin '{}')",
                    argument_name(argument),
                    plugin.name
                ));
                continue;
            }
            taken.extend(argument.option_strings.iter().map(String::as_str));
            argument_value(args, argument, sysargs)?;
            if argument.required && runs_action && !mentioned(argument, sysargs) {
                missing.push(argument_name(argument));
            }
        }
    }
    if !missing.is_empty() {
        return Err(format!(
            "the following arguments are required: {}",
            missing.join(", ")
        ));
    }
    Ok(())
}

/// Whether `sysargs` gives the argument (`--flag`, `--flag value` or `--flag=value`).
fn mentioned(argument: &ArgumentManifest, sysargs: &[String]) -> bool {
    sysargs.iter().any(|word| {
        argument
            .option_strings
            .iter()
            .any(|o| word == o || word.starts_with(&format!("{o}=")))
    })
}

/// Parse one argument from `sysargs` into `args`.
fn argument_value(
    args: &mut ArgsMap,
    argument: &ArgumentManifest,
    sysargs: &[String],
) -> Result<(), String> {
    let Some(dest) = destination(argument) else {
        return Ok(());
    };
    let action = normalize_action(&argument.action);
    let mut found: Vec<Option<String>> = Vec::new();
    let mut index = 0;
    while index < sysargs.len() {
        let word = &sysargs[index];
        let hit = argument.option_strings.iter().find_map(|option| {
            if word == option {
                Some(None)
            } else {
                word.strip_prefix(&format!("{option}="))
                    .map(|v| Some(v.to_string()))
            }
        });
        match hit {
            Some(Some(value)) => found.push(Some(value)),
            Some(None) => match action.as_str() {
                "store_true" | "store_false" | "store_const" | "count" | "append_const" => {
                    found.push(None)
                }
                _ => match sysargs.get(index + 1) {
                    Some(value) => {
                        found.push(Some(value.clone()));
                        index += 1;
                    }
                    None => {
                        return Err(format!(
                            "argument {}: expected one argument",
                            argument_name(argument)
                        ));
                    }
                },
            },
            None => {}
        }
        index += 1;
    }
    let given = match action.as_str() {
        "store_true" if !found.is_empty() => Some(PyValue::Bool(true)),
        "store_false" if !found.is_empty() => Some(PyValue::Bool(false)),
        "count" if !found.is_empty() => Some(PyValue::Int(found.len() as i128)),
        "store_const" if !found.is_empty() => Some(json_to_py(&argument.const_value)),
        "append" if !found.is_empty() => Some(PyValue::List(
            found
                .iter()
                .flatten()
                .map(|v| convert_argument(argument, v))
                .collect::<Result<_, _>>()?,
        )),
        "store" | "" => found
            .last()
            .and_then(|v| v.as_deref())
            .map(|v| convert_argument(argument, v))
            .transpose()?,
        _ => None,
    };
    let value = match given {
        Some(value) => Some(value),
        None if !args.contains_key(&dest) => Some(match action.as_str() {
            // `argparse`: `store_false` defaults to `True` unless a default was declared.
            "store_false" if argument.default.is_null() => PyValue::Bool(true),
            _ => json_to_py(&argument.default),
        }),
        None => None,
    };
    // PARITY: the keyword arguments of a plugin action go through v4's `parse_arguments`,
    // which pops every `False` and every `None` (a flag that was not given is absent, and
    // `kwargs.get('flag')` is `None`) and turns the string `'None'` into `None`. Native
    // plugins get the same keyword arguments as Python plugins.
    if let Some(value) = value.and_then(v4_argument_value) {
        args.insert(dest, value);
    }
    Ok(())
}

/// What v4's `parse_arguments` leaves of a parsed argument: nothing for `False` and `None` (the
/// argument is dropped), `None` (kept) for the string `'None'`, everything else unchanged.
pub(crate) fn v4_argument_value(value: PyValue) -> Option<PyValue> {
    match value {
        PyValue::Bool(false) | PyValue::None => None,
        PyValue::Str(text) if text == "None" => Some(PyValue::None),
        other => Some(other),
    }
}

pub(crate) fn json_to_py(value: &serde_json::Value) -> PyValue {
    match value {
        serde_json::Value::Null => PyValue::None,
        serde_json::Value::Bool(b) => PyValue::Bool(*b),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(|i| PyValue::Int(i.into()))
            .unwrap_or_else(|| PyValue::Float(n.as_f64().unwrap_or_default())),
        serde_json::Value::String(s) => PyValue::Str(s.clone()),
        serde_json::Value::Array(a) => PyValue::List(a.iter().map(json_to_py).collect()),
        serde_json::Value::Object(o) => PyValue::Dict(
            o.iter()
                .map(|(k, v)| (PyValue::Str(k.clone()), json_to_py(v)))
                .collect(),
        ),
    }
}

/// The connector attributes handed to a native connector.
fn config_json(attributes: &Map) -> serde_json::Value {
    serde_json::to_value(attributes).unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests;
