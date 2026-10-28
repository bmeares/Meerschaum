//! Native (C-ABI) plugins in the CLI: the manifest merge, action dispatch, plugin flags, and
//! the engine registry that resolves native connectors, `sync()` functions and sync hooks.
//!
//! Native plugins are discovered in the same `MRSM_PLUGINS_DIR` directories as Python plugins
//! (a directory with a `plugin.toml` and `lib<name>.so`). A native plugin shadows a Python
//! plugin of the same name. See `crates/meerschaum-plugin/README.md`.

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
fn with_command_host<R>(
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

/// A native instance connector for `keys` (`<type>:<label>`), if a native plugin declares the type.
pub fn instance_connector(
    natives: &NativePlugins,
    keys: &str,
    attributes: &Map,
) -> Option<Result<Arc<dyn InstanceConnector>, String>> {
    let (kind, label) = keys.split_once(':')?;
    if !natives.is_instance_type(kind) {
        return None;
    }
    Some(
        natives
            .connector(kind, label, config_json(attributes))?
            .map(|c| Arc::new(c) as Arc<dyn InstanceConnector>)
            .map_err(|e| e.to_string()),
    )
}

fn bound_value(bound: &meerschaum_connector::DateBound) -> Value {
    match bound {
        meerschaum_connector::DateBound::Int(i) => Value::Int(*i),
        other => Value::Str(other.isoformat()),
    }
}

fn pipe_json(pipe: &Pipe) -> Value {
    let mut map = Map::new();
    map.insert("connector_keys".into(), pipe.connector_keys().into());
    map.insert("metric_key".into(), pipe.metric_key().into());
    map.insert(
        "location_key".into(),
        pipe.location_key().map(String::from).into(),
    );
    map.insert("instance_keys".into(), pipe.instance_keys().into());
    map.insert("temporary".into(), pipe.temporary.into());
    map.insert(
        "parameters".into(),
        Value::Map(pipe.raw_parameters().clone()),
    );
    Value::Map(map)
}

/// The keyword arguments v4 passes to `sync()` (the hooks get `hook_kwargs`).
fn sync_kwargs(pipe: &Pipe, args: &SyncArgs) -> serde_json::Value {
    let mut map = args.extra.clone();
    map.insert("pipe".into(), pipe_json(pipe));
    map.insert(
        "begin".into(),
        match &args.begin {
            SyncBegin::Default => Value::Str(String::new()),
            SyncBegin::Unbounded => Value::Null,
            SyncBegin::At(b) => bound_value(b),
        },
    );
    map.insert("end".into(), args.end.as_ref().map(bound_value).into());
    map.insert("params".into(), args.params.clone().map(Value::Map).into());
    map.insert("check_existing".into(), args.check_existing.into());
    map.insert("force".into(), args.force.into());
    map.insert(
        "sync_timestamp".into(),
        args.sync_timestamp.to_rfc3339().into(),
    );
    serde_json::to_value(map).unwrap_or(serde_json::Value::Null)
}

/// The keyword arguments a sync hook receives: `sync_kwargs` without `pipe`, which Python
/// passes positionally (native hooks find it in `context.pipe`).
fn hook_kwargs(pipe: &Pipe, args: &SyncArgs) -> serde_json::Value {
    let mut kwargs = sync_kwargs(pipe, args);
    if let Some(map) = kwargs.as_object_mut() {
        map.remove("pipe");
    }
    kwargs
}

/// Where the CLI collects sync hook results to print them (shared with the Python hooks).
type HookReporterSlot = Arc<std::sync::RwLock<Option<meerschaum_engine::python::HookReporter>>>;

struct NativeHooks {
    natives: Arc<NativePlugins>,
    inner: Option<Arc<dyn SyncHooks>>,
    reporter: HookReporterSlot,
    /// Serves the hooks' host calls.
    registry: NativeRegistry,
}

/// One native hook's outcome: its result, and whether it raised (an error, not a tuple).
struct HookOutcome {
    plugin: String,
    hook: String,
    result: SuccessTuple,
    raised: bool,
}

/// Run one plugin's hooks in order. PARITY: as v4's `call_sync_hook`, an error is the tuple
/// `(False, "Failed to execute sync hook '<hook>' from plugin '<plugin>':\n<error>")`.
fn run_plugin_hooks(
    natives: &NativePlugins,
    name: &str,
    hooks: Vec<String>,
    post: bool,
    kwargs: &serde_json::Value,
    pipe: &serde_json::Value,
    host: &PluginHost,
) -> Vec<HookOutcome> {
    let failed = |hook: &str, error: &dyn std::fmt::Display| {
        SuccessTuple::fail(format!(
            "Failed to execute sync hook '{hook}' from plugin '{name}':\n{error}"
        ))
    };
    let plugin = match natives.plugin(name) {
        Ok(plugin) => plugin,
        Err(error) => {
            return hooks
                .into_iter()
                .map(|hook| HookOutcome {
                    result: failed(&hook, &error),
                    plugin: name.to_string(),
                    hook,
                    raised: true,
                })
                .collect();
        }
    };
    hooks
        .into_iter()
        .map(|hook| {
            let (result, raised) =
                match plugin.sync_hook(post, &hook, kwargs.clone(), pipe.clone(), Some(host)) {
                    Ok(tuple) => (tuple, false),
                    Err(error) => (failed(&hook, &error), true),
                };
            HookOutcome {
                plugin: name.to_string(),
                hook,
                result,
                raised,
            }
        })
        .collect()
}

impl NativeHooks {
    async fn run(&self, post: bool, pipe: &Pipe, args: &SyncArgs, kwargs: serde_json::Value) {
        // PARITY: v4 runs hooks from `actions/sync.py`, around `pipe.sync()`; a `pipe.sync()`
        // inside a hook runs no hooks.
        let mut host_args = args.clone();
        host_args.extra.insert("skip_hooks".into(), true.into());
        let host = Arc::new(PluginHost::engine(self.registry.clone(), host_args));
        let pipe = serde_json::to_value(pipe_json(pipe)).unwrap_or_default();
        let reporter = self
            .reporter
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        for (name, hooks) in self.natives.hooks(post) {
            let natives = self.natives.clone();
            let (kwargs, pipe, host) = (kwargs.clone(), pipe.clone(), host.clone());
            let outcomes = tokio::task::spawn_blocking(move || {
                run_plugin_hooks(&natives, &name, hooks, post, &kwargs, &pipe, &host)
            })
            .await
            .unwrap_or_else(|error| {
                vec![HookOutcome {
                    plugin: String::new(),
                    hook: String::new(),
                    result: SuccessTuple::fail(format!("Native plugin hook failed: {error}")),
                    raised: true,
                }]
            });
            // PARITY: hook failures are never fatal. v4 warns about a hook that raised and
            // prints every hook's tuple, successful or not (`actions/sync.py`).
            for outcome in outcomes {
                if outcome.raised {
                    tracing_warn(&outcome.result.message);
                }
                match &reporter {
                    Some(report) => report(outcome.result),
                    None if !outcome.result.success && !outcome.raised => {
                        tracing_warn(&format!(
                            "Native plugin '{}' hook failed: {}: {}",
                            outcome.plugin, outcome.hook, outcome.result.message
                        ));
                    }
                    None => {}
                }
            }
        }
    }
}

fn tracing_warn(message: &str) {
    eprintln!("Warning: {message}");
}

#[async_trait]
impl SyncHooks for NativeHooks {
    async fn pre_sync(&self, pipe: &Pipe, args: &SyncArgs) {
        self.run(false, pipe, args, hook_kwargs(pipe, args)).await;
        if let Some(inner) = &self.inner {
            inner.pre_sync(pipe, args).await;
        }
    }

    async fn post_sync(&self, pipe: &Pipe, args: &SyncArgs, result: &SyncResult) {
        let mut kwargs = hook_kwargs(pipe, args);
        kwargs["success_tuple"] = serde_json::json!([result.success, result.message]);
        kwargs["sync_duration"] =
            serde_json::json!(result.duration.map(|d| d.as_secs_f64()).unwrap_or_default());
        kwargs["sync_complete_timestamp"] = chrono::Utc::now().to_rfc3339().into();
        self.run(true, pipe, args, kwargs).await;
        if let Some(inner) = &self.inner {
            inner.post_sync(pipe, args, result).await;
        }
    }
}

/// A native plugin's own `sync()`.
struct NativeSync(meerschaum_plugin::NativeConnector, NativeRegistry);

#[async_trait]
impl PluginSync for NativeSync {
    async fn sync(&self, pipe: &Pipe, args: &SyncArgs) -> SyncResult {
        let connector = self
            .0
            .clone()
            .with_host_services(Arc::new(PluginHost::engine(self.1.clone(), args.clone())));
        let context = sync_kwargs(pipe, args);
        tokio::task::spawn_blocking(move || {
            connector
                .call("sync", context, None)
                .and_then(|reply| Ok(serde_json::from_value::<SuccessTuple>(reply.value)?))
                .map(SyncResult::from)
                .unwrap_or_else(|e| SyncResult::new(false, e.to_string()))
        })
        .await
        .unwrap_or_else(|e| SyncResult::new(false, e.to_string()))
    }

    async fn sync_native(
        &self,
        pipe: &Pipe,
        args: &SyncArgs,
        _env: &SyncEnv<'_>,
        _observer: Option<&dyn DeltaObserver>,
    ) -> SyncResult {
        self.sync(pipe, args).await
    }
}

/// Resolves native connectors, `sync()` functions and sync hooks before the wrapped registry.
#[derive(Clone)]
pub struct NativeRegistry {
    natives: Arc<NativePlugins>,
    engine_config: meerschaum_engine::EngineConfig,
    /// `meerschaum:connectors` (connector attributes by type and label).
    connectors: Map,
    inner: Arc<dyn ConnectorRegistry>,
    hook_reporter: HookReporterSlot,
    /// The sync's keyword arguments for streaming connectors' `subscribe` (`--listen`).
    stream_kwargs: Arc<std::sync::RwLock<Map>>,
    /// The session whose connectors host calls use beyond the run's (`set_session`).
    session: Arc<std::sync::RwLock<Option<crate::native::Session>>>,
    /// What host calls reach, built on first use.
    host_env: Arc<std::sync::Mutex<Option<HostEnv>>>,
    /// The config host calls read and write: the run's own (never the process's default root).
    host_config: Arc<meerschaum_core::config::Config>,
}

/// `meerschaum:connectors:<type>:default` patched with the label's section.
pub(crate) fn attributes(connectors: &Map, kind: &str, label: &str) -> Map {
    let section = |key: &str| {
        connectors
            .get(kind)
            .and_then(|v| v.get(key))
            .map(Value::to_map)
            .unwrap_or_default()
    };
    let specific = section(label);
    if specific.is_empty() {
        return Map::new();
    }
    meerschaum_core::value::apply_patch_maps(&section("default"), &specific)
}

impl NativeRegistry {
    /// Wrap `inner`. With no native plugins this is a pass-through. The plugins' host calls use
    /// a copy of `config` (its root, files and in-memory settings).
    pub fn new(
        natives: Arc<NativePlugins>,
        config: &meerschaum_core::config::Config,
        inner: Arc<dyn ConnectorRegistry>,
    ) -> Arc<Self> {
        Self::with_config(natives, host_config(config), inner)
    }

    /// [`new`](Self::new) whose host calls share `config` (a `write_plugin_config` updates it).
    pub fn with_config(
        natives: Arc<NativePlugins>,
        config: Arc<meerschaum_core::config::Config>,
        inner: Arc<dyn ConnectorRegistry>,
    ) -> Arc<Self> {
        Arc::new(Self {
            natives,
            engine_config: meerschaum_engine::EngineConfig::from_config(&config, None),
            connectors: config
                .get(&["meerschaum", "connectors"])
                .map(|v| v.to_map())
                .unwrap_or_default(),
            inner,
            hook_reporter: HookReporterSlot::default(),
            stream_kwargs: Arc::default(),
            session: Arc::default(),
            host_env: Arc::default(),
            host_config: config,
        })
    }

    /// Whether the plugins' host calls may write their config (`write_plugin_config`). The
    /// Python façade builds its registries over a read-only snapshot of its live config and
    /// allows the writes Python's own `write_plugin_config()` would make. Only for a registry
    /// made with [`new`](Self::new) (its config is its own copy).
    pub fn set_config_writes(&self, allow: bool) {
        self.host_config.with_state(|s| s.set_allow_writes(allow));
    }

    /// Serve the plugins' host calls (`host::pipe(...)`, `host::read_sql(...)`, ...) over
    /// `session`'s connectors, the ones the run itself uses. Without it, host calls build their
    /// connectors in a session of their own on the current runtime.
    pub fn set_session(&self, session: crate::native::Session) {
        *self.session.write().unwrap_or_else(|e| e.into_inner()) = Some(session);
        *self.host_env.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// What host calls during this registry's runs reach.
    fn host_env(&self) -> Result<HostEnv, String> {
        let mut slot = self.host_env.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(env) = slot.as_ref() {
            return Ok(env.clone());
        }
        let session = self
            .session
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let session = match session {
            Some(session) => session,
            None => crate::native::Session::with_runtime(
                tokio::runtime::Handle::try_current().map_err(|e| e.to_string())?,
            ),
        };
        let mut env = HostEnv::new(
            session,
            self.host_config.clone(),
            self.natives.clone(),
            None,
        );
        if !self.engine_config.home_instance.is_empty() {
            env.default_instance = self.engine_config.home_instance.clone();
        }
        env.defaults = self.engine_config.defaults.clone();
        *slot = Some(env.clone());
        Ok(env)
    }

    /// The keyword arguments streaming native connectors' `subscribe` receives (the same map
    /// `PythonRegistry::set_stream_kwargs` gives Python `subscribe` sources).
    pub fn set_stream_kwargs(&self, kwargs: Map) {
        *self
            .stream_kwargs
            .write()
            .unwrap_or_else(|e| e.into_inner()) = kwargs;
    }

    /// Print native sync hook results the way the Python hooks' results are printed
    /// (`PythonRegistry::set_hook_reporter`). Without a reporter, failures are warnings.
    pub fn set_hook_reporter(&self, reporter: meerschaum_engine::python::HookReporter) {
        *self
            .hook_reporter
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(reporter);
    }

    fn pipe_host(&self) -> Arc<PluginHost> {
        Arc::new(PluginHost::engine(self.clone(), SyncArgs::default()))
    }

    fn source_connector(
        &self,
        keys: &str,
    ) -> Option<Result<meerschaum_plugin::NativeConnector, String>> {
        let (kind, label) = keys.split_once(':')?;
        if kind == "plugin" {
            return self.natives.source(label).map(|r| {
                r.map(|c| c.with_host_services(self.pipe_host()))
                    .map_err(|e| e.to_string())
            });
        }
        let attributes = attributes(&self.connectors, kind, label);
        self.natives
            .connector(kind, label, config_json(&attributes))
            .map(|r| {
                r.map(|c| c.with_host_services(self.pipe_host()))
                    .map_err(|e| e.to_string())
            })
    }
}

// ---------------------------------------------------------------------------------------------
// Host calls: what native plugins ask the process (`meerschaum_plugin::callbacks`).
// ---------------------------------------------------------------------------------------------

/// What host calls reach in one process: its connectors (a [`Session`]), its config and its
/// native plugins.
///
/// [`Session`]: crate::native::Session
#[derive(Clone)]
pub struct HostEnv {
    session: crate::native::Session,
    config: Arc<meerschaum_core::config::Config>,
    natives: Arc<NativePlugins>,
    defaults: meerschaum_core::pipe::PipeDefaults,
    /// The instance of `host::pipe(...)` without `instance_keys` in a call without a Pipe.
    default_instance: String,
}

impl HostEnv {
    /// Host calls over `session`'s connectors and `config`. `default_instance` defaults to
    /// `meerschaum:instance`.
    pub fn new(
        session: crate::native::Session,
        config: Arc<meerschaum_core::config::Config>,
        natives: Arc<NativePlugins>,
        default_instance: Option<String>,
    ) -> HostEnv {
        let defaults = meerschaum_engine::EngineConfig::from_config(&config, None).defaults;
        let default_instance = default_instance.unwrap_or_else(|| {
            config
                .get(&["meerschaum", "instance"])
                .map(|v| v.py_str())
                .unwrap_or_else(|| "sql:main".into())
        });
        HostEnv {
            session,
            config,
            natives,
            defaults,
            default_instance,
        }
    }

    /// The connector `keys` from the session (built on first use).
    fn connector(&self, keys: &str) -> Result<crate::native::Instance, String> {
        blocking(|| self.session.instance(keys, &self.config)).map_err(|e| e.message)
    }

    /// The SQL connector `keys`.
    fn sql(&self, keys: &str) -> Result<Arc<meerschaum_connector_sql::SqlConnector>, String> {
        match self.connector(keys)? {
            crate::native::Instance::Sql(sql) => Ok(sql),
            crate::native::Instance::Other(_) => Err(format!(
                "'{keys}' is not a SQL connector: `read_sql` and `exec_sql` need `sql:<label>` keys."
            )),
        }
    }

    /// A sync engine registry over the session (for host syncs outside an engine run); its host
    /// calls share this config.
    fn registry(&self) -> NativeRegistry {
        let registry = NativeRegistry::with_config(
            self.natives.clone(),
            self.config.clone(),
            Arc::new(SessionRegistry(self.clone())),
        );
        registry.set_session(self.session.clone());
        (*registry).clone()
    }

    /// The plugin's config section (`plugins:<name>`), `null` when unset. A section a host call
    /// of this process wrote is read back from its file: this config may have been loaded
    /// before the write (another command in the shell, the next `--loop` run, another sync of
    /// a Python process).
    fn plugin_config(&self, plugin: &str) -> serde_json::Value {
        let value = if written_plugin_config(&self.config, plugin) {
            plugin_config_file(&self.config, plugin)
        } else {
            self.config.get_plugin_config(plugin, &[])
        };
        value
            .and_then(|value| serde_json::to_value(value).ok())
            .unwrap_or(serde_json::Value::Null)
    }

    /// `write_plugin_config(config)`: `plugins:<name>` replaced in the config directory's
    /// `plugins` file, which Python's `get_plugin_config()` reads, and in memory. The other
    /// plugins' sections stay as the file has them (`MRSM{}` references included).
    fn write_plugin_config(&self, plugin: &str, value: &serde_json::Value) -> SuccessTuple {
        use meerschaum_core::config::{ConfigMap, ConfigValue};
        let value: Value = match serde_json::from_value(value.clone()) {
            Ok(value) => value,
            Err(error) => return SuccessTuple::fail(format!("Invalid plugin config: {error}")),
        };
        let value = ConfigValue::from_value(&value);
        let (written, error) = self.config.with_state(|s| {
            // A `plugins` file that cannot be read is never replaced (it holds the other
            // plugins' settings).
            let on_disk = match s.read_config_lossless(
                None,
                Some(&["plugins".to_string()]),
                false,
                false,
                true,
            ) {
                Ok((on_disk, _)) => on_disk,
                Err(error) => {
                    let _ = s.take_notices();
                    return (
                        false,
                        Some((std::io::ErrorKind::InvalidData, error.to_string())),
                    );
                }
            };
            let mut plugins = on_disk
                .get("plugins")
                .and_then(ConfigValue::as_map)
                .cloned()
                .unwrap_or_default();
            plugins.insert(plugin.into(), value.clone());
            let mut section = ConfigMap::new();
            section.insert("plugins".into(), ConfigValue::Map(plugins));
            let written = s.write_config_lossless(Some(&section), None);
            // In memory too, when the section is loaded (else its first read loads the file).
            if written {
                let mut config = s.take_config_lossless();
                if let Some(in_memory) = config.get_mut("plugins").and_then(ConfigValue::as_map_mut)
                {
                    in_memory.insert(plugin.into(), value.clone());
                }
                s.set_config_lossless(config);
            }
            let _ = s.take_notices();
            (written, s.take_write_error())
        });
        if !written {
            let reason = match error {
                Some((_, message)) => format!(":\n{message}"),
                None if !self.config.with_state(|s| s.allow_writes()) => {
                    ": this Meerschaum does not write its config files here.".into()
                }
                None => ".".into(),
            };
            return SuccessTuple::fail(format!(
                "Failed to write the config of plugin '{plugin}'{reason}"
            ));
        }
        mark_plugin_config_written(&self.config, plugin);
        SuccessTuple::ok(format!("Wrote the config of plugin '{plugin}'."))
    }
}

/// A copy of `config` (its paths, defaults and in-memory config) for host calls that outlive
/// the borrow: an engine run's, a server's routes and pages.
pub fn host_config(
    config: &meerschaum_core::config::Config,
) -> Arc<meerschaum_core::config::Config> {
    Arc::new(duplicate_config(config))
}

fn duplicate_config(config: &meerschaum_core::config::Config) -> meerschaum_core::config::Config {
    let mut state = config.with_state(|s| s.clone());
    let _ = state.take_notices();
    let copy = meerschaum_core::config::Config::bare(
        state.paths().clone(),
        1,
        cfg!(windows),
        state.allow_writes(),
    );
    copy.with_state(|s| *s = state);
    copy
}

/// The plugin config sections host calls wrote in this process, by config directory. Their
/// files are what later host calls read (see [`HostEnv::plugin_config`]).
static WRITTEN_PLUGIN_CONFIGS: std::sync::Mutex<
    std::collections::BTreeSet<(std::path::PathBuf, String)>,
> = std::sync::Mutex::new(std::collections::BTreeSet::new());

fn written_key(
    config: &meerschaum_core::config::Config,
    plugin: &str,
) -> (std::path::PathBuf, String) {
    (config.paths().config_dir, plugin.to_string())
}

fn mark_plugin_config_written(config: &meerschaum_core::config::Config, plugin: &str) {
    WRITTEN_PLUGIN_CONFIGS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(written_key(config, plugin));
}

fn written_plugin_config(config: &meerschaum_core::config::Config, plugin: &str) -> bool {
    WRITTEN_PLUGIN_CONFIGS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&written_key(config, plugin))
}

/// `plugins:<plugin>` as the config directory's `plugins` file has it (`MRSM{}` resolved).
fn plugin_config_file(config: &meerschaum_core::config::Config, plugin: &str) -> Option<Value> {
    config.with_state(|s| {
        let read = s.read_config_lossless(None, Some(&["plugins".to_string()]), false, true, false);
        let _ = s.take_notices();
        read.ok()?
            .0
            .get("plugins")?
            .get(plugin)
            .map(meerschaum_core::config::ConfigValue::to_value)
    })
}

/// Whether native plugins wrote plugin config sections through host calls in this process that
/// [`take_written_plugin_configs`] has not taken yet.
pub fn has_written_plugin_configs() -> bool {
    !WRITTEN_PLUGIN_CONFIGS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_empty()
}

/// The plugin config sections native plugins wrote through host calls in this process into
/// `config`'s directory, with their values as written, forgotten here: for a host whose own
/// config lives elsewhere (the Python façade's live dictionary) to take them in.
pub fn take_written_plugin_configs(
    config: &meerschaum_core::config::Config,
) -> Vec<(String, serde_json::Value)> {
    let directory = config.paths().config_dir;
    let plugins: Vec<String> = {
        let mut written = WRITTEN_PLUGIN_CONFIGS
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let plugins = written
            .iter()
            .filter(|(dir, _)| *dir == directory)
            .map(|(_, plugin)| plugin.clone())
            .collect::<Vec<_>>();
        written.retain(|(dir, _)| *dir != directory);
        plugins
    };
    plugins
        .into_iter()
        .map(|plugin| {
            let value = plugin_config_file(config, &plugin)
                .and_then(|v| serde_json::to_value(v).ok())
                .unwrap_or(serde_json::Value::Null);
            (plugin, value)
        })
        .collect()
}

/// Run blocking work on a runtime thread: hand the worker off first (`block_in_place`) when
/// called from async code on a multi-threaded runtime.
fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// The connectors of a [`HostEnv`]'s session as an engine registry: what a host sync outside
/// an engine run (an action, a route) writes through.
struct SessionRegistry(HostEnv);

impl ConnectorRegistry for SessionRegistry {
    fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        self.0.connector(keys).map(|i| i.conn())
    }

    fn source(&self, keys: &str) -> Result<Option<Arc<dyn Connector>>, String> {
        if !matches!(keys.split(':').next(), Some("sql" | "api" | "valkey")) {
            return Ok(None);
        }
        let conn: Arc<dyn Connector> = self.instance(keys)?;
        Ok(Some(conn))
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        match self.0.connector(pipe.instance_keys()) {
            Ok(crate::native::Instance::Sql(sql)) => {
                let pipe = sql.sql_pipe(pipe).map_err(|e| e.to_string())?;
                Ok(sql.pipe_table_name(&pipe))
            }
            _ => pipe.target(None).map_err(|e| e.to_string()),
        }
    }
}

impl meerschaum_server::backends::InstanceResolver for HostEnv {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        self.connector(keys).ok().map(|instance| instance.conn())
    }
}

/// A host call made for an API caller (a route, MCP entry or console page): it may do what the
/// caller's scopes allow, on the instances the server allows.
#[derive(Clone)]
pub(crate) struct ApiAccess {
    state: meerschaum_server::AppState,
    /// The caller's granted scopes (`["*"]`: every scope).
    scopes: Vec<String>,
    /// Scopes the route or page grants its host calls for every caller (`host_scopes`; never
    /// `*`).
    host_scopes: Vec<String>,
    /// Who called (`None`: anonymous, or `--no-auth`).
    principal: Option<meerschaum_server::auth::Principal>,
}

/// Serves a native plugin's host calls (`meerschaum_plugin::HostServices`) in one context: an
/// engine run (fetch, sync, hooks, streams), an action, or an API call.
#[derive(Clone)]
pub(crate) struct PluginHost {
    /// The engine run's registry (its connectors, and the native layer `pipe.sync` goes
    /// through).
    registry: Option<NativeRegistry>,
    /// The process's connectors and config (from `registry` when unset).
    env: Option<HostEnv>,
    /// The sync options `pipe.sync(...)` on the request's Pipe inherits.
    args: SyncArgs,
    api: Option<ApiAccess>,
}

type HostResult<T> = meerschaum_plugin::Result<T>;

fn host_error(message: impl Into<String>) -> meerschaum_plugin::Error {
    meerschaum_plugin::Error::Message(message.into())
}

fn tuple_json(tuple: &SuccessTuple) -> serde_json::Value {
    serde_json::json!([tuple.success, tuple.message])
}

/// The refusal of an API caller's SQL (`read_sql`, `exec_sql`) that could reach Meerschaum's
/// internal tables, shared with MCP's `read_sql` over HTTP
/// ([`meerschaum_server::mcp::security::refuse_internal_tables`]).
fn refuse_internal_tables(query: &str) -> HostResult<()> {
    meerschaum_server::mcp::security::refuse_internal_tables(query).map_err(host_error)
}

/// A host call's bound as a `DateBound` (`None`: unbounded).
fn date_bound(
    call: &meerschaum_plugin::HostCall,
    name: &str,
) -> HostResult<Option<meerschaum_connector::DateBound>> {
    Ok(match call.bound(name)? {
        None => None,
        Some(meerschaum_plugin::Bound::Int(i)) => Some(meerschaum_connector::DateBound::Int(i)),
        Some(meerschaum_plugin::Bound::Datetime(text)) => Some(
            meerschaum_connector::DateBound::parse(&text)
                .ok_or_else(|| host_error(format!("`{name}` is not a timestamp: '{text}'")))?,
        ),
    })
}

/// A JSON object argument as a core map (`None` for `null`).
fn map_arg(call: &meerschaum_plugin::HostCall, name: &str) -> HostResult<Option<Map>> {
    match call.arg(name) {
        serde_json::Value::Null => Ok(None),
        value @ serde_json::Value::Object(_) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|e| host_error(format!("invalid `{name}`: {e}"))),
        _ => Err(host_error(format!("`{name}` must be a JSON object"))),
    }
}

/// The `DataQuery` of a `get_data` / `get_rowcount` / `clear` call.
fn data_query(call: &meerschaum_plugin::HostCall) -> HostResult<meerschaum_connector::DataQuery> {
    let strings = |name: &str| -> HostResult<Option<Vec<String>>> {
        match call.arg(name) {
            serde_json::Value::Null => Ok(None),
            value => serde_json::from_value(value.clone())
                .map(Some)
                .map_err(|e| host_error(format!("invalid `{name}`: {e}"))),
        }
    };
    Ok(meerschaum_connector::DataQuery {
        begin: date_bound(call, "begin")?,
        end: date_bound(call, "end")?,
        params: map_arg(call, "params")?,
        select_columns: strings("select_columns")?,
        omit_columns: strings("omit_columns")?,
        limit: call.arg("limit").as_i64(),
        order: call.str_arg("order").map(str::to_owned),
        ..Default::default()
    })
}

/// A Pipe's metadata as plugins see it (`{"keys", "parameters", "temporary"}`).
fn pipe_metadata(pipe: &Pipe) -> serde_json::Value {
    serde_json::json!({
        "keys": serde_json::to_value(pipe.keys()).unwrap_or_default(),
        "parameters": serde_json::to_value(pipe.raw_parameters()).unwrap_or_default(),
        "temporary": pipe.temporary,
    })
}

impl PluginHost {
    /// Host calls during an engine run (fetch, sync, hooks, streams).
    fn engine(registry: NativeRegistry, args: SyncArgs) -> PluginHost {
        PluginHost {
            registry: Some(registry),
            env: None,
            args,
            api: None,
        }
    }

    /// Host calls with the full trust of a local process (an action, MCP over stdio).
    pub(crate) fn local(env: HostEnv) -> PluginHost {
        PluginHost {
            registry: None,
            env: Some(env),
            args: SyncArgs::default(),
            api: None,
        }
    }

    /// Host calls for the API caller `principal` with `scopes` (`["*"]` grants every scope).
    pub(crate) fn api(
        env: HostEnv,
        state: meerschaum_server::AppState,
        scopes: Vec<String>,
        principal: Option<meerschaum_server::auth::Principal>,
    ) -> PluginHost {
        PluginHost {
            registry: None,
            env: Some(env),
            args: SyncArgs::default(),
            api: Some(ApiAccess {
                state,
                scopes,
                host_scopes: Vec::new(),
                principal,
            }),
        }
    }

    /// Also grant the host calls `scopes` for this caller (a route's or page's `host_scopes`).
    /// `*` is ignored: it would hand anonymous callers the whole host.
    fn with_host_scopes(mut self, scopes: &[String]) -> PluginHost {
        if let Some(api) = &mut self.api {
            api.host_scopes = scopes.iter().filter(|s| *s != "*").cloned().collect();
        }
        self
    }

    fn env(&self) -> HostResult<HostEnv> {
        match (&self.env, &self.registry) {
            (Some(env), _) => Ok(env.clone()),
            (None, Some(registry)) => registry.host_env().map_err(host_error),
            (None, None) => Err(host_error("no Meerschaum process serves these host calls")),
        }
    }

    /// Refuse what only a caller with every scope (an administrator, `--no-auth`) may do.
    async fn require_all(&self, what: &str) -> HostResult<()> {
        if let Some(access) = self.api_access("*", what).await?
            && !access.full_admin()
        {
            return Err(host_error(format!(
                "The caller may not {what} from an API request."
            )));
        }
        Ok(())
    }

    /// Resolve the caller again for each API host call. A named plugin grant adds a
    /// permission, never an identity, administrator status, or team membership.
    async fn api_access(
        &self,
        scope: &str,
        what: &str,
    ) -> HostResult<Option<meerschaum_server::access::Access>> {
        let Some(api) = &self.api else {
            return Ok(None);
        };
        use meerschaum_server::access::{Access, EVERYONE, TeamRole, rules};
        let mut access = if api.state.config.no_auth {
            Access::everything()
        } else if let Some(principal) = &api.principal {
            // A host call may follow a long-running plugin operation: reload credential
            // validity and scope narrowing as well as its owner's current access.
            use meerschaum_server::auth::{Principal, Scopes};
            let subject = match principal {
                Principal::User(user) => user.username.clone(),
                Principal::Token(token) => token.id.map(|id| id.to_string()).unwrap_or_default(),
            };
            let principal = meerschaum_server::auth::load_from_subject(&api.state, &subject)
                .await
                .map_err(|e| host_error(e.detail_text()))?
                .ok_or_else(|| host_error("Invalid or revoked credentials."))?;
            let mut access = meerschaum_server::access::access_of_fresh(
                &api.state,
                principal.clone(),
                &Scopes::List(api.scopes.clone()),
            )
            .await
            .map_err(|e| host_error(e.detail_text()))?;
            if let Principal::Token(token) = &principal {
                access.principal.permissions = meerschaum_server::access::intersect(
                    &access.principal.permissions,
                    &Scopes::List(token.scopes.clone()),
                );
            }
            // The request already resolved legacy scope spellings. Keep that exact
            // ceiling even if the user's permissions broaden while the plugin runs.
            if !api.scopes.iter().any(|scope| scope == "*") {
                access
                    .principal
                    .permissions
                    .retain(|permission| api.scopes.contains(permission));
            }
            access
        } else {
            Access {
                auth: None,
                principal: rules::Principal::new(
                    "",
                    false,
                    Default::default(),
                    vec![(EVERYONE.into(), TeamRole::Member)],
                ),
                primary_team: EVERYONE.into(),
                attributes: None,
                no_auth: false,
            }
        };
        // Reading the process config requires the caller's own full administrator rights;
        // a plugin may not assemble them from named host grants.
        if scope != "*" {
            access
                .principal
                .permissions
                .extend(api.host_scopes.iter().cloned());
        }
        if !access.has(scope) {
            return Err(host_error(format!(
                "The caller may not {what}: that needs the `{scope}` scope."
            )));
        }
        Ok(Some(access))
    }

    /// API pipe handles are descriptions from untrusted plugin memory. Always authorize
    /// the instance's registered parameters, and use supplied parameters only for a new
    /// pipe after the same registration and definition guards as the HTTP routes.
    async fn api_pipe(
        &self,
        mut pipe: Pipe,
        instance: &Arc<dyn InstanceConnector>,
        access: Option<&meerschaum_server::access::Access>,
        method: &str,
    ) -> HostResult<Pipe> {
        let (Some(api), Some(access)) = (&self.api, access) else {
            return Ok(pipe);
        };
        use meerschaum_plugin::callbacks::method as m;
        use meerschaum_server::access::{self, guard};
        for keys in [pipe.instance_keys(), pipe.connector_keys()] {
            guard::check_connector_teams(&api.state, access, keys)
                .map_err(|e| host_error(e.detail_text()))?;
        }
        let stored = api
            .state
            .get_pipe(
                pipe.connector_keys(),
                pipe.metric_key(),
                pipe.location_key(),
                Some(pipe.instance_keys()),
                true,
            )
            .await
            .map_err(|e| host_error(e.detail_text()))?;
        let registered = api
            .state
            .is_registered(&stored.pipe, Some(pipe.instance_keys()))
            .await
            .map_err(|e| host_error(e.detail_text()))?;
        let writes = matches!(method, m::SYNC | m::CLEAR | m::REGISTER);
        if registered || !matches!(method, m::SYNC | m::REGISTER) {
            pipe = stored.pipe.clone();
        } else {
            access
                .require(&["pipes:register"], &[])
                .map_err(|e| host_error(e.detail_text()))?;
            if !api.state.config.registration_pipes {
                return Err(host_error("The server has not allowed pipe registration."));
            }
            let parameters = guard::stamp_new_pipe(access, pipe.raw_parameters())
                .map_err(|e| host_error(e.detail_text()))?;
            guard::check_parameters(&api.state, access, instance, &pipe, None, &parameters)
                .await
                .map_err(|e| host_error(e.detail_text()))?;
            pipe.set_parameters(parameters);
            pipe.temporary = false;
        }
        if !access.can_see_pipe(&pipe) {
            return Err(host_error(access::hidden_pipe(&pipe).detail_text()));
        }
        if writes && !access.can_edit(pipe.raw_parameters()) {
            return Err(host_error(access::not_editable(&pipe).detail_text()));
        }
        let index = access::pipe_index(&api.state, Some(pipe.instance_keys()))
            .await
            .map_err(|e| host_error(e.detail_text()))?;
        // Internal tables stay unavailable even to administrator-owned narrow tokens, as
        // with API data routes and raw SQL. Administrators have no other hidden targets.
        guard::check_pipe_target(instance.as_ref(), &pipe, &index.hidden_targets(access))
            .map_err(|e| host_error(e.detail_text()))?;
        Ok(pipe)
    }

    /// Refuse `exec_sql` to an API caller who may not run actions, by the rule of the server's
    /// `/actions` route and MCP's `execute_action` (`is_user_allowed_to_execute`): an
    /// administrator may; another user or a token only when the server lets non-administrators
    /// run actions (`system:api:permissions:actions:non_admin`), and never under `--secure`; an
    /// anonymous caller never (except under `--no-auth`).
    async fn may_execute(&self) -> HostResult<()> {
        let Some(api) = &self.api else {
            return Ok(());
        };
        if api.state.config.no_auth {
            return Ok(());
        }
        let access = self
            .api_access("actions:execute", "execute SQL statements")
            .await?;
        let Some(principal) = access.as_ref().and_then(|access| access.auth.as_ref()) else {
            return Err(host_error(
                "An anonymous caller may not execute SQL statements.",
            ));
        };
        let non_admins =
            api.state.config.system_actions_non_admin && api.state.config.actions_non_admin;
        let allowed = meerschaum_server::auth::is_user_allowed_to_execute_with(
            &api.state,
            Some(principal),
            non_admins,
        )
        .await;
        if !allowed.success {
            return Err(host_error(format!(
                "The caller may not execute SQL statements: {}",
                allowed.message
            )));
        }
        Ok(())
    }

    /// The instance connector `keys`: the run's, else the session's; an API caller only gets the
    /// instances the server allows.
    fn instance(&self, keys: &str) -> HostResult<Arc<dyn InstanceConnector>> {
        if let Some(api) = &self.api {
            return api.state.get_api_connector(Some(keys)).map_err(|e| {
                host_error(
                    e.detail
                        .as_ref()
                        .and_then(|d| d.as_str())
                        .unwrap_or("This instance is not available.")
                        .to_string(),
                )
            });
        }
        if let Some(registry) = &self.registry
            && let Ok(instance) = registry.instance(keys)
        {
            return Ok(instance);
        }
        self.env()?
            .connector(keys)
            .map(|i| i.conn())
            .map_err(host_error)
    }

    /// The Pipe a call addresses: the request's (`call.pipe` is `None`) or the one it describes.
    fn pipe_for(
        &self,
        request: &meerschaum_plugin::Request,
        call: &meerschaum_plugin::HostCall,
    ) -> HostResult<Pipe> {
        let metadata = match &call.pipe {
            Some(metadata) => metadata,
            None => &request.context["pipe"],
        };
        if !metadata.is_object() {
            return Err(host_error(format!(
                "`pipe.{}()` needs a Pipe, and this call has none: get one with `host::pipe(...)`.",
                call.method
            )));
        }
        let mut state = metadata.get("keys").unwrap_or(metadata).clone();
        state["parameters"] = metadata
            .get("parameters")
            .filter(|p| p.is_object())
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let state: Map = serde_json::from_value(state).map_err(|e| host_error(e.to_string()))?;
        let defaults = match &self.registry {
            Some(registry) => registry.engine_config.defaults.clone(),
            None => self.env()?.defaults,
        };
        let mut pipe =
            Pipe::from_state(&state, &defaults).map_err(|e| host_error(e.to_string()))?;
        pipe.temporary = metadata["temporary"].as_bool().unwrap_or(false);
        self.refuse_internal(pipe.connector_keys())?;
        Ok(pipe)
    }

    /// API callers never reach Meerschaum's internal `mrsm` pipes (as the HTTP routes refuse).
    fn refuse_internal(&self, connector_keys: &str) -> HostResult<()> {
        if self.api.is_some() && connector_keys == "mrsm" {
            return Err(host_error(
                "Meerschaum's internal `mrsm` pipes are not available from API requests.",
            ));
        }
        Ok(())
    }

    /// The refusal of a write to `pipe` on a protected instance (`--allow-protected` and
    /// `MRSM_ALLOW_PROTECTED=1` allow it), as the core write paths refuse it.
    fn protected(
        instance: &dyn InstanceConnector,
        pipe: &Pipe,
        op: &str,
    ) -> Option<meerschaum_plugin::HostReply> {
        meerschaum_connector::protected_refusal(instance, pipe, op)
            .map(|refused| meerschaum_plugin::HostReply::json(tuple_json(&refused)))
    }

    /// The sync engine `pipe.sync(...)` writes through: the run's, else (outside a run, or for
    /// a Pipe on an instance the run does not use) one over the session's connectors, the way
    /// [`instance`](Self::instance) reaches them.
    fn engine_for_sync(&self, pipe: &Pipe) -> HostResult<meerschaum_engine::Engine> {
        let registry = match &self.registry {
            Some(registry) if registry.instance(pipe.instance_keys()).is_ok() => registry.clone(),
            _ => self.env()?.registry(),
        };
        let config = registry.engine_config.clone();
        Ok(meerschaum_engine::Engine::new(Arc::new(registry), config))
    }

    async fn serve(
        &self,
        request: &meerschaum_plugin::Request,
        call: &meerschaum_plugin::HostCall,
        data: Option<PipeInput>,
    ) -> HostResult<meerschaum_plugin::HostReply> {
        use meerschaum_plugin::HostReply;
        use meerschaum_plugin::callbacks::method as m;
        let json = HostReply::json;
        match call.method.as_str() {
            m::PLUGIN_CONFIG => Ok(json(self.env()?.plugin_config(&call.plugin))),
            m::WRITE_PLUGIN_CONFIG => {
                let written = self
                    .env()?
                    .write_plugin_config(&call.plugin, call.arg("config"));
                Ok(json(tuple_json(&written)))
            }
            m::CONFIG => {
                self.require_all("read the Meerschaum config").await?;
                let keys: Vec<String> = serde_json::from_value(call.arg("keys").clone())
                    .map_err(|e| host_error(format!("invalid `keys`: {e}")))?;
                let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
                Ok(json(
                    self.env()?
                        .config
                        .get(&keys)
                        .and_then(|value| serde_json::to_value(value).ok())
                        .unwrap_or_default(),
                ))
            }
            m::READ_SQL | m::EXEC_SQL => {
                let keys = call
                    .str_arg("connector_keys")
                    .ok_or_else(|| host_error("`connector_keys` names no connector"))?;
                let query = call.str_arg("query").unwrap_or_default();
                // Permissions first: an API caller learns nothing about connectors it may not use.
                let access = if call.method == m::READ_SQL {
                    let access = self.api_access("sql:read", "run SQL queries").await?;
                    if self.api.is_some() {
                        meerschaum_server::mcp::security::is_read_only_query(query)
                            .map_err(host_error)?;
                    }
                    access
                } else {
                    let access = self
                        .api_access("actions:execute", "execute SQL statements")
                        .await?;
                    self.may_execute().await?;
                    access
                };
                if self.api.is_some() {
                    refuse_internal_tables(query)?;
                }
                if let (Some(api), Some(access)) = (&self.api, &access) {
                    // Named SQL must use a served instance, within its connector's teams.
                    self.instance(keys)?;
                    meerschaum_server::access::guard::check_connector_teams(
                        &api.state, access, keys,
                    )
                    .map_err(|e| host_error(e.detail_text()))?;
                    let index = meerschaum_server::access::pipe_index(&api.state, Some(keys))
                        .await
                        .map_err(|e| host_error(e.detail_text()))?;
                    meerschaum_server::sql_guard::check_definition(
                        query,
                        &index.hidden_targets(access),
                    )
                    .map_err(host_error)?;
                }
                let sql = self.env()?.sql(keys).map_err(host_error)?;
                if call.method == m::READ_SQL {
                    // A protected connector runs only a read statement, read-only.
                    let frame = sql
                        .read_raw(query)
                        .await
                        .map_err(|e| host_error(e.to_string()))?;
                    return Ok(HostReply::data(Some(frame)));
                }
                if let Err(refused) = meerschaum_connector::check_protected(&*sql, "execute SQL") {
                    return Ok(json(tuple_json(&SuccessTuple::fail(refused))));
                }
                Ok(json(tuple_json(&match sql.driver().exec(query).await {
                    Ok(rows) => SuccessTuple::ok(format!("Executed the statement ({rows} rows).")),
                    Err(error) => SuccessTuple::fail(format!(
                        "Failed to execute the statement on '{keys}':\n{error}"
                    )),
                })))
            }
            m::PIPE => {
                let access = self.api_access("pipes:read", "read pipes").await?;
                let connector_keys = call
                    .str_arg("connector_keys")
                    .ok_or_else(|| host_error("`host::pipe()` needs connector keys"))?;
                let metric_key = call
                    .str_arg("metric_key")
                    .ok_or_else(|| host_error("`host::pipe()` needs a metric key"))?;
                self.refuse_internal(connector_keys)?;
                // The request's Pipe is `{"keys": {...}, ...}` in most calls and flat in a
                // plugin's own `sync` (its keyword arguments' `pipe`).
                let request_pipe = &request.context["pipe"];
                let request_keys = request_pipe.get("keys").unwrap_or(request_pipe);
                let instance_keys = match call.str_arg("instance_keys") {
                    Some(keys) => keys.to_string(),
                    None => match request_keys["instance_keys"].as_str() {
                        Some(keys) if !keys.is_empty() => keys.to_string(),
                        _ => self.env()?.default_instance,
                    },
                };
                let defaults = match &self.registry {
                    Some(registry) => registry.engine_config.defaults.clone(),
                    None => self.env()?.defaults,
                };
                let mut pipe = Pipe::builder(connector_keys, metric_key)
                    .location(call.str_arg("location_key"))
                    .instance(&instance_keys)
                    .defaults(defaults)
                    .build()
                    .map_err(|e| host_error(e.to_string()))?;
                let instance = self.instance(&instance_keys)?;
                if self.api.is_some() {
                    pipe = self
                        .api_pipe(pipe, &instance, access.as_ref(), m::PIPE)
                        .await?;
                } else if let Ok(attributes) = instance.get_pipe_attributes(&pipe).await
                    && let Some(Value::Map(parameters)) = attributes.get("parameters")
                {
                    pipe.set_parameters(parameters.clone());
                }
                Ok(json(pipe_metadata(&pipe)))
            }
            method => {
                let (scope, what) = match method {
                    m::SYNC => ("pipes:write", "sync pipes"),
                    // PARITY: wave 2's permission grid supersedes the original host-call
                    // write alias: deleting rows and registering each need their own scope.
                    m::CLEAR => ("pipes:delete", "clear pipes"),
                    m::REGISTER => ("pipes:register", "register pipes"),
                    m::GET_SYNC_TIME
                    | m::GET_DATA
                    | m::GET_ROWCOUNT
                    | m::GET_COLUMNS_TYPES
                    | m::EXISTS => ("pipes:read", "read pipes"),
                    other => {
                        return Err(meerschaum_plugin::Error::Unsupported(format!(
                            "unsupported host call: {other}"
                        )));
                    }
                };
                let access = self.api_access(scope, what).await?;
                let pipe = self.pipe_for(request, call)?;
                let instance = self.instance(pipe.instance_keys())?;
                let pipe = self
                    .api_pipe(pipe, &instance, access.as_ref(), method)
                    .await?;
                match method {
                    m::GET_SYNC_TIME => {
                        let newest = call.arg("newest").as_bool().unwrap_or(true);
                        let bound = instance
                            .get_sync_time(&pipe, map_arg(call, "params")?.as_ref(), newest)
                            .await
                            .map_err(|e| host_error(e.to_string()))?;
                        Ok(json(match bound {
                            None => serde_json::Value::Null,
                            Some(meerschaum_connector::DateBound::Int(i)) => serde_json::json!(i),
                            Some(other) => serde_json::json!(other.isoformat()),
                        }))
                    }
                    m::SYNC => {
                        if let Some(refused) = Self::protected(&*instance, &pipe, "sync") {
                            return Ok(refused);
                        }
                        // `pipe.replace(rows, begin, end)`: the window's rows are replaced in one
                        // transaction (a `null` bound comes from the rows); no rows empty it.
                        let replace = call.arg("replace").as_bool() == Some(true);
                        let data = match data {
                            Some(data) => data,
                            None if replace => PipeInput::Frame(meerschaum_plugin::Frame::empty()),
                            None => {
                                return Ok(json(serde_json::json!([true, "No data to sync."])));
                            }
                        };
                        // The request's Pipe syncs with the surrounding sync's options; any other
                        // Pipe with the defaults (its own window, never the caller's `replace`).
                        let mut args = if call.pipe.is_none() {
                            self.args.clone()
                        } else {
                            SyncArgs::default()
                        };
                        if replace {
                            args.begin = match date_bound(call, "begin")? {
                                Some(begin) => SyncBegin::At(begin),
                                None => SyncBegin::Default,
                            };
                            args.end = date_bound(call, "end")?;
                            args.replace = true;
                            args.extra.shift_remove("replace");
                        }
                        // Supplying input bypasses plugin_sync and fetch; no recursive plugin call.
                        let result = self
                            .engine_for_sync(&pipe)?
                            .sync_pipe(&pipe, Some(data), &args)
                            .await;
                        if result.success
                            && let Some(api) = &self.api
                        {
                            api.state.access_cache().forget_pipes();
                        }
                        Ok(json(serde_json::json!([result.success, result.message])))
                    }
                    m::GET_DATA => {
                        let query = data_query(call)?;
                        let frame =
                            meerschaum_connector::pipe_ops::get_data(&*instance, &pipe, &query)
                                .await
                                .map_err(|e| host_error(e.to_string()))?;
                        Ok(HostReply::data(frame))
                    }
                    m::GET_ROWCOUNT => {
                        let count =
                            match instance.get_pipe_rowcount(&pipe, &data_query(call)?).await {
                                Ok(count) => count.unwrap_or(0),
                                Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => 0,
                                Err(e) => return Err(host_error(e.to_string())),
                            };
                        Ok(json(serde_json::json!(count)))
                    }
                    m::CLEAR => {
                        if let Some(refused) = Self::protected(&*instance, &pipe, "clear") {
                            return Ok(refused);
                        }
                        if self.api.is_some()
                            && let Some(error) = meerschaum_server::access::guard::lock_conflict(
                                &pipe,
                                meerschaum_core::access::LockOp::Clear,
                            )
                        {
                            return Err(host_error(error.detail_text()));
                        }
                        let cleared = instance.clear_pipe(&pipe, &data_query(call)?).await;
                        Ok(json(tuple_json(&cleared)))
                    }
                    m::GET_COLUMNS_TYPES => {
                        let types = match instance.get_pipe_columns_types(&pipe).await {
                            Ok(types) => types,
                            Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => {
                                Default::default()
                            }
                            Err(e) => return Err(host_error(e.to_string())),
                        };
                        Ok(json(serde_json::to_value(types).unwrap_or_default()))
                    }
                    m::EXISTS => {
                        let exists = instance
                            .pipe_exists(&pipe)
                            .await
                            .map_err(|e| host_error(e.to_string()))?;
                        Ok(json(serde_json::json!(exists)))
                    }
                    m::REGISTER => {
                        if let Some(refused) = Self::protected(&*instance, &pipe, "register") {
                            return Ok(refused);
                        }
                        let result = instance.register_pipe(&pipe).await;
                        if result.success
                            && let Some(api) = &self.api
                        {
                            api.state.access_cache().forget_pipes();
                        }
                        Ok(json(tuple_json(&result)))
                    }
                    other => Err(meerschaum_plugin::Error::Unsupported(format!(
                        "unsupported host call: {other}"
                    ))),
                }
            }
        }
    }
}

/// Host services with the full trust of the local process, for native plugin calls a host
/// makes outside an engine run (the Python façade's native connectors).
pub fn local_host(env: HostEnv) -> Arc<dyn meerschaum_plugin::HostServices> {
    Arc::new(PluginHost::local(env))
}

impl meerschaum_plugin::HostServices for PluginHost {
    fn call(
        &self,
        request: &meerschaum_plugin::Request,
        method: &str,
        data: Option<meerschaum_plugin::Frame>,
    ) -> meerschaum_plugin::Result<serde_json::Value> {
        self.call_input(request, method, data.map(PipeInput::Frame))
    }

    /// A single-file plugin's `pipe.sync(rows)` syncs its rows as documents, like a Python
    /// plugin's `pipe.sync(docs)`.
    fn call_input(
        &self,
        request: &meerschaum_plugin::Request,
        method: &str,
        data: Option<PipeInput>,
    ) -> meerschaum_plugin::Result<serde_json::Value> {
        let call = meerschaum_plugin::HostCall::legacy("", method);
        self.host_call(request, &call, data)
            .map(|reply| reply.value)
    }

    fn host_call(
        &self,
        request: &meerschaum_plugin::Request,
        call: &meerschaum_plugin::HostCall,
        data: Option<PipeInput>,
    ) -> meerschaum_plugin::Result<meerschaum_plugin::HostReply> {
        // Native fetch and sync execute on blocking threads; actions on the CLI's thread. The
        // runtime stays free to service SQL/network operations while this callback waits.
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => self.env()?.session.handle(),
        };
        blocking(|| handle.block_on(self.serve(request, call, data)))
    }
}

impl ConnectorRegistry for NativeRegistry {
    fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        let (kind, label) = keys.split_once(':').unwrap_or((keys, ""));
        let attributes = attributes(&self.connectors, kind, label);
        match instance_connector(&self.natives, keys, &attributes) {
            Some(result) => result,
            None => self.inner.instance(keys),
        }
    }

    fn source(&self, keys: &str) -> Result<Option<Arc<dyn Connector>>, String> {
        match self.source_connector(keys) {
            Some(result) => Ok(Some(Arc::new(result?))),
            None => self.inner.source(keys),
        }
    }

    fn fetch_chunksize(&self, keys: &str) -> usize {
        self.inner.fetch_chunksize(keys)
    }

    fn source_for_sync(
        &self,
        keys: &str,
        args: &SyncArgs,
    ) -> Result<Option<Arc<dyn Connector>>, String> {
        match self.source_connector(keys) {
            Some(result) => {
                // The same fetch kwargs a Python plugin's `fetch(pipe, **kw)` receives.
                let kwargs = meerschaum_engine::sync::sync_kwargs(args);
                Ok(Some(Arc::new(
                    result?
                        .with_fetch_kwargs(&kwargs)
                        .with_host_services(Arc::new(PluginHost::engine(
                            self.clone(),
                            args.clone(),
                        ))),
                )))
            }
            None => self.inner.source_for_sync(keys, args),
        }
    }

    fn plugin_sync(&self, keys: &str) -> Option<Arc<dyn PluginSync>> {
        if let Some((kind, label)) = keys.split_once(':') {
            let overrides_sync = if kind == "plugin" {
                self.natives.has_data_function(label, "sync")
            } else {
                self.natives.connector_plugin(kind).is_some()
                    && self.natives.manifest()["connector_types"][kind]["methods"]
                        .as_array()
                        .is_some_and(|methods| methods.iter().any(|m| m == "sync"))
            };
            if overrides_sync {
                let connector = if kind == "plugin" {
                    // A sync-only plugin has no fetch source in the registry.
                    self.natives
                        .plugin(label)
                        .ok()?
                        .source(serde_json::Value::Null)
                } else {
                    self.source_connector(keys)?.ok()?
                };
                return Some(Arc::new(NativeSync(connector, self.clone())));
            }
        }
        self.inner.plugin_sync(keys)
    }

    fn stream_source(&self, keys: &str) -> Option<Arc<dyn StreamSource>> {
        if let Some((kind, label)) = keys.split_once(':') {
            // A streaming native connector (pull-model `subscribe`, see `native_streams`).
            let connector = if kind == "plugin" {
                self.natives
                    .stream_source(label)
                    .and_then(Result::ok)
                    .map(|c| c.with_host_services(self.pipe_host()))
            } else {
                self.source_connector(keys).and_then(Result::ok)
            };
            if let Some(connector) = connector.filter(|c| c.is_streaming()) {
                let kwargs = serde_json::to_value(
                    &*self.stream_kwargs.read().unwrap_or_else(|e| e.into_inner()),
                )
                .unwrap_or_default();
                return Some(Arc::new(crate::native_streams::NativeStreamSource::new(
                    connector, kwargs,
                )));
            }
        }
        self.inner.stream_source(keys)
    }

    fn legacy_stream(&self, keys: &str) -> Option<Arc<dyn LegacyStream>> {
        self.inner.legacy_stream(keys)
    }

    fn is_python(&self, keys: &str) -> bool {
        if let Some(("plugin", name)) = keys.split_once(':')
            && self.natives.contains(name)
        {
            return false;
        }
        self.inner.is_python(keys)
    }

    fn hooks(&self) -> Option<Arc<dyn SyncHooks>> {
        let inner = self.inner.hooks();
        if self.natives.hooks(false).is_empty() && self.natives.hooks(true).is_empty() {
            return inner;
        }
        Some(Arc::new(NativeHooks {
            natives: self.natives.clone(),
            inner,
            reporter: self.hook_reporter.clone(),
            registry: self.clone(),
        }))
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        self.inner.table_reference(pipe)
    }
}

// ---------------------------------------------------------------------------------------------
// Server extensions: native routes (`@api_plugin` equivalent) and console pages (`@web_page`).
// ---------------------------------------------------------------------------------------------

/// The `host_scopes` of each declared route, by plugin and route name.
type RouteHostScopes = std::collections::HashMap<(String, String), Vec<String>>;

/// The `host_scopes` a route or page declares, without `*` (each one is a notice).
fn host_scopes_of(
    declared: &serde_json::Value,
    what: &str,
    notices: &mut Vec<String>,
) -> Vec<String> {
    let scopes: Vec<String> = declared
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|s| s.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    if scopes.iter().any(|s| s == "*") {
        notices.push(format!(
            "{what}: `host_scopes` may not contain '*' (ignored); name the scopes instead."
        ));
    }
    scopes.into_iter().filter(|s| s != "*").collect()
}

/// The routes the native plugins declare: their manifest's `api_routes` (served by the
/// `api_route` call kind), or, for plugins written before those existed, the routes a plugin
/// lists from its `routes` method when its manifest names an `api_plugins` entry (served by the
/// `route` call kind with the original text-body context).
fn declared_routes(
    natives: &NativePlugins,
    notices: &mut Vec<String>,
) -> (
    Vec<meerschaum_server::native::NativeRoute>,
    std::collections::HashSet<String>,
    RouteHostScopes,
) {
    use meerschaum_server::native::NativeRoute;
    let mut routes = Vec::new();
    let mut legacy = std::collections::HashSet::new();
    let mut host_scopes = RouteHostScopes::new();
    for name in natives.names() {
        let entry = natives.plugin_entry(&name);
        match meerschaum_plugin::api::routes_of(entry) {
            Ok(specs) if !specs.is_empty() => {
                for spec in specs {
                    let granted = host_scopes_of(
                        &serde_json::json!(spec.host_scopes),
                        &format!("Native plugin '{name}' route '{}'", spec.name),
                        notices,
                    );
                    if !granted.is_empty() {
                        host_scopes.insert((name.clone(), spec.name.clone()), granted);
                    }
                    routes.push(NativeRoute {
                        plugin: name.clone(),
                        name: spec.name,
                        method: spec.method,
                        path: spec.path,
                        scopes: spec.scopes,
                        public: spec.public,
                    });
                }
                continue;
            }
            Ok(_) => {}
            Err(error) => {
                notices.push(format!(
                    "Native plugin '{name}' routes are disabled: invalid `api_routes`: {error}"
                ));
                continue;
            }
        }
        if !entry["api_plugins"]
            .as_array()
            .is_some_and(|a| !a.is_empty())
        {
            continue;
        }
        let listed = natives
            .plugin(&name)
            .and_then(|plugin| plugin.call_json("routes", serde_json::json!({})));
        let listed = match listed {
            Ok(listed) => listed,
            Err(error) => {
                notices.push(format!(
                    "Native plugin '{name}' routes are disabled: {error}"
                ));
                continue;
            }
        };
        legacy.insert(name.clone());
        for entry in listed.as_array().cloned().unwrap_or_default() {
            let path = entry["path"].as_str().unwrap_or_default().to_string();
            let method = entry["method"].as_str().unwrap_or("GET").to_string();
            routes.push(NativeRoute {
                plugin: name.clone(),
                name: format!("{method} {path}"),
                method,
                // The original routes had no path parameters.
                public: entry["public"].as_bool().unwrap_or(false),
                scopes: Vec::new(),
                path,
            });
        }
    }
    (routes, legacy, host_scopes)
}

/// The router serving the routes native plugins declare (see `declared_routes` and
/// `meerschaum_server::native`). Refused routes (a core route they would shadow, a conflict,
/// bad syntax) are skipped; each is one notice.
pub fn routes(
    state: &meerschaum_server::AppState,
    natives: &Arc<NativePlugins>,
    env: &HostEnv,
) -> (axum::Router, Vec<String>) {
    let mut notices = Vec::new();
    let (routes, legacy, host_scopes) = declared_routes(natives, &mut notices);
    if routes.is_empty() {
        return (axum::Router::new(), notices);
    }
    let handler = Arc::new(RouteHandler {
        natives: natives.clone(),
        legacy,
        host_scopes,
        limits: CallLimits::default(),
        env: env.clone(),
        state: state.clone(),
    });
    let (router, refused) = meerschaum_server::native::native_router(state, routes, handler);
    notices.extend(refused);
    (router, notices)
}

struct RouteHandler {
    natives: Arc<NativePlugins>,
    /// Plugins served through the original `route` call kind.
    legacy: std::collections::HashSet<String>,
    /// What each route's host calls may also do for every caller.
    host_scopes: RouteHostScopes,
    limits: CallLimits,
    /// The handlers' host calls, made for the caller.
    env: HostEnv,
    state: meerschaum_server::AppState,
}

/// The most server calls (`api_route`, `mcp_*`) one native plugin runs at once. They run in
/// parallel, each on a blocking thread; up to [`MAX_QUEUED_CALLS`] more wait up to
/// [`CALL_QUEUE_TIMEOUT`] for a slot and any beyond that are answered `503` at once, so a slow or
/// stuck handler holds at most this many threads and buffered route bodies (a route's body is read
/// only once it holds a slot) instead of piling them up.
const MAX_CONCURRENT_CALLS: usize = 32;
/// How many server calls may wait for one of a plugin's [`MAX_CONCURRENT_CALLS`] slots.
const MAX_QUEUED_CALLS: usize = 16;
/// How long a server call waits for one of its plugin's [`MAX_CONCURRENT_CALLS`] slots.
const CALL_QUEUE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// One plugin's call slots and the number of calls waiting for one.
#[derive(Clone)]
struct PluginSlots {
    semaphore: Arc<tokio::sync::Semaphore>,
    waiting: Arc<std::sync::atomic::AtomicUsize>,
}

/// Leaves a plugin's wait queue when dropped (including when the waiting request is cancelled).
struct Waiting(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// One semaphore of [`MAX_CONCURRENT_CALLS`] slots per native plugin, with a wait queue of at most
/// [`MAX_QUEUED_CALLS`].
#[derive(Clone)]
struct CallLimits {
    plugins: Arc<std::sync::Mutex<std::collections::HashMap<String, PluginSlots>>>,
    slots: usize,
    queue: usize,
    timeout: std::time::Duration,
}

impl Default for CallLimits {
    fn default() -> Self {
        Self::new(MAX_CONCURRENT_CALLS, MAX_QUEUED_CALLS, CALL_QUEUE_TIMEOUT)
    }
}

impl CallLimits {
    fn new(slots: usize, queue: usize, timeout: std::time::Duration) -> Self {
        Self {
            plugins: Default::default(),
            slots,
            queue,
            timeout,
        }
    }

    /// A slot of `plugin`'s calls, or the message to answer when the queue is full or no slot
    /// frees up in time.
    async fn acquire(&self, plugin: &str) -> Result<tokio::sync::OwnedSemaphorePermit, String> {
        use std::sync::atomic::Ordering;
        let slots = self
            .plugins
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(plugin.to_string())
            .or_insert_with(|| PluginSlots {
                semaphore: Arc::new(tokio::sync::Semaphore::new(self.slots)),
                waiting: Default::default(),
            })
            .clone();
        let busy = || {
            format!(
                "Native plugin '{plugin}' is busy ({} calls in progress).",
                self.slots
            )
        };
        if let Ok(permit) = slots.semaphore.clone().try_acquire_owned() {
            return Ok(permit);
        }
        let queued = slots.waiting.fetch_add(1, Ordering::SeqCst);
        let _waiting = Waiting(slots.waiting.clone());
        if queued >= self.queue {
            return Err(busy());
        }
        match tokio::time::timeout(self.timeout, slots.semaphore.acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            _ => Err(busy()),
        }
    }
}

fn busy_response(message: String) -> meerschaum_server::native::NativeRouteResponse {
    let busy =
        meerschaum_plugin::api::RouteResponse::error(503, message).header("retry-after", "1");
    meerschaum_server::native::NativeRouteResponse {
        status: busy.status,
        headers: busy.headers,
        body: busy.body,
    }
}

/// The reply of an original `route` call, parsed as before `api_route` existed: an invalid or
/// missing `status` is `200`, `content_type` defaults to `text/plain; charset=utf-8` and `body`
/// is text.
fn legacy_route_response(reply: &serde_json::Value) -> meerschaum_plugin::api::RouteResponse {
    let status = reply["status"]
        .as_u64()
        .and_then(|s| u16::try_from(s).ok())
        .and_then(|s| axum::http::StatusCode::from_u16(s).ok())
        .map_or(200, |s| s.as_u16());
    let content_type = reply["content_type"]
        .as_str()
        .unwrap_or("text/plain; charset=utf-8");
    meerschaum_plugin::api::RouteResponse::bytes(
        content_type,
        reply["body"]
            .as_str()
            .unwrap_or_default()
            .as_bytes()
            .to_vec(),
    )
    .with_status(status)
}

#[async_trait]
impl meerschaum_server::native::NativeRouteHandler for RouteHandler {
    async fn admit(
        &self,
        route: &meerschaum_server::native::NativeRoute,
    ) -> Result<
        meerschaum_server::native::NativeAdmission,
        meerschaum_server::native::NativeRouteResponse,
    > {
        self.limits
            .acquire(&route.plugin)
            .await
            .map(meerschaum_server::native::NativeAdmission::new)
            .map_err(busy_response)
    }

    async fn handle(
        &self,
        route: &meerschaum_server::native::NativeRoute,
        request: meerschaum_server::native::NativeRouteRequest,
    ) -> Result<meerschaum_server::native::NativeRouteResponse, String> {
        use meerschaum_plugin::api::RouteRequest;
        let natives = self.natives.clone();
        let plugin = route.plugin.clone();
        let legacy = self.legacy.contains(&route.plugin);
        let name = route.name.clone();
        let route_path = route.path.clone();
        // The body was read under this admission (see `admit`); a request that reaches `handle`
        // without one (a direct call) takes its slot now.
        let mut request = request;
        let admission = if request.admission.is_held() {
            std::mem::take(&mut request.admission)
        } else {
            match self.limits.acquire(&route.plugin).await {
                Ok(permit) => meerschaum_server::native::NativeAdmission::new(permit),
                Err(message) => return Ok(busy_response(message)),
            }
        };
        let host = PluginHost::api(
            self.env.clone(),
            self.state.clone(),
            request.scopes.clone(),
            request.caller.take(),
        )
        .with_host_scopes(
            self.host_scopes
                .get(&(route.plugin.clone(), route.name.clone()))
                .map_or(&[], Vec::as_slice),
        );
        let reply = tokio::task::spawn_blocking(move || -> meerschaum_plugin::Result<_> {
            // The slot is held until the plugin returns, even when the client has gone.
            let _admission = admission;
            let plugin = natives.plugin(&plugin)?;
            let host = Some(&host as &dyn meerschaum_plugin::HostServices);
            if legacy {
                let context = serde_json::json!({
                    "method": request.method,
                    "path": request.path,
                    "query": request.query,
                    "body": String::from_utf8_lossy(&request.body),
                    "user": request.principal.get("username").cloned().unwrap_or_default(),
                });
                // Serialized with the plugin's other calls, as the original adapter was.
                return Ok(legacy_route_response(
                    &plugin.call_json_with_host("route", context, host)?,
                ));
            }
            let principal = serde_json::from_value(request.principal).unwrap_or_default();
            plugin.api_route_with_host(
                &RouteRequest {
                    name,
                    method: request.method,
                    path: request.path,
                    route_path,
                    path_params: request.path_params,
                    query: request.query,
                    query_params: request.query_params,
                    headers: request.headers,
                    body: request.body,
                    principal,
                    scopes: request.scopes,
                },
                host,
            )
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        Ok(meerschaum_server::native::NativeRouteResponse {
            status: reply.status,
            headers: reply.headers,
            body: reply.body,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// MCP: native tools, resources and prompts (`mcp_tools`, `mcp_resources`, `mcp_prompts`).
// ---------------------------------------------------------------------------------------------

/// The MCP entries of the native plugins, or `None` when no plugin declares any. Invalid entries
/// are reported in `notices` and skipped.
pub fn mcp(
    natives: &Arc<NativePlugins>,
    notices: &mut Vec<String>,
    env: &HostEnv,
    state: &meerschaum_server::AppState,
) -> Option<Arc<dyn meerschaum_server::mcp::NativeMcp>> {
    let mut entries = NativeMcpEntries {
        natives: natives.clone(),
        env: env.clone(),
        state: state.clone(),
        limits: CallLimits::default(),
        tools: Vec::new(),
        resources: Vec::new(),
        prompts: Vec::new(),
    };
    let builtin = meerschaum_server::mcp::registry::builtin();
    for name in natives.names() {
        match meerschaum_plugin::mcp::entries_of(natives.plugin_entry(&name)) {
            Ok(found) => {
                // An entry never shadows a built-in or an earlier plugin's entry: each one hidden
                // is a notice, like a refused route.
                let mut hidden = |kind: &str, entry: &str, by: String| {
                    notices.push(format!(
                        "Native plugin '{name}': MCP {kind} '{entry}' is hidden: {by}."
                    ))
                };
                for tool in found.tools {
                    if builtin.tools.iter().any(|b| b.name == tool.name) {
                        hidden("tool", &tool.name, "a built-in tool has that name".into());
                    } else if let Some((other, _)) =
                        entries.tools.iter().find(|(_, t)| t.name == tool.name)
                    {
                        hidden("tool", &tool.name, format!("plugin '{other}' declares it"));
                    } else {
                        entries.tools.push((name.clone(), tool));
                    }
                }
                for resource in found.resources {
                    let uri = |u: &Option<String>, t: &Option<String>| {
                        u.clone().or_else(|| t.clone()).unwrap_or_default()
                    };
                    let this = uri(&resource.uri, &resource.uri_template);
                    if builtin
                        .resources
                        .iter()
                        .any(|b| uri(&b.uri, &b.uri_template) == this)
                    {
                        hidden("resource", &this, "a built-in resource has that URI".into());
                    } else if let Some((other, _)) = entries.resources.iter().find(|(_, r)| {
                        r.name == resource.name || uri(&r.uri, &r.uri_template) == this
                    }) {
                        hidden(
                            "resource",
                            &this,
                            format!("plugin '{other}' declares that name or URI"),
                        );
                    } else {
                        entries.resources.push((name.clone(), resource));
                    }
                }
                for prompt in found.prompts {
                    if builtin.prompts.iter().any(|b| b.name == prompt.name) {
                        hidden(
                            "prompt",
                            &prompt.name,
                            "a built-in prompt has that name".into(),
                        );
                    } else if let Some((other, _)) =
                        entries.prompts.iter().find(|(_, p)| p.name == prompt.name)
                    {
                        hidden(
                            "prompt",
                            &prompt.name,
                            format!("plugin '{other}' declares it"),
                        );
                    } else {
                        entries.prompts.push((name.clone(), prompt));
                    }
                }
            }
            Err(error) => notices.push(format!(
                "Native plugin '{name}' MCP entries are disabled: {error}"
            )),
        }
    }
    if entries.tools.is_empty() && entries.resources.is_empty() && entries.prompts.is_empty() {
        return None;
    }
    Some(Arc::new(entries))
}

struct NativeMcpEntries {
    natives: Arc<NativePlugins>,
    /// The entries' host calls, made for the caller.
    env: HostEnv,
    state: meerschaum_server::AppState,
    limits: CallLimits,
    tools: Vec<(String, meerschaum_plugin::mcp::ToolSpec)>,
    resources: Vec<(String, meerschaum_plugin::mcp::ResourceSpec)>,
    prompts: Vec<(String, meerschaum_plugin::mcp::PromptSpec)>,
}

fn core_value(json: serde_json::Value) -> Value {
    serde_json::from_value(json).unwrap_or_default()
}

fn caller_context(
    caller: &meerschaum_server::mcp::Caller,
) -> (Option<meerschaum_plugin::api::Principal>, Vec<String>) {
    let principal = serde_json::from_value(meerschaum_server::native::principal_json(
        caller.principal.as_ref(),
    ))
    .ok();
    (principal, caller.scope_list())
}

impl NativeMcpEntries {
    /// Run a plugin call off the async runtime, with host calls for `caller` (full trust over
    /// stdio, the caller's scopes over HTTP).
    async fn call<T: Send + 'static>(
        &self,
        plugin: &str,
        caller: &meerschaum_server::mcp::Caller,
        f: impl FnOnce(
            &meerschaum_plugin::LoadedPlugin,
            Option<&dyn meerschaum_plugin::HostServices>,
        ) -> meerschaum_plugin::Result<T>
        + Send
        + 'static,
    ) -> Result<T, String> {
        let permit = self.limits.acquire(plugin).await?;
        let natives = self.natives.clone();
        let plugin = plugin.to_string();
        let host = if caller.api {
            PluginHost::api(
                self.env.clone(),
                self.state.clone(),
                caller.scope_list(),
                caller.principal.clone(),
            )
        } else {
            PluginHost::local(self.env.clone())
        };
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(&*natives.plugin(&plugin)?, Some(&host))
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
    }
}

#[async_trait]
impl meerschaum_server::mcp::NativeMcp for NativeMcpEntries {
    fn tools(&self) -> Vec<meerschaum_server::mcp::registry::Tool> {
        use meerschaum_server::mcp::registry::{Source, Tool};
        self.tools
            .iter()
            .filter_map(|(_, t)| {
                let entry = serde_json::json!({
                    "name": t.name,
                    "description": t.description,
                    "title": t.title,
                    "scopes": t.scopes,
                    "read_only": t.hint("readOnlyHint"),
                    "destructive": t.hint("destructiveHint"),
                    "idempotent": t.hint("idempotentHint"),
                    "open_world": t.hint("openWorldHint"),
                    "input_schema": t.input_schema,
                    "output_schema": t.output_schema,
                });
                Tool::from_value(&core_value(entry), Source::Native)
            })
            .collect()
    }

    fn resources(&self) -> Vec<meerschaum_server::mcp::registry::Resource> {
        use meerschaum_server::mcp::registry::{Resource, Source};
        self.resources
            .iter()
            .filter_map(|(_, r)| {
                let entry = core_value(serde_json::to_value(r).unwrap_or_default());
                Resource::from_value(&entry, Source::Native)
            })
            .collect()
    }

    fn prompts(&self) -> Vec<meerschaum_server::mcp::registry::Prompt> {
        use meerschaum_server::mcp::registry::{Prompt, Source};
        self.prompts
            .iter()
            .filter_map(|(_, p)| {
                let entry = core_value(serde_json::to_value(p).unwrap_or_default());
                Prompt::from_value(&entry, Source::Native)
            })
            .collect()
    }

    async fn call_tool(
        &self,
        name: &str,
        arguments: &Map,
        caller: &meerschaum_server::mcp::Caller,
    ) -> Result<Value, String> {
        let Some((plugin, spec)) = self.tools.iter().find(|(_, t)| t.name == name) else {
            return Err(format!("Unknown tool: '{name}'."));
        };
        let arguments = serde_json::to_value(arguments).map_err(|e| e.to_string())?;
        spec.validate(&arguments)
            .map_err(|e| format!("Invalid arguments for '{name}': {e}"))?;
        let (principal, scopes) = caller_context(caller);
        let call = meerschaum_plugin::mcp::ToolCall {
            name: name.to_string(),
            arguments: match arguments {
                serde_json::Value::Object(map) => map,
                _ => Default::default(),
            },
            principal,
            scopes,
        };
        self.call(plugin, caller, move |p, host| {
            p.mcp_tool_with_host(&call, host)
        })
        .await
        .map(core_value)
    }

    async fn read_resource(
        &self,
        name: &str,
        uri: &str,
        variables: &Map,
        caller: &meerschaum_server::mcp::Caller,
    ) -> Result<meerschaum_server::mcp::NativeResource, String> {
        use meerschaum_plugin::mcp::{ResourceContents, ResourceRead};
        use meerschaum_server::mcp::{NativeResource, NativeResourceContents};
        let Some((plugin, _)) = self.resources.iter().find(|(_, r)| r.name == name) else {
            return Err(format!("Resource not found: {uri}"));
        };
        let (principal, scopes) = caller_context(caller);
        let read = ResourceRead {
            name: name.to_string(),
            uri: uri.to_string(),
            variables: match serde_json::to_value(variables) {
                Ok(serde_json::Value::Object(map)) => map,
                _ => Default::default(),
            },
            principal,
            scopes,
        };
        let contents = self
            .call(plugin, caller, move |p, host| {
                p.mcp_resource_with_host(&read, host)
            })
            .await?;
        Ok(match contents {
            ResourceContents::Text { text, mime_type } => NativeResource {
                contents: NativeResourceContents::Text(text),
                mime_type,
            },
            blob @ ResourceContents::Blob { .. } => {
                let wire = blob.to_json();
                NativeResource {
                    contents: NativeResourceContents::Blob(
                        wire["blob_base64"].as_str().unwrap_or_default().to_string(),
                    ),
                    mime_type: wire["mime_type"].as_str().map(str::to_owned),
                }
            }
        })
    }

    async fn get_prompt(
        &self,
        name: &str,
        arguments: &Map,
        caller: &meerschaum_server::mcp::Caller,
    ) -> Result<(Option<String>, Vec<Value>), String> {
        let Some((plugin, _)) = self.prompts.iter().find(|(_, p)| p.name == name) else {
            return Err(format!("Unknown prompt: '{name}'."));
        };
        let (principal, scopes) = caller_context(caller);
        let get = meerschaum_plugin::mcp::PromptGet {
            name: name.to_string(),
            arguments: match serde_json::to_value(arguments) {
                Ok(serde_json::Value::Object(map)) => map,
                _ => Default::default(),
            },
            principal,
            scopes,
        };
        let (description, messages) = self
            .call(plugin, caller, move |p, host| {
                p.mcp_prompt_with_host(&get, host)
            })
            .await?;
        Ok((description, messages.into_iter().map(core_value).collect()))
    }
}

struct NativePage {
    natives: Arc<NativePlugins>,
    plugin: String,
    path: String,
    function: Option<String>,
    /// The page's host calls, made for its viewer.
    env: HostEnv,
    state: meerschaum_server::AppState,
    /// What the page's host calls may also do for every viewer (`host_scopes`).
    host_scopes: Vec<String>,
}

/// Who a console page's host calls act for, and with which scopes: everything under
/// `--no-auth`; a signed-in viewer gets what their user would get from `/login` (every scope for
/// an administrator, else the user's scopes); a guest, a user who no longer exists, or one
/// whose permissions cannot be read gets none.
async fn page_caller(
    state: &meerschaum_server::AppState,
    username: &str,
) -> (Vec<String>, Option<meerschaum_server::auth::Principal>) {
    if state.config.no_auth {
        return (vec!["*".into()], None);
    }
    if username.is_empty() {
        return (Vec::new(), None);
    }
    let conn = state.instance();
    let mut user = meerschaum_core::users::User::new(username, None);
    match conn.get_user_id(&user).await {
        Ok(Some(id)) => user.user_id = Some(id),
        _ => return (Vec::new(), None),
    }
    let Ok(user_type) = conn.get_user_type(&user).await else {
        return (Vec::new(), None);
    };
    user.user_type = user_type;
    let principal = meerschaum_server::auth::Principal::User(user);
    let Ok(scopes) = meerschaum_server::auth::principal_scopes(state, &principal).await else {
        return (Vec::new(), None);
    };
    (scopes, Some(principal))
}

#[async_trait]
impl meerschaum_web::PageHandler for NativePage {
    async fn render(
        &self,
        query: &str,
        session: &serde_json::Value,
    ) -> (String, axum::http::StatusCode) {
        let natives = self.natives.clone();
        let plugin = self.plugin.clone();
        let context = serde_json::json!({
            "path": self.path,
            "function": self.function,
            "query": query,
            "username": session["username"],
        });
        let (scopes, principal) =
            page_caller(&self.state, session["username"].as_str().unwrap_or("")).await;
        let host = PluginHost::api(self.env.clone(), self.state.clone(), scopes, principal)
            .with_host_scopes(&self.host_scopes);
        let reply = tokio::task::spawn_blocking(move || {
            natives
                .plugin(&plugin)?
                .call_json_with_host("page", context, Some(&host))
        })
        .await;
        match reply {
            Ok(Ok(reply)) => {
                let status = reply["status"]
                    .as_u64()
                    .and_then(|s| u16::try_from(s).ok())
                    .and_then(|s| axum::http::StatusCode::from_u16(s).ok())
                    .unwrap_or(axum::http::StatusCode::OK);
                (
                    reply["html"].as_str().unwrap_or_default().to_string(),
                    status,
                )
            }
            Ok(Err(error)) => (
                format!("<p>{}</p>", html_escape(&error.to_string())),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
            Err(error) => (
                format!("<p>{}</p>", html_escape(&error.to_string())),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        }
    }
}

fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// The pages native plugins declare in their manifest (`web_pages`), served as fragments for the
/// Dash console.
pub fn pages(
    manifest: &Manifest,
    natives: &Arc<NativePlugins>,
    env: &HostEnv,
    state: &meerschaum_server::AppState,
) -> Vec<meerschaum_web::PluginPage> {
    let mut pages = Vec::new();
    for plugin in manifest
        .plugins
        .values()
        .filter(|p| natives.contains(&p.name))
    {
        for page in &plugin.web_pages {
            let path = page.path.trim_matches('/');
            let path = path.strip_prefix("dash/").unwrap_or(path).to_string();
            // `host_scopes` is a native manifest key (the shared manifest model omits it).
            let declared = natives.plugin_entry(&plugin.name)["web_pages"]
                .as_array()
                .and_then(|entries| entries.iter().find(|entry| entry["path"] == page.path))
                .map(|entry| entry["host_scopes"].clone())
                .unwrap_or_default();
            let mut notices = Vec::new();
            let host_scopes = host_scopes_of(
                &declared,
                &format!("Native plugin '{}' page '{path}'", plugin.name),
                &mut notices,
            );
            for notice in notices {
                tracing_warn(&notice);
            }
            pages.push(meerschaum_web::PluginPage {
                plugin: plugin.name.clone(),
                path: path.clone(),
                page_group: page
                    .page_group
                    .clone()
                    .unwrap_or_else(|| plugin.name.clone()),
                login_required: page.login_required,
                skip_navbar: page.skip_navbar,
                dark_theme: Some(page.dark_theme),
                content: meerschaum_web::PageContent::Handler(Arc::new(NativePage {
                    natives: natives.clone(),
                    plugin: plugin.name.clone(),
                    path,
                    function: page.function.clone(),
                    env: env.clone(),
                    state: state.clone(),
                    host_scopes,
                })),
            });
        }
    }
    pages
}

#[cfg(test)]
mod legacy_route_tests {
    use super::legacy_route_response;
    use serde_json::json;

    #[test]
    fn legacy_route_replies_parse_as_before() {
        // An invalid or missing status is 200; the content type defaults to text.
        for status in [json!("201"), json!(0), json!(70000), json!(null)] {
            let r = legacy_route_response(&json!({"status": status, "body": "ok"}));
            assert_eq!(r.status, 200, "{status}");
            assert_eq!(
                r.headers,
                vec![(
                    "content-type".to_string(),
                    "text/plain; charset=utf-8".to_string()
                )]
            );
            assert_eq!(r.body, b"ok");
        }
        let r = legacy_route_response(&json!({"status": 404, "content_type": "text/html"}));
        assert_eq!(r.status, 404);
        assert_eq!(r.headers[0].1, "text/html");
        assert!(r.body.is_empty());
    }
}

#[cfg(test)]
mod call_limit_tests {
    use super::CallLimits;
    use std::time::Duration;

    #[tokio::test]
    async fn the_wait_queue_is_bounded_and_excess_calls_are_refused_at_once() {
        let limits = CallLimits::new(2, 1, Duration::from_secs(30));
        let a = limits.acquire("p").await.unwrap();
        let _b = limits.acquire("p").await.unwrap();
        // The third waits in the queue (its one place).
        let waiter = {
            let limits = limits.clone();
            tokio::spawn(async move { limits.acquire("p").await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The fourth finds the queue full: refused without waiting for the timeout.
        let started = std::time::Instant::now();
        let refused = limits.acquire("p").await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(refused.contains("busy"), "{refused}");
        // Another plugin has its own slots.
        let _other = limits.acquire("q").await.unwrap();
        // A freed slot goes to the waiter, and the queue frees its place.
        drop(a);
        let _c = waiter.await.unwrap().unwrap();
        let queued = {
            let limits = limits.clone();
            tokio::spawn(async move { limits.acquire("p").await.map(|_| ()) })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!queued.is_finished());
        queued.abort();
        let _ = queued.await;
        // A cancelled waiter leaves the queue.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let next = {
            let limits = limits.clone();
            tokio::spawn(async move { limits.acquire("p").await.map(|_| ()) })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !next.is_finished(),
            "a cancelled waiter must not keep its place"
        );
        next.abort();
    }

    #[tokio::test]
    async fn a_queued_call_times_out() {
        let limits = CallLimits::new(1, 4, Duration::from_millis(50));
        let _a = limits.acquire("p").await.unwrap();
        assert!(limits.acquire("p").await.is_err());
    }
}

#[cfg(test)]
mod host_access_tests {
    use super::*;
    use meerschaum_core::users::User;
    use meerschaum_plugin::HostServices;
    use serde_json::json;

    #[test]
    fn api_sql_never_reaches_the_internal_tables() {
        for refused in [
            "SELECT * FROM mrsm_users",
            "select password_hash from \"mrsm_users\"",
            "SELECT * FROM `MRSM_TOKENS`",
            "SELECT * FROM [dbo].[mrsm_pipes]",
            "SELECT * FROM meerschaum.mrsm_plugins",
            "SELECT * FROM mrsm_engine_runs",
            "SELECT * FROM _mrsm_internal.engine_runs",
            "SELECT query_to_xml('select 1', true, true, '')",
            "SELECT * FROM query ('SELECT 1')",
            "SELECT database_to_xml(true, true, '')",
            "SELECT * FROM U&\"mrsm\\005fusers\"",
            "SELECT * FROM sqlite_dbpage",
            "SELECT 'mrsm_' || 'users'",
            "UPDATE mrsm_users SET user_type = 'admin'",
        ] {
            assert!(refuse_internal_tables(refused).is_err(), "{refused}");
        }
        let error = refuse_internal_tables("SELECT * FROM \"MRSM_USERS\"")
            .unwrap_err()
            .to_string();
        assert!(error.contains("'MRSM_USERS'"), "{error}");
        for allowed in [
            "SELECT * FROM plugin_hosted_parent",
            "SELECT query FROM searches",
            "SELECT COUNT(*) AS n FROM \"plugin_readings\"",
            "SELECT 'mrsm' AS name",
        ] {
            assert!(refuse_internal_tables(allowed).is_ok(), "{allowed}");
        }
    }

    fn call(method: &str, args: serde_json::Value) -> meerschaum_plugin::HostCall {
        meerschaum_plugin::HostCall {
            plugin: "isolated".into(),
            method: method.into(),
            pipe: None,
            args,
        }
    }

    #[test]
    fn a_commands_host_calls_use_its_own_config_and_later_reads_see_their_writes() {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let config = meerschaum_core::config::Config::new(
            meerschaum_core::config::ConfigOptions::with_env(vec![
                ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
                (
                    "MRSM_PLUGINS_DIR".into(),
                    plugins.path().display().to_string(),
                ),
            ]),
        )
        .unwrap();
        let mut out = Vec::new();
        let mut rt = crate::dispatch::Runtime::new(
            crate::types::DateContext::system(),
            80,
            &mut out,
            |_| None,
        );
        rt.config = Some(config);
        let natives = Arc::new(NativePlugins::empty());
        let request = meerschaum_plugin::Request {
            method: "action".into(),
            connector_type: String::new(),
            label: String::new(),
            config: serde_json::Value::Null,
            context: json!({}),
        };
        let write = |rt: &mut crate::dispatch::Runtime<'_>, value: serde_json::Value| {
            with_command_host(rt, &natives, None, |host| {
                host.host_call(
                    &request,
                    &call("write_plugin_config", json!({"config": value})),
                    None,
                )
            })
            .unwrap()
            .value
        };
        // Another plugin's section, with a config reference.
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let file = config_dir.join("plugins.json");
        std::fs::write(&file, r#"{"other": {"keys": "MRSM{meerschaum:instance}"}}"#).unwrap();

        assert_eq!(
            write(&mut rt, json!({"cursor": 5})),
            json!([true, "Wrote the config of plugin 'isolated'."])
        );
        // In the runtime's root (not the process's), the other section kept as the file had it.
        let on_disk: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
        assert_eq!(
            on_disk,
            json!({"other": {"keys": "MRSM{meerschaum:instance}"}, "isolated": {"cursor": 5}})
        );
        // The runtime got its config back, and it reads the write.
        assert_eq!(
            rt.config().get_plugin_config("isolated", &["cursor"]),
            Some(Value::Int(5))
        );
        assert!(rt.config().get_plugin_config("other", &["keys"]).is_some());

        // A copy made before a later write (another run's registry, the next `--loop` run) reads
        // that write back from the file.
        let stale = host_config(rt.config());
        write(&mut rt, json!({"cursor": 6}));
        let session = crate::native::Session::new();
        let env = HostEnv::new(session.clone(), stale, natives.clone(), None);
        assert_eq!(env.plugin_config("isolated"), json!({"cursor": 6}));
        let read = PluginHost::local(env)
            .host_call(&request, &call("plugin_config", json!({})), None)
            .unwrap();
        assert_eq!(read.value, json!({"cursor": 6}));

        // A `plugins` file that cannot be read is never replaced.
        std::fs::write(&file, "{not json").unwrap();
        let refused = write(&mut rt, json!({"cursor": 7}));
        assert_eq!(refused[0], json!(false), "{refused}");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "{not json");
        session.shutdown();
        if let Some(session) = rt.native.take() {
            session.shutdown();
        }
    }

    async fn server_state(
        root: &std::path::Path,
        configure: impl FnOnce(&mut meerschaum_server::ServerConfig),
    ) -> meerschaum_server::AppState {
        let uri = format!("sqlite:///{}", root.join("instance.db").display());
        let sql = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri("local", &uri)
                .await
                .unwrap(),
        );
        if sql
            .get_user_id(&User::new("root", None))
            .await
            .unwrap()
            .is_none()
        {
            let mut admin = User::new("root", Some("synthetic-pass-1"));
            admin.user_type = Some("admin".into());
            assert!(sql.register_user(&mut admin).await.success);
            let mut ada = User::new("ada", Some("synthetic-pass-1"));
            let mut attributes = Map::new();
            attributes.insert(
                "scopes".into(),
                Value::List(vec![
                    Value::Str("pipes:read".into()),
                    Value::Str("actions:execute".into()),
                    Value::Str("jobs:execute".into()),
                ]),
            );
            ada.attributes = Some(attributes);
            assert!(sql.register_user(&mut ada).await.success);
        }
        let mut config = meerschaum_server::ServerConfig::for_root("sql:local", root);
        configure(&mut config);
        let instance: Arc<dyn InstanceConnector> = sql;
        meerschaum_server::AppState::builder(config, instance)
            .secret(b"0123456789abcdef0123456789abcdef".to_vec())
            .build()
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn api_callers_get_what_their_users_may_do() {
        let root = tempfile::tempdir().unwrap();
        let state = server_state(root.path(), |_| {}).await;
        // Page viewers: what `/login` would grant them; nothing for a guest or an unknown name.
        assert!(page_caller(&state, "").await.0.is_empty());
        assert!(page_caller(&state, "nobody").await.0.is_empty());
        let (admin_scopes, admin) = page_caller(&state, "root").await;
        assert_eq!(admin_scopes, ["*"]);
        let (ada_scopes, ada) = page_caller(&state, "ada").await;
        assert_eq!(
            ada_scopes,
            [
                "pipes:read",
                "actions:execute",
                "jobs:execute",
                "webterm:use"
            ]
        );
        assert!(ada.is_some());

        // A page uses the server's current role policy, not v4's all-scopes fallback.
        let mut viewer = User::new("viewer", Some("synthetic-pass-1"));
        assert!(state.instance().register_user(&mut viewer).await.success);
        let limited = server_state(root.path(), |config| {
            config.default_role = "viewer".into();
        })
        .await;
        let (scopes, _) = page_caller(&limited, "viewer").await;
        assert!(scopes.iter().any(|scope| scope == "pipes:read"));
        assert!(!scopes.iter().any(|scope| scope == "actions:execute"));
        let attrs = meerschaum_core::access::attributes_with_permissions::<&str>(&Map::new(), &[]);
        assert!(
            state
                .instance()
                .set_user_attributes(&viewer, &attrs)
                .await
                .success
        );
        limited.access_cache().forget_identities();
        assert!(page_caller(&limited, "viewer").await.0.is_empty());

        let config =
            meerschaum_core::config::Config::new(meerschaum_core::config::ConfigOptions::with_env(
                vec![("MRSM_ROOT_DIR".into(), root.path().display().to_string())],
            ))
            .unwrap();
        let env = HostEnv::new(
            crate::native::Session::with_runtime(tokio::runtime::Handle::current()),
            host_config(&config),
            Arc::new(NativePlugins::empty()),
            None,
        );
        let api = |state: &meerschaum_server::AppState,
                   scopes: &[String],
                   principal: &Option<meerschaum_server::auth::Principal>| {
            PluginHost::api(
                env.clone(),
                state.clone(),
                scopes.to_vec(),
                principal.clone(),
            )
        };
        // `exec_sql`: an administrator may; a user with `actions:execute` (and `jobs:execute`, as
        // `/actions` asks) only where the server lets non-administrators run actions, and never
        // under `--secure`; an anonymous caller never.
        assert!(
            api(&state, &admin_scopes, &admin)
                .may_execute()
                .await
                .is_ok()
        );
        let refused = api(&state, &ada_scopes, &ada)
            .may_execute()
            .await
            .unwrap_err()
            .to_string();
        assert!(refused.contains("not allowed users"), "{refused}");
        assert!(
            api(&state, &["*".into()], &None)
                .may_execute()
                .await
                .is_err()
        );
        let open = server_state(root.path(), |c| c.system_actions_non_admin = true).await;
        assert!(api(&open, &ada_scopes, &ada).may_execute().await.is_ok());
        let secure = server_state(root.path(), |c| {
            c.system_actions_non_admin = true;
            c.actions_non_admin = false;
        })
        .await;
        assert!(api(&secure, &ada_scopes, &ada).may_execute().await.is_err());
        let no_auth = server_state(root.path(), |c| c.no_auth = true).await;
        assert!(
            api(&no_auth, &["*".into()], &None)
                .may_execute()
                .await
                .is_ok()
        );

        // `host_scopes` add named scopes for every caller, never `*`.
        let guest = api(&state, &[], &None).with_host_scopes(&["pipes:read".into(), "*".into()]);
        assert!(guest.api_access("pipes:read", "read pipes").await.is_ok());
        assert!(guest.api_access("pipes:write", "sync pipes").await.is_err());
        assert!(
            guest
                .require_all("read the Meerschaum config")
                .await
                .is_err()
        );
        let writer = api(&state, &[], &None).with_host_scopes(&["pipes:write".into()]);
        assert!(writer.api_access("pipes:write", "sync pipes").await.is_ok());
        assert!(
            writer
                .api_access("pipes:register", "register pipes")
                .await
                .is_err()
        );
        // Without them a guest may do nothing.
        assert!(
            api(&state, &[], &None)
                .api_access("pipes:read", "read pipes")
                .await
                .is_err()
        );

        // A request's effective scope ceiling stays exact when the owner gains rights.
        let ada_user = ada.as_ref().unwrap().as_user().unwrap();
        let expanded = meerschaum_core::access::attributes_with_permissions(
            &Map::new(),
            &["pipes:read", "pipes:write", "pipes:register"],
        );
        assert!(
            state
                .instance()
                .set_user_attributes(ada_user, &expanded)
                .await
                .success
        );
        let capped = api(&state, &["pipes:write".into()], &ada);
        assert!(capped.api_access("pipes:write", "sync pipes").await.is_ok());
        assert!(
            capped
                .api_access("pipes:register", "register pipes")
                .await
                .is_err()
        );

        // Credentials are reloaded within the same plugin invocation, including narrowed
        // or revoked tokens and a user deleted after the request was authenticated.
        use meerschaum_core::users::{Expiration, Token};
        let mut token = Token::new(
            Some("host-caller"),
            Expiration::Never,
            Some(vec!["pipes:read".into(), "pipes:write".into()]),
            366,
            chrono::Utc::now(),
        );
        token.user_id = state
            .instance()
            .get_user_id(&User::new("root", None))
            .await
            .unwrap();
        token.user = Some(User::new("root", None));
        let registered = state.instance().register_token(&mut token).await;
        assert!(registered.success, "{registered}");
        let credential =
            meerschaum_server::auth::load_from_subject(&state, &token.id.unwrap().to_string())
                .await
                .unwrap();
        let token_host = api(
            &state,
            &["pipes:read".into(), "pipes:write".into()],
            &credential,
        );
        assert!(
            token_host
                .api_access("pipes:write", "sync pipes")
                .await
                .is_ok()
        );
        let grants: Vec<String> = meerschaum_core::access::ALL_PERMISSIONS
            .iter()
            .map(|p| p.to_string())
            .collect();
        assert!(
            token_host
                .clone()
                .with_host_scopes(&grants)
                .require_all("read config")
                .await
                .is_err()
        );
        token.scopes = vec!["pipes:read".into()];
        assert!(state.instance().edit_token(&token).await.success);
        assert!(
            token_host
                .api_access("pipes:write", "sync pipes")
                .await
                .is_err()
        );
        assert!(
            token_host
                .api_access("pipes:read", "read pipes")
                .await
                .is_ok()
        );
        assert!(state.instance().invalidate_token(&token).await.success);
        assert!(
            token_host
                .api_access("pipes:read", "read pipes")
                .await
                .is_err()
        );
        let read =
            meerschaum_core::access::attributes_with_permissions(&Map::new(), &["pipes:read"]);
        assert!(
            state
                .instance()
                .set_user_attributes(&viewer, &read)
                .await
                .success
        );
        let deleted = api(
            &state,
            &["pipes:read".into()],
            &Some(meerschaum_server::auth::Principal::User(viewer.clone())),
        );
        assert!(deleted.api_access("pipes:read", "read pipes").await.is_ok());
        assert!(state.instance().delete_user(&viewer).await.success);
        assert!(
            deleted
                .api_access("pipes:read", "read pipes")
                .await
                .is_err()
        );
    }
}
