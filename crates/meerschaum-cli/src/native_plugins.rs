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

/// Run a native plugin action. `action` is the dispatched action name (`words` consumed).
pub fn run_action(
    plugin: &str,
    action: &str,
    args: &ArgsMap,
    natives: &NativePlugins,
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
    Ok(plugin
        .action(action, kwargs_json(&args))
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
    /// Serves the hooks' `Pipe` calls (`get_sync_time`, `sync`).
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
    host: &NativePipeHost,
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
        let host = Arc::new(NativePipeHost {
            registry: self.registry.clone(),
            args: host_args,
        });
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
        let connector = self.0.clone().with_host_services(Arc::new(NativePipeHost {
            registry: self.1.clone(),
            args: args.clone(),
        }));
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
    /// Wrap `inner`. With no native plugins this is a pass-through.
    pub fn new(
        natives: Arc<NativePlugins>,
        config: &meerschaum_core::config::Config,
        inner: Arc<dyn ConnectorRegistry>,
    ) -> Arc<Self> {
        Arc::new(Self {
            natives,
            engine_config: meerschaum_engine::EngineConfig::from_config(config, None),
            connectors: config
                .get(&["meerschaum", "connectors"])
                .map(|v| v.to_map())
                .unwrap_or_default(),
            inner,
            hook_reporter: HookReporterSlot::default(),
            stream_kwargs: Arc::default(),
        })
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

    fn pipe_host(&self) -> Arc<NativePipeHost> {
        Arc::new(NativePipeHost {
            registry: self.clone(),
            args: SyncArgs::default(),
        })
    }

    fn source_connector(
        &self,
        keys: &str,
    ) -> Option<Result<meerschaum_plugin::NativeConnector, String>> {
        let (kind, label) = keys.split_once(':')?;
        if kind == "plugin" {
            return self.natives.source(label).map(|r| {
                r.map(|c| {
                    c.with_host_services(Arc::new(NativePipeHost {
                        registry: self.clone(),
                        args: SyncArgs::default(),
                    }))
                })
                .map_err(|e| e.to_string())
            });
        }
        let attributes = attributes(&self.connectors, kind, label);
        self.natives
            .connector(kind, label, config_json(&attributes))
            .map(|r| {
                r.map(|c| {
                    c.with_host_services(Arc::new(NativePipeHost {
                        registry: self.clone(),
                        args: SyncArgs::default(),
                    }))
                })
                .map_err(|e| e.to_string())
            })
    }
}

struct NativePipeHost {
    registry: NativeRegistry,
    args: SyncArgs,
}
impl meerschaum_plugin::HostServices for NativePipeHost {
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
        use meerschaum_plugin::Error;
        let fail = |e: String| Error::Message(e);
        let metadata = &request.context["pipe"];
        let mut state = metadata.get("keys").unwrap_or(metadata).clone();
        state["parameters"] = metadata
            .get("parameters")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let state: Map = serde_json::from_value(state).map_err(|e| fail(e.to_string()))?;
        let mut pipe = Pipe::from_state(&state, &self.registry.engine_config.defaults)
            .map_err(|e| fail(e.to_string()))?;
        pipe.temporary = metadata["temporary"].as_bool().unwrap_or(false);
        let runtime = tokio::runtime::Handle::try_current().map_err(|e| fail(e.to_string()))?;
        // Native fetch and sync execute on blocking threads. Their runtime stays free to
        // service SQL/network operations while this synchronous callback waits.
        runtime.block_on(async {
            match method {
                "get_sync_time" => {
                    let instance = self.registry.instance(pipe.instance_keys()).map_err(fail)?;
                    let bound = instance
                        .get_sync_time(&pipe, None, true)
                        .await
                        .map_err(|e| fail(e.to_string()))?;
                    Ok(bound
                        .map(|b| match b {
                            meerschaum_connector::DateBound::Int(i) => serde_json::json!(i),
                            other => serde_json::json!(other.isoformat()),
                        })
                        .unwrap_or(serde_json::Value::Null))
                }
                "sync" => {
                    let Some(data) = data else {
                        return Ok(serde_json::json!([true, "No data to sync."]));
                    };
                    let engine = meerschaum_engine::Engine::new(
                        Arc::new(self.registry.clone()),
                        self.registry.engine_config.clone(),
                    );
                    // Supplying input bypasses plugin_sync and fetch; no recursive plugin call.
                    let result = engine.sync_pipe(&pipe, Some(data), &self.args).await;
                    Ok(serde_json::json!([result.success, result.message]))
                }
                _ => Err(Error::Unsupported(format!(
                    "unsupported Pipe host operation: {method}"
                ))),
            }
        })
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
                        .with_host_services(Arc::new(NativePipeHost {
                            registry: self.clone(),
                            args: args.clone(),
                        })),
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
) {
    use meerschaum_server::native::NativeRoute;
    let mut routes = Vec::new();
    let mut legacy = std::collections::HashSet::new();
    for name in natives.names() {
        let entry = natives.plugin_entry(&name);
        match meerschaum_plugin::api::routes_of(entry) {
            Ok(specs) if !specs.is_empty() => {
                routes.extend(specs.into_iter().map(|spec| NativeRoute {
                    plugin: name.clone(),
                    name: spec.name,
                    method: spec.method,
                    path: spec.path,
                    scopes: spec.scopes,
                    public: spec.public,
                }));
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
    (routes, legacy)
}

/// The router serving the routes native plugins declare (see `declared_routes` and
/// `meerschaum_server::native`). Refused routes (a core route they would shadow, a conflict,
/// bad syntax) are skipped; each is one notice.
pub fn routes(
    state: &meerschaum_server::AppState,
    natives: &Arc<NativePlugins>,
) -> (axum::Router, Vec<String>) {
    let mut notices = Vec::new();
    let (routes, legacy) = declared_routes(natives, &mut notices);
    if routes.is_empty() {
        return (axum::Router::new(), notices);
    }
    let handler = Arc::new(RouteHandler {
        natives: natives.clone(),
        legacy,
        limits: CallLimits::default(),
    });
    let (router, refused) = meerschaum_server::native::native_router(state, routes, handler);
    notices.extend(refused);
    (router, notices)
}

struct RouteHandler {
    natives: Arc<NativePlugins>,
    /// Plugins served through the original `route` call kind.
    legacy: std::collections::HashSet<String>,
    limits: CallLimits,
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
        let reply = tokio::task::spawn_blocking(move || -> meerschaum_plugin::Result<_> {
            // The slot is held until the plugin returns, even when the client has gone.
            let _admission = admission;
            let plugin = natives.plugin(&plugin)?;
            if legacy {
                let context = serde_json::json!({
                    "method": request.method,
                    "path": request.path,
                    "query": request.query,
                    "body": String::from_utf8_lossy(&request.body),
                    "user": request.principal.get("username").cloned().unwrap_or_default(),
                });
                // Serialized with the plugin's other calls, as the original adapter was.
                return Ok(legacy_route_response(&plugin.call_json("route", context)?));
            }
            let principal = serde_json::from_value(request.principal).unwrap_or_default();
            plugin.api_route(&RouteRequest {
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
            })
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
) -> Option<Arc<dyn meerschaum_server::mcp::NativeMcp>> {
    let mut entries = NativeMcpEntries {
        natives: natives.clone(),
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
    /// Run a plugin call off the async runtime.
    async fn call<T: Send + 'static>(
        &self,
        plugin: &str,
        f: impl FnOnce(&meerschaum_plugin::LoadedPlugin) -> meerschaum_plugin::Result<T>
        + Send
        + 'static,
    ) -> Result<T, String> {
        let permit = self.limits.acquire(plugin).await?;
        let natives = self.natives.clone();
        let plugin = plugin.to_string();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(&*natives.plugin(&plugin)?)
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
        self.call(plugin, move |p| p.mcp_tool(&call))
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
        let contents = self.call(plugin, move |p| p.mcp_resource(&read)).await?;
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
        let (description, messages) = self.call(plugin, move |p| p.mcp_prompt(&get)).await?;
        Ok((description, messages.into_iter().map(core_value).collect()))
    }
}

struct NativePage {
    natives: Arc<NativePlugins>,
    plugin: String,
    path: String,
    function: Option<String>,
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
        let reply = tokio::task::spawn_blocking(move || {
            natives.plugin(&plugin)?.call_json("page", context)
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
pub fn pages(manifest: &Manifest, natives: &Arc<NativePlugins>) -> Vec<meerschaum_web::PluginPage> {
    let mut pages = Vec::new();
    for plugin in manifest
        .plugins
        .values()
        .filter(|p| natives.contains(&p.name))
    {
        for page in &plugin.web_pages {
            let path = page.path.trim_matches('/');
            let path = path.strip_prefix("dash/").unwrap_or(path).to_string();
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
