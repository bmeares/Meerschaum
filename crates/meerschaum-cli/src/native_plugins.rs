//! Native (C-ABI) plugins in the CLI: the manifest merge, action dispatch, plugin flags, and
//! the engine registry that resolves native connectors, `sync()` functions and sync hooks.
//!
//! Native plugins are discovered in the same `MRSM_PLUGINS_DIR` directories as Python plugins
//! (a directory with a `plugin.toml` and `lib<name>.so`). A native plugin shadows a Python
//! plugin of the same name. See `crates/meerschaum-plugin/README.md`.

use std::sync::Arc;

use meerschaum_connector::{Connector, InstanceConnector, async_trait};
use meerschaum_core::success::SyncResult;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_engine::{
    ConnectorRegistry, PluginSync, SyncArgs, SyncBegin, SyncHooks,
    stream::{LegacyStream, StreamSource},
    sync::{DeltaObserver, SyncEnv},
};
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
            for argument in plugin["arguments"].as_array_mut().into_iter().flatten() {
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
    merged
}

/// The native plugin and action name that handle `words`, if any.
pub fn action_owner(
    words: &[String],
    manifest: &Manifest,
    native: &NativePlugins,
) -> Option<(String, String)> {
    let mut best: Option<(String, String)> = None;
    for plugin in manifest
        .plugins
        .values()
        .filter(|p| native.contains(&p.name))
    {
        for action in &plugin.actions {
            let parts: Vec<_> = action.name.split('_').collect();
            let matches = words.len() >= parts.len()
                && words.iter().zip(&parts).all(|(word, part)| word == part);
            if matches
                && best
                    .as_ref()
                    .is_none_or(|(_, current)| current.len() < action.name.len())
            {
                best = Some((plugin.name.clone(), action.name.clone()));
            }
        }
    }
    best
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
    let consumed = action.split('_').count();
    let words = native::words(&args, "action");
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

/// Populate the flags native plugins declared (`add_plugin_argument` equivalents) from the
/// raw sysargs. Supports the common argparse actions and a single value (`nargs` unset).
pub fn arguments(args: &mut ArgsMap, plugins: &[&PluginManifest], sysargs: &[String]) {
    for argument in plugins.iter().flat_map(|p| &p.arguments) {
        let Some(dest) = destination(argument) else {
            continue;
        };
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
                Some(None) => match normalize_action(&argument.action).as_str() {
                    "store_true" | "store_false" | "store_const" | "count" | "append_const" => {
                        found.push(None)
                    }
                    _ => {
                        if let Some(value) = sysargs.get(index + 1) {
                            found.push(Some(value.clone()));
                            index += 1;
                        }
                    }
                },
                None => {}
            }
            index += 1;
        }
        let convert = |text: &str| -> PyValue {
            match argument.type_name.as_deref() {
                Some("int") => text
                    .parse::<i128>()
                    .map(PyValue::Int)
                    .unwrap_or_else(|_| PyValue::Str(text.into())),
                Some("float") => text
                    .parse::<f64>()
                    .map(PyValue::Float)
                    .unwrap_or_else(|_| PyValue::Str(text.into())),
                _ => PyValue::Str(text.into()),
            }
        };
        let action = normalize_action(&argument.action);
        let value = match action.as_str() {
            "store_true" => Some(PyValue::Bool(!found.is_empty())),
            "store_false" => Some(PyValue::Bool(found.is_empty())),
            "count" => Some(PyValue::Int(found.len() as i128)),
            "store_const" if !found.is_empty() => Some(json_to_py(&argument.const_value)),
            "append" => (!found.is_empty())
                .then(|| PyValue::List(found.iter().flatten().map(|v| convert(v)).collect())),
            "store" | "" => found.last().and_then(|v| v.as_deref()).map(convert),
            _ => None,
        };
        match value {
            Some(value) => {
                args.insert(dest, value);
            }
            None if !args.contains_key(&dest) => {
                args.insert(dest, json_to_py(&argument.default));
            }
            None => {}
        }
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
    map.insert(
        "parameters".into(),
        Value::Map(pipe.raw_parameters().clone()),
    );
    Value::Map(map)
}

/// The keyword arguments v4 passes to `sync()` and the sync hooks.
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

struct NativeHooks {
    natives: Arc<NativePlugins>,
    inner: Option<Arc<dyn SyncHooks>>,
}

impl NativeHooks {
    async fn run(&self, post: bool, kwargs: serde_json::Value) {
        for (name, hooks) in self.natives.hooks(post) {
            let natives = self.natives.clone();
            let kwargs = kwargs.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                let plugin = natives.plugin(&name).map_err(|e| e.to_string())?;
                let mut failures = Vec::new();
                for hook in hooks {
                    match plugin.hook(post, &hook, kwargs.clone()) {
                        Ok(tuple) if !tuple.success => {
                            failures.push(format!("{hook}: {}", tuple.message))
                        }
                        Ok(_) => {}
                        Err(e) => failures.push(format!("{hook}: {e}")),
                    }
                }
                Ok::<_, String>((name, failures))
            })
            .await;
            // PARITY: hook failures are warnings, never fatal.
            match outcome {
                Ok(Ok((name, failures))) => {
                    for failure in failures {
                        tracing_warn(&format!("Native plugin '{name}' hook failed: {failure}"));
                    }
                }
                Ok(Err(error)) => tracing_warn(&format!("Native plugin hook failed: {error}")),
                Err(error) => tracing_warn(&format!("Native plugin hook failed: {error}")),
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
        self.run(false, sync_kwargs(pipe, args)).await;
        if let Some(inner) = &self.inner {
            inner.pre_sync(pipe, args).await;
        }
    }

    async fn post_sync(&self, pipe: &Pipe, args: &SyncArgs, result: &SyncResult) {
        let mut kwargs = sync_kwargs(pipe, args);
        kwargs["success_tuple"] = serde_json::json!([result.success, result.message]);
        kwargs["sync_duration"] =
            serde_json::json!(result.duration.map(|d| d.as_secs_f64()).unwrap_or_default());
        self.run(true, kwargs).await;
        if let Some(inner) = &self.inner {
            inner.post_sync(pipe, args, result).await;
        }
    }
}

/// A native plugin's own `sync()`.
struct NativeSync(meerschaum_plugin::NativeConnector);

#[async_trait]
impl PluginSync for NativeSync {
    async fn sync(&self, pipe: &Pipe, args: &SyncArgs) -> SyncResult {
        let connector = self.0.clone();
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
pub struct NativeRegistry {
    natives: Arc<NativePlugins>,
    /// `meerschaum:connectors` (connector attributes by type and label).
    connectors: Map,
    inner: Arc<dyn ConnectorRegistry>,
}

/// `meerschaum:connectors:<type>:default` patched with the label's section.
fn attributes(connectors: &Map, kind: &str, label: &str) -> Map {
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
            connectors: config
                .get(&["meerschaum", "connectors"])
                .map(|v| v.to_map())
                .unwrap_or_default(),
            inner,
        })
    }

    fn source_connector(
        &self,
        keys: &str,
    ) -> Option<Result<meerschaum_plugin::NativeConnector, String>> {
        let (kind, label) = keys.split_once(':')?;
        if kind == "plugin" {
            return self
                .natives
                .source(label)
                .map(|r| r.map_err(|e| e.to_string()));
        }
        let attributes = attributes(&self.connectors, kind, label);
        self.natives
            .connector(kind, label, config_json(&attributes))
            .map(|r| r.map_err(|e| e.to_string()))
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
                Ok(Some(Arc::new(result?.with_fetch_kwargs(&kwargs))))
            }
            None => self.inner.source_for_sync(keys, args),
        }
    }

    fn plugin_sync(&self, keys: &str) -> Option<Arc<dyn PluginSync>> {
        if let Some(("plugin", name)) = keys.split_once(':')
            && self.natives.has_data_function(name, "sync")
        {
            let connector = self.natives.source(name).and_then(Result::ok)?;
            return Some(Arc::new(NativeSync(connector)));
        }
        self.inner.plugin_sync(keys)
    }

    fn stream_source(&self, keys: &str) -> Option<Arc<dyn StreamSource>> {
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
        }))
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        self.inner.table_reference(pipe)
    }
}

// ---------------------------------------------------------------------------------------------
// Server extensions: native routes (`@api_plugin` equivalent) and console pages (`@web_page`).
// ---------------------------------------------------------------------------------------------

/// Paths a native route may not take: the console, the host's internals and the core API.
fn route_path_allowed(path: &str) -> bool {
    path.starts_with('/')
        && !path.starts_with("//")
        && !path.contains(['{', '}', '*', '?', '#', '%', '\\'])
        && !path.split('/').any(|s| s == ".." || s == ".")
        && !path.starts_with("/dash")
        && !path.starts_with("/webterm")
        && meerschaum_server::routes::route_group_of(path) == "other"
}

/// The router serving the routes native plugins export (`routes` / `route` methods). A route
/// requires the same credentials as the core API unless the plugin marks it `"public": true`
/// (or the server runs with `--no-auth`). Colliding or reserved paths are skipped and reported.
pub fn routes(
    state: &meerschaum_server::AppState,
    natives: &Arc<NativePlugins>,
) -> (axum::Router, Vec<String>) {
    use axum::http::Method;
    let mut router = axum::Router::new();
    let mut notices = Vec::new();
    let mut taken = std::collections::HashSet::new();
    for name in natives.names() {
        if !natives.plugin_entry(&name)["api_plugins"]
            .as_array()
            .is_some_and(|a| !a.is_empty())
        {
            continue;
        }
        let plugin = match natives.plugin(&name) {
            Ok(plugin) => plugin,
            Err(error) => {
                notices.push(format!(
                    "Native plugin '{name}' routes are disabled: {error}"
                ));
                continue;
            }
        };
        let listed = match plugin.call_json("routes", serde_json::json!({})) {
            Ok(listed) => listed,
            Err(error) => {
                notices.push(format!(
                    "Native plugin '{name}' routes are disabled: {error}"
                ));
                continue;
            }
        };
        for entry in listed.as_array().cloned().unwrap_or_default() {
            let path = entry["path"].as_str().unwrap_or_default().to_string();
            let method = entry["method"]
                .as_str()
                .unwrap_or("GET")
                .to_ascii_uppercase();
            let Ok(verb) = method.parse::<Method>() else {
                notices.push(format!(
                    "Native plugin '{name}': invalid method '{method}'."
                ));
                continue;
            };
            if !route_path_allowed(&path) || !taken.insert((method.clone(), path.clone())) {
                notices.push(format!(
                    "Native plugin '{name}': route {method} {path} is not allowed."
                ));
                continue;
            }
            let public = entry["public"].as_bool().unwrap_or(false);
            let handler = {
                let plugin = plugin.clone();
                let state = state.clone();
                move |req: axum::extract::Request| {
                    let plugin = plugin.clone();
                    let state = state.clone();
                    async move { serve_route(plugin, state, public, req).await }
                }
            };
            let filter = match verb {
                Method::GET => axum::routing::get(handler),
                Method::POST => axum::routing::post(handler),
                Method::PUT => axum::routing::put(handler),
                Method::PATCH => axum::routing::patch(handler),
                Method::DELETE => axum::routing::delete(handler),
                _ => {
                    notices.push(format!(
                        "Native plugin '{name}': unsupported method '{method}'."
                    ));
                    continue;
                }
            };
            router = router.route(&path, filter);
        }
    }
    (router, notices)
}

async fn serve_route(
    plugin: Arc<meerschaum_plugin::LoadedPlugin>,
    state: meerschaum_server::AppState,
    public: bool,
    req: axum::extract::Request,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let (parts, body) = req.into_parts();
    let mut user = serde_json::Value::Null;
    if !public {
        match meerschaum_server::auth::manager_unless_no_auth(&state, &parts.headers).await {
            Ok(Some(principal)) => {
                user = serde_json::json!(principal.username());
            }
            Ok(None) => {}
            Err(error) => return error.into_response(),
        }
    }
    let Ok(bytes) = axum::body::to_bytes(body, 16 * 1024 * 1024).await else {
        return axum::http::StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    let context = serde_json::json!({
        "method": parts.method.as_str(),
        "path": parts.uri.path(),
        "query": parts.uri.query().unwrap_or(""),
        "body": String::from_utf8_lossy(&bytes),
        "user": user,
    });
    let reply = tokio::task::spawn_blocking(move || plugin.call_json("route", context)).await;
    match reply {
        Ok(Ok(reply)) => {
            let status = reply["status"]
                .as_u64()
                .and_then(|s| u16::try_from(s).ok())
                .and_then(|s| axum::http::StatusCode::from_u16(s).ok())
                .unwrap_or(axum::http::StatusCode::OK);
            let content_type = reply["content_type"]
                .as_str()
                .unwrap_or("text/plain; charset=utf-8");
            let body = reply["body"].as_str().unwrap_or_default().to_string();
            (
                status,
                [(axum::http::header::CONTENT_TYPE, content_type.to_string())],
                body,
            )
                .into_response()
        }
        Ok(Err(error)) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )
            .into_response(),
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )
            .into_response(),
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

/// The console pages native plugins declare in their manifest (`web_pages`).
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
