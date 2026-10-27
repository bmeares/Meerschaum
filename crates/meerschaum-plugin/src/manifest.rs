//! The plugin manifest without hand-written JSON.
//!
//! [`Manifest`] declares what a plugin provides (`Plugin::manifest`); the SDK completes it
//! ([`complete`]) with the envelope (`schema_version`, `meerschaum_version`, ...) and the plugin's
//! name and version, which `export_plugin!` takes from Cargo (`CARGO_PKG_NAME`,
//! `CARGO_PKG_VERSION`) unless the plugin sets `Plugin::NAME` / `Plugin::VERSION`:
//!
//! ```
//! use meerschaum_plugin::prelude::*;
//!
//! #[derive(Default)]
//! struct Weather;
//!
//! impl Plugin for Weather {
//!     fn manifest(&self) -> Value {
//!         Manifest::new()
//!             .fetch()
//!             .action("weather", "Show the latest reading.")
//!             .post_sync_hook("report")
//!             .into()
//!     }
//! }
//! ```
//!
//! A complete manifest (the JSON document of earlier releases) is still accepted as is: the SDK
//! only fills in what is missing.
use crate::{
    api::RouteSpec,
    mcp::{PromptSpec, ResourceSpec, ToolSpec},
};
use serde_json::{Map, Value, json};

/// The key of the plugin's entry until the SDK knows the plugin's name.
const PLUGIN_KEY: &str = "$plugin";

/// What a plugin provides: its data functions, actions, flags, hooks, connector and executor
/// types, routes, MCP entries and console pages. See the [module docs](self).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Manifest {
    entry: Map<String, Value>,
    connector_types: Map<String, Value>,
    executor_types: Map<String, Value>,
}

/// A connector type a plugin provides (`<type>:<label>` connectors, Python's `@make_connector`).
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectorType {
    name: String,
    methods: Vec<String>,
    is_instance: bool,
    is_streaming: bool,
    is_thread_safe: bool,
}

impl ConnectorType {
    /// A source connector type answering `methods` (`fetch`, `register`, `sync`, ...).
    pub fn new<S: Into<String>>(name: &str, methods: impl IntoIterator<Item = S>) -> Self {
        ConnectorType {
            name: name.into(),
            methods: methods.into_iter().map(Into::into).collect(),
            is_instance: false,
            is_streaming: false,
            is_thread_safe: false,
        }
    }
    /// Usable as an instance (`-i <type>:<label>`): answer the instance methods
    /// (`register_pipe`, `sync_pipe`, `get_pipe_data`, `fetch_pipes_keys`, ...).
    pub fn instance(mut self) -> Self {
        self.is_instance = true;
        self
    }
    /// A pull-model stream source (`subscribe` among its methods, `sync pipes --listen`).
    pub fn streaming(mut self) -> Self {
        self.is_streaming = true;
        self
    }
    /// Its connectors may be called from several threads at once.
    pub fn thread_safe(mut self) -> Self {
        self.is_thread_safe = true;
        self
    }
}

impl Manifest {
    pub fn new() -> Self {
        Manifest::default()
    }

    fn push(mut self, key: &str, item: Value) -> Self {
        let list = self
            .entry
            .entry(key)
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Value::Array(items) = list
            && !(item.is_string() && items.contains(&item))
        {
            items.push(item);
        }
        self
    }

    /// `fetch` for `plugin:<name>` pipes (`Plugin::fetch`).
    pub fn fetch(self) -> Self {
        self.data_function("fetch")
    }
    /// A `sync` that replaces the built-in sync for `plugin:<name>` pipes (`Plugin::sync`).
    pub fn sync(self) -> Self {
        self.data_function("sync")
    }
    /// `register`: default parameters for new `plugin:<name>` pipes.
    pub fn register(self) -> Self {
        self.data_function("register")
    }
    /// `setup`, run by `mrsm setup plugins` and after `mrsm install plugins`.
    pub fn setup(self) -> Self {
        self.data_function("setup")
    }
    /// `subscribe`: `plugin:<name>` streams rows into `sync pipes --listen`.
    pub fn subscribe(self) -> Self {
        self.data_function("subscribe")
    }
    /// One of `fetch`, `sync`, `register`, `setup`, `subscribe`.
    pub fn data_function(self, name: &str) -> Self {
        self.push("data_functions", json!(name))
    }

    /// An action (`mrsm <name with spaces>`), answered in `Plugin::call` (`method == "action"`,
    /// `context.name`).
    pub fn action(self, name: &str, doc: &str) -> Self {
        self.push("actions", json!({"name": name, "doc": doc}))
    }

    /// A command-line flag for the plugin's actions, as argparse's `add_argument` JSON:
    /// `{"option_strings": ["--loud"], "action": "store_true", "help": "Shout."}` (also `type`,
    /// `default`, `choices`, `dest`, `required`, `metavar`, `const`).
    pub fn argument(self, argument: Value) -> Self {
        self.push("arguments", argument)
    }

    /// A `store_true` flag (`kwargs[<dest>]` is `true` when given).
    pub fn flag(self, option: &str, help: &str) -> Self {
        self.argument(json!({"option_strings": [option], "action": "store_true", "help": help}))
    }

    /// A hook run before every sync (`method == "pre_sync_hook"`, `context.name`).
    pub fn pre_sync_hook(self, name: &str) -> Self {
        self.push("pre_sync_hooks", json!(name))
    }
    /// A hook run after every sync (`method == "post_sync_hook"`, `context.name`).
    pub fn post_sync_hook(self, name: &str) -> Self {
        self.push("post_sync_hooks", json!(name))
    }

    /// A source connector type answering `methods`.
    pub fn connector_type<S: Into<String>>(
        self,
        name: &str,
        methods: impl IntoIterator<Item = S>,
    ) -> Self {
        self.connector(ConnectorType::new(name, methods))
    }

    /// A connector type with its options (instance, streaming, thread safety).
    pub fn connector(mut self, connector: ConnectorType) -> Self {
        self.connector_types.insert(
            connector.name.clone(),
            json!({
                "type": connector.name,
                "class_name": connector.name,
                "is_instance": connector.is_instance,
                "is_streaming": connector.is_streaming,
                "is_thread_safe": connector.is_thread_safe,
                "methods": connector.methods,
            }),
        );
        self.push("connectors", json!(connector.name))
    }

    /// A job executor type (`-e <type>:<label>`, see [`crate::executor`]) with the methods it
    /// implements.
    pub fn executor_type<S: Into<String>>(
        mut self,
        name: &str,
        methods: impl IntoIterator<Item = S>,
        doc: &str,
    ) -> Self {
        let methods: Vec<String> = methods.into_iter().map(Into::into).collect();
        self.executor_types.insert(
            name.into(),
            json!({"type": name, "class_name": name, "methods": methods, "doc": doc}),
        );
        self
    }

    /// An HTTP route served by `mrsm start api` (see [`crate::api`]).
    pub fn api_route(self, route: RouteSpec) -> Self {
        self.push("api_routes", route.to_json())
    }
    /// An MCP tool (see [`crate::mcp`]).
    pub fn mcp_tool(self, tool: ToolSpec) -> Self {
        self.push("mcp_tools", tool.to_json())
    }
    /// An MCP resource.
    pub fn mcp_resource(self, resource: ResourceSpec) -> Self {
        self.push(
            "mcp_resources",
            serde_json::to_value(resource).unwrap_or_default(),
        )
    }
    /// An MCP prompt.
    pub fn mcp_prompt(self, prompt: PromptSpec) -> Self {
        self.push(
            "mcp_prompts",
            serde_json::to_value(prompt).unwrap_or_default(),
        )
    }

    /// A console page at `/dash/<path>` whose HTML the `page` call returns (`context.function`
    /// is `function`). Without `login_required`, guests may open it.
    pub fn web_page(self, path: &str, function: &str, login_required: bool) -> Self {
        let path = path.trim_matches('/');
        let path = path.strip_prefix("dash/").unwrap_or(path);
        self.push(
            "web_pages",
            json!({"path": format!("/dash/{path}"), "page_key": path, "function": function,
                "login_required": login_required, "mode": "html"}),
        )
    }

    /// Give the host calls of the page declared at `path` these scopes for every viewer, on top
    /// of the viewer's own: `["pipes:read"]` lets a page without `login_required` read pipes for
    /// its guests. `*` is not accepted (the server ignores it).
    pub fn page_host_scopes<S: Into<String>>(
        mut self,
        path: &str,
        scopes: impl IntoIterator<Item = S>,
    ) -> Self {
        let path = path.trim_matches('/');
        let path = path.strip_prefix("dash/").unwrap_or(path);
        let scopes: Vec<Value> = scopes
            .into_iter()
            .map(|s| Value::String(s.into()))
            .collect();
        if let Some(Value::Array(pages)) = self.entry.get_mut("web_pages") {
            for page in pages.iter_mut().filter(|p| p["page_key"] == path) {
                page["host_scopes"] = Value::Array(scopes.clone());
            }
        }
        self
    }

    /// Python packages and `plugin:<name>` requirements (Python's `required`).
    pub fn required<S: Into<String>>(mut self, requirements: impl IntoIterator<Item = S>) -> Self {
        let items: Vec<Value> = requirements
            .into_iter()
            .map(|r| Value::String(r.into()))
            .collect();
        self.entry.insert("required".into(), Value::Array(items));
        self
    }

    /// Set any key of the plugin's manifest entry (`plugins.<name>.<key>`).
    pub fn set(mut self, key: &str, value: Value) -> Self {
        self.entry.insert(key.into(), value);
        self
    }

    /// The manifest document, completed by the SDK when the plugin is described.
    pub fn to_value(&self) -> Value {
        let mut manifest = json!({"plugins": {PLUGIN_KEY: self.entry}});
        if !self.connector_types.is_empty() {
            manifest["connector_types"] = Value::Object(self.connector_types.clone());
        }
        if !self.executor_types.is_empty() {
            manifest["executor_types"] = Value::Object(self.executor_types.clone());
        }
        manifest
    }
}

impl From<Manifest> for Value {
    fn from(manifest: Manifest) -> Value {
        manifest.to_value()
    }
}

/// Complete the manifest `Plugin::manifest` returned for the plugin `name` at `version`: fill in
/// the envelope (`schema_version`, `meerschaum_version` (this SDK's), `python_version`,
/// `fingerprint`), the plugin entry's key, `name` and `version`, the `plugin` (and other required
/// fields) of its connector and executor types, and the top-level hook lists. Values the plugin
/// set are kept.
pub fn complete(manifest: Value, name: &str, version: &str) -> Value {
    let mut manifest = match manifest {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    for (key, value) in [
        ("schema_version", json!(crate::MANIFEST_SCHEMA_VERSION)),
        ("meerschaum_version", json!(env!("CARGO_PKG_VERSION"))),
        ("python_version", json!("")),
        ("fingerprint", Value::Null),
    ] {
        manifest.entry(key).or_insert(value);
    }
    let plugins = manifest
        .entry("plugins")
        .or_insert_with(|| Value::Object(Map::new()));
    if !plugins.is_object() {
        *plugins = Value::Object(Map::new());
    }
    let plugins = plugins.as_object_mut().expect("an object");
    let mut entry = plugins
        .remove(PLUGIN_KEY)
        .or_else(|| plugins.remove(name))
        .unwrap_or_else(|| json!({}));
    if let Value::Object(entry) = &mut entry {
        entry
            .entry("name")
            .or_insert_with(|| Value::String(name.into()));
        entry
            .entry("version")
            .or_insert_with(|| Value::String(version.into()));
    }
    let hooks: Vec<(&str, bool)> = ["pre_sync_hooks", "post_sync_hooks"]
        .into_iter()
        .map(|key| {
            let declared = entry[key].as_array().is_some_and(|h| !h.is_empty());
            (key, declared)
        })
        .collect();
    plugins.insert(name.into(), entry);
    for section in ["connector_types", "executor_types"] {
        let Some(Value::Object(types)) = manifest.get_mut(section) else {
            continue;
        };
        for (type_name, desc) in types.iter_mut() {
            let Value::Object(desc) = desc else { continue };
            desc.entry("type")
                .or_insert_with(|| Value::String(type_name.clone()));
            desc.entry("plugin")
                .or_insert_with(|| Value::String(name.into()));
            desc.entry("class_name")
                .or_insert_with(|| Value::String(type_name.clone()));
            if section == "connector_types" {
                desc.entry("is_instance").or_insert(json!(false));
                desc.entry("is_thread_safe").or_insert(json!(false));
            }
        }
    }
    for (key, declared) in hooks {
        let list = manifest
            .entry(key)
            .or_insert_with(|| Value::Array(Vec::new()));
        if declared
            && let Value::Array(names) = list
            && !names.iter().any(|n| n == name)
        {
            names.push(Value::String(name.into()));
        }
    }
    Value::Object(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_built_manifest_is_completed_and_valid() {
        let built = Manifest::new()
            .fetch()
            .sync()
            .fetch()
            .action("hello", "Say hello.")
            .flag("--loud", "Shout.")
            .post_sync_hook("report")
            .connector(ConnectorType::new("memory", ["fetch", "sync_pipe"]).instance())
            .executor_type("procs", ["get_job_names"], "Local processes.")
            .api_route(RouteSpec::new("item", "GET", "/my/items/{id}"))
            .api_route(
                RouteSpec::new("status", "GET", "/my/status")
                    .public()
                    .host_scopes(["pipes:read"]),
            )
            .web_page("my/page", "page", false)
            .page_host_scopes("/dash/my/page/", ["pipes:read"])
            .required(["plugin:noaa"]);
        let manifest = complete(built.into(), "my-plugin", "1.2.3");
        crate::host::validate_manifest(&manifest).unwrap();
        let entry = &manifest["plugins"]["my-plugin"];
        assert_eq!(entry["name"], "my-plugin");
        assert_eq!(entry["version"], "1.2.3");
        assert_eq!(entry["data_functions"], json!(["fetch", "sync"]));
        assert_eq!(entry["connectors"], json!(["memory"]));
        assert_eq!(entry["web_pages"][0]["path"], "/dash/my/page");
        // A public route and a guest page whose host calls may read pipes for anyone.
        assert_eq!(entry["web_pages"][0]["host_scopes"], json!(["pipes:read"]));
        assert_eq!(entry["api_routes"][1]["host_scopes"], json!(["pipes:read"]));
        assert!(entry["api_routes"][0].get("host_scopes").is_none());
        assert_eq!(manifest["connector_types"]["memory"]["plugin"], "my-plugin");
        assert_eq!(manifest["connector_types"]["memory"]["is_instance"], true);
        assert_eq!(manifest["executor_types"]["procs"]["plugin"], "my-plugin");
        assert_eq!(manifest["post_sync_hooks"], json!(["my-plugin"]));
        assert_eq!(manifest["pre_sync_hooks"], json!([]));
        assert_eq!(manifest["meerschaum_version"], env!("CARGO_PKG_VERSION"));
        assert!(manifest["plugins"].get(PLUGIN_KEY).is_none());
    }

    #[test]
    fn complete_manifests_are_kept_and_an_empty_one_is_filled() {
        let full = json!({"schema_version": crate::MANIFEST_SCHEMA_VERSION, "meerschaum_version": "5.0.0",
            "python_version": "", "fingerprint": null,
            "plugins": {"demo": {"name": "demo", "version": "1.0.0", "actions": [{"name": "a"}]}},
            "connector_types": {}, "pre_sync_hooks": [], "post_sync_hooks": []});
        assert_eq!(complete(full.clone(), "demo", "1.0.0"), full);
        let minimal = complete(Value::Null, "demo", "2.0.0");
        crate::host::validate_manifest(&minimal).unwrap();
        assert_eq!(
            minimal["plugins"]["demo"],
            json!({"name": "demo", "version": "2.0.0"})
        );
    }
}
