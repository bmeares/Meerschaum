//! Plugin manifests (§4.3 "Plugin action manifests").
//!
//! The manifest lists, per installed plugin, its `@make_action` actions (with docstrings and
//! subactions), `add_plugin_argument` arguments, sync hooks, `@api_plugin` / `@dash_plugin` /
//! `@web_page` registrations, MCP tools / resources / prompts, custom connector types and data
//! functions. A worker builds it (`build_manifest`); the host caches it at
//! [`manifest_path`] keyed by a [`fingerprint`] of every plugin source tree (the relative paths,
//! sizes and modification times of its `.py` files), so `--help`, completion and argument
//! validation never start Python unless a plugin changed.
//!
//! **Staleness is idempotent.** A manifest is stale when the plugins *it tracks* changed. The
//! tracked set is every plugin Rust discovers in the plugins directories ([`discover_plugins`])
//! plus every plugin the manifest lists that discovery did not find but that reported a `path`
//! (Python also finds pip-installed plugins exposed through the `meerschaum.plugins`
//! entry-point group). The set is derived from the manifest itself, so right after
//! [`build_manifest`] on an unchanged tree [`stale_plugins`] is `(false, [])` whatever Python
//! reported. A manifest-only plugin without a `path` cannot be fingerprinted and is never
//! stale; a discovered plugin Python omitted is covered by the combined fingerprint. Only `.py`
//! files are hashed, so a plugin writing data files into its own tree at runtime does not
//! invalidate the cache, and the fingerprint is taken after the worker imported the plugins,
//! so a plugin that writes into its tree on import does not either.
//!
//! **Entry-point plugins** reach the manifest through [`crate::entry_points`]: the probe's answer
//! is recorded in [`Manifest::entry_point_plugins`], a manifest built with another set is stale
//! ([`stale_plugins_for`]), and an empty plugin tree still starts Python when an entry-point
//! plugin exists (v4 discovers them whatever the plugins directory holds).
//!
//! The JSON schema is `schema/manifest.schema.json` in this crate. The top-level
//! `api_plugins` / `dash_plugins` / `web_pages` / `mcp_tools` lists are the plugin names the
//! server's `HostManifest` reads to decide whether the API host is needed. The host is also
//! needed for the Dash web console (the `console` reason, [`HostReason::Console`]): the Rust
//! server hosts no console of its own, so whenever the `dash` extra is importable the host runs
//! and mounts it, with or without plugins.

use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{WorkerError, WorkerResult};
use crate::pool::WorkerPool;
use crate::protocol::{Msg, wire_to_json};
use crate::worker::CallOptions;

/// The manifest schema version (`meerschaum/worker/_manifest.py`). 4: custom connector
/// `methods` include methods imported into the class body, so cached version-3 manifests (which
/// missed them) are rebuilt. 5: a plugin's `data_functions` list every callable attribute (a
/// `functools.partial` or callable object `register` too), not only plain functions.
pub const MANIFEST_SCHEMA_VERSION: u32 = 5;

/// The manifest file name under the cache directory.
pub const MANIFEST_FILE_NAME: &str = "plugins_manifest.json";

/// The JSON schema of the manifest.
pub const MANIFEST_JSON_SCHEMA: &str = include_str!("../schema/manifest.schema.json");

/// A plugin action.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ActionManifest {
    /// The action name (`foo_bar` runs as `mrsm foo bar`).
    pub name: String,
    /// The docstring (`inspect.getdoc`).
    pub doc: Option<String>,
    /// `make_action(daemon=...)`: whether `-d` may detach it.
    #[serde(default = "default_true")]
    pub daemon: bool,
    /// Whether the plugin defines a custom action completer.
    #[serde(default)]
    pub has_completer: bool,
    /// Subactions defined in the plugin module (`<action>_<sub>`).
    #[serde(default)]
    pub subactions: Vec<SubactionManifest>,
}

fn default_true() -> bool {
    true
}

/// A subaction.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubactionManifest {
    /// The subaction name.
    pub name: String,
    /// The docstring.
    pub doc: Option<String>,
}

/// An `add_plugin_argument` argument (an `argparse.Action`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ArgumentManifest {
    /// `--foo`, `-f`.
    pub option_strings: Vec<String>,
    /// The namespace attribute.
    pub dest: Option<String>,
    /// The action class (`_StoreAction`, `_StoreTrueAction`, ...).
    pub action: String,
    /// `nargs`.
    #[serde(default)]
    pub nargs: serde_json::Value,
    /// `const`.
    #[serde(rename = "const", default)]
    pub const_value: serde_json::Value,
    /// `default`.
    #[serde(default)]
    pub default: serde_json::Value,
    /// `type.__name__`.
    #[serde(rename = "type")]
    pub type_name: Option<String>,
    /// `choices`.
    #[serde(default)]
    pub choices: serde_json::Value,
    /// `required`.
    #[serde(default)]
    pub required: bool,
    /// `help`.
    pub help: Option<String>,
    /// `metavar`.
    #[serde(default)]
    pub metavar: serde_json::Value,
}

/// A `@web_page` registration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WebPageManifest {
    /// The path (`/dash/foo/bar`).
    pub path: String,
    /// The navbar group.
    pub page_group: Option<String>,
    /// The page title key.
    pub page_key: Option<String>,
    /// `login_required`.
    #[serde(default = "default_true")]
    pub login_required: bool,
    /// `skip_navbar`.
    #[serde(default)]
    pub skip_navbar: bool,
    /// `dark_theme`.
    #[serde(default = "default_true")]
    pub dark_theme: bool,
    /// The layout function's name.
    pub function: Option<String>,
    /// `html` (page contract) or `dash` (legacy Dash layout).
    #[serde(default = "default_web_page_mode")]
    pub mode: String,
    /// The host route serving the page (`/_pages/<path>` or `/dash/<plugin>/<path>`).
    #[serde(default)]
    pub route: Option<String>,
}

fn default_web_page_mode() -> String {
    "dash".to_string()
}

/// An MCP tool registered with `meerschaum.mcp.tool(...)`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct McpToolManifest {
    /// The tool name.
    pub name: String,
    /// The description.
    pub description: String,
    /// The JSON schema of its input.
    pub input_schema: serde_json::Value,
    /// The JSON schema of its output.
    #[serde(default)]
    pub output_schema: serde_json::Value,
    /// The required scopes.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// The title.
    pub title: Option<String>,
    /// The MCP annotations.
    #[serde(default)]
    pub annotations: serde_json::Value,
}

/// An MCP resource (or resource template).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct McpResourceManifest {
    /// The resource name.
    pub name: String,
    /// The description.
    pub description: String,
    /// A static URI.
    pub uri: Option<String>,
    /// An RFC 6570 URI template.
    pub uri_template: Option<String>,
    /// The MIME type.
    pub mime_type: Option<String>,
    /// The required scopes.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// The title.
    pub title: Option<String>,
}

/// An MCP prompt.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct McpPromptManifest {
    /// The prompt name.
    pub name: String,
    /// The description.
    pub description: Option<String>,
}

/// A custom connector type declared with `@make_connector`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConnectorTypeManifest {
    /// The type (`foo` for `FooConnector`).
    #[serde(rename = "type")]
    pub connector_type: String,
    /// The plugin declaring it.
    pub plugin: String,
    /// The class name.
    pub class_name: String,
    /// `IS_INSTANCE`.
    pub is_instance: bool,
    /// `IS_THREAD_SAFE`.
    pub is_thread_safe: bool,
    /// `IS_STREAMING`.
    #[serde(default)]
    pub is_streaming: bool,
    /// `REQUIRED_ATTRIBUTES`.
    #[serde(default)]
    pub required_attributes: Vec<String>,
    /// `OPTIONAL_ATTRIBUTES`.
    #[serde(default)]
    pub optional_attributes: Vec<String>,
    /// Methods defined on the class itself.
    #[serde(default)]
    pub methods: Vec<String>,
    /// The class docstring.
    pub doc: Option<String>,
}

/// One plugin's entry.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PluginManifest {
    /// The plugin name.
    pub name: String,
    /// `__version__`.
    pub version: Option<String>,
    /// The module file.
    pub path: Option<String>,
    /// The `required` list.
    #[serde(default)]
    pub required: Vec<String>,
    /// Which of `fetch`, `sync`, `register`, `setup`, `subscribe` the module defines.
    #[serde(default)]
    pub data_functions: Vec<String>,
    /// Actions.
    #[serde(default)]
    pub actions: Vec<ActionManifest>,
    /// `add_plugin_argument` arguments.
    #[serde(default)]
    pub arguments: Vec<ArgumentManifest>,
    /// `@pre_sync_hook` function names.
    #[serde(default)]
    pub pre_sync_hooks: Vec<String>,
    /// `@post_sync_hook` function names.
    #[serde(default)]
    pub post_sync_hooks: Vec<String>,
    /// `@api_plugin` function names.
    #[serde(default)]
    pub api_plugins: Vec<String>,
    /// `@dash_plugin` function names.
    #[serde(default)]
    pub dash_plugins: Vec<String>,
    /// `@web_page` registrations.
    #[serde(default)]
    pub web_pages: Vec<WebPageManifest>,
    /// MCP tools.
    #[serde(default)]
    pub mcp_tools: Vec<McpToolManifest>,
    /// MCP resources.
    #[serde(default)]
    pub mcp_resources: Vec<McpResourceManifest>,
    /// MCP prompts.
    #[serde(default)]
    pub mcp_prompts: Vec<McpPromptManifest>,
    /// Custom connector types.
    #[serde(default)]
    pub connectors: Vec<String>,
    /// The import error, if the plugin failed to import.
    pub import_error: Option<String>,
    /// Static `attempt_import` findings: default-venv imports of undeclared packages (D20).
    #[serde(default)]
    pub venv_warnings: Vec<String>,
    /// The fingerprint of this plugin's source tree.
    #[serde(default)]
    pub fingerprint: Option<String>,
}

/// Why the API host is needed ([`Manifest::api_host_reasons`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostReason {
    /// A plugin registers `@api_plugin` functions.
    ApiPlugins,
    /// A plugin registers `@dash_plugin` functions.
    DashPlugins,
    /// A plugin registers `@web_page` layouts.
    WebPages,
    /// A plugin registers MCP tools, resources or prompts.
    McpTools,
    /// The Dash web console is served.
    Console,
}

impl HostReason {
    /// The manifest key (or reason name) of this reason.
    pub fn name(self) -> &'static str {
        match self {
            HostReason::ApiPlugins => "api_plugins",
            HostReason::DashPlugins => "dash_plugins",
            HostReason::WebPages => "web_pages",
            HostReason::McpTools => "mcp_tools",
            HostReason::Console => "console",
        }
    }
}

/// The plugin manifest.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// [`MANIFEST_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// The `meerschaum` version that built it.
    pub meerschaum_version: String,
    /// The Python version that built it.
    pub python_version: String,
    /// The interpreter that built it.
    #[serde(default)]
    pub python_executable: Option<String>,
    /// The fingerprint of every plugin source tree ([`fingerprint`]).
    pub fingerprint: Option<String>,
    /// Per-plugin entries, in plugin order.
    pub plugins: IndexMap<String, PluginManifest>,
    /// Custom connector types by type.
    #[serde(default)]
    pub connector_types: IndexMap<String, ConnectorTypeManifest>,
    /// Plugins with `@api_plugin` functions (read by the server's `HostManifest`).
    #[serde(default)]
    pub api_plugins: Vec<String>,
    /// Plugins with `@dash_plugin` functions.
    #[serde(default)]
    pub dash_plugins: Vec<String>,
    /// Plugins with `@web_page` layouts.
    #[serde(default)]
    pub web_pages: Vec<String>,
    /// Plugins registering MCP tools, resources or prompts.
    #[serde(default)]
    pub mcp_tools: Vec<String>,
    /// The Dash web console is served (the `dash` extra imports): the API host runs for it even
    /// when no plugin needs it. Written by the server's start-up probe, never by a worker.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub console: bool,
    /// Plugins with `@pre_sync_hook` functions.
    #[serde(default)]
    pub pre_sync_hooks: Vec<String>,
    /// Plugins with `@post_sync_hook` functions.
    #[serde(default)]
    pub post_sync_hooks: Vec<String>,
    /// Errors raised while importing all plugins at once.
    #[serde(default)]
    pub load_errors: IndexMap<String, String>,
    /// The entry-point plugins (`meerschaum.plugins` group) the interpreter had when this was
    /// built ([`crate::entry_points`]). A different set makes the manifest stale, whatever the
    /// plugin trees hold.
    #[serde(default)]
    pub entry_point_plugins: Vec<String>,
}

impl Manifest {
    /// Whether any plugin registers a sync hook (if not, no worker starts for hooks, §4.2).
    pub fn has_sync_hooks(&self) -> bool {
        !self.pre_sync_hooks.is_empty() || !self.post_sync_hooks.is_empty()
    }

    /// Whether the API host is needed (§7.3): by a plugin or by the Dash console.
    pub fn needs_api_host(&self) -> bool {
        !self.api_host_reasons().is_empty()
    }

    /// Why the API host is needed, in a stable order (empty: it never starts).
    pub fn api_host_reasons(&self) -> Vec<HostReason> {
        let mut reasons = Vec::new();
        for (reason, present) in [
            (HostReason::ApiPlugins, !self.api_plugins.is_empty()),
            (HostReason::DashPlugins, !self.dash_plugins.is_empty()),
            (HostReason::WebPages, !self.web_pages.is_empty()),
            (HostReason::McpTools, !self.mcp_tools.is_empty()),
            (HostReason::Console, self.console),
        ] {
            if present {
                reasons.push(reason);
            }
        }
        reasons
    }

    /// The plugin defining `action` (exact name), if any.
    pub fn action_plugin(&self, action: &str) -> Option<&PluginManifest> {
        self.plugins
            .values()
            .find(|p| p.actions.iter().any(|a| a.name == action))
    }

    /// The plugin declaring the connector `connector_type`.
    pub fn connector_type(&self, connector_type: &str) -> Option<&ConnectorTypeManifest> {
        self.connector_types.get(connector_type)
    }

    /// Read a cached manifest.
    pub fn read(path: &Path) -> WorkerResult<Option<Manifest>> {
        match std::fs::read_to_string(path) {
            Ok(text) => Ok(serde_json::from_str(&text).ok()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(WorkerError::Io(e)),
        }
    }

    /// Write the manifest atomically (temporary file + rename).
    pub fn write(&self, path: &Path) -> WorkerResult<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension(format!("json.{}.tmp", std::process::id()));
        let text =
            serde_json::to_string_pretty(self).map_err(|e| WorkerError::Protocol(e.to_string()))?;
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

/// Where the manifest is cached: `<root>/cache/plugins_manifest.json`
/// (`CACHE_RESOURCES_PATH`).
pub fn manifest_path(root_dir: &Path) -> PathBuf {
    root_dir.join("cache").join(MANIFEST_FILE_NAME)
}

/// A plugin source tree: a module file (`foo.py`) or a package directory (`foo/`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginSource {
    /// The plugin name.
    pub name: String,
    /// The file or directory.
    pub path: PathBuf,
}

/// The plugins found in `plugins_dirs`, in v4's discovery order (directories in order, entries
/// sorted by name; the first directory providing a name wins). `__init__.py`, hidden entries,
/// `__pycache__` and non-Python files are skipped.
pub fn discover_plugins(plugins_dirs: &[PathBuf]) -> Vec<PluginSource> {
    let mut out: Vec<PluginSource> = Vec::new();
    for dir in plugins_dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        let mut entries: Vec<_> = entries.flatten().map(|e| e.path()).collect();
        entries.sort();
        for path in entries {
            let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if file_name.starts_with('.') || file_name.starts_with('_') {
                continue;
            }
            let name = if path.is_dir() {
                if !path.join("__init__.py").exists() {
                    continue;
                }
                file_name.to_string()
            } else if let Some(stem) = file_name.strip_suffix(".py") {
                stem.to_string()
            } else {
                continue;
            };
            if out.iter().any(|p| p.name == name) {
                continue;
            }
            out.push(PluginSource { name, path });
        }
    }
    out
}

fn hash_tree(hasher: &mut Sha256, root: &Path, path: &Path) {
    let Ok(meta) = std::fs::metadata(path) else {
        return;
    };
    let rel = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");
    if meta.is_dir() {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name == "__pycache__" || (name.starts_with('.') && path != root) {
            return;
        }
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        let mut children: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
        children.sort();
        for child in children {
            hash_tree(hasher, root, &child);
        }
        return;
    }
    // Only Python sources: data files written into a plugin's tree must not invalidate it.
    if path.extension().and_then(|e| e.to_str()) != Some("py") {
        return;
    }
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    hasher.update(rel.as_bytes());
    hasher.update([0]);
    hasher.update(meta.len().to_le_bytes());
    hasher.update(mtime.to_le_bytes());
}

/// The fingerprint of one plugin's source tree: SHA-256 over every file's relative path, size
/// and modification time (`__pycache__` and `.pyc` files excluded).
pub fn plugin_fingerprint(source: &PluginSource) -> String {
    let mut hasher = Sha256::new();
    hasher.update(source.name.as_bytes());
    hasher.update([0]);
    let root = if source.path.is_dir() {
        source.path.clone()
    } else {
        source
            .path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default()
    };
    hash_tree(&mut hasher, &root, &source.path);
    hex::encode(hasher.finalize())
}

/// The combined fingerprint of every plugin in `plugins_dirs`, plus each plugin's own.
pub fn fingerprint(plugins_dirs: &[PathBuf]) -> (String, IndexMap<String, String>) {
    fingerprint_sources(&discover_plugins(plugins_dirs))
}

/// The combined fingerprint of `sources`, plus each plugin's own.
pub fn fingerprint_sources(sources: &[PluginSource]) -> (String, IndexMap<String, String>) {
    let mut per_plugin = IndexMap::new();
    let mut hasher = Sha256::new();
    hasher.update(MANIFEST_SCHEMA_VERSION.to_le_bytes());
    for source in sources {
        let fp = plugin_fingerprint(source);
        hasher.update(source.name.as_bytes());
        hasher.update([0]);
        hasher.update(fp.as_bytes());
        per_plugin.insert(source.name.clone(), fp);
    }
    (hex::encode(hasher.finalize()), per_plugin)
}

/// The plugins a manifest tracks: everything [`discover_plugins`] finds in `plugins_dirs`, then
/// every plugin of `listed` that discovery did not find but that has a `path` (entry-point
/// plugins). A package's `__init__.py` stands for its directory.
fn tracked_sources(
    plugins_dirs: &[PathBuf],
    listed: &IndexMap<String, PluginManifest>,
) -> Vec<PluginSource> {
    let mut sources = discover_plugins(plugins_dirs);
    for (name, info) in listed {
        if sources.iter().any(|s| &s.name == name) {
            continue;
        }
        let Some(path) = info.path.as_deref().filter(|p| !p.is_empty()) else {
            continue;
        };
        let mut path = PathBuf::from(path);
        if path.file_name().is_some_and(|n| n == "__init__.py")
            && let Some(parent) = path.parent()
        {
            path = parent.to_path_buf();
        }
        sources.push(PluginSource {
            name: name.clone(),
            path,
        });
    }
    sources
}

/// Whether a cached manifest is stale for `plugins_dirs`, and the plugins that changed
/// (added, removed or modified). See the module documentation for what is tracked. The
/// entry-point plugins are taken to be the ones the manifest was built with; see
/// [`stale_plugins_for`].
pub fn stale_plugins(manifest: &Manifest, plugins_dirs: &[PathBuf]) -> (bool, Vec<String>) {
    stale_plugins_for(manifest, plugins_dirs, &manifest.entry_point_plugins)
}

/// [`stale_plugins`] for the entry-point plugins `entry_points` the interpreter has now
/// ([`crate::entry_points::plugins`]): a manifest built with another set is stale, and the
/// plugins that appeared or disappeared are reported as changed.
pub fn stale_plugins_for(
    manifest: &Manifest,
    plugins_dirs: &[PathBuf],
    entry_points: &[String],
) -> (bool, Vec<String>) {
    let (mut stale, mut changed) = tracked_staleness(manifest, plugins_dirs);
    let mut built = manifest.entry_point_plugins.clone();
    built.sort();
    let mut now = entry_points.to_vec();
    now.sort();
    if built != now {
        stale = true;
        for name in built.iter().chain(&now) {
            if built.contains(name) != now.contains(name) && !changed.contains(name) {
                changed.push(name.clone());
            }
        }
    }
    (stale, changed)
}

fn tracked_staleness(manifest: &Manifest, plugins_dirs: &[PathBuf]) -> (bool, Vec<String>) {
    let sources = tracked_sources(plugins_dirs, &manifest.plugins);
    let (combined, per_plugin) = fingerprint_sources(&sources);
    let mut changed = Vec::new();
    for (name, fp) in &per_plugin {
        // A discovered plugin Python listed nothing for is covered by the combined fingerprint.
        if let Some(plugin) = manifest.plugins.get(name)
            && plugin.fingerprint.as_ref() != Some(fp)
        {
            changed.push(name.clone());
        }
    }
    for (name, plugin) in &manifest.plugins {
        // Listed with a fingerprint but no longer tracked: its source was removed.
        if !per_plugin.contains_key(name) && plugin.fingerprint.is_some() {
            changed.push(name.clone());
        }
    }
    let stale = manifest.schema_version != MANIFEST_SCHEMA_VERSION
        || manifest.fingerprint.as_deref() != Some(combined.as_str());
    if stale && changed.is_empty() {
        // Added plugins (listed nowhere yet) and a schema bump have no per-plugin report.
        changed.extend(
            per_plugin
                .keys()
                .filter(|name| !manifest.plugins.contains_key(*name))
                .cloned(),
        );
    }
    (stale, changed)
}

/// Build a fresh manifest and stamp it with the fingerprints of the plugins it tracks.
///
/// The manifest is built in a dedicated, freshly started worker (shut down afterwards): pooled
/// workers may hold modules imported before a plugin changed, and importing every plugin must
/// not leak into them.
///
/// `entry_points` are the entry-point plugins the interpreter has ([`crate::entry_points`]):
/// with neither a discovered plugin nor an entry point the manifest is empty and no Python
/// starts.
pub async fn build_manifest(
    pool: &WorkerPool,
    plugins_dirs: &[PathBuf],
    entry_points: &[String],
) -> WorkerResult<Manifest> {
    let (combined, per_plugin) = fingerprint(plugins_dirs);
    if per_plugin.is_empty() && entry_points.is_empty() {
        return Ok(Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            fingerprint: Some(combined),
            ..Manifest::default()
        });
    }
    let worker = pool.spawn_worker().await?;
    let reply = worker
        .call(
            "build_manifest",
            Msg::new(),
            Vec::new(),
            CallOptions {
                timeout: pool.config().call_timeout,
                chunks: None,
            },
        )
        .await;
    worker.shutdown(std::time::Duration::from_secs(5)).await;
    let reply = reply?;
    let json = wire_to_json(&reply.value);
    let mut manifest: Manifest = serde_json::from_value(json)
        .map_err(|e| WorkerError::Protocol(format!("invalid manifest from the worker: {e}")))?;
    manifest.schema_version = MANIFEST_SCHEMA_VERSION;
    manifest.entry_point_plugins = entry_points.to_vec();
    // Fingerprint after the import: a plugin that writes into its own tree while it is
    // imported must not make the fresh manifest stale (the check is idempotent).
    let sources = tracked_sources(plugins_dirs, &manifest.plugins);
    let (combined, per_plugin) = fingerprint_sources(&sources);
    manifest.fingerprint = Some(combined);
    for (name, info) in manifest.plugins.iter_mut() {
        info.fingerprint = per_plugin.get(name).cloned();
    }
    Ok(manifest)
}

/// How [`ensure_manifest`] obtained the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestSource {
    /// The cached manifest was fresh.
    Cache,
    /// The manifest was (re)built in a worker.
    Built,
}

/// Return the cached manifest at `path` if it is fresh for `plugins_dirs`, else rebuild it in a
/// worker and rewrite the cache. A rebuild also recycles the pool's idle workers so later calls
/// import the changed plugins.
pub async fn ensure_manifest(
    pool: &WorkerPool,
    path: &Path,
    plugins_dirs: &[PathBuf],
) -> WorkerResult<(Manifest, ManifestSource)> {
    let entry_points = probe_entry_points(pool, path, false);
    ensure_manifest_for(pool, path, plugins_dirs, &entry_points).await
}

/// [`ensure_manifest`] for entry-point plugins the caller already asked for.
pub async fn ensure_manifest_for(
    pool: &WorkerPool,
    path: &Path,
    plugins_dirs: &[PathBuf],
    entry_points: &[String],
) -> WorkerResult<(Manifest, ManifestSource)> {
    if let Some(cached) = Manifest::read(path)? {
        let (stale, _) = stale_plugins_for(&cached, plugins_dirs, entry_points);
        if !stale {
            return Ok((cached, ManifestSource::Cache));
        }
    }
    rebuild_manifest_for(pool, path, plugins_dirs, entry_points)
        .await
        .map(|manifest| (manifest, ManifestSource::Built))
}

/// The entry-point plugins of the pool's interpreter ([`crate::entry_points::plugins`]); the
/// cache sits next to the manifest at `manifest`.
pub fn probe_entry_points(pool: &WorkerPool, manifest: &Path, force: bool) -> Vec<String> {
    crate::entry_points::plugins(
        &pool.config().worker,
        &crate::entry_points::cache_path(manifest),
        force,
    )
}

/// Rebuild the manifest unconditionally, rewrite the cache at `path` and recycle the pool's idle
/// workers (an unchanged tree never gets here, so it keeps its warm workers).
pub async fn rebuild_manifest(
    pool: &WorkerPool,
    path: &Path,
    plugins_dirs: &[PathBuf],
) -> WorkerResult<Manifest> {
    let entry_points = probe_entry_points(pool, path, true);
    rebuild_manifest_for(pool, path, plugins_dirs, &entry_points).await
}

/// [`rebuild_manifest`] for entry-point plugins the caller already asked for.
pub async fn rebuild_manifest_for(
    pool: &WorkerPool,
    path: &Path,
    plugins_dirs: &[PathBuf],
    entry_points: &[String],
) -> WorkerResult<Manifest> {
    let manifest = build_manifest(pool, plugins_dirs, entry_points).await?;
    manifest.write(path)?;
    pool.recycle().await;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_manifest_never_starts_python() {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let mut config = crate::PoolConfig::default();
        config.worker.python = root.path().join("python-does-not-exist");
        let pool = WorkerPool::new(config);
        let path = manifest_path(root.path());
        let dirs = vec![plugins.path().to_path_buf()];
        let (manifest, source) = ensure_manifest(&pool, &path, &dirs).await.unwrap();
        assert!(manifest.plugins.is_empty());
        assert_eq!(source, ManifestSource::Built);
        assert_eq!(
            ensure_manifest(&pool, &path, &dirs).await.unwrap().1,
            ManifestSource::Cache
        );
        assert_eq!(pool.stats().spawned, 0);
    }

    #[tokio::test]
    async fn an_entry_point_plugin_starts_python_for_an_empty_tree() {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let mut config = crate::PoolConfig::default();
        config.worker.python = root.path().join("python-does-not-exist");
        let pool = WorkerPool::new(config);
        let dirs = vec![plugins.path().to_path_buf()];
        // No plugin of either kind: empty, no process. An entry point: a worker is wanted.
        assert!(
            build_manifest(&pool, &dirs, &[])
                .await
                .unwrap()
                .plugins
                .is_empty()
        );
        let wanted = vec!["w17bhello".to_string()];
        assert!(build_manifest(&pool, &dirs, &wanted).await.is_err());
    }

    #[test]
    fn a_different_entry_point_set_is_stale() {
        let plugins = tempfile::tempdir().unwrap();
        let dirs = vec![plugins.path().to_path_buf()];
        let (combined, _) = fingerprint(&dirs);
        let mut manifest = Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            fingerprint: Some(combined),
            ..Default::default()
        };
        let none: Vec<String> = Vec::new();
        let one = vec!["w17bhello".to_string()];
        assert_eq!(stale_plugins_for(&manifest, &dirs, &none), (false, vec![]));
        // Installed after the manifest was built.
        assert_eq!(
            stale_plugins_for(&manifest, &dirs, &one),
            (true, vec!["w17bhello".to_string()])
        );
        // Built with it: fresh, whatever order the probe lists them in.
        manifest.entry_point_plugins = vec!["w17bhello".into(), "w17bzed".into()];
        let two = vec!["w17bzed".to_string(), "w17bhello".to_string()];
        assert_eq!(stale_plugins_for(&manifest, &dirs, &two), (false, vec![]));
        // Uninstalled since.
        assert_eq!(
            stale_plugins_for(&manifest, &dirs, &one),
            (true, vec!["w17bzed".to_string()])
        );
        assert!(stale_plugins_for(&manifest, &dirs, &none).0);
        // `stale_plugins` judges against what the manifest was built with.
        assert_eq!(stale_plugins(&manifest, &dirs), (false, vec![]));
    }

    #[test]
    fn fingerprint_tracks_changes() {
        let dir = tempfile::tempdir().unwrap();
        let dirs = vec![dir.path().to_path_buf()];
        std::fs::write(dir.path().join("a.py"), "x = 1\n").unwrap();
        std::fs::create_dir(dir.path().join("b")).unwrap();
        std::fs::write(dir.path().join("b").join("__init__.py"), "").unwrap();
        std::fs::write(dir.path().join("README.md"), "ignored").unwrap();
        let names: Vec<_> = discover_plugins(&dirs)
            .into_iter()
            .map(|p| p.name)
            .collect();
        assert_eq!(names, vec!["a", "b"]);

        let (fp1, per1) = fingerprint(&dirs);
        let (fp2, _) = fingerprint(&dirs);
        assert_eq!(fp1, fp2);

        let mut manifest = Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            fingerprint: Some(fp1.clone()),
            ..Default::default()
        };
        for (name, fp) in &per1 {
            manifest.plugins.insert(
                name.clone(),
                PluginManifest {
                    name: name.clone(),
                    fingerprint: Some(fp.clone()),
                    ..Default::default()
                },
            );
        }
        assert_eq!(stale_plugins(&manifest, &dirs), (false, vec![]));

        std::fs::write(dir.path().join("b").join("extra.py"), "y = 2\n").unwrap();
        let (stale, changed) = stale_plugins(&manifest, &dirs);
        assert!(stale);
        assert_eq!(changed, vec!["b"]);

        std::fs::remove_file(dir.path().join("a.py")).unwrap();
        let (_, changed) = stale_plugins(&manifest, &dirs);
        assert_eq!(changed, vec!["b", "a"]);
    }

    /// What [`build_manifest`] does after the worker replied: list `plugins` and stamp them.
    fn stamped(dirs: &[PathBuf], plugins: Vec<PluginManifest>) -> Manifest {
        let mut manifest = Manifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            ..Default::default()
        };
        for plugin in plugins {
            manifest.plugins.insert(plugin.name.clone(), plugin);
        }
        let sources = tracked_sources(dirs, &manifest.plugins);
        let (combined, per_plugin) = fingerprint_sources(&sources);
        manifest.fingerprint = Some(combined);
        for (name, info) in manifest.plugins.iter_mut() {
            info.fingerprint = per_plugin.get(name).cloned();
        }
        manifest
    }

    fn listed(name: &str, path: Option<&Path>) -> PluginManifest {
        PluginManifest {
            name: name.into(),
            path: path.map(|p| p.display().to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn manifest_only_plugin_without_a_path_is_never_stale() {
        let plugins = tempfile::tempdir().unwrap();
        let dirs = vec![plugins.path().to_path_buf()];
        std::fs::write(plugins.path().join("a.py"), "x = 1\n").unwrap();
        // Python listed an extra plugin (an import error, no path) that discovery cannot see.
        let manifest = stamped(
            &dirs,
            vec![
                listed("a", Some(&plugins.path().join("a.py"))),
                listed("ghost", None),
            ],
        );
        assert_eq!(manifest.plugins["ghost"].fingerprint, None);
        assert_eq!(stale_plugins(&manifest, &dirs), (false, vec![]));
        assert_eq!(stale_plugins(&manifest, &dirs), (false, vec![]));
    }

    #[test]
    fn manifest_only_plugin_with_a_path_is_tracked() {
        let plugins = tempfile::tempdir().unwrap();
        let site = tempfile::tempdir().unwrap();
        let dirs = vec![plugins.path().to_path_buf()];
        std::fs::write(plugins.path().join("a.py"), "x = 1\n").unwrap();
        // An entry-point module next to unrelated files, and an entry-point package.
        let module = site.path().join("eppl.py");
        std::fs::write(&module, "x = 1\n").unwrap();
        std::fs::write(site.path().join("other.py"), "y = 1\n").unwrap();
        let package = site.path().join("epkg");
        std::fs::create_dir(&package).unwrap();
        std::fs::write(package.join("__init__.py"), "").unwrap();
        let manifest = stamped(
            &dirs,
            vec![
                listed("a", Some(&plugins.path().join("a.py"))),
                listed("eppl", Some(&module)),
                listed("epkg", Some(&package.join("__init__.py"))),
            ],
        );
        assert!(manifest.plugins["eppl"].fingerprint.is_some());
        assert!(manifest.plugins["epkg"].fingerprint.is_some());
        assert_eq!(stale_plugins(&manifest, &dirs), (false, vec![]));

        // Unrelated files next to the module and data files in the package change nothing.
        std::fs::write(site.path().join("other.py"), "y = 22222\n").unwrap();
        std::fs::write(package.join("cache.json"), "{}").unwrap();
        assert_eq!(stale_plugins(&manifest, &dirs), (false, vec![]));

        // Editing the module or any source of the package invalidates it.
        std::fs::write(&module, "x = 12345\n").unwrap();
        assert_eq!(stale_plugins(&manifest, &dirs), (true, vec!["eppl".into()]));
        let manifest = stamped(
            &dirs,
            vec![
                listed("a", Some(&plugins.path().join("a.py"))),
                listed("eppl", Some(&module)),
                listed("epkg", Some(&package.join("__init__.py"))),
            ],
        );
        std::fs::write(package.join("extra.py"), "z = 1\n").unwrap();
        assert_eq!(stale_plugins(&manifest, &dirs), (true, vec!["epkg".into()]));

        // Removing the source is a change too.
        let manifest = stamped(
            &dirs,
            vec![
                listed("a", Some(&plugins.path().join("a.py"))),
                listed("eppl", Some(&module)),
            ],
        );
        std::fs::remove_file(&module).unwrap();
        assert_eq!(stale_plugins(&manifest, &dirs), (true, vec!["eppl".into()]));
    }

    #[test]
    fn discovered_plugin_missing_from_the_manifest_does_not_loop() {
        let plugins = tempfile::tempdir().unwrap();
        let dirs = vec![plugins.path().to_path_buf()];
        std::fs::write(plugins.path().join("a.py"), "x = 1\n").unwrap();
        std::fs::write(plugins.path().join("b.py"), "x = 2\n").unwrap();
        // Python listed only `a`: `b` is discovered by Rust but absent from the manifest.
        let manifest = stamped(&dirs, vec![listed("a", Some(&plugins.path().join("a.py")))]);
        assert!(!manifest.plugins.contains_key("b"));
        assert_eq!(stale_plugins(&manifest, &dirs), (false, vec![]));
        assert_eq!(stale_plugins(&manifest, &dirs), (false, vec![]));

        // Editing the omitted plugin still invalidates the manifest, and the report names it.
        std::fs::write(plugins.path().join("b.py"), "x = 22222\n").unwrap();
        assert_eq!(stale_plugins(&manifest, &dirs), (true, vec!["b".into()]));

        // A plugin added after the build is reported as changed.
        let manifest = stamped(&dirs, vec![listed("a", Some(&plugins.path().join("a.py")))]);
        std::fs::write(plugins.path().join("c.py"), "x = 3\n").unwrap();
        let (stale, changed) = stale_plugins(&manifest, &dirs);
        // Unlisted plugins carry no fingerprint of their own: the report names all of them.
        assert!(stale);
        assert_eq!(changed, vec!["b", "c"]);
    }

    #[test]
    fn schema_version_bump_is_stale() {
        let plugins = tempfile::tempdir().unwrap();
        let dirs = vec![plugins.path().to_path_buf()];
        std::fs::write(plugins.path().join("a.py"), "x = 1\n").unwrap();
        let mut manifest = stamped(&dirs, vec![listed("a", Some(&plugins.path().join("a.py")))]);
        assert_eq!(stale_plugins(&manifest, &dirs), (false, vec![]));
        manifest.schema_version = MANIFEST_SCHEMA_VERSION - 1;
        let (stale, changed) = stale_plugins(&manifest, &dirs);
        assert!(stale);
        assert!(changed.is_empty());
    }

    #[test]
    fn data_files_do_not_change_the_fingerprint() {
        let plugins = tempfile::tempdir().unwrap();
        let dirs = vec![plugins.path().to_path_buf()];
        let package = plugins.path().join("pkg");
        std::fs::create_dir(&package).unwrap();
        std::fs::write(package.join("__init__.py"), "").unwrap();
        let before = fingerprint(&dirs);
        std::fs::write(package.join("state.json"), "{\"n\": 1}").unwrap();
        std::fs::write(package.join("state.json"), "{\"n\": 22}").unwrap();
        assert_eq!(fingerprint(&dirs), before);
        std::fs::write(package.join("mod.py"), "").unwrap();
        assert_ne!(fingerprint(&dirs), before);
    }

    #[test]
    fn the_console_is_a_reason_for_the_api_host() {
        let mut manifest = Manifest::default();
        assert!(!manifest.needs_api_host());
        assert!(manifest.api_host_reasons().is_empty());
        manifest.console = true;
        assert!(manifest.needs_api_host());
        assert_eq!(manifest.api_host_reasons(), vec![HostReason::Console]);
        manifest.mcp_tools = vec!["tools".into()];
        manifest.api_plugins = vec!["routes".into()];
        assert_eq!(
            manifest.api_host_reasons(),
            vec![
                HostReason::ApiPlugins,
                HostReason::McpTools,
                HostReason::Console
            ]
        );
        let names: Vec<_> = manifest
            .api_host_reasons()
            .into_iter()
            .map(HostReason::name)
            .collect();
        assert_eq!(names, vec!["api_plugins", "mcp_tools", "console"]);
        // The flag is written only when set (a worker never sets it), and read back.
        let text = serde_json::to_string(&manifest).unwrap();
        assert!(text.contains("\"console\":true"));
        assert!(
            !serde_json::to_string(&Manifest::default())
                .unwrap()
                .contains("console")
        );
        let back: Manifest = serde_json::from_str(&text).unwrap();
        assert!(back.console);
        let schema: serde_json::Value = serde_json::from_str(MANIFEST_JSON_SCHEMA).unwrap();
        assert_eq!(schema["properties"]["console"]["type"], "boolean");
    }

    #[test]
    fn schema_is_valid_json() {
        let schema: serde_json::Value = serde_json::from_str(MANIFEST_JSON_SCHEMA).unwrap();
        assert_eq!(
            schema["properties"]["schema_version"]["const"],
            MANIFEST_SCHEMA_VERSION
        );
        let required: Vec<&str> = schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        let sample = serde_json::to_value(Manifest::default()).unwrap();
        for key in required {
            assert!(sample.get(key).is_some(), "{key} missing from Manifest");
        }
        for key in sample.as_object().unwrap().keys() {
            assert!(
                schema["properties"].get(key).is_some(),
                "{key} missing from the schema"
            );
        }
    }
}
