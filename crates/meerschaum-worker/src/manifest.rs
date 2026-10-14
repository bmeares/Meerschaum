//! Plugin manifests (§4.3 "Plugin action manifests").
//!
//! The manifest lists, per installed plugin, its `@make_action` actions (with docstrings and
//! subactions), `add_plugin_argument` arguments, sync hooks, `@api_plugin` / `@dash_plugin` /
//! `@web_page` registrations, MCP tools / resources / prompts, custom connector types and data
//! functions. A worker builds it (`build_manifest`); the host caches it at
//! [`manifest_path`] keyed by a [`fingerprint`] of every plugin source tree (relative paths,
//! sizes and modification times), so `--help`, completion and argument validation never start
//! Python unless a plugin changed.
//!
//! The JSON schema is `schema/manifest.schema.json` in this crate. The top-level
//! `api_plugins` / `dash_plugins` / `web_pages` / `mcp_tools` lists are the plugin names the
//! server's `HostManifest` reads to decide whether the API host is needed.

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
/// missed them) are rebuilt.
pub const MANIFEST_SCHEMA_VERSION: u32 = 4;

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
    /// Plugins with `@pre_sync_hook` functions.
    #[serde(default)]
    pub pre_sync_hooks: Vec<String>,
    /// Plugins with `@post_sync_hook` functions.
    #[serde(default)]
    pub post_sync_hooks: Vec<String>,
    /// Errors raised while importing all plugins at once.
    #[serde(default)]
    pub load_errors: IndexMap<String, String>,
}

impl Manifest {
    /// Whether any plugin registers a sync hook (if not, no worker starts for hooks, §4.2).
    pub fn has_sync_hooks(&self) -> bool {
        !self.pre_sync_hooks.is_empty() || !self.post_sync_hooks.is_empty()
    }

    /// Whether any plugin needs the API host (§7.3).
    pub fn needs_api_host(&self) -> bool {
        !(self.api_plugins.is_empty()
            && self.dash_plugins.is_empty()
            && self.web_pages.is_empty()
            && self.mcp_tools.is_empty())
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
    if path.extension().and_then(|e| e.to_str()) == Some("pyc") {
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
    let mut per_plugin = IndexMap::new();
    let mut hasher = Sha256::new();
    hasher.update(MANIFEST_SCHEMA_VERSION.to_le_bytes());
    for source in discover_plugins(plugins_dirs) {
        let fp = plugin_fingerprint(&source);
        hasher.update(source.name.as_bytes());
        hasher.update([0]);
        hasher.update(fp.as_bytes());
        per_plugin.insert(source.name, fp);
    }
    (hex::encode(hasher.finalize()), per_plugin)
}

/// Whether a cached manifest is stale for `plugins_dirs`, and the plugins that changed
/// (added, removed or modified).
pub fn stale_plugins(manifest: &Manifest, plugins_dirs: &[PathBuf]) -> (bool, Vec<String>) {
    let (combined, per_plugin) = fingerprint(plugins_dirs);
    let mut changed = Vec::new();
    for (name, fp) in &per_plugin {
        let cached = manifest
            .plugins
            .get(name)
            .and_then(|p| p.fingerprint.as_ref());
        if cached != Some(fp) {
            changed.push(name.clone());
        }
    }
    for name in manifest.plugins.keys() {
        if !per_plugin.contains_key(name) {
            changed.push(name.clone());
        }
    }
    let stale = manifest.schema_version != MANIFEST_SCHEMA_VERSION
        || manifest.fingerprint.as_deref() != Some(combined.as_str())
        || !changed.is_empty();
    (stale, changed)
}

/// Build a fresh manifest and stamp it with the fingerprints of `plugins_dirs`.
///
/// The manifest is built in a dedicated, freshly started worker (shut down afterwards): pooled
/// workers may hold modules imported before a plugin changed, and importing every plugin must
/// not leak into them.
pub async fn build_manifest(pool: &WorkerPool, plugins_dirs: &[PathBuf]) -> WorkerResult<Manifest> {
    // Fingerprint first: a plugin edited while the worker imports it is rebuilt next time.
    let (combined, per_plugin) = fingerprint(plugins_dirs);
    if per_plugin.is_empty() {
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
    if let Some(cached) = Manifest::read(path)? {
        let (stale, _) = stale_plugins(&cached, plugins_dirs);
        if !stale {
            return Ok((cached, ManifestSource::Cache));
        }
    }
    let manifest = build_manifest(pool, plugins_dirs).await?;
    manifest.write(path)?;
    pool.recycle().await;
    Ok((manifest, ManifestSource::Built))
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

    #[test]
    fn schema_is_valid_json() {
        let schema: serde_json::Value = serde_json::from_str(MANIFEST_JSON_SCHEMA).unwrap();
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
