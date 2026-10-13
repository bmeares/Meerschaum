//! A process-level view of the native plugins in the configured plugin directories: the merged
//! manifest (cached without loading code on a fingerprint hit) and lazily loaded libraries.
//!
//! Plugin directories are trusted exactly as v4's Python plugin directories are: placing a
//! plugin there is consent to run its code. The libraries are loaded on first use only.
use crate::{
    LoadedPlugin, NativeConnector, Result,
    discovery::{self, Candidate},
    error,
};
use serde_json::Value;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

/// The native plugins found in an ordered list of plugin directories.
pub struct NativePlugins {
    candidates: Vec<Candidate>,
    manifest: Value,
    loaded: Mutex<HashMap<String, Arc<LoadedPlugin>>>,
}

impl NativePlugins {
    /// Discover native plugins and read or rebuild the manifest cached at `cache`.
    /// A cache hit loads no code. Directories without native plugins yield an empty set.
    pub fn open(directories: &[PathBuf], cache: &Path) -> Result<Self> {
        let discovery::Discovery {
            mut candidates,
            skipped,
        } = discovery::discover_report(directories);
        for item in &skipped {
            eprintln!("Warning: {}", item.message());
        }
        if candidates.is_empty() {
            return Ok(Self::empty());
        }
        // SAFETY: the configured plugin directories are trusted (see the module documentation).
        // Load-time failures were not visible to `discover_report`; warn about those here.
        let (manifest, load_skipped) =
            unsafe { discovery::cached_manifest_report(directories, cache) }?;
        let known = skipped.len();
        for item in load_skipped.iter().skip(known) {
            eprintln!("Warning: {}", item.message());
        }
        candidates.retain(|c| manifest["plugins"].get(&c.name).is_some());
        if candidates.is_empty() {
            return Ok(Self::empty());
        }
        Ok(Self {
            candidates,
            manifest,
            loaded: Mutex::new(HashMap::new()),
        })
    }

    /// No native plugins.
    pub fn empty() -> Self {
        Self {
            candidates: Vec::new(),
            manifest: Value::Null,
            loaded: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    /// The worker-shaped manifest of the native plugins (`Null` when there are none).
    pub fn manifest(&self) -> &Value {
        &self.manifest
    }

    /// The manifest entry of the plugin `name` (`Null` when unknown).
    pub fn plugin_entry(&self, name: &str) -> &Value {
        &self.manifest["plugins"][name]
    }

    /// The names of the discovered plugins, in discovery order.
    pub fn names(&self) -> Vec<String> {
        self.candidates.iter().map(|c| c.name.clone()).collect()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.candidates.iter().any(|c| c.name == name)
    }

    /// The plugin named `name`, loading its library on first use.
    pub fn plugin(&self, name: &str) -> Result<Arc<LoadedPlugin>> {
        let mut loaded = self
            .loaded
            .lock()
            .map_err(|_| error("plugin table poisoned"))?;
        if let Some(plugin) = loaded.get(name) {
            return Ok(plugin.clone());
        }
        let candidate = self
            .candidates
            .iter()
            .find(|c| c.name == name)
            .ok_or_else(|| error(format!("unknown native plugin: {name}")))?;
        // SAFETY: trusted plugin directory.
        let plugin = unsafe { LoadedPlugin::load(&candidate.library) }?;
        loaded.insert(name.to_owned(), plugin.clone());
        Ok(plugin)
    }

    /// The plugin that declares the connector type `connector_type`.
    pub fn connector_plugin(&self, connector_type: &str) -> Option<String> {
        let plugin = self.manifest["connector_types"][connector_type]["plugin"].as_str()?;
        self.contains(plugin).then(|| plugin.to_owned())
    }

    /// Whether the connector type is a native instance connector.
    pub fn is_instance_type(&self, connector_type: &str) -> bool {
        self.connector_plugin(connector_type).is_some()
            && self.manifest["connector_types"][connector_type]["is_instance"]
                .as_bool()
                .unwrap_or(false)
    }

    /// A connector `<connector_type>:<label>` from a native plugin, if one declares the type.
    pub fn connector(
        &self,
        connector_type: &str,
        label: &str,
        config: Value,
    ) -> Option<Result<NativeConnector>> {
        let name = self.connector_plugin(connector_type)?;
        Some(
            self.plugin(&name)
                .and_then(|plugin| plugin.connector(connector_type, label, config)),
        )
    }

    /// The source connector `plugin:<name>` if `name` is a native plugin with a `fetch`.
    pub fn source(&self, name: &str) -> Option<Result<NativeConnector>> {
        if !self.contains(name) {
            return None;
        }
        let functions = self.manifest["plugins"][name]["data_functions"].as_array()?;
        functions
            .iter()
            .any(|f| f == "fetch")
            .then(|| self.plugin(name).map(|plugin| plugin.source(Value::Null)))
    }

    /// Whether `plugin:<name>` is a native plugin declaring the data function `function`.
    pub fn has_data_function(&self, name: &str, function: &str) -> bool {
        self.contains(name)
            && self.manifest["plugins"][name]["data_functions"]
                .as_array()
                .is_some_and(|f| f.iter().any(|f| f == function))
    }

    /// Plugins with the pre or post sync hooks, with the hook names.
    pub fn hooks(&self, post: bool) -> Vec<(String, Vec<String>)> {
        let key = if post {
            "post_sync_hooks"
        } else {
            "pre_sync_hooks"
        };
        self.names()
            .into_iter()
            .filter_map(|name| {
                let names: Vec<String> = self.manifest["plugins"][&name][key]
                    .as_array()?
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect();
                (!names.is_empty()).then_some((name, names))
            })
            .collect()
    }
}
