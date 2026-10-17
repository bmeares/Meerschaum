//! How the engine reaches connectors (§4.4, D11, D12).
//!
//! The engine never constructs connectors itself. A [`ConnectorRegistry`] (implemented by the
//! CLI, the Python bindings and the server, each with its own config and worker pool) maps
//! connector keys to trait objects:
//!
//! - instance connectors ([`InstanceConnector`]): `sql:`, `valkey:`, `api:`, and the Python
//!   adapter's `PythonConnector` for custom instance connectors;
//! - source connectors ([`Connector`]): anything with `fetch`;
//! - optional seams for code that is not a plain `fetch`: a plugin's own `sync()`
//!   ([`PluginSync`]), streaming sources ([`crate::stream::StreamSource`] and the v4-pattern
//!   [`crate::stream::LegacyStream`]), and `@pre_sync_hook` / `@post_sync_hook`
//!   ([`SyncHooks`]). The worker agent's Python adapter implements these.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use meerschaum_connector::{Connector, InstanceConnector, async_trait};
use meerschaum_core::success::SyncResult;
use meerschaum_core::value::apply_patch_maps;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};

use crate::stream::{LegacyStream, StreamSource};
use crate::sync::{DeltaObserver, SyncArgs, SyncEnv};

/// A plugin connector whose module defines `sync(pipe, **kw)` (v4 `Pipe.sync` returns its
/// result instead of fetching, `core/Pipe/_sync.py` "If connector is a plugin with a `sync()`
/// method").
#[async_trait]
pub trait PluginSync: Send + Sync {
    /// Run the plugin's `sync()` for `pipe`.
    async fn sync(&self, pipe: &Pipe, args: &SyncArgs) -> SyncResult;

    /// Run with the enclosing native sync context, including its pre-commit ledger observer.
    /// Adapters may forward plugin callbacks through this context before acknowledging them.
    async fn sync_native(
        &self,
        pipe: &Pipe,
        args: &SyncArgs,
        env: &SyncEnv<'_>,
        observer: Option<&dyn DeltaObserver>,
    ) -> SyncResult {
        let _ = (env, observer);
        self.sync(pipe, args).await
    }
}

/// `@pre_sync_hook` / `@post_sync_hook` (§4.2). Called around every engine-run node sync with
/// the v4 keyword arguments; failures are logged, never fatal.
#[async_trait]
pub trait SyncHooks: Send + Sync {
    /// Before a pipe syncs.
    async fn pre_sync(&self, pipe: &Pipe, args: &SyncArgs);
    /// After a pipe synced (`success_tuple`, `sync_duration`, ...).
    async fn post_sync(&self, pipe: &Pipe, args: &SyncArgs, result: &SyncResult);
}

/// Resolves connector keys for the engine.
pub trait ConnectorRegistry: Send + Sync {
    /// The instance connector for `instance_keys`.
    fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String>;

    /// The source connector for `connector_keys`. `Ok(None)` when the keys name no
    /// connector that can fetch (v4: keys without `:`, "does not support fetching").
    fn source(&self, keys: &str) -> Result<Option<Arc<dyn Connector>>, String>;

    /// SQL fetch chunk size when the caller requests the configured default.
    fn fetch_chunksize(&self, _keys: &str) -> usize {
        100_000
    }

    /// Resolve a source for a sync, retaining plugin-specific keyword arguments.
    fn source_for_sync(
        &self,
        keys: &str,
        args: &SyncArgs,
    ) -> Result<Option<Arc<dyn Connector>>, String> {
        let _ = args;
        self.source(keys)
    }

    /// A plugin connector with its own `sync()`, if `keys` names one.
    fn plugin_sync(&self, keys: &str) -> Option<Arc<dyn PluginSync>> {
        let _ = keys;
        None
    }

    /// A streaming source (`IS_STREAMING = True` with `subscribe`), if `keys` names one.
    fn stream_source(&self, keys: &str) -> Option<Arc<dyn StreamSource>> {
        let _ = keys;
        None
    }

    /// A v4-pattern streaming connector (a `sync()` that subscribes and returns immediately),
    /// if `keys` names one (§5.6 "Compatibility").
    fn legacy_stream(&self, keys: &str) -> Option<Arc<dyn LegacyStream>> {
        let _ = keys;
        None
    }

    /// Whether syncing a pipe with these connector keys goes through the Python adapter
    /// (counts against `system.engine.python_workers`).
    fn is_python(&self, keys: &str) -> bool {
        let _ = keys;
        false
    }

    /// The registered sync hooks, if any plugin declares them.
    fn hooks(&self) -> Option<Arc<dyn SyncHooks>> {
        None
    }

    /// The table reference `{{ Pipe(...) }}` renders to. Python renders the quoted,
    /// schema-qualified name (`sql_item_name`) on SQL instances and `pipe.target` elsewhere;
    /// registries that hold SQL instances override this.
    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        let loaded = load_registered(self, pipe, None);
        loaded.target(None).map_err(|e| e.to_string())
    }
}

/// How long a registered-parameters lookup is reused by [`StaticRegistry`] (a run is far shorter;
/// the bound only keeps a registry that outlives its run from serving a stale `target`).
const REGISTERED_TTL: Duration = Duration::from_secs(30);

/// Registered parameters fetched for table references, by `instance|connector|metric|location`.
type RegisteredCache = Mutex<HashMap<String, (Instant, Option<Map>)>>;

fn pipe_cache_key(pipe: &Pipe) -> String {
    pipe.pipe_ref()
}

/// Whether the pipe's own (call-site) parameters already name its table.
fn names_target(pipe: &Pipe) -> bool {
    let raw = pipe.raw_parameters();
    ["target", "target_name", "target_table"]
        .iter()
        .any(|k| raw.contains_key(*k))
}

/// `pipe` carrying the parameters registered on its own instance (`Pipe.parameters` reads them
/// from there), so a `{{ Pipe('a', 'b') }}` reference to a pipe with an explicit `target`
/// renders that table instead of the default name. A reference is built from keys only, so
/// without this step the explicit target is lost.
///
/// The lookup is async while table references are synchronous (the template resolver's
/// contract). It runs on the current Tokio runtime when that is multi-threaded (every runtime
/// the CLI, the Python bindings and the server build); otherwise, or when the instance is
/// unknown, unreachable or the pipe unregistered, the pipe is returned as is.
fn load_registered(
    registry: &(impl ConnectorRegistry + ?Sized),
    pipe: &Pipe,
    cache: Option<&RegisteredCache>,
) -> Pipe {
    if names_target(pipe) {
        return pipe.clone();
    }
    let key = pipe_cache_key(pipe);
    if let Some(cache) = cache {
        let guard = cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((at, registered)) = guard.get(&key) {
            if at.elapsed() < REGISTERED_TTL {
                return with_registered(pipe, registered.as_ref());
            }
        }
    }
    let registered = fetch_registered(registry, pipe);
    if let Some(cache) = cache {
        cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, (Instant::now(), registered.clone()));
    }
    with_registered(pipe, registered.as_ref())
}

fn with_registered(pipe: &Pipe, registered: Option<&Map>) -> Pipe {
    match registered {
        Some(params) => {
            let mut out = pipe.clone();
            out.set_parameters(apply_patch_maps(params, pipe.raw_parameters()));
            out
        }
        None => pipe.clone(),
    }
}

fn fetch_registered(registry: &(impl ConnectorRegistry + ?Sized), pipe: &Pipe) -> Option<Map> {
    let instance = registry.instance(pipe.instance_keys()).ok()?;
    let handle = tokio::runtime::Handle::try_current().ok()?;
    if handle.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
        return None;
    }
    let attrs =
        tokio::task::block_in_place(|| handle.block_on(instance.get_pipe_attributes(pipe))).ok()?;
    attrs.get("parameters").map(Value::to_map)
}

/// Renders `{{ Pipe(...) }}` table references.
type TableRefFn = Arc<dyn Fn(&Pipe) -> Result<String, String> + Send + Sync>;

/// A [`ConnectorRegistry`] over fixed maps (tests, and hosts that build connectors up front).
#[derive(Default, Clone)]
pub struct StaticRegistry {
    instances: HashMap<String, Arc<dyn InstanceConnector>>,
    sources: HashMap<String, Arc<dyn Connector>>,
    chunksizes: HashMap<String, usize>,
    plugin_syncs: HashMap<String, Arc<dyn PluginSync>>,
    streams: HashMap<String, Arc<dyn StreamSource>>,
    legacy_streams: HashMap<String, Arc<dyn LegacyStream>>,
    hooks: Option<Arc<dyn SyncHooks>>,
    table_refs: Option<TableRefFn>,
    registered: Arc<RegisteredCache>,
}

impl std::fmt::Debug for StaticRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticRegistry")
            .field("instances", &self.instances.keys().collect::<Vec<_>>())
            .field("sources", &self.sources.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl StaticRegistry {
    /// An empty registry.
    pub fn new() -> StaticRegistry {
        StaticRegistry::default()
    }

    /// Register an instance connector under its keys.
    pub fn with_instance(mut self, keys: &str, conn: Arc<dyn InstanceConnector>) -> Self {
        self.instances.insert(keys.to_string(), conn);
        self
    }

    /// Register a source connector under its keys.
    pub fn with_source(mut self, keys: &str, conn: Arc<dyn Connector>) -> Self {
        self.sources.insert(keys.to_string(), conn);
        self
    }

    /// Set the configured SQL fetch chunk size.
    pub fn with_fetch_chunksize(mut self, keys: &str, chunksize: usize) -> Self {
        self.chunksizes.insert(keys.to_owned(), chunksize.max(1));
        self
    }

    /// Register a plugin `sync()`.
    pub fn with_plugin_sync(mut self, keys: &str, sync: Arc<dyn PluginSync>) -> Self {
        self.plugin_syncs.insert(keys.to_string(), sync);
        self
    }

    /// Register a stream source.
    pub fn with_stream(mut self, keys: &str, source: Arc<dyn StreamSource>) -> Self {
        self.streams.insert(keys.to_string(), source);
        self
    }

    /// Register a v4-pattern streaming connector.
    pub fn with_legacy_stream(mut self, keys: &str, source: Arc<dyn LegacyStream>) -> Self {
        self.legacy_streams.insert(keys.to_string(), source);
        self
    }

    /// Register sync hooks.
    pub fn with_hooks(mut self, hooks: Arc<dyn SyncHooks>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    /// Override how `{{ Pipe(...) }}` renders a table reference.
    pub fn with_table_reference(
        mut self,
        f: impl Fn(&Pipe) -> Result<String, String> + Send + Sync + 'static,
    ) -> Self {
        self.table_refs = Some(Arc::new(f));
        self
    }
}

impl ConnectorRegistry for StaticRegistry {
    fn fetch_chunksize(&self, keys: &str) -> usize {
        self.chunksizes.get(keys).copied().unwrap_or(100_000)
    }

    fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        self.instances
            .get(keys)
            .cloned()
            .ok_or_else(|| format!("No instance connector is registered for '{keys}'."))
    }

    fn source(&self, keys: &str) -> Result<Option<Arc<dyn Connector>>, String> {
        if !keys.contains(':') {
            return Ok(None);
        }
        if let Some(c) = self.sources.get(keys) {
            return Ok(Some(c.clone()));
        }
        Err(format!("No connector is registered for '{keys}'."))
    }

    fn plugin_sync(&self, keys: &str) -> Option<Arc<dyn PluginSync>> {
        self.plugin_syncs.get(keys).cloned()
    }

    fn stream_source(&self, keys: &str) -> Option<Arc<dyn StreamSource>> {
        self.streams.get(keys).cloned()
    }

    fn legacy_stream(&self, keys: &str) -> Option<Arc<dyn LegacyStream>> {
        self.legacy_streams.get(keys).cloned()
    }

    fn hooks(&self) -> Option<Arc<dyn SyncHooks>> {
        self.hooks.clone()
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        let loaded = load_registered(self, pipe, Some(&self.registered));
        match &self.table_refs {
            Some(f) => f(&loaded),
            None => loaded.target(None).map_err(|e| e.to_string()),
        }
    }
}

/// A [`SyncHooks`] that does nothing (for tests that count calls, wrap this).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoHooks;

#[async_trait]
impl SyncHooks for NoHooks {
    async fn pre_sync(&self, _pipe: &Pipe, _args: &SyncArgs) {}
    async fn post_sync(&self, _pipe: &Pipe, _args: &SyncArgs, _result: &SyncResult) {}
}

/// `(True, "Success")` as a [`SyncResult`].
pub fn success_result() -> SyncResult {
    SuccessTuple::success().into()
}
