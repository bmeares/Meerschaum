//! The connectors the native actions use: a Tokio runtime and the instance and source
//! connectors resolved from the config registry (`meerschaum/connectors/__init__.py`
//! `get_connector` and `connectors/parse.py` `parse_instance_keys`).
//!
//! Connectors are built from `meerschaum:connectors:<type>:default` patched with the label's
//! section, which includes the `MRSM_SQL_<LABEL>` / `MRSM_API_<LABEL>` environment connectors
//! (the config registry adds them when it is created).
//!
//! **Connectors built elsewhere.** A host that already holds connectors (the Python façade:
//! `mrsm.get_connector('sql', 'x', flavor='sqlite', database=...)` builds connectors from
//! attributes that are in no config file) registers them with [`Session::register_instance`] /
//! [`Session::register_sql`]; a lookup by keys then returns the registered connector instead of
//! reading the config. [`Session::with_runtime`] shares the host's Tokio runtime (connection pools
//! are bound to the runtime that created them), [`Session::set_registered_only`] makes lookups of
//! unregistered keys fail instead of connecting to configured connectors, and
//! [`Session::set_python_worker`] turns off the Python worker (empty manifests, no child
//! process). With nothing registered and the defaults, the config path is unchanged.
//!
//! **The plugin manifest is cached in memory per session.** The first [`Session::manifest`]
//! validates the plugin trees (one fingerprint walk, [`stale_plugins`]) and rebuilds the Python
//! manifest in a fresh worker only when something changed; later calls return the same `Arc`
//! without touching the disk. [`Session::invalidate_manifest`] re-arms the single validation
//! the next call performs: the shell does it after every command, so a plugin edited while the
//! user was typing is picked up at the next prompt (spec §4.3: "rebuilt when a file changes")
//! at the price of one cheap walk per prompt, never one per call. The worker pool is recycled
//! only when the manifest was really rebuilt (or replaced by a different cached one); an
//! unchanged tree keeps its warm workers. [`Session::refresh_manifest`] validates now
//! (`install`/`uninstall`/`setup`/`upgrade plugins`) and [`Session::reload_plugins`] rebuilds
//! unconditionally (`reload`). Native plugins are re-opened only when their discovery
//! signature changed.

use meerschaum_worker::manifest::{
    Manifest, ManifestSource, ensure_manifest_for, manifest_path, probe_entry_points,
    rebuild_manifest_for, stale_plugins_for,
};
use meerschaum_worker::{PoolConfig, WorkerPool};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::flavor::Flavor;
use meerschaum_connector_sql::instance::InstanceSettings;
use meerschaum_connector_sql::uri::{ConnectionOptions, ConnectionSpec, ConnectorAttributes};
use meerschaum_core::config::Config;
use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{pyfmt, static_config};
use meerschaum_plugin::NativePlugins;

use crate::builtin::ActionError;

/// An instance connector and its concrete type (SQL methods without a trait counterpart, such
/// as `get_partition_info` and the in-place sync, need the concrete connector).
#[derive(Clone)]
pub enum Instance {
    /// A `sql:` instance.
    Sql(Arc<SqlConnector>),
    /// Any other instance (`api:`, `valkey:`).
    Other(Arc<dyn InstanceConnector>),
}

impl Instance {
    /// The trait object.
    pub fn conn(&self) -> Arc<dyn InstanceConnector> {
        match self {
            Instance::Sql(c) => c.clone(),
            Instance::Other(c) => c.clone(),
        }
    }

    /// `str(connector)`.
    pub fn keys(&self) -> String {
        self.conn().connector_keys()
    }

    /// `connector.type`.
    pub fn type_name(&self) -> String {
        self.conn().connector_type().to_string()
    }

    /// The SQL connector, if this is a SQL instance.
    pub fn sql(&self) -> Option<&Arc<SqlConnector>> {
        match self {
            Instance::Sql(c) => Some(c),
            Instance::Other(_) => None,
        }
    }
}

/// The session's runtime: owned (the CLI) or a handle to the host's (the Python extension).
enum SessionRuntime {
    Owned(tokio::runtime::Runtime),
    Shared(tokio::runtime::Handle),
}

type WorkerEnvironment = Vec<(String, String)>;

impl SessionRuntime {
    fn handle(&self) -> &tokio::runtime::Handle {
        match self {
            SessionRuntime::Owned(rt) => rt.handle(),
            SessionRuntime::Shared(handle) => handle,
        }
    }
}

/// The Tokio runtime and the connectors built so far (`meerschaum.connectors.connectors`).
#[derive(Clone)]
pub struct Session {
    rt: Arc<SessionRuntime>,
    instances: Arc<Mutex<HashMap<String, Instance>>>,
    /// Keys whose connector was registered by the host (never rebuilt from config).
    registered: Arc<Mutex<HashSet<String>>>,
    /// Refuse to build connectors for keys nobody registered.
    registered_only: Arc<AtomicBool>,
    /// Whether the Python worker (and the plugin manifest it builds) may be used.
    python_worker: Arc<AtomicBool>,
    pool: Arc<Mutex<Option<WorkerPool>>>,
    worker_environment: Arc<Mutex<Option<WorkerEnvironment>>>,
    /// The interpreter the host suggests for its workers (the Python extension passes its own
    /// `sys.executable`); `MRSM_PYTHON` in the environment still wins.
    worker_python: Arc<Mutex<Option<String>>>,
    hook_results: Arc<Mutex<Vec<meerschaum_core::SuccessTuple>>>,
    manifests: Arc<Mutex<ManifestState>>,
}

/// The session's plugin manifests and whether they were validated since the last
/// [`Session::invalidate_manifest`].
#[derive(Default)]
struct ManifestState {
    python: Option<Arc<Manifest>>,
    natives: Option<Arc<NativePlugins>>,
    /// The native plugins' discovery signature `natives` was opened for.
    native_signature: Option<Vec<String>>,
    /// The error from opening the native plugins, reported as a `load_errors` entry.
    native_error: Option<String>,
    merged: Option<Arc<Manifest>>,
    validated: bool,
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    /// A new session with a multi-threaded runtime.
    pub fn new() -> Session {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        Self::from_runtime(SessionRuntime::Owned(rt))
    }

    /// A session on a runtime the caller owns (the Python extension's, which its connectors'
    /// pools were built on). `handle` must belong to a multi-threaded runtime that outlives the
    /// session; [`Session::block_on`] must not be called from inside that runtime.
    pub fn with_runtime(handle: tokio::runtime::Handle) -> Session {
        Self::from_runtime(SessionRuntime::Shared(handle))
    }

    fn from_runtime(rt: SessionRuntime) -> Session {
        Session {
            rt: Arc::new(rt),
            instances: Arc::new(Mutex::new(HashMap::new())),
            registered: Arc::new(Mutex::new(HashSet::new())),
            registered_only: Arc::new(AtomicBool::new(false)),
            python_worker: Arc::new(AtomicBool::new(true)),
            pool: Arc::new(Mutex::new(None)),
            worker_environment: Arc::new(Mutex::new(None)),
            worker_python: Arc::new(Mutex::new(None)),
            hook_results: Arc::new(Mutex::new(Vec::new())),
            manifests: Arc::new(Mutex::new(ManifestState::default())),
        }
    }

    /// Register a connector the host built for `keys` (`type:label`; a bare type means the
    /// default label). Later [`Session::instance`] lookups of those keys return it instead of
    /// building one from the config; registering again replaces it. Returns the connector it
    /// replaced (registered or built from config), if any.
    pub fn register_instance(&self, keys: &str, instance: Instance) -> Option<Instance> {
        let full = full_keys(keys);
        self.registered
            .lock()
            .expect("registered")
            .insert(full.clone());
        self.instances
            .lock()
            .expect("instances")
            .insert(full, instance)
    }

    /// [`Session::register_instance`] for a SQL connector.
    pub fn register_sql(&self, keys: &str, conn: Arc<SqlConnector>) -> Option<Instance> {
        self.register_instance(keys, Instance::Sql(conn))
    }

    /// Forget the connector for `keys` (registered or built); the next lookup builds it again.
    pub fn unregister(&self, keys: &str) -> Option<Instance> {
        let full = full_keys(keys);
        self.registered.lock().expect("registered").remove(&full);
        self.instances.lock().expect("instances").remove(&full)
    }

    /// Whether the host registered the connector for `keys`.
    pub fn is_registered(&self, keys: &str) -> bool {
        self.registered
            .lock()
            .expect("registered")
            .contains(&full_keys(keys))
    }

    /// When `true`, [`Session::instance`] fails for keys nobody registered (and never reads
    /// the config or starts a worker to build them). Off by default.
    pub fn set_registered_only(&self, on: bool) {
        self.registered_only.store(on, Ordering::SeqCst);
    }

    /// When `false`, the session never starts the Python worker: the plugin manifests are
    /// empty and non-native connector types cannot be resolved. On by default.
    pub fn set_python_worker(&self, on: bool) {
        self.python_worker.store(on, Ordering::SeqCst);
    }

    /// Whether this session may start the Python worker ([`Session::set_python_worker`]).
    pub fn worker_enabled(&self) -> bool {
        self.python_worker.load(Ordering::SeqCst)
    }

    /// Set an isolated session's complete worker environment before creating its pool.
    /// Clones share this setting; the host process environment is never changed.
    pub fn set_worker_environment(&self, env: Vec<(String, String)>) -> Result<(), ActionError> {
        let pool = self.pool.lock().expect("worker pool");
        if pool.is_some() {
            return Err(ActionError::new(
                "RuntimeError",
                "Worker pool already initialized.",
            ));
        }
        *self.worker_environment.lock().expect("worker environment") = Some(env);
        Ok(())
    }

    /// Suggest the interpreter the workers run with when `MRSM_PYTHON` is not set (the host
    /// process's environment is never changed). Takes effect when the pool is created.
    pub fn set_worker_python(&self, python: &str) {
        *self.worker_python.lock().expect("worker python") = Some(python.to_string());
    }

    /// Queue a hook result from an engine task for the host's output thread.
    pub fn report_hook_result(&self, result: meerschaum_core::SuccessTuple) {
        self.hook_results.lock().expect("hook results").push(result);
    }

    /// Drain hook results in the order the engine reported them.
    pub fn take_hook_results(&self) -> Vec<meerschaum_core::SuccessTuple> {
        std::mem::take(&mut *self.hook_results.lock().expect("hook results"))
    }

    /// The single lazily-created Python pool shared by this process session.
    pub fn pool(&self, config: &Config) -> WorkerPool {
        self.pool
            .lock()
            .expect("worker pool")
            .get_or_insert_with(|| {
                let mut settings = PoolConfig::from_config(config);
                if let Some(env) = &*self.worker_environment.lock().expect("worker environment") {
                    // Workers normally inherit the host. A compose session instead supplies
                    // a complete project view, including removal of host-only variables.
                    settings.worker.env_remove = std::env::vars_os()
                        .filter_map(|(key, _)| key.into_string().ok())
                        .filter_map(|key| (!env.iter().any(|(k, _)| k == &key)).then_some(key))
                        .collect();
                    settings.worker.python = env
                        .iter()
                        .find(|(k, v)| k == "MRSM_PYTHON" && !v.trim().is_empty())
                        .map(|(_, python)| python.into())
                        .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into());
                    settings.worker.env.extend(env.iter().cloned());
                }
                if self
                    .worker_environment
                    .lock()
                    .expect("worker environment")
                    .is_none()
                    && !std::env::var("MRSM_PYTHON").is_ok_and(|v| !v.trim().is_empty())
                    && let Some(python) = &*self.worker_python.lock().expect("worker python")
                {
                    settings.worker.python = python.into();
                }
                settings.worker.inherit_stdin = std::io::stdin().is_terminal();
                WorkerPool::new(settings)
            })
            .clone()
    }

    /// The fingerprinted manifest (Python plugins merged with native plugins), validated at most
    /// once per [`Session::invalidate_manifest`]. Starts no Python for an empty plugin tree or
    /// an unchanged cache.
    pub fn manifest(&self, config: &Config) -> Result<Arc<Manifest>, ActionError> {
        let mut state = self.manifests.lock().expect("manifests");
        self.validate_manifests(&mut state, config, false)?;
        Ok(state.merged.clone().expect("validated manifest"))
    }

    /// Re-arm the next [`Session::manifest`] call to validate the plugin trees again (one
    /// fingerprint walk). Cheap; the shell calls it after every command.
    pub fn invalidate_manifest(&self) {
        self.manifests.lock().expect("manifests").validated = false;
    }

    /// Validate the plugin trees now, rebuilding the manifest if a plugin changed.
    pub fn refresh_manifest(&self, config: &Config) -> Result<Arc<Manifest>, ActionError> {
        self.invalidate_manifest();
        self.manifest(config)
    }

    /// `reload`: rebuild the Python manifest unconditionally in a fresh worker, re-open the
    /// native plugins and recycle the worker pool.
    pub fn reload_plugins(&self, config: &Config) -> Result<Arc<Manifest>, ActionError> {
        let mut state = self.manifests.lock().expect("manifests");
        self.validate_manifests(&mut state, config, true)?;
        Ok(state.merged.clone().expect("validated manifest"))
    }

    fn validate_manifests(
        &self,
        state: &mut ManifestState,
        config: &Config,
        force: bool,
    ) -> Result<(), ActionError> {
        if !force && state.validated && state.merged.is_some() {
            return Ok(());
        }
        if !self.worker_enabled() {
            let empty = Arc::new(Manifest::default());
            state.python = Some(empty.clone());
            state.natives = Some(Arc::new(NativePlugins::empty()));
            state.merged = Some(empty);
            state.validated = true;
            return Ok(());
        }
        let paths = config.paths();
        let pool = self.pool(config);
        let manifest_file = manifest_path(&paths.root_dir);
        let previous = state.python.clone();
        let mut python_changed = state.merged.is_none();
        // v4 finds pip-installed plugins whatever the plugins directory holds: a cached probe of
        // the interpreter (a few `stat` calls) says whether any exist, so an empty tree with no
        // entry point still starts no Python.
        let entry_points = probe_entry_points(&pool, &manifest_file, force);
        let python = match previous.clone() {
            Some(cached)
                if !force && !stale_plugins_for(&cached, &paths.plugins_dirs, &entry_points).0 =>
            {
                cached
            }
            _ => {
                let (manifest, source) = if force {
                    self.block_on(rebuild_manifest_for(
                        &pool,
                        &manifest_file,
                        &paths.plugins_dirs,
                        &entry_points,
                    ))
                    .map(|manifest| (manifest, ManifestSource::Built))
                } else {
                    self.block_on(ensure_manifest_for(
                        &pool,
                        &manifest_file,
                        &paths.plugins_dirs,
                        &entry_points,
                    ))
                }
                .map_err(|e| ActionError::new("WorkerError", e.to_string()))?;
                // A cache another process rebuilt: this process's workers still hold the old
                // plugin modules, so they are recycled too (a rebuild already did).
                if source == ManifestSource::Cache
                    && previous.as_ref().is_some_and(|p| **p != manifest)
                {
                    self.block_on(pool.recycle());
                }
                python_changed |= previous.as_ref().is_none_or(|p| **p != manifest);
                Arc::new(manifest)
            }
        };
        python_changed |= force;
        let signature = native_signature(&paths.plugins_dirs);
        if force || state.natives.is_none() || state.native_signature.as_ref() != Some(&signature) {
            let (natives, error) = match NativePlugins::open(
                &paths.plugins_dirs,
                &crate::native_plugins::cache_path(&paths.root_dir),
            ) {
                Ok(natives) => (Arc::new(natives), None),
                // A broken native plugin must not disable the Python plugins.
                Err(error) => (Arc::new(NativePlugins::empty()), Some(error.to_string())),
            };
            state.natives = Some(natives);
            state.native_signature = Some(signature);
            state.merged = None;
            state.native_error = error;
        }
        if python_changed || state.merged.is_none() {
            let natives = state.natives.clone().expect("natives");
            let mut merged = crate::native_plugins::merge(&python, &natives);
            if let Some(error) = &state.native_error {
                merged
                    .load_errors
                    .insert("native plugins".into(), error.clone());
            }
            state.merged = Some(Arc::new(merged));
        }
        state.python = Some(python);
        state.validated = true;
        Ok(())
    }

    /// The Python-only manifest (what the Python worker adapters may claim).
    pub fn python_manifest(&self, config: &Config) -> Result<Arc<Manifest>, ActionError> {
        let mut state = self.manifests.lock().expect("manifests");
        // Engine tasks call this per sync: whatever the session holds is current enough.
        if let Some(python) = state.python.clone() {
            return Ok(python);
        }
        self.validate_manifests(&mut state, config, false)?;
        Ok(state.python.clone().expect("validated manifest"))
    }

    /// The native plugins (discovered with the manifest).
    pub fn native_plugins(&self, config: &Config) -> Result<Arc<NativePlugins>, ActionError> {
        let mut state = self.manifests.lock().expect("manifests");
        if let Some(natives) = state.natives.clone() {
            return Ok(natives);
        }
        self.validate_manifests(&mut state, config, false)?;
        Ok(state
            .natives
            .clone()
            .unwrap_or_else(|| Arc::new(NativePlugins::empty())))
    }

    /// Shut down pooled children before dropping the Tokio runtime.
    pub fn shutdown(&self) {
        let pool = self.pool.lock().expect("worker pool").clone();
        if let Some(pool) = pool {
            self.block_on(pool.shutdown());
        }
    }

    /// Run a future to completion.
    pub fn block_on<F: Future>(&self, fut: F) -> F::Output {
        match self.rt.as_ref() {
            SessionRuntime::Owned(rt) => rt.block_on(fut),
            SessionRuntime::Shared(handle) => handle.block_on(fut),
        }
    }

    /// The runtime handle (owned or shared).
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.rt.handle().clone()
    }

    /// Spawn a background task on the runtime.
    pub fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.rt.handle().spawn(fut);
    }

    /// The labels of connectors built so far, by type (`show connectors`' "Active connectors").
    pub fn active(&self) -> Vec<(String, String)> {
        let mut keys: Vec<String> = self
            .instances
            .lock()
            .expect("instances")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys.into_iter()
            .filter_map(|k| {
                k.split_once(':')
                    .map(|(t, l)| (t.to_string(), l.to_string()))
            })
            .collect()
    }

    /// `parse_instance_keys(keys)` → the connector (built once per keys).
    pub fn instance(&self, keys: &str, config: &Config) -> Result<Instance, ActionError> {
        let (typ, label) = split_keys(keys);
        let full = format!("{typ}:{label}");
        if let Some(i) = self.instances.lock().expect("instances").get(&full) {
            return Ok(i.clone());
        }
        if self.registered_only.load(Ordering::SeqCst) {
            return Err(ActionError::new(
                "ConnectorNotRegistered",
                format!("No connector was registered for '{full}'."),
            ));
        }
        let inst = match typ.as_str() {
            "sql" => Instance::Sql(Arc::new(self.sql_connector(&label, config)?)),
            "api" => {
                let c = meerschaum_connector_api::APIConnector::from_config(config, &label)
                    .map_err(|e| ActionError::new("InvalidAttributesError", e.to_string()))?;
                Instance::Other(Arc::new(c))
            }
            "valkey" => {
                let attrs = meerschaum_connector_valkey::ValkeyConnector::attributes_from_config(
                    config, &label,
                );
                let c = self
                    .block_on(meerschaum_connector_valkey::ValkeyConnector::connect(
                        &label, attrs,
                    ))
                    .map_err(|e| ActionError::new("ConnectionError", e.to_string()))?;
                Instance::Other(Arc::new(c))
            }
            _ if !self.worker_enabled() => {
                return Err(ActionError::new(
                    "WorkerError",
                    format!("Cannot build '{full}' without the Python worker."),
                ));
            }
            _ => {
                let natives = self.native_plugins(config)?;
                let attributes = connector_attributes(config, &typ, &label);
                if let Some(connector) =
                    crate::native_plugins::instance_connector(&natives, keys, &attributes)
                {
                    let connector = connector.map_err(|e| ActionError::new("ImportError", e))?;
                    let inst = Instance::Other(connector);
                    self.instances
                        .lock()
                        .expect("instances")
                        .insert(full, inst.clone());
                    return Ok(inst);
                }
                let connector = self
                    .block_on(meerschaum_worker::PythonConnector::connect(
                        keys,
                        self.pool(config),
                    ))
                    .map_err(|e| ActionError::new("WorkerError", e.to_string()))?;
                if !connector.info().is_instance {
                    return Err(ActionError::new(
                        "Exception",
                        format!("'{keys}' is not an instance connector."),
                    ));
                }
                Instance::Other(Arc::new(connector))
            }
        };
        self.instances
            .lock()
            .expect("instances")
            .insert(full, inst.clone());
        Ok(inst)
    }

    /// A `sql:<label>` connector from the config registry.
    pub fn sql_connector(&self, label: &str, config: &Config) -> Result<SqlConnector, ActionError> {
        let mut attrs = connector_attributes(config, "sql", label);
        // PARITY: SQLite / GeoPackage connectors do not inherit `sql:default`, and
        // `{SQLITE_DB_PATH}` (or a missing database for `sql:local`) is the root's SQLite file
        // (`SQLConnector.__init__`, `create_engine`).
        let flavor = attrs.get("flavor").map(pyfmt::py_str).unwrap_or_default();
        if matches!(flavor.as_str(), "sqlite" | "geopackage") {
            attrs = config
                .get(&["meerschaum", "connectors", "sql", label])
                .map(|v| v.to_map())
                .unwrap_or_default();
            let db = attrs.get("database").map(pyfmt::py_str);
            let is_placeholder = db.as_deref() == Some("{SQLITE_DB_PATH}");
            if is_placeholder || (label == "local" && db.is_none()) {
                let path = config.paths().path("SQLITE_DB_PATH");
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                attrs.insert(
                    "database".into(),
                    Value::Str(path.to_string_lossy().into_owned()),
                );
            }
        }
        if attrs.is_empty() {
            // PARITY: `SQLConnector.__init__` raises when neither `flavor` nor `uri` is set.
            return Err(ActionError::new(
                "ValueError",
                format!("    Missing flavor. Provide flavor as a key for 'sql:{label}'."),
            ));
        }
        let ca = sql_attributes(label, &attrs)?;
        let spec = ConnectionSpec::from_attributes(&ca)
            .map_err(|e| ActionError::new("ValueError", e.to_string()))?;
        let settings = InstanceSettings::from_config(spec.flavor, config);
        let conn = self
            .block_on(SqlConnector::connect_with(label, spec, settings))
            .map_err(|e| ActionError::new("OperationalError", e.to_string()))?;
        // Cross-instance `{{ Pipe(..., instance='sql:other') }}` references resolve against the
        // configured connectors, not only `MRSM_SQL_<LABEL>` environment variables.
        conn.set_instance_provider(Arc::new(ConfiguredSqlProvider::new(config)));
        Ok(conn)
    }
}

/// `type:label` with the default label filled in (the session's map key).
/// What native plugin discovery sees (names, trees and skipped directories): when it is
/// unchanged the opened [`NativePlugins`] and the warnings printed for it are still current.
fn native_signature(plugins_dirs: &[std::path::PathBuf]) -> Vec<String> {
    let discovery = meerschaum_plugin::discovery::discover_report(plugins_dirs);
    discovery
        .candidates
        .iter()
        .map(|c| format!("{}:{}:{}", c.name, c.version, c.fingerprint))
        .chain(discovery.skipped.iter().map(|s| s.message()))
        .collect()
}

fn full_keys(keys: &str) -> String {
    let (typ, label) = split_keys(keys);
    format!("{typ}:{label}")
}

/// Split `type:label` (the label defaults to `main`).
pub fn split_keys(keys: &str) -> (String, String) {
    let mut parts = keys.split(':');
    let typ = parts.next().unwrap_or_default().to_string();
    let label = parts
        .next()
        .map(str::to_string)
        .unwrap_or_else(|| static_config().connectors.default_label.clone());
    (typ, label)
}

/// `meerschaum:connectors:<type>:default` patched with the label's section.
pub fn connector_attributes(config: &Config, typ: &str, label: &str) -> Map {
    let specific = config
        .get(&["meerschaum", "connectors", typ, label])
        .map(|v| v.to_map())
        .unwrap_or_default();
    if specific.is_empty() {
        return Map::new();
    }
    let base = config
        .get(&["meerschaum", "connectors", typ, "default"])
        .map(|v| v.to_map())
        .unwrap_or_default();
    apply_patch_maps(&base, &specific)
}

fn sql_attributes(label: &str, attrs: &Map) -> Result<ConnectorAttributes, ActionError> {
    let mut ca = ConnectorAttributes {
        label: Some(label.to_string()),
        ..Default::default()
    };
    let s = |v: &Value| pyfmt::py_str(v);
    for (k, v) in attrs {
        if v.is_null() {
            continue;
        }
        match k.as_str() {
            "flavor" => {
                ca.flavor = Some(
                    s(v).parse::<Flavor>()
                        .map_err(|e| ActionError::new("ValueError", e.to_string()))?,
                )
            }
            "uri" => ca.uri = Some(s(v)),
            "host" => ca.host = Some(s(v)),
            "port" => ca.port = pyfmt::py_int(v).and_then(|p| u16::try_from(p).ok()),
            "username" => ca.username = Some(s(v)),
            "password" => ca.password = Some(s(v)),
            "database" => ca.database = Some(s(v)),
            "schema" => ca.schema = Some(s(v)),
            "options" => {
                ca.options = Some(match v {
                    Value::Map(m) => {
                        ConnectionOptions::Pairs(m.iter().map(|(k, v)| (k.clone(), s(v))).collect())
                    }
                    other => ConnectionOptions::Str(s(other)),
                })
            }
            "pool_size" => ca.pool_size = pyfmt::py_int(v).and_then(|p| u32::try_from(p).ok()),
            "max_overflow" => {
                ca.max_overflow = pyfmt::py_int(v).and_then(|p| u32::try_from(p).ok())
            }
            other => ca.extra.push((other.to_string(), s(v))),
        }
    }
    Ok(ca)
}

/// Builds `sql:<label>` connectors from the configured `meerschaum:connectors:sql` section
/// (falling back to `MRSM_SQL_<LABEL>` variables), lazily and once per label.
pub struct ConfiguredSqlProvider {
    sql: Map,
    sqlite_path: String,
    cache: Mutex<HashMap<String, Arc<SqlConnector>>>,
}

impl ConfiguredSqlProvider {
    /// A provider over the connectors in `config`.
    pub fn new(config: &Config) -> Self {
        Self {
            sql: config
                .get(&["meerschaum", "connectors", "sql"])
                .map(|v| v.to_map())
                .unwrap_or_default(),
            sqlite_path: config
                .paths()
                .path("SQLITE_DB_PATH")
                .to_string_lossy()
                .into_owned(),
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn build(&self, label: &str) -> Option<SqlConnector> {
        let section = |key: &str| self.sql.get(key).map(|v| v.to_map()).unwrap_or_default();
        let specific = section(label);
        if specific.is_empty() {
            return None;
        }
        let mut attrs = apply_patch_maps(&section("default"), &specific);
        let flavor = attrs.get("flavor").map(pyfmt::py_str).unwrap_or_default();
        if matches!(flavor.as_str(), "sqlite" | "geopackage") {
            attrs = specific;
            let db = attrs.get("database").map(pyfmt::py_str);
            if db.as_deref() == Some("{SQLITE_DB_PATH}") || (label == "local" && db.is_none()) {
                attrs.insert("database".into(), Value::Str(self.sqlite_path.clone()));
            }
        }
        let ca = sql_attributes(label, &attrs).ok()?;
        let spec = ConnectionSpec::from_attributes(&ca).ok()?;
        let settings = InstanceSettings::defaults(spec.flavor);
        SqlConnector::connect_lazy(label, spec, settings).ok()
    }
}

impl meerschaum_connector::InstanceProvider for ConfiguredSqlProvider {
    fn instance(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        let label = keys.strip_prefix("sql:")?;
        {
            let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(conn) = cache.get(label) {
                return Some(conn.clone() as Arc<dyn InstanceConnector>);
            }
        }
        match self.build(label) {
            Some(conn) => {
                let conn = Arc::new(conn);
                self.cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(label.to_string(), conn.clone());
                Some(conn as Arc<dyn InstanceConnector>)
            }
            None => meerschaum_connector_sql::instance::env_provider::EnvSqlProvider::shared()
                .instance(keys),
        }
    }
}

#[cfg(test)]
mod provider_tests {
    use super::*;
    use meerschaum_connector::InstanceProvider;
    use meerschaum_core::config::ConfigOptions;

    #[test]
    fn provider_resolves_configured_connectors() {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let config = Config::new(ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
            (
                "MRSM_PLUGINS_DIR".into(),
                plugins.path().display().to_string(),
            ),
        ]))
        .unwrap();
        let provider = ConfiguredSqlProvider::new(&config);
        // `sql:local` is a configured SQLite connector in the default config.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _guard = rt.enter();
        let conn = provider
            .instance("sql:local")
            .expect("configured connector");
        assert_eq!(conn.connector_keys(), "sql:local");
        assert!(provider.instance("sql:not_configured_anywhere").is_none());
        assert!(provider.instance("api:local").is_none());
    }
}

#[cfg(test)]
mod registration_tests {
    use super::*;
    use meerschaum_core::config::ConfigOptions;

    fn isolated_config() -> (tempfile::TempDir, tempfile::TempDir, Config) {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let config = Config::new(ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
            (
                "MRSM_PLUGINS_DIR".into(),
                plugins.path().display().to_string(),
            ),
        ]))
        .unwrap();
        (root, plugins, config)
    }

    fn adhoc_sqlite(session: &Session, dir: &std::path::Path, label: &str) -> Arc<SqlConnector> {
        let attrs = ConnectorAttributes {
            label: Some(label.into()),
            flavor: Some(Flavor::Sqlite),
            database: Some(dir.join(format!("{label}.db")).display().to_string()),
            ..Default::default()
        };
        let spec = ConnectionSpec::from_attributes(&attrs).unwrap();
        let settings = InstanceSettings::defaults(spec.flavor);
        let _guard = session.handle().enter();
        Arc::new(SqlConnector::connect_lazy(label, spec, settings).unwrap())
    }

    #[test]
    fn registered_connector_wins_over_config() {
        let (root, _plugins, config) = isolated_config();
        let session = Session::new();
        // Not in any config: a lookup fails until the host registers it.
        assert!(session.instance("sql:w9q_adhoc", &config).is_err());
        let conn = adhoc_sqlite(&session, root.path(), "w9q_adhoc");
        assert!(
            session
                .register_sql("sql:w9q_adhoc", conn.clone())
                .is_none()
        );
        assert!(session.is_registered("sql:w9q_adhoc"));
        let inst = session.instance("sql:w9q_adhoc", &config).unwrap();
        assert!(Arc::ptr_eq(inst.sql().unwrap(), &conn));
        assert_eq!(inst.keys(), "sql:w9q_adhoc");

        // A registered connector replaces one built from config for the same keys.
        let built = session.instance("sql:local", &config).unwrap();
        assert!(!session.is_registered("sql:local"));
        let local = adhoc_sqlite(&session, root.path(), "local");
        let replaced = session.register_sql("sql:local", local.clone()).unwrap();
        assert!(Arc::ptr_eq(replaced.sql().unwrap(), built.sql().unwrap()));
        let inst = session.instance("sql:local", &config).unwrap();
        assert!(Arc::ptr_eq(inst.sql().unwrap(), &local));

        // A bare type means the default label.
        let main = adhoc_sqlite(&session, root.path(), "main");
        session.register_sql("sql", main.clone());
        let inst = session.instance("sql:main", &config).unwrap();
        assert!(Arc::ptr_eq(inst.sql().unwrap(), &main));

        // Unregistering falls back to the config path again.
        session.unregister("sql:w9q_adhoc");
        assert!(!session.is_registered("sql:w9q_adhoc"));
        assert!(session.instance("sql:w9q_adhoc", &config).is_err());
    }

    #[test]
    fn registered_only_never_reads_config() {
        let (root, _plugins, config) = isolated_config();
        let session = Session::new();
        session.set_registered_only(true);
        session.set_python_worker(false);
        let err = session.instance("sql:local", &config).err().unwrap();
        assert_eq!(err.class, "ConnectorNotRegistered");
        assert!(session.instance("plugin:stress", &config).is_err());
        let conn = adhoc_sqlite(&session, root.path(), "w9q_only");
        session.register_sql("sql:w9q_only", conn.clone());
        let inst = session.instance("sql:w9q_only", &config).unwrap();
        assert!(Arc::ptr_eq(inst.sql().unwrap(), &conn));
        // No worker: empty manifests, no child process.
        let manifest = session.manifest(&config).unwrap();
        assert!(manifest.plugins.is_empty());
        assert!(session.python_manifest(&config).unwrap().plugins.is_empty());
        assert!(session.pool.lock().unwrap().is_none());
    }

    #[test]
    fn shared_runtime_runs_futures() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let session = Session::with_runtime(rt.handle().clone());
        assert_eq!(session.block_on(async { 41 + 1 }), 42);
        let (root, _plugins, config) = isolated_config();
        let conn = adhoc_sqlite(&session, root.path(), "w9q_shared");
        session.register_sql("sql:w9q_shared", conn);
        let inst = session.instance("sql:w9q_shared", &config).unwrap();
        let exists = session.block_on(async {
            let pipe = meerschaum_core::Pipe::new("a", "b").unwrap();
            inst.conn().pipe_exists(&pipe).await
        });
        assert!(matches!(exists, Ok(false)));
    }
}
