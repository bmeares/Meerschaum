//! The Python façade's engine context for one-pipe syncs (`Pipe.sync()`, design §4.4, D12, D14).
//!
//! `_core.sync_pipe` needs a [`ConnectorRegistry`]: the pipe's instance, the source connector a
//! no-data sync fetches from, the connectors `{{ Pipe(..., instance=...) }}` templates and
//! `reference`s name, and, for Python plugins and `@make_connector` connectors, the worker
//! adapter. [`build_registry`] builds exactly that, and nothing the call does not reach:
//!
//! - **Native connectors** (`sql:`, `api:`, `valkey:`) come first. The `_core.SqlConnector`s the
//!   façade already built are used as given (their pools are shared with the rest of the
//!   process); an `api:` / `valkey:` connector the Python process defined ad hoc is rebuilt from
//!   its attributes; any other key is resolved from the config registry the way the CLI does
//!   ([`Session::instance`]), lazily, when the flow first asks for it.
//! - **Python connectors** (`plugin:` and custom `@make_connector` types) go through the worker
//!   adapter ([`PythonRegistry`], wrapped in the native-plugin registry), with the process's
//!   worker session: the pool starts on the first call that needs a Python worker, runs
//!   `MRSM_PYTHON` (the façade defaults it to its own interpreter), and is told about the
//!   connectors the Python process defined ad hoc so the keys it sends resolve there too
//!   ([`meerschaum_worker::WorkerPool::define_connector`]). A sync whose source and instance are
//!   all native never starts Python.
//! - The same-instance `sql:` sync runs in place (`INSERT ... SELECT`, v4's
//!   `sync_pipe_inplace`), as v4's `Pipe.sync()` does when `system:experimental:inplace_sync`
//!   is on and `_inplace` was not turned off.
//!
//! The config the flow reads is the files under `MRSM_ROOT_DIR` overlaid with the Python
//! process's in-memory `meerschaum.config.config` ([`merged_config`]), never written back.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use meerschaum_cli::native::{Instance, Session};
use meerschaum_connector::{Connector, InstanceConnector};
use meerschaum_core::config::Config;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_engine::python::PythonRegistry;
use meerschaum_engine::sync::{DeltaObserver, SyncArgs, SyncBegin, SyncEnv};
use meerschaum_engine::{ConnectorRegistry, EngineConfig, PluginSync};
use meerschaum_sql::SqlConnector;
use pyo3::prelude::*;

/// The connector types the Rust crates implement themselves (no Python worker needed).
pub const NATIVE_TYPES: [&str; 3] = ["sql", "api", "valkey"];

/// `type:label` with the default label filled in.
pub fn full_keys(keys: &str) -> String {
    let (typ, label) = meerschaum_cli::native::session::split_keys(keys);
    format!("{typ}:{label}")
}

/// Whether the keys name a connector the Rust crates implement.
pub fn is_native(keys: &str) -> bool {
    keys.split(':')
        .next()
        .is_some_and(|t| NATIVE_TYPES.contains(&t))
}

/// The config of the Python process: the files under `MRSM_ROOT_DIR` overlaid with the
/// in-memory `meerschaum.config.config` (runtime patches are honored). Never writes files.
pub fn merged_config(py: Python<'_>) -> PyResult<Config> {
    crate::config_api::live_config(py)
}

/// The façade's worker session: one per process (v4 job daemons fork; a child builds its own) on
/// the extension's Tokio runtime, which the `_core.SqlConnector` pools were created on. It may
/// read the config registry and start the Python worker, unlike the registered-only session of
/// [`crate::verify_api::facade_session`].
pub fn worker_session() -> Session {
    static SESSION: Mutex<Option<(u32, Session)>> = Mutex::new(None);
    let pid = std::process::id();
    let mut slot = SESSION.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((owner, session)) = slot.as_ref()
        && *owner == pid
    {
        return session.clone();
    }
    let session = Session::with_runtime(crate::runtime::runtime().handle().clone());
    *slot = Some((pid, session.clone()));
    session
}

/// Run a future on the extension's runtime with the GIL released, on a worker thread of the
/// runtime (so `tokio::task::block_in_place`, which the registries use for lazy lookups, may
/// hand its thread off).
pub fn spawn_block_on<F>(py: Python<'_>, fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    py.allow_threads(|| {
        let handle = crate::runtime::runtime().handle().clone();
        let task = handle.spawn(fut);
        match handle.block_on(task) {
            Ok(out) => out,
            Err(e) => std::panic::resume_unwind(e.into_panic()),
        }
    })
}

/// What the façade tells the registry about the connectors of one call.
#[derive(Default)]
pub struct RegistryInput {
    /// The Rust SQL connectors the façade built (the instance, a SQL source), by keys.
    pub sql: Vec<(String, Arc<SqlConnector>)>,
    /// Attributes of connectors the Python process defined ad hoc, by keys (`api`, `valkey`,
    /// custom types; passed to the worker too).
    pub attributes: Vec<(String, Map)>,
    /// The instance keys of the pipe.
    pub instance_keys: String,
    /// The connector keys of the pipe (the source).
    pub connector_keys: String,
    /// Sync the same-instance `sql:` pipe in place.
    pub inplace: bool,
}

/// The native registry: the connectors of one call, built on demand.
struct FacadeNative {
    session: Session,
    /// The merged config, read the first time a connector has to be built from it (a call
    /// whose connectors the façade passed never reads it).
    config: std::sync::OnceLock<Arc<Config>>,
    instances: Mutex<HashMap<String, Arc<dyn InstanceConnector>>>,
    sql: Mutex<HashMap<String, Arc<SqlConnector>>>,
    attributes: HashMap<String, Map>,
    inplace: Option<(String, Arc<SqlConnector>)>,
}

impl FacadeNative {
    fn config(&self) -> Result<&Config, String> {
        // Set by `build_registry` from the snapshot taken with the interpreter lock held.
        self.config
            .get()
            .map(|c| c.as_ref())
            .ok_or_else(|| "The config snapshot of this call is missing.".to_string())
    }

    /// The instance connector for `keys`, built from the façade's connectors, its ad hoc
    /// attributes or the config registry.
    fn resolve(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        let full = full_keys(keys);
        if let Some(found) = self
            .instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&full)
        {
            return Ok(found.clone());
        }
        let (typ, label) = meerschaum_cli::native::session::split_keys(&full);
        let built: Arc<dyn InstanceConnector> = match typ.as_str() {
            "sql" => {
                let config = self.config()?;
                let conn = self
                    .in_blocking(|| self.session.instance(&full, config).map_err(|e| e.message))?;
                match conn {
                    Instance::Sql(sql) => {
                        self.sql
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(full.clone(), sql.clone());
                        sql
                    }
                    Instance::Other(other) => other,
                }
            }
            "api" => {
                let connector = match self.attributes.get(&full) {
                    Some(attributes) => {
                        meerschaum_api::APIConnector::new(Some(&label), attributes.clone())
                    }
                    None => meerschaum_api::APIConnector::from_config(self.config()?, &label),
                }
                .map_err(|e| e.to_string())?;
                Arc::new(connector)
            }
            "valkey" => {
                let attributes = match self.attributes.get(&full) {
                    Some(attributes) => attributes.clone(),
                    None => meerschaum_valkey::ValkeyConnector::attributes_from_config(
                        self.config()?,
                        &label,
                    ),
                };
                let connector = self
                    .in_blocking(|| {
                        self.session
                            .block_on(meerschaum_valkey::ValkeyConnector::connect(
                                &label, attributes,
                            ))
                            .map_err(|e| e.to_string())
                    })
                    .map_err(|e| e.to_string())?;
                Arc::new(connector)
            }
            _ => {
                return Err(format!("No instance connector is registered for '{full}'."));
            }
        };
        self.instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(full, built.clone());
        Ok(built)
    }

    /// Run a blocking lookup from inside the runtime.
    fn in_blocking<T, E: ToString>(&self, f: impl FnOnce() -> Result<T, E>) -> Result<T, String> {
        let run = || f().map_err(|e| e.to_string());
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(run)
            }
            _ => run(),
        }
    }
}

impl ConnectorRegistry for FacadeNative {
    fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        self.resolve(keys)
    }

    fn source(&self, keys: &str) -> Result<Option<Arc<dyn Connector>>, String> {
        if !keys.contains(':') {
            return Ok(None);
        }
        let conn: Arc<dyn Connector> = self.resolve(keys)?;
        Ok(Some(conn))
    }

    fn fetch_chunksize(&self, keys: &str) -> usize {
        self.sql
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&full_keys(keys))
            .map(|c| c.settings.chunksize.max(1) as usize)
            .unwrap_or(100_000)
    }

    fn plugin_sync(&self, keys: &str) -> Option<Arc<dyn PluginSync>> {
        let (home, sql) = self.inplace.as_ref()?;
        (full_keys(keys) == *home).then(|| Arc::new(Inplace(sql.clone())) as Arc<dyn PluginSync>)
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        if let Some(sql) = self
            .sql
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&full_keys(pipe.instance_keys()))
            .cloned()
            .or_else(|| {
                // A referenced pipe on another SQL instance (`{{ Pipe(..., instance=...) }}`).
                self.resolve(pipe.instance_keys()).ok()?;
                self.sql
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&full_keys(pipe.instance_keys()))
                    .cloned()
            })
        {
            let pipe = sql.sql_pipe(pipe).map_err(|e| e.to_string())?;
            return Ok(sql.pipe_table_name(&pipe));
        }
        pipe.target(None).map_err(|e| e.to_string())
    }
}

/// v4's same-instance sync (`SQLConnector.sync_pipe_inplace`): `INSERT ... SELECT` from the
/// source definition into the pipe's table. The message is v4's, without a chunk envelope.
struct Inplace(Arc<SqlConnector>);

#[meerschaum_connector::async_trait]
impl PluginSync for Inplace {
    async fn sync(&self, pipe: &Pipe, args: &SyncArgs) -> meerschaum_core::success::SyncResult {
        use meerschaum_sql::instance::fetch::metadef_bound;
        use meerschaum_sql::queries::metadef::Bound;
        let begin = match &args.begin {
            SyncBegin::Default => Bound::Default,
            SyncBegin::At(b) => metadef_bound(Some(b)),
            SyncBegin::Unbounded => Bound::None,
        };
        let result = self
            .0
            .sync_pipe_inplace(
                pipe,
                args.params.as_ref(),
                begin,
                metadef_bound(args.end.as_ref()),
                args.check_existing,
            )
            .await;
        meerschaum_engine::sync::result_from_message(result.success, result.message)
    }

    async fn sync_native(
        &self,
        pipe: &Pipe,
        args: &SyncArgs,
        _env: &SyncEnv<'_>,
        _observer: Option<&dyn DeltaObserver>,
    ) -> meerschaum_core::success::SyncResult {
        self.sync(pipe, args).await
    }
}

/// The registry of one call and, when the call read the config, its engine config.
pub struct Built {
    pub registry: Arc<dyn ConnectorRegistry>,
    pub config: Option<EngineConfig>,
}

/// Build the registry for one pipe's sync (module docs). `need_python` says whether the call can
/// reach a Python connector (the source or the instance is not native), which is what starts
/// the worker session's manifest and reads the config; otherwise nothing is read.
pub fn build_registry(
    py: Python<'_>,
    input: RegistryInput,
    need_python: bool,
) -> Result<Built, String> {
    let session = worker_session();
    suggest_worker_python(py, &session);
    let mut sql = HashMap::new();
    let mut instances: HashMap<String, Arc<dyn InstanceConnector>> = HashMap::new();
    for (keys, conn) in &input.sql {
        let full = full_keys(keys);
        sql.insert(full.clone(), conn.clone());
        instances.insert(full, conn.clone());
    }
    let inplace = (input.inplace
        && full_keys(&input.connector_keys) == full_keys(&input.instance_keys))
    .then(|| {
        sql.get(&full_keys(&input.instance_keys))
            .map(|c| (full_keys(&input.instance_keys), c.clone()))
    })
    .flatten();
    let native = Arc::new(FacadeNative {
        session: session.clone(),
        config: std::sync::OnceLock::new(),
        instances: Mutex::new(instances),
        sql: Mutex::new(sql),
        attributes: input
            .attributes
            .iter()
            .map(|(k, v)| (full_keys(k), v.clone()))
            .collect(),
        inplace,
    });
    // The live config of the process, as of this call (`set_config()`, `MRSM_CONFIG` patches and
    // `write_config()` are seen by the next sync): the engine's defaults, backtrack and filter
    // limits come from it, not from what the connectors captured when they were built.
    let config = publish_live_config(py).map_err(|e| e.to_string())?;
    let _ = native.config.set(config.clone());
    let mut engine_config = EngineConfig::from_config(&config, Some(&config.paths().root_dir));
    engine_config.home_instance = full_keys(&input.instance_keys);
    if !need_python {
        return Ok(Built {
            registry: native,
            config: Some(engine_config),
        });
    }
    let pool = session.pool(&config);
    for (keys, attributes) in &input.attributes {
        pool.define_connector(&full_keys(keys), attributes);
    }
    // Building the manifest may start the Python worker, whose messages reach the `tracing`
    // sink (`logging.rs`), which takes the interpreter lock on a runtime thread: never block on
    // the runtime while holding the lock (a fresh root deadlocked here).
    let manifest = py
        .allow_threads(|| session.python_manifest(&config))
        .map_err(|e| e.message.clone())?;
    let python = PythonRegistry::new(native, pool, manifest, engine_config.clone());
    let natives = py
        .allow_threads(|| session.native_plugins(&config))
        .map_err(|e| e.message)?;
    let registry = meerschaum_cli::native_plugins::NativeRegistry::new(natives, &config, python);
    Ok(Built {
        registry,
        config: Some(engine_config),
    })
}

/// Tell the session which interpreter its workers run with when `MRSM_PYTHON` is not set: the
/// one running this extension (`sys.executable`). The process environment is not touched.
pub fn suggest_worker_python(py: Python<'_>, session: &Session) {
    if let Ok(exe) = py
        .import("sys")
        .and_then(|sys| sys.getattr("executable"))
        .and_then(|e| e.extract::<String>())
        && !exe.is_empty()
    {
        session.set_worker_python(&exe);
    }
}

/// Snapshot the live config tree of this process (the dictionary behind
/// `meerschaum.config.config`) *now, with the interpreter lock held*, and publish it as what
/// `Config::current()` returns inside every native sync, verify and engine run that follows:
/// `MRSM{}` symlinks and every config read of the engine see `set_config()`, `MRSM_CONFIG`
/// patches and `write_config()` made in the process. Call it before releasing the lock; no
/// runtime thread ever calls back into Python for the config.
pub fn publish_live_config(py: Python<'_>) -> PyResult<Arc<Config>> {
    let config = Arc::new(merged_config(py)?);
    meerschaum_core::config::set_live_snapshot(config.clone());
    Ok(config)
}

/// The connectors `{{ Pipe(..., instance=...) }}` templates and `reference`s resolve against
/// during one call: the SQL connectors the façade holds (by keys), then the config registry
/// (read lazily, only when a template names an instance nobody passed), then `MRSM_SQL_*`.
struct CallProvider {
    sql: HashMap<String, Arc<SqlConnector>>,
    configured: std::sync::OnceLock<Option<meerschaum_cli::native::session::ConfiguredSqlProvider>>,
}

impl meerschaum_connector::InstanceProvider for CallProvider {
    fn instance(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        if let Some(found) = self.sql.get(&full_keys(keys)) {
            return Some(found.clone() as Arc<dyn InstanceConnector>);
        }
        let configured = self.configured.get_or_init(|| {
            Config::current()
                .ok()
                .map(|config| meerschaum_cli::native::session::ConfiguredSqlProvider::new(&config))
        });
        match configured {
            Some(provider) => provider.instance(keys),
            None => meerschaum_sql::instance::env_provider::EnvSqlProvider::shared().instance(keys),
        }
    }
}

/// Make every SQL connector of a call resolve cross-instance references against the others (a
/// source connector resolves the templates of the pipes it fetches for; without this, a pipe on
/// an ad hoc instance would be unreachable from the source).
pub fn install_instance_provider(sql: &[(String, Arc<SqlConnector>)]) {
    if sql.is_empty() {
        return;
    }
    let map: HashMap<String, Arc<SqlConnector>> = sql
        .iter()
        .map(|(keys, conn)| (full_keys(keys), conn.clone()))
        .collect();
    let provider: Arc<dyn meerschaum_connector::InstanceProvider> = Arc::new(CallProvider {
        sql: map,
        configured: std::sync::OnceLock::new(),
    });
    for (_, conn) in sql {
        conn.set_instance_provider(provider.clone());
    }
}

/// Make the connectors the Python process defined ad hoc known to `session` (and, when a worker
/// can be reached, to its pool): `api:` and `valkey:` instances are built from their attributes
/// and registered; every other type is only sent to the worker. SQL connectors are registered by
/// the caller (they are `_core.SqlConnector`s the façade already holds).
pub fn register_adhoc(
    py: Python<'_>,
    session: &Session,
    config: &Config,
    attributes: &[(String, Map)],
    define_for_workers: bool,
) -> Result<(), String> {
    let pool = define_for_workers.then(|| session.pool(config));
    for (keys, attrs) in attributes {
        let full = full_keys(keys);
        if let Some(pool) = &pool {
            pool.define_connector(&full, attrs);
        }
        if session.is_registered(&full) {
            continue;
        }
        let (typ, label) = meerschaum_cli::native::session::split_keys(&full);
        match typ.as_str() {
            "api" => {
                let connector = meerschaum_api::APIConnector::new(Some(&label), attrs.clone())
                    .map_err(|e| e.to_string())?;
                session.register_instance(&full, Instance::Other(Arc::new(connector)));
            }
            "valkey" => {
                let connector = py
                    .allow_threads(|| {
                        session.block_on(meerschaum_valkey::ValkeyConnector::connect(
                            &label,
                            attrs.clone(),
                        ))
                    })
                    .map_err(|e| e.to_string())?;
                session.register_instance(&full, Instance::Other(Arc::new(connector)));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Python plugin keyword arguments as the map the worker receives: callables and values that
/// are not data cannot cross the process boundary and are skipped (`None` and numbers, text,
/// lists and dictionaries of those cross).
pub fn is_forwardable(value: &Value) -> bool {
    match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::Str(_) => true,
        Value::List(items) => items.iter().all(is_forwardable),
        Value::Map(m) => m.values().all(is_forwardable),
        #[allow(unreachable_patterns)]
        _ => false,
    }
}
