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
//! validates the plugin trees (one fingerprint walk, [`stale_plugins`](meerschaum_worker::manifest::stale_plugins)) and rebuilds the Python
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

use meerschaum_worker::manifest::Manifest;
use meerschaum_worker::{TeamPools, WorkerPool};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::config::Config;
use meerschaum_core::value::Map;
use meerschaum_plugin::NativePlugins;
use meerschaum_plugin::source::compiler::CompilerPolicy;

use super::connectors;
use crate::builtin::ActionError;

mod instances;
mod teams;
mod worker;

pub use instances::{ConfiguredSqlProvider, connector_attributes, split_keys, sql_spec};

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
    /// Trusted host attachment; frozen before any manifest/discovery, shared by all children.
    source_compiler: Arc<OnceLock<CompilerPolicy>>,
    /// The isolated teams' pools and sessions of this process, shared by clones.
    teams: Arc<TeamSessions>,
    /// The team this session runs for (a team's session, [`Session::for_team`]).
    team: Option<Arc<TeamBinding>>,
}

/// The isolated teams' sessions of a process: one per team, rebuilt when the team's
/// configuration changes, with the pools under one limit.
#[derive(Default)]
struct TeamSessions {
    pools: std::sync::OnceLock<TeamPools>,
    sessions: Mutex<HashMap<String, (String, Session)>>,
}

/// What a team's session runs for.
pub struct TeamBinding {
    /// The team.
    pub scope: meerschaum_core::team_scope::TeamScope,
    /// The team's configuration (what its workers are started with), as a registry.
    pub config: Arc<Config>,
    /// The fingerprint of that configuration.
    pub fingerprint: String,
    /// The team's files (`team_scopes/<key>/`).
    pub files: meerschaum_core::team_scope::ScopeFiles,
    /// The environment of the team's processes.
    pub env: Vec<(String, String)>,
    /// The team's configuration, loaded once (every key), for [`TeamBinding::new_config`].
    snapshot: Map,
}

impl TeamBinding {
    /// A fresh registry of the team's configuration (what an action run for the team reads),
    /// from the snapshot loaded when the team's session was built (no file is read again).
    pub fn new_config(&self) -> Result<Config, ActionError> {
        // The snapshot already holds the team's configuration file: without the patch
        // variables, no lookup replays that patch to learn where a value came from.
        let patch_variables: Vec<String> = meerschaum_core::config::environment::patch_variables()
            .into_iter()
            .flat_map(|(var, file_var)| [var, file_var])
            .collect();
        let env: Vec<(String, String)> = self
            .env
            .iter()
            .filter(|(k, _)| !patch_variables.contains(k))
            .cloned()
            .collect();
        let config = Config::new(meerschaum_core::config::ConfigOptions::with_env(env))
            .map_err(|e| ActionError::new("ConfigError", e.to_string()))?;
        config.set_config(self.snapshot.clone());
        Ok(config)
    }
}

/// How [`Session::for_team`] found the team's session.
pub use meerschaum_worker::team::Lookup as TeamLookup;

/// An in-flight background manifest refresh started for an interactive shell/webterm so the
/// prompt opens immediately on cached metadata while stale plugins rebuild quietly off-thread.
struct BackgroundManifestRefresh {
    done: Condvar,
    outcome: Mutex<Option<Result<(), (String, String)>>>,
}

impl BackgroundManifestRefresh {
    fn new() -> Self {
        Self {
            done: Condvar::new(),
            outcome: Mutex::new(None),
        }
    }

    fn finish(&self, outcome: Result<(), ActionError>) {
        let stored = outcome.map_err(|e| (e.class, e.message));
        let mut guard = self.outcome.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(stored);
        self.done.notify_all();
    }

    fn wait(&self) -> Result<(), ActionError> {
        let mut guard = self.outcome.lock().unwrap_or_else(|e| e.into_inner());
        while guard.is_none() {
            guard = self.done.wait(guard).unwrap_or_else(|e| e.into_inner());
        }
        match guard.as_ref().expect("outcome populated") {
            Ok(()) => Ok(()),
            Err((class, message)) => Err(ActionError::new(class, message.clone())),
        }
    }
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
    /// Read-only mode (`doctor`): the manifests are whatever the last command cached on disk,
    /// never validated, rebuilt or imported.
    cached_only: bool,
    /// Bumped when `ManifestState` is reset or invalidated so stale background refreshes drop
    /// their results cleanly.
    generation: u64,
    /// Active background manifest refresh started by [`Session::warm_manifest_for_shell`].
    refreshing: Option<Arc<BackgroundManifestRefresh>>,
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

/// How long an interrupted action waits for the leases of its dropped engine tasks to be
/// handed back.
const LEASE_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

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
        let source_compiler = Arc::new(OnceLock::new());
        if let Some(policy) = crate::team::sandbox::source_compiler_policy() {
            let _ = source_compiler.set(policy);
        }
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
            source_compiler,
            teams: Arc::new(TeamSessions::default()),
            team: None,
        }
    }

    /// Shut down pooled children before dropping the Tokio runtime.
    pub fn shutdown(&self) {
        let pool = self.pool.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(pool) = pool {
            self.block_on(pool.shutdown());
        }
        if self.team.is_none()
            && let Some(pools) = self.teams.pools.get().cloned()
        {
            self.block_on(pools.shutdown());
        }
    }

    /// Run a future to completion.
    ///
    /// While the `mrsm` program runs an action, Ctrl-C cancels it: the future is dropped at its
    /// next await point and this call unwinds with [`crate::shell::interrupt::Interrupted`],
    /// which the dispatcher turns into `Interrupted.` (see `shell::interrupt`). Hosts which own
    /// their signals (the Python extension's `entry`, servers, jobs) never see that.
    pub fn block_on<F: Future>(&self, fut: F) -> F::Output {
        if !crate::shell::interrupt::cancellable() {
            return self.block_on_uninterruptible(fut);
        }
        match self.block_on_uninterruptible(crate::shell::interrupt::interruptible(fut)) {
            Ok(output) => output,
            Err(interrupted) => {
                // The cancelled action's engine tasks were dropped mid-sync: let them hand
                // their pipe leases back before the process (or the shell's next action) goes
                // on, so a rerun does not wait for the leases to expire. Returns at once when
                // none is held.
                self.block_on_uninterruptible(meerschaum_engine::drain_lease_releases(
                    LEASE_DRAIN_TIMEOUT,
                ));
                std::panic::resume_unwind(Box::new(interrupted))
            }
        }
    }

    /// [`Session::block_on`] without the Ctrl-C cancellation (clean-up that must finish).
    pub fn block_on_uninterruptible<F: Future>(&self, fut: F) -> F::Output {
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
            .unwrap_or_else(|e| e.into_inner())
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
    use meerschaum_connector_sql::flavor::Flavor;
    use meerschaum_connector_sql::instance::InstanceSettings;
    use meerschaum_connector_sql::uri::{ConnectionSpec, ConnectorAttributes};
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
    fn source_compiler_attachment_is_shared_and_frozen_before_manifest_initialization() {
        let session = Session::new();
        let cloned = session.clone();
        session.set_source_compiler(None).unwrap();
        assert!(cloned.set_source_compiler(None).is_err());
        assert!(matches!(
            cloned.source_compiler.get(),
            Some(CompilerPolicy::Confined(None))
        ));
        let (_root, _plugins, config) = isolated_config();
        session.set_python_worker(false);
        session.manifest(&config).unwrap();
        session.set_cached_manifest_only(false);
        assert!(session.set_source_compiler(None).is_err());

        let ambient = Session::new();
        ambient.set_python_worker(false);
        ambient.manifest(&config).unwrap();
        assert!(matches!(
            ambient.source_compiler.get(),
            Some(CompilerPolicy::Ambient)
        ));
        ambient.set_cached_manifest_only(false);
        assert!(
            ambient.set_source_compiler(None).is_err(),
            "Resetting manifest state must not reopen the compiler attachment"
        );
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
