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

use meerschaum_worker::manifest::{
    Manifest, ManifestSource, ensure_manifest_for, ensure_manifest_for_with_options, manifest_path,
    probe_entry_points, read_cached_manifest_for, read_fresh_manifest_for, rebuild_manifest_for,
    stale_plugins_for,
};
use meerschaum_worker::{PoolConfig, PoolScope, TeamPools, WorkerPool};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::instance::InstanceSettings;
use meerschaum_connector_sql::uri::{ConnectionSpec, ConnectorAttributes, InvalidPort};
use meerschaum_core::config::Config;
use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{pyfmt, static_config};
use meerschaum_plugin::NativePlugins;
use meerschaum_plugin::source::compiler::{CompilerPolicy, SourceCompiler};

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
        if let Some(policy) = crate::team_sandbox::source_compiler_policy() {
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

    /// The team this session runs for (`None`: the server's own session).
    pub fn team(&self) -> Option<&Arc<TeamBinding>> {
        self.team.as_ref()
    }

    /// The isolated teams' pools of this process, created with the limit of `config`
    /// (`system.engine.python_team_workers`) on first use.
    pub fn team_pools(&self, config: &Config) -> TeamPools {
        self.teams
            .pools
            .get_or_init(|| TeamPools::new(meerschaum_worker::pool::team_workers_limit(config)))
            .clone()
    }

    /// The session of an isolated team: its own connectors and worker pool, built from the
    /// team's configuration only ([`meerschaum_core::team_scope::team_config`] of the server's
    /// `config`). Python code the session runs starts in workers that never receive the
    /// server's configuration (`MRSM_CONFIG`, `MRSM_PATCH`, connector variables, config files)
    /// nor any other server variable outside an allowlist
    /// ([`meerschaum_core::team_scope::team_env`]), and whose callbacks may only touch the team's instances (and, with `member`, pipes that
    /// member may change). Reused while the team's configuration is unchanged; a change (a
    /// connector granted or revoked, a rename, a deletion, a rotated password) retires the
    /// team's workers and builds a new session. Returns the session and how it was found.
    pub fn for_team(
        &self,
        scope: &meerschaum_core::team_scope::TeamScope,
        member: Option<meerschaum_core::access::Principal>,
        config: &Config,
    ) -> Result<(Session, TeamLookup), ActionError> {
        scope
            .check_host_execution(config)
            .map_err(|message| ActionError::new("RuntimeError", message))?;
        use meerschaum_core::team_scope as ts;
        let io = |e: std::io::Error| ActionError::new("OSError", e.to_string());
        // A connector another process registered (a team's database) since the last read.
        config.refresh_connectors();
        let team_map = ts::team_config(config, scope);
        let fingerprint = ts::fingerprint(scope, &team_map);
        let key = scope.key();
        {
            let sessions = self
                .teams
                .sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some((current, session)) = sessions.get(&key)
                && *current == fingerprint
            {
                return Ok((session.clone(), TeamLookup::Warm));
            }
        }
        let paths = config.paths();
        let files =
            ts::write_scope_files(&ts::scope_dir(&paths.root_dir, &key), &team_map).map_err(io)?;
        ts::write_scope_record(&paths.root_dir, scope).map_err(io)?;
        let mut base: Vec<(String, String)> = meerschaum_core::process_env::vars()
            .into_iter()
            .filter(|(k, _)| k != "MRSM_ROOT_DIR" && k != "MRSM_PLUGINS_DIR")
            .collect();
        base.push((
            "MRSM_ROOT_DIR".into(),
            paths.root_dir.to_string_lossy().into_owned(),
        ));
        if !paths.plugins_dirs.is_empty() {
            let dirs: Vec<String> = paths
                .plugins_dirs
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect();
            base.push((
                "MRSM_PLUGINS_DIR".into(),
                serde_json::to_string(&dirs).unwrap_or_default(),
            ));
        }
        // An allowlist (`meerschaum_core::team_scope::team_env`): nothing else of the server's
        // environment (cloud credentials, tokens) reaches the team's workers.
        let passthrough = ts::Passthrough::from_config(config);
        passthrough.warn_refused();
        let mut team_env = ts::team_env(&base, &[], &files, scope, &fingerprint, &passthrough);
        // The team's filesystem sandbox: each worker restricts itself to these rules before
        // it imports anything of the team's (`system.engine.python_team_sandbox`). In `require`
        // mode on a system without Landlock, nothing of the team's starts.
        let sandbox = ts::sandbox::prepare(
            config,
            scope,
            &files,
            None,
            &files.dir.join(ts::sandbox::SPEC_FILE),
        )
        .map_err(|e| ActionError::new("RuntimeError", e.to_string()))?;
        if let Some(prepared) = &sandbox {
            ts::sandbox::apply_env(&mut team_env, prepared);
        }
        let team_config = Arc::new(
            Config::new(meerschaum_core::config::ConfigOptions::with_env(
                team_env.clone(),
            ))
            .map_err(|e| ActionError::new("ConfigError", e.to_string()))?,
        );
        let configured: std::collections::BTreeSet<String> = config
            .get(&["meerschaum", "connectors"])
            .map(|v| v.to_map())
            .unwrap_or_default()
            .iter()
            .flat_map(|(typ, labels)| {
                labels
                    .to_map()
                    .into_iter()
                    .filter(|(label, attrs)| label != "default" && !attrs.is_null())
                    .map(|(label, _)| format!("{typ}:{label}"))
                    .collect::<Vec<_>>()
            })
            .collect();

        let mut child = Session {
            rt: self.rt.clone(),
            instances: Arc::new(Mutex::new(HashMap::new())),
            registered: Arc::new(Mutex::new(HashSet::new())),
            registered_only: Arc::new(AtomicBool::new(false)),
            python_worker: self.python_worker.clone(),
            pool: Arc::new(Mutex::new(None)),
            worker_environment: Arc::new(Mutex::new(None)),
            worker_python: self.worker_python.clone(),
            hook_results: Arc::new(Mutex::new(Vec::new())),
            manifests: self.manifests.clone(),
            source_compiler: self.source_compiler.clone(),
            teams: self.teams.clone(),
            team: None,
        };
        let guard: Option<Arc<dyn meerschaum_worker::callback::CallbackGuard>> =
            member.map(|member| {
                Arc::new(TeamPipeGuard {
                    instances: child.instances.clone(),
                    member,
                }) as Arc<dyn meerschaum_worker::callback::CallbackGuard>
            });
        let pool_scope = PoolScope {
            scope: Arc::new(scope.clone()),
            configured: Arc::new(configured),
            guard,
        };
        let worker_python = self
            .worker_python
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let (pool, lookup) = self.team_pools(config).get(&key, &fingerprint, |live| {
            let mut settings = PoolConfig::for_team(&team_config, pool_scope, live);
            // Workers start from an empty environment: the team's allowlisted one only.
            settings.worker.env_clear = true;
            settings.worker.env = team_env.clone();
            // Each worker proves it loaded this configuration before it runs the team's code.
            settings.worker.team = Some(meerschaum_worker::worker::TeamExpectation {
                team: scope.team.clone(),
                fingerprint: fingerprint.clone(),
                connectors: scope.connectors.clone(),
                sandbox: sandbox.as_ref().map(|p| p.mode),
            });
            // A sandboxed worker works in the team's temporary directory (the server's working
            // directory is outside its rules).
            if let Some(prepared) = &sandbox {
                settings.worker.current_dir = Some(prepared.tmp_dir.clone());
            }
            if !std::env::var("MRSM_PYTHON").is_ok_and(|v| !v.trim().is_empty())
                && let Some(python) = &worker_python
            {
                settings.worker.python = python.into();
            }
            settings
        });
        *child.pool.lock().unwrap_or_else(|e| e.into_inner()) = Some(pool);
        // The keys of the team's configuration, loaded once: an action run for the team reads
        // them from memory.
        for key in team_map.keys() {
            let _ = team_config.get(&[key.as_str()]);
        }
        let mut snapshot = team_config.snapshot();
        // The values are substituted already; the originals would only slow every lookup (the
        // team's configuration is never written back).
        snapshot.shift_remove("_symlinks");
        // The engine's settings (v5-only defaults, absent from a written `system` file) in the
        // snapshot: each lookup of one finds it instead of re-substituting every default.
        if let Some(Value::Map(engine)) = team_config
            .defaults()
            .get("system")
            .and_then(|s| s.as_map())
            .and_then(|s| s.get("engine"))
            .cloned()
            && let Some(Value::Map(system)) = snapshot.get_mut("system")
        {
            let merged = match system.get("engine") {
                Some(Value::Map(own)) => apply_patch_maps(&engine, own),
                _ => engine,
            };
            system.insert("engine".into(), Value::Map(merged));
        }
        child.team = Some(Arc::new(TeamBinding {
            scope: scope.clone(),
            config: team_config,
            fingerprint: fingerprint.clone(),
            files,
            env: team_env,
            snapshot,
        }));
        let lookup = match lookup {
            TeamLookup::Warm => TeamLookup::Replaced,
            other => other,
        };
        self.teams
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, (fingerprint, child.clone()));
        Ok((child, lookup))
    }

    /// Retire the sessions (and workers) of the teams `keep` refuses (a deleted team's, or
    /// one whose configuration changed). Returns the retired teams' keys.
    pub fn retire_teams(&self, keep: impl Fn(&str, &str) -> bool) -> Vec<String> {
        let mut retired = Vec::new();
        let pools = self.teams.pools.get().cloned();
        let mut sessions = self
            .teams
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        sessions.retain(|key, (fingerprint, _)| {
            let kept = keep(key, fingerprint);
            if !kept {
                retired.push(key.clone());
                if let Some(pools) = &pools {
                    pools.retire(key);
                }
            }
            kept
        });
        retired
    }

    /// An already built connector of this session (never builds one).
    pub fn cached_instance(&self, keys: &str) -> Option<Instance> {
        self.instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&full_keys(keys))
            .cloned()
    }

    /// Register a connector the host built for `keys` (`type:label`; a bare type means the
    /// default label). Later [`Session::instance`] lookups of those keys return it instead of
    /// building one from the config; registering again replaces it. Returns the connector it
    /// replaced (registered or built from config), if any.
    pub fn register_instance(&self, keys: &str, instance: Instance) -> Option<Instance> {
        let full = full_keys(keys);
        self.registered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(full.clone());
        self.instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(full, instance)
    }

    /// [`Session::register_instance`] for a SQL connector.
    pub fn register_sql(&self, keys: &str, conn: Arc<SqlConnector>) -> Option<Instance> {
        self.register_instance(keys, Instance::Sql(conn))
    }

    /// Forget the connector for `keys` (registered or built); the next lookup builds it again.
    pub fn unregister(&self, keys: &str) -> Option<Instance> {
        let full = full_keys(keys);
        self.registered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&full);
        self.instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&full)
    }

    /// Whether the host registered the connector for `keys`.
    pub fn is_registered(&self, keys: &str) -> bool {
        self.registered
            .lock()
            .unwrap_or_else(|e| e.into_inner())
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

    /// Attach the trusted host's confined native-source compiler before any manifest or
    /// native discovery. `None` explicitly refuses source compilation; it never selects
    /// ambient Cargo. Clones and team children share this single immutable choice.
    /// This Rust host API has no configuration, environment, job or Python selector.
    pub fn set_source_compiler(
        &self,
        compiler: Option<Arc<dyn SourceCompiler>>,
    ) -> Result<(), ActionError> {
        let state = self.manifests.lock().unwrap_or_else(|e| e.into_inner());
        if state.python.is_some()
            || state.natives.is_some()
            || state.native_signature.is_some()
            || state.merged.is_some()
            || state.validated
        {
            return Err(ActionError::new(
                "RuntimeError",
                "The source compiler must be attached before manifest initialization.",
            ));
        }
        self.source_compiler
            .set(CompilerPolicy::Confined(compiler))
            .map_err(|_| {
                ActionError::new(
                    "RuntimeError",
                    "The source compiler policy is already fixed for this session.",
                )
            })
    }

    /// Whether this session may start the Python worker ([`Session::set_python_worker`]).
    pub fn worker_enabled(&self) -> bool {
        self.python_worker.load(Ordering::SeqCst)
    }

    /// Set an isolated session's complete worker environment before creating its pool.
    /// Clones share this setting; the host process environment is never changed.
    pub fn set_worker_environment(&self, env: Vec<(String, String)>) -> Result<(), ActionError> {
        let pool = self.pool.lock().unwrap_or_else(|e| e.into_inner());
        if pool.is_some() {
            return Err(ActionError::new(
                "RuntimeError",
                "Worker pool already initialized.",
            ));
        }
        *self
            .worker_environment
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(env);
        Ok(())
    }

    /// Suggest the interpreter the workers run with when `MRSM_PYTHON` is not set (the host
    /// process's environment is never changed). Takes effect when the pool is created.
    pub fn set_worker_python(&self, python: &str) {
        *self.worker_python.lock().unwrap_or_else(|e| e.into_inner()) = Some(python.to_string());
    }

    /// Queue a hook result from an engine task for the host's output thread.
    pub fn report_hook_result(&self, result: meerschaum_core::SuccessTuple) {
        self.hook_results
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(result);
    }

    /// Drain hook results in the order the engine reported them.
    pub fn take_hook_results(&self) -> Vec<meerschaum_core::SuccessTuple> {
        std::mem::take(&mut *self.hook_results.lock().unwrap_or_else(|e| e.into_inner()))
    }

    /// The single lazily-created Python pool shared by this process session.
    pub fn pool(&self, config: &Config) -> WorkerPool {
        self.pool
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(|| {
                let mut settings = PoolConfig::from_config(config);
                if let Some(env) = &*self
                    .worker_environment
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                {
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
                    .unwrap_or_else(|e| e.into_inner())
                    .is_none()
                    && !std::env::var("MRSM_PYTHON").is_ok_and(|v| !v.trim().is_empty())
                    && let Some(python) =
                        &*self.worker_python.lock().unwrap_or_else(|e| e.into_inner())
                {
                    settings.worker.python = python.into();
                }
                settings.worker.inherit_stdin =
                    std::io::stdin().is_terminal() || stdin_is_a_jobs_input();
                WorkerPool::new(settings)
            })
            .clone()
    }

    fn wait_for_background_refresh(&self) -> Result<(), ActionError> {
        let inflight = self
            .manifests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .refreshing
            .clone();
        if let Some(refresh) = inflight {
            return refresh.wait();
        }
        Ok(())
    }

    fn resolve_entry_points(
        pool: &WorkerPool,
        manifest_file: &std::path::Path,
        root_dir: &std::path::Path,
        force: bool,
    ) -> Result<Vec<String>, ActionError> {
        // v4 finds pip-installed plugins whatever the plugins directory holds: a cached probe of
        // the interpreter (a few `stat` calls) says whether any exist, so an empty tree with no
        // entry point still starts no Python.
        let mut entry_points = probe_entry_points(pool, manifest_file, force);
        // An executor's signed grant list also limits pip entry points. Its physical plugin
        // directory view already limits ordinary Python and native discovery.
        let inherited_grants = (!pool.config().worker.env_clear
            && !pool
                .config()
                .worker
                .env_remove
                .iter()
                .any(|key| key == "MRSM_EXECUTOR_PLUGINS"))
        .then(|| std::env::var("MRSM_EXECUTOR_PLUGINS").ok())
        .flatten();
        if let Some(grants) = pool
            .config()
            .worker
            .env
            .iter()
            .find(|(key, _)| key == "MRSM_EXECUTOR_PLUGINS")
            .map(|(_, grants)| grants.as_str())
            .or(inherited_grants.as_deref())
        {
            let grants: std::collections::BTreeSet<String> = serde_json::from_str(grants)
                .map_err(|_| ActionError::new("RuntimeError", "Invalid executor plugin grants."))?;
            entry_points.retain(|name| grants.contains(name));
        }
        if let Some(namespace) = meerschaum_core::team_scope::source_namespace::for_root(root_dir) {
            entry_points.retain(|name| namespace.names.contains(name));
        }
        Ok(entry_points)
    }

    /// Warm the plugin manifest for an interactive shell / webterm without blocking the prompt
    /// on a stale manifest rebuild.
    ///
    /// - When both Python and native manifest caches on disk are already current, loads them
    ///   immediately (~10–15 ms, no Python worker, no native `.so` load) and marks the session
    ///   validated.
    /// - When either cache is stale or missing, populates the last cached manifest from disk for
    ///   instant prompt completions and spawns a quiet background thread to rebuild the fresh
    ///   manifest. Any subsequent blocking [`Session::manifest`] call joins that thread instead
    ///   of starting a second rebuild.
    pub fn warm_manifest_for_shell(&self, config: &Config) {
        let compiler = self
            .source_compiler
            .get_or_init(|| crate::team_sandbox::source_compiler_policy().unwrap_or_default())
            .clone();
        let mut state = self.manifests.lock().unwrap_or_else(|e| e.into_inner());
        if (state.validated && state.merged.is_some()) || state.refreshing.is_some() {
            return;
        }
        if state.cached_only || !self.worker_enabled() {
            let _ = self.validate_manifests(&mut state, config, false);
            return;
        }
        let paths = config.paths();
        let root_dir = paths.root_dir.clone();
        let plugins_dirs = paths.plugins_dirs.clone();
        let pool = self.pool(config);
        let manifest_file = manifest_path(&root_dir);
        let native_cache = crate::native_plugins::cache_path(&root_dir);
        let Ok(entry_points) = Self::resolve_entry_points(&pool, &manifest_file, &root_dir, false)
        else {
            return;
        };
        let fresh_python = read_fresh_manifest_for(&manifest_file, &plugins_dirs, &entry_points)
            .ok()
            .flatten();
        let fresh_native = meerschaum_plugin::discovery::manifest_cache_ready_with_policy(
            &plugins_dirs,
            &native_cache,
            &compiler,
        );
        if let Some(python) = fresh_python
            && fresh_native
        {
            let python = Arc::new(python);
            let signature = native_signature(&plugins_dirs, &native_cache, &compiler);
            let (natives, error) =
                match NativePlugins::open_with_policy(&plugins_dirs, &native_cache, &compiler) {
                    Ok(natives) => (Arc::new(natives), None),
                    Err(error) => (Arc::new(NativePlugins::empty()), Some(error.to_string())),
                };
            let mut merged = crate::native_plugins::merge(&python, &natives);
            if let Some(error) = &error {
                merged
                    .load_errors
                    .insert("native plugins".into(), error.clone());
            }
            state.python = Some(python);
            state.natives = Some(natives);
            state.native_signature = Some(signature);
            state.native_error = error;
            state.merged = Some(Arc::new(merged));
            state.validated = true;
            return;
        }
        // Slow path: serve cached metadata immediately and rebuild quietly in the background.
        let fallback = crate::completions::cached_manifest_for(&root_dir, &plugins_dirs);
        state.merged = Some(Arc::new(fallback));
        let refresh = Arc::new(BackgroundManifestRefresh::new());
        state.refreshing = Some(refresh.clone());
        let expected_gen = state.generation;
        drop(state);

        let session = self.clone();
        let bg_refresh = refresh.clone();
        let spawned = std::thread::Builder::new()
            .name("mrsm-manifest".into())
            .spawn(move || {
                let outcome = (|| -> Result<(), ActionError> {
                    let (manifest, _source) = session
                        .block_on(ensure_manifest_for_with_options(
                            &pool,
                            &manifest_file,
                            &plugins_dirs,
                            &entry_points,
                            true,
                        ))
                        .map_err(|e| ActionError::new("WorkerError", e.to_string()))?;
                    let python = Arc::new(manifest);
                    let signature = native_signature(&plugins_dirs, &native_cache, &compiler);
                    let (natives, error) = match NativePlugins::open_with_policy_quiet(
                        &plugins_dirs,
                        &native_cache,
                        &compiler,
                    ) {
                        Ok(natives) => (Arc::new(natives), None),
                        Err(error) => (Arc::new(NativePlugins::empty()), Some(error.to_string())),
                    };
                    let mut merged = crate::native_plugins::merge(&python, &natives);
                    if let Some(error) = &error {
                        merged
                            .load_errors
                            .insert("native plugins".into(), error.clone());
                    }
                    let mut state = session.manifests.lock().unwrap_or_else(|e| e.into_inner());
                    if !state.cached_only && state.generation == expected_gen {
                        state.python = Some(python);
                        state.natives = Some(natives);
                        state.native_signature = Some(signature);
                        state.native_error = error;
                        state.merged = Some(Arc::new(merged));
                        state.validated = true;
                    }
                    if state
                        .refreshing
                        .as_ref()
                        .is_some_and(|r| Arc::ptr_eq(r, &bg_refresh))
                    {
                        state.refreshing = None;
                    }
                    Ok(())
                })();
                if outcome.is_err() {
                    let mut state = session.manifests.lock().unwrap_or_else(|e| e.into_inner());
                    if state
                        .refreshing
                        .as_ref()
                        .is_some_and(|r| Arc::ptr_eq(r, &bg_refresh))
                    {
                        state.refreshing = None;
                    }
                }
                bg_refresh.finish(outcome);
            });
        if spawned.is_err() {
            let mut state = self.manifests.lock().unwrap_or_else(|e| e.into_inner());
            if state
                .refreshing
                .as_ref()
                .is_some_and(|r| Arc::ptr_eq(r, &refresh))
            {
                state.refreshing = None;
            }
            drop(state);
            refresh.finish(Ok(()));
        }
    }

    /// Return the currently available plugin manifest without blocking on an in-flight
    /// background shell refresh. Falls back to [`Session::manifest`] when no background refresh
    /// is active.
    pub fn manifest_nonblocking(&self, config: &Config) -> Option<Arc<Manifest>> {
        let state = self.manifests.lock().unwrap_or_else(|e| e.into_inner());
        if state.validated
            && let Some(merged) = state.merged.clone()
        {
            return Some(merged);
        }
        if state.refreshing.is_some() {
            return state.merged.clone();
        }
        drop(state);
        self.manifest(config).ok()
    }

    /// The fingerprinted manifest (Python plugins merged with native plugins), validated at most
    /// once per [`Session::invalidate_manifest`]. Starts no Python for an empty plugin tree or
    /// an unchanged cache.
    pub fn manifest(&self, config: &Config) -> Result<Arc<Manifest>, ActionError> {
        self.wait_for_background_refresh()?;
        let mut state = self.manifests.lock().unwrap_or_else(|e| e.into_inner());
        let started = std::time::Instant::now();
        let validated = state.validated;
        self.validate_manifests(&mut state, config, false)?;
        if !validated {
            meerschaum_core::dprint!(
                "Validated the plugin manifest in {:?} (cached_only={}).",
                started.elapsed(),
                state.cached_only
            );
        }
        Ok(state.merged.clone().expect("validated manifest"))
    }

    /// Switch read-only manifest mode on or off (`doctor`): on, [`Session::manifest`] returns the
    /// manifests the last command cached on disk and never validates, rebuilds or imports
    /// anything; either way the manifest state is dropped, so the next call starts afresh.
    pub fn set_cached_manifest_only(&self, on: bool) {
        let mut guard = self.manifests.lock().unwrap_or_else(|e| e.into_inner());
        let generation = guard.generation.wrapping_add(1);
        *guard = ManifestState {
            cached_only: on,
            generation,
            ..ManifestState::default()
        };
    }

    /// Whether [`Session::set_cached_manifest_only`] is on.
    pub fn cached_manifest_only(&self) -> bool {
        self.manifests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cached_only
    }

    /// Re-arm the next [`Session::manifest`] call to validate the plugin trees again (one
    /// fingerprint walk). Cheap; the shell calls it after every command.
    pub fn invalidate_manifest(&self) {
        let mut guard = self.manifests.lock().unwrap_or_else(|e| e.into_inner());
        guard.validated = false;
        guard.generation = guard.generation.wrapping_add(1);
    }

    /// Validate the plugin trees now, rebuilding the manifest if a plugin changed.
    pub fn refresh_manifest(&self, config: &Config) -> Result<Arc<Manifest>, ActionError> {
        self.invalidate_manifest();
        self.manifest(config)
    }

    /// `reload`: rebuild the Python manifest unconditionally in a fresh worker, re-open the
    /// native plugins and recycle the worker pool.
    pub fn reload_plugins(&self, config: &Config) -> Result<Arc<Manifest>, ActionError> {
        // A refused interpreter (a stale `meerschaum`) may have been fixed: ask it again.
        meerschaum_worker::worker::clear_refusals();
        let _ = self.wait_for_background_refresh();
        let mut state = self.manifests.lock().unwrap_or_else(|e| e.into_inner());
        state.generation = state.generation.wrapping_add(1);
        self.validate_manifests(&mut state, config, true)?;
        Ok(state.merged.clone().expect("validated manifest"))
    }

    fn validate_manifests(
        &self,
        state: &mut ManifestState,
        config: &Config,
        force: bool,
    ) -> Result<(), ActionError> {
        // The manifests lock serializes the trusted setter against the first discovery.
        // Keep this frozen even when cached-only mode later drops ManifestState.
        let compiler = self
            .source_compiler
            .get_or_init(|| crate::team_sandbox::source_compiler_policy().unwrap_or_default());
        if !force && state.validated && state.merged.is_some() {
            return Ok(());
        }
        if state.cached_only {
            let paths = config.paths();
            let python =
                read_cached_manifest_for(&manifest_path(&paths.root_dir), &paths.plugins_dirs)
                    .ok()
                    .flatten()
                    .unwrap_or_default();
            state.python = Some(Arc::new(python));
            state.natives = Some(Arc::new(NativePlugins::empty()));
            state.merged = Some(Arc::new(crate::completions::cached_manifest_for(
                &paths.root_dir,
                &paths.plugins_dirs,
            )));
            state.validated = true;
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
        let entry_points =
            Self::resolve_entry_points(&pool, &manifest_file, &paths.root_dir, force)?;
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
                // Team workers hold plugin modules too.
                if previous.as_ref().is_some_and(|p| **p != manifest)
                    && let Some(pools) = self.teams.pools.get().cloned()
                {
                    self.block_on(pools.recycle_all());
                }
                python_changed |= previous.as_ref().is_none_or(|p| **p != manifest);
                Arc::new(manifest)
            }
        };
        python_changed |= force;
        let native_cache = crate::native_plugins::cache_path(&paths.root_dir);
        let signature = native_signature(&paths.plugins_dirs, &native_cache, compiler);
        if force || state.natives.is_none() || state.native_signature.as_ref() != Some(&signature) {
            let (natives, error) =
                match NativePlugins::open_with_policy(&paths.plugins_dirs, &native_cache, compiler)
                {
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
        self.wait_for_background_refresh()?;
        let mut state = self.manifests.lock().unwrap_or_else(|e| e.into_inner());
        // Engine tasks call this per sync: whatever the session holds is current enough.
        if let Some(python) = state.python.clone() {
            return Ok(python);
        }
        self.validate_manifests(&mut state, config, false)?;
        Ok(state.python.clone().expect("validated manifest"))
    }

    /// The native plugins (discovered with the manifest).
    pub fn native_plugins(&self, config: &Config) -> Result<Arc<NativePlugins>, ActionError> {
        self.wait_for_background_refresh()?;
        let mut state = self.manifests.lock().unwrap_or_else(|e| e.into_inner());
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

    /// `parse_instance_keys(keys)` → the connector (built once per keys).
    pub fn instance(&self, keys: &str, config: &Config) -> Result<Instance, ActionError> {
        let (typ, label) = split_keys(keys);
        let full = format!("{typ}:{label}");
        // Read the exact descriptor before a cached native driver or default attributes can
        // adopt these logical keys. API execution keeps the identity but carries no DB client.
        if let Some(alias) =
            meerschaum_connector_api::APIConnector::execution_alias_from_config(config, &full)
                .map_err(|e| ActionError::new("InvalidAttributesError", e.to_string()))?
        {
            return Ok(Instance::Other(Arc::new(alias)));
        }
        if let Some(i) = self
            .instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&full)
        {
            return Ok(i.clone());
        }
        if self.registered_only.load(Ordering::SeqCst) {
            return Err(ActionError::new(
                "ConnectorNotRegistered",
                format!("No connector was registered for '{full}'."),
            ));
        }
        if matches!(typ.as_str(), "api" | "valkey")
            && connector_attributes(config, &typ, &label).is_empty()
        {
            return Err(ActionError::new(
                "ValueError",
                super::connectors::undefined_connector_message(&typ, &label),
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
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(full, inst.clone());
                    return Ok(inst);
                }
                let connector = self
                    .block_on(meerschaum_worker::PythonConnector::connect(
                        keys,
                        self.pool(config),
                    ))
                    .map_err(|e| {
                        // A type no plugin declares: say where connectors come from.
                        use meerschaum_core::config::connector_types;
                        let paths = config.paths();
                        let known = connector_types::is_builtin_connector_type(&typ)
                            || connector_types::plugin_connector_types(
                                &paths.root_dir,
                                &paths.plugins_dirs,
                            )
                            .contains(&typ);
                        if known {
                            ActionError::new("WorkerError", e.to_string())
                        } else {
                            ActionError::new(
                                "ValueError",
                                super::connectors::undefined_connector_message(&typ, &label),
                            )
                        }
                    })?;
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
            .unwrap_or_else(|e| e.into_inner())
            .insert(full, inst.clone());
        Ok(inst)
    }

    /// A `sql:<label>` connector from the config registry.
    pub fn sql_connector(&self, label: &str, config: &Config) -> Result<SqlConnector, ActionError> {
        if meerschaum_connector_api::APIConnector::execution_alias_from_config(
            config,
            &format!("sql:{label}"),
        )
        .map_err(|e| ActionError::new("InvalidAttributesError", e.to_string()))?
        .is_some()
        {
            return Err(ActionError::new(
                "InvalidAttributesError",
                "This logical SQL connector uses the API; a native database client is unavailable.",
            ));
        }
        let spec = sql_spec(label, config)?;
        let settings = InstanceSettings::from_config(spec.flavor, config);
        let conn = self
            .block_on(SqlConnector::connect_with(label, spec, settings))
            .map_err(|e| {
                ActionError::new(
                    "OperationalError",
                    super::connectors::connect_failure_message(label, config, &e),
                )
            })?;
        // Cross-instance `{{ Pipe(..., instance='sql:other') }}` references resolve against the
        // configured connectors, not only `MRSM_SQL_<LABEL>` environment variables.
        conn.set_instance_provider(Arc::new(ConfiguredSqlProvider::new(config)));
        Ok(conn)
    }
}

/// The connection spec of `sql:<label>` from the config registry, without connecting.
pub fn sql_spec(label: &str, config: &Config) -> Result<ConnectionSpec, ActionError> {
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
        // v4 raised "Missing flavor" here; the connector is defined nowhere.
        return Err(ActionError::new(
            "ValueError",
            super::connectors::undefined_connector_message("sql", label),
        ));
    }
    ConnectorAttributes::from_attribute_map(label, &attrs, InvalidPort::Reject)
        .and_then(|ca| ConnectionSpec::from_attributes(&ca))
        .map_err(|e| ActionError::new("ValueError", e.to_string()))
}

/// `type:label` with the default label filled in (the session's map key).
/// What native plugin discovery sees (names, trees and skipped directories): when it is
/// unchanged the opened [`NativePlugins`] and the warnings printed for it are still current.
fn native_signature(
    plugins_dirs: &[std::path::PathBuf],
    cache: &std::path::Path,
    compiler: &CompilerPolicy,
) -> Vec<String> {
    let discovery = meerschaum_plugin::discovery::discover_report_in_with_policy(
        plugins_dirs,
        &meerschaum_plugin::discovery::build_dir_for_cache(cache),
        compiler,
    );
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

/// What a callback from a team's plugin code may do to a pipe: what a member of the team
/// may change through the API ([`meerschaum_core::access::can_edit`] of the pipe's stored
/// access). A pipe that is not registered yet, or on an instance the session has not built, is
/// left to the team's instance check.
struct TeamPipeGuard {
    instances: Arc<Mutex<HashMap<String, Instance>>>,
    member: meerschaum_core::access::Principal,
}

#[meerschaum_connector::async_trait]
impl meerschaum_worker::callback::CallbackGuard for TeamPipeGuard {
    async fn check(&self, request: &meerschaum_worker::CallbackRequest) -> Result<(), String> {
        let Some(pipe) = request.pipe.as_ref() else {
            return Ok(());
        };
        let instance = self
            .instances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&full_keys(pipe.instance_keys()))
            .cloned();
        let Some(instance) = instance else {
            return Ok(());
        };
        let conn = instance.conn();
        let stored = match conn.get_pipe_attributes(pipe).await {
            Ok(attributes) => attributes,
            Err(_) => return Ok(()),
        };
        let Some(Value::Map(parameters)) = stored.get("parameters") else {
            return Ok(());
        };
        let access = meerschaum_core::access::PipeAccess::from_parameters(parameters);
        if meerschaum_core::access::can_edit(&self.member, &access) {
            Ok(())
        } else {
            Err(format!("You may not change {pipe}."))
        }
    }
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
        let ca =
            ConnectorAttributes::from_attribute_map(label, &attrs, InvalidPort::Reject).ok()?;
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
    use meerschaum_connector_sql::flavor::Flavor;
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

/// Whether this process runs as a job and reads the job's input FIFO: its Python workers read it
/// too, so that a plugin's `input()` or `prompt()` is answered through `attach` like in v4 (where
/// the action ran inside the daemon). Elsewhere a piped standard input stays the action's own.
fn stdin_is_a_jobs_input() -> bool {
    let sc = meerschaum_core::static_config::static_config();
    if std::env::var_os(&sc.environment.daemon_id).is_none_or(|v| v.is_empty()) {
        return false;
    }
    #[cfg(unix)]
    {
        rustix::fs::fstat(std::io::stdin()).is_ok_and(|st| {
            rustix::fs::FileType::from_raw_mode(st.st_mode) == rustix::fs::FileType::Fifo
        })
    }
    #[cfg(not(unix))]
    {
        false
    }
}
