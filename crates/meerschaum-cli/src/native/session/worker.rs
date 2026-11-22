//! A session's Python worker pool and plugin manifest: validating, caching and refreshing the
//! manifest, and the native plugins it opens.

use meerschaum_worker::manifest::{
    Manifest, ManifestSource, ensure_manifest_for, ensure_manifest_for_with_options, manifest_path,
    probe_entry_points, read_cached_manifest_for, read_fresh_manifest_for, rebuild_manifest_for,
    stale_plugins_for,
};
use meerschaum_worker::{PoolConfig, WorkerPool};
use std::io::IsTerminal;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use meerschaum_core::config::Config;
use meerschaum_plugin::NativePlugins;
use meerschaum_plugin::source::compiler::{CompilerPolicy, SourceCompiler};

use crate::builtin::ActionError;

use super::instances::native_signature;
use super::*;

impl Session {
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
            .get_or_init(|| crate::team::sandbox::source_compiler_policy().unwrap_or_default())
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
            .get_or_init(|| crate::team::sandbox::source_compiler_policy().unwrap_or_default());
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
