//! Native plugin hooks, sync wrappers, and `NativeRegistry`.

use super::*;

/// A native instance connector for `keys` (`<type>:<label>`), if a native plugin declares the type.
pub fn instance_connector(
    natives: &NativePlugins,
    keys: &str,
    attributes: &Map,
) -> Option<Result<Arc<dyn InstanceConnector>, String>> {
    let (kind, label) = keys.split_once(':')?;
    if !natives.is_instance_type(kind) {
        return None;
    }
    Some(
        natives
            .connector(kind, label, config_json(attributes))?
            .map(|c| Arc::new(c) as Arc<dyn InstanceConnector>)
            .map_err(|e| e.to_string()),
    )
}

pub(super) fn bound_value(bound: &meerschaum_connector::DateBound) -> Value {
    match bound {
        meerschaum_connector::DateBound::Int(i) => Value::Int(*i),
        other => Value::Str(other.isoformat()),
    }
}

pub(super) fn pipe_json(pipe: &Pipe) -> Value {
    let mut map = Map::new();
    map.insert("connector_keys".into(), pipe.connector_keys().into());
    map.insert("metric_key".into(), pipe.metric_key().into());
    map.insert(
        "location_key".into(),
        pipe.location_key().map(String::from).into(),
    );
    map.insert("instance_keys".into(), pipe.instance_keys().into());
    map.insert("temporary".into(), pipe.temporary.into());
    map.insert(
        "parameters".into(),
        Value::Map(pipe.raw_parameters().clone()),
    );
    Value::Map(map)
}

/// The keyword arguments v4 passes to `sync()` (the hooks get `hook_kwargs`).
pub(super) fn sync_kwargs(pipe: &Pipe, args: &SyncArgs) -> serde_json::Value {
    let mut map = args.extra.clone();
    map.insert("pipe".into(), pipe_json(pipe));
    map.insert(
        "begin".into(),
        match &args.begin {
            SyncBegin::Default => Value::Str(String::new()),
            SyncBegin::Unbounded => Value::Null,
            SyncBegin::At(b) => bound_value(b),
        },
    );
    map.insert("end".into(), args.end.as_ref().map(bound_value).into());
    map.insert("params".into(), args.params.clone().map(Value::Map).into());
    map.insert("check_existing".into(), args.check_existing.into());
    map.insert("force".into(), args.force.into());
    map.insert(
        "sync_timestamp".into(),
        args.sync_timestamp.to_rfc3339().into(),
    );
    serde_json::to_value(map).unwrap_or(serde_json::Value::Null)
}

/// The keyword arguments a sync hook receives: `sync_kwargs` without `pipe`, which Python
/// passes positionally (native hooks find it in `context.pipe`).
pub(super) fn hook_kwargs(pipe: &Pipe, args: &SyncArgs) -> serde_json::Value {
    let mut kwargs = sync_kwargs(pipe, args);
    if let Some(map) = kwargs.as_object_mut() {
        map.remove("pipe");
    }
    kwargs
}

/// Where the CLI collects sync hook results to print them (shared with the Python hooks).
pub(super) type HookReporterSlot =
    Arc<std::sync::RwLock<Option<meerschaum_engine::python::HookReporter>>>;

pub(super) struct NativeHooks {
    pub(super) natives: Arc<NativePlugins>,
    pub(super) inner: Option<Arc<dyn SyncHooks>>,
    pub(super) reporter: HookReporterSlot,
    /// Serves the hooks' host calls.
    pub(super) registry: NativeRegistry,
}

/// One native hook's outcome: its result, and whether it raised (an error, not a tuple).
pub(super) struct HookOutcome {
    pub(super) plugin: String,
    pub(super) hook: String,
    pub(super) result: SuccessTuple,
    pub(super) raised: bool,
}

/// Run one plugin's hooks in order. PARITY: as v4's `call_sync_hook`, an error is the tuple
/// `(False, "Failed to execute sync hook '<hook>' from plugin '<plugin>':\n<error>")`.
pub(super) fn run_plugin_hooks(
    natives: &NativePlugins,
    name: &str,
    hooks: Vec<String>,
    post: bool,
    kwargs: &serde_json::Value,
    pipe: &serde_json::Value,
    host: &PluginHost,
) -> Vec<HookOutcome> {
    let failed = |hook: &str, error: &dyn std::fmt::Display| {
        SuccessTuple::fail(format!(
            "Failed to execute sync hook '{hook}' from plugin '{name}':\n{error}"
        ))
    };
    let plugin = match natives.plugin(name) {
        Ok(plugin) => plugin,
        Err(error) => {
            return hooks
                .into_iter()
                .map(|hook| HookOutcome {
                    result: failed(&hook, &error),
                    plugin: name.to_string(),
                    hook,
                    raised: true,
                })
                .collect();
        }
    };
    hooks
        .into_iter()
        .map(|hook| {
            let (result, raised) =
                match plugin.sync_hook(post, &hook, kwargs.clone(), pipe.clone(), Some(host)) {
                    Ok(tuple) => (tuple, false),
                    Err(error) => (failed(&hook, &error), true),
                };
            HookOutcome {
                plugin: name.to_string(),
                hook,
                result,
                raised,
            }
        })
        .collect()
}

impl NativeHooks {
    pub(super) async fn run(
        &self,
        post: bool,
        pipe: &Pipe,
        args: &SyncArgs,
        kwargs: serde_json::Value,
    ) {
        // PARITY: v4 runs hooks from `actions/sync.py`, around `pipe.sync()`; a `pipe.sync()`
        // inside a hook runs no hooks.
        let mut host_args = args.clone();
        host_args.extra.insert("skip_hooks".into(), true.into());
        host_args
            .extra
            .insert("_from_sync_hook".into(), true.into());
        let host = Arc::new(PluginHost::engine(self.registry.clone(), host_args));
        let pipe = serde_json::to_value(pipe_json(pipe)).unwrap_or_default();
        let reporter = self
            .reporter
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        for (name, hooks) in self.natives.hooks(post) {
            let natives = self.natives.clone();
            let (kwargs, pipe, host) = (kwargs.clone(), pipe.clone(), host.clone());
            // A native call may finish after its caller's deadline. Keep its slot until it exits.
            let Ok(slot) = self.registry.hook_slots.clone().acquire_owned().await else {
                return;
            };
            let outcomes = tokio::task::spawn_blocking(move || {
                let _slot = slot;
                run_plugin_hooks(&natives, &name, hooks, post, &kwargs, &pipe, &host)
            })
            .await
            .unwrap_or_else(|error| {
                vec![HookOutcome {
                    plugin: String::new(),
                    hook: String::new(),
                    result: SuccessTuple::fail(format!("Native plugin hook failed: {error}")),
                    raised: true,
                }]
            });
            // PARITY: hook failures are never fatal. v4 warns about a hook that raised and
            // prints every hook's tuple, successful or not (`actions/sync.py`).
            for outcome in outcomes {
                if outcome.raised {
                    tracing_warn(&outcome.result.message);
                }
                match &reporter {
                    Some(report) => report(outcome.result),
                    None if !outcome.result.success && !outcome.raised => {
                        tracing_warn(&format!(
                            "Native plugin '{}' hook failed: {}: {}",
                            outcome.plugin, outcome.hook, outcome.result.message
                        ));
                    }
                    None => {}
                }
            }
        }
    }
}

pub(super) fn tracing_warn(message: &str) {
    eprintln!("Warning: {message}");
}

#[async_trait]
impl SyncHooks for NativeHooks {
    async fn pre_sync(&self, pipe: &Pipe, args: &SyncArgs) {
        self.run(false, pipe, args, hook_kwargs(pipe, args)).await;
        if let Some(inner) = &self.inner {
            inner.pre_sync(pipe, args).await;
        }
    }

    async fn post_sync(&self, pipe: &Pipe, args: &SyncArgs, result: &SyncResult) {
        let mut kwargs = hook_kwargs(pipe, args);
        kwargs["success_tuple"] = serde_json::json!([result.success, result.message]);
        kwargs["sync_duration"] =
            serde_json::json!(result.duration.map(|d| d.as_secs_f64()).unwrap_or_default());
        kwargs["sync_complete_timestamp"] = chrono::Utc::now().to_rfc3339().into();
        self.run(true, pipe, args, kwargs).await;
        if let Some(inner) = &self.inner {
            inner.post_sync(pipe, args, result).await;
        }
    }
}

/// A native plugin's own `sync()`.
pub(super) struct NativeSync(meerschaum_plugin::NativeConnector, NativeRegistry);

#[async_trait]
impl PluginSync for NativeSync {
    async fn sync(&self, pipe: &Pipe, args: &SyncArgs) -> SyncResult {
        let connector = self
            .0
            .clone()
            .with_host_services(Arc::new(PluginHost::engine(self.1.clone(), args.clone())));
        let context = sync_kwargs(pipe, args);
        tokio::task::spawn_blocking(move || {
            connector
                .call("sync", context, None)
                .and_then(|reply| Ok(serde_json::from_value::<SuccessTuple>(reply.value)?))
                .map(SyncResult::from)
                .unwrap_or_else(|e| SyncResult::new(false, e.to_string()))
        })
        .await
        .unwrap_or_else(|e| SyncResult::new(false, e.to_string()))
    }

    async fn sync_native(
        &self,
        pipe: &Pipe,
        args: &SyncArgs,
        _env: &SyncEnv<'_>,
        _observer: Option<&dyn DeltaObserver>,
    ) -> SyncResult {
        self.sync(pipe, args).await
    }
}

/// Resolves native connectors, `sync()` functions and sync hooks before the wrapped registry.
#[derive(Clone)]
pub struct NativeRegistry {
    pub(super) natives: Arc<NativePlugins>,
    pub(super) engine_config: meerschaum_engine::EngineConfig,
    /// `meerschaum:connectors` (connector attributes by type and label).
    pub(super) connectors: Map,
    pub(super) inner: Arc<dyn ConnectorRegistry>,
    pub(super) hook_reporter: HookReporterSlot,
    hook_slots: Arc<tokio::sync::Semaphore>,
    /// The sync's keyword arguments for streaming connectors' `subscribe` (`--listen`).
    pub(super) stream_kwargs: Arc<std::sync::RwLock<Map>>,
    /// The session whose connectors host calls use beyond the run's (`set_session`).
    pub(super) session: Arc<std::sync::RwLock<Option<crate::native::Session>>>,
    /// What host calls reach, built on first use.
    pub(super) host_env: Arc<std::sync::Mutex<Option<HostEnv>>>,
    /// The config host calls read and write: the run's own (never the process's default root).
    pub(super) host_config: Arc<meerschaum_core::config::Config>,
}

/// `meerschaum:connectors:<type>:default` patched with the label's section.
pub(crate) fn attributes(connectors: &Map, kind: &str, label: &str) -> Map {
    let section = |key: &str| {
        connectors
            .get(kind)
            .and_then(|v| v.get(key))
            .map(Value::to_map)
            .unwrap_or_default()
    };
    let specific = section(label);
    if specific.is_empty() {
        return Map::new();
    }
    meerschaum_core::value::apply_patch_maps(&section("default"), &specific)
}

impl NativeRegistry {
    /// Wrap `inner`. With no native plugins this is a pass-through. The plugins' host calls use
    /// a copy of `config` (its root, files and in-memory settings).
    pub fn new(
        natives: Arc<NativePlugins>,
        config: &meerschaum_core::config::Config,
        inner: Arc<dyn ConnectorRegistry>,
    ) -> Arc<Self> {
        Self::with_config(natives, host_config(config), inner)
    }

    /// [`new`](Self::new) whose host calls share `config` (a `write_plugin_config` updates it).
    pub fn with_config(
        natives: Arc<NativePlugins>,
        config: Arc<meerschaum_core::config::Config>,
        inner: Arc<dyn ConnectorRegistry>,
    ) -> Arc<Self> {
        let engine_config = meerschaum_engine::EngineConfig::from_config(&config, None);
        Arc::new(Self {
            hook_slots: Arc::new(tokio::sync::Semaphore::new(engine_config.workers.max(1))),
            natives,
            engine_config,
            connectors: config
                .get(&["meerschaum", "connectors"])
                .map(|v| v.to_map())
                .unwrap_or_default(),
            inner,
            hook_reporter: HookReporterSlot::default(),
            stream_kwargs: Arc::default(),
            session: Arc::default(),
            host_env: Arc::default(),
            host_config: config,
        })
    }

    /// Whether the plugins' host calls may write their config (`write_plugin_config`). The
    /// Python façade builds its registries over a read-only snapshot of its live config and
    /// allows the writes Python's own `write_plugin_config()` would make. Only for a registry
    /// made with [`new`](Self::new) (its config is its own copy).
    pub fn set_config_writes(&self, allow: bool) {
        self.host_config.with_state(|s| s.set_allow_writes(allow));
    }

    /// Serve the plugins' host calls (`host::pipe(...)`, `host::read_sql(...)`, ...) over
    /// `session`'s connectors, the ones the run itself uses. Without it, host calls build their
    /// connectors in a session of their own on the current runtime.
    pub fn set_session(&self, session: crate::native::Session) {
        *self.session.write().unwrap_or_else(|e| e.into_inner()) = Some(session);
        *self.host_env.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// What host calls during this registry's runs reach.
    pub(super) fn host_env(&self) -> Result<HostEnv, String> {
        let mut slot = self.host_env.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(env) = slot.as_ref() {
            return Ok(env.clone());
        }
        let session = self
            .session
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let session = match session {
            Some(session) => session,
            None => crate::native::Session::with_runtime(
                tokio::runtime::Handle::try_current().map_err(|e| e.to_string())?,
            ),
        };
        let mut env = HostEnv::new(
            session,
            self.host_config.clone(),
            self.natives.clone(),
            None,
        );
        if !self.engine_config.home_instance.is_empty() {
            env.default_instance = self.engine_config.home_instance.clone();
        }
        env.defaults = self.engine_config.defaults.clone();
        *slot = Some(env.clone());
        Ok(env)
    }

    /// The keyword arguments streaming native connectors' `subscribe` receives (the same map
    /// `PythonRegistry::set_stream_kwargs` gives Python `subscribe` sources).
    pub fn set_stream_kwargs(&self, kwargs: Map) {
        *self
            .stream_kwargs
            .write()
            .unwrap_or_else(|e| e.into_inner()) = kwargs;
    }

    /// Print native sync hook results the way the Python hooks' results are printed
    /// (`PythonRegistry::set_hook_reporter`). Without a reporter, failures are warnings.
    pub fn set_hook_reporter(&self, reporter: meerschaum_engine::python::HookReporter) {
        *self
            .hook_reporter
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(reporter);
    }

    pub(super) fn pipe_host(&self) -> Arc<PluginHost> {
        Arc::new(PluginHost::engine(self.clone(), SyncArgs::default()))
    }

    pub(super) fn source_connector(
        &self,
        keys: &str,
    ) -> Option<Result<meerschaum_plugin::NativeConnector, String>> {
        let root_dir = self
            .host_config
            .paths()
            .root_dir
            .to_string_lossy()
            .into_owned();
        let (kind, label) = keys.split_once(':')?;
        if kind == "plugin" {
            return self.natives.source(label).map(|r| {
                r.map(|c| {
                    c.with_root_dir(root_dir)
                        .with_host_services(self.pipe_host())
                })
                .map_err(|e| e.to_string())
            });
        }
        let attributes = attributes(&self.connectors, kind, label);
        self.natives
            .connector(kind, label, config_json(&attributes))
            .map(|r| {
                r.map(|c| {
                    c.with_root_dir(root_dir)
                        .with_host_services(self.pipe_host())
                })
                .map_err(|e| e.to_string())
            })
    }
}

impl ConnectorRegistry for NativeRegistry {
    fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        let (kind, label) = keys.split_once(':').unwrap_or((keys, ""));
        let attributes = attributes(&self.connectors, kind, label);
        match instance_connector(&self.natives, keys, &attributes) {
            Some(result) => result,
            None => self.inner.instance(keys),
        }
    }

    fn source(&self, keys: &str) -> Result<Option<Arc<dyn Connector>>, String> {
        match self.source_connector(keys) {
            Some(result) => Ok(Some(Arc::new(result?))),
            None => self.inner.source(keys),
        }
    }

    fn fetch_chunksize(&self, keys: &str) -> usize {
        self.inner.fetch_chunksize(keys)
    }

    fn source_for_sync(
        &self,
        keys: &str,
        args: &SyncArgs,
    ) -> Result<Option<Arc<dyn Connector>>, String> {
        match self.source_connector(keys) {
            Some(result) => {
                // The same fetch kwargs a Python plugin's `fetch(pipe, **kw)` receives.
                let kwargs = meerschaum_engine::sync::sync_kwargs(args);
                Ok(Some(Arc::new(
                    result?
                        .with_fetch_kwargs(&kwargs)
                        .with_host_services(Arc::new(PluginHost::engine(
                            self.clone(),
                            args.clone(),
                        ))),
                )))
            }
            None => self.inner.source_for_sync(keys, args),
        }
    }

    fn plugin_sync(&self, keys: &str) -> Option<Arc<dyn PluginSync>> {
        if let Some((kind, label)) = keys.split_once(':') {
            let overrides_sync = if kind == "plugin" {
                self.natives.has_data_function(label, "sync")
            } else {
                self.natives.connector_plugin(kind).is_some()
                    && self.natives.manifest()["connector_types"][kind]["methods"]
                        .as_array()
                        .is_some_and(|methods| methods.iter().any(|m| m == "sync"))
            };
            if overrides_sync {
                let connector = if kind == "plugin" {
                    // A sync-only plugin has no fetch source in the registry.
                    self.natives
                        .plugin(label)
                        .ok()?
                        .source(serde_json::Value::Null)
                        .with_root_dir(self.host_config.paths().root_dir.to_string_lossy())
                } else {
                    self.source_connector(keys)?.ok()?
                };
                return Some(Arc::new(NativeSync(connector, self.clone())));
            }
        }
        self.inner.plugin_sync(keys)
    }

    fn stream_source(&self, keys: &str) -> Option<Arc<dyn StreamSource>> {
        if let Some((kind, label)) = keys.split_once(':') {
            // A streaming native connector (pull-model `subscribe`, see `native_streams`).
            let connector = if kind == "plugin" {
                self.natives
                    .stream_source(label)
                    .and_then(Result::ok)
                    .map(|c| {
                        c.with_root_dir(self.host_config.paths().root_dir.to_string_lossy())
                            .with_host_services(self.pipe_host())
                    })
            } else {
                self.source_connector(keys).and_then(Result::ok)
            };
            if let Some(connector) = connector.filter(|c| c.is_streaming()) {
                let kwargs = serde_json::to_value(
                    &*self.stream_kwargs.read().unwrap_or_else(|e| e.into_inner()),
                )
                .unwrap_or_default();
                return Some(Arc::new(crate::native_streams::NativeStreamSource::new(
                    connector, kwargs,
                )));
            }
        }
        self.inner.stream_source(keys)
    }

    fn legacy_stream(&self, keys: &str) -> Option<Arc<dyn LegacyStream>> {
        self.inner.legacy_stream(keys)
    }

    fn is_python(&self, keys: &str) -> bool {
        if let Some(("plugin", name)) = keys.split_once(':')
            && self.natives.contains(name)
        {
            return false;
        }
        self.inner.is_python(keys)
    }

    fn hooks(&self) -> Option<Arc<dyn SyncHooks>> {
        let inner = self.inner.hooks();
        if self.natives.hooks(false).is_empty() && self.natives.hooks(true).is_empty() {
            return inner;
        }
        Some(Arc::new(NativeHooks {
            natives: self.natives.clone(),
            inner,
            reporter: self.hook_reporter.clone(),
            registry: self.clone(),
        }))
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        self.inner.table_reference(pipe)
    }
}
