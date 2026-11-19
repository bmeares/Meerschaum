//! `HostEnv`, connector caching, and plugin config write tracking.

use super::*;

// ---------------------------------------------------------------------------------------------
// Host calls: what native plugins ask the process (`meerschaum_plugin::callbacks`).
// ---------------------------------------------------------------------------------------------

/// What host calls reach in one process: its connectors (a [`Session`]), its config and its
/// native plugins.
///
/// [`Session`]: crate::native::Session
#[derive(Clone)]
pub struct HostEnv {
    pub(super) session: crate::native::Session,
    pub(super) config: Arc<meerschaum_core::config::Config>,
    pub(super) natives: Arc<NativePlugins>,
    pub(super) defaults: meerschaum_core::pipe::PipeDefaults,
    /// The instance of `host::pipe(...)` without `instance_keys` in a call without a Pipe.
    pub(super) default_instance: String,
    /// The `meerschaum:connectors` section as last read, and when the config file was checked.
    pub(super) connectors: Arc<std::sync::Mutex<LiveConnectors>>,
}

/// `meerschaum:connectors`, re-read when the config file changes (a connector another process
/// registered: a team's database made from the console or `bootstrap team`).
#[derive(Default)]
pub(super) struct LiveConnectors {
    pub(super) checked: Option<std::time::Instant>,
    pub(super) modified: Option<std::time::SystemTime>,
    pub(super) section: Option<Arc<Map>>,
}

/// How often the config file's modification time is looked at.
pub(super) const CONNECTORS_CHECK: std::time::Duration = std::time::Duration::from_secs(1);

impl HostEnv {
    /// Host calls over `session`'s connectors and `config`. `default_instance` defaults to
    /// `meerschaum:instance`.
    pub fn new(
        session: crate::native::Session,
        config: Arc<meerschaum_core::config::Config>,
        natives: Arc<NativePlugins>,
        default_instance: Option<String>,
    ) -> HostEnv {
        let defaults = meerschaum_engine::EngineConfig::from_config(&config, None).defaults;
        let default_instance = default_instance.unwrap_or_else(|| {
            config
                .get(&["meerschaum", "instance"])
                .map(|v| v.py_str())
                .unwrap_or_else(|| "sql:main".into())
        });
        HostEnv {
            session,
            config,
            natives,
            defaults,
            default_instance,
            connectors: Default::default(),
        }
    }

    /// The connector `keys` from the session (built on first use).
    pub(super) fn connector(&self, keys: &str) -> Result<crate::native::Instance, String> {
        // 5.1: a Python instance connector (a plugin's type) built for a request of an isolated
        // team runs in that team's workers, with its configuration. Rust connectors are the
        // server's (the routes check the team's access to them).
        if !matches!(keys.split(':').next(), Some("sql" | "api" | "valkey"))
            && let Some(acting) = meerschaum_connector::acting_team::current()
        {
            return blocking(|| {
                let (session, _) = self.session.for_team(
                    &acting.scope,
                    Some(acting.member.clone()),
                    &self.config,
                )?;
                let config = match session.team() {
                    Some(binding) => binding.new_config()?,
                    None => {
                        return Err(crate::builtin::ActionError::new(
                            "RuntimeError",
                            "No team session.",
                        ));
                    }
                };
                session.instance(keys, &config)
            })
            .map_err(|e| e.message);
        }
        blocking(|| self.session.instance(keys, &self.config)).map_err(|e| e.message)
    }

    /// The SQL connector `keys`.
    pub(super) fn sql(
        &self,
        keys: &str,
    ) -> Result<Arc<meerschaum_connector_sql::SqlConnector>, String> {
        match self.connector(keys)? {
            crate::native::Instance::Sql(sql) => Ok(sql),
            crate::native::Instance::Other(_) => Err(format!(
                "'{keys}' is not a SQL connector: `read_sql` and `exec_sql` need `sql:<label>` keys."
            )),
        }
    }

    /// A sync engine registry over the session (for host syncs outside an engine run); its host
    /// calls share this config.
    pub(super) fn registry(&self) -> NativeRegistry {
        let registry = NativeRegistry::with_config(
            self.natives.clone(),
            self.config.clone(),
            Arc::new(SessionRegistry(self.clone())),
        );
        registry.set_session(self.session.clone());
        (*registry).clone()
    }

    /// The plugin's config section (`plugins:<name>`), `null` when unset. A section a host call
    /// of this process wrote is read back from its file: this config may have been loaded
    /// before the write (another command in the shell, the next `--loop` run, another sync of
    /// a Python process).
    pub(super) fn plugin_config(&self, plugin: &str) -> serde_json::Value {
        let value = if written_plugin_config(&self.config, plugin) {
            plugin_config_file(&self.config, plugin)
        } else {
            self.config.get_plugin_config(plugin, &[])
        };
        value
            .and_then(|value| serde_json::to_value(value).ok())
            .unwrap_or(serde_json::Value::Null)
    }

    /// `write_plugin_config(config)`: `plugins:<name>` replaced in the config directory's
    /// `plugins` file, which Python's `get_plugin_config()` reads, and in memory. The other
    /// plugins' sections stay as the file has them (`MRSM{}` references included).
    pub(super) fn write_plugin_config(
        &self,
        plugin: &str,
        value: &serde_json::Value,
    ) -> SuccessTuple {
        use meerschaum_core::config::{ConfigMap, ConfigValue};
        let value: Value = match serde_json::from_value(value.clone()) {
            Ok(value) => value,
            Err(error) => return SuccessTuple::fail(format!("Invalid plugin config: {error}")),
        };
        let value = ConfigValue::from_value(&value);
        let (written, error) = self.config.with_state(|s| {
            // A `plugins` file that cannot be read is never replaced (it holds the other
            // plugins' settings).
            let on_disk = match s.read_config_lossless(
                None,
                Some(&["plugins".to_string()]),
                false,
                false,
                true,
            ) {
                Ok((on_disk, _)) => on_disk,
                Err(error) => {
                    let _ = s.take_notices();
                    return (
                        false,
                        Some((std::io::ErrorKind::InvalidData, error.to_string())),
                    );
                }
            };
            let mut plugins = on_disk
                .get("plugins")
                .and_then(ConfigValue::as_map)
                .cloned()
                .unwrap_or_default();
            plugins.insert(plugin.into(), value.clone());
            let mut section = ConfigMap::new();
            section.insert("plugins".into(), ConfigValue::Map(plugins));
            let written = s.write_config_lossless(Some(&section), None);
            // In memory too, when the section is loaded (else its first read loads the file).
            if written {
                let mut config = s.take_config_lossless();
                if let Some(in_memory) = config.get_mut("plugins").and_then(ConfigValue::as_map_mut)
                {
                    in_memory.insert(plugin.into(), value.clone());
                }
                s.set_config_lossless(config);
            }
            let _ = s.take_notices();
            (written, s.take_write_error())
        });
        if !written {
            let reason = match error {
                Some((_, message)) => format!(":\n{message}"),
                None if !self.config.with_state(|s| s.allow_writes()) => {
                    ": this Meerschaum does not write its config files here.".into()
                }
                None => ".".into(),
            };
            return SuccessTuple::fail(format!(
                "Failed to write the config of plugin '{plugin}'{reason}"
            ));
        }
        mark_plugin_config_written(&self.config, plugin);
        SuccessTuple::ok(format!("Wrote the config of plugin '{plugin}'."))
    }
}

/// A copy of `config` (its paths, defaults and in-memory config) for host calls that outlive
/// the borrow: an engine run's, a server's routes and pages.
pub fn host_config(
    config: &meerschaum_core::config::Config,
) -> Arc<meerschaum_core::config::Config> {
    Arc::new(duplicate_config(config))
}

pub(super) fn duplicate_config(
    config: &meerschaum_core::config::Config,
) -> meerschaum_core::config::Config {
    let mut state = config.with_state(|s| s.clone());
    let _ = state.take_notices();
    let copy = meerschaum_core::config::Config::bare(
        state.paths().clone(),
        1,
        cfg!(windows),
        state.allow_writes(),
    );
    copy.with_state(|s| *s = state);
    copy
}

/// The plugin config sections host calls wrote in this process, by config directory. Their
/// files are what later host calls read (see [`HostEnv::plugin_config`]).
pub(super) static WRITTEN_PLUGIN_CONFIGS: std::sync::Mutex<
    std::collections::BTreeSet<(std::path::PathBuf, String)>,
> = std::sync::Mutex::new(std::collections::BTreeSet::new());

pub(super) fn written_key(
    config: &meerschaum_core::config::Config,
    plugin: &str,
) -> (std::path::PathBuf, String) {
    (config.paths().config_dir, plugin.to_string())
}

pub(super) fn mark_plugin_config_written(config: &meerschaum_core::config::Config, plugin: &str) {
    WRITTEN_PLUGIN_CONFIGS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(written_key(config, plugin));
}

pub(super) fn written_plugin_config(
    config: &meerschaum_core::config::Config,
    plugin: &str,
) -> bool {
    WRITTEN_PLUGIN_CONFIGS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&written_key(config, plugin))
}

/// `plugins:<plugin>` as the config directory's `plugins` file has it (`MRSM{}` resolved).
pub(super) fn plugin_config_file(
    config: &meerschaum_core::config::Config,
    plugin: &str,
) -> Option<Value> {
    config.with_state(|s| {
        let read = s.read_config_lossless(None, Some(&["plugins".to_string()]), false, true, false);
        let _ = s.take_notices();
        read.ok()?
            .0
            .get("plugins")?
            .get(plugin)
            .map(meerschaum_core::config::ConfigValue::to_value)
    })
}

/// Whether native plugins wrote plugin config sections through host calls in this process that
/// [`take_written_plugin_configs`] has not taken yet.
pub fn has_written_plugin_configs() -> bool {
    !WRITTEN_PLUGIN_CONFIGS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_empty()
}

/// The plugin config sections native plugins wrote through host calls in this process into
/// `config`'s directory, with their values as written, forgotten here: for a host whose own
/// config lives elsewhere (the Python façade's live dictionary) to take them in.
pub fn take_written_plugin_configs(
    config: &meerschaum_core::config::Config,
) -> Vec<(String, serde_json::Value)> {
    let directory = config.paths().config_dir;
    let plugins: Vec<String> = {
        let mut written = WRITTEN_PLUGIN_CONFIGS
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let plugins = written
            .iter()
            .filter(|(dir, _)| *dir == directory)
            .map(|(_, plugin)| plugin.clone())
            .collect::<Vec<_>>();
        written.retain(|(dir, _)| *dir != directory);
        plugins
    };
    plugins
        .into_iter()
        .map(|plugin| {
            let value = plugin_config_file(config, &plugin)
                .and_then(|v| serde_json::to_value(v).ok())
                .unwrap_or(serde_json::Value::Null);
            (plugin, value)
        })
        .collect()
}

/// Run blocking work on a runtime thread: hand the worker off first (`block_in_place`) when
/// called from async code on a multi-threaded runtime.
pub(super) fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// The connectors of a [`HostEnv`]'s session as an engine registry: what a host sync outside
/// an engine run (an action, a route) writes through.
pub(super) struct SessionRegistry(HostEnv);

impl ConnectorRegistry for SessionRegistry {
    fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        self.0.connector(keys).map(|i| i.conn())
    }

    fn source(&self, keys: &str) -> Result<Option<Arc<dyn Connector>>, String> {
        if !matches!(keys.split(':').next(), Some("sql" | "api" | "valkey")) {
            return Ok(None);
        }
        let conn: Arc<dyn Connector> = self.instance(keys)?;
        Ok(Some(conn))
    }

    fn table_reference(&self, pipe: &Pipe) -> Result<String, String> {
        match self.0.connector(pipe.instance_keys()) {
            Ok(crate::native::Instance::Sql(sql)) => {
                let pipe = sql.sql_pipe(pipe).map_err(|e| e.to_string())?;
                Ok(sql.pipe_table_name(&pipe))
            }
            _ => pipe.target(None).map_err(|e| e.to_string()),
        }
    }
}

impl HostEnv {
    /// The current `meerschaum:connectors` section (see [`LiveConnectors`]).
    pub(super) fn live_connectors(&self) -> Arc<Map> {
        let mut live = self.connectors.lock().unwrap_or_else(|e| e.into_inner());
        let due = live
            .checked
            .is_none_or(|at| at.elapsed() >= CONNECTORS_CHECK);
        if due {
            live.checked = Some(std::time::Instant::now());
            let modified = self
                .config
                .connectors_file()
                .and_then(|path| std::fs::metadata(path).ok())
                .and_then(|meta| meta.modified().ok());
            if live.section.is_none() || modified != live.modified {
                if live.section.is_some() {
                    self.config.refresh_connectors();
                }
                live.modified = modified;
                live.section = None;
            }
        }
        if let Some(section) = &live.section {
            return section.clone();
        }
        let section = Arc::new(
            self.config
                .get(&["meerschaum", "connectors"])
                .map(|v| v.to_map())
                .unwrap_or_default(),
        );
        live.section = Some(section.clone());
        section
    }
}

impl meerschaum_server::backends::InstanceResolver for HostEnv {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        // A connector registered since the server started is in the files, not in memory yet.
        self.live_connectors();
        self.connector(keys).ok().map(|instance| instance.conn())
    }

    fn connectors(&self) -> Option<Arc<Map>> {
        Some(self.live_connectors())
    }
}
