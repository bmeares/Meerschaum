//! Bootstrap-only hosting of one team's ordinary API, behind signed executor control.
//! `network` holds the standalone network roles, and `recovery` the bounded intent kept for
//! work stopped by a controlled configuration rebuild.

use super::*;
use meerschaum_server::executor::control::Verifier;
use meerschaum_server::executor::gateway::{Configuration, Gateway, RuntimeError, RuntimeFactory};
use std::path::PathBuf;
use tokio::sync::Mutex;

mod network;
mod recovery;

#[derive(Clone)]
struct PluginBinding {
    team_id: uuid::Uuid,
    generation: uuid::Uuid,
    grants: std::collections::BTreeSet<String>,
}

struct Resources {
    plugins: PluginBinding,
    session: Session,
    jobs: meerschaum_jobs::daemon::JobsContext,
    monitor: meerschaum_jobs::monitor::RestartMonitor,
    terminal_watch: tokio::task::JoinHandle<()>,
    terminal_tmux: Option<meerschaum_server::webterm::TmuxSettings>,
}

/// Control never dispatches a user action in its credentialed process. Main submits the
/// command as a tracked job, whose bootstrap establishes its own signed launch profile.
struct ControlActions {
    catalog: Option<meerschaum_server::executor::plugins::Validated>,
}
#[async_trait]
impl meerschaum_server::backends::ActionsBackend for ControlActions {
    fn list(&self) -> Vec<String> {
        crate::actions::CORE_ACTIONS
            .iter()
            .map(|action| action.name.to_string())
            .chain(
                self.catalog
                    .as_ref()
                    .and_then(|catalog| catalog.action_names().ok())
                    .into_iter()
                    .flatten(),
            )
            .collect()
    }
    async fn execute(&self, _: &str, _: Map) -> SuccessTuple {
        SuccessTuple::fail(
            "Executor commands must run as tracked jobs with a signed execution context.",
        )
    }
}

/// Only native, explicitly configured instances enter the long-lived control runtime. All
/// construction happens on its blocking build task, never by importing a custom connector.
struct NativeInstances {
    instances: std::collections::BTreeMap<String, Arc<dyn meerschaum_connector::InstanceConnector>>,
    connectors: Arc<Map>,
}
impl NativeInstances {
    fn prepare(session: &Session, config: &Config) -> Self {
        let connectors = config
            .get(&["meerschaum", "connectors"])
            .map(|value| value.to_map())
            .unwrap_or_default();
        let mut instances = std::collections::BTreeMap::new();
        for kind in ["sql", "api", "valkey"] {
            for (label, attributes) in connectors
                .get(kind)
                .map(|value| value.to_map())
                .unwrap_or_default()
            {
                if label == "default" || attributes.as_map().is_none() {
                    continue;
                }
                let keys = format!("{kind}:{label}");
                if let Ok(instance) = session.instance(&keys, config) {
                    instances.insert(keys, instance.conn());
                }
            }
        }
        Self {
            instances,
            connectors: Arc::new(connectors),
        }
    }
}
impl meerschaum_server::backends::InstanceResolver for NativeInstances {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn meerschaum_connector::InstanceConnector>> {
        self.instances.get(keys).cloned()
    }
    fn connectors(&self) -> Option<Arc<Map>> {
        Some(self.connectors.clone())
    }
}

struct Factory {
    root: PathBuf,
    control_root: PathBuf,
    resources: Mutex<Option<Resources>>,
    terminals: Arc<meerschaum_server::executor::terminal::TerminalSessions>,
    contexts: Arc<crate::executor::contexts::Contexts>,
    recovery: Arc<recovery::Recovery>,
    factory_attestation: Option<Arc<crate::executor::factory::local::Installed>>,
}

struct BuildingSession(Option<Session>);
impl Drop for BuildingSession {
    fn drop(&mut self) {
        if let Some(session) = self.0.take() {
            session.shutdown();
        }
    }
}

/// Only currently granted source trees participate in native and Python discovery. A fresh
/// view prevents an old manifest or stale symlink from reintroducing a revoked plugin.
fn plugin_view(
    root: &std::path::Path,
    grants: &std::collections::BTreeSet<String>,
) -> Result<PathBuf, RuntimeError> {
    let sources = root.join("plugins");
    std::fs::create_dir_all(&sources).map_err(|_| RuntimeError)?;
    let view = root
        .join("executor-plugin-views")
        .join(uuid::Uuid::new_v4().simple().to_string());
    meerschaum_core::team_scope::create_private_dir(&view).map_err(|_| RuntimeError)?;
    for name in grants {
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
        {
            return Err(RuntimeError);
        }
        for filename in [name.clone(), format!("{name}.py")] {
            let source = sources.join(&filename);
            if !source.exists() {
                continue;
            }
            #[cfg(unix)]
            std::os::unix::fs::symlink(&source, view.join(&filename)).map_err(|_| RuntimeError)?;
            #[cfg(not(unix))]
            return Err(RuntimeError);
        }
    }
    Ok(view)
}

impl Factory {
    /// Only native startup may attach the administrator's immutable installation.
    fn with_source_compiler(
        root: PathBuf,
        control_root: PathBuf,
        compiler: Option<Arc<crate::executor::compiler::Compiler>>,
        control: &Verifier,
    ) -> Result<Self, String> {
        let installed =
            crate::executor::factory::local::load(control, compiler.as_deref(), &control_root)?;
        let mut factory = Self::new(root, control_root)?;
        factory.factory_attestation = installed.map(Arc::new);
        factory.contexts.set_factory_sdk(
            factory
                .factory_attestation
                .as_ref()
                .and_then(|installed| installed.manifest.sdk.clone()),
        )?;
        factory.contexts.set_source_compiler(compiler)?;
        Ok(factory)
    }

    fn new(root: PathBuf, control_root: PathBuf) -> Result<Self, String> {
        let contexts =
            crate::executor::contexts::Contexts::for_factory(root.clone(), control_root.clone())?;
        let root = std::fs::canonicalize(root).map_err(|_| "The Team root is unavailable.")?;
        let control_root = std::fs::canonicalize(control_root)
            .map_err(|_| "The Factory control root is unavailable.")?;
        Ok(Self {
            contexts: Arc::new(contexts),
            recovery: Arc::new(recovery::Recovery::new(&control_root)),
            root,
            control_root,
            resources: Mutex::new(None),
            terminals: Arc::new(Default::default()),
            factory_attestation: None,
        })
    }
    async fn stop(&self, rebuilding: bool) -> Result<Vec<String>, RuntimeError> {
        self.terminals.clear();
        meerschaum_server::webterm::stop_owned_servers();
        let resources = self.resources.lock().await.take();
        if let Some(resources) = &resources {
            resources.terminal_watch.abort();
        }
        let contexts = self.contexts.clone();
        let recovery = self.recovery.clone();
        let root = self.control_root.clone();
        tokio::task::spawn_blocking(move || {
            let context = match resources {
                Some(resources) => {
                    resources.session.shutdown();
                    drop(resources.monitor);
                    resources.jobs
                }
                None => meerschaum_jobs::daemon::JobsContext::for_root(&root)
                    .with_launch_resolver(contexts.clone()),
            };
            let mut stopped = Vec::new();
            let mut active = Vec::new();
            let mut pending = Vec::new();
            let mut failed = false;
            for name in meerschaum_jobs::daemon::get_daemon_ids(&context.paths) {
                let Ok(mut job) = meerschaum_jobs::job::Job::open(&name, &context) else {
                    failed = true;
                    continue;
                };
                let status = job.status();
                if status != meerschaum_jobs::Status::Stopped {
                    let properties = job.properties();
                    let mut captured_revision = None;
                    if properties.contains_key(
                        meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY,
                    ) {
                        if rebuilding
                            && let Some(revision) = contexts.job_revision(&name, &properties)
                        {
                            captured_revision = Some(revision);
                            pending.push((
                                name.clone(),
                                recovery::Intent {
                                    revision,
                                    stopped: false,
                                    paused: status == meerschaum_jobs::Status::Paused,
                                },
                            ));
                        }
                    } else {
                        stopped.push(name);
                    }
                    active.push((job, captured_revision));
                }
            }
            // A failed persistence must not leave work alive. Recovery stays disabled, but
            // shutdown and credential removal still run before reporting the failure.
            if rebuilding {
                failed |= recovery.capture(pending).is_err();
            }
            for (mut job, captured_revision) in active {
                // A one-off can complete while another job is being stopped. It was not
                // interrupted by this rebuild, so it must not inherit the restart intent.
                if job.status() == meerschaum_jobs::Status::Stopped {
                    if let Some(revision) = captured_revision {
                        failed |= recovery.cancel(&job.name, revision).is_err();
                    }
                    continue;
                }
                if !job.stop(None).success || job.status() != meerschaum_jobs::Status::Stopped {
                    failed = true;
                } else if let Some(revision) = captured_revision {
                    // A one-off which completed just before stop() records no manual stop.
                    // It must not be replayed merely because the earlier snapshot was active.
                    failed |= if job.stop_time().is_some() {
                        recovery.confirm_stopped(&job.name, revision).is_err()
                    } else {
                        recovery.cancel(&job.name, revision).is_err()
                    };
                }
            }
            failed |= contexts.clear(&context).is_err();
            if failed {
                Err(RuntimeError)
            } else {
                Ok(stopped)
            }
        })
        .await
        .map_err(|_| RuntimeError)?
    }
}

#[async_trait]
impl RuntimeFactory for Factory {
    async fn metadata_quote(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        configuration_sha256: &str,
        request: meerschaum_server::executor::factory_attestation::quote::Request,
    ) -> Result<meerschaum_server::executor::factory_attestation::quote::Response, RuntimeError>
    {
        let installed = self.factory_attestation.clone().ok_or(RuntimeError)?;
        {
            let resources = self.resources.lock().await;
            let plugins = &resources.as_ref().ok_or(RuntimeError)?.plugins;
            if plugins.team_id != installed.authority.pin().team_id
                || plugins.generation != installed.authority.pin().generation
            {
                return Err(RuntimeError);
            }
        }
        let contexts = self.contexts.clone();
        let actor = actor.clone();
        let configuration_sha256 = configuration_sha256.to_string();
        tokio::task::spawn_blocking(move || {
            contexts
                .metadata_quote(&actor, &request, &installed, &configuration_sha256)
                .map_err(|_| RuntimeError)
        })
        .await
        .map_err(|_| RuntimeError)?
    }

    async fn context_sources_available(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        update: &meerschaum_server::executor::environment::Update,
    ) -> Result<bool, RuntimeError> {
        if update.target == meerschaum_server::executor::environment::Target::Terminal {
            let grant = self.terminals.grant(update.id, actor).ok_or(RuntimeError)?;
            if grant.owner != update.owner || update.owner_id.as_deref() != Some(&grant.owner_id) {
                return Err(RuntimeError);
            }
        }
        let digest = if let Some(digest) = &update.discovery_catalog_sha256 {
            Some(digest.clone())
        } else if let Some(registration) = &update.registration {
            let meerschaum_server::executor::registration::Sources::Archives { catalog } =
                &registration.sources
            else {
                return Err(RuntimeError);
            };
            Some(meerschaum_server::executor::plugins::sha256(
                &serde_json::to_vec(catalog).map_err(|_| RuntimeError)?,
            ))
        } else {
            None
        };
        let Some(digest) = digest else {
            return Ok(true);
        };
        let root = self.control_root.clone();
        let contexts = self.contexts.clone();
        let actor = actor.clone();
        let update = update.clone();
        tokio::task::spawn_blocking(move || {
            contexts
                .check_update_identity(&actor, &update)
                .map_err(|_| RuntimeError)?;
            crate::executor::plugins::Store::new(&root)
                .map_err(|_| RuntimeError)?
                .catalog_present(&digest)
                .map_err(|_| RuntimeError)
        })
        .await
        .map_err(|_| RuntimeError)?
    }

    async fn update_context(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        update: meerschaum_server::executor::environment::Update,
    ) -> Result<(), RuntimeError> {
        use meerschaum_server::executor::environment::Target;
        if update.target == Target::Terminal {
            let grant = self.terminals.grant(update.id, actor).ok_or(RuntimeError)?;
            if grant.owner != update.owner || update.owner_id.as_deref() != Some(&grant.owner_id) {
                return Err(RuntimeError);
            }
        }
        let contexts = self.contexts.clone();
        let recovery = self.recovery.clone();
        let discovery =
            if update.discovery_catalog_sha256.is_some() || update.registration.is_some() {
                Some(
                    self.resources
                        .lock()
                        .await
                        .as_ref()
                        .ok_or(RuntimeError)?
                        .plugins
                        .clone(),
                )
            } else {
                None
            };
        let root = self.control_root.clone();
        let jobs = if matches!(update.target, Target::Job { .. }) {
            Some(
                self.resources
                    .lock()
                    .await
                    .as_ref()
                    .map(|r| r.jobs.clone())
                    .ok_or(RuntimeError)?,
            )
        } else {
            None
        };
        let actor = actor.clone();
        tokio::task::spawn_blocking(move || {
            let target = update.target.clone();
            let revision = update.revision;
            if let Some(binding) = discovery {
                let registration = update
                    .registration
                    .as_ref()
                    .map(|registration| {
                        let meerschaum_server::executor::registration::Sources::Archives {
                            catalog,
                        } = &registration.sources
                        else {
                            return Err(RuntimeError);
                        };
                        Ok(meerschaum_server::executor::plugins::sha256(
                            &serde_json::to_vec(catalog).map_err(|_| RuntimeError)?,
                        ))
                    })
                    .transpose()?;
                let digest = registration
                    .as_deref()
                    .or(update.discovery_catalog_sha256.as_deref())
                    .ok_or(RuntimeError)?;
                let reserved = crate::actions::CORE_ACTIONS
                    .iter()
                    .map(|action| action.name.to_string())
                    .collect();
                let store =
                    crate::executor::plugins::Store::new(&root).map_err(|_| RuntimeError)?;
                let catalog = store
                    .read_subset_catalog(
                        digest,
                        binding.team_id,
                        binding.generation,
                        &binding.grants,
                        &reserved,
                    )
                    .map_err(|_| RuntimeError)?;
                let sources = store.sources(&catalog).map_err(|_| RuntimeError)?;
                let sources = sources
                    .into_iter()
                    .map(|(name, path)| (name.into(), path))
                    .collect();
                if update.registration.is_some() {
                    contexts
                        .update_registration(&actor, update, catalog, sources)
                        .map_err(|_| RuntimeError)?;
                } else if update.profile
                    == meerschaum_server::executor::environment::Profile::Metadata
                {
                    contexts
                        .update_discovery(&actor, update, catalog, sources)
                        .map_err(|_| RuntimeError)?;
                } else if update.profile == meerschaum_server::executor::environment::Profile::Team
                {
                    contexts
                        .update_team_sources(&actor, update, catalog, sources)
                        .map_err(|_| RuntimeError)?;
                } else {
                    contexts
                        .update_api_sources(&actor, update, catalog, sources)
                        .map_err(|_| RuntimeError)?;
                }
            } else {
                contexts.update(&actor, update).map_err(|_| RuntimeError)?;
            }
            if let (Target::Job { name }, Some(jobs)) = (target, jobs)
                && let Some(intent) = recovery.take(&name, revision).map_err(|_| RuntimeError)?
                && !intent.paused
            {
                let mut job = meerschaum_jobs::Job::open(&name, &jobs).map_err(|_| RuntimeError)?;
                if !job.exists() || job.sysargs().is_empty() {
                    // Definitions are main-owned. Only an explicit main start may recreate
                    // missing work from the canonical stored command and source authority.
                    return Err(RuntimeError);
                }
                if job.status() == meerschaum_jobs::Status::Stopped && !job.start().success {
                    return Err(RuntimeError);
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| RuntimeError)?
    }

    async fn revoke_context(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        id: uuid::Uuid,
        revision: u64,
    ) -> Result<(), RuntimeError> {
        let contexts = self.contexts.clone();
        let recovery = self.recovery.clone();
        let actor_copy = actor.clone();
        let jobs = self
            .resources
            .lock()
            .await
            .as_ref()
            .map(|resources| resources.jobs.clone());
        tokio::task::spawn_blocking(move || {
            match jobs {
                Some(jobs) => {
                    contexts.revoke_job(&actor_copy, id, revision, &jobs)?;
                }
                None => {
                    contexts.revoke(&actor_copy, id, Some(revision))?;
                }
            }
            recovery
                .cancel(&format!("mrsm-{}", id.simple()), revision)
                .map_err(|e| e.to_string())?;
            Ok::<_, String>(())
        })
        .await
        .map_err(|_| RuntimeError)?
        .map_err(|_| RuntimeError)?;
        if let Some(name) = self.terminals.revoke(id, actor).map_err(|_| RuntimeError)? {
            self.end_terminal(&name).await;
        }
        Ok(())
    }

    fn terminal_sessions(
        &self,
    ) -> Option<Arc<meerschaum_server::executor::terminal::TerminalSessions>> {
        Some(self.terminals.clone())
    }

    async fn end_terminal(&self, name: &str) {
        let contexts = self.contexts.clone();
        let retired_name = name.to_string();
        let _ = tokio::task::spawn_blocking(move || contexts.retire_terminal(&retired_name)).await;
        let tmux = self.resources.lock().await.as_ref().and_then(|resources| {
            resources
                .terminal_tmux
                .clone()
                .map(|tmux| (tmux, resources.plugins.team_id))
        });
        if let Some((tmux, team)) = tmux {
            let name = name.to_string();
            let _ = tokio::task::spawn_blocking(move || {
                meerschaum_server::webterm::end_executor_terminal(&tmux, team, &name)
            })
            .await;
        }
    }
    async fn revoke(&self) {
        // Revocation/shutdown does not create new recovery intent. Existing inert intent from
        // a failed controlled build remains, without credentials, for an authorized retry.
        let _ = self.stop(false).await;
        // This role has only its own container/process's explicitly owned terminal servers.
        meerschaum_server::webterm::stop_owned_servers();
    }

    async fn deliver_plugins(
        &self,
        delivery: meerschaum_server::executor::plugin_delivery::Delivery,
    ) -> Result<String, RuntimeError> {
        let reserved = crate::actions::CORE_ACTIONS
            .iter()
            .map(|action| action.name.to_string())
            .collect();
        let catalog = delivery
            .validate(
                delivery.catalog.team_id,
                delivery.catalog.generation,
                &reserved,
            )
            .map_err(|_| RuntimeError)?;
        let expected = catalog.digest().map_err(|_| RuntimeError)?;
        let credentials = delivery.credentials;
        struct Authentication(
            std::collections::BTreeMap<
                String,
                meerschaum_server::executor::plugin_delivery::Credential,
            >,
        );
        impl meerschaum_connector_api::client::authentication::RequestAuthentication for Authentication {
            fn authorization(
                &self,
                request: meerschaum_connector_api::client::authentication::Request<'_>,
            ) -> meerschaum_connector_api::traits::ConnectorResult<String> {
                let invalid = || {
                    meerschaum_connector_api::traits::ConnectorError::Invalid(
                        "The exact plugin pull capability is unavailable.".into(),
                    )
                };
                let name = request
                    .path
                    .strip_prefix("/plugins/")
                    .filter(|name| meerschaum_server::executor::plugins::valid_name(name))
                    .ok_or_else(invalid)?;
                let credential = self
                    .0
                    .get(name)
                    .filter(|credential| credential.expires_at > chrono::Utc::now().timestamp())
                    .ok_or_else(invalid)?;
                if request.method != "GET" {
                    return Err(invalid());
                }
                Ok(credential.authorization().to_string())
            }
        }
        let repository = crate::executor::plugins::Repository::new(
            &delivery.repository_origin,
            Arc::new(Authentication(credentials)),
        )
        .map_err(|_| RuntimeError)?;
        let root = self.control_root.clone();
        let store =
            tokio::task::spawn_blocking(move || crate::executor::plugins::Store::new(&root))
                .await
                .map_err(|_| RuntimeError)?
                .map_err(|_| RuntimeError)?;
        store
            .pull(&repository, &catalog)
            .await
            .map_err(|_| RuntimeError)?;
        Ok(expected)
    }

    async fn build(&self, configuration: Configuration) -> Result<axum::Router, RuntimeError> {
        let stopped = self.stop(true).await?;
        let root = self.root.clone();
        let control_root = self.control_root.clone();
        let terminals = self.terminals.clone();
        let contexts = self.contexts.clone();
        let handle = tokio::runtime::Handle::current();
        let (router, resources) = tokio::task::spawn_blocking(move || {
            let plugin_binding = PluginBinding {team_id:configuration.team_id,generation:configuration.generation,grants:configuration.plugins.clone()};
            let reserved = crate::actions::CORE_ACTIONS.iter().map(|action|action.name.to_string()).collect();
            let catalog = configuration.catalog_sha256.as_ref().map(|digest| {
                crate::executor::plugins::Store::new(&control_root)?.read_catalog(digest,meerschaum_server::executor::plugins::Expected {
                    team_id:configuration.team_id,generation:configuration.generation,grants:&configuration.plugins,reserved_actions:&reserved,
                })
            }).transpose().map_err(|_|RuntimeError)?;
            let plugin_metadata_pending = !configuration.plugins.is_empty() && catalog.as_ref().is_none_or(|catalog|!catalog.complete());
            let plugin_view = match &catalog {
                Some(catalog) => crate::executor::plugins::Store::new(&control_root).map_err(|_|RuntimeError)?.view(catalog).map_err(|_|RuntimeError)?.directory,
                None => plugin_view(&control_root, &configuration.plugins)?,
            };
            let config_directory = control_root.join("config");
            meerschaum_core::team_scope::create_private_dir(&config_directory).map_err(|_|RuntimeError)?;
            let mut env: Vec<(String, String)> = meerschaum_core::process_env::vars()
                .into_iter()
                .filter(|(key, _)| {
                    (meerschaum_core::team_scope::is_allowed(key)
                        || matches!(
                            key.as_str(),
                            "CARGO_HOME" | "RUSTUP_HOME" | "PIP_CACHE_DIR" | "UV_CACHE_DIR"
                        ))
                        && !matches!(
                            key.as_str(),
                            "MRSM_ROOT_DIR"
                                | "MRSM_PLUGINS_DIR"
                                | "MRSM_EXECUTOR_PLUGINS"
                                | "MRSM_CONFIG_DIR"
                                | "MRSM_CONFIG"
                                | "MRSM_CONFIG_FILE"
                                | "MRSM_PATCH"
                                | "MRSM_PATCH_FILE"
                                | "TMUX"
                                | "TMUX_PANE"
                        )
                })
                .chain([
                    ("MRSM_ROOT_DIR".into(), root.display().to_string()),
                    ("MRSM_PLUGINS_DIR".into(), plugin_view.display().to_string()),
                    ("MRSM_CONFIG_DIR".into(), config_directory.display().to_string()),
                    (
                        "MRSM_EXECUTOR_PLUGINS".into(),
                        serde_json::to_string(&configuration.plugins).map_err(|_| RuntimeError)?,
                    ),
                ])
                .collect();
            // Generated containers give the Factory a private home. Team processes retain
            // their durable home and user-installed commands after confinement.
            if env.iter().any(|(key, value)| key == "HOME" && std::path::Path::new(value) == control_root) {
                for (key, value) in &mut env {
                    match key.as_str() {
                        "HOME" => *value = root.display().to_string(),
                        "PATH" => *value = format!("{value}:{}/.local/bin", root.display()),
                        _ => {}
                    }
                }
            }
            let mut options = ConfigOptions::with_env(env.clone());
            options.allow_writes = true;
            let paths = meerschaum_core::config::Paths::from_env(
                &|name| options.env.iter().find(|(key, _)| key == name).map(|(_, value)| value.clone()),
                &options.home, options.windows, options.package_root.clone(),
            ).map_err(|_|RuntimeError)?;
            let config = Config::bare(paths, options.cpu_count, options.windows, true);
            config.set_config(configuration.config);
            if let Some(catalog) = &catalog {
                let mut snapshot = config.snapshot();
                snapshot.insert(meerschaum_server::executor::plugins::CONFIG_KEY.into(),
                    meerschaum_core::Value::from_json_str(&String::from_utf8(catalog.encode().map_err(|_|RuntimeError)?).map_err(|_|RuntimeError)?)
                        .map_err(|_|RuntimeError)?);
                config.set_config(snapshot);
            }
            contexts
                .configure_runtime(config.snapshot(), env.clone())
                .map_err(|_| RuntimeError)?;
            // The Factory never reloads configuration from a Team-writable directory.
            if !config.write_config(Some(&config.snapshot()), None) {
                return Err(RuntimeError);
            }
            let session = Session::with_runtime(handle);
            // An API server can retain its team database credentials. Neither Python imports
            // nor dlopen of a granted native plugin may happen in that credentialed process.
            session.set_python_worker(false);
            let mut building_session = BuildingSession(Some(session.clone()));
            session
                .set_worker_environment(env.clone())
                .map_err(|_| RuntimeError)?;
            let mut server_config = ServerConfig::from_config(
                &config,
                &ServerFlags {
                    no_auth: true,
                    ..Default::default()
                },
            );
            // Tmux sockets, environment files and API control state are parent-trusted too.
            // Keep them outside the Team's durable recursive write grant.
            let control_paths = meerschaum_core::config::Paths::for_root(&control_root);
            server_config.root_dir = control_root.clone();
            server_config.secret_key_path = control_paths.path("API_SECRET_KEY_PATH");
            server_config.grafana_password_path = control_paths.path("API_CONFIG_RESOURCES_PATH")
                .join(meerschaum_server::config::GRAFANA_PASSWORD_FILENAME);
            server_config.plugins_archives_path = control_paths.path("PLUGINS_ARCHIVES_RESOURCES_PATH");
            server_config.webterm_resources_path = control_paths.path("WEBTERM_INTERNAL_RESOURCES_PATH");
            server_config.webterm_tmux_config = None;
            let instance = session
                .instance(&server_config.instance_keys, &config)
                .map_err(|_| RuntimeError)?;
            let instances = Arc::new(NativeInstances::prepare(&session, &config));
            let mut output = std::io::sink();
            let mut runtime = Runtime::system(&mut output);
            runtime.native = Some(session.clone());
            runtime.config = Some(config);
            let jobs_context = meerschaum_jobs::JobsContext::new(
                meerschaum_jobs::JobPaths::for_root(&control_root, control_root.join("systemd-config")),
                meerschaum_jobs::JobsConfig::from_config(runtime.config()),
                {
                    let native = meerschaum_core::self_exe::native_executable()
                        .ok_or(RuntimeError)?.to_string_lossy().into_owned();
                    meerschaum_jobs::Launcher {
                        program: vec![native.clone()],
                        supervisor: vec![native, meerschaum_jobs::supervisor::SUPERVISOR_ARG.into()],
                    }
                },
            )
                .with_launch_environment(env.clone())
                .with_launch_resolver(contexts.clone());
            let jobs = Arc::new(crate::jobs_backend::LocalJobsBackend::new(
                jobs_context.clone(),
            ));
            let state = AppState::builder(server_config, instance.conn())
                .resolver(instances)
                .actions(Arc::new(ControlActions {catalog}))
                .jobs(jobs)
                .engine(Arc::new(
                    meerschaum_server::engine_store::InstanceEngine::new(
                        instance.conn(),
                        &instance.keys(),
                    ),
                ))
                .build()
                .map_err(|_| RuntimeError)?;
            let mut shell = meerschaum_server::webterm::ShellCommand::current_exe_shell(
                &state.config.instance_keys,
            )
            .map_err(|_| RuntimeError)?;
            shell.clear_env = true;
            shell.env = env;
            shell.cwd = Some(root.clone());
            let terminal_config = meerschaum_server::webterm::WebtermConfig::new(
                &state,
                Arc::new(crate::executor::contexts::Sessions {
                    terminals: terminals.clone(),
                    contexts: contexts.clone(),
                }),
                Arc::new(|_| String::new()),
                shell,
            );
            let terminal_tmux = terminal_config.tmux.clone();
            let terminal_routes =
                meerschaum_server::webterm::executor_routes(state.clone(), terminal_config);
            let router = meerschaum_server::app(state).merge(terminal_routes).layer(
                axum::middleware::from_fn(move |request: axum::extract::Request, next: axum::middleware::Next| async move {
                    use axum::response::IntoResponse;
                    let path = request.uri().path();
                    if request.method() == axum::http::Method::GET && path == "/actions" && plugin_metadata_pending {
                        return meerschaum_server::error::ApiError::unavailable("Confined plugin action metadata is not available on this executor yet. Submit an authorized tracked job.").into_response();
                    }
                    if path == "/dash" || path.starts_with("/dash/") {
                        return meerschaum_server::error::ApiError::unavailable("Executor plugin pages and console metadata require a confined plugin host; use the main API console.").into_response();
                    }
                    next.run(request).await
                }),
            );
            let monitor = meerschaum_jobs::monitor::spawn(
                jobs_context.clone(),
                meerschaum_jobs::monitor::default_interval(),
            )
            .map_err(|_| RuntimeError)?;
            let sweep_tmux = terminal_tmux.clone();
            let sweep_team = configuration.team_id;
            let sweep_contexts = contexts;
            let sweep_jobs = jobs_context.clone();
            let terminal_watch = tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tick.tick().await;
                    let contexts = sweep_contexts.clone();
                    let jobs = sweep_jobs.clone();
                    let _ = tokio::task::spawn_blocking(move || contexts.expire_jobs(&jobs)).await;
                    let names = terminals.expired();
                    for name in &names {
                        sweep_contexts.retire_terminal(name);
                    }
                    if let Some(tmux) = sweep_tmux.clone()
                        && !names.is_empty()
                    {
                        let _ = tokio::task::spawn_blocking(move || {
                            for name in names {
                                meerschaum_server::webterm::end_executor_terminal(&tmux, sweep_team, &name);
                            }
                        })
                        .await;
                    }
                }
            });
            // Unmarked legacy jobs retain their existing rebuild behavior. Marked jobs wait
            // for fresh main authority to consume a persisted matching-revision intent.
            for name in stopped {
                if let Ok(mut job) = meerschaum_jobs::job::Job::open(&name, &jobs_context) {
                    let _ = job.start();
                }
            }
            building_session.0 = None;
            Ok((
                router,
                Resources {
                    plugins: plugin_binding,
                    session,
                    jobs: jobs_context,
                    monitor,
                    terminal_watch,
                    terminal_tmux,
                },
            ))
        })
        .await
        .map_err(|_| RuntimeError)??;
        *self.resources.lock().await = Some(resources);
        Ok(router)
    }
}

/// Serve a native team executor. Bootstrap contains a public verifier only; native instance
/// connections open after signed configuration. Plugins execute inside confined launches.
pub fn start_executor(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    use std::io::Read;
    if meerschaum_core::self_exe::native_executable().is_none() {
        return Err(ActionError::new(
            "ExecutionUnavailable",
            "Executor roles require the standalone native mrsm binary. Use this release's full image or an administrator-configured native executor image; a Python console entry point cannot establish confinement before interpreter startup.",
        ));
    }
    if let Some(parameters) = native::params(args)
        && let Some(role) = parameters
            .get("role")
            .and_then(meerschaum_core::Value::as_str)
    {
        return start_network_role(role, &parameters, args, rt);
    }
    let bootstrap = std::env::var_os("MRSM_EXECUTOR_BOOTSTRAP").ok_or_else(|| {
        ActionError::new(
            "ValueError",
            "Set MRSM_EXECUTOR_BOOTSTRAP to the executor's public bootstrap file.",
        )
    })?;
    let mut bytes = Vec::new();
    std::fs::File::open(bootstrap)
        .map_err(io_error)?
        .take(64 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() > 64 * 1024 {
        return Err(ActionError::new(
            "ValueError",
            "Executor bootstrap exceeds its size limit.",
        ));
    }
    let verifier: Verifier = serde_json::from_slice(&bytes)
        .map_err(|_| ActionError::new("ValueError", "Invalid executor public bootstrap."))?;
    verifier.validate().map_err(io_error)?;
    let flags = flags(args)?;
    let host = flags.host.unwrap_or_else(|| "0.0.0.0".into());
    let port = flags.port.unwrap_or(8989);
    let source_compiler = crate::executor::compiler::provisioning::load()
        .map_err(|message| ActionError::new("ExecutionUnavailable", message))?;
    let control_root = native::string(args, "control_root")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            meerschaum_core::team_scope::source_namespace::FACTORY_CONTROL_ROOT.into()
        });
    let options = ConfigOptions::from_process();
    let paths = meerschaum_core::config::Paths::from_env(
        &|name| (name == "MRSM_ROOT_DIR").then(|| (rt.env)(name)).flatten(),
        &options.home,
        options.windows,
        None,
    )
    .map_err(io_error)?;
    let factory = Arc::new(
        Factory::with_source_compiler(paths.root_dir, control_root, source_compiler, &verifier)
            .map_err(|message| ActionError::new("ExecutionUnavailable", message))?,
    );
    rt.config = Some(Config::bare(
        meerschaum_core::config::Paths::for_root(&factory.control_root),
        options.cpu_count,
        options.windows,
        false,
    ));
    let gateway = Gateway::new(verifier, factory.clone())
        .map_err(io_error)?
        .router();
    native::info(
        rt,
        &format!("Team executor listening on {host}:{port}; awaiting signed configuration."),
    );
    crate::native::jobs::own_signals();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(io_error)?;
    let result = runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind((host.as_str(), port))
            .await
            .map_err(io_error)?;
        let mut shutdown = ShutdownSignal::new().map_err(io_error)?;
        let result = tokio::select! {
            result = axum::serve(listener, gateway).into_future() => result.map_err(io_error),
            result = shutdown.wait() => result.map_err(io_error),
        };
        factory.revoke().await;
        result.map(|()| SuccessTuple::success())
    });
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

fn start_network_role(
    role: &str,
    parameters: &Map,
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
) -> ActionResult {
    network::start(role, parameters, args, rt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_connector_api::APIConnector;
    use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
    use meerschaum_connector_api::client::{Body, Method, RequestOptions};
    use meerschaum_connector_api::traits::{ConnectorError, ConnectorResult};
    use meerschaum_server::executor::control::{Actor, Authority};
    use meerschaum_server::executor::gateway::{CONFIGURATION_PATH, HEALTH_PATH};

    struct Signer {
        authority: Authority,
        revision: String,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn executor_controller_updates_grants_without_stack_reconciliation_and_never_falls_back()
    {
        crate::executor::contexts::native_model_host().unwrap();
        use crate::executor_client::Controller;
        use crate::executor_registry::Registry;
        use meerschaum_core::access::Team;
        let main_root = tempfile::tempdir().unwrap();
        let executor_root = tempfile::tempdir().unwrap();
        std::fs::create_dir(main_root.path().join("plugins")).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let snapshot = serde_json::json!({"meerschaum": {"instance": "sql:main", "api_instance": "sql:main", "connectors": {
            "sql": {"main": {"flavor": "sqlite", "database": main_root.path().join("main.db").display().to_string(), "password": "main-private-password"},
                "team": {"flavor": "sqlite", "database": executor_root.path().join("work/team.db").display().to_string()}},
            "api": {"executor": {"host": "127.0.0.1", "port": port}}}}, "api": {"no_dash": true}});
        let snapshot = meerschaum_core::Value::from_json_str(&snapshot.to_string())
            .unwrap()
            .to_map();
        let options = ConfigOptions::with_env(vec![
            (
                "MRSM_ROOT_DIR".into(),
                main_root.path().display().to_string(),
            ),
            (
                "MRSM_PLUGINS_DIR".into(),
                main_root.path().join("plugins").display().to_string(),
            ),
        ]);
        let config = Config::new(options.clone()).unwrap();
        config.set_config(snapshot.clone());
        let session = Session::with_runtime(tokio::runtime::Handle::current());
        let instance = tokio::task::spawn_blocking(move || {
            session.instance("sql:main", &config).unwrap().conn()
        })
        .await
        .unwrap();
        let mut team = Team::new("Operators");
        team.attributes = meerschaum_core::Value::from_json_str(r#"{"isolated":true,"plugins_write":false,"instance":"sql:team","connectors":["sql:team"],"execution":{"mode":"api","connector":"api:executor"}}"#).unwrap().to_map();
        let registered = instance.register_team(&mut team).await;
        assert!(registered.success, "{}", registered.message);
        let desired = Registry::new(main_root.path())
            .reconcile(&[team.clone()])
            .unwrap();
        let verifier = desired.teams.values().next().unwrap().verifier.clone();
        let factory = Arc::new(
            Factory::new(
                executor_root.path().join("work"),
                executor_root.path().join("control"),
            )
            .unwrap(),
        );
        let router = Gateway::new(verifier, factory.clone()).unwrap().router();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let controller = Arc::new(Controller::new(
            main_root.path().into(),
            instance.clone(),
            options,
            snapshot,
        ));
        let result = async {
            let api = controller.prepare(&team).await.unwrap();
            let mut limited = meerschaum_server::access::Access::everything();
            limited.no_auth = false;
            limited.principal.permissions = ["jobs:read".into()].into_iter().collect();
            limited.primary_team = team.name.clone();
            let selection = meerschaum_server::executor::context::scope(
                Some(Arc::new(limited)),
                controller.selected(None),
            )
            .await
            .unwrap();
            assert_eq!(selection.and_then(|t| t.id), team.id);
            assert!(executor_root.path().join("work/team.db").is_file());
            let files = std::fs::read_dir(executor_root.path().join("control/config")).unwrap();
            for file in files {
                let file = file.unwrap();
                if file.file_type().unwrap().is_file() {
                    let text = std::fs::read_to_string(file.path()).unwrap();
                    assert!(!text.contains("main-private-password"));
                }
            }
            team.attributes.insert(
                "connectors".into(),
                meerschaum_core::Value::List(Vec::new()),
            );
            team.attributes.shift_remove("instance");
            let changed = instance.edit_team(&team).await;
            assert!(changed.success, "{}", changed.message);
            assert!(controller.prepare(&team).await.is_err());
            assert_eq!(
                api.get("/jobs", RequestOptions::default())
                    .await
                    .unwrap()
                    .status,
                503
            );
            let deleted = instance.delete_team(&team.name).await;
            assert!(deleted.success, "{}", deleted.message);
            assert!(controller.prepare(&team).await.is_err());
        };
        let outcome =
            futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(result)).await;
        factory.revoke().await;
        server.abort();
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }

    impl RequestAuthentication for Signer {
        fn authorization(&self, request: Request<'_>) -> ConnectorResult<String> {
            self.authority
                .issue(
                    Actor {
                        owner: "operator".into(),
                        username: Some("operator".into()),
                        permissions: Default::default(),
                        administrator: false,
                    },
                    meerschaum_server::executor::control::Request {
                        method: request.method,
                        path: request.path,
                        body: request
                            .body
                            .ok_or_else(|| ConnectorError::Invalid("Unbuffered body.".into()))?,
                    },
                    &self.revision,
                    chrono::Utc::now().timestamp(),
                )
                .map(|token| format!("Bearer {token}"))
                .map_err(|_| ConnectorError::Invalid("Could not sign executor request.".into()))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_executor_bootstraps_and_overwrites_untrusted_job_ownership() {
        crate::executor::contexts::native_model_host().unwrap();
        let root = tempfile::tempdir().unwrap();
        let team_id = uuid::Uuid::new_v4();
        let generation = uuid::Uuid::new_v4();
        let (authority, _) =
            Authority::generate(uuid::Uuid::new_v4(), team_id, generation).unwrap();
        let config = serde_json::json!({
            "meerschaum": {"instance": "sql:executor", "api_instance": "sql:executor",
                "connectors": {"sql": {"executor": {"flavor": "sqlite", "database": root.path().join("work/executor.db").display().to_string()}}}},
            "system": {"connectors": {"sql": {"create_engine": {"connect_args": {"timeout": 3}}}},
                "packages": {"auto_install": false}},
            "api": {"no_dash": true},
        });
        let configuration = Configuration {
            catalog_sha256: None,
            team_id,
            team_name: "Operators".into(),
            generation,
            plugins: Default::default(),
            config: meerschaum_core::Value::from_json_str(&config.to_string())
                .unwrap()
                .to_map(),
        };
        let factory =
            Arc::new(Factory::new(root.path().join("work"), root.path().join("control")).unwrap());
        let router = Gateway::new(authority.verifier(), factory.clone())
            .unwrap()
            .router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let attributes = serde_json::json!({"host": "127.0.0.1", "port": port});
        let api = APIConnector::new(
            Some("executor"),
            meerschaum_core::Value::from_json_str(&attributes.to_string())
                .unwrap()
                .to_map(),
        )
        .unwrap()
        .with_request_authentication(
            Arc::new(Signer {
                revision: configuration.fingerprint().unwrap(),
                authority,
            }),
            1024 * 1024,
        )
        .unwrap();
        let result = async {
            let response = api
                .make_request(
                    Method::Put,
                    CONFIGURATION_PATH,
                    RequestOptions {
                        body: Body::Json(serde_json::to_string(&configuration).unwrap()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(response.status, 200, "{}", response.text());
            let health = api
                .get(HEALTH_PATH, RequestOptions::default())
                .await
                .unwrap()
                .json()
                .unwrap();
            assert_eq!(health["ready"], true);
            assert_eq!(health["team_id"], team_id.to_string());
            let created = api
                .create_job(
                    "executor-owned",
                    &["show".into(), "version".into()],
                    Some(
                        meerschaum_core::Value::from_json_str(
                            r#"{"owner":"attacker","team":"Another","mrsm_owner":"attacker"}"#,
                        )
                        .unwrap()
                        .to_map(),
                    ),
                )
                .await;
            assert!(created.success, "{}", created.message);
            let properties = api.get_job_properties("executor-owned").await.unwrap();
            let json = serde_json::to_value(properties).unwrap();
            assert_eq!(json["owner"], "operator", "{json}");
            assert_eq!(json["team"], "Operators", "{json}");
            assert!(root.path().join("work/executor.db").is_file());
        };
        // Cleanup also runs if the assertions panic, before the temporary root is removed.
        let outcome =
            futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(result)).await;
        factory.revoke().await;
        server.abort();
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}
