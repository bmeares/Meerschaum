//! CLI hosting of the Rust HTTP and MCP servers, with the supervised Python API host.
//! The `executor` child hosts a team executor process (`mrsm start executor`); `native_backend`
//! is the backend both servers call, with its actions in `actions_backend` and its MCP tools in
//! `mcp_backend`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::{Map, SuccessTuple};
use meerschaum_server::host::{
    FileManifest, HostEndpoint, HostLauncher, HostManifest, HostProcess, HostSupervisor,
    PythonHostLauncher, SupervisorOptions, WithConsole, default_socket_path, probe_console,
    spawn_manifest_watcher,
};
use meerschaum_server::mcp::McpServer;
use meerschaum_server::{AppState, ServerConfig, ServerFlags};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::native::{self, Session};
use crate::value::{ArgsMap, PyValue};

mod actions_backend;
mod executor;
mod mcp_backend;
mod native_backend;
pub use executor::start_executor;
use native_backend::NativeBackend;

// Plugin imports may print to stdout. Keep the MCP protocol channel clean even when the
// supervised API host runs Python plugins with ordinary print() diagnostics.
// The host is a `HostChild`: its stdin is a pipe this process holds, so a host whose server is
// killed (SIGKILL, the OOM killer) exits instead of running on as an orphan.
struct StderrHostLauncher(PythonHostLauncher, Option<Vec<(String, String)>>);

#[async_trait]
impl HostLauncher for StderrHostLauncher {
    async fn launch(&self, socket: &std::path::Path) -> std::io::Result<Box<dyn HostProcess>> {
        self.launch_endpoint(&HostEndpoint::from(socket)).await
    }

    async fn launch_endpoint(
        &self,
        endpoint: &HostEndpoint,
    ) -> std::io::Result<Box<dyn HostProcess>> {
        let mut command = tokio::process::Command::new(&self.0.python);
        if let Some(environment) = &self.1 {
            command.env_clear().envs(environment.iter().cloned());
        }
        let mut child = meerschaum_server::host::HostChild::spawn(
            command
                .args([
                    meerschaum_core::self_exe::PYTHON_SAFE_PATH_FLAG,
                    "-m",
                    "meerschaum.api.host",
                ])
                .args(PythonHostLauncher::endpoint_args(endpoint))
                .envs(PythonHostLauncher::endpoint_env(endpoint))
                .envs(self.0.env.iter().map(|(key, value)| (key, value)))
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::inherit())
                .kill_on_drop(true),
        )?;
        if let Some(mut stdout) = child.child_mut().stdout.take() {
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut stdout, &mut tokio::io::stderr()).await;
            });
        }
        Ok(Box::new(child))
    }
}

/// Forwards the events of the engine runs in the server process to its run event streams.
struct LiveRunEvents(AppState);

impl meerschaum_engine::ProcessEventSink for LiveRunEvents {
    fn emit(&self, home_instance: &str, event: meerschaum_engine::Event) {
        if self.0.engine_streams.is_followed()
            && let Ok(json) = serde_json::to_value(&event)
        {
            self.0.engine_streams.publish_json(home_instance, &json);
        }
    }
}

fn io_error(error: impl std::fmt::Display) -> ActionError {
    ActionError::new("OSError", error.to_string())
}

/// Translate parsed v4 flags to the server's configuration.
pub fn flags(args: &ArgsMap) -> Result<ServerFlags, ActionError> {
    let port = native::int(args, "port")
        .map(|port| u16::try_from(port).map_err(io_error))
        .transpose()?;
    Ok(ServerFlags {
        instance: native::string(args, "mrsm_instance"),
        host: native::string(args, "host"),
        port,
        production: native::flag(args, "production"),
        secure: native::flag(args, "secure"),
        private: native::flag(args, "private"),
        no_auth: native::flag(args, "no_auth"),
        debug: native::flag(args, "debug"),
        no_dash: native::flag(args, "no_dash"),
        no_webterm: native::flag(args, "no_webterm"),
        public: native::flag(args, "public"),
        keyfile: native::string(args, "keyfile"),
        certfile: native::string(args, "certfile"),
    })
}

/// Start the HTTP API and `/mcp` until interrupted.
pub fn start_api(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    start(args, rt, false)
}

/// `start webterm`. v4 ran a Tornado terminal process next to the API; v5 serves the terminal
/// from the API server itself (`/dash/webterm`, `/webterm/<session>`), authenticated through the
/// console session, so there is nothing to start on its own.
pub fn start_webterm(_args: &ArgsMap, _rt: &mut Runtime<'_>) -> ActionResult {
    Ok(SuccessTuple::fail(
        "The web terminal is part of the API server in v5: run `start api` and open \
         `/dash/webterm` (or `/webterm/<session id>`). There is no separate terminal process; \
         `--no-webterm` turns the terminal off.",
    ))
}

/// Start newline-delimited MCP on stdio. The dispatcher's final tuple must go to stderr.
pub fn start_mcp(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    start(args, rt, true)
}

/// Only repair known dependencies when the Python facade is present. A missing interpreter,
/// failed probe, or a Rust-only installation must remain a usable standalone API server.
fn console_dependencies(probe: &meerschaum_server::host::ConsoleProbe) -> Vec<String> {
    if probe.available || probe.missing.iter().any(|name| name == "meerschaum") {
        return Vec::new();
    }
    if probe
        .missing
        .iter()
        .any(|name| !meerschaum_server::host::CONSOLE_MODULES.contains(&name.as_str()))
    {
        return Vec::new();
    }
    probe
        .missing
        .iter()
        .filter_map(|name| {
            native::packages::ALL_PACKAGES
                .iter()
                .rev()
                .find(|(import, _)| import == name)
                .map(|(_, install)| (*install).to_string())
        })
        .collect()
}

/// HTTPS for `start api` (`--keyfile` / `--certfile`, or `ssl_keyfile` / `ssl_certfile` under
/// `api:server`, or 4.1's `api:uvicorn`): the key and certificate are checked before anything
/// else starts, so a missing half, an unreadable file or a bad PEM stops `start api` with the
/// reason instead of a server that cannot answer `https://` clients. `Ok(None)`: plain HTTP.
/// The address of the internal listener goes into `config` for the API host.
pub fn prepare_https(
    config: &mut ServerConfig,
) -> Result<Option<meerschaum_server::tls::Https>, String> {
    meerschaum_server::tls::prepare(config).map_err(|e| e.to_string())
}

/// Print the server's certificate messages (a reload, a failed reload) the way the CLI prints its
/// own: information on stdout, warnings on stderr, styled for each stream (`config` gives the
/// styles; without one the text is plain).
fn tls_events(rt: &mut Runtime<'_>, config: Option<Config>) -> meerschaum_server::tls::TlsEvents {
    let (out, err) = (rt.settings(), rt.err_settings());
    Arc::new(move |event| {
        use meerschaum_server::tls::TlsEvent;
        use std::io::Write as _;
        let render = |settings: &crate::format::FormatSettings| match (&config, &event) {
            (Some(config), TlsEvent::Info(m)) => {
                crate::format::Formatter::new(settings.clone(), config).info(m, true)
            }
            (Some(config), TlsEvent::Warning(m)) => {
                crate::format::Formatter::new(settings.clone(), config).warn(m)
            }
            (None, event) => format!("{}\n", event.message()),
        };
        match &event {
            TlsEvent::Info(_) => {
                let mut stdout = std::io::stdout().lock();
                let _ = stdout.write_all(render(&out).as_bytes());
                let _ = stdout.flush();
            }
            TlsEvent::Warning(_) => {
                let mut stderr = std::io::stderr().lock();
                let _ = stderr.write_all(render(&err).as_bytes());
                let _ = stderr.flush();
            }
        }
    })
}

fn start(args: &ArgsMap, rt: &mut Runtime<'_>, stdio: bool) -> ActionResult {
    // The server waits on SIGINT / SIGTERM itself (`ShutdownSignal`): the spinner of its startup
    // must not take them.
    crate::native::jobs::own_signals();
    // A server prints nothing between requests: the API client's warnings go to `tracing`.
    crate::native::api_warnings::uninstall();
    let mut config = ServerConfig::from_config(rt.config(), &flags(args)?);
    // Before anything else starts (the console probe, the API host): MCP over stdio has no
    // listener, so it never serves HTTPS.
    let https = if stdio {
        None
    } else {
        match prepare_https(&mut config) {
            Ok(https) => https,
            Err(refusal) => return Ok(SuccessTuple::fail(refusal)),
        }
    };
    // Installation policy is loaded before any startup manifest/source discovery. A
    // configured invalid snapshot never silently becomes ambient/private compilation.
    let source_compiler = crate::executor::compiler::provisioning::load()
        .map_err(|message| ActionError::new("ExecutionUnavailable", message))?;
    let session = native::session(rt);
    let plugin_manifest = session.manifest(rt.config())?;
    let instance = session.instance(&config.instance_keys, rt.config())?;
    let mut options = ConfigOptions::from_process();
    options.env.retain(|(key, _)| key != "MRSM_ROOT_DIR");
    options.env.push((
        "MRSM_ROOT_DIR".into(),
        config.root_dir.display().to_string(),
    ));
    let executors = Arc::new(crate::executor::client::Controller::new(
        config.root_dir.clone(),
        instance.conn(),
        options.clone(),
        rt.config().snapshot(),
    ));
    let backend = Arc::new(NativeBackend {
        http: !stdio,
        options,
        snapshot: rt.config().snapshot(),
        session: session.clone(),
        executors: Some(executors.clone()),
        plugin_actions: plugin_manifest
            .plugins
            .values()
            .flat_map(|plugin| plugin.actions.iter().map(|action| action.name.clone()))
            .collect(),
    });
    let https = https.map(|https| https.with_events(tls_events(rt, backend.config().ok())));
    let manifest_path = config.root_dir.join("cache/plugins_manifest.json");
    let python = PythonHostLauncher::python_from_env();
    // The Dash console is the Python host's (D21): the host runs for it when its modules import.
    // Repair missing Python dependencies in the shared environment unless installs are
    // disabled. An unavailable console is one warning, and `/dash` is the pointer page.
    let console_available = if stdio || config.no_dash {
        false
    } else {
        let mut probe = probe_console(&python, &config.root_dir, &[]);
        let env = native::pip::PipEnv::new(rt);
        let missing = console_dependencies(&probe);
        if env.auto_install && !missing.is_empty() {
            native::info(
                rt,
                "Installing web console dependencies into the shared mrsm environment...",
            );
            native::pip::pip_install(
                &env,
                rt,
                &native::pip::PipInstall {
                    names: missing,
                    venv: "mrsm".into(),
                    debug: native::flag(args, "debug"),
                    // Install missing requirements without upgrading existing dependencies.
                    args: Some(Vec::new()),
                    ..Default::default()
                },
            );
            probe = probe_console(&python, &config.root_dir, &[]);
        }
        if !probe.available {
            native::warn(rt, &probe.warning());
        }
        probe.available
    };
    let manifest = HostManifest::from_file(&manifest_path)
        .map_err(io_error)?
        .with_console(console_available);
    let mut launcher = PythonHostLauncher::for_config(&config, python);
    launcher.env.push((
        "MRSM_SHADOWED_PYTHON_PLUGINS".into(),
        serde_json::to_string(&plugin_manifest.shadowed_plugins).expect("plugin names"),
    ));
    let supervisor = HostSupervisor::new(
        &manifest,
        Arc::new(StderrHostLauncher(launcher, None)),
        default_socket_path(&config.root_dir),
        SupervisorOptions::default(),
    );
    for notice in &config.notices {
        rt.eprint(&format!("{notice}\n"));
    }
    if !stdio && let Some(line) = config.preset_notice() {
        native::info(rt, &line);
    }
    // Where HTTPS is served and with which certificate (a warning when clients will refuse it).
    if let Some(https) = &https {
        for message in https.startup_messages(&config) {
            match message {
                meerschaum_server::tls::TlsEvent::Info(line) => native::info(rt, &line),
                meerschaum_server::tls::TlsEvent::Warning(line) => native::warn(rt, &line),
            }
        }
    }
    if !stdio {
        for warning in [
            config.open_terminal_warning(),
            config.open_no_auth_warning(),
        ]
        .into_iter()
        .flatten()
        {
            native::warn(rt, &warning);
        }
        // v5: one line when the PostgreSQL instance has an extension older than its server's
        // (bounded probe on its own connection; errors are ignored, startup never waits long).
        if let Some(sql) = instance.sql()
            && let Some(hint) = native::extensions::spec_hint(
                &session,
                sql.spec(),
                &instance.keys(),
                native::extensions::HINT_TIMEOUT,
            )
        {
            native::warn(rt, &hint);
        }
    }
    let jobs_context = executors
        .bind_host_jobs(crate::native::jobs::jobs_context(rt))
        .map_err(|message| ActionError::new("ExecutionUnavailable", message))?;
    executors
        .bind_host_compiler(source_compiler)
        .map_err(|message| ActionError::new("ExecutionUnavailable", message))?;
    executors
        .bind_host_terminals()
        .map_err(|message| ActionError::new("ExecutionUnavailable", message))?;
    let host_jobs = executors
        .host_jobs()
        .map_err(|message| ActionError::new("ExecutionUnavailable", message))?;
    // The isolated teams' scopes, recorded before the restart monitor starts their jobs
    // (with the team's configuration only), and kept current while the server runs.
    let team_scopes = {
        let backend = backend.clone();
        crate::team::scopes::TeamScopes::new(
            session.clone(),
            instance.conn(),
            Arc::new(move || backend.config().ok()),
            config.root_dir.clone(),
            Some(jobs_context.clone()),
        )
    };
    if let Err(error) = session.block_on(team_scopes.refresh()) {
        native::warn(
            rt,
            &format!("The isolated teams' scopes were not recorded: {error}"),
        );
    }
    // Whether isolated teams' processes are sandboxed here, and what the sandbox misses.
    if let Ok(server_config) = backend.config() {
        for (warning, line) in crate::team::sandbox::server_status(&server_config, &config.root_dir)
        {
            if warning {
                native::warn(rt, &line);
            } else {
                native::info(rt, &line);
            }
        }
        // Whether each code-running team's commands may run here, and why not.
        for (warning, line) in crate::team::execution::status_lines(&server_config) {
            if warning {
                native::warn(rt, &line);
            } else {
                native::info(rt, &line);
            }
        }
    }
    // PARITY: v4's API startup ran `start_check_jobs_thread()`: while the server runs, it
    // restarts the root's stopped `--restart` jobs that were not stopped by hand (a container or
    // machine restart), every `check_restart_seconds`. Stops with this function.
    let _restart_monitor = if stdio {
        None
    } else {
        meerschaum_jobs::monitor::spawn(
            jobs_context.clone(),
            meerschaum_jobs::monitor::default_interval(),
        )
        .map_err(|error| native::warn(rt, &format!("Cannot monitor job restarts: {error}")))
        .ok()
    };
    let jobs = Arc::new(crate::executor::jobs::DelegatingJobs::new(
        &config.root_dir,
        host_jobs.backend(),
        executors.clone(),
    ));
    // Engine state (`/engine/*`, §5.8 R2) persists on the server's own instance, so an API
    // instance's ledger, cursors, runs and leases survive a restart.
    let engine_store = Arc::new(meerschaum_server::engine_store::InstanceEngine::new(
        instance.conn(),
        &instance.keys(),
    ));
    // Resolve extra allowed instances through the process's configured connectors. Native
    // host calls and ordinary HTTP routes then use the same instance boundary.
    let plugin_host = crate::native_plugins::HostEnv::new(
        session.clone(),
        crate::native_plugins::host_config(rt.config()),
        session.native_plugins(rt.config())?,
        Some(instance.keys()),
    );
    let grafana_teams = meerschaum_web::grafana::GrafanaTeams::from_section(
        &config.grafana,
        &config.grafana_password_path,
    );
    if let Some(e) = grafana_teams.error() {
        native::warn(rt, &format!("The Grafana integration is inactive: {e}"));
    }
    // Once at startup: the data sources Grafana's anonymous visitors can query.
    let anonymous_check = match meerschaum_web::grafana::GrafanaSettings::from_section(
        &config.grafana,
        &config.grafana_password_path,
    ) {
        Ok(Some(settings)) if !stdio => Some((
            settings,
            rt.config()
                .get(&["meerschaum", "connectors", "sql", "main", "username"])
                .map(|v| v.py_str())
                .filter(|u| !u.is_empty() && u != "None"),
        )),
        _ => None,
    };
    let state = AppState::builder(config, instance.conn())
        .resolver(Arc::new(plugin_host.clone()))
        .native_self_api_engines()
        .team_events(Arc::new(TeamFollowers {
            config: crate::native_plugins::host_config(rt.config()),
            grafana: grafana_teams,
            scopes: team_scopes.clone(),
        }))
        .actions(backend.clone())
        .source_fetch(Arc::new(
            crate::source_fetch::backend::NativeSqlSources::new(
                session.clone(),
                crate::native_plugins::host_config(rt.config()),
            ),
        ))
        .jobs(jobs)
        .engine(engine_store)
        .pipelines(backend.clone())
        .host(Arc::new(meerschaum_server::proxy::HostProxy::supervised(
            supervisor.clone(),
        )))
        .build()
        .map_err(io_error)?;
    executors
        .bind_state(&state)
        .map_err(|error| ActionError::new("RuntimeError", error))?;
    let host_proxy = state.host.clone();
    // Engine runs this process starts (`POST /actions/sync`, MCP action tools) reach
    // `GET /engine/runs/{run_id}/events/stream` live, not only through the persisted tail.
    // Registered for as long as this function serves: dropping the registration (on return,
    // including an early error) releases the sink and the server state it holds.
    let _live_events =
        meerschaum_engine::add_process_event_sink(Arc::new(LiveRunEvents(state.clone())));
    // The syncs this server runs (its sync route, actions, MCP tools) record `api` as their
    // source in the pipes' sync statuses, for as long as it serves.
    let _sync_source = meerschaum_connector::sync_status::role_scope(
        meerschaum_connector::sync_status::SyncSource::Api,
    );
    // Native plugins' MCP tools, resources and prompts are served in-process (both transports).
    let mut mcp_notices = Vec::new();
    let native_mcp = crate::native_plugins::mcp(
        &session.native_plugins(rt.config())?,
        &mut mcp_notices,
        &plugin_host,
        &state,
    );
    for notice in mcp_notices {
        rt.eprint(&format!("{notice}\n"));
    }
    let mcp = Arc::new(
        McpServer::for_state(&state)
            .with_backend(backend)
            .with_native(native_mcp),
    );
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(workers) = native::int(args, "workers") {
        if workers < 1 {
            return Err(ActionError::new(
                "ValueError",
                "workers must be greater than zero",
            ));
        }
        builder.worker_threads(workers as usize);
    }
    let server_runtime = builder.enable_all().build().map_err(io_error)?;
    let result = server_runtime.block_on(async {
        let executor_heartbeat = tokio::spawn(executors.maintain());
        {
            let team_scopes = team_scopes.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(crate::team::scopes::REFRESH_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    interval.tick().await;
                    match team_scopes.refresh().await {
                        Ok(done) if !done.restarted.is_empty() || !done.retired.is_empty() => {
                            tracing::info!(
                                "Team scopes: restarted jobs {:?}, retired team workers {:?}.",
                                done.restarted,
                                done.retired
                            );
                        }
                        Ok(_) => {}
                        Err(error) => tracing::warn!("The team scopes were not refreshed: {error}"),
                    }
                }
            });
        }
        if let Some((settings, owner)) = anonymous_check {
            tokio::spawn(meerschaum_web::grafana::warn_about_anonymous_datasources(
                settings,
                meerschaum_connector_sql::team_database::MAIN_READONLY_ROLE.to_string(),
                owner,
            ));
        }
        // Register synchronously before either serving future can expose readiness. An async
        // ctrl_c() constructor would register only when select! first polls that branch.
        let mut shutdown = ShutdownSignal::new().map_err(io_error)?;
        let console = if stdio {
            None
        } else {
            // Start the host in the background (it serves the Dash console and the plugins) and
            // learn whether it mounted the console; serving does not wait for Python.
            if let Some(host) = host_proxy.clone()
                && supervisor.is_needed()
            {
                let secret = state.secret().to_vec();
                // `/healthcheck/ready` answers 503 ("starting") until the host answered or
                // failed to (bounded by `api:host:response_seconds` and the supervisor's
                // readiness wait), then 503 ("host_unavailable") for as long as the host is not
                // serving: no timeout makes a server that cannot serve its plugins look ready.
                let lifecycle = state.lifecycle.clone();
                lifecycle.set_starting(true);
                tokio::spawn(async move {
                    host.refresh_console(&secret).await;
                    lifecycle.set_starting(false);
                });
            }
            // Native plugin routes (no host process) are merged with the console's routes.
            let natives = session.native_plugins(rt.config())?;
            let (routes, notices) =
                crate::native_plugins::routes(&state, &natives, &plugin_host);
            for notice in notices {
                rt.eprint(&format!("{notice}\n"));
            }
            Some(match console(&state, &plugin_manifest, rt)? {
                Some(console) => console.merge(routes),
                None => routes,
            })
        };
        let watcher = spawn_manifest_watcher(
            Arc::new(WithConsole(FileManifest(manifest_path), console_available)),
            supervisor.clone(),
            mcp.federation.clone(),
            Duration::from_secs(1),
        );
        let result = if stdio {
            let serve = async {
                if supervisor.is_needed()
                    && let Some(federation) = &mcp.federation
                {
                    let _ = federation.refresh().await;
                }
                meerschaum_server::mcp::stdio::serve_stdio(&mcp, &state).await
            };
            tokio::select! {
                result = serve => Ok(result),
                result = shutdown.wait() => result.map(|()| SuccessTuple::success()).map_err(io_error),
            }
        } else {
            let grace = state.config.shutdown_grace;
            let lifecycle = state.lifecycle.clone();
            let serving = meerschaum_server::serve_with_tls(state.clone(), mcp, console, https);
            tokio::pin!(serving);
            let result = tokio::select! {
                result = &mut serving => result.map(|()| SuccessTuple::success()).map_err(io_error),
                result = shutdown.wait() => match result {
                    Err(error) => Err(io_error(error)),
                    // `api:shutdown:grace_seconds: 0`: stop at once.
                    Ok(()) if grace.is_zero() => Ok(SuccessTuple::success()),
                    Ok(()) => {
                        native::info(
                            rt,
                            &format!(
                                "Shutting down: finishing the requests in progress (up to {} s; \
                                 interrupt again to stop now).",
                                format_seconds(grace)
                            ),
                        );
                        drain(&lifecycle, serving.as_mut(), &mut shutdown, grace).await
                    }
                },
            };
            // A drain closed the web terminals: let them deliver their close frames.
            let deadline = tokio::time::Instant::now() + TERMINALS_CLOSE_TIMEOUT;
            while meerschaum_server::webterm::active_terminals() > 0
                && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            result
        };
        watcher.abort();
        executor_heartbeat.abort();
        // The sync statuses this server recorded (written in the background) before its
        // runtime goes away.
        meerschaum_connector::sync_status::flush(
            meerschaum_connector::sync_status::RECORD_TIMEOUT,
        )
        .await;
        supervisor.shutdown().await;
        // The web terminal's own tmux server, when this server started it.
        meerschaum_server::webterm::stop_owned_servers();
        result
    });
    // Tokio stdin uses a blocking reader which may still await input after Ctrl-C.
    server_runtime.shutdown_timeout(Duration::from_millis(100));
    result
}

/// The routes the Rust server keeps of the web console (v4 served it unless `--no-dash`): the
/// web terminal, the job-log stream and native plugin pages as fragments. The Dash console
/// itself is the Python API host's; the server proxies `/dash` to it (or shows the pointer page).
/// `None` with `--no-dash`.
pub fn console(
    state: &AppState,
    manifest: &meerschaum_worker::manifest::Manifest,
    rt: &mut Runtime<'_>,
) -> Result<Option<axum::Router>, ActionError> {
    if state.config.no_dash {
        return Ok(None);
    }
    let mut web = meerschaum_web::WebConfig::for_state(state);
    // PARITY: `--no-webterm` (also implied by `--no-auth`) removes the terminal page.
    // The terminal runs `<this binary> -i <instance>` on the server's root.
    if !state.config.no_webterm
        && let Ok(mut shell) =
            meerschaum_web::ShellCommand::current_exe_shell(&state.config.instance_keys)
    {
        shell.env.push((
            "MRSM_ROOT_DIR".into(),
            state.config.root_dir.display().to_string(),
        ));
        web.webterm = Some(meerschaum_web::WebtermSettings::new(shell));
    }
    let session = native::session(rt);
    let natives = session.native_plugins(rt.config())?;
    // The pages' host calls run for their viewer, on the server's own config.
    let plugin_host = crate::native_plugins::HostEnv::new(
        session,
        crate::native_plugins::host_config(rt.config()),
        natives.clone(),
        Some(state.config.instance_keys.clone()),
    );
    web.pages = crate::native_plugins::pages(manifest, &natives, &plugin_host, state);
    match meerschaum_web::router(state.clone(), web.clone()) {
        Ok(router) => Ok(Some(router)),
        Err(error) => {
            // A native page with a bad path must not take the terminal down.
            rt.eprint(&format!("Native plugin pages are disabled: {error}\n"));
            web.pages.clear();
            meerschaum_web::router(state.clone(), web)
                .map(Some)
                .map_err(io_error)
        }
    }
}

/// How long a stopping server lets its web terminals deliver their close frames.
const TERMINALS_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

/// `8`, `0.5`: a grace period as its config value reads.
fn format_seconds(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds.fract() == 0.0 {
        format!("{}", seconds as u64)
    } else {
        format!("{seconds}")
    }
}

/// Drain the server (`api:shutdown:grace_seconds`): its listeners stop accepting, long-lived
/// responses end, and the requests in progress finish, for at most `grace`. A second signal
/// stops at once.
async fn drain<F>(
    lifecycle: &meerschaum_server::lifecycle::Lifecycle,
    serving: std::pin::Pin<&mut F>,
    shutdown: &mut ShutdownSignal,
    grace: Duration,
) -> ActionResult
where
    F: std::future::Future<Output = std::io::Result<()>>,
{
    lifecycle.begin_drain();
    tokio::select! {
        result = serving => result.map(|()| SuccessTuple::success()).map_err(io_error),
        () = tokio::time::sleep(grace) => {
            tracing::warn!(
                "The requests still in progress after {} s were cut off.",
                format_seconds(grace)
            );
            Ok(SuccessTuple::success())
        }
        _ = shutdown.wait() => Ok(SuccessTuple::success()),
    }
}

/// Own the subscriptions through serving and supervisor cleanup, independently of which
/// select! branch is polled first or completes. Construction requires the entered runtime.
struct ShutdownSignal {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(windows)]
    interrupt: tokio::signal::windows::CtrlC,
}

impl ShutdownSignal {
    fn new() -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt())?,
                terminate: signal(SignalKind::terminate())?,
            })
        }
        #[cfg(windows)]
        {
            Ok(Self {
                interrupt: tokio::signal::windows::ctrl_c()?,
            })
        }
    }

    async fn wait(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        tokio::select! {
            _ = self.interrupt.recv() => Ok(()),
            _ = self.terminate.recv() => Ok(()),
        }
        #[cfg(windows)]
        {
            self.interrupt.recv().await;
            Ok(())
        }
    }
}

pub(crate) fn argument(value: meerschaum_core::Value) -> PyValue {
    use meerschaum_core::Value;
    match value {
        Value::Null => PyValue::None,
        Value::Bool(value) => PyValue::Bool(value),
        Value::Int(value) => PyValue::Int(value.into()),
        Value::Float(value) => PyValue::Float(value),
        Value::Str(value) => PyValue::Str(value),
        Value::List(values) => PyValue::List(values.into_iter().map(argument).collect()),
        Value::Map(values) => PyValue::Dict(
            values
                .into_iter()
                .map(|(key, value)| (PyValue::Str(key), argument(value)))
                .collect(),
        ),
    }
}

/// What follows a team's rename or deletion through the API server: its connector
/// reservations (`teams: [<name>]`) move to the new name or are retired (no later team of the
/// deleted team's name inherits its connector or database), then its Grafana organization.
struct TeamFollowers {
    config: Arc<meerschaum_core::config::Config>,
    grafana: meerschaum_web::grafana::GrafanaTeams,
    scopes: crate::team::scopes::TeamScopes,
}

impl TeamFollowers {
    fn reservations(&self, from: &str, to: &str) -> Result<(), String> {
        self.config
            .move_team_reservations(from, to)
            .map(|_| ())
            .map_err(|e| format!("The connectors reserved for team '{from}' were not updated: {e}"))
    }
}

#[async_trait::async_trait]
impl meerschaum_server::backends::TeamEvents for TeamFollowers {
    async fn renamed(&self, team: &meerschaum_core::access::Team, old: &str) -> Result<(), String> {
        let notes: Vec<String> = [
            self.reservations(old, &team.name),
            self.grafana.renamed(team, old).await,
            self.scopes.refresh().await.map(|_| ()),
        ]
        .into_iter()
        .filter_map(Result::err)
        .collect();
        if notes.is_empty() {
            Ok(())
        } else {
            Err(notes.join("\n"))
        }
    }

    async fn changed(&self) -> Result<(), String> {
        self.scopes.refresh().await.map(|_| ())
    }

    async fn deleted(&self, team: &meerschaum_core::access::Team) -> Result<(), String> {
        let retired = meerschaum_core::access::retired_reservation(&team.name, team.id.as_deref());
        let notes: Vec<String> = [
            self.reservations(&team.name, &retired),
            self.grafana.deleted(team).await,
            self.scopes.refresh().await.map(|_| ()),
        ]
        .into_iter()
        .filter_map(Result::err)
        .collect();
        if notes.is_empty() {
            Ok(())
        } else {
            Err(notes.join("\n"))
        }
    }
}

#[cfg(test)]
mod tests;
