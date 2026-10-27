//! CLI hosting of the Rust HTTP and MCP servers, with the supervised Python API host.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::{Map, Pipe, SuccessTuple};
use meerschaum_server::host::{
    FileManifest, HostEndpoint, HostLauncher, HostManifest, HostProcess, HostSupervisor,
    PythonHostLauncher, SupervisorOptions, WithConsole, default_socket_path, probe_console,
    spawn_manifest_watcher,
};
use meerschaum_server::mcp::{ConnectorRegister, McpBackend, McpServer, SqlReadError};
use meerschaum_server::{AppState, ServerConfig, ServerFlags};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::native::{self, Session};
use crate::value::{ArgsMap, PyValue};

// Plugin imports may print to stdout. Keep the MCP protocol channel clean even when the
// supervised API host runs Python plugins with ordinary print() diagnostics.
struct StderrHostLauncher(PythonHostLauncher);
struct HostChild(tokio::process::Child);

#[async_trait]
impl HostProcess for HostChild {
    async fn wait(&mut self) -> std::io::Result<Option<i32>> {
        Ok(self.0.wait().await?.code())
    }

    async fn kill(&mut self) -> std::io::Result<()> {
        self.0.kill().await
    }
}

#[async_trait]
impl HostLauncher for StderrHostLauncher {
    async fn launch(&self, socket: &std::path::Path) -> std::io::Result<Box<dyn HostProcess>> {
        self.launch_endpoint(&HostEndpoint::from(socket)).await
    }

    async fn launch_endpoint(
        &self,
        endpoint: &HostEndpoint,
    ) -> std::io::Result<Box<dyn HostProcess>> {
        let mut child = tokio::process::Command::new(&self.0.python)
            .args([
                meerschaum_core::self_exe::PYTHON_SAFE_PATH_FLAG,
                "-m",
                "meerschaum.api.host",
            ])
            .args(PythonHostLauncher::endpoint_args(endpoint))
            .envs(PythonHostLauncher::endpoint_env(endpoint))
            .envs(self.0.env.iter().map(|(key, value)| (key, value)))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        if let Some(mut stdout) = child.stdout.take() {
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut stdout, &mut tokio::io::stderr()).await;
            });
        }
        Ok(Box::new(HostChild(child)))
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

/// Why `start api` does not start: HTTPS was asked for (`--keyfile` / `--certfile`, or an
/// `ssl_keyfile` / `ssl_certfile` key under `api:server` or `api:uvicorn`), and the server does
/// not terminate TLS itself. Serving plain HTTP instead would leave every `https://` client
/// failing, while the operator believes the traffic is encrypted.
pub fn tls_refusal(args: &ArgsMap, config: &Config) -> Option<String> {
    const PROXY: &str = "Put a reverse proxy that terminates TLS (for example Caddy or nginx) in \
                         front of the API server";
    let flags: Vec<&str> = [("keyfile", "--keyfile"), ("certfile", "--certfile")]
        .into_iter()
        .filter(|(dest, _)| native::string(args, dest).is_some())
        .map(|(_, option)| option)
        .collect();
    if !flags.is_empty() {
        let (verb, them) = if flags.len() == 1 {
            ("is", "this flag")
        } else {
            ("are", "these flags")
        };
        return Some(format!(
            "The API server does not serve HTTPS itself, so `{}` {verb} not supported. {PROXY}, \
             and start it without {them}.",
            flags.join("` and `")
        ));
    }
    for section in ["server", "uvicorn"] {
        for key in ["ssl_keyfile", "ssl_certfile"] {
            let set = config
                .get(&["api", section, key])
                .is_some_and(|v| !v.is_null() && !v.py_str().trim().is_empty());
            if set {
                return Some(format!(
                    "The API server does not serve HTTPS itself, so `api:{section}:{key}` is not \
                     supported. {PROXY}, and remove the key (`mrsm edit config api`)."
                ));
            }
        }
    }
    None
}

fn start(args: &ArgsMap, rt: &mut Runtime<'_>, stdio: bool) -> ActionResult {
    if !stdio && let Some(refusal) = tls_refusal(args, rt.config()) {
        return Ok(SuccessTuple::fail(refusal));
    }
    // The server waits on SIGINT / SIGTERM itself (`ShutdownSignal`): the spinner of its startup
    // must not take them.
    crate::native::jobs::own_signals();
    // A server prints nothing between requests: the API client's warnings go to `tracing`.
    crate::native::api_warnings::uninstall();
    let config = ServerConfig::from_config(rt.config(), &flags(args)?);
    let session = native::session(rt);
    let plugin_manifest = session.manifest(rt.config())?;
    let instance = session.instance(&config.instance_keys, rt.config())?;
    let mut options = ConfigOptions::from_process();
    options.env.retain(|(key, _)| key != "MRSM_ROOT_DIR");
    options.env.push((
        "MRSM_ROOT_DIR".into(),
        config.root_dir.display().to_string(),
    ));
    let backend = Arc::new(NativeBackend {
        options,
        snapshot: rt.config().snapshot(),
        session: session.clone(),
        plugin_actions: plugin_manifest
            .plugins
            .values()
            .flat_map(|plugin| plugin.actions.iter().map(|action| action.name.clone()))
            .collect(),
    });
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
    let launcher = PythonHostLauncher::for_config(&config, python);
    let supervisor = HostSupervisor::new(
        &manifest,
        Arc::new(StderrHostLauncher(launcher)),
        default_socket_path(&config.root_dir),
        SupervisorOptions::default(),
    );
    for notice in &config.notices {
        rt.eprint(&format!("{notice}\n"));
    }
    if !stdio && let Some(line) = config.preset_notice() {
        native::info(rt, &line);
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
    let jobs = Arc::new(crate::jobs_backend::LocalJobsBackend::new(
        crate::native::jobs::jobs_context(rt),
    ));
    // Engine state (`/engine/*`, §5.8 R2) persists on the server's own instance, so an API
    // instance's ledger, cursors, runs and leases survive a restart.
    let engine_store = Arc::new(meerschaum_server::engine_store::InstanceEngine::new(
        instance.conn(),
        &instance.keys(),
    ));
    let state = AppState::builder(config, instance.conn())
        .actions(backend.clone())
        .jobs(jobs)
        .engine(engine_store)
        .pipelines(backend.clone())
        .host(Arc::new(meerschaum_server::proxy::HostProxy::supervised(
            supervisor.clone(),
        )))
        .build()
        .map_err(io_error)?;
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
    // Native plugins' host calls (`host::pipe(...)`, `host::read_sql(...)`) run for the caller,
    // on the server's own config.
    let plugin_host = crate::native_plugins::HostEnv::new(
        session.clone(),
        crate::native_plugins::host_config(rt.config()),
        session.native_plugins(rt.config())?,
        Some(instance.keys()),
    );
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
                tokio::spawn(async move {
                    host.refresh_console(&secret).await;
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
            tokio::select! {
                result = meerschaum_server::serve_with_mcp_and_console(state, mcp, console) => result.map(|()| SuccessTuple::success()).map_err(io_error),
                result = shutdown.wait() => result.map(|()| SuccessTuple::success()).map_err(io_error),
            }
        };
        watcher.abort();
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

// Native actions synchronously drive the shared Session runtime. Run them on a blocking
// thread so they never nest block_on inside the server's async executor.
#[derive(Clone)]
struct NativeBackend {
    options: ConfigOptions,
    snapshot: Map,
    session: Session,
    plugin_actions: Vec<String>,
}

impl NativeBackend {
    fn config(&self) -> Result<Config, ActionError> {
        let config = Config::new(self.options.clone()).map_err(io_error)?;
        config.set_config(self.snapshot.clone());
        Ok(config)
    }

    async fn sync(
        &self,
        pipe: &Pipe,
        begin: Option<String>,
        end: Option<String>,
        verify: bool,
    ) -> SuccessTuple {
        let backend = self.clone();
        let pipe = pipe.clone();
        tokio::task::spawn_blocking(move || {
            let mut stderr = std::io::stderr();
            let mut runtime = Runtime::system(&mut stderr);
            // A server console is not a terminal session: no spinners, no colored graphs.
            runtime.progress = crate::format::progress::ProgressSink::Off;
            runtime.live = false;
            runtime.native = Some(backend.session.clone());
            runtime.config = match backend.config() {
                Ok(config) => Some(config),
                Err(error) => return SuccessTuple::fail(error.message),
            };
            let session = native::session(&mut runtime);
            let instance = match session.instance(pipe.instance_keys(), runtime.config()) {
                Ok(instance) => instance,
                Err(error) => return SuccessTuple::fail(error.message),
            };
            let mut argv = vec!["sync".to_string(), "pipes".to_string()];
            for (flag, bound) in [("--begin", begin), ("--end", end)] {
                if let Some(bound) = bound {
                    argv.extend([flag.to_string(), bound]);
                }
            }
            let args = match crate::parse::parse_arguments(&argv, &runtime.dates) {
                Ok(args) => args,
                Err(error) => return SuccessTuple::fail(error.message),
            };
            let sync_args = native::sync::SyncArgs::from_args(&args);
            let mut selected = native::pipes::SelectedPipe {
                pipe,
                id: None,
                hydrated: false,
            };
            if verify {
                native::verify::verify_pipe(
                    &mut runtime,
                    &session,
                    &instance,
                    &mut selected,
                    &args,
                    &sync_args,
                )
            } else {
                native::sync::sync_pipe(
                    &mut runtime,
                    &session,
                    &instance,
                    &mut selected,
                    &sync_args,
                )
            }
        })
        .await
        .unwrap_or_else(|error| SuccessTuple::fail(error.to_string()))
    }

    /// Run a command line (`["plan", "pipelines", ...]`) the way the server runs actions, with
    /// what it prints captured: the action's result and its output.
    async fn run_captured(&self, argv: Vec<String>) -> Result<(SuccessTuple, String), String> {
        let backend = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut out: Vec<u8> = Vec::new();
            let result = {
                let mut runtime = Runtime::system(&mut out);
                // A server console is not a terminal session: no spinners, no colored graphs.
                runtime.progress = crate::format::progress::ProgressSink::Off;
                runtime.live = false;
                runtime.native = Some(backend.session.clone());
                runtime.config = Some(backend.config().map_err(|e| e.message)?);
                let args =
                    crate::parse::parse_arguments(&argv, &runtime.dates).map_err(|e| e.message)?;
                meerschaum_core::debug::without_scopes(|| {
                    crate::dispatch::entry_with_args(args, &mut runtime, 0)
                })
            };
            Ok((result, String::from_utf8_lossy(&out).into_owned()))
        })
        .await
        .map_err(|e| e.to_string())?
    }
}

#[async_trait]
impl meerschaum_server::backends::PipelinesBackend for NativeBackend {
    /// `plan pipelines NAME -i REGISTRY --nopretty`: the engine's static plan of the saved
    /// Pipeline, which moves no data and registers nothing.
    async fn plan(
        &self,
        name: &str,
        registry: &str,
        options: &meerschaum_server::backends::PlanOptions,
    ) -> Result<serde_json::Value, meerschaum_server::backends::PlanError> {
        use meerschaum_server::backends::PlanError;
        let mut argv: Vec<String> = ["plan", "pipelines", name, "-i", registry, "--nopretty"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        for (flag, value) in [("--begin", &options.begin), ("--end", &options.end)] {
            if let Some(value) = value {
                argv.push(flag.into());
                argv.push(value.clone());
            }
        }
        let (result, output) = self.run_captured(argv).await.map_err(PlanError::Failed)?;
        // A plan that cannot be made (a missing member, a cycle, a bad bound) is the
        // Pipeline's problem, not the server's.
        if !result.success {
            return Err(PlanError::Invalid(result.message));
        }
        let line = output
            .lines()
            .rev()
            .find(|l| l.trim_start().starts_with('{'))
            .ok_or_else(|| PlanError::Failed("The planner printed no plan.".into()))?;
        let plan: meerschaum_engine::Plan = serde_json::from_str(line.trim())
            .map_err(|e| PlanError::Failed(format!("The planner printed an invalid plan: {e}")))?;
        Ok(serde_json::json!({
            "plan": plan,
            "text": plan.to_string(),
            "warnings": plan.warnings,
        }))
    }
}

fn argument(value: meerschaum_core::Value) -> PyValue {
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

#[async_trait]
impl meerschaum_server::backends::ActionsBackend for NativeBackend {
    fn list(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        crate::actions::CORE_ACTIONS
            .iter()
            .map(|action| action.name.to_string())
            .chain(self.plugin_actions.iter().cloned())
            .filter(|name| seen.insert(name.clone()))
            .collect()
    }

    async fn execute(&self, action: &str, keywords: Map) -> SuccessTuple {
        let backend = self.clone();
        let action = action.to_string();
        tokio::task::spawn_blocking(move || {
            let mut stderr = std::io::stderr();
            let mut runtime = Runtime::system(&mut stderr);
            // A server console is not a terminal session: no spinners, no colored graphs.
            runtime.progress = crate::format::progress::ProgressSink::Off;
            runtime.live = false;
            runtime.native = Some(backend.session.clone());
            runtime.config = match backend.config() {
                Ok(config) => Some(config),
                Err(error) => return SuccessTuple::fail(error.message),
            };
            let mut args: ArgsMap = keywords
                .into_iter()
                .map(|(key, value)| (key, argument(value)))
                .collect();
            let mut words = vec![action];
            words.extend(native::words(&args, "action"));
            args.insert("action".into(), PyValue::str_list(&words));
            // A client's `debug` reaches the action, but does not switch on debug output (and
            // `debug=True` for worker calls) for every request the server is handling.
            meerschaum_core::debug::without_scopes(|| {
                crate::dispatch::entry_with_args(args, &mut runtime, 0)
            })
        })
        .await
        .unwrap_or_else(|error| SuccessTuple::fail(error.to_string()))
    }
}

#[async_trait]
impl McpBackend for NativeBackend {
    /// `Pipe.register()`'s connector half through the session's Python worker (the same ask as
    /// `register pipes`), so the MCP `register_pipe` tool and `mrsm register pipes` agree.
    async fn connector_register(&self, pipe: &Pipe) -> ConnectorRegister {
        let backend = self.clone();
        let pipe = pipe.clone();
        tokio::task::spawn_blocking(move || {
            let Ok(config) = backend.config() else {
                return ConnectorRegister::Undefined;
            };
            match native::manage::ask_connector_register(&backend.session, &config, &pipe) {
                None => ConnectorRegister::Undefined,
                Some(Ok(outcome)) if outcome.valid => {
                    ConnectorRegister::Parameters(outcome.parameters)
                }
                Some(Ok(outcome)) => ConnectorRegister::Invalid(outcome.repr),
                Some(Err(error)) => {
                    tracing::warn!("`register()` failed for {pipe}: {error}");
                    ConnectorRegister::Undefined
                }
            }
        })
        .await
        .unwrap_or(ConnectorRegister::Undefined)
    }

    async fn sync_pipe(
        &self,
        pipe: &Pipe,
        begin: Option<String>,
        end: Option<String>,
    ) -> SuccessTuple {
        self.sync(pipe, begin, end, false).await
    }

    /// `sync pipes -c CK -m MK -l LK -i IK --parents [N] --children [N]`: the pipe with its
    /// upstream or downstream pipes, in dependency order (the Syncing Engine).
    async fn sync_pipe_graph(
        &self,
        pipe: &Pipe,
        begin: Option<String>,
        end: Option<String>,
        parents: Option<meerschaum_core::Value>,
        children: Option<meerschaum_core::Value>,
    ) -> SuccessTuple {
        if parents.is_none() && children.is_none() {
            return self.sync(pipe, begin, end, false).await;
        }
        let mut argv: Vec<String> = vec![
            "sync".into(),
            "pipes".into(),
            "-c".into(),
            pipe.connector_keys().into(),
            "-m".into(),
            pipe.metric_key().into(),
            "-l".into(),
            pipe.location_key().unwrap_or("None").into(),
            "-i".into(),
            pipe.instance_keys().into(),
        ];
        for (flag, depth) in [("--parents", parents), ("--children", children)] {
            match depth {
                Some(meerschaum_core::Value::Int(n)) => argv.push(format!("{flag}={n}")),
                Some(_) => argv.push(flag.into()),
                None => {}
            }
        }
        for (flag, bound) in [("--begin", begin), ("--end", end)] {
            if let Some(bound) = bound {
                argv.extend([flag.to_string(), bound]);
            }
        }
        match self.run_captured(argv).await {
            Ok((result, _)) => result,
            Err(error) => SuccessTuple::fail(error),
        }
    }

    async fn verify_pipe(
        &self,
        pipe: &Pipe,
        begin: Option<String>,
        end: Option<String>,
    ) -> SuccessTuple {
        self.sync(pipe, begin, end, true).await
    }

    async fn read_sql(
        &self,
        connector_keys: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<meerschaum_core::Value>, SqlReadError> {
        let backend = self.clone();
        let keys = connector_keys.to_string();
        let query = query.to_string();
        tokio::task::spawn_blocking(move || {
            let config = backend.config().map_err(|_| SqlReadError::Unsupported)?;
            let session = backend.session.clone();
            let instance = session
                .instance(&keys, &config)
                .map_err(|_| SqlReadError::Unsupported)?;
            let sql = instance.sql().ok_or(SqlReadError::Unsupported)?;
            let frame = session
                // A protected connector runs only a read statement, read-only.
                .block_on(sql.read_raw(&query))
                .map_err(|_| SqlReadError::Failed)?;
            let json = meerschaum_server::serialize::data_to_json(
                &frame,
                &meerschaum_server::serialize::DataJsonOptions {
                    epoch: false,
                    date_unit: meerschaum_server::serialize::DateUnit::Ms,
                    double_precision: 10,
                    geometry_format: meerschaum_kernel::serialize::GeometryFormat::WkbHex,
                },
            );
            let mut rows: Vec<meerschaum_core::Value> =
                serde_json::from_str(&json).map_err(|_| SqlReadError::Failed)?;
            rows.truncate(limit);
            Ok(rows)
        })
        .await
        .map_err(|_| SqlReadError::Failed)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_repairs_only_missing_dependencies_with_python_package_present() {
        use meerschaum_server::host::ConsoleProbe;
        let probe = |names: &[&str]| ConsoleProbe {
            available: names.is_empty(),
            missing: names.iter().map(|s| (*s).into()).collect(),
        };
        assert_eq!(
            console_dependencies(&probe(&["dash", "jwt"])),
            vec!["dash>=4.1.0", "PyJWT>=2.8.0"]
        );
        assert!(console_dependencies(&probe(&["meerschaum", "dash"])).is_empty());
        assert!(console_dependencies(&probe(&["no Python interpreter"])).is_empty());
        assert!(console_dependencies(&probe(&[])).is_empty());
        for name in meerschaum_server::host::CONSOLE_MODULES
            .iter()
            .filter(|n| **n != "meerschaum")
        {
            assert_eq!(console_dependencies(&probe(&[name])).len(), 1, "{name}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn shutdown_signals_are_registered_before_wait_is_polled() {
        const CHILD_SIGNAL: &str = "MRSM_TEST_SERVER_REGISTRATION_SIGNAL";
        const TEST: &str = "server::tests::shutdown_signals_are_registered_before_wait_is_polled";
        if let Some(signum) = std::env::var_os(CHILD_SIGNAL) {
            let signum: i32 = signum.to_str().unwrap().parse().unwrap();
            assert!(matches!(
                signum,
                signal_hook::consts::SIGINT | signal_hook::consts::SIGTERM
            ));
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let mut shutdown = ShutdownSignal::new().unwrap();
                // Deliver only to this owned child, before wait() has even been constructed.
                signal_hook::low_level::raise(signum).unwrap();
                tokio::time::timeout(Duration::from_secs(5), shutdown.wait())
                    .await
                    .expect("registered signal was lost before the first wait poll")
                    .unwrap();
            });
            return;
        }
        for signum in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture"])
                .env(CHILD_SIGNAL, signum.to_string())
                .env_remove("TMUX")
                .env_remove("TMUX_PANE")
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        assert!(
                            status.success(),
                            "registration child for {signum} exited {status}"
                        );
                        break;
                    }
                    Ok(None) if std::time::Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    result => {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("registration child for {signum} did not finish: {result:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn server_flags_preserve_cli_values() {
        let args = ArgsMap::from_iter([
            ("port".into(), PyValue::Int(8123)),
            ("production".into(), PyValue::Bool(true)),
            ("secure".into(), PyValue::Bool(true)),
            ("no_dash".into(), PyValue::Bool(true)),
            ("mrsm_instance".into(), PyValue::Str("sql:wiring".into())),
        ]);
        let flags = flags(&args).unwrap();
        assert_eq!(flags.port, Some(8123));
        assert_eq!(flags.instance.as_deref(), Some("sql:wiring"));
        assert!(flags.production && flags.secure && flags.no_dash);
    }

    #[test]
    fn invalid_port_is_rejected() {
        let args = ArgsMap::from_iter([("port".into(), PyValue::Int(65536))]);
        assert!(flags(&args).is_err());
    }

    /// `--keyfile` / `--certfile` (and the `ssl_*` config keys v4 handed to Uvicorn) stop
    /// `start api` with the way to get HTTPS, instead of serving plain HTTP silently.
    #[test]
    fn https_options_are_refused() {
        let root = tempfile::tempdir().unwrap();
        let config_with = |patch: &str| {
            let mut env = vec![("MRSM_ROOT_DIR".into(), root.path().display().to_string())];
            if !patch.is_empty() {
                env.push(("MRSM_CONFIG".into(), patch.to_string()));
            }
            Config::new(ConfigOptions::with_env(env)).unwrap()
        };
        let plain = config_with("");
        assert_eq!(tls_refusal(&ArgsMap::new(), &plain), None);
        let both = ArgsMap::from_iter([
            ("keyfile".into(), PyValue::Str("key.pem".into())),
            ("certfile".into(), PyValue::Str("cert.pem".into())),
        ]);
        let text = tls_refusal(&both, &plain).unwrap();
        assert!(
            text.contains("so `--keyfile` and `--certfile` are not supported")
                && text.contains("reverse proxy that terminates TLS")
                && text.ends_with("start it without these flags."),
            "{text}"
        );
        let one = ArgsMap::from_iter([("certfile".into(), PyValue::Str("cert.pem".into()))]);
        let text = tls_refusal(&one, &plain).unwrap();
        assert!(text.contains("so `--certfile` is not supported"), "{text}");
        for section in ["server", "uvicorn"] {
            let config = config_with(&format!(
                r#"{{"api": {{"{section}": {{"ssl_keyfile": "/etc/ssl/key.pem"}}}}}}"#
            ));
            let text = tls_refusal(&ArgsMap::new(), &config).unwrap();
            assert!(
                text.contains(&format!("`api:{section}:ssl_keyfile` is not supported")),
                "{text}"
            );
        }
        let empty = config_with(r#"{"api": {"uvicorn": {"ssl_certfile": ""}}}"#);
        assert_eq!(tls_refusal(&ArgsMap::new(), &empty), None);
    }

    #[test]
    fn mcp_reads_sqlite_with_row_limit() {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let options = ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
            (
                "MRSM_PLUGINS_DIR".into(),
                plugins.path().display().to_string(),
            ),
        ]);
        let config = Config::new(options.clone()).unwrap();
        let session = Session::new();
        let backend = NativeBackend {
            options,
            snapshot: config.snapshot(),
            session: session.clone(),
            plugin_actions: Vec::new(),
        };
        let rows = session
            .block_on(backend.read_sql("sql:local", "SELECT 1 AS n UNION ALL SELECT 2 AS n", 1))
            .unwrap();
        assert_eq!(
            serde_json::to_value(rows).unwrap(),
            serde_json::json!([{ "n": 1 }])
        );
        let invalid = session.block_on(backend.read_sql(
            "sql:local",
            "SELECT * FROM wiring_missing_table",
            1,
        ));
        assert_eq!(invalid, Err(SqlReadError::Failed));
    }
}
