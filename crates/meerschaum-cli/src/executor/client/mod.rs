//! Signed main-API clients for one team's ordinary executor API.
//!
//! The children hold the main side of each delegated operation: `jobs` (canonical persistent
//! jobs, with `pipe_windows` planning their sync windows) and `terminals` (terminal identities);
//! `batches`, `data_commits`, `inference`, `pipe_bounds` and `pipe_operation` (native pipe data
//! and graph capture); `placement`, `registration`, `source_selection` and `reader_capabilities`
//! (where an operation runs and which sources it may read); `extensions`, `plugins`,
//! `host_plugins` and `factory_metadata` (plugin discovery and metadata); `host_terminals`
//! (source-bound API terminals on the main host); `metadata_plan` (read-only pipeline planning);
//! `team_pipe_job` (a Team job's credentials); and `configuration_diagnostics` (bounded refusals).

use std::path::PathBuf;
use std::sync::Arc;

use meerschaum_connector::InstanceConnector;
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_connector_api::traits::{ConnectorError, ConnectorResult};
use meerschaum_core::access::{EVERYONE, Team, same_team};
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::execution::{Mode, Policy};
use meerschaum_core::team_scope::{TeamScope, team_config};
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::context;
use meerschaum_server::executor::control::{Actor, Authority};
use meerschaum_server::executor::gateway::{CONFIGURATION_PATH, Configuration, HEALTH_PATH};
use uuid::Uuid;

use crate::executor::jobs::Catalog;
use crate::executor::registry::Registry;

mod batches;
mod configuration_diagnostics;
mod data_commits;
mod extensions;
mod factory_metadata;
mod host_plugins;
mod host_terminals;
mod inference;
mod jobs;
mod metadata_plan;
mod pipe_bounds;
mod pipe_operation;
mod placement;
mod plugins;
mod reader_capabilities;
mod registration;
mod source_selection;
mod team_pipe_job;
mod terminals;

type StateLookup = Arc<dyn Fn() -> Option<meerschaum_server::AppState> + Send + Sync>;

impl Controller {
    /// Attach an administrator-selected immutable offline compiler to the Main-host runtime.
    /// Call immediately after `bind_host_jobs`, before private preparation or metadata cache
    /// lookup. No HTTP request, job property or environment can invoke this constructor.
    pub fn bind_host_source_compiler(
        &self,
        root: &std::path::Path,
        identity: &str,
        anchor: &std::path::Path,
    ) -> Result<(), String> {
        let jobs = self.host_jobs()?;
        let compiler = crate::executor::compiler::Compiler::open(root, identity, anchor)?;
        jobs.set_source_compiler(Some(Arc::new(compiler)))
    }

    /// Native startup passes its already-verified fixed installation proof exactly once.
    pub(crate) fn bind_host_compiler(
        &self,
        compiler: Option<Arc<crate::executor::compiler::Compiler>>,
    ) -> Result<(), String> {
        self.host_jobs()?.set_source_compiler(compiler)
    }
}

/// Bound every HTTP request, SSE connection and websocket handshake to the approved caller.
struct Signer {
    registry_root: PathBuf,
    authority: Authority,
    actor: Actor,
    revision: String,
}

impl RequestAuthentication for Signer {
    fn authorization(&self, request: Request<'_>) -> ConnectorResult<String> {
        // A prepared connection can outlive deletion or generation rotation. Check main's
        // private authority again at every HTTP/WS admission, including cached clients.
        let expected = self.authority.verifier();
        let (record, _) = Registry::new(&self.registry_root)
            .authority(expected.team_id)
            .map_err(|_| {
                ConnectorError::Invalid(
                    "This executor's control authority is unavailable or revoked.".into(),
                )
            })?;
        if record.verifier != expected {
            return Err(ConnectorError::Invalid(
                "This executor's control generation has changed.".into(),
            ));
        }
        let body = request.body.ok_or_else(|| {
            ConnectorError::Invalid("Executor control requires a buffered request body.".into())
        })?;
        self.authority
            .issue(
                self.actor.clone(),
                meerschaum_server::executor::control::Request {
                    method: request.method,
                    path: request.path,
                    body,
                },
                &self.revision,
                chrono::Utc::now().timestamp(),
            )
            .map(|credential| format!("Bearer {credential}"))
            .map_err(|_| {
                ConnectorError::Invalid("Could not authenticate the executor operation.".into())
            })
    }
}

/// Captured from the SAME Signer constructor as this connection. Never Deserialize/Clone;
/// a caller cannot supply an actor, configuration revision or control generation afterward.
pub(crate) struct PreparedExecutor {
    pub(crate) api: Arc<APIConnector>,
    team_pipe_authorities: std::collections::BTreeMap<String, String>,
    actor: Actor,
    configuration_sha256: String,
    control: meerschaum_server::executor::control::Verifier,
}

/// Non-serializable admission held by the single Main request that created the action.
pub(crate) struct NoAuthAction {
    name: String,
    remote_name: String,
    revision: u64,
    argv: Vec<String>,
    properties: Map,
    control: meerschaum_server::executor::control::Verifier,
    configuration_sha256: String,
    actor: Actor,
}

impl NoAuthAction {
    fn ordinary_action(record: &crate::executor::jobs::Record) -> bool {
        record.action_request
            && record.placement == crate::executor::jobs::JobPlacement::Remote
            && record.profile == meerschaum_server::executor::environment::Profile::Team
            && record.callback.is_none()
            && record.owner_binding.is_none()
            && record.source_selection.is_none()
            && record.operation.is_none()
            && record.connector_registration.is_none()
            && record.extension.is_none()
            && record.discovery.is_none()
            && !record.callback_suspended
    }

    fn matches_record(&self, record: &crate::executor::jobs::Record) -> bool {
        Self::ordinary_action(record)
            && self.control.team_id == record.team_id
            && self.name == record.name
            && self.remote_name == record.remote_name
            && self.revision == record.context_revision
            && self.argv == record.sysargs
            && self.properties == record.properties
    }

    fn agrees(
        &self,
        no_auth_request: bool,
        record: &crate::executor::jobs::Record,
        control: &meerschaum_server::executor::control::Verifier,
        configuration_sha256: &str,
        actor: &Actor,
    ) -> bool {
        no_auth_request
            && self.matches_record(record)
            && self.control == *control
            && self.configuration_sha256 == configuration_sha256
            && self.actor == *actor
    }
}

/// Contention defers a healthy lease; unavailable storage never establishes new authority.
#[derive(Clone, Debug, PartialEq, Eq)]
enum TerminalStorageError {
    Busy,
    Unavailable(String),
}
impl std::fmt::Display for TerminalStorageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => formatter.write_str("Terminal identities are busy. Try again."),
            Self::Unavailable(message) => formatter.write_str(message),
        }
    }
}
impl From<String> for TerminalStorageError {
    fn from(message: String) -> Self {
        Self::Unavailable(message)
    }
}
impl From<&str> for TerminalStorageError {
    fn from(message: &str) -> Self {
        Self::Unavailable(message.into())
    }
}

/// Main-API-only controller. Team records are re-read before issuing control credentials;
/// deleted teams and changed execution selections cannot use an earlier cached authority.
#[derive(Clone)]
pub struct Controller {
    root: PathBuf,
    instance: Arc<dyn InstanceConnector>,
    options: ConfigOptions,
    snapshot: Map,
    state: Arc<std::sync::OnceLock<StateLookup>>,
    host_jobs: Arc<std::sync::OnceLock<Arc<crate::executor::host_jobs::HostJobs>>>,
    host_expiry_running: Arc<std::sync::atomic::AtomicBool>,
    host_terminals: Arc<std::sync::OnceLock<Arc<host_terminals::HostTerminals>>>,
    terminal_catalog_gate: Arc<tokio::sync::Mutex<()>>,
}

impl Controller {
    /// Main-API configuration; no part of this controller is sent to an executor.
    pub fn new(
        root: PathBuf,
        instance: Arc<dyn InstanceConnector>,
        options: ConfigOptions,
        snapshot: Map,
    ) -> Self {
        Self {
            root,
            instance,
            options,
            snapshot,
            state: Arc::new(std::sync::OnceLock::new()),
            host_jobs: Arc::new(std::sync::OnceLock::new()),
            host_expiry_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            host_terminals: Arc::new(std::sync::OnceLock::new()),
            terminal_catalog_gate: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Bind the authoritative native host runtime before either restart monitor can run.
    pub fn bind_host_jobs(
        &self,
        context: meerschaum_jobs::JobsContext,
    ) -> Result<meerschaum_jobs::JobsContext, String> {
        let jobs = Arc::new(crate::executor::host_jobs::HostJobs::new(
            &self.root, context,
        )?);
        let context = jobs.context();
        self.host_jobs
            .set(jobs)
            .map_err(|_| "The host job runtime is already bound.".to_string())?;
        Ok(context)
    }

    pub(crate) fn host_jobs(&self) -> Result<Arc<crate::executor::host_jobs::HostJobs>, String> {
        self.host_jobs
            .get()
            .cloned()
            .ok_or_else(|| "The native host job runtime is unavailable.".into())
    }

    pub(crate) fn allow_unattributed_jobs(&self) -> Result<bool, String> {
        self.state().map(|state| state.config.no_auth)
    }

    fn no_auth_request(&self) -> bool {
        self.allow_unattributed_jobs() == Ok(true)
            && context::current().as_ref().is_none_or(|caller| {
                caller.no_auth && caller.auth.is_none() && caller.credential.is_none()
            })
    }

    pub(crate) fn no_auth_action(
        &self,
        record: &crate::executor::jobs::Record,
        prepared: &PreparedExecutor,
    ) -> Option<NoAuthAction> {
        (self.no_auth_request()
            && NoAuthAction::ordinary_action(record)
            && prepared.control.team_id == record.team_id)
            .then(|| NoAuthAction {
                name: record.name.clone(),
                remote_name: record.remote_name.clone(),
                revision: record.context_revision,
                argv: record.sysargs.clone(),
                properties: record.properties.clone(),
                control: prepared.control.clone(),
                configuration_sha256: prepared.configuration_sha256.clone(),
                actor: prepared.actor.clone(),
            })
    }

    async fn validate_no_auth_action(
        &self,
        receipt: &NoAuthAction,
        record: &crate::executor::jobs::Record,
    ) -> Result<(), String> {
        let refused = "The original no-auth Main action admission is unavailable.";
        if !self.no_auth_request() || !receipt.matches_record(record) {
            return Err(refused.into());
        }
        let prepared = self.prepare_id_bound(receipt.control.team_id).await?;
        let current = Catalog::new(&self.root)
            .get(&receipt.name)?
            .ok_or(refused)?;
        if !receipt.agrees(
            self.no_auth_request(),
            &current,
            &prepared.control,
            &prepared.configuration_sha256,
            &prepared.actor,
        ) {
            return Err(refused.into());
        }
        Ok(())
    }

    /// At most one owned blocking sweep; expired process shutdown cannot stall JWT renewal
    /// for healthy host jobs or leave overlapping sweeps queued on the runtime.
    pub(crate) fn expire_host_jobs(&self) {
        use std::sync::atomic::Ordering;
        let Ok(jobs) = self.host_jobs() else {
            return;
        };
        if self
            .host_expiry_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        struct Running(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Running {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let running = Running(self.host_expiry_running.clone());
        tokio::task::spawn_blocking(move || {
            let _running = running;
            jobs.expire();
        });
    }

    /// Bind main's live authorization state without retaining its backends in a reference cycle.
    pub fn bind_state(&self, state: &meerschaum_server::AppState) -> Result<(), String> {
        self.state
            .set(Arc::new(state.weak_upgrade()))
            .map_err(|_| "The executor controller already belongs to a server.".into())
    }

    fn state(&self) -> Result<meerschaum_server::AppState, String> {
        self.state
            .get()
            .and_then(|lookup| lookup())
            .ok_or_else(|| "The main API's current authorization state is unavailable.".into())
    }

    fn config(&self) -> Result<Config, String> {
        let config = Config::new(self.options.clone())
            .map_err(|_| "Could not read executor controller configuration.")?;
        config.set_config(self.snapshot.clone());
        config.refresh_connectors();
        Ok(config)
    }

    /// Resolve a resource's team, or the current caller's primary team. Administrators acting
    /// on a team-owned resource follow its executor too. No selection preserves host behavior.
    pub async fn selected(&self, resource_team: Option<&str>) -> Result<Option<Team>, String> {
        let caller = meerschaum_server::executor::context::current();
        let name = resource_team.map(str::to_string).or_else(|| {
            caller
                .as_ref()
                .filter(|a| !a.no_auth && !a.full_admin())
                .map(|a| a.primary_team.clone())
        });
        let Some(name) = name.filter(|n| !same_team(Some(n), Some(EVERYONE))) else {
            return Ok(None);
        };
        let team = self
            .instance
            .get_team(&name)
            .await
            .map_err(|_| "Could not read the selected team's current execution settings.")?
            .ok_or("The team selected for execution no longer exists.")?;
        let config = self.config()?;
        let policy = policy(&config)?;
        let settings = team.execution().map_err(|e| e.to_string())?;
        if settings.mode == Mode::Host {
            policy.check(&team, true).map_err(|e| e.to_string())?;
            return Ok(None);
        }
        Ok(Some(team))
    }

    /// Apply the current filtered config before admitting an operation and validate a signed
    /// health response. Network, version and configuration failures never fall back to host.
    pub async fn prepare(&self, team: &Team) -> Result<Arc<APIConnector>, String> {
        self.prepare_bound(team).await.map(|prepared| prepared.api)
    }

    pub(crate) async fn prepare_bound(&self, team: &Team) -> Result<PreparedExecutor, String> {
        let id = team
            .id
            .as_deref()
            .and_then(|v| Uuid::parse_str(v).ok())
            .ok_or("A separate executor needs a registered UUID team.")?;
        let current = self
            .instance
            .get_teams()
            .await
            .map_err(|_| "Could not read current team execution settings.")?
            .into_iter()
            .find(|t| t.id.as_deref().and_then(|v| Uuid::parse_str(v).ok()) == Some(id))
            .ok_or("The selected executor's team has been deleted.")?;
        let settings = current.execution().map_err(|e| e.to_string())?;
        if !settings.mode.is_remote() {
            return Err("The team no longer selects a separate executor.".into());
        }
        let config = self.config()?;
        let (record, authority) = Registry::new(&self.root).authority(id)?;
        if record.settings != settings {
            return Err("The executor's desired settings have changed; an administrator must reconcile them with `mrsm stack up`.".into());
        }
        let scope = TeamScope::of(
            &current,
            &config
                .get(&["meerschaum", "connectors"])
                .map(|v| v.to_map())
                .unwrap_or_default(),
        );
        let configuration = Configuration {
            catalog_sha256: None,
            team_id: id,
            team_name: current.name.clone(),
            generation: record.verifier.generation,
            plugins: scope.plugins.clone(),
            config: team_config(&config, &scope),
        };
        if settings.mode == Mode::Container {
            refuse_file_instances(&configuration.config)?;
        }
        let actor = meerschaum_server::executor::context::current()
            .map(|caller| meerschaum_server::executor::context::actor(&caller))
            .unwrap_or_else(|| Actor {
                owner: "server".into(),
                username: None,
                permissions: ["*".into()].into_iter().collect(),
                administrator: true,
            });
        let revision = configuration
            .fingerprint()
            .map_err(|_| "Could not encode the executor configuration.")?;
        let connector = match settings.mode {
            Mode::Api => {
                let label = settings
                    .connector
                    .as_deref()
                    .and_then(|s| s.strip_prefix("api:"))
                    .ok_or("Invalid executor connector.")?;
                if !config
                    .get(&["meerschaum", "connectors", "api", label])
                    .is_some_and(|v| v.as_map().is_some())
                {
                    return Err("The selected API executor connector is not defined.".into());
                }
                APIConnector::from_config(&config, label)
                    .map_err(|_| "Could not configure the selected executor connection.")?
            }
            Mode::Container => APIConnector::new(
                Some(&record.key),
                Map::from_iter([
                    ("host".into(), format!("executor-{}", record.key).into()),
                    ("port".into(), Value::Int(8989)),
                    ("protocol".into(), "http".into()),
                ]),
            )
            .map_err(|_| "Could not configure the container executor connection.")?,
            Mode::Host => return Err("The team no longer selects a separate executor.".into()),
        }
        .with_request_authentication(
            Arc::new(Signer {
                registry_root: self.root.clone(),
                authority,
                actor: actor.clone(),
                revision: revision.clone(),
            }),
            16 * 1024 * 1024,
        )
        .map_err(|_| "Could not create the authenticated executor connection.")?;
        let configuration_started = std::time::Instant::now();
        let response = connector
            .make_request(
                Method::Put,
                CONFIGURATION_PATH,
                RequestOptions {
                    body: Body::Json(
                        serde_json::to_string(&configuration)
                            .map_err(|_| "Could not encode executor configuration.")?,
                    ),
                    timeout: Some(std::time::Duration::from_secs(70)),
                    ..Default::default()
                },
            )
            .await
            .map_err(
                |_| "The selected executor is unavailable; an administrator must reconcile it.",
            )?;
        if response.status != 200 {
            configuration_diagnostics::refused(&response, configuration_started.elapsed());
            return Err("The selected executor refused its configuration; check its generation, release and runtime health.".into());
        }
        let health = connector
            .get(HEALTH_PATH, RequestOptions::default())
            .await
            .map_err(|_| "The selected executor did not answer its health probe.")?;
        let health = health
            .json()
            .map_err(|_| "The executor returned invalid health metadata.")?;
        if health["ready"] != true
            || health["version"] != env!("CARGO_PKG_VERSION")
            || health["protocol"] != meerschaum_server::executor::control::PROTOCOL
            || health["team_id"] != id.to_string()
            || health["generation"] != record.verifier.generation.to_string()
            || health["configuration_sha256"] != revision
        {
            return Err(
                "The selected executor's identity, version or configuration does not match.".into(),
            );
        }
        policy(&config)?
            .check(&current, true)
            .map_err(|e| e.to_string())?;
        Ok(PreparedExecutor {
            api: Arc::new(connector),
            team_pipe_authorities: team_pipe_job::authorities(&current, &configuration),
            actor,
            configuration_sha256: revision,
            control: record.verifier,
        })
    }

    /// Follow main-owned job placement by UUID, regardless of the caller's primary team.
    pub async fn prepare_id(&self, id: Uuid) -> Result<Arc<APIConnector>, String> {
        self.prepare_id_bound(id).await.map(|prepared| prepared.api)
    }

    async fn prepare_id_bound(&self, id: Uuid) -> Result<PreparedExecutor, String> {
        let team = self
            .instance
            .get_teams()
            .await
            .map_err(|_| "Could not read current executor placement.")?
            .into_iter()
            .find(|team| team.id.as_deref().and_then(|v| Uuid::parse_str(v).ok()) == Some(id))
            .ok_or("This job's executor team has been deleted.")?;
        self.prepare_bound(&team).await
    }

    /// Execute a whole operation as a tracked, revocable team job. Main-owned metadata survives
    /// a lost HTTP response; configuration changes stop its process through the jobs backend.
    pub async fn run(
        self: &Arc<Self>,
        team: &Team,
        sysargs: Vec<String>,
    ) -> Result<(meerschaum_core::SuccessTuple, String), String> {
        self.run_tracked(Some(team.name.clone()), "executor_action", sysargs)
            .await
    }

    /// Ordinary authenticated Host actions use their original source's canonical API worker.
    pub async fn run_host_api(
        self: &Arc<Self>,
        sysargs: Vec<String>,
    ) -> Result<(meerschaum_core::SuccessTuple, String), String> {
        let caller = meerschaum_server::executor::context::current()
            .filter(|caller| !caller.no_auth && !caller.full_admin() && caller.auth.is_some())
            .ok_or("The Host action has no authenticated original source.")?;
        if meerschaum_server::team_execution::code_running_team(&caller).is_some() {
            return Err("This command needs its authorized Team execution path.".into());
        }
        if self.selected(None).await?.is_some() {
            return Err(
                "This source selects a remote executor; Host fallback is unavailable.".into(),
            );
        }
        let team = (!meerschaum_core::access::is_everyone(Some(&caller.primary_team)))
            .then(|| caller.primary_team.clone());
        self.run_tracked(team, "api_action", sysargs).await
    }

    /// Preserve a typed endpoint's data scopes while tracking its confined worker.
    pub async fn run_pipe_operation(
        self: &Arc<Self>,
        operation: meerschaum_server::executor::pipe_operation::PipeOperation,
        session: &crate::native::Session,
    ) -> Result<(meerschaum_core::SuccessTuple, String), String> {
        self.run_captured_pipe(operation, None, session).await
    }

    pub(crate) async fn run_direct_action(
        self: &Arc<Self>,
        input: meerschaum_server::executor::pipe_operation::DirectAction,
        session: &crate::native::Session,
    ) -> Result<(meerschaum_core::SuccessTuple, String), String> {
        let state = self.state()?;
        let source = meerschaum_server::executor::context::current()
            .ok_or("The direct action lost its original source.")?;
        let caller = meerschaum_server::executor::context::refresh_direct_action(&state, &source)
            .await
            .map_err(|error| error.detail_text())?;
        let request = input
            .resolve(&state, &caller)
            .await
            .map_err(|error| error.detail_text())?;
        self.run_captured_pipe(request, Some(input), session).await
    }

    async fn run_captured_pipe(
        self: &Arc<Self>,
        mut operation: meerschaum_server::executor::pipe_operation::PipeOperation,
        direct: Option<meerschaum_server::executor::pipe_operation::DirectAction>,
        session: &crate::native::Session,
    ) -> Result<(meerschaum_core::SuccessTuple, String), String> {
        let operation_clock = crate::types::DateContext::system();
        let caller = meerschaum_server::executor::context::current()
            .ok_or("The pipe operation lost its original source.")?;
        caller
            .require(&["pipes:write"], &[])
            .map_err(|error| error.detail_text())?;
        let state = self.state()?;
        let caller = if direct.is_some() {
            meerschaum_server::executor::context::refresh_direct_action(&state, &caller)
                .await
                .map_err(|error| error.detail_text())?
        } else {
            caller
        };
        let argv = self
            .capture_registered_pipe(&state, &caller, &mut operation, &operation_clock)
            .await?;
        let operation = match direct {
            Some(input) => {
                meerschaum_server::executor::context::refresh_direct_action(&state, &caller)
                    .await
                    .map_err(|error| error.detail_text())?;
                meerschaum_server::executor::pipe_operation::GraphOperation::Direct(
                    meerschaum_server::executor::pipe_operation::ActionPipe {
                        input,
                        normalized_at: operation_clock.now_utc.and_utc().timestamp(),
                        pipe: operation,
                    },
                )
            }
            None => operation.into(),
        };
        meerschaum_server::executor::context::graph_operation_scope(Arc::new(operation), async {
            match self.command_team(&argv, session).await? {
                Some(team) => self.run(&team, argv).await,
                None => self.run_host_api(argv).await,
            }
        })
        .await
    }

    async fn capture_registered_pipe(
        &self,
        state: &meerschaum_server::AppState,
        caller: &meerschaum_server::access::Access,
        operation: &mut meerschaum_server::executor::pipe_operation::PipeOperation,
        operation_clock: &crate::types::DateContext,
    ) -> Result<Vec<String>, String> {
        let parts: Vec<_> = operation.pipe_ref.split('|').collect();
        if parts.len() != 4
            || !parts[0]
                .split_once(':')
                .is_some_and(|(kind, _)| matches!(kind, "sql" | "api" | "valkey"))
            || !meerschaum_server::access::guard::may_use_connector(state, caller, parts[0])
        {
            return Err("The pipe operation has no authorized native logical instance.".into());
        }
        let conn = state
            .get_api_connector(Some(parts[0]))
            .map_err(|error| error.detail_text())?;
        let pipe = meerschaum_core::Pipe::builder(parts[1], parts[2])
            .location((parts[3] != "None").then_some(parts[3]))
            .instance(parts[0])
            .build()
            .map_err(|_| "Invalid canonical pipe operation.")?;
        operation.registration = conn
            .get_pipe_id(&pipe)
            .await
            .map_err(|_| "Could not capture the original pipe registration.")?
            .map(|id| id.to_value())
            .ok_or("The pipe operation requires its registered identity.")?;
        pipe_bounds::capture(operation, operation_clock)?;
        let argv = operation.sysargs().map_err(|error| error.detail_text())?;
        operation
            .authorize(state, caller, &argv)
            .await
            .map_err(|error| error.detail_text())?;
        operation.metadata = Some(
            pipe_operation::capture(
                state,
                caller,
                operation,
                &meerschaum_engine::EngineConfig::from_config(&self.config()?, Some(&self.root)),
            )
            .await
            .map_err(|error| error.detail_text())?,
        );
        Ok(argv)
    }

    async fn run_tracked(
        self: &Arc<Self>,
        team: Option<String>,
        origin: &str,
        sysargs: Vec<String>,
    ) -> Result<(meerschaum_core::SuccessTuple, String), String> {
        use meerschaum_server::backends::{CreateJob, JobsBackend, LogEvent, NoJobs};
        let config = self.config()?;
        let server = meerschaum_server::ServerConfig::from_config(&config, &Default::default());
        let caller = meerschaum_server::executor::context::current();
        let owner = caller.as_ref().and_then(|caller| {
            caller
                .username()
                .map(str::to_string)
                .or_else(|| meerschaum_server::routes::execute::owner_label(caller.auth.as_ref()))
        });
        let name = format!("execute-{}", Uuid::new_v4().simple());
        let mut properties = Map::new();
        meerschaum_core::job_access::JobAccess::stamp(
            &mut properties,
            owner.as_deref(),
            team.as_deref(),
        );
        properties.insert("mrsm_execution".into(), Value::from_json_str(&serde_json::json!({
            "kind": "temporary_execution", "origin": origin, "owner": owner,
            "retention": {"success_seconds": server.execution_retention[0], "failure_seconds": server.execution_retention[1], "summary_seconds": server.execution_retention[2]}
        }).to_string()).map_err(|_| "Could not encode delegated execution metadata.")?);
        // Captured JSON plans and output retain their original text rather than timestamps.
        properties.insert(
            "logs".into(),
            Value::Map(Map::from_iter([
                ("write_timestamps".into(), Value::Bool(false)),
                ("lines_to_show".into(), Value::Int(0)),
            ])),
        );
        let jobs =
            crate::executor::jobs::DelegatingJobs::new(&self.root, Arc::new(NoJobs), self.clone());
        // This is Main's exact typed operation, not the public legacy jobs input shape.
        // The legacy cleaner deletes literal `api` data values and their following argument.
        meerschaum_server::executor::admission::user_sysargs(&sysargs)
            .map_err(|error| error.detail_text())?;
        let mut no_auth = None;
        match meerschaum_server::executor::context::action_request_scope(Box::pin(
            jobs.create_canonical(&name, sysargs, Some(properties), Some(&mut no_auth)),
        ))
        .await
        {
            CreateJob::Started(result) if result.success => {}
            CreateJob::Started(result) => return Err(result.message),
            CreateJob::Unavailable(error) => return Err(error),
            CreateJob::Exists => {
                return Err("The delegated execution name is already reserved.".into());
            }
        }
        let mut monitor = jobs
            .monitor_logs(&name)
            .await
            .ok_or_else(|| format!("The executor's logs are unavailable; inspect Job '{name}'."))?;
        let mut output = String::new();
        while let Some(event) = monitor.events.recv().await {
            match event {
                LogEvent::Line(line) => {
                    if output.len().saturating_add(line.len()) > 16 * 1024 * 1024 {
                        return Err(format!(
                            "Captured output exceeds its size limit; inspect Job '{name}'."
                        ));
                    }
                    output.push_str(&line);
                }
                LogEvent::Stopped(_) => {
                    return Ok((
                        Box::pin(self.final_tracked_result(&name, no_auth.as_ref())).await?,
                        output,
                    ));
                }
                // Input remains on the main API's authorized job websocket.
                LogEvent::StdinRequested => {}
            }
        }
        Ok((
            Box::pin(self.final_tracked_result(&name, no_auth.as_ref())).await?,
            output,
        ))
    }

    /// PID exit and log-stream closure precede the supervisor's atomic final result.
    /// Consult only the private canonical placement, retaining the original operation.
    async fn final_tracked_result(
        &self,
        name: &str,
        no_auth: Option<&NoAuthAction>,
    ) -> Result<meerschaum_core::SuccessTuple, String> {
        let root = self.root.clone();
        let name_owned = name.to_string();
        let record = tokio::task::spawn_blocking(move || Catalog::new(&root).get(&name_owned))
            .await
            .map_err(|_| "The canonical execution lookup was interrupted.")??
            .ok_or("The canonical execution is unavailable.")?;
        if no_auth.is_some_and(|receipt| !receipt.matches_record(&record)) {
            return Err("The original no-auth Main action admission is unavailable.".into());
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(35);
        loop {
            let body = if record.placement == crate::executor::jobs::JobPlacement::HostApi {
                let host = self.host_jobs()?;
                context::scope(None, host.backend().try_get(&record.remote_name))
                    .await?
                    .ok_or("The canonical execution result is unavailable.")?
            } else {
                let prepared = self.prepare_id_bound(record.team_id).await?;
                if no_auth.is_some_and(|receipt| {
                    !receipt.agrees(
                        self.no_auth_request(),
                        &record,
                        &prepared.control,
                        &prepared.configuration_sha256,
                        &prepared.actor,
                    )
                }) {
                    return Err("The original no-auth Main action admission is unavailable.".into());
                }
                let api = prepared.api;
                api.clear_job_metadata_cache();
                serde_json::to_value(
                    api.get_job_metadata(&record.remote_name)
                        .await
                        .map_err(|_| "The canonical execution result is unavailable.")?,
                )
                .map_err(|_| "The executor returned invalid execution metadata.")?
            };
            if let Some((success, message)) = completed_result(&body) {
                if let Some(receipt) = no_auth {
                    Box::pin(self.validate_no_auth_action(receipt, &record)).await?;
                } else {
                    Box::pin(self.validate_job_result_source(&record)).await?;
                }
                if let Some(extension) = &record.extension {
                    if !success {
                        return Err(message.chars().take(2048).collect());
                    }
                    extension
                        .validate_reply(message.as_bytes())
                        .map_err(|error| error.detail_text())?;
                    return Ok(meerschaum_core::SuccessTuple {
                        success: true,
                        message: message.into(),
                    });
                }
                let mut sanitizer = crate::executor::jobs::LogSanitizer::default();
                return Ok(meerschaum_core::SuccessTuple {
                    success,
                    message: sanitizer.push(message).chars().take(16 * 1024).collect(),
                });
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!(
                    "The final execution result is unavailable; inspect Job '{name}'."
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(75)).await;
        }
    }

    /// Maintain authenticated leases for configured executors, independent of active browser
    /// sessions and user requests. A main API outage lets executors stop work after their lease.
    pub async fn maintain(self: Arc<Self>) {
        // The terminal owner checks have their own budget: one unreachable team's setup must
        // not prevent detached terminals on healthy executors from renewing their leases.
        let mut owner_maintenance = tokio::task::JoinSet::new();
        owner_maintenance.spawn(crate::executor::jobs::DelegatingJobs::run_schedules(
            self.clone(),
            self.root.clone(),
        ));
        let host_owners = self.clone();
        owner_maintenance.spawn(async move {
            let mut ticks = tokio::time::interval(std::time::Duration::from_secs(15));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                host_owners.maintain_host_terminals().await;
            }
        });
        let owners = self.clone();
        owner_maintenance.spawn(async move {
            let mut ticks = tokio::time::interval(std::time::Duration::from_secs(15));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                owners.maintain_terminals().await;
            }
        });
        let jobs = self.clone();
        owner_maintenance.spawn(async move {
            let mut ticks = tokio::time::interval(std::time::Duration::from_secs(15));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticks.tick().await;
                jobs.maintain_jobs().await;
            }
        });
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let teams = match self.instance.get_teams().await {
                Ok(teams) => teams,
                Err(_) => {
                    tracing::warn!(
                        "Executor leases were not renewed: current teams are unavailable."
                    );
                    continue;
                }
            };
            // Only main reads the authoritative store. Changes made by another main worker
            // or an administrator through SQL must retire old signing generations too.
            if Registry::new(&self.root).reconcile(&teams).is_err() {
                tracing::warn!("Executor leases were not renewed: identity reconciliation failed.");
                continue;
            }
            let mut probes = tokio::task::JoinSet::new();
            for team in teams
                .into_iter()
                .filter(|team| team.execution().is_ok_and(|s| s.mode.is_remote()))
            {
                if probes.len() >= 32 {
                    let _ = probes.join_next().await;
                }
                let controller = self.clone();
                probes.spawn(async move {
                    if controller.prepare(&team).await.is_err() {
                        tracing::debug!(
                            team_id = team.id.as_deref(),
                            "The selected team executor is unavailable."
                        );
                    }
                });
            }
            while probes.join_next().await.is_some() {}
        }
    }
}

/// The outward metadata result is read before properties and may predate completion.
/// Consume the result and ended marker from the same committed properties snapshot.
pub(crate) fn completed_result(body: &serde_json::Value) -> Option<(bool, &str)> {
    let properties = &body["daemon"]["properties"];
    if body["status"] != "stopped"
        || body["completion_pending"] == true
        || body["completion_unavailable"] == true
    {
        return None;
    }
    let process = serde_json::from_value::<Value>(properties["process"].clone()).ok()?;
    let timestamps = meerschaum_jobs::detached::Timestamps::from_properties(&Map::from_iter([(
        "process".into(),
        process,
    )]));
    if !timestamps.is_completed() {
        return None;
    }
    let result = properties["result"].as_array()?;
    if result.len() != 2 {
        return None;
    }
    Some((result[0].as_bool()?, result[1].as_str()?))
}

fn policy(config: &Config) -> Result<Policy, String> {
    match config.get(&["api", "execution", "require_team_executor"]) {
        None | Some(Value::Null) => Ok(Policy::default()),
        Some(Value::Bool(required)) => Ok(Policy {
            require_team_executor: required,
        }),
        Some(_) => Err("api:execution:require_team_executor must be a boolean.".into()),
    }
}

fn refuse_file_instances(config: &Map) -> Result<(), String> {
    let Some(Value::Map(meerschaum)) = config.get("meerschaum") else {
        return Ok(());
    };
    let Some(Value::Map(connectors)) = meerschaum.get("connectors") else {
        return Ok(());
    };
    if let Some(Value::Map(sql)) = connectors.get("sql") {
        for (label, attributes) in sql {
            if label == "default" || attributes.is_null() {
                continue;
            }
            if let Value::Map(attributes) = attributes {
                let file = attributes
                    .get("flavor")
                    .and_then(Value::as_str)
                    .is_some_and(|s| matches!(s, "sqlite" | "duckdb" | "geopackage"));
                let uri_file = attributes
                    .get("uri")
                    .and_then(Value::as_str)
                    .is_some_and(|s| {
                        ["sqlite:", "duckdb:", "geopackage:"]
                            .iter()
                            .any(|prefix| s.starts_with(prefix))
                    });
                if file || uri_file {
                    return Err("A container executor cannot open a main-host SQLite, DuckDB or GeoPackage path. Use a network database before selecting container execution.".into());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod no_auth_action_tests {
    use super::*;
    use crate::executor::jobs::Record;

    #[tokio::test]
    async fn no_auth_action_result_requires_its_live_mode_original_launch_and_bound_executor() {
        let team = Uuid::new_v4();
        let (authority, _) = Authority::generate(Uuid::new_v4(), team, Uuid::new_v4()).unwrap();
        let control = authority.verifier();
        let actor = context::actor(&meerschaum_server::access::Access::everything());
        let mut record = Record::new(
            "execute-fixture",
            team,
            vec!["show".into(), "version".into()],
            Map::new(),
        );
        record.action_request = true;
        let receipt = NoAuthAction {
            name: record.name.clone(),
            remote_name: record.remote_name.clone(),
            revision: record.context_revision,
            argv: record.sysargs.clone(),
            properties: record.properties.clone(),
            control: control.clone(),
            actor: actor.clone(),
            configuration_sha256: "first".into(),
        };
        let root = tempfile::tempdir().unwrap();
        let instance = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", root.path().join("main.db").display()),
            )
            .await
            .unwrap(),
        );
        let bind = |no_auth| {
            let mut config = meerschaum_server::ServerConfig::for_root("sql:main", root.path());
            config.no_auth = no_auth;
            let state = meerschaum_server::AppState::builder(config, instance.clone())
                .build()
                .unwrap();
            let controller = Controller::new(
                root.path().into(),
                instance.clone(),
                ConfigOptions::with_env(vec![(
                    "MRSM_ROOT_DIR".into(),
                    root.path().display().to_string(),
                )]),
                Map::new(),
            );
            controller.bind_state(&state).unwrap();
            (state, controller)
        };
        let (state, main) = bind(true);
        assert!(receipt.agrees(main.no_auth_request(), &record, &control, "first", &actor));
        drop(state);
        assert!(!main.no_auth_request());
        let (_authenticated, changed_main) = bind(false);
        assert_eq!(
            changed_main
                .validate_no_auth_action(&receipt, &record)
                .await
                .unwrap_err(),
            "The original no-auth Main action admission is unavailable."
        );
        assert!(!receipt.agrees(false, &record, &control, "first", &actor));
        assert!(!receipt.agrees(true, &record, &control, "changed", &actor));
        let mut replaced = control.clone();
        replaced.generation = Uuid::new_v4();
        assert!(!receipt.agrees(true, &record, &replaced, "first", &actor));
        let mut other_actor = actor.clone();
        other_actor.owner = "another-actor".into();
        assert!(!receipt.agrees(true, &record, &control, "first", &other_actor));
        let mut changed = record.clone();
        changed.callback = Some(Uuid::new_v4());
        assert!(!receipt.matches_record(&changed));
        changed = record.clone();
        changed.context_revision += 1;
        assert!(!receipt.matches_record(&changed));
        changed = record.clone();
        changed.remote_name = format!("mrsm-{}", Uuid::new_v4().simple());
        assert!(!receipt.matches_record(&changed));
    }
}
