//! Private, source-bound API terminals on Main. Console sessions never become shell credentials.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use meerschaum_core::access::same_team;
use meerschaum_core::team_scope::{create_private_dir, write_private};
use meerschaum_jobs::supervisor::LifecycleOwner;
use meerschaum_server::access::Access;
use meerschaum_server::auth::{self, Principal, Scopes};
use meerschaum_server::executor::terminal::TerminalGrant;
use meerschaum_server::executor::{callbacks, environment, terminal};
use meerschaum_server::webterm::remote::Terminal;
use meerschaum_server::webterm::{ShellCommand, WebtermConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{Controller, TerminalStorageError};
use crate::executor_contexts::{Contexts, HostInputs};

const MAX_RECORDS: usize = 512;
const MAX_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    team_id: Option<Uuid>,
    grant: TerminalGrant,
    credential: String,
    permissions: BTreeSet<String>,
    callback: Uuid,
    snapshot: String,
    owner: LifecycleOwner,
    version: String,
    #[serde(default)]
    source_ceiling: Option<meerschaum_server::executor::source_ceiling::Ceiling>,
}

#[derive(Clone)]
struct Catalog(PathBuf);
impl Catalog {
    fn update<T>(
        &self,
        operation: impl FnOnce(&mut Vec<Record>) -> Result<T, String>,
    ) -> Result<T, TerminalStorageError> {
        use std::io::Read;
        create_private_dir(&self.0).map_err(|_| "Could not prepare host terminal storage.")?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.0.join("lock"))
            .map_err(|_| "Could not open the host terminal catalog lock.")?;
        fs4::FileExt::try_lock(&lock).map_err(|error| match error {
            fs4::TryLockError::WouldBlock => TerminalStorageError::Busy,
            fs4::TryLockError::Error(_) => {
                TerminalStorageError::from("Host terminal identities are unavailable.")
            }
        })?;
        let path = self.0.join("catalog.json");
        let mut records: Vec<Record> = match std::fs::symlink_metadata(&path) {
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && !self.0.join("initialized").exists() =>
            {
                Vec::new()
            }
            Ok(metadata) if metadata.is_file() && metadata.len() <= MAX_BYTES => {
                let file = std::fs::File::open(&path)
                    .map_err(|_| "Could not read host terminal identities.")?;
                let mut bytes = Vec::new();
                file.take(MAX_BYTES + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|_| "Could not read host terminal identities.")?;
                if bytes.len() as u64 > MAX_BYTES {
                    return Err("Host terminal identities exceed their byte bound.".into());
                }
                serde_json::from_slice(&bytes)
                    .map_err(|_| "Invalid host terminal identity storage.")?
            }
            _ => return Err("Host terminal identity storage is unavailable.".into()),
        };
        let mut ids = BTreeSet::new();
        if records.len() > MAX_RECORDS
            || records.iter().any(|record| {
                record.grant.id.is_nil()
                    || record.callback.is_nil()
                    || record.team_id.is_some_and(|id| id.is_nil())
                    || record.grant.owner.is_empty()
                    || record.grant.owner.len() > 256
                    || record.grant.owner_id.is_empty()
                    || record.credential.is_empty()
                    || record.permissions.len() > 256
                    || record.snapshot.len() != 64
                    || record
                        .source_ceiling
                        .as_ref()
                        .is_some_and(|ceiling| ceiling.validate().is_err())
                    || !ids.insert(record.grant.id)
            })
        {
            return Err("Invalid host terminal identities.".into());
        }
        let result = operation(&mut records)?;
        if records.len() > MAX_RECORDS {
            return Err("Host terminal capacity is exhausted.".into());
        }
        let bytes = serde_json::to_string(&records)
            .map_err(|_| "Could not encode host terminal identities.")?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err("Host terminal identities exceed their byte bound.".into());
        }
        write_private(&path, &bytes).map_err(|_| "Could not persist host terminal identities.")?;
        write_private(&self.0.join("initialized"), "1")
            .map_err(|_| "Could not initialize host terminal identities.")?;
        Ok(result)
    }
}

pub(crate) struct HostTerminals {
    owner: Option<LifecycleOwner>,
    contexts: Arc<Contexts>,
    config: Mutex<Option<WebtermConfig>>,
    catalog: Catalog,
    catalog_gate: tokio::sync::Mutex<()>,
}

impl HostTerminals {
    fn new(root: &Path) -> Self {
        Self {
            owner: LifecycleOwner::current().ok(),
            contexts: Arc::new(Contexts::new(root.into())),
            config: Mutex::new(None),
            catalog: Catalog(
                root.join(crate::executor_registry::DIRECTORY)
                    .join("host-terminals"),
            ),
            catalog_gate: tokio::sync::Mutex::new(()),
        }
    }

    fn configuration(&self, state: &meerschaum_server::AppState) -> Result<WebtermConfig, String> {
        let mut current = self
            .config
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(config) = current.as_ref() {
            return Ok(config.clone());
        }
        let native = meerschaum_core::self_exe::native_executable()
            .ok_or("Host API terminals require the standalone native launcher.")?;
        let mut config = WebtermConfig::headless(
            state,
            ShellCommand::new(native.to_string_lossy().into_owned()),
        );
        let directory = self.catalog.0.join("tmux");
        create_private_dir(&directory)
            .map_err(|_| "Could not prepare private host terminal resources.")?;
        config.policy_dir = directory.clone();
        if let Some(tmux) = config.tmux.as_mut() {
            tmux.socket = Some(
                tmux.socket
                    .as_ref()
                    .ok_or("A host terminal requires a dedicated tmux socket.")?
                    .with_extension("host-api"),
            );
            tmux.env_dir = directory;
            tmux.config_file = None;
        }
        *current = Some(config.clone());
        Ok(config)
    }

    async fn update_catalog<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Vec<Record>) -> Result<T, String> + Send + 'static,
    ) -> Result<T, TerminalStorageError> {
        let _gate = self.catalog_gate.lock().await;
        let catalog = self.catalog.clone();
        tokio::task::spawn_blocking(move || catalog.update(operation))
            .await
            .map_err(|_| TerminalStorageError::from("The host terminal identity task failed."))?
    }

    async fn current(&self, record: &Record) -> Result<bool, TerminalStorageError> {
        let record = record.clone();
        self.update_catalog(move |records| Ok(records.iter().any(|current| current == &record)))
            .await
    }

    fn inputs(
        &self,
        state: &meerschaum_server::AppState,
        caller: &Access,
        config: &meerschaum_core::Config,
        environment: &[(String, String)],
    ) -> Result<(HostInputs, String), String> {
        let base = crate::executor_host_jobs::filtered_base(state, caller)?;
        let (sources, grants) =
            crate::executor_host_jobs::plugin_sources(caller, &config.paths().plugins_dirs)?;
        let files = crate::executor_plugin_files::Store::new(&state.config.root_dir)?
            .snapshot(&sources, None)?;
        let ceiling =
            meerschaum_server::executor::source_ceiling::Ceiling::from_files(&files.hashes)?;
        if let Some(Principal::Execution(identity)) = &caller.auth
            && identity.grant.source_ceiling.clone().unwrap_or_default() != ceiling
        {
            return Err("The Host terminal's actual source content differs from its captured ceiling. Open a fresh terminal.".into());
        }
        let bytes = serde_json::to_vec(&(
            &base,
            &grants,
            &files.hashes,
            &files.view_sha256,
            &caller.principal.permissions,
        ))
        .map_err(|_| "Could not identify the terminal's source snapshot.")?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err("The terminal's source snapshot exceeds its byte bound.".into());
        }
        let snapshot = Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let mut environment = environment.to_vec();
        environment.retain(|(name, _)| {
            !matches!(name.as_str(), "MRSM_PLUGINS_DIR" | "MRSM_EXECUTOR_PLUGINS")
        });
        environment.push((
            "MRSM_EXECUTOR_PLUGINS".into(),
            serde_json::to_string(&grants).map_err(|_| "Invalid terminal plugin grants.")?,
        ));
        Ok((
            HostInputs {
                base,
                environment,
                sources: files.sources,
            },
            snapshot,
        ))
    }

    async fn retire(&self, record: &Record) -> Result<(), String> {
        let contexts = self.contexts.clone();
        let record = record.clone();
        let config = self
            .config
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        tokio::task::spawn_blocking(move || {
            let actor = meerschaum_server::executor::control::Actor {
                owner: "main".into(),
                username: None,
                permissions: Default::default(),
                administrator: true,
            };
            contexts.revoke(&actor, record.grant.id, Some(1))?;
            if let Some(config) = config
                && let Some(mut tmux) = config.tmux
            {
                if let Some(socket) = tmux.socket.as_ref() {
                    tmux.socket = Some(meerschaum_server::webterm::restricted_socket(socket));
                }
                let session = meerschaum_server::webterm::tmux_session_name(
                    &terminal_name(record.grant.id),
                    &tmux.session_suffix,
                );
                config.policy.forget_session(&session);
                meerschaum_server::webterm::end_tmux_session(&tmux, &session);
            }
            Ok(())
        })
        .await
        .map_err(|_| "Host terminal retirement failed.".to_string())?
    }
}

fn terminal_name(id: Uuid) -> String {
    format!("host-api-terminal-{}", id.simple())
}

fn trace_lease_failure(record: &Record, reason: &'static str) {
    tracing::debug!(target: "mrsm::terminal_transport", layer = "host_lease", launch = %record.grant.id, reason, "The original terminal lease could not renew.");
}

impl Controller {
    async fn host_terminal_inputs(
        &self,
        runtime: &Arc<HostTerminals>,
        caller: &Access,
    ) -> Result<(HostInputs, String), String> {
        let (runtime, state, config, environment, caller) = (
            runtime.clone(),
            self.state()?,
            self.config()?,
            self.options.env.clone(),
            caller.clone(),
        );
        tokio::task::spawn_blocking(move || runtime.inputs(&state, &caller, &config, &environment))
            .await
            .map_err(|_| "The terminal source snapshot could not be prepared.".to_string())?
    }

    pub fn bind_host_terminals(&self) -> Result<(), String> {
        self.host_terminals
            .set(Arc::new(HostTerminals::new(&self.root)))
            .map_err(|_| "The host terminal runtime is already bound.".into())
    }

    fn host_terminals(&self) -> Result<Arc<HostTerminals>, String> {
        self.host_terminals
            .get()
            .cloned()
            .ok_or("The private host terminal runtime is unavailable.".into())
    }

    async fn host_terminal_lock(&self, id: Uuid) -> Result<std::fs::File, String> {
        meerschaum_server::executor::storage::lock(
            self.root.join("executors/host-terminal-leases"),
            id,
        )
        .await
        .map_err(|_| "The host terminal lease is busy or unavailable.".into())
    }

    async fn host_terminal_owner(
        &self,
        owner: &str,
        credential: &str,
        permissions: &BTreeSet<String>,
        team_id: Option<Uuid>,
    ) -> Result<(Access, String), String> {
        let state = self.state()?;
        let principal =
            auth::load_from_bound_subject(&state, owner, Some("user"), Some(credential))
                .await
                .map_err(|_| "Could not verify the terminal's original user credential.")?
                .ok_or("The terminal's original user credential is no longer current.")?;
        let mut access = meerschaum_server::access::access_of_fresh(
            &state,
            principal,
            &Scopes::List(permissions.iter().cloned().collect()),
        )
        .await
        .map_err(|_| "Could not verify current terminal permissions.")?;
        access.credential = Some(credential.into());
        access
            .require(&["webterm:use", "actions:execute", "jobs:execute"], &[])
            .map_err(|_| "Terminal permissions are unavailable.")?;
        if !state.config.actions_non_admin
            || access.is_admin()
            || meerschaum_server::team_execution::code_running_team(&access).is_some()
        {
            return Err("The terminal's execution mode changed. Open it again.".into());
        }
        let current = access
            .team_records
            .iter()
            .find(|team| same_team(Some(&team.name), Some(&access.primary_team)));
        match (team_id, current) {
            (Some(id), Some(team))
                if team
                    .id
                    .as_deref()
                    .and_then(|value| Uuid::parse_str(value).ok())
                    == Some(id)
                    && team.execution().is_ok_and(|settings| {
                        settings.mode == meerschaum_core::execution::Mode::Host
                    }) => {}
            (None, None) if same_team(Some(&access.primary_team), None) => {}
            _ => return Err("The terminal's registered execution placement changed.".into()),
        }
        let user = meerschaum_core::users::User::new(owner, None);
        let owner_id = state
            .instance()
            .get_user_id(&user)
            .await
            .map_err(|_| "Could not identify the terminal owner.")?
            .filter(|id| !id.is_null())
            .ok_or("The terminal owner no longer exists.")?
            .py_str();
        Ok((access, owner_id))
    }

    pub async fn host_terminal(
        self: &Arc<Self>,
        team_id: Option<Uuid>,
        caller: &Access,
    ) -> Result<Terminal, String> {
        let state = self.state()?;
        let runtime = self.host_terminals()?;
        let host_owner = runtime
            .owner
            .clone()
            .ok_or("The native host process identity cannot be proved.")?;
        let owner = caller
            .username()
            .ok_or("A host terminal requires a registered user.")?;
        if !matches!(caller.auth, Some(Principal::User(_))) {
            return Err("This terminal needs its original console user credential.".into());
        }
        let credential = caller
            .credential
            .as_deref()
            .ok_or("The terminal has no original user credential.")?;
        let (access, owner_id) = self
            .host_terminal_owner(owner, credential, &caller.principal.permissions, team_id)
            .await?;
        let url = state
            .config
            .execution_callback_url
            .clone()
            .ok_or("Configure api:execution:callback_url for API-only host terminals.")?;
        environment::validate_callback_url(&url)?;
        let key: [u8; 32] =
            Sha256::digest(format!("{owner_id}\0{team_id:?}\0{credential}").as_bytes()).into();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&key[..16]);
        let _owner_lock = self.host_terminal_lock(Uuid::from_bytes(bytes)).await?;
        let (lookup_owner, lookup_registered, lookup_credential) =
            (owner.to_string(), owner_id.clone(), credential.to_string());
        let existing = runtime
            .update_catalog(move |records| {
                Ok(records
                    .iter()
                    .find(|record| {
                        record.team_id == team_id
                            && record.grant.owner == lookup_owner
                            && record.grant.owner_id == lookup_registered
                            && record.credential == lookup_credential
                    })
                    .cloned())
            })
            .await
            .map_err(|error| error.to_string())?;
        let mut record = if let Some(record) = existing {
            if record.owner != host_owner || record.version != crate::VERSION {
                if record.owner.alive() {
                    return Err("This terminal belongs to another serving process.".into());
                }
                self.end_host_terminal(&record).await;
                return Err("The previous terminal owner stopped. Open a fresh terminal.".into());
            }
            record
        } else {
            let selection = Box::pin(self.capture_sources(
                team_id.unwrap_or_else(Uuid::nil),
                crate::executor_jobs::JobPlacement::HostApi,
                &access,
            ))
            .await?;
            let ceiling = super::source_selection::ceiling(selection.as_ref())?;
            let grant = callbacks::register_with_sources(
                &state,
                &access,
                Uuid::new_v4(),
                team_id,
                &["webterm:use", "actions:execute", "jobs:execute"],
                callbacks::Purpose::Command,
                ceiling.clone(),
            )
            .await
            .map_err(|_| "The terminal's original source cannot authorize callbacks.")?;
            let prepared = async {
                let identity = terminal::api_source(&state, grant.id, grant.launch_id, team_id)
                    .await
                    .map_err(|_| "The terminal source changed.")?;
                let (_, snapshot) = self
                    .host_terminal_inputs(&runtime, &identity.caller())
                    .await?;
                Ok::<_, String>(snapshot)
            }
            .await;
            let snapshot = match prepared {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    let _ = callbacks::revoke(&state, grant.id).await;
                    return Err(error);
                }
            };
            let record = Record {
                team_id,
                grant: TerminalGrant {
                    id: grant.launch_id,
                    owner: owner.into(),
                    owner_id,
                },
                credential: credential.into(),
                permissions: access.principal.permissions.clone(),
                callback: grant.id,
                snapshot,
                owner: host_owner,
                version: crate::VERSION.into(),
                source_ceiling: Some(ceiling),
            };
            let persisted = record.clone();
            if let Err(error) = runtime
                .update_catalog(move |records| {
                    records.push(persisted);
                    Ok(())
                })
                .await
            {
                let _ = callbacks::revoke(&state, grant.id).await;
                return Err(error.to_string());
            }
            record
        };
        let _lease_lock = self.host_terminal_lock(record.grant.id).await?;
        if let Err(error) = self.renew_host_terminal(&runtime, &mut record).await {
            if error != TerminalStorageError::Busy {
                self.end_host_terminal(&record).await;
            }
            return Err(error.to_string());
        }
        let contexts = runtime.contexts.clone();
        let grant = record.grant.clone();
        let runtime_copy = runtime.clone();
        let state_copy = state.clone();
        let config = tokio::task::spawn_blocking(move || {
            let mut config = runtime_copy.configuration(&state_copy)?;
            config.shell = contexts.terminal_shell(grant.id, &grant, &config.shell)?;
            Ok::<_, String>(config)
        })
        .await
        .map_err(|_| "The private terminal runtime could not be prepared.")??;
        self.verify_prepared_host_terminal(&runtime, &record, &config.shell)
            .await?;
        meerschaum_server::webterm::open_api_terminal(config, terminal_name(record.grant.id)).await
    }

    /// This shell is still exclusively host metadata: open_api_terminal has not received it.
    async fn verify_prepared_host_terminal(
        &self,
        runtime: &HostTerminals,
        record: &Record,
        shell: &ShellCommand,
    ) -> Result<(), String> {
        let verified = async {
            let state = self.state()?;
            terminal::api_source(&state, record.callback, record.grant.id, record.team_id)
                .await
                .map_err(|_| "The terminal source changed while opening.")?;
            if !runtime
                .current(record)
                .await
                .map_err(|error| error.to_string())?
            {
                return Err("The terminal source changed while opening.".to_string());
            }
            Ok(())
        }
        .await;
        if let Err(error) = verified {
            let shell = shell.clone();
            let canceled = tokio::task::spawn_blocking(move || {
                meerschaum_server::webterm::cancel_prepared_api_terminal(&shell)
            })
            .await;
            self.end_host_terminal(record).await;
            if !matches!(canceled, Ok(Ok(()))) {
                trace_lease_failure(record, "pre_open_unused_nonce_cancel_refused");
                return Err("The terminal source changed before launch, and its pending lifecycle cleanup could not be proven. An administrator must check the owned runtime before retrying.".into());
            }
            return Err(error);
        }
        Ok(())
    }

    async fn renew_host_terminal(
        &self,
        runtime: &Arc<HostTerminals>,
        record: &mut Record,
    ) -> Result<(), TerminalStorageError> {
        if !runtime.current(record).await? {
            trace_lease_failure(record, "catalog_lease_replaced");
            return Err("This terminal's canonical lease was retired.".into());
        }
        let state = self.state()?;
        let (access, id) = self
            .host_terminal_owner(
                &record.grant.owner,
                &record.credential,
                &record.permissions,
                record.team_id,
            )
            .await
            .inspect_err(|_| {
                trace_lease_failure(record, "original_owner_refused");
            })?;
        if id != record.grant.owner_id {
            trace_lease_failure(record, "registered_owner_changed");
            return Err("The terminal's registered owner changed.".into());
        }
        let identity =
            terminal::api_source(&state, record.callback, record.grant.id, record.team_id)
                .await
                .map_err(|_| {
                    trace_lease_failure(record, "original_callback_refused");
                    "The terminal's source lease ended."
                })?;
        if identity.grant.source_ceiling.clone().unwrap_or_default()
            != record.source_ceiling.clone().unwrap_or_default()
        {
            trace_lease_failure(record, "captured_source_ceiling_changed");
            return Err(
                "The terminal lost its immutable original source ceiling. Open it again.".into(),
            );
        }
        let (inputs, snapshot) = self
            .host_terminal_inputs(runtime, &identity.caller())
            .await
            .inspect_err(|_| {
                trace_lease_failure(record, "source_snapshot_prepare_refused");
            })?;
        if snapshot != record.snapshot {
            trace_lease_failure(record, "source_snapshot_changed");
            return Err(
                "The terminal's grants or source files changed. Open a fresh terminal.".into(),
            );
        }
        callbacks::renew(&state, record.callback, record.grant.id)
            .await
            .map_err(|_| {
                trace_lease_failure(record, "original_callback_renew_refused");
                "The terminal source cannot renew its lease."
            })?;
        let url = state
            .config
            .execution_callback_url
            .clone()
            .ok_or("The host callback origin is unavailable.")?;
        let update = environment::Update {
            metadata_deadline: None,
            extension: None,
            discovery_catalog_sha256: None,
            registration: None,
            id: record.grant.id,
            revision: 1,
            owner: record.grant.owner.clone(),
            owner_id: Some(record.grant.owner_id.clone()),
            profile: environment::Profile::Api,
            target: environment::Target::Terminal,
            callback_url: url,
            credential: callbacks::credential(&state, record.callback, record.grant.id)
                .await
                .map_err(|_| "The terminal credential cannot be refreshed.")?,
        };
        let contexts = runtime.contexts.clone();
        tokio::task::spawn_blocking(move || contexts.update_host(update, inputs))
            .await
            .map_err(|_| "Terminal preparation failed.")?
            .inspect_err(|_| {
                trace_lease_failure(record, "context_rotation_refused");
            })?;
        let identity =
            terminal::api_source(&state, record.callback, record.grant.id, record.team_id)
                .await
                .map_err(|_| {
                    trace_lease_failure(record, "post_context_source_refused");
                    "The terminal source changed while preparing."
                })?;
        let (_, snapshot) = self
            .host_terminal_inputs(runtime, &identity.caller())
            .await
            .inspect_err(|_| {
                trace_lease_failure(record, "post_context_snapshot_refused");
            })?;
        if snapshot != record.snapshot
            || access.principal.permissions != record.permissions
            || !runtime.current(record).await?
        {
            trace_lease_failure(record, "post_context_permissions_or_snapshot_changed");
            return Err("The terminal's permissions changed. Open it again.".into());
        }
        Ok(())
    }

    async fn end_host_terminal(&self, record: &Record) {
        if let Ok(state) = self.state() {
            let _ = callbacks::revoke(&state, record.callback).await;
        }
        if let Ok(runtime) = self.host_terminals()
            && runtime.retire(record).await.is_ok()
        {
            let id = record.grant.id;
            let _ = runtime
                .update_catalog(move |records| {
                    records.retain(|current| current.grant.id != id);
                    Ok(())
                })
                .await;
        }
    }

    pub async fn revoke_host_terminal(&self, team_id: Option<Uuid>, owner: &str, credential: &str) {
        let Ok(runtime) = self.host_terminals() else {
            return;
        };
        let (owner, credential) = (owner.to_string(), credential.to_string());
        let Ok(records) = runtime
            .update_catalog(move |records| {
                Ok(records
                    .iter()
                    .filter(|record| {
                        record.team_id == team_id
                            && record.grant.owner == owner
                            && record.credential == credential
                    })
                    .cloned()
                    .collect::<Vec<_>>())
            })
            .await
        else {
            return;
        };
        for record in records {
            if let Ok(_lock) = self.host_terminal_lock(record.grant.id).await {
                self.end_host_terminal(&record).await;
            }
        }
    }

    pub(super) async fn maintain_host_terminals(self: &Arc<Self>) {
        let Ok(runtime) = self.host_terminals() else {
            return;
        };
        let Ok(records) = runtime.update_catalog(|records| Ok(records.clone())).await else {
            return;
        };
        let mut tasks = tokio::task::JoinSet::new();
        for mut record in records {
            if tasks.len() >= 32 {
                let _ = tasks.join_next().await;
            }
            let (controller, runtime) = (self.clone(), runtime.clone());
            tasks.spawn(async move {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    let Ok(_lock) = controller.host_terminal_lock(record.grant.id).await else {
                        return;
                    };
                    if runtime.owner.as_ref() != Some(&record.owner) {
                        if !record.owner.alive() {
                            controller.end_host_terminal(&record).await;
                        }
                        return;
                    }
                    if let Err(error) = controller.renew_host_terminal(&runtime, &mut record).await
                        && error != TerminalStorageError::Busy
                    {
                        controller.end_host_terminal(&record).await;
                    }
                })
                .await;
            });
        }
        while tasks.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_core::access::{Principal as Rules, Role};

    #[cfg(target_os = "linux")]
    #[test]
    fn source_refused_before_open_retires_its_never_handed_off_pending_context() {
        const MARKER: &str = "MRSM_TEST_HOST_PREOPEN_GUARD";
        if std::env::var_os(MARKER).is_none() {
            let output=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","executor_client::host_terminals::tests::source_refused_before_open_retires_its_never_handed_off_pending_context","--nocapture","--test-threads=1"])
                .env(MARKER,"1").env_remove("TMUX").env_remove("TMUX_PANE").output().unwrap();
            assert!(
                output.status.success(),
                "The isolated native pre-open cleanup fixture failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        meerschaum_core::self_exe::register_native_current_executable().unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                use meerschaum_core::users::User;
                let root = tempfile::tempdir().unwrap();
                std::fs::create_dir(root.path().join("plugins")).unwrap();
                let instance = Arc::new(
                    meerschaum_connector_sql::SqlConnector::from_uri(
                        "main",
                        &format!("sqlite:///{}", root.path().join("preopen.db").display()),
                    )
                    .await
                    .unwrap(),
                );
                let mut user = User::new("preopen_operator", Some("synthetic-preopen-password"));
                user.attributes = Some(
                    Value::from_json_str(r#"{"role":"operator"}"#)
                        .unwrap()
                        .to_map(),
                );
                assert!(instance.register_user(&mut user).await.success);
                let mut server = meerschaum_server::ServerConfig::for_root("sql:main", root.path());
                server.actions_non_admin = true;
                server.connectors = Value::from_json_str(r#"{"sql":{"main":{"flavor":"sqlite"}}}"#)
                    .unwrap()
                    .to_map();
                let state = meerschaum_server::AppState::builder(server, instance.clone())
                    .build()
                    .unwrap();
                let principal = auth::load_from_subject(&state, &user.username)
                    .await
                    .unwrap()
                    .unwrap();
                let mut caller = meerschaum_server::access::access_of_fresh(
                    &state,
                    principal,
                    &Scopes::List(vec![
                        "actions:execute".into(),
                        "jobs:execute".into(),
                        "webterm:use".into(),
                    ]),
                )
                .await
                .unwrap();
                caller.credential = auth::user_credential(&state, &user).await.unwrap();
                let options = meerschaum_core::config::ConfigOptions::with_env(vec![(
                    "MRSM_ROOT_DIR".into(),
                    root.path().display().to_string(),
                )]);
                let controller =
                    Controller::new(root.path().into(), instance.clone(), options, Map::new());
                controller.bind_state(&state).unwrap();
                controller.bind_host_terminals().unwrap();
                let runtime = controller.host_terminals().unwrap();
                let callback = callbacks::register_with_sources(
                    &state,
                    &caller,
                    Uuid::new_v4(),
                    None,
                    &["webterm:use", "actions:execute", "jobs:execute"],
                    callbacks::Purpose::Command,
                    Default::default(),
                )
                .await
                .unwrap();
                let record = Record {
                    team_id: None,
                    grant: TerminalGrant {
                        id: callback.launch_id,
                        owner: user.username.clone(),
                        owner_id: instance.get_user_id(&user).await.unwrap().unwrap().py_str(),
                    },
                    credential: caller.credential.clone().unwrap(),
                    permissions: caller.principal.permissions.clone(),
                    callback: callback.id,
                    snapshot: "0".repeat(64),
                    owner: runtime.owner.clone().unwrap(),
                    version: crate::VERSION.into(),
                    source_ceiling: Some(Default::default()),
                };
                runtime
                    .catalog
                    .update(|records| {
                        records.push(record.clone());
                        Ok(())
                    })
                    .unwrap();
                let environment = vec![
                    ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
                    ("MRSM_EXECUTOR_PLUGINS".into(), "[]".into()),
                    (
                        "MRSM_PYTHON".into(),
                        std::env::var("MRSM_PYTHON").unwrap_or_else(|_| "/usr/bin/python3".into()),
                    ),
                    (
                        "PYTHONPATH".into(),
                        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                            .join("../..")
                            .canonicalize()
                            .unwrap()
                            .display()
                            .to_string(),
                    ),
                ];
                runtime
                    .contexts
                    .update_host(
                        environment::Update {
                            metadata_deadline: None,
                            extension: None,
                            discovery_catalog_sha256: None,
                            registration: None,
                            id: record.grant.id,
                            revision: 1,
                            owner: record.grant.owner.clone(),
                            owner_id: Some(record.grant.owner_id.clone()),
                            profile: environment::Profile::Api,
                            target: environment::Target::Terminal,
                            callback_url: "http://127.0.0.1:1".into(),
                            credential: callbacks::credential(
                                &state,
                                record.callback,
                                record.grant.id,
                            )
                            .await
                            .unwrap(),
                        },
                        HostInputs {
                            base: crate::executor_host_jobs::filtered_base(&state, &caller)
                                .unwrap(),
                            environment,
                            sources: Vec::new(),
                        },
                    )
                    .unwrap();
                let shell = runtime
                    .contexts
                    .terminal_shell(
                        record.grant.id,
                        &record.grant,
                        &ShellCommand::new("/unused-ambient-shell"),
                    )
                    .unwrap();
                let pending = PathBuf::from(
                    shell
                        .env
                        .iter()
                        .find(|(key, _)| key == "MRSM_TERMINAL_LAUNCH")
                        .unwrap()
                        .1
                        .clone(),
                );
                let before: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&pending).unwrap()).unwrap();
                assert!(before["guardian"].is_null() && before["child"].is_null());
                callbacks::revoke(&state, record.callback).await.unwrap();
                assert!(
                    controller
                        .verify_prepared_host_terminal(&runtime, &record, &shell)
                        .await
                        .is_err()
                );
                assert!(!pending.exists());
                assert!(
                    !root
                        .path()
                        .join("execution-contexts")
                        .join(record.grant.id.simple().to_string())
                        .exists()
                );
                assert!(!runtime.current(&record).await.unwrap());
                assert!(
                    callbacks::load_identity(&state, record.callback, record.grant.id)
                        .await
                        .is_err()
                );
            });
    }
    use meerschaum_core::{Map, Value};

    #[test]
    fn initialized_host_terminal_catalog_never_turns_loss_or_corruption_into_empty_authority() {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog(root.path().join("catalog"));
        assert!(catalog.update(|records| Ok(records.is_empty())).unwrap());
        std::fs::remove_file(catalog.0.join("catalog.json")).unwrap();
        assert!(catalog.update(|records| Ok(records.len())).is_err());
        std::fs::write(catalog.0.join("catalog.json"), "{broken").unwrap();
        assert!(catalog.update(|records| Ok(records.len())).is_err());
    }

    #[tokio::test]
    async fn busy_catalog_refuses_without_waiting_on_the_serving_runtime() {
        let root = tempfile::tempdir().unwrap();
        let runtime = HostTerminals::new(root.path());
        runtime.update_catalog(|_| Ok(())).await.unwrap();
        let path = runtime.catalog.0.join("catalog.json");
        let before = std::fs::read(&path).unwrap();
        let lock = std::fs::OpenOptions::new()
            .append(true)
            .open(runtime.catalog.0.join("lock"))
            .unwrap();
        fs4::FileExt::lock(&lock).unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            runtime.update_catalog(|_| -> Result<(), String> {
                panic!("Busy storage must not mutate authority.")
            }),
        )
        .await
        .unwrap();
        assert_eq!(result, Err(TerminalStorageError::Busy));
        assert_eq!(std::fs::read(path).unwrap(), before);
        drop(lock);
        runtime.update_catalog(|_| Ok(())).await.unwrap();
    }

    #[tokio::test]
    async fn source_snapshot_uses_only_importable_granted_sources_and_changes_before_renewal() {
        let root = tempfile::tempdir().unwrap();
        let plugins = root.path().join("plugins");
        std::fs::create_dir_all(plugins.join("private_artifacts")).unwrap();
        std::fs::create_dir_all(plugins.join("private.py")).unwrap();
        std::fs::write(
            plugins.join("private.py/credential"),
            "synthetic-misleading-directory",
        )
        .unwrap();
        std::fs::create_dir_all(plugins.join("plain_package")).unwrap();
        std::fs::write(
            plugins.join("plain_package/__init__.py"),
            "raise RuntimeError('do not import package')",
        )
        .unwrap();
        std::fs::write(
            plugins.join("allowed.py"),
            "raise RuntimeError('do not import')",
        )
        .unwrap();
        std::fs::write(plugins.join(".env"), "synthetic-neighbor").unwrap();
        std::fs::write(
            plugins.join("private_artifacts/credential"),
            "synthetic-unrelated",
        )
        .unwrap();
        let instance = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", root.path().join("snapshot.db").display()),
            )
            .await
            .unwrap(),
        );
        let mut server = meerschaum_server::ServerConfig::for_root("sql:main", root.path());
        server.connectors =
            Value::from_json_str(r#"{"sql":{"main":{"password":"synthetic-main-private"}}}"#)
                .unwrap()
                .to_map();
        let state = meerschaum_server::AppState::builder(server, instance)
            .secret(b"synthetic-snapshot-secret-01234567890123456789".to_vec())
            .build()
            .unwrap();
        let mut options = meerschaum_core::config::ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
            ("MRSM_PLUGINS_DIR".into(), plugins.display().to_string()),
        ]);
        options.allow_writes = false;
        options.home = root.path().into();
        let config = meerschaum_core::Config::new(options).unwrap();
        let mut caller = Access::everything();
        caller.no_auth = false;
        caller.principal = Rules::new("alice", false, Role::Operator.permissions(), Vec::new());
        let runtime = HostTerminals::new(root.path());
        let (inputs, first) = runtime.inputs(&state, &caller, &config, &[]).unwrap();
        assert_eq!(
            inputs
                .sources
                .iter()
                .map(|(name, _)| name.to_string_lossy().into_owned())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["allowed.py".into(), "plain_package".into()])
        );
        let serialized = Value::Map(inputs.base).to_json_string();
        assert!(!serialized.contains("synthetic-main-private"));
        assert!(!serialized.contains("password"));
        assert_eq!(
            first,
            runtime.inputs(&state, &caller, &config, &[]).unwrap().1
        );
        let source = plugins.join("allowed.py");
        let metadata = std::fs::metadata(&source).unwrap();
        let mut changed = std::fs::read(&source).unwrap();
        changed[0] = b'R';
        std::fs::write(&source, changed).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&source)
            .unwrap()
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(metadata.modified().unwrap())
                    .set_accessed(metadata.accessed().unwrap()),
            )
            .unwrap();
        assert_eq!(std::fs::metadata(&source).unwrap().len(), metadata.len());
        assert_eq!(
            std::fs::metadata(&source).unwrap().modified().unwrap(),
            metadata.modified().unwrap()
        );
        assert_ne!(
            first,
            runtime.inputs(&state, &caller, &config, &[]).unwrap().1
        );
    }

    #[tokio::test]
    async fn old_console_credential_cannot_retire_a_replacement_users_terminal() {
        let root = tempfile::tempdir().unwrap();
        let instance = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", root.path().join("owner.db").display()),
            )
            .await
            .unwrap(),
        );
        let options = meerschaum_core::config::ConfigOptions::with_env(vec![(
            "MRSM_ROOT_DIR".into(),
            root.path().display().to_string(),
        )]);
        let controller = Controller::new(root.path().into(), instance, options, Map::new());
        controller.bind_host_terminals().unwrap();
        let runtime = controller.host_terminals().unwrap();
        let owner = runtime.owner.clone().unwrap();
        let record = |credential: &str, registered: &str| Record {
            team_id: None,
            grant: TerminalGrant {
                id: Uuid::new_v4(),
                owner: "alice".into(),
                owner_id: registered.into(),
            },
            credential: credential.into(),
            permissions: BTreeSet::new(),
            callback: Uuid::new_v4(),
            snapshot: "0".repeat(64),
            owner: owner.clone(),
            version: crate::VERSION.into(),
            source_ceiling: None,
        };
        let old = record("synthetic-old-binding", "old-registration");
        let new = record("synthetic-new-binding", "new-registration");
        runtime
            .catalog
            .update(|records| {
                records.extend([old.clone(), new.clone()]);
                Ok(())
            })
            .unwrap();
        controller
            .revoke_host_terminal(None, "alice", "synthetic-old-binding")
            .await;
        let remaining = runtime
            .catalog
            .update(|records| Ok(records.clone()))
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].grant, new.grant);
    }
}
