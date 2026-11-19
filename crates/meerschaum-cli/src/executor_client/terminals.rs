//! Main-owned terminal identities and detached owner-lease renewal.

use std::collections::BTreeSet;
use std::sync::Arc;

use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::{Body, Method, RequestOptions, SocketMessage};
use meerschaum_core::access::Team;
use meerschaum_core::team_scope::{create_private_dir, write_private};
use meerschaum_server::access::Access;
use meerschaum_server::auth::{self, Principal, Scopes};
use meerschaum_server::executor::gateway::{CONTEXTS_PATH, TERMINALS_PATH};
use meerschaum_server::executor::terminal::TerminalGrant;
use meerschaum_server::executor::{callbacks, environment, registration::Sources};
use meerschaum_server::webterm::remote::{Decoder, Event, QUEUE, Terminal};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{Controller, TerminalStorageError};

const MAX_RECORDS: usize = 512;
const MAX_BYTES: usize = 4 * 1024 * 1024;

#[cfg(test)]
#[path = "terminal_source_tests.rs"]
mod source_tests;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    team_id: Uuid,
    grant: TerminalGrant,
    #[serde(default)]
    profile: environment::Profile,
    credential: String,
    permissions: BTreeSet<String>,
    #[serde(default)]
    callback: Option<Uuid>,
    #[serde(default)]
    source_selection: Option<Sources>,
    #[serde(default)]
    sources_captured: bool,
}

struct Catalog(std::path::PathBuf, Arc<tokio::sync::Mutex<()>>);

impl Catalog {
    fn update<T>(
        &self,
        update: impl FnOnce(&mut Vec<Record>) -> Result<T, String>,
    ) -> Result<T, TerminalStorageError> {
        create_private_dir(&self.0).map_err(|_| "Could not prepare terminal identity storage.")?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.0.join("terminals.lock"))
            .map_err(|_| "Could not open terminal identity lock.")?;
        fs4::FileExt::try_lock(&lock).map_err(|error| match error {
            fs4::TryLockError::WouldBlock => TerminalStorageError::Busy,
            fs4::TryLockError::Error(_) => {
                TerminalStorageError::from("Terminal identities are unavailable.")
            }
        })?;
        let path = self.0.join("terminals.json");
        let mut records: Vec<Record> = match std::fs::File::open(&path) {
            Ok(file) => {
                use std::io::Read;
                let mut bytes = Vec::new();
                file.take(MAX_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|_| "Could not read terminal identities.")?;
                if bytes.len() > MAX_BYTES {
                    return Err("Terminal identity storage exceeds its limit.".into());
                }
                serde_json::from_slice(&bytes)
                    .map_err(|_| "Invalid main terminal identity storage.")?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(_) => return Err("Could not read terminal identities.".into()),
        };
        if records.len() > MAX_RECORDS {
            return Err("Terminal identity capacity exceeded.".into());
        }
        let result = update(&mut records)?;
        let json =
            serde_json::to_string(&records).map_err(|_| "Could not encode terminal identities.")?;
        if json.len() > MAX_BYTES {
            return Err("Terminal identity storage exceeds its limit.".into());
        }
        write_private(&path, &json).map_err(|_| "Could not persist terminal identities.")?;
        Ok(result)
    }

    async fn update_async<T: Send + 'static>(
        self,
        update: impl FnOnce(&mut Vec<Record>) -> Result<T, String> + Send + 'static,
    ) -> Result<T, TerminalStorageError> {
        let gate = self.1.clone();
        let _guard = gate.lock().await;
        tokio::task::spawn_blocking(move || self.update(update))
            .await
            .map_err(|_| TerminalStorageError::from("The terminal identity task failed."))?
    }
}

impl Controller {
    fn terminals(&self) -> Catalog {
        Catalog(
            self.root.join(crate::executor_registry::DIRECTORY),
            self.terminal_catalog_gate.clone(),
        )
    }

    async fn terminal_lock(&self, id: Uuid) -> Result<std::fs::File, String> {
        let directory = self
            .root
            .join(crate::executor_registry::DIRECTORY)
            .join("terminal-leases");
        meerschaum_server::executor::storage::lock(directory, id)
            .await
            .map_err(|_| "The terminal lease is busy or unavailable.".into())
    }

    async fn terminal_current(&self, record: &Record) -> Result<bool, TerminalStorageError> {
        Ok(self.terminal_record(record).await?.is_some())
    }

    async fn terminal_record(
        &self,
        record: &Record,
    ) -> Result<Option<Record>, TerminalStorageError> {
        let record = record.clone();
        self.terminals()
            .update_async(move |records| {
                Ok(records
                    .iter()
                    .find(|r| {
                        r.grant == record.grant
                            && r.team_id == record.team_id
                            && r.credential == record.credential
                            && r.profile == record.profile
                            && r.source_selection == record.source_selection
                            && r.sources_captured == record.sources_captured
                    })
                    .cloned())
            })
            .await
    }

    /// All terminal permissions and team grants are resolved from main's live authoritative store.
    async fn terminal_owner(
        &self,
        owner: &str,
        permissions: &BTreeSet<String>,
        team_id: Uuid,
        ceiling: Option<environment::Profile>,
    ) -> Result<(Access, String, String, Team, environment::Profile), String> {
        let state = self.state()?;
        let user = meerschaum_core::users::User::new(owner, None);
        let id = state
            .instance()
            .get_user_id(&user)
            .await
            .map_err(|_| "Could not verify the terminal's registered owner.")?
            .filter(|id| !id.is_null())
            .ok_or("The terminal's owner no longer exists.")?
            .py_str();
        let credential = auth::user_credential(&state, &user)
            .await
            .map_err(|_| "Could not verify the terminal's current credential.")?
            .ok_or("The terminal's owner has no current credential.")?;
        let mut access = meerschaum_server::access::access_of_fresh(
            &state,
            Principal::User(user),
            &Scopes::List(permissions.iter().cloned().collect()),
        )
        .await
        .map_err(|_| "Could not verify the terminal's current permissions.")?;
        access.credential = Some(credential.clone());
        if !state.config.actions_non_admin
            || !["webterm:use", "actions:execute", "jobs:execute"]
                .iter()
                .all(|p| access.has(p))
        {
            return Err("The owner no longer has terminal permissions.".into());
        }
        let team = access
            .team_records
            .iter()
            .find(|team| {
                team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) == Some(team_id)
                    && meerschaum_core::access::same_team(
                        Some(&team.name),
                        Some(&access.primary_team),
                    )
                    && access.in_team(Some(&team.name))
                    && access.principal.permits_team(Some(&team.name))
            })
            .cloned()
            .ok_or("The owner no longer acts for this registered team.")?;
        let code = access.full_admin()
            || meerschaum_server::team_execution::code_running_team(&access)
                .is_some_and(|current| current.id == team.id);
        let profile = match ceiling {
            Some(environment::Profile::Metadata) => {
                return Err("Metadata discovery cannot become a terminal.".into());
            }
            Some(environment::Profile::Api) => environment::Profile::Api,
            Some(environment::Profile::Team) if !code => {
                return Err(
                    "This Team terminal lost its original code authority; open a new terminal."
                        .into(),
                );
            }
            _ if code => environment::Profile::Team,
            _ => environment::Profile::Api,
        };
        if profile == environment::Profile::Api
            && !meerschaum_server::executor::terminal::API_TERMINALS_AVAILABLE
        {
            return Err(
                "API-only terminals are awaiting native runtime acceptance on this release.".into(),
            );
        }
        if !team.execution().is_ok_and(|s| s.mode.is_remote()) {
            return Err("This team no longer selects a separate executor.".into());
        }
        Ok((access, id, credential, team, profile))
    }

    pub async fn terminal(
        self: &Arc<Self>,
        team: &Team,
        caller: &Access,
    ) -> Result<Terminal, String> {
        if !matches!(caller.auth, Some(Principal::User(_))) {
            return Err("A terminal needs its original console user credential.".into());
        }
        let team_id = team
            .id
            .as_deref()
            .and_then(|id| Uuid::parse_str(id).ok())
            .ok_or("A terminal needs a registered team.")?;
        let owner = caller
            .username()
            .ok_or("A terminal needs a registered user.")?;
        let (access, owner_id, credential, current, profile) = self
            .terminal_owner(owner, &caller.principal.permissions, team_id, None)
            .await
            .map_err(|error| error.to_string())?;
        let prepared_permissions = access.principal.permissions.clone();
        if caller.credential.as_ref() != Some(&credential) {
            return Err("The terminal request's owner credential is no longer current.".into());
        }
        // Prepare can take time. Resolve the owner again afterward before signing a new lease.
        let connector = meerschaum_server::executor::context::scope(
            Some(Arc::new(access)),
            self.prepare(&current),
        )
        .await?;
        let (access, owner_id_now, credential_now, _, _) = self
            .terminal_owner(owner, &caller.principal.permissions, team_id, Some(profile))
            .await?;
        if owner_id != owner_id_now
            || credential != credential_now
            || prepared_permissions != access.principal.permissions
        {
            return Err("The terminal's owner credential changed.".into());
        }
        let (record_owner, record_permissions) =
            (owner.to_string(), access.principal.permissions.clone());
        let mut record = self
            .terminals()
            .update_async(move |records| {
                if let Some(record) = records.iter().find(|r| {
                    r.team_id == team_id
                        && r.profile == profile
                        && r.grant.owner == record_owner
                        && r.grant.owner_id == owner_id
                        && r.credential == credential
                }) {
                    return Ok(record.clone());
                }
                if records.len() >= MAX_RECORDS {
                    return Err("Main's terminal capacity is exhausted.".into());
                }
                let record = Record {
                    team_id,
                    profile,
                    grant: TerminalGrant {
                        id: Uuid::new_v4(),
                        owner: record_owner,
                        owner_id,
                    },
                    credential,
                    permissions: record_permissions,
                    callback: None,
                    source_selection: None,
                    sources_captured: false,
                };
                records.push(record.clone());
                Ok(record)
            })
            .await
            .map_err(|error| error.to_string())?;
        let _lease_lock = self.terminal_lock(record.grant.id).await?;
        record = self
            .terminal_record(&record)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("This terminal lease was revoked while connecting.")?;
        // A freshly prepared client's permissions are an upper bound from the same request.
        put_lease(&connector, &record.grant).await?;
        self.put_terminal_context(&connector, &mut record, &access)
            .await?;
        let mut socket = connector
            .open_authenticated_socket(&format!("/websocket/{}", record.grant.id))
            .await
            .map_err(|_| "The executor terminal is unavailable.")?;
        let (input, mut commands) = mpsc::channel(QUEUE);
        let (events_tx, events) = mpsc::channel(QUEUE);
        tokio::spawn(async move {
            let mut decoder = Decoder::default();
            loop {
                tokio::select! {
                    () = events_tx.closed() => break,
                    input = commands.recv() => {
                        let Some(input): Option<meerschaum_server::webterm::remote::Input> = input else { break; };
                        let Ok(wire) = input.wire() else { break; };
                        if socket.send(SocketMessage::Text(wire)).await.is_err() { break; }
                    }
                    message = socket.next() => {
                        let Ok(Some(SocketMessage::Text(raw))) = message else { break; };
                        let Ok(event) = decoder.decode(&raw) else { break; };
                        let end = event == Event::Disconnected;
                        if !matches!(tokio::time::timeout(std::time::Duration::from_secs(5), events_tx.send(event)).await, Ok(Ok(()))) || end { break; }
                    }
                }
            }
            socket.close().await;
        });
        Ok(Terminal { input, events })
    }

    pub async fn revoke_terminal(&self, team_id: Uuid, owner: &str) {
        let owner = owner.to_string();
        let removed = self
            .terminals()
            .update_async(move |records| {
                let removed = records
                    .iter()
                    .filter(|r| r.team_id == team_id && r.grant.owner == owner)
                    .cloned()
                    .collect::<Vec<_>>();
                records.retain(|r| !(r.team_id == team_id && r.grant.owner == owner));
                Ok(removed)
            })
            .await;
        if let Ok(records) = removed {
            for record in records {
                self.end_terminal(&record).await;
            }
        }
    }

    async fn end_terminal(&self, record: &Record) {
        if let (Ok(state), Some(id)) = (self.state(), record.callback) {
            let _ = callbacks::revoke(&state, id).await;
        }
        // Maintenance credentials exist only on main. Current generations can revoke an old
        // owner; a retired/unreachable generation instead expires its own 90-second leases.
        if let Ok(connector) = self.prepare_id(record.team_id).await
            && let Ok(_lease_lock) = self.terminal_lock(record.grant.id).await
        {
            let _ = connector
                .make_request(
                    Method::Delete,
                    &format!("{CONTEXTS_PATH}{}", record.grant.id),
                    RequestOptions {
                        body: Body::Json("{\"revision\":1}".into()),
                        ..Default::default()
                    },
                )
                .await;
            let _ = connector
                .make_request(
                    Method::Delete,
                    &format!("{TERMINALS_PATH}{}", record.grant.id),
                    RequestOptions::default(),
                )
                .await;
        }
    }

    pub(super) async fn maintain_terminals(self: &Arc<Self>) {
        let Ok(records) = self
            .terminals()
            .update_async(|records| Ok(records.clone()))
            .await
        else {
            return;
        };
        let mut renewals = tokio::task::JoinSet::new();
        for record in records {
            if renewals.len() >= 64 {
                let _ = renewals.join_next().await;
            }
            let controller = self.clone();
            renewals.spawn(async move {
                // With at most 512 records, a full failed lap stays within the 90-second
                // remote lease. Never serialize healthy owners behind an unavailable team.
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    controller.maintain_terminal(record),
                )
                .await;
            });
        }
        while renewals.join_next().await.is_some() {}
    }

    async fn maintain_terminal(&self, mut record: Record) {
        let current = self
            .terminal_owner(
                &record.grant.owner,
                &record.permissions,
                record.team_id,
                Some(record.profile),
            )
            .await;
        match current {
            Ok((access, id, credential, team, _))
                if id == record.grant.owner_id && credential == record.credential =>
            {
                meerschaum_server::executor::context::scope(
                    Some(Arc::new(access.clone())),
                    async {
                        if let Ok(connector) = self.prepare(&team).await
                            && let Ok(_lease_lock) = self.terminal_lock(record.grant.id).await
                            && self.terminal_current(&record).await == Ok(true)
                        {
                            let Ok(Some(current)) = self.terminal_record(&record).await else {
                                return;
                            };
                            record = current;
                            if put_lease(&connector, &record.grant).await.is_ok() {
                                let _ = self
                                    .put_terminal_context(&connector, &mut record, &access)
                                    .await;
                            }
                        }
                    },
                )
                .await;
            }
            _ => {
                let id = record.grant.id;
                let removed = self
                    .terminals()
                    .update_async(move |records| {
                        let present = records.iter().any(|r| r.grant.id == id);
                        records.retain(|r| r.grant.id != id);
                        Ok(present)
                    })
                    .await;
                if removed == Ok(true) {
                    self.end_terminal(&record).await;
                }
            }
        }
    }

    async fn put_terminal_context(
        &self,
        connector: &APIConnector,
        record: &mut Record,
        access: &Access,
    ) -> Result<(), String> {
        let state = self.state()?;
        let callback_url = self.callback_url(record.team_id).await?;
        if record.callback.is_none() && !record.sources_captured {
            let sources = Box::pin(self.capture_sources(
                record.team_id,
                crate::executor_jobs::JobPlacement::Remote,
                access,
            ))
            .await?;
            let expected = record.clone();
            let saved = sources.clone();
            self.terminals()
                .update_async(move |records| {
                    let current = records
                        .iter_mut()
                        .find(|current| {
                            current.grant == expected.grant
                                && current.credential == expected.credential
                                && current.profile == expected.profile
                                && current.callback.is_none()
                                && !current.sources_captured
                        })
                        .ok_or("The terminal source was captured or retired by another request.")?;
                    current.source_selection = saved;
                    current.sources_captured = true;
                    Ok(())
                })
                .await
                .map_err(|error| error.to_string())?;
            record.source_selection = sources;
            record.sources_captured = true;
        }
        if record.callback.is_some() {
            if let Err(error) = self.terminal_source_identity(record).await {
                self.retire_terminal_at(connector, record).await;
                return Err(error);
            }
        } else {
            Box::pin(self.confirm_sources(
                record.team_id,
                crate::executor_jobs::JobPlacement::Remote,
                record.source_selection.as_ref(),
                access,
            ))
            .await?;
        }
        let result = if let Some(id) = record.callback {
            callbacks::renew(&state, id, record.grant.id).await
        } else {
            callbacks::register_with_sources(
                &state,
                access,
                record.grant.id,
                Some(record.team_id),
                &["webterm:use", "actions:execute", "jobs:execute"],
                if record.profile == environment::Profile::Api {
                    callbacks::Purpose::Command
                } else {
                    callbacks::Purpose::Jobs
                },
                super::source_selection::ceiling(record.source_selection.as_ref())?,
            )
            .await
        };
        let grant = match result {
            Ok(grant) => grant,
            Err(error) => {
                if error.status.is_client_error()
                    && let Some(id) = record.callback
                {
                    // An expired/retired launch must get a new terminal UUID, never regain its
                    // old identity by silently minting a replacement callback credential.
                    let launch_id = record.grant.id;
                    let _ = self
                        .terminals()
                        .update_async(move |records| {
                            records
                                .retain(|r| !(r.grant.id == launch_id && r.callback == Some(id)));
                            Ok(())
                        })
                        .await;
                    let _ = callbacks::revoke(&state, id).await;
                }
                return Err("The terminal's callback authority is unavailable or revoked.".into());
            }
        };
        if record.callback.is_none() {
            let (expected, callback_id) = (record.clone(), grant.id);
            let updated = self
                .terminals()
                .update_async(move |records| {
                    let current = records
                        .iter_mut()
                        .find(|r| {
                            r.grant == expected.grant
                                && r.team_id == expected.team_id
                                && r.credential == expected.credential
                                && r.source_selection == expected.source_selection
                                && r.sources_captured == expected.sources_captured
                        })
                        .ok_or("The terminal was revoked during callback preparation.")?;
                    current.callback = Some(callback_id);
                    Ok(())
                })
                .await;
            if let Err(error) = updated {
                let _ = callbacks::revoke(&state, grant.id).await;
                return Err(error.to_string());
            }
            record.callback = Some(grant.id);
        }
        let identity = match self.terminal_source_identity(record).await {
            Ok(identity) => identity,
            Err(error) => {
                self.retire_terminal_at(connector, record).await;
                return Err(error);
            }
        };
        if !self
            .terminal_current(record)
            .await
            .map_err(|error| error.to_string())?
        {
            let _ = callbacks::revoke(&state, grant.id).await;
            return Err("The terminal's canonical identity was revoked.".into());
        }
        let mut update = environment::Update {
            metadata_deadline: None,
            extension: None,
            discovery_catalog_sha256: if matches!(
                record.profile,
                environment::Profile::Api | environment::Profile::Team
            ) {
                match &record.source_selection {
                    Some(Sources::Archives { catalog }) => {
                        Some(meerschaum_server::executor::plugins::sha256(
                            &serde_json::to_vec(catalog)
                                .map_err(|_| "Invalid terminal source catalog.")?,
                        ))
                    }
                    _ => None,
                }
            } else {
                None
            },
            registration: None,
            id: record.grant.id,
            revision: 1,
            owner: record.grant.owner.clone(),
            owner_id: Some(record.grant.owner_id.clone()),
            profile: record.profile,
            target: environment::Target::Terminal,
            callback_url,
            credential: callbacks::credential(&state, grant.id, record.grant.id)
                .await
                .map_err(|_| "Could not issue the terminal callback credential.")?,
        };
        let mut response = connector
            .make_request(
                Method::Put,
                &format!("{CONTEXTS_PATH}{}", update.id),
                RequestOptions {
                    body: Body::Json(
                        serde_json::to_string(&update)
                            .map_err(|_| "Could not encode the terminal environment.")?,
                    ),
                    timeout: Some(std::time::Duration::from_secs(10)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| "The executor did not accept its terminal environment.")?;
        if response.status == 409
            && response
                .json()
                .ok()
                .and_then(|body| body["detail"]["code"].as_str().map(str::to_string))
                .as_deref()
                == Some(meerschaum_server::executor::gateway::SOURCE_CATALOG_MISSING)
        {
            Box::pin(self.stage_remote_sources(
                record.team_id,
                record.source_selection.as_ref(),
                &identity,
                connector,
            ))
            .await?
            .ok_or("This terminal has no exact source catalog to pull.")?;
            let _ = self.terminal_source_identity(record).await?;
            update.credential = callbacks::credential(&state, grant.id, record.grant.id)
                .await
                .map_err(|_| "The terminal's source changed during its source pull.")?;
            response = connector
                .make_request(
                    Method::Put,
                    &format!("{CONTEXTS_PATH}{}", update.id),
                    RequestOptions {
                        body: Body::Json(
                            serde_json::to_string(&update)
                                .map_err(|_| "Invalid terminal context.")?,
                        ),
                        timeout: Some(std::time::Duration::from_secs(10)),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| "The executor did not accept its exact terminal sources.")?;
        }
        if response.status != 200 {
            return Err("The executor refused its terminal environment.".into());
        }
        Ok(())
    }

    async fn terminal_source_identity(
        &self,
        record: &Record,
    ) -> Result<callbacks::Identity, String> {
        let state = self.state()?;
        let grant = record
            .callback
            .ok_or("The terminal has no captured original source.")?;
        let identity = if record.profile == environment::Profile::Api {
            meerschaum_server::executor::terminal::api_source(
                &state,
                grant,
                record.grant.id,
                Some(record.team_id),
            )
            .await
        } else {
            callbacks::load_identity(&state, grant, record.grant.id).await
        }
        .map_err(|_| "The terminal's original source is unavailable.".to_string())?;
        if identity.grant.team_id != Some(record.team_id)
            || (record.profile == environment::Profile::Team
                && identity.grant.purpose != callbacks::Purpose::Jobs)
        {
            return Err("The terminal has another original source ceiling.".into());
        }
        super::source_selection::agrees(record.source_selection.as_ref(), &identity)?;
        Box::pin(self.confirm_sources(
            record.team_id,
            crate::executor_jobs::JobPlacement::Remote,
            record.source_selection.as_ref(),
            &identity.caller(),
        ))
        .await?;
        Ok(identity)
    }

    /// Called with this terminal's Main lease held. Never reacquire that same file lock.
    async fn retire_terminal_at(&self, connector: &APIConnector, record: &Record) {
        if let (Ok(state), Some(id)) = (self.state(), record.callback) {
            let _ = callbacks::revoke(&state, id).await;
        }
        let context_stopped = connector
            .make_request(
                Method::Delete,
                &format!("{CONTEXTS_PATH}{}", record.grant.id),
                RequestOptions {
                    body: Body::Json("{\"revision\":1}".into()),
                    timeout: Some(std::time::Duration::from_secs(5)),
                    ..Default::default()
                },
            )
            .await
            .is_ok_and(|response| response.status == 200);
        let lease_stopped = connector
            .make_request(
                Method::Delete,
                &format!("{TERMINALS_PATH}{}", record.grant.id),
                RequestOptions {
                    timeout: Some(std::time::Duration::from_secs(5)),
                    ..Default::default()
                },
            )
            .await
            .is_ok_and(|response| response.status == 200);
        if context_stopped && lease_stopped {
            let expected = record.clone();
            let _ = self
                .terminals()
                .update_async(move |records| {
                    records.retain(|current| {
                        !(current.grant == expected.grant
                            && current.callback == expected.callback
                            && current.credential == expected.credential
                            && current.source_selection == expected.source_selection)
                    });
                    Ok(())
                })
                .await;
        }
    }
}

async fn put_lease(connector: &APIConnector, grant: &TerminalGrant) -> Result<(), String> {
    let response = connector
        .make_request(
            Method::Put,
            &format!("{TERMINALS_PATH}{}", grant.id),
            RequestOptions {
                body: Body::Json(
                    serde_json::to_string(grant)
                        .map_err(|_| "Could not encode the terminal lease.")?,
                ),
                timeout: Some(std::time::Duration::from_secs(10)),
                ..Default::default()
            },
        )
        .await
        .map_err(|_| "The executor did not accept the terminal lease.")?;
    if response.status != 200 {
        return Err("The executor refused the terminal lease.".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_record() -> Record {
        Record {
            profile: environment::Profile::Team,
            team_id: Uuid::new_v4(),
            grant: TerminalGrant {
                id: Uuid::new_v4(),
                owner: "alice".into(),
                owner_id: "registered-alice".into(),
            },
            credential: "synthetic-credential-fingerprint".into(),
            permissions: ["webterm:use".into()].into_iter().collect(),
            callback: None,
            source_selection: None,
            sources_captured: false,
        }
    }

    fn controller(root: &std::path::Path) -> Arc<Controller> {
        // A connector that is never called: these tests exercise cross-worker canonical storage.
        Arc::new(Controller::new(
            root.into(),
            Arc::new(
                APIConnector::new(
                    Some("unused"),
                    meerschaum_core::Map::from_iter([
                        ("host".into(), "127.0.0.1".into()),
                        ("port".into(), 1.into()),
                    ]),
                )
                .unwrap(),
            ),
            meerschaum_core::config::ConfigOptions::with_env(vec![(
                "MRSM_ROOT_DIR".into(),
                root.display().to_string(),
            )]),
            Default::default(),
        ))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn another_main_worker_cannot_renew_a_revoked_inflight_terminal() {
        let root = tempfile::tempdir().unwrap();
        let first = controller(root.path());
        let second = controller(root.path());
        let record = fixture_record();
        first
            .terminals()
            .update(|records| {
                records.push(record.clone());
                Ok(())
            })
            .unwrap();
        let lock = first.terminal_lock(record.grant.id).await.unwrap();
        let other = second.clone();
        let id = record.grant.id;
        let pending = tokio::spawn(async move { other.terminal_lock(id).await });
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert!(
            !pending.is_finished(),
            "Separate controllers did not share the lease lock."
        );
        // Main revocation removes the authority while another worker is waiting to renew.
        first
            .terminals()
            .update(|records| {
                records.clear();
                Ok(())
            })
            .unwrap();
        drop(lock);
        let _lock = pending.await.unwrap().unwrap();
        assert!(!second.terminal_current(&record).await.unwrap());
        let replacement = fixture_record();
        second
            .terminals()
            .update(|records| {
                records.push(replacement.clone());
                Ok(())
            })
            .unwrap();
        assert!(!first.terminal_current(&record).await.unwrap());
        assert!(first.terminal_current(&replacement).await.unwrap());
    }

    #[test]
    fn terminal_catalog_corruption_is_never_an_empty_identity_set() {
        let root = tempfile::tempdir().unwrap();
        let controller = controller(root.path());
        let path = controller.terminals().0.join("terminals.json");
        controller
            .terminals()
            .update(|records| {
                records.push(fixture_record());
                Ok(())
            })
            .unwrap();
        std::fs::write(&path, "broken").unwrap();
        assert!(
            controller
                .terminals()
                .update(|records| Ok(records.len()))
                .is_err()
        );
        assert_eq!(std::fs::read_to_string(path).unwrap(), "broken");
    }

    #[tokio::test]
    async fn busy_remote_catalog_refuses_without_waiting_on_the_serving_runtime() {
        let root = tempfile::tempdir().unwrap();
        let controller = controller(root.path());
        controller
            .terminals()
            .update_async(|_| Ok(()))
            .await
            .unwrap();
        let directory = controller.terminals().0;
        let before = std::fs::read(directory.join("terminals.json")).unwrap();
        let lock = std::fs::OpenOptions::new()
            .append(true)
            .open(directory.join("terminals.lock"))
            .unwrap();
        fs4::FileExt::lock(&lock).unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            controller
                .terminals()
                .update_async(|_| -> Result<(), String> {
                    panic!("Busy storage must not mutate authority.")
                }),
        )
        .await
        .unwrap();
        assert_eq!(result, Err(TerminalStorageError::Busy));
        assert_eq!(
            std::fs::read(directory.join("terminals.json")).unwrap(),
            before
        );
        drop(lock);
        controller
            .terminals()
            .update_async(|_| Ok(()))
            .await
            .unwrap();
    }
}
