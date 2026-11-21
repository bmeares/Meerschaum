//! `LaunchEnvironment` and terminal `SessionLookup` implementations for executor contexts.

use super::*;

impl std::fmt::Debug for Contexts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContexts").finish_non_exhaustive()
    }
}

impl meerschaum_jobs::daemon::LaunchEnvironment for Contexts {
    fn compute_deadline(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<Option<meerschaum_jobs::supervisor::ComputeDeadline>> {
        let Some(revision) = self.job_revision(name, properties) else {
            if self.authoritative(name, properties) {
                return Err(std::io::Error::other(
                    "The job has no current compute authority.",
                ));
            }
            return Ok(None);
        };
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|id| Uuid::parse_str(id).ok())
            .ok_or_else(|| std::io::Error::other("Invalid canonical compute identity."))?;
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let entry = inner
            .entries
            .get(&id)
            .filter(|entry| entry.revision == revision)
            .ok_or_else(|| {
                std::io::Error::other("The compute generation changed before launch.")
            })?;
        match (entry.identity.profile, entry.identity.metadata_deadline) {
            (Profile::Metadata, Some(expires_at)) => {
                Ok(Some(meerschaum_jobs::supervisor::ComputeDeadline {
                    expires_at,
                }))
            }
            (Profile::Metadata, _) | (_, Some(_)) => Err(std::io::Error::other(
                "The compute deadline does not match its private profile.",
            )),
            _ => Ok(None),
        }
    }
    fn result_protocol(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<meerschaum_jobs::supervisor::ResultProtocol> {
        use meerschaum_jobs::supervisor::ResultProtocol;
        if !self.authoritative(name, properties) {
            return Ok(ResultProtocol::Ordinary);
        }
        let revision = self.job_revision(name, properties).ok_or_else(|| {
            std::io::Error::other("The result protocol has no current signed context.")
        })?;
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|id| Uuid::parse_str(id).ok())
            .ok_or_else(|| std::io::Error::other("Invalid canonical result target."))?;
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let entry = inner
            .entries
            .get(&id)
            .filter(|entry| entry.revision == revision)
            .ok_or_else(|| std::io::Error::other("The result context changed before launch."))?;
        Ok(if entry.identity.profile == Profile::Metadata {
            ResultProtocol::Metadata
        } else if entry.identity.profile == Profile::Api
            && entry.identity.extension_sha256.is_some()
        {
            ResultProtocol::Extension
        } else if entry.identity.profile == Profile::Api
            && entry.identity.registration_sha256.is_some()
        {
            ResultProtocol::Registration
        } else {
            ResultProtocol::Ordinary
        })
    }
    fn retained_generation(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<Option<meerschaum_jobs::supervisor::JobGeneration>> {
        let revision = self.job_revision(name, properties).ok_or_else(|| {
            std::io::Error::other("The result has no current unexpired canonical source.")
        })?;
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(|| std::io::Error::other("Invalid canonical result identity."))?;
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let entry = inner
            .entries
            .get(&id)
            .filter(|entry| {
                entry.revision == revision
                    && entry.expires_at > chrono::Utc::now().timestamp()
                    && entry.identity.target == (Target::Job { name: name.into() })
            })
            .ok_or_else(|| std::io::Error::other("The retained result source is unavailable."))?;
        let path = self
            .root
            .join(id.simple().to_string())
            .join("identity.json");
        let proof = ContextProof::read(&path).map_err(std::io::Error::other)?;
        if proof.id != id
            || proof.revision != revision
            || proof.identity != entry.identity
            || !proof.attempted
            || proof.cleared_for_rebuild
            || proof.generation != entry.generation
            || proof.compiler != entry.compiler
            || entry.expires_at <= chrono::Utc::now().timestamp()
        {
            return Err(std::io::Error::other(
                "The retained native result generation changed.",
            ));
        }
        if let Some(generation) = &entry.generation {
            generation.validate(name)?;
        }
        Ok(entry.generation.clone())
    }

    fn job_generation(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<Option<meerschaum_jobs::supervisor::JobGeneration>> {
        let revision = self
            .job_revision(name, properties)
            .ok_or_else(|| std::io::Error::other("The job has no current canonical generation."))?;
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(|| std::io::Error::other("Invalid canonical job."))?;
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let generation = inner
            .entries
            .get(&id)
            .filter(|entry| entry.revision == revision)
            .map(|entry| entry.generation.clone())
            .ok_or_else(|| std::io::Error::other("The job generation changed before launch."))?;
        if let Some(generation) = &generation {
            let path = self
                .root
                .join(id.simple().to_string())
                .join("identity.json");
            let mut proof = ContextProof::read(&path).map_err(std::io::Error::other)?;
            if proof.revision != revision || proof.generation.as_ref() != Some(generation) {
                return Err(std::io::Error::other(
                    "The sealed job lifecycle binding changed.",
                ));
            }
            proof.attempted = true;
            write_private(
                &path,
                &serde_json::to_string(&proof).map_err(std::io::Error::other)?,
            )?;
            generation.pending()?;
        }
        Ok(generation)
    }
    fn authoritative(&self, name: &str, properties: &meerschaum_core::Map) -> bool {
        if properties.contains_key(meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY) {
            return true;
        }
        name.strip_prefix("mrsm-")
            .and_then(|value| Uuid::parse_str(value).ok())
            .is_some_and(|id| {
                self.inner
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .entries
                    .contains_key(&id)
                    || self.factory() && self.recorded(id)
            })
    }

    fn tracks_unattributed_publication(&self) -> bool {
        self.factory()
    }

    fn unattributed_team_environment(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<Option<Vec<(String, String)>>> {
        if !self.factory() || self.authoritative(name, properties) {
            return Ok(None);
        }
        self.unattributed_team_job(name)
            .map(Some)
            .map_err(std::io::Error::other)
    }

    fn owner_guard(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<Option<meerschaum_jobs::supervisor::LifecycleOwner>> {
        if properties.contains_key(meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY) {
            self.job_revision(name, properties).ok_or_else(|| {
                std::io::Error::other("The job's current signed owner context is unavailable.")
            })?;
        } else if !self.factory() {
            return Ok(None);
        } else if self.authoritative(name, properties) {
            return Err(std::io::Error::other(
                "The signed Factory job lost its launch identity.",
            ));
        }
        // This host process owns the context. Neither persisted job properties nor its old
        // environment can nominate another process to keep an orphaned worker alive.
        meerschaum_jobs::supervisor::LifecycleOwner::current().map(Some)
    }

    fn environment(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
        base: &[(String, String)],
    ) -> std::io::Result<Vec<(String, String)>> {
        use meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY;
        let marked = properties.get(JOB_CONTEXT_PROPERTY);
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|s| Uuid::parse_str(s).ok());
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = id.and_then(|id| inner.entries.get(&id).map(|e| (id, e)));
        if marked.is_none() && entry.is_none() {
            if self.factory() && id.is_some_and(|id| self.recorded(id)) {
                return Err(std::io::Error::other(
                    "The signed Factory job lost its launch identity.",
                ));
            }
            return Ok(base.to_vec());
        }
        let (id, entry) = entry.ok_or_else(|| {
            std::io::Error::other("The job's signed launch context is unavailable.")
        })?;
        if marked
            .and_then(meerschaum_core::Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
            != Some(id)
            || entry.identity.target != (Target::Job { name: name.into() })
            || entry.expires_at <= chrono::Utc::now().timestamp()
        {
            return Err(std::io::Error::other(
                "The job's signed launch context is invalid or stale.",
            ));
        }
        entry.environment.clone().ok_or_else(|| {
            std::io::Error::other("The signed launch has no exact prepared runtime.")
        })
    }
}

/// Both the lease and its rotating context must be current before creating a shell/window.
pub(crate) struct Sessions {
    pub terminals: Arc<TerminalSessions>,
    pub contexts: Arc<Contexts>,
}

#[async_trait::async_trait]
impl SessionLookup for Sessions {
    async fn username(&self, id: &str) -> Result<Option<String>, String> {
        self.terminals.username(id).await
    }
    async fn restricted(&self, id: &str) -> Result<bool, String> {
        let caller = meerschaum_server::executor::gateway::current()
            .ok_or("An executor terminal requires signed authority.")?;
        let id = Uuid::parse_str(id).map_err(|_| "Invalid terminal identity.")?;
        let grant = self
            .terminals
            .grant(id, &caller.claims.actor)
            .ok_or("This terminal lease is unavailable.")?;
        self.contexts.terminal_restricted(id, &grant)
    }
    async fn shell(&self, id: &str, configured: &ShellCommand) -> Result<ShellCommand, String> {
        let caller = meerschaum_server::executor::gateway::current()
            .ok_or("An executor terminal requires signed authority.")?;
        let id = Uuid::parse_str(id).map_err(|_| "Invalid terminal identity.")?;
        let grant = self
            .terminals
            .grant(id, &caller.claims.actor)
            .ok_or("This terminal lease is unavailable.")?;
        let contexts = self.contexts.clone();
        let configured = configured.clone();
        tokio::task::spawn_blocking(move || contexts.shell(id, &grant, &configured))
            .await
            .map_err(|_| "The terminal context was interrupted.".to_string())?
    }
}
