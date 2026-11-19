//! Main-created connector registration hooks use an exact private API operation.

use super::{callbacks, plugins};
use crate::{
    AppState,
    access::{Access, guard},
    error::ApiError,
};
use meerschaum_core::{Map, Pipe, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Only this validated descriptor may be copied into an API worker's sealed configuration.
pub const CONFIG_KEY: &str = "_executor_connector_registration";
/// Fixed built-in argument, interpreted only with the sealed descriptor and mandatory profile.
pub const ACTION: &str = "registration-result";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Sources {
    /// Host-selected import files, with paths relative to their granted plugin entry point.
    Files { hashes: BTreeMap<String, String> },
    /// Exact approved archive objects already verified by the executor's immutable store.
    Archives { catalog: plugins::Catalog },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    /// A Main-created hook cannot keep renewing after its bounded caller operation ends.
    pub deadline: i64,
    pub pipe_ref: String,
    pub parameters: Map,
    pub sources: Sources,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    Undefined,
    Parameters { parameters: Map },
    Invalid { representation: String },
}

impl Registration {
    pub fn sysargs(&self) -> Vec<String> {
        vec!["show".into(), "plugins".into(), ACTION.into()]
    }

    pub fn validate(&self) -> Result<(), ApiError> {
        let parts: Vec<_> = self.pipe_ref.split('|').collect();
        if self.deadline <= 0
            || parts.len() != 4
            || parts.iter().any(|part| {
                part.is_empty() || part.len() > 256 || part.chars().any(char::is_control)
            })
            || parts[1] == "mrsm"
            || self.parameters.len() > 256
            || Value::Map(self.parameters.clone()).to_json_string().len() > 64 * 1024
        {
            return Err(ApiError::new(
                403,
                "Invalid canonical connector registration.",
            ));
        }
        match &self.sources {
            Sources::Files { hashes } => {
                if hashes.is_empty()
                    || hashes.len() > 4096
                    || hashes.iter().any(|(path, digest)| {
                        path.is_empty()
                            || path.len() > 1024
                            || path.starts_with('/')
                            || path.contains('\\')
                            || path.split('/').any(|part| matches!(part, "" | "." | ".."))
                            || !plugins::valid_digest(digest)
                    })
                {
                    return Err(ApiError::new(403, "Invalid registration source snapshot."));
                }
            }
            Sources::Archives { catalog } => {
                if catalog.plugins.is_empty()
                    || catalog
                        .plugins
                        .values()
                        .any(|plugin| plugin.metadata.is_some())
                {
                    return Err(ApiError::new(
                        403,
                        "Registration requires its exact source-only archive catalog.",
                    ));
                }
                let grants = catalog.plugins.keys().cloned().collect();
                plugins::Validated::decode(
                    &serde_json::to_vec(catalog)
                        .map_err(|_| ApiError::new(403, "Invalid source catalog."))?,
                    plugins::Expected {
                        team_id: catalog.team_id,
                        generation: catalog.generation,
                        grants: &grants,
                        reserved_actions: &Default::default(),
                    },
                )
                .map_err(|_| ApiError::new(403, "The registration source catalog is invalid."))?;
            }
        }
        if serde_json::to_vec(self)
            .map_err(|_| ApiError::new(403, "Invalid registration descriptor."))?
            .len()
            > 1024 * 1024
        {
            return Err(ApiError::new(
                403,
                "The registration descriptor exceeds its limit.",
            ));
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<String, ApiError> {
        self.validate()?;
        Ok(plugins::sha256(&serde_json::to_vec(self).map_err(
            |_| ApiError::new(403, "Invalid registration descriptor."),
        )?))
    }

    pub fn pipe(&self) -> Result<Pipe, ApiError> {
        self.validate()?;
        let parts: Vec<_> = self.pipe_ref.split('|').collect();
        Pipe::builder(parts[1], parts[2])
            .location((parts[3] != "None").then_some(parts[3]))
            .instance(parts[0])
            .parameters(self.parameters.clone())
            .build()
            .map_err(|_| ApiError::new(403, "Invalid registration pipe."))
    }

    pub async fn authorize(&self, state: &AppState, caller: &Access) -> Result<(), ApiError> {
        if self.deadline <= chrono::Utc::now().timestamp() {
            return Err(ApiError::new(
                403,
                "The connector registration operation expired.",
            ));
        }
        caller.require(&["pipes:register"], &[])?;
        let pipe = self.pipe()?;
        if !state.config.registration_pipes
            || !guard::may_use_connector(state, caller, pipe.instance_keys())
        {
            return Err(ApiError::new(
                403,
                "The registration's logical instance is unavailable.",
            ));
        }
        guard::check_source_connector(state, caller, pipe.connector_keys())?;
        let conn = state.get_api_connector(Some(pipe.instance_keys()))?;
        if !conn
            .get_pipe_attributes(&pipe)
            .await
            .map_err(|_| ApiError::unavailable("Could not refresh registration authority."))?
            .is_empty()
            || conn
                .get_pipe_id(&pipe)
                .await
                .map_err(|_| ApiError::unavailable("Could not refresh registration identity."))?
                .is_some()
        {
            return Err(ApiError::new(
                409,
                "The registration target is already registered.",
            ));
        }
        let stamped = guard::stamp_new_pipe(caller, pipe.raw_parameters())?;
        guard::check_parameters(state, caller, &conn, &pipe, None, &stamped).await?;
        if let Sources::Archives { catalog } = &self.sources {
            let directory = state.config.plugins_archives_path.clone();
            let sources = catalog.plugins.clone();
            tokio::task::spawn_blocking(move || {
                use sha2::{Digest, Sha256};
                use std::io::Read;
                let mut remaining = 128 * 1024 * 1024usize;
                for (name, source) in sources {
                    let path = directory.join(format!("{name}.tar.gz"));
                    #[cfg(unix)]
                    let file = std::fs::File::from(
                        rustix::fs::open(
                            &path,
                            rustix::fs::OFlags::RDONLY
                                | rustix::fs::OFlags::NOFOLLOW
                                | rustix::fs::OFlags::NONBLOCK
                                | rustix::fs::OFlags::CLOEXEC,
                            rustix::fs::Mode::empty(),
                        )
                        .map_err(|_| ())?,
                    );
                    #[cfg(not(unix))]
                    return Err(());
                    #[cfg(unix)]
                    {
                        let metadata = file.metadata().map_err(|_| ())?;
                        if !metadata.is_file()
                            || metadata.len() > 32 * 1024 * 1024
                            || metadata.len() > remaining as u64
                        {
                            return Err(());
                        }
                        let limit = remaining.min(32 * 1024 * 1024);
                        let mut reader = file.take(limit as u64 + 1);
                        let mut buffer = [0u8; 64 * 1024];
                        let mut bytes = 0usize;
                        let mut digest = Sha256::new();
                        loop {
                            let count = reader.read(&mut buffer).map_err(|_| ())?;
                            if count == 0 {
                                break;
                            }
                            bytes = bytes.saturating_add(count);
                            if bytes > limit {
                                return Err(());
                            }
                            digest.update(&buffer[..count]);
                        }
                        let actual = digest
                            .finalize()
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>();
                        if actual != source.archive_sha256 {
                            return Err(());
                        }
                        remaining -= bytes;
                    }
                }
                Ok::<_, ()>(())
            })
            .await
            .map_err(|_| {
                ApiError::unavailable("Registration source verification was interrupted.")
            })?
            .map_err(|_| {
                ApiError::new(
                    403,
                    "Registration source content changed or is unavailable.",
                )
            })?;
        }
        Ok(())
    }

    pub async fn admit(
        &self,
        state: &AppState,
        grant: Uuid,
        launch: Uuid,
        team: Option<Uuid>,
        words: &[String],
    ) -> Result<callbacks::Identity, ApiError> {
        if self.sysargs() != words {
            return Err(ApiError::new(403, "The registration command changed."));
        }
        let identity = super::admission::typed_api_identity(state, grant, launch, team).await?;
        self.authorize(state, &identity.caller()).await?;
        if let Sources::Archives { catalog } = &self.sources {
            if identity.grant.team_id != Some(catalog.team_id)
                || identity.grant.generation != Some(catalog.generation)
            {
                return Err(ApiError::new(
                    403,
                    "The registration source generation changed.",
                ));
            }
            let caller = identity.caller();
            let team_grants = identity
                .team
                .as_ref()
                .map(|team| team.granted_plugins())
                .unwrap_or_default();
            for name in catalog.plugins.keys() {
                if !team_grants.contains(name)
                    || !meerschaum_core::access::can_use_plugin(&caller.principal, name)
                {
                    return Err(ApiError::new(
                        403,
                        "A registration source grant was revoked.",
                    ));
                }
            }
        }
        Ok(identity)
    }
}
