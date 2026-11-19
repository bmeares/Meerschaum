//! Main-created credential-free discovery. A user flag or catalog-shaped output is not authority.

use std::collections::BTreeSet;

use meerschaum_connector_api::models::ExecutionCredential;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{callbacks, environment, plugins};
use crate::AppState;
use crate::access::Access;
use crate::auth::Principal;
use crate::error::ApiError;

#[path = "plugin_host_discovery.rs"]
mod host;

/// Private canonical operation. Public job properties never deserialize this descriptor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Discovery {
    pub catalog: plugins::Catalog,
    pub origin: Origin,
    /// Immutable Main deadline. A canceled request cannot renew pure compute indefinitely.
    pub deadline: i64,
}

/// Captured only from the originating Main endpoint's typed admission.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Origin {
    Extension,
    Action,
    Job,
    Pipe {
        operation: Box<super::pipe_operation::GraphOperation>,
    },
    Register {
        connector_keys: String,
    },
}

impl Origin {
    pub fn capture() -> Self {
        if super::context::extension_discovery() {
            Self::Extension
        } else if let Some(connector_keys) = super::context::registration_source() {
            Self::Register { connector_keys }
        } else if let Some(operation) = super::context::pipe_operation() {
            Self::Pipe {
                operation: Box::new((*operation).clone()),
            }
        } else if super::context::action_request() {
            Self::Action
        } else {
            Self::Job
        }
    }
    fn required(&self) -> &'static str {
        match self {
            Self::Extension => "plugins:read",
            Self::Action => "actions:execute",
            Self::Job => "jobs:execute",
            Self::Pipe { .. } => "pipes:write",
            Self::Register { .. } => "pipes:register",
        }
    }
    fn requirements(&self) -> std::collections::BTreeSet<String> {
        match self {
            Self::Pipe { operation } => operation.required_scopes(),
            _ => std::collections::BTreeSet::from([self.required().to_string()]),
        }
    }
    async fn authorize(&self, state: &AppState, caller: &Access) -> Result<(), ApiError> {
        caller.require(
            &self
                .requirements()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            &[],
        )?;
        if let Self::Pipe { operation } = self {
            operation
                .authorize(state, caller, &operation.sysargs()?)
                .await?;
        }
        if let Self::Register { connector_keys } = self {
            if connector_keys.is_empty()
                || connector_keys.len() > 2048
                || connector_keys.chars().any(char::is_control)
            {
                return Err(refused());
            }
            crate::access::guard::check_source_connector(state, caller, connector_keys)?;
        }
        Ok(())
    }
}

fn refused() -> ApiError {
    ApiError::new(
        403,
        "The original source cannot authorize this exact metadata discovery.",
    )
}

impl Discovery {
    /// A built-in command selected by Main, never a replacement caller action.
    pub fn sysargs(&self) -> Vec<String> {
        ["show", "plugins", "--plugin-manifest"]
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    fn validated(&self) -> Result<plugins::Validated, ApiError> {
        if matches!(
            self.catalog.placement,
            super::plugin_host::Placement::Host(_)
        ) {
            return host::validated(self);
        }
        if self.deadline <= chrono::Utc::now().timestamp() {
            return Err(refused());
        }
        if self
            .catalog
            .plugins
            .values()
            .any(|plugin| plugin.metadata.is_some())
        {
            return Err(refused());
        }
        let grants = self.catalog.plugins.keys().cloned().collect();
        plugins::Validated::decode(
            &serde_json::to_vec(&self.catalog).map_err(|_| refused())?,
            plugins::Expected {
                team_id: self.catalog.team_id,
                generation: self.catalog.generation,
                grants: &grants,
                reserved_actions: &BTreeSet::new(),
            },
        )
        .map_err(|_| refused())
    }

    pub fn catalog_sha256(&self) -> Result<String, ApiError> {
        self.validated()?.digest().map_err(|_| refused())
    }

    /// Store original authority on Main only. No bearer from this proof enters discovery.
    /// An execution parent's immutable purpose remains unchanged.
    pub async fn register(
        &self,
        state: &AppState,
        caller: &Access,
        launch_id: Uuid,
    ) -> Result<callbacks::Grant, ApiError> {
        self.validated()?;
        let purpose = match &caller.auth {
            Some(Principal::Execution(parent)) => parent.grant.purpose,
            _ if matches!(
                self.origin,
                Origin::Pipe { .. } | Origin::Register { .. } | Origin::Extension
            ) =>
            {
                callbacks::Purpose::Command
            }
            _ => callbacks::Purpose::Jobs,
        };
        let ceiling =
            super::source_ceiling::Ceiling::from_catalog(&self.catalog).map_err(|_| refused())?;
        let required = self.origin.requirements();
        let required = required.iter().map(String::as_str).collect::<Vec<_>>();
        let grant = callbacks::register_with_sources(
            state,
            caller,
            launch_id,
            match &self.catalog.placement {
                super::plugin_host::Placement::Remote => Some(self.catalog.team_id),
                super::plugin_host::Placement::Host(host) => host.team_id,
            },
            &required,
            purpose,
            ceiling,
        )
        .await?;
        if let Err(error) = self
            .admit(
                state,
                grant.id,
                launch_id,
                self.catalog.team_id,
                &self.sysargs(),
            )
            .await
        {
            let _ = callbacks::revoke(state, grant.id).await;
            return Err(error);
        }
        Ok(grant)
    }

    /// Source proof may be suspended for explicit admission, but never authenticates a pull.
    /// The caller must reactivate it before separately issuing exact archive capabilities.
    pub async fn admit(
        &self,
        state: &AppState,
        grant_id: Uuid,
        launch_id: Uuid,
        team_id: Uuid,
        sysargs: &[String],
    ) -> Result<callbacks::Identity, ApiError> {
        if matches!(
            self.catalog.placement,
            super::plugin_host::Placement::Host(_)
        ) {
            return host::admit(self, state, grant_id, launch_id, team_id, sysargs).await;
        }
        self.validated()?;
        if sysargs != self.sysargs() || team_id != self.catalog.team_id {
            return Err(refused());
        }
        let identity = callbacks::load_admission_identity(state, grant_id, launch_id).await?;
        if identity.grant.source_ceiling.as_ref()
            != Some(
                &super::source_ceiling::Ceiling::from_catalog(&self.catalog)
                    .map_err(|_| refused())?,
            )
        {
            return Err(refused());
        }
        let team = identity.team.as_ref().ok_or_else(refused)?;
        let caller = identity.caller();
        if identity.grant.team_id != Some(team_id)
            || identity.grant.generation != Some(self.catalog.generation)
            || team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) != Some(team_id)
            || !identity
                .grant
                .selection
                .as_ref()
                .is_some_and(|settings| settings.mode.is_remote())
            || !self
                .origin
                .requirements()
                .is_subset(&identity.grant.required)
            || !identity
                .source_access()
                .principal
                .permits_team(Some(&team.name))
            || !meerschaum_core::access::same_team(
                caller.principal.team_boundary.as_deref(),
                Some(&team.name),
            )
        {
            return Err(refused());
        }
        self.origin.authorize(state, &caller).await?;
        let team_grants: BTreeSet<_> = team.granted_plugins().into_iter().collect();
        for name in self.catalog.plugins.keys() {
            if !team_grants.contains(name)
                || !meerschaum_core::access::can_use_plugin(&caller.principal, name)
            {
                return Err(refused());
            }
        }
        Ok(identity)
    }

    /// Lifecycle rotation contains no reusable authentication, even for a live same revision.
    pub fn credential(&self) -> ExecutionCredential {
        ExecutionCredential {
            token: environment::METADATA_TOKEN.into(),
            expires_at: (chrono::Utc::now().timestamp() + callbacks::TOKEN_SECONDS)
                .min(self.deadline),
        }
    }

    /// Only the stored Main-created Metadata operation may commit a shared result. The caller
    /// must also observe its canonical job completed and revalidate its original source.
    pub fn accept(
        &self,
        stored_profile: environment::Profile,
        signed_discovery_digest: &str,
        bytes: &[u8],
        reserved_actions: &BTreeSet<String>,
    ) -> Result<plugins::Validated, ApiError> {
        if matches!(
            self.catalog.placement,
            super::plugin_host::Placement::Host(_)
        ) {
            return host::accept(
                self,
                stored_profile,
                signed_discovery_digest,
                bytes,
                reserved_actions,
            );
        }
        if stored_profile != environment::Profile::Metadata
            || signed_discovery_digest != self.catalog_sha256()?
        {
            return Err(refused());
        }
        let grants = self.catalog.plugins.keys().cloned().collect();
        let output = plugins::Validated::decode(
            bytes,
            plugins::Expected {
                team_id: self.catalog.team_id,
                generation: self.catalog.generation,
                grants: &grants,
                reserved_actions,
            },
        )
        .map_err(|_| refused())?;
        if !output.complete()
            || self.catalog.plugins.iter().any(|(name, source)| {
                output.catalog().plugins.get(name).is_none_or(|discovered| {
                    discovered.archive_sha256 != source.archive_sha256
                        || discovered.kind != source.kind
                })
            })
        {
            return Err(refused());
        }
        // Normalize defaults under the same decoded bound before control/cache publication.
        let _ = output.encode().map_err(|_| refused())?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credentialed_or_different_source_results_cannot_enter_shared_metadata_cache() {
        let discovery = Discovery {
            origin: Origin::Action,
            deadline: chrono::Utc::now().timestamp() + 60,
            catalog: plugins::Catalog {
                placement: Default::default(),
                schema: plugins::SCHEMA,
                release: env!("CARGO_PKG_VERSION").into(),
                team_id: Uuid::new_v4(),
                generation: Uuid::new_v4(),
                plugins: std::collections::BTreeMap::from([(
                    "probe".into(),
                    plugins::Plugin {
                        archive_sha256: "a".repeat(64),
                        kind: plugins::SourceKind::PythonModule,
                        metadata: None,
                    },
                )]),
            },
        };
        let mut complete = discovery.catalog.clone();
        complete.plugins.get_mut("probe").unwrap().metadata = Some(plugins::Metadata::default());
        let bytes = serde_json::to_vec(&complete).unwrap();
        let digest = discovery.catalog_sha256().unwrap();
        for profile in [environment::Profile::Api, environment::Profile::Team] {
            assert!(
                discovery
                    .accept(profile, &digest, &bytes, &BTreeSet::new())
                    .is_err()
            );
        }
        assert!(
            discovery
                .accept(
                    environment::Profile::Metadata,
                    &digest,
                    &bytes,
                    &BTreeSet::new()
                )
                .unwrap()
                .complete()
        );
        complete.generation = Uuid::new_v4();
        assert!(
            discovery
                .accept(
                    environment::Profile::Metadata,
                    &digest,
                    &serde_json::to_vec(&complete).unwrap(),
                    &BTreeSet::new()
                )
                .is_err()
        );
        complete = discovery.catalog.clone();
        complete.plugins.get_mut("probe").unwrap().archive_sha256 = "b".repeat(64);
        complete.plugins.get_mut("probe").unwrap().metadata = Some(plugins::Metadata::default());
        assert!(
            discovery
                .accept(
                    environment::Profile::Metadata,
                    &digest,
                    &serde_json::to_vec(&complete).unwrap(),
                    &BTreeSet::new()
                )
                .is_err()
        );
        assert!(
            discovery
                .accept(
                    environment::Profile::Metadata,
                    &digest,
                    &serde_json::to_vec(&discovery.catalog).unwrap(),
                    &BTreeSet::new()
                )
                .is_err()
        );
    }
}
