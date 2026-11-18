//! Main-created exact extension calls. Public job properties never select this operation.

use super::{callbacks, environment, plugin_handlers, plugins, source_ceiling};
use crate::{AppState, access::Access, error::ApiError};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const CONFIG_KEY: &str = "_executor_extension_operation";
pub const ACTION: &str = "extension-result";
pub const MAX_DESCRIPTOR_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller: Option<super::plugin_caller::Caller>,
    pub deadline: i64,
    pub catalog: plugins::Catalog,
    pub invocation: plugin_handlers::Invocation,
}

fn refused() -> ApiError {
    ApiError::new(403, "The exact extension source or handler is unavailable.")
}

impl Operation {
    pub fn sysargs(&self) -> Vec<String> {
        vec!["show".into(), "plugins".into(), ACTION.into()]
    }

    pub fn validated(&self) -> Result<plugins::Validated, ApiError> {
        if let Some(caller) = &self.caller {
            caller.validate()?;
        }
        if self.deadline <= 0
            || serde_json::to_vec(self).map_err(|_| refused())?.len() > MAX_DESCRIPTOR_BYTES
        {
            return Err(refused());
        }
        let grants = self.catalog.plugins.keys().cloned().collect();
        let bytes = serde_json::to_vec(&self.catalog).map_err(|_| refused())?;
        let catalog = match &self.catalog.placement {
            super::plugin_host::Placement::Remote => plugins::Validated::decode(
                &bytes,
                plugins::Expected {
                    team_id: self.catalog.team_id,
                    generation: self.catalog.generation,
                    grants: &grants,
                    reserved_actions: &Default::default(),
                },
            ),
            super::plugin_host::Placement::Host(host) => {
                plugins::Validated::decode_host(&bytes, host, &grants, &Default::default())
            }
        }
        .map_err(|_| refused())?;
        if !catalog.complete() {
            return Err(refused());
        }
        let registry = catalog
            .catalog()
            .plugins
            .get(&self.invocation.plugin)
            .and_then(|source| source.metadata.as_ref())
            .and_then(|metadata| metadata.registry.as_ref())
            .ok_or_else(refused)?;
        let handler = registry
            .handlers()
            .find(|handler| handler.id == self.invocation.handler_id)
            .ok_or_else(refused)?;
        self.invocation
            .validate(
                &catalog.source_digest().map_err(|_| refused())?,
                registry,
                handler,
            )
            .map_err(|_| refused())?;
        if let plugin_handlers::Request::Mcp { arguments } = &self.invocation.request {
            let metadata = catalog.catalog().plugins[&self.invocation.plugin]
                .metadata
                .as_ref()
                .ok_or_else(refused)?;
            let object = arguments.as_object().ok_or_else(refused)?;
            if let plugin_handlers::Selector::McpResource { name } = &handler.selector {
                let entry = metadata
                    .mcp_resources
                    .iter()
                    .find(|entry| &entry.name == name)
                    .ok_or_else(refused)?;
                let entry = meerschaum_core::Value::from_json_str(
                    &serde_json::to_string(entry).map_err(|_| refused())?,
                )
                .map_err(|_| refused())?;
                let resource = crate::mcp::registry::Resource::from_value(
                    &entry,
                    crate::mcp::registry::Source::Native,
                )
                .ok_or_else(refused)?;
                let uri = object
                    .get("uri")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(refused)?;
                let variables = resource.matches(uri).ok_or_else(refused)?;
                let expected = serde_json::from_str::<serde_json::Value>(
                    &meerschaum_core::Value::Map(variables).to_json_string(),
                )
                .map_err(|_| refused())?;
                if object.len() != 2 || object.get("variables") != Some(&expected) {
                    return Err(refused());
                }
            }
            if let plugin_handlers::Selector::McpPrompt { name } = &handler.selector {
                let entry = metadata
                    .mcp_prompts
                    .iter()
                    .find(|entry| &entry.name == name)
                    .ok_or_else(refused)?;
                if object.iter().any(|(name, value)| {
                    !value.is_string()
                        || !entry.arguments.iter().any(|argument| {
                            argument.get("name").and_then(serde_json::Value::as_str)
                                == Some(name.as_str())
                        })
                }) || entry.arguments.iter().any(|argument| {
                    argument.get("required") == Some(&serde_json::Value::Bool(true))
                        && argument
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .is_none_or(|name| !object.contains_key(name))
                }) {
                    return Err(refused());
                }
            }
        }
        Ok(catalog)
    }

    pub fn handler(&self) -> Result<plugin_handlers::Handler, ApiError> {
        let catalog = self.validated()?;
        catalog.catalog().plugins[&self.invocation.plugin]
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.registry.as_ref())
            .and_then(|registry| {
                registry
                    .handlers()
                    .find(|handler| handler.id == self.invocation.handler_id)
            })
            .cloned()
            .ok_or_else(refused)
    }

    pub fn required(&self) -> Result<std::collections::BTreeSet<String>, ApiError> {
        let mut required = self.handler()?.scopes;
        required.insert("plugins:read".into());
        Ok(required)
    }

    pub fn digest(&self) -> Result<String, ApiError> {
        self.validated()?;
        Ok(plugins::sha256(
            &serde_json::to_vec(self).map_err(|_| refused())?,
        ))
    }

    /// Genuine optional Team: Everyone remains Host(None), never a registered nil Team.
    pub fn team_id(&self) -> Option<Uuid> {
        match &self.catalog.placement {
            super::plugin_host::Placement::Remote => Some(self.catalog.team_id),
            super::plugin_host::Placement::Host(host) => host.team_id,
        }
    }
    pub fn source_catalog(&self) -> plugins::Catalog {
        let mut catalog = self.catalog.clone();
        for source in catalog.plugins.values_mut() {
            source.metadata = None;
        }
        catalog
    }
    pub fn sources(&self) -> super::registration::Sources {
        match &self.catalog.placement {
            super::plugin_host::Placement::Remote => super::registration::Sources::Archives {
                catalog: self.source_catalog(),
            },
            super::plugin_host::Placement::Host(host) => super::registration::Sources::Files {
                hashes: host.files.clone(),
            },
        }
    }

    /// Preserve the existing Remote API while optional Team placement is explicit.
    pub fn authorize(
        &self,
        caller: &Access,
        team: &meerschaum_core::access::Team,
    ) -> Result<(), ApiError> {
        self.authorize_placement(caller, Some(team))
    }
    pub fn authorize_placement(
        &self,
        caller: &Access,
        team: Option<&meerschaum_core::access::Team>,
    ) -> Result<(), ApiError> {
        self.authorize_with_public(caller, team, caller)
    }
    fn authorize_with_public(
        &self,
        caller: &Access,
        team: Option<&meerschaum_core::access::Team>,
        original: &Access,
    ) -> Result<(), ApiError> {
        self.validated()?;
        if let Some(public) = &self.caller {
            public.matches(original)?;
        }
        if self.deadline <= chrono::Utc::now().timestamp() || caller.auth.is_none() {
            return Err(refused());
        }
        match (&self.catalog.placement, team) {
            (super::plugin_host::Placement::Remote, Some(team)) => {
                if team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok())
                    != Some(self.catalog.team_id)
                    || !team.is_isolated()
                    || !team
                        .execution()
                        .is_ok_and(|settings| settings.mode.is_remote())
                {
                    return Err(refused());
                }
            }
            (super::plugin_host::Placement::Host(host), Some(team)) => {
                if host.team_id.is_none()
                    || team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) != host.team_id
                    || !team.execution().is_ok_and(|settings| {
                        settings.mode == meerschaum_core::execution::Mode::Host
                    })
                {
                    return Err(refused());
                }
            }
            (super::plugin_host::Placement::Host(host), None) if host.team_id.is_none() => {}
            _ => return Err(refused()),
        }
        let name = team.map(|team| team.name.as_str());
        if !caller.principal.permits_team(name)
            || caller
                .principal
                .team_boundary
                .as_deref()
                .is_some_and(|boundary| !meerschaum_core::access::same_team(Some(boundary), name))
            || (team.is_some() && !caller.full_admin() && !caller.in_team(name))
        {
            return Err(refused());
        }
        let required = self.required()?;
        caller.require(
            &required.iter().map(String::as_str).collect::<Vec<_>>(),
            &[],
        )?;
        let ceiling =
            source_ceiling::Ceiling::from_catalog(&self.catalog).map_err(|_| refused())?;
        if !ceiling.within_parent(caller) || !ceiling.current(caller, team) {
            return Err(refused());
        }
        Ok(())
    }

    /// Hash actual Main-owned archives before the final fresh source projection. This is
    /// bounded data verification; it never extracts, imports or executes a plugin on Main.
    pub async fn verify_sources(&self, state: &AppState) -> Result<(), ApiError> {
        self.validated()?;
        if let super::plugin_host::Placement::Host(host) = &self.catalog.placement {
            // Main's Controller separately verifies the exact actual Files/view after each
            // awaited byte read. A server operation cannot select arbitrary source paths.
            if super::plugin_host::read_generation(&state.config.root_dir).map_err(|_| refused())?
                != host.generation
            {
                return Err(refused());
            }
            return Ok(());
        }
        let directory = state.config.plugins_archives_path.clone();
        let sources = self.catalog.plugins.clone();
        tokio::task::spawn_blocking(move || {
            use sha2::{Digest, Sha256};
            use std::io::Read;
            let mut remaining = 128 * 1024 * 1024usize;
            for (name, source) in sources {
                #[cfg(unix)]
                let file = std::fs::File::from(
                    rustix::fs::open(
                        directory.join(format!("{name}.tar.gz")),
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
                    let mut digest = Sha256::new();
                    let mut count = 0usize;
                    let mut buffer = [0u8; 64 * 1024];
                    loop {
                        let n = reader.read(&mut buffer).map_err(|_| ())?;
                        if n == 0 {
                            break;
                        }
                        count = count.checked_add(n).ok_or(())?;
                        if count > limit {
                            return Err(());
                        }
                        digest.update(&buffer[..n]);
                    }
                    if digest
                        .finalize()
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                        != source.archive_sha256
                    {
                        return Err(());
                    }
                    remaining -= count;
                }
            }
            Ok::<_, ()>(())
        })
        .await
        .map_err(|_| ApiError::unavailable("Extension source verification was interrupted."))?
        .map_err(|_| refused())
    }

    pub async fn admit(
        &self,
        state: &AppState,
        grant: Uuid,
        launch: Uuid,
        team: Option<Uuid>,
        words: &[String],
    ) -> Result<callbacks::Identity, ApiError> {
        if self.sysargs() != words || team != self.team_id() {
            return Err(refused());
        }
        self.verify_sources(state).await?;
        // No await after this load: expiry, original roles, parent and Team revocation are
        // reprojected together after every potentially slow source verification.
        let identity = callbacks::load_admission_identity(state, grant, launch).await?;
        let required = self.required()?;
        if identity.grant.purpose != callbacks::Purpose::Command
            || identity.grant.team_id != team
            || match &self.catalog.placement {
                super::plugin_host::Placement::Remote => {
                    identity.grant.generation != Some(self.catalog.generation)
                }
                super::plugin_host::Placement::Host(_) => {
                    // Host source authority is the separately checked Main epoch. The
                    // callback's own revocation generation is revalidated by its loader.
                    identity.grant.selection.as_ref().is_some_and(|settings| {
                        settings.mode != meerschaum_core::execution::Mode::Host
                    })
                }
            }
            || identity.grant.source_ceiling.as_ref()
                != Some(
                    &source_ceiling::Ceiling::from_catalog(&self.catalog).map_err(|_| refused())?,
                )
            || !required.is_subset(&identity.grant.required)
            || !required
                .iter()
                .all(|scope| identity.source_access().has(scope))
        {
            return Err(refused());
        }
        let current_team = identity.team.as_ref();
        if let Some(id) = team {
            let _ = super::admission::profile(&identity, id)?;
        } else if current_team.is_some() || !identity.source_access().principal.permits_team(None) {
            return Err(refused());
        }
        // A handler's declaration may require a permission outside the callback RPC
        // surface (for example users:read). Its Main-owned required set captured the
        // initiating request's token/parent narrowing, and load_admission_identity just
        // revalidated the original source. Retain only those exact declared permissions
        // in this local admission proof. Never put them into the returned Identity,
        // Grant.permissions or a callback bearer, whose route surface stays unchanged.
        let mut admission = identity.caller();
        admission.principal.permissions.extend(required);
        self.authorize_with_public(&admission, current_team, identity.source_access())?;
        Ok(identity)
    }

    pub fn validate_reply(&self, bytes: &[u8]) -> Result<plugin_handlers::Reply, ApiError> {
        if bytes.len() > plugins::MAX_CATALOG_BYTES {
            return Err(refused());
        }
        let reply: plugin_handlers::Reply = serde_json::from_slice(bytes).map_err(|_| refused())?;
        reply
            .validate(&self.handler()?.selector)
            .map_err(|_| refused())?;
        Ok(reply)
    }
}

/// Only the private Api profile may consume the descriptor, even for a Code Team.
pub fn validate_profile(profile: environment::Profile) -> Result<(), ApiError> {
    if profile == environment::Profile::Api {
        Ok(())
    } else {
        Err(refused())
    }
}

#[cfg(test)]
mod host_placement_tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn everyone_extension_never_accepts_an_unregistered_team_as_none() {
        use super::super::plugin_host::{HostIdentity, Placement};
        let host = HostIdentity {
            team_id: None,
            generation: Uuid::new_v4(),
            files: BTreeMap::from([("probe.py".into(), "a".repeat(64))]),
            view_sha256: "b".repeat(64),
        };
        let mut catalog = plugins::Catalog {
            schema: plugins::SCHEMA,
            release: env!("CARGO_PKG_VERSION").into(),
            team_id: Uuid::nil(),
            generation: host.generation,
            placement: Placement::Host(host.clone()),
            plugins: BTreeMap::from([(
                "probe".into(),
                plugins::Plugin {
                    archive_sha256: String::new(),
                    kind: plugins::SourceKind::PythonModule,
                    metadata: None,
                },
            )]),
        };
        let digest = plugins::Validated::decode_host(
            &serde_json::to_vec(&catalog).unwrap(),
            &host,
            &BTreeSet::from(["probe".into()]),
            &Default::default(),
        )
        .unwrap()
        .source_digest()
        .unwrap();
        let source = host.plugin_digests().unwrap().remove("probe").unwrap();
        let selector = plugin_handlers::Selector::Api {
            method: "GET".into(),
            path: "/probe".into(),
        };
        let handler = plugin_handlers::Handler {
            id: plugin_handlers::handler_id(&digest, "probe", &source, &selector).unwrap(),
            selector,
            scopes: BTreeSet::new(),
            dash: None,
        };
        catalog.plugins.get_mut("probe").unwrap().metadata = Some(plugins::Metadata {
            api_routes: vec![plugins::Route {
                name: "probe".into(),
                method: "GET".into(),
                path: "/probe".into(),
                scopes: Vec::new(),
            }],
            registry: Some(plugin_handlers::Registry {
                source_catalog_sha256: digest.clone(),
                plugin: "probe".into(),
                archive_sha256: String::new(),
                files_sha256: Some(source),
                api: plugin_handlers::Discovery::Complete {
                    entries: vec![handler.clone()],
                },
                mcp: plugin_handlers::Discovery::Complete {
                    entries: Vec::new(),
                },
                pages: plugin_handlers::Discovery::Complete {
                    entries: Vec::new(),
                },
                dash_callbacks: plugin_handlers::Discovery::Complete {
                    entries: Vec::new(),
                },
            }),
            ..Default::default()
        });
        let operation = Operation {
            caller: None,
            deadline: chrono::Utc::now().timestamp() + 60,
            catalog,
            invocation: plugin_handlers::Invocation {
                catalog_sha256: digest,
                plugin: "probe".into(),
                handler_id: handler.id,
                request: plugin_handlers::Request::Api {
                    method: "GET".into(),
                    path: "/probe".into(),
                    query: Vec::new(),
                    headers: Default::default(),
                    body: String::new(),
                },
            },
        };
        let mut caller = Access::everything();
        caller.no_auth = false;
        caller.auth = Some(crate::auth::Principal::User(
            meerschaum_core::users::User::new("reader", None),
        ));
        caller.principal = meerschaum_core::access::Principal::new(
            "reader",
            false,
            BTreeSet::from(["plugins:read".into()]),
            vec![(
                meerschaum_core::access::EVERYONE.into(),
                meerschaum_core::access::TeamRole::Member,
            )],
        );
        operation.authorize_placement(&caller, None).unwrap();
        let id = Uuid::new_v4();
        let source_digest = operation.validated().unwrap().source_digest().unwrap();
        assert_eq!(
            source_digest,
            plugins::sha256(&serde_json::to_vec(&operation.source_catalog()).unwrap()),
            "Remote source-only hashing must retain this same serialized shape"
        );
        let update = super::super::environment::Update {
            metadata_deadline: None,
            extension: Some(operation.clone()),
            registration: None,
            discovery_catalog_sha256: Some(source_digest),
            id,
            revision: 1,
            owner: "reader".into(),
            owner_id: Some("registered-reader".into()),
            profile: super::super::environment::Profile::Api,
            target: super::super::environment::Target::Job {
                name: format!("mrsm-{}", id.simple()),
            },
            callback_url: "http://127.0.0.1:8989".into(),
            credential: meerschaum_connector_api::models::ExecutionCredential {
                token: "synthetic-extension-credential".into(),
                expires_at: chrono::Utc::now().timestamp() + 20,
            },
        };
        let actor = super::super::control::Actor {
            owner: "reader".into(),
            username: Some("reader".into()),
            permissions: Default::default(),
            administrator: false,
        };
        update.validate(&actor).unwrap();
        let mut changed = update.clone();
        changed.discovery_catalog_sha256 = Some("f".repeat(64));
        assert!(changed.validate(&actor).is_err());
        for mutate_epoch in [false, true] {
            let mut changed = update.clone();
            let operation = changed.extension.as_mut().unwrap();
            let super::super::plugin_host::Placement::Host(host) = &mut operation.catalog.placement
            else {
                unreachable!()
            };
            if mutate_epoch {
                host.generation = Uuid::new_v4();
                operation.catalog.generation = host.generation;
            } else {
                host.view_sha256 = "c".repeat(64);
            }
            assert!(
                changed.validate(&actor).is_err(),
                "A Host epoch/view change cannot reuse an extension source digest"
            );
        }
        let mut changed = update.clone();
        changed.profile = super::super::environment::Profile::Team;
        assert!(changed.validate(&actor).is_err());
        let mut changed = update.clone();
        changed.target = super::super::environment::Target::Terminal;
        assert!(changed.validate(&actor).is_err());
        let unregistered = meerschaum_core::access::Team::new(meerschaum_core::access::EVERYONE);
        assert!(unregistered.id.is_none());
        assert!(
            operation
                .authorize_placement(&caller, Some(&unregistered))
                .is_err(),
            "An explicit unregistered Team must never alias genuine Everyone None"
        );
        assert_eq!(operation.team_id(), None);
        assert!(matches!(
            operation.sources(),
            super::super::registration::Sources::Files { .. }
        ));
    }
}
