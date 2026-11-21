//! Main-host API-only job runtime. Public names and source authority stay in the common catalog.
//!
//! This adapter never calls main's HTTP job routes. Its private job names use a direct local
//! backend, while data and nested job requests use the launch's bounded main-API credential.
//! Its `metadata` child prepares credential-free Metadata launches and their cache attestation.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use meerschaum_core::access::same_team;
use meerschaum_core::team_scope::api_execution;
use meerschaum_core::{Config, Map, Value};
use meerschaum_jobs::JobsContext;
use meerschaum_server::AppState;
use meerschaum_server::access::{Access, guard};
use meerschaum_server::backends::JobsBackend;
use meerschaum_server::executor::environment::{Profile, Target, Update};
use uuid::Uuid;

use crate::executor::contexts::{Contexts, HostInputs};

mod metadata;

/// Host-selected local lifecycle with launch-specific configuration and mandatory confinement.
pub(crate) struct HostJobs {
    contexts: Arc<Contexts>,
    context: JobsContext,
    backend: Arc<dyn JobsBackend>,
}

impl HostJobs {
    /// Install this context on the main restart monitor too: marked jobs without restored
    /// source authority must fail closed instead of starting with ambient host configuration.
    pub(crate) fn new(root: &Path, context: JobsContext) -> Result<Self, String> {
        if context.paths.root != root || context.launch_resolver.is_some() {
            return Err("The main job runtime already has a different launch policy.".into());
        }
        let contexts = Arc::new(Contexts::new(root.to_path_buf()));
        let context = context.with_launch_resolver(contexts.clone());
        Ok(Self {
            contexts,
            backend: Arc::new(crate::jobs_backend::LocalJobsBackend::new(context.clone())),
            context,
        })
    }

    /// Trusted native construction, before any private job preparation or cache lookup.
    pub(crate) fn set_source_compiler(
        &self,
        compiler: Option<Arc<crate::executor::compiler::Compiler>>,
    ) -> Result<(), String> {
        self.contexts.set_source_compiler(compiler)
    }
    /// A Main cache lookup freezes this selection; a worker never supplies this identity.
    pub(crate) fn source_compiler_identity(&self) -> Option<String> {
        self.contexts.source_compiler_identity()
    }

    pub(crate) fn context(&self) -> JobsContext {
        self.context.clone()
    }

    /// Internal transport only. Canonical visibility and the held source lease are checked
    /// before calling this backend; never expose its private UUID names as public jobs.
    pub(crate) fn backend(&self) -> Arc<dyn JobsBackend> {
        self.backend.clone()
    }

    /// Only genuine absence authorizes explicit definition restoration. The generic local
    /// backend's bool existence API intentionally cannot distinguish a corrupt definition.
    pub(crate) fn definition_exists(&self, name: &str) -> Result<bool, String> {
        use std::io::Read;
        let valid = name
            .strip_prefix("mrsm-")
            .and_then(|value| Uuid::parse_str(value).ok())
            .is_some_and(|id| !id.is_nil() && name == format!("mrsm-{}", id.simple()));
        if !valid {
            return Err("The host job has no canonical private identity.".into());
        }
        let directory = self.context.paths.jobs_dir.join(name);
        match std::fs::symlink_metadata(&directory) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Ok(metadata) if metadata.is_dir() => {}
            _ => return Err("The host job definition is unavailable.".into()),
        }
        let path = directory.join("properties.json");
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|_| "The host job definition is unavailable.")?;
        if !metadata.is_file() || metadata.len() > 4 * 1024 * 1024 {
            return Err("The host job definition is invalid or oversized.".into());
        }
        let file =
            std::fs::File::open(path).map_err(|_| "The host job definition is unreadable.")?;
        let mut bytes = Vec::new();
        file.take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "The host job definition is unreadable.")?;
        if bytes.len() > 4 * 1024 * 1024
            || serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .is_none_or(|value| !value.is_object())
        {
            return Err("The host job definition is invalid or oversized.".into());
        }
        Ok(true)
    }

    /// Caller is freshly reconstructed from the immutable original callback source. Only
    /// admitted label names and granted source files reach the per-launch alias preparation.
    pub(crate) fn update(
        &self,
        state: &AppState,
        caller: &Access,
        config: &Config,
        trusted_environment: &[(String, String)],
        update: Update,
        accepted_instances: &BTreeSet<String>,
    ) -> Result<(), String> {
        if update.profile != Profile::Api || !matches!(update.target, Target::Job { .. }) {
            return Err("A host API job requires the API-only job profile.".into());
        }
        let mut base = filtered_base_for_operation(state, caller, accepted_instances)?;
        let (sources, grants) = plugin_sources(caller, &config.paths().plugins_dirs)?;
        let snapshot = crate::executor::plugin_files::Store::new(&state.config.root_dir)?
            .snapshot(&sources, None)?;
        let hashes = &snapshot.hashes;
        if let Some(meerschaum_server::auth::Principal::Execution(identity)) = &caller.auth
            && identity.grant.source_ceiling.clone().unwrap_or_default()
                != meerschaum_server::executor::source_ceiling::Ceiling::from_files(hashes)?
        {
            return Err(
                "The Host source content differs from this launch's immutable source ceiling."
                    .into(),
            );
        }
        if let Some(registration) = &update.registration {
            let meerschaum_server::executor::registration::Sources::Files { hashes } =
                &registration.sources
            else {
                return Err("A Host registration cannot adopt a remote archive catalog.".into());
            };
            if snapshot.hashes != *hashes {
                return Err(
                    "The Host registration's original source content or grant set changed.".into(),
                );
            }
        }
        if let Some(extension) = &update.extension {
            extension.validated().map_err(|error| error.detail_text())?;
            let meerschaum_server::executor::plugin_host::Placement::Host(host) =
                &extension.catalog.placement
            else {
                return Err("A Host extension cannot adopt remote archives.".into());
            };
            if snapshot.hashes != host.files
                || snapshot.view_sha256 != host.view_sha256
                || meerschaum_server::executor::plugin_host::read_generation(
                    &state.config.root_dir,
                )? != host.generation
            {
                return Err("The Host extension source view changed before delivery.".into());
            }
            base.insert(
                meerschaum_server::executor::plugins::CONFIG_KEY.into(),
                Value::from_json_str(
                    &serde_json::to_string(&extension.source_catalog())
                        .map_err(|_| "Invalid Host extension catalog.")?,
                )
                .map_err(|_| "Invalid Host extension catalog.")?,
            );
        }
        let mut environment = trusted_environment.to_vec();
        environment.retain(|(name, _)| {
            !matches!(name.as_str(), "MRSM_PLUGINS_DIR" | "MRSM_EXECUTOR_PLUGINS")
        });
        environment.push((
            "MRSM_EXECUTOR_PLUGINS".into(),
            serde_json::to_string(&grants).map_err(|_| "Could not encode host plugin grants.")?,
        ));
        self.contexts.update_host(
            update,
            HostInputs {
                base,
                environment,
                sources: snapshot.sources,
            },
        )
    }

    pub(crate) fn update_metadata(
        &self,
        state: &AppState,
        caller: &Access,
        config: &Config,
        trusted_environment: &[(String, String)],
        update: Update,
        discovery: &meerschaum_server::executor::plugin_discovery::Discovery,
    ) -> Result<(), String> {
        metadata::update(
            self,
            state,
            caller,
            config,
            trusted_environment,
            update,
            discovery,
        )
    }

    pub(crate) fn attest_discovery(
        &self,
        record: &crate::executor_jobs::Record,
        digest: &str,
    ) -> Result<(), String> {
        metadata::attest(self, record, digest)
    }

    pub(crate) async fn revoke(&self, id: Uuid, revision: u64) -> Result<(), String> {
        let administrator = meerschaum_server::executor::control::Actor {
            owner: "main".into(),
            username: None,
            permissions: Default::default(),
            administrator: true,
        };
        let contexts = self.contexts.clone();
        let context = self.context.clone();
        // The owned task keeps the exact-revision lock until stop and retirement finish,
        // even when a caller's request deadline expires while waiting for this operation.
        tokio::task::spawn_blocking(move || {
            contexts
                .revoke_job(&administrator, id, revision, &context)
                .map(|_| ())
        })
        .await
        .map_err(|_| "The host job retirement task failed.".to_string())?
    }

    pub(crate) fn expire(&self) {
        self.contexts.expire_jobs(&self.context);
    }
}

/// Credential-free label snapshot. Do not pass the main configuration to API alias projection:
/// even hidden connector names must be absent from this launch's configuration and discovery.
pub(crate) fn filtered_base(state: &AppState, caller: &Access) -> Result<Map, String> {
    filtered_base_for_operation(state, caller, &BTreeSet::new())
}

fn filtered_base_for_operation(
    state: &AppState,
    caller: &Access,
    accepted: &BTreeSet<String>,
) -> Result<Map, String> {
    if accepted.len() > 512
        || accepted
            .iter()
            .any(|keys| api_execution::split_keys(keys).is_err())
    {
        return Err("The accepted operation has invalid logical transport labels.".into());
    }
    let default = caller
        .team_records
        .iter()
        .find(|team| same_team(Some(&team.name), Some(&caller.primary_team)))
        .and_then(|team| team.default_instance())
        .unwrap_or_else(|| state.config.instance_keys.clone());
    let mut connectors = Map::new();
    let mut admitted = BTreeSet::new();
    for (kind, labels) in state.connectors().iter() {
        if !matches!(kind.as_str(), "sql" | "api" | "valkey") {
            continue;
        }
        let mut visible = Map::new();
        for (label, attributes) in labels.to_map() {
            let keys = format!("{kind}:{label}");
            if label == "default"
                || attributes.as_map().is_none()
                || api_execution::split_keys(&keys).is_err()
                || !guard::may_use_connector(state, caller, &keys)
                || (!caller.has("connectors:read") && keys != default && !accepted.contains(&keys))
                || (!state.config.allow_multiple_instances && keys != state.config.instance_keys)
                || !state
                    .config
                    .allowed_instance_keys
                    .iter()
                    .any(|pattern| meerschaum_server::config::fnmatch(&keys, pattern))
            {
                continue;
            }
            admitted.insert(keys);
            visible.insert(label, Value::Map(Map::new()));
        }
        if !visible.is_empty() {
            connectors.insert(kind.clone(), Value::Map(visible));
        }
    }
    if !admitted.contains(&default) {
        return Err(
            "The host job's logical default instance is not available to its source.".into(),
        );
    }
    if !accepted.is_subset(&admitted) {
        return Err("A logical connector required by the accepted operation is no longer authorized or served.".into());
    }
    Ok(Map::from_iter([(
        "meerschaum".into(),
        Value::Map(Map::from_iter([
            ("instance".into(), default.into()),
            ("connectors".into(), Value::Map(connectors)),
        ])),
    )]))
}

/// Host-selected read-only sources, resolved without manifest discovery or imports. Entry
/// points receive the same immutable cap; no team venv or writable source directory is granted.
pub(crate) fn plugin_sources(
    caller: &Access,
    directories: &[PathBuf],
) -> Result<crate::executor::host_sources::SelectedSources, String> {
    crate::executor::host_sources::select(caller, directories)
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_core::access::{Principal, Role, Team, TeamGrants, TeamRole};

    fn caller(team: &Team) -> Access {
        let mut caller = Access::everything();
        caller.no_auth = false;
        caller.primary_team = team.name.clone();
        caller.team_records = vec![team.clone()];
        caller.principal = Principal::new(
            "alice",
            false,
            Role::Operator.permissions(),
            vec![(team.name.clone(), TeamRole::Member)],
        )
        .with_grants(TeamGrants::of(
            &[(team.name.clone(), TeamRole::Member)],
            std::slice::from_ref(team),
            Some(&team.name),
        ));
        caller
    }

    #[test]
    fn host_definition_recovery_refuses_corruption_and_ambiguous_partial_directories() {
        let root = tempfile::tempdir().unwrap();
        let jobs = HostJobs::new(root.path(), JobsContext::for_root(root.path())).unwrap();
        let name = format!("mrsm-{}", Uuid::new_v4().simple());
        assert!(!jobs.definition_exists(&name).unwrap());
        let directory = root.path().join("jobs").join(&name);
        std::fs::create_dir_all(&directory).unwrap();
        assert!(jobs.definition_exists(&name).is_err());
        std::fs::write(directory.join("properties.json"), "{broken").unwrap();
        assert!(jobs.definition_exists(&name).is_err());
        std::fs::write(directory.join("properties.json"), "{}").unwrap();
        assert!(jobs.definition_exists(&name).unwrap());
        assert!(jobs.definition_exists("../other").is_err());
    }

    #[tokio::test]
    async fn label_projection_filters_current_team_and_served_instance_policy_without_credentials()
    {
        let root = tempfile::tempdir().unwrap();
        let instance = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", root.path().join("model.db").display()),
            )
            .await
            .unwrap(),
        );
        let mut config = meerschaum_server::ServerConfig::for_root("sql:main", root.path());
        config.allow_multiple_instances = true;
        config.allowed_instance_keys = vec!["sql:*".into()];
        let connectors = Value::from_json_str(
            r#"{"sql":{"main":{"password":"main-only","teams":["Wetlands"]},"visible":{"password":"visible-only","teams":["Wetlands"]},"unselected":{"password":"unselected-only"},"hidden":{"password":"hidden-only","teams":["Other"]}},"api":{"unserved":{"password":"api-only"}}}"#,
        )
        .unwrap()
        .to_map();
        config.connectors = connectors;
        let state = AppState::builder(config, instance).build().unwrap();
        let mut team = Team::new("Wetlands");
        team.attributes = Value::from_json_str(
            r#"{"isolated":true,"instance":"sql:main","connectors":["sql:main","sql:visible"]}"#,
        )
        .unwrap()
        .to_map();
        let mut source = caller(&team);
        let base = filtered_base(&state, &source).unwrap();
        let serialized = Value::Map(base.clone()).to_json_string();
        assert!(!serialized.contains("password"));
        assert!(!serialized.contains("hidden"));
        assert!(!serialized.contains("unserved"));
        let base = serde_json::to_value(base).unwrap();
        assert!(
            base["meerschaum"]["connectors"]["sql"]["visible"]
                .as_object()
                .unwrap()
                .is_empty()
        );
        source.principal.permissions.remove("connectors:read");
        let base = serde_json::to_value(filtered_base(&state, &source).unwrap()).unwrap();
        assert!(
            base["meerschaum"]["connectors"]["sql"]
                .get("visible")
                .is_none()
        );
        assert!(
            base["meerschaum"]["connectors"]["sql"]
                .get("main")
                .is_some()
        );
        source.principal.grants.isolated = false;
        assert!(guard::may_use_connector(&state, &source, "sql:unselected"));
        let accepted = BTreeSet::from(["sql:main".to_string(), "sql:visible".to_string()]);
        let projected = filtered_base_for_operation(&state, &source, &accepted).unwrap();
        let encoded = Value::Map(projected.clone()).to_json_string();
        assert!(
            !encoded.contains("password")
                && !encoded.contains("unselected")
                && !encoded.contains("hidden")
        );
        let projected = serde_json::to_value(projected).unwrap();
        assert!(
            projected["meerschaum"]["connectors"]["sql"]["visible"]
                .as_object()
                .unwrap()
                .is_empty()
        );
        assert!(
            !source.has("connectors:read"),
            "Alias selection synthesized connector enumeration permission"
        );
        for unavailable in ["sql:hidden", "api:unserved", "sql:absent"] {
            assert!(
                filtered_base_for_operation(&state, &source, &BTreeSet::from([unavailable.into()]))
                    .is_err(),
                "Accepted unavailable {unavailable}"
            );
        }
        source.principal.teams.clear();
        source.principal.grants.connectors.clear();
        source.team_records.clear();
        assert!(
            filtered_base_for_operation(&state, &source, &accepted).is_err(),
            "A removed Team member retained selected transport aliases"
        );
    }

    #[test]
    fn source_projection_does_not_import_or_grant_neighbors_and_applies_current_plugin_grants() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("allowed.py"),
            "raise RuntimeError('never import')",
        )
        .unwrap();
        std::fs::write(
            root.path().join("hidden.py"),
            "raise RuntimeError('never import')",
        )
        .unwrap();
        std::fs::write(root.path().join(".env"), "synthetic-private-neighbor").unwrap();
        let mut team = Team::new("Wetlands");
        team.attributes = Value::from_json_str(r#"{"isolated":true,"plugins":["allowed"]}"#)
            .unwrap()
            .to_map();
        let (sources, grants) =
            plugin_sources(&caller(&team), &[root.path().to_path_buf()]).unwrap();
        assert_eq!(grants, BTreeSet::from(["allowed".into()]));
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].0, std::ffi::OsString::from("allowed.py"));
        assert_eq!(sources[0].1, root.path().join("allowed.py"));
    }
}
