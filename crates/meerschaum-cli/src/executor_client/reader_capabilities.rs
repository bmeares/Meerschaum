//! Inert reader classification: no jobs, imports, bearer issuance, store or epoch creation.
use super::Controller;
use meerschaum_core::access::{Team, can_use_plugin};
use meerschaum_server::{
    access::Access,
    executor::{
        admission, callbacks,
        plugin_capabilities::Capabilities,
        plugin_host::{HostIdentity, Placement},
        plugins,
        source_ceiling::Ceiling,
    },
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub(super) const UNAVAILABLE: &str = "Plugin source classification is unavailable. Ask an administrator to refresh confined plugin metadata for this source set, then retry the plan.";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Flags {
    pub python: bool,
    pub streaming: bool,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Observed {
    pub source_digest: String,
    factory_pin: Option<String>,
    pub entries: BTreeMap<String, Option<Flags>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Cached {
    producer_schema: u16,
    profile: meerschaum_server::executor::environment::Profile,
    signed_source_sha256: String,
    #[serde(default)]
    compiler_sha256: Option<String>,
    #[serde(default)]
    factory_receipt: Option<super::factory_metadata::Receipt>,
    catalog: plugins::Catalog,
}
struct Inputs {
    root: PathBuf,
    archives: PathBuf,
    directories: Vec<PathBuf>,
    caller: Access,
    team: Option<Team>,
    generation: Option<Uuid>,
    host_jobs: Option<std::sync::Arc<crate::executor_host_jobs::HostJobs>>,
}
impl Controller {
    /// A saved graph may reread the evidence created for its original submission.
    /// Its pending callback has no runtime authority to launch a discovery job.
    pub(super) async fn cached_graph_catalog(
        &self,
        proof: &callbacks::Identity,
    ) -> Result<plugins::Validated, String> {
        let inputs = self.reader_inputs(proof, proof.caller())?;
        tokio::task::spawn_blocking(move || {
            let (catalog, pin_digest) = cached_catalog(&inputs)?;
            let live = seed(&inputs)?;
            let pin = super::factory_metadata::pin(&inputs.root, live.catalog())?;
            if catalog.source_digest().map_err(|_| UNAVAILABLE)?
                != live.source_digest().map_err(|_| UNAVAILABLE)?
                || pin_digest
                    != pin
                        .as_ref()
                        .map(meerschaum_server::executor::factory_attestation::quote::pin_digest)
                        .transpose()?
            {
                return Err(UNAVAILABLE.into());
            }
            Ok(catalog)
        })
        .await
        .map_err(|_| UNAVAILABLE)?
    }

    pub(super) async fn cached_reader_capabilities(
        &self,
        proof: &callbacks::Identity,
        request: &Access,
        keys: &BTreeSet<String>,
    ) -> Result<Observed, String> {
        if keys.len() > 100_000
            || keys.iter().map(String::len).sum::<usize>() > 16 * 1024 * 1024
            || keys.iter().any(|key| {
                key.len() > 2048 || key.chars().any(char::is_control) || !key.contains(':')
            })
        {
            return Err(UNAVAILABLE.into());
        }
        let state = self.state()?;
        let identity = callbacks::load_identity(&state, proof.grant.id, proof.grant.launch_id)
            .await
            .map_err(|_| UNAVAILABLE)?;
        let caller =
            admission::reader_metadata_access(&identity, request).map_err(|_| UNAVAILABLE)?;
        for key in keys {
            meerschaum_server::access::guard::check_source_connector(&state, &caller, key)
                .map_err(|_| UNAVAILABLE)?;
        }
        let inputs = self.reader_inputs(&identity, caller)?;
        let requested = keys.clone();
        let observed = tokio::task::spawn_blocking(move || lookup(inputs, &requested))
            .await
            .map_err(|_| UNAVAILABLE)??;
        // Source selection, cache parsing and hashing run off async workers. Their completion
        // cannot substitute stale request authority or stale original source bytes.
        let identity = callbacks::load_identity(&state, proof.grant.id, proof.grant.launch_id)
            .await
            .map_err(|_| UNAVAILABLE)?;
        let caller =
            admission::reader_metadata_access(&identity, request).map_err(|_| UNAVAILABLE)?;
        for key in keys {
            meerschaum_server::access::guard::check_source_connector(&state, &caller, key)
                .map_err(|_| UNAVAILABLE)?;
        }
        let inputs = self.reader_inputs(&identity, caller)?;
        let (current, pin) = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let catalog = seed(&inputs)?;
            let pin = super::factory_metadata::pin(&inputs.root, catalog.catalog())?;
            let digest = pin
                .as_ref()
                .map(meerschaum_server::executor::factory_attestation::quote::pin_digest)
                .transpose()?;
            Ok((catalog.source_digest().map_err(|_| UNAVAILABLE)?, digest))
        })
        .await
        .map_err(|_| UNAVAILABLE)??;
        if current != observed.source_digest || pin != observed.factory_pin {
            return Err(UNAVAILABLE.into());
        }
        let identity = callbacks::load_identity(&state, proof.grant.id, proof.grant.launch_id)
            .await
            .map_err(|_| UNAVAILABLE)?;
        let caller =
            admission::reader_metadata_access(&identity, request).map_err(|_| UNAVAILABLE)?;
        for key in keys {
            meerschaum_server::access::guard::check_source_connector(&state, &caller, key)
                .map_err(|_| UNAVAILABLE)?;
        }
        Ok(observed)
    }
    fn reader_inputs(
        &self,
        identity: &callbacks::Identity,
        caller: Access,
    ) -> Result<Inputs, String> {
        let team = identity
            .grant
            .team_id
            .map(|id| {
                caller
                    .team_records
                    .iter()
                    .find(|team| {
                        team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) == Some(id)
                    })
                    .cloned()
                    .ok_or(UNAVAILABLE)
            })
            .transpose()?;
        Ok(Inputs {
            root: self.root.clone(),
            archives: self.state()?.config.plugins_archives_path.clone(),
            directories: self.config()?.paths().plugins_dirs.clone(),
            caller,
            team,
            generation: identity.grant.generation,
            host_jobs: self.host_jobs().ok(),
        })
    }
}
fn seed(inputs: &Inputs) -> Result<plugins::Validated, String> {
    let reserved = reserved_actions();
    let (catalog, grants) = if let Some(team) = inputs.team.as_ref().filter(|team| {
        team.execution()
            .is_ok_and(|settings| settings.mode.is_remote())
    }) {
        let grants: BTreeSet<_> = team
            .granted_plugins()
            .into_iter()
            .filter(|name| {
                can_use_plugin(&inputs.caller.principal, name)
                    && meerschaum_server::executor::source_ceiling::permits_name(
                        &inputs.caller,
                        name,
                    )
            })
            .collect();
        if grants.is_empty() {
            return Err(UNAVAILABLE.into());
        }
        let id = team
            .id
            .as_deref()
            .and_then(|id| Uuid::parse_str(id).ok())
            .ok_or(UNAVAILABLE)?;
        let generation = inputs.generation.ok_or(UNAVAILABLE)?;
        (
            super::plugins::source_catalog(&inputs.archives, id, generation, &grants)
                .map_err(|_| UNAVAILABLE)?,
            grants,
        )
    } else {
        let (sources, grants) =
            crate::executor_host_jobs::plugin_sources(&inputs.caller, &inputs.directories)
                .map_err(|_| UNAVAILABLE)?;
        if grants.is_empty() {
            return Err(UNAVAILABLE.into());
        }
        let files = crate::executor_plugin_files::inspect(&inputs.root, &sources)
            .map_err(|_| UNAVAILABLE)?;
        let generation =
            std::str::from_utf8(&regular(&inputs.root.join("executor-host-identity"), 64)?)
                .ok()
                .and_then(|text| Uuid::parse_str(text).ok())
                .filter(|id| !id.is_nil())
                .ok_or(UNAVAILABLE)?;
        let team_id = inputs
            .team
            .as_ref()
            .map(|team| {
                team.id
                    .as_deref()
                    .and_then(|id| Uuid::parse_str(id).ok())
                    .ok_or(UNAVAILABLE)
            })
            .transpose()?;
        let host = HostIdentity {
            team_id,
            generation,
            files: files.hashes,
            view_sha256: files.view_sha256,
        };
        let entries = grants
            .iter()
            .map(|name| {
                Ok((
                    name.clone(),
                    plugins::Plugin {
                        archive_sha256: String::new(),
                        kind: host.source_kind(name).map_err(|_| UNAVAILABLE)?,
                        metadata: None,
                    },
                ))
            })
            .collect::<Result<_, &str>>()?;
        (
            plugins::Catalog {
                schema: plugins::SCHEMA,
                release: env!("CARGO_PKG_VERSION").into(),
                team_id: team_id.unwrap_or_else(Uuid::nil),
                generation,
                placement: Placement::Host(host),
                plugins: entries,
            },
            grants,
        )
    };
    let ceiling = Ceiling::from_catalog(&catalog).map_err(|_| UNAVAILABLE)?;
    if !ceiling.within_parent(&inputs.caller)
        || !ceiling.current(&inputs.caller, inputs.team.as_ref())
    {
        return Err(UNAVAILABLE.into());
    }
    let bytes = serde_json::to_vec(&catalog).map_err(|_| UNAVAILABLE)?;
    match &catalog.placement {
        Placement::Remote => plugins::Validated::decode(
            &bytes,
            plugins::Expected {
                team_id: catalog.team_id,
                generation: catalog.generation,
                grants: &grants,
                reserved_actions: &reserved,
            },
        ),
        Placement::Host(host) => plugins::Validated::decode_host(&bytes, host, &grants, &reserved),
    }
    .map_err(|_| UNAVAILABLE.into())
}
fn cached_catalog(inputs: &Inputs) -> Result<(plugins::Validated, Option<String>), String> {
    let seed = seed(inputs)?;
    let digest = seed.source_digest().map_err(|_| UNAVAILABLE)?;
    let pin =
        super::factory_metadata::pin(&inputs.root, seed.catalog()).map_err(|_| UNAVAILABLE)?;
    let pin_sha256 = pin
        .as_ref()
        .map(meerschaum_server::executor::factory_attestation::quote::pin_digest)
        .transpose()
        .map_err(|_| UNAVAILABLE)?;
    let compiler_sha256 = super::plugins::metadata_compiler(
        seed.catalog(),
        if !seed.catalog().placement.is_remote() {
            inputs
                .host_jobs
                .as_ref()
                .and_then(|jobs| jobs.source_compiler_identity())
        } else {
            pin.as_ref()
                .and_then(|pin| pin.artifacts.compiler_sha256.clone())
        },
    )
    .map_err(|_| UNAVAILABLE)?;
    let path = super::plugins::metadata_cache_path(
        &inputs.root,
        &digest,
        compiler_sha256.as_deref(),
        pin_sha256.as_deref(),
    );
    let cached: Cached =
        serde_json::from_slice(&regular(&path, plugins::MAX_CATALOG_BYTES + 4096)?)
            .map_err(|_| UNAVAILABLE)?;
    if cached.producer_schema != plugins::PRODUCER_SCHEMA
        || cached.profile != meerschaum_server::executor::environment::Profile::Metadata
        || cached.signed_source_sha256 != digest
        || cached.compiler_sha256 != compiler_sha256
        || !super::factory_metadata::matches(cached.factory_receipt.as_ref(), pin_sha256.as_deref())
    {
        return Err(UNAVAILABLE.into());
    }
    let grants = seed.catalog().plugins.keys().cloned().collect();
    let reserved = reserved_actions();
    let bytes = serde_json::to_vec(&cached.catalog).map_err(|_| UNAVAILABLE)?;
    let catalog = match &seed.catalog().placement {
        Placement::Remote => plugins::Validated::decode(
            &bytes,
            plugins::Expected {
                team_id: seed.catalog().team_id,
                generation: seed.catalog().generation,
                grants: &grants,
                reserved_actions: &reserved,
            },
        ),
        Placement::Host(host) => plugins::Validated::decode_host(&bytes, host, &grants, &reserved),
    }
    .map_err(|_| UNAVAILABLE)?;
    if !catalog.complete() || catalog.source_digest().map_err(|_| UNAVAILABLE)? != digest {
        return Err(UNAVAILABLE.into());
    }
    Ok((catalog, pin_sha256))
}

fn lookup(inputs: Inputs, keys: &BTreeSet<String>) -> Result<Observed, String> {
    let (catalog, pin_sha256) = cached_catalog(&inputs)?;
    let digest = catalog.source_digest().map_err(|_| UNAVAILABLE)?;
    let capabilities = Capabilities {
        source_catalog_sha256: digest.clone(),
        plugins: catalog
            .catalog()
            .plugins
            .iter()
            .map(|(name, source)| {
                source
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.capabilities.clone())
                    .map(|caps| (name.clone(), caps))
                    .ok_or(UNAVAILABLE)
            })
            .collect::<Result<_, _>>()?,
    };
    capabilities.validate(&catalog).map_err(|_| UNAVAILABLE)?;
    // Keep only scalar observations. Do not copy large method/schema sets for every label.
    let mut types = BTreeMap::new();
    let entries = keys
        .iter()
        .map(|key| {
            let (kind, _) = key.split_once(':').ok_or(UNAVAILABLE)?;
            let cache_key = if kind == "plugin" { key.as_str() } else { kind };
            let flags = *types.entry(cache_key.to_string()).or_insert_with(|| {
                capabilities.lookup(key).map(|caps| Flags {
                    python: caps.python,
                    streaming: caps.streaming,
                })
            });
            Ok((key.clone(), flags))
        })
        .collect::<Result<_, &str>>()?;
    Ok(Observed {
        source_digest: digest,
        factory_pin: pin_sha256,
        entries,
    })
}
fn regular(path: &Path, limit: usize) -> Result<Vec<u8>, String> {
    use std::io::Read;
    #[cfg(not(unix))]
    return Err(UNAVAILABLE.into());
    #[cfg(unix)]
    {
        let file = std::fs::File::from(
            rustix::fs::open(
                path,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::NONBLOCK
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(|_| UNAVAILABLE)?,
        );
        let metadata = file.metadata().map_err(|_| UNAVAILABLE)?;
        if !metadata.is_file() || metadata.len() > limit as u64 {
            return Err(UNAVAILABLE.into());
        }
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| UNAVAILABLE)?;
        if bytes.len() > limit {
            return Err(UNAVAILABLE.into());
        }
        Ok(bytes)
    }
}

fn reserved_actions() -> BTreeSet<String> {
    crate::actions::CORE_ACTIONS
        .iter()
        .map(|action| action.name.to_string())
        .collect()
}
