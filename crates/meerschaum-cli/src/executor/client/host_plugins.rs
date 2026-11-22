//! Main-host credential-free metadata. This module never imports selected plugin code.
//!
//! The private catalog and the local sealed launch proof, rather than worker environment or
//! catalog-shaped output, establish whether a completed result is eligible for shared cache.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use meerschaum_core::{Map, Value};
use meerschaum_server::access::Access;
use meerschaum_server::executor::plugin_discovery::{Discovery, Origin};
use meerschaum_server::executor::plugin_host::{HostIdentity, Placement};
use meerschaum_server::executor::{callbacks, context, environment, plugins};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::Controller;
use crate::executor::jobs::{Catalog, JobPlacement, Record};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cached {
    #[serde(default)]
    producer_schema: u16,
    profile: environment::Profile,
    signed_source_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compiler_sha256: Option<String>,
    catalog: plugins::Catalog,
}

impl Controller {
    /// Host/Everyone use genuine file snapshots and a private host generation, never a
    /// synthesized registered Team or an executor archive claim.
    pub fn discover_host_plugin_catalog(
        &self,
        team_id: Option<Uuid>,
        grants: BTreeSet<String>,
    ) -> std::pin::Pin<
        Box<impl std::future::Future<Output = Result<plugins::Validated, String>> + Send + '_>,
    > {
        Box::pin(async move {
            let caller = context::current()
                .filter(|caller| !caller.no_auth && caller.auth.is_some())
                .ok_or("Host metadata requires its authenticated original Main operation.")?;
            let seed = self.capture_host_catalog(team_id, grants, &caller).await?;
            let compiler_sha256 = super::plugins::metadata_compiler(
                seed.catalog(),
                self.host_jobs()?.source_compiler_identity(),
            )?;
            let discovery = Discovery {
                catalog: seed.catalog().clone(),
                origin: Origin::capture(),
                deadline: chrono::Utc::now().timestamp() + 60,
            };
            let digest = discovery.catalog_sha256().map_err(|e| e.detail_text())?;
            let lease_id = Uuid::parse_str(&digest[..32])
                .map_err(|_| "Invalid immutable Host metadata lease identity.")?;
            let _catalog_lease = meerschaum_server::executor::storage::lock(
                self.root.join("executor-plugin-discovery-leases"),
                lease_id,
            )
            .await
            .map_err(
                |_| "This immutable metadata view is busy or unavailable. Retry this operation.",
            )?;
            let state = self.state()?;
            let team_name = if let Some(id) = team_id {
                Some(
                    self.instance
                        .get_teams()
                        .await
                        .map_err(|_| "The Host metadata Team is unavailable.")?
                        .into_iter()
                        .find(|team| {
                            team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) == Some(id)
                        })
                        .ok_or("The Host metadata Team was deleted.")?
                        .name,
                )
            } else {
                None
            };
            let mut properties = Map::new();
            let actor = context::actor(&caller);
            meerschaum_core::job_access::JobAccess::stamp(
                &mut properties,
                Some(&actor.owner),
                team_name.as_deref(),
            );
            properties.insert(
                "logs".into(),
                Value::Map(Map::from_iter([
                    ("write_timestamps".into(), false.into()),
                    ("stdin".into(), false.into()),
                ])),
            );
            properties.insert("mrsm_execution".into(), Value::from_json_str(r#"{"kind":"temporary_execution","origin":"plugin_metadata","retention":{"success_seconds":60,"failure_seconds":300,"summary_seconds":300}}"#).map_err(|_| "Invalid Host metadata tracking properties.")?);
            let mut record = Record::new(
                &format!("metadata-{}", Uuid::new_v4().simple()),
                team_id.unwrap_or_else(Uuid::nil),
                discovery.sysargs(),
                properties,
            );
            record.placement = JobPlacement::HostApi;
            record.profile = environment::Profile::Metadata;
            record.discovery = Some(discovery.clone());
            record.callback = self.register_job_callback(&mut record, None).await?;
            let grant = record
                .callback
                .ok_or("Host metadata has no original source proof.")?;
            let cancellation_state = Arc::new(CancellationState::default());
            let mut cancellation = Cancellation {
                controller: self.clone(),
                record: Some(record.clone()),
                state: cancellation_state.clone(),
            };
            let launch = canonical_id(&record)?;
            let reserved = reserved_actions();
            let cache_path = super::plugins::metadata_cache_path(
                &self.root,
                &digest,
                compiler_sha256.as_deref(),
                None,
            );
            let read_path = cache_path.clone();
            let cached = tokio::task::spawn_blocking(move || read_cached(&read_path))
                .await
                .map_err(|_| "Host metadata cache lookup was interrupted.")?;
            let cached = match cached {
                Ok(cached) => cached,
                Err(error) => {
                    self.discard_job_callback(grant).await;
                    return Err(error);
                }
            };
            if let Some(cached) = cached {
                let result = async {
                    if cached.compiler_sha256 != compiler_sha256 {
                        return Err("The cached Host metadata uses another native compiler.".into());
                    }
                    let identity = discovery
                        .admit(&state, grant, launch, record.team_id, &record.sysargs)
                        .await
                        .map_err(|e| e.detail_text())?;
                    self.confirm_host_discovery_sources(&discovery, &identity.caller())
                        .await?;
                    discovery
                        .admit(&state, grant, launch, record.team_id, &record.sysargs)
                        .await
                        .map_err(|e| e.detail_text())?;
                    discovery
                        .accept(
                            cached.profile,
                            &cached.signed_source_sha256,
                            &serde_json::to_vec(&cached.catalog)
                                .map_err(|_| "Invalid Host cache encoding.")?,
                            &reserved,
                        )
                        .map_err(|e| e.detail_text())
                }
                .await;
                self.discard_job_callback(grant).await;
                cancellation.record = None;
                return result;
            }
            let root = self.root.clone();
            let reservation = record.clone();
            // The blocking task owns this lease until reservation completes. Cancellation
            // cleanup cannot observe absence and then lose a later detached reservation.
            let reservation_lease = self.job_lock(&record).await?;
            let reservation_state = cancellation_state.clone();
            let reserved_name = tokio::task::spawn_blocking(move || {
                let _lease = reservation_lease;
                let catalog = Catalog::new(&root);
                let reserved = catalog.reserve(reservation.clone());
                if matches!(reserved, Ok(true))
                    && reservation_state.canceled.load(Ordering::Acquire)
                {
                    // This owned reservation has not been delivered or launched. Only its
                    // complete canonical identity can be removed by the catalog operation.
                    let _ = catalog.remove(&reservation);
                }
                reserved
            })
            .await
            .map_err(|_| "Host metadata reservation was interrupted.")?;
            if !matches!(reserved_name, Ok(true)) {
                self.discard_job_callback(grant).await;
                return Err("The canonical Host metadata job could not be reserved.".into());
            }
            let _lease = self.job_lock(&record).await?;
            let jobs = self.host_jobs()?;
            let backend = jobs.backend();
            let mut operation = async {
                cancellation_state
                    .delivery_started
                    .store(true, Ordering::Release);
                self.deliver_job_context(&mut record, false).await?;
                let identity = discovery
                    .admit(&state, grant, launch, record.team_id, &record.sysargs)
                    .await
                    .map_err(|e| e.detail_text())?;
                self.confirm_host_discovery_sources(&discovery, &identity.caller())
                    .await?;
                let mut properties = record.properties.clone();
                properties.insert(
                    environment::JOB_CONTEXT_PROPERTY.into(),
                    launch.to_string().into(),
                );
                let created = context::scope(
                    None,
                    backend.create(
                        &record.remote_name,
                        record.sysargs.clone(),
                        Some(properties),
                    ),
                )
                .await;
                match created {
                    meerschaum_server::backends::CreateJob::Started(result) => {
                        cancellation_state.definition_owned.store(true, Ordering::Release);
                        if !result.success {
                            return Err(result.message);
                        }
                    }
                    meerschaum_server::backends::CreateJob::Unavailable(error) => return Err(error),
                    meerschaum_server::backends::CreateJob::Exists => {
                        return Err("The private Host metadata definition already exists; refusing to adopt it.".into());
                    }
                }
                let began = Instant::now();
                let bytes = loop {
                    if began.elapsed() > Duration::from_secs(35) {
                        return Err(format!(
                            "Host metadata discovery timed out; inspect Job '{}'.",
                            record.name
                        ));
                    }
                    discovery
                        .admit(&state, grant, launch, record.team_id, &record.sysargs)
                        .await
                        .map_err(|e| e.detail_text())?;
                    let body = context::scope(None, backend.try_get(&record.remote_name))
                        .await?
                        .ok_or("The canonical Host metadata definition is unavailable.")?;
                    if let Some((success, message)) = super::completed_result(&body) {
                        if !success {
                            return Err(format!(
                                "Credential-free Host discovery failed: {}",
                                message.chars().take(2048).collect::<String>()
                            ));
                        }
                        if message.len() > plugins::MAX_CATALOG_BYTES {
                            return Err(
                                "The Host metadata result exceeds its decoded bound.".into()
                            );
                        }
                        break message.as_bytes().to_vec();
                    }
                    tokio::time::sleep(Duration::from_millis(75)).await;
                };
                let root = self.root.clone();
                let name = record.name.clone();
                let current = tokio::task::spawn_blocking(move || Catalog::new(&root).get(&name))
                    .await
                    .map_err(|_| "The canonical Host metadata identity is unavailable.")??
                    .ok_or("The canonical Host metadata identity was removed.")?;
                if current.remote_name != record.remote_name
                    || current.placement != JobPlacement::HostApi
                    || current.profile != environment::Profile::Metadata
                    || current.discovery != record.discovery
                    || current.callback != record.callback
                    || current.context_revision != record.context_revision
                {
                    return Err(
                        "The canonical Host metadata identity changed before cache commit.".into(),
                    );
                }
                let identity = discovery
                    .admit(&state, grant, launch, current.team_id, &current.sysargs)
                    .await
                    .map_err(|e| e.detail_text())?;
                self.confirm_host_discovery_sources(&discovery, &identity.caller())
                    .await?;
                // This is host-selected context state, not an environment variable or producer claim.
                let attestation_jobs = jobs.clone();
                let attestation_record = current.clone();
                let attestation_digest = digest.clone();
                tokio::task::spawn_blocking(move || {
                    attestation_jobs.attest_discovery(&attestation_record, &attestation_digest)
                })
                .await
                .map_err(|_| "The sealed Host metadata attestation was interrupted.")??;
                let identity = discovery
                    .admit(&state, grant, launch, current.team_id, &current.sysargs)
                    .await
                    .map_err(|e| e.detail_text())?;
                self.confirm_host_discovery_sources(&discovery, &identity.caller())
                    .await?;
                discovery
                    .admit(&state, grant, launch, current.team_id, &current.sysargs)
                    .await
                    .map_err(|e| e.detail_text())?;
                let complete = discovery
                    .accept(current.profile, &digest, &bytes, &reserved)
                    .map_err(|e| e.detail_text())?;
                let cached = Cached {
                    producer_schema: plugins::PRODUCER_SCHEMA,
                    profile: current.profile,
                    signed_source_sha256: digest.clone(),
                    compiler_sha256: compiler_sha256.clone(),
                    catalog: complete.catalog().clone(),
                };
                tokio::task::spawn_blocking(move || {
                    meerschaum_core::team_scope::create_private_dir(
                        cache_path
                            .parent()
                            .ok_or("Invalid Host metadata cache path.")?,
                    )
                    .map_err(|_| "The private Host metadata cache is unavailable.")?;
                    meerschaum_core::team_scope::write_private(
                        &cache_path,
                        &serde_json::to_string(&cached)
                            .map_err(|_| "Invalid Host metadata cache encoding.")?,
                    )
                    .map_err(|_| "Could not commit verified Host metadata.")
                })
                .await
                .map_err(|_| "Host metadata cache commit was interrupted.")??;
                // Disk publication and attestation are awaited host work too. Never return
                // a stale view merely because its immutable, credential-free data is cached.
                let identity = discovery
                    .admit(&state, grant, launch, current.team_id, &current.sysargs)
                    .await
                    .map_err(|e| e.detail_text())?;
                self.confirm_host_discovery_sources(&discovery, &identity.caller())
                    .await?;
                Ok(complete)
            }
            .await;
            if let Err(message) = &mut operation
                && callbacks::load_identity(&state, grant, launch)
                    .await
                    .is_ok()
                && let Some(logs) = context::scope(None, backend.logs(&record.remote_name)).await
            {
                let logs = crate::executor::jobs::LogSanitizer::default().push(&logs);
                let logs = meerschaum_server::redact::mask_uri_passwords(&logs);
                let tail = logs
                    .chars()
                    .rev()
                    .take(4096)
                    .collect::<String>()
                    .chars()
                    .rev()
                    .collect::<String>();
                if !tail.is_empty() {
                    message.push_str(&format!(
                        "\nCanonical Host metadata job '{}':\n{tail}",
                        record.name
                    ));
                }
            }
            let cleanup = async {
                jobs.revoke(launch, record.context_revision).await?;
                if !cancellation_state.definition_owned.load(Ordering::Acquire) {
                    return Err("This operation did not create the private Host metadata definition; its canonical identity is retained for recovery.".into());
                }
                let result = context::scope(None, backend.delete(&record.remote_name)).await;
                if result.success {
                    Ok::<_, String>(())
                } else {
                    Err(result.message)
                }
            }
            .await;
            if cleanup.is_ok() {
                let root = self.root.clone();
                let record = record.clone();
                let _ =
                    tokio::task::spawn_blocking(move || Catalog::new(&root).remove(&record)).await;
            }
            if operation.is_ok() {
                let fresh = async {
                    let identity = discovery
                        .admit(&state, grant, launch, record.team_id, &record.sysargs)
                        .await
                        .map_err(|e| e.detail_text())?;
                    self.confirm_host_discovery_sources(&discovery, &identity.caller())
                        .await?;
                    discovery
                        .admit(&state, grant, launch, record.team_id, &record.sysargs)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.detail_text())
                }
                .await;
                if let Err(error) = fresh {
                    operation = Err(error);
                }
            }
            self.discard_job_callback(grant).await;
            cancellation.record = None;
            operation
        })
    }

    async fn capture_host_catalog(
        &self,
        team_id: Option<Uuid>,
        grants: BTreeSet<String>,
        caller: &Access,
    ) -> Result<plugins::Validated, String> {
        let root = self.root.clone();
        let directories = self.config()?.paths().plugins_dirs.clone();
        let caller = caller.clone();
        tokio::task::spawn_blocking(move || {
            if grants.len() > plugins::MAX_PLUGINS
                || grants.iter().any(|name| !plugins::valid_name(name))
            {
                return Err("Invalid Host source grant set.".into());
            }
            let (mut sources, available) =
                crate::executor::host_jobs::plugin_sources(&caller, &directories)?;
            if !grants.is_subset(&available) {
                return Err("An exact Host metadata source is unavailable or unauthorized.".into());
            }
            sources.retain(|(filename, _)| {
                filename.to_str().is_some_and(|filename| {
                    grants.contains(
                        filename
                            .strip_suffix(".py")
                            .or_else(|| filename.strip_suffix(".rs"))
                            .unwrap_or(filename),
                    )
                })
            });
            let snapshot =
                crate::executor::plugin_files::Store::new(&root)?.snapshot(&sources, None)?;
            let host = HostIdentity {
                team_id,
                generation: host_generation(&root)?,
                files: snapshot.hashes,
                view_sha256: snapshot.view_sha256,
            };
            let plugins = grants
                .iter()
                .map(|name| {
                    Ok((
                        name.clone(),
                        plugins::Plugin {
                            archive_sha256: String::new(),
                            kind: host.source_kind(name)?,
                            metadata: None,
                        },
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, String>>()?;
            let catalog = plugins::Catalog {
                schema: plugins::SCHEMA,
                release: env!("CARGO_PKG_VERSION").into(),
                team_id: team_id.unwrap_or_else(Uuid::nil),
                generation: host.generation,
                placement: Placement::Host(host.clone()),
                plugins,
            };
            plugins::Validated::decode_host(
                &serde_json::to_vec(&catalog).map_err(|_| "Invalid Host source catalog.")?,
                &host,
                &grants,
                &reserved_actions(),
            )
            .map_err(|e| e.to_string())
        })
        .await
        .map_err(|_| "Host source snapshot preparation was interrupted.".to_string())?
    }

    pub(super) async fn confirm_host_discovery_sources(
        &self,
        discovery: &Discovery,
        caller: &Access,
    ) -> Result<(), String> {
        let Placement::Host(host) = &discovery.catalog.placement else {
            return Err("This discovery has no Host source identity.".into());
        };
        let current = self
            .capture_host_catalog(
                host.team_id,
                discovery.catalog.plugins.keys().cloned().collect(),
                caller,
            )
            .await?;
        if current.catalog() != &discovery.catalog {
            return Err(
                "The Host source paths, bytes or source authority changed before metadata use."
                    .into(),
            );
        }
        Ok(())
    }
}

/// Cancellation schedules exact source retirement without borrowing the canceled request.
/// A lost runtime cannot run this task; the immutable native deadline remains the backstop.
struct Cancellation {
    controller: Controller,
    record: Option<Record>,
    state: Arc<CancellationState>,
}
#[derive(Default)]
struct CancellationState {
    canceled: AtomicBool,
    delivery_started: AtomicBool,
    definition_owned: AtomicBool,
}
impl Drop for Cancellation {
    fn drop(&mut self) {
        let Some(record) = self.record.take() else {
            return;
        };
        self.state.canceled.store(true, Ordering::Release);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let state = self.state.clone();
        let controller = self.controller.clone();
        runtime.spawn(async move {
            // This callback was captured before cache I/O or reservation. Its authority must
            // end even if cancellation happened before any canonical catalog row existed.
            if let Some(callback) = record.callback {
                controller.discard_job_callback(callback).await;
            }
            let Ok(_lease) = controller.job_lock(&record).await else {
                return;
            };
            let root = controller.root.clone();
            let name = record.name.clone();
            let current = tokio::task::spawn_blocking(move || Catalog::new(&root).get(&name)).await;
            let Ok(Ok(Some(current))) = current else {
                return;
            };
            if current.remote_name == record.remote_name
                && current.callback == record.callback
                && current.context_revision == record.context_revision
                && current.discovery == record.discovery
                && current.profile == environment::Profile::Metadata
                && current.placement == JobPlacement::HostApi
            {
                let Ok(jobs) = controller.host_jobs() else {
                    return;
                };
                let Ok(launch) = canonical_id(&current) else {
                    return;
                };
                let retired = jobs.revoke(launch, current.context_revision).await.is_ok();
                // No public start can run a typed Metadata operation. Before our delivery
                // phase, genuine definition absence also proves this owned reservation did
                // not start; an uncertain post-delivery generation is always retained.
                let never_started = if state.delivery_started.load(Ordering::Acquire) {
                    false
                } else {
                    let jobs = jobs.clone();
                    let name = current.remote_name.clone();
                    matches!(
                        tokio::task::spawn_blocking(move || jobs.definition_exists(&name)).await,
                        Ok(Ok(false))
                    )
                };
                if never_started {
                    let root = controller.root.clone();
                    let _ =
                        tokio::task::spawn_blocking(move || Catalog::new(&root).remove(&current))
                            .await;
                } else if retired && state.definition_owned.load(Ordering::Acquire) {
                    let deleted =
                        context::scope(None, jobs.backend().delete(&current.remote_name)).await;
                    if deleted.success {
                        let root = controller.root.clone();
                        let _ = tokio::task::spawn_blocking(move || {
                            Catalog::new(&root).remove(&current)
                        })
                        .await;
                    }
                }
            }
        });
    }
}

fn canonical_id(record: &Record) -> Result<Uuid, String> {
    record
        .remote_name
        .strip_prefix("mrsm-")
        .and_then(|id| Uuid::parse_str(id).ok())
        .filter(|id| !id.is_nil() && record.remote_name == format!("mrsm-{}", id.simple()))
        .ok_or_else(|| "The Host metadata job has no canonical private identity.".into())
}
fn reserved_actions() -> BTreeSet<String> {
    crate::actions::CORE_ACTIONS
        .iter()
        .map(|action| action.name.to_string())
        .collect()
}
fn read_cached(path: &Path) -> Result<Option<Cached>, String> {
    let bytes = match read_regular(path, plugins::MAX_CATALOG_BYTES + 4096) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("The private Host metadata cache is unsafe or unavailable.".into()),
        Ok(bytes) => bytes,
    };
    let cached: Cached =
        serde_json::from_slice(&bytes).map_err(|_| "Invalid private Host metadata cache.")?;
    Ok((cached.producer_schema == plugins::PRODUCER_SCHEMA).then_some(cached))
}
fn host_generation(root: &Path) -> Result<Uuid, String> {
    use std::io::{Read, Seek, Write};
    #[cfg(not(unix))]
    return Err("Host metadata requires the supported native platform.".into());
    #[cfg(unix)]
    {
        let mut file = std::fs::File::from(
            rustix::fs::open(
                root.join("executor-host-identity"),
                rustix::fs::OFlags::RDWR
                    | rustix::fs::OFlags::CREATE
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::NONBLOCK
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
            )
            .map_err(|_| "The Main Host generation is unsafe or unavailable.")?,
        );
        file.try_lock()
            .map_err(|_| "The Main Host generation is busy or unavailable.")?;
        let metadata = file
            .metadata()
            .map_err(|_| "The Main Host generation is unavailable.")?;
        if !metadata.is_file() || metadata.len() > 64 {
            return Err("Invalid Main Host generation file.".into());
        }
        if metadata.len() == 0 {
            let identity = Uuid::new_v4();
            file.write_all(identity.to_string().as_bytes())
                .and_then(|()| file.sync_all())
                .map_err(|_| "Could not persist the Main Host generation.")?;
            file.rewind()
                .map_err(|_| "The Main Host generation is unavailable.")?;
        }
        let mut bytes = Vec::new();
        file.take(65)
            .read_to_end(&mut bytes)
            .map_err(|_| "The Main Host generation is unreadable.")?;
        if bytes.len() > 64 {
            return Err("Invalid Main Host generation.".into());
        }
        std::str::from_utf8(&bytes)
            .ok()
            .and_then(|text| Uuid::parse_str(text).ok())
            .filter(|id| !id.is_nil())
            .ok_or_else(|| "Invalid Main Host generation.".into())
    }
}

fn read_regular(path: &Path, limit: usize) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    #[cfg(unix)]
    let file = std::fs::File::from(rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?);
    #[cfg(not(unix))]
    return Err(std::io::Error::other("Unsupported Host metadata platform."));
    #[cfg(unix)]
    {
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > limit as u64 {
            return Err(std::io::Error::other("Invalid bounded Host metadata file."));
        }
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(std::io::Error::other("Oversized Host metadata file."));
        }
        Ok(bytes)
    }
}
