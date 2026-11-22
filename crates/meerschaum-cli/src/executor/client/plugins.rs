//! Main-created metadata jobs. Source proofs and archive capabilities never enter discovery.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::gateway::{CONTEXTS_PATH, PLUGIN_DELIVERY_PATH};
use meerschaum_server::executor::plugin_discovery::{Discovery, Origin};
use meerschaum_server::executor::{callbacks, context, environment, plugin_delivery, plugins};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::Controller;
use crate::executor::jobs::{Catalog, Record};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cached {
    #[serde(default)]
    producer_schema: u16,
    profile: environment::Profile,
    signed_source_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compiler_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    factory_receipt: Option<super::factory_metadata::Receipt>,
    catalog: plugins::Catalog,
}

impl Controller {
    /// Resolve metadata for an exact source ceiling under the originating Main operation.
    /// Global Team configuration and another caller's live view are never replaced.
    pub fn discover_plugin_catalog(
        &self,
        team_id: Uuid,
        grants: BTreeSet<String>,
    ) -> std::pin::Pin<
        Box<impl std::future::Future<Output = Result<plugins::Validated, String>> + Send + '_>,
    > {
        Box::pin(async move {
            let state = self.state()?;
            let caller = context::current()
                .filter(|caller| !caller.no_auth && caller.auth.is_some())
                .ok_or("Plugin discovery requires its authenticated original Main operation.")?;
            let (registered, _) =
                crate::executor::registry::Registry::new(&self.root).authority(team_id)?;
            let generation = registered.verifier.generation;
            let archives = state.config.plugins_archives_path.clone();
            let selected = grants.clone();
            let catalog = tokio::task::spawn_blocking(move || {
                source_catalog(&archives, team_id, generation, &selected)
            })
            .await
            .map_err(|_| "Plugin source selection was interrupted.")??;
            let discovery = Discovery {
                catalog,
                origin: Origin::capture(),
                deadline: chrono::Utc::now().timestamp() + 60,
            };
            let pin = super::factory_metadata::pin(&self.root, &discovery.catalog)?;
            let pin_sha256 = pin
                .as_ref()
                .map(meerschaum_server::executor::factory_attestation::quote::pin_digest)
                .transpose()?;
            let compiler_sha256 = metadata_compiler(
                &discovery.catalog,
                pin.as_ref()
                    .and_then(|pin| pin.artifacts.compiler_sha256.clone()),
            )?;
            let digest = discovery
                .catalog_sha256()
                .map_err(|error| error.detail_text())?;
            let mut properties = Map::new();
            let owner = context::actor(&caller).owner;
            let teams = self
                .instance
                .get_teams()
                .await
                .map_err(|_| "The discovery Team is unavailable.")?;
            let team = teams
                .iter()
                .find(|team| {
                    team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) == Some(team_id)
                })
                .ok_or("The discovery Team was deleted.")?;
            meerschaum_core::job_access::JobAccess::stamp(
                &mut properties,
                Some(&owner),
                Some(&team.name),
            );
            properties.insert(
                "logs".into(),
                Value::Map(Map::from_iter([
                    ("write_timestamps".into(), false.into()),
                    ("stdin".into(), false.into()),
                ])),
            );
            properties.insert("mrsm_execution".into(), Value::from_json_str(r#"{"kind":"temporary_execution","origin":"plugin_metadata","retention":{"success_seconds":60,"failure_seconds":300,"summary_seconds":300}}"#).map_err(|_|"Invalid discovery tracking metadata.")?);
            let mut record = Record::new(
                &format!("metadata-{}", Uuid::new_v4().simple()),
                team_id,
                discovery.sysargs(),
                properties,
            );
            record.profile = environment::Profile::Metadata;
            record.discovery = Some(discovery.clone());
            record.callback = self.register_job_callback(&mut record, None).await?;
            let grant = record
                .callback
                .ok_or("Discovery could not bind its original source.")?;
            let launch = canonical_id(&record)?;
            let reserved = reserved_actions();
            let cache_path = metadata_cache_path(
                &self.root,
                &digest,
                compiler_sha256.as_deref(),
                pin_sha256.as_deref(),
            );
            let read_path = cache_path.clone();
            let cached = tokio::task::spawn_blocking(move || read_cached(&read_path))
                .await
                .map_err(|_| "Plugin metadata cache lookup was interrupted.")?;
            let cached = match cached {
                Ok(cached) => cached,
                Err(error) => {
                    self.discard_job_callback(grant).await;
                    return Err(error);
                }
            };
            if let Some(cached) = cached {
                let result = async {
                    if cached.producer_schema != plugins::PRODUCER_SCHEMA {
                        return Err("The cached metadata uses another producer schema.".into());
                    }
                    if cached.compiler_sha256 != compiler_sha256
                        || !super::factory_metadata::matches(
                            cached.factory_receipt.as_ref(),
                            pin_sha256.as_deref(),
                        )
                    {
                        return Err("The cached metadata does not match the executor's current compiler installation.".into());
                    }
                    discovery
                        .admit(&state, grant, launch, team_id, &record.sysargs)
                        .await
                        .map_err(|error| error.detail_text())?;
                    self.confirm_discovery_sources(&discovery).await?;
                    if super::factory_metadata::pin(&self.root, &discovery.catalog)? != pin {
                        return Err("The executor's compiler installation changed during discovery.".into());
                    }
                    discovery
                        .accept(
                            cached.profile,
                            &cached.signed_source_sha256,
                            &serde_json::to_vec(&cached.catalog)
                                .map_err(|_| "Invalid cached plugin metadata.")?,
                            &reserved,
                        )
                        .map_err(|error| error.detail_text())
                }
                .await;
                self.discard_job_callback(grant).await;
                return result;
            }
            let root = self.root.clone();
            let reservation = record.clone();
            let reserved_name =
                tokio::task::spawn_blocking(move || Catalog::new(&root).reserve(reservation))
                    .await
                    .map_err(|_| "Discovery reservation was interrupted.")?;
            if !matches!(reserved_name, Ok(true)) {
                self.discard_job_callback(grant).await;
                return Err("The canonical discovery could not be reserved.".into());
            }
            let _lease = self.job_lock(&record).await?;
            let mut operation = Box::pin(async {
                let identity = discovery
                    .admit(&state, grant, launch, team_id, &record.sysargs)
                    .await
                    .map_err(|error| error.detail_text())?;
                let prepared =
                    context::scope(Some(Arc::new(identity.caller())), self.prepare_id_bound(team_id))
                        .await?;
                let api = &prepared.api;
                let mut credentials = BTreeMap::new();
                for (name, source) in &discovery.catalog.plugins {
                    credentials.insert(
                        name.clone(),
                        plugin_delivery::issue(
                            &state,
                            grant,
                            launch,
                            team_id,
                            generation,
                            name,
                            &source.archive_sha256,
                        )
                        .await
                        .map_err(|error| error.detail_text())?,
                    );
                }
                let delivery = plugin_delivery::Delivery {
                    catalog: discovery.catalog.clone(),
                    repository_origin: self.callback_url(team_id).await?,
                    credentials,
                };
                let response = api
                    .make_request(
                        Method::Post,
                        PLUGIN_DELIVERY_PATH,
                        RequestOptions {
                            body: Body::Json(
                                serde_json::to_string(&delivery)
                                    .map_err(|_| "Invalid signed plugin delivery.")?,
                            ),
                            timeout: Some(Duration::from_secs(20)),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|_| "The exact plugin sources could not be delivered.")?;
                if response.status != 200
                    || response
                        .json()
                        .ok()
                        .and_then(|body| body["catalog_sha256"].as_str().map(str::to_string))
                        != Some(digest.clone())
                {
                    return Err("The executor did not stage the exact discovery sources.".into());
                }
                self.deliver_job_context(&mut record, false).await?;
                let mut properties = record.properties.clone();
                properties.insert(
                    environment::JOB_CONTEXT_PROPERTY.into(),
                    launch.to_string().into(),
                );
                let created = api
                    .create_job(&record.remote_name, &record.sysargs, Some(properties))
                    .await;
                if !created.success {
                    return Err(created.message);
                }
                let began = Instant::now();
                let bytes = loop {
                    if began.elapsed() > Duration::from_secs(35) {
                        return Err(format!(
                            "Metadata discovery timed out; inspect Job '{}'.",
                            record.name
                        ));
                    }
                    discovery
                        .admit(&state, grant, launch, team_id, &record.sysargs)
                        .await
                        .map_err(|error| error.detail_text())?;
                    let response = api
                        .make_request(
                            Method::Get,
                            &format!("/jobs/{}", record.remote_name),
                            RequestOptions {
                                timeout: Some(Duration::from_secs(4)),
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(|_| "The canonical discovery result is unavailable.")?;
                    if response.status != 200 {
                        return Err("The canonical discovery result is unavailable.".into());
                    }
                    let body = response
                        .json()
                        .map_err(|_| "The executor returned invalid discovery metadata.")?;
                    // The outward result can predate the committed properties snapshot.
                    // Completion and its result must come from the same finished snapshot.
                    if let Some((success, message)) = super::completed_result(&body) {
                        if !success {
                            return Err(format!(
                                "Credential-free discovery failed: {}",
                                message.chars().take(2048).collect::<String>()
                            ));
                        }
                        if message.len() > plugins::MAX_CATALOG_BYTES {
                            return Err("The metadata result exceeds its limit.".into());
                        }
                        break message.as_bytes().to_vec();
                    }
                    tokio::time::sleep(Duration::from_millis(75)).await;
                };
                let root = self.root.clone();
                let name = record.name.clone();
                let current = tokio::task::spawn_blocking(move || Catalog::new(&root).get(&name))
                    .await
                    .map_err(|_| "The canonical discovery identity is unavailable.")??
                    .ok_or("The canonical discovery was removed.")?;
                if current.remote_name != record.remote_name
                    || current.profile != environment::Profile::Metadata
                    || current.discovery != record.discovery
                    || current.callback != record.callback
                    || current.context_revision != record.context_revision
                {
                    return Err(
                        "The canonical discovery identity changed before metadata commit.".into(),
                    );
                }
                let identity = discovery
                    .admit(&state, grant, launch, team_id, &current.sysargs)
                    .await
                    .map_err(|error| error.detail_text())?;
                let factory_receipt = if let Some(pin) = &pin {
                    let live = context::scope(
                        Some(Arc::new(identity.caller())),
                        self.prepare_id_bound(team_id),
                    ).await?;
                    if live.actor != prepared.actor
                        || live.control != prepared.control
                        || live.configuration_sha256 != prepared.configuration_sha256
                    {
                        return Err("The executor configuration or metadata job owner changed during discovery.".into());
                    }
                    Some(live.verify_metadata(
                        pin, launch, current.context_revision, &digest, discovery.deadline, &bytes,
                    ).await?)
                } else {
                    None
                };
                self.confirm_discovery_sources(&discovery).await?;
                discovery
                    .admit(&state, grant, launch, team_id, &current.sysargs)
                    .await
                    .map_err(|error| error.detail_text())?;
                if super::factory_metadata::pin(&self.root, &discovery.catalog)? != pin {
                    return Err("The executor's compiler installation changed during discovery.".into());
                }
                let complete = discovery
                    .accept(current.profile, &digest, &bytes, &reserved)
                    .map_err(|error| error.detail_text())?;
                let cached = Cached {
                    producer_schema: plugins::PRODUCER_SCHEMA,
                    profile: current.profile,
                    signed_source_sha256: digest.clone(),
                    compiler_sha256: compiler_sha256.clone(),
                    factory_receipt,
                    catalog: complete.catalog().clone(),
                };
                tokio::task::spawn_blocking(move || {
                    meerschaum_core::team_scope::create_private_dir(
                        cache_path
                            .parent()
                            .ok_or("Invalid private metadata cache.")?,
                    )
                    .map_err(|_| "The private metadata cache is unavailable.")?;
                    meerschaum_core::team_scope::write_private(
                        &cache_path,
                        &serde_json::to_string(&cached)
                            .map_err(|_| "Invalid metadata cache encoding.")?,
                    )
                    .map_err(|_| "Could not commit verified credential-free metadata.")
                })
                .await
                .map_err(|_| "Metadata cache commit was interrupted.")??;
                Ok(complete)
            })
            .await;
            // Retain bounded, sanitized evidence before exact retirement removes these private
            // temporary job files. Only this still-fresh original identity may read its logs.
            if let Err(message) = &mut operation
                && let Ok(identity) = callbacks::load_identity(&state, grant, launch).await
                && let Ok(api) =
                    context::scope(Some(Arc::new(identity.caller())), self.prepare_id(team_id))
                        .await
                && let Ok(logs) = api.get_logs(&record.remote_name).await
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
                        "\nCanonical metadata job '{}':\n{tail}",
                        record.name
                    ));
                }
            }
            // Keep an uncertain stop in the canonical catalog for ordinary exact-source maintenance.
            // Its immutable discovery deadline prevents an abandoned request from renewing forever.
            let cleanup: Result<(), String> = async {
                let identity = callbacks::load_identity(&state, grant, launch)
                    .await
                    .map_err(|_| "Discovery source is inactive.")?;
                let api =
                    context::scope(Some(Arc::new(identity.caller())), self.prepare_id(team_id))
                        .await?;
                let response = api
                    .make_request(
                        Method::Delete,
                        &format!("{CONTEXTS_PATH}{launch}"),
                        RequestOptions {
                            body: Body::Json(
                                serde_json::json!({"revision":record.context_revision}).to_string(),
                            ),
                            timeout: Some(Duration::from_secs(5)),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|_| "Discovery retirement is uncertain.")?;
                if response.status != 200 {
                    return Err("Discovery retirement is uncertain.".into());
                }
                if !api.delete_job(&record.remote_name).await.success {
                    return Err("Discovery definition retirement is uncertain.".into());
                }
                Ok(())
            }
            .await;
            self.discard_job_callback(grant).await;
            if cleanup.is_ok() {
                let root = self.root.clone();
                let record = record.clone();
                let _ =
                    tokio::task::spawn_blocking(move || Catalog::new(&root).remove(&record)).await;
            }
            operation
        })
    }

    async fn confirm_discovery_sources(&self, discovery: &Discovery) -> Result<(), String> {
        let path = self.state()?.config.plugins_archives_path.clone();
        let expected = discovery.catalog.clone();
        tokio::task::spawn_blocking(move || {
            let names = expected.plugins.keys().cloned().collect();
            let current = source_catalog(&path, expected.team_id, expected.generation, &names)?;
            if current != expected {
                return Err("Plugin sources changed before metadata commit.".into());
            }
            Ok(())
        })
        .await
        .map_err(|_| "Final source verification was interrupted.".to_string())?
    }
}

fn canonical_id(record: &Record) -> Result<Uuid, String> {
    record
        .remote_name
        .strip_prefix("mrsm-")
        .and_then(|name| Uuid::parse_str(name).ok())
        .filter(|id| !id.is_nil())
        .ok_or_else(|| "The metadata operation has no canonical identity.".into())
}
fn reserved_actions() -> BTreeSet<String> {
    crate::actions::CORE_ACTIONS
        .iter()
        .map(|action| action.name.to_string())
        .collect()
}
fn read_regular(path: &Path, limit: usize) -> Result<Vec<u8>, String> {
    #[cfg(unix)]
    let file = std::fs::File::from(
        rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| "The exact plugin source or metadata is unavailable.")?,
    );
    #[cfg(not(unix))]
    return Err("Credential-free discovery requires a supported native platform.".into());
    #[cfg(unix)]
    {
        let metadata = file
            .metadata()
            .map_err(|_| "The exact plugin source is unavailable.")?;
        if !metadata.is_file() || metadata.len() > limit as u64 {
            return Err("The plugin source or metadata exceeds its bound.".into());
        }
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "The plugin source is unreadable.")?;
        if bytes.len() > limit {
            return Err("The plugin source exceeds its bound.".into());
        }
        Ok(bytes)
    }
}
pub(super) fn metadata_cache_path(
    root: &Path,
    source_digest: &str,
    compiler: Option<&str>,
    pin_sha256: Option<&str>,
) -> std::path::PathBuf {
    let compiler = compiler
        .map(|digest| format!("-compiler-{digest}"))
        .unwrap_or_default();
    let factory = pin_sha256
        .map(|digest| format!("-factory-{digest}"))
        .unwrap_or_default();
    root.join("executor-plugin-metadata").join(format!(
        "v{}-{source_digest}{compiler}{factory}.json",
        plugins::PRODUCER_SCHEMA
    ))
}

/// The compiler identity comes from an immutable native host constructor, independently of
/// source hashes and child-produced declarations. Packaged/native-remote adapters remain
/// unavailable until their own exact compilation and attestation contracts are implemented.
pub(super) fn metadata_compiler(
    catalog: &plugins::Catalog,
    trusted: Option<String>,
) -> Result<Option<String>, String> {
    if trusted
        .as_ref()
        .is_some_and(|identity| !plugins::valid_digest(identity))
    {
        return Err("The host-selected compiler identity is invalid.".into());
    }
    if catalog
        .plugins
        .values()
        .any(|plugin| plugin.kind == plugins::SourceKind::NativePackage)
    {
        return Err(
            "Confined native package metadata requires a supported pinned compiler adapter.".into(),
        );
    }
    if !catalog
        .plugins
        .values()
        .any(|plugin| plugin.kind == plugins::SourceKind::NativeSource)
    {
        return Ok(trusted);
    }
    match trusted {
        Some(identity) if plugins::valid_digest(&identity) => Ok(Some(identity)),
        _ => Err(
            "Confined Rust metadata requires its host-selected attested offline compiler.".into(),
        ),
    }
}

fn read_cached(path: &Path) -> Result<Option<Cached>, String> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("The private metadata cache is unavailable.".into()),
        Ok(_) => {}
    }
    let bytes = read_regular(path, plugins::MAX_CATALOG_BYTES + 4096)?;
    let cached: Cached =
        serde_json::from_slice(&bytes).map_err(|_| "The private metadata cache is invalid.")?;
    Ok((cached.producer_schema == plugins::PRODUCER_SCHEMA).then_some(cached))
}
pub(super) fn source_catalog(
    path: &Path,
    team_id: Uuid,
    generation: Uuid,
    names: &BTreeSet<String>,
) -> Result<plugins::Catalog, String> {
    if names.len() > plugins::MAX_PLUGINS || names.iter().any(|name| !plugins::valid_name(name)) {
        return Err("Invalid discovery source ceiling.".into());
    }
    let mut sources = BTreeMap::new();
    for name in names {
        let bytes = read_regular(
            &path.join(format!("{name}.tar.gz")),
            crate::executor::plugins::MAX_ARCHIVE_BYTES,
        )?;
        sources.insert(
            name.clone(),
            plugins::Plugin {
                archive_sha256: plugins::sha256(&bytes),
                kind: source_kind(name, &bytes)?,
                metadata: None,
            },
        );
    }
    Ok(plugins::Catalog {
        placement: Default::default(),
        schema: plugins::SCHEMA,
        release: env!("CARGO_PKG_VERSION").into(),
        team_id,
        generation,
        plugins: sources,
    })
}
fn source_kind(name: &str, bytes: &[u8]) -> Result<plugins::SourceKind, String> {
    let decoder = flate2::read::GzDecoder::new(bytes).take(132 * 1024 * 1024);
    let mut archive = tar::Archive::new(decoder);
    let mut kinds = BTreeSet::new();
    for (index, item) in archive
        .entries()
        .map_err(|_| "Invalid plugin source archive.")?
        .raw(true)
        .enumerate()
    {
        if index >= 4096 {
            return Err("Too many plugin archive entries.".into());
        }
        let item = item.map_err(|_| "Invalid plugin source archive.")?;
        let path = item.path().map_err(|_| "Invalid plugin entry point.")?;
        if path == Path::new(&format!("{name}.py")) {
            kinds.insert(0);
        }
        if path == Path::new(&format!("{name}/__init__.py")) {
            kinds.insert(1);
        }
        if path == Path::new(&format!("{name}.rs")) {
            kinds.insert(2);
        }
        if path == Path::new(&format!("{name}/plugin.toml")) {
            kinds.insert(3);
        }
    }
    if kinds.len() != 1 {
        return Err("The exact plugin archive has no unambiguous supported entry point.".into());
    }
    match kinds.into_iter().next().unwrap() {
        0 => Ok(plugins::SourceKind::PythonModule),
        1 => Ok(plugins::SourceKind::PythonPackage),
        2 => Ok(plugins::SourceKind::NativeSource),
        _ => Ok(plugins::SourceKind::NativePackage),
    }
}
