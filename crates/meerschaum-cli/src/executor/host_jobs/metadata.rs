//! Host credential-free preparation and cache attestation, under Main's canonical source lease.
//! Kept separate from ordinary API alias projection: no data labels or callback transport enter
//! a Metadata launch, even though both profiles use the same immutable file-source objects.

use super::*;
use meerschaum_server::executor::{plugin_discovery::Discovery, plugin_host::Placement, plugins};

pub(super) fn update(
    jobs: &HostJobs,
    state: &AppState,
    caller: &Access,
    config: &Config,
    trusted: &[(String, String)],
    update: Update,
    discovery: &Discovery,
) -> Result<(), String> {
    if update.profile != Profile::Metadata
        || !matches!(update.target, Target::Job { .. })
        || update.registration.is_some()
        || update.extension.is_some()
        || update.metadata_deadline != Some(discovery.deadline)
        || update.credential.token != meerschaum_server::executor::environment::METADATA_TOKEN
        || update.callback_url != meerschaum_server::executor::environment::METADATA_ORIGIN
        || update.discovery_catalog_sha256.as_deref()
            != Some(
                discovery
                    .catalog_sha256()
                    .map_err(|e| e.detail_text())?
                    .as_str(),
            )
    {
        return Err(
            "The Host metadata launch lost its exact credential-free canonical operation.".into(),
        );
    }
    let Placement::Host(host) = &discovery.catalog.placement else {
        return Err("A Host metadata launch cannot select remote archives.".into());
    };
    let grants = discovery
        .catalog
        .plugins
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if discovery
        .catalog
        .plugins
        .values()
        .any(|source| source.metadata.is_some())
    {
        return Err("A metadata launch requires the exact source-only catalog.".into());
    }
    let catalog = plugins::Validated::decode_host(
        &serde_json::to_vec(&discovery.catalog).map_err(|_| "Invalid Host metadata catalog.")?,
        host,
        &grants,
        &BTreeSet::new(),
    )
    .map_err(|e| e.to_string())?;
    let (mut sources, available) = plugin_sources(caller, &config.paths().plugins_dirs)?;
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
    let ceiling =
        meerschaum_server::executor::source_ceiling::Ceiling::from_catalog(catalog.catalog())?;
    let snapshot = crate::executor::plugin_files::Store::new(&state.config.root_dir)?
        .snapshot(&sources, Some(&ceiling))?;
    if snapshot.hashes != host.files || snapshot.view_sha256 != host.view_sha256 {
        return Err("The Host metadata source paths or copied bytes changed.".into());
    }
    // This map deliberately contains no meerschaum.connectors, instance labels, caller
    // registration input, extension invocation or reusable callback authentication.
    let base = Map::from_iter([(
        plugins::CONFIG_KEY.into(),
        Value::from_json_str(
            &String::from_utf8(catalog.encode().map_err(|e| e.to_string())?)
                .map_err(|_| "Invalid Host metadata encoding.")?,
        )
        .map_err(|_| "Invalid Host metadata encoding.")?,
    )]);
    let mut environment = trusted.to_vec();
    environment
        .retain(|(key, _)| !matches!(key.as_str(), "MRSM_PLUGINS_DIR" | "MRSM_EXECUTOR_PLUGINS"));
    environment.push((
        "MRSM_EXECUTOR_PLUGINS".into(),
        serde_json::to_string(&grants).map_err(|_| "Invalid Host metadata source grants.")?,
    ));
    jobs.contexts.update_host(
        update,
        HostInputs {
            base,
            environment,
            sources: snapshot.sources,
        },
    )
}

pub(super) fn attest(
    jobs: &HostJobs,
    record: &crate::executor_jobs::Record,
    digest: &str,
) -> Result<(), String> {
    if record.placement != crate::executor_jobs::JobPlacement::HostApi
        || record.profile != Profile::Metadata
    {
        return Err(
            "Only Main's exact Host Metadata operation may publish shared metadata.".into(),
        );
    }
    let discovery = record
        .discovery
        .as_ref()
        .ok_or("The Host metadata operation is missing.")?;
    if !matches!(discovery.catalog.placement, Placement::Host(_))
        || discovery.catalog_sha256().map_err(|e| e.detail_text())? != digest
    {
        return Err("The Host metadata source identity changed.".into());
    }
    let id = record
        .remote_name
        .strip_prefix("mrsm-")
        .and_then(|id| Uuid::parse_str(id).ok())
        .filter(|id| !id.is_nil() && record.remote_name == format!("mrsm-{}", id.simple()))
        .ok_or("The Host metadata job has no canonical launch identity.")?;
    jobs.contexts.attest_metadata(
        id,
        record.context_revision,
        &record.remote_name,
        digest,
        discovery.deadline,
    )
}
