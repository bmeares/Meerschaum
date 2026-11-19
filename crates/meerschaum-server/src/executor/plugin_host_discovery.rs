//! Host-specific admission for the same Main-created credential-free Discovery operation.
//! This module is a child of plugin_discovery; it never mints a bearer for the runtime.

use super::*;
use crate::executor::plugin_host::Placement;

pub(super) fn validated(discovery: &Discovery) -> Result<plugins::Validated, ApiError> {
    let Placement::Host(host) = &discovery.catalog.placement else {
        return Err(refused());
    };
    if discovery.deadline <= chrono::Utc::now().timestamp()
        || discovery
            .catalog
            .plugins
            .values()
            .any(|plugin| plugin.metadata.is_some())
    {
        return Err(refused());
    }
    plugins::Validated::decode_host(
        &serde_json::to_vec(&discovery.catalog).map_err(|_| refused())?,
        host,
        &discovery.catalog.plugins.keys().cloned().collect(),
        &BTreeSet::new(),
    )
    .map_err(|_| refused())
}

pub(super) async fn admit(
    discovery: &Discovery,
    state: &AppState,
    grant_id: Uuid,
    launch_id: Uuid,
    team_id: Uuid,
    sysargs: &[String],
) -> Result<callbacks::Identity, ApiError> {
    validated(discovery)?;
    let Placement::Host(host) = &discovery.catalog.placement else {
        return Err(refused());
    };
    if sysargs != discovery.sysargs()
        || team_id != host.team_id.unwrap_or_else(Uuid::nil)
        || read_generation(&state.config.root_dir)? != host.generation
    {
        return Err(refused());
    }
    let identity = callbacks::load_admission_identity(state, grant_id, launch_id).await?;
    let ceiling = super::super::source_ceiling::Ceiling::from_catalog(&discovery.catalog)
        .map_err(|_| refused())?;
    let caller = identity.caller();
    if identity.grant.team_id != host.team_id
        || identity.grant.source_ceiling.as_ref() != Some(&ceiling)
        || !identity
            .grant
            .required
            .contains(discovery.origin.required())
    {
        return Err(refused());
    }
    match (host.team_id, identity.team.as_ref()) {
        (Some(id), Some(team)) => {
            if team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) != Some(id)
                || !identity
                    .grant
                    .selection
                    .as_ref()
                    .is_some_and(|settings| settings.mode == meerschaum_core::execution::Mode::Host)
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
        }
        (None, None) => {
            if identity.grant.selection.is_some()
                || !identity.source_access().principal.permits_team(None)
            {
                return Err(refused());
            }
        }
        _ => return Err(refused()),
    }
    if !ceiling.current(&caller, identity.team.as_ref()) {
        return Err(refused());
    }
    discovery.origin.authorize(state, &caller).await?;
    Ok(identity)
}

pub(super) fn accept(
    discovery: &Discovery,
    stored_profile: environment::Profile,
    source_digest: &str,
    bytes: &[u8],
    reserved_actions: &BTreeSet<String>,
) -> Result<plugins::Validated, ApiError> {
    let seed = validated(discovery)?;
    if stored_profile != environment::Profile::Metadata
        || source_digest != seed.digest().map_err(|_| refused())?
    {
        return Err(refused());
    }
    let Placement::Host(host) = &discovery.catalog.placement else {
        return Err(refused());
    };
    let result = plugins::Validated::decode_host(
        bytes,
        host,
        &discovery.catalog.plugins.keys().cloned().collect(),
        reserved_actions,
    )
    .map_err(|_| refused())?;
    if !result.complete()
        || result.source_digest().map_err(|_| refused())?
            != seed.source_digest().map_err(|_| refused())?
    {
        return Err(refused());
    }
    let _ = result.encode().map_err(|_| refused())?;
    Ok(result)
}

/// Reading a private persisted identity does not create or recreate host authority.
/// Only the Main driver initializes it; removal or replacement invalidates old operations.
pub(super) fn read_generation(root: &std::path::Path) -> Result<Uuid, ApiError> {
    use std::io::Read;
    #[cfg(unix)]
    let file = std::fs::File::from(
        rustix::fs::open(
            root.join("executor-host-identity"),
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| refused())?,
    );
    #[cfg(not(unix))]
    return Err(refused());
    #[cfg(unix)]
    {
        let metadata = file.metadata().map_err(|_| refused())?;
        if !metadata.is_file() || metadata.len() > 64 {
            return Err(refused());
        }
        let mut bytes = Vec::new();
        file.take(65)
            .read_to_end(&mut bytes)
            .map_err(|_| refused())?;
        if bytes.len() > 64 {
            return Err(refused());
        }
        std::str::from_utf8(&bytes)
            .ok()
            .and_then(|text| Uuid::parse_str(text).ok())
            .filter(|id| !id.is_nil())
            .ok_or_else(refused)
    }
}
