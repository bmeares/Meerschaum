//! Main-selected immutable executable sources for one canonical launch.

use super::Controller;
use crate::executor_jobs::{JobPlacement, Record};
use meerschaum_core::access::can_use_plugin;
use meerschaum_server::access::Access;
use meerschaum_server::executor::registration::Sources;
use meerschaum_server::executor::source_ceiling::{self, Ceiling};
use meerschaum_server::executor::{callbacks, environment, plugin_delivery, plugins};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use uuid::Uuid;

pub(super) fn ceiling(sources: Option<&Sources>) -> Result<Ceiling, String> {
    match sources {
        Some(Sources::Archives { catalog }) => Ceiling::from_catalog(catalog),
        Some(Sources::Files { hashes }) => Ceiling::from_files(hashes),
        None => Ok(Ceiling::default()),
    }
}

/// A private catalog and its callback must describe exactly the same content. In particular,
/// legacy missing selectors remain empty; a current Team grant never repairs that identity.
pub(super) fn agrees(
    sources: Option<&Sources>,
    identity: &callbacks::Identity,
) -> Result<(), String> {
    let expected = ceiling(sources)?;
    if identity.grant.source_ceiling.clone().unwrap_or_default() != expected {
        return Err("The launch's immutable source selection no longer matches its original callback. Recreate the job explicitly.".into());
    }
    Ok(())
}

impl Controller {
    pub(super) async fn capture_job_sources(
        &self,
        record: &Record,
        caller: &Access,
    ) -> Result<Option<Sources>, String> {
        if let Some(extension) = &record.extension {
            return Ok(Some(extension.sources()));
        }
        if let Some(registration) = &record.connector_registration {
            return Ok(Some(registration.sources.clone()));
        }
        if record.discovery.is_some() {
            return Err("Discovery uses its separate credential-free source selection.".into());
        }
        self.capture_sources(record.team_id, record.placement, caller)
            .await
    }

    pub(super) async fn capture_sources(
        &self,
        team_id: Uuid,
        placement: JobPlacement,
        caller: &Access,
    ) -> Result<Option<Sources>, String> {
        if placement == JobPlacement::HostApi {
            let caller = caller.clone();
            let directories = self.config()?.paths().plugins_dirs.clone();
            let root = self.root.clone();
            let hashes = tokio::task::spawn_blocking(move || {
                let (sources, _) =
                    crate::executor::host_jobs::plugin_sources(&caller, &directories)?;
                crate::executor::plugin_files::Store::new(&root)?
                    .snapshot(&sources, None)
                    .map(|snapshot| snapshot.hashes)
            })
            .await
            .map_err(|_| "Host source selection was interrupted.")??;
            return Ok((!hashes.is_empty()).then_some(Sources::Files { hashes }));
        }
        let team = self
            .instance
            .get_teams()
            .await
            .map_err(|_| "The registered source Team is unavailable.")?
            .into_iter()
            .find(|team| {
                team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) == Some(team_id)
            })
            .ok_or("The registered source Team was removed.")?;
        let names: BTreeSet<_> = team
            .granted_plugins()
            .into_iter()
            .filter(|name| {
                can_use_plugin(&caller.principal, name)
                    && source_ceiling::permits_name(caller, name)
            })
            .collect();
        if names.is_empty() {
            return Ok(None);
        }
        let (registered, _) =
            crate::executor_registry::Registry::new(&self.root).authority(team_id)?;
        let generation = registered.verifier.generation;
        let path = self.state()?.config.plugins_archives_path.clone();
        let catalog = tokio::task::spawn_blocking(move || {
            super::plugins::source_catalog(&path, team_id, generation, &names)
        })
        .await
        .map_err(|_| "Remote source selection was interrupted.")??;
        let ceiling = Ceiling::from_catalog(&catalog)?;
        if !ceiling.within_parent(caller) || !ceiling.current(caller, Some(&team)) {
            return Err("The selected plugin content exceeds the original source ceiling.".into());
        }
        Ok(Some(Sources::Archives { catalog }))
    }

    /// Recheck content before any reissue, including explicit restarts of existing definitions.
    pub(super) async fn confirm_job_sources(
        &self,
        record: &Record,
        caller: &Access,
    ) -> Result<(), String> {
        if let Some(discovery) = &record.discovery
            && matches!(
                discovery.catalog.placement,
                meerschaum_server::executor::plugin_host::Placement::Host(_)
            )
        {
            return Box::pin(self.confirm_host_discovery_sources(discovery, caller)).await;
        }
        self.confirm_sources(
            record.team_id,
            record.placement,
            record.source_selection.as_ref(),
            caller,
        )
        .await
    }

    pub(super) async fn confirm_sources(
        &self,
        team_id: Uuid,
        placement: JobPlacement,
        selection: Option<&Sources>,
        caller: &Access,
    ) -> Result<(), String> {
        let Some(expected) = selection else {
            return Ok(());
        };
        match expected {
            Sources::Files { hashes } => {
                if placement != JobPlacement::HostApi {
                    return Err("Remote execution cannot adopt a Host source snapshot.".into());
                }
                let caller = caller.clone();
                let directories = self.config()?.paths().plugins_dirs.clone();
                let root = self.root.clone();
                let expected = hashes.clone();
                tokio::task::spawn_blocking(move || {
                    let (sources, _) = crate::executor::host_jobs::plugin_sources(&caller, &directories)?;
                    let snapshot = crate::executor::plugin_files::Store::new(&root)?.snapshot(&sources, None)?;
                    if snapshot.hashes != expected {
                        return Err("The original Host plugin content or grant set changed. Recreate the job explicitly.".into());
                    }
                    Ok(())
                }).await.map_err(|_| "Host source verification was interrupted.")?
            }
            Sources::Archives { catalog } => {
                if placement != JobPlacement::Remote || catalog.team_id != team_id {
                    return Err("The source catalog belongs to a different placement.".into());
                }
                let (registered, _) =
                    crate::executor_registry::Registry::new(&self.root).authority(team_id)?;
                if registered.verifier.generation != catalog.generation {
                    return Err(
                        "The original source generation was retired. Recreate the job explicitly."
                            .into(),
                    );
                }
                let path = self.state()?.config.plugins_archives_path.clone();
                let expected = catalog.clone();
                tokio::task::spawn_blocking(move || {
                    let current = super::plugins::source_catalog(&path, expected.team_id, expected.generation,
                        &expected.plugins.keys().cloned().collect())?;
                    if current != expected {
                        return Err("The original remote plugin content changed. Recreate the job explicitly.".into());
                    }
                    Ok(())
                }).await.map_err(|_| "Remote source verification was interrupted.")?
            }
        }
    }

    pub(super) async fn stage_job_sources(
        &self,
        record: &Record,
        identity: &callbacks::Identity,
        api: &meerschaum_connector_api::APIConnector,
    ) -> Result<Option<String>, String> {
        if !matches!(
            record.profile,
            environment::Profile::Api | environment::Profile::Team
        ) || record.placement != JobPlacement::Remote
        {
            return Ok(None);
        }
        self.stage_remote_sources(
            record.team_id,
            record.source_selection.as_ref(),
            identity,
            api,
        )
        .await
    }

    pub(super) async fn stage_remote_sources(
        &self,
        team_id: Uuid,
        selection: Option<&Sources>,
        identity: &callbacks::Identity,
        api: &meerschaum_connector_api::APIConnector,
    ) -> Result<Option<String>, String> {
        use meerschaum_connector_api::client::{Body, Method, RequestOptions};
        let Some(Sources::Archives { catalog }) = selection else {
            return Ok(None);
        };
        if identity.grant.team_id != Some(team_id) || catalog.team_id != team_id {
            return Err("This source delivery belongs to another registered Team.".into());
        }
        agrees(selection, identity)?;
        self.confirm_sources(team_id, JobPlacement::Remote, selection, &identity.caller())
            .await?;
        let digest =
            plugins::sha256(&serde_json::to_vec(catalog).map_err(|_| "Invalid source catalog.")?);
        let mut credentials = BTreeMap::new();
        for (name, source) in &catalog.plugins {
            credentials.insert(
                name.clone(),
                plugin_delivery::issue(
                    &self.state()?,
                    identity.grant.id,
                    identity.grant.launch_id,
                    team_id,
                    catalog.generation,
                    name,
                    &source.archive_sha256,
                )
                .await
                .map_err(|error| error.detail_text())?,
            );
        }
        let delivery = plugin_delivery::Delivery {
            catalog: catalog.clone(),
            repository_origin: self.callback_url(team_id).await?,
            credentials,
        };
        let response = api
            .make_request(
                Method::Post,
                meerschaum_server::executor::gateway::PLUGIN_DELIVERY_PATH,
                RequestOptions {
                    body: Body::Json(
                        serde_json::to_string(&delivery).map_err(|_| "Invalid source delivery.")?,
                    ),
                    timeout: Some(Duration::from_secs(20)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| "The executor could not pull this launch's exact plugin sources.")?;
        if response.status != 200
            || response
                .json()
                .ok()
                .and_then(|body| body["catalog_sha256"].as_str().map(str::to_string))
                != Some(digest.clone())
        {
            return Err("The executor did not confirm this launch's exact source catalog.".into());
        }
        Ok(Some(digest))
    }
}
