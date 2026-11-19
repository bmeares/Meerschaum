//! Typed plugin discovery and exact calls across explicit Remote/Host/Everyone placement.
use super::Controller;
use meerschaum_core::access::{Team, is_everyone, same_team};
use meerschaum_server::{
    access::Access,
    executor::{
        context, extensions, plugin_handlers, plugin_host::Placement, plugins, source_ceiling,
    },
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use uuid::Uuid;

impl Controller {
    async fn extension_team(&self, caller: &Access) -> Result<Option<Team>, String> {
        if caller.full_admin() || is_everyone(Some(&caller.primary_team)) {
            return Ok(None);
        }
        let captured = caller
            .team_records
            .iter()
            .find(|team| same_team(Some(&team.name), Some(&caller.primary_team)))
            .and_then(|team| team.id.as_deref())
            .and_then(|id| Uuid::parse_str(id).ok())
            .ok_or("The extension's original registered Team is unavailable.")?;
        let team = self
            .instance
            .get_team(&caller.primary_team)
            .await
            .map_err(|_| "The extension Team lookup failed.")?
            .filter(|team| {
                team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) == Some(captured)
            })
            .ok_or("The extension Team was deleted or replaced.")?;
        let selected = self.selected(Some(&team.name)).await?;
        match selected {
            Some(current) if current.id == team.id => Ok(Some(current)),
            None if team
                .execution()
                .is_ok_and(|settings| settings.mode == meerschaum_core::execution::Mode::Host) =>
            {
                Ok(Some(team))
            }
            _ => {
                Err("The extension placement changed during admission; refresh the request.".into())
            }
        }
    }
    pub async fn extension_catalog(self: &Arc<Self>) -> Result<Option<plugins::Validated>, String> {
        let caller = context::current()
            .filter(|caller| !caller.no_auth && caller.auth.is_some())
            .ok_or("Plugin discovery lost its authenticated original caller.")?;
        caller
            .require(&["plugins:read"], &[])
            .map_err(|error| error.detail_text())?;
        let team = self.extension_team(&caller).await?;
        let remote = team.as_ref().is_some_and(|team| {
            team.execution()
                .is_ok_and(|settings| settings.mode.is_remote())
        });
        if remote {
            let team = team.as_ref().ok_or("The extension Team is unavailable.")?;
            if !team.is_isolated() {
                return Err("A remote extension requires a registered isolated Team.".into());
            }
            let id = team
                .id
                .as_deref()
                .and_then(|id| Uuid::parse_str(id).ok())
                .ok_or("Extension discovery needs a real Team UUID.")?;
            let sources: BTreeSet<_> = team
                .granted_plugins()
                .into_iter()
                .filter(|name| {
                    meerschaum_core::access::can_use_plugin(&caller.principal, name)
                        && source_ceiling::permits_name(&caller, name)
                })
                .collect();
            if sources.is_empty() {
                return Ok(None);
            }
            return context::extension_discovery_scope(self.discover_plugin_catalog(id, sources))
                .await
                .map(Some);
        }
        let caller_for_sources = caller.clone();
        let directories = self.config()?.paths().plugins_dirs.clone();
        let sources = tokio::task::spawn_blocking(move || {
            crate::executor_host_jobs::plugin_sources(&caller_for_sources, &directories)
                .map(|(_, names)| names)
        })
        .await
        .map_err(|_| "Host extension source selection was interrupted.")??;
        if sources.is_empty() {
            return Ok(None);
        }
        let team_id = team
            .as_ref()
            .map(|team| {
                team.id
                    .as_deref()
                    .and_then(|id| Uuid::parse_str(id).ok())
                    .ok_or("The Host extension lost its real Team UUID.")
            })
            .transpose()?;
        context::extension_discovery_scope(self.discover_host_plugin_catalog(team_id, sources))
            .await
            .map(Some)
    }
    pub(super) async fn confirm_extension_sources(
        &self,
        operation: &extensions::Operation,
        caller: &Access,
    ) -> Result<(), String> {
        let Placement::Host(host) = &operation.catalog.placement else {
            return Ok(());
        };
        let host = host.clone();
        let caller = caller.clone();
        let root = self.root.clone();
        let directories = self.config()?.paths().plugins_dirs.clone();
        let names = operation
            .catalog
            .plugins
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        tokio::task::spawn_blocking(move || {
            if meerschaum_server::executor::plugin_host::read_generation(&root)? != host.generation
            {
                return Err("The Host extension generation was retired.".into());
            }
            let (mut sources, available) =
                crate::executor_host_jobs::plugin_sources(&caller, &directories)?;
            if !names.is_subset(&available) {
                return Err("A Host extension source grant was revoked.".into());
            }
            sources.retain(|(filename, _)| {
                filename.to_str().is_some_and(|name| {
                    names.contains(
                        name.strip_suffix(".py")
                            .or_else(|| name.strip_suffix(".rs"))
                            .unwrap_or(name),
                    )
                })
            });
            let actual = crate::executor_plugin_files::inspect(&root, &sources)?;
            if actual.hashes != host.files || actual.view_sha256 != host.view_sha256 {
                return Err("The exact Host extension source paths or bytes changed.".into());
            }
            Ok(())
        })
        .await
        .map_err(|_| "Host extension source verification was interrupted.")?
    }
    pub async fn extension_invoke(
        self: &Arc<Self>,
        invocation: plugin_handlers::Invocation,
    ) -> Result<plugin_handlers::Reply, String> {
        let catalog = self
            .extension_catalog()
            .await?
            .ok_or("No selected confined extension sources are available.")?;
        let caller = context::current().ok_or("The extension lost its original caller.")?;
        let operation = Arc::new(extensions::Operation {
            caller: Some(
                meerschaum_server::executor::plugin_caller::Caller::capture(
                    &caller,
                    context::extension_console_session(),
                )
                .map_err(|error| error.detail_text())?,
            ),
            deadline: chrono::Utc::now().timestamp() + 60,
            catalog: catalog.catalog().clone(),
            invocation,
        });
        operation.validated().map_err(|error| error.detail_text())?;
        let team = self.extension_team(&caller).await?;
        operation
            .authorize_placement(&caller, team.as_ref())
            .map_err(|error| error.detail_text())?;
        self.confirm_extension_sources(&operation, &caller).await?;
        let request = context::extension_scope(
            operation.clone(),
            self.run_tracked(
                team.map(|team| team.name),
                "plugin_extension",
                operation.sysargs(),
            ),
        );
        let (result,_)=tokio::time::timeout(Duration::from_secs(45),request).await.map_err(|_|"The extension exceeded its execution budget; its short source lease is being retired.")??;
        if !result.success {
            return Err(result.message.chars().take(2048).collect());
        }
        operation
            .validate_reply(result.message.as_bytes())
            .map_err(|error| error.detail_text())
    }
}
