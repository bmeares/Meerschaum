//! A job's callback URL and grant: registration, confirmation of a stored start, schedule source
//! or graph request, and revocation.

use super::{admit_job, contains_uncaptured_graph, launch};
use crate::executor::client::Controller;
use crate::executor::jobs::{JobPlacement, Record};
use meerschaum_server::executor::pipe_operation::{GraphOperation, StoredIntent};
use meerschaum_server::executor::{callbacks, context, environment};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

impl Controller {
    pub(crate) async fn callback_url(&self, team_id: Uuid) -> Result<String, String> {
        let state = self.state()?;
        let url = match &state.config.execution_callback_url {
            Some(url) => url.clone(),
            None => {
                let teams = self
                    .instance
                    .get_teams()
                    .await
                    .map_err(|_| "Could not resolve callback placement.")?;
                if !teams
                    .iter()
                    .find(|t| {
                        t.id.as_deref().and_then(|s| Uuid::parse_str(s).ok()) == Some(team_id)
                    })
                    .is_some_and(|t| {
                        t.execution()
                            .is_ok_and(|s| s.mode == meerschaum_core::execution::Mode::Container)
                    })
                {
                    return Err("Configure `api:execution:callback_url` with the main API origin reachable by this executor.".into());
                }
                format!("http://api:{}", state.config.port)
            }
        };
        environment::validate_callback_url(&url)?;
        Ok(url)
    }

    pub(crate) fn register_job_callback<'a>(
        &'a self,
        record: &'a mut Record,
        prepared: Option<&'a super::PreparedExecutor>,
    ) -> Pin<Box<impl Future<Output = Result<Option<Uuid>, String>> + Send + 'a>> {
        Box::pin(async move {
            let Some(caller) = context::current().filter(|c| !c.no_auth) else {
                return Ok(None);
            };
            // A non-isolated team does not itself grant permission to run arbitrary code.
            // API-only placement carries a distinct callback ceiling and receives no Team secrets.
            if record.profile == environment::Profile::Team
                && !caller.full_admin()
                && !meerschaum_server::team_execution::code_running_team(&caller).is_some_and(
                    |team| {
                        team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok())
                            == Some(record.team_id)
                    },
                )
            {
                return Err("This source cannot authorize the credentialed Team profile.".into());
            }
            let state = self.state()?;
            let _ = self.job_callback_url(record).await?;
            if record.placement == JobPlacement::HostApi
                && record.profile != environment::Profile::Api
                && !(record.profile == environment::Profile::Metadata
                    && record.discovery_placement())
            {
                return Err("A host API job cannot use Team credentials.".into());
            }
            if record.discovery.is_none() {
                record.source_selection =
                    Box::pin(self.capture_job_sources(record, &caller)).await?;
            } else if record.source_selection.is_some() {
                return Err("Discovery cannot adopt an ordinary source selector.".into());
            }
            let uncaptured_graph = record.operation.is_none()
                && record.discovery.is_none()
                && record.extension.is_none()
                && record.connector_registration.is_none()
                && contains_uncaptured_graph(&record.sysargs)?;
            if uncaptured_graph && record.profile == environment::Profile::Team {
                context::stored_create_words(&record.name, &record.sysargs)
                    .map_err(|error| error.detail_text())?;
                let executor =
                    prepared.ok_or("This Team job has no signed executor configuration.")?;
                record.team_pipe_authority =
                    Some(super::team_pipe_job::capture(record, &caller, executor)?);
            } else if uncaptured_graph
                && let Some(request) = context::pipeline_request(&record.name, &record.sysargs)
            {
                if record.action_request || record.profile != environment::Profile::Api {
                    return Err("This Pipeline needs its accepted API-only run profile.".into());
                }
                record.operation =
                    Some(Box::pin(self.capture_pipeline_run(record, &request)).await?);
            } else if uncaptured_graph {
                if record.action_request || record.profile != environment::Profile::Api {
                    return Err("This graph needs a captured API-only job.".into());
                }
                let original = context::stored_create_words(&record.name, &record.sysargs)
                    .map_err(|error| error.detail_text())?;
                let intent =
                    StoredIntent::capture(&original).map_err(|error| error.detail_text())?;
                let source = context::refresh_stored_request(&state, &record.name, true)
                    .await
                    .map_err(|error| error.detail_text())?;
                let captured = Box::pin(self.capture_stored_pipe(record, &source, intent)).await?;
                record.sysargs = captured
                    .worker_args()
                    .map_err(|error| error.detail_text())?;
                record.operation = Some(GraphOperation::Stored(captured));
                let jitter = meerschaum_jobs::JobsConfig::from_config(&self.config()?)
                    .schedule_jitter_seconds;
                record.schedule = crate::executor::jobs::JobSchedule::new(
                    record,
                    chrono::Utc::now().fixed_offset(),
                    jitter,
                )?;
            }
            let direct = matches!(
                &record.operation,
                Some(meerschaum_server::executor::pipe_operation::GraphOperation::Direct(_))
            );
            let stored = matches!(&record.operation, Some(GraphOperation::Stored(_)));
            let pipeline_request =
                context::pipeline_request(&record.name, &record.sysargs).filter(|_| {
                    record
                        .operation
                        .as_ref()
                        .is_some_and(|op| op.pipeline_selection().is_some())
                });
            let caller = if let Some(request) = &pipeline_request {
                request.refresh(&state).await.map_err(|e| e.detail_text())?
            } else if stored {
                context::refresh_stored_request(&state, &record.name, true)
                    .await
                    .map_err(|error| error.detail_text())?
            } else if direct {
                if record.profile != environment::Profile::Api {
                    return Err("This captured built-in action requires an API-only launch.".into());
                }
                context::refresh_direct_action(&state, &caller)
                    .await
                    .map_err(|error| error.detail_text())?
            } else {
                caller
            };
            let mut grant = if let Some(discovery) = &record.discovery {
                if record.profile != environment::Profile::Metadata
                    || !record.discovery_placement()
                    || record.operation.is_some()
                    || record.action_request
                    || discovery.sysargs() != record.sysargs
                {
                    return Err(
                        "Metadata discovery has no exact private canonical operation.".into(),
                    );
                }
                discovery
                    .register(&state, &caller, launch(record)?)
                    .await
                    .map_err(|error| error.detail_text())?
            } else if let Some(extension) = &record.extension {
                if record.profile != environment::Profile::Api
                    || !record.extension_placement()
                    || record.operation.is_some()
                    || record.connector_registration.is_some()
                    || record.action_request
                {
                    return Err("The extension lost its exact private API ceiling.".into());
                }
                self.confirm_extension_sources(extension, &caller).await?;
                extension
                    .verify_sources(&state)
                    .await
                    .map_err(|error| error.detail_text())?;
                let team = extension
                    .team_id()
                    .map(|id| {
                        caller
                            .team_records
                            .iter()
                            .find(|team| {
                                team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok())
                                    == Some(id)
                            })
                            .ok_or("The extension Team is unavailable.")
                    })
                    .transpose()?;
                extension
                    .authorize_placement(&caller, team)
                    .map_err(|error| error.detail_text())?;
                let required = extension.required().map_err(|error| error.detail_text())?;
                callbacks::register_with_sources(
                    &state,
                    &caller,
                    launch(record)?,
                    extension.team_id(),
                    &required.iter().map(String::as_str).collect::<Vec<_>>(),
                    callbacks::Purpose::Command,
                    super::source_selection::ceiling(record.source_selection.as_ref())?,
                )
                .await
                .map_err(|error| error.detail_text())?
            } else {
                if record.profile == environment::Profile::Metadata {
                    return Err(
                        "A metadata profile requires Main's typed discovery operation.".into(),
                    );
                }
                let required = if let Some(registration) = &record.connector_registration {
                    if record.profile != environment::Profile::Api
                        || record.operation.is_some()
                        || record.action_request
                    {
                        return Err("The connector registration lost its fixed API ceiling.".into());
                    }
                    registration
                        .authorize(&state, &caller)
                        .await
                        .map_err(|error| error.detail_text())?;
                    "pipes:register"
                } else if let Some(operation) = &record.operation {
                    if record.profile != environment::Profile::Api {
                        return Err("A typed pipe operation requires its API-only profile.".into());
                    }
                    operation
                        .authorize(&state, &caller, &record.sysargs)
                        .await
                        .map_err(|error| error.detail_text())?;
                    "pipes:write"
                } else if record.action_request {
                    caller
                        .require(&["actions:execute"], &[])
                        .map_err(|error| error.detail_text())?;
                    "actions:execute"
                } else if (record.profile == environment::Profile::Api
                    && meerschaum_server::execute_policy::check_bound_command(
                        &state,
                        &caller,
                        &record.sysargs,
                    )
                    .map_err(|error| error.detail_text())?)
                    || meerschaum_server::execute_access::check(&state, &caller, &record.sysargs)
                        .await
                        .map_err(|error| error.detail_text())?
                        .needs_actions
                {
                    "actions:execute"
                } else {
                    "jobs:execute"
                };
                let required_scopes = record
                    .operation
                    .as_ref()
                    .map(|operation| operation.required_scopes())
                    .unwrap_or_else(|| std::collections::BTreeSet::from([required.to_string()]));
                let required = required_scopes
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let sources = super::source_selection::ceiling(record.source_selection.as_ref())?;
                let registered = if record.operation.is_some() {
                    callbacks::register_graph_with_sources(
                        &state,
                        &caller,
                        launch(record)?,
                        (!record.team_id.is_nil()).then_some(record.team_id),
                        &required,
                        sources,
                    )
                    .await
                } else {
                    callbacks::register_with_sources(
                        &state,
                        &caller,
                        launch(record)?,
                        (!record.team_id.is_nil()).then_some(record.team_id),
                        &required,
                        if record.profile == environment::Profile::Api {
                            callbacks::Purpose::Command
                        } else {
                            callbacks::Purpose::Jobs
                        },
                        sources,
                    )
                    .await
                };
                registered.map_err(|_| "The job's source identity cannot authorize callbacks.")?
            };
            if record.operation.is_some() && grant.graph_origin.is_some() {
                let _ = callbacks::revoke(&state, grant.id).await;
                return Err("Nested graphs require Main's exact intersection of the inherited node windows.".into());
            }
            let identity = match callbacks::load_identity(&state, grant.id, grant.launch_id).await {
                Ok(identity) => identity,
                Err(_) => {
                    let _ = callbacks::revoke(&state, grant.id).await;
                    return Err(
                        "The job's source identity changed while registering its owner.".into(),
                    );
                }
            };
            record.owner_binding = identity.owner_binding();
            if let Err(error) = self
                .issue_operation_windows(record, &grant, Some(&caller))
                .await
            {
                let _ = callbacks::revoke(&state, grant.id).await;
                return Err(error);
            }
            if record.operation.is_some() {
                grant = match callbacks::bind_graph_origin(
                    &state,
                    grant.id,
                    grant.launch_id,
                    record.context_revision,
                    None,
                )
                .await
                {
                    Ok(grant) => grant,
                    Err(error) => {
                        let _ = callbacks::revoke(&state, grant.id).await;
                        return Err(error.detail_text());
                    }
                };
            }
            if let Err(error) = admit_job(self, &state, record, grant.id).await {
                let _ = callbacks::revoke(&state, grant.id).await;
                return Err(error);
            }
            if let Some(request) = &pipeline_request
                && let Err(error) = request.refresh(&state).await
            {
                let _ = callbacks::revoke(&state, grant.id).await;
                return Err(error.detail_text());
            }
            if direct && let Err(error) = context::refresh_direct_action(&state, &caller).await {
                let _ = callbacks::revoke(&state, grant.id).await;
                return Err(error.detail_text());
            }
            if stored
                && let Err(error) =
                    context::refresh_stored_request(&state, &record.name, true).await
            {
                let _ = callbacks::revoke(&state, grant.id).await;
                return Err(error.detail_text());
            }
            if record.schedule.is_some() {
                // Revision one reserves the immutable original source. It is never installed on
                // an executor. The first actual fire issues revision two with current bounds.
                if let Err(error) = callbacks::suspend(&state, grant.id, grant.launch_id).await {
                    let _ = callbacks::revoke(&state, grant.id).await;
                    return Err(error.detail_text());
                }
                record.callback_suspended = true;
            }
            Ok(Some(grant.id))
        })
    }

    pub(super) async fn job_callback_url(&self, record: &Record) -> Result<String, String> {
        if record.profile == environment::Profile::Metadata {
            if !record.discovery_placement() {
                return Err("Metadata discovery has no private source selection.".into());
            }
            return Ok(environment::METADATA_ORIGIN.into());
        }
        if record.placement != JobPlacement::HostApi {
            return self.callback_url(record.team_id).await;
        }
        let state = self.state()?;
        let url = state.config.execution_callback_url.as_ref()
            .ok_or("Configure `api:execution:callback_url` with the trusted main API origin before starting API-only host jobs.")?;
        environment::validate_callback_url(url)?;
        Ok(url.clone())
    }

    pub(crate) async fn confirm_stored_start(&self, record: &Record) -> Result<(), String> {
        let state = self.state()?;
        let requester = context::refresh_stored_request(&state, &record.name, false)
            .await
            .map_err(|error| error.detail_text())?;
        if context::scope(Some(requester), self.job_view(record.clone()))
            .await?
            .is_none()
        {
            return Err("The saved job is no longer visible to this caller.".into());
        }
        Ok(())
    }

    pub(crate) async fn confirm_schedule_source(&self, record: &Record) -> Result<(), String> {
        if record.schedule.is_none() || !matches!(record.operation, Some(GraphOperation::Stored(_)))
        {
            return Err("This job has no saved graph schedule.".into());
        }
        let state = self.state()?;
        let grant = record
            .callback
            .ok_or("This schedule lost its original source.")?;
        let identity = admit_job(self, &state, record, grant).await?;
        if self.capture_job_sources(record, &identity.caller()).await? != record.source_selection {
            return Err("The saved job's plugin source set changed.".into());
        }
        // Recheck original authority after the potentially slow source enumeration.
        admit_job(self, &state, record, grant).await?;
        Ok(())
    }

    pub(crate) async fn confirm_graph_request(
        &self,
        record: &Record,
        restart: bool,
    ) -> Result<(), String> {
        if let Some(operation) = record
            .operation
            .as_ref()
            .filter(|op| op.pipeline_selection().is_some())
        {
            if restart {
                return Err(
                    "Submit a fresh Pipeline run to capture its current targets and revision."
                        .into(),
                );
            }
            let request = context::pipeline_request(&record.name, &record.sysargs)
                .ok_or("Missing original Pipeline request.")?;
            let state = self.state()?;
            let current = request.refresh(&state).await.map_err(|e| e.detail_text())?;
            if self.capture_job_sources(record, &current).await? != record.source_selection {
                return Err("The admitted Pipeline source set changed.".into());
            }
            operation
                .authorize(&state, &current, &record.sysargs)
                .await
                .map_err(|e| e.detail_text())?;
            request.refresh(&state).await.map_err(|e| e.detail_text())?;
            return Ok(());
        }
        let stored = matches!(&record.operation, Some(GraphOperation::Stored(_)));
        if !stored && !matches!(&record.operation, Some(GraphOperation::Direct(_))) {
            return Ok(());
        }
        let state = self.state()?;
        let current = if stored {
            let requester = context::refresh_stored_request(&state, &record.name, !restart)
                .await
                .map_err(|error| error.detail_text())?;
            if restart {
                let grant = record
                    .callback
                    .ok_or("The saved job lost its original source.")?;
                Arc::new(admit_job(self, &state, record, grant).await?.caller())
            } else {
                requester
            }
        } else {
            let original =
                context::current().ok_or("This direct action lost its original actor.")?;
            context::refresh_direct_action(&state, &original)
                .await
                .map_err(|error| error.detail_text())?
        };
        if self.capture_job_sources(record, &current).await? != record.source_selection {
            return Err("The exact source set changed during direct action admission.".into());
        }
        record
            .operation
            .as_ref()
            .ok_or("Missing direct action capture.")?
            .authorize(&state, &current, &record.sysargs)
            .await
            .map_err(|error| error.detail_text())?;
        if stored {
            if restart {
                self.confirm_stored_start(record).await?;
            } else {
                context::refresh_stored_request(&state, &record.name, true)
                    .await
                    .map_err(|error| error.detail_text())?;
            }
        } else {
            context::refresh_direct_action(&state, &current)
                .await
                .map_err(|error| error.detail_text())?;
        }
        Ok(())
    }

    pub(crate) async fn discard_job_callback(&self, id: Uuid) {
        if let Ok(state) = self.state() {
            let _ = callbacks::revoke(&state, id).await;
        }
    }
}
