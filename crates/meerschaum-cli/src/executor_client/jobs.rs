//! Canonical persistent jobs own callback authority independently of their parent launch.

use super::Controller;
use crate::executor_jobs::{Catalog, JobPlacement, Record};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_core::{SuccessTuple, Value, users::User};
use meerschaum_server::access::Access;
use meerschaum_server::executor::pipe_operation::{GraphOperation, StoredIntent, StoredPipe};
use meerschaum_server::executor::{admission, callbacks, context, environment};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

#[path = "pipe_windows.rs"]
mod pipe_windows;

#[cfg(test)]
#[path = "context_delivery_tests.rs"]
mod context_delivery_tests;
#[cfg(test)]
#[path = "job_future_tests.rs"]
mod future_size_tests;

fn launch(record: &Record) -> Result<Uuid, String> {
    record
        .remote_name
        .strip_prefix("mrsm-")
        .and_then(|s| Uuid::parse_str(s).ok())
        .filter(|id| !id.is_nil())
        .ok_or_else(|| "The job has no canonical launch identity.".into())
}

/// Reuse the chain expansion, normal parser and typed overrides used for launch placement.
fn contains_uncaptured_graph(argv: &[String]) -> Result<bool, String> {
    let clock = crate::types::DateContext::system();
    for step in meerschaum_server::execute_policy::command_steps(argv)? {
        let mut args =
            crate::parse::parse_arguments(&step.words, &clock).map_err(|error| error.message)?;
        args.extend(
            step.patch
                .into_iter()
                .map(|(key, value)| (key, crate::server::argument(value))),
        );
        let words = crate::native::words(&args, "action");
        if crate::actions::get_action(&words, crate::actions::CORE_ACTIONS).is_some_and(
            |resolved| {
                matches!(resolved.action.name, "sync" | "verify")
                    && !matches!(
                        resolved.function_name(),
                        "_verify_packages" | "_verify_plugins" | "_verify_venvs"
                    )
            },
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

impl Controller {
    async fn capture_stored_pipe(
        &self,
        record: &Record,
        caller: &Access,
        intent: StoredIntent,
    ) -> Result<StoredPipe, String> {
        let state = self.state()?;
        let clock = crate::types::DateContext::system();
        let mut pipe = intent
            .action()
            .map_err(|error| error.detail_text())?
            .resolve(&state, caller)
            .await
            .map_err(|error| error.detail_text())?;
        let words =
            Box::pin(self.capture_registered_pipe(&state, caller, &mut pipe, &clock)).await?;
        let session = crate::native::Session::with_runtime(tokio::runtime::Handle::current());
        session.set_python_worker(false);
        let placement = context::scope(
            Some(Arc::new(caller.clone())),
            Box::pin(self.command_team(&words, &session)),
        )
        .await?;
        let expected = if record.placement == JobPlacement::Remote {
            Some(record.team_id)
        } else {
            None
        };
        let actual = placement
            .map(|team| {
                team.id
                    .as_deref()
                    .and_then(|id| Uuid::parse_str(id).ok())
                    .ok_or("The stored job's executor Team is no longer registered.")
            })
            .transpose()?;
        if actual != expected {
            return Err("The stored job's pipe selects a different executor.".into());
        }
        Ok(StoredPipe {
            intent,
            normalized_at: clock.now_utc.and_utc().timestamp(),
            pipe,
        })
    }

    /// Revalidate the original source after a final result has been read. The operation may
    /// have updated its own target metadata, so this checks its retained source capability.
    async fn capture_pipeline_run(
        &self,
        record: &Record,
        request: &meerschaum_server::executor::pipe_operation::PipelineRunRequest,
    ) -> Result<GraphOperation, String> {
        let state = self.state()?;
        let mut operation = request.capture(&state).await.map_err(|e| e.detail_text())?;
        let caller = request.refresh(&state).await.map_err(|e| e.detail_text())?;
        let (selection, _) = operation
            .pipeline_selection()
            .ok_or("Missing captured Pipeline selection.")?;
        let targets = meerschaum_engine::Selection::refs(
            selection
                .roots
                .keys()
                .map(meerschaum_engine::PipeRef::from_string),
        )
        .with_graph(true);
        let settings =
            meerschaum_engine::EngineConfig::from_config(&self.config()?, Some(&self.root));
        let snapshot = super::pipe_operation::capture_selected(
            &state,
            &caller,
            targets,
            &selection.roots,
            &settings,
        )
        .await
        .map_err(|e| e.detail_text())?;
        let session = crate::native::Session::with_runtime(tokio::runtime::Handle::current());
        session.set_python_worker(false);
        for reference in selection.roots.keys() {
            let parts: Vec<_> = reference.split('|').collect();
            let words = [
                "sync", "pipes", "-i", parts[0], "-c", parts[1], "-m", parts[2], "-l", parts[3],
            ]
            .map(str::to_string);
            let team = context::scope(
                Some(caller.clone()),
                Box::pin(self.command_team(&words, &session)),
            )
            .await?;
            let actual = team
                .map(|team| {
                    team.id
                        .as_deref()
                        .and_then(|id| Uuid::parse_str(id).ok())
                        .ok_or("A Pipeline target lost its Team registration.")
                })
                .transpose()?;
            let expected = (record.placement == JobPlacement::Remote).then_some(record.team_id);
            if actual != expected {
                return Err("The Pipeline targets select different executors.".into());
            }
        }
        operation
            .set_pipeline_snapshot(snapshot)
            .map_err(|e| e.detail_text())?;
        let caller = request.refresh(&state).await.map_err(|e| e.detail_text())?;
        operation
            .authorize(&state, &caller, &record.sysargs)
            .await
            .map_err(|e| e.detail_text())?;
        Ok(operation)
    }

    pub(super) async fn validate_job_result_source(&self, record: &Record) -> Result<(), String> {
        let state = self.state()?;
        let grant = record
            .callback
            .ok_or("The execution has no original source proof.")?;
        if record.extension.is_some() {
            admit_job(self, &state, record, grant).await?;
            return Ok(());
        }
        let current = callbacks::load_identity(&state, grant, launch(record)?)
            .await
            .map_err(|error| error.detail_text())?;
        self.confirm_job_sources(record, &current.caller()).await?;
        let current = callbacks::load_identity(&state, grant, launch(record)?)
            .await
            .map_err(|error| error.detail_text())?;
        if record.owner_binding.is_some() && record.owner_binding != current.owner_binding() {
            return Err("The execution belongs to another registered source.".into());
        }
        super::source_selection::agrees(record.source_selection.as_ref(), &current)
    }

    /// A read primitive can borrow only the exact Main-created operation behind this live
    /// callback. The catalog and fresh source proof remain authoritative on every call.
    pub(crate) fn pipe_operation_read<'a>(
        &'a self,
        caller: &'a Access,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<meerschaum_server::executor::pipe_operation::ReadAccess>,
                        String,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let Some(meerschaum_server::auth::Principal::Execution(identity)) = &caller.auth else {
                return Ok(None);
            };
            if identity.grant.graph_tracking != 1 {
                return Err("This older derived execution has unknown graph provenance. Recreate it using its original credentials.".into());
            }
            if identity.grant.purpose != callbacks::Purpose::Command
                || !identity.grant.required.contains("pipes:write")
            {
                if identity.grant.graph_origin.is_some() {
                    return Err(
                        "The inherited graph lost its immutable command/write requirements.".into(),
                    );
                }
                return Ok(None);
            }
            let grant = identity.grant.id;
            let launch_id = identity.grant.launch_id;
            let origin = identity.grant.graph_origin.clone();
            let inherited = origin.clone();
            let catalog = Catalog::new(&self.root);
            let (record, issuer_record) = tokio::task::spawn_blocking(move || {
                let records = catalog.records_if_available()?;
                let consumer = records
                    .values()
                    .find(|record| {
                        record.callback == Some(grant)
                            && launch(record).ok() == Some(launch_id)
                            && !record.callback_suspended
                    })
                    .cloned();
                let issuer = if let Some(origin) = origin {
                    Some(
                        records
                            .values()
                            .find(|record| {
                                record.callback == Some(origin.grant)
                                    && launch(record).ok() == Some(origin.launch)
                                    && record.context_revision == origin.context_revision
                                    && !record.callback_suspended
                            })
                            .cloned()
                            .ok_or("The originating graph was retired, replaced or suspended.")?,
                    )
                } else {
                    None
                };
                Ok::<_, String>((consumer, issuer))
            })
            .await
            .map_err(|_| "Could not inspect the canonical pipe operation.")??;
            let Some(record) = record else {
                // Current direct terminal/handler grants can prove no graph without
                // inventing a job record. A graph consumer never uses this fallback.
                if inherited.is_none() && identity.grant.graph_tracking == 1 {
                    return Ok(None);
                }
                return Err("The canonical pipe operation is absent or retired.".into());
            };
            let state = self.state()?;
            let live = callbacks::load_identity(&state, grant, launch_id)
                .await
                .map_err(|error| error.detail_text())?;
            if live.grant.graph_origin != inherited
                || live.grant.graph_tracking != 1
                || live.grant.graph_pending
            {
                return Err("The callback lost its exact graph provenance.".into());
            }
            let registries =
                meerschaum_server::executor::pipe_operation::NativeGraphRegistries::load(
                    &state,
                    issuer_record
                        .as_ref()
                        .and_then(|issuer| issuer.operation.as_ref())
                        .and_then(GraphOperation::snapshot),
                )
                .await
                .map_err(|error| error.detail_text())?;
            registries
                .scope(
                    &state,
                    Box::pin(async {
                        let current = admit_job(self, &state, &record, grant).await?;
                        let mut bounded = current.caller();
                        bounded.principal.permissions = meerschaum_server::access::intersect(
                            &bounded.principal.permissions,
                            &caller.scopes(),
                        );
                        self.confirm_job_sources(&record, &current.caller()).await?;
                        let binding =
                            meerschaum_server::executor::pipe_operation::CallbackBinding {
                                grant,
                                launch: launch_id,
                                context_revision: record.context_revision,
                            };
                        let issuer_record = if let Some(issuer_record) = issuer_record {
                            let origin = inherited
                                .as_ref()
                                .ok_or("The graph issuer binding is absent.")?;
                            if &binding != origin && record.operation.is_some() {
                                return Err(
                                "Nested graphs require Main's exact window intersection admission."
                                    .into(),
                            );
                            }
                            if origin != &binding || issuer_record.name != record.name {
                                let active_issuer =
                                    callbacks::load_identity(&state, origin.grant, origin.launch)
                                        .await
                                        .map_err(|error| error.detail_text())?;
                                if active_issuer.grant.graph_origin.as_ref() != Some(origin)
                                    || active_issuer.grant.graph_tracking != 1
                                    || active_issuer.grant.graph_pending
                                {
                                    return Err(
                                        "The originating graph has no active exact Main binding."
                                            .into(),
                                    );
                                }
                                let source =
                                    admit_job(self, &state, &issuer_record, origin.grant).await?;
                                self.confirm_job_sources(&issuer_record, &source.caller())
                                    .await?;
                                if source.owner_binding() != current.owner_binding()
                                    || source.grant.team_id != current.grant.team_id
                                {
                                    return Err(
                        "The graph issuer belongs to another registered source or Team boundary."
                            .into(),
                    );
                                }
                            }
                            issuer_record
                        } else {
                            if record.operation.is_some() {
                                return Err(
                                    "The canonical graph has no exact originating window binding."
                                        .into(),
                                );
                            }
                            return Ok(None);
                        };
                        let operation = issuer_record
                            .operation
                            .clone()
                            .ok_or("The originating graph lost its exact Main operation.")?;
                        if issuer_record.profile != environment::Profile::Api
                            || operation.snapshot().is_none()
                        {
                            return Err(
                                "The pipe operation has no captured API-only read capability."
                                    .into(),
                            );
                        }
                        // Keep the actual consumer's narrower principal. Fresh issuer admission
                        // never turns its original source or plugin set into the child's authority.
                        operation
                            .authorize(&state, &bounded, &issuer_record.sysargs)
                            .await
                            .map_err(|error| error.detail_text())?;
                        let issuer = inherited.ok_or("The graph issuer is absent.")?;
                        let pending_nodes = operation
                            .snapshot()
                            .ok_or("The graph snapshot is absent.")?
                            .members()
                            .keys()
                            .filter(|reference| {
                                issuer_record.commit_journal.has_pending_for(reference)
                            })
                            .cloned()
                            .collect::<std::collections::BTreeSet<_>>();
                        if issuer_record.commit_journal.has_pending() && pending_nodes.is_empty() {
                            return Err(
                    "The graph has unfinished commit evidence without a current admitted node."
                        .into(),
                );
                        }
                        Ok(Some(
                            meerschaum_server::executor::pipe_operation::ReadAccess {
                                caller: bounded,
                                operation,
                                binding,
                                issuer,
                                pending_nodes,
                            },
                        ))
                    }),
                )
                .await
        })
    }

    /// Project display names from immutable team IDs before applying current visibility.
    /// A replacement team/name or username cannot adopt historical canonical metadata.
    pub(crate) async fn job_views(&self, records: Vec<Record>) -> Result<Vec<Record>, String> {
        use meerschaum_core::job_access::{JobAccess, can_see_job};
        if records.is_empty() {
            return Ok(records);
        }
        let teams = self
            .instance
            .get_teams()
            .await
            .map_err(|_| "Could not resolve canonical job teams.")?;
        let caller = context::current();
        let mut visible = Vec::new();
        for mut record in records {
            let everyone = record.placement == JobPlacement::HostApi && record.team_id.is_nil();
            let team = teams.iter().find(|t| {
                t.id.as_deref().and_then(|s| Uuid::parse_str(s).ok()) == Some(record.team_id)
            });
            let old = JobAccess::from_properties(&record.name, &record.properties);
            if let Some(team) = team {
                JobAccess::stamp(
                    &mut record.properties,
                    old.owner.as_deref(),
                    Some(&team.name),
                );
            }
            if let Some(caller) = &caller
                && !caller.no_auth
                && !caller.is_admin()
            {
                let member = everyone && caller.principal.is_member(None)
                    || team.is_some_and(|team| {
                        caller.team_records.iter().any(|t| t.id == team.id)
                            && caller.principal.is_member(Some(&team.name))
                    });
                let within_boundary = if everyone {
                    caller.principal.permits_team(None)
                } else {
                    team.is_some_and(|team| caller.principal.permits_team(Some(&team.name)))
                };
                if caller.principal.team_boundary.is_some() && (!member || !within_boundary) {
                    continue;
                }
                let label = caller.username().map(str::to_string).or_else(|| {
                    meerschaum_server::routes::execute::owner_label(caller.auth.as_ref())
                });
                let owns = old.owner.is_some()
                    && old.owner == label
                    && record
                        .owner_binding
                        .as_ref()
                        .is_some_and(|binding| binding.matches(caller));
                // Unbound legacy records have no provable owner shortcut. Their current team
                // still sees public jobs, and its managers retain private-history oversight.
                if !owns {
                    let mut principal = caller.principal.clone();
                    principal.username.clear();
                    if !member
                        || !can_see_job(
                            &principal,
                            &JobAccess::from_properties(&record.name, &record.properties),
                        )
                    {
                        continue;
                    }
                }
            }
            visible.push(record);
        }
        Ok(visible)
    }

    pub(crate) async fn job_view(&self, record: Record) -> Result<Option<Record>, String> {
        Ok(self.job_views(vec![record]).await?.pop())
    }

    pub(crate) async fn team_id(&self, name: &str) -> Result<Option<Uuid>, String> {
        Ok(self
            .instance
            .get_team(name)
            .await
            .map_err(|_| "Could not resolve the registered team identity.")?
            .and_then(|t| t.id.and_then(|id| Uuid::parse_str(&id).ok())))
    }

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
                record.schedule = crate::executor_jobs::JobSchedule::new(
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

    async fn job_callback_url(&self, record: &Record) -> Result<String, String> {
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

    /// Only an explicit start with the canonical lease held may restore a missing definition.
    /// A failed or malformed existence read never authorizes creation or local fallback.
    pub(crate) async fn start_job(&self, record: &mut Record, api: &APIConnector) -> SuccessTuple {
        match remote_job_exists(api, &record.remote_name).await {
            Ok(true) => return api.start_job(&record.remote_name).await,
            Err(error) => return SuccessTuple::fail(error),
            Ok(false) => {}
        }
        match self.restore_job(record).await {
            Ok(result) => result,
            Err(error) => {
                self.suspend_job_context(record, false).await;
                SuccessTuple::fail(error)
            }
        }
    }

    /// Starting an already running process is idempotent. Recheck its original source without
    /// replacing its credential file, generation or revision underneath that live process.
    pub(crate) async fn keep_running_job(&self, record: &Record) -> SuccessTuple {
        let checked = async {
            let state = self.state()?;
            if let Some(grant) = record.callback {
                let identity = admit_job(self, &state, record, grant).await?;
                self.confirm_job_sources(record, &identity.caller()).await?;
                callbacks::load_identity(&state, grant, launch(record)?)
                    .await
                    .map_err(|_| "The running job's original authority is inactive.".to_string())?;
                if record.callback_suspended {
                    return Err("The running job's original authority is suspended.".into());
                }
            } else if !state.config.no_auth {
                return Err("The running job has no provable original source; stop it and recreate it explicitly.".into());
            }
            Ok::<_, String>(())
        }.await;
        match checked {
            Ok(()) => SuccessTuple::ok(format!("Job '{}' is already running.", record.name)),
            Err(error) => {
                self.suspend_job_context(record, false).await;
                SuccessTuple::fail(error)
            }
        }
    }

    async fn restore_job(&self, record: &mut Record) -> Result<SuccessTuple, String> {
        let canonical = Catalog::new(&self.root)
            .get(&record.name)?
            .filter(|current| {
                current.remote_name == record.remote_name
                    && current.callback == record.callback
                    && current.context_revision == record.context_revision
                    && !current.callback_suspended
            })
            .ok_or("This canonical job was removed, replaced or suspended.")?;
        *record = canonical;
        let grant = record.callback.ok_or(
            "The missing definition has no authenticated original source; recreate the job explicitly.",
        )?;
        if record.context_revision <= 1 || record.sysargs.is_empty() {
            return Err(
                "The missing definition needs a fresh explicit launch revision and stored command."
                    .into(),
            );
        }
        let state = self.state()?;
        let identity = admit_job(self, &state, record, grant).await?;
        let caller = identity.caller();
        let actor = Arc::new(caller);
        let api = context::scope(Some(actor), self.prepare_job_executor(record)).await?;
        // Preparation or policy checks may rebuild a runtime. Rotate only this already-issued
        // revision again, rechecking the original registration and current permissions.
        self.deliver_job_context(record, false).await?;
        let _ = admit_job(self, &state, record, grant).await?;
        let mut properties = record.properties.clone();
        for key in [
            meerschaum_core::job_access::TEAM_EXECUTION_KEY,
            meerschaum_jobs::config_files::TEAM_SCOPE_PROPERTY,
            meerschaum_core::job_access::OWNER_BINDING_KEY,
            meerschaum_core::job_access::TEAM_ID_KEY,
            environment::JOB_CONTEXT_PROPERTY,
        ] {
            properties.shift_remove(key);
        }
        properties.insert(
            environment::JOB_CONTEXT_PROPERTY.into(),
            launch(record)?.to_string().into(),
        );
        if let Some(Value::Map(execution)) = properties.get_mut("mrsm_execution") {
            execution.insert("kind".into(), "delegated_execution".into());
        }
        // Only the original main-owned name and command are used. Keep the reservation on a
        // failed/lost response: creation might have succeeded and must never be duplicated.
        Ok(api
            .create_job(&record.remote_name, &record.sysargs, Some(properties))
            .await)
    }

    pub(crate) async fn job_lock(&self, record: &Record) -> Result<std::fs::File, String> {
        meerschaum_server::executor::storage::lock(
            self.root.join("executors/job-leases"),
            launch(record)?,
        )
        .await
        .map_err(|_| "The job's launch lease is busy or unavailable.".into())
    }

    /// With the canonical job lease held, recheck its stored authority and rotate delivery.
    /// Explicit restart preserves the ORIGINAL source proof, never adopts the restart caller.
    pub(crate) fn deliver_job_context<'a>(
        &'a self,
        record: &'a mut Record,
        restart: bool,
    ) -> Pin<Box<impl Future<Output = Result<(), String>> + Send + 'a>> {
        self.deliver_job_context_for(record, restart, false)
    }

    pub(crate) fn deliver_scheduled_context<'a>(
        &'a self,
        record: &'a mut Record,
    ) -> Pin<Box<impl Future<Output = Result<(), String>> + Send + 'a>> {
        self.deliver_job_context_for(record, true, true)
    }

    fn deliver_job_context_for<'a>(
        &'a self,
        record: &'a mut Record,
        restart: bool,
        scheduled: bool,
    ) -> Pin<Box<impl Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let Some(previous) = record.callback else {
                return Ok(());
            };
            let state = self.state()?;
            let canonical = Catalog::new(&self.root)
                .get(&record.name)?
                .filter(|r| r.remote_name == record.remote_name && r.callback == record.callback)
                .ok_or("This canonical job was removed or replaced.")?;
            *record = canonical;
            if scheduled
                && !record
                    .schedule
                    .as_ref()
                    .is_some_and(|schedule| schedule.claimed())
            {
                return Err("The scheduled job has no claimed firing.".into());
            }
            // An initial schedule has never delivered revision one to any runtime.
            if !restart && record.schedule.is_some() && record.context_revision == 1 {
                return Err("The saved job is waiting for its first scheduled fire.".into());
            }
            let identity = match admit_job(self, &state, record, previous).await {
                Ok(identity) => identity,
                Err(error) => {
                    // No reissue has occurred. An ambient administrator cannot replace the source.
                    self.suspend_job_context(record, false).await;
                    return Err(error);
                }
            };
            if let Err(error) = Box::pin(self.confirm_job_sources(record, &identity.caller())).await
            {
                self.suspend_job_context(record, false).await;
                return Err(error);
            }
            if restart && record.commit_journal.has_pending() {
                return Err("This graph has unfinished native commit evidence. Do not automatically replay it; inspect its owned runtime and committed/rollback proof before restarting.".into());
            }
            // Preserve the registered identity and command, while resolving relative bounds and
            // obtaining fresh graph metadata for this run from the original creation credential.
            let next_stored = if let (true, Some(GraphOperation::Stored(previous))) =
                (restart, &record.operation)
            {
                if !scheduled {
                    context::refresh_stored_request(&state, &record.name, false)
                        .await
                        .map_err(|error| error.detail_text())?;
                }
                let caller = identity.caller();
                if self.capture_job_sources(record, &caller).await? != record.source_selection {
                    return Err("The stored job's plugin source set changed.".into());
                }
                let fresh =
                    Box::pin(self.capture_stored_pipe(record, &caller, previous.intent.clone()))
                        .await?;
                if fresh.pipe.registration != previous.pipe.registration
                    || fresh.worker_args().map_err(|error| error.detail_text())? != record.sysargs
                {
                    return Err("The saved job's registration or command changed. Recreate the job for the new registration.".into());
                }
                Some(GraphOperation::Stored(fresh))
            } else {
                None
            };
            let unissued_first =
                scheduled && record.context_revision == 1 && record.callback_suspended;
            if restart && record.profile.private_runtime() && !unissued_first {
                self.retire_previous_job_generation(record, identity.caller())
                    .await?;
            }
            let mut grant = if restart {
                callbacks::resume(&state, previous, launch(record)?).await
            } else {
                callbacks::renew(&state, previous, launch(record)?).await
            }
            .map_err(|_| "The job's original callback authority is unavailable or revoked.")?;
            if restart {
                let mut candidate = record.clone();
                if let Some(operation) = next_stored {
                    candidate.operation = Some(operation);
                }
                candidate.callback = Some(grant.id);
                candidate.context_revision = candidate
                    .context_revision
                    .checked_add(1)
                    .ok_or("The job's launch revision is exhausted.")?;
                if let Err(error) = self
                    .issue_operation_windows(&mut candidate, &grant, None)
                    .await
                {
                    let _ = callbacks::revoke(&state, grant.id).await;
                    return Err(error);
                }
                if candidate.operation.is_some() {
                    let old_binding =
                        meerschaum_server::executor::pipe_operation::CallbackBinding {
                            grant: previous,
                            launch: launch(record)?,
                            context_revision: record.context_revision,
                        };
                    grant = match callbacks::bind_graph_origin(
                        &state,
                        grant.id,
                        grant.launch_id,
                        candidate.context_revision,
                        Some(old_binding),
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
                let directory = self.root.clone();
                let expected = record.clone();
                let operation = candidate.operation.clone();
                let next = grant.id;
                let transition = tokio::task::spawn_blocking(move || {
                    Catalog::new(&directory).replace_callback(&expected, previous, next, operation)
                })
                .await;
                record.context_revision = match transition {
                    Ok(Ok(revision)) => revision,
                    result => {
                        let _ = callbacks::revoke(&state, grant.id).await;
                        return Err(match result {
                            Ok(Err(error)) => error,
                            _ => "The canonical callback transition was interrupted.".into(),
                        });
                    }
                };
                record.operation = candidate.operation;
                record.callback = Some(grant.id);
                let _ = callbacks::revoke(&state, previous).await;
            }
            let identity = match admit_job(self, &state, record, grant.id).await {
                Ok(identity) => identity,
                Err(error) => {
                    self.suspend_job_context(record, false).await;
                    return Err(error);
                }
            };
            let caller = identity.caller();
            let actor = context::actor(&caller);
            let owner_id = if let Some(username) = caller.username() {
                self.instance
                    .get_user_id(&User::new(username, None))
                    .await
                    .map_err(|_| "Could not verify the job's registered source identity.")?
                    .filter(|id| !id.is_null())
                    .map(|id| id.py_str())
            } else {
                None
            };
            let extension_catalog_sha256 = record
                .extension
                .as_ref()
                .map(|extension| {
                    extension
                        .validated()
                        .map_err(|error| error.detail_text())?
                        .source_digest()
                        .map_err(|error| error.to_string())
                })
                .transpose()?;
            let mut update = environment::Update {
                metadata_deadline: record
                    .discovery
                    .as_ref()
                    .map(|discovery| discovery.deadline),
                extension: record.extension.clone(),
                registration: record.connector_registration.clone(),
                discovery_catalog_sha256: record
                    .discovery
                    .as_ref()
                    .map(|discovery| discovery.catalog_sha256())
                    .transpose()
                    .map_err(|error| error.detail_text())?
                    .or(extension_catalog_sha256)
                    .or_else(|| {
                        if !matches!(
                            record.profile,
                            environment::Profile::Api | environment::Profile::Team
                        ) {
                            return None;
                        }
                        match &record.source_selection {
                            Some(
                                meerschaum_server::executor::registration::Sources::Archives {
                                    catalog,
                                },
                            ) => serde_json::to_vec(catalog)
                                .ok()
                                .map(|bytes| meerschaum_server::executor::plugins::sha256(&bytes)),
                            _ => None,
                        }
                    }),
                id: launch(record)?,
                revision: record.context_revision,
                owner: actor.owner,
                owner_id,
                profile: record.profile,
                target: environment::Target::Job {
                    name: record.remote_name.clone(),
                },
                callback_url: self.job_callback_url(record).await?,
                credential: job_credential(self, &state, record, grant.id).await?,
            };
            // Metadata discovery can return here while its submitting job is being planned.
            // Keep the delivery future out of the nested caller-scope stack frames.
            let result = context::scope(Some(Arc::new(caller)), Box::pin(async {
                if record.placement == JobPlacement::HostApi {
                    let identity = admit_job(self, &state, record, grant.id).await?;
                    update.credential = job_credential(self, &state, record, grant.id).await?;
                    let jobs = self.host_jobs()?;
                    let state = state.clone();
                    let caller = identity.caller();
                    let config = self.config()?;
                    let environment = self.options.env.clone();
                    let discovery = record.discovery.clone();
                    let accepted_instances=record.operation.as_ref()
                        .and_then(|operation|operation.snapshot())
                        .map(meerschaum_server::executor::pipe_operation::Snapshot::transport_instances)
                        .transpose()?.unwrap_or_default();
                    return tokio::task::spawn_blocking(move || {
                        if let Some(discovery) = discovery {
                            jobs.update_metadata(&state, &caller, &config, &environment, update, &discovery)
                        } else {
                            jobs.update(&state, &caller, &config, &environment, update, &accepted_instances)
                        }
                    })
                    .await
                    .map_err(|_| "The host context preparation was interrupted.".to_string())?;
                }
                let api = self.prepare_job_executor(record).await?;
                // Setup may construct a runtime. Do not adopt stale source authority or send a
                // credential that expired while preparing that runtime.
                if let Err(error) = admit_job(self, &state, record, grant.id).await {
                    self.suspend_job_context(record, false).await;
                    return Err(error);
                }
                let identity = admit_job(self, &state, record, grant.id).await?;
                Box::pin(self.confirm_job_sources(record, &identity.caller())).await?;
                update.credential = job_credential(self, &state, record, grant.id).await?;
                match put_context(&api, &update).await? {
                    ContextDelivery::Applied => Ok(()),
                    ContextDelivery::MissingSources => {
                        Box::pin(self.stage_job_sources(record, &identity, &api))
                            .await?
                            .ok_or(
                                "The missing source catalog does not belong to this exact API job.",
                            )?;
                        // Pulling may take time. Recheck the original source and rotate only the
                        // same signed selector; no other refusal authorizes this retry.
                        let _ = admit_job(self, &state, record, grant.id).await?;
                        update.credential = job_credential(self, &state, record, grant.id).await?;
                        match put_context(&api, &update).await? {
                        ContextDelivery::Applied => Ok(()),
                        ContextDelivery::MissingSources => Err(
                            "The exact source catalog is still unavailable after its bounded pull."
                                .into(),
                        ),
                    }
                    }
                }
            }))
            .await;
            if restart && result.is_err() {
                self.suspend_job_context(record, false).await;
            }
            result
        })
    }

    /// Called only under the canonical job lease, after the original command was freshly
    /// admitted and start preflight confirmed stopped/missing. Retire the old revision before
    /// minting its successor, so rapid completed jobs cannot accumulate old readable grants.
    async fn retire_previous_job_generation(
        &self,
        record: &Record,
        caller: Access,
    ) -> Result<(), String> {
        let id = launch(record)?;
        if record.remote_name != format!("mrsm-{}", id.simple()) || record.context_revision == 0 {
            return Err("This job has no canonical previous generation.".into());
        }
        if record.placement == JobPlacement::HostApi {
            return self.host_jobs()?.revoke(id, record.context_revision).await;
        }
        context::scope(Some(Arc::new(caller)), async {
            let api = self.prepare_id(record.team_id).await?;
            let response = api.make_request(Method::Delete,
                &format!("{}{}", meerschaum_server::executor::gateway::CONTEXTS_PATH, id),
                RequestOptions { body: Body::Json(serde_json::json!({"revision":record.context_revision}).to_string()),
                    timeout: Some(std::time::Duration::from_secs(8)), ..Default::default() })
                .await.map_err(|_| "The executor did not confirm retirement of the previous API generation.")?;
            if response.status != 200 || response.json().ok().and_then(|body| body["success"].as_bool()) != Some(true) {
                return Err("The executor did not prove cleanup of the previous API process tree. Retry after it stops. If its lifecycle proof is unavailable, an administrator must retire the owned execution runtime and confirm its old processes are gone before recovery; removing metadata is insufficient.".into());
            }
            Ok(())
        }).await
    }

    /// Retire runtime delivery, retaining an inactive original proof only for a stopped definition.
    pub(crate) async fn suspend_job_context(&self, record: &Record, deleting: bool) {
        let Some(id) = record.callback else {
            return;
        };
        if let Ok(state) = self.state() {
            if deleting {
                let _ = callbacks::revoke(&state, id).await;
            } else if let Ok(launch) = launch(record)
                && callbacks::suspend(&state, id, launch).await.is_ok()
            {
                let _ = Catalog::new(&self.root).update(record, |r| {
                    if r.callback == record.callback {
                        r.callback_suspended = true;
                    }
                });
            }
        }
        let Ok(launch) = launch(record) else {
            return;
        };
        if record.placement == JobPlacement::HostApi {
            if let Ok(jobs) = self.host_jobs() {
                let _ = jobs.revoke(launch, record.context_revision).await;
            }
            return;
        }
        context::scope(None, async {
            if let Ok(api) = self.prepare_id(record.team_id).await {
                let _ = api
                    .make_request(
                        Method::Delete,
                        &format!(
                            "{}{}",
                            meerschaum_server::executor::gateway::CONTEXTS_PATH,
                            launch
                        ),
                        RequestOptions {
                            body: Body::Json(
                                serde_json::json!({"revision": record.context_revision})
                                    .to_string(),
                            ),
                            timeout: Some(std::time::Duration::from_secs(5)),
                            ..Default::default()
                        },
                    )
                    .await;
            }
        })
        .await;
    }

    pub(super) async fn maintain_jobs(self: &Arc<Self>) {
        self.expire_host_jobs();
        let Ok(records) = Catalog::new(&self.root).records() else {
            return;
        };
        let mut tasks = tokio::task::JoinSet::new();
        for mut record in records
            .into_values()
            .filter(|r| r.callback.is_some() && !r.callback_suspended)
        {
            if tasks.len() >= 64 {
                let _ = tasks.join_next().await;
            }
            let controller = self.clone();
            tasks.spawn(async move {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    let Ok(_lock) = controller.job_lock(&record).await else {
                        return;
                    };
                    let Ok(Some(current)) = Catalog::new(&controller.root).get(&record.name) else {
                        return;
                    };
                    if current.remote_name != record.remote_name || current.callback_suspended {
                        return;
                    }
                    record = current;
                    let Ok(state) = controller.state() else {
                        return;
                    };
                    if let (Some(id), Ok(launch)) = (record.callback, launch(&record)) {
                        match callbacks::load_identity(&state, id, launch).await {
                            Err(error) if error.status.is_client_error() => {
                                controller.suspend_job_context(&record, false).await;
                                return;
                            }
                            Err(_) => return,
                            Ok(_) => {}
                        }
                    }
                    // A controlled rebuild may leave a stopped job waiting for its fresh
                    // same-revision context. Deliver before interpreting that state as completion.
                    if controller
                        .deliver_job_context(&mut record, false)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if record.placement == JobPlacement::HostApi {
                        let Ok(jobs) = controller.host_jobs() else {
                            return;
                        };
                        let metadata =
                            context::scope(None, jobs.backend().try_get(&record.remote_name)).await;
                        if let Some(metadata) = metadata.ok().flatten()
                            && chrono::Utc::now().timestamp()
                                >= record.admitted_at.saturating_add(5)
                        {
                            if super::completed_result(&metadata).is_some() {
                                let _ = Catalog::new(&controller.root).observe_stopped(&record);
                                controller.suspend_job_context(&record, false).await;
                            } else if metadata["completion_unavailable"] == true {
                                // Missing/dead supervisor proof never keeps source authority
                                // alive or marks a write completed. Preserve uncertain history.
                                controller.suspend_job_context(&record, false).await;
                            }
                        }
                        return;
                    }
                    let Ok(api) = context::scope(None, controller.prepare_id(record.team_id)).await
                    else {
                        return;
                    };
                    let Ok(metadata) = api.get_job_metadata(&record.remote_name).await else {
                        return;
                    };
                    if chrono::Utc::now().timestamp() < record.admitted_at.saturating_add(5) {
                        return;
                    }
                    let Ok(metadata) = serde_json::to_value(metadata) else {
                        return;
                    };
                    if super::completed_result(&metadata).is_some() {
                        let _ = Catalog::new(&controller.root).observe_stopped(&record);
                        controller.suspend_job_context(&record, false).await;
                    } else if metadata["completion_unavailable"] == true {
                        controller.suspend_job_context(&record, false).await;
                    }
                })
                .await;
            });
        }
        while tasks.join_next().await.is_some() {}
    }
}

fn admit_job<'a>(
    controller: &'a Controller,
    state: &'a meerschaum_server::AppState,
    record: &'a Record,
    grant: Uuid,
) -> Pin<Box<impl Future<Output = Result<callbacks::Identity, String>> + Send + 'a>> {
    Box::pin(async move {
        if let Some(extension) = &record.extension {
            if record.profile != environment::Profile::Api
                || !record.extension_placement()
                || record.discovery.is_some()
                || record.operation.is_some()
                || record.action_request
                || record.connector_registration.is_some()
                || record.source_selection.as_ref() != Some(&extension.sources())
            {
                return Err("The extension lost its exact immutable operation.".into());
            }
            let current = callbacks::load_admission_identity(state, grant, launch(record)?)
                .await
                .map_err(|error| error.detail_text())?;
            controller
                .confirm_extension_sources(extension, &current.caller())
                .await?;
            let identity = extension
                .admit(
                    state,
                    grant,
                    launch(record)?,
                    extension.team_id(),
                    &record.sysargs,
                )
                .await
                .map_err(|error| error.detail_text())?;
            if record.owner_binding.is_some() && record.owner_binding != identity.owner_binding() {
                return Err("The extension belongs to another registered source.".into());
            }
            super::source_selection::agrees(record.source_selection.as_ref(), &identity)?;
            return Ok(identity);
        }
        if let Some(registration) = &record.connector_registration {
            if record.profile != environment::Profile::Api
                || record.discovery.is_some()
                || record.operation.is_some()
                || record.action_request
            {
                return Err(
                    "The connector registration lost its exact API-only descriptor.".into(),
                );
            }
            let identity = registration
                .admit(
                    state,
                    grant,
                    launch(record)?,
                    (!record.team_id.is_nil()).then_some(record.team_id),
                    &record.sysargs,
                )
                .await
                .map_err(|error| error.detail_text())?;
            if record.owner_binding.is_some() && record.owner_binding != identity.owner_binding() {
                return Err(
                    "The connector registration belongs to another original source.".into(),
                );
            }
            if record.source_selection.as_ref() != Some(&registration.sources) {
                return Err("The registration lost its immutable Main source selection.".into());
            }
            super::source_selection::agrees(record.source_selection.as_ref(), &identity)?;
            return Ok(identity);
        }
        if let Some(discovery) = &record.discovery {
            if record.profile != environment::Profile::Metadata
                || !record.discovery_placement()
                || record.operation.is_some()
                || record.action_request
            {
                return Err("Metadata discovery lost its private operation ceiling.".into());
            }
            let identity = discovery
                .admit(
                    state,
                    grant,
                    launch(record)?,
                    record.team_id,
                    &record.sysargs,
                )
                .await
                .map_err(|error| error.detail_text())?;
            if record.owner_binding.is_some() && record.owner_binding != identity.owner_binding() {
                return Err("The discovery belongs to another registered original source.".into());
            }
            return Ok(identity);
        }
        if record.profile == environment::Profile::Metadata {
            return Err("A metadata profile requires Main's typed discovery operation.".into());
        }
        let admitted = admit_job_source(state, record, grant, launch(record)?).await;
        let admitted = admitted.map_err(|error| error.detail_text())?;
        if admitted.profile != record.profile {
            return Err("The job's source requires a different credential profile; recreate the job explicitly.".into());
        }
        if record.owner_binding.is_some()
            && record.owner_binding != admitted.identity.owner_binding()
        {
            return Err("The canonical job belongs to another registered source.".into());
        }
        super::source_selection::agrees(record.source_selection.as_ref(), &admitted.identity)?;
        if record.team_pipe_authority.is_some()
            || (record.profile == environment::Profile::Team
                && contains_uncaptured_graph(&record.sysargs)?)
        {
            super::team_pipe_job::admit(record, &admitted.identity)?;
        }
        if let Some(operation) = &record.operation {
            if admitted.profile != environment::Profile::Api {
                return Err("The typed pipe operation lost its API-only ceiling.".into());
            }
            if let GraphOperation::Stored(job) = operation {
                let source = admitted.identity.source_access();
                let allowed = meerschaum_server::auth::is_user_allowed_to_execute(
                    state,
                    source.auth.as_ref(),
                )
                .await;
                if !allowed.success {
                    return Err(allowed.message);
                }
                if job
                    .intent
                    .cadence()
                    .map_err(|error| error.detail_text())?
                    .is_some()
                {
                    // The original credential must still authorize the submitted repeat flags.
                    // The one-shot worker's own grant only needs jobs:execute and pipes:write.
                    meerschaum_server::execute_access::check(state, source, job.intent.original())
                        .await
                        .map_err(|error| error.detail_text())?;
                }
            }
            operation
                .authorize(state, &admitted.identity.caller(), &record.sysargs)
                .await
                .map_err(|error| error.detail_text())?;
        }
        Ok(admitted.identity)
    })
}

// The issued Pipeline already has Main's exact dependency and window proof. Revalidating
// the generic CLI selection would run the metadata planner again inside every held write.
// Keep the legacy planner future out of the selected admission path's frame.
fn admit_job_source<'a>(
    state: &'a meerschaum_server::AppState,
    record: &'a Record,
    grant: Uuid,
    launch: Uuid,
) -> Pin<
    Box<
        dyn Future<Output = Result<admission::Admitted, meerschaum_server::error::ApiError>>
            + Send
            + 'a,
    >,
> {
    let team = (!record.team_id.is_nil()).then_some(record.team_id);
    let captured_pipeline = record.operation.as_ref().is_some_and(|operation| {
        operation.pipeline_selection().is_some()
            && operation
                .snapshot()
                .is_some_and(|snapshot| snapshot.main_instructions().is_some())
    });
    if captured_pipeline
        && record.profile == environment::Profile::Api
        && (record.placement == JobPlacement::HostApi || team.is_some())
    {
        return Box::pin(async move {
            let identity = admission::typed_api_identity(state, grant, launch, team).await?;
            let binding = meerschaum_server::executor::pipe_operation::CallbackBinding {
                grant,
                launch,
                context_revision: record.context_revision,
            };
            if identity.grant.graph_tracking != 1
                || identity.grant.graph_pending
                || identity.grant.graph_origin.as_ref() != Some(&binding)
            {
                return Err(meerschaum_server::error::ApiError::new(
                    403,
                    "The captured Pipeline lost its issued Main graph binding.",
                ));
            }
            // admit_job still rechecks the exact command, Pipeline registration and every
            // captured member with identity.caller(), which retains the original scope ceiling.
            Ok(admission::Admitted {
                profile: environment::Profile::Api,
                identity,
            })
        });
    }
    if record.placement == JobPlacement::HostApi {
        Box::pin(admission::host_job(
            state,
            grant,
            launch,
            team,
            &record.sysargs,
        ))
    } else {
        Box::pin(admission::job(
            state,
            grant,
            launch,
            record.team_id,
            &record.sysargs,
        ))
    }
}

async fn job_credential(
    controller: &Controller,
    state: &meerschaum_server::AppState,
    record: &Record,
    grant: Uuid,
) -> Result<meerschaum_connector_api::models::ExecutionCredential, String> {
    // Discovery retains original source proof on Main, never a reusable callback bearer.
    let _ = admit_job(controller, state, record, grant).await?;
    if let Some(discovery) = &record.discovery {
        return Ok(discovery.credential());
    }
    callbacks::credential(state, grant, launch(record)?)
        .await
        .map_err(|_| "Could not rotate the job's original callback credential.".into())
}

async fn remote_job_exists(api: &APIConnector, name: &str) -> Result<bool, String> {
    let response = api
        .make_request(
            Method::Get,
            &format!("/jobs/{name}/exists"),
            RequestOptions {
                timeout: Some(std::time::Duration::from_secs(5)),
                ..Default::default()
            },
        )
        .await
        .map_err(|_| "The executor did not confirm whether the canonical job exists.")?;
    if response.status != 200 {
        return Err("The executor did not confirm whether the canonical job exists.".into());
    }
    response
        .json()
        .map_err(|_| "The executor returned invalid canonical job existence metadata.")?
        .as_bool()
        .ok_or_else(|| "The executor returned invalid canonical job existence metadata.".into())
}

enum ContextDelivery {
    Applied,
    MissingSources,
}

async fn put_context(
    api: &APIConnector,
    update: &environment::Update,
) -> Result<ContextDelivery, String> {
    let response = api
        .make_request(
            Method::Put,
            &format!(
                "{}{}",
                meerschaum_server::executor::gateway::CONTEXTS_PATH,
                update.id
            ),
            RequestOptions {
                body: Body::Json(
                    serde_json::to_string(update)
                        .map_err(|_| "Could not encode the job environment.")?,
                ),
                timeout: Some(std::time::Duration::from_secs(5)),
                ..Default::default()
            },
        )
        .await
        .map_err(|_| "The executor did not accept the job environment.")?;
    if response.status == 409
        && response
            .json()
            .ok()
            .and_then(|body| body["detail"]["code"].as_str().map(str::to_string))
            .as_deref()
            == Some(meerschaum_server::executor::gateway::SOURCE_CATALOG_MISSING)
    {
        return Ok(ContextDelivery::MissingSources);
    }
    if response.status != 200 {
        return Err("The executor refused the job environment.".into());
    }
    Ok(ContextDelivery::Applied)
}

#[cfg(test)]
mod missing_definition_tests {
    #[test]
    fn ordinary_show_remains_available_and_sync_steps_require_graph_capture() {
        let args = |words: &[&str]| {
            words
                .iter()
                .map(|word| word.to_string())
                .collect::<Vec<_>>()
        };
        assert!(!super::contains_uncaptured_graph(&args(&["show", "version"])).unwrap());
        assert!(
            super::contains_uncaptured_graph(&args(&["show", "version", "+", "verify", "pipes"]))
                .unwrap()
        );
        assert!(
            super::contains_uncaptured_graph(&args(&["sync", "pipes", "--begin=0", "--end=2"]))
                .unwrap()
        );
    }

    use super::*;
    use axum::response::IntoResponse;

    struct Authentication;
    impl meerschaum_connector_api::client::authentication::RequestAuthentication for Authentication {
        fn authorization(
            &self,
            _: meerschaum_connector_api::client::authentication::Request<'_>,
        ) -> meerschaum_connector_api::traits::ConnectorResult<String> {
            Ok("Bearer synthetic-fixture".into())
        }
    }

    #[tokio::test]
    async fn only_explicit_boolean_absence_authorizes_definition_recovery() {
        let reply = Arc::new(std::sync::Mutex::new((200u16, "false".to_string())));
        let handler = reply.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let router = axum::Router::new().route(
            "/jobs/{name}/exists",
            axum::routing::get(move || {
                let reply = handler.clone();
                async move {
                    let (status, body) = reply.lock().unwrap().clone();
                    (axum::http::StatusCode::from_u16(status).unwrap(), body).into_response()
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let api = APIConnector::new(
            Some("absence-fixture"),
            Value::from_json_str(&serde_json::json!({"host":"127.0.0.1", "port":port}).to_string())
                .unwrap()
                .to_map(),
        )
        .unwrap()
        .with_request_authentication(Arc::new(Authentication), 1024)
        .unwrap();
        assert!(!remote_job_exists(&api, "mrsm-fixture").await.unwrap());
        *reply.lock().unwrap() = (200, "true".into());
        assert!(remote_job_exists(&api, "mrsm-fixture").await.unwrap());
        for (status, body) in [
            (200, "{}"),
            (200, "null"),
            (200, "\"false\""),
            (200, "invalid"),
            (204, ""),
            (401, "false"),
            (403, "false"),
            (404, "false"),
            (503, "false"),
        ] {
            *reply.lock().unwrap() = (status, body.into());
            assert!(
                remote_job_exists(&api, "mrsm-fixture").await.is_err(),
                "HTTP {status}, {body}"
            );
        }
        server.abort();
        let _ = server.await;
        assert!(remote_job_exists(&api, "mrsm-fixture").await.is_err());
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    use meerschaum_connector::InstanceConnector;
    use meerschaum_core::Map;
    use meerschaum_core::access::{Principal, Role, Team, TeamRole};
    use meerschaum_core::config::{Config, ConfigOptions};
    use meerschaum_server::access::Access;
    use meerschaum_server::auth;
    use meerschaum_server::backends::JobsBackend;

    pub(super) async fn fixture(
        root: &std::path::Path,
    ) -> (Arc<Controller>, Arc<dyn InstanceConnector>) {
        let mut options = ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.display().to_string()),
            (
                "MRSM_PLUGINS_DIR".into(),
                root.join("plugins").display().to_string(),
            ),
        ]);
        options.home = root.into();
        options.allow_writes = false;
        let snapshot = Value::from_json_str(&serde_json::json!({"meerschaum":{"instance":"sql:main", "connectors":{"sql":{"main":{"flavor":"sqlite", "database":root.join("identity.db")}}}}}).to_string()).unwrap().to_map();
        let config = Config::new(options.clone()).unwrap();
        config.set_config(snapshot.clone());
        let session = crate::native::Session::with_runtime(tokio::runtime::Handle::current());
        session.set_python_worker(false);
        let instance = tokio::task::spawn_blocking(move || {
            session.instance("sql:main", &config).unwrap().conn()
        })
        .await
        .unwrap();
        (
            Arc::new(Controller::new(
                root.into(),
                instance.clone(),
                options,
                snapshot,
            )),
            instance,
        )
    }

    fn caller(user: &User, id: &Value, team: &Team, role: TeamRole) -> Access {
        let mut user = user.clone();
        user.user_id = Some(id.clone());
        let mut access = Access::everything();
        access.no_auth = false;
        access.principal = Principal::new(
            &user.username,
            false,
            Role::Operator.permissions(),
            vec![(team.name.clone(), role)],
        );
        access.primary_team = team.name.clone();
        access.team_records = vec![team.clone()];
        access.auth = Some(auth::Principal::User(user));
        access
    }

    fn record(name: &str, team: &Team, owner: &User, id: &Value) -> Record {
        let mut properties = Map::new();
        meerschaum_core::job_access::JobAccess::stamp(
            &mut properties,
            Some(&owner.username),
            Some(&team.name),
        );
        let mut record = Record::new(
            name,
            Uuid::parse_str(team.id.as_deref().unwrap()).unwrap(),
            vec!["show".into(), "version".into()],
            properties,
        );
        record.owner_binding = Some(callbacks::OwnerBinding::User {
            username: owner.username.clone(),
            user_id: id.clone(),
        });
        record
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn names_are_projected_by_team_id_and_deleted_names_never_adopt_history() {
        let root = tempfile::tempdir().unwrap();
        let (controller, instance) = fixture(root.path()).await;
        let mut alice = User::new("alice", Some("synthetic-pass"));
        let mut bob = User::new("bob", Some("synthetic-pass"));
        let mut charlie = User::new("charlie", Some("synthetic-pass"));
        for user in [&mut alice, &mut bob, &mut charlie] {
            assert!(instance.register_user(user).await.success);
        }
        let alice_id = instance.get_user_id(&alice).await.unwrap().unwrap();
        let bob_id = instance.get_user_id(&bob).await.unwrap().unwrap();
        let charlie_id = instance.get_user_id(&charlie).await.unwrap().unwrap();
        let mut harbor = Team::new("Harbor");
        harbor.members.insert("alice".into(), TeamRole::Member);
        harbor.members.insert("bob".into(), TeamRole::Member);
        assert!(instance.register_team(&mut harbor).await.success);
        let public = record("nightly", &harbor, &alice, &alice_id);
        let private = record("execute-private", &harbor, &alice, &alice_id);
        assert!(instance.rename_team("Harbor", "Quay").await.success);
        let quay = instance.get_team("Quay").await.unwrap().unwrap();
        assert_eq!(quay.id, harbor.id);
        let bob_access = Arc::new(caller(&bob, &bob_id, &quay, TeamRole::Member));
        let visible = context::scope(
            Some(bob_access),
            controller.job_views(vec![public.clone(), private.clone()]),
        )
        .await
        .unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(
            visible[0].properties.get("team").and_then(Value::as_str),
            Some("Quay")
        );
        let manager = Arc::new(caller(&bob, &bob_id, &quay, TeamRole::Manager));
        assert_eq!(
            context::scope(Some(manager), controller.job_views(vec![private.clone()]))
                .await
                .unwrap()
                .len(),
            1
        );
        let mut replacement = Team::new("Harbor");
        replacement
            .members
            .insert("charlie".into(), TeamRole::Member);
        assert!(instance.register_team(&mut replacement).await.success);
        let new_member = Arc::new(caller(
            &charlie,
            &charlie_id,
            &replacement,
            TeamRole::Member,
        ));
        assert!(
            context::scope(
                Some(new_member),
                controller.job_views(vec![public.clone(), private.clone()])
            )
            .await
            .unwrap()
            .is_empty()
        );
        assert!(instance.delete_team("Quay").await.success);
        let mut new_quay = Team::new("Quay");
        new_quay.members.insert("charlie".into(), TeamRole::Manager);
        assert!(instance.register_team(&mut new_quay).await.success);
        assert_ne!(new_quay.id, quay.id);
        let new_manager = Arc::new(caller(&charlie, &charlie_id, &new_quay, TeamRole::Manager));
        assert!(
            context::scope(
                Some(new_manager),
                controller.job_views(vec![public.clone(), private.clone()])
            )
            .await
            .unwrap()
            .is_empty()
        );
        let owner = caller(&alice, &alice_id, &replacement, TeamRole::Member);
        assert_eq!(
            context::scope(
                Some(Arc::new(owner.clone())),
                controller.job_views(vec![private.clone()])
            )
            .await
            .unwrap()
            .len(),
            1
        );
        let mut bounded = owner;
        bounded.principal = bounded.principal.within_team(&replacement.name);
        assert!(
            context::scope(Some(Arc::new(bounded)), controller.job_views(vec![private]))
                .await
                .unwrap()
                .is_empty(),
            "A team-bound credential used historical ownership to escape its UUID boundary."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hidden_remote_placement_never_falls_back_to_a_local_job_of_the_same_name() {
        let root = tempfile::tempdir().unwrap();
        let (controller, instance) = fixture(root.path()).await;
        let mut user = User::new("viewer", Some("synthetic-pass"));
        assert!(instance.register_user(&mut user).await.success);
        let id = instance.get_user_id(&user).await.unwrap().unwrap();
        let mut current = Team::new("Reused");
        current
            .members
            .insert(user.username.clone(), TeamRole::Member);
        assert!(instance.register_team(&mut current).await.success);
        let mut retired = current.clone();
        retired.id = Some(Uuid::new_v4().to_string());
        let name = format!("tpr-shadow-{}", Uuid::new_v4().simple());
        let mut hidden = record(
            &name,
            &retired,
            &User::new("another-owner", None),
            &999.into(),
        );
        hidden.owner_binding = None;
        Catalog::new(root.path()).reserve(hidden).unwrap();
        let marker = root.path().join("must-not-start");
        let mut local = meerschaum_jobs::JobsContext::for_root(root.path())
            .with_launch_environment(vec![("PATH".into(), "/bin:/usr/bin".into())]);
        local.config.executor = Some("local".into());
        local.launcher.supervisor = vec![
            "/bin/sh".into(),
            "-c".into(),
            format!("touch '{}'", marker.display()),
        ];
        let mut dormant = meerschaum_jobs::Job::new(
            &name,
            Some(vec!["show".into(), "version".into()]),
            meerschaum_jobs::JobOptions {
                executor_keys: Some("local".into()),
                ..Default::default()
            },
            &local,
        )
        .unwrap();
        assert!(dormant.daemon().write_properties().success);
        let backend = crate::executor_jobs::DelegatingJobs::new(
            root.path(),
            Arc::new(crate::jobs_backend::LocalJobsBackend::new(local)),
            controller,
        );
        let actor = Arc::new(caller(&user, &id, &current, TeamRole::Member));
        context::scope(Some(actor), async {
            assert!(backend.try_get(&name).await.unwrap().is_none());
            assert!(!backend.exists(&name).await);
            assert!(!backend.start(&name).await.success);
            assert!(backend.logs(&name).await.is_none());
            assert!(backend.monitor_logs(&name).await.is_none());
            assert!(
                !backend
                    .try_list_definitions()
                    .await
                    .unwrap()
                    .contains_key(&name)
            );
        })
        .await;
        assert!(
            !marker.exists(),
            "A hidden delegated name started its local bystander."
        );
    }
}
