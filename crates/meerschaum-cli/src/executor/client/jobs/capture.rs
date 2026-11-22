//! Capture of a job's stored pipe and Pipeline run, revalidation of its result source, and the
//! pipe-operation reads a live job callback may borrow.

use super::{admit_job, launch};
use crate::executor::client::Controller;
use crate::executor::jobs::{Catalog, JobPlacement, Record};
use meerschaum_server::access::Access;
use meerschaum_server::executor::pipe_operation::{GraphOperation, StoredIntent, StoredPipe};
use meerschaum_server::executor::{callbacks, context, environment};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

impl Controller {
    pub(super) async fn capture_stored_pipe(
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
    pub(super) async fn capture_pipeline_run(
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

    pub(in crate::executor::client) async fn validate_job_result_source(
        &self,
        record: &Record,
    ) -> Result<(), String> {
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
}
