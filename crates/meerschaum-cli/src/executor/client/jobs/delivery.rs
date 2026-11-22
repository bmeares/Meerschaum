//! Delivery of a job's runtime context to its executor under the canonical job lease: rotation,
//! retirement of the previous generation and suspension.

use super::{admit_job, job_credential, launch};
use crate::executor::client::Controller;
use crate::executor::jobs::{Catalog, JobPlacement, Record};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_core::users::User;
use meerschaum_server::access::Access;
use meerschaum_server::executor::pipe_operation::GraphOperation;
use meerschaum_server::executor::{callbacks, context, environment};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

impl Controller {
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
}

pub(super) enum ContextDelivery {
    Applied,
    MissingSources,
}

pub(super) async fn put_context(
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
