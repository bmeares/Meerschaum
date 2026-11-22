//! Admission of a job's original command and source against its callback grant, and the
//! credential minted for an admitted job.

use super::{contains_uncaptured_graph, launch};
use crate::executor::client::Controller;
use crate::executor::jobs::{JobPlacement, Record};
use meerschaum_server::executor::pipe_operation::GraphOperation;
use meerschaum_server::executor::{admission, callbacks, environment};
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

pub(super) fn admit_job<'a>(
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

pub(super) async fn job_credential(
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
