//! Native inference advances one canonical operation member without adopting peer edits.
use super::*;
use crate::executor_jobs::{Catalog, Record};
use meerschaum_connector::mutation::MetadataInference;
use meerschaum_server::access::{Access, guard};
use meerschaum_server::executor::{admission, callbacks, environment};

impl Controller {
    pub(crate) async fn accept_pipe_inference(
        &self,
        caller: &Access,
        change: MetadataInference,
    ) -> Result<bool, String> {
        change.validate()?;
        let Some(meerschaum_server::auth::Principal::Execution(parent)) = &caller.auth else {
            return Ok(false);
        };
        if parent.grant.graph_tracking != 1 || parent.grant.graph_pending {
            return Err("Native inference has unknown or incomplete graph provenance.".into());
        }
        let Some(origin) = parent.grant.graph_origin.clone() else {
            return Ok(false);
        };
        let expected_origin = origin.clone();
        let grant = parent.grant.id;
        let launch = parent.grant.launch_id;
        let catalog = Catalog::new(&self.root);
        let (record, issuer): (Record, Record) = tokio::task::spawn_blocking(move || {
            let records = catalog.records_if_available()?;
            let consumer = records
                .values()
                .find(|record| {
                    record.callback == Some(grant)
                        && record.remote_name == format!("mrsm-{}", launch.simple())
                        && !record.callback_suspended
                })
                .cloned()
                .ok_or("The native inference consumer is absent or retired.")?;
            let issuer = records
                .values()
                .find(|record| {
                    record.callback == Some(origin.grant)
                        && record.remote_name == format!("mrsm-{}", origin.launch.simple())
                        && record.context_revision == origin.context_revision
                        && !record.callback_suspended
                })
                .cloned()
                .ok_or("The native inference issuer is absent or retired.")?;
            Ok::<_, String>((consumer, issuer))
        })
        .await
        .map_err(|_| "Canonical inference lookup was interrupted.")??;
        let Some(mut snapshot) = issuer
            .operation
            .as_ref()
            .and_then(|operation| operation.snapshot().cloned())
        else {
            return Err("The inference issuer lost its captured graph snapshot.".into());
        };
        if record.profile != environment::Profile::Api
            || issuer.profile != environment::Profile::Api
        {
            return Err("Inference requires the captured API-only operation.".into());
        }
        if !snapshot.accept_inference(&change)? {
            return Err("The native inference is outside the issuer's writable snapshot.".into());
        }
        let state = self.state()?;
        // This path deliberately checks the source and changed object before advancing the
        // old snapshot. Ordinary admission would compare against metadata we just committed.
        let identity = admission::typed_api_identity(
            &state,
            grant,
            launch,
            (!record.team_id.is_nil()).then_some(record.team_id),
        )
        .await
        .map_err(|error| error.detail_text())?;
        if record.owner_binding.is_some() && record.owner_binding != identity.owner_binding() {
            return Err("The native inference belongs to another registered source.".into());
        }
        if identity.grant.graph_origin.as_ref() != Some(&expected_origin)
            || identity.grant.graph_tracking != 1
            || identity.grant.graph_pending
        {
            return Err("Native inference lost the consumer's original graph lineage.".into());
        }
        let issuer_identity = admission::typed_api_identity(
            &state,
            expected_origin.grant,
            expected_origin.launch,
            (!issuer.team_id.is_nil()).then_some(issuer.team_id),
        )
        .await
        .map_err(|error| error.detail_text())?;
        if issuer_identity.grant.graph_origin.as_ref() != Some(&expected_origin)
            || issuer_identity.grant.graph_tracking != 1
            || issuer_identity.grant.graph_pending
            || issuer.owner_binding.as_ref() != issuer_identity.owner_binding().as_ref()
            || issuer_identity.owner_binding() != identity.owner_binding()
            || issuer.team_id != record.team_id
        {
            return Err("Native inference belongs to another graph issuer/source boundary.".into());
        }
        let mut current = identity.caller();
        current.principal.permissions =
            meerschaum_server::access::intersect(&current.principal.permissions, &caller.scopes());
        self.confirm_job_sources(&issuer, &issuer_identity.caller())
            .await?;
        super::source_selection::agrees(issuer.source_selection.as_ref(), &issuer_identity)?;
        self.confirm_job_sources(&record, &current).await?;
        super::source_selection::agrees(record.source_selection.as_ref(), &identity)?;
        current
            .require(&["pipes:write"], &[])
            .map_err(|error| error.detail_text())?;
        guard::check_connector_teams(&state, &current, change.pipe.instance_keys())
            .map_err(|error| error.detail_text())?;
        let conn = state
            .get_api_connector(Some(change.pipe.instance_keys()))
            .map_err(|error| error.detail_text())?;
        let attributes = conn
            .get_pipe_attributes(&change.pipe)
            .await
            .map_err(|_| "Could not verify committed native inference.")?;
        let registration = conn
            .get_pipe_id(&change.pipe)
            .await
            .map_err(|_| "Could not verify inferred registration.")?
            .map(|id| id.to_value());
        if registration.as_ref() != Some(&change.registration)
            || attributes.get("parameters").and_then(Value::as_map) != Some(&change.after)
        {
            return Err("Native inference differs from the current registered metadata.".into());
        }
        let mut pipe = change.pipe.clone();
        pipe.set_parameters(change.after.clone());
        if !current.can_see_pipe(&pipe) || !current.can_edit(pipe.raw_parameters()) {
            return Err("Native inference lost its current object permission.".into());
        }
        guard::check_source_connector(&state, &current, pipe.connector_keys())
            .map_err(|error| error.detail_text())?;
        guard::check_parameters(
            &state,
            &current,
            &conn,
            &pipe,
            Some(&change.before),
            &change.after,
        )
        .await
        .map_err(|error| error.detail_text())?;
        let fresh = callbacks::load_identity(&state, grant, launch)
            .await
            .map_err(|error| error.detail_text())?;
        super::source_selection::agrees(record.source_selection.as_ref(), &fresh)?;
        let fresh_issuer =
            callbacks::load_identity(&state, expected_origin.grant, expected_origin.launch)
                .await
                .map_err(|error| error.detail_text())?;
        super::source_selection::agrees(issuer.source_selection.as_ref(), &fresh_issuer)?;
        if fresh.grant.graph_origin.as_ref() != Some(&expected_origin)
            || fresh_issuer.grant.graph_origin.as_ref() != Some(&expected_origin)
            || fresh.grant.graph_pending
            || fresh_issuer.grant.graph_pending
            || fresh.owner_binding() != identity.owner_binding()
            || fresh_issuer.owner_binding() != identity.owner_binding()
        {
            return Err("The native inference source changed before its canonical receipt.".into());
        }
        let mut final_source = fresh.caller();
        final_source.principal.permissions = meerschaum_server::access::intersect(
            &final_source.principal.permissions,
            &caller.scopes(),
        );
        final_source
            .require(&["pipes:write"], &[])
            .map_err(|error| error.detail_text())?;
        if !final_source.can_see_pipe(&pipe) || !final_source.can_edit(pipe.raw_parameters()) {
            return Err("Native inference lost its final object permission.".into());
        }
        guard::check_connector_teams(&state, &final_source, pipe.instance_keys())
            .map_err(|error| error.detail_text())?;
        guard::check_source_connector(&state, &final_source, pipe.connector_keys())
            .map_err(|error| error.detail_text())?;
        let catalog = Catalog::new(&self.root);
        tokio::task::spawn_blocking(move || {
            catalog.accept_issued_inference(&record, &issuer, &change)
        })
        .await
        .map_err(|_| "Canonical inference persistence was interrupted.")?
    }
}
