//! Runtime leases and run history for a captured graph on a registered self-API alias.
use super::{SCOPES, registry::RegisteredObject, selection::SelectedEngine};
use crate::{
    AppState,
    access::{self, Access},
    auth::Principal,
    error::ApiError,
    executor::pipe_operation::{CallbackBinding, ReadAccess},
    routes::Ctx,
};
use axum::http::HeaderMap;
use meerschaum_connector::mutation::NativeTarget;
use meerschaum_connector_api::models::LeaseRequest;
use serde_json::Value as Json;

#[derive(Clone, PartialEq, Eq)]
pub(super) struct RuntimeGraph {
    consumer: CallbackBinding,
    issuer: CallbackBinding,
}

fn denied() -> ApiError {
    ApiError::new(
        403,
        "The engine request is outside its captured graph runtime.",
    )
}

impl RuntimeGraph {
    pub(super) async fn fresh(
        &self,
        selected: &SelectedEngine,
        state: &AppState,
        headers: &HeaderMap,
        scopes: &[&str],
    ) -> Result<(ReadAccess, Access), ApiError> {
        let original = access::require_fresh(state, headers, scopes).await?;
        let started = std::time::Instant::now();
        tracing::debug!(target: "meerschaum_server::engine_runtime", "Refreshing admitted runtime graph");
        let mut graph = state
            .actions
            .pipe_operation_read(&original)
            .await
            .map_err(ApiError::unavailable)?
            .ok_or_else(denied)?;
        if self.consumer != graph.binding
            || self.issuer != graph.issuer
            || !graph.permits_instance(selected.logical())
            || graph.operation.is_closed()
        {
            return Err(denied());
        }
        let caller = selected
            .fresh_headers_inner(state, headers, scopes, Some(&graph.binding))
            .await?;
        graph.confirm_request(state, &original, &caller)?;
        tracing::debug!(target: "meerschaum_server::engine_runtime", elapsed_ms = started.elapsed().as_millis(), "Confirmed runtime graph and service");
        Ok((graph, caller))
    }

    pub(super) fn target(
        graph: &ReadAccess,
        reference: &str,
        object: &RegisteredObject,
    ) -> Result<NativeTarget, ApiError> {
        let target = NativeTarget::from_registered(&object.logical, &object.physical)
            .map_err(|error| ApiError::new(409, error))?;
        let member = graph
            .operation
            .snapshot()
            .and_then(|snapshot| snapshot.members().get(reference))
            .ok_or_else(denied)?;
        if target.logical_reference() != reference
            || target.registration() != member.registration.py_str()
        {
            return Err(denied());
        }
        graph
            .write_window(&object.logical)?
            .for_native_target(&target)
            .map_err(|error| ApiError::new(409, error))?;
        Ok(target)
    }

    pub(super) fn history_rows(&self, metric: &str, rows: &[Json]) -> Result<(), ApiError> {
        if rows.is_empty() || rows.len() > 10_000 {
            return Err(denied());
        }
        let schema = meerschaum_connector_api::engine_pipes::engine_dtypes_context(metric)
            .ok_or_else(denied)?;
        let prefix = format!("mrsm-graph-{}-", self.consumer.launch);
        for row in rows {
            let run = row["run_id"]
                .as_str()
                .and_then(|id| id.strip_prefix(&prefix))
                .and_then(|id| uuid::Uuid::parse_str(id).ok());
            if run.is_none_or(|id| id.get_version_num() != 4)
                || !row.as_object().is_some_and(|fields| {
                    fields
                        .keys()
                        .all(|key| schema.explicit_dtypes.contains_key(key))
                })
                || (metric == "engine_nodes" || !row["pipe_ref"].is_null())
                    && row["pipe_ref"].as_str().is_none_or(str::is_empty)
                || metric == "engine_runs"
                    && (!row["resumed_from"].is_null()
                        || row["targets"].is_null()
                        || row["selection"].is_null())
            {
                return Err(denied());
            }
        }
        Ok(())
    }
}

pub(super) async fn capture(ctx: &Ctx) -> Result<Option<RuntimeGraph>, ApiError> {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, SCOPES).await?;
    admit(ctx, &caller).await
}

pub(super) async fn admit(ctx: &Ctx, caller: &Access) -> Result<Option<RuntimeGraph>, ApiError> {
    let selected = ctx.selected_engine(caller)?;
    if !selected.is_native_alias()
        || !matches!(&caller.auth, Some(Principal::Execution(identity)) if identity.grant.required.contains("pipes:write"))
    {
        return Ok(None);
    }
    selected.require_native_write()?;
    let mut graph = ctx
        .state
        .actions
        .pipe_operation_read(caller)
        .await
        .map_err(ApiError::unavailable)?
        .ok_or_else(denied)?;
    if !graph.permits_instance(selected.logical()) || graph.operation.is_closed() {
        return Err(denied());
    }
    let fresh = selected
        .fresh_headers_inner(&ctx.state, &ctx.headers, SCOPES, Some(&graph.binding))
        .await?;
    graph.confirm_request(&ctx.state, caller, &fresh)?;
    let runtime = RuntimeGraph {
        consumer: graph.binding,
        issuer: graph.issuer,
    };
    if selected.runtime_graph.get_or_init(|| runtime.clone()) != &runtime {
        return Err(denied());
    }
    Ok(Some(runtime))
}

pub(super) async fn lease(
    ctx: &Ctx,
    caller: &Access,
    runtime: Option<&RuntimeGraph>,
    request: &LeaseRequest,
) -> Result<(String, String), ApiError> {
    let Some(runtime) = runtime else {
        return Ok((request.pipe_ref.clone(), request.holder.clone()));
    };
    tracing::debug!(target: "meerschaum_server::engine_runtime", "Mapping registered runtime lease");
    // Each renewal requires the same current launch. Cap the held resource at
    // Main's own callback lease interval if a worker disappears before release.
    let max_ttl_ms = (crate::executor::callbacks::LEASE_SECONDS as u64) * 1000;
    if request.holder.is_empty() || request.holder.len() > 256 || request.ttl_ms > max_ttl_ms {
        return Err(denied());
    }
    let selected = ctx.selected_engine(caller)?;
    let registry =
        super::registry::NativeRegistry::load(&ctx.state, selected.logical(), selected.connector())
            .await?;
    let (graph, caller) = runtime
        .fresh(&selected, &ctx.state, &ctx.headers, SCOPES)
        .await?;
    let service = selected
        .service_access()
        .ok_or_else(ApiError::invalid_credentials)?;
    service.require(&["pipes:read", "pipes:write"], &[])?;
    let object = registry.check(&ctx.state, &caller, &service, &request.pipe_ref, true)?;
    let target = RuntimeGraph::target(&graph, &request.pipe_ref, object)?;
    let holder = format!(
        "mrsm-graph-{}-{}",
        runtime.consumer.launch,
        crate::executor::plugins::sha256(request.holder.as_bytes())
    );
    Ok((target.physical_reference().into(), holder))
}
