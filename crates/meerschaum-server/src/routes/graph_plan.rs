//! Private per-launch instruction channel, independent of generic action execution.
use super::{Ctx, body_bytes};
use crate::{
    AppState, ServerConfig,
    auth::Principal,
    envelope,
    error::{ApiError, ApiResult},
    executor::plugin_caller::RequestSource,
};
use axum::{Router, body::Body, routing::post};
use meerschaum_connector_api::graph_plan::{GRAPH_PLAN_PATH, GraphPlanRequest};

pub fn routes(app: Router<AppState>, _: &ServerConfig) -> Router<AppState> {
    app.route(GRAPH_PLAN_PATH, post(instruction))
}
async fn instruction(ctx: Ctx, body: Body) -> ApiResult {
    let original = RequestSource::resolve(&ctx.state, &ctx.headers, &[]).await?;
    let initial = original.access();
    if !matches!(initial.auth.as_ref(), Some(Principal::Execution(_))) {
        return Err(ApiError::new(
            403,
            "Canonical graph instructions require this launch's active execution callback.",
        ));
    }
    let request: GraphPlanRequest = serde_json::from_slice(&body_bytes(body, Some(4096)).await?)
        .map_err(|_| ApiError::new(400, "Invalid canonical graph instruction request."))?;
    request
        .validate()
        .map_err(|_| ApiError::new(400, "Invalid registered graph node."))?;
    let caller = original.refresh(&ctx.state, &[]).await?;
    let mut operation = ctx
        .state
        .actions
        .pipe_operation_read(&caller)
        .await
        .map_err(ApiError::unavailable)?;
    if let (
        Some(captured),
        GraphPlanRequest::Pipeline {
            registry,
            name,
            run_id,
        },
    ) = (&operation, &request)
    {
        captured
            .operation
            .pipeline_bootstrap(registry, name, run_id)?;
        let connector = ctx.state.get_api_connector(Some(registry))?;
        let runs = super::pipelines::store_of(&ctx.state, &connector)
            .get_runs(&crate::backends::RunsFilter {
                run_id: Some(run_id.clone()),
                limit: Some(1),
                ..Default::default()
            })
            .await
            .map_err(crate::engine_stream::api_error)?;
        // Never expose another run's contents. Repeat the same canonical admission
        // after the store read, including when this is called under the worker's lease.
        let source = original.refresh(&ctx.state, &[]).await?;
        let fresh = ctx
            .state
            .actions
            .pipe_operation_read(&source)
            .await
            .map_err(ApiError::unavailable)?
            .ok_or_else(|| ApiError::unavailable("The captured Pipeline is no longer active."))?;
        if fresh.issuer != captured.issuer || fresh.binding != captured.binding {
            return Err(ApiError::new(409, "The captured Pipeline launch changed."));
        }
        fresh.operation.pipeline_bootstrap(registry, name, run_id)?;
        if !runs.is_empty() {
            return Err(ApiError::new(409, "The Pipeline run ID is already in use."));
        }
        operation = Some(fresh);
    }
    if let (Some(captured), GraphPlanRequest::Ready { reference }) = (&mut operation, &request)
        && captured
            .operation
            .pipeline_layer()
            .is_some_and(|layer| layer.children.contains(reference.as_str()))
        && captured
            .operation
            .snapshot()
            .and_then(|s| s.main_instructions())
            .ok_or_else(ApiError::internal)?
            .planned_node(reference)?["provisional"]
            == true
    {
        let before = original.refresh(&ctx.state, &[]).await?;
        captured.confirm_request(&ctx.state, &initial, &before)?;
        ctx.state
            .actions
            .ready_pipe_operation(&captured.caller, reference)
            .await
            .map_err(ApiError::unavailable)?;
        let after = original.refresh(&ctx.state, &[]).await?;
        let fresh = ctx
            .state
            .actions
            .pipe_operation_read(&after)
            .await
            .map_err(ApiError::unavailable)?
            .ok_or_else(|| ApiError::unavailable("The captured Pipeline is no longer active."))?;
        if fresh.binding != captured.binding || fresh.issuer != captured.issuer {
            return Err(ApiError::new(
                409,
                "The captured Pipeline launch changed during replanning.",
            ));
        }
        operation = Some(fresh);
    }
    let current = original.refresh(&ctx.state, &[]).await?;
    // No awaits after refreshing the SAME original header/permission ceiling.
    let instruction = match operation {
        Some(mut operation) => {
            operation.confirm_request(&ctx.state, &initial, &current)?;
            Some(operation.worker_instruction(&request)?)
        }
        None => {
            // A missing/custom backend cannot silently declassify a captured graph.
            // Explicit no-graph is Main's verified provenance, never transport fallback.
            let known_no_graph = |caller: &crate::access::Access| {
                matches!(caller.auth.as_ref(), Some(Principal::Execution(identity))
                    if identity.grant.graph_tracking == 1
                        && !identity.grant.graph_pending
                        && identity.grant.graph_origin.is_none())
            };
            if !known_no_graph(&initial) || !known_no_graph(&current) {
                return Err(ApiError::unavailable(
                    "The originating graph is unavailable. Missing instructions cannot select local planning.",
                ));
            }
            None
        }
    };
    Ok(envelope::ok(
        "Main graph instructions.",
        serde_json::json!({"instruction":instruction})
            .as_object()
            .unwrap()
            .clone(),
    ))
}
