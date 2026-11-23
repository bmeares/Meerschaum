//! Execution choices come only from Main's canonical plan/window pair.
use super::{CallbackBinding, ReadAccess};
use crate::error::ApiError;
use meerschaum_connector_api::graph_plan::{
    GraphBinding, GraphInstruction, GraphPlanReply, GraphPlanRequest,
};

fn binding(value: &CallbackBinding) -> GraphBinding {
    GraphBinding {
        grant: value.grant,
        launch: value.launch,
        revision: value.context_revision,
    }
}
fn unavailable() -> ApiError {
    ApiError::unavailable(
        "Main has not issued this registered node's exact executable window. Worker-local bounds, progress or success cannot replace admission.",
    )
}
impl ReadAccess {
    /// Finish an awaited instruction lookup under the same original request.
    /// Role/grant gains during the wait cannot enlarge object visibility or scopes.
    pub fn confirm_request(
        &mut self,
        state: &crate::AppState,
        original: &crate::access::Access,
        current: &crate::access::Access,
    ) -> Result<(), ApiError> {
        let denied = || {
            ApiError::new(
                403,
                "The graph instruction request lost its original bounded source.",
            )
        };
        for caller in [original, current] {
            let Some(crate::auth::Principal::Execution(identity)) = &caller.auth else {
                return Err(denied());
            };
            if identity.grant.id != self.binding.grant
                || identity.grant.launch_id != self.binding.launch
                || identity.grant.graph_tracking != 1
                || identity.grant.graph_pending
                || identity.grant.graph_origin.as_ref() != Some(&self.issuer)
            {
                return Err(denied());
            }
            caller.require(
                &self
                    .operation
                    .required_scopes()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                &[],
            )?;
            let snapshot = self.operation.snapshot().ok_or_else(denied)?;
            for (reference, member) in snapshot.members() {
                let parts = reference.split('|').collect::<Vec<_>>();
                if parts.len() != 4
                    || !crate::access::guard::may_use_connector(state, caller, parts[0])
                {
                    return Err(denied());
                }
                let pipe = meerschaum_core::Pipe::builder(parts[1], parts[2])
                    .instance(parts[0])
                    .location((parts[3] != "None").then_some(parts[3]))
                    .defaults(state.config.pipe_defaults.clone())
                    .parameters(member.parameters.clone())
                    .build()
                    .map_err(|_| denied())?;
                if !caller.can_see_pipe(state, &pipe)
                    || member.writable && !caller.can_edit_pipe(state, &pipe)
                {
                    return Err(denied());
                }
                crate::access::guard::check_source_connector(state, caller, pipe.connector_keys())?;
            }
        }
        self.caller = current.clone();
        self.caller
            .principal
            .permissions
            .retain(|permission| original.has(permission));
        self.caller.principal.is_admin &= original.principal.is_admin;
        Ok(())
    }
    pub fn worker_instruction(
        &self,
        request: &GraphPlanRequest,
    ) -> Result<GraphPlanReply, ApiError> {
        let snapshot = self.operation.snapshot().ok_or_else(unavailable)?;
        let instructions = snapshot.main_instructions().ok_or_else(unavailable)?;
        if instructions.verification().is_some()
            && !matches!(
                request,
                GraphPlanRequest::Verify { .. } | GraphPlanRequest::Dispatch
            )
        {
            return Err(unavailable());
        }
        let instruction = match request {
            GraphPlanRequest::Pipeline {
                registry,
                name,
                run_id,
            } => {
                let (definition, roots) =
                    self.operation.pipeline_bootstrap(registry, name, run_id)?;
                GraphInstruction::Pipeline { definition, roots }
            }
            GraphPlanRequest::Dispatch => GraphInstruction::Dispatch {
                verify: instructions.verification().is_some(),
                argv_sha256: super::dispatch_argv_sha256(&self.operation.sysargs()?)?,
            },
            GraphPlanRequest::Initial => {
                GraphInstruction::Initial(instructions.main_plan().clone())
            }
            GraphPlanRequest::Ready { reference } => {
                if self.pending_nodes.contains(reference) {
                    return Err(ApiError::unavailable(
                        "This node has unfinished native commit evidence. Automatic replay or dependent issuance is unavailable.",
                    ));
                }
                let member = snapshot
                    .members()
                    .get(reference)
                    .filter(|member| member.writable)
                    .ok_or_else(unavailable)?;
                let node = instructions.planned_node(reference)?;
                if node
                    .get("provisional")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
                {
                    // Ready replanning requires independently proven Main parent
                    // completion. Neither worker success nor dirty ledger is proof.
                    return Err(unavailable());
                }
                if matches!(
                    node.get("action").and_then(serde_json::Value::as_str),
                    Some("root" | "incremental" | "backfill" | "rebuild")
                ) && !member
                    .node_window
                    .as_ref()
                    .is_some_and(|window| window.is_issued())
                {
                    return Err(unavailable());
                }
                GraphInstruction::Ready(node)
            }
            GraphPlanRequest::Verify { reference } => {
                let verification = instructions
                    .verification()
                    .filter(|verification| &verification.reference == reference)
                    .ok_or_else(unavailable)?;
                if self.pending_nodes.contains(reference) {
                    return Err(unavailable());
                }
                GraphInstruction::Verify(
                    serde_json::to_value(verification).map_err(|_| unavailable())?,
                )
            }
        };
        Ok(GraphPlanReply {
            version: 1,
            consumer: binding(&self.binding),
            issuer: binding(&self.issuer),
            instruction,
        })
    }
}
