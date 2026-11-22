//! Bare worker consumption of Main bounds. The optional authority is
//! installed only through the native first-enforcement transport.
use super::{Engine, RunOptions};
use crate::{Action, Begin, PipeRef, PlanAuthority, PlannedNode, Selection, SyncArgs, SyncBegin};
use meerschaum_core::{Pipe, SyncResult};
use meerschaum_kernel::enforce::PipeInput;

/// Same Root/chunk precedence as scheduled native execution.
fn calls(node: &PlannedNode, args: &SyncArgs) -> Result<Vec<SyncArgs>, String> {
    if node.provisional || node.clear || node.action == Action::Rebuild {
        return Err("Main has not admitted a bare clear, rebuild or provisional sync.".into());
    }
    if !node.action.runs() {
        return Ok(Vec::new()); // explicit Main no-op, never worker zero-row evidence
    }
    let bounds = node
        .bounds
        .ok_or("Main omitted this bare sync's exact bounds.")?;
    if matches!(bounds.begin, Begin::Default) {
        return Err("Main must resolve Default before a delegated bare fetch.".into());
    }
    if node.chunks.len() > 1024 || node.ranges.len() > 1024 {
        return Err("Main's bounded native calls are unavailable.".into());
    }
    Ok(crate::native::native_call_args(
        node.action,
        Some(bounds),
        &node.chunks,
        &node.ranges,
        args,
    ))
}

impl Engine {
    /// Engine::sync_pipe calls this ONLY when self.plan_authority is Some; its
    /// ordinary None path is byte-for-byte unchanged. SDK sync_api must construct
    /// this Engine from its already-built registry/config and attach the same
    /// native graph adapter before invoking Engine::sync_pipe.
    pub(super) async fn accepted_bare(
        &self,
        pipe: &Pipe,
        input: Option<PipeInput>,
        args: &SyncArgs,
        authority: &dyn PlanAuthority,
    ) -> SyncResult {
        let outcome = async {
            let reference = PipeRef::from_pipe(pipe);
            let (graph, _, _) = self
                .resolve(&RunOptions {
                    selection: Some(Selection::refs([reference.clone()])),
                    instances: vec![pipe.instance_keys().into()],
                    ..Default::default()
                })
                .await?;
            let issued = crate::accepted_plan::validate_ready(
                &graph,
                &reference,
                authority.ready(&reference).await?,
            )?;
            if graph
                .node(&reference)
                .is_none_or(|node| node.pipe.raw_parameters() != pipe.raw_parameters())
            {
                return Err(
                    "The bare sync's registered definition changed; Main must admit it again."
                        .into(),
                );
            }
            let calls = calls(&issued, args)?;
            if calls.is_empty() {
                return Ok(SyncResult::new(
                    true,
                    "Main issued no runnable call for this node.",
                ));
            }
            let env = crate::sync::SyncEnv {
                delivery: authority.node_delivery(&reference),
                registry: self.registry.as_ref(),
                defaults: &self.config.defaults,
                default_backtrack_minutes: self.config.default_backtrack_minutes,
                filter_params_index_limit: self.config.filter_params_index_limit,
                known: None,
            };
            if let Some(input) = input {
                // Frame input is written once. Actual native cast/autotime/OLD-row
                // enforcement uses Main DataWindow; do not duplicate a supplied
                // frame into every source-call span or silently filter outliers.
                return Ok(crate::sync::sync_pipe(&env, pipe, Some(input), args, None).await);
            }
            if calls.iter().any(|call|!matches!(args.begin,SyncBegin::Default)&&args.begin!=call.begin||args.end.is_some()&&args.end!=call.end) {
                return Err("This nested fetch needs Main's exact subcall intersection; incompatible explicit bounds cannot select a wider call.".into());
            }
            let mut results = Vec::with_capacity(calls.len());
            for call in calls {
                // Concrete Main bounds: no worker watermark/default recomputation.
                let result = crate::sync::sync_pipe(&env, pipe, None, &call, None).await;
                let failed = !result.success;
                results.push(result);
                if failed {
                    break;
                }
            }
            // Reuse native::combine (narrow pub(crate) visibility), preserving actual
            // committed counts on a later failure; never imply global rollback.
            Ok(crate::native::combine(pipe, results))
        }
        .await;
        outcome.unwrap_or_else(|error: String| SyncResult::new(false, error))
    }
}
