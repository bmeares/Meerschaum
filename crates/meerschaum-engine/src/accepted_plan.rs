//! Optional worker consumption of Main-selected execution instructions.
//! This trait is installed only by the trusted CLI per-launch adapter. It grants
//! no database authority: Main's canonical catalog/window checks remain decisive.
//! Ordinary Engine construction has no authority and keeps normal planning.

use crate::{Action, Begin, Graph, PipeRef, Plan, PlannedNode};
use std::collections::BTreeSet;

const MAX_NODES: usize = 1024;
const MAX_SPANS: usize = 1024;
const MAX_BYTES: usize = 2 * 1024 * 1024;

#[meerschaum_connector::async_trait]
/// A trusted per-launch plan source. Main's canonical catalog retains authority.
pub trait PlanAuthority: std::fmt::Debug + Send + Sync {
    /// An automatic run ID whose ownership Main can check when storing scoped history.
    fn new_runtime_run_id(&self, _registry: &str) -> Option<String> {
        None
    }

    /// After the normal run-ID lease is acquired, a captured Pipeline may prove this
    /// single ID is unused without reading general run or event history.
    async fn confirms_unused_run_id(&self, _registry: &str, _run_id: &str) -> Result<bool, String> {
        Ok(false)
    }
    /// Create a fresh continuation for exactly one independently admitted node attempt.
    fn node_delivery(
        &self,
        _node: &PipeRef,
    ) -> Option<meerschaum_connector::batch_delivery::NodeDelivery> {
        None
    }
    /// The local plan is a validation proposal only. The adapter sends no
    /// worker-computed bounds to Main as authority. Main returns its exact plan
    /// from the canonical admitted request and once-captured relative clock.
    async fn initial(&self, local: &Plan) -> Result<Plan, String>;
    /// Main independently verifies original source, canonical graph and actual
    /// committed-parent receipts before issuing a ready node. No worker success,
    /// proposed watermark, dirty ledger or bounds argument can stand in for them.
    async fn ready(&self, node: &PipeRef) -> Result<PlannedNode, String>;
}
fn unavailable() -> String {
    "Main has not issued an exact executable plan for this registered graph node. Refresh the original operation; worker-local replanning cannot widen its admitted windows.".into()
}

/// Structural expectation only: delegated workers do not read watermarks or
/// ledgers to propose bounds before Main returns its actual accepted plan.
pub(crate) fn structural_proposal(
    graph: &Graph,
    selected: &crate::Subgraph,
) -> Result<Plan, String> {
    let mut nodes = Vec::with_capacity(selected.order.len());
    for reference in &selected.order {
        let node = graph.node(reference).ok_or_else(unavailable)?;
        nodes.push(PlannedNode {
            pipe_ref: reference.clone(),
            action: Action::Skip,
            bounds: None,
            ranges: Vec::new(),
            chunks: Vec::new(),
            reason: "Awaiting Main's exact instructions.".into(),
            provisional: false,
            requires_confirmation: false,
            clear: false,
            consumes: Vec::new(),
            definition_hash: node.definition_hash.clone(),
            stored_definition_hash: None,
            watermark: None,
            warnings: Vec::new(),
            backtrack: None,
            bounds_text: String::new(),
        });
    }
    Ok(Plan {
        nodes,
        edges: selected
            .edges
            .iter()
            .map(|edge| (edge.parent.clone(), edge.child.clone()))
            .collect(),
        flat: selected.flat,
        warnings: selected.warnings.clone(),
    })
}
fn encoded_bound(value: &impl serde::Serialize) -> Result<(), String> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|bytes| *bytes <= MAX_BYTES)
                .ok_or_else(|| {
                    std::io::Error::other("Main execution instructions exceed their bound")
                })?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter(0), value).map_err(|_| unavailable())
}
fn scalar_bounds(begin: Option<crate::Bound>, end: Option<crate::Bound>) -> Result<(), String> {
    match (begin, end) {
        (Some(crate::Bound::Int(begin)), Some(crate::Bound::Int(end))) if begin >= end => {
            Err(unavailable())
        }
        (Some(crate::Bound::Datetime(begin)), Some(crate::Bound::Datetime(end)))
            if begin >= end =>
        {
            Err(unavailable())
        }
        (Some(crate::Bound::Int(_)), Some(crate::Bound::Datetime(_)))
        | (Some(crate::Bound::Datetime(_)), Some(crate::Bound::Int(_))) => Err(unavailable()),
        _ => Ok(()),
    }
}
pub(crate) fn validate_node(graph: &Graph, node: &PlannedNode, ready: bool) -> Result<(), String> {
    let registered = graph.node(&node.pipe_ref).ok_or_else(unavailable)?;
    if registered.definition_hash != node.definition_hash
        || node.clear
        || node.action == Action::Rebuild
        || node.ranges.len() > MAX_SPANS
        || node.chunks.len() > MAX_SPANS
        || ready && node.provisional
    {
        return Err(unavailable());
    }
    encoded_bound(node)?;
    if node.action.runs() && !node.provisional {
        let bounds = node.bounds.ok_or_else(unavailable)?;
        if matches!(bounds.begin, Begin::Default) {
            return Err(unavailable());
        }
        let typed = |value: Option<crate::Bound>| {
            matches!(
                (registered.axis.kind, value),
                (_, None)
                    | (crate::AxisKind::Int, Some(crate::Bound::Int(_)))
                    | (crate::AxisKind::Datetime, Some(crate::Bound::Datetime(_)))
            )
        };
        if !typed(bounds.begin.bound()) || !typed(bounds.end) {
            return Err(unavailable());
        }
        scalar_bounds(bounds.begin.bound(), bounds.end)?;
        for span in node.ranges.iter().chain(&node.chunks) {
            if !typed(span.begin) || !typed(span.end) {
                return Err(unavailable());
            }
            scalar_bounds(span.begin, span.end)?;
        }
    }
    Ok(())
}
pub(crate) fn validate_initial(graph: &Graph, local: &Plan, issued: Plan) -> Result<Plan, String> {
    encoded_bound(&issued)?;
    if issued.nodes.len() > MAX_NODES || issued.flat != local.flat || issued.edges != local.edges {
        return Err(unavailable());
    }
    let expected = local
        .nodes
        .iter()
        .map(|node| &node.pipe_ref)
        .collect::<BTreeSet<_>>();
    let actual = issued
        .nodes
        .iter()
        .map(|node| &node.pipe_ref)
        .collect::<BTreeSet<_>>();
    if expected != actual || actual.len() != issued.nodes.len() {
        return Err(unavailable());
    }
    for node in &issued.nodes {
        validate_node(graph, node, false)?;
    }
    Ok(issued)
}
pub(crate) fn validate_ready(
    graph: &Graph,
    reference: &PipeRef,
    issued: PlannedNode,
) -> Result<PlannedNode, String> {
    if &issued.pipe_ref != reference {
        return Err(unavailable());
    }
    validate_node(graph, &issued, true)?;
    Ok(issued)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Bounds, GraphOptions, PipeRecord};
    fn fixture() -> (Graph, Plan) {
        let mut record = PipeRecord::new("sql:main", "manual:fixture", "window", None);
        record.parameters = meerschaum_core::Value::from_json_str(
            &serde_json::json!({
                "columns":{"datetime":"tick"},"dtypes":{"tick":"int64"}
            })
            .to_string(),
        )
        .unwrap()
        .to_map();
        let graph = Graph::build([record], &GraphOptions::default());
        let node = graph.nodes().next().unwrap();
        let planned: PlannedNode = serde_json::from_value(serde_json::json!({
            "pipe_ref":node.pipe_ref,"action":"root","bounds":{"begin":5,"end":10},
            "reason":"Main issued","definition_hash":node.definition_hash
        }))
        .unwrap();
        let plan = Plan {
            nodes: vec![planned],
            edges: Vec::new(),
            flat: true,
            warnings: Vec::new(),
        };
        (graph, plan)
    }
    #[test]
    fn main_concrete_bounds_replace_a_worker_default_and_missing_or_extra_nodes_refuse() {
        let (graph, issued) = fixture();
        let mut local = issued.clone();
        local.nodes[0].bounds = Some(Bounds {
            begin: Begin::Default,
            end: None,
        });
        assert_eq!(
            validate_initial(&graph, &local, issued.clone())
                .unwrap()
                .nodes[0]
                .bounds,
            issued.nodes[0].bounds
        );
        let mut missing = issued.clone();
        missing.nodes.clear();
        assert!(validate_initial(&graph, &local, missing).is_err());
        let mut duplicate = issued.clone();
        duplicate.nodes.push(issued.nodes[0].clone());
        assert!(validate_initial(&graph, &local, duplicate).is_err());
        assert!(
            validate_initial(&graph, &local, local.clone()).is_err(),
            "Worker default became a Main-resolved plan"
        );
    }
    #[test]
    fn ready_node_must_be_fresh_concrete_same_definition_and_cannot_clear_target() {
        let (graph, issued) = fixture();
        let node = issued.nodes[0].clone();
        assert!(validate_ready(&graph, &node.pipe_ref, node.clone()).is_ok());
        let mut provisional = node.clone();
        provisional.provisional = true;
        assert!(validate_ready(&graph, &node.pipe_ref, provisional).is_err());
        let mut changed = node.clone();
        changed.definition_hash = "changed".into();
        assert!(validate_ready(&graph, &node.pipe_ref, changed).is_err());
        let mut clear = node.clone();
        clear.clear = true;
        assert!(validate_ready(&graph, &node.pipe_ref, clear).is_err());
    }
}
