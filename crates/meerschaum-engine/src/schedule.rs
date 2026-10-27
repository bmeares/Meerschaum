//! Duration estimates, HEFT priorities and the critical path (§5.5).
//!
//! - Durations are an exponentially weighted moving average per `(pipe, action)` from run
//!   history ([`EwmaDurations`]), with a configurable default for pipes that never ran.
//! - Each node's priority is its **upward rank**: its expected duration plus the largest upward
//!   rank among its children (the HEFT list-scheduling heuristic), so the critical path goes
//!   first.
//! - The critical-path method (CPM) gives each node its earliest/latest start and finish and its
//!   slack; these drive the planned bars and the critical-path highlight in the Gantt chart.
//!   CPM ignores resource limits (workers and semaphores).

use std::collections::HashMap;
use std::time::Duration;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::node::PipeRef;
use crate::plan::{Action, Plan};
use crate::runs::{NodeRow, NodeStatus};

/// A source of expected node durations.
pub trait DurationSource: Send + Sync {
    /// The expected duration of running `action` on `node`. Actions that do not sync take no
    /// time.
    fn estimate(&self, node: &PipeRef, action: Action) -> Duration;
}

/// EWMA duration estimates per `(pipe, action)`.
///
/// A node is estimated from its own history: the EWMA of its earlier successful syncs with the
/// same action, else of all its earlier successful syncs (any action), else [`Self::default`]
/// (`system.engine.default_duration_seconds`, 30 s).
#[derive(Debug, Clone)]
pub struct EwmaDurations {
    /// The smoothing factor in `(0, 1]` (weight of the newest observation).
    pub alpha: f64,
    /// The estimate for pipes without history.
    pub default: Duration,
    values: HashMap<(PipeRef, Action), f64>,
    any_action: HashMap<PipeRef, f64>,
}

impl Default for EwmaDurations {
    fn default() -> Self {
        EwmaDurations::new(0.3, Duration::from_secs(30))
    }
}

impl EwmaDurations {
    /// Empty estimates.
    pub fn new(alpha: f64, default: Duration) -> EwmaDurations {
        EwmaDurations {
            alpha: alpha.clamp(f64::EPSILON, 1.0),
            default,
            values: HashMap::new(),
            any_action: HashMap::new(),
        }
    }

    /// Where an estimate comes from: `history` (the node's own earlier runs) or `default`.
    pub fn source(&self, node: &PipeRef, action: Action) -> &'static str {
        if self.values.contains_key(&(node.clone(), action)) || self.any_action.contains_key(node) {
            "history"
        } else {
            "default"
        }
    }

    /// Fold in one observed duration.
    pub fn observe(&mut self, node: &PipeRef, action: Action, duration: Duration) {
        let x = duration.as_secs_f64();
        let alpha = self.alpha;
        self.values
            .entry((node.clone(), action))
            .and_modify(|v| *v = alpha * x + (1.0 - alpha) * *v)
            .or_insert(x);
        self.any_action
            .entry(node.clone())
            .and_modify(|v| *v = alpha * x + (1.0 - alpha) * *v)
            .or_insert(x);
    }

    /// Build estimates from `mrsm:engine_nodes` rows (oldest first); only finished syncs count.
    pub fn from_history<'a>(
        rows: impl IntoIterator<Item = &'a NodeRow>,
        alpha: f64,
        default: Duration,
    ) -> EwmaDurations {
        let mut out = EwmaDurations::new(alpha, default);
        for row in rows {
            if row.status == NodeStatus::Succeeded
                && let Some(secs) = row.duration_seconds
            {
                out.observe(
                    &row.pipe_ref,
                    row.action,
                    Duration::from_secs_f64(secs.max(0.0)),
                );
            }
        }
        out
    }
}

impl DurationSource for EwmaDurations {
    fn estimate(&self, node: &PipeRef, action: Action) -> Duration {
        if !action.runs() {
            return Duration::ZERO;
        }
        self.values
            .get(&(node.clone(), action))
            .or_else(|| self.any_action.get(node))
            .map_or(self.default, |s| Duration::from_secs_f64(*s))
    }
}

/// One node's CPM times, relative to the run start.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NodeTimes {
    /// Expected duration.
    pub duration: Duration,
    /// HEFT upward rank (the priority).
    pub upward_rank: Duration,
    /// Earliest start.
    pub earliest_start: Duration,
    /// Earliest finish.
    pub earliest_finish: Duration,
    /// Latest start without delaying the run.
    pub latest_start: Duration,
    /// Latest finish without delaying the run.
    pub latest_finish: Duration,
    /// `latest_start − earliest_start`; zero on the critical path.
    pub slack: Duration,
}

/// The CPM schedule of a plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schedule {
    /// Per-node times, in plan order.
    pub nodes: IndexMap<PipeRef, NodeTimes>,
    /// The expected run length with unlimited workers.
    pub makespan: Duration,
    /// The longest chain by upward rank.
    pub critical_path: Vec<PipeRef>,
}

/// The action a node is expected to run. A provisional `skip` (a child planned against state
/// its running parents are about to change) is re-planned when they finish and usually syncs
/// then, so it is scheduled as an `incremental`.
fn expected_action(node: &crate::plan::PlannedNode) -> Action {
    if node.provisional && node.action == Action::Skip {
        Action::Incremental
    } else {
        node.action
    }
}

/// Compute upward ranks and CPM times for a plan.
pub fn schedule(plan: &Plan, durations: &dyn DurationSource) -> Schedule {
    let order: Vec<&PipeRef> = plan.nodes.iter().map(|n| &n.pipe_ref).collect();
    let dur: HashMap<&PipeRef, Duration> = plan
        .nodes
        .iter()
        .map(|n| {
            (
                &n.pipe_ref,
                durations.estimate(&n.pipe_ref, expected_action(n)),
            )
        })
        .collect();
    let mut parents: HashMap<&PipeRef, Vec<&PipeRef>> = HashMap::new();
    let mut children: HashMap<&PipeRef, Vec<&PipeRef>> = HashMap::new();
    for (p, c) in &plan.edges {
        parents.entry(c).or_default().push(p);
        children.entry(p).or_default().push(c);
    }
    let mut es: HashMap<&PipeRef, Duration> = HashMap::new();
    let mut ef: HashMap<&PipeRef, Duration> = HashMap::new();
    for r in &order {
        let start = parents
            .get(r)
            .into_iter()
            .flatten()
            .filter_map(|p| ef.get(p))
            .max()
            .copied()
            .unwrap_or(Duration::ZERO);
        es.insert(r, start);
        ef.insert(r, start + dur[r]);
    }
    let makespan = ef.values().max().copied().unwrap_or(Duration::ZERO);
    let mut lf: HashMap<&PipeRef, Duration> = HashMap::new();
    let mut ls: HashMap<&PipeRef, Duration> = HashMap::new();
    let mut rank: HashMap<&PipeRef, Duration> = HashMap::new();
    for r in order.iter().rev() {
        let kids = children.get(r).map(Vec::as_slice).unwrap_or(&[]);
        let finish = kids
            .iter()
            .filter_map(|c| ls.get(c))
            .min()
            .copied()
            .unwrap_or(makespan);
        lf.insert(r, finish);
        ls.insert(r, finish.saturating_sub(dur[r]));
        let below = kids
            .iter()
            .filter_map(|c| rank.get(c))
            .max()
            .copied()
            .unwrap_or(Duration::ZERO);
        rank.insert(r, dur[r] + below);
    }
    let nodes: IndexMap<PipeRef, NodeTimes> = order
        .iter()
        .map(|r| {
            (
                (*r).clone(),
                NodeTimes {
                    duration: dur[r],
                    upward_rank: rank[r],
                    earliest_start: es[r],
                    earliest_finish: ef[r],
                    latest_start: ls[r],
                    latest_finish: lf[r],
                    slack: ls[r].saturating_sub(es[r]),
                },
            )
        })
        .collect();
    let mut critical_path = Vec::new();
    let mut current = order
        .iter()
        .filter(|r| !parents.contains_key(**r))
        .max_by(|a, b| rank[**a].cmp(&rank[**b]).then_with(|| b.cmp(a)))
        .copied();
    while let Some(r) = current {
        critical_path.push(r.clone());
        current = children
            .get(r)
            .into_iter()
            .flatten()
            .filter(|c| dur[r] + rank[**c] == rank[r])
            .max_by(|a, b| rank[**a].cmp(&rank[**b]).then_with(|| b.cmp(a)))
            .copied();
    }
    Schedule {
        nodes,
        makespan,
        critical_path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::PlannedNode;

    fn node(m: &str, action: Action) -> PlannedNode {
        PlannedNode {
            pipe_ref: PipeRef::new("i", "c", m, None),
            action,
            bounds: None,
            ranges: vec![],
            chunks: vec![],
            reason: String::new(),
            provisional: false,
            requires_confirmation: false,
            clear: false,
            consumes: vec![],
            definition_hash: String::new(),
            stored_definition_hash: None,
            watermark: None,
            warnings: vec![],
            backtrack: None,
            bounds_text: String::new(),
        }
    }

    #[test]
    fn diamond_cpm() {
        let r = |m: &str| PipeRef::new("i", "c", m, None);
        let plan = Plan {
            nodes: vec![
                node("a", Action::Root),
                node("b", Action::Incremental),
                node("c", Action::Backfill),
                node("d", Action::Incremental),
            ],
            edges: vec![
                (r("a"), r("b")),
                (r("a"), r("c")),
                (r("b"), r("d")),
                (r("c"), r("d")),
            ],
            flat: false,
            warnings: vec![],
        };
        let mut d = EwmaDurations::new(0.5, Duration::from_secs(10));
        d.observe(&r("c"), Action::Backfill, Duration::from_secs(40));
        d.observe(&r("b"), Action::Incremental, Duration::from_secs(20));
        d.observe(&r("b"), Action::Incremental, Duration::from_secs(10));
        assert_eq!(
            d.estimate(&r("b"), Action::Incremental),
            Duration::from_secs(15)
        );
        let s = schedule(&plan, &d);
        assert_eq!(s.makespan, Duration::from_secs(60));
        assert_eq!(s.critical_path, vec![r("a"), r("c"), r("d")]);
        let b = s.nodes[&r("b")];
        assert_eq!(b.earliest_start, Duration::from_secs(10));
        assert_eq!(b.latest_start, Duration::from_secs(35));
        assert_eq!(b.slack, Duration::from_secs(25));
        assert_eq!(s.nodes[&r("c")].slack, Duration::ZERO);
        assert_eq!(s.nodes[&r("a")].upward_rank, Duration::from_secs(60));
    }

    #[test]
    fn provisional_skips_are_scheduled_as_syncs() {
        let r = |m: &str| PipeRef::new("i", "c", m, None);
        let mut child = node("b", Action::Skip);
        child.provisional = true;
        let plan = Plan {
            nodes: vec![node("a", Action::Root), child, node("c", Action::Skip)],
            edges: vec![(r("a"), r("b"))],
            flat: false,
            warnings: vec![],
        };
        let s = schedule(&plan, &EwmaDurations::new(0.5, Duration::from_secs(10)));
        assert_eq!(s.nodes[&r("b")].duration, Duration::from_secs(10));
        assert_eq!(s.nodes[&r("c")].duration, Duration::ZERO);
        assert_eq!(s.makespan, Duration::from_secs(20));
    }

    #[test]
    fn estimates_fall_back_to_the_node_then_the_default() {
        let r = |m: &str| PipeRef::new("i", "c", m, None);
        let mut d = EwmaDurations::new(0.5, Duration::from_secs(30));
        d.observe(&r("a"), Action::Incremental, Duration::from_secs(8));
        assert_eq!(
            d.estimate(&r("a"), Action::Incremental),
            Duration::from_secs(8)
        );
        // Another action of the same node: the node's own history, not the default.
        assert_eq!(
            d.estimate(&r("a"), Action::Backfill),
            Duration::from_secs(8)
        );
        assert_eq!(d.source(&r("a"), Action::Backfill), "history");
        assert_eq!(d.estimate(&r("b"), Action::Root), Duration::from_secs(30));
        assert_eq!(d.source(&r("b"), Action::Root), "default");
        assert_eq!(d.estimate(&r("a"), Action::Skip), Duration::ZERO);
    }
}
