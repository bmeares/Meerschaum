//! The engine's read API for the console (§5.7, §5.9, §8.2): run history, per-run Gantt data,
//! the pipe graph with status and ledger lag on its edges, and persisted events to poll.
//!
//! Everything here reads the instance-backed engine state (the `mrsm:engine_*` pipes), so a
//! console in a different process from the run sees the same data. Nothing here writes.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use meerschaum_connector::PipesKeysQuery;
use meerschaum_core::success::Bound;
use serde::{Deserialize, Serialize};

use crate::axis::Interval;
use crate::engine::{EngineConfig, fetch_records};
use crate::events::EventRow;
use crate::graph::{Graph, GraphOptions, GraphWarning};
use crate::instance_state::InstanceState;
use crate::ledger::{dirty_intervals, map_to_child};
use crate::node::PipeRef;
use crate::plan::Action;
use crate::registry::ConnectorRegistry;
use crate::runs::{NodeRow, NodeStatus, RunRecord, RunRow};
use crate::schedule::DurationSource;
use crate::state::{StateError, StateSource};

/// Overall status of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Not finished.
    Running,
    /// Finished successfully.
    Succeeded,
    /// Finished with a failure.
    Failed,
}

impl RunStatus {
    /// The lowercase name.
    pub fn name(&self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::Succeeded => "succeeded",
            RunStatus::Failed => "failed",
        }
    }

    fn of(run: &RunRow) -> RunStatus {
        match (run.finished, run.success) {
            (None, _) => RunStatus::Running,
            (Some(_), Some(false)) => RunStatus::Failed,
            (Some(_), _) => RunStatus::Succeeded,
        }
    }
}

/// Filters for the runs table.
#[derive(Debug, Clone, Default)]
pub struct RunFilter {
    /// Exactly this run.
    pub run_id: Option<String>,
    /// Only runs with this exact pipeline name.
    pub run_name: Option<String>,
    /// Only runs with this status.
    pub status: Option<RunStatus>,
    /// Only runs that target or contain a node whose ref contains this text.
    pub pipe: Option<String>,
    /// Only runs started at or after this time.
    pub since: Option<DateTime<Utc>>,
    /// Maximum rows (default 100).
    pub limit: Option<usize>,
}

/// One row of the runs table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSummary {
    /// The stored run.
    pub run: RunRow,
    /// Derived status.
    pub status: RunStatus,
    /// Wall-clock seconds (to now for a running run).
    pub duration_seconds: f64,
    /// Node counts by status name.
    pub counts: BTreeMap<String, usize>,
}

/// One Gantt row: planned and actual bars for a node of a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GanttBar {
    /// The node.
    pub pipe_ref: PipeRef,
    /// The planned action.
    pub action: Action,
    /// The node status.
    pub status: NodeStatus,
    /// Planned start (run start plus the schedule's planned offset).
    pub planned_start: Option<DateTime<Utc>>,
    /// Planned end: the planned start plus [`Self::planned_duration_seconds`].
    pub planned_end: Option<DateTime<Utc>>,
    /// The planned duration: the EWMA of the node's earlier successful runs (same action,
    /// else any action), else `system.engine.default_duration_seconds` (30 s). `0` for a node
    /// whose action does not sync (`skip`, `observe`, ...).
    #[serde(default)]
    pub planned_duration_seconds: Option<f64>,
    /// Where the planned duration comes from: `history` or `default`.
    #[serde(default)]
    pub planned_source: Option<String>,
    /// Actual start.
    pub actual_start: Option<DateTime<Utc>>,
    /// Actual end.
    pub actual_end: Option<DateTime<Utc>>,
}

/// A run with its nodes and bars.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunDetail {
    /// The summary row.
    pub summary: RunSummary,
    /// Per-node results.
    pub nodes: Vec<NodeRow>,
    /// One bar pair per node, in planned order.
    pub bars: Vec<GanttBar>,
}

/// A dirty range on the child's axis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DirtyRange {
    /// Inclusive start (`None` is unbounded).
    pub begin: Option<Bound>,
    /// Exclusive end (`None` is unbounded).
    pub end: Option<Bound>,
}

/// Ledger lag of one edge: parent changes the child has not consumed yet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeView {
    /// Upstream node.
    pub parent: PipeRef,
    /// Downstream node.
    pub child: PipeRef,
    /// Where the edge was declared (`parents`, `children`, `template`).
    pub sources: Vec<String>,
    /// The instance holding the edge's ledger and cursor.
    pub state_instance: String,
    /// Whether the child has a cursor (without one it plans from watermarks).
    pub has_cursor: bool,
    /// The cursor's consumed sequence number.
    pub cursor_seq: Option<i64>,
    /// The parent's newest ledger sequence number.
    pub latest_seq: Option<i64>,
    /// Number of unconsumed ledger entries.
    pub pending_entries: usize,
    /// Dirty ranges on the child's axis (empty when caught up).
    pub dirty: Vec<DirtyRange>,
}

impl EdgeView {
    /// Whether the child is behind its parent.
    pub fn lagging(&self) -> bool {
        self.pending_entries > 0
    }
}

/// One node of the pipeline view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeView {
    /// The node.
    pub pipe_ref: PipeRef,
    /// Human label (`connector:metric[:location]`).
    pub label: String,
    /// The instance that stores the pipe.
    pub instance: String,
    /// Status in the most recent run that touched it.
    pub status: Option<NodeStatus>,
    /// That run.
    pub last_run_id: Option<String>,
    /// When the node last finished.
    pub last_finished: Option<DateTime<Utc>>,
}

/// The DAG with status and lag.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PipelineView {
    /// Nodes in graph order.
    pub nodes: Vec<NodeView>,
    /// Edges in graph order.
    pub edges: Vec<EdgeView>,
    /// Graph warnings, rendered.
    pub warnings: Vec<String>,
}

impl PipelineView {
    /// Edges into `node` (its Parents tab).
    pub fn parents_of<'a>(&'a self, node: &'a PipeRef) -> impl Iterator<Item = &'a EdgeView> {
        self.edges.iter().filter(move |e| &e.child == node)
    }

    /// Edges out of `node` (its Children tab).
    pub fn children_of<'a>(&'a self, node: &'a PipeRef) -> impl Iterator<Item = &'a EdgeView> {
        self.edges.iter().filter(move |e| &e.parent == node)
    }
}

/// Read-only access to engine state for the console.
pub struct EngineReader {
    registry: Arc<dyn ConnectorRegistry>,
    config: EngineConfig,
    state: InstanceState,
}

impl std::fmt::Debug for EngineReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineReader")
            .field("home", &self.config.home_instance)
            .finish_non_exhaustive()
    }
}

fn summarize(run: RunRow, nodes: &[NodeRow]) -> RunSummary {
    let end = run.finished.unwrap_or_else(Utc::now);
    let mut counts = BTreeMap::new();
    for n in nodes {
        let name = serde_json::to_value(n.status)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default();
        *counts.entry(name).or_insert(0) += 1;
    }
    RunSummary {
        status: RunStatus::of(&run),
        duration_seconds: (end - run.started).num_milliseconds().max(0) as f64 / 1000.0,
        run,
        counts,
    }
}

impl EngineReader {
    /// A reader over `registry`; runs, nodes and events are read from `config.home_instance`.
    pub fn new(registry: Arc<dyn ConnectorRegistry>, config: EngineConfig) -> EngineReader {
        let state = InstanceState::new(registry.clone(), &config.home_instance);
        EngineReader {
            registry,
            config,
            state,
        }
    }

    /// Run history (the runs table), newest first.
    pub async fn runs(&self, filter: &RunFilter) -> Result<Vec<RunSummary>, StateError> {
        let limit = filter.limit.unwrap_or(100);
        let mut out = Vec::new();
        // Over-read so the filters below still fill the page.
        for run in self
            .state
            .recent_runs(if filter.run_name.is_some() {
                usize::MAX
            } else {
                limit.saturating_mul(4).max(limit)
            })
            .await?
        {
            if filter.run_id.as_ref().is_some_and(|id| *id != run.run_id) {
                continue;
            }
            if filter
                .run_name
                .as_ref()
                .is_some_and(|name| !crate::runs::run_has_name(&run.run_id, name))
            {
                continue;
            }
            if filter.since.is_some_and(|s| run.started < s) {
                continue;
            }
            if filter.status.is_some_and(|s| s != RunStatus::of(&run)) {
                continue;
            }
            let nodes = self
                .state
                .load_run(&run.run_id)
                .await?
                .map(|r| r.nodes)
                .unwrap_or_default();
            if let Some(text) = &filter.pipe {
                let label = text.to_lowercase();
                let matches = |pipe: &PipeRef| {
                    pipe.as_str().contains(text.as_str())
                        || pipe.label().to_lowercase().contains(&label)
                        || pipe.label_with_instance().to_lowercase().contains(&label)
                };
                let hit =
                    run.targets.iter().any(matches) || nodes.iter().any(|n| matches(&n.pipe_ref));
                if !hit {
                    continue;
                }
            }
            out.push(summarize(run, &nodes));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// One run with planned-vs-actual bars. A node's planned bar starts at its CPM earliest
    /// start and lasts its planned duration: the EWMA of the node's successful runs before this
    /// one (same action, else any action), else `system.engine.default_duration_seconds`
    /// (30 s). The run's own node rows get the same value in `planned_duration_seconds`.
    pub async fn run_detail(&self, run_id: &str) -> Result<Option<RunDetail>, StateError> {
        let Some(RunRecord { run, mut nodes }) = self.state.load_run(run_id).await? else {
            return Ok(None);
        };
        nodes.sort_by(|a, b| {
            a.planned_start_seconds
                .partial_cmp(&b.planned_start_seconds)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.started.cmp(&b.started))
        });
        let history = self.state.node_history(None).await.unwrap_or_default();
        let estimates = crate::schedule::EwmaDurations::from_history(
            history
                .iter()
                .filter(|h| h.run_id != run_id && h.started.is_some_and(|s| s < run.started)),
            self.config.ewma_alpha,
            self.config.default_duration,
        );
        let mut bars = Vec::with_capacity(nodes.len());
        for n in nodes.iter_mut() {
            let planned_start = n
                .planned_start_seconds
                .map(|s| run.started + ChronoDuration::milliseconds((s * 1000.0).round() as i64));
            let source = estimates.source(&n.pipe_ref, n.action);
            if planned_start.is_some() && n.planned_duration_seconds.is_none() {
                n.planned_duration_seconds =
                    Some(estimates.estimate(&n.pipe_ref, n.action).as_secs_f64());
            }
            let planned_end = planned_start
                .zip(n.planned_duration_seconds)
                .map(|(p, secs)| p + ChronoDuration::milliseconds((secs * 1000.0).round() as i64));
            bars.push(GanttBar {
                pipe_ref: n.pipe_ref.clone(),
                action: n.action,
                status: n.status,
                planned_start,
                planned_end,
                planned_duration_seconds: n.planned_duration_seconds,
                planned_source: planned_start.map(|_| source.to_string()),
                actual_start: n.started,
                actual_end: n.finished,
            });
        }
        Ok(Some(RunDetail {
            summary: summarize(run, &nodes),
            nodes,
            bars,
        }))
    }

    /// Persisted events of `run_id` with `seq > after_seq`, in order. The console polls (or
    /// streams) this to follow a run started by any process.
    pub async fn events_after(
        &self,
        run_id: &str,
        after_seq: Option<u64>,
    ) -> Result<Vec<EventRow>, StateError> {
        let mut rows = self.state.load_events(run_id).await?;
        if let Some(after) = after_seq {
            rows.retain(|r| r.seq > after);
        }
        Ok(rows)
    }

    /// The id of the newest running run, else the newest run.
    pub async fn current_run_id(&self) -> Result<Option<String>, StateError> {
        let runs = self.state.recent_runs(20).await?;
        Ok(runs
            .iter()
            .find(|r| RunStatus::of(r) == RunStatus::Running)
            .or(runs.first())
            .map(|r| r.run_id.clone()))
    }

    /// The pipe graph (every registered pipe of `instances`, default: the home instance) with
    /// each node's latest run status and each edge's ledger lag.
    pub async fn pipeline(&self, instances: &[String]) -> Result<PipelineView, String> {
        let instances: Vec<String> = if instances.is_empty() {
            vec![self.config.home_instance.clone()]
        } else {
            instances.to_vec()
        };
        let mut records = Vec::new();
        for i in &instances {
            records.extend(
                fetch_records(
                    self.registry.as_ref(),
                    &self.config.defaults,
                    i,
                    &PipesKeysQuery::default(),
                )
                .await?,
            );
        }
        let graph = Graph::build(
            records,
            &GraphOptions {
                defaults: self.config.defaults.clone(),
                implicit_edges: self.config.implicit_edges,
                default_backtrack_minutes: self.config.default_backtrack_minutes as f64,
            },
        );
        // Latest status per node, newest run first.
        let mut latest: BTreeMap<PipeRef, (NodeStatus, String, Option<DateTime<Utc>>)> =
            BTreeMap::new();
        if let Ok(runs) = self.state.recent_runs(25).await {
            for run in runs {
                let Ok(Some(rec)) = self.state.load_run(&run.run_id).await else {
                    continue;
                };
                for n in rec.nodes {
                    latest.entry(n.pipe_ref.clone()).or_insert((
                        n.status,
                        run.run_id.clone(),
                        n.finished,
                    ));
                }
            }
        }
        let mut view = PipelineView::default();
        for node in graph.nodes() {
            let l = latest.get(&node.pipe_ref);
            view.nodes.push(NodeView {
                pipe_ref: node.pipe_ref.clone(),
                label: node.pipe.to_string(),
                instance: node.record.instance_keys.clone(),
                status: l.map(|l| l.0),
                last_run_id: l.map(|l| l.1.clone()),
                last_finished: l.and_then(|l| l.2),
            });
        }
        for w in graph.warnings() {
            view.warnings.push(match w {
                GraphWarning::StaleReference { node, missing, .. } => {
                    format!("{node} references unregistered pipe {missing}")
                }
                other => format!("{other:?}"),
            });
        }
        for e in graph.edges() {
            let (Some(parent), Some(child)) = (graph.node(&e.parent), graph.node(&e.child)) else {
                continue;
            };
            let cursor = self
                .state
                .cursor(&e.state_instance, &e.parent, &e.child)
                .await
                .ok()
                .flatten();
            let latest_seq = self
                .state
                .latest_seq(&e.state_instance, &e.parent)
                .await
                .ok()
                .flatten();
            let (entries, dirty) = match &cursor {
                Some(c) => {
                    let entries = self
                        .state
                        .ledger_after(&e.state_instance, &e.parent, c.seq)
                        .await
                        .unwrap_or_default();
                    let ivs: Vec<Interval> = map_to_child(
                        &dirty_intervals(&entries, &parent.axis),
                        &parent.axis,
                        &child.axis,
                        &e.options,
                    );
                    let dirty = ivs
                        .iter()
                        .map(|iv| DirtyRange {
                            begin: iv.begin.and_then(|p| child.axis.bound(p)),
                            end: iv.end.and_then(|p| child.axis.bound(p)),
                        })
                        .collect();
                    (entries.len(), dirty)
                }
                None => (0, Vec::new()),
            };
            let mut sources = Vec::new();
            if e.sources.parents {
                sources.push("parents".to_string());
            }
            if e.sources.children {
                sources.push("children".to_string());
            }
            if e.sources.template {
                sources.push("template".to_string());
            }
            view.edges.push(EdgeView {
                parent: e.parent.clone(),
                child: e.child.clone(),
                sources,
                state_instance: e.state_instance.clone(),
                has_cursor: cursor.is_some(),
                cursor_seq: cursor.map(|c| c.seq),
                latest_seq,
                pending_entries: entries,
                dirty,
            });
        }
        Ok(view)
    }
}
