//! The engine's read API for the console (§5.7, §5.9, §8.2): run history, per-run Gantt data,
//! the pipe graph with status and ledger lag on its edges, and persisted events to poll.
//!
//! Everything here reads the instance-backed engine state (the `mrsm:engine_*` pipes), so a
//! console in a different process from the run sees the same data. Nothing here writes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use meerschaum_connector::PipesKeysQuery;
use meerschaum_core::success::Bound;
use meerschaum_core::{Map, Value};
use serde::{Deserialize, Serialize};

use crate::axis::Interval;
use crate::engine::{EngineConfig, fetch_records};
use crate::events::EventRow;
use crate::graph::{Graph, GraphOptions, GraphWarning};
use crate::instance_state::{InstanceState, run_from_doc};
use crate::ledger::{EdgeCursor, LedgerEntry, dirty_intervals, map_to_child};
use crate::node::PipeRef;
use crate::plan::Action;
use crate::registry::ConnectorRegistry;
use crate::runs::{EnginePipe, NodeRow, NodeStatus, RunRecord, RunRow};
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
    /// Runs of the live saved Pipeline UUID, or this exact name for unsaved selections.
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

/// How many runs of one name started in one time bucket with one status
/// ([`EngineReader::run_activity`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityBucket {
    /// When the bucket begins (a multiple of the bucket size since the Unix epoch).
    pub start: DateTime<Utc>,
    /// The run name ([`crate::runs::run_name`]), `None` for IDs not made by the engine.
    pub name: Option<String>,
    /// The runs' status.
    pub status: RunStatus,
    /// How many.
    pub count: u64,
}

/// One run named in [`RunActivity::names`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityRun {
    /// The run ID.
    pub run_id: String,
    /// When it started.
    pub started: DateTime<Utc>,
    /// Its status.
    pub status: RunStatus,
}

/// The latest runs of one run name in [`RunActivity`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameActivity {
    /// The run name.
    pub name: String,
    /// The newest run.
    pub latest: ActivityRun,
    /// The newest run that succeeded.
    pub last_success: Option<ActivityRun>,
    /// The newest run that failed.
    pub last_failure: Option<ActivityRun>,
}

/// The runs started since a time, counted per bucket, name and status, from one light read
/// (no selections, targets or node rows): what an activity chart draws.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunActivity {
    /// The counts, oldest bucket first.
    pub buckets: Vec<ActivityBucket>,
    /// Per run name, its latest runs, most recently run first.
    pub names: Vec<NameActivity>,
    /// How many runs were read.
    pub runs: u64,
    /// When the oldest run read started.
    pub oldest: Option<DateTime<Utc>>,
    /// Whether the read stopped at its limit: buckets before `oldest` were not read.
    pub truncated: bool,
}

/// What [`EngineReader::health`] reads: a window of runs and nodes, narrowed like the
/// console's Performance page (one Pipe, one saved Pipeline, the pipes a viewer may read).
#[derive(Debug, Clone)]
pub struct HealthQuery {
    /// Runs and nodes started at or after this time.
    pub since: DateTime<Utc>,
    /// The end of the window (the charts' last bucket ends here); normally now.
    pub until: DateTime<Utc>,
    /// How many buckets the run charts have (at least one).
    pub buckets: usize,
    /// Only this pipe (`pipe_ref`): its runs, status, edges and nodes.
    pub pipe: Option<String>,
    /// Only the runs of this saved Pipeline (by the names in their selections) and their pipes.
    pub pipeline: Option<String>,
    /// Only these pipes (`pipe_ref`s); `None` is every pipe. A run counts when one of its nodes
    /// is visible; an edge, when both its ends are.
    pub visible: Option<std::collections::BTreeSet<String>>,
    /// The most runs, nodes and ledger entries read, each (the newest).
    pub limit: usize,
}

impl HealthQuery {
    /// The last day, 48 buckets, every pipe, at most 20,000 rows of each kind.
    pub fn last_day() -> HealthQuery {
        let until = Utc::now();
        HealthQuery {
            since: until - ChronoDuration::hours(24),
            until,
            buckets: 48,
            pipe: None,
            pipeline: None,
            visible: None,
            limit: 20_000,
        }
    }

    fn sees(&self, pipe_ref: &str) -> bool {
        self.visible
            .as_ref()
            .is_none_or(|visible| visible.contains(pipe_ref))
    }
}

/// The runs of one chart bucket ([`RunHealth::buckets`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunBucket {
    /// When the bucket begins.
    pub start: DateTime<Utc>,
    /// Runs that finished successfully.
    pub succeeded: u64,
    /// Runs that finished with a failure.
    pub failed: u64,
    /// Runs not finished yet.
    pub unfinished: u64,
    /// The average wall-clock seconds of the finished runs.
    pub average_seconds: Option<f64>,
    /// Their 95th percentile (linear interpolation, like `percentile_cont(0.95)`).
    pub p95_seconds: Option<f64>,
    /// The slowest.
    pub max_seconds: Option<f64>,
}

/// A run in [`RunHealth::recent`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthRun {
    /// The run ID.
    pub run_id: String,
    /// The run name ([`crate::runs::run_name`]).
    pub name: Option<String>,
    /// When it started.
    pub started: DateTime<Utc>,
    /// When it finished.
    pub finished: Option<DateTime<Utc>>,
    /// Its status.
    pub status: RunStatus,
    /// Wall-clock seconds (to now while running).
    pub duration_seconds: f64,
    /// Planned nodes.
    pub num_nodes: usize,
}

/// The window's runs ([`EngineHealth::runs`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RunHealth {
    /// Runs started in the window.
    pub runs: u64,
    /// Of those, finished with a failure.
    pub failed: u64,
    /// Of those, not finished.
    pub unfinished: u64,
    /// The 95th percentile of the finished runs' wall-clock seconds.
    pub p95_seconds: Option<f64>,
    /// The length of a bucket.
    pub bucket_seconds: i64,
    /// Every bucket of the window, oldest first.
    pub buckets: Vec<RunBucket>,
    /// The newest runs (at most ten).
    pub recent: Vec<HealthRun>,
}

/// One pipe's freshness ([`FreshnessHealth::pipes`]): its recorded sync status and how far it
/// is behind its parents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipeFreshness {
    /// The pipe.
    pub pipe_ref: String,
    /// Whether its latest sync succeeded.
    pub success: bool,
    /// When its latest sync ended.
    pub last_attempt: Option<DateTime<Utc>>,
    /// When its latest successful sync ended.
    pub last_success: Option<DateTime<Utc>>,
    /// When its latest failed sync ended.
    pub last_failure: Option<DateTime<Utc>>,
    /// Failed syncs since the last success.
    pub consecutive_failures: u64,
    /// How long the latest sync took.
    pub duration_seconds: Option<f64>,
    /// What ran the latest sync (`cli`, `job`, `engine`, `api`, `python`).
    pub source: String,
    /// The job it ran in.
    pub job: Option<String>,
    /// Seconds of parent data it has not consumed: the newest pending parent change's end
    /// minus the parent's watermark when the child last consumed (the most over its parents;
    /// zero when caught up; `None` without a datetime axis or a cursor).
    pub lag_seconds: Option<f64>,
    /// Unconsumed parent changes (ledger entries), over its parents.
    pub pending: u64,
}

/// [`EngineHealth::freshness`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FreshnessHealth {
    /// Pipes whose latest sync failed.
    pub failing: u64,
    /// Pipes without a successful sync.
    pub never_succeeded: u64,
    /// The longest time since a pipe's last successful sync.
    pub longest_since_success_seconds: Option<f64>,
    /// Edges whose child has not consumed its parent's changes.
    pub behind: u64,
    /// The pipes, failing longest first, then longest since a success (at most 500).
    pub pipes: Vec<PipeFreshness>,
}

/// One child's dirty-range backlog ([`BacklogHealth::children`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChildBacklog {
    /// The child (`None`: parents whose children have never consumed a change).
    pub child_ref: Option<PipeRef>,
    /// The parents with changes it has not consumed.
    pub parents: Vec<PipeRef>,
    /// Unconsumed changes.
    pub pending: u64,
    /// When the oldest was recorded.
    pub oldest_pending: Option<DateTime<Utc>>,
    /// Seconds since then.
    pub waited_seconds: Option<f64>,
    /// The earliest dirty bound.
    pub dirty_begin: Option<Bound>,
    /// The latest dirty bound.
    pub dirty_end: Option<Bound>,
}

/// [`EngineHealth::backlog`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BacklogHealth {
    /// Unconsumed changes over every edge.
    pub pending: u64,
    /// The longest a change has waited.
    pub longest_wait_seconds: Option<f64>,
    /// Per child, longest waiting first (at most 500).
    pub children: Vec<ChildBacklog>,
}

/// A node that failed, did not run, or was retried ([`EngineHealth::nodes`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeIssue {
    /// The run.
    pub run_id: String,
    /// When the run started.
    pub run_started: DateTime<Utc>,
    /// The node.
    pub pipe_ref: PipeRef,
    /// The planned action.
    pub action: Action,
    /// The outcome.
    pub status: NodeStatus,
    /// Sync attempts.
    pub attempts: u32,
    /// Chunks synced.
    pub num_chunks: usize,
    /// How long its sync took.
    pub duration_seconds: Option<f64>,
    /// Why the planner chose the action.
    pub reason: String,
}

/// The engine's health on an instance ([`EngineReader::health`]): what the Grafana "Engine"
/// dashboard shows, for the console.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct EngineHealth {
    /// The window's runs.
    pub runs: RunHealth,
    /// Every recorded pipe's sync status and lag.
    pub freshness: FreshnessHealth,
    /// Dirty ranges children have not consumed.
    pub backlog: BacklogHealth,
    /// The window's failed, unrun and retried nodes, newest run first (at most 200).
    pub nodes: Vec<NodeIssue>,
    /// Whether a read stopped at [`HealthQuery::limit`].
    pub truncated: bool,
}

/// `percentile_cont(p)` of `values` (sorted in place).
fn percentile(values: &mut [f64], p: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let position = p.clamp(0.0, 1.0) * (values.len() - 1) as f64;
    let (low, high) = (position.floor() as usize, position.ceil() as usize);
    Some(values[low] + (values[high] - values[low]) * (position - low as f64))
}

fn seconds_between(from: DateTime<Utc>, to: DateTime<Utc>) -> f64 {
    (to - from).num_milliseconds() as f64 / 1000.0
}

/// An order on bounds of either kind (timestamps by microsecond).
fn bound_key(bound: &Bound) -> (u8, i64) {
    match bound {
        Bound::Int(value) => (0, *value),
        Bound::Datetime(time) => (1, time.timestamp_micros()),
    }
}

/// The edges' backlog: `(parent, child, pending, oldest, dirty begin, dirty end, lag)` per edge
/// (a parent without any cursor is one edge without a child, as in the Grafana dashboard).
type EdgeBacklog = (
    PipeRef,
    Option<PipeRef>,
    u64,
    Option<DateTime<Utc>>,
    Option<Bound>,
    Option<Bound>,
    Option<f64>,
);

fn edge_backlog(cursors: &[EdgeCursor], ledger: &[LedgerEntry]) -> Vec<EdgeBacklog> {
    let mut by_parent: BTreeMap<&PipeRef, Vec<&LedgerEntry>> = BTreeMap::new();
    for entry in ledger {
        by_parent.entry(&entry.pipe_ref).or_default().push(entry);
    }
    let consumed = |parent: &PipeRef, child: Option<&PipeRef>, seq: i64, watermark| {
        let pending: Vec<&&LedgerEntry> = by_parent
            .get(parent)
            .map(|entries| entries.iter().filter(|e| e.seq > seq).collect())
            .unwrap_or_default();
        let begin = pending.iter().filter_map(|e| e.begin).min_by_key(bound_key);
        let end = pending.iter().filter_map(|e| e.end).max_by_key(bound_key);
        let lag = if pending.is_empty() {
            Some(0.0)
        } else {
            match (end, watermark) {
                (Some(Bound::Datetime(end)), Some(Bound::Datetime(mark))) => {
                    Some(seconds_between(mark, end).max(0.0))
                }
                _ => None,
            }
        };
        (
            parent.clone(),
            child.cloned(),
            pending.len() as u64,
            pending.iter().map(|e| e.created).min(),
            begin,
            end,
            lag,
        )
    };
    let mut out: Vec<EdgeBacklog> = cursors
        .iter()
        .map(|c| {
            consumed(
                &c.parent_ref,
                Some(&c.child_ref),
                c.seq,
                c.parent_watermark_at_cursor,
            )
        })
        .collect();
    let with_cursor: std::collections::BTreeSet<&PipeRef> =
        cursors.iter().map(|c| &c.parent_ref).collect();
    for parent in by_parent.keys() {
        if !with_cursor.contains(parent) {
            out.push(consumed(parent, None, 0, None));
        }
    }
    out
}

/// [`EngineReader::health`] over rows already read.
pub fn aggregate_health(
    query: &HealthQuery,
    now: DateTime<Utc>,
    runs: &[RunRow],
    nodes: &[crate::instance_state::WindowNode],
    statuses: &[meerschaum_connector::sync_status::SyncStatus],
    cursors: &[EdgeCursor],
    ledger: &[LedgerEntry],
) -> EngineHealth {
    use std::collections::BTreeSet;
    let mut nodes_of: HashMap<&str, Vec<&crate::instance_state::WindowNode>> = HashMap::new();
    for node in nodes {
        nodes_of.entry(&node.run_id).or_default().push(node);
    }
    let filtered = query.pipe.is_some() || query.pipeline.is_some() || query.visible.is_some();
    let included: Vec<&RunRow> = runs
        .iter()
        .filter(|run| {
            let members = nodes_of.get(run.run_id.as_str());
            let any = |test: &dyn Fn(&str) -> bool| {
                members.is_some_and(|nodes| nodes.iter().any(|n| test(n.pipe_ref.as_str())))
            };
            !filtered
                || (query
                    .pipeline
                    .as_ref()
                    .is_none_or(|name| run.belongs_to_pipeline(name))
                    && query
                        .pipe
                        .as_ref()
                        .is_none_or(|pipe| any(&|r| r == pipe.as_str()))
                    && (query.visible.is_none() || any(&|r| query.sees(r))))
        })
        .collect();
    let run_ids: BTreeSet<&str> = included.iter().map(|run| run.run_id.as_str()).collect();
    let unfinished_runs: BTreeSet<&str> = included
        .iter()
        .filter(|run| run.finished.is_none())
        .map(|run| run.run_id.as_str())
        .collect();
    // The pipes of the selected Pipeline: the nodes of its runs in the window.
    let members: Option<BTreeSet<&str>> = query.pipeline.as_ref().map(|_| {
        nodes
            .iter()
            .filter(|n| run_ids.contains(n.run_id.as_str()))
            .map(|n| n.pipe_ref.as_str())
            .collect()
    });
    let selected = |pipe_ref: &str| {
        query.sees(pipe_ref)
            && query.pipe.as_ref().is_none_or(|pipe| pipe == pipe_ref)
            && members
                .as_ref()
                .is_none_or(|members| members.contains(pipe_ref))
    };

    // Runs.
    let span = (query.until - query.since).num_seconds().max(1);
    let count = query.buckets.max(1) as i64;
    let bucket_seconds = (span.unsigned_abs().div_ceil(count.unsigned_abs()) as i64).max(1);
    let mut buckets: Vec<(RunBucket, Vec<f64>)> = (0..count)
        .map(|i| {
            (
                RunBucket {
                    start: query.since + ChronoDuration::seconds(i * bucket_seconds),
                    succeeded: 0,
                    failed: 0,
                    unfinished: 0,
                    average_seconds: None,
                    p95_seconds: None,
                    max_seconds: None,
                },
                Vec::new(),
            )
        })
        .collect();
    let mut health = RunHealth {
        bucket_seconds,
        ..Default::default()
    };
    let mut finished_seconds = Vec::new();
    for run in &included {
        let status = RunStatus::of(run);
        let seconds = seconds_between(run.started, run.finished.unwrap_or(now)).max(0.0);
        health.runs += 1;
        let index = ((run.started - query.since).num_seconds() / bucket_seconds).clamp(0, count - 1)
            as usize;
        let (bucket, durations) = &mut buckets[index];
        match status {
            RunStatus::Running => {
                health.unfinished += 1;
                bucket.unfinished += 1;
            }
            RunStatus::Failed => {
                health.failed += 1;
                bucket.failed += 1;
            }
            RunStatus::Succeeded => bucket.succeeded += 1,
        }
        if status != RunStatus::Running {
            finished_seconds.push(seconds);
            durations.push(seconds);
        }
        if health.recent.len() < 10 {
            health.recent.push(HealthRun {
                run_id: run.run_id.clone(),
                name: crate::runs::run_name(&run.run_id).map(str::to_owned),
                started: run.started,
                finished: run.finished,
                status,
                duration_seconds: seconds,
                num_nodes: run.num_nodes,
            });
        }
    }
    health.p95_seconds = percentile(&mut finished_seconds, 0.95);
    health.buckets = buckets
        .into_iter()
        .map(|(mut bucket, mut durations)| {
            if !durations.is_empty() {
                bucket.average_seconds =
                    Some(durations.iter().sum::<f64>() / durations.len() as f64);
                bucket.max_seconds = durations.iter().copied().reduce(f64::max);
                bucket.p95_seconds = percentile(&mut durations, 0.95);
            }
            bucket
        })
        .collect();

    // Backlog: an edge counts when both its ends are selected (or, without a child, its parent).
    let edges: Vec<EdgeBacklog> = edge_backlog(cursors, ledger)
        .into_iter()
        .filter(|(parent, child, ..)| {
            let (parent, child) = (parent.as_str(), child.as_ref().map(PipeRef::as_str));
            query.sees(parent)
                && child.is_none_or(|child| query.sees(child))
                && (selected(parent) || child.is_some_and(selected))
        })
        .collect();
    let mut backlog = BacklogHealth::default();
    let mut children: BTreeMap<Option<PipeRef>, ChildBacklog> = BTreeMap::new();
    let mut lag: HashMap<&str, (u64, Option<f64>)> = HashMap::new();
    let mut behind = 0;
    for (parent, child, pending, oldest, begin, end, edge_lag) in &edges {
        if let Some(child) = child {
            let entry = lag.entry(child.as_str()).or_insert((0, None));
            entry.0 += pending;
            entry.1 = match (entry.1, edge_lag) {
                (Some(a), Some(b)) => Some(a.max(*b)),
                (a, b) => a.or(*b),
            };
            if *pending > 0 {
                behind += 1;
            }
        }
        if *pending == 0 {
            continue;
        }
        backlog.pending += pending;
        let row = children.entry(child.clone()).or_insert(ChildBacklog {
            child_ref: child.clone(),
            parents: Vec::new(),
            pending: 0,
            oldest_pending: None,
            waited_seconds: None,
            dirty_begin: None,
            dirty_end: None,
        });
        if !row.parents.contains(parent) {
            row.parents.push(parent.clone());
        }
        row.pending += pending;
        row.oldest_pending = match (row.oldest_pending, *oldest) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        row.dirty_begin = [row.dirty_begin, *begin]
            .into_iter()
            .flatten()
            .min_by_key(bound_key);
        row.dirty_end = [row.dirty_end, *end]
            .into_iter()
            .flatten()
            .max_by_key(bound_key);
    }
    let mut children: Vec<ChildBacklog> = children
        .into_values()
        .map(|mut row| {
            row.parents.sort();
            row.waited_seconds = row
                .oldest_pending
                .map(|oldest| seconds_between(oldest, now).max(0.0));
            row
        })
        .collect();
    children.sort_by(|a, b| {
        a.oldest_pending
            .cmp(&b.oldest_pending)
            .then_with(|| a.child_ref.cmp(&b.child_ref))
    });
    children.truncate(500);
    backlog.longest_wait_seconds = children
        .iter()
        .filter_map(|row| row.waited_seconds)
        .reduce(f64::max);
    backlog.children = children;

    // Freshness.
    let mut freshness = FreshnessHealth {
        behind,
        ..Default::default()
    };
    for status in statuses {
        if status.connector_keys == meerschaum_connector::policy::INTERNAL_CONNECTOR_KEYS
            || !selected(&status.pipe_ref)
        {
            continue;
        }
        if !status.success && status.last_attempt.is_some() {
            freshness.failing += 1;
        }
        match status.last_success {
            None => freshness.never_succeeded += 1,
            Some(success) => {
                let since = seconds_between(success, now).max(0.0);
                freshness.longest_since_success_seconds = Some(
                    freshness
                        .longest_since_success_seconds
                        .map_or(since, |longest| longest.max(since)),
                );
            }
        }
        let (pending, lag_seconds) = lag
            .get(status.pipe_ref.as_str())
            .copied()
            .unwrap_or((0, None));
        freshness.pipes.push(PipeFreshness {
            pipe_ref: status.pipe_ref.clone(),
            success: status.success,
            last_attempt: status.last_attempt,
            last_success: status.last_success,
            last_failure: status.last_failure,
            consecutive_failures: status.consecutive_failures,
            duration_seconds: status.duration_seconds,
            source: status.source.clone(),
            job: status.job.clone(),
            lag_seconds,
            pending,
        });
    }
    freshness.pipes.sort_by(|a, b| {
        b.consecutive_failures
            .cmp(&a.consecutive_failures)
            .then_with(|| a.last_success.cmp(&b.last_success))
            .then_with(|| a.pipe_ref.cmp(&b.pipe_ref))
    });
    freshness.pipes.truncate(500);

    // Nodes: failed, not run (in a finished run) or retried.
    let issues: Vec<NodeIssue> = nodes
        .iter()
        .filter(|n| run_ids.contains(n.run_id.as_str()) && selected(n.pipe_ref.as_str()))
        .filter(|n| match n.status {
            NodeStatus::Failed | NodeStatus::UpstreamFailed | NodeStatus::RebuildPending => true,
            NodeStatus::Pending => !unfinished_runs.contains(n.run_id.as_str()),
            _ => n.attempts > 1,
        })
        .take(200)
        .map(|n| NodeIssue {
            run_id: n.run_id.clone(),
            run_started: n.run_started,
            pipe_ref: n.pipe_ref.clone(),
            action: n.action,
            status: n.status,
            attempts: n.attempts,
            num_chunks: n.num_chunks,
            duration_seconds: n.duration_seconds,
            reason: n.reason.clone(),
        })
        .collect();

    EngineHealth {
        runs: health,
        freshness,
        backlog,
        nodes: issues,
        truncated: false,
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

/// [`EngineReader::run_activity`] over rows already read (newest first).
fn aggregate_activity(
    rows: &[crate::instance_state::ActivityRow],
    bucket_seconds: i64,
    limit: usize,
) -> RunActivity {
    let bucket_seconds = bucket_seconds.max(1);
    let mut buckets: BTreeMap<(i64, Option<String>, &'static str), (RunStatus, u64)> =
        BTreeMap::new();
    let mut names: Vec<NameActivity> = Vec::new();
    let mut positions: HashMap<String, usize> = HashMap::new();
    let mut ordered: Vec<&crate::instance_state::ActivityRow> = rows.iter().collect();
    ordered.sort_by(|a, b| b.started.cmp(&a.started).then(b.run_id.cmp(&a.run_id)));
    for row in &ordered {
        let status = match (row.finished, row.success) {
            (None, _) => RunStatus::Running,
            (Some(_), Some(false)) => RunStatus::Failed,
            (Some(_), _) => RunStatus::Succeeded,
        };
        let name = crate::runs::run_name(&row.run_id).map(str::to_owned);
        let start = row.started.timestamp().div_euclid(bucket_seconds) * bucket_seconds;
        buckets
            .entry((start, name.clone(), status.name()))
            .or_insert((status, 0))
            .1 += 1;
        let Some(name) = name else {
            continue;
        };
        let run = ActivityRun {
            run_id: row.run_id.clone(),
            started: row.started,
            status,
        };
        let index = *positions.entry(name.clone()).or_insert_with(|| {
            names.push(NameActivity {
                name,
                latest: run.clone(),
                last_success: None,
                last_failure: None,
            });
            names.len() - 1
        });
        let entry = &mut names[index];
        match status {
            RunStatus::Succeeded if entry.last_success.is_none() => entry.last_success = Some(run),
            RunStatus::Failed if entry.last_failure.is_none() => entry.last_failure = Some(run),
            _ => {}
        }
    }
    RunActivity {
        buckets: buckets
            .into_iter()
            .filter_map(|((start, name, _), (status, count))| {
                Some(ActivityBucket {
                    start: DateTime::from_timestamp(start, 0)?,
                    name,
                    status,
                    count,
                })
            })
            .collect(),
        names,
        runs: rows.len() as u64,
        oldest: ordered.last().map(|row| row.started),
        truncated: limit > 0 && rows.len() >= limit,
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

    /// Whether every persisted node still belongs to its captured registration and ACL.
    /// Unknown/legacy/incomplete runs are false. This is private authorization evidence;
    /// callers must not mint new stamps for old rows or expose stored proofs in public JSON.
    pub async fn run_generations(
        &self,
        run_ids: &[String],
    ) -> Result<BTreeMap<String, bool>, StateError> {
        use meerschaum_connector::sync_metrics::{registration_matches, registrations};
        let mut out: BTreeMap<String, bool> =
            run_ids.iter().cloned().map(|id| (id, false)).collect();
        let ids: Vec<String> = out.keys().cloned().collect();
        if ids.is_empty() {
            return Ok(out);
        }
        let home = self
            .registry
            .instance(&self.config.home_instance)
            .map_err(|message| StateError::Unreachable {
                instance: self.config.home_instance.clone(),
                message,
            })?;
        if let Some(remote) =
            home.engine_run_generations(&ids)
                .await
                .map_err(|error| StateError::Unreachable {
                    instance: self.config.home_instance.clone(),
                    message: error.to_string(),
                })?
        {
            for (id, valid) in remote {
                if let Some(current) = out.get_mut(&id) {
                    *current = valid;
                }
            }
            return Ok(out);
        }
        let mut nodes = self.state.load_nodes_for_runs(&ids).await?;
        type InstanceRegistrations = (String, HashMap<String, String>);
        let mut current: HashMap<String, Option<InstanceRegistrations>> = HashMap::new();
        let mut seen = std::collections::HashSet::new();
        for batch in ids.chunks(128) {
            let params: Map = [(
                "run_id".into(),
                Value::List(batch.iter().cloned().map(Value::Str).collect()),
            )]
            .into_iter()
            .collect();
            for doc in self
                .state
                .read_rows(&self.config.home_instance, EnginePipe::Runs, params, None)
                .await?
            {
                let Some(run) = run_from_doc(&doc) else {
                    continue;
                };
                if !out.contains_key(&run.run_id) {
                    continue;
                }
                if !seen.insert(run.run_id.clone()) {
                    out.insert(run.run_id, false);
                    continue;
                }
                let rows = nodes.remove(&run.run_id).unwrap_or_default();
                if run.num_nodes == 0 || rows.len() < run.num_nodes {
                    continue;
                }
                let mut valid = true;
                for node in rows {
                    let reference = node.registration_ref.as_ref().unwrap_or(&node.pipe_ref);
                    let instance = reference.instance_keys();
                    if !current.contains_key(instance) {
                        let snapshot = match self.registry.instance(instance) {
                            Ok(conn) => registrations(conn.as_ref())
                                .await
                                .ok()
                                .map(|rows| (conn.connector_keys(), rows)),
                            Err(_) => None,
                        };
                        current.insert(instance.to_owned(), snapshot);
                    }
                    if !current.get(instance).and_then(Option::as_ref).is_some_and(
                        |(keys, rows)| {
                            registration_matches(
                                reference.as_str(),
                                node.registration_id.as_deref(),
                                keys,
                                rows,
                            )
                        },
                    ) {
                        valid = false;
                        break;
                    }
                }
                out.insert(run.run_id, valid);
            }
        }
        Ok(out)
    }

    /// The runs started at or after `since`, counted per `bucket_seconds`-long bucket, run name
    /// and status, with each name's latest run, last success and last failure. At most `limit`
    /// runs are read (the newest); [`RunActivity::truncated`] says whether that cut the read short.
    pub async fn run_activity(
        &self,
        since: DateTime<Utc>,
        bucket_seconds: i64,
        limit: usize,
    ) -> Result<RunActivity, StateError> {
        let rows = self.state.run_activity_rows(since, limit).await?;
        Ok(aggregate_activity(&rows, bucket_seconds, limit))
    }

    /// The engine's health for the console's Performance page (the Grafana "Engine"
    /// dashboard's panels): the window's runs by outcome and duration, every recorded pipe's
    /// sync status with its lag behind its parents, the children's unconsumed dirty ranges, and
    /// the window's failed, unrun and retried nodes. Runs, nodes, statuses, cursors and ledger
    /// are read from the home instance in one bounded read each; an instance without engine
    /// state reads as empty.
    pub async fn health(&self, query: &HealthQuery) -> Result<EngineHealth, StateError> {
        let home = &self.config.home_instance;
        let limit = query.limit.max(1);
        let runs = self
            .state
            .window_runs(query.since, limit, query.pipeline.is_some())
            .await?;
        let nodes = self.state.window_nodes(query.since, limit).await?;
        let conn = self
            .registry
            .instance(home)
            .map_err(|message| StateError::Unreachable {
                instance: home.clone(),
                message,
            })?;
        let statuses =
            conn.get_sync_statuses(home)
                .await
                .map_err(|error| StateError::Unreachable {
                    instance: home.clone(),
                    message: error.to_string(),
                })?;
        let cursors = self.state.all_cursors(home).await?;
        let ledger = self.state.newest_ledger(home, limit).await?;
        let truncated = runs.len() >= limit || nodes.len() >= limit || ledger.len() >= limit;
        let mut health = aggregate_health(
            query,
            Utc::now(),
            &runs,
            &nodes,
            &statuses,
            &cursors,
            &ledger,
        );
        health.truncated = truncated;
        Ok(health)
    }

    /// Run history, newest first, using indexed pages and bulk node reads.
    pub async fn runs(&self, filter: &RunFilter) -> Result<Vec<RunSummary>, StateError> {
        let limit = filter.limit.unwrap_or(100);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let page_size = limit.clamp(128, 512);
        let pipeline = match filter.run_name.as_deref() {
            Some(name) => self.pipeline_identity(name).await,
            None => None,
        };
        let mut before = None;
        let mut out = Vec::new();
        loop {
            let page = self
                .state
                .recent_runs_query(page_size, filter.since, before)
                .await?;
            let Some(boundary) = page.last().map(|run| run.started) else {
                break;
            };
            let complete = page.len() < page_size;
            let seen: std::collections::HashSet<String> =
                page.iter().map(|run| run.run_id.clone()).collect();
            self.append_summaries(page, filter, pipeline.as_ref(), limit, &mut out)
                .await?;
            if out.len() >= limit || complete {
                break;
            }
            // Complete the entire boundary second before advancing the exclusive cursor:
            // SQLite datetime expressions can discard subsecond precision.
            let ties = self
                .state
                .runs_in_second(boundary)
                .await?
                .into_iter()
                .filter(|run| !seen.contains(&run.run_id))
                .collect();
            self.append_summaries(ties, filter, pipeline.as_ref(), limit, &mut out)
                .await?;
            if out.len() >= limit {
                break;
            }
            before = Some(boundary);
        }
        Ok(out)
    }

    /// One fresh saved identity per named history request, independent of page/run count.
    async fn pipeline_identity(&self, name: &str) -> Option<(String, String)> {
        let conn = self.registry.instance(&self.config.home_instance).ok()?;
        let remote = conn.saved_pipeline_identity(name).await.ok().flatten();
        if remote.is_some() || conn.connector_type() == "api" {
            return remote.filter(|(id, registry)| {
                !id.is_empty() && !id.starts_with("legacy:") && !registry.is_empty()
            });
        }
        let pipe = meerschaum_core::Pipe::builder("mrsm", "pipeline")
            .location(Some(name))
            .instance(&self.config.home_instance)
            .build()
            .ok()?;
        let attributes = conn.get_pipe_attributes(&pipe).await.ok()?;
        let parameters = attributes.get("parameters")?.as_map()?;
        let definition = parameters.get("pipeline")?.as_map()?;
        let id = definition.get("id")?.as_str()?;
        if id.is_empty() || id.starts_with("legacy:") {
            return None;
        }
        Some((id.to_owned(), conn.connector_keys()))
    }

    async fn append_summaries(
        &self,
        runs: Vec<RunRow>,
        filter: &RunFilter,
        pipeline: Option<&(String, String)>,
        limit: usize,
        out: &mut Vec<RunSummary>,
    ) -> Result<(), StateError> {
        let remaining = limit.saturating_sub(out.len());
        let candidates: Vec<RunRow> = runs
            .into_iter()
            .filter(|run| {
                !filter.run_id.as_ref().is_some_and(|id| *id != run.run_id)
                    && !filter
                        .run_name
                        .as_ref()
                        .is_some_and(|name| !match pipeline {
                            Some((id, registry)) => {
                                run.pipeline_definitions().iter().any(|snapshot| {
                                    snapshot.get("id").and_then(serde_json::Value::as_str)
                                        == Some(id.as_str())
                                        && (meerschaum_connector::sync_metrics::snapshot_registry(
                                            snapshot,
                                        ) == Some(registry.as_str())
                                            || (self.config.home_instance.starts_with("api:")
                                                && snapshot.get("registration_registry").is_none()
                                                && snapshot
                                                    .get("registry")
                                                    .and_then(serde_json::Value::as_str)
                                                    == Some(self.config.home_instance.as_str())))
                                })
                            }
                            None => run.belongs_to_pipeline(name),
                        })
                    && !filter.since.is_some_and(|since| run.started < since)
                    && !filter
                        .status
                        .is_some_and(|status| status != RunStatus::of(run))
            })
            .take(if filter.pipe.is_none() {
                remaining
            } else {
                usize::MAX
            })
            .collect();
        let ids: Vec<String> = candidates.iter().map(|run| run.run_id.clone()).collect();
        let mut nodes = self.state.load_nodes_for_runs(&ids).await?;
        for run in candidates {
            let rows = nodes.remove(&run.run_id).unwrap_or_default();
            if let Some(text) = &filter.pipe {
                let label = text.to_lowercase();
                let matches = |pipe: &PipeRef| {
                    pipe.as_str().contains(text.as_str())
                        || pipe.label().to_lowercase().contains(&label)
                        || pipe.label_with_instance().to_lowercase().contains(&label)
                };
                if !run.targets.iter().any(matches)
                    && !rows.iter().any(|node| matches(&node.pipe_ref))
                {
                    continue;
                }
            }
            out.push(summarize(run, &rows));
            if out.len() >= limit {
                break;
            }
        }
        Ok(())
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
        let duration_nodes: Vec<_> = nodes
            .iter()
            .map(|node| (node.pipe_ref.clone(), node.action))
            .collect();
        let history = self
            .state
            .node_duration_history(
                &duration_nodes,
                None,
                Some(run.started),
                self.config.duration_history_limit(),
            )
            .await
            .unwrap_or_default();
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
