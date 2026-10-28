//! The public entry point (§5.2, §5.5, §5.7): [`Engine::run`], [`Engine::plan`], the event
//! stream ([`Engine::subscribe`]), a bare single-pipe sync ([`Engine::sync_pipe`], R1), and
//! continuous mode ([`Engine::run_loop`]) with lap-compatible `--loop --min-seconds N`.
//!
//! An [`Engine`] owns nothing but configuration and shared state handles. Each run:
//!
//! 1. reads every registered pipe (with parameters) from the selection's instances in one
//!    `fetch_pipes_keys` per instance and builds the [`Graph`];
//! 2. resolves the [`Selection`] (flat `-c/-m/-l/-t` selections take their targets from a
//!    filtered `fetch_pipes_keys`, exactly the set v4's `get_pipes` returns);
//! 3. plans against the instance-backed state ([`InstanceState`]), with `--resume` marking the
//!    resumed run's completed nodes;
//! 4. executes with the [`NativeExecutor`] under per-pipe leases ([`InstanceLeases`]);
//! 5. persists the run, its nodes and (batched) its events on the home instance, compacts
//!    consumed ledger entries, and applies retention.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use meerschaum_connector::{PipeKeysEntry, PipesKeysQuery};
use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::success::{Bound, SyncResult};
use meerschaum_core::{Config, Map, Pipe, Value};
use meerschaum_kernel::enforce::PipeInput;
use tokio::sync::broadcast;

mod triggers;

use crate::events::{Event, EventKind, EventRow, EventSink};
use crate::executor::{
    ExecuteContext, ExecutorOptions, LeaseHold, LeaseSource, Limits, Priority, execute,
};
use crate::graph::{Graph, GraphOptions};
use crate::instance_state::InstanceState;
use crate::leases::InstanceLeases;
use crate::ledger::compaction_floor;
use crate::native::{FaultInjector, NativeConfig, NativeExecutor, NativeFlush, NodeSyncOptions};
use crate::node::{PipeRecord, PipeRef};
use crate::plan::{Plan, PlanOptions, plan};
use crate::registry::ConnectorRegistry;
use crate::runs::{NodeStatus, RunRecord};
use crate::schedule::{EwmaDurations, schedule};
use crate::selection::{Pipeline, Selection, Subgraph, Targets};
use crate::state::{StateSource, StateWriter};
use crate::stream::{SinkOptions, StopToken, StreamConfig, SuperviseOptions, run_sink, supervise};
use crate::sync::{SyncArgs, SyncEnv};

/// Which events are persisted to `mrsm:engine_events` (`system.engine.events`, default `all`).
///
/// `all` keeps the chunk-level events (`chunk_started`, `chunk_finished`, `node_planned`,
/// `stream_flushed`) that the console's live run view and `GET /engine/runs/{run_id}/events/stream`
/// replay; they roughly double the rows of a run whose nodes sync one chunk each. Retention
/// (`system.engine.run_history_days`) prunes them with the rest of the run's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventGranularity {
    /// None.
    Off,
    /// Run and node transitions (no `node_planned`, `chunk_*` or `stream_flushed`).
    Nodes,
    /// Every event.
    #[default]
    All,
}

impl EventGranularity {
    fn keeps(&self, kind: &EventKind) -> bool {
        match self {
            EventGranularity::Off => false,
            EventGranularity::All => true,
            EventGranularity::Nodes => !matches!(
                kind,
                EventKind::NodePlanned { .. }
                    | EventKind::ChunkStarted { .. }
                    | EventKind::ChunkFinished { .. }
                    | EventKind::StreamFlushed { .. }
            ),
        }
    }
}

/// Engine configuration (`system.engine.*` plus the pipe defaults it needs).
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Pipe defaults (`meerschaum:instance`, `verify.chunk_minutes`, ...).
    pub defaults: PipeDefaults,
    /// The home instance: runs, nodes and events are stored here, and pipes are read from it
    /// when a run names no instances.
    pub home_instance: String,
    /// `pipes:parameters:fetch:backtrack_minutes`.
    pub default_backtrack_minutes: i64,
    /// `pipes:sync:filter_params_index_limit`.
    pub filter_params_index_limit: usize,
    /// `system.engine.implicit_edges` (R7).
    pub implicit_edges: bool,
    /// The global worker count.
    pub workers: usize,
    /// `system.engine.limits` and `system.engine.python_workers`.
    pub limits: Limits,
    /// `system.engine.auto_rebuild`.
    pub auto_rebuild: bool,
    /// `system.engine.inclusive_cap`: cap children at their parents' watermark plus one unit
    /// (see [`PlanOptions::inclusive_cap`]).
    pub inclusive_cap: bool,
    /// `system.engine.flat_ledger`: write ledger entries in flat runs (laps) too.
    pub flat_ledger: bool,
    /// Persist runs, nodes and events (`false`: memory only).
    pub persist_runs: bool,
    /// `system.engine.events`: persisted event granularity.
    pub events: EventGranularity,
    /// `system.engine.state_flush_ms`.
    pub state_flush: Duration,
    /// `system.engine.run_history_days`.
    pub run_history_days: i64,
    /// Lease TTL (renewed every third).
    pub lease_ttl: Duration,
    /// How long to wait for a lease held elsewhere.
    pub lease_wait: Duration,
    /// How often to poll a held lease.
    pub lease_poll: Duration,
    /// Where machine-local fallback lease files live (`{root}/cache/engine_leases`).
    pub lease_dir: PathBuf,
    /// The lease holder name (a job name, else `mrsm:<host>:<pid>`,
    /// [`meerschaum_core::holder::process_holder`]).
    pub holder: String,
    /// EWMA smoothing of durations.
    pub ewma_alpha: f64,
    /// Expected duration of a node without history (`system.engine.default_duration_seconds`,
    /// default 30): the planned length of its Gantt bar and its CPM duration.
    pub default_duration: Duration,
    /// `system.engine.chunk_pipelining` (R6; accepted, not implemented).
    pub chunk_pipelining: bool,
    /// `system.engine.debounce_seconds`: the minimum time between two triggered runs of a
    /// child of a stream source when the child sets no `min_seconds` of its own (`None`: use
    /// the loop's `--min-seconds`). See `docs/design/v5.0.0.md` §5.10.
    pub debounce_seconds: Option<f64>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            defaults: PipeDefaults::default(),
            home_instance: "sql:main".into(),
            default_backtrack_minutes: 1440,
            filter_params_index_limit: 250,
            implicit_edges: true,
            workers: 4,
            limits: Limits::default(),
            auto_rebuild: false,
            inclusive_cap: true,
            flat_ledger: true,
            persist_runs: true,
            events: EventGranularity::All,
            state_flush: Duration::from_millis(500),
            run_history_days: 30,
            lease_ttl: Duration::from_secs(60),
            lease_wait: Duration::from_secs(30),
            lease_poll: Duration::from_millis(500),
            lease_dir: std::env::temp_dir().join("mrsm_engine_leases"),
            holder: meerschaum_core::holder::process_holder(),
            ewma_alpha: 0.3,
            default_duration: Duration::from_secs(30),
            chunk_pipelining: false,
            debounce_seconds: None,
        }
    }
}

impl EngineConfig {
    /// Read `system.engine.*` from a config registry; `root_dir` locates the lease fallback.
    pub fn from_config(config: &Config, root_dir: Option<&std::path::Path>) -> EngineConfig {
        let d = EngineConfig::default();
        let get = |keys: &[&str]| config.get(keys);
        let int = |keys: &[&str]| get(keys).as_ref().and_then(meerschaum_core::pyfmt::py_int);
        let flag = |keys: &[&str], default: bool| get(keys).map_or(default, |v| v.truthy());
        let mut limits = Limits::default();
        if let Some(Value::Map(m)) = get(&["system", "engine", "limits", "connectors"]) {
            for (k, v) in m {
                if let Some(n) = meerschaum_core::pyfmt::py_int(&v) {
                    limits.connectors.insert(k, n.max(0) as usize);
                }
            }
        }
        if let Some(Value::Map(m)) = get(&["system", "engine", "limits", "instances"]) {
            for (k, v) in m {
                if let Some(n) = meerschaum_core::pyfmt::py_int(&v) {
                    limits.instances.insert(k, n.max(0) as usize);
                }
            }
        }
        limits.python_workers =
            int(&["system", "engine", "python_workers"]).map(|n| n.max(0) as usize);
        let defaults = PipeDefaults::from_config(config);
        EngineConfig {
            home_instance: defaults.instance_keys.clone(),
            defaults,
            default_backtrack_minutes: int(&["pipes", "parameters", "fetch", "backtrack_minutes"])
                .unwrap_or(d.default_backtrack_minutes),
            filter_params_index_limit: int(&["pipes", "sync", "filter_params_index_limit"])
                .map_or(d.filter_params_index_limit, |n| n.max(0) as usize),
            implicit_edges: flag(&["system", "engine", "implicit_edges"], d.implicit_edges),
            workers: int(&["system", "engine", "workers"]).map_or(d.workers, |n| n.max(1) as usize),
            limits,
            auto_rebuild: flag(&["system", "engine", "auto_rebuild"], d.auto_rebuild),
            inclusive_cap: flag(&["system", "engine", "inclusive_cap"], d.inclusive_cap),
            flat_ledger: flag(&["system", "engine", "flat_ledger"], d.flat_ledger),
            persist_runs: flag(&["system", "engine", "persist_runs"], d.persist_runs),
            events: match get(&["system", "engine", "events"])
                .map(|v| meerschaum_core::pyfmt::py_str(&v))
            {
                Some(s) if s == "all" => EventGranularity::All,
                Some(s) if s == "nodes" => EventGranularity::Nodes,
                Some(s) if s == "off" || s == "False" => EventGranularity::Off,
                _ => d.events,
            },
            state_flush: int(&["system", "engine", "state_flush_ms"])
                .map_or(d.state_flush, |ms| Duration::from_millis(ms.max(0) as u64)),
            run_history_days: int(&["system", "engine", "run_history_days"])
                .unwrap_or(d.run_history_days),
            lease_dir: root_dir
                .map(|r| r.join("cache").join("engine_leases"))
                .unwrap_or(d.lease_dir),
            chunk_pipelining: flag(&["system", "engine", "chunk_pipelining"], false),
            default_duration: get(&["system", "engine", "default_duration_seconds"])
                .as_ref()
                .and_then(|v| v.as_f64())
                .filter(|s| s.is_finite() && *s >= 0.0)
                .map_or(d.default_duration, Duration::from_secs_f64),
            debounce_seconds: get(&["system", "engine", "debounce_seconds"])
                .as_ref()
                .and_then(|v| v.as_f64())
                .filter(|s| *s >= 0.0),
            ..d
        }
    }
}

/// Asks the user to confirm rebuilds (interactive runs).
pub type ConfirmFn = Arc<dyn Fn(&Plan) -> bool + Send + Sync>;

/// Stops a [`StopToken`] when dropped (a run that is cancelled instead of finishing).
struct StopOnDrop(StopToken);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// One engine run's options (`sync pipes` flags, `pipe.sync(parents=...)`, `Pipeline.run`).
#[derive(Clone, Default)]
pub struct RunOptions {
    /// The selection.
    pub selection: Option<Selection>,
    /// The instances to read pipes from (default: the home instance).
    pub instances: Vec<String>,
    /// `--begin`.
    pub begin: Option<Bound>,
    /// `--end`.
    pub end: Option<Bound>,
    /// `--yes` / `force` for rebuilds.
    pub yes: bool,
    /// A person can confirm rebuilds.
    pub interactive: bool,
    /// Asked when a rebuild needs confirmation (interactive runs).
    pub confirm: Option<ConfirmFn>,
    /// `--resume <run_id>`.
    pub resume: Option<String>,
    /// `-w`: overrides the configured worker count.
    pub workers: Option<usize>,
    /// Per-node sync flags.
    pub sync: NodeSyncOptions,
    /// Use this run ID.
    pub run_id: Option<String>,
    /// Frozen registered definitions admitted to this run (identity and revision).
    pub pipeline_definitions: Vec<serde_json::Value>,
    /// Nodes to leave out of a flat selection (stream sources handled by continuous mode).
    pub exclude: BTreeSet<PipeRef>,
    /// Nodes held back from this run, with every descendant of theirs (any selection): used by
    /// the stream triggers to honor a child's `min_seconds`. Unlike `exclude`, a held node is
    /// removed after the selection is resolved, so its in-selection parents still run.
    pub hold: BTreeSet<PipeRef>,
    /// `--chunk-pipelining` for this run (R6); `None` uses `system.engine.chunk_pipelining`.
    pub chunk_pipelining: Option<bool>,
    /// Also deliver this run's events here (every event, in emission order, regardless of
    /// `system.engine.events`), in addition to [`Engine::subscribe`]. The Python bindings use
    /// it for `Pipeline.sync(on_event=...)`. `emit` must not block.
    pub on_event: Option<Arc<dyn EventSink>>,
}

impl std::fmt::Debug for RunOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunOptions")
            .field("selection", &self.selection)
            .field("instances", &self.instances)
            .field("resume", &self.resume)
            .field("workers", &self.workers)
            .finish_non_exhaustive()
    }
}

/// A node's final outcome, in completion order.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeOutcome {
    /// The node.
    pub pipe_ref: PipeRef,
    /// `str(pipe)` as v4 prints it.
    pub pipe: String,
    /// The result (failures carry the error as the message).
    pub result: SyncResult,
}

impl NodeOutcome {
    /// v4's per-pipe lap line: `Finished syncing {pipe}:\n{msg}\n` or
    /// `Error while syncing {pipe}:\n{msg}\n` (`actions/sync.py`, `worker_fn`).
    pub fn v4_message(&self) -> String {
        format!(
            "{} {}:\n{}\n",
            if self.result.success {
                "Finished syncing"
            } else {
                "Error while syncing"
            },
            self.pipe,
            self.result.message
        )
    }
}

/// What a run produced.
#[derive(Debug, Clone)]
pub struct RunResult {
    /// Whether the run succeeded (§5.10 "Run outcome").
    pub success: bool,
    /// A summary (or the reason the run could not start).
    pub message: String,
    /// The plan that was executed (`None` if planning failed).
    pub plan: Option<Plan>,
    /// The run record (`None` if the run never started).
    pub record: Option<RunRecord>,
    /// Node outcomes in completion order.
    pub outcomes: Vec<NodeOutcome>,
    /// The critical path of the plan.
    pub critical_path: Vec<PipeRef>,
    /// Graph, selection, state and lease warnings.
    pub warnings: Vec<String>,
    /// Wall-clock duration.
    pub duration: Duration,
}

impl RunResult {
    fn failed(message: impl Into<String>, duration: Duration) -> RunResult {
        RunResult {
            success: false,
            message: message.into(),
            plan: None,
            record: None,
            outcomes: Vec::new(),
            critical_path: Vec::new(),
            warnings: Vec::new(),
            duration,
        }
    }

    /// The run ID, if the run started.
    pub fn run_id(&self) -> Option<&str> {
        self.record.as_ref().map(|r| r.run.run_id.as_str())
    }

    /// The outcome for one node.
    pub fn outcome(&self, pipe_ref: &PipeRef) -> Option<&NodeOutcome> {
        self.outcomes.iter().find(|o| o.pipe_ref == *pipe_ref)
    }

    /// The pipes this run left `rebuild_pending`: their definition changed and the rebuild (clear
    /// the table, re-sync the full range) was not approved. They were not synced.
    pub fn rebuild_pending(&self) -> Vec<PendingRebuild> {
        self.record
            .as_ref()
            .map(|r| {
                r.nodes
                    .iter()
                    .filter(|n| n.status == NodeStatus::RebuildPending)
                    .map(|n| PendingRebuild {
                        pipe_ref: n.pipe_ref.clone(),
                        reason: n.reason.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The v4 lap summary of this run.
    pub fn lap_summary(&self, lap: u64) -> LapSummary {
        let (ok, bad): (Vec<&NodeOutcome>, Vec<&NodeOutcome>) =
            self.outcomes.iter().partition(|o| o.result.success);
        LapSummary {
            lap,
            success_pipes: ok.iter().map(|o| o.pipe.clone()).collect(),
            failure_pipes: bad.iter().map(|o| o.pipe.clone()).collect(),
            duration: self.duration,
            aborted: self.record.is_none(),
            rebuild_pending: self.rebuild_pending(),
            locked: self.locked(),
        }
    }

    /// The pipes this run did not sync because they are locked (`parameters['locks']`: `sync`,
    /// or `drop` / `clear` for a rebuild): their node status is `locked`, not a failure.
    pub fn locked(&self) -> Vec<PipeRef> {
        self.record
            .as_ref()
            .map(|r| {
                r.nodes
                    .iter()
                    .filter(|n| n.status == NodeStatus::Locked)
                    .map(|n| n.pipe_ref.clone())
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// A pipe a run did not sync because its definition changed and the rebuild was not approved
/// (`--yes`, `system.engine.auto_rebuild`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRebuild {
    /// The pipe.
    pub pipe_ref: PipeRef,
    /// Why (what changed and how to approve it).
    pub reason: String,
}

impl PendingRebuild {
    /// How to approve the rebuilds, for the line printed under the list.
    pub const HOW_TO_APPROVE: &'static str = "Approve the rebuild with `--yes` (it clears each table, then re-syncs the full range), or set `system.engine.auto_rebuild: true`.";
}

/// One lap of continuous mode, as v4 reports it.
#[derive(Debug, Clone, PartialEq)]
pub struct LapSummary {
    /// 1-based lap number.
    pub lap: u64,
    /// Pipes that synced, in completion order (`str(pipe)`).
    pub success_pipes: Vec<String>,
    /// Pipes that failed.
    pub failure_pipes: Vec<String>,
    /// How long the lap took.
    pub duration: Duration,
    /// The lap could not start (v4: "Syncing was aborted.").
    pub aborted: bool,
    /// Pipes the lap did not sync, waiting for a rebuild nobody approved (v5 only).
    pub rebuild_pending: Vec<PendingRebuild>,
    /// Pipes the lap did not sync because they are locked (v5 only): not failures.
    pub locked: Vec<PipeRef>,
}

impl LapSummary {
    /// v4's lap message, given the lap duration rendered by `interval_str`.
    pub fn v4_message(&self, duration_text: &str) -> String {
        if self.aborted {
            return "Syncing was aborted.".into();
        }
        let n = self.success_pipes.len() + self.failure_pipes.len();
        let mut message = format!(
            "It took {duration_text} to sync {n} pipe{}\n    ({} succeeded, {} failed).",
            if n != 1 { "s" } else { "" },
            self.success_pipes.len(),
            self.failure_pipes.len()
        );
        if !self.rebuild_pending.is_empty() {
            message.push_str(&format!(
                "\n    {} not synced: rebuild_pending.",
                self.rebuild_pending.len()
            ));
        }
        if !self.locked.is_empty() {
            message.push_str(&format!("\n    {} not synced: locked.", self.locked.len()));
        }
        message
    }

    /// v4's action result: success when any pipe succeeded, or (v5) when nothing failed and
    /// the pipes not synced are locked (a lock is not a failure).
    // PARITY: `_sync_pipes` returns `len(success_pipes) > 0` (meerschaum/actions/sync.py:455),
    // so a lap with one success and many failures is a success.
    pub fn v4_success(&self) -> bool {
        !self.aborted
            && (!self.success_pipes.is_empty()
                || (self.failure_pipes.is_empty() && !self.locked.is_empty()))
    }
}

/// What continuous mode reports, in v4's output order: each pipe as it finishes, the lap
/// summary, then the sleep (or the error cooldown).
#[derive(Debug, Clone, PartialEq)]
pub enum LapEvent {
    /// A pipe finished within a lap.
    PipeFinished(NodeOutcome),
    /// A lap ended.
    LapFinished(LapSummary),
    /// Sleeping `min_seconds` after the lap (`Sleeping for N seconds.`).
    Sleeping {
        /// Seconds.
        seconds: f64,
        /// v4's info line.
        message: String,
    },
    /// The lap failed to run; waiting `cooldown` seconds (v4: `2 * (min_seconds + 1)`, ×1.5 on
    /// consecutive failures).
    Cooldown {
        /// Seconds.
        seconds: f64,
        /// v4's warning line.
        message: String,
    },
    /// Something the person running the loop should know, said once when the loop starts (a
    /// pipe's schedule that is ignored or does not parse).
    Warning {
        /// The warning.
        message: String,
    },
}

/// Continuous-mode options.
#[derive(Debug, Clone, Default)]
pub struct LoopOptions {
    /// `--loop`: keep running laps.
    pub run_loop: bool,
    /// `--listen`: keep stream sources (and v4-pattern connectors) running after the laps.
    pub listen: bool,
    /// `--min-seconds` (v4 default 1).
    pub min_seconds: f64,
    /// Stop after this many laps (tests, `--loop` with a budget).
    pub max_laps: Option<u64>,
    /// The stop signal (`stop jobs`, CTRL-C).
    pub stop: StopToken,
    /// Name each lap's run after this (`{name}-{timestamp}-{suffix}`,
    /// [`crate::runs::new_named_run_id`]): a saved Pipeline's loop. `None`: the default ids. A
    /// configured [`RunOptions::run_id`] still names the first lap only.
    pub run_name: Option<String>,
}

/// The result of [`Engine::run_loop`].
#[derive(Debug, Clone)]
pub struct LoopResult {
    /// Every lap's summary.
    pub laps: Vec<LapSummary>,
    /// v4's final `(success, message)` (the last lap's), given `interval_str` output.
    pub last: Option<RunResult>,
}

/// A plan without execution (`mrsm plan pipes`).
#[derive(Debug, Clone)]
pub struct PlannedRun {
    /// The graph.
    pub graph: Arc<Graph>,
    /// The resolved selection.
    pub subgraph: Subgraph,
    /// The plan.
    pub plan: Plan,
}

/// Fans events out to subscribers, the persisted buffer and the run's outcome collector.
struct RunSink {
    broadcast: broadcast::Sender<Event>,
    persist: Option<Arc<Mutex<Vec<EventRow>>>>,
    granularity: EventGranularity,
    labels: HashMap<PipeRef, String>,
    outcomes: Mutex<Vec<NodeOutcome>>,
    on_lap: Option<Arc<dyn Fn(LapEvent) + Send + Sync>>,
    on_event: Option<Arc<dyn EventSink>>,
    /// The instance that keeps the run (for the process sinks).
    home_instance: String,
}

impl EventSink for RunSink {
    fn emit(&self, event: Event) {
        if let Some(buf) = &self.persist
            && self.granularity.keeps(&event.kind)
        {
            buf.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(event.to_row());
        }
        let outcome = match (&event.kind, &event.pipe_ref) {
            (EventKind::NodeFinished { result }, Some(r)) => Some((r, result.clone())),
            (
                EventKind::NodeFailed {
                    error,
                    will_retry: false,
                    ..
                },
                Some(r),
            ) => Some((r, SyncResult::new(false, error.clone()))),
            _ => None,
        };
        if let Some((r, result)) = outcome {
            let o = NodeOutcome {
                pipe_ref: r.clone(),
                pipe: self.labels.get(r).cloned().unwrap_or_else(|| r.label()),
                result,
            };
            if let Some(cb) = &self.on_lap {
                cb(LapEvent::PipeFinished(o.clone()));
            }
            self.outcomes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(o);
        }
        if let Some(sink) = &self.on_event {
            sink.emit(event.clone());
        }
        for (_, sink) in PROCESS_SINKS
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            sink.emit(&self.home_instance, event.clone());
        }
        let _ = self.broadcast.send(event);
    }
}

/// A consumer of the events of every engine run in this process ([`add_process_event_sink`]).
/// `emit` must not block.
pub trait ProcessEventSink: Send + Sync {
    /// Receive one event of a run kept on `home_instance` (its keys, `sql:main`): run IDs are
    /// unique per instance, so the instance and the run ID together name the run.
    fn emit(&self, home_instance: &str, event: Event);
}

/// Sinks that receive the events of every run in this process ([`add_process_event_sink`]),
/// by registration ID.
static PROCESS_SINKS: std::sync::RwLock<Vec<(u64, Arc<dyn ProcessEventSink>)>> =
    std::sync::RwLock::new(Vec::new());

/// Also deliver the events of every engine run in this process to `sink` (whatever started
/// the run) until the returned registration is dropped. `mrsm start api` installs one for as
/// long as it serves, so the runs its actions start in the server process
/// (`POST /actions/sync`, MCP action tools) stream live to
/// `GET /engine/runs/{run_id}/events/stream`.
#[must_use = "the sink is removed when the registration is dropped"]
pub fn add_process_event_sink(sink: Arc<dyn ProcessEventSink>) -> ProcessEventSinkRegistration {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    PROCESS_SINKS
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .push((id, sink));
    ProcessEventSinkRegistration { id }
}

/// The number of registered process event sinks (for tests).
pub fn process_event_sinks() -> usize {
    PROCESS_SINKS
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .len()
}

/// A sink registered with [`add_process_event_sink`]; dropping it removes the sink (and the
/// process's reference to it).
#[derive(Debug)]
pub struct ProcessEventSinkRegistration {
    id: u64,
}

impl Drop for ProcessEventSinkRegistration {
    fn drop(&mut self) {
        PROCESS_SINKS
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(id, _)| *id != self.id);
    }
}

/// Own a run-id admission lease until the run finishes or is cancelled.
struct RunAdmission {
    _lease: LeaseHold<InstanceLeases>,
    heartbeat: tokio::task::JoinHandle<()>,
}
impl Drop for RunAdmission {
    fn drop(&mut self) {
        self.heartbeat.abort();
    }
}

/// The Syncing Engine.
pub struct Engine {
    registry: Arc<dyn ConnectorRegistry>,
    config: EngineConfig,
    events: broadcast::Sender<Event>,
    state: Arc<InstanceState>,
    leases: Arc<InstanceLeases>,
    faults: Option<Arc<dyn FaultInjector>>,
    cadence: Option<Arc<dyn crate::cadence::Cadence>>,
    last_retention: Mutex<Option<Instant>>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

fn record_from_entry(
    instance: &str,
    e: &PipeKeysEntry,
    registry: &dyn ConnectorRegistry,
) -> PipeRecord {
    let location = e
        .location_key
        .as_deref()
        .filter(|l| !matches!(*l, "None" | "[None]" | "null"));
    let mut rec = PipeRecord::new(instance, &e.connector_keys, &e.metric_key, location);
    match &e.extra {
        Some(Value::Map(params)) => rec.parameters = params.clone(),
        Some(Value::List(tags)) => {
            rec.parameters
                .insert("tags".into(), Value::List(tags.clone()));
        }
        _ => {}
    }
    rec.streaming = registry.stream_source(&e.connector_keys).is_some()
        || registry.legacy_stream(&e.connector_keys).is_some();
    rec.python = registry.is_python(&e.connector_keys);
    rec
}

/// Read every registered pipe of `instance` as graph records (batch `fetch_pipes_keys`; the
/// list form reads each pipe's parameters and identity together). `registry` must resolve
/// `instance`. Preserve that pre-work registration snapshot for status and history privacy.
pub async fn fetch_records(
    registry: &dyn ConnectorRegistry,
    defaults: &PipeDefaults,
    instance: &str,
    query: &PipesKeysQuery,
) -> Result<Vec<PipeRecord>, String> {
    let conn = registry.instance(instance)?;
    let keys = conn
        .registered_pipes(query)
        .await
        .map_err(|e| format!("Failed to read pipes from '{instance}': {e}"))?;
    let mut out = Vec::with_capacity(keys.len());
    for (identity, entry) in keys {
        let mut rec = record_from_entry(instance, &entry, registry);
        rec.pipe_id = identity.map(|identity| identity.to_value());
        if !matches!(entry.extra, Some(Value::Map(_))) {
            // Custom dictionary backends may return only tags. Keep their metadata
            // fallback, and only attach identity to the parameters read atomically with it.
            rec.pipe_id = None;
            if let Ok(pipe) = rec.to_pipe(defaults)
                && let Ok(attributes) = conn.get_pipe_attributes(&pipe).await
                && let Some(Value::Map(parameters)) = attributes.get("parameters")
            {
                rec.parameters = parameters.clone();
                rec.pipe_id = attributes.get("pipe_id").cloned();
            }
        }
        out.push(rec);
    }
    Ok(out)
}

impl Engine {
    /// An engine over `registry`.
    pub fn new(registry: Arc<dyn ConnectorRegistry>, config: EngineConfig) -> Engine {
        let state = Arc::new(InstanceState::new(registry.clone(), &config.home_instance));
        let leases = Arc::new(InstanceLeases::new(
            registry.clone(),
            state.clone(),
            config.lease_dir.clone(),
        ));
        let (events, _) = broadcast::channel(4096);
        Engine {
            registry,
            config,
            events,
            state,
            leases,
            faults: None,
            cadence: None,
            last_retention: Mutex::new(None),
        }
    }

    /// Inject crashes into every node sync (tests).
    pub fn with_faults(mut self, faults: Arc<dyn FaultInjector>) -> Engine {
        self.faults = Some(faults);
        self
    }

    /// Let continuous mode honor `parameters['schedule']` with this schedule parser (§5.5).
    pub fn with_cadence(mut self, cadence: Arc<dyn crate::cadence::Cadence>) -> Engine {
        self.cadence = Some(cadence);
        self
    }

    /// The configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// The instance-backed engine state (run history, ledger, cursors).
    pub fn state(&self) -> &Arc<InstanceState> {
        &self.state
    }

    /// Subscribe to the live event stream of every run (§5.7).
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// A bare `pipe.sync()` (R1): v4 behavior, no graph, no capping, no ledger.
    pub async fn sync_pipe(
        &self,
        pipe: &Pipe,
        input: Option<PipeInput>,
        args: &SyncArgs,
    ) -> SyncResult {
        let env = SyncEnv {
            registry: self.registry.as_ref(),
            defaults: &self.config.defaults,
            default_backtrack_minutes: self.config.default_backtrack_minutes,
            filter_params_index_limit: self.config.filter_params_index_limit,
            known: None,
        };
        crate::sync::sync_pipe(&env, pipe, input, args, None).await
    }

    async fn fetch_records(
        &self,
        instance: &str,
        query: &PipesKeysQuery,
    ) -> Result<Vec<PipeRecord>, String> {
        fetch_records(
            self.registry.as_ref(),
            &self.config.defaults,
            instance,
            query,
        )
        .await
    }

    fn instances(&self, opts: &RunOptions) -> Vec<String> {
        if opts.instances.is_empty() {
            vec![self.config.home_instance.clone()]
        } else {
            opts.instances.clone()
        }
    }

    /// Build the graph and resolve the selection, without planning.
    pub async fn resolve(
        &self,
        opts: &RunOptions,
    ) -> Result<(Arc<Graph>, Subgraph, Selection), String> {
        let mut instances = self.instances(opts);
        let mut selection = opts
            .selection
            .clone()
            .unwrap_or_else(|| Selection::filter(Default::default()));
        if let Some(run_id) = &opts.resume
            && opts.selection.is_none()
            && let Ok(Some(rec)) = self.state.load_run(run_id).await
            && let Some(s) = rec
                .run
                .selection
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
        {
            selection = s;
        }
        for instance in selection.target_instances() {
            if !instances.contains(&instance) {
                instances.push(instance);
            }
        }
        let flat = selection.is_flat();
        let mut records = Vec::new();
        for i in &instances {
            records.extend(self.fetch_records(i, &PipesKeysQuery::default()).await?);
        }
        // Flat filters select exactly what v4's `get_pipes` would: ask the instance.
        if let (true, Targets::Filter(f)) = (flat, &selection.targets)
            && !f.is_empty()
        {
            let query = PipesKeysQuery {
                connector_keys: f.connector_keys.clone(),
                metric_keys: f.metric_keys.clone(),
                location_keys: f.location_keys.iter().map(|l| Some(l.clone())).collect(),
                tags: f.tags.clone(),
                params: None,
            };
            let mut refs = Vec::new();
            for i in &instances {
                if f.instance_keys.as_ref().is_some_and(|k| k != i) {
                    continue;
                }
                refs.extend(
                    self.fetch_records(i, &query)
                        .await?
                        .iter()
                        .map(PipeRecord::pipe_ref),
                );
            }
            selection = Selection {
                targets: Targets::Refs(refs),
                graph: selection.graph,
                parents: selection.parents,
                children: selection.children,
            };
        }
        if !opts.exclude.is_empty() && flat {
            let base: Vec<PipeRef> = match &selection.targets {
                Targets::Refs(r) => r.clone(),
                Targets::Filter(f) => records
                    .iter()
                    .filter(|r| f.matches(r))
                    .map(PipeRecord::pipe_ref)
                    .collect(),
            };
            selection.targets = Targets::Refs(
                base.into_iter()
                    .filter(|r| !opts.exclude.contains(r))
                    .collect(),
            );
        }
        let graph_options = GraphOptions {
            defaults: self.config.defaults.clone(),
            implicit_edges: self.config.implicit_edges,
            default_backtrack_minutes: self.config.default_backtrack_minutes as f64,
        };
        let mut visited: BTreeSet<String> = instances.into_iter().collect();
        let graph = loop {
            let graph = Graph::build(records.clone(), &graph_options);
            let missing: Vec<_> = selection
                .referenced_instances(&graph)
                .difference(&visited)
                .cloned()
                .collect();
            if missing.is_empty() {
                break graph;
            }
            for instance in missing {
                visited.insert(instance.clone());
                records.extend(
                    self.fetch_records(&instance, &PipesKeysQuery::default())
                        .await?,
                );
            }
        };
        let mut sub = Pipeline::new(selection.clone())
            .resolve(&graph)
            .map_err(|e| e.to_string())?;
        if !opts.hold.is_empty() {
            let mut remove = opts.hold.clone();
            for h in &opts.hold {
                remove.extend(sub.descendants(h));
            }
            sub = sub.without(&graph, &remove);
        }
        self.state
            .register_pipes(graph.nodes().map(|n| (n.pipe_ref.clone(), n.pipe.clone())));
        Ok((Arc::new(graph), sub, selection))
    }

    fn plan_options(&self, opts: &RunOptions) -> PlanOptions {
        PlanOptions {
            begin: opts.begin,
            end: opts.end,
            interactive: opts.interactive,
            yes: opts.yes,
            auto_rebuild: self.config.auto_rebuild,
            inclusive_cap: self.config.inclusive_cap,
            ..PlanOptions::default()
        }
    }

    async fn resume_options(
        &self,
        opts: &RunOptions,
        plan_opts: &mut PlanOptions,
    ) -> Result<(), String> {
        if let Some(run_id) = &opts.resume {
            let rec = self
                .state
                .load_run(run_id)
                .await
                .map_err(|e| format!("Cannot read run {run_id}: {e}"))?
                .ok_or_else(|| {
                    format!(
                        "Run {run_id} was not found on '{}'.",
                        self.config.home_instance
                    )
                })?;
            plan_opts.completed = rec.completed_nodes();
            plan_opts.resumed_from = Some(run_id.clone());
        }
        Ok(())
    }

    /// Plan a selection without running it (`mrsm plan pipes`).
    pub async fn plan(&self, opts: &RunOptions) -> Result<PlannedRun, String> {
        let (graph, subgraph, _) = self.resolve(opts).await?;
        let mut plan_opts = self.plan_options(opts);
        self.resume_options(opts, &mut plan_opts).await?;
        let plan = plan(&graph, &subgraph, self.state.as_ref(), &plan_opts).await;
        Ok(PlannedRun {
            graph,
            subgraph,
            plan,
        })
    }

    /// `planned` as the executor is expected to run it: every provisional node re-planned as if
    /// its parents had synced new rows ([`crate::plan::expected`]). `mrsm plan pipes` prints it.
    pub async fn expected_plan(
        &self,
        planned: &PlannedRun,
        opts: &RunOptions,
    ) -> Result<Plan, String> {
        let mut plan_opts = self.plan_options(opts);
        self.resume_options(opts, &mut plan_opts).await?;
        Ok(crate::plan::expected(
            &planned.graph,
            &planned.subgraph,
            &planned.plan,
            self.state.as_ref(),
            &plan_opts,
        )
        .await)
    }

    /// Run a selection to completion (`sync pipes [--parents] [--children]`,
    /// `pipe.sync(parents=...)`, `Pipeline.run`).
    pub async fn run(&self, opts: RunOptions) -> RunResult {
        self.run_inner(opts, None).await
    }

    async fn run_inner(
        &self,
        opts: RunOptions,
        on_lap: Option<Arc<dyn Fn(LapEvent) + Send + Sync>>,
    ) -> RunResult {
        let t0 = Instant::now();
        // A caller-supplied id is an immutable execution identity, never an upsert key.
        // Serialize admission across processes before checking persisted history.
        let _admission = if let Some(run_id) = opts.run_id.as_ref() {
            if run_id.is_empty()
                || run_id.len() > 200
                || !run_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
            {
                return RunResult::failed("Invalid run id.", t0.elapsed());
            }
            let key = PipeRef::new(
                &self.config.home_instance,
                "mrsm",
                "engine_run_admission",
                Some(run_id),
            );
            let holder = crate::runs::new_run_id();
            let ttl = Duration::from_secs(300);
            let lease =
                match LeaseHold::acquire(self.leases.clone(), key.clone(), holder.clone(), ttl)
                    .await
                {
                    Ok(Some(lease)) => lease,
                    Ok(None) => {
                        return RunResult::failed(
                            format!("Run id '{run_id}' is already in use."),
                            t0.elapsed(),
                        );
                    }
                    Err(e) => {
                        return RunResult::failed(
                            format!("Cannot reserve run id '{run_id}': {e}"),
                            t0.elapsed(),
                        );
                    }
                };
            match self.state.load_run(run_id).await {
                Ok(None) => {}
                Ok(Some(_)) => {
                    return RunResult::failed(
                        format!("Run id '{run_id}' already exists; choose a new run id."),
                        t0.elapsed(),
                    );
                }
                Err(e) => {
                    return RunResult::failed(
                        format!("Cannot check run id '{run_id}': {e}"),
                        t0.elapsed(),
                    );
                }
            }
            let leases = self.leases.clone();
            let heartbeat = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    if !leases.renew(&key, &holder, ttl).await.unwrap_or(false) {
                        break;
                    }
                }
            });
            Some(RunAdmission {
                _lease: lease,
                heartbeat,
            })
        } else {
            None
        };
        let (graph, sub, selection) = match self.resolve(&opts).await {
            Ok(x) => x,
            Err(e) => return RunResult::failed(e, t0.elapsed()),
        };
        let mut plan_opts = self.plan_options(&opts);
        if let Err(e) = self.resume_options(&opts, &mut plan_opts).await {
            return RunResult::failed(e, t0.elapsed());
        }
        let mut the_plan = plan(&graph, &sub, self.state.as_ref(), &plan_opts).await;
        if the_plan.needs_confirmation() {
            match &opts.confirm {
                Some(f) if f(&the_plan) => the_plan.confirm_rebuilds(),
                _ => the_plan.decline_rebuilds(),
            }
        }
        if meerschaum_core::debug::enabled() {
            for n in &the_plan.nodes {
                meerschaum_core::dprint!(
                    "Plan {}: {:?} ({}); bounds {:?}; {} chunk(s) {:?}{}",
                    n.pipe_ref.label(),
                    n.action,
                    n.reason,
                    n.bounds,
                    n.chunks.len(),
                    n.chunks.iter().take(3).collect::<Vec<_>>(),
                    if n.chunks.len() > 3 { " ..." } else { "" }
                );
            }
        }
        let mut warnings: Vec<String> = the_plan.warnings.clone();
        for n in &the_plan.nodes {
            warnings.extend(
                n.warnings
                    .iter()
                    .map(|w| format!("{}: {w}", n.pipe_ref.label())),
            );
        }

        let durations = match self
            .state
            .node_history(Some(
                Utc::now() - chrono::TimeDelta::days(self.config.run_history_days),
            ))
            .await
        {
            Ok(rows) => EwmaDurations::from_history(
                &rows,
                self.config.ewma_alpha,
                self.config.default_duration,
            ),
            Err(_) => EwmaDurations::new(self.config.ewma_alpha, self.config.default_duration),
        };
        let critical_path = schedule(&the_plan, &durations).critical_path;

        let native_config = NativeConfig {
            defaults: self.config.defaults.clone(),
            default_backtrack_minutes: self.config.default_backtrack_minutes,
            filter_params_index_limit: self.config.filter_params_index_limit,
            write_ledger: !sub.flat || self.config.flat_ledger,
            sync: opts.sync.clone(),
        };
        let mut native = NativeExecutor::new(
            self.registry.clone(),
            graph.clone(),
            self.state.clone(),
            native_config,
            sub.flat,
        );
        if let Some(f) = &self.faults {
            native = native.with_faults(f.clone());
        }
        let executor = Arc::new(native);

        let persist = (self.config.persist_runs && self.config.events != EventGranularity::Off)
            .then(|| Arc::new(Mutex::new(Vec::new())));
        let sink = Arc::new(RunSink {
            broadcast: self.events.clone(),
            persist: persist.clone(),
            granularity: self.config.events,
            labels: graph
                .nodes()
                .map(|n| (n.pipe_ref.clone(), n.pipe.to_string()))
                .collect(),
            outcomes: Mutex::new(Vec::new()),
            on_lap,
            on_event: opts.on_event.clone(),
            home_instance: self.config.home_instance.clone(),
        });

        // Batched event persistence (`system.engine.state_flush_ms`).
        let flusher_stop = StopToken::new();
        // A run dropped mid-way (Ctrl-C, a closed request) still ends its flusher: it writes
        // what is buffered and returns, instead of waking every `state_flush_ms` for good.
        let _flusher_guard = StopOnDrop(flusher_stop.clone());
        let flusher = persist.clone().map(|buf| {
            let state = self.state.clone();
            let stop = flusher_stop.clone();
            let every = self.config.state_flush.max(Duration::from_millis(10));
            tokio::spawn(async move {
                // A failed write (a SQLite instance busy with a node's long statement) keeps
                // its rows for the next flush, so a run's first events are not lost; a write
                // that keeps failing gives them up after `MAX_FAILURES` flushes.
                const MAX_FAILURES: u32 = 20;
                let mut failures = 0u32;
                loop {
                    let stopped = stop.sleep(every).await;
                    let rows: Vec<EventRow> =
                        std::mem::take(&mut *buf.lock().unwrap_or_else(|e| e.into_inner()));
                    let mut pending = rows;
                    // At the end, a few quick retries before giving up.
                    let attempts = if stopped { 4 } else { 1 };
                    for attempt in 0..attempts {
                        if pending.is_empty() {
                            break;
                        }
                        match state.write_events(&pending).await {
                            Ok(()) => {
                                pending.clear();
                                failures = 0;
                            }
                            Err(e) => {
                                failures += 1;
                                tracing::debug!("Cannot persist engine events: {e}");
                                if attempt + 1 < attempts {
                                    tokio::time::sleep(Duration::from_millis(250)).await;
                                }
                            }
                        }
                    }
                    if !pending.is_empty() && !stopped && failures < MAX_FAILURES {
                        let mut guard = buf.lock().unwrap_or_else(|e| e.into_inner());
                        pending.append(&mut guard);
                        *guard = pending;
                    }
                    if stopped {
                        return;
                    }
                }
            })
        });

        let exec_opts = ExecutorOptions {
            workers: opts.workers.unwrap_or(self.config.workers).max(1),
            limits: self.config.limits.clone(),
            holder: self.config.holder.clone(),
            lease_ttl: self.config.lease_ttl,
            lease_wait: self.config.lease_wait,
            lease_poll: self.config.lease_poll,
            chunk_pipelining: opts
                .chunk_pipelining
                .unwrap_or(self.config.chunk_pipelining),
            run_id: opts.run_id.clone(),
            plan: plan_opts.clone(),
            selection: serde_json::to_value(&selection).ok().map(|mut snapshot| {
                snapshot["pipeline_definitions"] = serde_json::json!(opts.pipeline_definitions);
                snapshot["resolved_targets"] = serde_json::json!(sub.order);
                snapshot.to_string()
            }),
            priority: if sub.flat {
                Priority::PlanOrder
            } else {
                Priority::UpwardRank
            },
        };
        let memory_state;
        let record = if self.config.persist_runs {
            let ctx = ExecuteContext {
                graph: &graph,
                subgraph: &sub,
                state: self.state.as_ref(),
                executor: executor.clone(),
                leases: self.leases.clone(),
                sink: sink.clone(),
                durations: &durations,
            };
            execute(ctx, the_plan.clone(), &exec_opts).await
        } else {
            // Run history stays in memory; cursors and hashes still go to the instances.
            memory_state = NoRunHistory(self.state.clone());
            let ctx = ExecuteContext {
                graph: &graph,
                subgraph: &sub,
                state: &memory_state,
                executor: executor.clone(),
                leases: self.leases.clone(),
                sink: sink.clone(),
                durations: &durations,
            };
            execute(ctx, the_plan.clone(), &exec_opts).await
        };
        flusher_stop.stop();
        if let Some(h) = flusher {
            let _ = h.await;
        }
        // The nodes' sync statuses (written in the background as each node ended).
        self.state
            .flush_sync_statuses(
                meerschaum_connector::sync_status::RECORD_TIMEOUT + Duration::from_secs(1),
            )
            .await;

        // Compact consumed ledger entries of the nodes that ran, then retention.
        if !sub.flat || self.config.flat_ledger {
            let ran: Vec<PipeRef> = record
                .nodes
                .iter()
                .filter(|n| n.status == NodeStatus::Succeeded)
                .map(|n| n.pipe_ref.clone())
                .collect();
            for r in ran.iter().chain(sub.edges.iter().map(|e| &e.parent)) {
                self.compact(&graph, r).await;
            }
        }
        self.retention().await;

        // A locked pipe was not synced: say so where every caller shows warnings (a locked node
        // has no outcome of its own).
        warnings.extend(
            record
                .nodes
                .iter()
                .filter(|n| n.status == NodeStatus::Locked)
                .map(|n| format!("{}: {}", n.pipe_ref.label(), n.reason)),
        );
        warnings.extend(self.leases.warnings());
        let outcomes =
            std::mem::take(&mut *sink.outcomes.lock().unwrap_or_else(|e| e.into_inner()));
        RunResult {
            success: record.run.success.unwrap_or(false),
            message: record.run.message.clone().unwrap_or_default(),
            plan: Some(the_plan),
            record: Some(record),
            outcomes,
            critical_path,
            warnings,
            duration: t0.elapsed(),
        }
    }

    /// Prune a parent's ledger entries every child with a cursor has consumed (§5.4).
    async fn compact(&self, graph: &Graph, parent: &PipeRef) {
        let mut by_instance: HashMap<String, Vec<Option<i64>>> = HashMap::new();
        for e in graph.child_edges(parent) {
            let seq = self
                .state
                .cursor(&e.state_instance, parent, &e.child)
                .await
                .ok()
                .flatten()
                .map(|c| c.seq);
            by_instance
                .entry(e.state_instance.clone())
                .or_default()
                .push(seq);
        }
        for (instance, seqs) in by_instance {
            if let Some(floor) = compaction_floor(seqs)
                && let Err(e) = self.state.prune_ledger(&instance, parent, floor).await
            {
                tracing::debug!("Cannot compact the ledger of {}: {e}", parent.label());
            }
        }
    }

    /// Retention, at most hourly: runs, nodes and events older than `run_history_days`
    /// (consumed ledger entries are compacted after every run instead).
    async fn retention(&self) {
        if !self.config.persist_runs || self.config.run_history_days <= 0 {
            return;
        }
        {
            let mut last = self
                .last_retention
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if last.is_some_and(|t| t.elapsed() < Duration::from_secs(3600)) {
                return;
            }
            *last = Some(Instant::now());
        }
        let before = Utc::now() - chrono::TimeDelta::days(self.config.run_history_days);
        for e in self.state.prune_history(before).await {
            tracing::debug!("Engine retention: {e}");
        }
    }

    /// Continuous mode. On a flat selection, `--loop --min-seconds N` behaves as v4 laps did:
    /// every selected pipe syncs, then the engine sleeps `N` seconds **after** the lap ends
    /// (`actions/sync.py:419`); a lap that cannot run waits `2 * (N + 1)` seconds, growing
    /// ×1.5 on consecutive failures (`actions/sync.py:320`, `:377`). Selections with
    /// `--parents` / `--children` run one engine run per lap, so children only sync when the
    /// planner sees new parent data. With `--listen` (or `--loop`), stream sources run under
    /// supervision for as long as the loop does.
    pub async fn run_loop(
        &self,
        opts: RunOptions,
        loop_opts: LoopOptions,
        on_lap: Arc<dyn Fn(LapEvent) + Send + Sync>,
    ) -> LoopResult {
        let stop = loop_opts.stop.clone();
        let min_seconds = loop_opts.min_seconds;
        let base_cooldown = 2.0 * (min_seconds + 1.0);
        let mut cooldown = base_cooldown;
        let mut laps = Vec::new();
        let mut last: Option<RunResult> = None;
        let mut opts = opts;
        let mut streams: Option<tokio::task::JoinSet<()>> = None;
        let mut triggers: Option<triggers::Triggers> = None;
        let mut lap = 0u64;
        let mut legacy_outcomes: Vec<NodeOutcome> = Vec::new();
        // `parameters['schedule']` (§5.5): root nodes with a schedule only sync when due.
        let mut schedules = crate::cadence::PipeSchedules::default();
        let mut schedules_flat = true;
        if let (true, Some(cadence)) = (loop_opts.run_loop, self.cadence.as_ref())
            && let Ok((graph, sub, _)) = self.resolve(&opts).await
        {
            // Every pipe of a flat selection is a root (v4 semantics); in a graph run only the
            // pipes without parents in the run follow their schedule.
            let roots = sub
                .order
                .iter()
                .filter(|r| sub.flat || sub.parent_edges(r).next().is_none())
                .filter_map(|r| graph.node(r).map(|n| (&n.pipe_ref, &n.record.parameters)));
            let (parsed, mut warnings) =
                crate::cadence::PipeSchedules::parse(cadence.as_ref(), roots, Utc::now());
            // Said once per loop (S4): a scheduled pipe with parents does not follow its schedule.
            warnings.extend(crate::cadence::ignored_schedules(&graph, &sub));
            for message in warnings {
                tracing::warn!("{message}");
                on_lap(LapEvent::Warning { message });
            }
            schedules = parsed;
            schedules_flat = sub.flat;
        }
        loop {
            if stop.is_stopped() {
                break;
            }
            if streams.is_none()
                && (loop_opts.listen || loop_opts.run_loop)
                && let Ok((graph, sub, _)) = self.resolve(&opts).await
            {
                let hub = triggers::TriggerHub::new();
                let (set, excluded, emitter, starts) =
                    self.spawn_streams(&graph, &sub, &stop, &opts.sync, &hub);
                // PARITY: v4's `--listen` lap called each v4-pattern connector's `sync()`
                // (which subscribes and returns) and reported it like any pipe; the first
                // lap reports those returns the same way.
                legacy_outcomes = starts.collect(&stop, &on_lap).await;
                if !excluded.is_empty() {
                    opts.exclude.extend(excluded.clone());
                    triggers = Some(triggers::Triggers::new(
                        hub,
                        graph.clone(),
                        sub,
                        opts.clone(),
                        min_seconds,
                        emitter,
                    ));
                }
                streams = Some(set);
            }
            if let Some(t) = triggers.as_mut() {
                self.run_triggers(t).await;
            }
            let mut lap_opts = opts.clone();
            let mut due_now: Vec<PipeRef> = Vec::new();
            let lap_now = Utc::now();
            if !schedules.is_empty() {
                let held = schedules.not_due(lap_now);
                due_now = schedules
                    .nodes()
                    .into_iter()
                    .filter(|r| !held.contains(r))
                    .collect();
                if schedules_flat {
                    lap_opts.exclude.extend(held.iter().cloned());
                } else {
                    lap_opts.hold.extend(held.iter().cloned());
                }
                if !held.is_empty() {
                    let idle = match self.resolve(&lap_opts).await {
                        Ok((_, sub, _)) => sub.order.is_empty(),
                        Err(_) => false,
                    };
                    if idle {
                        // Nothing is due: wait for the earliest fire time.
                        let wait = schedules
                            .next_due()
                            .map(|t| (t - lap_now).to_std().unwrap_or(Duration::ZERO))
                            .unwrap_or_else(|| Duration::from_secs_f64(min_seconds.max(1.0)));
                        if self
                            .wait_with_triggers(triggers.as_mut(), &stop, Some(wait))
                            .await
                        {
                            break;
                        }
                        continue;
                    }
                }
            }
            lap += 1;
            // A configured first-lap correlation id must not overwrite later laps.
            if lap > 1 {
                lap_opts.run_id = None;
            }
            if lap_opts.run_id.is_none()
                && let Some(name) = loop_opts.run_name.as_deref()
            {
                lap_opts.run_id = Some(crate::runs::new_named_run_id(name));
            }
            let mut result = self.run_inner(lap_opts, Some(on_lap.clone())).await;
            if let Some(t) = triggers.as_mut() {
                t.note_ran(result.outcomes.iter().map(|o| &o.pipe_ref));
            }
            if !legacy_outcomes.is_empty() && result.record.is_some() {
                let mut outcomes = std::mem::take(&mut legacy_outcomes);
                outcomes.append(&mut result.outcomes);
                result.outcomes = outcomes;
            }
            for r in &due_now {
                schedules.ran(r, lap_now);
            }
            if result.record.is_none() {
                // The lap could not run (graph or instance failure): v4's cooldown.
                let message = format!(
                    "Failed to sync all pipes. Waiting for {} seconds, then trying again.",
                    meerschaum_core::pyfmt::py_str(&py_number(cooldown))
                );
                tracing::warn!("{}: {message}", result.message);
                on_lap(LapEvent::Cooldown {
                    seconds: cooldown,
                    message,
                });
                if self
                    .wait_with_triggers(
                        triggers.as_mut(),
                        &stop,
                        Some(Duration::from_secs_f64(cooldown.max(0.0))),
                    )
                    .await
                {
                    last = Some(result);
                    break;
                }
                cooldown = (cooldown * 1.5).trunc();
                last = Some(result);
                if loop_opts.max_laps.is_some_and(|m| lap >= m) {
                    break;
                }
                continue;
            }
            cooldown = base_cooldown;
            let summary = result.lap_summary(lap);
            on_lap(LapEvent::LapFinished(summary.clone()));
            laps.push(summary);
            last = Some(result);
            if !loop_opts.run_loop || loop_opts.max_laps.is_some_and(|m| lap >= m) {
                break;
            }
            if min_seconds > 0.0 {
                // A schedule due sooner than `--min-seconds` shortens the sleep.
                let sleep_seconds = schedules
                    .next_due()
                    .map(|t| (t - Utc::now()).num_milliseconds().max(0) as f64 / 1000.0)
                    .map_or(min_seconds, |d| d.min(min_seconds));
                on_lap(LapEvent::Sleeping {
                    seconds: sleep_seconds,
                    message: format!(
                        "Sleeping for {} second{}.",
                        meerschaum_core::pyfmt::py_str(&py_number(sleep_seconds)),
                        if sleep_seconds.abs() != 1.0 { "s" } else { "" }
                    ),
                });
                if self
                    .wait_with_triggers(
                        triggers.as_mut(),
                        &stop,
                        Some(Duration::from_secs_f64(sleep_seconds)),
                    )
                    .await
                {
                    break;
                }
            }
        }
        if let Some(mut set) = streams {
            if loop_opts.listen && !loop_opts.run_loop {
                self.wait_with_triggers(triggers.as_mut(), &stop, None)
                    .await;
            }
            stop.stop();
            while set.join_next().await.is_some() {}
        }
        LoopResult { laps, last }
    }

    /// Start supervised subscriptions for the selected stream sources. Returns the tasks and
    /// the nodes they own (left out of flat laps).
    fn spawn_streams(
        &self,
        graph: &Arc<Graph>,
        sub: &Subgraph,
        stop: &StopToken,
        sync: &NodeSyncOptions,
        hub: &Arc<triggers::TriggerHub>,
    ) -> (
        tokio::task::JoinSet<()>,
        BTreeSet<PipeRef>,
        crate::events::Emitter,
        LegacyStarts,
    ) {
        let mut set = tokio::task::JoinSet::new();
        let mut owned = BTreeSet::new();
        let (starts_tx, starts_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut starts = LegacyStarts {
            rx: starts_rx,
            expected: 0,
        };
        let native_config = NativeConfig {
            defaults: self.config.defaults.clone(),
            default_backtrack_minutes: self.config.default_backtrack_minutes,
            filter_params_index_limit: self.config.filter_params_index_limit,
            write_ledger: true,
            sync: sync.clone(),
        };
        let sink_options = SinkOptions {
            skip_check_existing: !sync.check_existing,
            ..Default::default()
        };
        let mut native = NativeExecutor::new(
            self.registry.clone(),
            graph.clone(),
            self.state.clone(),
            native_config,
            false,
        );
        // Crash injection reaches stream flushes too (§14).
        if let Some(f) = &self.faults {
            native = native.with_faults(f.clone());
        }
        let executor = Arc::new(native);
        let emitter = crate::events::Emitter::new(
            &crate::runs::new_run_id(),
            Arc::new(triggers::FlushTap {
                hub: hub.clone(),
                broadcast: self.events.clone(),
            }),
        );
        for r in &sub.order {
            let Some(node) = graph.node(r) else { continue };
            let ck = node.record.connector_keys.clone();
            if let Some(source) = self.registry.stream_source(&ck) {
                owned.insert(r.clone());
                let pipe = node.pipe.clone();
                let pipe_ref = r.clone();
                let config = StreamConfig::from_parameters(pipe.parameters());
                let flusher = NativeFlush::new(executor.clone());
                let registry = self.registry.clone();
                let stop = stop.clone();
                let emitter = emitter.clone();
                set.spawn(async move {
                    let (tx, rx) = tokio::sync::mpsc::channel(1024);
                    let sub_source = source.clone();
                    let sub_pipe = pipe.clone();
                    let sub_stop = stop.clone();
                    let sub_emitter = emitter.clone();
                    let sub_registry = registry.clone();
                    let subscriber = async move {
                        supervise(
                            &PipeRef::from_pipe(&sub_pipe),
                            || {
                                let tx = tx.clone();
                                let s = sub_source.clone();
                                let p = sub_pipe.clone();
                                let st = sub_stop.clone();
                                let registry = sub_registry.clone();
                                async move {
                                    let result = s.subscribe(&p, tx, st).await;
                                    record_stream_failure(registry.as_ref(), &p, &result);
                                    result
                                }
                            },
                            SuperviseOptions::default(),
                            Some(&sub_emitter),
                            &sub_stop,
                        )
                        .await;
                    };
                    let sink = run_sink(
                        &pipe,
                        config,
                        sink_options,
                        rx,
                        source.as_ref(),
                        &flusher,
                        Some(&emitter),
                        &stop,
                    );
                    let _ = tokio::join!(subscriber, sink);
                    let _ = pipe_ref;
                });
            } else if let Some(legacy) = self.registry.legacy_stream(&ck) {
                owned.insert(r.clone());
                let pipe = node.pipe.clone();
                let stop = stop.clone();
                let emitter = emitter.clone();
                // Every callback `pipe.sync()` is a flush: ledger first, `stream_flushed`
                // (trigger `callback`), children triggering.
                if legacy.reports_return() {
                    starts.expected += 1;
                }
                let sink: Arc<dyn crate::stream::LegacySink> = Arc::new(
                    crate::native::NativeLegacySink::new(executor.clone(), Some(emitter.clone()))
                        .with_outcomes(starts_tx.clone()),
                );
                let registry = self.registry.clone();
                set.spawn(async move {
                    supervise(
                        &PipeRef::from_pipe(&pipe),
                        || {
                            let l = legacy.clone();
                            let p = pipe.clone();
                            let st = stop.clone();
                            let sink = sink.clone();
                            let registry = registry.clone();
                            async move {
                                let result = l.run(&p, sink, st).await;
                                record_stream_failure(registry.as_ref(), &p, &result);
                                result
                            }
                        },
                        SuperviseOptions::default(),
                        Some(&emitter),
                        &stop,
                    )
                    .await;
                });
            }
        }
        (set, owned, emitter, starts)
    }
}

/// A stream that stopped with an error (its subscription failed or its connector's `sync()`
/// failed) is a failed sync of its pipe: record it in the pipe's sync status. Its flushes and
/// callback syncs record their own outcomes.
fn record_stream_failure(
    registry: &dyn ConnectorRegistry,
    pipe: &Pipe,
    result: &Result<(), String>,
) {
    if let Err(error) = result {
        let failed = SyncResult::new(false, format!("Listening for new data stopped: {error}"));
        crate::status::record_result(registry, pipe, &failed, false);
    }
}

/// How long the first lap of `--listen` waits for v4-pattern connectors' `sync()` to return
/// before it runs (a connector whose `sync()` blocks is reported as still starting).
const LEGACY_START_WAIT: Duration = Duration::from_secs(30);

/// The `sync()` returns of the v4-pattern streams the loop started, for its first lap.
struct LegacyStarts {
    rx: tokio::sync::mpsc::UnboundedReceiver<NodeOutcome>,
    expected: usize,
}

impl LegacyStarts {
    /// Wait (bounded by [`LEGACY_START_WAIT`] or `stop`) for every expected return, reporting
    /// each as a `PipeFinished` lap event in arrival order.
    async fn collect(
        mut self,
        stop: &StopToken,
        on_lap: &Arc<dyn Fn(LapEvent) + Send + Sync>,
    ) -> Vec<NodeOutcome> {
        let mut out = Vec::new();
        let deadline = tokio::time::Instant::now() + LEGACY_START_WAIT;
        while out.len() < self.expected {
            let next = tokio::select! {
                _ = stop.stopped() => break,
                _ = tokio::time::sleep_until(deadline) => break,
                next = self.rx.recv() => next,
            };
            let Some(outcome) = next else { break };
            on_lap(LapEvent::PipeFinished(outcome.clone()));
            out.push(outcome);
        }
        out
    }
}

/// A float as Python prints it in v4 messages (`20` for an int-valued float from `int` args).
fn py_number(x: f64) -> Value {
    if x.fract() == 0.0 && x.abs() < 1e15 {
        Value::Int(x as i64)
    } else {
        Value::Float(x)
    }
}

/// Instance state that keeps run history out of the instance (`persist_runs: false`).
struct NoRunHistory(Arc<InstanceState>);

impl StateSource for NoRunHistory {
    async fn watermark(&self, node: &PipeRef) -> Result<Option<Bound>, crate::state::StateError> {
        self.0.watermark(node).await
    }
    async fn definition_hash(
        &self,
        node: &PipeRef,
    ) -> Result<Option<String>, crate::state::StateError> {
        self.0.definition_hash(node).await
    }
    async fn cursor(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        child: &PipeRef,
    ) -> Result<Option<crate::ledger::EdgeCursor>, crate::state::StateError> {
        self.0.cursor(state_instance, parent, child).await
    }
    async fn ledger_after(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        after_seq: i64,
    ) -> Result<Vec<crate::ledger::LedgerEntry>, crate::state::StateError> {
        self.0.ledger_after(state_instance, parent, after_seq).await
    }
    async fn latest_seq(
        &self,
        state_instance: &str,
        parent: &PipeRef,
    ) -> Result<Option<i64>, crate::state::StateError> {
        self.0.latest_seq(state_instance, parent).await
    }
}

impl StateWriter for NoRunHistory {
    async fn put_cursor(
        &self,
        state_instance: &str,
        cursor: crate::ledger::EdgeCursor,
    ) -> Result<(), crate::state::StateError> {
        self.0.put_cursor(state_instance, cursor).await
    }
    async fn put_cursors(
        &self,
        state_instance: &str,
        cursors: Vec<crate::ledger::EdgeCursor>,
    ) -> Result<(), crate::state::StateError> {
        self.0.put_cursors(state_instance, cursors).await
    }
    async fn put_definition_hash(
        &self,
        node: &PipeRef,
        hash: &str,
    ) -> Result<(), crate::state::StateError> {
        self.0.put_definition_hash(node, hash).await
    }
    async fn append_ledger(
        &self,
        state_instance: &str,
        pipe_ref: &PipeRef,
        spans: Vec<(Option<Bound>, Option<Bound>)>,
    ) -> Result<Vec<crate::ledger::LedgerEntry>, crate::state::StateError> {
        self.0.append_ledger(state_instance, pipe_ref, spans).await
    }
    async fn prune_ledger(
        &self,
        state_instance: &str,
        parent: &PipeRef,
        through_seq: i64,
    ) -> Result<usize, crate::state::StateError> {
        self.0
            .prune_ledger(state_instance, parent, through_seq)
            .await
    }
    async fn record_run(&self, _run: &RunRecord) -> Result<(), crate::state::StateError> {
        Ok(())
    }
    fn record_sync_status(&self, node: &PipeRef, outcome: crate::status::SyncOutcome) {
        // Sync statuses are not run history: they are kept either way.
        self.0.record_sync_status(node, outcome);
    }
}

/// `params` helper for callers building [`NodeSyncOptions::params`].
pub fn params_from_json(json: &str) -> Option<Map> {
    Value::from_json_str(json).ok().map(|v| v.to_map())
}
