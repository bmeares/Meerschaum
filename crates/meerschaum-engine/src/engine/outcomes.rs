//! What runs report: each node's outcome, a run's result and pending rebuilds, continuous-mode
//! laps and their events, and a plan made without execution.

use std::sync::Arc;
use std::time::Duration;

use meerschaum_core::success::SyncResult;

use crate::graph::Graph;
use crate::node::PipeRef;
use crate::plan::Plan;
use crate::runs::{NodeStatus, RunRecord};
use crate::selection::Subgraph;

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
    pub(super) fn failed(message: impl Into<String>, duration: Duration) -> RunResult {
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

/// How many lap summaries an unbounded `--loop` keeps for its [`LoopResult`]: the most recent
/// ones. A loop with [`LoopOptions::max_laps`](super::LoopOptions::max_laps), or a single run, keeps every lap.
pub const RETAINED_LAPS: usize = 64;

/// The result of [`Engine::run_loop`](super::Engine::run_loop).
#[derive(Debug, Clone)]
pub struct LoopResult {
    /// The laps' summaries, oldest first: every lap of a bounded loop, the last
    /// [`RETAINED_LAPS`] of an unbounded one (a job loops for months).
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
