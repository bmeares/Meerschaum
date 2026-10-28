//! Run history and the internal engine-state pipes (§5.8).
//!
//! Durable engine state lives on the instance as internal pipes, following the pattern of
//! `get_users_pipe()`: `Pipe('mrsm', <name>, instance=self, target='mrsm_<name>',
//! temporary=True, static=True, ...)`. [`EnginePipe::parameters`] gives each pipe's
//! `columns`/`dtypes`; the row structs here serialize with exactly those column names.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use meerschaum_core::Map;
use meerschaum_core::success::Bound;
use serde::{Deserialize, Serialize};

use crate::node::PipeRef;
use crate::plan::Action;

/// A node's final status in a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// Never started (the run was interrupted).
    Pending,
    /// Synced successfully.
    Succeeded,
    /// Failed after its retries.
    Failed,
    /// Planned `skip` (nothing to do).
    Skipped,
    /// A parent failed.
    UpstreamFailed,
    /// A rebuild is waiting for confirmation.
    RebuildPending,
    /// A stream source (read only).
    Observed,
    /// Not synced because the pipe is locked (`parameters['locks']`: `sync`, or `drop` /
    /// `clear` for a rebuild). Not a failure: pipes downstream use its current data.
    Locked,
}

impl NodeStatus {
    /// Whether a resumed run should not re-plan this node.
    pub fn is_complete(&self) -> bool {
        matches!(
            self,
            NodeStatus::Succeeded | NodeStatus::Skipped | NodeStatus::Observed
        )
    }
}

/// One row of `mrsm:engine_runs` (datetime `started`, primary key `run_id`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRow {
    /// The run ID (name, UTC start time, and collision suffix).
    pub run_id: String,
    /// When the run started.
    pub started: DateTime<Utc>,
    /// When the run ended (`None` while running).
    pub finished: Option<DateTime<Utc>>,
    /// Whether every node succeeded or was legitimately skipped.
    pub success: Option<bool>,
    /// A summary.
    pub message: Option<String>,
    /// The selection's targets.
    pub targets: Vec<PipeRef>,
    /// The global worker limit.
    pub workers: usize,
    /// The number of selected nodes.
    pub num_nodes: usize,
    /// The run this one resumed (`--resume`).
    pub resumed_from: Option<String>,
    /// The lease holder / job name.
    pub holder: String,
    /// The selection as JSON (targets and depths), so `--resume` can re-plan it.
    #[serde(default)]
    pub selection: Option<String>,
}

impl RunRow {
    /// Definition snapshots admitted to this run. Older rows have no snapshots.
    pub fn pipeline_definitions(&self) -> Vec<serde_json::Value> {
        self.selection
            .as_deref()
            .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
            .and_then(|value| {
                value
                    .get("pipeline_definitions")
                    .and_then(|v| v.as_array())
                    .cloned()
            })
            .unwrap_or_default()
    }

    /// Match stored Pipeline identity metadata, with name parsing only for legacy history.
    pub fn belongs_to_pipeline(&self, name: &str) -> bool {
        let definitions = self.pipeline_definitions();
        if definitions.is_empty() {
            return run_has_name(&self.run_id, name);
        }
        definitions
            .iter()
            .any(|definition| definition["name"].as_str() == Some(name))
    }
}

/// One row of `mrsm:engine_nodes` (datetime `started`, ids `run_id`, `pipe_ref`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeRow {
    /// The run.
    pub run_id: String,
    /// The node.
    pub pipe_ref: PipeRef,
    /// The executed (re-planned) action.
    pub action: Action,
    /// The final status.
    pub status: NodeStatus,
    /// The planner's reason.
    pub reason: String,
    /// Sync lower bound (`None` = unbounded or v4 default).
    pub begin: Option<Bound>,
    /// Sync upper bound.
    pub end: Option<Bound>,
    /// Number of chunks.
    pub num_chunks: usize,
    /// Attempts made.
    pub attempts: u32,
    /// When the first attempt started.
    pub started: Option<DateTime<Utc>>,
    /// When the last attempt ended.
    pub finished: Option<DateTime<Utc>>,
    /// Duration of the successful attempt, in seconds.
    pub duration_seconds: Option<f64>,
    /// Rows inserted.
    pub inserted: u64,
    /// Rows updated.
    pub updated: u64,
    /// Rows upserted.
    pub upserted: u64,
    /// Rows deleted.
    pub deleted: u64,
    /// The sync's message or error.
    pub message: Option<String>,
    /// CPM earliest start, seconds from run start.
    pub planned_start_seconds: Option<f64>,
    /// CPM slack, seconds.
    pub slack_seconds: Option<f64>,
    /// The planned duration in seconds (the estimate the CPM schedule used: the EWMA of the
    /// node's earlier runs, else `system.engine.default_duration_seconds`). Planned end =
    /// run start + `planned_start_seconds` + this. Not a column of `mrsm:engine_nodes`
    /// (whose schema is static): [`crate::EngineReader::run_detail`] recomputes it from the
    /// run history for stored runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planned_duration_seconds: Option<f64>,
}

impl NodeRow {
    /// A row for a node that has not run.
    pub fn new(run_id: &str, pipe_ref: &PipeRef, action: Action, reason: &str) -> NodeRow {
        NodeRow {
            run_id: run_id.to_string(),
            pipe_ref: pipe_ref.clone(),
            action,
            status: NodeStatus::Pending,
            reason: reason.to_string(),
            begin: None,
            end: None,
            num_chunks: 0,
            attempts: 0,
            started: None,
            finished: None,
            duration_seconds: None,
            inserted: 0,
            updated: 0,
            upserted: 0,
            deleted: 0,
            message: None,
            planned_start_seconds: None,
            slack_seconds: None,
            planned_duration_seconds: None,
        }
    }
}

/// One row of `mrsm:engine_state` (primary key `pipe_ref`, upsert).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateRow {
    /// The node.
    pub pipe_ref: PipeRef,
    /// The definition hash ([`crate::definition_hash`]).
    pub definition_hash: String,
    /// When it was written.
    pub updated: DateTime<Utc>,
}

/// A run and its nodes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    /// The run row.
    pub run: RunRow,
    /// The node rows, in plan order.
    pub nodes: Vec<NodeRow>,
}

impl RunRecord {
    /// A node's row.
    pub fn node(&self, pipe_ref: &PipeRef) -> Option<&NodeRow> {
        self.nodes.iter().find(|n| n.pipe_ref == *pipe_ref)
    }

    /// For `--resume`: the nodes that need no re-planning.
    pub fn completed_nodes(&self) -> BTreeSet<PipeRef> {
        self.nodes
            .iter()
            .filter(|n| n.status.is_complete())
            .map(|n| n.pipe_ref.clone())
            .collect()
    }

    /// For `--resume`: the nodes that failed or never ran.
    pub fn unfinished_nodes(&self) -> BTreeSet<PipeRef> {
        self.nodes
            .iter()
            .filter(|n| {
                !n.status.is_complete()
                    && !matches!(n.status, NodeStatus::RebuildPending | NodeStatus::Locked)
            })
            .map(|n| n.pipe_ref.clone())
            .collect()
    }
}

/// The internal engine-state pipes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EnginePipe {
    /// `mrsm:engine_state`: definition hashes.
    State,
    /// `mrsm:engine_ledger`: the dirty-range ledger.
    Ledger,
    /// `mrsm:engine_cursors`: edge cursors.
    Cursors,
    /// `mrsm:engine_runs`: runs.
    Runs,
    /// `mrsm:engine_nodes`: per-node results.
    Nodes,
    /// `mrsm:engine_events`: events.
    Events,
}

impl EnginePipe {
    /// Every engine pipe.
    pub const ALL: [EnginePipe; 6] = [
        EnginePipe::State,
        EnginePipe::Ledger,
        EnginePipe::Cursors,
        EnginePipe::Runs,
        EnginePipe::Nodes,
        EnginePipe::Events,
    ];

    /// The metric key (`engine_ledger`).
    pub fn metric_key(&self) -> &'static str {
        match self {
            EnginePipe::State => "engine_state",
            EnginePipe::Ledger => "engine_ledger",
            EnginePipe::Cursors => "engine_cursors",
            EnginePipe::Runs => "engine_runs",
            EnginePipe::Nodes => "engine_nodes",
            EnginePipe::Events => "engine_events",
        }
    }

    /// The default target table (`mrsm_engine_ledger`). On SQL schema flavors the table lives
    /// in `_mrsm_internal` and carries the instance schema as a suffix when that is not the
    /// flavor's default (`InstanceConnector::engine_state_pipe`).
    pub fn target(&self) -> String {
        format!("mrsm_{}", self.metric_key())
    }

    /// The pipe's parameters: `columns`, `dtypes`, `upsert`, `target`, `static`
    /// ([`meerschaum_connector::pipe_ops::engine_state_parameters`], where instance connectors
    /// also find them to forget a dropped pipe's state).
    pub fn parameters(&self) -> Map {
        meerschaum_connector::pipe_ops::engine_state_parameters(self.metric_key())
            .expect("every engine pipe has parameters")
    }
}

/// A readable UTC run ID for an unnamed selection.
pub fn new_run_id() -> String {
    new_named_run_id("pipeline")
}

/// Name + UTC timestamp + a short collision suffix. Microseconds keep runs sortable;
/// random entropy prevents collisions across processes and hosts sharing an instance.
pub fn new_named_run_id(name: &str) -> String {
    let slug: String = name
        .chars()
        .take(128)
        .map(|c| {
            if c.is_ascii_alphanumeric() || "-_.".contains(c) {
                c
            } else {
                '-'
            }
        })
        .collect();
    let slug = if slug.trim_matches('-').is_empty() {
        "pipeline"
    } else {
        &slug
    };
    format!(
        "{}-{}-{}",
        slug,
        Utc::now().format("%Y-%m-%dT%H-%M-%S%.6fZ"),
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    )
}

/// Match a complete pipeline name before the generated timestamp and collision suffix.
pub fn run_has_name(run_id: &str, name: &str) -> bool {
    let Some(rest) = run_id.strip_prefix(&format!("{name}-")) else {
        return false;
    };
    let Some((timestamp, suffix)) = rest.rsplit_once('-') else {
        return false;
    };
    suffix.len() == 8
        && suffix.bytes().all(|b| b.is_ascii_hexdigit())
        && chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%dT%H-%M-%S%.6fZ").is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_pipes() {
        for p in EnginePipe::ALL {
            let params = p.parameters();
            assert_eq!(params["target"], meerschaum_core::Value::Str(p.target()));
            assert!(params["columns"].is_map());
        }
        // The ledger's columns match the row struct's field names.
        let row = serde_json::to_value(crate::ledger::LedgerEntry {
            pipe_ref: PipeRef::from_string("x"),
            seq: 1,
            begin: None,
            end: None,
            created: Utc::now(),
        })
        .unwrap();
        let dtypes = EnginePipe::Ledger.parameters()["dtypes"].clone();
        for key in row.as_object().unwrap().keys() {
            assert!(dtypes.get(key).is_some(), "{key}");
        }
        let row = serde_json::to_value(NodeRow::new(
            "r",
            &PipeRef::from_string("x"),
            Action::Skip,
            "",
        ))
        .unwrap();
        let dtypes = EnginePipe::Nodes.parameters()["dtypes"].clone();
        for key in row.as_object().unwrap().keys() {
            assert!(dtypes.get(key).is_some(), "{key}");
        }
    }

    #[test]
    fn run_ids_are_readable_and_unique() {
        let a = new_run_id();
        assert!(a.starts_with("pipeline-20"));
        assert!(a.contains('T') && a.contains('Z'));
        assert!(new_named_run_id("daily totals").starts_with("daily-totals-"));
        assert_ne!(a, new_run_id());
        let named = new_named_run_id("daily-totals");
        assert!(run_has_name(&named, "daily-totals"));
        assert!(!run_has_name(&named, "daily"));
    }
}
