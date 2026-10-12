//! Run history and the internal engine-state pipes (§5.8).
//!
//! Durable engine state lives on the instance as internal pipes, following the pattern of
//! `get_users_pipe()`: `Pipe('mrsm', <name>, instance=self, target='mrsm_<name>',
//! temporary=True, static=True, ...)`. [`EnginePipe::parameters`] gives each pipe's
//! `columns`/`dtypes`; the row structs here serialize with exactly those column names.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use meerschaum_core::success::Bound;
use meerschaum_core::{Map, Value};
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
    /// The run ID (a ULID: sortable by start time).
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
            .filter(|n| !n.status.is_complete() && n.status != NodeStatus::RebuildPending)
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

    /// The target table (`mrsm_engine_ledger`; on SQL in the `_mrsm_internal` schema).
    pub fn target(&self) -> String {
        format!("mrsm_{}", self.metric_key())
    }

    /// The pipe's parameters: `columns`, `dtypes`, `upsert`, `target`, `static`.
    pub fn parameters(&self) -> Map {
        let json = match self {
            EnginePipe::State => {
                r#"{"columns": {"primary": "pipe_ref"},
                    "dtypes": {"pipe_ref": "string", "definition_hash": "string", "updated": "datetime"},
                    "upsert": true}"#
            }
            EnginePipe::Ledger => {
                r#"{"columns": {"datetime": "created", "pipe_ref": "pipe_ref", "seq": "seq"},
                    "dtypes": {"created": "datetime", "pipe_ref": "string", "seq": "int",
                               "begin": "json", "end": "json"}}"#
            }
            EnginePipe::Cursors => {
                r#"{"columns": {"parent_ref": "parent_ref", "child_ref": "child_ref"},
                    "dtypes": {"parent_ref": "string", "child_ref": "string", "seq": "int",
                               "parent_watermark_at_cursor": "json"},
                    "upsert": true}"#
            }
            EnginePipe::Runs => {
                r#"{"columns": {"datetime": "started", "primary": "run_id"},
                    "dtypes": {"run_id": "string", "started": "datetime", "finished": "datetime",
                               "success": "bool", "message": "string", "targets": "json",
                               "workers": "int", "num_nodes": "int", "resumed_from": "string",
                               "holder": "string"},
                    "upsert": true}"#
            }
            EnginePipe::Nodes => {
                r#"{"columns": {"datetime": "started", "run_id": "run_id", "pipe_ref": "pipe_ref"},
                    "dtypes": {"run_id": "string", "pipe_ref": "string", "action": "string",
                               "status": "string", "reason": "string", "begin": "json",
                               "end": "json", "num_chunks": "int", "attempts": "int",
                               "started": "datetime", "finished": "datetime",
                               "duration_seconds": "float", "inserted": "int", "updated": "int",
                               "upserted": "int", "deleted": "int", "message": "string",
                               "planned_start_seconds": "float", "slack_seconds": "float"},
                    "upsert": true}"#
            }
            EnginePipe::Events => {
                r#"{"columns": {"datetime": "ts", "run_id": "run_id", "seq": "seq"},
                    "dtypes": {"ts": "datetime", "run_id": "string", "seq": "int",
                               "pipe_ref": "string", "event": "string", "payload": "json"}}"#
            }
        };
        let mut params = Value::from_json_str(json)
            .expect("valid engine pipe parameters")
            .to_map();
        params.insert("target".into(), Value::Str(self.target()));
        params.insert("static".into(), Value::Bool(true));
        params
    }
}

const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// A new run ID: a ULID (48-bit millisecond timestamp + 80 random bits, Crockford base32).
pub fn new_run_id() -> String {
    let millis = Utc::now().timestamp_millis().max(0) as u128 & ((1 << 48) - 1);
    let random = uuid::Uuid::new_v4().as_u128() & ((1 << 80) - 1);
    let value = (millis << 80) | random;
    (0..26)
        .rev()
        .map(|i| CROCKFORD[((value >> (i * 5)) & 0x1f) as usize] as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_pipes() {
        for p in EnginePipe::ALL {
            let params = p.parameters();
            assert_eq!(params["target"], Value::Str(p.target()));
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
    fn run_ids_are_ulids() {
        let a = new_run_id();
        assert_eq!(a.len(), 26);
        assert!(a.chars().all(|c| CROCKFORD.contains(&(c as u8))));
        assert_ne!(a, new_run_id());
    }
}
