//! The typed event stream (§5.7). The CLI, the console and run history render from it.
//!
//! Every transition emits one [`Event`]: `run_started`, `node_planned`, `node_started`,
//! `chunk_started`, `chunk_finished`, `node_finished{SyncResult}`, `node_failed{error,
//! traceback}`, `node_skipped{reason}`, `node_rebuild_pending`, `watermark_advanced`,
//! `stream_flushed{rows, bytes, trigger}`, `stream_restarted{error}`, `run_finished`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use meerschaum_core::success::{Bound, SyncResult};
use serde::{Deserialize, Serialize};

use crate::node::PipeRef;
use crate::plan::Action;
use crate::stream::FlushTrigger;

/// What happened.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum EventKind {
    /// A run began.
    RunStarted {
        /// Number of selected nodes.
        nodes: usize,
        /// Global worker limit.
        workers: usize,
        /// The selection's targets.
        targets: Vec<PipeRef>,
    },
    /// A node was planned (again, when re-planned after its parents).
    NodePlanned {
        /// The action.
        action: Action,
        /// Why.
        reason: String,
        /// Whether the plan may change once parents finish.
        provisional: bool,
    },
    /// A node's sync began.
    NodeStarted {
        /// The action.
        action: Action,
        /// 1 for the first attempt.
        attempt: u32,
    },
    /// A chunk's sync began (emitted by the [`crate::SyncExecutor`]).
    ChunkStarted {
        /// Chunk index within the node's chunk list.
        chunk: usize,
        /// Lower bound.
        begin: Option<Bound>,
        /// Upper bound.
        end: Option<Bound>,
    },
    /// A chunk's sync ended.
    ChunkFinished {
        /// Chunk index.
        chunk: usize,
        /// Whether it succeeded.
        success: bool,
        /// The message.
        message: String,
    },
    /// A node's sync succeeded.
    NodeFinished {
        /// The structured result.
        result: SyncResult,
    },
    /// A node's attempt failed.
    NodeFailed {
        /// The error message.
        error: String,
        /// A traceback, when the executor has one (Python).
        traceback: Option<String>,
        /// The failed attempt.
        attempt: u32,
        /// Whether the retry policy schedules another attempt.
        will_retry: bool,
    },
    /// A node did not run (`skip`, `observe`, `upstream_failed`, resumed).
    NodeSkipped {
        /// Why.
        reason: String,
    },
    /// A node needs a confirmed rebuild and did not run.
    NodeRebuildPending {
        /// Why.
        reason: String,
    },
    /// A node's watermark moved.
    WatermarkAdvanced {
        /// Before the sync.
        from: Option<Bound>,
        /// After the sync.
        to: Option<Bound>,
    },
    /// A stream source flushed a micro-batch (§5.6).
    StreamFlushed {
        /// Rows flushed.
        rows: u64,
        /// Bytes flushed.
        bytes: u64,
        /// Which trigger fired.
        trigger: FlushTrigger,
    },
    /// A stream source's worker restarted.
    StreamRestarted {
        /// The error that stopped it.
        error: String,
    },
    /// A run ended.
    RunFinished {
        /// Whether every node succeeded or was legitimately skipped.
        success: bool,
        /// A summary.
        message: String,
    },
}

impl EventKind {
    /// The event name (`node_started`, ...).
    pub fn name(&self) -> &'static str {
        match self {
            EventKind::RunStarted { .. } => "run_started",
            EventKind::NodePlanned { .. } => "node_planned",
            EventKind::NodeStarted { .. } => "node_started",
            EventKind::ChunkStarted { .. } => "chunk_started",
            EventKind::ChunkFinished { .. } => "chunk_finished",
            EventKind::NodeFinished { .. } => "node_finished",
            EventKind::NodeFailed { .. } => "node_failed",
            EventKind::NodeSkipped { .. } => "node_skipped",
            EventKind::NodeRebuildPending { .. } => "node_rebuild_pending",
            EventKind::WatermarkAdvanced { .. } => "watermark_advanced",
            EventKind::StreamFlushed { .. } => "stream_flushed",
            EventKind::StreamRestarted { .. } => "stream_restarted",
            EventKind::RunFinished { .. } => "run_finished",
        }
    }
}

/// One event with its envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// The run.
    pub run_id: String,
    /// Per-run sequence number (emission order).
    pub seq: u64,
    /// When it happened.
    pub ts: DateTime<Utc>,
    /// The node, for node-level events.
    pub pipe_ref: Option<PipeRef>,
    /// The payload.
    #[serde(flatten)]
    pub kind: EventKind,
}

/// One row of the `mrsm:engine_events` pipe (datetime `ts`, ids `run_id`, `seq`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRow {
    /// When it happened.
    pub ts: DateTime<Utc>,
    /// The run.
    pub run_id: String,
    /// Per-run sequence number.
    pub seq: u64,
    /// The node, if any.
    pub pipe_ref: Option<PipeRef>,
    /// The event name.
    pub event: String,
    /// The payload as JSON text.
    pub payload: String,
}

impl Event {
    /// The persisted row.
    pub fn to_row(&self) -> EventRow {
        EventRow {
            ts: self.ts,
            run_id: self.run_id.clone(),
            seq: self.seq,
            pipe_ref: self.pipe_ref.clone(),
            event: self.kind.name().to_string(),
            payload: serde_json::to_string(&self.kind).unwrap_or_default(),
        }
    }
}

/// A consumer of events. `emit` must not block (buffer or forward to a channel).
pub trait EventSink: Send + Sync {
    /// Receive one event.
    fn emit(&self, event: Event);
}

/// Discards events.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullSink;

impl EventSink for NullSink {
    fn emit(&self, _event: Event) {}
}

/// Collects events in memory.
#[derive(Debug, Default)]
pub struct VecSink {
    events: Mutex<Vec<Event>>,
}

impl VecSink {
    /// A copy of the collected events.
    pub fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The names of the collected events, optionally for one node.
    pub fn names(&self, pipe_ref: Option<&PipeRef>) -> Vec<&'static str> {
        self.events()
            .iter()
            .filter(|e| pipe_ref.is_none() || e.pipe_ref.as_ref() == pipe_ref)
            .map(|e| e.kind.name())
            .collect()
    }
}

impl EventSink for VecSink {
    fn emit(&self, event: Event) {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(event);
    }
}

/// Forwards events to a tokio channel (the live view and `run.events()`).
#[derive(Debug, Clone)]
pub struct ChannelSink(pub tokio::sync::mpsc::UnboundedSender<Event>);

impl EventSink for ChannelSink {
    fn emit(&self, event: Event) {
        let _ = self.0.send(event);
    }
}

/// Stamps events for one run with sequence numbers and timestamps.
#[derive(Clone)]
pub struct Emitter {
    run_id: String,
    seq: std::sync::Arc<AtomicU64>,
    sink: std::sync::Arc<dyn EventSink>,
}

impl std::fmt::Debug for Emitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Emitter")
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

impl Emitter {
    /// An emitter for a run.
    pub fn new(run_id: &str, sink: std::sync::Arc<dyn EventSink>) -> Emitter {
        Emitter {
            run_id: run_id.to_string(),
            seq: std::sync::Arc::new(AtomicU64::new(0)),
            sink,
        }
    }

    /// The run ID.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Emit an event.
    pub fn emit(&self, pipe_ref: Option<&PipeRef>, kind: EventKind) {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        self.sink.emit(Event {
            run_id: self.run_id.clone(),
            seq,
            ts: Utc::now(),
            pipe_ref: pipe_ref.cloned(),
            kind,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_with_event_tag() {
        let sink = std::sync::Arc::new(VecSink::default());
        let em = Emitter::new("run1", sink.clone());
        let r = PipeRef::from_string("i|c|m|None");
        em.emit(
            Some(&r),
            EventKind::NodeSkipped {
                reason: "nothing to do".into(),
            },
        );
        let e = &sink.events()[0];
        let json = serde_json::to_value(e).unwrap();
        assert_eq!(json["event"], "node_skipped");
        assert_eq!(json["reason"], "nothing to do");
        let row = e.to_row();
        assert_eq!(row.event, "node_skipped");
        assert!(row.payload.contains("\"reason\""));
        let back: Event = serde_json::from_value(json).unwrap();
        assert_eq!(&back, e);
    }
}
