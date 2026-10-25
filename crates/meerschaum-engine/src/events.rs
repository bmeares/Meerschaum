//! The typed event stream (§5.7). The CLI, the console and run history render from it.
//!
//! Every transition emits one [`Event`]: `run_started`, `node_planned`, `node_started`,
//! `chunk_started`, `chunk_finished`, `node_finished{SyncResult}`, `node_failed{error,
//! traceback}`, `node_skipped{reason}`, `node_rebuild_pending`, `watermark_advanced`,
//! `stream_flushed{rows, bytes, trigger}`, `children_triggered{run_id, nodes, held, flushes}`,
//! `stream_restarted{error}`, `run_finished`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use meerschaum_core::success::{Bound, SyncResult};
use serde::{Deserialize, Serialize};

use crate::node::PipeRef;
use crate::plan::Action;
use crate::stream::FlushTrigger;

/// Which kind of chunk a `chunk_started` / `chunk_finished` event describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkSource {
    /// A planned window (a chunk of the plan, or a dirty range).
    #[default]
    Window,
    /// A piece of the fetched data, in fetch order.
    Fetch,
}

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
    ///
    /// Chunk events come in two [`ChunkSource`]s: `window` chunks are the planner's bounded
    /// windows (one per planned chunk or dirty range) and `fetch` chunks are the pieces a
    /// source's frame is synced in (SQL fetch chunks, `--chunksize`), numbered in **fetch
    /// order** across the whole node sync. Consumers that only knew `chunk`, `begin` and `end`
    /// keep working; the fields after `end` are additive and default when absent.
    ChunkStarted {
        /// Chunk index within the node's chunk list (`window`) or the node's fetch order
        /// (`fetch`), starting at 0.
        chunk: usize,
        /// Lower bound (a `fetch` chunk: the smallest datetime value in the chunk).
        begin: Option<Bound>,
        /// Upper bound (a `fetch` chunk: the largest datetime value in the chunk).
        end: Option<Bound>,
        /// Which kind of chunk.
        #[serde(default)]
        source: ChunkSource,
        /// Rows in the chunk, when known up front (`fetch` chunks).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rows: Option<u64>,
        /// Number of chunks in the list, when known up front (`window` chunks).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        total: Option<usize>,
    },
    /// A chunk's sync ended.
    ChunkFinished {
        /// Chunk index (matches the `chunk_started` event).
        chunk: usize,
        /// Whether it succeeded.
        success: bool,
        /// The message.
        message: String,
        /// Which kind of chunk.
        #[serde(default)]
        source: ChunkSource,
        /// Rows in the chunk (the rows handed to the sync, before diffing).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rows: Option<u64>,
        /// Rows inserted by this chunk.
        #[serde(default)]
        inserted: u64,
        /// Rows updated by this chunk.
        #[serde(default)]
        updated: u64,
        /// Rows upserted by this chunk.
        #[serde(default)]
        upserted: u64,
        /// Lower bound (as in `chunk_started`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        begin: Option<Bound>,
        /// Upper bound (as in `chunk_started`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end: Option<Bound>,
        /// How long the chunk took, in milliseconds.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
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
    /// A stream source flushed a micro-batch (§5.6), or a v4-pattern (`LegacyStream`)
    /// connector's callback called `pipe.sync()` (trigger `callback`).
    ///
    /// The fields after `trigger` are additive (wave 9) and default when absent.
    StreamFlushed {
        /// Rows flushed (the rows handed to the sync, before diffing).
        rows: u64,
        /// Bytes flushed (`0` for `callback` flushes, whose size the engine never sees).
        bytes: u64,
        /// Which trigger fired.
        trigger: FlushTrigger,
        /// Whether the flush committed. `subscribe` streams only report committed flushes
        /// (a failed one keeps its rows for the next trigger); `callback` flushes report
        /// failures too, because the plugin, not the engine, decides whether to retry.
        #[serde(default = "default_true")]
        success: bool,
        /// The smallest datetime-axis value among the rows the flush changed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        begin: Option<Bound>,
        /// The largest datetime-axis value among the rows the flush changed (inclusive).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end: Option<Bound>,
        /// Rows inserted.
        #[serde(default)]
        inserted: u64,
        /// Rows updated.
        #[serde(default)]
        updated: u64,
        /// Rows upserted.
        #[serde(default)]
        upserted: u64,
        /// The sync's message (`callback` flushes; empty otherwise).
        #[serde(default, skip_serializing_if = "String::is_empty")]
        message: String,
    },
    /// A stream source's flush triggered a run over its descendants (§5.6 "Downstream
    /// triggering"). The run itself has its own `run_id` and events.
    ChildrenTriggered {
        /// The triggered run.
        run_id: String,
        /// Descendants the run covers (held-back ones excluded).
        nodes: usize,
        /// Descendants left out because their `min_seconds` had not elapsed; they run on a
        /// later trigger.
        held: usize,
        /// Flushes folded into this trigger (flushes that arrived while a run was pending).
        flushes: u64,
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
            EventKind::ChildrenTriggered { .. } => "children_triggered",
            EventKind::StreamRestarted { .. } => "stream_restarted",
            EventKind::RunFinished { .. } => "run_finished",
        }
    }
}

fn default_true() -> bool {
    true
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
        // Debug output: chunk boundaries and node outcomes as the executor reaches them.
        meerschaum_core::dprint!(
            "{}{}: {kind:?}",
            kind.name(),
            pipe_ref
                .map(|r| format!(" {}", r.label()))
                .unwrap_or_default()
        );
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
