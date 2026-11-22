//! The run monitor: a live Gantt chart, the node table and the event log of one engine run.
//!
//! [`RunState`] is a reducer over the engine's typed events ([`meerschaum_engine::Event`]): it
//! is fed the same way whether the run is in this process ([`super::RunFeed::Live`], the
//! engine's broadcast channel) or anywhere else ([`super::RunFeed::Stored`], the events the
//! engine persists on the instance, read with `after_seq`). A stored run's [`RunDetail`] (its
//! node rows and planned bars) fills in what the events do not carry (planned starts and
//! durations) and covers runs whose events were not persisted (`system.engine.events: off`).

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use crossterm::event::{KeyCode, KeyEvent};
use indexmap::IndexMap;
use meerschaum_engine::viz::fmt_duration;
use meerschaum_engine::{
    Action, Bound, ChunkSource, EngineReader, Event, EventKind, NodeStatus, PipeRef, RunDetail,
    RunStatus,
};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap};

use super::app::{Step, View, centered};
use super::theme::{Theme, Tone, truncate};

mod event_text;

use event_text::{bound_str, first_line};

pub use event_text::{describe, event_line, event_of_row, short_label};

/// The most event lines the log keeps.
const LOG_LINES: usize = 2000;

/// A node's state as the monitor shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Selected, not planned yet.
    Pending,
    /// Planned, waiting for a worker or its parents.
    Planned,
    /// Syncing.
    Running,
    /// An attempt failed and another one is scheduled.
    Retrying,
    /// Synced.
    Succeeded,
    /// Failed after its retries.
    Failed,
    /// Did not run (`skip`, resumed, ...).
    Skipped,
    /// A parent failed.
    UpstreamFailed,
    /// A rebuild is waiting for `--yes`.
    RebuildPending,
    /// A stream source.
    Observed,
    /// Not synced: the pipe is locked (`parameters['locks']`).
    Locked,
}

impl Phase {
    /// The lowercase name.
    pub fn name(self) -> &'static str {
        match self {
            Phase::Pending => "pending",
            Phase::Planned => "planned",
            Phase::Running => "running",
            Phase::Retrying => "retrying",
            Phase::Succeeded => "succeeded",
            Phase::Failed => "failed",
            Phase::Skipped => "skipped",
            Phase::UpstreamFailed => "upstream_failed",
            Phase::RebuildPending => "rebuild_pending",
            Phase::Observed => "observed",
            Phase::Locked => "locked",
        }
    }

    /// How the phase is colored.
    pub fn tone(self) -> Tone {
        match self {
            Phase::Pending | Phase::Planned => Tone::Dim,
            Phase::Running | Phase::Observed => Tone::Active,
            Phase::Retrying | Phase::RebuildPending | Phase::Skipped | Phase::Locked => Tone::Warn,
            Phase::Succeeded => Tone::Good,
            Phase::Failed | Phase::UpstreamFailed => Tone::Bad,
        }
    }

    /// Whether the node is done for this run.
    pub fn is_final(self) -> bool {
        !matches!(
            self,
            Phase::Pending | Phase::Planned | Phase::Running | Phase::Retrying
        )
    }

    fn of_status(status: NodeStatus) -> Phase {
        match status {
            NodeStatus::Pending => Phase::Pending,
            NodeStatus::Succeeded => Phase::Succeeded,
            NodeStatus::Failed => Phase::Failed,
            NodeStatus::Skipped => Phase::Skipped,
            NodeStatus::UpstreamFailed => Phase::UpstreamFailed,
            NodeStatus::RebuildPending => Phase::RebuildPending,
            NodeStatus::Observed => Phase::Observed,
            NodeStatus::Locked => Phase::Locked,
        }
    }
}

/// One node of the run.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeState {
    /// The node.
    pub pipe_ref: PipeRef,
    /// The (re-)planned action.
    pub action: Option<Action>,
    /// The planner's reason (or the skip reason).
    pub reason: String,
    /// Whether the plan may still change.
    pub provisional: bool,
    /// The state.
    pub phase: Phase,
    /// The current (or last) attempt.
    pub attempt: u32,
    /// Failed attempts that were retried.
    pub retries: u32,
    /// Planned windows finished.
    pub chunks_done: usize,
    /// Planned windows, when known.
    pub chunks_total: Option<usize>,
    /// Fetch chunks synced.
    pub fetch_chunks: usize,
    /// The window being synced.
    pub chunk_window: Option<(Option<Bound>, Option<Bound>)>,
    /// Rows handed to the instance.
    pub rows: u64,
    /// Rows inserted.
    pub inserted: u64,
    /// Rows updated.
    pub updated: u64,
    /// Rows upserted.
    pub upserted: u64,
    /// When the first attempt started.
    pub started: Option<DateTime<Utc>>,
    /// When the node finished.
    pub finished: Option<DateTime<Utc>>,
    /// The sync's message or the last error.
    pub message: Option<String>,
    /// CPM earliest start, seconds from the run start (stored runs).
    pub planned_start_seconds: Option<f64>,
    /// The planned duration in seconds (stored runs).
    pub planned_duration_seconds: Option<f64>,
    /// The highest sequence number applied to this node: an older event that arrives late
    /// (a live feed's seed) never moves the node back to an earlier state.
    pub last_seq: Option<u64>,
}

impl NodeState {
    fn new(pipe_ref: PipeRef) -> NodeState {
        NodeState {
            pipe_ref,
            action: None,
            reason: String::new(),
            provisional: false,
            phase: Phase::Pending,
            attempt: 0,
            retries: 0,
            chunks_done: 0,
            chunks_total: None,
            fetch_chunks: 0,
            chunk_window: None,
            rows: 0,
            inserted: 0,
            updated: 0,
            upserted: 0,
            started: None,
            finished: None,
            message: None,
            planned_start_seconds: None,
            planned_duration_seconds: None,
            last_seq: None,
        }
    }

    /// Wall seconds so far (to `now` while running).
    pub fn duration_seconds(&self, now: DateTime<Utc>) -> Option<f64> {
        let start = self.started?;
        let end = self.finished.unwrap_or(now);
        Some((end - start).num_milliseconds().max(0) as f64 / 1000.0)
    }

    /// Changed rows (inserted, updated and upserted) per second.
    pub fn rows_per_second(&self, now: DateTime<Utc>) -> Option<f64> {
        let secs = self.duration_seconds(now)?;
        let changed = self.changed();
        (secs > 0.0 && changed > 0).then(|| changed as f64 / secs)
    }

    /// Inserted, updated and upserted rows.
    pub fn changed(&self) -> u64 {
        self.inserted + self.updated + self.upserted
    }

    /// `3/8` (windows), `12 fetched` or empty.
    pub fn chunk_progress(&self) -> String {
        match (self.chunks_total, self.chunks_done, self.fetch_chunks) {
            (Some(total), done, _) => format!("{done}/{total}"),
            (None, 0, 0) => String::new(),
            (None, done, 0) => done.to_string(),
            (None, _, fetched) => format!("{fetched} fetched"),
        }
    }
}

/// One line of the event log.
#[derive(Debug, Clone, PartialEq)]
pub struct LogLine {
    /// The event's sequence number (the log is kept in this order).
    pub seq: u64,
    /// When.
    pub ts: DateTime<Utc>,
    /// The node, if any.
    pub node: Option<String>,
    /// The text.
    pub text: String,
    /// The color.
    pub tone: Tone,
}

/// Everything the monitor knows about one run.
#[derive(Debug, Clone, PartialEq)]
pub struct RunState {
    /// The run.
    pub run_id: String,
    /// When it started.
    pub started: Option<DateTime<Utc>>,
    /// When it finished.
    pub finished: Option<DateTime<Utc>>,
    /// The run's success (when finished).
    pub success: Option<bool>,
    /// The run's summary.
    pub message: Option<String>,
    /// The worker limit.
    pub workers: usize,
    /// The selected node count announced by `run_started`.
    pub num_nodes: usize,
    /// The selection's targets.
    pub targets: Vec<PipeRef>,
    /// Nodes in the order they appeared (planned order for stored runs).
    pub nodes: IndexMap<PipeRef, NodeState>,
    /// The event log.
    pub log: VecDeque<LogLine>,
    /// Sequence numbers already applied (live and stored events overlap).
    seen: BTreeSet<u64>,
    /// The highest sequence number applied.
    pub last_seq: Option<u64>,
    /// When the newest applied event happened.
    pub last_event: Option<DateTime<Utc>>,
}

impl RunState {
    /// An empty run.
    pub fn new(run_id: &str) -> RunState {
        RunState {
            run_id: run_id.to_string(),
            started: None,
            finished: None,
            success: None,
            message: None,
            workers: 0,
            num_nodes: 0,
            targets: Vec::new(),
            nodes: IndexMap::new(),
            log: VecDeque::new(),
            seen: BTreeSet::new(),
            last_seq: None,
            last_event: None,
        }
    }

    fn node(&mut self, pipe_ref: &PipeRef) -> &mut NodeState {
        self.nodes
            .entry(pipe_ref.clone())
            .or_insert_with(|| NodeState::new(pipe_ref.clone()))
    }

    /// Whether the run has finished.
    pub fn is_finished(&self) -> bool {
        self.finished.is_some()
    }

    /// Apply one event (events of other runs and repeated sequence numbers are ignored).
    /// Events may arrive out of order (a live feed's seed lands after newer broadcast events):
    /// counts still add up, but an event older than what a node has already seen does not
    /// change its state or times, and the log stays in sequence order.
    /// Returns whether it changed anything.
    pub fn apply(&mut self, event: &Event) -> bool {
        if event.run_id != self.run_id || !self.seen.insert(event.seq) {
            return false;
        }
        self.last_seq = Some(self.last_seq.map_or(event.seq, |s| s.max(event.seq)));
        let ts = event.ts;
        self.last_event = Some(self.last_event.map_or(ts, |t| t.max(ts)));
        if self.started.is_none_or(|s| ts < s) {
            self.started = Some(ts);
        }
        if let Some(r) = &event.pipe_ref {
            let node = self.node(r);
            let stale = node.last_seq.is_some_and(|last| event.seq < last);
            if !stale {
                node.last_seq = Some(event.seq);
            }
            if stale {
                // Only what adds up: rows and chunk counts.
                match &event.kind {
                    EventKind::ChunkFinished {
                        source,
                        rows,
                        inserted,
                        updated,
                        upserted,
                        ..
                    } => {
                        match source {
                            ChunkSource::Window => node.chunks_done += 1,
                            ChunkSource::Fetch => node.fetch_chunks += 1,
                        }
                        node.rows += rows.unwrap_or(0);
                        // A finished node's totals came from its result, which counts these.
                        if !node.phase.is_final() {
                            node.inserted += inserted;
                            node.updated += updated;
                            node.upserted += upserted;
                        }
                    }
                    EventKind::NodeStarted { .. } => {
                        node.started = Some(node.started.map_or(ts, |s| s.min(ts)));
                    }
                    EventKind::ChunkStarted { source, total, .. } => {
                        node.started = Some(node.started.map_or(ts, |s| s.min(ts)));
                        if *source == ChunkSource::Window && node.chunks_total.is_none() {
                            node.chunks_total = *total;
                        }
                    }
                    EventKind::NodePlanned { action, reason, .. } => {
                        node.action = node.action.or(Some(*action));
                        if node.reason.is_empty() {
                            node.reason = reason.clone();
                        }
                    }
                    EventKind::NodeFailed { will_retry, .. } if *will_retry => {
                        node.retries += 1;
                    }
                    _ => {}
                }
            } else {
                match &event.kind {
                    EventKind::NodePlanned {
                        action,
                        reason,
                        provisional,
                    } => {
                        node.action = Some(*action);
                        node.reason = reason.clone();
                        node.provisional = *provisional;
                        if node.phase == Phase::Pending {
                            node.phase = Phase::Planned;
                        }
                    }
                    EventKind::NodeStarted { action, attempt } => {
                        node.action = Some(*action);
                        node.attempt = *attempt;
                        node.phase = Phase::Running;
                        node.finished = None;
                        if node.started.is_none() {
                            node.started = Some(ts);
                        }
                    }
                    EventKind::ChunkStarted {
                        source,
                        total,
                        begin,
                        end,
                        ..
                    } => {
                        if node.started.is_none() {
                            node.started = Some(ts);
                        }
                        if node.phase != Phase::Observed {
                            node.phase = Phase::Running;
                        }
                        if *source == ChunkSource::Window {
                            if total.is_some() {
                                node.chunks_total = *total;
                            }
                            node.chunk_window = Some((*begin, *end));
                        }
                    }
                    EventKind::ChunkFinished {
                        source,
                        rows,
                        inserted,
                        updated,
                        upserted,
                        ..
                    } => {
                        match source {
                            ChunkSource::Window => node.chunks_done += 1,
                            ChunkSource::Fetch => node.fetch_chunks += 1,
                        }
                        node.rows += rows.unwrap_or(0);
                        node.inserted += inserted;
                        node.updated += updated;
                        node.upserted += upserted;
                    }
                    EventKind::NodeFinished { result } => {
                        node.phase = if result.success {
                            Phase::Succeeded
                        } else {
                            Phase::Failed
                        };
                        node.finished = Some(ts);
                        // The result's totals are authoritative (chunk events may be missing).
                        node.inserted = node.inserted.max(result.inserted);
                        node.updated = node.updated.max(result.updated);
                        node.upserted = node.upserted.max(result.upserted);
                        node.message = Some(result.message.clone());
                    }
                    EventKind::NodeFailed {
                        error,
                        attempt,
                        will_retry,
                        ..
                    } => {
                        node.attempt = *attempt;
                        node.message = Some(error.clone());
                        if *will_retry {
                            node.retries += 1;
                            node.phase = Phase::Retrying;
                        } else {
                            node.phase = Phase::Failed;
                            node.finished = Some(ts);
                        }
                    }
                    EventKind::NodeSkipped { reason } => {
                        node.phase = if reason.contains("upstream") {
                            Phase::UpstreamFailed
                        } else if meerschaum_core::access::is_lock_refusal(reason)
                            || reason.contains(meerschaum_core::access::LOCK_REFUSAL_MARKER)
                        {
                            Phase::Locked
                        } else {
                            Phase::Skipped
                        };
                        node.reason = reason.clone();
                        node.finished = Some(ts);
                    }
                    EventKind::NodeRebuildPending { reason } => {
                        node.phase = Phase::RebuildPending;
                        node.reason = reason.clone();
                        node.finished = Some(ts);
                    }
                    EventKind::StreamFlushed {
                        rows,
                        inserted,
                        updated,
                        upserted,
                        ..
                    } => {
                        if node.started.is_none() {
                            node.started = Some(ts);
                        }
                        node.phase = Phase::Observed;
                        node.rows += rows;
                        node.inserted += inserted;
                        node.updated += updated;
                        node.upserted += upserted;
                    }
                    _ => {}
                }
            }
        }
        match &event.kind {
            EventKind::RunStarted {
                nodes,
                workers,
                targets,
            } => {
                self.started = Some(ts);
                self.num_nodes = *nodes;
                self.workers = *workers;
                self.targets = targets.clone();
            }
            EventKind::RunFinished { success, message } => {
                self.finished = Some(ts);
                self.success = Some(*success);
                self.message = Some(message.clone());
            }
            _ => {}
        }
        let (text, tone) = describe(&event.kind);
        let at = self.log.partition_point(|l| l.seq < event.seq);
        self.log.insert(
            at,
            LogLine {
                seq: event.seq,
                ts,
                node: event.pipe_ref.as_ref().map(short_label),
                text,
                tone,
            },
        );
        while self.log.len() > LOG_LINES {
            self.log.pop_front();
        }
        true
    }

    /// Merge a stored run's detail: the run row, planned bars and final node rows.
    pub fn apply_detail(&mut self, detail: &RunDetail) {
        let run = &detail.summary.run;
        self.started = Some(run.started);
        self.workers = run.workers;
        self.num_nodes = run.num_nodes;
        if self.targets.is_empty() {
            self.targets = run.targets.clone();
        }
        if detail.summary.status != RunStatus::Running {
            self.finished = run.finished.or(self.finished);
            self.success = run.success.or(self.success);
            if self.message.is_none() {
                self.message = run.message.clone();
            }
        }
        for row in &detail.nodes {
            let node = self.node(&row.pipe_ref);
            node.planned_start_seconds = row.planned_start_seconds;
            node.planned_duration_seconds = row.planned_duration_seconds;
            if node.action.is_none() {
                node.action = Some(row.action);
            }
            if node.reason.is_empty() {
                node.reason = row.reason.clone();
            }
            // Node rows are written when a node ends: they are final.
            if row.finished.is_some() || row.status != NodeStatus::Pending {
                let phase = Phase::of_status(row.status);
                if phase.is_final() {
                    node.phase = phase;
                }
                node.started = node.started.or(row.started);
                node.finished = row.finished.or(node.finished);
                node.attempt = node.attempt.max(row.attempts);
                node.inserted = node.inserted.max(row.inserted);
                node.updated = node.updated.max(row.updated);
                node.upserted = node.upserted.max(row.upserted);
                if node.chunks_total.is_none() && row.num_chunks > 0 {
                    node.chunks_total = Some(row.num_chunks);
                    if phase == Phase::Succeeded {
                        node.chunks_done = row.num_chunks;
                    }
                }
                if row.message.is_some() {
                    node.message = row.message.clone();
                }
            }
        }
    }

    /// `(running, succeeded, failed, other finished, waiting)`.
    pub fn tally(&self) -> (usize, usize, usize, usize, usize) {
        let mut t = (0, 0, 0, 0, 0);
        for n in self.nodes.values() {
            match n.phase {
                Phase::Running | Phase::Retrying | Phase::Observed => t.0 += 1,
                Phase::Succeeded => t.1 += 1,
                Phase::Failed | Phase::UpstreamFailed => t.2 += 1,
                Phase::Skipped | Phase::RebuildPending | Phase::Locked => t.3 += 1,
                Phase::Pending | Phase::Planned => t.4 += 1,
            }
        }
        t
    }

    /// The run's wall seconds (to `now` while running).
    pub fn wall_seconds(&self, now: DateTime<Utc>) -> f64 {
        match self.started {
            Some(start) => {
                (self.finished.unwrap_or(now) - start)
                    .num_milliseconds()
                    .max(0) as f64
                    / 1000.0
            }
            None => 0.0,
        }
    }

    /// The status word.
    pub fn status_name(&self) -> &'static str {
        match (self.finished, self.success) {
            (None, _) => "running",
            (Some(_), Some(false)) => "failed",
            (Some(_), _) => "succeeded",
        }
    }
}

/// One Gantt bar as cells: `(waited, planned-only, active)` spans in `width` cells.
///
/// `start` / `end` are seconds from the run start (`end` is now for a running node); the
/// planned span shades the cells where the node was expected but has not run.
pub fn gantt_cells(
    span: f64,
    width: usize,
    actual: Option<(f64, f64)>,
    planned: Option<(f64, f64)>,
) -> Vec<char> {
    let mut cells = vec![' '; width];
    if width == 0 {
        return cells;
    }
    let span = span.max(0.001);
    let cell = |secs: f64| -> usize {
        ((secs.max(0.0) / span) * width as f64)
            .floor()
            .min(width as f64) as usize
    };
    if let Some((s, e)) = planned {
        let (a, b) = (cell(s).min(width - 1), cell(e).max(cell(s) + 1).min(width));
        for c in &mut cells[a..b] {
            *c = 'p';
        }
    }
    if let Some((s, e)) = actual {
        let a = cell(s).min(width - 1);
        let b = cell(e).max(a + 1).min(width);
        for c in &mut cells[..a] {
            if *c == ' ' {
                *c = 'w';
            }
        }
        for c in &mut cells[a..b] {
            *c = 'a';
        }
    }
    cells
}

/// Messages from the stored-events poller.
pub enum Poll {
    /// New persisted events.
    Events(Vec<Event>),
    /// The run's detail.
    Detail(Box<RunDetail>),
    /// The run does not exist (yet).
    Missing,
    /// A read failed.
    Error(String),
}

/// Polls the instance for a run's persisted events and detail on the session's runtime until
/// dropped.
pub struct StoredPoller {
    rx: Receiver<Poll>,
    stop: Arc<AtomicBool>,
}

impl Drop for StoredPoller {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

impl StoredPoller {
    /// Start polling. With `once`, read once and stop.
    pub fn start(
        session: &crate::native::Session,
        reader: Arc<EngineReader>,
        run_id: &str,
        once: bool,
    ) -> StoredPoller {
        let (tx, rx) = std::sync::mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let run_id = run_id.to_string();
        session.spawn(async move {
            let mut after: Option<u64> = None;
            let mut round: u64 = 0;
            let mut finished = false;
            loop {
                if flag.load(Ordering::SeqCst) {
                    return;
                }
                // The detail every 3 rounds (node rows land as nodes finish).
                if round.is_multiple_of(3) {
                    let msg = match reader.run_detail(&run_id).await {
                        Ok(Some(detail)) => {
                            finished = detail.summary.status != RunStatus::Running;
                            Poll::Detail(Box::new(detail))
                        }
                        Ok(None) => Poll::Missing,
                        Err(error) => Poll::Error(error.to_string()),
                    };
                    if tx.send(msg).is_err() {
                        return;
                    }
                }
                match reader.events_after(&run_id, after).await {
                    Ok(rows) => {
                        if let Some(last) = rows.iter().map(|r| r.seq).max() {
                            after = Some(after.map_or(last, |a| a.max(last)));
                        }
                        let events: Vec<Event> = rows.iter().filter_map(event_of_row).collect();
                        if !events.is_empty() && tx.send(Poll::Events(events)).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        if tx.send(Poll::Error(error.to_string())).is_err() {
                            return;
                        }
                    }
                }
                if once {
                    return;
                }
                round += 1;
                // A finished run changes rarely (retention, a late writer): poll it slowly.
                let pause = if finished { 3000 } else { 700 };
                tokio::time::sleep(Duration::from_millis(pause)).await;
            }
        });
        StoredPoller { rx, stop }
    }
}

/// How long a live feed holds broadcast events back while it waits for the persisted events
/// before them (events the engine failed to persist, or did not persist at all with
/// `system.engine.events: nodes`, never arrive).
pub const SEED_WAIT: Duration = Duration::from_secs(3);

/// Where the monitor's events come from.
pub enum Source {
    /// The engine's broadcast channel, plus the events persisted before the receiver was made.
    ///
    /// The engine persists events in batches (`system.engine.state_flush_ms`), so a receiver
    /// made mid-run misses events that are not on the instance yet either. Until the persisted
    /// events reach the first broadcast one (or the run is over, or [`SEED_WAIT`] passes), the
    /// broadcast events are held back and the seed keeps polling; then both are applied in
    /// sequence order and the seed stops.
    Live {
        /// The channel.
        rx: tokio::sync::broadcast::Receiver<Event>,
        /// Polls the persisted events until caught up (`None` once it has).
        seed: Option<StoredPoller>,
        /// The channel closed.
        closed: bool,
        /// Broadcast events held back while seeding, by sequence number.
        held: std::collections::BTreeMap<u64, Event>,
        /// The highest persisted sequence number the seed has read.
        seed_max: Option<u64>,
        /// When the first event was held.
        holding_since: Option<std::time::Instant>,
    },
    /// The instance.
    Stored(StoredPoller),
    /// Nothing more (tests).
    None,
}

/// Which pane the arrow keys scroll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    /// The nodes (Gantt and table).
    Nodes,
    /// The event log.
    Log,
}

/// The run monitor view.
pub struct RunMonitor {
    /// The reducer.
    pub state: RunState,
    source: Source,
    /// The selected node (index into `state.nodes`).
    pub selected: usize,
    /// The details box is open.
    pub details: bool,
    /// Which pane scrolls.
    pub focus: Focus,
    /// Lines scrolled up from the newest log line (0 follows the log).
    pub log_scroll: usize,
    /// The instance keys (title).
    pub instance: String,
    /// Detaching back to the inline view is offered.
    pub can_detach: bool,
    /// The last read error or warning.
    pub notice: Option<(String, Tone)>,
    /// A fixed clock (tests).
    pub now: Option<DateTime<Utc>>,
    /// Shade the planned (forecast) spans in the timeline (`p`; off by default, D24).
    pub show_plan: bool,
}

impl Source {
    /// A live feed: the broadcast receiver and the seed (`None`: nothing to catch up on).
    pub fn live(rx: tokio::sync::broadcast::Receiver<Event>, seed: Option<StoredPoller>) -> Source {
        Source::Live {
            rx,
            seed,
            closed: false,
            held: std::collections::BTreeMap::new(),
            seed_max: None,
            holding_since: None,
        }
    }
}

impl RunMonitor {
    /// A monitor over `source`.
    pub fn new(run_id: &str, instance: &str, source: Source) -> RunMonitor {
        let can_detach = matches!(source, Source::Live { .. });
        RunMonitor {
            state: RunState::new(run_id),
            source,
            selected: 0,
            details: false,
            focus: Focus::Nodes,
            log_scroll: 0,
            instance: instance.to_string(),
            can_detach,
            notice: None,
            now: None,
            show_plan: false,
        }
    }

    fn now(&self) -> DateTime<Utc> {
        self.now.unwrap_or_else(Utc::now)
    }

    fn apply_poll(&mut self, msg: Poll) -> bool {
        match msg {
            Poll::Events(events) => {
                let mut changed = false;
                for e in &events {
                    changed |= self.state.apply(e);
                }
                changed
            }
            Poll::Detail(detail) => {
                let before = self.state.clone();
                self.state.apply_detail(&detail);
                self.notice = None;
                before != self.state
            }
            Poll::Missing => {
                self.notice = Some((
                    format!("Run '{}' is not on this instance (yet).", self.state.run_id),
                    Tone::Warn,
                ));
                true
            }
            Poll::Error(error) => {
                self.notice = Some((format!("Cannot read the run: {error}"), Tone::Bad));
                true
            }
        }
    }

    /// The selected node.
    pub fn selected_node(&self) -> Option<&NodeState> {
        self.state.nodes.get_index(self.selected).map(|(_, n)| n)
    }

    fn draw_header(&self, frame: &mut Frame<'_>, area: Rect, theme: &Theme) {
        let now = self.now();
        let (running, ok, failed, other, waiting) = self.state.tally();
        let status = self.state.status_name();
        let tone = match status {
            "running" => Tone::Active,
            "failed" => Tone::Bad,
            _ => Tone::Good,
        };
        let total = self.state.num_nodes.max(self.state.nodes.len());
        let mut spans = vec![
            Span::styled(format!(" {status} "), theme.style(tone)),
            Span::raw(format!(
                " wall {}  nodes {total}  workers {}  ",
                fmt_duration(self.state.wall_seconds(now)),
                self.state.workers
            )),
            Span::styled(format!("{running} running "), theme.style(Tone::Active)),
            Span::styled(format!("{ok} ok "), theme.style(Tone::Good)),
            Span::styled(format!("{failed} failed "), theme.style(Tone::Bad)),
        ];
        if other > 0 {
            spans.push(Span::styled(
                format!("{other} skipped "),
                theme.style(Tone::Warn),
            ));
        }
        spans.push(Span::styled(
            format!("{waiting} waiting"),
            theme.style(Tone::Dim),
        ));
        let mut lines = vec![Line::from(spans)];
        if let Some(message) = &self.state.message {
            lines.push(Line::from(Span::styled(
                format!(" {}", first_line(message)),
                theme.style(tone),
            )));
        }
        frame.render_widget(Paragraph::new(lines), area);
    }

    fn draw_gantt(&self, frame: &mut Frame<'_>, area: Rect, theme: &Theme) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_set(theme.border())
            .title(" Timeline ");
        let inner = block.inner(area);
        frame.render_widget(block, area);
        if inner.height == 0 || inner.width < 4 {
            return;
        }
        let now = self.now();
        let start = self.state.started;
        let label_w = (inner.width as usize / 3).clamp(4, 40);
        let dur_w = 8;
        let bar_w = (inner.width as usize).saturating_sub(label_w + dur_w + 2);
        let secs_of = |t: DateTime<Utc>| -> f64 {
            start.map_or(0.0, |s| (t - s).num_milliseconds().max(0) as f64 / 1000.0)
        };
        // D24: the axis is the observed wall time; forecasts never stretch it.
        let span = self.state.wall_seconds(now).max(1.0);
        let (done, waited, planned) = theme.bars();
        let rows = inner.height as usize;
        let offset = scroll_offset(self.selected, rows, self.state.nodes.len());
        let mut lines = Vec::new();
        for (i, node) in self
            .state
            .nodes
            .values()
            .enumerate()
            .skip(offset)
            .take(rows)
        {
            let actual = node
                .started
                .map(|s| (secs_of(s), node.finished.map_or(secs_of(now), secs_of)));
            // Forecasts are opt-in (`p`, D24).
            let plan = node
                .planned_start_seconds
                .zip(node.planned_duration_seconds)
                .filter(|(_, d)| self.show_plan && *d > 0.0)
                .map(|(s, d)| (s, s + d));
            let cells = gantt_cells(span, bar_w, actual, plan);
            let label = truncate(&short_label(&node.pipe_ref), label_w, theme.ellipsis());
            let label_style = if i == self.selected {
                theme.selected()
            } else {
                Style::default()
            };
            let mut spans = vec![Span::styled(format!("{label:<label_w$}"), label_style)];
            spans.push(Span::raw(" "));
            let bar_tone = node.phase.tone();
            let mut run = String::new();
            let mut kind = ' ';
            let flush = |spans: &mut Vec<Span<'static>>, run: &mut String, kind: char| {
                if run.is_empty() {
                    return;
                }
                let style = match kind {
                    'a' => theme.style(bar_tone),
                    'p' | 'w' => theme.style(Tone::Dim),
                    _ => Style::default(),
                };
                spans.push(Span::styled(std::mem::take(run), style));
            };
            for c in cells {
                if c != kind {
                    flush(&mut spans, &mut run, kind);
                    kind = c;
                }
                run.push_str(match c {
                    'a' => done,
                    'w' => waited,
                    'p' => planned,
                    _ => " ",
                });
            }
            flush(&mut spans, &mut run, kind);
            let dur = node
                .duration_seconds(now)
                .map(fmt_duration)
                .unwrap_or_default();
            spans.push(Span::styled(
                format!(" {dur:>dur_w$}", dur_w = dur_w - 1),
                theme.style(Tone::Dim),
            ));
            lines.push(Line::from(spans));
        }
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_table(&self, frame: &mut Frame<'_>, area: Rect, theme: &Theme) {
        let now = self.now();
        let header = Row::new(
            [
                "node", "state", "action", "chunks", "ins", "upd", "rows/s", "try", "time",
            ]
            .map(|h| Cell::from(h).style(theme.style(Tone::Accent))),
        );
        let rows_visible = area.height.saturating_sub(3) as usize;
        let offset = scroll_offset(self.selected, rows_visible, self.state.nodes.len());
        let rows: Vec<Row> = self
            .state
            .nodes
            .values()
            .enumerate()
            .skip(offset)
            .take(rows_visible)
            .map(|(i, n)| {
                let tries = if n.retries > 0 {
                    format!("{} ({}r)", n.attempt, n.retries)
                } else if n.attempt > 0 {
                    n.attempt.to_string()
                } else {
                    String::new()
                };
                let row = Row::new(vec![
                    Cell::from(short_label(&n.pipe_ref)),
                    Cell::from(n.phase.name()).style(theme.style(n.phase.tone())),
                    Cell::from(n.action.map(|a| a.name()).unwrap_or("")),
                    Cell::from(n.chunk_progress()),
                    Cell::from(if n.inserted > 0 {
                        n.inserted.to_string()
                    } else {
                        String::new()
                    }),
                    Cell::from(if n.updated + n.upserted > 0 {
                        (n.updated + n.upserted).to_string()
                    } else {
                        String::new()
                    }),
                    Cell::from(
                        n.rows_per_second(now)
                            .map(|r| format!("{r:.0}"))
                            .unwrap_or_default(),
                    ),
                    Cell::from(tries),
                    Cell::from(
                        n.duration_seconds(now)
                            .map(fmt_duration)
                            .unwrap_or_default(),
                    ),
                ]);
                if i == self.selected {
                    row.style(theme.selected())
                } else {
                    row
                }
            })
            .collect();
        let table = Table::new(
            rows,
            [
                Constraint::Min(12),
                Constraint::Length(15),
                Constraint::Length(11),
                Constraint::Length(10),
                Constraint::Length(8),
                Constraint::Length(8),
                Constraint::Length(7),
                Constraint::Length(6),
                Constraint::Length(7),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_set(theme.border())
                .title(" Nodes "),
        );
        frame.render_widget(table, area);
    }

    fn draw_log(&self, frame: &mut Frame<'_>, area: Rect, theme: &Theme) {
        let title = if self.log_scroll > 0 {
            format!(" Events (scrolled {} up, End follows) ", self.log_scroll)
        } else {
            " Events ".to_string()
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .border_set(theme.border())
            .title(title);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        let rows = inner.height as usize;
        let total = self.state.log.len();
        let end = total.saturating_sub(self.log_scroll.min(total.saturating_sub(rows.min(total))));
        let start = end.saturating_sub(rows);
        let lines: Vec<Line> = self
            .state
            .log
            .range(start..end)
            .map(|l| {
                let mut spans = vec![Span::styled(
                    format!("{} ", l.ts.format("%H:%M:%S")),
                    theme.style(Tone::Dim),
                )];
                if let Some(node) = &l.node {
                    spans.push(Span::raw(format!("{node}  ")));
                }
                spans.extend(super::theme::ansi_spans(
                    &l.text,
                    theme.style(l.tone),
                    theme.color,
                ));
                Line::from(spans)
            })
            .collect();
        frame.render_widget(Paragraph::new(lines), inner);
    }

    fn draw_details(&self, frame: &mut Frame<'_>, area: Rect, theme: &Theme) {
        let Some(n) = self.selected_node() else {
            return;
        };
        let now = self.now();
        let fmt_ts = |t: Option<DateTime<Utc>>| {
            t.map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "-".into())
        };
        let mut rows: Vec<(&str, String)> = vec![
            ("pipe", n.pipe_ref.label()),
            ("instance", n.pipe_ref.instance_keys().to_string()),
            ("state", n.phase.name().to_string()),
            (
                "action",
                n.action
                    .map(|a| format!("{a}{}", if n.provisional { " (provisional)" } else { "" }))
                    .unwrap_or_else(|| "-".into()),
            ),
            ("reason", n.reason.clone()),
            ("attempt", format!("{} ({} retried)", n.attempt, n.retries)),
            ("chunks", n.chunk_progress()),
        ];
        if let Some((begin, end)) = &n.chunk_window {
            rows.push((
                "window",
                format!("{} -> {}", bound_str(begin), bound_str(end)),
            ));
        }
        rows.extend([
            ("rows", n.rows.to_string()),
            (
                "changed",
                format!(
                    "{} inserted, {} updated, {} upserted",
                    n.inserted, n.updated, n.upserted
                ),
            ),
            (
                "rows/s",
                n.rows_per_second(now)
                    .map(|r| format!("{r:.1}"))
                    .unwrap_or_else(|| "-".into()),
            ),
            ("started", fmt_ts(n.started)),
            ("finished", fmt_ts(n.finished)),
            (
                "duration",
                n.duration_seconds(now)
                    .map(fmt_duration)
                    .unwrap_or_else(|| "-".into()),
            ),
        ]);
        if let (Some(s), Some(d)) = (n.planned_start_seconds, n.planned_duration_seconds) {
            rows.push((
                "planned",
                format!("+{} for {}", fmt_duration(s), fmt_duration(d)),
            ));
        }
        let mut lines: Vec<Line> = rows
            .into_iter()
            .map(|(k, v)| {
                Line::from(vec![
                    Span::styled(format!("{k:>9}  "), theme.style(Tone::Accent)),
                    Span::raw(v),
                ])
            })
            .collect();
        if let Some(message) = &n.message {
            lines.push(Line::from(""));
            for l in message.lines().take(12) {
                lines.push(Line::from(Span::styled(
                    l.to_string(),
                    theme.style(if n.phase == Phase::Failed {
                        Tone::Bad
                    } else {
                        Tone::Normal
                    }),
                )));
            }
        }
        let w = area.width.saturating_sub(4).min(100);
        let h = (lines.len() as u16 + 2).min(area.height);
        let rect = centered(area, w, h);
        frame.render_widget(Clear, rect);
        frame.render_widget(
            Paragraph::new(lines).wrap(Wrap { trim: false }).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_set(theme.border())
                    .title(format!(" {} (Esc closes) ", short_label(&n.pipe_ref))),
            ),
            rect,
        );
    }
}

/// The first row to show so that `selected` stays visible in `rows` rows.
pub fn scroll_offset(selected: usize, rows: usize, total: usize) -> usize {
    if rows == 0 || total <= rows {
        return 0;
    }
    selected.saturating_sub(rows - 1).min(total - rows)
}

impl View for RunMonitor {
    fn title(&self) -> String {
        format!("run {} on {}", self.state.run_id, self.instance)
    }

    fn help(&self) -> Vec<(&'static str, &'static str)> {
        let mut keys = vec![
            ("Up/Down, j/k", "select a node (or scroll the log)"),
            ("Enter", "node details (Esc closes)"),
            ("Tab", "switch between nodes and the event log"),
            ("PgUp/PgDn", "scroll the event log"),
            ("End, G", "follow the newest events"),
            ("Home, g", "first node / oldest event"),
            ("p", "show / hide the forecast spans in the timeline"),
        ];
        if self.can_detach {
            keys.push(("d", "back to the inline view"));
        }
        keys
    }

    fn hints(&self) -> String {
        let detach = if self.can_detach { "  d inline" } else { "" };
        format!("Up/Down select  Enter details  Tab log  PgUp/PgDn scroll{detach}")
    }

    fn status(&self) -> Option<(String, Tone)> {
        if self.notice.is_some() {
            return self.notice.clone();
        }
        // A run whose process died is never marked finished: say so once it has gone quiet.
        let last = self.state.last_event.or(self.state.started)?;
        let idle = (self.now() - last).to_std().unwrap_or_default();
        (!self.state.is_finished() && idle >= super::watch::IDLE_LIMIT).then(|| {
            (
                format!(
                    "No new events for {}; the run's process may have died.",
                    fmt_duration(idle.as_secs_f64())
                ),
                Tone::Warn,
            )
        })
    }

    fn tick(&mut self) -> bool {
        let mut changed = false;
        let mut polls = Vec::new();
        match &mut self.source {
            Source::Live {
                rx,
                seed,
                closed,
                held,
                seed_max,
                holding_since,
            } => {
                let mut seed_polls = Vec::new();
                if let Some(poller) = seed {
                    loop {
                        match poller.rx.try_recv() {
                            Ok(msg) => seed_polls.push(msg),
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Disconnected) => {
                                *seed = None;
                                break;
                            }
                        }
                    }
                }
                if !*closed {
                    loop {
                        match rx.try_recv() {
                            Ok(event) if seed.is_some() => {
                                holding_since.get_or_insert_with(std::time::Instant::now);
                                held.insert(event.seq, event);
                            }
                            Ok(event) => changed |= self.state.apply(&event),
                            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                                self.notice =
                                    Some((format!("The view fell {n} events behind."), Tone::Warn));
                                changed = true;
                            }
                            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                                *closed = true;
                                break;
                            }
                        }
                    }
                }
                let first_held = held.keys().next().copied();
                for msg in seed_polls {
                    match msg {
                        Poll::Events(events) => {
                            for e in &events {
                                *seed_max = Some(seed_max.map_or(e.seq, |m| m.max(e.seq)));
                                // What was held is newer and lands below, in order.
                                if first_held.is_none_or(|first| e.seq < first) {
                                    changed |= self.state.apply(e);
                                }
                            }
                        }
                        // Nothing persisted yet is expected mid-run.
                        Poll::Missing => {}
                        other => polls.push(other),
                    }
                }
                if seed.is_some() {
                    let caught_up = match first_held {
                        // Nothing held yet: keep applying the persisted events.
                        None => false,
                        Some(first) => {
                            first == 0
                                || seed_max.is_some_and(|m| m + 1 >= first)
                                || self.state.is_finished()
                                || holding_since.is_some_and(|t| t.elapsed() >= SEED_WAIT)
                        }
                    };
                    if caught_up {
                        *seed = None;
                    }
                }
                if seed.is_none() && !held.is_empty() {
                    for event in std::mem::take(held).into_values() {
                        changed |= self.state.apply(&event);
                    }
                    *holding_since = None;
                }
            }
            Source::Stored(poller) => {
                while let Ok(msg) = poller.rx.try_recv() {
                    polls.push(msg);
                }
            }
            Source::None => {}
        }
        for msg in polls {
            changed |= self.apply_poll(msg);
        }
        if self.selected >= self.state.nodes.len() {
            self.selected = self.state.nodes.len().saturating_sub(1);
        }
        changed
    }

    fn draw(&mut self, frame: &mut Frame<'_>, area: Rect, theme: &Theme) {
        let header_h = if self.state.message.is_some() { 2 } else { 1 };
        let n = self.state.nodes.len() as u16;
        let body_h = area.height.saturating_sub(header_h);
        // Small terminals drop the timeline first, then shrink the log.
        let show_gantt = body_h >= 16;
        let gantt_h = if show_gantt {
            (n + 2).min(body_h / 3).max(3)
        } else {
            0
        };
        let table_h = (n + 3).min((body_h - gantt_h) / 2).max(4.min(body_h));
        let [header, gantt, table, log] = Layout::vertical([
            Constraint::Length(header_h),
            Constraint::Length(gantt_h),
            Constraint::Length(table_h),
            Constraint::Min(0),
        ])
        .areas(area);
        self.draw_header(frame, header, theme);
        if show_gantt {
            self.draw_gantt(frame, gantt, theme);
        }
        self.draw_table(frame, table, theme);
        if log.height >= 3 {
            self.draw_log(frame, log, theme);
        }
        if self.details {
            self.draw_details(frame, area, theme);
        }
    }

    fn on_key(&mut self, key: KeyEvent) -> Step {
        let n = self.state.nodes.len();
        if self.details {
            match key.code {
                KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => self.details = false,
                KeyCode::Up | KeyCode::Char('k') => self.selected = self.selected.saturating_sub(1),
                KeyCode::Down | KeyCode::Char('j') => {
                    self.selected = (self.selected + 1).min(n.saturating_sub(1))
                }
                _ => return Step::Ignored,
            }
            return Step::Continue;
        }
        let log_len = self.state.log.len();
        match (key.code, self.focus) {
            (KeyCode::Tab | KeyCode::BackTab, _) => {
                self.focus = match self.focus {
                    Focus::Nodes => Focus::Log,
                    Focus::Log => Focus::Nodes,
                }
            }
            (KeyCode::Up | KeyCode::Char('k'), Focus::Nodes) => {
                self.selected = self.selected.saturating_sub(1)
            }
            (KeyCode::Down | KeyCode::Char('j'), Focus::Nodes) => {
                self.selected = (self.selected + 1).min(n.saturating_sub(1))
            }
            (KeyCode::Up | KeyCode::Char('k'), Focus::Log) => {
                self.log_scroll = (self.log_scroll + 1).min(log_len)
            }
            (KeyCode::Down | KeyCode::Char('j'), Focus::Log) => {
                self.log_scroll = self.log_scroll.saturating_sub(1)
            }
            (KeyCode::PageUp, _) => self.log_scroll = (self.log_scroll + 10).min(log_len),
            (KeyCode::PageDown, _) => self.log_scroll = self.log_scroll.saturating_sub(10),
            (KeyCode::End | KeyCode::Char('G'), _) => {
                self.log_scroll = 0;
                if self.focus == Focus::Nodes {
                    self.selected = n.saturating_sub(1);
                }
            }
            (KeyCode::Home | KeyCode::Char('g'), Focus::Nodes) => self.selected = 0,
            (KeyCode::Home | KeyCode::Char('g'), Focus::Log) => self.log_scroll = log_len,
            (KeyCode::Char('p'), _) => self.show_plan = !self.show_plan,
            (KeyCode::Enter, _) if n > 0 => self.details = true,
            (KeyCode::Char('d'), _) if self.can_detach => return Step::Detach,
            _ => return Step::Ignored,
        }
        Step::Continue
    }
}

#[cfg(test)]
mod tests;
