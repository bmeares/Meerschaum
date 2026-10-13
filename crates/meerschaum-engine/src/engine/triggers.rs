//! Downstream triggering of stream sources' children (§5.6, §5.10).
//!
//! When a supervised stream flushes a micro-batch, its descendants must catch up without
//! waiting for the next lap. Each flush is recorded in a [`TriggerHub`]; the continuous loop
//! ([`Engine::run_loop`]) drains it between laps ([`Engine::wait_with_triggers`]) and runs an
//! ordinary engine run over `stream + descendants` (the stream is `observe`, so the planner
//! reads the ledger entries the flush wrote and syncs the children incrementally or
//! `backfill`s the chunks that late data dirtied).
//!
//! - **Rate limit.** A node runs at most once per its interval: the pipe's own
//!   `parameters['min_seconds']`, else `system.engine.debounce_seconds`, else the loop's
//!   `--min-seconds`. A node whose interval has not elapsed is **held** (with its
//!   descendants) via [`RunOptions::hold`]; the stream stays dirty and the hold ends at the
//!   earliest due time.
//! - **Coalescing.** Flushes that arrive while a trigger run is executing (or while every
//!   child is held) only bump a counter, so one later run covers all of them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, broadcast};
use tokio::time::Instant;

use super::{Engine, RunOptions};
use crate::events::{Emitter, Event, EventKind, EventSink};
use crate::graph::Graph;
use crate::node::PipeRef;
use crate::selection::{Depth, Selection, Subgraph};
use crate::stream::StopToken;

/// Records which stream nodes flushed since the last trigger run.
#[derive(Debug, Default)]
pub(super) struct TriggerHub {
    pending: Mutex<BTreeMap<PipeRef, u64>>,
    notify: Notify,
}

impl TriggerHub {
    pub(super) fn new() -> Arc<TriggerHub> {
        Arc::new(TriggerHub::default())
    }

    fn record(&self, node: &PipeRef) {
        *self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(node.clone())
            .or_default() += 1;
        self.notify.notify_one();
    }

    fn take(&self) -> BTreeMap<PipeRef, u64> {
        std::mem::take(&mut *self.pending.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

/// The stream tasks' event sink: forwards to the engine's broadcast and records flushes.
pub(super) struct FlushTap {
    pub(super) hub: Arc<TriggerHub>,
    pub(super) broadcast: broadcast::Sender<Event>,
}

impl EventSink for FlushTap {
    fn emit(&self, event: Event) {
        if let (EventKind::StreamFlushed { .. }, Some(r)) = (&event.kind, &event.pipe_ref) {
            self.hub.record(r);
        }
        let _ = self.broadcast.send(event);
    }
}

/// The trigger bookkeeping of one continuous run.
pub(super) struct Triggers {
    pub(super) hub: Arc<TriggerHub>,
    pub(super) graph: Arc<Graph>,
    pub(super) sub: Subgraph,
    /// The loop's options; trigger runs reuse its sync flags and instances.
    pub(super) base: RunOptions,
    /// The loop's `--min-seconds`.
    pub(super) min_seconds: f64,
    pub(super) emitter: Emitter,
    /// Stream node to the number of flushes not yet covered by a run.
    dirty: BTreeMap<PipeRef, u64>,
    last_run: HashMap<PipeRef, Instant>,
    next_due: Option<Instant>,
}

impl Triggers {
    pub(super) fn new(
        hub: Arc<TriggerHub>,
        graph: Arc<Graph>,
        sub: Subgraph,
        base: RunOptions,
        min_seconds: f64,
        emitter: Emitter,
    ) -> Triggers {
        Triggers {
            hub,
            graph,
            sub,
            base,
            min_seconds,
            emitter,
            dirty: BTreeMap::new(),
            last_run: HashMap::new(),
            next_due: None,
        }
    }

    /// The minimum time between two runs of `node` (see the module docs).
    fn interval(&self, node: &PipeRef, debounce: Option<f64>) -> Duration {
        let own = self.graph.node(node).and_then(|n| {
            n.pipe
                .parameters()
                .get("min_seconds")
                .and_then(|v| v.as_f64())
        });
        let seconds = own.or(debounce).unwrap_or(self.min_seconds).max(0.0);
        Duration::from_secs_f64(seconds)
    }
}

fn earlier(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, None) => x,
        (None, y) => y,
    }
}

impl Engine {
    /// Run the triggers that are due: one run per dirty stream over the stream and its
    /// descendants that are not held back.
    pub(super) async fn run_triggers(&self, t: &mut Triggers) {
        for (stream, n) in t.hub.take() {
            *t.dirty.entry(stream).or_default() += n;
        }
        t.next_due = None;
        let streams: Vec<PipeRef> = t.dirty.keys().cloned().collect();
        for stream in streams {
            let flushes = t.dirty.get(&stream).copied().unwrap_or(0);
            let desc = t.sub.descendants(&stream);
            if desc.is_empty() {
                t.dirty.remove(&stream);
                continue;
            }
            let now = Instant::now();
            let mut held = BTreeSet::new();
            let mut due = None;
            for child in &desc {
                let interval = t.interval(child, self.config.debounce_seconds);
                if let Some(last) = t.last_run.get(child) {
                    let at = *last + interval;
                    if at > now {
                        held.insert(child.clone());
                        due = earlier(due, Some(at));
                    }
                }
            }
            let mut closure = held.clone();
            for h in &held {
                closure.extend(t.sub.descendants(h));
            }
            let runnable: Vec<PipeRef> = desc.difference(&closure).cloned().collect();
            if runnable.is_empty() {
                t.next_due = earlier(t.next_due, due);
                continue;
            }
            for child in &runnable {
                t.last_run.insert(child.clone(), now);
            }
            let mut opts = t.base.clone();
            opts.selection =
                Some(Selection::refs([stream.clone()]).with_children(Some(Depth::All)));
            opts.hold = closure.clone();
            opts.begin = None;
            opts.end = None;
            opts.resume = None;
            opts.run_id = None;
            opts.exclude.clear();
            let result = self.run_inner(opts, None).await;
            t.emitter.emit(
                Some(&stream),
                EventKind::ChildrenTriggered {
                    run_id: result.run_id().unwrap_or_default().to_string(),
                    nodes: runnable.len(),
                    held: closure.len(),
                    flushes,
                },
            );
            if held.is_empty() {
                t.dirty.remove(&stream);
            } else {
                t.dirty.insert(stream, 0);
                t.next_due = earlier(t.next_due, due);
            }
        }
    }

    /// Wait `duration` (forever when `None`) or until `stop`, running due triggers meanwhile.
    /// Returns `true` when stopped.
    pub(super) async fn wait_with_triggers(
        &self,
        mut triggers: Option<&mut Triggers>,
        stop: &StopToken,
        duration: Option<Duration>,
    ) -> bool {
        let deadline = duration.map(|d| Instant::now() + d);
        let hub = triggers.as_ref().map(|t| t.hub.clone());
        loop {
            if stop.is_stopped() {
                return true;
            }
            if let Some(t) = triggers.as_deref_mut() {
                self.run_triggers(t).await;
            }
            if stop.is_stopped() {
                return true;
            }
            let now = Instant::now();
            if deadline.is_some_and(|d| now >= d) {
                return false;
            }
            let wake = earlier(deadline, triggers.as_ref().and_then(|t| t.next_due));
            let notified = async {
                match &hub {
                    Some(h) => h.notify.notified().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                _ = stop.stopped() => return true,
                _ = notified => {}
                _ = tokio::time::sleep_until(wake.unwrap_or(now)), if wake.is_some() => {}
            }
        }
    }
}
