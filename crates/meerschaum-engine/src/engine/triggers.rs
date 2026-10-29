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
        // Only committed flushes dirty the stream (a failed `callback` flush changed nothing).
        if let (EventKind::StreamFlushed { success: true, .. }, Some(r)) =
            (&event.kind, &event.pipe_ref)
        {
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

    /// Note nodes a lap just synced, so a trigger run right after it still honors their
    /// `min_seconds` (a lap of a `--children` selection also runs the streams' children).
    pub(super) fn note_ran<'a>(&mut self, nodes: impl IntoIterator<Item = &'a PipeRef>) {
        let now = Instant::now();
        for node in nodes {
            self.last_run.insert(node.clone(), now);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::FlushTrigger;

    fn flushed(success: bool) -> EventKind {
        EventKind::StreamFlushed {
            rows: 1,
            bytes: 0,
            trigger: FlushTrigger::Callback,
            success,
            begin: None,
            end: None,
            inserted: u64::from(success),
            updated: 0,
            upserted: 0,
            message: String::new(),
        }
    }

    #[test]
    fn only_committed_flushes_dirty_a_stream() {
        let hub = TriggerHub::new();
        let (broadcast, _rx) = broadcast::channel(8);
        let tap = Emitter::new(
            "run",
            Arc::new(FlushTap {
                hub: hub.clone(),
                broadcast,
            }),
        );
        let r = PipeRef::from_string("sql:i|legacy:x|m|None");
        tap.emit(Some(&r), flushed(false));
        assert!(hub.take().is_empty());
        tap.emit(Some(&r), flushed(true));
        tap.emit(Some(&r), flushed(true));
        assert_eq!(hub.take().get(&r), Some(&2));
    }

    // Exercise the actual private dispatch gate without database or wall-clock work. Once
    // dispatched, run_inner fails on the deliberately unregistered instance; its outcome
    // cannot change the common throttle prefix, stamp, or emitted trigger accounting.
    fn dispatch_fixture() -> (
        Engine,
        Triggers,
        PipeRef,
        PipeRef,
        Arc<crate::events::VecSink>,
    ) {
        use meerschaum_core::{Map, Value};
        let instance = "sql:trigger_unit";
        let mut parent = crate::PipeRecord::new(instance, "mqtt:test", "landing", None);
        parent.streaming = true;
        let stream = parent.pipe_ref();
        let child = crate::PipeRecord::new(instance, instance, "totals", None).with_parameters(
            Map::from_iter([
                ("min_seconds".into(), Value::Float(0.6)),
                (
                    "parents".into(),
                    Value::List(vec![Value::Str(
                        "Pipe('mqtt:test', 'landing', instance='sql:trigger_unit')".into(),
                    )]),
                ),
            ]),
        );
        let child_ref = child.pipe_ref();
        let graph = Arc::new(Graph::build(
            [parent, child],
            &crate::graph::GraphOptions::default(),
        ));
        let sub =
            crate::Pipeline::new(Selection::refs([stream.clone()]).with_children(Some(Depth::All)))
                .resolve(&graph)
                .unwrap();
        assert_eq!(
            sub.descendants(&stream),
            BTreeSet::from([child_ref.clone()])
        );
        let engine = Engine::new(
            Arc::new(crate::StaticRegistry::new()),
            crate::EngineConfig {
                home_instance: instance.into(),
                persist_runs: false,
                ..Default::default()
            },
        );
        let sink = Arc::new(crate::events::VecSink::default());
        let triggers = Triggers::new(
            TriggerHub::new(),
            graph,
            sub,
            RunOptions::default(),
            0.0,
            Emitter::new("dispatch-unit", sink.clone()),
        );
        (engine, triggers, stream, child_ref, sink)
    }

    fn dispatched_flushes(sink: &crate::events::VecSink) -> Vec<u64> {
        sink.events()
            .into_iter()
            .filter_map(|event| match event.kind {
                EventKind::ChildrenTriggered {
                    nodes: 1, flushes, ..
                } => Some(flushes),
                _ => None,
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn trigger_dispatch_min_seconds_uses_monotonic_gate() {
        let (engine, mut triggers, stream, child, sink) = dispatch_fixture();
        let start = Instant::now();
        let interval = Duration::from_millis(600);
        triggers.hub.record(&stream);
        engine.run_triggers(&mut triggers).await;
        assert_eq!(triggers.last_run.get(&child), Some(&start));
        assert_eq!(dispatched_flushes(&sink), vec![1]);

        // Variable downstream work/completion time does not define eligibility. At 200 ms
        // three more flushes arrive; they must be retained behind the actual 600 ms gate.
        tokio::time::advance(Duration::from_millis(200)).await;
        for _ in 0..3 {
            triggers.hub.record(&stream);
        }
        engine.run_triggers(&mut triggers).await;
        assert_eq!(triggers.last_run.get(&child), Some(&start));
        assert_eq!(triggers.dirty.get(&stream), Some(&3));
        assert_eq!(triggers.next_due, Some(start + interval));
        assert_eq!(dispatched_flushes(&sink), vec![1]);

        tokio::time::advance(Duration::from_millis(399)).await;
        engine.run_triggers(&mut triggers).await;
        assert_eq!(Instant::now() - start, Duration::from_millis(599));
        assert_eq!(triggers.last_run.get(&child), Some(&start));
        assert_eq!(triggers.dirty.get(&stream), Some(&3));
        assert_eq!(dispatched_flushes(&sink), vec![1]);

        tokio::time::advance(Duration::from_millis(1)).await;
        engine.run_triggers(&mut triggers).await;
        let second = *triggers.last_run.get(&child).unwrap();
        assert_eq!(second - start, interval);
        assert_eq!(dispatched_flushes(&sink), vec![1, 3]);
        assert!(!triggers.dirty.contains_key(&stream));

        // A fast subsequent run cannot reuse the old window: another full interval is due.
        triggers.hub.record(&stream);
        engine.run_triggers(&mut triggers).await;
        assert_eq!(triggers.next_due, Some(second + interval));
        tokio::time::advance(Duration::from_millis(599)).await;
        engine.run_triggers(&mut triggers).await;
        assert_eq!(triggers.last_run.get(&child), Some(&second));
        assert_eq!(dispatched_flushes(&sink), vec![1, 3]);
        tokio::time::advance(Duration::from_millis(1)).await;
        engine.run_triggers(&mut triggers).await;
        let third = *triggers.last_run.get(&child).unwrap();
        assert_eq!(third - second, interval);
        assert_eq!(dispatched_flushes(&sink), vec![1, 3, 1]);
        assert!(!triggers.dirty.contains_key(&stream));
    }

    #[tokio::test(start_paused = true)]
    async fn initial_lap_stamp_holds_first_trigger_for_its_full_interval() {
        let (engine, mut triggers, stream, child, sink) = dispatch_fixture();
        let lap_finished = Instant::now();
        triggers.note_ran([&child]);
        triggers.hub.record(&stream);
        triggers.hub.record(&stream);
        engine.run_triggers(&mut triggers).await;
        assert_eq!(triggers.last_run.get(&child), Some(&lap_finished));
        assert_eq!(
            triggers.next_due,
            Some(lap_finished + Duration::from_millis(600))
        );
        assert_eq!(triggers.dirty.get(&stream), Some(&2));
        assert!(dispatched_flushes(&sink).is_empty());
        tokio::time::advance(Duration::from_millis(599)).await;
        engine.run_triggers(&mut triggers).await;
        assert!(dispatched_flushes(&sink).is_empty());
        tokio::time::advance(Duration::from_millis(1)).await;
        engine.run_triggers(&mut triggers).await;
        assert_eq!(
            triggers.last_run.get(&child),
            Some(&(lap_finished + Duration::from_millis(600)))
        );
        assert_eq!(dispatched_flushes(&sink), vec![2]);
        assert!(!triggers.dirty.contains_key(&stream));
    }
}
