//! [`FakeExecutor`]: a scripted [`SyncExecutor`] for tests, with per-node attempts ([`FakeStep`]).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use meerschaum_core::success::{Bound, SyncResult};

use crate::events::EventKind;
use crate::node::PipeRef;
use crate::state::{MemoryState, StateWriter};

use super::{SyncExecutor, SyncRequest};

/// One scripted attempt of the [`FakeExecutor`].
#[derive(Debug, Clone)]
pub struct FakeStep {
    /// How long the sync takes.
    pub delay: Duration,
    /// The result.
    pub result: SyncResult,
    /// Ledger entries to append on success, per state instance.
    pub ledger: Vec<(String, Option<Bound>, Option<Bound>)>,
    /// The node's watermark after a successful sync (`None` leaves it unchanged).
    pub watermark: Option<Option<Bound>>,
}

impl FakeStep {
    /// A successful step.
    pub fn ok(delay: Duration) -> FakeStep {
        FakeStep {
            delay,
            result: SyncResult::new(true, "Success"),
            ledger: Vec::new(),
            watermark: None,
        }
    }

    /// A failing step.
    pub fn fail(message: &str) -> FakeStep {
        FakeStep {
            delay: Duration::ZERO,
            result: SyncResult::new(false, message),
            ledger: Vec::new(),
            watermark: None,
        }
    }
}

/// An in-memory [`SyncExecutor`] for tests: scripted results per node, optional effects on a
/// [`MemoryState`], and a record of calls and peak concurrency.
#[derive(Debug, Default)]
pub struct FakeExecutor {
    scripts: Mutex<HashMap<PipeRef, VecDeque<FakeStep>>>,
    default_delay: Duration,
    state: Option<Arc<MemoryState>>,
    calls: Mutex<Vec<SyncRequest>>,
    running: AtomicUsize,
    peak: AtomicUsize,
}

impl FakeExecutor {
    /// Every unscripted sync succeeds after `default_delay`. Effects go to `state`, if given.
    pub fn new(default_delay: Duration, state: Option<Arc<MemoryState>>) -> FakeExecutor {
        FakeExecutor {
            default_delay,
            state,
            ..Default::default()
        }
    }

    /// Queue a step for a node (consumed one per attempt).
    pub fn script(&self, node: &PipeRef, step: FakeStep) {
        self.scripts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(node.clone())
            .or_default()
            .push_back(step);
    }

    /// The requests received, in call order.
    pub fn calls(&self) -> Vec<SyncRequest> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The nodes synced, in call order.
    pub fn synced(&self) -> Vec<PipeRef> {
        self.calls().into_iter().map(|c| c.pipe_ref).collect()
    }

    /// The highest number of concurrent syncs observed.
    pub fn peak_concurrency(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

impl SyncExecutor for FakeExecutor {
    async fn sync(&self, request: SyncRequest) -> SyncResult {
        let step = self
            .scripts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&request.pipe_ref)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| FakeStep::ok(self.default_delay));
        let pipe_ref = request.pipe_ref.clone();
        for (i, chunk) in request.chunks.iter().enumerate() {
            request.events.emit(EventKind::ChunkStarted {
                chunk: i,
                begin: chunk.begin,
                end: chunk.end,
                source: crate::events::ChunkSource::Window,
                rows: None,
                total: Some(request.chunks.len()),
            });
        }
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(request.clone());
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(step.delay).await;
        self.running.fetch_sub(1, Ordering::SeqCst);
        for (i, chunk) in request.chunks.iter().enumerate() {
            request.events.emit(EventKind::ChunkFinished {
                chunk: i,
                success: step.result.success,
                message: step.result.message.clone(),
                source: crate::events::ChunkSource::Window,
                rows: None,
                inserted: 0,
                updated: 0,
                upserted: 0,
                begin: chunk.begin,
                end: chunk.end,
                duration_ms: Some(step.delay.as_millis() as u64),
            });
        }
        if step.result.success
            && let Some(state) = &self.state
        {
            for (instance, begin, end) in &step.ledger {
                let _ = state
                    .append_ledger(instance, &pipe_ref, vec![(*begin, *end)])
                    .await;
            }
            if let Some(wm) = step.watermark {
                state.set_watermark(&pipe_ref, wm);
            }
        }
        step.result
    }
}
