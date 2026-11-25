//! Recording in this process: the bounded queue of metrics, the background writer that stores
//! them in batches, flushing, and the rate-limited warning about dropped writes.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use chrono::{TimeDelta, Utc};
use meerschaum_core::Pipe;

use crate::InstanceConnector;
use crate::sync_status::SyncOutcome;

use super::{
    BATCH_SIZE, MAX_PENDING, SyncMetric, WRITE_TIMEOUT, observed_registration, retention_days,
};

/// The least time between two warnings about dropped metric writes (the ones in between are
/// counted into the next warning).
const DROP_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// Rate limit of the warnings about dropped metric writes: a slow instance drops every batch,
/// and one warning per interval says so without flooding the log.
#[derive(Debug, Default)]
struct DropWarnings {
    last: Option<Instant>,
    suppressed: u64,
}

impl DropWarnings {
    /// Count a dropped write at `now`: `Some(n)` when it is to be logged, with the `n` drops
    /// not logged since the last warning.
    fn note(&mut self, now: Instant) -> Option<u64> {
        if self
            .last
            .is_some_and(|last| now.duration_since(last) < DROP_WARNING_INTERVAL)
        {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

/// Warn (at most once per [`DROP_WARNING_INTERVAL`]) that `count` metrics were not written.
fn warn_dropped(count: usize, reason: &str) {
    static WARNINGS: Mutex<DropWarnings> = Mutex::new(DropWarnings {
        last: None,
        suppressed: 0,
    });
    let noted = WARNINGS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .note(Instant::now());
    if let Some(suppressed) = noted {
        let earlier = if suppressed > 0 {
            format!(" ({suppressed} more dropped writes were not logged.)")
        } else {
            String::new()
        };
        tracing::warn!("Dropped {count} sync metric(s): {reason}{earlier}");
    }
}

struct Pending {
    conn: Arc<dyn InstanceConnector>,
    metric: SyncMetric,
}

/// Own a drained group until its write is acknowledged. Runtime cancellation may happen
/// after the database committed; retrying the same event IDs is idempotent on every backend.
struct PendingGroup {
    conn: Arc<dyn InstanceConnector>,
    metrics: Vec<SyncMetric>,
}

impl Drop for PendingGroup {
    fn drop(&mut self) {
        if self.metrics.is_empty() {
            return;
        }
        let mut state = recorder();
        for metric in self.metrics.drain(..).rev() {
            if state.pending.len() == MAX_PENDING {
                // The queue's normal overload policy prefers newer outcomes.
                break;
            }
            state.pending.push_front(Pending {
                conn: self.conn.clone(),
                metric,
            });
        }
    }
}

#[derive(Default)]
struct Recorder {
    pid: u32,
    pending: VecDeque<Pending>,
    running: bool,
    pruned: HashMap<String, Instant>,
}

fn recorder() -> std::sync::MutexGuard<'static, Recorder> {
    static STATE: OnceLock<Mutex<Recorder>> = OnceLock::new();
    let mut state = STATE
        .get_or_init(|| Mutex::new(Recorder::default()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if state.pid != std::process::id() {
        *state = Recorder {
            pid: std::process::id(),
            ..Default::default()
        };
    }
    state
}

fn changed() -> &'static tokio::sync::Notify {
    static NOTIFY: OnceLock<tokio::sync::Notify> = OnceLock::new();
    NOTIFY.get_or_init(tokio::sync::Notify::new)
}

tokio::task_local! { static HOOK_WRITE: (); }

/// Update the latest status of a write made by a sync hook without enqueueing a metric.
pub fn without_hook_metrics(f: impl FnOnce()) {
    HOOK_WRITE.sync_scope((), f)
}

/// Queue metadata without blocking the original sync or retaining unbounded history in memory.
pub fn record(conn: Arc<dyn InstanceConnector>, pipe: &Pipe, outcome: &SyncOutcome) {
    if HOOK_WRITE.try_with(|_| ()).is_ok()
        || !crate::sync_status::is_recorded(pipe)
        || (if outcome.registration_captured {
            outcome.registration_id.is_none()
        } else {
            observed_registration(pipe).is_none()
        })
    {
        return;
    }
    {
        let mut state = recorder();
        if state.pending.len() == MAX_PENDING {
            state.pending.pop_front();
            tracing::debug!("Sync metric queue is full; dropped oldest telemetry outcome.");
        }
        state.pending.push_back(Pending {
            conn,
            metric: SyncMetric::from_outcome(pipe, outcome),
        });
    }
    start_writer();
}

struct WriterGuard {
    armed: bool,
}

impl Drop for WriterGuard {
    fn drop(&mut self) {
        if self.armed {
            recorder().running = false;
            changed().notify_waiters();
        }
    }
}

fn start_writer() {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let mut state = recorder();
    if state.running || state.pending.is_empty() {
        return;
    }
    state.running = true;
    let guard = WriterGuard { armed: true };
    drop(state);
    handle.spawn(async move {
        let mut guard = guard;
        loop {
            let batch: Vec<_> = {
                let mut state = recorder();
                let count = BATCH_SIZE.min(state.pending.len());
                if count == 0 {
                    state.running = false;
                    guard.armed = false;
                    changed().notify_waiters();
                    return;
                }
                state.pending.drain(..count).collect()
            };
            let mut groups: HashMap<usize, PendingGroup> = HashMap::new();
            for pending in batch {
                let identity = Arc::as_ptr(&pending.conn) as *const () as usize;
                groups
                    .entry(identity)
                    .or_insert_with(|| PendingGroup {
                        conn: pending.conn.clone(),
                        metrics: Vec::new(),
                    })
                    .metrics
                    .push(pending.metric);
            }
            for (_, mut group) in groups {
                match tokio::time::timeout(
                    WRITE_TIMEOUT,
                    group.conn.record_sync_metrics(&group.metrics),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => warn_dropped(
                        group.metrics.len(),
                        &format!("cannot record sync metrics: {error}"),
                    ),
                    Err(_) => warn_dropped(
                        group.metrics.len(),
                        &format!(
                            "the write exceeded its time budget of {} seconds.",
                            WRITE_TIMEOUT.as_secs()
                        ),
                    ),
                }
                // Completed errors keep telemetry best-effort; cancellation, in contrast,
                // drops the still-owned group and returns it to the bounded queue.
                group.metrics.clear();
            }
            changed().notify_waiters();
            tokio::task::yield_now().await;
        }
    });
}

/// Whether this process still owns unwritten metadata.
pub fn has_unwritten() -> bool {
    let state = recorder();
    state.running || !state.pending.is_empty()
}

/// Flush with a bounded wait for explicit checks and graceful process shutdown.
pub async fn flush(timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let notified = changed().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        start_writer();
        if !has_unwritten() {
            return;
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            return;
        }
    }
}

/// Expire history through indexed timestamps, with bounded per-document cleanup where needed.
pub async fn prune<C: InstanceConnector + ?Sized>(conn: &C) {
    let due = if conn.connector_type() == "valkey" {
        // Its bounded per-document cleanup must keep pace with incoming write batches.
        true
    } else {
        let mut state = recorder();
        let key = format!("{}:{conn:p}", conn.connector_keys());
        if state
            .pruned
            .get(&key)
            .is_none_or(|last| last.elapsed() > Duration::from_secs(60))
        {
            if state.pruned.len() >= MAX_PENDING {
                state.pruned.clear();
            }
            state.pruned.insert(key, Instant::now());
            true
        } else {
            false
        }
    };
    if !due {
        return;
    }
    let days = retention_days();
    if days == 0 {
        return;
    }
    let _ = tokio::time::timeout(WRITE_TIMEOUT, async {
        if !conn.claim_history_retention("metrics").await? {
            return Ok(());
        }
        conn.expire_engine_metrics((Utc::now() - TimeDelta::days(days)).timestamp_micros())
            .await
    })
    .await;
}
