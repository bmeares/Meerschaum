//! Recording in this process: the per-pipe debounce, the background writers and held-success
//! timers, and settling, flushing and forgetting what the process still holds.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use meerschaum_core::Pipe;

use crate::traits::InstanceConnector;

use super::{DEBOUNCE, RECORD_TIMEOUT, SETTLE_TIMEOUT, SyncOutcome, SyncStatus, is_recorded};

/// The recording state of one pipe in this process.
#[derive(Default)]
pub(super) struct Track {
    /// A privacy/registration change starts a fresh debounce segment too.
    registration_id: Option<String>,
    /// When its status was last written (or a write was decided).
    last_write: Option<Instant>,
    /// Whether the last written outcome was a success.
    last_success: bool,
    /// A success held back by the debounce.
    pub(super) pending: Option<SyncOutcome>,
    /// A timer will queue `pending` when the window ends.
    flush_scheduled: bool,
    /// Outcomes to write, oldest first: a writer merges them in one read and one upsert.
    pub(super) queue: Vec<SyncOutcome>,
    /// A writer task writes `queue` (until it is empty).
    pub(super) writing: bool,
    /// Where the outcomes go: the instance connector and the pipe of the latest outcome.
    /// Held strongly only while an outcome still needs to be written.
    target: Option<(Arc<dyn InstanceConnector>, Pipe)>,
    /// Preserve debounce state while the caller still owns this connector.
    owner: Option<Weak<dyn InstanceConnector>>,
    /// Bumped when the pipe's state is forgotten ([`forget`]): nothing decided before is written.
    pub(super) epoch: u64,
}

impl Track {
    fn take_idle_target(&mut self) -> Option<(Arc<dyn InstanceConnector>, Pipe)> {
        if self.writing || self.pending.is_some() || !self.queue.is_empty() {
            return None;
        }
        self.target.take()
    }
}

/// What [`decide`] says to do with an outcome.
#[derive(Debug, PartialEq)]
pub(super) enum Decision {
    /// Write these outcomes now (a held success first).
    Write(Vec<SyncOutcome>),
    /// Held back; a flush is due at this instant (when one needs scheduling).
    Hold { schedule: Option<Instant> },
}

/// One connector owns its own queue even when another connector uses the same visible alias.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct TrackKey {
    pipe_ref: String,
    owner: usize,
}

impl TrackKey {
    pub(super) fn new<C: InstanceConnector + ?Sized>(conn: &C, pipe_ref: &str) -> Self {
        Self {
            pipe_ref: pipe_ref.into(),
            owner: conn as *const C as *const () as usize,
        }
    }
}

/// This process's recording state, by connector and `pipe_ref`. A forked child (v4-style job daemons fork)
/// starts afresh: its parent's held successes, queued outcomes, writers and locks are not its
/// own.
#[derive(Default)]
pub(super) struct State {
    pid: u32,
    pub(super) tracks: HashMap<TrackKey, Track>,
    locks: HashMap<String, Arc<tokio::sync::Mutex<()>>>,
    next_epoch: u64,
    last_prune: Option<Instant>,
}

impl State {
    fn new_epoch(&mut self) -> u64 {
        self.next_epoch = self.next_epoch.wrapping_add(1);
        self.next_epoch
    }

    fn prune_idle(&mut self) {
        let now = Instant::now();
        if self
            .last_prune
            .is_some_and(|last| now.duration_since(last) < Duration::from_secs(1))
        {
            return;
        }
        self.last_prune = Some(now);
        self.tracks.retain(|_, track| {
            track.writing
                || track.pending.is_some()
                || !track.queue.is_empty()
                || track.target.is_some()
                || track
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.strong_count() > 0)
        });
        self.locks.retain(|_, lock| Arc::strong_count(lock) > 1);
    }
}

/// The recording state of this process (reset when the process ID changed: a fork).
pub(super) fn state() -> std::sync::MutexGuard<'static, State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    let mut guard = STATE
        .get_or_init(|| Mutex::new(State::default()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let pid = std::process::id();
    if guard.pid != pid {
        *guard = State {
            pid,
            ..State::default()
        };
    }
    guard
}

/// Woken whenever a writer finished a batch or stopped, and when a pipe is forgotten (what
/// [`settle_where`] waits on).
fn changed() -> &'static tokio::sync::Notify {
    static CHANGED: OnceLock<tokio::sync::Notify> = OnceLock::new();
    CHANGED.get_or_init(tokio::sync::Notify::new)
}

fn write_lock(key: &str) -> Arc<tokio::sync::Mutex<()>> {
    state().locks.entry(key.to_string()).or_default().clone()
}

/// The debounce rule for one outcome of the pipe `key` at `now` (see the module docs).
pub(super) fn decide(
    track: &mut Track,
    outcome: SyncOutcome,
    now: Instant,
    window: Duration,
) -> Decision {
    let within = track
        .last_write
        .is_some_and(|w| now.saturating_duration_since(w) < window);
    if outcome.success && track.last_success && within {
        track.pending = Some(outcome);
        let schedule = (!track.flush_scheduled).then(|| {
            track.flush_scheduled = true;
            track.last_write.map_or(now, |w| w + window)
        });
        return Decision::Hold { schedule };
    }
    let mut batch: Vec<SyncOutcome> = track.pending.take().into_iter().collect();
    track.last_success = outcome.success;
    track.last_write = Some(now);
    batch.push(outcome);
    Decision::Write(batch)
}

/// Mark `track` as written by a new writer task, unless one runs already or nothing is queued.
fn claim_writer(track: &mut Track) -> bool {
    if track.writing || track.queue.is_empty() {
        return false;
    }
    track.writing = true;
    true
}

/// Forget this process's debounce state of one pipe (tests: the next success writes at once).
#[doc(hidden)]
pub fn reset_debounce(pipe_ref: &str) {
    for (key, track) in &mut state().tracks {
        if key.pipe_ref != pipe_ref {
            continue;
        }
        track.last_write = None;
        track.last_success = false;
    }
}

/// Record `outcome` for `pipe` on its instance `conn` (see the module docs): debounced and
/// written in the background, so this returns at once and never fails. Pipes without a status
/// ([`is_recorded`]) are skipped.
pub fn record(conn: Arc<dyn InstanceConnector>, pipe: &Pipe, outcome: SyncOutcome) {
    record_in(conn, pipe, outcome, DEBOUNCE);
}

/// [`record`] with a debounce `window`.
pub(super) fn record_in(
    conn: Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    mut outcome: SyncOutcome,
    window: Duration,
) {
    if !is_recorded(pipe) {
        return;
    }
    if !outcome.registration_captured {
        outcome = outcome.with_registration(crate::sync_metrics::observed_registration(pipe));
    }
    crate::sync_metrics::record(conn.clone(), pipe, &outcome);
    let key = TrackKey::new(conn.as_ref(), &pipe.pipe_ref());
    let (start, timer, _replaced) = {
        let mut state = state();
        state.prune_idle();
        // An old timer must not act on a new connector that reuses a freed address.
        let epoch = state.new_epoch();
        let track = state.tracks.entry(key.clone()).or_insert_with(|| Track {
            epoch,
            ..Track::default()
        });
        track.owner = Some(Arc::downgrade(&conn));
        if track.registration_id != outcome.registration_id {
            track.registration_id = outcome.registration_id.clone();
            track.last_write = None;
            track.last_success = false;
            track.pending = None;
        }
        // The previous target is dropped after the lock (it may hold the last connector).
        let replaced = track.target.replace((conn, pipe.clone()));
        match decide(track, outcome, Instant::now(), window) {
            Decision::Write(outcomes) => {
                track.queue.extend(outcomes);
                (claim_writer(track), None, replaced)
            }
            Decision::Hold { schedule } => {
                (false, schedule.map(|due| (due, track.epoch)), replaced)
            }
        }
    };
    if start {
        // Without a runtime the outcomes wait for the next record or read on one.
        drop(start_writer(key.clone()));
    }
    if let Some((due, epoch)) = timer {
        start_timer(key, due, epoch);
    }
}

/// Clears a pipe's `writing` mark when its writer ends without doing so itself (its runtime
/// shut down, or it never ran), so the next record or read starts another.
struct WriterGuard {
    key: TrackKey,
    armed: bool,
}

impl Drop for WriterGuard {
    fn drop(&mut self) {
        if self.armed {
            let retired = {
                let mut state = state();
                state.tracks.get_mut(&self.key).and_then(|track| {
                    track.writing = false;
                    track.take_idle_target()
                })
            };
            drop(retired);
            changed().notify_waiters();
        }
    }
}

/// Start the writer of the pipe `key` (claimed with [`claim_writer`]) on the current runtime.
/// Without a runtime, the writer is returned for the caller to await (or drop).
fn start_writer(key: TrackKey) -> Option<impl std::future::Future<Output = ()> + Send + 'static> {
    let guard = WriterGuard {
        key: key.clone(),
        armed: true,
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(write_queue(key, guard));
            None
        }
        Err(_) => Some(write_queue(key, guard)),
    }
}

/// Write the queued outcomes of the pipe `key` until none are left.
async fn write_queue(key: TrackKey, mut guard: WriterGuard) {
    loop {
        let batch = {
            let mut state = state();
            let Some(track) = state.tracks.get_mut(&key) else {
                guard.armed = false;
                return;
            };
            if track.queue.is_empty() {
                track.writing = false;
                let retired = track.take_idle_target();
                guard.armed = false;
                drop(state);
                drop(retired);
                changed().notify_waiters();
                return;
            }
            let outcomes = std::mem::take(&mut track.queue);
            track
                .target
                .clone()
                .map(|(conn, pipe)| (conn, pipe, outcomes, track.epoch))
        };
        if let Some((conn, pipe, outcomes, epoch)) = batch {
            write_batch(&key, conn.as_ref(), &pipe, &outcomes, epoch).await;
        }
        changed().notify_waiters();
    }
}

/// Merge `outcomes` into the stored status of `pipe` under its write lock, unless the pipe was
/// forgotten since they were queued (`epoch`): bounded by [`RECORD_TIMEOUT`], errors logged at
/// debug level.
async fn write_batch(
    key: &TrackKey,
    conn: &dyn InstanceConnector,
    pipe: &Pipe,
    outcomes: &[SyncOutcome],
    epoch: u64,
) {
    let lock = write_lock(&key.pipe_ref);
    let write = async {
        let _guard = lock.lock().await;
        if state().tracks.get(key).map(|t| t.epoch) != Some(epoch) {
            return Ok(None);
        }
        conn.record_sync_status(pipe, outcomes).await
    };
    match tokio::time::timeout(RECORD_TIMEOUT, write).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::debug!("Cannot record the sync status of {pipe}: {e}"),
        Err(_) => tracing::debug!(
            "Recording the sync status of {pipe} took longer than {} seconds.",
            RECORD_TIMEOUT.as_secs()
        ),
    }
}

/// Clears a pipe's `flush_scheduled` mark when its timer ends without doing so itself (its
/// runtime shut down), so the next held success schedules another.
pub(super) struct TimerGuard {
    pub(super) key: TrackKey,
    pub(super) epoch: u64,
    pub(super) armed: bool,
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        let retired = {
            let mut state = state();
            match state.tracks.get_mut(&self.key) {
                Some(track) if track.epoch == self.epoch => {
                    if self.armed {
                        track.flush_scheduled = false;
                    }
                    track.take_idle_target()
                }
                _ => None,
            }
        };
        drop(retired);
    }
}

/// Queue the held success of the pipe `key` when its window ends (`due`). Without a runtime the
/// held success goes out with the next record or read.
fn start_timer(key: TrackKey, due: Instant, epoch: u64) {
    let guard = TimerGuard {
        key: key.clone(),
        epoch,
        armed: true,
    };
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(write_held(key, due, guard));
    }
}

/// The end of a debounce window: queue the held success, unless the pipe was forgotten since
/// (`guard.epoch`) or no longer exists on its instance (another process dropped or deleted it
/// within the window).
pub(super) async fn write_held(key: TrackKey, due: Instant, mut guard: TimerGuard) {
    tokio::time::sleep_until(due.into()).await;
    let held = {
        let mut state = state();
        let Some(track) = state.tracks.get_mut(&key) else {
            return;
        };
        guard.armed = false;
        if track.epoch != guard.epoch {
            return;
        }
        track.flush_scheduled = false;
        let held = track.pending.take().zip(track.target.clone());
        if held.is_some() {
            track.last_write = Some(Instant::now());
            track.last_success = true;
        }
        held
    };
    let Some((outcome, (conn, pipe))) = held else {
        return;
    };
    if let Ok(Ok(false)) = tokio::time::timeout(RECORD_TIMEOUT, conn.pipe_exists(&pipe)).await {
        tracing::debug!("Not recording the held sync of {pipe}: the pipe no longer exists.");
        return;
    }
    let start = {
        let mut state = state();
        match state.tracks.get_mut(&key) {
            Some(track) if track.epoch == guard.epoch => {
                track.queue.push(outcome);
                claim_writer(track)
            }
            _ => false,
        }
    };
    if start {
        drop(start_writer(key));
    }
}

/// Write what this process holds for the pipes `which` selects (with `held`, their held
/// successes too) and wait until it is written, up to `timeout`.
pub(super) async fn settle_where(which: impl Fn(&str) -> bool, held: bool, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let notified = changed().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let (busy, start) = {
            let mut state = state();
            let now = Instant::now();
            let mut busy = false;
            let mut start = Vec::new();
            for (key, track) in state.tracks.iter_mut() {
                if !which(&key.pipe_ref) {
                    continue;
                }
                if held && let Some(outcome) = track.pending.take() {
                    track.queue.push(outcome);
                    track.last_write = Some(now);
                    track.last_success = true;
                }
                if track.queue.is_empty() && !track.writing {
                    continue;
                }
                busy = true;
                if claim_writer(track) {
                    start.push(key.clone());
                }
            }
            (busy, start)
        };
        if !busy {
            return;
        }
        for key in start {
            if let Some(write) = start_writer(key) {
                write.await;
            }
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            tracing::debug!("Gave up waiting for sync status writes after {timeout:?}.");
            return;
        }
    }
}

/// Before reading the status of `pipe_ref`: write what this process holds for it, its held
/// success included, so a read right after a sync sees that sync (up to [`SETTLE_TIMEOUT`]).
pub async fn settle_pipe(pipe_ref: &str) {
    settle_where(|key| key == pipe_ref, true, SETTLE_TIMEOUT).await;
}

/// Before reading every status on `instance_keys`: [`settle_pipe`] for each of its pipes.
pub async fn settle_instance(instance_keys: &str) {
    let prefix = format!("{instance_keys}|");
    settle_where(|key| key.starts_with(&prefix), true, SETTLE_TIMEOUT).await;
}

/// Wait (up to `timeout`) until the outcomes this process queued for `pipe_refs` are written;
/// held successes stay held (the end of an engine run: a `--loop` keeps its debounce).
pub async fn wait_written(pipe_refs: &HashSet<String>, timeout: Duration) {
    settle_where(|key| pipe_refs.contains(key), false, timeout).await;
}

/// Whether this process holds outcomes it has not written yet (held, queued or being written).
pub fn has_unwritten() -> bool {
    crate::sync_metrics::has_unwritten()
        || state()
            .tracks
            .values()
            .any(|t| t.pending.is_some() || !t.queue.is_empty() || t.writing)
}

/// Write everything this process holds, held successes included, and wait until it is written
/// (up to `timeout`): what a process runs before it exits, so its latest syncs are kept.
pub async fn flush(timeout: Duration) {
    // Both independent writers get the same shutdown budget. Waiting for them in
    // sequence could consume twice the caller's timeout and starve a restarted metrics task.
    tokio::join!(
        settle_where(|_| true, true, timeout),
        crate::sync_metrics::flush(timeout),
    );
}

/// Forget what this process holds for the status of `pipe_ref` before its stored status is
/// cleared (the pipe was dropped, deleted or fully cleared): its held success and queued
/// outcomes are dropped, nothing decided before is written, and a write in progress is waited
/// for (up to [`RECORD_TIMEOUT`]). Keep the returned guard until the stored status is cleared, so
/// that no write of this process lands after it.
pub async fn forget(pipe_ref: &str) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    forget_tracks(pipe_ref, None).await
}

/// Forget only the status queue owned by `conn`; another physical instance may use the same alias.
pub async fn forget_on<C: InstanceConnector + ?Sized>(
    conn: &C,
    pipe_ref: &str,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    forget_tracks(pipe_ref, Some(TrackKey::new(conn, pipe_ref).owner)).await
}

async fn forget_tracks(
    pipe_ref: &str,
    owner: Option<usize>,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    let retired = {
        let mut state = state();
        let epoch = state.new_epoch();
        let mut retired = Vec::new();
        for (key, track) in &mut state.tracks {
            if key.pipe_ref != pipe_ref || owner.is_some_and(|owner| key.owner != owner) {
                continue;
            }
            track.epoch = epoch;
            track.pending = None;
            track.queue.clear();
            track.flush_scheduled = false;
            track.last_write = None;
            track.last_success = false;
            if let Some(target) = track.take_idle_target() {
                retired.push(target);
            }
        }
        retired
    };
    drop(retired);
    changed().notify_waiters();
    let lock = write_lock(pipe_ref);
    tokio::time::timeout(RECORD_TIMEOUT, lock.lock_owned())
        .await
        .ok()
}

/// Store outcomes now, without the debounce or the background (the API server records what a
/// client posts, which the client debounced): bounded by [`RECORD_TIMEOUT`], errors as `Err`
/// text.
pub async fn record_now(
    conn: &dyn InstanceConnector,
    pipe: &Pipe,
    outcomes: &[SyncOutcome],
) -> Result<Option<SyncStatus>, String> {
    let lock = write_lock(&pipe.pipe_ref());
    // The wait for another write of this pipe is inside the time bound too.
    let write = async {
        let _guard = lock.lock().await;
        conn.record_sync_status(pipe, outcomes).await
    };
    match tokio::time::timeout(RECORD_TIMEOUT, write).await {
        Ok(Ok(status)) => Ok(status),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!(
            "recording the sync status took longer than {} seconds",
            RECORD_TIMEOUT.as_secs()
        )),
    }
}
