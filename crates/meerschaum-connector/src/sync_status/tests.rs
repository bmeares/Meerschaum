use std::collections::HashMap;
use std::time::Instant;

use chrono::TimeZone;
use indexmap::IndexMap;

use crate::traits::{ConnectorResult, DataQuery, InstanceConnector, SyncOptions};

use super::recorder::{
    Decision, TimerGuard, Track, TrackKey, decide, record_in, settle_where, state, write_held,
};
use super::*;

fn at(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap()
}

fn outcome(success: bool, secs: i64) -> SyncOutcome {
    SyncOutcome {
        finished: at(secs),
        ..SyncOutcome::new(success, format!("o{secs}"), SyncSource::Cli, None)
    }
}

fn pipe() -> Pipe {
    Pipe::builder("sql:src", "weather")
        .location(Some("home"))
        .instance("sql:main")
        .build()
        .unwrap()
}

#[test]
fn failures_count_until_a_success() {
    let s = SyncStatus::for_pipe(&pipe())
        .apply(&outcome(true, 0).with_counts(3, 1, 0))
        .apply(&outcome(false, 10))
        .apply(&outcome(false, 20));
    assert!(!s.success);
    assert_eq!(s.consecutive_failures, 2);
    assert_eq!(s.last_success, Some(at(0)));
    assert_eq!(s.last_failure, Some(at(20)));
    assert_eq!(s.last_attempt, Some(at(20)));
    assert_eq!((s.rows_inserted, s.rows_updated), (0, 0));
    let s = s.apply(&outcome(true, 30).with_counts(5, 0, 0));
    assert!(s.success);
    assert_eq!(s.consecutive_failures, 0);
    assert_eq!(s.last_success, Some(at(30)));
    assert_eq!(s.last_failure, Some(at(20)));
    assert_eq!(s.rows_inserted, 5);
    assert_eq!(s.message, "o30");
    assert_eq!(s.source, "cli");
    assert_eq!(s.pipe_ref, "sql:main|sql:src|weather|home");
}

#[test]
fn an_older_outcome_only_moves_its_timestamp() {
    let s = SyncStatus::for_pipe(&pipe())
        .apply(&outcome(false, 20))
        .apply(&outcome(true, 10));
    assert!(!s.success, "the latest attempt (a failure) stays");
    assert_eq!(s.consecutive_failures, 1);
    assert_eq!(s.last_success, Some(at(10)));
    assert_eq!(s.last_attempt, Some(at(20)));
    assert_eq!(s.message, "o20");
}

#[test]
fn rows_round_trip_and_json_has_the_contract_fields() {
    let s = SyncStatus::for_pipe(&pipe()).apply(
        &outcome(true, 0)
            .with_counts(1, 2, 3)
            .with_duration(Some(Duration::from_millis(1500))),
    );
    let back = status_from_doc(&status_doc(&s)).unwrap();
    assert_eq!(back, s);
    let j = s.to_json();
    for key in [
        "pipe_ref",
        "connector_keys",
        "metric_key",
        "location_key",
        "instance_keys",
        "last_attempt",
        "last_success",
        "last_failure",
        "success",
        "message",
        "rows_inserted",
        "rows_updated",
        "duration_seconds",
        "consecutive_failures",
        "source",
        "job",
    ] {
        assert!(j.get(key).is_some(), "{key}");
    }
    assert_eq!(j["last_attempt"], "2027-01-15T08:00:00.000000+00:00");
    assert!(j["last_failure"].is_null());
    assert_eq!(j["duration_seconds"], 1.5);
    let parsed: SyncStatus = serde_json::from_value(j).unwrap();
    assert_eq!(parsed, s);
}

#[test]
fn rekey_rewrites_the_instance() {
    let s = rekey(SyncStatus::for_pipe(&pipe()), "api:main");
    assert_eq!(s.pipe_ref, "api:main|sql:src|weather|home");
    assert_eq!(s.instance_keys, "api:main");
}

#[test]
fn long_messages_keep_both_ends() {
    let long = format!("{}{}", "a".repeat(2000), "Error: the end");
    let short = truncate_message(&long);
    assert!(short.chars().count() <= MAX_MESSAGE_CHARS + 3);
    assert!(short.starts_with("aaa"));
    assert!(short.ends_with("Error: the end"));
    assert!(short.contains('…'));
    assert_eq!(truncate_message("  ok  "), "ok");
    // Character boundaries, not bytes.
    let wide = "é".repeat(3000);
    assert!(truncate_message(&wide).chars().count() <= MAX_MESSAGE_CHARS + 3);
}

#[test]
fn counts_come_from_sync_messages() {
    assert_eq!(
        message_counts("Inserted 1,024, updated 3 rows."),
        (1024, 3, 0)
    );
    assert_eq!(message_counts("Upserted 7 rows."), (0, 0, 7));
    assert_eq!(message_counts("Replaced 2 rows with 3 rows."), (3, 0, 0));
    assert_eq!(message_counts("Success"), (0, 0, 0));
}

#[test]
fn successes_are_debounced_failures_never() {
    let window = Duration::from_secs(5);
    let t0 = Instant::now();
    let mut t = Track::default();
    // The first success writes.
    assert!(
        matches!(decide(&mut t, outcome(true, 0), t0, window), Decision::Write(b) if b.len() == 1)
    );
    // A second success inside the window is held, with one flush scheduled at the window end.
    assert_eq!(
        decide(
            &mut t,
            outcome(true, 1),
            t0 + Duration::from_secs(1),
            window
        ),
        Decision::Hold {
            schedule: Some(t0 + window)
        }
    );
    assert_eq!(
        decide(
            &mut t,
            outcome(true, 2),
            t0 + Duration::from_secs(2),
            window
        ),
        Decision::Hold { schedule: None }
    );
    // A failure writes at once, after the held success.
    match decide(
        &mut t,
        outcome(false, 3),
        t0 + Duration::from_secs(3),
        window,
    ) {
        Decision::Write(b) => {
            assert_eq!(b.len(), 2);
            assert!(b[0].success && b[0].message == "o2");
            assert!(!b[1].success);
        }
        other => panic!("{other:?}"),
    }
    // The first success after a failure writes at once.
    assert!(matches!(
        decide(&mut t, outcome(true, 4), t0 + Duration::from_secs(4), window),
        Decision::Write(b) if b.len() == 1
    ));
    // After the window a success writes again.
    assert!(matches!(
        decide(
            &mut t,
            outcome(true, 20),
            t0 + Duration::from_secs(20),
            window
        ),
        Decision::Write(_)
    ));
}

#[test]
fn the_api_role_wins_then_jobs_then_graph_runs() {
    // (The role is process-global: only this test touches it.)
    let _scope = role_scope(SyncSource::Python);
    let job = current_job();
    let (source, name) = detect_source(true);
    match job {
        Some(j) => assert_eq!((source, name), (SyncSource::Job, Some(j))),
        None => {
            assert_eq!((source, name), (SyncSource::Engine, None));
            assert_eq!(detect_source(false), (SyncSource::Python, None));
        }
    }
    {
        let _api = role_scope(SyncSource::Api);
        assert_eq!(detect_source(true), (SyncSource::Api, None));
    }
    assert_eq!(process_role(), SyncSource::Python);
}

#[test]
fn internal_and_temporary_pipes_have_no_status() {
    assert!(is_recorded(&pipe()));
    let internal = Pipe::builder("mrsm", "pipeline")
        .instance("sql:main")
        .build()
        .unwrap();
    assert!(!is_recorded(&internal));
    let mut temp = pipe();
    temp.temporary = true;
    assert!(!is_recorded(&temp));
}

// -----------------------------------------------------------------------------------------
// Background writes, reads, forgetting and flushing, over an in-memory instance.

use std::sync::atomic::{AtomicBool, AtomicUsize};

use crate::traits::{Connector, DateBound, PipeId, PipesKeys, PipesKeysQuery, async_trait};
use meerschaum_core::SuccessTuple;
use meerschaum_kernel::Frame;

/// Tests that hold successes, or flush every pipe's, run one at a time.
static HELD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// An instance that keeps statuses in memory and counts its finished writes; its writes may
/// be slow, and it may say that its pipes are gone.
#[derive(Default)]
struct Fake {
    statuses: Mutex<HashMap<String, SyncStatus>>,
    writes: Arc<AtomicUsize>,
    write_delay: Duration,
    gone: AtomicBool,
    metrics: Mutex<HashMap<String, crate::sync_metrics::SyncMetric>>,
    metric_writes: AtomicUsize,
    hold_metric_ack: AtomicBool,
    metric_committed: tokio::sync::Notify,
    metric_ack: tokio::sync::Notify,
}

impl Fake {
    fn new() -> Arc<Fake> {
        Arc::new(Fake::default())
    }

    fn slow(delay: Duration) -> Arc<Fake> {
        Arc::new(Fake {
            write_delay: delay,
            ..Fake::default()
        })
    }

    fn writes(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }

    fn stored(&self, pipe: &Pipe) -> Option<SyncStatus> {
        self.statuses.lock().unwrap().get(&pipe.pipe_ref()).cloned()
    }

    fn clear(&self) {
        self.statuses.lock().unwrap().clear();
    }
}

#[async_trait]
impl Connector for Fake {
    fn connector_type(&self) -> &str {
        "fake"
    }

    fn label(&self) -> &str {
        "status"
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }
}

#[async_trait]
impl InstanceConnector for Fake {
    async fn register_pipe(&self, _: &Pipe) -> SuccessTuple {
        SuccessTuple::success()
    }

    async fn get_pipe_attributes(&self, _: &Pipe) -> ConnectorResult<Map> {
        Ok(Map::new())
    }

    async fn get_pipe_id(&self, _: &Pipe) -> ConnectorResult<Option<PipeId>> {
        Ok(None)
    }

    async fn fetch_pipes_keys(&self, _: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        Ok(PipesKeys::List(Vec::new()))
    }

    async fn pipe_exists(&self, _: &Pipe) -> ConnectorResult<bool> {
        Ok(!self.gone.load(Ordering::SeqCst))
    }

    async fn drop_pipe(&self, _: &Pipe) -> SuccessTuple {
        SuccessTuple::success()
    }

    async fn sync_pipe(&self, _: &Pipe, _: Frame, _: &SyncOptions) -> SuccessTuple {
        SuccessTuple::success()
    }

    async fn get_pipe_data(&self, _: &Pipe, _: &DataQuery) -> ConnectorResult<Option<Frame>> {
        Ok(None)
    }

    async fn get_sync_time(
        &self,
        _: &Pipe,
        _: Option<&Map>,
        _: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        Ok(None)
    }

    async fn get_pipe_rowcount(&self, _: &Pipe, _: &DataQuery) -> ConnectorResult<Option<i64>> {
        Ok(None)
    }

    async fn get_pipe_columns_types(&self, _: &Pipe) -> ConnectorResult<IndexMap<String, String>> {
        Ok(IndexMap::new())
    }

    async fn record_sync_status(
        &self,
        pipe: &Pipe,
        outcomes: &[SyncOutcome],
    ) -> ConnectorResult<Option<SyncStatus>> {
        tokio::time::sleep(self.write_delay).await;
        let mut statuses = self.statuses.lock().unwrap();
        let base = statuses
            .remove(&pipe.pipe_ref())
            .unwrap_or_else(|| SyncStatus::for_pipe(pipe));
        let status = outcomes.iter().fold(base, SyncStatus::apply);
        statuses.insert(pipe.pipe_ref(), status.clone());
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(Some(status))
    }

    async fn record_sync_metrics(
        &self,
        metrics: &[crate::sync_metrics::SyncMetric],
    ) -> ConnectorResult<()> {
        self.metric_writes.fetch_add(1, Ordering::SeqCst);
        for metric in metrics {
            self.metrics
                .lock()
                .unwrap()
                .insert(metric.event_id.clone(), metric.clone());
        }
        self.metric_committed.notify_one();
        if self.hold_metric_ack.load(Ordering::SeqCst) {
            self.metric_ack.notified().await;
        }
        Ok(())
    }
}

fn unique_pipe(tag: &str) -> Pipe {
    let metric = format!("{tag}_{}", uuid::Uuid::new_v4().simple());
    Pipe::builder("plugin:fake", &metric)
        .instance("fake:status")
        .build()
        .unwrap()
}

/// Wait for the writes queued for `key`, leaving its held success held.
async fn written(key: &str) {
    settle_where(|k| k == key, false, SETTLE_TIMEOUT).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_returns_at_once_and_a_read_waits_for_its_write() {
    let fake = Fake::slow(Duration::from_millis(300));
    let pipe = unique_pipe("background");
    let started = Instant::now();
    record(fake.clone(), &pipe, outcome(false, 0));
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "the sync waited for its status"
    );
    assert_eq!(fake.writes(), 0);
    settle_pipe(&pipe.pipe_ref()).await;
    assert_eq!(fake.writes(), 1);
    let s = fake.stored(&pipe).unwrap();
    assert_eq!((s.success, s.consecutive_failures), (false, 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_status_writes_release_transient_connectors() {
    let _serial = HELD.lock().await;
    let fake = Fake::slow(Duration::from_millis(20));
    let writes = fake.writes.clone();
    let owner = Arc::downgrade(&fake);
    let pipe = unique_pipe("transient_status_owner");
    record(fake.clone(), &pipe, outcome(false, 0));
    drop(fake);
    assert!(
        owner.upgrade().is_some(),
        "the queued write lost its connector"
    );
    written(&pipe.pipe_ref()).await;
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    for _ in 0..100 {
        if owner.strong_count() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(
        owner.upgrade().is_none(),
        "completed telemetry retained its connector"
    );
}

#[tokio::test]
async fn held_status_keeps_its_connector_only_until_flushed() {
    let _serial = HELD.lock().await;
    let fake = Fake::new();
    let writes = fake.writes.clone();
    let owner = Arc::downgrade(&fake);
    let pipe = unique_pipe("held_transient_owner");
    let window = Duration::from_secs(60);
    record_in(fake.clone(), &pipe, outcome(true, 1), window);
    written(&pipe.pipe_ref()).await;
    record_in(fake.clone(), &pipe, outcome(true, 2), window);
    drop(fake);
    assert!(
        owner.upgrade().is_some(),
        "the held success lost its connector"
    );
    settle_pipe(&pipe.pipe_ref()).await;
    assert_eq!(writes.load(Ordering::SeqCst), 2);
    assert!(
        owner.upgrade().is_none(),
        "the flushed success retained its connector"
    );
}

#[tokio::test]
async fn old_timer_cannot_flush_a_recreated_recording_track() {
    let _serial = HELD.lock().await;
    let fake = Fake::new();
    let pipe = unique_pipe("recreated_track");
    let key = TrackKey::new(fake.as_ref(), &pipe.pipe_ref());
    let window = Duration::from_secs(60);
    record_in(fake.clone(), &pipe, outcome(true, 1), window);
    written(&pipe.pipe_ref()).await;
    record_in(fake.clone(), &pipe, outcome(true, 2), window);
    let old_epoch = state().tracks[&key].epoch;
    settle_pipe(&pipe.pipe_ref()).await;
    // Recreate the same key, as when an allocator reuses a retired connector's address.
    state().tracks.remove(&key);
    record_in(fake.clone(), &pipe, outcome(true, 3), window);
    written(&pipe.pipe_ref()).await;
    record_in(fake.clone(), &pipe, outcome(true, 4), window);
    write_held(
        key.clone(),
        Instant::now(),
        TimerGuard {
            key,
            epoch: old_epoch,
            armed: true,
        },
    )
    .await;
    written(&pipe.pipe_ref()).await;
    assert_eq!(
        fake.writes(),
        3,
        "an old timer flushed the new owner's held success"
    );
    assert_eq!(fake.stored(&pipe).unwrap().message, "o3");
    settle_pipe(&pipe.pipe_ref()).await;
    assert_eq!(fake.stored(&pipe).unwrap().message, "o4");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outcomes_queued_during_a_write_go_out_together_in_order() {
    let fake = Fake::slow(Duration::from_millis(200));
    let pipe = unique_pipe("queued");
    record(fake.clone(), &pipe, outcome(false, 0));
    // The first write runs; two more failures queue up behind it.
    tokio::time::sleep(Duration::from_millis(50)).await;
    record(fake.clone(), &pipe, outcome(false, 1));
    record(fake.clone(), &pipe, outcome(false, 2));
    written(&pipe.pipe_ref()).await;
    assert_eq!(fake.writes(), 2, "the queued failures are one write");
    let s = fake.stored(&pipe).unwrap();
    assert_eq!(s.consecutive_failures, 3);
    assert_eq!(s.message, "o2");
}

#[test]
fn identical_aliases_keep_their_queued_outcomes_on_their_own_connector() {
    let first = Fake::new();
    let second = Fake::new();
    let pipe = unique_pipe("same_alias_queued");
    // No runtime may consume either queue before both targets have been recorded.
    record(first.clone(), &pipe, outcome(false, 1));
    record(second.clone(), &pipe, outcome(false, 2));
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        settle_pipe(&pipe.pipe_ref()).await;
    });
    let first_status = first.stored(&pipe).unwrap();
    let second_status = second.stored(&pipe).unwrap();
    assert_eq!(first_status.message, "o1");
    assert_eq!(second_status.message, "o2");
    assert_eq!(first_status.consecutive_failures, 1);
    assert_eq!(second_status.consecutive_failures, 1);
}

#[tokio::test]
async fn identical_aliases_do_not_share_debounce_or_deletion_state() {
    let _serial = HELD.lock().await;
    let first = Fake::new();
    let second = Fake::new();
    let pipe = unique_pipe("same_alias_held");
    let window = Duration::from_secs(60);
    record_in(first.clone(), &pipe, outcome(true, 1), window);
    written(&pipe.pipe_ref()).await;
    record_in(first.clone(), &pipe, outcome(true, 2), window);
    record_in(second.clone(), &pipe, outcome(true, 3), window);
    written(&pipe.pipe_ref()).await;
    assert_eq!(first.stored(&pipe).unwrap().message, "o1");
    assert_eq!(second.stored(&pipe).unwrap().message, "o3");
    record_in(second.clone(), &pipe, outcome(true, 4), window);
    let guard = forget_on(first.as_ref(), &pipe.pipe_ref()).await;
    first.clear();
    drop(guard);
    settle_pipe(&pipe.pipe_ref()).await;
    assert!(
        first.stored(&pipe).is_none(),
        "the deleted instance stays clear"
    );
    assert_eq!(second.stored(&pipe).unwrap().message, "o4");
}

#[test]
fn cancelled_metrics_acknowledgement_is_retried_without_changing_event_identity() {
    let _serial = HELD.blocking_lock();
    let fake = Fake::new();
    fake.hold_metric_ack.store(true, Ordering::SeqCst);
    let pipe = unique_pipe("cancelled_metrics");
    pipe.set_registration_id(Some("synthetic-registration".into()));
    let first = tokio::runtime::Runtime::new().unwrap();
    first.block_on(async {
        crate::sync_metrics::record(fake.clone(), &pipe, &outcome(true, 1));
        tokio::time::timeout(Duration::from_secs(1), fake.metric_committed.notified())
            .await
            .unwrap();
    });
    let event_id = fake.metrics.lock().unwrap().keys().next().unwrap().clone();
    drop(first);
    assert!(crate::sync_metrics::has_unwritten());
    fake.hold_metric_ack.store(false, Ordering::SeqCst);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        crate::sync_metrics::flush(Duration::from_secs(1)).await;
    });
    assert!(!crate::sync_metrics::has_unwritten());
    assert_eq!(fake.metric_writes.load(Ordering::SeqCst), 2);
    let rows = fake.metrics.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows.contains_key(&event_id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forgetting_a_pipe_waits_for_its_write_and_drops_what_is_queued() {
    let fake = Fake::slow(Duration::from_millis(300));
    let pipe = unique_pipe("forget");
    let key = pipe.pipe_ref();
    record(fake.clone(), &pipe, outcome(false, 0));
    tokio::time::sleep(Duration::from_millis(50)).await;
    record(fake.clone(), &pipe, outcome(false, 1));
    let guard = forget(&key).await;
    assert!(guard.is_some());
    assert_eq!(fake.writes(), 1, "the write in progress finished first");
    // The caller clears the stored status while it holds the guard.
    fake.clear();
    drop(guard);
    settle_pipe(&key).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(fake.writes(), 1, "the queued failure was dropped");
    assert!(fake.stored(&pipe).is_none());

    // The pipe records again afterwards, from scratch.
    record(fake.clone(), &pipe, outcome(false, 2));
    settle_pipe(&key).await;
    assert_eq!(fake.stored(&pipe).unwrap().consecutive_failures, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forgetting_a_pipe_drops_its_held_success() {
    let _serial = HELD.lock().await;
    let fake = Fake::new();
    let pipe = unique_pipe("forget_held");
    let key = pipe.pipe_ref();
    let window = Duration::from_millis(300);
    record_in(fake.clone(), &pipe, outcome(true, 0), window);
    written(&key).await;
    record_in(fake.clone(), &pipe, outcome(true, 1), window);
    assert_eq!(fake.writes(), 1, "the second success is held");
    drop(forget(&key).await);
    fake.clear();
    // The window ends and a read follows: nothing brings the status back.
    tokio::time::sleep(window * 3).await;
    settle_pipe(&key).await;
    assert_eq!(fake.writes(), 1);
    assert!(fake.stored(&pipe).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_success_is_written_when_its_window_ends_unless_its_pipe_is_gone() {
    let _serial = HELD.lock().await;
    let fake = Fake::new();
    let pipe = unique_pipe("held");
    let key = pipe.pipe_ref();
    let window = Duration::from_millis(300);
    record_in(fake.clone(), &pipe, outcome(true, 0), window);
    record_in(fake.clone(), &pipe, outcome(true, 1), window);
    written(&key).await;
    assert_eq!(fake.writes(), 1);
    assert_eq!(fake.stored(&pipe).unwrap().message, "o0");
    tokio::time::sleep(window * 3).await;
    assert_eq!(fake.writes(), 2, "written when the window ended");
    assert_eq!(fake.stored(&pipe).unwrap().message, "o1");

    // Another process deletes the pipe within a window: its held success is dropped.
    record_in(fake.clone(), &pipe, outcome(true, 2), window);
    written(&key).await;
    record_in(fake.clone(), &pipe, outcome(true, 3), window);
    let before = fake.writes();
    fake.gone.store(true, Ordering::SeqCst);
    tokio::time::sleep(window * 3).await;
    assert_eq!(fake.writes(), before);
    assert_eq!(fake.stored(&pipe).unwrap().message, "o2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_and_a_flush_write_held_successes() {
    let _serial = HELD.lock().await;
    let fake = Fake::new();
    let window = Duration::from_secs(60);

    // A read of the pipe writes its held success first.
    let read = unique_pipe("read");
    record_in(fake.clone(), &read, outcome(true, 0), window);
    record_in(fake.clone(), &read, outcome(true, 1), window);
    settle_pipe(&read.pipe_ref()).await;
    assert_eq!(fake.stored(&read).unwrap().message, "o1");

    // So does a read of its instance, and the flush before a process exits.
    let bulk = unique_pipe("bulk");
    record_in(fake.clone(), &bulk, outcome(true, 0), window);
    record_in(fake.clone(), &bulk, outcome(true, 1), window);
    settle_instance("fake:status").await;
    assert_eq!(fake.stored(&bulk).unwrap().message, "o1");
    let exiting = unique_pipe("exiting");
    record_in(fake.clone(), &exiting, outcome(true, 0), window);
    record_in(fake.clone(), &exiting, outcome(true, 1), window);
    flush(Duration::from_secs(2)).await;
    assert_eq!(fake.stored(&exiting).unwrap().message, "o1");
    let state = state();
    let track = &state.tracks[&TrackKey::new(fake.as_ref(), &exiting.pipe_ref())];
    assert!(track.pending.is_none() && track.queue.is_empty() && !track.writing);
}

#[test]
fn a_writer_lost_with_its_runtime_is_replaced() {
    let fake = Fake::slow(Duration::from_millis(1_000));
    let pipe = unique_pipe("lost");
    let key = pipe.pipe_ref();
    let runtime = || {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    };
    let first = runtime();
    first.block_on(async {
        record(fake.clone(), &pipe, outcome(false, 0));
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    // The runtime goes away in the middle of the write: the write is lost with it.
    drop(first);
    assert!(
        !state().tracks[&TrackKey::new(fake.as_ref(), &key)].writing,
        "the lost writer let go"
    );
    runtime().block_on(async {
        record(fake.clone(), &pipe, outcome(false, 1));
        settle_pipe(&key).await;
    });
    assert_eq!(fake.writes(), 1);
    assert_eq!(fake.stored(&pipe).unwrap().message, "o1");
}
