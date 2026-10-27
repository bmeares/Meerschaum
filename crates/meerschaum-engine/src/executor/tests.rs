use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use meerschaum_core::success::Bound;
use meerschaum_core::{Map, Value};

use super::*;
use crate::events::VecSink;
use crate::graph::{Graph, GraphOptions};
use crate::plan::{Action, PlanOptions, plan};
use crate::runs::NodeStatus;
use crate::schedule::EwmaDurations;
use crate::selection::{Depth, Pipeline, Selection};
use crate::state::{MemoryState, MemoryStateData, StateSource};

fn params(json: &str) -> Map {
    Value::from_json_str(json).unwrap().to_map()
}

fn rec(metric: &str, json: &str) -> PipeRecord {
    PipeRecord::new("sql:main", "sql:main", metric, None).with_parameters(params(json))
}

fn r(metric: &str) -> PipeRef {
    PipeRef::new("sql:main", "sql:main", metric, None)
}

fn dt(day: u32, hour: u32) -> Bound {
    Bound::Datetime(Utc.with_ymd_and_hms(2026, 10, day, hour, 0, 0).unwrap())
}

struct Harness {
    graph: Graph,
    state: Arc<MemoryState>,
    fake: Arc<FakeExecutor>,
    leases: Arc<MemoryLeases>,
    sink: Arc<VecSink>,
    durations: EwmaDurations,
}

impl Harness {
    fn new(records: Vec<PipeRecord>, state: MemoryStateData) -> Harness {
        let state = Arc::new(MemoryState::new(state));
        Harness {
            graph: Graph::build(records, &GraphOptions::default()),
            fake: Arc::new(FakeExecutor::new(
                Duration::from_millis(10),
                Some(state.clone()),
            )),
            state,
            leases: Arc::new(MemoryLeases::default()),
            sink: Arc::new(VecSink::default()),
            durations: EwmaDurations::new(0.5, Duration::from_secs(10)),
        }
    }

    async fn run(&self, selection: Selection, options: ExecutorOptions) -> (Plan, RunRecord) {
        let sub = Pipeline::new(selection).resolve(&self.graph).unwrap();
        let plan = plan(&self.graph, &sub, &*self.state, &options.plan).await;
        let ctx = ExecuteContext {
            graph: &self.graph,
            subgraph: &sub,
            state: &*self.state,
            executor: self.fake.clone(),
            leases: self.leases.clone(),
            sink: self.sink.clone(),
            durations: &self.durations,
        };
        let record = execute(ctx, plan.clone(), &options).await;
        (plan, record)
    }
}

fn rollup_records() -> Vec<PipeRecord> {
    vec![
        rec(
            "raw",
            r#"{"columns": {"datetime": "ts"}, "verify": {"chunk_hours": 1}}"#,
        ),
        rec(
            "rollup",
            r#"{"columns": {"datetime": "day"}, "verify": {"chunk_days": 1},
                "sql": "SELECT date_trunc('day', ts) AS day, count(*) FROM {{ Pipe('sql:main', 'raw') }} GROUP BY 1"}"#,
        ),
    ]
}

fn rollup_state() -> MemoryStateData {
    let mut s = MemoryStateData::default();
    s.watermarks.insert(r("raw"), Some(dt(12, 9)));
    s.watermarks.insert(r("rollup"), Some(dt(12, 0)));
    s.ledger.insert(
        "sql:main".into(),
        vec![crate::ledger::LedgerEntry {
            pipe_ref: r("raw"),
            seq: 5,
            begin: Some(dt(12, 8)),
            end: Some(dt(12, 9)),
            created: Utc::now(),
        }],
    );
    s.cursors.insert(
        "sql:main".into(),
        vec![EdgeCursor {
            parent_ref: r("raw"),
            child_ref: r("rollup"),
            seq: 5,
            parent_watermark_at_cursor: Some(dt(12, 9)),
        }],
    );
    s
}

#[tokio::test(start_paused = true)]
async fn rollup_is_replanned_after_its_parent_syncs() {
    let h = Harness::new(rollup_records(), rollup_state());
    // raw syncs one new hour of data: a ledger entry and a new watermark.
    h.fake.script(
        &r("raw"),
        FakeStep {
            ledger: vec![("sql:main".into(), Some(dt(12, 9)), Some(dt(12, 10)))],
            watermark: Some(Some(dt(12, 10))),
            ..FakeStep::ok(Duration::from_secs(1))
        },
    );
    let sel = Selection::refs([r("rollup")]).with_parents(Some(Depth::All));
    let (plan, record) = h.run(sel, ExecutorOptions::default()).await;
    // Statically, rollup has nothing to do (cursor-first skip), provisional on raw.
    let p = plan.node(&r("rollup")).unwrap();
    assert_eq!((p.action, p.provisional), (Action::Skip, true));
    // After raw synced, rollup is re-planned and runs an incremental sync, capped at raw's
    // new watermark.
    assert_eq!(h.fake.synced(), vec![r("raw"), r("rollup")]);
    let call = &h.fake.calls()[1];
    assert_eq!(call.action, Action::Incremental);
    let bounds = call.bounds.unwrap();
    assert_eq!(bounds.begin, crate::plan::Begin::At(dt(11, 0)));
    assert_eq!(bounds.end, Some(dt(12, 10)));
    assert_eq!(
        record.node(&r("rollup")).unwrap().status,
        NodeStatus::Succeeded
    );
    // The cursor moved past the new entry, with the parent watermark it consumed.
    let cursor = h
        .state
        .cursor("sql:main", &r("raw"), &r("rollup"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.seq, 6);
    assert_eq!(cursor.parent_watermark_at_cursor, Some(dt(12, 10)));
    assert!(
        h.sink
            .names(Some(&r("raw")))
            .contains(&"watermark_advanced")
    );
    assert!(record.run.success.unwrap());
}

#[tokio::test(start_paused = true)]
async fn rollup_stays_skipped_when_its_parent_brings_nothing() {
    let h = Harness::new(rollup_records(), rollup_state());
    let sel = Selection::refs([r("rollup")]).with_parents(Some(Depth::All));
    let (_, record) = h.run(sel, ExecutorOptions::default()).await;
    assert_eq!(h.fake.synced(), vec![r("raw")]);
    assert_eq!(
        record.node(&r("rollup")).unwrap().status,
        NodeStatus::Skipped
    );
    // The baseline definition hash is recorded for both nodes.
    let data = h.state.snapshot();
    assert_eq!(data.definition_hashes.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn failures_propagate_and_retries_follow_the_policy() {
    let records = vec![
        rec("a", r#"{"retry": {"max": 2, "backoff_seconds": 30}}"#),
        rec("b", r#"{"parents": ["Pipe('sql:main', 'a')"]}"#),
        rec("c", r#"{"parents": ["Pipe('sql:main', 'b')"]}"#),
        rec("x", "{}"),
    ];
    let h = Harness::new(records, MemoryStateData::default());
    for _ in 0..3 {
        h.fake.script(&r("a"), FakeStep::fail("connection reset"));
    }
    let sel = Selection::refs([r("c"), r("x")]).with_parents(Some(Depth::All));
    let start = tokio::time::Instant::now();
    let (_, record) = h.run(sel, ExecutorOptions::default()).await;
    assert_eq!(
        h.fake.synced().iter().filter(|p| **p == r("a")).count(),
        3,
        "one attempt plus two retries"
    );
    assert!(
        start.elapsed() >= Duration::from_secs(60),
        "two 30 s backoffs"
    );
    assert_eq!(record.node(&r("a")).unwrap().status, NodeStatus::Failed);
    assert_eq!(record.node(&r("a")).unwrap().attempts, 3);
    assert_eq!(
        record.node(&r("b")).unwrap().status,
        NodeStatus::UpstreamFailed
    );
    assert_eq!(
        record.node(&r("c")).unwrap().status,
        NodeStatus::UpstreamFailed
    );
    assert_eq!(record.node(&r("x")).unwrap().status, NodeStatus::Succeeded);
    assert!(!record.run.success.unwrap());

    // --resume re-plans only what failed or never ran.
    h.fake.script(&r("a"), FakeStep::ok(Duration::ZERO));
    let options = ExecutorOptions {
        plan: PlanOptions {
            completed: record.completed_nodes(),
            resumed_from: Some(record.run.run_id.clone()),
            ..PlanOptions::default()
        },
        ..ExecutorOptions::default()
    };
    let before = h.fake.synced().len();
    let sel = Selection::refs([r("c"), r("x")]).with_parents(Some(Depth::All));
    let (plan, resumed) = h.run(sel, options).await;
    assert_eq!(plan.node(&r("x")).unwrap().action, Action::Skip);
    assert_eq!(&h.fake.synced()[before..], &[r("a"), r("b"), r("c")]);
    assert!(resumed.run.success.unwrap());
    assert_eq!(resumed.run.resumed_from, Some(record.run.run_id.clone()));
}

#[tokio::test(start_paused = true)]
async fn semaphores_cap_concurrency() {
    let mut records: Vec<PipeRecord> = (0..6).map(|i| rec(&format!("p{i}"), "{}")).collect();
    records.push(PipeRecord::new("sql:main", "plugin:x", "py", None));
    records[6].python = true;
    let h = Harness::new(records, MemoryStateData::default());
    let mut limits = Limits::default();
    limits.connectors.insert("sql:main".into(), 2);
    let options = ExecutorOptions {
        workers: 8,
        limits,
        ..ExecutorOptions::default()
    };
    let sel = Selection::filter(Default::default());
    let (_, record) = h.run(sel, options).await;
    assert_eq!(record.nodes.len(), 7);
    // Two sql:main nodes plus the plugin node.
    assert_eq!(h.fake.peak_concurrency(), 3);
    assert!(record.run.success.unwrap());

    let h = Harness::new(
        (0..3).map(|i| rec(&format!("p{i}"), "{}")).collect(),
        MemoryStateData::default(),
    );
    let options = ExecutorOptions {
        workers: 0,
        ..ExecutorOptions::default()
    };
    let (_, record) = h.run(Selection::filter(Default::default()), options).await;
    assert!(record.nodes.iter().all(|n| n.status == NodeStatus::Failed));
}

#[tokio::test(start_paused = true)]
async fn critical_path_goes_first() {
    let records = vec![
        rec("short", "{}"),
        rec("root", "{}"),
        rec("long", r#"{"parents": ["Pipe('sql:main', 'root')"]}"#),
    ];
    let mut h = Harness::new(records, MemoryStateData::default());
    h.durations
        .observe(&r("long"), Action::Incremental, Duration::from_secs(100));
    let options = ExecutorOptions {
        workers: 1,
        ..ExecutorOptions::default()
    };
    let sel = Selection::refs([r("short"), r("long")]).with_parents(Some(Depth::All));
    h.run(sel, options).await;
    assert_eq!(h.fake.synced(), vec![r("root"), r("long"), r("short")]);
}

#[tokio::test(start_paused = true)]
async fn a_held_lease_fails_the_attempt() {
    let h = Harness::new(vec![rec("a", "{}")], MemoryStateData::default());
    h.leases
        .acquire(&r("a"), "another-job", Duration::from_secs(3600))
        .await
        .unwrap();
    let options = ExecutorOptions {
        lease_wait: Duration::from_secs(2),
        lease_poll: Duration::from_millis(250),
        ..ExecutorOptions::default()
    };
    let (_, record) = h.run(Selection::refs([r("a")]), options).await;
    let row = record.node(&r("a")).unwrap();
    assert_eq!(row.status, NodeStatus::Failed);
    assert!(
        row.message
            .as_deref()
            .unwrap()
            .contains("another process is syncing")
    );
    assert!(h.fake.synced().is_empty());
    assert_eq!(h.leases.holder(&r("a")).as_deref(), Some("another-job"));
}

#[tokio::test(start_paused = true)]
async fn a_held_lease_is_not_recorded_as_a_failed_sync() {
    let h = Harness::new(
        vec![rec("a", "{}"), rec("b", "{}")],
        MemoryStateData::default(),
    );
    h.leases
        .acquire(&r("a"), "another-job", Duration::from_secs(3600))
        .await
        .unwrap();
    h.fake.script(&r("b"), FakeStep::fail("the source is down"));
    let options = ExecutorOptions {
        lease_wait: Duration::from_secs(2),
        lease_poll: Duration::from_millis(250),
        ..ExecutorOptions::default()
    };
    let (_, record) = h.run(Selection::refs([r("a"), r("b")]), options).await;
    assert_eq!(record.node(&r("a")).unwrap().status, NodeStatus::Failed);
    assert_eq!(record.node(&r("b")).unwrap().status, NodeStatus::Failed);
    let statuses = h.state.snapshot().sync_statuses;
    // Another process is syncing `a` (and records that sync itself); `b` really failed.
    assert!(!statuses.contains_key(&r("a")), "{statuses:?}");
    let b = &statuses[&r("b")];
    assert!(!b.success);
    assert_eq!(b.consecutive_failures, 1);
    assert_eq!(b.message, "the source is down");
}

#[tokio::test(start_paused = true)]
async fn rebuild_pending_does_not_block_descendants() {
    let records = vec![
        rec("src", r#"{"sql": "SELECT 2"}"#),
        rec("dst", r#"{"parents": ["Pipe('sql:main', 'src')"]}"#),
    ];
    let mut state = MemoryStateData::default();
    state.definition_hashes.insert(r("src"), "v1:0000".into());
    let h = Harness::new(records, state);
    let sel = Selection::refs([r("dst")]).with_parents(Some(Depth::All));
    let (first, record) = h.run(sel, ExecutorOptions::default()).await;
    assert_eq!(
        first.node(&r("src")).unwrap().action,
        Action::RebuildPending
    );
    assert_eq!(
        record.node(&r("src")).unwrap().status,
        NodeStatus::RebuildPending
    );
    assert_eq!(h.fake.synced(), vec![r("dst")]);
    // The run names the pipe and how to approve, for the CLI to print (never a silent stop).
    let result = crate::engine::RunResult {
        success: true,
        message: String::new(),
        plan: None,
        record: Some(record.clone()),
        outcomes: Vec::new(),
        critical_path: Vec::new(),
        warnings: Vec::new(),
        duration: std::time::Duration::ZERO,
    };
    let pending = result.rebuild_pending();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].pipe_ref, r("src"));
    assert!(pending[0].reason.contains("--yes"), "{}", pending[0].reason);
    assert_eq!(result.lap_summary(1).rebuild_pending, pending);
    assert!(
        result
            .lap_summary(1)
            .v4_message("1 second")
            .contains("1 not synced: rebuild_pending")
    );
    assert!(
        h.sink
            .names(Some(&r("src")))
            .contains(&"node_rebuild_pending")
    );
    // The stale hash is kept until someone confirms.
    assert_eq!(
        h.state.snapshot().definition_hashes[&r("src")],
        "v1:0000".to_string()
    );

    // With --yes, the rebuild clears and re-syncs, then records the new hash.
    let options = ExecutorOptions {
        plan: PlanOptions {
            yes: true,
            ..PlanOptions::default()
        },
        ..ExecutorOptions::default()
    };
    let sel = Selection::refs([r("src")]);
    let sub = Pipeline::new(sel.with_children(Some(Depth::All)))
        .resolve(&h.graph)
        .unwrap();
    let plan = plan(&h.graph, &sub, &*h.state, &options.plan).await;
    assert_eq!(plan.node(&r("src")).unwrap().action, Action::Rebuild);
    let ctx = ExecuteContext {
        graph: &h.graph,
        subgraph: &sub,
        state: &*h.state,
        executor: h.fake.clone(),
        leases: h.leases.clone(),
        sink: h.sink.clone(),
        durations: &h.durations,
    };
    execute(ctx, plan, &options).await;
    let last = h
        .fake
        .calls()
        .into_iter()
        .find(|c| c.pipe_ref == r("src"))
        .unwrap();
    assert!(last.clear);
    assert_ne!(
        h.state.snapshot().definition_hashes[&r("src")],
        "v1:0000".to_string()
    );
}

struct LostLease;
impl LeaseSource for LostLease {
    async fn acquire(&self, _: &PipeRef, _: &str, _: Duration) -> Result<bool, StateError> {
        Ok(true)
    }
    async fn renew(&self, _: &PipeRef, _: &str, _: Duration) -> Result<bool, StateError> {
        Ok(false)
    }
    async fn release(&self, _: &PipeRef, _: &str) -> Result<(), StateError> {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn a_lost_lease_cancels_sync_before_it_commits() {
    let h = Harness::new(vec![rec("a", "{}")], MemoryStateData::default());
    let mut step = FakeStep::ok(Duration::from_secs(60));
    step.watermark = Some(Some(Bound::Int(99)));
    h.fake.script(&r("a"), step);
    let selection = Selection::refs([r("a")]);
    let sub = Pipeline::new(selection).resolve(&h.graph).unwrap();
    let options = ExecutorOptions {
        lease_ttl: Duration::from_secs(3),
        ..Default::default()
    };
    let plan = plan(&h.graph, &sub, &*h.state, &options.plan).await;
    let record = execute(
        ExecuteContext {
            graph: &h.graph,
            subgraph: &sub,
            state: &*h.state,
            executor: h.fake.clone(),
            leases: Arc::new(LostLease),
            sink: h.sink.clone(),
            durations: &h.durations,
        },
        plan,
        &options,
    )
    .await;
    let row = record.node(&r("a")).unwrap();
    assert_eq!(row.status, NodeStatus::Failed);
    assert!(row.message.as_deref().unwrap().contains("lost during sync"));
    assert_eq!(h.state.watermark(&r("a")).await.unwrap(), None);
}

/// Dropping a run mid-sync (Ctrl-C, a closed request) hands the held leases back at once
/// instead of leaving them to their TTL.
#[tokio::test(start_paused = true)]
async fn a_cancelled_run_releases_its_leases() {
    let h = Harness::new(
        vec![rec("a", "{}"), rec("b", "{}")],
        MemoryStateData::default(),
    );
    for metric in ["a", "b"] {
        h.fake
            .script(&r(metric), FakeStep::ok(Duration::from_secs(600)));
    }
    let options = ExecutorOptions {
        lease_ttl: Duration::from_secs(60),
        ..Default::default()
    };
    // The run is dropped by the timeout, as `Session::block_on` drops it on Ctrl-C.
    let cancelled = tokio::time::timeout(
        Duration::from_secs(5),
        h.run(Selection::refs([r("a"), r("b")]), options),
    )
    .await;
    assert!(cancelled.is_err(), "the run was still going");
    drain_lease_releases(Duration::from_secs(5)).await;
    let held = h.leases.held.lock().unwrap();
    assert!(held.is_empty(), "leases still held: {held:?}");
}

/// A run that finishes releases its leases itself and arms no release task.
#[tokio::test(start_paused = true)]
async fn a_finished_run_leaves_no_pending_release() {
    let h = Harness::new(vec![rec("a", "{}")], MemoryStateData::default());
    h.fake.script(&r("a"), FakeStep::ok(Duration::from_secs(1)));
    let (_, record) = h
        .run(Selection::refs([r("a")]), ExecutorOptions::default())
        .await;
    assert_eq!(record.node(&r("a")).unwrap().status, NodeStatus::Succeeded);
    assert!(h.leases.held.lock().unwrap().is_empty());
}
