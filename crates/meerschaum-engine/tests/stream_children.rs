//! Stream flushes trigger the stream's children (§5.6 "Downstream triggering", §5.10), rate
//! limited by the child's `min_seconds`, with flushes that arrive meanwhile coalesced.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use indexmap::IndexMap;
use meerschaum_core::Pipe;
use meerschaum_engine::stream::{MemorySource, StreamMessage};
use meerschaum_engine::{
    Depth, Engine, EventKind, KeyFilter, LapEvent, LoopOptions, RunOptions, Selection,
    StaticRegistry, StopToken,
};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

fn reading(minute: u32, ack: u64) -> StreamMessage {
    let ts = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, minute, 0)
        .unwrap();
    let mut doc = IndexMap::new();
    doc.insert("ts".to_string(), KValue::Datetime(DateTimeValue::utc(ts)));
    doc.insert("id".to_string(), KValue::Int(1));
    doc.insert("v".to_string(), KValue::Float(f64::from(minute)));
    StreamMessage {
        docs: vec![doc],
        bytes: 32,
        ack: Some(ack),
        redelivered: false,
    }
}

async fn setup(
    child_min_seconds: Option<f64>,
    gap: Duration,
    count: u32,
) -> (Env, Arc<MemorySource>) {
    let env = env("stream_children").await;
    let source = Arc::new(MemorySource::new(
        (0..count).map(|i| reading(i, u64::from(i) + 1)).collect(),
        gap,
    ));
    let landing = Pipe::builder("mqtt:test", "readings")
        .instance(INSTANCE)
        .parameters(json_map(&serde_json::json!({
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
            // Every message flushes on its own.
            "stream": {"idle_ms": 20, "max_rows": 1},
        })))
        .build()
        .unwrap();
    let res = env.conn.register_pipe(&landing).await;
    assert!(res.success, "{}", res.message);
    let mut child = serde_json::json!({
        "sql": "SELECT ts, SUM(v) AS total FROM \"mqtt_test_readings\" GROUP BY ts",
        "columns": {"datetime": "ts"},
        "parents": ["Pipe('mqtt:test', 'readings', instance='sql:eng')"],
    });
    if let Some(s) = child_min_seconds {
        child["min_seconds"] = serde_json::json!(s);
    }
    env.register("totals", &child).await;
    (env, source)
}

fn listen_opts() -> RunOptions {
    RunOptions {
        // `--listen --children`: a flat selection keeps v4 semantics (no children).
        selection: Some(
            Selection::filter(KeyFilter {
                connector_keys: vec!["mqtt:test".into()],
                ..Default::default()
            })
            .with_children(Some(Depth::All)),
        ),
        ..Default::default()
    }
}

async fn run_listen(
    env: &Env,
    source: Arc<MemorySource>,
    seconds: f64,
) -> (Vec<meerschaum_engine::Event>, Arc<Engine>) {
    let registry = Arc::new(StaticRegistry::clone(&env.registry).with_stream("mqtt:test", source));
    let engine = Arc::new(Engine::new(registry, env.config()));
    let mut events = engine.subscribe();
    let stop = StopToken::new();
    let task = {
        let engine = engine.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let on_lap: Arc<dyn Fn(LapEvent) + Send + Sync> = Arc::new(|_| {});
            engine
                .run_loop(
                    listen_opts(),
                    LoopOptions {
                        listen: true,
                        stop,
                        ..Default::default()
                    },
                    on_lap,
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
    stop.stop();
    let _ = task.await;
    let mut out = Vec::new();
    while let Ok(e) = events.try_recv() {
        out.push(e);
    }
    (out, engine)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_triggers_the_children_without_waiting_for_a_lap() {
    let (env, source) = setup(None, Duration::from_millis(80), 5).await;
    // No child `min_seconds`, no debounce setting, `--min-seconds` 0: every flush may trigger.
    let (events, _engine) = run_listen(&env, source, 2.5).await;
    let triggered = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::ChildrenTriggered { .. }))
        .count();
    assert!(triggered >= 1, "no child run was triggered");
    // The aggregate caught up with every flushed row, in one loop with no further lap.
    let totals = env.table("totals").await;
    assert_eq!(totals.rows.len(), 5, "{totals:#?}");
    let finished_totals = events
        .iter()
        .filter(|e| {
            matches!(e.kind, EventKind::NodeFinished { .. })
                && e.pipe_ref
                    .as_ref()
                    .is_some_and(|r| r.as_str().contains("|totals|"))
        })
        .count();
    assert!(finished_totals >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_min_seconds_rate_limits_and_coalesces_flushes() {
    // Six flushes, 100 ms apart (0.5 s of stream); the child may run once per 0.6 s.
    let (env, source) = setup(Some(0.6), Duration::from_millis(100), 6).await;
    let (events, _engine) = run_listen(&env, source, 3.0).await;
    let runs: Vec<_> = events
        .iter()
        .filter(|e| {
            matches!(e.kind, EventKind::NodeFinished { .. })
                && e.pipe_ref
                    .as_ref()
                    .is_some_and(|r| r.as_str().contains("|totals|"))
        })
        .collect();
    // The first flush ran the child at once; the rest were coalesced into one held run.
    assert!(
        (1..=3).contains(&runs.len()),
        "the child ran {} times for 6 flushes",
        runs.len()
    );
    for pair in runs.windows(2) {
        let gap = (pair[1].ts - pair[0].ts).to_std().unwrap_or_default();
        assert!(
            gap >= Duration::from_millis(550),
            "child runs {gap:?} apart violate min_seconds=0.6"
        );
    }
    let flushes: u64 = events
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::ChildrenTriggered { flushes, .. } => Some(*flushes),
            _ => None,
        })
        .sum();
    assert!(
        flushes >= 2,
        "the coalesced trigger covered {flushes} flushes"
    );
    // Nothing was lost to the hold: the last held run caught the aggregate up.
    assert_eq!(env.table("totals").await.rows.len(), 6);
}
