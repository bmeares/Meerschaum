//! v4-pattern streams (`LegacyStream`, §5.6 compatibility path) report every callback
//! `pipe.sync()` as a flush (wave 9): a `stream_flushed` event with trigger `callback` (rows,
//! begin/end, success), a ledger entry written before the data commit, and the same children
//! triggering as a `subscribe` stream (§5.10 "Stream-flush triggering"). A callback that syncs
//! a pipe outside the graph stays a bare `pipe.sync()` (R1) and reports nothing.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use indexmap::IndexMap;
use meerschaum_connector::async_trait;
use meerschaum_core::Pipe;
use meerschaum_engine::stream::{FlushTrigger, LegacySink, LegacyStream};
use meerschaum_engine::{
    Bound, Depth, Engine, EventKind, KeyFilter, LapEvent, LoopOptions, PipeRef, RunOptions,
    Selection, StateSource, StaticRegistry, StopToken, SyncArgs,
};
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

const CONNECTOR: &str = "legacy:test";

fn ts(minute: u32) -> chrono::NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, minute, 0)
        .unwrap()
}

fn reading(minute: u32) -> IndexMap<String, KValue> {
    let mut doc = IndexMap::new();
    doc.insert(
        "ts".to_string(),
        KValue::Datetime(DateTimeValue::utc(ts(minute))),
    );
    doc.insert("id".to_string(), KValue::Int(1));
    doc.insert("v".to_string(), KValue::Float(f64::from(minute)));
    doc
}

/// A v4 plugin whose `sync()` subscribes and returns, and whose `on_message` callback calls
/// `pipe.sync(df)` once per message. It also mirrors each message into an unrelated pipe that
/// is not in the engine's graph.
struct FakeLegacy {
    minutes: Vec<u32>,
    gap: Duration,
    mirror: Pipe,
}

#[async_trait]
impl LegacyStream for FakeLegacy {
    async fn run(
        &self,
        pipe: &Pipe,
        sink: Arc<dyn LegacySink>,
        stop: StopToken,
    ) -> Result<(), String> {
        for minute in &self.minutes {
            let input = PipeInput::Docs(vec![reading(*minute)]);
            let result = sink.sync(pipe, Some(input), &SyncArgs::default()).await;
            if !result.success {
                return Err(result.message);
            }
            let mirrored = sink
                .sync(
                    &self.mirror,
                    Some(PipeInput::Docs(vec![reading(*minute)])),
                    &SyncArgs::default(),
                )
                .await;
            assert!(mirrored.success, "{}", mirrored.message);
            if stop.sleep(self.gap).await {
                return Ok(());
            }
        }
        // The v4 connector's worker stays alive until the job stops.
        stop.stopped().await;
        Ok(())
    }
}

async fn setup() -> (Env, Pipe) {
    let env = env("legacy_flush").await;
    let landing = Pipe::builder(CONNECTOR, "readings")
        .instance(INSTANCE)
        .parameters(json_map(&serde_json::json!({
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        })))
        .build()
        .unwrap();
    let res = env.conn.register_pipe(&landing).await;
    assert!(res.success, "{}", res.message);
    env.register(
        "totals",
        &serde_json::json!({
            "sql": "SELECT ts, SUM(v) AS total FROM \"legacy_test_readings\" GROUP BY ts",
            "columns": {"datetime": "ts"},
            "parents": ["Pipe('legacy:test', 'readings', instance='sql:eng')"],
        }),
    )
    .await;
    // Not registered: the graph never sees it.
    let mirror = Pipe::builder("legacy:mirror", "copies")
        .instance(INSTANCE)
        .parameters(json_map(&serde_json::json!({
            "columns": {"datetime": "ts", "id": "id"},
        })))
        .build()
        .unwrap();
    (env, mirror)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callback_syncs_are_flushes_that_write_the_ledger_and_trigger_children() {
    let (env, mirror) = setup().await;
    let legacy = Arc::new(FakeLegacy {
        minutes: vec![0, 1, 2],
        gap: Duration::from_millis(150),
        mirror: mirror.clone(),
    });
    let registry =
        Arc::new(StaticRegistry::clone(&env.registry).with_legacy_stream(CONNECTOR, legacy));
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
                    RunOptions {
                        // `--listen --children`.
                        selection: Some(
                            Selection::filter(KeyFilter {
                                connector_keys: vec![CONNECTOR.into()],
                                ..Default::default()
                            })
                            .with_children(Some(Depth::All)),
                        ),
                        ..Default::default()
                    },
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
    let landing_ref = PipeRef::new(INSTANCE, CONNECTOR, "readings", None);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while env.table("totals").await.rows.len() < 3 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.stop();
    let _ = task.await;
    let mut seen = Vec::new();
    while let Ok(e) = events.try_recv() {
        seen.push(e);
    }

    // One `callback` flush per landing-pipe sync, with its rows and bounds.
    let flushes: Vec<_> = seen
        .iter()
        .filter_map(|e| match &e.kind {
            EventKind::StreamFlushed {
                rows,
                trigger,
                success,
                begin,
                end,
                inserted,
                ..
            } => Some((
                e.pipe_ref.clone(),
                *rows,
                *trigger,
                *success,
                *begin,
                *end,
                *inserted,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(flushes.len(), 3, "{flushes:#?}");
    for (i, (pipe_ref, rows, trigger, success, begin, end, inserted)) in flushes.iter().enumerate()
    {
        assert_eq!(pipe_ref.as_ref(), Some(&landing_ref));
        assert_eq!(*trigger, FlushTrigger::Callback);
        assert!(*success);
        assert_eq!(*rows, 1);
        assert_eq!(*inserted, 1);
        let at = Bound::Datetime(ts(i as u32).and_utc());
        assert_eq!((*begin, *end), (Some(at), Some(at)), "flush {i}");
    }

    // Ledger entries were written for the stream (it has a child).
    let seq = engine
        .state()
        .latest_seq(INSTANCE, &landing_ref)
        .await
        .unwrap();
    assert!(seq.is_some_and(|s| s >= 3), "ledger seq {seq:?}");

    // The flushes triggered the child, which caught up with every row.
    assert!(
        seen.iter()
            .any(|e| matches!(e.kind, EventKind::ChildrenTriggered { .. })),
        "no child run was triggered"
    );
    assert_eq!(env.table("totals").await.rows.len(), 3);

    // The mirror pipe synced bare: rows landed, no flush was reported for it.
    assert_eq!(env.table_of("legacy:mirror", "copies").await.rows.len(), 3);
    assert!(
        flushes
            .iter()
            .all(|f| f.0.as_ref().is_none_or(|r| !r.as_str().contains("copies")))
    );

    // Every callback sync recorded its pipe's sync status (rc15 C2): the stream's and the
    // mirror's. (Successes within five seconds are held, so the stored rows may be the first's.)
    use meerschaum_connector::InstanceConnector;
    for (ck, mk) in [(CONNECTOR, "readings"), ("legacy:mirror", "copies")] {
        let pipe = Pipe::builder(ck, mk).instance(INSTANCE).build().unwrap();
        let status = env
            .conn
            .get_sync_status(&pipe)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("no sync status for {pipe}"));
        assert!(status.success, "{status:?}");
        assert_eq!(status.consecutive_failures, 0);
        assert_eq!(status.rows_inserted, 1, "{status:?}");
    }
}
