//! Continuous mode (§5.5): lap-compatible `--loop --min-seconds N` on a flat selection, v4's
//! error cooldown, and a supervised stream source under `--listen` (§5.6) on SQLite.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::*;
use meerschaum_engine::stream::{MemorySource, StreamMessage, StreamSource};
use meerschaum_engine::{
    Engine, EngineConfig, KeyFilter, LapEvent, LoopOptions, RunOptions, Selection, StateSource,
    StaticRegistry, StopToken,
};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

type Log = Arc<Mutex<Vec<(Instant, LapEvent)>>>;

fn recorder() -> (Log, Arc<dyn Fn(LapEvent) + Send + Sync>) {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let l = log.clone();
    (
        log,
        Arc::new(move |e| l.lock().unwrap().push((Instant::now(), e))),
    )
}

fn source_params() -> serde_json::Value {
    serde_json::json!({
        "sql": "SELECT ts, id, v FROM src",
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loop_laps_match_v4_order_sleep_and_filters() {
    let env = env("laps").await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)")
        .await;
    // Registered c, a, b. v4's `get_pipes` order is the instance's `ORDER BY connector_keys,
    // metric_key, location_key` (`connectors/sql/_pipes.py:345`), so `-w 1` laps sync a, c.
    for location in ["c", "a", "b"] {
        let pipe = env.pipe("m", &source_params());
        let pipe = meerschaum_core::Pipe::builder(INSTANCE, "m")
            .location(Some(location))
            .instance(INSTANCE)
            .parameters(pipe.parameters().clone())
            .build()
            .unwrap();
        let res =
            meerschaum_connector::InstanceConnector::register_pipe(env.conn.as_ref(), &pipe).await;
        assert!(res.success, "{}", res.message);
    }
    let engine = env.engine();
    let (log, on_lap) = recorder();
    let opts = RunOptions {
        // `sync pipes -l _b -w 1 --loop --min-seconds 1`
        selection: Some(Selection::filter(KeyFilter {
            location_keys: vec!["_b".into()],
            ..Default::default()
        })),
        workers: Some(1),
        ..Default::default()
    };
    let result = engine
        .run_loop(
            opts,
            LoopOptions {
                run_loop: true,
                min_seconds: 1.0,
                max_laps: Some(2),
                ..Default::default()
            },
            on_lap,
        )
        .await;
    assert_eq!(result.laps.len(), 2);
    let log = log.lock().unwrap().clone();
    let kinds: Vec<String> = log
        .iter()
        .map(|(_, e)| match e {
            LapEvent::PipeFinished(o) => {
                format!("pipe {}", o.pipe_ref.parts().location_key.unwrap_or("None"))
            }
            LapEvent::LapFinished(s) => format!("lap {}", s.lap),
            LapEvent::Sleeping { message, .. } => message.clone(),
            LapEvent::Cooldown { message, .. } => message.clone(),
            LapEvent::Warning { message } => message.clone(),
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            "pipe a",
            "pipe c",
            "lap 1",
            "Sleeping for 1 second.",
            "pipe a",
            "pipe c",
            "lap 2",
        ]
    );
    // The sleep starts after the lap ends.
    let lap1_end = log[2].0;
    let lap2_first = log[4].0;
    assert!(lap2_first.duration_since(lap1_end) >= Duration::from_secs(1));

    let summary = &result.laps[0];
    assert!(summary.v4_success());
    assert_eq!(
        summary.v4_message("1s"),
        "It took 1s to sync 2 pipes\n    (2 succeeded, 0 failed)."
    );
    match &log[0].1 {
        LapEvent::PipeFinished(o) => {
            assert!(
                o.v4_message().starts_with("Finished syncing "),
                "{}",
                o.v4_message()
            );
            assert!(o.result.success);
        }
        other => panic!("{other:?}"),
    }
    // The first lap inserted the row, the second found nothing new.
    let last = result.last.unwrap();
    assert!(last.outcomes.iter().all(|o| o.result.inserted == 0));
}

#[tokio::test(start_paused = true)]
async fn failed_laps_wait_v4_cooldown() {
    // No instance is registered, so a lap cannot even read the pipes.
    let engine = Engine::new(
        Arc::new(StaticRegistry::new()),
        EngineConfig {
            home_instance: "sql:missing".into(),
            persist_runs: false,
            ..EngineConfig::default()
        },
    );
    let (log, on_lap) = recorder();
    let result = engine
        .run_loop(
            RunOptions::default(),
            LoopOptions {
                run_loop: true,
                min_seconds: 1.0,
                max_laps: Some(2),
                ..Default::default()
            },
            on_lap,
        )
        .await;
    assert!(result.laps.is_empty());
    let cooldowns: Vec<(f64, String)> = log
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, e)| match e {
            LapEvent::Cooldown { seconds, message } => Some((*seconds, message.clone())),
            _ => None,
        })
        .collect();
    // `cooldown = 2 * (min_seconds + 1)`, then `int(cooldown * 1.5)`.
    assert_eq!(
        cooldowns,
        vec![
            (
                4.0,
                "Failed to sync all pipes. Waiting for 4 seconds, then trying again.".into()
            ),
            (
                6.0,
                "Failed to sync all pipes. Waiting for 6 seconds, then trying again.".into()
            ),
        ]
    );
}

fn reading(minute: u32, id: i64, v: f64, ack: u64) -> StreamMessage {
    let ts = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, minute, 0)
        .unwrap();
    let mut doc = indexmap::IndexMap::new();
    doc.insert("ts".to_string(), KValue::Datetime(DateTimeValue::utc(ts)));
    doc.insert("id".to_string(), KValue::Int(id));
    doc.insert("v".to_string(), KValue::Float(v));
    StreamMessage {
        docs: vec![doc],
        bytes: 32,
        ack: Some(ack),
        redelivered: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listen_runs_supervised_stream_sources() {
    let base = env("stream").await;
    let source = Arc::new(MemorySource::new(
        (0..6)
            .map(|i| reading(i, 1, i as f64, u64::from(i) + 1))
            .collect(),
        Duration::from_millis(20),
    ));
    let stream_source: Arc<dyn StreamSource> = source.clone();
    let registry =
        Arc::new(StaticRegistry::clone(&base.registry).with_stream("mqtt:test", stream_source));
    let mut params = serde_json::json!({
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        "stream": {"idle_ms": 50, "max_rows": 4},
    });
    let landing = base.pipe("unused", &params);
    let landing = meerschaum_core::Pipe::builder("mqtt:test", "readings")
        .instance(INSTANCE)
        .parameters(landing.parameters().clone())
        .build()
        .unwrap();
    let res =
        meerschaum_connector::InstanceConnector::register_pipe(base.conn.as_ref(), &landing).await;
    assert!(res.success, "{}", res.message);
    // A child makes the stream write ledger entries on each flush.
    params = serde_json::json!({
        "sql": "SELECT ts, SUM(v) AS total FROM \"mqtt_test_readings\" GROUP BY ts",
        "columns": {"datetime": "ts"},
        "parents": ["Pipe('mqtt:test', 'readings', instance='sql:eng')"],
    });
    base.register("totals", &params).await;

    let engine = Engine::new(registry, base.config());
    let stop = StopToken::new();
    let (_log, on_lap) = recorder();
    let stopper = {
        let stop = stop.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            stop.stop();
        }
    };
    let opts = RunOptions {
        selection: Some(Selection::filter(KeyFilter {
            connector_keys: vec!["mqtt:test".into()],
            ..Default::default()
        })),
        ..Default::default()
    };
    let (result, ()) = tokio::join!(
        engine.run_loop(
            opts,
            LoopOptions {
                listen: true,
                stop: stop.clone(),
                ..Default::default()
            },
            on_lap,
        ),
        stopper
    );
    // The lap left the stream source to its subscription.
    assert_eq!(result.laps.len(), 1);
    assert!(result.last.unwrap().outcomes.is_empty());

    let landing_ref = meerschaum_engine::PipeRef::from_pipe(&landing);
    let rows = {
        let pipe = meerschaum_core::Pipe::builder("mqtt:test", "readings")
            .instance(INSTANCE)
            .build()
            .unwrap();
        meerschaum_connector::InstanceConnector::get_pipe_data(
            base.conn.as_ref(),
            &pipe,
            &Default::default(),
        )
        .await
        .unwrap()
        .map_or(0, |df| df.height())
    };
    assert_eq!(rows, 6);
    // Every message was acknowledged, and only after its rows committed.
    assert_eq!(source.acked(), vec![1, 2, 3, 4, 5, 6]);
    let seq = engine
        .state()
        .latest_seq(INSTANCE, &landing_ref)
        .await
        .unwrap();
    assert!(seq.is_some_and(|s| s >= 2), "{seq:?}");
}
