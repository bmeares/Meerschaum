//! Stream tests against a disposable Mosquitto (§5.6, §14): QoS 1 with a persistent session,
//! redelivery, and a forced crash mid-flush, through both the native `subscribe` path and the
//! v4-pattern adapter path.
//!
//! Gated on `MRSM_TEST_MQTT_URI` (an empty value means `mqtt://127.0.0.1:1893`, the
//! `mrsm-v5-test-mosquitto` container: `docker run -d --rm --name mrsm-v5-test-mosquitto
//! -p 127.0.0.1:1893:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf`). Port 1883
//! is never used.
//!
//! The crash is a process death simulated by aborting the engine task right after the data
//! commit of a flush and before the acknowledgement: the connection drops without a PUBACK, so
//! the broker redelivers (`DUP`) every unacknowledged message to the restarted subscriber, which
//! must end with every row exactly once and nothing left unacknowledged.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use common::mqtt::{self, MqttLegacy, MqttSource};
use common::*;
use meerschaum_connector::DataQuery;
use meerschaum_core::Pipe;
use meerschaum_engine::{
    Engine, FaultInjector, KeyFilter, LapEvent, LoopOptions, PipeRef, RunOptions, Selection,
    StaticRegistry, StopToken, SyncArgs,
};
use meerschaum_kernel::enforce::PipeInput;

const CONNECTOR: &str = "mqtt:w7";
const MESSAGES: usize = 6;

/// Fails the first data commit's follow-up: the commit happened, the acknowledgement must not.
struct CrashAfterCommit {
    fired: AtomicBool,
    flag: Arc<AtomicBool>,
}

impl FaultInjector for CrashAfterCommit {
    fn after_data_commit(&self, _node: &PipeRef) -> Result<(), String> {
        if !self.fired.swap(true, Ordering::SeqCst) {
            self.flag.store(true, Ordering::SeqCst);
            return Err("simulated crash after the data commit".into());
        }
        Ok(())
    }
}

fn landing_params() -> serde_json::Value {
    serde_json::json!({
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        "stream": {"idle_ms": 50, "max_rows": 100},
    })
}

async fn register_landing(env: &Env) -> Pipe {
    let landing = Pipe::builder(CONNECTOR, "readings")
        .instance(INSTANCE)
        .parameters(json_map(&landing_params()))
        .build()
        .unwrap();
    let res = env.conn.register_pipe(&landing).await;
    assert!(res.success, "{}", res.message);
    landing
}

async fn landing_rows(env: &Env) -> Vec<String> {
    let pipe = Pipe::builder(CONNECTOR, "readings")
        .instance(INSTANCE)
        .build()
        .unwrap();
    let Some(df) = env
        .conn
        .get_pipe_data(&pipe, &DataQuery::default())
        .await
        .unwrap()
    else {
        return Vec::new();
    };
    let docs = meerschaum_connector::pipe_ops::frame_to_docs(&df);
    let mut rows: Vec<String> = docs
        .iter()
        .map(|d| format!("{} {}", norm_kvalue(&d["ts"]), norm_kvalue(&d["v"])))
        .collect();
    rows.sort();
    rows
}

fn payload(i: usize) -> String {
    format!(r#"{{"ts": "2026-01-01T00:{i:02}:00Z", "id": 1, "v": {i}.0}}"#)
}

fn expected_rows() -> Vec<String> {
    (0..MESSAGES)
        .map(|i| format!("2026-01-01T00:{i:02}:00Z {i}.000000"))
        .collect()
}

fn listen_opts() -> RunOptions {
    RunOptions {
        selection: Some(Selection::filter(KeyFilter {
            connector_keys: vec![CONNECTOR.into()],
            ..Default::default()
        })),
        sync: meerschaum_engine::NodeSyncOptions {
            // The stream skips the existing-row diff; redelivered messages must still dedupe.
            check_existing: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn spawn_listener(
    engine: Arc<Engine>,
    stop: StopToken,
) -> tokio::task::JoinHandle<meerschaum_engine::LoopResult> {
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
}

async fn wait_until(secs: u64, mut f: impl AsyncFnMut() -> bool) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(secs) {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

struct Names {
    client: String,
    topic: String,
}

fn names() -> Names {
    let tag = uuid::Uuid::new_v4().simple().to_string();
    Names {
        client: format!("w7-engine-{tag}"),
        topic: format!("w7/engine/{tag}"),
    }
}

async fn seed(addr: &str, n: &Names) {
    mqtt::create_session(addr, &n.client, &n.topic).await;
    for i in 0..MESSAGES {
        mqtt::publish(addr, &n.topic, &payload(i)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_subscribe_survives_a_crash_mid_flush() {
    let Some(addr) = mqtt::broker() else {
        eprintln!("skipped: MRSM_TEST_MQTT_URI is not set");
        return;
    };
    let n = names();
    let env = env("mqtt_native").await;
    register_landing(&env).await;
    seed(&addr, &n).await;

    // First incarnation: the data commits, then the "process" dies before acknowledging.
    let crashed = Arc::new(AtomicBool::new(false));
    let source = Arc::new(MqttSource::new(&addr, &n.client, &n.topic));
    let registry = Arc::new(StaticRegistry::clone(&env.registry).with_stream(CONNECTOR, source));
    let engine = Arc::new(Engine::new(registry, env.config()).with_faults(Arc::new(
        CrashAfterCommit {
            fired: AtomicBool::new(false),
            flag: crashed.clone(),
        },
    )));
    let first = spawn_listener(engine, StopToken::new());
    assert!(
        wait_until(60, async || crashed.load(Ordering::SeqCst)).await,
        "the flush never reached its commit"
    );
    first.abort();
    let _ = first.await;
    assert!(
        !landing_rows(&env).await.is_empty(),
        "the crash happened after the commit, so rows are there"
    );

    // Second incarnation: the broker redelivers the unacknowledged messages with DUP.
    let source = Arc::new(MqttSource::new(&addr, &n.client, &n.topic));
    let registry =
        Arc::new(StaticRegistry::clone(&env.registry).with_stream(CONNECTOR, source.clone()));
    let engine = Arc::new(Engine::new(registry, env.config()));
    let stop = StopToken::new();
    let second = spawn_listener(engine, stop.clone());
    assert!(
        wait_until(60, async || landing_rows(&env).await.len() == MESSAGES).await,
        "rows after the restart: {:?}",
        landing_rows(&env).await
    );
    // Let the sink acknowledge, then stop gracefully.
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.stop();
    let _ = second.await;

    assert_eq!(landing_rows(&env).await, expected_rows());
    let deliveries = source.deliveries();
    assert!(
        deliveries.len() >= MESSAGES && deliveries.iter().all(|(_, dup)| *dup),
        "every message must be a redelivery after the crash: {deliveries:?}"
    );
    let left = mqtt::drain_session(&addr, &n.client, Duration::from_millis(800)).await;
    assert!(left.is_empty(), "unacknowledged messages remain: {left:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_adapter_survives_a_crash_mid_callback() {
    let Some(addr) = mqtt::broker() else {
        eprintln!("skipped: MRSM_TEST_MQTT_URI is not set");
        return;
    };
    let n = names();
    let env = env("mqtt_legacy").await;
    let landing = register_landing(&env).await;
    seed(&addr, &n).await;

    // The v4 plugin's callback: `pipe.sync(df)` through the native path, then (in paho) the
    // automatic PUBACK when the callback returns.
    let make_legacy = |engine: Arc<Engine>, crash: Option<Arc<AtomicBool>>| {
        let landing = landing.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let callback: mqtt::Callback = Arc::new(move |doc, dup| {
            let engine = engine.clone();
            let landing = landing.clone();
            let crash = crash.clone();
            let calls = calls.clone();
            Box::pin(async move {
                let args = SyncArgs {
                    // A redelivery must diff even if the stream otherwise skips it.
                    check_existing: true,
                    ..Default::default()
                };
                let _ = dup;
                let res = engine
                    .sync_pipe(&landing, Some(PipeInput::Docs(vec![doc])), &args)
                    .await;
                if res.success && calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    if let Some(flag) = &crash {
                        // Committed; the process dies before the PUBACK.
                        flag.store(true, Ordering::SeqCst);
                        std::future::pending::<()>().await;
                    }
                }
                res.success
            })
        });
        Arc::new(MqttLegacy {
            addr: addr.clone(),
            client_id: n.client.clone(),
            topic: n.topic.clone(),
            callback,
        })
    };

    let crashed = Arc::new(AtomicBool::new(false));
    // The callbacks sync through their own bare engine (the native path, R1).
    let bootstrap = Arc::new(Engine::new(env.registry.clone(), env.config()));
    let legacy = make_legacy(bootstrap, Some(crashed.clone()));
    let registry =
        Arc::new(StaticRegistry::clone(&env.registry).with_legacy_stream(CONNECTOR, legacy));
    let engine = Arc::new(Engine::new(registry, env.config()));
    let first = spawn_listener(engine, StopToken::new());
    assert!(
        wait_until(60, async || crashed.load(Ordering::SeqCst)).await,
        "the callback never committed"
    );
    first.abort();
    let _ = first.await;
    assert_eq!(landing_rows(&env).await.len(), 1);

    let bootstrap = Arc::new(Engine::new(env.registry.clone(), env.config()));
    let legacy = make_legacy(bootstrap, None);
    let registry =
        Arc::new(StaticRegistry::clone(&env.registry).with_legacy_stream(CONNECTOR, legacy));
    let engine = Arc::new(Engine::new(registry, env.config()));
    let stop = StopToken::new();
    let second = spawn_listener(engine, stop.clone());
    assert!(
        wait_until(60, async || landing_rows(&env).await.len() == MESSAGES).await,
        "rows after the restart: {:?}",
        landing_rows(&env).await
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.stop();
    let _ = second.await;

    assert_eq!(landing_rows(&env).await, expected_rows());
    let left = mqtt::drain_session(&addr, &n.client, Duration::from_millis(800)).await;
    assert!(left.is_empty(), "unacknowledged messages remain: {left:?}");
}
