//! Pool behavior end to end: handshake, Python exceptions, crash isolation, timeouts, restart
//! backoff and concurrency.

mod common;

use std::time::Duration;

use meerschaum_worker::plugins::{ActionRequest, ping, run_action};
use meerschaum_worker::protocol::Msg;
use meerschaum_worker::{CallOptions, PROTOCOL_VERSION, Worker, WorkerError};

use common::TestEnv;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handshake_and_ping() {
    require_python!();
    let env = TestEnv::new();
    let worker = Worker::spawn(&env.worker_options()).await.expect("spawns");
    assert_eq!(worker.hello().protocol, PROTOCOL_VERSION);
    assert!(worker.hello().pid > 0);
    assert!(!worker.hello().meerschaum.is_empty());
    assert!(["msgpack", "stdlib"].contains(&worker.hello().msgpack.as_str()));
    let reply = worker
        .call(
            "ping",
            Msg::new().set("echo", "hi"),
            Vec::new(),
            CallOptions::default(),
        )
        .await
        .expect("pings");
    let m = Msg::from_wire(reply.value).unwrap();
    assert_eq!(m.str("echo"), Some("hi"));
    assert_eq!(m.u64("pid"), Some(worker.pid() as u64));

    let err = worker
        .call("no_such_op", Msg::new(), Vec::new(), CallOptions::default())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Unknown worker operation"),
        "{err}"
    );
    assert!(!err.is_fatal());
    worker.shutdown(Duration::from_secs(5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_fails_only_the_running_call() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let pid1 = ping(&pool).await.expect("first ping");

    let err = run_action(
        &pool,
        &ActionRequest::Sysargs(vec!["wk".into(), "crash".into()]),
        None,
        CallOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, WorkerError::Crashed { .. }), "{err}");
    assert!(err.to_string().contains("action"), "{err}");

    // The next call gets a fresh worker.
    let pid2 = ping(&pool).await.expect("ping after the crash");
    assert_ne!(pid1, pid2);
    let stats = pool.stats();
    assert_eq!(stats.spawned, 2);
    assert_eq!(stats.discarded, 1);
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_op_and_python_exceptions() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let pid1 = ping(&pool).await.unwrap();

    // A Python exception keeps the worker.
    let err = pool
        .call(
            "setup",
            Msg::new(),
            Vec::new(),
            CallOptions::default(),
            None,
        )
        .await
        .unwrap_err();
    assert!(!err.is_fatal(), "{err}");
    assert_eq!(ping(&pool).await.unwrap(), pid1);

    // `os._exit()` mid-call.
    let err = pool
        .call(
            "crash",
            Msg::new().set("code", 9u64),
            Vec::new(),
            CallOptions::default(),
            None,
        )
        .await
        .unwrap_err();
    match &err {
        WorkerError::Crashed { op, detail } => {
            assert_eq!(op, "crash");
            assert!(detail.contains('9'), "{detail}");
        }
        other => panic!("expected a crash, got {other}"),
    }
    assert_ne!(ping(&pool).await.unwrap(), pid1);
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_kills_the_worker() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(1);
    let pid1 = ping(&pool).await.unwrap();
    let started = std::time::Instant::now();
    let err = run_action(
        &pool,
        &ActionRequest::Sysargs(vec!["wk".into(), "sleep".into()]),
        None,
        CallOptions {
            timeout: Some(Duration::from_secs(3)),
            chunks: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, WorkerError::Timeout { .. }), "{err}");
    assert!(started.elapsed() < Duration::from_secs(30));
    assert_ne!(ping(&pool).await.unwrap(), pid1);
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_runs_calls_concurrently() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(2);
    let (a, b) = tokio::join!(ping(&pool), ping(&pool));
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(pool.stats().spawned >= 1);
    assert!(pool.idle_count() <= 2);
    // Both workers are reused afterwards.
    let c = ping(&pool).await.unwrap();
    assert!(c == a || c == b);
    pool.shutdown().await;
    assert!(matches!(
        ping(&pool).await.unwrap_err(),
        WorkerError::ShuttingDown
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_spawns_back_off() {
    let env = TestEnv::without_init();
    let mut opts = env.worker_options();
    opts.python = "/nonexistent/python-for-mrsm-tests".into();
    let pool = meerschaum_worker::WorkerPool::new(meerschaum_worker::PoolConfig {
        size: 1,
        worker: opts,
        backoff_initial: Duration::from_millis(200),
        backoff_max: Duration::from_millis(400),
        ..Default::default()
    });
    assert!(matches!(
        ping(&pool).await.unwrap_err(),
        WorkerError::Spawn(_)
    ));
    let started = std::time::Instant::now();
    assert!(ping(&pool).await.is_err());
    assert!(started.elapsed() >= Duration::from_millis(150));
}
