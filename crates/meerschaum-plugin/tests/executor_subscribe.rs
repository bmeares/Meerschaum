//! ABI-level tests of native executors and pull-model subscriptions, over the SDK examples
//! `native_executor` (`procs:<label>`) and `native_stream` (`ticker:<label>`,
//! `plugin:native_stream`). All data is synthetic; every test uses temporary directories.
use meerschaum_core::{Pipe, Value};
use meerschaum_plugin::{
    LoadedPlugin,
    executor::{LogEvent, MonitorLogs},
    serde_json::json,
    subscribe::NativeBatch,
};
use std::{
    path::PathBuf,
    process::Command,
    sync::OnceLock,
    time::{Duration, Instant},
};

fn artifact(name: &str) -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let status = Command::new(env!("CARGO"))
            .args(["build", "-p", "meerschaum-plugin", "--examples"])
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", plugins.path())
            .status()
            .unwrap();
        assert!(status.success());
    });
    let executable = std::env::current_exe().unwrap();
    let profile = executable.parent().unwrap().parent().unwrap();
    profile.join("examples").join(format!(
        "{}{name}.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}

fn pipe(connector: &str, fetch: serde_json::Value) -> Pipe {
    let mut pipe = Pipe::builder(connector, "stream_abi").build().unwrap();
    pipe.set_parameters(
        Value::from_json_str(&json!({"fetch": fetch}).to_string())
            .unwrap()
            .to_map(),
    );
    pipe
}

fn ids(batch: &NativeBatch) -> Vec<i64> {
    match batch {
        NativeBatch::Data { frame, .. } => (0..frame.height())
            .map(|i| match &frame.row(i)[0] {
                meerschaum_kernel::Value::Int(id) => *id,
                other => panic!("unexpected id {other:?}"),
            })
            .collect(),
        _ => Vec::new(),
    }
}

#[test]
fn executor_methods_unsupported_and_log_monitor() {
    // SAFETY: the test builds this SDK-owned example from the repository.
    let plugin = unsafe { LoadedPlugin::load(artifact("native_executor")) }.unwrap();
    assert_eq!(
        plugin.manifest["executor_types"]["procs"]["plugin"],
        "native_executor"
    );
    assert!(plugin.executor("nope", "x", json!({})).is_err());
    let jobs = tempfile::tempdir().unwrap();
    let executor = plugin
        .executor("procs", "abi", json!({"jobs_dir": jobs.path()}))
        .unwrap();
    assert_eq!(executor.keys(), "procs:abi");
    assert_eq!(
        executor.call("get_job_names", json!({})).unwrap(),
        json!([])
    );
    assert_eq!(
        executor
            .call("get_job_metadata", json!({"name": "missing"}))
            .unwrap(),
        serde_json::Value::Null
    );
    // `pause_job` is not implemented by the example: status 3, not an error string.
    assert!(matches!(
        executor.call("pause_job", json!({"name": "missing"})),
        Err(meerschaum_plugin::Error::Unsupported(_))
    ));
    assert!(
        executor
            .monitor_logs("missing", MonitorLogs::default())
            .is_err()
    );

    // A stopped job written by hand: the defaults derive status and timestamps from metadata.
    let dir = jobs.path().join("handmade");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("job.json"),
        json!({"sysargs": ["show", "version"], "began": "2026-01-01T00:00:00+00:00",
            "ended": null, "stop_time": null})
        .to_string(),
    )
    .unwrap();
    std::fs::write(dir.join("output.log"), "first\nsecond\npart").unwrap();
    assert_eq!(
        executor.call("get_job_names", json!({})).unwrap(),
        json!(["handmade"])
    );
    assert_eq!(
        executor
            .call("get_job_exists", json!({"name": "handmade"}))
            .unwrap(),
        json!(true)
    );
    assert_eq!(
        executor
            .call("get_job_status", json!({"name": "handmade"}))
            .unwrap(),
        json!("stopped")
    );
    assert_eq!(
        executor
            .call("get_job_began", json!({"name": "handmade"}))
            .unwrap(),
        json!("2026-01-01T00:00:00+00:00")
    );
    assert!(executor.call("get_jobs", json!({})).unwrap()["handmade"]["daemon"].is_object());

    let mut stream = executor
        .monitor_logs("handmade", MonitorLogs::default())
        .unwrap();
    assert_eq!(
        stream.next_lines(Duration::from_millis(200)).unwrap(),
        LogEvent::Lines(vec!["first".into(), "second".into()])
    );
    assert_eq!(
        stream.next_lines(Duration::from_millis(50)).unwrap(),
        LogEvent::Timeout
    );
    std::fs::OpenOptions::new()
        .append(true)
        .open(dir.join("output.log"))
        .and_then(|mut f| std::io::Write::write_all(&mut f, b"ial\n"))
        .unwrap();
    assert_eq!(
        stream.next_lines(Duration::from_millis(500)).unwrap(),
        LogEvent::Lines(vec!["partial".into()])
    );
    stream.close().unwrap();
    stream.close().unwrap();
    assert!(stream.next_lines(Duration::from_millis(10)).is_err());

    // `stop_on_exit` ends the stream of a job without a live process.
    let mut ending = executor
        .monitor_logs(
            "handmade",
            MonitorLogs {
                stop_on_exit: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(matches!(
        ending.next_lines(Duration::from_millis(200)).unwrap(),
        LogEvent::Lines(_)
    ));
    assert!(matches!(
        ending.next_lines(Duration::from_millis(200)).unwrap(),
        LogEvent::End(Some(_))
    ));
}

#[test]
fn ticker_subscription_pulls_acks_and_closes() {
    // SAFETY: the test builds this SDK-owned example from the repository.
    let plugin = unsafe { LoadedPlugin::load(artifact("native_stream")) }.unwrap();
    let connector = plugin.connector("ticker", "abi", json!({})).unwrap();
    assert!(connector.is_streaming());
    assert!(plugin.source(json!({})).is_streaming());
    let demo = plugin
        .connector("plugin", "native_stream", json!({}))
        .unwrap();
    assert!(demo.is_streaming());

    let subscription = connector
        .subscribe(
            &pipe(
                "ticker:abi",
                json!({"count": 3, "interval_ms": 20, "start": 10}),
            ),
            json!({}),
        )
        .unwrap();
    let mut seen = Vec::new();
    let mut tokens = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && Instant::now() < deadline {
        let batch = subscription.next_batch(Duration::from_millis(200)).unwrap();
        if let NativeBatch::Data {
            ack, redelivered, ..
        } = &batch
        {
            assert!(!redelivered);
            tokens.push(ack.unwrap());
        }
        seen.extend(ids(&batch));
    }
    assert_eq!(seen, vec![10, 11, 12]);
    assert_eq!(tokens, vec![10, 11, 12]);
    // Exhausted: timeouts, never `end`.
    assert!(matches!(
        subscription.next_batch(Duration::from_millis(20)).unwrap(),
        NativeBatch::Timeout
    ));
    subscription.ack(&tokens).unwrap();

    // A blocking `next_batch` is not serialized with the plugin's other calls.
    let blocked = std::thread::scope(|scope| {
        let waiting = scope.spawn(|| subscription.next_batch(Duration::from_millis(1500)));
        std::thread::sleep(Duration::from_millis(100));
        let started = Instant::now();
        let other = plugin.call_json("unknown_method", json!({}));
        let elapsed = started.elapsed();
        assert!(matches!(
            other,
            Err(meerschaum_plugin::Error::Unsupported(_))
        ));
        waiting.join().unwrap().unwrap();
        elapsed
    });
    assert!(blocked < Duration::from_millis(1000), "{blocked:?}");

    let handle = subscription.handle();
    subscription.close().unwrap();
    subscription.close().unwrap();
    drop(subscription);
    // The handle is gone once closed.
    let stale = meerschaum_plugin::Request {
        method: "next_batch".into(),
        connector_type: "ticker".into(),
        label: "abi".into(),
        config: json!({}),
        context: json!({"handle": handle, "timeout_ms": 10}),
    };
    assert!(plugin.invoke_stream(&stale).is_err());
}

#[test]
fn plugin_level_channel_subscription_delivers_in_order() {
    // SAFETY: the test builds this SDK-owned example from the repository.
    let plugin = unsafe { LoadedPlugin::load(artifact("native_stream")) }.unwrap();
    let source = plugin.source(json!({}));
    let subscription = source
        .subscribe(
            &pipe(
                "plugin:native_stream",
                json!({"count": 4, "interval_ms": 10}),
            ),
            json!({}),
        )
        .unwrap();
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < 4 && Instant::now() < deadline {
        seen.extend(ids(&subscription
            .next_batch(Duration::from_millis(200))
            .unwrap()));
    }
    assert_eq!(seen, vec![1, 2, 3, 4]);
    // Dropping closes the handle; the producer thread notices on its next send.
    drop(subscription);
}
