//! `sync pipes --listen` engages the v4-pattern adapter for a custom MQTT-style connector
//! (§5.6 "Compatibility", wave 9): a `@make_connector` class assembled from imported methods,
//! whose `sync(pipe)` subscribes and returns and whose message callback calls `pipe.sync(docs)`
//! (the shape of the public `mqtt-connector` plugin; the plugin here is synthetic, in
//! `fixtures/w9_legacy_mqtt/`). The listener must
//!
//! - call the connector's `sync()` once per pipe and report it like v4's lap (`Finished
//!   syncing ...: Syncing ... in a background thread.`), with the `@post_sync_hook` receiving
//!   the result, timestamps and duration;
//! - keep the worker alive and land every published message.
//!
//! Gated on `MRSM_TEST_MQTT_URI` (an empty value means `mqtt://127.0.0.1:1893`, the
//! `mrsm-v5-test-mosquitto` container); the Python worker uses `python3` with the repository on
//! `PYTHONPATH`. Everything runs in a temporary root with its own plugins directory.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use meerschaum_connector::DataQuery;
use meerschaum_core::{Pipe, Value};
use meerschaum_sql::{
    SqlConnector,
    flavor::Flavor,
    uri::{ConnectionSpec, ConnectorAttributes},
};

const MESSAGES: usize = 6;

fn broker() -> Option<(String, u16)> {
    let raw = std::env::var("MRSM_TEST_MQTT_URI").ok()?;
    let raw = if raw.is_empty() {
        "mqtt://127.0.0.1:1893".to_string()
    } else {
        raw
    };
    let rest = raw.split_once("://").map_or(raw.as_str(), |(_, r)| r);
    let (host, port) = rest.trim_end_matches('/').rsplit_once(':')?;
    Some((host.to_string(), port.parse().ok()?))
}

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn python_ready() -> bool {
    Command::new("python3")
        .args(["-c", "import meerschaum, pandas"])
        .env("PYTHONPATH", repo())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn remaining_length(mut n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            return out;
        }
    }
}

fn packet(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![kind];
    out.extend(remaining_length(body.len()));
    out.extend_from_slice(body);
    out
}

fn mqtt_string(s: &str) -> Vec<u8> {
    let mut out = (s.len() as u16).to_be_bytes().to_vec();
    out.extend_from_slice(s.as_bytes());
    out
}

/// Publish `payloads` to `topic` at QoS 0 (MQTT 3.1.1, clean session).
fn publish(addr: &(String, u16), topic: &str, payloads: &[String]) {
    let mut stream = TcpStream::connect((addr.0.as_str(), addr.1)).unwrap();
    let mut connect = mqtt_string("MQTT");
    connect.extend([4, 0x02, 0, 60]);
    connect.extend(mqtt_string(&format!("w9pub-{}", std::process::id())));
    stream.write_all(&packet(0x10, &connect)).unwrap();
    let mut connack = [0u8; 4];
    stream.read_exact(&mut connack).unwrap();
    assert_eq!(connack[0] >> 4, 2, "CONNACK");
    for payload in payloads {
        let mut body = mqtt_string(topic);
        body.extend_from_slice(payload.as_bytes());
        stream.write_all(&packet(0x30, &body)).unwrap();
    }
    stream.write_all(&[0xE0, 0]).unwrap();
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Stop the listener (SIGINT, then SIGKILL) and every worker it started on this root.
fn stop(child: &mut Child, root: &Path) {
    let _ = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status();
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
    let needle = format!("MRSM_ROOT_DIR={}", root.display());
    if let Ok(entries) = std::fs::read_dir("/proc") {
        for entry in entries.flatten() {
            let pid = entry.file_name().to_string_lossy().into_owned();
            if !pid.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }
            let Ok(environ) = std::fs::read(entry.path().join("environ")) else {
                continue;
            };
            if environ
                .split(|b| *b == 0)
                .any(|var| var == needle.as_bytes())
            {
                let _ = Command::new("kill").args(["-KILL", &pid]).status();
            }
        }
    }
}

fn rowcount(rt: &tokio::runtime::Runtime, conn: &SqlConnector, pipe: &Pipe) -> i64 {
    rt.block_on(conn.get_pipe_rowcount(pipe, &DataQuery::default()))
        .ok()
        .flatten()
        .unwrap_or(0)
}

#[test]
fn listen_runs_a_v4_pattern_connector_and_lands_every_message() {
    let Some(addr) = broker() else {
        eprintln!("skipped: MRSM_TEST_MQTT_URI is not set");
        return;
    };
    if !python_ready() {
        eprintln!("skipped: python3 cannot import meerschaum from the repository");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    let plugins = tmp.path().join("plugins");
    std::fs::create_dir_all(root.join("sqlite")).unwrap();
    copy_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/w9_legacy_mqtt"),
        &plugins,
    );
    let hook_log = tmp.path().join("hooks.jsonl");
    let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let topic = format!("w9/legacy/{tag}");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let attrs = ConnectorAttributes {
        flavor: Some(Flavor::Sqlite),
        database: Some(
            root.join("sqlite/mrsm_local.db")
                .to_string_lossy()
                .into_owned(),
        ),
        ..Default::default()
    };
    let conn = rt
        .block_on(SqlConnector::connect(
            "local",
            ConnectionSpec::from_attributes(&attrs).unwrap(),
        ))
        .unwrap();
    let mut pipe = Pipe::builder("w9mqtt:test", "readings")
        .instance("sql:local")
        .build()
        .unwrap();
    pipe.set_parameters(
        Value::from_json_str(
            &serde_json::json!({
                "fetch": {"topic": topic},
                "columns": {"datetime": "ts", "id": "n"},
                "dtypes": {"ts": "datetime", "n": "int", "v": "float"},
            })
            .to_string(),
        )
        .unwrap()
        .to_map(),
    );
    assert!(rt.block_on(conn.register_pipe(&pipe)).success);

    let log_path = tmp.path().join("listen.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm"));
    for (key, _) in std::env::vars() {
        if key.starts_with("MRSM_") || matches!(key.as_str(), "RUST_LOG" | "NO_COLOR" | "TERM") {
            command.env_remove(key);
        }
    }
    let mut child = command
        .env("MRSM_ROOT_DIR", &root)
        .env("MRSM_PLUGINS_DIR", &plugins)
        .env("MRSM_NOASK", "true")
        .env("MRSM_PYTHON", "python3")
        // The worker's `pipe.sync(docs)` takes the native path (the 5.0 default once the gate
        // goes), and a missing package fails loudly instead of installing inside the deadline.
        .env("MRSM_CORE", "rust")
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("PYTHONPATH", repo())
        .env("W9_HOOK_LOG", &hook_log)
        .env(
            "MRSM_W9MQTT_TEST",
            serde_json::json!({"host": addr.0, "port": addr.1}).to_string(),
        )
        .env(
            "MRSM_CONFIG",
            r#"{"meerschaum":{"instance":"sql:local"},"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false}}"#,
        )
        .args([
            "sync",
            "pipes",
            "-c",
            "w9mqtt:test",
            "-i",
            "sql:local",
            "--listen",
        ])
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();

    let read_log = || std::fs::read_to_string(&log_path).unwrap_or_default();
    let deadline = Instant::now() + Duration::from_secs(120);
    while !read_log().contains("Listening for new data") && Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let started = read_log();
    if !started.contains("Listening for new data") {
        stop(&mut child, &root);
        panic!("the listener never started:\n{started}");
    }

    let payloads: Vec<String> = (0..MESSAGES)
        .map(|n| {
            serde_json::json!({"ts": format!("2026-01-01 00:0{n}:00"), "n": n, "v": n as f64 / 2.0})
                .to_string()
        })
        .collect();
    publish(&addr, &topic, &payloads);
    let deadline = Instant::now() + Duration::from_secs(60);
    while rowcount(&rt, &conn, &pipe) < MESSAGES as i64 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(250));
    }
    let rows = rowcount(&rt, &conn, &pipe);
    stop(&mut child, &root);
    let output = read_log();
    assert_eq!(rows, MESSAGES as i64, "rows landed:\n{output}");

    // v4's lap report of the connector's own `sync()`, then the listener.
    assert!(!output.contains("No data were fetched"), "{output}");
    let finished = output.find("Finished syncing").expect(&output);
    assert!(
        output[finished..].contains("in a background thread."),
        "{output}"
    );
    assert!(finished < output.find("Listening for new data").unwrap());

    // The post-sync hook fired once, for the `sync()` call, with v4's keyword arguments.
    let hooks: Vec<serde_json::Value> = std::fs::read_to_string(&hook_log)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(hooks.len(), 1, "{hooks:?}\n{output}");
    assert_eq!(hooks[0]["success"], true);
    assert!(
        hooks[0]["message"]
            .as_str()
            .unwrap()
            .ends_with("in a background thread."),
        "{hooks:?}"
    );
    assert_eq!(
        hooks[0]["present"],
        serde_json::json!([
            "success_tuple",
            "sync_complete_timestamp",
            "sync_duration",
            "sync_timestamp"
        ])
    );
}
