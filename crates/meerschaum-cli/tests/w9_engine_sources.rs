//! The engine registry builds only the connectors a selection can reach (wave 9): a pipe whose
//! source connector is unrelated to the selection is never connected, not even to build the
//! registry. The unrelated connector here points at a local TCP listener that records every
//! connection attempt; a graph run, a flat run, `plan pipes`, `show graph` and `show runs`
//! over other pipes must leave it untouched.
//!
//! Every command runs the `mrsm` binary with `MRSM_ROOT_DIR` and an empty `MRSM_PLUGINS_DIR`
//! inside a temporary directory; nothing touches the default root.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use meerschaum_connector_sql::{
    SqlConnector,
    flavor::Flavor,
    uri::{ConnectionSpec, ConnectorAttributes},
};
use meerschaum_core::{Pipe, Value};

/// A listener that counts connection attempts (and closes them at once).
struct Tripwire {
    port: u16,
    hits: Arc<AtomicUsize>,
}

impl Tripwire {
    fn start() -> Tripwire {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stream.is_ok() {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        Tripwire { port, hits }
    }

    fn hits(&self) -> usize {
        // Let a late connection attempt land before reading the count.
        std::thread::sleep(Duration::from_millis(200));
        self.hits.load(Ordering::SeqCst)
    }
}

struct Env {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    plugins: PathBuf,
    config: String,
}

impl Env {
    fn run(&self, args: &[&str]) -> Output {
        run_in(&self.root, &self.plugins, &self.config, args)
    }

    fn ok(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(
            out.status.success(),
            "{args:?}\nstdout={}\nstderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
}

fn run_in(root: &Path, plugins: &Path, config: &str, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    for (key, _) in std::env::vars() {
        if key.starts_with("MRSM_") || matches!(key.as_str(), "RUST_LOG" | "NO_COLOR" | "TERM") {
            command.env_remove(key);
        }
    }
    command
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_PLUGINS_DIR", plugins)
        .env("MRSM_NOASK", "true")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .env("MRSM_CONFIG", config)
        .args(args)
        .output()
        .unwrap()
}

fn pipe(connector: &str, metric: &str, sql: &str) -> Pipe {
    let mut pipe = Pipe::builder(connector, metric)
        .instance("sql:local")
        .build()
        .unwrap();
    pipe.set_parameters(
        Value::from_json_str(
            &serde_json::json!({
                "columns": {"datetime": "ts", "id": "site"},
                "dtypes": {"ts": "datetime"},
                "sql": sql,
            })
            .to_string(),
        )
        .unwrap()
        .to_map(),
    );
    pipe
}

/// A SQLite instance with the chain `raw -> clean -> daily` from `sql:local`, plus `elsewhere`,
/// whose source `sql:unreachable` is the tripwire.
fn setup(tripwire: &Tripwire) -> Env {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    let plugins = tmp.path().join("plugins");
    std::fs::create_dir_all(root.join("sqlite")).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let attrs = ConnectorAttributes {
        flavor: Some(Flavor::Sqlite),
        database: Some(
            root.join("sqlite/mrsm_local.db")
                .to_string_lossy()
                .into_owned(),
        ),
        ..Default::default()
    };
    let connector = runtime
        .block_on(SqlConnector::connect(
            "local",
            ConnectionSpec::from_attributes(&attrs).unwrap(),
        ))
        .unwrap();
    let queries = [
        "CREATE TABLE readings (ts TEXT, site TEXT, temp REAL)".to_string(),
        "INSERT INTO readings VALUES ('2024-01-01 00:00:00', 'a', 1.0), \
         ('2024-01-02 00:00:00', 'b', 3.0)"
            .to_string(),
    ];
    assert!(
        runtime
            .block_on(connector.exec_queries(&queries, true, true))
            .iter()
            .all(Option::is_some)
    );
    let raw = pipe("sql:local", "raw", "SELECT * FROM readings");
    let clean = pipe(
        "sql:local",
        "clean",
        "SELECT ts, site, temp FROM {{ Pipe('sql:local', 'raw', instance='sql:local') }}",
    );
    let daily = pipe(
        "sql:local",
        "daily",
        "SELECT ts, site, temp FROM {{ Pipe('sql:local', 'clean', instance='sql:local') }}",
    );
    let elsewhere = pipe("sql:unreachable", "elsewhere", "SELECT 1 AS ts");
    for p in [&raw, &clean, &daily, &elsewhere] {
        assert!(runtime.block_on(connector.register_pipe(p)).success);
    }
    let config = serde_json::json!({
        "meerschaum": {
            "instance": "sql:local",
            "connectors": {"sql": {"unreachable": {
                "flavor": "postgresql",
                "host": "127.0.0.1",
                "port": tripwire.port,
                "username": "nobody",
                "password": "nothing",
                "database": "nowhere",
            }}},
        },
        "shell": {"updates": {"check": false}},
        "formatting": {"ansi": false, "unicode": false},
    })
    .to_string();
    Env {
        _tmp: tmp,
        root,
        plugins,
        config,
    }
}

#[test]
fn unrelated_sources_are_never_connected() {
    let tripwire = Tripwire::start();
    let env = setup(&tripwire);

    let out = env.ok(&["sync", "pipes", "-m", "daily", "--parents", "--yes"]);
    assert!(out.contains("critical path"), "{out}");
    let rows = env.ok(&["show", "rowcounts", "-m", "daily", "--nopretty"]);
    assert!(rows.contains('2'), "{rows}");
    env.ok(&["sync", "pipes", "-m", "raw", "-m", "clean", "--yes"]);
    env.ok(&["sync", "pipes", "-m", "raw", "--children", "1", "--yes"]);
    env.ok(&["plan", "pipes", "-m", "daily", "--parents"]);
    env.ok(&["show", "graph", "-m", "daily", "--parents"]);
    env.ok(&["show", "runs"]);
    assert_eq!(
        tripwire.hits(),
        0,
        "a connector of a pipe outside the selection was contacted"
    );

    // The tripwire works: selecting the unrelated pipe does contact its source.
    let _ = env.run(&["sync", "pipes", "-m", "elsewhere", "--yes"]);
    assert!(tripwire.hits() > 0, "the tripwire saw no connection");
}
