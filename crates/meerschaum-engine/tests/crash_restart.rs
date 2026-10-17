//! Crash injection with a real process death and a restart (§14, wave 9), on SQLite in a
//! temporary directory and on PostgreSQL-family (`MRSM_TEST_PG_URI`, schema `w9_engine`).
//!
//! `tests/crash.rs` fails a node in process (the executor sees the error and records the run).
//! Here the engine runs in a child process (this test binary again, `crash_child`) whose fault
//! injector calls `std::process::exit` at one of the two points the write ordering (§5.8)
//! protects, so nothing after it runs: no lease release, no run record, no event flush, no
//! rollback code. The parent then "restarts" (a fresh engine over the same instance) and
//! proves that
//!
//! 1. **between the ledger write and the data commit** (`after_ledger_write` on the root): the
//!    ledger holds the interrupted run's entry, the root's table and every cursor are
//!    untouched, and the interrupted run left no run record;
//! 2. **between the data commit and the cursor advance** (`before_cursor_advance` on the
//!    middle node): the node's rows are committed, its cursor on the parent did not move;
//!
//! and in both cases the next run converges on exactly the expected tables (no row lost, none
//! duplicated), consumes the ledger, and a third run has nothing left to do.
//!
//! `MRSM_TEST_PG_URI` gates the PostgreSQL variants; an empty value selects the disposable
//! test container (`postgresql://test:test1234@localhost:5559/testdb`). Every PostgreSQL table
//! lives in schema `w9_engine` with a per-run suffix, and is dropped at the end.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use common::{json_map, norm_kvalue, temp_dir};
use meerschaum_connector::pipe_ops::frame_to_docs;
use meerschaum_connector::{DataQuery, InstanceConnector};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::Pipe;
use meerschaum_engine::{
    Depth, Engine, EngineConfig, FaultInjector, NodeStatus, PipeRef, RunOptions, RunResult,
    Selection, StateSource, StaticRegistry,
};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

const KEYS: &str = "sql:w9crash";
const LABEL: &str = "w9crash";
const SCHEMA: &str = "w9_engine";
const CHILD_ENV: &str = "MRSM_W9_CRASH_CHILD";
const CHILD_EXIT: i32 = 86;

/// Where a crash happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Point {
    /// After the root's ledger entry is written, before its data commit.
    AfterLedgerWrite,
    /// After the middle node's data commit, before its cursor advance.
    BeforeCursorAdvance,
}

impl Point {
    fn name(self) -> &'static str {
        match self {
            Point::AfterLedgerWrite => "after_ledger_write",
            Point::BeforeCursorAdvance => "before_cursor_advance",
        }
    }

    fn parse(s: &str) -> Point {
        match s {
            "after_ledger_write" => Point::AfterLedgerWrite,
            "before_cursor_advance" => Point::BeforeCursorAdvance,
            other => panic!("unknown crash point {other}"),
        }
    }
}

/// One backend: how to connect (the child reconnects from the same description).
#[derive(Debug, Clone)]
struct Backend {
    kind: &'static str,
    target: String,
}

fn pg_uri() -> Option<String> {
    let raw = std::env::var("MRSM_TEST_PG_URI").ok()?;
    let base = if raw.is_empty() {
        "postgresql://test:test1234@localhost:5559/testdb".to_string()
    } else {
        raw
    };
    let sep = if base.contains('?') { '&' } else { '?' };
    Some(format!("{base}{sep}schema={SCHEMA}"))
}

async fn connect(backend: &Backend) -> Arc<SqlConnector> {
    let uri = match backend.kind {
        "sqlite" => format!("sqlite:///{}", backend.target),
        _ => backend.target.clone(),
    };
    Arc::new(
        SqlConnector::from_uri(LABEL, &uri)
            .await
            .unwrap_or_else(|e| panic!("connect to {}: {e}", backend.kind)),
    )
}

fn registry(conn: &Arc<SqlConnector>) -> Arc<StaticRegistry> {
    let instance: Arc<dyn InstanceConnector> = conn.clone();
    let source: Arc<dyn meerschaum_connector::Connector> = conn.clone();
    let sql = conn.clone();
    Arc::new(
        StaticRegistry::new()
            .with_instance(KEYS, instance)
            .with_source(KEYS, source)
            .with_table_reference(move |pipe: &Pipe| {
                let sp = sql.sql_pipe(pipe).map_err(|e| e.to_string())?;
                Ok(sql.pipe_table_name(&sp))
            }),
    )
}

fn config(dir: &Path) -> EngineConfig {
    EngineConfig {
        home_instance: KEYS.into(),
        lease_dir: dir.join("leases"),
        // A crashed holder's lease row blocks the restart only until its TTL.
        lease_ttl: Duration::from_secs(3),
        lease_wait: Duration::from_secs(30),
        lease_poll: Duration::from_millis(50),
        state_flush: Duration::from_millis(20),
        holder: format!("w9crash:{}", std::process::id()),
        ..EngineConfig::default()
    }
}

/// The chain `a_raw → b_clean → c_high`, named with a per-run suffix.
#[derive(Debug, Clone)]
struct Chain {
    tag: String,
    flavor: &'static str,
}

impl Chain {
    fn metric(&self, base: &str) -> String {
        format!("{base}_{}", self.tag)
    }

    fn src(&self) -> String {
        match self.flavor {
            "sqlite" => format!("w9_src_{}", self.tag),
            _ => format!("\"{SCHEMA}\".\"w9_src_{}\"", self.tag),
        }
    }

    fn pipe_ref(&self, base: &str) -> PipeRef {
        PipeRef::new(KEYS, KEYS, &self.metric(base), None)
    }

    fn pipe(&self, base: &str) -> Pipe {
        Pipe::builder(KEYS, &self.metric(base))
            .instance(KEYS)
            .build()
            .expect("valid pipe")
    }

    fn parameters(&self, base: &str) -> serde_json::Value {
        let sql = match base {
            "a_raw" => format!("SELECT ts, sensor_id, value FROM {}", self.src()),
            "b_clean" => format!(
                "SELECT ts, sensor_id, value FROM {{{{ Pipe('{KEYS}', '{}', instance='{KEYS}') }}}} \
                 WHERE value >= 0",
                self.metric("a_raw")
            ),
            "c_high" => format!(
                "SELECT ts, sensor_id, value FROM {{{{ Pipe('{KEYS}', '{}', instance='{KEYS}') }}}} \
                 WHERE value >= 5",
                self.metric("b_clean")
            ),
            other => panic!("unknown node {other}"),
        };
        serde_json::json!({
            "sql": sql,
            "columns": {"datetime": "ts", "id": "sensor_id"},
            "dtypes": {"ts": "datetime", "sensor_id": "int", "value": "float"},
            // Several windows per graph sync: the cursor crash happens after all of them.
            "verify": {"chunk_minutes": 120},
        })
    }

    fn options(&self, run_id: Option<String>) -> RunOptions {
        RunOptions {
            selection: Some(
                Selection::refs([self.pipe_ref("c_high")]).with_parents(Some(Depth::All)),
            ),
            run_id,
            ..Default::default()
        }
    }
}

const NODES: [&str; 3] = ["a_raw", "b_clean", "c_high"];

/// `(minute offset from 2026-01-01 00:00, sensor, value)`.
fn phase_rows(phase: usize) -> Vec<(i64, i64, f64)> {
    let grid = |from: i64, to: i64| {
        (from..to)
            .flat_map(|i| (1..=2).map(move |s| (i * 15, s, (((i * 7 + s * 5) % 23) - 3) as f64)))
    };
    match phase {
        0 => grid(0, 24).collect(),
        // New rows after the watermark, plus a late row inside the backtrack window.
        _ => grid(24, 40).chain([(125, 3, 9.0)]).collect(),
    }
}

fn ts(minute: i64) -> chrono::NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        + chrono::TimeDelta::minutes(minute)
}

fn insert_sql(chain: &Chain, rows: &[(i64, i64, f64)]) -> String {
    let values: Vec<String> = rows
        .iter()
        .map(|(m, s, v)| format!("('{}', {s}, {v:.1})", ts(*m).format("%Y-%m-%d %H:%M:%S")))
        .collect();
    format!(
        "INSERT INTO {} (ts, sensor_id, value) VALUES {}",
        chain.src(),
        values.join(", ")
    )
}

/// The rows a node must hold after `phase`, rendered like [`table`].
fn expected(node: &str, phase: usize) -> Vec<String> {
    let min = match node {
        "a_raw" => f64::NEG_INFINITY,
        "b_clean" => 0.0,
        _ => 5.0,
    };
    let mut rows: Vec<String> = (0..=phase)
        .flat_map(phase_rows)
        .filter(|(_, _, v)| *v >= min)
        .map(|(m, s, v)| {
            [
                norm_kvalue(&KValue::Int(s)),
                norm_kvalue(&KValue::Datetime(DateTimeValue::utc(ts(m)))),
                norm_kvalue(&KValue::Float(v)),
            ]
            .join(" | ")
        })
        .collect();
    rows.sort();
    rows
}

/// A node's table as sorted `sensor_id | ts | value` lines (duplicates kept).
async fn table(conn: &SqlConnector, chain: &Chain, node: &str) -> Vec<String> {
    let df = conn
        .get_pipe_data(&chain.pipe(node), &DataQuery::default())
        .await
        .expect("read pipe data");
    let Some(df) = df else { return Vec::new() };
    let mut rows: Vec<String> = frame_to_docs(&df)
        .iter()
        .map(|d| {
            ["sensor_id", "ts", "value"]
                .iter()
                .map(|c| norm_kvalue(d.get(*c).unwrap_or(&KValue::Null)))
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect();
    rows.sort();
    rows
}

fn status(result: &RunResult, chain: &Chain, node: &str) -> NodeStatus {
    result
        .record
        .as_ref()
        .expect("a run record")
        .nodes
        .iter()
        .find(|n| n.pipe_ref == chain.pipe_ref(node))
        .unwrap_or_else(|| panic!("{node} in the run"))
        .status
}

/// Ends the process at the configured point (a crash: no destructor, no cleanup runs).
struct ExitAt {
    point: Point,
    node: PipeRef,
}

impl ExitAt {
    fn exit(&self, point: Point, node: &PipeRef) -> Result<(), String> {
        if point == self.point && *node == self.node {
            eprintln!(
                "crash_child: exiting at {} of {}",
                point.name(),
                node.label()
            );
            std::process::exit(CHILD_EXIT);
        }
        Ok(())
    }
}

impl FaultInjector for ExitAt {
    fn after_ledger_write(&self, node: &PipeRef) -> Result<(), String> {
        self.exit(Point::AfterLedgerWrite, node)
    }

    fn before_cursor_advance(&self, node: &PipeRef) -> Result<(), String> {
        self.exit(Point::BeforeCursorAdvance, node)
    }
}

/// The child process: run the chain with the crash injected. Only runs when the parent sets
/// `MRSM_W9_CRASH_CHILD`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "started by the crash tests as a child process"]
async fn crash_child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let spec: serde_json::Value = serde_json::from_str(&spec).expect("child spec");
    let s = |k: &str| spec[k].as_str().expect(k).to_string();
    let backend = Backend {
        kind: if s("kind") == "sqlite" {
            "sqlite"
        } else {
            "postgresql"
        },
        target: s("target"),
    };
    let chain = Chain {
        tag: s("tag"),
        flavor: backend.kind,
    };
    let point = Point::parse(&s("point"));
    let node = match point {
        Point::AfterLedgerWrite => chain.pipe_ref("a_raw"),
        Point::BeforeCursorAdvance => chain.pipe_ref("b_clean"),
    };
    let conn = connect(&backend).await;
    let engine = Engine::new(registry(&conn), config(Path::new(&s("dir"))))
        .with_faults(Arc::new(ExitAt { point, node }));
    let result = engine.run(chain.options(Some(s("run_id")))).await;
    // Reaching this line means the crash point never fired.
    panic!("the run finished without crashing: {}", result.message);
}

fn spawn_child(backend: &Backend, chain: &Chain, point: Point, dir: &Path, run_id: &str) {
    let root = dir.join("root");
    let plugins = dir.join("plugins");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    let spec = serde_json::json!({
        "kind": backend.kind,
        "target": backend.target,
        "tag": chain.tag,
        "point": point.name(),
        "dir": dir,
        "run_id": run_id,
    });
    let out = Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "crash_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, spec.to_string())
        .env("MRSM_ROOT_DIR", &root)
        .env("MRSM_PLUGINS_DIR", &plugins)
        .output()
        .expect("start the child");
    assert_eq!(
        out.status.code(),
        Some(CHILD_EXIT),
        "the child did not crash at {}:\nstdout={}\nstderr={}",
        point.name(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

async fn scenario(backend: Backend, point: Point) {
    let dir = temp_dir(&format!("w9crash_{}", point.name()));
    let chain = Chain {
        tag: uuid::Uuid::new_v4().simple().to_string()[..8].to_string(),
        flavor: backend.kind,
    };
    let conn = connect(&backend).await;
    if backend.kind != "sqlite" {
        // Concurrent creation can fail inside the server; the CREATE TABLE below verifies it.
        let _ = conn
            .exec(&format!("CREATE SCHEMA IF NOT EXISTS \"{SCHEMA}\""))
            .await;
    }
    let ts_type = if backend.kind == "sqlite" {
        "TEXT"
    } else {
        "TIMESTAMP"
    };
    assert!(
        conn.exec(&format!(
            "CREATE TABLE {} (ts {ts_type}, sensor_id INTEGER, value DOUBLE PRECISION)",
            chain.src()
        ))
        .await
        .is_some()
    );
    for node in NODES {
        let pipe = Pipe::builder(KEYS, &chain.metric(node))
            .instance(KEYS)
            .parameters(json_map(&chain.parameters(node)))
            .build()
            .unwrap();
        let res = conn.register_pipe(&pipe).await;
        assert!(res.success, "register {node}: {}", res.message);
    }
    let engine = || Engine::new(registry(&conn), config(&dir));

    // Phase 0: a clean run.
    assert!(
        conn.exec(&insert_sql(&chain, &phase_rows(0)))
            .await
            .is_some()
    );
    let first = engine().run(chain.options(None)).await;
    assert!(first.success, "{}\n{:#?}", first.message, first.outcomes);
    for node in NODES {
        assert_eq!(
            table(&conn, &chain, node).await,
            expected(node, 0),
            "{node}"
        );
    }
    let (a, b, c) = (
        chain.pipe_ref("a_raw"),
        chain.pipe_ref("b_clean"),
        chain.pipe_ref("c_high"),
    );
    let state = engine().state().clone();
    let seq_before = state.latest_seq(KEYS, &a).await.unwrap();
    let cursor_ab = state
        .cursor(KEYS, &a, &b)
        .await
        .unwrap()
        .expect("a → b cursor");
    let cursor_bc = state
        .cursor(KEYS, &b, &c)
        .await
        .unwrap()
        .expect("b → c cursor");

    // Phase 1, crashed.
    assert!(
        conn.exec(&insert_sql(&chain, &phase_rows(1)))
            .await
            .is_some()
    );
    let run_id = meerschaum_engine::runs::new_run_id();
    spawn_child(&backend, &chain, point, &dir, &run_id);

    // After the restart.
    let state = engine().state().clone();
    assert!(
        state.load_run(&run_id).await.unwrap().is_none(),
        "an interrupted run leaves no run record"
    );
    assert_eq!(
        state.cursor(KEYS, &b, &c).await.unwrap(),
        Some(cursor_bc.clone())
    );
    match point {
        Point::AfterLedgerWrite => {
            let seq_after = state.latest_seq(KEYS, &a).await.unwrap();
            assert!(seq_after > seq_before, "{seq_before:?} → {seq_after:?}");
            let orphan = state
                .ledger_after(KEYS, &a, seq_before.unwrap_or(0))
                .await
                .unwrap();
            assert!(!orphan.is_empty(), "the interrupted run's ledger entry");
            for node in NODES {
                assert_eq!(
                    table(&conn, &chain, node).await,
                    expected(node, 0),
                    "{node}: nothing committed"
                );
            }
            assert_eq!(
                state.cursor(KEYS, &a, &b).await.unwrap(),
                Some(cursor_ab.clone())
            );
        }
        Point::BeforeCursorAdvance => {
            assert_eq!(table(&conn, &chain, "a_raw").await, expected("a_raw", 1));
            assert_eq!(
                table(&conn, &chain, "b_clean").await,
                expected("b_clean", 1),
                "b_clean committed every window before the crash"
            );
            assert_eq!(table(&conn, &chain, "c_high").await, expected("c_high", 0));
            let cursor = state.cursor(KEYS, &a, &b).await.unwrap().unwrap();
            assert_eq!(cursor.seq, cursor_ab.seq, "the cursor did not advance");
            assert!(
                state.latest_seq(KEYS, &a).await.unwrap() > Some(cursor.seq),
                "a_raw's phase-1 entries are unconsumed"
            );
        }
    }

    // The re-run converges: every row exactly once.
    let next = engine().run(chain.options(None)).await;
    assert!(next.success, "{}\n{:#?}", next.message, next.outcomes);
    assert_eq!(status(&next, &chain, "b_clean"), NodeStatus::Succeeded);
    assert_eq!(status(&next, &chain, "c_high"), NodeStatus::Succeeded);
    for node in NODES {
        assert_eq!(
            table(&conn, &chain, node).await,
            expected(node, 1),
            "{node}"
        );
    }
    let latest = state.latest_seq(KEYS, &a).await.unwrap().unwrap();
    let cursor = state.cursor(KEYS, &a, &b).await.unwrap().unwrap();
    assert!(
        cursor.seq >= latest,
        "cursor {} < latest {latest}",
        cursor.seq
    );

    // Nothing is left to do.
    let last = engine().run(chain.options(None)).await;
    assert!(last.success, "{}\n{:#?}", last.message, last.outcomes);
    assert_eq!(status(&last, &chain, "b_clean"), NodeStatus::Skipped);
    assert_eq!(status(&last, &chain, "c_high"), NodeStatus::Skipped);
    for node in NODES {
        assert_eq!(
            table(&conn, &chain, node).await,
            expected(node, 1),
            "{node}"
        );
    }

    if backend.kind != "sqlite" {
        for node in NODES {
            let pipe = chain.pipe(node);
            let _ = conn.drop_pipe(&pipe).await;
            let _ = conn.delete_pipe(&pipe).await;
        }
        let _ = conn
            .exec(&format!("DROP TABLE IF EXISTS {}", chain.src()))
            .await;
    }
    if std::env::var_os("MRSM_KEEP_TEST_DB").is_none() {
        let _ = std::fs::remove_dir_all(&dir);
        if backend.kind == "sqlite" {
            if let Some(db_dir) = Path::new(&backend.target).parent() {
                let _ = std::fs::remove_dir_all(db_dir);
            }
        }
    }
}

fn sqlite(tag: &str) -> Backend {
    let dir: PathBuf = temp_dir(tag);
    Backend {
        kind: "sqlite",
        target: dir.join("w9crash.db").to_string_lossy().into_owned(),
    }
}

fn postgresql() -> Option<Backend> {
    Some(Backend {
        kind: "postgresql",
        target: pg_uri()?,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_crash_between_ledger_write_and_data_commit() {
    scenario(sqlite("w9crash_db_ledger"), Point::AfterLedgerWrite).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_crash_between_data_commit_and_cursor_advance() {
    scenario(sqlite("w9crash_db_cursor"), Point::BeforeCursorAdvance).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgresql_crash_between_ledger_write_and_data_commit() {
    let Some(backend) = postgresql() else {
        eprintln!("skipped: MRSM_TEST_PG_URI is not set");
        return;
    };
    scenario(backend, Point::AfterLedgerWrite).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgresql_crash_between_data_commit_and_cursor_advance() {
    let Some(backend) = postgresql() else {
        eprintln!("skipped: MRSM_TEST_PG_URI is not set");
        return;
    };
    scenario(backend, Point::BeforeCursorAdvance).await;
}
