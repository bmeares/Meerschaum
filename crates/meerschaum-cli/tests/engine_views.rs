//! `plan pipes`, `show graph`, `show runs` / `show run`, the Gantt chart of graph runs, v5-only
//! help, and native plugins in `show plugins`, through the `mrsm` binary over a temporary root
//! with a SQLite instance. Nothing here touches the default root: every command sets
//! `MRSM_ROOT_DIR` and an empty `MRSM_PLUGINS_DIR` inside a temporary directory.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use meerschaum_connector_sql::{
    SqlConnector,
    flavor::Flavor,
    uri::{ConnectionSpec, ConnectorAttributes},
};
use meerschaum_core::{Pipe, Value};

struct Env {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    plugins: PathBuf,
}

impl Env {
    fn run(&self, args: &[&str]) -> Output {
        run_in(&self.root, &self.plugins, args)
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

fn run_in(root: &Path, plugins: &Path, args: &[&str]) -> Output {
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
        .env(
            "MRSM_CONFIG",
            r#"{"meerschaum":{"instance":"sql:local"},"shell":{"updates":{"check":false}},"formatting":{"ansi":false,"unicode":false}}"#,
        )
        .args(args)
        .output()
        .unwrap()
}

fn pipe(metric: &str, sql: &str) -> Pipe {
    let mut pipe = Pipe::builder("sql:local", metric)
        .instance("sql:local")
        .build()
        .unwrap();
    pipe.set_parameters(
        Value::from_json_str(
            &serde_json::json!({
                "columns": {"datetime": "ts", "id": "site"},
                "dtypes": {"ts": "datetime"},
                "sql": sql,
                "tags": ["demo"],
            })
            .to_string(),
        )
        .unwrap()
        .to_map(),
    );
    pipe
}

/// A SQLite instance with `readings` and the chain `raw -> clean -> daily`.
fn chain() -> Env {
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
         ('2024-01-01 06:00:00', 'a', 2.0), ('2024-01-02 00:00:00', 'b', 3.0)"
            .to_string(),
    ];
    assert!(
        runtime
            .block_on(connector.exec_queries(&queries, true, true))
            .iter()
            .all(Option::is_some)
    );
    let raw = pipe("raw", "SELECT * FROM readings");
    let clean = pipe(
        "clean",
        "SELECT ts, site, temp FROM {{ Pipe('sql:local', 'raw', instance='sql:local') }} \
         WHERE temp IS NOT NULL",
    );
    let daily = pipe(
        "daily",
        "SELECT date(ts) AS ts, site, AVG(temp) AS avg_temp \
         FROM {{ Pipe('sql:local', 'clean', instance='sql:local') }} GROUP BY 1, 2",
    );
    for p in [&raw, &clean, &daily] {
        assert!(runtime.block_on(connector.register_pipe(p)).success);
    }
    Env {
        _tmp: tmp,
        root,
        plugins,
    }
}

#[test]
fn plan_pipes_prints_the_plan_without_syncing() {
    let env = chain();
    let out = env.ok(&[
        "plan",
        "pipes",
        "-m",
        "daily",
        "--parents",
        "-i",
        "sql:local",
    ]);
    eprintln!("{out}");
    assert!(out.starts_with("node"), "{out}");
    for metric in ["raw", "clean", "daily"] {
        assert!(
            out.contains(&format!("Pipe('sql:local', '{metric}')")),
            "{out}"
        );
    }
    assert!(out.contains("root"), "{out}");
    assert!(out.contains("Nothing was synced."), "{out}");
    // Nothing was synced: no target tables, no run history.
    let rows = env.ok(&[
        "show",
        "rowcounts",
        "-m",
        "raw",
        "--nopretty",
        "-i",
        "sql:local",
    ]);
    assert!(!rows.contains("3"), "plan must not sync: {rows}");
    let runs = env.ok(&["show", "runs", "--nopretty"]);
    assert_eq!(runs.trim(), "[]", "{runs}");
}

#[test]
fn plan_pipes_json_is_a_plan() {
    let env = chain();
    let out = env.ok(&["plan", "pipes", "-t", "demo", "--parents", "--nopretty"]);
    let plan: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(plan["nodes"].as_array().unwrap().len(), 3, "{out}");
    assert_eq!(plan["flat"], false, "{out}");
    let actions: Vec<_> = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["action"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(actions[0], "root", "{actions:?}");
}

#[test]
fn plan_pipes_respects_parents_depth_and_reports_no_pipes() {
    let env = chain();
    let one = env.ok(&[
        "plan",
        "pipes",
        "-m",
        "daily",
        "--parents",
        "1",
        "--nopretty",
    ]);
    let plan: serde_json::Value = serde_json::from_str(one.trim()).unwrap();
    assert_eq!(plan["nodes"].as_array().unwrap().len(), 2, "{one}");
    let none = env.run(&["plan", "pipes", "-m", "does_not_exist"]);
    assert!(!none.status.success());
    assert!(String::from_utf8_lossy(&none.stdout).contains("No pipes selected."));
}

#[test]
fn show_graph_draws_the_dag() {
    let env = chain();
    let out = env.ok(&["show", "graph", "-m", "daily", "--parents"]);
    eprintln!("{out}");
    // A piped stdout gets the plain layout: a header, then the tree, then the legend.
    assert!(!out.contains('\u{1b}'), "no colors off a terminal: {out}");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(
        lines[0], "Pipe graph on sql:local: 3 pipes, 2 edges, 1 root, depth 3",
        "{out}"
    );
    let tree: Vec<&str> = lines[2..5].to_vec();
    assert!(tree[0].ends_with("Pipe('sql:local', 'raw')"), "{out}");
    assert!(tree[1].ends_with("Pipe('sql:local', 'clean')"), "{out}");
    // The chain is built from `{{ Pipe() }}` references, so the edges are drawn dashed.
    assert!(
        ["\u{2514}\u{2504}\u{2504} ", "`~~ "]
            .iter()
            .any(|branch| tree[1].starts_with(branch)),
        "{out}"
    );
    assert!(out.contains("implicit {{ Pipe() }} edge"), "{out}");
    assert!(tree[2].contains("Pipe('sql:local', 'daily')"), "{out}");
    assert!(
        tree[2].contains('\u{2605}') || tree[2].contains('*'),
        "the selected pipe is marked: {out}"
    );
    assert!(
        lines[2..].iter().any(|l| l.contains("root")),
        "legend: {out}"
    );
    let json = env.ok(&["show", "graph", "-t", "demo", "--nopretty"]);
    let graph: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
    assert_eq!(graph["nodes"].as_array().unwrap().len(), 3, "{json}");
    assert_eq!(graph["edges"].as_array().unwrap().len(), 2, "{json}");
}

#[test]
fn show_graph_draws_a_cycle_instead_of_flattening_it() {
    let env = chain();
    for (metric, parent) in [("e", "f"), ("f", "e")] {
        let params = format!(r#"{{"parents":[{{"connector":"sql:local","metric":"{parent}"}}]}}"#);
        env.ok(&[
            "register",
            "pipes",
            "-c",
            "sql:local",
            "-m",
            metric,
            "-t",
            "loop",
            "--params",
            &params,
        ]);
    }
    let out = env.ok(&["show", "graph", "-t", "loop"]);
    assert!(out.contains("2 pipes, 2 edges"), "the real edges: {out}");
    assert!(out.contains("cycle: Pipe('sql:local',"), "{out}");
    assert!(!out.contains("Not connected"), "{out}");
    let json = env.ok(&["show", "graph", "-t", "loop", "--nopretty"]);
    let graph: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
    assert_eq!(graph["edges"].as_array().unwrap().len(), 2, "{json}");
    assert_eq!(graph["cycles"].as_array().unwrap().len(), 1, "{json}");
    for node in graph["nodes"].as_array().unwrap() {
        assert_eq!(node["parents"].as_array().unwrap().len(), 1, "{json}");
    }
    // A graph without a cycle reports none.
    let json = env.ok(&["show", "graph", "-t", "demo", "--nopretty"]);
    let graph: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
    assert!(graph["cycles"].as_array().unwrap().is_empty(), "{json}");
}

#[test]
fn graph_sync_ends_with_a_gantt_and_runs_are_listed() {
    let env = chain();
    let out = env.ok(&["sync", "pipes", "-m", "daily", "--parents", "--yes"]);
    eprintln!("{out}");
    assert!(out.contains("critical path"), "{out}");
    assert!(out.contains("\u{2588}") || out.contains("#"), "{out}");
    for metric in ["raw", "clean", "daily"] {
        assert!(out.contains(metric), "{out}");
    }
    let json = env.ok(&["show", "runs", "--nopretty"]);
    let runs: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
    let runs = runs.as_array().unwrap();
    assert_eq!(runs.len(), 1, "{json}");
    assert_eq!(runs[0]["status"], "succeeded", "{json}");
    let run_id = runs[0]["run"]["run_id"].as_str().unwrap().to_string();
    let table = env.ok(&["show", "runs"]);
    eprintln!("{table}");
    assert!(table.contains(&run_id), "{table}");
    assert!(table.contains("succeeded"), "{table}");
    assert!(table.contains("3/3"), "{table}");
    let last_one = env.ok(&["show", "runs", "--last", "1"]);
    assert!(last_one.contains(&run_id), "{last_one}");
    // A first run's 30 s default forecast must not stretch its actual timeline.
    assert!(!out.contains("30s    planned"), "{out}");
    // `show run <id>` draws the same chart.
    let gantt = env.ok(&["show", "run", &run_id]);
    assert!(gantt.starts_with(&format!("run {run_id}")), "{gantt}");
    assert!(gantt.contains("critical path"), "{gantt}");
    assert!(
        !gantt.contains('\u{2592}') && !gantt.contains("planned"),
        "{gantt}"
    );
    let detail = env.ok(&["show", "run", &run_id, "--nopretty"]);
    let detail: serde_json::Value = serde_json::from_str(detail.trim()).unwrap();
    for bar in detail["bars"].as_array().unwrap() {
        assert!(bar["planned_end"].is_string(), "{bar}");
        assert_eq!(bar["planned_duration_seconds"], 30.0, "{bar}");
    }
    let missing = env.run(&["show", "run", "01NOPE"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stdout).contains("No run '01NOPE'"));
}

/// Replace the `columns` of a registered pipe of the chain (what `pipe.edit()` does).
fn edit_columns(env: &Env, metric: &str, columns: serde_json::Value) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let attrs = ConnectorAttributes {
        flavor: Some(Flavor::Sqlite),
        database: Some(
            env.root
                .join("sqlite/mrsm_local.db")
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
    let mut edited = pipe(metric, "SELECT * FROM readings");
    let mut parameters = edited.parameters().clone();
    parameters.insert(
        "columns".into(),
        Value::from_json_str(&columns.to_string()).unwrap(),
    );
    edited.set_parameters(parameters);
    assert!(
        runtime
            .block_on(connector.edit_pipe(&edited, false))
            .success
    );
}

#[test]
fn a_pending_rebuild_is_printed_with_the_way_out_and_never_a_silent_stop() {
    let env = chain();
    env.ok(&["sync", "pipes", "-m", "raw", "--parents"]);
    // A `value` column is not part of the definition: the pipe still syncs.
    edit_columns(
        &env,
        "raw",
        serde_json::json!({"datetime": "ts", "id": "site", "value": "temp"}),
    );
    let out = env.ok(&["sync", "pipes", "-m", "raw", "--parents"]);
    assert!(
        !out.contains("rebuild_pending") && out.contains("Successfully synced pipes"),
        "{out}"
    );
    // A different `id` column is a definition change: nothing syncs, and the run says why.
    edit_columns(
        &env,
        "raw",
        serde_json::json!({"datetime": "ts", "id": "temp"}),
    );
    let run = env.run(&["sync", "pipes", "-m", "raw", "--parents"]);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(!run.status.success(), "{text}");
    assert!(text.contains("NOT synced"), "{text}");
    assert!(text.contains("raw"), "{text}");
    assert!(
        text.contains("--yes") && text.contains("auto_rebuild"),
        "{text}"
    );
    assert!(text.contains("1 not synced: rebuild_pending"), "{text}");
    // `--nopretty` prints one line per pipe on stdout.
    let plain = env.run(&["sync", "pipes", "-m", "raw", "--parents", "--nopretty"]);
    let stdout = String::from_utf8_lossy(&plain.stdout);
    assert!(!plain.status.success(), "{stdout}");
    assert!(stdout.contains("rebuild_pending: "), "{stdout}");
    assert!(stdout.contains("--yes"), "{stdout}");
    // `--yes` approves it.
    let approved = env.ok(&["sync", "pipes", "-m", "raw", "--parents", "--yes"]);
    assert!(
        approved.contains("Successfully synced pipes") && !approved.contains("NOT synced"),
        "{approved}"
    );
}

#[test]
fn flat_sync_output_has_no_gantt() {
    let env = chain();
    let out = env.ok(&["sync", "pipes", "-t", "demo", "--yes"]);
    assert!(!out.contains("critical path"), "{out}");
    assert!(out.contains("Successfully synced pipes"), "{out}");
}

#[test]
fn graph_sync_with_nopretty_has_no_gantt() {
    let env = chain();
    let out = env.ok(&["sync", "pipes", "-m", "daily", "--parents", "--nopretty"]);
    assert!(!out.contains("critical path"), "{out}");
}

#[test]
fn sync_help_lists_v5_flags_after_the_v4_text() {
    let env = chain();
    let out = env.ok(&["sync", "pipes", "--help"]);
    assert!(
        out.starts_with("\nFetch and sync new data for pipes."),
        "{out}"
    );
    let at = out.find("v5 additions (not in v4):").expect(&out);
    for flag in ["--parents", "--children", "--resume", "--chunk-pipelining"] {
        assert!(out[at..].contains(flag), "{flag} in {out}");
        assert!(
            !out[..at].contains(flag),
            "{flag} leaked into v4 text: {out}"
        );
    }
    let plan_help = env.ok(&["plan", "pipes", "--help"]);
    assert!(plan_help.contains("without syncing"), "{plan_help}");
}

#[test]
fn additive_v5_actions_are_visible_in_action_listings() {
    let env = chain();
    let actions = env.ok(&["show", "actions", "--nopretty"]);
    assert!(actions.lines().any(|l| l == "plan"), "{actions}");
    assert!(actions.lines().any(|l| l == "analyze"), "{actions}");
}

/// A recorded run with fixed times: a chain with one failed branch and one skipped node.
fn recorded_run() -> meerschaum_engine::RunRecord {
    use chrono::{TimeDelta, TimeZone, Utc};
    use meerschaum_engine::{Action, NodeRow, NodeStatus, PipeRef, RunRecord, RunRow};
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let at = |s: i64| t0 + TimeDelta::seconds(s);
    let node = |metric: &str,
                action: Action,
                status: NodeStatus,
                actual: Option<(i64, i64)>,
                planned: (f64, f64, f64)| {
        let pipe_ref = PipeRef::new("sql:local", "sql:local", metric, None);
        let mut row = NodeRow::new("01JW9GANTT", &pipe_ref, action, "");
        row.status = status;
        if let Some((s, f)) = actual {
            row.started = Some(at(s));
            row.finished = Some(at(f));
            if status == NodeStatus::Succeeded {
                row.duration_seconds = Some((f - s) as f64);
            }
        }
        row.planned_start_seconds = Some(planned.0);
        row.planned_duration_seconds = Some(planned.1);
        row.slack_seconds = Some(planned.2);
        row
    };
    let mut raw = node(
        "raw",
        Action::Root,
        NodeStatus::Succeeded,
        Some((0, 40)),
        (0.0, 30.0, 0.0),
    );
    raw.inserted = 12_301;
    let mut clean = node(
        "clean",
        Action::Incremental,
        NodeStatus::Succeeded,
        Some((40, 90)),
        (30.0, 45.0, 0.0),
    );
    clean.inserted = 12_290;
    clean.updated = 11;
    let mut geo = node(
        "geo",
        Action::Backfill,
        NodeStatus::Failed,
        Some((40, 49)),
        (30.0, 20.0, 25.0),
    );
    geo.message = Some("connection reset\ntraceback".into());
    let daily = node(
        "daily",
        Action::Skip,
        NodeStatus::Skipped,
        None,
        (75.0, 0.0, 0.0),
    );
    RunRecord {
        run: RunRow {
            run_id: "01JW9GANTT".into(),
            started: t0,
            finished: Some(at(100)),
            success: Some(false),
            message: Some("1 failed".into()),
            targets: vec![],
            workers: 4,
            num_nodes: 4,
            resumed_from: None,
            holder: "test".into(),
            selection: None,
        },
        nodes: vec![raw, clean, geo, daily],
    }
}

#[test]
fn gantt_shows_observed_bars_without_forecast_overlays() {
    let text = meerschaum_cli::native::engine_views::gantt(&recorded_run(), 100);
    assert_eq!(text, GOLDEN_GANTT, "\n{text}");
}

const GOLDEN_GANTT: &str = r#"run 01JW9GANTT  4 nodes  4 workers  wall 1m40s  critical path: Pipe('sql:local', 'raw') → Pipe('sql:local', 'clean')
Pipe('sql:local', 'raw')    █████████████░░░░░░░░░░░░░░░░░░░░  40s    +12,301 ins                  ✔
Pipe('sql:local', 'clean')  ░░░░░░░░░░░░░█████████████████░░░  50s    +12,290 ins  ~11 upd         ✔
Pipe('sql:local', 'geo')    ░░░░░░░░░░░░░███                   9s     error: connection reset      ✘
Pipe('sql:local', 'daily')  ·                                         skip                         ○
"#;

#[test]
fn gantt_scales_to_actual_time_when_forecast_is_longer() {
    use chrono::TimeDelta;
    let mut run = recorded_run();
    run.nodes.truncate(2);
    // The run took 10 s; the plan expected 30 s + 45 s.
    run.run.finished = Some(run.run.started + TimeDelta::seconds(10));
    for (n, (s, f)) in run.nodes.iter_mut().zip([(0, 4), (4, 10)]) {
        n.started = Some(run.run.started + TimeDelta::seconds(s));
        n.finished = Some(run.run.started + TimeDelta::seconds(f));
        n.duration_seconds = Some((f - s) as f64);
    }
    let text = meerschaum_cli::native::engine_views::gantt(&run, 60);
    let golden = r#"run 01JW9GANTT  2 nodes  4 workers  wall 10s  critical path: Pipe('sql:local', 'raw') → Pipe('sql:local', 'clean')
Pipe('sql:local', 'raw')    ████░░░░░░  4s     +12,301 ins                  ✔
Pipe('sql:local', 'clean')  ░░░░██████  6s     +12,290 ins  ~11 upd         ✔
"#;
    assert_eq!(text, golden, "\n{text}");
}

#[test]
fn gantt_of_rows_without_a_plan_has_no_planned_bars() {
    let mut run = recorded_run();
    for n in &mut run.nodes {
        n.planned_duration_seconds = None;
    }
    let text = meerschaum_cli::native::engine_views::gantt(&run, 100);
    assert!(!text.contains('\u{2592}'), "{text}");
    assert!(!text.contains("planned"), "{text}");
    assert_eq!(text.lines().count(), 5, "{text}");
}

/// `plan pipes` shows a provisional node with the window the executor re-plans it with once
/// its parents have synced (a backfill from the start of the parent's grid cell), not the
/// static tail window: the planned and executed windows agree.
#[test]
fn plan_pipes_shows_the_window_the_executor_passes() {
    let env = chain();
    env.ok(&["sync", "pipes", "-m", "clean", "--parents", "--yes"]);

    // A late reading before `raw`'s watermark (2024-01-02 00:00): `raw` syncs it, writes a
    // ledger entry for its 30-day grid cell, and `clean` backfills that cell up to the cap.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let attrs = ConnectorAttributes {
        flavor: Some(Flavor::Sqlite),
        database: Some(
            env.root
                .join("sqlite/mrsm_local.db")
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
    let queries = ["INSERT INTO readings VALUES ('2024-01-01 12:00:00', 'c', 4.0)".to_string()];
    assert!(
        runtime
            .block_on(connector.exec_queries(&queries, true, true))
            .iter()
            .all(Option::is_some)
    );

    let out = env.ok(&["plan", "pipes", "-m", "clean", "--parents", "--nopretty"]);
    let plan: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    let planned = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["pipe_ref"].as_str().unwrap_or_default().contains("clean"))
        .unwrap_or_else(|| panic!("{out}"))
        .clone();
    assert_eq!(planned["provisional"], true, "{planned}");
    assert_eq!(planned["action"], "backfill", "{planned}");
    // The static plan shows the tail window (own watermark − 1 day); the executor starts on
    // the grid: 2024-01-01 is day 19723 since the epoch, so its 30-day cell starts 13 days
    // earlier.
    let begin = planned["bounds"]["begin"].to_string();
    assert!(begin.contains("2023-12-19"), "{planned}");
    let table = env.ok(&["plan", "pipes", "-m", "clean", "--parents"]);
    assert!(table.contains("backfill*  [2023-12-19 00:00"), "{table}");

    env.ok(&["sync", "pipes", "-m", "clean", "--parents", "--yes"]);
    let runs = env.ok(&["show", "runs", "--nopretty"]);
    let runs: serde_json::Value = serde_json::from_str(runs.trim()).unwrap();
    let run_id = runs[0]["run"]["run_id"].as_str().unwrap().to_string();
    let detail = env.ok(&["show", "run", &run_id, "--nopretty"]);
    let detail: serde_json::Value = serde_json::from_str(detail.trim()).unwrap();
    let executed = detail["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["pipe_ref"].as_str().unwrap_or_default().contains("clean"))
        .unwrap_or_else(|| panic!("{detail}"))
        .clone();
    assert_eq!(executed["action"], planned["action"], "{executed}");
    assert_eq!(executed["begin"], planned["bounds"]["begin"], "{executed}");
    assert_eq!(executed["end"], planned["bounds"]["end"], "{executed}");
}
