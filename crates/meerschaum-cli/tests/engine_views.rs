//! `plan pipes`, `show graph`, `show runs` / `show run`, the Gantt chart of graph runs, v5-only
//! help, and native plugins in `show plugins`, through the `mrsm` binary over a temporary root
//! with a SQLite instance. Nothing here touches the default root: every command sets
//! `MRSM_ROOT_DIR` and an empty `MRSM_PLUGINS_DIR` inside a temporary directory.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use meerschaum_core::{Pipe, Value};
use meerschaum_sql::{
    SqlConnector,
    flavor::Flavor,
    uri::{ConnectionSpec, ConnectorAttributes},
};

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
    let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm"));
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
        assert!(out.contains(&format!("sql:local/{metric}")), "{out}");
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
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 3, "{out}");
    assert!(lines[0].starts_with("sql:local/raw"), "{out}");
    assert!(lines[1].starts_with("  sql:local/clean"), "{out}");
    assert!(
        lines[1].contains("<-") || lines[1].contains('\u{2190}'),
        "{out}"
    );
    assert!(lines[2].starts_with("    sql:local/daily*"), "{out}");
    let json = env.ok(&["show", "graph", "-t", "demo", "--nopretty"]);
    let graph: serde_json::Value = serde_json::from_str(json.trim()).unwrap();
    assert_eq!(graph["nodes"].as_array().unwrap().len(), 3, "{json}");
    assert_eq!(graph["edges"].as_array().unwrap().len(), 2, "{json}");
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
    // `show run <id>` draws the same chart.
    let gantt = env.ok(&["show", "run", &run_id]);
    assert!(gantt.starts_with(&format!("run {run_id}")), "{gantt}");
    assert!(gantt.contains("critical path"), "{gantt}");
    let missing = env.run(&["show", "run", "01NOPE"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stdout).contains("No run '01NOPE'"));
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
fn v5_only_actions_stay_out_of_v4_listings() {
    let env = chain();
    let actions = env.ok(&["show", "actions", "--nopretty"]);
    assert!(!actions.lines().any(|l| l == "plan"), "{actions}");
}
