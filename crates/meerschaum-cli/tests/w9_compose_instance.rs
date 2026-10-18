//! `mrsm compose explain` / `up --dry` over the REAL instance connectors (wave 9, task V).
//!
//! The expected output is recorded from the released v4.1.4 by
//! `scripts/parity/gen_w9_compose_instance.py` (`scripts/parity/run.sh`) against a real SQLite
//! instance, into `tests/fixtures/w9_compose_instance/expected.json`. The synthetic project
//! (`projects/main`) has five registered or unregistered pipes (up to date, parameters added,
//! outdated, not registered, temporary), `{{ Pipe() }}` references to a registered pipe with an
//! explicit `target` and to an unregistered pipe, and one tagged pipe the project no longer
//! defines.
//!
//! Each test builds its own temporary root, plugins directory, home and project; nothing reads
//! the real config. The PostgreSQL test runs when `MRSM_TEST_POSTGIS_URI` is set (an empty value
//! selects the disposable test container) and uses the schema `w9v`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use meerschaum_cli::compose_action::compose_native;
use meerschaum_cli::native::Session;
use meerschaum_compose::command::{MrsmCommand, RecordingRunner};
use meerschaum_compose::env::ProcessEnv;
use meerschaum_compose::error::{ErrorKind, Message};
use meerschaum_compose::ops::NoInput;
use meerschaum_compose::project::Host;
use meerschaum_compose::subactions::{ComposeArgs, ComposeOutput};
use meerschaum_connector::{PipeId, PipesKeys, PipesKeysQuery};
use meerschaum_core::config::{Config, ConfigOptions};
use meerschaum_core::value::{Map, Value};

const MRSM: &str = env!("CARGO_BIN_EXE_mrsm-cli");

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/w9_compose_instance")
}

fn expected() -> serde_json::Value {
    let text = std::fs::read_to_string(fixtures().join("expected.json")).unwrap();
    serde_json::from_str(&text).unwrap()
}

fn case() -> serde_json::Value {
    let text = std::fs::read_to_string(fixtures().join("case.json")).unwrap();
    serde_json::from_str(&text).unwrap()
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

/// A temporary host (root, plugins, home) with a copy of one fixture project.
struct Sandbox {
    _tmp: tempfile::TempDir,
    base: PathBuf,
    project: PathBuf,
}

impl Sandbox {
    fn new(project: &str) -> Sandbox {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().canonicalize().unwrap();
        for sub in ["host-root", "host-plugins", "home"] {
            std::fs::create_dir(base.join(sub)).unwrap();
        }
        let dir = base.join("proj");
        copy_dir(&fixtures().join("projects").join(project), &dir);
        // v4 and v5 both need the project root to exist before a connector can live in it.
        std::fs::create_dir(dir.join("root")).unwrap();
        Sandbox {
            _tmp: tmp,
            base,
            project: dir,
        }
    }

    /// The host environment: an isolated root, plugins directory and home. `extra` are added.
    fn env(&self, extra: &[(&str, String)]) -> Vec<(String, String)> {
        let mut env = vec![
            (
                "PATH".to_string(),
                std::env::var("PATH").unwrap_or_default(),
            ),
            ("HOME".into(), self.base.join("home").display().to_string()),
            (
                "MRSM_ROOT_DIR".into(),
                self.base.join("host-root").display().to_string(),
            ),
            (
                "MRSM_PLUGINS_DIR".into(),
                self.base.join("host-plugins").display().to_string(),
            ),
            ("COLUMNS".into(), "100".into()),
            ("LANG".into(), "C.UTF-8".into()),
        ];
        for (k, v) in extra {
            env.push((k.to_string(), v.clone()));
        }
        env
    }

    fn host(&self, extra: &[(&str, String)]) -> Host {
        let env: ProcessEnv = self.env(extra).into_iter().collect();
        Host::new(&self.project, env).expect("host")
    }

    fn mrsm(&self, args: &[&str], extra: &[(&str, String)]) -> Output {
        Command::new(MRSM)
            .args(args)
            .env_clear()
            .envs(self.env(extra))
            .current_dir(&self.project)
            .output()
            .expect("run mrsm")
    }

    /// Register the pipes of `case.json` with the native CLI (`register pipes`), on the
    /// instance `MRSM_SQL_W9V` describes.
    fn register(&self, w9v: &str, only: Option<&str>) {
        for rec in case()["registered"].as_array().unwrap() {
            let metric = rec["metric"].as_str().unwrap();
            if only.is_some_and(|o| o != metric) {
                continue;
            }
            let params = rec["parameters"].to_string();
            let out = self.mrsm(
                &[
                    "register", "pipes", "-c", "sql:w9v", "-m", metric, "-l", "None", "-i",
                    "sql:w9v", "--params", &params, "--noask",
                ],
                &[("MRSM_SQL_W9V", w9v.to_string())],
            );
            assert!(
                out.status.success(),
                "register {metric}: {}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }
}

fn sqlite_env(path: &Path) -> String {
    format!(
        r#"{{"flavor":"sqlite","database":"{}"}}"#,
        path.display().to_string().replace('\\', "/")
    )
}

fn compose_args(action: &[&str], dry: bool) -> ComposeArgs {
    let mut sysargs = vec!["compose".to_string()];
    sysargs.extend(action.iter().map(|a| a.to_string()));
    if dry {
        sysargs.push("--dry".into());
    }
    ComposeArgs {
        action: action.iter().map(|a| a.to_string()).collect(),
        sysargs,
        file: None,
        env_file: None,
        dry,
        force: false,
        presync: false,
        no_jobs: false,
        isolated: false,
        drop: false,
        yes: false,
        noask: true,
        nopretty: false,
        debug: false,
        width: Some(100),
    }
}

/// Run a natively served subaction with a recording runner (nothing is spawned).
fn run(host: &Host, action: &[&str], dry: bool) -> (ComposeOutput, Vec<MrsmCommand>) {
    let mut runner = RecordingRunner::default();
    let mut prompter = NoInput;
    let output = compose_native(host, &compose_args(action, dry), &mut runner, &mut prompter);
    let commands = output.commands.clone();
    (output, commands)
}

fn scenario_run(scenario: &str, run: &str) -> serde_json::Value {
    expected()["scenarios"][scenario]["runs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == run)
        .unwrap_or_else(|| panic!("no run '{run}' in scenario '{scenario}'"))
        .clone()
}

fn infos(output: &ComposeOutput) -> Vec<String> {
    output
        .messages
        .items
        .iter()
        .filter_map(|m| match m {
            Message::Info(i) => Some(i.clone()),
            Message::Warn(_) => None,
        })
        .collect()
}

fn warnings(output: &ComposeOutput) -> Vec<String> {
    output
        .messages
        .items
        .iter()
        .filter_map(|m| match m {
            Message::Warn(w) => Some(w.clone()),
            Message::Info(_) => None,
        })
        .collect()
}

/// `sysargs` with the `--params` JSON parsed (key order is not part of the contract).
fn normalized(sysargs: &[String]) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    let mut after_params = false;
    for arg in sysargs {
        if after_params {
            out.push(serde_json::from_str(arg).expect("--params is JSON"));
            after_params = false;
        } else {
            after_params = arg == "--params";
            out.push(serde_json::Value::String(arg.clone()));
        }
    }
    out
}

fn expected_commands(run: &serde_json::Value) -> Vec<Vec<serde_json::Value>> {
    run["commands"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            let sysargs: Vec<String> = c["sysargs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|a| a.as_str().unwrap().to_string())
                .collect();
            normalized(&sysargs)
        })
        .collect()
}

fn actual_commands(commands: &[MrsmCommand]) -> Vec<Vec<serde_json::Value>> {
    commands.iter().map(|c| normalized(&c.sysargs)).collect()
}

/// The registration status of each pipe in an explain table, in order.
fn statuses(explain: &str) -> Vec<String> {
    const MARKS: [&str; 5] = [
        "Up-to-date",
        "Params added",
        "Outdated",
        "Not registered",
        "Temporary",
    ];
    explain
        .lines()
        .filter_map(|line| {
            let left = line.split('│').next()?;
            MARKS
                .iter()
                .find(|m| left.contains(*m))
                .map(|m| m.to_string())
        })
        .collect()
}

fn assert_explain(output: &ComposeOutput, run: &serde_json::Value) {
    let result = output.result.as_ref().expect("explain succeeds");
    assert!(result.success, "{}", result.message);
    let actual = output.explain.clone().expect("explain text");
    let want = run["explain"].as_str().unwrap();
    assert_eq!(
        actual.trim_end(),
        want.trim_end(),
        "explain differs from v4.1.4\n--- actual ---\n{actual}\n--- v4.1.4 ---\n{want}"
    );
}

/// The registered pipes on `sql:w9v` as `{(ck, mk, lk): parameters}` (`fetch_pipes_keys`).
fn registry(env_value: &str) -> BTreeMap<(String, String, Option<String>), Map> {
    let session = Session::new();
    let config = Config::new(ConfigOptions::with_env(vec![
        ("MRSM_SQL_W9V".to_string(), env_value.to_string()),
        (
            "MRSM_ROOT_DIR".to_string(),
            std::env::temp_dir().display().to_string(),
        ),
    ]))
    .unwrap();
    let instance = session.instance("sql:w9v", &config).expect("connect");
    let keys = session
        .block_on(instance.conn().fetch_pipes_keys(&PipesKeysQuery::default()))
        .expect("fetch_pipes_keys");
    let entries: Vec<(Option<PipeId>, _)> = match keys {
        PipesKeys::List(l) => l.into_iter().map(|e| (None, e)).collect(),
        PipesKeys::Dict(d) => d.into_iter().map(|(id, e)| (Some(id), e)).collect(),
    };
    let out = entries
        .into_iter()
        .map(|(_, e)| {
            let params = match e.extra {
                Some(Value::Map(m)) => m,
                Some(Value::Str(s)) => Value::from_json_str(&s).unwrap().to_map(),
                _ => Map::new(),
            };
            let lk = e.location_key.filter(|l| l != "None");
            ((e.connector_keys, e.metric_key, lk), params)
        })
        .collect();
    session.shutdown();
    out
}

fn expected_registry(run: &serde_json::Value) -> BTreeMap<(String, String, Option<String>), Map> {
    run["registry"]
        .as_array()
        .unwrap()
        .iter()
        .map(|rec| {
            let params = Value::from_json_str(&rec["parameters"].to_string())
                .unwrap()
                .to_map();
            (
                (
                    rec["connector_keys"].as_str().unwrap().to_string(),
                    rec["metric_key"].as_str().unwrap().to_string(),
                    rec["location_key"].as_str().map(str::to_string),
                ),
                params,
            )
        })
        .collect()
}

fn assert_registry(
    actual: &BTreeMap<(String, String, Option<String>), Map>,
    run: &serde_json::Value,
) {
    let want = expected_registry(run);
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        want.keys().collect::<Vec<_>>(),
        "registered pipes"
    );
    for (keys, params) in &want {
        assert_eq!(
            Value::Map(actual[keys].clone()),
            Value::Map(params.clone()),
            "registered parameters of {keys:?}"
        );
    }
}

/// `explain` and `up --dry` on a database holding the pipes of `case.json`: the statuses, the
/// messages, the registration commands (with `{{ Pipe() }}` rendered to table names), and the
/// edits `up` makes (v4 edits registered pipes and removes the project's tag from stale ones
/// even with `--dry`; `--dry` only skips the jobs).
fn check_registered_project(sandbox: &Sandbox, extra: &[(&str, String)], w9v: &str, quote: &str) {
    let host = sandbox.host(extra);
    // PostgreSQL stores the parameters as JSONB, which returns the keys in its own order, so the
    // remote column differs from the SQLite recording there: compare the statuses.
    let assert_same_explain = |output: &ComposeOutput, run: &serde_json::Value| {
        if quote.is_empty() {
            assert_explain(output, run);
        } else {
            let result = output.result.as_ref().expect("explain succeeds");
            assert!(result.success, "{}", result.message);
            assert_eq!(
                statuses(output.explain.as_deref().unwrap()),
                statuses(run["explain"].as_str().unwrap())
            );
        }
    };

    let (explain, commands) = run(&host, &["explain"], false);
    assert_same_explain(&explain, &scenario_run("main", "explain"));
    assert_eq!(warnings(&explain), Vec::<String>::new());
    assert_eq!(
        commands
            .iter()
            .map(|c| c.sysargs.clone())
            .collect::<Vec<_>>(),
        vec![vec!["show", "version", "--no-daemon", "--tags", "w9proj"]],
    );
    // Explaining writes nothing.
    assert_registry(&registry(w9v), &scenario_run("main", "explain"));

    let (up, commands) = run(&host, &["up"], true);
    let want = scenario_run("main", "up-dry");
    let result = up.result.as_ref().expect("up --dry succeeds");
    assert_eq!(
        (result.success, result.message.clone()),
        (
            want["result"][0].as_bool().unwrap(),
            want["result"][1].as_str().unwrap().to_string()
        )
    );
    let want_messages: Vec<String> = want["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m[1].as_str().unwrap().to_string())
        .collect();
    assert_eq!(infos(&up), want_messages);
    assert_eq!(warnings(&up), Vec::<String>::new());

    let mut want_commands = expected_commands(&want);
    if !quote.is_empty() {
        // Schema-qualified table names (`sql_item_name(target, flavor, schema)`).
        for command in &mut want_commands {
            for arg in command.iter_mut() {
                if let Some(sql) = arg.get_mut("sql") {
                    let text = sql.as_str().unwrap().to_string();
                    *sql = serde_json::Value::String(
                        text.replace("\"w9_alpha_tbl\"", &format!("\"{quote}\".\"w9_alpha_tbl\""))
                            .replace(
                                "\"sql_w9v_delta_north\"",
                                &format!("\"{quote}\".\"sql_w9v_delta_north\""),
                            ),
                    );
                }
            }
        }
    }
    assert_eq!(actual_commands(&commands), want_commands);

    // `up --dry` edited the registrations (the registration commands were only recorded).
    assert_registry(&registry(w9v), &want);

    let (after, _) = run(&host, &["explain"], false);
    assert_same_explain(&after, &scenario_run("main", "explain-after"));
}

#[test]
fn explain_and_up_dry_match_v4_with_a_project_defined_sqlite_instance() {
    let sandbox = Sandbox::new("main");
    let db = sandbox.project.join("root/w9v.db");
    sandbox.register(&sqlite_env(&db), None);
    assert!(db.exists());
    check_registered_project(&sandbox, &[], &sqlite_env(&db), "");
}

#[test]
fn the_connector_the_file_defines_wins_over_an_environment_connector() {
    // v4 builds file-defined connectors with `get_connector(keys, **attributes)`, whose keyword
    // attributes override the `MRSM_SQL_W9V` variable of the same label.
    let sandbox = Sandbox::new("main");
    let db = sandbox.project.join("root/w9v.db");
    sandbox.register(&sqlite_env(&db), None);
    let other = sandbox.base.join("other.sqlite");
    sandbox.register(&sqlite_env(&other), Some("alpha"));
    let (explain, _) = run(
        &sandbox.host(&[("MRSM_SQL_W9V", sqlite_env(&other))]),
        &["explain"],
        false,
    );
    assert_explain(&explain, &scenario_run("envlayer", "explain"));
}

#[test]
fn an_environment_connector_is_the_instance_when_the_file_defines_none() {
    let sandbox = Sandbox::new("envonly");
    let db = sandbox.base.join("env.sqlite");
    sandbox.register(&sqlite_env(&db), None);
    let (explain, _) = run(
        &sandbox.host(&[("MRSM_SQL_W9V", sqlite_env(&db))]),
        &["explain"],
        false,
    );
    assert_explain(&explain, &scenario_run("envonly", "explain"));
    // The project environment's own `environment:` entries are layered the same way: a URI
    // works as well as the JSON attributes.
    let (explain, _) = run(
        &sandbox.host(&[("MRSM_SQL_W9V", format!("sqlite:///{}", db.display()))]),
        &["explain"],
        false,
    );
    assert_explain(&explain, &scenario_run("envonly", "explain"));
}

#[test]
fn explain_never_creates_the_database_of_a_file_instance() {
    // v4's explain creates the SQLite file (and its `mrsm_*` tables and cache files); the
    // registry of a database that does not exist is empty without connecting to it.
    let sandbox = Sandbox::new("main");
    let db = sandbox.project.join("root/w9v.db");
    let before_v4 = scenario_run("nodb", "explain");
    assert!(
        before_v4["files"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f == "root/w9v.db"),
        "v4 creates the database"
    );
    let (explain, _) = run(&sandbox.host(&[]), &["explain"], false);
    assert_explain(&explain, &before_v4);
    assert!(!db.exists(), "explain created {}", db.display());
    let created: Vec<String> = walk(&sandbox.project)
        .into_iter()
        .filter(|p| {
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            name.ends_with(".db") || name.ends_with(".sqlite") || name.ends_with(".gpkg")
        })
        .map(|p| p.display().to_string())
        .collect();
    assert!(created.is_empty(), "database files created: {created:?}");

    // `up --dry` on the missing database: every pipe is registered (by the recorded commands),
    // and the table references of unregistered pipes are the default targets.
    let (up, commands) = run(&sandbox.host(&[]), &["up"], true);
    let want = scenario_run("nodb_up", "up-dry");
    assert!(up.result.as_ref().unwrap().success);
    assert_eq!(
        infos(&up),
        want["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m[1].as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(actual_commands(&commands), expected_commands(&want));
    assert!(!db.exists(), "up --dry created {}", db.display());
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            out.extend(walk(&entry.path()));
        } else {
            out.push(entry.path());
        }
    }
    out
}

#[test]
fn an_unreachable_instance_raises_like_v4() {
    // v4: `get_pipes` raises `Exception('\n 🛑 <error>')`, which fails the subaction.
    let v4 = scenario_run("unreachable", "explain");
    assert_eq!(v4["error"]["type"], "Exception");
    assert!(
        v4["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("\n 🛑 ")
    );

    let sandbox = Sandbox::new("unreachable");
    for (action, dry) in [(&["explain"][..], false), (&["up"][..], true)] {
        let (output, _) = run(&sandbox.host(&[]), action, dry);
        let error = output.result.expect_err("an unreachable instance fails");
        assert_eq!(error.kind, ErrorKind::Exception, "{error}");
        assert!(error.message.starts_with("\n 🛑 "), "{}", error.message);
    }
}

#[test]
fn the_binary_serves_explain_and_registers_in_the_project_environment() {
    let sandbox = Sandbox::new("main");
    let db = sandbox.project.join("root/w9v.db");
    sandbox.register(&sqlite_env(&db), None);

    let explain = sandbox.mrsm(&["compose", "explain"], &[]);
    let stdout = String::from_utf8_lossy(&explain.stdout).to_string();
    assert!(
        explain.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&explain.stderr)
    );
    assert_eq!(
        stdout.trim_end(),
        scenario_run("main", "explain")["explain"]
            .as_str()
            .unwrap()
            .trim_end()
    );

    // `up --dry` registers the missing pipes by running `register pipes` as a child process:
    // it must see the project's connector and root, not the host's.
    let up = sandbox.mrsm(&["compose", "up", "--dry"], &[]);
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "{text}");
    let registered = registry(&sqlite_env(&db));
    let delta = registered
        .get(&("sql:w9v".into(), "delta".into(), Some("north".into())))
        .unwrap_or_else(|| panic!("delta was not registered:\n{text}"));
    assert_eq!(
        delta.get("tags"),
        Some(&Value::List(vec![Value::Str("w9proj".into())]))
    );
    let epsilon = registered
        .get(&("sql:w9v".into(), "epsilon".into(), None))
        .unwrap_or_else(|| panic!("epsilon was not registered:\n{text}"));
    assert_eq!(
        epsilon.get("sql"),
        Some(&Value::Str(
            "SELECT * FROM \"w9_alpha_tbl\" JOIN \"sql_w9v_delta_north\"".into()
        ))
    );
    // Nothing was registered in the host's root.
    assert!(!sandbox.base.join("host-root/sqlite").exists());
}

fn postgis_uri() -> Option<String> {
    let raw = std::env::var("MRSM_TEST_POSTGIS_URI").ok()?;
    Some(if raw.is_empty() {
        "postgis://test:test1234@localhost:5449/testdb".to_string()
    } else {
        raw
    })
}

#[test]
fn explain_and_up_dry_match_v4_on_postgresql_schema_w9v() {
    let Some(base_uri) = postgis_uri() else {
        eprintln!("skipped: MRSM_TEST_POSTGIS_URI is not set");
        return;
    };
    let sep = if base_uri.contains('?') { '&' } else { '?' };
    let uri = format!("{base_uri}{sep}schema=w9v");

    // A clean schema of our own.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let admin = rt
        .block_on(meerschaum_connector_sql::SqlConnector::from_uri(
            "w9admin", &base_uri,
        ))
        .expect("connect to the PostgreSQL test container");
    let reset = |sql: &str| rt.block_on(admin.driver().exec(sql)).expect(sql);
    reset("DROP SCHEMA IF EXISTS w9v CASCADE");
    reset("CREATE SCHEMA w9v");

    // The instance comes from the environment only (the project defines no connector), so the
    // same expected output as the SQLite run applies, with schema-qualified table references.
    let sandbox = Sandbox::new("envonly");
    sandbox.register(&uri, None);
    check_registered_project(&sandbox, &[("MRSM_SQL_W9V", uri.clone())], &uri, "w9v");

    reset("DROP SCHEMA IF EXISTS w9v CASCADE");
}
