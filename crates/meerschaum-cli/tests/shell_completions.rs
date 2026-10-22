//! The shell's completion candidates match v4.1.4's `ShellCompleter.get_completions` for every
//! input in `fixtures/shell_completions.json` (text, start position, display and the highlighted
//! prefix), against a temporary root with the same connectors and two stopped jobs.
//!
//! Regenerate the fixture with
//! `scripts/parity/run.sh crates/meerschaum-cli/tests/fixtures/gen_shell_screens.py`.

use meerschaum_cli::dispatch::Runtime;
use meerschaum_cli::shell::{self, ShellState};
use meerschaum_cli::types::DateContext;
use meerschaum_core::config::{Config, ConfigOptions};
use serde_json::Value as Json;

fn fixture() -> Json {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/shell_completions.json"
    );
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn no_env(_: &str) -> Option<String> {
    None
}

#[test]
fn completions_match_v4() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_path_buf();
    // Isolation: everything lives in a fresh temporary directory, SQLite only.
    assert!(base.starts_with(std::env::temp_dir()), "{base:?}");
    let root = base.join("root");
    let plugins = base.join("plugins");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    let env: Vec<(String, String)> = vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_PLUGINS_DIR".into(),
            plugins.to_string_lossy().into_owned(),
        ),
        (
            "MRSM_CONFIG".into(),
            r#"{"meerschaum": {"instance": "sql:local"}, "shell": {"updates": {"check_remote": false}}}"#
                .into(),
        ),
        (
            "MRSM_SQL_MAIN".into(),
            format!("sqlite:///{}", base.join("main.db").display()),
        ),
        (
            "MRSM_SQL_X".into(),
            format!("sqlite:///{}", base.join("x.db").display()),
        ),
    ];
    let mut opts = ConfigOptions::with_env(env);
    opts.home = base.join("home");
    let config = Config::new(opts).unwrap();
    assert!(config.paths().root_dir.starts_with(&base));

    let fx = fixture();
    let mut out = Vec::new();
    let mut rt = Runtime::new(DateContext::system(), 80, &mut out, no_env);
    rt.config = Some(config);
    let ctx = meerschaum_cli::native::jobs::jobs_context(&mut rt);
    for job in fx["setup"]["jobs"].as_array().unwrap() {
        let name = job["name"].as_str().unwrap();
        let sysargs: Vec<String> = job["sysargs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap().to_string())
            .collect();
        let mut daemon =
            meerschaum_jobs::Daemon::new(name, sysargs, None, Default::default(), &ctx);
        let result = daemon.write_properties();
        assert!(result.success, "{result:?}");
    }

    let state = ShellState::new(&mut rt, None);
    let data = shell::completion_data(&state, &mut rt);
    let dates = DateContext::system();
    // The jobs and connectors are really there (the fixture would also catch this).
    let jobs: Vec<String> = data
        .get_completions("start job ", &dates)
        .into_iter()
        .map(|c| c.text)
        .collect();
    assert_eq!(jobs, vec!["alpha-job", "beta-job"]);
    assert!(fx["cases"].as_array().unwrap().len() > 200);
    let mut failures = Vec::new();
    for case in fx["cases"].as_array().unwrap() {
        let input = case["input"].as_str().unwrap();
        let expected: Vec<(String, i64, String, usize)> = case["completions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                let display = c["display"].as_str().unwrap().to_string();
                let styles = c["display_styles"].as_array().unwrap();
                // `[('fg:ansiblue bold', prefix), ('', rest)]`: the prefix is the display minus
                // the rest; a single unstyled fragment highlights nothing.
                let highlight = if styles.len() == 2 {
                    let rest = c["display"].as_str().unwrap().chars().count();
                    let start = -c["start_position"].as_i64().unwrap();
                    (start as usize).min(rest)
                } else {
                    0
                };
                (
                    c["text"].as_str().unwrap().to_string(),
                    c["start_position"].as_i64().unwrap(),
                    display,
                    highlight,
                )
            })
            .collect();
        // Prefixes also offer v5 subactions (e.g. `sync p` offers `pipeline`). Exclude only
        // these registered additions while comparing against the unchanged v4 fixture.
        // Restrict this to the subaction position so later argument values are compared intact.
        let words: Vec<&str> = input
            .rsplit('+')
            .next()
            .unwrap_or(input)
            .split_whitespace()
            .collect();
        let subaction_parent = ((words.len() == 2 && !input.ends_with(char::is_whitespace))
            || (words.len() == 1 && input.ends_with(char::is_whitespace)))
        .then(|| words[0]);
        let got: Vec<(String, i64, String, usize)> = data
            .get_completions(input, &dates)
            .into_iter()
            .filter(|c| {
                !((words.is_empty() || (words.len() == 1 && !input.ends_with(char::is_whitespace)))
                    && meerschaum_cli::actions::V5_ONLY_ACTIONS.contains(&c.text.as_str()))
                    && !subaction_parent.is_some_and(|action| {
                        meerschaum_cli::actions::is_v5_only_subaction(action, &c.text)
                    })
            })
            .map(|c| (c.text, c.start_position, c.display, c.highlight))
            .collect();
        if got != expected {
            failures.push(format!("{input:?}\n  v4: {expected:?}\n  v5: {got:?}"));
        }
    }
    // The overlay above hides nothing else: the v5-only word is really offered.
    assert!(
        data.get_completions("show c", &dates)
            .iter()
            .any(|c| c.text == "completions")
    );
    assert!(
        data.get_completions("upgrade p", &dates)
            .iter()
            .any(|c| c.text == "postgres")
    );
    assert!(
        failures.is_empty(),
        "{} of {} inputs differ:\n{}",
        failures.len(),
        fx["cases"].as_array().unwrap().len(),
        failures.join("\n")
    );
}

#[test]
fn sync_pipeline_completion_is_exposed_in_v5() {
    let data = shell::completer::CompletionData::default();
    let completions = data.get_completions("sync p", &DateContext::system());
    let pipeline = completions
        .iter()
        .find(|c| c.text == "pipeline")
        .expect("the interactive shell offers the v5 pipeline subaction");
    assert_eq!(pipeline.start_position, -1);
    assert_eq!(pipeline.display, "pipeline");
    assert_eq!(pipeline.highlight, 1);
    assert!(completions.iter().any(|c| c.text == "pipes"));
}

/// v5-only: `install driver`, `uninstall driver` and `show drivers` complete driver names (not
/// part of the v4 fixture).
#[test]
fn driver_names_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_path_buf();
    let root = base.join("root");
    let plugins = base.join("plugins");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    let env: Vec<(String, String)> = vec![
        ("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned()),
        (
            "MRSM_PLUGINS_DIR".into(),
            plugins.to_string_lossy().into_owned(),
        ),
        (
            "MRSM_CONFIG".into(),
            r#"{"shell": {"updates": {"check_remote": false}}}"#.into(),
        ),
    ];
    let mut opts = ConfigOptions::with_env(env);
    opts.home = base.join("home");
    let config = Config::new(opts).unwrap();
    let mut out = Vec::new();
    let mut rt = Runtime::new(DateContext::system(), 80, &mut out, no_env);
    rt.config = Some(config);
    let state = ShellState::new(&mut rt, None);
    let data = shell::completion_data(&state, &mut rt);
    let dates = DateContext::system();
    let texts = |input: &str| -> Vec<String> {
        data.get_completions(input, &dates)
            .into_iter()
            .map(|c| c.text)
            .collect()
    };
    assert_eq!(texts("install driver "), vec!["duckdb", "oracle"]);
    assert_eq!(texts("install drivers d"), vec!["duckdb"]);
    assert_eq!(texts("install driver duckdb "), vec!["oracle"]);
    assert_eq!(texts("uninstall driver "), vec!["duckdb"]);
    assert_eq!(texts("show drivers "), vec!["duckdb", "oracle"]);
    assert_eq!(texts("show drivers o"), vec!["oracle"]);
}

#[test]
fn registered_pipeline_names_complete_on_the_selected_registry() {
    use meerschaum_cli::shell::completer::CompletionData;
    let data = CompletionData {
        instance_keys: "sql:local".into(),
        pipelines: Some(std::sync::Arc::new(|registry| {
            if registry == "sql:remote" {
                vec!["remote-daily".into()]
            } else {
                vec!["daily".into(), "weekly".into()]
            }
        })),
        ..Default::default()
    };
    let dates = DateContext::system();
    assert!(
        data.get_completions("show ", &dates)
            .iter()
            .any(|c| c.text == "pipelines")
    );
    assert!(
        data.get_completions("p", &dates)
            .iter()
            .any(|c| c.text == "plan")
    );
    let names: Vec<_> = data
        .get_completions("sync pipelines d", &dates)
        .into_iter()
        .map(|c| c.text)
        .collect();
    assert_eq!(names, vec!["daily"]);
    let names: Vec<_> = data
        .get_completions("show pipelines -i sql:remote r", &dates)
        .into_iter()
        .map(|c| c.text)
        .collect();
    assert_eq!(
        names,
        vec!["remote-daily"],
        "parsed: {:?}",
        meerschaum_cli::parse::parse_line("show pipelines -i sql:remote r", &dates)
    );
}
