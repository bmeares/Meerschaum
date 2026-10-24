//! Single-file typed plugins call back into a real SQLite instance through the CLI.
use std::{
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};
#[path = "../../meerschaum-plugin/tests/support/compile_single_file.rs"]
mod compile_single_file;

fn run(root: &std::path::Path, plugins: &std::path::Path, args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", root)
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_PLUGINS_DIR", plugins)
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let start = Instant::now();
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if start.elapsed() > Duration::from_secs(60) {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("native plugin call timed out: {}", text(&output));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn exercise(source: &str) {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    std::fs::write(plugins.path().join("typed.rs"), source).unwrap();
    let builds = root.path().join("cache/native_build");
    let found = meerschaum_plugin::discovery::discover_report_in(&[plugins.path().into()], &builds);
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    let candidate = &found.candidates[0];
    compile_single_file::compile(candidate);
    std::fs::write(
        builds.join("typed/mrsm-source-fingerprint"),
        &candidate.fingerprint,
    )
    .unwrap();
    let register = run(
        root.path(),
        plugins.path(),
        &[
            "register",
            "pipe",
            "-c",
            "plugin:typed",
            "-m",
            "reading",
            "-i",
            "sql:local",
            "--noask",
            "--nopretty",
            "--params",
            r#"{"columns":{"datetime":"id"},"dtypes":{"id":"int"}}"#,
        ],
    );
    assert!(register.status.success(), "{}", text(&register));
    for _ in 0..2 {
        let output = run(
            root.path(),
            plugins.path(),
            &[
                "sync",
                "pipes",
                "-c",
                "plugin:typed",
                "-m",
                "reading",
                "-i",
                "sql:local",
                "--nopretty",
                "--begin",
                "0",
                "--end",
                "10",
            ],
        );
        assert!(output.status.success(), "{}", text(&output));
        // Each call reads the previous sync time and generates the next id. Returning the
        // same id or failing to write via pipe.sync would not insert a row on both runs.
        assert!(
            text(&output).contains("Inserted 1, updated 0 rows."),
            "{}",
            text(&output)
        );
        assert!(
            !text(&output).contains("Building native plugin"),
            "{}",
            text(&output)
        );
    }
}

#[test]
fn fetch_reads_sync_time_from_the_host() {
    exercise(
        r#"
        fn fetch(pipe: &Pipe, end: Option<Bound>, begin: Option<Bound>) -> Result<Vec<Value>> {
            assert_eq!(begin, Some(Bound::Int(0)));
            assert_eq!(end, Some(Bound::Int(10)));
            assert_eq!(pipe.metric_key(), "reading");
            let last = match pipe.get_sync_time()? { Some(Bound::Int(i)) => i, None => 0, _ => panic!("integer axis expected") };
            Ok(vec![json!({"id": last + 1})])
        }
    "#,
    );
}

#[test]
fn sync_writes_through_the_engine_without_recursing() {
    exercise(
        r#"
        fn sync(pipe: &Pipe, begin: Option<Bound>, end: Option<Bound>) -> Result<SuccessTuple> {
            assert_eq!(begin, Some(Bound::Int(0)));
            assert_eq!(end, Some(Bound::Int(10)));
            let last = match pipe.get_sync_time()? { Some(Bound::Int(i)) => i, None => 0, _ => panic!("integer axis expected") };
            pipe.sync(vec![json!({"id": last + 1})])
        }
    "#,
    );
}

fn prepared(source: &str) -> (tempfile::TempDir, tempfile::TempDir) {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    std::fs::write(plugins.path().join("typed.rs"), source).unwrap();
    let builds = root.path().join("cache/native_build");
    let found = meerschaum_plugin::discovery::discover_report_in(&[plugins.path().into()], &builds);
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    let candidate = &found.candidates[0];
    compile_single_file::compile(candidate);
    std::fs::write(
        builds.join("typed/mrsm-source-fingerprint"),
        &candidate.fingerprint,
    )
    .unwrap();
    (root, plugins)
}

#[test]
fn single_file_actions_run_and_appear_in_help_without_rebuilding() {
    let (root, plugins) = prepared(
        r#"
        /// Greet the supplied names from a single Rust file.
        #[make_action]
        fn greet(action: Vec<String>, kwargs: &Value) -> (bool, String) {
            assert_eq!(kwargs["debug"], true);
            (true, format!("Hello, {}!", action.join(", ")))
        }
        #[make_action]
        fn count_words(action: Vec<String>) -> (bool, String) {
            (true, format!("{} words", action.len()))
        }
        #[make_action(name = "salute", daemon = false)]
        fn renamed() -> (bool, &'static str) { (true, "renamed action") }
    "#,
    );
    for (args, expected) in [
        (
            vec!["greet", "Alice", "Bob", "--debug"],
            "Hello, Alice, Bob!",
        ),
        (vec!["salute"], "renamed action"),
        (vec!["count_words", "Alice", "Bob"], "2 words"),
        (vec!["count", "words", "Alice", "Bob"], "2 words"),
        (
            vec!["greet", "--help"],
            "Greet the supplied names from a single Rust file.",
        ),
        (vec!["show", "actions", "--nopretty"], "greet"),
        (
            vec!["--help"],
            "Greet the supplied names from a single Rust file.",
        ),
    ] {
        let output = run(root.path(), plugins.path(), &args);
        let text = text(&output);
        assert!(output.status.success(), "{text}");
        assert!(text.contains(expected), "{text}");
        assert!(!text.contains("Building native plugin"), "{text}");
    }
}

#[test]
fn single_file_source_connectors_register_fetch_sync_and_receive_configuration() {
    let (root, plugins) = prepared(
        r#"
        #[make_connector]
        mod readings {
            fn register(connector: &Connector) -> Value {
                assert_eq!(connector.label(), "office");
                json!({"columns": {"datetime": "id"}, "dtypes": {"id": "int"}})
            }
            fn fetch(connector: &Connector) -> Vec<Value> {
                vec![json!({"id": 1, "station": connector.config()["station"]})]
            }
        }
        #[make_connector]
        mod writer {
            fn register() -> Value {
                json!({"columns": {"datetime": "id"}, "dtypes": {"id": "int"}})
            }
            fn sync(pipe: &Pipe, connector: &Connector, kwargs: &Value) -> Result<SuccessTuple> {
                assert_eq!(connector.label(), "office");
                assert!(kwargs["check_existing"].is_boolean());
                pipe.sync(vec![json!({"id": 2})])
            }
        }
    "#,
    );
    // Configuration uses the same default/label merge as packaged native connectors.
    std::fs::create_dir_all(root.path().join("config")).unwrap();
    std::fs::write(root.path().join("config/meerschaum.yaml"),
        "instance: sql:local\nconnectors:\n  readings:\n    default:\n      station: default\n    office:\n      station: north\n").unwrap();
    for connector in ["readings:office", "writer:office"] {
        let output = run(
            root.path(),
            plugins.path(),
            &[
                "register",
                "pipes",
                "-c",
                connector,
                "-m",
                "reading",
                "-i",
                "sql:local",
                "--noask",
                "--nopretty",
            ],
        );
        assert!(output.status.success(), "{}", text(&output));
        let output = run(
            root.path(),
            plugins.path(),
            &[
                "sync",
                "pipes",
                "-c",
                connector,
                "-i",
                "sql:local",
                "--nopretty",
            ],
        );
        let out = text(&output);
        assert!(output.status.success(), "{out}");
        assert!(out.contains("Inserted 1, updated 0 rows."), "{out}");
        assert!(
            !out.contains("cannot be found") && !out.contains("Traceback"),
            "{out}"
        );
    }
    let shown = run(
        root.path(),
        plugins.path(),
        &[
            "show",
            "data",
            "-c",
            "readings:office",
            "-m",
            "reading",
            "-i",
            "sql:local",
            "--nopretty",
        ],
    );
    assert!(shown.status.success(), "{}", text(&shown));
    assert!(text(&shown).contains("north"), "{}", text(&shown));
}
