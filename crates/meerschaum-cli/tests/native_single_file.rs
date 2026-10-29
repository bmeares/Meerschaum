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

/// The rows every plugin below returns: one column per inferred dtype.
const TYPED_ROWS: &str = r#"vec![
    json!({"id": 1, "f": 1.5, "b": true, "dt": "2024-01-01 00:00:00", "s": "a", "n": null, "ni": null, "mix": 1}),
    json!({"id": 2, "f": 2.5, "b": false, "dt": "2024-01-02 00:00:00", "s": "b", "n": 3, "ni": 1.5, "mix": 2.5}),
]"#;

/// Rows from a single-file plugin get the column types the same rows get from a Python
/// plugin's `fetch()` / `pipe.sync(docs)` (v4: `pd.DataFrame(docs)` + `parse_df_datetimes`),
/// not one `TEXT` column each: through `plugin:<name>`'s `fetch`, a `#[make_connector]`
/// type's `fetch` and a `sync()` calling `pipe.sync(rows)`.
#[test]
fn single_file_rows_sync_with_python_plugin_dtypes() {
    let source = format!(
        r#"
        fn fetch() -> Vec<Value> {{ {TYPED_ROWS} }}
        #[make_connector]
        mod readings {{
            fn fetch() -> Vec<Value> {{ {TYPED_ROWS} }}
        }}
        #[make_connector]
        mod writer {{
            fn sync(pipe: &Pipe) -> Result<SuccessTuple> {{ pipe.sync({TYPED_ROWS}) }}
        }}
    "#
    );
    let (root, plugins) = prepared(&source);
    for connector in ["plugin:typed", "readings:office", "writer:office"] {
        let args = ["-c", connector, "-m", "m", "-i", "sql:local", "--nopretty"];
        let output = run(
            root.path(),
            plugins.path(),
            &[&["register", "pipes", "--noask"], &args[..]].concat(),
        );
        assert!(output.status.success(), "{}", text(&output));
        let output = run(
            root.path(),
            plugins.path(),
            &[&["sync", "pipes"], &args[..]].concat(),
        );
        let out = text(&output);
        assert!(output.status.success(), "{out}");
        assert!(out.contains("Inserted 2, updated 0 rows."), "{out}");
        let shown = run(
            root.path(),
            plugins.path(),
            &[&["show", "columns"], &args[..]].concat(),
        );
        let out = text(&shown);
        assert!(shown.status.success(), "{out}");
        // What `mrsm sync pipes -c plugin:<python plugin>` creates for the same documents
        // (SQLite stores booleans as FLOAT; the null-leading int column is BIGINT).
        assert!(
            out.contains(
                r#"{"id": "BIGINT", "f": "FLOAT", "b": "FLOAT", "dt": "DATETIME", "s": "TEXT", "n": "BIGINT", "ni": "FLOAT", "mix": "FLOAT"}"#
            ),
            "{connector}: {out}"
        );
    }
}

/// v4's `_sync`: a `fetch()` returning an empty list is "No new rows were returned" (success),
/// while `None` is "No data were fetched" (failure). An empty `Vec<Value>` from a single-file
/// plugin is zero rows, and an `Option<Vec<Value>>` expresses `None`, for `plugin:<name>` and
/// `#[make_connector]` types alike. A `sync()` calling `pipe.sync(vec![])` syncs zero rows, as
/// a Python plugin's `pipe.sync([])` does ("Inserted 0, updated 0 rows." once the pipe's
/// columns are known).
#[test]
fn single_file_empty_rows_are_zero_rows_and_none_is_no_data() {
    let (root, plugins) = prepared(
        r#"
        fn fetch() -> Vec<Value> { vec![] }
        #[make_connector]
        mod readings {
            fn fetch() -> Vec<Value> { vec![] }
        }
        #[make_connector]
        mod maybe {
            fn fetch(connector: &Connector) -> Option<Vec<Value>> {
                (connector.label() == "empty").then(Vec::new)
            }
        }
        #[make_connector]
        mod writer {
            fn sync(pipe: &Pipe) -> Result<SuccessTuple> { pipe.sync(vec![]) }
        }
    "#,
    );
    for (connector, success, expected) in [
        (
            "plugin:typed",
            true,
            "No new rows were returned for Pipe('plugin:typed', 'm', instance='sql:local').",
        ),
        (
            "readings:office",
            true,
            "No new rows were returned for Pipe('readings:office', 'm', instance='sql:local').",
        ),
        (
            "maybe:empty",
            true,
            "No new rows were returned for Pipe('maybe:empty', 'm', instance='sql:local').",
        ),
        (
            "maybe:none",
            false,
            "No data were fetched for Pipe('maybe:none', 'm', instance='sql:local').",
        ),
        ("writer:office", true, "Inserted 0, updated 0 rows."),
    ] {
        let args = ["-c", connector, "-m", "m", "-i", "sql:local", "--nopretty"];
        let params = r#"{"columns": {"datetime": "id"}, "dtypes": {"id": "int"}}"#;
        let output = run(
            root.path(),
            plugins.path(),
            &[
                &["register", "pipes", "--noask", "--params", params],
                &args[..],
            ]
            .concat(),
        );
        assert!(output.status.success(), "{}", text(&output));
        let output = run(
            root.path(),
            plugins.path(),
            &[&["sync", "pipes"], &args[..]].concat(),
        );
        let out = text(&output);
        assert_eq!(output.status.success(), success, "{connector}: {out}");
        assert!(out.contains(expected), "{connector}: {out}");
        assert!(
            !out.contains("No data were fetched") || !success,
            "{connector}: {out}"
        );
    }
}
