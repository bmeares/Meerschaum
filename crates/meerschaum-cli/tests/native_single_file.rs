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
