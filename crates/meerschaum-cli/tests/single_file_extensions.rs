//! A single-file Rust plugin's sync hooks, `setup`, plugin arguments and `VERSION` through the
//! `mrsm` binary, with a temporary root and plugins directory. All data is synthetic.
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};
#[path = "../../meerschaum-plugin/tests/support/compile_single_file.rs"]
mod compile_single_file;

const SOURCE: &str = r#"
#![add_plugin_argument("--times", "-n", type = int, default = 1, help = "Repeat the greeting.")]
#![add_plugin_argument("--direction", choices = ["north", "south"])]
#![add_plugin_argument("--station", required = true)]
#![add_plugin_argument("--tagged", "-t", action = "store_true")]
const VERSION: &str = "1.2.3";
const REQUIRED: &[&str] = &[];

fn fetch() -> Vec<Value> {
    vec![json!({"id": 1})]
}

#[pre_sync_hook]
fn before(pipe: &Pipe, kwargs: &Value) -> Result<(bool, String)> {
    // Python passes the Pipe positionally, not among the kwargs; hooks get its host calls.
    let synced = pipe.get_sync_time()?.is_some();
    Ok((true, format!("HOOK_PRE {} pipe_kwarg={} synced={synced}", pipe.metric_key(), kwargs.get("pipe").is_some())))
}

#[post_sync_hook]
fn after(success_tuple: SuccessTuple, sync_duration: f64, kwargs: &Value) -> (bool, String) {
    let complete = kwargs["sync_complete_timestamp"].is_string();
    (success_tuple.success && sync_duration >= 0.0 && complete, "HOOK_POST_OK".into())
}

#[post_sync_hook]
fn failing() -> (bool, &'static str) {
    (false, "HOOK_POST_FAILED")
}

#[post_sync_hook]
fn raising() -> Result<SuccessTuple> {
    Err(Error::Message("HOOK_RAISED".into()))
}

fn setup(kwargs: &Value) -> (bool, String) {
    (kwargs["force"] == true, "SETUP_NEEDS_FORCE".into())
}

#[add_plugin_argument("--loud", action = "store_true", help = "Shout.")]
#[make_action]
fn greet(action: Vec<String>, kwargs: &Value) -> (bool, String) {
    let name = action.first().cloned().unwrap_or_else(|| "world".into());
    let mut text = format!("Hello, {name}!");
    if kwargs["loud"] == true {
        text = text.to_uppercase();
    }
    (true, text.repeat(kwargs["times"].as_u64().unwrap_or(0) as usize))
}
"#;

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}

impl Home {
    fn new() -> Home {
        let home = Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        };
        std::fs::write(home.plugins.path().join("typed.rs"), SOURCE).unwrap();
        let builds = home.root.path().join("cache/native_build");
        let found = meerschaum_plugin::discovery::discover_report_in(
            &[home.plugins.path().into()],
            &builds,
        );
        assert!(found.skipped.is_empty(), "{:?}", found.skipped);
        let candidate = &found.candidates[0];
        assert_eq!(candidate.version, "1.2.3");
        compile_single_file::compile(candidate);
        std::fs::write(
            builds.join("typed/mrsm-source-fingerprint"),
            &candidate.fingerprint,
        )
        .unwrap();
        home
    }

    fn run(&self, args: &[&str]) -> (bool, String) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env(
                "MRSM_CONFIG",
                r#"{"formatting":{"ansi":false,"unicode":false}}"#,
            )
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let start = Instant::now();
        while child.try_wait().unwrap().is_none() {
            if start.elapsed() > Duration::from_secs(120) {
                child.kill().unwrap();
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let output: Output = child.wait_with_output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !text.contains("Building native plugin"),
            "the fixture build must be reused: {text}"
        );
        (output.status.success(), text)
    }
}

fn position(text: &str, needle: &str) -> usize {
    text.find(needle)
        .unwrap_or_else(|| panic!("missing {needle:?} in:\n{text}"))
}

#[test]
fn hook_results_are_printed_like_python_hooks_and_never_fail_the_sync() {
    let home = Home::new();
    let (ok, out) = home.run(&[
        "register",
        "pipe",
        "-c",
        "plugin:typed",
        "-m",
        "reading",
        "-i",
        "sql:local",
        "--noask",
        "--params",
        r#"{"columns":{"datetime":"id","id":"id"},"dtypes":{"id":"int"}}"#,
    ]);
    assert!(ok, "{out}");
    let (ok, out) = home.run(&[
        "sync",
        "pipes",
        "-c",
        "plugin:typed",
        "-m",
        "reading",
        "-i",
        "sql:local",
    ]);
    assert!(ok, "{out}");
    assert!(out.contains("Inserted 1, updated 0 rows."), "{out}");
    // PARITY: every hook's tuple is printed in order, successful or not, and a hook that
    // raised is also a warning (v4 `actions/sync.py`).
    let pre = position(&out, "HOOK_PRE reading pipe_kwarg=false synced=false");
    let post = position(&out, "HOOK_POST_OK");
    let failed = position(&out, "HOOK_POST_FAILED");
    let raised = position(
        &out,
        "Failed to execute sync hook 'raising' from plugin 'typed':",
    );
    assert!(pre < post && post < failed && failed < raised, "{out}");
    assert!(out.contains("HOOK_RAISED"), "{out}");
    assert!(!out.contains("Native plugin 'typed' hook failed"), "{out}");
}

#[test]
fn plugin_arguments_reach_the_action_and_setup_follows_python_results() {
    let home = Home::new();
    let (ok, out) = home.run(&["greet", "Alice", "--station", "x"]);
    assert!(ok, "{out}");
    assert!(out.contains("Hello, Alice!"), "{out}");
    assert!(!out.contains("Hello, Alice!Hello"), "{out}");
    // PARITY: `-t` is the built-in `--tags`; v4's `add_plugin_argument` warns and skips it.
    assert!(
        out.contains("argument --tagged/-t: conflicting option string: -t (plugin 'typed')"),
        "{out}"
    );
    let (ok, out) = home.run(&[
        "greet",
        "Alice",
        "--loud",
        "-n",
        "2",
        "--direction",
        "north",
        "--station",
        "x",
    ]);
    assert!(ok, "{out}");
    assert!(out.contains("HELLO, ALICE!HELLO, ALICE!"), "{out}");

    // argparse's checks: `type`, `choices`, and `required` for the plugin's own action.
    for (args, message) in [
        (
            &["greet", "--station", "x", "--times", "abc"][..],
            "argument --times/-n: invalid int value: 'abc'",
        ),
        (
            &["greet", "--station", "x", "--direction", "west"][..],
            "argument --direction: invalid choice: 'west' (choose from 'north', 'south')",
        ),
        (
            &["greet", "Alice"][..],
            "the following arguments are required: --station",
        ),
    ] {
        let (ok, out) = home.run(args);
        assert!(!ok, "{out}");
        assert!(out.contains(message), "{message:?} in:\n{out}");
        assert!(!out.contains("Hello"), "{out}");
    }

    let (ok, out) = home.run(&["setup", "plugins", "typed", "--force"]);
    assert!(ok, "{out}");
    assert!(out.contains("(1 succeeded, 0 failed)"), "{out}");
    let (_, out) = home.run(&["setup", "plugins", "typed"]);
    assert!(out.contains("SETUP_NEEDS_FORCE"), "{out}");
    assert!(out.contains("Failed to set up plugin 'typed'."), "{out}");

    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(home.root.path().join("cache/native_plugins_manifest.json")).unwrap(),
    )
    .unwrap();
    let entry = &manifest["plugins"]["typed"];
    assert_eq!(entry["version"], "1.2.3");
    assert_eq!(
        entry["data_functions"],
        serde_json::json!(["fetch", "setup"])
    );
}
