//! Native (C-ABI) plugins through the `mrsm` binary: discovery in `MRSM_PLUGINS_DIR`, manifest
//! merge, actions, flags, and a sync from a native `plugin:` source into a SQLite instance.
//! The plugin is the SDK example (`crates/meerschaum-plugin/examples/native_demo.rs`).

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;

fn demo_library() -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let status = Command::new(env!("CARGO"))
            .args([
                "build",
                "-p",
                "meerschaum-plugin",
                "--example",
                "native_demo",
            ])
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", plugins.path())
            .status()
            .unwrap();
        assert!(status.success());
    });
    // The example lands in the shared target directory next to this test's executable.
    let executable = std::env::current_exe().unwrap();
    let profile = executable.parent().unwrap().parent().unwrap();
    profile.join("examples").join(format!(
        "{}native_demo.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}

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
        let dir = home.plugins.path().join("native_demo");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("plugin.toml"),
            include_str!("../../meerschaum-plugin/examples/plugin.toml"),
        )
        .unwrap();
        std::fs::copy(
            demo_library(),
            dir.join(meerschaum_plugin::discovery::library_name("native_demo")),
        )
        .unwrap();
        home
    }

    fn run(&self, args: &[&str]) -> Output {
        run_in(self.root.path(), self.plugins.path(), args)
    }
}

fn run_in(root: &Path, plugins: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_mrsm"))
        .args(args)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", root)
        .env("MRSM_ROOT_DIR", root)
        .env("MRSM_PLUGINS_DIR", plugins)
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .output()
        .unwrap()
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn native_action_dispatches_and_is_listed() {
    let home = Home::new();
    let output = home.run(&["hello"]);
    let out = text(&output);
    assert!(out.contains("Hello from native_demo"), "{out}");
    assert!(output.status.success(), "{out}");

    let output = home.run(&["show", "actions", "--nopretty"]);
    let out = text(&output);
    assert!(out.lines().any(|l| l.trim() == "hello"), "{out}");
    assert!(out.lines().any(|l| l.trim() == "panic"), "{out}");
}

#[test]
fn show_plugins_lists_native_plugins_with_a_marker() {
    // No Python plugin here: any `.py` file would start the Python worker.
    let home = Home::new();
    let pretty = text(&home.run(&["show", "plugins"]));
    assert!(pretty.contains("native_demo (native)"), "{pretty}");
    // `--nopretty` keeps bare names for scripts.
    let plain =
        String::from_utf8_lossy(&home.run(&["show", "plugins", "--nopretty"]).stdout).into_owned();
    assert_eq!(plain, "native_demo\n");
}

#[test]
fn native_action_panic_is_contained() {
    let home = Home::new();
    let output = home.run(&["panic"]);
    let out = text(&output);
    assert!(!output.status.success(), "{out}");
    assert!(out.contains("panicked"), "{out}");
    // The process survives the plugin's panic and keeps serving actions.
    assert!(text(&home.run(&["hello"])).contains("Hello from native_demo"));
}

#[test]
fn native_flag_is_parsed_from_sysargs() {
    let home = Home::new();
    // `echo`-style flags reach the plugin through the action kwargs; the demo greets regardless,
    // so assert the manifest-declared flag is accepted rather than rejected.
    let output = home.run(&["hello", "--native-demo"]);
    assert!(output.status.success(), "{}", text(&output));
}

#[test]
fn native_plugin_sync_runs_through_the_engine() {
    let home = Home::new();
    let registered = home.run(&[
        "register",
        "pipe",
        "-c",
        "plugin:native_demo",
        "-m",
        "demo",
        "-i",
        "sql:local",
        "--noask",
        "--nopretty",
    ]);
    assert!(registered.status.success(), "{}", text(&registered));
    let synced = home.run(&[
        "sync",
        "pipes",
        "-c",
        "plugin:native_demo",
        "-m",
        "demo",
        "-i",
        "sql:local",
        "--nopretty",
    ]);
    let out = text(&synced);
    assert!(synced.status.success(), "{out}");
    // PARITY: a plugin that defines `sync()` runs it instead of fetching (the demo's `sync()`
    // keeps the frame in its own in-memory store), so the engine reports its success tuple.
    assert!(out.contains("Success"), "{out}");
}

#[test]
fn empty_plugins_dir_is_unaffected() {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let output = run_in(
        root.path(),
        plugins.path(),
        &["show", "actions", "--nopretty"],
    );
    assert!(output.status.success(), "{}", text(&output));
    assert!(!text(&output).lines().any(|l| l.trim() == "hello"));
}
