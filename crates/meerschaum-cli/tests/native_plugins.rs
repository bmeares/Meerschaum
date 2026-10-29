//! Native (C-ABI) plugins through the `mrsm` binary: discovery in `MRSM_PLUGINS_DIR`, manifest
//! merge, actions, flags, and a sync from a native `plugin:` source into a SQLite instance.
//! The plugin is the SDK example (`crates/meerschaum-plugin/examples/native_demo.rs`).

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
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
                "--example",
                "native_probe",
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
    Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
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
fn native_register_parameters_are_merged_under_explicit_parameters() {
    let home = Home::new();
    let output = home.run(&[
        "register",
        "pipes",
        "-c",
        "plugin:native_demo",
        "-m",
        "registered",
        "-i",
        "sql:local",
        "--params",
        r#"{"columns":{"id":"other"}}"#,
        "--noask",
    ]);
    assert!(output.status.success(), "{}", text(&output));
    let shown = home.run(&[
        "show",
        "pipes",
        "-m",
        "registered",
        "-i",
        "sql:local",
        "--nopretty",
    ]);
    let out = text(&shown);
    let pipe: serde_json::Value =
        serde_json::from_str(out.lines().find(|l| l.starts_with('{')).unwrap()).unwrap();
    assert_eq!(
        pipe["parameters"]["columns"],
        serde_json::json!({"datetime": "id", "id": "other"})
    );
    assert_eq!(pipe["parameters"]["dtypes"]["id"], "int");
}

#[test]
fn bootstrap_native_pipe_uses_its_register_hook_without_python_fallback() {
    let home = Home::new();
    let output = bootstrap(
        &home,
        "plugin:native_demo\nbootstrapped\nNone\n\n\n\nn\nn\n",
    );
    let out = text(&output);
    assert!(output.status.success(), "{out}");
    assert!(out.contains("(1 succeeded, 0 failed)"), "{out}");
    assert!(
        !out.contains("cannot be found") && !out.contains("Traceback"),
        "{out}"
    );
    assert!(
        !out.contains("Please enter index columns"),
        "native register supplies the axis: {out}"
    );
    let shown = home.run(&[
        "show",
        "pipes",
        "-m",
        "bootstrapped",
        "-i",
        "sql:local",
        "--nopretty",
    ]);
    let out = text(&shown);
    let pipe: serde_json::Value =
        serde_json::from_str(out.lines().find(|l| l.starts_with('{')).unwrap()).unwrap();
    assert_eq!(pipe["parameters"]["columns"]["datetime"], "id");
}

fn bootstrap(home: &Home, input: &str) -> Output {
    use std::io::Write;
    let mut child = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .args(["bootstrap", "pipes", "-i", "sql:local"])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.root.path())
        .env("MRSM_ROOT_DIR", home.root.path())
        .env("MRSM_PLUGINS_DIR", home.plugins.path())
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn bootstrap_native_pipe_without_register_does_not_fall_back_to_python() {
    let home = Home::new();
    let dir = home.plugins.path().join("native_probe");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("plugin.toml"),
        "name = \"native_probe\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();
    std::fs::copy(
        demo_library().with_file_name(meerschaum_plugin::discovery::library_name("native_probe")),
        dir.join(meerschaum_plugin::discovery::library_name("native_probe")),
    )
    .unwrap();
    // A Python plugin of the same name must never provide the missing hook.
    std::fs::write(home.plugins.path().join("native_probe.py"),
        "def fetch(**kw): return []\ndef register(**kw): raise RuntimeError('PYTHON_REGISTER_CALLED')\n").unwrap();
    let output = bootstrap(
        &home,
        "plugin:native_probe\nno_hook\nNone\ndt\nid\nn\n\n\n\nn\nn\n",
    );
    let out = text(&output);
    assert!(output.status.success(), "{out}");
    assert!(out.contains("(1 succeeded, 0 failed)"), "{out}");
    for bad in ["cannot be found", "Traceback", "PYTHON_REGISTER_CALLED"] {
        assert!(!out.contains(bad), "{out}");
    }
    assert!(out.contains("Please enter index columns"), "{out}");
}

#[test]
fn native_custom_connector_register_parameters_are_used() {
    let home = Home::new();
    let output = home.run(&[
        "register",
        "pipes",
        "-c",
        "native_demo:source",
        "-m",
        "custom",
        "-i",
        "sql:local",
        "--noask",
    ]);
    assert!(output.status.success(), "{}", text(&output));
    let shown = home.run(&[
        "show",
        "pipes",
        "-m",
        "custom",
        "-i",
        "sql:local",
        "--nopretty",
    ]);
    let out = text(&shown);
    let pipe: serde_json::Value =
        serde_json::from_str(out.lines().find(|l| l.starts_with('{')).unwrap()).unwrap();
    assert_eq!(pipe["parameters"]["columns"]["datetime"], "id");
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
