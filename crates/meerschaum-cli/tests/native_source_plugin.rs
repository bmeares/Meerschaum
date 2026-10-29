//! Rust source plugins through the `mrsm` binary: a plugin directory that is a Cargo crate is
//! built by Meerschaum (`cargo build --release` into `<root>/cache/native_build/<name>`), the
//! build is reused until the sources change, and a missing toolchain or a mismatched crate is
//! skipped without stopping the other plugins. The crate is the SDK example
//! (`crates/meerschaum-plugin/examples/native_demo.rs`) copied into a temporary plugins directory.
//! The first build compiles the SDK's dependency tree and takes minutes. Every run uses a
//! temporary root and plugins directory and a scrubbed environment.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;

const DEMO_SOURCE: &str = include_str!("../../meerschaum-plugin/examples/native_demo.rs");
const DEMO_DESCRIPTOR: &str = include_str!("../../meerschaum-plugin/examples/plugin.toml");

fn plugin_crate_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../meerschaum-plugin")
}

/// A plugin crate named `package` in `<plugins>/<dir>`; `name` is the plugin.toml name.
fn write_source_plugin(plugins: &Path, dir: &str, name: &str, package: &str, source: &str) {
    let path = plugins.join(dir);
    std::fs::create_dir_all(path.join("src")).unwrap();
    std::fs::write(
        path.join("plugin.toml"),
        DEMO_DESCRIPTOR.replace("native_demo", name),
    )
    .unwrap();
    // Unoptimized: only the plugin's own code, the SDK's dependencies are compiled either way.
    std::fs::write(
        path.join("Cargo.toml"),
        format!(
            "[package]\nname = \"{package}\"\nversion = \"1.0.0\"\nedition = \"2024\"\n\n\
             [lib]\ncrate-type = [\"cdylib\"]\n\n\
             [dependencies]\nmeerschaum-plugin = {{ path = {:?} }}\n\n\
             [profile.release]\nopt-level = 0\ndebug = 0\npanic = \"unwind\"\n\n[workspace]\n",
            plugin_crate_path().display().to_string()
        ),
    )
    .unwrap();
    // Resolve with the workspace's pins so the build needs no registry index update.
    let lock = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock");
    if let Ok(lock) = std::fs::read(lock) {
        std::fs::write(path.join("Cargo.lock"), lock).unwrap();
    }
    std::fs::write(path.join("src/lib.rs"), source).unwrap();
}

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
        Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
        }
    }

    fn built_library(&self) -> PathBuf {
        self.root
            .path()
            .join("cache/native_build/native_demo/release")
            .join(format!(
                "{}native_demo.{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_EXTENSION
            ))
    }

    /// Run `mrsm` with `PATH` as the parent's (so `cargo` is found) unless `path` is given.
    fn run_with(&self, args: &[&str], path: Option<&Path>, cargo: Option<&Path>) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .args(args)
            .env_clear()
            .env(
                "PATH",
                path.map(Path::as_os_str)
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| std::env::var_os("PATH").unwrap_or_default()),
            )
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_NO_UPDATE_CHECK", "1");
        // The rustup shim and the registry cache live in the real home directory.
        let real_home = std::env::var_os("HOME").map(PathBuf::from);
        for (var, default) in [("RUSTUP_HOME", ".rustup"), ("CARGO_HOME", ".cargo")] {
            let value = std::env::var_os(var)
                .or_else(|| real_home.as_ref().map(|h| h.join(default).into_os_string()));
            if let Some(value) = value {
                command.env(var, value);
            }
        }
        if let Some(cargo) = cargo {
            command.env("CARGO", cargo);
        }
        command.output().unwrap()
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_with(args, None, None)
    }
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

const BUILDING: &str = "Building native plugin 'native_demo' (cargo build --release)...";

#[test]
fn source_plugin_is_built_cached_and_rebuilt_on_change() {
    let home = Home::new();
    write_source_plugin(
        home.plugins.path(),
        "native_demo",
        "native_demo",
        "native_demo",
        DEMO_SOURCE,
    );

    // First run: build, list as native, run the action.
    let output = home.run(&["show", "plugins"]);
    let out = text(&output);
    assert!(output.status.success(), "{out}");
    assert!(stderr(&output).contains(BUILDING), "{out}");
    assert!(out.contains("native_demo (native)"), "{out}");
    let library = home.built_library();
    assert!(library.is_file(), "{out}");
    // The plugins directory stays clean: no library, no target directory.
    let listing: Vec<_> = std::fs::read_dir(home.plugins.path().join("native_demo"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        listing
            .iter()
            .all(|n| ["Cargo.lock", "Cargo.toml", "plugin.toml", "src"].contains(&n.as_str())),
        "{listing:?}"
    );
    let manifest =
        std::fs::read_to_string(home.root.path().join("cache/native_plugins_manifest.json"))
            .unwrap();
    let manifest: serde_json::Value = serde_json::from_str(&manifest).unwrap();
    assert_eq!(
        std::fs::canonicalize(
            manifest["plugins"]["native_demo"]["library"]
                .as_str()
                .unwrap()
        )
        .unwrap(),
        std::fs::canonicalize(&library).unwrap()
    );
    let built_at = std::fs::metadata(&library).unwrap().modified().unwrap();

    let output = home.run(&["hello"]);
    let out = text(&output);
    assert!(out.contains("Hello from native_demo"), "{out}");
    assert!(!stderr(&output).contains("Building native plugin"), "{out}");
    // Completion and help read the manifest cache and build nothing.
    let output = home.run(&["--help"]);
    assert!(!stderr(&output).contains("Building native plugin"));
    assert_eq!(
        std::fs::metadata(&library).unwrap().modified().unwrap(),
        built_at,
        "a second run must not rebuild"
    );

    // A source edit triggers exactly one rebuild.
    let source = home.plugins.path().join("native_demo/src/lib.rs");
    std::fs::write(&source, format!("{DEMO_SOURCE}\n// edited\n")).unwrap();
    let output = home.run(&["hello"]);
    let out = text(&output);
    assert!(stderr(&output).contains(BUILDING), "{out}");
    assert!(out.contains("Hello from native_demo"), "{out}");
    let rebuilt_at = std::fs::metadata(&library).unwrap().modified().unwrap();
    assert!(rebuilt_at > built_at, "the edit must rebuild the library");
    let output = home.run(&["hello"]);
    assert!(!stderr(&output).contains("Building native plugin"));
    assert_eq!(
        std::fs::metadata(&library).unwrap().modified().unwrap(),
        rebuilt_at
    );
}

#[test]
fn crate_with_a_mismatched_name_is_skipped() {
    let home = Home::new();
    write_source_plugin(
        home.plugins.path(),
        "native_demo",
        "native_demo",
        "another_name",
        DEMO_SOURCE,
    );
    let output = home.run(&["show", "plugins"]);
    let err = stderr(&output);
    assert!(output.status.success(), "{err}");
    assert!(
        err.contains("Skipping invalid native plugin at")
            && err.contains("package name 'another_name' does not match"),
        "{err}"
    );
    assert!(!err.contains("Building native plugin"), "{err}");
    assert!(!text(&output).contains("native_demo (native)"));
}

#[test]
fn without_cargo_the_source_plugin_is_skipped_and_prebuilt_plugins_load() {
    let home = Home::new();
    write_source_plugin(
        home.plugins.path(),
        "a_source_only",
        "source_only",
        "source_only",
        DEMO_SOURCE,
    );
    let prebuilt = home.plugins.path().join("native_demo");
    std::fs::create_dir_all(&prebuilt).unwrap();
    std::fs::write(prebuilt.join("plugin.toml"), DEMO_DESCRIPTOR).unwrap();
    std::fs::copy(
        demo_library(),
        prebuilt.join(meerschaum_plugin::discovery::library_name("native_demo")),
    )
    .unwrap();

    let no_cargo = tempfile::tempdir().unwrap();
    let output = home.run_with(&["show", "plugins"], Some(no_cargo.path()), None);
    let out = text(&output);
    assert!(output.status.success(), "{out}");
    let err = stderr(&output);
    let library = meerschaum_plugin::discovery::library_name("source_only");
    assert!(
        err.contains("Warning: Skipping native plugin at '")
            && err.contains(
                "it is a source crate and no `cargo` was found on PATH (install a Rust \
                 toolchain or ship "
            )
            && err.contains(&library),
        "{err}"
    );
    assert!(out.contains("native_demo (native)"), "{out}");
    assert!(!out.contains("source_only (native)"), "{out}");
    assert!(!home.root.path().join("cache/native_build").exists());
    let output = home.run_with(&["hello"], Some(no_cargo.path()), None);
    assert!(
        text(&output).contains("Hello from native_demo"),
        "{}",
        text(&output)
    );

    // `CARGO` names the toolchain; a path that does not exist counts as no cargo.
    let output = home.run_with(
        &["show", "plugins"],
        None,
        Some(&no_cargo.path().join("missing-cargo")),
    );
    assert!(
        stderr(&output).contains("no `cargo` was found on PATH"),
        "{}",
        text(&output)
    );
}

#[test]
fn a_stale_shipped_library_loads_with_a_warning_when_there_is_no_cargo() {
    let home = Home::new();
    write_source_plugin(
        home.plugins.path(),
        "native_demo",
        "native_demo",
        "native_demo",
        DEMO_SOURCE,
    );
    let library = home
        .plugins
        .path()
        .join("native_demo")
        .join(meerschaum_plugin::discovery::library_name("native_demo"));
    std::fs::copy(demo_library(), &library).unwrap();
    // The shipped library is older than the sources.
    std::fs::File::options()
        .write(true)
        .open(&library)
        .unwrap()
        .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(3600))
        .unwrap();

    let no_cargo = tempfile::tempdir().unwrap();
    let output = home.run_with(&["hello"], Some(no_cargo.path()), None);
    let out = text(&output);
    assert!(out.contains("Hello from native_demo"), "{out}");
    let err = stderr(&output);
    assert!(
        err.contains("Warning: Using native plugin at '")
            && err.contains("its library is older than the sources and was not rebuilt"),
        "{err}"
    );
    assert!(!home.root.path().join("cache/native_build").exists());
}
