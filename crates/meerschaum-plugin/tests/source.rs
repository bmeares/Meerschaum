//! Source plugins (Cargo crates built by Meerschaum): discovery, the build stamp, rebuild on a
//! source change, skip messages and the manifest entry. The tests run a stand-in `cargo`
//! script that installs the prebuilt `native_demo` example where Cargo would put it, so they
//! stay fast; `crates/meerschaum-cli/tests/native_source_plugin.rs` runs the real `cargo`.
#![cfg(unix)]

use meerschaum_plugin::discovery::{self, Discovery};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

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

struct Fixture {
    root: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Fixture {
        Fixture {
            root: tempfile::tempdir().unwrap(),
        }
    }

    fn plugins(&self) -> PathBuf {
        self.root.path().join("plugins")
    }

    fn build_dir(&self) -> PathBuf {
        self.root.path().join("cache/native_build")
    }

    fn cache(&self) -> PathBuf {
        self.root.path().join("cache/native_plugins_manifest.json")
    }

    /// A source crate `<plugins>/<dir>` with the given `[package] name`.
    fn crate_dir(&self, dir: &str, package: &str, cdylib: bool) -> PathBuf {
        let path = self.plugins().join(dir);
        std::fs::create_dir_all(path.join("src")).unwrap();
        std::fs::write(
            path.join("plugin.toml"),
            "name = \"native_demo\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();
        let lib = if cdylib {
            "[lib]\ncrate-type = [\"cdylib\"]\n"
        } else {
            ""
        };
        std::fs::write(
            path.join("Cargo.toml"),
            format!("[package]\nname = \"{package}\"\nversion = \"1.0.0\"\n{lib}"),
        )
        .unwrap();
        std::fs::write(path.join("src/lib.rs"), "// v1\n").unwrap();
        path
    }

    /// A stand-in cargo that logs its arguments and installs the demo library (or fails).
    fn fake_cargo(&self, fail: bool) -> PathBuf {
        let script = self
            .root
            .path()
            .join(if fail { "cargo-fail" } else { "cargo-ok" });
        let log = self.root.path().join("cargo.log");
        let body = if fail {
            "echo 'Compiling native_demo'; for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100; do echo \"noise $i\" >&2; done; echo 'error[E0425]: cannot find value `boom`' >&2; exit 101".to_owned()
        } else {
            format!(
                "td=''\nwhile [ $# -gt 0 ]; do if [ \"$1\" = --target-dir ]; then td=\"$2\"; fi; if [ \"$1\" = --manifest-path ]; then mp=\"$2\"; fi; shift; done\n# Like cargo, resolve and write Cargo.lock next to the manifest.\necho '# resolved' > \"$(dirname \"$mp\")/Cargo.lock\"\nmkdir -p \"$td/release\"\ncp '{}' \"$td/release/libnative_demo.so\"\n",
                demo_library().display()
            )
        };
        let text = format!(
            "#!/bin/sh\necho \"$CARGO_INCREMENTAL $@\" >> '{}'\n{body}\n",
            log.display()
        );
        std::fs::write(&script, text).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        script
    }

    fn builds(&self) -> usize {
        std::fs::read_to_string(self.root.path().join("cargo.log"))
            .map(|log| log.lines().count())
            .unwrap_or(0)
    }

    fn scan(&self, cargo: Option<&Path>) -> Discovery {
        let mut found = discovery::discover_report_in(&[self.plugins()], &self.build_dir());
        if let (Some(cargo), Some(candidate)) = (cargo, found.candidates.first_mut())
            && let Some(build) = candidate.build.as_mut()
        {
            build.cargo = cargo.to_path_buf();
        }
        found
    }
}

#[test]
fn source_plugin_builds_once_and_rebuilds_when_the_sources_change() {
    let fx = Fixture::new();
    let dir = fx.crate_dir("native_demo", "native_demo", true);
    let cargo = fx.fake_cargo(false);

    let mut first = fx.scan(Some(&cargo));
    assert!(first.skipped.is_empty(), "{:?}", first.skipped);
    let candidate = &first.candidates[0];
    assert!(candidate.source_crate);
    let spec = candidate.build.as_ref().expect("needs a build");
    assert_eq!(spec.manifest_path, dir.join("Cargo.toml"));
    assert_eq!(spec.target_dir, fx.build_dir().join("native_demo"));
    assert!(
        candidate
            .library
            .starts_with(fx.build_dir().join("native_demo/release"))
    );
    assert!(!candidate.library.exists());
    first.build_pending();
    assert!(first.skipped.is_empty(), "{:?}", first.skipped);
    let library = first.candidates[0].library.clone();
    assert!(library.is_file());
    assert!(!dir.join(discovery::library_name("native_demo")).exists());
    assert_eq!(fx.builds(), 1);
    let log = std::fs::read_to_string(fx.root.path().join("cargo.log")).unwrap();
    assert!(log.contains("0 build --release --manifest-path"), "{log}");
    assert!(log.contains("--target-dir"), "{log}");

    // Current: no build is scheduled and cargo is not run again.
    let mut second = fx.scan(Some(&cargo));
    assert!(second.candidates[0].build.is_none());
    second.build_pending();
    assert_eq!(fx.builds(), 1);
    // The lock cargo wrote during the build is part of the stamped fingerprint: no rebuild.
    assert!(dir.join("Cargo.lock").is_file());
    assert_eq!(
        second.candidates[0].fingerprint,
        first.candidates[0].fingerprint
    );

    // The manifest records the built library path and keys on the source fingerprint.
    // SAFETY: the library is a copy of the trusted SDK example.
    let (manifest, skipped) =
        unsafe { discovery::cached_manifest_report(&[fx.plugins()], &fx.cache()) }.unwrap();
    assert!(skipped.is_empty(), "{skipped:?}");
    assert_eq!(fx.builds(), 1, "a current build must not run cargo");
    assert_eq!(
        manifest["plugins"]["native_demo"]["library"],
        library.to_string_lossy().as_ref()
    );
    assert_eq!(
        manifest["plugins"]["native_demo"]["path"],
        dir.to_string_lossy().as_ref()
    );

    // Touching a file without changing it is not a source change.
    let src = dir.join("src/lib.rs");
    std::fs::write(&src, "// v1\n").unwrap();
    assert!(fx.scan(Some(&cargo)).candidates[0].build.is_none());

    // A file outside src/ (an include_str! asset, a build.rs input) is part of the fingerprint.
    std::fs::create_dir_all(dir.join("assets")).unwrap();
    std::fs::write(dir.join("assets/data.txt"), "1").unwrap();
    let mut asset = fx.scan(Some(&cargo));
    assert!(asset.candidates[0].build.is_some());
    asset.build_pending();
    assert_eq!(fx.builds(), 2);
    std::fs::write(dir.join("assets/data.txt"), "2").unwrap();
    let mut asset = fx.scan(Some(&cargo));
    assert!(asset.candidates[0].build.is_some());
    asset.build_pending();
    assert_eq!(fx.builds(), 3);
    assert!(fx.scan(Some(&cargo)).candidates[0].build.is_none());

    // Editing src/ changes the fingerprint and schedules a rebuild.
    std::fs::write(&src, "// v2\n").unwrap();
    let mut third = fx.scan(Some(&cargo));
    assert!(third.candidates[0].build.is_some());
    assert_ne!(
        third.candidates[0].fingerprint,
        first.candidates[0].fingerprint
    );
    third.build_pending();
    assert_eq!(fx.builds(), 4);
    assert!(fx.scan(Some(&cargo)).candidates[0].build.is_none());

    // A deleted build directory is rebuilt even though the sources are unchanged.
    std::fs::remove_dir_all(fx.build_dir()).unwrap();
    assert!(fx.scan(Some(&cargo)).candidates[0].build.is_some());
}

#[test]
fn mismatched_crate_name_and_non_cdylib_are_skipped() {
    let fx = Fixture::new();
    fx.crate_dir("a_wrong_name", "something_else", true);
    fx.crate_dir("b_rlib", "native_demo", false);
    let found = fx.scan(None);
    assert!(found.candidates.is_empty());
    assert_eq!(found.skipped.len(), 2);
    let first = found.skipped[0].message();
    assert!(
        first.starts_with("Skipping invalid native plugin at ")
            && first.contains("package name 'something_else' does not match"),
        "{first}"
    );
    assert!(
        found.skipped[1].reason.contains("cdylib"),
        "{:?}",
        found.skipped[1]
    );
    assert_eq!(fx.builds(), 0);
}

#[test]
fn failed_build_prints_the_cargo_tail_and_leaves_other_plugins_loading() {
    let fx = Fixture::new();
    fx.crate_dir("a_broken", "native_demo", true);
    // A prebuilt plugin of another name after the broken source crate.
    let good = fx.plugins().join("z_prebuilt");
    std::fs::create_dir_all(&good).unwrap();
    std::fs::write(
        good.join("plugin.toml"),
        "name = \"prebuilt\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();
    let mut found = fx.scan(Some(&fx.fake_cargo(true)));
    assert_eq!(found.candidates.len(), 1);
    found.build_pending();
    assert!(found.candidates.is_empty());
    // The prebuilt plugin has no library, so it is invalid too; the broken one is a source skip.
    let broken = found
        .skipped
        .iter()
        .find(|s| s.directory.ends_with("a_broken"))
        .expect("broken skipped");
    assert!(broken.source_crate);
    let message = broken.message();
    assert!(
        message.starts_with("Skipping native plugin at "),
        "{message}"
    );
    assert!(
        message.contains("error[E0425]: cannot find value `boom`"),
        "{message}"
    );
    assert!(message.contains("noise 100"), "{message}");
    assert!(
        !message.contains("noise 1\n"),
        "only the tail is kept: {message}"
    );
}

#[test]
fn shipped_library_wins_unless_the_sources_are_newer() {
    let fx = Fixture::new();
    let dir = fx.crate_dir("native_demo", "native_demo", true);
    let library = dir.join(discovery::library_name("native_demo"));
    std::fs::copy(demo_library(), &library).unwrap();
    // Library newer than every source file: prebuilt, no build.
    let future = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);
    std::fs::File::options()
        .write(true)
        .open(&library)
        .unwrap()
        .set_modified(future)
        .unwrap();
    let found = fx.scan(None);
    assert_eq!(found.candidates[0].library, library);
    assert!(!found.candidates[0].source_crate && found.candidates[0].build.is_none());
    // A source file newer than the shipped library: build it.
    let past = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
    std::fs::File::options()
        .write(true)
        .open(&library)
        .unwrap()
        .set_modified(past)
        .unwrap();
    let found = fx.scan(Some(&fx.fake_cargo(false)));
    assert!(found.candidates[0].source_crate);
    assert!(found.candidates[0].build.is_some());
}

#[test]
fn a_failed_build_is_remembered_until_the_sources_change_or_the_build_dir_is_removed() {
    let fx = Fixture::new();
    let dir = fx.crate_dir("native_demo", "native_demo", true);
    let failing = fx.fake_cargo(true);
    let working = fx.fake_cargo(false);

    let mut first = fx.scan(Some(&failing));
    first.build_pending();
    assert!(first.candidates.is_empty());
    assert_eq!(fx.builds(), 1);

    // The next run repeats the saved compiler diagnostic without invoking cargo.
    let mut second = fx.scan(Some(&working));
    assert!(second.candidates.is_empty());
    second.build_pending();
    assert_eq!(fx.builds(), 1, "a known-broken source must not rebuild");
    let message = second.skipped[0].message();
    assert!(
        message.starts_with("Skipping native plugin at ")
            && message.contains("the last build failed; edit the sources or delete '")
            && message.contains("to retry")
            && message.contains("error[E0425]"),
        "{message}"
    );
    assert!(message.contains(&fx.build_dir().join("native_demo").display().to_string()));

    // Editing the sources retries (and a working cargo builds them).
    std::fs::write(dir.join("src/lib.rs"), "// fixed\n").unwrap();
    let mut third = fx.scan(Some(&working));
    assert!(third.skipped.is_empty(), "{:?}", third.skipped);
    third.build_pending();
    assert_eq!(fx.builds(), 2);
    assert!(third.candidates[0].library.is_file());

    // Removing the build directory also clears a remembered failure.
    std::fs::write(dir.join("src/lib.rs"), "// broken again\n").unwrap();
    let mut fourth = fx.scan(Some(&failing));
    fourth.build_pending();
    assert_eq!(fx.builds(), 3);
    assert!(fx.scan(Some(&working)).candidates.is_empty());
    std::fs::remove_dir_all(fx.build_dir()).unwrap();
    let retry = fx.scan(Some(&working));
    assert!(retry.skipped.is_empty() && retry.candidates[0].build.is_some());
}
