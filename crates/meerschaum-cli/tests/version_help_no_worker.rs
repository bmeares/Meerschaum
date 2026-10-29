//! `mrsm --version` and `mrsm --help` never start a Python worker (wave 18, decision 5).
//!
//! The plugins directory holds a plugin, so validating the plugin trees would start the
//! interpreter. `MRSM_PYTHON` is a script which records every start; a control command which does
//! validate the trees proves that the script would have been noticed.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_mrsm-cli");

struct Fixture {
    _dir: tempfile::TempDir,
    root: std::path::PathBuf,
    plugins: std::path::PathBuf,
    python: std::path::PathBuf,
    marker: std::path::PathBuf,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    let plugins = dir.path().join("plugins");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&plugins).unwrap();
    std::fs::write(plugins.join("w18sh.py"), "__version__ = '1.0.0'\n").unwrap();
    let marker = dir.path().join("python-started");
    let python = dir.path().join("fake-python");
    std::fs::write(
        &python,
        format!(
            "#!/bin/sh\necho started >> '{}'\nexit 1\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&python, std::fs::Permissions::from_mode(0o755)).unwrap();
    Fixture {
        _dir: dir,
        root,
        plugins,
        python,
        marker,
    }
}

fn run(f: &Fixture, args: &[&str]) -> std::process::Output {
    let mut command = Command::new(BIN);
    command
        .env_clear()
        .args(args)
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", &f.root)
        .env("MRSM_ROOT_DIR", &f.root)
        .env("MRSM_PLUGINS_DIR", &f.plugins)
        .env("MRSM_PYTHON", &f.python)
        .env("MRSM_NO_AUTO_INSTALL", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1");
    command.current_dir(Path::new(&f.root)).output().unwrap()
}

fn started(f: &Fixture) -> bool {
    f.marker.exists()
}

#[test]
fn version_starts_no_worker() {
    let f = fixture();
    for flag in ["--version", "-V"] {
        let out = run(&f, &[flag]);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.starts_with("Meerschaum v"), "{stdout}");
        assert!(
            !String::from_utf8_lossy(&out.stderr).contains("version"),
            "no version-mismatch warning: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let out = run(&f, &["--version", "--nopretty"]);
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        meerschaum_cli::VERSION
    );
    assert!(!started(&f), "`--version` started the Python interpreter");
}

#[test]
fn help_starts_no_worker() {
    let f = fixture();
    for args in [
        vec!["--help"],
        vec!["-h"],
        vec!["sync", "pipes", "--help"],
        vec!["show", "--help"],
        vec!["help"],
        vec!["help", "sync", "pipes"],
    ] {
        let out = run(&f, &args);
        assert!(
            out.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!out.stdout.is_empty(), "{args:?} printed nothing");
    }
    assert!(!started(&f), "`--help` started the Python interpreter");
}

#[test]
fn a_command_which_validates_the_plugins_does_start_the_interpreter() {
    // The control: without it the two tests above could pass for the wrong reason.
    let f = fixture();
    let _ = run(&f, &["show", "plugins"]);
    assert!(
        started(&f),
        "`show plugins` was expected to probe the interpreter"
    );
}
