//! A variable of the environment that is not valid UTF-8 must not end the binary
//! (`std::env::vars()` panics on it; the exit code was 101).
#![cfg(unix)]

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::process::Command;

fn run(args: &[&str]) -> (Option<i32>, String, String) {
    let root = tempfile::tempdir().unwrap();
    let plugins = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .args(args)
        .env("MRSM_ROOT_DIR", root.path())
        .env("MRSM_PLUGINS_DIR", plugins.path())
        .env("W16BAD", OsStr::from_bytes(&[0xff, 0xfe]))
        .output()
        .unwrap();
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn show_version_succeeds_with_a_non_utf8_variable() {
    let (code, stdout, stderr) = run(&["show", "version", "--nopretty"]);
    assert_eq!(code, Some(0), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(stdout.starts_with("5."), "{stdout}");
}

#[test]
fn show_config_and_environment_succeed_with_a_non_utf8_variable() {
    for args in [
        &["show", "config", "meerschaum", "--nopretty"][..],
        &["show", "environment", "--nopretty"][..],
    ] {
        let (code, _, stderr) = run(args);
        assert_eq!(code, Some(0), "{args:?}: {stderr}");
        assert!(!stderr.contains("panicked"), "{args:?}: {stderr}");
    }
}
