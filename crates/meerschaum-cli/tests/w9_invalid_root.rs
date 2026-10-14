//! A missing `MRSM_ROOT_DIR` or `MRSM_CONFIG_DIR` prints v4's message and exits 1
//! (`config/_paths.py`) instead of panicking.

use std::process::Command;

fn run(env: &[(&str, &std::path::Path)]) -> (Option<i32>, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mrsm"));
    cmd.args(["show", "version"]);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let output = cmd.output().unwrap();
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn missing_root_dir_exits_with_v4_message() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("missing");
    let (code, stderr) = run(&[
        ("MRSM_ROOT_DIR", &missing),
        ("MRSM_PLUGINS_DIR", tmp.path()),
    ]);
    assert_eq!(code, Some(1), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(
        stderr.contains(&format!(
            "Invalid root directory '{}' set for environment variable 'MRSM_ROOT_DIR'.\n\
             Please enter a valid path for MRSM_ROOT_DIR.",
            missing.display()
        )),
        "{stderr}"
    );
}

#[test]
fn missing_config_dir_exits_with_v4_message() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("missing");
    let (code, stderr) = run(&[
        ("MRSM_ROOT_DIR", tmp.path()),
        ("MRSM_PLUGINS_DIR", tmp.path()),
        ("MRSM_CONFIG_DIR", &missing),
    ]);
    assert_eq!(code, Some(1), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(
        stderr.contains("set for environment variable 'MRSM_CONFIG_DIR'\n"),
        "{stderr}"
    );
}
