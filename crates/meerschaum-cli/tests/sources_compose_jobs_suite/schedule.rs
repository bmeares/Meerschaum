//! `--schedule`: the action runs on the schedule until the process is interrupted, then the
//! scheduler exits successfully (`utils/schedule.py` `schedule_function`, `entry_with_args`).

#[cfg(unix)]
#[test]
fn schedule_runs_until_interrupted() {
    use std::process::{Command, Stdio};
    use std::time::Duration;

    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("root")).unwrap();
    std::fs::create_dir_all(tmp.path().join("plugins")).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    for (k, _) in std::env::vars() {
        if k.starts_with("MRSM_") {
            cmd.env_remove(k);
        }
    }
    let child = cmd
        .args(["show", "version", "--schedule", "every 1 second"])
        .env("MRSM_ROOT_DIR", tmp.path().join("root"))
        .env("MRSM_PLUGINS_DIR", tmp.path().join("plugins"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(2500));
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let runs = stdout.matches("Meerschaum v").count();
    assert!(runs >= 2, "{stdout}");
    assert!(stdout.contains("Exited scheduler after"), "{stdout}");
    assert_eq!(out.status.code(), Some(0), "{stdout}");
}

#[test]
fn invalid_schedule_fails() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("root")).unwrap();
    std::fs::create_dir_all(tmp.path().join("plugins")).unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
        .args(["show", "version", "-s", "every 3 fortnights", "--nopretty"])
        .env("MRSM_ROOT_DIR", tmp.path().join("root"))
        .env("MRSM_PLUGINS_DIR", tmp.path().join("plugins"))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!stdout.contains("Meerschaum v"), "{stdout}");
}
