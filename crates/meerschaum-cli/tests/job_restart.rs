//! `mrsm stop jobs x -y; mrsm start jobs x -y` restarts the job: a stop returns once the job is
//! really stopped (its supervisor included), so the start which follows is not told that the
//! job "is already running".
#![cfg(unix)]

use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
}

impl Home {
    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_mrsm-cli"))
            .args(args)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_PYTHON", self.root.path().join("no-python"))
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env(
                "MRSM_CONFIG",
                r#"{"formatting":{"ansi":false,"unicode":false},"system":{"cli":{"check_updates":false}}}"#,
            )
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn wait_until(secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn stop_then_start_from_the_command_line_restarts_the_job() {
    let home = Home {
        root: tempfile::tempdir().unwrap(),
        plugins: tempfile::tempdir().unwrap(),
    };
    // `mrsm sh sleep 8` runs a `sleep` of its own, which keeps the supervisor's output pipes open
    // after the worker is gone: the supervisor outlives its worker for a while.
    let created = home.run(&["sh", "sleep", "8", "-d", "--name", "x", "--noask"]);
    assert!(created.status.success(), "{}", text(&created));
    let running = |home: &Home| {
        let out = home.run(&["show", "jobs", "x"]);
        text(&out).contains("running")
    };
    wait_until(30, "the job to run", || running(&home));

    for round in 0..2 {
        let stopped = home.run(&["stop", "jobs", "x", "-y"]);
        assert!(
            stopped.status.success(),
            "round {round}: {}",
            text(&stopped)
        );
        let started = home.run(&["start", "jobs", "x", "-y"]);
        let started_text = text(&started);
        assert!(started.status.success(), "round {round}: {started_text}");
        assert!(
            !started_text.contains("already running"),
            "round {round}: {started_text}"
        );
        wait_until(30, "the job to run again", || running(&home));
    }
    let _ = home.run(&["stop", "jobs", "x", "-y"]);
    let deleted = home.run(&["delete", "jobs", "x", "-f"]);
    assert!(deleted.status.success(), "{}", text(&deleted));
}
