//! RC25: a web terminal on a server that runs as a Meerschaum `systemd` job showed `[CLOSED]`.
//! The terminal's `mrsm` inherited the job's variables (`MRSM_SYSTEMD_LOG_PATH`, ...), wrapped
//! itself as that job (`meerschaum_jobs::systemd_job`), refused to start an interactive shell
//! ("The job has no action to run (an interactive shell cannot start in a job).") and exited.
//!
//! The real `mrsm` (this crate's `mrsm-cli`) runs on a PTY with a fake job's variables, once with
//! the terminal's scrub (`meerschaum_server::webterm::scrub_job_context`) and once without. Every
//! file is in a temporary directory: no real job, unit, root or terminal is touched.
#![cfg(unix)]

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use portable_pty::{CommandBuilder, PtySize, native_pty_system};

/// A fake job: its rotating log, result file and stdin FIFO, which a terminal must not touch.
struct FakeJob {
    dir: tempfile::TempDir,
}

impl FakeJob {
    fn new() -> FakeJob {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("rc25-api.stdin");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(made.success());
        FakeJob { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn vars(&self) -> Vec<(String, String)> {
        let path = |name: &str| self.path(name).display().to_string();
        vec![
            ("MRSM_DAEMON_ID".into(), "rc25-api".into()),
            ("INVOCATION_ID".into(), "rc25invocation".into()),
            ("JOURNAL_STREAM".into(), "8:1234".into()),
            ("MRSM_SYSTEMD_LOG_PATH".into(), path("rc25-api.log")),
            (
                "MRSM_SYSTEMD_RESULT_PATH".into(),
                path("rc25-api.result.json"),
            ),
            ("MRSM_SYSTEMD_STDIN_PATH".into(), path("rc25-api.stdin")),
            // What `meerschaum_jobs::Job::env` gives every job (RC28: the terminal's shell lost
            // its editor, toolbar and readiness to it).
            ("MRSM_NONINTERACTIVE".into(), "true".into()),
        ]
    }

    /// The job's files as they are now: (log exists, result text, stdin is still a FIFO).
    fn state(&self) -> (bool, Option<String>, bool) {
        use std::os::unix::fs::FileTypeExt;
        let fifo = std::fs::symlink_metadata(self.path("rc25-api.stdin"))
            .is_ok_and(|m| m.file_type().is_fifo());
        (
            self.path("rc25-api.log").exists(),
            std::fs::read_to_string(self.path("rc25-api.result.json")).ok(),
            fifo,
        )
    }
}

/// `text` without its escape sequences (CSI and OSC), as a terminal would show it.
fn without_ansi(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\x1b' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('[') => {
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
            }
            Some(']') => {
                while let Some(c) = chars.next() {
                    if c == '\x07' || (c == '\x1b' && chars.next_if_eq(&'\\').is_some()) {
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    out
}

struct Outcome {
    /// Still running (an interactive shell) when the check ended.
    alive: bool,
    /// What the PTY showed.
    output: String,
}

/// Run `mrsm -i sql:local` on a PTY with the fake job's variables (scrubbed or not), in a
/// temporary root, until its status line shows or it exits (at most 30 s), then 2 s more.
fn run_shell(job: &FakeJob, root: &Path, scrub: bool) -> Outcome {
    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_mrsm-cli"));
    command.args(["-i", "sql:local"]);
    // Nothing from the test's own environment decides the outcome.
    for (key, _) in std::env::vars_os() {
        let key = key.to_string_lossy().into_owned();
        if key.starts_with("MRSM_") {
            command.env_remove(&key);
        }
    }
    command.env_remove("TMUX");
    command.env_remove("TMUX_PANE");
    command.env("MRSM_ROOT_DIR", root.join("root"));
    command.env("MRSM_PLUGINS_DIR", root.join("plugins"));
    command.env("MRSM_NO_UPDATE_CHECK", "1");
    command.env("MRSM_NO_AUTO_INSTALL", "1");
    // Exercise job-context wrapping without the bulk startup gate's random jitter or
    // host load / memory admission waits (which can outlast this fixture's deadline).
    command.env(meerschaum_jobs::startup::KIND_ENV, "none");
    command.env("HOME", root);
    command.env("TERM", "xterm-256color");
    for (key, value) in job.vars() {
        command.env(key, value);
    }
    if scrub {
        meerschaum_server::webterm::scrub_job_context(&mut command);
    }
    command.cwd(root);
    std::fs::create_dir_all(root.join("root")).unwrap();
    std::fs::create_dir_all(root.join("plugins")).unwrap();

    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 100,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let mut child = pair.slave.spawn_command(command).unwrap();
    drop(pair.slave);
    // The PTY's output, capped (a runaway screen never grows the test without bound).
    const MAX_OUTPUT: usize = 1 << 20;
    let output = Arc::new(Mutex::new(Vec::<u8>::new()));
    let mut reader = pair.master.try_clone_reader().unwrap();
    let sink = output.clone();
    let reading = std::thread::spawn(move || {
        let mut buffer = [0u8; 4096];
        while let Ok(n) = reader.read(&mut buffer) {
            if n == 0 {
                break;
            }
            let mut out = sink.lock().unwrap();
            let room = MAX_OUTPUT.saturating_sub(out.len());
            out.extend_from_slice(&buffer[..n.min(room)]);
        }
    });
    let text = || String::from_utf8_lossy(&output.lock().unwrap()).into_owned();
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() || text().contains("sql:local") {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    std::thread::sleep(Duration::from_secs(2));
    let alive = child.try_wait().unwrap().is_none();
    if alive {
        let _ = child.kill();
    }
    let _ = child.wait();
    // Closing the master ends the reader (EOF or an error); join it so nothing outlives the test.
    drop(pair.master);
    reading.join().unwrap();
    Outcome {
        alive,
        output: text(),
    }
}

#[test]
fn a_terminal_shell_stays_interactive_under_a_jobs_environment() {
    let job = FakeJob::new();
    let root = tempfile::tempdir().unwrap();
    let before = job.state();
    assert_eq!(before, (false, None, true));

    let shell = run_shell(&job, root.path(), true);
    assert!(shell.alive, "the scrubbed shell exited: {:?}", shell.output);
    assert!(shell.output.contains("sql:local"), "{:?}", shell.output);
    // The line editor runs (not the plain line reader `MRSM_NONINTERACTIVE` selects): it draws
    // the bottom toolbar, the instance followed by the executor.
    assert!(
        without_ansi(&shell.output).contains("sql:local | "),
        "no toolbar: {:?}",
        shell.output
    );
    assert!(
        !shell.output.contains("cannot start in a job"),
        "{:?}",
        shell.output
    );
    // The job's log, result and FIFO are untouched.
    assert_eq!(job.state(), before);
}

/// Without the scrub, the same shell is the job's wrapper: it exits and writes the job's result.
/// (What the terminal on an API run as a `systemd` job did before RC25.)
#[test]
fn without_the_scrub_the_shell_wraps_itself_as_the_job() {
    let job = FakeJob::new();
    let root = tempfile::tempdir().unwrap();
    let shell = run_shell(&job, root.path(), false);
    assert!(!shell.alive, "{:?}", shell.output);
    let (_, result, _) = job.state();
    let result = result.expect("the wrapper writes the job's result");
    assert!(
        result.contains("an interactive shell cannot start in a job"),
        "{result}"
    );
}

/// Every variable the jobs crate uses to mark a job's process is one the terminal removes.
#[test]
fn every_job_variable_is_scrubbed() {
    let scrubbed = meerschaum_server::webterm::job_context_vars();
    let sc = &meerschaum_core::static_config::static_config().environment;
    for name in [
        meerschaum_jobs::systemd_job::CHILD_ENV,
        meerschaum_jobs::systemd_job::INVOCATION_ID_ENV,
        meerschaum_jobs::supervisor::RESULT_PATH_ENV,
        meerschaum_jobs::supervisor::SUPERVISOR_ENV,
        meerschaum_jobs::supervisor::PROGRAM_ENV,
        meerschaum_jobs::health::LAPS_PATH_ENV,
        meerschaum_jobs::health::LAPS_PARENT_ENV,
        meerschaum_jobs::startup::RELEASE_ENV,
        meerschaum_jobs::startup::KIND_ENV,
        sc.daemon_id.as_str(),
        sc.systemd_log_path.as_str(),
        sc.systemd_result_path.as_str(),
        sc.systemd_stdin_path.as_str(),
        sc.systemd_delete_job.as_str(),
        sc.compose_project.as_str(),
        sc.compose_root.as_str(),
        sc.noninteractive.as_str(),
    ] {
        assert!(
            scrubbed.iter().any(|var| var == name),
            "{name} is not scrubbed"
        );
    }
}
