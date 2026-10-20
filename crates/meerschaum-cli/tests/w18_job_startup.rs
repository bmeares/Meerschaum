//! The job startup gate (D23) through the real binary: how `start jobs` classifies a start,
//! the worker releasing its slot after its first lap, and the `systemd` unit wrapper waiting at
//! the gate.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

struct Home {
    root: tempfile::TempDir,
    plugins: tempfile::TempDir,
    config: String,
}

impl Home {
    fn new(startup: &str) -> Home {
        Home {
            root: tempfile::tempdir().unwrap(),
            plugins: tempfile::tempdir().unwrap(),
            config: format!(
                r#"{{"formatting":{{"ansi":false,"unicode":false}},"system":{{"cli":{{"check_updates":false}}}},"jobs":{{"startup":{startup}}}}}"#
            ),
        }
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command
            .args(args)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.root.path())
            .env("MRSM_ROOT_DIR", self.root.path())
            .env("MRSM_PLUGINS_DIR", self.plugins.path())
            .env("MRSM_PYTHON", self.root.path().join("no-python"))
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_CONFIG", &self.config)
            .stdin(Stdio::null());
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }

    fn properties(&self, job: &str) -> Option<serde_json::Value> {
        let text = std::fs::read_to_string(
            self.root
                .path()
                .join("jobs")
                .join(job)
                .join("properties.json"),
        )
        .ok()?;
        serde_json::from_str(&text).ok()
    }

    fn stamp(&self, job: &str, key: &str) -> Option<chrono::NaiveDateTime> {
        let props = self.properties(job)?;
        let text = props.get("process")?.get(key)?.as_str()?.to_string();
        chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%dT%H:%M:%S%.f")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%dT%H:%M:%S"))
            .ok()
    }

    /// The worker of the job's latest launch has begun.
    fn began(&self, job: &str) -> bool {
        match (self.stamp(job, "began"), self.stamp(job, "launched")) {
            (Some(began), Some(launched)) => began >= launched,
            (Some(_), None) => true,
            _ => false,
        }
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

/// The command lines of the supervisors of one job (they carry `daemon_id='<name>'`).
fn supervisor_commands(name: &str) -> Vec<String> {
    let out = Command::new("pgrep")
        .args(["-af", &format!("daemon_id='{name}'")])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.contains("__mrsm_job_supervisor__"))
        .map(str::to_string)
        .collect()
}

/// Deletes the jobs when dropped, so a failing assertion leaves no process behind.
struct Cleanup<'a>(&'a Home, Vec<&'static str>);

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        for name in &self.1 {
            let _ = self.0.run(&["stop", "jobs", name, "-y"]);
            let _ = self.0.run(&["delete", "jobs", name, "-f"]);
        }
    }
}

#[test]
fn start_jobs_classifies_the_start() {
    // A huge jitter keeps a bulk-started job at the gate, where its supervisor's command line
    // names the policy it was given; no slots and no load checks, so nothing else interferes.
    let home = Home::new(
        r#"{"jitter_seconds":3600,"max_load":0,"min_free_memory_percent":0,"max_concurrent":0}"#,
    );
    let _cleanup = Cleanup(&home, vec!["w18jbk1", "w18jbk2"]);
    for name in ["w18jbk1", "w18jbk2"] {
        let created = home.run(&[
            "sh",
            "sleep",
            "60",
            "-d",
            "--name",
            name,
            "--no-stagger",
            "--noask",
        ]);
        assert!(created.status.success(), "{}", text(&created));
        wait_until(30, "the job to begin", || home.began(name));
    }
    let stopped = home.run(&["stop", "jobs", "w18jbk1", "w18jbk2", "-y"]);
    assert!(stopped.status.success(), "{}", text(&stopped));

    // Two jobs: a bulk start.
    let started = home.run(&["start", "jobs", "w18jbk1", "w18jbk2", "-y"]);
    assert!(started.status.success(), "{}", text(&started));
    for name in ["w18jbk1", "w18jbk2"] {
        let commands = supervisor_commands(name);
        assert_eq!(commands.len(), 1, "{name}: {commands:?}");
        assert!(commands[0].contains(r#""kind":"bulk""#), "{}", commands[0]);
        assert!(
            commands[0].contains(r#""jitter_seconds":3600.0"#),
            "{}",
            commands[0]
        );
        // Waiting at the gate: running, and its worker has not begun.
        assert!(!home.began(name), "{name} began although it must wait");
    }
    // A job stopped at the gate never runs.
    let stopped = home.run(&["stop", "jobs", "w18jbk1", "w18jbk2", "-y"]);
    assert!(stopped.status.success(), "{}", text(&stopped));
    wait_until(30, "the supervisors to exit", || {
        supervisor_commands("w18jbk1").is_empty() && supervisor_commands("w18jbk2").is_empty()
    });
    assert!(!home.began("w18jbk1") || home.properties("w18jbk1").unwrap()["result"][0] == true);

    // One job: an explicit start, no jitter.
    let started = home.run(&["start", "jobs", "w18jbk1", "-y"]);
    assert!(started.status.success(), "{}", text(&started));
    wait_until(30, "the single start to begin", || home.began("w18jbk1"));
    let commands = supervisor_commands("w18jbk1");
    assert_eq!(commands.len(), 1, "{commands:?}");
    assert!(
        commands[0].contains(r#""kind":"single""#),
        "{}",
        commands[0]
    );
    assert!(
        commands[0].contains(r#""jitter_seconds":0.0"#),
        "{}",
        commands[0]
    );
    let _ = home.run(&["stop", "jobs", "w18jbk1", "-y"]);

    // `--no-stagger`: no gate, no policy argument.
    let started = home.run(&["start", "jobs", "w18jbk1", "w18jbk2", "--no-stagger", "-y"]);
    assert!(started.status.success(), "{}", text(&started));
    for name in ["w18jbk1", "w18jbk2"] {
        let commands = supervisor_commands(name);
        assert_eq!(commands.len(), 1, "{name}: {commands:?}");
        assert!(!commands[0].contains("--startup"), "{}", commands[0]);
    }
    let _ = home.run(&["stop", "jobs", "w18jbk1", "w18jbk2", "-y"]);

    // The environment variable compose sets: `MRSM_JOB_START_KIND=bulk` for one job.
    let mut command = home.command(&["start", "jobs", "w18jbk1", "-y"]);
    command.env("MRSM_JOB_START_KIND", "bulk");
    let started = command.output().unwrap();
    assert!(started.status.success(), "{}", text(&started));
    let commands = supervisor_commands("w18jbk1");
    assert_eq!(commands.len(), 1, "{commands:?}");
    assert!(commands[0].contains(r#""kind":"bulk""#), "{}", commands[0]);
}

#[test]
fn a_new_job_keeps_its_startup_jitter_override() {
    let home = Home::new(r#"{"jitter_seconds":3600,"max_load":0,"min_free_memory_percent":0}"#);
    let _cleanup = Cleanup(&home, vec!["w18jbo"]);
    let created = home.run(&[
        "sh",
        "sleep",
        "30",
        "-d",
        "--name",
        "w18jbo",
        "--startup-jitter",
        "7s",
        "--noask",
    ]);
    assert!(created.status.success(), "{}", text(&created));
    let props = home.properties("w18jbo").expect("properties");
    assert_eq!(props["startup"]["jitter_seconds"], 7.0, "{props}");
}

#[test]
fn a_worker_frees_its_slot_after_its_first_lap() {
    // One slot, a 25 s timeout: if the scheduled job did not signal its first lap, the second
    // job would wait the whole 25 s and then give up.
    let home = Home::new(
        r#"{"jitter_seconds":0,"max_load":0,"min_free_memory_percent":0,"max_concurrent":1,"slot_timeout_seconds":25}"#,
    );
    let _cleanup = Cleanup(&home, vec!["w18jblap", "w18jbnext"]);
    let created = home.run(&[
        "show",
        "version",
        "--nopretty",
        "-s",
        "every 3 seconds",
        "-d",
        "--name",
        "w18jblap",
        "--noask",
    ]);
    assert!(created.status.success(), "{}", text(&created));
    wait_until(20, "the scheduled job to begin", || home.began("w18jblap"));
    let created = home.run(&["sh", "sleep", "30", "-d", "--name", "w18jbnext", "--noask"]);
    assert!(created.status.success(), "{}", text(&created));
    let began = Instant::now();
    wait_until(20, "the next job to begin", || home.began("w18jbnext"));
    assert!(
        began.elapsed() < Duration::from_secs(15),
        "the next job waited {:?} for the slot",
        began.elapsed()
    );
    let log = std::fs::read_to_string(home.root.path().join("logs").join("w18jbnext.log.0"))
        .unwrap_or_default();
    assert!(!log.contains("gave up waiting"), "{log}");
}

// --- the unit wrapper ---------------------------------------------------------------------------

struct Unit {
    home: Home,
}

impl Unit {
    fn new(startup: &str) -> Unit {
        let home = Home::new(startup);
        for sub in [
            "systemd/logs",
            "systemd/services/w18jbunit",
            "jobs/.startup",
        ] {
            std::fs::create_dir_all(home.root.path().join(sub)).unwrap();
        }
        Unit { home }
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.home.root.path().join(rel)
    }

    fn wrapper(&self) -> Command {
        let mut command = self
            .home
            .command(&["-m", "meerschaum", "show", "version", "--nopretty"]);
        command
            .env("MRSM_DAEMON_ID", "w18jbunit")
            .env(
                "MRSM_SYSTEMD_LOG_PATH",
                self.path("systemd/logs/mrsm-w18jbunit.service.log"),
            )
            .env(
                "MRSM_SYSTEMD_RESULT_PATH",
                self.path("systemd/services/w18jbunit/mrsm-w18jbunit.service.result.json"),
            )
            .env(
                "MRSM_SYSTEMD_STDIN_PATH",
                self.path("systemd/services/w18jbunit/mrsm-w18jbunit.service.stdin"),
            )
            .env("MRSM_SYSTEMD_DELETE_JOB", "0");
        command
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.path("systemd/logs/mrsm-w18jbunit.service.log.0"))
            .unwrap_or_default()
    }
}

fn holder_of(slot: &Path) -> Option<std::process::Child> {
    if Command::new("flock").arg("--version").output().is_err() {
        eprintln!("skipped: no flock(1)");
        return None;
    }
    Some(
        Command::new("flock")
            .arg(slot)
            .args(["sleep", "40"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

/// Kill `flock` and the `sleep` it started.
fn release(mut holder: std::process::Child) {
    let _ = Command::new("pkill")
        .args(["-KILL", "-P", &holder.id().to_string()])
        .status();
    let _ = holder.kill();
    let _ = holder.wait();
}

#[test]
fn the_unit_wrapper_waits_at_the_gate_and_an_explicit_start_hint_skips_it() {
    let unit = Unit::new(
        r#"{"jitter_seconds":0,"max_load":0,"min_free_memory_percent":0,"max_concurrent":1,"slot_timeout_seconds":2}"#,
    );
    let slot = unit.path("jobs/.startup/slot-0.lock");
    let Some(holder) = holder_of(&slot) else {
        return;
    };
    // Wait until the other process really holds the slot.
    let probe = |slot: &Path| {
        Command::new("flock")
            .args(["-n"])
            .arg(slot)
            .args(["true"])
            .status()
            .map(|s| !s.success())
            .unwrap_or(false)
    };
    wait_until(10, "the slot to be held", || probe(&slot));

    // A unit start is a bulk start: it waits for the slot, then gives up after the timeout.
    let began = Instant::now();
    let out = unit.wrapper().output().unwrap();
    assert!(out.status.success(), "{}", text(&out));
    assert!(
        began.elapsed() >= Duration::from_millis(1900),
        "the wrapper did not wait: {:?}",
        began.elapsed()
    );
    let log = unit.log();
    assert!(
        log.contains("Startup: waiting because every startup slot is busy"),
        "{log}"
    );
    assert!(log.contains("gave up waiting after 2 s"), "{log}");

    // An explicit `start job x` (single) is left as a hint for the wrapper; `--no-stagger`
    // (none) skips the gate: the wrapper does not wait at all.
    meerschaum_jobs::startup::write_start_hint(
        &unit.path("jobs/.startup"),
        "w18jbunit",
        meerschaum_jobs::StartKind::None,
    );
    let began = Instant::now();
    let out = unit.wrapper().output().unwrap();
    assert!(out.status.success(), "{}", text(&out));
    assert!(
        began.elapsed() < Duration::from_millis(1900),
        "the hint was ignored: {:?}",
        began.elapsed()
    );
    // The hint is used once.
    assert!(
        meerschaum_jobs::startup::take_start_hint(&unit.path("jobs/.startup"), "w18jbunit")
            .is_none()
    );
    release(holder);
}
