//! The job side of the `systemd` executor, run by hand: what a unit's
//! `ExecStart=<mrsm> -m meerschaum <sysargs>` does when it starts with the `MRSM_SYSTEMD_*`
//! variables (the rotating service log with timestamps, the result file, stop on request).
//!
//! No `systemctl` is involved: the test starts the binary as the unit would and never sets
//! `MRSM_SYSTEMD_DELETE_JOB` (the delete-after-completion path asks `systemctl` to remove the
//! unit, which tests must not do; it is covered with a fake runner in `meerschaum-jobs`).

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

struct Unit {
    dir: tempfile::TempDir,
}

impl Unit {
    fn new() -> Unit {
        let dir = tempfile::tempdir().unwrap();
        for sub in ["root", "plugins", "systemd/logs", "systemd/services/sysjob"] {
            std::fs::create_dir_all(dir.path().join(sub)).unwrap();
        }
        Unit { dir }
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }

    fn log_text(&self) -> String {
        let logs = self.path("systemd/logs");
        let mut files: Vec<PathBuf> = std::fs::read_dir(logs)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("mrsm-sysjob.service.log"))
            })
            .collect();
        files.sort();
        files
            .iter()
            .map(|p| std::fs::read_to_string(p).unwrap_or_default())
            .collect::<Vec<_>>()
            .concat()
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mrsm"));
        command
            .args(["-m", "meerschaum"])
            .args(args)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", self.path("root"))
            .env("MRSM_ROOT_DIR", self.path("root"))
            .env("MRSM_PLUGINS_DIR", self.path("plugins"))
            .env("MRSM_PYTHON", self.path("no-python"))
            .env("MRSM_NO_UPDATE_CHECK", "1")
            .env("MRSM_DAEMON_ID", "sysjob")
            .env(
                "MRSM_SYSTEMD_LOG_PATH",
                self.path("systemd/logs/mrsm-sysjob.service.log"),
            )
            .env(
                "MRSM_SYSTEMD_RESULT_PATH",
                self.path("systemd/services/sysjob/mrsm-sysjob.service.result.json"),
            )
            .env(
                "MRSM_SYSTEMD_STDIN_PATH",
                self.path("systemd/services/sysjob/mrsm-sysjob.service.stdin"),
            )
            .env("MRSM_SYSTEMD_DELETE_JOB", "0")
            .stdin(Stdio::null());
        command
    }

    fn result(&self) -> Option<serde_json::Value> {
        let text = std::fs::read_to_string(
            self.path("systemd/services/sysjob/mrsm-sysjob.service.result.json"),
        )
        .ok()?;
        serde_json::from_str(&text).ok()
    }
}

/// Every descendant of `pid` (children first level by level), by `pgrep -P`.
#[cfg(unix)]
fn descendants(pid: u32) -> Vec<u32> {
    let mut found = Vec::new();
    let mut queue = vec![pid];
    while let Some(parent) = queue.pop() {
        let out = Command::new("pgrep")
            .args(["-P", &parent.to_string()])
            .output()
            .unwrap();
        for child in String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .filter_map(|p| p.parse::<u32>().ok())
        {
            found.push(child);
            queue.push(child);
        }
    }
    found
}

/// The start time of a process (field 22 of `/proc/<pid>/stat`, clock ticks since boot): with
/// the PID it identifies one process for good, so a recycled PID is never mistaken for it.
#[cfg(unix)]
fn start_time(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name (field 2) may contain spaces and parentheses: count from the last `)`.
    let rest = &stat[stat.rfind(')')? + 2..];
    rest.split_whitespace().nth(19)?.parse().ok()
}

/// Kills the recorded processes (and the wrapper) when dropped, so a failing assertion cannot
/// leave a job's process tree behind. Each PID is recorded with its start time and only killed
/// while it is still that process.
#[cfg(unix)]
struct Reaper(Vec<(u32, Option<u64>)>);

#[cfg(unix)]
impl Reaper {
    fn new(pids: Vec<u32>) -> Reaper {
        Reaper(pids.into_iter().map(|p| (p, start_time(p))).collect())
    }
}

#[cfg(unix)]
impl Drop for Reaper {
    fn drop(&mut self) {
        for (pid, started) in &self.0 {
            if started.is_some() && start_time(*pid) == *started {
                let _ = Command::new("kill")
                    .args(["-KILL", &pid.to_string()])
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }
}

fn wait_exit(child: &mut std::process::Child, secs: u64) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "the job did not exit");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_unit_logs_with_timestamps_and_records_the_result() {
    let unit = Unit::new();
    let status = unit
        .command(&["show", "version", "--nopretty"])
        .output()
        .unwrap();
    assert!(status.status.success());
    // Nothing reaches the unit's own stdout: it all goes to the service log.
    assert!(String::from_utf8_lossy(&status.stdout).trim().is_empty());
    let log = unit.log_text();
    // `show version --nopretty` prints the bare version.
    assert!(log.contains(env!("CARGO_PKG_VERSION")), "{log}");
    let stamp = regex::Regex::new(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2} \| ").unwrap();
    assert!(stamp.is_match(&log), "{log}");
    let result = unit.result().expect("a result file");
    assert_eq!(result[0], serde_json::json!(true));
    // The wrapper's marker never leaks into the recorded result path.
    assert!(!Path::new(&unit.path("systemd/logs/mrsm-sysjob.service.child-result.json")).exists());
}

#[test]
fn a_failing_unit_exits_nonzero_and_records_the_failure() {
    let unit = Unit::new();
    let status = unit.command(&["sh", "false"]).status().unwrap();
    assert!(!status.success());
    let result = unit.result().expect("a result file");
    assert_eq!(result[0], serde_json::json!(false));
}

#[cfg(unix)]
#[test]
fn a_unit_stopped_on_request_exits_zero() {
    let unit = Unit::new();
    let mut child = unit.command(&["sh", "sleep", "20"]).spawn().unwrap();
    // Wait for the wrapped child to be running (the service log appears with its first write;
    // the wrapper itself has opened the log by then).
    std::thread::sleep(Duration::from_millis(1500));
    // The `sleep` the shell action starts outlives a SIGINT sent to the wrapper alone: record
    // the whole tree so it is reaped at the end.
    let mut tree = descendants(child.id());
    tree.push(child.id());
    let reaper = Reaper::new(tree.clone());
    let signalled = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(signalled.success());
    let status = wait_exit(&mut child, 20);
    // `RestartPreventExitStatus=0`: a stop on request must not make systemd restart the unit.
    assert!(status.success(), "{status:?}");
    // Nothing of the job's tree is left behind (the `sleep` is orphaned by the SIGINT).
    drop(reaper);
    let deadline = Instant::now() + Duration::from_secs(10);
    while tree
        .iter()
        .any(|pid| Path::new(&format!("/proc/{pid}")).exists())
    {
        assert!(Instant::now() < deadline, "processes left behind: {tree:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Outside a unit (no `MRSM_SYSTEMD_*` variables) a leading `-m` is the metric filter, never a
/// module prefix: stripping it would turn `mrsm -m meerschaum delete pipes -y` into a delete of
/// every pipe.
#[test]
fn a_leading_dash_m_is_kept_outside_a_unit() {
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_mrsm"))
        .args(["-m", "meerschaum", "show", "arguments"])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", dir.path())
        .env("MRSM_ROOT_DIR", dir.path())
        .env("MRSM_PLUGINS_DIR", dir.path().join("plugins"))
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    // `-m` takes `meerschaum show arguments` as its three metric keys, so there is no action:
    // the interactive shell starts (and ends at the closed stdin) and no action ran. Had the
    // prefix been stripped, `show arguments` would have printed its dictionary.
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(text.contains("Thank you for using Meerschaum"), "{text}");
    assert!(!text.contains("'filtered_sysargs'"), "{text}");
}

/// `systemctl kill` signals the whole control group, so inside a unit (`INVOCATION_ID` is set)
/// both the wrapper and the job receive the SIGINT. The wrapper must not forward it a second
/// time: two SIGINTs make the job exit 130 in the middle of its work instead of stopping at
/// the next checkpoint.
#[cfg(unix)]
#[test]
fn a_signal_the_service_manager_delivered_to_the_cgroup_is_not_forwarded_again() {
    let unit = Unit::new();
    let mut wrapper = unit
        .command(&[
            "show",
            "version",
            "--nopretty",
            ":",
            "--loop",
            "--min-seconds",
            "1",
        ])
        .env("INVOCATION_ID", "0123456789abcdef0123456789abcdef")
        .spawn()
        .unwrap();
    let _reaper = Reaper::new(vec![wrapper.id()]);
    // Wait for the job (the wrapper's only child) to be up and writing to the log.
    let deadline = Instant::now() + Duration::from_secs(30);
    let child_pid = loop {
        let pids = Command::new("pgrep")
            .args(["-P", &wrapper.id().to_string()])
            .output()
            .unwrap();
        let pid = String::from_utf8_lossy(&pids.stdout)
            .split_whitespace()
            .next()
            .and_then(|p| p.parse::<u32>().ok());
        if let Some(pid) = pid
            && unit.log_text().contains(env!("CARGO_PKG_VERSION"))
        {
            break pid;
        }
        assert!(Instant::now() < deadline, "the job did not start");
        std::thread::sleep(Duration::from_millis(100));
    };
    // What `systemctl kill -s SIGINT <unit>` does: signal every process of the cgroup.
    let mut tree = descendants(wrapper.id());
    tree.push(wrapper.id());
    let _tree = Reaper::new(tree);
    for pid in [wrapper.id(), child_pid] {
        assert!(
            Command::new("kill")
                .args(["-INT", &pid.to_string()])
                .status()
                .unwrap()
                .success()
        );
    }
    let status = wait_exit(&mut wrapper, 30);
    assert!(status.success(), "{status:?}");
    let result = unit.result().expect("a result file");
    assert!(
        !result.to_string().contains("130"),
        "the job was force-quit by a second SIGINT: {result}"
    );
}
