//! The replaceable process runner behind the `systemd` executor.
//!
//! `meerschaum-jobs` never shells out to `systemctl` directly: the [`crate::systemd::SystemdExecutor`]
//! asks a [`ProcessRunner`] (held by [`crate::JobsContext`]) to run `systemctl --user ...`, so tests
//! substitute [`FakeSystemctl`], which records every command and emulates the handful of
//! sub-commands the executor uses without touching the host's services.
//!
//! A real `systemctl --user` smoke test exists behind `MRSM_TEST_SYSTEMD_SMOKE` (unset by default);
//! see `tests/systemd_executor.rs`.

use std::collections::HashMap;
use std::io;
use std::sync::Mutex;

use crate::process::{self, ProcState};

/// What a finished command produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandOutput {
    /// The exit code (`None` when killed by a signal).
    pub code: Option<i32>,
    /// Standard output.
    pub stdout: String,
    /// Standard error.
    pub stderr: String,
}

impl CommandOutput {
    /// A successful command with this standard output.
    pub fn ok(stdout: impl Into<String>) -> CommandOutput {
        CommandOutput {
            code: Some(0),
            stdout: stdout.into(),
            stderr: String::new(),
        }
    }

    /// A failed command (exit code 1).
    pub fn failed(stderr: impl Into<String>) -> CommandOutput {
        CommandOutput {
            code: Some(1),
            stdout: String::new(),
            stderr: stderr.into(),
        }
    }

    /// Whether the exit code was 0.
    pub fn success(&self) -> bool {
        self.code == Some(0)
    }
}

/// Runs external commands and inspects processes on behalf of the `systemd` executor.
pub trait ProcessRunner: Send + Sync + std::fmt::Debug {
    /// Run `program args...` to completion, capturing its output. An `Err` means the program
    /// could not be started (for example `systemctl` is not installed).
    fn run(&self, program: &str, args: &[String]) -> io::Result<CommandOutput>;

    /// `psutil.Process(pid).status()` reduced to what the executor distinguishes.
    fn process_state(&self, pid: i32) -> ProcState {
        process::proc_state(pid)
    }
}

/// The real runner: [`std::process::Command`] with a closed stdin.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemRunner;

impl ProcessRunner for SystemRunner {
    fn run(&self, program: &str, args: &[String]) -> io::Result<CommandOutput> {
        let output = std::process::Command::new(program)
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()?;
        Ok(CommandOutput {
            code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

/// One emulated service of [`FakeSystemctl`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FakeService {
    /// `is-active` output: `active`, `activating`, `inactive`, `failed`, ...
    pub active_state: String,
    /// `MainPID` (0 when not running).
    pub main_pid: i32,
    /// `ActiveEnterTimestamp` (systemd's `Day YYYY-MM-DD HH:MM:SS TZ` text, or empty).
    pub active_enter: String,
    /// `InactiveEnterTimestamp`.
    pub inactive_enter: String,
    /// Whether `enable` was run (and not `disable`).
    pub enabled: bool,
    /// Whether the process was suspended with `SIGSTOP`.
    pub stopped: bool,
    /// `MainPID` names a process which no longer exists.
    pub process_gone: bool,
}

#[derive(Debug, Default)]
struct FakeState {
    calls: Vec<Vec<String>>,
    services: HashMap<String, FakeService>,
    failing: Vec<String>,
    stop_on_sigint: bool,
    next_pid: i32,
    unavailable: bool,
    /// Scripted `is-active` answers by unit (the last one repeats), overriding the state.
    is_active_scripts: HashMap<String, std::collections::VecDeque<String>>,
}

/// A recording stand-in for `systemctl --user`.
///
/// It answers `is-active`, `show --property=...`, `start`, `stop`, `enable`, `disable`, `kill -s`
/// and `daemon-reload`. `start` makes a service `active` with a fake PID; `kill -s SIGSTOP` /
/// `SIGCONT` suspend and resume it; `kill -s SIGINT` stops it when
/// [`FakeSystemctl::stop_on_sigint`] is set (a job which handles SIGINT); `stop` always stops it.
#[derive(Debug, Default)]
pub struct FakeSystemctl {
    state: Mutex<FakeState>,
}

impl FakeSystemctl {
    /// A fake with no services.
    pub fn new() -> FakeSystemctl {
        FakeSystemctl {
            state: Mutex::new(FakeState {
                next_pid: 4_000_000,
                stop_on_sigint: true,
                ..FakeState::default()
            }),
        }
    }

    /// Whether `kill -s SIGINT` stops the service (default `true`).
    pub fn stop_on_sigint(&self, yes: bool) {
        self.state.lock().expect("fake state").stop_on_sigint = yes;
    }

    /// Make `systemctl` itself unavailable (every run is an `io::Error`).
    pub fn set_unavailable(&self, yes: bool) {
        self.state.lock().expect("fake state").unavailable = yes;
    }

    /// Fail every command whose first sub-command is `subcommand` (exit code 1).
    pub fn fail_on(&self, subcommand: &str) {
        self.state
            .lock()
            .expect("fake state")
            .failing
            .push(subcommand.to_string());
    }

    /// Script the `is-active` answers of a unit (each query pops one; the last repeats).
    pub fn script_is_active(&self, name: &str, answers: &[&str]) {
        self.state
            .lock()
            .expect("fake state")
            .is_active_scripts
            .insert(
                name.to_string(),
                answers.iter().map(|a| a.to_string()).collect(),
            );
    }

    /// Every command run, as the full argument vector after `systemctl` (`--user` included).
    pub fn calls(&self) -> Vec<Vec<String>> {
        self.state.lock().expect("fake state").calls.clone()
    }

    /// The commands run, rendered as `start mrsm-x.service` (without `--user`).
    pub fn rendered_calls(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .map(|c| {
                c.iter()
                    .filter(|a| a.as_str() != "--user")
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect()
    }

    /// Forget the recorded commands.
    pub fn clear_calls(&self) {
        self.state.lock().expect("fake state").calls.clear();
    }

    /// The emulated service (if it was ever started or configured).
    pub fn service(&self, name: &str) -> Option<FakeService> {
        self.state
            .lock()
            .expect("fake state")
            .services
            .get(name)
            .cloned()
    }

    /// Configure a service directly (for example an externally started one).
    pub fn set_service(&self, name: &str, service: FakeService) {
        self.state
            .lock()
            .expect("fake state")
            .services
            .insert(name.to_string(), service);
    }

    /// The fake PID `start` assigned (or configured) for a service.
    pub fn pid(&self, name: &str) -> Option<i32> {
        self.service(name)
            .map(|s| s.main_pid)
            .filter(|pid| *pid != 0)
    }
}

impl ProcessRunner for FakeSystemctl {
    fn run(&self, program: &str, args: &[String]) -> io::Result<CommandOutput> {
        let mut state = self.state.lock().expect("fake state");
        if state.unavailable {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{program}: No such file or directory"),
            ));
        }
        let mut full = vec![program.to_string()];
        full.extend(args.iter().cloned());
        let argv: Vec<String> = args.to_vec();
        state.calls.push(argv.clone());
        if program != "systemctl" {
            return Ok(CommandOutput::failed(format!(
                "unexpected program {program}"
            )));
        }
        let rest: Vec<&str> = argv
            .iter()
            .map(String::as_str)
            .filter(|a| *a != "--user")
            .collect();
        let Some(&sub) = rest.first() else {
            return Ok(CommandOutput::failed("missing sub-command"));
        };
        if state.failing.iter().any(|f| f == sub) {
            return Ok(CommandOutput::failed(format!("fake failure of `{sub}`")));
        }
        match sub {
            "daemon-reload" => Ok(CommandOutput::ok("")),
            "enable" | "disable" => {
                let enable = sub == "enable";
                if let Some(name) = rest.get(1) {
                    state.services.entry(name.to_string()).or_default().enabled = enable;
                }
                Ok(CommandOutput::ok(""))
            }
            "start" => {
                let Some(name) = rest.get(1) else {
                    return Ok(CommandOutput::failed("missing unit"));
                };
                state.next_pid += 1;
                let pid = state.next_pid;
                let svc = state.services.entry(name.to_string()).or_default();
                svc.active_state = "active".into();
                svc.main_pid = pid;
                svc.stopped = false;
                svc.active_enter = "Tue 2000-01-04 10:00:00 UTC".into();
                svc.inactive_enter = String::new();
                Ok(CommandOutput::ok(""))
            }
            "stop" => {
                let Some(name) = rest.get(1) else {
                    return Ok(CommandOutput::failed("missing unit"));
                };
                let svc = state.services.entry(name.to_string()).or_default();
                svc.active_state = "inactive".into();
                svc.main_pid = 0;
                svc.stopped = false;
                svc.inactive_enter = "Tue 2000-01-04 11:00:00 UTC".into();
                Ok(CommandOutput::ok(""))
            }
            "kill" => {
                // `kill -s SIGNAL unit`
                let signal = rest.get(2).copied().unwrap_or("");
                let Some(name) = rest.get(3) else {
                    return Ok(CommandOutput::failed("missing unit"));
                };
                let stop_on_sigint = state.stop_on_sigint;
                let Some(svc) = state.services.get_mut(*name) else {
                    return Ok(CommandOutput::failed(format!("Unit {name} not loaded.")));
                };
                match signal {
                    "SIGSTOP" => svc.stopped = true,
                    "SIGCONT" => svc.stopped = false,
                    "SIGINT" if stop_on_sigint => {
                        svc.active_state = "inactive".into();
                        svc.main_pid = 0;
                        svc.stopped = false;
                        svc.inactive_enter = "Tue 2000-01-04 11:00:00 UTC".into();
                    }
                    _ => {}
                }
                Ok(CommandOutput::ok(""))
            }
            "is-active" => {
                let name = rest.get(1).copied().unwrap_or("");
                let scripted = state.is_active_scripts.get_mut(name).and_then(|queue| {
                    if queue.len() > 1 {
                        queue.pop_front()
                    } else {
                        queue.front().cloned()
                    }
                });
                let text = scripted.unwrap_or_else(|| {
                    state
                        .services
                        .get(name)
                        .map(|s| s.active_state.clone())
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "inactive".to_string())
                });
                // `systemctl is-active` exits non-zero for anything but `active`.
                Ok(CommandOutput {
                    code: Some(if text == "active" { 0 } else { 3 }),
                    stdout: format!("{text}\n"),
                    stderr: String::new(),
                })
            }
            "show" => {
                let name = rest.get(1).copied().unwrap_or("");
                let prop = rest
                    .iter()
                    .find_map(|a| a.strip_prefix("--property="))
                    .unwrap_or("");
                let svc = state.services.get(name).cloned().unwrap_or_default();
                let value = match prop {
                    "MainPID" => svc.main_pid.to_string(),
                    "ActiveEnterTimestamp" => svc.active_enter,
                    "InactiveEnterTimestamp" => svc.inactive_enter,
                    _ => String::new(),
                };
                Ok(CommandOutput::ok(format!("{prop}={value}\n")))
            }
            other => Ok(CommandOutput::failed(format!("unsupported `{other}`"))),
        }
    }

    fn process_state(&self, pid: i32) -> ProcState {
        let state = self.state.lock().expect("fake state");
        match state.services.values().find(|s| s.main_pid == pid) {
            Some(svc) if svc.process_gone => ProcState::Gone,
            Some(svc) if svc.stopped => ProcState::Stopped,
            Some(_) => ProcState::Alive,
            None => ProcState::Gone,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(fake: &FakeSystemctl, args: &[&str]) -> CommandOutput {
        let mut full = vec!["--user".to_string()];
        full.extend(args.iter().map(|s| s.to_string()));
        fake.run("systemctl", &full).unwrap()
    }

    #[test]
    fn the_fake_emulates_a_service_lifecycle() {
        let fake = FakeSystemctl::new();
        assert_eq!(
            run(&fake, &["is-active", "mrsm-a.service"]).stdout,
            "inactive\n"
        );
        assert!(run(&fake, &["start", "mrsm-a.service"]).success());
        assert_eq!(
            run(&fake, &["is-active", "mrsm-a.service"]).stdout,
            "active\n"
        );
        let pid = fake.pid("mrsm-a.service").unwrap();
        assert_eq!(
            run(&fake, &["show", "mrsm-a.service", "--property=MainPID"]).stdout,
            format!("MainPID={pid}\n")
        );
        assert_eq!(fake.process_state(pid), ProcState::Alive);
        assert!(run(&fake, &["kill", "-s", "SIGSTOP", "mrsm-a.service"]).success());
        assert_eq!(fake.process_state(pid), ProcState::Stopped);
        assert!(run(&fake, &["kill", "-s", "SIGCONT", "mrsm-a.service"]).success());
        assert!(run(&fake, &["kill", "-s", "SIGINT", "mrsm-a.service"]).success());
        assert_eq!(
            run(&fake, &["is-active", "mrsm-a.service"]).stdout,
            "inactive\n"
        );
        assert_eq!(
            fake.rendered_calls().first().unwrap(),
            "is-active mrsm-a.service"
        );
    }

    #[test]
    fn failures_and_unavailability() {
        let fake = FakeSystemctl::new();
        fake.fail_on("enable");
        assert!(!run(&fake, &["enable", "mrsm-a.service"]).success());
        fake.set_unavailable(true);
        assert!(
            fake.run(
                "systemctl",
                &["--user".to_string(), "daemon-reload".to_string()]
            )
            .is_err()
        );
    }
}
