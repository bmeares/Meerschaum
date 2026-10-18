//! The job side of the `systemd` executor: what runs inside the unit.
//!
//! v4's `meerschaum/_internal/entry.py` sets this up at import when `MRSM_SYSTEMD_LOG_PATH` is in
//! the environment: stdout and stderr go to the service's rotating log (with timestamps), stdin
//! is the service's FIFO, and when the action finishes the result goes to
//! `MRSM_SYSTEMD_RESULT_PATH` (or, for `delete_after_completion`, the job deletes itself). The
//! unit's `ExecStart=` is `<program> -m meerschaum <sysargs>`.
//!
//! The `mrsm` binary has no such import step, so a host calls [`run_if_requested`] first thing in
//! `main` (after the job supervisor's own hook): when the systemd variables are present it
//! becomes a small wrapper which runs *itself* once more with the same arguments (marked by
//! [`CHILD_ENV`]), copies the child's output into the rotating log, feeds it the FIFO, forwards
//! termination signals, and records the result. [`strip_module_prefix`] drops the leading
//! `-m meerschaum` the unit passes (`python -m meerschaum` has no counterpart in a binary). Inside
//! the Python extension the unit is `<python> -P -m meerschaum <sysargs>` (v4's own line plus `-P`)
//! and the interpreter has already consumed `-P -m meerschaum`; the wrapped child is then
//! `python -P -m meerschaum <sysargs>` again (`meerschaum_core::self_exe`).

use meerschaum_core::static_config::static_config;

/// Set by systemd in every unit it runs.
pub const INVOCATION_ID_ENV: &str = "INVOCATION_ID";

/// Set in the wrapped child so that it does not wrap itself again.
pub const CHILD_ENV: &str = "MRSM_SYSTEMD_JOB_CHILD";

/// Remove a leading `-m meerschaum` pair (the unit's `ExecStart=` is shaped like
/// `python -m meerschaum ...` so that v4 can read the sysargs back).
pub fn strip_module_prefix(args: Vec<String>) -> Vec<String> {
    if args.len() >= 2 && args[0] == "-m" && args[1] == "meerschaum" {
        return args[2..].to_vec();
    }
    args
}

/// The variables of a systemd job, when this process runs inside one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemdJobEnv {
    /// The job's name (`MRSM_DAEMON_ID`).
    pub name: String,
    /// `MRSM_SYSTEMD_LOG_PATH`.
    pub log_path: std::path::PathBuf,
    /// `MRSM_SYSTEMD_RESULT_PATH`.
    pub result_path: Option<std::path::PathBuf>,
    /// `MRSM_SYSTEMD_STDIN_PATH`.
    pub stdin_path: Option<std::path::PathBuf>,
    /// `MRSM_SYSTEMD_DELETE_JOB` is neither unset, `0` nor `false`.
    pub delete_job: bool,
}

impl SystemdJobEnv {
    /// Read the job's variables from an environment (`None` outside a systemd job).
    pub fn from_env(env: &dyn Fn(&str) -> Option<String>) -> Option<SystemdJobEnv> {
        let sc = static_config();
        let log_path = env(&sc.environment.systemd_log_path)?;
        let delete = env(&sc.environment.systemd_delete_job).unwrap_or_default();
        Some(SystemdJobEnv {
            name: env(&sc.environment.daemon_id).unwrap_or_default(),
            log_path: log_path.into(),
            result_path: env(&sc.environment.systemd_result_path).map(Into::into),
            stdin_path: env(&sc.environment.systemd_stdin_path)
                .filter(|p| !p.is_empty())
                .map(Into::into),
            delete_job: !matches!(delete.as_str(), "" | "0" | "false"),
        })
    }
}

/// If this process was started by a `systemd` job unit, run the job through the wrapper and
/// exit with its status; otherwise return. Hosts call this first thing in `main`.
pub fn run_if_requested() {
    let args: Vec<String> = std::env::args_os()
        .skip(1)
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    run_if_requested_with(&args);
}

/// [`run_if_requested`] for a host whose real arguments are not `std::env::args_os()` (the
/// Python extension: the arguments after `python -m meerschaum`).
pub fn run_if_requested_with(args: &[String]) {
    if std::env::var_os(CHILD_ENV).is_some() {
        return;
    }
    let Some(job) = SystemdJobEnv::from_env(&|k| std::env::var(k).ok()) else {
        return;
    };
    let args = strip_module_prefix(args.to_vec());
    std::process::exit(imp::run(&job, &args));
}

#[cfg(not(unix))]
mod imp {
    use super::SystemdJobEnv;

    pub fn run(_job: &SystemdJobEnv, _args: &[String]) -> i32 {
        eprintln!("systemd jobs are not supported on this platform.");
        1
    }
}

#[cfg(unix)]
mod imp {
    use std::fs;
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use nix::sys::signal::{SigSet, Signal};
    use nix::unistd::Pid;

    use super::{CHILD_ENV, INVOCATION_ID_ENV, SystemdJobEnv};
    use crate::daemon::JobsContext;
    use crate::rotating::{Interceptor, RotatingFile, RotatingFileOptions};
    use crate::supervisor::RESULT_PATH_ENV;
    use crate::systemd::SystemdExecutor;

    pub fn run(job: &SystemdJobEnv, args: &[String]) -> i32 {
        let ctx = JobsContext::from_env().ok();
        let config = ctx.as_ref().map(|c| c.config.clone()).unwrap_or_default();
        let log = match RotatingFile::new(
            &job.log_path,
            RotatingFileOptions {
                num_files_to_keep: config.num_files_to_keep,
                max_file_size: config.max_file_size,
                write_timestamps: true,
                timestamp_format: config.timestamp_format.clone(),
            },
        ) {
            Ok(l) => Arc::new(Mutex::new(l)),
            Err(e) => {
                eprintln!(
                    "Cannot open the service log '{}': {e}",
                    job.log_path.display()
                );
                return 1;
            }
        };

        // Termination signals are forwarded to the child by a dedicated thread.
        let mut sigset = SigSet::empty();
        for s in [Signal::SIGTERM, Signal::SIGINT, Signal::SIGHUP] {
            sigset.add(s);
        }
        let _ = sigset.thread_block();

        // This program again: the binary itself, or `python -m meerschaum` in the extension.
        let (program, leading) = meerschaum_core::self_exe::self_program();
        let child_result = job.log_path.with_extension("child-result.json");
        let _ = fs::remove_file(&child_result);
        let mut cmd = Command::new(program);
        cmd.args(leading)
            .args(args)
            .env(CHILD_ENV, "1")
            .env(RESULT_PATH_ENV, &child_result)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match job
            .stdin_path
            .as_ref()
            .map(|p| crate::stdin::open_fifo_for_daemon(p))
        {
            Some(Ok(f)) => {
                cmd.stdin(Stdio::from(f));
            }
            Some(Err(e)) => {
                eprintln!("Cannot open the stdin FIFO: {e}");
                cmd.stdin(Stdio::null());
            }
            None => {
                cmd.stdin(Stdio::null());
            }
        }
        {
            use std::os::unix::process::CommandExt;
            // SAFETY: runs in the forked child before `exec` and only restores the default
            // dispositions of the termination signals and an empty signal mask (both
            // async-signal-safe), so a forwarded signal reaches the worker.
            unsafe {
                cmd.pre_exec(|| {
                    use nix::sys::signal::{SigHandler, signal};
                    for s in [Signal::SIGINT, Signal::SIGTERM, Signal::SIGHUP] {
                        signal(s, SigHandler::SigDfl).map_err(std::io::Error::from)?;
                    }
                    SigSet::empty()
                        .thread_set_mask()
                        .map_err(std::io::Error::from)?;
                    Ok(())
                });
            }
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to start the job: {e}");
                return 1;
            }
        };
        let pid = child.id() as i32;
        let forwarded = Arc::new(AtomicI32::new(0));
        // Inside a unit the service manager signals the whole control group (`systemctl kill`
        // and the stop's `KillSignal=` reach the wrapper *and* the job), so the job already has
        // the signal: forwarding it again would deliver a second SIGINT, which the job treats
        // as "force quit" (exit 130 in the middle of a chunk). `INVOCATION_ID` is what systemd
        // sets for every unit it runs; a hand-started wrapper has none and does forward.
        let in_unit = std::env::var_os(INVOCATION_ID_ENV).is_some();
        {
            let forwarded = forwarded.clone();
            std::thread::spawn(move || {
                loop {
                    if let Ok(sig) = sigset.wait() {
                        forwarded.store(sig as i32, Ordering::SeqCst);
                        if !in_unit {
                            let _ = nix::sys::signal::kill(Pid::from_raw(pid), sig);
                        }
                    }
                }
            });
        }

        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let readers: Vec<Box<dyn Read + Send>> = vec![
            Box::new(child.stdout.take().expect("piped stdout")),
            Box::new(child.stderr.take().expect("piped stderr")),
        ];
        let reset = Arc::new(AtomicBool::new(false));
        for mut reader in readers {
            let log = log.clone();
            let tx = tx.clone();
            let reset = reset.clone();
            std::thread::spawn(move || {
                let mut interceptor = Interceptor::new();
                let mut buf = vec![0u8; 1024];
                loop {
                    let n = match reader.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    if reset.swap(false, Ordering::Relaxed) {
                        interceptor.reset();
                    }
                    let Ok(mut l) = log.lock() else { break };
                    let prefix = l.get_timestamp_prefix_str();
                    let data = interceptor.transform(&buf[..n], &prefix);
                    let _ = l.write_raw(&data);
                }
                let _ = tx.send(());
            });
        }
        drop(tx);

        let status = child.wait();
        for _ in 0..2 {
            if rx.recv_timeout(Duration::from_secs(2)).is_err() {
                break;
            }
        }

        let result = read_result(&child_result).or_else(|| {
            status.as_ref().ok().map(|s| {
                if s.success() {
                    (true, "Success".to_string())
                } else {
                    (
                        false,
                        format!("Returned exit code: {}", s.code().unwrap_or(1)),
                    )
                }
            })
        });
        let _ = fs::remove_file(&child_result);
        if job.delete_job {
            if let Some(ctx) = ctx {
                use crate::executor::JobExecutor;
                let _ = SystemdExecutor::new(ctx).delete_job(&job.name);
            }
        } else if let (Some(path), Some((success, message))) = (&job.result_path, &result) {
            if path.parent().is_some_and(|p| p.exists()) {
                let json = serde_json::json!([success, message]).to_string();
                let _ = fs::write(path, json);
            }
        }

        // A job stopped on request (`stop` sends SIGINT) exits 0 so that systemd does not
        // restart it (`RestartPreventExitStatus=0`); a failing job exits non-zero.
        let asked_to_stop = forwarded.load(Ordering::SeqCst) != 0;
        match (&status, asked_to_stop) {
            (_, true) => 0,
            (Ok(s), _) => s.code().unwrap_or(1),
            (Err(_), _) => 1,
        }
    }

    fn read_result(path: &std::path::Path) -> Option<(bool, String)> {
        let text = fs::read_to_string(path).ok()?;
        match meerschaum_core::value::Value::from_json_str(&text).ok()? {
            meerschaum_core::value::Value::List(items) if items.len() == 2 => Some((
                items[0].as_bool()?,
                items[1].as_str().map(str::to_string).unwrap_or_default(),
            )),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_prefix_is_stripped_only_as_a_pair() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(
            strip_module_prefix(s(&["-m", "meerschaum", "show", "version"])),
            s(&["show", "version"])
        );
        assert_eq!(
            strip_module_prefix(s(&["-m", "metric", "show"])),
            s(&["-m", "metric", "show"])
        );
        assert_eq!(strip_module_prefix(s(&["show"])), s(&["show"]));
    }

    #[test]
    fn the_job_environment_is_read_like_entry_py() {
        let env: std::collections::HashMap<&str, &str> = [
            ("MRSM_DAEMON_ID", "my job"),
            (
                "MRSM_SYSTEMD_LOG_PATH",
                "/r/systemd/logs/mrsm-my-job.service.log",
            ),
            (
                "MRSM_SYSTEMD_RESULT_PATH",
                "/r/systemd/services/my job/x.result.json",
            ),
            (
                "MRSM_SYSTEMD_STDIN_PATH",
                "/r/systemd/services/my job/x.stdin",
            ),
            ("MRSM_SYSTEMD_DELETE_JOB", "1"),
        ]
        .into_iter()
        .collect();
        let job = SystemdJobEnv::from_env(&|k| env.get(k).map(|v| v.to_string())).expect("a job");
        assert_eq!(job.name, "my job");
        assert!(job.delete_job);
        assert!(job.stdin_path.is_some() && job.result_path.is_some());
        assert!(SystemdJobEnv::from_env(&|_| None).is_none());
        let mut keep = env.clone();
        keep.insert("MRSM_SYSTEMD_DELETE_JOB", "0");
        assert!(
            !SystemdJobEnv::from_env(&|k| keep.get(k).map(|v| v.to_string()))
                .unwrap()
                .delete_job
        );
    }
}
