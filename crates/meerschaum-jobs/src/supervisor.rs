//! The detached job supervisor: the Rust counterpart of `Daemon._run_exit` inside
//! `daemon.DaemonContext`.
//!
//! `Daemon::run` launches the supervisor with a double fork (`fork`, `setsid`, `fork`, `exec`): the
//! intermediate child exits immediately, so the supervisor is re-parented to init, has no
//! controlling terminal, and outlives the launching process. The supervisor then:
//!
//! 1. starts the worker (`<program> <sysargs>`) with stdin on the job's FIFO and stdout / stderr
//!    on two pipes;
//! 2. writes the worker's PID into `process.pid` (holding an `flock` on it, like Python's
//!    `pidfile=InterProcessLock(...)`), and records `result: null` and `process.began`;
//! 3. copies the worker's output into the rotating log, through one [`Interceptor`] per stream
//!    (the timestamp prefixes), reinstalling the interceptors every `refresh_files_seconds` like
//!    Python's log-refresh timer;
//! 4. forwards `SIGTERM` / `SIGINT` / `SIGHUP` it receives to the worker;
//! 5. when the worker exits, records `result` and `process.ended` (or removes the job when the
//!    output is not kept), and removes the PID file.
//!
//! The PID file holds the *worker's* PID, so `pause` (SIGSTOP), `stop` (SIGINT / SIGTERM) and the
//! Python `Job` class act on the process doing the work, exactly as with a Python daemon. The
//! supervisor's own command line carries `daemon_id='<id>'` so that `_find_detached_pids` (Python
//! and Rust) finds it when the PID file is missing.
//!
//! The worker may report its `SuccessTuple` by writing `[success, message]` as JSON to the path
//! in `MRSM_JOB_RESULT_PATH`; otherwise the result is derived from its exit status.

use std::path::PathBuf;
use std::process::Command;

use crate::config::Seconds;
use crate::startup::StartupPolicy;

/// The first argument that turns a host binary into the supervisor
/// (see [`run_if_requested`]).
pub const SUPERVISOR_ARG: &str = "__mrsm_job_supervisor__";

/// The environment variable through which a worker may report its result.
pub const RESULT_PATH_ENV: &str = "MRSM_JOB_RESULT_PATH";

/// `MRSM_JOB_SUPERVISOR`: override the supervisor command (shell-split).
pub const SUPERVISOR_ENV: &str = "MRSM_JOB_SUPERVISOR";

/// `MRSM_JOB_PROGRAM`: override the program which runs a job's sysargs (shell-split).
pub const PROGRAM_ENV: &str = "MRSM_JOB_PROGRAM";

/// The result file a worker may write (`jobs/<id>/.result.json`).
pub const RESULT_FILE_NAME: &str = ".result.json";

/// How jobs are launched: the supervisor command and the program which receives the sysargs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Launcher {
    /// The supervisor command (program and leading arguments).
    pub supervisor: Vec<String>,
    /// The program a job's sysargs are appended to (the `mrsm` binary by default).
    pub program: Vec<String>,
}

impl Launcher {
    /// `MRSM_JOB_SUPERVISOR` / `MRSM_JOB_PROGRAM` when set, else this program's own command
    /// ([`meerschaum_core::self_exe::self_command`]: the current executable, or
    /// `python -m meerschaum` inside the Python extension). The host must call
    /// [`run_if_requested`] (or [`run_if_requested_with`]) first thing.
    pub fn from_env() -> Launcher {
        let own = meerschaum_core::self_exe::self_command();
        let from_var = |var: &str| {
            std::env::var(var)
                .ok()
                .and_then(|v| crate::shlex::split(&v).ok())
                .filter(|v| !v.is_empty())
        };
        Launcher {
            supervisor: from_var(SUPERVISOR_ENV).unwrap_or_else(|| {
                let mut command = own.clone();
                command.push(SUPERVISOR_ARG.to_string());
                command
            }),
            program: from_var(PROGRAM_ENV).unwrap_or(own),
        }
    }
}

/// Everything the supervisor needs, resolved by the launching process.
#[derive(Debug, Clone, PartialEq)]
pub struct SupervisorOptions {
    /// The job's name.
    pub daemon_id: String,
    /// `DAEMON_RESOURCES_PATH`.
    pub jobs_dir: PathBuf,
    /// The rotating log's base path.
    pub log_path: PathBuf,
    /// `DAEMON_ERROR_LOG_PATH`.
    pub error_log_path: PathBuf,
    /// The log offset file (removed with the logs when the output is not kept).
    pub log_offset_path: PathBuf,
    /// `keep_daemon_output`.
    pub keep_output: bool,
    /// `logs.write_timestamps`.
    pub write_timestamps: bool,
    /// `logs.timestamp_format`.
    pub timestamp_format: String,
    /// `logs.num_files_to_keep`.
    pub num_files_to_keep: i64,
    /// `logs.max_file_size`.
    pub max_file_size: i64,
    /// `logs.refresh_files_seconds`.
    pub refresh_files_seconds: Seconds,
    /// `logs.stdin`: attach the FIFO as the worker's stdin.
    pub capture_stdin: bool,
    /// The worker's working directory.
    pub cwd: PathBuf,
    /// The worker command (program and sysargs).
    pub command: Vec<String>,
    /// The startup gate the supervisor passes before it starts the worker (`None`: none).
    pub startup: Option<StartupPolicy>,
}

impl SupervisorOptions {
    /// Encode as supervisor arguments.
    pub fn to_args(&self) -> Vec<String> {
        let b = |v: bool| if v { "1" } else { "0" }.to_string();
        let mut args = vec![
            crate::process::daemon_marker(&self.daemon_id),
            "--daemon-id".into(),
            self.daemon_id.clone(),
            "--jobs-dir".into(),
            self.jobs_dir.to_string_lossy().to_string(),
            "--log-path".into(),
            self.log_path.to_string_lossy().to_string(),
            "--error-log-path".into(),
            self.error_log_path.to_string_lossy().to_string(),
            "--log-offset-path".into(),
            self.log_offset_path.to_string_lossy().to_string(),
            "--keep-output".into(),
            b(self.keep_output),
            "--write-timestamps".into(),
            b(self.write_timestamps),
            "--timestamp-format".into(),
            self.timestamp_format.clone(),
            "--num-files".into(),
            self.num_files_to_keep.to_string(),
            "--max-file-size".into(),
            self.max_file_size.to_string(),
            "--refresh-seconds".into(),
            self.refresh_files_seconds.as_f64().to_string(),
            "--stdin".into(),
            b(self.capture_stdin),
            "--cwd".into(),
            self.cwd.to_string_lossy().to_string(),
        ];
        if let Some(policy) = &self.startup {
            args.push("--startup".into());
            args.push(policy.to_arg());
        }
        args.push("--".into());
        args.extend(self.command.iter().cloned());
        args
    }

    /// Decode supervisor arguments (with or without the leading [`SUPERVISOR_ARG`]).
    pub fn from_args<I: IntoIterator<Item = String>>(args: I) -> Result<SupervisorOptions, String> {
        let mut it = args.into_iter().peekable();
        let mut opts = SupervisorOptions {
            daemon_id: String::new(),
            jobs_dir: PathBuf::new(),
            log_path: PathBuf::new(),
            error_log_path: PathBuf::new(),
            log_offset_path: PathBuf::new(),
            keep_output: true,
            write_timestamps: true,
            timestamp_format: "%Y-%m-%d %H:%M".into(),
            num_files_to_keep: 5,
            max_file_size: 100_000,
            refresh_files_seconds: Seconds::Int(5),
            capture_stdin: true,
            cwd: PathBuf::from("."),
            command: Vec::new(),
            startup: None,
        };
        while let Some(arg) = it.next() {
            if arg == SUPERVISOR_ARG || arg.starts_with("daemon_id=") {
                continue;
            }
            if arg == "--" {
                opts.command = it.by_ref().collect();
                break;
            }
            let value = it
                .next()
                .ok_or_else(|| format!("Missing value for '{arg}'."))?;
            match arg.as_str() {
                "--daemon-id" => opts.daemon_id = value,
                "--jobs-dir" => opts.jobs_dir = value.into(),
                "--log-path" => opts.log_path = value.into(),
                "--error-log-path" => opts.error_log_path = value.into(),
                "--log-offset-path" => opts.log_offset_path = value.into(),
                "--keep-output" => opts.keep_output = value == "1",
                "--write-timestamps" => opts.write_timestamps = value == "1",
                "--timestamp-format" => opts.timestamp_format = value,
                "--num-files" => {
                    opts.num_files_to_keep = value.parse().map_err(|e| format!("{e}"))?
                }
                "--max-file-size" => {
                    opts.max_file_size = value.parse().map_err(|e| format!("{e}"))?
                }
                "--refresh-seconds" => {
                    opts.refresh_files_seconds =
                        Seconds::Float(value.parse().map_err(|e| format!("{e}"))?)
                }
                "--stdin" => opts.capture_stdin = value == "1",
                "--cwd" => opts.cwd = value.into(),
                "--startup" => opts.startup = Some(StartupPolicy::from_arg(&value)?),
                other => return Err(format!("Unknown supervisor argument '{other}'.")),
            }
        }
        if opts.daemon_id.is_empty() || opts.command.is_empty() {
            return Err("The supervisor needs a daemon ID and a command.".into());
        }
        Ok(opts)
    }
}

/// If this process was started as the job supervisor (its first argument is
/// [`SUPERVISOR_ARG`]), run the supervisor and exit. Hosts call this first thing in `main`.
pub fn run_if_requested() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    run_if_requested_with(&args);
}

/// [`run_if_requested`] for a host whose real command line is not `std::env::args()` (the
/// Python extension: the arguments after `python -m meerschaum`).
pub fn run_if_requested_with(args: &[String]) {
    if args.first().map(String::as_str) == Some(SUPERVISOR_ARG) {
        std::process::exit(main_with_args(args[1..].iter().cloned()));
    }
}

/// The supervisor's entry point: parse the arguments and supervise. Returns the exit code.
pub fn main_with_args<I: IntoIterator<Item = String>>(args: I) -> i32 {
    match SupervisorOptions::from_args(args) {
        Ok(opts) => supervise(opts),
        Err(e) => {
            eprintln!("{e}");
            2
        }
    }
}

/// Start `command` fully detached: double fork with `setsid`, stdio on `/dev/null`.
#[cfg(unix)]
pub fn spawn_detached(command: &mut Command) -> std::io::Result<()> {
    use std::os::unix::process::CommandExt;
    use std::process::Stdio;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: the closure runs in the forked child before `exec` and only calls the
    // async-signal-safe `setsid`, `fork` and `_exit`. The intermediate child `_exit`s at once;
    // the grandchild returns and `std` proceeds to `exec` the supervisor, so no allocation or
    // lock happens between `fork` and `exec` in either process.
    unsafe {
        command.pre_exec(|| {
            nix::unistd::setsid().map_err(std::io::Error::from)?;
            match nix::unistd::fork().map_err(std::io::Error::from)? {
                nix::unistd::ForkResult::Parent { .. } => libc::_exit(0),
                nix::unistd::ForkResult::Child => Ok(()),
            }
        });
    }
    let mut intermediate = command.spawn()?;
    intermediate.wait()?;
    Ok(())
}

/// Start `command` fully detached (not supported on this platform).
#[cfg(not(unix))]
pub fn spawn_detached(_command: &mut Command) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Cannot run background jobs on Windows.",
    ))
}

/// Run the supervisor (see the module documentation). Returns the exit code.
#[cfg(unix)]
pub fn supervise(opts: SupervisorOptions) -> i32 {
    imp::supervise(opts)
}

/// Run the supervisor (not supported on this platform).
#[cfg(not(unix))]
pub fn supervise(_opts: SupervisorOptions) -> i32 {
    eprintln!("Cannot run background jobs on Windows.");
    1
}

#[cfg(unix)]
mod imp {
    use std::fs::{self, File, OpenOptions};
    use std::io::{Read, Write};
    use std::os::fd::OwnedFd;
    use std::process::{Command, ExitStatus, Stdio};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use meerschaum_core::value::{Map, Value};
    use nix::fcntl::{Flock, FlockArg};
    use nix::sys::signal::{SigSet, Signal};
    use nix::unistd::Pid;

    use super::{RESULT_FILE_NAME, RESULT_PATH_ENV, SupervisorOptions};
    use crate::config::{DaemonPaths, JobPaths};
    use crate::properties::{merge_properties, read_properties, write_properties};
    use crate::rotating::{Interceptor, RotatingFile, RotatingFileOptions};

    fn log_error(opts: &SupervisorOptions, msg: &str) {
        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&opts.error_log_path)
        {
            let _ = write!(f, "Error in Daemon '{}':\n\n{msg}\n\n", opts.daemon_id);
        }
    }

    /// The in-memory properties of the supervisor, merged with the file like `Daemon.properties`.
    struct Props {
        path: std::path::PathBuf,
        mem: Map,
    }

    impl Props {
        fn get(&mut self) -> Map {
            let file = read_properties(&self.path);
            self.mem = merge_properties(&self.mem, file.as_ref());
            self.mem.clone()
        }

        fn set(&mut self, key: &str, value: Value) {
            let mut p = self.get();
            p.insert(key.to_string(), value);
            self.mem = p;
        }

        fn capture(&mut self, key: &str) {
            let mut p = self.get();
            let process = p
                .entry("process".to_string())
                .or_insert_with(Value::new_map);
            if !process.is_map() {
                *process = Value::new_map();
            }
            if let Some(m) = process.as_map_mut() {
                m.insert(
                    key.to_string(),
                    Value::Str(crate::pytime::naive_isoformat(crate::pytime::utcnow())),
                );
            }
            self.mem = p;
            self.write();
        }

        fn write(&mut self) {
            let p = self.get();
            let _ = write_properties(&self.path, &p);
        }
    }

    fn set_cloexec(fd: &OwnedFd) {
        use nix::fcntl::{FcntlArg, FdFlag, fcntl};
        let _ = fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC));
    }

    fn pipe() -> std::io::Result<(OwnedFd, OwnedFd)> {
        let (r, w) = nix::unistd::pipe().map_err(std::io::Error::from)?;
        set_cloexec(&r);
        set_cloexec(&w);
        Ok((r, w))
    }

    fn result_from_status(status: &ExitStatus, daemon_id: &str) -> Option<(bool, String)> {
        use std::os::unix::process::ExitStatusExt;
        if let Some(code) = status.code() {
            return Some(if code == 0 {
                (true, "Success".to_string())
            } else {
                (false, format!("Returned exit code: {code}"))
            });
        }
        match status.signal() {
            // PARITY: Python's SIGTERM handler raises `SystemExit(0)`, which `_run_exit` records
            // as `(True, "Stopped daemon '<id>'.")`.
            Some(s) if s == libc::SIGTERM => Some((true, format!("Stopped daemon '{daemon_id}'."))),
            // PARITY: an unhandled SIGINT surfaces as `KeyboardInterrupt`, recorded as a failure.
            Some(s) if s == libc::SIGINT => Some((false, "KeyboardInterrupt".to_string())),
            // PARITY: a SIGKILLed Python daemon never records a result (nor `ended`).
            Some(s) if s == libc::SIGKILL => None,
            Some(s) => Some((
                false,
                format!("Daemon '{daemon_id}' was terminated by signal {s}."),
            )),
            None => Some((false, "Unknown exit status.".to_string())),
        }
    }

    fn read_result_file(path: &std::path::Path) -> Option<(bool, String)> {
        let text = fs::read_to_string(path).ok()?;
        let _ = fs::remove_file(path);
        match Value::from_json_str(&text).ok()? {
            Value::List(items) if items.len() == 2 => {
                let success = items[0].as_bool()?;
                let msg = items[1].as_str().map(str::to_string).unwrap_or_default();
                Some((success, msg))
            }
            _ => None,
        }
    }

    /// Record the end of the run (`ended`), or remove the job and its logs when the output is not
    /// kept (`cleanup()`).
    fn finish(
        opts: &SupervisorOptions,
        paths: &JobPaths,
        dpaths: &DaemonPaths,
        props: &mut Props,
        log: &Arc<Mutex<RotatingFile>>,
    ) {
        if opts.keep_output {
            props.capture("ended");
        } else {
            if crate::names::is_direct_child(&paths.jobs_dir, &dpaths.dir, &opts.daemon_id) {
                let _ = fs::remove_dir_all(&dpaths.dir);
            }
            if let Ok(mut l) = log.lock() {
                l.delete(false);
            }
            let _ = fs::remove_file(&opts.log_offset_path);
        }
    }

    pub(super) fn supervise(opts: SupervisorOptions) -> i32 {
        let paths = JobPaths {
            jobs_dir: opts.jobs_dir.clone(),
            ..JobPaths::for_root(opts.jobs_dir.parent().unwrap_or(&opts.jobs_dir), "/")
        };
        let dpaths = DaemonPaths::new(&paths, &opts.daemon_id);
        let mut props = Props {
            path: dpaths.properties.clone(),
            mem: Map::new(),
        };

        // Signals are handled by a dedicated thread (`sigwait`); block them before any thread
        // starts so that every thread inherits the mask (the worker's mask is cleared below).
        let mut sigset = SigSet::empty();
        for s in [Signal::SIGTERM, Signal::SIGINT, Signal::SIGHUP] {
            sigset.add(s);
        }
        let _ = sigset.thread_block();

        let log = match RotatingFile::new(
            &opts.log_path,
            RotatingFileOptions {
                num_files_to_keep: opts.num_files_to_keep,
                max_file_size: opts.max_file_size,
                write_timestamps: opts.write_timestamps,
                timestamp_format: opts.timestamp_format.clone(),
            },
        ) {
            Ok(l) => l,
            Err(e) => {
                log_error(&opts, &e.to_string());
                return 1;
            }
        };
        let log = Arc::new(Mutex::new(log));
        if let Err(e) = log.lock().map(|mut l| l.refresh_files(0)).unwrap_or(Ok(())) {
            log_error(&opts, &format!("Failed to open the log: {e}"));
        }

        let stdin: Stdio = if opts.capture_stdin {
            match crate::stdin::open_fifo_for_daemon(&dpaths.stdin) {
                Ok(f) => Stdio::from(f),
                Err(e) => {
                    log_error(&opts, &format!("Cannot daemonize without stdin: {e}"));
                    Stdio::null()
                }
            }
        } else {
            Stdio::null()
        };

        let pipes = pipe().and_then(|o| pipe().map(|e| (o, e)));
        let ((out_r, out_w), (err_r, err_w)) = match pipes {
            Ok(p) => p,
            Err(e) => {
                log_error(&opts, &format!("Failed to create pipes: {e}"));
                return 1;
            }
        };

        // `process.pid`: under an exclusive `flock` for the job's lifetime. It names this
        // supervisor while the startup gate holds the worker back (a stop signals it), then the
        // worker.
        let _ = fs::create_dir_all(&dpaths.dir);
        let pid_lock = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&dpaths.pid)
            .ok()
            .and_then(|f| Flock::lock(f, FlockArg::LockExclusiveNonblock).ok());
        let _ = fs::write(&dpaths.pid, std::process::id().to_string());

        // Termination signals: before the worker exists they stop the wait at the startup gate;
        // afterwards they are forwarded to the worker. `signals.0` is the worker's PID (0 until
        // it runs), `signals.1` a signal which arrived before that.
        let signals = Arc::new(Mutex::new((0i32, 0i32)));
        let stopping = Arc::new(AtomicBool::new(false));
        {
            let signals = signals.clone();
            let stopping = stopping.clone();
            std::thread::spawn(move || {
                loop {
                    if let Ok(sig) = sigset.wait() {
                        let Ok(mut state) = signals.lock() else {
                            continue;
                        };
                        if state.0 > 0 {
                            let _ = nix::sys::signal::kill(Pid::from_raw(state.0), sig);
                        } else {
                            state.1 = sig as i32;
                            stopping.store(true, Ordering::SeqCst);
                        }
                    }
                }
            });
        }

        // The startup gate (see `crate::startup`): jitter, load and memory, then a slot.
        let write_log_line = |msg: &str| {
            if let Ok(mut l) = log.lock() {
                let line = if opts.write_timestamps {
                    format!("{}{msg}\n", l.get_timestamp_prefix_str())
                } else {
                    format!("{msg}\n")
                };
                let _ = l.write_raw(line.as_bytes());
            }
        };
        let admission = match &opts.startup {
            None => None,
            Some(policy) => {
                match crate::startup::admit(policy, &opts.daemon_id, &stopping, &mut |m| {
                    write_log_line(m)
                }) {
                    Ok(admission) => Some(admission),
                    Err(crate::startup::Stopped) => {
                        write_log_line("Startup: stopped before the job started.");
                        props.set(
                            "result",
                            Value::List(vec![
                                Value::Bool(true),
                                Value::Str(format!("Stopped daemon '{}'.", opts.daemon_id)),
                            ]),
                        );
                        finish(&opts, &paths, &dpaths, &mut props, &log);
                        drop(pid_lock);
                        let _ = fs::remove_file(&dpaths.pid);
                        let _ = fs::remove_file(&dpaths.stdin_block);
                        return 0;
                    }
                }
            }
        };
        let release_file = admission.as_ref().map(|a| a.release_file().to_path_buf());

        let result_path = dpaths.dir.join(RESULT_FILE_NAME);
        let _ = fs::remove_file(&result_path);
        let mut cmd = Command::new(&opts.command[0]);
        cmd.args(&opts.command[1..])
            .current_dir(&opts.cwd)
            .stdin(stdin)
            .stdout(Stdio::from(out_w))
            .stderr(Stdio::from(err_w))
            .env(RESULT_PATH_ENV, &result_path);
        if let Some(a) = &admission {
            cmd.env(crate::startup::RELEASE_ENV, a.release_env());
        }
        {
            use std::os::unix::process::CommandExt;
            // SAFETY: runs in the forked worker before `exec`; `sigaction` (via `signal`) and
            // `pthread_sigmask` are async-signal-safe and nothing is allocated. Termination
            // signals get their default dispositions and an empty mask, so a SIGINT / SIGTERM
            // from `stop` reaches the worker even if the launching shell ignored them (ignored
            // dispositions survive `exec`).
            unsafe {
                cmd.pre_exec(|| {
                    use nix::sys::signal::{SigHandler, signal};
                    for s in [
                        Signal::SIGINT,
                        Signal::SIGTERM,
                        Signal::SIGHUP,
                        Signal::SIGQUIT,
                    ] {
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
                drop(cmd);
                let msg = format!("Failed to start '{}': {e}", opts.command[0]);
                log_error(&opts, &msg);
                if let Ok(mut l) = log.lock() {
                    let _ = l.write_raw(format!("{msg}\n").as_bytes());
                }
                props.set(
                    "result",
                    Value::List(vec![Value::Bool(false), Value::Str(msg)]),
                );
                props.capture("ended");
                drop(admission);
                drop(pid_lock);
                let _ = fs::remove_file(&dpaths.pid);
                return 1;
            }
        };
        // Close our copies of the write ends so that the readers see EOF.
        drop(cmd);
        let pid = child.id() as i32;

        // `process.pid`: now the worker's PID (the `flock` taken above stays).
        let _ = fs::write(&dpaths.pid, pid.to_string());

        props.set("result", Value::Null);
        props.capture("began");

        // A signal which arrived while the worker was being started is forwarded now.
        if let Ok(mut state) = signals.lock() {
            state.0 = pid;
            if state.1 != 0
                && let Ok(sig) = Signal::try_from(state.1)
            {
                let _ = nix::sys::signal::kill(Pid::from_raw(pid), sig);
            }
        }

        // The slot is held until the worker's first lap is done, it exits, or the timeout.
        let exited = Arc::new(AtomicBool::new(false));
        if let Some(admission) = admission {
            let exited = exited.clone();
            let opts = opts.clone();
            let log = log.clone();
            let settle = Duration::from_secs_f64(
                opts.startup
                    .as_ref()
                    .map_or(0.0, |p| p.settle_seconds.max(0.0)),
            );
            std::thread::spawn(move || {
                admission.hold(&exited, Some((pid, settle)), &mut |m| {
                    if let Ok(mut l) = log.lock() {
                        let line = if opts.write_timestamps {
                            format!("{}{m}\n", l.get_timestamp_prefix_str())
                        } else {
                            format!("{m}\n")
                        };
                        let _ = l.write_raw(line.as_bytes());
                    }
                });
            });
        }

        // Python reinstalls the interceptors on every log-refresh tick.
        let resets = [
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];
        let done = Arc::new(AtomicBool::new(false));
        {
            let resets = resets.clone();
            let done = done.clone();
            let interval = opts
                .refresh_files_seconds
                .duration()
                .max(Duration::from_millis(10));
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    std::thread::sleep(interval);
                    for r in &resets {
                        r.store(true, Ordering::Relaxed);
                    }
                }
            });
        }

        let (tx, rx) = std::sync::mpsc::channel::<()>();
        for (fd, reset) in [(out_r, resets[0].clone()), (err_r, resets[1].clone())] {
            let log = log.clone();
            let tx = tx.clone();
            let write_timestamps = opts.write_timestamps;
            std::thread::spawn(move || {
                let mut file = File::from(fd);
                let mut interceptor = Interceptor::new();
                let mut buf = vec![0u8; 1024];
                loop {
                    let n = match file.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(_) => break,
                    };
                    if reset.swap(false, Ordering::Relaxed) {
                        interceptor.reset();
                    }
                    let Ok(mut l) = log.lock() else { break };
                    let data = if write_timestamps {
                        let prefix = l.get_timestamp_prefix_str();
                        interceptor.transform(&buf[..n], &prefix)
                    } else {
                        buf[..n].to_vec()
                    };
                    let _ = l.write_raw(&data);
                }
                let _ = tx.send(());
            });
        }
        drop(tx);

        let status = child.wait();
        exited.store(true, Ordering::SeqCst);
        if let Some(file) = &release_file {
            let _ = fs::remove_file(file);
        }
        // Drain the remaining output (a background grandchild may keep the pipes open).
        for _ in 0..2 {
            if rx.recv_timeout(Duration::from_secs(2)).is_err() {
                break;
            }
        }
        done.store(true, Ordering::Relaxed);

        let result = match &status {
            Ok(s) => {
                read_result_file(&result_path).or_else(|| result_from_status(s, &opts.daemon_id))
            }
            Err(e) => Some((false, e.to_string())),
        };

        if let Some((success, msg)) = result {
            props.set(
                "result",
                Value::List(vec![Value::Bool(success), Value::Str(msg)]),
            );
            finish(&opts, &paths, &dpaths, &mut props, &log);
        }

        // The PID file outlives a Python daemon until the next status check; remove it now if it
        // still names our worker (so a recycled PID is never mistaken for the job), or this
        // supervisor (a status read which found the job without its PID file wrote it back).
        if fs::read_to_string(&dpaths.pid)
            .ok()
            .as_deref()
            .map(str::trim)
            .is_some_and(|p| p == pid.to_string() || p == std::process::id().to_string())
        {
            let _ = fs::remove_file(&dpaths.pid);
        }
        drop(pid_lock);
        let _ = fs::remove_file(&dpaths.stdin_block);
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_round_trip() {
        let opts = SupervisorOptions {
            daemon_id: "my job".into(),
            jobs_dir: "/r/jobs".into(),
            log_path: "/r/logs/my job.log".into(),
            error_log_path: "/r/daemon_errors.log".into(),
            log_offset_path: "/r/logs/.my job.log.offset".into(),
            keep_output: false,
            write_timestamps: true,
            timestamp_format: "%H:%M".into(),
            num_files_to_keep: 3,
            max_file_size: 1000,
            refresh_files_seconds: Seconds::Float(0.5),
            capture_stdin: false,
            cwd: "/tmp".into(),
            command: vec!["mrsm".into(), "--".into(), "-x".into()],
            startup: Some(StartupPolicy {
                kind: crate::startup::StartKind::Bulk,
                jitter_seconds: 1.5,
                max_load: 1.5,
                min_free_memory_percent: 10.0,
                max_concurrent: 2,
                slot_timeout_seconds: 300.0,
                settle_seconds: 15.0,
                dir: "/r/jobs/.startup".into(),
            }),
        };
        let mut args = vec![SUPERVISOR_ARG.to_string()];
        args.extend(opts.to_args());
        assert!(args.iter().any(|a| a == "daemon_id='my job'"));
        assert_eq!(SupervisorOptions::from_args(args).unwrap(), opts);
    }
}
