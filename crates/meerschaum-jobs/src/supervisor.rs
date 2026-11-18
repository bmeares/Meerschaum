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
//! 3. copies the worker's output into the rotating log, through one [`Interceptor`](crate::rotating::Interceptor) per stream
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
//!
//! **A deleted job stays deleted.** The launcher records a random `process.launch_id` in the
//! job's properties and passes it as `--launch-id`. The supervisor never creates the job's
//! directory: when it starts, after the startup gate, and right after the worker started, it
//! checks [`launch_state`]. A job whose directory is gone (deleted between `start()` returning
//! and the supervisor starting) exits without writing anything (an already started worker is
//! stopped), and a launch superseded by a newer one (`process.launch_id` changed) yields to it.

use std::path::{Path, PathBuf};
use std::process::Command;

use meerschaum_core::value::Value;

use crate::config::Seconds;
use crate::startup::StartupPolicy;

#[path = "supervisor_deadline.rs"]
mod deadline;
#[path = "supervisor_owner.rs"]
mod owner;
pub use deadline::ComputeDeadline;
#[path = "supervisor_exec.rs"]
mod exec;
pub use exec::seal_native_command;
pub use owner::{
    FactoryPublication, JobGeneration, LifecycleOwner, MetadataResult, record_terminal_child,
    terminal_child_pid,
};
pub use owner::{
    cancel_unlaunched_terminal, claim_terminal_launch, complete_terminal_launch,
    prepare_terminal_launch, record_terminal_launch_child, stop_terminal_launches,
    terminal_launches_inactive,
};

/// The first argument that turns a host binary into the supervisor
/// (see [`run_if_requested`]).
pub const SUPERVISOR_ARG: &str = "__mrsm_job_supervisor__";

/// Only an exact native lifecycle entry selected by the host may precede command confinement.
/// Client/persisted env never supplies this reserved marker; a confined descendant forging it
/// still inherits its existing kernel restrictions and cannot regain the host's privileges.
pub fn trusted_launch(arguments: &[String]) -> bool {
    if meerschaum_core::self_exe::native_executable().is_none() {
        return false;
    }
    match std::env::var(meerschaum_core::team_scope::sandbox::runtime::LIFECYCLE_VAR).as_deref() {
        Ok("supervisor") => arguments.first().map(String::as_str) == Some(SUPERVISOR_ARG),
        Ok("systemd") => {
            arguments.first().map(String::as_str) != Some(SUPERVISOR_ARG)
                && std::env::var_os(crate::systemd_job::CHILD_ENV).is_none()
                && std::env::var_os(
                    &meerschaum_core::static_config::static_config()
                        .environment
                        .systemd_log_path,
                )
                .is_some()
        }
        _ => false,
    }
}

/// The environment variable through which a worker may report its result.
pub const RESULT_PATH_ENV: &str = "MRSM_JOB_RESULT_PATH";

/// `MRSM_JOB_SUPERVISOR`: override the supervisor command (shell-split).
pub const SUPERVISOR_ENV: &str = "MRSM_JOB_SUPERVISOR";

/// `MRSM_JOB_PROGRAM`: override the program which runs a job's sysargs (shell-split).
pub const PROGRAM_ENV: &str = "MRSM_JOB_PROGRAM";

/// The result file a worker may write (`jobs/<id>/.result.json`).
pub const RESULT_FILE_NAME: &str = ".result.json";

/// The failure recorded for a worker a signal ended (other than the SIGTERM / SIGINT a stop
/// sends): `Daemon '<id>' was killed by signal 9 (SIGKILL).`, or `... was terminated by signal
/// <n> (<NAME>).` for the others.
#[cfg(unix)]
pub fn signal_message(daemon_id: &str, signal: i32) -> String {
    let verb = if signal == libc::SIGKILL {
        "killed"
    } else {
        "terminated"
    };
    match signal_name(signal) {
        Some(name) => format!("Daemon '{daemon_id}' was {verb} by signal {signal} ({name})."),
        None => format!("Daemon '{daemon_id}' was {verb} by signal {signal}."),
    }
}

/// The name of a signal (`SIGKILL`), when this platform knows it.
#[cfg(unix)]
fn signal_name(signal: i32) -> Option<&'static str> {
    nix::sys::signal::Signal::try_from(signal)
        .ok()
        .map(|s| s.as_str())
}

/// The key under `properties['process']` that names the current launch (see [`launch_state`]).
pub const LAUNCH_ID_KEY: &str = "launch_id";

/// Whether a supervisor's launch is still the job's (see [`launch_state`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchState {
    /// The job exists and this is its current launch (or the launch carries no id).
    Current,
    /// The job was started again after this launch (`process.launch_id` differs).
    Superseded,
    /// The job's directory is gone: it was deleted.
    Deleted,
}

/// Whether the launch `launch_id` of the job whose files are `dir` / `properties` is still
/// current. A properties file that is being rewritten (missing or unparseable while the
/// directory exists) is read again for up to a second; one that stays unreadable, or a launch
/// without an id (an older launcher), counts as current, so a job is never refused on a
/// transient read.
pub fn launch_state(dir: &Path, properties: &Path, launch_id: Option<&str>) -> LaunchState {
    for _ in 0..20 {
        if !dir.is_dir() {
            return LaunchState::Deleted;
        }
        let Some(id) = launch_id else {
            return LaunchState::Current;
        };
        let parsed = std::fs::read_to_string(properties)
            .ok()
            .and_then(|text| Value::from_json_str(&text).ok())
            .and_then(|value| value.as_map().cloned());
        if let Some(props) = parsed {
            let recorded = props
                .get("process")
                .and_then(|p| p.get(LAUNCH_ID_KEY))
                .and_then(Value::as_str);
            return match recorded {
                Some(recorded) if recorded != id => LaunchState::Superseded,
                _ => LaunchState::Current,
            };
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    if dir.is_dir() {
        LaunchState::Current
    } else {
        LaunchState::Deleted
    }
}

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

/// Result bounds are selected by the trusted host, never a worker file or persisted env.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ResultProtocol {
    /// Legacy and ordinary jobs retain the two MiB encoded-file limit.
    #[default]
    Ordinary,
    /// Credential-free discovery, selected only by its signed native context.
    Metadata,
    /// Exact Main-created API connector registration, selected by its sealed descriptor.
    Registration,
    /// Exact Main-created extension, selected by its sealed private API descriptor.
    Extension,
}
impl ResultProtocol {
    /// Maximum encoded tuple file, accounting for JSON escapes without raising decoded caps.
    pub const fn envelope_limit(self) -> usize {
        match self {
            Self::Ordinary => 2 * 1024 * 1024,
            // A JSON escape uses at most six ASCII bytes for one input byte. This also
            // covers ensure_ascii surrogate pairs, quote-heavy documents and the tuple.
            Self::Metadata | Self::Extension => {
                meerschaum_core::team_scope::api_execution::MAX_METADATA_BYTES * 6 + 128
            }
            Self::Registration => {
                meerschaum_core::team_scope::api_execution::MAX_REGISTRATION_BYTES * 6 + 128
            }
        }
    }
    fn as_arg(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::Metadata => "metadata",
            Self::Registration => "registration",
            Self::Extension => "extension",
        }
    }
}

/// Everything the supervisor needs, resolved by the launching process.
#[derive(Debug, Clone, PartialEq)]
pub struct SupervisorOptions {
    /// Immutable host-selected Metadata compute lifetime. Renewal never replaces it.
    pub compute_deadline: Option<ComputeDeadline>,
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
    /// The launch this supervisor runs (`properties['process']['launch_id']`, see
    /// [`launch_state`]; `None`: an older launcher, nothing to compare).
    pub launch_id: Option<String>,
    /// Host-selected control API process. Its death stops this detached launch. Never read
    /// from job properties or environment; legacy standalone jobs have no lifecycle owner.
    pub lifecycle_owner: Option<LifecycleOwner>,
    /// Exact sealed API generation selected by the trusted runtime resolver.
    pub job_generation: Option<JobGeneration>,
    /// Signed-context-selected Metadata jobs may carry a larger encoded tuple only.
    pub result_protocol: ResultProtocol,
}

impl SupervisorOptions {
    fn validate_compute_profile(&self) -> Result<(), String> {
        if self.compute_deadline.is_some() != (self.result_protocol == ResultProtocol::Metadata)
            || (self.compute_deadline.is_some()
                && (self.lifecycle_owner.is_none() || self.job_generation.is_none()))
        {
            return Err(
                "A compute deadline requires the host-selected sealed Metadata profile.".into(),
            );
        }
        Ok(())
    }
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
        if let Some(launch_id) = &self.launch_id {
            args.push("--launch-id".into());
            args.push(launch_id.clone());
        }
        if let Some(owner) = &self.lifecycle_owner {
            args.push("--_mrsm-lifecycle-owner".into());
            args.push(owner.to_arg());
        }
        if let Some(deadline) = self.compute_deadline {
            args.push("--_mrsm-compute-deadline".into());
            args.push(deadline.expires_at.to_string());
        }
        if let Some(generation) = &self.job_generation {
            args.push("--_mrsm-job-generation".into());
            args.push(serde_json::to_string(generation).expect("fixed generation binding"));
        }
        args.push("--_mrsm-result-protocol".into());
        args.push(self.result_protocol.as_arg().into());
        args.push("--".into());
        args.extend(self.command.iter().cloned());
        args
    }

    /// Decode supervisor arguments (with or without the leading [`SUPERVISOR_ARG`]).
    pub fn from_args<I: IntoIterator<Item = String>>(args: I) -> Result<SupervisorOptions, String> {
        let mut it = args.into_iter().peekable();
        let mut opts = SupervisorOptions {
            compute_deadline: None,
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
            launch_id: None,
            lifecycle_owner: None,
            job_generation: None,
            result_protocol: ResultProtocol::Ordinary,
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
                "--launch-id" => opts.launch_id = Some(value),
                "--_mrsm-lifecycle-owner" => {
                    opts.lifecycle_owner = Some(LifecycleOwner::from_arg(&value)?)
                }
                "--_mrsm-compute-deadline" => {
                    opts.compute_deadline = Some(ComputeDeadline {
                        expires_at: value
                            .parse()
                            .map_err(|_| "Invalid host compute deadline.")?,
                    });
                }
                "--_mrsm-result-protocol" => {
                    opts.result_protocol = match value.as_str() {
                        "ordinary" => ResultProtocol::Ordinary,
                        "metadata" => ResultProtocol::Metadata,
                        "registration" => ResultProtocol::Registration,
                        "extension" => ResultProtocol::Extension,
                        _ => return Err("Invalid host result protocol.".into()),
                    };
                }
                "--_mrsm-job-generation" => {
                    if value.len() > 8192 {
                        return Err("Invalid job generation binding.".into());
                    }
                    opts.job_generation = Some(
                        serde_json::from_str(&value)
                            .map_err(|_| "Invalid job generation binding.")?,
                    );
                }
                other => return Err(format!("Unknown supervisor argument '{other}'.")),
            }
        }
        if opts.result_protocol != ResultProtocol::Ordinary
            && (opts.job_generation.is_none() || opts.lifecycle_owner.is_none())
        {
            return Err("Typed results require a host-owned sealed native generation.".into());
        }
        opts.validate_compute_profile()?;
        if opts.daemon_id.is_empty() || opts.command.is_empty() {
            return Err("The supervisor needs a daemon ID and a command.".into());
        }
        if let Some(generation) = &opts.job_generation {
            generation
                .validate(&opts.daemon_id)
                .map_err(|_| "Invalid job generation binding.")?;
            if opts.lifecycle_owner.is_none() {
                return Err("An API generation needs its native lifetime owner.".into());
            }
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

    use super::{LaunchState, RESULT_FILE_NAME, RESULT_PATH_ENV, SupervisorOptions, launch_state};
    use crate::config::{DaemonPaths, JobPaths};
    use crate::properties::{merge_properties, read_properties, write_existing_properties};
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

        /// Write the properties into the job's directory, which is never created again (a job
        /// deleted while it ran stays deleted).
        fn write(&mut self) {
            let p = self.get();
            let _ = write_existing_properties(&self.path, &p);
        }
    }

    /// Give up a launch that is no longer the job's: a deleted job's log files (created again
    /// by this supervisor) are removed; a superseded launch leaves the shared files to the newer
    /// one. The PID file is removed when it still names `pids`.
    fn abandon(
        opts: &SupervisorOptions,
        dpaths: &DaemonPaths,
        log: &Arc<Mutex<RotatingFile>>,
        state: LaunchState,
        pids: &[i32],
    ) {
        if state == LaunchState::Deleted {
            if let Ok(mut l) = log.lock() {
                l.delete(false);
            }
            let _ = fs::remove_file(&opts.log_offset_path);
        }
        remove_pid_file_naming(dpaths, pids);
    }

    /// Remove `process.pid` if it names one of `pids` (never another launch's process).
    fn remove_pid_file_naming(dpaths: &DaemonPaths, pids: &[i32]) {
        if fs::read_to_string(&dpaths.pid)
            .ok()
            .as_deref()
            .map(str::trim)
            .is_some_and(|p| pids.iter().any(|pid| p == pid.to_string()))
        {
            let _ = fs::remove_file(&dpaths.pid);
        }
    }

    /// Stop a worker started for a launch that turned out not to be the job's any more.
    fn stop_worker(child: &mut std::process::Child) {
        let pid = Pid::from_raw(child.id() as i32);
        let _ = nix::sys::signal::kill(pid, Signal::SIGTERM);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if matches!(child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = child.kill();
        let _ = child.wait();
    }

    /// Owner death never resumes a paused command. The direct Child stays unreaped while
    /// signalled, so its PID cannot be adopted by an unrelated process between these calls.
    fn stop_owned_worker(child: &mut std::process::Child) {
        if matches!(child.try_wait(), Ok(Some(_))) {
            return;
        }
        let pid = child.id() as i32;

        if crate::process::proc_state(pid) != crate::process::ProcState::Stopped {
            let _ = nix::sys::signal::kill(Pid::from_raw(pid), Signal::SIGTERM);
            let deadline = std::time::Instant::now() + Duration::from_secs(1);
            while std::time::Instant::now() < deadline {
                if matches!(child.try_wait(), Ok(Some(_))) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let _ = child.kill();
        let _ = child.wait();
    }

    struct OwnerWatch {
        done: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
    }
    impl OwnerWatch {
        fn start(
            owner: Option<super::LifecycleOwner>,
            deadline: Option<std::time::Instant>,
            stopping: Arc<AtomicBool>,
            dead: Arc<AtomicBool>,
            expired: Arc<AtomicBool>,
        ) -> Option<Self> {
            if owner.is_none() && deadline.is_none() {
                return None;
            }
            let done = Arc::new(AtomicBool::new(false));
            let finished = done.clone();
            let thread = std::thread::spawn(move || {
                while !finished.load(Ordering::SeqCst) {
                    if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                        expired.store(true, Ordering::SeqCst);
                        dead.store(true, Ordering::SeqCst);
                        stopping.store(true, Ordering::SeqCst);
                        return;
                    }
                    if owner.as_ref().is_some_and(|owner| !owner.alive()) {
                        dead.store(true, Ordering::SeqCst);
                        stopping.store(true, Ordering::SeqCst);
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            });
            Some(Self {
                done,
                thread: Some(thread),
            })
        }
    }
    impl Drop for OwnerWatch {
        fn drop(&mut self) {
            self.done.store(true, Ordering::SeqCst);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    const OWNER_ENDED: &str =
        "The executor runtime owner ended; explicitly start the job to run it again.";
    const COMPUTE_ENDED: &str = "Credential-free metadata reached its immutable compute deadline. Trigger a new admitted operation to discover again.";

    fn mark_owner_stopped(opts: &SupervisorOptions, paths: &DaemonPaths) {
        // Never recreate a deleted directory or stop a superseding launch. Opening the file
        // directly avoids write_stop_file's legacy create-parent behavior.
        if launch_state(&paths.dir, &paths.properties, opts.launch_id.as_deref())
            != LaunchState::Current
        {
            return;
        }
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&paths.stop)
        {
            let data = serde_json::json!({
                "action": "kill",
                "stop_time": crate::pytime::aware_isoformat(crate::pytime::utcnow()),
            });
            let _ = file.write_all(data.to_string().as_bytes());
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
            // v4 recorded nothing for a SIGKILLed daemon (the daemon was the process that died,
            // so the job read "No result available."). The supervisor outlives its worker and
            // records the failure with its cause.
            Some(s) => Some((false, super::signal_message(daemon_id, s))),
            None => Some((false, "Unknown exit status.".to_string())),
        }
    }

    #[cfg(test)]
    pub(super) fn read_result_file(path: &std::path::Path) -> Option<(bool, String)> {
        read_result_file_with_protocol(path, super::ResultProtocol::Ordinary)
    }
    pub(super) fn read_result_file_with_protocol(
        path: &std::path::Path,
        protocol: super::ResultProtocol,
    ) -> Option<(bool, String)> {
        let refused = || {
            Some((
                false,
                "The job result file is invalid or unavailable.".into(),
            ))
        };
        let bytes = match crate::untrusted_file::read_regular(path, protocol.envelope_limit(), true)
        {
            Ok(None) => return None,
            Ok(Some(bytes)) => bytes,
            Err(_) => return refused(),
        };
        let Ok(text) = std::str::from_utf8(&bytes) else {
            return refused();
        };
        match Value::from_json_str(text) {
            Ok(Value::List(items)) if items.len() == 2 => {
                match (items[0].as_bool(), items[1].as_str()) {
                    (Some(_), Some(message)) if match protocol {
                        super::ResultProtocol::Ordinary => false,
                        super::ResultProtocol::Metadata | super::ResultProtocol::Extension => message.len() > meerschaum_core::team_scope::api_execution::MAX_METADATA_BYTES,
                        super::ResultProtocol::Registration => message.len() > meerschaum_core::team_scope::api_execution::MAX_REGISTRATION_BYTES,
                    } => refused(),
                    (Some(success), Some(message)) => Some((success, message.into())),
                    _ => refused(),
                }
            }
            _ => refused(),
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

    struct NativeLifecycle {
        children: Option<crate::process_children::OwnedChildren>,
        generation: Option<super::JobGeneration>,
        spawned: bool,
    }
    impl NativeLifecycle {
        fn retire(&mut self) -> std::io::Result<()> {
            if let Some(children) = self.children.take() {
                drop(children);
                if let Some(generation) = &self.generation {
                    super::owner::record_job_cleanup(generation, !self.spawned)?;
                }
            }
            Ok(())
        }
    }
    impl Drop for NativeLifecycle {
        fn drop(&mut self) {
            let _ = self.retire();
        }
    }

    pub(super) fn supervise(opts: SupervisorOptions) -> i32 {
        if let Err(message) = opts.validate_compute_profile() {
            log_error(&opts, &message);
            return 1;
        }
        // The signed Main deadline is resolved once before any worker can start. Credential
        // rotation, a backward wall-clock jump or child-controlled state cannot extend it.
        let compute_deadline = match opts
            .compute_deadline
            .map(super::ComputeDeadline::pin)
            .transpose()
        {
            Ok(deadline) => deadline,
            Err(error) => {
                log_error(&opts, &error.to_string());
                return 1;
            }
        };
        let mut children = if opts.lifecycle_owner.is_some() {
            match crate::process_children::OwnedChildren::prepare() {
                Ok(children) => Some(NativeLifecycle {
                    children: Some(children),
                    generation: opts.job_generation.clone(),
                    spawned: false,
                }),
                Err(error) => {
                    eprintln!("The native job lifecycle cannot inspect its descendants: {error}");
                    return 1;
                }
            }
        } else {
            None
        };
        let paths = JobPaths {
            jobs_dir: opts.jobs_dir.clone(),
            ..JobPaths::for_root(opts.jobs_dir.parent().unwrap_or(&opts.jobs_dir), "/")
        };
        let dpaths = DaemonPaths::new(&paths, &opts.daemon_id);
        let launch_id = opts.launch_id.as_deref();
        // A job deleted before this supervisor got here, or started again since, is left alone:
        // nothing is created, written or run.
        if launch_state(&dpaths.dir, &dpaths.properties, launch_id) != LaunchState::Current {
            return 0;
        }
        let _publication = if opts.lifecycle_owner.is_some() && opts.job_generation.is_none() {
            match launch_id.and_then(|id| super::FactoryPublication::claim(&dpaths.dir, id).ok()) {
                Some(guard) => Some(guard),
                None => {
                    log_error(
                        &opts,
                        "The native supervisor could not claim its result publication.",
                    );
                    return 1;
                }
            }
        } else {
            None
        };
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
        // worker. The job's directory is never created here: when it is gone, the job was
        // deleted while this supervisor started.
        let own_pid = std::process::id() as i32;
        let pid_lock = match OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&dpaths.pid)
        {
            Ok(file) => Flock::lock(file, FlockArg::LockExclusiveNonblock).ok(),
            Err(_) if !dpaths.dir.is_dir() => {
                abandon(&opts, &dpaths, &log, LaunchState::Deleted, &[]);
                return 0;
            }
            Err(_) => None,
        };
        let _ = fs::write(&dpaths.pid, own_pid.to_string());

        // Termination signals: before the worker exists they stop the wait at the startup gate;
        // afterwards they are forwarded to the worker. `signals.0` is the worker's PID (0 until
        // it runs), `signals.1` a signal which arrived before that.
        let signals = Arc::new(Mutex::new((0i32, 0i32)));
        let stopping = Arc::new(AtomicBool::new(false));
        let owner_dead = Arc::new(AtomicBool::new(false));
        let deadline_expired = Arc::new(AtomicBool::new(false));
        let lifecycle_message = || {
            if deadline_expired.load(Ordering::SeqCst) {
                COMPUTE_ENDED
            } else {
                OWNER_ENDED
            }
        };
        let owner_watch = OwnerWatch::start(
            opts.lifecycle_owner.clone(),
            compute_deadline,
            stopping.clone(),
            owner_dead.clone(),
            deadline_expired.clone(),
        );
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
                        let lost_owner = owner_dead.load(Ordering::SeqCst);
                        if lost_owner {
                            mark_owner_stopped(&opts, &dpaths);
                        }
                        props.set(
                            "result",
                            Value::List(vec![
                                Value::Bool(!lost_owner),
                                Value::Str(if lost_owner {
                                    lifecycle_message().into()
                                } else {
                                    format!("Stopped daemon '{}'.", opts.daemon_id)
                                }),
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

        if owner_dead.load(Ordering::SeqCst)
            || opts
                .lifecycle_owner
                .as_ref()
                .is_some_and(|owner| !owner.alive())
        {
            mark_owner_stopped(&opts, &dpaths);
            props.set(
                "result",
                Value::List(vec![
                    Value::Bool(false),
                    Value::Str(lifecycle_message().into()),
                ]),
            );
            finish(&opts, &paths, &dpaths, &mut props, &log);
            drop(pid_lock);
            remove_pid_file_naming(&dpaths, &[own_pid]);
            return 0;
        }

        // The job may have been deleted (or started again) while this supervisor waited.
        let state = launch_state(&dpaths.dir, &dpaths.properties, launch_id);
        if state != LaunchState::Current {
            drop(admission);
            abandon(&opts, &dpaths, &log, state, &[own_pid]);
            drop(pid_lock);
            return 0;
        }

        // 5.1: a sandboxed team job's command may write only in its work directory: its result,
        // heartbeat and start-up release go there (the job's properties and PID file, which
        // this supervisor and the server trust, stay out of its reach).
        let work_dir = meerschaum_core::team_scope::sandbox::job_work_dir_from_env();
        let mut admission = admission;
        if let (Some(work), Some(a)) = (&work_dir, admission.as_mut()) {
            a.relocate_release(work);
        }
        let release_file = admission.as_ref().map(|a| a.release_file().to_path_buf());
        let output_dir = work_dir.clone().unwrap_or_else(|| dpaths.dir.clone());
        let result_path = output_dir.join(RESULT_FILE_NAME);
        let _ = fs::remove_file(&result_path);
        let mut cmd = Command::new(&opts.command[0]);
        cmd.env_remove(meerschaum_core::team_scope::sandbox::runtime::LIFECYCLE_VAR);
        cmd.args(&opts.command[1..])
            .current_dir(&opts.cwd)
            .stdin(stdin)
            .stdout(Stdio::from(out_w))
            .stderr(Stdio::from(err_w))
            .env(RESULT_PATH_ENV, &result_path)
            // The worker's lap heartbeats (job health): see `crate::health`.
            .env(
                crate::health::LAPS_PATH_ENV,
                output_dir.join(crate::health::LAPS_FILE_NAME),
            )
            .env(
                crate::health::LAPS_PARENT_ENV,
                std::process::id().to_string(),
            );
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

        if opts.lifecycle_owner.is_some()
            && let Err(error) = super::seal_native_command(&mut cmd)
        {
            log_error(
                &opts,
                &format!("The native descriptor barrier is unavailable: {error}"),
            );
            return 1;
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
        if let Some(lifecycle) = children.as_mut() {
            lifecycle.spawned = true;
        }
        // Close our copies of the write ends so that the readers see EOF.
        drop(cmd);
        let pid = child.id() as i32;

        if let Some(generation) = &opts.job_generation
            && super::owner::record_job_child(generation, pid).is_err()
        {
            stop_owned_worker(&mut child);
            props.set(
                "result",
                Value::List(vec![
                    Value::Bool(false),
                    Value::Str("The native job lifecycle proof could not be recorded.".into()),
                ]),
            );
            props.capture("ended");
            drop(admission);
            drop(pid_lock);
            let _ = fs::remove_file(&dpaths.pid);
            return 1;
        }

        // `process.pid`: now the worker's PID (the `flock` taken above stays).
        let _ = fs::write(&dpaths.pid, pid.to_string());

        // Deleted (or started again) in the moment the worker took to start: stop it, record
        // nothing.
        let state = launch_state(&dpaths.dir, &dpaths.properties, launch_id);
        if state != LaunchState::Current {
            stop_worker(&mut child);
            drop(admission);
            if let Some(file) = &release_file {
                let _ = fs::remove_file(file);
            }
            abandon(&opts, &dpaths, &log, state, &[pid, own_pid]);
            drop(pid_lock);
            return 0;
        }

        props.set("result", Value::Null);
        props.capture("began");

        // `input.stdin.block` while the worker (or any of its descendants) waits for input:
        // see `crate::stdin_watch`.
        let stdin_watch = opts.capture_stdin.then(|| {
            crate::stdin_watch::StdinWatch::spawn(&dpaths.stdin, &dpaths.stdin_block, pid)
        });
        let nudger = stdin_watch.as_ref().map(|w| w.nudger());

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
            let nudger = nudger.clone();
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
                    drop(l);
                    if let Some(n) = &nudger {
                        n.nudge();
                    }
                }
                let _ = tx.send(());
            });
        }
        drop(tx);

        let status = if opts.lifecycle_owner.is_some() {
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => break Ok(status),
                    Err(error) => {
                        stop_owned_worker(&mut child);
                        break Err(error);
                    }
                    Ok(None) => {}
                }
                if owner_dead.load(Ordering::SeqCst) {
                    mark_owner_stopped(&opts, &dpaths);
                    stop_owned_worker(&mut child);
                    break child.wait();
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        } else {
            child.wait()
        };
        // Daemonized workers remain descendants even after setsid or their parent exits.
        // Retire them before status/finished records could authorize another generation.
        if let Some(mut lifecycle) = children.take()
            && lifecycle.retire().is_err()
        {
            props.set(
                "result",
                Value::List(vec![
                    Value::Bool(false),
                    Value::Str("The native descendant cleanup proof could not be recorded.".into()),
                ]),
            );
            props.capture("ended");
            return 1;
        }
        drop(owner_watch);
        exited.store(true, Ordering::SeqCst);
        // A job which ended while it waited for input waits no more.
        if let Some(w) = &stdin_watch {
            w.halt();
        }
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

        let mut received_metadata_result = false;
        let mut result = if owner_dead.load(Ordering::SeqCst) {
            let _ = fs::remove_file(&result_path);
            Some((false, lifecycle_message().into()))
        } else {
            match &status {
                // The worker writes its result as it exits: after a signal ended it, a result file
                // came from someone else (a descendant that inherited `MRSM_JOB_RESULT_PATH`), and
                // the signal is the result.
                Ok(s) if std::os::unix::process::ExitStatusExt::signal(s).is_some() => {
                    let _ = fs::remove_file(&result_path);
                    result_from_status(s, &opts.daemon_id)
                }
                Ok(s) => {
                    let decoded =
                        read_result_file_with_protocol(&result_path, opts.result_protocol);
                    received_metadata_result =
                        decoded.as_ref().is_some_and(|(success, _)| *success);
                    decoded.or_else(|| result_from_status(s, &opts.daemon_id))
                }
                Err(e) => Some((false, e.to_string())),
            }
        };

        if opts.result_protocol == super::ResultProtocol::Metadata
            && result.as_ref().is_some_and(|(success, _)| *success)
        {
            // Only an actual bounded result-file tuple and normal worker exit may become
            // a native completion receipt. Exit-status fallback never establishes provenance.
            let recorded = match (
                opts.job_generation.as_ref(),
                opts.launch_id.as_deref(),
                opts.compute_deadline,
                result.as_ref(),
            ) {
                (Some(generation), Some(launch), Some(deadline), Some((true, message)))
                    if received_metadata_result
                        && status.as_ref().is_ok_and(|status| status.success()) =>
                {
                    super::owner::record_metadata_result(
                        generation,
                        launch,
                        deadline.expires_at,
                        true,
                        true,
                        message,
                    )
                }
                _ => Err(std::io::Error::other("Missing native Metadata completion.")),
            };
            if recorded.is_err() {
                result = Some((
                    false,
                    "The native Metadata completion receipt could not be recorded.".into(),
                ));
            }
        }

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
        remove_pid_file_naming(&dpaths, &[pid, own_pid]);
        drop(pid_lock);
        drop(stdin_watch);
        let _ = fs::remove_file(&dpaths.stdin_block);
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn typed_result_protocol_bounds_ascii_escapes_and_non_bmp_without_raising_ordinary_limits() {
        fn envelope(message: &str) -> String {
            use std::fmt::Write;
            let mut output = String::from("[true,\"");
            for character in message.chars() {
                match character {
                    '\"' => output.push_str("\\\""),
                    '\\' => output.push_str("\\\\"),
                    character if !character.is_ascii() || character.is_control() => {
                        let mut encoded = [0u16; 2];
                        for unit in character.encode_utf16(&mut encoded) {
                            write!(&mut output, "\\u{unit:04x}").unwrap();
                        }
                    }
                    character => output.push(character),
                }
            }
            output.push_str("\"]");
            output
        }
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("result.json");
        let limit = meerschaum_core::team_scope::api_execution::MAX_METADATA_BYTES;
        for message in ["\"\\".repeat(limit / 2 - 32), "é😀".repeat(limit / 6 - 32)] {
            let encoded = envelope(&message);
            assert!(encoded.len() > ResultProtocol::Ordinary.envelope_limit());
            assert!(encoded.len() <= ResultProtocol::Metadata.envelope_limit());
            std::fs::write(&path, &encoded).unwrap();
            assert!(!imp::read_result_file(&path).unwrap().0);
            for protocol in [ResultProtocol::Metadata, ResultProtocol::Extension] {
                std::fs::write(&path, &encoded).unwrap();
                let result = imp::read_result_file_with_protocol(&path, protocol).unwrap();
                assert!(result.0 && result.1 == message);
            }
        }
        let oversized = "x".repeat(limit + 1);
        for protocol in [ResultProtocol::Metadata, ResultProtocol::Extension] {
            std::fs::write(&path, envelope(&oversized)).unwrap();
            assert!(
                !imp::read_result_file_with_protocol(&path, protocol)
                    .unwrap()
                    .0
            );
        }
        let limit = meerschaum_core::team_scope::api_execution::MAX_REGISTRATION_BYTES;
        let message = "é😀".repeat(limit / 6 - 32);
        let encoded = envelope(&message);
        assert!(encoded.len() > ResultProtocol::Ordinary.envelope_limit());
        std::fs::write(&path, &encoded).unwrap();
        let result =
            imp::read_result_file_with_protocol(&path, ResultProtocol::Registration).unwrap();
        assert!(result.0 && result.1 == message);
        std::fs::write(&path, envelope(&"x".repeat(limit + 1))).unwrap();
        assert!(
            !imp::read_result_file_with_protocol(&path, ResultProtocol::Registration)
                .unwrap()
                .0
        );
    }

    #[test]
    fn unsafe_or_malformed_worker_result_is_a_failure_instead_of_exit_success() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("result.json");
        assert!(
            imp::read_result_file(&path).is_none(),
            "Missing legacy result retains exit-status behavior."
        );
        std::fs::write(&path, br#"[true,"owned"]"#).unwrap();
        assert_eq!(imp::read_result_file(&path), Some((true, "owned".into())));
        for bytes in [b"malformed".as_slice(), br#"[true,123]"#] {
            std::fs::write(&path, bytes).unwrap();
            assert!(!imp::read_result_file(&path).unwrap().0);
        }
        let outside = root.path().join("outside.json");
        std::fs::write(&outside, br#"[true,"outside"]"#).unwrap();
        symlink(&outside, &path).unwrap();
        assert!(!imp::read_result_file(&path).unwrap().0);
        assert_eq!(std::fs::read(&outside).unwrap(), br#"[true,"outside"]"#);
        std::fs::write(&path, vec![b'x'; 2 * 1024 * 1024 + 1]).unwrap();
        assert!(!imp::read_result_file(&path).unwrap().0);
    }

    #[test]
    fn args_round_trip() {
        let opts = SupervisorOptions {
            compute_deadline: None,
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
            launch_id: Some("aB3".into()),
            lifecycle_owner: LifecycleOwner::current().ok(),
            job_generation: None,
            result_protocol: ResultProtocol::Ordinary,
        };
        let mut args = vec![SUPERVISOR_ARG.to_string()];
        args.extend(opts.to_args());
        assert!(args.iter().any(|a| a == "daemon_id='my job'"));
        assert_eq!(SupervisorOptions::from_args(args).unwrap(), opts);
    }

    #[test]
    fn launch_states() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("jobs").join("x");
        let props = dir.join("properties.json");
        assert_eq!(launch_state(&dir, &props, Some("a")), LaunchState::Deleted);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&props, r#"{"process": {"launch_id": "a"}}"#).unwrap();
        assert_eq!(launch_state(&dir, &props, Some("a")), LaunchState::Current);
        assert_eq!(
            launch_state(&dir, &props, Some("b")),
            LaunchState::Superseded
        );
        // A launcher without ids, or properties without one, cannot be compared.
        assert_eq!(launch_state(&dir, &props, None), LaunchState::Current);
        std::fs::write(&props, r#"{"label": "x"}"#).unwrap();
        assert_eq!(launch_state(&dir, &props, Some("b")), LaunchState::Current);
        // A file that stays unreadable is not taken for a deleted job.
        std::fs::write(&props, "{").unwrap();
        assert_eq!(launch_state(&dir, &props, Some("b")), LaunchState::Current);
    }
}
