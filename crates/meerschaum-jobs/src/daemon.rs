//! Port of `meerschaum.utils.daemon.Daemon`: the on-disk state of one job and its supervision.

use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};

use crate::config::{DaemonPaths, JobPaths, JobsConfig, Seconds};
use crate::process::{self, ProcState, Sig};
use crate::properties::{self, merge_properties};
use crate::rotating::{RotatingFile, RotatingFileOptions};
use crate::stdin::StdinFile;
use crate::supervisor::{Launcher, SupervisorOptions};

/// The module and function Python daemons run (`target=entry`).
pub const TARGET_MODULE: &str = "meerschaum._internal.entry";
/// See [`TARGET_MODULE`].
pub const TARGET_NAME: &str = "entry";

/// Where jobs live and how they run: paths, configuration and launcher.
#[derive(Debug, Clone)]
pub struct JobsContext {
    /// The root's job-related paths.
    pub paths: JobPaths,
    /// The `jobs` configuration.
    pub config: JobsConfig,
    /// How the supervisor and the worker are started.
    pub launcher: Launcher,
}

impl JobsContext {
    /// From the process environment (`MRSM_ROOT_DIR`, config files, `MRSM_JOB_*`).
    pub fn from_env() -> Result<JobsContext, String> {
        let config = meerschaum_core::config::Config::from_env().map_err(|e| e.to_string())?;
        Ok(JobsContext {
            paths: JobPaths::from_config(&config),
            config: JobsConfig::from_config(&config),
            launcher: Launcher::from_env(),
        })
    }

    /// For a root directory with the default configuration.
    pub fn for_root(root: impl Into<PathBuf>) -> JobsContext {
        JobsContext {
            paths: JobPaths::for_root(root, crate::config::default_dot_config_dir()),
            config: JobsConfig::default(),
            launcher: Launcher::from_env(),
        }
    }

    /// Replace the launcher.
    pub fn with_launcher(mut self, launcher: Launcher) -> JobsContext {
        self.launcher = launcher;
        self
    }
}

/// `Daemon.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// `'running'`.
    Running,
    /// `'paused'`.
    Paused,
    /// `'stopped'`.
    Stopped,
}

impl Status {
    /// The Python string.
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Running => "running",
            Status::Paused => "paused",
            Status::Stopped => "stopped",
        }
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Errors constructing a [`Daemon`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DaemonError {
    /// The daemon's properties file does not exist.
    #[error("Daemon '{0}' does not exist. Pass a target to create a new Daemon.")]
    NotFound(String),
}

/// A daemon (the process side of a job) and its directory `jobs/<daemon_id>/`.
#[derive(Debug, Clone)]
pub struct Daemon {
    /// The job's name.
    pub daemon_id: String,
    ctx: JobsContext,
    paths: DaemonPaths,
    /// `_properties`.
    mem: Map,
    target_args: Vec<String>,
    /// Whether `target_args` came from `properties['target']` (Rust-written jobs).
    has_target: bool,
    label: String,
}

impl Daemon {
    /// `Daemon(daemon_id=...)` for an existing daemon (its `properties.json` must exist).
    ///
    /// Python reads the sysargs from `pickle.pkl`; Rust reads `target.args[0]` from the properties
    /// (written by Rust) or splits the `label` (Python never persists `target`, see
    /// [`Daemon::setup`]).
    pub fn open(daemon_id: &str, ctx: &JobsContext) -> Result<Daemon, DaemonError> {
        let paths = DaemonPaths::new(&ctx.paths, daemon_id);
        let Some(props) = properties::read_properties(&paths.properties) else {
            return Err(DaemonError::NotFound(daemon_id.to_string()));
        };
        let label = props
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or(daemon_id)
            .to_string();
        let from_target = sysargs_from_properties(&props);
        let has_target = from_target.is_some();
        let target_args = from_target
            .or_else(|| crate::shlex::split(&label).ok())
            .unwrap_or_default();
        Ok(Daemon {
            daemon_id: daemon_id.to_string(),
            ctx: ctx.clone(),
            paths,
            mem: Map::new(),
            target_args,
            has_target,
            label,
        })
    }

    /// `Daemon(target=entry, target_args=[sysargs], daemon_id=..., label=..., properties=...)`.
    pub fn new(
        daemon_id: &str,
        sysargs: Vec<String>,
        label: Option<String>,
        properties: Map,
        ctx: &JobsContext,
    ) -> Daemon {
        let label = label.unwrap_or_else(|| TARGET_NAME.to_string());
        let mut mem = properties;
        mem.insert("label".into(), Value::Str(label.clone()));
        Daemon {
            daemon_id: daemon_id.to_string(),
            ctx: ctx.clone(),
            paths: DaemonPaths::new(&ctx.paths, daemon_id),
            mem,
            target_args: sysargs,
            has_target: true,
            label,
        }
    }

    /// The daemon's file layout.
    pub fn paths(&self) -> &DaemonPaths {
        &self.paths
    }

    /// The context (paths, config, launcher).
    pub fn context(&self) -> &JobsContext {
        &self.ctx
    }

    /// `label`.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// `target_args[0]`: the job's sysargs.
    pub fn sysargs(&self) -> &[String] {
        &self.target_args
    }

    /// `properties['target']['args'][0]`, if the properties carry a target (Python-written
    /// properties never do; Python then reads the sysargs from `pickle.pkl`).
    pub fn target_sysargs(&self) -> Option<&[String]> {
        self.has_target.then_some(self.target_args.as_slice())
    }

    /// `hidden`: names starting with `_` or `.`.
    pub fn hidden(&self) -> bool {
        self.daemon_id.starts_with('_') || self.daemon_id.starts_with('.')
    }

    /// `read_properties()`.
    pub fn read_properties(&self) -> Option<Map> {
        properties::read_properties(&self.paths.properties)
    }

    /// The getter `properties`: the file's properties patched with the in-memory ones.
    pub fn properties(&mut self) -> Map {
        let file = self.read_properties();
        self.mem = merge_properties(&self.mem, file.as_ref());
        self.mem.clone()
    }

    /// Patch the in-memory properties (`self._properties.update(...)`).
    pub fn update_properties(&mut self, patch: &Map) {
        for (k, v) in patch {
            self.mem.insert(k.clone(), v.clone());
        }
    }

    /// Pop a key from the in-memory properties.
    pub fn pop_property(&mut self, key: &str) -> Option<Value> {
        self.mem.shift_remove(key)
    }

    /// `write_properties()`.
    pub fn write_properties(&mut self) -> SuccessTuple {
        let props = self.properties();
        properties::write_properties(&self.paths.properties, &props)
    }

    /// `_capture_process_timestamp(key)`: record `process.<key>` and write the properties.
    pub fn capture_process_timestamp(&mut self, key: &str, write: bool) -> SuccessTuple {
        if !matches!(key, "began" | "ended" | "paused" | "stopped") {
            return SuccessTuple::fail(format!("Invalid key '{key}'."));
        }
        let mut props = self.properties();
        let process = props
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
        self.mem = props;
        if write {
            return self.write_properties();
        }
        SuccessTuple::success()
    }

    fn logs_cf(&mut self) -> Map {
        self.properties()
            .get("logs")
            .and_then(Value::as_map)
            .cloned()
            .unwrap_or_default()
    }

    /// `log_path`: `properties['logs']['path']` or `logs/<id>.log`.
    pub fn log_path(&mut self) -> PathBuf {
        let logs = self.logs_cf();
        match logs.get("path").and_then(Value::as_str) {
            Some(p) => PathBuf::from(p),
            None => self.paths.log.clone(),
        }
    }

    /// The rotating-log settings, `properties['logs']` over the configuration.
    pub fn log_options(&mut self) -> RotatingFileOptions {
        let logs = self.logs_cf();
        let cfg = &self.ctx.config;
        RotatingFileOptions {
            write_timestamps: logs
                .get("write_timestamps")
                .filter(|v| !v.is_null())
                .map(Value::truthy)
                .unwrap_or(cfg.timestamps_enabled),
            timestamp_format: logs
                .get("timestamp_format")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| cfg.timestamp_format.clone()),
            num_files_to_keep: logs
                .get("num_files_to_keep")
                .and_then(Value::as_i64)
                .unwrap_or(cfg.num_files_to_keep),
            max_file_size: logs
                .get("max_file_size")
                .and_then(Value::as_i64)
                .unwrap_or(cfg.max_file_size),
        }
    }

    /// A fresh `RotatingFile` over the daemon's logs (`rotating_log`).
    pub fn rotating_log(&mut self) -> Option<RotatingFile> {
        let path = self.log_path();
        let opts = self.log_options();
        RotatingFile::new(path, opts).ok()
    }

    /// `log_text`: the text of every log subfile.
    pub fn log_text(&mut self) -> Option<String> {
        Some(self.rotating_log()?.read())
    }

    /// `readlines()`: the lines since the persisted cursor (`logs/.<id>.log.offset`), which is
    /// then advanced.
    pub fn readlines(&mut self) -> Vec<String> {
        let Some(mut log) = self.rotating_log() else {
            return Vec::new();
        };
        log.set_cursor(self.read_log_offset());
        let lines = log.readlines();
        let (ix, pos) = log.cursor();
        if let Some(parent) = self.paths.log_offset.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::write(&self.paths.log_offset, format!("{ix} {pos}"));
        lines
    }

    /// `_read_log_offset()`.
    pub fn read_log_offset(&self) -> (i64, u64) {
        let Ok(text) = fs::read_to_string(&self.paths.log_offset) else {
            return (0, 0);
        };
        let parts: Vec<&str> = text.split(' ').collect();
        let parsed = (
            parts
                .first()
                .and_then(|p| meerschaum_core::pyfmt::parse_py_int(p)),
            parts
                .get(1)
                .and_then(|p| meerschaum_core::pyfmt::parse_py_int(p)),
        );
        match parsed {
            (Some(a), Some(b)) if b >= 0 => (a, b as u64),
            _ => (0, 0),
        }
    }

    /// The stdin FIFO (`stdin_file`).
    pub fn stdin_file(&self) -> StdinFile {
        StdinFile::new(&self.paths.stdin, Some(self.paths.stdin_block.clone()))
            .with_refresh(self.ctx.config.cli_refresh_seconds.duration())
    }

    /// `_remove_blocking_stdin_file()`.
    pub fn remove_blocking_stdin_file(&self) -> SuccessTuple {
        match fs::remove_file(&self.paths.stdin_block) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => SuccessTuple::fail(e.to_string()),
            _ => SuccessTuple::success(),
        }
    }

    /// `pid`: the PID file's contents, or a detached process found by its command line.
    pub fn pid(&self) -> Option<i32> {
        if !self.paths.pid.exists() {
            return process::find_detached_pids(&self.daemon_id)
                .first()
                .copied();
        }
        let text = fs::read_to_string(&self.paths.pid).ok()?;
        if text.is_empty() {
            return None;
        }
        // PARITY: `int(text.rstrip())`.
        meerschaum_core::pyfmt::parse_py_int(text.trim_end()).map(|p| p as i32)
    }

    fn unlink_pid(&self) {
        let _ = fs::remove_file(&self.paths.pid);
    }

    /// `process`: the PID if the process exists (the PID file is removed otherwise).
    pub fn process(&self) -> Option<i32> {
        let pid = self.pid()?;
        if process::proc_state(pid) == ProcState::Gone {
            self.unlink_pid();
            return None;
        }
        Some(pid)
    }

    /// `status`: `running`, `paused` or `stopped`.
    pub fn status(&self) -> Status {
        let Some(pid) = self.process() else {
            return Status::Stopped;
        };
        match process::proc_state(pid) {
            ProcState::Stopped => Status::Paused,
            ProcState::Zombie | ProcState::Gone => {
                self.unlink_pid();
                Status::Stopped
            }
            ProcState::Alive => Status::Running,
        }
    }

    /// `get_timeout_seconds(timeout)`.
    pub fn get_timeout_seconds(&self, timeout: Option<Seconds>) -> Seconds {
        timeout.unwrap_or(self.ctx.config.timeout_seconds)
    }

    /// `get_check_timeout_interval_seconds(interval)`.
    pub fn get_check_timeout_interval_seconds(&self, interval: Option<Seconds>) -> Seconds {
        interval.unwrap_or(self.ctx.config.check_timeout_interval_seconds)
    }

    /// Poll `predicate` every `interval` for up to `timeout`.
    fn wait_until(
        &self,
        timeout: Seconds,
        interval: Seconds,
        mut predicate: impl FnMut(&Self) -> bool,
    ) -> bool {
        let begin = Instant::now();
        while begin.elapsed() < timeout.duration() {
            if predicate(self) {
                return true;
            }
            std::thread::sleep(interval.duration().max(Duration::from_millis(1)));
        }
        false
    }

    fn timeout_msg(&self, verb: &str, timeout: Seconds) -> String {
        format!(
            "Failed to {verb} daemon '{}' within {timeout} second{}.",
            self.daemon_id,
            timeout.plural()
        )
    }

    /// `mkdir_if_not_exists(allow_dirty_run)`.
    pub fn mkdir_if_not_exists(&self, allow_dirty_run: bool) -> SuccessTuple {
        if let Err(e) = fs::create_dir_all(&self.paths.dir) {
            return SuccessTuple::fail(e.to_string());
        }
        let already_exists = fs::read_dir(&self.paths.dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(true);
        if already_exists && !allow_dirty_run {
            return SuccessTuple::fail(format!(
                "Daemon '{id}' already exists. To allow this daemon to run, do one of the following:\n  - Execute `daemon.cleanup()`.\n  - Delete the directory '{dir}'.\n  - Pass `allow_dirty_run=True` to `daemon.run()`.\n",
                id = self.daemon_id,
                dir = self.paths.dir.display(),
            ));
        }
        SuccessTuple::success()
    }

    /// `_setup()`: record the target and write the properties before launching.
    ///
    /// PARITY (divergence): Python intends to persist
    /// `target = {name, module, args, kw}` here, but `self._properties.update({...})` binds the
    /// old dict before evaluating `self.target_kw`, whose getter replaces `self._properties`, so
    /// the target never reaches `properties.json` for jobs (`target_kw` is empty). Rust does write
    /// it: without a pickle, Python's `Daemon(daemon_id=...)` rebuilds a daemon from the target in
    /// `properties.json`, and deletes a target-less properties file.
    pub fn setup(&mut self, allow_dirty_run: bool) -> SuccessTuple {
        let mut target = Map::new();
        target.insert("name".into(), Value::Str(TARGET_NAME.into()));
        target.insert("module".into(), Value::Str(TARGET_MODULE.into()));
        target.insert(
            "args".into(),
            Value::List(vec![Value::List(
                self.target_args.iter().cloned().map(Value::Str).collect(),
            )]),
        );
        target.insert("kw".into(), Value::new_map());
        let _ = self.properties();
        self.mem.insert("target".into(), Value::Map(target));
        let made = self.mkdir_if_not_exists(allow_dirty_run);
        if !made.success {
            return made;
        }
        self.write_properties()
    }

    /// `run(keep_daemon_output, allow_dirty_run)`: start the daemon in the background.
    pub fn run(&mut self, keep_daemon_output: bool, allow_dirty_run: bool) -> SuccessTuple {
        if cfg!(windows) {
            return SuccessTuple::fail("Cannot run background jobs on Windows.");
        }
        if self.status() == Status::Paused {
            return self.resume(None, None);
        }
        let _ = properties::remove_stop_file(&self.paths.stop);
        if self.status() == Status::Running {
            return SuccessTuple::ok(format!("Daemon '{}' is already running.", self.daemon_id));
        }
        let made = self.mkdir_if_not_exists(allow_dirty_run);
        if !made.success {
            return made;
        }
        let setup = self.setup(true);
        if !setup.success {
            return setup;
        }
        match self.launch(keep_daemon_output) {
            Ok(()) => SuccessTuple::success(),
            Err(e) => {
                SuccessTuple::fail(format!("Failed to start daemon '{}'.\n{e}", self.daemon_id))
            }
        }
    }

    /// The supervisor options for this daemon.
    pub fn supervisor_options(&mut self, keep_output: bool) -> SupervisorOptions {
        let props = self.properties();
        let logs = props
            .get("logs")
            .and_then(Value::as_map)
            .cloned()
            .unwrap_or_default();
        let log_opts = self.log_options();
        let refresh = logs
            .get("refresh_files_seconds")
            .and_then(|v| match v {
                Value::Int(i) => Some(Seconds::Int(*i)),
                Value::Float(f) => Some(Seconds::Float(*f)),
                _ => None,
            })
            .unwrap_or(self.ctx.config.refresh_files_seconds);
        let cwd = props
            .get("cwd")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("/"));
        let mut command = self.ctx.launcher.program.clone();
        command.extend(self.target_args.iter().cloned());
        SupervisorOptions {
            daemon_id: self.daemon_id.clone(),
            jobs_dir: self.ctx.paths.jobs_dir.clone(),
            log_path: self.log_path(),
            error_log_path: self.ctx.paths.daemon_error_log.clone(),
            log_offset_path: self.paths.log_offset.clone(),
            keep_output,
            write_timestamps: log_opts.write_timestamps,
            timestamp_format: log_opts.timestamp_format,
            num_files_to_keep: log_opts.num_files_to_keep,
            max_file_size: log_opts.max_file_size,
            refresh_files_seconds: refresh,
            capture_stdin: logs.get("stdin").is_none_or(Value::truthy),
            cwd,
            command,
        }
    }

    /// The environment the worker runs with (on top of the launching process's environment).
    pub fn worker_env(&mut self) -> Vec<(String, String)> {
        let sc = meerschaum_core::static_config::static_config();
        let mut env = vec![
            (
                "LINES".to_string(),
                self.ctx.config.terminal_lines.to_string(),
            ),
            (
                "COLUMNS".to_string(),
                self.ctx.config.terminal_columns.to_string(),
            ),
            (sc.environment.daemon_id.clone(), self.daemon_id.clone()),
            ("PYTHONUNBUFFERED".to_string(), "1".to_string()),
        ];
        // Allow the user to override environment variables.
        if let Some(Value::Map(user_env)) = self.properties().get("env") {
            for (k, v) in user_env {
                env.push((k.clone(), v.py_str()));
            }
        }
        env
    }

    fn launch(&mut self, keep_output: bool) -> std::io::Result<()> {
        let opts = self.supervisor_options(keep_output);
        let launcher = &self.ctx.launcher;
        let Some((program, leading)) = launcher.supervisor.split_first() else {
            return Err(std::io::Error::other("No supervisor command."));
        };
        let mut cmd = std::process::Command::new(program);
        cmd.args(leading).args(opts.to_args());
        let sc = meerschaum_core::static_config::static_config();
        cmd.env_remove(&sc.environment.systemd_stdin_path);
        for (k, v) in self.worker_env() {
            cmd.env(k, v);
        }
        crate::supervisor::spawn_detached(&mut cmd)
    }

    fn sweep_stop(&self, action: &str) {
        let _ = properties::write_stop_file(&self.paths.stop, action);
        self.stdin_file().close();
        let _ = self.remove_blocking_stdin_file();
    }

    /// `_send_signal(signal, timeout)`: signal the daemon and wait until it is no longer running.
    pub fn send_signal(
        &self,
        sig: Sig,
        timeout: Option<Seconds>,
        interval: Option<Seconds>,
    ) -> SuccessTuple {
        let Some(pid) = self.pid() else {
            return SuccessTuple::fail(format!(
                "Daemon '{}' is not running, cannot send signal '{}'.",
                self.daemon_id,
                sig.py_str()
            ));
        };
        if let Err(e) = process::send_signal(pid, sig) {
            return SuccessTuple::fail(format!("Failed to send signal {}:\n{e}", sig.py_str()));
        }
        let timeout = self.get_timeout_seconds(timeout);
        let interval = self.get_check_timeout_interval_seconds(interval);
        if !timeout.is_truthy() {
            return SuccessTuple::ok(format!(
                "Successfully sent '{}' to daemon '{}'.",
                sig.py_str(),
                self.daemon_id
            ));
        }
        if self.wait_until(timeout, interval, |d| d.status() != Status::Running) {
            return SuccessTuple::success();
        }
        SuccessTuple::fail(format!(
            "Failed to stop daemon '{}' (PID: {pid}) within {timeout} second{}.",
            self.daemon_id,
            timeout.plural()
        ))
    }

    /// `_kill_detached_processes(timeout)`: SIGTERM, then SIGKILL, every process whose command
    /// line carries this daemon's marker. Returns how many were targeted.
    pub fn kill_detached_processes(&self, timeout: Option<Seconds>) -> usize {
        let pids = process::find_detached_pids(&self.daemon_id);
        if pids.is_empty() {
            return 0;
        }
        for pid in &pids {
            let _ = process::send_signal(*pid, Sig::Term);
        }
        let timeout = self.get_timeout_seconds(timeout);
        let wait = if timeout.is_truthy() {
            timeout.duration()
        } else {
            Duration::from_secs(8)
        };
        let deadline = Instant::now() + wait;
        for pid in &pids {
            let left = deadline.saturating_duration_since(Instant::now());
            if !process::wait_gone(*pid, left) {
                let _ = process::send_signal(*pid, Sig::Kill);
            }
        }
        pids.len()
    }

    fn reaped_msg(reaped: usize, otherwise: &str) -> String {
        if reaped > 0 {
            format!(
                "Reaped {reaped} detached process{}.",
                if reaped != 1 { "es" } else { "" }
            )
        } else {
            otherwise.to_string()
        }
    }

    /// `kill(timeout)`: SIGTERM (then SIGKILL) the daemon.
    pub fn kill(&mut self, timeout: Option<Seconds>) -> SuccessTuple {
        if !self.paths.pid.exists() {
            let reaped = self.kill_detached_processes(timeout);
            self.sweep_stop("kill");
            return SuccessTuple::ok(Self::reaped_msg(reaped, "Process has already stopped."));
        }
        if self.status() != Status::Paused {
            let sent = self.send_signal(Sig::Term, timeout, None);
            if sent.success {
                self.kill_detached_processes(timeout);
                self.sweep_stop("kill");
                return sent;
            }
        }
        if self.status() == Status::Stopped {
            let reaped = self.kill_detached_processes(timeout);
            self.sweep_stop("kill");
            return SuccessTuple::ok(Self::reaped_msg(reaped, "Process has already stopped."));
        }
        let Some(pid) = self.process() else {
            self.sweep_stop("kill");
            return SuccessTuple::success();
        };
        let process_repr = format!("psutil.Process(pid={pid})");
        let _ = process::send_signal(pid, Sig::Term);
        let _ = process::send_signal(pid, Sig::Kill);
        // PARITY: `process.wait(timeout=None)` waits indefinitely.
        let wait = timeout.map(Seconds::duration).unwrap_or(Duration::MAX);
        if !process::wait_gone(pid, wait) {
            return SuccessTuple::fail(format!(
                "Failed to kill job {} ({process_repr}) with exception: timeout after {} seconds",
                self.daemon_id,
                timeout.map(|t| t.to_string()).unwrap_or_default()
            ));
        }
        // A supervised worker is a zombie until its supervisor reaps it.
        let reaped = (0..50).any(|_| {
            let gone = process::proc_state(pid) == ProcState::Gone;
            if !gone {
                std::thread::sleep(Duration::from_millis(10));
            }
            gone
        });
        if !reaped {
            // PARITY: the message lacks its f-string prefix in Python.
            return SuccessTuple::fail("Failed to stop daemon '{self}' ({process}).");
        }
        self.unlink_pid();
        self.sweep_stop("kill");
        SuccessTuple::success()
    }

    /// `quit(timeout)`: SIGINT the daemon (a paused daemon is killed).
    pub fn quit(&mut self, timeout: Option<Seconds>) -> SuccessTuple {
        if self.status() == Status::Paused {
            return self.kill(timeout);
        }
        if !self.paths.pid.exists() {
            let reaped = self.kill_detached_processes(timeout);
            self.sweep_stop("quit");
            return SuccessTuple::ok(Self::reaped_msg(reaped, "Process is not running."));
        }
        let sent = self.send_signal(Sig::Int, timeout, None);
        if sent.success {
            self.sweep_stop("quit");
        }
        sent
    }

    /// `pause(timeout)`: SIGSTOP the daemon and wait until it is stopped.
    pub fn pause(&mut self, timeout: Option<Seconds>, interval: Option<Seconds>) -> SuccessTuple {
        let _ = self.remove_blocking_stdin_file();
        let Some(pid) = self.process() else {
            return SuccessTuple::fail(format!(
                "Daemon '{}' is not running and cannot be paused.",
                self.daemon_id
            ));
        };
        if self.status() == Status::Paused {
            return SuccessTuple::ok(format!("Daemon '{}' is already paused.", self.daemon_id));
        }
        self.sweep_stop("pause");
        if let Err(e) = process::send_signal(pid, Sig::Stop) {
            return SuccessTuple::fail(format!(
                "Failed to pause daemon '{}':\n{e}",
                self.daemon_id
            ));
        }
        let timeout = self.get_timeout_seconds(timeout);
        let interval = self.get_check_timeout_interval_seconds(interval);
        if !timeout.is_truthy() {
            let state = process::proc_state(pid);
            let success = matches!(state, ProcState::Stopped | ProcState::Gone);
            if success {
                self.capture_process_timestamp("paused", true);
                return SuccessTuple::success();
            }
            return SuccessTuple::fail(format!("Failed to suspend daemon '{}'.", self.daemon_id));
        }
        let begin = Instant::now();
        while begin.elapsed() < timeout.duration() {
            match process::proc_state(pid) {
                ProcState::Stopped => {
                    self.capture_process_timestamp("paused", true);
                    return SuccessTuple::success();
                }
                ProcState::Gone => {
                    return SuccessTuple::fail(format!(
                        "Process exited unexpectedly. Was it killed?\nprocess no longer exists (pid={pid})"
                    ));
                }
                _ => {}
            }
            std::thread::sleep(interval.duration());
        }
        SuccessTuple::fail(self.timeout_msg("pause", timeout))
    }

    /// `resume(timeout)`: SIGCONT a paused daemon and wait until it runs.
    pub fn resume(&mut self, timeout: Option<Seconds>, interval: Option<Seconds>) -> SuccessTuple {
        match self.status() {
            Status::Running => {
                return SuccessTuple::ok(format!(
                    "Daemon '{}' is already running.",
                    self.daemon_id
                ));
            }
            Status::Stopped => {
                return SuccessTuple::fail(format!(
                    "Daemon '{}' is stopped and cannot be resumed.",
                    self.daemon_id
                ));
            }
            Status::Paused => {}
        }
        let _ = properties::remove_stop_file(&self.paths.stop);
        let Some(pid) = self.process() else {
            return SuccessTuple::fail(format!("Cannot resume daemon '{}'.", self.daemon_id));
        };
        if let Err(e) = process::send_signal(pid, Sig::Cont) {
            return SuccessTuple::fail(format!(
                "Failed to resume daemon '{}':\n{e}",
                self.daemon_id
            ));
        }
        let timeout = self.get_timeout_seconds(timeout);
        let interval = self.get_check_timeout_interval_seconds(interval);
        if !timeout.is_truthy() {
            if self.status() == Status::Running {
                self.capture_process_timestamp("began", true);
                return SuccessTuple::success();
            }
            return SuccessTuple::fail(format!("Failed to resume daemon '{}'.", self.daemon_id));
        }
        if self.wait_until(timeout, interval, |d| d.status() == Status::Running) {
            self.capture_process_timestamp("began", true);
            return SuccessTuple::success();
        }
        SuccessTuple::fail(self.timeout_msg("resume", timeout))
    }

    /// `_read_stop_file()`.
    pub fn read_stop_file(&self) -> Option<Map> {
        properties::read_stop_file(&self.paths.stop)
    }

    /// `_write_stop_file(action)`.
    pub fn write_stop_file(&self, action: &str) -> SuccessTuple {
        properties::write_stop_file(&self.paths.stop, action)
    }

    /// `_remove_stop_file()`.
    pub fn remove_stop_file(&self) -> SuccessTuple {
        properties::remove_stop_file(&self.paths.stop)
    }

    /// `cleanup(keep_logs)`: remove the daemon's directory (and its logs and log offset).
    pub fn cleanup(&mut self, keep_logs: bool) -> SuccessTuple {
        if self.paths.dir.exists() {
            if let Err(e) = fs::remove_dir_all(&self.paths.dir) {
                return SuccessTuple::fail(format!(
                    "Failed to clean up '{}':\n{e}",
                    self.daemon_id
                ));
            }
        }
        if !keep_logs {
            if let Some(mut log) = self.rotating_log() {
                log.delete(false);
            }
            if self.paths.log_offset.exists() {
                if let Err(e) = fs::remove_file(&self.paths.log_offset) {
                    return SuccessTuple::fail(format!(
                        "Failed to remove offset file for '{}':\n{e}",
                        self.daemon_id
                    ));
                }
            }
        }
        SuccessTuple::success()
    }
}

impl std::fmt::Display for Daemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.daemon_id)
    }
}

/// `properties['target']['args'][0]` as strings, if present.
pub fn sysargs_from_properties(props: &Map) -> Option<Vec<String>> {
    let args = props.get("target")?.get("args")?.as_list()?;
    let first = args.first()?.as_list()?;
    Some(first.iter().map(|v| v.py_str()).collect())
}

/// `get_daemon_ids()`: the names under `jobs/` which have a `properties.json`, sorted.
pub fn get_daemon_ids(paths: &JobPaths) -> Vec<String> {
    let Ok(entries) = fs::read_dir(&paths.jobs_dir) else {
        return Vec::new();
    };
    let mut ids: Vec<String> = entries
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| paths.jobs_dir.join(n).join("properties.json").exists())
        .collect();
    ids.sort();
    ids
}
