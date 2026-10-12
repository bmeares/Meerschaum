//! Port of `meerschaum.jobs.Job`: a named job managing a [`Daemon`] locally (or, for other
//! executor keys, delegating to a [`crate::executor::JobExecutor`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use meerschaum_core::pyfmt::{PyDateTime, py_strip};
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};

use crate::config::Seconds;
use crate::daemon::{Daemon, JobsContext, Status};
use crate::executor::{JobExecutor, make_executor};
use crate::pytime;

/// `BANNED_CHARS`: characters a job name may not contain.
pub const BANNED_CHARS: [char; 12] = [',', ';', '\'', '"', '$', '#', '=', '*', '&', '!', '`', '~'];

/// `RESTART_FLAGS`: sysargs which make a job restart when it stops on its own.
pub const RESTART_FLAGS: [&str; 5] = ["-s", "--restart", "--loop", "--schedule", "--cron"];

/// Errors constructing a [`Job`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JobError {
    /// The name contains a banned character.
    #[error("Invalid name: ({0}) is not allowed.")]
    InvalidName(char),
    /// The sysargs string could not be split.
    #[error("{0}")]
    Sysargs(String),
}

/// The optional arguments of `Job(...)`.
#[derive(Debug, Clone, Default)]
pub struct JobOptions {
    /// `env`: environment variables for the job's process.
    pub env: Option<IndexMap<String, String>>,
    /// `executor_keys`: `local` (default unless `meerschaum.executor` is configured), `systemd`,
    /// or `api:<label>`.
    pub executor_keys: Option<String>,
    /// `delete_after_completion`.
    pub delete_after_completion: bool,
    /// `refresh_seconds` (default `system.cli.refresh_seconds`).
    pub refresh_seconds: Option<Seconds>,
    /// `_properties`: a patch for the daemon's properties.
    pub properties: Option<Map>,
    /// `_externally_managed`.
    pub externally_managed: bool,
}

/// What a `monitor_logs` callback asks for (raising `StopMonitoringLogs` in Python).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorControl {
    /// Keep monitoring.
    Continue,
    /// Stop monitoring (`StopMonitoringLogs`).
    Stop,
}

/// `stop_callback_function`: receives the job's result.
pub type StopCallback<'a> = Box<dyn FnMut(&SuccessTuple) + 'a>;

/// The options of [`Job::monitor_logs`].
pub struct MonitorOptions<'a> {
    /// `stop_on_exit`: stop when the job stops.
    pub stop_on_exit: bool,
    /// `strip_timestamps`: remove the leading timestamps from lines.
    pub strip_timestamps: bool,
    /// `accept_input`: answer the job when it blocks on stdin.
    pub accept_input: bool,
    /// `stop_event`.
    pub stop: Option<Arc<AtomicBool>>,
    /// `input_callback_function`: returns the text to write to the job's stdin.
    pub input: Option<Box<dyn FnMut() -> Option<String> + 'a>>,
    /// `stop_callback_function`: called with the job's result when it stops.
    pub on_stop: Option<StopCallback<'a>>,
    /// Called for the clear-screen token (Python calls `clear_screen()`).
    pub on_clear: Option<Box<dyn FnMut() + 'a>>,
    /// How often to poll the log files (Python waits on `watchfiles` events).
    pub poll_interval: Option<Duration>,
    /// `_wait_if_stopped`.
    pub wait_if_stopped: bool,
}

impl Default for MonitorOptions<'_> {
    fn default() -> Self {
        MonitorOptions {
            stop_on_exit: false,
            strip_timestamps: false,
            accept_input: true,
            stop: None,
            input: None,
            on_stop: None,
            on_clear: None,
            poll_interval: None,
            wait_if_stopped: true,
        }
    }
}

/// A job: a named background process running Meerschaum sysargs.
pub struct Job {
    /// The job's name (the daemon ID).
    pub name: String,
    /// The executor keys (`local`, `systemd`, `api:<label>`).
    pub executor_keys: String,
    /// Warnings Python would have emitted while constructing the job.
    pub warnings: Vec<String>,
    ctx: JobsContext,
    sysargs: Vec<String>,
    properties_patch: Map,
    daemon: Option<Daemon>,
    refresh_seconds: Seconds,
    externally_managed: bool,
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Job {
    /// `Job(name, sysargs, ...)`.
    pub fn new(
        name: &str,
        sysargs: Option<Vec<String>>,
        opts: JobOptions,
        ctx: &JobsContext,
    ) -> Result<Job, JobError> {
        if let Some(c) = BANNED_CHARS.iter().find(|c| name.contains(**c)) {
            return Err(JobError::InvalidName(*c));
        }
        let sc = meerschaum_core::static_config::static_config();
        let and_key = &sc.system.arguments.and_key;
        let escaped_and_key = &sc.system.arguments.escaped_and_key;
        let sysargs: Option<Vec<String>> = sysargs.map(|args| {
            args.into_iter()
                .map(|a| {
                    if &a == escaped_and_key {
                        and_key.clone()
                    } else {
                        a
                    }
                })
                .collect()
        });
        let executor_keys = opts
            .executor_keys
            .clone()
            .unwrap_or_else(|| get_executor_keys_from_context(ctx));
        let daemon = if executor_keys == "local" {
            Daemon::open(name, ctx).ok()
        } else {
            None
        };

        let mut patch = opts.properties.clone().unwrap_or_default();
        if opts.externally_managed {
            patch.insert("externally_managed".into(), Value::Bool(true));
        }
        if let Some(env) = opts.env.as_ref().filter(|e| !e.is_empty()) {
            patch.insert(
                "env".into(),
                Value::Map(
                    env.iter()
                        .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                        .collect(),
                ),
            );
        }
        if opts.delete_after_completion {
            patch.insert("delete_after_completion".into(), Value::Bool(true));
        }

        let mut warnings = Vec::new();
        // PARITY: `daemon.properties['target']['args'][0]`; Python-written properties have no
        // target, so the sysargs of a Python job are only loaded lazily (see `sysargs()`).
        let daemon_sysargs: Option<Vec<String>> = daemon
            .as_ref()
            .and_then(|d| d.target_sysargs().map(<[String]>::to_vec))
            .filter(|s| !s.is_empty());
        if let (Some(ds), Some(s)) = (&daemon_sysargs, &sysargs) {
            if !s.is_empty() && ds != s {
                warnings.push("Given sysargs differ from existing sysargs.".to_string());
            }
        }
        let sysargs: Vec<String> = daemon_sysargs
            .or(sysargs.filter(|s| !s.is_empty()))
            .unwrap_or_default()
            .into_iter()
            .filter(|a| a != "-d" && a != "--daemon")
            .collect();
        if RESTART_FLAGS.iter().any(|f| sysargs.iter().any(|a| a == f)) {
            patch.insert("restart".into(), Value::Bool(true));
        }
        Ok(Job {
            name: name.to_string(),
            executor_keys,
            warnings,
            ctx: ctx.clone(),
            sysargs,
            properties_patch: patch,
            daemon,
            refresh_seconds: opts
                .refresh_seconds
                .unwrap_or(ctx.config.cli_refresh_seconds),
            externally_managed: opts.externally_managed,
        })
    }

    /// `Job(name, 'sysargs string')`: the sysargs are `shlex.split`.
    pub fn from_sysargs_str(
        name: &str,
        sysargs: &str,
        opts: JobOptions,
        ctx: &JobsContext,
    ) -> Result<Job, JobError> {
        let args = crate::shlex::split(sysargs).map_err(|e| JobError::Sysargs(e.to_string()))?;
        Job::new(name, Some(args), opts, ctx)
    }

    /// An existing local job by name.
    pub fn open(name: &str, ctx: &JobsContext) -> Result<Job, JobError> {
        Job::new(
            name,
            None,
            JobOptions {
                executor_keys: Some("local".into()),
                ..Default::default()
            },
            ctx,
        )
    }

    /// The executor for non-local keys.
    pub fn executor(&self) -> Option<Box<dyn JobExecutor>> {
        if self.executor_keys == "local" {
            return None;
        }
        Some(make_executor(&self.executor_keys, &self.ctx))
    }

    /// `daemon`: the managed daemon.
    pub fn daemon(&mut self) -> &mut Daemon {
        if self.daemon.is_none() {
            let label = crate::shlex::join(&self.sysargs);
            self.daemon = Some(Daemon::new(
                &self.name,
                self.sysargs.clone(),
                Some(label),
                self.properties_patch.clone(),
                &self.ctx,
            ));
        }
        self.daemon.as_mut().expect("daemon")
    }

    /// `start()`: start (or resume) the job.
    pub fn start(&mut self) -> SuccessTuple {
        if let Some(ex) = self.executor() {
            if !ex.get_job_exists(&self.name) {
                let props = self.daemon().properties();
                return ex.create_job(&self.name, &self.sysargs.clone(), Some(&props));
            }
            return ex.start_job(&self.name);
        }
        if self.is_running() {
            return SuccessTuple::ok(format!("{self} is already running."));
        }
        let keep = !self.delete_after_completion();
        let result = self.daemon().run(keep, true);
        if !result.success {
            return result;
        }
        SuccessTuple::ok(format!("Started {self}."))
    }

    /// `stop(timeout_seconds)`: SIGINT the job, falling back to SIGTERM / SIGKILL.
    pub fn stop(&mut self, timeout_seconds: Option<Seconds>) -> SuccessTuple {
        if let Some(ex) = self.executor() {
            return ex.stop_job(&self.name);
        }
        if self.daemon().status() == Status::Stopped {
            if !self.restarts() {
                return SuccessTuple::ok(format!("{self} is not running."));
            } else if self.stop_time().is_some() {
                return SuccessTuple::ok(format!(
                    "{self} will not restart until manually started."
                ));
            }
        }
        let quit = self.daemon().quit(timeout_seconds);
        if quit.success {
            return SuccessTuple::ok(format!("Stopped {self}."));
        }
        self.warnings.push(format!(
            "Failed to gracefully quit {self}: {}",
            quit.message
        ));
        let kill = self.daemon().kill(timeout_seconds);
        if !kill.success {
            return kill;
        }
        SuccessTuple::ok(format!("Killed {self}."))
    }

    /// `pause(timeout_seconds)`: SIGSTOP the job.
    pub fn pause(&mut self, timeout_seconds: Option<Seconds>) -> SuccessTuple {
        if let Some(ex) = self.executor() {
            return ex.pause_job(&self.name);
        }
        let result = self.daemon().pause(timeout_seconds, None);
        if !result.success {
            return result;
        }
        SuccessTuple::ok(format!("Paused {self}."))
    }

    /// Resume a paused job (Python resumes through `start()`).
    pub fn resume(&mut self, timeout_seconds: Option<Seconds>) -> SuccessTuple {
        if let Some(ex) = self.executor() {
            return ex.start_job(&self.name);
        }
        let result = self.daemon().resume(timeout_seconds, None);
        if !result.success {
            return result;
        }
        SuccessTuple::ok(format!("Resumed {self}."))
    }

    /// Stop the job if it is running or paused, then start it again.
    pub fn restart(&mut self, timeout_seconds: Option<Seconds>) -> SuccessTuple {
        if self.status() != Status::Stopped {
            let stopped = self.stop(timeout_seconds);
            if !stopped.success {
                return stopped;
            }
        }
        self.start()
    }

    /// `delete()`: stop the job if it is running, then remove its directory and logs.
    pub fn delete(&mut self) -> SuccessTuple {
        if let Some(ex) = self.executor() {
            return ex.delete_job(&self.name);
        }
        // PARITY: only a *running* job is stopped first; a paused job's process is left behind.
        if self.is_running() {
            let stopped = self.stop(None);
            if !stopped.success {
                return stopped;
            }
        }
        let cleaned = self.daemon().cleanup(false);
        if !cleaned.success {
            return cleaned;
        }
        self.daemon().pop_property("result");
        SuccessTuple::ok(format!("Deleted {self}."))
    }

    /// `is_running()`.
    pub fn is_running(&mut self) -> bool {
        self.status() == Status::Running
    }

    /// `exists()`.
    pub fn exists(&mut self) -> bool {
        if let Some(ex) = self.executor() {
            return ex.get_job_exists(&self.name);
        }
        self.daemon().paths().dir.exists()
    }

    /// `get_logs()`.
    pub fn get_logs(&mut self) -> Option<String> {
        if let Some(ex) = self.executor() {
            return ex.get_logs(&self.name);
        }
        self.daemon().log_text()
    }

    /// `status`.
    pub fn status(&mut self) -> Status {
        if let Some(ex) = self.executor() {
            return match ex.get_job_status(&self.name).as_str() {
                "running" => Status::Running,
                "paused" => Status::Paused,
                _ => Status::Stopped,
            };
        }
        self.daemon().status()
    }

    /// `pid`.
    pub fn pid(&mut self) -> Option<i32> {
        if let Some(ex) = self.executor() {
            return ex
                .get_job_metadata(&self.name)
                .get("daemon")
                .and_then(|d| d.get("pid"))
                .and_then(Value::as_i64)
                .map(|p| p as i32);
        }
        self.daemon().pid()
    }

    /// The `restart` property: whether the job restarts when it stops on its own.
    pub fn restarts(&mut self) -> bool {
        if let Some(ex) = self.executor() {
            return ex
                .get_job_metadata(&self.name)
                .get("restart")
                .is_some_and(Value::truthy);
        }
        self.daemon()
            .properties()
            .get("restart")
            .is_some_and(Value::truthy)
    }

    /// `result`: the job's `SuccessTuple`.
    pub fn result(&mut self) -> SuccessTuple {
        if self.is_running() {
            return SuccessTuple::ok(format!("{self} is running."));
        }
        if let Some(ex) = self.executor() {
            return ex
                .get_job_metadata(&self.name)
                .get("result")
                .and_then(success_tuple_from_value)
                .unwrap_or_else(|| SuccessTuple::fail("No result available."));
        }
        // PARITY: Python falls back to the in-process `_results` registry, which only holds
        // results of daemons run in the same process; for other processes it is empty.
        self.daemon()
            .properties()
            .get("result")
            .and_then(success_tuple_from_value)
            .unwrap_or_else(|| SuccessTuple::fail("No result available."))
    }

    /// `sysargs`.
    pub fn sysargs(&mut self) -> Vec<String> {
        if !self.sysargs.is_empty() {
            return self.sysargs.clone();
        }
        if let Some(ex) = self.executor() {
            return ex
                .get_job_metadata(&self.name)
                .get("sysargs")
                .and_then(Value::as_list)
                .map(|l| l.iter().map(Value::py_str).collect())
                .unwrap_or_default();
        }
        let args = self.daemon().sysargs().to_vec();
        self.sysargs = args.clone();
        args
    }

    /// `get_daemon_properties()`.
    pub fn get_daemon_properties(&self) -> Map {
        let mut props = self
            .executor()
            .map(|ex| ex.get_job_properties(&self.name))
            .unwrap_or_default();
        for (k, v) in &self.properties_patch {
            props.insert(k.clone(), v.clone());
        }
        props
    }

    fn process_timestamp(&mut self, key: &str) -> Option<PyDateTime> {
        if let Some(ex) = self.executor() {
            let s = match key {
                "began" => ex.get_job_began(&self.name),
                "ended" => ex.get_job_ended(&self.name),
                _ => ex.get_job_paused(&self.name),
            }?;
            return pytime::fromisoformat(&s).map(|d| d.to_utc().strip_tz());
        }
        let props = self.daemon().properties();
        let s = props.get("process")?.get(key)?.as_str()?.to_string();
        pytime::fromisoformat(&s)
    }

    /// `began`.
    pub fn began(&mut self) -> Option<PyDateTime> {
        self.process_timestamp("began")
    }

    /// `ended`.
    pub fn ended(&mut self) -> Option<PyDateTime> {
        self.process_timestamp("ended")
    }

    /// `paused`.
    pub fn paused(&mut self) -> Option<PyDateTime> {
        self.process_timestamp("paused")
    }

    /// `stop_time`: when the job was manually stopped (`.stop.json`).
    pub fn stop_time(&mut self) -> Option<PyDateTime> {
        if let Some(ex) = self.executor() {
            return ex.get_job_stop_time(&self.name);
        }
        let data = self.daemon().read_stop_file()?;
        let s = data.get("stop_time")?.as_str()?;
        if s.is_empty() {
            return None;
        }
        pytime::fromisoformat(s)
    }

    /// `hidden`.
    pub fn hidden(&mut self) -> bool {
        self.name.starts_with('_') || self.name.starts_with('.') || self.is_externally_managed()
    }

    /// `_is_externally_managed`.
    pub fn is_externally_managed(&mut self) -> bool {
        (self.executor_keys == "local")
            && (self.externally_managed || self.daemon().paths().externally_managed.exists())
    }

    /// `_set_externally_managed()`.
    pub fn set_externally_managed(&mut self) {
        self.externally_managed = true;
        let path = self.daemon().paths().externally_managed.clone();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path);
    }

    /// `check_restart()`: restart a stopped `--restart` / `--loop` job unless manually stopped.
    pub fn check_restart(&mut self) -> SuccessTuple {
        if self.is_running() {
            return SuccessTuple::ok(format!("{self} is running."));
        }
        if !self.restarts() {
            return SuccessTuple::ok(format!("{self} does not need to be restarted."));
        }
        if self.stop_time().is_some() {
            return SuccessTuple::ok(format!("{self} was manually stopped."));
        }
        self.start()
    }

    /// `label`: the sysargs joined, pipelines compressed, one chained command per line.
    pub fn label(&mut self) -> String {
        let sysargs = compress_pipeline_sysargs(&self.sysargs());
        py_strip(
            &crate::shlex::join(&sysargs)
                .replace(" + ", "\n+ ")
                .replace(" : ", "\n: "),
        )
        .to_string()
    }

    /// `env`: the job's environment (defaults patched with `properties['env']`).
    pub fn env(&mut self) -> IndexMap<String, String> {
        let sc = meerschaum_core::static_config::static_config();
        let mut env = IndexMap::new();
        env.insert("PYTHONUNBUFFERED".to_string(), "1".to_string());
        env.insert(
            "LINES".to_string(),
            self.ctx.config.terminal_lines.to_string(),
        );
        env.insert(
            "COLUMNS".to_string(),
            self.ctx.config.terminal_columns.to_string(),
        );
        env.insert(sc.environment.noninteractive.clone(), "true".to_string());
        if let Some(Value::Map(user)) = self.daemon().properties().get("env") {
            for (k, v) in user {
                env.insert(k.clone(), v.py_str());
            }
        }
        env
    }

    /// `delete_after_completion`.
    pub fn delete_after_completion(&mut self) -> bool {
        self.daemon()
            .properties()
            .get("delete_after_completion")
            .is_some_and(Value::truthy)
    }

    /// `is_blocking_on_stdin()`.
    pub fn is_blocking_on_stdin(&mut self) -> bool {
        if let Some(ex) = self.executor() {
            return ex.get_job_is_blocking_on_stdin(&self.name);
        }
        self.is_running() && self.daemon().paths().stdin_block.exists()
    }

    /// `get_prompt_kwargs()`.
    pub fn get_prompt_kwargs(&mut self) -> Map {
        if let Some(ex) = self.executor() {
            return ex.get_job_prompt_kwargs(&self.name);
        }
        let path = self.daemon().paths().prompt_kwargs.clone();
        std::fs::read_to_string(path)
            .ok()
            .and_then(|t| Value::from_json_str(&t).ok())
            .map(|v| v.to_map())
            .unwrap_or_default()
    }

    /// `write_stdin(data)`.
    pub fn write_stdin(&mut self, data: &str) -> std::io::Result<()> {
        self.daemon().stdin_file().write(data.as_bytes())
    }

    /// `monitor_logs(callback, ...)`: emit the latest `lines_to_show` lines, then new lines as
    /// they are written, until the callback stops, the stop flag is set, or (with `stop_on_exit`)
    /// the job stops.
    pub fn monitor_logs(
        &mut self,
        mut callback: impl FnMut(&str) -> MonitorControl,
        mut opts: MonitorOptions<'_>,
    ) -> SuccessTuple {
        if let Some(ex) = self.executor() {
            return ex.monitor_logs(&self.name, &mut callback, opts);
        }
        let sc = meerschaum_core::static_config::static_config();
        let stop_token = sc.jobs.stop_token.clone();
        let clear_token = sc.jobs.clear_token.clone();
        let flush_token = py_strip(&sc.jobs.flush_token).to_string();
        let fmt = self.ctx.config.timestamp_format.clone();
        let lines_to_show = self
            .daemon()
            .properties()
            .get("logs")
            .and_then(|l| l.get("lines_to_show"))
            .and_then(Value::as_i64)
            .unwrap_or(self.ctx.config.lines_to_show)
            .max(0) as usize;
        let Some(mut log) = self.daemon().rotating_log() else {
            return SuccessTuple::fail("Could not open the job's logs.");
        };
        let poll = opts
            .poll_interval
            .unwrap_or_else(|| self.refresh_seconds.duration())
            .max(Duration::from_millis(10));

        let mut emitted_text = false;
        // Returns true to stop monitoring.
        let mut emit = |log: &mut crate::rotating::RotatingFile,
                        emitted_text: &mut bool,
                        opts: &mut MonitorOptions<'_>|
         -> bool {
            let lines = log.readlines();
            let start = lines.len().saturating_sub(lines_to_show);
            for line in &lines[start..] {
                if opts
                    .stop
                    .as_ref()
                    .is_some_and(|s| s.load(Ordering::Relaxed))
                {
                    return true;
                }
                let stripped_extra = pytime::strip_timestamp_from_line(py_strip(line), &fmt);
                let mut out = line.clone();
                if stripped_extra == stop_token {
                    return true;
                }
                if stripped_extra == clear_token {
                    if let Some(on_clear) = opts.on_clear.as_mut() {
                        on_clear();
                    }
                    continue;
                }
                if stripped_extra == flush_token {
                    out = String::new();
                }
                if opts.strip_timestamps && !out.is_empty() {
                    out = pytime::strip_timestamp_from_line(&out, &fmt);
                }
                *emitted_text = true;
                if callback(&out) == MonitorControl::Stop {
                    return true;
                }
            }
            false
        };

        if emit(&mut log, &mut emitted_text, &mut opts) {
            return SuccessTuple::success();
        }
        let signature = |log: &crate::rotating::RotatingFile| {
            let path = log.get_latest_subfile_path();
            let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            (path, len)
        };
        let mut last_sig = signature(&log);
        let mut stopped_sleep = 0.1_f64;
        loop {
            if opts
                .stop
                .as_ref()
                .is_some_and(|s| s.load(Ordering::Relaxed))
            {
                break;
            }
            if opts.stop_on_exit && self.status() == Status::Stopped {
                if !emitted_text && opts.wait_if_stopped && stopped_sleep < 0.2 {
                    std::thread::sleep(Duration::from_secs_f64(stopped_sleep));
                    stopped_sleep = (stopped_sleep * 1.1 * 1000.0).round() / 1000.0;
                    if emit(&mut log, &mut emitted_text, &mut opts) {
                        return SuccessTuple::success();
                    }
                    continue;
                }
                if emitted_text || !opts.wait_if_stopped {
                    let result = self.result();
                    if let Some(on_stop) = opts.on_stop.as_mut() {
                        on_stop(&result);
                    }
                }
                break;
            }
            if opts.accept_input && emitted_text && self.is_blocking_on_stdin() {
                if emit(&mut log, &mut emitted_text, &mut opts) {
                    return SuccessTuple::success();
                }
                let data = match opts.input.as_mut() {
                    Some(input) => input(),
                    None => {
                        let mut line = String::new();
                        std::io::stdin().read_line(&mut line).ok().map(|_| line)
                    }
                };
                if let Some(data) = data {
                    let _ = self.write_stdin(&data);
                }
            }
            let sig = signature(&log);
            if sig != last_sig {
                last_sig = sig;
                if emit(&mut log, &mut emitted_text, &mut opts) {
                    return SuccessTuple::success();
                }
            }
            let begin = Instant::now();
            while begin.elapsed() < poll {
                std::thread::sleep(Duration::from_millis(10).min(poll));
            }
        }
        emit(&mut log, &mut emitted_text, &mut opts);
        SuccessTuple::success()
    }
}

impl std::fmt::Display for Job {
    /// `Job("name", "sysargs")`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // PARITY: `self.sysargs` lazily loads the daemon's sysargs (from the pickle in Python),
        // which are gone once the job's directory is deleted.
        let sysargs = match &self.daemon {
            Some(d) if self.sysargs.is_empty() && d.paths().dir.exists() => d.sysargs().to_vec(),
            _ => self.sysargs.clone(),
        };
        write!(f, "Job(\"{}\"", self.name)?;
        if !sysargs.is_empty() {
            write!(f, ", \"{}\"", crate::shlex::join(&sysargs))?;
        }
        write!(f, ")")
    }
}

/// `[success, message]` as a [`SuccessTuple`] (`tuple(result)`).
pub fn success_tuple_from_value(value: &Value) -> Option<SuccessTuple> {
    let list = value.as_list()?;
    if list.len() < 2 {
        return None;
    }
    Some(SuccessTuple {
        success: list[0].truthy(),
        message: list[1].py_str(),
    })
}

/// `compress_pipeline_sysargs`: `start pipeline <args> -P '{"sub_args_line": ...}'` back to
/// `<sub args> : <args>`.
pub fn compress_pipeline_sysargs(sysargs: &[String]) -> Vec<String> {
    if sysargs.len() < 2 || sysargs[0] != "start" || sysargs[1] != "pipeline" {
        return sysargs.to_vec();
    }
    let Some(params_ix) = sysargs.iter().position(|a| a == "-P") else {
        return sysargs.to_vec();
    };
    let pipeline_args = &sysargs[2..params_ix];
    let Some(params_str) = sysargs.last() else {
        return sysargs.to_vec();
    };
    let Ok(Value::Map(params)) = Value::from_json_str(params_str) else {
        return sysargs.to_vec();
    };
    let Some(line) = params
        .get("sub_args_line")
        .filter(|v| v.truthy())
        .map(Value::py_str)
    else {
        return sysargs.to_vec();
    };
    let Ok(mut out) = crate::shlex::split(&line) else {
        return sysargs.to_vec();
    };
    out.push(":".into());
    out.extend(pipeline_args.iter().cloned());
    out
}

/// `get_executor_keys_from_context()`: the configured `meerschaum.executor`, else `local`.
///
/// Python picks `systemd` on hosts with systemd and the default root; the Rust systemd executor
/// is a stub in this wave, so `local` is the default.
pub fn get_executor_keys_from_context(ctx: &JobsContext) -> String {
    ctx.config
        .executor
        .clone()
        .unwrap_or_else(|| "local".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banned_names_and_label() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = JobsContext::for_root(dir.path());
        let err = Job::new("a;b", None, JobOptions::default(), &ctx).unwrap_err();
        assert_eq!(err.to_string(), "Invalid name: (;) is not allowed.");
        let mut job = Job::from_sysargs_str(
            "x",
            "sync pipes -d : --loop + show version",
            JobOptions::default(),
            &ctx,
        )
        .unwrap();
        assert_eq!(job.label(), "sync pipes\n: --loop\n+ show version");
        assert!(job.restarts());
        assert_eq!(
            job.to_string(),
            "Job(\"x\", \"sync pipes : --loop + show version\")"
        );
        assert_eq!(job.status(), Status::Stopped);
        assert_eq!(job.result().message, "No result available.");
    }
}
