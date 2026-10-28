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
use crate::rotating::RotatingFile;
use crate::startup::StartKind;

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
    /// The name is not one ordinary path component (`..`, `a/b`, ...).
    #[error("Invalid name: {0}.")]
    InvalidPathName(String),
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
    start_kind: StartKind,
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

fn mark_persistent_job(properties: &mut Map) {
    properties
        .entry("mrsm_execution".into())
        .or_insert_with(|| {
            let mut metadata = Map::new();
            metadata.insert("kind".into(), Value::Str("persistent_job".into()));
            metadata.insert("origin".into(), Value::Str("user_job".into()));
            Value::Map(metadata)
        });
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
        if let Some(reason) = crate::names::invalid_job_name_reason(name) {
            return Err(JobError::InvalidPathName(reason));
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
        // New user Jobs are persistent regardless of their name. Do not rewrite an
        // existing legacy record; automatic API workers provide their explicit kind.
        if executor_keys == "local" && daemon.is_none() {
            mark_persistent_job(&mut patch);
            // A job started from inside another job (a console run's `start pipeline NAME`) is
            // that job's owner's (see `crate::ownership`). The `systemd` executor's jobs pass
            // here too: their properties are a local job of the same name.
            crate::ownership::inherit_parent_access(
                &mut patch,
                &crate::config::DaemonPaths::new(&ctx.paths, name).dir,
                crate::ownership::current_job_dir(&ctx.paths).as_deref(),
            );
        }
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
        if let (Some(ds), Some(s)) = (&daemon_sysargs, &sysargs)
            && !s.is_empty()
            && ds != s
        {
            warnings.push("Given sysargs differ from existing sysargs.".to_string());
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
            start_kind: StartKind::default(),
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

    /// How [`Job::start`] is classified for the startup gate.
    pub fn start_kind(&self) -> StartKind {
        self.start_kind
    }

    /// Classify [`Job::start`] for the startup gate: [`StartKind::Bulk`] for a start which is one
    /// of many (jitter, load and memory wait, slot), [`StartKind::Single`] for an explicit start
    /// (slot only), [`StartKind::None`] for `--no-stagger`.
    pub fn set_start_kind(&mut self, kind: StartKind) {
        self.start_kind = kind;
    }

    /// `start()`: start (or resume) the job.
    pub fn start(&mut self) -> SuccessTuple {
        if self.executor_keys == "systemd" {
            // The unit cannot carry per-start environment: the wrapper reads this hint.
            crate::startup::write_start_hint(
                &self.ctx.paths.jobs_dir.join(crate::startup::STARTUP_DIR),
                &self.name,
                self.start_kind,
            );
        }
        if let Some(ex) = self.executor() {
            // A failed read is an error, never "the job does not exist": creating over an
            // unreachable endpoint must not look like success.
            match ex.try_get_job_exists(&self.name) {
                Err(e) => return SuccessTuple::fail(e),
                Ok(false) => {
                    let mut props = self.daemon().memory_properties();
                    mark_persistent_job(&mut props);
                    // An API server of any version runs a chain in v4's `start pipeline` form.
                    let sysargs = if self.executor_keys.starts_with("api:") {
                        meerschaum_core::chain::to_legacy(&self.sysargs)
                    } else {
                        self.sysargs.clone()
                    };
                    let created = ex.create_job(&self.name, &sysargs, Some(&props));
                    self.warnings.extend(ex.take_warnings());
                    return created;
                }
                Ok(true) => return ex.start_job(&self.name),
            }
        }
        if self.is_running() {
            return SuccessTuple::ok(format!("{self} is already running."));
        }
        let keep = !self.delete_after_completion();
        let kind = self.start_kind;
        self.daemon().set_start_kind(kind);
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
        self.try_exists().unwrap_or(false)
    }

    /// `exists()` which reports a failed remote read instead of answering `false`.
    pub fn try_exists(&mut self) -> Result<bool, String> {
        if let Some(ex) = self.executor() {
            return ex.try_get_job_exists(&self.name);
        }
        Ok(self.daemon().paths().dir.exists())
    }

    /// `get_logs()`.
    pub fn get_logs(&mut self) -> Option<String> {
        self.try_get_logs().ok().flatten()
    }

    /// `get_logs()` which reports a failed remote read.
    pub fn try_get_logs(&mut self) -> Result<Option<String>, String> {
        if let Some(ex) = self.executor() {
            return ex.try_get_logs(&self.name);
        }
        Ok(self.daemon().log_text())
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
            return ex.get_job_pid(&self.name);
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
        // An automatic restart is a bulk start (`--no-stagger` still bypasses the gate).
        let previous = self.start_kind;
        if previous != StartKind::None {
            self.start_kind = StartKind::Bulk;
        }
        let started = self.start();
        self.start_kind = previous;
        started
    }

    /// The job's health (see [`crate::health`]): from the heartbeat file of a local or `systemd`
    /// job, from the server's job metadata for an `api:` job (`unknown` when the server does not
    /// report it).
    pub fn health(&mut self) -> crate::health::JobHealth {
        let now = crate::pytime::utcnow();
        let kind = self
            .executor_keys
            .split(':')
            .next()
            .unwrap_or_default()
            .to_string();
        match kind.as_str() {
            "local" | "systemd" => {
                let status = self.status();
                let record = crate::health::read_record(&crate::health::laps_path(
                    &self.ctx.paths.jobs_dir,
                    &self.name,
                ));
                let began = if kind == "local" {
                    self.began().map(|b| match b {
                        PyDateTime::Aware(a) => a.with_timezone(&chrono::Utc),
                        PyDateTime::Naive(n) => n.and_utc(),
                    })
                } else {
                    None
                };
                crate::health::assess(record.as_ref(), status, began, now)
            }
            _ => {
                let Some(ex) = self.executor() else {
                    return crate::health::JobHealth::unknown("no executor");
                };
                let meta = ex.get_job_metadata(&self.name);
                if let Some(health) = meta
                    .get("health")
                    .and_then(crate::health::JobHealth::from_value)
                {
                    return health;
                }
                match self.status() {
                    Status::Running => {
                        crate::health::JobHealth::unknown("the executor does not report job health")
                    }
                    status => crate::health::JobHealth::stopped(status == Status::Paused),
                }
            }
        }
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
        if let Some(Value::Map(user)) = self.properties().get("env") {
            for (k, v) in user {
                env.insert(k.clone(), v.py_str());
            }
        }
        env
    }

    /// The job's properties: the daemon's for a local job, the executor's (patched with this
    /// job's own) for a `systemd` or `api:` job (`Job.daemon.properties` builds its daemon from
    /// `get_daemon_properties()` for those).
    pub fn properties(&mut self) -> Map {
        if self.executor_keys == "local" {
            return self.daemon().properties();
        }
        let mut props = self.get_daemon_properties();
        props
            .entry("label".to_string())
            .or_insert_with(|| Value::Str(crate::shlex::join(&self.sysargs)));
        props
    }

    /// Who the job belongs to (`owner`, `team`; see [`meerschaum_core::job_access`]): a job
    /// started through the API server or the console records them, a local job belongs to
    /// Everyone. A `systemd` or `api:` job reports what its executor recorded (an `api:`
    /// server records the caller, whatever this job's own properties say).
    pub fn access(&mut self) -> meerschaum_core::job_access::JobAccess {
        let props = match self.executor() {
            Some(ex) => ex.get_job_properties(&self.name),
            None => self.daemon().properties(),
        };
        meerschaum_core::job_access::JobAccess::from_properties(&self.name, &props)
    }

    /// `delete_after_completion`.
    pub fn delete_after_completion(&mut self) -> bool {
        self.properties()
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
        opts: MonitorOptions<'_>,
    ) -> SuccessTuple {
        if let Some(ex) = self.executor() {
            return ex.monitor_logs(&self.name, &mut callback, opts);
        }
        monitor_log_stream(self, callback, opts)
    }
}

impl MonitorSource for Job {
    fn rotating_log(&mut self) -> Option<RotatingFile> {
        self.daemon().rotating_log()
    }

    fn lines_to_show(&mut self) -> i64 {
        self.daemon()
            .properties()
            .get("logs")
            .and_then(|l| l.get("lines_to_show"))
            .and_then(Value::as_i64)
            .unwrap_or(self.ctx.config.lines_to_show)
    }

    fn timestamp_format(&self) -> String {
        self.ctx.config.timestamp_format.clone()
    }

    fn refresh_seconds(&self) -> Seconds {
        self.refresh_seconds
    }

    fn status(&mut self) -> Status {
        Job::status(self)
    }

    fn result(&mut self) -> SuccessTuple {
        Job::result(self)
    }

    fn is_blocking_on_stdin(&mut self) -> bool {
        Job::is_blocking_on_stdin(self)
    }

    fn write_stdin(&mut self, data: &str) -> std::io::Result<()> {
        Job::write_stdin(self, data)
    }
}

/// What [`monitor_log_stream`] needs from a job: its rotating log, status, result and stdin.
///
/// [`Job`] implements it for local jobs; the `systemd` executor implements it over the service's
/// log file and `systemctl` status (v4 passes `_rotating_log`, `_status_hook` and `_stdin_file`
/// to the hidden sister job instead).
pub trait MonitorSource {
    /// A fresh rotating log over the job's output.
    fn rotating_log(&mut self) -> Option<RotatingFile>;
    /// `logs.lines_to_show`.
    fn lines_to_show(&mut self) -> i64;
    /// The timestamp format of log lines.
    fn timestamp_format(&self) -> String;
    /// The poll interval (`system.cli.refresh_seconds`).
    fn refresh_seconds(&self) -> Seconds;
    /// The job's status.
    fn status(&mut self) -> Status;
    /// The job's result.
    fn result(&mut self) -> SuccessTuple;
    /// Whether the job waits for input.
    fn is_blocking_on_stdin(&mut self) -> bool;
    /// Send input to the job.
    fn write_stdin(&mut self, data: &str) -> std::io::Result<()>;
}

/// The first index of `lines[(-1 * lines_to_show):]`.
///
/// PARITY: Python slicing. A positive count is the last `n` lines; `0` is `lines[-0:]`, the whole
/// list (`do_action_async` relies on it to emit everything); a negative `n` is `lines[|n|:]`,
/// which skips the first `|n|` lines.
pub fn slice_start(len: usize, lines_to_show: i64) -> usize {
    match lines_to_show {
        0 => 0,
        n if n > 0 => len.saturating_sub(usize::try_from(n).unwrap_or(usize::MAX)),
        n => usize::try_from(n.unsigned_abs())
            .unwrap_or(usize::MAX)
            .min(len),
    }
}

/// `Job.monitor_logs_async` over a [`MonitorSource`].
pub fn monitor_log_stream(
    src: &mut dyn MonitorSource,
    mut callback: impl FnMut(&str) -> MonitorControl,
    mut opts: MonitorOptions<'_>,
) -> SuccessTuple {
    let sc = meerschaum_core::static_config::static_config();
    let stop_token = sc.jobs.stop_token.clone();
    let clear_token = sc.jobs.clear_token.clone();
    let flush_token = py_strip(&sc.jobs.flush_token).to_string();
    let fmt = src.timestamp_format();
    let lines_to_show = src.lines_to_show();
    let Some(mut log) = src.rotating_log() else {
        return SuccessTuple::fail("Could not open the job's logs.");
    };
    let poll = opts
        .poll_interval
        .unwrap_or_else(|| src.refresh_seconds().duration())
        .max(Duration::from_millis(10));
    let mut emitted_text = false;
    // Returns true to stop monitoring.
    let mut emit = |log: &mut crate::rotating::RotatingFile,
                    emitted_text: &mut bool,
                    opts: &mut MonitorOptions<'_>|
     -> bool {
        let lines = log.readlines();
        let start = slice_start(lines.len(), lines_to_show);
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
        if opts.stop_on_exit && src.status() == Status::Stopped {
            if !emitted_text && opts.wait_if_stopped && stopped_sleep < 0.2 {
                std::thread::sleep(Duration::from_secs_f64(stopped_sleep));
                stopped_sleep = (stopped_sleep * 1.1 * 1000.0).round() / 1000.0;
                if emit(&mut log, &mut emitted_text, &mut opts) {
                    return SuccessTuple::success();
                }
                continue;
            }
            if emitted_text || !opts.wait_if_stopped {
                let result = src.result();
                if let Some(on_stop) = opts.on_stop.as_mut() {
                    on_stop(&result);
                }
            }
            break;
        }
        if opts.accept_input && emitted_text && src.is_blocking_on_stdin() {
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
                let _ = src.write_stdin(&data);
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

/// `compress_pipeline_sysargs`: a serialized action chain (`chain <args> -P '{"sub_args_line":
/// ...}'`, or v4's `start pipeline ...`) back to `<sub args> : <args>` (see
/// [`meerschaum_core::chain`]).
pub fn compress_pipeline_sysargs(sysargs: &[String]) -> Vec<String> {
    meerschaum_core::chain::compress(sysargs)
}

/// `is_systemd_available()`: `/run/systemd/system` is a directory.
pub fn is_systemd_available() -> bool {
    std::path::Path::new("/run/systemd/system").is_dir()
}

/// The default root directory for a context (`DEFAULT_ROOT_DIR_PATH`: `<dot config>/meerschaum`),
/// derived from the systemd user directory (`<dot config>/systemd/user`).
pub fn default_root_dir(ctx: &JobsContext) -> Option<std::path::PathBuf> {
    let dot_config = ctx.paths.systemd_user_dir.parent()?.parent()?;
    Some(dot_config.join(if cfg!(windows) {
        "Meerschaum"
    } else {
        "meerschaum"
    }))
}

/// The rule of `get_executor_keys_from_context()` without its inputs: the configured executor,
/// else `systemd` when systemd is available and this is not a development install, else `local`.
///
/// Every root uses `systemd` (v4 and 5.0 before rc15: only the default root), so the jobs of a
/// Compose project with its own root come back after a reboot; each root's units have their own
/// names (see [`crate::systemd::SystemdExecutor::get_service_name`]).
pub fn choose_executor_keys(
    configured: Option<&str>,
    systemd_available: bool,
    development: bool,
) -> String {
    if let Some(configured) = configured {
        return configured.to_string();
    }
    if systemd_available && !development {
        "systemd".to_string()
    } else {
        "local".to_string()
    }
}

/// `get_executor_keys_from_context()`: the configured `meerschaum.executor`; else `systemd` on a
/// systemd host; else `local`.
///
/// PARITY: v4 also requires `is_installed('meerschaum', venv=None, allow_outside_venv=False)`
/// (`jobs/__init__.py:430`) so that a source checkout never installs units pointing at itself.
/// The counterpart here is [`is_development_install`]: a debug build, a binary under a Cargo
/// target directory, or (set by the Python extension) a package imported from a source checkout
/// keeps its jobs `local`, so the test suites never create units on a developer's or a CI host's
/// systemd. An explicit `-e systemd` (or `meerschaum.executor: systemd`) still works there.
pub fn get_executor_keys_from_context(ctx: &JobsContext) -> String {
    choose_executor_keys(
        ctx.config.executor.as_deref(),
        is_systemd_available(),
        is_development_install(),
    )
}

/// 0: not decided yet, 1: an installed copy, 2: a development install.
static DEVELOPMENT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Whether this process runs Meerschaum from a source checkout: a debug build, a binary under a
/// Cargo target directory, or a host that said so with [`set_development_install`] (the Python
/// extension, for a package imported from the repository). Such a process never chooses the
/// `systemd` executor on its own (see [`get_executor_keys_from_context`]).
pub fn is_development_install() -> bool {
    match DEVELOPMENT.load(Ordering::SeqCst) {
        1 => false,
        2 => true,
        _ => {
            let development = cfg!(debug_assertions)
                || meerschaum_core::self_exe::self_command()
                    .first()
                    .is_some_and(|program| in_cargo_target(std::path::Path::new(program)));
            DEVELOPMENT.store(if development { 2 } else { 1 }, Ordering::SeqCst);
            development
        }
    }
}

/// Declare whether this process runs from a source checkout (see [`is_development_install`]).
pub fn set_development_install(development: bool) {
    DEVELOPMENT.store(if development { 2 } else { 1 }, Ordering::SeqCst);
}

/// Whether `path` is under a Cargo target directory (an ancestor holding the `CACHEDIR.TAG` Cargo
/// writes, or a Cargo profile directory: `.fingerprint/` next to `deps/`), whatever its name or
/// profile. Cargo writes the tag only when it creates the target directory itself, so a target
/// directory that already existed (a symlink to another disk, a CI runner's cache) has none.
pub fn in_cargo_target(path: &std::path::Path) -> bool {
    let text = path.to_string_lossy();
    // A binary rebuilt while it runs reads back as `<path> (deleted)` on Linux.
    let path = std::path::Path::new(text.strip_suffix(" (deleted)").unwrap_or(&text));
    let resolved = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    resolved.ancestors().any(|a| {
        std::fs::read_to_string(a.join("CACHEDIR.TAG"))
            .is_ok_and(|tag| tag.contains("created by cargo"))
            || (a.join(".fingerprint").is_dir() && a.join("deps").is_dir())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_to_show_slices_like_python() {
        // lines[(-1 * n):] over 5 lines
        assert_eq!(slice_start(5, 2), 3);
        assert_eq!(slice_start(5, 30), 0);
        assert_eq!(slice_start(5, 0), 0);
        assert_eq!(slice_start(5, -2), 2);
        assert_eq!(slice_start(5, -9), 5);
    }

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

    #[test]
    fn executor_selection_rule() {
        assert_eq!(choose_executor_keys(Some("api:x"), true, false), "api:x");
        assert_eq!(
            choose_executor_keys(Some("systemd"), false, true),
            "systemd"
        );
        // Any root uses systemd on a systemd host...
        assert_eq!(choose_executor_keys(None, true, false), "systemd");
        assert_eq!(choose_executor_keys(None, false, false), "local");
        // ...except from a source checkout (a test run, a development binary).
        assert_eq!(choose_executor_keys(None, true, true), "local");
        let dir = tempfile::tempdir().unwrap();
        let mut ctx = JobsContext::for_root(dir.path().join("root"));
        ctx.paths.systemd_user_dir = dir.path().join("home/.config/systemd/user");
        assert_eq!(
            default_root_dir(&ctx).unwrap(),
            dir.path().join("home/.config/meerschaum")
        );
        // The tests are a debug build: never systemd by themselves, whatever the host runs.
        assert!(is_development_install());
        assert_eq!(get_executor_keys_from_context(&ctx), "local");
        ctx.paths.root = dir.path().join("home/.config/meerschaum");
        assert_eq!(get_executor_keys_from_context(&ctx), "local");
        ctx.config.executor = Some("api:other".into());
        assert_eq!(get_executor_keys_from_context(&ctx), "api:other");
        // A Cargo target directory is a source checkout, whatever the profile is called.
        let target = dir.path().join("target");
        std::fs::create_dir_all(target.join("custom/deps")).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n# This file is a cache directory tag created by cargo.\n",
        )
        .unwrap();
        assert!(in_cargo_target(&target.join("custom/deps/mrsm")));
        assert!(!in_cargo_target(&dir.path().join("bin/mrsm")));
        // A target directory that existed before Cargo ran has no tag: its profile layout counts.
        let untagged = dir.path().join("runner1-target").join("release");
        std::fs::create_dir_all(untagged.join("deps")).unwrap();
        std::fs::write(untagged.join("mrsm"), "binary").unwrap();
        assert!(!in_cargo_target(&untagged.join("mrsm")));
        std::fs::create_dir_all(untagged.join(".fingerprint")).unwrap();
        assert!(in_cargo_target(&untagged.join("mrsm")));
    }
}

#[cfg(test)]
mod rc6_kind_tests {
    use super::*;
    #[test]
    fn new_manual_execute_name_is_persistent_but_legacy_and_automatic_kinds_survive() {
        let root = tempfile::tempdir().unwrap();
        let ctx = JobsContext::for_root(root.path());
        let mut manual = Job::new(
            "execute-manual",
            Some(vec!["show".into(), "version".into()]),
            JobOptions {
                executor_keys: Some("local".into()),
                ..Default::default()
            },
            &ctx,
        )
        .unwrap();
        assert_eq!(
            manual.properties()["mrsm_execution"]
                .get("kind")
                .unwrap()
                .as_str(),
            Some("persistent_job")
        );
        let mut automatic = Job::new(
            "execute-automatic",
            Some(vec!["show".into(), "version".into()]),
            JobOptions {
                executor_keys: Some("local".into()),
                properties: Some(
                    Value::from_json_str(r#"{"mrsm_execution":{"kind":"temporary_execution"}}"#)
                        .unwrap()
                        .to_map(),
                ),
                ..Default::default()
            },
            &ctx,
        )
        .unwrap();
        assert_eq!(
            automatic.properties()["mrsm_execution"]
                .get("kind")
                .unwrap()
                .as_str(),
            Some("temporary_execution")
        );
        // A pre-rc6 on-disk record keeps the legacy prefix classification.
        let mut daemon = Daemon::new(
            "execute-legacy",
            vec!["show".into(), "version".into()],
            None,
            Map::new(),
            &ctx,
        );
        assert!(daemon.write_properties().success);
        let mut legacy = Job::new(
            "execute-legacy",
            None,
            JobOptions {
                executor_keys: Some("local".into()),
                ..Default::default()
            },
            &ctx,
        )
        .unwrap();
        assert!(!legacy.properties().contains_key("mrsm_execution"));
    }
}
