//! Port of `meerschaum.jobs.Executor` (the job-executor interface) and of the module-level job
//! queries in `meerschaum/jobs/__init__.py`.
//!
//! - [`LocalExecutor`] (`local`) is implemented over [`Job`] / [`crate::daemon::Daemon`].
//! - [`crate::systemd::SystemdExecutor`] (`systemd`) renders the same unit files as Python and
//!   reads its job directories; operations which need `systemctl` are not implemented yet.
//! - [`ApiExecutor`] (`api:<label>`) is a stub: every operation reports not-implemented.

use indexmap::IndexMap;
use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};

use crate::daemon::{JobsContext, Status, get_daemon_ids};
use crate::job::{Job, JobOptions, MonitorControl, MonitorOptions};

/// The `Executor` interface: manage jobs by name.
pub trait JobExecutor {
    /// The executor keys (`local`, `systemd`, `api:main`).
    fn executor_keys(&self) -> String;
    /// `get_job_names()`: every job, including hidden ones.
    fn get_job_names(&self) -> Vec<String>;
    /// `get_job_exists(name)`.
    fn get_job_exists(&self, name: &str) -> bool;
    /// `get_job_metadata(name)`: `{sysargs, result, restart, daemon: {status, pid, properties}}`.
    fn get_job_metadata(&self, name: &str) -> Map;
    /// `get_job_properties(name)`.
    fn get_job_properties(&self, name: &str) -> Map;
    /// `get_job_status(name)`: `running`, `paused` or `stopped`.
    fn get_job_status(&self, name: &str) -> String;
    /// `get_job_began(name)`.
    fn get_job_began(&self, name: &str) -> Option<String>;
    /// `get_job_ended(name)`.
    fn get_job_ended(&self, name: &str) -> Option<String>;
    /// `get_job_paused(name)`.
    fn get_job_paused(&self, name: &str) -> Option<String>;
    /// `create_job(name, sysargs, properties)`.
    fn create_job(&self, name: &str, sysargs: &[String], properties: Option<&Map>) -> SuccessTuple;
    /// `start_job(name)`.
    fn start_job(&self, name: &str) -> SuccessTuple;
    /// `stop_job(name)`.
    fn stop_job(&self, name: &str) -> SuccessTuple;
    /// `pause_job(name)`.
    fn pause_job(&self, name: &str) -> SuccessTuple;
    /// `delete_job(name)`.
    fn delete_job(&self, name: &str) -> SuccessTuple;
    /// `get_logs(name)`.
    fn get_logs(&self, name: &str) -> Option<String>;
    /// `get_job_stop_time(name)`.
    fn get_job_stop_time(&self, name: &str) -> Option<PyDateTime>;
    /// `monitor_logs(name, callback, ...)`.
    fn monitor_logs(
        &self,
        name: &str,
        callback: &mut dyn FnMut(&str) -> MonitorControl,
        opts: MonitorOptions<'_>,
    ) -> SuccessTuple;
    /// `get_job_is_blocking_on_stdin(name)`.
    fn get_job_is_blocking_on_stdin(&self, name: &str) -> bool;
    /// `get_job_prompt_kwargs(name)`.
    fn get_job_prompt_kwargs(&self, name: &str) -> Map;
}

/// The message every unimplemented executor operation returns.
pub fn not_implemented(executor_keys: &str, operation: &str) -> SuccessTuple {
    SuccessTuple::fail(format!(
        "The '{executor_keys}' executor does not implement `{operation}` yet."
    ))
}

/// Build the executor for `executor_keys`.
pub fn make_executor(executor_keys: &str, ctx: &JobsContext) -> Box<dyn JobExecutor> {
    let typ = executor_keys.split(':').next().unwrap_or("");
    match typ {
        "systemd" => Box::new(crate::systemd::SystemdExecutor::new(ctx.clone())),
        "local" | "None" | "" => Box::new(LocalExecutor::new(ctx.clone())),
        _ => Box::new(ApiExecutor::new(executor_keys)),
    }
}

/// The `local` executor: daemons under `MRSM_ROOT_DIR/jobs`.
#[derive(Debug, Clone)]
pub struct LocalExecutor {
    ctx: JobsContext,
}

impl LocalExecutor {
    /// A local executor over a context.
    pub fn new(ctx: JobsContext) -> LocalExecutor {
        LocalExecutor { ctx }
    }

    fn job(&self, name: &str) -> Option<Job> {
        Job::open(name, &self.ctx).ok()
    }
}

impl JobExecutor for LocalExecutor {
    fn executor_keys(&self) -> String {
        "local".into()
    }

    fn get_job_names(&self) -> Vec<String> {
        get_daemon_ids(&self.ctx.paths)
    }

    fn get_job_exists(&self, name: &str) -> bool {
        self.ctx.paths.daemon_dir(name).exists()
    }

    fn get_job_metadata(&self, name: &str) -> Map {
        let Some(mut job) = self.job(name) else {
            return Map::new();
        };
        let mut daemon = Map::new();
        daemon.insert("status".into(), Value::Str(job.status().as_str().into()));
        daemon.insert(
            "pid".into(),
            job.pid()
                .map(|p| Value::Int(p as i64))
                .unwrap_or(Value::Null),
        );
        daemon.insert("properties".into(), Value::Map(job.daemon().properties()));
        let result = job.result();
        let mut meta = Map::new();
        meta.insert(
            "sysargs".into(),
            Value::List(job.sysargs().into_iter().map(Value::Str).collect()),
        );
        meta.insert(
            "result".into(),
            Value::List(vec![
                Value::Bool(result.success),
                Value::Str(result.message),
            ]),
        );
        meta.insert("restart".into(), Value::Bool(job.restarts()));
        meta.insert("daemon".into(), Value::Map(daemon));
        meta
    }

    fn get_job_properties(&self, name: &str) -> Map {
        self.job(name)
            .map(|mut j| j.daemon().properties())
            .unwrap_or_default()
    }

    fn get_job_status(&self, name: &str) -> String {
        self.job(name)
            .map(|mut j| j.status().as_str().to_string())
            .unwrap_or_else(|| "stopped".into())
    }

    fn get_job_began(&self, name: &str) -> Option<String> {
        self.job(name)?.began().map(|d| d.isoformat())
    }

    fn get_job_ended(&self, name: &str) -> Option<String> {
        self.job(name)?.ended().map(|d| d.isoformat())
    }

    fn get_job_paused(&self, name: &str) -> Option<String> {
        self.job(name)?.paused().map(|d| d.isoformat())
    }

    fn create_job(&self, name: &str, sysargs: &[String], properties: Option<&Map>) -> SuccessTuple {
        let opts = JobOptions {
            executor_keys: Some("local".into()),
            properties: properties.cloned(),
            ..Default::default()
        };
        match Job::new(name, Some(sysargs.to_vec()), opts, &self.ctx) {
            Ok(mut job) => job.start(),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    fn start_job(&self, name: &str) -> SuccessTuple {
        match self.job(name) {
            Some(mut job) => job.start(),
            None => SuccessTuple::fail(format!("Job '{name}' does not exist.")),
        }
    }

    fn stop_job(&self, name: &str) -> SuccessTuple {
        match self.job(name) {
            Some(mut job) => job.stop(None),
            None => SuccessTuple::fail(format!("Job '{name}' does not exist.")),
        }
    }

    fn pause_job(&self, name: &str) -> SuccessTuple {
        match self.job(name) {
            Some(mut job) => job.pause(None),
            None => SuccessTuple::fail(format!("Job '{name}' does not exist.")),
        }
    }

    fn delete_job(&self, name: &str) -> SuccessTuple {
        match self.job(name) {
            Some(mut job) => job.delete(),
            None => SuccessTuple::fail(format!("Job '{name}' does not exist.")),
        }
    }

    fn get_logs(&self, name: &str) -> Option<String> {
        self.job(name)?.get_logs()
    }

    fn get_job_stop_time(&self, name: &str) -> Option<PyDateTime> {
        self.job(name)?.stop_time()
    }

    fn monitor_logs(
        &self,
        name: &str,
        callback: &mut dyn FnMut(&str) -> MonitorControl,
        opts: MonitorOptions<'_>,
    ) -> SuccessTuple {
        match self.job(name) {
            Some(mut job) => job.monitor_logs(callback, opts),
            None => SuccessTuple::fail(format!("Job '{name}' does not exist.")),
        }
    }

    fn get_job_is_blocking_on_stdin(&self, name: &str) -> bool {
        self.job(name).is_some_and(|mut j| j.is_blocking_on_stdin())
    }

    fn get_job_prompt_kwargs(&self, name: &str) -> Map {
        self.job(name)
            .map(|mut j| j.get_prompt_kwargs())
            .unwrap_or_default()
    }
}

/// The `api:<label>` executor (a stub: the HTTP client wiring lands with `meerschaum-api`).
#[derive(Debug, Clone)]
pub struct ApiExecutor {
    keys: String,
}

impl ApiExecutor {
    /// A stub for `api:<label>`.
    pub fn new(keys: &str) -> ApiExecutor {
        ApiExecutor { keys: keys.into() }
    }
}

impl JobExecutor for ApiExecutor {
    fn executor_keys(&self) -> String {
        self.keys.clone()
    }
    fn get_job_names(&self) -> Vec<String> {
        Vec::new()
    }
    fn get_job_exists(&self, _name: &str) -> bool {
        false
    }
    fn get_job_metadata(&self, _name: &str) -> Map {
        Map::new()
    }
    fn get_job_properties(&self, _name: &str) -> Map {
        Map::new()
    }
    fn get_job_status(&self, _name: &str) -> String {
        "stopped".into()
    }
    fn get_job_began(&self, _name: &str) -> Option<String> {
        None
    }
    fn get_job_ended(&self, _name: &str) -> Option<String> {
        None
    }
    fn get_job_paused(&self, _name: &str) -> Option<String> {
        None
    }
    fn create_job(&self, _name: &str, _sysargs: &[String], _p: Option<&Map>) -> SuccessTuple {
        not_implemented(&self.keys, "create_job")
    }
    fn start_job(&self, _name: &str) -> SuccessTuple {
        not_implemented(&self.keys, "start_job")
    }
    fn stop_job(&self, _name: &str) -> SuccessTuple {
        not_implemented(&self.keys, "stop_job")
    }
    fn pause_job(&self, _name: &str) -> SuccessTuple {
        not_implemented(&self.keys, "pause_job")
    }
    fn delete_job(&self, _name: &str) -> SuccessTuple {
        not_implemented(&self.keys, "delete_job")
    }
    fn get_logs(&self, _name: &str) -> Option<String> {
        None
    }
    fn get_job_stop_time(&self, _name: &str) -> Option<PyDateTime> {
        None
    }
    fn monitor_logs(
        &self,
        _name: &str,
        _callback: &mut dyn FnMut(&str) -> MonitorControl,
        _opts: MonitorOptions<'_>,
    ) -> SuccessTuple {
        not_implemented(&self.keys, "monitor_logs")
    }
    fn get_job_is_blocking_on_stdin(&self, _name: &str) -> bool {
        false
    }
    fn get_job_prompt_kwargs(&self, _name: &str) -> Map {
        Map::new()
    }
}

/// `get_jobs(executor_keys, include_hidden)` for the local executor: stopped jobs (by `ended`),
/// then paused (by `paused`), then running (by `began`), like `get_daemons()`; externally
/// managed jobs are excluded.
pub fn get_jobs(ctx: &JobsContext, include_hidden: bool) -> IndexMap<String, Job> {
    let mut entries: Vec<(Status, String, Job)> = Vec::new();
    for name in get_daemon_ids(&ctx.paths) {
        let Ok(mut job) = Job::open(&name, ctx) else {
            continue;
        };
        let status = job.status();
        let key = match status {
            Status::Stopped => "ended",
            Status::Paused => "paused",
            Status::Running => "began",
        };
        let props = job.daemon().properties();
        // PARITY: missing timestamps sort last (`'9999'`).
        let ts = props
            .get("process")
            .and_then(|p| p.get(key))
            .map(Value::py_str)
            .unwrap_or_else(|| "9999".to_string());
        entries.push((status, ts, job));
    }
    let rank = |s: Status| match s {
        Status::Stopped => 0,
        Status::Paused => 1,
        Status::Running => 2,
    };
    entries.sort_by(|a, b| rank(a.0).cmp(&rank(b.0)).then_with(|| a.1.cmp(&b.1)));
    let mut out = IndexMap::new();
    for (_, _, mut job) in entries {
        if (include_hidden || !job.hidden()) && !job.is_externally_managed() {
            out.insert(job.name.clone(), job);
        }
    }
    out
}

/// `get_filtered_jobs(filter_list)`: the named jobs (a list of only `_`-prefixed names excludes
/// those names instead).
pub fn get_filtered_jobs(
    ctx: &JobsContext,
    filter_list: &[String],
    include_hidden: bool,
) -> (IndexMap<String, Job>, Vec<String>) {
    let mut jobs = get_jobs(ctx, true);
    let mut warnings = Vec::new();
    if filter_list.is_empty() {
        jobs.retain(|_, j| include_hidden || !j.hidden());
        return (jobs, warnings);
    }
    let with_underscores: Vec<&String> =
        filter_list.iter().filter(|n| n.starts_with('_')).collect();
    let mut out = IndexMap::new();
    for name in filter_list {
        match jobs.shift_remove(name) {
            Some(job) => {
                out.insert(name.clone(), job);
            }
            None => warnings.push(format!("Job '{name}' does not exist.")),
        }
    }
    if out.is_empty() && !with_underscores.is_empty() {
        let exclude: Vec<String> = with_underscores
            .iter()
            .map(|n| n.trim_start_matches('_').to_string())
            .collect();
        let all = get_jobs(ctx, true);
        return (
            all.into_iter()
                .filter(|(n, _)| !exclude.contains(n))
                .collect(),
            warnings,
        );
    }
    (out, warnings)
}

/// `get_running_jobs` / `get_paused_jobs` / `get_stopped_jobs`.
pub fn get_jobs_with_status(
    ctx: &JobsContext,
    status: Status,
    include_hidden: bool,
) -> IndexMap<String, Job> {
    get_jobs(ctx, include_hidden)
        .into_iter()
        .filter_map(|(n, mut j)| (j.status() == status).then_some((n, j)))
        .collect()
}

/// `get_restart_jobs`: jobs created with a restart flag.
pub fn get_restart_jobs(ctx: &JobsContext, include_hidden: bool) -> IndexMap<String, Job> {
    get_jobs(ctx, include_hidden)
        .into_iter()
        .filter_map(|(n, mut j)| j.restarts().then_some((n, j)))
        .collect()
}

/// `check_restart_jobs(jobs)`: restart stopped restart-jobs which were not manually stopped.
pub fn check_restart_jobs(jobs: &mut IndexMap<String, Job>) -> SuccessTuple {
    if jobs.is_empty() {
        return SuccessTuple::ok("No jobs to restart.");
    }
    let mut success_names = Vec::new();
    let mut fail_names = Vec::new();
    for (name, job) in jobs.iter_mut() {
        if job.check_restart().success {
            success_names.push(name.as_str());
        } else {
            fail_names.push(name.as_str());
        }
    }
    let items = |names: &[&str]| {
        let quoted: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
        match quoted.len() {
            0 => String::new(),
            1 => quoted[0].clone(),
            2 => format!("{} and {}", quoted[0], quoted[1]),
            n => format!("{}, and {}", quoted[..n - 1].join(", "), quoted[n - 1]),
        }
    };
    let plural = if success_names.len() != 1 { "s" } else { "" };
    if fail_names.is_empty() {
        SuccessTuple::ok(format!(
            "Successfully restarted job{plural} {}.",
            items(&success_names)
        ))
    } else {
        // PARITY: the plural in the failure message follows the number of *successes*.
        SuccessTuple::fail(format!(
            "Failed to restart job{plural} {}.",
            items(&fail_names)
        ))
    }
}
