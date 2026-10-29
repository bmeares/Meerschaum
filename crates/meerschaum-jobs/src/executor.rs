//! Port of `meerschaum.jobs.Executor` (the job-executor interface) and of the module-level job
//! queries in `meerschaum/jobs/__init__.py`.
//!
//! - [`LocalExecutor`] (`local`) is implemented over [`Job`] / [`crate::daemon::Daemon`].
//! - [`crate::systemd::SystemdExecutor`] (`systemd`) renders the same unit files as Python and
//!   drives `systemctl --user` through the context's [`crate::runner::ProcessRunner`].
//! - [`ApiExecutor`] (`api:<label>`) manages jobs on a remote API through the client the
//!   context's [`crate::remote::RemoteJobsResolver`] provides (see [`crate::remote`]).
//! - Any other `<type>:<label>` is resolved by the context's [`CustomExecutors`] (the host
//!   installs one over the executor types of native plugins); unresolved keys fall through to
//!   [`ApiExecutor`], whose resolver rejects them.

use indexmap::IndexMap;
use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};

use crate::daemon::{JobsContext, Status, get_daemon_ids};
use crate::job::{Job, JobOptions, MonitorControl, MonitorOptions, success_tuple_from_value};
pub use crate::remote::ApiExecutor;

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

    /// Warnings the executor produced while operating (drained): the library never prints, the
    /// caller shows them (`Job::start` moves them into `Job::warnings`).
    fn take_warnings(&self) -> Vec<String> {
        Vec::new()
    }

    /// [`JobExecutor::get_job_names`], with the failure of a remote read as an error. Decisions
    /// (listings, ownership, replacement) use this variant so a transport failure is never read
    /// as "no jobs".
    fn try_get_job_names(&self) -> Result<Vec<String>, String> {
        Ok(self.get_job_names())
    }
    /// [`JobExecutor::get_job_exists`] which reports a failed read.
    fn try_get_job_exists(&self, name: &str) -> Result<bool, String> {
        Ok(self.get_job_exists(name))
    }
    /// [`JobExecutor::get_job_metadata`] which reports a failed read and a missing job (`None`).
    fn try_get_job_metadata(&self, name: &str) -> Result<Option<Map>, String> {
        let meta = self.get_job_metadata(name);
        Ok((!meta.is_empty()).then_some(meta))
    }
    /// Every job's metadata in one read, for executors which can (the API): `None` means "list
    /// the names and read each job".
    fn try_get_jobs(&self) -> Result<Option<IndexMap<String, Map>>, String> {
        Ok(None)
    }
    /// [`JobExecutor::get_logs`] which reports a failed read.
    fn try_get_logs(&self, name: &str) -> Result<Option<String>, String> {
        Ok(self.get_logs(name))
    }
    /// `get_job_pid(name)`.
    fn get_job_pid(&self, name: &str) -> Option<i32> {
        self.get_job_metadata(name)
            .get("daemon")
            .and_then(|d| d.get("pid"))
            .and_then(Value::as_i64)
            .map(|p| p as i32)
    }
    /// `get_job_result(name)`.
    fn get_job_result(&self, name: &str) -> Option<SuccessTuple> {
        self.get_job_metadata(name)
            .get("result")
            .and_then(success_tuple_from_value)
    }
}

/// Resolves executor keys of types the jobs crate does not implement (`<type>:<label>`
/// executors of native plugins), installed with [`JobsContext::with_executors`].
pub trait CustomExecutors: Send + Sync + std::fmt::Debug {
    /// The executor for `executor_keys`; `None` when the type is not one of these executors,
    /// `Some(Err(..))` when it is but cannot be built.
    fn resolve(&self, executor_keys: &str) -> Option<Result<Box<dyn JobExecutor>, String>>;
}

/// An executor that cannot be built: every operation reports why.
#[derive(Debug, Clone)]
pub struct UnavailableExecutor {
    keys: String,
    error: String,
}

impl UnavailableExecutor {
    /// An executor for `keys` failing with `error`.
    pub fn new(keys: &str, error: impl Into<String>) -> UnavailableExecutor {
        UnavailableExecutor {
            keys: keys.into(),
            error: error.into(),
        }
    }
}

impl JobExecutor for UnavailableExecutor {
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
    fn create_job(&self, _: &str, _: &[String], _: Option<&Map>) -> SuccessTuple {
        SuccessTuple::fail(self.error.clone())
    }
    fn start_job(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::fail(self.error.clone())
    }
    fn stop_job(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::fail(self.error.clone())
    }
    fn pause_job(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::fail(self.error.clone())
    }
    fn delete_job(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::fail(self.error.clone())
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
        SuccessTuple::fail(self.error.clone())
    }
    fn get_job_is_blocking_on_stdin(&self, _name: &str) -> bool {
        false
    }
    fn get_job_prompt_kwargs(&self, _name: &str) -> Map {
        Map::new()
    }
    fn try_get_job_names(&self) -> Result<Vec<String>, String> {
        Err(self.error.clone())
    }
    fn try_get_job_exists(&self, _name: &str) -> Result<bool, String> {
        Err(self.error.clone())
    }
    fn try_get_job_metadata(&self, _name: &str) -> Result<Option<Map>, String> {
        Err(self.error.clone())
    }
    fn try_get_logs(&self, _name: &str) -> Result<Option<String>, String> {
        Err(self.error.clone())
    }
}

/// Build the executor for `executor_keys`.
pub fn make_executor(executor_keys: &str, ctx: &JobsContext) -> Box<dyn JobExecutor> {
    let typ = executor_keys.split(':').next().unwrap_or("");
    match typ {
        "systemd" => {
            let executor = crate::systemd::SystemdExecutor::new(ctx.clone());
            Box::new(match &ctx.process_env {
                Some(env) => executor.with_process_env(env.clone()),
                None => executor,
            })
        }
        "local" | "None" | "" => Box::new(LocalExecutor::new(ctx.clone())),
        "api" => Box::new(ApiExecutor::new(executor_keys, ctx.remote.clone())),
        _ => match ctx
            .executors
            .as_ref()
            .and_then(|r| r.resolve(executor_keys))
        {
            Some(Ok(executor)) => executor,
            Some(Err(error)) => Box::new(UnavailableExecutor::new(executor_keys, error)),
            None => Box::new(ApiExecutor::new(executor_keys, ctx.remote.clone())),
        },
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
    filter_jobs(get_jobs(ctx, true), filter_list, include_hidden)
}

/// The filtering half of `get_filtered_jobs` over an existing listing.
pub fn filter_jobs(
    mut jobs: IndexMap<String, Job>,
    filter_list: &[String],
    include_hidden: bool,
) -> (IndexMap<String, Job>, Vec<String>) {
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
        // Nothing was removed from `jobs` (every lookup failed): it is still the full listing.
        let exclude: Vec<String> = with_underscores
            .iter()
            .map(|n| n.trim_start_matches('_').to_string())
            .collect();
        return (
            jobs.into_iter()
                .filter(|(n, _)| !exclude.contains(n))
                .collect(),
            warnings,
        );
    }
    (out, warnings)
}

/// A job listing: the jobs and the warnings Python would have emitted while listing them.
#[derive(Debug)]
pub struct JobListing {
    /// The jobs by name.
    pub jobs: IndexMap<String, Job>,
    /// Warnings (`Job 'x' exists in both local and systemd.`).
    pub warnings: Vec<String>,
}

/// Normalize `-e` / `executor_keys=` values: `None`, `""` and `"None"` mean "from the context".
pub fn normalize_executor_keys(keys: Option<&str>) -> Option<String> {
    keys.filter(|k| !k.is_empty() && !k.eq_ignore_ascii_case("none"))
        .map(str::to_string)
}

/// A [`Job`] for a remote or `systemd` job whose metadata is known (v4 builds
/// `Job(name, sysargs, executor_keys=..., _properties=daemon.properties)` from a listing).
pub fn job_from_metadata(
    name: &str,
    executor_keys: &str,
    meta: &Map,
    ctx: &JobsContext,
) -> Result<Job, String> {
    let sysargs: Vec<String> = meta
        .get("sysargs")
        .and_then(Value::as_list)
        .map(|l| l.iter().map(Value::py_str).collect())
        .unwrap_or_default();
    let properties = meta
        .get("daemon")
        .and_then(|d| d.get("properties"))
        .map(Value::to_map);
    Job::new(
        name,
        Some(sysargs),
        JobOptions {
            executor_keys: Some(executor_keys.to_string()),
            properties,
            ..Default::default()
        },
        ctx,
    )
    .map_err(|e| e.to_string())
}

/// The jobs of a non-local executor (`Executor.get_jobs()`), hidden ones filtered unless asked
/// for. A failed read is an error (an unreachable or unauthorized endpoint is not "no jobs").
pub fn get_executor_jobs(
    ctx: &JobsContext,
    executor_keys: &str,
    include_hidden: bool,
) -> Result<IndexMap<String, Job>, String> {
    let executor = make_executor(executor_keys, ctx);
    let mut jobs: IndexMap<String, Job> = IndexMap::new();
    match executor.try_get_jobs()? {
        Some(all) => {
            for (name, meta) in all {
                jobs.insert(
                    name.clone(),
                    job_from_metadata(&name, executor_keys, &meta, ctx)?,
                );
            }
        }
        None => {
            let mut names = executor.try_get_job_names()?;
            names.sort();
            for name in names {
                let job = Job::new(
                    &name,
                    None,
                    JobOptions {
                        executor_keys: Some(executor_keys.to_string()),
                        ..Default::default()
                    },
                    ctx,
                )
                .map_err(|e| e.to_string())?;
                jobs.insert(name, job);
            }
        }
    }
    jobs.retain(|_, job| include_hidden || !job.hidden());
    Ok(jobs)
}

/// `get_jobs(executor_keys, include_hidden, combine_local_and_systemd)`.
///
/// `executor_keys` of `None` is the context's executor. On a `systemd` context, `local` and
/// `systemd` listings are combined (warning about names present in both; `systemd` wins).
pub fn get_jobs_for(
    ctx: &JobsContext,
    executor_keys: Option<&str>,
    include_hidden: bool,
    combine_local_and_systemd: bool,
) -> Result<JobListing, String> {
    let context_keys = crate::job::get_executor_keys_from_context(ctx);
    let keys = normalize_executor_keys(executor_keys).unwrap_or_else(|| context_keys.clone());
    let kind = keys.split(':').next().unwrap_or("");
    let combine = combine_local_and_systemd
        && matches!(kind, "local" | "systemd" | "None" | "")
        && context_keys == "systemd";
    let local = |hidden: bool| get_jobs(ctx, hidden);
    let mut warnings = Vec::new();
    if combine {
        let mut jobs = local(include_hidden);
        let systemd = get_executor_jobs(ctx, "systemd", include_hidden)?;
        let shared: Vec<String> = systemd
            .keys()
            .filter(|n| jobs.contains_key(*n))
            .cloned()
            .collect();
        if !shared.is_empty() {
            warnings.push(format!(
                "Job{} {} exist{} in both `local` and `systemd`.",
                if shared.len() != 1 { "s" } else { "" },
                items_str(&shared),
                if shared.len() == 1 { "s" } else { "" },
            ));
        }
        jobs.extend(systemd);
        return Ok(JobListing { jobs, warnings });
    }
    let jobs = match keys.as_str() {
        "local" | "None" | "" => local(include_hidden),
        other => get_executor_jobs(ctx, other, include_hidden)?,
    };
    Ok(JobListing { jobs, warnings })
}

/// The result of [`get_filtered_jobs_for`].
#[derive(Debug)]
pub struct FilteredJobs {
    /// The selected jobs.
    pub jobs: IndexMap<String, Job>,
    /// Warnings from listing (always emitted by Python).
    pub listing_warnings: Vec<String>,
    /// `Job 'x' does not exist.` for each requested name that is missing (emitted when the
    /// caller asks for warnings).
    pub missing: Vec<String>,
}

/// `get_filtered_jobs(executor_keys, filter_list, include_hidden, combine_local_and_systemd)`.
pub fn get_filtered_jobs_for(
    ctx: &JobsContext,
    executor_keys: Option<&str>,
    filter_list: &[String],
    include_hidden: bool,
    combine_local_and_systemd: bool,
) -> Result<FilteredJobs, String> {
    let listing = get_jobs_for(ctx, executor_keys, true, combine_local_and_systemd)?;
    let (jobs, missing) = filter_jobs(listing.jobs, filter_list, include_hidden);
    Ok(FilteredJobs {
        jobs,
        listing_warnings: listing.warnings,
        missing,
    })
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
        let owned: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        items_str(&owned)
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

/// `items_str(items)`: `'a'`, `'a' and 'b'`, `'a', 'b', and 'c'`.
pub fn items_str(items: &[String]) -> String {
    let quoted: Vec<String> = items.iter().map(|n| format!("'{n}'")).collect();
    match quoted.len() {
        0 => String::new(),
        1 => quoted[0].clone(),
        2 => format!("{} and {}", quoted[0], quoted[1]),
        n => format!("{}, and {}", quoted[..n - 1].join(", "), quoted[n - 1]),
    }
}
