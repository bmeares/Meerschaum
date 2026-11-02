//! Every job of a root with its state, read in one pass (the console's Jobs page, `GET /jobs`).
//!
//! Reading a `systemd` job's status, PID, result, health and times one getter at a time asks
//! `systemctl --user` a few times per getter, which cost seconds per listing on a host with a
//! few dozen jobs. [`get_job_summaries`] lists the same jobs as
//! [`crate::executor::get_jobs_for`] (`local`, combined with `systemd` on a `systemd` context)
//! over [`crate::systemd::snapshot_context`]: one batched `systemctl show` answers every unit's
//! state, and each value is what the job's own getter returns (the same code reads it).

use indexmap::IndexMap;
use meerschaum_core::pyfmt::PyDateTime;
use meerschaum_core::success::SuccessTuple;
use meerschaum_core::value::{Map, Value};
use serde_json::{Value as Json, json};

use crate::daemon::{JobsContext, Status};
use crate::executor::get_jobs_for;
use crate::health::JobHealth;
use crate::job::Job;

/// A job as one listing saw it.
#[derive(Debug, Clone, PartialEq)]
pub struct JobSummary {
    /// The job's name.
    pub name: String,
    /// `local` or `systemd`.
    pub executor_keys: String,
    /// `job.sysargs`.
    pub sysargs: Vec<String>,
    /// `job.label` (one chained command per line).
    pub label: String,
    /// `job.status`.
    pub status: Status,
    /// `job.pid`.
    pub pid: Option<i32>,
    /// `job.result` (a running job's is "`<job>` is running.").
    pub result: SuccessTuple,
    /// `job.restart`.
    pub restart: bool,
    /// `job.health`.
    pub health: JobHealth,
    /// `job.began`.
    pub began: Option<PyDateTime>,
    /// `job.ended`.
    pub ended: Option<PyDateTime>,
    /// `job.paused`.
    pub paused: Option<PyDateTime>,
    /// `job.stop_time`: when it was stopped by hand.
    pub stop_time: Option<PyDateTime>,
    /// The daemon's properties (owner, team, execution kind, ...): `job.properties()`.
    pub properties: Map,
}

fn iso(dt: &Option<PyDateTime>) -> Json {
    dt.as_ref()
        .map(|d| Json::String(d.isoformat()))
        .unwrap_or(Json::Null)
}

fn to_json(value: &Value) -> Json {
    serde_json::to_value(value).unwrap_or(Json::Null)
}

impl JobSummary {
    /// Read a job's summary with its getters.
    pub fn read(job: &mut Job) -> JobSummary {
        let status = job.status();
        JobSummary {
            name: job.name.clone(),
            executor_keys: job.executor_keys.clone(),
            sysargs: job.sysargs(),
            label: job.label(),
            status,
            pid: job.pid(),
            result: job.result(),
            restart: job.restarts(),
            health: job.health(),
            began: job.began(),
            ended: job.ended(),
            paused: job.paused(),
            stop_time: job.stop_time(),
            properties: job.properties(),
        }
    }

    /// As JSON: `{name, executor_keys, sysargs, label, status, pid, result, restart, health,
    /// began, ended, paused, stop_time, properties}` (times in ISO-8601, as the getters' Python
    /// counterparts parse them).
    pub fn to_json(&self) -> Json {
        json!({
            "name": self.name,
            "executor_keys": self.executor_keys,
            "sysargs": self.sysargs,
            "label": self.label,
            "status": self.status.as_str(),
            "pid": self.pid,
            "result": [self.result.success, self.result.message],
            "restart": self.restart,
            "health": self.health.to_json(),
            "began": iso(&self.began),
            "ended": iso(&self.ended),
            "paused": iso(&self.paused),
            "stop_time": iso(&self.stop_time),
            "properties": to_json(&Value::Map(self.properties.clone())),
        })
    }

    /// As a core map (for the Python façade).
    pub fn to_map(&self) -> Map {
        Value::from_json_str(&self.to_json().to_string())
            .map(|v| v.to_map())
            .unwrap_or_default()
    }
}

/// The jobs [`get_jobs_for`] lists for `local` (combined with `systemd` on a `systemd` context),
/// each with its state, from one batched `systemctl show` for the `systemd` jobs (see the module
/// documentation). In the listing's order.
pub fn get_job_summaries(
    ctx: &JobsContext,
    include_hidden: bool,
) -> Result<IndexMap<String, JobSummary>, String> {
    let ctx = listing_context(ctx, Some("local"), true);
    let listing = get_jobs_for(&ctx, Some("local"), include_hidden, true)?;
    Ok(listing
        .jobs
        .into_iter()
        .map(|(name, mut job)| (name, JobSummary::read(&mut job)))
        .collect())
}

/// The context for one listing of `executor_keys` (see [`get_jobs_for`]) which then reads each
/// job's state: [`crate::systemd::snapshot_context`] when the listing includes `systemd` jobs,
/// else `ctx` itself.
pub fn listing_context(
    ctx: &JobsContext,
    executor_keys: Option<&str>,
    combine_local_and_systemd: bool,
) -> JobsContext {
    let context_keys = crate::job::get_executor_keys_from_context(ctx);
    let keys = crate::executor::normalize_executor_keys(executor_keys)
        .unwrap_or_else(|| context_keys.clone());
    let lists_systemd = keys == "systemd"
        || (combine_local_and_systemd
            && matches!(keys.as_str(), "local" | "None" | "")
            && context_keys == "systemd");
    if lists_systemd {
        crate::systemd::snapshot_context(ctx)
    } else {
        ctx.clone()
    }
}
