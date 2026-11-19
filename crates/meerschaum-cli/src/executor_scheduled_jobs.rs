//! Main advances each saved graph's schedule before issuing a fresh original-source launch.
//! The schedule and its claimed fire survive a Main restart and are shared by all API workers.
use super::{DelegatingJobs, JobPlacement, Record};
use chrono::{DateTime, FixedOffset, TimeDelta, Utc};
use meerschaum_core::SuccessTuple;
use meerschaum_jobs::{JitterSpec, parse_schedule_jittered, parse_schedule_resumed};
use meerschaum_server::backends::NoJobs;
use meerschaum_server::executor::pipe_operation::{Cadence, GraphOperation};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

type Fire = DateTime<FixedOffset>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobSchedule {
    next: Option<Fire>,
    last: Option<Fire>,
    /// A crash between claiming and acknowledging cannot replay the previous fire.
    claiming: bool,
    paused: bool,
    error: Option<String>,
    jitter: f64,
}

fn cadence(record: &Record) -> Result<Option<Cadence>, String> {
    match &record.operation {
        Some(GraphOperation::Stored(job)) => {
            job.intent.cadence().map_err(|error| error.detail_text())
        }
        _ => Ok(None),
    }
}

fn next_loop(now: Fire, seconds: f64) -> Result<Fire, String> {
    let interval = std::time::Duration::try_from_secs_f64(seconds)
        .ok()
        .and_then(|value| TimeDelta::from_std(value).ok())
        .ok_or("Invalid saved job interval.")?;
    now.checked_add_signed(interval)
        .ok_or_else(|| "The next job fire is outside the calendar.".into())
}

impl JobSchedule {
    pub(crate) fn new(record: &Record, now: Fire, jitter: f64) -> Result<Option<Self>, String> {
        let Some(cadence) = cadence(record)? else {
            return Ok(None);
        };
        let mut state = Self {
            next: None,
            last: None,
            claiming: false,
            paused: false,
            error: None,
            jitter,
        };
        state.arm(&record.name, &cadence, now)?;
        Ok(Some(state))
    }

    pub(crate) fn claimed(&self) -> bool {
        self.claiming
    }

    fn arm(&mut self, name: &str, cadence: &Cadence, now: Fire) -> Result<(), String> {
        self.next = match cadence {
            Cadence::Schedule { expression, jitter } => {
                let spec = JitterSpec {
                    default_seconds: jitter.unwrap_or(self.jitter),
                    seed: name,
                };
                Some(
                    parse_schedule_jittered(expression, now, Some(spec))
                        .map_err(|e| e.py_message())?
                        .next()
                        .ok_or("The schedule has no next firing time.")?,
                )
            }
            Cadence::Loop { min_seconds } => {
                next_loop(now, *min_seconds)?;
                Some(now)
            }
            Cadence::Restart => Some(now),
        };
        self.error = None;
        self.paused = false;
        Ok(())
    }

    fn claim(&mut self, name: &str, cadence: &Cadence, now: Fire) -> Result<bool, String> {
        let Some(due) = self.next.filter(|f| *f <= now) else {
            return Ok(false);
        };
        if self.claiming {
            return Ok(false);
        }
        self.next = match cadence {
            Cadence::Schedule { expression, jitter } => {
                let spec = JitterSpec {
                    default_seconds: jitter.unwrap_or(self.jitter),
                    seed: name,
                };
                parse_schedule_resumed(expression, due, Some(spec))
                    .map_err(|e| e.py_message())?
                    .next_after(now)
            }
            Cadence::Loop { min_seconds } => Some(next_loop(now, *min_seconds)?),
            Cadence::Restart => Some(now + TimeDelta::seconds(1)),
        };
        self.last = Some(due);
        self.claiming = true;
        Ok(true)
    }

    pub(super) fn metadata(&self, body: &mut serde_json::Value) {
        body["restart"] = true.into();
        body["schedule"] = serde_json::json!({"next_fire": self.next, "last_fire": self.last, "firing": self.claiming, "error": self.error});
        if self.next.is_some() || self.claiming {
            body["status"] = "running".into();
            body["daemon"]["status"] = "running".into();
        } else if self.last.is_none() || self.paused || self.error.is_some() {
            let status = if self.paused { "paused" } else { "stopped" };
            body["status"] = status.into();
            body["daemon"]["status"] = status.into();
        }
        if let Some(error) = &self.error {
            body["result"] = serde_json::json!([false, error]);
        }
    }
}

impl DelegatingJobs {
    /// The public starter enables future firings. It never supplies a worker credential.
    pub(super) async fn schedule_control(
        &self,
        record: &Record,
        operation: &str,
    ) -> Option<SuccessTuple> {
        let schedule = record.schedule.as_ref()?;
        let cadence = match cadence(record) {
            Ok(Some(cadence)) => cadence,
            _ => {
                return Some(SuccessTuple::fail(
                    "This saved schedule has no registered command.",
                ));
            }
        };
        let mut updated = schedule.clone();
        match operation {
            "start" => {
                if let Err(error) = self.controller.confirm_stored_start(record).await {
                    return Some(SuccessTuple::fail(error));
                }
                if let Err(error) = self.controller.confirm_schedule_source(record).await {
                    return Some(SuccessTuple::fail(error));
                }
                if updated.claiming {
                    return Some(SuccessTuple::fail(
                        "The last scheduled launch was interrupted. Stop the job before starting it again.",
                    ));
                }
                if updated.next.is_some() {
                    return Some(SuccessTuple::ok("The job schedule is active."));
                }
                if let Err(error) = updated.arm(&record.name, &cadence, Utc::now().fixed_offset()) {
                    return Some(SuccessTuple::fail(error));
                }
            }
            "stop" | "pause" | "delete" => {
                updated.claiming = false;
                updated.next = None;
                updated.paused = operation == "pause";
                updated.error = None;
            }
            _ => return None,
        }
        if let Err(error) = self
            .catalog
            .update(record, |current| current.schedule = Some(updated))
        {
            return Some(SuccessTuple::fail(error));
        }
        if operation == "start" {
            return Some(SuccessTuple::ok("Started the job schedule."));
        }
        if record.context_revision == 1 {
            if operation == "delete" {
                self.controller.suspend_job_context(record, true).await;
                return Some(match self.catalog.remove(record) {
                    Ok(true) => SuccessTuple::success(),
                    _ => SuccessTuple::fail("Could not remove the job schedule."),
                });
            }
            return Some(SuccessTuple::success());
        }
        None
    }

    pub(crate) async fn run_schedules(
        controller: Arc<crate::executor_client::Controller>,
        root: std::path::PathBuf,
    ) {
        let jobs = Arc::new(Self::new(&root, Arc::new(NoJobs), controller));
        let catalog_file = jobs.catalog.directory.join("catalog.json");
        let mut ticks = tokio::time::interval(std::time::Duration::from_secs(1));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut previous_stamp = None;
        let mut next_fire: Option<Fire> = None;
        let mut last_scan = None;
        loop {
            ticks.tick().await;
            let now = Utc::now().fixed_offset();
            let stamp = tokio::fs::metadata(&catalog_file)
                .await
                .ok()
                .and_then(|m| Some((m.modified().ok()?, m.len())));
            // Idle Main workers normally need only a stat. A periodic full scan also
            // notices replacements on filesystems with coarse modification times.
            if stamp == previous_stamp
                && next_fire.is_none_or(|due| due > now)
                && last_scan.is_some_and(|scan: std::time::Instant| scan.elapsed().as_secs() < 15)
            {
                continue;
            }
            let catalog = jobs.catalog.clone();
            let Ok(Ok(records)) = tokio::task::spawn_blocking(move || catalog.records()).await
            else {
                continue;
            };
            previous_stamp = stamp;
            last_scan = Some(std::time::Instant::now());
            next_fire = records
                .values()
                .filter_map(|r| r.schedule.as_ref())
                .filter(|s| !s.claiming)
                .filter_map(|s| s.next)
                .min();
            let mut pending = tokio::task::JoinSet::new();
            for record in records.into_values().filter(|r| {
                r.schedule
                    .as_ref()
                    .is_some_and(|s| !s.claiming && s.next.is_some_and(|f| f <= now))
            }) {
                if pending.len() >= 8 {
                    let _ = pending.join_next().await;
                }
                let jobs = jobs.clone();
                pending.spawn(async move {
                    if let Err(error) = jobs.fire_schedule(&record).await {
                        tracing::debug!(%error, "Scheduled pipe job could not start");
                    }
                });
            }
            while pending.join_next().await.is_some() {}
        }
    }

    async fn fire_schedule(&self, observed: &Record) -> Result<(), String> {
        let _lock = self.controller.job_lock(observed).await?;
        let Some(mut record) = self
            .catalog
            .get(&observed.name)?
            .filter(|r| r.remote_name == observed.remote_name && r.schedule == observed.schedule)
        else {
            return Ok(());
        };
        if self.preflight_start(&record).await? {
            return Ok(());
        }
        let cadence = cadence(&record)?.ok_or("The scheduled pipe command is missing.")?;
        let mut schedule = record
            .schedule
            .clone()
            .ok_or("The saved cadence is missing.")?;
        if !schedule.claim(&record.name, &cadence, Utc::now().fixed_offset())? {
            return Ok(());
        }
        self.catalog
            .update(&record, |current| current.schedule = Some(schedule.clone()))?;
        record.schedule = Some(schedule.clone());
        let result: Result<(), String> = async {
            self.controller
                .deliver_scheduled_context(&mut record)
                .await?;
            let result = if record.placement == JobPlacement::HostApi {
                self.controller.confirm_schedule_source(&record).await?;
                self.mutate_host(&record, "start").await
            } else {
                let api = self.controller.prepare_id(record.team_id).await?;
                self.controller.confirm_schedule_source(&record).await?;
                self.controller.start_job(&mut record, &api).await
            };
            if result.success {
                Ok(())
            } else {
                Err(result.message)
            }
        }
        .await;
        schedule.claiming = false;
        if let Err(error) = &result {
            schedule.next = None;
            schedule.error = Some(super::text(error, 4096));
            self.controller.suspend_job_context(&record, false).await;
        }
        self.catalog.update(&record, |current| {
            current.schedule = Some(schedule);
            if result.is_ok() {
                current.completed_at = None;
            }
        })?;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_due_fire_runs_once_and_preserves_phase_across_downtime() {
        let origin = DateTime::parse_from_rfc3339("2026-11-17T10:00:00Z").unwrap();
        let cadence = Cadence::Schedule {
            expression: "every 7 minutes".into(),
            jitter: None,
        };
        let mut state = JobSchedule {
            next: Some(origin),
            last: None,
            claiming: false,
            paused: false,
            error: None,
            jitter: 0.0,
        };
        let recovered_at = origin + TimeDelta::minutes(19);
        assert!(state.claim("saved", &cadence, recovered_at).unwrap());
        assert_eq!(state.last, Some(origin));
        assert_eq!(state.next, Some(origin + TimeDelta::minutes(21)));
        let mut recovered: JobSchedule =
            serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        assert!(
            !recovered
                .claim("saved", &cadence, recovered_at + TimeDelta::minutes(14))
                .unwrap()
        );
        recovered.claiming = false;
        assert!(!recovered.claim("saved", &cadence, recovered_at).unwrap());
        assert!(
            recovered
                .claim("saved", &cadence, origin + TimeDelta::minutes(21))
                .unwrap()
        );
        assert_eq!(recovered.next, Some(origin + TimeDelta::minutes(28)));
    }
}
