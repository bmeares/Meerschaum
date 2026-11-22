//! The `--schedule` loop (`utils/schedule.py`): running an action on its schedule, with jitter,
//! until it finishes or a stop is requested.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use meerschaum_core::SuccessTuple;
use meerschaum_jobs::config::JobsConfig;
use meerschaum_jobs::scheduler::RootScheduler;

use super::stop_flag::{install_stop_handler, sleep_unless_stopped, stop_requested_flag};
use crate::dispatch::Runtime;

/// `schedule_function(do_action, schedule)`: run `action` on the schedule until it finishes or
/// a stop is requested. Returns `Err` with the schedule error message.
pub fn run_schedule(
    rt: &mut Runtime<'_>,
    schedule: &str,
    action: impl FnMut(&mut Runtime<'_>) -> SuccessTuple,
) -> SuccessTuple {
    run_schedule_jittered(rt, schedule, None, "run", action)
}

/// [`run_schedule`] with the schedule's jitter (v5, D23): every fire time moves by a FIXED offset
/// in `[0, jitter]` seconds, seeded by the job's name (the schedule text outside a job), so a
/// job keeps its offset across fires and restarts and jobs with one cadence spread out. The
/// bound is the schedule's own `jitter <n> <unit>` suffix, else `jitter` (`--jitter`), else
/// `jobs.schedule.jitter_seconds` (default 0: no jitter). Each run is a lap of the job's health
/// (`noun`: what a run does, `sync` or `run`).
pub fn run_schedule_jittered(
    rt: &mut Runtime<'_>,
    schedule: &str,
    jitter: Option<f64>,
    noun: &str,
    mut action: impl FnMut(&mut Runtime<'_>) -> SuccessTuple,
) -> SuccessTuple {
    let now = chrono::Utc::now().fixed_offset();
    // A job process (`MRSM_DAEMON_ID` is the job name) keeps its schedule per root: a restart
    // resumes the recorded next fire time instead of re-anchoring the cadence. Foreground runs
    // have no job name and keep nothing.
    let job = (rt.env)(&meerschaum_core::static_config().environment.daemon_id)
        .filter(|name| !name.is_empty());
    let default_jitter =
        jitter.unwrap_or_else(|| JobsConfig::from_config(rt.config()).schedule_jitter_seconds);
    let spec = meerschaum_jobs::JitterSpec {
        default_seconds: default_jitter,
        seed: job.as_deref().unwrap_or(schedule),
    };
    // The one parse of this run that warns (an unknown zone abbreviation after `starting`): the
    // warning reaches the job's log once, not once per helper that re-parses the schedule.
    let mut trigger = match meerschaum_jobs::parse_schedule_warned(schedule, now, Some(spec)) {
        Ok((t, warnings)) => {
            for message in warnings {
                super::warn(rt, &message);
            }
            t
        }
        Err(e) => return SuccessTuple::fail(e.py_message()),
    };
    install_stop_handler(&super::session(rt));
    let stop = stop_requested_flag();
    let start = Instant::now();
    let scheduler = job
        .as_ref()
        .map(|_| RootScheduler::for_root(&rt.config().paths().root_dir));
    let mut pending = match (&scheduler, &job) {
        (Some(scheduler), Some(job)) => scheduler
            .next_fire_jittered(job, schedule, now, default_jitter)
            .ok()
            .flatten()
            .filter(|next| {
                scheduler
                    .state(job)
                    .is_some_and(|s| s.next_fire == Some(*next))
            }),
        _ => None,
    };
    if let Some(resumed) = pending {
        // Keep the cadence of the resumed fire: an interval (`daily`) drawn from this process's
        // start would put the fire after it one restart-to-fire gap early.
        match meerschaum_jobs::parse_schedule_resumed(schedule, resumed, Some(spec)) {
            Ok(t) => trigger = t,
            Err(e) => return SuccessTuple::fail(e.py_message()),
        }
    }
    let mut finished = false;
    // Job health: each scheduled run is a lap, expected every period of the schedule (the gap
    // to the next fire, measured as the schedule runs; see `meerschaum_jobs::health`).
    let mut heartbeat =
        super::jobs_laps::heartbeat(meerschaum_jobs::schedule::period_seconds(schedule), noun);
    while !stop.load(Ordering::SeqCst) {
        let Some(next) = pending.take().or_else(|| trigger.next()) else {
            break;
        };
        let now = chrono::Utc::now().with_timezone(next.offset());
        if next <= now {
            pending = trigger.next_after(now);
            finished = pending.is_none();
        }
        let wait = (next - now).num_microseconds().unwrap_or(0).max(0) as f64 / 1e6;
        if heartbeat.record().interval_seconds.is_none_or(|i| i < wait) {
            heartbeat.set_interval(Some(wait));
            heartbeat.flush();
        }
        if wait >= 1.0 {
            // Idle until the first fire: a job's startup slot (D23) is not needed for that.
            meerschaum_jobs::startup::signal_first_lap_done();
        }
        heartbeat.waiting(Duration::try_from_secs_f64(wait).unwrap_or_default());
        if sleep_unless_stopped(wait) {
            break;
        }
        heartbeat.lap_started();
        let result = action(rt);
        heartbeat.lap_finished(result.success, &result.message);
        meerschaum_jobs::startup::signal_first_lap_done();
        if !finished && pending.is_none() {
            // The fire after the one just served. A resumed fire (a restart between two fires) is
            // `trigger`'s first occurrence: its `next()` would return it again, already due, and
            // run the job twice.
            pending = trigger.next_after(next);
            finished = pending.is_none();
        }
        if let (Some(scheduler), Some(job)) = (&scheduler, &job) {
            // Recorded with the offset of the fire it served (the schedule's zone, if any).
            let fired = chrono::Utc::now().with_timezone(next.offset());
            let _ = scheduler.record_fire(job, schedule, fired, pending);
        }
        if finished {
            break;
        }
    }
    SuccessTuple::ok(format!(
        "Exited scheduler after {}.",
        super::sync::interval_str(start.elapsed().as_secs_f64())
    ))
}
