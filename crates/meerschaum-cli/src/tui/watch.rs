//! The `watch` action (v5): monitors, as opposed to `show`'s one-shot output.
//!
//! - `watch run [RUN_ID]`: the run monitor for a run (the newest run when no ID is given; see
//!   [`newest_run`]). Off a terminal (or
//!   with `--nopretty`) it follows the run's persisted events line by line (JSON lines with
//!   `--nopretty`) until the run ends, or until it has been idle for `--timeout` seconds
//!   (default [`IDLE_LIMIT`]): a run whose process died is never marked finished.
//! - `watch jobs [NAME ...]`: the jobs monitor. Off a terminal it prints `show jobs`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_core::SuccessTuple;
use meerschaum_engine::{EngineReader, Event, EventKind, RunFilter, RunStatus};

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::native::{Session, flag, float, session, string, words};
use crate::value::ArgsMap;

use super::jobs_view::{JobsMonitor, JobsSource};
use super::run_view::{RunMonitor, Source, StoredPoller, event_line, event_of_row};
use super::{available, engine_reader, show};

/// How often line output polls the instance for new events.
const FOLLOW_EVERY: Duration = Duration::from_millis(500);

/// How long line output follows a running run with no new events before it gives up (the run's
/// process may have died: its row is never marked finished). `--timeout` overrides it; `0`
/// waits for good.
pub const IDLE_LIMIT: Duration = Duration::from_secs(600);

/// The run `watch run` opens without a RUN_ID: the newest run by start time, finished or not.
///
/// An unfinished run does not take precedence: a run whose process was killed stays "running"
/// for good, and would otherwise shadow every later run. `show runs` lists the runs still
/// going; `watch run RUN_ID` opens one of them.
pub fn newest_run(session: &Session, reader: &EngineReader) -> Result<Option<String>, String> {
    let runs = session
        .block_on(reader.runs(&RunFilter {
            limit: Some(1),
            ..Default::default()
        }))
        .map_err(|e| e.to_string())?;
    Ok(runs.first().map(|r| r.run.run_id.clone()))
}

/// `watch runs` / `watch run [RUN_ID]`.
pub fn watch_runs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = crate::native::get_instance(args, rt)?;
    let session = session(rt);
    let reader = engine_reader(rt, &session, &inst)?;
    let run_id = match words(args, "action").into_iter().find(|w| !w.is_empty()) {
        Some(id) => id,
        None => match newest_run(&session, &reader) {
            Ok(Some(id)) => id,
            Ok(None) => {
                return Ok(SuccessTuple::fail(
                    "No runs have been recorded on this instance.",
                ));
            }
            Err(error) => {
                return Ok(SuccessTuple::fail(format!(
                    "Cannot read the run history: {error}"
                )));
            }
        },
    };
    match session.block_on(reader.run_detail(&run_id)) {
        Ok(Some(_)) => {}
        Ok(None) => return Ok(SuccessTuple::fail(format!("No run '{run_id}' was found."))),
        Err(error) => {
            return Ok(SuccessTuple::fail(format!(
                "Cannot read run {run_id}: {error}"
            )));
        }
    }
    if available(rt, args) {
        let poller = StoredPoller::start(&session, Arc::new(reader), &run_id, false);
        let mut view = RunMonitor::new(&run_id, &inst.keys(), Source::Stored(poller));
        show(rt, &mut view)?;
        return Ok(SuccessTuple::success());
    }
    let idle_limit = match float(args, "timeout_seconds") {
        Some(seconds) if seconds <= 0.0 => None,
        Some(seconds) => Some(Duration::from_secs_f64(seconds)),
        None => Some(IDLE_LIMIT),
    };
    follow_run(rt, &reader, &run_id, flag(args, "nopretty"), idle_limit)
}

/// Print a run's events as they are persisted until it ends. Returns the run's outcome, or a
/// failure once the run has had no new events for `idle_limit`.
fn follow_run(
    rt: &mut Runtime<'_>,
    reader: &EngineReader,
    run_id: &str,
    nopretty: bool,
    idle_limit: Option<Duration>,
) -> ActionResult {
    let session = session(rt);
    let mut after: Option<u64> = None;
    let mut last_activity = Instant::now();
    let mut completed: Option<(bool, String)> = None;
    loop {
        let rows = match session.block_on(reader.events_after(run_id, after)) {
            Ok(rows) => rows,
            Err(error) => {
                return Ok(SuccessTuple::fail(format!(
                    "Cannot read the events of run {run_id}: {error}"
                )));
            }
        };
        let mut finished: Option<(bool, String)> = None;
        if !rows.is_empty() {
            last_activity = Instant::now();
        }
        for row in &rows {
            after = Some(after.map_or(row.seq, |a| a.max(row.seq)));
            let Some(event) = event_of_row(row) else {
                continue;
            };
            rt.print(&format!("{}\n", line(&event, nopretty)));
            if let EventKind::RunFinished { success, message } = &event.kind {
                finished = Some((*success, message.clone()));
            }
        }
        if finished.is_none() && rows.is_empty() {
            if let Some(outcome) = completed.take() {
                finished = Some(outcome);
            }
            // Events may be off (`system.engine.events: off`): the run row says when it ends.
            else if let Ok(Some(detail)) = session.block_on(reader.run_detail(run_id))
                && detail.summary.status != RunStatus::Running
            {
                completed = Some((
                    detail.summary.status == RunStatus::Succeeded,
                    detail.summary.run.message.clone().unwrap_or_default(),
                ));
                // Completion follows the event flush. Drain again after observing it.
                continue;
            }
        }
        if let Some((success, message)) = finished {
            let message = if message.is_empty() {
                format!(
                    "Run {run_id} {}.",
                    if success { "succeeded" } else { "failed" }
                )
            } else {
                message
            };
            return Ok(SuccessTuple { success, message });
        }
        if let Some(limit) = idle_limit
            && last_activity.elapsed() >= limit
        {
            return Ok(SuccessTuple::fail(format!(
                "Run {run_id} has had no new events for {}s and is not marked finished; \
                 its process may have died. Pass `--timeout` with more seconds (0 waits for good) \
                 to keep following it.",
                limit.as_secs()
            )));
        }
        let mut waited = Duration::ZERO;
        while waited < FOLLOW_EVERY {
            crate::shell::interrupt::check();
            std::thread::sleep(Duration::from_millis(50));
            waited += Duration::from_millis(50);
        }
    }
}

fn line(event: &Event, nopretty: bool) -> String {
    if nopretty {
        serde_json::to_string(event).unwrap_or_default()
    } else {
        event_line(event)
    }
}

/// `watch jobs [NAME ...]`.
pub fn watch_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    if !available(rt, args) {
        return crate::native::jobs::show_jobs(args, rt);
    }
    let ctx = crate::native::jobs::jobs_context(rt);
    let executor_keys = string(args, "executor_keys");
    let executor = meerschaum_jobs::executor::normalize_executor_keys(executor_keys.as_deref())
        .unwrap_or_else(|| meerschaum_jobs::job::get_executor_keys_from_context(&ctx))
        .replace("systemd:main", "systemd");
    let source = JobsSource {
        ctx,
        executor_keys,
        filter: words(args, "action"),
    };
    let mut view = JobsMonitor::new(Some(source), &executor);
    show(rt, &mut view)?;
    Ok(SuccessTuple::success())
}

/// `watch` with an unknown or missing subaction: v4's `choose_subaction`.
pub fn watch(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    crate::native::subactions::choose_subaction("watch", args, rt)
}

/// Run the `watch` function `function` (`_watch_runs`, `_watch_jobs`, `watch`).
pub fn run(function: &str, args: &ArgsMap, rt: &mut Runtime<'_>) -> Option<ActionResult> {
    if function == "watch" {
        return Some(watch(args, rt));
    }
    super::view_for_function(function).map(|view| (view.action)(args, rt))
}
