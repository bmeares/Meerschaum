//! Serial laps with a separate due time and retry cooldown for each step of one job.

use super::*;
use meerschaum_jobs::LapPacer;
use std::sync::atomic::Ordering;

struct Step {
    words: Vec<String>,
    pacer: LapPacer,
    due: Instant,
}

pub(super) fn run(
    sysargs: &[String],
    patch: &ArgsMap,
    delay: f64,
    rt: &mut Runtime<'_>,
    depth: usize,
) -> ActionResult {
    let now = Instant::now();
    let mut steps = Vec::new();
    for words in
        split_chained_sysargs(sysargs).map_err(|e| ActionError::new(&e.class, e.message))?
    {
        let args = parse_arguments(&words, &rt.dates)
            .map_err(|e| ActionError::new(&e.class, e.message))?;
        if let Some(PyValue::Exception(e)) = args.get(keys::FAILURE_KEY) {
            return Ok(SuccessTuple::fail(e.message.clone()));
        }
        if native::flag(&args, "loop")
            || native::flag(&args, "daemon")
            || args.get("schedule").is_some_and(PyValue::is_truthy)
        {
            return Ok(SuccessTuple::fail(
                "Each independent step must finish one lap. Put --loop and -d after the colon.",
            ));
        }
        let seconds = native::float(&args, "min_seconds").unwrap_or(delay);
        if std::time::Duration::try_from_secs_f64(seconds).is_err() {
            return Ok(SuccessTuple::fail(
                "A step interval must be a finite, nonnegative duration.",
            ));
        }
        steps.push(Step {
            words,
            pacer: LapPacer::new(seconds),
            due: now,
        });
    }
    if steps.is_empty() {
        return Ok(SuccessTuple::fail(
            "A looping chain needs at least one step.",
        ));
    }
    let shortest = steps.iter().map(|s| s.pacer.min_seconds()).reduce(f64::min);
    let mut heartbeat = native::jobs_laps::heartbeat(shortest, "run");
    let stop = native::jobs::stop_requested_flag();
    native::jobs::install_stop_handler(&native::session(rt));
    let mut last = SuccessTuple::success();
    while !stop.load(Ordering::SeqCst) {
        for step in &mut steps {
            let began = Instant::now();
            if began < step.due || stop.load(Ordering::SeqCst) {
                continue;
            }
            heartbeat.lap_started();
            let dates = DateContext::system();
            last = run_chain(&step.words, &dates, Some(patch), |args| {
                entry_with_args(args, rt, depth + 1)
            })
            .unwrap_or_else(|e| SuccessTuple::fail(e.message));
            let Some(due) =
                Instant::now().checked_add(step.pacer.after_lap(began, last.success).duration())
            else {
                return Ok(SuccessTuple::fail(
                    "The next lap exceeds the supported clock range.",
                ));
            };
            step.due = due;
            heartbeat.lap_finished(last.success, &last.message);
            native::info(rt, &last.message);
        }
        meerschaum_jobs::startup::signal_first_lap_done();
        let next = steps.iter().map(|s| s.due).min().expect("nonempty steps");
        let wait = next.saturating_duration_since(Instant::now());
        heartbeat.waiting(wait);
        native::jobs::sleep_unless_stopped(wait.as_secs_f64());
    }
    Ok(last)
}
