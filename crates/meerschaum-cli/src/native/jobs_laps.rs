//! `--loop` for any action, and the lap heartbeats of the CLI's loops (job health, see
//! `meerschaum_jobs::health`).
//!
//! `mrsm <action> --loop [--min-seconds N]` repeats the action like `sync pipes --loop` does:
//! `N` seconds (default 1) after each lap ends, a cooldown that grows on consecutive failures,
//! and a stop request (`stop jobs`, Ctrl-C) ends the loop after the current lap. A paused job is
//! stopped by its executor (SIGSTOP), so it simply resumes where it was. Actions that loop on
//! their own (`sync`, `verify` and `deduplicate pipes`, chains) or hand `--loop` to a job (`start
//! job ...`, `-d`) keep their behavior; see [`handles_own_loop`].

use std::sync::atomic::Ordering;
use std::time::Instant;

use meerschaum_core::SuccessTuple;
use meerschaum_jobs::{Heartbeat, LapPacer, LapPause};

use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::value::{ArgsMap, PyValue};

use super::{flag, float, info, print_tuple, warn};

/// The action functions that already loop (or hand `--loop` to something that does): the
/// dispatcher never wraps them in [`run_looping`].
const OWN_LOOP: &[&str] = &[
    // They loop themselves (`sync pipelines NAME --loop` in the engine's continuous mode, so
    // members follow their own `schedule`).
    "_sync_pipes",
    "_sync_pipelines",
    "_sync_pipeline",
    "_verify_pipes",
    "_verify_rowcounts",
    "_deduplicate_pipes",
    "_restart_jobs",
    "_chain",
    "_start_pipeline",
    // `--loop` is the job's (`start job sync pipes --loop`), or the Pipeline job's.
    "_start_jobs",
    "_stop_jobs",
    "_pause_jobs",
    "_delete_jobs",
    "_edit_jobs",
    "_bootstrap_jobs",
    "_start_pipelines",
    "_stop_pipelines",
    "_attach_jobs",
    "_attach_logs",
    "_show_logs",
    // Long-running or interactive by nature.
    "_start_api",
    "_start_mcp",
    "_start_webterm",
    "_start_worker",
    "_start_gui",
    "_start_daemons",
    "_api_start",
    "api",
    "compose",
    "stack",
    "watch",
    "_watch_jobs",
    "_watch_logs",
    "_watch_runs",
];

/// Whether the action function handles `--loop` itself (a sync, a chain, the jobs actions,
/// servers and watchers), so the dispatcher must not wrap it in [`run_looping`].
pub fn handles_own_loop(function: &str) -> bool {
    OWN_LOOP.contains(&function)
}

/// What a lap of `words` does, for the job health text: `sync` when every chained step syncs,
/// else `run`.
pub fn lap_noun<S: AsRef<str>>(words: &[S]) -> &'static str {
    let mut steps = words.split(|w| w.as_ref() == "+").filter(|s| !s.is_empty());
    let mut any = false;
    let all_sync = steps.all(|step| {
        any = true;
        step.first().is_some_and(|w| w.as_ref() == "sync")
    });
    if any && all_sync { "sync" } else { "run" }
}

/// `--min-seconds` as v4 prints it (`1.0`, `300.0`), and as a number (default 1).
fn min_seconds(args: &ArgsMap) -> (f64, String) {
    let seconds = float(args, "min_seconds").unwrap_or(1.0);
    let text = match args.get("min_seconds") {
        Some(v) if !matches!(v, PyValue::None) => v.py_str(),
        _ => "1".to_string(),
    };
    (seconds, text)
}

/// The heartbeat of a loop with this `--min-seconds` (inactive outside a job).
pub fn heartbeat(interval_seconds: Option<f64>, noun: &str) -> Heartbeat {
    Heartbeat::from_env(interval_seconds, noun)
}

/// The heartbeat of `sync pipes --loop` / `--listen` over `pipes` (every pipe of a flat
/// selection starts its laps): see [`sync_cadence`].
pub fn sync_heartbeat(args: &ArgsMap, pipes: &[super::pipes::SelectedPipe]) -> Heartbeat {
    let (interval, stall) = sync_cadence(args, pipes.iter().map(|sp| &sp.pipe));
    let mut heartbeat = heartbeat(interval, "sync");
    heartbeat.set_stall_seconds(stall);
    heartbeat
}

/// What the laps of `sync --loop` / `--listen` are expected at, given the pipes that start them
/// (`roots`: every pipe of a flat selection, the pipes without parents in a graph run): every
/// `--min-seconds`, or the shortest `schedule` of `roots` when each of them syncs only on its
/// own schedule (the loop idles between their fire times). `--listen` alone has no cadence: it
/// is stalled after an hour without a sync (the second value, a fixed stall threshold).
pub fn sync_cadence<'a>(
    args: &ArgsMap,
    roots: impl IntoIterator<Item = &'a meerschaum_core::Pipe>,
) -> (Option<f64>, Option<f64>) {
    if !flag(args, "loop") {
        return (
            Some(meerschaum_jobs::health::LISTEN_INTERVAL_SECONDS),
            Some(meerschaum_jobs::health::LISTEN_STALL_SECONDS),
        );
    }
    let (seconds, _) = min_seconds(args);
    let periods: Vec<Option<f64>> = roots
        .into_iter()
        .map(|pipe| {
            pipe.parameters()
                .get("schedule")
                .and_then(meerschaum_core::Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .and_then(meerschaum_jobs::schedule::period_seconds)
        })
        .collect();
    let scheduled = !periods.is_empty() && periods.iter().all(Option::is_some);
    let shortest = periods.into_iter().flatten().reduce(f64::min);
    let interval = match shortest {
        Some(period) if scheduled => seconds.max(period),
        _ => seconds.max(0.0),
    };
    (Some(interval), None)
}

/// What an engine event means for the heartbeat of a sync loop: progress within a lap (a chunk
/// or a pipe synced), or a stream flush (`--listen`), which counts as a lap of its own.
pub fn sync_event(heartbeat: &mut Heartbeat, event: &meerschaum_engine::Event, listen_only: bool) {
    use meerschaum_engine::EventKind;
    match &event.kind {
        EventKind::ChunkFinished { .. } | EventKind::NodeFinished { .. } => heartbeat.progress(),
        EventKind::StreamFlushed {
            success, message, ..
        } => {
            if listen_only {
                heartbeat.event(*success, message);
            } else {
                heartbeat.progress();
            }
        }
        _ => {}
    }
}

/// Run `lap` until a stop is requested, `--min-seconds` apart (see the module documentation).
/// `label` names the action in messages (`show pipes`). Returns the last lap's result.
pub fn run_looping(
    rt: &mut Runtime<'_>,
    args: &ArgsMap,
    label: &str,
    noun: &str,
    mut lap: impl FnMut(&ArgsMap, &mut Runtime<'_>) -> SuccessTuple,
) -> SuccessTuple {
    let nopretty = flag(args, "nopretty");
    let (seconds, seconds_text) = min_seconds(args);
    let mut pacer = LapPacer::new(seconds);
    let mut heartbeat = heartbeat(Some(pacer.min_seconds()), noun);
    let stop = super::jobs::stop_requested_flag();
    super::jobs::install_stop_handler(&super::session(rt));
    let mut result = SuccessTuple::fail(format!("`{label}` did not run."));
    let mut laps = 0u64;
    // Whether the last lap's result was printed here (else the caller prints it).
    let mut printed = false;
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let began = Instant::now();
        heartbeat.lap_started();
        result = lap(args, rt);
        laps += 1;
        printed = false;
        heartbeat.lap_finished(result.success, &result.message);
        // A job's startup slot (D23) is held until its first lap is done.
        meerschaum_jobs::startup::signal_first_lap_done();
        if stop.load(Ordering::SeqCst) {
            break;
        }
        if !nopretty {
            print_tuple(rt, &result, TupleArgs::default());
            printed = true;
        }
        let pause = pacer.after_lap(began, result.success);
        heartbeat.waiting(pause.duration());
        match pause {
            LapPause::Sleep(pause) => {
                if pause.is_zero() {
                    continue;
                }
                let s = if seconds.abs() != 1.0 { "s" } else { "" };
                info(rt, &format!("Sleeping for {seconds_text} second{s}."));
                if super::jobs::sleep_unless_stopped(pause.as_secs_f64()) {
                    break;
                }
            }
            LapPause::Cooldown(pause) => {
                warn(
                    rt,
                    &format!(
                        "`{label}` failed. Trying again in {}.",
                        meerschaum_jobs::health::humanize_seconds(pause.as_secs_f64())
                    ),
                );
                if super::jobs::sleep_unless_stopped(pause.as_secs_f64()) {
                    break;
                }
            }
        }
    }
    if laps == 0 {
        return SuccessTuple::fail(format!("`{label}` was stopped before it ran."));
    }
    if printed {
        return SuccessTuple {
            success: result.success,
            message: format!(
                "Stopped `{label}` after {laps} lap{}.",
                if laps == 1 { "" } else { "s" }
            ),
        };
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nouns_and_own_loops() {
        assert_eq!(lap_noun(&["sync", "pipes"]), "sync");
        assert_eq!(
            lap_noun(&["sync", "pipes", "+", "sync", "pipelines", "x"]),
            "sync"
        );
        assert_eq!(lap_noun(&["sync", "pipes", "+", "show", "version"]), "run");
        assert_eq!(lap_noun(&["show", "pipes"]), "run");
        assert_eq!(lap_noun::<&str>(&[]), "run");
        assert!(handles_own_loop("_sync_pipes"));
        // A Pipeline's loop is the engine's continuous mode, not laps of whole runs.
        assert!(handles_own_loop("_sync_pipelines"));
        assert!(handles_own_loop("_sync_pipeline"));
        assert!(handles_own_loop("_start_jobs"));
        assert!(!handles_own_loop("_show_pipes"));
        assert!(!handles_own_loop("sh"));
    }

    #[test]
    fn a_loop_expects_the_shortest_root_schedule_and_listening_an_hour() {
        let ctx = crate::types::DateContext::system();
        let parse = |words: &[&str]| {
            let words: Vec<String> = words.iter().map(|s| s.to_string()).collect();
            crate::parse::parse_arguments(&words, &ctx).unwrap()
        };
        let pipe = |schedule: Option<&str>| {
            let mut pipe = meerschaum_core::Pipe::new("sql:a", "m").unwrap();
            if let Some(schedule) = schedule {
                let mut parameters = meerschaum_core::value::Map::new();
                parameters.insert("schedule".into(), schedule.into());
                pipe.set_parameters(parameters);
            }
            pipe
        };
        let looping = parse(&["sync", "pipes", "--loop", "--min-seconds", "60"]);
        let hourly = pipe(Some("every 1 hour"));
        let daily = pipe(Some("every 1 day"));
        let plain = pipe(None);
        // Every root on a schedule: the shortest schedule is the pace.
        assert_eq!(
            sync_cadence(&looping, [&hourly, &daily]),
            (Some(3600.0), None)
        );
        // A root without one syncs every lap.
        assert_eq!(
            sync_cadence(&looping, [&hourly, &plain]),
            (Some(60.0), None)
        );
        // `--listen` alone: an hour, as a fixed threshold.
        assert_eq!(
            sync_cadence(&parse(&["sync", "pipes", "--listen"]), [&plain]),
            (Some(3600.0), Some(3600.0))
        );
    }
}
