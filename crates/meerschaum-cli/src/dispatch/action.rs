//! Running one resolved action: the table from action function to implementation, `-s`
//! schedules and `--loop` laps, and the message of an action that raised.

use std::time::Instant;

use super::chain::{chain_params, run_pipeline};
use super::{Runtime, action_words};
use crate::actions::Resolved;
use crate::builtin::{self, ActionError, ActionResult};
use crate::native;
use crate::value::{ArgsMap, PyValue};

use meerschaum_core::SuccessTuple;

/// The action words for messages, shortened.
pub(super) fn action_label(action: &[String]) -> String {
    let label = action.join(" ");
    if label.chars().count() <= 40 {
        return label;
    }
    let mut short: String = label.chars().take(39).collect();
    short.push('…');
    short
}

/// Run an action once, on its `-s` schedule, or in laps for `--loop` (when it does not loop
/// by itself: `loops`).
#[allow(clippy::too_many_arguments)]
pub(super) fn repeat(
    rt: &mut Runtime<'_>,
    args: &ArgsMap,
    skip_schedule: bool,
    loops: bool,
    label: &str,
    noun: &str,
    mut once: impl FnMut(&ArgsMap, &mut Runtime<'_>) -> SuccessTuple,
) -> SuccessTuple {
    let scheduled = !skip_schedule && args.get("schedule").is_some_and(PyValue::is_truthy);
    let looping = loops && native::flag(args, "loop");
    if (scheduled || looping)
        && let Some(failed) = invalid_durations(args)
    {
        return failed;
    }
    // Preserve programmatic overrides. Only dates originally parsed from this
    // command should follow the clock on the next scheduled or looping lap.
    let words: Vec<String> =
        if (scheduled || looping) && ["begin", "end"].iter().any(|key| args.contains_key(*key)) {
            args.get("sysargs")
                .and_then(PyValue::as_list)
                .into_iter()
                .flatten()
                .map(PyValue::py_str)
                .collect()
        } else {
            Vec::new()
        };
    let original = (!words.is_empty())
        .then(|| crate::parse::parse_arguments(&words, &rt.dates).ok())
        .flatten();
    let mut run_lap = |args: &ArgsMap, rt: &mut Runtime<'_>| {
        let Some(original) = original.as_ref() else {
            return once(args, rt);
        };
        match lap_bounds(args, original, &words, &crate::types::DateContext::system()) {
            Ok(fresh) => once(&fresh, rt),
            Err(message) => SuccessTuple::fail(message),
        }
    };
    if let Some(schedule) = args.get("schedule").filter(|v| v.is_truthy())
        && !skip_schedule
    {
        let schedule = schedule.py_str();
        // `--jitter 30s` (v5): the schedule's fixed offset; `jobs.schedule.jitter_seconds` else.
        let jitter = args
            .get("jitter")
            .map(PyValue::py_str)
            .and_then(|v| meerschaum_jobs::schedule::parse_jitter_seconds(&v));
        return crate::native::jobs::run_schedule_jittered(rt, &schedule, jitter, noun, |rt| {
            run_lap(args, rt)
        });
    }
    if looping {
        // The host repeats the action, so each lap runs it once: `loop` is false in a lap's
        // arguments (a plugin action that reads it, or hands its arguments to another action,
        // must not loop inside the first lap).
        let mut lap_args = args.clone();
        lap_args.insert("loop".into(), PyValue::Bool(false));
        return native::jobs_laps::run_looping(rt, &lap_args, label, noun, run_lap);
    }
    once(args, rt)
}

fn lap_bounds(
    args: &ArgsMap,
    original: &ArgsMap,
    words: &[String],
    clock: &crate::types::DateContext,
) -> Result<ArgsMap, String> {
    let parsed = crate::parse::parse_arguments(words, clock).map_err(|e| e.message)?;
    if let Some(PyValue::Exception(error)) = parsed.get(crate::spec::keys::FAILURE_KEY) {
        return Err(error.message.clone());
    }
    let mut fresh = args.clone();
    for key in ["begin", "end"] {
        if original.get(key) == args.get(key)
            && let Some(value) = parsed.get(key)
        {
            fresh.insert(key.into(), value.clone());
        }
    }
    Ok(fresh)
}

/// `--min-seconds` / `--timeout-seconds` that no timer can hold: the failure to return before a
/// loop or a schedule starts (`run_action` checks each run).
fn invalid_durations(args: &ArgsMap) -> Option<SuccessTuple> {
    for key in ["min_seconds", "timeout_seconds"] {
        let Some(seconds) = native::float(args, key) else {
            continue;
        };
        let valid = seconds.is_finite()
            && std::time::Duration::try_from_secs_f64(seconds.max(0.0))
                .ok()
                .and_then(|duration| Instant::now().checked_add(duration))
                .is_some();
        if !valid {
            return Some(SuccessTuple::fail(format!(
                "Invalid value for --{}: use a finite number of seconds within the supported timer range.",
                key.replace('_', "-")
            )));
        }
    }
    None
}

pub(super) fn run_action(
    resolved: &Resolved,
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    depth: usize,
) -> SuccessTuple {
    // Python's float parser accepts NaN, infinity and values too large for a native timer.
    // Keep that parser contract, but fail before performing an action rather than panic in
    // Duration::from_secs_f64 (or Instant addition) after a job has already been changed.
    if resolved.function_name() != "_show_arguments" {
        for key in ["min_seconds", "timeout_seconds"] {
            let Some(seconds) = native::float(args, key) else {
                continue;
            };
            let valid = seconds.is_finite()
                && std::time::Duration::try_from_secs_f64(seconds.max(0.0))
                    .ok()
                    .and_then(|duration| Instant::now().checked_add(duration))
                    .is_some();
            if !valid {
                return SuccessTuple::fail(format!(
                    "Invalid value for --{}: use a finite number of seconds within the supported timer range.",
                    key.replace('_', "-")
                ));
            }
        }
    }
    native::drivers::register_install_reporter(args, rt);
    let result: ActionResult = match resolved.function_name() {
        // A serialized action chain, and v4's form of it (`start pipeline ... -P
        // '{"sub_args_line": ...}'`, which jobs on disk and older API servers carry).
        "_chain" => run_pipeline(args, rt, depth),
        "_start_pipeline" if chain_params(args) => run_pipeline(args, rt, depth),
        // A saved Pipeline's background job.
        "_start_pipeline" | "_start_pipelines" => native::pipeline_jobs::start(args, rt),
        "_stop_pipelines" => native::pipeline_jobs::stop(args, rt),
        "_show_version" => builtin::show_version(args, rt),
        "_show_arguments" => builtin::show_arguments(args, rt),
        "_show_help" => builtin::show_help(args, rt),
        "help" => builtin::help(args, rt),
        "_show_actions" => builtin::show_actions(args, rt),
        "_show_config" => builtin::show_config(args, rt),
        "_show_environment" => builtin::show_environment(args, rt),
        "_show_plugins" => builtin::show_plugins(args, rt),
        "_edit_config" => builtin::edit_config(args, rt),
        "reload" => builtin::reload(args, rt),
        "compose" => crate::compose_action::compose_action(args, rt),
        "python" => builtin::python(args, rt),
        // A server never finishes a "first lap": a job's startup slot (D23) is freed as it starts.
        "_start_api" => {
            meerschaum_jobs::startup::signal_first_lap_done();
            crate::server::start_api(args, rt)
        }
        "_start_executor" => {
            meerschaum_jobs::startup::signal_first_lap_done();
            crate::server::start_executor(args, rt)
        }
        "_start_mcp" => {
            meerschaum_jobs::startup::signal_first_lap_done();
            crate::server::start_mcp(args, rt)
        }
        "_start_webterm" => {
            meerschaum_jobs::startup::signal_first_lap_done();
            crate::server::start_webterm(args, rt)
        }
        "sh" => builtin::sh(args, rt),
        "clear" => builtin::clear(args, rt),
        other if native::residual::handles(other) => native::residual::run(other, args, rt)
            .unwrap_or_else(|| Ok(SuccessTuple::fail("Unhandled action."))),
        other => match crate::native::run(other, args, rt) {
            Some(r) => r,
            None => Err(ActionError::new(
                "NotImplementedError",
                format!(
                    "The action `{}` has no native implementation.",
                    resolved.display_name()
                ),
            )),
        },
    };
    match result {
        Ok(tup) => tup,
        Err(e) => action_failure(resolved, args, &e, rt),
    }
}

/// `_do_action_wrapper`'s message when the action raises.
fn action_failure(
    resolved: &Resolved,
    args: &ArgsMap,
    e: &ActionError,
    _rt: &mut Runtime<'_>,
) -> SuccessTuple {
    let mut name_parts: Vec<String> = resolved
        .function_name()
        .split('_')
        .map(str::to_string)
        .collect();
    name_parts.extend(action_words(args));
    let action_name = name_parts.join(" ");
    // v4 logs the traceback (`kw.get('debug', True)`) and then returns the same text in this
    // message: the error is printed once, in the result's block.
    let mut msg = format!(
        "Failed to execute `{}` with `{}`",
        action_name.trim(),
        e.class
    );
    if e.message.is_empty() {
        msg.push('.');
    } else {
        msg.push_str(&format!(":\n\n{}", e.message));
    }
    SuccessTuple::fail(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_retention_bounds_follow_the_lap_clock_and_preserve_overrides() {
        let clock = |day| {
            let date = chrono::NaiveDate::from_ymd_opt(2026, 10, day).unwrap();
            crate::types::DateContext {
                now_utc: date.and_hms_opt(12, 0, 0).unwrap(),
                dateutil_default: date.and_hms_opt(0, 0, 0).unwrap(),
                local_year: 2026,
            }
        };
        let words = ["clear", "pipes", "--end", "16 days ago"].map(str::to_owned);
        let original = crate::parse::parse_arguments(&words, &clock(24)).unwrap();
        let later = lap_bounds(&original, &original, &words, &clock(25)).unwrap();
        assert_ne!(later["end"], original["end"]);
        assert_eq!(later["end"].py_str(), "2026-10-09 00:00:00+00:00");
        let mut overridden = original.clone();
        overridden.insert("end".into(), PyValue::Int(12));
        assert_eq!(
            lap_bounds(&overridden, &original, &words, &clock(25)).unwrap()["end"],
            PyValue::Int(12)
        );
    }
}
