//! Action chains: splitting `+` and `:` command lines into steps, running them in order and
//! summarizing them, and the serialized `chain` action that repeats a chain (`xN`, `--loop`).

mod independent;

use std::time::Instant;

use super::child::timed_pipeline_step;
use super::{MAX_ENTRY_DEPTH, Runtime, contains, entry_with_args};
use crate::actions;
use crate::builtin::{ActionError, ActionResult};
use crate::native;
use crate::parse::parse_arguments;
use crate::shlex;
use crate::spec::keys;
use crate::sysargs::{
    get_pipeline_sysargs, split_chained_sysargs, split_pipeline_sysargs,
    sysargs_has_api_executor_keys,
};
use crate::types::DateContext;
use crate::value::{ArgsMap, PyException, PyValue};

use meerschaum_core::SuccessTuple;

/// `entry_without_daemon` without the shell's executor keys and the systemd result file:
/// split `:` pipeline flags and `+` chains, parse each step and run `execute` until one fails.
pub fn run_chain<F>(
    sysargs: &[String],
    dates: &DateContext,
    patch_args: Option<&ArgsMap>,
    execute: F,
) -> Result<SuccessTuple, PyException>
where
    F: FnMut(ArgsMap) -> SuccessTuple,
{
    run_chain_inner(sysargs, dates, patch_args, true, execute)
}

pub(super) fn run_chain_inner<F>(
    sysargs: &[String],
    dates: &DateContext,
    patch_args: Option<&ArgsMap>,
    validate_arguments: bool,
    mut execute: F,
) -> Result<SuccessTuple, PyException>
where
    F: FnMut(ArgsMap) -> SuccessTuple,
{
    let (sysargs, pipeline_args) = split_pipeline_sysargs(sysargs);
    let has_daemon = contains(&sysargs, "-d") || contains(&sysargs, "--daemon");
    let has_start_job = sysargs.len() >= 2 && sysargs[0] == "start" && sysargs[1] == "job";
    let pipeline_has_api_executor_keys = sysargs_has_api_executor_keys(&pipeline_args);

    let mut chained = if has_daemon || has_start_job || pipeline_has_api_executor_keys {
        vec![sysargs.clone()]
    } else {
        split_chained_sysargs(&sysargs)?
    };
    if !pipeline_args.is_empty() {
        chained = vec![get_pipeline_sysargs(&sysargs, &pipeline_args, patch_args)];
    }

    let mut results: Vec<SuccessTuple> = Vec::new();
    let mut steps: Vec<(Instant, Instant)> = Vec::new();
    for step in &chained {
        let step: Vec<String> = step
            .iter()
            .map(|a| {
                if a == keys::ESCAPED_PIPELINE {
                    keys::PIPELINE.to_string()
                } else {
                    a.clone()
                }
            })
            .collect();
        let mut args = parse_arguments(&step, dates)?;
        if let Some(patch) = patch_args {
            for (k, v) in patch {
                args.insert(k.clone(), v.clone());
            }
        }
        if validate_arguments && let Some(PyValue::Exception(e)) = args.get(keys::FAILURE_KEY) {
            let text = args.get("text").map(PyValue::py_str).unwrap_or_default();
            if !text.starts_with("show arguments") {
                let mut msg = "Invalid arguments:".to_string();
                if !text.is_empty() {
                    msg.push('\n');
                    msg.push_str(&text);
                }
                msg.push_str("\n    ");
                msg.push_str(&e.message);
                return Ok(SuccessTuple::fail(msg));
            }
        }
        let start = Instant::now();
        let result = execute(args);
        steps.push((start, Instant::now()));
        let ok = result.success;
        results.push(result);
        if !ok {
            break;
        }
    }

    if chained.len() <= 1 {
        return Ok(results
            .into_iter()
            .next()
            .unwrap_or_else(SuccessTuple::success));
    }
    Ok(summarize_chain(&chained, &results, &steps))
}

/// `make_header(message, left_pad, top=False)` with the Unicode ruler.
fn make_header(message: &str, left_pad: usize) -> String {
    let max_len = message
        .split('\n')
        .map(|w| w.chars().count())
        .max()
        .unwrap_or(0);
    let left = " ".repeat(left_pad);
    format!(
        "{left}{}\n{left}{}",
        message.replace('\n', &format!("\n{left}")),
        "─".repeat(max_len)
    )
}

fn seconds_str(secs: f64) -> String {
    format!("{:.2} seconds", secs)
}

/// The multi-step message of `entry_without_daemon` (durations formatted in seconds).
fn summarize_chain(
    chained: &[Vec<String>],
    results: &[SuccessTuple],
    steps: &[(Instant, Instant)],
) -> SuccessTuple {
    let success = results.iter().all(|r| r.success);
    let successes: Vec<&String> = results
        .iter()
        .filter(|r| r.success)
        .map(|r| &r.message)
        .collect();
    let total = match (steps.first(), steps.last()) {
        (Some(first), Some(last)) => Some(last.1.duration_since(first.0).as_secs_f64()),
        _ => None,
    };
    let plural = |n: usize| if n != 1 { "s" } else { "" };
    let mut msg = String::new();
    if success {
        msg.push_str("Successfully c");
    } else {
        // PARITY: v4 says "Failed pipeline after ..."; chains are "action chains" now that a
        // Pipeline is a saved selection of pipes.
        msg.push_str(&format!(
            "The action chain failed after {} step{}.\n\nC",
            successes.len(),
            plural(successes.len())
        ));
    }
    msg.push_str(&format!(
        "ompleted {} step{}",
        successes.len(),
        plural(successes.len())
    ));
    if let Some(t) = total {
        msg.push_str(&format!(" in {}", seconds_str(t)));
    }
    msg.push_str(":\n\n");
    let mut parts = Vec::new();
    for (i, (m, step)) in successes.iter().zip(chained).enumerate() {
        let dur = steps
            .get(i)
            .map(|(a, b)| b.duration_since(*a).as_secs_f64())
            .unwrap_or(0.0);
        parts.push(format!(
            "{}\n    {m}\n",
            make_header(
                &format!("{}\n  ({})", shlex::join(step), seconds_str(dur)),
                4
            )
        ));
    }
    msg.push_str(&parts.join("\n"));
    let mut msg = msg.trim_end_matches('\n').to_string();
    if let Some(last) = results.last()
        && !last.success
    {
        let fail_step = &chained[results.len() - 1];
        msg.push_str(&format!(
            "\n\nFailed to complete step:\n\n{}\n    {}",
            make_header(&shlex::join(fail_step), 4),
            last.message
        ));
    }
    if let Some(t) = total {
        let n = successes.len();
        let header = format!("| {n} step{} in {} |", plural(n), seconds_str(t));
        msg.push_str(&format!(
            "\n\n\n    {}\n    {header}\n    {}",
            "-".repeat(header.len()),
            "-".repeat(header.len())
        ));
    }
    SuccessTuple {
        success,
        message: msg.trim_end().to_string(),
    }
}

/// The hidden action of a serialized chain: `chain` never resolves through the action table,
/// so it is never listed in help or completion.
pub(super) static CHAIN_ACTION_DEF: actions::ActionDef = actions::ActionDef {
    name: meerschaum_core::chain::CHAIN_ACTION,
    function: "_chain",
    accepts_executor_keys: false,
    subactions: &[],
};

/// Whether the words and arguments are a serialized chain's (`chain ... -P '{"sub_args_line":
/// ...}'`): the word alone is not enough (a plugin may have an action named `chain`).
pub(super) fn is_chain_call(action: &[String], args: &ArgsMap) -> bool {
    action.first().map(String::as_str) == Some(meerschaum_core::chain::CHAIN_ACTION)
        && chain_params(args)
}

/// Whether `-s` describes the job the action starts or stops rather than the action itself:
/// `start pipeline NAME -s ...` gives the schedule to the job it creates, and `stop pipeline NAME
/// -s ...` stops that job once. A chain in v4's `start pipeline ... -P {...}` form keeps its
/// schedule (it is a scheduled chain).
pub(super) fn schedule_is_the_jobs(function: &str, args: &ArgsMap) -> bool {
    match function {
        "_start_pipelines" | "_stop_pipelines" => true,
        "_start_pipeline" => !chain_params(args),
        _ => false,
    }
}

/// Whether `params` carry a chain (`{"sub_args_line": "..."}`).
pub(super) fn chain_params(args: &ArgsMap) -> bool {
    args.get("params")
        .map(native::py_to_value)
        .is_some_and(|p| meerschaum_core::chain::params_carry_chain(&p))
}

/// Run a serialized action chain (`chain <flags> -P '{"sub_args_line": ...}'`, or v4's `start
/// pipeline ...` form): its steps once, `xN` times or in laps for `--loop`. Core steps never
/// start the Python core simply because they form a chain.
pub(super) fn run_pipeline(args: &ArgsMap, rt: &mut Runtime<'_>, depth: usize) -> ActionResult {
    let params = match args.get("params") {
        Some(PyValue::Dict(values)) => values,
        _ => return Ok(SuccessTuple::fail("Nothing to do.")),
    };
    let get_param = |key: &str| {
        params
            .iter()
            .find(|(k, _)| k.as_str() == Some(key))
            .map(|(_, v)| v)
    };
    let Some(PyValue::Str(line)) = get_param("sub_args_line").filter(|v| v.is_truthy()) else {
        return Ok(SuccessTuple::fail("Nothing to do."));
    };
    if depth >= MAX_ENTRY_DEPTH {
        return Ok(SuccessTuple::fail(
            "RecursionError: maximum recursion depth exceeded",
        ));
    }
    let sysargs = shlex::split(line).map_err(|e| ActionError::new("ValueError", e))?;
    let patch: ArgsMap = match get_param("patch_args") {
        Some(PyValue::Dict(values)) => values
            .iter()
            .map(|(k, v)| (k.py_str(), v.clone()))
            .collect(),
        _ => ArgsMap::new(),
    };
    let timeout = native::float(args, "timeout_seconds");
    let timeout_text = args
        .get("timeout_seconds")
        .map(PyValue::py_str)
        .unwrap_or_default();
    let patch_json = match get_param("patch_args") {
        Some(value @ PyValue::Dict(_)) => value
            .json_dumps_compact()
            .map_err(|e| ActionError::new(&e.class, e.message))?,
        _ => "null".to_string(),
    };
    let looping = native::flag(args, "loop");
    let count = native::words(args, "action")
        .first()
        .and_then(|s| s.trim_start_matches('x').parse::<i64>().ok())
        .unwrap_or(if looping { -1 } else { 1 });
    let delay = native::float(args, "min_seconds").unwrap_or(1.0);
    if native::flag(args, "independent_laps") {
        if !looping || timeout.is_some() {
            return Ok(SuccessTuple::fail(
                "Use --independent-laps with : --loop and without a per-process timeout.",
            ));
        }
        return independent::run(&sysargs, &patch, delay, rt, depth);
    }
    let stop = native::jobs::stop_requested_flag();
    native::jobs::install_stop_handler(&native::session(rt));
    // Job health: each run of a repeated chain is a lap (a single run leaves the record to the
    // loops of its steps).
    let mut heartbeat = (looping || count != 1).then(|| {
        native::jobs_laps::heartbeat(
            looping.then_some(delay.max(0.0)),
            native::jobs_laps::lap_noun(&sysargs),
        )
    });
    let mut ran = 0;
    // PARITY: v4 says "pipeline" for chains ("Did not pipeline.", "Ran pipeline 3 times."); a
    // Pipeline is now a saved selection of pipes, so the messages say "action chain".
    let did_not_run = "The action chain did not run.";
    let mut result = SuccessTuple::fail(did_not_run);
    loop {
        if stop.load(std::sync::atomic::Ordering::SeqCst) {
            native::warn(rt, "Cancelled the action chain.");
            if result.message == did_not_run {
                result.message = "The action chain was cancelled.".into();
            }
            break;
        }
        if let Some(heartbeat) = heartbeat.as_mut() {
            heartbeat.lap_started();
        }
        let dates = rt.dates.clone();
        result = match timeout {
            None => match run_chain(&sysargs, &dates, Some(&patch), |args| {
                entry_with_args(args, rt, depth + 1)
            }) {
                Ok(result) => result,
                Err(error) => SuccessTuple::fail(format!("{}: {}", error.class, error.message)),
            },
            // PARITY: with `--timeout-seconds` v4 runs each step in a child interpreter and
            // kills it when the time is up; here the child is this binary.
            Some(seconds) => {
                match timed_pipeline_step(rt, line, &patch_json, seconds, &timeout_text) {
                    Ok(Some(done)) => done,
                    Ok(None) => result.clone(),
                    Err(error) => {
                        native::warn(rt, &error.message);
                        result.clone()
                    }
                }
            }
        };
        ran += 1;
        if let Some(heartbeat) = heartbeat.as_mut() {
            heartbeat.lap_finished(result.success, &result.message);
        }
        // A job's startup slot (D23) is held until its first lap is done.
        meerschaum_jobs::startup::signal_first_lap_done();
        // PARITY: v4 sleeps between attempts, including after failures, and `--loop`
        // overrides the finite `xN` count (`actions/start.py::_start_pipeline`).
        if !looping && count == 1 {
            break;
        }
        if delay != 0.0 && ran != count {
            if let Some(heartbeat) = heartbeat.as_mut() {
                heartbeat.waiting(
                    std::time::Duration::try_from_secs_f64(delay.max(0.0)).unwrap_or_default(),
                );
            }
            native::info(
                rt,
                &format!(
                    "Sleeping for {} seconds...",
                    args.get("min_seconds")
                        .map(PyValue::py_str)
                        .unwrap_or_else(|| "1.0".into())
                ),
            );
            native::jobs::sleep_unless_stopped(delay);
        }
        if !looping && ran >= count {
            break;
        }
    }
    if count != 1 {
        native::info(
            rt,
            &format!(
                "Ran the action chain {ran} time{}.",
                if ran == 1 { "" } else { "s" }
            ),
        );
    }
    Ok(result)
}
