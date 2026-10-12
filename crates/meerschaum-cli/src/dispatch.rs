//! The `mrsm` entry point and action dispatch, ported from `meerschaum/__main__.py` (`main`),
//! `meerschaum/_internal/entry.py` (`entry_without_daemon`, `entry_with_args`) and
//! `meerschaum/_internal/arguments/_parser.py` (`parse_help`, `parse_version`).
//!
//! Only `show version`, `show arguments`, `show help` and `--help` run natively in this wave;
//! every other action resolves by the v4 naming convention and returns a failure tuple.

use std::io::Write;
use std::time::Instant;

use crate::actions::{self, CORE_ACTIONS, Resolved};
use crate::env;
use crate::help;
use crate::parse::parse_arguments;
use crate::schema;
use crate::shlex;
use crate::spec::keys;
use crate::sysargs::{
    get_pipeline_sysargs, split_chained_sysargs, split_pipeline_sysargs,
    sysargs_has_api_executor_keys,
};
use crate::types::DateContext;
use crate::value::{ArgsMap, PyException, PyValue};

/// `(success, message)`, the v4 `SuccessTuple`.
// TODO(core-merge): replace with `meerschaum_core::SuccessTuple` once the core crate exports it.
pub type SuccessTuple = (bool, String);

/// The flag that prints the argument JSON schema (Rust-only; intercepted before parsing).
pub const PRINT_ARG_SCHEMA_FLAG: &str = "--print-arg-schema";

/// v4 recurses through `entry` when a `+` survives into `entry_with_args`; cap the depth.
const MAX_ENTRY_DEPTH: usize = 32;

/// The dispatcher's environment: clock, terminal width and output stream.
pub struct Runtime<'a> {
    /// Clock inputs for datetime parsing.
    pub dates: DateContext,
    /// Help width in columns.
    pub columns: usize,
    /// Where actions print.
    pub out: &'a mut dyn Write,
    /// Environment lookup (for `MRSM_*` variables).
    pub env: fn(&str) -> Option<String>,
}

impl<'a> Runtime<'a> {
    /// The process runtime writing to `out`.
    pub fn system(out: &'a mut dyn Write) -> Self {
        Self {
            dates: DateContext::system(),
            columns: help::terminal_columns(),
            out,
            env: env::process_env,
        }
    }
}

fn contains(sysargs: &[String], flag: &str) -> bool {
    sysargs.iter().any(|a| a == flag)
}

/// `meerschaum.__main__.main`: returns the process exit code.
pub fn main(sysargs: &[String], rt: &mut Runtime<'_>) -> i32 {
    if contains(sysargs, PRINT_ARG_SCHEMA_FLAG) {
        let schema = serde_json::to_string_pretty(&schema::arg_schema()).unwrap_or_default();
        let _ = writeln!(rt.out, "{schema}");
        return 0;
    }
    if contains(sysargs, "--help") || contains(sysargs, "-h") {
        parse_help(sysargs, rt);
        return 0;
    }
    if contains(sysargs, "--version") || contains(sysargs, "-V") {
        parse_version(sysargs, rt);
        return 0;
    }
    let nopretty = contains(sysargs, "--nopretty");
    let result = if sysargs.is_empty() || contains(sysargs, "--shell") {
        (
            false,
            "The Meerschaum shell is not implemented in Rust yet.".to_string(),
        )
    } else {
        entry(sysargs, rt)
    };
    // PARITY: v4 only sets a failing exit code when the tuple is printed, so `--nopretty`
    // always exits 0 (`meerschaum/__main__.py` `main`).
    if nopretty {
        return 0;
    }
    print_tuple(&result, rt.out);
    if result.0 { 0 } else { 1 }
}

/// `print_tuple(tup, upper_padding=1)`.
pub fn print_tuple(tup: &SuccessTuple, out: &mut dyn Write) {
    // PARITY: STATIC_CONFIG['system']['success']['ignore'] is missing a comma, so 'success' and
    // 'Succeeded' are concatenated into 'successSucceeded' (`meerschaum/_internal/static.py`).
    let ignore = ["Success", "successSucceeded", ""];
    if ignore.contains(&tup.1.trim()) {
        return;
    }
    let icon = if tup.0 { "🎉" } else { "💢" };
    let msg = format!(" {icon} {}", tup.1);
    let mut lines: Vec<String> = Vec::new();
    for (i, line) in msg.split('\n').enumerate() {
        if i > 0 && !line.starts_with(' ') {
            lines.push(format!("    {line}"));
        } else {
            lines.push(line.to_string());
        }
    }
    let _ = writeln!(out, "\n{}\n", lines.join("\n"));
}

/// `entry(sysargs)`: run a (possibly chained) command line.
pub fn entry(sysargs: &[String], rt: &mut Runtime<'_>) -> SuccessTuple {
    entry_at_depth(sysargs, rt, 0)
}

fn entry_at_depth(sysargs: &[String], rt: &mut Runtime<'_>, depth: usize) -> SuccessTuple {
    if depth > MAX_ENTRY_DEPTH {
        // PARITY: v4 recurses without bound here and dies with RecursionError
        // (`entry_with_args` re-entering `entry` for `start job x + ...`).
        return (
            false,
            "RecursionError: maximum recursion depth exceeded".into(),
        );
    }
    let dates = rt.dates.clone();
    let result = run_chain(sysargs, &dates, None, |args| {
        entry_with_args(args, rt, depth)
    });
    match result {
        Ok(tup) => tup,
        Err(e) => (false, format!("{}: {}", e.class, e.message)),
    }
}

/// `entry_without_daemon` without the shell's executor keys and the systemd result file:
/// split `:` pipeline flags and `+` chains, parse each step and run `execute` until one fails.
pub fn run_chain<F>(
    sysargs: &[String],
    dates: &DateContext,
    patch_args: Option<&ArgsMap>,
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
        if let Some(PyValue::Exception(e)) = args.get(keys::FAILURE_KEY) {
            let text = args.get("text").map(PyValue::py_str).unwrap_or_default();
            if !text.starts_with("show arguments") {
                let mut msg = "Invalid arguments:".to_string();
                if !text.is_empty() {
                    msg.push('\n');
                    msg.push_str(&text);
                }
                msg.push_str("\n    ");
                msg.push_str(&e.message);
                return Ok((false, msg));
            }
        }
        let start = Instant::now();
        let result = execute(args);
        steps.push((start, Instant::now()));
        let ok = result.0;
        results.push(result);
        if !ok {
            break;
        }
    }

    if chained.len() <= 1 {
        return Ok(results
            .into_iter()
            .next()
            .unwrap_or((true, "Success".into())));
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
    let success = results.iter().all(|r| r.0);
    let successes: Vec<&String> = results.iter().filter(|r| r.0).map(|r| &r.1).collect();
    let total = match (steps.first(), steps.last()) {
        (Some(first), Some(last)) => Some(last.1.duration_since(first.0).as_secs_f64()),
        _ => None,
    };
    let plural = |n: usize| if n != 1 { "s" } else { "" };
    let mut msg = String::new();
    if success {
        msg.push_str("Successfully c");
    } else {
        msg.push_str(&format!(
            "Failed pipeline after {} step{}.\n\nC",
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
        && !last.0
    {
        let fail_step = &chained[results.len() - 1];
        msg.push_str(&format!(
            "\n\nFailed to complete step:\n\n{}\n    {}",
            make_header(&shlex::join(fail_step), 4),
            last.1
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
    (success, msg.trim_end().to_string())
}

fn action_words(args: &ArgsMap) -> Vec<String> {
    args.get("action")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default()
}

/// `entry_with_args(**args)`: resolve the action function and run it.
pub fn entry_with_args(mut args: ArgsMap, rt: &mut Runtime<'_>, depth: usize) -> SuccessTuple {
    let sysargs: Vec<String> = args
        .get("sysargs")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default();
    if contains(&sysargs, keys::AND) {
        let sysargs: Vec<String> = if contains(&sysargs, "-d") || contains(&sysargs, "--daemon") {
            sysargs
                .iter()
                .map(|a| {
                    if a == keys::AND {
                        keys::ESCAPED_AND.into()
                    } else {
                        a.clone()
                    }
                })
                .collect()
        } else {
            sysargs
        };
        return entry_at_depth(&sysargs, rt, depth + 1);
    }

    let mut action = action_words(&args);
    if action.is_empty() || (action[0] == "mrsm" && action.len() == 1) {
        return (
            false,
            "The Meerschaum shell is not implemented in Rust yet.".into(),
        );
    }

    let mut skip_schedule = false;
    let executor_keys = args
        .get("executor_keys")
        .map(PyValue::py_str)
        .unwrap_or_else(|| "local".into());
    if executor_keys.starts_with("api:") {
        let accepts =
            actions::get_action(&action, CORE_ACTIONS).is_some_and(|r| r.accepts_executor_keys());
        if !accepts {
            let label = executor_keys
                .rsplit(':')
                .next()
                .unwrap_or_default()
                .to_string();
            action.insert(0, "api".into());
            action.insert(1, label);
            skip_schedule = true;
        }
    }
    if args.get("daemon").is_some_and(PyValue::is_truthy) && action[0] != "stack" {
        action.splice(0..0, ["start".to_string(), "jobs".to_string()]);
    }
    let resolved: Resolved = match actions::get_action(&action, CORE_ACTIONS) {
        Some(r) => r,
        None => {
            // Unknown actions run as shell commands in v4.
            action.insert(0, "sh".into());
            match actions::get_action(&["sh"], CORE_ACTIONS) {
                Some(r) => r,
                None => return (false, "action `sh` is not available".into()),
            }
        }
    };
    if action[0] == "start" && action.get(1).is_some_and(|a| a == "job" || a == "jobs") {
        skip_schedule = true;
    }
    let remaining = actions::remove_leading_action(&action, CORE_ACTIONS);
    args.insert("action".into(), PyValue::str_list(&remaining));
    env::apply_env_arguments(&mut args, rt.env);

    if args.get("schedule").is_some_and(PyValue::is_truthy) && !skip_schedule {
        return (
            false,
            "Scheduling actions (`--schedule`) is not implemented in Rust yet.".into(),
        );
    }
    run_action(&resolved, &args, rt)
}

fn run_action(resolved: &Resolved, args: &ArgsMap, rt: &mut Runtime<'_>) -> SuccessTuple {
    match resolved.function_name() {
        "_show_version" => {
            let version = env!("CARGO_PKG_VERSION");
            let nopretty = args.get("nopretty").is_some_and(PyValue::is_truthy);
            let _ = if nopretty {
                writeln!(rt.out, "{version}")
            } else {
                writeln!(rt.out, "Meerschaum v{version}")
            };
            (true, "Success".into())
        }
        "_show_arguments" => {
            let _ = writeln!(rt.out, "{}", format_args_map(args));
            (true, "Success".into())
        }
        "_show_help" => {
            let _ = write!(rt.out, "{}", help::format_help(rt.columns));
            (true, "Success".into())
        }
        _ => (
            false,
            format!(
                "action `{}` is not implemented in Rust yet",
                resolved.display_name()
            ),
        ),
    }
}

/// Render the arguments map as a Python `dict` literal, one key per line.
pub fn format_args_map(args: &ArgsMap) -> String {
    if args.is_empty() {
        return "{}".into();
    }
    let lines: Vec<String> = args
        .iter()
        .map(|(k, v)| format!("    {}: {},", crate::value::repr_str(k), v.py_repr()))
        .collect();
    format!("{{\n{}\n}}", lines.join("\n"))
}

/// `parse_help(sysargs)`: choose which help message to print.
pub fn parse_help(sysargs: &[String], rt: &mut Runtime<'_>) {
    let args = parse_arguments(sysargs, &rt.dates).unwrap_or_default();
    let action = action_words(&args);
    if action.is_empty() {
        let _ = write!(rt.out, "{}", help::format_help(rt.columns));
        return;
    }
    let Some(main) = CORE_ACTIONS.iter().find(|a| a.name == action[0]) else {
        // `show actions`
        let names: Vec<&str> = CORE_ACTIONS.iter().map(|a| a.name).collect();
        let _ = writeln!(rt.out, "Available actions:\n\n  {}", names.join("\n  "));
        return;
    };
    if action.len() > 1 {
        let word = &action[1];
        let sub = main.subactions.iter().find(|s| s.name == word).or_else(|| {
            main.subactions
                .iter()
                .find(|s| s.name == format!("{word}s"))
        });
        if let Some(sub) = sub {
            let words = vec![main.name.to_string(), sub.name.to_string()];
            let _ = write!(rt.out, "{}", help::format_action_help(&words, rt.columns));
            return;
        }
    }
    let _ = write!(
        rt.out,
        "{}",
        help::format_action_help(&[main.name.to_string()], rt.columns)
    );
}

/// `parse_version(sysargs)`.
pub fn parse_version(sysargs: &[String], rt: &mut Runtime<'_>) {
    let version = env!("CARGO_PKG_VERSION");
    let _ = if contains(sysargs, "--nopretty") {
        writeln!(rt.out, "{version}")
    } else {
        writeln!(rt.out, "Meerschaum v{version}")
    };
}
