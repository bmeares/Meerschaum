//! `edit jobs`: show each job's arguments, read the edited text and recreate the job.

use meerschaum_core::SuccessTuple;
use meerschaum_core::value::Value;
use meerschaum_jobs::{Job, JobOptions};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::value::ArgsMap;

use super::{executor_keys, filtered_jobs, jobs_context};
use crate::native::{flag, info, items_str, plural, words, yes_no};

/// `edit jobs`: show each job's arguments, read the edited text, and recreate the job.
///
/// The native prompt reads one line (v4 opens a multi-line `prompt_toolkit` editor seeded with
/// the job's label, submitted with Esc + Enter): an empty line keeps the current arguments, and
/// end of input or an interrupt ends the action with "Nothing was changed."
pub fn edit_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let exec_keys = executor_keys(args);
    let (yes, noask) = (flag(args, "yes"), flag(args, "noask"));
    let debug = flag(args, "debug");
    let ctx = jobs_context(rt);
    let mut jobs = match filtered_jobs(rt, &ctx, exec_keys.as_deref(), &action, false, false) {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
    if jobs.is_empty() {
        return Ok(SuccessTuple::fail("No jobs to edit."));
    }
    let names: Vec<String> = jobs.keys().cloned().collect();
    let mut num_edited = 0usize;
    for name in &names {
        let Some(job) = jobs.get_mut(name) else {
            continue;
        };
        let sysargs = job.sysargs();
        // A serialized chain (`chain ... -P '{"sub_args_line": ...}'`) is shown in its
        // `a + b : --loop` form.
        let pipeline_lines = pipeline_lines(&sysargs);
        let sysargs_str = match &pipeline_lines {
            Some((sub, pipeline)) => format!("{sub} : {pipeline}"),
            None => crate::shlex::join(&sysargs),
        };
        crate::shell::clear_screen(rt, debug);
        info(
            rt,
            &format!(
                "Editing arguments for job '{name}'.\n    Press [Esc + Enter] to submit, [CTRL + C] to exit.\n\n    Tip: join actions with `+`, add flags for the whole chain after `:`.\n    https://meerschaum.io/reference/actions/#chaining-actions\n"
            ),
        );
        let label = job.label();
        rt.print(&format!("{label}\n"));
        let Some(answer) = super::prompt(rt, "", false, noask) else {
            rt.print("\n");
            return Ok(SuccessTuple::ok("Nothing was changed."));
        };
        let new_str = if answer.trim().is_empty() {
            label
        } else {
            answer
        };
        if new_str.trim() == sysargs_str.trim() {
            continue;
        }
        let new_sysargs = match crate::shlex::split(&new_str) {
            Ok(s) => s,
            Err(e) => return Err(ActionError::new("ValueError", e.to_string())),
        };
        let (new_sysargs, pipeline_args) = crate::sysargs::split_pipeline_sysargs(&new_sysargs);
        let chained = crate::sysargs::split_chained_sysargs(&new_sysargs)
            .map_err(|e| ActionError::new(&e.class, e.message.clone()))?;
        crate::shell::clear_screen(rt, debug);
        if chained.len() > 1 {
            let options: Vec<String> = chained.iter().map(|s| crate::shlex::join(s)).collect();
            let text = rt.with_formatter(|f| {
                f.print_options(
                    &options,
                    &crate::format::OptionsArgs {
                        header: Some(format!("\nSteps in Job '{name}':")),
                        number_options: true,
                        ..Default::default()
                    },
                )
            });
            rt.print(&text);
        } else {
            let header = rt.with_formatter(|f| f.make_header(&format!("Action for Job '{name}':")));
            rt.print(&format!(
                "\n{header}\n{}\n",
                crate::shlex::join(&new_sysargs)
            ));
        }
        if !pipeline_args.is_empty() {
            let header = rt.with_formatter(|f| f.make_header("Chain Arguments:"));
            rt.print(&format!(
                "\n{header}\n{}\n\n",
                crate::shlex::join(&pipeline_args)
            ));
        }
        if !yes_no(
            rt,
            &format!(
                "Are you sure you want to recreate job '{name}' with the above arguments?\n    The job will be started if you continue."
            ),
            "n",
            yes,
            noask,
        )? {
            return Ok(SuccessTuple::ok("Nothing was changed."));
        }
        let deleted = job.delete();
        if !deleted.success {
            return Ok(deleted);
        }
        let mut new_job = Job::from_sysargs_str(
            name,
            &new_str,
            JobOptions {
                executor_keys: exec_keys.clone(),
                ..Default::default()
            },
            &ctx,
        )
        .map_err(|e| ActionError::new("ValueError", e.to_string()))?;
        let started = new_job.start();
        if !started.success {
            return Ok(started);
        }
        num_edited += 1;
    }
    let msg = if num_edited > 0 {
        format!(
            "Successfully edited job{} {}.",
            plural(names.len()),
            items_str(&names)
        )
    } else {
        "Nothing was edited.".to_string()
    };
    Ok(SuccessTuple::ok(msg))
}

/// `(sub_args_line, pipeline_args_line)` of a serialized chain job (`chain ... -P '{...}'`, or
/// v4's `start pipeline ... -P '{...}'`).
fn pipeline_lines(sysargs: &[String]) -> Option<(String, String)> {
    let action_len = meerschaum_core::chain::action_len(sysargs)?;
    let rest = &sysargs[action_len..];
    let params_index = rest.iter().position(|a| a == "-P")?;
    let params = rest.get(params_index + 1)?;
    let parsed = Value::from_json_str(params).ok()?;
    let sub_args_line = parsed.get("sub_args_line").map(Value::py_str)?;
    let kept: Vec<String> = rest
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != params_index && *i != params_index + 1)
        .map(|(_, a)| a.clone())
        .collect();
    Some((sub_args_line, crate::shlex::join(&kept)))
}
