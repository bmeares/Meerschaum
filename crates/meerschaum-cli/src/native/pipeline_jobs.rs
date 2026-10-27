//! `start pipeline NAME` / `stop pipeline NAME` (and the plurals): run a saved Pipeline in the
//! background, or stop it.
//!
//! A saved Pipeline's background job is one whose command syncs it on its registry:
//! `sync pipelines NAME -i <registry> ...` (what the console's Manage → Schedule creates). `start
//! pipeline` starts those jobs; without one it creates `pipeline-NAME`, which runs `sync
//! pipelines NAME -i <registry> --loop` (`-s` makes it a schedule instead, `--min-seconds` sets
//! the pace, `--name` names the job). `stop pipeline` stops them.
//!
//! Before rc15, `start pipeline ... -P '{"sub_args_line": ...}'` was how chains (`a + b : --loop`)
//! were stored; the dispatcher still runs that form as a chain (`dispatch::run_action`).

use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_jobs::{Job, JobOptions, StartKind};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::value::{ArgsMap, PyValue};

use super::{info, items_str, plural, string, warn, words};

fn error(message: impl ToString) -> ActionError {
    ActionError::new("ValueError", message.to_string())
}

/// Whether a job's `sysargs` sync the saved Pipeline `name` of `registry` (the console's rule:
/// `sync pipeline(s) NAME ... -i <registry>`).
pub fn syncs_pipeline(sysargs: &[String], name: &str, registry: &str) -> bool {
    let starts = sysargs.len() >= 3
        && sysargs[0] == "sync"
        && matches!(sysargs[1].as_str(), "pipeline" | "pipelines")
        && sysargs[2] == name;
    if !starts {
        return false;
    }
    let instance = sysargs.iter().enumerate().find_map(|(i, a)| {
        if matches!(a.as_str(), "-i" | "--instance" | "--mrsm-instance") {
            return sysargs.get(i + 1).cloned();
        }
        ["--instance=", "--mrsm-instance="]
            .iter()
            .find_map(|prefix| a.strip_prefix(prefix).map(str::to_string))
    });
    instance.as_deref() == Some(registry)
}

/// The saved Pipelines' names on `instance`, for messages.
fn saved_names(
    rt: &mut Runtime<'_>,
    instance: &super::Instance,
) -> Result<Vec<String>, ActionError> {
    Ok(super::pipelines::list(rt, instance)?
        .into_iter()
        .map(|d| d.name)
        .collect())
}

fn not_saved(names: &[String], registry: &str, saved: &[String]) -> SuccessTuple {
    let what = if names.len() == 1 {
        format!("Pipeline '{}' is not saved on '{registry}'.", names[0])
    } else {
        format!(
            "Pipelines {} are not saved on '{registry}'.",
            items_str(names)
        )
    };
    let next = if saved.is_empty() {
        "No Pipelines are saved there yet: create one with `bootstrap pipeline`.".to_string()
    } else {
        format!("Saved Pipelines: {}.", saved.join(", "))
    };
    SuccessTuple::fail(format!("{what}\n{next}"))
}

fn no_names(registry: &str, saved: Option<&[String]>, verb: &str) -> SuccessTuple {
    let Some(saved) = saved else {
        // The saved Pipelines could not be read (an unreachable instance).
        return SuccessTuple::fail(format!(
            "Name the Pipeline to {verb}, e.g. `{verb} pipeline NAME -i {registry}`."
        ));
    };
    if saved.is_empty() {
        return SuccessTuple::fail(format!(
            "No Pipelines are saved on '{registry}': create one with `bootstrap pipeline`."
        ));
    }
    SuccessTuple::fail(format!(
        "Name the Pipeline to {verb}, e.g. `{verb} pipeline {}`.\nSaved Pipelines on '{registry}': {}.",
        saved[0],
        saved.join(", ")
    ))
}

/// The pace of a new Pipeline job given neither `--min-seconds` nor a `min_seconds` default saved
/// in the Pipeline: a lap a minute rather than `sync pipes --loop`'s second, since each lap is a
/// recorded run of the whole graph (members with a `schedule` of their own sync when it is due).
pub const DEFAULT_JOB_MIN_SECONDS: &str = "60";

/// The command of a new background job for the Pipeline `name` (`saved_pace`: the Pipeline
/// saves a `min_seconds` default, which the job then follows).
fn new_job_sysargs(args: &ArgsMap, name: &str, registry: &str, saved_pace: bool) -> Vec<String> {
    let mut sysargs: Vec<String> = ["sync", "pipelines", name, "-i", registry]
        .iter()
        .map(|s| s.to_string())
        .collect();
    match string(args, "schedule").filter(|s| !s.trim().is_empty()) {
        Some(schedule) => {
            sysargs.push("--schedule".into());
            sysargs.push(schedule);
            if let Some(jitter) = string(args, "jitter") {
                sysargs.push("--jitter".into());
                sysargs.push(jitter);
            }
        }
        None => {
            sysargs.push("--loop".into());
            match args
                .get("min_seconds")
                .filter(|v| !matches!(v, PyValue::None))
            {
                Some(seconds) => {
                    sysargs.push("--min-seconds".into());
                    sysargs.push(seconds.py_str());
                }
                None if !saved_pace => {
                    sysargs.push("--min-seconds".into());
                    sysargs.push(DEFAULT_JOB_MIN_SECONDS.into());
                }
                None => {}
            }
        }
    }
    // Writes to protected instances are allowed only when the job carries the flag (Q20).
    if super::flag(args, "allow_protected") {
        sysargs.push("--allow-protected".into());
    }
    sysargs
}

/// Whether a job's `sysargs` give it a schedule (`-s` / `--schedule`).
fn is_scheduled(sysargs: &[String]) -> bool {
    sysargs
        .iter()
        .any(|a| a == "-s" || a == "--schedule" || a.starts_with("--schedule="))
}

/// Which of a Pipeline's jobs (`(job, scheduled)`) `start pipeline NAME` starts, and which it
/// leaves alone: its scheduled jobs when it has any (several may share a Pipeline, on different
/// schedules), else all of them.
fn jobs_to_start(matching: &[(String, bool)]) -> (Vec<String>, Vec<String>) {
    let any_scheduled = matching.iter().any(|(_, scheduled)| *scheduled);
    let (mut start, mut left) = (Vec::new(), Vec::new());
    for (job, scheduled) in matching {
        if *scheduled || !any_scheduled {
            start.push(job.clone());
        } else {
            left.push(job.clone());
        }
    }
    (start, left)
}

/// Whether running jobs (`(job, scheduled)`) sync a Pipeline's pipes more than once: several
/// run and one of them is a loop (a loop alongside a schedule, or two loops). Several schedules
/// on one Pipeline are a choice (different options or times).
fn syncs_twice(running: &[(String, bool)]) -> bool {
    running.len() > 1 && running.iter().any(|(_, scheduled)| !*scheduled)
}

/// Warn when [`syncs_twice`].
fn warn_if_concurrent(rt: &mut Runtime<'_>, pipeline: &str, running: &[(String, bool)]) {
    if !syncs_twice(running) {
        return;
    }
    let names: Vec<String> = running.iter().map(|(job, _)| job.clone()).collect();
    warn(
        rt,
        &format!(
            "Jobs {} all sync Pipeline '{pipeline}', so its pipes sync more than once: stop the \
             ones it does not need with `stop job NAME`.",
            items_str(&names)
        ),
    );
}

/// The jobs (by name) of every executor this command sees, hidden ones included.
fn listed_jobs(
    rt: &mut Runtime<'_>,
    ctx: &meerschaum_jobs::JobsContext,
    executor_keys: Option<&str>,
) -> Result<IndexMap<String, Job>, ActionError> {
    let listing = meerschaum_jobs::executor::get_jobs_for(ctx, executor_keys, true, true)
        .map_err(|e| error(format!("Could not read the jobs: {e}")))?;
    for w in &listing.warnings {
        warn(rt, w);
    }
    Ok(listing.jobs)
}

/// `start pipeline NAME [NAME ...]`: start each saved Pipeline's background job, creating
/// `pipeline-NAME` when it has none.
pub fn start(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let requested = dedup(words(args, "action"));
    let instance = super::get_instance(args, rt)?;
    let registry = instance.keys();
    if requested.is_empty() {
        let saved = saved_names(rt, &instance).ok();
        return Ok(no_names(&registry, saved.as_deref(), "start"));
    }
    let definitions = super::pipelines::list(rt, &instance)?;
    let saved: Vec<String> = definitions.iter().map(|d| d.name.clone()).collect();
    let unknown: Vec<String> = requested
        .iter()
        .filter(|n| !saved.contains(n))
        .cloned()
        .collect();
    if !unknown.is_empty() {
        return Ok(not_saved(&unknown, &registry, &saved));
    }
    let explicit_name = string(args, "name");
    if explicit_name.is_some() && requested.len() > 1 {
        return Ok(SuccessTuple::fail(
            "`--name` names one job: start one Pipeline at a time with `--name`.",
        ));
    }
    let ctx = super::jobs::jobs_context(rt);
    let executor_keys = string(args, "executor_keys");
    let mut jobs = listed_jobs(rt, &ctx, executor_keys.as_deref())?;
    let (mut started, mut failed): (Vec<String>, Vec<String>) = (Vec::new(), Vec::new());
    for name in &requested {
        // The Pipeline's jobs: whether each has a schedule, and whether it runs already.
        let mut matching: Vec<(String, bool)> = Vec::new();
        let mut running: Vec<(String, bool)> = Vec::new();
        for (job_name, job) in jobs.iter_mut() {
            let sysargs = job.sysargs();
            if syncs_pipeline(&sysargs, name, &registry) {
                let entry = (job_name.clone(), is_scheduled(&sysargs));
                if job.is_running() {
                    running.push(entry.clone());
                }
                matching.push(entry);
            }
        }
        let targets: Vec<String> = match &explicit_name {
            Some(job_name) if jobs.contains_key(job_name) => {
                if !matching.iter().any(|(job, _)| job == job_name) {
                    return Ok(SuccessTuple::fail(format!(
                        "The job '{job_name}' does not run Pipeline '{name}'; pick another `--name`."
                    )));
                }
                vec![job_name.clone()]
            }
            Some(_) => Vec::new(),
            None => {
                let (start, left) = jobs_to_start(&matching);
                if !left.is_empty() {
                    info(
                        rt,
                        &format!(
                            "Pipeline '{name}' has a scheduled job: left {} as {} (`start job \
                             NAME` starts one).",
                            items_str(&left),
                            if left.len() == 1 {
                                "it was"
                            } else {
                                "they were"
                            },
                        ),
                    );
                }
                start
            }
        };
        if targets.is_empty() {
            let job_name = explicit_name
                .clone()
                .unwrap_or_else(|| format!("pipeline-{name}"));
            if jobs.contains_key(&job_name) {
                return Ok(SuccessTuple::fail(format!(
                    "A job named '{job_name}' already exists and does not run Pipeline '{name}'; \
                     pass `--name` to choose another name."
                )));
            }
            let saved_pace = definitions
                .iter()
                .find(|d| &d.name == name)
                .is_some_and(|d| d.defaults.contains_key("min_seconds"));
            let sysargs = new_job_sysargs(args, name, &registry, saved_pace);
            let command = crate::shlex::join(&sysargs);
            let scheduled = is_scheduled(&sysargs);
            let mut job = Job::new(
                &job_name,
                Some(sysargs),
                JobOptions {
                    executor_keys: executor_keys.clone(),
                    env: super::jobs::compose_env(rt),
                    ..Default::default()
                },
                &ctx,
            )
            .map_err(|e| error(e.to_string()))?;
            job.set_start_kind(StartKind::Single);
            let result = job.start();
            super::jobs::emit_job_warnings(rt, &mut job);
            if result.success {
                info(
                    rt,
                    &format!("Created job '{job_name}' for Pipeline '{name}':\n    {command}"),
                );
                // `--name` may create a second job next to a running one.
                running.push((job_name.clone(), scheduled));
                warn_if_concurrent(rt, name, &running);
                started.push(job_name);
            } else {
                warn(
                    rt,
                    &format!(
                        "Failed to start job '{job_name}' for Pipeline '{name}':\n{}",
                        result.message
                    ),
                );
                failed.push(job_name);
            }
            continue;
        }
        for job_name in targets {
            let Some(job) = jobs.get_mut(&job_name) else {
                continue;
            };
            if job.is_running() {
                info(
                    rt,
                    &format!("The job '{job_name}' for Pipeline '{name}' is already running."),
                );
                started.push(job_name);
                continue;
            }
            job.set_start_kind(StartKind::Single);
            let result = job.start();
            super::jobs::emit_job_warnings(rt, job);
            if result.success {
                info(
                    rt,
                    &format!(
                        "Started job '{job_name}' for Pipeline '{name}':\n    {}",
                        job.label().replace('\n', " ")
                    ),
                );
                if let Some(entry) = matching.iter().find(|(job, _)| *job == job_name) {
                    running.push(entry.clone());
                }
                started.push(job_name);
            } else {
                warn(
                    rt,
                    &format!(
                        "Failed to start job '{job_name}' for Pipeline '{name}':\n{}",
                        result.message
                    ),
                );
                failed.push(job_name);
            }
        }
        warn_if_concurrent(rt, name, &running);
    }
    Ok(summary("start", "Started", &started, &failed))
}

/// `stop pipeline NAME [NAME ...]`: stop each saved Pipeline's background jobs.
pub fn stop(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let requested = dedup(words(args, "action"));
    let instance = super::get_instance(args, rt)?;
    let registry = instance.keys();
    if requested.is_empty() {
        let saved = saved_names(rt, &instance).ok();
        return Ok(no_names(&registry, saved.as_deref(), "stop"));
    }
    let saved = saved_names(rt, &instance)?;
    let ctx = super::jobs::jobs_context(rt);
    let executor_keys = string(args, "executor_keys");
    let mut jobs = listed_jobs(rt, &ctx, executor_keys.as_deref())?;
    let timeout = match args.get("timeout_seconds") {
        Some(PyValue::Int(i)) => i64::try_from(*i).ok().map(meerschaum_jobs::Seconds::Int),
        Some(PyValue::Float(f)) => Some(meerschaum_jobs::Seconds::Float(*f)),
        _ => None,
    };
    // A Pipeline's jobs, by the Pipeline's name. A name that is not saved still stops the jobs
    // that sync it (a deleted Pipeline's job); a name with neither is refused before anything
    // is stopped, as `start` does.
    let mut targets: Vec<(String, Vec<String>)> = Vec::new();
    let mut unknown: Vec<String> = Vec::new();
    for name in &requested {
        let mut matching: Vec<String> = Vec::new();
        for (job_name, job) in jobs.iter_mut() {
            if syncs_pipeline(&job.sysargs(), name, &registry) {
                matching.push(job_name.clone());
            }
        }
        if matching.is_empty() && !saved.contains(name) {
            unknown.push(name.clone());
        }
        targets.push((name.clone(), matching));
    }
    if !unknown.is_empty() {
        return Ok(not_saved(&unknown, &registry, &saved));
    }
    let (mut stopped, mut failed): (Vec<String>, Vec<String>) = (Vec::new(), Vec::new());
    for (name, matching) in targets {
        if matching.is_empty() {
            info(rt, &format!("No background job runs Pipeline '{name}'."));
            continue;
        }
        for job_name in matching {
            let Some(job) = jobs.get_mut(&job_name) else {
                continue;
            };
            // A stopped job is left alone, unless it would restart by itself (a stopped
            // `--loop` job that was not stopped by hand): stopping it keeps it stopped.
            if job.status() == meerschaum_jobs::Status::Stopped
                && (!job.restarts() || job.stop_time().is_some())
            {
                info(
                    rt,
                    &format!("The job '{job_name}' (Pipeline '{name}') is not running."),
                );
                continue;
            }
            let result = job.stop(timeout);
            super::jobs::emit_job_warnings(rt, job);
            if result.success {
                info(
                    rt,
                    &format!("Stopped job '{job_name}' (Pipeline '{name}')."),
                );
                stopped.push(job_name);
            } else {
                warn(
                    rt,
                    &format!("Failed to stop job '{job_name}':\n{}", result.message),
                );
                failed.push(job_name);
            }
        }
    }
    if stopped.is_empty() && failed.is_empty() {
        return Ok(SuccessTuple::ok(
            "No background jobs were running for these Pipelines.",
        ));
    }
    Ok(summary("stop", "Stopped", &stopped, &failed))
}

fn dedup(names: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for name in names {
        if !out.contains(&name) {
            out.push(name);
        }
    }
    out
}

fn summary(verb: &str, done: &str, ok: &[String], failed: &[String]) -> SuccessTuple {
    let mut message = String::new();
    if !ok.is_empty() {
        message.push_str(&format!(
            "{done} job{} {}.",
            plural(ok.len()),
            items_str(ok)
        ));
    }
    if !failed.is_empty() {
        if !message.is_empty() {
            message.push('\n');
        }
        message.push_str(&format!(
            "Failed to {verb} job{} {}.",
            plural(failed.len()),
            items_str(failed)
        ));
    }
    SuccessTuple {
        success: failed.is_empty(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_pipeline_job_syncs_the_pipeline_on_its_registry() {
        let console = v(&[
            "sync",
            "pipeline",
            "daily",
            "-i",
            "sql:main",
            "--schedule",
            "hourly",
        ]);
        assert!(syncs_pipeline(&console, "daily", "sql:main"));
        assert!(!syncs_pipeline(&console, "daily", "sql:other"));
        assert!(!syncs_pipeline(&console, "weekly", "sql:main"));
        let created = v(&["sync", "pipelines", "daily", "-i", "sql:main", "--loop"]);
        assert!(syncs_pipeline(&created, "daily", "sql:main"));
        assert!(syncs_pipeline(
            &v(&["sync", "pipelines", "daily", "--instance=sql:main"]),
            "daily",
            "sql:main"
        ));
        // Without `-i` the job's registry is whatever the default instance is when it runs.
        assert!(!syncs_pipeline(
            &v(&["sync", "pipelines", "daily", "--loop"]),
            "daily",
            "sql:main"
        ));
        assert!(!syncs_pipeline(
            &v(&["sync", "pipes", "-i", "sql:main"]),
            "daily",
            "sql:main"
        ));
    }

    #[test]
    fn new_jobs_loop_or_follow_a_schedule() {
        let ctx = crate::types::DateContext::system();
        let parse = |words: &[&str]| crate::parse::parse_arguments(&v(words), &ctx).unwrap();
        // A lap a minute by default: each lap is a recorded run of the whole graph.
        assert_eq!(
            new_job_sysargs(
                &parse(&["start", "pipeline", "daily"]),
                "daily",
                "sql:main",
                false
            ),
            v(&[
                "sync",
                "pipelines",
                "daily",
                "-i",
                "sql:main",
                "--loop",
                "--min-seconds",
                DEFAULT_JOB_MIN_SECONDS
            ])
        );
        // A Pipeline that saves a `min_seconds` default keeps it.
        assert_eq!(
            new_job_sysargs(
                &parse(&["start", "pipeline", "daily"]),
                "daily",
                "sql:main",
                true
            ),
            v(&["sync", "pipelines", "daily", "-i", "sql:main", "--loop"])
        );
        assert_eq!(
            new_job_sysargs(
                &parse(&["start", "pipeline", "daily", "--min-seconds", "1"]),
                "daily",
                "sql:main",
                true
            ),
            v(&[
                "sync",
                "pipelines",
                "daily",
                "-i",
                "sql:main",
                "--loop",
                "--min-seconds",
                "1.0"
            ])
        );
        let scheduled = new_job_sysargs(
            &parse(&["start", "pipeline", "daily", "-s", "every 10 minutes"]),
            "daily",
            "sql:main",
            false,
        );
        assert_eq!(
            scheduled,
            v(&[
                "sync",
                "pipelines",
                "daily",
                "-i",
                "sql:main",
                "--schedule",
                "every 10 minutes"
            ])
        );
        assert!(is_scheduled(&scheduled));
        assert!(is_scheduled(&v(&["sync", "pipeline", "x", "-s", "daily"])));
        assert!(is_scheduled(&v(&[
            "sync",
            "pipeline",
            "x",
            "--schedule=daily"
        ])));
        assert!(!is_scheduled(&v(&["sync", "pipelines", "x", "--loop"])));
    }

    #[test]
    fn a_pipeline_with_a_schedule_starts_its_scheduled_jobs() {
        let jobs = |items: &[(&str, bool)]| -> Vec<(String, bool)> {
            items.iter().map(|(j, s)| (j.to_string(), *s)).collect()
        };
        // A loop job and a scheduled one: only the scheduled one starts (Q31).
        assert_eq!(
            jobs_to_start(&jobs(&[("pipeline-daily", false), ("sched-daily", true)])),
            (v(&["sched-daily"]), v(&["pipeline-daily"]))
        );
        // Several schedules are a choice: all of them start.
        assert_eq!(
            jobs_to_start(&jobs(&[("hourly", true), ("nightly", true)])),
            (v(&["hourly", "nightly"]), v(&[]))
        );
        // No schedule: the loop job (or jobs) start.
        assert_eq!(
            jobs_to_start(&jobs(&[("pipeline-daily", false)])),
            (v(&["pipeline-daily"]), v(&[]))
        );
        assert_eq!(jobs_to_start(&[]), (v(&[]), v(&[])));
        // A loop next to another running job syncs the pipes twice; two schedules do not.
        assert!(syncs_twice(&jobs(&[("a", false), ("b", true)])));
        assert!(syncs_twice(&jobs(&[("a", false), ("b", false)])));
        assert!(!syncs_twice(&jobs(&[("a", true), ("b", true)])));
        assert!(!syncs_twice(&jobs(&[("a", false)])));
    }
}
