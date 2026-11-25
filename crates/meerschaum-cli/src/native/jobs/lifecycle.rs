//! Starting, stopping, pausing, restarting and deleting jobs, and moving local jobs to systemd.

use std::sync::atomic::Ordering;

use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_core::static_config;
use meerschaum_core::value::Value;
use meerschaum_jobs::{Job, JobOptions, JobsContext, StartKind, Status};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::value::{ArgsMap, PyValue};

use super::listing::pprint_jobs;
use super::stop_flag::{install_stop_handler, sleep_unless_stopped, stop_requested_flag};
use super::{
    executor_keys, filtered_jobs, filtered_jobs_with, jobs_context, subset,
    warn_custom_executor_failure, with_status,
};
use crate::native::{
    flag, info, items_str, items_str_with, plural, print_tuple, string, warn, words, yes_no,
};

/// Whether a word names a v4 action (`actions` keys), or the internal action of a serialized
/// chain (a `-d` chain is never taken for a job of that name).
fn is_action_name(word: &str) -> bool {
    word == meerschaum_core::chain::CHAIN_ACTION
        || crate::actions::CORE_ACTIONS.iter().any(|a| a.name == word)
}

/// How a `start jobs` is classified for the startup gate (D23): `--no-stagger` bypasses it,
/// `$MRSM_JOB_START_KIND` (set by compose, which starts jobs one command at a time) names the
/// kind, more than one job is a bulk start, and one job is an explicit start (no jitter).
pub fn start_kind_of(args: &ArgsMap, rt: &mut Runtime<'_>, job_count: usize) -> StartKind {
    if flag(args, "no_stagger") {
        return StartKind::None;
    }
    if let Some(kind) = (rt.env)(meerschaum_jobs::startup::KIND_ENV)
        .as_deref()
        .and_then(StartKind::parse)
    {
        return kind;
    }
    if job_count > 1 {
        StartKind::Bulk
    } else {
        StartKind::Single
    }
}

/// `start jobs`.
pub fn start_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let name = string(args, "name");
    let sysargs: Vec<String> = words(args, "sysargs");
    let line = string(args, "line");
    let rm = flag(args, "rm");
    let force = flag(args, "force");
    let yes = flag(args, "yes");
    let noask = flag(args, "noask");
    let nopretty = flag(args, "nopretty");
    let exec_keys = executor_keys(args);
    let ctx = jobs_context(rt);
    let mut jobs = match filtered_jobs(rt, &ctx, exec_keys.as_deref(), &action, false, false) {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };

    let mut names: Vec<String>;
    let mut new_job = !action.is_empty();
    if !action.is_empty() {
        let mut known: Vec<String> = Vec::new();
        let mut unknown: Vec<String> = Vec::new();
        for a in &action {
            if jobs.contains_key(a) {
                known.push(a.clone());
            } else {
                unknown.push(a.clone());
            }
        }
        if !known.is_empty()
            && !unknown.is_empty()
            && known[0] == action[0]
            && is_action_name(&known[0])
        {
            let k = known.remove(0);
            unknown.insert(0, k);
        }
        new_job = known.is_empty();
        if !new_job && !unknown.is_empty() && !nopretty {
            warn(
                rt,
                &format!(
                    "Unknown job{} {} will be ignored.",
                    if unknown.len() > 1 { "s" } else { "" },
                    items_str(&unknown)
                ),
            );
        }
        if new_job {
            names = vec![
                name.clone()
                    .unwrap_or_else(|| meerschaum_jobs::names::get_new_daemon_name(&ctx.paths)),
            ];
        } else if name.is_none() {
            names = known;
        } else {
            return Ok(SuccessTuple::fail(format!(
                "Unknown job{} {}.",
                plural(action.len()),
                items_str_with(&action, true, true, "or")
            )));
        }
    } else if let Some(n) = &name {
        new_job = false;
        names = vec![n.clone()];
    } else {
        let running = with_status(&mut jobs, Status::Running);
        let paused = with_status(&mut jobs, Status::Paused);
        let stopped = with_status(&mut jobs, Status::Stopped);
        if stopped.is_empty() && paused.is_empty() {
            if running.is_empty() {
                return Ok(SuccessTuple::fail("No jobs to start"));
            }
            return Ok(SuccessTuple::ok("All jobs are running."));
        }
        names = stopped.into_iter().chain(paused).collect();
    }

    if names.is_empty() {
        return Ok(SuccessTuple::fail("No jobs to start."));
    }

    let mut skipped: Vec<String> = Vec::new();
    if !force {
        let mut filtered = match filtered_jobs(rt, &ctx, exec_keys.as_deref(), &names, false, false)
        {
            Ok(jobs) => jobs,
            Err(failed) => return Ok(failed),
        };
        if !filtered.is_empty() {
            let running = with_status(&mut filtered, Status::Running);
            if !running.is_empty() {
                let mut running_jobs = subset(&ctx, &filtered, &running);
                pprint_jobs(rt, &mut running_jobs, false);
                if yes_no(rt, "Do you want to first stop these jobs?", "n", yes, noask)? {
                    let mut stop_args = ArgsMap::new();
                    let mut stop_action = vec!["jobs".to_string()];
                    stop_action.extend(running.iter().cloned());
                    stop_args.insert("action".into(), PyValue::str_list(&stop_action[1..]));
                    stop_args.insert("force".into(), PyValue::Bool(true));
                    if let Some(k) = &exec_keys {
                        stop_args.insert("executor_keys".into(), PyValue::Str(k.clone()));
                    }
                    let res = stop_jobs(&stop_args, rt)?;
                    if !res.success {
                        warn(
                            rt,
                            &format!(
                                "Failed to stop job{}{}.",
                                plural(running.len()),
                                items_str(&running)
                            ),
                        );
                        for n in &running {
                            names.retain(|x| x != n);
                            filtered.shift_remove(n);
                        }
                    }
                } else {
                    info(
                        rt,
                        &format!(
                            "Skipping already running job{} {}.",
                            plural(running.len()),
                            items_str(&running)
                        ),
                    );
                    for n in &running {
                        names.retain(|x| x != n);
                        filtered.shift_remove(n);
                        skipped.push(n.clone());
                    }
                }
            }
            if filtered.is_empty() {
                return Ok(SuccessTuple {
                    success: !skipped.is_empty(),
                    message: "No jobs to start.".into(),
                });
            }
            pprint_jobs(rt, &mut filtered, nopretty);
            info(
                rt,
                &format!(
                    "Starting the job{} {}...",
                    plural(names.len()),
                    items_str(&names)
                ),
            );
        }
    }

    let compose_env: Vec<(String, String)> = compose_env(rt).into_iter().flatten().collect();
    let (mut successes, mut failures) = (Vec::new(), Vec::new());
    let start_kind = start_kind_of(args, rt, names.len());
    let startup_jitter = string(args, "startup_jitter")
        .and_then(|v| meerschaum_jobs::schedule::parse_jitter_seconds(&v));
    for n in names.clone() {
        let tup = if new_job {
            run_new_job(
                rt,
                &ctx,
                &n,
                &sysargs,
                line.as_deref(),
                exec_keys.clone(),
                rm,
                compose_env.clone(),
                (force, yes, noask),
                (start_kind, startup_jitter),
            )
        } else {
            let mut job = match jobs.shift_remove(&n) {
                Some(j) => j,
                None => match Job::new(
                    &n,
                    None,
                    JobOptions {
                        executor_keys: exec_keys.clone(),
                        ..Default::default()
                    },
                    &ctx,
                ) {
                    Ok(j) => j,
                    Err(e) => return Err(ActionError::new("ValueError", e.to_string())),
                },
            };
            job.set_start_kind(start_kind);
            job.start()
        };
        if !nopretty {
            print_tuple(rt, &tup, TupleArgs::default());
        }
        if tup.success {
            successes.push(n);
        } else {
            failures.push(n);
        }
    }
    let mut msg = String::new();
    if !successes.is_empty() {
        msg.push_str(&format!(
            "Successfully started job{} {}.{}",
            plural(successes.len()),
            items_str(&successes),
            if failures.is_empty() { "" } else { "\n" }
        ));
    }
    if !failures.is_empty() {
        msg.push_str(&format!(
            "Failed to start job{} {}.",
            plural(failures.len()),
            items_str(&failures)
        ));
    }
    if msg.is_empty() {
        msg = "Nothing changed.".into();
    }
    Ok(SuccessTuple {
        success: failures.is_empty(),
        message: msg,
    })
}

#[allow(clippy::too_many_arguments)]
fn run_new_job(
    rt: &mut Runtime<'_>,
    ctx: &JobsContext,
    name: &str,
    sysargs: &[String],
    line: Option<&str>,
    executor_keys: Option<String>,
    rm: bool,
    compose_env: Vec<(String, String)>,
    (force, yes, noask): (bool, bool, bool),
    (start_kind, startup_jitter): (StartKind, Option<f64>),
) -> SuccessTuple {
    if sysargs.len() >= 2 && sysargs[0] == "start" && sysargs[1].starts_with("job") {
        return SuccessTuple::fail("Create a new job with `-d` / `--daemon`.");
    }
    let job_sysargs: String = match line {
        Some(l) => l.to_string(),
        None => crate::shlex::join(
            &sysargs
                .iter()
                .filter(|a| *a != "-d" && *a != "--daemon")
                .cloned()
                .collect::<Vec<_>>(),
        ),
    };
    let env =
        (!compose_env.is_empty()).then(|| compose_env.into_iter().collect::<IndexMap<_, _>>());
    // `--startup-jitter 30s` is this job's own startup jitter bound (persisted as the `startup`
    // property, which compose's per-job settings also write).
    let properties = startup_jitter.map(|seconds| {
        let mut startup = meerschaum_core::value::Map::new();
        startup.insert("jitter_seconds".into(), Value::Float(seconds));
        let mut patch = meerschaum_core::value::Map::new();
        patch.insert("startup".into(), Value::Map(startup));
        patch
    });
    let opts = JobOptions {
        env,
        executor_keys,
        delete_after_completion: rm,
        properties,
        ..Default::default()
    };
    let mut job = match Job::from_sysargs_str(name, &job_sysargs, opts, ctx) {
        Ok(j) => j,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    job.set_start_kind(start_kind);
    for w in std::mem::take(&mut job.warnings) {
        warn(rt, &w);
    }
    if !job.exists() {
        let confirm = force
            || yes_no(
                rt,
                &format!("Create new job '{name}'?\n    {job_sysargs}\n"),
                "y",
                yes,
                noask,
            )
            // PARITY: `except Exception: confirm = False`.
            .unwrap_or(false);
        if !confirm {
            return SuccessTuple::fail("Nothing changed.");
        }
    }
    let started = job.start();
    emit_job_warnings(rt, &mut job);
    started
}

/// The Compose stamps a job started from a Compose project records in its environment (the
/// project's name, and the root compose file of a tree): `None` outside Compose.
pub fn compose_env(rt: &mut Runtime<'_>) -> Option<IndexMap<String, String>> {
    let environment = &static_config().environment;
    let env: IndexMap<String, String> = [&environment.compose_project, &environment.compose_root]
        .into_iter()
        .filter_map(|var| {
            (rt.env)(var)
                .filter(|v| !v.is_empty())
                .map(|value| (var.clone(), value))
        })
        .collect();
    (!env.is_empty()).then_some(env)
}

/// Show the warnings a job collected (while it was built, and while its executor created it: a
/// `systemd` unit without a pinned interpreter) through the formatter.
pub fn emit_job_warnings(rt: &mut Runtime<'_>, job: &mut Job) {
    for w in std::mem::take(&mut job.warnings) {
        warn(rt, &w);
    }
}

/// `stop jobs`.
pub fn stop_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let (force, yes, noask) = (flag(args, "force"), flag(args, "yes"), flag(args, "noask"));
    let timeout = timeout_seconds(args);
    let ctx = jobs_context(rt);
    let exec_keys = executor_keys(args);
    let mut jobs = match filtered_jobs(rt, &ctx, exec_keys.as_deref(), &action, !nopretty, false) {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
    let running = with_status(&mut jobs, Status::Running);
    let paused = with_status(&mut jobs, Status::Paused);
    let restart: Vec<String> = jobs
        .iter_mut()
        .filter_map(|(n, j)| j.restarts().then(|| n.clone()))
        .collect();
    let stopped: Vec<String> = with_status(&mut jobs, Status::Stopped)
        .into_iter()
        .filter(|n| !restart.contains(n))
        .collect();
    let mut to_stop: Vec<String> = Vec::new();
    for n in running.iter().chain(&paused).chain(&restart) {
        if !to_stop.contains(n) {
            to_stop.push(n.clone());
        }
    }
    if !action.is_empty() && !stopped.is_empty() && !nopretty {
        warn(
            rt,
            &format!(
                "Skipping stopped job{} '{}'.",
                plural(stopped.len()),
                stopped.join("', '")
            ),
        );
    }
    if to_stop.is_empty() {
        if !jobs.is_empty() {
            return Ok(SuccessTuple::ok("The selected jobs are currently stopped."));
        }
        return Ok(SuccessTuple::fail(
            "No running, paused or restarting jobs to stop.",
        ));
    }
    if action.is_empty() && !force {
        let mut sub = subset(&ctx, &jobs, &to_stop);
        pprint_jobs(rt, &mut sub, false);
        if !yes_no(rt, "Stop the above jobs?", "n", yes, noask)? {
            return Ok(SuccessTuple::fail("No jobs were stopped."));
        }
    }
    let (mut ok, mut fail) = (0usize, 0usize);
    for n in &to_stop {
        let Some(job) = jobs.get_mut(n) else { continue };
        let res = job.stop(timeout);
        for w in std::mem::take(&mut job.warnings) {
            warn(rt, &w);
        }
        warn_custom_executor_failure(rt, job, &res);
        if res.success {
            ok += 1;
        } else {
            fail += 1;
        }
    }
    let mut msg = format!("Stopped {ok} job{}.", plural(ok));
    if fail > 0 {
        msg.push_str(&format!("\nFailed to stop {fail} job{}.", plural(fail)));
    }
    Ok(SuccessTuple {
        success: ok > 0,
        message: msg,
    })
}

fn timeout_seconds(args: &ArgsMap) -> Option<meerschaum_jobs::Seconds> {
    match args.get("timeout_seconds") {
        Some(PyValue::Int(i)) => i64::try_from(*i).ok().map(meerschaum_jobs::Seconds::Int),
        Some(PyValue::Float(f)) => Some(meerschaum_jobs::Seconds::Float(*f)),
        _ => None,
    }
}

/// `pause jobs`.
pub fn pause_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let (force, yes, noask) = (flag(args, "force"), flag(args, "yes"), flag(args, "noask"));
    let ctx = jobs_context(rt);
    let exec_keys = executor_keys(args);
    let mut jobs = match filtered_jobs(rt, &ctx, exec_keys.as_deref(), &action, !nopretty, false) {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
    let running = with_status(&mut jobs, Status::Running);
    let paused = with_status(&mut jobs, Status::Paused);
    let stopped = with_status(&mut jobs, Status::Stopped);
    if !action.is_empty() && !stopped.is_empty() && !nopretty {
        warn(
            rt,
            &format!(
                "Skipping stopped job{} '{}'.",
                if stopped.len() > 1 { "s" } else { "" },
                stopped.join("', '")
            ),
        );
    }
    let to_pause: Vec<String> = running.iter().chain(&paused).cloned().collect();
    if to_pause.is_empty() {
        return Ok(SuccessTuple::fail(
            "No running jobs to pause. You can start jobs with `-d` or `start jobs`.",
        ));
    }
    if action.is_empty() && !force {
        let mut sub = subset(&ctx, &jobs, &running);
        pprint_jobs(rt, &mut sub, false);
        if !yes_no(rt, "Pause all running jobs?", "n", yes, noask)? {
            return Ok(SuccessTuple::fail("No jobs were paused."));
        }
    }
    let (mut successes, mut fails) = (Vec::new(), Vec::new());
    for n in &to_pause {
        let Some(job) = jobs.get_mut(n) else { continue };
        let res = job.pause(None);
        warn_custom_executor_failure(rt, job, &res);
        if res.success {
            successes.push(n.clone());
        } else {
            fails.push(n.clone());
        }
    }
    let mut msg = String::new();
    if !successes.is_empty() {
        msg.push_str(&format!(
            "Successfully paused job{} {}.{}",
            plural(successes.len()),
            items_str(&successes),
            if fails.is_empty() { "" } else { "\n" }
        ));
    }
    if !fails.is_empty() {
        msg.push_str(&format!(
            "Failed to pause job{} {}.",
            plural(fails.len()),
            items_str(&fails)
        ));
    }
    Ok(SuccessTuple {
        success: fails.is_empty(),
        message: msg,
    })
}

/// `restart jobs`.
pub fn restart_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let do_loop = flag(args, "loop");
    let min_seconds = super::float(args, "min_seconds").unwrap_or(1.0);
    let min_seconds_str = match args.get("min_seconds") {
        Some(v) if !matches!(v, PyValue::None) => v.py_str(),
        _ => "1.0".to_string(),
    };
    let ctx = jobs_context(rt);
    // PARITY: `get_filtered_jobs(executor_keys or 'local', ..., combine_local_and_systemd=False)`
    // (`actions/restart.py:84`): restarting defaults to the local executor, not the context's.
    let exec_keys = executor_keys(args).unwrap_or_else(|| "local".to_string());
    if do_loop {
        install_stop_handler(&super::session(rt));
    }
    loop {
        let jobs = match filtered_jobs_with(rt, &ctx, Some(&exec_keys), &action, false, true, false)
        {
            Ok(jobs) => jobs,
            Err(failed) => return Ok(failed),
        };
        let mut restart_jobs: IndexMap<String, Job> = if action.is_empty() {
            jobs.into_iter()
                .filter_map(|(n, mut j)| j.restarts().then_some((n, j)))
                .collect()
        } else {
            jobs
        };
        if restart_jobs.is_empty() && !do_loop {
            return Ok(SuccessTuple::ok("No jobs need to be restarted."));
        }
        if flag(args, "no_stagger") {
            // Restarts are bulk starts (D23); `--no-stagger` bypasses the startup gate.
            for job in restart_jobs.values_mut() {
                job.set_start_kind(StartKind::None);
            }
        }
        // Earlier 5.0 release candidates ran the jobs of every root but the default one locally; on a
        // systemd host they move to systemd now (stopped first, logs kept), so they come back
        // after a reboot. An explicit `-e` keeps them where they are.
        if executor_keys(args).is_none() && moves_local_jobs_to_systemd(&ctx) {
            let names: Vec<String> = restart_jobs.keys().cloned().collect();
            for name in names {
                let Some(mut local) = restart_jobs.shift_remove(&name) else {
                    continue;
                };
                let moved = move_job_to_systemd(rt, &ctx, &mut local);
                if !moved.success {
                    warn(
                        rt,
                        &format!("Could not move job '{name}' to systemd:\n{}", moved.message),
                    );
                    restart_jobs.insert(name, local);
                }
            }
            if restart_jobs.is_empty() && !do_loop {
                return Ok(SuccessTuple::ok("Moved the jobs to systemd."));
            }
        }
        let names: Vec<String> = restart_jobs.keys().cloned().collect();
        info(
            rt,
            &format!(
                "Checking job{} {}...",
                plural(names.len()),
                items_str(&names)
            ),
        );
        let res = meerschaum_jobs::executor::check_restart_jobs(&mut restart_jobs);
        if !do_loop {
            return Ok(res);
        }
        if min_seconds != 0.0 {
            info(rt, &format!("Sleeping for {min_seconds_str} seconds..."));
            if sleep_unless_stopped(min_seconds) {
                return Ok(res);
            }
        }
        if stop_requested_flag().load(Ordering::SeqCst) {
            return Ok(res);
        }
    }
}

/// Whether this context's `local` jobs belong on `systemd` (see [`move_job_to_systemd`]): the
/// context chooses `systemd` and its root is not the default one, whose local jobs were always
/// someone's explicit choice.
pub fn moves_local_jobs_to_systemd(ctx: &JobsContext) -> bool {
    meerschaum_jobs::job::get_executor_keys_from_context(ctx) == "systemd"
        && !meerschaum_jobs::systemd::SystemdExecutor::new(ctx.clone()).is_default_root()
}

/// Recreate a `local` job under `systemd` with the same command and settings: the unit's
/// creation stops the local process and moves its logs (see
/// `meerschaum_jobs::systemd::SystemdExecutor`).
pub fn move_job_to_systemd(
    rt: &mut Runtime<'_>,
    ctx: &JobsContext,
    local: &mut Job,
) -> SuccessTuple {
    let sysargs = local.sysargs();
    if sysargs.is_empty() {
        return SuccessTuple::fail("The job has no command.");
    }
    let mut properties = local.daemon().properties();
    for key in ["target", "label", "result", "process", "externally_managed"] {
        properties.shift_remove(key);
    }
    let mut job = match Job::new(
        &local.name,
        Some(sysargs),
        JobOptions {
            executor_keys: Some("systemd".into()),
            properties: Some(properties),
            ..Default::default()
        },
        ctx,
    ) {
        Ok(job) => job,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    job.set_start_kind(StartKind::Single);
    let started = job.start();
    emit_job_warnings(rt, &mut job);
    started
}

/// `delete jobs`.
pub fn delete_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let (force, yes, noask) = (flag(args, "force"), flag(args, "yes"), flag(args, "noask"));
    let ctx = jobs_context(rt);
    let exec_keys = executor_keys(args);
    let mut jobs = match filtered_jobs(rt, &ctx, exec_keys.as_deref(), &action, false, false) {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
    let selected_executor = exec_keys
        .clone()
        .unwrap_or_else(|| meerschaum_jobs::job::get_executor_keys_from_context(&ctx));
    let exclusions = !action.is_empty()
        && action.iter().all(|n| n.starts_with('_'))
        && !action.iter().any(|n| jobs.contains_key(n));
    let mut missing = Vec::new();
    if !exclusions {
        for name in &action {
            if jobs.contains_key(name) || missing.contains(name) {
                continue;
            }
            // Retired local/systemd jobs may only have their properties and lap history.
            // An explicit name can claim this metadata after its unit has gone.
            if matches!(selected_executor.as_str(), "local" | "systemd")
                && meerschaum_jobs::names::invalid_job_name_reason(name).is_none()
                && meerschaum_jobs::names::is_direct_child(
                    &ctx.paths.jobs_dir,
                    &ctx.paths.jobs_dir.join(name),
                    name,
                )
                && ctx
                    .paths
                    .jobs_dir
                    .join(name)
                    .join("properties.json")
                    .is_file()
                && !ctx.paths.systemd_jobs_dir.join(name).exists()
                && let Ok(mut job) = Job::new(
                    name,
                    None,
                    JobOptions {
                        executor_keys: Some("local".into()),
                        ..Default::default()
                    },
                    &ctx,
                )
                && job.status() == Status::Stopped
            {
                jobs.insert(name.clone(), job);
                continue;
            }
            missing.push(name.clone());
        }
    }
    if jobs.is_empty() {
        return Ok(if missing.is_empty() {
            SuccessTuple::ok("No jobs to delete; nothing to do.")
        } else {
            SuccessTuple::fail(format!("Jobs not found: {}.", items_str(&missing)))
        });
    }
    let mut delete_all = false;
    if action.is_empty() && !force {
        pprint_jobs(rt, &mut jobs, false);
        if !yes_no(
            rt,
            "Delete all jobs? This cannot be undone!",
            "n",
            yes,
            noask,
        )? {
            return Ok(SuccessTuple::fail("No jobs were deleted."));
        }
        delete_all = true;
    }
    let running = with_status(&mut jobs, Status::Running);
    let paused = with_status(&mut jobs, Status::Paused);
    let mut to_delete = with_status(&mut jobs, Status::Stopped);
    let to_stop: Vec<String> = running.iter().chain(&paused).cloned().collect();
    if !to_stop.is_empty() {
        crate::shell::clear_screen(rt, flag(args, "debug"));
        if !force {
            let mut sub = subset(&ctx, &jobs, &to_stop);
            pprint_jobs(rt, &mut sub, nopretty);
        }
        if force || yes_no(rt, "Stop these jobs?", "n", yes, noask)? {
            let mut stop_args = args.clone();
            stop_args.insert("action".into(), PyValue::str_list(&to_stop));
            let _ = stop_jobs(&stop_args, rt)?;
            let mut check = subset(&ctx, &jobs, &to_stop);
            if !with_status(&mut check, Status::Running).is_empty() {
                return Ok(SuccessTuple::fail(
                    "Failed to kill running jobs. Please stop these jobs before deleting.",
                ));
            }
            to_delete.extend(to_stop);
        }
    }
    if to_delete.is_empty() {
        return Ok(SuccessTuple::fail("No jobs to delete."));
    }
    if !force && !delete_all {
        let mut sub = subset(&ctx, &jobs, &to_delete);
        pprint_jobs(rt, &mut sub, nopretty);
        if !yes_no(
            rt,
            "Are you sure you want to delete these jobs?",
            "n",
            yes,
            noask,
        )? {
            return Ok(SuccessTuple::fail("No jobs were deleted."));
        }
    }
    let mut deleted = Vec::new();
    for n in &to_delete {
        let res = match jobs.get_mut(n) {
            Some(j) => {
                let res = j.delete();
                warn_custom_executor_failure(rt, j, &res);
                res
            }
            None => SuccessTuple::fail(format!("Job '{n}' does not exist.")),
        };
        if !res.success {
            warn(rt, &format!("Failed to delete job '{n}'."));
            continue;
        }
        deleted.push(n.clone());
    }
    let mut message = format!(
        "Deleted job{} {}.",
        plural(deleted.len()),
        items_str(&deleted)
    );
    if !missing.is_empty() {
        message.push_str(&format!(" Jobs not found: {}.", items_str(&missing)));
    }
    Ok(SuccessTuple {
        success: missing.is_empty() && deleted.len() == to_delete.len(),
        message,
    })
}
