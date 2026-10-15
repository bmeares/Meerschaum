//! The job actions over `meerschaum-jobs`: `start jobs` (`actions/start.py`), `stop jobs`
//! (`actions/stop.py`), `pause jobs` (`actions/pause.py`), `restart jobs`
//! (`actions/restart.py`), `attach jobs|logs` (`actions/attach.py`), `delete jobs`
//! (`actions/delete.py`), `show jobs` / `show logs` (`actions/show.py`), `pprint_jobs`
//! (`utils/formatting/_jobs.py`), the `--schedule` loop (`utils/schedule.py`) and the process-wide
//! stop flag (`utils/threading.py` `stop_requested`).

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_core::static_config;
use meerschaum_core::value::Value;
use meerschaum_jobs::config::{JobPaths, JobsConfig};
use meerschaum_jobs::executor::get_filtered_jobs;
use meerschaum_jobs::scheduler::RootScheduler;
use meerschaum_jobs::{
    Job, JobOptions, JobsContext, Launcher, MonitorControl, MonitorOptions, Status,
};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::format::rich::console::{Group, Justify, Renderable};
use crate::format::rich::table::{ASCII, Column, ROUNDED, Table};
use crate::format::rich::text::Text;
use crate::value::{ArgsMap, PyValue};

use super::{
    flag, info, items_str, items_str_with, plural, print_tuple, string, warn, words, yes_no,
};

static STOP: OnceLock<Arc<AtomicBool>> = OnceLock::new();
static SIGNALS: AtomicUsize = AtomicUsize::new(0);
static HANDLER: OnceLock<()> = OnceLock::new();

/// `stop_requested()`: set when the process receives SIGINT / SIGTERM (see
/// [`install_stop_handler`]).
pub fn stop_requested_flag() -> Arc<AtomicBool> {
    STOP.get_or_init(|| Arc::new(AtomicBool::new(false)))
        .clone()
}

/// Begin a new interactive command after any previous command has finished stopping.
pub fn reset_stop_request() {
    SIGNALS.store(0, Ordering::SeqCst);
    stop_requested_flag().store(false, Ordering::SeqCst);
}

/// Turn the first SIGINT / SIGTERM into a stop request for long-running actions (laps,
/// schedules, following logs); a second signal exits immediately (v4 raises `KeyboardInterrupt`
/// at the next checkpoint).
pub fn install_stop_handler(session: &super::Session) {
    if HANDLER.set(()).is_err() {
        return;
    }
    let flag = stop_requested_flag();
    session.spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let (Ok(mut int), Ok(mut term)) = (
                signal(SignalKind::interrupt()),
                signal(SignalKind::terminate()),
            ) else {
                return;
            };
            loop {
                tokio::select! {
                    _ = int.recv() => {},
                    _ = term.recv() => {},
                }
                flag.store(true, Ordering::SeqCst);
                if SIGNALS.fetch_add(1, Ordering::SeqCst) >= 1 {
                    std::process::exit(130);
                }
            }
        }
        #[cfg(not(unix))]
        {
            loop {
                if tokio::signal::ctrl_c().await.is_err() {
                    return;
                }
                flag.store(true, Ordering::SeqCst);
                if SIGNALS.fetch_add(1, Ordering::SeqCst) >= 1 {
                    std::process::exit(130);
                }
            }
        }
    });
}

/// Sleep up to `seconds`, waking early on a stop request. Returns whether a stop was requested.
pub fn sleep_unless_stopped(seconds: f64) -> bool {
    let stop = stop_requested_flag();
    let deadline = Instant::now() + Duration::from_secs_f64(seconds.max(0.0));
    while Instant::now() < deadline {
        if stop.load(Ordering::SeqCst) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50).min(deadline - Instant::now()));
    }
    stop.load(Ordering::SeqCst)
}

/// The jobs context for the runtime's config registry.
pub fn jobs_context(rt: &mut Runtime<'_>) -> JobsContext {
    let config = rt.config();
    JobsContext {
        paths: JobPaths::from_config(config),
        config: JobsConfig::from_config(config),
        launcher: Launcher::from_env(),
    }
}

fn executor_keys(args: &ArgsMap) -> Option<String> {
    string(args, "executor_keys")
}

/// `get_filtered_jobs(executor_keys, action, warn=...)`.
fn filtered_jobs(
    rt: &mut Runtime<'_>,
    ctx: &JobsContext,
    filter: &[String],
    warn_missing: bool,
    include_hidden: bool,
) -> IndexMap<String, Job> {
    let (jobs, warnings) = get_filtered_jobs(ctx, filter, include_hidden);
    if warn_missing {
        for w in warnings {
            warn(rt, &w);
        }
    }
    jobs
}

fn with_status(jobs: &mut IndexMap<String, Job>, status: Status) -> Vec<String> {
    jobs.iter_mut()
        .filter_map(|(n, j)| (j.status() == status).then(|| n.clone()))
        .collect()
}

fn subset(ctx: &JobsContext, names: &[String]) -> IndexMap<String, Job> {
    names
        .iter()
        .filter_map(|n| Job::open(n, ctx).ok().map(|j| (n.clone(), j)))
        .collect()
}

/// `pprint_job(job, nopretty)`.
fn pprint_job(rt: &mut Runtime<'_>, job: &mut Job, nopretty: bool) {
    if !nopretty {
        info(rt, &format!("Command for job '{}':", job.name));
        let label = job.label();
        rt.print(&format!("\n{label}\n\n"));
    } else {
        rt.print(&format!("{}\n", job.name));
    }
}

/// `pprint_jobs(jobs, nopretty)`.
pub fn pprint_jobs(rt: &mut Runtime<'_>, jobs: &mut IndexMap<String, Job>, nopretty: bool) {
    let running = with_status(jobs, Status::Running);
    let paused = with_status(jobs, Status::Paused);
    let stopped = with_status(jobs, Status::Stopped);
    if nopretty {
        for (names, title) in [
            (&running, "Running jobs"),
            (&paused, "Paused jobs"),
            (&stopped, "Stopped jobs"),
        ] {
            if names.is_empty() {
                continue;
            }
            let _ = title;
            for n in names {
                if let Some(j) = jobs.get_mut(n) {
                    pprint_job(rt, j, true);
                }
            }
        }
        return;
    }
    let mut executors: Vec<String> = Vec::new();
    for j in jobs.values() {
        let k = j.executor_keys.replace("systemd:main", "systemd");
        if !executors.contains(&k) {
            executors.push(k);
        }
    }
    if executors.is_empty() {
        executors.push("local".into());
    }
    let executor_style = rt
        .config()
        .get(&["shell", "ansi", "executor", "rich", "style"])
        .map(|v| v.py_str())
        .unwrap_or_default();

    struct Row {
        name: String,
        executor: String,
        label: String,
        status: Vec<(String, &'static str)>,
        result: Option<SuccessTuple>,
    }
    let mut rows: Vec<Row> = Vec::new();
    for (names, kind) in [(&running, 0), (&paused, 1), (&stopped, 2)] {
        for n in names {
            let Some(job) = jobs.get_mut(n) else { continue };
            let mut status: Vec<(String, &'static str)> = Vec::new();
            let mut result = None;
            match kind {
                0 => {
                    if job.is_blocking_on_stdin() {
                        status.push(("waiting for input".into(), "yellow"));
                    } else {
                        status.push((job.status().as_str().to_string(), "green"));
                    }
                    if let Some(pid) = job.pid() {
                        status.push((format!("PID: {pid}"), ""));
                    }
                    if job.restarts() {
                        status.push(("(restarts)".into(), ""));
                    }
                }
                1 => {
                    status.push((job.status().as_str().to_string(), "yellow"));
                    if job.restarts() {
                        status.push(("(restarts)".into(), ""));
                    }
                }
                _ => {
                    status.push((job.status().as_str().to_string(), "red"));
                    if job.restarts() {
                        if job.stop_time().is_none() {
                            status.push(("(restarts)".into(), ""));
                        } else {
                            status.push(("(start to resume restarts)".into(), ""));
                        }
                    }
                    result = Some(job.result());
                }
            }
            rows.push(Row {
                name: job.name.clone(),
                executor: job.executor_keys.replace("systemd:main", "systemd"),
                label: job.label(),
                status,
                result,
            });
        }
    }

    let text = rt.with_formatter(|f| {
        let ansi = f.settings.ansi;
        let st = |s: &str| if ansi { s.to_string() } else { String::new() };
        let mut table = Table::new();
        table.title = Some(Text::plain_text(&format!(
            "\nJobs on Executor{} {}",
            plural(executors.len()),
            items_str(&executors)
        )));
        table.box_chars = Some(if f.settings.unicode { ROUNDED } else { ASCII });
        table.show_lines = true;
        table.show_header = ansi;
        table.add_column(Column {
            header: Text::plain_text("Name"),
            justify: Justify::Right,
            style: st("magenta").as_str().into(),
            ..Column::default()
        });
        table.add_column(Column {
            header: Text::plain_text("Executor"),
            style: st(&executor_style).as_str().into(),
            ..Column::default()
        });
        table.add_column(Column {
            header: Text::plain_text("Command"),
            ..Column::default()
        });
        table.add_column(Column {
            header: Text::plain_text("Status"),
            ..Column::default()
        });
        for row in &rows {
            let mut group: Vec<Box<dyn Renderable>> = row
                .status
                .iter()
                .map(|(t, s)| Box::new(Text::new(t, st(s).as_str())) as Box<dyn Renderable>)
                .collect();
            if let Some(tup) = &row.result {
                let msg = tup
                    .message
                    .split('\n')
                    .map(str::trim)
                    .collect::<Vec<_>>()
                    .join("\n");
                let formatted = f.format_success_tuple(
                    &SuccessTuple {
                        success: tup.success,
                        message: msg,
                    },
                    0,
                    0,
                    0,
                    false,
                );
                let t = Text::plain_text("\n").plus(&Text::from_ansi(&formatted, ""));
                group.push(Box::new(t));
            }
            table.add_row(vec![
                Some(
                    Box::new(crate::format::rich::console::render_str(&row.name, ""))
                        as Box<dyn Renderable>,
                ),
                Some(Box::new(crate::format::rich::console::render_str(
                    &row.executor,
                    "",
                ))),
                Some(Box::new(crate::format::rich::console::render_str(
                    &row.label, "",
                ))),
                Some(Box::new(Group { renderables: group })),
            ]);
        }
        f.console().print(&table)
    });
    rt.print(&text);
}

/// `show jobs`.
pub fn show_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let action = words(args, "action");
    let ctx = jobs_context(rt);
    let mut jobs = filtered_jobs(rt, &ctx, &action, false, false);
    if jobs.is_empty() {
        if action.is_empty() && !nopretty {
            info(rt, "No running or stopped jobs.");
            rt.print(
                "    You can start a background job with `-d` or `--daemon`,\n    or run the command `start job` before action commands.\n\n    Examples:\n      - start api -d\n      - start job sync pipes --loop\n",
            );
        }
        return Ok(SuccessTuple::ok("No jobs to show."));
    }
    pprint_jobs(rt, &mut jobs, nopretty);
    Ok(SuccessTuple::ok("Success"))
}

/// Whether a word names a v4 action (`actions` keys).
fn is_action_name(word: &str) -> bool {
    crate::actions::CORE_ACTIONS.iter().any(|a| a.name == word)
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
    let mut jobs = filtered_jobs(rt, &ctx, &action, false, false);

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
        let mut filtered = filtered_jobs(rt, &ctx, &names, false, false);
        if !filtered.is_empty() {
            let running = with_status(&mut filtered, Status::Running);
            if !running.is_empty() {
                let mut running_jobs = subset(&ctx, &running);
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

    let compose_var = static_config().environment.compose_project.clone();
    let compose_project = (rt.env)(&compose_var).filter(|v| !v.is_empty());
    let (mut successes, mut failures) = (Vec::new(), Vec::new());
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
                compose_project
                    .as_ref()
                    .map(|p| (compose_var.clone(), p.clone())),
                (force, yes, noask),
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
    compose_env: Option<(String, String)>,
    (force, yes, noask): (bool, bool, bool),
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
    let env = compose_env.map(|(k, v)| {
        let mut m = IndexMap::new();
        m.insert(k, v);
        m
    });
    let opts = JobOptions {
        env,
        executor_keys,
        delete_after_completion: rm,
        ..Default::default()
    };
    let mut job = match Job::from_sysargs_str(name, &job_sysargs, opts, ctx) {
        Ok(j) => j,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
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
    job.start()
}

/// `stop jobs`.
pub fn stop_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let (force, yes, noask) = (flag(args, "force"), flag(args, "yes"), flag(args, "noask"));
    let timeout = timeout_seconds(args);
    let ctx = jobs_context(rt);
    let mut jobs = filtered_jobs(rt, &ctx, &action, !nopretty, false);
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
        let mut sub = subset(&ctx, &to_stop);
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
    let mut jobs = filtered_jobs(rt, &ctx, &action, !nopretty, false);
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
        let mut sub = subset(&ctx, &running);
        pprint_jobs(rt, &mut sub, false);
        if !yes_no(rt, "Pause all running jobs?", "n", yes, noask)? {
            return Ok(SuccessTuple::fail("No jobs were paused."));
        }
    }
    let (mut successes, mut fails) = (Vec::new(), Vec::new());
    for n in &to_pause {
        let Some(job) = jobs.get_mut(n) else { continue };
        if job.pause(None).success {
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
    if do_loop {
        install_stop_handler(&super::session(rt));
    }
    loop {
        let jobs = filtered_jobs(rt, &ctx, &action, false, true);
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

/// `delete jobs`.
pub fn delete_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let (force, yes, noask) = (flag(args, "force"), flag(args, "yes"), flag(args, "noask"));
    let ctx = jobs_context(rt);
    let mut jobs = filtered_jobs(rt, &ctx, &action, false, false);
    if jobs.is_empty() {
        return Ok(SuccessTuple::ok("No jobs to delete; nothing to do."));
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
            let mut sub = subset(&ctx, &to_stop);
            pprint_jobs(rt, &mut sub, nopretty);
        }
        if force || yes_no(rt, "Stop these jobs?", "n", yes, noask)? {
            let mut stop_args = args.clone();
            stop_args.insert("action".into(), PyValue::str_list(&to_stop));
            let _ = stop_jobs(&stop_args, rt)?;
            let mut check = subset(&ctx, &to_stop);
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
        let mut sub = subset(&ctx, &to_delete);
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
        let res = match Job::open(n, &ctx) {
            Ok(mut j) => j.delete(),
            Err(e) => SuccessTuple::fail(e.to_string()),
        };
        if !res.success {
            warn(rt, &format!("Failed to delete job '{n}'."));
            continue;
        }
        deleted.push(n.clone());
    }
    Ok(SuccessTuple {
        success: !deleted.is_empty(),
        message: format!(
            "Deleted job{} {}.",
            plural(deleted.len()),
            items_str(&deleted)
        ),
    })
}

/// `attach jobs`.
pub fn attach_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let name = string(args, "name");
    if action.is_empty() && name.is_none() {
        return Ok(SuccessTuple::fail(
            "Provide the name of the job to attach to.",
        ));
    }
    let name = name.unwrap_or_else(|| action[0].clone());
    let ctx = jobs_context(rt);
    let mut job = Job::new(
        &name,
        None,
        JobOptions {
            executor_keys: executor_keys(args),
            ..Default::default()
        },
        &ctx,
    )
    .map_err(|e| ActionError::new("ValueError", e.to_string()))?;
    if !job.exists() {
        return Ok(SuccessTuple::fail(format!(
            "Job '{}' does not exist.",
            job.name
        )));
    }
    install_stop_handler(&super::session(rt));
    let result = std::cell::RefCell::new(SuccessTuple::ok("Success"));
    let out = std::cell::RefCell::new(&mut *rt.out);
    let stop = stop_requested_flag();
    let opts = MonitorOptions {
        stop_on_exit: true,
        strip_timestamps: true,
        accept_input: true,
        stop: Some(stop),
        input: Some(Box::new(|| {
            let mut line = String::new();
            match std::io::stdin().read_line(&mut line) {
                Ok(0) | Err(_) => None,
                Ok(_) => Some(line),
            }
        })),
        on_stop: Some(Box::new(|r: &SuccessTuple| {
            *result.borrow_mut() = r.clone();
        })),
        ..Default::default()
    };
    let _ = job.monitor_logs(
        |line: &str| {
            let mut o = out.borrow_mut();
            let _ = o.write_all(line.as_bytes());
            let _ = o.flush();
            MonitorControl::Continue
        },
        opts,
    );
    Ok(result.into_inner())
}

/// `datetime.strptime(prefix, format)` for the log timestamp formats (`%Y-%m-%d %H:%M` etc.).
fn parse_line_timestamp(prefix: &str, fmt: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(prefix, fmt)
        .ok()
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(prefix, fmt)
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
        })
}

/// `show logs` / `attach logs`.
pub fn show_logs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    let ctx = jobs_context(rt);
    let mut jobs = filtered_jobs(rt, &ctx, &action, false, false);
    if nopretty {
        for (name, job) in jobs.iter_mut() {
            let logs = job.get_logs().unwrap_or_else(|| "None".into());
            rt.print(&format!("\n-*-\nMRSM_JOB: {name}\n-*-\n{logs}\n"));
        }
        return Ok(SuccessTuple::ok("Success"));
    }

    let colors: Vec<String> = rt
        .config()
        .get(&["jobs", "logs", "colors"])
        .and_then(|v| v.as_list().map(|l| l.iter().map(Value::py_str).collect()))
        .filter(|l: &Vec<String>| !l.is_empty())
        .unwrap_or_else(|| vec!["cyan".into()]);
    let min_buffer_len = rt
        .config()
        .get(&["jobs", "logs", "min_buffer_len"])
        .and_then(|v| v.as_i64())
        .unwrap_or(10) as usize;
    let ts_fmt = ctx.config.timestamp_format.clone();
    let follow_fmt = ctx.config.follow_timestamp_format.clone();
    let ansi = rt.settings().ansi;
    if ansi {
        info(rt, "Watching logs...");
    } else {
        rt.print("Watching logs...\n");
    }

    install_stop_handler(&super::session(rt));
    let stop = stop_requested_flag();
    let (tx, rx) = std::sync::mpsc::channel::<(String, String)>();
    let mut tasks: IndexMap<String, (Arc<AtomicBool>, std::thread::JoinHandle<()>)> =
        IndexMap::new();
    let now_len = chrono::Utc::now()
        .format(&ts_fmt)
        .to_string()
        .chars()
        .count();
    let mut previous_ts: Option<chrono::NaiveDateTime> = None;
    let mut last_refresh = Instant::now() - Duration::from_secs(2);
    let mut names: Vec<String> = jobs.keys().cloned().collect();
    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        if last_refresh.elapsed() >= Duration::from_secs(1) {
            last_refresh = Instant::now();
            let current = filtered_jobs(rt, &ctx, &action, false, false);
            names = current.keys().cloned().collect();
            for name in &names {
                if tasks.contains_key(name) {
                    continue;
                }
                let job_stop = Arc::new(AtomicBool::new(false));
                let (tx, ctx, n, js) = (tx.clone(), ctx.clone(), name.clone(), job_stop.clone());
                let handle = std::thread::spawn(move || {
                    let Ok(mut job) = Job::open(&n, &ctx) else {
                        return;
                    };
                    let _ = job.monitor_logs(
                        |line: &str| {
                            if tx.send((n.clone(), line.to_string())).is_err() {
                                return MonitorControl::Stop;
                            }
                            MonitorControl::Continue
                        },
                        MonitorOptions {
                            accept_input: false,
                            stop_on_exit: false,
                            stop: Some(js),
                            ..Default::default()
                        },
                    );
                });
                tasks.insert(name.clone(), (job_stop, handle));
            }
            let gone: Vec<String> = tasks
                .keys()
                .filter(|n| !names.contains(n))
                .cloned()
                .collect();
            for n in gone {
                if let Some((s, h)) = tasks.shift_remove(&n) {
                    s.store(true, Ordering::SeqCst);
                    let _ = h.join();
                }
            }
        }
        let Ok((name, line)) = rx.recv_timeout(Duration::from_millis(100)) else {
            continue;
        };
        let prefix: String = line.chars().take(now_len).collect();
        let (line_ts, rest) = match parse_line_timestamp(&prefix, &ts_fmt) {
            Some(ts) => {
                previous_ts = Some(ts);
                (Some(ts), line.chars().skip(now_len + 3).collect::<String>())
            }
            None => (previous_ts, line.clone()),
        };
        if rest.is_empty() || rest == "\n" {
            continue;
        }
        let max_len_id = names.iter().map(|n| n.chars().count()).max().unwrap_or(0) + 1;
        let buffer_len = min_buffer_len.max(max_len_id);
        let spaces = " ".repeat(buffer_len.saturating_sub(name.chars().count()));
        let spaces = if spaces.is_empty() {
            " ".into()
        } else {
            spaces
        };
        let color = names
            .iter()
            .position(|n| *n == name)
            .map(|i| colors[i % colors.len()].clone())
            .unwrap_or_else(|| colors[0].clone());
        let line_prefix = format!(
            "{spaces}{} | ",
            line_ts
                .map(|t| t.format(&follow_fmt).to_string())
                .unwrap_or_default()
        );
        let body = rest.strip_suffix('\n').unwrap_or(&rest);
        let text = rt.with_formatter(|f| {
            let mut t = Text::plain_text(&name);
            t.append_str(&format!("{line_prefix}{body}"), "");
            if f.settings.ansi {
                let end = name.chars().count() + line_prefix.chars().count();
                t.stylize(color.as_str(), 0, Some(end));
            }
            f.console().print_text(&t)
        });
        rt.print(&text);
    }
    for (_, (s, h)) in tasks {
        s.store(true, Ordering::SeqCst);
        let _ = h.join();
    }
    let _ = ansi;
    Ok(SuccessTuple::ok("Success"))
}

/// `schedule_function(do_action, schedule)`: run `action` on the schedule until it finishes or
/// a stop is requested. Returns `Err` with the schedule error message.
pub fn run_schedule(
    rt: &mut Runtime<'_>,
    schedule: &str,
    mut action: impl FnMut(&mut Runtime<'_>) -> SuccessTuple,
) -> SuccessTuple {
    let now = chrono::Utc::now().fixed_offset();
    let mut trigger = match meerschaum_jobs::parse_schedule(schedule, now) {
        Ok(t) => t,
        Err(e) => return SuccessTuple::fail(e.py_message()),
    };
    install_stop_handler(&super::session(rt));
    let stop = stop_requested_flag();
    let start = Instant::now();
    // A job process (`MRSM_DAEMON_ID` is the job name) keeps its schedule per root: a restart
    // resumes the recorded next fire time instead of re-anchoring the cadence. Foreground runs
    // have no job name and keep nothing.
    let job = (rt.env)(&meerschaum_core::static_config().environment.daemon_id)
        .filter(|name| !name.is_empty());
    let scheduler = job
        .as_ref()
        .map(|_| RootScheduler::for_root(&rt.config().paths().root_dir));
    let mut pending = match (&scheduler, &job) {
        (Some(scheduler), Some(job)) => scheduler
            .next_fire(job, schedule, now)
            .ok()
            .flatten()
            .filter(|next| {
                scheduler
                    .state(job)
                    .is_some_and(|s| s.next_fire == Some(*next))
            }),
        _ => None,
    };
    let mut finished = false;
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
        if sleep_unless_stopped(wait) {
            break;
        }
        let _ = action(rt);
        if !finished && pending.is_none() {
            pending = trigger.next();
        }
        if let (Some(scheduler), Some(job)) = (&scheduler, &job) {
            let fired = chrono::Utc::now().fixed_offset();
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
