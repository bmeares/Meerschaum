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
use meerschaum_jobs::executor::get_filtered_jobs_for;
use meerschaum_jobs::scheduler::RootScheduler;
use meerschaum_jobs::{
    Job, JobOptions, JobsContext, Launcher, MonitorControl, MonitorOptions, StartKind, Status,
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
static HANDLER: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

static EMBEDDED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Whether [`install_stop_handler`] turned SIGINT / SIGTERM into stop requests in this process.
pub fn stop_handler_installed() -> bool {
    HANDLER.load(Ordering::SeqCst) || crate::shell::interrupt::watching()
}

static OWN_SIGNALS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Whether this process embeds the dispatcher (the Python extension, outside `mrsm`'s own run).
pub fn is_embedded() -> bool {
    EMBEDDED.load(Ordering::SeqCst)
}

/// The running action handles SIGINT / SIGTERM itself (the servers: `start api` / `start mcp`
/// wait on them for a graceful shutdown), so nothing else may take them, the progress spinner's
/// guard included.
pub fn own_signals() {
    OWN_SIGNALS.store(true, Ordering::SeqCst);
}

/// Whether the running action handles the signals itself (see [`own_signals`]).
pub fn signals_owned() -> bool {
    OWN_SIGNALS.load(Ordering::SeqCst)
}

/// Give the signals back after [`own_signals`], for an action that owns them only while it
/// runs (`upgrade postgres` and friends: a shell goes on afterwards).
pub fn disown_signals() {
    OWN_SIGNALS.store(false, Ordering::SeqCst);
}

/// Mark this process as embedding the dispatcher (the Python extension). The process-wide
/// SIGINT / SIGTERM handler of [`install_stop_handler`], which ends the process on a second
/// signal, belongs to the `mrsm` binary: an embedding host is never given one, and sets
/// [`stop_requested_flag`] for the call it runs instead.
pub fn set_embedded(embedded: bool) {
    EMBEDDED.store(embedded, Ordering::SeqCst);
}

/// `stop_requested()`: set when the process receives SIGINT / SIGTERM (see
/// [`install_stop_handler`]).
///
/// An embedding host runs each call under [`with_call_stop`]: the loops of that call (which read
/// this on the thread that runs it) then see the call's own flag, so stopping one call neither
/// reaches nor is reset by another. Without one (the `mrsm` binary) it is the process-wide flag.
pub fn stop_requested_flag() -> Arc<AtomicBool> {
    CALL_STOP
        .with(|slot| slot.borrow().clone())
        .unwrap_or_else(process_stop_flag)
}

fn process_stop_flag() -> Arc<AtomicBool> {
    STOP.get_or_init(|| Arc::new(AtomicBool::new(false)))
        .clone()
}

thread_local! {
    static CALL_STOP: std::cell::RefCell<Option<Arc<AtomicBool>>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `f` with `flag` as this thread's stop flag (see [`stop_requested_flag`]).
pub fn with_call_stop<R>(flag: Arc<AtomicBool>, f: impl FnOnce() -> R) -> R {
    let previous = CALL_STOP.with(|slot| slot.borrow_mut().replace(flag));
    let result = f();
    CALL_STOP.with(|slot| *slot.borrow_mut() = previous);
    result
}

/// Forget that [`install_stop_handler`] ran, and any signal it counted. The handler is a task of the
/// session's runtime, which ends with the session: a host which runs the CLI more than once in
/// one process (the Python extension's `_native.main`) calls this after each run so the next run
/// installs its own.
pub fn release_stop_handler() {
    HANDLER.store(false, Ordering::SeqCst);
    reset_stop_request();
}

/// Begin a new interactive command after any previous command has finished stopping.
pub fn reset_stop_request() {
    SIGNALS.store(0, Ordering::SeqCst);
    process_stop_flag().store(false, Ordering::SeqCst);
}

/// Turn the first SIGINT / SIGTERM into a stop request for long-running actions (laps,
/// schedules, following logs); a second signal exits immediately (v4 raises `KeyboardInterrupt`
/// at the next checkpoint). In a shell the second signal cancels the action instead of ending
/// the shell.
///
/// The signals are handled by the process-wide watcher of [`crate::shell::interrupt`]: this marks
/// the running action as one that reads [`stop_requested_flag`] itself, so that the first Ctrl-C
/// sets the flag and drops nothing (an action which never calls this is cancelled outright by the
/// first Ctrl-C).
pub fn install_stop_handler(session: &super::Session) {
    // An embedding host (the Python extension) owns the process's signals: it polls them
    // itself and sets the stop flag for the call it runs (see [`set_embedded`]).
    if EMBEDDED.load(Ordering::SeqCst) {
        return;
    }
    HANDLER.store(true, Ordering::SeqCst);
    crate::shell::interrupt::install(Some(session));
    crate::shell::interrupt::mark_cooperative();
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
///
/// Besides the paths and configuration it installs the resolver for `api:<label>` executors
/// ([`super::remote_jobs::CliRemoteResolver`], over the session's runtime): the jobs crate owns
/// the client trait and the CLI injects the `APIConnector` implementation.
pub fn jobs_context(rt: &mut Runtime<'_>) -> JobsContext {
    let session = super::session(rt);
    let config = rt.config();
    // `<type>:<label>` executors of native plugins (none: the context resolves no other types).
    let natives = session.native_plugins(config).ok();
    let resolver = super::remote_jobs::CliRemoteResolver::new(
        session.clone(),
        super::remote_jobs::CliRemoteResolver::section(config),
    );
    let context = JobsContext::new(
        JobPaths::from_config(config),
        JobsConfig::from_config(config),
        Launcher::from_env(),
    )
    .with_remote(Arc::new(resolver));
    match natives.filter(|n| !n.executor_types().is_empty()) {
        // The executors' host calls (`host::plugin_config()`, ...) use the session's connectors.
        Some(natives) => context.with_executors(Arc::new(
            crate::native_executors::NativeExecutors::new(natives, config)
                .with_host(session, config),
        )),
        None => context,
    }
}

/// A failed operation on a job of a native plugin's executor shows the executor's reason
/// (``"`pause_job` is not supported by the 'procs:main' executor."``); the built-in executors
/// keep v4's summary-only output.
fn warn_custom_executor_failure(rt: &mut Runtime<'_>, job: &Job, result: &SuccessTuple) {
    let kind = job.executor_keys.split(':').next().unwrap_or_default();
    if !result.success && !matches!(kind, "local" | "systemd" | "api" | "None" | "") {
        warn(rt, &result.message);
    }
}

fn executor_keys(args: &ArgsMap) -> Option<String> {
    string(args, "executor_keys")
}

/// `get_filtered_jobs(executor_keys, action, warn=..., include_hidden=...)`: the named jobs of
/// the executor (`None`: the context's, combining `local` and `systemd` on a systemd host).
///
/// A failed listing (an unreachable or unauthorized API, a broken `systemctl`) is an `Err` with
/// the failed `SuccessTuple` for the action to return: it is never read as "no jobs".
fn filtered_jobs(
    rt: &mut Runtime<'_>,
    ctx: &JobsContext,
    exec_keys: Option<&str>,
    filter: &[String],
    warn_missing: bool,
    include_hidden: bool,
) -> Result<IndexMap<String, Job>, SuccessTuple> {
    filtered_jobs_with(
        rt,
        ctx,
        exec_keys,
        filter,
        warn_missing,
        include_hidden,
        true,
    )
}

fn filtered_jobs_with(
    rt: &mut Runtime<'_>,
    ctx: &JobsContext,
    exec_keys: Option<&str>,
    filter: &[String],
    warn_missing: bool,
    include_hidden: bool,
    combine_local_and_systemd: bool,
) -> Result<IndexMap<String, Job>, SuccessTuple> {
    let filtered = get_filtered_jobs_for(
        ctx,
        exec_keys,
        filter,
        include_hidden,
        combine_local_and_systemd,
    )
    .map_err(|e| {
        let keys = meerschaum_jobs::executor::normalize_executor_keys(exec_keys)
            .unwrap_or_else(|| meerschaum_jobs::job::get_executor_keys_from_context(ctx));
        SuccessTuple::fail(format!("Could not get the jobs of executor '{keys}': {e}"))
    })?;
    for w in &filtered.listing_warnings {
        warn(rt, w);
    }
    if warn_missing {
        for w in &filtered.missing {
            warn(rt, w);
        }
    }
    Ok(filtered.jobs)
}

fn with_status(jobs: &mut IndexMap<String, Job>, status: Status) -> Vec<String> {
    jobs.iter_mut()
        .filter_map(|(n, j)| (j.status() == status).then(|| n.clone()))
        .collect()
}

/// Jobs of the same executors as `jobs`, freshly opened by name (a `Job` is not `Clone`): for
/// printing a subset and for re-reading statuses after an action.
fn subset(
    ctx: &JobsContext,
    jobs: &IndexMap<String, Job>,
    names: &[String],
) -> IndexMap<String, Job> {
    names
        .iter()
        .filter_map(|n| {
            let keys = jobs.get(n).map(|j| j.executor_keys.clone())?;
            Job::new(
                n,
                None,
                JobOptions {
                    executor_keys: Some(keys),
                    ..Default::default()
                },
                ctx,
            )
            .ok()
            .map(|j| (n.clone(), j))
        })
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

/// The `show jobs` status line of a running job's health: `last good sync 2 min ago`,
/// `stalled: last good sync 3 h ago`, `failing: last 2 laps failed`; nothing while unknown.
pub fn health_line(health: &meerschaum_jobs::health::JobHealth) -> Option<(String, &'static str)> {
    use meerschaum_jobs::health::HealthState;
    match health.state {
        HealthState::Healthy => Some((health.message.clone(), "")),
        HealthState::Stalled => Some((format!("stalled: {}", health.message), "yellow")),
        HealthState::Failing => Some((format!("failing: {}", health.message), "red")),
        HealthState::Stopped | HealthState::Unknown => None,
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
                    // Job health: how its laps go (nothing for a job that does not lap).
                    if let Some(line) = health_line(&job.health()) {
                        status.push(line);
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
    let exec_keys = executor_keys(args);
    let progress = crate::format::progress::Progress::start(rt, args, "Reading jobs");
    let jobs = filtered_jobs(rt, &ctx, exec_keys.as_deref(), &action, false, false);
    drop(progress);
    let mut jobs = match jobs {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
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
        // Meerschaum before rc15 ran the jobs of every root but the default one locally; on a
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
    match job.try_exists() {
        Err(e) => return Ok(SuccessTuple::fail(e)),
        Ok(false) => {
            return Ok(SuccessTuple::fail(format!(
                "Job '{}' does not exist.",
                job.name
            )));
        }
        Ok(true) => {}
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
    let exec_keys = executor_keys(args);
    let mut jobs = match filtered_jobs(rt, &ctx, exec_keys.as_deref(), &action, false, false) {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
    // A job's log keeps its styles; printed off a terminal (or under `NO_COLOR`) it is plain.
    let plain = {
        let settings = rt.settings();
        !settings.ansi || settings.no_color
    };
    if nopretty {
        for (name, job) in jobs.iter_mut() {
            let logs = match job.try_get_logs() {
                Ok(logs) => logs.unwrap_or_else(|| "None".into()),
                Err(e) => return Ok(SuccessTuple::fail(e)),
            };
            let logs = if plain {
                crate::format::remove_ansi(&logs)
            } else {
                logs
            };
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
    let mut job_keys: IndexMap<String, String> = IndexMap::new();
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
            let current = match filtered_jobs(rt, &ctx, exec_keys.as_deref(), &action, false, false)
            {
                Ok(jobs) => jobs,
                // An unreachable endpoint ends the follower with its error.
                Err(failed) => {
                    for (_, (s, h)) in tasks {
                        s.store(true, Ordering::SeqCst);
                        let _ = h.join();
                    }
                    return Ok(failed);
                }
            };
            names = current.keys().cloned().collect();
            for (n, j) in &current {
                job_keys.insert(n.clone(), j.executor_keys.clone());
            }
            for name in &names {
                if tasks.contains_key(name) {
                    continue;
                }
                let job_stop = Arc::new(AtomicBool::new(false));
                let keys = job_keys.get(name).cloned();
                let (tx, ctx, n, js) = (tx.clone(), ctx.clone(), name.clone(), job_stop.clone());
                let handle = std::thread::spawn(move || {
                    let Ok(mut job) = Job::new(
                        &n,
                        None,
                        JobOptions {
                            executor_keys: keys,
                            ..Default::default()
                        },
                        &ctx,
                    ) else {
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
        let body = if plain {
            std::borrow::Cow::Owned(crate::format::remove_ansi(body))
        } else {
            std::borrow::Cow::Borrowed(body)
        };
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
    action: impl FnMut(&mut Runtime<'_>) -> SuccessTuple,
) -> SuccessTuple {
    run_schedule_jittered(rt, schedule, None, "run", action)
}

/// [`run_schedule`] with the schedule's jitter (v5, D23): every fire time moves by a FIXED offset
/// in `[0, jitter]` seconds, seeded by the job's name (the schedule text outside a job), so a
/// job keeps its offset across fires and restarts and jobs with one cadence spread out. The
/// bound is the schedule's own `jitter <n> <unit>` suffix, else `jitter` (`--jitter`), else
/// `jobs.schedule.jitter_seconds` (default 0: no jitter). Each run is a lap of the job's health
/// (`noun`: what a run does, `sync` or `run`).
pub fn run_schedule_jittered(
    rt: &mut Runtime<'_>,
    schedule: &str,
    jitter: Option<f64>,
    noun: &str,
    mut action: impl FnMut(&mut Runtime<'_>) -> SuccessTuple,
) -> SuccessTuple {
    let now = chrono::Utc::now().fixed_offset();
    // A job process (`MRSM_DAEMON_ID` is the job name) keeps its schedule per root: a restart
    // resumes the recorded next fire time instead of re-anchoring the cadence. Foreground runs
    // have no job name and keep nothing.
    let job = (rt.env)(&meerschaum_core::static_config().environment.daemon_id)
        .filter(|name| !name.is_empty());
    let default_jitter =
        jitter.unwrap_or_else(|| JobsConfig::from_config(rt.config()).schedule_jitter_seconds);
    let spec = meerschaum_jobs::JitterSpec {
        default_seconds: default_jitter,
        seed: job.as_deref().unwrap_or(schedule),
    };
    let mut trigger = match meerschaum_jobs::parse_schedule_jittered(schedule, now, Some(spec)) {
        Ok(t) => t,
        Err(e) => return SuccessTuple::fail(e.py_message()),
    };
    install_stop_handler(&super::session(rt));
    let stop = stop_requested_flag();
    let start = Instant::now();
    let scheduler = job
        .as_ref()
        .map(|_| RootScheduler::for_root(&rt.config().paths().root_dir));
    let mut pending = match (&scheduler, &job) {
        (Some(scheduler), Some(job)) => scheduler
            .next_fire_jittered(job, schedule, now, default_jitter)
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
    // Job health: each scheduled run is a lap, expected every period of the schedule (the gap
    // to the next fire, measured as the schedule runs; see `meerschaum_jobs::health`).
    let mut heartbeat =
        super::jobs_laps::heartbeat(meerschaum_jobs::schedule::period_seconds(schedule), noun);
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
        if heartbeat.record().interval_seconds.is_none_or(|i| i < wait) {
            heartbeat.set_interval(Some(wait));
            heartbeat.flush();
        }
        if wait >= 1.0 {
            // Idle until the first fire: a job's startup slot (D23) is not needed for that.
            meerschaum_jobs::startup::signal_first_lap_done();
        }
        heartbeat.waiting(Duration::try_from_secs_f64(wait).unwrap_or_default());
        if sleep_unless_stopped(wait) {
            break;
        }
        heartbeat.lap_started();
        let result = action(rt);
        heartbeat.lap_finished(result.success, &result.message);
        meerschaum_jobs::startup::signal_first_lap_done();
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

#[cfg(test)]
mod warning_tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct Sink(Rc<RefCell<Vec<u8>>>);

    impl std::io::Write for Sink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.borrow_mut().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn the_warnings_of_a_started_job_are_printed_once_through_the_formatter() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = JobsContext::for_root(dir.path());
        let mut job = Job::new("w", None, Default::default(), &ctx).unwrap();
        job.warnings
            .push("No Python interpreter is pinned in the unit of job 'w'.".into());
        let (mut out, err) = (Vec::new(), Sink::default());
        {
            let mut rt = Runtime::new(
                crate::types::DateContext::system(),
                80,
                &mut out,
                |_: &str| None,
            );
            rt.err = Box::new(err.clone());
            emit_job_warnings(&mut rt, &mut job);
            emit_job_warnings(&mut rt, &mut job);
        }
        let text = String::from_utf8(err.0.borrow().clone()).unwrap();
        assert_eq!(
            text.matches("No Python interpreter is pinned").count(),
            1,
            "{text}"
        );
        assert!(text.contains("🔔"), "{text}");
        assert!(job.warnings.is_empty());
    }

    /// `restart jobs` (and `compose up`) move a non-default root's `local` job to systemd: the
    /// unit gets the job's command and settings, the local directory becomes the hidden sister.
    #[test]
    fn a_local_job_of_another_root_moves_to_systemd() {
        use meerschaum_jobs::executor::JobExecutor;
        let dir = tempfile::tempdir().unwrap();
        let fake = Arc::new(meerschaum_jobs::FakeSystemctl::new());
        let mut ctx = JobsContext::for_root(dir.path().join("project/root"))
            .with_process_runner(fake.clone())
            .with_launcher(meerschaum_jobs::Launcher {
                supervisor: vec!["unused".into()],
                program: vec!["/usr/bin/mrsm".into()],
            })
            .with_process_env(vec![("PATH".into(), "/usr/bin".into())]);
        ctx.paths.systemd_user_dir = dir.path().join("home/.config/systemd/user");
        // As an installed copy on a systemd host would choose it.
        ctx.config.executor = Some("systemd".into());
        assert!(moves_local_jobs_to_systemd(&ctx));
        let mut env = IndexMap::new();
        env.insert("MRSM__COMPOSE_PROJECT".to_string(), "weather".to_string());
        let mut local = Job::new(
            "weather sync",
            Some(vec!["sync".into(), "pipes".into(), "--loop".into()]),
            JobOptions {
                executor_keys: Some("local".into()),
                env: Some(env),
                ..Default::default()
            },
            &ctx,
        )
        .unwrap();
        assert!(local.daemon().setup(true).success);
        let (mut out, err) = (Vec::new(), Sink::default());
        let moved = {
            let mut rt = Runtime::new(
                crate::types::DateContext::system(),
                80,
                &mut out,
                |_: &str| None,
            );
            rt.err = Box::new(err.clone());
            move_job_to_systemd(&mut rt, &ctx, &mut local)
        };
        assert!(moved.success, "{}", moved.message);
        let warned = String::from_utf8(err.0.borrow().clone()).unwrap();
        assert!(
            warned.contains("Moved the local job 'weather sync' to systemd"),
            "{warned}"
        );
        let ex = meerschaum_jobs::systemd::SystemdExecutor::new(ctx.clone());
        assert_eq!(ex.get_job_names(), vec!["weather sync".to_string()]);
        // Named after the Compose project, with the same command and environment.
        assert_eq!(
            ex.get_service_name("weather sync"),
            "mrsm-weather.weather-sync.service"
        );
        assert_eq!(
            ex.get_job_sysargs("weather sync"),
            vec!["sync", "pipes", "--loop"]
        );
        let props = ex.get_job_properties("weather sync");
        assert_eq!(
            props
                .get("env")
                .and_then(|e| e.get("MRSM__COMPOSE_PROJECT"))
                .and_then(Value::as_str),
            Some("weather")
        );
        assert!(
            fake.rendered_calls()
                .iter()
                .any(|c| c.starts_with("start mrsm-weather."))
        );
        // The default root's local jobs were someone's choice: they stay.
        let mut default_ctx = ctx.clone();
        default_ctx.paths.root = dir.path().join("home/.config/meerschaum");
        assert!(!moves_local_jobs_to_systemd(&default_ctx));
    }
}
