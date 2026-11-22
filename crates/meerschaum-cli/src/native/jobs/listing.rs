//! `show jobs` and `pprint_jobs` (`utils/formatting/_jobs.py`): the jobs table and each job's
//! health line.

use indexmap::IndexMap;
use meerschaum_core::SuccessTuple;
use meerschaum_jobs::{Job, Status};

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::rich::console::{Group, Justify, Renderable};
use crate::format::rich::table::{ASCII, Column, ROUNDED, Table};
use crate::format::rich::text::Text;
use crate::value::ArgsMap;

use super::{executor_keys, filtered_jobs, jobs_context, with_status};
use crate::native::{flag, info, items_str, plural, words};

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
