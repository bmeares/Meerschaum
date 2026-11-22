//! Any other action run inside the project (`_compose_default`), with the job actions (`start`,
//! `stop`, `pause`, `restart`, `delete`) scoped to the project's own jobs.

use super::*;

/// What `_scope_job_action` decided.
enum Scoped {
    /// Run these sysargs.
    Run(Vec<String>),
    /// Nothing to run (or refused).
    Done(SuccessTuple),
}

/// The flags compose reads itself and the action must not see.
const COMPOSE_VALUE_FLAGS: &[&str] = &["--file", "--env-file", "--project"];

/// `_compose_default`: run any other action inside the project.
pub(super) fn compose_default(
    ctx: &mut Ctx,
    project: &Project,
    run: &RunTree,
    ix: usize,
) -> Result<SuccessTuple, ComposeError> {
    let project_name = project.project_name();
    let mut isolated: Vec<String> = Vec::new();
    let mut skip_next = false;
    // PARITY: only `--file` and `--env-file` (not their aliases) are stripped
    // (default.py:30). The v5 flags `--project`, `--standalone` and `--no-stagger` are compose's.
    for arg in ctx.args.sysargs.iter().skip(1) {
        if skip_next {
            skip_next = false;
            continue;
        }
        if COMPOSE_VALUE_FLAGS.contains(&arg.as_str()) {
            skip_next = true;
            continue;
        }
        if arg.starts_with("--project=") || arg == "--standalone" || arg == "--no-stagger" {
            continue;
        }
        isolated.push(arg.clone());
    }
    let action = ctx.args.action.clone();
    let sysargs = match scope_job_action(ctx, project, &action, isolated, run, ix)? {
        Scoped::Run(s) => s,
        Scoped::Done(result) => return Ok(result),
    };
    let subprocess = project.config.get("isolation").and_then(Value::as_str) == Some("subprocess")
        || action.is_empty()
        || ["install", "setup"].contains(&action[0].as_str());
    if action.is_empty() {
        ctx.out.messages.info(format!(
            "Starting a shell in compose project '{project_name}'..."
        ));
    } else {
        ctx.out.messages.info(format!(
            "Running '{}' in compose project '{project_name}'...",
            action.join(" ")
        ));
    }
    // Syncing against a host connector that is not defined fails here, naming it.
    if action.first().is_some_and(|a| a == "sync" || a == "verify") {
        let pc = ProjectConfig::new(ctx.host, project)?;
        if let Some(message) = refs::blocking_message(&refs::findings(project, &pc, &[], &[])) {
            ctx.bulk_start = false;
            return Ok(SuccessTuple::fail(message));
        }
    }
    let result = ctx.run(
        &sysargs,
        project,
        RunOptions {
            subprocess: Some(subprocess),
            replace: false,
            ..RunOptions::default()
        },
    );
    ctx.bulk_start = false;
    result
}

fn filter_status(jobs: &IndexMap<String, JobInfo>, f: impl Fn(&JobInfo) -> bool) -> Vec<String> {
    jobs.iter()
        .filter(|(_, j)| f(j))
        .map(|(n, _)| n.clone())
        .collect()
}

/// The project of the tree each named job belongs to: the project that owns the existing job,
/// else the project that configures it, else the primary project. A name owned by a project
/// outside the tree stays with the primary project (which refuses it).
fn route_job_names(
    ctx: &mut Ctx,
    run: &RunTree,
    jobs: &IndexMap<String, JobInfo>,
    names: &[String],
) -> Result<IndexMap<String, usize>, ComposeError> {
    let mut configured: Vec<(usize, Vec<String>)> = Vec::new();
    for ix in &run.selected {
        let project = &run.tree.nodes[*ix].project;
        let keys = match get_jobs_commands(&project.config, &[], &mut ctx.out.messages) {
            Ok(commands) => commands.into_keys().collect(),
            Err(_) => Vec::new(),
        };
        configured.push((*ix, keys));
    }
    let mut routes = IndexMap::new();
    for name in names {
        let owner = jobs.get(name).and_then(job_project_name).and_then(|p| {
            run.selected
                .iter()
                .copied()
                .find(|ix| run.tree.nodes[*ix].project.project_name() == p)
        });
        let target = owner
            .or_else(|| {
                configured
                    .iter()
                    .find(|(_, keys)| keys.contains(name))
                    .map(|(ix, _)| *ix)
            })
            .unwrap_or(run.primary);
        routes.insert(name.clone(), target);
    }
    Ok(routes)
}

fn scope_job_action(
    ctx: &mut Ctx,
    project: &Project,
    action: &[String],
    sysargs: Vec<String>,
    run: &RunTree,
    node: usize,
) -> Result<Scoped, ComposeError> {
    if action.len() < 2
        || !SCOPED_JOB_ACTIONS.contains(&action[0].as_str())
        || (action[1] != "job" && action[1] != "jobs")
    {
        return Ok(Scoped::Run(sysargs));
    }
    let verb = action[0].as_str();
    let project_name = project.project_name();
    let jobs = match ctx.project_jobs() {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(Scoped::Done(failed)),
    };
    let mut job_names: Vec<String> = action[2..].to_vec();
    let mut sysargs = sysargs;
    if !job_names.is_empty() {
        if ctx.multi {
            // Several projects share the command: each takes the jobs that are its own.
            let routes = route_job_names(ctx, run, &jobs, &job_names)?;
            let mine: Vec<String> = job_names
                .iter()
                .filter(|n| routes.get(*n) == Some(&node))
                .cloned()
                .collect();
            if mine.is_empty() {
                return Ok(Scoped::Done(SuccessTuple::ok(format!(
                    "None of the named jobs belong to compose project '{project_name}'."
                ))));
            }
            if mine.len() != job_names.len() {
                // Keep only this project's names in the command.
                for ix in 0..sysargs.len().saturating_sub(1) {
                    if sysargs[ix..ix + 2] == action[..2]
                        && sysargs.len() >= ix + 2 + job_names.len()
                        && sysargs[ix + 2..ix + 2 + job_names.len()] == job_names[..]
                    {
                        let mut out: Vec<String> = sysargs[..ix + 2].to_vec();
                        out.extend(mine.iter().cloned());
                        out.extend(sysargs[ix + 2 + job_names.len()..].iter().cloned());
                        sysargs = out;
                        break;
                    }
                }
            }
            job_names = mine;
        }
        let foreign: Vec<&String> = job_names
            .iter()
            .filter(|n| {
                jobs.get(*n)
                    .is_some_and(|j| job_project_name(j).is_some_and(|p| p != project_name))
            })
            .collect();
        if !foreign.is_empty() {
            return Ok(Scoped::Done(SuccessTuple::fail(format!(
                "Refusing to {verb} job{} owned by another Compose project: {}\nRun `{verb} jobs` from the owning project's compose file.",
                plural(foreign.len()),
                foreign
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))));
        }
        let skipped = ctx.managed_elsewhere(project, &jobs, &job_names);
        if !skipped.is_empty() {
            let remaining: Vec<String> = job_names
                .iter()
                .filter(|n| !skipped.contains(n))
                .cloned()
                .collect();
            if remaining.is_empty() {
                return Ok(Scoped::Done(SuccessTuple::ok(format!(
                    "No jobs to {verb} in compose project '{project_name}'."
                ))));
            }
            for ix in 0..sysargs.len().saturating_sub(1) {
                if sysargs[ix..ix + 2] == action[..2]
                    && sysargs.len() >= ix + 2 + job_names.len()
                    && sysargs[ix + 2..ix + 2 + job_names.len()] == job_names[..]
                {
                    let mut out: Vec<String> = sysargs[..ix + 2].to_vec();
                    out.extend(remaining.iter().cloned());
                    out.extend(sysargs[ix + 2 + job_names.len()..].iter().cloned());
                    sysargs = out;
                    break;
                }
            }
        }
        return Ok(Scoped::Run(sysargs));
    }

    let names = get_project_job_names(&project.config, &[], &jobs, &mut ctx.out.messages)?;
    let skipped = ctx.managed_elsewhere(project, &jobs, &names);
    let project_jobs: IndexMap<String, JobInfo> = names
        .iter()
        .filter(|n| !skipped.contains(n))
        .filter_map(|n| jobs.get(n).map(|j| (n.clone(), j.clone())))
        .collect();
    if project_jobs.is_empty() {
        return Ok(Scoped::Done(SuccessTuple::ok(format!(
            "No jobs in compose project '{project_name}'."
        ))));
    }
    let mut to_act: Vec<String> = Vec::new();
    let add = |names: Vec<String>, to_act: &mut Vec<String>| {
        for n in names {
            if !to_act.contains(&n) {
                to_act.push(n);
            }
        }
    };
    match verb {
        "start" => {
            add(
                filter_status(&project_jobs, |j| j.status == "stopped"),
                &mut to_act,
            );
            add(
                filter_status(&project_jobs, |j| j.status == "paused"),
                &mut to_act,
            );
            if to_act.is_empty() {
                return Ok(Scoped::Done(SuccessTuple::ok("All jobs are running.")));
            }
        }
        "restart" => {
            add(filter_status(&project_jobs, |j| j.restart), &mut to_act);
            if to_act.is_empty() {
                return Ok(Scoped::Done(SuccessTuple::ok(
                    "No jobs need to be restarted.",
                )));
            }
        }
        "stop" | "pause" => {
            add(
                filter_status(&project_jobs, |j| j.status == "running"),
                &mut to_act,
            );
            add(
                filter_status(&project_jobs, |j| j.status == "paused"),
                &mut to_act,
            );
            if verb == "stop" {
                add(filter_status(&project_jobs, |j| j.restart), &mut to_act);
            }
            if to_act.is_empty() {
                return Ok(Scoped::Done(SuccessTuple::ok(format!(
                    "No jobs to {verb} in compose project '{project_name}'."
                ))));
            }
        }
        _ => to_act = project_jobs.keys().cloned().collect(),
    }
    let project_job_names: Vec<String> = project_jobs
        .keys()
        .filter(|n| to_act.contains(n))
        .cloned()
        .collect();

    if (verb == "stop" || verb == "pause") && !ctx.args.force {
        let mut capitalized = verb.to_string();
        capitalized[..1].make_ascii_uppercase();
        let noask = ctx.args.noask;
        if !ctx.yes_no(&format!("{capitalized} the above jobs?"), false, noask)? {
            return Ok(Scoped::Done(SuccessTuple::fail(format!(
                "No jobs were {}.",
                if verb == "stop" { "stopped" } else { "paused" }
            ))));
        }
    }

    // `start jobs` and `restart jobs` over the project's jobs are bulk starts.
    if verb == "start" || verb == "restart" {
        ctx.bulk_start = true;
    }
    for ix in 0..sysargs.len().saturating_sub(1) {
        if sysargs[ix..ix + 2] == action[..2] {
            let mut out: Vec<String> = sysargs[..ix + 2].to_vec();
            out.extend(project_job_names.iter().cloned());
            out.extend(sysargs[ix + 2..].iter().cloned());
            return Ok(Scoped::Run(out));
        }
    }
    Ok(Scoped::Done(SuccessTuple::fail(format!(
        "Could not scope `{}` to compose project '{project_name}'.\nName the jobs explicitly.",
        action.join(" ")
    ))))
}
