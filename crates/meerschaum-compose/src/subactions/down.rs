//! `compose down`: stop the project's jobs and, with `-v`, drop its pipes.

use super::*;

/// `compose down [-v]`.
pub(super) fn compose_down(
    ctx: &mut Ctx,
    project: &Project,
    pc: &ProjectConfig,
) -> Result<SuccessTuple, ComposeError> {
    let project_name = project.project_name();
    let jobs = match ctx.project_jobs() {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
    let owned = get_project_job_names(&project.config, &[], &jobs, &mut ctx.out.messages)?;
    let skipped = ctx.managed_elsewhere(project, &jobs, &owned);
    for job_name in owned.into_iter().filter(|n| !skipped.contains(n)) {
        let r = ctx.run(
            &["delete".into(), "job".into(), job_name.clone(), "-f".into()],
            project,
            RunOptions::default(),
        )?;
        if !r.success {
            return Ok(SuccessTuple::fail(format!(
                "Failed to delete job '{job_name}':\n{}",
                r.message
            )));
        }
    }
    if !ctx.args.drop {
        return Ok(SuccessTuple::success());
    }

    build_custom_connectors(ctx, project)?;
    let all = defined_pipes(ctx, project, pc)?;
    let pipes: Vec<Pipe> = all
        .into_iter()
        .filter(|p| ctx.instance.get_id(p).is_some())
        .collect();
    if pipes.is_empty() {
        return Ok(SuccessTuple::fail("No pipes to delete."));
    }
    let groups = instance_pipes(&pipes);
    let question = format!(
        "Remove {} pipe{} from this project? Unshared pipes and their data will be deleted{}.",
        pipes.len(),
        plural(pipes.len()),
        if pipes.len() > 1 {
            if groups.len() == 1 {
                " on 1 instance".to_string()
            } else {
                format!(" across {} instances", groups.len())
            }
        } else {
            String::new()
        }
    );
    if !ctx.yes_no(&question, false, false)? {
        return Ok(SuccessTuple::ok("Nothing was deleted."));
    }

    let mut failures: Vec<String> = Vec::new();
    for pipe in &pipes {
        let attrs = match ctx.instance.get_pipe_attributes(pipe) {
            Ok(a) => a,
            Err(e) => {
                failures.push(format!("Could not verify ownership of {pipe}: {e}"));
                continue;
            }
        };
        let remote_params = attrs.get("parameters").cloned();
        let remote_tags = match &remote_params {
            Some(Value::Map(m)) if !m.is_empty() => m.get("tags").cloned(),
            _ => None,
        };
        let (Some(Value::Map(remote_params)), Some(Value::List(remote_tags))) =
            (remote_params, remote_tags)
        else {
            failures.push(format!(
                "Could not verify ownership of {pipe}; it was preserved."
            ));
            continue;
        };
        let remaining: Vec<Value> = remote_tags
            .iter()
            .filter(|t| py_str(t) != project_name)
            .cloned()
            .collect();
        if remaining.len() == remote_tags.len() {
            ctx.out.messages.info(format!(
                "Preserving {pipe}; it is not tagged as '{project_name}'."
            ));
            continue;
        }
        let mut pipe = pipe.clone();
        pipe.set_parameters(remote_params.clone());
        if !remaining.is_empty() {
            ctx.out.messages.info(format!(
                "Preserving shared {pipe} and removing tag '{project_name}'."
            ));
            let mut params = remote_params;
            params.insert("tags".into(), Value::List(remaining));
            pipe.set_parameters(params);
            let r = ctx.instance.edit_pipe(&pipe);
            if !r.success {
                failures.push(format!("Failed to untag {pipe}: {}", r.message));
            }
            continue;
        }
        ctx.out.messages.info(format!("Deleting unshared {pipe}."));
        let r = ctx.instance.delete_pipe(&pipe);
        for warning in ctx.instance.take_warnings() {
            ctx.out.messages.warn(warning);
        }
        if !r.success {
            failures.push(format!("Failed to delete {pipe}: {}", r.message));
        }
    }
    if !failures.is_empty() {
        return Ok(SuccessTuple::fail(failures.join("\n")));
    }
    Ok(SuccessTuple::success())
}
