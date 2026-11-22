//! `compose up`: register and sync the project's pipes and start its jobs, refusing to take over
//! jobs the project does not own.

use super::*;

/// `compose up` (and `compose run`, which is `up --no-jobs --presync`).
pub(super) fn compose_up(
    ctx: &mut Ctx,
    project: &Project,
    pc: &ProjectConfig,
    mode: UpMode,
) -> Result<SuccessTuple, ComposeError> {
    let no_daemon_flags: Vec<String> =
        if project.config.get("isolation").and_then(Value::as_str) == Some("subprocess") {
            vec!["--no-daemon".into()]
        } else {
            Vec::new()
        };

    // A host connector the compose file references and this host does not define fails before
    // anything is installed.
    if let Some(message) = refs::blocking_message(&refs::findings(project, pc, &[], &[])) {
        return Ok(SuccessTuple::fail(message));
    }

    let result = check_and_install_plugins(
        project,
        pc,
        None,
        ctx.args.debug,
        &mut ctx.runner,
        &mut ctx.out.messages,
    )?;
    if !result.success {
        return Ok(result);
    }

    build_custom_connectors(ctx, project)?;
    let pipes = defined_pipes(ctx, project, pc)?;
    // Refuse to start on a connector or plugin that is not there, naming it and where it is
    // referenced (plugins listed under `plugins:` were installed above).
    let findings = project_findings(ctx, project, pc);
    if let Some(message) = refs::blocking_message(&findings) {
        return Ok(SuccessTuple::fail(message));
    }
    // Every job `up` would start is checked before anything changes: a job of another project
    // with one of its names refuses the whole `up` (no pipe is registered or synced first).
    if !mode.dry && !mode.no_jobs {
        // The messages of this look are shown only with a refusal: the jobs are listed again
        // (and their warnings shown) when they start.
        let mark = ctx.out.messages.items.len();
        let jobs_commands = get_jobs_commands(&project.config, &pipes, &mut Messages::default())?;
        let existing = match ctx.project_jobs() {
            Ok(jobs) => jobs,
            Err(failed) => return Ok(failed),
        };
        let owned =
            get_project_job_names(&project.config, &pipes, &existing, &mut Messages::default())?;
        if let Some(refusal) = foreign_collisions(&jobs_commands, &existing, &owned) {
            return Ok(refusal);
        }
        ctx.out.messages.items.truncate(mark);
    }
    let groups = instance_pipes(&pipes);
    let project_name = project.project_name();
    let no_project_jobs = jobs_disabled(&project.config);
    let explicit_jobs = no_project_jobs || project.config.get("jobs").is_some_and(Value::truthy);
    let tags = vec![project_name.clone()];

    let mut remote_instance_pipes: IndexMap<String, Vec<Pipe>> = IndexMap::new();
    for inst in groups.keys() {
        let remote = get_pipes_or_raise(ctx, inst, &tags)?;
        remote_instance_pipes.insert(inst.clone(), remote);
    }

    let mut updated: Vec<Pipe> = Vec::new();
    for pipe in &pipes {
        let mut updated_registration = false;
        let registered = remote_instance_pipes
            .get(pipe.instance_keys())
            .is_some_and(|rs| {
                rs.iter().any(|r| {
                    r.connector_keys() == pipe.connector_keys()
                        && r.metric_key() == pipe.metric_key()
                        && r.location_key() == pipe.location_key()
                })
            });
        let remote_params = ctx.instance.get_parameters(pipe).unwrap_or_default();
        let local_params = pipe.raw_parameters().clone();
        let local_str = json_sorted_compact(&Value::Map(local_params.clone()));
        let remote_str = json_sorted_compact(&Value::Map(remote_params.clone()));

        if pipe.temporary {
            ctx.out.messages.info(format!(
                "{pipe} is temporary, will not modify registration."
            ));
        } else if !registered {
            let existing_id = ctx.instance.get_id(pipe);
            if existing_id.is_some() {
                ctx.out.messages.info(format!(
                    "{pipe} already exists on '{}'; adding tag '{project_name}'...",
                    pipe.instance_keys()
                ));
                let fresh = ctx
                    .instance
                    .get_parameters(pipe)
                    .unwrap_or_else(|| remote_params.clone());
                let effective = effective_parameters(ctx, pc, pipe);
                let mut merged_tags: Vec<Value> = Vec::new();
                for t in tags_list(&fresh).into_iter().chain(tags_list(&effective)) {
                    if !merged_tags.contains(&t) {
                        merged_tags.push(t);
                    }
                }
                let mut merged = effective.clone();
                merged.insert("tags".into(), Value::List(merged_tags));
                let mut remote_pipe = pipe.clone();
                remote_pipe.set_parameters(merged);
                let edit = ctx.instance.edit_pipe(&remote_pipe);
                if !edit.success {
                    ctx.out.messages.warn(format!(
                        "Failed to add tag '{project_name}' to {pipe}:\n{}",
                        edit.message
                    ));
                }
            } else {
                ctx.out.messages.info(format!("Registering {pipe}..."));
                let params = effective_parameters(ctx, pc, pipe);
                let mut a: Vec<String> = vec![
                    "register".into(),
                    "pipes".into(),
                    "-c".into(),
                    pipe.connector_keys().into(),
                    "-m".into(),
                    pipe.metric_key().into(),
                    "-l".into(),
                    location_arg(pipe),
                    "-i".into(),
                    pipe.instance_keys().into(),
                    "--params".into(),
                    json_dumps(&Value::Map(params), JsonStyle::Compact, true),
                    "--noask".into(),
                ];
                a.extend(no_daemon_flags.iter().cloned());
                let r = ctx.run(
                    &a,
                    project,
                    RunOptions {
                        replace: false,
                        subprocess: Some(false),
                        ..RunOptions::default()
                    },
                )?;
                if !r.success {
                    ctx.out.messages.warn(format!("Failed to register {pipe}."));
                }
            }
            updated_registration = true;
        } else if local_str != remote_str {
            ctx.out
                .messages
                .info(format!("Updating parameters for {pipe}..."));
            let edit = ctx.instance.edit_pipe(pipe);
            if !edit.success {
                ctx.out.messages.warn(format!("Failed to edit {pipe}."));
            }
            updated_registration = true;
        }

        if updated_registration || mode.presync || pipe.temporary {
            updated.push(pipe.clone());
        }
    }

    // Untag the pipes which are tagged but no longer defined.
    for inst in groups.keys() {
        let tagged = get_pipes_or_raise(ctx, inst, &tags)?;
        for mut tagged_pipe in tagged {
            if pipes.contains(&tagged_pipe) {
                continue;
            }
            let params = ctx
                .instance
                .get_parameters(&tagged_pipe)
                .map(|r| apply_patch_maps(&r, tagged_pipe.raw_parameters()))
                .unwrap_or_else(|| tagged_pipe.raw_parameters().clone());
            let Some(Value::List(current)) = params.get("tags").cloned() else {
                ctx.out.messages.warn(format!(
                    "{tagged_pipe} was incorrectly tagged with '{project_name}'..."
                ));
                continue;
            };
            let remaining: Vec<Value> = current
                .into_iter()
                .filter(|t| py_str(t) != project_name)
                .collect();
            let mut new_params = params.clone();
            new_params.insert("tags".into(), Value::List(remaining));
            tagged_pipe.set_parameters(new_params);
            ctx.out.messages.info(format!(
                "Removing tag '{project_name}' from {tagged_pipe}..."
            ));
            ctx.instance.edit_pipe(&tagged_pipe);
        }
    }

    for spec in crate::pipelines::defined(project, pc)? {
        let revision = ctx
            .pipeline_revisions
            .get(&(
                project_name.clone(),
                spec.instance.clone(),
                spec.name.clone(),
            ))
            .copied()
            .unwrap_or(None);
        let saved = ctx.instance.save_pipeline(&spec, &project_name, revision);
        if !saved.success {
            return Ok(saved);
        }
    }

    if mode.dry {
        return Ok(SuccessTuple::ok(format!(
            "Updated parameters for {} pipe{}{}{} instance{}.",
            pipes.len(),
            plural(pipes.len()),
            if groups.len() != 1 {
                " across "
            } else {
                " on "
            },
            groups.len(),
            plural(groups.len()),
        )));
    }

    let mut ran_verification_sync = false;
    if mode.presync
        || (!explicit_jobs && !updated.is_empty() && config_has_changed(project, ctx.session))
    {
        ran_verification_sync = true;
        let r = run_initial_syncs(ctx, project, &updated)?;
        if !r.success {
            return Ok(r);
        }
    }

    if mode.no_jobs {
        let msg = if ran_verification_sync {
            format!(
                "Synced {} pipe{} across {} instance{}.",
                updated.len(),
                plural(updated.len()),
                groups.len(),
                plural(groups.len())
            )
        } else if !updated.is_empty() {
            // PARITY: Python never pluralizes "instance" here (up.py:265).
            format!(
                "Updated {} pipe{} across {} instance.",
                updated.len(),
                plural(updated.len()),
                groups.len()
            )
        } else {
            "Nothing to do.".to_string()
        };
        return Ok(SuccessTuple::ok(msg));
    }

    let mut jobs_commands = get_jobs_commands(&project.config, &pipes, &mut ctx.out.messages)?;
    let existing = match ctx.project_jobs() {
        Ok(jobs) => jobs,
        Err(failed) => return Ok(failed),
    };
    let to_delete =
        get_project_job_names(&project.config, &pipes, &existing, &mut ctx.out.messages)?;
    // Checked again: another project may have started a job while the pipes synced.
    if let Some(refusal) = foreign_collisions(&jobs_commands, &existing, &to_delete) {
        return Ok(refusal);
    }
    // Jobs a root compose started are managed from there.
    let skipped = ctx.managed_elsewhere(project, &existing, &to_delete);
    let to_delete: Vec<String> = to_delete
        .into_iter()
        .filter(|n| !skipped.contains(n))
        .collect();
    jobs_commands.retain(|name, _| !skipped.contains(name));

    for job_name in &to_delete {
        // A local job the project's systemd executor takes over is not deleted: starting it
        // again moves it to systemd (stopping it first) and keeps its logs.
        if jobs_commands.contains_key(job_name) && ctx.jobs.moves_to_systemd(job_name) {
            ctx.out.messages.info(format!(
                "Moving job '{job_name}' to systemd so it survives a reboot..."
            ));
            continue;
        }
        let r = ctx.run(
            &["delete".into(), "job".into(), job_name.clone(), "-f".into()],
            project,
            RunOptions {
                capture_output: !ctx.args.debug,
                replace: false,
                ..RunOptions::default()
            },
        )?;
        if !r.success {
            return Ok(SuccessTuple::fail(format!(
                "Failed to delete job '{job_name}':\n{}",
                r.message
            )));
        }
    }

    // Several jobs (or several projects) starting together share the startup gate.
    // Each job is started by its own command, but they are one bulk start for the gate.
    ctx.bulk_start = !jobs_commands.is_empty();
    for (job_name, command) in &jobs_commands {
        ctx.out
            .messages
            .info(format!("Starting job '{job_name}'..."));
        let r = ctx.run(
            command,
            project,
            RunOptions {
                replace: false,
                ..RunOptions::default()
            },
        );
        let r = match r {
            Ok(r) => r,
            Err(e) => {
                ctx.bulk_start = false;
                return Err(e);
            }
        };
        if !r.success {
            ctx.bulk_start = false;
            return Ok(SuccessTuple::fail(format!(
                "Failed to start job '{job_name}':\n{}",
                r.message
            )));
        }
    }

    ctx.bulk_start = false;
    if mode.force {
        let mut a = vec!["show".to_string(), "logs".to_string()];
        a.extend(jobs_commands.keys().cloned());
        ctx.run(
            &a,
            project,
            RunOptions {
                replace: false,
                ..RunOptions::default()
            },
        )?;
    }

    let mut msg = if no_project_jobs {
        "This project starts no jobs (`jobs: {}`).".to_string()
    } else if explicit_jobs {
        format!(
            "Running {} background job{}.",
            jobs_commands.len(),
            plural(jobs_commands.len())
        )
    } else if pipes.len() == 1 {
        format!("Syncing {} in a background job.", pipes[0])
    } else {
        format!(
            "Syncing {} pipe{}{}{} instance{}.",
            pipes.len(),
            plural(pipes.len()),
            if jobs_commands.len() != 1 {
                " across "
            } else {
                " on "
            },
            jobs_commands.len(),
            plural(jobs_commands.len())
        )
    };
    if !mode.force && !no_project_jobs {
        msg.push_str("\nRun `mrsm compose logs` or pass `-f` to follow logs output.");
    }
    Ok(SuccessTuple::ok(msg))
}

/// The refusal of `up` when a job it would start has the name of a job this project does not
/// own (`owned`: the existing jobs that belong to the project).
fn foreign_collisions(
    jobs_commands: &IndexMap<String, Vec<String>>,
    existing: &IndexMap<String, JobInfo>,
    owned: &[String],
) -> Option<SuccessTuple> {
    let collisions: Vec<&str> = jobs_commands
        .keys()
        .filter(|n| existing.contains_key(*n) && !owned.contains(n))
        .map(String::as_str)
        .collect();
    if collisions.is_empty() {
        return None;
    }
    Some(SuccessTuple::fail(format!(
        "Refusing to replace job{} not owned by this Compose project: {}\n\
         Nothing was registered, synced or started. Rename the job{} in this compose file, or \
         stop and delete {} first.",
        plural(collisions.len()),
        collisions.join(", "),
        plural(collisions.len()),
        if collisions.len() == 1 { "it" } else { "them" },
    )))
}

/// `run_initial_syncs(pipes, compose_config, sysargs)`: two passes of syncing.
fn run_initial_syncs(
    ctx: &mut Ctx,
    project: &Project,
    pipes: &[Pipe],
) -> Result<SuccessTuple, ComposeError> {
    const FLAGS_TO_REMOVE: &[&str] = &[
        "-c",
        "-C",
        "--connector-keys",
        "-m",
        "-M",
        "--metric-keys",
        "-l",
        "-L",
        "--location-keys",
        "-i",
        "-I",
        "--mrsm-instance",
        "--instance",
    ];
    let sysargs = &ctx.args.sysargs;
    let removed: Vec<usize> = sysargs
        .iter()
        .enumerate()
        .filter(|(_, f)| FLAGS_TO_REMOVE.contains(&f.as_str()))
        .map(|(i, _)| i)
        .collect();
    // PARITY: every other argument is forwarded, including the `compose` action words
    // (`sync pipes -c ... compose run --no-daemon`), as in up.py:367.
    let mut flags: Vec<String> = sysargs
        .iter()
        .enumerate()
        .filter(|(i, _)| !removed.contains(i) && !(*i > 0 && removed.contains(&(i - 1))))
        .map(|(_, f)| f.clone())
        .collect();
    if !flags.iter().any(|f| f == "--no-daemon") {
        flags.push("--no-daemon".into());
    }

    let sync_one = |ctx: &mut Ctx, pipe: &Pipe| -> Result<SuccessTuple, ComposeError> {
        if pipe.temporary {
            return Ok(ctx.instance.sync_pipe(pipe));
        }
        let mut a: Vec<String> = vec![
            "sync".into(),
            "pipes".into(),
            "-c".into(),
            pipe.connector_keys().into(),
            "-m".into(),
            pipe.metric_key().into(),
            "-l".into(),
            location_arg(pipe),
            "-i".into(),
            pipe.instance_keys().into(),
        ];
        a.extend(flags.iter().cloned());
        ctx.run(
            &a,
            project,
            RunOptions {
                replace: false,
                ..RunOptions::default()
            },
        )
    };

    let mut failed = Vec::new();
    for pipe in pipes {
        ctx.out.messages.info(format!("Syncing {pipe}..."));
        let r = sync_one(ctx, pipe)?;
        if !r.success {
            ctx.out
                .messages
                .warn(format!("Failed to sync {pipe}:\n{}", r.message));
            failed.push(pipe.clone());
        }
    }
    if failed.is_empty() {
        return Ok(SuccessTuple::success());
    }
    for pipe in &failed {
        ctx.out.messages.info(format!("Retry syncing {pipe}..."));
        let r = sync_one(ctx, pipe)?;
        if !r.success {
            ctx.out
                .messages
                .warn(format!("Failed to sync {pipe}:\n{}", r.message));
            return Ok(SuccessTuple::fail(format!(
                "Unable to begin syncing {pipe}:\n{}",
                r.message
            )));
        }
    }
    Ok(SuccessTuple::success())
}
