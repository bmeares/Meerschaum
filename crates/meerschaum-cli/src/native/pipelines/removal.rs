//! Deleting and dropping Pipelines: the jobs that still refer to a definition, the tags it
//! owns, and the destructive actions themselves.

use super::analysis::change_tag;
use super::*;

pub(super) fn scheduled_reference(
    args: &[String],
    name: &str,
    registry: &str,
    dates: &crate::types::DateContext,
) -> Result<bool, ActionError> {
    if !args
        .iter()
        .any(|arg| ["--schedule", "-s", "--loop"].contains(&arg.split('=').next().unwrap_or("")))
    {
        return Ok(false);
    }
    let (commands, global_args) = crate::sysargs::split_pipeline_sysargs(args);
    let inherited = crate::parse::parse_arguments(&global_args, dates).map_err(error)?;
    for step in crate::sysargs::split_chained_sysargs(&commands).map_err(error)? {
        let parsed = crate::parse::parse_arguments(&step, dates).map_err(error)?;
        let action = words(&parsed, "action");
        if action.first().is_none_or(|a| a != "sync")
            || action
                .get(1)
                .is_none_or(|a| !["pipeline", "pipelines"].contains(&a.as_str()))
        {
            continue;
        }
        let configured = super::string(&parsed, "mrsm_instance")
            .or_else(|| super::string(&inherited, "mrsm_instance"));
        if action[2..].iter().any(|word| word == name)
            && configured.as_deref().is_none_or(|value| value == registry)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn referenced_jobs(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    name: &str,
) -> Result<Vec<String>, ActionError> {
    let ctx = super::jobs::jobs_context(rt);
    let listing = meerschaum_jobs::executor::get_jobs_for(&ctx, None, true, true).map_err(error)?;
    let mut found = Vec::new();
    if instance.keys().starts_with("api:") {
        let pipe = metadata(rt, instance, name)?;
        let attributes = session(rt)
            .block_on(instance.conn().get_pipe_attributes(&pipe))
            .map_err(error)?;
        let references = attributes
            .get("pipeline_referencing_jobs")
            .and_then(Value::as_list)
            .ok_or_else(|| {
                error("The API instance could not confirm Pipeline schedule references.")
            })?;
        found.extend(
            references
                .iter()
                .map(|name| format!("{}:{}", instance.keys(), name.py_str())),
        );
    }
    for (job_name, mut job) in listing.jobs {
        if scheduled_reference(&job.sysargs(), name, &instance.keys(), &rt.dates)? {
            found.push(job_name);
        }
    }
    Ok(found)
}

pub(super) fn remove_associations(
    rt: &mut Runtime<'_>,
    definition: &Definition,
) -> Result<(), ActionError> {
    let mut errors = Vec::new();
    for association in &definition.tag_associations {
        let pipe = parse_ref(rt, &association.pipe_ref)?;
        let instance = session(rt).instance(pipe.instance_keys(), rt.config())?;
        if session(rt)
            .block_on(instance.conn().get_pipe_id(&pipe))
            .map_err(error)?
            .is_none()
        {
            continue;
        }
        match change_tag(
            rt,
            &association.pipe_ref,
            &association.tag,
            &definition.id,
            false,
        ) {
            Ok(result) if result.success => (),
            Ok(result) => errors.push(format!("{}: {}", association.pipe_ref, result.message)),
            Err(err) => errors.push(format!("{}: {}", association.pipe_ref, err.message)),
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(error(format!(
            "Tag cleanup incomplete; definition retained for retry. {}",
            errors.join("; ")
        )))
    }
}

pub(super) fn destructive(
    action: &str,
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    instance: &Instance,
) -> ActionResult {
    let definitions = names(rt, instance, args, true)?;
    let targets: BTreeSet<_> = definitions
        .iter()
        .flat_map(|d| d.targets.iter().cloned())
        .collect();
    let all = list(rt, instance)?;
    let mut frozen = Vec::new();
    for target in &targets {
        let (inst, pipe) = member(rt, target)?;
        let mut shared: BTreeSet<String> = all
            .iter()
            .filter(|d| d.targets.contains(target))
            .map(|d| d.name.clone())
            .collect();
        if let Some(provenance) = pipe
            .raw_parameters()
            .get("_pipeline_tags")
            .and_then(Value::as_map)
        {
            for (tag, entry) in provenance {
                if let Some(owners) = entry
                    .as_map()
                    .and_then(|entry| entry.get("owners"))
                    .and_then(Value::as_list)
                {
                    shared.extend(
                        owners
                            .iter()
                            .map(|owner| format!("{tag} (owner {})", owner.py_str())),
                    );
                }
            }
        }
        rt.print(&format!(
            "{target} (Known Pipeline memberships / owners: {})\n",
            shared.into_iter().collect::<Vec<_>>().join(", ")
        ));
        frozen.push((inst, pipe));
    }
    let query = DataQuery {
        begin: super::show::bound(args, "begin"),
        end: super::show::bound(args, "end"),
        params: super::params(args),
        ..Default::default()
    };
    rt.print(&format!(
        "{action}: {} explicit members; traversal excluded. Bounds: {:?} to {:?}.\n",
        frozen.len(),
        query.begin,
        query.end
    ));
    if !confirm(
        rt,
        args,
        "Proceed with this exact destructive selection?",
        true,
    )? {
        return Ok(SuccessTuple::fail("No Pipe data changed."));
    }
    for definition in &definitions {
        if read(rt, instance, &definition.name)?.revision != definition.revision {
            return Err(error(
                "Definition changed; review a new destructive preview.",
            ));
        }
    }
    let mut success = true;
    for (instance, pipe) in frozen {
        let result = if action == "drop" {
            session(rt).block_on(instance.conn().drop_pipe(&pipe))
        } else {
            session(rt).block_on(instance.conn().clear_pipe(&pipe, &query))
        };
        rt.print(&format!(
            "{}: {}\n",
            PipeRef::from_pipe(&pipe),
            result.message
        ));
        success &= result.success;
    }
    Ok(SuccessTuple {
        success,
        message: format!("{action} complete; Pipeline and Pipe registrations preserved."),
    })
}
