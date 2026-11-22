//! Registering, bootstrapping, editing and copying a Pipeline definition.

use super::removal::{referenced_jobs, remove_associations};
use super::*;

/// Ask for the name of a new Pipeline until it is valid and not taken (`bootstrap pipelines`
/// without a name). An empty answer or Ctrl-D cancels.
fn prompt_new_name(rt: &mut Runtime<'_>, instance: &Instance) -> Result<String, ActionError> {
    let taken: BTreeSet<String> = list(rt, instance)?.into_iter().map(|d| d.name).collect();
    loop {
        let answer = super::prompt(rt, "Pipeline name:", true, false)
            .ok_or_else(|| error("Creation cancelled."))?;
        let name = answer.trim().to_string();
        if name.is_empty() {
            return Err(error("Creation cancelled."));
        }
        if let Err(e) = validate_name(&name) {
            rt.print(&format!("{}\n", e.message));
            continue;
        }
        if taken.contains(&name) {
            rt.print(&format!("Pipeline '{name}' already exists.\n"));
            continue;
        }
        return Ok(name);
    }
}

pub(super) fn save(
    action: &str,
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    instance: &Instance,
    lease: &MutationLease,
) -> ActionResult {
    let words = words(args, "action");
    // `bootstrap` is a wizard: without a name on the command line it asks for one.
    let prompted;
    let name = match words.first() {
        Some(name) => name,
        None if action == "bootstrap" && rt.live && !flag(args, "noask") => {
            prompted = prompt_new_name(rt, instance)?;
            &prompted
        }
        None => return Err(error("Provide a Pipeline name.")),
    };
    validate_name(name)?;
    let existing = list(rt, instance)?.into_iter().find(|d| &d.name == name);
    if ["register", "bootstrap"].contains(&action) && existing.is_some() {
        return Err(error(format!("Pipeline '{name}' already exists.")));
    }
    if ["edit", "copy"].contains(&action) && existing.is_none() {
        return Err(error(format!("Pipeline '{name}' does not exist.")));
    }
    let mut definition = existing.clone().unwrap_or_else(|| Definition {
        schema_version: 2,
        id: uuid::Uuid::new_v4().to_string(),
        registry: instance.keys(),
        name: name.clone(),
        ..Default::default()
    });
    if action == "copy" {
        definition.name = words
            .get(1)
            .ok_or_else(|| error("Usage: copy pipelines NAME NEW_NAME"))?
            .clone();
        definition.id = uuid::Uuid::new_v4().to_string();
        definition.revision = 1;
        definition.tag_associations.clear();
        validate_definition(rt, &mut definition)?;
        if list(rt, instance)?
            .iter()
            .any(|d| d.name == definition.name)
        {
            return Err(error("Destination Pipeline already exists."));
        }
        lease.check()?;
        write(rt, instance, &definition, true)?;
        return Ok(SuccessTuple::success());
    }
    let patch = super::params(args)
        .map(|p| serde_json::from_str::<Json>(&Value::Map(p).to_json_string()).map_err(error))
        .transpose()?;
    if let Some(mut patch) = patch {
        if let Some(inner) = patch.get("pipeline") {
            patch = inner.clone();
        }
        if action == "edit" && patch.get("revision").and_then(Json::as_u64).is_none() {
            return Err(error(
                "Include the current revision in --params when editing a Pipeline.",
            ));
        }
        if let Some(revision) = patch.get("revision").and_then(Json::as_u64)
            && action == "edit"
            && revision != definition.revision
        {
            return Err(error("Pipeline revision changed; reload before saving."));
        }
        let mut value = serde_json::to_value(&definition).map_err(error)?;
        for (key, item) in patch
            .as_object()
            .ok_or_else(|| error("Pipeline parameters must be an object."))?
        {
            if key == "name" && action != "edit" {
                continue;
            }
            if [
                "targets",
                "parents",
                "children",
                "graph",
                "description",
                "defaults",
                "layout",
                "name",
            ]
            .contains(&key.as_str())
            {
                value[key] = item.clone();
            }
        }
        definition = serde_json::from_value(value).map_err(error)?;
    } else if action == "edit" {
        if !rt.live || flag(args, "noask") {
            return Err(error(
                "Use --params with the expected revision in noninteractive edits.",
            ));
        }
        let directory = rt.config().paths().root_dir.join("cache");
        std::fs::create_dir_all(&directory).map_err(error)?;
        let path = directory.join(format!("pipeline-edit-{}.yaml", uuid::Uuid::new_v4()));
        std::fs::write(
            &path,
            meerschaum_core::yaml::dump(
                &serde_json::from_value::<Value>(serde_json::to_value(&definition).map_err(error)?)
                    .map_err(error)?,
            )
            .map_err(error)?,
        )
        .map_err(error)?;
        let edited = super::editor::edit_file(rt, &path).map_err(error)?;
        let data = std::fs::read(&path).map_err(error)?;
        let _ = std::fs::remove_file(path);
        if !edited {
            return Ok(SuccessTuple::fail("Editor cancelled."));
        }
        let yaml = meerschaum_core::yaml::load(std::str::from_utf8(&data).map_err(error)?)
            .map_err(error)?;
        let edited: Definition = serde_json::from_str(&yaml.to_json_string()).map_err(error)?;
        if edited.revision != definition.revision {
            return Err(error("Do not change the revision field."));
        }
        definition.name = edited.name;
        definition.targets = edited.targets;
        definition.parents = edited.parents;
        definition.children = edited.children;
        definition.description = edited.description;
        definition.defaults = edited.defaults;
        definition.layout = edited.layout;
    } else {
        let create_new = action == "bootstrap"
            && rt.live
            && !flag(args, "noask")
            && super::yes_no(
                rt,
                "Create a new Pipe with the native Pipe wizard?",
                "n",
                false,
                false,
            )?;
        let before: BTreeSet<_> = if create_new {
            super::pipes::get_pipes(rt, instance, &Default::default())?
                .iter()
                .map(|p| PipeRef::from_pipe(&p.pipe).to_string())
                .collect()
        } else {
            BTreeSet::new()
        };
        if create_new {
            let result = super::bootstrap::bootstrap_pipes(args, rt)?;
            if !result.success {
                return Ok(result);
            }
        }
        let pipes = if create_new {
            super::pipes::get_pipes(rt, instance, &Default::default())?
                .into_iter()
                .filter(|p| !before.contains(&PipeRef::from_pipe(&p.pipe).to_string()))
                .collect()
        } else {
            super::pipes::selected_pipes(rt, args)?.1
        };
        // Listings leave out the internal `mrsm` pipes, but `-c mrsm` names them: they are never
        // a Pipeline's members.
        definition.targets = pipes
            .iter()
            .filter(|p| p.pipe.connector_keys() != "mrsm")
            .map(|p| PipeRef::from_pipe(&p.pipe).to_string())
            .collect();
        if action == "bootstrap" && rt.live {
            let answer = super::prompt(rt, "Member references (comma-separated instance|connector|metric|location); Enter keeps the selection:", true, false)
                .ok_or_else(|| error("Creation cancelled."))?;
            if !answer.trim().is_empty() {
                definition.targets = answer.split(',').map(|r| r.trim().to_string()).collect();
            }
        }
    }
    for key in ["parents", "children"] {
        if let Some(value) = args.get(key) {
            let json =
                serde_json::from_str(&super::py_to_value(value).to_json_string()).map_err(error)?;
            if key == "parents" {
                definition.parents = json;
            } else {
                definition.children = json;
            }
        }
    }
    validate_definition(rt, &mut definition)?;
    let rename = definition.name != *name;
    if let Some(old) = &existing {
        if read(rt, instance, name)?.revision != old.revision {
            return Err(error("Pipeline revision changed; reload before saving."));
        }
        if rename {
            let jobs = referenced_jobs(rt, instance, name)?;
            if !jobs.is_empty() {
                return Err(error(format!(
                    "Remove or reassign scheduled Jobs first: {}",
                    jobs.join(", ")
                )));
            }
            if list(rt, instance)?
                .iter()
                .any(|d| d.name == definition.name)
            {
                return Err(error("Destination Pipeline already exists."));
            }
            remove_associations(rt, old)?;
            definition.tag_associations.clear();
        }
    }
    definition.revision += 1;
    lease.check()?;
    write(rt, instance, &definition, existing.is_none() || rename)?;
    if rename && existing.is_some() {
        let old = metadata(rt, instance, name)?;
        let result = session(rt).block_on(instance.conn().delete_pipe(&old));
        if !result.success {
            return Err(error(format!(
                "New definition saved; old registration removal failed: {}",
                result.message
            )));
        }
    }
    rt.print(&format!(
        "{}\n",
        serde_json::to_string_pretty(&definition).map_err(error)?
    ));
    Ok(SuccessTuple::ok(
        "Pipeline saved. Run analyze pipelines to review and approve membership tags.",
    ))
}
