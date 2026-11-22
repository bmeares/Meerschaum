//! `analyze pipelines`: resolving members and traversal into a preview of tag changes, and
//! applying an approved preview.

use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(super) struct Change {
    pipe_ref: String,
    tag: String,
    action: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Analysis {
    name: String,
    registry: String,
    definition: Definition,
    revision: u64,
    resolved: Vec<Json>,
    pub(super) changes: Vec<Change>,
    shared: BTreeMap<String, Vec<String>>,
    pub(super) errors: Vec<String>,
    fingerprint: String,
}

pub(super) fn analyze(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    definition: &Definition,
) -> Result<Analysis, ActionError> {
    let mut analysis = Analysis {
        name: definition.name.clone(),
        registry: instance.keys(),
        definition: definition.clone(),
        revision: definition.revision,
        resolved: Vec::new(),
        changes: Vec::new(),
        shared: BTreeMap::new(),
        errors: Vec::new(),
        fingerprint: String::new(),
    };
    let refs = match resolve(rt, instance, definition, &ArgsMap::new()) {
        Ok(refs) => refs,
        Err(err) => {
            analysis.errors.push(err.message);
            Vec::new()
        }
    };
    let expected: BTreeSet<Association> = refs
        .iter()
        .map(|reference| Association {
            pipe_ref: reference.clone(),
            tag: format!("pipeline-{}", definition.name),
        })
        .collect();
    let previous: BTreeSet<Association> = definition.tag_associations.iter().cloned().collect();
    let mut snapshots = BTreeMap::new();
    for association in expected.union(&previous) {
        match member(rt, &association.pipe_ref) {
            Ok((_, pipe)) => {
                let params: Json = serde_json::from_str(
                    &Value::Map(pipe.raw_parameters().clone()).to_json_string(),
                )
                .map_err(error)?;
                let tags = params["tags"].as_array().cloned().unwrap_or_default();
                let provenance = params["_pipeline_tags"].clone();
                if let Some(entries) = provenance.as_object() {
                    for (tag, entry) in entries {
                        for owner in entry["owners"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Json::as_str)
                            .filter(|owner| *owner != definition.id)
                        {
                            analysis
                                .shared
                                .entry(association.pipe_ref.clone())
                                .or_default()
                                .push(format!("{tag} (owner {owner})"));
                        }
                    }
                }
                let owners = provenance[&association.tag]["owners"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                snapshots.insert(
                    format!("{}\n{}", association.pipe_ref, association.tag),
                    json!({"tags":tags,"provenance":provenance}),
                );
                let owned = owners.contains(&json!(definition.id));
                let action = if expected.contains(association)
                    && (!tags.contains(&json!(association.tag)) || !owned)
                {
                    Some("add")
                } else if !expected.contains(association) && owned {
                    Some("remove")
                } else {
                    None
                };
                if let Some(action) = action {
                    analysis.changes.push(Change {
                        pipe_ref: association.pipe_ref.clone(),
                        tag: association.tag.clone(),
                        action: action.into(),
                    });
                }
            }
            Err(err) => analysis.errors.push(err.message),
        }
    }
    for reference in &refs {
        analysis.resolved.push(json!({"pipe_ref":reference,"reason":if definition.targets.contains(reference) {"explicit member"} else {"configured traversal"}}));
    }
    for other in list(rt, instance)? {
        if other.id == definition.id {
            continue;
        }
        for reference in &refs {
            if other.targets.contains(reference) {
                analysis
                    .shared
                    .entry(reference.clone())
                    .or_default()
                    .push(other.name.clone());
            }
        }
    }
    for owners in analysis.shared.values_mut() {
        owners.sort();
        owners.dedup();
    }
    if !analysis.errors.is_empty() {
        analysis.changes.retain(|c| c.action != "remove");
    }
    analysis.fingerprint = Sha256::digest(json!({"definition":definition,"resolved":refs,"snapshots":snapshots,"errors":analysis.errors}).to_string().as_bytes()).iter().map(|b| format!("{b:02x}")).collect();
    Ok(analysis)
}

pub(super) fn change_tag(
    rt: &mut Runtime<'_>,
    reference: &str,
    tag: &str,
    owner: &str,
    add: bool,
) -> ActionResult {
    let lock_pipe = parse_ref(rt, reference)?;
    let lock_instance = session(rt).instance(lock_pipe.instance_keys(), rt.config())?;
    let lease = MutationLease::acquire(session(rt), lock_instance, lock_pipe)?;
    let (instance, mut pipe) = member(rt, reference)?;
    let mut params: Json =
        serde_json::from_str(&Value::Map(pipe.raw_parameters().clone()).to_json_string())
            .map_err(error)?;
    let mut tags = params["tags"].as_array().cloned().unwrap_or_default();
    let mut provenance = params["_pipeline_tags"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    let mut association = provenance
        .get(tag)
        .cloned()
        .unwrap_or_else(|| json!({"preexisting":tags.contains(&json!(tag)),"owners":[]}));
    let mut owners = association["owners"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if add {
        if !owners.contains(&json!(owner)) {
            owners.push(json!(owner));
        }
        if !tags.contains(&json!(tag)) {
            tags.push(json!(tag));
        }
    } else if owners.contains(&json!(owner)) {
        owners.retain(|value| value != &json!(owner));
        if owners.is_empty() && !association["preexisting"].as_bool().unwrap_or(false) {
            tags.retain(|value| value != &json!(tag));
        }
    } else {
        return Ok(SuccessTuple::success());
    }
    if owners.is_empty() {
        provenance.remove(tag);
    } else {
        association["owners"] = json!(owners);
        provenance.insert(tag.into(), association);
    }
    params["tags"] = json!(tags);
    if provenance.is_empty() {
        params.as_object_mut().unwrap().remove("_pipeline_tags");
    } else {
        params["_pipeline_tags"] = json!(provenance);
    }
    pipe.set_parameters(
        Value::from_json_str(&params.to_string())
            .map_err(error)?
            .to_map(),
    );
    lease.check()?;
    Ok(session(rt).block_on(instance.conn().edit_pipe(&pipe, false)))
}

pub(super) fn apply(rt: &mut Runtime<'_>, instance: &Instance, preview: &Analysis) -> ActionResult {
    let lease = registry_lease(rt, instance)?;
    let latest = read(rt, instance, &preview.name)?;
    let current = analyze(rt, instance, &latest)?;
    if current.fingerprint != preview.fingerprint {
        rt.print(&format!(
            "{}\n",
            serde_json::to_string_pretty(&current).map_err(error)?
        ));
        return Ok(SuccessTuple::fail(
            "Analysis changed; review and approve the refreshed proposal.",
        ));
    }
    if !current.errors.is_empty() {
        return Ok(SuccessTuple::fail(
            "Resolve analysis errors before applying tags.",
        ));
    }
    let mut definition = latest;
    let mut associations: BTreeSet<_> = definition.tag_associations.iter().cloned().collect();
    associations.extend(current.changes.iter().map(|c| Association {
        pipe_ref: c.pipe_ref.clone(),
        tag: c.tag.clone(),
    }));
    definition.tag_associations = associations.iter().cloned().collect();
    lease.check()?;
    write(rt, instance, &definition, false)?;
    let mut failures = 0;
    for change in current.changes {
        let result = change_tag(
            rt,
            &change.pipe_ref,
            &change.tag,
            &definition.id,
            change.action == "add",
        )
        .unwrap_or_else(|err| SuccessTuple::fail(err.message));
        rt.print(&format!("{}: {}\n", change.pipe_ref, result.message));
        if !result.success {
            failures += 1;
        } else if change.action == "remove" {
            associations.remove(&Association {
                pipe_ref: change.pipe_ref,
                tag: change.tag,
            });
        }
    }
    let latest = read(rt, instance, &definition.name)?;
    if latest.revision != definition.revision {
        return Ok(SuccessTuple::fail(
            "Definition changed during tag application; analyze again.",
        ));
    }
    definition.tag_associations = associations.into_iter().collect();
    lease.check()?;
    write(rt, instance, &definition, false)?;
    Ok(SuccessTuple {
        success: failures == 0,
        message: format!(
            "Membership tags updated; {failures} failed writes. Run analyze pipelines to retry failures."
        ),
    })
}
