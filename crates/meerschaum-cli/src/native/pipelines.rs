//! Registered Pipeline lifecycle. Exact references never become a Cartesian key filter.
//! Definitions and tag ownership use the same versioned schema as the Python SDK.
use std::collections::{BTreeMap, BTreeSet};

use meerschaum_connector::DataQuery;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_engine::{PipeRef, Selection};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};

use super::pipes::{SelectedPipe, build_pipe, defaults};
use super::{Instance, flag, session, words};

/// A renewable backend lease shared by CLI and SDK metadata mutations. Registry leases
/// serialize definition revisions; member leases serialize tag ownership read/modify/write.
pub struct MutationLease {
    session: super::Session,
    instance: Instance,
    pipe: Pipe,
    holder: String,
    heartbeat: tokio::task::JoinHandle<()>,
}

impl MutationLease {
    pub fn acquire(
        session: super::Session,
        instance: Instance,
        pipe: Pipe,
    ) -> Result<Self, ActionError> {
        let holder = format!("pipeline-mutation-{}", uuid::Uuid::new_v4());
        let ttl = std::time::Duration::from_secs(300);
        let acquired = session.block_on(async {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            loop {
                if instance
                    .conn()
                    .acquire_pipe_lease(&pipe, &holder, ttl)
                    .await
                    .map_err(error)?
                {
                    return Ok(());
                }
                if std::time::Instant::now() >= deadline {
                    return Err(error("Pipeline metadata is being edited; retry shortly."));
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        });
        acquired?;
        let renew_instance = instance.clone();
        let renew_pipe = pipe.clone();
        let renew_holder = holder.clone();
        let heartbeat = session.handle().spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                if !renew_instance
                    .conn()
                    .acquire_pipe_lease(&renew_pipe, &renew_holder, ttl)
                    .await
                    .unwrap_or(false)
                {
                    break;
                }
            }
        });
        Ok(Self {
            session,
            instance,
            pipe,
            holder,
            heartbeat,
        })
    }

    pub fn check(&self) -> Result<(), ActionError> {
        if self
            .session
            .block_on(self.instance.conn().acquire_pipe_lease(
                &self.pipe,
                &self.holder,
                std::time::Duration::from_secs(300),
            ))
            .map_err(error)?
        {
            Ok(())
        } else {
            Err(error(
                "Lost Pipeline mutation lease; reload before retrying.",
            ))
        }
    }
}

impl Drop for MutationLease {
    fn drop(&mut self) {
        self.heartbeat.abort();
        let _ = self.session.block_on(
            self.instance
                .conn()
                .release_pipe_lease(&self.pipe, &self.holder),
        );
    }
}

fn registry_lease(rt: &mut Runtime<'_>, instance: &Instance) -> Result<MutationLease, ActionError> {
    let pipe = build_pipe(
        "mrsm",
        "pipeline_registry",
        None,
        &instance.keys(),
        None,
        &defaults(rt),
    )?;
    MutationLease::acquire(session(rt), instance.clone(), pipe)
}
use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::value::ArgsMap;

fn error(message: impl ToString) -> ActionError {
    ActionError::new("ValueError", message.to_string())
}

/// A recoverable ownership entry, including the tag's name before a rename.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Association {
    pub pipe_ref: String,
    pub tag: String,
}

/// Persisted Pipeline schema (legacy definitions are migrated in memory on reads).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Definition {
    pub schema_version: u32,
    pub id: String,
    pub revision: u64,
    pub registry: String,
    pub name: String,
    pub targets: Vec<String>,
    pub parents: Json,
    pub children: Json,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph: Option<bool>,
    pub description: String,
    pub defaults: BTreeMap<String, Json>,
    pub layout: Json,
    pub tag_associations: Vec<Association>,
}

/// Validate a name consistently with the SDK (ASCII, 1–128 bytes).
pub fn validate_name(name: &str) -> Result<(), ActionError> {
    meerschaum_core::pipeline::validate_name(name).map_err(error)
}

fn metadata(rt: &mut Runtime<'_>, instance: &Instance, name: &str) -> Result<Pipe, ActionError> {
    validate_name(name)?;
    build_pipe(
        "mrsm",
        "pipeline",
        Some(name),
        &instance.keys(),
        None,
        &defaults(rt),
    )
}

fn normalize(mut definition: Definition, name: &str, registry: &str) -> Definition {
    definition.name = name.into();
    definition.registry = registry.into();
    definition.schema_version = 2;
    if definition.id.is_empty() {
        definition.id = format!("legacy:{registry}:{name}");
    }
    definition
}

/// Read a definition without modifying the registration or any member tags.
pub fn read(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    name: &str,
) -> Result<Definition, ActionError> {
    let pipe = metadata(rt, instance, name)?;
    let attributes = session(rt)
        .block_on(instance.conn().get_pipe_attributes(&pipe))
        .map_err(error)?;
    let definition = attributes
        .get("parameters")
        .and_then(Value::as_map)
        .and_then(|p| p.get("pipeline"))
        .ok_or_else(|| {
            error(format!(
                "Pipeline '{name}' does not exist on '{}'.",
                instance.keys()
            ))
        })?;
    let definition: Definition =
        serde_json::from_str(&definition.to_json_string()).map_err(error)?;
    Ok(normalize(definition, name, &instance.keys()))
}

/// List registered selections on one registry.
pub fn list(rt: &mut Runtime<'_>, instance: &Instance) -> Result<Vec<Definition>, ActionError> {
    let selected = super::pipes::get_pipes(
        rt,
        instance,
        &super::pipes::Selection {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        },
    )?;
    let mut definitions = Vec::new();
    for selected in selected {
        if let Some(name) = selected.pipe.location_key() {
            definitions.push(read(rt, instance, name)?);
        }
    }
    definitions.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(definitions)
}

fn write(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    definition: &Definition,
    create: bool,
) -> ActionResult {
    let mut pipe = metadata(rt, instance, &definition.name)?;
    let mut parameters = Map::new();
    if !create {
        let attributes = session(rt)
            .block_on(instance.conn().get_pipe_attributes(&pipe))
            .map_err(error)?;
        if let Some(current) = attributes.get("parameters").and_then(Value::as_map) {
            for key in ["access", "locks", "compose"] {
                if let Some(value) = current.get(key) {
                    parameters.insert(key.into(), value.clone());
                }
            }
        }
    }
    parameters.insert(
        "pipeline".into(),
        Value::from_json_str(&serde_json::to_string(definition).map_err(error)?).map_err(error)?,
    );
    pipe.set_parameters(parameters);
    let result = if create {
        session(rt).block_on(instance.conn().register_pipe(&pipe))
    } else {
        session(rt).block_on(instance.conn().edit_pipe(&pipe, false))
    };
    if !result.success {
        return Err(error(result.message));
    }
    Ok(result)
}

fn parse_ref(rt: &mut Runtime<'_>, reference: &str) -> Result<Pipe, ActionError> {
    let parts: Vec<_> = reference.splitn(4, '|').collect();
    if parts.len() != 4 || parts[..3].iter().any(|p| p.is_empty()) || parts[1] == "mrsm" {
        return Err(error(format!("Invalid Pipeline member: {reference}")));
    }
    let reference = PipeRef::from_string(reference);
    let parsed = reference.parts();
    build_pipe(
        parsed.connector_keys,
        parsed.metric_key,
        parsed.location_key,
        parsed.instance_keys,
        None,
        &defaults(rt),
    )
}

fn member(rt: &mut Runtime<'_>, reference: &str) -> Result<(Instance, Pipe), ActionError> {
    let mut pipe = parse_ref(rt, reference)?;
    let session = session(rt);
    let instance = session.instance(pipe.instance_keys(), rt.config())?;
    if session
        .block_on(instance.conn().get_pipe_id(&pipe))
        .map_err(error)?
        .is_none()
    {
        return Err(error(format!(
            "Missing registered Pipeline member: {reference}"
        )));
    }
    let attributes = session
        .block_on(instance.conn().get_pipe_attributes(&pipe))
        .map_err(error)?;
    if let Some(params) = attributes.get("parameters").and_then(Value::as_map) {
        pipe.set_parameters(params.clone());
    }
    Ok((instance, pipe))
}

use meerschaum_core::pipeline::EXECUTION_DEFAULTS;
fn validate_execution_defaults(defaults: &BTreeMap<String, Json>) -> Result<(), ActionError> {
    let unknown: Vec<_> = defaults
        .keys()
        .filter(|key| !EXECUTION_DEFAULTS.contains(&key.as_str()))
        .collect();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(error(format!(
            "Unsupported Pipeline execution defaults: {unknown:?}"
        )))
    }
}

fn validate_definition(
    rt: &mut Runtime<'_>,
    definition: &mut Definition,
) -> Result<(), ActionError> {
    validate_name(&definition.name)?;
    validate_execution_defaults(&definition.defaults)?;
    definition.targets.sort();
    definition.targets.dedup();
    for reference in &definition.targets {
        parse_ref(rt, reference)?;
    }
    for depth in [&definition.parents, &definition.children] {
        if !(depth.is_null() || depth.is_boolean() || depth.as_u64().is_some()) {
            return Err(error(
                "Traversal depth must be a boolean or nonnegative integer.",
            ));
        }
    }
    Ok(())
}

fn effective_args(args: &ArgsMap, definition: &Definition) -> Result<ArgsMap, ActionError> {
    validate_execution_defaults(&definition.defaults)?;
    let mut options = args.clone();
    for (key, value) in definition.defaults.iter().chain(
        [
            ("parents".to_string(), definition.parents.clone()),
            ("children".to_string(), definition.children.clone()),
        ]
        .iter()
        .map(|(k, v)| (k, v)),
    ) {
        if !options.contains_key(key) {
            options.insert(key.clone(), crate::native_plugins::json_to_py(value));
        }
    }
    Ok(options)
}

fn selected(rt: &mut Runtime<'_>, refs: &[String]) -> Result<Vec<SelectedPipe>, ActionError> {
    refs.iter()
        .map(|reference| {
            let (_, pipe) = member(rt, reference)?;
            Ok(SelectedPipe {
                pipe,
                id: None,
                hydrated: true,
            })
        })
        .collect()
}

/// Resolve each Pipeline with its own traversal before unioning several selections.
fn resolve(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    definition: &Definition,
    args: &ArgsMap,
) -> Result<Vec<String>, ActionError> {
    let sub = resolve_graph(rt, instance, definition, args)?;
    if !sub.cycles.is_empty() {
        return Err(error(format!("Dependency cycle: {:?}", sub.cycles)));
    }
    let errors: Vec<_> = sub
        .warnings
        .iter()
        .filter(|warning| !warning.contains("crosses an integer/datetime axis; assuming"))
        .collect();
    if !errors.is_empty() {
        return Err(error(format!("Unresolved dependency graph: {errors:?}")));
    }
    let refs: Vec<_> = sub.order.iter().map(ToString::to_string).collect();
    for reference in &definition.targets {
        if !refs.contains(reference) {
            return Err(error(format!("Unresolved Pipeline member: {reference}")));
        }
    }
    // Missing instance credentials or registrations must never be silently omitted.
    for reference in &refs {
        member(rt, reference)?;
    }
    Ok(refs)
}

/// Read dependency metadata without planning a sync or applying membership tags.
fn resolve_graph(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    definition: &Definition,
    args: &ArgsMap,
) -> Result<meerschaum_engine::selection::Subgraph, ActionError> {
    if definition.targets.is_empty() {
        return Ok(Default::default());
    }
    let pipes = selected(rt, &definition.targets)?;
    let session = session(rt);
    let engine = super::sync::engine::build(rt, &session, instance, &pipes, true, false, None)?;
    let effective = effective_args(args, definition)?;
    let mut opts = super::sync::run_options(
        &effective,
        instance,
        &pipes,
        &super::sync::SyncArgs::from_args(&effective),
    );
    opts.selection = Some(
        Selection::refs(definition.targets.iter().map(PipeRef::from_string))
            .with_parents(super::sync::depth(&effective, "parents"))
            .with_children(super::sync::depth(&effective, "children"))
            .with_graph(definition.graph.unwrap_or(true)),
    );
    let (_, sub, _) = session.block_on(engine.resolve(&opts)).map_err(error)?;
    Ok(sub)
}

fn show_grid(rt: &mut Runtime<'_>, instance: &Instance, definitions: &[Definition], rows: &[Json]) {
    let text = rt.with_formatter(|formatter| {
        use crate::format::{
            PipeKeys,
            rich::{
                table::{Column, SIMPLE, Table},
                text::Text,
            },
        };
        let mut table = Table::new();
        table.expand = true;
        table.box_chars = if formatter.settings.unicode {
            Some(SIMPLE)
        } else {
            None
        };
        for label in ["Pipeline", "Members", "Latest run"] {
            table.add_column(Column {
                header: Text::plain_text(label),
                ..Default::default()
            });
        }
        for (definition, row) in definitions.iter().zip(rows) {
            let mut label = formatter.pipeline_repr_text(&definition.name, &instance.keys());
            if !definition.description.is_empty() {
                label.append_text(&Text::plain_text(&format!("\n{}", definition.description)));
            }
            let mut members = Text::plain_text(&format!(
                "{} · {}\n",
                count_noun(definition.targets.len(), "member"),
                count_noun(
                    row["instance_count"].as_u64().unwrap_or_default() as usize,
                    "instance"
                ),
            ));
            if definition.targets.is_empty() {
                members.append_text(&Text::plain_text("No member pipes."));
            }
            for reference in &definition.targets {
                let parts: Vec<_> = reference.splitn(4, '|').collect();
                if parts.len() != 4 {
                    continue;
                }
                let keys = PipeKeys {
                    connector: parts[1].into(),
                    metric: parts[2].into(),
                    location: if parts[3] == "None" {
                        None
                    } else {
                        Some(parts[3].into())
                    },
                    instance: Some(parts[0].into()),
                };
                members.append_text(&formatter.pipe_repr_text(&keys));
                members.append_text(&Text::plain_text("\n"));
            }
            let status = row["latest_run"]["status"]
                .as_str()
                .unwrap_or("No runs yet");
            let (symbol, color) = match status {
                "running" => ("▶", "cyan"),
                "succeeded" => ("✔", "green"),
                "failed" => ("✘", "red"),
                _ => ("○", "dim"),
            };
            let status = format!(
                "{} {status}",
                if formatter.settings.unicode {
                    symbol
                } else {
                    "•"
                }
            );
            table.add_row(vec![
                Some(Box::new(label)),
                Some(Box::new(members)),
                Some(Box::new(Text::new(
                    &status,
                    if formatter.settings.ansi { color } else { "" },
                ))),
            ]);
        }
        formatter.console().print(&table)
    });
    rt.print(&text);
}

/// `"1 member"` / `"2 members"`.
fn count_noun(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("{count} {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

fn show_summary(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    definition: &Definition,
    row: &Json,
    args: &ArgsMap,
    detail: bool,
) {
    let _ = args;
    let status = row["latest_run"]["status"]
        .as_str()
        .unwrap_or("No runs yet");
    let text = rt.with_formatter(|formatter| {
        use crate::format::rich::text::Text;
        let symbol = match (status, formatter.settings.unicode) {
            ("running", true) => "▶",
            ("succeeded", true) => "✔",
            ("failed", true) => "✘",
            ("running", false) => ">",
            ("succeeded", false) => "+",
            ("failed", false) => "x",
            (_, true) => "○",
            _ => "-",
        };
        let color = if formatter.settings.ansi {
            match status {
                "succeeded" => "green",
                "failed" => "red",
                "running" => "cyan",
                _ => "dim",
            }
        } else {
            ""
        };
        let mut text = Text::plain_text(&format!("{symbol} "));
        if !color.is_empty() {
            text.stylize(color, 0, None);
        }
        text.append_text(&formatter.pipeline_repr_text(&definition.name, &instance.keys()));
        text.append_text(&Text::plain_text(&format!(
            "  · {} · {} · ",
            count_noun(definition.targets.len(), "member"),
            count_noun(
                row["instance_count"].as_u64().unwrap_or_default() as usize,
                "instance"
            ),
        )));
        text.append_text(&Text::new(status, color));
        formatter.text_to_str(&text)
    });
    rt.print(&format!("{text}\n"));
    if !definition.description.is_empty() {
        rt.print(&format!("  {}\n", definition.description));
    }
    if detail {
        rt.print(&format!(
            "  ID: {}\n  Revision: {}\n  Parents: {} | Children: {}\n",
            definition.id, definition.revision, definition.parents, definition.children
        ));
        for reference in &definition.targets {
            rt.print(&format!("  • {reference}\n"));
        }
    }
    if definition.targets.is_empty() {
        rt.print("  No member pipes.\n");
    }
    if let Some(message) = row["status_error"].as_str() {
        rt.print(&format!("  Run history unavailable: {message}\n"));
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Change {
    pipe_ref: String,
    tag: String,
    action: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Analysis {
    name: String,
    registry: String,
    definition: Definition,
    revision: u64,
    resolved: Vec<Json>,
    changes: Vec<Change>,
    shared: BTreeMap<String, Vec<String>>,
    errors: Vec<String>,
    fingerprint: String,
}

fn analyze(
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

fn change_tag(
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

fn apply(rt: &mut Runtime<'_>, instance: &Instance, preview: &Analysis) -> ActionResult {
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

fn scheduled_reference(
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

fn referenced_jobs(
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

fn remove_associations(rt: &mut Runtime<'_>, definition: &Definition) -> Result<(), ActionError> {
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

fn confirm(
    rt: &mut Runtime<'_>,
    args: &ArgsMap,
    message: &str,
    force: bool,
) -> Result<bool, ActionError> {
    if flag(args, "yes") || (force && flag(args, "force")) {
        return Ok(true);
    }
    if !rt.live || super::check_noask(rt, flag(args, "noask")) {
        return Ok(false);
    }
    super::yes_no(rt, message, "n", false, false)
}

fn names(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    args: &ArgsMap,
    all: bool,
) -> Result<Vec<Definition>, ActionError> {
    let requested = words(args, "action");
    if requested.is_empty() && all {
        return list(rt, instance);
    }
    if requested.is_empty() {
        return Err(error("Provide a registered Pipeline name."));
    }
    let mut available: BTreeMap<_, _> = list(rt, instance)?
        .into_iter()
        .map(|d| (d.name.clone(), d))
        .collect();
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
    for name in requested {
        if !seen.insert(name.clone()) {
            continue;
        }
        if let Some(definition) = available.remove(&name) {
            selected.push(definition);
        } else {
            super::warn(
                rt,
                &format!(
                    "Pipeline '{name}' does not exist on '{}'; skipping.",
                    instance.keys()
                ),
            );
        }
    }
    if selected.is_empty() {
        return Err(error("No valid Pipeline names were selected."));
    }
    Ok(selected)
}

/// All lifecycle entry points share exact-reference storage and preview semantics.
pub fn run(action: &str, args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let instance = super::get_instance(args, rt)?;
    let lease = if ["register", "bootstrap", "edit", "copy", "delete"].contains(&action) {
        Some(registry_lease(rt, &instance)?)
    } else {
        None
    };
    match action {
        "show" => {
            let definitions = names(rt, &instance, args, true)?;
            // Machine-readable output reads only the saved state and registry history.
            let registry = meerschaum_engine::registry::StaticRegistry::new()
                .with_instance(&instance.keys(), instance.conn());
            let root_dir = rt.config().paths().root_dir;
            let mut config =
                meerschaum_engine::EngineConfig::from_config(rt.config(), Some(&root_dir));
            config.home_instance = instance.keys();
            let reader =
                meerschaum_engine::EngineReader::new(std::sync::Arc::new(registry), config);
            let history = session(rt).block_on(reader.runs(&meerschaum_engine::RunFilter {
                limit: Some(100),
                ..Default::default()
            }));
            let rows: Vec<Json> = definitions.iter().map(|definition| {
                let mut row = serde_json::to_value(definition).unwrap();
                let instances: BTreeSet<_> = definition.targets.iter().map(|reference| reference.split('|').next().unwrap_or("")).collect();
                row["member_count"] = json!(definition.targets.len());
                row["instance_count"] = json!(instances.len());
                row["instances"] = json!(instances);
                row["latest_run"] = match &history {
                    Ok(runs) => runs.iter().find(|run| {
                        let snapshots = run.run.pipeline_definitions();
                        if snapshots.is_empty() { run.run.belongs_to_pipeline(&definition.name) }
                        else { snapshots.iter().any(|snapshot| snapshot["id"].as_str() == Some(&definition.id)) }
                    }).map(|run| json!({"run_id":run.run.run_id,"status":run.status,"started":run.run.started,"finished":run.run.finished,"duration_seconds":run.duration_seconds})).unwrap_or(Json::Null),
                    Err(err) => { row["status_error"] = json!(err.to_string()); Json::Null },
                };
                row
            }).collect();
            if flag(args, "nopretty") {
                rt.print(&format!(
                    "{}\n",
                    serde_json::to_string_pretty(&rows).map_err(error)?
                ));
            } else {
                if definitions.is_empty() {
                    rt.print(&format!(
                        "No pipelines registered on {}.\n",
                        instance.keys()
                    ));
                }
                let detail = definitions.len() == 1 && !words(args, "action").is_empty();
                if detail {
                    show_summary(rt, &instance, &definitions[0], &rows[0], args, true);
                } else if !definitions.is_empty() {
                    show_grid(rt, &instance, &definitions, &rows);
                }
                if !definitions.is_empty() {
                    let key = rt.with_formatter(|formatter| {
                        use crate::format::rich::{panel::Panel, text::Text};
                        let legend = if formatter.settings.unicode {
                            "▶ running   ✔ succeeded   ✘ failed   ○ no runs"
                        } else {
                            "> running   + succeeded   x failed   - no runs"
                        };
                        let mut panel = Panel::new(Box::new(Text::plain_text(legend)));
                        panel.title = Some(Text::plain_text("Keys"));
                        if !formatter.settings.unicode {
                            panel.box_chars = crate::format::rich::table::ASCII;
                        }
                        formatter.console().print(&panel)
                    });
                    rt.print(&key);
                    rt.print("Run analyze pipelines NAME to inspect traversal and approve membership tags.\n");
                }
            }
            Ok(SuccessTuple::success())
        }
        "analyze" => {
            let definitions = names(rt, &instance, args, true)?;
            let mut success = true;
            for definition in definitions {
                let preview = analyze(rt, &instance, &definition)?;
                rt.print(&format!(
                    "{}\n",
                    serde_json::to_string_pretty(&preview).map_err(error)?
                ));
                if !preview.errors.is_empty() {
                    success = false;
                    continue;
                }
                if !preview.changes.is_empty()
                    && confirm(
                        rt,
                        args,
                        "Apply these Pipeline membership tag changes?",
                        false,
                    )?
                {
                    let result = apply(rt, &instance, &preview)?;
                    success &= result.success;
                    rt.print(&format!("{}\n", result.message));
                }
            }
            Ok(SuccessTuple {
                success,
                message: "Pipeline analysis complete.".into(),
            })
        }
        "edit" => {
            let definitions = names(rt, &instance, args, true)?;
            if definitions.is_empty() {
                return Ok(SuccessTuple::fail("No Pipelines to edit."));
            }
            for definition in definitions {
                let mut selected = args.clone();
                selected.insert(
                    "action".into(),
                    crate::value::PyValue::str_list(&[definition.name]),
                );
                let result = save(action, &selected, rt, &instance, lease.as_ref().unwrap())?;
                if !result.success {
                    return Ok(result);
                }
            }
            Ok(SuccessTuple::success())
        }
        "register" | "bootstrap" | "copy" => {
            save(action, args, rt, &instance, lease.as_ref().unwrap())
        }
        "delete" => {
            let definitions = names(rt, &instance, args, true)?;
            if definitions.is_empty() {
                return Ok(SuccessTuple::fail("No Pipelines to delete."));
            }
            for definition in &definitions {
                let jobs = referenced_jobs(rt, &instance, &definition.name)?;
                if !jobs.is_empty() {
                    return Err(error(format!(
                        "Remove or reassign scheduled Jobs first: {}",
                        jobs.join(", ")
                    )));
                }
            }
            rt.print(&format!(
                "Delete definitions only (preserve Pipe registrations/data/history): {}\n",
                definitions
                    .iter()
                    .map(|d| d.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            if !confirm(
                rt,
                args,
                "Delete these Pipeline definitions and their owned tag associations?",
                true,
            )? {
                return Ok(SuccessTuple::fail("No Pipeline definitions deleted."));
            }
            for definition in definitions {
                if read(rt, &instance, &definition.name)?.revision != definition.revision {
                    return Err(error("Definition changed; retry deletion."));
                }
                remove_associations(rt, &definition)?;
                lease.as_ref().unwrap().check()?;
                let pipe = metadata(rt, &instance, &definition.name)?;
                let result = session(rt).block_on(instance.conn().delete_pipe(&pipe));
                if !result.success {
                    return Ok(result);
                }
            }
            Ok(SuccessTuple::success())
        }
        "drop" | "clear" => destructive(action, args, rt, &instance),
        "sync" | "plan" => execute(action, args, rt, &instance),
        _ => Err(error("Unknown Pipeline action.")),
    }
}

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

fn save(
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

fn destructive(
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

/// The saved Pipelines a command names, resolved and ready to run.
struct PreparedRun {
    /// Their definitions, as read now.
    definitions: Vec<Definition>,
    /// The pipes of their selections.
    pipes: Vec<SelectedPipe>,
    /// The engine over those pipes.
    engine: meerschaum_engine::Engine,
    /// The run's options (the exact selection and the definitions it runs).
    options: meerschaum_engine::RunOptions,
    /// The command's arguments with the first Pipeline's execution defaults filled in.
    effective: ArgsMap,
}

/// Read and resolve the Pipelines `args` name (`None`: they select no pipes). `continuous`: for
/// the engine's continuous mode, whose stream sources get the loop's options (and `--listen`
/// keeps v4-pattern connectors subscribed).
fn prepare_run(
    action: &str,
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    instance: &Instance,
    continuous: bool,
) -> Result<Option<PreparedRun>, ActionError> {
    let definitions = names(rt, instance, args, true)?;
    let graph = definitions[0].graph.unwrap_or(true);
    if definitions
        .iter()
        .any(|definition| definition.graph.unwrap_or(true) != graph)
    {
        return Err(error(
            "Combined Pipelines have conflicting graph settings; run them separately.",
        ));
    }
    let default_keys: BTreeSet<_> = definitions
        .iter()
        .flat_map(|definition| definition.defaults.keys())
        .collect();
    for key in default_keys {
        if !args.contains_key(key)
            && definitions
                .iter()
                .skip(1)
                .any(|definition| definition.defaults.get(key) != definitions[0].defaults.get(key))
        {
            return Err(error(format!(
                "Combined Pipelines have conflicting execution default '{key}'; provide an explicit --{} override.",
                key.replace('_', "-")
            )));
        }
    }
    let mut refs = BTreeSet::new();
    for definition in &definitions {
        refs.extend(resolve(rt, instance, definition, args)?);
    }
    if refs.is_empty() {
        return Ok(None);
    }
    let refs: Vec<_> = refs.into_iter().collect();
    let pipes = selected(rt, &refs)?;
    let session = session(rt);
    if action != "plan" {
        super::sync::prepare_live(rt, args);
    }
    let effective = effective_args(args, &definitions[0])?;
    let (listen, stream_kwargs) = if continuous {
        (
            flag(&effective, "listen"),
            Some(super::sync::stream_kwargs(&effective)),
        )
    } else {
        (false, None)
    };
    let engine =
        super::sync::engine::build(rt, &session, instance, &pipes, true, listen, stream_kwargs)?;
    let mut options = super::sync::run_options(
        &effective,
        instance,
        &pipes,
        &super::sync::SyncArgs::from_args(&effective),
    );
    options.selection =
        Some(Selection::refs(refs.iter().map(PipeRef::from_string)).with_graph(graph));
    options.pipeline_definitions = definitions
        .iter()
        .map(|d| serde_json::to_value(d).unwrap())
        .collect();
    Ok(Some(PreparedRun {
        definitions,
        pipes,
        engine,
        options,
        effective,
    }))
}

/// The name a run of `definitions` is given (`daily-2026-...`; `combined` for several).
fn run_name(definitions: &[Definition]) -> &str {
    if definitions.len() == 1 {
        definitions[0].name.as_str()
    } else {
        "combined"
    }
}

fn execute(
    action: &str,
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    instance: &Instance,
) -> ActionResult {
    if action == "sync" && (flag(args, "loop") || flag(args, "listen")) {
        return execute_loop(args, rt, instance);
    }
    let Some(PreparedRun {
        definitions,
        engine,
        mut options,
        ..
    }) = prepare_run(action, args, rt, instance, false)?
    else {
        return Ok(SuccessTuple::fail("Pipeline has no target Pipes."));
    };
    let session = session(rt);
    if action == "plan" {
        let plan = session.block_on(engine.plan(&options)).map_err(error)?;
        if flag(args, "nopretty") {
            rt.print(&format!(
                "{}\n",
                serde_json::to_string(&plan.plan).map_err(error)?
            ));
        } else {
            rt.print(&format!("{}", plan.plan));
        }
        return Ok(SuccessTuple::success());
    }
    let run_id = options
        .run_id
        .clone()
        .unwrap_or_else(|| meerschaum_engine::runs::new_named_run_id(run_name(&definitions)));
    rt.print(&format!("Run: {run_id}\n"));
    options.run_id = Some(run_id);
    // Each member's block prints as it ends (a job's log too), then the totals.
    let view = super::sync::LiveView::Nodes { instance, args };
    let (result, summary) =
        super::sync::run_reported(rt, &session, engine, options, view, flag(args, "nopretty"));
    for warning in result.warnings {
        super::warn(rt, &warning);
    }
    Ok(SuccessTuple {
        success: result.success,
        message: format!("{}{summary}", result.message),
    })
}

/// Each Pipeline's identity and revision: what an edit changes.
fn revisions(definitions: &[Definition]) -> Vec<(String, String, u64)> {
    definitions
        .iter()
        .map(|d| (d.name.clone(), d.id.clone(), d.revision))
        .collect()
}

/// `sync pipelines NAME --loop` (what `start pipeline NAME` runs) and `--listen`: the engine's
/// continuous mode over the Pipelines' members, as `sync pipes --parents --loop` runs, so a
/// member with a `schedule` syncs when it is due and a lap with nothing due records no run.
/// The loop reads the Pipelines again every `RELOAD_CHECK`: after an edit (a new revision), it
/// stops after its current lap and starts again over the new members. An edit it cannot run (a
/// member that is not registered, an unreachable instance) is retried like a failing lap.
fn execute_loop(args: &ArgsMap, rt: &mut Runtime<'_>, instance: &Instance) -> ActionResult {
    let session = session(rt);
    super::jobs::install_stop_handler(&session);
    let signal = super::jobs::stop_requested_flag();
    let mut heartbeat: Option<meerschaum_jobs::Heartbeat> = None;
    let mut retries =
        meerschaum_jobs::LapPacer::new(super::float(args, "min_seconds").unwrap_or(1.0));
    loop {
        let run = match prepare_run("sync", args, rt, instance, true) {
            Ok(Some(run)) => run,
            Ok(None) => return Ok(SuccessTuple::fail("Pipeline has no target Pipes.")),
            // The first start fails as `sync pipes --loop` does; a reload keeps the job alive.
            Err(e) if heartbeat.is_some() => {
                let pause = retries
                    .after_lap(std::time::Instant::now(), false)
                    .duration();
                if let Some(heartbeat) = heartbeat.as_mut() {
                    heartbeat.lap_finished(false, &e.message);
                    heartbeat.waiting(pause);
                }
                super::warn(
                    rt,
                    &format!(
                        "Cannot sync the edited Pipeline: {}\n    Trying again in {}.",
                        e.message,
                        meerschaum_jobs::health::humanize_seconds(pause.as_secs_f64())
                    ),
                );
                // Another edit (a fix) is tried at once; else the pause runs out.
                let failed = names(rt, instance, args, true).ok().map(|d| revisions(&d));
                let deadline = std::time::Instant::now()
                    .checked_add(pause)
                    .unwrap_or_else(|| std::time::Instant::now() + super::sync::RELOAD_CHECK);
                loop {
                    let step = deadline
                        .saturating_duration_since(std::time::Instant::now())
                        .min(super::sync::RELOAD_CHECK);
                    if step.is_zero() {
                        break;
                    }
                    if super::jobs::sleep_unless_stopped(step.as_secs_f64()) {
                        return Ok(SuccessTuple::fail(e.message));
                    }
                    let now = names(rt, instance, args, true).ok().map(|d| revisions(&d));
                    if now.is_some() && now != failed {
                        break;
                    }
                }
                continue;
            }
            Err(e) => return Err(e),
        };
        retries.after_lap(std::time::Instant::now(), true);
        // Job health: the pace of the members that start the graph (their own schedules).
        let roots: BTreeSet<PipeRef> = match session.block_on(run.engine.resolve(&run.options)) {
            Ok((_, sub, _)) => sub
                .order
                .iter()
                .filter(|r| sub.flat || sub.parent_edges(r).next().is_none())
                .cloned()
                .collect(),
            Err(_) => BTreeSet::new(),
        };
        let (interval, stall) = super::jobs_laps::sync_cadence(
            &run.effective,
            run.pipes
                .iter()
                .map(|sp| &sp.pipe)
                .filter(|pipe| roots.is_empty() || roots.contains(&PipeRef::from_pipe(pipe))),
        );
        let heartbeat =
            heartbeat.get_or_insert_with(|| super::jobs_laps::heartbeat(interval, "sync"));
        heartbeat.set_interval(interval);
        heartbeat.set_stall_seconds(stall);
        let loaded = revisions(&run.definitions);
        let mut reload = |rt: &mut Runtime<'_>| -> bool {
            // An unreadable registry keeps the members the loop has.
            names(rt, instance, args, true).is_ok_and(|now| revisions(&now) != loaded)
        };
        let end = super::sync::run_continuous(
            &run.effective,
            rt,
            &session,
            instance,
            run.engine,
            run.options,
            heartbeat,
            super::sync::LoopExtras {
                run_name: Some(run_name(&run.definitions).to_string()),
                reload: Some(&mut reload),
            },
        )?;
        if !end.reload || signal.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(end.result);
        }
        let changed: Vec<String> = run.definitions.iter().map(|d| d.name.clone()).collect();
        super::info(
            rt,
            &format!(
                "Pipeline{} {} {} edited: reloading {} members.",
                if changed.len() == 1 { "" } else { "s" },
                super::items_str(&changed),
                if changed.len() == 1 { "was" } else { "were" },
                if changed.len() == 1 { "its" } else { "their" },
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scheduled_chains_resolve_each_instance() {
        let args: Vec<_> = [
            "sync",
            "pipelines",
            "A",
            "-i",
            "sql:a",
            "+",
            "sync",
            "pipelines",
            "B",
            "-i",
            "sql:b",
            "--schedule",
            "daily",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        let dates = crate::types::DateContext::system();
        assert!(scheduled_reference(&args, "A", "sql:a", &dates).unwrap());
        assert!(scheduled_reference(&args, "B", "sql:b", &dates).unwrap());
        assert!(!scheduled_reference(&args, "B", "sql:a", &dates).unwrap());
        assert!(!scheduled_reference(&args, "A", "sql:b", &dates).unwrap());
    }
    #[test]
    fn defaults_cannot_override_selection_or_identity() {
        for key in [
            "parents",
            "children",
            "targets",
            "instance",
            "run_id",
            "pipeline_definitions",
        ] {
            assert!(
                validate_execution_defaults(&BTreeMap::from([(key.into(), json!(true))])).is_err()
            );
        }
        assert!(
            validate_execution_defaults(&BTreeMap::from([
                ("begin".into(), json!(10)),
                ("workers".into(), json!(2))
            ]))
            .is_ok()
        );
    }
}
