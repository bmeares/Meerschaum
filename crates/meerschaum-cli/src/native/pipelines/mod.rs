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
use super::{
    bootstrap, editor, float, get_instance, graph_plan, info, items_str, jobs, jobs_laps, params,
    pipes, prompt, py_to_value, show, string, sync, warn, yes_no,
};

mod analysis;
mod display;
mod editing;
mod execution;
mod removal;

use analysis::{analyze, apply};
use display::{show_grid, show_summary};
use editing::save;
use execution::execute;
use removal::{destructive, referenced_jobs, remove_associations};

pub(crate) use execution::run_captured;

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

/// Keep traversal and induced dependency settings identical in plans and runs.
fn definition_selection(definition: &Definition, effective: &ArgsMap) -> Selection {
    Selection::refs(definition.targets.iter().map(PipeRef::from_string))
        .with_parents(super::sync::depth(effective, "parents"))
        .with_children(super::sync::depth(effective, "children"))
        .with_graph(definition.graph.unwrap_or(true))
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
    opts.selection = Some(definition_selection(definition, &effective));
    let (_, sub, _) = session.block_on(engine.resolve(&opts)).map_err(error)?;
    Ok(sub)
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
        "sync" | "plan" => execute(action, args, rt, &instance, None),
        _ => Err(error("Unknown Pipeline action.")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use removal::scheduled_reference;
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
