//! Declarative Pipeline membership. References are exact; Pipe parameters own graph edges.

use crate::error::ComposeError;
use crate::pipes::ProjectConfig;
use crate::project::Project;
use meerschaum_core::pipeline::{EXECUTION_DEFAULTS, validate_name};
use meerschaum_core::{Map, Pipe, Value};
use std::collections::BTreeSet;

/// One resolved declaration, without generated registry identity or revision.
#[derive(Debug, Clone, PartialEq)]
pub struct PipelineSpec {
    /// Instance-local name.
    pub name: String,
    /// Registry instance, independent of member instances.
    pub instance: String,
    /// Authored fields in the saved Pipeline schema.
    pub definition: Map,
}

impl PipelineSpec {
    /// Internal registration used by every saved Pipeline client.
    pub fn registration(&self) -> Pipe {
        Pipe::builder("mrsm", "pipeline")
            .location(Some(self.name.as_str()))
            .instance(&self.instance)
            .build()
            .expect("validated Pipeline keys")
    }

    /// Check ownership and return the current revision, without modifying metadata.
    pub fn check_owner(
        &self,
        parameters: Option<&Map>,
        project: &str,
    ) -> Result<Option<u64>, String> {
        let Some(parameters) = parameters else {
            return Ok(None);
        };
        if parameters
            .get("compose")
            .and_then(|v| v.get("project_name"))
            .and_then(Value::as_str)
            != Some(project)
        {
            return Err(format!(
                "Pipeline '{}' on '{}' is not owned by Compose project '{project}'. Rename the declaration or use the owning project.",
                self.name, self.instance
            ));
        }
        let definition = parameters
            .get("pipeline")
            .and_then(Value::as_map)
            .ok_or_else(|| format!("Invalid saved Pipeline '{}'.", self.name))?;
        if !self
            .definition
            .iter()
            .all(|(key, value)| definition.get(key) == Some(value))
        {
            let locks = meerschaum_core::access::Locks::from_parameters(parameters);
            if let Some(refusal) = meerschaum_core::access::lock_refusal(
                &self.registration(),
                &locks,
                meerschaum_core::access::LockOp::EditParameters,
            ) {
                return Err(refusal);
            }
        }
        let revision = definition
            .get("revision")
            .and_then(Value::as_i64)
            .filter(|n| *n >= 0)
            .ok_or_else(|| format!("Invalid revision for Pipeline '{}'.", self.name))?;
        Ok(Some(revision as u64))
    }

    /// Build a compare-and-swap update while preserving access, layout and tag provenance.
    /// The caller holds the shared Pipeline registry lease through reading and writing.
    pub fn registration_parameters(
        &self,
        previous: Option<&Map>,
        project: &str,
        expected: Option<u64>,
        new_id: &str,
    ) -> Result<Option<Map>, String> {
        let revision = self.check_owner(previous, project)?;
        if revision != expected {
            return Err(format!(
                "Pipeline '{}' changed while Compose was applying it; retry.",
                self.name
            ));
        }
        let mut parameters = previous.cloned().unwrap_or_default();
        let mut saved = parameters
            .get("pipeline")
            .and_then(Value::as_map)
            .cloned()
            .unwrap_or_default();
        if previous.is_some()
            && self
                .definition
                .iter()
                .all(|(key, value)| saved.get(key) == Some(value))
        {
            return Ok(None);
        }
        for (key, value) in &self.definition {
            saved.insert(key.clone(), value.clone());
        }
        saved.insert("schema_version".into(), Value::Int(2));
        saved.insert("name".into(), Value::from(self.name.clone()));
        saved.insert("registry".into(), Value::from(self.instance.clone()));
        saved
            .entry("id".into())
            .or_insert_with(|| Value::from(new_id));
        saved.insert(
            "revision".into(),
            Value::Int(
                revision
                    .unwrap_or(0)
                    .checked_add(1)
                    .and_then(|n| i64::try_from(n).ok())
                    .ok_or("Pipeline revision overflow.")?,
            ),
        );
        saved
            .entry("layout".into())
            .or_insert_with(|| Value::Map(Map::new()));
        saved
            .entry("tag_associations".into())
            .or_insert_with(|| Value::List(Vec::new()));
        parameters.insert("pipeline".into(), Value::Map(saved));
        parameters.insert(
            "compose".into(),
            Value::Map([("project_name".into(), Value::from(project))].into()),
        );
        Ok(Some(parameters))
    }
}

fn invalid(path: &str, message: &str) -> ComposeError {
    ComposeError::value(format!("{path}: {message}"))
}

/// Resolve explicit member keys without reading a registry or expanding a live filter.
pub fn defined(
    project: &Project,
    config: &ProjectConfig,
) -> Result<Vec<PipelineSpec>, ComposeError> {
    let Some(raw) = project.config.get("pipelines") else {
        return Ok(Vec::new());
    };
    let declarations = raw
        .as_map()
        .ok_or_else(|| invalid("pipelines", "expected a mapping of names to definitions"))?;
    let default_instance = config
        .get(&["meerschaum", "instance"])
        .map(|v| meerschaum_core::pyfmt::py_str(&v))
        .unwrap_or_else(|| config.pipe_defaults().instance_keys.clone());
    let mut output = Vec::new();
    for (name, raw) in declarations {
        let path = format!("pipelines.{name}");
        validate_name(name).map_err(|e| invalid(&path, &e))?;
        let spec = raw
            .as_map()
            .ok_or_else(|| invalid(&path, "expected a mapping"))?;
        for key in spec.keys() {
            if ![
                "instance",
                "description",
                "pipes",
                "parents",
                "children",
                "defaults",
            ]
            .contains(&key.as_str())
            {
                return Err(invalid(&format!("{path}.{key}"), "unknown Pipeline field"));
            }
        }
        let instance = match spec.get("instance") {
            None => default_instance.clone(),
            Some(Value::Str(s)) if !s.is_empty() && !s.contains('|') => s.clone(),
            _ => {
                return Err(invalid(
                    &format!("{path}.instance"),
                    "expected instance connector keys",
                ));
            }
        };
        let instance = Pipe::builder("mrsm", "pipeline")
            .location(Some(name.as_str()))
            .instance(&instance)
            .build()
            .map_err(|e| invalid(&path, &e.to_string()))?
            .instance_keys()
            .to_string();
        let members = spec
            .get("pipes")
            .and_then(Value::as_list)
            .filter(|p| !p.is_empty())
            .ok_or_else(|| {
                invalid(
                    &format!("{path}.pipes"),
                    "expected a nonempty list of exact Pipe keys",
                )
            })?;
        let mut targets = BTreeSet::new();
        for (index, raw) in members.iter().enumerate() {
            let location = format!("{path}.pipes[{index}]");
            let member = raw.as_map().ok_or_else(|| {
                invalid(
                    &location,
                    "expected connector, metric, optional location and instance",
                )
            })?;
            for key in member.keys() {
                if !["connector", "metric", "location", "instance"].contains(&key.as_str()) {
                    return Err(invalid(
                        &format!("{location}.{key}"),
                        "membership contains keys only; define parameters under pipes",
                    ));
                }
            }
            let text = |key: &str, fallback: Option<&str>| -> Result<String, ComposeError> {
                match member.get(key) {
                    None if fallback.is_some() => Ok(fallback.unwrap().into()),
                    Some(Value::Str(s)) if !s.is_empty() && !s.contains('|') => Ok(s.clone()),
                    _ => Err(invalid(
                        &format!("{location}.{key}"),
                        "expected a nonempty string without '|'",
                    )),
                }
            };
            let connector = text("connector", None)?;
            let metric = text("metric", None)?;
            if connector == "mrsm" {
                return Err(invalid(
                    &location,
                    "internal metadata Pipes cannot be Pipeline members",
                ));
            }
            let instance = text("instance", Some(&default_instance))?;
            let key = match member.get("location") {
                None | Some(Value::Null) => "None".to_string(),
                Some(Value::Str(s)) if !s.is_empty() && !s.contains('|') => s.clone(),
                _ => {
                    return Err(invalid(
                        &format!("{location}.location"),
                        "expected a string or null",
                    ));
                }
            };
            let pipe = Pipe::builder(&connector, &metric)
                .location((key != "None").then_some(key.as_str()))
                .instance(&instance)
                .build()
                .map_err(|e| invalid(&location, &e.to_string()))?;
            let reference = format!(
                "{}|{}|{}|{}",
                pipe.instance_keys(),
                pipe.connector_keys(),
                pipe.metric_key(),
                pipe.location_key().unwrap_or("None")
            );
            if !targets.insert(reference.clone()) {
                return Err(invalid(&location, &format!("duplicate member {reference}")));
            }
        }
        let mut definition = Map::new();
        definition.insert(
            "targets".into(),
            Value::List(targets.into_iter().map(Value::from).collect()),
        );
        for key in ["parents", "children"] {
            let value = spec.get(key).cloned().unwrap_or(Value::Bool(false));
            if !matches!(value, Value::Bool(_) | Value::Int(0..)) {
                return Err(invalid(
                    &format!("{path}.{key}"),
                    "expected a boolean or nonnegative traversal depth",
                ));
            }
            definition.insert(key.into(), value);
        }
        let description = spec
            .get("description")
            .cloned()
            .unwrap_or_else(|| Value::from(""));
        if !matches!(description, Value::Str(_)) {
            return Err(invalid(&format!("{path}.description"), "expected a string"));
        }
        definition.insert("description".into(), description);
        let defaults = match spec.get("defaults") {
            None => Map::new(),
            Some(Value::Map(m)) => m.clone(),
            _ => return Err(invalid(&format!("{path}.defaults"), "expected a mapping")),
        };
        for key in defaults.keys() {
            if !EXECUTION_DEFAULTS.contains(&key.as_str()) {
                return Err(invalid(
                    &format!("{path}.defaults.{key}"),
                    "unsupported execution default; keep schedules in jobs",
                ));
            }
        }
        for (key, value) in &defaults {
            let valid = match key.as_str() {
                "workers" | "retries" => matches!(value, Value::Int(0..)),
                "chunksize" => matches!(value, Value::Null | Value::Int(-1..)),
                "min_seconds" => {
                    !matches!(value, Value::Bool(_))
                        && value.as_f64().is_some_and(|n| n.is_finite() && n >= 0.0)
                }
                "params" => matches!(value, Value::Map(_)),
                "begin" | "end" => matches!(value, Value::Null | Value::Str(_) | Value::Int(_)),
                _ => matches!(value, Value::Bool(_)),
            };
            if !valid {
                return Err(invalid(
                    &format!("{path}.defaults.{key}"),
                    "invalid execution default type or range",
                ));
            }
        }
        definition.insert("defaults".into(), Value::Map(defaults));
        output.push(PipelineSpec {
            name: name.clone(),
            instance,
            definition,
        });
    }
    Ok(output)
}
