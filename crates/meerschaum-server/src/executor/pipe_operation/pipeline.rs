//! Main-owned Pipeline sync admission for an explicit registered set of native roots.
use super::{
    CapturedGraph, GraphEntry, GraphOperation, GraphOrigin, GraphRequest, NativeSelection,
    PipelineIdentity, Snapshot,
};
use crate::{
    AppState,
    access::{Access, guard},
    error::ApiError,
    executor::plugin_caller::RequestSource,
    routes::pipelines,
};
use meerschaum_core::{Pipe, Value};
use serde_json::Value as Json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

fn refused() -> ApiError {
    ApiError::new(
        403,
        "This Pipeline needs its exact registered identity, targets and accepted native options.",
    )
}
const SCOPES: &[&str] = &["jobs:execute", "pipes:write"];

/// Supported definitions use the native graph order over their explicitly registered targets.
pub fn pipeline_targets(
    definition: &Json,
    registry: &str,
    name: &str,
) -> Result<BTreeSet<String>, ApiError> {
    let inactive = |key: &str| {
        definition
            .get(key)
            .is_none_or(|value| value.is_null() || *value == false || value.as_u64() == Some(0))
    };
    if !registry.starts_with("sql:")
        || registry.len() > 256
        || meerschaum_core::pipeline::validate_name(name).is_err()
        || definition["name"] != name
        || definition["registry"] != registry
        || definition["schema_version"].as_u64() != Some(2)
        || definition["revision"].as_u64().is_none_or(|n| n == 0)
        || definition["id"]
            .as_str()
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            .is_none_or(|id| id.is_nil())
        || definition
            .get("graph")
            .is_some_and(|value| !value.is_null() && value != &Json::Bool(true))
        || !inactive("parents")
        || !inactive("children")
        || definition
            .get("defaults")
            .is_some_and(|v| !v.as_object().is_some_and(|map| map.is_empty()))
        || definition
            .get("tag_associations")
            .is_some_and(|v| !v.as_array().is_some_and(|a| a.is_empty()))
    {
        return Err(refused());
    }
    let targets = definition["targets"]
        .as_array()
        .filter(|targets| !targets.is_empty() && targets.len() <= 256)
        .ok_or_else(refused)?;
    let mut roots = BTreeSet::new();
    for target in targets {
        let reference = target.as_str().ok_or_else(refused)?;
        let parts: Vec<_> = reference.split('|').collect();
        if parts.len() != 4
            || parts[0] != registry
            || parts[1] == "mrsm"
            || parts.iter().any(|part| part.is_empty())
            || reference.len() > 2048
            || reference.chars().any(char::is_control)
            || !roots.insert(reference.to_owned())
        {
            return Err(refused());
        }
    }
    Ok(roots)
}

/// Original Pipeline admission retained while Main reserves and creates its worker job.
pub struct PipelineRunRequest {
    pub(crate) source: Arc<RequestSource>,
    pub(crate) job: String,
    pub(crate) words: Vec<String>,
    pub(crate) registry: String,
    pub(crate) name: String,
    pub(crate) definition: Json,
    pub(crate) run_id: String,
    pub(crate) begin: Option<String>,
    pub(crate) end: Option<String>,
}
impl PipelineRunRequest {
    pub(crate) fn matches(&self, job: &str, words: &[String]) -> bool {
        self.job == job && self.words == words
    }
    pub async fn refresh(&self, state: &AppState) -> Result<Arc<Access>, ApiError> {
        self.source.refresh(state, SCOPES).await
    }
    /// Resolve from the original route credential and current physical registrations.
    pub async fn capture(&self, state: &AppState) -> Result<GraphOperation, ApiError> {
        let caller = self.refresh(state).await?;
        let roots = pipeline_targets(&self.definition, &self.registry, &self.name)?;
        if !state.config.actions_non_admin
            || !guard::may_use_connector(state, &caller, &self.registry)
            || !crate::engine_stream::valid_run_id(&self.run_id)
        {
            return Err(refused());
        }
        let conn = state.get_api_connector(Some(&self.registry))?;
        let (definition, parameters) =
            pipelines::visible_registration(state, &caller, &conn, &self.name).await?;
        if definition != self.definition
            || serde_json::to_vec(&definition)
                .map_err(|_| refused())?
                .len()
                > 128 * 1024
        {
            return Err(refused());
        }
        let pipe = pipelines::registration_pipe(state, &conn, &self.name)?;
        let registration = conn
            .get_pipe_id(&pipe)
            .await
            .map_err(|_| refused())?
            .ok_or_else(refused)?
            .to_value();
        let mut registrations = BTreeMap::new();
        for reference in roots {
            let parts: Vec<_> = reference.split('|').collect();
            let pipe = Pipe::builder(parts[1], parts[2])
                .location((parts[3] != "None").then_some(parts[3]))
                .instance(&self.registry)
                .defaults(state.config.pipe_defaults.clone())
                .build()
                .map_err(|_| refused())?;
            if pipe.pipe_ref() != reference {
                return Err(refused());
            }
            let id = conn
                .get_pipe_id(&pipe)
                .await
                .map_err(|_| refused())?
                .ok_or_else(refused)?
                .to_value();
            registrations.insert(reference, id);
        }
        let identity = PipelineIdentity {
            registry: self.registry.clone(),
            name: self.name.clone(),
            registration,
            parameters,
        };
        let selection = NativeSelection {
            sysargs: self.words.clone(),
            instances: BTreeSet::from([self.registry.clone()]),
            roots: registrations,
            begin: self.begin.clone(),
            end: self.end.clone(),
            parents: None,
            children: None,
            verify: false,
            normalized_at: chrono::Utc::now().timestamp(),
        };
        let graph = GraphOperation::Captured(CapturedGraph {
            version: 2,
            origin: GraphOrigin::capture(
                GraphEntry::PipelineEndpoint,
                SCOPES.iter().map(|v| v.to_string()).collect(),
            )?,
            request: GraphRequest::Pipelines {
                selection,
                definitions: vec![identity],
            },
            snapshot: None,
        });
        graph.pipeline_bootstrap(&self.registry, &self.name, &self.run_id)?;
        Ok(graph)
    }
}

/// Registered roots and direct children that wait on the same set of parents.
#[derive(Debug, PartialEq, Eq)]
pub struct PipelineLayer<'a> {
    pub parents: BTreeSet<&'a str>,
    pub children: BTreeSet<&'a str>,
}
impl<'a> PipelineLayer<'a> {
    /// Every selected target occurs in the layer and every child depends on all its parents.
    pub fn from_edges(
        targets: impl Iterator<Item = &'a str>,
        edges: impl Iterator<Item = (&'a str, &'a str)>,
    ) -> Option<Self> {
        let targets: BTreeSet<_> = targets.collect();
        let mut pairs = BTreeSet::new();
        let mut parents = BTreeSet::new();
        let mut children = BTreeSet::new();
        for (parent, child) in edges {
            if !targets.contains(parent)
                || !targets.contains(child)
                || !pairs.insert((parent, child))
            {
                return None;
            }
            parents.insert(parent);
            children.insert(child);
        }
        if pairs.is_empty()
            || !parents.is_disjoint(&children)
            || targets.len() != parents.len() + children.len()
            || pairs.len() != parents.len().checked_mul(children.len())?
        {
            return None;
        }
        Some(Self { parents, children })
    }
}

impl GraphOperation {
    pub fn pipeline_selection(&self) -> Option<(&NativeSelection, &PipelineIdentity)> {
        let Self::Captured(graph) = self else {
            return None;
        };
        let GraphRequest::Pipelines {
            selection,
            definitions,
        } = &graph.request
        else {
            return None;
        };
        let [definition] = definitions.as_slice() else {
            return None;
        };
        (graph.version == 2 && graph.origin.is_pipeline_endpoint())
            .then_some((selection, definition))
    }

    /// Read the exact two-layer dependencies captured in the native plan.
    pub fn pipeline_layer(&self) -> Option<PipelineLayer<'_>> {
        let (selection, _) = self.pipeline_selection()?;
        let plan = self.snapshot()?.main_instructions()?.main_plan();
        let edges = plan
            .get("edges")?
            .as_array()?
            .iter()
            .map(|edge| {
                let [parent, child] = edge.as_array()?.as_slice() else {
                    return None;
                };
                Some((parent.as_str()?, child.as_str()?))
            })
            .collect::<Option<Vec<_>>>()?;
        PipelineLayer::from_edges(
            selection.roots.keys().map(String::as_str),
            edges.into_iter(),
        )
    }

    pub fn set_pipeline_snapshot(&mut self, snapshot: Snapshot) -> Result<(), ApiError> {
        if self.pipeline_selection().is_none() {
            return Err(refused());
        }
        let Self::Captured(graph) = self else {
            return Err(refused());
        };
        if graph.snapshot.is_some() {
            return Err(refused());
        }
        graph.snapshot = Some(snapshot);
        Ok(())
    }

    pub fn pipeline_bootstrap(
        &self,
        registry: &str,
        name: &str,
        run_id: &str,
    ) -> Result<(Json, Vec<String>), ApiError> {
        let (selection, identity) = self.pipeline_selection().ok_or_else(refused)?;
        let definition = identity
            .parameters
            .get("pipeline")
            .map(crate::json::from_core)
            .ok_or_else(refused)?;
        let roots = pipeline_targets(&definition, registry, name)?;
        let mut words = [
            "sync",
            "pipelines",
            name,
            "-i",
            registry,
            "--run-id",
            run_id,
        ]
        .map(str::to_string)
        .to_vec();
        for (flag, value) in [("--begin", &selection.begin), ("--end", &selection.end)] {
            if let Some(value) = value {
                words.extend([flag.into(), value.clone()]);
            }
        }
        if identity.name != name
            || identity.registry != registry
            || identity.registration.is_null()
            || selection.sysargs != words
            || !crate::engine_stream::valid_run_id(run_id)
            || selection.instances != BTreeSet::from([registry.to_string()])
            || selection.verify
            || selection.parents.is_some()
            || selection.children.is_some()
            || selection.normalized_at <= 0
            || selection.roots.keys().ne(roots.iter())
            || selection.roots.values().any(Value::is_null)
        {
            return Err(refused());
        }
        Ok((definition, roots.into_iter().collect()))
    }

    pub(super) async fn authorize_pipeline(
        &self,
        state: &AppState,
        caller: &Access,
    ) -> Result<(), ApiError> {
        let (selection, identity) = self.pipeline_selection().ok_or_else(refused)?;
        let run_id = selection.sysargs.get(6).ok_or_else(refused)?;
        let (definition, _) =
            self.pipeline_bootstrap(&identity.registry, &identity.name, run_id)?;
        if !state.config.actions_non_admin
            || !guard::may_use_connector(state, caller, &identity.registry)
        {
            return Err(refused());
        }
        let conn = state.get_api_connector(Some(&identity.registry))?;
        let (current, parameters) =
            pipelines::visible_registration(state, caller, &conn, &identity.name).await?;
        let registration = pipelines::registration_pipe(state, &conn, &identity.name)?;
        let id = conn
            .get_pipe_id(&registration)
            .await
            .map_err(|_| refused())?
            .map(|id| id.to_value());
        if current != definition
            || parameters != identity.parameters
            || id.as_ref() != Some(&identity.registration)
        {
            return Err(refused());
        }
        let snapshot = self.snapshot().ok_or_else(refused)?;
        let writes: BTreeMap<_, _> = snapshot
            .members()
            .iter()
            .filter(|(_, member)| member.writable)
            .map(|(reference, member)| (reference.clone(), member.registration.clone()))
            .collect();
        if writes != selection.roots {
            return Err(refused());
        }
        snapshot.validate(state, caller).await
    }
}
