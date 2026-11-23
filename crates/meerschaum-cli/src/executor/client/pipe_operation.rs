//! Pure native graph capture for a Main-created typed pipe operation.
use meerschaum_connector::PipesKeysQuery;
use meerschaum_core::Value;
use meerschaum_engine::{Depth, Graph, GraphOptions, PipeRef, Selection, StaticRegistry};
use meerschaum_server::executor::pipe_operation::{PipeOperation, Snapshot, SnapshotMember};
use meerschaum_server::{
    AppState,
    access::{Access, guard},
    error::ApiError,
};
use std::collections::{BTreeMap, BTreeSet};
const MAX_PIPES: usize = 4096;
const MAX_MEMBERS: usize = 256;
const MAX_BYTES: usize = 4 * 1024 * 1024;
fn denied() -> ApiError {
    ApiError::new(
        403,
        "The pipe operation graph is unavailable or unauthorized.",
    )
}
pub(super) async fn capture(
    state: &AppState,
    caller: &Access,
    operation: &PipeOperation,
    settings: &meerschaum_engine::EngineConfig,
) -> Result<Snapshot, ApiError> {
    let _ = operation.sysargs()?;
    let root = PipeRef::from_string(&operation.pipe_ref);
    let selection = Selection::refs([root.clone()])
        .with_parents(operation.parents.as_ref().and_then(Depth::from_value))
        .with_children(operation.children.as_ref().and_then(Depth::from_value));
    capture_selected(
        state,
        caller,
        selection,
        &BTreeMap::from([(operation.pipe_ref.clone(), operation.registration.clone())]),
        settings,
    )
    .await
}

pub(super) async fn capture_selected(
    state: &AppState,
    caller: &Access,
    selection: Selection,
    roots: &BTreeMap<String, Value>,
    settings: &meerschaum_engine::EngineConfig,
) -> Result<Snapshot, ApiError> {
    if roots.is_empty() || roots.len() > MAX_MEMBERS {
        return Err(denied());
    }
    let options = GraphOptions {
        defaults: settings.defaults.clone(),
        implicit_edges: settings.implicit_edges,
        default_backtrack_minutes: settings.default_backtrack_minutes as f64,
    };
    let mut registry = StaticRegistry::new();
    let mut visited = BTreeSet::new();
    let mut pending = selection.target_instances();
    let mut records = Vec::new();
    let mut bytes = 0usize;
    let graph = loop {
        for keys in std::mem::take(&mut pending) {
            if !visited.insert(keys.clone()) {
                continue;
            }
            if visited.len() > 64
                || !matches!(keys.split(':').next(), Some("sql" | "api" | "valkey"))
                || !guard::may_use_connector(state, caller, &keys)
            {
                return Err(denied());
            }
            let conn = state.get_api_connector(Some(&keys))?;
            registry = registry.with_instance(&keys, conn);
            let batch = meerschaum_engine::engine::fetch_records(
                &registry,
                &options.defaults,
                &keys,
                &PipesKeysQuery::default(),
            )
            .await
            .map_err(|_| ApiError::unavailable("Could not capture the pipe operation graph."))?;
            if records.len().saturating_add(batch.len()) > MAX_PIPES {
                return Err(denied());
            }
            for record in &batch {
                bytes = bytes
                    .saturating_add(Value::Map(record.parameters.clone()).to_json_string().len());
                if bytes > MAX_BYTES {
                    return Err(denied());
                }
            }
            records.extend(batch);
        }
        let graph = Graph::build(records.clone(), &options);
        pending = selection
            .referenced_instances(&graph)
            .difference(&visited)
            .cloned()
            .collect();
        if pending.is_empty() {
            break graph;
        }
    };
    let selected: BTreeSet<_> = graph
        .reach(
            roots.keys().map(PipeRef::from_string),
            selection.parents,
            selection.children,
        )
        .into_iter()
        .collect();
    if selected.is_empty() {
        return Err(denied());
    }
    let mut queue: Vec<_> = selected.iter().cloned().collect();
    let mut members = BTreeMap::new();
    while let Some(reference) = queue.pop() {
        if members.contains_key(reference.as_str()) {
            continue;
        }
        if members.len() >= MAX_MEMBERS {
            return Err(denied());
        }
        let node = graph.node(&reference).ok_or_else(denied)?;
        let record = &node.record;
        let writable = selected.contains(&reference);
        let pipe = record.to_pipe(&options.defaults).map_err(|_| denied())?;
        if pipe.connector_keys() == "mrsm"
            || !caller.can_see_pipe(state, &pipe)
            || (writable && !caller.can_edit_pipe(state, &pipe))
        {
            return Err(denied());
        }
        guard::check_source_connector(state, caller, pipe.connector_keys())?;
        if writable
            && let Some(error) = guard::lock_conflict(&pipe, meerschaum_core::access::LockOp::Sync)
        {
            return Err(error);
        }
        let registration = record
            .pipe_id
            .clone()
            .filter(|id| !id.is_null())
            .ok_or_else(denied)?;
        let mut dependencies = meerschaum_core::template::referenced_pipes_in_parameters(
            pipe.raw_parameters(),
            Some(&pipe),
            &options.defaults,
        );
        dependencies.extend(
            pipe.references(&options.defaults)
                .map_err(|_| denied())?
                .into_iter()
                .flatten(),
        );
        for dependency in dependencies {
            if !dependency.raw_parameters().is_empty() {
                return Err(denied());
            }
            queue.push(PipeRef::from_pipe(&dependency));
        }
        members.insert(
            reference.to_string(),
            SnapshotMember {
                registration,
                parameters: record.parameters.clone(),
                writable,
                node_window: None,
            },
        );
    }
    if roots.iter().any(|(reference, id)| {
        members
            .get(reference)
            .is_none_or(|member| !member.writable || &member.registration != id)
    }) {
        return Err(denied());
    }
    Snapshot::new(members)
}
