//! Main-only typed-root planner. Instructions and window evidence are installed
//! atomically before callback/runtime delivery; mutation enforcement is separate.
//!
//! This phase issues initial nonprovisional native sync windows. Provisional children,
//! implicit rebuild/clear and verification's separate effective-bound path remain
//! explicit NotIssued until their own Main-controlled issuer is implemented.

use super::{Controller, JobPlacement, Record};
use meerschaum_connector::DateBound;
use meerschaum_connector_api::graph_plan::WINDOW_PLANNING_TIMEOUT;
use meerschaum_engine::{
    Action, AxisKind, Begin, Bound, ConnectorRegistry, Depth, EdgeCursor, Graph, GraphOptions,
    GraphWarning, InstanceState, LedgerEntry, PipeRecord, PipeRef, Pipeline, PlannedNode,
    Selection, StateError, StateSource, StaticRegistry, SyncBegin,
};
use meerschaum_server::executor::callbacks;
use meerschaum_server::executor::pipe_operation::{
    NativeInstructions, NodeWindow, PipelineLayer, ReadAccess, WindowAxis, WindowCoverage,
    WindowRange,
};
use meerschaum_server::executor::plugin_capabilities::Capabilities;
use meerschaum_server::executor::source_ceiling::Ceiling;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const MAX_STATE_ROWS: usize = 100_000;
const MAX_STATE_BYTES: usize = 16 * 1024 * 1024;
const MAX_RANGES: usize = 1024;

fn unavailable() -> String {
    "Main could not issue the exact accepted node windows. Refresh the admitted operation after checking its original source and registered graph.".into()
}

/// One private catalog transition stores instructions and permits together. A
/// partial result must never expose an executable plan without its node windows.
struct IssuedPlan {
    windows: BTreeMap<String, NodeWindow>,
    instructions: NativeInstructions,
}

/// Mirror NativeExecutor::sync's actual call selection, not a hull of the plan's
/// display bounds. Root/Rebuild ignore chunks/ranges; other actions prefer chunks.
/// Default is not an unbounded spelling: the native Main issuer resolves it first.
fn native_call_windows(planned: &PlannedNode) -> Result<Vec<WindowRange>, String> {
    if !planned.action.runs() || planned.provisional {
        return Err(unavailable());
    }
    let bounds = planned.bounds.ok_or_else(unavailable)?;
    if matches!(bounds.begin, Begin::Default) {
        return Err(unavailable());
    }
    let spans = if matches!(planned.action, Action::Root | Action::Rebuild) {
        Vec::new()
    } else if !planned.chunks.is_empty() {
        planned.chunks.clone()
    } else {
        planned.ranges.clone()
    };
    if spans.len() > MAX_RANGES {
        return Err(unavailable());
    }
    if spans.is_empty() {
        Ok(vec![WindowRange {
            begin: bounds.begin.bound(),
            end: bounds.end,
        }])
    } else {
        Ok(spans
            .into_iter()
            .map(|span| WindowRange {
                begin: span.begin,
                end: span.end,
            })
            .collect())
    }
}

struct Proof<'a> {
    controller: &'a Controller,
    record: &'a Record,
    grant: Uuid,
}
impl Proof<'_> {
    async fn check(&self) -> Result<(), String> {
        let state = self.controller.state()?;
        if self.record.profile != meerschaum_server::executor::environment::Profile::Api {
            return Err(
                "This graph needs its distinct credentialed Team admission/receipt adapter.".into(),
            );
        }
        let launch = super::launch(self.record)?;
        let mut identity = meerschaum_server::executor::admission::typed_api_identity(
            &state,
            self.grant,
            launch,
            (!self.record.team_id.is_nil()).then_some(self.record.team_id),
        )
        .await
        .map_err(|error| error.detail_text())?;
        if self.record.owner_binding != identity.owner_binding() {
            return Err(unavailable());
        }
        super::super::source_selection::agrees(self.record.source_selection.as_ref(), &identity)?;
        self.record
            .operation
            .as_ref()
            .ok_or_else(unavailable)?
            .authorize_capture(&state, &identity.caller(), &self.record.sysargs)
            .await
            .map_err(|error| error.detail_text())?;
        self.controller
            .confirm_job_sources(self.record, &identity.caller())
            .await?;
        let current = meerschaum_server::executor::admission::typed_api_identity(
            &state,
            self.grant,
            launch,
            (!self.record.team_id.is_nil()).then_some(self.record.team_id),
        )
        .await
        .map_err(|error| error.detail_text())?;
        // The supervisor may renew this same live grant during a state read. Its
        // lease can only extend here. Every authority and launch field must still agree.
        if identity.grant.lease_until > chrono::Utc::now().timestamp()
            && current.grant.lease_until > identity.grant.lease_until
        {
            identity.grant.lease_until = current.grant.lease_until;
        }
        if identity != current {
            return Err(unavailable());
        }
        Ok(())
    }
}

/// Same read-only state adapter used by the native metadata planner, but authorized
/// by the accepted endpoint's actual scopes. No synthetic pipes:read/jobs permission.
struct ScopedState<'a> {
    completed_parents: Option<BTreeMap<PipeRef, Option<i64>>>,
    proof: Proof<'a>,
    inner: InstanceState,
    members: BTreeSet<PipeRef>,
    failed: AtomicBool,
}
impl ScopedState<'_> {
    async fn check(&self, nodes: &[&PipeRef], instance: Option<&str>) -> Result<(), StateError> {
        let result = async {
            if nodes.iter().any(|node| !self.members.contains(*node))
                || instance.is_some_and(|instance| {
                    !self
                        .members
                        .iter()
                        .any(|node| node.instance_keys() == instance)
                })
            {
                return Err(unavailable());
            }
            self.proof.check().await
        }
        .await;
        result.map_err(|_| {
            self.failed.store(true, Ordering::Release);
            StateError::Other(unavailable())
        })
    }
    fn bounded<T>(&self, result: Result<T, StateError>) -> Result<T, StateError> {
        result.map_err(|_| {
            self.failed.store(true, Ordering::Release);
            StateError::Other(unavailable())
        })
    }
}
impl StateSource for ScopedState<'_> {
    async fn watermark(&self, node: &PipeRef) -> Result<Option<Bound>, StateError> {
        self.check(&[node], None).await?;
        let value = self.bounded(self.inner.watermark(node).await)?;
        self.check(&[node], None).await?;
        if let Some(completed) = self
            .completed_parents
            .as_ref()
            .and_then(|parents| parents.get(node))
            && value != completed.map(Bound::Int)
        {
            return self.bounded(Err(StateError::Other(
                "The parent watermark changed after its native completion receipt.".into(),
            )));
        }
        Ok(value)
    }
    async fn oldest_watermark(&self, node: &PipeRef) -> Result<Option<Bound>, StateError> {
        self.check(&[node], None).await?;
        let result = self.bounded(self.inner.oldest_watermark(node).await);
        self.check(&[node], None).await?;
        result
    }
    async fn definition_hash(&self, node: &PipeRef) -> Result<Option<String>, StateError> {
        self.check(&[node], None).await?;
        let result = self.bounded(self.inner.definition_hash(node).await);
        self.check(&[node], None).await?;
        result
    }
    async fn cursor(
        &self,
        instance: &str,
        parent: &PipeRef,
        child: &PipeRef,
    ) -> Result<Option<EdgeCursor>, StateError> {
        if child.instance_keys() != instance {
            return Err(StateError::Other(unavailable()));
        }
        self.check(&[parent, child], Some(instance)).await?;
        let result = self.bounded(self.inner.cursor(instance, parent, child).await);
        self.check(&[parent, child], Some(instance)).await?;
        result
    }
    async fn ledger_after(
        &self,
        instance: &str,
        parent: &PipeRef,
        after: i64,
    ) -> Result<Vec<LedgerEntry>, StateError> {
        self.check(&[parent], Some(instance)).await?;
        let result = self.bounded(self.inner.ledger_after(instance, parent, after).await);
        self.check(&[parent], Some(instance)).await?;
        result
    }
    async fn latest_seq(
        &self,
        instance: &str,
        parent: &PipeRef,
    ) -> Result<Option<i64>, StateError> {
        self.check(&[parent], Some(instance)).await?;
        let result = self.bounded(self.inner.latest_seq(instance, parent).await);
        self.check(&[parent], Some(instance)).await?;
        result
    }
}

/// Only concrete ISO/integer bounds reach this phase. Relative expressions must be
/// normalized ONCE by Main before sysargs capture, not reparsed later by the worker.
fn explicit_bound(text: Option<&str>) -> Result<Option<DateBound>, String> {
    match text {
        None => Ok(None),
        // Match native CLI parse_datetime -> show::bound, which represents the
        // explicit-null spelling as a non-bound string. Root planning still has
        // Begin::Default and MUST resolve actual backtrack; this does not issue an
        // unbounded permit merely because the original argv contained "None".
        Some("None" | "[None]") => Ok(None),
        Some(text) => DateBound::parse(text).map(Some).ok_or_else(||
            "This operation's bound must first be normalized once by Main to an integer or ISO timestamp.".into()),
    }
}

async fn prepare_pipeline_children(
    graph: &Graph,
    subgraph: &meerschaum_engine::Subgraph,
    plan: &mut meerschaum_engine::Plan,
    registry: &StaticRegistry,
    scoped: &ScopedState<'_>,
    options: &meerschaum_engine::PlanOptions,
) -> Result<(), String> {
    let unsupported = || {
        "Main cannot confirm completion of the entire parent window for this saved Pipeline."
            .to_string()
    };
    let layer = PipelineLayer::from_edges(
        subgraph.targets.iter().map(PipeRef::as_str),
        subgraph
            .edges
            .iter()
            .map(|edge| (edge.parent.as_str(), edge.child.as_str())),
    )
    .ok_or_else(unsupported)?;
    let (Some(Bound::Int(begin)), Some(Bound::Int(end))) = (options.begin, options.end) else {
        return Err(unsupported());
    };
    if begin >= end
        || plan.nodes.len() != subgraph.targets.len()
        || subgraph
            .edges
            .iter()
            .any(|edge| !edge.options.is_default() || edge.cross_instance)
    {
        return Err(unsupported());
    }
    let root = graph
        .node(&subgraph.edges[0].parent)
        .ok_or_else(unsupported)?;
    let instance = root.pipe.instance_keys();
    let conn = registry.instance(instance)?;
    if !instance.starts_with("sql:") || !conn.supports_windowed_writes() {
        return Err(unsupported());
    }
    for reference in &subgraph.targets {
        let node = graph.node(reference).ok_or_else(unsupported)?;
        let planned = plan.node(reference).ok_or_else(unsupported)?;
        let parent = layer.parents.contains(reference.as_str());
        if node.axis.kind != AxisKind::Int
            || node.axis.explicit_unit != root.axis.explicit_unit
            || node.pipe.connector_keys() != instance
            || node.pipe.instance_keys() != instance
            || node.pipe.fetch_replace() != parent
            || planned.clear
            || planned.requires_confirmation
            || (parent && (planned.provisional || planned.action != Action::Root))
            || (!parent
                && (!planned.provisional
                    || meerschaum_engine::ledger::definition_changed(
                        planned.stored_definition_hash.as_deref(),
                        node.pipe.parameters(),
                    )))
        {
            return Err(unsupported());
        }
        if parent
            && !conn
                .pipe_exists(&node.pipe)
                .await
                .map_err(|_| unsupported())?
        {
            return Err(unsupported());
        }
    }
    for edge in &subgraph.edges {
        if scoped
            .cursor(&edge.state_instance, &edge.parent, &edge.child)
            .await
            .map_err(|_| unsupported())?
            .is_some()
        {
            return Err(unsupported());
        }
    }
    scoped.proof.check().await?;
    let Some(completed) = &scoped.completed_parents else {
        return Ok(());
    };
    if completed
        .keys()
        .map(PipeRef::as_str)
        .ne(layer.parents.iter().copied())
    {
        return Err(unsupported());
    }
    for (parent, expected) in completed {
        if scoped.watermark(parent).await.map_err(|_| unsupported())? != expected.map(Bound::Int) {
            return Err(unsupported());
        }
    }
    let states = completed
        .keys()
        .map(|p| (p.clone(), meerschaum_engine::plan::ParentState::Succeeded))
        .collect();
    // A parent without a watermark cannot certify a non-empty interval. Refuse a
    // plan based on another parent's cap when one of the required caps is absent.
    let watermark = completed
        .values()
        .copied()
        .collect::<Option<Vec<_>>>()
        .and_then(|values| values.into_iter().min());
    let cap = watermark.map(|w| end.min(w.saturating_add(i64::from(options.inclusive_cap))));
    for reference in layer.children {
        let child_ref = PipeRef::from_string(reference);
        let ready = meerschaum_engine::plan::plan_node(
            graph, subgraph, &child_ref, &states, scoped, options,
        )
        .await;
        if scoped.failed.load(Ordering::Acquire)
            || ready.clear
            || ready.requires_confirmation
            || ready.provisional
            || !matches!(
                ready.action,
                Action::Skip | Action::Incremental | Action::Backfill
            )
            || scoped
                .watermark(&child_ref)
                .await
                .map_err(|_| unsupported())?
                != ready.watermark
            || !matches!(ready.watermark, None | Some(Bound::Int(_)))
        {
            return Err(unsupported());
        }
        if ready.action.runs() {
            let cap = cap.filter(|cap| begin < *cap).ok_or_else(unsupported)?;
            let mut previous_end = begin;
            for range in native_call_windows(&ready)? {
                let (Some(Bound::Int(lo)), Some(Bound::Int(hi))) = (range.begin, range.end) else {
                    return Err(unsupported());
                };
                if lo < previous_end || hi <= lo || hi > cap {
                    return Err(unsupported());
                }
                if ready.watermark.is_none() && lo != previous_end {
                    return Err(unsupported());
                }
                previous_end = hi;
            }
            if ready.watermark.is_none()
                && (ready.action != Action::Incremental || previous_end != cap)
            {
                return Err(unsupported());
            }
        } else if ready.watermark.is_none() && cap.is_some_and(|cap| begin < cap) {
            return Err(unsupported());
        }
        *plan
            .nodes
            .iter_mut()
            .find(|node| node.pipe_ref == child_ref)
            .ok_or_else(unsupported)? = ready;
    }
    Ok(())
}

impl Controller {
    pub(super) fn issue_operation_windows<'a>(
        &'a self,
        record: &'a mut Record,
        grant: &'a callbacks::Grant,
        submitter: Option<&'a meerschaum_server::access::Access>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            if record.operation.is_none() {
                return Ok(());
            }
            // Owned record remains unchanged on failed/partial state reads or source changes.
            let issued = tokio::time::timeout(
                WINDOW_PLANNING_TIMEOUT,
                Box::pin(self.plan_operation_windows(record, grant.id, submitter, None)),
            )
            .await
            .map_err(|_| {
                "Main node-window planning exceeded its bounded deadline.".to_string()
            })??;
            record
                .operation
                .as_mut()
                .and_then(|operation| operation.snapshot_mut())
                .ok_or_else(unavailable)?
                .install_issued(issued.windows, issued.instructions)
                .map_err(|error| error.detail_text())
        })
    }

    /// Install the selected child windows together after all their SQLite parents commit.
    pub(crate) async fn ready_prepared_child(
        &self,
        caller: &meerschaum_server::access::Access,
        child: &str,
    ) -> Result<(), String> {
        let read = self
            .pipe_operation_read(caller)
            .await?
            .ok_or_else(unavailable)?;
        let layer = read.operation.pipeline_layer().ok_or_else(unavailable)?;
        if !layer.children.contains(child) || read.binding != read.issuer {
            return Err(unavailable());
        }
        let contexts = |access: &ReadAccess| {
            layer
                .parents
                .iter()
                .map(|&parent| {
                    access
                        .native_batch_context(parent)
                        .map(|context| (PipeRef::from_string(parent), context))
                        .map_err(|error| error.detail_text())
                })
                .collect::<Result<BTreeMap<_, _>, String>>()
        };
        let parent_contexts = contexts(&read)?;
        let existing = read
            .operation
            .snapshot()
            .and_then(|snapshot| snapshot.main_instructions())
            .ok_or_else(unavailable)?
            .planned_node(child)
            .map_err(|error| error.detail_text())?;
        if existing["provisional"] != true {
            return Ok(());
        }
        let installation = Box::pin(async {
            let (_, record) = self.commit_records(&read).await?;
            if record.operation.as_ref() != Some(&read.operation) {
                return Err(unavailable());
            }
            let watermarks = parent_contexts.iter().map(|(parent, context)| {
                let watermark = record.commit_journal.completed_replacement(context)?
                    .ok_or_else(|| "Main is waiting for every parent's completed native source and target watermark.".to_string())?;
                Ok::<_, String>((parent.clone(), watermark))
            }).collect::<Result<_, _>>()?;
            let mut issued = tokio::time::timeout(
                WINDOW_PLANNING_TIMEOUT,
                self.plan_operation_windows(
                    &record,
                    read.issuer.grant,
                    None,
                    Some(watermarks),
                ),
            )
            .await
            .map_err(|_| unavailable())??;
            let mut proposed = read.operation.clone();
            let updates = layer.children
                .iter()
                .map(|&child| {
                    let ready = issued
                        .instructions
                        .planned_node(child)
                        .map_err(|error| error.detail_text())?;
                    let window = issued.windows.remove(child).ok_or_else(unavailable)?;
                    Ok::<_, String>((child.to_owned(), (window, ready)))
                })
                .collect::<Result<_, _>>()?;
            proposed
                .snapshot_mut()
                .ok_or_else(unavailable)?
                .install_ready_children(updates)
                .map_err(|error| error.detail_text())?;
            let fresh = self
                .pipe_operation_read(caller)
                .await?
                .ok_or_else(unavailable)?;
            if fresh.binding != read.binding
                || fresh.issuer != read.issuer
                || fresh.operation != read.operation
                || contexts(&fresh)? != parent_contexts
            {
                return Err(unavailable());
            }
            let catalog = crate::executor::jobs::Catalog::new(&self.root);
            tokio::task::spawn_blocking(move || {
                catalog.install_prepared_children(&record, proposed)
            })
            .await
            .map_err(|_| unavailable())?
        })
        .await;
        if installation.is_ok() {
            return Ok(());
        }
        // Another child's Ready can win the same compare-and-swap. Only accept the
        // now-current, authorized child instruction under the original parent receipt.
        let current = self
            .pipe_operation_read(caller)
            .await?
            .ok_or_else(unavailable)?;
        if current.binding == read.binding
            && current.issuer == read.issuer
            && current.operation.pipeline_layer().as_ref() == Some(&layer)
            && contexts(&current)? == parent_contexts
            && current
                .operation
                .snapshot()
                .and_then(|s| s.main_instructions())
                .ok_or_else(unavailable)?
                .planned_node(child)
                .map_err(|error| error.detail_text())?["provisional"]
                != true
        {
            Ok(())
        } else {
            installation
        }
    }

    async fn plan_operation_windows(
        &self,
        record: &Record,
        grant: Uuid,
        submitter: Option<&meerschaum_server::access::Access>,
        completed: Option<BTreeMap<PipeRef, Option<i64>>>,
    ) -> Result<IssuedPlan, String> {
        let proof = Proof {
            controller: self,
            record,
            grant,
        };
        proof.check().await?;
        let state = self.state()?;
        let graph_operation = record.operation.as_ref().ok_or_else(unavailable)?;
        let snapshot = graph_operation.snapshot().ok_or_else(unavailable)?;
        let settings =
            meerschaum_engine::EngineConfig::from_config(&self.config()?, Some(&self.root));
        let (roots, begin, end, parents, children, verify, row_params) =
            if let Some(pipe) = graph_operation.endpoint() {
                (
                    vec![PipeRef::from_string(&pipe.pipe_ref)],
                    pipe.begin.as_deref(),
                    pipe.end.as_deref(),
                    pipe.parents.as_ref().and_then(Depth::from_value),
                    pipe.children.as_ref().and_then(Depth::from_value),
                    pipe.verify,
                    pipe.row_params.as_ref(),
                )
            } else if let Some((pipeline, _)) = graph_operation.pipeline_selection() {
                (
                    pipeline.roots.keys().map(PipeRef::from_string).collect(),
                    pipeline.begin.as_deref(),
                    pipeline.end.as_deref(),
                    None,
                    None,
                    false,
                    None,
                )
            } else {
                return Err(unavailable());
            };
        let root = roots.first().cloned().ok_or_else(unavailable)?;
        let selection = Selection::refs(roots.clone())
            .with_graph(graph_operation.pipeline_selection().is_some())
            .with_parents(parents)
            .with_children(children);
        let selected_sources =
            super::super::source_selection::ceiling(record.source_selection.as_ref())?;
        let plugin_keys = snapshot
            .members()
            .keys()
            .filter_map(|reference| {
                let reference = PipeRef::from_string(reference.clone());
                let keys = reference.parts().connector_keys;
                (!matches!(keys.split(':').next(), Some("sql" | "api" | "valkey")))
                    .then(|| keys.to_string())
            })
            .collect::<BTreeSet<_>>();
        let check_cli = record.source_selection.is_some()
            && (graph_operation.pipeline_selection().is_some()
                || matches!(
                    graph_operation,
                    meerschaum_server::executor::pipe_operation::GraphOperation::Direct(_)
                        | meerschaum_server::executor::pipe_operation::GraphOperation::Stored(_)
                ));
        let capabilities = if plugin_keys.is_empty() && !check_cli {
            None
        } else {
            // Only the original submit request may populate the confined metadata cache.
            // Reissues reread it under the recorded grant and recheck the exact source set.
            let identity = meerschaum_server::executor::admission::typed_api_identity(
                &state,
                grant,
                super::launch(record)?,
                (!record.team_id.is_nil()).then_some(record.team_id),
            )
            .await
            .map_err(|error| error.detail_text())?;
            let validated = if let Some(submitter) = submitter {
                meerschaum_server::executor::context::scope(
                    Some(Arc::new(submitter.clone())),
                    Box::pin(meerschaum_server::executor::context::graph_operation_scope(
                        Arc::new(graph_operation.clone()),
                        Box::pin(async {
                            if record.placement == JobPlacement::HostApi {
                                self.discover_host_plugin_catalog(
                                    (!record.team_id.is_nil()).then_some(record.team_id),
                                    selected_sources.names(),
                                )
                                .await
                            } else {
                                self.discover_plugin_catalog(
                                    record.team_id,
                                    selected_sources.names(),
                                )
                                .await
                            }
                        }),
                    )),
                )
                .await?
            } else {
                self.cached_graph_catalog(&identity).await?
            };
            if Ceiling::from_catalog(validated.catalog())? != selected_sources {
                return Err(unavailable());
            }
            if check_cli
                && validated.catalog().plugins.values().any(|plugin| {
                    plugin
                        .metadata
                        .as_ref()
                        .and_then(|metadata| metadata.cli_arguments.as_ref())
                        != Some(&meerschaum_server::executor::plugins::ArgumentRegistration::Absent)
                })
            {
                return Err("The selected plugins must declare no additional CLI arguments for this captured command.".into());
            }
            let capabilities = Capabilities {
                source_catalog_sha256: validated.source_digest().map_err(|_| unavailable())?,
                plugins: validated
                    .catalog()
                    .plugins
                    .iter()
                    .filter_map(|(name, plugin)| {
                        Some((
                            name.clone(),
                            plugin.metadata.as_ref()?.capabilities.clone()?,
                        ))
                    })
                    .collect(),
            };
            capabilities.validate(&validated)?;
            proof.check().await?;
            Some(capabilities)
        };
        let mut registry = StaticRegistry::new();
        let mut instances = BTreeSet::new();
        let mut records = Vec::new();
        for (reference, member) in snapshot.members() {
            let reference = PipeRef::from_string(reference.clone());
            let parts = reference.parts();
            if instances.insert(parts.instance_keys.to_string()) {
                registry = registry.with_instance(
                    parts.instance_keys,
                    state
                        .get_api_connector(Some(parts.instance_keys))
                        .map_err(|error| error.detail_text())?,
                );
            }
            let classified = capabilities
                .as_ref()
                .and_then(|caps| caps.lookup(parts.connector_keys));
            if plugin_keys.contains(parts.connector_keys) && classified.is_none() {
                return Err(
                    "The selected source has no proven confined connector classification.".into(),
                );
            }
            records.push(PipeRecord {
                instance_keys: parts.instance_keys.into(),
                connector_keys: parts.connector_keys.into(),
                metric_key: parts.metric_key.into(),
                location_key: parts.location_key.map(str::to_string),
                parameters: member.parameters.clone(),
                pipe_id: Some(member.registration.clone()),
                streaming: classified.as_ref().is_some_and(|caps| caps.streaming),
                python: classified.as_ref().is_some_and(|caps| caps.python),
            });
        }
        let graph = Graph::build(
            records,
            &GraphOptions {
                defaults: settings.defaults.clone(),
                implicit_edges: settings.implicit_edges,
                default_backtrack_minutes: settings.default_backtrack_minutes as f64,
            },
        );
        if graph.warnings().iter().any(|warning| {
            matches!(
                warning,
                GraphWarning::InvalidAxis { .. }
                    | GraphWarning::InvalidRecord { .. }
                    | GraphWarning::DuplicateRecord { .. }
            )
        }) {
            return Err(unavailable());
        }
        let subgraph = Pipeline::new(selection)
            .resolve(&graph)
            .map_err(|_| unavailable())?;
        if subgraph.order.iter().any(|reference| {
            snapshot
                .members()
                .get(reference.as_str())
                .is_none_or(|member| !member.writable)
        }) {
            return Err(unavailable());
        }
        let root_node = graph.node(&root).ok_or_else(unavailable)?;
        let root_instance = registry.instance(root.instance_keys())?;
        proof.check().await?;
        let accepted_rows = if let Some(params) = row_params {
            use meerschaum_server::executor::pipe_operation::{AcceptedRows, RowColumn};
            if !root_instance.supports_windowed_writes()
                || subgraph.order != vec![root.clone()]
                || root_node.pipe.connector_keys() != root_node.pipe.instance_keys()
            {
                return Err("Filtered graph operations require a single native SQLite source and destination on the same instance.".into());
            }
            let types = root_instance
                .get_pipe_columns_types(&root_node.pipe)
                .await
                .map_err(|_| unavailable())?;
            proof.check().await?;
            let columns = params.keys().map(|name| {
                let native = meerschaum_connector::mutation::RowColumn {
                    dtype: root_node.pipe.dtype(name),
                    native_type: types.get(name).cloned(),
                    literal_null_marker: false,
                };
                if !native.supports_integer_equality() {
                    return Err("Filtered SQLite operations require a declared integer filter column in the existing destination.".to_string());
                }
                Ok((name.clone(), RowColumn {
                    dtype: native.dtype,
                    native_type: native.native_type,
                    literal_null_marker: native.literal_null_marker,
                }))
            }).collect::<Result<BTreeMap<_,_>,String>>()?;
            AcceptedRows::Filter {
                params: params.clone(),
                columns,
            }
        } else {
            meerschaum_server::executor::pipe_operation::AcceptedRows::All
        };
        let root_dtypes = meerschaum_connector::pipe_ops::get_dtypes(
            root_instance.as_ref(),
            &root_node.pipe,
            root_node.pipe.parameters(),
            true,
        )
        .await
        .map_err(|_| unavailable())?;
        proof.check().await?;
        let env = meerschaum_engine::sync::SyncEnv {
            delivery: None,
            registry: &registry,
            defaults: &settings.defaults,
            default_backtrack_minutes: settings.default_backtrack_minutes,
            filter_params_index_limit: settings.filter_params_index_limit,
            known: None,
        };
        let normalize = |text: Option<&str>| -> Result<Option<Bound>, String> {
            let Some(bound) = explicit_bound(text)? else {
                return Ok(None);
            };
            let normalized = meerschaum_engine::sync::parse_date_bound(
                &env,
                &root_node.pipe,
                &root_dtypes,
                bound,
            )?;
            Ok(Some(
                meerschaum_engine::sync::date_bound_to_bound(&normalized)
                    .ok_or_else(unavailable)?,
            ))
        };
        if verify {
            for bound in [begin, end] {
                if matches!(
                    (root_node.axis.kind, explicit_bound(bound)?),
                    (AxisKind::Int, Some(DateBound::Datetime(_)))
                        | (AxisKind::Datetime, Some(DateBound::Int(_)))
                ) {
                    return Err("This verification needs Main's exact mixed-axis native grid adapter; bounds cannot be converted before choosing its grid.".into());
                }
            }
            if subgraph.order != vec![root.clone()] {
                return Err(
                    "Native verification does not traverse a worker-selected graph.".into(),
                );
            }
            let aware = root_node
                .pipe
                .column("datetime")
                .as_ref()
                .and_then(|column| root_dtypes.get(column))
                .is_none_or(|dtype| dtype == "datetime" || dtype.to_lowercase().contains("utc"));
            proof.check().await?;
            let oldest = root_instance
                .get_sync_time(&root_node.pipe, row_params, false)
                .await
                .map_err(|_| unavailable())?;
            proof.check().await?;
            let newest = root_instance
                .get_sync_time(&root_node.pipe, row_params, true)
                .await
                .map_err(|_| unavailable())?;
            proof.check().await?;
            let interval = root_node
                .pipe
                .get_chunk_interval(None, &settings.defaults)
                .map_err(|_| unavailable())?;
            let default_begin = meerschaum_engine::sync::determine_begin(
                &env,
                root_instance.as_ref(),
                &root_node.pipe,
                &root_dtypes,
                &SyncBegin::Default,
                None,
                true,
            )
            .await
            .map_err(|_| unavailable())?;
            proof.check().await?;
            let verification = crate::native::verify_plan::plan(
                &root_node.pipe,
                normalize(begin)?.map(Into::into),
                normalize(end)?.map(Into::into),
                interval,
                &crate::native::verify_plan::Observations {
                    local_oldest: oldest,
                    local_newest: newest,
                    remote_oldest: None,
                    remote_newest: None,
                    aware,
                },
                default_begin,
            )?;
            let mut windows = BTreeMap::new();
            for (reference, member) in snapshot.members() {
                let node = graph
                    .node(&PipeRef::from_string(reference.clone()))
                    .ok_or_else(unavailable)?;
                let window = if reference != root.as_str() || verification.calls.is_empty() {
                    NodeWindow::not_issued(
                        &node.pipe,
                        member.registration.clone(),
                        "Main issued no data call for this verification member.",
                    )
                } else {
                    let axis = match node.axis.kind {
                        AxisKind::None => WindowAxis::WholeRelation,
                        AxisKind::Int => WindowAxis::Integer {
                            column: node.axis.column.clone().ok_or_else(unavailable)?,
                            explicit_unit: node
                                .axis
                                .explicit_unit
                                .map(|unit| unit.name().to_string()),
                        },
                        AxisKind::Datetime => {
                            let column = node.axis.column.clone().ok_or_else(unavailable)?;
                            WindowAxis::Datetime {
                                dtype: node.pipe.dtype(&column).ok_or_else(unavailable)?,
                                column,
                            }
                        }
                    };
                    NodeWindow::from_main_plan(
                        &node.pipe,
                        member.registration.clone(),
                        axis,
                        verification.calls.clone(),
                        verification.coverage,
                        node.pipe.fetch_replace(),
                    )
                    .and_then(|window| window.with_accepted_rows(accepted_rows.clone()))
                    .map_err(|error| error.detail_text())?
                };
                windows.insert(reference.clone(), window);
            }
            let descriptor = meerschaum_server::executor::pipe_operation::Verification {
                reference: root.to_string(),
                definition_hash: root_node.definition_hash.clone(),
                calls: verification.calls,
                row_params: row_params.cloned(),
                reason: if verification.fallback_sync {
                    "Main resolved the native fallback sync."
                } else {
                    "Main resolved the native verification grid."
                }
                .into(),
            };
            let plan = serde_json::json!({"nodes":[{"pipe_ref":root.as_str(),"action":"skip","bounds":null,"reason":"Main uses separate exact verification calls.","definition_hash":root_node.definition_hash}],"edges":[],"flat":true,"warnings":[]});
            let instructions =
                NativeInstructions::from_main_verify(plan, descriptor, snapshot.members())
                    .map_err(|error| error.detail_text())?;
            proof.check().await?;
            return Ok(IssuedPlan {
                windows,
                instructions,
            });
        }
        let options = meerschaum_engine::PlanOptions {
            begin: normalize(begin)?,
            end: normalize(end)?,
            interactive: false,
            yes: false,
            auto_rebuild: false,
            max_chunks: MAX_RANGES,
            inclusive_cap: settings.inclusive_cap,
            ..Default::default()
        };
        if roots.iter().any(|reference| {
            graph.node(reference).is_none_or(|node| {
                if node.axis.kind == AxisKind::None {
                    return graph_operation.pipeline_selection().is_some()
                        && (begin.is_some() || end.is_some());
                }
                [options.begin, options.end]
                    .into_iter()
                    .flatten()
                    .any(|bound| node.axis.pos(&bound).is_none())
            })
        }) {
            // The public planner warns/ignores incompatible bounds for display. A
            // capability issuer must not turn such a warning into an unbounded permit.
            return Err(unavailable());
        }
        if roots.len() > 1 && (begin.is_some() || end.is_some()) {
            for reference in roots.iter().skip(1) {
                let node = graph.node(reference).ok_or_else(unavailable)?;
                let instance = registry.instance(reference.instance_keys())?;
                let types = meerschaum_connector::pipe_ops::get_dtypes(
                    instance.as_ref(),
                    &node.pipe,
                    node.pipe.parameters(),
                    true,
                )
                .await
                .map_err(|_| unavailable())?;
                for (bound, expected) in [(begin, options.begin), (end, options.end)] {
                    if let Some(input) = explicit_bound(bound)? {
                        let normalized = meerschaum_engine::sync::parse_date_bound(
                            &env, &node.pipe, &types, input,
                        )?;
                        if meerschaum_engine::sync::date_bound_to_bound(&normalized) != expected {
                            return Err(unavailable());
                        }
                    }
                }
                proof.check().await?;
            }
        }
        let inner = InstanceState::new(Arc::new(registry.clone()), root.instance_keys())
            .with_read_limit(MAX_STATE_ROWS)
            .with_read_byte_limit(MAX_STATE_BYTES);
        inner.register_pipes(
            graph
                .nodes()
                .map(|node| (PipeRef::from_pipe(&node.pipe), node.pipe.clone())),
        );
        let scoped = ScopedState {
            completed_parents: completed,
            proof,
            inner,
            members: snapshot
                .members()
                .keys()
                .cloned()
                .map(PipeRef::from_string)
                .collect(),
            failed: AtomicBool::new(false),
        };
        let mut plan = meerschaum_engine::plan(&graph, &subgraph, &scoped, &options).await;
        if scoped.failed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        scoped.proof.check().await?;
        if graph_operation.pipeline_selection().is_some()
            && plan.nodes.iter().any(|node| node.provisional)
        {
            Box::pin(prepare_pipeline_children(
                &graph, &subgraph, &mut plan, &registry, &scoped, &options,
            ))
            .await?;
        } else if scoped.completed_parents.is_some() {
            return Err(unavailable());
        }

        // Resolve a native Default exactly once, in the actual instruction Plan
        // that will be sent back to this launch. Resolving a separate local copy
        // would leave the worker free to re-evaluate backtrack against newer data.
        for planned in &mut plan.nodes {
            if !planned.action.runs()
                || planned.provisional
                || verify
                || planned.clear
                || planned.requires_confirmation
            {
                continue;
            }
            let Some(bounds) = planned.bounds.as_mut() else {
                return Err(unavailable());
            };
            if !matches!(bounds.begin, Begin::Default) {
                continue;
            }
            let node = graph.node(&planned.pipe_ref).ok_or_else(unavailable)?;
            scoped.proof.check().await?;
            let conn = registry.instance(node.pipe.instance_keys())?;
            let dtypes = meerschaum_connector::pipe_ops::get_dtypes(
                conn.as_ref(),
                &node.pipe,
                node.pipe.parameters(),
                true,
            )
            .await
            .map_err(|_| unavailable())?;
            scoped.proof.check().await?;
            let end = bounds.end.map(DateBound::from);
            let begin = meerschaum_engine::sync::determine_begin(
                &env,
                conn.as_ref(),
                &node.pipe,
                &dtypes,
                &SyncBegin::Default,
                end.as_ref(),
                true,
            )
            .await
            .map_err(|_| unavailable())?;
            scoped.proof.check().await?;
            bounds.begin = match begin {
                None => Begin::Unbounded,
                Some(begin) => Begin::At(
                    meerschaum_engine::sync::date_bound_to_bound(&begin).ok_or_else(unavailable)?,
                ),
            };
            planned.bounds_text = planned.describe_bounds();
        }
        let mut windows = BTreeMap::new();
        for (reference, member) in snapshot.members() {
            let node = graph
                .node(&PipeRef::from_string(reference.clone()))
                .ok_or_else(unavailable)?;
            let planned = plan.node(&PipeRef::from_string(reference.clone()));
            let not_issued =
                |reason| NodeWindow::not_issued(&node.pipe, member.registration.clone(), reason);
            let window = match planned {
                _ if !member.writable => not_issued("This node is a metadata-only dependency."),
                _ if verify => {
                    not_issued("Verification requires its own Main-issued effective chunk bounds.")
                }
                Some(planned) if planned.provisional => not_issued(
                    "Main must replan this provisional child after proven upstream commits.",
                ),
                Some(planned) if !planned.action.runs() || planned.bounds.is_none() => {
                    not_issued("The native planner issued no runnable range.")
                }
                Some(planned) if planned.clear || planned.requires_confirmation => {
                    not_issued("This node requires a separately admitted clear/rebuild window.")
                }
                Some(planned) => {
                    let ranges = native_call_windows(planned)?;
                    let axis = match node.axis.kind {
                        AxisKind::None => WindowAxis::WholeRelation,
                        AxisKind::Int => WindowAxis::Integer {
                            column: node.axis.column.clone().ok_or_else(unavailable)?,
                            explicit_unit: node
                                .axis
                                .explicit_unit
                                .map(|unit| unit.name().to_string()),
                        },
                        AxisKind::Datetime => {
                            let column = node.axis.column.clone().ok_or_else(unavailable)?;
                            WindowAxis::Datetime {
                                dtype: node.pipe.dtype(&column).ok_or_else(unavailable)?,
                                column,
                            }
                        }
                    };
                    // These are the concrete per-node native calls before range union.
                    // Complementary axis predicates may cover all scalar positions while
                    // excluding NULL; normalizing their hull must not mint no-predicate.
                    let coverage = if ranges.len() == 1
                        && ranges[0].begin.is_none()
                        && ranges[0].end.is_none()
                    {
                        WindowCoverage::WholeRelation
                    } else {
                        WindowCoverage::AxisRanges
                    };
                    NodeWindow::from_main_plan(
                        &node.pipe,
                        member.registration.clone(),
                        axis,
                        ranges,
                        coverage,
                        node.pipe.fetch_replace(),
                    )
                    .and_then(|window| window.with_accepted_rows(accepted_rows.clone()))
                    .map_err(|error| error.detail_text())?
                }
                None => not_issued("This node was absent from the runnable native plan."),
            };
            windows.insert(reference.clone(), window);
        }
        scoped.proof.check().await?;
        let instructions = NativeInstructions::from_main(
            serde_json::to_value(&plan).map_err(|_| unavailable())?,
            snapshot.members(),
        )
        .map_err(|error| error.detail_text())?;
        Ok(IssuedPlan {
            windows,
            instructions,
        })
    }
}

#[cfg(test)]
mod instruction_tests {
    use super::*;

    fn node(action: &str) -> PlannedNode {
        serde_json::from_value(serde_json::json!({
            "pipe_ref":"sql:main|manual:fixture|window|None", "action":action,
            "bounds":{"begin":0,"end":100}, "reason":"Main native fixture",
            "definition_hash":"fixture",
            "chunks":[{"begin":10,"end":20},{"begin":30,"end":40}],
            "ranges":[{"begin":0,"end":100}]
        }))
        .unwrap()
    }

    #[test]
    fn emitted_native_calls_use_chunks_without_authorizing_their_gap_or_display_hull() {
        let windows = native_call_windows(&node("backfill")).unwrap();
        assert_eq!(
            windows,
            vec![
                WindowRange {
                    begin: Some(Bound::Int(10)),
                    end: Some(Bound::Int(20))
                },
                WindowRange {
                    begin: Some(Bound::Int(30)),
                    end: Some(Bound::Int(40))
                },
            ]
        );
        let mut ranges = node("incremental");
        ranges.chunks.clear();
        assert_eq!(
            native_call_windows(&ranges).unwrap(),
            vec![WindowRange {
                begin: Some(Bound::Int(0)),
                end: Some(Bound::Int(100))
            },]
        );
    }

    #[test]
    fn root_uses_its_resolved_bound_and_default_never_becomes_unbounded() {
        let mut root = node("root");
        assert_eq!(
            native_call_windows(&root).unwrap(),
            vec![WindowRange {
                begin: Some(Bound::Int(0)),
                end: Some(Bound::Int(100))
            },]
        );
        root.bounds.as_mut().unwrap().begin = Begin::Default;
        assert!(native_call_windows(&root).is_err());
        root.bounds.as_mut().unwrap().begin = Begin::Unbounded;
        root.bounds.as_mut().unwrap().end = None;
        assert_eq!(
            native_call_windows(&root).unwrap(),
            vec![WindowRange {
                begin: None,
                end: None
            },]
        );
        root.provisional = true;
        assert!(native_call_windows(&root).is_err());
    }
}
