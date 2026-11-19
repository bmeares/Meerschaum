//! Read primitives needed by one accepted write operation, without a general read scope.

use std::collections::{BTreeMap, BTreeSet};

use meerschaum_core::{Map, Pipe, Value};
use serde::{Deserialize, Serialize};

use super::{CallbackBinding, GraphOperation, NativeInstructions, NodeWindow};
use crate::AppState;
use crate::access::{Access, guard};
use crate::error::ApiError;

const MAX_MEMBERS: usize = 256;
const MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotMember {
    /// Stable instance-local registration, captured with the parameters.
    pub registration: Value,
    /// Original accepted registered parameters.
    pub parameters: Map,
    /// The endpoint selected this node for a write, rather than metadata dependency only.
    pub writable: bool,
    /// Missing legacy evidence never implies an issued native data window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_window: Option<NodeWindow>,
}

/// Immutable Main-created graph evidence. Fields cannot be selected by an HTTP request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    members: BTreeMap<String, SnapshotMember>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    instructions: Option<NativeInstructions>,
}

/// The freshly admitted canonical operation, returned only by Main's private host adapter.
pub struct ReadAccess {
    /// Fresh bounded callback principal, with its original scopes unchanged.
    pub caller: Access,
    /// Private immutable operation descriptor.
    pub operation: GraphOperation,
    /// Exact canonical consumer selected by Main, never a request decoder.
    pub binding: CallbackBinding,
    /// The originating canonical graph. Direct typed roots are their own issuer.
    pub issuer: CallbackBinding,
    /// Fresh Main journal state for worker readiness only. Native Ticket-owned
    /// rechecks use their exact commit authority rather than a blanket denial.
    pub pending_nodes: BTreeSet<String>,
}

fn reference(instance: &str, connector: &str, metric: &str, location: Option<&str>) -> String {
    format!(
        "{instance}|{connector}|{metric}|{}",
        location.unwrap_or("None")
    )
}

fn denied() -> ApiError {
    ApiError::new(403, "This read is outside the accepted pipe operation.")
}

impl ReadAccess {
    fn issued_node(&self, pipe: &Pipe) -> Result<&NodeWindow, ApiError> {
        let member = self
            .operation
            .snapshot()
            .and_then(|snapshot| snapshot.members.get(&pipe.pipe_ref()))
            .filter(|member| member.writable && member.parameters == *pipe.raw_parameters())
            .ok_or_else(denied)?;
        let window = member.node_window.as_ref().ok_or_else(|| {
            ApiError::unavailable("Main has not issued this captured node's read window.")
        })?;
        window.validate(pipe, &member.registration)?;
        Ok(window)
    }
    fn source_window(
        &self,
        request: &meerschaum_connector_api::source_fetch::Request,
        replace: bool,
    ) -> Result<String, ApiError> {
        if !self.permits_source(
            &request.instance_keys,
            &request.pipe.connector_keys,
            &request.pipe.metric_key,
            request.pipe.location_key.as_deref(),
            &request.source_keys,
        ) {
            return Err(denied());
        }
        let member = self
            .operation
            .snapshot()
            .and_then(|snapshot| {
                snapshot.members.get(&reference(
                    &request.instance_keys,
                    &request.pipe.connector_keys,
                    &request.pipe.metric_key,
                    request.pipe.location_key.as_deref(),
                ))
            })
            .ok_or_else(denied)?;
        let pipe = Pipe::builder(&request.pipe.connector_keys, &request.pipe.metric_key)
            .instance(&request.instance_keys)
            .location(request.pipe.location_key.as_deref())
            .parameters(member.parameters.clone())
            .build()
            .map_err(|_| denied())?;
        let window = self.issued_node(&pipe)?;
        let query = request.data_query().map_err(|_| denied())?;
        window.source(&query)?;
        if replace {
            window.replacement(&query)?;
        }
        window.seal(&self.binding, &self.issuer)
    }
    fn read_window(
        &self,
        pipe: &Pipe,
        query: &meerschaum_connector::DataQuery,
    ) -> Result<String, ApiError> {
        let window = self.issued_node(pipe)?;
        if query.remote {
            window.source(query)?
        } else {
            window.read(query)?
        };
        window.seal(&self.binding, &self.issuer)
    }
    /// A process-local native write capability for this exact captured registered
    /// member. Logical aliases need their captured SelectedEngine mapping adapter;
    /// a caller cannot substitute a physical reference string here.
    pub fn write_window(
        &self,
        pipe: &Pipe,
    ) -> Result<meerschaum_connector::mutation::DataWindow, ApiError> {
        let member = self
            .operation
            .snapshot()
            .and_then(|snapshot| snapshot.members.get(&pipe.pipe_ref()))
            .filter(|member| member.writable && member.parameters == *pipe.raw_parameters())
            .ok_or_else(denied)?;
        let window = member.node_window.as_ref().ok_or_else(|| ApiError::unavailable(
            "This accepted graph node has no issued Main write window; local or broad-scope fallback is unavailable.",
        ))?;
        window.validate(pipe, &member.registration)?;
        window.data_window(&self.binding, &self.issuer)
    }
    /// Metadata is restricted to exact registered selected/dependency identities.
    pub fn permits(
        &self,
        instance: &str,
        connector: &str,
        metric: &str,
        location: Option<&str>,
    ) -> bool {
        self.operation.snapshot().is_some_and(|snapshot| {
            snapshot
                .members
                .contains_key(&reference(instance, connector, metric, location))
        })
    }

    /// A filtered catalog may be requested only for an instance in this operation.
    pub fn permits_instance(&self, instance: &str) -> bool {
        self.operation.snapshot().is_some_and(|snapshot| {
            snapshot
                .members
                .keys()
                .any(|reference| reference.split('|').next() == Some(instance))
        })
    }

    /// Source data is an internal part of syncing the exact accepted writable node.
    pub fn permits_source(
        &self,
        instance: &str,
        connector: &str,
        metric: &str,
        location: Option<&str>,
        source: &str,
    ) -> bool {
        let reference = reference(instance, connector, metric, location);
        source == connector
            && self.operation.snapshot().is_some_and(|snapshot| {
                snapshot
                    .members
                    .get(reference.as_str())
                    .is_some_and(|member| member.writable)
            })
    }

    /// State rows never expose unrelated pipelines, runs, event payloads or other nodes.
    pub fn permits_state_row(&self, metric: &str, row: &serde_json::Value) -> bool {
        let allowed = |key: &str| {
            row.get(key)
                .and_then(serde_json::Value::as_str)
                .is_some_and(|reference| {
                    self.operation
                        .snapshot()
                        .is_some_and(|snapshot| snapshot.members.contains_key(reference))
                })
        };
        match metric {
            "engine_state" | "engine_ledger" | "engine_nodes" => allowed("pipe_ref"),
            "engine_cursors" => allowed("parent_ref") && allowed("child_ref"),
            _ => false,
        }
    }

    /// Push exact allowed references into the native read before it allocates any rows.
    pub fn constrain_state_query(
        &self,
        metric: &str,
        query: &mut meerschaum_connector_api::engine_pipes::EnginePipeQuery,
    ) -> Result<(), ApiError> {
        let fields: &[&str] = match metric {
            "engine_state" | "engine_ledger" | "engine_nodes" => &["pipe_ref"],
            "engine_cursors" => &["parent_ref", "child_ref"],
            _ => return Err(denied()),
        };
        let members = &self.operation.snapshot().ok_or_else(denied)?.members;
        let params = query.params.get_or_insert_with(|| serde_json::json!({}));
        let params = params.as_object_mut().ok_or_else(denied)?;
        for field in fields {
            let allowed: Vec<_> = match params.get(*field) {
                None | Some(serde_json::Value::Null) => members.keys().cloned().collect(),
                Some(serde_json::Value::String(reference)) if members.contains_key(reference) => {
                    vec![reference.clone()]
                }
                Some(serde_json::Value::Array(references)) => references
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .filter(|reference| members.contains_key(*reference))
                            .map(str::to_string)
                            .ok_or_else(denied)
                    })
                    .collect::<Result<_, _>>()?,
                _ => return Err(denied()),
            };
            if allowed.is_empty() {
                return Err(denied());
            }
            params.insert((*field).into(), serde_json::json!(allowed));
        }
        // Never select arbitrary added JSON columns or prior messages, traces and snapshots.
        let columns: &[&str] = match metric {
            "engine_state" => &["pipe_ref", "definition_hash", "updated"],
            "engine_ledger" => &["created", "pipe_ref", "seq", "begin", "end"],
            "engine_cursors" => &[
                "parent_ref",
                "child_ref",
                "seq",
                "parent_watermark_at_cursor",
                "role",
            ],
            "engine_nodes" => &[
                "run_id",
                "pipe_ref",
                "action",
                "status",
                "started",
                "actual_started",
                "attempts",
                "finished",
                "duration_seconds",
            ],
            _ => return Err(denied()),
        };
        let columns: Vec<_> = columns.iter().map(|field| (*field).to_string()).collect();
        if query
            .select_columns
            .as_ref()
            .is_some_and(|selected| selected.iter().any(|field| !columns.contains(field)))
        {
            return Err(denied());
        }
        query.select_columns.get_or_insert(columns);
        if query
            .limit
            .is_some_and(|limit| !(0..=100_000).contains(&limit))
        {
            return Err(ApiError::new(
                413,
                "The operation state read exceeds its row bound.",
            ));
        }
        Ok(())
    }
}

impl Snapshot {
    pub fn members(&self) -> &BTreeMap<String, SnapshotMember> {
        &self.members
    }

    pub fn main_instructions(&self) -> Option<&NativeInstructions> {
        self.instructions.as_ref()
    }

    /// Install the SAME Main-resolved executable plan and window evidence in one
    /// bounded catalog value. No partial installation becomes visible on error.
    pub fn install_issued(
        &mut self,
        mut windows: BTreeMap<String, NodeWindow>,
        instructions: NativeInstructions,
    ) -> Result<(), ApiError> {
        if windows.len() != self.members.len() {
            return Err(denied());
        }
        let mut next = self.clone();
        for (reference, member) in &mut next.members {
            member.node_window = Some(windows.remove(reference).ok_or_else(denied)?);
        }
        instructions.validate_members(&next.members)?;
        next.instructions = Some(instructions);
        if serde_json::to_vec(&next).map_err(|_| denied())?.len() > MAX_BYTES {
            return Err(denied());
        }
        *self = next;
        Ok(())
    }
    /// Replace the provisional children together, preserving the issued parent,
    /// selected registrations, graph edges and original options.
    pub fn install_ready_children(
        &mut self,
        mut updates: BTreeMap<String, (NodeWindow, serde_json::Value)>,
    ) -> Result<(), ApiError> {
        if updates.is_empty() {
            return Err(denied());
        }
        let original = self.main_instructions().ok_or_else(denied)?;
        let mut plan = original.main_plan().clone();
        let mut windows = self
            .members
            .iter()
            .map(|(r, m)| Ok((r.clone(), m.node_window.clone().ok_or_else(denied)?)))
            .collect::<Result<BTreeMap<_, _>, ApiError>>()?;
        for node in plan["nodes"].as_array_mut().ok_or_else(denied)? {
            let reference = node["pipe_ref"].as_str().ok_or_else(denied)?;
            let Some((window, ready)) = updates.remove(reference) else {
                continue;
            };
            if node["provisional"] != true
                || ready["pipe_ref"] != reference
                || ready["provisional"] == true
                || ready["definition_hash"] != node["definition_hash"]
            {
                return Err(denied());
            }
            windows.insert(reference.to_owned(), window);
            *node = ready;
        }
        if !updates.is_empty() {
            return Err(denied());
        }
        let instructions = NativeInstructions::from_main(plan, &self.members)?;
        self.install_issued(windows, instructions)
    }

    /// Logical labels required by the accepted graph. This selects transport names only;
    /// Main must still apply the current caller, Team and served-instance policy to each.
    pub fn transport_instances(&self) -> Result<BTreeSet<String>, String> {
        if self.members.is_empty() || self.members.len() > MAX_MEMBERS {
            return Err("The accepted operation graph is unavailable.".into());
        }
        let mut instances = BTreeSet::new();
        for (reference, member) in &self.members {
            let parts: Vec<_> = reference.split('|').collect();
            if reference.len() > 2048 || parts.len() != 4 {
                return Err("Invalid logical identity in the accepted operation graph.".into());
            }
            meerschaum_core::team_scope::api_execution::split_keys(parts[0])?;
            instances.insert(parts[0].to_string());
            if member.writable
                && matches!(parts[1].split(':').next(), Some("sql" | "api" | "valkey"))
            {
                meerschaum_core::team_scope::api_execution::split_keys(parts[1])?;
                instances.insert(parts[1].to_string());
            }
            if instances.len() > 512 {
                return Err("The accepted operation exceeds its logical transport bound.".into());
            }
        }
        Ok(instances)
    }

    /// Advance only a selected member after a trusted native inference receipt. Exact old
    /// parameters make this a CAS; peer edits and recreated registrations cannot be adopted.
    pub fn accept_inference(
        &mut self,
        change: &meerschaum_connector::mutation::MetadataInference,
    ) -> Result<bool, String> {
        change.validate()?;
        let Some(member) = self.members.get(&change.pipe.pipe_ref()) else {
            return Ok(false);
        };
        if !member.writable
            || member.registration != change.registration
            || member.parameters != change.before
        {
            return Err(
                "Native inference no longer matches its selected registration and parameters."
                    .into(),
            );
        }
        let mut next = self.clone();
        next.members
            .get_mut(&change.pipe.pipe_ref())
            .expect("the selected member exists in the cloned snapshot")
            .parameters = change.after.clone();
        if let Some(window) = next
            .members
            .get_mut(&change.pipe.pipe_ref())
            .and_then(|member| member.node_window.as_mut())
        {
            let mut after = change.pipe.clone();
            after.set_parameters(change.after.clone());
            window.inferred(&after, &change.registration)?;
        }
        if serde_json::to_vec(&next)
            .map_err(|_| "Invalid inferred operation metadata.")?
            .len()
            > MAX_BYTES
        {
            return Err("Inferred operation metadata exceeds its byte bound.".into());
        }
        *self = next;
        Ok(true)
    }

    /// Construct bounded evidence in the trusted Main host; no request decoder calls this.
    pub fn new(members: BTreeMap<String, SnapshotMember>) -> Result<Self, ApiError> {
        let snapshot = Self {
            members,
            instructions: None,
        };
        if snapshot.members.is_empty()
            || snapshot.members.len() > MAX_MEMBERS
            || serde_json::to_vec(&snapshot).map_err(|_| denied())?.len() > MAX_BYTES
        {
            return Err(denied());
        }
        Ok(snapshot)
    }

    pub(super) async fn validate(&self, state: &AppState, caller: &Access) -> Result<(), ApiError> {
        if self.members.is_empty() || self.members.len() > MAX_MEMBERS {
            return Err(denied());
        }
        for (reference, member) in &self.members {
            let parts: Vec<_> = reference.split('|').collect();
            if parts.len() != 4
                || parts[1] == "mrsm"
                || !matches!(parts[0].split(':').next(), Some("sql" | "api" | "valkey"))
                || !guard::may_use_connector(state, caller, parts[0])
            {
                return Err(denied());
            }
            if let Some(object) = crate::routes::engine::graph_registry::registered(
                state,
                caller,
                reference,
                member.writable,
            )? {
                if object.registration != member.registration
                    || object.logical.raw_parameters() != &member.parameters
                {
                    return Err(denied());
                }
                if member.writable
                    && let Some(error) =
                        guard::lock_conflict(&object.logical, meerschaum_core::access::LockOp::Sync)
                {
                    return Err(error);
                }
                continue;
            }
            let conn = state.get_api_connector(Some(parts[0]))?;
            let mut pipe = Pipe::builder(parts[1], parts[2])
                .location((parts[3] != "None").then_some(parts[3]))
                .instance(parts[0])
                .defaults(state.config.pipe_defaults.clone())
                .build()
                .map_err(|_| denied())?;
            let attributes = conn
                .get_pipe_attributes(&pipe)
                .await
                .map_err(|_| ApiError::unavailable("Could not validate accepted pipe metadata."))?;
            let parameters = attributes
                .get("parameters")
                .and_then(Value::as_map)
                .ok_or_else(denied)?;
            let registration = conn
                .get_pipe_id(&pipe)
                .await
                .map_err(|_| {
                    ApiError::unavailable("Could not validate accepted pipe registration.")
                })?
                .map(|id| id.to_value());
            pipe.set_parameters(parameters.clone());
            if registration.as_ref() != Some(&member.registration)
                || parameters != &member.parameters
                || !caller.can_see_pipe(&pipe)
                || (member.writable && !caller.can_edit(parameters))
            {
                return Err(ApiError::new(
                    403,
                    "The accepted pipe registration, definition or object permission changed.",
                ));
            }
            guard::check_source_connector(state, caller, pipe.connector_keys())?;
            let index = crate::access::pipe_index(state, Some(parts[0])).await?;
            guard::check_pipe_target(conn.as_ref(), &pipe, &index.hidden_targets(caller))?;
            if member.writable
                && let Some(error) =
                    guard::lock_conflict(&pipe, meerschaum_core::access::LockOp::Sync)
            {
                return Err(error);
            }
        }
        Ok(())
    }
}

/// Original request proof retained across body, metadata and native read awaits.
pub(crate) struct ReadRequest {
    source: Option<crate::executor::plugin_caller::RequestSource>,
    initial: std::sync::Arc<Access>,
}
pub(crate) struct SourceAccess {
    pub caller: Access,
    pub window: Option<String>,
}
impl ReadRequest {
    pub(crate) async fn capture(
        ctx: &crate::routes::Ctx,
        scopes: &[&str],
    ) -> Result<Self, ApiError> {
        if ctx.state.config.no_auth {
            return Ok(Self {
                source: None,
                initial: std::sync::Arc::new(
                    crate::access::require_fresh(&ctx.state, &ctx.headers, scopes).await?,
                ),
            });
        }
        let source = crate::executor::plugin_caller::RequestSource::resolve(
            &ctx.state,
            &ctx.headers,
            scopes,
        )
        .await?;
        let initial = source.access();
        Ok(Self {
            source: Some(source),
            initial,
        })
    }
    pub(crate) fn initial(&self) -> &Access {
        &self.initial
    }
    async fn current(&self, ctx: &crate::routes::Ctx) -> Result<Access, ApiError> {
        match &self.source {
            Some(source) => Ok((*source.refresh(&ctx.state, &[]).await?).clone()),
            None => Ok((*self.initial).clone()),
        }
    }
    pub(crate) async fn graph(
        &self,
        ctx: &crate::routes::Ctx,
    ) -> Result<Option<ReadAccess>, ApiError> {
        let caller = self.current(ctx).await?;
        let Some(crate::auth::Principal::Execution(identity)) = &caller.auth else {
            return Ok(None);
        };
        if identity.grant.graph_tracking != 1 || identity.grant.graph_pending {
            return Err(ApiError::unavailable(
                "This execution has no proven current graph lineage.",
            ));
        }
        if identity.grant.graph_origin.is_none() {
            return Ok(None);
        };
        let mut capability = ctx
            .state
            .actions
            .pipe_operation_read(&caller)
            .await
            .map_err(|message| ApiError::new(403, message))?
            .ok_or_else(|| {
                ApiError::unavailable(
                    "The captured graph is unavailable; broad read scopes cannot replace it.",
                )
            })?;
        let current = self.current(ctx).await?;
        capability.confirm_request(&ctx.state, &self.initial, &current)?;
        if capability.operation.snapshot().is_none() || capability.operation.is_closed() {
            return Err(denied());
        }
        Ok(Some(capability))
    }
    pub(crate) async fn check(
        &self,
        ctx: &crate::routes::Ctx,
        pipe: &Pipe,
        query: &meerschaum_connector::DataQuery,
    ) -> Result<Option<String>, ApiError> {
        match self.graph(ctx).await? {
            Some(capability) => capability.read_window(pipe, query).map(Some),
            None => {
                self.current(ctx).await?;
                Ok(None)
            }
        }
    }
    pub(crate) async fn finish(
        &self,
        ctx: &crate::routes::Ctx,
        pipe: &Pipe,
        query: &meerschaum_connector::DataQuery,
        window: &Option<String>,
    ) -> Result<(), ApiError> {
        if &self.check(ctx, pipe, query).await? != window {
            return Err(ApiError::unavailable(
                "Main's read window changed while this request was running.",
            ));
        }
        Ok(())
    }
    pub(crate) async fn source(
        &self,
        ctx: &crate::routes::Ctx,
        request: &meerschaum_connector_api::source_fetch::Request,
        replace: bool,
    ) -> Result<SourceAccess, ApiError> {
        if let Some(capability) = self.graph(ctx).await? {
            let window = capability.source_window(request, replace)?;
            return Ok(SourceAccess {
                caller: capability.caller,
                window: Some(window),
            });
        }
        let caller = self.current(ctx).await?;
        let scopes: &[&str] = if replace {
            &["sql:read", "pipes:read", "pipes:write"]
        } else {
            &["sql:read", "pipes:read"]
        };
        caller.require(scopes, &[])?;
        Ok(SourceAccess {
            caller,
            window: None,
        })
    }
}

/// Existing graphs are bounded before considering ordinary broad read scopes.
pub(crate) async fn authorize(
    ctx: &crate::routes::Ctx,
) -> Result<(Access, Option<ReadAccess>), ApiError> {
    let request = ReadRequest::capture(ctx, &[]).await?;
    if let Some(capability) = request.graph(ctx).await? {
        return Ok((capability.caller.clone(), Some(capability)));
    }
    let caller = request.current(ctx).await?;
    caller.require(&["pipes:read"], &[])?;
    Ok((caller, None))
}
