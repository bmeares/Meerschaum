//! Main-created pipe operations retain endpoint scopes while using tracked execution.

use meerschaum_core::{Pipe, Value};
use serde::{Deserialize, Serialize};

mod accepted_rows;
pub mod commits;
mod direct_action;
mod stored;
pub use stored::{Cadence, StoredIntent, StoredPipe};
mod graph_operation;
pub use direct_action::{ActionPipe, DirectAction};
mod instructions;
mod reads;
mod windows;
mod worker_instructions;
pub use accepted_rows::{AcceptedRows, RowColumn};
mod pipeline;
pub use crate::routes::engine::graph_registry::NativeGraphRegistries;
pub use graph_operation::{
    CapturedGraph, GraphEntry, GraphOperation, GraphOrigin, GraphRequest, NativeSelection,
    PipelineIdentity,
};
pub use instructions::{NativeInstructions, Verification};
pub use pipeline::{PipelineLayer, PipelineRunRequest, pipeline_targets};
pub(crate) use reads::ReadRequest;
pub(crate) use reads::authorize as authorize_read;
pub use reads::{ReadAccess, Snapshot, SnapshotMember};
pub use windows::{CallbackBinding, NodeWindow, WindowAxis, WindowCoverage, WindowRange};

use crate::AppState;
use crate::access::{Access, guard};
use crate::error::ApiError;

/// Hash the exact UTF-8 argument vector, retaining boundaries between its words.
pub fn dispatch_argv_sha256(words: &[String]) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(words).map_err(|_| ApiError::internal())?;
    Ok(super::plugins::sha256(&bytes))
}

/// A typed endpoint operation, never decoded from public job properties or environment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipeOperation {
    /// Exact logical registered pipe identity.
    pub pipe_ref: String,
    /// Instance-local registration identity captured after endpoint authorization.
    pub registration: Value,
    /// Verification uses the same write/object scopes as its dedicated endpoint.
    pub verify: bool,
    /// Original row selection from a direct action, preserved independently of graph selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_params: Option<meerschaum_core::Map>,
    /// Original lower bound.
    pub begin: Option<String>,
    /// Original upper bound.
    pub end: Option<String>,
    /// Explicit graph traversal only, when the endpoint accepted it.
    pub parents: Option<Value>,
    /// Explicit graph traversal only, when the endpoint accepted it.
    pub children: Option<Value>,
    /// Main-captured registered graph; a public job property never supplies this authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Snapshot>,
}

impl PipeOperation {
    /// Reconstruct only the accepted built-in operation; arbitrary actions/flags cannot hide
    /// behind its narrower original endpoint permissions.
    pub fn sysargs(&self) -> Result<Vec<String>, ApiError> {
        let parts: Vec<_> = self.pipe_ref.split('|').collect();
        if parts.len() != 4
            || parts.iter().any(|part| part.is_empty())
            || parts.iter().any(|part| part.chars().any(char::is_control))
            || parts
                .iter()
                .any(|part| part.starts_with('-') || part.starts_with('_') || part.contains('*'))
            || parts[1..]
                .iter()
                .any(|part| part.starts_with('[') || part.ends_with(']'))
            || parts[1] == "mrsm"
            || self.pipe_ref.len() > 2048
            || self.registration.is_null()
            || self.begin.as_ref().is_some_and(|value| value.len() > 256)
            || self.end.as_ref().is_some_and(|value| value.len() > 256)
        {
            return Err(ApiError::new(403, "Invalid canonical pipe operation."));
        }
        let mut words = vec![
            if self.verify { "verify" } else { "sync" }.into(),
            "pipes".into(),
            format!("--connector-keys={}", parts[1]),
            format!("--metric-keys={}", parts[2]),
            format!("--location-keys={}", parts[3]),
            format!("--mrsm-instance={}", parts[0]),
        ];
        for (flag, value) in [("--begin", &self.begin), ("--end", &self.end)] {
            if let Some(value) = value {
                if value.chars().any(char::is_control) {
                    return Err(ApiError::new(403, "Invalid pipe operation bound."));
                }
                words.push(format!("{flag}={value}"));
            }
        }
        if let Some(params) = &self.row_params {
            direct_action::validate_row_params(params)?;
            words.push(format!(
                "--params={}",
                Value::Map(params.clone()).to_json_string()
            ));
        }
        for (flag, value) in [("--parents", &self.parents), ("--children", &self.children)] {
            match value {
                None | Some(Value::Null) | Some(Value::Bool(false)) => {}
                Some(Value::Bool(true)) => words.push(flag.into()),
                Some(Value::Int(depth)) if *depth >= 0 => words.push(format!("{flag}={depth}")),
                _ => return Err(ApiError::new(403, "Invalid pipe traversal depth.")),
            }
        }
        super::admission::user_sysargs(&words)?;
        Ok(words)
    }

    /// Every launch and renewal checks the immutable command, current object rights and
    /// original registration. Main tracking never grants generic jobs/actions privileges.
    pub async fn authorize(
        &self,
        state: &AppState,
        caller: &Access,
        words: &[String],
    ) -> Result<(), ApiError> {
        caller.require(&["pipes:write"], &[])?;
        if self.sysargs()? != words {
            return Err(ApiError::new(403, "The tracked pipe command changed."));
        }
        let parts: Vec<_> = self.pipe_ref.split('|').collect();
        if !parts[0]
            .split_once(':')
            .is_some_and(|(kind, _)| matches!(kind, "sql" | "api" | "valkey"))
            || !guard::may_use_connector(state, caller, parts[0])
        {
            return Err(ApiError::new(
                403,
                "The pipe operation's logical instance is unavailable.",
            ));
        }
        let (pipe, registration) = if let Some(object) =
            crate::routes::engine::graph_registry::registered(state, caller, &self.pipe_ref, true)?
        {
            (object.logical, Some(object.registration))
        } else {
            let conn = state.get_api_connector(Some(parts[0]))?;
            let mut pipe = Pipe::builder(parts[1], parts[2])
                .location((parts[3] != "None").then_some(parts[3]))
                .instance(parts[0])
                .defaults(state.config.pipe_defaults.clone())
                .build()
                .map_err(|_| ApiError::new(403, "Invalid pipe operation identity."))?;
            let attributes = conn
                .get_pipe_attributes(&pipe)
                .await
                .map_err(|_| ApiError::unavailable("Could not revalidate the pipe operation."))?;
            let parameters = attributes
                .get("parameters")
                .and_then(Value::as_map)
                .ok_or_else(ApiError::not_found)?;
            pipe.set_parameters(parameters.clone());
            let registration = conn
                .get_pipe_id(&pipe)
                .await
                .map_err(|_| ApiError::unavailable("Could not revalidate the pipe registration."))?
                .map(|id| id.to_value());
            (pipe, registration)
        };
        if registration.as_ref() != Some(&self.registration)
            || !caller.can_see_pipe(state, &pipe)
            || !caller.can_edit_pipe(state, &pipe)
        {
            return Err(ApiError::new(
                403,
                "The original pipe registration or object permission changed.",
            ));
        }
        if let Some(error) = guard::lock_conflict(&pipe, meerschaum_core::access::LockOp::Sync) {
            return Err(error);
        }
        crate::execute_access::check(state, caller, words).await?;
        if let Some(snapshot) = &self.metadata {
            snapshot.validate(state, caller).await?;
        }
        Ok(())
    }
}
