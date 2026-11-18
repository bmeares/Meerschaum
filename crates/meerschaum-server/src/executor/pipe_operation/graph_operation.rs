//! Common Main-admitted graph model. Legacy endpoint serialization
//! remains readable. New selection/Pipeline models remain closed until their exact
//! native captures, instruction issuer and runtime enforcement are registered.

use super::{PipeOperation, Snapshot};
use crate::{AppState, access::Access, error::ApiError};
use meerschaum_core::{Map, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The accepted Main entry point, never a client job property or callback choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphEntry {
    PipeEndpoint,
    Action,
    StoredJob,
    PipelineEndpoint,
}
/// Exact original route/command requirements, preserved on every new admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphOrigin {
    entry: GraphEntry,
    required: BTreeSet<String>,
}
impl GraphOrigin {
    /// Called only with the Main route's actual accepted policy requirements.
    pub fn capture(entry: GraphEntry, required: BTreeSet<String>) -> Result<Self, ApiError> {
        let origin = Self { entry, required };
        origin.validate()?;
        Ok(origin)
    }
    fn validate(&self) -> Result<(), ApiError> {
        if !self.required.contains("pipes:write")
            || self.required.len() > 16
            || self.required.iter().any(|scope| {
                scope.is_empty() || scope.len() > 64 || scope.chars().any(char::is_control)
            })
            || matches!(
                self.entry,
                GraphEntry::StoredJob | GraphEntry::PipelineEndpoint
            ) && !self.required.contains("jobs:execute")
        {
            return Err(ApiError::new(
                403,
                "The graph lost its exact original scope requirements.",
            ));
        }
        Ok(())
    }
    pub(super) fn is_pipeline_endpoint(&self) -> bool {
        self.entry == GraphEntry::PipelineEndpoint
            && self.required.len() == 2
            && self.required.contains("jobs:execute")
            && self.required.contains("pipes:write")
    }
    pub fn required(&self) -> &BTreeSet<String> {
        &self.required
    }
}

/// Native parsed selection. Main supplies actual root registrations, original
/// typed traversal and once-normalized argv; no request decoder constructs this.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSelection {
    pub sysargs: Vec<String>,
    pub instances: BTreeSet<String>,
    pub roots: BTreeMap<String, Value>,
    pub begin: Option<String>,
    pub end: Option<String>,
    pub parents: Option<Value>,
    pub children: Option<Value>,
    pub verify: bool,
    pub normalized_at: i64,
}
/// Saved Pipeline registration and definition captured by Main before member resolution.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineIdentity {
    pub registry: String,
    pub name: String,
    pub registration: Value,
    pub parameters: Map,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "request",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum GraphRequest {
    Selection(NativeSelection),
    Pipelines {
        selection: NativeSelection,
        definitions: Vec<PipelineIdentity>,
    },
    Empty(NativeSelection),
    Unknown {
        sysargs: Vec<String>,
        reason: String,
    },
}
impl GraphRequest {
    fn sysargs(&self) -> &[String] {
        match self {
            Self::Selection(selection)
            | Self::Empty(selection)
            | Self::Pipelines { selection, .. } => &selection.sysargs,
            Self::Unknown { sysargs, .. } => sysargs,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedGraph {
    pub(super) version: u8,
    pub(super) origin: GraphOrigin,
    pub(super) request: GraphRequest,
    pub(super) snapshot: Option<Snapshot>,
}

/// One canonical graph operation, independent of whether the public entry selected
/// a single pipe, multiple native roots, a saved Pipeline or an empty selection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GraphOperation {
    /// Private legacy endpoint data remains byte-shape compatible.
    Pipe(PipeOperation),
    Direct(super::ActionPipe),
    Stored(super::StoredPipe),
    /// Main-created typed native graph data. Its current runtime state is closed.
    Captured(CapturedGraph),
}
impl From<PipeOperation> for GraphOperation {
    fn from(operation: PipeOperation) -> Self {
        Self::Pipe(operation)
    }
}
impl GraphOperation {
    /// Main can retain an unresolved command without treating it as a non-graph
    /// handler. No worker runs from this closed state.
    pub fn closed(
        origin: GraphOrigin,
        request: GraphRequest,
        snapshot: Option<Snapshot>,
    ) -> Result<Self, ApiError> {
        origin.validate()?;
        let graph = Self::Captured(CapturedGraph {
            version: 1,
            origin,
            request,
            snapshot,
        });
        graph.sysargs()?;
        Ok(graph)
    }
    pub fn snapshot(&self) -> Option<&Snapshot> {
        match self {
            Self::Pipe(pipe) => pipe.metadata.as_ref(),
            Self::Direct(action) => action.pipe.metadata.as_ref(),
            Self::Stored(job) => job.pipe.metadata.as_ref(),
            Self::Captured(graph) => graph.snapshot.as_ref(),
        }
    }
    pub fn snapshot_mut(&mut self) -> Option<&mut Snapshot> {
        match self {
            Self::Pipe(pipe) => pipe.metadata.as_mut(),
            Self::Direct(action) => action.pipe.metadata.as_mut(),
            Self::Stored(job) => job.pipe.metadata.as_mut(),
            Self::Captured(graph) => graph.snapshot.as_mut(),
        }
    }
    pub fn endpoint(&self) -> Option<&PipeOperation> {
        match self {
            Self::Pipe(pipe) => Some(pipe),
            Self::Direct(action) => Some(&action.pipe),
            Self::Stored(job) => Some(&job.pipe),
            _ => None,
        }
    }
    /// Main-only planning admission for an exact typed-root capture. It does not
    /// issue a credential, runtime-ready decision or an unbounded data permit.
    pub async fn authorize_capture(
        &self,
        state: &AppState,
        caller: &Access,
        words: &[String],
    ) -> Result<(), ApiError> {
        match self {
            Self::Captured(_)
                if self.pipeline_selection().is_some() && self.snapshot().is_some() =>
            {
                self.authorize(state, caller, words).await
            }
            Self::Stored(job) if job.pipe.metadata.is_some() => {
                self.authorize(state, caller, words).await
            }
            Self::Direct(action) if action.pipe.metadata.is_some() => {
                self.authorize(state, caller, words).await
            }
            Self::Pipe(pipe) if pipe.metadata.is_some() => {
                pipe.authorize(state, caller, words).await
            }
            _ => Err(ApiError::unavailable(
                "This graph needs its exact native Main capture before issuing node windows.",
            )),
        }
    }
    pub fn required_scopes(&self) -> BTreeSet<String> {
        match self {
            Self::Stored(_) => BTreeSet::from(["jobs:execute".into(), "pipes:write".into()]),
            Self::Direct(_) => BTreeSet::from(["actions:execute".into(), "pipes:write".into()]),
            Self::Pipe(_) => BTreeSet::from(["pipes:write".into()]),
            Self::Captured(graph) => graph.origin.required.clone(),
        }
    }
    /// No new generic variant becomes runnable before its exact Main capture and
    /// native window/runtime proof are implemented. This is not a permission flag.
    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Captured(_)) && self.pipeline_selection().is_none()
    }
    pub fn sysargs(&self) -> Result<Vec<String>, ApiError> {
        match self {
            Self::Pipe(pipe) => pipe.sysargs(),
            Self::Direct(action) => action.pipe.sysargs(),
            Self::Stored(job) => job.worker_args(),
            Self::Captured(graph) => {
                graph.origin.validate()?;
                let words = graph.request.sysargs();
                if (graph.version != 1 && self.pipeline_selection().is_none())
                    || words.is_empty()
                    || words.len() > 4096
                    || words.iter().any(|word| {
                        word.len() > 8192 || word.chars().any(|character| character == '\0')
                    })
                    || words.iter().map(String::len).sum::<usize>() > 128 * 1024
                {
                    return Err(ApiError::new(403, "Invalid captured native graph command."));
                }
                super::super::admission::user_sysargs(words)?;
                Ok(words.to_vec())
            }
        }
    }
    pub async fn authorize(
        &self,
        state: &AppState,
        caller: &Access,
        words: &[String],
    ) -> Result<(), ApiError> {
        match self {
            Self::Pipe(pipe) => pipe.authorize(state, caller, words).await,
            Self::Stored(job) => {
                caller.require(&["jobs:execute", "pipes:write"], &[])?;
                if job.worker_args()? != words || job.pipe.metadata.is_none() {
                    return Err(ApiError::new(
                        403,
                        "The stored graph lost its canonical private worker command or captured registration.",
                    ));
                }
                job.pipe
                    .authorize(state, caller, &job.pipe.sysargs()?)
                    .await
            }
            Self::Direct(action) => {
                caller.require(&["actions:execute", "pipes:write"], &[])?;
                let original = action.input.request_for(&action.pipe.pipe_ref)?;
                if original.verify != action.pipe.verify
                    || original.row_params != action.pipe.row_params
                {
                    return Err(ApiError::new(
                        403,
                        "The canonical action differs from its original selection.",
                    ));
                }
                action.pipe.authorize(state, caller, words).await
            }
            Self::Captured(graph) => {
                graph.origin.validate()?;
                caller.require(
                    &graph
                        .origin
                        .required
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>(),
                    &[],
                )?;
                if self.sysargs()? != words {
                    return Err(ApiError::new(
                        403,
                        "The original native graph command changed.",
                    ));
                }
                if self.pipeline_selection().is_some() {
                    return self.authorize_pipeline(state, caller).await;
                }
                Err(ApiError::unavailable(match &graph.request {
                    GraphRequest::Unknown { .. } => {
                        "Main could not capture this built-in graph; no ordinary data-scope fallback is available."
                    }
                    GraphRequest::Empty(_) => {
                        "The accepted native graph is empty and has no executable data window."
                    }
                    _ => {
                        "This native graph needs its exact Main capture, instructions and native window proof before launch."
                    }
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_pipe_shape_and_required_scope_remain_stable() {
        let pipe = PipeOperation {
            row_params: None,
            pipe_ref: "sql:main|sql:main|metric|None".into(),
            registration: Value::Int(1),
            verify: false,
            begin: None,
            end: None,
            parents: None,
            children: None,
            metadata: None,
        };
        let json = serde_json::to_value(&pipe).unwrap();
        let operation: GraphOperation = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(&operation).unwrap(), json);
        assert_eq!(
            operation.required_scopes(),
            BTreeSet::from(["pipes:write".into()])
        );
        assert!(!operation.is_closed());
        assert_eq!(operation.sysargs().unwrap(), pipe.sysargs().unwrap());
    }
    #[test]
    fn known_unknown_graph_keeps_original_job_scope_and_cannot_become_ordinary_handler() {
        assert!(
            GraphOrigin::capture(
                GraphEntry::StoredJob,
                BTreeSet::from(["pipes:write".into()])
            )
            .is_err()
        );
        let origin = GraphOrigin::capture(
            GraphEntry::StoredJob,
            BTreeSet::from(["pipes:write".into(), "jobs:execute".into()]),
        )
        .unwrap();
        let words = vec!["sync".into(), "pipes".into()];
        let graph = GraphOperation::closed(
            origin,
            GraphRequest::Unknown {
                sysargs: words.clone(),
                reason: "No exact native capture".into(),
            },
            None,
        )
        .unwrap();
        let saved: GraphOperation =
            serde_json::from_value(serde_json::to_value(&graph).unwrap()).unwrap();
        assert!(saved.is_closed());
        assert_eq!(saved.sysargs().unwrap(), words);
        assert_eq!(
            saved.required_scopes(),
            BTreeSet::from(["pipes:write".into(), "jobs:execute".into()])
        );
        assert!(saved.snapshot().is_none());
    }
}
