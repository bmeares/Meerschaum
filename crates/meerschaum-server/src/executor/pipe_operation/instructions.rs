//! Main-selected native execution instructions, stored atomically with windows.
//! These instructions never substitute for canonical request and mutation checks.

use super::SnapshotMember;
use crate::error::ApiError;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeInstructions {
    plan: Json,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verification: Option<Verification>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verification {
    pub reference: String,
    pub definition_hash: String,
    pub calls: Vec<super::WindowRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_params: Option<meerschaum_core::Map>,
    pub reason: String,
}
fn unavailable() -> ApiError {
    ApiError::unavailable(
        "Main has not issued this graph node's exact executable bounds. A local or worker-proposed plan cannot replace missing Main admission.",
    )
}
impl NativeInstructions {
    pub fn main_plan(&self) -> &Json {
        &self.plan
    }
    /// Called only by the trusted native Main planner, never a request decoder.
    /// Engine types stay in CLI/engine; server stores bounded instruction JSON.
    pub fn from_main(
        plan: Json,
        members: &BTreeMap<String, SnapshotMember>,
    ) -> Result<Self, ApiError> {
        let instructions = Self {
            plan,
            verification: None,
        };
        instructions.validate_structure(members)?;
        Ok(instructions)
    }
    pub fn from_main_verify(
        plan: Json,
        verification: Verification,
        members: &BTreeMap<String, SnapshotMember>,
    ) -> Result<Self, ApiError> {
        if verification.calls.len() > 1024
            || verification.reason.len() > 256
            || verification.definition_hash.is_empty()
            || verification.definition_hash.len() > 256
            || members.iter().filter(|(_, member)| member.writable).count() != 1
            || !members
                .get(&verification.reference)
                .is_some_and(|member| member.writable)
        {
            return Err(unavailable());
        }
        let instructions = Self {
            plan,
            verification: Some(verification),
        };
        instructions.validate_structure(members)?;
        Ok(instructions)
    }
    pub fn verification(&self) -> Option<&Verification> {
        self.verification.as_ref()
    }
    fn validate_structure(
        &self,
        members: &BTreeMap<String, SnapshotMember>,
    ) -> Result<(), ApiError> {
        let nodes = self
            .plan
            .get("nodes")
            .and_then(Json::as_array)
            .ok_or_else(unavailable)?;
        if nodes.len() > 256
            || serde_json::to_vec(&self.plan)
                .map_err(|_| unavailable())?
                .len()
                > 2 * 1024 * 1024 - 4096
        {
            return Err(unavailable());
        }
        let mut seen = BTreeSet::new();
        for node in nodes {
            let reference = node
                .get("pipe_ref")
                .and_then(Json::as_str)
                .ok_or_else(unavailable)?;
            if !seen.insert(reference)
                || !members.get(reference).is_some_and(|member| member.writable)
                || node.get("clear").and_then(Json::as_bool).unwrap_or(false)
                || node.get("action").and_then(Json::as_str) == Some("rebuild")
                || node
                    .get("definition_hash")
                    .and_then(Json::as_str)
                    .is_none_or(|hash| hash.is_empty() || hash.len() > 256)
            {
                return Err(unavailable());
            }
        }
        let selected = members
            .iter()
            .filter_map(|(reference, member)| member.writable.then_some(reference.as_str()))
            .collect::<BTreeSet<_>>();
        if seen != selected {
            return Err(unavailable());
        }
        let edges = self
            .plan
            .get("edges")
            .and_then(Json::as_array)
            .ok_or_else(unavailable)?;
        if edges.len() > 256 * 256 {
            return Err(unavailable());
        }
        let mut seen_edges = BTreeSet::new();
        for edge in edges {
            let edge = edge
                .as_array()
                .filter(|edge| edge.len() == 2)
                .ok_or_else(unavailable)?;
            let parent = edge[0].as_str().ok_or_else(unavailable)?;
            let child = edge[1].as_str().ok_or_else(unavailable)?;
            if parent == child
                || !seen.contains(parent)
                || !seen.contains(child)
                || !seen_edges.insert((parent, child))
            {
                return Err(unavailable());
            }
        }
        Ok(())
    }
    /// The atomic catalog installation verifies the SAME Plan and corresponding
    /// windows. Missing/NotIssued runnable nodes cannot become executable merely
    /// because a whole-plan instruction happened to contain them.
    pub(super) fn validate_members(
        &self,
        members: &BTreeMap<String, SnapshotMember>,
    ) -> Result<(), ApiError> {
        self.validate_structure(members)?;
        if let Some(verification) = &self.verification
            && !verification.calls.is_empty()
        {
            let window = members
                .get(&verification.reference)
                .and_then(|member| member.node_window.as_ref())
                .filter(|window| window.is_issued())
                .ok_or_else(unavailable)?;
            for call in &verification.calls {
                window.source(&meerschaum_connector::DataQuery {
                    begin: call.begin.map(Into::into),
                    end: call.end.map(Into::into),
                    params: verification.row_params.clone(),
                    ..Default::default()
                })?;
            }
        }
        for node in self
            .plan
            .get("nodes")
            .and_then(Json::as_array)
            .ok_or_else(unavailable)?
        {
            let reference = node
                .get("pipe_ref")
                .and_then(Json::as_str)
                .ok_or_else(unavailable)?;
            let action = node
                .get("action")
                .and_then(Json::as_str)
                .ok_or_else(unavailable)?;
            let provisional = node
                .get("provisional")
                .and_then(Json::as_bool)
                .unwrap_or(false);
            if !provisional
                && matches!(action, "root" | "incremental" | "backfill" | "rebuild")
                && (node.get("bounds").and_then(|bounds| bounds.get("begin"))
                    == Some(&Json::String(String::new()))
                    || !members
                        .get(reference)
                        .and_then(|member| member.node_window.as_ref())
                        .is_some_and(|window| window.is_issued()))
            {
                return Err(unavailable());
            }
        }
        Ok(())
    }
    pub fn planned_node(&self, reference: &str) -> Result<Json, ApiError> {
        self.plan
            .get("nodes")
            .and_then(Json::as_array)
            .and_then(|nodes| {
                nodes
                    .iter()
                    .find(|node| node.get("pipe_ref").and_then(Json::as_str) == Some(reference))
            })
            .cloned()
            .ok_or_else(unavailable)
    }
}
