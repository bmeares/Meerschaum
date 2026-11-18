//! Main's canonical native instructions. A request never supplies candidate
//! bounds, watermarks, parent completion or other execution authority.

use crate::{ConnectorError, ConnectorResult};
pub use meerschaum_connector::batch_delivery::GraphBinding;
use serde::{Deserialize, Serialize};

pub const GRAPH_PLAN_PATH: &str = "/_mrsm/execution/graph-plan";
pub const MAX_GRAPH_PLAN_BYTES: usize = 2 * 1024 * 1024;
/// Main's budget for computing native windows, including a dependent node's Ready request.
pub const WINDOW_PLANNING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphPlanRequest {
    Dispatch,
    Pipeline {
        registry: String,
        name: String,
        run_id: String,
    },
    Initial,
    Ready {
        reference: String,
    },
    Verify {
        reference: String,
    },
}
impl GraphPlanRequest {
    pub fn validate(&self) -> ConnectorResult<()> {
        if let Self::Ready { reference } | Self::Verify { reference } = self
            && (reference.len() > 2048
                || reference.split('|').count() != 4
                || reference.split('|').any(str::is_empty)
                || reference.chars().any(char::is_control))
        {
            return Err(ConnectorError::Invalid(
                "Invalid registered graph node reference.".into(),
            ));
        }
        if let Self::Pipeline {
            registry,
            name,
            run_id,
        } = self
            && [registry, name, run_id].iter().any(|value| {
                value.is_empty() || value.len() > 512 || value.chars().any(char::is_control)
            })
        {
            return Err(ConnectorError::Invalid(
                "Invalid Pipeline graph request.".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "instruction",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum GraphInstruction {
    Dispatch {
        verify: bool,
        argv_sha256: String,
    },
    Pipeline {
        definition: serde_json::Value,
        roots: Vec<String>,
    },
    Initial(serde_json::Value),
    Ready(serde_json::Value),
    Verify(serde_json::Value),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPlanReply {
    pub version: u8,
    pub consumer: GraphBinding,
    pub issuer: GraphBinding,
    pub instruction: GraphInstruction,
}
impl GraphPlanReply {
    pub fn validate(&self) -> ConnectorResult<()> {
        if self.version != 1
            || [&self.consumer, &self.issuer].into_iter().any(|binding| {
                binding.grant.is_nil() || binding.launch.is_nil() || binding.revision == 0
            })
        {
            return Err(ConnectorError::Backend(
                "Main returned invalid graph instruction provenance.".into(),
            ));
        }
        Ok(())
    }
}
