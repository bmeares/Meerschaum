//! Main-issued batch delivery state. Wire identifiers carry no native commit authority.

use crate::{ConnectorError, ConnectorResult};
pub use meerschaum_connector::batch_delivery::{BatchSelector, GraphBinding};
use serde::{Deserialize, Serialize};

pub const GRAPH_BATCH_PATH: &str = "/_mrsm/execution/graph-batch";
pub const BATCH_HEADER: &str = "x-mrsm-graph-batch";
pub const MAX_BATCH_REQUEST: usize = 4096;
pub const MAX_BATCH_REPLY: usize = 128 * 1024;
pub(crate) fn invalid() -> ConnectorError {
    ConnectorError::Invalid("Invalid native batch identity/provenance.".into())
}
fn reference(value: &str) -> bool {
    value.len() <= 2048
        && value.split('|').count() == 4
        && !value.split('|').any(str::is_empty)
        && !value.chars().any(char::is_control)
}
fn sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchReceipt {
    pub selector: BatchSelector,
    pub sha256: String,
}
impl BatchReceipt {
    pub fn validate(&self) -> ConnectorResult<()> {
        self.selector.validate()?;
        if !sha(&self.sha256) {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum BatchRequest {
    Issue {
        reference: String,
        predecessor: Option<BatchSelector>,
        request_sha256: String,
    },
    Delivered {
        reference: String,
        receipt: BatchReceipt,
    },
}
impl BatchRequest {
    pub fn reference(&self) -> &str {
        match self {
            Self::Issue { reference, .. } | Self::Delivered { reference, .. } => reference,
        }
    }
    pub fn validate(&self) -> ConnectorResult<()> {
        if !reference(self.reference()) {
            return Err(invalid());
        }
        match self {
            Self::Issue {
                predecessor,
                request_sha256,
                ..
            } => {
                if !sha(request_sha256) {
                    return Err(invalid());
                }
                if let Some(previous) = predecessor {
                    previous.validate()?;
                }
            }
            Self::Delivered { receipt, .. } => receipt.validate()?,
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BatchOutcome {
    Reserved {
        selector: BatchSelector,
    },
    /// Actual native COMMIT was accepted, but original response delivery is unproved.
    /// Caller must not issue DML or turn missing original counts into EOF/success.
    AlreadyCommitted {
        receipt: BatchReceipt,
    },
    Unknown,
    Delivered {
        receipt: BatchReceipt,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchReply {
    pub version: u8,
    pub consumer: GraphBinding,
    pub issuer: GraphBinding,
    pub reference: String,
    pub outcome: BatchOutcome,
}
impl BatchReply {
    pub fn validate(&self, request: &BatchRequest) -> ConnectorResult<()> {
        if self.version != 1
            || self.reference != request.reference()
            || [&self.consumer, &self.issuer]
                .into_iter()
                .any(|actor| actor.grant.is_nil() || actor.launch.is_nil() || actor.revision == 0)
        {
            return Err(invalid());
        }
        match (&self.outcome, request) {
            (BatchOutcome::Reserved { selector }, BatchRequest::Issue { .. }) => {
                selector.validate()?
            }
            (BatchOutcome::AlreadyCommitted { receipt }, BatchRequest::Issue { .. }) => {
                receipt.validate()?
            }
            (BatchOutcome::Unknown, BatchRequest::Issue { .. }) => {}
            (
                BatchOutcome::Delivered { receipt },
                BatchRequest::Delivered {
                    receipt: expected, ..
                },
            ) if receipt == expected => receipt.validate()?,
            _ => return Err(invalid()),
        }
        Ok(())
    }
}
