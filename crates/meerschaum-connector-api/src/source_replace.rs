//! Two-phase bounded replacement of a registered, canonical SQL source.
//!
//! Requests contain structured selectors and a main-owned opaque lease, never SQL,
//! private spool paths, connector attributes, or reusable credentials.

use serde::{Deserialize, Serialize};

use crate::source_fetch::{self, Bound};

/// Wire version of the prepared source replacement contract.
pub const VERSION: u32 = 1;
/// Replacement summaries and results are small, independently of source size.
pub const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// The operation on one main-owned prepared replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Materialize, validate and summarize a frozen source without destination writes.
    Prepare,
    /// Reauthorize a prepared lease and commit its complete replacement atomically.
    Commit,
    /// Release a prepared lease without changing its destination.
    Close,
}

/// Prepare and commit must use the exact same canonical source request and options.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Prepared source protocol version.
    pub version: u32,
    /// Canonical source identity, bounds, filters and projections. Its registered home pipe
    /// is also the destination; client-local definitions and target overrides are excluded.
    pub source: source_fetch::Request,
    /// Which operation this request performs.
    pub phase: Phase,
    /// None while preparing; an exact opaque main-owned lease for commit/close.
    pub id: Option<String>,
    /// Preserve the sync caller's dtype enforcement choice.
    pub enforce_dtypes: bool,
}

impl Request {
    /// Structural validation only; every phase still requires original-actor authorization.
    pub fn validate(&self) -> Result<(), String> {
        self.source.validate()?;
        if self.version != VERSION || (self.phase == Phase::Prepare) != self.id.is_none() {
            return Err("Invalid prepared source replacement request.".into());
        }
        if let Some(id) = &self.id {
            let uuid = uuid::Uuid::parse_str(id).map_err(|_| "Invalid replacement lease.")?;
            if uuid.is_nil() || id != &uuid.simple().to_string() {
                return Err("Invalid replacement lease.".into());
            }
        }
        if serde_json::to_vec(self)
            .map_err(|_| "Invalid replacement request.")?
            .len()
            > source_fetch::MAX_REQUEST_BYTES
        {
            return Err("The replacement request exceeds its byte limit.".into());
        }
        Ok(())
    }
}

/// Constant-size facts from the exact completed snapshot, after filtering/enforcement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Summary {
    /// Inclusive replacement bound; None means unbounded.
    pub begin: Option<Bound>,
    /// Exclusive replacement bound; None means unbounded.
    pub end: Option<Bound>,
    /// Earliest written axis value, if present.
    pub first: Option<Bound>,
    /// Latest written axis value, if present.
    pub last: Option<Bound>,
    /// Rows within the replacement window.
    pub rows: u64,
    /// Source rows excluded by window/filter semantics.
    pub skipped: u64,
}

impl From<&meerschaum_connector::source_replace::SourceReplaceSummary> for Summary {
    fn from(summary: &meerschaum_connector::source_replace::SourceReplaceSummary) -> Self {
        Self {
            begin: summary.begin.as_ref().map(Bound::from_bound),
            end: summary.end.as_ref().map(Bound::from_bound),
            first: summary.first.as_ref().map(Bound::from_bound),
            last: summary.last.as_ref().map(Bound::from_bound),
            rows: summary.rows,
            skipped: summary.skipped,
        }
    }
}

/// No full-result frame is returned by any replacement phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Response {
    /// Prepared, immutable source lease. Destination data and metadata are unchanged.
    Prepared {
        /// Main-owned opaque lease, bound to original actor/source/destination/query.
        id: String,
        /// Bounded observer summary of the exact prepared rows.
        summary: Summary,
    },
    /// One atomic destination transaction has completed successfully.
    Committed {
        /// Native replacement result, including inserted/deleted counts.
        result: meerschaum_core::success::SyncResult,
    },
    /// The lease was released without a destination mutation.
    Closed,
}

/// Route on the same canonical registered pipe as the ordinary source broker.
pub fn path(pipe: &source_fetch::PipeKeys) -> String {
    format!("{}/replace", source_fetch::path(pipe))
}
