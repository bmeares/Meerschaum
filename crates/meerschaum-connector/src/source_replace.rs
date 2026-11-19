//! Optional bounded replacement of a main-owned registered source snapshot.
//!
//! These types carry structured identity and summary data only. Native SQL, source
//! credentials, private spool paths, and full-result frames never cross this interface.

use meerschaum_core::success::SyncResult;

use crate::{ConnectorResult, DateBound, async_trait};

/// Local transport pairing for two logical aliases of one trusted Main and sealed
/// credential provider. This value is compared locally and is never sent over HTTP.
#[derive(Clone, PartialEq, Eq)]
pub struct SourceReplaceTransport {
    /// Canonical callback origin.
    pub origin: String,
    /// Exact sealed credential provider identity, without reading its bearer.
    pub provider: String,
}

impl std::fmt::Debug for SourceReplaceTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceReplaceTransport")
            .field("origin", &self.origin)
            .field("provider", &"<sealed>")
            .finish()
    }
}

/// Constant-size summary of the exact, completed source snapshot and replacement window.
/// This is never a fabricated delta Frame; observers explicitly opt into this contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceReplaceSummary {
    /// Inclusive replacement bound, or an unbounded lower side.
    pub begin: Option<DateBound>,
    /// Exclusive replacement bound, or an unbounded upper side.
    pub end: Option<DateBound>,
    /// Earliest written datetime/axis value, when one exists.
    pub first: Option<DateBound>,
    /// Latest written datetime/axis value, when one exists.
    pub last: Option<DateBound>,
    /// Rows within the replacement window, after dtype enforcement and filters.
    pub rows: u64,
    /// Source rows outside the window and therefore not written.
    pub skipped: u64,
}

/// An exclusive, bounded main-owned snapshot lease, prepared without destination writes.
/// Commit must reauthorize its original actor and both resources with a fresh credential.
#[async_trait]
pub trait PreparedSourceReplace: Send {
    /// Summary of this immutable prepared replacement.
    fn summary(&self) -> &SourceReplaceSummary;

    /// Commit all schema, metadata, window deletion and bounded row batches in one
    /// destination transaction. Failure/cancellation must roll back that transaction.
    async fn commit(&mut self) -> ConnectorResult<SyncResult>;

    /// Release the prepared snapshot without changing its destination.
    async fn close(&mut self) -> ConnectorResult<()>;

    /// Automatic cleanup after an unsuccessful operation. Remote preparations may
    /// need to retain their lease when the outcome of a submitted commit is unknown.
    async fn abandon(&mut self) -> ConnectorResult<()> {
        self.close().await
    }
}
