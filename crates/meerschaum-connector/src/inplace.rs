//! Optional in-place sync seam for instance connectors whose source is the same backend
//! (e.g., SQL `INSERT ... SELECT` without pulling rows into memory).

use meerschaum_core::Map;
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::PipeDtypesContext;

use crate::traits::{DateBound, async_trait};

/// The lower bound of an in-place sync: `''` (sync time minus backtrack), unbounded (`None`),
/// or an explicit datetime/integer bound.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum InplaceBegin {
    /// Default (`begin=''`): the pipe's sync time minus its backtrack interval.
    #[default]
    Default,
    /// No lower bound (`begin=None`).
    Unbounded,
    /// An explicit lower bound.
    At(DateBound),
}

/// Options for [`crate::InstanceConnector::sync_pipe_inplace`].
#[derive(Debug, Clone, PartialEq)]
pub struct InplaceSyncOptions {
    /// `begin`: default, unbounded, or an explicit bound.
    pub begin: InplaceBegin,
    /// `end` (exclusive).
    pub end: Option<DateBound>,
    /// `params`: source filter parameters.
    pub params: Option<Map>,
    /// `check_existing`: diff against existing rows.
    pub check_existing: bool,
    /// `replace`: replace the rows of `[begin, end)` in one transaction instead of diffing.
    pub replace: bool,
}

impl Default for InplaceSyncOptions {
    fn default() -> Self {
        InplaceSyncOptions {
            begin: InplaceBegin::Default,
            end: None,
            params: None,
            check_existing: true,
            replace: false,
        }
    }
}

/// Sees the rows an in-place sync is about to write, before it writes them: the engine records
/// its dirty-range ledger here, so the commit that follows keeps v4's in-place semantics (the
/// definition's column types, no frame round trip).
#[async_trait]
pub trait InplaceObserver: Send + Sync {
    /// `delta` has one row per logical insertion or change (excluding unchanged UPSERTs), with
    /// only the pipe's datetime column (no columns when the pipe has none). `Err` aborts the
    /// sync with nothing merged.
    async fn before_merge(&self, delta: &Frame) -> Result<(), String>;

    /// A replacing in-place sync is about to delete the rows of `[begin, end)` (`None`:
    /// unbounded on that side) and insert `delta`'s rows in their place: called instead of
    /// [`before_merge`](Self::before_merge). The window's rows may be deleted without a
    /// replacement, so a ledger marks the whole window. The default hands over `delta` only.
    async fn before_replace(
        &self,
        delta: &Frame,
        begin: Option<&DateBound>,
        end: Option<&DateBound>,
    ) -> Result<(), String> {
        let _ = (begin, end);
        self.before_merge(delta).await
    }

    /// The pipe's dtypes as this sync reads its table (handed over before the first
    /// [`before_merge`](Self::before_merge) / [`before_replace`](Self::before_replace), and
    /// again after the sync altered the table), so an observer that types the staged rows needs
    /// no catalog read of its own. The default ignores them.
    fn pipe_dtypes(&self, dtypes: &PipeDtypesContext) {
        let _ = dtypes;
    }
}
