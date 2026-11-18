//! Host-owned authorization checks retained through native write waits.
//!
//! Only the API host installs these checks. They are process-local capabilities, not
//! environment flags or pipe parameters. Drivers capture a check before spawning work and
//! retain it on the transaction; task-local context alone does not cross a spawned task.

use meerschaum_core::{Map, Pipe, Value};
use std::future::Future;
use std::sync::Arc;

mod batch;
pub use batch::{
    BatchClaimOutcome, MAX_NATIVE_BATCH_BODY, NativeBatch, NativeBatchReceipt,
    request_digest as native_request_digest,
};
pub mod commit;
mod window;
pub use commit::{NativeCommitReceipt, NativeDataEffect, NativeSchemaEffect, PendingDataCommit};
pub use window::{
    DataAxis, DataCoverage, DataSpan, DataWindow, NativeTarget, RowColumn, RowPredicate,
};

/// A successful native metadata inference, created by the trusted data writer only.
/// It is never decoded from an HTTP body, job property or environment variable.
#[derive(Clone)]
pub struct MetadataInference {
    /// Canonical instance and pipe keys selected by the native writer.
    pub pipe: Pipe,
    /// Stable instance-local registration selected before the metadata write.
    pub registration: Value,
    /// Complete raw parameters compared under the native write lock.
    pub before: Map,
    /// Complete raw parameters committed by native dtype / index-role inference.
    pub after: Map,
}

impl MetadataInference {
    /// Inference cannot change a source, target, access policy, lock or registration.
    pub fn validate(&self) -> Result<(), String> {
        let mut before = self.before.clone();
        let mut after = self.after.clone();
        for key in ["dtypes", "columns", "autoincrement"] {
            before.shift_remove(key);
            after.shift_remove(key);
        }
        if self.registration.is_null()
            || before != after
            || self.pipe.pipe_ref().len() > 2048
            || Value::Map(self.after.clone()).to_json_string().len() > 4 * 1024 * 1024
        {
            return Err("Native inference changed protected pipe metadata.".into());
        }
        Ok(())
    }
}

/// Reconstruct the original caller and canonical object authority before a native write.
#[crate::async_trait]
pub trait MutationCheck: Send + Sync {
    /// Process-local Main batch claim, never selected from a request/environment flag.
    /// Native target adapters retain it through task spawning and actual COMMIT.
    fn native_batch(&self) -> Option<NativeBatch> {
        None
    }

    /// Main's process-local canonical row constraint. Missing means an ordinary caller,
    /// never an implicitly issued window for a present graph operation.
    async fn write_window(&self) -> Result<Option<DataWindow>, String> {
        Ok(None)
    }

    /// Persist a target commit intent before native COMMIT. Windowed native callers
    /// require a returned object; ordinary mutations retain their existing commit path.
    async fn prepare_data_commit(
        &self,
        _effect: &NativeDataEffect,
    ) -> Result<Option<Arc<dyn PendingDataCommit>>, String> {
        Ok(None)
    }

    /// Native driver receipt, emitted only after awaiting cleanup of a scoped write
    /// transaction. Read-only rollback, connection drop and scheduled rollback are not
    /// completion proofs. Wrappers must forward both success and failure to their parent.
    fn rollback_completed(&self, _succeeded: bool) {}

    /// Only the Main data-sync authority opts in. Ordinary PATCH and peer operations do not.
    fn tracks_metadata_inference(&self) -> bool {
        false
    }

    /// Report a committed native metadata transition even if the subsequent data write fails.
    async fn metadata_inferred(&self, _change: MetadataInference) -> Result<(), String> {
        Ok(())
    }

    /// A typed host operation may retain its external database authorization snapshot
    /// while its exact native write lock excludes other commits. Checks whose policy uses
    /// their own uncommitted data must keep the default. Never selected by request metadata.
    fn lock_snapshot_reads(&self) -> bool {
        false
    }

    /// Main proved the physical pipe and the live authorization share this native store.
    fn same_store_innodb_reads(&self) -> bool {
        false
    }

    /// Whether the original request is an ordinary full administrator operation.
    /// Backend compatibility for this request does not grant an execution credential or
    /// a restricted caller the native write path.
    fn direct_administrator(&self) -> bool {
        false
    }

    /// A failure refuses the write. A driver must roll back a transaction it has already opened.
    async fn check(&self) -> Result<(), String>;
}

tokio::task_local! {
    static CHECK: Arc<dyn MutationCheck>;
    static CHECKING: ();
}

/// Run a trusted mutation with its original authorization callback.
pub async fn scope<F: Future>(check: Arc<dyn MutationCheck>, future: F) -> F::Output {
    CHECK.scope(check, future).await
}

/// Capture before task spawning or database acquisition. Authority's own canonical reads
/// run outside this scope so their internal reads cannot recursively acquire a write lock.
pub fn current() -> Option<Arc<dyn MutationCheck>> {
    if CHECKING.try_with(|_| ()).is_ok() {
        return None;
    }
    CHECK.try_with(Arc::clone).ok()
}

/// Native drivers may expose a lock-backed read snapshot only to the trusted check itself.
pub fn rechecking() -> bool {
    CHECKING.try_with(|_| ()).is_ok()
}

/// Recheck a captured capability, including inside a transaction or a spawned driver task.
pub async fn recheck(check: &Arc<dyn MutationCheck>) -> Result<(), String> {
    CHECKING.scope((), check.check()).await
}
/// Reconstruct the current window outside the outer mutation scope, so canonical
/// reads cannot recursively acquire the target writer they are authorizing.
pub async fn current_window(check: &Arc<dyn MutationCheck>) -> Result<Option<DataWindow>, String> {
    CHECKING.scope((), check.write_window()).await
}

/// Validate a whole normalized native frame before any registration, inference or
/// persistent schema side effect. Missing is ordinary only when the captured host
/// authority itself proves there is no graph window.
pub async fn admit_frame(
    pipe: &Pipe,
    frame: &meerschaum_kernel::Frame,
    options: &crate::SyncOptions,
) -> Result<Option<DataWindow>, String> {
    let Some(check) = current() else {
        return Ok(None);
    };
    let window = CHECKING.scope((), check.write_window()).await?;
    if let Some(window) = &window {
        window.validate_frame(pipe, frame, options)?;
    }
    Ok(window)
}
