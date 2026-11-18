//! Native commit hook contract. Process-local Rust objects only;
//! never deserialized from API/plugin/environment/result JSON.

use super::DataWindow;
use crate::{DataQuery, SyncOptions};
use meerschaum_core::{Pipe, Value};
use std::sync::Arc;

/// Actual repo-compiled native schema effects in the SAME target transaction.
/// This descriptor is constructed after held schema checks, never decoded from a
/// request, child result, environment or optimistic row count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeSchemaEffect {
    kind: &'static str,
    before_sha256: String,
    after_sha256: String,
    statements_sha256: String,
}
impl NativeSchemaEffect {
    /// Seal the actual native before/after schema and repo-compiled DDL.
    pub fn create_target(
        before: String,
        after: String,
        statements: String,
    ) -> Result<Self, String> {
        Self::new("create_target", before, after, statements)
    }
    /// Record nullable columns added by the native backend in the target transaction.
    pub fn add_columns(before: String, after: String, statements: String) -> Result<Self, String> {
        Self::new("add_columns", before, after, statements)
    }
    fn new(
        kind: &'static str,
        before: String,
        after: String,
        statements: String,
    ) -> Result<Self, String> {
        let valid = |value: &str| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if before == after || !valid(&before) || !valid(&after) || !valid(&statements) {
            return Err(
                "The native schema effect has no exact before/after/statement proof.".into(),
            );
        }
        Ok(Self {
            kind,
            before_sha256: before,
            after_sha256: after,
            statements_sha256: statements,
        })
    }
    /// The fixed repo-owned transition kind.
    pub fn kind(&self) -> &str {
        self.kind
    }
    /// Exact schema observed before the structural statements.
    pub fn before_sha256(&self) -> &str {
        &self.before_sha256
    }
    /// Exact schema observed after the structural statements.
    pub fn after_sha256(&self) -> &str {
        &self.after_sha256
    }
    /// Digest of the exact native structural statements.
    pub fn statements_sha256(&self) -> &str {
        &self.statements_sha256
    }
}

/// SQLite consumed the entire prepared native source for this exact replacement. Only
/// its held writer can supply the resulting integer target watermark. This native
/// object is never accepted from an API body, a worker command, or run history.
#[derive(Clone, Debug)]
pub struct PreparedReplacement {
    pub snapshot_sha256: String,
    pub rows: u64,
    pub watermark: Option<i64>,
}

impl PreparedReplacement {
    /// The first completion adapter supports exactly one bounded integer window on
    /// the original physical registration, without a projection, row filter or remap.
    pub fn covers(window: &DataWindow, options: &SyncOptions) -> bool {
        use super::{DataAxis, DataSpan, RowPredicate};
        use meerschaum_core::success::Bound;
        let (Some(crate::DateBound::Int(begin)), Some(crate::DateBound::Int(end))) =
            (&options.begin, &options.end)
        else {
            return false;
        };
        let exact = DataSpan {
            begin: Some(Bound::Int(*begin)),
            end: Some(Bound::Int(*end)),
        };
        options.replace
            && begin < end
            && options.params.as_ref().is_none_or(|p| p.is_empty())
            && window.native_target().is_none()
            && matches!(window.axis(), DataAxis::Integer { .. })
            && window.row_predicate() == &RowPredicate::All
            && window.spans() == std::slice::from_ref(&exact)
            && window.replacement_spans() == Some(std::slice::from_ref(&exact))
    }
}

/// Actual native target effects about to commit. Main creates a durable intent
/// from this bounded descriptor while the exact writer and authority still hold.
#[derive(Clone, Debug)]
pub struct NativeDataEffect {
    logical_reference: String,
    physical_reference: String,
    registration: Value,
    window: DataWindow,
    request: DataQuery,
    replace: bool,
    /// Target-only native rows_affected, not input rows or TEMP/ledger statements.
    affected: Vec<u64>,
    schema: Option<NativeSchemaEffect>,
    prepared: Option<PreparedReplacement>,
    batch: Option<super::NativeBatch>,
}
impl NativeDataEffect {
    /// Called by a native adapter after its exact locked schema/OLD-row/frame
    /// checks and actual DML, before database COMMIT. A future alias adapter must
    /// use its captured typed SelectedEngine mapping; raw ref replacement refuses.
    pub fn from_native(
        pipe: &Pipe,
        registration: Value,
        window: &DataWindow,
        options: &SyncOptions,
        affected: Vec<u64>,
    ) -> Result<Self, String> {
        if pipe.pipe_ref() != window.native_reference()
            || registration.is_null()
            || affected.len() > 4096
            || window
                .native_target()
                .is_some_and(|target| target.registration() != registration.py_str())
        {
            return Err(
                "The native effect lost its exact Main node/registration/window binding.".into(),
            );
        }
        window.validate_replacement(options)?;
        let batch = super::current().and_then(|check| check.native_batch());
        if batch
            .as_ref()
            .is_some_and(|batch| batch.reference() != window.reference())
        {
            return Err("The native batch belongs to another logical target.".into());
        }
        let _ = affected
            .iter()
            .try_fold(0u64, |total, count| total.checked_add(*count))
            .ok_or("Native affected-row accounting overflowed.")?;
        Ok(Self {
            logical_reference: window.reference().into(),
            physical_reference: pipe.pipe_ref(),
            registration,
            window: window.clone(),
            request: DataQuery {
                begin: options.begin.clone(),
                end: options.end.clone(),
                params: options.params.clone(),
                ..Default::default()
            },
            affected,
            replace: options.replace,
            schema: None,
            prepared: None,
            batch,
        })
    }
    /// Attach actual complete-source evidence from the held native SQLite replacement.
    pub fn with_prepared_replacement(
        mut self,
        prepared: PreparedReplacement,
    ) -> Result<Self, String> {
        if !self.replace
            || self.batch.is_none()
            || self.affected.get(1) != Some(&prepared.rows)
            || self.affected.len() != 2
            || prepared.snapshot_sha256.len() != 64
            || !prepared
                .snapshot_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("The prepared source does not match its native replacement effect.".into());
        }
        self.prepared = Some(prepared);
        Ok(self)
    }
    pub fn prepared_replacement(&self) -> Option<&PreparedReplacement> {
        self.prepared.as_ref()
    }
    /// The same opaque Main request owner retained by the actual target runner.
    pub fn native_batch(&self) -> Option<&super::NativeBatch> {
        self.batch.as_ref()
    }

    pub fn logical_reference(&self) -> &str {
        &self.logical_reference
    }
    pub fn physical_reference(&self) -> &str {
        &self.physical_reference
    }
    pub fn registration(&self) -> &Value {
        &self.registration
    }
    pub fn window(&self) -> &DataWindow {
        &self.window
    }
    pub fn request(&self) -> &DataQuery {
        &self.request
    }
    pub fn replacement(&self) -> bool {
        self.replace
    }
    pub fn affected(&self) -> &[u64] {
        &self.affected
    }
    pub fn has_target_effects(&self) -> bool {
        self.affected.iter().any(|count| *count > 0)
    }
    /// Attach a structural effect verified by the same actual native writer.
    pub fn with_schema(mut self, schema: NativeSchemaEffect) -> Self {
        self.schema = Some(schema);
        self
    }
    /// Actual structural effect, if this native transaction changed its target.
    pub fn schema(&self) -> Option<&NativeSchemaEffect> {
        self.schema.as_ref()
    }
    /// A real schema change remains an effect when target row counts are zero.
    pub fn has_effects(&self) -> bool {
        self.has_target_effects() || self.schema.is_some()
    }
}

/// A Main-created durable intent. Implementations retain an exact canonical source
/// and private Ticket. A dropped object never clears an unfinished durable attempt.
#[crate::async_trait]
pub trait PendingDataCommit: Send + Sync {
    /// Repeat original authority with this exact owned Ticket after persistence, before
    /// COMMIT. New requests cannot adopt a pending attempt through this method.
    async fn before_commit(&self, effect: &NativeDataEffect) -> Result<(), String>;
    /// Record actual successful COMMIT before any fresh acceptance. Losing source authority
    /// cannot erase the observation. This method does not acknowledge parent completion.
    async fn committed(&self, effect: &NativeDataEffect) -> Result<(), String>;
    /// Freshly accept the exact already-committed effect after native metadata inference.
    /// Failure leaves a committed/unknown blocker and cannot request rollback or replay.
    async fn acknowledge(&self, effect: &NativeDataEffect) -> Result<(), String>;
    /// Only an awaited, successful actual native rollback may close this owned attempt.
    /// Connection drop, cancellation, zero rows and error text are not rollback evidence.
    async fn rolled_back(&self) -> Result<(), String>;
}

/// A process-local native receipt. No request/result/env can deserialize it. Drop retains
/// the durable committed/unknown blocker; target counts never mean source EOF or parent done.
pub struct NativeCommitReceipt {
    effect: NativeDataEffect,
    pending: Arc<dyn PendingDataCommit>,
}
impl NativeCommitReceipt {
    /// Called only by a trusted native driver after actual successful COMMIT and durable
    /// committed observation. It does not confer permission to execute any further work.
    pub fn observed(effect: NativeDataEffect, pending: Arc<dyn PendingDataCommit>) -> Self {
        Self { effect, pending }
    }
    /// Target-only native effects, never a worker-reported count.
    pub fn effect(&self) -> &NativeDataEffect {
        &self.effect
    }
    /// Final acceptance follows existing truthful native metadata inference publication.
    /// An error is already committed: never represent it as rollback or retryable absence.
    pub async fn acknowledge(self) -> Result<(), String> {
        super::CHECKING.scope((), self.pending.acknowledge(&self.effect)).await
            .map_err(|_| "Target data already committed, but Main could not accept its durable receipt. Do not automatically retry or issue dependent work; inspect the original canonical operation.".into())
    }
}

/// Reserve durable Main intent INSIDE an admitted native transaction after exact target
/// DML/guards. A scoped positive effect requires a real host object, never empty success.
pub async fn prepare(
    authority: &Arc<dyn super::MutationCheck>,
    effect: &NativeDataEffect,
) -> Result<Option<Arc<dyn PendingDataCommit>>, String> {
    match (authority.native_batch(), effect.native_batch()) {
        (Some(held), Some(claim)) if held.same_claim(claim) => {}
        (None, None) if !effect.has_effects() => return Ok(None),
        (None, None) => {}
        _ => {
            return Err(
                "The native transaction and its target effect have different batch owners.".into(),
            );
        }
    }
    super::recheck(authority).await?;
    let window = super::CHECKING
        .scope((), authority.write_window())
        .await?
        .ok_or("This native target commit has no Main-issued data window.")?;
    if window != *effect.window() {
        return Err("The native target effect differs from its current Main window.".into());
    }
    super::CHECKING.scope((),authority.prepare_data_commit(effect)).await?.map(Some)
        .ok_or_else(||"Main could not durably prepare this native commit; the target must roll back before publication.".into())
}

/// Repeat the exact owned Ticket's source proof after awaited durable persistence.
pub async fn before_commit(
    pending: &dyn PendingDataCommit,
    effect: &NativeDataEffect,
) -> Result<(), String> {
    super::CHECKING
        .scope((), pending.before_commit(effect))
        .await
}
/// Called only after actual COMMIT succeeded. It records truth before fresh receipt acceptance.
pub async fn committed(
    pending: &dyn PendingDataCommit,
    effect: &NativeDataEffect,
) -> Result<(), String> {
    super::CHECKING.scope((), pending.committed(effect)).await
}
/// Called only after the exact actual rollback completed successfully.
pub async fn rolled_back(pending: &dyn PendingDataCommit) -> Result<(), String> {
    super::CHECKING.scope((), pending.rolled_back()).await
}
