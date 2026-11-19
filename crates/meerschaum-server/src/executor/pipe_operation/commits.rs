//! Durable native target-commit evidence. This state belongs ONLY in
//! Main's private canonical Record. A callback body, worker result, ledger entry,
//! environment variable or plugin return value never constructs a Ticket.

use super::{ReadAccess, windows::CallbackBinding};
use crate::error::ApiError;
use meerschaum_connector::{DateBound, mutation::NativeDataEffect};
use meerschaum_core::{Map, Value, success::Bound};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;
#[path = "batches.rs"]
pub mod batches;

const MAX_OUTSTANDING: usize = 256;
const MAX_JOURNAL_BYTES: usize = 4 * 1024 * 1024;
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;

fn unavailable() -> String {
    "The native node has an unfinished or changed commit proof. Do not automatically retry or issue dependent work; retire its owned runtime and inspect the original operation.".into()
}

/// Main derives this through a fresh canonical ReadAccess, never a DTO decoder.
/// Native inference may preserve its seal; arbitrary peer metadata cannot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    consumer: CallbackBinding,
    issuer: CallbackBinding,
    reference: String,
    physical_reference: String,
    registration: Value,
    window_sha256: String,
    effect_sha256: String,
}

pub struct Ticket {
    id: Uuid,
    binding: Binding,
    batch: Option<Arc<batches::Claim>>,
    /// Native per-statement counts retained until this exact target transaction is acknowledged.
    affected: Vec<u64>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn valid_digest(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
fn valid_reference(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 2048
        && text.split('|').count() == 4
        && !text.chars().any(char::is_control)
}
impl Binding {
    fn validate(&self) -> Result<(), String> {
        let callback = |binding: &CallbackBinding| {
            !binding.grant.is_nil() && !binding.launch.is_nil() && binding.context_revision > 0
        };
        if !callback(&self.consumer)
            || !callback(&self.issuer)
            || !valid_reference(&self.reference)
            || !valid_reference(&self.physical_reference)
            || self.registration.is_null()
            || !valid_digest(&self.window_sha256)
            || !valid_digest(&self.effect_sha256)
        {
            return Err(unavailable());
        }
        Ok(())
    }
    pub fn reference(&self) -> &str {
        &self.reference
    }
    pub fn consumer(&self) -> &CallbackBinding {
        &self.consumer
    }
    pub fn issuer(&self) -> &CallbackBinding {
        &self.issuer
    }
}
impl Ticket {
    /// This identity is usable only with this process-local non-deserializable Ticket.
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn binding(&self) -> &Binding {
        &self.binding
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    /// Durable BEFORE COMMIT. After a crash this is UNKNOWN, never never-written.
    Prepared,
    /// Actual native COMMIT returned success. Lack of fresh acceptance still blocks.
    Committed,
    /// Actual native receipt accepted; original HTTP delivery is still outstanding.
    AcceptedAwaitingDelivery,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    id: Uuid,
    binding: Binding,
    phase: Phase,
    affected: u64,
    #[serde(default, skip_serializing_if = "is_false")]
    structural: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    batch: Option<batches::Selector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completed_source: Option<CompletedSource>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletedSource {
    snapshot_sha256: String,
    rows: u64,
    watermark: Option<i64>,
}
impl From<&meerschaum_connector::mutation::commit::PreparedReplacement> for CompletedSource {
    fn from(source: &meerschaum_connector::mutation::commit::PreparedReplacement) -> Self {
        Self {
            snapshot_sha256: source.snapshot_sha256.clone(),
            rows: source.rows,
            watermark: source.watermark,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveredReplacement {
    source: CompletedSource,
    context: batches::Context,
    selector: batches::Selector,
    receipt_sha256: String,
}
/// Compact audit only. Counts/digests cannot prove source exhaustion, unchanged
/// candidates, completion of a chunk, successful parent work or a child watermark.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Audit {
    acknowledged_commits: u64,
    affected_rows: u64,
    rolled_back_attempts: u64,
    chain_sha256: String,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Journal {
    pending: BTreeMap<String, Pending>,
    audit: Audit,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    completed_replacements: BTreeMap<String, DeliveredReplacement>,
    #[serde(default, skip_serializing_if = "batches::Batches::is_empty")]
    batches: batches::Batches,
}

fn bound(value: &DateBound) -> Bound {
    match value {
        DateBound::Int(value) => Bound::Int(*value),
        DateBound::Datetime(value) => Bound::Datetime(value.utc.and_utc()),
    }
}
struct HashWriter {
    digest: Sha256,
    bytes: usize,
    limit: usize,
}
impl std::io::Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|bytes| *bytes <= self.limit)
            .ok_or_else(|| std::io::Error::other("native commit proof exceeds its byte bound"))?;
        self.digest.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn digest(value: &impl Serialize, limit: usize) -> Result<String, String> {
    let mut writer = HashWriter {
        digest: Sha256::new(),
        bytes: 0,
        limit,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| unavailable())?;
    Ok(writer
        .digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
#[derive(Serialize)]
struct Effect<'a> {
    logical: &'a str,
    physical: &'a str,
    registration: &'a Value,
    window: &'a str,
    begin: Option<Bound>,
    end: Option<Bound>,
    params: Option<&'a Map>,
    replacement: bool,
    affected: &'a [u64],
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<SchemaEffect<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_source: Option<CompletedSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch: Option<BatchEffect<'a>>,
}
#[derive(Serialize)]
struct BatchEffect<'a> {
    selector: &'a batches::Selector,
    request_sha256: &'a str,
}
#[derive(Serialize)]
struct SchemaEffect<'a> {
    kind: &'a str,
    before: &'a str,
    after: &'a str,
    statements: &'a str,
}
fn effect_claim(effect: &NativeDataEffect) -> Result<Option<Arc<batches::Claim>>, String> {
    let Some(native) = effect.native_batch() else {
        return Ok(None);
    };
    let claim = native.owner::<batches::Claim>().ok_or_else(unavailable)?;
    if native.reference() != effect.logical_reference()
        || native.request_sha256() != claim.request_sha256()
        || claim.context().reference() != effect.logical_reference()
        || claim.context().registration() != effect.registration()
        || claim.context().window_sha256() != effect.window().proof()
    {
        return Err(unavailable());
    }
    Ok(Some(claim))
}
fn effect_digest(effect: &NativeDataEffect) -> Result<String, String> {
    let claim = effect_claim(effect)?;
    digest(
        &Effect {
            logical: effect.logical_reference(),
            physical: effect.physical_reference(),
            registration: effect.registration(),
            window: effect.window().proof(),
            begin: effect.request().begin.as_ref().map(bound),
            end: effect.request().end.as_ref().map(bound),
            params: effect.request().params.as_ref(),
            replacement: effect.replacement(),
            affected: effect.affected(),
            batch: claim.as_ref().map(|claim| BatchEffect {
                selector: claim.selector(),
                request_sha256: claim.request_sha256(),
            }),
            completed_source: effect.prepared_replacement().map(CompletedSource::from),
            schema: effect.schema().map(|schema| SchemaEffect {
                kind: schema.kind(),
                before: schema.before_sha256(),
                after: schema.after_sha256(),
                statements: schema.statements_sha256(),
            }),
        },
        MAX_REQUEST_BYTES,
    )
}
impl Binding {
    fn batch_context(&self) -> Result<batches::Context, String> {
        batches::Context::from_native(
            self.consumer.clone(),
            self.issuer.clone(),
            self.reference.clone(),
            self.registration.clone(),
            self.window_sha256.clone(),
        )
    }
}
impl ReadAccess {
    /// Pure member/window derivation AFTER the controller's fresh originating source proof.
    pub fn native_batch_context(&self, reference: &str) -> Result<batches::Context, ApiError> {
        let snapshot = self
            .operation
            .snapshot()
            .ok_or_else(|| ApiError::unavailable(unavailable()))?;
        let member = snapshot
            .members()
            .get(reference)
            .filter(|member| member.writable)
            .ok_or_else(|| ApiError::unavailable(unavailable()))?;
        let parts = reference.split('|').collect::<Vec<_>>();
        if parts.len() != 4 {
            return Err(ApiError::new(403, unavailable()));
        }
        let pipe = meerschaum_core::Pipe::builder(parts[1], parts[2])
            .instance(parts[0])
            .location((parts[3] != "None").then_some(parts[3]))
            .parameters(member.parameters.clone())
            .build()
            .map_err(|_| ApiError::new(403, unavailable()))?;
        let window = self.write_window(&pipe)?;
        self.caller.require(
            &self
                .operation
                .required_scopes()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            &[],
        )?;
        batches::Context::from_native(
            self.binding.clone(),
            self.issuer.clone(),
            reference.into(),
            member.registration.clone(),
            window.proof().into(),
        )
        .map_err(ApiError::unavailable)
    }
    /// Called from Main's process-local native adapter with the actual normalized
    /// frame/OLD-row/schema checks already held. No peer supplies effect/counts.
    pub fn native_commit_binding(&self, effect: &NativeDataEffect) -> Result<Binding, ApiError> {
        let snapshot = self
            .operation
            .snapshot()
            .ok_or_else(|| ApiError::new(403, unavailable()))?;
        let member = snapshot
            .members()
            .get(effect.logical_reference())
            .filter(|member| member.writable && &member.registration == effect.registration())
            .ok_or_else(|| ApiError::new(403, unavailable()))?;
        // The process-local target is minted from actual registered logical/physical
        // Pipes only after the host's captured SelectedEngine proof. No request can
        // deserialize one or turn a changed reference string into a native target.
        if effect.physical_reference() != effect.logical_reference()
            && effect.window().native_target().is_none_or(|target| {
                target.logical_reference() != effect.logical_reference()
                    || target.physical_reference() != effect.physical_reference()
                    || target.registration() != effect.registration().py_str()
            })
        {
            return Err(ApiError::unavailable(
                "The native effect has no exact registered target mapping.",
            ));
        }
        let parts = effect.logical_reference().split('|').collect::<Vec<_>>();
        if parts.len() != 4 {
            return Err(ApiError::new(403, unavailable()));
        }
        let pipe = meerschaum_core::Pipe::builder(parts[1], parts[2])
            .instance(parts[0])
            .location((parts[3] != "None").then_some(parts[3]))
            .parameters(member.parameters.clone())
            .build()
            .map_err(|_| ApiError::new(403, unavailable()))?;
        if self.write_window(&pipe)? != effect.window().logical_window() {
            return Err(ApiError::new(403, unavailable()));
        }
        self.caller.require(
            &self
                .operation
                .required_scopes()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            &[],
        )?;
        let binding = Binding {
            consumer: self.binding.clone(),
            issuer: self.issuer.clone(),
            reference: effect.logical_reference().into(),
            physical_reference: effect.physical_reference().into(),
            registration: effect.registration().clone(),
            window_sha256: effect.window().proof().into(),
            effect_sha256: effect_digest(effect).map_err(ApiError::unavailable)?,
        };
        if let Some(claim) = effect_claim(effect).map_err(ApiError::unavailable)?
            && claim.context() != &binding.batch_context().map_err(ApiError::unavailable)?
        {
            return Err(ApiError::unavailable(unavailable()));
        }
        Ok(binding)
    }
}

impl Journal {
    /// Any unfinished attempt blocks automatic retry, restart and dependent issuance.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty() || self.batches.has_pending()
    }
    /// The caller must hold its own exact private Ticket to continue a prepared attempt.
    pub fn has_pending_for(&self, reference: &str) -> bool {
        self.pending.contains_key(reference) || self.batches.has_pending_for(reference)
    }
    /// Validate private disk state before using it, including after an API restart.
    pub fn validate(&self) -> Result<(), String> {
        if self.pending.len() > MAX_OUTSTANDING
            || self.pending.iter().any(|(reference, pending)| {
                reference != &pending.binding.reference
                    || pending.id.is_nil()
                    || pending.affected == 0 && !pending.structural && pending.batch.is_none()
                    || pending.phase == Phase::AcceptedAwaitingDelivery && pending.batch.is_none()
                    || pending.binding.validate().is_err()
            })
            || self.audit.acknowledged_commits == 0 && self.audit.affected_rows != 0
            || if self.audit.acknowledged_commits == 0 && self.audit.rolled_back_attempts == 0 {
                !self.audit.chain_sha256.is_empty()
            } else {
                !valid_digest(&self.audit.chain_sha256)
            }
        {
            return Err(unavailable());
        }
        if self.completed_replacements.len() > MAX_OUTSTANDING
            || self
                .completed_replacements
                .iter()
                .any(|(reference, delivered)| {
                    reference != delivered.context.reference()
                        || !valid_digest(&delivered.source.snapshot_sha256)
                        || !self.batches.is_last_delivery(
                            &delivered.context,
                            &delivered.selector,
                            &delivered.receipt_sha256,
                        )
                        || self.has_pending_for(reference)
                })
        {
            return Err(unavailable());
        }
        if self.pending.values().any(|pending| {
            pending.completed_source.as_ref().is_some_and(|source| {
                pending.batch.is_none() || !valid_digest(&source.snapshot_sha256)
            })
        }) {
            return Err(unavailable());
        }
        self.batches.validate()?;
        for (context, selector, effect, phase) in self.batches.native_pending() {
            let pending = self
                .pending
                .get(context.reference())
                .ok_or_else(unavailable)?;
            let expected = match phase {
                batches::NativePhase::Prepared => Phase::Prepared,
                batches::NativePhase::Committed => Phase::Committed,
                batches::NativePhase::Accepted => Phase::AcceptedAwaitingDelivery,
            };
            if pending.batch.as_ref() != Some(selector)
                || &pending.binding.batch_context()? != context
                || pending.binding.effect_sha256 != effect
                || pending.phase != expected
            {
                return Err(unavailable());
            }
        }
        for pending in self
            .pending
            .values()
            .filter(|pending| pending.batch.is_some())
        {
            if !self
                .batches
                .native_pending()
                .any(|(context, selector, effect, phase)| {
                    pending.batch.as_ref() == Some(selector)
                        && context.reference() == pending.binding.reference
                        && effect == pending.binding.effect_sha256
                        && matches!(
                            (phase, pending.phase),
                            (batches::NativePhase::Prepared, Phase::Prepared)
                                | (batches::NativePhase::Committed, Phase::Committed)
                                | (
                                    batches::NativePhase::Accepted,
                                    Phase::AcceptedAwaitingDelivery
                                )
                        )
                })
            {
                return Err(unavailable());
            }
        }
        let _ = digest(self, MAX_JOURNAL_BYTES)?;
        Ok(())
    }
    fn checked(&self) -> Result<(), String> {
        self.validate()
    }
    pub fn blocks(&self, reference: &str) -> bool {
        self.has_pending_for(reference)
    }
    /// A completed registered source and native watermark for this exact, freshly
    /// admitted window. Delivery of an ordinary worker-supplied frame never sets this.
    pub fn completed_replacement(
        &self,
        context: &batches::Context,
    ) -> Result<Option<Option<i64>>, String> {
        self.validate()?;
        Ok(self
            .completed_replacements
            .get(context.reference())
            .filter(|record| &record.context == context)
            .map(|record| record.source.watermark))
    }
    /// Native control adapters persist+fsync this transition before returning its selector.
    pub fn issue_batch(
        &mut self,
        context: batches::Context,
        predecessor: Option<batches::Selector>,
        request_sha256: String,
    ) -> Result<batches::Issue, String> {
        self.validate()?;
        if self
            .pending
            .get(context.reference())
            .is_some_and(|pending| pending.batch.is_none())
        {
            return Err(unavailable());
        }
        let mut next = self.clone();
        let reference = context.reference().to_string();
        let issued = next.batches.issue(context, predecessor, request_sha256)?;
        if matches!(issued, batches::Issue::Reserved(_)) {
            next.completed_replacements.remove(&reference);
        }
        next.checked()?;
        *self = next;
        Ok(issued)
    }
    /// Actual data-route CAS; no wire Claim/commit assertion exists.
    pub fn claim_batch(
        &mut self,
        context: &batches::Context,
        selector: &batches::Selector,
        actual_sha256: &str,
    ) -> Result<batches::ClaimOutcome, String> {
        self.validate()?;
        let mut next = self.clone();
        let claimed = next.batches.claim(context, selector, actual_sha256)?;
        next.checked()?;
        *self = next;
        Ok(claimed)
    }
    pub fn cancel_reserved_batch(
        &mut self,
        context: &batches::Context,
        selector: &batches::Selector,
        request_sha256: &str,
    ) -> Result<(), String> {
        self.validate()?;
        if self.pending.contains_key(context.reference()) {
            return Err(unavailable());
        }
        let mut next = self.clone();
        next.batches
            .cancel_reserved(context, selector, request_sha256)?;
        next.checked()?;
        *self = next;
        Ok(())
    }
    /// Only the native target runner's awaited rollback can call this owner path.
    /// It also handles persistence completed before prepare's waiter received Ticket.
    pub fn rollback_native_batch(&mut self, claim: Arc<batches::Claim>) -> Result<(), String> {
        self.validate()?;
        if let Some(pending) = self.pending.get(claim.context().reference()) {
            if pending.batch.as_ref() != Some(claim.selector())
                || &pending.binding.batch_context()? != claim.context()
            {
                return Err(unavailable());
            }
            let ticket = Ticket {
                id: pending.id,
                binding: pending.binding.clone(),
                batch: Some(claim),
                affected: Vec::new(),
            };
            return self.rolled_back(&ticket, true);
        }
        let mut next = self.clone();
        next.batches.rolled_back(&claim, true)?;
        next.checked()?;
        *self = next;
        Ok(())
    }
    pub fn confirm_batch_claim(
        &self,
        claim: &batches::Claim,
        fresh: &batches::Context,
    ) -> Result<Option<batches::Receipt>, String> {
        self.validate()?;
        self.batches.confirm_claim(claim, fresh)
    }
    pub fn accepted_batch_receipt(
        &self,
        claim: &batches::Claim,
    ) -> Result<batches::Receipt, String> {
        self.confirm_batch_claim(claim, claim.context())?
            .ok_or_else(unavailable)
    }
    /// Only a claim still atomically proven never handed to a native target can retire here.
    pub fn reject_batch_before_handoff(&mut self, claim: &batches::Claim) -> Result<(), String> {
        self.validate()?;
        if self.pending.contains_key(claim.context().reference()) {
            return Err(unavailable());
        }
        let mut next = self.clone();
        next.batches.reject_before_handoff(claim)?;
        next.checked()?;
        *self = next;
        Ok(())
    }

    /// Catalog write+fsync MUST complete before the Ticket is returned to native
    /// COMMIT. The caller owns the per-node async gate throughout this transition.
    pub fn prepare(
        &mut self,
        binding: Binding,
        effect: &NativeDataEffect,
    ) -> Result<Ticket, String> {
        binding.validate()?;
        self.validate()?;
        let batch = effect_claim(effect)?;
        if !effect.has_effects() && batch.is_none()
            || binding.reference != effect.logical_reference()
            || binding.physical_reference != effect.physical_reference()
            || &binding.registration != effect.registration()
            || binding.window_sha256 != effect.window().proof()
            || effect_digest(effect)? != binding.effect_sha256
            || self.pending.contains_key(&binding.reference)
        {
            return Err(unavailable());
        }
        if let Some(claim) = &batch {
            self.batches
                .confirm_claim(claim, &binding.batch_context()?)?;
        } else if self.batches.has_pending_for(&binding.reference) {
            return Err(unavailable());
        }
        let affected = effect
            .affected()
            .iter()
            .try_fold(0u64, |sum, value| sum.checked_add(*value))
            .ok_or_else(unavailable)?;
        let id = Uuid::new_v4();
        let mut next = self.clone();
        if let Some(claim) = &batch {
            next.batches
                .prepared(claim, binding.effect_sha256.clone())?;
        }
        next.pending.insert(
            binding.reference.clone(),
            Pending {
                id,
                binding: binding.clone(),
                phase: Phase::Prepared,
                affected,
                structural: effect.schema().is_some(),
                completed_source: effect.prepared_replacement().map(CompletedSource::from),
                batch: batch.as_ref().map(|claim| claim.selector().clone()),
            },
        );
        next.checked()?;
        *self = next;
        Ok(Ticket {
            id,
            binding,
            affected: if batch.is_some() {
                effect.affected().to_vec()
            } else {
                Vec::new()
            },
            batch,
        })
    }
    fn pending_mut(&mut self, ticket: &Ticket) -> Result<&mut Pending, String> {
        self.pending
            .get_mut(&ticket.binding.reference)
            .filter(|pending| {
                pending.id == ticket.id
                    && pending.binding == ticket.binding
                    && pending.batch.as_ref() == ticket.batch.as_ref().map(|claim| claim.selector())
            })
            .ok_or_else(unavailable)
    }
    /// Own-ticket precommit proof. A recheck cannot adopt another pending attempt
    /// or reuse a Ticket after its transaction was already observed committed.
    pub fn confirm_prepared(&self, ticket: &Ticket, fresh: &Binding) -> Result<(), String> {
        self.validate()?;
        if &ticket.binding != fresh
            || self
                .pending
                .get(&ticket.binding.reference)
                .is_none_or(|pending| {
                    pending.id != ticket.id
                        || pending.binding != ticket.binding
                        || pending.batch.as_ref()
                            != ticket.batch.as_ref().map(|claim| claim.selector())
                        || pending.phase != Phase::Prepared
                })
        {
            return Err(unavailable());
        }
        Ok(())
    }
    /// Truth recording after actual COMMIT needs the owned native Ticket, not new
    /// data authority. If persistence fails Prepared stays UNKNOWN and blocks.
    pub fn observed_commit(&mut self, ticket: &Ticket) -> Result<(), String> {
        self.validate()?;
        let mut next = self.clone();
        let pending = next.pending_mut(ticket)?;
        if pending.phase == Phase::Prepared {
            pending.phase = Phase::Committed;
        }
        if let Some(claim) = &ticket.batch {
            next.batches.observed_commit(claim)?;
        }
        next.checked()?;
        *self = next;
        Ok(())
    }
    fn compact_accepted(&mut self, pending: Pending) -> Result<(), String> {
        self.audit.acknowledged_commits = self
            .audit
            .acknowledged_commits
            .checked_add(1)
            .ok_or_else(unavailable)?;
        self.audit.affected_rows = self
            .audit
            .affected_rows
            .checked_add(pending.affected)
            .ok_or_else(unavailable)?;
        self.audit.chain_sha256 = digest(&(&self.audit.chain_sha256, pending), MAX_JOURNAL_BYTES)?;
        Ok(())
    }
    /// Fresh native acceptance retains a batched receipt until exact wire delivery.
    /// Ordinary unbatched native providers retain their previous immediate compaction.
    pub fn acknowledge(&mut self, ticket: &Ticket, fresh: &Binding) -> Result<(), String> {
        self.validate()?;
        if &ticket.binding != fresh {
            return Err(unavailable());
        }
        let pending = self.pending_mut(ticket)?.clone();
        if !matches!(
            pending.phase,
            Phase::Committed | Phase::AcceptedAwaitingDelivery
        ) {
            return Err(unavailable());
        }
        let mut next = self.clone();
        if let Some(claim) = &ticket.batch {
            let receipt = batches::Receipt::from_native(
                claim.selector().clone(),
                fresh.effect_sha256.clone(),
                ticket.affected.clone(),
                pending.structural,
            )?;
            next.batches
                .accepted(claim, &fresh.batch_context()?, receipt)?;
            next.pending_mut(ticket)?.phase = Phase::AcceptedAwaitingDelivery;
        } else {
            next.pending.remove(&ticket.binding.reference);
            next.compact_accepted(pending)?;
        }
        next.checked()?;
        *self = next;
        Ok(())
    }
    /// A response-delivery acknowledgement, never EOF/step completion or a new permit.
    pub fn deliver_batch(
        &mut self,
        fresh: &batches::Context,
        selector: &batches::Selector,
        receipt_sha256: &str,
    ) -> Result<(), String> {
        self.validate()?;
        let pending = self
            .pending
            .get(fresh.reference())
            .filter(|pending| pending.batch.as_ref() == Some(selector))
            .cloned();
        if let Some(pending) = &pending
            && (pending.phase != Phase::AcceptedAwaitingDelivery
                || &pending.binding.batch_context()? != fresh)
        {
            return Err(unavailable());
        }
        let mut next = self.clone();
        next.batches.delivered(fresh, selector, receipt_sha256)?;
        if let Some(pending) = pending {
            next.pending.remove(fresh.reference());
            if let Some(source) = &pending.completed_source {
                next.completed_replacements.insert(
                    fresh.reference().to_owned(),
                    DeliveredReplacement {
                        source: source.clone(),
                        context: fresh.clone(),
                        selector: selector.clone(),
                        receipt_sha256: receipt_sha256.to_owned(),
                    },
                );
            }
            next.compact_accepted(pending)?;
        }
        next.checked()?;
        *self = next;
        Ok(())
    }
    /// Only the owned native target's actual awaited rollback may clear either link.
    pub fn rolled_back(&mut self, ticket: &Ticket, succeeded: bool) -> Result<(), String> {
        self.validate()?;
        if !succeeded || self.pending_mut(ticket)?.phase != Phase::Prepared {
            return Err(unavailable());
        }
        let mut next = self.clone();
        let pending = next
            .pending
            .remove(&ticket.binding.reference)
            .ok_or_else(unavailable)?;
        if let Some(claim) = &ticket.batch {
            next.batches.rolled_back(claim, true)?;
        }
        next.audit.rolled_back_attempts = next
            .audit
            .rolled_back_attempts
            .checked_add(1)
            .ok_or_else(unavailable)?;
        next.audit.chain_sha256 = digest(
            &(&self.audit.chain_sha256, pending, "actual_rollback"),
            MAX_JOURNAL_BYTES,
        )?;
        next.checked()?;
        *self = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_connector::{
        SyncOptions,
        mutation::{DataAxis, DataCoverage, DataSpan, DataWindow},
    };

    fn fixture() -> (Binding, NativeDataEffect) {
        let pipe = meerschaum_core::Pipe::builder("sql:main", "commit-fixture")
            .instance("sql:main")
            .build()
            .unwrap();
        let window = DataWindow::new(
            pipe.pipe_ref(),
            "a".repeat(64),
            DataAxis::Integer {
                column: "tick".into(),
            },
            DataCoverage::AxisRanges,
            vec![DataSpan {
                begin: Some(Bound::Int(5)),
                end: Some(Bound::Int(10)),
            }],
            None,
        )
        .unwrap();
        let effect = NativeDataEffect::from_native(
            &pipe,
            Value::Int(1),
            &window,
            &SyncOptions {
                begin: Some(DateBound::Int(5)),
                end: Some(DateBound::Int(10)),
                ..Default::default()
            },
            vec![2, 1],
        )
        .unwrap();
        let callback = CallbackBinding {
            grant: Uuid::new_v4(),
            launch: Uuid::new_v4(),
            context_revision: 1,
        };
        let binding = Binding {
            consumer: callback.clone(),
            issuer: callback,
            reference: pipe.pipe_ref(),
            physical_reference: pipe.pipe_ref(),
            registration: Value::Int(1),
            window_sha256: window.proof().into(),
            effect_sha256: effect_digest(&effect).unwrap(),
        };
        (binding, effect)
    }

    #[test]
    fn a_valid_effect_digest_does_not_override_its_bound_node_registration_or_window() {
        let (binding, effect) = fixture();
        for index in 0..4 {
            let mut changed = binding.clone();
            match index {
                0 => changed.reference = "sql:main|sql:other|foreign|None".into(),
                1 => changed.physical_reference = "sql:other|sql:main|foreign|None".into(),
                2 => changed.registration = Value::Int(2),
                _ => changed.window_sha256 = "b".repeat(64),
            }
            // The effect digest remains valid: each independent binding must match too.
            let mut journal = Journal::default();
            assert!(journal.prepare(changed, &effect).is_err());
            assert!(!journal.has_pending());
        }
    }

    #[test]
    fn a_prepared_attempt_survives_reload_and_neither_missing_ack_nor_failed_rollback_clears_it() {
        let (binding, effect) = fixture();
        let mut journal = Journal::default();
        let ticket = journal.prepare(binding.clone(), &effect).unwrap();
        let mut restored: Journal =
            serde_json::from_slice(&serde_json::to_vec(&journal).unwrap()).unwrap();
        assert!(restored.blocks(&binding.reference));
        assert!(restored.prepare(binding.clone(), &effect).is_err());
        assert!(
            restored.acknowledge(&ticket, &binding).is_err(),
            "Prepared was relabeled committed without native evidence"
        );
        assert!(restored.rolled_back(&ticket, false).is_err());
        assert!(restored.blocks(&binding.reference));
        restored.rolled_back(&ticket, true).unwrap();
        assert!(!restored.blocks(&binding.reference));
        assert_eq!(restored.audit.rolled_back_attempts, 1);
        assert_eq!(restored.audit.acknowledged_commits, 0);
    }

    #[test]
    fn committed_but_changed_authority_or_window_remains_blocked_and_cannot_be_rolled_back() {
        let (binding, effect) = fixture();
        let mut journal = Journal::default();
        let ticket = journal.prepare(binding.clone(), &effect).unwrap();
        journal.observed_commit(&ticket).unwrap();
        let committed = journal.clone();
        let mut stale = binding.clone();
        stale.consumer.context_revision += 1;
        assert!(journal.acknowledge(&ticket, &stale).is_err());
        stale = binding.clone();
        stale.window_sha256 = "b".repeat(64);
        assert!(journal.acknowledge(&ticket, &stale).is_err());
        assert!(
            journal.rolled_back(&ticket, true).is_err(),
            "Actual commit was falsely relabeled rolled back"
        );
        assert_eq!(journal, committed);
        assert!(journal.blocks(&binding.reference));
        journal.acknowledge(&ticket, &binding).unwrap();
        assert!(!journal.blocks(&binding.reference));
        assert_eq!(journal.audit.affected_rows, 3);
        assert_eq!(journal.audit.acknowledged_commits, 1);
    }

    #[test]
    fn completed_attempts_compact_without_a_lifetime_restart_or_write_limit() {
        let (binding, effect) = fixture();
        let mut journal = Journal::default();
        for _ in 0..130 {
            let ticket = journal.prepare(binding.clone(), &effect).unwrap();
            journal.observed_commit(&ticket).unwrap();
            journal.acknowledge(&ticket, &binding).unwrap();
        }
        assert!(journal.pending.is_empty());
        assert_eq!(journal.audit.affected_rows, 390);
        assert_eq!(journal.audit.acknowledged_commits, 130);
        assert!(serde_json::to_vec(&journal).unwrap().len() < 1024);
        // This audit intentionally has no source-complete/parent-succeeded bit.
        // Target counts cannot release a provisional child on their own.
    }

    struct BatchCheck(meerschaum_connector::mutation::NativeBatch);
    #[meerschaum_connector::async_trait]
    impl meerschaum_connector::mutation::MutationCheck for BatchCheck {
        fn native_batch(&self) -> Option<meerschaum_connector::mutation::NativeBatch> {
            Some(self.0.clone())
        }
        async fn check(&self) -> Result<(), String> {
            Ok(())
        }
    }
    async fn batched(
        affected: Vec<u64>,
    ) -> (Journal, Binding, NativeDataEffect, Arc<batches::Claim>) {
        let (mut binding, original) = fixture();
        let mut journal = Journal::default();
        let context = binding.batch_context().unwrap();
        let batches::Issue::Reserved(selector) = journal
            .issue_batch(context.clone(), None, "b".repeat(64))
            .unwrap()
        else {
            panic!("No reservation")
        };
        let batches::ClaimOutcome::Native(claim) = journal
            .claim_batch(&context, &selector, &"b".repeat(64))
            .unwrap()
        else {
            panic!("No native claim")
        };
        let claim = Arc::new(claim);
        let native = meerschaum_connector::mutation::NativeBatch::from_host(
            context.reference().into(),
            claim.request_sha256().into(),
            claim.clone(),
            batches::Claim::begin_target,
        )
        .unwrap();
        native.begin_target().unwrap();
        let check: Arc<dyn meerschaum_connector::mutation::MutationCheck> =
            Arc::new(BatchCheck(native));
        let pipe = meerschaum_core::Pipe::builder("sql:main", "commit-fixture")
            .instance("sql:main")
            .build()
            .unwrap();
        let effect = meerschaum_connector::mutation::scope(check, async {
            NativeDataEffect::from_native(
                &pipe,
                original.registration().clone(),
                original.window(),
                &SyncOptions {
                    begin: Some(DateBound::Int(5)),
                    end: Some(DateBound::Int(10)),
                    ..Default::default()
                },
                affected,
            )
            .unwrap()
        })
        .await;
        binding.effect_sha256 = effect_digest(&effect).unwrap();
        (journal, binding, effect, claim)
    }
    #[tokio::test]
    async fn batched_native_acceptance_survives_reload_until_exact_response_delivery() {
        let (mut journal, binding, effect, claim) = batched(vec![2, 1]).await;
        let ticket = journal.prepare(binding.clone(), &effect).unwrap();
        journal.observed_commit(&ticket).unwrap();
        journal.acknowledge(&ticket, &binding).unwrap();
        assert!(journal.has_pending());
        let receipt = journal.accepted_batch_receipt(&claim).unwrap();
        let bytes = serde_json::to_vec(&journal).unwrap();
        let mut loaded: Journal = serde_json::from_slice(&bytes).unwrap();
        loaded.validate().unwrap();
        assert!(loaded.has_pending_for(binding.reference()));
        assert!(
            matches!(loaded.issue_batch(binding.batch_context().unwrap(),None,"b".repeat(64)).unwrap(),batches::Issue::Accepted(found) if found.sha256()==receipt.sha256())
        );
        assert!(matches!(
            loaded
                .claim_batch(claim.context(), claim.selector(), claim.request_sha256())
                .unwrap(),
            batches::ClaimOutcome::Accepted(_)
        ));
        // Late duplicate truth observation must not downgrade Accepted to Committed.
        loaded.observed_commit(&ticket).unwrap();
        loaded.validate().unwrap();
        let mut foreign = binding.clone();
        foreign.consumer.grant = Uuid::new_v4();
        assert!(
            loaded
                .deliver_batch(
                    &foreign.batch_context().unwrap(),
                    receipt.selector(),
                    receipt.sha256()
                )
                .is_err()
        );
        loaded
            .deliver_batch(
                &binding.batch_context().unwrap(),
                receipt.selector(),
                receipt.sha256(),
            )
            .unwrap();
        assert!(!loaded.has_pending());
        assert_eq!(loaded.audit.acknowledged_commits, 1);
        assert_eq!(loaded.audit.affected_rows, 3);
        loaded
            .deliver_batch(
                &binding.batch_context().unwrap(),
                receipt.selector(),
                receipt.sha256(),
            )
            .unwrap();
        assert_eq!(loaded.audit.acknowledged_commits, 1);
        assert!(
            loaded
                .issue_batch(binding.batch_context().unwrap(), None, "b".repeat(64))
                .is_err(),
            "Same-revision reconstruction replayed delivered work"
        );
        assert!(
            journal.rolled_back(&ticket, true).is_err(),
            "Accepted native truth became rollback"
        );
    }
    #[tokio::test]
    async fn exact_native_batch_can_record_zero_effect_commit_without_inventing_positive_counts() {
        let (mut journal, binding, effect, claim) = batched(vec![0]).await;
        assert!(!effect.has_effects());
        assert!(effect.native_batch().is_some());
        let ticket = journal.prepare(binding.clone(), &effect).unwrap();
        journal.observed_commit(&ticket).unwrap();
        journal.acknowledge(&ticket, &binding).unwrap();
        let receipt = journal.accepted_batch_receipt(&claim).unwrap();
        assert!(journal.has_pending());
        journal
            .deliver_batch(
                &binding.batch_context().unwrap(),
                receipt.selector(),
                receipt.sha256(),
            )
            .unwrap();
        assert_eq!(journal.audit.affected_rows, 0);
        assert_eq!(journal.audit.acknowledged_commits, 1);
        let (plain, old) = fixture();
        let mut unbatched = Journal::default();
        let pipe = meerschaum_core::Pipe::builder("sql:main", "commit-fixture")
            .instance("sql:main")
            .build()
            .unwrap();
        let empty = NativeDataEffect::from_native(
            &pipe,
            old.registration().clone(),
            old.window(),
            &SyncOptions::default(),
            vec![0],
        )
        .unwrap();
        let mut plain = plain;
        plain.effect_sha256 = effect_digest(&empty).unwrap();
        assert!(
            unbatched.prepare(plain, &empty).is_err(),
            "Unowned zero counts minted an intent"
        );
    }
    #[tokio::test]
    async fn unknown_batch_and_actual_target_rollback_keep_intrinsic_disk_links_consistent() {
        let (mut journal, binding, effect, claim) = batched(vec![1]).await;
        let ticket = journal.prepare(binding.clone(), &effect).unwrap();
        assert!(journal.rolled_back(&ticket, false).is_err());
        assert!(matches!(
            journal
                .issue_batch(binding.batch_context().unwrap(), None, "b".repeat(64))
                .unwrap(),
            batches::Issue::Unknown
        ));
        journal.rolled_back(&ticket, true).unwrap();
        assert!(!journal.has_pending());
        // After actual rollback no native pending remains and the retired selector
        // still cannot become a fresh request without its Main predecessor.
        assert!(
            journal
                .issue_batch(binding.batch_context().unwrap(), None, "b".repeat(64))
                .is_err()
        );
        let (mut other, binding, effect, _) = batched(vec![1]).await;
        let ticket = other.prepare(binding, &effect).unwrap();
        other.observed_commit(&ticket).unwrap();
        let mut encoded = serde_json::to_value(&other).unwrap();
        encoded["pending"] = serde_json::json!({});
        assert!(
            serde_json::from_value::<Journal>(encoded)
                .unwrap()
                .validate()
                .is_err(),
            "Detached committed batch lost its native blocker"
        );
        assert!(journal.accepted_batch_receipt(&claim).is_err());
    }
}

// Required actual integration, NOT supplied by this comparison model:
// - private canonical Record Journal field, no public metadata projection;
// - owned spawn_blocking try-lock fsync CAS under exact callback/launch/revision,
//   operation/source/owner/registration/window seal; cloned maps swap only on success;
// - per-node async gate; no blocking filesystem/lock operation on a Tokio worker;
// - Main native PendingDataCommit retains Ticket, records observed commit BEFORE
//   fresh acknowledge, suspends on postcommit failure, never attempts rollback;
// - actual central rollback completion queues exact Ticket cleanup only after its
//   owned writer is known rolled back; Drop/absence/dead PID is not cleanup proof;
// - callback lookup, ready issuance and mutation preflight block unfinished entries
//   even after API/executor restart, including every automatic retry;
// - acknowledged audit counts are NOT parent completion/source EOF receipts.
