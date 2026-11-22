//! Bounded Main-owned per-node delivery state; native transport activation is separate.
//! Actual transaction hooks and fresh original-source admission remain mandatory.
use super::CallbackBinding;
use meerschaum_core::Value;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use uuid::Uuid;

const MAX_NODES: usize = 256;
fn refused() -> String {
    "This native batch has an issued, unknown or undelivered outcome; it cannot be replayed as new work.".into()
}
fn sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn digest(value: &impl Serialize) -> Result<String, String> {
    let bytes = serde_json::to_vec(value).map_err(|_| refused())?;
    if bytes.len() > 1024 * 1024 {
        return Err(refused());
    }
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selector {
    pub stream: Uuid,
    pub ordinal: u64,
    pub batch: Uuid,
}
impl Selector {
    fn valid(&self) -> bool {
        !self.stream.is_nil() && !self.batch.is_nil() && self.ordinal > 0
    }
}
/// Main's exact current canonical member, never decoded from a request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    consumer: CallbackBinding,
    issuer: CallbackBinding,
    reference: String,
    registration: Value,
    window_sha256: String,
}
impl Context {
    pub fn from_native(
        consumer: CallbackBinding,
        issuer: CallbackBinding,
        reference: String,
        registration: Value,
        window_sha256: String,
    ) -> Result<Self, String> {
        let context = Self {
            consumer,
            issuer,
            reference,
            registration,
            window_sha256,
        };
        context.validate()?;
        Ok(context)
    }
    fn validate(&self) -> Result<(), String> {
        if [&self.consumer, &self.issuer].into_iter().any(|actor| {
            actor.grant.is_nil() || actor.launch.is_nil() || actor.context_revision == 0
        }) || self.reference.len() > 2048
            || self.reference.split('|').count() != 4
            || self.reference.chars().any(char::is_control)
            || self.registration.is_null()
            || !sha(&self.window_sha256)
        {
            return Err(refused());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    selector: Selector,
    effect_sha256: String,
    receipt_sha256: String,
    affected: Vec<u64>,
    structural: bool,
}
impl Receipt {
    pub fn selector(&self) -> &Selector {
        &self.selector
    }
    pub fn sha256(&self) -> &str {
        &self.receipt_sha256
    }
    pub fn from_native(
        selector: Selector,
        effect_sha256: String,
        affected: Vec<u64>,
        structural: bool,
    ) -> Result<Self, String> {
        if !selector.valid()
            || !sha(&effect_sha256)
            || affected.len() > 4096
            || affected
                .iter()
                .try_fold(0u64, |sum, value| sum.checked_add(*value))
                .is_none()
        {
            return Err(refused());
        }
        let receipt_sha256 = digest(&(&selector, &effect_sha256, &affected, structural))?;
        Ok(Self {
            selector,
            effect_sha256,
            receipt_sha256,
            affected,
            structural,
        })
    }
    fn valid(&self) -> bool {
        self.selector.valid()
            && sha(&self.effect_sha256)
            && self.affected.len() <= 4096
            && self
                .affected
                .iter()
                .try_fold(0u64, |sum, value| sum.checked_add(*value))
                .is_some()
            && matches!(digest(&(&self.selector,&self.effect_sha256,&self.affected,self.structural)),Ok(sha) if sha==self.receipt_sha256)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Reserved,
    Applying,
    Prepared,
    Committed,
    AcceptedAwaitingDelivery,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NativePhase {
    Prepared,
    Committed,
    Accepted,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Active {
    context: Context,
    selector: Selector,
    predecessor: Option<Selector>,
    request_sha256: String,
    phase: Phase,
    claim: Option<Uuid>,
    effect_sha256: Option<String>,
    receipt: Option<Receipt>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stream {
    id: Uuid,
    origin: Context,
    next: u64,
    retired_through: u64,
    last_retired: Option<Selector>,
    /// Bounded proof for retrying the most recently delivered acknowledgement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_delivery_sha256: Option<String>,
    audit_sha256: String,
    active: Option<Active>,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batches {
    nodes: BTreeMap<String, Stream>,
}
/// No Clone/serde/public fields. Dropping a claim never clears durable work.
pub struct Claim {
    context: Context,
    selector: Selector,
    request_sha256: String,
    nonce: Uuid,
    started: std::sync::atomic::AtomicU8,
}
impl Claim {
    pub fn context(&self) -> &Context {
        &self.context
    }
    pub fn selector(&self) -> &Selector {
        &self.selector
    }
    pub fn request_sha256(&self) -> &str {
        &self.request_sha256
    }
    /// Main's actual target runner, after durable claim reservation. The shared
    /// atomic state excludes a concurrent no-handoff retirement.
    pub fn never_started(&self) -> bool {
        self.started.load(std::sync::atomic::Ordering::Acquire) == 0
    }
    pub fn begin_target(&self) -> Result<(), String> {
        self.started
            .compare_exchange(
                0,
                1,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .map_err(|_| refused())?;
        Ok(())
    }
}
impl Context {
    pub fn reference(&self) -> &str {
        &self.reference
    }
    pub fn consumer(&self) -> &CallbackBinding {
        &self.consumer
    }
    pub fn issuer(&self) -> &CallbackBinding {
        &self.issuer
    }
    pub fn registration(&self) -> &Value {
        &self.registration
    }
    pub fn window_sha256(&self) -> &str {
        &self.window_sha256
    }
}
pub enum Issue {
    Reserved(Selector),
    Accepted(Receipt),
    Unknown,
}
pub enum ClaimOutcome {
    Native(Claim),
    Accepted(Receipt),
    Unknown,
}
impl Batches {
    pub(super) fn is_last_delivery(
        &self,
        context: &Context,
        selector: &Selector,
        receipt: &str,
    ) -> bool {
        self.nodes.get(context.reference()).is_some_and(|stream| {
            stream.last_retired.as_ref() == Some(selector)
                && digest(&(context, selector, receipt))
                    .is_ok_and(|proof| stream.last_delivery_sha256.as_ref() == Some(&proof))
        })
    }
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    pub(super) fn native_pending(
        &self,
    ) -> impl Iterator<Item = (&Context, &Selector, &str, NativePhase)> {
        self.nodes
            .values()
            .filter_map(|stream| stream.active.as_ref())
            .filter_map(|active| {
                let phase = match active.phase {
                    Phase::Prepared => NativePhase::Prepared,
                    Phase::Committed => NativePhase::Committed,
                    Phase::AcceptedAwaitingDelivery => NativePhase::Accepted,
                    _ => return None,
                };
                Some((
                    &active.context,
                    &active.selector,
                    active.effect_sha256.as_deref()?,
                    phase,
                ))
            })
    }
    pub fn has_pending(&self) -> bool {
        self.nodes.values().any(|stream| stream.active.is_some())
    }
    pub fn has_pending_for(&self, reference: &str) -> bool {
        self.nodes
            .get(reference)
            .is_some_and(|stream| stream.active.is_some())
    }
    /// Only the actual owner's nonce can continue this exact claimed request.
    pub fn confirm_claim(&self, claim: &Claim, fresh: &Context) -> Result<Option<Receipt>, String> {
        self.validate()?;
        let active = self
            .nodes
            .get(&fresh.reference)
            .and_then(|stream| stream.active.as_ref())
            .filter(|active| {
                fresh == &claim.context
                    && &active.context == fresh
                    && active.selector == claim.selector
                    && active.request_sha256 == claim.request_sha256
                    && active.claim == Some(claim.nonce)
                    && active.phase != Phase::Reserved
            })
            .ok_or_else(refused)?;
        Ok(active.receipt.clone())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.nodes.len() > MAX_NODES {
            return Err(refused());
        }
        for (reference, stream) in &self.nodes {
            stream.origin.validate()?;
            if &stream.origin.reference != reference
                || stream.id.is_nil()
                || stream.next == 0
                || stream.retired_through >= stream.next
                || !stream.audit_sha256.is_empty() && !sha(&stream.audit_sha256)
                || stream.retired_through > 0 && stream.audit_sha256.is_empty()
                || stream
                    .last_delivery_sha256
                    .as_ref()
                    .is_some_and(|digest| stream.last_retired.is_none() || !sha(digest))
                || (stream.retired_through > 0) != stream.last_retired.is_some()
                || stream.last_retired.as_ref().is_some_and(|last| {
                    !last.valid()
                        || last.stream != stream.id
                        || last.ordinal != stream.retired_through
                })
            {
                return Err(refused());
            }
            if stream.active.is_none() && stream.retired_through.checked_add(1) != Some(stream.next)
            {
                return Err(refused());
            }
            if let Some(active) = &stream.active {
                active.context.validate()?;
                if &active.context.reference != reference
                    || active.context.issuer != stream.origin.issuer
                    || active.context.registration != stream.origin.registration
                    || active.predecessor != stream.last_retired
                    || !active.selector.valid()
                    || active.selector.stream != stream.id
                    || active.selector.ordinal <= stream.retired_through
                    || active.selector.ordinal.checked_add(1) != Some(stream.next)
                    || !sha(&active.request_sha256)
                    || matches!(active.phase, Phase::Reserved) != (active.claim.is_none())
                    || active.claim.is_some_and(|id| id.is_nil())
                    || active
                        .effect_sha256
                        .as_ref()
                        .is_some_and(|value| !sha(value))
                    || matches!(
                        active.phase,
                        Phase::Prepared | Phase::Committed | Phase::AcceptedAwaitingDelivery
                    ) != active.effect_sha256.is_some()
                    || matches!(active.phase, Phase::AcceptedAwaitingDelivery)
                        != active.receipt.is_some()
                    || active.receipt.as_ref().is_some_and(|receipt| {
                        !receipt.valid()
                            || receipt.selector != active.selector
                            || Some(&receipt.effect_sha256) != active.effect_sha256.as_ref()
                    })
                {
                    return Err(refused());
                }
            }
        }
        let _ = digest(self)?;
        Ok(())
    }
    /// Persist this transition before returning any selector to a native client.
    pub fn issue(
        &mut self,
        context: Context,
        predecessor: Option<Selector>,
        request_sha256: String,
    ) -> Result<Issue, String> {
        self.validate()?;
        context.validate()?;
        if !sha(&request_sha256) {
            return Err(refused());
        }
        if !self.nodes.contains_key(&context.reference) && self.nodes.len() >= MAX_NODES {
            return Err(refused());
        }
        let stream = self
            .nodes
            .entry(context.reference.clone())
            .or_insert_with(|| Stream {
                id: Uuid::new_v4(),
                origin: context.clone(),
                next: 1,
                retired_through: 0,
                last_retired: None,
                last_delivery_sha256: None,
                audit_sha256: String::new(),
                active: None,
            });
        if let Some(active) = &stream.active {
            if active.context != context
                || active.predecessor != predecessor
                || active.request_sha256 != request_sha256
            {
                return Err(refused());
            }
            return Ok(match active.phase {
                Phase::Reserved => Issue::Reserved(active.selector.clone()),
                Phase::AcceptedAwaitingDelivery => {
                    Issue::Accepted(active.receipt.clone().ok_or_else(refused)?)
                }
                _ => Issue::Unknown,
            });
        }
        if context.issuer != stream.origin.issuer {
            // Only a newly Main-admitted canonical issuer revision can restart
            // without a predecessor. Same-revision controlled rebuilds must not
            // blindly replay already delivered source chunks from ordinal one.
            if predecessor.is_some()
                || context.issuer.launch != stream.origin.issuer.launch
                || context.issuer.context_revision <= stream.origin.issuer.context_revision
            {
                return Err(refused());
            }
            let audit = digest(&(
                &stream.audit_sha256,
                &stream.last_retired,
                "fresh_canonical_revision",
            ))?;
            *stream = Stream {
                id: Uuid::new_v4(),
                origin: context.clone(),
                next: 1,
                retired_through: 0,
                last_retired: None,
                last_delivery_sha256: None,
                audit_sha256: audit,
                active: None,
            };
        } else if context.registration != stream.origin.registration
            || predecessor != stream.last_retired
        {
            return Err(refused());
        }
        stream.origin = context.clone();
        let selector = Selector {
            stream: stream.id,
            ordinal: stream.next,
            batch: Uuid::new_v4(),
        };
        stream.next = stream.next.checked_add(1).ok_or_else(refused)?;
        stream.active = Some(Active {
            context,
            selector: selector.clone(),
            predecessor,
            request_sha256,
            phase: Phase::Reserved,
            claim: None,
            effect_sha256: None,
            receipt: None,
        });
        self.validate()?;
        Ok(Issue::Reserved(selector))
    }
    /// Only exact Reserved can be cancelled after a failed final source check.
    /// A competing Applying claim wins and retains its own unknown outcome.
    pub fn cancel_reserved(
        &mut self,
        context: &Context,
        selector: &Selector,
        request_sha256: &str,
    ) -> Result<(), String> {
        self.validate()?;
        let active = self
            .nodes
            .get(context.reference())
            .and_then(|stream| stream.active.as_ref())
            .filter(|active| {
                active.context == *context
                    && active.selector == *selector
                    && active.request_sha256 == request_sha256
                    && active.phase == Phase::Reserved
            })
            .ok_or_else(refused)?;
        let _ = active;
        self.retire(context.reference(), selector, "reserved_not_handed_off")
    }
    /// Original/final header/source checks surround the awaited durable Catalog CAS.
    /// The real body/options digest is calculated by Main, not accepted from a header.
    pub fn claim(
        &mut self,
        context: &Context,
        selector: &Selector,
        actual_request_sha256: &str,
    ) -> Result<ClaimOutcome, String> {
        self.validate()?;
        let active = self
            .nodes
            .get_mut(&context.reference)
            .and_then(|stream| stream.active.as_mut())
            .filter(|active| {
                &active.context == context
                    && &active.selector == selector
                    && active.request_sha256 == actual_request_sha256
            })
            .ok_or_else(refused)?;
        if active.phase == Phase::AcceptedAwaitingDelivery {
            return Ok(ClaimOutcome::Accepted(
                active.receipt.clone().ok_or_else(refused)?,
            ));
        }
        if active.phase != Phase::Reserved {
            return Ok(ClaimOutcome::Unknown);
        }
        let nonce = Uuid::new_v4();
        active.claim = Some(nonce);
        active.phase = Phase::Applying;
        let claim = Claim {
            context: context.clone(),
            selector: selector.clone(),
            request_sha256: actual_request_sha256.into(),
            nonce,
            started: std::sync::atomic::AtomicU8::new(0),
        };
        self.validate()?;
        Ok(ClaimOutcome::Native(claim))
    }
    fn active_owned(&mut self, claim: &Claim) -> Result<&mut Active, String> {
        self.nodes
            .get_mut(&claim.context.reference)
            .and_then(|stream| stream.active.as_mut())
            .filter(|active| {
                active.context == claim.context
                    && active.selector == claim.selector
                    && active.request_sha256 == claim.request_sha256
                    && active.claim == Some(claim.nonce)
            })
            .ok_or_else(refused)
    }
    /// Called by the native target adapter before it acquires/uses its real writer.
    pub fn mark_native_started(&self, claim: &Claim) -> Result<(), String> {
        self.validate()?;
        let active = self
            .nodes
            .get(&claim.context.reference)
            .and_then(|stream| stream.active.as_ref())
            .filter(|active| {
                active.context == claim.context
                    && active.selector == claim.selector
                    && active.claim == Some(claim.nonce)
                    && active.phase == Phase::Applying
            })
            .ok_or_else(refused)?;
        let _ = active;
        claim.begin_target()
    }
    /// In the SAME durable Catalog write as actual Journal::prepare, before COMMIT.
    pub fn prepared(&mut self, claim: &Claim, effect_sha256: String) -> Result<(), String> {
        self.validate()?;
        if claim.started.load(std::sync::atomic::Ordering::Acquire) != 1 || !sha(&effect_sha256) {
            return Err(refused());
        }
        let active = self.active_owned(claim)?;
        if active.phase != Phase::Applying {
            return Err(refused());
        }
        active.effect_sha256 = Some(effect_sha256);
        active.phase = Phase::Prepared;
        self.validate()
    }
    /// Only actual native COMMIT success: deliberately no fresh authority requirement.
    pub fn observed_commit(&mut self, claim: &Claim) -> Result<(), String> {
        let active = self.active_owned(claim)?;
        match active.phase {
            Phase::Prepared => active.phase = Phase::Committed,
            Phase::Committed | Phase::AcceptedAwaitingDelivery => {}
            _ => return Err(refused()),
        }
        self.validate()
    }
    /// Retain accepted truth until typed delivery; this does not issue child readiness.
    pub fn accepted(
        &mut self,
        claim: &Claim,
        fresh: &Context,
        receipt: Receipt,
    ) -> Result<(), String> {
        self.validate()?;
        if fresh != &claim.context || !receipt.valid() {
            return Err(refused());
        }
        let active = self.active_owned(claim)?;
        if receipt.selector != active.selector
            || active.effect_sha256.as_ref() != Some(&receipt.effect_sha256)
        {
            return Err(refused());
        }
        if active.phase == Phase::AcceptedAwaitingDelivery {
            return if active.receipt.as_ref() == Some(&receipt) {
                Ok(())
            } else {
                Err(refused())
            };
        }
        if active.phase != Phase::Committed {
            return Err(refused());
        }
        active.receipt = Some(receipt);
        active.phase = Phase::AcceptedAwaitingDelivery;
        self.validate()
    }
    fn retire(&mut self, reference: &str, selector: &Selector, event: &str) -> Result<(), String> {
        let stream = self.nodes.get_mut(reference).ok_or_else(refused)?;
        let active = stream
            .active
            .as_ref()
            .filter(|active| &active.selector == selector)
            .ok_or_else(refused)?;
        let audit = digest(&(&stream.audit_sha256, active, event))?;
        stream.last_delivery_sha256 = active
            .receipt
            .as_ref()
            .map(|receipt| digest(&(&active.context, selector, receipt.sha256())))
            .transpose()?;
        stream.retired_through = selector.ordinal;
        stream.last_retired = Some(selector.clone());
        stream.audit_sha256 = audit;
        stream.active = None;
        self.validate()
    }
    pub fn delivered(
        &mut self,
        fresh: &Context,
        selector: &Selector,
        receipt_sha256: &str,
    ) -> Result<(), String> {
        self.validate()?;
        let stream = self.nodes.get(&fresh.reference).ok_or_else(refused)?;
        if selector.stream != stream.id {
            return Err(refused());
        }
        if selector.ordinal <= stream.retired_through {
            // A retry can confirm only the original consumer's exact delivery.
            return if self.is_last_delivery(fresh, selector, receipt_sha256) {
                Ok(())
            } else {
                Err(refused())
            };
        }
        let active = stream
            .active
            .as_ref()
            .filter(|active| {
                &active.context == fresh
                    && &active.selector == selector
                    && active.phase == Phase::AcceptedAwaitingDelivery
                    && active
                        .receipt
                        .as_ref()
                        .is_some_and(|receipt| receipt.receipt_sha256 == receipt_sha256)
            })
            .ok_or_else(refused)?;
        let _ = active;
        self.retire(&fresh.reference, selector, "delivered")
    }
    /// Only the host's exact claim proven never handed to any native writer.
    pub fn reject_before_handoff(&mut self, claim: &Claim) -> Result<(), String> {
        if self.active_owned(claim)?.phase != Phase::Applying {
            return Err(refused());
        }
        // CAS excludes a simultaneous native handoff, not merely a best-effort
        // read of an atomic flag followed by an awaited catalog mutation.
        claim
            .started
            .compare_exchange(
                0,
                2,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .map_err(|_| refused())?;
        self.retire(&claim.context.reference, &claim.selector, "not_handed_off")
    }
    /// The native owner's awaited REAL rollback, never generic/probe rollback.
    pub fn rolled_back(&mut self, claim: &Claim, succeeded: bool) -> Result<(), String> {
        if !succeeded
            || claim.started.load(std::sync::atomic::Ordering::Acquire) != 1
            || !matches!(
                self.active_owned(claim)?.phase,
                Phase::Applying | Phase::Prepared
            )
        {
            return Err(refused());
        }
        self.retire(
            &claim.context.reference,
            &claim.selector,
            "actual_target_rollback",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn context() -> Context {
        let actor = CallbackBinding {
            grant: Uuid::new_v4(),
            launch: Uuid::new_v4(),
            context_revision: 1,
        };
        Context::from_native(
            actor.clone(),
            actor,
            "sql:main|manual:fixture|delivery|None".into(),
            Value::Int(1),
            "a".repeat(64),
        )
        .unwrap()
    }
    fn reserve(model: &mut Batches, context: &Context) -> Selector {
        let Issue::Reserved(selector) = model.issue(context.clone(), None, "b".repeat(64)).unwrap()
        else {
            panic!("No reservation")
        };
        selector
    }
    fn native_claim(model: &mut Batches, context: &Context, selector: &Selector) -> Claim {
        let ClaimOutcome::Native(claim) = model.claim(context, selector, &"b".repeat(64)).unwrap()
        else {
            panic!("No native claim")
        };
        claim
    }
    fn reload(model: &Batches) -> Batches {
        serde_json::from_slice(&serde_json::to_vec(model).unwrap()).unwrap()
    }
    #[test]
    fn postcommit_lost_response_returns_retained_receipt_without_a_second_native_claim() {
        let mut model = Batches::default();
        let context = context();
        let selector = reserve(&mut model, &context);
        assert_eq!(
            reserve(&mut model, &context),
            selector,
            "Lost reservation response allocated a new ID"
        );
        let claim = native_claim(&mut model, &context, &selector);
        model.mark_native_started(&claim).unwrap();
        assert!(matches!(
            reload(&model)
                .claim(&context, &selector, &"b".repeat(64))
                .unwrap(),
            ClaimOutcome::Unknown
        ));
        model.prepared(&claim, "c".repeat(64)).unwrap();
        model.observed_commit(&claim).unwrap();
        let receipt =
            Receipt::from_native(selector.clone(), "c".repeat(64), vec![1], false).unwrap();
        model.accepted(&claim, &context, receipt.clone()).unwrap();
        // HTTP response deliberately not delivered: private state survives reload.
        let mut restored = reload(&model);
        restored.validate().unwrap();
        assert!(restored.has_pending());
        assert!(
            matches!(restored.issue(context.clone(),None,"b".repeat(64)).unwrap(),Issue::Accepted(found) if found==receipt)
        );
        assert!(
            matches!(restored.claim(&context,&selector,&"b".repeat(64)).unwrap(),ClaimOutcome::Accepted(found) if found==receipt)
        );
        assert!(
            restored
                .issue(context.clone(), None, "d".repeat(64))
                .is_err()
        );
        restored
            .delivered(&context, &selector, &receipt.receipt_sha256)
            .unwrap();
        assert!(!restored.has_pending());
        restored
            .delivered(&context, &selector, &receipt.receipt_sha256)
            .unwrap();
        assert!(
            restored
                .issue(context.clone(), None, "b".repeat(64))
                .is_err(),
            "Same-revision process reconstruction replayed an already delivered chunk"
        );
        let Issue::Reserved(next) = restored
            .issue(context.clone(), Some(selector.clone()), "b".repeat(64))
            .unwrap()
        else {
            panic!("No next reservation")
        };
        assert!(
            restored
                .issue(context.clone(), None, "b".repeat(64))
                .is_err(),
            "An older identical issue request adopted its successor reservation"
        );
        assert_eq!(next.ordinal, selector.ordinal + 1);
        assert_ne!(next.batch, selector.batch);
        assert!(
            restored
                .claim(&context, &selector, &"b".repeat(64))
                .is_err(),
            "Retired selector became a new native attempt"
        );
    }
    #[test]
    fn unknown_commit_and_revoked_ack_keep_same_node_blocked_across_consumer_changes() {
        let mut model = Batches::default();
        let original = context();
        let selector = reserve(&mut model, &original);
        let claim = native_claim(&mut model, &original, &selector);
        model.mark_native_started(&claim).unwrap();
        model.prepared(&claim, "c".repeat(64)).unwrap();
        let mut derived = original.clone();
        derived.consumer.grant = Uuid::new_v4();
        assert!(
            reload(&model)
                .issue(derived.clone(), None, "b".repeat(64))
                .is_err()
        );
        assert!(model.rolled_back(&claim, false).is_err());
        model.observed_commit(&claim).unwrap();
        assert!(
            model.rolled_back(&claim, true).is_err(),
            "Committed truth was relabeled rollback"
        );
        let receipt =
            Receipt::from_native(selector.clone(), "c".repeat(64), vec![1], false).unwrap();
        assert!(model.accepted(&claim, &derived, receipt.clone()).is_err());
        assert!(matches!(
            reload(&model)
                .issue(original.clone(), None, "b".repeat(64))
                .unwrap(),
            Issue::Unknown
        ));
        model.accepted(&claim, &original, receipt.clone()).unwrap();
        assert!(
            model
                .delivered(&derived, &selector, &receipt.receipt_sha256)
                .is_err()
        );
        assert!(model.has_pending());
    }
    #[test]
    fn no_handoff_cancel_excludes_native_start_and_only_actual_owned_rollback_retires_started_work()
    {
        let mut model = Batches::default();
        let original = context();
        let selector = reserve(&mut model, &original);
        let claim = native_claim(&mut model, &original, &selector);
        model.reject_before_handoff(&claim).unwrap();
        assert!(model.mark_native_started(&claim).is_err());
        let Issue::Reserved(next) = model
            .issue(original.clone(), Some(selector), "b".repeat(64))
            .unwrap()
        else {
            panic!("No next reservation")
        };
        let claim = native_claim(&mut model, &original, &next);
        model.mark_native_started(&claim).unwrap();
        assert!(model.reject_before_handoff(&claim).is_err());
        assert!(model.rolled_back(&claim, false).is_err());
        assert!(model.has_pending());
        model.rolled_back(&claim, true).unwrap();
        assert!(!model.has_pending());
    }

    #[test]
    fn delivered_predecessor_is_required_for_same_revision_next_batch_and_fresh_revision_resets_safely()
     {
        let mut model = Batches::default();
        let original = context();
        let first = reserve(&mut model, &original);
        let claim = native_claim(&mut model, &original, &first);
        model.mark_native_started(&claim).unwrap();
        model.prepared(&claim, "c".repeat(64)).unwrap();
        model.observed_commit(&claim).unwrap();
        let receipt = Receipt::from_native(first.clone(), "c".repeat(64), vec![1], false).unwrap();
        model.accepted(&claim, &original, receipt.clone()).unwrap();
        model
            .delivered(&original, &first, &receipt.receipt_sha256)
            .unwrap();
        assert!(
            reload(&model)
                .issue(original.clone(), None, "b".repeat(64))
                .is_err()
        );
        let mut descendant = original.clone();
        descendant.consumer.grant = Uuid::new_v4();
        assert!(
            reload(&model)
                .issue(descendant, None, "b".repeat(64))
                .is_err()
        );
        let mut fresh = original.clone();
        fresh.issuer.grant = Uuid::new_v4();
        fresh.issuer.context_revision += 1;
        fresh.consumer = fresh.issuer.clone();
        let Issue::Reserved(second) = model.issue(fresh.clone(), None, "b".repeat(64)).unwrap()
        else {
            panic!("Fresh canonical authority could not start")
        };
        assert_ne!(first.stream, second.stream);
        assert_eq!(second.ordinal, 1);
        assert!(model.claim(&original, &first, &"b".repeat(64)).is_err());
    }
}
