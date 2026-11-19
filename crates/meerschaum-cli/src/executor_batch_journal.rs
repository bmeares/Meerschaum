//! Private batch Catalog transitions. These run only in owned blocking tasks.
//! No wire Claim or commit assertion reaches this module.
use super::*;
use meerschaum_server::executor::pipe_operation::commits::batches::{
    Claim, ClaimOutcome, Context, Issue, Receipt, Selector,
};
use std::sync::Arc;

fn batch_records(
    records: &BTreeMap<String, Record>,
    consumer: &Record,
    issuer: &Record,
    context: &Context,
) -> Result<(), String> {
    let live_consumer = records.get(&consumer.name).ok_or_else(unavailable)?;
    let live_issuer = records.get(&issuer.name).ok_or_else(unavailable)?;
    if !same_operation(live_consumer, consumer)
        || live_consumer.callback_suspended
        || !same_operation(live_issuer, issuer)
        || live_issuer.callback_suspended
        || !matches_binding(live_consumer, context.consumer())
        || !matches_binding(live_issuer, context.issuer())
    {
        return Err(unavailable());
    }
    Ok(())
}
impl Catalog {
    pub(crate) fn issue_native_batch(
        &self,
        consumer: &Record,
        issuer: &Record,
        context: Context,
        predecessor: Option<Selector>,
        request_sha256: String,
    ) -> Result<Issue, String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        batch_records(&records, consumer, issuer, &context)?;
        let current = records.get(&issuer.name).ok_or_else(unavailable)?;
        if current
            .operation
            .as_ref()
            .and_then(|op| op.pipeline_layer())
            .is_some_and(|layer| layer.parents.contains(context.reference()))
            && current
                .commit_journal
                .completed_replacement(&context)?
                .is_some()
        {
            return Err("This Pipeline already completed this parent window.".into());
        }
        let issued = records
            .get_mut(&issuer.name)
            .ok_or_else(unavailable)?
            .commit_journal
            .issue_batch(context, predecessor, request_sha256)?;
        self.durable_commit_write(&records)?;
        Ok(issued)
    }
    pub(crate) fn claim_native_batch(
        &self,
        consumer: &Record,
        issuer: &Record,
        context: &Context,
        selector: &Selector,
        request_sha256: &str,
    ) -> Result<ClaimOutcome, String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        batch_records(&records, consumer, issuer, context)?;
        let claimed = records
            .get_mut(&issuer.name)
            .ok_or_else(unavailable)?
            .commit_journal
            .claim_batch(context, selector, request_sha256)?;
        self.durable_commit_write(&records)?;
        Ok(claimed)
    }
    pub(crate) fn inspect_native_batch(
        &self,
        consumer: &Record,
        issuer: &Record,
        context: &Context,
        claim: &Claim,
    ) -> Result<Option<Receipt>, String> {
        let _lock = self.commit_lock()?;
        let records = self.read()?;
        batch_records(&records, consumer, issuer, context)?;
        records
            .get(&issuer.name)
            .ok_or_else(unavailable)?
            .commit_journal
            .confirm_batch_claim(claim, context)
    }
    pub(crate) fn deliver_native_batch(
        &self,
        consumer: &Record,
        issuer: &Record,
        context: &Context,
        selector: &Selector,
        receipt_sha256: &str,
    ) -> Result<(), String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        batch_records(&records, consumer, issuer, context)?;
        records
            .get_mut(&issuer.name)
            .ok_or_else(unavailable)?
            .commit_journal
            .deliver_batch(context, selector, receipt_sha256)?;
        self.durable_commit_write(&records)
    }
    /// Exact Reserved is known not handed off; a competing claim wins and refuses cancel.
    pub(crate) fn cancel_reserved_native_batch(
        &self,
        issuer: &Record,
        context: &Context,
        selector: &Selector,
        request_sha256: &str,
    ) -> Result<(), String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        let live = records
            .get_mut(&issuer.name)
            .filter(|record| record.remote_name == issuer.remote_name)
            .ok_or_else(unavailable)?;
        live.commit_journal
            .cancel_reserved_batch(context, selector, request_sha256)?;
        self.durable_commit_write(&records)
    }
    /// The same opaque Claim's start CAS excludes a concurrent target handoff.
    pub(crate) fn reject_native_batch_before_handoff(
        &self,
        issuer: &Record,
        claim: &Claim,
    ) -> Result<(), String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        let live = records
            .get_mut(&issuer.name)
            .filter(|record| record.remote_name == issuer.remote_name)
            .ok_or_else(unavailable)?;
        live.commit_journal.reject_batch_before_handoff(claim)?;
        self.durable_commit_write(&records)
    }
    /// Actual target rollback truth, even if source/lease expired while the writer waited.
    pub(crate) fn rollback_native_batch(
        &self,
        issuer: &Record,
        claim: Arc<Claim>,
    ) -> Result<(), String> {
        let _lock = self.commit_lock()?;
        let mut records = self.read()?;
        let live = records
            .get_mut(&issuer.name)
            .filter(|record| record.remote_name == issuer.remote_name)
            .ok_or_else(unavailable)?;
        live.commit_journal.rollback_native_batch(claim)?;
        self.durable_commit_write(&records)
    }
}
