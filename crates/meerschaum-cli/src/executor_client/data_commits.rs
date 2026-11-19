//! Exact Main-owned native target commit. No request/result deserialization.

use super::Controller;
use crate::executor_jobs::{Catalog, Record};
use meerschaum_connector::mutation::{NativeDataEffect, PendingDataCommit};
use meerschaum_server::{
    access::Access,
    executor::pipe_operation::{
        CallbackBinding, ReadAccess,
        commits::{Binding, Ticket},
    },
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

type Gates = Mutex<BTreeMap<(std::path::PathBuf, uuid::Uuid, String), Weak<AsyncMutex<()>>>>;
static GATES: OnceLock<Gates> = OnceLock::new();
const MAX_ACTIVE_NODES: usize = 4096;

fn refused() -> String {
    "The native node has an unfinished or changed commit. Inspect its original canonical operation; do not automatically replay or issue dependent work.".into()
}

fn binding_matches(record: &Record, binding: &CallbackBinding) -> bool {
    record.callback == Some(binding.grant)
        && record.remote_name == format!("mrsm-{}", binding.launch.simple())
        && record.context_revision == binding.context_revision
}

impl Controller {
    /// Native-only path called by DataAdmission after its captured target proof. A
    /// future route adapter cannot decode this effect or its returned owned Ticket.
    pub(crate) async fn prepare_pipe_commit(
        &self,
        caller: &Access,
        effect: NativeDataEffect,
    ) -> Result<Arc<dyn PendingDataCommit>, String> {
        let current = self
            .pipe_operation_read(caller)
            .await?
            .ok_or_else(refused)?;
        if effect.native_batch().is_none() {
            return Err("The captured native transaction has no Main-issued batch claim.".into());
        }
        let binding = current
            .native_commit_binding(&effect)
            .map_err(|e| e.detail_text())?;
        let _guard = self.commit_node_guard(&current, effect.logical_reference())?;
        let (consumer, issuer) = self.commit_records(&current).await?;
        // Revalidate after the awaited canonical read; prepare's CAS covers all
        // captured operation/source/owner fields rather than adopting newer grants.
        let fresh = self
            .pipe_operation_read(caller)
            .await?
            .ok_or_else(refused)?;
        if fresh
            .native_commit_binding(&effect)
            .map_err(|e| e.detail_text())?
            != binding
        {
            return Err(refused());
        }
        let catalog = Catalog::new(&self.root);
        let saved_consumer = consumer.clone();
        let saved_issuer = issuer.clone();
        let saved_effect = effect.clone();
        let ticket = tokio::task::spawn_blocking(move || {
            catalog.prepare_data_commit(&saved_consumer, &saved_issuer, binding, &saved_effect)
        })
        .await
        .map_err(|_| refused())??;
        // Cancellation before this object escapes leaves the durable unknown. A
        // completed blocking reservation is never cleaned merely because its waiter died.
        Ok(Arc::new(Pending {
            controller: self.clone(),
            original: caller.clone(),
            consumer,
            issuer,
            ticket: Arc::new(ticket),
            _guard,
        }))
    }

    pub(super) fn commit_node_guard(
        &self,
        current: &ReadAccess,
        reference: &str,
    ) -> Result<OwnedMutexGuard<()>, String> {
        let mut gates = GATES
            .get_or_init(Default::default)
            .lock()
            .map_err(|_| refused())?;
        gates.retain(|_, gate| gate.strong_count() > 0);
        let key = (
            self.root.clone(),
            current.issuer.grant,
            reference.to_owned(),
        );
        let gate = if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
            gate
        } else {
            if gates.len() >= MAX_ACTIVE_NODES {
                return Err(refused());
            }
            let gate = Arc::new(AsyncMutex::new(()));
            gates.insert(key, Arc::downgrade(&gate));
            gate
        };
        // The SQL writer already holds its actual DB lock. Never await a node
        // gate here: a previous receipt may need that DB during final fresh ACK.
        gate.try_lock_owned().map_err(|_| refused())
    }

    pub(super) async fn commit_records(
        &self,
        current: &ReadAccess,
    ) -> Result<(Record, Record), String> {
        let consumer = current.binding.clone();
        let issuer = current.issuer.clone();
        let catalog = Catalog::new(&self.root);
        tokio::task::spawn_blocking(move || {
            let records = catalog.records_if_available()?;
            let find = |binding: &CallbackBinding| {
                records
                    .values()
                    .find(|record| !record.callback_suspended && binding_matches(record, binding))
                    .cloned()
                    .ok_or_else(refused)
            };
            Ok((find(&consumer)?, find(&issuer)?))
        })
        .await
        .map_err(|_| refused())?
    }
}

struct Pending {
    controller: Controller,
    original: Access,
    consumer: Record,
    issuer: Record,
    ticket: Arc<Ticket>,
    _guard: OwnedMutexGuard<()>,
}

impl Pending {
    async fn fresh(&self, effect: &NativeDataEffect) -> Result<(Record, Record, Binding), String> {
        let current = self
            .controller
            .pipe_operation_read(&self.original)
            .await?
            .ok_or_else(refused)?;
        let fresh = current
            .native_commit_binding(effect)
            .map_err(|e| e.detail_text())?;
        if &fresh != self.ticket.binding() {
            return Err(refused());
        }
        let (consumer, issuer) = self.controller.commit_records(&current).await?;
        if consumer.owner_binding != self.consumer.owner_binding
            || consumer.source_selection != self.consumer.source_selection
            || consumer.placement != self.consumer.placement
            || issuer.owner_binding != self.issuer.owner_binding
            || issuer.source_selection != self.issuer.source_selection
            || issuer.placement != self.issuer.placement
        {
            return Err(refused());
        }
        let current = self
            .controller
            .pipe_operation_read(&self.original)
            .await?
            .ok_or_else(refused)?;
        if current
            .native_commit_binding(effect)
            .map_err(|e| e.detail_text())?
            != fresh
        {
            return Err(refused());
        }
        Ok((consumer, issuer, fresh))
    }
}

#[meerschaum_connector::async_trait]
impl PendingDataCommit for Pending {
    async fn before_commit(&self, effect: &NativeDataEffect) -> Result<(), String> {
        let (consumer, issuer, binding) = self.fresh(effect).await?;
        let catalog = Catalog::new(&self.controller.root);
        let ticket = self.ticket.clone();
        // Own-ticket precommit CAS verifies the pending exact attempt without
        // marking it committed or granting a new operation. Add this read-only
        // Catalog method together with registration of this draft.
        tokio::task::spawn_blocking(move || {
            catalog.confirm_data_commit(&consumer, &issuer, &ticket, &binding)
        })
        .await
        .map_err(|_| refused())?
    }
    async fn committed(&self, _effect: &NativeDataEffect) -> Result<(), String> {
        let catalog = Catalog::new(&self.controller.root);
        let issuer = self.issuer.clone();
        let ticket = self.ticket.clone();
        tokio::task::spawn_blocking(move || catalog.observe_data_commit(&issuer, &ticket))
            .await
            .map_err(|_| refused())?
    }
    async fn acknowledge(&self, effect: &NativeDataEffect) -> Result<(), String> {
        let (consumer, issuer, binding) = self.fresh(effect).await?;
        let catalog = Catalog::new(&self.controller.root);
        let ticket = self.ticket.clone();
        tokio::task::spawn_blocking(move || {
            catalog.acknowledge_data_commit(&consumer, &issuer, &ticket, &binding)
        })
        .await
        .map_err(|_| refused())?
    }
    async fn rolled_back(&self) -> Result<(), String> {
        let catalog = Catalog::new(&self.controller.root);
        let issuer = self.issuer.clone();
        let ticket = self.ticket.clone();
        tokio::task::spawn_blocking(move || catalog.rollback_data_commit(&issuer, &ticket))
            .await
            .map_err(|_| refused())?
    }
}
