//! Canonical native batch delivery. Wire identifiers select Main-owned state;
//! only an exact durable data-route claim produces a nonserde native capability.
use super::Controller;
use crate::executor_jobs::{Catalog, Record};
use meerschaum_connector::mutation::{BatchClaimOutcome, NativeBatch, NativeBatchReceipt};
use meerschaum_connector_api::client::{
    BatchOutcome, BatchReceipt, BatchReply, BatchRequest, BatchSelector, GraphBinding,
};
use meerschaum_server::{
    access::Access,
    executor::pipe_operation::{
        ReadAccess,
        commits::batches::{self, Claim, Context, Selector},
    },
};
use std::sync::Arc;

fn refused() -> String {
    "The canonical native batch is changed, unknown or not delivered. No target replay is allowed; inspect the original operation.".into()
}
fn selector(value: &BatchSelector) -> Selector {
    Selector {
        stream: value.stream,
        ordinal: value.ordinal,
        batch: value.batch,
    }
}
fn wire_selector(value: &Selector) -> BatchSelector {
    BatchSelector {
        stream: value.stream,
        ordinal: value.ordinal,
        batch: value.batch,
    }
}
fn wire_receipt(value: &batches::Receipt) -> BatchReceipt {
    BatchReceipt {
        selector: wire_selector(value.selector()),
        sha256: value.sha256().into(),
    }
}
fn native_receipt(value: &batches::Receipt) -> Result<NativeBatchReceipt, String> {
    NativeBatchReceipt::from_host(
        value.selector().stream,
        value.selector().ordinal,
        value.selector().batch,
        value.sha256().into(),
    )
}
fn reply(context: &Context, outcome: BatchOutcome) -> BatchReply {
    let actor =
        |binding: &meerschaum_server::executor::pipe_operation::CallbackBinding| GraphBinding {
            grant: binding.grant,
            launch: binding.launch,
            revision: binding.context_revision,
        };
    BatchReply {
        version: 1,
        consumer: actor(context.consumer()),
        issuer: actor(context.issuer()),
        reference: context.reference().into(),
        outcome,
    }
}

struct Admission {
    context: Context,
    consumer: Record,
    issuer: Record,
    _gate: tokio::sync::OwnedMutexGuard<()>,
}
/// Final clone owns cleanup even if the request is dropped after the blocking
/// claim persisted. The start CAS proves no target handoff; started work is kept.
struct NeverHandedOff {
    root: std::path::PathBuf,
    issuer: Record,
    claim: Arc<Claim>,
}
impl Drop for NeverHandedOff {
    fn drop(&mut self) {
        if !self.claim.never_started() {
            return;
        }
        let root = self.root.clone();
        let issuer = self.issuer.clone();
        let claim = self.claim.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let _task = runtime.spawn_blocking(move || {
                // CAS+private Catalog lock excludes a racing native handoff.
                // A busy/failed write retains unknown; it never cleans another claim.
                let _ = Catalog::new(&root).reject_native_batch_before_handoff(&issuer, &claim);
            });
        }
    }
}

impl Controller {
    async fn batch_current(
        &self,
        caller: &Access,
        reference: &str,
    ) -> Result<Option<(ReadAccess, Context)>, String> {
        let Some(read) = Box::pin(self.pipe_operation_read(caller)).await? else {
            return Ok(None);
        };
        let context = read
            .native_batch_context(reference)
            .map_err(|error| error.detail_text())?;
        Ok(Some((read, context)))
    }
    async fn batch_admission(
        &self,
        caller: &Access,
        reference: &str,
    ) -> Result<Option<Admission>, String> {
        let Some((read, context)) = Box::pin(self.batch_current(caller, reference)).await? else {
            return Ok(None);
        };
        let gate = self.commit_node_guard(&read, reference)?;
        let (consumer, issuer) = Box::pin(self.commit_records(&read)).await?;
        let (_, fresh) = Box::pin(self.batch_current(caller, reference))
            .await?
            .ok_or_else(refused)?;
        if fresh != context {
            return Err(refused());
        }
        Ok(Some(Admission {
            context,
            consumer,
            issuer,
            _gate: gate,
        }))
    }
    async fn batch_confirm(&self, caller: &Access, expected: &Context) -> Result<(), String> {
        let (_, current) = Box::pin(self.batch_current(caller, expected.reference()))
            .await?
            .ok_or_else(refused)?;
        if &current != expected {
            return Err(refused());
        }
        Ok(())
    }
    /// Repeat the original source and window proof on both sides of the Catalog read.
    /// The native writer may hold its own node gate while rechecking its pending claim.
    async fn inspect_pipe_batch(
        &self,
        caller: &Access,
        batch: &NativeBatch,
    ) -> Result<Option<batches::Receipt>, String> {
        let claim = batch.owner::<Claim>().ok_or_else(refused)?;
        let (read, context) = Box::pin(self.batch_current(caller, batch.reference()))
            .await?
            .ok_or_else(refused)?;
        if claim.context() != &context || claim.request_sha256() != batch.request_sha256() {
            return Err(refused());
        }
        let (consumer, issuer) = Box::pin(self.commit_records(&read)).await?;
        let catalog = Catalog::new(&self.root);
        let expected = context.clone();
        let receipt = tokio::task::spawn_blocking(move || {
            catalog.inspect_native_batch(&consumer, &issuer, &expected, &claim)
        })
        .await
        .map_err(|_| refused())??;
        Box::pin(self.batch_confirm(caller, &context)).await?;
        Ok(receipt)
    }
    pub(crate) async fn confirm_pipe_batch(
        &self,
        caller: &Access,
        batch: &NativeBatch,
    ) -> Result<(), String> {
        self.inspect_pipe_batch(caller, batch).await.map(|_| ())
    }
    pub(crate) async fn accepted_pipe_batch(
        &self,
        caller: &Access,
        batch: &NativeBatch,
    ) -> Result<NativeBatchReceipt, String> {
        let receipt = self
            .inspect_pipe_batch(caller, batch)
            .await?
            .ok_or_else(refused)?;
        native_receipt(&receipt)
    }
    pub(crate) async fn graph_batch(
        &self,
        caller: &Access,
        request: &BatchRequest,
    ) -> Result<Option<BatchReply>, String> {
        request.validate().map_err(|_| refused())?;
        let Some(admission) = Box::pin(self.batch_admission(caller, request.reference())).await?
        else {
            return Ok(None);
        };
        let Admission {
            context,
            consumer,
            issuer,
            _gate,
        } = admission;
        let catalog = Catalog::new(&self.root);
        let saved_consumer = consumer.clone();
        let saved_issuer = issuer.clone();
        let saved_context = context.clone();
        let outcome = match request {
            BatchRequest::Issue {
                predecessor,
                request_sha256,
                ..
            } => {
                let predecessor = predecessor.as_ref().map(selector);
                let digest = request_sha256.clone();
                let issued = tokio::task::spawn_blocking(move || {
                    catalog.issue_native_batch(
                        &saved_consumer,
                        &saved_issuer,
                        saved_context,
                        predecessor,
                        digest,
                    )
                })
                .await
                .map_err(|_| refused())??;
                if let Err(error) = Box::pin(self.batch_confirm(caller, &context)).await {
                    if let batches::Issue::Reserved(selector) = &issued {
                        let catalog = Catalog::new(&self.root);
                        let old_issuer = issuer.clone();
                        let old_context = context.clone();
                        let old_selector = selector.clone();
                        let digest = request_sha256.clone();
                        let _ = tokio::task::spawn_blocking(move || {
                            catalog.cancel_reserved_native_batch(
                                &old_issuer,
                                &old_context,
                                &old_selector,
                                &digest,
                            )
                        })
                        .await;
                    }
                    return Err(error);
                }
                match issued {
                    batches::Issue::Reserved(selector) => BatchOutcome::Reserved {
                        selector: wire_selector(&selector),
                    },
                    batches::Issue::Accepted(receipt) => BatchOutcome::AlreadyCommitted {
                        receipt: wire_receipt(&receipt),
                    },
                    batches::Issue::Unknown => BatchOutcome::Unknown,
                }
            }
            BatchRequest::Delivered { receipt, .. } => {
                let selected = selector(&receipt.selector);
                let digest = receipt.sha256.clone();
                tokio::task::spawn_blocking(move || {
                    catalog.deliver_native_batch(
                        &saved_consumer,
                        &saved_issuer,
                        &saved_context,
                        &selected,
                        &digest,
                    )
                })
                .await
                .map_err(|_| refused())??;
                // Delivery persisted; revocation now cannot turn it into a replay.
                Box::pin(self.batch_confirm(caller, &context)).await?;
                BatchOutcome::Delivered {
                    receipt: receipt.clone(),
                }
            }
        };
        let _ = &_gate;
        Ok(Some(reply(&context, outcome)))
    }
    pub(crate) async fn claim_pipe_batch(
        &self,
        caller: &Access,
        reference: &str,
        selected: &BatchSelector,
        actual_request_sha256: &str,
    ) -> Result<BatchClaimOutcome, String> {
        selected.validate().map_err(|_| refused())?;
        let Admission {
            context,
            consumer,
            issuer,
            _gate,
        } = Box::pin(self.batch_admission(caller, reference))
            .await?
            .ok_or_else(refused)?;
        let catalog = Catalog::new(&self.root);
        let saved_consumer = consumer.clone();
        let saved_issuer = issuer.clone();
        let saved_context = context.clone();
        let selected = selector(selected);
        let digest = actual_request_sha256.to_owned();
        // Construct never-handoff ownership INSIDE the blocking task, before its
        // result can be cancelled/lost by the awaiting request.
        let root = self.root.clone();
        let claimed = tokio::task::spawn_blocking(move || {
            let claimed = catalog.claim_native_batch(
                &saved_consumer,
                &saved_issuer,
                &saved_context,
                &selected,
                &digest,
            )?;
            Ok::<_, String>(match claimed {
                batches::ClaimOutcome::Native(claim) => {
                    let claim = Arc::new(claim);
                    let guard = Arc::new(NeverHandedOff {
                        root,
                        issuer: saved_issuer,
                        claim: claim.clone(),
                    });
                    OwnedOutcome::Native(claim, guard)
                }
                batches::ClaimOutcome::Accepted(receipt) => OwnedOutcome::Accepted(receipt),
                batches::ClaimOutcome::Unknown => OwnedOutcome::Unknown,
            })
        })
        .await
        .map_err(|_| refused())??;
        Box::pin(self.batch_confirm(caller, &context)).await?;
        let _ = &_gate;
        match claimed {
            OwnedOutcome::Native(claim, guard) => {
                let controller = self.clone();
                let old_issuer = issuer.clone();
                let rollback_claim = claim.clone();
                let native = NativeBatch::from_host(
                    reference.into(),
                    actual_request_sha256.into(),
                    claim,
                    Claim::begin_target,
                )?
                .with_host_lifecycle(
                    move || {
                        let catalog = Catalog::new(&controller.root);
                        let issuer = old_issuer.clone();
                        let claim = rollback_claim.clone();
                        async move {
                            tokio::task::spawn_blocking(move || {
                                catalog.rollback_native_batch(&issuer, claim)
                            })
                            .await
                            .map_err(|_| refused())?
                        }
                    },
                    guard,
                );
                Ok(BatchClaimOutcome::Native(native))
            }
            OwnedOutcome::Accepted(receipt) => Ok(BatchClaimOutcome::AlreadyCommitted(
                native_receipt(&receipt)?,
            )),
            OwnedOutcome::Unknown => Ok(BatchClaimOutcome::Unknown),
        }
    }
}
enum OwnedOutcome {
    Native(Arc<Claim>, Arc<NeverHandedOff>),
    Accepted(batches::Receipt),
    Unknown,
}
