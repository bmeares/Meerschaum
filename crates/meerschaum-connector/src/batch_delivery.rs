//! Worker-local continuation of one Main-admitted node attempt. Main owns the batch journal.
use crate::{ConnectorError, ConnectorResult};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphBinding {
    pub grant: Uuid,
    pub launch: Uuid,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchSelector {
    pub stream: Uuid,
    pub ordinal: u64,
    pub batch: Uuid,
}
impl BatchSelector {
    pub fn validate(&self) -> ConnectorResult<()> {
        if self.stream.is_nil() || self.batch.is_nil() || self.ordinal == 0 {
            return Err(ConnectorError::Invalid(
                "Invalid native batch identity/provenance.".into(),
            ));
        }
        Ok(())
    }
}

fn refused() -> ConnectorError {
    ConnectorError::Invalid(
        "The native write lost its original node continuation or has an unresolved delivery."
            .into(),
    )
}

/// Created from the trusted node's actor pair. Each admission gets a separate cursor.
#[derive(Clone, Debug)]
pub struct NodeDelivery(Arc<Owner>);
#[derive(Debug)]
struct Owner {
    consumer: GraphBinding,
    issuer: GraphBinding,
    reference: String,
    issued: AtomicBool,
    failed: AtomicBool,
    cursor: Arc<Mutex<Cursor>>,
}
#[derive(Default, Debug)]
struct Cursor {
    previous: Option<BatchSelector>,
    unresolved: bool,
}

tokio::task_local! { static WRITING: Option<NodeDelivery>; }

impl NodeDelivery {
    pub fn new(consumer: GraphBinding, issuer: GraphBinding, reference: String) -> Self {
        Self(Arc::new(Owner {
            consumer,
            issuer,
            reference,
            issued: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            cursor: Default::default(),
        }))
    }
    pub fn consumer(&self) -> &GraphBinding {
        &self.0.consumer
    }
    pub fn issuer(&self) -> &GraphBinding {
        &self.0.issuer
    }
    pub fn reference(&self) -> &str {
        &self.0.reference
    }
    /// Includes confirmed earlier writes; replaying the whole node would repeat them.
    pub fn has_issued(&self) -> bool {
        self.0.issued.load(Ordering::Acquire)
    }
    /// A failed plugin callback must survive a later successful plugin return.
    pub fn record_failure(&self) {
        self.0.failed.store(true, Ordering::Release);
    }
    pub fn check_available(&self) -> ConnectorResult<()> {
        if self.0.failed.load(Ordering::Acquire) {
            return Err(refused());
        }
        let cursor = self.0.cursor.try_lock().map_err(|_| refused())?;
        if cursor.unresolved {
            return Err(refused());
        }
        Ok(())
    }
    /// Reconcile a completed engine call with the original node's write attempts.
    pub fn finish(&self, result: &mut meerschaum_core::success::SyncResult) {
        if result.success
            && let Err(error) = self.check_available()
        {
            self.record_failure();
            result.success = false;
            result.message = error.to_string();
        } else if !result.success && self.has_issued() {
            self.record_failure();
        }
    }
    /// The engine installs this only around its destination data write or prepared commit.
    pub fn current_write() -> Option<Self> {
        WRITING.try_with(Clone::clone).ok().flatten()
    }
    pub fn acquire(&self, reference: &str) -> ConnectorResult<DeliveryLease> {
        if self.0.failed.load(Ordering::Acquire) || reference != self.reference() {
            return Err(refused());
        }
        let cursor = self
            .0
            .cursor
            .clone()
            .try_lock_owned()
            .map_err(|_| refused())?;
        if cursor.unresolved {
            return Err(refused());
        }
        Ok(DeliveryLease {
            owner: self.clone(),
            cursor,
        })
    }
}

/// The caller owns this guard for one counted frame (or an exact prepared lease commit).
pub struct DeliveryLease {
    owner: NodeDelivery,
    cursor: OwnedMutexGuard<Cursor>,
}
impl DeliveryLease {
    pub fn owner(&self) -> &NodeDelivery {
        &self.owner
    }
    pub fn unresolved(&self) -> bool {
        self.cursor.unresolved
    }
    pub fn issue(&mut self) -> ConnectorResult<Option<BatchSelector>> {
        if self.owner.0.failed.load(Ordering::Acquire) || self.cursor.unresolved {
            return Err(refused());
        }
        self.cursor.unresolved = true;
        self.owner.0.issued.store(true, Ordering::Release);
        Ok(self.cursor.previous.clone())
    }
    pub fn confirm_next(&self, next: &BatchSelector) -> ConnectorResult<()> {
        next.validate()?;
        let follows = match &self.cursor.previous {
            None => next.ordinal == 1,
            Some(previous) => {
                previous.stream == next.stream
                    && previous.ordinal.checked_add(1) == Some(next.ordinal)
                    && previous.batch != next.batch
            }
        };
        (self.cursor.unresolved && follows)
            .then_some(())
            .ok_or_else(refused)
    }
    /// Called only after Main returned the matching Delivered receipt.
    pub fn delivered(&mut self, next: BatchSelector) -> ConnectorResult<()> {
        self.confirm_next(&next)?;
        self.cursor.previous = Some(next);
        self.cursor.unresolved = false;
        Ok(())
    }
}

pub async fn destination_write<F: Future>(owner: Option<&NodeDelivery>, write: F) -> F::Output {
    WRITING.scope(owner.cloned(), write).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn node_attempts_have_independent_exclusive_continuations() {
        let actor = || GraphBinding {
            grant: Uuid::new_v4(),
            launch: Uuid::new_v4(),
            revision: 1,
        };
        let consumer = actor();
        let issuer = actor();
        let new_owner = || NodeDelivery::new(consumer.clone(), issuer.clone(), "same node".into());
        let first = new_owner();
        let concurrent = new_owner();
        let mut first_call = first.acquire("same node").unwrap();
        assert!(first.acquire("same node").is_err());
        assert!(first_call.issue().unwrap().is_none());
        drop(first_call);
        assert!(first.acquire("same node").is_err());
        let mut independent = concurrent.acquire("same node").unwrap();
        assert!(independent.issue().unwrap().is_none());
        drop(independent);
        destination_write(Some(&first), async {
            assert!(NodeDelivery::current_write().unwrap().has_issued());
            destination_write(None, async {
                assert!(NodeDelivery::current_write().is_none());
            })
            .await;
            assert!(NodeDelivery::current_write().unwrap().has_issued());
        })
        .await;
        assert!(NodeDelivery::current_write().is_none());
    }
}
