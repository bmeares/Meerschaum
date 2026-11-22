//! Durable target intent around the actual native transaction boundary.
//!
//! Native window adapters must supply their exact held target/schema/OLD-row guard.
//! Ordinary SQL commit and separately committed metadata inference keep their old paths.

use super::*;
use meerschaum_connector::mutation::{
    NativeBatch, NativeCommitReceipt, NativeDataEffect, PendingDataCommit, commit,
};
use std::sync::Arc;

#[cfg(all(test, unix))]
mod tests;

/// Process-local native writer/file/schema evidence. Only repo-owned target adapters
/// implement this; no request, environment or result may deserialize a guard.
#[async_trait::async_trait]
pub trait NativeCommitGuard: Send + Sync {
    /// Revalidate this exact native target while the original transaction holds its lock.
    async fn recheck(&self, transaction: &mut SqlTransaction) -> Result<()>;
}

impl SqlTransaction {
    /// Retire the claim only after the actual target transaction rolled back.
    pub(crate) async fn rollback_target(self, batch: Option<&NativeBatch>) -> Result<()> {
        self.rollback().await?;
        if let Some(batch) = batch {
            batch
                .target_rolled_back()
                .await
                .map_err(|_| crate::SqlError::NativeRollbackUnacknowledged)?;
        }
        Ok(())
    }

    async fn rollback_intent(
        self,
        pending: Option<&Arc<dyn PendingDataCommit>>,
        original: crate::SqlError,
    ) -> Result<Option<NativeCommitReceipt>> {
        // This method belongs only to commit_with_effect's actual target lane.
        // A metadata/probe transaction cannot retire a native batch through it.
        let native_batch = self
            .mutation_check
            .as_ref()
            .and_then(|check| check.native_batch());
        if let Some(pending) = pending {
            self.rollback().await?;
            commit::rolled_back(pending.as_ref())
                .await
                .map_err(|_| crate::SqlError::NativeRollbackUnacknowledged)?;
        } else {
            self.rollback_target(native_batch.as_ref()).await?;
        }
        Err(original)
    }

    /// After exact target DML, durably prepare before actual COMMIT and record its
    /// actual outcome afterward. The native caller acknowledges the returned receipt
    /// after its existing committed metadata-inference publication has completed.
    pub async fn commit_with_effect(
        mut self,
        effect: NativeDataEffect,
        guard: &dyn NativeCommitGuard,
    ) -> Result<Option<NativeCommitReceipt>> {
        let Some(authority) = self.mutation_check.clone().filter(|_| self.write_lock) else {
            return self
                .rollback_intent(
                    None,
                    crate::SqlError::InvalidArgument(
                        "The native target commit has no captured Main writer authority.".into(),
                    ),
                )
                .await;
        };
        if let Err(error) = self.check_write().await {
            return self.rollback_intent(None, error).await;
        }
        if let Err(error) = guard.recheck(&mut self).await {
            return self.rollback_intent(None, error).await;
        }
        let pending = match self
            .with_held_authorization_reads(commit::prepare(&authority, &effect))
            .await
            .and_then(|result| result.map_err(crate::SqlError::InvalidArgument))
        {
            Ok(pending) => pending,
            Err(error) => return self.rollback_intent(None, error).await,
        };
        if let Some(pending) = &pending {
            // A general new-request blocker cannot adopt or refuse its own private
            // Ticket. This object retains exact source/canonical ownership after I/O.
            let checked = self
                .with_held_authorization_reads(commit::before_commit(pending.as_ref(), &effect))
                .await
                .and_then(|result| result.map_err(crate::SqlError::InvalidArgument));
            if let Err(error) = checked {
                return self.rollback_intent(Some(pending), error).await;
            }
        } else if let Err(error) = self.check_write().await {
            return self.rollback_intent(None, error).await;
        }
        // Journal/source waits cannot replace the native target identity/lock proof.
        if let Err(error) = guard.recheck(&mut self).await {
            return self.rollback_intent(pending.as_ref(), error).await;
        }
        self.authorization_pin.take();
        let outcome = match self.inner {
            TxInner::PinnedRead => Err(crate::SqlError::InvalidArgument(
                "An authorization snapshot cannot commit target data.".into(),
            )),
            TxInner::Postgres(transaction) => {
                transaction.commit().await.map_err(crate::SqlError::from)
            }
            TxInner::Mysql(transaction, _) => {
                transaction.commit().await.map_err(crate::SqlError::from)
            }
            TxInner::Sqlite(transaction) => {
                transaction.commit().await.map_err(crate::SqlError::from)
            }
            #[cfg(any(feature = "mssql", feature = "duckdb", feature = "oracle"))]
            TxInner::Native(transaction) => transaction.commit().await,
        };
        if let Err(error) = outcome {
            // COMMIT was submitted. Error, loss, or cancellation cannot be relabeled
            // an observed rollback. Durable Prepared remains an unknown blocker.
            return Err(if pending.is_some() {
                crate::SqlError::NativeCommitUnknown
            } else {
                error
            });
        }
        if let Some(pending) = pending {
            commit::committed(pending.as_ref(), &effect)
                .await
                .map_err(|_| crate::SqlError::NativeCommittedUnacknowledged)?;
            return Ok(Some(NativeCommitReceipt::observed(effect, pending)));
        }
        Ok(None)
    }
}
