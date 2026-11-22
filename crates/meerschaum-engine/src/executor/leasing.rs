//! Leases around node syncs: the [`LeaseSource`] trait with its in-memory and no-op sources, and
//! the lease one attempt holds, handed back even when the attempt is dropped mid-sync.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::node::PipeRef;
use crate::state::StateError;

/// Per-pipe leases (§5.8): an atomic acquire with a TTL, renewed while a node syncs.
pub trait LeaseSource: Send + Sync + 'static {
    /// Try to take the lease. `Ok(false)` when another holder has it.
    fn acquire(
        &self,
        node: &PipeRef,
        holder: &str,
        ttl: Duration,
    ) -> impl Future<Output = Result<bool, StateError>> + Send;

    /// Extend a held lease. `Ok(false)` when the lease was lost.
    fn renew(
        &self,
        node: &PipeRef,
        holder: &str,
        ttl: Duration,
    ) -> impl Future<Output = Result<bool, StateError>> + Send;

    /// Release a held lease (a no-op if another holder has it).
    fn release(
        &self,
        node: &PipeRef,
        holder: &str,
    ) -> impl Future<Output = Result<(), StateError>> + Send;
}

/// In-memory leases with expiry (on tokio's clock).
#[derive(Debug, Default)]
pub struct MemoryLeases {
    pub(super) held: Mutex<HashMap<PipeRef, (String, tokio::time::Instant)>>,
}

impl MemoryLeases {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PipeRef, (String, tokio::time::Instant)>> {
        self.held.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The current holder, if the lease is live.
    pub fn holder(&self, node: &PipeRef) -> Option<String> {
        let now = tokio::time::Instant::now();
        self.lock()
            .get(node)
            .filter(|(_, exp)| *exp > now)
            .map(|(h, _)| h.clone())
    }
}

impl LeaseSource for MemoryLeases {
    async fn acquire(
        &self,
        node: &PipeRef,
        holder: &str,
        ttl: Duration,
    ) -> Result<bool, StateError> {
        let now = tokio::time::Instant::now();
        let mut held = self.lock();
        match held.get(node) {
            Some((h, exp)) if *exp > now && h != holder => Ok(false),
            _ => {
                held.insert(node.clone(), (holder.to_string(), now + ttl));
                Ok(true)
            }
        }
    }

    async fn renew(&self, node: &PipeRef, holder: &str, ttl: Duration) -> Result<bool, StateError> {
        let now = tokio::time::Instant::now();
        let mut held = self.lock();
        match held.get_mut(node) {
            Some((h, exp)) if h == holder => {
                *exp = now + ttl;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn release(&self, node: &PipeRef, holder: &str) -> Result<(), StateError> {
        let mut held = self.lock();
        if held.get(node).is_some_and(|(h, _)| h == holder) {
            held.remove(node);
        }
        Ok(())
    }
}

/// Leases that are always granted (single process, or leases disabled).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoLeases;

impl LeaseSource for NoLeases {
    async fn acquire(&self, _: &PipeRef, _: &str, _: Duration) -> Result<bool, StateError> {
        Ok(true)
    }

    async fn renew(&self, _: &PipeRef, _: &str, _: Duration) -> Result<bool, StateError> {
        Ok(true)
    }

    async fn release(&self, _: &PipeRef, _: &str) -> Result<(), StateError> {
        Ok(())
    }
}

/// Pending acquisitions and held leases of this process not handed back yet.
static HELD_LEASES: AtomicUsize = AtomicUsize::new(0);

/// Wait (up to `timeout`) until every lease a node attempt of this process took has been handed
/// back to its instance.
///
/// Dropping a run (Ctrl-C in the CLI, a closed API request, a Python `KeyboardInterrupt`) drops
/// its node attempts mid-sync; each hands its lease back in a task of its own (see
/// `LeaseHold`). A process that is about to exit awaits this first, or the leases stay held
/// until their TTL runs out and the next run waits for them. Counted before acquisition starts,
/// so a backend's committed claim whose acknowledgement is still pending is waited for too.
pub async fn drain_lease_releases(timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    while HELD_LEASES.load(Ordering::SeqCst) > 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// An owned pending claim or held lease: released by the attempt itself, or, when the attempt
/// is dropped before it got there (the run was cancelled), by a task spawned from [`Drop`].
pub(crate) struct LeaseHold<L: LeaseSource> {
    leases: Arc<L>,
    pipe_ref: PipeRef,
    holder: String,
    armed: bool,
}

impl<L: LeaseSource> LeaseHold<L> {
    /// Count a held lease or an acquisition that is about to start.
    pub(crate) fn new(leases: Arc<L>, pipe_ref: PipeRef, holder: String) -> Self {
        HELD_LEASES.fetch_add(1, Ordering::SeqCst);
        LeaseHold {
            leases,
            pipe_ref,
            holder,
            armed: true,
        }
    }

    /// Finish acquisition on an owned task before handing its guard to the caller.
    /// Cancelling a database await need not cancel its committed write. If the caller drops
    /// the receiver, a late successful claim drops its guard and releases only this holder.
    /// Count before spawning so shutdown also drains the unacknowledged-acquire window.
    pub(crate) async fn acquire(
        leases: Arc<L>,
        pipe_ref: PipeRef,
        holder: String,
        ttl: Duration,
    ) -> Result<Option<Self>, StateError> {
        let mut hold = Self::new(leases, pipe_ref, holder);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let acquired = hold.leases.acquire(&hold.pipe_ref, &hold.holder, ttl).await;
            let result = match acquired {
                Ok(true) => Ok(Some(hold)),
                Ok(false) => {
                    hold.disarm();
                    Ok(None)
                }
                Err(error) => {
                    hold.disarm();
                    Err(error)
                }
            };
            // An abandoned successful handoff drops its guard here, after acquisition settled.
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| {
            StateError::Other(
                "The lease acquisition task ended before returning its result.".into(),
            )
        })?
    }

    /// The attempt handed the lease back itself.
    pub(super) fn disarm(&mut self) {
        if std::mem::replace(&mut self.armed, false) {
            HELD_LEASES.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Uncounts a lease when dropped (the release task finished, or never got to run).
struct Uncount;

impl Drop for Uncount {
    fn drop(&mut self) {
        HELD_LEASES.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<L: LeaseSource> Drop for LeaseHold<L> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // The count moves to the release task (or ends here when there is no runtime to run it:
        // it is shutting down, and the TTL expires the lease).
        let uncount = Uncount;
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (leases, pipe_ref, holder) = (
            self.leases.clone(),
            self.pipe_ref.clone(),
            self.holder.clone(),
        );
        runtime.spawn(async move {
            let _uncount = uncount;
            let _ = leases.release(&pipe_ref, &holder).await;
        });
    }
}
