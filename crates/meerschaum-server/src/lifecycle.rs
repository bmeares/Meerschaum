//! The server's lifecycle: starting, serving and draining (v5).
//!
//! A server that is told to stop (`SIGTERM` / `SIGINT` in `start api`) first **drains**: its
//! listeners stop accepting connections, requests in progress finish, and long-lived responses
//! (Server-Sent Events, websockets, upgraded connections to the API host) end at once, so they
//! never hold the drain open. `GET /healthcheck/ready` answers `503` from the moment the drain
//! begins (and while the server is still starting, or while the API host it needs is not
//! serving: [`crate::host::HostSupervisor::is_serving`]), so that a load balancer stops sending
//! it new requests; `GET /healthcheck` is unchanged (uptime monitors match it).

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;

/// Whether the server is starting, serving or draining. One per [`crate::AppState`].
#[derive(Debug)]
pub struct Lifecycle {
    starting: AtomicBool,
    drain: watch::Sender<bool>,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Lifecycle {
            starting: AtomicBool::new(false),
            drain: watch::Sender::new(false),
        }
    }
}

impl Lifecycle {
    /// Mark the server as still starting (or done starting): `GET /healthcheck/ready` answers
    /// `503` meanwhile.
    pub fn set_starting(&self, starting: bool) {
        self.starting.store(starting, Ordering::SeqCst);
    }

    /// Whether the server is still starting ([`Lifecycle::set_starting`]).
    pub fn is_starting(&self) -> bool {
        self.starting.load(Ordering::SeqCst)
    }

    /// Begin the drain: listeners stop accepting, long-lived responses end. Idempotent.
    pub fn begin_drain(&self) {
        self.drain.send_replace(true);
    }

    /// Whether the drain has begun.
    pub fn is_draining(&self) -> bool {
        *self.drain.borrow()
    }

    /// A future-producing handle that resolves when the drain begins (or the lifecycle is gone).
    pub fn signal(&self) -> DrainSignal {
        DrainSignal {
            receiver: self.drain.subscribe(),
            _sender: None,
        }
    }
}

/// Resolves when the drain begins ([`DrainSignal::wait`]); cheap to clone.
#[derive(Debug, Clone)]
pub struct DrainSignal {
    receiver: watch::Receiver<bool>,
    /// The sender of a signal that never fires: kept so that the receiver never sees it closed.
    _sender: Option<std::sync::Arc<watch::Sender<bool>>>,
}

impl DrainSignal {
    /// A signal that never fires (a caller with no lifecycle).
    pub fn never() -> DrainSignal {
        let (sender, receiver) = watch::channel(false);
        DrainSignal {
            receiver,
            _sender: Some(std::sync::Arc::new(sender)),
        }
    }

    /// Whether the drain has begun.
    pub fn is_draining(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Wait until the drain begins (at once when it already has, or when the server state that
    /// owned the lifecycle is gone).
    pub async fn wait(mut self) {
        let _ = self.receiver.wait_for(|draining| *draining).await;
    }
}

/// `stream` until the drain begins: a long-lived response (SSE) ends then instead of holding
/// the drain open.
pub fn until_drain<S>(stream: S, signal: DrainSignal) -> impl futures_util::Stream<Item = S::Item>
where
    S: futures_util::Stream,
{
    use futures_util::StreamExt as _;
    stream.take_until(Box::pin(signal.wait()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_signal_fires_once_the_drain_begins() {
        let lifecycle = Lifecycle::default();
        assert!(!lifecycle.is_draining() && !lifecycle.is_starting());
        let signal = lifecycle.signal();
        let waiting = tokio::spawn(signal.clone().wait());
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        lifecycle.begin_drain();
        lifecycle.begin_drain();
        tokio::time::timeout(std::time::Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap();
        assert!(lifecycle.is_draining() && signal.is_draining());
        // A signal taken after the drain began fires at once.
        tokio::time::timeout(std::time::Duration::from_secs(1), lifecycle.signal().wait())
            .await
            .unwrap();
        // A lifecycle that is gone releases its waiters.
        let gone = Lifecycle::default();
        let signal = gone.signal();
        drop(gone);
        tokio::time::timeout(std::time::Duration::from_secs(1), signal.wait())
            .await
            .unwrap();
        // `never` stays pending.
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(30),
                DrainSignal::never().wait()
            )
            .await
            .is_err()
        );
    }
}
