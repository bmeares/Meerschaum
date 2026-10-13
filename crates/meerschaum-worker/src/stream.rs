//! Supervised Python streams (§5.6).
//!
//! - [`StreamMode::V4Sync`]: the v4 pattern. A connector's `sync()` subscribes (and returns, or
//!   blocks), and its message callbacks call `pipe.sync(df)`. The connector runs in a dedicated,
//!   long-lived worker; every callback `pipe.sync()` is forwarded to the host's
//!   [`CallbackHandler`] (the native sync path).
//! - [`StreamMode::Subscribe`]: `IS_STREAMING = True` connectors' `subscribe(pipe, sink)`; each
//!   `sink(df)` reaches the handler as a `push` callback.
//!
//! The supervisor restarts the worker with exponential backoff when it exits or when `sync()`
//! returns a failure, until [`PythonStream::stop`] is called.

use std::sync::Arc;
use std::time::Duration;

use meerschaum_core::{Map, Pipe, SuccessTuple};
use rmpv::Value as Wire;
use tokio::sync::{mpsc, watch};

use crate::callback::CallbackHandler;
use crate::pool::WorkerPool;
use crate::protocol::{Msg, map_to_wire};
use crate::worker::CallOptions;

/// Which entry point the stream runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMode {
    /// The connector's v4 `sync(pipe, **kwargs)`.
    V4Sync,
    /// `subscribe(pipe, sink)`.
    Subscribe,
}

/// Supervision settings.
#[derive(Debug, Clone)]
pub struct StreamOptions {
    /// Restart the worker when it exits.
    pub restart: bool,
    /// The first restart delay.
    pub backoff_initial: Duration,
    /// The longest restart delay.
    pub backoff_max: Duration,
    /// Give up after this many consecutive restarts.
    pub max_restarts: Option<u32>,
}

impl Default for StreamOptions {
    fn default() -> Self {
        StreamOptions {
            restart: true,
            backoff_initial: Duration::from_millis(500),
            backoff_max: Duration::from_secs(60),
            max_restarts: None,
        }
    }
}

/// What happened to a stream (the engine renders these as `stream_restarted`, ...).
#[derive(Debug, Clone, PartialEq)]
pub enum StreamEvent {
    /// A worker started running the stream.
    Started {
        /// The worker's PID.
        pid: u32,
    },
    /// `sync()` / `subscribe()` returned; the worker stays alive for callbacks.
    Returned(SuccessTuple),
    /// The worker exited or the call failed.
    Exited {
        /// Why.
        error: String,
    },
    /// The supervisor will restart after `delay`.
    Restarting {
        /// The consecutive restart count.
        attempt: u32,
        /// The backoff delay.
        delay: Duration,
    },
    /// The stream stopped (by request or after `max_restarts`).
    Stopped,
}

/// A running stream.
pub struct PythonStream {
    stop_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

fn backoff(opts: &StreamOptions, attempt: u32) -> Duration {
    let factor = 2u32.saturating_pow(attempt.saturating_sub(1).min(16));
    opts.backoff_initial
        .saturating_mul(factor)
        .min(opts.backoff_max)
}

impl PythonStream {
    /// Start supervising `connector_keys`' stream for `pipe`. Events arrive on the returned
    /// receiver.
    pub fn start(
        pool: WorkerPool,
        connector_keys: &str,
        pipe: &Pipe,
        kwargs: &Map,
        mode: StreamMode,
        handler: Arc<dyn CallbackHandler>,
        opts: StreamOptions,
    ) -> (PythonStream, mpsc::UnboundedReceiver<StreamEvent>) {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = watch::channel(false);
        let args = Msg::new()
            .set("connector", connector_keys)
            .set("pipe", map_to_wire(&pipe.to_state()))
            .set("kwargs", map_to_wire(kwargs))
            .set("keep_alive", true);
        let op = match mode {
            StreamMode::V4Sync => "sync",
            StreamMode::Subscribe => "subscribe",
        };
        let task = tokio::spawn(supervise(pool, op, args, handler, opts, stop_rx, events_tx));
        (PythonStream { stop_tx, task }, events_rx)
    }

    /// Stop the stream: ask the worker to exit (killing it after a grace period) and wait for
    /// the supervisor.
    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.task.await;
    }

    /// Whether the supervisor is still running.
    pub fn is_running(&self) -> bool {
        !self.task.is_finished()
    }
}

async fn supervise(
    pool: WorkerPool,
    op: &'static str,
    args: Msg,
    handler: Arc<dyn CallbackHandler>,
    opts: StreamOptions,
    mut stop_rx: watch::Receiver<bool>,
    events: mpsc::UnboundedSender<StreamEvent>,
) {
    let mut attempt: u32 = 0;
    let stopped = |rx: &watch::Receiver<bool>| *rx.borrow();
    loop {
        if stopped(&stop_rx) {
            break;
        }
        let worker = tokio::select! {
            w = pool.spawn_worker() => w,
            _ = stop_rx.changed() => break,
        };
        let error = match worker {
            Err(e) => e.to_string(),
            Ok(worker) => {
                worker.set_callback_handler(Some(Arc::clone(&handler)));
                let _ = events.send(StreamEvent::Started { pid: worker.pid() });
                let mut stop_requested = false;
                let mut error = String::new();
                {
                    let call = worker.call(op, args.clone(), Vec::new(), CallOptions::default());
                    tokio::select! {
                        res = call => match res {
                            Ok(reply) => {
                                let st = match &reply.value {
                                    Wire::Array(items) if items.len() == 2 => SuccessTuple {
                                        success: items[0].as_bool().unwrap_or(false),
                                        message: items[1].as_str().unwrap_or_default().to_string(),
                                    },
                                    _ => SuccessTuple::success(),
                                };
                                let ok = st.success;
                                let _ = events.send(StreamEvent::Returned(st.clone()));
                                if ok {
                                    attempt = 0;
                                    // Keep the worker alive for callbacks until it exits or
                                    // the stream is stopped.
                                    tokio::select! {
                                        _ = worker.closed() => {
                                            error = "the stream worker exited".into();
                                        }
                                        _ = stop_rx.changed() => stop_requested = true,
                                    }
                                } else {
                                    error = st.message;
                                }
                            }
                            Err(e) => error = e.to_string(),
                        },
                        _ = stop_rx.changed() => stop_requested = true,
                    }
                }
                if stop_requested || stopped(&stop_rx) {
                    worker.shutdown(Duration::from_secs(5)).await;
                    break;
                }
                worker.kill().await;
                error
            }
        };
        let _ = events.send(StreamEvent::Exited {
            error: error.clone(),
        });
        if !opts.restart {
            break;
        }
        attempt = attempt.saturating_add(1);
        if opts.max_restarts.is_some_and(|m| attempt > m) {
            break;
        }
        let delay = backoff(&opts, attempt);
        let _ = events.send(StreamEvent::Restarting { attempt, delay });
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = stop_rx.changed() => break,
        }
    }
    let _ = events.send(StreamEvent::Stopped);
}
