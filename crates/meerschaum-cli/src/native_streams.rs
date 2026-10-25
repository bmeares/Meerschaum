//! Streaming connectors of native plugins as engine stream sources (`sync pipes --listen`, the
//! engine's supervised streams): the pull-model subscription of
//! `crates/meerschaum-plugin/src/subscribe.rs` behind [`StreamSource`].
//!
//! `subscribe` opens the plugin's subscription with the sync's keyword arguments (the same
//! `kwargs` a Python `subscribe` source receives: `begin`, `end`, `params`, flags), then a loop
//! asks for the next batch (at most [`POLL`] inside the plugin, on a blocking thread) and hands
//! each batch to the engine's sink before asking again: the sink's bounded channel is the
//! back-pressure, so a slow instance slows the source. The engine acknowledges committed batches
//! through [`StreamSource::ack`].
//!
//! Plugin ack tokens are scoped to one subscription (an MQTT packet id, a Kafka offset), while the
//! engine keeps one sink across resubscribes and may flush (and acknowledge) a batch of the
//! previous subscription after the next one opened. The source therefore hands the sink its own
//! tokens and remembers which subscription each came from; a token whose subscription has
//! closed is dropped (the source redelivers that message), never forwarded to its successor.
//!
//! A stop closes the subscription; an error closes it and returns `Err` (the supervisor
//! resubscribes with backoff), and `end` returns `Ok` (resubscribed after the initial backoff, as
//! when a Python `subscribe` returns).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indexmap::IndexMap;
use meerschaum_connector::async_trait;
use meerschaum_core::Pipe;
use meerschaum_engine::stream::{AckToken, StopToken, StreamMessage, StreamSender, StreamSource};
use meerschaum_kernel::{Frame, Value as KValue};
use meerschaum_plugin::NativeConnector;
use meerschaum_plugin::subscribe::{NativeBatch, NativeSubscription};

/// How long one `next_batch` may wait inside the plugin before the host checks for a stop.
pub const POLL: Duration = Duration::from_millis(250);

/// The open subscription and the engine tokens handed out for it.
#[derive(Default)]
struct Tokens {
    /// The generation of the open subscription (0: none yet).
    generation: u64,
    current: Option<Arc<NativeSubscription>>,
    /// The next engine token.
    next: AckToken,
    /// Engine token -> (subscription generation, plugin token).
    pending: HashMap<AckToken, (u64, u64)>,
}

impl Tokens {
    /// Start a new subscription; returns its generation.
    fn open(&mut self) -> u64 {
        self.generation += 1;
        self.generation
    }

    /// Forget the unacknowledged tokens of `generation`.
    fn close(&mut self, generation: u64) {
        self.pending.retain(|_, (g, _)| *g != generation);
    }

    /// The engine token for a plugin token of subscription `generation`.
    fn issue(&mut self, generation: u64, plugin_token: u64) -> AckToken {
        self.next += 1;
        self.pending.insert(self.next, (generation, plugin_token));
        self.next
    }

    /// The plugin tokens of `tokens` that belong to the open subscription; the rest are dropped.
    fn take(&mut self, tokens: &[AckToken]) -> Vec<u64> {
        let generation = self.generation;
        tokens
            .iter()
            .filter_map(|token| match self.pending.remove(token) {
                Some((g, t)) if g == generation => Some(t),
                _ => {
                    tracing::debug!("Dropping ack token {token} of a closed subscription.");
                    None
                }
            })
            .collect()
    }
}

/// A native streaming connector.
pub struct NativeStreamSource {
    connector: NativeConnector,
    /// The sync's keyword arguments, sent with `subscribe`.
    kwargs: serde_json::Value,
    state: Mutex<Tokens>,
}

impl NativeStreamSource {
    /// A stream source over a streaming native connector; `kwargs` reach the plugin's `subscribe`.
    pub fn new(connector: NativeConnector, kwargs: serde_json::Value) -> NativeStreamSource {
        NativeStreamSource {
            connector,
            kwargs: if kwargs.is_object() {
                kwargs
            } else {
                serde_json::json!({})
            },
            state: Mutex::new(Tokens::default()),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, Tokens> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Make `subscription` the open one; returns its generation.
    fn open(&self, subscription: Arc<NativeSubscription>) -> u64 {
        let mut state = self.state();
        state.current = Some(subscription);
        state.open()
    }

    /// Forget the subscription of `generation` and its unacknowledged tokens.
    fn closed(&self, generation: u64) {
        let mut state = self.state();
        if state.generation == generation {
            state.current = None;
        }
        state.close(generation);
    }

    /// The engine token for a plugin token of subscription `generation`.
    fn token(&self, generation: u64, plugin_token: u64) -> AckToken {
        self.state().issue(generation, plugin_token)
    }

    /// The open subscription and the plugin tokens of `tokens` that belong to it.
    fn resolve(&self, tokens: &[AckToken]) -> Option<(Arc<NativeSubscription>, Vec<u64>)> {
        let mut state = self.state();
        let plugin_tokens = state.take(tokens);
        let current = state.current.clone()?;
        (!plugin_tokens.is_empty()).then_some((current, plugin_tokens))
    }
}

/// The rows of a frame as the sink's documents.
pub fn frame_docs(frame: &Frame) -> Vec<IndexMap<String, KValue>> {
    let names: Vec<String> = frame.columns().iter().map(|c| c.name.clone()).collect();
    (0..frame.height())
        .map(|i| names.iter().cloned().zip(frame.row(i)).collect())
        .collect()
}

fn frame_bytes(frame: &Frame) -> u64 {
    frame
        .to_record_batch()
        .map(|batch| batch.get_array_memory_size() as u64)
        .unwrap_or_default()
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| e.to_string())?
}

#[async_trait]
impl StreamSource for NativeStreamSource {
    async fn subscribe(
        &self,
        pipe: &Pipe,
        sink: StreamSender,
        stop: StopToken,
    ) -> Result<(), String> {
        let connector = self.connector.clone();
        let opened = pipe.clone();
        let kwargs = self.kwargs.clone();
        let subscription = Arc::new(
            blocking(move || {
                connector
                    .subscribe(&opened, kwargs)
                    .map_err(|e| e.to_string())
            })
            .await?,
        );
        let generation = self.open(subscription.clone());
        let outcome = loop {
            if stop.is_stopped() {
                break Ok(());
            }
            let next = subscription.clone();
            match blocking(move || next.next_batch(POLL).map_err(|e| e.to_string())).await {
                Ok(NativeBatch::Data {
                    frame,
                    ack,
                    redelivered,
                }) => {
                    let message = StreamMessage {
                        docs: frame_docs(&frame),
                        bytes: frame_bytes(&frame),
                        ack: ack.map(|t| self.token(generation, t)),
                        redelivered,
                    };
                    // An empty batch still carries its token to the sink, which acknowledges it
                    // with the next committed flush (as the Python path does).
                    if message.docs.is_empty() && message.ack.is_none() {
                        continue;
                    }
                    tokio::select! {
                        sent = sink.send(message) => if sent.is_err() { break Ok(()); },
                        _ = stop.stopped() => break Ok(()),
                    }
                }
                Ok(NativeBatch::Timeout) => {}
                Ok(NativeBatch::End) => break Ok(()),
                Err(error) => break Err(error),
            }
        };
        self.closed(generation);
        let closing = subscription;
        let closed = blocking(move || closing.close().map_err(|e| e.to_string())).await;
        outcome.and(closed)
    }

    async fn ack(&self, tokens: Vec<AckToken>) -> Result<(), String> {
        // Tokens of a closed subscription are not delivered (the source redelivers them).
        let Some((subscription, plugin_tokens)) = self.resolve(&tokens) else {
            return Ok(());
        };
        blocking(move || subscription.ack(&plugin_tokens).map_err(|e| e.to_string())).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_rows_become_documents() {
        let frame = Frame::from_object_columns(vec![
            ("id".into(), vec![KValue::Int(1), KValue::Int(2)]),
            ("v".into(), vec![KValue::Float(0.5), KValue::Null]),
        ])
        .unwrap();
        let docs = frame_docs(&frame);
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0]["id"], KValue::Int(1));
        assert_eq!(docs[1]["v"], KValue::Null);
        assert!(frame_bytes(&frame) > 0);
    }

    #[test]
    fn tokens_never_reach_a_later_subscription() {
        let mut tokens = Tokens::default();
        let first = tokens.open();
        let a = tokens.issue(first, 7);
        let b = tokens.issue(first, 8);
        // The first subscription ends with `b` still buffered in the sink.
        assert_eq!(tokens.take(&[a]), vec![7]);
        tokens.close(first);
        let second = tokens.open();
        // The second subscription reuses plugin token 7: a distinct engine token.
        let c = tokens.issue(second, 7);
        assert_ne!(c, a);
        // `b` (flushed late) is dropped, not acknowledged on the second subscription.
        assert_eq!(tokens.take(&[b, c]), vec![7]);
        assert!(tokens.pending.is_empty());
        // A stale token after a resubscribe without a close is dropped too.
        let d = tokens.issue(second, 9);
        let third = tokens.open();
        let e = tokens.issue(third, 1);
        assert_eq!(tokens.take(&[d, e]), vec![1]);
    }
}
