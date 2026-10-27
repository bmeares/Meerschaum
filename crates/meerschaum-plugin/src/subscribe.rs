//! Streaming connectors: a pull-model subscription over the plugin ABI.
//!
//! A connector type that declares `"is_streaming": true` and lists `subscribe` in its manifest
//! `methods` (or a plugin whose `data_functions` lists `subscribe`, for `plugin:<name>`) is a
//! stream source for `sync pipes --listen` and the engine's supervised streams.
//!
//! The host drives it with four JSON methods (data crosses as an Arrow C stream, exactly like
//! `fetch`):
//!
//! | Method | Context | Reply value | Output stream |
//! |---|---|---|---|
//! | `subscribe` | `{pipe, kwargs}` | `{"handle": u64}` | none |
//! | `next_batch` | `{handle, timeout_ms}` | `{"status": "batch" \| "timeout" \| "end", "ack": u64 \| null, "redelivered": bool}` | the batch when `status` is `batch` |
//! | `ack_batches` | `{handle, tokens: [u64]}` | `null` | none |
//! | `close_subscription` | `{handle}` | `null` | none |
//!
//! `subscribe` is an ordinary serialized call; `next_batch`, `ack_batches` and
//! `close_subscription` are **not** serialized with the plugin's other calls, so one blocking
//! `next_batch` never holds up a `fetch` or another subscription. All four get the connector's
//! host services: each may make host calls on its calling thread. The host calls
//! `next_batch` again only after it has handed the previous batch to the engine's sink, so a slow
//! consumer slows the source down. `ack_batches` arrives once the rows of those batches committed.
//! `close_subscription` is called exactly once per handle (after `end`, on stop, or on error).
//!
//! In the SDK, implement [`crate::Plugin::subscribe`] and return a [`Subscription`]. The SDK owns
//! the handle table: the four methods above never reach [`crate::Plugin::call`].
use crate::{
    Frame, Result,
    handles::{Handles, handle_of, timeout_of},
    sdk::{Reply, Request},
};
use serde_json::{Value, json};
use std::{
    sync::{LazyLock, mpsc},
    time::Duration,
};

/// The result of one [`Subscription::next_batch`].
pub enum Batch {
    /// Rows. `ack` is the token [`Subscription::ack`] receives once the rows committed
    /// (`None`: the source does not acknowledge); `redelivered` forces `check_existing`.
    Data {
        frame: Frame,
        ack: Option<u64>,
        redelivered: bool,
    },
    /// Nothing arrived within the timeout; the host will ask again.
    Timeout,
    /// The subscription ended (the host closes it and the engine subscribes again after a
    /// backoff, as when a Python `subscribe` returns).
    End,
}

impl Batch {
    /// Rows without an acknowledgement token.
    pub fn data(frame: Frame) -> Self {
        Batch::Data {
            frame,
            ack: None,
            redelivered: false,
        }
    }
    /// Rows to acknowledge with `token` after they commit.
    pub fn with_ack(frame: Frame, token: u64) -> Self {
        Batch::Data {
            frame,
            ack: Some(token),
            redelivered: false,
        }
    }
}

/// An open subscription. It may block in `next_batch` up to `timeout` (on its own thread or a
/// channel); it is `Send` because the host may call it from any thread, one call at a time.
pub trait Subscription: Send {
    /// The next batch, waiting at most `timeout`.
    fn next_batch(&mut self, timeout: Duration) -> Result<Batch>;
    /// Acknowledge committed batches (only ever called after the commit).
    fn ack(&mut self, tokens: &[u64]) -> Result<()> {
        let _ = tokens;
        Ok(())
    }
    /// Release the source. Called once; dropping the subscription follows.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A subscription fed by a producer thread through a bounded channel: `send` blocks when
/// `capacity` frames wait, which is how a slow consumer slows the producer. Closing (or the host
/// dropping the subscription) disconnects the channel, so the producer's next `send` fails and
/// its thread can return. A producer that drops its sender ends the subscription.
pub struct ChannelSubscription {
    receiver: Option<mpsc::Receiver<Frame>>,
}

impl ChannelSubscription {
    /// A sender for the producer thread and the subscription to return from `subscribe`.
    pub fn new(capacity: usize) -> (mpsc::SyncSender<Frame>, Self) {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        (
            sender,
            Self {
                receiver: Some(receiver),
            },
        )
    }
}

impl Subscription for ChannelSubscription {
    fn next_batch(&mut self, timeout: Duration) -> Result<Batch> {
        let Some(receiver) = &self.receiver else {
            return Ok(Batch::End);
        };
        match receiver.recv_timeout(timeout) {
            Ok(frame) => Ok(Batch::data(frame)),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(Batch::Timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Ok(Batch::End),
        }
    }
    fn close(&mut self) -> Result<()> {
        self.receiver = None;
        Ok(())
    }
}

type Open = dyn Subscription;
static OPEN: LazyLock<Handles<Open>> = LazyLock::new(Handles::new);

/// SDK routing of the subscription methods (`None`: not a subscription method).
pub(crate) fn dispatch<P: crate::Plugin>(request: &Request) -> Option<Result<Reply>> {
    let reply = match request.method.as_str() {
        "subscribe" => P::default()
            .subscribe(request)
            .map(|subscription| Reply::json(json!({"handle": OPEN.insert(subscription)}))),
        "next_batch" => next_batch(&request.context),
        "ack_batches" => (|| {
            let tokens: Vec<u64> =
                serde_json::from_value(request.context["tokens"].clone()).map_err(crate::error)?;
            let entry = OPEN.get(handle_of(&request.context)?)?;
            let mut subscription = entry.lock().unwrap_or_else(|e| e.into_inner());
            subscription.ack(&tokens).map(|()| Reply::json(Value::Null))
        })(),
        "close_subscription" => (|| {
            // Closing an unknown handle is a no-op: close is idempotent.
            if let Some(entry) = OPEN.remove(handle_of(&request.context)?) {
                entry.lock().unwrap_or_else(|e| e.into_inner()).close()?;
            }
            Ok(Reply::json(Value::Null))
        })(),
        _ => return None,
    };
    Some(reply)
}

fn next_batch(context: &Value) -> Result<Reply> {
    let entry = OPEN.get(handle_of(context)?)?;
    let mut subscription = entry.lock().unwrap_or_else(|e| e.into_inner());
    Ok(match subscription.next_batch(timeout_of(context))? {
        Batch::Data {
            frame,
            ack,
            redelivered,
        } => Reply {
            value: json!({"status": "batch", "ack": ack, "redelivered": redelivered}),
            data: Some(frame),
        },
        Batch::Timeout => {
            Reply::json(json!({"status": "timeout", "ack": null, "redelivered": false}))
        }
        Batch::End => Reply::json(json!({"status": "end", "ack": null, "redelivered": false})),
    })
}

// ---------------------------------------------------------------------------------------------
// Host side.
// ---------------------------------------------------------------------------------------------

/// What [`NativeSubscription::next_batch`] received.
#[derive(Debug)]
pub enum NativeBatch {
    /// Rows, the token to acknowledge them with, and whether they were redelivered.
    Data {
        frame: Frame,
        ack: Option<u64>,
        redelivered: bool,
    },
    /// Nothing within the timeout.
    Timeout,
    /// The source ended.
    End,
}

/// An open subscription of a native connector. Dropping it closes the handle.
pub struct NativeSubscription {
    plugin: std::sync::Arc<crate::LoadedPlugin>,
    request: Request,
    handle: u64,
    closed: std::sync::atomic::AtomicBool,
    /// The connector's host services: a batch method may make host calls too.
    host: Option<std::sync::Arc<dyn crate::HostServices>>,
}

impl std::fmt::Debug for NativeSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeSubscription")
            .field("plugin", &self.plugin.name)
            .field("handle", &self.handle)
            .finish()
    }
}

impl NativeSubscription {
    pub(crate) fn new(
        plugin: std::sync::Arc<crate::LoadedPlugin>,
        base: Request,
        handle: u64,
        host: Option<std::sync::Arc<dyn crate::HostServices>>,
    ) -> Self {
        Self {
            plugin,
            request: base,
            handle,
            closed: std::sync::atomic::AtomicBool::new(false),
            host,
        }
    }

    /// The plugin's handle.
    pub fn handle(&self) -> u64 {
        self.handle
    }

    fn stream_call(&self, method: &str, mut context: Value) -> Result<crate::Reply> {
        context["handle"] = json!(self.handle);
        let request = Request {
            method: method.into(),
            context,
            ..self.request.clone()
        };
        self.plugin
            .invoke_stream_with_host(&request, self.host.as_deref())
    }

    /// The next batch, waiting at most `timeout` inside the plugin.
    pub fn next_batch(&self, timeout: Duration) -> Result<NativeBatch> {
        let reply = self.stream_call(
            "next_batch",
            json!({"timeout_ms": timeout.as_millis() as u64}),
        )?;
        match reply.value["status"].as_str() {
            Some("batch") => {
                let ack = reply.value["ack"].as_u64();
                Ok(match (reply.data, ack) {
                    (Some(frame), _) => NativeBatch::Data {
                        frame,
                        ack,
                        redelivered: reply.value["redelivered"].as_bool().unwrap_or(false),
                    },
                    // An empty batch with a token (a filtered message) still has to be
                    // acknowledged once the rows before it commit.
                    (None, Some(_)) => NativeBatch::Data {
                        frame: Frame::empty(),
                        ack,
                        redelivered: false,
                    },
                    // A batch without data or a token: nothing to sync or acknowledge.
                    (None, None) => NativeBatch::Timeout,
                })
            }
            Some("timeout") => Ok(NativeBatch::Timeout),
            Some("end") => Ok(NativeBatch::End),
            other => Err(crate::error(format!(
                "invalid next_batch status: {}",
                other.unwrap_or("null")
            ))),
        }
    }

    /// Acknowledge committed batches.
    pub fn ack(&self, tokens: &[u64]) -> Result<()> {
        self.stream_call("ack_batches", json!({"tokens": tokens}))
            .map(|_| ())
    }

    /// Close the handle (idempotent; also done on drop).
    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        self.stream_call("close_subscription", json!({}))
            .map(|_| ())
    }
}

impl Drop for NativeSubscription {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Plugin;

    fn frame(id: i64) -> Frame {
        Frame::from_object_columns(vec![(
            "id".to_string(),
            vec![meerschaum_kernel::Value::Int(id)],
        )])
        .unwrap()
    }

    #[derive(Default)]
    struct Counter;
    impl Plugin for Counter {
        const NAME: &'static str = "counter";
        const VERSION: &'static str = "1";
        fn manifest(&self) -> Value {
            Value::Null
        }
        fn subscribe(&self, _request: &Request) -> Result<Box<dyn Subscription>> {
            let (sender, subscription) = ChannelSubscription::new(1);
            std::thread::spawn(move || {
                for id in 0..3 {
                    if sender.send(frame(id)).is_err() {
                        return;
                    }
                }
            });
            Ok(Box::new(subscription))
        }
    }

    fn request(method: &str, context: Value) -> Request {
        Request {
            method: method.into(),
            connector_type: "counter".into(),
            label: "a".into(),
            config: Value::Null,
            context,
        }
    }

    #[test]
    fn channel_subscription_delivers_then_ends_and_closes() {
        let opened = dispatch::<Counter>(&request("subscribe", json!({})))
            .unwrap()
            .unwrap();
        let handle = opened.value["handle"].as_u64().unwrap();
        let mut ids = Vec::new();
        loop {
            let reply = dispatch::<Counter>(&request(
                "next_batch",
                json!({"handle": handle, "timeout_ms": 1000}),
            ))
            .unwrap()
            .unwrap();
            match reply.value["status"].as_str().unwrap() {
                "batch" => ids.push(reply.data.unwrap().height()),
                "timeout" => continue,
                _ => break,
            }
        }
        assert_eq!(ids, vec![1, 1, 1]);
        let before = OPEN.len();
        dispatch::<Counter>(&request("close_subscription", json!({"handle": handle})))
            .unwrap()
            .unwrap();
        assert_eq!(OPEN.len(), before - 1);
        // Idempotent close; unknown handles are errors for next_batch.
        assert!(
            dispatch::<Counter>(&request("close_subscription", json!({"handle": handle})))
                .unwrap()
                .is_ok()
        );
        assert!(
            dispatch::<Counter>(&request("next_batch", json!({"handle": handle})))
                .unwrap()
                .is_err()
        );
    }

    #[test]
    fn default_subscribe_is_unsupported() {
        #[derive(Default)]
        struct Plain;
        impl Plugin for Plain {
            const NAME: &'static str = "plain";
            const VERSION: &'static str = "1";
            fn manifest(&self) -> Value {
                Value::Null
            }
        }
        assert!(matches!(
            dispatch::<Plain>(&request("subscribe", json!({}))).unwrap(),
            Err(crate::Error::Unsupported(_))
        ));
        assert!(dispatch::<Plain>(&request("fetch", json!({}))).is_none());
    }
}
