//! `retry_connect()` for a Valkey instance: one attempt connects (when no connection exists yet)
//! and sends `PING`.

use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::retry::{Attempt, AttemptCx, RetryProbe};
use meerschaum_core::Map;

use crate::ValkeyConnector;

/// The [`RetryProbe`] of a Valkey connector.
pub struct ValkeyRetryProbe {
    label: String,
    attributes: Map,
    /// The connection, from the first attempt that made one (or the caller's, when it has one).
    live: Option<Arc<ValkeyConnector>>,
}

impl ValkeyRetryProbe {
    /// A probe that connects with `attributes`, or uses `live` when the caller already
    /// connected.
    pub fn new(
        label: impl Into<String>,
        attributes: Map,
        live: Option<Arc<ValkeyConnector>>,
    ) -> ValkeyRetryProbe {
        ValkeyRetryProbe {
            label: label.into(),
            attributes,
            live,
        }
    }

    /// The connection an attempt made, for the caller to keep using.
    pub fn live(&self) -> Option<Arc<ValkeyConnector>> {
        self.live.clone()
    }

    /// The longest an attempt may take: the connection (bounded by the connection timeout, which
    /// the connect itself enforces) and the `PING`, and a second of grace.
    pub fn attempt_timeout(attributes: &Map) -> Duration {
        let attribute = attributes.get("connect_timeout").map(|v| match v {
            meerschaum_core::Value::Str(s) => s.clone(),
            other => meerschaum_core::pyfmt::py_str(other),
        });
        meerschaum_core::connect::connect_timeout(attribute.as_deref())
            .saturating_mul(2)
            .saturating_add(Duration::from_secs(1))
    }
}

impl RetryProbe for ValkeyRetryProbe {
    async fn attempt(&mut self, cx: &mut AttemptCx<'_>) -> Attempt {
        let connector = match &self.live {
            Some(connector) => connector.clone(),
            None => match ValkeyConnector::connect(&self.label, self.attributes.clone()).await {
                Ok(connector) => {
                    let connector = Arc::new(connector);
                    self.live = Some(connector.clone());
                    connector
                }
                Err(e) => {
                    cx.detail(&e.to_string());
                    return Attempt::Failed;
                }
            },
        };
        match connector.ping().await {
            Ok(true) => Attempt::Connected,
            // v4 printed nothing for a `False` answer.
            Ok(false) => Attempt::Failed,
            Err(e) => {
                cx.detail(&e.to_string());
                Attempt::Failed
            }
        }
    }
}
