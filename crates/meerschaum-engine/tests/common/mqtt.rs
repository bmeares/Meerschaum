//! A minimal MQTT 3.1.1 client for the stream tests against a disposable Mosquitto
//! (`MRSM_TEST_MQTT_URI`, for example `mqtt://127.0.0.1:1893`): QoS 1, persistent sessions,
//! manual acknowledgement. Hand-written so the engine's dev-dependencies stay small.
#![allow(dead_code)]

use std::sync::Arc;

use indexmap::IndexMap;
use meerschaum_connector::async_trait;
use meerschaum_core::Pipe;
use meerschaum_engine::stream::{
    AckToken, LegacySink, LegacyStream, StopToken, StreamMessage, StreamSender, StreamSource,
};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

/// The broker address from `MRSM_TEST_MQTT_URI` (`mqtt://host:port`); `None` skips the test.
pub fn broker() -> Option<String> {
    let raw = std::env::var("MRSM_TEST_MQTT_URI").ok()?;
    let raw = if raw.is_empty() {
        "mqtt://127.0.0.1:1893".to_string()
    } else {
        raw
    };
    let addr = raw
        .trim_start_matches("mqtt://")
        .trim_start_matches("tcp://")
        .trim_end_matches('/')
        .to_string();
    assert!(
        !addr.ends_with(":1883"),
        "refusing to use port 1883: it is the user's own broker"
    );
    Some(addr)
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn put_len(out: &mut Vec<u8>, mut n: usize) {
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
}

fn packet(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    put_len(&mut out, body.len());
    out.extend_from_slice(body);
    out
}

/// One inbound packet.
#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    /// CONNACK (session present, return code).
    ConnAck(bool, u8),
    /// SUBACK.
    SubAck,
    /// PUBACK for a publish we sent.
    PubAck(u16),
    /// PUBLISH from the broker.
    Publish {
        /// The topic.
        topic: String,
        /// The payload.
        payload: Vec<u8>,
        /// The packet ID (QoS 1).
        pid: Option<u16>,
        /// The DUP flag: a redelivery.
        dup: bool,
    },
    /// Anything else (PINGRESP, ...).
    Other(u8),
}

/// Read one packet; `None` when the connection closed.
pub async fn read_packet(r: &mut OwnedReadHalf) -> Option<Packet> {
    let first = r.read_u8().await.ok()?;
    let mut len = 0usize;
    let mut shift = 0;
    loop {
        let b = r.read_u8().await.ok()?;
        len |= ((b & 0x7f) as usize) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await.ok()?;
    Some(match first >> 4 {
        2 => Packet::ConnAck(body[0] & 1 == 1, body[1]),
        9 => Packet::SubAck,
        4 => Packet::PubAck(u16::from_be_bytes([body[0], body[1]])),
        3 => {
            let tlen = u16::from_be_bytes([body[0], body[1]]) as usize;
            let topic = String::from_utf8_lossy(&body[2..2 + tlen]).to_string();
            let qos = (first >> 1) & 3;
            let mut at = 2 + tlen;
            let pid = (qos > 0).then(|| {
                let p = u16::from_be_bytes([body[at], body[at + 1]]);
                at += 2;
                p
            });
            Packet::Publish {
                topic,
                payload: body[at..].to_vec(),
                pid,
                dup: first & 0x08 != 0,
            }
        }
        other => Packet::Other(other),
    })
}

/// Connect with a client ID; `clean = false` keeps the broker-side session (and queued QoS 1
/// messages) across connections.
pub async fn connect(addr: &str, client_id: &str, clean: bool) -> (OwnedReadHalf, OwnedWriteHalf) {
    let stream = TcpStream::connect(addr).await.expect("connect to broker");
    let (mut r, mut w) = stream.into_split();
    let mut body = Vec::new();
    put_str(&mut body, "MQTT");
    body.push(4);
    body.push(if clean { 0x02 } else { 0x00 });
    body.extend_from_slice(&0u16.to_be_bytes());
    put_str(&mut body, client_id);
    w.write_all(&packet(0x10, &body)).await.unwrap();
    match read_packet(&mut r).await {
        Some(Packet::ConnAck(_, 0)) => {}
        other => panic!("CONNECT failed: {other:?}"),
    }
    (r, w)
}

/// SUBSCRIBE at QoS 1 without waiting: a resumed session may deliver queued messages before
/// the SUBACK, and the caller's read loop must see them.
pub async fn send_subscribe(w: &mut OwnedWriteHalf, topic: &str) {
    let mut body = vec![0, 1];
    put_str(&mut body, topic);
    body.push(1);
    w.write_all(&packet(0x82, &body)).await.unwrap();
}

/// SUBSCRIBE at QoS 1 and wait for the SUBACK (a fresh session, nothing queued).
pub async fn subscribe(r: &mut OwnedReadHalf, w: &mut OwnedWriteHalf, topic: &str) {
    let mut body = vec![0, 1];
    put_str(&mut body, topic);
    body.push(1);
    w.write_all(&packet(0x82, &body)).await.unwrap();
    loop {
        match read_packet(r).await {
            Some(Packet::SubAck) => return,
            // Queued messages may arrive first; they are redelivered unacknowledged.
            Some(_) => {}
            None => panic!("connection closed during SUBSCRIBE"),
        }
    }
}

/// PUBLISH at QoS 1 and wait for the PUBACK.
pub async fn publish(addr: &str, topic: &str, payload: &str) {
    let (mut r, mut w) = connect(
        addr,
        &format!("pub-{}", uuid::Uuid::new_v4().simple()),
        true,
    )
    .await;
    let mut body = Vec::new();
    put_str(&mut body, topic);
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(payload.as_bytes());
    w.write_all(&packet(0x32, &body)).await.unwrap();
    loop {
        match read_packet(&mut r).await {
            Some(Packet::PubAck(1)) => break,
            Some(_) => {}
            None => panic!("connection closed before PUBACK"),
        }
    }
    let _ = w.write_all(&packet(0xE0, &[])).await;
}

/// Open the persistent session and subscription, then disconnect (so later publishes queue).
pub async fn create_session(addr: &str, client_id: &str, topic: &str) {
    let (mut r, mut w) = connect(addr, client_id, false).await;
    subscribe(&mut r, &mut w, topic).await;
    let _ = w.write_all(&packet(0xE0, &[])).await;
}

/// The messages left in a persistent session (delivered, never acknowledged here), as
/// `(payload, dup)`, waiting `wait` for stragglers.
pub async fn drain_session(
    addr: &str,
    client_id: &str,
    wait: std::time::Duration,
) -> Vec<(String, bool)> {
    let (mut r, mut w) = connect(addr, client_id, false).await;
    let mut out = Vec::new();
    while let Ok(Some(p)) = tokio::time::timeout(wait, read_packet(&mut r)).await {
        if let Packet::Publish {
            payload, pid, dup, ..
        } = p
        {
            out.push((String::from_utf8_lossy(&payload).to_string(), dup));
            // Leave it unacknowledged: do not consume the broker's copy.
            let _ = pid;
        }
    }
    let _ = w.write_all(&packet(0xE0, &[])).await;
    out
}

/// A payload `{"ts": "...", "id": N, "v": X}` as one row.
pub fn doc_of(payload: &[u8]) -> IndexMap<String, KValue> {
    let json: serde_json::Value = serde_json::from_slice(payload).expect("JSON payload");
    let mut doc = IndexMap::new();
    let ts = chrono::DateTime::parse_from_rfc3339(json["ts"].as_str().unwrap())
        .unwrap()
        .naive_utc();
    doc.insert("ts".to_string(), KValue::Datetime(DateTimeValue::utc(ts)));
    doc.insert("id".to_string(), KValue::Int(json["id"].as_i64().unwrap()));
    doc.insert("v".to_string(), KValue::Float(json["v"].as_f64().unwrap()));
    doc
}

/// The native path: `subscribe(pipe, sink)` pushes each message with its packet ID as the
/// acknowledgement token; `ack` sends the PUBACKs, after the engine's sink committed.
pub struct MqttSource {
    /// Broker address.
    pub addr: String,
    /// Persistent session client ID.
    pub client_id: String,
    /// Topic.
    pub topic: String,
    writer: Arc<Mutex<Option<OwnedWriteHalf>>>,
    deliveries: std::sync::Mutex<Vec<(u16, bool)>>,
}

impl MqttSource {
    /// Every delivery received, as `(packet ID, DUP)`.
    pub fn deliveries(&self) -> Vec<(u16, bool)> {
        self.deliveries.lock().unwrap().clone()
    }

    /// A source for a topic on a persistent session.
    pub fn new(addr: &str, client_id: &str, topic: &str) -> MqttSource {
        MqttSource {
            addr: addr.into(),
            client_id: client_id.into(),
            topic: topic.into(),
            writer: Arc::new(Mutex::new(None)),
            deliveries: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl StreamSource for MqttSource {
    async fn subscribe(
        &self,
        _pipe: &Pipe,
        sink: StreamSender,
        stop: StopToken,
    ) -> Result<(), String> {
        let (mut r, mut w) = connect(&self.addr, &self.client_id, false).await;
        send_subscribe(&mut w, &self.topic).await;
        *self.writer.lock().await = Some(w);
        loop {
            tokio::select! {
                _ = stop.stopped() => {
                    // A graceful stop: the sink flushes and acknowledges what it buffered.
                    return Ok(());
                }
                p = read_packet(&mut r) => match p {
                    Some(Packet::Publish { payload, pid: Some(pid), dup, .. }) => {
                        self.deliveries.lock().unwrap().push((pid, dup));
                        let msg = StreamMessage {
                            docs: vec![doc_of(&payload)],
                            bytes: payload.len() as u64,
                            ack: Some(u64::from(pid)),
                            redelivered: dup,
                        };
                        if sink.send(msg).await.is_err() {
                            return Ok(());
                        }
                    }
                    Some(_) => {}
                    None => return Err("the broker closed the connection".into()),
                }
            }
        }
    }

    async fn ack(&self, tokens: Vec<AckToken>) -> Result<(), String> {
        let mut guard = self.writer.lock().await;
        let Some(w) = guard.as_mut() else {
            return Err("not connected".into());
        };
        for t in tokens {
            let pid = (t as u16).to_be_bytes();
            w.write_all(&packet(0x40, &pid))
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

/// What a v4-pattern plugin's `on_message` callback does with a message: sync it (through the
/// native path) and return `true` when it committed.
pub type Callback = Arc<
    dyn Fn(Arc<dyn LegacySink>, IndexMap<String, KValue>, bool) -> futures_like::BoxFut
        + Send
        + Sync,
>;

/// A tiny boxed future alias (no `futures` dependency in the tests).
pub mod futures_like {
    use std::future::Future;
    use std::pin::Pin;
    /// A boxed `bool` future.
    pub type BoxFut = Pin<Box<dyn Future<Output = bool> + Send>>;
}

/// The v4 pattern: a `sync()` subscribes and returns, callbacks call `pipe.sync()`, and the
/// client acknowledges (PUBACK) when the callback returns, like paho's automatic acknowledgement.
pub struct MqttLegacy {
    /// Broker address.
    pub addr: String,
    /// Persistent session client ID.
    pub client_id: String,
    /// Topic.
    pub topic: String,
    /// The callback each message runs (`pipe.sync(df)`).
    pub callback: Callback,
}

#[async_trait]
impl LegacyStream for MqttLegacy {
    async fn run(
        &self,
        _pipe: &Pipe,
        sink: Arc<dyn LegacySink>,
        stop: StopToken,
    ) -> Result<(), String> {
        let (mut r, mut w) = connect(&self.addr, &self.client_id, false).await;
        send_subscribe(&mut w, &self.topic).await;
        loop {
            tokio::select! {
                _ = stop.stopped() => return Ok(()),
                p = read_packet(&mut r) => match p {
                    Some(Packet::Publish { payload, pid: Some(pid), dup, .. }) => {
                        let committed = (self.callback)(sink.clone(), doc_of(&payload), dup).await;
                        if committed {
                            w.write_all(&packet(0x40, &pid.to_be_bytes()))
                                .await
                                .map_err(|e| e.to_string())?;
                        }
                    }
                    Some(_) => {}
                    None => return Err("the broker closed the connection".into()),
                }
            }
        }
    }
}
