//! The worker wire protocol (version [`PROTOCOL_VERSION`]); see the crate README for the full
//! specification.
//!
//! Every frame is `u32 BE payload length | u8 kind | payload`. Kind [`KIND_CONTROL`] carries a
//! MessagePack map; kind [`KIND_ARROW`] carries one self-contained Arrow IPC stream. A control
//! message whose `frames` field is `n` is immediately followed by `n` data frames.
//!
//! Rich scalars travel as MessagePack extension types ([`ext`]). This module also converts wire
//! values to and from the core (`parameters`) and kernel (cell) value types, and frames to and
//! from Arrow IPC streams.

use std::sync::Arc;

use chrono::NaiveDate;
use indexmap::IndexMap;
use meerschaum_connector::DateBound;
use meerschaum_core::{Map, Value as CValue};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue, parse_datetime_value};
use meerschaum_kernel::{Decimal, Frame, Geometry};
use rmpv::Value as Wire;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{WorkerError, WorkerResult};

/// The protocol version. The worker announces its version in `hello`; the host refuses a
/// worker speaking another version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Frame kind: a MessagePack control message.
pub const KIND_CONTROL: u8 = 1;
/// Frame kind: one Arrow IPC stream.
pub const KIND_ARROW: u8 = 2;

/// The largest frame payload (the length prefix is a `u32`).
pub const MAX_FRAME_BYTES: usize = u32::MAX as usize;

/// MessagePack extension type codes.
pub mod ext {
    /// `datetime`: `isoformat()` as UTF-8 (aware or naive).
    pub const DATETIME: i8 = 1;
    /// `date`: `isoformat()` as UTF-8.
    pub const DATE: i8 = 2;
    /// `Decimal`: `str()` as UTF-8.
    pub const DECIMAL: i8 = 3;
    /// `UUID`: the 16 raw bytes.
    pub const UUID: i8 = 4;
    /// A geometry as WKB.
    pub const GEOMETRY: i8 = 5;
    /// `timedelta`: total seconds as a UTF-8 float string.
    pub const TIMEDELTA: i8 = 6;
}

/// A control message: a MessagePack map with string keys.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Msg(pub Vec<(Wire, Wire)>);

impl Msg {
    /// An empty message.
    pub fn new() -> Msg {
        Msg(Vec::new())
    }

    /// Set `key` (replacing an existing entry).
    pub fn set(mut self, key: &str, value: impl Into<Wire>) -> Msg {
        self.insert(key, value);
        self
    }

    /// Set `key` in place.
    pub fn insert(&mut self, key: &str, value: impl Into<Wire>) {
        let value = value.into();
        if let Some(slot) = self.0.iter_mut().find(|(k, _)| k.as_str() == Some(key)) {
            slot.1 = value;
        } else {
            self.0.push((Wire::from(key), value));
        }
    }

    /// The value for `key`.
    pub fn get(&self, key: &str) -> Option<&Wire> {
        self.0
            .iter()
            .find(|(k, _)| k.as_str() == Some(key))
            .map(|(_, v)| v)
    }

    /// The string value for `key`.
    pub fn str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(Wire::as_str)
    }

    /// The integer value for `key`.
    pub fn u64(&self, key: &str) -> Option<u64> {
        self.get(key).and_then(Wire::as_u64)
    }

    /// Take the value for `key` out of the message.
    pub fn take(&mut self, key: &str) -> Option<Wire> {
        let ix = self.0.iter().position(|(k, _)| k.as_str() == Some(key))?;
        Some(self.0.remove(ix).1)
    }

    /// Build from a wire map.
    pub fn from_wire(value: Wire) -> WorkerResult<Msg> {
        match value {
            Wire::Map(m) => Ok(Msg(m)),
            other => Err(WorkerError::Protocol(format!(
                "a control message must be a map, got {other}"
            ))),
        }
    }
}

impl From<Msg> for Wire {
    fn from(m: Msg) -> Wire {
        Wire::Map(m.0)
    }
}

/// A control message plus its data frames.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Envelope {
    /// The control message.
    pub msg: Msg,
    /// The Arrow IPC streams that followed it.
    pub frames: Vec<Vec<u8>>,
}

async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 5];
    r.read_exact(&mut header).await?;
    let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    Ok((header[4], payload))
}

/// Read one control message and its data frames.
pub async fn read_envelope<R: AsyncRead + Unpin>(r: &mut R) -> WorkerResult<Envelope> {
    let (kind, payload) = read_frame(r).await?;
    if kind != KIND_CONTROL {
        return Err(WorkerError::Protocol(format!(
            "expected a control frame, got kind {kind}"
        )));
    }
    let value = rmpv::decode::read_value(&mut payload.as_slice())
        .map_err(|e| WorkerError::Protocol(format!("invalid MessagePack: {e}")))?;
    let msg = Msg::from_wire(value)?;
    let n = msg.u64("frames").unwrap_or(0) as usize;
    let mut frames = Vec::with_capacity(n);
    for _ in 0..n {
        let (kind, data) = read_frame(r).await?;
        if kind != KIND_ARROW {
            return Err(WorkerError::Protocol(format!(
                "expected a data frame, got kind {kind}"
            )));
        }
        frames.push(data);
    }
    Ok(Envelope { msg, frames })
}

/// Encode a control message (setting its `frames` count) and its data frames into one buffer.
pub fn encode_envelope(mut msg: Msg, frames: &[Vec<u8>]) -> WorkerResult<Vec<u8>> {
    msg.insert("frames", frames.len() as u64);
    let mut payload = Vec::new();
    rmpv::encode::write_value(&mut payload, &Wire::from(msg))
        .map_err(|e| WorkerError::Protocol(format!("cannot encode message: {e}")))?;
    let total = 5 + payload.len() + frames.iter().map(|f| 5 + f.len()).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    push_frame(&mut out, KIND_CONTROL, &payload)?;
    for frame in frames {
        push_frame(&mut out, KIND_ARROW, frame)?;
    }
    Ok(out)
}

fn push_frame(out: &mut Vec<u8>, kind: u8, payload: &[u8]) -> WorkerResult<()> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(WorkerError::Protocol(
            "a frame exceeds 4 GiB; sync it in smaller chunks".into(),
        ));
    }
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
    Ok(())
}

/// Write a control message and its data frames.
pub async fn write_envelope<W: AsyncWrite + Unpin>(
    w: &mut W,
    msg: Msg,
    frames: &[Vec<u8>],
) -> WorkerResult<()> {
    let buf = encode_envelope(msg, frames)?;
    w.write_all(&buf).await?;
    w.flush().await?;
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Value conversions
// ------------------------------------------------------------------------------------------------

/// A core (parameters) value on the wire.
pub fn core_to_wire(v: &CValue) -> Wire {
    match v {
        CValue::Null => Wire::Nil,
        CValue::Bool(b) => Wire::Boolean(*b),
        CValue::Int(i) => Wire::from(*i),
        CValue::Float(f) => Wire::F64(*f),
        CValue::Str(s) => Wire::from(s.as_str()),
        CValue::List(items) => Wire::Array(items.iter().map(core_to_wire).collect()),
        CValue::Map(m) => map_to_wire(m),
    }
}

/// A core map on the wire.
pub fn map_to_wire(m: &Map) -> Wire {
    Wire::Map(
        m.iter()
            .map(|(k, v)| (Wire::from(k.as_str()), core_to_wire(v)))
            .collect(),
    )
}

fn ext_str(data: &[u8]) -> String {
    String::from_utf8_lossy(data).into_owned()
}

/// A wire value as a core value. Rich scalars become their string forms (timestamps
/// `isoformat()`, decimals `str()`, UUIDs hyphenated, geometries WKB hex); `timedelta` becomes
/// float seconds and binary data a hex string.
pub fn wire_to_core(v: &Wire) -> CValue {
    match v {
        Wire::Nil => CValue::Null,
        Wire::Boolean(b) => CValue::Bool(*b),
        Wire::Integer(i) => match i.as_i64() {
            Some(i) => CValue::Int(i),
            None => CValue::Float(i.as_f64().unwrap_or(f64::NAN)),
        },
        Wire::F32(f) => CValue::Float(*f as f64),
        Wire::F64(f) => CValue::Float(*f),
        Wire::String(s) => CValue::Str(
            s.as_str()
                .map(String::from)
                .unwrap_or_else(|| String::from_utf8_lossy(s.as_bytes()).into_owned()),
        ),
        Wire::Binary(b) => CValue::Str(hex::encode(b)),
        Wire::Array(items) => CValue::List(items.iter().map(wire_to_core).collect()),
        Wire::Map(m) => CValue::Map(wire_map_to_core(m)),
        Wire::Ext(code, data) => match *code {
            ext::UUID if data.len() == 16 => CValue::Str(
                uuid::Uuid::from_slice(data)
                    .map(|u| u.to_string())
                    .unwrap_or_default(),
            ),
            ext::GEOMETRY => CValue::Str(hex::encode_upper(data)),
            ext::TIMEDELTA => ext_str(data)
                .parse::<f64>()
                .map(CValue::Float)
                .unwrap_or(CValue::Null),
            _ => CValue::Str(ext_str(data)),
        },
    }
}

/// A wire map as a core map (non-string keys are stringified).
pub fn wire_map_to_core(m: &[(Wire, Wire)]) -> Map {
    m.iter()
        .map(|(k, v)| {
            let key = match k.as_str() {
                Some(s) => s.to_string(),
                None => meerschaum_core::pyfmt::py_str(&wire_to_core(k)),
            };
            (key, wire_to_core(v))
        })
        .collect()
}

/// A kernel (cell) value on the wire.
pub fn kernel_to_wire(v: &KValue) -> Wire {
    match v {
        KValue::Null | KValue::NA | KValue::NaT => Wire::Nil,
        KValue::Bool(b) => Wire::Boolean(*b),
        KValue::Int(i) => Wire::from(*i),
        KValue::Float(f) => Wire::F64(*f),
        KValue::Str(s) => Wire::from(s.as_str()),
        KValue::Bytes(b) => Wire::Binary(b.clone()),
        KValue::Decimal(d) => Wire::Ext(ext::DECIMAL, d.to_sci_string().into_bytes()),
        KValue::Uuid(u) => Wire::Ext(ext::UUID, u.as_bytes().to_vec()),
        KValue::Datetime(dt) => Wire::Ext(ext::DATETIME, dt.isoformat().into_bytes()),
        KValue::Date(d) => Wire::Ext(ext::DATE, d.format("%Y-%m-%d").to_string().into_bytes()),
        KValue::Json(j) => json_to_wire(j),
        KValue::Geometry(g) => Wire::Ext(ext::GEOMETRY, g.to_wkb(false)),
    }
}

fn json_to_wire(j: &serde_json::Value) -> Wire {
    match j {
        serde_json::Value::Null => Wire::Nil,
        serde_json::Value::Bool(b) => Wire::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Wire::from(i)
            } else if let Some(u) = n.as_u64() {
                Wire::from(u)
            } else {
                Wire::F64(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        serde_json::Value::String(s) => Wire::from(s.as_str()),
        serde_json::Value::Array(items) => Wire::Array(items.iter().map(json_to_wire).collect()),
        serde_json::Value::Object(m) => Wire::Map(
            m.iter()
                .map(|(k, v)| (Wire::from(k.as_str()), json_to_wire(v)))
                .collect(),
        ),
    }
}

/// A wire value as JSON (rich scalars become their string forms, as `json_serialize_value`
/// does).
pub fn wire_to_json(v: &Wire) -> serde_json::Value {
    match v {
        Wire::Nil => serde_json::Value::Null,
        Wire::Boolean(b) => serde_json::Value::Bool(*b),
        Wire::Integer(i) => match (i.as_i64(), i.as_u64()) {
            (Some(i), _) => serde_json::Value::from(i),
            (None, Some(u)) => serde_json::Value::from(u),
            _ => serde_json::Value::Null,
        },
        Wire::F32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Wire::F64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Wire::Array(items) => serde_json::Value::Array(items.iter().map(wire_to_json).collect()),
        Wire::Map(m) => serde_json::Value::Object(
            m.iter()
                .map(|(k, v)| {
                    let key = k
                        .as_str()
                        .map(String::from)
                        .unwrap_or_else(|| meerschaum_core::pyfmt::py_str(&wire_to_core(k)));
                    (key, wire_to_json(v))
                })
                .collect(),
        ),
        other => match wire_to_core(other) {
            CValue::Str(s) => serde_json::Value::String(s),
            CValue::Float(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        },
    }
}

/// A wire value as a kernel (cell) value: extension types become typed scalars, binary data
/// `bytes`, maps and arrays JSON containers.
pub fn wire_to_kernel(v: &Wire) -> KValue {
    match v {
        Wire::Nil => KValue::Null,
        Wire::Boolean(b) => KValue::Bool(*b),
        Wire::Integer(i) => match i.as_i64() {
            Some(i) => KValue::Int(i),
            None => KValue::Float(i.as_f64().unwrap_or(f64::NAN)),
        },
        Wire::F32(f) => KValue::Float(*f as f64),
        Wire::F64(f) => KValue::Float(*f),
        Wire::String(s) => KValue::Str(
            s.as_str()
                .map(String::from)
                .unwrap_or_else(|| String::from_utf8_lossy(s.as_bytes()).into_owned()),
        ),
        Wire::Binary(b) => KValue::Bytes(b.clone()),
        Wire::Array(_) | Wire::Map(_) => KValue::Json(wire_to_json(v)),
        Wire::Ext(code, data) => match *code {
            ext::DATETIME => parse_datetime_value(&ext_str(data))
                .map(KValue::Datetime)
                .unwrap_or_else(|| KValue::Str(ext_str(data))),
            ext::DATE => NaiveDate::parse_from_str(&ext_str(data), "%Y-%m-%d")
                .map(KValue::Date)
                .unwrap_or_else(|_| KValue::Str(ext_str(data))),
            ext::DECIMAL => Decimal::parse(&ext_str(data))
                .map(KValue::Decimal)
                .unwrap_or_else(|_| KValue::Str(ext_str(data))),
            ext::UUID => uuid::Uuid::from_slice(data)
                .map(KValue::Uuid)
                .unwrap_or_else(|_| KValue::Bytes(data.clone())),
            ext::GEOMETRY => Geometry::from_wkb(data)
                .map(KValue::Geometry)
                .unwrap_or_else(|_| KValue::Bytes(data.clone())),
            ext::TIMEDELTA => ext_str(data)
                .parse::<f64>()
                .map(KValue::Float)
                .unwrap_or(KValue::Null),
            _ => KValue::Bytes(data.clone()),
        },
    }
}

/// A datetime-axis bound on the wire (`datetime` extension or integer).
pub fn bound_to_wire(b: Option<&DateBound>) -> Wire {
    match b {
        None => Wire::Nil,
        Some(DateBound::Int(i)) => Wire::from(*i),
        Some(DateBound::Datetime(dt)) => Wire::Ext(ext::DATETIME, dt.isoformat().into_bytes()),
    }
}

/// A datetime-axis bound from the wire (`None` for nil or any other type).
pub fn wire_to_bound(v: &Wire) -> Option<DateBound> {
    match wire_to_kernel(v) {
        KValue::Datetime(dt) => Some(DateBound::Datetime(dt)),
        KValue::Int(i) => Some(DateBound::Int(i)),
        KValue::Date(d) => Some(DateBound::Datetime(DateTimeValue::naive(
            d.and_hms_opt(0, 0, 0)?,
        ))),
        _ => None,
    }
}

/// A list of documents from the wire.
pub fn wire_to_docs(v: &Wire) -> WorkerResult<Vec<IndexMap<String, KValue>>> {
    let Wire::Array(items) = v else {
        return Err(WorkerError::Protocol("documents must be a list".into()));
    };
    items
        .iter()
        .map(|item| match item {
            Wire::Map(m) => Ok(m
                .iter()
                .map(|(k, v)| {
                    let key = k
                        .as_str()
                        .map(String::from)
                        .unwrap_or_else(|| meerschaum_core::pyfmt::py_str(&wire_to_core(k)));
                    (key, wire_to_kernel(v))
                })
                .collect()),
            other => Err(WorkerError::Protocol(format!(
                "each document must be a map, got {other}"
            ))),
        })
        .collect()
}

/// A dict of lists from the wire.
pub fn wire_to_columns(v: &Wire) -> WorkerResult<IndexMap<String, Vec<KValue>>> {
    let Wire::Map(m) = v else {
        return Err(WorkerError::Protocol("columns must be a map".into()));
    };
    Ok(m.iter()
        .map(|(k, v)| {
            let key = k
                .as_str()
                .map(String::from)
                .unwrap_or_else(|| meerschaum_core::pyfmt::py_str(&wire_to_core(k)));
            let values = match v {
                Wire::Array(items) => items.iter().map(wire_to_kernel).collect(),
                other => vec![wire_to_kernel(other)],
            };
            (key, values)
        })
        .collect())
}

/// A list of documents on the wire.
pub fn docs_to_wire(docs: &[IndexMap<String, KValue>]) -> Wire {
    Wire::Array(
        docs.iter()
            .map(|d| {
                Wire::Map(
                    d.iter()
                        .map(|(k, v)| (Wire::from(k.as_str()), kernel_to_wire(v)))
                        .collect(),
                )
            })
            .collect(),
    )
}

// ------------------------------------------------------------------------------------------------
// Arrow IPC
// ------------------------------------------------------------------------------------------------

/// Serialize a frame as one Arrow IPC stream (with the kernel's GeoArrow / JSON / UUID field
/// tags, so Python restores the special columns).
pub fn frame_to_ipc(df: &Frame) -> WorkerResult<Vec<u8>> {
    let batch = df
        .to_record_batch()
        .map_err(|e| WorkerError::Protocol(format!("cannot convert the frame to Arrow: {e}")))?;
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| WorkerError::Protocol(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| WorkerError::Protocol(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| WorkerError::Protocol(e.to_string()))?;
    }
    Ok(buf)
}

/// Read one Arrow IPC stream into a frame (an empty stream keeps its columns).
pub fn ipc_to_frame(data: &[u8]) -> WorkerResult<Frame> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(data), None)
        .map_err(|e| WorkerError::Protocol(format!("invalid Arrow IPC stream: {e}")))?;
    let schema = reader.schema();
    let mut batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| WorkerError::Protocol(format!("invalid Arrow IPC stream: {e}")))?;
    batches.retain(|b| b.num_rows() > 0);
    if batches.is_empty() {
        batches.push(arrow::record_batch::RecordBatch::new_empty(Arc::clone(
            &schema,
        )));
    }
    Frame::from_record_batches(&batches)
        .map_err(|e| WorkerError::Protocol(format!("cannot read the Arrow data: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_roundtrip() {
        let msg = Msg::new()
            .set("t", "call")
            .set("id", 7u64)
            .set("op", "ping");
        let frames = vec![vec![1u8, 2, 3]];
        let buf = encode_envelope(msg, &frames).unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let env = rt
            .block_on(read_envelope(&mut buf.as_slice()))
            .expect("decodes");
        assert_eq!(env.msg.str("op"), Some("ping"));
        assert_eq!(env.msg.u64("frames"), Some(1));
        assert_eq!(env.frames, frames);
    }

    #[test]
    fn rich_scalars_roundtrip() {
        let dt = parse_datetime_value("2024-01-02T03:04:05+00:00").unwrap();
        let values = vec![
            KValue::Datetime(dt),
            KValue::Decimal(Decimal::parse("1.10").unwrap()),
            KValue::Uuid(uuid::Uuid::nil()),
            KValue::Date(NaiveDate::from_ymd_opt(2024, 1, 2).unwrap()),
            KValue::Bytes(vec![0, 1]),
            KValue::Int(-3),
            KValue::Str("x".into()),
        ];
        for v in values {
            assert_eq!(wire_to_kernel(&kernel_to_wire(&v)), v, "{v:?}");
        }
    }

    #[test]
    fn bounds_roundtrip() {
        let b = DateBound::parse("2024-01-01T00:00:00+00:00").unwrap();
        assert_eq!(wire_to_bound(&bound_to_wire(Some(&b))), Some(b));
        assert_eq!(
            wire_to_bound(&bound_to_wire(Some(&DateBound::Int(5)))),
            Some(DateBound::Int(5))
        );
        assert_eq!(wire_to_bound(&Wire::Nil), None);
    }

    #[test]
    fn core_values_roundtrip() {
        let v =
            CValue::from_json_str(r#"{"a": [1, 2.5, "x", null, true], "b": {"c": {}}}"#).unwrap();
        assert_eq!(wire_to_core(&core_to_wire(&v)), v);
    }
}
