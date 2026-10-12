//! Serialization helpers from `meerschaum/utils/dtypes/__init__.py`: bytes (base64 / hex),
//! datetimes, dates, decimals, JSON values and geometry (`serialize_geometry`).

use base64::Engine as _;

use crate::cast::value_is_null;
use crate::decimal::Decimal;
use crate::geometry::Geometry;
use crate::value::{DateTimeValue, Value};

/// Errors from (de)serialization.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SerializeError {
    #[error("Error: {0}")]
    Binascii(String),
    #[error("TypeError: {0}")]
    Type(String),
    #[error("ValueError: {0}")]
    Value(String),
}

/// `serialize_bytes()`: base64; null-like non-bytes values pass through (`Ok(None)` → return the
/// input unchanged).
pub fn serialize_bytes(value: &Value) -> Result<Option<String>, SerializeError> {
    match value {
        Value::Bytes(b) => Ok(Some(base64::engine::general_purpose::STANDARD.encode(b))),
        v if value_is_null(v) => Ok(None),
        v => Err(SerializeError::Type(format!(
            "a bytes-like object is required, not '{}'",
            v.type_name()
        ))),
    }
}

/// `encode_bytes_for_bytea()`: `\x`-prefixed hex for PostgreSQL `BYTEA`.
pub fn encode_bytes_for_bytea(
    value: &Value,
    with_prefix: bool,
) -> Result<Option<String>, SerializeError> {
    match value {
        Value::Bytes(b) => Ok(Some(format!(
            "{}{}",
            if with_prefix { "\\x" } else { "" },
            hex::encode(b)
        ))),
        v if value_is_null(v) => Ok(None),
        v => Err(SerializeError::Type(format!(
            "argument should be bytes, not '{}'",
            v.type_name()
        ))),
    }
}

/// `deserialize_bytes_string()` for a string: hex when prefixed with `\x` (or `force_hex`),
/// otherwise base64 decoded leniently like `base64.b64decode` (non-alphabet characters dropped).
pub fn deserialize_bytes_string(data: &str, force_hex: bool) -> Result<Vec<u8>, SerializeError> {
    let is_hex = force_hex || data.starts_with("\\x");
    if is_hex {
        let h = data.strip_prefix("\\x").unwrap_or(data);
        if h.len() % 2 != 0 && h.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(SerializeError::Binascii("Odd-length string".into()));
        }
        if !h.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(SerializeError::Binascii(
                "Non-hexadecimal digit found".into(),
            ));
        }
        return hex::decode(h).map_err(|e| SerializeError::Binascii(e.to_string()));
    }
    b64decode_lenient(data)
}

/// `base64.b64decode(s)` (non-validating): characters outside the alphabet are discarded and
/// padding is checked per Python's `binascii.a2b_base64`.
pub fn b64decode_lenient(data: &str) -> Result<Vec<u8>, SerializeError> {
    let filtered: String = data
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '+' || *c == '/' || *c == '=')
        .collect();
    let (body, pad) = match filtered.find('=') {
        Some(ix) => (&filtered[..ix], &filtered[ix..]),
        None => (filtered.as_str(), ""),
    };
    let n = body.len();
    match n % 4 {
        1 => {
            return Err(SerializeError::Binascii(format!(
                "Invalid base64-encoded string: number of data characters ({n}) cannot be 1 more than a multiple of 4"
            )));
        }
        2 if pad.len() < 2 => return Err(SerializeError::Binascii("Incorrect padding".into())),
        3 if pad.is_empty() => return Err(SerializeError::Binascii("Incorrect padding".into())),
        _ => {}
    }
    base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(body)
        .or_else(|_| {
            base64::engine::GeneralPurpose::new(
                &base64::alphabet::STANDARD,
                base64::engine::GeneralPurposeConfig::new()
                    .with_decode_allow_trailing_bits(true)
                    .with_decode_padding_mode(base64::engine::DecodePaddingMode::RequireNone),
            )
            .decode(body)
        })
        .map_err(|e| SerializeError::Binascii(e.to_string()))
}

/// `serialize_datetime()`: `isoformat()` plus `Z` for naive datetimes.
pub fn serialize_datetime(dt: &DateTimeValue) -> String {
    let mut s = dt.isoformat();
    if dt.tz.is_none() {
        s.push('Z');
    }
    s
}

/// `serialize_date()`.
pub fn serialize_date(d: &chrono::NaiveDate) -> String {
    d.format("%Y-%m-%d").to_string()
}

/// `serialize_decimal()`: `f"{x:f}"`, `None` for `NaN`, optionally quantized first.
pub fn serialize_decimal(
    x: &Decimal,
    quantize: bool,
    precision: Option<i64>,
    scale: Option<i64>,
) -> Result<Option<String>, SerializeError> {
    if x.is_nan() {
        return Ok(None);
    }
    let x = if quantize && scale.unwrap_or(0) != 0 && precision.unwrap_or(0) != 0 {
        x.quantize(precision.unwrap_or(0), scale.unwrap_or(0))
            .map_err(|e| SerializeError::Value(e.to_string()))?
    } else {
        x.clone()
    };
    Ok(Some(x.to_plain_string()))
}

/// `json_serialize_value(x, default_to_str=True)` as a JSON value (`None` → `null`).
pub fn json_serialize_value(x: &Value) -> serde_json::Value {
    use serde_json::Value as J;
    match x {
        Value::Datetime(dt) => J::String(serialize_datetime(dt)),
        Value::Date(d) => J::String(serialize_date(d)),
        Value::Bytes(b) => J::String(base64::engine::general_purpose::STANDARD.encode(b)),
        Value::Decimal(d) => match serialize_decimal(d, false, None, None) {
            Ok(Some(s)) => J::String(s),
            _ => J::Null,
        },
        Value::Geometry(g) => J::String(g.to_wkb_hex(true)),
        v if value_is_null(v) => J::Null,
        Value::Json(j) => J::String(compact_json(j)),
        other => J::String(other.py_str()),
    }
}

/// `json.dumps(x, separators=(',', ':'))` (insertion order kept, non-ASCII escaped like Python).
pub fn compact_json(j: &serde_json::Value) -> String {
    let s = serde_json::to_string(j).unwrap_or_default();
    ensure_ascii(&s)
}

/// `json.dumps(x, sort_keys=True, separators=(',', ':'), default=str)`.
pub fn compact_json_sorted(j: &serde_json::Value) -> String {
    fn sort(j: &serde_json::Value) -> serde_json::Value {
        match j {
            serde_json::Value::Object(o) => {
                let mut keys: Vec<&String> = o.keys().collect();
                keys.sort();
                let mut m = serde_json::Map::new();
                for k in keys {
                    m.insert(k.clone(), sort(&o[k]));
                }
                serde_json::Value::Object(m)
            }
            serde_json::Value::Array(a) => serde_json::Value::Array(a.iter().map(sort).collect()),
            other => other.clone(),
        }
    }
    compact_json(&sort(j))
}

/// Python's `json.dumps` escapes non-ASCII characters by default (`ensure_ascii=True`).
fn ensure_ascii(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii() {
            out.push(c);
        } else {
            let mut buf = [0u16; 2];
            for unit in c.encode_utf16(&mut buf) {
                out.push_str(&format!("\\u{unit:04x}"));
            }
        }
    }
    out
}

/// `json.dumps(value, default=json_serialize_value, separators=(',', ':'), allow_nan=False)`
/// for a single value (as `to_polars` serializes JSON columns).
pub fn json_dumps_value(value: &Value) -> String {
    match value {
        Value::Json(j) => compact_json(j),
        Value::Str(s) => compact_json(&serde_json::Value::String(s.clone())),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => crate::value::py_float_repr(*f),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        v if v.is_na() => "null".to_string(),
        other => compact_json(&json_serialize_value(other)),
    }
}

/// The output of [`serialize_geometry`].
#[derive(Clone, Debug, PartialEq)]
pub enum GeometryOutput {
    Text(String),
    Bytes(Vec<u8>),
    GeoJson(serde_json::Value),
}

/// Geometry serialization formats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GeometryFormat {
    WkbHex,
    Wkb,
    Wkt,
    GeoJson,
    GpkgWkb,
}

impl GeometryFormat {
    pub fn parse(s: &str) -> Self {
        match s {
            "wkb_hex" => Self::WkbHex,
            "geojson" => Self::GeoJson,
            "gpkg_wkb" => Self::GpkgWkb,
            // PARITY: any other `wkb*` format is plain WKB bytes; anything else is WKT.
            s if s.starts_with("wkb") => Self::Wkb,
            _ => Self::Wkt,
        }
    }
}

/// `serialize_geometry()`.
///
/// `geojson` with an `srid` reprojects to EPSG:4326 through pyproj in v4; without PROJ the
/// kernel supports only the identity case (`srid` 4326) and returns `Err` otherwise.
pub fn serialize_geometry(
    geom: &Value,
    format: GeometryFormat,
    srid: Option<i64>,
) -> Result<Option<GeometryOutput>, SerializeError> {
    if value_is_null(geom) {
        return Ok(None);
    }
    let g: &Geometry = match geom {
        Value::Geometry(g) => g,
        other => {
            if format == GeometryFormat::GeoJson {
                return Err(SerializeError::Type(format!(
                    "One of the arguments is of incorrect type. Please provide only Geometry objects. ({})",
                    other.type_name()
                )));
            }
            return Ok(Some(GeometryOutput::Text(other.py_str())));
        }
    };
    Ok(Some(match format {
        GeometryFormat::GeoJson => {
            if let Some(s) = srid.filter(|s| *s != 0 && *s != 4326) {
                return Err(SerializeError::Value(format!(
                    "Reprojection from EPSG:{s} to EPSG:4326 requires PROJ (not available in the kernel)."
                )));
            }
            GeometryOutput::GeoJson(g.to_geojson())
        }
        GeometryFormat::WkbHex => GeometryOutput::Text(g.to_wkb_hex(true)),
        GeometryFormat::Wkb => GeometryOutput::Bytes(g.to_wkb(true)),
        GeometryFormat::GpkgWkb => GeometryOutput::Bytes(g.to_gpkg_wkb(srid.map(|s| s as i32))),
        GeometryFormat::Wkt => GeometryOutput::Text(g.to_wkt()),
    }))
}

/// `deserialize_geometry()`: WKB bytes or hex into a geometry.
pub fn deserialize_geometry(data: &Value) -> Result<Geometry, SerializeError> {
    match data {
        Value::Bytes(b) => Geometry::from_wkb(b).map_err(|e| SerializeError::Value(e.to_string())),
        Value::Str(s) => {
            Geometry::from_wkb_hex(s).map_err(|e| SerializeError::Value(e.to_string()))
        }
        other => Err(SerializeError::Type(format!(
            "Expected bytes or string, got {}",
            other.type_name()
        ))),
    }
}
