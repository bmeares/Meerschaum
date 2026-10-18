//! Shared helpers for the parity tests: load the JSON fixtures produced by
//! `tests/fixtures/gen_*.py`, decode tagged values and frames, and compare kernel output
//! against v4 output.
#![allow(dead_code)]

use std::path::PathBuf;

use base64::Engine as _;
use meerschaum_kernel::Value;
use meerschaum_kernel::decimal::Decimal;
use meerschaum_kernel::dtypes::{Srid, polars_dtype_for_pandas};
use meerschaum_kernel::frame::{Column, ColumnData, Frame, json_to_value, values_to_series};
use meerschaum_kernel::geometry::Geometry;
use meerschaum_kernel::value::{DateTimeValue, TzInfo, parse_iso_datetime};
use serde_json::Value as J;

pub fn load_fixture(name: &str) -> J {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(format!("{name}.json"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&text).expect("valid fixture JSON")
}

pub fn cases(fixture: &J) -> &Vec<J> {
    fixture["cases"].as_array().expect("cases")
}

/// Decode a tagged fixture value into a [`Value`].
pub fn decode(j: &J) -> Value {
    match j {
        J::Null => Value::Null,
        J::Bool(b) => Value::Bool(*b),
        J::String(s) => Value::Str(s.clone()),
        J::Object(o) => {
            let t = o.get("t").and_then(|t| t.as_str()).unwrap_or("");
            let v = o.get("v");
            match t {
                "int" => match v {
                    Some(J::Number(n)) => Value::Int(n.as_i64().expect("i64")),
                    Some(J::String(s)) => Value::Str(s.clone()),
                    _ => Value::Null,
                },
                "float" => match v {
                    Some(J::Number(n)) => Value::Float(n.as_f64().unwrap()),
                    Some(J::String(s)) => Value::Float(match s.as_str() {
                        "nan" => f64::NAN,
                        "inf" => f64::INFINITY,
                        _ => f64::NEG_INFINITY,
                    }),
                    _ => Value::Null,
                },
                "na" => Value::NA,
                "nat" => Value::NaT,
                "dec" => Value::Decimal(Decimal::parse(v.unwrap().as_str().unwrap()).unwrap()),
                "uuid" => Value::Uuid(uuid::Uuid::parse_str(v.unwrap().as_str().unwrap()).unwrap()),
                "bytes" => Value::Bytes(
                    base64::engine::general_purpose::STANDARD
                        .decode(v.unwrap().as_str().unwrap())
                        .unwrap(),
                ),
                "bytes_hex" => Value::Bytes(hex::decode(v.unwrap().as_str().unwrap()).unwrap()),
                "dt" => Value::Datetime(decode_dt(
                    v.unwrap().as_str().unwrap(),
                    o.get("tz").and_then(|t| t.as_str()),
                )),
                "date" => Value::Date(
                    chrono::NaiveDate::parse_from_str(v.unwrap().as_str().unwrap(), "%Y-%m-%d")
                        .unwrap(),
                ),
                "json" => json_to_value(v.unwrap().clone()),
                "geom" => {
                    Value::Geometry(Geometry::from_wkb_hex(o["wkb"].as_str().unwrap()).unwrap())
                }
                other => panic!("unknown tag {other}"),
            }
        }
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap())
            }
        }
        J::Array(_) => panic!("unexpected array"),
    }
}

pub fn decode_dt(iso: &str, tz: Option<&str>) -> DateTimeValue {
    let (naive, offset) = parse_iso_datetime(iso).unwrap_or_else(|| panic!("bad iso {iso}"));
    match tz {
        None => DateTimeValue::naive(naive),
        Some(name) => {
            let off = offset.map(|o| o.local_minus_utc()).unwrap_or(0);
            let utc = naive - chrono::Duration::seconds(off as i64);
            let tz = TzInfo::parse(name)
                .unwrap_or_else(|| TzInfo::Fixed(chrono::FixedOffset::east_opt(off).unwrap()));
            DateTimeValue { utc, tz: Some(tz) }
        }
    }
}

/// Encode a [`Value`] into the tagged fixture form.
pub fn encode(v: &Value) -> J {
    use serde_json::json;
    match v {
        Value::Null => J::Null,
        Value::NA => json!({"t": "na"}),
        Value::NaT => json!({"t": "nat"}),
        Value::Bool(b) => J::Bool(*b),
        Value::Int(i) => json!({"t": "int", "v": i}),
        Value::Float(f) => {
            if f.is_nan() {
                json!({"t": "float", "v": "nan"})
            } else if f.is_infinite() {
                json!({"t": "float", "v": if *f > 0.0 { "inf" } else { "-inf" }})
            } else {
                json!({"t": "float", "v": f})
            }
        }
        Value::Str(s) => J::String(s.clone()),
        Value::Bytes(b) => {
            json!({"t": "bytes", "v": base64::engine::general_purpose::STANDARD.encode(b)})
        }
        Value::Decimal(d) => json!({"t": "dec", "v": d.to_sci_string()}),
        Value::Uuid(u) => json!({"t": "uuid", "v": u.hyphenated().to_string()}),
        Value::Datetime(d) => {
            json!({"t": "dt", "v": d.isoformat(), "tz": d.tz.as_ref().map(|t| t.name())})
        }
        Value::Date(d) => json!({"t": "date", "v": d.format("%Y-%m-%d").to_string()}),
        Value::Json(j) => json!({"t": "json", "v": j}),
        Value::Geometry(g) => {
            json!({"t": "geom", "wkt": g.to_wkt_full(), "wkb": g.to_wkb_hex(false)})
        }
    }
}

/// Whether a tagged value is null-like (None / NA / NaT / NaN / Decimal NaN).
pub fn is_null_tag(j: &J) -> bool {
    match j {
        J::Null => true,
        J::Object(o) => match o.get("t").and_then(|t| t.as_str()) {
            Some("na") | Some("nat") => true,
            Some("float") => o.get("v") == Some(&J::String("nan".into())),
            Some("dec") => o.get("v") == Some(&J::String("NaN".into())),
            _ => false,
        },
        _ => false,
    }
}

/// Compare two tagged values with null normalization and semantic datetime / geometry / number
/// comparison. Returns a description of the mismatch, if any.
pub fn compare_values(expected: &J, actual: &J) -> Result<(), String> {
    if is_null_tag(expected) || is_null_tag(actual) {
        if is_null_tag(expected) && is_null_tag(actual) {
            return Ok(());
        }
        return Err(format!("expected {expected}, got {actual}"));
    }
    let e = decode(expected);
    let a = decode(actual);
    let ok = match (&e, &a) {
        (Value::Datetime(x), Value::Datetime(y)) => {
            x.utc == y.utc && x.tz.is_some() == y.tz.is_some()
        }
        (Value::Geometry(x), Value::Geometry(y)) => x.to_wkb(false) == y.to_wkb(false),
        (Value::Float(x), Value::Float(y)) => x == y || (x.is_nan() && y.is_nan()),
        (Value::Int(x), Value::Float(y)) | (Value::Float(y), Value::Int(x)) => (*x as f64) == *y,
        (Value::Decimal(x), Value::Decimal(y)) => x.to_sci_string() == y.to_sci_string(),
        (Value::Json(x), Value::Json(y)) => x == y,
        _ => expected == actual,
    };
    if ok {
        Ok(())
    } else {
        Err(format!("expected {expected}, got {actual}"))
    }
}

/// Logical dtype class used to compare pandas dtypes (v4) with kernel dtypes.
///
/// Polars does not distinguish NumPy / nullable / Arrow-backed pandas columns, so dtypes are
/// compared by logical class (`int64`, `float64`, `bool`, `string`, `datetime[us, UTC]`,
/// `numeric`, `json`, `uuid`, `bytes`, `date`, `geometry`, `object`, `null`).
pub fn logical_dtype(dtype: &str, values: &[J]) -> String {
    let d = dtype.trim();
    let base = d.strip_suffix("[pyarrow]").unwrap_or(d);
    let lower = base.to_lowercase();
    if lower.starts_with("datetime64") || lower.starts_with("timestamp") {
        let inner = base
            .split_once('[')
            .map(|(_, r)| r.trim_end_matches(']'))
            .unwrap_or("");
        let mut parts = inner.splitn(2, ',');
        let unit = parts.next().unwrap_or("").trim().to_string();
        let tz = parts
            .next()
            .map(|t| t.trim().trim_start_matches("tz=").to_string());
        return match tz {
            Some(tz) => format!("datetime[{unit}, {tz}]"),
            None => format!("datetime[{unit}]"),
        };
    }
    if lower.starts_with("decimal") {
        return "numeric".into();
    }
    match lower.as_str() {
        "int8" | "int16" | "int32" | "int64" | "uint8" | "uint16" | "uint32" | "uint64" => {
            return lower;
        }
        "float64" | "double" | "float32" | "float" => {
            return if lower == "float32" || (lower == "float" && d.ends_with("[pyarrow]")) {
                "float32".into()
            } else {
                "float64".into()
            };
        }
        "bool" | "boolean" => return "bool".into(),
        "str" | "string" | "large_string" | "string_view" => return "string".into(),
        "binary" | "large_binary" => return "bytes".into(),
        "date32[day]" | "date64[ms]" => return "date".into(),
        "geometry" => return "geometry".into(),
        _ => {}
    }
    if lower.starts_with("date32") || lower.starts_with("date64") {
        return "date".into();
    }
    // object: infer from values.
    let non_null: Vec<&J> = values.iter().filter(|v| !is_null_tag(v)).collect();
    if non_null.is_empty() {
        return "null".into();
    }
    let tag = |v: &J| -> String {
        match v {
            J::String(_) => "string".into(),
            J::Bool(_) => "bool".into(),
            J::Object(o) => match o.get("t").and_then(|t| t.as_str()).unwrap_or("") {
                "dec" => "numeric".into(),
                "uuid" => "uuid".into(),
                "json" => "json".into(),
                "bytes" => "bytes".into(),
                "date" => "date".into(),
                "geom" => "geometry".into(),
                "dt" => "datetime".into(),
                "int" => "int".into(),
                "float" => "float".into(),
                t => t.to_string(),
            },
            _ => "other".into(),
        }
    };
    let first = tag(non_null[0]);
    if non_null.iter().all(|v| tag(v) == first) {
        format!("object<{first}>")
    } else {
        "object<mixed>".into()
    }
}

/// Build a kernel frame from a fixture frame (`{"columns": [{name, dtype, crs, values}]}`).
pub fn decode_frame(j: &J) -> Frame {
    let cols = j["columns"].as_array().expect("columns");
    let height = j["index"].as_array().map(|a| a.len()).unwrap_or(0);
    let mut columns = Vec::new();
    for c in cols {
        let name = c["name"].as_str().unwrap().to_string();
        let dtype = c["dtype"].as_str().unwrap();
        let values: Vec<Value> = c["values"].as_array().unwrap().iter().map(decode).collect();
        let mut data = None;
        if dtype != "object"
            && dtype != "geometry"
            && let Some(pl_dtype) = polars_dtype_for_pandas(dtype)
        {
            // Floats: NaN is null in pandas float columns.
            let values: Vec<Value> = values
                .iter()
                .map(|v| if v.is_na() { Value::Null } else { v.clone() })
                .collect();
            // pandas second-precision columns map to milliseconds (Polars has no `s` unit).
            data = values_to_series(&name, &values, &pl_dtype).map(ColumnData::Native);
            assert!(
                data.is_some(),
                "could not build native column {name} ({dtype})"
            );
        }
        let data = data.unwrap_or(ColumnData::Object(values));
        let mut column = Column::new(name, data);
        if let Some(crs) = c.get("crs").and_then(|c| c.as_str()) {
            column.crs = Some(
                match crs.strip_prefix("EPSG:").and_then(|s| s.parse().ok()) {
                    Some(i) => Srid::Code(i),
                    None => Srid::Authority(crs.to_string()),
                },
            );
        }
        columns.push(column);
    }
    if columns.is_empty() {
        return Frame::with_height(height);
    }
    Frame::new(columns).expect("valid frame")
}

/// Compare a kernel frame against a fixture frame: column names and order, logical dtypes,
/// row count and values (with null normalization).
pub fn compare_frame(expected: &J, actual: &Frame, context: &str) -> Result<(), String> {
    let cols = expected["columns"].as_array().expect("columns");
    let names: Vec<String> = cols
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect();
    if names != actual.names() {
        return Err(format!(
            "{context}: columns differ: expected {names:?}, got {:?}",
            actual.names()
        ));
    }
    let expected_height = expected["index"].as_array().map(|a| a.len()).unwrap_or(0);
    if !names.is_empty() && expected_height != actual.height() {
        return Err(format!(
            "{context}: height differs: expected {expected_height}, got {}",
            actual.height()
        ));
    }
    for c in cols {
        let name = c["name"].as_str().unwrap();
        let ev = c["values"].as_array().unwrap();
        let data = &actual.column(name).unwrap().data;
        let av: Vec<J> = data.values().iter().map(encode).collect();
        let e_dtype = logical_dtype(c["dtype"].as_str().unwrap(), ev);
        let a_dtype = logical_dtype(&data.dtype_str(), &av);
        if !dtype_classes_match(&e_dtype, &a_dtype) {
            return Err(format!(
                "{context}: column '{name}' dtype differs: expected {e_dtype} ({}), got {a_dtype} ({})",
                c["dtype"],
                data.dtype_str()
            ));
        }
        for (i, (e, a)) in ev.iter().zip(av.iter()).enumerate() {
            compare_values(e, a).map_err(|m| format!("{context}: column '{name}' row {i}: {m}"))?;
        }
        if let Some(crs) = c.get("crs").and_then(|c| c.as_str()) {
            let actual_crs = actual
                .column(name)
                .unwrap()
                .crs
                .as_ref()
                .and_then(|c| c.to_crs());
            if actual_crs.as_deref() != Some(crs) {
                return Err(format!(
                    "{context}: column '{name}' crs differs: expected {crs}, got {actual_crs:?}"
                ));
            }
        }
    }
    Ok(())
}

/// Dtype classes that are equivalent once pandas backends are erased.
pub fn dtype_classes_match(expected: &str, actual: &str) -> bool {
    if expected == actual {
        return true;
    }
    let norm = |s: &str| -> String {
        match s {
            // An all-null object column has no observable type.
            "null" => "null".into(),
            "object<string>" => "string".into(),
            "object<numeric>" => "numeric".into(),
            "object<uuid>" => "uuid".into(),
            "object<json>" => "json".into(),
            "object<bytes>" => "bytes".into(),
            "object<date>" => "date".into(),
            "object<geometry>" => "geometry".into(),
            "object<int>" => "int64".into(),
            "object<bool>" => "bool".into(),
            "object<float>" => "float64".into(),
            s if s.starts_with("datetime[s") => s.replacen("datetime[s", "datetime[ms", 1),
            s => s.to_string(),
        }
    };
    let (e, a) = (norm(expected), norm(actual));
    e == a
        || e == "null"
        || a == "null"
        || (e == "object<datetime>" && a.starts_with("datetime"))
        || (a == "object<datetime>" && e.starts_with("datetime"))
}
