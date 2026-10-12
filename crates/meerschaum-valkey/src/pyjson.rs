//! `json.dumps(doc, default=json_serialize_value, separators=(',', ':'), sort_keys=...)` for
//! documents of kernel values, as the Python connector serializes rows and index documents.

use meerschaum_core::pyfmt::{float_repr, json_dumps_str};
use meerschaum_kernel::serialize::json_serialize_value;
use meerschaum_kernel::value::Value as KValue;

use meerschaum_api::pipe_ops::Doc;

fn write_float(f: f64, out: &mut String) {
    // `json.dumps` writes non-finite floats as JavaScript literals (`allow_nan=True`).
    if f.is_nan() {
        out.push_str("NaN");
    } else if f == f64::INFINITY {
        out.push_str("Infinity");
    } else if f == f64::NEG_INFINITY {
        out.push_str("-Infinity");
    } else {
        out.push_str(&float_repr(f));
    }
}

fn write_json(j: &serde_json::Value, sort_keys: bool, out: &mut String) {
    match j {
        serde_json::Value::Null => out.push_str("null"),
        serde_json::Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => out.push_str(&i.to_string()),
            None => match n.as_u64() {
                Some(u) => out.push_str(&u.to_string()),
                None => write_float(n.as_f64().unwrap_or(f64::NAN), out),
            },
        },
        serde_json::Value::String(s) => out.push_str(&json_dumps_str(s, true)),
        serde_json::Value::Array(a) => {
            out.push('[');
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(v, sort_keys, out);
            }
            out.push(']');
        }
        serde_json::Value::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            if sort_keys {
                keys.sort();
            }
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&json_dumps_str(k, true));
                out.push(':');
                write_json(&o[k.as_str()], sort_keys, out);
            }
            out.push('}');
        }
    }
}

/// Write one value as `json.dumps` with `default=json_serialize_value` would.
pub fn write_value(v: &KValue, sort_keys: bool, out: &mut String) {
    match v {
        // Enforced datetime columns are Arrow-backed in v4, whose nulls come out of
        // `to_dict()` as `None` (`null`). (A NumPy-backed `NaT` would serialize as `"NaTZ"`
        // through `serialize_datetime`; the kernel has no NumPy-backed datetimes.)
        KValue::Null | KValue::NA | KValue::NaT => out.push_str("null"),
        KValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        KValue::Int(i) => out.push_str(&i.to_string()),
        KValue::Float(f) => write_float(*f, out),
        KValue::Str(s) => out.push_str(&json_dumps_str(s, true)),
        KValue::Json(j) => write_json(j, sort_keys, out),
        other => write_json(&json_serialize_value(other), sort_keys, out),
    }
}

/// `json.dumps(doc, default=json_serialize_value, separators=(',', ':'), sort_keys=sort_keys)`.
pub fn dumps_doc(doc: &Doc, sort_keys: bool) -> String {
    let mut keys: Vec<&String> = doc.keys().collect();
    if sort_keys {
        keys.sort();
    }
    let mut out = String::from("{");
    for (i, k) in keys.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&json_dumps_str(k, true));
        out.push(':');
        write_value(&doc[k.as_str()], sort_keys, &mut out);
    }
    out.push('}');
    out
}

/// `serialize_document(doc)` (sorted keys).
pub fn serialize_document(doc: &Doc) -> String {
    dumps_doc(doc, true)
}

/// `json.loads` of a stored document into kernel values (containers stay JSON values).
pub fn loads_doc(s: &str) -> Result<Doc, serde_json::Error> {
    let j: serde_json::Value = serde_json::from_str(&sanitize_nan(s))?;
    Ok(match j {
        serde_json::Value::Object(o) => o
            .into_iter()
            .map(|(k, v)| (k, json_scalar_to_kernel(v)))
            .collect(),
        _ => Doc::new(),
    })
}

/// A JSON value as a kernel value (`json.loads` semantics: numbers stay `int` / `float`).
pub fn json_scalar_to_kernel(v: serde_json::Value) -> KValue {
    match v {
        serde_json::Value::Null => KValue::Null,
        serde_json::Value::Bool(b) => KValue::Bool(b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => KValue::Int(i),
            None => {
                let f = n.as_f64().unwrap_or(f64::NAN);
                if f == NAN_SENTINEL {
                    KValue::Float(f64::NAN)
                } else if f == POS_INF_SENTINEL {
                    KValue::Float(f64::INFINITY)
                } else if f == NEG_INF_SENTINEL {
                    KValue::Float(f64::NEG_INFINITY)
                } else {
                    KValue::Float(f)
                }
            }
        },
        serde_json::Value::String(s) => KValue::Str(s),
        other => KValue::Json(other),
    }
}

/// A sentinel for `NaN` (which `serde_json` cannot parse) while loading Python's `json.dumps`
/// output; it is converted back to `NaN` by [`json_scalar_to_kernel`].
const NAN_SENTINEL: f64 = f64::MIN;
/// See [`NAN_SENTINEL`] (`Infinity`).
const POS_INF_SENTINEL: f64 = f64::MAX;
/// See [`NAN_SENTINEL`] (`-Infinity`).
const NEG_INF_SENTINEL: f64 = -1.797_693_134_862_315_5e308;

/// Replace bare `NaN` / `Infinity` tokens (outside strings) so `serde_json` can parse the text.
fn sanitize_nan(s: &str) -> std::borrow::Cow<'_, str> {
    if !s.contains("NaN") && !s.contains("Infinity") {
        return std::borrow::Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let mut in_str = false;
    let mut escaped = false;
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if in_str {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        if c == '"' {
            in_str = true;
            out.push(c);
            i += 1;
            continue;
        }
        let rest: String = chars[i..chars.len().min(i + 9)].iter().collect();
        if rest.starts_with("NaN") {
            out.push_str("-1.7976931348623157e308");
            i += 3;
        } else if rest.starts_with("-Infinity") {
            out.push_str("-1.7976931348623155e308");
            i += 9;
        } else if rest.starts_with("Infinity") {
            out.push_str("1.7976931348623157e308");
            i += 8;
        } else {
            out.push(c);
            i += 1;
        }
    }
    std::borrow::Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_dumps() {
        let doc: Doc = [
            ("val".to_string(), KValue::Float(1.5)),
            ("station".to_string(), KValue::Str("é".into())),
            ("n".to_string(), KValue::Int(3)),
            ("x".to_string(), KValue::Float(f64::NAN)),
            ("na".to_string(), KValue::NA),
            (
                "j".to_string(),
                KValue::Json(serde_json::json!({"b": 1.0, "a": [1, "x"]})),
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            serialize_document(&doc),
            "{\"j\":{\"a\":[1,\"x\"],\"b\":1.0},\"n\":3,\"na\":null,\"station\":\"\\u00e9\",\"val\":1.5,\"x\":NaN}"
        );
        let back = loads_doc(&serialize_document(&doc)).unwrap();
        assert!(matches!(back["x"], KValue::Float(f) if f.is_nan()));
        assert!(matches!(back["n"], KValue::Int(3)));
    }
}
