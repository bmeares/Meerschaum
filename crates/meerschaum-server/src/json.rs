//! Python-compatible JSON bodies.
//!
//! FastAPI's `JSONResponse` renders `json.dumps(content, ensure_ascii=False, allow_nan=False,
//! separators=(",", ":"))`; routes that build their own `Response(json.dumps(...))` use the
//! default `json.dumps` style (`", "`, `": "`, `ensure_ascii=True`). Floats follow Python's
//! `repr`, which differs from `serde_json` (`1e+20` vs `1e20`), so bodies go through
//! `meerschaum_core::pyfmt::json_dumps`.

use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::{SuccessTuple, Value};
use serde_json::Value as Json;

/// A `serde_json` value as a core value (key order preserved).
pub fn to_core(v: &Json) -> Value {
    serde_json::from_value(v.clone()).unwrap_or(Value::Null)
}

/// A core value as a `serde_json` value.
pub fn from_core(v: &Value) -> Json {
    serde_json::to_value(v).unwrap_or(Json::Null)
}

/// FastAPI's `JSONResponse` body.
pub fn fastapi_json(v: &Json) -> String {
    json_dumps(&to_core(v), JsonStyle::Compact, false)
}

/// `json.dumps(v)` with the default separators and `ensure_ascii=True`.
pub fn python_json(v: &Value) -> String {
    json_dumps(v, JsonStyle::Default, true)
}

/// A JSON response rendered like FastAPI's `JSONResponse`.
pub fn json_response(status: StatusCode, v: &Json) -> Response {
    raw_json(status, fastapi_json(v))
}

/// `200` with a FastAPI-rendered JSON body.
pub fn ok_json(v: &Json) -> Response {
    json_response(StatusCode::OK, v)
}

/// A pre-rendered JSON body (`fastapi.Response(content, media_type='application/json')`).
pub fn raw_json(status: StatusCode, body: String) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(body),
    )
        .into_response()
}

/// A `SuccessTuple` response: `[true, "message"]` (`SuccessTupleResponseModel`).
pub fn tuple_response(t: &SuccessTuple) -> Response {
    ok_json(&Json::Array(vec![
        Json::Bool(t.success),
        Json::String(t.message.clone()),
    ]))
}

/// `[success, message]` from parts.
pub fn tuple(success: bool, message: impl Into<String>) -> Response {
    tuple_response(&SuccessTuple {
        success,
        message: message.into(),
    })
}

/// A URL path segment: everything but RFC 3986's unreserved characters is percent-encoded.
pub fn quote_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A query string of `(key, value)` pairs, each part percent-encoded.
pub fn query_string(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", quote_segment(k), quote_segment(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// pydantic v2's JSON rendering of an aware UTC `datetime`: `2026-01-01T00:00:00Z`, with
/// `.ffffff` only when there are microseconds.
pub fn pydantic_datetime(dt: &chrono::DateTime<chrono::Utc>) -> String {
    let base = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = dt.timestamp_subsec_micros();
    if micros == 0 {
        format!("{base}Z")
    } else {
        format!("{base}.{micros:06}Z")
    }
}

/// Python's `datetime.isoformat()` of an aware UTC timestamp (`+00:00`).
pub fn py_isoformat_utc(dt: &chrono::DateTime<chrono::Utc>) -> String {
    let base = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = dt.timestamp_subsec_micros();
    if micros == 0 {
        format!("{base}+00:00")
    } else {
        format!("{base}.{micros:06}+00:00")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn python_rendering() {
        assert_eq!(
            fastapi_json(&json!({"a": 1e20, "b": "é", "c": [true, null]})),
            "{\"a\":1e+20,\"b\":\"é\",\"c\":[true,null]}"
        );
        assert_eq!(
            python_json(&to_core(&json!([["a", 1]]))),
            "[[\"a\", 1]]".to_string()
        );
    }
}
