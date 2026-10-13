//! JSON-RPC 2.0 envelopes, the protocol constants and the pagination cursors
//! (`meerschaum/mcp/_dispatch.py`, `_registry.py`).
//!
//! Messages are [`Value`]s (insertion-ordered), so responses render with v4's key order.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE;
use meerschaum_core::{Map, Value};

use crate::auth::Scopes;

/// JSON-RPC: invalid JSON.
pub const PARSE_ERROR: i64 = -32700;
/// JSON-RPC: not a request object.
pub const INVALID_REQUEST: i64 = -32600;
/// JSON-RPC: unknown method.
pub const METHOD_NOT_FOUND: i64 = -32601;
/// JSON-RPC: bad parameters.
pub const INVALID_PARAMS: i64 = -32602;
/// JSON-RPC: the handler failed.
pub const INTERNAL_ERROR: i64 = -32603;

/// A map from pairs, in order.
pub fn map_of<const N: usize>(pairs: [(&str, Value); N]) -> Map {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// `Value::Str`.
pub fn s(v: impl Into<String>) -> Value {
    Value::Str(v.into())
}

/// `jsonrpc_result(request_id, result)`.
pub fn result(id: Value, result: Value) -> Value {
    Value::Map(map_of([
        ("jsonrpc", s("2.0")),
        ("id", id),
        ("result", result),
    ]))
}

/// `jsonrpc_error(request_id, code, message)`.
pub fn error(id: Value, code: i64, message: impl Into<String>) -> Value {
    Value::Map(map_of([
        ("jsonrpc", s("2.0")),
        ("id", id),
        (
            "error",
            Value::Map(map_of([
                ("code", Value::Int(code)),
                ("message", s(message)),
            ])),
        ),
    ]))
}

/// `_tool_error(request_id, message)`: a tool failure reported in the result (`isError`), so
/// the model can see it.
pub fn tool_error(id: Value, message: impl Into<String>) -> Value {
    result(
        id,
        Value::Map(map_of([
            (
                "content",
                Value::List(vec![Value::Map(map_of([
                    ("type", s("text")),
                    ("text", s(message)),
                ]))]),
            ),
            ("isError", Value::Bool(true)),
        ])),
    )
}

/// `has_scope(current_scopes, required_scopes)`.
pub fn has_scope(current: &Scopes, required: &[String]) -> bool {
    if required.is_empty() || current.has("*") {
        return true;
    }
    required.iter().all(|scope| current.has(scope))
}

/// `encode_cursor(offset)`: urlsafe base64 of `{"offset":N}`.
pub fn encode_cursor(offset: usize) -> String {
    URL_SAFE.encode(format!("{{\"offset\":{offset}}}"))
}

/// `decode_cursor(cursor)`: the offset, `0` when unset, or `Invalid cursor: ...`.
pub fn decode_cursor(cursor: Option<&Value>, max_offset: i64) -> Result<usize, String> {
    let raw = match cursor {
        None | Some(Value::Null) => return Ok(0),
        Some(Value::Str(s)) if s.is_empty() => return Ok(0),
        Some(Value::Str(s)) => s.clone(),
        // `cursor.encode(...)` on a non-string raises `AttributeError`, which `decode_cursor`
        // catches and reports as an invalid cursor.
        Some(other) if !other.truthy() => return Ok(0),
        Some(other) => other.py_str(),
    };
    let invalid = || format!("Invalid cursor: {raw}");
    // PARITY: `base64.urlsafe_b64decode` is `b64decode(validate=False)` after mapping `-_` to
    // `+/`: characters outside the alphabet are discarded, bad padding raises.
    let translated: String = raw
        .chars()
        .map(|c| match c {
            '-' => '+',
            '_' => '/',
            other => other,
        })
        .collect();
    let bytes = meerschaum_core::users::a2b_base64(translated.as_bytes()).map_err(|_| invalid())?;
    let parsed: Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    let offset = match parsed.get("offset") {
        Some(Value::Int(i)) => *i,
        // `isinstance(True, int)`.
        Some(Value::Bool(b)) => i64::from(*b),
        _ => return Err(invalid()),
    };
    if offset < 0 || offset > max_offset {
        return Err(invalid());
    }
    usize::try_from(offset).map_err(|_| invalid())
}

/// `paginate(items, cursor, page_size)`: a page and the next cursor.
pub fn paginate<T: Clone>(
    items: &[T],
    cursor: Option<&Value>,
    page_size: usize,
    max_offset: i64,
) -> Result<(Vec<T>, Option<String>), String> {
    let offset = decode_cursor(cursor, max_offset)?;
    let start = offset.min(items.len());
    let end = (offset + page_size).min(items.len());
    let page = items[start..end].to_vec();
    let next = offset + page.len();
    Ok((page, (next < items.len()).then(|| encode_cursor(next))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_roundtrip() {
        assert_eq!(encode_cursor(10), "eyJvZmZzZXQiOjEwfQ==");
        let c = Value::Str(encode_cursor(5));
        assert_eq!(decode_cursor(Some(&c), 1_000_000), Ok(5));
        assert_eq!(decode_cursor(None, 10), Ok(0));
        for bad in ["nope", "!!", "x", "eyJvZmZzZXQiOi0xfQ=="] {
            assert_eq!(
                decode_cursor(Some(&Value::Str(bad.into())), 1_000_000),
                Err(format!("Invalid cursor: {bad}"))
            );
        }
        let (page, next) = paginate(&[1, 2, 3], None, 2, 100).unwrap();
        assert_eq!(page, vec![1, 2]);
        assert_eq!(next.as_deref(), Some("eyJvZmZzZXQiOjJ9"));
    }

    #[test]
    fn scopes() {
        let reader = Scopes::List(vec!["pipes:read".into()]);
        assert!(has_scope(&reader, &[]));
        assert!(has_scope(&reader, &["pipes:read".into()]));
        assert!(!has_scope(
            &reader,
            &["pipes:read".into(), "pipes:write".into()]
        ));
        assert!(has_scope(&Scopes::List(vec!["*".into()]), &["x".into()]));
    }
}
