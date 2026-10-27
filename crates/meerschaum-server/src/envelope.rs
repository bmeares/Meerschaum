//! One JSON shape for the routes that are new in v5 (`/pipelines`, `/pipelines/*`, `/engine/*`,
//! `/execute`, `/execute/*`): every body is an object
//!
//! ```json
//! {"success": true, "message": "Found 2 runs.", "runs": [...]}
//! {"success": false, "message": "Pipeline 'daily' is not registered on 'sql:main'."}
//! ```
//!
//! and the status code says what happened: `200` (done), `400` (invalid request), `401` / `403`
//! (authentication, permission), `404` (missing), `409` (conflict: already exists, held lease,
//! changed revision), `429` (rate limit), `500` (failure), `501` / `503` (not available here).
//! The routes v4 has keep v4's shapes byte for byte.
//!
//! Handlers build their success bodies with [`ok`] and return [`ApiError`]s as every handler
//! does; the [`v5_errors`] middleware turns any error response of these paths (a handler's
//! `{"detail": ...}`, a validation error, the router's `404` / `405`, the rate limiter's `429`,
//! a plain-text `500`) into the shape above. A `500`'s message names what failed only for an
//! administrator (or under `--no-auth`); other callers get a generic message, and the details
//! are in the server's log.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use meerschaum_core::SuccessTuple;
use serde_json::{Map as JsonMap, Value as Json, json};

use crate::error::{ApiError, InternalError};
use crate::json::{json_response, raw_json};
use crate::state::AppState;

/// The message of a `500` for callers who are not administrators.
pub const GENERIC_FAILURE: &str = "The server could not complete this request. An administrator can find the error in the server's log.";

/// The largest error body the middleware reads (error bodies are small; anything larger is
/// replaced by its status's reason).
const MAX_ERROR_BODY: usize = 1 << 20;

/// Whether `path` is one of the v5-only routes, which answer in this module's shape (decided on
/// the [`crate::routes::decoded_path`], as every request classification is).
pub fn is_v5_path(path: &str) -> bool {
    let path = crate::routes::decoded_path(path);
    let path = path.as_ref();
    let under = |prefix: &str| {
        path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/'))
    };
    under("/pipelines") || under("/engine") || under("/execute")
}

/// `{"success": true, "message": message, **fields}` with `200`.
pub fn ok(message: impl Into<String>, fields: JsonMap<String, Json>) -> Response {
    json_response(StatusCode::OK, &body(true, message.into(), fields))
}

/// [`ok`] with one field.
pub fn ok_with(message: impl Into<String>, key: &str, value: Json) -> Response {
    let mut fields = JsonMap::new();
    fields.insert(key.to_string(), value);
    ok(message, fields)
}

/// [`ok`] with one field whose JSON is already rendered (large row sets skip a parse).
pub fn ok_with_raw(message: &str, key: &str, rendered: &str) -> Response {
    let head = crate::json::fastapi_json(&json!({"success": true, "message": message}));
    // `{"success":true,"message":"..."}` + `,"key":<rendered>}`.
    let head = head.strip_suffix('}').unwrap_or(&head);
    let key = crate::json::fastapi_json(&Json::String(key.to_string()));
    raw_json(StatusCode::OK, format!("{head},{key}:{rendered}}}"))
}

/// A `SuccessTuple` as this shape: `200` when it succeeded, else an error with `failure` as its
/// status (and the tuple's message).
pub fn tuple(result: &SuccessTuple, failure: u16) -> Result<Response, ApiError> {
    if result.success {
        Ok(ok(result.message.clone(), JsonMap::new()))
    } else {
        Err(ApiError::new(failure, result.message.clone()))
    }
}

/// The body of this shape.
pub fn body(success: bool, message: String, fields: JsonMap<String, Json>) -> Json {
    let mut out = JsonMap::new();
    out.insert("success".into(), Json::Bool(success));
    out.insert("message".into(), Json::String(message));
    for (k, v) in fields {
        if k != "success" && k != "message" {
            out.insert(k, v);
        }
    }
    Json::Object(out)
}

/// One pydantic validation error as text: `body.sysargs: Field required`.
fn validation_text(error: &Json) -> String {
    let loc = error
        .get("loc")
        .and_then(Json::as_array)
        .map(|l| {
            l.iter()
                .map(|p| match p {
                    Json::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect::<Vec<_>>()
                .join(".")
        })
        .unwrap_or_default();
    let msg = error
        .get("msg")
        .and_then(Json::as_str)
        .unwrap_or("Invalid value");
    if loc.is_empty() {
        msg.to_string()
    } else {
        format!("{loc}: {msg}")
    }
}

/// Whether the caller is an administrator (or the server runs `--no-auth`), for a `500`'s
/// message. Any failure to tell counts as not.
async fn caller_is_admin(state: &AppState, headers: &HeaderMap) -> bool {
    if state.config.no_auth {
        return true;
    }
    match crate::auth::load_user_or_token(state, headers).await {
        Ok(principal) => crate::auth::is_admin(state, Some(&principal)).await,
        Err(_) => false,
    }
}

/// The middleware that gives every error of the v5-only routes this module's shape (see the
/// module docs). Successful responses pass through untouched.
pub async fn v5_errors(State(state): State<AppState>, req: Request, next: Next) -> Response {
    if !is_v5_path(req.uri().path()) {
        return next.run(req).await;
    }
    let headers = req.headers().clone();
    let response = next.run(req).await;
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error()) {
        return response;
    }
    let (mut parts, raw) = response.into_parts();
    let internal = parts.extensions.remove::<InternalError>();
    let bytes = axum::body::to_bytes(raw, MAX_ERROR_BODY)
        .await
        .unwrap_or_default();
    let parsed: Option<Json> = serde_json::from_slice(&bytes).ok();
    if let Some(Json::Object(o)) = &parsed
        && o.get("success").is_some_and(Json::is_boolean)
        && o.get("message").is_some_and(Json::is_string)
    {
        // Already in shape.
        return Response::from_parts(parts, Body::from(bytes));
    }
    let reason = status.canonical_reason().unwrap_or("Error").to_string();
    let mut fields = JsonMap::new();
    let message = match &parsed {
        Some(Json::Object(o)) => match o.get("detail") {
            Some(Json::String(s)) => s.clone(),
            Some(Json::Array(errors)) => {
                let text: Vec<String> = errors.iter().map(validation_text).collect();
                fields.insert("errors".into(), Json::Array(errors.clone()));
                if text.is_empty() {
                    reason.clone()
                } else {
                    text.join("; ")
                }
            }
            Some(Json::Null) | None => reason.clone(),
            Some(other) => other.to_string(),
        },
        _ => {
            let text = String::from_utf8_lossy(&bytes).trim().to_string();
            if text.is_empty() || text.len() > 2000 {
                reason.clone()
            } else {
                text
            }
        }
    };
    // A request that does not validate is a bad request.
    let status = if status == StatusCode::UNPROCESSABLE_ENTITY {
        StatusCode::BAD_REQUEST
    } else {
        status
    };
    let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
        let detail = match internal {
            Some(InternalError(text)) => text,
            None => message,
        };
        if caller_is_admin(&state, &headers).await {
            detail
        } else {
            GENERIC_FAILURE.to_string()
        }
    } else {
        message
    };
    let mut out = json_response(status, &body(false, message, fields)).into_response();
    for (name, value) in parts.headers.iter() {
        if name != header::CONTENT_TYPE && name != header::CONTENT_LENGTH {
            out.headers_mut().insert(name.clone(), value.clone());
        }
    }
    let extensions = std::mem::take(&mut parts.extensions);
    out.extensions_mut().extend(extensions);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v5_paths() {
        for path in [
            "/pipelines",
            "/pipelines/daily",
            "/pipelines/keys",
            "/engine/runs",
            "/engine/runs/abc/events/stream",
            "/execute",
            "/execute/runs",
        ] {
            assert!(is_v5_path(path), "{path}");
        }
        for path in [
            "/pipes/a/b/c",
            "/pipelinesx",
            "/engineering",
            "/executed",
            "/",
        ] {
            assert!(!is_v5_path(path), "{path}");
        }
    }

    #[test]
    fn validation_errors_read_as_text() {
        assert_eq!(
            validation_text(&json!({"loc": ["body", "sysargs"], "msg": "Field required"})),
            "body.sysargs: Field required"
        );
        assert_eq!(validation_text(&json!({"msg": "Nope"})), "Nope");
    }

    #[test]
    fn raw_fields_render_inside_the_object() {
        let r = ok_with_raw("Read 1 row.", "rows", "[{\"a\":1}]");
        let (_, body) = r.into_parts();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let bytes = rt.block_on(axum::body::to_bytes(body, usize::MAX)).unwrap();
        let j: Json = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            j,
            json!({"success": true, "message": "Read 1 row.", "rows": [{"a": 1}]})
        );
    }
}
