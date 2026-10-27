//! FastAPI-compatible error responses: `HTTPException` (`{"detail": ...}`), request validation
//! errors (422 with pydantic's error list) and Starlette's plain-text 500.

use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value as Json, json};

use crate::json::json_response;

/// An error response. Handlers return `Result<Response, ApiError>`.
#[derive(Debug, Clone)]
pub struct ApiError {
    /// The status code.
    pub status: StatusCode,
    /// The body. `None` is Starlette's plain-text `Internal Server Error`.
    pub detail: Option<Json>,
    /// Extra headers (`WWW-Authenticate`).
    pub headers: Vec<(HeaderName, HeaderValue)>,
    /// What failed, for an unhandled error ([`ApiError::exception`]): never in a v4 body; the
    /// v5-only routes show it to administrators ([`crate::envelope`]). Boxed: errors stay small.
    pub internal: Option<Box<String>>,
}

/// The message of an unhandled error, carried on its response for [`crate::envelope`].
#[derive(Debug, Clone)]
pub struct InternalError(pub String);

impl ApiError {
    /// `HTTPException(status_code, detail)`.
    pub fn new(status: u16, detail: impl Into<String>) -> ApiError {
        ApiError {
            status: StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            detail: Some(Json::String(detail.into())),
            headers: Vec::new(),
            internal: None,
        }
    }

    /// `HTTPException(status_code, detail=<any JSON>)`.
    pub fn with_detail(status: u16, detail: Json) -> ApiError {
        ApiError {
            status: StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            detail: Some(detail),
            headers: Vec::new(),
            internal: None,
        }
    }

    /// `503` for a failed read the request depends on (an instance that did not answer):
    /// authorization fails closed with this rather than granting or refusing on a guess.
    pub fn unavailable(detail: impl Into<String>) -> ApiError {
        ApiError::new(503, detail)
    }

    /// Add a header (`WWW-Authenticate: Bearer`).
    pub fn header(mut self, name: HeaderName, value: &'static str) -> ApiError {
        self.headers.push((name, HeaderValue::from_static(value)));
        self
    }

    /// `401` with `WWW-Authenticate: <scheme>`.
    pub fn unauthorized(detail: &str, scheme: Option<&'static str>) -> ApiError {
        let e = ApiError::new(401, detail);
        match scheme {
            Some(s) => e.header(header::WWW_AUTHENTICATE, s),
            None => e,
        }
    }

    /// `fastapi_login.exceptions.InvalidCredentialsException`.
    pub fn invalid_credentials() -> ApiError {
        ApiError::unauthorized("Invalid credentials", Some("Bearer"))
    }

    /// Starlette's unhandled-exception response: `500 Internal Server Error` as plain text.
    pub fn internal() -> ApiError {
        ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            detail: None,
            headers: Vec::new(),
            internal: None,
        }
    }

    /// An unhandled exception in a handler (logged without request data, then a plain 500).
    pub fn exception(context: &str, err: impl std::fmt::Display) -> ApiError {
        tracing::warn!("{context}: {err}");
        let mut e = ApiError::internal();
        e.internal = Some(Box::new(format!("{context}: {err}")));
        e
    }

    /// This error with what failed (see [`ApiError::internal`](struct@ApiError)), shown to
    /// administrators on the v5-only routes.
    pub fn with_internal(mut self, internal: impl Into<String>) -> ApiError {
        self.internal = Some(Box::new(internal.into()));
        self
    }

    /// `404 {"detail": "Not Found"}` (Starlette's unmatched route).
    pub fn not_found() -> ApiError {
        ApiError::new(404, "Not Found")
    }

    /// `405 {"detail": "Method Not Allowed"}`.
    pub fn method_not_allowed() -> ApiError {
        ApiError::new(405, "Method Not Allowed")
    }

    /// A request validation error (`RequestValidationError` → 422 `{"detail": [errors]}`).
    pub fn validation(errors: Vec<Json>) -> ApiError {
        ApiError::with_detail(422, Json::Array(errors))
    }

    /// One pydantic error for a query/path/body field.
    pub fn field_error(
        kind: &str,
        loc: &[&str],
        msg: &str,
        input: Json,
        ctx: Option<Json>,
    ) -> Json {
        let mut e = json!({
            "type": kind,
            "loc": loc,
            "msg": msg,
            "input": input,
        });
        if let (Some(ctx), Some(o)) = (ctx, e.as_object_mut()) {
            o.insert("ctx".into(), ctx);
        }
        e
    }

    /// `{"type": "missing", "loc": [...], "msg": "Field required", "input": null}`.
    pub fn missing(loc: &[&str]) -> ApiError {
        ApiError::validation(vec![ApiError::field_error(
            "missing",
            loc,
            "Field required",
            Json::Null,
            None,
        )])
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut resp = match self.detail {
            Some(detail) => json_response(self.status, &json!({ "detail": detail })),
            None => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                "Internal Server Error",
            )
                .into_response(),
        };
        for (k, v) in self.headers {
            resp.headers_mut().insert(k, v);
        }
        if let Some(internal) = self.internal {
            resp.extensions_mut().insert(InternalError(*internal));
        }
        resp
    }
}

/// The result type of every handler.
pub type ApiResult = Result<Response, ApiError>;
