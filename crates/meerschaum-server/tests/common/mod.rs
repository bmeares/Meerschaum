//! Shared helpers for the server's integration tests.

#![allow(dead_code)]

pub mod memory;

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use meerschaum_server::{AppState, ServerConfig};
use tower::ServiceExt;

pub use memory::MemoryInstance;

/// The JWT secret used by the tests.
pub const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef0123456789abcdef";

/// A config for `sql:fixture` with paths under `root`.
pub fn config(root: &std::path::Path) -> ServerConfig {
    ServerConfig::for_root("sql:fixture", root)
}

/// A state over a fresh in-memory instance with an `admin` user (`admin` / `pass`).
pub fn state_with(cfg: ServerConfig) -> (AppState, Arc<MemoryInstance>) {
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        .build()
        .expect("state");
    (state, instance)
}

/// A response's parts.
pub struct Resp {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: bytes::Bytes,
    pub unmatched: bool,
}

impl Resp {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Send a request through the router in-process.
pub async fn send(app: &Router, req: Request<Body>) -> Resp {
    let resp = app.clone().oneshot(req).await.expect("infallible");
    let unmatched = resp
        .extensions()
        .get::<meerschaum_server::routes::Unmatched>()
        .is_some();
    let (parts, body) = resp.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX).await.expect("body");
    Resp {
        status: parts.status,
        headers: parts.headers,
        body,
        unmatched,
    }
}

/// A request builder shortcut.
pub fn req(
    method: &str,
    uri: &str,
    auth: Option<&str>,
    body: Option<(&str, Vec<u8>)>,
) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(a) = auth {
        b = b.header("authorization", a);
    }
    match body {
        Some((ct, bytes)) => b
            .header("content-type", ct)
            .body(Body::from(bytes))
            .unwrap(),
        None => b.body(Body::empty()).unwrap(),
    }
}

/// Log in and return `Bearer <jwt>`.
pub async fn login(app: &Router, username: &str, password: &str, scope: Option<&str>) -> String {
    let mut form = format!("username={username}&password={password}");
    if let Some(s) = scope {
        form.push_str(&format!("&scope={s}"));
    }
    let r = send(
        app,
        req(
            "POST",
            "/login",
            None,
            Some(("application/x-www-form-urlencoded", form.into_bytes())),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    format!("Bearer {}", r.json()["access_token"].as_str().unwrap())
}
