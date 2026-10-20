//! Boundaries needed when composing the console with the API, and the Dash console's entry
//! (`/dash`, `/dash/*`): the Python API host serves the console (D21); the server proxies to it
//! when it mounted it and shows a static pointer page otherwise.
use crate::AppState;
use crate::host::ConsoleStatus;
use axum::{
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

fn session_target(target: &str) -> bool {
    target
        .rsplit('.')
        .next()
        .unwrap_or(target)
        .trim_matches(['"', '\'', '`', '[', ']'])
        .eq_ignore_ascii_case("mrsm_sessions")
}
fn decode(segment: &str) -> String {
    // URI path '+' is literal, unlike form/query '+'.
    meerschaum_connector_api::wire::parse_query(&format!("key={}", segment.replace('+', "%2B")))
        .into_iter()
        .next()
        .map(|(_, value)| value)
        .unwrap_or_default()
}
/// The session table is internal, just like the users table. Protect both an existing alias
/// and an attempted registration/edit targeting it. Internal connector calls bypass HTTP.
pub async fn protect_sessions(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    let parts: Vec<_> = req.uri().path().split('/').map(str::to_owned).collect();
    if parts.len() < 5 || parts[1] != "pipes" {
        return next.run(req).await;
    }
    let ck = decode(&parts[2]);
    let mk = decode(&parts[3]);
    let lk = decode(&parts[4]);
    let query = crate::query::Query::parse(req.uri().query());
    let instance = query.opt_str("instance_keys");
    if let Ok(pipe) = state
        .get_pipe(&ck, &mk, Some(&lk), instance.as_deref(), true)
        .await
        && pipe
            .pipe
            .target(None)
            .is_ok_and(|target| session_target(&target))
    {
        return (StatusCode::FORBIDDEN, "The session table is internal.").into_response();
    }
    if matches!(*req.method(), Method::POST | Method::PATCH)
        && parts.get(5).is_some_and(|p| p == "register" || p == "edit")
    {
        let (head, body) = req.into_parts();
        let bytes = match to_bytes(body, usize::MAX).await {
            Ok(b) => b,
            Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        };
        if let Ok(parameters) = serde_json::from_slice::<serde_json::Value>(&bytes)
            && ["target", "target_name", "target_table"]
                .iter()
                .any(|key| parameters[*key].as_str().is_some_and(session_target))
        {
            return (StatusCode::FORBIDDEN, "The session table is internal.").into_response();
        }
        req = Request::from_parts(head, Body::from(bytes));
    }
    next.run(req).await
}

/// Whether `path` belongs to the console: `/dash` and everything under `/dash/`.
pub fn is_console_path(path: &str) -> bool {
    path == "/dash" || path.starts_with("/dash/")
}

/// The pointer page: what `/dash` shows when this server has no console to serve (no Python
/// package with the `dash` extra, a host that did not mount it, `mrsm` standalone). `reason`
/// says why (escaped here).
pub fn pointer_page(reason: &str) -> String {
    let reason = reason
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;");
    format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
<meta name=\"color-scheme\" content=\"light dark\"><title>Meerschaum</title>\
<style>body{{font-family:system-ui,sans-serif;max-width:40rem;margin:4rem auto;padding:0 1rem;line-height:1.5}}\
code{{background:rgba(127,127,127,.2);padding:.1rem .35rem;border-radius:.25rem}}\
small{{opacity:.7}}</style></head><body>\
<h1>Meerschaum</h1>\
<p>The web console comes with the Python package: <code>pip install meerschaum[dash]</code></p>\
<p>This server runs without it. The API is available (<a href=\"/docs\">/docs</a>).</p>\
<p><small>{reason}</small></p>\
</body></html>"
    )
}

/// Serve a console path: proxy to the API host when it mounted the Dash console, else the
/// pointer page (`200` for `/dash` and `/dash/`, `404` below them). Everything about the
/// request reaches the host unchanged, so the Dash app sees the paths it was mounted at.
pub async fn console_entry(state: AppState, req: Request) -> Response {
    let path = req.uri().path().to_string();
    if state.config.no_dash {
        let mut resp = crate::error::ApiError::not_found().into_response();
        resp.extensions_mut().insert(crate::routes::Unmatched);
        return resp;
    }
    let status = match &state.host {
        Some(host) => host.console(state.secret()).await,
        None => ConsoleStatus::Unavailable("this server has no Python API host".into()),
    };
    match (status, &state.host) {
        (ConsoleStatus::Mounted, Some(host)) => host.forward(req).await,
        (status, _) => {
            let reason = match status {
                ConsoleStatus::Unavailable(reason) => reason,
                _ => String::new(),
            };
            let root = matches!(path.as_str(), "/dash" | "/dash/");
            let mut resp = if root {
                axum::response::Html(pointer_page(&reason)).into_response()
            } else {
                StatusCode::NOT_FOUND.into_response()
            };
            resp.headers_mut().insert(
                axum::http::header::CACHE_CONTROL,
                axum::http::HeaderValue::from_static("no-store"),
            );
            resp
        }
    }
}
