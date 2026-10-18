//! Boundaries needed when composing the console with the API.
use crate::AppState;
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
