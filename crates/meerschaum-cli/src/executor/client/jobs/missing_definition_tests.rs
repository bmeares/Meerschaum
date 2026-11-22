//! Which commands of a job need a captured graph before a missing definition is restored.

#[test]
fn ordinary_show_remains_available_and_sync_steps_require_graph_capture() {
    let args = |words: &[&str]| {
        words
            .iter()
            .map(|word| word.to_string())
            .collect::<Vec<_>>()
    };
    assert!(!super::contains_uncaptured_graph(&args(&["show", "version"])).unwrap());
    assert!(
        super::contains_uncaptured_graph(&args(&["show", "version", "+", "verify", "pipes"]))
            .unwrap()
    );
    assert!(
        super::contains_uncaptured_graph(&args(&["sync", "pipes", "--begin=0", "--end=2"]))
            .unwrap()
    );
}

use super::*;
use axum::response::IntoResponse;

struct Authentication;
impl meerschaum_connector_api::client::authentication::RequestAuthentication for Authentication {
    fn authorization(
        &self,
        _: meerschaum_connector_api::client::authentication::Request<'_>,
    ) -> meerschaum_connector_api::traits::ConnectorResult<String> {
        Ok("Bearer synthetic-fixture".into())
    }
}

#[tokio::test]
async fn only_explicit_boolean_absence_authorizes_definition_recovery() {
    let reply = Arc::new(std::sync::Mutex::new((200u16, "false".to_string())));
    let handler = reply.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let router = axum::Router::new().route(
        "/jobs/{name}/exists",
        axum::routing::get(move || {
            let reply = handler.clone();
            async move {
                let (status, body) = reply.lock().unwrap().clone();
                (axum::http::StatusCode::from_u16(status).unwrap(), body).into_response()
            }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let api = APIConnector::new(
        Some("absence-fixture"),
        Value::from_json_str(&serde_json::json!({"host":"127.0.0.1", "port":port}).to_string())
            .unwrap()
            .to_map(),
    )
    .unwrap()
    .with_request_authentication(Arc::new(Authentication), 1024)
    .unwrap();
    assert!(!remote_job_exists(&api, "mrsm-fixture").await.unwrap());
    *reply.lock().unwrap() = (200, "true".into());
    assert!(remote_job_exists(&api, "mrsm-fixture").await.unwrap());
    for (status, body) in [
        (200, "{}"),
        (200, "null"),
        (200, "\"false\""),
        (200, "invalid"),
        (204, ""),
        (401, "false"),
        (403, "false"),
        (404, "false"),
        (503, "false"),
    ] {
        *reply.lock().unwrap() = (status, body.into());
        assert!(
            remote_job_exists(&api, "mrsm-fixture").await.is_err(),
            "HTTP {status}, {body}"
        );
    }
    server.abort();
    let _ = server.await;
    assert!(remote_job_exists(&api, "mrsm-fixture").await.is_err());
}
