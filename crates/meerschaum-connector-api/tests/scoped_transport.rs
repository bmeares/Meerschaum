//! A process-local mutation check must never be silently discarded across an HTTP write.
mod common;

use common::{Canned, MockServer};
use meerschaum_connector::{
    async_trait,
    mutation::{self, MutationCheck},
};
use meerschaum_connector_api::{
    APIConnector,
    client::{Body, Method, RequestOptions},
};
use meerschaum_core::Value;
use std::sync::Arc;

struct Authority;
#[async_trait]
impl MutationCheck for Authority {
    async fn check(&self) -> Result<(), String> {
        Ok(())
    }
}
fn response(status: u16, value: serde_json::Value) -> Canned {
    Canned {
        status,
        content_type: "application/json".into(),
        body: value.to_string().into_bytes(),
    }
}
fn client(port: u16) -> APIConnector {
    APIConnector::new(
        Some("remote"),
        [
            ("host".into(), Value::Str("127.0.0.1".into())),
            ("port".into(), Value::Int(port.into())),
            ("username".into(), Value::Str("service".into())),
            (
                "password".into(),
                Value::Str("synthetic-service-password".into()),
            ),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap()
}

#[tokio::test]
async fn scoped_nonread_refuses_before_login_or_http_and_ordinary_direct_request_still_works() {
    let server = MockServer::start().await;
    let client = client(server.port);
    for path in ["/engine/state", "/login", "/tokens/validate"] {
        let result = mutation::scope(
            Arc::new(Authority),
            client.make_request(
                Method::Post,
                path,
                RequestOptions {
                    use_token: false,
                    body: Body::Json("{}".into()),
                    ..Default::default()
                },
            ),
        )
        .await;
        assert!(matches!(
            result,
            Err(meerschaum_connector_api::ConnectorError::NotImplemented(_))
        ));
        assert!(server.take().0.is_empty());
    }
    server.push_response(response(200, serde_json::json!({"success":true})));
    let result = client
        .make_request(
            Method::Post,
            "/engine/state",
            RequestOptions {
                use_token: false,
                body: Body::Json("[]".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(result.ok());
    assert_eq!(server.take().0.len(), 1);
}

#[tokio::test]
async fn scoped_explicit_api_key_validation_keeps_its_fixed_authentication_only_path() {
    let server = MockServer::start().await;
    let client = APIConnector::new(
        Some("key"),
        [
            ("host".into(), Value::Str("127.0.0.1".into())),
            ("port".into(), Value::Int(server.port.into())),
            (
                "api_key".into(),
                Value::Str("mrsm-key:synthetic-service-key".into()),
            ),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    server.push_response(response(200, serde_json::json!({"valid":true})));
    let result = mutation::scope(Arc::new(Authority), client.login()).await;
    assert!(result.success, "{}", result.message);
    let requests = server.take().0;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].target, "/tokens/validate");
    assert_eq!(
        requests[0].header("authorization"),
        Some("Bearer mrsm-key:synthetic-service-key")
    );
}

#[tokio::test]
async fn scoped_get_keeps_private_verified_login_and_refused_token_refresh_without_public_post_escape()
 {
    let server = MockServer::start().await;
    let client = client(server.port);
    server.push_response(response(200,serde_json::json!({"access_token":"first-synthetic-token","expires":"2099-01-01T00:00:00.000000"})));
    server.push_response(response(401, serde_json::json!({"detail":"revoked"})));
    server.push_response(response(200,serde_json::json!({"access_token":"second-synthetic-token","expires":"2099-01-01T00:00:00.000000"})));
    server.push_response(response(200, serde_json::json!({"safe":true})));
    let result = mutation::scope(
        Arc::new(Authority),
        client.get("/info", RequestOptions::default()),
    )
    .await
    .unwrap();
    assert!(result.ok());
    let requests = server.take().0;
    assert_eq!(
        requests
            .iter()
            .map(|request| request.target.as_str())
            .collect::<Vec<_>>(),
        vec!["/login", "/info", "/login", "/info"]
    );
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        vec!["POST", "GET", "POST", "GET"]
    );
    assert_eq!(
        requests[3].header("authorization"),
        Some("Bearer second-synthetic-token")
    );
}
