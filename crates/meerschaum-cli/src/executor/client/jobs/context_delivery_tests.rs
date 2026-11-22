//! HTTP source retry classification must not confuse authority failures with absent objects.

use super::delivery::{ContextDelivery, put_context};
use super::*;
use axum::response::IntoResponse;
use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
use meerschaum_connector_api::traits::ConnectorResult;

struct Authentication;
impl RequestAuthentication for Authentication {
    fn authorization(&self, _: Request<'_>) -> ConnectorResult<String> {
        Ok("Bearer synthetic-context-classification".into())
    }
}

#[tokio::test]
async fn only_the_exact_missing_catalog_status_and_code_allow_one_pull() {
    let reply = Arc::new(std::sync::Mutex::new((200u16, "{}".to_string())));
    let observed = reply.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let router = axum::Router::new().fallback(move || {
        let observed = observed.clone();
        async move {
            let (status, body) = observed.lock().unwrap().clone();
            (axum::http::StatusCode::from_u16(status).unwrap(), body).into_response()
        }
    });
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let api = APIConnector::new(
        Some("context-delivery-fixture"),
        Value::from_json_str(&serde_json::json!({"host":"127.0.0.1","port":port}).to_string())
            .unwrap()
            .to_map(),
    )
    .unwrap()
    .with_request_authentication(Arc::new(Authentication), 4096)
    .unwrap();
    let id = Uuid::new_v4();
    let update = environment::Update {
        metadata_deadline: None,
        id,
        revision: 1,
        owner: "operator".into(),
        owner_id: Some("registered-operator".into()),
        profile: environment::Profile::Api,
        target: environment::Target::Job {
            name: format!("mrsm-{}", id.simple()),
        },
        extension: None,
        registration: None,
        discovery_catalog_sha256: Some("a".repeat(64)),
        callback_url: "https://main.example".into(),
        credential: meerschaum_connector_api::models::ExecutionCredential {
            token: "synthetic-short-context".into(),
            expires_at: chrono::Utc::now().timestamp() + 30,
        },
    };
    assert!(matches!(
        put_context(&api, &update).await.unwrap(),
        ContextDelivery::Applied
    ));
    let missing = serde_json::json!({"detail":{"code":meerschaum_server::executor::gateway::SOURCE_CATALOG_MISSING}}).to_string();
    *reply.lock().unwrap() = (409, missing.clone());
    assert!(matches!(
        put_context(&api, &update).await.unwrap(),
        ContextDelivery::MissingSources
    ));
    for (status, body) in [
        (401, missing.clone()),
        (403, missing.clone()),
        (503, missing),
        (409, "{}".into()),
        (409, "invalid".into()),
        (409, r#"{"detail":{"code":"authority_refused"}}"#.into()),
    ] {
        *reply.lock().unwrap() = (status, body);
        assert!(
            put_context(&api, &update).await.is_err(),
            "HTTP {status} may not authorize a source pull."
        );
    }
    server.abort();
    let _ = server.await;
}
