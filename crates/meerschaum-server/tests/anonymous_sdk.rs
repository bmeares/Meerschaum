mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use common::{SECRET, req, send};
use meerschaum_server::AppState;

#[tokio::test]
async fn public_reads_accept_the_legacy_sdk_anonymous_sentinel() {
    check_repository(false).await;
}

#[tokio::test]
async fn private_reads_still_require_valid_credentials() {
    check_repository(true).await;
}

async fn check_repository(private: bool) {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("anonymous-sdk"));
    instance.seed_user("admin", "pass", Some("admin"));
    let mut config = common::config(dir.path());
    config.private = private;
    let state = AppState::builder(config, instance)
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let admin = common::login(&app, "admin", "pass", None).await;
    for endpoint in ["/plugins", "/users"] {
        for token in [
            None,
            Some("Bearer None"),
            Some("Bearer invalid-token"),
            Some(admin.as_str()),
        ] {
            let result = send(&app, req("GET", endpoint, token, None)).await;
            let expected = if token == Some(admin.as_str())
                || (!private && matches!(token, None | Some("Bearer None")))
            {
                StatusCode::OK
            } else {
                StatusCode::UNAUTHORIZED
            };
            assert_eq!(
                result.status,
                expected,
                "{endpoint}, private={private}, anonymous={}: {}",
                token.is_none(),
                result.text()
            );
        }
    }
}
