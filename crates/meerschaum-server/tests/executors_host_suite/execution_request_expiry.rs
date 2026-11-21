//! A request-local execution cache cannot extend a presented bearer's native-write lifetime.
use crate::common;
use axum::{
    Router,
    http::{HeaderMap, StatusCode},
    middleware,
    response::IntoResponse,
    routing::post,
};
use meerschaum_core::Value;
use meerschaum_server::{access, auth, executor::callbacks};
use std::sync::Arc;
use tokio::sync::Notify;

#[tokio::test]
async fn callback_bearer_expiring_during_an_await_is_rejected_by_fresh_mutation_admission() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        Value::from_json_str(r#"{"role":"operator"}"#)
            .unwrap()
            .to_map(),
    );
    let app = meerschaum_server::app(state.clone());
    let original = common::login(&app, "alice", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", original.parse().unwrap());
    let source = access::resolve(&state, &headers).await.unwrap();
    let grant = callbacks::register(
        &state,
        &source,
        uuid::Uuid::new_v4(),
        None,
        &["pipes:write"],
        callbacks::Purpose::Command,
    )
    .await
    .unwrap();
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let payload = auth::jwt_decode(&credential.token, common::SECRET, chrono::Utc::now()).unwrap();
    let expiry = chrono::Utc::now().timestamp() + 2;
    let mut claims = Value::from_json_str(&serde_json::to_string(&payload).unwrap())
        .unwrap()
        .to_map();
    claims.insert("exp".into(), expiry.into());
    let bearer = format!("Bearer {}", auth::jwt_encode(&claims, common::SECRET));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let endpoint = {
        let (state, entered, release) = (state.clone(), entered.clone(), release.clone());
        post(move |headers: HeaderMap| {
            let (state, entered, release) = (state.clone(), entered.clone(), release.clone());
            async move {
                access::require_fresh(&state, &headers, &["pipes:write"])
                    .await
                    .unwrap();
                entered.notify_one();
                release.notified().await;
                match access::require_fresh(&state, &headers, &["pipes:write"]).await {
                    Ok(_) => StatusCode::NO_CONTENT.into_response(),
                    Err(error) => error.into_response(),
                }
            }
        })
    };
    let router = Router::new()
        .route("/pipes/fixture-expiry", endpoint)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            callbacks::guard,
        ));
    let response = tokio::spawn(async move {
        common::send(
            &router,
            common::req("POST", "/pipes/fixture-expiry", Some(&bearer), None),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    while chrono::Utc::now().timestamp() <= expiry {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        callbacks::load_identity(&state, grant.id, grant.launch_id)
            .await
            .is_ok(),
        "The source grant itself should remain live during this shorter JWT expiry."
    );
    release.notify_one();
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), response)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        response.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        response.text()
    );
}

#[tokio::test]
async fn fresh_mutation_admission_reloads_grant_role_and_original_registration_after_an_await() {
    use meerschaum_connector::InstanceConnector;
    use meerschaum_core::users::User;
    for change in ["grant", "role", "original-registration"] {
        let root = tempfile::tempdir().unwrap();
        let (state, instance) = common::state_with(common::config(root.path()));
        instance.seed_user("alice", "pass", None);
        instance.seed_user_attributes(
            "alice",
            Value::from_json_str(r#"{"role":"operator"}"#)
                .unwrap()
                .to_map(),
        );
        let app = meerschaum_server::app(state.clone());
        let original = common::login(&app, "alice", "pass", None).await;
        let mut headers = HeaderMap::new();
        headers.insert("authorization", original.parse().unwrap());
        let source = access::require_fresh(&state, &headers, &["pipes:write"])
            .await
            .unwrap();
        let grant = callbacks::register(
            &state,
            &source,
            uuid::Uuid::new_v4(),
            None,
            &["pipes:write"],
            callbacks::Purpose::Command,
        )
        .await
        .unwrap();
        let credential = callbacks::credential(&state, grant.id, grant.launch_id)
            .await
            .unwrap();
        let bearer = format!("Bearer {}", credential.token);
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let endpoint = {
            let (state, entered, release) = (state.clone(), entered.clone(), release.clone());
            post(move |headers: HeaderMap| {
                let (state, entered, release) = (state.clone(), entered.clone(), release.clone());
                async move {
                    access::require_fresh(&state, &headers, &["pipes:write"])
                        .await
                        .unwrap();
                    entered.notify_one();
                    release.notified().await;
                    match access::require_fresh(&state, &headers, &["pipes:write"]).await {
                        Ok(_) => StatusCode::NO_CONTENT.into_response(),
                        Err(error) => error.into_response(),
                    }
                }
            })
        };
        let router = Router::new().route("/pipes/fixture-fresh", endpoint).layer(
            middleware::from_fn_with_state(state.clone(), callbacks::guard),
        );
        let response = tokio::spawn(async move {
            common::send(
                &router,
                common::req("POST", "/pipes/fixture-fresh", Some(&bearer), None),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        match change {
            "grant" => callbacks::revoke(&state, grant.id).await.unwrap(),
            "role" => instance.seed_user_attributes(
                "alice",
                Value::from_json_str(r#"{"role":"viewer"}"#)
                    .unwrap()
                    .to_map(),
            ),
            "original-registration" => {
                let user = User::new("alice", None);
                let old_id = instance.get_user_id(&user).await.unwrap();
                let password_hash = instance.get_user_password_hash(&user).await.unwrap();
                assert!(instance.delete_user(&user).await.success);
                let mut replacement = User::new("alice", None);
                replacement.password_hash = password_hash;
                replacement.attributes = Some(
                    Value::from_json_str(r#"{"role":"operator"}"#)
                        .unwrap()
                        .to_map(),
                );
                assert!(instance.register_user(&mut replacement).await.success);
                assert_ne!(instance.get_user_id(&replacement).await.unwrap(), old_id);
            }
            _ => unreachable!(),
        }
        if change == "role" {
            let original = auth::load_user(&state, "alice").await.unwrap().unwrap();
            let current =
                access::access_of_fresh(&state, original, &auth::Scopes::List(vec!["*".into()]))
                    .await
                    .unwrap();
            assert!(
                !current.has("pipes:write"),
                "the fixture must actually narrow the original role"
            );
        }
        assert!(
            auth::jwt_decode(&credential.token, common::SECRET, chrono::Utc::now()).is_ok(),
            "The presented JWT remains unexpired for {change}."
        );
        release.notify_one();
        let response = tokio::time::timeout(std::time::Duration::from_secs(5), response)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            response.status,
            StatusCode::UNAUTHORIZED,
            "{change}: {}",
            response.text()
        );
    }
}
