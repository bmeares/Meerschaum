//! Typed pipe endpoints never turn their write permission into generic execution authority.
mod common;

use axum::http::HeaderMap;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_server::executor::callbacks::{self, Purpose};
use meerschaum_server::executor::pipe_operation::PipeOperation;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

#[tokio::test]
async fn typed_pipe_admission_tracks_original_registration_and_fresh_endpoint_permissions() {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"custom","scopes":["pipes:read","pipes:write"]})),
    );
    let mut pipe = Pipe::builder("manual:fixture", "selected")
        .instance("sql:fixture")
        .parameters(map(
            json!({"target":"allowed_target","access":{"owner":"alice","visibility":"private"}}),
        ))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let state = meerschaum_server::AppState::builder(common::config(root.path()), instance.clone())
        .secret(common::SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state.clone());
    let bearer = common::login(&app, "alice", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", bearer.parse().unwrap());
    let source = meerschaum_server::access::resolve(&state, &headers)
        .await
        .unwrap();
    assert!(!source.has("actions:execute") && !source.has("jobs:execute"));
    let operation = PipeOperation {
        row_params: None,
        pipe_ref: pipe.pipe_ref(),
        registration: instance
            .get_pipe_id(&pipe)
            .await
            .unwrap()
            .unwrap()
            .to_value(),
        verify: false,
        begin: None,
        end: None,
        parents: None,
        children: None,
        metadata: None,
    };
    let words = operation.sysargs().unwrap();
    operation.authorize(&state, &source, &words).await.unwrap();
    let grant = callbacks::register(
        &state,
        &source,
        Uuid::new_v4(),
        None,
        &["pipes:write"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let identity = callbacks::load_identity(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    assert!(!identity.caller().has("actions:execute") && !identity.caller().has("jobs:execute"));
    operation
        .authorize(&state, &identity.caller(), &words)
        .await
        .unwrap();
    let mut changed = words.clone();
    changed.extend(["--shell".into(), "--loop".into()]);
    assert!(
        operation
            .authorize(&state, &identity.caller(), &changed)
            .await
            .is_err()
    );
    let original = operation.registration.clone();
    assert!(instance.delete_pipe(&pipe).await.success);
    assert!(instance.register_pipe(&pipe).await.success);
    assert_ne!(
        instance
            .get_pipe_id(&pipe)
            .await
            .unwrap()
            .unwrap()
            .to_value(),
        original
    );
    assert!(
        operation
            .authorize(&state, &identity.caller(), &words)
            .await
            .is_err(),
        "A replacement pipe adopted old endpoint authority"
    );
    let mut replacement = operation.clone();
    replacement.registration = instance
        .get_pipe_id(&pipe)
        .await
        .unwrap()
        .unwrap()
        .to_value();
    replacement
        .authorize(&state, &identity.caller(), &replacement.sysargs().unwrap())
        .await
        .unwrap();
    pipe.set_parameters(map(
        json!({"target":"mrsm_users","access":{"owner":"alice","visibility":"private"}}),
    ));
    assert!(instance.edit_pipe(&pipe, false).await.success);
    assert!(
        replacement
            .authorize(&state, &identity.caller(), &replacement.sysargs().unwrap())
            .await
            .is_err(),
        "A registered internal target bypassed pipe SQL protection"
    );
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"custom","scopes":["pipes:read"]})),
    );
    assert!(
        callbacks::load_identity(&state, grant.id, grant.launch_id)
            .await
            .is_err(),
        "Endpoint permission revocation kept renewing original authority"
    );
}

#[tokio::test]
async fn a_unique_location_resolves_from_the_configured_default_instance() {
    use meerschaum_server::executor::pipe_operation::{DirectAction, StoredIntent};
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("operator", "pass", None);
    instance.seed_user_attributes(
        "operator",
        map(json!({"role":"custom","scopes":["actions:execute","pipes:write"]})),
    );
    let app = meerschaum_server::app(state.clone());
    let token = common::login(&app, "operator", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", token.parse().unwrap());
    let caller = meerschaum_server::access::resolve(&state, &headers)
        .await
        .unwrap();
    let scoped = |location: Option<&str>| {
        Pipe::builder("sql:fixture", "weather")
            .instance("sql:fixture")
            .location(location)
            .build()
            .unwrap()
    };
    let station = scoped(Some("north"));
    assert!(instance.register_pipe(&station).await.success);
    let mut kw =
        map(json!({"action":["pipes"],"connector_keys":["sql:fixture"],"metric_keys":["weather"]}));
    let input = DirectAction::capture("verify", &kw).unwrap().unwrap();
    assert_eq!(
        input.resolve(&state, &caller).await.unwrap().pipe_ref,
        station.pipe_ref()
    );
    let stored = StoredIntent::capture(
        &["sync", "pipes", "-c", "sql:fixture", "-m", "weather"].map(str::to_owned),
    )
    .unwrap();
    assert_eq!(
        stored
            .action()
            .unwrap()
            .resolve(&state, &caller)
            .await
            .unwrap()
            .pipe_ref,
        station.pipe_ref()
    );

    let unlocated = scoped(None);
    assert!(instance.register_pipe(&unlocated).await.success);
    assert!(input.resolve(&state, &caller).await.is_err());
    kw.insert(
        "location_keys".into(),
        Value::List(vec![Value::Str("*".into())]),
    );
    let all = DirectAction::capture("sync", &kw).unwrap().unwrap();
    assert!(all.resolve(&state, &caller).await.is_err());
    kw.insert("location_keys".into(), Value::List(vec![Value::Null]));
    let selected = DirectAction::capture("sync", &kw).unwrap().unwrap();
    assert_eq!(
        selected.resolve(&state, &caller).await.unwrap().pipe_ref,
        unlocated.pipe_ref()
    );
}
