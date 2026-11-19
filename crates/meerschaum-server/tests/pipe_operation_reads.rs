//! Write callbacks borrow exact operation primitives without acquiring a general read scope.
mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use axum::http::{HeaderMap, StatusCode};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_server::AppState;
use meerschaum_server::access::Access;
use meerschaum_server::auth::Principal;
use meerschaum_server::backends::ActionsBackend;
use meerschaum_server::executor::callbacks::{self, Purpose};
use meerschaum_server::executor::pipe_operation::{
    PipeOperation, ReadAccess, Snapshot, SnapshotMember,
};
use serde_json::json;
use uuid::Uuid;

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}

struct AcceptedOperation {
    state: OnceLock<AppState>,
    launch: Mutex<Option<(Uuid, Uuid)>>,
    operation: PipeOperation,
}

#[tokio::test]
async fn captured_graph_cannot_edit_its_registered_definition_through_the_public_mutation_route() {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"custom","scopes":["pipes:write"]})),
    );
    let pipe = Pipe::builder("manual:fixture", "guarded_mutation")
        .instance("sql:fixture")
        .parameters(map(
            json!({"target":"original_target","access":{"owner":"alice","visibility":"private"}}),
        ))
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    let registration = instance
        .get_pipe_id(&pipe)
        .await
        .unwrap()
        .unwrap()
        .to_value();
    let operation = PipeOperation {
        row_params: None,
        pipe_ref: pipe.pipe_ref(),
        registration: registration.clone(),
        verify: false,
        begin: None,
        end: None,
        parents: None,
        children: None,
        metadata: Some(
            Snapshot::new(BTreeMap::from([(
                pipe.pipe_ref(),
                SnapshotMember {
                    registration,
                    parameters: pipe.raw_parameters().clone(),
                    writable: true,
                    node_window: None,
                },
            )]))
            .unwrap(),
        ),
    };
    let accepted = Arc::new(AcceptedOperation {
        state: OnceLock::new(),
        launch: Mutex::new(None),
        operation,
    });
    let state = AppState::builder(common::config(root.path()), instance.clone())
        .secret(common::SECRET.to_vec())
        .actions(accepted.clone())
        .build()
        .unwrap();
    assert!(accepted.state.set(state.clone()).is_ok());
    let app = meerschaum_server::app(state.clone());
    let original = common::login(&app, "alice", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", original.parse().unwrap());
    let source = meerschaum_server::access::resolve(&state, &headers)
        .await
        .unwrap();
    let grant = callbacks::register_graph_with_sources(
        &state,
        &source,
        Uuid::new_v4(),
        None,
        &["pipes:write"],
        Default::default(),
    )
    .await
    .unwrap();
    let grant = callbacks::bind_graph_origin(&state, grant.id, grant.launch_id, 1, None)
        .await
        .unwrap();
    *accepted.launch.lock().unwrap() = Some((grant.id, grant.launch_id));
    let identity = callbacks::load_identity(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    assert!(identity.caller().has("pipes:write"));
    let token = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let callback = format!("Bearer {}", token.token);
    let path = "/pipes/manual:fixture/guarded_mutation/None/edit?patch=true";
    let before = instance.get_pipe_attributes(&pipe).await.unwrap();
    let denied = common::send(
        &app,
        common::req(
            "PATCH",
            path,
            Some(&callback),
            Some((
                "application/json",
                serde_json::to_vec(&json!({"target":"forbidden_target"})).unwrap(),
            )),
        ),
    )
    .await;
    assert_eq!(
        denied.status,
        StatusCode::NOT_IMPLEMENTED,
        "{}",
        denied.text()
    );
    assert!(
        denied
            .text()
            .contains("native schema/maintenance instruction")
    );
    assert_eq!(
        instance.get_pipe_attributes(&pipe).await.unwrap(),
        before,
        "A denied graph mutation reached the backend"
    );
    let ordinary = common::send(
        &app,
        common::req(
            "PATCH",
            path,
            Some(&original),
            Some((
                "application/json",
                serde_json::to_vec(&json!({"target":"ordinary_target"})).unwrap(),
            )),
        ),
    )
    .await;
    assert_eq!(ordinary.status, StatusCode::OK, "{}", ordinary.text());
    assert_ne!(
        instance.get_pipe_attributes(&pipe).await.unwrap(),
        before,
        "The proven nongraph route did not retain ordinary edits"
    );
}
#[async_trait]
impl ActionsBackend for AcceptedOperation {
    fn list(&self) -> Vec<String> {
        vec!["sync".into()]
    }
    async fn execute(&self, _: &str, _: Map) -> SuccessTuple {
        SuccessTuple::fail("Unused")
    }
    async fn pipe_operation_read(&self, caller: &Access) -> Result<Option<ReadAccess>, String> {
        let Some(Principal::Execution(identity)) = &caller.auth else {
            return Ok(None);
        };
        if *self.launch.lock().unwrap() != Some((identity.grant.id, identity.grant.launch_id)) {
            return Ok(None);
        }
        let state = self.state.get().unwrap();
        let fresh = callbacks::load_identity(state, identity.grant.id, identity.grant.launch_id)
            .await
            .map_err(|error| error.detail_text())?;
        let caller = fresh.caller();
        self.operation
            .authorize(state, &caller, &self.operation.sysargs().unwrap())
            .await
            .map_err(|error| error.detail_text())?;
        let binding = meerschaum_server::executor::pipe_operation::CallbackBinding {
            grant: identity.grant.id,
            launch: identity.grant.launch_id,
            context_revision: 1,
        };
        Ok(Some(ReadAccess {
            caller,
            operation: self.operation.clone().into(),
            binding: binding.clone(),
            issuer: binding,
            pending_nodes: Default::default(),
        }))
    }
}

#[tokio::test]
async fn write_only_callback_reads_only_its_accepted_registration_and_never_general_data() {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"custom","scopes":["pipes:write"]})),
    );
    let pipe = Pipe::builder("manual:fixture", "selected")
        .instance("sql:fixture")
        .parameters(map(
            json!({"columns":{"id":"id"},"access":{"owner":"alice","visibility":"private"}}),
        ))
        .build()
        .unwrap();
    let unrelated = Pipe::builder("manual:fixture", "unrelated")
        .instance("sql:fixture")
        .parameters(pipe.raw_parameters().clone())
        .build()
        .unwrap();
    assert!(instance.register_pipe(&pipe).await.success);
    assert!(instance.register_pipe(&unrelated).await.success);
    let registration = instance
        .get_pipe_id(&pipe)
        .await
        .unwrap()
        .unwrap()
        .to_value();
    let operation = PipeOperation {
        row_params: None,
        pipe_ref: pipe.pipe_ref(),
        registration: registration.clone(),
        verify: false,
        begin: None,
        end: None,
        parents: None,
        children: None,
        metadata: Some(
            Snapshot::new(BTreeMap::from([(
                pipe.pipe_ref(),
                SnapshotMember {
                    registration,
                    parameters: pipe.raw_parameters().clone(),
                    writable: true,
                    node_window: None,
                },
            )]))
            .unwrap(),
        ),
    };
    let accepted = Arc::new(AcceptedOperation {
        state: OnceLock::new(),
        launch: Mutex::new(None),
        operation,
    });
    let state = AppState::builder(common::config(root.path()), instance.clone())
        .secret(common::SECRET.to_vec())
        .actions(accepted.clone())
        .build()
        .unwrap();
    assert!(accepted.state.set(state.clone()).is_ok());
    let app = meerschaum_server::app(state.clone());
    let original = common::login(&app, "alice", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", original.parse().unwrap());
    let source = meerschaum_server::access::resolve(&state, &headers)
        .await
        .unwrap();
    assert!(!source.has("pipes:read") && !source.has("sql:read"));
    let grant = callbacks::register_graph_with_sources(
        &state,
        &source,
        Uuid::new_v4(),
        None,
        &["pipes:write"],
        Default::default(),
    )
    .await
    .unwrap();
    let grant = callbacks::bind_graph_origin(&state, grant.id, grant.launch_id, 1, None)
        .await
        .unwrap();
    *accepted.launch.lock().unwrap() = Some((grant.id, grant.launch_id));
    let credential = callbacks::credential(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let callback = format!("Bearer {}", credential.token);
    for permission in ["pipes:read", "sql:read", "actions:execute", "jobs:execute"] {
        assert!(
            !grant.permissions.contains(permission),
            "Borrowing an operation synthesized {permission}"
        );
    }
    let selected = "/pipes/manual:fixture/selected/None";
    let attributes = common::send(
        &app,
        common::req(
            "GET",
            &format!("{selected}/attributes"),
            Some(&callback),
            None,
        ),
    )
    .await;
    assert_eq!(attributes.status, StatusCode::OK, "{}", attributes.text());
    let catalog = common::send(
        &app,
        common::req("GET", "/pipes/keys?as_dict=true", Some(&callback), None),
    )
    .await;
    assert_eq!(catalog.status, StatusCode::OK, "{}", catalog.text());
    assert!(catalog.text().contains("selected") && !catalog.text().contains("unrelated"));
    for (token, path) in [
        (&original, format!("{selected}/attributes")),
        (&original, format!("{selected}/rowcount")),
        (
            &callback,
            "/pipes/manual:fixture/unrelated/None/attributes".into(),
        ),
        (&callback, format!("{selected}/data")),
        (&callback, format!("{selected}/docs")),
        (&callback, format!("{selected}/csv")),
        (&callback, "/engine/runs".into()),
    ] {
        let response = common::send(&app, common::req("GET", &path, Some(token), None)).await;
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "{path}: {}",
            response.text()
        );
    }
    // A second genuine callback from the same user has no accepted canonical operation.
    let unbound = callbacks::register(
        &state,
        &source,
        Uuid::new_v4(),
        None,
        &["pipes:write"],
        Purpose::Command,
    )
    .await
    .unwrap();
    let unbound = callbacks::credential(&state, unbound.id, unbound.launch_id)
        .await
        .unwrap();
    let response = common::send(
        &app,
        common::req(
            "GET",
            &format!("{selected}/attributes"),
            Some(&format!("Bearer {}", unbound.token)),
            None,
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::FORBIDDEN);
    assert!(instance.delete_pipe(&pipe).await.success);
    assert!(instance.register_pipe(&pipe).await.success);
    let response = common::send(
        &app,
        common::req(
            "GET",
            &format!("{selected}/attributes"),
            Some(&callback),
            None,
        ),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::FORBIDDEN,
        "A replacement registration adopted the read capability"
    );
    instance.seed_user_attributes(
        "alice",
        map(json!({"role":"custom","scopes":["pipes:read"]})),
    );
    let response = common::send(
        &app,
        common::req(
            "GET",
            &format!("{selected}/attributes"),
            Some(&callback),
            None,
        ),
    )
    .await;
    assert_ne!(
        response.status,
        StatusCode::OK,
        "Source revocation preserved operation reads"
    );
}
