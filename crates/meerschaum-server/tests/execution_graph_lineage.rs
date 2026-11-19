//! Main-only graph reservation never exports an incomplete bearer or loses a
//! parent's window when deriving an otherwise ordinary handler callback.
mod common;

use axum::http::HeaderMap;
use meerschaum_core::{Map, Value};
use meerschaum_server::{
    AppState,
    access::Access,
    auth::Principal,
    executor::{
        callbacks::{self, Purpose},
        pipe_operation::CallbackBinding,
        source_ceiling::Ceiling,
    },
};
use uuid::Uuid;

fn map(value: serde_json::Value) -> Map {
    Value::from_json_str(&value.to_string()).unwrap().to_map()
}
async fn caller(state: &AppState) -> Access {
    let app = meerschaum_server::app(state.clone());
    let bearer = common::login(&app, "alice", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", bearer.parse().unwrap());
    meerschaum_server::access::resolve(state, &headers)
        .await
        .unwrap()
}
async fn authenticated(state: &AppState, grant: &callbacks::Grant) -> Access {
    let credential = callbacks::credential(state, grant.id, grant.launch_id)
        .await
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        "authorization",
        format!("Bearer {}", credential.token).parse().unwrap(),
    );
    meerschaum_server::access::resolve(state, &headers)
        .await
        .unwrap()
}
async fn pending(state: &AppState, caller: &Access) -> callbacks::Grant {
    callbacks::register_graph_with_sources(
        state,
        caller,
        Uuid::new_v4(),
        None,
        &["pipes:write"],
        Ceiling::default(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn incomplete_roots_cannot_issue_bearers_and_derived_handlers_cannot_drop_bound_origin() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes("alice", map(serde_json::json!({"role":"operator"})));
    let source = caller(&state).await;
    let reservation = pending(&state, &source).await;
    assert!(reservation.graph_pending && reservation.graph_origin.is_none());
    assert!(
        callbacks::credential(&state, reservation.id, reservation.launch_id)
            .await
            .is_err()
    );
    let not_ready = callbacks::load_identity(&state, reservation.id, reservation.launch_id)
        .await
        .unwrap()
        .caller();
    assert!(
        callbacks::register(
            &state,
            &not_ready,
            Uuid::new_v4(),
            None,
            &["pipes:write"],
            Purpose::Command
        )
        .await
        .is_err(),
        "An incomplete Main-only graph became a runtime parent"
    );
    // The trusted Controller invokes this ONLY after atomic plan/window issue.
    let bound =
        callbacks::bind_graph_origin(&state, reservation.id, reservation.launch_id, 1, None)
            .await
            .unwrap();
    let expected = CallbackBinding {
        grant: bound.id,
        launch: bound.launch_id,
        context_revision: 1,
    };
    assert_eq!(bound.graph_origin, Some(expected.clone()));
    assert!(!bound.graph_pending);
    let parent = authenticated(&state, &bound).await;
    let child = callbacks::register(
        &state,
        &parent,
        Uuid::new_v4(),
        None,
        &["pipes:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    assert_eq!(child.graph_origin, Some(expected));
    assert_eq!(child.depth, bound.depth + 1);
    assert_eq!(
        child.permissions, bound.permissions,
        "Derivation synthesized or removed source authority"
    );
    assert!(child.required.contains("pipes:write") && child.required.contains("pipes:read"));
    assert!(
        callbacks::bind_graph_origin(&state, child.id, child.launch_id, 1, None)
            .await
            .is_err(),
        "A derived handler replaced its ancestor graph with a fresh root"
    );
    let child_access = authenticated(&state, &child).await;
    assert!(
        state
            .actions
            .pipe_operation_read(&child_access)
            .await
            .is_err(),
        "A backend without graph enforcement silently fell back to broad read scopes"
    );
}

#[tokio::test]
async fn proven_nongraph_handlers_keep_normal_semantics_but_stale_legacy_lineage_does_not() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes("alice", map(serde_json::json!({"role":"operator"})));
    let source = caller(&state).await;
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
    let parent = authenticated(&state, &grant).await;
    let child = callbacks::register(
        &state,
        &parent,
        Uuid::new_v4(),
        None,
        &["pipes:read"],
        Purpose::Command,
    )
    .await
    .unwrap();
    assert!(child.graph_origin.is_none() && !child.graph_pending && child.graph_tracking == 1);
    let child_access = authenticated(&state, &child).await;
    assert!(
        state
            .actions
            .pipe_operation_read(&child_access)
            .await
            .unwrap()
            .is_none()
    );
    let mut legacy = serde_json::to_value(&grant).unwrap();
    legacy.as_object_mut().unwrap().remove("graph_tracking");
    meerschaum_core::team_scope::write_private(
        &state
            .config
            .root_dir
            .join("executors/callbacks")
            .join(format!("{}.json", grant.id)),
        &serde_json::to_string(&legacy).unwrap(),
    )
    .unwrap();
    // The previously authenticated Access still says version1. Registration must
    // reload the private parent, rather than trust that stale captured marker.
    assert!(
        callbacks::register(
            &state,
            &parent,
            Uuid::new_v4(),
            None,
            &["pipes:read"],
            Purpose::Command
        )
        .await
        .is_err()
    );
    let loaded = callbacks::load_identity(&state, grant.id, grant.launch_id)
        .await
        .unwrap();
    assert_eq!(loaded.grant.graph_tracking, 0);
    assert!(
        state
            .actions
            .pipe_operation_read(&loaded.caller())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn restart_needs_its_exact_previous_binding_and_fresh_original_permissions() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes("alice", map(serde_json::json!({"role":"operator"})));
    let source = caller(&state).await;
    let first = pending(&state, &source).await;
    let first = callbacks::bind_graph_origin(&state, first.id, first.launch_id, 1, None)
        .await
        .unwrap();
    let previous = first.graph_origin.clone().unwrap();
    let next = callbacks::resume(&state, first.id, first.launch_id)
        .await
        .unwrap();
    assert!(next.graph_pending && next.graph_origin == Some(previous.clone()));
    assert!(
        callbacks::credential(&state, next.id, next.launch_id)
            .await
            .is_err()
    );
    let mut wrong = previous.clone();
    wrong.context_revision += 1;
    assert!(
        callbacks::bind_graph_origin(&state, next.id, next.launch_id, 2, Some(wrong))
            .await
            .is_err()
    );
    let next = callbacks::bind_graph_origin(&state, next.id, next.launch_id, 2, Some(previous))
        .await
        .unwrap();
    assert_eq!(
        next.graph_origin,
        Some(CallbackBinding {
            grant: next.id,
            launch: next.launch_id,
            context_revision: 2
        })
    );
    assert!(matches!(
        authenticated(&state, &next).await.auth,
        Some(Principal::Execution(_))
    ));
    let late = pending(&state, &source).await;
    instance.seed_user_attributes("alice", map(serde_json::json!({"role":"viewer"})));
    assert!(
        callbacks::bind_graph_origin(&state, late.id, late.launch_id, 1, None)
            .await
            .is_err()
    );
    assert!(
        callbacks::credential(&state, late.id, late.launch_id)
            .await
            .is_err()
    );
}
