//! A child keeps the exact accepted source set, including after Team grants grow.
mod common;

use axum::http::HeaderMap;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::{Map, Value};
use meerschaum_server::executor::{
    callbacks, plugin_delivery, plugins,
    source_ceiling::{Ceiling, Fingerprint},
};
use std::collections::BTreeMap;
use uuid::Uuid;

fn map(value: serde_json::Value) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}
fn source(name: &str, byte: char) -> Ceiling {
    Ceiling {
        sources: BTreeMap::from([(
            name.into(),
            Fingerprint::Archive {
                source_kind: plugins::SourceKind::PythonModule,
                archive_sha256: byte.to_string().repeat(64),
            },
        )]),
    }
}

#[tokio::test]
async fn nested_sources_never_adopt_new_team_grants_changed_content_or_legacy_missing_capture() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(root.path()));
    instance.seed_user("alice", "pass", None);
    instance.seed_user_attributes(
        "alice",
        map(serde_json::json!({"role":"operator","primary_team":"Harbor"})),
    );
    let mut team = Team::new("Harbor");
    team.attributes =
        map(serde_json::json!({"isolated":true,"plugins_write":true,"plugins":["a"]}));
    team.members.insert("alice".into(), TeamRole::Member);
    let registered = instance.register_team(&mut team).await;
    assert!(registered.success, "{registered}");
    let id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
    let generation = Uuid::new_v4();
    callbacks::set_generation(root.path(), id, Some(generation)).unwrap();
    let app = meerschaum_server::app(state.clone());
    let bearer = common::login(&app, "alice", "pass", None).await;
    let mut headers = HeaderMap::new();
    headers.insert("authorization", bearer.parse().unwrap());
    let direct = meerschaum_server::access::require_fresh(&state, &headers, &["jobs:execute"])
        .await
        .unwrap();
    let parent = callbacks::register_with_sources(
        &state,
        &direct,
        Uuid::new_v4(),
        Some(id),
        &["jobs:execute"],
        callbacks::Purpose::Jobs,
        source("a", 'a'),
    )
    .await
    .unwrap();
    team.attributes
        .insert("plugins".into(), Value::List(vec!["a".into(), "b".into()]));
    assert!(instance.edit_team(&team).await.success);
    let identity = callbacks::load_identity(&state, parent.id, parent.launch_id)
        .await
        .unwrap();
    let child = identity.caller();
    assert!(child.principal.grants.plugins.contains("a"));
    assert!(!child.principal.grants.plugins.contains("b"));
    assert!(
        callbacks::register_with_sources(
            &state,
            &child,
            Uuid::new_v4(),
            Some(id),
            &["jobs:execute"],
            callbacks::Purpose::Jobs,
            source("b", 'b')
        )
        .await
        .is_err()
    );
    assert!(
        callbacks::register_with_sources(
            &state,
            &child,
            Uuid::new_v4(),
            Some(id),
            &["jobs:execute"],
            callbacks::Purpose::Jobs,
            source("a", 'c')
        )
        .await
        .is_err()
    );
    assert!(
        plugin_delivery::issue(
            &state,
            parent.id,
            parent.launch_id,
            id,
            generation,
            "b",
            &"b".repeat(64)
        )
        .await
        .is_err()
    );
    plugin_delivery::issue(
        &state,
        parent.id,
        parent.launch_id,
        id,
        generation,
        "a",
        &"a".repeat(64),
    )
    .await
    .unwrap();
    let nested = callbacks::register_with_sources(
        &state,
        &child,
        Uuid::new_v4(),
        Some(id),
        &["jobs:execute"],
        callbacks::Purpose::Jobs,
        source("a", 'a'),
    )
    .await
    .unwrap();
    assert_eq!(nested.source_ceiling, parent.source_ceiling);
    assert_eq!(
        callbacks::renew(&state, nested.id, nested.launch_id)
            .await
            .unwrap()
            .source_ceiling,
        nested.source_ceiling
    );
    callbacks::suspend(&state, nested.id, nested.launch_id)
        .await
        .unwrap();
    let resumed = callbacks::resume(&state, nested.id, nested.launch_id)
        .await
        .unwrap();
    assert_eq!(resumed.source_ceiling, nested.source_ceiling);
    callbacks::load_identity(&state, resumed.id, resumed.launch_id)
        .await
        .unwrap();
    // Fresh firstparty requests may accept newly granted B in a separate launch.
    let direct = meerschaum_server::access::require_fresh(&state, &headers, &["jobs:execute"])
        .await
        .unwrap();
    callbacks::register_with_sources(
        &state,
        &direct,
        Uuid::new_v4(),
        Some(id),
        &["jobs:execute"],
        callbacks::Purpose::Jobs,
        source("b", 'b'),
    )
    .await
    .unwrap();
    // Simulate a pre-capture private record; new Team names do not supply missing authority.
    let path = root
        .path()
        .join("executors/callbacks")
        .join(format!("{}.json", parent.id));
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    value.as_object_mut().unwrap().remove("source_ceiling");
    meerschaum_core::team_scope::write_private(&path, &value.to_string()).unwrap();
    let legacy = callbacks::load_identity(&state, parent.id, parent.launch_id)
        .await
        .unwrap()
        .caller();
    assert!(legacy.principal.grants.plugins.is_empty());
    assert!(
        callbacks::register_with_sources(
            &state,
            &legacy,
            Uuid::new_v4(),
            Some(id),
            &["jobs:execute"],
            callbacks::Purpose::Jobs,
            source("a", 'a')
        )
        .await
        .is_err()
    );
    callbacks::register(
        &state,
        &legacy,
        Uuid::new_v4(),
        Some(id),
        &["jobs:execute"],
        callbacks::Purpose::Jobs,
    )
    .await
    .unwrap();
    team.attributes
        .insert("plugins".into(), Value::List(vec!["b".into()]));
    assert!(instance.edit_team(&team).await.success);
    assert!(
        callbacks::load_identity(&state, resumed.id, resumed.launch_id)
            .await
            .is_err()
    );
}
