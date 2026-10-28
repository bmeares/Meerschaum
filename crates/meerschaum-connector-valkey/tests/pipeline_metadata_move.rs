//! Atomic live metadata moves retain authority; failed/stale moves never recreate a source.
use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::PipesKeysQuery;
use meerschaum_connector_valkey::{FredBackend, MemoryBackend, ValkeyConnector};
use meerschaum_core::{Map, Pipe, Value};
use serde_json::json;

fn parameters(name: &str, identity: &str) -> Map {
    serde_json::from_value::<Value>(json!({"pipeline":{"name":name,"id":identity},"access":{"owner":"alice","visibility":"private"}})).unwrap().to_map()
}
fn pipe(name: &str, parameters: Map) -> Pipe {
    Pipe::builder("mrsm", "pipeline")
        .location(Some(name))
        .instance("valkey:move")
        .parameters(parameters)
        .build()
        .unwrap()
}

#[tokio::test]
async fn atomic_move_preserves_registration_uuid_access_and_refuses_stale_or_forged_authority() {
    let backend = Arc::new(MemoryBackend::new());
    let conn = Arc::new(ValkeyConnector::with_backend("move", Map::new(), backend));
    let identity = uuid::Uuid::new_v4().to_string();
    let source = pipe("old", parameters("old", &identity));
    assert!(conn.register_pipe(&source).await.success);
    let expected = conn.get_pipe_attributes(&source).await.unwrap();
    let updated = parameters("new", &identity);
    let mut forged = updated.clone();
    forged.insert("access".into(), Value::Map(Map::new()));
    assert!(
        conn.move_pipeline_registration(&source, "new", &expected, &forged)
            .await
            .is_err()
    );
    assert!(
        conn.move_pipeline_registration(
            &source,
            "new",
            &expected,
            &parameters("new", &uuid::Uuid::new_v4().to_string())
        )
        .await
        .is_err()
    );
    assert!(
        conn.move_pipeline_registration(&source, "new", &expected, &updated)
            .await
            .unwrap()
    );
    let destination = pipe("new", Map::new());
    let moved = conn.get_pipe_attributes(&destination).await.unwrap();
    assert_eq!(moved.get("pipe_id"), expected.get("pipe_id"));
    assert_eq!(moved.get("parameters"), Some(&Value::Map(updated)));
    assert!(conn.get_pipe_attributes(&source).await.unwrap().is_empty());
    assert!(
        !conn
            .move_pipeline_registration(
                &source,
                "third",
                &expected,
                &parameters("third", &identity)
            )
            .await
            .unwrap()
    );
    assert!(conn.register_pipe(&source).await.success);
    assert_ne!(
        conn.get_pipe_attributes(&source)
            .await
            .unwrap()
            .get("pipe_id"),
        expected.get("pipe_id")
    );
    assert!(
        !conn
            .move_pipeline_registration(
                &source,
                "third",
                &expected,
                &parameters("third", &identity)
            )
            .await
            .unwrap()
    );
    let keys = conn
        .fetch_pipes_keys(&PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(keys.len(), 2);
}

#[tokio::test]
async fn concurrent_moves_have_one_winner_and_changed_or_occupied_metadata_is_untouched() {
    let conn = Arc::new(ValkeyConnector::with_backend(
        "move",
        Map::new(),
        Arc::new(MemoryBackend::new()),
    ));
    let identity = uuid::Uuid::new_v4().to_string();
    let mut source = pipe("old", parameters("old", &identity));
    assert!(conn.register_pipe(&source).await.success);
    let expected = conn.get_pipe_attributes(&source).await.unwrap();
    let mut changed = source.raw_parameters().clone();
    changed.insert("description".into(), Value::Str("changed".into()));
    source.set_parameters(changed);
    assert!(conn.edit_pipe(&source).await.success);
    assert!(
        !conn
            .move_pipeline_registration(&source, "one", &expected, &parameters("one", &identity))
            .await
            .unwrap()
    );
    let expected = conn.get_pipe_attributes(&source).await.unwrap();
    let mut one = source.raw_parameters().clone();
    one.get_mut("pipeline")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .insert("name".into(), Value::Str("one".into()));
    let mut two = one.clone();
    two.get_mut("pipeline")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .insert("name".into(), Value::Str("two".into()));
    let (a, b) = tokio::join!(
        conn.move_pipeline_registration(&source, "one", &expected, &one),
        conn.move_pipeline_registration(&source, "two", &expected, &two)
    );
    assert_ne!(a.unwrap(), b.unwrap());
    let remaining = if conn
        .get_pipe_id(&pipe("one", Map::new()))
        .await
        .unwrap()
        .is_some()
    {
        "one"
    } else {
        "two"
    };
    let occupied = pipe(
        "occupied",
        parameters("occupied", &uuid::Uuid::new_v4().to_string()),
    );
    assert!(conn.register_pipe(&occupied).await.success);
    let live = pipe(remaining, Map::new());
    let expected = conn.get_pipe_attributes(&live).await.unwrap();
    assert!(
        !conn
            .move_pipeline_registration(
                &live,
                "occupied",
                &expected,
                &parameters("occupied", &identity)
            )
            .await
            .unwrap()
    );
    assert!(
        conn.get_pipe_attributes(&live)
            .await
            .unwrap()
            .get("pipe_id")
            .is_some()
    );
}

/// No default address: this test only touches an explicitly supplied disposable instance.
#[tokio::test]
async fn live_lua_move_preserves_storage_and_refuses_stale_recreated_and_occupied_sources() {
    let Ok(url) = std::env::var("MRSM_VALKEY_TEST_URL") else {
        eprintln!("SKIPPED: set MRSM_VALKEY_TEST_URL to an owned disposable Valkey instance");
        return;
    };
    assert_eq!(std::env::var("MRSM_TEST_ISOLATED").as_deref(), Ok("1"));
    let backend = Arc::new(
        FredBackend::connect(&url, Some(Duration::from_secs(3)))
            .await
            .expect("the explicitly supplied disposable Valkey must be reachable"),
    );
    let conn = ValkeyConnector::with_backend("move", Map::new(), backend.clone());
    let prefix = uuid::Uuid::new_v4().simple().to_string();
    let old = format!("{prefix}-old");
    let one = format!("{prefix}-one");
    let two = format!("{prefix}-two");
    let occupied_name = format!("{prefix}-occupied");
    let identity = uuid::Uuid::new_v4().to_string();
    let source = pipe(&old, parameters(&old, &identity));
    assert!(conn.register_pipe(&source).await.success);
    let expected = conn.get_pipe_attributes(&source).await.unwrap();
    let first = parameters(&one, &identity);
    let second = parameters(&two, &identity);
    let (a, b) = tokio::join!(
        conn.move_pipeline_registration(&source, &one, &expected, &first),
        conn.move_pipeline_registration(&source, &two, &expected, &second)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_ne!(
        a, b,
        "the Lua compare-and-move must have exactly one winner"
    );
    let (winner_name, loser_name, winner_parameters) = if a {
        (&one, &two, first)
    } else {
        (&two, &one, second)
    };
    let winner = pipe(winner_name, Map::new());
    let moved = conn.get_pipe_attributes(&winner).await.unwrap();
    assert_eq!(moved.get("pipe_id"), expected.get("pipe_id"));
    assert_eq!(
        moved.get("parameters"),
        Some(&Value::Map(winner_parameters))
    );
    assert!(conn.get_pipe_attributes(&source).await.unwrap().is_empty());
    assert!(
        conn.get_pipe_attributes(&pipe(loser_name, Map::new()))
            .await
            .unwrap()
            .is_empty()
    );
    let rows = conn
        .fetch_pipes_keys(&PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            location_keys: vec![Some(old.clone()), Some(one.clone()), Some(two.clone())],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the registration sorted set moves with its keys"
    );

    let occupied = pipe(
        &occupied_name,
        parameters(&occupied_name, &uuid::Uuid::new_v4().to_string()),
    );
    assert!(conn.register_pipe(&occupied).await.success);
    assert!(
        !conn
            .move_pipeline_registration(
                &winner,
                &occupied_name,
                &moved,
                &parameters(&occupied_name, &identity)
            )
            .await
            .unwrap()
    );
    assert_eq!(conn.get_pipe_attributes(&winner).await.unwrap(), moved);
    assert!(conn.register_pipe(&source).await.success);
    assert_ne!(
        conn.get_pipe_attributes(&source)
            .await
            .unwrap()
            .get("pipe_id"),
        expected.get("pipe_id")
    );
    assert!(
        !conn
            .move_pipeline_registration(
                &source,
                loser_name,
                &expected,
                &parameters(loser_name, &identity)
            )
            .await
            .unwrap()
    );
    for registration in [&source, &winner, &occupied] {
        assert!(conn.delete_pipe(registration).await.success);
    }
    backend.quit().await.unwrap();
}
