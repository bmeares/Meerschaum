//! Scoped callers must not silently use an unguarded queued write implementation.
use std::sync::Arc;

use meerschaum_connector::mutation::{MutationCheck, scope};
use meerschaum_connector::{DataQuery, SyncOptions};
use meerschaum_connector_valkey::ValkeyConnector;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::docs::frame_from_docs;

struct Admitted;

#[meerschaum_connector::async_trait]
impl MutationCheck for Admitted {
    async fn check(&self) -> Result<(), String> {
        Ok(())
    }
}

#[tokio::test]
async fn scoped_valkey_mutations_refuse_before_any_changes_and_local_pool_remains_usable() {
    let connector = ValkeyConnector::in_memory("authority");
    let mut pipe = Pipe::builder("manual:posted", "scope")
        .instance("valkey:authority")
        .build()
        .unwrap();
    pipe.set_parameters(
        Value::from_json_str(r#"{"columns":{"id":"id"}}"#)
            .unwrap()
            .to_map(),
    );
    assert!(connector.register_pipe(&pipe).await.success);
    let rows = || {
        frame_from_docs(&[[("id".into(), meerschaum_kernel::value::Value::Int(1))]
            .into_iter()
            .collect()])
        .unwrap()
    };
    assert!(
        connector
            .sync_pipe(&pipe, rows(), &SyncOptions::default())
            .await
            .success
    );
    let before = connector.get_pipe_attributes(&pipe).await.unwrap();
    let new = Pipe::builder("manual:posted", "new")
        .instance("valkey:authority")
        .build()
        .unwrap();
    let mut changed = pipe.clone();
    changed.update_parameters(Map::from_iter([("tags".into(), Value::from("changed"))]));
    scope(Arc::new(Admitted), async {
        for result in [
            connector.register_pipe(&new).await,
            connector.edit_pipe(&changed).await,
            connector
                .sync_pipe(&pipe, rows(), &SyncOptions::default())
                .await,
            connector.clear_pipe(&pipe, &DataQuery::default()).await,
            connector.drop_pipe(&pipe).await,
            connector.delete_pipe(&pipe).await,
        ] {
            assert!(!result.success, "{result:?}");
            assert!(result.message.contains("after server lock acquisition"));
        }
    })
    .await;
    assert_eq!(connector.get_pipe_attributes(&pipe).await.unwrap(), before);
    assert!(
        connector
            .get_pipe_attributes(&new)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        connector
            .get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .unwrap(),
        Some(1)
    );
    assert!(
        connector
            .clear_pipe(&pipe, &DataQuery::default())
            .await
            .success
    );
    assert_eq!(
        connector
            .get_pipe_data(&pipe, &DataQuery::default())
            .await
            .unwrap()
            .map(|frame| frame.height())
            .unwrap_or(0),
        0
    );
}
