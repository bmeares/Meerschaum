//! A protected instance (`protected: true`, rc15 Q20) refuses a sync before its source is
//! fetched: a large fetch never runs only to have its write refused.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::*;
use meerschaum_connector::{Connector, ConnectorResult, DataQuery, InstanceConnector, async_trait};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::Pipe;
use meerschaum_engine::{Engine, StaticRegistry, SyncArgs};
use meerschaum_kernel::Frame;

/// A source that counts its fetches (and has nothing to give).
struct Counted(Arc<AtomicUsize>);

#[async_trait]
impl Connector for Counted {
    fn connector_type(&self) -> &str {
        "plugin"
    }

    fn label(&self) -> &str {
        "counted"
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }

    async fn fetch(&self, _pipe: &Pipe, _query: &DataQuery) -> ConnectorResult<Option<Frame>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protected_instances_refuse_syncs_before_fetching() {
    assert!(std::env::var("MRSM_ALLOW_PROTECTED").is_err());
    let env = env("f2_protected_sync").await;
    let protected = SqlConnector::from_uri(
        "eng",
        &format!("sqlite:///{}?protected=true", env.db.display()),
    )
    .await
    .unwrap();
    assert!(protected.is_protected());
    let fetches = Arc::new(AtomicUsize::new(0));
    let instance: Arc<dyn InstanceConnector> = Arc::new(protected);
    let source: Arc<dyn Connector> = Arc::new(Counted(fetches.clone()));
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance(INSTANCE, instance)
            .with_source("plugin:counted", source),
    );
    let engine = Engine::new(registry, env.config());
    let pipe = Pipe::builder("plugin:counted", "readings")
        .instance(INSTANCE)
        .build()
        .unwrap();

    let result = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(!result.success);
    assert!(
        result
            .message
            .starts_with("Refusing to sync Pipe('plugin:counted', 'readings'"),
        "{}",
        result.message
    );
    assert!(
        result.message.ends_with(
            "on protected instance 'sql:eng'. Pass --allow-protected (or set MRSM_ALLOW_PROTECTED=1)."
        ),
        "{}",
        result.message
    );
    assert_eq!(fetches.load(Ordering::SeqCst), 0, "the source was fetched");

    // Allowed, the sync goes ahead and fetches.
    {
        let _allowed = meerschaum_core::protected::allow(true);
        let _ = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    }
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
}
