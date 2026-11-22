//! Execution transports keep data comparison on Main while conservatively propagating dirt.

use crate::common::*;
use indexmap::IndexMap;
use meerschaum_connector::{
    Connector, ConnectorError, ConnectorResult, DataQuery, DateBound, InstanceConnector, PipeId,
    PipesKeys, PipesKeysQuery, SyncOptions, async_trait,
};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::{Map, Pipe, SuccessTuple};
use meerschaum_engine::{
    Bound, Depth, Engine, RunOptions, Selection, StaticRegistry, state::StateSource,
};
use meerschaum_kernel::{Frame, value::Value as KValue};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct Issued {
    plan: meerschaum_engine::Plan,
    denied: std::sync::atomic::AtomicBool,
    ready: AtomicUsize,
}
#[async_trait]
impl meerschaum_engine::PlanAuthority for Issued {
    fn node_delivery(
        &self,
        node: &meerschaum_engine::PipeRef,
    ) -> Option<meerschaum_connector::batch_delivery::NodeDelivery> {
        use meerschaum_connector::batch_delivery::{GraphBinding, NodeDelivery};
        let actor = || GraphBinding {
            grant: uuid::Uuid::new_v4(),
            launch: uuid::Uuid::new_v4(),
            revision: 1,
        };
        Some(NodeDelivery::new(actor(), actor(), node.as_str().into()))
    }
    async fn initial(
        &self,
        expected: &meerschaum_engine::Plan,
    ) -> Result<meerschaum_engine::Plan, String> {
        assert!(
            expected
                .nodes
                .iter()
                .all(|node| node.bounds.is_none() && node.watermark.is_none()),
            "Worker state was used to propose authority"
        );
        Ok(self.plan.clone())
    }
    async fn ready(
        &self,
        _: &meerschaum_engine::PipeRef,
    ) -> Result<meerschaum_engine::PlannedNode, String> {
        self.ready.fetch_add(1, Ordering::SeqCst);
        if self.denied.load(Ordering::SeqCst) {
            return Err("Main revoked this node.".into());
        }
        Ok(self.plan.nodes[0].clone())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delegated_initial_and_bare_sync_use_main_bounds_without_local_watermarks() {
    let env = env("main_bare_window").await;
    env.exec("CREATE TABLE raw_input (id INTEGER); INSERT INTO raw_input VALUES (-5),(6),(15)")
        .await;
    let pipe=env.register("accepted",&json!({"sql":"SELECT id FROM raw_input","target":"accepted_data","columns":{"datetime":"id","id":"id"},"dtypes":{"id":"int64"}})).await;
    let node:meerschaum_engine::PlannedNode=serde_json::from_value(json!({"pipe_ref":pipe.pipe_ref(),"action":"root","bounds":{"begin":5,"end":10},"reason":"Main native window","definition_hash":meerschaum_engine::definition_hash(pipe.raw_parameters())})).unwrap();
    let authority = Arc::new(Issued {
        plan: meerschaum_engine::Plan {
            nodes: vec![node],
            edges: vec![],
            flat: true,
            warnings: vec![],
        },
        denied: std::sync::atomic::AtomicBool::new(false),
        ready: AtomicUsize::new(0),
    });
    let transport = Arc::new(WriteOnly {
        main: env.conn.clone(),
        forbidden_reads: AtomicUsize::new(0),
        checked_writes: AtomicUsize::new(0),
        watermark_reads: AtomicUsize::new(0),
        deny_watermark: true,
    });
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance(INSTANCE, transport.clone())
            .with_source(INSTANCE, env.conn.clone()),
    );
    let engine = Engine::new(registry, env.config()).with_plan_authority(authority.clone());
    let planned = engine
        .plan(&RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("accepted")])),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(planned.plan, authority.plan);
    assert_eq!(transport.watermark_reads.load(Ordering::SeqCst), 0);
    let refused = engine
        .sync_pipe(
            &pipe,
            None,
            &meerschaum_engine::SyncArgs {
                begin: meerschaum_engine::SyncBegin::At(DateBound::Int(0)),
                end: Some(DateBound::Int(20)),
                ..Default::default()
            },
        )
        .await;
    assert!(
        !refused.success && refused.message.contains("subcall intersection"),
        "{refused:?}"
    );
    assert_eq!(transport.checked_writes.load(Ordering::SeqCst), 0);
    let result = engine.sync_pipe(&pipe, None, &Default::default()).await;
    assert!(result.success, "{}", result.message);
    assert_eq!(result.rows_written(), 1);
    let rows = env
        .conn
        .get_pipe_data(&pipe, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows.height(), 1);
    assert_eq!(rows.row(0)[0], KValue::Int(6));
    assert_eq!(transport.watermark_reads.load(Ordering::SeqCst), 0);
    authority.denied.store(true, Ordering::SeqCst);
    let denied = engine.sync_pipe(&pipe, None, &Default::default()).await;
    assert!(
        !denied.success && denied.message.contains("revoked"),
        "{denied:?}"
    );
    assert_eq!(
        env.conn
            .get_pipe_data(&pipe, &DataQuery::default())
            .await
            .unwrap()
            .unwrap()
            .height(),
        1
    );
}

struct WriteOnly {
    main: Arc<SqlConnector>,
    forbidden_reads: AtomicUsize,
    checked_writes: AtomicUsize,
    watermark_reads: AtomicUsize,
    deny_watermark: bool,
}
#[async_trait]
impl Connector for WriteOnly {
    fn connector_type(&self) -> &str {
        "sql"
    }
    fn label(&self) -> &str {
        "eng"
    }
    async fn test_connection(&self) -> ConnectorResult<bool> {
        Connector::test_connection(self.main.as_ref()).await
    }
}
#[async_trait]
impl InstanceConnector for WriteOnly {
    fn require_node_delivery(&self, _: &Pipe) -> ConnectorResult<()> {
        assert!(self.deny_watermark);
        Ok(())
    }
    fn server_filters_existing_rows(&self) -> bool {
        true
    }
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        InstanceConnector::register_pipe(self.main.as_ref(), pipe).await
    }
    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        InstanceConnector::get_pipe_attributes(self.main.as_ref(), pipe).await
    }
    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        InstanceConnector::get_pipe_id(self.main.as_ref(), pipe).await
    }
    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        InstanceConnector::fetch_pipes_keys(self.main.as_ref(), query).await
    }
    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        InstanceConnector::pipe_exists(self.main.as_ref(), pipe).await
    }
    async fn drop_pipe(&self, _: &Pipe) -> SuccessTuple {
        SuccessTuple::fail("This fixture does not grant table drops")
    }
    async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        InstanceConnector::edit_pipe(self.main.as_ref(), pipe, patch).await
    }
    async fn sync_pipe(&self, pipe: &Pipe, frame: Frame, options: &SyncOptions) -> SuccessTuple {
        InstanceConnector::sync_pipe(self.main.as_ref(), pipe, frame, options).await
    }
    async fn sync_pipe_with_counts(
        &self,
        pipe: &Pipe,
        frame: Frame,
        options: &SyncOptions,
    ) -> meerschaum_core::success::SyncResult {
        if pipe.connector_keys() != "mrsm" {
            if self.deny_watermark {
                let owner = meerschaum_connector::batch_delivery::NodeDelivery::current_write()
                    .expect("bare Main admission must reach the destination write");
                assert_eq!(owner.reference(), pipe.pipe_ref());
            }
            assert!(
                options.check_existing,
                "Client filtering bypass disabled Main's existing-row check"
            );
            self.checked_writes.fetch_add(1, Ordering::SeqCst);
        }
        InstanceConnector::sync_pipe_with_counts(self.main.as_ref(), pipe, frame, options).await
    }
    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        if pipe.connector_keys() != "mrsm" {
            self.forbidden_reads.fetch_add(1, Ordering::SeqCst);
            return Err(ConnectorError::Invalid(
                "General stored data reads are denied".into(),
            ));
        }
        InstanceConnector::get_pipe_data(self.main.as_ref(), pipe, query).await
    }
    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        self.watermark_reads.fetch_add(1, Ordering::SeqCst);
        if self.deny_watermark {
            return Err(ConnectorError::Backend(
                "Delegated workers must not derive Main bounds from a local watermark.".into(),
            ));
        }
        InstanceConnector::get_sync_time(self.main.as_ref(), pipe, params, newest).await
    }
    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        InstanceConnector::get_pipe_rowcount(self.main.as_ref(), pipe, query).await
    }
    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        InstanceConnector::get_pipe_columns_types(self.main.as_ref(), pipe).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_comparison_deduplicates_unchanged_rows_and_propagates_changed_parent_cells() {
    let env = env("execution_write_only").await;
    env.exec("CREATE TABLE upstream (id INTEGER, value INTEGER); INSERT INTO upstream VALUES (1,11),(2,21)").await;
    let parent = env.register("parent", &json!({"sql":"SELECT id, value FROM upstream","target":"parent_data","columns":{"datetime":"id","id":"id"},"dtypes":{"id":"int64","value":"int64"},"fetch":{"backtrack_minutes":3},"verify":{"chunk_minutes":1}})).await;
    let child = env.register("child", &json!({"sql":"SELECT id, value FROM parent_data","target":"child_data","columns":{"datetime":"id","id":"id"},"dtypes":{"id":"int64","value":"int64"},"parents":["Pipe('sql:eng','parent')"],"verify":{"chunk_minutes":1}})).await;
    let transport = Arc::new(WriteOnly {
        main: env.conn.clone(),
        forbidden_reads: AtomicUsize::new(0),
        checked_writes: AtomicUsize::new(0),
        watermark_reads: AtomicUsize::new(0),
        deny_watermark: false,
    });
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance(INSTANCE, transport.clone())
            .with_source(INSTANCE, env.conn.clone())
            .with_table_reference(|pipe| pipe.target(None).map_err(|error| error.to_string())),
    );
    let engine = Engine::new(registry, env.config());
    let selection = Selection::refs([env.pipe_ref("parent")]).with_children(Some(Depth::All));
    let run = || RunOptions {
        selection: Some(selection.clone()),
        begin: Some(Bound::Int(0)),
        end: Some(Bound::Int(3)),
        yes: true,
        ..Default::default()
    };
    let lap = |phase: &'static str| {
        let options = run();
        let engine = &engine;
        let transport = &transport;
        async move {
            tokio::time::timeout(std::time::Duration::from_secs(20), engine.run(options))
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "{phase} timed out: {} Main writes, {} forbidden reads",
                        transport.checked_writes.load(Ordering::SeqCst),
                        transport.forbidden_reads.load(Ordering::SeqCst)
                    )
                })
        }
    };
    let first = lap("initial").await;
    assert!(first.success, "{}", first.message);
    assert_eq!(
        first
            .outcomes
            .iter()
            .find(|node| node.pipe_ref == env.pipe_ref("parent"))
            .unwrap()
            .result
            .rows_written(),
        2
    );
    let before = engine
        .state()
        .latest_seq(INSTANCE, &env.pipe_ref("parent"))
        .await
        .unwrap()
        .unwrap();
    let unchanged = lap("unchanged").await;
    assert!(unchanged.success, "{}", unchanged.message);
    assert!(
        unchanged
            .outcomes
            .iter()
            .all(|node| node.result.rows_written() == 0),
        "Server-side comparison wrote unchanged rows: {:?}",
        unchanged.outcomes
    );
    assert!(
        engine
            .state()
            .latest_seq(INSTANCE, &env.pipe_ref("parent"))
            .await
            .unwrap()
            .unwrap()
            > before,
        "Conservative observer omitted the accepted input's dirty-cell proof"
    );
    env.exec("UPDATE upstream SET value=13 WHERE id=1").await;
    let changed = lap("changed").await;
    assert!(changed.success, "{}", changed.message);
    let parent_outcome = changed
        .outcomes
        .iter()
        .find(|node| node.pipe_ref == env.pipe_ref("parent"))
        .unwrap();
    assert_eq!(
        parent_outcome.result.rows_written(),
        1,
        "Main did not compute the exact changed-row count"
    );
    let stored = env
        .conn
        .get_pipe_data(&parent, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.height(),
        2,
        "Repeated accepted input duplicated destination data"
    );
    let propagated = env
        .conn
        .get_pipe_data(&child, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    let values = meerschaum_connector::pipe_ops::frame_to_docs(&propagated);
    assert!(
        values
            .iter()
            .any(|row| row.get("id") == Some(&KValue::Int(1))
                && row.get("value") == Some(&KValue::Int(13))),
        "Changed parent cell did not reach its child: {values:?}"
    );
    assert_eq!(transport.forbidden_reads.load(Ordering::SeqCst), 0);
    assert!(transport.checked_writes.load(Ordering::SeqCst) >= 3);
}
