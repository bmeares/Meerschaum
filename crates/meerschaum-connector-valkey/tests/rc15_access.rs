//! The access model on Valkey instances (in-memory backend, rc15 wave 2): locks (AC5), the team
//! store (AC3) and exact user lookups (FX1), with `meerschaum_connector::testing`'s checks.
//!
//! The same checks also run against a **custom instance connector** that implements only the
//! pipe methods ([`Plain`]): users, teams and locks then come from the `InstanceConnector`
//! trait's defaults over its internal pipes (what a `@make_connector` instance gets).

use meerschaum_connector::async_trait;
use meerschaum_connector::traits::{
    Connector, ConnectorResult, DataQuery, DateBound, InstanceConnector, PipeId, PipesKeys,
    PipesKeysQuery, SyncOptions,
};
use meerschaum_connector_valkey::ValkeyConnector;
use meerschaum_core::{Map, Pipe, SuccessTuple};
use meerschaum_kernel::Frame;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valkey_locks_teams_and_users() {
    let conn = ValkeyConnector::in_memory("rc15_access");
    meerschaum_connector::testing::check_locks(&conn, "vk").await;
    meerschaum_connector::testing::check_teams(&conn, "vk").await;
    meerschaum_connector::testing::check_user_lookups(&conn, "vk").await;
}

/// A custom instance connector: pipes stored through an in-memory Valkey connector, and every
/// user, team and lock method left to the trait's defaults.
struct Plain(ValkeyConnector);

#[async_trait]
impl Connector for Plain {
    fn connector_type(&self) -> &str {
        "custom"
    }

    fn label(&self) -> &str {
        "plain"
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }
}

#[async_trait]
impl InstanceConnector for Plain {
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.0.register_pipe(pipe).await
    }

    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        self.0.get_pipe_attributes(pipe).await
    }

    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        InstanceConnector::get_pipe_id(&self.0, pipe).await
    }

    async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        // The custom connector writes as told; the lock check is the default's caller's (the
        // core wraps every custom instance's writes the same way, `meerschaum-worker`).
        if let Some(refused) = meerschaum_connector::edit_refusal(self, pipe, patch).await {
            return refused;
        }
        self.0.edit_pipe(pipe).await
    }

    async fn delete_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if let Some(refused) = meerschaum_connector::lock_refusal(
            self,
            pipe,
            meerschaum_core::access::LockOp::DeleteAndDrop,
        )
        .await
        {
            return refused;
        }
        self.0.delete_pipe(pipe).await
    }

    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        InstanceConnector::fetch_pipes_keys(&self.0, query).await
    }

    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        self.0.pipe_exists(pipe).await
    }

    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if let Some(refused) =
            meerschaum_connector::lock_refusal(self, pipe, meerschaum_core::access::LockOp::Drop)
                .await
        {
            return refused;
        }
        self.0.drop_pipe(pipe).await
    }

    async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple {
        if let Some(refused) = meerschaum_connector::lock_refusal(
            self,
            pipe,
            meerschaum_connector::locks::sync_op(opts.replace),
        )
        .await
        {
            return refused;
        }
        self.0.sync_pipe(pipe, df, opts).await
    }

    async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        if let Some(refused) =
            meerschaum_connector::lock_refusal(self, pipe, meerschaum_core::access::LockOp::Clear)
                .await
        {
            return refused;
        }
        self.0.clear_pipe(pipe, query).await
    }

    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        self.0.get_pipe_data(pipe, query).await
    }

    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        InstanceConnector::get_sync_time(&self.0, pipe, params, newest).await
    }

    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        self.0.get_pipe_rowcount(pipe, query).await
    }

    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<indexmap::IndexMap<String, String>> {
        self.0.get_pipe_columns_types(pipe).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_instance_defaults() {
    let conn = Plain(ValkeyConnector::in_memory("rc15_plain"));
    meerschaum_connector::testing::check_user_lookups(&conn, "cu").await;
    meerschaum_connector::testing::check_teams(&conn, "cu").await;
    meerschaum_connector::testing::check_locks(&conn, "cu").await;
}
