//! Pause a real native metadata read after SQLite returned its bounded snapshot.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use meerschaum_cli::source_fetch_backend::NativeSqlSources;
use meerschaum_connector::{
    Connector, ConnectorError, ConnectorResult, DataQuery, DateBound, InstanceConnector, PipeId,
    PipeKeysEntry, PipesKeys, PipesKeysQuery, SyncOptions, async_trait,
};
use meerschaum_core::{Map, Pipe, SuccessTuple};
use meerschaum_kernel::Frame;
use meerschaum_server::backends::{SourceFetchBackend, SourceFetchError, SourcePage};
use meerschaum_server::routes::source_fetch::Plan;
use tokio::sync::Notify;

#[derive(Default)]
pub struct Pause {
    remaining: AtomicUsize,
    pub entered: Notify,
    pub release: Notify,
}
impl Pause {
    pub fn final_page_metadata_read(&self) {
        // The page route prepares before reading and again before serving its result.
        // Hold the final source read of the second preparation: an earlier pause would
        // also be caught by the old post-read preparation and would not expose this race.
        self.remaining.store(4, Ordering::SeqCst);
    }
}

pub struct Sources {
    pub inner: Arc<NativeSqlSources>,
    pub pause: Arc<Pause>,
}
#[async_trait]
impl SourceFetchBackend for Sources {
    async fn native_instance(
        &self,
        keys: &str,
    ) -> Result<Arc<dyn InstanceConnector>, SourceFetchError> {
        let inner = self.inner.native_instance(keys).await?;
        Ok(if keys == "sql:source" {
            Arc::new(Metadata {
                inner,
                pause: self.pause.clone(),
            })
        } else {
            inner
        })
    }
    async fn fetch(
        &self,
        plan: Plan,
    ) -> Result<meerschaum_connector_api::source_fetch::Response, SourceFetchError> {
        self.inner.fetch(plan).await
    }
    async fn page(
        &self,
        plan: Plan,
        cursor: Option<meerschaum_connector_api::source_fetch::Cursor>,
    ) -> Result<SourcePage, SourceFetchError> {
        self.inner.page(plan, cursor).await
    }
    async fn close_snapshot(
        &self,
        cursor: &meerschaum_connector_api::source_fetch::Cursor,
        owner: Option<&str>,
        credential: &str,
    ) -> Result<(), SourceFetchError> {
        self.inner.close_snapshot(cursor, owner, credential).await
    }
}
struct Metadata {
    inner: Arc<dyn InstanceConnector>,
    pause: Arc<Pause>,
}
#[async_trait]
impl Connector for Metadata {
    fn connector_type(&self) -> &str {
        self.inner.connector_type()
    }
    fn label(&self) -> &str {
        self.inner.label()
    }
    async fn test_connection(&self) -> ConnectorResult<bool> {
        self.inner.test_connection().await
    }
}
#[async_trait]
impl InstanceConnector for Metadata {
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.inner.register_pipe(pipe).await
    }
    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        self.inner.get_pipe_attributes(pipe).await
    }
    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        self.inner.get_pipe_id(pipe).await
    }
    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        self.inner.fetch_pipes_keys(query).await
    }
    async fn registered_pipes(
        &self,
        query: &PipesKeysQuery,
    ) -> ConnectorResult<Vec<(Option<PipeId>, PipeKeysEntry)>> {
        let result = self.inner.registered_pipes(query).await?;
        if self
            .pause
            .remaining
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok_and(|remaining| remaining == 1)
        {
            self.pause.entered.notify_one();
            tokio::time::timeout(Duration::from_secs(10), self.pause.release.notified())
                .await
                .map_err(|_| ConnectorError::Backend("Test metadata wait expired.".into()))?;
        }
        Ok(result)
    }
    fn pipe_table_reference(&self, pipe: &Pipe) -> Option<String> {
        self.inner.pipe_table_reference(pipe)
    }
    fn pipe_table_schema(&self, pipe: &Pipe) -> Option<String> {
        self.inner.pipe_table_schema(pipe)
    }
    fn pipe_target_aliases(&self, pipe: &Pipe) -> Vec<String> {
        self.inner.pipe_target_aliases(pipe)
    }
    fn sql_identifier_byte_limit(&self) -> Option<usize> {
        self.inner.sql_identifier_byte_limit()
    }
    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        self.inner.pipe_exists(pipe).await
    }
    async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        self.inner.edit_pipe(pipe, patch).await
    }
    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.inner.drop_pipe(pipe).await
    }
    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        self.inner.get_pipe_data(pipe, query).await
    }
    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        self.inner.get_pipe_rowcount(pipe, query).await
    }
    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<indexmap::IndexMap<String, String>> {
        self.inner.get_pipe_columns_types(pipe).await
    }
    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        self.inner.get_sync_time(pipe, params, newest).await
    }
    async fn sync_pipe(&self, pipe: &Pipe, data: Frame, opts: &SyncOptions) -> SuccessTuple {
        self.inner.sync_pipe(pipe, data, opts).await
    }
}
