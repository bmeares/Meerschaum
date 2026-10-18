//! `Connector` / `InstanceConnector` for `APIConnector`.

use std::time::Duration;

use indexmap::IndexMap;
use meerschaum_core::users::{Token, User};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use uuid::Uuid;

use crate::models::{LeaseRequest, TokenModel};
use crate::pipe_ops::Doc;
use crate::traits::{
    ColumnIndexInfo, Connector, ConnectorError, ConnectorResult, DataQuery, DateBound,
    InstanceConnector, PipeId, PipesKeys, PipesKeysQuery, PluginRecord, SyncOptions, async_trait,
};

use super::APIConnector;

/// After a successful drop, delete or full clear, forget the pipe's Syncing Engine state
/// (ledger, cursors, definition hash; `pipe_ops::forget_engine_state`). The rows are keyed on
/// this client's `pipe_ref` (`api:<label>|...`), which the server cannot derive, so the client
/// clears them over `/engine/pipes/*`. Best effort: `res` is returned unchanged.
async fn forget_engine_state_after(
    conn: &APIConnector,
    pipe: &Pipe,
    res: SuccessTuple,
) -> SuccessTuple {
    if res.success {
        let forgotten = crate::pipe_ops::forget_engine_state(conn, pipe).await;
        if !forgotten.success {
            tracing::warn!("{}", forgotten.message);
        }
    }
    res
}

#[async_trait]
impl Connector for APIConnector {
    fn connector_type(&self) -> &str {
        "api"
    }

    fn label(&self) -> &str {
        &self.label
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        self.test_connection_inner().await
    }

    /// `fetch(pipe, begin, end, params)`: read the source pipe named by
    /// `parameters['fetch']['pipe']` (or legacy `connector_keys` / `metric_key` /
    /// `location_key`) from this API, patching `fetch.params` and selecting
    /// `fetch.select_columns` / `fetch.omit_columns`.
    async fn fetch(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Option<Frame>> {
        let fetch = match pipe.parameters().get("fetch") {
            Some(Value::Map(m)) if !m.is_empty() => m.clone(),
            _ => {
                super::warnings::warn_fmt!("Missing 'fetch' parameters for {pipe}.");
                return Ok(None);
            }
        };
        let meta = fetch.get("pipe").map(|v| v.to_map()).unwrap_or_default();
        let s = |m: &Map, k: &str| {
            m.get(k)
                .filter(|v| !v.is_null())
                .map(meerschaum_core::pyfmt::py_str)
        };
        let (ck, mk, lk) = if meta.is_empty() {
            (
                s(&fetch, "connector_keys"),
                s(&fetch, "metric_key"),
                s(&fetch, "location_key"),
            )
        } else {
            (
                s(&meta, "connector").or_else(|| s(&meta, "connector_keys")),
                s(&meta, "metric").or_else(|| s(&meta, "metric_key")),
                s(&meta, "location").or_else(|| s(&meta, "location_key")),
            )
        };
        let (Some(ck), Some(mk)) = (ck, mk) else {
            super::warnings::warn_fmt!("Missing `fetch:pipe` keys for {pipe}.");
            return Ok(None);
        };
        let source = Pipe::builder(&ck, &mk)
            .location(lk.as_deref())
            .instance(&self.connector_keys())
            .build()
            .map_err(|e| ConnectorError::Invalid(e.to_string()))?;
        let base = query.params.clone().unwrap_or_default();
        let patch = fetch.get("params").map(|v| v.to_map()).unwrap_or_default();
        let list = |k: &str| -> Option<Vec<String>> {
            fetch.get(k).and_then(|v| v.as_list()).map(|l| {
                l.iter()
                    .map(meerschaum_core::pyfmt::py_str)
                    .collect::<Vec<_>>()
            })
        };
        let q = DataQuery {
            select_columns: list("select_columns"),
            omit_columns: list("omit_columns"),
            params: Some(meerschaum_core::value::apply_patch_maps(&base, &patch)),
            ..query.clone()
        };
        APIConnector::get_pipe_data(self, &source, &q).await
    }
}

#[async_trait]
impl InstanceConnector for APIConnector {
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        APIConnector::register_pipe(self, pipe).await
    }

    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        APIConnector::get_pipe_attributes(self, pipe).await
    }

    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        APIConnector::get_pipe_id(self, pipe).await
    }

    async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        APIConnector::edit_pipe(self, pipe, patch).await
    }

    async fn delete_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let res = APIConnector::delete_pipe(self, pipe).await;
        forget_engine_state_after(self, pipe, res).await
    }

    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        APIConnector::fetch_pipes_keys(self, query).await
    }

    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        APIConnector::pipe_exists(self, pipe).await
    }

    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let res = APIConnector::drop_pipe(self, pipe).await;
        forget_engine_state_after(self, pipe, res).await
    }

    async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple {
        APIConnector::sync_pipe(self, pipe, df, opts).await
    }

    async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        let res = APIConnector::clear_pipe(self, pipe, query).await;
        if !crate::pipe_ops::is_full_clear(query) {
            return res;
        }
        forget_engine_state_after(self, pipe, res).await
    }

    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        APIConnector::get_pipe_data(self, pipe, query).await
    }

    async fn get_pipe_docs(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Vec<Doc>> {
        APIConnector::get_pipe_docs(self, pipe, query).await
    }

    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        APIConnector::get_sync_time(self, pipe, params, newest).await
    }

    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        Ok(Some(
            APIConnector::get_pipe_rowcount(self, pipe, query).await?,
        ))
    }

    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        Ok(self
            .get_pipe_columns_types_opt(pipe)
            .await?
            .unwrap_or_default())
    }

    async fn get_pipe_columns_indices(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, Vec<ColumnIndexInfo>>> {
        Ok(self
            .get_pipe_columns_indices_opt(pipe)
            .await?
            .unwrap_or_default())
    }

    async fn get_pipe_index_names(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        APIConnector::get_pipe_index_names(self, pipe).await
    }

    async fn get_pipe_size(&self, pipe: &Pipe) -> ConnectorResult<Option<i64>> {
        APIConnector::get_pipe_size(self, pipe).await
    }

    async fn compress_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        APIConnector::compress_pipe(self, pipe).await
    }

    async fn decompress_pipe(&self, pipe: &Pipe, no_policy: bool) -> SuccessTuple {
        APIConnector::decompress_pipe(self, pipe, no_policy).await
    }

    async fn vacuum_pipe(&self, pipe: &Pipe, full: bool) -> SuccessTuple {
        APIConnector::vacuum_pipe(self, pipe, full).await
    }

    async fn analyze_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        APIConnector::analyze_pipe(self, pipe).await
    }

    async fn partition_pipe(&self, pipe: &Pipe, chunk_minutes: Option<i64>) -> SuccessTuple {
        APIConnector::partition_pipe(self, pipe, chunk_minutes).await
    }

    async fn acquire_pipe_lease(
        &self,
        pipe: &Pipe,
        holder: &str,
        ttl: Duration,
    ) -> ConnectorResult<bool> {
        self.acquire_engine_lease(&LeaseRequest {
            pipe_ref: pipe.pipe_ref(),
            holder: holder.to_string(),
            ttl_ms: ttl.as_millis() as u64,
        })
        .await
    }

    async fn release_pipe_lease(&self, pipe: &Pipe, holder: &str) -> ConnectorResult<bool> {
        self.release_engine_lease(&LeaseRequest {
            pipe_ref: pipe.pipe_ref(),
            holder: holder.to_string(),
            ttl_ms: 0,
        })
        .await
    }

    async fn register_user(&self, user: &mut User) -> SuccessTuple {
        APIConnector::register_user(self, user).await
    }

    async fn get_user_id(&self, user: &User) -> ConnectorResult<Option<Value>> {
        APIConnector::get_user_id(self, user).await
    }

    async fn get_users(&self) -> ConnectorResult<Vec<String>> {
        APIConnector::get_users(self).await
    }

    async fn edit_user(&self, user: &User) -> SuccessTuple {
        APIConnector::edit_user(self, user).await
    }

    async fn delete_user(&self, user: &User) -> SuccessTuple {
        APIConnector::delete_user(self, user).await
    }

    async fn get_user_password_hash(&self, user: &User) -> ConnectorResult<Option<String>> {
        APIConnector::get_user_password_hash(self, user).await
    }

    async fn get_user_type(&self, user: &User) -> ConnectorResult<Option<String>> {
        APIConnector::get_user_type(self, user).await
    }

    async fn get_user_attributes(&self, user: &User) -> ConnectorResult<Option<Map>> {
        Ok(APIConnector::get_user_attributes(self, user)
            .await?
            .and_then(|v| v.as_map().cloned()))
    }

    /// Plugins are registered with their archive through
    /// [`APIConnector::register_plugin`]; without one this fails like v4 without a file.
    async fn register_plugin(&self, plugin: &PluginRecord, _force: bool) -> SuccessTuple {
        SuccessTuple::fail(format!(
            "Failed to register plugin '{plugin}': an archive is required (use `APIConnector::register_plugin`)."
        ))
    }

    async fn get_plugin_version(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        APIConnector::get_plugin_version(self, plugin).await
    }

    async fn get_plugin_attributes(&self, plugin: &PluginRecord) -> ConnectorResult<Map> {
        APIConnector::get_plugin_attributes(self, plugin).await
    }

    async fn get_plugins(
        &self,
        user_id: Option<&Value>,
        search_term: Option<&str>,
    ) -> ConnectorResult<Vec<String>> {
        APIConnector::get_plugins(self, user_id, search_term).await
    }

    async fn delete_plugin(&self, plugin: &PluginRecord) -> SuccessTuple {
        APIConnector::delete_plugin(self, plugin).await
    }

    async fn register_token(&self, token: &mut Token) -> SuccessTuple {
        APIConnector::register_token(self, token).await
    }

    async fn edit_token(&self, token: &Token) -> SuccessTuple {
        APIConnector::edit_token(self, token).await
    }

    async fn invalidate_token(&self, token: &Token) -> SuccessTuple {
        APIConnector::invalidate_token(self, token).await
    }

    async fn delete_token(&self, token: &Token) -> SuccessTuple {
        APIConnector::delete_token(self, token).await
    }

    async fn get_tokens(
        &self,
        _user: Option<&User>,
        labels: Option<&[String]>,
        _ids: Option<&[Uuid]>,
    ) -> ConnectorResult<Vec<TokenModel>> {
        APIConnector::get_tokens(self, labels).await
    }

    async fn get_token_model(&self, token_id: &Uuid) -> ConnectorResult<Option<TokenModel>> {
        APIConnector::get_token_model(self, Some(*token_id)).await
    }

    async fn get_token_scopes(&self, token_id: &Uuid) -> ConnectorResult<Vec<String>> {
        APIConnector::get_token_scopes(self, Some(*token_id)).await
    }

    async fn token_exists(&self, token_id: &Uuid) -> ConnectorResult<bool> {
        APIConnector::token_exists(self, Some(*token_id)).await
    }
}
