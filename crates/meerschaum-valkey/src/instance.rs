//! `fetch` (`meerschaum/connectors/valkey/_fetch.py`), the lease pair (§5.8: `SET NX PX`), and
//! the `Connector` / `InstanceConnector` trait implementations.

use std::time::Duration;

use indexmap::IndexMap;
use meerschaum_api::pipe_ops::Doc;
use meerschaum_api::traits::{
    ColumnIndexInfo, Connector, ConnectorError, ConnectorResult, DataQuery, DateBound,
    InstanceConnector, PipeId, PipesKeys, PipesKeysQuery, PluginRecord, SyncOptions, async_trait,
};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;

use crate::connector::{ValkeyConnector, be};
use crate::keys::get_lease_key;
use crate::pyjson::loads_doc;

impl ValkeyConnector {
    /// `fetch(pipe, begin, end)`: read the source key named by `parameters['valkey']['key']`:
    /// a set's members, a sorted set's members within the score bounds, or `{key: value}`.
    pub async fn fetch_docs(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Vec<Doc>> {
        let source_key = match pipe.parameters().get("valkey").and_then(|v| v.get("key")) {
            Some(v) if v.truthy() => meerschaum_core::pyfmt::py_str(v),
            _ => return Ok(Vec::new()),
        };
        let key_type = match self.backend.key_type(&source_key).await {
            Ok(t) => t,
            Err(_) => {
                tracing::warn!("Could not determine the type for key '{source_key}'.");
                return Ok(Vec::new());
            }
        };
        let begin = query.begin.as_ref().map(DateBound::to_value);
        let end = query.end.as_ref().map(DateBound::to_value);
        let (min, max) = Self::score_bounds(begin.as_ref(), end.as_ref())?;
        match key_type.as_str() {
            "set" => self
                .backend
                .smembers(&source_key)
                .await
                .map_err(be)?
                .iter()
                .map(|m| loads_doc(m).map_err(ConnectorError::from))
                .collect(),
            "zset" => self
                .backend
                .zrangebyscore(&source_key, min, max)
                .await
                .map_err(be)?
                .iter()
                .map(|m| loads_doc(m).map_err(ConnectorError::from))
                .collect(),
            _ => {
                let val = self.get(&source_key).await?;
                let mut doc = Doc::new();
                doc.insert(
                    source_key.clone(),
                    val.map(meerschaum_kernel::value::Value::Str)
                        .unwrap_or(meerschaum_kernel::value::Value::Null),
                );
                Ok(vec![doc])
            }
        }
    }

    /// Acquire (or extend, for the current holder) the lease on `pipe` with `SET NX PX`.
    pub async fn acquire_lease(
        &self,
        pipe: &Pipe,
        holder: &str,
        ttl: Duration,
    ) -> ConnectorResult<bool> {
        let key = get_lease_key(&pipe.pipe_ref());
        let ms = ttl.as_millis().max(1) as u64;
        if self.backend.set_nx_px(&key, holder, ms).await.map_err(be)? {
            return Ok(true);
        }
        self.backend
            .compare_and_pexpire(&key, holder, ms)
            .await
            .map_err(be)
    }

    /// Release the lease on `pipe` if `holder` holds it.
    pub async fn release_lease(&self, pipe: &Pipe, holder: &str) -> ConnectorResult<bool> {
        let key = get_lease_key(&pipe.pipe_ref());
        self.backend
            .compare_and_delete(&key, holder)
            .await
            .map_err(be)
    }
}

#[async_trait]
impl Connector for ValkeyConnector {
    fn connector_type(&self) -> &str {
        "valkey"
    }

    fn label(&self) -> &str {
        &self.label
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        self.ping().await
    }

    async fn fetch(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Option<Frame>> {
        let docs = self.fetch_docs(pipe, query).await?;
        Ok(Some(
            meerschaum_kernel::docs::frame_from_docs(&docs)
                .map_err(|e| ConnectorError::Backend(e.to_string()))?,
        ))
    }
}

#[async_trait]
impl InstanceConnector for ValkeyConnector {
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        ValkeyConnector::register_pipe(self, pipe).await
    }

    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        ValkeyConnector::get_pipe_attributes(self, pipe).await
    }

    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        Ok(ValkeyConnector::get_pipe_id(self, pipe)
            .await?
            .map(PipeId::Int))
    }

    async fn edit_pipe(&self, pipe: &Pipe, _patch: bool) -> SuccessTuple {
        ValkeyConnector::edit_pipe(self, pipe).await
    }

    async fn delete_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        ValkeyConnector::delete_pipe(self, pipe).await
    }

    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        Ok(PipesKeys::List(
            ValkeyConnector::fetch_pipes_keys(self, query).await?,
        ))
    }

    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        ValkeyConnector::pipe_exists(self, pipe).await
    }

    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        ValkeyConnector::drop_pipe(self, pipe).await
    }

    async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple {
        ValkeyConnector::sync_pipe(self, pipe, df, opts).await
    }

    async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        ValkeyConnector::clear_pipe(self, pipe, query).await
    }

    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        ValkeyConnector::get_pipe_data(self, pipe, query).await
    }

    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        _params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        ValkeyConnector::get_sync_time(self, pipe, newest).await
    }

    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        ValkeyConnector::get_pipe_rowcount(self, pipe, query).await
    }

    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        ValkeyConnector::get_pipe_columns_types(self, pipe).await
    }

    async fn get_pipe_columns_indices(
        &self,
        _pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, Vec<ColumnIndexInfo>>> {
        Ok(IndexMap::new())
    }

    async fn acquire_pipe_lease(
        &self,
        pipe: &Pipe,
        holder: &str,
        ttl: Duration,
    ) -> ConnectorResult<bool> {
        self.acquire_lease(pipe, holder, ttl).await
    }

    async fn release_pipe_lease(&self, pipe: &Pipe, holder: &str) -> ConnectorResult<bool> {
        self.release_lease(pipe, holder).await
    }

    fn get_users_pipe(&self) -> Pipe {
        self.valkey_users_pipe()
    }

    async fn register_user(&self, user: &mut User) -> SuccessTuple {
        self.valkey_register_user(user).await
    }

    async fn get_user_id(&self, user: &User) -> ConnectorResult<Option<Value>> {
        Ok(self.valkey_get_user_id(user).await.map(Value::Str))
    }

    async fn edit_user(&self, user: &User) -> SuccessTuple {
        self.valkey_edit_user(user).await
    }

    async fn delete_user(&self, user: &User) -> SuccessTuple {
        self.valkey_delete_user(user).await
    }

    async fn get_users(&self) -> ConnectorResult<Vec<String>> {
        self.valkey_get_users().await
    }

    async fn get_user_password_hash(&self, user: &User) -> ConnectorResult<Option<String>> {
        Ok(self.valkey_get_user_password_hash(user).await)
    }

    async fn get_user_type(&self, user: &User) -> ConnectorResult<Option<String>> {
        Ok(self.valkey_get_user_type(user).await)
    }

    async fn get_user_attributes(&self, user: &User) -> ConnectorResult<Option<Map>> {
        Ok(self.valkey_get_user_attributes(user).await)
    }

    fn get_plugins_pipe(&self) -> Pipe {
        self.valkey_plugins_pipe()
    }

    async fn register_plugin(&self, plugin: &PluginRecord, _force: bool) -> SuccessTuple {
        self.valkey_register_plugin(plugin).await
    }

    async fn get_plugin_version(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        Ok(self.valkey_get_plugin_version(plugin).await)
    }

    async fn get_plugin_user_id(&self, plugin: &PluginRecord) -> ConnectorResult<Option<Value>> {
        Ok(self.valkey_get_plugin_user_id(plugin).await.map(Value::Str))
    }

    async fn get_plugin_username(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        Ok(self.valkey_get_plugin_username(plugin).await)
    }

    async fn get_plugin_attributes(&self, plugin: &PluginRecord) -> ConnectorResult<Map> {
        Ok(self.valkey_get_plugin_attributes(plugin).await)
    }

    async fn delete_plugin(&self, plugin: &PluginRecord) -> SuccessTuple {
        self.valkey_delete_plugin(plugin).await
    }
}
