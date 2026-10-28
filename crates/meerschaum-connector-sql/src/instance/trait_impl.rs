//! `Connector` and `InstanceConnector` for [`SqlConnector`], delegating to the inherent methods.
//!
//! Signature mismatches with the SQL semantics (the trait is owned by `meerschaum-connector`):
//! - `get_plugin_id` returns the integer `plugin_id` as a string (SQL ids are integers).
//! - `delete_plugin` keeps the SQL success tuple for unregistered plugins (`(True, ...)`).
//! - `fetch(pipe, DataQuery)` maps an absent `begin` to an unbounded query (Python's `''`
//!   default needs the instance sync time, which the source connector does not know).

use indexmap::IndexMap;
use meerschaum_connector::{
    ColumnIndexInfo, Connector, ConnectorError, ConnectorResult, DataQuery, DateBound,
    InstanceConnector, PipeId, PipesKeys, PipesKeysQuery, PluginRecord, SyncOptions, async_trait,
    pipe_ops,
};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;

use super::SqlConnector;
use super::fetch::{FetchArgs, metadef_bound};

/// `SQLConnector.get_users_pipe()` (`_users.py:15`).
pub fn sql_users_pipe(instance_keys: &str) -> Pipe {
    let s = |pairs: &[(&str, &str)]| -> Value {
        Value::Map(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), Value::Str(v.to_string())))
                .collect(),
        )
    };
    Pipe::builder("mrsm", "users")
        .instance(instance_keys)
        .kwarg("temporary", true)
        .kwarg("cache", true)
        .kwarg("static", true)
        .kwarg("null_indices", false)
        .kwarg("enforce", false)
        .kwarg("autoincrement", true)
        .kwarg("columns", s(&[("primary", "user_id")]))
        .kwarg(
            "dtypes",
            s(&[
                ("user_id", "int"),
                ("username", "string"),
                ("attributes", "json"),
                ("user_type", "string"),
            ]),
        )
        .kwarg("indices", s(&[("unique", "username")]))
        .build()
        .expect("static internal pipe keys are valid")
}

#[async_trait]
impl Connector for SqlConnector {
    fn connector_type(&self) -> &str {
        "sql"
    }

    fn label(&self) -> &str {
        &self.label
    }

    fn is_thread_safe(&self) -> bool {
        SqlConnector::is_thread_safe(self)
    }

    fn is_protected(&self) -> bool {
        SqlConnector::is_protected(self)
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(self.value(self.flavor.test_query()).await.is_some())
    }

    async fn fetch(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Option<Frame>> {
        self.require_schema().await?;
        let args = FetchArgs {
            begin: metadef_bound(query.begin.as_ref()),
            end: metadef_bound(query.end.as_ref()),
            params: query
                .params
                .as_ref()
                .map(|p| p.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            ..Default::default()
        };
        SqlConnector::fetch(self, pipe, &args).await
    }
}

#[async_trait]
impl InstanceConnector for SqlConnector {
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        SqlConnector::register_pipe(self, pipe).await
    }

    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        self.require_schema().await?;
        SqlConnector::get_pipe_attributes(self, pipe).await
    }

    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        self.require_schema().await?;
        Ok(SqlConnector::get_pipe_id(self, pipe)
            .await?
            .map(PipeId::Int))
    }

    async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        SqlConnector::edit_pipe(self, pipe, patch).await
    }

    async fn delete_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let res = SqlConnector::delete_pipe(self, pipe).await;
        forget_engine_state_after(self, pipe, res).await
    }

    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        self.require_schema().await?;
        SqlConnector::fetch_pipes_keys(self, query).await
    }

    fn pipe_table_reference(&self, pipe: &Pipe) -> Option<String> {
        // An undiscovered default schema would name the wrong table: no reference at all.
        if !self.schema_known() {
            return None;
        }
        let loaded = self.with_registered_parameters(pipe);
        let sp = self.sql_pipe(&loaded).ok()?;
        Some(self.pipe_table_name(&sp))
    }

    fn pipe_target_aliases(&self, pipe: &Pipe) -> Vec<String> {
        let Ok(sp) = self.sql_pipe(pipe) else {
            return Vec::new();
        };
        // `sql_item_name` also truncates explicit targets, which `Pipe::target` leaves
        // intact. Keep its plain name as well as its qualified/quoted reference.
        let physical = crate::ident::truncate_item_name(&sp.target, self.flavor);
        let mut aliases = vec![physical.clone(), self.pipe_table_name(&sp)];
        if let Some(limit) = self.sql_identifier_byte_limit() {
            // PostgreSQL clips identifiers on a UTF-8 boundary, after the compatibility
            // naming algorithm has applied its character limit.
            let mut end = physical.len().min(limit);
            while !physical.is_char_boundary(end) {
                end -= 1;
            }
            let clipped = physical[..end].to_string();
            if !aliases.contains(&clipped) {
                aliases.push(clipped);
            }
        }
        aliases
    }

    fn sql_identifier_byte_limit(&self) -> Option<usize> {
        matches!(
            self.flavor,
            crate::Flavor::Postgresql
                | crate::Flavor::Postgis
                | crate::Flavor::Timescaledb
                | crate::Flavor::TimescaledbHa
                | crate::Flavor::Citus
        )
        .then_some(63)
    }

    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::pipe_exists(self, pipe).await
    }

    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        let res = SqlConnector::drop_pipe(self, pipe).await;
        forget_engine_state_after(self, pipe, res).await
    }

    async fn drop_pipe_indices(&self, pipe: &Pipe) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::drop_pipe_indices(self, pipe, None).await
    }

    async fn create_pipe_indices(&self, pipe: &Pipe) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::create_pipe_indices(self, pipe, None).await
    }

    async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        SqlConnector::sync_pipe(self, pipe, df, opts).await
    }

    async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        let res = SqlConnector::clear_pipe(self, pipe, query).await;
        if !meerschaum_connector::pipe_ops::is_full_clear(query) {
            return res;
        }
        forget_engine_state_after(self, pipe, res).await
    }

    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::get_pipe_data(self, pipe, query).await
    }

    async fn get_pipe_docs(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Vec<IndexMap<String, meerschaum_kernel::Value>>> {
        SqlConnector::get_pipe_docs(self, pipe, query).await
    }

    async fn get_backtrack_data(
        &self,
        pipe: &Pipe,
        backtrack_minutes: i64,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::get_backtrack_data(self, pipe, Some(backtrack_minutes), query).await
    }

    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::get_sync_time(self, pipe, params, newest, false).await
    }

    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::get_pipe_rowcount(self, pipe, query).await
    }

    async fn get_pipe_rowcount_estimate(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::get_pipe_rowcount_estimate(self, pipe, query, None).await
    }

    async fn get_pipe_schema_plan(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<meerschaum_connector::SchemaPlan> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::get_pipe_schema_plan(self, pipe).await
    }

    async fn apply_pipe_schema(
        &self,
        pipe: &Pipe,
        plan: &meerschaum_connector::SchemaPlan,
        safe_only: bool,
    ) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::apply_pipe_schema(self, pipe, plan, safe_only).await
    }

    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::get_pipe_columns_types(self, pipe).await
    }

    async fn get_pipe_columns_indices(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, Vec<ColumnIndexInfo>>> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        Ok(SqlConnector::get_pipe_columns_indices(self, pipe)
            .await?
            .into_iter()
            .map(|(col, docs)| {
                (
                    col,
                    docs.into_iter()
                        .map(|d| ColumnIndexInfo {
                            name: d.name.unwrap_or_default(),
                            index_type: d.type_.unwrap_or_default(),
                        })
                        .collect(),
                )
            })
            .collect())
    }

    async fn get_pipe_index_names(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        Ok(SqlConnector::get_pipe_index_names(self, pipe)?
            .into_iter()
            .map(|(k, v)| (k, Value::Str(v)))
            .collect())
    }

    async fn deduplicate_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::deduplicate_pipe(self, pipe, query).await
    }

    async fn get_pipe_size(&self, pipe: &Pipe) -> ConnectorResult<Option<i64>> {
        self.require_schema().await?;
        let pipe = &self.prepare_pipe(pipe).await;
        Ok(SqlConnector::get_pipe_size(self, pipe).await)
    }

    async fn compress_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::compress_pipe(self, pipe, false).await
    }

    async fn apply_compression_policy(&self, pipe: &Pipe) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::apply_compression_policy(self, pipe).await
    }

    async fn create_instance_tables(&self) -> ConnectorResult<bool> {
        self.require_schema().await?;
        Ok(self.ensure_instance_tables(true).await)
    }

    async fn decompress_pipe(&self, pipe: &Pipe, no_policy: bool) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::decompress_pipe(self, pipe, no_policy).await
    }

    async fn vacuum_pipe(&self, pipe: &Pipe, full: bool) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::vacuum_pipe(self, pipe, full).await
    }

    async fn analyze_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::analyze_pipe(self, pipe).await
    }

    async fn acquire_pipe_lease(
        &self,
        pipe: &Pipe,
        holder: &str,
        ttl: std::time::Duration,
    ) -> ConnectorResult<bool> {
        self.require_schema().await?;
        SqlConnector::acquire_pipe_lease(self, pipe, holder, ttl).await
    }

    async fn release_pipe_lease(&self, pipe: &Pipe, holder: &str) -> ConnectorResult<bool> {
        self.require_schema().await?;
        SqlConnector::release_pipe_lease(self, pipe, holder).await
    }

    async fn engine_state_pipe(
        &self,
        instance_keys: &str,
        metric_key: &str,
    ) -> ConnectorResult<Option<Pipe>> {
        // The namespace depends on the instance schema: ask the database first if unknown.
        self.require_schema().await?;
        Ok(SqlConnector::engine_state_pipe(
            self,
            instance_keys,
            metric_key,
        ))
    }

    async fn partition_pipe(&self, pipe: &Pipe, chunk_minutes: Option<i64>) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        let pipe = &self.prepare_pipe(pipe).await;
        SqlConnector::partition_pipe(self, pipe, chunk_minutes).await
    }

    // Users (`_users.py`): the `mrsm_users` table.

    fn get_users_pipe(&self) -> Pipe {
        sql_users_pipe(&self.keys())
    }

    async fn register_user(&self, user: &mut User) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        SqlConnector::register_user(self, user).await
    }

    async fn get_user_id(&self, user: &User) -> ConnectorResult<Option<Value>> {
        self.require_schema().await?;
        SqlConnector::get_user_id(self, user).await
    }

    async fn get_username(&self, user_id: &Value) -> ConnectorResult<Option<Value>> {
        self.require_schema().await?;
        SqlConnector::get_username(self, user_id).await
    }

    async fn get_users(&self) -> ConnectorResult<Vec<String>> {
        self.require_schema().await?;
        SqlConnector::get_users(self).await
    }

    async fn edit_user(&self, user: &User) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        SqlConnector::edit_user(self, user).await
    }

    async fn delete_user(&self, user: &User) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        // It also removes the user from every team and clears their ownership of pipes.
        SqlConnector::delete_user(self, user).await
    }

    async fn set_user_attributes(&self, user: &User, attributes: &Map) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        SqlConnector::set_user_attributes(self, user, attributes).await
    }

    async fn get_user_password_hash(&self, user: &User) -> ConnectorResult<Option<String>> {
        self.require_schema().await?;
        SqlConnector::get_user_password_hash(self, user).await
    }

    async fn get_user_type(&self, user: &User) -> ConnectorResult<Option<String>> {
        self.require_schema().await?;
        SqlConnector::get_user_type(self, user).await
    }

    async fn get_user_attributes(&self, user: &User) -> ConnectorResult<Option<Map>> {
        self.require_schema().await?;
        SqlConnector::get_user_attributes(self, user).await
    }

    async fn get_user_email(&self, user: &User) -> ConnectorResult<Option<String>> {
        self.require_schema().await?;
        SqlConnector::get_user_email(self, user).await
    }

    // Plugins (`_plugins.py`): the `mrsm_plugins` table.

    fn get_plugins_pipe(&self) -> Pipe {
        pipe_ops::plugins_pipe(&self.keys(), "int")
    }

    async fn register_plugin(&self, plugin: &PluginRecord, force: bool) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        SqlConnector::register_plugin(self, plugin, force).await
    }

    async fn register_plugin_as_owner(&self, plugin: &PluginRecord, force: bool) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        SqlConnector::register_plugin_as_owner(self, plugin, force).await
    }

    async fn get_plugin_id(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        self.require_schema().await?;
        Ok(SqlConnector::get_plugin_id(self, plugin)
            .await?
            .map(|i| i.to_string()))
    }

    async fn get_plugin_version(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        self.require_schema().await?;
        SqlConnector::get_plugin_version(self, plugin).await
    }

    async fn get_plugin_user_id(&self, plugin: &PluginRecord) -> ConnectorResult<Option<Value>> {
        self.require_schema().await?;
        Ok(SqlConnector::get_plugin_user_id(self, plugin)
            .await?
            .map(Value::Int))
    }

    async fn get_plugin_username(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        self.require_schema().await?;
        SqlConnector::get_plugin_username(self, plugin).await
    }

    async fn get_plugin_attributes(&self, plugin: &PluginRecord) -> ConnectorResult<Map> {
        self.require_schema().await?;
        SqlConnector::get_plugin_attributes(self, plugin).await
    }

    async fn get_plugins(
        &self,
        user_id: Option<&Value>,
        search_term: Option<&str>,
    ) -> ConnectorResult<Vec<String>> {
        self.require_schema().await?;
        SqlConnector::get_plugins(self, user_id, search_term).await
    }

    async fn delete_plugin(&self, plugin: &PluginRecord) -> SuccessTuple {
        if let Err(e) = self.require_schema().await {
            return SuccessTuple::fail(e.to_string());
        }
        SqlConnector::delete_plugin(self, plugin).await
    }

    // Tokens: the generic pipe-based implementation (`instance/_tokens.py`) with the SQL users
    // pipe's `user_id` dtype.

    fn get_tokens_pipe(&self) -> Pipe {
        pipe_ops::tokens_pipe(&self.keys(), "int")
    }
}

impl From<ConnectorError> for crate::SqlError {
    fn from(e: ConnectorError) -> Self {
        crate::SqlError::InvalidArgument(e.to_string())
    }
}

/// After a successful drop, delete or full clear, forget the pipe's Syncing Engine state
/// (ledger, cursors, definition hash; `pipe_ops::forget_engine_state`) so a pipe recreated
/// under the same keys starts fresh, as in v4. Best effort: `res` is returned unchanged.
async fn forget_engine_state_after(
    conn: &SqlConnector,
    pipe: &Pipe,
    res: SuccessTuple,
) -> SuccessTuple {
    if res.success {
        let forgotten = meerschaum_connector::pipe_ops::forget_engine_state(conn, pipe).await;
        if !forgotten.success {
            tracing::warn!("{}", forgotten.message);
        }
        // GAP-002: the cursors and ledger entries written onto other instances.
        let remote = conn.forget_remote_engine_state(pipe).await;
        if !remote.success {
            tracing::warn!("{}", remote.message);
        }
    }
    res
}
