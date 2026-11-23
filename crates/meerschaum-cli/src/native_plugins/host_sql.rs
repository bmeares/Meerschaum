//! Parameterized SQL (`read_sql_with` / `exec_sql_with`) and host-managed SQL transactions
//! (`tx_begin` / `tx_read_sql` / `tx_exec_sql` / `tx_commit` / `tx_rollback`) for [`PluginHost`].

use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use meerschaum_connector_sql::{
    SqlConnector,
    driver::{SqlTransaction, named_params::bind_named_sql},
};
use meerschaum_core::SuccessTuple;
use meerschaum_plugin::{HostCall, HostReply};
use serde_json::json;

use super::host::{HostResult, PluginHost, host_error, tuple_json};

pub(super) struct OpenTx {
    connector_keys: String,
    sql: Arc<SqlConnector>,
    tx: Option<SqlTransaction>,
    wrote_queries: Vec<String>,
}

/// Concurrent map of open host-managed SQL transactions for a [`PluginHost`].
///
/// Each open transaction has its own `tokio::sync::Mutex`, so concurrent HTTP route handlers
/// running against the same host never contend on each other in Rust; they synchronize only in
/// the database (e.g. via `pg_advisory_xact_lock` when sharing a `lock_key`).
#[derive(Clone, Default)]
pub(super) struct TxRegistry {
    next_id: Arc<AtomicU64>,
    open: Arc<Mutex<HashMap<u64, Arc<tokio::sync::Mutex<OpenTx>>>>>,
}

impl TxRegistry {
    fn insert(&self, tx: OpenTx) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Arc::new(tokio::sync::Mutex::new(tx)));
        id
    }

    fn get(&self, id: u64) -> Option<Arc<tokio::sync::Mutex<OpenTx>>> {
        self.open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned()
    }

    fn remove(&self, id: u64) -> Option<Arc<tokio::sync::Mutex<OpenTx>>> {
        self.open
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
    }
}

impl PluginHost {
    /// Authorize the caller's scopes, connector access, and execute permission before touching
    /// connector metadata (so unauthorized callers cannot probe whether a connector exists).
    pub(super) async fn authorize_sql_caller(
        &self,
        keys: &str,
        is_read: bool,
    ) -> HostResult<Option<meerschaum_server::access::Access>> {
        let access = if is_read {
            self.api_access("sql:read", "run SQL queries").await?
        } else {
            let access = self
                .api_access("actions:execute", "execute SQL statements")
                .await?;
            self.may_execute().await?;
            access
        };
        if let (Some(api), Some(access)) = (&self.api, &access) {
            self.instance(keys)?;
            meerschaum_server::access::guard::check_connector_teams(&api.state, access, keys)
                .map_err(|e| host_error(e.detail_text()))?;
        }
        Ok(access)
    }

    /// Enforce the unified SQL guard policy once on the final SQL statement for an API caller.
    pub(super) async fn check_api_sql(
        &self,
        keys: &str,
        query: &str,
        is_read: bool,
        access: Option<&meerschaum_server::access::Access>,
    ) -> HostResult<()> {
        let Some(api) = &self.api else {
            return Ok(());
        };
        let hidden = match access {
            Some(access) => {
                let index = meerschaum_server::access::pipe_index(&api.state, Some(keys))
                    .await
                    .map_err(|e| host_error(e.detail_text()))?;
                index.hidden_targets(&api.state, access)
            }
            None => meerschaum_server::sql_guard::HiddenTables::default(),
        };
        let policy = if is_read {
            meerschaum_server::sql_guard::SqlPolicy::ReadOnlyHostQuery { hidden: &hidden }
        } else {
            meerschaum_server::sql_guard::SqlPolicy::RawExec { hidden: &hidden }
        };
        meerschaum_server::sql_guard::check_sql(query, &policy)
            .map_err(|e| host_error(e.into_message()))
    }

    pub(super) async fn authorize_sql_query(
        &self,
        keys: &str,
        query: &str,
        is_read: bool,
    ) -> HostResult<()> {
        let access = self.authorize_sql_caller(keys, is_read).await?;
        self.check_api_sql(keys, query, is_read, access.as_ref())
            .await
    }

    pub(super) async fn serve_sql_with(
        &self,
        call: &HostCall,
        is_read: bool,
    ) -> HostResult<HostReply> {
        let keys = call
            .str_arg("connector_keys")
            .ok_or_else(|| host_error("`connector_keys` names no connector"))?;
        let query = call.str_arg("query").unwrap_or_default();

        // Check caller permissions before revealing connector existence.
        let access = self.authorize_sql_caller(keys, is_read).await?;

        let sql = self.env()?.sql(keys).map_err(host_error)?;
        let (rewritten, bound) = bind_named_sql(sql.flavor(), query, call.arg("params"))
            .map_err(|e| host_error(e.to_string()))?;

        // Run the unified SQL guard on the rewritten SQL so `:param` names cannot false-positive.
        self.check_api_sql(keys, &rewritten, is_read, access.as_ref())
            .await?;

        if is_read {
            let frame = sql
                .read_raw_with(&rewritten, &bound)
                .await
                .map_err(|e| host_error(e.to_string()))?;
            return Ok(HostReply::data(Some(frame)));
        }

        if let Err(refused) = meerschaum_connector::check_protected(&*sql, "execute SQL") {
            return Ok(HostReply::json(tuple_json(&SuccessTuple::fail(refused))));
        }
        sql.note_raw_statement(&rewritten, false);
        let result = sql.driver().exec_with(&rewritten, &bound).await;
        sql.note_raw_statement(&rewritten, result.is_err());
        Ok(HostReply::json(tuple_json(&match result {
            Ok(rows) => SuccessTuple::ok(format!("Executed the statement ({rows} rows).")),
            Err(error) => SuccessTuple::fail(format!(
                "Failed to execute the statement on '{keys}':\n{error}"
            )),
        })))
    }

    pub(super) async fn serve_tx_begin(&self, call: &HostCall) -> HostResult<HostReply> {
        let keys = call
            .str_arg("connector_keys")
            .ok_or_else(|| host_error("`connector_keys` names no connector"))?;
        let lock_key = call.str_arg("lock_key");

        let access = self
            .api_access("sql:read", "start a SQL transaction")
            .await?;
        if let (Some(api), Some(access)) = (&self.api, &access) {
            self.instance(keys)?;
            meerschaum_server::access::guard::check_connector_teams(&api.state, access, keys)
                .map_err(|e| host_error(e.detail_text()))?;
        }

        let sql = self.env()?.sql(keys).map_err(host_error)?;
        let mut tx = if let Err(refused) =
            meerschaum_connector::check_protected(&*sql, "start a write SQL transaction")
        {
            if lock_key.is_some() {
                return Err(host_error(refused));
            }
            sql.driver()
                .begin_read_only()
                .await
                .map_err(|e| host_error(e.to_string()))?
        } else {
            sql.driver()
                .begin_write()
                .await
                .map_err(|e| host_error(e.to_string()))?
        };

        if let Some(key) = lock_key
            && let Err(err) = tx.advisory_xact_lock(sql.flavor(), key).await
        {
            let _ = tx.rollback().await;
            return Err(host_error(err.to_string()));
        }

        let tx_id = self.txs.insert(OpenTx {
            connector_keys: keys.to_string(),
            sql,
            tx: Some(tx),
            wrote_queries: Vec::new(),
        });
        Ok(HostReply::json(json!({"tx_id": tx_id})))
    }

    pub(super) async fn serve_tx_sql(
        &self,
        call: &HostCall,
        is_read: bool,
    ) -> HostResult<HostReply> {
        let tx_id = call
            .arg("tx_id")
            .as_u64()
            .ok_or_else(|| host_error("missing or invalid `tx_id`"))?;
        let query = call.str_arg("query").unwrap_or_default();
        let entry = self
            .txs
            .get(tx_id)
            .ok_or_else(|| host_error(format!("unknown or finished SQL transaction {tx_id}")))?;
        let mut open = entry.lock().await;
        let keys = open.connector_keys.clone();
        let (rewritten, bound) = bind_named_sql(open.sql.flavor(), query, call.arg("params"))
            .map_err(|e| host_error(e.to_string()))?;

        self.authorize_sql_query(&keys, &rewritten, is_read).await?;

        if is_read {
            let tx = open
                .tx
                .as_mut()
                .ok_or_else(|| host_error(format!("SQL transaction {tx_id} has already closed")))?;
            let frame = tx
                .read_frame_with(&rewritten, &bound)
                .await
                .map_err(|e| host_error(e.to_string()))?;
            return Ok(HostReply::data(Some(frame)));
        }

        if let Err(refused) = meerschaum_connector::check_protected(&*open.sql, "execute SQL") {
            return Ok(HostReply::json(tuple_json(&SuccessTuple::fail(refused))));
        }
        open.sql.note_raw_statement(&rewritten, false);
        let tx = open
            .tx
            .as_mut()
            .ok_or_else(|| host_error(format!("SQL transaction {tx_id} has already closed")))?;
        let result = tx.exec_with(&rewritten, &bound).await;
        match &result {
            Ok(_) => open.wrote_queries.push(rewritten),
            Err(_) => open.sql.note_raw_statement(&rewritten, true),
        }
        Ok(HostReply::json(tuple_json(&match result {
            Ok(rows) => SuccessTuple::ok(format!("Executed the statement ({rows} rows).")),
            Err(error) => SuccessTuple::fail(format!(
                "Failed to execute the statement on '{keys}':\n{error}"
            )),
        })))
    }

    pub(super) async fn serve_tx_commit(&self, call: &HostCall) -> HostResult<HostReply> {
        let tx_id = call
            .arg("tx_id")
            .as_u64()
            .ok_or_else(|| host_error("missing or invalid `tx_id`"))?;
        let entry = self
            .txs
            .remove(tx_id)
            .ok_or_else(|| host_error(format!("unknown or finished SQL transaction {tx_id}")))?;
        let mut open = entry.lock().await;
        let Some(tx) = open.tx.take() else {
            return Err(host_error(format!(
                "SQL transaction {tx_id} has already closed"
            )));
        };
        let result = tx.commit().await;
        match result {
            Ok(()) => {
                for q in &open.wrote_queries {
                    open.sql.note_raw_statement(q, false);
                }
                Ok(HostReply::json(tuple_json(&SuccessTuple::ok(
                    "Committed the transaction.",
                ))))
            }
            Err(error) => Ok(HostReply::json(tuple_json(&SuccessTuple::fail(format!(
                "Failed to commit transaction on '{}':\n{error}",
                open.connector_keys
            ))))),
        }
    }

    pub(super) async fn serve_tx_rollback(&self, call: &HostCall) -> HostResult<HostReply> {
        let tx_id = call
            .arg("tx_id")
            .as_u64()
            .ok_or_else(|| host_error("missing or invalid `tx_id`"))?;
        let Some(entry) = self.txs.remove(tx_id) else {
            return Ok(HostReply::json(tuple_json(&SuccessTuple::ok(
                "Transaction already finished.",
            ))));
        };
        let mut open = entry.lock().await;
        if let Some(tx) = open.tx.take() {
            let _ = tx.rollback().await;
        }
        Ok(HostReply::json(tuple_json(&SuccessTuple::ok(
            "Rolled back the transaction.",
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::Session;
    use crate::native_plugins::{HostEnv, PluginHost};
    use meerschaum_core::config::{Config, ConfigOptions};
    use meerschaum_plugin::{HostServices, NativePlugins, Request, callbacks::method as m};

    fn test_host(root: &std::path::Path) -> PluginHost {
        let env = vec![(
            "MRSM_ROOT_DIR".to_string(),
            root.to_string_lossy().into_owned(),
        )];
        let config = Arc::new(Config::new(ConfigOptions::with_env(env)).unwrap());
        let session = Session::new();
        let natives = Arc::new(NativePlugins::empty());
        PluginHost::local(HostEnv::new(
            session,
            config,
            natives,
            Some("sql:local".into()),
        ))
    }

    fn req() -> Request {
        Request {
            method: "action".into(),
            connector_type: "plugin".into(),
            label: "test".into(),
            config: serde_json::Value::Null,
            context: json!({
                "pipe": {
                    "keys": {
                        "connector_keys": "plugin:test",
                        "metric_key": "telemetry",
                        "location_key": null,
                        "instance_keys": "sql:local"
                    },
                    "parameters": {},
                    "temporary": false
                }
            }),
        }
    }

    fn call(method: &str, args: serde_json::Value) -> HostCall {
        HostCall {
            plugin: "test".into(),
            method: method.into(),
            pipe: None,
            args,
        }
    }

    #[test]
    fn host_serves_root_dir_pipe_id_named_sql_and_transactions_on_sqlite() {
        let tmp = tempfile::tempdir().unwrap();
        let host = test_host(tmp.path());
        let r = req();

        // B. root_dir resolves to the call's Config root (not process env).
        let root_reply = host
            .host_call(&r, &call(m::ROOT_DIR, json!({})), None)
            .unwrap();
        assert_eq!(root_reply.value, json!(tmp.path().to_string_lossy()));

        // C. Pipe::id and Pipe::is_registered distinguish unregistered vs registered-empty-params.
        let id_before = host
            .host_call(&r, &call(m::GET_PIPE_ID, json!({})), None)
            .unwrap();
        assert!(id_before.value.is_null());
        let reg_before = host
            .host_call(&r, &call(m::IS_REGISTERED, json!({})), None)
            .unwrap();
        assert_eq!(reg_before.value, json!(false));

        let reg = host
            .host_call(&r, &call(m::REGISTER, json!({})), None)
            .unwrap();
        assert_eq!(reg.value[0], json!(true));

        let id_after = host
            .host_call(&r, &call(m::GET_PIPE_ID, json!({})), None)
            .unwrap();
        assert!(id_after.value.as_i64().is_some(), "{:?}", id_after.value);
        let reg_after = host
            .host_call(&r, &call(m::IS_REGISTERED, json!({})), None)
            .unwrap();
        assert_eq!(reg_after.value, json!(true));

        // A. exec_sql_with + read_sql_with with named :param binds.
        let ddl = host
            .host_call(
                &r,
                &call(
                    m::EXEC_SQL_WITH,
                    json!({
                        "connector_keys": "sql:local",
                        "query": "CREATE TABLE cmdq (id INTEGER PRIMARY KEY, counter TEXT, note TEXT)",
                        "params": {}
                    }),
                ),
                None,
            )
            .unwrap();
        assert_eq!(ddl.value[0], json!(true), "{:?}", ddl.value);

        let ins = host
            .host_call(
                &r,
                &call(
                    m::EXEC_SQL_WITH,
                    json!({
                        "connector_keys": "sql:local",
                        "query": "INSERT INTO cmdq (id, counter, note) VALUES (:id, :counter, :note)",
                        "params": {"id": 1, "counter": "c1", "note": null}
                    }),
                ),
                None,
            )
            .unwrap();
        assert_eq!(ins.value[0], json!(true), "{:?}", ins.value);

        let read = host
            .host_call(
                &r,
                &call(
                    m::READ_SQL_WITH,
                    json!({
                        "connector_keys": "sql:local",
                        "query": "SELECT id, counter FROM cmdq WHERE counter = :counter",
                        "params": {"counter": "c1"}
                    }),
                ),
                None,
            )
            .unwrap();
        assert_eq!(read.data.unwrap().height(), 1);

        // E. Transaction with lock_key: commit persists, rollback discards.
        let tx1 = host
            .host_call(
                &r,
                &call(
                    m::TX_BEGIN,
                    json!({"connector_keys": "sql:local", "lock_key": "cmdq:c1"}),
                ),
                None,
            )
            .unwrap();
        let tx1_id = tx1.value["tx_id"].as_u64().unwrap();
        let tx1_ins = host
            .host_call(
                &r,
                &call(
                    m::TX_EXEC_SQL,
                    json!({
                        "tx_id": tx1_id,
                        "query": "INSERT INTO cmdq (id, counter, note) VALUES (:id, :counter, :note)",
                        "params": {"id": 2, "counter": "c1", "note": "committed"}
                    }),
                ),
                None,
            )
            .unwrap();
        assert_eq!(tx1_ins.value[0], json!(true));
        let tx1_read = host
            .host_call(
                &r,
                &call(
                    m::TX_READ_SQL,
                    json!({
                        "tx_id": tx1_id,
                        "query": "SELECT id FROM cmdq WHERE counter = :c ORDER BY id",
                        "params": {"c": "c1"}
                    }),
                ),
                None,
            )
            .unwrap();
        assert_eq!(tx1_read.data.unwrap().height(), 2);
        let commit = host
            .host_call(&r, &call(m::TX_COMMIT, json!({"tx_id": tx1_id})), None)
            .unwrap();
        assert_eq!(commit.value[0], json!(true));

        let tx2 = host
            .host_call(
                &r,
                &call(
                    m::TX_BEGIN,
                    json!({"connector_keys": "sql:local", "lock_key": "cmdq:c1"}),
                ),
                None,
            )
            .unwrap();
        let tx2_id = tx2.value["tx_id"].as_u64().unwrap();
        let _ = host
            .host_call(
                &r,
                &call(
                    m::TX_EXEC_SQL,
                    json!({
                        "tx_id": tx2_id,
                        "query": "INSERT INTO cmdq (id, counter, note) VALUES (:id, :counter, :note)",
                        "params": {"id": 3, "counter": "c1", "note": "rolled_back"}
                    }),
                ),
                None,
            )
            .unwrap();
        let rb = host
            .host_call(&r, &call(m::TX_ROLLBACK, json!({"tx_id": tx2_id})), None)
            .unwrap();
        assert_eq!(rb.value[0], json!(true));

        let final_read = host
            .host_call(
                &r,
                &call(
                    m::READ_SQL_WITH,
                    json!({
                        "connector_keys": "sql:local",
                        "query": "SELECT id FROM cmdq WHERE counter = :c",
                        "params": {"c": "c1"}
                    }),
                ),
                None,
            )
            .unwrap();
        assert_eq!(final_read.data.unwrap().height(), 2);
    }
}
