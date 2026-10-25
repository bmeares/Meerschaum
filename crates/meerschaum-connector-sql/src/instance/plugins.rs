//! Plugin registrations in the `mrsm_plugins` table (`meerschaum/connectors/sql/_plugins.py`).

use meerschaum_connector::{ConnectorResult, PluginRecord};
use meerschaum_core::{Map, SuccessTuple, Value};

use crate::driver::SqlParam;
use crate::driver::params::{placeholder, placeholders};

use super::users::parse_attributes;
use super::{SqlConnector, be, frame_docs, kv_str};

/// `packaging.version.parse(a) >= packaging.version.parse(b)`; a forwarding wrapper over
/// [`meerschaum_core::version::version_ge`] (invalid versions compare false).
pub fn version_ge(a: &str, b: &str) -> bool {
    meerschaum_core::version::version_ge(a, b)
}

impl SqlConnector {
    /// `register_plugin(plugin, force)`: insert or update the plugin's row. Without `force`,
    /// the new version must be greater than the registered one.
    pub async fn register_plugin(&self, plugin: &PluginRecord, force: bool) -> SuccessTuple {
        let _ = self.ensure_instance_tables(true).await;
        let old_id = match self.get_plugin_id(plugin).await {
            Ok(id) => id,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        if old_id.is_some() && !force {
            let old_version = self
                .get_plugin_version(plugin)
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            let new_version = plugin.version.clone().unwrap_or_default();
            if !old_version.is_empty()
                && !new_version.is_empty()
                && version_ge(&old_version, &new_version)
            {
                return SuccessTuple::fail(format!(
                    "Version '{new_version}' of plugin '{plugin}' must be greater than existing version '{old_version}'."
                ));
            }
        }
        let table = self.instance_table_name("mrsm_plugins");
        let f = self.flavor;
        let user_id = match &plugin.user_id {
            None | Some(Value::Null) => SqlParam::Null,
            Some(Value::Int(i)) => SqlParam::Int(*i),
            Some(other) => other
                .py_str()
                .parse::<i64>()
                .map(SqlParam::Int)
                .unwrap_or_else(|_| SqlParam::Text(other.py_str())),
        };
        let mut params = vec![
            SqlParam::Text(plugin.name.clone()),
            SqlParam::opt_text(plugin.version.as_deref()),
            self.json_param(&plugin.attributes),
            user_id,
        ];
        let query = match old_id {
            None => format!(
                "INSERT INTO {table} (plugin_name, version, attributes, user_id) VALUES ({})",
                placeholders(f, 1, 4)
            ),
            Some(id) => {
                params.push(SqlParam::Int(id));
                format!(
                    "UPDATE {table} SET plugin_name={}, version={}, attributes={}, user_id={} WHERE {table}.plugin_id = {}",
                    placeholder(f, 1),
                    placeholder(f, 2),
                    placeholder(f, 3),
                    placeholder(f, 4),
                    placeholder(f, 5)
                )
            }
        };
        match self.exec_params(&query, &params).await {
            Some(_) => SuccessTuple::ok(format!("Successfully registered plugin '{plugin}'.")),
            // Another process registered the plugin after the check above (the unique key
            // refused the second row; `unique_keys.rs`): register again, as an update with its
            // version check. That call finds the row, so it never comes back here.
            None if old_id.is_none() && matches!(self.get_plugin_id(plugin).await, Ok(Some(_))) => {
                Box::pin(self.register_plugin(plugin, force)).await
            }
            None => SuccessTuple::fail(format!("Failed to register plugin '{plugin}'.")),
        }
    }

    async fn plugin_value(
        &self,
        plugin: &PluginRecord,
        column: &str,
    ) -> ConnectorResult<Option<crate::driver::SqlValue>> {
        if !self.ensure_instance_tables(true).await {
            return Ok(None);
        }
        let table = self.instance_table_name("mrsm_plugins");
        let query = format!(
            "SELECT {table}.{column} \nFROM {table} \nWHERE {table}.plugin_name = {}",
            placeholder(self.flavor, 1)
        );
        self.value_params(&query, &[SqlParam::Text(plugin.name.clone())])
            .await
    }

    /// `get_plugin_id(plugin)`.
    pub async fn get_plugin_id(&self, plugin: &PluginRecord) -> ConnectorResult<Option<i64>> {
        Ok(self
            .plugin_value(plugin, "plugin_id")
            .await
            .ok()
            .flatten()
            .and_then(|v| v.as_i64()))
    }

    /// `get_plugin_version(plugin)`.
    pub async fn get_plugin_version(
        &self,
        plugin: &PluginRecord,
    ) -> ConnectorResult<Option<String>> {
        Ok(self
            .plugin_value(plugin, "version")
            .await?
            .map(|v| v.to_string()))
    }

    /// `get_plugin_user_id(plugin)`.
    pub async fn get_plugin_user_id(&self, plugin: &PluginRecord) -> ConnectorResult<Option<i64>> {
        Ok(self
            .plugin_value(plugin, "user_id")
            .await
            .ok()
            .flatten()
            .and_then(|v| v.as_i64()))
    }

    /// `get_plugin_username(plugin)`.
    ///
    /// PARITY: Python joins the conditions with `and`, which SQLAlchemy reduces to the first
    /// clause, so the query cross-joins users and plugins and returns the first owner's name
    /// regardless of `plugin` (`_plugins.py:186`).
    pub async fn get_plugin_username(
        &self,
        plugin: &PluginRecord,
    ) -> ConnectorResult<Option<String>> {
        let _ = plugin;
        let users = self.instance_table_name("mrsm_users");
        let plugins = self.instance_table_name("mrsm_plugins");
        let query = format!(
            "SELECT {users}.username \nFROM {users}, {plugins} \nWHERE {users}.user_id = {plugins}.user_id"
        );
        Ok(self.value(&query).await.map(|v| v.to_string()))
    }

    /// `get_plugin_attributes(plugin)` (`{}` if unset).
    pub async fn get_plugin_attributes(&self, plugin: &PluginRecord) -> ConnectorResult<Map> {
        Ok(self
            .plugin_value(plugin, "attributes")
            .await?
            .and_then(|v| parse_attributes(&v))
            .unwrap_or_default())
    }

    /// `get_plugins(user_id, search_term)`: plugin names, optionally owned by `user_id` and
    /// starting with `search_term`.
    pub async fn get_plugins(
        &self,
        user_id: Option<&Value>,
        search_term: Option<&str>,
    ) -> ConnectorResult<Vec<String>> {
        let _ = self.ensure_instance_tables(true).await;
        let table = self.instance_table_name("mrsm_plugins");
        let f = self.flavor;
        let mut wheres = Vec::new();
        let mut params = Vec::new();
        if let Some(uid) = user_id.filter(|u| !u.is_null()) {
            params.push(match uid {
                Value::Int(i) => SqlParam::Int(*i),
                other => SqlParam::Text(other.py_str()),
            });
            wheres.push(format!(
                "{table}.user_id = {}",
                placeholder(f, params.len())
            ));
        }
        if let Some(term) = search_term {
            params.push(SqlParam::Text(format!("{term}%")));
            wheres.push(format!(
                "{table}.plugin_name LIKE {}",
                placeholder(f, params.len())
            ));
        }
        let mut query = format!("SELECT {table}.plugin_name \nFROM {table}");
        if !wheres.is_empty() {
            query.push_str(&format!(" \nWHERE {}", wheres.join(" AND ")));
        }
        let df = self.read_params(&query, &params).await.map_err(be)?;
        Ok(frame_docs(&df)
            .iter()
            .filter_map(|d| kv_str(d.get("plugin_name")))
            .collect())
    }

    /// `delete_plugin(plugin)`.
    pub async fn delete_plugin(&self, plugin: &PluginRecord) -> SuccessTuple {
        let plugin_id = match self.get_plugin_id(plugin).await {
            Ok(Some(id)) => id,
            Ok(None) => return SuccessTuple::ok(format!("Plugin '{plugin}' was not registered.")),
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let table = self.instance_table_name("mrsm_plugins");
        let query = format!(
            "DELETE FROM {table} WHERE {table}.plugin_id = {}",
            placeholder(self.flavor, 1)
        );
        match self.exec_params(&query, &[SqlParam::Int(plugin_id)]).await {
            Some(_) => SuccessTuple::ok(format!("Successfully deleted plugin '{plugin}'.")),
            None => SuccessTuple::fail(format!("Failed to delete plugin '{plugin}'.")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions() {
        assert!(version_ge("1.0.0", "1.0"));
        assert!(!version_ge("1.0.0", "1.0.1"));
        assert!(version_ge("1.10", "1.9"));
        assert!(!version_ge("1.0rc1", "1.0"));
        assert!(version_ge("1.0.post1", "1.0"));
    }
}
