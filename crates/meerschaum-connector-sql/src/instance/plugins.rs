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

/// How many times [`SqlConnector::register_plugin`] reads and writes the plugin's row before it
/// gives up because another process keeps changing it.
const REGISTER_ATTEMPTS: usize = 3;

impl SqlConnector {
    /// `register_plugin(plugin, force)`: insert or update the plugin's row. Without `force`,
    /// the new version must be greater than the registered one.
    ///
    /// New in 5.0 (the unique key on `plugin_name`, `unique_keys.rs`): when another process
    /// registers the plugin between the existence check and the insert, the registration
    /// becomes an update of that row (with its version check), refused when the row belongs to
    /// another user than `plugin.user_id`. An update only applies to the row as it was read
    /// (same id and owner), and a row which keeps changing fails the call after a few attempts.
    pub async fn register_plugin(&self, plugin: &PluginRecord, force: bool) -> SuccessTuple {
        self.register_plugin_with(plugin, force, false).await
    }

    /// [`Self::register_plugin`] for a caller which verified the owner (the API's plugin upload
    /// route): an existing row is only updated while its owner is `plugin.user_id`, so a row
    /// another user registers after the caller's check keeps its owner. The refusal is
    /// [`meerschaum_connector::pipe_ops::plugin_owner_conflict`].
    pub async fn register_plugin_as_owner(
        &self,
        plugin: &PluginRecord,
        force: bool,
    ) -> SuccessTuple {
        self.register_plugin_with(plugin, force, true).await
    }

    // `keep_owner` is opt-in rather than the default: v4's `register_plugin` (and so the public
    // Python `SQLConnector.register_plugin`) moves an existing row to `plugin.user_id`.
    async fn register_plugin_with(
        &self,
        plugin: &PluginRecord,
        force: bool,
        keep_owner: bool,
    ) -> SuccessTuple {
        let _ = self.ensure_instance_tables(true).await;
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
        let requested_owner = match &user_id {
            SqlParam::Int(i) => Some(*i),
            _ => None,
        };
        let another_user = || meerschaum_connector::pipe_ops::plugin_owner_conflict(plugin);
        // The owner of the row when this call first saw it.
        let mut seen_owner: Option<Option<i64>> = None;
        // Whether this call's insert lost to another process's.
        let mut lost_insert = false;
        for _ in 0..REGISTER_ATTEMPTS {
            let old_id = match self.get_plugin_id(plugin).await {
                Ok(id) => id,
                Err(e) => return SuccessTuple::fail(e.to_string()),
            };
            let mut owner = None;
            if old_id.is_some() {
                owner = self.get_plugin_user_id(plugin).await.ok().flatten();
                if keep_owner && owner.is_some() && owner != requested_owner {
                    return another_user();
                }
                match seen_owner {
                    // Registered by another process after this call found no row: it may only
                    // update a row of its own user.
                    None if lost_insert
                        && requested_owner.is_some()
                        && owner != requested_owner =>
                    {
                        return another_user();
                    }
                    None => seen_owner = Some(owner),
                    Some(seen) if seen != owner => return another_user(),
                    Some(_) => {}
                }
                if !force {
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
            }
            let mut params = vec![
                SqlParam::Text(plugin.name.clone()),
                SqlParam::opt_text(plugin.version.as_deref()),
                self.json_param(&plugin.attributes),
                user_id.clone(),
            ];
            let query = match old_id {
                None => format!(
                    "INSERT INTO {table} (plugin_name, version, attributes, user_id) VALUES ({})",
                    placeholders(f, 1, 4)
                ),
                Some(id) => {
                    params.push(SqlParam::Int(id));
                    // Only the row as it was read: not one whose owner changed since.
                    let owner_clause = match owner {
                        Some(owner) => {
                            params.push(SqlParam::Int(owner));
                            format!(" AND {table}.user_id = {}", placeholder(f, 6))
                        }
                        None => format!(" AND {table}.user_id IS NULL"),
                    };
                    format!(
                        "UPDATE {table} SET plugin_name={}, version={}, attributes={}, user_id={} WHERE {table}.plugin_id = {}{owner_clause}",
                        placeholder(f, 1),
                        placeholder(f, 2),
                        placeholder(f, 3),
                        placeholder(f, 4),
                        placeholder(f, 5)
                    )
                }
            };
            match self.exec_params(&query, &params).await {
                Some(n) if old_id.is_none() || n > 0 => {
                    return SuccessTuple::ok(format!("Successfully registered plugin '{plugin}'."));
                }
                // The row was deleted or changed owner after it was read: read it again.
                Some(_) => {}
                // Another process registered the plugin after the check above (the unique key
                // refused the second row): read its row and update it.
                None if old_id.is_none()
                    && matches!(self.get_plugin_id(plugin).await, Ok(Some(_))) =>
                {
                    lost_insert = true;
                }
                None => {
                    return SuccessTuple::fail(format!("Failed to register plugin '{plugin}'."));
                }
            }
        }
        SuccessTuple::fail(format!(
            "Failed to register plugin '{plugin}': its registration kept changing during {REGISTER_ATTEMPTS} attempts."
        ))
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
