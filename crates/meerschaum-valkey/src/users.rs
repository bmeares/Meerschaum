//! Users and plugins (`meerschaum/connectors/valkey/_users.py`, `_plugins.py`): registrations
//! are synced into the `mrsm_users` / `mrsm_plugins` pipes and each field is a separate key.

use indexmap::IndexMap;
use meerschaum_api::pipe_ops::{self, Doc};
use meerschaum_api::traits::{ConnectorResult, DataQuery, PluginRecord};
use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::value::Value as KValue;

use crate::connector::{ValkeyConnector, be};
use crate::keys::{PLUGINS_TABLE, USERS_TABLE, get_plugin_key, get_user_key};

/// The `valkey-py` error for setting `None` (`DataError`).
const NONE_VALUE_ERROR: &str =
    "Invalid input of type: 'NoneType'. Convert to a bytes, string, int or float first.";

fn internal_pipe(instance_keys: &str, metric: &str, column: &str, target: &str) -> Pipe {
    Pipe::builder("mrsm", metric)
        .instance(instance_keys)
        .kwarg("columns", Value::List(vec![Value::Str(column.into())]))
        .kwarg("temporary", true)
        .kwarg("target", target)
        .build()
        .expect("static internal pipe keys are valid")
}

fn compact_json(m: &Map) -> String {
    json_dumps(&Value::Map(m.clone()), JsonStyle::Compact, true)
}

fn user_id_str(user: &User) -> String {
    match &user.user_id {
        Some(v) if !v.is_null() => meerschaum_core::pyfmt::py_str(v),
        _ => "None".into(),
    }
}

impl ValkeyConnector {
    /// The pipe which stores the registered users
    /// (`Pipe('mrsm', 'users', columns=['user_id'], temporary=True, target='mrsm_users')`).
    pub fn valkey_users_pipe(&self) -> Pipe {
        internal_pipe(&self.to_string(), "users", "user_id", USERS_TABLE)
    }

    /// The pipe which stores the registered plugins.
    pub fn valkey_plugins_pipe(&self) -> Pipe {
        internal_pipe(&self.to_string(), "plugins", "plugin_name", PLUGINS_TABLE)
    }

    /// `get_user_keys_vals(user, mutable_only)`: the keys and values to set for a user (`None`
    /// for unset values), in v4's order.
    pub fn get_user_keys_vals(
        user: &User,
        password_hash: Option<String>,
        mutable_only: bool,
    ) -> Result<IndexMap<String, Option<String>>, String> {
        let user_id = user_id_str(user);
        let attributes = compact_json(&user.attributes.clone().unwrap_or_default());
        let mut mutable: IndexMap<String, Option<String>> = IndexMap::new();
        mutable.insert(
            get_user_key(&user_id, "attributes", false)?,
            Some(attributes),
        );
        mutable.insert(get_user_key(&user_id, "email", false)?, user.email.clone());
        mutable.insert(
            get_user_key(&user_id, "type", false)?,
            user.user_type.clone(),
        );
        mutable.insert(
            get_user_key(&user_id, "password_hash", false)?,
            password_hash,
        );
        if mutable_only {
            return Ok(mutable);
        }
        let mut all: IndexMap<String, Option<String>> = IndexMap::new();
        all.insert(
            get_user_key(&user_id, "username", false)?,
            Some(user.username.clone()),
        );
        all.insert(
            get_user_key(&user.username, "user_id", true)?,
            Some(user_id.clone()),
        );
        all.extend(mutable);
        Ok(all)
    }

    fn password_hash_of(user: &mut User) -> Option<String> {
        if user.password_hash.is_none() && user.password.is_empty() {
            return None;
        }
        user.get_password_hash().ok()
    }

    /// Register a new user: a new 12-character `user_id`, a row in `mrsm_users` and one key per
    /// field.
    pub async fn valkey_register_user(&self, user: &mut User) -> SuccessTuple {
        user.user_id = Some(Value::Str((self.id_factory)()));
        let password_hash = Self::password_hash_of(user);
        let keys_vals = match Self::get_user_keys_vals(user, password_hash, false) {
            Ok(kv) => kv,
            Err(e) => {
                return SuccessTuple::fail(format!("Failed to register '{}':\n{e}", user.username));
            }
        };
        let users_pipe = self.valkey_users_pipe();
        let doc: Doc = [
            ("user_id".to_string(), KValue::Str(user_id_str(user))),
            ("username".to_string(), KValue::Str(user.username.clone())),
        ]
        .into_iter()
        .collect();
        let res = pipe_ops::sync_docs(self, &users_pipe, vec![doc], false).await;
        if !res.success {
            return res;
        }
        let mut result = SuccessTuple::success();
        for (key, val) in &keys_vals {
            if let Some(val) = val {
                if let Err(e) = self.set(key, val).await {
                    result =
                        SuccessTuple::fail(format!("Failed to register '{}':\n{e}", user.username));
                    break;
                }
            }
        }
        if !result.success {
            let keys: Vec<String> = keys_vals.keys().cloned().collect();
            let _ = self.backend.del(&keys).await;
        }
        result
    }

    /// Return the ID for a user (`mrsm_user:username:{username}:user_id`), or `None`.
    pub async fn valkey_get_user_id(&self, user: &User) -> Option<String> {
        let key = get_user_key(&user.username, "user_id", true).ok()?;
        self.get(&key).await.ok().flatten()
    }

    async fn resolved_user_id(&self, user: &User) -> String {
        match &user.user_id {
            Some(v) if !v.is_null() => meerschaum_core::pyfmt::py_str(v),
            _ => self
                .valkey_get_user_id(user)
                .await
                .unwrap_or_else(|| "None".into()),
        }
    }

    /// Set each key, restoring the old values on failure.
    async fn set_or_restore(
        &self,
        keys_vals: &IndexMap<String, Option<String>>,
        old: &IndexMap<String, Option<String>>,
        what: &str,
    ) -> SuccessTuple {
        let mut result = SuccessTuple::success();
        for (key, val) in keys_vals {
            let res = match val {
                Some(v) => self.set(key, v).await.map_err(|e| e.to_string()),
                None => Err(NONE_VALUE_ERROR.to_string()),
            };
            if let Err(e) = res {
                result = SuccessTuple::fail(format!("Failed to {what}:\n{e}"));
                break;
            }
        }
        if !result.success {
            for (key, val) in old {
                match val {
                    Some(v) => {
                        if self.set(key, v).await.is_err() {
                            break;
                        }
                    }
                    // PARITY: restoring a missing value raises and stops the restore loop.
                    None => break,
                }
            }
        }
        result
    }

    /// Edit the mutable fields (attributes, email, type, password hash) of an existing user.
    ///
    /// PARITY: unset fields make `valkey-py` raise (`None` cannot be set), so editing a user
    /// without an email or type fails and restores the previous values (`_users.py`,
    /// `edit_user`).
    pub async fn valkey_edit_user(&self, user: &User) -> SuccessTuple {
        let mut user = user.clone();
        let password_hash = Self::password_hash_of(&mut user);
        let keys_vals = match Self::get_user_keys_vals(&user, password_hash, true) {
            Ok(kv) => kv,
            Err(e) => return SuccessTuple::fail(format!("Failed to edit user:\n{e}")),
        };
        let mut old = IndexMap::new();
        for key in keys_vals.keys() {
            match self.get(key).await {
                Ok(v) => {
                    old.insert(key.clone(), v);
                }
                Err(e) => return SuccessTuple::fail(format!("Failed to edit user:\n{e}")),
            }
        }
        self.set_or_restore(&keys_vals, &old, "edit user").await
    }

    /// Return the user's attributes (`None` if missing or invalid).
    pub async fn valkey_get_user_attributes(&self, user: &User) -> Option<Map> {
        let user_id = self.resolved_user_id(user).await;
        let key = get_user_key(&user_id, "attributes", false).ok()?;
        let s = self.get(&key).await.ok().flatten()?;
        Value::from_json_str(&s)
            .ok()
            .and_then(|v| v.as_map().cloned())
    }

    /// Delete a user's keys and its row in `mrsm_users`.
    pub async fn valkey_delete_user(&self, user: &User) -> SuccessTuple {
        let user_id = self.resolved_user_id(user).await;
        let users_pipe = self.valkey_users_pipe();
        let keys_vals = match Self::get_user_keys_vals(user, user.password_hash.clone(), false) {
            Ok(kv) => kv,
            Err(e) => return SuccessTuple::fail(format!("Failed to delete user:\n{e}")),
        };
        let mut old = IndexMap::new();
        for key in keys_vals.keys() {
            match self.get(key).await {
                Ok(v) => {
                    old.insert(key.clone(), v);
                }
                Err(e) => return SuccessTuple::fail(format!("Failed to delete user:\n{e}")),
            }
        }
        let mut params = Map::new();
        params.insert("user_id".into(), Value::Str(user_id));
        let cleared = self
            .clear_pipe(
                &users_pipe,
                &DataQuery {
                    params: Some(params),
                    ..Default::default()
                },
            )
            .await;
        if !cleared.success {
            return cleared;
        }
        let keys: Vec<String> = keys_vals.keys().cloned().collect();
        if let Err(e) = self.backend.del(&keys).await {
            for (key, val) in &old {
                match val {
                    Some(v) => {
                        if self.set(key, v).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            return SuccessTuple::fail(format!("Failed to delete user:\n{e}"));
        }
        SuccessTuple::success()
    }

    /// Get the registered usernames (from the `mrsm_users` pipe).
    pub async fn valkey_get_users(&self) -> ConnectorResult<Vec<String>> {
        let users_pipe = self.valkey_users_pipe();
        let docs = pipe_ops::get_docs(self, &users_pipe, &DataQuery::default()).await?;
        Ok(docs
            .iter()
            .filter_map(|d| d.get("username"))
            .map(|v| v.py_str())
            .collect())
    }

    async fn user_field(&self, user: &User, sub_key: &str) -> Option<String> {
        let user_id = self.resolved_user_id(user).await;
        let key = get_user_key(&user_id, sub_key, false).ok()?;
        self.get(&key).await.ok().flatten()
    }

    /// Return the user's password hash.
    pub async fn valkey_get_user_password_hash(&self, user: &User) -> Option<String> {
        self.user_field(user, "password_hash").await
    }

    /// Return the user's type.
    pub async fn valkey_get_user_type(&self, user: &User) -> Option<String> {
        self.user_field(user, "type").await
    }

    /// `get_plugin_keys_vals(plugin, mutable_only)`.
    pub fn get_plugin_keys_vals(
        plugin: &PluginRecord,
        mutable_only: bool,
    ) -> Result<IndexMap<String, Option<String>>, String> {
        let mut mutable: IndexMap<String, Option<String>> = IndexMap::new();
        mutable.insert(
            get_plugin_key(&plugin.name, "attributes")?,
            Some(compact_json(&plugin.attributes)),
        );
        mutable.insert(
            get_plugin_key(&plugin.name, "version")?,
            plugin.version.clone(),
        );
        if mutable_only {
            return Ok(mutable);
        }
        let mut all: IndexMap<String, Option<String>> = IndexMap::new();
        all.insert(
            get_plugin_key(&plugin.name, "user_id")?,
            plugin
                .user_id
                .as_ref()
                .filter(|v| !v.is_null())
                .map(meerschaum_core::pyfmt::py_str),
        );
        all.extend(mutable);
        Ok(all)
    }

    /// Register a new plugin to the `mrsm_plugins` "table".
    ///
    /// PARITY: unlike the base implementation, an existing registration is not checked
    /// (`_plugins.py`, `register_plugin`).
    pub async fn valkey_register_plugin(&self, plugin: &PluginRecord) -> SuccessTuple {
        let keys_vals = match Self::get_plugin_keys_vals(plugin, false) {
            Ok(kv) => kv,
            Err(e) => {
                return SuccessTuple::fail(format!(
                    "Failed to register plugin '{}':\n{e}",
                    plugin.name
                ));
            }
        };
        let plugins_pipe = self.valkey_plugins_pipe();
        let doc: Doc = [
            ("plugin_name".to_string(), KValue::Str(plugin.name.clone())),
            (
                "user_id".to_string(),
                plugin
                    .user_id
                    .as_ref()
                    .map(pipe_ops::core_to_kernel)
                    .unwrap_or(KValue::Null),
            ),
        ]
        .into_iter()
        .collect();
        let res = pipe_ops::sync_docs(self, &plugins_pipe, vec![doc], false).await;
        if !res.success {
            return res;
        }
        let mut result = SuccessTuple::success();
        for (key, val) in &keys_vals {
            if let Some(val) = val {
                if let Err(e) = self.set(key, val).await {
                    result = SuccessTuple::fail(format!(
                        "Failed to register plugin '{}':\n{e}",
                        plugin.name
                    ));
                    break;
                }
            }
        }
        if !result.success {
            let keys: Vec<String> = keys_vals.keys().cloned().collect();
            let _ = self.backend.del(&keys).await;
        }
        result
    }

    async fn plugin_field(&self, plugin: &PluginRecord, sub_key: &str) -> Option<String> {
        let key = get_plugin_key(&plugin.name, sub_key).ok()?;
        self.get(&key).await.ok().flatten()
    }

    /// Return a plugin's version.
    pub async fn valkey_get_plugin_version(&self, plugin: &PluginRecord) -> Option<String> {
        self.plugin_field(plugin, "version").await
    }

    /// Return a plugin's owner's user ID.
    pub async fn valkey_get_plugin_user_id(&self, plugin: &PluginRecord) -> Option<String> {
        self.plugin_field(plugin, "user_id").await
    }

    /// Return the username of a plugin's owner.
    pub async fn valkey_get_plugin_username(&self, plugin: &PluginRecord) -> Option<String> {
        let user_id = self.valkey_get_plugin_user_id(plugin).await?;
        let key = get_user_key(&user_id, "username", false).ok()?;
        self.get(&key).await.ok().flatten()
    }

    /// Return the attributes of a plugin (`{}` if missing).
    pub async fn valkey_get_plugin_attributes(&self, plugin: &PluginRecord) -> Map {
        match self.plugin_field(plugin, "attributes").await {
            Some(s) if !s.is_empty() => Value::from_json_str(&s)
                .ok()
                .and_then(|v| v.as_map().cloned())
                .unwrap_or_default(),
            _ => Map::new(),
        }
    }

    /// Delete a plugin from the plugins table and its keys.
    pub async fn valkey_delete_plugin(&self, plugin: &PluginRecord) -> SuccessTuple {
        let plugins_pipe = self.valkey_plugins_pipe();
        let mut params = Map::new();
        params.insert("plugin_name".into(), Value::Str(plugin.name.clone()));
        let cleared = self
            .clear_pipe(
                &plugins_pipe,
                &DataQuery {
                    params: Some(params),
                    ..Default::default()
                },
            )
            .await;
        if !cleared.success {
            return cleared;
        }
        let keys_vals = match Self::get_plugin_keys_vals(plugin, false) {
            Ok(kv) => kv,
            Err(e) => {
                return SuccessTuple::fail(format!(
                    "Failed to delete plugin '{}':\n{e}",
                    plugin.name
                ));
            }
        };
        let keys: Vec<String> = keys_vals.keys().cloned().collect();
        match self.backend.del(&keys).await.map_err(be) {
            Ok(_) => SuccessTuple::success(),
            Err(e) => {
                SuccessTuple::fail(format!("Failed to delete plugin '{}':\n{e}", plugin.name))
            }
        }
    }
}
