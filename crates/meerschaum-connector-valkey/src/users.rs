//! Users and plugins (`meerschaum/connectors/valkey/_users.py`, `_plugins.py`): registrations
//! are synced into the `mrsm_users` / `mrsm_plugins` pipes and each field is a separate key.

use indexmap::IndexMap;
use meerschaum_connector::pipe_ops::{self, Doc};
use meerschaum_connector::traits::{ConnectorResult, DataQuery, PluginRecord};
use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::value::Value as KValue;

use crate::backend::WriteOp;
use crate::connector::{ValkeyConnector, be};
use crate::keys::{
    PLUGINS_TABLE, USERS_TABLE, get_document_key, get_plugin_key, get_user_key, quote_table,
};

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

    /// [`password_hash_of`](Self::password_hash_of) on a blocking thread: PBKDF2 takes long enough
    /// to starve the client's I/O when several registrations run at once on the runtime's workers.
    async fn password_hash_off_runtime(user: &mut User) -> Option<String> {
        if user.password_hash.is_some() || user.password.is_empty() {
            return Self::password_hash_of(user);
        }
        let mut copy = user.clone();
        match tokio::task::spawn_blocking(move || {
            let hash = Self::password_hash_of(&mut copy);
            (copy.password_hash, hash)
        })
        .await
        {
            Ok((cached, hash)) => {
                user.password_hash = cached;
                hash
            }
            Err(_) => Self::password_hash_of(user),
        }
    }

    /// Register a new user: a new 12-character `user_id`, a row in `mrsm_users` and one key per
    /// field.
    pub async fn valkey_register_user(&self, user: &mut User) -> SuccessTuple {
        user.user_id = Some(Value::Str((self.id_factory)()));
        let password_hash = Self::password_hash_off_runtime(user).await;
        let keys_vals = match Self::get_user_keys_vals(user, password_hash, false) {
            Ok(kv) => kv,
            Err(e) => {
                return SuccessTuple::fail(format!("Failed to register '{}':\n{e}", user.username));
            }
        };
        let claim_key = match get_user_key(&user.username, "user_id", true) {
            Ok(key) => key,
            Err(e) => {
                return SuccessTuple::fail(format!("Failed to register '{}':\n{e}", user.username));
            }
        };
        let user_id = user_id_str(user);
        let fail = |e: &dyn std::fmt::Display| {
            SuccessTuple::fail(format!("Failed to register '{}':\n{e}", user.username))
        };
        let exists = || SuccessTuple::fail(format!("User '{}' already exists.", user.username));
        match self.get(&claim_key).await {
            Ok(Some(_)) => return exists(),
            Ok(None) => {}
            Err(e) => return fail(&e),
        }
        // The row goes in first, under this attempt's new `user_id`; the username key (the claim)
        // and the field keys are then written in one atomic step. Of two processes registering
        // the username at once only one claims it (v4 wrote over an existing user's keys
        // here), and a failure removes this attempt's row. A row without its claim (a process
        // that died in between) is not listed and is removed by the next registration.
        let users_pipe = self.valkey_users_pipe();
        let doc: Doc = [
            ("user_id".to_string(), KValue::Str(user_id.clone())),
            ("username".to_string(), KValue::Str(user.username.clone())),
        ]
        .into_iter()
        .collect();
        let res = pipe_ops::sync_docs(self, &users_pipe, vec![doc], false).await;
        if !res.success {
            self.remove_user_row(&user_id).await;
            return res;
        }
        let ops: Vec<WriteOp> = keys_vals
            .iter()
            .filter(|(key, _)| **key != claim_key)
            .filter_map(|(key, val)| {
                val.as_ref().map(|v| WriteOp::Set {
                    key: key.clone(),
                    value: v.clone(),
                })
            })
            .collect();
        match self
            .backend
            .claim_and_write(&claim_key, &user_id, &ops)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.remove_user_row(&user_id).await;
                return exists();
            }
            Err(e) => match self.get(&claim_key).await {
                // A reply lost after the script ran (e.g. a timeout) still registered the user.
                Ok(Some(v)) if v == user_id => {}
                // Not claimed by this attempt: drop its row.
                Ok(_) => {
                    self.remove_user_row(&user_id).await;
                    return fail(&e);
                }
                // Unknown: keep the row (unlisted without its claim) rather than orphan a claim.
                Err(_) => return fail(&e),
            },
        }
        self.remove_unclaimed_user_rows(&user.username, &user_id)
            .await;
        SuccessTuple::success()
    }

    /// Remove the `mrsm_users` row of `user_id` (its index member and its document key), even
    /// if only part of it was written. Best effort.
    async fn remove_user_row(&self, user_id: &str) {
        let table_name = quote_table(USERS_TABLE);
        let indices = vec!["user_id".to_string()];
        let doc: Doc = [("user_id".to_string(), KValue::Str(user_id.to_string()))]
            .into_iter()
            .collect();
        let _ = self
            .backend
            .srem(&table_name, &get_document_key(&doc, &indices, None))
            .await;
        let _ = self
            .backend
            .del(&[get_document_key(&doc, &indices, Some(&table_name))])
            .await;
    }

    /// Remove the `mrsm_users` rows of `username` other than `user_id`'s (left by registrations
    /// which wrote their row but never claimed the username). Best effort.
    async fn remove_unclaimed_user_rows(&self, username: &str, user_id: &str) {
        let users_pipe = self.valkey_users_pipe();
        let mut params = Map::new();
        params.insert("username".into(), Value::Str(username.to_string()));
        let query = DataQuery {
            params: Some(params),
            ..Default::default()
        };
        let Ok(docs) = pipe_ops::get_docs(self, &users_pipe, &query).await else {
            return;
        };
        for doc in docs {
            let Some(other) = doc.get("user_id").map(|v| v.py_str()) else {
                continue;
            };
            if other != user_id
                && doc.get("username").map(|v| v.py_str()).as_deref() == Some(username)
            {
                self.remove_user_row(&other).await;
            }
        }
    }

    /// Return the ID for a user (`mrsm_user:username:{username}:user_id`), or `None` when it is
    /// not registered (a username that cannot be part of a key never is).
    ///
    /// 5.0: a read that fails is an error. v4 answered `None`, so a caller deciding who someone
    /// is could take a failed read for "no such user"; the user reads below do the same.
    pub async fn valkey_get_user_id(&self, user: &User) -> ConnectorResult<Option<String>> {
        let Ok(key) = get_user_key(&user.username, "user_id", true) else {
            return Ok(None);
        };
        self.get(&key).await
    }

    /// The user's ID (`user.user_id`, else its lookup), or `"None"` when it is not registered
    /// (v4 then reads the keys of the ID `None`, which do not exist).
    async fn resolved_user_id(&self, user: &User) -> ConnectorResult<String> {
        Ok(match &user.user_id {
            Some(v) if !v.is_null() => meerschaum_core::pyfmt::py_str(v),
            _ => self
                .valkey_get_user_id(user)
                .await?
                .unwrap_or_else(|| "None".into()),
        })
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
        let password_hash = Self::password_hash_off_runtime(&mut user).await;
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

    /// Return the user's attributes (`None` if missing or not a JSON object).
    ///
    /// 5.0: a read that fails is an error, never `None`. A user without a `scopes` attribute
    /// holds every scope (v4's rule), so a failed read answered as `None` granted every scope.
    pub async fn valkey_get_user_attributes(&self, user: &User) -> ConnectorResult<Option<Map>> {
        let user_id = self.resolved_user_id(user).await?;
        let Ok(key) = get_user_key(&user_id, "attributes", false) else {
            return Ok(None);
        };
        Ok(self.get(&key).await?.and_then(|s| {
            Value::from_json_str(&s)
                .ok()
                .and_then(|v| v.as_map().cloned())
        }))
    }

    /// Replace a user's attributes (the `attributes` key) and nothing else.
    pub async fn valkey_set_user_attributes(&self, user: &User, attributes: &Map) -> SuccessTuple {
        let user_id = match &user.user_id {
            Some(v) if !v.is_null() => meerschaum_core::pyfmt::py_str(v),
            _ => match self.valkey_get_user_id(user).await {
                Ok(Some(id)) => id,
                Ok(None) => {
                    return SuccessTuple::fail(format!("User '{}' does not exist.", user.username));
                }
                Err(e) => return SuccessTuple::fail(format!("Failed to edit user:\n{e}")),
            },
        };
        let key = match get_user_key(&user_id, "attributes", false) {
            Ok(k) => k,
            Err(e) => return SuccessTuple::fail(format!("Failed to edit user:\n{e}")),
        };
        match self.set(&key, &compact_json(attributes)).await {
            Ok(_) => SuccessTuple::success(),
            Err(e) => SuccessTuple::fail(format!("Failed to edit user:\n{e}")),
        }
    }

    /// Delete a user's keys and its row in `mrsm_users`.
    pub async fn valkey_delete_user(&self, user: &User) -> SuccessTuple {
        let user_id = match self.resolved_user_id(user).await {
            Ok(user_id) => user_id,
            Err(e) => return SuccessTuple::fail(format!("Failed to delete user:\n{e}")),
        };
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
        // The user leaves every team and their pipes keep their team, without an owner (every
        // surface reaches this method: the trait, the Python façade, the API server).
        let forgotten = meerschaum_connector::teams::forget_user(self, &user.username).await;
        if !forgotten.success {
            tracing::warn!("{}", forgotten.message);
        }
        SuccessTuple::success()
    }

    /// Get the registered usernames (from the `mrsm_users` pipe).
    pub async fn valkey_get_users(&self) -> ConnectorResult<Vec<String>> {
        let users_pipe = self.valkey_users_pipe();
        let docs = pipe_ops::get_docs(self, &users_pipe, &DataQuery::default()).await?;
        let mut users = Vec::new();
        for doc in &docs {
            let Some(username) = doc.get("username").map(|v| v.py_str()) else {
                continue;
            };
            // A row whose username is not claimed by its `user_id` is a registration that
            // never finished (see `valkey_register_user`).
            let user_id = doc.get("user_id").map(|v| v.py_str());
            let claimed = match get_user_key(&username, "user_id", true) {
                Ok(key) => self.get(&key).await?,
                Err(_) => None,
            };
            if claimed.is_some() && claimed == user_id {
                users.push(username);
            }
        }
        Ok(users)
    }

    async fn user_field(&self, user: &User, sub_key: &str) -> ConnectorResult<Option<String>> {
        let user_id = self.resolved_user_id(user).await?;
        let Ok(key) = get_user_key(&user_id, sub_key, false) else {
            return Ok(None);
        };
        self.get(&key).await
    }

    /// Return the user's password hash (`None` if missing; a failed read is an error).
    pub async fn valkey_get_user_password_hash(
        &self,
        user: &User,
    ) -> ConnectorResult<Option<String>> {
        self.user_field(user, "password_hash").await
    }

    /// Return the user's type (`None` if missing; a failed read is an error).
    pub async fn valkey_get_user_type(&self, user: &User) -> ConnectorResult<Option<String>> {
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
            if let Some(val) = val
                && let Err(e) = self.set(key, val).await
            {
                result = SuccessTuple::fail(format!(
                    "Failed to register plugin '{}':\n{e}",
                    plugin.name
                ));
                break;
            }
        }
        if !result.success {
            let keys: Vec<String> = keys_vals.keys().cloned().collect();
            let _ = self.backend.del(&keys).await;
        }
        result
    }

    /// New in 5.0: [`Self::valkey_register_plugin`] for a caller which verified the owner (the
    /// API's plugin upload route; `InstanceConnector::register_plugin_as_owner`). The owner key
    /// is claimed with `SET NX` before anything is written, so a registration owned by another
    /// user, including one which appears after the caller's check, is refused and keeps its
    /// owner. `register_plugin` itself keeps v4's unchecked overwrite.
    pub async fn valkey_register_plugin_as_owner(&self, plugin: &PluginRecord) -> SuccessTuple {
        let owner_key = match get_plugin_key(&plugin.name, "user_id") {
            Ok(key) => key,
            Err(e) => {
                return SuccessTuple::fail(format!(
                    "Failed to register plugin '{}':\n{e}",
                    plugin.name
                ));
            }
        };
        let requested = plugin
            .user_id
            .as_ref()
            .filter(|v| !v.is_null())
            .map(meerschaum_core::pyfmt::py_str);
        let conflict = || pipe_ops::plugin_owner_conflict(plugin);
        let failed = |e: String| {
            SuccessTuple::fail(format!("Failed to register plugin '{}':\n{e}", plugin.name))
        };
        match requested {
            Some(requested) => {
                let mut claimed = false;
                // The owner key is only deleted with the plugin: a few attempts cover a delete
                // between the `SET NX` and the read.
                for _ in 0..3 {
                    match self.backend.set_nx(&owner_key, &requested).await {
                        Ok(true) => {
                            claimed = true;
                            break;
                        }
                        Ok(false) => {}
                        Err(e) => return failed(e.to_string()),
                    }
                    match self.get(&owner_key).await {
                        Ok(Some(owner)) if owner == requested => {
                            claimed = true;
                            break;
                        }
                        Ok(Some(_)) => return conflict(),
                        Ok(None) => {}
                        Err(e) => return failed(e.to_string()),
                    }
                }
                if !claimed {
                    return failed("its owner kept changing.".into());
                }
            }
            None => match self.get(&owner_key).await {
                Ok(Some(_)) => return conflict(),
                Ok(None) => {}
                Err(e) => return failed(e.to_string()),
            },
        }
        self.valkey_register_plugin(plugin).await
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
