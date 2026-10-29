//! Users in the `mrsm_users` table (`meerschaum/connectors/sql/_users.py`). Password hashes use
//! `meerschaum_core::users` (passlib's `pbkdf2-sha256` modular-crypt format), so v4 user rows
//! validate unchanged.

use meerschaum_connector::ConnectorResult;
use meerschaum_core::users::{User, valid_username};
use meerschaum_core::{Map, SuccessTuple, Value};

use crate::driver::params::{placeholder, placeholders};
use crate::driver::{SqlParam, SqlValue};
use crate::flavor::Flavor;

use super::{SqlConnector, be, frame_docs, kv_str};

/// A user ID value as a bound parameter.
fn id_param(v: &Value) -> SqlParam {
    match v {
        Value::Int(i) => SqlParam::Int(*i),
        Value::Null => SqlParam::Null,
        other => match other.py_str().parse::<i64>() {
            Ok(i) => SqlParam::Int(i),
            Err(_) => SqlParam::Text(other.py_str()),
        },
    }
}

/// Parse a stored attributes value (JSONB, or JSON text).
pub(crate) fn parse_attributes(v: &SqlValue) -> Option<Map> {
    match v {
        SqlValue::Json(s) | SqlValue::Text(s) => match Value::from_json_str(s) {
            Ok(Value::Map(m)) => Some(m),
            Ok(Value::Str(inner)) => Value::from_json_str(&inner).ok().map(|v| v.to_map()),
            _ => {
                tracing::warn!("Received unexpected type for attributes: {s}");
                None
            }
        },
        _ => None,
    }
}

impl SqlConnector {
    /// `register_user(user)`.
    pub async fn register_user(&self, user: &mut User) -> SuccessTuple {
        let valid = valid_username(&user.username);
        if !valid.success {
            return valid;
        }
        match self.get_user_id(user).await {
            Ok(Some(_)) => {
                return SuccessTuple::fail(format!("User '{}' already exists.", user.username));
            }
            Ok(None) => {}
            Err(e) => return SuccessTuple::fail(e.to_string()),
        }
        let _ = self.ensure_instance_tables(true).await;
        let password_hash = match user.get_password_hash() {
            Ok(h) => h,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let attributes = user.attributes.clone().unwrap_or_default();
        let table = self.instance_table_name("mrsm_users");
        let query = format!(
            "INSERT INTO {table} (username, password_hash, email, user_type, attributes) VALUES ({})",
            placeholders(self.flavor, 1, 5)
        );
        let params = vec![
            SqlParam::Text(user.username.clone()),
            SqlParam::Text(password_hash),
            SqlParam::opt_text(user.email.as_deref()),
            SqlParam::opt_text(user.user_type.as_deref()),
            self.json_param(&attributes),
        ];
        match self.exec_params(&query, &params).await {
            Some(_) => {
                SuccessTuple::ok(format!("Successfully registered user '{}'.", user.username))
            }
            // Another process registered the username after the check above (the unique key
            // refused the second row; `unique_keys.rs`).
            None if matches!(self.get_user_id(user).await, Ok(Some(_))) => {
                SuccessTuple::fail(format!("User '{}' already exists.", user.username))
            }
            None => SuccessTuple::fail(format!("Failed to register user '{}'.", user.username)),
        }
    }

    /// `valid_username(username)`.
    pub fn valid_username(username: &str) -> SuccessTuple {
        valid_username(username)
    }

    /// `edit_user(user)`: update the user's row (password, email, type, attributes).
    ///
    /// PARITY: an unset email or type (`None != ''`) is written as `NULL` (`_users.py:157`).
    pub async fn edit_user(&self, user: &User) -> SuccessTuple {
        let user_id = match &user.user_id {
            Some(id) => Some(id.clone()),
            None => match self.get_user_id(user).await {
                Ok(id) => id,
                Err(e) => return SuccessTuple::fail(e.to_string()),
            },
        };
        let Some(user_id) = user_id else {
            return SuccessTuple::fail(format!(
                "User '{0}' does not exist. Register user '{0}' before editing.",
                user.username
            ));
        };
        let valid = valid_username(&user.username);
        if !valid.success {
            return valid;
        }
        let mut sets: Vec<(&str, SqlParam)> = vec![
            ("user_id", id_param(&user_id)),
            ("username", SqlParam::Text(user.username.clone())),
        ];
        // PARITY deviation: v4 (`_users.py:163`) also sets `user_id` on MSSQL, where it is an
        // `IDENTITY` column ("Cannot update identity column"), so every edit failed there.
        if self.flavor == Flavor::Mssql {
            sets.remove(0);
        }
        if !user.password.is_empty() {
            let mut u = user.clone();
            match u.get_password_hash() {
                Ok(h) => sets.push(("password_hash", SqlParam::Text(h))),
                Err(e) => return SuccessTuple::fail(e.to_string()),
            }
        }
        if user.email.as_deref() != Some("") {
            sets.push(("email", SqlParam::opt_text(user.email.as_deref())));
        }
        if let Some(attrs) = user.attributes.as_ref().filter(|a| !a.is_empty()) {
            sets.push(("attributes", self.json_param(attrs)));
        }
        if user.user_type.as_deref() != Some("") {
            sets.push(("user_type", SqlParam::opt_text(user.user_type.as_deref())));
        }
        let table = self.instance_table_name("mrsm_users");
        let f = self.flavor;
        let set_str: Vec<String> = sets
            .iter()
            .enumerate()
            .map(|(i, (k, _))| format!("{k}={}", placeholder(f, i + 1)))
            .collect();
        let query = format!(
            "UPDATE {table} SET {} WHERE {table}.user_id = {}",
            set_str.join(", "),
            placeholder(f, sets.len() + 1)
        );
        let mut params: Vec<SqlParam> = sets.into_iter().map(|(_, p)| p).collect();
        params.push(id_param(&user_id));
        match self.exec_params(&query, &params).await {
            Some(_) => SuccessTuple::ok(format!("Successfully edited user '{}'.", user.username)),
            None => SuccessTuple::fail(format!("Failed to edit user '{}'.", user.username)),
        }
    }

    /// `get_user_id(user)`: the user's `user_id` if registered.
    pub async fn get_user_id(&self, user: &User) -> ConnectorResult<Option<Value>> {
        if !self.ensure_instance_tables(true).await {
            return Ok(None);
        }
        let table = self.instance_table_name("mrsm_users");
        let query = format!(
            "SELECT {table}.user_id \nFROM {table} \nWHERE {table}.username = {}",
            placeholder(self.flavor, 1)
        );
        let v = self
            .value_params(&query, &[SqlParam::Text(user.username.clone())])
            .await?;
        Ok(v.and_then(|v| v.as_i64()).map(Value::Int))
    }

    async fn resolve_user_id(&self, user: &User) -> ConnectorResult<Option<Value>> {
        match &user.user_id {
            Some(id) => Ok(Some(id.clone())),
            None => self.get_user_id(user).await,
        }
    }

    async fn user_column(&self, user: &User, column: &str) -> ConnectorResult<Option<SqlValue>> {
        let Some(user_id) = self.resolve_user_id(user).await? else {
            return Ok(None);
        };
        let table = self.instance_table_name("mrsm_users");
        let query = format!(
            "SELECT {table}.{column} \nFROM {table} \nWHERE {table}.user_id = {}",
            placeholder(self.flavor, 1)
        );
        self.value_params(&query, &[id_param(&user_id)]).await
    }

    /// `get_user_attributes(user)`.
    pub async fn get_user_attributes(&self, user: &User) -> ConnectorResult<Option<Map>> {
        Ok(self
            .user_column(user, "attributes")
            .await?
            .and_then(|v| parse_attributes(&v)))
    }

    /// `get_user_password_hash(user)`.
    pub async fn get_user_password_hash(&self, user: &User) -> ConnectorResult<Option<String>> {
        Ok(self
            .user_column(user, "password_hash")
            .await?
            .map(|v| v.to_string()))
    }

    /// `get_user_type(user)`.
    pub async fn get_user_type(&self, user: &User) -> ConnectorResult<Option<String>> {
        Ok(self
            .user_column(user, "user_type")
            .await?
            .map(|v| v.to_string()))
    }

    /// The user's email address (`None` when unset or empty).
    pub async fn get_user_email(&self, user: &User) -> ConnectorResult<Option<String>> {
        Ok(self
            .user_column(user, "email")
            .await?
            .filter(|v| !matches!(v, SqlValue::Null))
            .map(|v| v.to_string())
            .filter(|e| !e.trim().is_empty()))
    }

    /// `get_username(user_id)` (the `InstanceConnector` method, over `mrsm_users`).
    pub async fn get_username(&self, user_id: &Value) -> ConnectorResult<Option<Value>> {
        let table = self.instance_table_name("mrsm_users");
        let query = format!(
            "SELECT {table}.username \nFROM {table} \nWHERE {table}.user_id = {}",
            placeholder(self.flavor, 1)
        );
        Ok(self
            .value_params(&query, &[id_param(user_id)])
            .await?
            .map(|v| Value::Str(v.to_string())))
    }

    /// `delete_user(user)`: delete the user and the plugins they own.
    pub async fn delete_user(&self, user: &User) -> SuccessTuple {
        let user_id = match self.resolve_user_id(user).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                return SuccessTuple::fail(format!(
                    "User '{}' is not registered and cannot be deleted.",
                    user.username
                ));
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let users = self.instance_table_name("mrsm_users");
        let plugins = self.instance_table_name("mrsm_plugins");
        let f = self.flavor;
        let q1 = format!(
            "DELETE FROM {users} WHERE {users}.user_id = {}",
            placeholder(f, 1)
        );
        if self.exec_params(&q1, &[id_param(&user_id)]).await.is_none() {
            return SuccessTuple::fail(format!("Failed to delete user '{}'.", user.username));
        }
        let q2 = format!(
            "DELETE FROM {plugins} WHERE {plugins}.user_id = {}",
            placeholder(f, 1)
        );
        if self.exec_params(&q2, &[id_param(&user_id)]).await.is_none() {
            return SuccessTuple::fail(format!(
                "Failed to delete plugins of user '{}'.",
                user.username
            ));
        }
        // The user leaves every team and their pipes keep their team, without an owner (every
        // surface reaches this method: the trait, the Python façade, the API server).
        let forgotten = meerschaum_connector::teams::forget_user(self, &user.username).await;
        if !forgotten.success {
            tracing::warn!("{}", forgotten.message);
        }
        SuccessTuple::ok(format!("Successfully deleted user '{}'", user.username))
    }

    /// `set_user_attributes(user, attributes)`: replace the user's attributes and nothing else
    /// (their type, email and password stay).
    pub async fn set_user_attributes(&self, user: &User, attributes: &Map) -> SuccessTuple {
        let user_id = match self.resolve_user_id(user).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                return SuccessTuple::fail(format!("User '{}' does not exist.", user.username));
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let table = self.instance_table_name("mrsm_users");
        let f = self.flavor;
        let query = format!(
            "UPDATE {table} SET attributes={} WHERE {table}.user_id = {}",
            placeholder(f, 1),
            placeholder(f, 2)
        );
        let params = vec![self.json_param(attributes), id_param(&user_id)];
        match self.exec_params(&query, &params).await {
            Some(_) => SuccessTuple::ok(format!("Successfully edited user '{}'.", user.username)),
            None => SuccessTuple::fail(format!("Failed to edit user '{}'.", user.username)),
        }
    }

    /// `get_users()`: the registered usernames.
    pub async fn get_users(&self) -> ConnectorResult<Vec<String>> {
        let _ = self.ensure_instance_tables(true).await;
        let table = self.instance_table_name("mrsm_users");
        let df = self
            .read(&format!("SELECT {table}.username \nFROM {table}"))
            .await
            .map_err(be)?;
        Ok(frame_docs(&df)
            .iter()
            .filter_map(|d| kv_str(d.get("username")))
            .collect())
    }
}
