//! User routes (`meerschaum/connectors/api/_users.py`).

use meerschaum_core::users::User;
use meerschaum_core::{Map, SuccessTuple, Value};

use crate::models::CurrentUserResponse;
use crate::pipe_ops;
use crate::routes;
use crate::traits::ConnectorResult;

use super::pipes::py_json;
use super::{APIConnector, ApiResponse, Body, RequestOptions};

/// The v4 parse of a user route's result: `{"detail": ...}` fails, a list is the tuple, anything
/// unparseable falls back to `fallback`.
fn user_tuple(response: &ApiResponse, fallback: String) -> SuccessTuple {
    match response.json() {
        Ok(serde_json::Value::Object(o)) if o.contains_key("detail") => {
            SuccessTuple::fail(match &o["detail"] {
                serde_json::Value::String(s) => s.clone(),
                other => crate::models::detail_repr(other),
            })
        }
        Ok(serde_json::Value::Array(items)) if items.len() >= 2 => SuccessTuple {
            success: items[0].as_bool().unwrap_or(false),
            message: items[1]
                .as_str()
                .map(String::from)
                .unwrap_or_else(|| items[1].to_string()),
        },
        _ => SuccessTuple::fail(fallback),
    }
}

fn attributes_json(user: &User) -> String {
    py_json(&Value::Map(user.attributes.clone().unwrap_or_default()))
}

impl APIConnector {
    /// `get_users()`: `GET /users` (`[]` on errors).
    pub async fn get_users(&self) -> ConnectorResult<Vec<String>> {
        let r = self.get(routes::USERS, RequestOptions::default()).await?;
        if !r.ok() {
            return Ok(Vec::new());
        }
        Ok(r.json()
            .ok()
            .and_then(|j| serde_json::from_value::<Vec<String>>(j).ok())
            .unwrap_or_default())
    }

    /// `edit_user(user)`: `POST /users/edit` as a form (`type`, `email` omitted when `None`).
    pub async fn edit_user(&self, user: &User) -> SuccessTuple {
        // `requests` drops `None` form values; an empty password is `None` in v4's `User`.
        let mut form = vec![("username".to_string(), user.username.clone())];
        if !user.password.is_empty() {
            form.push(("password".into(), user.password.clone()));
        }
        if let Some(t) = &user.user_type {
            form.push(("type".into(), t.clone()));
        }
        if let Some(e) = &user.email {
            form.push(("email".into(), e.clone()));
        }
        form.push(("attributes".into(), attributes_json(user)));
        let r = self
            .post(
                &format!("{}/edit", routes::USERS),
                RequestOptions {
                    body: Body::Form(form),
                    ..Default::default()
                },
            )
            .await;
        match r {
            Ok(r) => {
                let fallback = if r.ok() {
                    r.text()
                } else {
                    format!("Failed to edit user '{user}'.")
                };
                user_tuple(&r, fallback)
            }
            Err(_) => SuccessTuple::fail(format!("Failed to edit user '{user}'.")),
        }
    }

    /// `register_user(user)`: `POST /users/register` as a form.
    pub async fn register_user(&self, user: &User) -> SuccessTuple {
        let mut form = vec![("username".to_string(), user.username.clone())];
        if !user.password.is_empty() {
            form.push(("password".into(), user.password.clone()));
        }
        form.push(("attributes".into(), attributes_json(user)));
        if let Some(t) = user.user_type.as_ref().filter(|t| !t.is_empty()) {
            form.push(("type".into(), t.clone()));
        }
        if let Some(e) = user.email.as_ref().filter(|e| !e.is_empty()) {
            form.push(("email".into(), e.clone()));
        }
        let r = self
            .post(
                &format!("{}/register", routes::USERS),
                RequestOptions {
                    body: Body::Form(form),
                    ..Default::default()
                },
            )
            .await;
        match r {
            Ok(r) => {
                let fallback = if r.ok() {
                    r.text()
                } else {
                    format!("Failed to register user '{user}'.")
                };
                user_tuple(&r, fallback)
            }
            Err(_) => SuccessTuple::fail(format!("Failed to register user '{user}'.")),
        }
    }

    /// `get_user_id(user)`: `GET /users/{username}/id` as an int, a UUID string or a string.
    ///
    /// PARITY: an error body is not detected: `str()` of the parsed `{"detail": ...}` dict is
    /// returned as the ID (`_users.py`, `get_user_id`).
    pub async fn get_user_id(&self, user: &User) -> ConnectorResult<Option<Value>> {
        let r = self
            .get(
                &format!("{}/{}/id", routes::USERS, user.username),
                RequestOptions::default(),
            )
            .await?;
        let Ok(j) = r.json() else {
            return Ok(None);
        };
        let text = meerschaum_core::pyfmt::py_str(&pipe_ops::json_to_core(&j));
        if meerschaum_core::pyfmt::is_int(&text)
            && let Ok(i) = text.trim().parse()
        {
            return Ok(Some(Value::Int(i)));
        }
        Ok(Some(Value::Str(text)))
    }

    /// `delete_user(user)`: `DELETE /users/{username}`.
    pub async fn delete_user(&self, user: &User) -> SuccessTuple {
        let r = self
            .delete(
                &format!("{}/{}", routes::USERS, user.username),
                RequestOptions::default(),
            )
            .await;
        let fallback = format!("Failed to delete user '{}'.", user.username);
        match r {
            Ok(r) => user_tuple(&r, fallback),
            Err(_) => SuccessTuple::fail(fallback),
        }
    }

    /// `get_user_attributes(user)`: `GET /users/{username}/attributes` (`None` if unparseable).
    pub async fn get_user_attributes(&self, user: &User) -> ConnectorResult<Option<Value>> {
        let r = self
            .get(
                &format!("{}/{}/attributes", routes::USERS, user.username),
                RequestOptions::default(),
            )
            .await?;
        Ok(r.json().ok().map(|j| pipe_ops::json_to_core(&j)))
    }

    /// `get_user_password_hash(user)`: `GET /users/{username}/password_hash` (chaining only).
    pub async fn get_user_password_hash(&self, user: &User) -> ConnectorResult<Option<String>> {
        self.get_user_str(user, "password_hash").await
    }

    /// `get_user_type(user)`: `GET /users/{username}/type` (chaining only).
    pub async fn get_user_type(&self, user: &User) -> ConnectorResult<Option<String>> {
        self.get_user_str(user, "type").await
    }

    async fn get_user_str(&self, user: &User, field: &str) -> ConnectorResult<Option<String>> {
        let r = self
            .get(
                &format!("{}/{}/{field}", routes::USERS, user.username),
                RequestOptions::default(),
            )
            .await?;
        if !r.ok() {
            return Ok(None);
        }
        Ok(match r.json()? {
            serde_json::Value::Null => None,
            serde_json::Value::String(s) => Some(s),
            other => Some(other.to_string()),
        })
    }

    /// `GET /users/me`: the current user.
    pub async fn get_current_user(&self) -> ConnectorResult<CurrentUserResponse> {
        let j = self
            .get(&format!("{}/me", routes::USERS), RequestOptions::default())
            .await?
            .json_or_detail()?;
        Ok(serde_json::from_value(j)?)
    }

    /// The attributes of the current user as a map (convenience over [`Self::get_current_user`]).
    pub async fn get_current_user_attributes(&self) -> ConnectorResult<Map> {
        Ok(self
            .get_current_user()
            .await?
            .attributes
            .unwrap_or_default())
    }
}
