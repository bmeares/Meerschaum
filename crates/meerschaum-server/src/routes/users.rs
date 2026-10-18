//! `/users/*` (`api/routes/_users.py`).

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::routing::{delete, get, post};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Value};
use serde_json::{Value as Json, json};

use super::{Ctx, body_bytes, form_get, read_form};
use crate::auth::{Principal, is_uuid, scoped_auth, scoped_auth_if_private};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{from_core, ok_json, tuple, tuple_response};
use crate::state::AppState;

/// `DISALLOW_CHAINING_MESSAGE` (`api/_chain.py`).
pub const DISALLOW_CHAINING_MESSAGE: &str = "API chaining is not enabled on this Meerschaum instance!\n\nIf you manage this server and want to allow this API instance be a parent instance for other API instances,\nrun `edit config api` and search for `permissions`.\nUnder 'api:permissions:chaining', change the value of `child_apis` to `true`, and restart the API.\n\nIt is highly encouraged that you serve this API over HTTPS! Sensitive data like pipe data and salted\npassword hashes will be shared between APIs.";

/// Register the `users` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    // PARITY: Starlette keeps matching after a path-only match, so `DELETE /users/me`,
    // `/users/register` and `/users/edit` reach `DELETE /users/{username}`.
    app.route("/users/me", get(read_current_user).delete(delete_static))
        .route("/users", get(get_users))
        .route("/users/register", post(register_user).delete(delete_static))
        .route("/users/edit", post(edit_user).delete(delete_static))
        .route("/users/{username}/id", get(get_user_id))
        .route("/users/{username}/attributes", get(get_user_attributes))
        .route("/users/{username}", delete(delete_user))
        .route(
            "/users/{username}/password_hash",
            get(get_user_password_hash),
        )
        .route("/users/{username}/type", get(get_user_type))
}

/// The user behind a principal; tokens have no username (Python raises `AttributeError`).
fn principal_user(p: &Principal) -> Result<&User, ApiError> {
    p.as_user().ok_or_else(ApiError::internal)
}

fn user_id_json(v: Option<Value>) -> Json {
    match v {
        Some(id) if is_uuid(&id.py_str()) => Json::String(id.py_str()),
        Some(id) => from_core(&id),
        None => Json::Null,
    }
}

async fn read_current_user(ctx: Ctx) -> ApiResult {
    let state = &ctx.state;
    let principal = if state.config.no_auth {
        None
    } else {
        scoped_auth(state, &ctx.headers, &["users:read"]).await?
    };
    let Some(p) = principal else {
        return Ok(ok_json(&json!({
            "username": "no_auth",
            "user_id": null,
            "user_type": "admin",
            "attributes": {},
        })));
    };
    let user = principal_user(&p)?;
    let conn = state.instance();
    let user_id = conn
        .get_user_id(user)
        .await
        .map_err(|e| ApiError::exception("get_user_id", e))?;
    let user_type = conn
        .get_user_type(user)
        .await
        .map_err(|e| ApiError::exception("get_user_type", e))?;
    let attributes = conn
        .get_user_attributes(user)
        .await
        .map_err(|e| ApiError::exception("get_user_attributes", e))?;
    Ok(ok_json(&json!({
        "username": user.username,
        "user_id": user_id_json(user_id),
        "user_type": user_type,
        "attributes": attributes.map(|a| from_core(&Value::Map(a))),
    })))
}

async fn get_users(ctx: Ctx) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["users:read"]).await?;
    let users = ctx
        .state
        .instance()
        .get_users()
        .await
        .map_err(|e| ApiError::exception("get_users", e))?;
    Ok(ok_json(&json!(users)))
}

fn parse_attributes(raw: Option<&str>) -> Result<Option<Map>, ()> {
    match raw {
        None => Ok(None),
        Some(s) => meerschaum_core::pyfmt::string_to_dict(s)
            .map(Some)
            .map_err(|_| ()),
    }
}

async fn register_user(ctx: Ctx, body: Body) -> ApiResult {
    let state = &ctx.state;
    let form = read_form(&ctx.headers, body_bytes(body).await?).await;
    scoped_auth_if_private(state, &ctx.headers, &["users:register", "users:write"]).await?;
    let (Some(username), Some(password)) =
        (form_get(&form, "username"), form_get(&form, "password"))
    else {
        return Err(ApiError::new(
            406,
            "A username and password must be submitted.",
        ));
    };
    let Ok(attributes) = parse_attributes(form_get(&form, "attributes")) else {
        return Ok(tuple(
            false,
            "Invalid dictionary string received for attributes.",
        ));
    };
    if !state.config.registration_users {
        return Ok(tuple(
            false,
            "The administrator for this server has not allowed user registration.\n\n\
             Please contact the system administrator, or if you are running this server, \
             open the configuration file with `edit config api` and search for 'permissions'.  \
             Under the keys api:permissions:registration, \
             you can toggle various registration types.",
        ));
    }
    let user_type = form_get(&form, "type").map(String::from);
    if user_type.as_deref() == Some("admin") {
        return Ok(tuple(
            false,
            "New users cannot be of type 'admin' when using the API connector. \
             Register a normal user first, then edit the user from an authorized account, \
             or use a SQL connector instead.",
        ));
    }
    let conn = state.instance();
    let mut user = User::new(username, Some(password));
    user.user_type = user_type;
    user.email = form_get(&form, "email").map(String::from);
    user.attributes = attributes;
    user.instance_keys = Some(conn.connector_keys());
    Ok(tuple_response(&conn.register_user(&mut user).await))
}

async fn edit_user(ctx: Ctx, body: Body) -> ApiResult {
    let state = &ctx.state;
    let form = read_form(&ctx.headers, body_bytes(body).await?).await;
    let principal = scoped_auth(state, &ctx.headers, &["users:write"]).await?;
    let Ok(attributes) = parse_attributes(form_get(&form, "attributes")) else {
        return Ok(tuple(
            false,
            "Invalid dictionary string received for attributes.",
        ));
    };
    let conn = state.instance();
    let username = form_get(&form, "username").unwrap_or("None");
    let mut user = User::new(username, form_get(&form, "password"));
    user.email = form_get(&form, "email").map(String::from);
    user.attributes = attributes;
    user.instance_keys = Some(conn.connector_keys());
    let curr_user = match &principal {
        Some(p) => Some(principal_user(p)?),
        None => None,
    };
    let user_type = match curr_user {
        Some(u) => conn.get_user_type(u).await.ok().flatten(),
        None => Some("admin".into()),
    };
    let is_admin = user_type.as_deref() == Some("admin");
    if is_admin && let Some(t) = form_get(&form, "type") {
        user.user_type = Some(t.to_string());
    }
    if is_admin || curr_user.is_some_and(|u| u.username == user.username) {
        return Ok(tuple_response(&conn.edit_user(&user).await));
    }
    // PARITY: `HTTPException(status=403, ...)` is a `TypeError`, a 500 (`_users.py:156`).
    Err(ApiError::internal())
}

async fn get_user_id(ctx: Ctx, Path(username): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["users:read"]).await?;
    let id = ctx
        .state
        .instance()
        .get_user_id(&User::new(&username, None))
        .await
        .map_err(|e| ApiError::exception("get_user_id", e))?;
    Ok(ok_json(&user_id_json(id)))
}

async fn get_user_attributes(ctx: Ctx, Path(username): Path<String>) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["users:read"]).await?;
    let attrs = ctx
        .state
        .instance()
        .get_user_attributes(&User::new(&username, None))
        .await
        .map_err(|e| ApiError::exception("get_user_attributes", e))?
        .unwrap_or_default();
    Ok(ok_json(&from_core(&Value::Map(attrs))))
}

async fn delete_named(ctx: Ctx, username: String) -> ApiResult {
    let state = &ctx.state;
    let principal = if state.config.no_auth {
        None
    } else {
        scoped_auth(state, &ctx.headers, &["users:delete"]).await?
    };
    let conn = state.instance();
    let user = User::new(&username, None);
    let curr_user = match &principal {
        Some(p) => Some(principal_user(p)?),
        None => None,
    };
    let user_type = match curr_user {
        Some(u) => conn
            .get_user_type(u)
            .await
            .map_err(|e| ApiError::exception("get_user_type", e))?,
        None => Some("admin".into()),
    };
    if user_type.as_deref() == Some("admin") || curr_user.is_some_and(|u| u.username == username) {
        return Ok(tuple_response(&conn.delete_user(&user).await));
    }
    Ok(tuple(
        false,
        format!("Cannot delete user '{username}': Permission denied"),
    ))
}

async fn delete_user(ctx: Ctx, Path(username): Path<String>) -> ApiResult {
    delete_named(ctx, username).await
}

async fn delete_static(ctx: Ctx, uri: axum::http::Uri) -> ApiResult {
    let name = uri.path().rsplit('/').next().unwrap_or("").to_string();
    delete_named(ctx, name).await
}

async fn chained_field(ctx: Ctx, username: String, field: &str) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["users:read", "instance:chain"]).await?;
    if !ctx.state.config.chaining_child_apis {
        return Err(ApiError::new(403, DISALLOW_CHAINING_MESSAGE));
    }
    let conn = ctx.state.instance();
    let user = User::new(&username, None);
    let v = if field == "password_hash" {
        conn.get_user_password_hash(&user).await
    } else {
        conn.get_user_type(&user).await
    }
    .map_err(|e| ApiError::exception(field, e))?;
    Ok(ok_json(&json!(v)))
}

async fn get_user_password_hash(ctx: Ctx, Path(username): Path<String>) -> ApiResult {
    chained_field(ctx, username, "password_hash").await
}

async fn get_user_type(ctx: Ctx, Path(username): Path<String>) -> ApiResult {
    chained_field(ctx, username, "type").await
}
