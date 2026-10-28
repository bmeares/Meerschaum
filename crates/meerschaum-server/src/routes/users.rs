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
        // rc15 access model (the v5 shape): every user's permissions and teams, one user's.
        .route(
            "/users/permissions",
            get(list_permissions).delete(delete_static),
        )
        .route(
            "/users/{username}/permissions",
            get(get_permissions).patch(patch_permissions),
        )
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

/// One user's row of `GET /users/permissions` (`None` when the user does not exist): type, role,
/// effective permissions, teams, primary team, and the email address when `caller` may see it
/// (AC13: their own, administrators, the managers of a team the user is in).
async fn user_row(
    state: &AppState,
    caller: &crate::access::Access,
    username: &str,
    teams: Option<Vec<(String, meerschaum_core::access::TeamRole)>>,
) -> Result<Option<Json>, ApiError> {
    use meerschaum_core::access as rules;
    let conn = state.instance();
    let mut user = User::new(username, None);
    user.instance_keys = Some(conn.connector_keys());
    let read = |what: &'static str| {
        move |e: meerschaum_connector::ConnectorError| ApiError::exception(what, e)
    };
    let Some(user_id) = conn.get_user_id(&user).await.map_err(read("get_user_id"))? else {
        return Ok(None);
    };
    let user_type = conn
        .get_user_type(&user)
        .await
        .map_err(read("get_user_type"))?;
    let attributes = conn
        .get_user_attributes(&user)
        .await
        .map_err(read("get_user_attributes"))?;
    let teams = match teams {
        Some(teams) => super::teams::with_everyone(teams),
        None => crate::access::user_teams(&conn, username)
            .await
            .map_err(read("get_user_teams"))?,
    };
    let default_role = crate::access::default_role(state);
    let role = rules::user_role(user_type.as_deref(), attributes.as_ref(), default_role);
    let permissions = rules::ordered_permissions(&rules::user_permissions(
        user_type.as_deref(),
        attributes.as_ref(),
        default_role,
    ));
    let email = if caller.can_see_email(username, &teams) {
        conn.get_user_email(&user)
            .await
            .map_err(read("get_user_email"))?
    } else {
        None
    };
    Ok(Some(json!({
        "username": username,
        "user_id": user_id_json(Some(user_id)),
        "user_type": user_type,
        "role": role.name(),
        "permissions": permissions,
        "explicit": rules::explicit_scopes(attributes.as_ref()).is_some(),
        "teams": super::teams::user_teams_json(&teams),
        "primary_team": rules::primary_team(attributes.as_ref(), &teams),
        "email": email,
    })))
}

/// `GET /users/permissions`: every user's row ([`user_row`]), by username.
async fn list_permissions(ctx: Ctx) -> ApiResult {
    let caller = crate::access::require(&ctx.state, &ctx.headers, &["users:read"]).await?;
    let conn = ctx.state.instance();
    let mut usernames = conn
        .get_users()
        .await
        .map_err(|e| ApiError::exception("get_users", e))?;
    usernames.sort();
    let teams = conn
        .get_teams()
        .await
        .map_err(|e| ApiError::exception("get_teams", e))?;
    let by_user = super::teams::teams_by_user(&teams);
    let mut rows = Vec::with_capacity(usernames.len());
    for username in &usernames {
        let teams = by_user.get(username).cloned().unwrap_or_default();
        if let Some(row) = user_row(&ctx.state, &caller, username, Some(teams)).await? {
            rows.push(row);
        }
    }
    let n = rows.len();
    Ok(crate::envelope::ok_with(
        format!("Found {n} user{}.", if n == 1 { "" } else { "s" }),
        "users",
        Json::Array(rows),
    ))
}

/// `GET /users/{username}/permissions`: one user's row ([`user_row`]).
async fn get_permissions(ctx: Ctx, Path(username): Path<String>) -> ApiResult {
    let caller = crate::access::require(&ctx.state, &ctx.headers, &["users:read"]).await?;
    let row = user_row(&ctx.state, &caller, &username, None)
        .await?
        .ok_or_else(|| ApiError::new(404, format!("User '{username}' does not exist.")))?;
    Ok(crate::envelope::ok_with(
        format!("The permissions of '{username}'."),
        "user",
        row,
    ))
}

/// `PATCH /users/{username}/permissions`: `role` (`viewer`, `editor`, `operator`) or explicit
/// `permissions` (administrators), `primary_team` (administrators, or the user to one of their
/// teams), or the whole `attributes` (administrators; a user's own, whose rights stay).
async fn patch_permissions(ctx: Ctx, Path(username): Path<String>, body: Body) -> ApiResult {
    use meerschaum_core::access as rules;
    let caller = crate::access::require(&ctx.state, &ctx.headers, &["users:write"]).await?;
    let body = super::pipes::parse_json_body(&body_bytes(body).await?)?
        .unwrap_or(Json::Object(Default::default()));
    let Some(request) = body.as_object() else {
        return Err(ApiError::new(
            400,
            "The request body must be a JSON object.",
        ));
    };
    let admin = caller.no_auth || caller.is_admin();
    let own = caller.username() == Some(username.as_str());
    if !admin && !own {
        return Err(ApiError::new(
            403,
            "Only administrators may change another user's permissions.",
        ));
    }
    let conn = ctx.state.instance();
    let mut user = User::new(&username, None);
    user.instance_keys = Some(conn.connector_keys());
    if conn
        .get_user_id(&user)
        .await
        .map_err(|e| ApiError::exception("get_user_id", e))?
        .is_none()
    {
        return Err(ApiError::new(
            404,
            format!("User '{username}' does not exist."),
        ));
    }
    let stored = conn
        .get_user_attributes(&user)
        .await
        .map_err(|e| ApiError::exception("get_user_attributes", e))?
        .unwrap_or_default();
    let mut attributes = match request.get("attributes") {
        None | Some(Json::Null) => stored.clone(),
        Some(Json::Object(o)) => {
            let given = crate::json::to_core(&Json::Object(o.clone())).to_map();
            if admin {
                given
            } else {
                keep_reserved(&stored, given).map_err(|message| ApiError::new(403, message))?
            }
        }
        Some(other) => {
            return Err(ApiError::new(
                400,
                format!("`attributes` must be a dictionary, got {other}."),
            ));
        }
    };
    let rights = request.contains_key("role") || request.contains_key("permissions");
    if rights && !admin {
        return Err(ApiError::new(
            403,
            "Only administrators may change a user's role or permissions.",
        ));
    }
    if let Some(role) = request.get("role").filter(|v| !v.is_null()) {
        let Some(role) = role
            .as_str()
            .and_then(rules::Role::parse)
            .filter(|r| rules::Role::DEFAULTABLE.contains(r))
        else {
            return Err(ApiError::new(
                400,
                format!(
                    "Invalid role {role}: use 'viewer', 'editor' or 'operator' (administrators are the user type 'admin')."
                ),
            ));
        };
        attributes = rules::attributes_with_role(&attributes, role);
    }
    if let Some(permissions) = request.get("permissions").filter(|v| !v.is_null()) {
        let Some(items) = permissions.as_array() else {
            return Err(ApiError::new(
                400,
                "`permissions` must be a list of permission names.",
            ));
        };
        let names: Vec<String> = items
            .iter()
            .filter_map(Json::as_str)
            .map(str::to_string)
            .collect();
        attributes = rules::attributes_with_permissions(&attributes, &names);
    }
    if let Some(primary) = request.get("primary_team") {
        match primary {
            Json::Null => {
                attributes.shift_remove(rules::PRIMARY_TEAM_ATTRIBUTE);
            }
            Json::String(team) => {
                let teams = crate::access::user_teams(&conn, &username)
                    .await
                    .map_err(|e| ApiError::exception("get_user_teams", e))?;
                let Some((name, _)) = teams
                    .iter()
                    .find(|(t, _)| rules::same_team(Some(t), Some(team)))
                else {
                    return Err(ApiError::new(
                        400,
                        format!("'{username}' is not a member of the team '{team}'."),
                    ));
                };
                attributes.insert(
                    rules::PRIMARY_TEAM_ATTRIBUTE.into(),
                    Value::Str(name.clone()),
                );
            }
            other => {
                return Err(ApiError::new(
                    400,
                    format!("`primary_team` must be a team name or null, got {other}."),
                ));
            }
        }
    }
    let res = conn.set_user_attributes(&user, &attributes).await;
    ctx.state.access_cache().forget_identities();
    if !res.success {
        return Err(ApiError::new(500, res.message));
    }
    let row = user_row(&ctx.state, &caller, &username, None)
        .await?
        .ok_or_else(|| ApiError::new(404, format!("User '{username}' does not exist.")))?;
    Ok(crate::envelope::ok_with(
        format!("Updated the permissions of '{username}'."),
        "user",
        row,
    ))
}

/// Whether `username` is the instance's only administrator.
async fn last_admin(
    conn: &std::sync::Arc<dyn meerschaum_connector::InstanceConnector>,
    username: &str,
) -> Result<bool, ApiError> {
    let users = conn
        .get_users()
        .await
        .map_err(crate::auth::scopes_unavailable)?;
    let mut admins = 0;
    let mut is_admin = false;
    for name in users {
        let typ = conn
            .get_user_type(&User::new(&name, None))
            .await
            .map_err(crate::auth::scopes_unavailable)?;
        if meerschaum_core::access::is_admin_type(typ.as_deref()) {
            admins += 1;
            is_admin |= name == username;
        }
    }
    Ok(is_admin && admins == 1)
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

/// Drop the attributes that hold access rights (`scopes`, `role`, ...) from `attributes`:
/// only administrators set them through the API.
fn without_reserved(attributes: Option<Map>) -> Option<Map> {
    attributes.map(|mut a| {
        for key in crate::access::RESERVED_USER_ATTRIBUTES {
            a.shift_remove(*key);
        }
        a
    })
}

/// The refusal of a user who is not an administrator changing the attributes that hold rights.
const RIGHTS_REFUSAL: &str = "Only administrators may change a user's role or permissions.";

/// The attributes a user who is not an administrator stores for their own account: `given`, with
/// the attributes that hold rights (`scopes`, `role`, ...) as `stored` has them. Leaving one out
/// keeps it; giving another value is refused ([`RIGHTS_REFUSAL`]), never silently dropped.
fn keep_reserved(stored: &Map, mut given: Map) -> Result<Map, &'static str> {
    for key in crate::access::RESERVED_USER_ATTRIBUTES {
        match (given.get(*key), stored.get(*key)) {
            (Some(new), old) if Some(new) != old => return Err(RIGHTS_REFUSAL),
            (None, Some(old)) => {
                given.insert(key.to_string(), old.clone());
            }
            _ => {}
        }
    }
    Ok(given)
}

/// `POST /users/register`.
///
/// - **First run** (rc15): while the user store has no users, anyone may register (also under
///   `--private`, whatever `api:permissions:registration:users` says) and the first account is
///   an administrator. Registrations are serialized so two visitors cannot both be first.
/// - **Administrators** register users whatever the registration setting, and may set their
///   type and permissions.
/// - **Self-registration** needs `api:permissions:registration:users: true` (off by default): the
///   new user gets the default role and is in Everyone; it cannot set its own type or rights.
async fn register_user(ctx: Ctx, body: Body) -> ApiResult {
    let state = &ctx.state;
    let form = read_form(&ctx.headers, body_bytes(body).await?).await;
    let conn = state.instance();
    let _first_user = state.first_user.lock().await;
    let first_run = conn
        .get_users()
        .await
        .map_err(|e| {
            crate::error::ApiError::unavailable(
                "Could not read the users from the instance; try again.",
            )
            .with_internal(e.to_string())
        })?
        .is_empty()
        && !state.config.public;
    let caller = if first_run {
        None
    } else {
        if state.config.private {
            crate::access::require(state, &ctx.headers, &["users:register", "users:write"]).await?;
        }
        crate::access::resolve_optional(state, &ctx.headers)
            .await
            .unwrap_or(None)
    };
    let admin_caller = caller
        .as_ref()
        .is_some_and(|a| a.is_admin() && a.has("users:register"));
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
    if !first_run && !admin_caller && !state.config.registration_users {
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
    if crate::access::rules::is_admin_type(user_type.as_deref()) && !first_run && !admin_caller {
        return Ok(tuple(
            false,
            "New users cannot be of type 'admin' when using the API connector. \
             Register a normal user first, then edit the user from an authorized account, \
             or use a SQL connector instead.",
        ));
    }
    let mut user = User::new(username, Some(password));
    user.user_type = if first_run {
        Some("admin".into())
    } else {
        user_type
    };
    user.email = form_get(&form, "email").map(String::from);
    user.attributes = if admin_caller || first_run {
        attributes
    } else {
        without_reserved(attributes)
    };
    user.instance_keys = Some(conn.connector_keys());
    let res = conn.register_user(&mut user).await;
    if res.success {
        state.access_cache().forget_identities();
    }
    Ok(tuple_response(&res))
}

async fn edit_user(ctx: Ctx, body: Body) -> ApiResult {
    let state = &ctx.state;
    let form = read_form(&ctx.headers, body_bytes(body).await?).await;
    let principal = scoped_auth(state, &ctx.headers, &["users:write"]).await?;
    let _user_change = state.first_user.lock().await;
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
        Some(u) => conn
            .get_user_type(u)
            .await
            .map_err(crate::auth::scopes_unavailable)?,
        None => Some("admin".into()),
    };
    let is_admin = crate::access::rules::is_admin_type(user_type.as_deref());
    if is_admin && let Some(t) = form_get(&form, "type") {
        // The instance keeps an administrator (rc15).
        if !meerschaum_core::access::is_admin_type(Some(t))
            && last_admin(&conn, &user.username).await?
        {
            return Ok(tuple(
                false,
                format!(
                    "'{}' is the only administrator and stays one.",
                    user.username
                ),
            ));
        }
        user.user_type = Some(t.to_string());
    }
    if !is_admin && curr_user.is_some_and(|u| u.username == user.username) {
        // A user edits its own account, never its own rights: the stored `scopes`, `role`, ...
        // stay (rc15).
        if user.attributes.is_some() {
            let stored = conn
                .get_user_attributes(&user)
                .await
                .map_err(|e| ApiError::exception("get_user_attributes", e))?
                .unwrap_or_default();
            match keep_reserved(&stored, user.attributes.take().unwrap_or_default()) {
                Ok(attributes) => user.attributes = Some(attributes),
                Err(message) => return Ok(tuple(false, message)),
            }
        }
    }
    if is_admin || curr_user.is_some_and(|u| u.username == user.username) {
        let res = conn.edit_user(&user).await;
        if res.success {
            state.access_cache().forget_identities();
        }
        return Ok(tuple_response(&res));
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
    let _user_change = state.first_user.lock().await;
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
    if crate::access::rules::is_admin_type(user_type.as_deref())
        || curr_user.is_some_and(|u| u.username == username)
    {
        // The instance keeps an administrator (rc15).
        if last_admin(&conn, &username).await? {
            return Ok(tuple(
                false,
                format!("Cannot delete '{username}': it is the only administrator."),
            ));
        }
        let res = conn.delete_user(&user).await;
        if res.success {
            state.access_cache().forget_identities();
            state.access_cache().forget_pipes();
        }
        return Ok(tuple_response(&res));
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
