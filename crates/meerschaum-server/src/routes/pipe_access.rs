//! A pipe's access and locks over HTTP (rc15 access model, AC4, AC5, AC12), in the `pipes` route
//! group and the v5 shape (`{"success", "message", ...}`, [`crate::envelope`]):
//!
//! | Route | Scopes | What it does |
//! |---|---|---|
//! | `GET /pipes/{connector}/{metric}/{location}/access` | `pipes:read` | `access` (`owner`, `team`, `visibility`, `edit`), `locks`, and what the caller `may` do |
//! | `PATCH /pipes/{connector}/{metric}/{location}/access` | `pipes:write` | Change `owner`, `team`, `visibility` and `edit` (the keys given): the owner, the team's managers and administrators |
//! | `POST /pipes/{connector}/{metric}/{location}/lock` | `pipes:write` | Add `locks` (default `all`): the owner, the team's managers and administrators |
//! | `POST /pipes/{connector}/{metric}/{location}/unlock` | `pipes:write` | Remove `locks` (default `all`) |
//!
//! The same routes exist for saved Pipelines (`/pipelines/{name}/access`, `/lock`, `/unlock`,
//! [`super::pipelines`]). A pipe the caller cannot see is `404`; a change it may not make `403`;
//! an access change under a `parameters` lock `409` (locks can always be changed by whoever may
//! lock).

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::routing::{get, post};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Map, Pipe, Value};
use serde_json::{Map as JsonMap, Value as Json, json};

use super::{Ctx, body_bytes};
use crate::access::{self, Access, Locks, PipeAccess, rules};
use crate::envelope;
use crate::error::{ApiError, ApiResult};
use crate::json::{from_core, to_core};
use crate::state::AppState;

type Keys3 = Path<(String, String, String)>;

/// Register the routes (part of the `pipes` group).
pub fn routes(app: Router<AppState>) -> Router<AppState> {
    let p = "/pipes/{connector_keys}/{metric_key}/{location_key}";
    app.route(&format!("{p}/access"), get(get_access).patch(patch_access))
        .route(&format!("{p}/lock"), post(lock))
        .route(&format!("{p}/unlock"), post(unlock))
}

/// The JSON of a pipe's access and locks, with what `caller` may do.
pub fn access_fields(caller: &Access, parameters: &Map) -> JsonMap<String, Json> {
    let pipe_access = PipeAccess::from_parameters(parameters);
    let locks = Locks::from_parameters(parameters);
    let mut fields = JsonMap::new();
    fields.insert("access".into(), from_core(&pipe_access.to_value()));
    fields.insert("locks".into(), json!(locks.names()));
    fields.insert(
        "may".into(),
        json!({
            "edit": caller.can_edit(parameters),
            "manage_access": caller.can_manage_access(parameters),
            "lock": caller.can_lock(parameters),
        }),
    );
    fields
}

/// The pipe `ck|mk|lk` of the request's instance, registered and visible to `caller`.
async fn registered_visible(
    ctx: &Ctx,
    caller: &Access,
    ck: &str,
    mk: &str,
    lk: &str,
) -> Result<(Arc<dyn InstanceConnector>, Pipe), ApiError> {
    let ik = ctx.instance_keys();
    let h = ctx
        .state
        .get_pipe(ck, mk, Some(lk), ik.as_deref(), true)
        .await?;
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    let registered = conn
        .get_pipe_id(&h.pipe)
        .await
        .map_err(|e| ApiError::exception("get_pipe_id", e))?
        .is_some();
    if !registered || !caller.can_see_pipe(&h.pipe) {
        return Err(ApiError::new(404, format!("{} is not registered.", h.pipe)));
    }
    Ok((conn, h.pipe.clone()))
}

/// The registered parameters of `pipe` on `conn` (after a change).
pub async fn registered(conn: &Arc<dyn InstanceConnector>, pipe: &Pipe) -> Result<Map, ApiError> {
    let attrs = conn
        .get_pipe_attributes(pipe)
        .await
        .map_err(|e| ApiError::exception("get_pipe_attributes", e))?;
    Ok(crate::state::parameters_of(&attrs).unwrap_or_default())
}

/// A change's result as a response error: `409` for a lock refusal, else `500`.
pub fn change_failed(res: &meerschaum_core::SuccessTuple) -> ApiError {
    if rules::is_lock_refusal(&res.message) {
        ApiError::new(409, res.message.clone())
    } else {
        ApiError::new(500, res.message.clone())
    }
}

/// After a change of access or locks through this server: its caches read them again.
pub async fn forget(state: &AppState, conn: &Arc<dyn InstanceConnector>) {
    state.access_cache().forget_pipes();
    let _ = state.pipes(Some(&conn.connector_keys()), true).await;
}

/// The access `caller` asks for `pipe` (stored `parameters`) with the keys of `body` (any of
/// `owner`, `team`, `visibility`, `edit`), checked: the new access.
pub fn changed_access(
    caller: &Access,
    parameters: &Map,
    body: &Json,
) -> Result<PipeAccess, ApiError> {
    let Some(request) = body.as_object() else {
        return Err(ApiError::new(
            400,
            "The body must be a JSON object with any of `owner`, `team`, `visibility` and `edit`.",
        ));
    };
    let old = PipeAccess::from_parameters(parameters);
    let mut merged = match old.to_value() {
        Value::Map(m) => m,
        _ => Map::new(),
    };
    for (key, value) in request {
        merged.insert(key.clone(), to_core(value));
    }
    let new = PipeAccess::parse(&Value::Map(merged)).map_err(|e| ApiError::new(400, e))?;
    if !caller.no_auth {
        rules::check_access_change(&caller.principal, &old, &new)
            .map_err(|e| ApiError::new(403, e))?;
    }
    Ok(new)
}

/// The locks named by a lock or unlock body (`{"locks": [...]}` or `"sync,drop"`; none: `all`).
pub fn requested_locks(body: &Json) -> Result<Locks, ApiError> {
    let text = match body.get("locks") {
        None | Some(Json::Null) => String::new(),
        Some(Json::String(s)) => s.clone(),
        Some(Json::Array(items)) => items
            .iter()
            .map(|i| {
                i.as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| i.to_string())
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(other) => {
            return Err(ApiError::new(
                400,
                format!("`locks` must be a list of lock names, got {other}."),
            ));
        }
    };
    Locks::parse_list(&text).map_err(|e| ApiError::new(400, e))
}

/// Refuse a lock change of `pipe` (stored `parameters`) by `caller` who may not lock it.
pub fn check_may_lock(
    caller: &Access,
    pipe: &Pipe,
    parameters: &Map,
    lock: bool,
) -> Result<(), ApiError> {
    if caller.no_auth || caller.can_lock(parameters) {
        return Ok(());
    }
    Err(ApiError::new(
        403,
        format!(
            "Only the owner of {pipe}, its team's managers and administrators may {} it.",
            if lock { "lock" } else { "unlock" }
        ),
    ))
}

async fn json_body(body: Body) -> Result<Json, ApiError> {
    Ok(super::pipes::parse_json_body(&body_bytes(body).await?)?
        .unwrap_or(Json::Object(JsonMap::new())))
}

async fn get_access(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let (_, pipe) = registered_visible(&ctx, &caller, &ck, &mk, &lk).await?;
    let fields = access_fields(&caller, pipe.raw_parameters());
    Ok(envelope::ok(format!("The access of {pipe}."), fields))
}

async fn patch_access(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let body = json_body(body).await?;
    let (conn, pipe) = registered_visible(&ctx, &caller, &ck, &mk, &lk).await?;
    let new = changed_access(&caller, pipe.raw_parameters(), &body)?;
    // A connector reserved for some teams keeps its pipes with them.
    let old_team = PipeAccess::from_parameters(pipe.raw_parameters()).team;
    if !rules::same_team(old_team.as_deref(), new.team.as_deref()) {
        access::guard::check_pipe_connector(
            &ctx.state,
            &caller,
            pipe.connector_keys(),
            new.team.as_deref(),
        )?;
    }
    let res = conn.set_pipe_access(&pipe, &new).await;
    forget(&ctx.state, &conn).await;
    if !res.success {
        return Err(change_failed(&res));
    }
    let parameters = registered(&conn, &pipe).await?;
    Ok(envelope::ok(
        res.message,
        access_fields(&caller, &parameters),
    ))
}

async fn lock_or_unlock(
    ctx: Ctx,
    keys: (String, String, String),
    body: Body,
    lock: bool,
) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let body = json_body(body).await?;
    let requested = requested_locks(&body)?;
    let (conn, pipe) = registered_visible(&ctx, &caller, &keys.0, &keys.1, &keys.2).await?;
    check_may_lock(&caller, &pipe, pipe.raw_parameters(), lock)?;
    let res = if lock {
        conn.lock_pipe(&pipe, &requested).await
    } else {
        conn.unlock_pipe(&pipe, &requested).await
    };
    forget(&ctx.state, &conn).await;
    if !res.success {
        return Err(change_failed(&res));
    }
    let parameters = registered(&conn, &pipe).await?;
    Ok(envelope::ok(
        res.message,
        access_fields(&caller, &parameters),
    ))
}

async fn lock(ctx: Ctx, Path(keys): Keys3, body: Body) -> ApiResult {
    lock_or_unlock(ctx, keys, body, true).await
}

async fn unlock(ctx: Ctx, Path(keys): Keys3, body: Body) -> ApiResult {
    lock_or_unlock(ctx, keys, body, false).await
}
