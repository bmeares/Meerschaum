//! `/pipes/*` and `/metadata` (`api/routes/_pipes.py`).

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post};
use chrono::TimeDelta;
use meerschaum_connector::replace::WindowBound;
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector, PipesKeys, PipesKeysQuery};
use meerschaum_core::pipe::chunk::ChunkIntervalArg;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::serialize::GeometryFormat;
use serde_json::{Value as Json, json};

use super::{Ctx, body_bytes};
use crate::access::{self, Access, rules::LockOp};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{from_core, ok_json, python_json, raw_json, to_core, tuple, tuple_response};
use crate::pipe_methods::{self, SyncKwargs, SyncPayload};
use crate::serialize::{DataJsonOptions, DateUnit, bound_json, data_to_json, to_csv};
use crate::state::{AppState, PipeHandle, normalize_location, pipe_attributes};

pub(crate) mod mutation;

const PIPES_READ: &[&str] = &["pipes:read"];
const PIPES_WRITE: &[&str] = &["pipes:write"];
const PIPES_DELETE: &[&str] = &["pipes:delete"];
const PIPES_DROP: &[&str] = &["pipes:drop"];

type Keys3 = Path<(String, String, String)>;

/// Register the `pipes` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    let p = "/pipes/{connector_keys}/{metric_key}/{location_key}";
    let app = app
        .route("/pipes", get(get_pipes))
        .route("/pipes/keys", get(fetch_pipes_keys))
        .route("/pipes/{connector_keys}", get(get_pipes_by_connector))
        .route(
            "/pipes/{connector_keys}/{metric_key}",
            get(get_pipes_by_connector_and_metric),
        )
        .route(p, get(get_pipe_by_keys))
        .route(&format!("{p}/register"), post(register_pipe))
        .route(&format!("{p}/edit"), patch(edit_pipe))
        .route(&format!("{p}/delete"), delete(delete_pipe))
        .route(&format!("{p}/sync_time"), get(get_sync_time))
        .route(&format!("{p}/data"), post(sync_pipe).get(get_pipe_data))
        .route(&format!("{p}/docs"), get(get_pipe_docs))
        .route(&format!("{p}/chunk_bounds"), get(get_pipe_chunk_bounds))
        .route(&format!("{p}/drop"), delete(drop_pipe))
        .route(&format!("{p}/clear"), delete(clear_pipe))
        .route(&format!("{p}/cache"), delete(delete_pipe_cache))
        .route(&format!("{p}/csv"), get(get_pipe_csv))
        .route(&format!("{p}/id"), get(get_pipe_id))
        .route(&format!("{p}/attributes"), get(get_pipe_attributes))
        .route(&format!("{p}/exists"), get(get_pipe_exists))
        .route(&format!("{p}/rowcount"), get(get_pipe_rowcount))
        .route(&format!("{p}/columns/types"), get(get_pipe_columns_types))
        .route(
            &format!("{p}/columns/indices"),
            get(get_pipe_columns_indices),
        )
        .route(&format!("{p}/indices/names"), get(get_pipe_index_names))
        .route(&format!("{p}/size"), get(get_pipe_size))
        .route(&format!("{p}/compress"), post(compress_pipe))
        .route(&format!("{p}/decompress"), post(decompress_pipe))
        .route(&format!("{p}/vacuum"), post(vacuum_pipe))
        .route(&format!("{p}/analyze"), post(analyze_pipe))
        .route(&format!("{p}/partition"), post(partition_pipe))
        .route(
            meerschaum_connector_api::routes::METADATA,
            post(create_metadata),
        );
    // A pipe's access and locks (rc15, the v5 shape).
    super::pipe_access::routes(app)
}

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

/// Parse a JSON request body (`fastapi.Body`): `Ok(None)` when empty, a 422 `json_invalid`
/// error when malformed.
pub fn parse_json_body(body: &[u8]) -> Result<Option<Json>, ApiError> {
    if body.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(body).map(Some).map_err(|e| {
        ApiError::validation(vec![ApiError::field_error(
            "json_invalid",
            &["body"],
            "JSON decode error",
            json!({}),
            Some(json!({"error": e.to_string()})),
        )])
    })
}

/// Validate a body parameter `Optional[Dict[str, Any]] = None` (after authentication, like
/// FastAPI's field validation).
fn optional_dict_body(body: Option<Json>) -> Result<Option<Map>, ApiError> {
    match body {
        None | Some(Json::Null) => Ok(None),
        Some(Json::Object(o)) => Ok(Some(to_core(&Json::Object(o)).to_map())),
        Some(other) => Err(ApiError::validation(vec![ApiError::field_error(
            "dict_type",
            &["body"],
            "Input should be a valid dictionary",
            other,
            None,
        )])),
    }
}

/// `json.loads(params)` for a `params` query string: `'null'` and absent mean `{}`, invalid or
/// non-dict JSON is a 409.
fn parse_params(params: Option<&str>) -> Result<Map, ApiError> {
    let err = || ApiError::new(409, "Params must be a valid JSON-encoded dictionary.");
    match params {
        None | Some("null") => Ok(Map::new()),
        Some(p) => match Value::from_json_str(p) {
            Ok(Value::Map(m)) => Ok(m),
            _ => Err(err()),
        },
    }
}

fn parse_select_columns(raw: Option<&str>) -> Result<Vec<String>, ApiError> {
    let err = || ApiError::new(409, "Selected columns must be a JSON-encoded list.");
    match raw {
        None | Some("null") => Ok(Vec::new()),
        Some(s) => match Value::from_json_str(s) {
            Ok(Value::List(items)) => Ok(items.iter().map(Value::py_str).collect()),
            _ => Err(err()),
        },
    }
}

/// PARITY: `omit_columns` is only checked for `None`, so any valid JSON passes
/// (`api/routes/_pipes.py:539`); dict keys and strings are iterated like Python does.
fn parse_omit_columns(raw: Option<&str>) -> Result<Vec<String>, ApiError> {
    let err = || ApiError::new(409, "Omitted columns must be a JSON-encoded list.");
    match raw {
        None | Some("null") => Ok(Vec::new()),
        Some(s) => match Value::from_json_str(s) {
            Ok(Value::Null) | Err(_) => Err(err()),
            Ok(Value::List(items)) => Ok(items.iter().map(Value::py_str).collect()),
            Ok(Value::Map(m)) => Ok(m.keys().cloned().collect()),
            Ok(Value::Str(s)) => Ok(s.chars().map(String::from).collect()),
            Ok(_) => Err(ApiError::internal()),
        },
    }
}

fn nonempty(v: Vec<String>) -> Option<Vec<String>> {
    (!v.is_empty()).then_some(v)
}

async fn handle(
    ctx: &Ctx,
    ck: &str,
    mk: &str,
    lk: &str,
    refresh: bool,
) -> Result<(Arc<dyn InstanceConnector>, Arc<PipeHandle>), ApiError> {
    let ik = ctx.instance_keys();
    let h = ctx
        .state
        .get_pipe(ck, mk, Some(lk), ik.as_deref(), refresh)
        .await?;
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    Ok((conn, h))
}

/// Authorize `scopes` (the caller's effective permissions, rc15) and return its access.
async fn authorize(ctx: &Ctx, scopes: &[&str]) -> Result<Access, ApiError> {
    access::require(&ctx.state, &ctx.headers, scopes).await
}

async fn authorize_metadata(ctx: &Ctx, ck: &str, mk: &str, lk: &str) -> Result<Access, ApiError> {
    let (caller, operation) = crate::executor::pipe_operation::authorize_read(ctx).await?;
    let instance = ctx
        .instance_keys()
        .unwrap_or_else(|| ctx.state.config.instance_keys.clone());
    if operation.is_some_and(|operation| {
        !operation.permits(&instance, ck, mk, normalize_location(Some(lk)).as_deref())
    }) {
        return Err(ApiError::new(
            403,
            "This metadata is outside the accepted pipe operation.",
        ));
    }
    Ok(caller)
}

async fn authorize_catalog(
    ctx: &Ctx,
) -> Result<(Access, Option<crate::executor::pipe_operation::ReadAccess>), ApiError> {
    let result = crate::executor::pipe_operation::authorize_read(ctx).await?;
    let instance = ctx
        .instance_keys()
        .unwrap_or_else(|| ctx.state.config.instance_keys.clone());
    if result
        .1
        .as_ref()
        .is_some_and(|operation| !operation.permits_instance(&instance))
    {
        return Err(ApiError::new(
            403,
            "This catalog is outside the accepted pipe operation.",
        ));
    }
    Ok(result)
}

/// Whether `access` may see the registered pipe `ck|mk|lk` of the request's instance (an
/// unregistered pipe has the default access): decided on the instance's pipes as of at most
/// [`access::INDEX_TTL`] ago.
async fn sees(
    ctx: &Ctx,
    access: &Access,
    ck: &str,
    mk: &str,
    lk: Option<&str>,
) -> Result<bool, ApiError> {
    if access.is_admin() {
        return Ok(true);
    }
    let index = access::pipe_index(&ctx.state, ctx.instance_keys().as_deref()).await?;
    Ok(index
        .get(ck, mk, lk)
        .is_none_or(|p| access.can_see_indexed(p)))
}

/// [`handle`] for a pipe `access` can see (else the `404` of a pipe that does not exist).
async fn visible(
    ctx: &Ctx,
    access: &Access,
    ck: &str,
    mk: &str,
    lk: &str,
    refresh: bool,
) -> Result<(Arc<dyn InstanceConnector>, Arc<PipeHandle>), ApiError> {
    let location = normalize_location(Some(lk));
    let visible = sees(ctx, access, ck, mk, location.as_deref()).await?;
    let (conn, h) = handle(ctx, ck, mk, lk, refresh).await?;
    if !visible || !access.can_see_pipe(&h.pipe) {
        return Err(access::hidden_pipe(&h.pipe));
    }
    if !access.is_admin() {
        let index = access::pipe_index(&ctx.state, ctx.instance_keys().as_deref()).await?;
        if access::guard::check_pipe_target(conn.as_ref(), &h.pipe, &index.hidden_targets(access))
            .is_err()
        {
            return Err(access::hidden_pipe(&h.pipe));
        }
    }
    Ok((conn, h))
}

/// [`visible`] for a pipe `access` may also change (else `403`).
async fn editable(
    ctx: &Ctx,
    access: &Access,
    ck: &str,
    mk: &str,
    lk: &str,
    refresh: bool,
) -> Result<(Arc<dyn InstanceConnector>, Arc<PipeHandle>), ApiError> {
    let (conn, h) = visible(ctx, access, ck, mk, lk, refresh).await?;
    if !access.can_edit(h.pipe.raw_parameters()) {
        return Err(access::not_editable(&h.pipe));
    }
    Ok((conn, h))
}

/// After a change of a pipe's parameters through this server: the pipes' access is read again.
fn forget_access(ctx: &Ctx) {
    ctx.state.access_cache().forget_pipes();
}

fn target(pipe: &Pipe) -> String {
    pipe.target(None).unwrap_or_else(|_| pipe.target_legacy())
}

fn refuse_protected(pipe: &Pipe, verb: &str) -> Result<(), ApiError> {
    let t = target(pipe);
    if meerschaum_connector_api::routes::PROTECTED_TARGETS.contains(&t.as_str()) {
        return Err(ApiError::new(
            409,
            format!("Cannot {verb} protected table '{t}'."),
        ));
    }
    Ok(())
}

async fn attributes_tree<'a, I>(
    conn: &Arc<dyn InstanceConnector>,
    locations: I,
) -> Result<serde_json::Map<String, Json>, ApiError>
where
    I: IntoIterator<Item = (&'a Option<String>, &'a Arc<PipeHandle>)>,
{
    let mut out = serde_json::Map::new();
    for (lk, h) in locations {
        let attrs = pipe_attributes(conn, &h.pipe).await?;
        // `locations['None'] = locations.pop(None)`.
        out.insert(
            lk.clone().unwrap_or_else(|| "None".into()),
            from_core(&Value::Map(attrs)),
        );
    }
    Ok(out)
}

// ------------------------------------------------------------------------------------------------
// Registration and attributes
// ------------------------------------------------------------------------------------------------

/// `POST .../register`. Registering needs `pipes:register` (rc15; `pipes:write` still works for
/// credentials made before it). The new pipe's owner is the caller (an administrator may name
/// another) and its team defaults to the caller's primary team; its SQL definition passes the
/// SQL guard ([`crate::sql_guard`]); a connector reserved for other teams is refused.
async fn register_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let ik = ctx.instance_keys();
    let body = parse_json_body(&body_bytes(body, ctx.state.config.limits.body).await?)?;
    // Genuine legacy credentials are expanded during access resolution. Rechecking a raw
    // `pipes:write` alias here would restore registration that a modern role removed.
    let access = access::require_fresh(&ctx.state, &ctx.headers, &["pipes:register"]).await?;
    let parameters = optional_dict_body(body)?;
    if !ctx.state.config.registration_pipes {
        return Ok(tuple(
            false,
            "The administrator for this server has not allowed pipe registration.\n\n\
             Please contact the system administrator, or if you are running this server, \
             open the configuration file with `edit config api` and search for 'permissions'. \
             Under the keys `api:permissions:registration`, \
             you can toggle various registration types.",
        ));
    }
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    if ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(
            409,
            format!("{} already registered.", h.pipe),
        ));
    }
    access::guard::check_source_connector(&ctx.state, &access, &ck)?;
    let mut pipe = h.pipe.clone();
    let requested = parameters
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| pipe.raw_parameters().clone());
    let stamped = access::guard::stamp_new_pipe(&access, &requested)?;
    access::guard::check_parameters(&ctx.state, &access, &conn, &pipe, None, &stamped).await?;
    if !stamped.is_empty() {
        pipe.set_parameters(stamped);
    }
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let fresh = access::require_fresh(&ctx.state, &ctx.headers, &["pipes:register"]).await?;
    if fresh.principal != access.principal || fresh.primary_team != access.primary_team {
        return Err(ApiError::new(
            409,
            "Your pipe registration permissions changed during preparation; retry the request.",
        ));
    }
    access::guard::check_source_connector(&ctx.state, &fresh, &ck)?;
    let index = access::index_from_dict(&conn.connector_keys(), &dict, conn.as_ref());
    access::guard::check_pipe_target(conn.as_ref(), &pipe, &index.hidden_targets(&fresh))?;
    let res = mutation::run(
        &ctx,
        &conn,
        &pipe,
        &fresh,
        mutation::Operation::Register,
        conn.register_pipe(&pipe),
    )
    .await?;
    ctx.state.pipes(ik.as_deref(), true).await?;
    forget_access(&ctx);
    Ok(tuple_response(&res))
}

/// `PATCH .../edit`: a pipe the caller may change. `access` and `locks` the body leaves out keep
/// their stored values; changing them needs the rights to (the owner, the team's managers,
/// administrators); a SQL definition passes the SQL guard.
async fn edit_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let ik = ctx.instance_keys();
    let body = parse_json_body(&body_bytes(body, ctx.state.config.limits.body).await?)?;
    let access = authorize(&ctx, PIPES_WRITE).await?;
    let parameters = match body {
        None => return Err(ApiError::missing(&["body"])),
        Some(Json::Object(o)) => to_core(&Json::Object(o)).to_map(),
        Some(other) => {
            return Err(ApiError::validation(vec![ApiError::field_error(
                "dict_type",
                &["body"],
                "Input should be a valid dictionary",
                other,
                None,
            )]));
        }
    };
    let patch = ctx.query.bool("patch", false)?;
    if !ctx.state.config.actions_non_admin {
        return Ok(tuple(
            false,
            "The administrator for this server has not allowed actions.\n\n\
             Please contact the system administrator, or if you are running this server, \
             open the configuration file with `edit config api` and search for 'permissions'. \
             Under the keys `api:permissions:actions`, \
             you can toggle non-admin actions.",
        ));
    }
    let (conn, h) = editable(&ctx, &access, &ck, &mk, &lk, true).await?;
    if !ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(409, format!("{} is not registered.", h.pipe)));
    }
    let old = h.pipe.raw_parameters().clone();
    let requested = if patch {
        meerschaum_core::value::apply_patch_maps(&old, &parameters)
    } else {
        parameters
    };
    let new = access::guard::merge_edit(&access, &old, requested)?;
    access::guard::check_parameters(&ctx.state, &access, &conn, &h.pipe, Some(&old), &new).await?;
    if let Some(e) = access::guard::parameters_lock_conflict(&h.pipe, &old, &new) {
        return Ok(tuple(false, e.detail_text()));
    }
    let mut pipe = h.pipe.clone();
    pipe.set_parameters(new);
    let res = mutation::run(
        &ctx,
        &conn,
        &pipe,
        &access,
        mutation::Operation::Edit(old),
        conn.edit_pipe(&pipe, false),
    )
    .await?;
    ctx.state.pipes(ik.as_deref(), true).await?;
    forget_access(&ctx);
    Ok(tuple_response(&res))
}

async fn delete_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let ik = ctx.instance_keys();
    let access = authorize(&ctx, PIPES_DELETE).await?;
    let (conn, h) = editable(&ctx, &access, &ck, &mk, &lk, true).await?;
    if !ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(409, format!("{} is not registered.", h.pipe)));
    }
    if let Some(e) = access::guard::lock_conflict(&h.pipe, LockOp::Delete) {
        return Ok(tuple(false, e.detail_text()));
    }
    let res = mutation::run(
        &ctx,
        &conn,
        &h.pipe,
        &access,
        mutation::Operation::Delete,
        conn.delete_pipe(&h.pipe),
    )
    .await?;
    ctx.state.pipes(ik.as_deref(), true).await?;
    forget_access(&ctx);
    Ok(tuple_response(&res))
}

fn json_str_list(raw: &str, key: &str) -> Result<Vec<Value>, ApiError> {
    match Value::from_json_str(raw) {
        Ok(Value::List(items)) => Ok(items),
        Ok(Value::Null) => Ok(Vec::new()),
        Ok(other) => Ok(vec![other]),
        Err(e) => Err(ApiError::exception(&format!("json.loads({key})"), e)),
    }
}

/// `keys` without the pipes `access` cannot see.
async fn visible_keys(ctx: &Ctx, access: &Access, keys: PipesKeys) -> Result<PipesKeys, ApiError> {
    if access.is_admin() {
        return Ok(keys);
    }
    let index = access::pipe_index(&ctx.state, ctx.instance_keys().as_deref()).await?;
    let visible = |e: &meerschaum_connector::PipeKeysEntry| {
        if e.connector_keys == "mrsm" {
            return false;
        }
        let lk = normalize_location(e.location_key.as_deref());
        index
            .get(&e.connector_keys, &e.metric_key, lk.as_deref())
            .is_none_or(|p| access.can_see_indexed(p))
    };
    Ok(match keys {
        PipesKeys::List(v) => PipesKeys::List(v.into_iter().filter(|e| visible(e)).collect()),
        PipesKeys::Dict(m) => PipesKeys::Dict(m.into_iter().filter(|(_, e)| visible(e)).collect()),
    })
}

async fn fetch_pipes_keys(ctx: Ctx) -> ApiResult {
    let (access, operation) = authorize_catalog(&ctx).await?;
    let q = &ctx.query;
    let as_dict = q.bool("as_dict", false)?;
    let conn = ctx
        .state
        .get_api_connector(ctx.instance_keys().as_deref())?;
    let strs = |v: Vec<Value>| v.iter().map(Value::py_str).collect::<Vec<_>>();
    let query = PipesKeysQuery {
        connector_keys: strs(json_str_list(
            &q.str_or("connector_keys", "[]"),
            "connector_keys",
        )?),
        metric_keys: strs(json_str_list(
            &q.str_or("metric_keys", "[]"),
            "metric_keys",
        )?),
        location_keys: json_str_list(&q.str_or("location_keys", "[]"), "location_keys")?
            .iter()
            .map(|v| match v {
                Value::Null => None,
                other => Some(other.py_str()),
            })
            .collect(),
        tags: strs(json_str_list(&q.str_or("tags", "[]"), "tags")?),
        params: match Value::from_json_str(&q.str_or("params", "{}")) {
            Ok(Value::Map(m)) => Some(m),
            Ok(_) => None,
            Err(e) => return Err(ApiError::exception("json.loads(params)", e)),
        },
    };
    let keys = conn
        .fetch_pipes_keys(&query)
        .await
        .map_err(|e| ApiError::exception("fetch_pipes_keys", e))?;
    let keys = visible_keys(&ctx, &access, keys).await?;
    let keys = if let Some(operation) = &operation {
        let instance = ctx
            .instance_keys()
            .unwrap_or_else(|| ctx.state.config.instance_keys.clone());
        let allowed = |entry: &meerschaum_connector::PipeKeysEntry| {
            operation.permits(
                &instance,
                &entry.connector_keys,
                &entry.metric_key,
                entry.location_key.as_deref(),
            )
        };
        match keys {
            PipesKeys::List(entries) => {
                PipesKeys::List(entries.into_iter().filter(allowed).collect())
            }
            PipesKeys::Dict(entries) => PipesKeys::Dict(
                entries
                    .into_iter()
                    .filter(|(_, entry)| allowed(entry))
                    .collect(),
            ),
        }
    } else {
        keys
    };
    let out = match keys {
        PipesKeys::Dict(m) => {
            if as_dict {
                Json::Object(
                    m.iter()
                        .map(|(k, v)| (k.to_string(), from_core(&Value::List(v.to_list()))))
                        .collect(),
                )
            } else {
                Json::Array(
                    m.values()
                        .map(|v| from_core(&Value::List(v.to_list().into_iter().take(3).collect())))
                        .collect(),
                )
            }
        }
        PipesKeys::List(l) => {
            if as_dict {
                Json::Object(
                    l.iter()
                        .enumerate()
                        .map(|(i, v)| (i.to_string(), from_core(&Value::List(v.to_list()))))
                        .collect(),
                )
            } else {
                Json::Array(
                    l.iter()
                        .map(|v| from_core(&Value::List(v.to_list().into_iter().take(3).collect())))
                        .collect(),
                )
            }
        }
    };
    Ok(ok_json(&out))
}

async fn get_pipes(ctx: Ctx) -> ApiResult {
    let (access, operation) = authorize_catalog(&ctx).await?;
    let q = &ctx.query;
    let ik = q.opt_str("instance_keys").filter(|s| !s.is_empty());
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    // `get_pipes(connector_keys=..., ...)` with the raw strings as one-element filters.
    let one = |k: &str| {
        q.get(k)
            .filter(|s| !s.is_empty())
            .map(|s| vec![s.to_string()])
            .unwrap_or_default()
    };
    let query = PipesKeysQuery {
        connector_keys: one("connector_keys"),
        metric_keys: one("metric_keys"),
        location_keys: one("location_keys")
            .into_iter()
            .map(|l| normalize_location(Some(&l)))
            .collect(),
        ..Default::default()
    };
    let keys = conn
        .fetch_pipes_keys(&query)
        .await
        .map_err(|e| ApiError::exception("fetch_pipes_keys", e))?;
    // PARITY: these pipes are built from `get_pipes()` and their attributes are the local
    // ones: no `pipe_id`, and `instance_keys: null` (`api/routes/_pipes.py:223`).
    let mut out = serde_json::Map::new();
    for entry in keys.entries() {
        if operation.as_ref().is_some_and(|operation| {
            !operation.permits(
                &conn.connector_keys(),
                &entry.connector_keys,
                &entry.metric_key,
                entry.location_key.as_deref(),
            )
        }) {
            continue;
        }
        let lk = normalize_location(entry.location_key.as_deref());
        let params = match &entry.extra {
            Some(Value::Map(m)) => Value::Map(m.clone()),
            Some(Value::Str(s)) => match Value::from_json_str(s) {
                Ok(v @ Value::Map(_)) => v,
                _ => Value::new_map(),
            },
            _ => {
                let h = ctx
                    .state
                    .get_pipe(
                        &entry.connector_keys,
                        &entry.metric_key,
                        lk.as_deref().or(Some("None")),
                        ik.as_deref(),
                        false,
                    )
                    .await?;
                Value::Map(h.pipe.raw_parameters().clone())
            }
        };
        let listed_pipe = Pipe::builder(&entry.connector_keys, &entry.metric_key)
            .location(lk.as_deref())
            .parameters(params.to_map())
            .build()
            .map_err(|e| ApiError::exception("Pipe", e))?;
        if !access.can_see_pipe(&listed_pipe) {
            continue;
        }
        let attrs = json!({
            "connector_keys": entry.connector_keys,
            "metric_key": entry.metric_key,
            "location_key": lk,
            "instance_keys": null,
            "parameters": from_core(&params),
        });
        let metrics = out
            .entry(entry.connector_keys.clone())
            .or_insert_with(|| Json::Object(Default::default()));
        if let Some(m) = metrics.as_object_mut() {
            let locs = m
                .entry(entry.metric_key.clone())
                .or_insert_with(|| Json::Object(Default::default()));
            if let Some(l) = locs.as_object_mut() {
                l.insert(lk.unwrap_or_else(|| "None".into()), attrs);
            }
        }
    }
    Ok(ok_json(&Json::Object(out)))
}

async fn get_pipes_by_connector(ctx: Ctx, Path(ck): Path<String>) -> ApiResult {
    let access = authorize(&ctx, PIPES_READ).await?;
    let ik = ctx.instance_keys();
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let not_found = || ApiError::new(404, format!("Connector '{ck}' not found."));
    let Some(metrics) = dict.get(&ck) else {
        return Err(not_found());
    };
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    let mut out = serde_json::Map::new();
    for (mk, locations) in metrics {
        let seen: Vec<_> = locations
            .iter()
            .filter(|(_, h)| access.can_see_pipe(&h.pipe))
            .collect();
        if seen.is_empty() {
            continue;
        }
        out.insert(
            mk.clone(),
            Json::Object(attributes_tree(&conn, seen).await?),
        );
    }
    if out.is_empty() && !metrics.is_empty() {
        return Err(not_found());
    }
    Ok(ok_json(&Json::Object(out)))
}

async fn get_pipes_by_connector_and_metric(
    ctx: Ctx,
    Path((ck, mk)): Path<(String, String)>,
) -> ApiResult {
    let access = authorize(&ctx, PIPES_READ).await?;
    let ik = ctx.instance_keys();
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let visible = |locations: &indexmap::IndexMap<Option<String>, Arc<PipeHandle>>| {
        locations.values().any(|h| access.can_see_pipe(&h.pipe))
    };
    let Some(metrics) = dict.get(&ck).filter(|m| m.values().any(visible)) else {
        return Err(ApiError::new(404, format!("Connector '{ck}' not found.")));
    };
    let Some(locations) = metrics.get(&mk).filter(|l| visible(l)) else {
        return Err(ApiError::new(404, format!("Metric '{mk}' not found.")));
    };
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    let seen: Vec<_> = locations
        .iter()
        .filter(|(_, h)| access.can_see_pipe(&h.pipe))
        .collect();
    Ok(ok_json(&Json::Object(attributes_tree(&conn, seen).await?)))
}

async fn get_pipe_by_keys(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let ik = ctx.instance_keys();
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let visible = |locations: &indexmap::IndexMap<Option<String>, Arc<PipeHandle>>| {
        locations.values().any(|h| access.can_see_pipe(&h.pipe))
    };
    let Some(metrics) = dict.get(&ck).filter(|m| m.values().any(visible)) else {
        return Err(ApiError::new(404, format!("Connector '{ck}' not found.")));
    };
    let Some(locations) = metrics.get(&mk).filter(|l| visible(l)) else {
        return Err(ApiError::new(404, format!("Metric '{mk}' not found.")));
    };
    let loc = normalize_location(Some(&lk));
    let Some(h) = locations.get(&loc).filter(|h| access.can_see_pipe(&h.pipe)) else {
        return Err(ApiError::new(
            404,
            format!(
                "location_key '{}' not found.",
                loc.as_deref().unwrap_or("None")
            ),
        ));
    };
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    let attrs = pipe_attributes(&conn, &h.pipe).await?;
    Ok(ok_json(&from_core(&Value::Map(attrs))))
}

async fn get_pipe_id(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let id = conn
        .get_pipe_id(&h.pipe)
        .await
        .map_err(|e| ApiError::exception("get_pipe_id", e))?;
    match id {
        None => Err(ApiError::new(404, "Pipe is not registered.")),
        Some(id) => Ok(ok_json(&from_core(&id.to_value()))),
    }
}

async fn get_pipe_attributes(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, true).await?;
    let attrs = pipe_attributes(&conn, &h.pipe).await?;
    Ok(ok_json(&from_core(&Value::Map(attrs))))
}

async fn get_pipe_exists(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    // A catalog query that fails is a `500`, never `false`: a client deciding whether a pipe
    // has rows to read must not take a failed instance for a missing table (v4 clients still
    // read any refusal as `False`).
    let exists = conn
        .pipe_exists_checked(&h.pipe)
        .await
        .map_err(|e| ApiError::exception("pipe_exists", e))?;
    Ok(ok_json(&Json::Bool(exists)))
}

async fn create_metadata(ctx: Ctx) -> ApiResult {
    authorize(&ctx, &["actions:execute"]).await?;
    let conn = ctx
        .state
        .get_api_connector(ctx.instance_keys().as_deref())?;
    // `if conn.type not in ('sql', 'api'): return False`.
    if !matches!(conn.connector_type(), "sql" | "api") {
        return Ok(ok_json(&Json::Bool(false)));
    }
    // `get_tables(mrsm_instance=conn)`: creates the instance tables of a SQL instance (an API
    // instance has none of its own).
    // PARITY: v4 only logs a failed `create_all` (`tables/__init__.py: create_tables`) and still
    // answers `true`; a failure here is a 500 carrying the reason.
    match conn.create_instance_tables().await {
        Ok(true) => Ok(ok_json(&Json::Bool(true))),
        Ok(false) => Err(ApiError::new(500, "Failed to create the instance tables.")),
        Err(e) => Err(ApiError::new(500, e.to_string())),
    }
}

// ------------------------------------------------------------------------------------------------
// Data
// ------------------------------------------------------------------------------------------------

async fn get_sync_time(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, &[]).await?;
    let body = parse_json_body(&body_bytes(body, ctx.state.config.limits.body).await?)?;
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let params = optional_dict_body(body)?;
    let newest = ctx.query.bool("newest", true)?;
    let _remote = ctx.query.bool("remote", false)?;
    let round_down = ctx.query.bool("round_down", true)?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let query = DataQuery {
        params: params.clone(),
        ..Default::default()
    };
    let window = original.check(&ctx, &h.pipe, &query).await?;
    let st = pipe_methods::get_sync_time(
        &conn,
        &h.pipe,
        params.as_ref(),
        newest,
        round_down,
        &ctx.state.config.pipe_defaults,
    )
    .await?;
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    Ok(ok_json(&from_core(&bound_json(&st))))
}

/// Parse the body of `POST .../data` like the Python route: JSON, else UTF-8 text of
/// `string_to_dict` lines.
fn parse_sync_body(body: &[u8]) -> Result<Option<SyncPayload>, ApiError> {
    let data = match serde_json::from_slice::<Json>(body) {
        Ok(j) => {
            let empty = match &j {
                Json::Null => true,
                Json::Bool(b) => !b,
                Json::Number(n) => n.as_f64() == Some(0.0),
                Json::String(s) => s.is_empty(),
                Json::Array(a) => a.is_empty(),
                Json::Object(o) => o.is_empty(),
            };
            if empty {
                return Ok(None);
            }
            match j {
                Json::String(s) => SyncText(s),
                other => return Ok(Some(SyncPayload::Json(other))),
            }
        }
        Err(_) => SyncText(String::from_utf8_lossy(body).into_owned()),
    };
    let SyncText(text) = data;
    if text.is_empty() {
        return Ok(None);
    }
    // PARITY: `not data.lstrip()[0] not in ('{', '[')` is always true for a non-blank string,
    // so every string is parsed as simple-dict lines (`api/routes/_pipes.py:411`).
    if text.trim().is_empty() {
        // PARITY: `HTTPException(status=400, ...)` is a `TypeError`, a 500.
        return Err(ApiError::internal());
    }
    let mut docs = Vec::new();
    for line in meerschaum_core::pyfmt::py_splitlines(&text, false) {
        match meerschaum_core::pyfmt::string_to_dict(line) {
            Ok(d) => docs.push(d),
            Err(_) => return Err(ApiError::internal()),
        }
    }
    if docs.is_empty() {
        return Err(ApiError::internal());
    }
    Ok(Some(SyncPayload::Docs(docs)))
}

struct SyncText(String);

/// Data POST admission retains the request's credential, never an ambient task identity.
/// Native preparation can await metadata or retries; each subsequent write rechecks the
/// original credential and the stored pipe instead of the earlier route snapshot.
struct DataAdmission {
    batch: Option<Arc<super::graph_batch::WriteBatch>>,
    state: AppState,
    headers: axum::http::HeaderMap,
    columns: Option<Value>,
    replace: bool,
    registration: std::sync::Mutex<Option<String>>,
    reference: String,
    logical_reference: String,
    native_alias: Option<super::engine::selection::SelectedEngine>,
    innodb_user_service: bool,
    direct_administrator: bool,
    graph_target: std::sync::Mutex<
        Option<(
            crate::executor::pipe_operation::CallbackBinding,
            meerschaum_connector::mutation::NativeTarget,
        )>,
    >,
    parameters: std::sync::Mutex<Option<Map>>,
    inference_failure: std::sync::Mutex<Option<String>>,
    snapshot_safe: std::sync::atomic::AtomicBool,
}

/// Native MCP document writes retain the same original credential and inference authority
/// as the data POST route, including after database lock waits.
pub(crate) fn document_admission(
    state: &AppState,
    headers: axum::http::HeaderMap,
    pipe: &Pipe,
) -> Arc<dyn pipe_methods::SyncAdmission> {
    captured_data_admission(state, headers, pipe, false, None)
}
pub(super) fn replacement_admission(
    state: &AppState,
    headers: axum::http::HeaderMap,
    pipe: &Pipe,
    batch: Option<Arc<super::graph_batch::WriteBatch>>,
) -> Arc<dyn pipe_methods::SyncAdmission> {
    captured_data_admission(state, headers, pipe, true, batch)
}
fn captured_data_admission(
    state: &AppState,
    headers: axum::http::HeaderMap,
    pipe: &Pipe,
    replace: bool,
    batch: Option<Arc<super::graph_batch::WriteBatch>>,
) -> Arc<dyn pipe_methods::SyncAdmission> {
    Arc::new(DataAdmission {
        batch,
        state: state.clone(),
        headers,
        columns: None,
        replace,
        registration: Default::default(),
        reference: pipe.pipe_ref(),
        logical_reference: pipe.pipe_ref(),
        native_alias: None,
        innodb_user_service: false,
        direct_administrator: false,
        graph_target: Default::default(),
        parameters: Default::default(),
        inference_failure: Default::default(),
        snapshot_safe: Default::default(),
    })
}

/// Shared synchronous data-destination guards. The caller and metadata must have been
/// refreshed after the last preparation await; this helper performs no canonical reads.
pub(crate) fn check_data_destination(
    caller: &Access,
    conn: &dyn InstanceConnector,
    pipe: &Pipe,
    hidden: &crate::sql_guard::HiddenTables,
) -> Result<(), ApiError> {
    if !caller.can_see_pipe(pipe) {
        return Err(access::hidden_pipe(pipe));
    }
    if !caller.can_edit(pipe.raw_parameters()) {
        return Err(access::not_editable(pipe));
    }
    access::guard::check_pipe_target(conn, pipe, hidden)?;
    let table = target(pipe);
    if meerschaum_connector_api::routes::PROTECTED_TARGETS.contains(&table.as_str()) {
        return Err(ApiError::new(
            409,
            format!("Cannot sync data to protected table '{table}'."),
        ));
    }
    Ok(())
}

impl DataAdmission {
    fn check_administrator(&self, caller: &Access) -> Result<(), ApiError> {
        if self.direct_administrator && !mutation::direct_administrator(caller) {
            return Err(ApiError::new(
                403,
                "The original administrator authority changed.",
            ));
        }
        Ok(())
    }

    async fn canonical(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        requested: &Pipe,
    ) -> Result<Pipe, ApiError> {
        if let Some(reason) = self
            .inference_failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
        {
            return Err(ApiError::new(409, reason));
        }
        if let Some(alias) = &self.native_alias {
            if !Arc::ptr_eq(conn, &alias.connector()) || requested.pipe_ref() != self.reference {
                return Err(ApiError::new(
                    409,
                    "The selected native data target changed.",
                ));
            }
            let source = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
            let graph = self
                .state
                .actions
                .pipe_operation_read(&source)
                .await
                .map_err(|reason| ApiError::new(409, reason))?
                .is_some();
            let pipe = if graph {
                alias
                    .native_graph_target(&self.state, &self.headers, &self.logical_reference)
                    .await?
                    .0
            } else {
                alias
                    .native_data_pipe(&self.state, &self.headers, &self.logical_reference)
                    .await?
            };
            let registration = pipe.registration_id();
            let parameters = pipe.raw_parameters().clone();
            {
                let mut expected = self
                    .registration
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if expected.is_some() && *expected != registration {
                    return Err(ApiError::new(409, "The logical data registration changed."));
                }
                *expected = registration;
            }
            {
                let mut expected = self
                    .parameters
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if expected
                    .as_ref()
                    .is_some_and(|accepted| accepted != &parameters)
                {
                    return Err(ApiError::new(
                        409,
                        "The logical data definition changed during preparation.",
                    ));
                }
                *expected = Some(parameters);
            }
            return Ok(pipe);
        }
        if conn.connector_type() == "api" {
            return Err(ApiError::new(
                501,
                "Scoped API data writes require a bound native or forwarding mutation capability.",
            ));
        }
        self.state
            .get_api_connector(Some(requested.instance_keys()))?;
        let source = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        if matches!(&source.auth, Some(crate::auth::Principal::Execution(identity)) if identity.grant.required.contains("pipes:write"))
        {
            // A typed operation must not use a peer-edited definition before the native
            // inference receipt has advanced its exact canonical snapshot.
            self.state
                .actions
                .pipe_operation_read(&source)
                .await
                .map_err(|reason| ApiError::new(409, reason))?;
        }
        let attributes = conn
            .get_pipe_attributes(requested)
            .await
            .map_err(|e| ApiError::exception("Failed to load a pipe's attributes", e))?;
        let registered = !attributes.is_empty();
        let stored_parameters = crate::state::parameters_of(&attributes);
        if registered {
            let mut expected = self
                .parameters
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if expected.is_some() && *expected != stored_parameters {
                return Err(ApiError::new(
                    409,
                    "The pipe definition changed during this data request; retry with fresh metadata.",
                ));
            }
            *expected = stored_parameters.clone();
        }
        let registration = attributes
            .get("pipe_id")
            .and_then(|identity| match identity {
                Value::Int(_) | Value::Str(_) => Some(identity.py_str()),
                _ => None,
            });
        {
            let mut expected = self.registration.lock().unwrap_or_else(|e| e.into_inner());
            if expected.is_some() && *expected != registration {
                return Err(ApiError::new(
                    409,
                    "The pipe's registration changed during the sync; retry the request.",
                ));
            }
            if registered {
                *expected = registration;
            }
        }
        // The hidden-target index and source permissions may have changed while metadata
        // was prepared. Refresh both; a failed read is not default public access.
        let dict = self
            .state
            .pipes(Some(requested.instance_keys()), true)
            .await?;
        let caller = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        self.check_administrator(&caller)?;
        access::guard::check_connector_teams(&self.state, &caller, requested.instance_keys())?;
        let mut pipe = requested.clone();
        pipe.temporary = false;
        pipe.set_parameters(if registered {
            crate::state::parameters_of(&attributes).ok_or_else(|| {
                ApiError::new(503, "The pipe's registered parameters are unavailable.")
            })?
        } else {
            // The data route accepts columns, not caller-selected access or SQL definitions.
            Map::new()
        });
        if pipe.columns().is_empty()
            && let Some(columns) = &self.columns
        {
            pipe.set_columns(columns.clone())
                .map_err(|e| ApiError::exception("pipe.columns", e))?;
        }
        if !registered {
            caller.require(&["pipes:register"], &[])?;
            if !self.state.config.registration_pipes {
                return Err(ApiError::new(
                    403,
                    "The server has not allowed pipe registration.",
                ));
            }
            access::guard::check_source_connector(&self.state, &caller, pipe.connector_keys())?;
            let stamped = access::guard::stamp_new_pipe(&caller, pipe.raw_parameters())?;
            access::guard::check_parameters(&self.state, &caller, conn, &pipe, None, &stamped)
                .await?;
            pipe.set_parameters(stamped);
        }
        // Resolve authority again after parameter/template checks, which can themselves await.
        let caller = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        self.check_administrator(&caller)?;
        access::guard::check_connector_teams(&self.state, &caller, pipe.instance_keys())?;
        if !registered {
            caller.require(&["pipes:register"], &[])?;
            access::guard::check_source_connector(&self.state, &caller, pipe.connector_keys())?;
            // No authority from the first read may set the new registration's identity.
            let mut parameters = pipe.raw_parameters().clone();
            parameters.shift_remove("access");
            pipe.set_parameters(access::guard::stamp_new_pipe(&caller, &parameters)?);
            let team = access::rules::PipeAccess::from_parameters(pipe.raw_parameters()).team;
            access::guard::check_pipe_connector(
                &self.state,
                &caller,
                pipe.connector_keys(),
                team.as_deref(),
            )?;
        }
        let index = access::index_from_dict(pipe.instance_keys(), &dict, conn.as_ref());
        check_data_destination(
            &caller,
            conn.as_ref(),
            &pipe,
            &index.hidden_targets(&caller),
        )?;
        Ok(pipe)
    }
}

#[async_trait::async_trait]
impl pipe_methods::SyncAdmission for DataAdmission {
    fn direct_administrator(&self) -> bool {
        self.direct_administrator
    }

    fn native_batch(&self) -> Option<meerschaum_connector::mutation::NativeBatch> {
        self.batch.as_ref().map(|batch| batch.native.clone())
    }
    async fn begin(&self, conn: &Arc<dyn InstanceConnector>, pipe: &Pipe) -> Result<(), ApiError> {
        self.canonical(conn, pipe).await?;
        if let Some(batch) = &self.batch {
            return batch.confirm(&self.state).await;
        }
        let source = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        if self
            .state
            .actions
            .pipe_operation_read(&source)
            .await
            .map_err(|reason| ApiError::new(409, reason))?
            .is_some()
        {
            return Err(ApiError::new(
                409,
                "A captured graph mutation requires its Main-issued native batch claim.",
            ));
        }
        Ok(())
    }
    async fn write_window(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        pipe: &Pipe,
    ) -> Result<Option<meerschaum_connector::mutation::DataWindow>, ApiError> {
        let canonical = self.canonical(conn, pipe).await?;
        let source = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        let Some(read) = self
            .state
            .actions
            .pipe_operation_read(&source)
            .await
            .map_err(|reason| ApiError::new(409, reason))?
        else {
            return Ok(None);
        };
        // Only a captured graph target can opt direct SQL into a held auth
        // snapshot, and it cannot target that snapshot's own auth/registration
        // storage through a logical or physical alias. The SQL native adapter
        // independently repeats this before binding the writer.
        access::guard::check_pipe_target(
            conn.as_ref(),
            &canonical,
            &crate::sql_guard::HiddenTables::default(),
        )?;
        self.snapshot_safe
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(alias) = &self.native_alias {
            if !Arc::ptr_eq(conn, &alias.connector()) {
                return Err(ApiError::new(409, "The native graph connector changed."));
            }
            let (physical, window) = alias
                .native_graph_target(&self.state, &self.headers, &self.logical_reference)
                .await?;
            if physical.pipe_ref() != canonical.pipe_ref()
                || physical.raw_parameters() != canonical.raw_parameters()
            {
                return Err(ApiError::new(
                    409,
                    "The native graph target changed after preparation.",
                ));
            }
            let target = window
                .native_target()
                .cloned()
                .ok_or_else(|| ApiError::new(409, "The native graph mapping disappeared."))?;
            let mut retained = self
                .graph_target
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if retained
                .as_ref()
                .is_some_and(|(binding, old)| binding != &read.binding || old != &target)
            {
                return Err(ApiError::new(
                    409,
                    "The retained native graph mapping changed.",
                ));
            }
            *retained = Some((read.binding, target));
            Ok(Some(window))
        } else {
            Ok(Some(read.write_window(&canonical)?))
        }
    }
    async fn prepare_data_commit(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        pipe: &Pipe,
        effect: &meerschaum_connector::mutation::NativeDataEffect,
    ) -> Result<Option<Arc<dyn meerschaum_connector::mutation::PendingDataCommit>>, ApiError> {
        let window = self
            .write_window(conn, pipe)
            .await?
            .ok_or_else(|| ApiError::new(409, "The target commit lost its Main-issued window."))?;
        if &window != effect.window()
            || self.reference != effect.physical_reference()
            || self.logical_reference != effect.logical_reference()
        {
            return Err(ApiError::new(
                409,
                "The native target effect differs from this exact data request.",
            ));
        }
        let source = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
        self.state
            .actions
            .prepare_pipe_commit(&source, effect.clone())
            .await
            .map(Some)
            .map_err(|reason| ApiError::new(409, reason))
    }
    fn lock_snapshot_reads(&self) -> bool {
        self.native_alias.is_some()
            || self
                .snapshot_safe
                .load(std::sync::atomic::Ordering::Acquire)
    }
    fn same_store_innodb_reads(&self, conn: &Arc<dyn InstanceConnector>) -> bool {
        self.innodb_user_service
            && self.batch.is_some()
            && !self.state.config.no_auth
            && Arc::ptr_eq(conn, &self.state.instance())
    }
    fn tracks_metadata_inference(&self) -> bool {
        true
    }

    async fn metadata_inferred(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        change: meerschaum_connector::mutation::MetadataInference,
    ) -> Result<(), ApiError> {
        let result = async {
            change
                .validate()
                .map_err(|reason| ApiError::new(409, reason))?;
            if change.pipe.pipe_ref() != self.reference
                || self
                    .registration
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .as_deref()
                    != Some(change.registration.py_str().as_str())
                || self
                    .parameters
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .as_ref()
                    != Some(&change.before)
            {
                return Err(ApiError::new(
                    409,
                    "Native inference lost the accepted data request's registration or parameters.",
                ));
            }
            let attributes = conn.get_pipe_attributes(&change.pipe).await.map_err(|_| {
                ApiError::unavailable("Could not confirm committed native inference.")
            })?;
            if attributes.get("pipe_id") != Some(&change.registration)
                || crate::state::parameters_of(&attributes).as_ref() != Some(&change.after)
            {
                return Err(ApiError::new(
                    409,
                    "The registered metadata differs from committed native inference.",
                ));
            }
            if let Some(alias) = &self.native_alias {
                let retained = self
                    .graph_target
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                let pipe = if let Some((binding, target)) = retained {
                    let logical = alias
                        .native_graph_inference(
                            &self.state,
                            &self.headers,
                            &binding,
                            &target,
                            &change,
                        )
                        .await?;
                    let source =
                        access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
                    if !self
                        .state
                        .actions
                        .accept_pipe_inference(&source, logical)
                        .await
                        .map_err(|reason| ApiError::new(409, reason))?
                    {
                        return Err(ApiError::new(
                            409,
                            "The native graph inference was not accepted by its canonical issuer.",
                        ));
                    }
                    alias
                        .native_graph_target(&self.state, &self.headers, &self.logical_reference)
                        .await?
                        .0
                } else {
                    alias
                        .native_data_pipe(&self.state, &self.headers, &self.logical_reference)
                        .await?
                };
                if pipe.registration_id().as_deref() != Some(change.registration.py_str().as_str())
                    || pipe.raw_parameters() != &change.after
                {
                    return Err(ApiError::new(
                        409,
                        "The logical native inference registration changed.",
                    ));
                }
                let mut expected = self
                    .parameters
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if expected.as_ref() != Some(&change.before) {
                    return Err(ApiError::new(
                        409,
                        "Concurrent logical native inference changed this request.",
                    ));
                }
                *expected = Some(change.after);
                return Ok(());
            }
            let caller = access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
            access::guard::check_parameters(
                &self.state,
                &caller,
                conn,
                &change.pipe,
                Some(&change.before),
                &change.after,
            )
            .await?;
            self.state
                .actions
                .accept_pipe_inference(&caller, change.clone())
                .await
                .map_err(|reason| ApiError::new(409, reason))?;
            access::require_fresh(&self.state, &self.headers, PIPES_WRITE).await?;
            let mut expected = self
                .parameters
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if expected.as_ref() != Some(&change.before) {
                return Err(ApiError::new(
                    409,
                    "Concurrent native inference changed this data request.",
                ));
            }
            *expected = Some(change.after);
            Ok(())
        }
        .await;
        if let Err(error) = &result {
            let reason = format!(
                "Native metadata inference committed before this data request was refused: {}. Its committed metadata was not rolled back. Start a fresh operation after resolving its original authorization or metadata conflict.",
                meerschaum_core::debug::redact_text(&error.detail_text())
                    .chars()
                    .take(512)
                    .collect::<String>()
            );
            *self
                .inference_failure
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(reason.clone());
            return Err(ApiError::new(409, reason));
        }
        result
    }

    async fn prepare(
        &self,
        conn: &Arc<dyn InstanceConnector>,
        pipe: &Pipe,
    ) -> Result<Pipe, ApiError> {
        let pipe = self.canonical(conn, pipe).await?;
        if let Some(error) = access::guard::lock_conflict(
            &pipe,
            if self.replace {
                LockOp::Replace
            } else {
                LockOp::Sync
            },
        ) {
            return Err(error);
        }
        Ok(pipe)
    }
}

async fn sync_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let batch_request = super::graph_batch::WriteRequest::capture(&ctx).await?;
    let caller = authorize(&ctx, PIPES_WRITE).await?;
    let q = &ctx.query;
    let check_existing = q.bool("check_existing", true)?;
    let blocking = q.bool("blocking", true)?;
    if batch_request.is_some() && !blocking {
        return Err(ApiError::new(
            400,
            "A native batch requires a synchronous write result.",
        ));
    }
    let force = q.bool("force", false)?;
    let workers = q.opt_int("workers")?;
    let columns = q.opt_str("columns");
    let special_dtypes =
        pipe_methods::special_input_hints(q.opt_str("new_special_cols").as_deref())?;
    // `replace=true` (rc15): the rows replace the window `[begin, end)` matching `params`; with no
    // rows the window is cleared. A bound that is not sent is the posted rows' own range (as for
    // `pipe.sync(df, replace=True)`); `None` (or empty) is no bound on that side.
    let replace = q.bool("replace", false)?;
    let window_bound = |key: &str| -> Result<WindowBound, ApiError> {
        match q.opt_str(key) {
            None => Ok(WindowBound::FromRows),
            Some(v) if v.is_empty() || v == "None" => Ok(WindowBound::Unbounded),
            Some(v) => DateBound::parse(&v)
                .map(WindowBound::At)
                .ok_or_else(|| ApiError::new(400, format!("Invalid `{key}`: '{v}'."))),
        }
    };
    let (window_begin, window_end) = (window_bound("begin")?, window_bound("end")?);
    let window_params = match q.opt_str("params").filter(|p| !p.is_empty()) {
        Some(p) => Some(
            Value::from_json_str(&p)
                .map_err(|e| ApiError::exception("json.loads(params)", e))?
                .to_map(),
        ),
        None => None,
    };
    let limit = ctx.state.config.limits.data;
    let limit = if batch_request.is_some() {
        let maximum = meerschaum_connector::mutation::MAX_NATIVE_BATCH_BODY;
        Some(limit.map_or(maximum, |limit| limit.min(maximum)))
    } else {
        limit
    };
    let body = body_bytes(body, limit).await?;
    let (payload, no_rows) = match parse_sync_body(&body)? {
        Some(payload) => (payload, false),
        None if replace => (SyncPayload::Json(Json::Array(Vec::new())), true),
        None if batch_request.is_some() => {
            return Err(ApiError::new(
                400,
                "A native data batch requires a target operation.",
            ));
        }
        None => return Ok(tuple(true, "No data to sync.")),
    };
    let selected = ctx.selected_engine(&caller)?;
    let graph = ctx
        .state
        .actions
        .pipe_operation_read(&caller)
        .await
        .map_err(|reason| ApiError::new(409, reason))?
        .is_some();
    if graph && batch_request.is_none() {
        return Err(ApiError::new(
            409,
            "A captured graph data write requires its Main-issued batch selector.",
        ));
    }
    if (selected.connector().connector_type() == "api" || selected.is_native_alias())
        && !(graph && selected.is_native_alias())
    {
        selected.require_native_write()?;
        selected.fresh(&ctx, PIPES_WRITE).await?;
    }
    let (configured, h) = handle(&ctx, &ck, &mk, &lk, true).await?;
    let logical_reference = h.pipe.pipe_ref();
    let (conn, requested, native_alias) = if configured.connector_type() == "api" {
        selected.require_native_write()?;
        let physical = if graph {
            selected
                .native_graph_target(&ctx.state, &ctx.headers, &logical_reference)
                .await?
                .0
        } else {
            selected
                .native_data_pipe(&ctx.state, &ctx.headers, &logical_reference)
                .await?
        };
        (selected.connector(), physical, Some(selected))
    } else {
        (configured, h.pipe.clone(), None)
    };
    let batch = match batch_request {
        Some(request) => Some(
            request
                .claim(&ctx, &logical_reference, "data", &body)
                .await?,
        ),
        None => None,
    };
    let admission = Arc::new(DataAdmission {
        batch: batch.clone(),
        state: ctx.state.clone(),
        headers: ctx.headers.clone(),
        columns: columns
            .map(|c| {
                Value::from_json_str(&c).map_err(|e| ApiError::exception("json.loads(columns)", e))
            })
            .transpose()?,
        replace,
        registration: Default::default(),
        reference: requested.pipe_ref(),
        logical_reference,
        innodb_user_service: native_alias
            .as_ref()
            .is_some_and(|alias| alias.has_password_service())
            && matches!(&caller.auth,
                Some(crate::auth::Principal::Execution(identity)) if identity.is_user_bound()),
        direct_administrator: !graph
            && batch.is_none()
            && native_alias.is_none()
            && mutation::direct_administrator(&caller),
        native_alias,
        graph_target: Default::default(),
        parameters: Default::default(),
        inference_failure: Default::default(),
        snapshot_safe: Default::default(),
    });
    let pipe = admission.canonical(&conn, &requested).await?;
    if let Some(e) = access::guard::lock_conflict(
        &pipe,
        if replace {
            LockOp::Replace
        } else {
            LockOp::Sync
        },
    ) {
        return Ok(tuple(false, e.detail_text()));
    }
    // Clearing a window without rows needs the window: `replace=true` alone with an empty body
    // must not clear the whole table (a pipe without a datetime column has only that window).
    if no_rows
        && window_begin == WindowBound::FromRows
        && window_end == WindowBound::FromRows
        && pipe.column("datetime").is_some()
    {
        return Err(ApiError::new(
            400,
            meerschaum_connector::replace::no_rows_window(&pipe),
        ));
    }
    let kw = SyncKwargs {
        special_dtypes,
        check_existing,
        force,
        workers,
        replace,
        begin: window_begin,
        end: window_end,
        params: window_params,
    };
    let defaults = ctx.state.config.pipe_defaults.clone();
    // The pipe's sync status (rc15 C2) records this sync (`routes::sync_status`).
    let (started, status_headers) = (std::time::Instant::now(), ctx.headers.clone());
    let registration = meerschaum_connector::sync_metrics::observed_registration(&pipe);
    if !blocking {
        let msg = format!("Spawned asyncronous sync for {pipe}.");
        tokio::spawn(async move {
            let res = match pipe_methods::sync_with_admission(
                &conn,
                &pipe,
                &payload,
                &kw,
                &defaults,
                Some(admission.clone()),
            )
            .await
            {
                Ok(result) => result,
                Err(error) => SuccessTuple::fail(error.detail_text()),
            };
            super::sync_status::record_data_post(
                &status_headers,
                &conn,
                &pipe,
                &res,
                started,
                registration,
            );
            if !res.success {
                tracing::warn!("Asynchronous sync failed for {pipe}:\n{}", res.message);
            }
        });
        // PARITY: the typo `asyncronous` is v4's (`core/Pipe/_sync.py:539`).
        return Ok(tuple(true, msg));
    }
    let res = pipe_methods::sync_with_admission(
        &conn,
        &pipe,
        &payload,
        &kw,
        &defaults,
        Some(admission.clone()),
    )
    .await?;
    h.invalidate();
    // Queued before the response (a delete that follows forgets it); written in the background.
    super::sync_status::record_data_post(
        &status_headers,
        &conn,
        &pipe,
        &res,
        started,
        registration,
    );
    if let Some(batch) = batch
        && res.success
    {
        let receipt = batch.receipt(&ctx.state).await?;
        #[derive(serde::Serialize)]
        struct BatchResult<'a> {
            result: (bool, &'a str),
            receipt: meerschaum_connector_api::graph_batch::BatchReceipt,
        }
        let mut writer = crate::json::Bounded {
            bytes: Vec::new(),
            limit: meerschaum_connector_api::graph_batch::MAX_BATCH_REPLY,
        };
        serde_json::to_writer(
            &mut writer,
            &BatchResult {
                result: (res.success, &res.message),
                receipt,
            },
        )
        .map_err(|_| {
            ApiError::unavailable("The committed batch result exceeds its delivery limit.")
        })?;
        return Ok(crate::json::raw_json(
            axum::http::StatusCode::OK,
            String::from_utf8(writer.bytes).map_err(|_| ApiError::internal())?,
        ));
    }
    Ok(tuple_response(&res))
}

struct DataArgs {
    select_columns: Vec<String>,
    omit_columns: Vec<String>,
    params: Map,
    limit: i64,
    order: String,
}

fn data_args(ctx: &Ctx) -> Result<DataArgs, ApiError> {
    let q = &ctx.query;
    let max = ctx.state.config.max_response_row_limit;
    let limit = q.int("limit", max)?;
    let order = q.str_or("order", "asc");
    if limit > max {
        return Err(ApiError::new(
            413,
            format!("Requested limit {limit} exceeds the maximum response size of {max} rows."),
        ));
    }
    Ok(DataArgs {
        params: parse_params(q.get("params"))?,
        select_columns: parse_select_columns(q.get("select_columns"))?,
        omit_columns: parse_omit_columns(q.get("omit_columns"))?,
        limit,
        order,
    })
}

async fn get_pipe_data(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, PIPES_READ).await?;
    let access = original.initial().clone();
    let q = &ctx.query;
    let date_format = q.str_or("date_format", "iso");
    let date_unit = q.str_or("date_unit", "us");
    let double_precision = q.int("double_precision", 15)?;
    let geometry_format = q.str_or("geometry_format", "wkb_hex");
    let args = data_args(&ctx)?;
    let ik = ctx.instance_keys();
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    if !ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(
            409,
            "Pipe must be registered with the datetime column specified.",
        ));
    }
    refuse_protected_read(&h.pipe)?;
    let query = DataQuery {
        select_columns: nonempty(args.select_columns),
        omit_columns: nonempty(args.omit_columns),
        begin,
        end,
        params: Some(args.params),
        limit: Some(args.limit.min(ctx.state.config.max_response_row_limit)),
        order: Some(args.order),
        ..Default::default()
    };
    let window = original.check(&ctx, &h.pipe, &query).await?;
    let df = meerschaum_connector::pipe_ops::get_data(conn.as_ref(), &h.pipe, &query)
        .await
        .map_err(|e| ApiError::exception("get_pipe_data", e))?;
    let Some(df) = df else {
        return Err(ApiError::new(
            400,
            "Could not fetch data with the given parameters.",
        ));
    };
    let unit = DateUnit::parse(&date_unit).ok_or_else(ApiError::internal)?;
    let geometry_format = match geometry_format.as_str() {
        "geojson" => GeometryFormat::GeoJson,
        "wkt" => GeometryFormat::Wkt,
        _ => GeometryFormat::WkbHex,
    };
    let opts = DataJsonOptions {
        epoch: date_format == "epoch",
        date_unit: unit,
        double_precision: u32::try_from(double_precision.clamp(0, 15)).unwrap_or(15),
        geometry_format,
    };
    let encoded = data_to_json(&df, &opts);
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    Ok(raw_json(StatusCode::OK, encoded))
}

async fn get_pipe_docs(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, PIPES_READ).await?;
    let access = original.initial().clone();
    let args = data_args(&ctx)?;
    let q = &ctx.query;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    refuse_protected_read(&h.pipe)?;
    let query = DataQuery {
        select_columns: nonempty(args.select_columns),
        omit_columns: nonempty(args.omit_columns),
        begin,
        end,
        params: Some(args.params),
        limit: Some(args.limit.min(ctx.state.config.max_response_row_limit)),
        order: Some(args.order),
        ..Default::default()
    };
    // v4's route calls `pipe.get_docs()`, i.e. the instance's own `get_pipe_docs()`: the raw
    // rows, not the dtype-enforced frame (SQLite keeps its stored datetime text
    // `2024-01-02 03:04:05.000000`, MySQL-family naive datetimes serialize as `...Z`, the
    // PostgreSQL family as `...+00:00`).
    let window = original.check(&ctx, &h.pipe, &query).await?;
    let docs = conn
        .get_pipe_docs(&h.pipe, &query)
        .await
        .map_err(|e| ApiError::exception("get_pipe_docs", e))?;
    let value = Value::List(
        docs.iter()
            .map(|d| Value::Map(d.iter().map(|(k, v)| (k.clone(), doc_value(v))).collect()))
            .collect(),
    );
    let encoded = python_json(&value);
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    Ok(raw_json(StatusCode::OK, encoded))
}

/// A document cell as v4 serializes it: a SQL `NULL` is `null`, also in a float column, where the
/// frame holds `NaN` (v4's rows carry `None`).
// PARITY: a stored float `NaN` also becomes `null` (v4 would write the invalid-JSON `NaN`);
// the frame cannot tell the two apart.
fn doc_value(v: &meerschaum_kernel::Value) -> Value {
    match meerschaum_connector::pipe_ops::kernel_to_core(v) {
        Value::Float(f) if f.is_nan() => Value::Null,
        other => other,
    }
}

fn refuse_protected_read(pipe: &Pipe) -> Result<(), ApiError> {
    let t = target(pipe);
    if meerschaum_connector_api::routes::PROTECTED_TARGETS.contains(&t.as_str()) {
        return Err(ApiError::new(
            409,
            format!("Cannot retrieve data from protected table '{t}'."),
        ));
    }
    Ok(())
}

async fn get_pipe_chunk_bounds(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, &[]).await?;
    if original.graph(&ctx).await?.is_some() {
        return Err(ApiError::unavailable(
            "Delegated chunk grids must come from Main's exact native verification instructions.",
        ));
    }
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let q = &ctx.query;
    let bounded = q.bool("bounded", true)?;
    let chunk_interval_minutes = q.opt_int("chunk_interval_minutes")?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    let (_, dt_typ) = pipe_methods::dt_info(&conn, &h.pipe).await;
    let chunk_interval = chunk_interval_minutes.map(|m| {
        if meerschaum_core::dtypes::are_dtypes_equal(&dt_typ, "int") {
            ChunkIntervalArg::Int(m)
        } else {
            ChunkIntervalArg::Delta(TimeDelta::minutes(m))
        }
    });
    let bounds = pipe_methods::get_chunk_bounds(
        &conn,
        &h.pipe,
        begin,
        end,
        bounded,
        chunk_interval,
        defaults,
    )
    .await?;
    let value = Value::List(
        bounds
            .iter()
            .map(|(b, e)| Value::List(vec![bound_json(b), bound_json(e)]))
            .collect(),
    );
    Ok(raw_json(StatusCode::OK, python_json(&value)))
}

async fn drop_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_DROP).await?;
    let ik = ctx.instance_keys();
    let (conn, h) = editable(&ctx, &access, &ck, &mk, &lk, true).await?;
    if let Some(e) = access::guard::lock_conflict(&h.pipe, LockOp::Drop) {
        return Ok(tuple(false, e.detail_text()));
    }
    let res = mutation::run(
        &ctx,
        &conn,
        &h.pipe,
        &access,
        mutation::Operation::Drop,
        conn.drop_pipe(&h.pipe),
    )
    .await?;
    h.invalidate();
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

async fn clear_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_DELETE).await?;
    let q = &ctx.query;
    let params = parse_params(q.get("params"))?;
    let ik = ctx.instance_keys();
    let (conn, h) = editable(&ctx, &access, &ck, &mk, &lk, true).await?;
    if let Some(e) = access::guard::lock_conflict(&h.pipe, LockOp::Clear) {
        return Ok(tuple(false, e.detail_text()));
    }
    let defaults = &ctx.state.config.pipe_defaults;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    let query = DataQuery {
        begin,
        end,
        params: Some(params),
        ..Default::default()
    };
    let clear = conn.clear_pipe(&h.pipe, &query);
    let res = mutation::run(
        &ctx,
        &conn,
        &h.pipe,
        &access,
        mutation::Operation::Clear,
        clear,
    )
    .await?;
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

async fn delete_pipe_cache(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_READ).await?;
    let (_, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    // `pipe._invalidate_cache(hard=True)` on the shared object.
    h.invalidate();
    Ok(tuple_response(&SuccessTuple::success()))
}

async fn get_pipe_csv(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, PIPES_READ).await?;
    let access = original.initial().clone();
    let q = &ctx.query;
    let params = parse_params(q.get("params"))?;
    let ik = ctx.instance_keys();
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    refuse_protected_read(&h.pipe)?;
    if !ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(409, "Pipe must be registered."));
    }
    let defaults = &ctx.state.config.pipe_defaults;
    let mut begin =
        pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let mut end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    if h.pipe.column("datetime").is_some() && original.graph(&ctx).await?.is_none() {
        if begin.is_none() {
            begin =
                pipe_methods::get_sync_time(&conn, &h.pipe, None, false, false, defaults).await?;
        }
        if end.is_none() {
            end = pipe_methods::get_sync_time(&conn, &h.pipe, None, true, false, defaults)
                .await?
                .map(|e| match e {
                    DateBound::Int(i) => DateBound::Int(i + 1),
                    DateBound::Datetime(dt) => {
                        DateBound::Datetime(meerschaum_kernel::value::DateTimeValue {
                            utc: dt.utc + TimeDelta::minutes(1),
                            tz: dt.tz,
                        })
                    }
                });
        }
    }
    let bounds_text = match (&begin, &end) {
        (Some(b), Some(e)) => format!("-{}-{}", b.py_str(), e.py_str()),
        _ => String::new(),
    };
    let filename = format!("{}{bounds_text}.csv", target(&h.pipe));
    let query = DataQuery {
        begin,
        end,
        params: Some(params),
        ..Default::default()
    };
    let window = original.check(&ctx, &h.pipe, &query).await?;
    let df = meerschaum_connector::pipe_ops::get_data(conn.as_ref(), &h.pipe, &query)
        .await
        .map_err(|e| ApiError::exception("get_pipe_data", e))?;
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    let Some(df) = df else {
        // PARITY: `df.to_csv` on `None` raises `AttributeError`, a 500.
        return Err(ApiError::internal());
    };
    let mut resp = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/csv; charset=utf-8")],
        to_csv(&df),
    )
        .into_response();
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename={filename}")) {
        resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
    }
    Ok(resp)
}

/// `GET .../rowcount`: v4's route, plus (new in v5) `estimate=true` for the database's row
/// estimate (exact where there is none or counting is cheap) and `params` as a JSON query string
/// for clients that cannot send a body with a `GET` (the body wins when both are given).
async fn get_pipe_rowcount(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, &[]).await?;
    let body = parse_json_body(&body_bytes(body, ctx.state.config.limits.body).await?)?;
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let params = match optional_dict_body(body)? {
        Some(params) => Some(params),
        None => query_params_dict(&ctx)?,
    };
    let remote = ctx.query.bool("remote", false)?;
    let estimate = ctx.query.bool("estimate", false)?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let q = &ctx.query;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    // A count the instance fails to read is a `500`, not `0` (rc15 Q17).
    let query = DataQuery {
        begin: begin.clone(),
        end: end.clone(),
        params: params.clone(),
        remote,
        ..Default::default()
    };
    let window = original.check(&ctx, &h.pipe, &query).await?;
    if window.is_some() && estimate {
        return Err(ApiError::unavailable(
            "A full-table estimate cannot represent this Main-issued finite read.",
        ));
    }
    let n = pipe_methods::try_get_rowcount(&conn, &h.pipe, begin, end, params, remote, estimate)
        .await?;
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    Ok(ok_json(&json!(n)))
}

/// `?params=<JSON object>` (an empty value or `null` means none).
fn query_params_dict(ctx: &Ctx) -> Result<Option<Map>, ApiError> {
    let Some(text) = ctx.query.get("params").filter(|t| !t.trim().is_empty()) else {
        return Ok(None);
    };
    match Value::from_json_str(text) {
        Ok(Value::Map(m)) => Ok(Some(m)),
        Ok(Value::Null) => Ok(None),
        _ => Err(ApiError::validation(vec![ApiError::field_error(
            "dict_type",
            &["query", "params"],
            "Input should be a JSON object",
            Json::String(text.to_string()),
            None,
        )])),
    }
}

async fn get_pipe_columns_types(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let max_age = ctx.state.config.columns_types_cache_seconds;
    let types = match h.cached_columns_types(max_age) {
        Some(t) => t,
        None => {
            let t = conn
                .get_pipe_columns_types(&h.pipe)
                .await
                .map_err(|e| ApiError::exception("get_pipe_columns_types", e))?;
            h.cache_columns_types(t.clone());
            t
        }
    };
    let types = if types.is_empty() {
        meerschaum_connector::pipe_ops::get_dtypes(
            conn.as_ref(),
            &h.pipe,
            h.pipe.parameters(),
            true,
        )
        .await
        .map_err(|e| ApiError::exception("get_dtypes", e))?
    } else {
        types
    };
    Ok(ok_json(&serde_json::to_value(types).unwrap_or(Json::Null)))
}

async fn get_pipe_columns_indices(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let indices = conn
        .get_pipe_columns_indices(&h.pipe)
        .await
        .map_err(|e| ApiError::exception("get_pipe_columns_indices", e))?;
    Ok(ok_json(
        &serde_json::to_value(indices).unwrap_or(Json::Null),
    ))
}

async fn get_pipe_index_names(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let access = authorize_metadata(&ctx, &ck, &mk, &lk).await?;
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let names = match conn.get_pipe_index_names(&h.pipe).await {
        Ok(m) => m,
        // `hasattr(instance_connector, 'get_pipe_index_names')` is false: `{}`.
        Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => Map::new(),
        Err(e) => return Err(ApiError::exception("get_pipe_index_names", e)),
    };
    Ok(ok_json(&from_core(&Value::Map(names))))
}

async fn get_pipe_size(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let original = crate::executor::pipe_operation::ReadRequest::capture(&ctx, PIPES_READ).await?;
    let access = original.initial().clone();
    let (conn, h) = visible(&ctx, &access, &ck, &mk, &lk, false).await?;
    let query = DataQuery::default();
    let window = original.check(&ctx, &h.pipe, &query).await?;
    let size = match conn.get_pipe_size(&h.pipe).await {
        Ok(s) => s,
        Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => None,
        Err(e) => return Err(ApiError::exception("get_pipe_size", e)),
    };
    original.finish(&ctx, &h.pipe, &query, &window).await?;
    Ok(ok_json(&json!(size)))
}

async fn maintenance<F, Fut>(
    ctx: Ctx,
    access: Access,
    keys: (String, String, String),
    verb: &str,
    op: F,
) -> ApiResult
where
    F: FnOnce(Arc<dyn InstanceConnector>, Pipe) -> Fut,
    Fut: std::future::Future<Output = SuccessTuple>,
{
    let ik = ctx.instance_keys();
    let (conn, h) = editable(&ctx, &access, &keys.0, &keys.1, &keys.2, true).await?;
    refuse_protected(&h.pipe, verb)?;
    // Repartitioning changes the table (the `drop` lock).
    if verb == "repartition"
        && let Some(e) = access::guard::lock_conflict(&h.pipe, LockOp::Schema)
    {
        return Ok(tuple(false, e.detail_text()));
    }
    let res = mutation::run(
        &ctx,
        &conn,
        &h.pipe,
        &access,
        mutation::Operation::Maintenance {
            schema: verb == "repartition",
        },
        op(conn.clone(), h.pipe.clone()),
    )
    .await?;
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

async fn compress_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_WRITE).await?;
    maintenance(ctx, access, keys, "compress", |c, p| async move {
        c.compress_pipe(&p).await
    })
    .await
}

async fn decompress_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_WRITE).await?;
    let no_policy = ctx.query.bool("no_policy", false)?;
    maintenance(ctx, access, keys, "decompress", move |c, p| async move {
        c.decompress_pipe(&p, no_policy).await
    })
    .await
}

async fn vacuum_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_WRITE).await?;
    let full = ctx.query.bool("full", false)?;
    maintenance(ctx, access, keys, "vacuum", move |c, p| async move {
        c.vacuum_pipe(&p, full).await
    })
    .await
}

async fn analyze_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_WRITE).await?;
    maintenance(ctx, access, keys, "analyze", |c, p| async move {
        c.analyze_pipe(&p).await
    })
    .await
}

async fn partition_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    let access = authorize(&ctx, PIPES_WRITE).await?;
    let chunk_minutes = ctx.query.opt_int("chunk_minutes")?;
    maintenance(ctx, access, keys, "repartition", move |c, p| async move {
        c.partition_pipe(&p, chunk_minutes).await
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_body_forms() {
        assert!(parse_sync_body(b"[]").unwrap().is_none());
        assert!(parse_sync_body(b"").unwrap().is_none());
        assert!(matches!(
            parse_sync_body(b"{\"a\": [1]}").unwrap(),
            Some(SyncPayload::Json(_))
        ));
        match parse_sync_body(b"a:1,b:2\nc:3").unwrap() {
            Some(SyncPayload::Docs(d)) => assert_eq!(d.len(), 2),
            other => panic!("unexpected {other:?}"),
        }
        assert!(parse_sync_body(b"   ").is_err());
    }

    #[test]
    fn params_parsing() {
        assert!(parse_params(Some("null")).unwrap().is_empty());
        assert!(parse_params(Some("[1]")).is_err());
        assert!(parse_select_columns(Some("{}")).is_err());
        assert_eq!(parse_omit_columns(Some("{\"a\": 1}")).unwrap(), vec!["a"]);
        assert!(parse_omit_columns(Some("bad")).is_err());
    }
}
