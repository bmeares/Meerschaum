//! `/pipes/*` and `/metadata` (`api/routes/_pipes.py`).

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{delete, get, patch, post};
use chrono::TimeDelta;
use meerschaum_connector::{DataQuery, DateBound, InstanceConnector, PipesKeys, PipesKeysQuery};
use meerschaum_core::pipe::chunk::ChunkIntervalArg;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::serialize::GeometryFormat;
use serde_json::{Value as Json, json};

use super::{Ctx, body_bytes};
use crate::auth::scoped_auth;
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{from_core, ok_json, python_json, raw_json, to_core, tuple, tuple_response};
use crate::pipe_methods::{self, SyncKwargs, SyncPayload};
use crate::serialize::{DataJsonOptions, DateUnit, bound_json, data_to_json, to_csv};
use crate::state::{AppState, PipeHandle, normalize_location, pipe_attributes};

const PIPES_READ: &[&str] = &["pipes:read"];
const PIPES_WRITE: &[&str] = &["pipes:write"];
const PIPES_DELETE: &[&str] = &["pipes:delete"];
const PIPES_DROP: &[&str] = &["pipes:drop"];

/// Targets that may not be synced into or read from (`mrsm_tokens` is missing from the
/// `GET .../data` list).
const PROTECTED_DATA_READ: &[&str] = &["mrsm_users", "mrsm_plugins", "mrsm_pipes"];

type Keys3 = Path<(String, String, String)>;

/// Register the `pipes` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    let p = "/pipes/{connector_keys}/{metric_key}/{location_key}";
    app.route("/pipes", get(get_pipes))
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
        .route(meerschaum_api::routes::METADATA, post(create_metadata))
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

fn target(pipe: &Pipe) -> String {
    pipe.target(None).unwrap_or_else(|_| pipe.target_legacy())
}

fn refuse_protected(pipe: &Pipe, verb: &str) -> Result<(), ApiError> {
    let t = target(pipe);
    if meerschaum_api::routes::PROTECTED_TARGETS.contains(&t.as_str()) {
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

async fn register_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let ik = ctx.instance_keys();
    let body = parse_json_body(&body_bytes(body).await?)?;
    scoped_auth(&ctx.state, &ctx.headers, PIPES_WRITE).await?;
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
    let mut pipe = h.pipe.clone();
    if let Some(params) = parameters.filter(|p| !p.is_empty()) {
        pipe.set_parameters(params);
    }
    let res = conn.register_pipe(&pipe).await;
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

async fn edit_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let ik = ctx.instance_keys();
    let body = parse_json_body(&body_bytes(body).await?)?;
    scoped_auth(&ctx.state, &ctx.headers, PIPES_WRITE).await?;
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
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    if !ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(409, format!("{} is not registered.", h.pipe)));
    }
    let mut pipe = h.pipe.clone();
    pipe.set_parameters(parameters);
    let res = conn.edit_pipe(&pipe, patch).await;
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

async fn delete_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    let ik = ctx.instance_keys();
    scoped_auth(&ctx.state, &ctx.headers, PIPES_DELETE).await?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    if !ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(409, format!("{} is not registered.", h.pipe)));
    }
    let res = conn.delete_pipe(&h.pipe).await;
    ctx.state.pipes(ik.as_deref(), true).await?;
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

async fn fetch_pipes_keys(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
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
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
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
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let ik = ctx.instance_keys();
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let Some(metrics) = dict.get(&ck) else {
        return Err(ApiError::new(404, format!("Connector '{ck}' not found.")));
    };
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    let mut out = serde_json::Map::new();
    for (mk, locations) in metrics {
        out.insert(
            mk.clone(),
            Json::Object(attributes_tree(&conn, locations).await?),
        );
    }
    Ok(ok_json(&Json::Object(out)))
}

async fn get_pipes_by_connector_and_metric(
    ctx: Ctx,
    Path((ck, mk)): Path<(String, String)>,
) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let ik = ctx.instance_keys();
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let Some(metrics) = dict.get(&ck) else {
        return Err(ApiError::new(404, format!("Connector '{ck}' not found.")));
    };
    let Some(locations) = metrics.get(&mk) else {
        return Err(ApiError::new(404, format!("Metric '{mk}' not found.")));
    };
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    Ok(ok_json(&Json::Object(
        attributes_tree(&conn, locations).await?,
    )))
}

async fn get_pipe_by_keys(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let ik = ctx.instance_keys();
    let dict = ctx.state.pipes(ik.as_deref(), true).await?;
    let Some(metrics) = dict.get(&ck) else {
        return Err(ApiError::new(404, format!("Connector '{ck}' not found.")));
    };
    let Some(locations) = metrics.get(&mk) else {
        return Err(ApiError::new(404, format!("Metric '{mk}' not found.")));
    };
    let loc = normalize_location(Some(&lk));
    let Some(h) = locations.get(&loc) else {
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
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
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
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, true).await?;
    let attrs = pipe_attributes(&conn, &h.pipe).await?;
    Ok(ok_json(&from_core(&Value::Map(attrs))))
}

async fn get_pipe_exists(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    let exists = conn
        .pipe_exists(&h.pipe)
        .await
        .map_err(|e| ApiError::exception("pipe_exists", e))?;
    Ok(ok_json(&Json::Bool(exists)))
}

async fn create_metadata(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["actions:execute"]).await?;
    let conn = ctx
        .state
        .get_api_connector(ctx.instance_keys().as_deref())?;
    // `get_tables()` only applies to SQL and API instances; other instances answer `false`.
    // TODO(connector-trait): no `create_metadata()` / `get_tables()` on `InstanceConnector`, so
    // the internal tables are not created here; SQL instances create them on first use.
    let ok = matches!(conn.connector_type(), "sql" | "api");
    Ok(ok_json(&Json::Bool(ok)))
}

// ------------------------------------------------------------------------------------------------
// Data
// ------------------------------------------------------------------------------------------------

async fn get_sync_time(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let body = parse_json_body(&body_bytes(body).await?)?;
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let params = optional_dict_body(body)?;
    let newest = ctx.query.bool("newest", true)?;
    let _remote = ctx.query.bool("remote", false)?;
    let round_down = ctx.query.bool("round_down", true)?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    let st = pipe_methods::get_sync_time(
        &conn,
        &h.pipe,
        params.as_ref(),
        newest,
        round_down,
        &ctx.state.config.pipe_defaults,
    )
    .await?;
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

async fn sync_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_WRITE).await?;
    let q = &ctx.query;
    let check_existing = q.bool("check_existing", true)?;
    let blocking = q.bool("blocking", true)?;
    let force = q.bool("force", false)?;
    let workers = q.opt_int("workers")?;
    let columns = q.opt_str("columns");
    let body = body_bytes(body).await?;
    let Some(payload) = parse_sync_body(&body)? else {
        return Ok(tuple(true, "No data to sync."));
    };
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, true).await?;
    let t = target(&h.pipe);
    if meerschaum_api::routes::PROTECTED_TARGETS.contains(&t.as_str()) {
        return Err(ApiError::new(
            409,
            format!("Cannot sync data to protected table '{t}'."),
        ));
    }
    let mut pipe = h.pipe.clone();
    if pipe.columns().is_empty() {
        if let Some(c) = columns {
            let cols = Value::from_json_str(&c)
                .map_err(|e| ApiError::exception("json.loads(columns)", e))?;
            pipe.set_columns(cols)
                .map_err(|e| ApiError::exception("pipe.columns", e))?;
        }
    }
    let kw = SyncKwargs {
        check_existing,
        force,
        workers,
    };
    let defaults = ctx.state.config.pipe_defaults.clone();
    if !blocking {
        let msg = format!("Spawned asyncronous sync for {pipe}.");
        tokio::spawn(async move {
            let res = pipe_methods::sync(&conn, &pipe, &payload, &kw, &defaults).await;
            if !res.success {
                tracing::warn!("Asynchronous sync failed for {pipe}:\n{}", res.message);
            }
        });
        // PARITY: the typo `asyncronous` is v4's (`core/Pipe/_sync.py:539`).
        return Ok(tuple(true, msg));
    }
    let res = pipe_methods::sync(&conn, &pipe, &payload, &kw, &defaults).await;
    h.invalidate();
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
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let q = &ctx.query;
    let date_format = q.str_or("date_format", "iso");
    let date_unit = q.str_or("date_unit", "us");
    let double_precision = q.int("double_precision", 15)?;
    let geometry_format = q.str_or("geometry_format", "wkb_hex");
    let args = data_args(&ctx)?;
    let ik = ctx.instance_keys();
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    if !ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(
            409,
            "Pipe must be registered with the datetime column specified.",
        ));
    }
    let t = target(&h.pipe);
    if PROTECTED_DATA_READ.contains(&t.as_str()) {
        return Err(ApiError::new(
            409,
            format!("Cannot retrieve data from protected table '{t}'."),
        ));
    }
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
    Ok(raw_json(StatusCode::OK, data_to_json(&df, &opts)))
}

async fn get_pipe_docs(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let args = data_args(&ctx)?;
    let q = &ctx.query;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
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
    let docs = meerschaum_connector::pipe_ops::get_docs(conn.as_ref(), &h.pipe, &query)
        .await
        .map_err(|e| ApiError::exception("get_pipe_docs", e))?;
    let value = Value::List(
        docs.iter()
            .map(|d| {
                Value::Map(
                    d.iter()
                        .map(|(k, v)| {
                            (k.clone(), meerschaum_connector::pipe_ops::kernel_to_core(v))
                        })
                        .collect(),
                )
            })
            .collect(),
    );
    Ok(raw_json(StatusCode::OK, python_json(&value)))
}

fn refuse_protected_read(pipe: &Pipe) -> Result<(), ApiError> {
    let t = target(pipe);
    if meerschaum_api::routes::PROTECTED_TARGETS.contains(&t.as_str()) {
        return Err(ApiError::new(
            409,
            format!("Cannot retrieve data from protected table '{t}'."),
        ));
    }
    Ok(())
}

async fn get_pipe_chunk_bounds(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let q = &ctx.query;
    let bounded = q.bool("bounded", true)?;
    let chunk_interval_minutes = q.opt_int("chunk_interval_minutes")?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
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
    scoped_auth(&ctx.state, &ctx.headers, PIPES_DROP).await?;
    let ik = ctx.instance_keys();
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    let res = conn.drop_pipe(&h.pipe).await;
    h.invalidate();
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

async fn clear_pipe(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_DELETE).await?;
    let q = &ctx.query;
    let params = parse_params(q.get("params"))?;
    let ik = ctx.instance_keys();
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    let res = conn
        .clear_pipe(
            &h.pipe,
            &DataQuery {
                begin,
                end,
                params: Some(params),
                ..Default::default()
            },
        )
        .await;
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

async fn delete_pipe_cache(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let (_, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    // `pipe._invalidate_cache(hard=True)` on the shared object.
    h.invalidate();
    Ok(tuple_response(&SuccessTuple::success()))
}

async fn get_pipe_csv(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let q = &ctx.query;
    let params = parse_params(q.get("params"))?;
    let ik = ctx.instance_keys();
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    if !ctx.state.is_registered(&h.pipe, ik.as_deref()).await? {
        return Err(ApiError::new(409, "Pipe must be registered."));
    }
    let defaults = &ctx.state.config.pipe_defaults;
    let mut begin =
        pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let mut end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    if h.pipe.column("datetime").is_some() {
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
    let df = meerschaum_connector::pipe_ops::get_data(
        conn.as_ref(),
        &h.pipe,
        &DataQuery {
            begin,
            end,
            params: Some(params),
            ..Default::default()
        },
    )
    .await
    .map_err(|e| ApiError::exception("get_pipe_data", e))?;
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

async fn get_pipe_rowcount(ctx: Ctx, Path((ck, mk, lk)): Keys3, body: Body) -> ApiResult {
    let body = parse_json_body(&body_bytes(body).await?)?;
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let params = optional_dict_body(body)?;
    let remote = ctx.query.bool("remote", false)?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    let defaults = &ctx.state.config.pipe_defaults;
    let q = &ctx.query;
    let begin = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("begin"), defaults).await?;
    let end = pipe_methods::parse_date_bound(&conn, &h.pipe, q.get("end"), defaults).await?;
    let n = pipe_methods::get_rowcount(&conn, &h.pipe, begin, end, params, remote).await;
    Ok(ok_json(&json!(n)))
}

async fn get_pipe_columns_types(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
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
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    let indices = conn
        .get_pipe_columns_indices(&h.pipe)
        .await
        .map_err(|e| ApiError::exception("get_pipe_columns_indices", e))?;
    Ok(ok_json(
        &serde_json::to_value(indices).unwrap_or(Json::Null),
    ))
}

async fn get_pipe_index_names(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    let names = match conn.get_pipe_index_names(&h.pipe).await {
        Ok(m) => m,
        // `hasattr(instance_connector, 'get_pipe_index_names')` is false: `{}`.
        Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => Map::new(),
        Err(e) => return Err(ApiError::exception("get_pipe_index_names", e)),
    };
    Ok(ok_json(&from_core(&Value::Map(names))))
}

async fn get_pipe_size(ctx: Ctx, Path((ck, mk, lk)): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_READ).await?;
    let (conn, h) = handle(&ctx, &ck, &mk, &lk, false).await?;
    let size = match conn.get_pipe_size(&h.pipe).await {
        Ok(s) => s,
        Err(meerschaum_connector::ConnectorError::NotImplemented(_)) => None,
        Err(e) => return Err(ApiError::exception("get_pipe_size", e)),
    };
    Ok(ok_json(&json!(size)))
}

async fn maintenance<F, Fut>(
    ctx: Ctx,
    keys: (String, String, String),
    verb: &str,
    op: F,
) -> ApiResult
where
    F: FnOnce(Arc<dyn InstanceConnector>, Pipe) -> Fut,
    Fut: std::future::Future<Output = SuccessTuple>,
{
    let ik = ctx.instance_keys();
    let (conn, h) = handle(&ctx, &keys.0, &keys.1, &keys.2, false).await?;
    refuse_protected(&h.pipe, verb)?;
    let res = op(conn, h.pipe.clone()).await;
    ctx.state.pipes(ik.as_deref(), true).await?;
    Ok(tuple_response(&res))
}

async fn compress_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_WRITE).await?;
    maintenance(ctx, keys, "compress", |c, p| async move {
        c.compress_pipe(&p).await
    })
    .await
}

async fn decompress_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_WRITE).await?;
    let no_policy = ctx.query.bool("no_policy", false)?;
    maintenance(ctx, keys, "decompress", move |c, p| async move {
        c.decompress_pipe(&p, no_policy).await
    })
    .await
}

async fn vacuum_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_WRITE).await?;
    let full = ctx.query.bool("full", false)?;
    maintenance(ctx, keys, "vacuum", move |c, p| async move {
        c.vacuum_pipe(&p, full).await
    })
    .await
}

async fn analyze_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_WRITE).await?;
    maintenance(ctx, keys, "analyze", |c, p| async move {
        c.analyze_pipe(&p).await
    })
    .await
}

async fn partition_pipe(ctx: Ctx, Path(keys): Keys3) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, PIPES_WRITE).await?;
    let chunk_minutes = ctx.query.opt_int("chunk_minutes")?;
    maintenance(ctx, keys, "repartition", move |c, p| async move {
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
