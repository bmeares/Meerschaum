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

// The handlers in the submodules name these sibling route modules as `super::...`.
use super::{Ctx, body_bytes, engine, graph_batch, sync_status};
use crate::access::{self, Access, rules::LockOp};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{from_core, ok_json, python_json, raw_json, to_core, tuple, tuple_response};
use crate::pipe_methods::{self, SyncKwargs, SyncPayload};
use crate::serialize::{DataJsonOptions, DateUnit, bound_json, data_to_json, to_csv};
use crate::state::{AppState, PipeHandle, normalize_location, pipe_attributes};

mod catalog;
mod data;
mod maintenance;
pub(crate) mod mutation;
mod registration;
mod schema;
mod sync;

use catalog::{
    create_metadata, fetch_pipes_keys, get_pipe_attributes, get_pipe_by_keys, get_pipe_exists,
    get_pipe_id, get_pipes, get_pipes_by_connector, get_pipes_by_connector_and_metric,
};
use data::{get_pipe_chunk_bounds, get_pipe_csv, get_pipe_data, get_pipe_docs, get_pipe_rowcount};
use maintenance::{
    analyze_pipe, clear_pipe, compress_pipe, decompress_pipe, delete_pipe_cache, drop_pipe,
    partition_pipe, vacuum_pipe,
};
use registration::{delete_pipe, edit_pipe, register_pipe};
use schema::{
    get_pipe_columns_indices, get_pipe_columns_types, get_pipe_index_names, get_pipe_size,
};
pub(super) use sync::replacement_admission;
pub(crate) use sync::{check_data_destination, document_admission};
use sync::{get_sync_time, sync_pipe};

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
    // A pipe's access and locks (the v5 shape).
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

/// Authorize `scopes` (the caller's effective permissions) and return its access.
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

/// Administrators need no catalog index for access checks.
async fn visibility_index(
    ctx: &Ctx,
    access: &Access,
) -> Result<Option<Arc<access::PipeIndex>>, ApiError> {
    if access.is_admin() {
        return Ok(None);
    }
    access::pipe_index(&ctx.state, ctx.instance_keys().as_deref())
        .await
        .map(Some)
}

/// Check the pipe's own access and the instance's index of protected resources.
fn check_pipe_visibility(
    access: &Access,
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    index: Option<&access::PipeIndex>,
) -> Result<(), ApiError> {
    if !access.can_see_pipe(pipe) {
        return Err(access::hidden_pipe(pipe));
    }
    if let Some(index) = index {
        let indexed = index.get(
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        );
        if indexed.is_some_and(|p| !access.can_see_indexed(p))
            || access::guard::check_pipe_target(conn.as_ref(), pipe, &index.hidden_targets(access))
                .is_err()
        {
            return Err(access::hidden_pipe(pipe));
        }
    }
    Ok(())
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
    let index = visibility_index(ctx, access).await?;
    let (conn, h) = handle(ctx, ck, mk, lk, refresh).await?;
    check_pipe_visibility(access, &conn, &h.pipe, index.as_deref())?;
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

#[cfg(test)]
mod tests {
    use super::sync::parse_sync_body;
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
