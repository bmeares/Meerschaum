//! The `/engine/*` route family (§5.8, new in v5): engine state for API instances, backed by an
//! [`EngineBackend`](crate::backends::EngineBackend) (`start api`: the server's instance,
//! [`crate::engine_store::InstanceEngine`]).
//!
//! Every route answers in the v5 shape (`{"success": bool, "message": str, ...}` with real
//! status codes, [`crate::envelope`]). Reads (`GET`, and `POST .../read`) need `pipes:read`,
//! writes `pipes:write`.
//!
//! **Access (rc15).** A caller who is not an administrator sees the runs of the pipes it can see
//! (a run is visible when one of its pipes is; the nodes of hidden pipes are anonymized,
//! [`crate::access::runs`]), the ledger and state of pipes it can see, and the raw state rows of
//! those pipes only; writing engine state (ledger entries, cursors, states, runs, events,
//! leases, raw rows) for a pipe needs edit rights on it.
//!
//! `/engine/pipes/{metric}/{read,write,clear,exists}` expose the engine's six state pipes
//! (`mrsm:engine_*`) to an `APIConnector`, whose engine keeps its state through them; any other
//! metric is a `404` (no generic access to `mrsm` keys). Rows travel as `orient='records'` JSON
//! under `rows`.
//!
//! `GET /engine/runs` lists runs with filters (`pipeline`, `pipe`, `status`, `since`, `limit`,
//! `run_id`, `instance_keys`), `GET /engine/runs/{run_id}` returns one run and its nodes, and
//! `GET /engine/runs/{run_id}/events/stream` follows a run's events as Server-Sent Events
//! ([`crate::engine_stream`]).

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::routing::{get, post};
use meerschaum_connector_api::engine_pipes::{self, EnginePipeQuery};
use meerschaum_connector_api::models::{
    EdgeCursor, EngineEvent, EngineState, LeaseRequest, LedgerEntry, RunRecord,
};
use serde::de::DeserializeOwned;
use serde_json::{Map as JsonMap, Value as Json, json};

use super::Ctx;
use crate::access::{self, Access, RefFilter};
use crate::backends::{EngineStoreError, RunStatusFilter, RunsFilter};
use crate::config::ServerConfig;
use crate::envelope;
use crate::error::{ApiError, ApiResult};
use crate::routes::pipes::parse_json_body;
use crate::state::AppState;

/// The scopes of the write routes.
const SCOPES: &[&str] = &["pipes:write"];

/// The scopes of the read routes.
const READ_SCOPES: &[&str] = &["pipes:read"];

/// The most runs one `GET /engine/runs` returns.
pub const MAX_RUNS: usize = 1000;

/// The runs `GET /engine/runs` returns without `limit`.
pub const DEFAULT_RUNS: usize = 100;

/// Register the `engine` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/engine/ledger", post(post_ledger).get(get_ledger))
        .route("/engine/cursors", axum::routing::put(put_cursors))
        .route("/engine/state", get(get_state).put(put_state))
        .route("/engine/runs", post(post_runs).get(get_runs))
        .route("/engine/runs/{run_id}", get(get_run))
        .route("/engine/run-generations", post(run_generations))
        .route(
            "/engine/runs/{run_id}/events/stream",
            get(stream_run_events),
        )
        .route("/engine/events", post(post_events))
        .route("/engine/leases", post(acquire_lease).delete(release_lease))
        // The bodies are read by `body_json` with `ENGINE_BODY_LIMIT` (raw-body handlers are not
        // covered by `DefaultBodyLimit`).
        .route("/engine/pipes/{metric}/read", post(read_pipe))
        .route("/engine/pipes/{metric}/write", post(write_pipe))
        .route("/engine/pipes/{metric}/clear", post(clear_pipe))
        .route("/engine/pipes/{metric}/exists", get(pipe_exists))
}

/// Authorize, then refuse anything but the six engine state pipes.
async fn engine_metric(ctx: &Ctx, metric: &str, scopes: &[&str]) -> Result<Access, ApiError> {
    let caller = engine_access(ctx, scopes).await?;
    if !engine_pipes::is_engine_metric(metric) {
        return Err(ApiError::new(
            404,
            format!("'{metric}' is not an engine state pipe."),
        ));
    }
    Ok(caller)
}

async fn engine_access(ctx: &Ctx, scopes: &[&str]) -> Result<Access, ApiError> {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, scopes).await?;
    access::guard::check_connector_teams(
        &ctx.state,
        &caller,
        &ctx.state.instance().connector_keys(),
    )?;
    Ok(caller)
}

/// Private native-host authorization evidence, without transferring stable registration IDs.
async fn run_generations(ctx: Ctx, body: Body) -> ApiResult {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, READ_SCOPES).await?;
    let value = body_json(body).await?;
    let ids: Vec<String> =
        serde_json::from_value(value.get("run_ids").cloned().unwrap_or(Json::Null))
            .map_err(|_| ApiError::new(400, "run_ids must be an array of strings."))?;
    if ids.len() > 256
        || ids
            .iter()
            .any(|id| id.is_empty() || id.len() > 512 || id.chars().any(char::is_control))
    {
        return Err(ApiError::new(400, "Supply at most 256 valid run IDs."));
    }
    let ik = ctx.instance_keys();
    let conn = ctx.state.get_api_connector(ik.as_deref())?;
    access::guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let mut result: std::collections::HashMap<String, bool> =
        ids.iter().cloned().map(|id| (id, false)).collect();
    if !ids.is_empty() {
        let params = [(
            "run_id".into(),
            meerschaum_core::Value::List(
                ids.into_iter().map(meerschaum_core::Value::Str).collect(),
            ),
        )]
        .into_iter()
        .collect();
        if let Some(pipe) = conn
            .engine_state_pipe(&conn.connector_keys(), "engine_runs")
            .await
            .map_err(|_| ApiError::new(503, "Cannot validate run history."))?
            && let Some(frame) = conn
                .get_pipe_data(
                    &pipe,
                    &meerschaum_connector::DataQuery {
                        params: Some(params),
                        limit: Some(256),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| ApiError::new(503, "Cannot validate run history."))?
        {
            let rows: Vec<Json> =
                serde_json::from_str(&engine_pipes::records_json(&frame)).unwrap_or_default();
            let records = access::runs::headers_from_rows(&rows);
            let mut filter = RefFilter::new(&ctx.state, &caller, &conn.connector_keys())
                .for_engine()
                .requiring_generations();
            filter.prime_run_generations(&records).await;
            // Validate raw stored snapshot proof before public projection strips it. A host's
            // admin transport must not authorize a recreated or name-only legacy Pipeline.
            let mut current_snapshots = std::collections::HashSet::new();
            for record in &records {
                if access::runs::record_pipelines_visible(&mut filter, record).await {
                    current_snapshots.insert(record.run_id.clone());
                }
            }
            let visible = access::runs::visible_runs(&mut filter, records).await;
            for record in visible {
                if let Some(proven) = result.get_mut(&record.run_id) {
                    *proven = filter.run_generation_current(&record.run_id)
                        && current_snapshots.contains(&record.run_id);
                }
            }
        }
    }
    Ok(envelope::ok_with(
        "Checked requested run histories.",
        "generations",
        serde_json::to_value(result).unwrap_or(Json::Null),
    ))
}

/// The filter of refs for `caller` on the server's instance (the engine pipes' and the `api:`
/// clients' refs name the server's pipes by their last three keys).
fn filter_for(ctx: &Ctx, caller: &Access) -> RefFilter {
    RefFilter::new(&ctx.state, caller, &ctx.state.instance().connector_keys()).for_engine()
}

fn refused(message: String) -> ApiError {
    ApiError::new(403, message)
}

fn store_error(e: EngineStoreError) -> ApiError {
    crate::engine_stream::api_error(e)
}

/// The largest `/engine/*` request body (64 MiB). The engine's biggest batch is one run's
/// node rows (about 1 KB per node) or a flush of buffered events, a few MB even for thousands of
/// nodes; the cap leaves an order of magnitude of headroom and bounds what an authorized client
/// can make the server buffer.
pub const ENGINE_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// The body as JSON (`null` for an empty body), read up to [`ENGINE_BODY_LIMIT`] (`413` beyond).
/// Callers authorize first.
async fn body_json(body: Body) -> Result<Json, ApiError> {
    let bytes = axum::body::to_bytes(body, ENGINE_BODY_LIMIT)
        .await
        .map_err(|_| {
            ApiError::new(
                413,
                format!(
                    "The request body exceeds the engine routes' limit of {} MiB.",
                    ENGINE_BODY_LIMIT >> 20
                ),
            )
        })?;
    Ok(parse_json_body(&bytes)?.unwrap_or(Json::Null))
}

fn query_from(j: Json) -> Result<EnginePipeQuery, ApiError> {
    if j.is_null() {
        return Ok(EnginePipeQuery::default());
    }
    let query: EnginePipeQuery = serde_json::from_value(j.clone()).map_err(|e| {
        ApiError::validation(vec![ApiError::field_error(
            "model_type",
            &["body"],
            &e.to_string(),
            j,
            None,
        )])
    })?;
    if query.limit.is_some_and(|limit| limit <= 0)
        || query.order.as_ref().is_some_and(|order| {
            !order.eq_ignore_ascii_case("asc") && !order.eq_ignore_ascii_case("desc")
        })
    {
        return Err(ApiError::new(
            400,
            "Engine read limit must be positive and order must be 'asc' or 'desc'.",
        ));
    }
    Ok(query)
}

/// `"1 row"` / `"2 rows"`.
fn count(n: usize, singular: &str, plural: &str) -> String {
    format!("{n} {}", if n == 1 { singular } else { plural })
}

async fn read_pipe(ctx: Ctx, Path(metric): Path<String>, body: Body) -> ApiResult {
    let caller = engine_metric(&ctx, &metric, READ_SCOPES).await?;
    let mut query = query_from(body_json(body).await?)?;
    // Keep the access columns until the rows have been filtered. Otherwise selecting only
    // `message`, for example, removes `pipe_ref` before authorization sees it.
    let requested_columns = if caller.is_admin() {
        None
    } else {
        query.select_columns.take()
    };
    // A filtered short page must mean the visible history is exhausted. Keep the
    // existing server-side ACL scan for restricted readers; limit its visible prefix.
    let requested_limit = if caller.is_admin() {
        None
    } else {
        query.limit.take()
    };
    let df = ctx
        .state
        .engine
        .read_pipe(&metric, &query)
        .await
        .map_err(store_error)?;
    Ok(match df {
        Some(df) if caller.is_admin() => {
            let message = format!("Read {}.", count(df.height(), "row", "rows"));
            envelope::ok_with_raw(&message, "rows", &engine_pipes::records_json(&df))
        }
        Some(df) => {
            // Only the rows of pipes the caller can see.
            let rows: Vec<Json> =
                serde_json::from_str(&engine_pipes::records_json(&df)).unwrap_or_default();
            let mut rows =
                access::runs::visible_rows(&mut filter_for(&ctx, &caller), &metric, rows).await;
            if let Some(columns) = requested_columns {
                for row in &mut rows {
                    if let Some(fields) = row.as_object_mut() {
                        fields.retain(|key, _| columns.contains(key));
                    }
                }
            }
            if let Some(limit) = requested_limit {
                rows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
            }
            let message = format!("Read {}.", count(rows.len(), "row", "rows"));
            envelope::ok_with(message, "rows", Json::Array(rows))
        }
        None => envelope::ok_with("Read 0 rows.", "rows", json!([])),
    })
}

async fn write_pipe(ctx: Ctx, Path(metric): Path<String>, body: Body) -> ApiResult {
    let caller = engine_metric(&ctx, &metric, SCOPES).await?;
    let j = body_json(body).await?;
    let Some(items) = j.as_array() else {
        return Err(ApiError::missing(&["body"]));
    };
    let mut docs = engine_pipes::docs_from_records(items);
    if metric == "engine_nodes" {
        canonicalize_node_registrations(&ctx, &mut docs).await?;
    }
    if metric == "engine_runs" {
        let mut selections: Vec<Json> = docs
            .iter()
            .map(|doc| match doc.get("selection") {
                Some(meerschaum_kernel::value::Value::Json(value)) => value.clone(),
                Some(meerschaum_kernel::value::Value::Str(text)) => {
                    serde_json::from_str(text).unwrap_or(Json::Null)
                }
                _ => Json::Null,
            })
            .collect();
        canonicalize_pipeline_registries(&ctx, &mut selections).await?;
        for (doc, selection) in docs.iter_mut().zip(selections) {
            if !selection.is_null() {
                doc.insert(
                    "selection".into(),
                    meerschaum_kernel::value::Value::Str(selection.to_string()),
                );
            }
        }
    }
    access::runs::check_rows_editable(&mut filter_for(&ctx, &caller), &docs)
        .await
        .map_err(refused)?;
    check_run_ids_editable(
        &ctx,
        &caller,
        &items
            .iter()
            .filter_map(|r| r.get("run_id").and_then(Json::as_str))
            .map(str::to_string)
            .collect::<Vec<_>>(),
    )
    .await?;
    // An engine on an `api:` instance flushes its events here: its runs' streams get them live.
    let events = (metric == "engine_events").then(|| docs.clone());
    let res = ctx
        .state
        .engine
        .write_pipe(&metric, docs)
        .await
        .map_err(store_error)?;
    if let Some(docs) = events
        && res.success
    {
        // `/engine/pipes/*` keep their rows on the server's own instance.
        let instance = ctx.state.instance().connector_keys();
        ctx.state.engine_streams.publish_docs(&instance, &docs);
    }
    envelope::tuple(&res, 500)
}

/// Keep the caller's original ref while proving which exact API-local registrations it
/// captured. Unknown legacy or foreign identities remain unproven to the metrics reader.
async fn canonicalize_node_registrations(
    ctx: &Ctx,
    docs: &mut [meerschaum_connector::pipe_ops::Doc],
) -> Result<(), ApiError> {
    use meerschaum_kernel::value::Value as KValue;
    for doc in docs.iter_mut() {
        // A submitted canonical override is never authoritative.
        doc.shift_remove("registration_ref");
    }
    if docs.is_empty() {
        return Ok(());
    }
    let Some(alias) = client_instance_alias(ctx)? else {
        return Ok(());
    };
    let fresh = if ctx.state.config.no_auth {
        Access::everything()
    } else {
        let principal = crate::auth::load_user_or_token(&ctx.state, &ctx.headers).await?;
        let granted = crate::auth::granted_scopes(&ctx.state, &ctx.headers, &principal).await?;
        access::access_of_fresh(&ctx.state, principal, &granted).await?
    };
    fresh.require(SCOPES, &[])?;
    let conn = ctx.state.instance();
    access::guard::check_connector_teams(&ctx.state, &fresh, &conn.connector_keys())?;
    let registered = ctx.state.pipes(None, true).await?;
    for doc in docs.iter_mut() {
        let Some(KValue::Str(reference)) = doc.get("pipe_ref") else {
            continue;
        };
        if reference.split('|').next() != Some(alias) {
            continue;
        }
        let Some(KValue::Str(captured)) = doc.get("registration_id") else {
            continue;
        };
        let Some((connector, metric, location)) =
            meerschaum_connector::sync_metrics::reference_keys(reference)
        else {
            continue;
        };
        access::guard::check_connector_teams(&ctx.state, &fresh, connector)?;
        let handle = registered
            .get(connector)
            .and_then(|metrics| metrics.get(metric))
            .and_then(|locations| locations.get(&location.map(str::to_string)))
            .ok_or_else(|| ApiError::new(404, "Pipe was not found."))?;
        if !fresh.can_see_pipe(&handle.pipe) {
            return Err(ApiError::new(404, "Pipe was not found."));
        }
        if !fresh.can_edit(handle.pipe.parameters()) {
            return Err(access::not_editable(&handle.pipe));
        }
        if meerschaum_connector::sync_metrics::observed_registration(&handle.pipe).as_ref()
            != Some(captured)
        {
            return Err(ApiError::new(
                409,
                "The Pipe registration changed since capture.",
            ));
        }
        doc.insert(
            "registration_ref".into(),
            KValue::Str(handle.pipe.pipe_ref()),
        );
    }
    Ok(())
}

fn client_instance_alias(ctx: &Ctx) -> Result<Option<&str>, ApiError> {
    let Some(alias) = ctx.headers.get(engine_pipes::CLIENT_INSTANCE_HEADER) else {
        return Ok(None);
    };
    let alias = alias
        .to_str()
        .map_err(|_| ApiError::new(400, "Invalid engine client instance."))?;
    if !alias.starts_with("api:") || alias.len() <= 4 || alias.contains('|') {
        return Err(ApiError::new(400, "Invalid engine client instance."));
    }
    Ok(Some(alias))
}

/// Preserve the original snapshot registry while proving an exact API-local alias.
/// A client cannot relabel a foreign or stale Pipeline by submitting its own proof field.
async fn canonicalize_pipeline_registries(
    ctx: &Ctx,
    selections: &mut [Json],
) -> Result<(), ApiError> {
    for selection in selections.iter_mut() {
        if let Some(snapshots) = selection
            .get_mut("pipeline_definitions")
            .and_then(Json::as_array_mut)
        {
            for snapshot in snapshots {
                if let Some(snapshot) = snapshot.as_object_mut() {
                    snapshot.remove("registration_registry");
                }
            }
        }
    }
    let Some(alias) = client_instance_alias(ctx)? else {
        return Ok(());
    };
    let has_local = selections.iter().any(|selection| {
        selection
            .get("pipeline_definitions")
            .and_then(Json::as_array)
            .is_some_and(|snapshots| {
                snapshots
                    .iter()
                    .any(|snapshot| snapshot.get("registry").and_then(Json::as_str) == Some(alias))
            })
    });
    if !has_local {
        return Ok(());
    }
    let caller = access::require_fresh(&ctx.state, &ctx.headers, SCOPES).await?;
    let conn = ctx.state.instance();
    access::guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let saved = conn
        .registered_pipes(&meerschaum_connector::PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await
        .map_err(|_| ApiError::new(503, "Cannot read saved Pipeline access."))?;
    let mut current = std::collections::HashMap::new();
    for (_, entry) in saved {
        let parameters = match entry.extra {
            Some(meerschaum_core::Value::Map(parameters)) => parameters,
            Some(meerschaum_core::Value::Str(text)) => {
                match meerschaum_core::Value::from_json_str(&text) {
                    Ok(meerschaum_core::Value::Map(parameters)) => parameters,
                    _ => continue,
                }
            }
            _ => continue,
        };
        if caller.can_see(&parameters)
            && let Some(name) = entry.location_key
            && let Some(identity) = parameters
                .get("pipeline")
                .and_then(|definition| definition.get("id"))
                .and_then(meerschaum_core::Value::as_str)
        {
            current.insert(name, identity.to_string());
        }
    }
    for selection in selections {
        let Some(snapshots) = selection
            .get_mut("pipeline_definitions")
            .and_then(Json::as_array_mut)
        else {
            continue;
        };
        for snapshot in snapshots {
            if snapshot.get("registry").and_then(Json::as_str) != Some(alias) {
                continue;
            }
            let name = snapshot
                .get("name")
                .and_then(Json::as_str)
                .ok_or_else(|| ApiError::new(400, "Invalid saved Pipeline snapshot."))?;
            let Some(identity) = current.get(name) else {
                return Err(ApiError::new(404, "Saved Pipeline was not found."));
            };
            if snapshot.get("id").and_then(Json::as_str) != Some(identity.as_str()) {
                return Err(ApiError::new(
                    409,
                    "The saved Pipeline changed since capture.",
                ));
            }
            snapshot
                .as_object_mut()
                .expect("snapshot has string fields")
                .insert(
                    "registration_registry".into(),
                    Json::String(conn.connector_keys()),
                );
        }
    }
    Ok(())
}

/// `GET /engine/runs/{run_id}/events/stream` ([`crate::engine_stream`]).
async fn stream_run_events(ctx: Ctx, Path(run_id): Path<String>) -> ApiResult {
    crate::engine_stream::serve(ctx, run_id).await
}

async fn clear_pipe(ctx: Ctx, Path(metric): Path<String>, body: Body) -> ApiResult {
    let caller = engine_metric(&ctx, &metric, SCOPES).await?;
    let query = query_from(body_json(body).await?)?;
    bounded_clear(&metric, &query)?;
    if !caller.is_admin() {
        // A caller who is not an administrator clears the rows of pipes it may change (or of
        // its runs): never the history of every pipe.
        let params = query.params_map().unwrap_or_default();
        let refs: Vec<String> = ["pipe_ref", "child_ref"]
            .iter()
            .filter_map(|k| params.get(*k))
            .flat_map(|v| match v {
                meerschaum_core::Value::List(items) => {
                    items.iter().map(meerschaum_core::Value::py_str).collect()
                }
                other => vec![other.py_str()],
            })
            .collect();
        if refs.is_empty() && !params.contains_key("run_id") {
            return Err(refused(format!(
                "Only administrators may clear '{metric}' by time."
            )));
        }
        check_editable(&ctx, &caller, refs).await?;
        if let Some(run_ids) = params.get("run_id") {
            let ids: Vec<String> = match run_ids {
                meerschaum_core::Value::List(items) => {
                    items.iter().map(meerschaum_core::Value::py_str).collect()
                }
                other => vec![other.py_str()],
            };
            check_run_ids_editable(&ctx, &caller, &ids).await?;
        }
    }
    let res = ctx
        .state
        .engine
        .clear_pipe(&metric, &query)
        .await
        .map_err(store_error)?;
    envelope::tuple(&res, 500)
}

/// The state pipes whose rows age out by time: an `end`-only clear is their retention
/// (`InstanceState::prune_history`). Each has a datetime column for `end` to bound.
const RETENTION_METRICS: &[&str] = &["engine_runs", "engine_nodes", "engine_events"];

/// Whether a `params` value selects rows positively: a scalar (not `null`, `"None"` or a
/// `_`-prefixed negation) or a non-empty list of them. The engine only ever sends pipe refs,
/// run IDs and sequence numbers.
fn positive_selector(v: &meerschaum_core::Value) -> bool {
    use meerschaum_core::Value;
    let scalar = |v: &Value| match v {
        Value::Str(s) => !s.is_empty() && !s.starts_with('_') && s != "None",
        Value::Int(_) | Value::Float(_) | Value::Bool(_) => true,
        _ => false,
    };
    match v {
        Value::List(items) => !items.is_empty() && items.iter().all(scalar),
        other => scalar(other),
    }
}

/// Never wipe a state table: a clear must name its rows with `params` over the metric's own
/// columns (a pipe's ledger, cursors or hash; an unknown column would be dropped by the
/// connector and the `WHERE` would match everything), or, for the retention metrics only,
/// bound them with `end`.
fn bounded_clear(metric: &str, query: &EnginePipeQuery) -> Result<(), ApiError> {
    if query.limit.is_some() || query.order.is_some() {
        return Err(ApiError::new(
            400,
            "limit and order apply to engine reads only.",
        ));
    }
    let refuse = |why: &str| {
        Err(ApiError::new(
            400,
            format!("Refusing to clear '{metric}': {why}"),
        ))
    };
    let columns = engine_pipes::engine_dtypes_context(metric)
        .map(|c| c.dtypes)
        .unwrap_or_default();
    match query.params_map().filter(|p| !p.is_empty()) {
        Some(params) => {
            if let Some(k) = params.keys().find(|k| !columns.contains_key(*k)) {
                return refuse(&format!("`{k}` is not a column of this state pipe."));
            }
            if let Some(k) = params
                .iter()
                .find(|(_, v)| !positive_selector(v))
                .map(|(k, _)| k)
            {
                return refuse(&format!(
                    "`{k}` must select rows by value (no null, `None` or `_` negation)."
                ));
            }
            Ok(())
        }
        None if query.end.is_some() && RETENTION_METRICS.contains(&metric) => Ok(()),
        None if query.end.is_some() => {
            refuse("an `end`-only clear applies to runs, nodes and events only.")
        }
        None => refuse("`params` (or, for runs, nodes and events, `end`) is required."),
    }
}

async fn pipe_exists(ctx: Ctx, Path(metric): Path<String>) -> ApiResult {
    engine_metric(&ctx, &metric, READ_SCOPES).await?;
    let exists = ctx
        .state
        .engine
        .pipe_exists(&metric)
        .await
        .map_err(store_error)?;
    let message = if exists {
        format!("The table of '{metric}' exists.")
    } else {
        format!("The table of '{metric}' does not exist.")
    };
    Ok(envelope::ok_with(message, "exists", json!(exists)))
}

async fn body_as<T: DeserializeOwned>(ctx: &Ctx, body: Body) -> Result<(Access, T), ApiError> {
    let caller = engine_access(ctx, SCOPES).await?;
    let j = body_json(body).await?;
    if j.is_null() {
        return Err(ApiError::missing(&["body"]));
    }
    let value = serde_json::from_value(j.clone()).map_err(|e| {
        ApiError::validation(vec![ApiError::field_error(
            "model_type",
            &["body"],
            &e.to_string(),
            j,
            None,
        )])
    })?;
    Ok((caller, value))
}

/// Refuse writes of engine state naming pipes `caller` may not change.
async fn check_editable(ctx: &Ctx, caller: &Access, refs: Vec<String>) -> Result<(), ApiError> {
    access::runs::check_refs_editable(
        &mut filter_for(ctx, caller),
        refs.iter().map(String::as_str),
    )
    .await
    .map_err(refused)
}

/// A run selector must not bypass its pipes' rights. Check every member of an existing run
/// before replacing its metadata, writing its events or clearing its history by run ID.
async fn check_run_ids_editable(
    ctx: &Ctx,
    caller: &Access,
    ids: &[String],
) -> Result<(), ApiError> {
    if caller.is_admin() {
        return Ok(());
    }
    let mut seen = std::collections::HashSet::new();
    let mut filter = filter_for(ctx, caller);
    for id in ids {
        if !seen.insert(id) {
            continue;
        }
        let rows = ctx
            .state
            .engine
            .get_runs(&RunsFilter {
                run_id: Some(id.clone()),
                ..Default::default()
            })
            .await
            .map_err(store_error)?;
        if !rows.is_empty() {
            filter.prime_run_generations(&rows).await;
            if !filter.run_generation_current(id) {
                return Err(ApiError::new(404, "Run was not found."));
            }
        }
        for row in rows.iter().filter(|row| row.pipe_ref.is_none()) {
            if !access::runs::record_pipelines_visible(&mut filter, row).await {
                return Err(ApiError::new(404, "Run was not found."));
            }
        }
        let refs = rows
            .iter()
            .flat_map(|row| {
                row.pipe_ref
                    .clone()
                    .into_iter()
                    .chain(access::runs::targets_of(row))
            })
            .collect::<Vec<_>>();
        if !rows.is_empty() && refs.is_empty() {
            return Err(refused(
                "Only administrators may change history whose pipes cannot be determined.".into(),
            ));
        }
        check_editable(ctx, caller, refs).await?;
    }
    Ok(())
}

/// `404` for the state of a pipe `caller` cannot see.
async fn check_visible(ctx: &Ctx, caller: &Access, pipe_ref: &str) -> Result<(), ApiError> {
    if filter_for(ctx, caller).visible(pipe_ref).await {
        return Ok(());
    }
    Err(ApiError::new(
        404,
        format!("No engine state for '{pipe_ref}'."),
    ))
}

async fn post_ledger(ctx: Ctx, body: Body) -> ApiResult {
    let (caller, entries): (Access, Vec<LedgerEntry>) = body_as(&ctx, body).await?;
    check_editable(
        &ctx,
        &caller,
        entries.iter().map(|e| e.pipe_ref.clone()).collect(),
    )
    .await?;
    envelope::tuple(&ctx.state.engine.append_ledger(entries).await, 500)
}

async fn get_ledger(ctx: Ctx) -> ApiResult {
    let caller = engine_access(&ctx, READ_SCOPES).await?;
    let Some(child_ref) = ctx.query.opt_str("child_ref") else {
        return Err(ApiError::missing(&["query", "child_ref"]));
    };
    check_visible(&ctx, &caller, &child_ref).await?;
    let after_seq = ctx.query.opt_int("after_seq")?;
    let mut entries = Vec::new();
    let mut filter = filter_for(&ctx, &caller);
    for entry in ctx.state.engine.ledger_after(&child_ref, after_seq).await {
        if filter.visible(&entry.pipe_ref).await {
            entries.push(entry);
        }
    }
    Ok(envelope::ok_with(
        format!(
            "Found {}.",
            count(entries.len(), "ledger entry", "ledger entries")
        ),
        "entries",
        serde_json::to_value(entries).unwrap_or(Json::Null),
    ))
}

async fn put_cursors(ctx: Ctx, body: Body) -> ApiResult {
    let (caller, cursors): (Access, Vec<EdgeCursor>) = body_as(&ctx, body).await?;
    check_editable(
        &ctx,
        &caller,
        cursors.iter().map(|c| c.child_ref.clone()).collect(),
    )
    .await?;
    envelope::tuple(&ctx.state.engine.put_cursors(cursors).await, 500)
}

async fn get_state(ctx: Ctx) -> ApiResult {
    let caller = engine_access(&ctx, READ_SCOPES).await?;
    let Some(pipe_ref) = ctx.query.opt_str("pipe_ref") else {
        return Err(ApiError::missing(&["query", "pipe_ref"]));
    };
    check_visible(&ctx, &caller, &pipe_ref).await?;
    match ctx.state.engine.get_state(&pipe_ref).await {
        Some(state) => Ok(envelope::ok_with(
            format!("Found the engine state of '{pipe_ref}'."),
            "state",
            serde_json::to_value(state).unwrap_or(Json::Null),
        )),
        None => Err(ApiError::new(
            404,
            format!("No engine state for '{pipe_ref}'."),
        )),
    }
}

async fn put_state(ctx: Ctx, body: Body) -> ApiResult {
    let (caller, states): (Access, Vec<EngineState>) = body_as(&ctx, body).await?;
    check_editable(
        &ctx,
        &caller,
        states.iter().map(|s| s.pipe_ref.clone()).collect(),
    )
    .await?;
    envelope::tuple(&ctx.state.engine.put_states(states).await, 500)
}

async fn post_runs(ctx: Ctx, body: Body) -> ApiResult {
    let (caller, mut runs): (Access, Vec<RunRecord>) = body_as(&ctx, body).await?;
    let mut nodes: Vec<meerschaum_connector::pipe_ops::Doc> = runs
        .iter()
        .filter_map(|run| {
            let reference = run.pipe_ref.as_ref()?;
            let mut doc = meerschaum_connector::pipe_ops::Doc::new();
            doc.insert(
                "pipe_ref".into(),
                meerschaum_kernel::value::Value::Str(reference.clone()),
            );
            if let Some(identity) = run
                .extra
                .get("registration_id")
                .and_then(meerschaum_core::Value::as_str)
            {
                doc.insert(
                    "registration_id".into(),
                    meerschaum_kernel::value::Value::Str(identity.into()),
                );
            }
            Some(doc)
        })
        .collect();
    canonicalize_node_registrations(&ctx, &mut nodes).await?;
    for (run, node) in runs
        .iter_mut()
        .filter(|run| run.pipe_ref.is_some())
        .zip(nodes)
    {
        run.extra.shift_remove("registration_ref");
        if let Some(meerschaum_kernel::value::Value::Str(reference)) = node.get("registration_ref")
        {
            run.extra.insert(
                "registration_ref".into(),
                meerschaum_core::Value::Str(reference.clone()),
            );
        }
    }
    let mut selections: Vec<Json> = runs
        .iter()
        .map(|run| match run.extra.get("selection") {
            Some(meerschaum_core::Value::Str(text)) => {
                serde_json::from_str(text).unwrap_or(Json::Null)
            }
            Some(value) => serde_json::to_value(value).unwrap_or(Json::Null),
            None => Json::Null,
        })
        .collect();
    canonicalize_pipeline_registries(&ctx, &mut selections).await?;
    for (run, selection) in runs.iter_mut().zip(selections) {
        if !selection.is_null() {
            let value = if matches!(
                run.extra.get("selection"),
                Some(meerschaum_core::Value::Str(_))
            ) {
                meerschaum_core::Value::Str(selection.to_string())
            } else {
                serde_json::from_value(selection).expect("valid core JSON value")
            };
            run.extra.insert("selection".into(), value);
        }
    }
    check_editable(
        &ctx,
        &caller,
        runs.iter()
            .flat_map(|r| {
                r.pipe_ref
                    .clone()
                    .into_iter()
                    .chain(access::runs::targets_of(r))
            })
            .collect(),
    )
    .await?;
    check_run_ids_editable(
        &ctx,
        &caller,
        &runs.iter().map(|r| r.run_id.clone()).collect::<Vec<_>>(),
    )
    .await?;
    envelope::tuple(&ctx.state.engine.record_runs(runs).await, 500)
}

/// Parse an ISO 8601 timestamp (naive means UTC), or `400`.
fn parse_since(text: &str) -> Result<chrono::DateTime<chrono::Utc>, ApiError> {
    let invalid = || {
        ApiError::new(
            400,
            format!("`since` must be an ISO 8601 datetime, got '{text}'."),
        )
    };
    let value = meerschaum_kernel::value::parse_datetime_value(text.trim()).ok_or_else(invalid)?;
    Ok(chrono::DateTime::from_naive_utc_and_offset(
        value.utc,
        chrono::Utc,
    ))
}

/// The [`RunsFilter`] of a request's query (`pipeline`, `pipe`, `status`, `since`, `limit`,
/// `run_id`).
pub fn runs_filter(ctx: &Ctx) -> Result<RunsFilter, ApiError> {
    let status = match ctx.query.opt_str("status") {
        None => None,
        Some(s) if s.trim().is_empty() || s.trim().eq_ignore_ascii_case("any") => None,
        Some(s) => Some(RunStatusFilter::parse(&s).ok_or_else(|| {
            ApiError::new(
                400,
                format!("`status` must be 'success', 'failed' or 'running', got '{s}'."),
            )
        })?),
    };
    let limit = match ctx
        .query
        .opt_int("limit")
        .map_err(|_| ApiError::new(400, "`limit` must be a whole number."))?
    {
        None => DEFAULT_RUNS,
        Some(n) if n < 1 => return Err(ApiError::new(400, "`limit` must be at least 1.")),
        Some(n) => usize::try_from(n).unwrap_or(MAX_RUNS).min(MAX_RUNS),
    };
    Ok(RunsFilter {
        run_id: ctx.query.opt_str("run_id").filter(|s| !s.is_empty()),
        pipeline: ctx.query.opt_str("pipeline").filter(|s| !s.is_empty()),
        pipe_ref: ctx.query.opt_str("pipe").filter(|s| !s.is_empty()),
        status,
        since: ctx
            .query
            .opt_str("since")
            .filter(|s| !s.trim().is_empty())
            .map(|s| parse_since(&s))
            .transpose()?,
        limit: Some(limit),
    })
}

async fn get_runs(ctx: Ctx) -> ApiResult {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, READ_SCOPES).await?;
    let filter = runs_filter(&ctx)?;
    let (store, instances) = crate::engine_stream::store_for(&ctx, &caller)?;
    let runs = store.get_runs(&filter).await.map_err(store_error)?;
    let default = instances.first().cloned().unwrap_or_default();
    let runs = access::runs::visible_runs(
        &mut RefFilter::new(&ctx.state, &caller, &default).for_engine(),
        runs,
    )
    .await;
    let n = runs.iter().filter(|r| r.pipe_ref.is_none()).count();
    Ok(envelope::ok_with(
        format!("Found {}.", count(n, "run", "runs")),
        "runs",
        serde_json::to_value(runs).unwrap_or(Json::Null),
    ))
}

/// One run and its nodes, `None` if the store does not know it.
pub async fn run_detail(
    store: &dyn crate::backends::EngineBackend,
    run_id: &str,
) -> Result<Option<(RunRecord, Vec<RunRecord>)>, EngineStoreError> {
    let filter = RunsFilter {
        run_id: Some(run_id.to_string()),
        limit: Some(1),
        ..Default::default()
    };
    let rows = store.get_runs(&filter).await?;
    let (nodes, runs): (Vec<RunRecord>, Vec<RunRecord>) =
        rows.into_iter().partition(|r| r.pipe_ref.is_some());
    Ok(runs.into_iter().next().map(|run| (run, nodes)))
}

/// [`run_detail`] as `filter`'s caller may read it (`None` for a run it cannot see).
pub async fn visible_run_detail(
    store: &dyn crate::backends::EngineBackend,
    run_id: &str,
    filter: &mut RefFilter,
) -> Result<Option<(RunRecord, Vec<RunRecord>)>, EngineStoreError> {
    let Some((run, nodes)) = run_detail(store, run_id).await? else {
        return Ok(None);
    };
    let mut records = vec![run];
    records.extend(nodes);
    let mut records = access::runs::visible_runs(filter, records).await;
    let Some(position) = records.iter().position(|r| r.pipe_ref.is_none()) else {
        return Ok(None);
    };
    let run = records.remove(position);
    Ok(Some((run, records)))
}

async fn get_run(ctx: Ctx, Path(run_id): Path<String>) -> ApiResult {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, READ_SCOPES).await?;
    let (store, instances) = crate::engine_stream::store_for(&ctx, &caller)?;
    let default = instances.first().cloned().unwrap_or_default();
    let Some((run, nodes)) = visible_run_detail(
        store.as_ref(),
        &run_id,
        &mut RefFilter::new(&ctx.state, &caller, &default).for_engine(),
    )
    .await
    .map_err(store_error)?
    else {
        return Err(ApiError::new(404, format!("Run '{run_id}' was not found.")));
    };
    let mut fields = JsonMap::new();
    let message = format!(
        "Run '{run_id}' is {} ({}).",
        match run.status.as_str() {
            "success" => "a success",
            "failed" => "a failure",
            _ => "running",
        },
        count(nodes.len(), "pipe", "pipes")
    );
    fields.insert(
        "run".into(),
        serde_json::to_value(&run).unwrap_or(Json::Null),
    );
    fields.insert(
        "nodes".into(),
        serde_json::to_value(&nodes).unwrap_or(Json::Null),
    );
    Ok(envelope::ok(message, fields))
}

async fn post_events(ctx: Ctx, body: Body) -> ApiResult {
    let (caller, events): (Access, Vec<EngineEvent>) = body_as(&ctx, body).await?;
    check_editable(
        &ctx,
        &caller,
        events.iter().filter_map(|e| e.pipe_ref.clone()).collect(),
    )
    .await?;
    check_run_ids_editable(
        &ctx,
        &caller,
        &events.iter().map(|e| e.run_id.clone()).collect::<Vec<_>>(),
    )
    .await?;
    envelope::tuple(&ctx.state.engine.record_events(events).await, 500)
}

/// A lease answer: `200` when done, `409` when the holder does not (or may not) hold it.
fn lease_response(ok: bool, done: String, refused: String) -> ApiResult {
    if ok {
        Ok(envelope::ok(done, JsonMap::new()))
    } else {
        Err(ApiError::new(409, refused))
    }
}

async fn acquire_lease(ctx: Ctx, body: Body) -> ApiResult {
    let (caller, req): (Access, LeaseRequest) = body_as(&ctx, body).await?;
    check_editable(&ctx, &caller, vec![req.pipe_ref.clone()]).await?;
    let ok = ctx
        .state
        .engine
        .acquire_lease(
            &req.pipe_ref,
            &req.holder,
            Duration::from_millis(req.ttl_ms),
        )
        .await;
    lease_response(
        ok,
        format!("Acquired the lease on '{}'.", req.pipe_ref),
        format!(
            "Could not acquire the lease on '{}': another process holds it.",
            req.pipe_ref
        ),
    )
}

async fn release_lease(ctx: Ctx, body: Body) -> ApiResult {
    let (caller, req): (Access, LeaseRequest) = body_as(&ctx, body).await?;
    check_editable(&ctx, &caller, vec![req.pipe_ref.clone()]).await?;
    let ok = ctx
        .state
        .engine
        .release_lease(&req.pipe_ref, &req.holder)
        .await;
    lease_response(
        ok,
        format!("Released the lease on '{}'.", req.pipe_ref),
        format!(
            "Could not release the lease on '{}': '{}' does not hold it.",
            req.pipe_ref, req.holder
        ),
    )
}
