//! Saved Pipelines over HTTP (new in v5). Every route answers in the v5 shape
//! (`{"success": bool, "message": str, ...}` with real status codes, [`crate::envelope`]).
//!
//! | Route | Scopes | What it does |
//! |---|---|---|
//! | `GET /pipelines` | `pipes:read` | The saved Pipelines of the instance (`pipelines`) |
//! | `GET /pipelines/{name}` | `pipes:read` | One Pipeline: `pipeline` (its definition), `members`, `last_run` |
//! | `POST /pipelines/{name}/plan` | `pipes:read` | The plan of a run (`plan`, `text`, `warnings`); nothing moves, nothing is registered |
//! | `POST /pipelines/{name}/sync` | `pipes:write`, `jobs:execute` | Start a run as a background job: `run_id`, `job`, `stream` |
//! | `GET /pipelines/keys` | `pipes:read` | The registration keys (`keys`) |
//! | `POST /pipelines/{name}/register` | `pipes:write` | Register a definition (`409` if it exists) |
//! | `PATCH /pipelines/{name}/edit` | `pipes:write` | Replace (or `patch`) a definition (`404` if missing) |
//! | `DELETE /pipelines/{name}/delete` | `pipes:delete` | Delete a definition (`409` while scheduled jobs use it) |
//! | `GET /pipelines/{name}/id`, `/attributes` | `pipes:read` | The registration's ID and raw attributes |
//! | `POST`/`DELETE /pipelines/leases` | `pipes:write` | The metadata leases of Pipeline edits |
//! | `GET`/`PATCH /pipelines/{name}/access`, `POST .../lock`, `.../unlock` | `pipes:read` / `pipes:write` | The Pipeline's access and locks (as for pipes, [`super::pipe_access`]) |
//!
//! **Access (rc15).** A saved Pipeline carries `access` and `locks` like a pipe (its registration
//! pipe's parameters): the routes answer `404` for a Pipeline the caller cannot see, its new
//! registration is the caller's (in the caller's primary team), and changing it needs edit rights
//! on it. Members the caller cannot see are left out of every answer and counted
//! (`hidden_members`); a plan's hidden nodes are anonymized; an edit keeps them. Syncing needs
//! edit rights on every pipe the run would sync (a member locked against syncs is skipped by the
//! engine and needs none).
//!
//! The metadata routes transport only the `mrsm/pipeline/<name>` registration's raw parameters;
//! the CLI and SDK own lifecycle validation, tag approval and schedule checks. Generic `mrsm`
//! pipe data stays private.
use super::{Ctx, body_bytes};
use crate::{
    access::{self, Access, RefFilter},
    auth::{Principal, is_admin},
    backends::{PlanError, PlanOptions, RunsFilter},
    config::ServerConfig,
    envelope,
    error::{ApiError, ApiResult},
    json::{from_core, to_core},
    state::AppState,
};
use axum::{
    Router,
    body::Body,
    extract::Path,
    routing::{get, post},
};
use meerschaum_connector::{InstanceConnector, PipesKeysQuery};
use meerschaum_core::{Map, Pipe, Value};
use serde_json::{Map as JsonMap, Value as Json, json};
use std::sync::Arc;
use std::time::Duration;

/// How long the run event stream a Pipeline sync names waits for the run to start (`?wait=`).
pub const RUN_STREAM_WAIT_SECONDS: u64 = 30;

/// Register the `pipelines` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/pipelines", get(list))
        .route("/pipelines/keys", get(keys))
        .route(
            "/pipelines/leases",
            post(acquire_lease).delete(release_lease),
        )
        .route("/pipelines/{name}", get(detail))
        .route("/pipelines/{name}/plan", post(plan))
        .route("/pipelines/{name}/sync", post(sync))
        .route("/pipelines/{name}/register", post(register))
        .route("/pipelines/{name}/edit", axum::routing::patch(edit))
        .route("/pipelines/{name}/rename", post(rename))
        .route("/pipelines/{name}/delete", axum::routing::delete(delete))
        .route("/pipelines/{name}/id", get(id))
        .route("/pipelines/{name}/attributes", get(attributes))
        .route(
            "/pipelines/{name}/access",
            get(get_access).patch(patch_access),
        )
        .route("/pipelines/{name}/lock", post(lock))
        .route("/pipelines/{name}/unlock", post(unlock))
}

/// The SDK's name rule (ASCII, 1 to 128 bytes, starting with a letter or digit).
pub fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty()
        || name.len() > 128
        || !name.as_bytes()[0].is_ascii_alphanumeric()
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
    {
        return Err(ApiError::new(
            400,
            "Invalid Pipeline name: use 1 to 128 letters, digits, '.', '_' or '-', starting with a letter or digit.",
        ));
    }
    Ok(())
}

fn instance_keys_of(ctx: &Ctx) -> Option<String> {
    ctx.instance_keys()
        .or_else(|| ctx.query.opt_str("instance"))
}

fn connector(ctx: &Ctx) -> Result<Arc<dyn InstanceConnector>, ApiError> {
    ctx.state
        .get_api_connector(instance_keys_of(ctx).as_deref())
}

/// The registration pipe `Pipe('mrsm', 'pipeline', name)` on the registry instance.
pub fn registration_pipe(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
) -> Result<Pipe, ApiError> {
    validate_name(name)?;
    Pipe::builder("mrsm", "pipeline")
        .location(Some(name))
        .instance(&conn.connector_keys())
        .defaults(state.config.pipe_defaults.clone())
        .build()
        .map_err(|e| ApiError::new(400, e.to_string()))
}

fn metadata(ctx: &Ctx, conn: &Arc<dyn InstanceConnector>, name: &str) -> Result<Pipe, ApiError> {
    registration_pipe(&ctx.state, conn, name)
}

fn failed(context: &str, error: impl std::fmt::Display) -> ApiError {
    ApiError::exception(context, error)
}

async fn parameters(body: Body, name: &str) -> Result<Map, ApiError> {
    let body = super::pipes::parse_json_body(&body_bytes(body).await?)?
        .ok_or_else(|| ApiError::missing(&["body"]))?;
    let Json::Object(_) = body else {
        return Err(ApiError::new(
            400,
            "Expected raw Pipeline parameters object.",
        ));
    };
    let Some(definition) = body.get("pipeline").and_then(Json::as_object) else {
        return Err(ApiError::new(
            400,
            "Parameters must contain a Pipeline definition object.",
        ));
    };
    if definition
        .get("name")
        .and_then(Json::as_str)
        .is_some_and(|value| value != name)
    {
        return Err(ApiError::new(
            400,
            "Pipeline definition name must match the route.",
        ));
    }
    if definition.get("targets").is_some_and(|targets| {
        !targets
            .as_array()
            .is_some_and(|rows| rows.iter().all(Json::is_string))
    }) {
        return Err(ApiError::new(
            400,
            "Pipeline targets must be exact reference strings.",
        ));
    }
    Ok(to_core(&body).to_map())
}

fn canonicalize_client_members(ctx: &Ctx, registry: &str, parameters: &mut Map) {
    if let Some(Value::Map(definition)) = parameters.get_mut("pipeline") {
        let alias = ctx
            .headers
            .get(meerschaum_connector_api::engine_pipes::CLIENT_INSTANCE_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|alias| alias.starts_with("api:") && !alias.contains('|'));
        if let Some(alias) = alias {
            meerschaum_connector_api::engine_pipes::rebind_pipeline_definition(
                definition, alias, registry,
            );
        } else {
            definition.insert("registry".into(), Value::Str(registry.to_string()));
        }
    }
}

/// The saved definition of `name` (`parameters.pipeline`), `None` when it is not registered.
pub async fn definition_on(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
) -> Result<Option<Json>, ApiError> {
    let pipe = registration_pipe(state, conn, name)?;
    if conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| failed("get_pipe_id", e))?
        .is_none()
    {
        return Ok(None);
    }
    let attrs = conn
        .get_pipe_attributes(&pipe)
        .await
        .map_err(|e| failed("get_pipe_attributes", e))?;
    Ok(crate::state::parameters_of(&attrs)
        .and_then(|p| p.get("pipeline").cloned())
        .map(|d| from_core(&d)))
}

/// The definition of `name`, or `404`.
pub async fn require_definition_on(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
) -> Result<Json, ApiError> {
    definition_on(state, conn, name).await?.ok_or_else(|| {
        ApiError::new(
            404,
            format!(
                "Pipeline '{name}' is not registered on '{}'.",
                conn.connector_keys()
            ),
        )
    })
}

fn plural(n: usize, singular: &str, plural: &str) -> String {
    format!("{n} {}", if n == 1 { singular } else { plural })
}

/// The saved Pipelines of the registry `conn` with their registrations' parameters (`access`
/// and `locks` live there), by name (each definition carries its `name`).
pub async fn list_registrations(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
) -> Result<Vec<(Json, Map)>, ApiError> {
    let listing = conn
        .fetch_pipes_keys(&PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await
        .map_err(|e| failed("fetch_pipes_keys", e))?;
    let mut pipelines: Vec<(String, Json, Map)> = Vec::new();
    for row in listing.entries() {
        if let Some(name) = &row.location_key {
            let pipe = registration_pipe(state, conn, name)?;
            let attrs = conn
                .get_pipe_attributes(&pipe)
                .await
                .map_err(|e| failed("get_pipe_attributes", e))?;
            let parameters = crate::state::parameters_of(&attrs).unwrap_or_default();
            if let Some(definition) = parameters.get("pipeline").cloned() {
                pipelines.push((name.clone(), from_core(&definition), parameters));
            }
        }
    }
    pipelines.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(pipelines
        .into_iter()
        .map(|(name, mut definition, parameters)| {
            if let Some(o) = definition.as_object_mut() {
                o.entry("name").or_insert(json!(name));
            }
            (definition, parameters)
        })
        .collect())
}

/// The registration parameters of the Pipeline `name` (`None` when it is not registered).
pub async fn registration_parameters(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
) -> Result<Option<Map>, ApiError> {
    let pipe = registration_pipe(state, conn, name)?;
    if conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| failed("get_pipe_id", e))?
        .is_none()
    {
        return Ok(None);
    }
    let attrs = conn
        .get_pipe_attributes(&pipe)
        .await
        .map_err(|e| failed("get_pipe_attributes", e))?;
    Ok(Some(
        crate::state::parameters_of(&attrs).unwrap_or_default(),
    ))
}

/// `definition` as `caller` may read it: the members (`targets`) it cannot see left out and
/// counted (`hidden_members`).
pub async fn visible_definition(filter: &mut RefFilter, mut definition: Json) -> Json {
    let targets = targets_of(&definition);
    let mut kept = Vec::with_capacity(targets.len());
    let mut hidden = 0usize;
    for target in targets {
        if filter.visible(&target).await {
            kept.push(Json::String(target));
        } else {
            hidden += 1;
        }
    }
    if let Some(o) = definition.as_object_mut() {
        // The response declares its actual served registry, never a user-supplied alias.
        o.insert("registry".into(), json!(filter.default_instance()));
        if o.contains_key("targets") {
            o.insert("targets".into(), Json::Array(kept));
        }
        if hidden > 0 {
            o.insert("hidden_members".into(), json!(hidden));
        }
    }
    definition
}

/// The registered Pipeline `name` that `caller` may see: its definition and registration
/// parameters (`404` otherwise, as for one that does not exist).
pub async fn visible_registration(
    state: &AppState,
    caller: &Access,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
) -> Result<(Json, Map), ApiError> {
    let missing = || {
        ApiError::new(
            404,
            format!(
                "Pipeline '{name}' is not registered on '{}'.",
                conn.connector_keys()
            ),
        )
    };
    let parameters = registration_parameters(state, conn, name)
        .await?
        .ok_or_else(missing)?;
    if !caller.can_see(&parameters) {
        return Err(missing());
    }
    let definition = parameters
        .get("pipeline")
        .map(from_core)
        .ok_or_else(missing)?;
    Ok((definition, parameters))
}

/// The saved Pipelines of the registry `conn`, by name (each definition carries its `name`).
pub async fn list_definitions(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
) -> Result<Vec<Json>, ApiError> {
    let listing = conn
        .fetch_pipes_keys(&PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await
        .map_err(|e| failed("fetch_pipes_keys", e))?;
    let mut pipelines: Vec<(String, Json)> = Vec::new();
    for row in listing.entries() {
        if let Some(name) = &row.location_key {
            let pipe = registration_pipe(state, conn, name)?;
            let attrs = conn
                .get_pipe_attributes(&pipe)
                .await
                .map_err(|e| failed("get_pipe_attributes", e))?;
            if let Some(definition) =
                crate::state::parameters_of(&attrs).and_then(|p| p.get("pipeline").cloned())
            {
                pipelines.push((name.clone(), from_core(&definition)));
            }
        }
    }
    pipelines.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(pipelines
        .into_iter()
        .map(|(name, mut definition)| {
            if let Some(o) = definition.as_object_mut() {
                o.entry("name").or_insert(json!(name));
            }
            definition
        })
        .collect())
}

async fn list(ctx: Ctx) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let registry = conn.connector_keys();
    let mut filter = RefFilter::new(&ctx.state, &caller, &registry);
    let mut pipelines = Vec::new();
    for (definition, parameters) in list_registrations(&ctx.state, &conn).await? {
        if caller.can_see(&parameters) {
            pipelines.push(visible_definition(&mut filter, definition).await);
        }
    }
    let message = format!(
        "Found {} on '{}'.",
        plural(pipelines.len(), "Pipeline", "Pipelines"),
        conn.connector_keys()
    );
    Ok(envelope::ok_with(message, "pipelines", json!(pipelines)))
}

async fn keys(ctx: Ctx) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let decode = |key: &str| -> Result<Vec<Value>, ApiError> {
        match Value::from_json_str(&ctx.query.str_or(key, "[]")) {
            Ok(Value::List(items)) => Ok(items),
            Ok(Value::Null) => Ok(Vec::new()),
            _ => Err(ApiError::new(400, format!("Invalid {key} filter."))),
        }
    };
    let query = PipesKeysQuery {
        connector_keys: vec!["mrsm".into()],
        metric_keys: vec!["pipeline".into()],
        location_keys: decode("location_keys")?
            .into_iter()
            .map(|value| {
                if value.is_null() {
                    None
                } else {
                    Some(value.py_str())
                }
            })
            .collect(),
        tags: decode("tags")?.iter().map(Value::py_str).collect(),
        params: ctx
            .query
            .get("params")
            .map(Value::from_json_str)
            .transpose()
            .map_err(|e| ApiError::new(400, format!("Invalid params filter: {e}")))?
            .and_then(|value| value.as_map().cloned()),
    };
    let keys = conn
        .registered_pipes(&query)
        .await
        .map_err(|e| failed("fetch_pipes_keys", e))?;
    let mut rows: Vec<Json> = Vec::new();
    let mut dictionary = serde_json::Map::new();
    let mut as_dict = ctx.query.bool("as_dict", false)?;
    let mut filter = RefFilter::new(&ctx.state, &caller, &conn.connector_keys());
    for (identity, mut row) in keys {
        let mut parameters = match row.extra {
            Some(Value::Map(parameters)) => parameters,
            Some(Value::Str(text)) => match Value::from_json_str(&text) {
                Ok(Value::Map(parameters)) => parameters,
                _ => continue,
            },
            _ => continue,
        };
        if !caller.can_see(&parameters) {
            continue;
        }
        if let Some(definition) = parameters.get("pipeline") {
            let visible = visible_definition(&mut filter, from_core(definition)).await;
            parameters.insert("pipeline".into(), to_core(&visible));
        }
        row.extra = Some(Value::Map(parameters));
        let value = from_core(&Value::List(row.to_list()));
        rows.push(value.clone());
        if let Some(identity) = identity {
            dictionary.insert(identity.to_string(), value);
        } else {
            as_dict = false;
        }
    }
    Ok(envelope::ok_with(
        format!("Found {}.", plural(rows.len(), "Pipeline", "Pipelines")),
        "keys",
        if as_dict {
            Json::Object(dictionary)
        } else {
            json!(rows)
        },
    ))
}

/// A member reference `instance|connector|metric|location` as JSON, with whether it is
/// registered (`null` when its instance cannot be checked from here).
pub async fn member(state: &AppState, reference: &str) -> Json {
    let parts: Vec<&str> = reference.splitn(4, '|').collect();
    let [instance, ck, mk, lk] = parts.as_slice() else {
        return json!({"pipe_ref": reference, "registered": null});
    };
    let location = crate::state::normalize_location(Some(lk));
    let registered = match state.get_api_connector(Some(instance)) {
        Ok(conn) => match Pipe::builder(ck, mk)
            .location(location.as_deref())
            .instance(instance)
            .build()
        {
            Ok(pipe) => match conn.get_pipe_id(&pipe).await {
                Ok(id) => json!(id.is_some()),
                Err(_) => Json::Null,
            },
            Err(_) => Json::Null,
        },
        Err(_) => Json::Null,
    };
    json!({
        "pipe_ref": reference,
        "connector_keys": ck,
        "metric_key": mk,
        "location_key": location,
        "instance_keys": instance,
        "registered": registered,
    })
}

/// The members (targets) a definition names.
pub fn targets_of(definition: &Json) -> Vec<String> {
    definition
        .get("targets")
        .and_then(Json::as_array)
        .map(|t| {
            t.iter()
                .filter_map(Json::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// The engine store of the registry instance `conn` (the server's own, or that instance's state
/// pipes): where a saved Pipeline's runs are kept.
pub fn store_of(
    state: &AppState,
    conn: &Arc<dyn InstanceConnector>,
) -> Arc<dyn crate::backends::EngineBackend> {
    let keys = conn.connector_keys();
    if crate::engine_stream::own_instance_names(state).contains(&keys) {
        state.engine.clone()
    } else {
        Arc::new(crate::engine_store::InstanceEngine::new(
            conn.clone(),
            &keys,
        ))
    }
}

/// The latest run of the Pipeline `name` on its registry `conn` (`null` if none, or if the
/// history cannot be read: it is informational here).
pub async fn last_run(
    state: &AppState,
    caller: &Access,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
) -> Json {
    let store = store_of(state, conn);
    match store
        .get_runs(&RunsFilter {
            pipeline: Some(name.to_string()),
            limit: Some(1),
            ..Default::default()
        })
        .await
    {
        Ok(runs) => access::runs::visible_runs(
            &mut RefFilter::new(state, caller, &conn.connector_keys()).for_engine(),
            runs,
        )
        .await
        .into_iter()
        .find(|r| r.pipe_ref.is_none())
        .map(|r| serde_json::to_value(r).unwrap_or(Json::Null))
        .unwrap_or(Json::Null),
        Err(e) => {
            tracing::debug!("Cannot read the runs of Pipeline '{name}': {e}");
            Json::Null
        }
    }
}

/// `GET /pipelines/{name}`: the definition, its members and the latest run.
async fn detail(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let (definition, _) = visible_registration(&ctx.state, &caller, &conn, &name).await?;
    let registry = conn.connector_keys();
    let mut filter = RefFilter::new(&ctx.state, &caller, &registry);
    let mut members = Vec::new();
    let mut hidden = 0usize;
    for reference in targets_of(&definition) {
        if filter.visible(&reference).await {
            members.push(member(&ctx.state, &reference).await);
        } else {
            hidden += 1;
        }
    }
    let definition = visible_definition(&mut filter, definition).await;
    let last_run = last_run(&ctx.state, &caller, &conn, &name).await;
    let mut fields = JsonMap::new();
    let message = format!(
        "Pipeline '{name}' on '{}' has {}{}.",
        conn.connector_keys(),
        plural(members.len() + hidden, "member", "members"),
        if hidden > 0 {
            format!(" ({})", plural(hidden, "hidden pipe", "hidden pipes"))
        } else {
            String::new()
        }
    );
    fields.insert("pipeline".into(), definition);
    fields.insert("members".into(), json!(members));
    fields.insert("hidden_members".into(), json!(hidden));
    fields.insert("last_run".into(), last_run);
    Ok(envelope::ok(message, fields))
}

/// A plan as `caller` may read it: the nodes of pipes it cannot see anonymized (`pipe_ref` and
/// details dropped, `hidden: true`), the edges to them dropped, `hidden_nodes` counted, and the
/// rendered `text` (which names every node) left out when any node is hidden.
pub async fn visible_plan(filter: &mut RefFilter, mut planned: Json) -> Json {
    let mut hidden: Vec<String> = Vec::new();
    if let Some(nodes) = planned
        .get_mut("plan")
        .and_then(|p| p.get_mut("nodes"))
        .and_then(Json::as_array_mut)
    {
        for node in nodes.iter_mut() {
            let Some(pipe_ref) = node
                .get("pipe_ref")
                .and_then(Json::as_str)
                .map(str::to_string)
            else {
                continue;
            };
            if filter.visible(&pipe_ref).await {
                continue;
            }
            hidden.push(pipe_ref);
            *node = json!({
                "pipe_ref": null,
                "hidden": true,
                "action": node.get("action").cloned().unwrap_or(Json::Null),
            });
        }
        for node in nodes.iter_mut() {
            if let Some(consumes) = node.get_mut("consumes").and_then(Json::as_array_mut) {
                consumes.retain(|c| {
                    !c.get("parent")
                        .and_then(Json::as_str)
                        .is_some_and(|p| hidden.iter().any(|h| h == p))
                });
            }
        }
    }
    if hidden.is_empty() {
        return planned;
    }
    if let Some(edges) = planned
        .get_mut("plan")
        .and_then(|p| p.get_mut("edges"))
        .and_then(Json::as_array_mut)
    {
        edges.retain(|edge| {
            !edge.as_array().is_some_and(|pair| {
                pair.iter()
                    .filter_map(Json::as_str)
                    .any(|r| hidden.iter().any(|h| h == r))
            })
        });
    }
    // Warnings that name a hidden pipe go too (they name it by its keys).
    let names_hidden = |w: &Json| {
        w.as_str().is_some_and(|text| {
            hidden.iter().any(|h| {
                access::split_ref(h)
                    .is_some_and(|(ck, mk, _)| text.contains(ck) && text.contains(mk))
            })
        })
    };
    if let Some(nodes) = planned
        .get_mut("plan")
        .and_then(|p| p.get_mut("nodes"))
        .and_then(Json::as_array_mut)
    {
        for node in nodes {
            if node.get("reason").is_some_and(names_hidden)
                && let Some(fields) = node.as_object_mut()
            {
                fields.remove("reason");
            }
            if let Some(warnings) = node.get_mut("warnings").and_then(Json::as_array_mut) {
                warnings.retain(|warning| !names_hidden(warning));
            }
        }
    }
    for warnings in [planned.get_mut("warnings")].into_iter().flatten() {
        if let Some(list) = warnings.as_array_mut() {
            list.retain(|w| !names_hidden(w));
        }
    }
    if let Some(list) = planned
        .get_mut("plan")
        .and_then(|p| p.get_mut("warnings"))
        .and_then(Json::as_array_mut)
    {
        list.retain(|w| !names_hidden(w));
    }
    if let Some(o) = planned.as_object_mut() {
        o.remove("text");
        o.insert("hidden_nodes".into(), json!(hidden.len()));
    }
    planned
}

/// The `begin` / `end` of a plan or sync body (strings or integers), `400` otherwise.
pub fn bounds_of(body: &Json) -> Result<PlanOptions, ApiError> {
    let bound = |key: &str| -> Result<Option<String>, ApiError> {
        match body.get(key) {
            None | Some(Json::Null) => Ok(None),
            Some(Json::String(s)) if s.trim().is_empty() => Ok(None),
            Some(Json::String(s)) => Ok(Some(s.trim().to_string())),
            Some(Json::Number(n)) if n.is_i64() => Ok(Some(n.to_string())),
            Some(other) => Err(ApiError::new(
                400,
                format!("`{key}` must be an ISO 8601 datetime or an integer, got {other}."),
            )),
        }
    };
    Ok(PlanOptions {
        begin: bound("begin")?,
        end: bound("end")?,
    })
}

async fn optional_body(body: Body) -> Result<Json, ApiError> {
    let body = super::pipes::parse_json_body(&body_bytes(body).await?)?.unwrap_or(Json::Null);
    match body {
        Json::Null | Json::Object(_) => Ok(body),
        _ => Err(ApiError::new(
            400,
            "The request body must be a JSON object.",
        )),
    }
}

/// `POST /pipelines/{name}/plan`: what a run would do, read-only.
async fn plan(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let body = optional_body(body).await?;
    let options = bounds_of(&body)?;
    let conn = connector(&ctx)?;
    visible_registration(&ctx.state, &caller, &conn, &name).await?;
    let planned = ctx
        .state
        .pipelines
        .plan(&name, &conn.connector_keys(), &options)
        .await
        .map_err(plan_error)?;
    let registry = conn.connector_keys();
    let planned = visible_plan(&mut RefFilter::new(&ctx.state, &caller, &registry), planned).await;
    let mut fields = planned.as_object().cloned().unwrap_or_default();
    let nodes = fields
        .get("plan")
        .and_then(|p| p.get("nodes"))
        .and_then(Json::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    fields.entry("warnings").or_insert(json!([]));
    Ok(envelope::ok(
        format!(
            "Planned Pipeline '{name}': {}.",
            plural(nodes, "pipe", "pipes")
        ),
        fields,
    ))
}

/// A plan failure as a response.
pub fn plan_error(e: PlanError) -> ApiError {
    match e {
        PlanError::Unavailable => ApiError::new(501, e.to_string()),
        PlanError::Invalid(m) => ApiError::new(400, m),
        PlanError::Failed(m) => ApiError::new(500, "Failed to plan the Pipeline.").with_internal(m),
    }
}

/// A new run ID for a Pipeline run started over HTTP: when and how it started
/// (`2026-10-27T05-00-00Z-api-1a2b3c4d5e6f`).
pub fn new_run_id() -> String {
    format!(
        "{}-api-{}",
        chrono::Utc::now().format("%Y-%m-%dT%H-%M-%SZ"),
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// Whether a caller may run Pipelines: administrators always; others when the server lets
/// non-administrators run actions (`api:permissions:actions:non_admin`, off under `--secure`)
/// and every instance the run touches is one they may use.
async fn may_run(
    state: &AppState,
    principal: Option<&Principal>,
    instances: &[String],
) -> Result<(), ApiError> {
    if is_admin(state, principal).await {
        return Ok(());
    }
    if !state.config.actions_non_admin {
        return Err(ApiError::new(
            403,
            "The administrator for this server has not allowed users to run Pipelines.",
        ));
    }
    for keys in instances {
        state.get_api_connector(Some(keys)).map_err(|e| {
            ApiError::new(
                403,
                format!(
                    "Cannot run this Pipeline: its pipes on '{keys}' are not available to you ({}).",
                    match &e.detail {
                        Some(Json::String(s)) => s.clone(),
                        _ => "not allowed".into(),
                    }
                ),
            )
        })?;
    }
    Ok(())
}

/// Refuse a run of the Pipeline `name` that would sync a pipe `caller` may not change (a pipe
/// locked against syncs is skipped by the engine and needs no rights): the plan's nodes, or the
/// members when this server cannot plan.
pub async fn check_members_editable(
    state: &AppState,
    caller: &Access,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
    definition: &Json,
    options: &PlanOptions,
) -> Result<(), ApiError> {
    if caller.no_auth || caller.is_admin() {
        return Ok(());
    }
    let registry = conn.connector_keys();
    let refs: Vec<String> = match state.pipelines.plan(name, &registry, options).await {
        Ok(planned) => planned
            .get("plan")
            .and_then(|p| p.get("nodes"))
            .and_then(Json::as_array)
            .map(|nodes| {
                nodes
                    .iter()
                    .filter_map(|n| n.get("pipe_ref").and_then(Json::as_str))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        // Without a plan (this server cannot plan, or the plan fails, as the run then will):
        // the members.
        Err(_) => targets_of(definition),
    };
    let mut filter = RefFilter::new(state, caller, &registry);
    let mut refused = 0usize;
    for pipe_ref in &refs {
        let (editable, sync_locked) = filter.editable(pipe_ref).await;
        if !editable && !sync_locked {
            refused += 1;
        }
    }
    if refused > 0 {
        return Err(ApiError::new(
            403,
            format!(
                "You cannot run Pipeline '{name}': you may not change {} it would sync.",
                plural(refused, "pipe", "of the pipes")
            ),
        ));
    }
    Ok(())
}

/// What [`start_sync`] started.
#[derive(Debug, Clone)]
pub struct StartedSync {
    /// The run's ID.
    pub run_id: String,
    /// The background job that runs it.
    pub job: String,
    /// The fields of the response (`run_id`, `job`, `stream`, `log_stream`, `logs`).
    pub fields: JsonMap<String, Json>,
    /// The job's start message.
    pub message: String,
}

async fn launch_reserved_run(
    state: AppState,
    principal: Option<Principal>,
    conn: Arc<dyn InstanceConnector>,
    run_id: String,
    job: String,
    sysargs: Vec<String>,
    metadata: Json,
) -> Result<meerschaum_core::SuccessTuple, ApiError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let unavailable = || ApiError::new(503, "Timed out reserving the Pipeline run.");
    let store = store_of(&state, &conn);
    let resource = format!("{}|mrsm|pipeline_launch|{run_id}", conn.connector_keys());
    let holder = uuid::Uuid::new_v4().to_string();
    if !tokio::time::timeout_at(
        deadline,
        store.acquire_lease(&resource, &holder, Duration::from_secs(65)),
    )
    .await
    .map_err(|_| unavailable())?
    {
        return Err(ApiError::new(
            409,
            "The Pipeline run ID is already reserved.",
        ));
    }
    let result = tokio::time::timeout_at(deadline, async {
        let existing = store
            .get_runs(&crate::backends::RunsFilter {
                run_id: Some(run_id),
                limit: Some(1),
                ..Default::default()
            })
            .await
            .map_err(crate::engine_stream::api_error)?;
        if !existing.is_empty() || state.jobs.exists(&job).await {
            return Err(ApiError::new(409, "The Pipeline run ID already exists."));
        }
        super::execute::start_execution(&state, principal.as_ref(), &job, &sysargs, metadata).await
    })
    .await
    .map_err(|_| unavailable());
    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        store.release_lease(&resource, &holder),
    )
    .await;
    result?
}

/// Start a run of the saved Pipeline `name` (on the registry `conn`) as a background job, the
/// way the console does (`sync pipelines NAME -i REGISTRY --run-id RUN_ID`, a temporary
/// execution owned by the caller). `POST /pipelines/{name}/sync` and the MCP `sync_pipeline`
/// tool share it.
pub async fn start_sync(
    state: &AppState,
    principal: Option<&Principal>,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
    definition: &Json,
    options: &PlanOptions,
    run_id: Option<String>,
) -> Result<StartedSync, ApiError> {
    let registry = conn.connector_keys();
    let mut instances = vec![registry.clone()];
    for reference in targets_of(definition) {
        if let Some(instance) = reference.split('|').next()
            && !instances.iter().any(|i| i == instance)
        {
            instances.push(instance.to_string());
        }
    }
    may_run(state, principal, &instances).await?;
    let caller = super::jobs::access_of_principal(state, principal).await?;
    check_members_editable(state, &caller, conn, name, definition, options).await?;
    let run_id = match run_id {
        Some(id) if crate::engine_stream::valid_run_id(&id) => id,
        Some(_) => {
            return Err(ApiError::new(
                400,
                "`run_id` must be up to 200 letters, digits, '-', '_', '.' or ':', starting with a letter or digit.",
            ));
        }
        None => new_run_id(),
    };
    let mut sysargs: Vec<String> = [
        "sync",
        "pipelines",
        name,
        "-i",
        &registry,
        "--run-id",
        &run_id,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for (flag, value) in [("--begin", &options.begin), ("--end", &options.end)] {
        if let Some(value) = value {
            sysargs.push(flag.into());
            sysargs.push(value.clone());
        }
    }
    // Job metadata is the durable pre-header reservation. Its deterministic name plus
    // the launch lease keeps two requests/workers from accepting the same run ID.
    use sha2::Digest;
    let digest = sha2::Sha256::digest(format!("{registry}|{run_id}"));
    let suffix: String = digest
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let job = format!("execute-{suffix}");
    let metadata = json!({
        "origin": "api_pipeline_sync",
        "run_id": run_id,
        "pipeline": {
            "name": name,
            "registry": registry,
            "id": definition.get("id"),
            "revision": definition.get("revision"),
        },
    });
    // Once accepted, finish the bounded launch even if its HTTP request is cancelled.
    // No provisional running history is written: SSE waits for the runner's trusted header.
    let started = tokio::spawn(launch_reserved_run(
        state.clone(),
        principal.cloned(),
        conn.clone(),
        run_id.clone(),
        job.clone(),
        sysargs,
        metadata,
    ))
    .await
    .map_err(|_| ApiError::new(503, "The Pipeline launch was interrupted."))??;
    if !started.success {
        return Err(ApiError::new(500, started.message));
    }
    let mut fields = JsonMap::new();
    fields.insert("run_id".into(), json!(run_id));
    fields.insert("job".into(), json!(job));
    // The run's events (Server-Sent Events), when the `engine` group is served.
    let stream = if state.config.route_group_is_allowed("engine") {
        let wait = RUN_STREAM_WAIT_SECONDS.to_string();
        let own = crate::engine_stream::own_instance_names(state).contains(&registry);
        let mut query = vec![("wait", wait.as_str())];
        if !own {
            query.push(("instance_keys", registry.as_str()));
        }
        json!(format!(
            "/engine/runs/{}/events/stream?{}",
            crate::json::quote_segment(&run_id),
            crate::json::query_string(&query)
        ))
    } else {
        Json::Null
    };
    fields.insert("stream".into(), stream);
    let logs = super::execute::log_routes(state, &job);
    fields.insert(
        "log_stream".into(),
        logs.get("stream").cloned().unwrap_or(Json::Null),
    );
    fields.insert(
        "logs".into(),
        logs.get("logs").cloned().unwrap_or(Json::Null),
    );
    Ok(StartedSync {
        run_id,
        job,
        fields,
        message: started.message,
    })
}

/// `POST /pipelines/{name}/sync`: start a run in the background.
async fn sync(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller =
        access::require(&ctx.state, &ctx.headers, &["pipes:write", "jobs:execute"]).await?;
    let principal = caller.auth.clone();
    let body = optional_body(body).await?;
    let options = bounds_of(&body)?;
    let run_id = match body.get("run_id") {
        None | Some(Json::Null) => None,
        Some(Json::String(s)) => Some(s.clone()),
        Some(_) => return Err(ApiError::new(400, "`run_id` must be a string.")),
    };
    let conn = connector(&ctx)?;
    let (definition, _) = visible_registration(&ctx.state, &caller, &conn, &name).await?;
    let started = start_sync(
        &ctx.state,
        principal.as_ref(),
        &conn,
        &name,
        &definition,
        &options,
        run_id,
    )
    .await?;
    Ok(envelope::ok(
        format!(
            "Started Pipeline '{name}' (run '{}') as job '{}'.",
            started.run_id, started.job
        ),
        started.fields,
    ))
}

/// Refuse members of `definition` that `caller` cannot see (other than those in `allowed`).
async fn check_new_members(
    filter: &mut RefFilter,
    definition: Option<&Value>,
    allowed: &[String],
) -> Result<(), ApiError> {
    let targets = definition
        .map(|d| targets_of(&from_core(d)))
        .unwrap_or_default();
    for target in targets {
        if !allowed.contains(&target) && !filter.visible(&target).await {
            return Err(ApiError::new(
                403,
                format!("Refusing a Pipeline member you cannot see ('{target}')."),
            ));
        }
    }
    Ok(())
}

async fn register(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, &["pipes:register"]).await?;
    if !ctx.state.config.registration_pipes {
        return Err(ApiError::new(403, "Pipe registration is disabled."));
    }
    let conn = connector(&ctx)?;
    access::guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let mut pipe = metadata(&ctx, &conn, &name)?;
    let mut requested = parameters(body, &name).await?;
    let registry = conn.connector_keys();
    canonicalize_client_members(&ctx, &registry, &mut requested);
    if let Some(Value::Map(definition)) = requested.get_mut("pipeline") {
        definition.insert("id".into(), Value::Str(uuid::Uuid::new_v4().to_string()));
        definition.insert("name".into(), Value::Str(name.clone()));
    }
    check_new_members(
        &mut RefFilter::new(&ctx.state, &caller, &registry),
        requested.get("pipeline"),
        &[],
    )
    .await?;
    pipe.set_parameters(access::guard::stamp_new_pipe(&caller, &requested)?);
    if conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| failed("get_pipe_id", e))?
        .is_some()
    {
        return Err(ApiError::new(
            409,
            format!("Pipeline '{name}' is already registered."),
        ));
    }
    let res = conn.register_pipe(&pipe).await;
    ctx.state.access_cache().forget_pipes();
    envelope::tuple(&res, 500)
}

async fn edit(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require_fresh(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let conn = connector(&ctx)?;
    access::guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let mut pipe = metadata(&ctx, &conn, &name)?;
    let Some(old) = registration_parameters(&ctx.state, &conn, &name).await? else {
        return Err(ApiError::new(
            404,
            format!("Pipeline '{name}' is not registered."),
        ));
    };
    if !caller.can_see(&old) {
        return Err(ApiError::new(
            404,
            format!("Pipeline '{name}' is not registered."),
        ));
    }
    if !caller.can_edit(&old) {
        return Err(ApiError::new(
            403,
            format!(
                "You cannot change Pipeline '{name}': only its owner, its team's managers and administrators may."
            ),
        ));
    }
    let patch = ctx.query.bool("patch", false)?;
    let mut requested = parameters(body, &name).await?;
    canonicalize_client_members(&ctx, &conn.connector_keys(), &mut requested);
    let requested = if patch {
        meerschaum_core::value::apply_patch_maps(&old, &requested)
    } else {
        requested
    };
    let mut new = access::guard::merge_edit(&caller, &old, requested)?;
    if let Some(Value::Map(definition)) = new.get_mut("pipeline") {
        let identity = old
            .get("pipeline")
            .and_then(|pipeline| pipeline.get("id"))
            .and_then(Value::as_str)
            .filter(|identity| !identity.is_empty() && !identity.starts_with("legacy:"));
        if let Some(identity) = identity {
            if definition
                .get("id")
                .is_some_and(|requested| requested.as_str() != Some(identity))
            {
                return Err(ApiError::new(
                    409,
                    "A saved Pipeline identity cannot be replaced.",
                ));
            }
            definition.insert("id".into(), Value::Str(identity.to_string()));
        } else {
            definition.insert("id".into(), Value::Str(uuid::Uuid::new_v4().to_string()));
        }
    }
    // Members the caller cannot see stay (it could not have meant to drop them); a new member
    // must be one it can see.
    let registry = conn.connector_keys();
    let mut filter = RefFilter::new(&ctx.state, &caller, &registry);
    let old_targets = old
        .get("pipeline")
        .map(|d| targets_of(&from_core(d)))
        .unwrap_or_default();
    let mut hidden_old = Vec::new();
    for target in &old_targets {
        if !filter.visible(target).await {
            hidden_old.push(target.clone());
        }
    }
    check_new_members(&mut filter, new.get("pipeline"), &old_targets).await?;
    if !hidden_old.is_empty()
        && let Some(Value::Map(definition)) = new.get_mut("pipeline")
    {
        let mut targets: Vec<Value> = match definition.get("targets") {
            Some(Value::List(items)) => items.clone(),
            _ => Vec::new(),
        };
        for target in hidden_old {
            if !targets.iter().any(|t| t.py_str() == target) {
                targets.push(Value::Str(target));
            }
        }
        definition.insert("targets".into(), Value::List(targets));
    }
    pipe.set_parameters(new);
    let res = conn.edit_pipe(&pipe, false).await;
    ctx.state.access_cache().forget_pipes();
    envelope::tuple(&res, 500)
}

#[derive(serde::Deserialize)]
struct RenameBody {
    new_name: String,
    #[serde(default)]
    expected_revision: Option<i64>,
    #[serde(default)]
    expected_id: Option<String>,
}

/// Transfer only a live authorized registration; old UUIDs cannot be supplied for creation.
async fn rename(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require_fresh(
        &ctx.state,
        &ctx.headers,
        &[
            "pipes:read",
            "pipes:write",
            "pipes:register",
            "pipes:delete",
        ],
    )
    .await?;
    validate_name(&name)?;
    if !ctx.state.config.registration_pipes {
        return Err(ApiError::new(403, "Pipe registration is disabled."));
    }
    let request: RenameBody = serde_json::from_slice(&body_bytes(body).await?).map_err(|_| {
        ApiError::new(
            400,
            "Expected a destination name and optional source identity/revision.",
        )
    })?;
    validate_name(&request.new_name)?;
    let conn = connector(&ctx)?;
    access::guard::check_connector_teams(&ctx.state, &caller, &conn.connector_keys())?;
    let source = metadata(&ctx, &conn, &name)?;
    let expected = conn
        .get_pipe_attributes(&source)
        .await
        .map_err(|_| ApiError::new(503, "Cannot read Pipeline registration."))?;
    let parameters = crate::state::parameters_of(&expected)
        .filter(|parameters| caller.can_see(parameters))
        .ok_or_else(|| ApiError::new(404, "Pipeline was not found."))?;
    if !caller.can_edit(&parameters) {
        return Err(ApiError::new(403, "You cannot rename this Pipeline."));
    }
    for operation in [
        meerschaum_core::access::LockOp::EditParameters,
        meerschaum_core::access::LockOp::DeleteAndDrop,
    ] {
        if let Some(refusal) =
            meerschaum_connector::locks::refusal_of(&source, Some(&parameters), operation)
        {
            return Err(ApiError::new(403, refusal.message));
        }
    }
    let Some(Value::Map(current)) = parameters.get("pipeline") else {
        return Err(ApiError::new(409, "Pipeline definition is unavailable."));
    };
    let old_id = current.get("id").and_then(Value::as_str);
    let established =
        old_id.filter(|identity| !identity.is_empty() && !identity.starts_with("legacy:"));
    if request.expected_id.as_deref().is_some_and(|identity| {
        established.is_some_and(|current| identity != current)
            || (established.is_none()
                && !identity.starts_with("legacy:")
                && old_id != Some(identity))
    }) {
        return Err(ApiError::new(
            409,
            "Pipeline identity changed; reload before renaming.",
        ));
    }
    let revision = current.get("revision").and_then(Value::as_i64).unwrap_or(0);
    if request
        .expected_revision
        .is_some_and(|expected| expected != revision)
    {
        return Err(ApiError::new(
            409,
            "Pipeline revision changed; reload before renaming.",
        ));
    }
    let (jobs, hidden) = referencing_jobs(&ctx, &conn, &name, &caller).await;
    if !jobs.is_empty() || hidden > 0 {
        return Err(ApiError::new(
            409,
            "Remove or reassign referencing scheduled Jobs before renaming.",
        ));
    }
    if request.new_name == name {
        let visible = visible_definition(
            &mut RefFilter::new(&ctx.state, &caller, &conn.connector_keys()),
            from_core(&Value::Map(current.clone())),
        )
        .await;
        return Ok(envelope::ok_with(
            "Pipeline name is unchanged.",
            "pipeline",
            visible,
        ));
    }
    let mut updated = parameters.clone();
    let Some(Value::Map(definition)) = updated.get_mut("pipeline") else {
        unreachable!("checked");
    };
    definition.insert("name".into(), Value::Str(request.new_name.clone()));
    definition.insert("registry".into(), Value::Str(conn.connector_keys()));
    definition.insert(
        "revision".into(),
        Value::Int(
            revision
                .checked_add(1)
                .ok_or_else(|| ApiError::new(409, "Pipeline revision is out of range."))?,
        ),
    );
    definition.insert(
        "id".into(),
        Value::Str(
            established
                .map(str::to_string)
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        ),
    );
    if !conn
        .move_pipeline_registration(&source, &request.new_name, &expected, &updated)
        .await
        .map_err(|_| ApiError::new(503, "Cannot atomically rename this Pipeline."))?
    {
        return Err(ApiError::new(
            409,
            "Pipeline source changed or destination already exists.",
        ));
    }
    ctx.state.access_cache().forget_pipes();
    let definition = updated.get("pipeline").map(from_core).unwrap_or(Json::Null);
    let visible = visible_definition(
        &mut RefFilter::new(&ctx.state, &caller, &conn.connector_keys()),
        definition,
    )
    .await;
    Ok(envelope::ok_with(
        "Renamed the live Pipeline registration.",
        "pipeline",
        visible,
    ))
}

/// The scheduled jobs that run Pipeline `name`: the ones `caller` may see (sorted), and how many
/// others there are (a job of another team is only counted, rc15 AC9).
async fn referencing_jobs(
    ctx: &Ctx,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
    caller: &Access,
) -> (Vec<String>, usize) {
    let mut found = Vec::new();
    let mut hidden = 0;
    for (job_name, meta) in ctx.state.jobs.list().await {
        let Some(args) = meta.get("sysargs").and_then(Json::as_array) else {
            continue;
        };
        let args: Vec<_> = args.iter().filter_map(Json::as_str).collect();
        if !args
            .iter()
            .any(|arg| ["--schedule", "-s", "--loop"].contains(arg))
        {
            continue;
        }
        for step in args.split(|arg| ["+", ":"].contains(arg)) {
            if step.len() < 3 || step[0] != "sync" || !["pipeline", "pipelines"].contains(&step[1])
            {
                continue;
            }
            let registry = step
                .windows(2)
                .find(|pair| ["-i", "--instance", "--mrsm-instance"].contains(&pair[0]))
                .map(|pair| pair[1]);
            let targets = step[2..].iter().take_while(|arg| !arg.starts_with('-'));
            if targets.into_iter().any(|target| *target == name)
                && registry.unwrap_or(&ctx.state.config.instance_keys) == conn.connector_keys()
            {
                if super::jobs::can_see(caller, &job_name, Some(&meta)) {
                    found.push(job_name.clone());
                } else {
                    hidden += 1;
                }
                break;
            }
        }
    }
    found.sort();
    found.dedup();
    (found, hidden)
}

/// `a, b and 2 hidden jobs` (the jobs the caller may not see are only counted).
fn jobs_label(visible: &[String], hidden: usize) -> String {
    let mut parts: Vec<String> = visible.to_vec();
    if hidden > 0 {
        parts.push(format!(
            "{hidden} hidden job{}",
            if hidden == 1 { "" } else { "s" }
        ));
    }
    match parts.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} and {last}", rest.join(", ")),
        Some((last, _)) => last.clone(),
        None => String::new(),
    }
}

async fn delete(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:delete"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
    let parameters = registration_parameters(&ctx.state, &conn, &name)
        .await?
        .filter(|p| caller.can_see(p))
        .ok_or_else(|| ApiError::new(404, format!("Pipeline '{name}' is not registered.")))?;
    if !caller.can_edit(&parameters) {
        return Err(ApiError::new(
            403,
            format!(
                "You cannot delete Pipeline '{name}': only its owner, its team's managers and administrators may."
            ),
        ));
    }
    let (jobs, hidden_jobs) = referencing_jobs(&ctx, &conn, &name, &caller).await;
    if !jobs.is_empty() || hidden_jobs > 0 {
        return Err(ApiError::new(
            409,
            format!(
                "Remove or reassign the scheduled jobs that run this Pipeline first: {}.",
                jobs_label(&jobs, hidden_jobs)
            ),
        ));
    }
    envelope::tuple(&conn.delete_pipe(&pipe).await, 500)
}

async fn id(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
    visible_registration(&ctx.state, &caller, &conn, &name).await?;
    let id = conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| failed("get_pipe_id", e))?
        .ok_or_else(|| ApiError::new(404, format!("Pipeline '{name}' is not registered.")))?;
    Ok(envelope::ok_with(
        format!("Pipeline '{name}' is registered."),
        "id",
        from_core(&id.to_value()),
    ))
}

async fn attributes(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
    let (definition, _) = visible_registration(&ctx.state, &caller, &conn, &name).await?;
    let mut attrs = crate::state::pipe_attributes(&conn, &pipe).await?;
    // Members follow the caller's access; the declared registry is authoritative for admin
    // transport too, so legacy user-supplied aliases never rebind a foreign member.
    let registry = conn.connector_keys();
    let visible = visible_definition(
        &mut RefFilter::new(&ctx.state, &caller, &registry),
        definition,
    )
    .await;
    if let Some(Value::Map(parameters)) = attrs.get_mut("parameters") {
        parameters.insert("pipeline".into(), crate::json::to_core(&visible));
    }
    // The jobs the caller may see by name; the others only counted, as one last entry (so a
    // client that refuses to rename a Pipeline some job runs still refuses) and in
    // `pipeline_hidden_referencing_jobs`.
    let (mut jobs, hidden_jobs) = referencing_jobs(&ctx, &conn, &name, &caller).await;
    if hidden_jobs > 0 {
        jobs.push(jobs_label(&[], hidden_jobs));
    }
    attrs.insert(
        "pipeline_referencing_jobs".into(),
        Value::List(jobs.into_iter().map(Value::Str).collect()),
    );
    attrs.insert(
        "pipeline_hidden_referencing_jobs".into(),
        Value::Int(hidden_jobs as i64),
    );
    Ok(envelope::ok_with(
        format!("The attributes of Pipeline '{name}'."),
        "attributes",
        from_core(&Value::Map(attrs)),
    ))
}

async fn lease(ctx: Ctx, body: Body, release: bool) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let request: meerschaum_connector_api::models::LeaseRequest =
        serde_json::from_slice(&body_bytes(body).await?)
            .map_err(|e| ApiError::new(400, format!("Invalid lease request: {e}")))?;
    let parts: Vec<_> = request.pipe_ref.splitn(4, '|').collect();
    if parts.len() != 4
        || parts[1] != "mrsm"
        || !["pipeline", "pipeline_registry"].contains(&parts[2])
    {
        return Err(ApiError::new(
            400,
            "Only Pipeline metadata leases are accepted.",
        ));
    }
    let conn = connector(&ctx)?;
    let location = if parts[3] == "None" {
        None
    } else {
        Some(parts[3])
    };
    if let Some(name) = location {
        validate_name(name)?;
        // The lease of a Pipeline's edit: a Pipeline the caller may change.
        if parts[2] == "pipeline"
            && let Some(parameters) = registration_parameters(&ctx.state, &conn, name).await?
            && !caller.can_edit(&parameters)
        {
            return Err(ApiError::new(
                if caller.can_see(&parameters) {
                    403
                } else {
                    404
                },
                format!("You cannot change Pipeline '{name}'."),
            ));
        }
    }
    let pipe = Pipe::builder("mrsm", parts[2])
        .location(location)
        .instance(&conn.connector_keys())
        .build()
        .map_err(|e| ApiError::new(400, e.to_string()))?;
    let success = if release {
        conn.release_pipe_lease(&pipe, &request.holder).await
    } else {
        conn.acquire_pipe_lease(
            &pipe,
            &request.holder,
            Duration::from_millis(request.ttl_ms),
        )
        .await
    }
    .map_err(|e| failed("Pipeline metadata lease", e))?;
    match (success, release) {
        (true, false) => Ok(envelope::ok(
            format!("Acquired the lease on '{}'.", request.pipe_ref),
            JsonMap::new(),
        )),
        (true, true) => Ok(envelope::ok(
            format!("Released the lease on '{}'.", request.pipe_ref),
            JsonMap::new(),
        )),
        (false, false) => Err(ApiError::new(
            409,
            format!(
                "Could not acquire the lease on '{}': another process holds it.",
                request.pipe_ref
            ),
        )),
        (false, true) => Err(ApiError::new(
            409,
            format!(
                "Could not release the lease on '{}': '{}' does not hold it.",
                request.pipe_ref, request.holder
            ),
        )),
    }
}
async fn acquire_lease(ctx: Ctx, body: Body) -> ApiResult {
    lease(ctx, body, false).await
}
async fn release_lease(ctx: Ctx, body: Body) -> ApiResult {
    lease(ctx, body, true).await
}

/// The registration pipe of the Pipeline `name` that `caller` may see, with its parameters.
async fn visible_pipeline_pipe(
    ctx: &Ctx,
    caller: &Access,
    name: &str,
) -> Result<(Arc<dyn InstanceConnector>, Pipe, Map), ApiError> {
    let conn = connector(ctx)?;
    let (_, parameters) = visible_registration(&ctx.state, caller, &conn, name).await?;
    let mut pipe = registration_pipe(&ctx.state, &conn, name)?;
    pipe.set_parameters(parameters.clone());
    Ok((conn, pipe, parameters))
}

async fn get_access(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let (_, _, parameters) = visible_pipeline_pipe(&ctx, &caller, &name).await?;
    Ok(envelope::ok(
        format!("The access of Pipeline '{name}'."),
        super::pipe_access::access_fields(&caller, &parameters),
    ))
}

async fn patch_access(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let body = optional_body(body).await?;
    let body = if body.is_null() { json!({}) } else { body };
    let (conn, pipe, parameters) = visible_pipeline_pipe(&ctx, &caller, &name).await?;
    let new = super::pipe_access::changed_access(&caller, &parameters, &body)?;
    let res = conn.set_pipe_access(&pipe, &new).await;
    super::pipe_access::forget(&ctx.state, &conn).await;
    if !res.success {
        return Err(super::pipe_access::change_failed(&res));
    }
    let parameters = super::pipe_access::registered(&conn, &pipe).await?;
    Ok(envelope::ok(
        res.message,
        super::pipe_access::access_fields(&caller, &parameters),
    ))
}

async fn lock_or_unlock(ctx: Ctx, name: String, body: Body, lock: bool) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let body = optional_body(body).await?;
    let requested = super::pipe_access::requested_locks(&body)?;
    let (conn, pipe, parameters) = visible_pipeline_pipe(&ctx, &caller, &name).await?;
    super::pipe_access::check_may_lock(&caller, &pipe, &parameters, lock)?;
    let res = if lock {
        conn.lock_pipe(&pipe, &requested).await
    } else {
        conn.unlock_pipe(&pipe, &requested).await
    };
    super::pipe_access::forget(&ctx.state, &conn).await;
    if !res.success {
        return Err(super::pipe_access::change_failed(&res));
    }
    let parameters = super::pipe_access::registered(&conn, &pipe).await?;
    Ok(envelope::ok(
        res.message,
        super::pipe_access::access_fields(&caller, &parameters),
    ))
}

async fn lock(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    lock_or_unlock(ctx, name, body, true).await
}

async fn unlock(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    lock_or_unlock(ctx, name, body, false).await
}
