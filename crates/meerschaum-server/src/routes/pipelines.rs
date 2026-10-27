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
//!
//! The metadata routes transport only the `mrsm/pipeline/<name>` registration's raw parameters;
//! the CLI and SDK own lifecycle validation, tag approval and schedule checks. Generic `mrsm`
//! pipe data stays private.
use super::{Ctx, body_bytes};
use crate::{
    auth::{Principal, is_admin, scoped_auth},
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
        .route("/pipelines/{name}/delete", axum::routing::delete(delete))
        .route("/pipelines/{name}/id", get(id))
        .route("/pipelines/{name}/attributes", get(attributes))
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

async fn require_definition(
    ctx: &Ctx,
    conn: &Arc<dyn InstanceConnector>,
    name: &str,
) -> Result<Json, ApiError> {
    require_definition_on(&ctx.state, conn, name).await
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
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let pipelines = list_definitions(&ctx.state, &conn).await?;
    let message = format!(
        "Found {} on '{}'.",
        plural(pipelines.len(), "Pipeline", "Pipelines"),
        conn.connector_keys()
    );
    Ok(envelope::ok_with(message, "pipelines", json!(pipelines)))
}

async fn keys(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
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
        .fetch_pipes_keys(&query)
        .await
        .map_err(|e| failed("fetch_pipes_keys", e))?;
    let rows: Vec<Json> = keys
        .entries()
        .iter()
        .map(|row| from_core(&Value::List(row.to_list())))
        .collect();
    Ok(envelope::ok_with(
        format!("Found {}.", plural(rows.len(), "Pipeline", "Pipelines")),
        "keys",
        json!(rows),
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
pub async fn last_run(state: &AppState, conn: &Arc<dyn InstanceConnector>, name: &str) -> Json {
    let store = store_of(state, conn);
    match store
        .get_runs(&RunsFilter {
            pipeline: Some(name.to_string()),
            limit: Some(1),
            ..Default::default()
        })
        .await
    {
        Ok(runs) => runs
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
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let definition = require_definition(&ctx, &conn, &name).await?;
    let mut members = Vec::new();
    for reference in targets_of(&definition) {
        members.push(member(&ctx.state, &reference).await);
    }
    let last_run = last_run(&ctx.state, &conn, &name).await;
    let mut fields = JsonMap::new();
    let message = format!(
        "Pipeline '{name}' on '{}' has {}.",
        conn.connector_keys(),
        plural(members.len(), "member", "members")
    );
    fields.insert("pipeline".into(), definition);
    fields.insert("members".into(), json!(members));
    fields.insert("last_run".into(), last_run);
    Ok(envelope::ok(message, fields))
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
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let body = optional_body(body).await?;
    let options = bounds_of(&body)?;
    let conn = connector(&ctx)?;
    require_definition(&ctx, &conn, &name).await?;
    let planned = ctx
        .state
        .pipelines
        .plan(&name, &conn.connector_keys(), &options)
        .await
        .map_err(plan_error)?;
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
    let job = super::execute::new_execution_name();
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
    let started =
        super::execute::start_execution(state, principal, &job, &sysargs, metadata).await?;
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
    let principal = scoped_auth(&ctx.state, &ctx.headers, &["pipes:write", "jobs:execute"]).await?;
    let body = optional_body(body).await?;
    let options = bounds_of(&body)?;
    let run_id = match body.get("run_id") {
        None | Some(Json::Null) => None,
        Some(Json::String(s)) => Some(s.clone()),
        Some(_) => return Err(ApiError::new(400, "`run_id` must be a string.")),
    };
    let conn = connector(&ctx)?;
    let definition = require_definition(&ctx, &conn, &name).await?;
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

async fn register(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    if !ctx.state.config.registration_pipes {
        return Err(ApiError::new(403, "Pipe registration is disabled."));
    }
    let conn = connector(&ctx)?;
    let mut pipe = metadata(&ctx, &conn, &name)?;
    pipe.set_parameters(parameters(body, &name).await?);
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
    envelope::tuple(&conn.register_pipe(&pipe).await, 500)
}

async fn edit(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let conn = connector(&ctx)?;
    let mut pipe = metadata(&ctx, &conn, &name)?;
    if conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| failed("get_pipe_id", e))?
        .is_none()
    {
        return Err(ApiError::new(
            404,
            format!("Pipeline '{name}' is not registered."),
        ));
    }
    pipe.set_parameters(parameters(body, &name).await?);
    envelope::tuple(
        &conn.edit_pipe(&pipe, ctx.query.bool("patch", false)?).await,
        500,
    )
}

async fn referencing_jobs(ctx: &Ctx, conn: &Arc<dyn InstanceConnector>, name: &str) -> Vec<String> {
    let mut found = Vec::new();
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
                found.push(job_name.clone());
                break;
            }
        }
    }
    found.sort();
    found.dedup();
    found
}

async fn delete(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:delete"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
    if conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| failed("get_pipe_id", e))?
        .is_none()
    {
        return Err(ApiError::new(
            404,
            format!("Pipeline '{name}' is not registered."),
        ));
    }
    let jobs = referencing_jobs(&ctx, &conn, &name).await;
    if !jobs.is_empty() {
        return Err(ApiError::new(
            409,
            format!(
                "Remove or reassign the scheduled jobs that run this Pipeline first: {}.",
                jobs.join(", ")
            ),
        ));
    }
    envelope::tuple(&conn.delete_pipe(&pipe).await, 500)
}

async fn id(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
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
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
    if conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| failed("get_pipe_id", e))?
        .is_none()
    {
        return Err(ApiError::new(
            404,
            format!("Pipeline '{name}' is not registered."),
        ));
    }
    let mut attrs = crate::state::pipe_attributes(&conn, &pipe).await?;
    attrs.insert(
        "pipeline_referencing_jobs".into(),
        Value::List(
            referencing_jobs(&ctx, &conn, &name)
                .await
                .into_iter()
                .map(Value::Str)
                .collect(),
        ),
    );
    Ok(envelope::ok_with(
        format!("The attributes of Pipeline '{name}'."),
        "attributes",
        from_core(&Value::Map(attrs)),
    ))
}

async fn lease(ctx: Ctx, body: Body, release: bool) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
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
