//! Narrow metadata transport for registered Pipelines. Generic `mrsm` Pipe data stays private.
//! The CLI and SDK own lifecycle validation, tag approval, and schedule checks; these routes
//! transport only the `mrsm/pipeline/<name>` registration's raw parameters.
use super::{Ctx, body_bytes};
use crate::{
    auth::scoped_auth,
    config::ServerConfig,
    error::{ApiError, ApiResult},
    json::{from_core, ok_json, to_core, tuple_response},
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
use serde_json::{Value as Json, json};
use std::sync::Arc;
use std::time::Duration;

/// Metadata routes deliberately expose no data, SQL, sync, drop, or arbitrary internal metric.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/pipelines", get(list))
        .route("/pipelines/keys", get(keys))
        .route(
            "/pipelines/leases",
            post(acquire_lease).delete(release_lease),
        )
        .route("/pipelines/{name}/register", post(register))
        .route("/pipelines/{name}/edit", axum::routing::patch(edit))
        .route("/pipelines/{name}/delete", axum::routing::delete(delete))
        .route("/pipelines/{name}/id", get(id))
        .route("/pipelines/{name}/attributes", get(attributes))
}

fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty()
        || name.len() > 128
        || !name.as_bytes()[0].is_ascii_alphanumeric()
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
    {
        return Err(ApiError::new(422, "Invalid Pipeline name."));
    }
    Ok(())
}

fn connector(ctx: &Ctx) -> Result<Arc<dyn InstanceConnector>, ApiError> {
    let instance = ctx
        .instance_keys()
        .or_else(|| ctx.query.opt_str("instance"));
    ctx.state.get_api_connector(instance.as_deref())
}

fn metadata(ctx: &Ctx, conn: &Arc<dyn InstanceConnector>, name: &str) -> Result<Pipe, ApiError> {
    validate_name(name)?;
    Pipe::builder("mrsm", "pipeline")
        .location(Some(name))
        .instance(&conn.connector_keys())
        .defaults(ctx.state.config.pipe_defaults.clone())
        .build()
        .map_err(|e| ApiError::new(422, e.to_string()))
}

async fn parameters(body: Body, name: &str) -> Result<Map, ApiError> {
    let body = super::pipes::parse_json_body(&body_bytes(body).await?)?
        .ok_or_else(|| ApiError::missing(&["body"]))?;
    let Json::Object(_) = body else {
        return Err(ApiError::new(
            422,
            "Expected raw Pipeline parameters object.",
        ));
    };
    let Some(definition) = body.get("pipeline").and_then(Json::as_object) else {
        return Err(ApiError::new(
            422,
            "Parameters must contain a Pipeline definition object.",
        ));
    };
    if definition
        .get("name")
        .and_then(Json::as_str)
        .is_some_and(|value| value != name)
    {
        return Err(ApiError::new(
            422,
            "Pipeline definition name must match the route.",
        ));
    }
    if definition.get("targets").is_some_and(|targets| {
        !targets
            .as_array()
            .is_some_and(|rows| rows.iter().all(Json::is_string))
    }) {
        return Err(ApiError::new(
            422,
            "Pipeline targets must be exact reference strings.",
        ));
    }
    Ok(to_core(&body).to_map())
}

async fn list(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let listing = conn
        .fetch_pipes_keys(&PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await
        .map_err(|e| ApiError::new(500, e.to_string()))?;
    let mut result = serde_json::Map::new();
    for row in listing.entries() {
        if let Some(name) = &row.location_key {
            let pipe = metadata(&ctx, &conn, name)?;
            let attrs = conn
                .get_pipe_attributes(&pipe)
                .await
                .map_err(|e| ApiError::new(500, e.to_string()))?;
            if let Some(definition) = attrs
                .get("parameters")
                .and_then(Value::as_map)
                .and_then(|p| p.get("pipeline"))
            {
                result.insert(name.clone(), from_core(definition));
            }
        }
    }
    Ok(ok_json(&Json::Object(result)))
}

async fn keys(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let decode = |key: &str| -> Result<Vec<Value>, ApiError> {
        match Value::from_json_str(&ctx.query.str_or(key, "[]")) {
            Ok(Value::List(items)) => Ok(items),
            Ok(Value::Null) => Ok(Vec::new()),
            _ => Err(ApiError::new(422, format!("Invalid {key} filter"))),
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
            .map_err(|e| ApiError::new(422, e.to_string()))?
            .and_then(|value| value.as_map().cloned()),
    };
    let keys = conn
        .fetch_pipes_keys(&query)
        .await
        .map_err(|e| ApiError::new(500, e.to_string()))?;
    let rows: Vec<Json> = keys
        .entries()
        .iter()
        .map(|row| from_core(&Value::List(row.to_list())))
        .collect();
    Ok(ok_json(&json!(rows)))
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
        .map_err(|e| ApiError::new(500, e.to_string()))?
        .is_some()
    {
        return Err(ApiError::new(409, "Pipeline is already registered."));
    }
    Ok(tuple_response(&conn.register_pipe(&pipe).await))
}

async fn edit(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let conn = connector(&ctx)?;
    let mut pipe = metadata(&ctx, &conn, &name)?;
    if conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| ApiError::new(500, e.to_string()))?
        .is_none()
    {
        return Err(ApiError::new(409, "Pipeline is not registered."));
    }
    pipe.set_parameters(parameters(body, &name).await?);
    Ok(tuple_response(
        &conn.edit_pipe(&pipe, ctx.query.bool("patch", false)?).await,
    ))
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
    if !referencing_jobs(&ctx, &conn, &name).await.is_empty() {
        return Err(ApiError::new(
            409,
            "Remove or reassign scheduled Jobs referencing this Pipeline first.",
        ));
    }
    Ok(tuple_response(&conn.delete_pipe(&pipe).await))
}

async fn id(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
    let id = conn
        .get_pipe_id(&pipe)
        .await
        .map_err(|e| ApiError::new(500, e.to_string()))?
        .ok_or_else(|| ApiError::new(404, "Pipeline is not registered."))?;
    Ok(ok_json(&from_core(&id.to_value())))
}

async fn attributes(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:read"]).await?;
    let conn = connector(&ctx)?;
    let pipe = metadata(&ctx, &conn, &name)?;
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
    Ok(ok_json(&from_core(&Value::Map(attrs))))
}

async fn lease(ctx: Ctx, body: Body, release: bool) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["pipes:write"]).await?;
    let request: meerschaum_connector_api::models::LeaseRequest =
        serde_json::from_slice(&body_bytes(body).await?)
            .map_err(|e| ApiError::new(422, e.to_string()))?;
    let parts: Vec<_> = request.pipe_ref.splitn(4, '|').collect();
    if parts.len() != 4
        || parts[1] != "mrsm"
        || !["pipeline", "pipeline_registry"].contains(&parts[2])
    {
        return Err(ApiError::new(
            422,
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
        .map_err(|e| ApiError::new(422, e.to_string()))?;
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
    .map_err(|e| ApiError::new(500, e.to_string()))?;
    Ok(ok_json(&json!({"success": success})))
}
async fn acquire_lease(ctx: Ctx, body: Body) -> ApiResult {
    lease(ctx, body, false).await
}
async fn release_lease(ctx: Ctx, body: Body) -> ApiResult {
    lease(ctx, body, true).await
}
