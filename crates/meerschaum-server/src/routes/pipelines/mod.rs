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
//! | `POST /pipelines/{name}/register` | `pipes:register` | Register a definition (`409` if it exists) |
//! | `PATCH /pipelines/{name}/edit` | `pipes:write` | Replace (or `patch`) a definition (`404` if missing) |
//! | `DELETE /pipelines/{name}/delete` | `pipes:delete` | Delete a definition (`409` while scheduled jobs use it) |
//! | `GET /pipelines/{name}/id`, `/attributes` | `pipes:read` | The registration's ID and raw attributes |
//! | `POST`/`DELETE /pipelines/leases` | `pipes:write` | The metadata leases of Pipeline edits |
//! | `GET`/`PATCH /pipelines/{name}/access`, `POST .../lock`, `.../unlock` | `pipes:read` / `pipes:write` | The Pipeline's access and locks (as for pipes, [`super::pipe_access`]) |
//!
//! **Access.** A saved Pipeline carries `access` and `locks` like a pipe (its registration
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

mod catalog;
mod locks;
mod plan;
mod registration;
mod run;

// The handlers in the submodules name these sibling route modules as `super::...`.
use super::{Ctx, body_bytes, execute, jobs, pipe_access};
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

use catalog::{detail, keys, list};
pub use catalog::{
    last_run, list_definitions, list_registrations, member, registration_parameters, store_of,
    targets_of, visible_definition, visible_registration,
};
use locks::{acquire_lease, get_access, lock, patch_access, release_lease, unlock};
use plan::plan;
pub use plan::{bounds_of, plan_error, visible_plan};
use registration::{attributes, delete, edit, id, register, rename};
use run::sync;
pub use run::{StartedSync, check_members_editable, new_run_id, start_sync};

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

async fn parameters(body: Body, name: &str, limit: Option<usize>) -> Result<Map, ApiError> {
    let body = super::pipes::parse_json_body(&body_bytes(body, limit).await?)?
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
    if definition
        .get("graph")
        .is_some_and(|value| !value.is_null() && !value.is_boolean())
    {
        return Err(ApiError::new(
            400,
            "Pipeline graph must be a boolean or null.",
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

async fn optional_body(body: Body, limit: Option<usize>) -> Result<Json, ApiError> {
    let body =
        super::pipes::parse_json_body(&body_bytes(body, limit).await?)?.unwrap_or(Json::Null);
    match body {
        Json::Null | Json::Object(_) => Ok(body),
        _ => Err(ApiError::new(
            400,
            "The request body must be a JSON object.",
        )),
    }
}
