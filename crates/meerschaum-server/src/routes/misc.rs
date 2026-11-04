//! `/`, `/favicon.ico`, `/chaining`, `/info`, `/healthcheck` and (with `--debug`) `/id`
//! (`api/routes/_index.py`, `_misc.py`), plus v5's `/healthcheck/ready`.

use axum::Router;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use meerschaum_connector::PipesKeysQuery;
use serde_json::json;

use super::{Ctx, redirect};
use crate::auth::scoped_auth_if_private;
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{json_response, ok_json};
use crate::state::AppState;

/// Register the `index` route group (`/`).
pub fn index_routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/", get(index))
}

/// Register the `misc` route group. The public preset keeps only `/healthcheck`,
/// `/healthcheck/ready` and `/favicon.ico` (unless the allowlist names `misc`).
pub fn routes(app: Router<AppState>, cfg: &ServerConfig) -> Router<AppState> {
    let app = app
        .route("/favicon.ico", get(get_favicon))
        .route("/healthcheck", get(get_healthcheck))
        .route(READY_PATH, get(get_ready));
    if !cfg.serves_full_misc() {
        return app;
    }
    let app = app
        .route("/chaining", get(get_chaining_status))
        .route("/info", get(get_instance_info));
    if cfg.debug {
        app.route("/id", get(get_ids))
    } else {
        app
    }
}

/// `INDEX_REDIRECT_URL`: the console when the Dash app is served, else the docs (or the
/// OpenAPI document when docs are disabled).
pub fn index_redirect_url(cfg: &ServerConfig) -> &'static str {
    if !cfg.no_dash {
        if cfg.route_group_is_allowed("dash") {
            "/dash"
        } else {
            "/dash/login"
        }
    } else if cfg.docs_enabled {
        "/docs"
    } else {
        "/openapi.json"
    }
}

async fn index(ctx: Ctx) -> ApiResult {
    Ok(redirect(index_redirect_url(&ctx.state.config)))
}

async fn get_favicon(ctx: Ctx) -> ApiResult {
    let Some(path) = ctx
        .state
        .config
        .static_path
        .as_ref()
        .map(|p| p.join("ico").join("logo.ico"))
    else {
        // PARITY: `FileResponse` on a missing file raises (a 500).
        return Err(ApiError::internal());
    };
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|e| ApiError::exception("Failed to read the favicon", e))?;
    let mut resp = (StatusCode::OK, bytes).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("image/vnd.microsoft.icon"),
    );
    Ok(resp)
}

async fn get_chaining_status(ctx: Ctx) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["instance:read"]).await?;
    Ok(ok_json(&json!(ctx.state.config.chaining_child_apis)))
}

async fn get_instance_info(ctx: Ctx) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["instance:read"]).await?;
    let conn = ctx
        .state
        .get_api_connector(ctx.instance_keys().as_deref())?;
    let num_plugins = conn
        .get_plugins(None, None)
        .await
        .map_err(|e| ApiError::exception("get_plugins", e))?
        .len();
    let num_users = conn
        .get_users()
        .await
        .map_err(|e| ApiError::exception("get_users", e))?
        .len();
    let num_pipes = conn
        .fetch_pipes_keys(&PipesKeysQuery::default())
        .await
        .map_err(|e| ApiError::exception("fetch_pipes_keys", e))?
        .len();
    Ok(ok_json(&json!({
        "version": ctx.state.config.version,
        "num_plugins": num_plugins,
        "num_users": num_users,
        "num_pipes": num_pipes,
    })))
}

async fn get_healthcheck(ctx: Ctx) -> ApiResult {
    let conn = ctx
        .state
        .get_api_connector(ctx.instance_keys().as_deref())?;
    let success = conn.test_connection().await.unwrap_or(false);
    let message = if success {
        "Success"
    } else {
        "Failed to connect to instance connector."
    };
    let status = if success {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    Ok(json_response(
        status,
        &json!({"success": success, "message": message}),
    ))
}

/// `GET /healthcheck/ready` (v5): whether this server should get new requests. `200` only once
/// it serves everything it mounts; `503` while it is still starting, while the Python API host
/// it needs (for Python plugins and the web console) is not serving (not started yet, refused
/// to start, restarting or not answering its health checks: no timeout turns this into a `200`),
/// while it drains (shutting down: see [`crate::lifecycle`]), or when its instance does not
/// answer. The body is `{"success", "message"}`, plus `"reason"` on a `503` (see
/// [`NotReady::reason`]). `/healthcheck` is unchanged.
pub const READY_PATH: &str = "/healthcheck/ready";

/// The message of `/healthcheck/ready` while the server drains.
pub const SHUTTING_DOWN_MESSAGE: &str = "The server is shutting down.";

/// The message of `/healthcheck/ready` while the server starts.
pub const STARTING_MESSAGE: &str = "The server is starting.";

/// The message of `/healthcheck/ready` while the API host the server needs is not serving.
pub const HOST_UNAVAILABLE_MESSAGE: &str =
    "The API host (Python plugins and the web console) is not serving.";

/// The message of `/healthcheck/ready` when the instance does not answer.
pub const INSTANCE_UNAVAILABLE_MESSAGE: &str = "Failed to connect to instance connector.";

/// Why `/healthcheck/ready` answers `503`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotReady {
    /// The server drains (shutting down).
    Draining,
    /// The server is starting (its API host has not answered for the first time yet).
    Starting,
    /// The API host the server needs is not serving.
    HostUnavailable,
    /// The instance connector does not answer.
    InstanceUnavailable,
}

impl NotReady {
    /// The body's `reason`: `draining`, `starting`, `host_unavailable` or
    /// `instance_unavailable`.
    pub fn reason(self) -> &'static str {
        match self {
            NotReady::Draining => "draining",
            NotReady::Starting => "starting",
            NotReady::HostUnavailable => "host_unavailable",
            NotReady::InstanceUnavailable => "instance_unavailable",
        }
    }

    /// The body's `message`.
    pub fn message(self) -> &'static str {
        match self {
            NotReady::Draining => SHUTTING_DOWN_MESSAGE,
            NotReady::Starting => STARTING_MESSAGE,
            NotReady::HostUnavailable => HOST_UNAVAILABLE_MESSAGE,
            NotReady::InstanceUnavailable => INSTANCE_UNAVAILABLE_MESSAGE,
        }
    }
}

/// Whether the server is ready, short of asking its instance: `Some` reason when it drains, is
/// starting, or needs an API host that is not serving. A host that is needed and not serving is
/// started in the background ([`crate::host::HostSupervisor::start_in_background`]), so a
/// server out of a load balancer's rotation still gets ready again.
pub fn not_ready(state: &AppState) -> Option<NotReady> {
    let lifecycle = &state.lifecycle;
    if lifecycle.is_draining() {
        return Some(NotReady::Draining);
    }
    if lifecycle.is_starting() {
        return Some(NotReady::Starting);
    }
    if let Some(supervisor) = state.host.as_ref().and_then(|host| host.supervisor())
        && supervisor.is_needed()
        && !supervisor.is_serving()
    {
        supervisor.start_in_background();
        return Some(NotReady::HostUnavailable);
    }
    None
}

async fn get_ready(ctx: Ctx) -> ApiResult {
    let refusal = match not_ready(&ctx.state) {
        Some(refusal) => Some(refusal),
        None if !ctx
            .state
            .instance()
            .test_connection()
            .await
            .unwrap_or(false) =>
        {
            Some(NotReady::InstanceUnavailable)
        }
        None => None,
    };
    let (status, body) = match refusal {
        Some(refusal) => (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"success": false, "message": refusal.message(), "reason": refusal.reason()}),
        ),
        None => (
            StatusCode::OK,
            json!({"success": true, "message": "Success"}),
        ),
    };
    let mut response = json_response(status, &body);
    // A readiness answer is never cached by a proxy in front of the server.
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

async fn get_ids(ctx: Ctx) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["instance:read"]).await?;
    Ok(ok_json(&json!({
        "server": ctx.state.config.server_id,
        "process": std::process::id(),
    })))
}
