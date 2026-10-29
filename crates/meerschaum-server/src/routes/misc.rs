//! `/`, `/favicon.ico`, `/chaining`, `/info`, `/healthcheck` and (with `--debug`) `/id`
//! (`api/routes/_index.py`, `_misc.py`).

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

/// Register the `misc` route group. The public preset keeps only `/healthcheck` and
/// `/favicon.ico` (unless the allowlist names `misc`).
pub fn routes(app: Router<AppState>, cfg: &ServerConfig) -> Router<AppState> {
    let app = app
        .route("/favicon.ico", get(get_favicon))
        .route("/healthcheck", get(get_healthcheck));
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

async fn get_ids(ctx: Ctx) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["instance:read"]).await?;
    Ok(ok_json(&json!({
        "server": ctx.state.config.server_id,
        "process": std::process::id(),
    })))
}
