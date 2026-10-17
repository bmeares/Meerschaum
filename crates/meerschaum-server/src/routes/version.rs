//! `/version` and `/version/mrsm` (`api/routes/_version.py`).

use axum::Router;
use axum::routing::get;
use serde_json::json;

use super::Ctx;
use crate::auth::scoped_auth_if_private;
use crate::config::ServerConfig;
use crate::error::ApiResult;
use crate::json::ok_json;
use crate::state::AppState;

/// Register the `version` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/version", get(get_version))
        .route("/version/mrsm", get(get_version))
}

async fn get_version(ctx: Ctx) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["instance:read"]).await?;
    Ok(ok_json(&json!(ctx.state.config.version)))
}
