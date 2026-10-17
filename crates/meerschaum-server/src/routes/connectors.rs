//! `/connectors` (`api/routes/_connectors.py`).

use axum::Router;
use axum::extract::Path;
use axum::routing::get;
use meerschaum_core::Value;
use serde_json::{Value as Json, json};

use super::Ctx;
use crate::auth::scoped_auth;
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::ok_json;
use crate::state::AppState;

/// Register the `connectors` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/connectors", get(get_connectors))
        .route("/connectors/{type}", get(get_connectors_by_type))
}

fn labels(state: &AppState, typ: &str) -> Vec<String> {
    match state.config.connectors.get(typ) {
        Some(Value::Map(m)) => m.keys().filter(|k| *k != "default").cloned().collect(),
        _ => Vec::new(),
    }
}

fn connectors_for(state: &AppState, typ: Option<&str>) -> Result<Json, ApiError> {
    if let Some(t) = typ {
        if !state.config.connectors.contains_key(t) {
            return Err(ApiError::new(404, format!("No connectors of type '{t}'.")));
        }
        return Ok(json!(labels(state, t)));
    }
    let mut out = serde_json::Map::new();
    for typ in state.config.connectors.keys() {
        out.insert(typ.clone(), json!(labels(state, typ)));
    }
    Ok(Json::Object(out))
}

async fn get_connectors(ctx: Ctx) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["connectors:read"]).await?;
    let typ = ctx.query.opt_str("type");
    Ok(ok_json(&connectors_for(&ctx.state, typ.as_deref())?))
}

async fn get_connectors_by_type(ctx: Ctx, Path(typ): Path<String>) -> ApiResult {
    scoped_auth(&ctx.state, &ctx.headers, &["connectors:read"]).await?;
    Ok(ok_json(&connectors_for(&ctx.state, Some(&typ))?))
}
