//! `/connectors` (`api/routes/_connectors.py`).
//!
//! A connector with `teams: [...]` (rc15) is for those teams' pipes: it is listed to
//! administrators and the members of those teams only.

use axum::Router;
use axum::extract::Path;
use axum::routing::get;
use meerschaum_core::Value;
use serde_json::{Value as Json, json};

use super::Ctx;
use crate::access::{self, Access};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::ok_json;
use crate::state::AppState;

/// Register the `connectors` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/connectors", get(get_connectors))
        .route("/connectors/{type}", get(get_connectors_by_type))
}

fn labels(state: &AppState, caller: &Access, typ: &str) -> Vec<String> {
    match state.config.connectors.get(typ) {
        Some(Value::Map(m)) => m
            .keys()
            .filter(|k| *k != "default")
            .filter(|k| access::guard::may_use_connector(state, caller, &format!("{typ}:{k}")))
            .cloned()
            .collect(),
        _ => Vec::new(),
    }
}

fn connectors_for(state: &AppState, caller: &Access, typ: Option<&str>) -> Result<Json, ApiError> {
    if let Some(t) = typ {
        if !state.config.connectors.contains_key(t) {
            return Err(ApiError::new(404, format!("No connectors of type '{t}'.")));
        }
        return Ok(json!(labels(state, caller, t)));
    }
    let mut out = serde_json::Map::new();
    for typ in state.config.connectors.keys() {
        out.insert(typ.clone(), json!(labels(state, caller, typ)));
    }
    Ok(Json::Object(out))
}

async fn get_connectors(ctx: Ctx) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["connectors:read"]).await?;
    let typ = ctx.query.opt_str("type");
    Ok(ok_json(&connectors_for(
        &ctx.state,
        &caller,
        typ.as_deref(),
    )?))
}

async fn get_connectors_by_type(ctx: Ctx, Path(typ): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["connectors:read"]).await?;
    Ok(ok_json(&connectors_for(&ctx.state, &caller, Some(&typ))?))
}
