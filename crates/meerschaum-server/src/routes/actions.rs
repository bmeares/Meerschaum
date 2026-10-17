//! `/actions` (`api/routes/_actions.py`), dispatched through an
//! [`ActionsBackend`](crate::backends::ActionsBackend).

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::routing::{get, post};
use meerschaum_core::Value;
use serde_json::{Value as Json, json};

use super::{Ctx, body_bytes};
use crate::auth::{is_user_allowed_to_execute, scoped_auth, scoped_auth_if_private};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{ok_json, to_core, tuple, tuple_response};
use crate::routes::pipes::parse_json_body;
use crate::state::AppState;

/// Register the `actions` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/actions", get(get_actions))
        .route("/actions/{action}", post(do_action_legacy))
}

async fn get_actions(ctx: Ctx) -> ApiResult {
    scoped_auth_if_private(&ctx.state, &ctx.headers, &["actions:execute"]).await?;
    Ok(ok_json(&json!(ctx.state.actions.list())))
}

async fn do_action_legacy(ctx: Ctx, Path(action): Path<String>, body: Body) -> ApiResult {
    let body = parse_json_body(&body_bytes(body).await?)?;
    let principal = scoped_auth(&ctx.state, &ctx.headers, &["actions:execute"]).await?;
    let mut keywords = match body {
        None => return Err(ApiError::missing(&["body"])),
        Some(Json::Object(o)) => to_core(&Json::Object(o)).to_map(),
        Some(other) => {
            return Err(ApiError::validation(vec![ApiError::field_error(
                "dict_type",
                &["body"],
                "Input should be a valid dictionary",
                other,
                None,
            )]));
        }
    };
    let allowed = is_user_allowed_to_execute(&ctx.state, principal.as_ref()).await;
    if !allowed.success {
        return Ok(tuple_response(&allowed));
    }
    if !ctx.state.actions.list().iter().any(|a| a == &action) {
        return Ok(tuple(false, format!("Invalid action '{action}'.")));
    }
    if !keywords.contains_key("mrsm_instance") {
        keywords.insert(
            "mrsm_instance".into(),
            Value::Str(ctx.state.instance().connector_keys()),
        );
    }
    let debug = keywords
        .shift_remove("debug")
        .unwrap_or(Value::Bool(ctx.state.config.debug));
    keywords.insert("debug".into(), debug);
    Ok(tuple_response(
        &ctx.state.actions.execute(&action, keywords).await,
    ))
}
