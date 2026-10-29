//! `/tokens/*` (`api/routes/_tokens.py`).

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use meerschaum_connector::TokenModel;
use meerschaum_core::users::{Expiration, Token};
use meerschaum_core::{Map, Value};
use serde_json::{Value as Json, json};
use uuid::Uuid;

use super::{Ctx, body_bytes};
use crate::auth::{
    Principal, disallowed_scopes, grantable_scopes, is_uuid, manager_unless_no_auth,
    scope_cap_error, token_from_authorization,
};
use crate::config::ServerConfig;
use crate::error::{ApiError, ApiResult};
use crate::json::{
    from_core, ok_json, py_isoformat_utc, pydantic_datetime, python_json, raw_json, tuple,
    tuple_response,
};
use crate::routes::pipes::parse_json_body;
use crate::state::AppState;

/// Register the `tokens` route group.
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    // PARITY: Starlette keeps matching after a path-only match, so `GET`/`DELETE` on
    // `/tokens/register` and `/tokens/validate` reach `/tokens/{token_id}`.
    app.route("/tokens", get(get_tokens))
        .route(
            "/tokens/register",
            post(register_token)
                .get(get_token_static)
                .delete(delete_token_static),
        )
        .route(
            "/tokens/validate",
            post(validate_api_key)
                .get(get_token_static)
                .delete(delete_token_static),
        )
        .route(
            "/tokens/{token_id}",
            get(get_token_model).delete(delete_token),
        )
        .route("/tokens/{token_id}/edit", post(edit_token))
        .route("/tokens/{token_id}/invalidate", post(invalidate_token))
}

/// A token model as `GetTokenResponseModel` renders it (pydantic datetimes, no secret hash).
fn token_response(m: &TokenModel) -> Json {
    json!({
        "id": m.id.map(|u| u.to_string()),
        "creation": m.creation.map(|d| pydantic_datetime(&d)),
        "expiration": m.expiration.map(|d| pydantic_datetime(&d)),
        "label": m.label,
        "user_id": m.user_id.as_ref().map(from_core),
        "scopes": m.scopes,
        "is_valid": m.is_valid,
    })
}

async fn get_tokens(ctx: Ctx) -> ApiResult {
    let principal = manager_unless_no_auth(&ctx.state, &ctx.headers).await?;
    let labels = ctx.query.str_or("labels", "");
    let labels: Option<Vec<String>> =
        (!labels.is_empty()).then(|| labels.split(',').map(String::from).collect());
    let user = match &principal {
        Some(Principal::User(u)) => Some(u),
        // PARITY: a token principal has no username (`AttributeError`, a 500).
        Some(Principal::Token(_)) => return Err(ApiError::internal()),
        None => None,
    };
    let tokens = ctx
        .state
        .instance()
        .get_tokens(user, labels.as_deref(), None)
        .await
        .map_err(|e| ApiError::exception("get_tokens", e))?;
    Ok(ok_json(&Json::Array(
        tokens.iter().map(token_response).collect(),
    )))
}

/// A random token label (`get_new_daemon_name()`).
fn new_label() -> String {
    meerschaum_core::users::generate_password(12)
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|_| "token".into())
}

fn parse_dt(v: &Json, field: &str) -> Result<Option<DateTime<Utc>>, ApiError> {
    let err = || {
        ApiError::validation(vec![ApiError::field_error(
            "datetime_from_date_parsing",
            &["body", field],
            "Input should be a valid datetime or date, invalid character in year",
            v.clone(),
            None,
        )])
    };
    match v {
        Json::Null => Ok(None),
        Json::String(s) => {
            let dt = meerschaum_kernel::value::parse_datetime_value(s).ok_or_else(err)?;
            let dt = meerschaum_kernel::cast::coerce_datetime(&dt, false);
            Ok(Some(dt.utc.and_utc()))
        }
        Json::Number(n) => n
            .as_i64()
            .and_then(|s| DateTime::from_timestamp(s, 0))
            .map(Some)
            .ok_or_else(err),
        _ => Err(err()),
    }
}

fn scopes_of(v: Option<&Json>) -> Result<Vec<String>, ApiError> {
    match v {
        None => Ok(meerschaum_connector_api::models::scopes::all_scopes()),
        Some(Json::Array(items)) => Ok(items
            .iter()
            .map(|i| {
                i.as_str()
                    .map(String::from)
                    .unwrap_or_else(|| i.to_string())
            })
            .collect()),
        Some(other) => Err(ApiError::validation(vec![ApiError::field_error(
            "list_type",
            &["body", "scopes"],
            "Input should be a valid list",
            other.clone(),
            None,
        )])),
    }
}

async fn register_token(ctx: Ctx, body: Body) -> ApiResult {
    let body = parse_json_body(&body_bytes(body).await?)?;
    let principal = manager_unless_no_auth(&ctx.state, &ctx.headers).await?;
    let Some(Json::Object(model)) = body else {
        return Err(match body {
            None => ApiError::missing(&["body"]),
            Some(other) => ApiError::validation(vec![ApiError::field_error(
                "model_attributes_type",
                &["body"],
                "Input should be a valid dictionary or object to extract fields from",
                other,
                None,
            )]),
        });
    };
    let label = model
        .get("label")
        .and_then(Json::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .unwrap_or_else(new_label);
    let expiration = parse_dt(model.get("expiration").unwrap_or(&Json::Null), "expiration")?;
    let mut scopes = scopes_of(model.get("scopes"))?;
    // A token carries at most its owner's scopes: no scopes asked for means the owner's own, and
    // anything beyond them is refused, never trimmed.
    if let Some(cap) = grantable_scopes(&ctx.state, principal.as_ref()).await? {
        let asked = model
            .get("scopes")
            .and_then(Json::as_array)
            .is_some_and(|a| !a.is_empty());
        if asked {
            let refused = disallowed_scopes(&scopes, &cap, &[]);
            if !refused.is_empty() {
                return Err(scope_cap_error(&refused));
            }
        } else {
            scopes = cap;
        }
    }
    let conn = ctx.state.instance();
    let mut token = Token::new(
        Some(&label),
        // PARITY: the request model's `expiration` defaults to `None`, so tokens registered
        // over the API without an expiration never expire.
        match expiration {
            Some(dt) => Expiration::At(dt),
            None => Expiration::Never,
        },
        Some(scopes),
        ctx.state.config.token_default_expiration_days,
        Utc::now(),
    );
    token.instance_keys = Some(conn.connector_keys());
    if let Some(Principal::User(u)) = &principal {
        token.user = Some(u.clone());
        token.user_id = u.user_id.clone();
    }
    let (id, secret) = token
        .generate_credentials()
        .map_err(|e| ApiError::exception("generate_credentials", e))?;
    let res = conn.register_token(&mut token).await;
    if !res.success {
        return Err(ApiError::new(
            409,
            format!("Could not register new token:\n{}", res.message),
        ));
    }
    Ok(ok_json(&json!({
        "label": token.label,
        "secret": secret,
        "id": id.to_string(),
        "api_key": token.get_api_key(),
        "expiration": token.expiration.map(|d| pydantic_datetime(&d)),
    })))
}

async fn validate_api_key(ctx: Ctx) -> ApiResult {
    if !ctx.state.config.no_auth {
        // `HTTPBearer(auto_error=False)`: only `Bearer <credentials>`.
        let auth = ctx
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let (scheme, _, credentials) = crate::auth::partition(auth, ' ');
        if !scheme.eq_ignore_ascii_case("bearer") || credentials.is_empty() {
            return Err(ApiError::unauthorized("Not authenticated.", Some("Bearer")));
        }
        token_from_authorization(&ctx.state, credentials).await?;
    }
    Ok(tuple(true, "Success"))
}

async fn token_model_named(ctx: Ctx, token_id: String) -> ApiResult {
    let principal = manager_unless_no_auth(&ctx.state, &ctx.headers).await?;
    if !is_uuid(&token_id) {
        return Err(ApiError::new(400, "Invalid token ID."));
    }
    let id =
        Uuid::parse_str(token_id.trim()).map_err(|_| ApiError::new(400, "Invalid token ID."))?;
    let conn = ctx.state.instance();
    let model = conn
        .get_token_model(&id)
        .await
        .map_err(|e| ApiError::exception("get_token_model", e))?
        .ok_or_else(|| ApiError::new(404, "Token does not exist."))?;
    let curr_user = match &principal {
        Some(Principal::User(u)) => Some(u),
        Some(Principal::Token(_)) => return Err(ApiError::internal()),
        None => None,
    };
    let curr_user_id = match curr_user {
        Some(u) => conn.get_user_id(u).await.ok().flatten(),
        None => None,
    };
    if let Some(owner) = model.user_id.as_ref().filter(|v| v.truthy())
        && Some(owner) != curr_user_id.as_ref()
    {
        let curr_type = match curr_user {
            Some(u) => conn.get_user_type(u).await.ok().flatten(),
            // PARITY: `get_user_type(None)` fails in Python; treat as not an admin.
            None => None,
        };
        if curr_type.as_deref() != Some("admin") {
            return Err(ApiError::new(403, "Cannot edit another user's token."));
        }
    }
    // `json.dumps(payload, default=json_serialize_value)`, nulls via `value_is_null`.
    let mut payload = Map::new();
    payload.insert(
        "id".into(),
        model
            .id
            .map(|u| Value::Str(u.to_string()))
            .unwrap_or(Value::Null),
    );
    payload.insert(
        "creation".into(),
        model
            .creation
            .map(|d| Value::Str(py_isoformat_utc(&d)))
            .unwrap_or(Value::Null),
    );
    payload.insert(
        "expiration".into(),
        model
            .expiration
            .map(|d| Value::Str(py_isoformat_utc(&d)))
            .unwrap_or(Value::Null),
    );
    payload.insert("label".into(), model.label.clone().into());
    payload.insert(
        "user_id".into(),
        model.user_id.clone().unwrap_or(Value::Null),
    );
    payload.insert(
        "scopes".into(),
        Value::List(model.scopes.iter().cloned().map(Value::Str).collect()),
    );
    payload.insert("is_valid".into(), Value::Bool(model.is_valid));
    Ok(raw_json(StatusCode::OK, python_json(&Value::Map(payload))))
}

async fn get_token_model(ctx: Ctx, Path(token_id): Path<String>) -> ApiResult {
    token_model_named(ctx, token_id).await
}

async fn get_token_static(ctx: Ctx, uri: axum::http::Uri) -> ApiResult {
    let name = uri.path().rsplit('/').next().unwrap_or("").to_string();
    token_model_named(ctx, name).await
}

/// Load a token for a change and require the caller to own it or be an administrator; returns the
/// model and whether the caller is an administrator. An unowned token is an administrator's.
///
/// FIX (intentional v4 change): v4's `edit`, `invalidate` and `delete` routes never checked who
/// owned the token, so any user could relabel, re-scope, delete or re-validate another user's.
/// Under `--no-auth` there is no identity, so nothing is checked.
async fn owned_token(
    ctx: &Ctx,
    principal: &Option<Principal>,
    token_id: &str,
    action: &str,
) -> Result<(TokenModel, bool), ApiError> {
    if !is_uuid(token_id) {
        return Err(ApiError::new(400, "Token ID must be a UUID."));
    }
    let id = Uuid::parse_str(token_id.trim())
        .map_err(|_| ApiError::new(400, "Token ID must be a UUID."))?;
    let conn = ctx.state.instance();
    let model = conn
        .get_token_model(&id)
        .await
        .map_err(|e| ApiError::exception("get_token_model", e))?
        .ok_or_else(|| ApiError::new(404, "Token does not exist."))?;
    let Some(principal) = principal else {
        return Ok((model, true));
    };
    let user = match principal {
        Principal::User(u) => u,
        Principal::Token(_) => return Err(ApiError::internal()),
    };
    let is_admin = conn.get_user_type(user).await.ok().flatten().as_deref() == Some("admin");
    let user_id = conn.get_user_id(user).await.ok().flatten();
    let owns = model
        .user_id
        .as_ref()
        .filter(|v| v.truthy())
        .is_some_and(|owner| Some(owner) == user_id.as_ref());
    if !is_admin && !owns {
        return Err(ApiError::new(
            403,
            format!("Cannot {action} another user's token."),
        ));
    }
    Ok((model, is_admin))
}

async fn edit_token(ctx: Ctx, Path(token_id): Path<String>, body: Body) -> ApiResult {
    let body = parse_json_body(&body_bytes(body).await?)?;
    let principal = manager_unless_no_auth(&ctx.state, &ctx.headers).await?;
    let Some(Json::Object(model)) = body else {
        return Err(ApiError::missing(&["body"]));
    };
    let mut errors = Vec::new();
    for field in ["creation", "expiration", "label"] {
        if !model.contains_key(field) {
            errors.push(ApiError::field_error(
                "missing",
                &["body", field],
                "Field required",
                Json::Object(model.clone()),
                None,
            ));
        }
    }
    if model.get("creation") == Some(&Json::Null) {
        errors.push(ApiError::field_error(
            "datetime_type",
            &["body", "creation"],
            "Input should be a valid datetime",
            Json::Null,
            None,
        ));
    }
    if !errors.is_empty() {
        return Err(ApiError::validation(errors));
    }
    let creation = parse_dt(&model["creation"], "creation")?;
    let expiration = parse_dt(&model["expiration"], "expiration")?;
    let label = model.get("label").and_then(Json::as_str).map(String::from);
    let mut scopes = scopes_of(model.get("scopes"))?;
    let (existing, is_admin) = owned_token(&ctx, &principal, &token_id, "edit").await?;
    // An edit that does not say `is_valid` keeps it, and only an administrator may make an
    // invalidated token valid again.
    let is_valid = model
        .get("is_valid")
        .and_then(Json::as_bool)
        .unwrap_or(existing.is_valid);
    if is_valid && !existing.is_valid && !is_admin {
        return Err(ApiError::new(
            403,
            "Only an administrator may make an invalidated token valid again.",
        ));
    }
    let id = Uuid::parse_str(token_id.trim())
        .map_err(|_| ApiError::new(400, "Token ID must be a UUID."))?;
    let conn = ctx.state.instance();
    // An edit that omits `scopes` (or sends none) keeps the token's own; the cap applies to the
    // rest, and scopes the token already holds stay, so renaming never fails.
    let asked = model
        .get("scopes")
        .and_then(Json::as_array)
        .is_some_and(|a| !a.is_empty());
    if asked {
        if let Some(cap) = grantable_scopes(&ctx.state, principal.as_ref()).await? {
            let refused = disallowed_scopes(&scopes, &cap, &existing.scopes);
            if !refused.is_empty() {
                return Err(scope_cap_error(&refused));
            }
        }
    } else {
        scopes = existing.scopes.clone();
    }
    let mut token = Token::new(
        label.as_deref(),
        match expiration {
            Some(dt) => Expiration::At(dt),
            None => Expiration::Never,
        },
        Some(scopes),
        ctx.state.config.token_default_expiration_days,
        Utc::now(),
    );
    token.id = Some(id);
    token.creation = creation;
    token.is_valid = is_valid;
    token.instance_keys = Some(conn.connector_keys());
    Ok(tuple_response(&conn.edit_token(&token).await))
}

fn bare_token(id: Uuid) -> Token {
    let mut t = Token::new(None, Expiration::Never, None, 366, Utc::now());
    t.id = Some(id);
    t
}

async fn invalidate_token(ctx: Ctx, Path(token_id): Path<String>) -> ApiResult {
    let principal = manager_unless_no_auth(&ctx.state, &ctx.headers).await?;
    owned_token(&ctx, &principal, &token_id, "invalidate").await?;
    let id = Uuid::parse_str(token_id.trim())
        .ok()
        .filter(|_| is_uuid(&token_id))
        .ok_or_else(|| ApiError::new(400, "Token ID must be a UUID."))?;
    Ok(tuple_response(
        &ctx.state.instance().invalidate_token(&bare_token(id)).await,
    ))
}

async fn delete_named(ctx: Ctx, token_id: String) -> ApiResult {
    let principal = manager_unless_no_auth(&ctx.state, &ctx.headers).await?;
    owned_token(&ctx, &principal, &token_id, "delete").await?;
    let id = Uuid::parse_str(token_id.trim())
        .ok()
        .filter(|_| is_uuid(&token_id))
        .ok_or_else(|| ApiError::new(400, "Token ID must be a UUID."))?;
    Ok(tuple_response(
        &ctx.state.instance().delete_token(&bare_token(id)).await,
    ))
}

async fn delete_token(ctx: Ctx, Path(token_id): Path<String>) -> ApiResult {
    delete_named(ctx, token_id).await
}

async fn delete_token_static(ctx: Ctx, uri: axum::http::Uri) -> ApiResult {
    let name = uri.path().rsplit('/').next().unwrap_or("").to_string();
    delete_named(ctx, name).await
}
