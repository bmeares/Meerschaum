//! `POST /login` (`api/routes/_login.py`): the OAuth2 password and client-credentials grants,
//! issuing HS256 JWTs with the claims `{sub, scopes, type, exp}`.

use axum::Router;
use axum::body::Body;
use axum::routing::post;
use base64::Engine;
use chrono::{Duration, Utc};
use meerschaum_core::users::{User, verify_password};
use meerschaum_core::{Map, Value};
use serde_json::{Value as Json, json};
use uuid::Uuid;

use super::{Ctx, body_bytes, form_get, read_form};
use crate::auth::{is_uuid, jwt_encode};
use crate::error::{ApiError, ApiResult};
use crate::json::ok_json;
use crate::state::AppState;

/// Register `/login`.
pub fn routes(app: Router<AppState>) -> Router<AppState> {
    app.route(meerschaum_connector_api::routes::LOGIN, post(login))
}

/// `datetime.isoformat()` of a naive timestamp (microseconds omitted when zero).
pub fn naive_isoformat(dt: chrono::NaiveDateTime) -> String {
    let base = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = dt.and_utc().timestamp_subsec_micros();
    if micros == 0 {
        base
    } else {
        format!("{base}.{micros:06}")
    }
}

/// `CustomOAuth2PasswordRequestForm`.
#[derive(Debug, Default)]
struct LoginForm {
    grant_type: Option<String>,
    username: Option<String>,
    password: Option<String>,
    scope: String,
    client_id: Option<String>,
    client_secret: Option<String>,
}

fn nonempty(s: &Option<String>) -> bool {
    s.as_deref().is_some_and(|s| !s.is_empty())
}

async fn login(ctx: Ctx, body: Body) -> ApiResult {
    let state = &ctx.state;
    let body = body_bytes(body).await?;
    let form = read_form(&ctx.headers, body).await;
    let mut data = LoginForm {
        grant_type: form_get(&form, "grant_type").map(String::from),
        username: form_get(&form, "username").map(String::from),
        password: form_get(&form, "password").map(String::from),
        scope: form_get(&form, "scope").unwrap_or("").to_string(),
        client_id: form_get(&form, "client_id").map(String::from),
        client_secret: form_get(&form, "client_secret").map(String::from),
    };
    if let Some(gt) = &data.grant_type {
        // `Form(None, pattern="password|client_credentials")` (a regex search).
        if !gt.contains("password") && !gt.contains("client_credentials") {
            return Err(ApiError::validation(vec![ApiError::field_error(
                "string_pattern_mismatch",
                &["body", "grant_type"],
                "String should match pattern 'password|client_credentials'",
                Json::String(gt.clone()),
                Some(json!({"pattern": "password|client_credentials"})),
            )]));
        }
    }

    // Client credentials from the `Authorization` header (Basic or Bearer, optionally
    // `mrsm-key:`-prefixed) when nothing else was submitted.
    let authorization = ctx
        .headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    if !nonempty(&data.username)
        && !nonempty(&data.password)
        && !nonempty(&data.client_id)
        && !nonempty(&data.client_secret)
    {
        if let Some(auth) = authorization.filter(|a| !a.is_empty()) {
            let parts: Vec<&str> = auth.split_whitespace().collect();
            if let [scheme, credentials] = parts.as_slice() {
                let credentials = credentials.strip_prefix("mrsm-key:").unwrap_or(credentials);
                if scheme.eq_ignore_ascii_case("basic") || scheme.eq_ignore_ascii_case("bearer") {
                    let decoded = base64::engine::general_purpose::STANDARD
                        .decode(credentials)
                        .ok()
                        .and_then(|b| String::from_utf8(b).ok());
                    if let Some((id, secret)) = decoded.as_deref().and_then(|d| d.split_once(':')) {
                        data.client_id = Some(id.to_string());
                        data.client_secret = Some(secret.to_string());
                        data.grant_type = Some("client_credentials".into());
                    }
                }
            }
        }
    }

    let grant_type = match data.grant_type.as_deref().filter(|g| !g.is_empty()) {
        Some(g) => g.to_string(),
        None if nonempty(&data.username) && nonempty(&data.password) => "password".into(),
        None => "client_credentials".into(),
    };

    let conn = state.instance();
    let no_auth = state.config.no_auth;
    let py_str = |s: &Option<String>| s.clone().unwrap_or_else(|| "None".into());
    let (allowed_scopes, user_type, sub): (Vec<String>, Option<String>, String) =
        if grant_type == "password" {
            let username = py_str(&data.username);
            let password = py_str(&data.password);
            let user = User::new(&username, Some(&password));
            let correct = no_auth || {
                let hash = conn
                    .get_user_password_hash(&user)
                    .await
                    .map_err(|e| ApiError::exception("get_user_password_hash", e))?;
                match hash {
                    Some(h) => verify_password(&password, &h).unwrap_or(false),
                    None => false,
                }
            };
            if !correct {
                return Err(ApiError::invalid_credentials());
            }
            let attrs = conn.get_user_attributes(&user).await.ok().flatten();
            let mut user = user;
            user.attributes = attrs;
            let scopes = user.get_scopes();
            let typ = conn.get_user_type(&user).await.ok().flatten();
            (scopes, typ, username)
        } else if grant_type == "client_credentials" {
            let client_id = py_str(&data.client_id);
            if !is_uuid(&client_id) {
                return Err(ApiError::invalid_credentials());
            }
            let token_id =
                Uuid::parse_str(client_id.trim()).map_err(|_| ApiError::invalid_credentials())?;
            let correct = no_auth || {
                let hash = conn
                    .get_token_secret_hash(&token_id)
                    .await
                    .map_err(|e| ApiError::exception("get_token_secret_hash", e))?
                    .unwrap_or_else(|| "None".into());
                verify_password(&py_str(&data.client_secret), &hash).unwrap_or(false)
            };
            if !correct {
                return Err(ApiError::invalid_credentials());
            }
            let scopes = conn
                .get_token_scopes(&token_id)
                .await
                .map_err(|e| ApiError::exception("get_token_scopes", e))?;
            (scopes, None, client_id)
        } else {
            return Err(ApiError::invalid_credentials());
        };

    let requested: Vec<String> = data.scope.split_whitespace().map(String::from).collect();
    // Admins implicitly hold every scope, so their stored scope list must not filter the grant.
    let final_scopes: Vec<String> =
        if allowed_scopes.iter().any(|s| s == "*") || user_type.as_deref() == Some("admin") {
            if requested.is_empty() {
                vec!["*".into()]
            } else {
                requested
            }
        } else if requested.is_empty() {
            allowed_scopes
        } else {
            requested
                .into_iter()
                .filter(|s| allowed_scopes.contains(s))
                .collect()
        };

    let expires_minutes = meerschaum_connector_api::routes::TOKEN_EXPIRES_MINUTES;
    let now = Utc::now();
    let expires_dt = (now + Duration::minutes(expires_minutes)).naive_utc();
    let mut claims = Map::new();
    claims.insert("sub".into(), Value::Str(sub));
    claims.insert(
        "scopes".into(),
        Value::List(final_scopes.into_iter().map(Value::Str).collect()),
    );
    claims.insert(
        "type".into(),
        user_type.map(Value::Str).unwrap_or(Value::Null),
    );
    claims.insert(
        "exp".into(),
        Value::Int((now + Duration::minutes(expires_minutes)).timestamp()),
    );
    let access_token = jwt_encode(&claims, state.secret());
    Ok(ok_json(&json!({
        "access_token": access_token,
        "token_type": "bearer",
        "expires": naive_isoformat(expires_dt),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn isoformat_drops_zero_micros() {
        let d = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        assert_eq!(
            naive_isoformat(d.and_hms_opt(3, 4, 5).unwrap()),
            "2026-01-02T03:04:05"
        );
        assert_eq!(
            naive_isoformat(d.and_hms_micro_opt(3, 4, 5, 42).unwrap()),
            "2026-01-02T03:04:05.000042"
        );
    }
}
