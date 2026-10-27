//! Authentication and authorization: the secret key file, HS256 JWTs, the `fastapi_login`
//! manager, long-lived API keys (`mrsm-key:`), `ScopedAuth` and `CurrentScopes`
//! (`meerschaum/api/_oauth2.py`, `_tokens.py`, `routes/_login.py`).
//!
//! JWTs are byte-identical to PyJWT's for the same claims (sorted compact header, compact
//! payload in insertion order) and are signed with the root directory's secret file, so tokens
//! issued by 4.1 (and by the Python API host) validate here and the other way around.

use std::path::Path;
use std::sync::Arc;

use axum::http::HeaderMap;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use meerschaum_connector::{InstanceConnector, TokenModel};
use meerschaum_core::users::User;
use meerschaum_core::{SuccessTuple, Value};
use serde_json::{Map as JsonMap, Value as Json};
use sha2::Sha256;
use uuid::Uuid;

use crate::error::ApiError;
use crate::state::AppState;

type HmacSha256 = Hmac<Sha256>;

/// The scopes `is_user_allowed_to_execute()` requires of non-admins (`NECESSARY_SCOPES`).
pub const NECESSARY_SCOPES: &[&str] = &["actions:execute", "jobs:execute"];

// ------------------------------------------------------------------------------------------------
// Secret key
// ------------------------------------------------------------------------------------------------

/// `generate_secret_key()`: read the secret key file, or create it with 24 random bytes as hex.
/// The key is the file's UTF-8 bytes (the hex text itself, not the decoded bytes).
pub fn generate_secret_key(path: &Path) -> std::io::Result<Vec<u8>> {
    if path.exists() {
        return Ok(std::fs::read_to_string(path)?.into_bytes());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes =
        meerschaum_core::users::urandom(24).map_err(|e| std::io::Error::other(e.to_string()))?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    std::fs::write(path, &hex)?;
    Ok(hex.into_bytes())
}

// ------------------------------------------------------------------------------------------------
// JWT (HS256)
// ------------------------------------------------------------------------------------------------

/// Why a JWT was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JwtError {
    /// Not three base64url segments, bad JSON, or a non-object payload.
    #[error("Not enough segments")]
    Malformed,
    /// The header's `alg` is not `HS256`.
    #[error("The specified alg value is not allowed")]
    Algorithm,
    /// The signature does not match.
    #[error("Signature verification failed")]
    Signature,
    /// `exp` is in the past.
    #[error("Signature has expired")]
    Expired,
    /// `nbf` is in the future.
    #[error("The token is not yet valid (nbf)")]
    Immature,
    /// A registered claim has the wrong type (`exp`, `nbf`, `iat` not integers, `sub` not a
    /// string).
    #[error("Invalid claim: {0}")]
    Claim(&'static str),
}

/// `jwt.encode(claims, secret, 'HS256')` (PyJWT): the header is `{"alg":"HS256","typ":"JWT"}`
/// (sorted keys), the payload is compact JSON in the claims' insertion order with
/// `ensure_ascii=True`.
pub fn jwt_encode(claims: &meerschaum_core::Map, secret: &[u8]) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = meerschaum_core::pyfmt::json_dumps(
        &Value::Map(claims.clone()),
        meerschaum_core::pyfmt::JsonStyle::Compact,
        true,
    );
    let payload = URL_SAFE_NO_PAD.encode(payload.as_bytes());
    let signing_input = format!("{header}.{payload}");
    let sig = sign(signing_input.as_bytes(), secret);
    format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(sig))
}

fn sign(input: &[u8], secret: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(input);
    mac.finalize().into_bytes().to_vec()
}

fn b64url_decode(s: &str) -> Result<Vec<u8>, JwtError> {
    URL_SAFE_NO_PAD
        .decode(s.trim_end_matches('='))
        .map_err(|_| JwtError::Malformed)
}

fn int_claim(payload: &JsonMap<String, Json>, key: &'static str) -> Result<Option<i64>, JwtError> {
    match payload.get(key) {
        None => Ok(None),
        Some(Json::Number(n)) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .map(Some)
            .ok_or(JwtError::Claim(key)),
        Some(_) => Err(JwtError::Claim(key)),
    }
}

/// `jwt.decode(token, secret, algorithms=['HS256'])` (PyJWT / python-jose): verify the signature
/// and the registered claims, and return the payload.
pub fn jwt_decode(
    token: &str,
    secret: &[u8],
    now: DateTime<Utc>,
) -> Result<JsonMap<String, Json>, JwtError> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(JwtError::Malformed);
    }
    let header: Json =
        serde_json::from_slice(&b64url_decode(parts[0])?).map_err(|_| JwtError::Malformed)?;
    if header.get("alg").and_then(Json::as_str) != Some("HS256") {
        return Err(JwtError::Algorithm);
    }
    let sig = b64url_decode(parts[2])?;
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(format!("{}.{}", parts[0], parts[1]).as_bytes());
    mac.verify_slice(&sig).map_err(|_| JwtError::Signature)?;
    let payload: Json =
        serde_json::from_slice(&b64url_decode(parts[1])?).map_err(|_| JwtError::Malformed)?;
    let Json::Object(payload) = payload else {
        return Err(JwtError::Malformed);
    };
    let now = now.timestamp();
    if let Some(exp) = int_claim(&payload, "exp")?
        && exp <= now
    {
        return Err(JwtError::Expired);
    }
    if let Some(nbf) = int_claim(&payload, "nbf")?
        && nbf > now
    {
        return Err(JwtError::Immature);
    }
    int_claim(&payload, "iat")?;
    if payload.get("sub").is_some_and(|s| !s.is_string()) {
        return Err(JwtError::Claim("sub"));
    }
    Ok(payload)
}

// ------------------------------------------------------------------------------------------------
// Principals and scopes
// ------------------------------------------------------------------------------------------------

/// The authenticated principal (`Union[User, Token]`).
#[derive(Debug, Clone, PartialEq)]
pub enum Principal {
    /// A user (password login).
    User(User),
    /// A token (client-credentials login or an API key).
    Token(TokenModel),
}

impl Principal {
    /// The username for users (`curr_user.username`); tokens have none (Python raises
    /// `AttributeError`, a 500).
    pub fn username(&self) -> Option<&str> {
        match self {
            Principal::User(u) => Some(&u.username),
            Principal::Token(_) => None,
        }
    }

    /// The principal as a `User` (tokens map to a user with no name, as their methods would
    /// fail in Python).
    pub fn as_user(&self) -> Option<&User> {
        match self {
            Principal::User(u) => Some(u),
            Principal::Token(_) => None,
        }
    }
}

/// Granted scopes: a list, or (for JWTs whose `scopes` claim is a string) Python's substring
/// membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scopes {
    /// A list of scope names.
    List(Vec<String>),
    /// A string claim.
    ///
    /// PARITY: `'*' in current_scopes` is a substring test when an instance returned the scopes
    /// as a JSON string (`api/_oauth2.py:239`).
    Str(String),
}

impl Scopes {
    /// `scope in current_scopes`.
    pub fn has(&self, scope: &str) -> bool {
        match self {
            Scopes::List(l) => l.iter().any(|s| s == scope),
            Scopes::Str(s) => s.contains(scope),
        }
    }

    /// From a JWT `scopes` claim (`payload.get('scopes', [])`).
    pub fn from_claim(v: Option<&Json>) -> Scopes {
        match v {
            Some(Json::String(s)) => Scopes::Str(s.clone()),
            Some(Json::Array(items)) => Scopes::List(
                items
                    .iter()
                    .map(|i| match i {
                        Json::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .collect(),
            ),
            _ => Scopes::List(Vec::new()),
        }
    }

    /// The scopes as a list (`['*']` when granted everything).
    pub fn to_list(&self) -> Vec<String> {
        match self {
            Scopes::List(l) => l.clone(),
            Scopes::Str(s) => vec![s.clone()],
        }
    }
}

/// Python's `is_uuid(s)`.
pub fn is_uuid(s: &str) -> bool {
    Uuid::parse_str(s.trim()).is_ok()
}

fn authorization(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

/// The `503` of a credential read that failed: the request is refused, never authenticated
/// (or turned away as a bad credential) on a guess.
fn credentials_unavailable(error: impl std::fmt::Display) -> ApiError {
    tracing::warn!("Could not read a principal from the instance: {error}");
    ApiError::unavailable("Could not read your credentials from the instance; try again.")
}

/// The instance's token as a principal (`conn.get_token(id)`); a failed read is a `503`.
async fn get_token(
    conn: &Arc<dyn InstanceConnector>,
    id: &Uuid,
) -> Result<Option<TokenModel>, ApiError> {
    conn.get_token_model(id)
        .await
        .map_err(credentials_unavailable)
}

/// `load_user_or_token_from_manager(sub)`: a current token when `sub` is a UUID, else a user
/// that still exists on the instance (`Ok(None)` otherwise). Deleted users cannot retain access
/// through an old JWT. A read that fails is an error (`503`).
pub async fn load_from_subject(state: &AppState, sub: &str) -> Result<Option<Principal>, ApiError> {
    let conn = state.instance();
    if is_uuid(sub) {
        let Ok(id) = Uuid::parse_str(sub.trim()) else {
            return Ok(None);
        };
        // A JWT is an access credential for its backing token, not a way to outlive the
        // token's revocation or expiry. API keys enforce these checks on every request too.
        return Ok(get_token(&conn, &id)
            .await?
            .filter(|token| {
                token.is_valid
                    && token
                        .expiration
                        .is_none_or(|expiration| expiration > Utc::now())
            })
            .map(Principal::Token));
    }
    let mut user = User::new(sub, None);
    user.instance_keys = Some(conn.connector_keys());
    // An indefinitely cached ID would let a deleted user keep the scopes in an existing JWT.
    // Resolve existence on each request, including when another process deleted the account.
    let Some(id) = conn
        .get_user_id(&user)
        .await
        .map_err(credentials_unavailable)?
    else {
        return Ok(None);
    };
    user.user_id = Some(id);
    Ok(Some(Principal::User(user)))
}

/// `manager(request)` (`fastapi_login.LoginManager` with `OAuth2PasswordBearer`): a `Bearer`
/// JWT, decoded and loaded; a bad credential is `401 Invalid credentials`, an instance that
/// cannot be read `503`.
pub async fn manager(state: &AppState, headers: &HeaderMap) -> Result<Principal, ApiError> {
    let auth = authorization(headers).unwrap_or_default();
    let (scheme, _, token) = partition(&auth, ' ');
    if auth.is_empty() || !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return Err(ApiError::invalid_credentials());
    }
    let payload = jwt_decode(token, state.secret(), Utc::now())
        .map_err(|_| ApiError::invalid_credentials())?;
    let Some(sub) = payload.get("sub").and_then(Json::as_str) else {
        return Err(ApiError::invalid_credentials());
    };
    load_from_subject(state, sub)
        .await?
        .ok_or_else(ApiError::invalid_credentials)
}

/// Python's `str.partition(sep)`.
pub fn partition(s: &str, sep: char) -> (&str, &str, &str) {
    match s.split_once(sep) {
        Some((a, b)) => (a, " ", b),
        None => (s, "", ""),
    }
}

/// `get_token_from_authorization(authorization)`: decode and verify an API key
/// (`[mrsm-key:]base64(id:secret)`).
pub async fn token_from_authorization(
    state: &AppState,
    authorization: &str,
) -> Result<TokenModel, ApiError> {
    let raw = authorization
        .strip_prefix("mrsm-key:")
        .unwrap_or(authorization);
    let bad_format = || {
        ApiError::unauthorized(
            "Invalid token format. Expected Base64-encoded 'token_id:secret'.",
            Some("Bearer"),
        )
    };
    // PARITY: `base64.b64decode` (non-strict) discards characters outside the alphabet.
    let decoded = meerschaum_core::users::a2b_base64(raw.as_bytes()).map_err(|_| bad_format())?;
    let text = String::from_utf8(decoded).map_err(|_| bad_format())?;
    let (id, secret) = text.split_once(':').ok_or_else(bad_format)?;
    let id = Uuid::parse_str(id).map_err(|_| bad_format())?;
    let conn = state.instance();
    let invalid = || ApiError::unauthorized("Invalid or revoked token.", Some("Bearer"));
    let token = get_token(&conn, &id).await?.ok_or_else(invalid)?;
    if !token.is_valid {
        return Err(invalid());
    }
    if token.expiration.is_some_and(|exp| exp <= Utc::now()) {
        // `get_expiration_status()` invalidates an expired token on the instance.
        let mut t = core_token(&token);
        t.is_valid = false;
        let res = conn.invalidate_token(&t).await;
        if !res.success {
            tracing::warn!("Failed to invalidate {}:\n{}", t, res.message);
        }
        return Err(ApiError::unauthorized("Token has expired.", Some("Bearer")));
    }
    let hash = token.secret_hash.clone().unwrap_or_default();
    if !meerschaum_core::users::verify_password(secret, &hash).unwrap_or(false) {
        return Err(ApiError::unauthorized("Invalid secret.", Some("Bearer")));
    }
    Ok(token)
}

/// A core `Token` from a token model.
pub fn core_token(m: &TokenModel) -> meerschaum_core::users::Token {
    meerschaum_core::users::Token {
        label: m.label.clone(),
        creation: m.creation,
        expiration: m.expiration,
        instance_keys: None,
        user: None,
        user_id: m.user_id.clone(),
        scopes: m.scopes.clone(),
        is_valid: m.is_valid,
        id: m.id,
        secret: None,
        secret_hash: m.secret_hash.clone(),
    }
}

/// `load_user_or_token(request)`.
pub async fn load_user_or_token(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Principal, ApiError> {
    let auth = authorization(headers).unwrap_or_default();
    if auth.is_empty() {
        return Err(ApiError::unauthorized("Not authenticated.", None));
    }
    // PARITY: every occurrence of the scheme names is removed (`str.replace`).
    let stripped = auth.replace("Basic ", "").replace("Bearer ", "");
    if !stripped.starts_with("mrsm-key:") {
        return manager(state, headers).await;
    }
    token_from_authorization(state, &stripped)
        .await
        .map(Principal::Token)
}

/// The `503` of a scope read that failed: authorization fails closed (it never falls back to
/// "every scope", which is what a user without a `scopes` attribute holds).
pub fn scopes_unavailable(error: impl std::fmt::Display) -> ApiError {
    tracing::warn!("Could not read a principal's scopes from the instance: {error}");
    ApiError::unavailable("Could not read your permissions from the instance; try again.")
}

/// `user.get_scopes(refresh=True)` / `token.get_scopes(refresh=True)`.
///
/// A read that fails is an error (`503`), never a guess: a user whose attributes cannot be read
/// gets no scopes rather than v4's "no `scopes` attribute = every scope".
pub async fn principal_scopes(
    state: &AppState,
    principal: &Principal,
) -> Result<Vec<String>, ApiError> {
    let conn = state.instance();
    match principal {
        Principal::User(u) => {
            User::scopes_from_read(conn.get_user_attributes(u).await).map_err(scopes_unavailable)
        }
        Principal::Token(t) => match t.id {
            Some(id) => conn.get_token_scopes(&id).await.map_err(scopes_unavailable),
            None => Ok(Vec::new()),
        },
    }
}

/// `get_granted_scopes(request, user_or_token)`: API keys are resolved against the instance on
/// every request; JWTs carry their own scopes.
pub async fn granted_scopes(
    state: &AppState,
    headers: &HeaderMap,
    principal: &Principal,
) -> Result<Scopes, ApiError> {
    if state.config.no_auth {
        return Ok(Scopes::List(vec!["*".into()]));
    }
    let auth = authorization(headers).unwrap_or_default();
    if auth.contains("mrsm-key:") {
        return Ok(Scopes::List(principal_scopes(state, principal).await?));
    }
    if auth.is_empty() {
        return Err(ApiError::unauthorized("Not authenticated.", None));
    }
    let (scheme, _, token) = partition(&auth, ' ');
    if token.is_empty() || !scheme.eq_ignore_ascii_case("bearer") {
        return Err(ApiError::unauthorized(
            "Unsupported authentication scheme.",
            None,
        ));
    }
    let payload = jwt_decode(token, state.secret(), Utc::now())
        .map_err(|_| ApiError::unauthorized("Invalid access token.", Some("Bearer")))?;
    Ok(Scopes::from_claim(payload.get("scopes")))
}

/// The scopes a principal may hand to a token it registers or edits: `None` when it may grant
/// anything (an administrator, a holder of `*`, or `--no-auth`), else the scopes it holds. A
/// failed read of the principal's type or scopes is an error, never a wider grant.
///
/// FIX (intentional v4 change): v4 let any user register a token with any scopes, `*` included.
pub async fn grantable_scopes(
    state: &AppState,
    principal: Option<&Principal>,
) -> Result<Option<Vec<String>>, ApiError> {
    let Some(principal) = principal else {
        return Ok(None);
    };
    if let Principal::User(user) = principal {
        let user_type = state
            .instance()
            .get_user_type(user)
            .await
            .map_err(scopes_unavailable)?;
        if user_type.as_deref() == Some("admin") {
            return Ok(None);
        }
    }
    let held = principal_scopes(state, principal).await?;
    if held.iter().any(|s| s == "*") {
        return Ok(None);
    }
    Ok(Some(held))
}

/// The requested scopes outside `cap` and `already` (scopes a token being edited already holds
/// stay), in request order.
pub fn disallowed_scopes(requested: &[String], cap: &[String], already: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for scope in requested {
        if !cap.contains(scope) && !already.contains(scope) && !out.contains(scope) {
            out.push(scope.clone());
        }
    }
    out
}

/// The 403 for scopes a caller may not grant.
pub fn scope_cap_error(disallowed: &[String]) -> ApiError {
    let names: Vec<String> = disallowed.iter().map(|s| format!("'{s}'")).collect();
    ApiError::new(
        403,
        format!(
            "Cannot grant scopes you do not hold: {}. A token may only carry the scopes of its owner (`*` only for administrators and holders of `*`).",
            names.join(", ")
        ),
    )
}

/// `ScopedAuth(scopes)`: authenticate a user session or a scoped token and require every scope.
/// Returns `None` under `--no-auth`.
pub async fn scoped_auth(
    state: &AppState,
    headers: &HeaderMap,
    scopes: &[&str],
) -> Result<Option<Principal>, ApiError> {
    scoped_auth_with_aliases(state, headers, scopes, &[]).await
}

/// [`scoped_auth`] where a required scope may also be satisfied by a legacy spelling: each
/// `(scope, legacy)` pair of `aliases` accepts a token that explicitly holds `legacy` in place
/// of `scope`. The 403 still names the current scope.
pub async fn scoped_auth_with_aliases(
    state: &AppState,
    headers: &HeaderMap,
    scopes: &[&str],
    aliases: &[(&str, &str)],
) -> Result<Option<Principal>, ApiError> {
    if state.config.no_auth {
        return Ok(None);
    }
    let principal = load_user_or_token(state, headers).await?;
    let granted = granted_scopes(state, headers, &principal).await?;
    if granted.has("*") {
        return Ok(Some(principal));
    }
    let auth = meerschaum_connector_api::models::ScopedAuth::new(scopes);
    for scope in &auth.scopes {
        let legacy = aliases
            .iter()
            .find(|(current, _)| current == scope)
            .map(|(_, legacy)| *legacy);
        if !granted.has(scope) && !legacy.is_some_and(|l| granted.has(l)) {
            return Err(ApiError::new(
                403,
                format!("Missing required scope: '{scope}'"),
            ));
        }
    }
    Ok(Some(principal))
}

/// `ScopedAuth(scopes)` only when the server runs with `--private` (`... if private else None`).
pub async fn scoped_auth_if_private(
    state: &AppState,
    headers: &HeaderMap,
    scopes: &[&str],
) -> Result<Option<Principal>, ApiError> {
    if state.config.private {
        scoped_auth(state, headers, scopes).await
    } else {
        Ok(None)
    }
}

/// `CurrentScopes`: the caller's granted scopes without requiring any.
pub async fn current_scopes(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(Option<Principal>, Scopes), ApiError> {
    if state.config.no_auth {
        return Ok((None, Scopes::List(vec!["*".into()])));
    }
    let principal = load_user_or_token(state, headers).await?;
    let scopes = granted_scopes(state, headers, &principal).await?;
    Ok((Some(principal), scopes))
}

/// `fastapi.Depends(manager) if not no_auth else None`.
pub async fn manager_unless_no_auth(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<Principal>, ApiError> {
    if state.config.no_auth {
        return Ok(None);
    }
    manager(state, headers).await.map(Some)
}

/// Whether the caller is an administrator. Without a principal (`--no-auth`) everyone is.
pub async fn is_admin(state: &AppState, principal: Option<&Principal>) -> bool {
    let Some(principal) = principal else {
        return true;
    };
    match principal {
        Principal::User(u) => {
            state
                .instance()
                .get_user_type(u)
                .await
                .ok()
                .flatten()
                .unwrap_or_default()
                == "admin"
        }
        Principal::Token(_) => false,
    }
}

/// `is_user_allowed_to_execute(user)`: the `/actions` and job routes, which read
/// `system:api:permissions:actions:non_admin` (see [`is_user_allowed_to_execute_with`]).
pub async fn is_user_allowed_to_execute(
    state: &AppState,
    principal: Option<&Principal>,
) -> SuccessTuple {
    is_user_allowed_to_execute_with(state, principal, state.config.system_actions_non_admin).await
}

/// Who may run actions through the web console: `POST /execute` and the job-log stream
/// (`GET /logs/{name}/stream`) both ask this one function, so they cannot disagree. It is the rule
/// of v4's console (`api/dash/sessions.py`): administrators, and other users only when
/// `api:permissions:actions:non_admin` is true (its default; `start api --secure` sets it false),
/// plus the scopes `is_user_allowed_to_execute` asks of non-administrators. v4's other key,
/// `system:api:permissions:actions:non_admin` (unset by default), gates `/actions` and the job
/// routes and does not apply here.
pub async fn is_user_allowed_to_execute_console(
    state: &AppState,
    principal: Option<&Principal>,
) -> SuccessTuple {
    is_user_allowed_to_execute_with(state, principal, state.config.actions_non_admin).await
}

/// `is_user_allowed_to_execute(user)` with the "non-administrators may run actions" switch given
/// by the caller: `/actions` and the job routes pass `system:api:permissions:actions:non_admin`
/// (v4's key, unset by default), the console's `POST /execute` passes
/// `api:permissions:actions:non_admin` (`ServerConfig::actions_non_admin`, true by default,
/// false under `--secure`).
pub async fn is_user_allowed_to_execute_with(
    state: &AppState,
    principal: Option<&Principal>,
    allow_non_admin: bool,
) -> SuccessTuple {
    let Some(principal) = principal else {
        return SuccessTuple::success();
    };
    if is_admin(state, Some(principal)).await {
        return SuccessTuple::success();
    }
    if !allow_non_admin {
        return SuccessTuple::fail(
            "The administrator for this server has not allowed users to perform actions.",
        );
    }
    // A failed read refuses (fail closed).
    let scopes = match principal_scopes(state, principal).await {
        Ok(scopes) => scopes,
        Err(_) => {
            return SuccessTuple::fail(
                "Could not read your permissions from the instance; try again.",
            );
        }
    };
    // FIX (intentional v4 change): a literal `*` grants every scope, so it satisfies the
    // necessary scopes too (v4 compared `scope not in scopes`, which refused a `*` holder).
    if scopes.iter().any(|s| s == "*") {
        return SuccessTuple::success();
    }
    for scope in NECESSARY_SCOPES {
        if !scopes.iter().any(|s| s == scope) {
            return SuccessTuple::fail(format!("Missing scope '{scope}'."));
        }
    }
    SuccessTuple::success()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_roundtrip_matches_pyjwt_layout() {
        let mut claims = meerschaum_core::Map::new();
        claims.insert("sub".into(), Value::Str("alice".into()));
        claims.insert("scopes".into(), Value::List(vec![Value::Str("*".into())]));
        claims.insert("type".into(), Value::Str("admin".into()));
        claims.insert("exp".into(), Value::Int(4_102_444_800));
        let token = jwt_encode(&claims, b"secret");
        assert!(token.starts_with("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."));
        let payload = jwt_decode(&token, b"secret", Utc::now()).unwrap();
        assert_eq!(payload["sub"], "alice");
        assert_eq!(
            jwt_decode(&token, b"other", Utc::now()),
            Err(JwtError::Signature)
        );
        let mut expired = claims.clone();
        expired.insert("exp".into(), Value::Int(1));
        assert_eq!(
            jwt_decode(&jwt_encode(&expired, b"s"), b"s", Utc::now()),
            Err(JwtError::Expired)
        );
    }

    #[test]
    fn secret_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("api").join(".api_secret_key");
        let a = generate_secret_key(&path).unwrap();
        assert_eq!(a.len(), 48);
        assert_eq!(generate_secret_key(&path).unwrap(), a);
    }

    #[test]
    fn string_scopes_are_substring_tests() {
        let s = Scopes::from_claim(Some(&Json::String("[\"pipes:read\"]".into())));
        assert!(s.has("pipes:read"));
        assert!(!s.has("*"));
    }
}
