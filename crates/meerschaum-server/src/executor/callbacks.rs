//! Main-owned, revocable authority for one execution's ordinary API callbacks.
//! There is no HTTP credential-minting route. Only the trusted launch supervisor creates grants.

use std::collections::BTreeSet;
use std::path::PathBuf;

use chrono::Utc;
use meerschaum_core::{Map, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::AppState;
use crate::access::Access;
use crate::auth::{self, Principal, Scopes};
use crate::error::ApiError;

pub const TOKEN_SECONDS: i64 = 30;
pub const LEASE_SECONDS: i64 = 90;
pub const AUDIENCE: &str = "mrsm-execution-v1";
// Includes up to 256 immutable source fingerprints with 128-byte names.
const MAX_GRANT_BYTES: u64 = 128 * 1024;

tokio::task_local! {
    static REQUEST: ([u8; 32], Access);
}

fn request_key(headers: &axum::http::HeaderMap) -> Option<[u8; 32]> {
    use sha2::Digest;
    Some(sha2::Sha256::digest(headers.get(axum::http::header::AUTHORIZATION)?.as_bytes()).into())
}

/// Validated only within this request and for this exact credential; never a cross-request cache.
pub(crate) fn current(headers: &axum::http::HeaderMap) -> Option<Access> {
    let key = request_key(headers)?;
    REQUEST
        .try_with(|(stored, access)| (key == *stored).then(|| access.clone()))
        .ok()
        .flatten()
}

fn execution_bearer(headers: &axum::http::HeaderMap) -> bool {
    use base64::Engine;
    let Some(value) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    else {
        return false;
    };
    let Some((scheme, token)) = value.split_once(' ') else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("bearer") || token.len() > 32 * 1024 {
        return false;
    }
    let Some(payload) = token.split('.').nth(1) else {
        return false;
    };
    let Ok(bytes) =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload.trim_end_matches('='))
    else {
        return false;
    };
    let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return false;
    };
    payload
        .get(auth::PRINCIPAL_CLAIM)
        .and_then(serde_json::Value::as_str)
        == Some("execution")
}

fn route_allowed(purpose: Purpose, method: &axum::http::Method, path: &str) -> bool {
    let path = crate::routes::decoded_path(path);
    if matches!(
        path.as_ref(),
        meerschaum_connector_api::graph_plan::GRAPH_PLAN_PATH
            | meerschaum_connector_api::graph_batch::GRAPH_BATCH_PATH
            | "/execute/validate"
    ) {
        return purpose == Purpose::Command && *method == axum::http::Method::POST;
    }
    let group = path
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or_default();
    match group {
        "version" | "healthcheck" => *method == axum::http::Method::GET,
        "jobs" | "logs" => true,
        "pipes" | "metadata" | "pipelines" | "engine" => purpose == Purpose::Command,
        "connectors" => purpose == Purpose::Command && *method == axum::http::Method::GET,
        _ => false,
    }
}

/// Enforce the credential's purpose before core, console or extension handlers run. In
/// particular, no login, token/user/team administration, plugin publication or recursive
/// action/execute request may adopt a derived principal as an ordinary user.
pub async fn guard(
    axum::extract::State(state): axum::extract::State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if super::plugin_delivery::recognizes(request.headers()) {
        return super::plugin_delivery::guard(axum::extract::State(state), request, next).await;
    }
    if !execution_bearer(request.headers()) {
        return next.run(request).await;
    }
    let headers = request.headers().clone();
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let checked = async {
        if state.config.no_auth {
            return Err(refused());
        }
        let access = match current(&headers) {
            Some(access) => access,
            None => {
                let principal = auth::manager(&state, &headers).await?;
                let granted = auth::granted_scopes(&state, &headers, &principal).await?;
                crate::access::access_of(&state, principal, &granted).await?
            }
        };
        let Some(Principal::Execution(identity)) = &access.auth else {
            return Err(refused());
        };
        if !route_allowed(identity.grant.purpose, &method, &path) {
            return Err(ApiError::new(
                403,
                "This execution credential cannot call that API surface.",
            ));
        }
        Ok(access)
    }
    .await;
    let access = match checked {
        Ok(access) => access,
        Err(error) => return error.into_response(),
    };
    let Some(key) = request_key(request.headers()) else {
        return refused().into_response();
    };
    REQUEST.scope((key, access), next.run(request)).await
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UserBinding {
    username: String,
    user_id: Value,
    credential: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Origin {
    User {
        username: String,
        user_id: Value,
        credential: String,
    },
    Token {
        id: Uuid,
        user_id: Option<Value>,
        creation: Option<chrono::DateTime<Utc>>,
        proof: Option<String>,
        owner: Option<UserBinding>,
    },
}

/// Team-code processes need only canonical job management on main. API-only commands also
/// reach the ordinary data API, always through its current resource and instance checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    Jobs,
    Command,
}

/// Immutable ownership for canonical history, independent of display-name reuse, password
/// changes and the live launch lease. This stays in main's private catalog.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerBinding {
    User {
        username: String,
        user_id: Value,
    },
    Token {
        id: Uuid,
        creation: Option<chrono::DateTime<Utc>>,
        hash: Option<String>,
    },
}

impl std::fmt::Debug for OwnerBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnerBinding").finish_non_exhaustive()
    }
}

fn history_hash(token: &meerschaum_connector::TokenModel) -> Option<String> {
    use base64::Engine;
    use sha2::Digest;
    token.secret_hash.as_ref().map(|hash| {
        let digest: [u8; 32] = sha2::Sha256::digest(hash.as_bytes()).into();
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
    })
}

impl OwnerBinding {
    /// Capture an already-authenticated registration without its password or bearer credential.
    pub fn capture(caller: &Access) -> Option<Self> {
        if caller.no_auth {
            return None;
        }
        match &caller.auth {
            Some(Principal::Execution(identity)) => identity.owner_binding(),
            Some(Principal::User(user)) => Some(Self::User {
                username: caller.username()?.to_string(),
                user_id: user.user_id.clone().filter(|id| !id.is_null())?,
            }),
            Some(Principal::Token(token)) => match caller.username() {
                Some(username) => Some(Self::User {
                    username: username.to_string(),
                    user_id: token.user_id.clone().filter(|id| !id.is_null())?,
                }),
                None => Some(Self::Token {
                    id: token.id?,
                    creation: token.creation,
                    hash: history_hash(token),
                }),
            },
            None => None,
        }
    }

    /// Callers have already authenticated; compare their registered identity rather than name.
    pub fn matches(&self, caller: &Access) -> bool {
        if let Some(Principal::Execution(identity)) = &caller.auth {
            return identity.owner_binding().as_ref() == Some(self);
        }
        match (self, &caller.auth) {
            (Self::User { username, user_id }, Some(Principal::User(user))) => {
                caller.username() == Some(username) && user.user_id.as_ref() == Some(user_id)
            }
            (Self::User { username, user_id }, Some(Principal::Token(token))) => {
                caller.username() == Some(username) && token.user_id.as_ref() == Some(user_id)
            }
            (Self::Token { id, creation, hash }, Some(Principal::Token(token))) => {
                token.id == Some(*id) && token.creation == *creation && history_hash(token) == *hash
            }
            _ => false,
        }
    }
}

/// Canonical launch authority in main's private root. No connector credentials are stored here.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub id: Uuid,
    pub launch_id: Uuid,
    pub team_id: Option<Uuid>,
    pub generation: Option<Uuid>,
    pub selection: Option<meerschaum_core::execution::Settings>,
    pub version: String,
    pub permissions: BTreeSet<String>,
    pub required: BTreeSet<String>,
    pub lease_until: i64,
    pub depth: u8,
    pub purpose: Purpose,
    /// Exact accepted runtime sources. Missing legacy capture permits no derived sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ceiling: Option<super::source_ceiling::Ceiling>,
    /// Main's immutable originating graph, inherited by narrower derived handlers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_origin: Option<super::pipe_operation::CallbackBinding>,
    /// Version one explicitly distinguishes no graph from missing legacy provenance.
    #[serde(default)]
    pub graph_tracking: u8,
    /// Main has not completed this direct graph's atomic issuer/binding yet.
    #[serde(default)]
    pub graph_pending: bool,
    origin: Origin,
}

/// A verified callback identity retains its live parent's access, never global administrator
/// privileges. The explicit Principal variant prevents ordinary user-token helpers adopting it.
#[derive(Clone, Debug)]
pub struct Identity {
    pub grant: Grant,
    pub(crate) current: Box<Access>,
    pub(crate) team: Option<meerschaum_core::access::Team>,
    /// Fresh, credential-free API transport capability for a non-isolated host team. This is
    /// never decoded from claims or persisted grant input, and grants no object visibility.
    transport_connectors: BTreeSet<String>,
}

impl Identity {
    pub(crate) fn is_user_bound(&self) -> bool {
        matches!(self.grant.origin, Origin::User { .. })
    }
}

impl PartialEq for Identity {
    fn eq(&self, other: &Self) -> bool {
        self.grant == other.grant
            && self.current.principal == other.current.principal
            && self.current.primary_team == other.current.primary_team
            && self.team == other.team
            && self.transport_connectors == other.transport_connectors
    }
}

impl Identity {
    /// Exact API-served logical keys available inside this identity's immutable team boundary.
    /// These are not native connector attributes and do not bypass any object or SQL checks.
    pub fn transport_connectors(&self) -> &BTreeSet<String> {
        &self.transport_connectors
    }

    /// Fresh original authority for admission checks only. Never pass this access to a child,
    /// sign it into an executor request, or use it instead of the bounded callback principal.
    pub fn source_access(&self) -> &Access {
        &self.current
    }

    /// The identity owning this launch's history; no bearer, password or reusable token.
    pub fn owner_binding(&self) -> Option<OwnerBinding> {
        match &self.grant.origin {
            Origin::User {
                username, user_id, ..
            } => Some(OwnerBinding::User {
                username: username.clone(),
                user_id: user_id.clone(),
            }),
            Origin::Token {
                owner: Some(owner), ..
            } => Some(OwnerBinding::User {
                username: owner.username.clone(),
                user_id: owner.user_id.clone(),
            }),
            Origin::Token {
                id,
                creation,
                owner: None,
                ..
            } => match &self.current.auth {
                Some(Principal::Token(token)) => Some(OwnerBinding::Token {
                    id: *id,
                    creation: *creation,
                    hash: history_hash(token),
                }),
                _ => None,
            },
        }
    }

    /// The bounded execution principal for internal placement/control calls, after live validation.
    pub fn caller(&self) -> Access {
        access(
            self,
            &Scopes::List(self.grant.permissions.iter().cloned().collect()),
        )
    }

    /// Stream accounting uses the original identity's bucket, not a new allowance per launch.
    pub fn owner_key(&self) -> String {
        match self.current.auth.as_ref() {
            Some(Principal::User(user)) => format!("user:{}", user.username),
            Some(Principal::Token(token)) => format!(
                "token:{}",
                token.id.map(|id| id.to_string()).unwrap_or_default()
            ),
            _ => format!("execution:{}", self.grant.id),
        }
    }
}

pub(crate) async fn principal(
    state: &AppState,
    payload: &serde_json::Map<String, serde_json::Value>,
) -> Result<Principal, ApiError> {
    let text = |key| payload.get(key).and_then(serde_json::Value::as_str);
    let now = Utc::now().timestamp();
    let issued = payload
        .get("iat")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(refused)?;
    let expires = payload
        .get("exp")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(refused)?;
    if expires <= issued || expires.saturating_sub(issued) > TOKEN_SECONDS {
        return Err(refused());
    }
    if text("aud") != Some(AUDIENCE)
        || payload
            .get("exp")
            .and_then(serde_json::Value::as_i64)
            .is_none_or(|exp| exp > now + TOKEN_SECONDS + 5)
        || payload
            .get("iat")
            .and_then(serde_json::Value::as_i64)
            .is_none_or(|iat| iat > now + 5)
    {
        return Err(refused());
    }
    let id = text("sub")
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(refused)?;
    let launch = text("mrsm_launch")
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(refused)?;
    let identity = load_identity(state, id, launch).await?;
    if identity.grant.graph_pending {
        return Err(refused());
    }
    Ok(Principal::Execution(Box::new(identity)))
}

fn refused() -> ApiError {
    ApiError::unauthorized(
        "Execution authority is expired, revoked or no longer permitted.",
        Some("Bearer"),
    )
}
fn storage_error() -> ApiError {
    ApiError::unavailable("Could not read current execution authority.")
}

fn directory(state: &AppState) -> PathBuf {
    state.config.root_dir.join("executors/callbacks")
}

fn generation_path(root: &std::path::Path, team: Uuid) -> PathBuf {
    root.join("executors/callbacks/generations")
        .join(format!("{team}.json"))
}

/// The trusted main registry retires old callback generations before publishing its next
/// deployment. An executor receives neither this private directory nor the authority to write it.
pub fn set_generation(
    root: &std::path::Path,
    team: Uuid,
    generation: Option<Uuid>,
) -> Result<(), ApiError> {
    let path = generation_path(root, team);
    meerschaum_core::team_scope::create_private_dir(path.parent().ok_or_else(storage_error)?)
        .map_err(|_| storage_error())?;
    let text = serde_json::to_string(&generation).map_err(|_| storage_error())?;
    if std::fs::read_to_string(&path).ok().as_deref() == Some(&text) {
        return Ok(());
    }
    meerschaum_core::team_scope::write_private(&path, &text).map_err(|_| storage_error())
}

fn generation(state: &AppState, team: Uuid) -> Result<Option<Uuid>, ApiError> {
    match std::fs::read(generation_path(&state.config.root_dir, team)) {
        Ok(bytes) if bytes.len() <= 128 => {
            serde_json::from_slice(&bytes).map_err(|_| storage_error())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        _ => Err(storage_error()),
    }
}
fn path(state: &AppState, id: Uuid) -> PathBuf {
    directory(state).join(format!("{id}.json"))
}

fn callback_permission(purpose: Purpose, permission: &str) -> bool {
    meerschaum_core::access::ALL_PERMISSIONS.contains(&permission)
        && (permission.starts_with("jobs:")
            || matches!(permission, "actions:execute" | "webterm:use" | "logs:read")
            || (purpose == Purpose::Command
                && (permission.starts_with("pipes:")
                    || matches!(permission, "sql:read" | "connectors:read" | "plugins:read"))))
}

fn load(state: &AppState, id: Uuid) -> Result<Grant, ApiError> {
    read_authority(state, id, true)
}

fn read_authority(state: &AppState, id: Uuid, active: bool) -> Result<Grant, ApiError> {
    use std::io::Read;
    let path = path(state, id);
    let meta = std::fs::symlink_metadata(&path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            refused()
        } else {
            storage_error()
        }
    })?;
    if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > MAX_GRANT_BYTES {
        return Err(storage_error());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o077 != 0 {
            return Err(storage_error());
        }
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| storage_error())?
        .take(MAX_GRANT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| storage_error())?;
    if bytes.len() as u64 > MAX_GRANT_BYTES {
        return Err(storage_error());
    }
    let grant: Grant = serde_json::from_slice(&bytes).map_err(|_| storage_error())?;
    if grant.id != id
        || id.is_nil()
        || grant.launch_id.is_nil()
        || grant.depth > 8
        || grant.version != env!("CARGO_PKG_VERSION")
        || grant.team_id.is_some_and(|id| id.is_nil())
        || (active && grant.lease_until <= Utc::now().timestamp())
        || grant
            .permissions
            .iter()
            .any(|p| !callback_permission(grant.purpose, p))
        || grant.required.len() > 64
        || grant.graph_tracking > 1
        || grant.graph_pending && (grant.graph_tracking != 1 || grant.purpose != Purpose::Command)
        || grant.graph_origin.as_ref().is_some_and(|origin| {
            grant.graph_tracking != 1
                || grant.purpose != Purpose::Command
                || origin.grant.is_nil()
                || origin.launch.is_nil()
                || origin.context_revision == 0
        })
    {
        return Err(refused());
    }
    if let Some(ceiling) = &grant.source_ceiling {
        ceiling.validate().map_err(|_| refused())?;
    }
    Ok(grant)
}

fn write(state: &AppState, grant: &Grant) -> Result<(), ApiError> {
    meerschaum_core::team_scope::create_private_dir(&directory(state))
        .map_err(|_| storage_error())?;
    let json = serde_json::to_string(grant).map_err(|_| storage_error())?;
    if json.len() as u64 > MAX_GRANT_BYTES {
        return Err(storage_error());
    }
    meerschaum_core::team_scope::write_private(&path(state, grant.id), &json)
        .map_err(|_| storage_error())
}

fn token_proof(state: &AppState, token: &meerschaum_connector::TokenModel) -> Option<String> {
    token
        .secret_hash
        .as_ref()
        .map(|hash| auth::credential_fingerprint(state.secret(), &format!("token:{hash}")))
}

async fn token_owner(
    state: &AppState,
    token: &meerschaum_connector::TokenModel,
) -> Result<Option<UserBinding>, ApiError> {
    let Some(id) = token.user_id.as_ref().filter(|id| id.truthy()) else {
        return Ok(None);
    };
    let username = state
        .instance()
        .get_username(id)
        .await
        .map_err(|_| storage_error())?
        .ok_or_else(refused)?
        .py_str();
    let principal = auth::load_user(state, &username)
        .await?
        .ok_or_else(refused)?;
    let user = principal.as_user().ok_or_else(refused)?;
    if user.user_id.as_ref() != Some(id) {
        return Err(refused());
    }
    Ok(Some(UserBinding {
        username,
        user_id: id.clone(),
        credential: auth::user_credential(state, user).await?,
    }))
}

async fn origin(state: &AppState, caller: &Access) -> Result<(Origin, u8), ApiError> {
    let principal = caller.auth.as_ref().ok_or_else(refused)?;
    match principal {
        Principal::User(user) => {
            let original_id = user.user_id.clone();
            let credential = caller.credential.clone().ok_or_else(refused)?;
            let current = auth::load_user(state, &user.username)
                .await?
                .ok_or_else(refused)?;
            let user = current.as_user().ok_or_else(refused)?;
            if original_id
                .as_ref()
                .is_some_and(|id| Some(id) != user.user_id.as_ref())
                || auth::user_credential(state, user).await?.as_ref() != Some(&credential)
            {
                return Err(refused());
            }
            Ok((
                Origin::User {
                    username: user.username.clone(),
                    user_id: user.user_id.clone().ok_or_else(refused)?,
                    credential,
                },
                0,
            ))
        }
        Principal::Token(token) => {
            let id = token.id.ok_or_else(refused)?;
            let Some(Principal::Token(current)) = auth::load_token(state, &id.to_string()).await?
            else {
                return Err(refused());
            };
            if current.user_id != token.user_id
                || current.creation != token.creation
                || (token.secret_hash.is_some()
                    && token_proof(state, &current) != token_proof(state, token))
            {
                return Err(refused());
            }
            Ok((
                Origin::Token {
                    id,
                    user_id: token.user_id.clone(),
                    creation: token.creation,
                    proof: token_proof(state, token),
                    owner: token_owner(state, &current).await?,
                },
                0,
            ))
        }
        Principal::Execution(identity) => {
            let current = load_identity(state, identity.grant.id, identity.grant.launch_id).await?;
            if current.grant.depth >= 8 {
                return Err(refused());
            }
            Ok((current.grant.origin, current.grant.depth + 1))
        }
    }
}

/// Register one already authorized launch. `required` records the route permissions that
/// must remain held for this execution; child jobs retain their own canonical launch lifetime.
pub async fn register(
    state: &AppState,
    caller: &Access,
    launch_id: Uuid,
    team_id: Option<Uuid>,
    required: &[&str],
    purpose: Purpose,
) -> Result<Grant, ApiError> {
    register_with_sources(
        state,
        caller,
        launch_id,
        team_id,
        required,
        purpose,
        super::source_ceiling::Ceiling::default(),
    )
    .await
}

/// Main binds the exact actual source view before minting any child/runtime credential.
/// The immutable parent snapshot, not fresh Team additions, bounds derived source authority.
pub async fn register_with_sources(
    state: &AppState,
    caller: &Access,
    launch_id: Uuid,
    team_id: Option<Uuid>,
    required: &[&str],
    purpose: Purpose,
    sources: super::source_ceiling::Ceiling,
) -> Result<Grant, ApiError> {
    register_sources(
        state,
        caller,
        launch_id,
        team_id,
        required,
        sources,
        RegistrationKind::Ordinary(purpose),
    )
    .await
}

/// Main-only direct graph reservation. Credential issuance remains closed until
/// Main atomically stores the exact plan/windows and binds their canonical origin.
pub async fn register_graph_with_sources(
    state: &AppState,
    caller: &Access,
    launch_id: Uuid,
    team_id: Option<Uuid>,
    required: &[&str],
    sources: super::source_ceiling::Ceiling,
) -> Result<Grant, ApiError> {
    register_sources(
        state,
        caller,
        launch_id,
        team_id,
        required,
        sources,
        RegistrationKind::Graph,
    )
    .await
}

enum RegistrationKind {
    Ordinary(Purpose),
    Graph,
}

async fn register_sources(
    state: &AppState,
    caller: &Access,
    launch_id: Uuid,
    team_id: Option<Uuid>,
    required: &[&str],
    sources: super::source_ceiling::Ceiling,
    kind: RegistrationKind,
) -> Result<Grant, ApiError> {
    let (purpose, graph_pending) = match kind {
        RegistrationKind::Ordinary(purpose) => (purpose, false),
        RegistrationKind::Graph => (Purpose::Command, true),
    };
    sources.validate().map_err(|_| refused())?;
    if !sources.within_parent(caller) {
        return Err(ApiError::new(
            403,
            "This launch cannot adopt new plugin sources or versions. Recreate it explicitly using the original credentials.",
        ));
    }
    if required.is_empty()
        || graph_pending && !required.contains(&"pipes:write")
        || !required.iter().all(|permission| caller.has(permission))
        || state.config.no_auth
        || caller.no_auth
        || launch_id.is_nil()
        || team_id.is_some_and(|id| id.is_nil())
    {
        return Err(refused());
    }
    if let Some(Principal::Execution(parent)) = &caller.auth
        && (parent.grant.team_id.is_some_and(|id| Some(id) != team_id)
            || parent.grant.purpose != purpose)
    {
        return Err(refused());
    }
    let permissions = meerschaum_core::access::ALL_PERMISSIONS
        .iter()
        .filter(|p| callback_permission(purpose, p) && caller.has(p))
        .map(|p| p.to_string())
        .collect::<BTreeSet<_>>();
    if permissions.is_empty() {
        return Err(refused());
    }
    let (origin, depth) = origin(state, caller).await?;
    let graph_origin = if let Some(Principal::Execution(parent)) = &caller.auth {
        let current = load_identity(state, parent.grant.id, parent.grant.launch_id).await?;
        if current.grant.graph_tracking != 1 {
            return Err(ApiError::unavailable(
                "This older execution has unknown graph provenance. Recreate it using its original credentials before deriving another execution.",
            ));
        }
        if current.grant.graph_pending {
            return Err(ApiError::unavailable(
                "Main must finish binding this graph to its execution plan before it can start a child execution.",
            ));
        }
        current.grant.graph_origin
    } else {
        None
    };
    let selection = if let Some(id) = team_id {
        let team = state
            .instance()
            .get_teams()
            .await
            .map_err(|_| storage_error())?
            .into_iter()
            .find(|t| t.id.as_deref().and_then(|s| Uuid::parse_str(s).ok()) == Some(id))
            .ok_or_else(refused)?;
        Some(team.execution().map_err(|_| refused())?)
    } else {
        None
    };
    let generation = team_id
        .map(|id| generation(state, id))
        .transpose()?
        .flatten();
    if selection.as_ref().is_some_and(|s| s.mode.is_remote()) && generation.is_none() {
        return Err(refused());
    }
    let mut required: BTreeSet<String> = required.iter().map(|p| p.to_string()).collect();
    if let Some(Principal::Execution(parent)) = &caller.auth {
        required.extend(parent.grant.required.iter().cloned());
    }
    let grant = Grant {
        id: Uuid::new_v4(),
        launch_id,
        team_id,
        generation,
        selection,
        version: env!("CARGO_PKG_VERSION").into(),
        permissions,
        required,
        lease_until: Utc::now().timestamp() + LEASE_SECONDS,
        depth,
        origin,
        purpose,
        source_ceiling: Some(sources),
        graph_origin,
        graph_tracking: 1,
        graph_pending,
    };
    let _ = validate(state, grant.clone()).await?;
    write(state, &grant)?;
    Ok(grant)
}

/// Bind a direct canonical graph after Main issues its exact plan/windows and
/// before any credential is delivered. A worker/HTTP route never invokes this.
/// Restart can replace ONLY its exact previous self binding; an inherited graph
/// cannot be discarded to create a wider nested graph.
pub async fn bind_graph_origin(
    state: &AppState,
    id: Uuid,
    launch_id: Uuid,
    revision: u64,
    previous: Option<super::pipe_operation::CallbackBinding>,
) -> Result<Grant, ApiError> {
    if revision == 0
        || previous.as_ref().is_some_and(|old| {
            old.launch != launch_id
                || old.context_revision.checked_add(1) != Some(revision)
                || old.grant == id
        })
    {
        return Err(refused());
    }
    let expected = load(state, id)?;
    if expected.launch_id != launch_id
        || expected.purpose != Purpose::Command
        || !expected.required.contains("pipes:write")
        || expected.graph_tracking != 1
        || !expected.graph_pending
        || expected.graph_origin != previous
    {
        return Err(ApiError::new(
            403,
            "The graph cannot replace an inherited or changed originating window. Nested graphs need Main's exact intersection admission.",
        ));
    }
    let _ = validate(state, expected.clone()).await?;
    let _lock = super::storage::lock(directory(state).join("locks"), id)
        .await
        .map_err(|_| storage_error())?;
    let mut current = load(state, id)?;
    if current != expected {
        return Err(refused());
    }
    current.graph_origin = Some(super::pipe_operation::CallbackBinding {
        grant: id,
        launch: launch_id,
        context_revision: revision,
    });
    current.graph_pending = false;
    write(state, &current)?;
    Ok(current)
}

/// Rotate a still-live main-owned lease; deletion is final, and cannot be undone by a late
/// worker that was preparing a renewal. The canonical supervisor decides whether to call this.
pub async fn renew(state: &AppState, id: Uuid, launch_id: Uuid) -> Result<Grant, ApiError> {
    let expected = load(state, id)?;
    if expected.launch_id != launch_id {
        return Err(refused());
    }
    let _ = validate(state, expected.clone()).await?;
    let _lock = super::storage::lock(directory(state).join("locks"), id)
        .await
        .map_err(|_| storage_error())?;
    let mut current = load(state, id)?;
    if current != expected {
        return Err(refused());
    }
    current.lease_until = Utc::now().timestamp() + LEASE_SECONDS;
    write(state, &current)?;
    Ok(current)
}

/// Stop a persistent job's callbacks while retaining its original source proof for a later,
/// explicitly authorized restart. Ordinary renewal and bearer validation refuse suspension.
pub async fn suspend(state: &AppState, id: Uuid, launch_id: Uuid) -> Result<(), ApiError> {
    let _lock = super::storage::lock(directory(state).join("locks"), id)
        .await
        .map_err(|_| storage_error())?;
    let mut grant = read_authority(state, id, false)?;
    if grant.launch_id != launch_id {
        return Err(refused());
    }
    grant.lease_until = 0;
    write(state, &grant)
}

/// Main's canonical job supervisor may resume the SAME source identity after checking its
/// stored definition. A new grant UUID invalidates every old bearer and renewal, and source
/// password/token/team/permissions are all checked again. There is no public mint endpoint.
pub async fn resume(state: &AppState, id: Uuid, launch_id: Uuid) -> Result<Grant, ApiError> {
    let _lock = super::storage::lock(directory(state).join("locks"), id)
        .await
        .map_err(|_| storage_error())?;
    let mut grant = read_authority(state, id, false)?;
    if grant.launch_id != launch_id {
        return Err(refused());
    }
    let mut suspended = grant.clone();
    suspended.lease_until = 0;
    if grant
        .graph_origin
        .as_ref()
        .is_some_and(|origin| origin.grant == grant.id && origin.launch == grant.launch_id)
    {
        grant.graph_pending = true;
    }
    grant.id = Uuid::new_v4();
    grant.lease_until = Utc::now().timestamp() + LEASE_SECONDS;
    let _ = validate(state, grant.clone()).await?;
    // Keep the old, inactive source proof until the canonical caller persists the new ID.
    // A crash between these two writes cannot resurrect old tokens or lose restart identity.
    write(state, &suspended)?;
    write(state, &grant)?;
    Ok(grant)
}

/// Revoke only the UUID main registered, never a path or identity selected by an executor.
pub async fn revoke(state: &AppState, id: Uuid) -> Result<(), ApiError> {
    let _lock = super::storage::lock(directory(state).join("locks"), id)
        .await
        .map_err(|_| storage_error())?;
    match std::fs::remove_file(path(state, id)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(storage_error()),
    }
}

/// Issue a short-lived bearer, suitable for the API client's rotating credential file. The
/// secret key and source user's/token's reusable credentials never leave main.
pub async fn credential(
    state: &AppState,
    id: Uuid,
    launch_id: Uuid,
) -> Result<meerschaum_connector_api::models::ExecutionCredential, ApiError> {
    let identity = load_identity(state, id, launch_id).await?;
    if identity.grant.graph_pending {
        return Err(ApiError::unavailable(
            "Main has not completed this graph's exact plan, windows and source binding.",
        ));
    }
    let now = Utc::now().timestamp();
    let expires_at = (now + TOKEN_SECONDS).min(identity.grant.lease_until);
    let claims = Map::from_iter([
        ("sub".into(), id.to_string().into()),
        (auth::PRINCIPAL_CLAIM.into(), "execution".into()),
        ("aud".into(), AUDIENCE.into()),
        ("mrsm_launch".into(), launch_id.to_string().into()),
        ("iat".into(), now.into()),
        ("exp".into(), expires_at.into()),
        (
            "scopes".into(),
            Value::List(
                identity
                    .grant
                    .permissions
                    .iter()
                    .cloned()
                    .map(Value::from)
                    .collect(),
            ),
        ),
    ]);
    Ok(meerschaum_connector_api::models::ExecutionCredential {
        token: auth::jwt_encode(&claims, state.secret()),
        expires_at,
    })
}

/// Refresh a launch's authority for an internal host call. This never mints a credential and
/// cannot bypass the live origin, team, permission and lease checks.
pub async fn load_identity(
    state: &AppState,
    id: Uuid,
    launch_id: Uuid,
) -> Result<Identity, ApiError> {
    if state.config.no_auth {
        return Err(refused());
    }
    let grant = load(state, id)?;
    if grant.launch_id != launch_id {
        return Err(refused());
    }
    validate(state, grant).await
}

/// Recheck the original source of a canonical stopped job before admitting its stored command.
/// Suspended/expired leases are permitted ONLY for this check. This does not mint a credential,
/// reactivate callbacks, authorize a bearer or prove a running launch. Ordinary reads,
/// renewals and executor requests must continue to use `load_identity`.
pub async fn load_admission_identity(
    state: &AppState,
    id: Uuid,
    launch_id: Uuid,
) -> Result<Identity, ApiError> {
    if state.config.no_auth {
        return Err(refused());
    }
    let grant = read_authority(state, id, false)?;
    if grant.launch_id != launch_id {
        return Err(refused());
    }
    validate(state, grant).await
}

async fn validate(state: &AppState, grant: Grant) -> Result<Identity, ApiError> {
    let parent = match &grant.origin {
        Origin::User {
            username,
            user_id,
            credential,
        } => {
            let principal =
                auth::load_from_bound_subject(state, username, Some("user"), Some(credential))
                    .await?
                    .ok_or_else(refused)?;
            if principal.as_user().and_then(|u| u.user_id.as_ref()) != Some(user_id) {
                return Err(refused());
            }
            principal
        }
        Origin::Token {
            id,
            user_id,
            creation,
            proof,
            owner,
        } => {
            let Some(Principal::Token(token)) = auth::load_token(state, &id.to_string()).await?
            else {
                return Err(refused());
            };
            if &token.user_id != user_id
                || &token.creation != creation
                || (proof.is_some() && token_proof(state, &token) != *proof)
                || token_owner(state, &token).await?.as_ref() != owner.as_ref()
            {
                return Err(refused());
            }
            Principal::Token(token)
        }
    };
    let scopes = Scopes::List(auth::stored_scopes(state, &parent).await?);
    let current = Box::pin(crate::access::access_of_fresh(state, parent, &scopes)).await?;
    if !grant
        .required
        .iter()
        .all(|permission| current.has(permission))
    {
        return Err(refused());
    }
    let team = if let Some(id) = grant.team_id {
        let team = state
            .instance()
            .get_teams()
            .await
            .map_err(|_| storage_error())?
            .into_iter()
            .find(|t| t.id.as_deref().and_then(|s| Uuid::parse_str(s).ok()) == Some(id))
            .ok_or_else(refused)?;
        if Some(team.execution().map_err(|_| refused())?) != grant.selection
            || generation(state, id)? != grant.generation
        {
            return Err(refused());
        }
        if !current.is_admin() && !current.in_team(Some(&team.name)) {
            return Err(refused());
        }
        Some(team)
    } else {
        None
    };
    let transport_connectors = transport_connectors(state, &grant, &current, team.as_ref())?;
    let identity = Identity {
        grant,
        current: Box::new(current),
        team,
        transport_connectors,
    };
    if let Some(ceiling) = &identity.grant.source_ceiling
        && !ceiling.current(&identity.caller(), identity.team.as_ref())
    {
        return Err(refused());
    }
    Ok(identity)
}

/// A normal host team may use shared API-served instances without acquiring the open side's
/// visibility. A multi-team source cannot carry a sibling's private connector into this team.
fn transport_connectors(
    state: &AppState,
    grant: &Grant,
    original: &Access,
    team: Option<&meerschaum_core::access::Team>,
) -> Result<BTreeSet<String>, ApiError> {
    let mut allowed = BTreeSet::new();
    let Some(team) = team.filter(|team| {
        grant.purpose == Purpose::Command
            && !team.is_isolated()
            && grant
                .selection
                .as_ref()
                .is_some_and(|selection| selection.mode == meerschaum_core::execution::Mode::Host)
    }) else {
        return Ok(allowed);
    };
    for (kind, labels) in state.connectors().iter() {
        if !matches!(kind.as_str(), "sql" | "api" | "valkey") {
            continue;
        }
        for (label, attributes) in labels.to_map() {
            let keys = format!("{kind}:{label}");
            if attributes.as_map().is_none()
                || meerschaum_core::team_scope::api_execution::split_keys(&keys).is_err()
                || (!state.config.allow_multiple_instances && keys != state.config.instance_keys)
                || !state
                    .config
                    .allowed_instance_keys
                    .iter()
                    .any(|pattern| crate::config::fnmatch(&keys, pattern))
                || !crate::access::guard::may_use_connector(state, original, &keys)
                || crate::access::guard::connector_teams(state, &keys).is_some_and(|teams| {
                    !teams.iter().any(|name| {
                        meerschaum_core::access::same_team(Some(name), Some(&team.name))
                    })
                })
            {
                continue;
            }
            if allowed.len() >= 1024 {
                return Err(refused());
            }
            allowed.insert(keys);
        }
    }
    Ok(allowed)
}

/// Project a verified execution onto current permissions and one freshly resolved team.
pub(crate) fn access(identity: &Identity, granted: &Scopes) -> Access {
    let mut access = (*identity.current).clone();
    access.no_auth = false;
    access
        .principal
        .permissions
        .retain(|p| identity.grant.permissions.contains(p));
    let permissions = crate::access::intersect(&access.principal.permissions, granted);
    access.principal.permissions = permissions;
    let administrator = access.principal.is_admin;
    access.principal.is_admin = false;
    if let Some(team) = &identity.team {
        let role = if administrator {
            meerschaum_core::access::TeamRole::Manager
        } else {
            access
                .principal
                .team_role(Some(&team.name))
                .unwrap_or(meerschaum_core::access::TeamRole::Member)
        };
        access.principal.teams = vec![(team.name.clone(), role)];
        access.primary_team = team.name.clone();
        access.team_records = vec![team.clone()];
        access.principal = access.principal.within_team(&team.name);
    }
    let mut grants = meerschaum_core::access::TeamGrants::of(
        &access.principal.teams,
        &access.team_records,
        Some(&access.primary_team),
    );
    grants
        .connectors
        .extend(identity.transport_connectors.iter().cloned());
    if grants.isolated {
        grants.plugins.retain(|name| {
            identity
                .grant
                .source_ceiling
                .as_ref()
                .is_some_and(|ceiling| ceiling.permits_name(name))
        });
    } else {
        // An immutable Team boundary also walls an otherwise open Host principal. Its
        // original source set was already selected from the open user's actual files;
        // preserve only those captured names rather than adopting every installed source.
        grants.plugins = identity
            .grant
            .source_ceiling
            .as_ref()
            .map(|ceiling| ceiling.names())
            .unwrap_or_default();
    }
    if identity.grant.purpose == Purpose::Command {
        grants.runs_code = false;
    }
    access.apply_grants(grants);
    access.auth = Some(Principal::Execution(Box::new(identity.clone())));
    access
}

#[cfg(test)]
mod history_tests {
    use super::*;

    #[test]
    fn command_callbacks_validate_terminal_lines_without_opening_execution_routes() {
        use axum::http::Method;
        assert!(route_allowed(
            Purpose::Command,
            &Method::POST,
            "/execute/validate"
        ));
        for purpose in [Purpose::Jobs, Purpose::Command] {
            for method in [Method::GET, Method::POST, Method::PUT, Method::DELETE] {
                for path in [
                    "/execute",
                    "/execute/validate/extra",
                    "/actions/show",
                    "/actions",
                ] {
                    assert!(
                        !route_allowed(purpose, &method, path),
                        "{purpose:?} {method} {path}"
                    );
                }
                if purpose == Purpose::Jobs || method != Method::POST {
                    assert!(!route_allowed(purpose, &method, "/execute/validate"));
                }
            }
        }
    }

    #[test]
    fn unowned_token_history_does_not_follow_reused_ids_or_rotated_secrets() {
        let id = Uuid::new_v4();
        let token: meerschaum_connector::TokenModel = serde_json::from_value(serde_json::json!({
            "id": id, "creation": Utc::now(), "secret_hash": "synthetic-stored-hash-one",
            "scopes": ["jobs:read"], "is_valid": true
        }))
        .unwrap();
        let binding = OwnerBinding::Token {
            id,
            creation: token.creation,
            hash: history_hash(&token),
        };
        let mut caller = Access::everything();
        caller.no_auth = false;
        caller.auth = Some(Principal::Token(token.clone()));
        assert!(binding.matches(&caller));
        let mut rotated = token.clone();
        rotated.secret_hash = Some("synthetic-stored-hash-two".into());
        caller.auth = Some(Principal::Token(rotated));
        assert!(!binding.matches(&caller));
        let mut recreated = token;
        recreated.creation = recreated
            .creation
            .map(|time| time + chrono::Duration::seconds(1));
        caller.auth = Some(Principal::Token(recreated));
        assert!(!binding.matches(&caller));
    }
}
