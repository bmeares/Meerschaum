//! Who may see and do what through the API (the access model, contract K3).
//!
//! Every request resolves an [`Access`]: the authenticated principal ([`crate::auth::Principal`],
//! a user or a token) and the rules' [`rules::Principal`] (username, administrator flag,
//! effective permissions, teams). Routes, MCP tools, the engine and Pipelines routes ask it
//! [`Access::can_see`] / [`Access::can_edit`] / [`Access::can_manage_access`] /
//! [`Access::can_lock`] about a pipe's parameters. The rules are
//! [`meerschaum_core::access`]'s ([`rules`]).
//!
//! - **Effective permissions** (AC2): an administrator holds every permission; otherwise the
//!   user's explicit permissions (`attributes.scopes`), else its stored role
//!   (`attributes.role`), else the server's default role (`api:permissions:default_role`).
//!   A credential's own scopes (a JWT's `scopes` claim, an API key's scopes) are intersected with
//!   them at use time ([`rules::token_permissions`]), so a narrowed role or a removed permission
//!   applies to existing tokens at once. Scope lists written before 5.0 keep what they allowed
//!   (`pipes:write` also registers pipes and sets SQL definitions; `actions:execute` also opens
//!   the web terminal).
//! - **API keys** act as their owner (they see what the owner sees), within their scopes; a key
//!   whose owner was deleted is refused; an unowned key (made locally) holds its own scopes in
//!   Everyone.
//! - **Caching:** the identity behind a credential (user type, attributes, teams) is kept for
//!   [`IDENTITY_TTL`] and dropped when this server changes a user or a team; a login, a new
//!   API key and running actions read it fresh ([`access_of_fresh`]), and a read that fails is
//!   a `503`, never a guess. The pipes' access is read in one bulk read per instance and kept
//!   for [`INDEX_TTL`].
//! - **`--no-auth`:** everyone is an administrator.
//!
//! The local CLI and Python API are not filtered (an instance's credentials are the trust
//! boundary); locks apply everywhere and are enforced by the core.

pub mod guard;
mod history;
pub mod rules;
pub mod runs;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, Value};

use crate::auth::{self, Principal as AuthPrincipal, Scopes};
use crate::error::ApiError;
use crate::state::{AppState, PipesDict};

pub use rules::{EVERYONE, Lock, Locks, PipeAccess, Role, TeamRole};

/// How long the identity behind a credential (type, attributes, teams) is reused.
pub const IDENTITY_TTL: Duration = Duration::from_secs(5);

/// How long an instance's pipes' access (one bulk read) is reused.
pub const INDEX_TTL: Duration = Duration::from_secs(3);

/// The user attributes that hold access rights: only administrators set them through the API
/// (self-registration and a user's own edits keep the stored values).
pub const RESERVED_USER_ATTRIBUTES: &[&str] = &[
    rules::SCOPES_ATTRIBUTE,
    rules::ROLE_ATTRIBUTE,
    "permissions",
    "teams",
];

/// Who a credential belongs to, as read from the instance.
#[derive(Debug, Clone)]
struct Identity {
    username: String,
    user_type: Option<String>,
    attributes: Option<Map>,
    teams: Vec<(String, TeamRole)>,
}

/// The caches behind [`resolve`] and [`pipe_index`] (one per server state).
#[derive(Debug, Default)]
pub struct AccessCache {
    identities: Mutex<HashMap<String, (Instant, Identity)>>,
    indexes: Mutex<HashMap<String, (Instant, Arc<PipeIndex>)>>,
}

impl AccessCache {
    /// Forget every cached identity (a user, role or team changed).
    pub fn forget_identities(&self) {
        self.identities
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// Forget the cached pipes' access of every instance (a pipe's access changed).
    pub fn forget_pipes(&self) {
        self.indexes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

/// The access of one request.
#[derive(Debug, Clone)]
pub struct Access {
    /// The authenticated principal (`None` under `--no-auth`).
    pub auth: Option<AuthPrincipal>,
    /// The rules' principal. Its `permissions` are the request's: an administrator's API key
    /// holds only its scopes (its `is_admin` is the owner's, for what it may see).
    pub principal: rules::Principal,
    /// The caller's primary team (the default team of the pipes it creates).
    pub primary_team: String,
    /// The caller's stored attributes (`None` for an unowned token, or `--no-auth`).
    pub attributes: Option<Map>,
    /// Under `--no-auth` (everyone is an administrator).
    pub no_auth: bool,
}

impl Access {
    /// The access of everyone under `--no-auth` (and of the server itself).
    pub fn everything() -> Access {
        Access {
            auth: None,
            principal: rules::Principal::admin(""),
            primary_team: EVERYONE.to_string(),
            attributes: None,
            no_auth: true,
        }
    }

    /// Whether the caller is an administrator (everyone is under `--no-auth`).
    pub fn is_admin(&self) -> bool {
        self.principal.is_admin
    }

    /// The caller's username (a token's owner for an API key; `None` when anonymous).
    pub fn username(&self) -> Option<&str> {
        Some(self.principal.username.as_str()).filter(|u| !u.is_empty())
    }

    /// Whether the caller holds `permission` (an administrator's API key: only its scopes).
    pub fn has(&self, permission: &str) -> bool {
        self.principal.permissions.contains(permission)
            || self.principal.permissions.contains(rules::ALL_SCOPE)
            || (self.full_admin() && !rules::ALL_PERMISSIONS.contains(&permission))
    }

    /// An administrator with every permission (a password login, a `*` key, `--no-auth`).
    pub fn full_admin(&self) -> bool {
        self.principal.is_admin
            && rules::ALL_PERMISSIONS
                .iter()
                .all(|p| self.principal.permissions.contains(*p))
    }

    /// Require every permission of `scopes`, each also satisfied by its legacy spelling in
    /// `aliases` (`(permission, legacy)`): `403 Missing required scope: '...'` otherwise.
    pub fn require(&self, scopes: &[&str], aliases: &[(&str, &str)]) -> Result<(), ApiError> {
        for scope in scopes {
            let legacy = aliases
                .iter()
                .find(|(current, _)| current == scope)
                .map(|(_, legacy)| *legacy);
            if !self.has(scope) && !legacy.is_some_and(|l| self.has(l)) {
                return Err(ApiError::new(
                    403,
                    format!("Missing required scope: '{scope}'"),
                ));
            }
        }
        Ok(())
    }

    /// Whether the caller is in `team` (everyone is in Everyone).
    pub fn in_team(&self, team: Option<&str>) -> bool {
        self.principal.is_admin || self.principal.is_member(team)
    }

    /// Whether the caller manages `team` (administrators manage every team).
    pub fn manages(&self, team: Option<&str>) -> bool {
        rules::can_manage_team(&self.principal, team.unwrap_or(EVERYONE))
    }

    /// Whether the caller may see a pipe with these parameters.
    pub fn can_see(&self, parameters: &Map) -> bool {
        rules::can_see(&self.principal, &PipeAccess::from_parameters(parameters))
    }

    /// Whether a pipe may be exposed through the generic pipe routes. Internal state has
    /// dedicated routes which filter its rows; its raw tables must not bypass those checks.
    /// Saved Pipelines use their dedicated routes, which also hide inaccessible members.
    pub fn can_see_pipe(&self, pipe: &Pipe) -> bool {
        self.can_see(pipe.raw_parameters())
            && (self.is_admin()
                || pipe.target(None).is_ok_and(|target| {
                    crate::sql_guard::check_target(
                        &target,
                        &crate::sql_guard::HiddenTables::default(),
                    )
                    .is_ok()
                }))
    }

    /// [`Access::can_see_pipe`] for an entry of the bulk pipe index.
    pub fn can_see_indexed(&self, pipe: &IndexedPipe) -> bool {
        self.can_see(&pipe.parameters)
            && (self.is_admin()
                || (pipe.connector_keys == "mrsm" && pipe.metric_key == "pipeline")
                || pipe.targets.iter().all(|target| {
                    crate::sql_guard::check_target(
                        target,
                        &crate::sql_guard::HiddenTables::default(),
                    )
                    .is_ok()
                }))
    }

    /// Whether the caller may change a pipe with these parameters (within its permissions).
    pub fn can_edit(&self, parameters: &Map) -> bool {
        rules::can_edit(&self.principal, &PipeAccess::from_parameters(parameters))
    }

    /// Whether the caller may change a pipe's `access`.
    pub fn can_manage_access(&self, parameters: &Map) -> bool {
        rules::can_manage_access(&self.principal, &PipeAccess::from_parameters(parameters))
    }

    /// Whether the caller may lock and unlock a pipe.
    pub fn can_lock(&self, parameters: &Map) -> bool {
        rules::can_lock(&self.principal, &PipeAccess::from_parameters(parameters))
    }

    /// Whether the caller may see the email of `username`, a user in `teams`.
    pub fn can_see_email(&self, username: &str, teams: &[(String, TeamRole)]) -> bool {
        self.no_auth || rules::can_see_email_of(&self.principal, username, teams)
    }

    /// The caller's permissions as a scopes list (`['*']` for an administrator with every
    /// permission).
    pub fn scopes(&self) -> Scopes {
        if self.full_admin() {
            Scopes::List(vec![rules::ALL_SCOPE.into()])
        } else {
            Scopes::List(rules::ordered_permissions(&self.principal.permissions))
        }
    }
}

/// The server's default role (`api:permissions:default_role`, as [`crate::config`] resolved it).
pub fn default_role(state: &AppState) -> Role {
    Role::parse(&state.config.default_role).unwrap_or(rules::DEFAULT_ROLE)
}

/// Scopes whose old spelling a credential may still hold: `(legacy, current)`. v4's job route
/// asked for the misspelled `josb:stop`; a token that holds it holds `jobs:stop`.
pub const LEGACY_SPELLINGS: &[(&str, &str)] = &[("josb:stop", "jobs:stop")];

/// The scopes a credential grants, as a list: a JWT's `scopes` string claim (an instance quirk)
/// holds the permissions it contains; a legacy spelling ([`LEGACY_SPELLINGS`]) also grants the
/// current one.
fn granted_list(granted: &Scopes) -> Vec<String> {
    let mut out = match granted {
        Scopes::List(l) => l.clone(),
        Scopes::Str(s) => {
            let mut out: Vec<String> = rules::ALL_PERMISSIONS
                .iter()
                .filter(|p| s.contains(**p))
                .map(|p| p.to_string())
                .collect();
            if s.contains('*') {
                out.push(rules::ALL_SCOPE.into());
            }
            out
        }
    };
    for (legacy, current) in LEGACY_SPELLINGS {
        if out.iter().any(|s| s == legacy) && !out.iter().any(|s| s == current) {
            out.push(current.to_string());
        }
    }
    out
}

/// `effective ∩ granted`: what a credential may do for its owner ([`rules::token_permissions`]:
/// lists written before 5.0 keep what they allowed). `*` on either side keeps the other side
/// whole: a `*` credential holds its owner's permissions (a plugin's own scopes included), and
/// an owner with `*` among their permissions lends a credential every scope it lists.
pub fn intersect(effective: &rules::Permissions, granted: &Scopes) -> rules::Permissions {
    let list = granted_list(granted);
    if list.iter().any(|s| s.trim() == rules::ALL_SCOPE) {
        return effective.clone();
    }
    if effective.contains(rules::ALL_SCOPE) {
        return rules::scopes_to_permissions(&list);
    }
    rules::token_permissions(&list, effective)
}

/// A user's effective permissions ([`rules::user_permissions`]), with `*` added for an
/// administrator and kept when a user's own permissions list it: they pass the checks of
/// plugins' own scopes too (as in v4), and their credentials keep every scope they list.
pub fn user_permissions(
    user_type: Option<&str>,
    attributes: Option<&Map>,
    default_role: Role,
) -> rules::Permissions {
    let mut out = rules::user_permissions(user_type, attributes, default_role);
    if rules::is_admin_type(user_type)
        || rules::explicit_scopes(attributes)
            .is_some_and(|scopes| scopes.iter().any(|s| s.trim() == rules::ALL_SCOPE))
    {
        out.insert(rules::ALL_SCOPE.to_string());
    }
    out
}

/// The teams of `username` on `conn` with their roles (Everyone included).
pub async fn user_teams(
    conn: &Arc<dyn InstanceConnector>,
    username: &str,
) -> Result<Vec<(String, TeamRole)>, meerschaum_connector::ConnectorError> {
    let mut teams = conn.get_user_teams(username).await?;
    if !teams.iter().any(|(t, _)| rules::is_everyone(Some(t))) {
        teams.insert(0, (EVERYONE.to_string(), TeamRole::Member));
    }
    Ok(teams)
}

fn identity_unavailable(e: impl std::fmt::Display) -> ApiError {
    auth::scopes_unavailable(e)
}

/// Read the identity of a user (type, attributes, teams), with the cache (`fresh`: read it from
/// the instance, and keep it for the next requests).
async fn user_identity(
    state: &AppState,
    username: &str,
    fresh: bool,
) -> Result<Identity, ApiError> {
    let key = format!("user:{username}");
    if !fresh && let Some(identity) = cached_identity(state, &key) {
        return Ok(identity);
    }
    let conn = state.instance();
    let mut user = User::new(username, None);
    user.instance_keys = Some(conn.connector_keys());
    let user_type = conn
        .get_user_type(&user)
        .await
        .map_err(identity_unavailable)?;
    let attributes = conn
        .get_user_attributes(&user)
        .await
        .map_err(identity_unavailable)?;
    let teams = user_teams(&conn, username)
        .await
        .map_err(identity_unavailable)?;
    let identity = Identity {
        username: username.to_string(),
        user_type,
        attributes,
        teams,
    };
    store_identity(state, key, identity.clone());
    Ok(identity)
}

fn cached_identity(state: &AppState, key: &str) -> Option<Identity> {
    let cache = state
        .access_cache()
        .identities
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    cache
        .get(key)
        .filter(|(at, _)| at.elapsed() < IDENTITY_TTL)
        .map(|(_, identity)| identity.clone())
}

fn store_identity(state: &AppState, key: String, identity: Identity) {
    let mut cache = state
        .access_cache()
        .identities
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if cache.len() > 4096 {
        cache.retain(|_, (at, _)| at.elapsed() < IDENTITY_TTL);
    }
    cache.insert(key, (Instant::now(), identity));
}

/// The identity a token acts as: its owner's (`None` for an unowned token). A token whose owner
/// no longer exists is refused.
async fn token_identity(
    state: &AppState,
    token: &meerschaum_connector::TokenModel,
    fresh: bool,
) -> Result<Option<Identity>, ApiError> {
    let Some(user_id) = token.user_id.clone().filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let username = state
        .instance()
        .get_username(&user_id)
        .await
        .map_err(identity_unavailable)?
        .map(|v| v.py_str());
    let Some(username) = username else {
        return Err(ApiError::unauthorized(
            "Invalid or revoked token.",
            Some("Bearer"),
        ));
    };
    user_identity(state, &username, fresh).await.map(Some)
}

/// The access of a principal whose credential grants `granted`, from an identity read at most
/// [`IDENTITY_TTL`] ago.
pub async fn access_of(
    state: &AppState,
    principal: AuthPrincipal,
    granted: &Scopes,
) -> Result<Access, ApiError> {
    access_of_with(state, principal, granted, false).await
}

/// [`access_of`] from an identity read now (`503` when it cannot be read): what a login, a new
/// credential and running actions are decided on.
pub async fn access_of_fresh(
    state: &AppState,
    principal: AuthPrincipal,
    granted: &Scopes,
) -> Result<Access, ApiError> {
    access_of_with(state, principal, granted, true).await
}

async fn access_of_with(
    state: &AppState,
    principal: AuthPrincipal,
    granted: &Scopes,
    fresh: bool,
) -> Result<Access, ApiError> {
    let identity = match &principal {
        AuthPrincipal::User(user) => Some(user_identity(state, &user.username, fresh).await?),
        AuthPrincipal::Token(token) => token_identity(state, token, fresh).await?,
    };
    let (username, is_admin, permissions, teams, attributes) = match identity {
        Some(identity) => {
            let effective = user_permissions(
                identity.user_type.as_deref(),
                identity.attributes.as_ref(),
                default_role(state),
            );
            (
                identity.username,
                rules::is_admin_type(identity.user_type.as_deref()),
                intersect(&effective, granted),
                identity.teams,
                identity.attributes,
            )
        }
        // An unowned token (made locally): its own scopes (`*` kept: every scope, a plugin's
        // too), in Everyone only.
        None => {
            let list = granted_list(granted);
            let mut permissions = rules::scopes_to_permissions(&list);
            if list.iter().any(|s| s.trim() == rules::ALL_SCOPE) {
                permissions.insert(rules::ALL_SCOPE.to_string());
            }
            (
                String::new(),
                false,
                permissions,
                vec![(EVERYONE.to_string(), TeamRole::Member)],
                None,
            )
        }
    };
    let primary_team = rules::primary_team(attributes.as_ref(), &teams);
    Ok(Access {
        auth: Some(principal),
        principal: rules::Principal {
            username,
            is_admin,
            permissions,
            teams,
        },
        primary_team,
        attributes,
        no_auth: false,
    })
}

/// Resolve the access of a request (`401` without valid credentials, `503` when they cannot be
/// read). Under `--no-auth`: [`Access::everything`].
pub async fn resolve(state: &AppState, headers: &HeaderMap) -> Result<Access, ApiError> {
    if state.config.no_auth {
        return Ok(Access::everything());
    }
    let principal = auth::load_user_or_token(state, headers).await?;
    let granted = auth::granted_scopes(state, headers, &principal).await?;
    access_of(state, principal, &granted).await
}

/// Resolve the access of a request and require `scopes` (see [`Access::require`]).
pub async fn require(
    state: &AppState,
    headers: &HeaderMap,
    scopes: &[&str],
) -> Result<Access, ApiError> {
    require_with_aliases(state, headers, scopes, &[]).await
}

/// Resolve a request's current identity without the identity cache, then require its scopes.
pub async fn require_fresh(
    state: &AppState,
    headers: &HeaderMap,
    scopes: &[&str],
) -> Result<Access, ApiError> {
    let caller = if state.config.no_auth {
        Access::everything()
    } else {
        let principal = auth::load_user_or_token(state, headers).await?;
        let granted = auth::granted_scopes(state, headers, &principal).await?;
        access_of_fresh(state, principal, &granted).await?
    };
    caller.require(scopes, &[])?;
    Ok(caller)
}

/// [`require`] with legacy spellings.
pub async fn require_with_aliases(
    state: &AppState,
    headers: &HeaderMap,
    scopes: &[&str],
    aliases: &[(&str, &str)],
) -> Result<Access, ApiError> {
    let access = resolve(state, headers).await?;
    access.require(scopes, aliases)?;
    Ok(access)
}

/// The access of a request with credentials, or `None` without (an anonymous caller of a route
/// that serves one); invalid credentials are refused like on any route.
pub async fn resolve_optional(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<Access>, ApiError> {
    if state.config.no_auth {
        return Ok(Some(Access::everything()));
    }
    if !headers.contains_key(axum::http::header::AUTHORIZATION) {
        return Ok(None);
    }
    resolve(state, headers).await.map(Some)
}

// ------------------------------------------------------------------------------------------------
// Pipes
// ------------------------------------------------------------------------------------------------

/// One registered pipe of an instance, as far as access is concerned.
#[derive(Debug, Clone)]
pub struct IndexedPipe {
    /// The connector keys.
    pub connector_keys: String,
    /// The metric key.
    pub metric_key: String,
    /// The location key.
    pub location_key: Option<String>,
    /// The parameters.
    pub parameters: Map,
    /// The target table names (as configured and as the default).
    pub targets: Vec<String>,
}

impl IndexedPipe {
    /// `connector|metric|location` (`None` for no location).
    pub fn keys_ref(&self) -> String {
        format!(
            "{}|{}|{}",
            self.connector_keys,
            self.metric_key,
            self.location_key.as_deref().unwrap_or("None")
        )
    }
}

/// The registered pipes of an instance (one bulk read).
#[derive(Debug, Clone, Default)]
pub struct PipeIndex {
    /// The instance keys.
    pub instance_keys: String,
    /// The pipes.
    pub pipes: Vec<IndexedPipe>,
    /// The instance's database identifier clipping rule, also used for SQL definitions.
    pub identifier_byte_limit: Option<usize>,
}

impl PipeIndex {
    /// The pipe with these keys.
    pub fn get(&self, ck: &str, mk: &str, lk: Option<&str>) -> Option<&IndexedPipe> {
        self.pipes.iter().find(|p| {
            p.connector_keys == ck && p.metric_key == mk && p.location_key.as_deref() == lk
        })
    }

    /// The pipe a `pipe_ref` names (`instance|connector|metric|location` or without the
    /// instance; only the last three parts are compared).
    pub fn by_ref(&self, pipe_ref: &str) -> Option<&IndexedPipe> {
        let (ck, mk, lk) = split_ref(pipe_ref)?;
        self.get(ck, mk, lk)
    }

    /// The target names of the pipes `access` cannot see.
    pub fn hidden_targets(&self, access: &Access) -> crate::sql_guard::HiddenTables {
        if access.is_admin() {
            return crate::sql_guard::HiddenTables::default();
        }
        crate::sql_guard::HiddenTables::new(
            self.pipes
                .iter()
                .filter(|p| !access.can_see(&p.parameters))
                .flat_map(|p| p.targets.iter().cloned()),
        )
        .with_identifier_byte_limit(self.identifier_byte_limit)
    }

    /// How many pipes `access` cannot see.
    pub fn hidden_count(&self, access: &Access) -> usize {
        if access.is_admin() {
            return 0;
        }
        self.pipes
            .iter()
            .filter(|p| !access.can_see(&p.parameters))
            .count()
    }
}

/// `(connector, metric, location)` of a `pipe_ref` with or without its instance.
pub fn split_ref(pipe_ref: &str) -> Option<(&str, &str, Option<&str>)> {
    let parts: Vec<&str> = pipe_ref.split('|').collect();
    let (ck, mk, lk) = match parts.as_slice() {
        [_, ck, mk, lk] => (*ck, *mk, *lk),
        [ck, mk, lk] => (*ck, *mk, *lk),
        [ck, mk] => (*ck, *mk, "None"),
        _ => return None,
    };
    let lk = (!matches!(lk, "None" | "" | "null" | "[None]")).then_some(lk);
    Some((ck, mk, lk))
}

/// The target table names of a pipe (configured and default).
pub fn pipe_targets(pipe: &Pipe) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(t) = pipe.target(None) {
        out.push(t);
    }
    let legacy = pipe.target_legacy();
    if !out.contains(&legacy) {
        out.push(legacy);
    }
    out
}

/// The configured/default targets and the physical names an instance actually uses.
pub fn pipe_targets_on(conn: &dyn InstanceConnector, pipe: &Pipe) -> Vec<String> {
    let mut targets = pipe_targets(pipe);
    for target in conn.pipe_target_aliases(pipe) {
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    targets
}

pub(crate) fn index_from_dict(
    instance_keys: &str,
    dict: &PipesDict,
    conn: &dyn InstanceConnector,
) -> PipeIndex {
    let mut pipes = Vec::new();
    for (ck, metrics) in dict {
        for (mk, locations) in metrics {
            for (lk, handle) in locations {
                pipes.push(IndexedPipe {
                    connector_keys: ck.clone(),
                    metric_key: mk.clone(),
                    location_key: lk.clone(),
                    parameters: handle.pipe.raw_parameters().clone(),
                    targets: pipe_targets_on(conn, &handle.pipe),
                });
            }
        }
    }
    PipeIndex {
        instance_keys: instance_keys.to_string(),
        pipes,
        identifier_byte_limit: conn.sql_identifier_byte_limit(),
    }
}

/// The registered pipes of `instance_keys` (the server's instance by default) and their access:
/// one bulk read, reused for [`INDEX_TTL`].
pub async fn pipe_index(
    state: &AppState,
    instance_keys: Option<&str>,
) -> Result<Arc<PipeIndex>, ApiError> {
    let conn = state.get_api_connector(instance_keys)?;
    let keys = conn.connector_keys();
    {
        let cache = state
            .access_cache()
            .indexes
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some((at, index)) = cache.get(&keys)
            && at.elapsed() < INDEX_TTL
        {
            return Ok(index.clone());
        }
    }
    let dict = state.pipes(instance_keys, true).await?;
    let index = Arc::new(index_from_dict(&keys, &dict, conn.as_ref()));
    state
        .access_cache()
        .indexes
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(keys, (Instant::now(), index.clone()));
    Ok(index)
}

/// Whether `access` may see the pipe a `pipe_ref` names on `index` (a pipe that is not
/// registered there has the default access: visible).
pub fn can_see_ref(access: &Access, index: &PipeIndex, pipe_ref: &str) -> bool {
    if access.is_admin() {
        return true;
    }
    match index.by_ref(pipe_ref) {
        Some(p) => access.can_see_indexed(p),
        None => true,
    }
}

/// Who may see and change the pipes `pipe_ref`s name, across instances, within one request
/// (each instance's pipes are read once). A pipe on an instance this server does not serve to
/// the caller is hidden; a pipe that is not registered has the default access.
pub struct RefFilter {
    state: AppState,
    access: Access,
    default_instance: String,
    indexes: HashMap<String, Option<Arc<PipeIndex>>>,
    pipelines: Option<HashMap<String, String>>,
    registrations: HashMap<String, Option<HashMap<String, String>>>,
    run_generations: HashMap<String, bool>,
    strict_generations: bool,
    hidden_seen: bool,
    client_refs: bool,
}

impl RefFilter {
    /// A filter for `access`; refs without an instance are on `default_instance`.
    pub fn new(state: &AppState, access: &Access, default_instance: &str) -> RefFilter {
        RefFilter {
            state: state.clone(),
            access: access.clone(),
            default_instance: default_instance.to_string(),
            indexes: HashMap::new(),
            pipelines: None,
            registrations: HashMap::new(),
            run_generations: HashMap::new(),
            strict_generations: false,
            hidden_seen: false,
            client_refs: false,
        }
    }

    /// The caller's access.
    pub fn access(&self) -> &Access {
        &self.access
    }

    /// The exact selected registry used to interpret local metadata references.
    pub fn default_instance(&self) -> &str {
        &self.default_instance
    }

    /// Engine clients record refs with their own `api:<label>` for this server. Interpret
    /// those refs on the selected registry instead of requiring the server to know that label.
    pub fn for_engine(mut self) -> Self {
        self.client_refs = true;
        self
    }

    /// Whether this request has encountered an inaccessible member of its run or plan.
    pub fn has_hidden(&self) -> bool {
        self.hidden_seen
    }

    /// Whole-run metadata may name saved Pipelines whose member Pipes are public.
    /// Match every snapshot to a fresh, visible registration on this exact registry.
    /// Missing snapshots retain the legacy per-Pipe history policy.
    pub async fn pipeline_snapshots_visible(&mut self, selection: &serde_json::Value) -> bool {
        if self.access.is_admin() && !self.strict_generations {
            return true;
        }
        let snapshots = match selection.get("pipeline_definitions") {
            None | Some(serde_json::Value::Null) => return true,
            Some(serde_json::Value::Array(snapshots)) if snapshots.is_empty() => return true,
            Some(serde_json::Value::Array(snapshots)) => snapshots,
            _ => return false,
        };
        let Ok(conn) = self.state.get_api_connector(Some(&self.default_instance)) else {
            return false;
        };
        if guard::check_connector_teams(&self.state, &self.access, &conn.connector_keys()).is_err()
        {
            return false;
        }
        if self.pipelines.is_none() {
            let mut identities: HashMap<String, Option<String>> = HashMap::new();
            let listing = conn
                .registered_pipes(&meerschaum_connector::PipesKeysQuery {
                    connector_keys: vec!["mrsm".into()],
                    metric_keys: vec!["pipeline".into()],
                    ..Default::default()
                })
                .await;
            if let Ok(listing) = listing {
                for (_, entry) in listing {
                    let parameters = match entry.extra {
                        Some(Value::Map(parameters)) => parameters,
                        Some(Value::Str(text)) => match Value::from_json_str(&text) {
                            Ok(Value::Map(parameters)) => parameters,
                            _ => continue,
                        },
                        _ => continue,
                    };
                    let (Some(name), Some(Value::Map(definition))) =
                        (entry.location_key, parameters.get("pipeline"))
                    else {
                        continue;
                    };
                    let identity = match definition.get("id") {
                        // A name-only legacy identity cannot prove continuity after recreation.
                        None => continue,
                        Some(Value::Str(identity))
                            if !identity.is_empty() && !identity.starts_with("legacy:") =>
                        {
                            identity.clone()
                        }
                        _ => continue,
                    };
                    let visible_name = self.access.can_see(&parameters).then_some(name);
                    // A duplicate UUID is ambiguous even when its other registration is hidden.
                    identities
                        .entry(identity)
                        .and_modify(|name| *name = None)
                        .or_insert(visible_name);
                }
            }
            // A failed metadata read withholds the run, never assumes public access.
            self.pipelines = Some(
                identities
                    .into_iter()
                    .filter_map(|(identity, name)| name.map(|name| (identity, name)))
                    .collect(),
            );
        }
        let visible = self.pipelines.as_ref().expect("initialized");
        snapshots.iter().all(|snapshot| {
            if snapshot
                .get("id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|identity| identity.starts_with("legacy:"))
            {
                return false;
            }
            let Some(_) = snapshot.get("name").and_then(serde_json::Value::as_str) else {
                return false;
            };
            let Some(identity) = snapshot.get("id").and_then(serde_json::Value::as_str) else {
                return false;
            };
            visible.contains_key(identity)
                && meerschaum_connector::sync_metrics::snapshot_registry(snapshot)
                    == Some(conn.connector_keys().as_str())
        })
    }

    /// Whether any member of a stored engine run is visible. Raw event-table reads use this
    /// as well as checking each event's pipe, since run-level events have no `pipe_ref`.
    pub async fn run_visible(&mut self, run_id: &str) -> bool {
        let records = self
            .state
            .engine
            .get_runs(&crate::backends::RunsFilter {
                run_id: Some(run_id.to_string()),
                ..Default::default()
            })
            .await;
        match records {
            Ok(records) => !runs::visible_runs(self, records).await.is_empty(),
            Err(_) => false,
        }
    }

    async fn entry(&mut self, pipe_ref: &str) -> Option<Option<IndexedPipe>> {
        let parts: Vec<&str> = pipe_ref.split('|').collect();
        let instance = if parts.len() == 4 && !(self.client_refs && parts[0].starts_with("api:")) {
            parts[0].to_string()
        } else {
            self.default_instance.clone()
        };
        if !self.indexes.contains_key(&instance) {
            let index = pipe_index(&self.state, Some(&instance)).await.ok();
            self.indexes.insert(instance.clone(), index);
        }
        let index = self.indexes.get(&instance)?.as_ref()?;
        Some(index.by_ref(pipe_ref).cloned())
    }

    /// Whether the caller may see the pipe `pipe_ref` names.
    pub async fn visible(&mut self, pipe_ref: &str) -> bool {
        if self.access.is_admin() {
            return true;
        }
        let visible = match self.entry(pipe_ref).await {
            None => false,
            Some(None) => true,
            Some(Some(p)) => self.access.can_see_indexed(&p),
        };
        self.hidden_seen |= !visible;
        visible
    }

    /// Whether the caller may change the pipe `pipe_ref` names (`locked_sync`: whether its
    /// `sync` lock skips it anyway).
    pub async fn editable(&mut self, pipe_ref: &str) -> (bool, bool) {
        let entry = self.entry(pipe_ref).await;
        let locked = matches!(&entry, Some(Some(p)) if rules::Locks::from_parameters(&p.parameters).contains(rules::Lock::Sync));
        if self.access.is_admin() {
            return (true, locked);
        }
        let editable = match entry {
            None => false,
            Some(None) => true,
            Some(Some(p)) => self.access.can_see_indexed(&p) && self.access.can_edit(&p.parameters),
        };
        (editable, locked)
    }
}

/// The `404` of a pipe the caller cannot see (the same as a pipe that does not exist).
pub fn hidden_pipe(pipe: &Pipe) -> ApiError {
    ApiError::new(404, format!("{pipe} was not found."))
}

/// The `403` of a pipe the caller may see but not change.
pub fn not_editable(pipe: &Pipe) -> ApiError {
    ApiError::new(
        403,
        format!(
            "You cannot change {pipe}: only its owner, its team's managers and administrators may."
        ),
    )
}

/// The value a stored parameter key had (for comparing an edit with what is stored).
pub fn stored(parameters: Option<&Map>, key: &str) -> Option<Value> {
    parameters.and_then(|p| p.get(key)).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_capped_by_their_owners_permissions() {
        let editor = Role::Editor.permissions();
        let all = Scopes::List(vec!["*".into()]);
        assert_eq!(intersect(&editor, &all), editor);
        let narrow = Scopes::List(vec!["pipes:read".into(), "actions:execute".into()]);
        assert_eq!(
            intersect(&editor, &narrow).into_iter().collect::<Vec<_>>(),
            vec!["pipes:read".to_string()]
        );
        // `pipes:write` in a list written before 5.0 still registers pipes.
        let legacy = Scopes::List(vec!["pipes:write".into()]);
        assert!(intersect(&editor, &legacy).contains("pipes:register"));
        let text = Scopes::Str("[\"pipes:read\"]".into());
        assert_eq!(
            intersect(&editor, &text).into_iter().collect::<Vec<_>>(),
            vec!["pipes:read".to_string()]
        );
    }

    #[test]
    fn pipe_refs_split_with_or_without_instance() {
        assert_eq!(
            split_ref("sql:main|sql:a|w|None"),
            Some(("sql:a", "w", None))
        );
        assert_eq!(
            split_ref("sql:a|w|home"),
            Some(("sql:a", "w", Some("home")))
        );
        assert_eq!(split_ref("x"), None);
    }

    #[test]
    fn an_administrators_narrow_key_sees_everything_but_holds_its_scopes() {
        let mut access = Access::everything();
        access.no_auth = false;
        access.principal.permissions = ["pipes:read".to_string()].into_iter().collect();
        assert!(access.is_admin() && !access.full_admin());
        assert!(access.has("pipes:read") && !access.has("pipes:write"));
        assert!(access.require(&["pipes:write"], &[]).is_err());
        let private = Value::from_json_str(
            r#"{"access": {"owner": "x", "team": "t", "visibility": "private"}}"#,
        )
        .unwrap()
        .to_map();
        assert!(access.can_see(&private));
        assert!(Access::everything().has("my_plugin:custom"));
    }
}
