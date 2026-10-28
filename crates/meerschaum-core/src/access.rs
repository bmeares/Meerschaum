//! The access model: permissions, roles, teams, pipe access and locks.
//!
//! Pure data and rules, no I/O. Two kinds of rules live here:
//!
//! - **Who may do what** (permissions, roles, teams, a pipe's owner, team and visibility). The
//!   API server, the MCP server and the web console apply these to the users they serve. Local
//!   command-line and Python access with the instance's own credentials is trusted and sees
//!   everything: the database credentials are the trust boundary.
//! - **Locks** (`parameters['locks']`). Every write path checks them, local ones included
//!   (`meerschaum_connector::locks`); `-y`, `--force` and `--allow-protected` never bypass them.
//!
//! The model is open by default: a pipe without an `access` key belongs to the team Everyone,
//! which every user is a member of, and anyone who can see a pipe may change it within their
//! permissions. On an instance where nobody sets `access` or `locks`, everything behaves as before.
//!
//! # Permissions and roles
//!
//! Permissions are the token scopes ([`ALL_PERMISSIONS`], `static.toml`'s `tokens.scopes`).
//! A user's effective permissions ([`user_permissions`], [`effective_permissions`]):
//!
//! 1. an administrator (user type `admin`) holds every permission;
//! 2. explicit permissions (`attributes['scopes']`, v4's key) are used as listed (`*` = all);
//! 3. otherwise the role stored in `attributes['role']` ([`Role`]), else the instance's default
//!    role (`api:permissions:default_role`, [`resolve_default_role`]; `editor` by default).
//!
//! Lists written before these permissions existed ([`expand_legacy_scopes`]) keep what they
//! allowed: `pipes:write` there also grants `pipes:register` and `pipes:define_sql`, and
//! `actions:execute` grants `webterm:use`. A token's scopes are intersected with its user's
//! current effective permissions when it is used ([`token_permissions`]).
//!
//! # Teams
//!
//! [`Team`]s have ids and unique names (the username rules, [`valid_team_name`]; names compare
//! without case). The built-in team [`EVERYONE`] has every user as a member; it cannot be
//! deleted or left, and its record may name managers. A user's teams carry a [`TeamRole`]:
//! managers add and remove members of their team and manage the access of its pipes. A user's
//! primary team ([`primary_team`]) is the default team of the pipes they create.
//!
//! # Pipe access
//!
//! `parameters['access']` ([`PipeAccess`]): `owner` (a username or null), `team` (a team name;
//! null is Everyone), `visibility` ([`Visibility`]: `team` (default), `private` (the owner, the
//! team's managers and administrators) or `everyone`) and `edit` ([`EditMode`]: `team` (default,
//! anyone who can see the pipe may change it within their permissions) or `owner` (the owner,
//! the team's managers and administrators)). The rules: [`can_see`], [`can_edit`],
//! [`can_manage_access`], [`can_lock`].
//!
//! # Locks
//!
//! `parameters['locks']` ([`Locks`]): a list of [`Lock`]s. `parameters` refuses parameter edits
//! (changing `locks` itself stays allowed), `sync` refuses writes (syncs, replace windows,
//! deduplication, verification syncs), `clear` refuses row deletes (clears, replace windows,
//! rebuilds), `drop` refuses drops and rebuilds (a rebuild is a drop) and table schema changes
//! (`verify schema`, repartitioning, dropping indices), `delete` refuses unregistering, and `all`
//! is all of them. [`check_lock`] says whether a [`LockOp`] is refused; [`lock_refusal`] builds
//! the message, which names the pipe, the lock and how to unlock it.

use std::collections::BTreeSet;
use std::fmt;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::pipe::Pipe;
use crate::static_config::static_config;
use crate::success::SuccessTuple;
use crate::value::{Map, Value};

// ---------------------------------------------------------------------------------------------
// Permissions
// ---------------------------------------------------------------------------------------------

/// The permission names (the token scopes), as associated constants:
/// `Permission::PIPES_READ`, `Permission::WEBTERM_USE`, ...
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Permission;

impl Permission {
    /// Read pipes' parameters and the contents of their tables.
    pub const PIPES_READ: &'static str = "pipes:read";
    /// Edit pipes' parameters and sync (write) their tables.
    pub const PIPES_WRITE: &'static str = "pipes:write";
    /// Drop pipes' tables.
    pub const PIPES_DROP: &'static str = "pipes:drop";
    /// Delete (unregister) pipes.
    pub const PIPES_DELETE: &'static str = "pipes:delete";
    /// Execute actions (`/execute`, the actions routes, the MCP `execute_action` tool).
    pub const ACTIONS_EXECUTE: &'static str = "actions:execute";
    /// List the connectors (secrets masked).
    pub const CONNECTORS_READ: &'static str = "connectors:read";
    /// Run read-only SQL queries.
    pub const SQL_READ: &'static str = "sql:read";
    /// Read jobs.
    pub const JOBS_READ: &'static str = "jobs:read";
    /// Create and edit jobs.
    pub const JOBS_WRITE: &'static str = "jobs:write";
    /// Start jobs.
    pub const JOBS_EXECUTE: &'static str = "jobs:execute";
    /// Delete jobs.
    pub const JOBS_DELETE: &'static str = "jobs:delete";
    /// Read jobs' logs.
    pub const LOGS_READ: &'static str = "logs:read";
    /// Stop jobs.
    pub const JOBS_STOP: &'static str = "jobs:stop";
    /// Pause jobs.
    pub const JOBS_PAUSE: &'static str = "jobs:pause";
    /// Read the instance's system-level metadata.
    pub const INSTANCE_READ: &'static str = "instance:read";
    /// Chain API instances with the associated credentials.
    pub const INSTANCE_CHAIN: &'static str = "instance:chain";
    /// Register and update plugins.
    pub const PLUGINS_WRITE: &'static str = "plugins:write";
    /// Read registered plugins.
    pub const PLUGINS_READ: &'static str = "plugins:read";
    /// Delete plugins.
    pub const PLUGINS_DELETE: &'static str = "plugins:delete";
    /// Read one's own account (administrators: every account).
    pub const USERS_READ: &'static str = "users:read";
    /// Edit one's own account (administrators: every account).
    pub const USERS_WRITE: &'static str = "users:write";
    /// Register user accounts.
    pub const USERS_REGISTER: &'static str = "users:register";
    /// Delete one's own account (administrators: every account).
    pub const USERS_DELETE: &'static str = "users:delete";
    /// Register new pipes (before rc15 a part of `pipes:write`, which still implies it in lists
    /// written before rc15: [`expand_legacy_scopes`]).
    pub const PIPES_REGISTER: &'static str = "pipes:register";
    /// Set or change pipes' SQL definitions (`fetch:definition`, `sql`, `query`) and the fetch
    /// parameters of `sql:` connectors through the API.
    pub const PIPES_DEFINE_SQL: &'static str = "pipes:define_sql";
    /// Use the web terminal.
    pub const WEBTERM_USE: &'static str = "webterm:use";
}

/// Every permission, in the order of `static.toml`'s `tokens.scopes` (v4's scopes, then the
/// permissions added in 5.0).
pub const ALL_PERMISSIONS: [&str; 26] = [
    Permission::PIPES_READ,
    Permission::PIPES_WRITE,
    Permission::PIPES_DROP,
    Permission::PIPES_DELETE,
    Permission::ACTIONS_EXECUTE,
    Permission::CONNECTORS_READ,
    Permission::SQL_READ,
    Permission::JOBS_READ,
    Permission::JOBS_WRITE,
    Permission::JOBS_EXECUTE,
    Permission::JOBS_DELETE,
    Permission::LOGS_READ,
    Permission::JOBS_STOP,
    Permission::JOBS_PAUSE,
    Permission::INSTANCE_READ,
    Permission::INSTANCE_CHAIN,
    Permission::PLUGINS_WRITE,
    Permission::PLUGINS_READ,
    Permission::PLUGINS_DELETE,
    Permission::USERS_READ,
    Permission::USERS_WRITE,
    Permission::USERS_REGISTER,
    Permission::USERS_DELETE,
    Permission::PIPES_REGISTER,
    Permission::PIPES_DEFINE_SQL,
    Permission::WEBTERM_USE,
];

/// The permissions added in 5.0: a scope list naming none of them was written before they
/// existed ([`expand_legacy_scopes`]).
pub const NEW_PERMISSIONS: [&str; 3] = [
    Permission::PIPES_REGISTER,
    Permission::PIPES_DEFINE_SQL,
    Permission::WEBTERM_USE,
];

/// The wildcard scope: every permission.
pub const ALL_SCOPE: &str = "*";

/// A set of permission names.
pub type Permissions = BTreeSet<String>;

/// Every permission as a set.
pub fn all_permissions() -> Permissions {
    ALL_PERMISSIONS.iter().map(|p| p.to_string()).collect()
}

/// The text `static.toml` gives a permission (`None` for an unknown name).
pub fn permission_description(permission: &str) -> Option<String> {
    static_config().tokens.scopes.get(permission).cloned()
}

/// A scope list as permissions: `*` expands to every permission, and a list written before the
/// 5.0 permissions existed keeps what it allowed ([`expand_legacy_scopes`]). Unknown names are
/// kept (a plugin may check its own).
pub fn scopes_to_permissions<S: AsRef<str>>(scopes: &[S]) -> Permissions {
    if scopes.iter().any(|s| s.as_ref().trim() == ALL_SCOPE) {
        return all_permissions();
    }
    expand_legacy_scopes(scopes)
}

/// A scope list written before the 5.0 permissions existed (it names none of
/// [`NEW_PERMISSIONS`]) keeps what it allowed: `pipes:write` also grants `pipes:register` and
/// `pipes:define_sql`, and `actions:execute` grants `webterm:use` (the web terminal executes
/// actions). A list that names any 5.0 permission is taken as written.
pub fn expand_legacy_scopes<S: AsRef<str>>(scopes: &[S]) -> Permissions {
    let mut out: Permissions = scopes
        .iter()
        .map(|s| s.as_ref().trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let legacy = !NEW_PERMISSIONS.iter().any(|p| out.contains(*p));
    if legacy {
        if out.contains(Permission::PIPES_WRITE) {
            out.insert(Permission::PIPES_REGISTER.to_string());
            out.insert(Permission::PIPES_DEFINE_SQL.to_string());
        }
        if out.contains(Permission::ACTIONS_EXECUTE) {
            out.insert(Permission::WEBTERM_USE.to_string());
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------------------------

/// The user attribute that stores a user's role preset (`attributes['role']`).
pub const ROLE_ATTRIBUTE: &str = "role";
/// The user attribute that stores a user's explicit permissions (`attributes['scopes']`, v4's
/// key).
pub const SCOPES_ATTRIBUTE: &str = "scopes";
/// The user attribute that stores a user's primary team (`attributes['primary_team']`).
pub const PRIMARY_TEAM_ATTRIBUTE: &str = "primary_team";
/// The user type of administrators.
pub const ADMIN_USER_TYPE: &str = "admin";

/// A role: a preset of permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Reads pipes, jobs, logs, plugins and the connectors list (secrets masked); reads and
    /// edits their own account.
    Viewer,
    /// Viewer, plus registering, editing and syncing pipes (SQL definitions included), starting
    /// existing jobs and running read-only SQL.
    Editor,
    /// Editor, plus executing actions, the web terminal, and creating, stopping, pausing and
    /// deleting jobs.
    Operator,
    /// An administrator (user type `admin`): every permission, users, teams and connector
    /// details included.
    Admin,
    /// Explicit permissions that are not one of the presets.
    Custom,
}

impl Role {
    /// The presets a user can be given, in order of power.
    pub const PRESETS: [Role; 4] = [Role::Viewer, Role::Editor, Role::Operator, Role::Admin];

    /// The roles that may be an instance's default role.
    pub const DEFAULTABLE: [Role; 3] = [Role::Viewer, Role::Editor, Role::Operator];

    /// The stored name (`viewer`, `editor`, `operator`, `admin`, `custom`).
    pub fn name(&self) -> &'static str {
        match self {
            Role::Viewer => "viewer",
            Role::Editor => "editor",
            Role::Operator => "operator",
            Role::Admin => "admin",
            Role::Custom => "custom",
        }
    }

    /// The name people read (`Viewer`, ...).
    pub fn label(&self) -> &'static str {
        match self {
            Role::Viewer => "Viewer",
            Role::Editor => "Editor",
            Role::Operator => "Operator",
            Role::Admin => "Admin",
            Role::Custom => "Custom",
        }
    }

    /// Parse a role name (any case, surrounding whitespace ignored).
    pub fn parse(text: &str) -> Option<Role> {
        match text.trim().to_lowercase().as_str() {
            "viewer" => Some(Role::Viewer),
            "editor" => Some(Role::Editor),
            "operator" => Some(Role::Operator),
            "admin" | "administrator" => Some(Role::Admin),
            "custom" => Some(Role::Custom),
            _ => None,
        }
    }

    /// The permissions of the preset (`Custom` has none of its own).
    pub fn permissions(&self) -> Permissions {
        let names: &[&str] = match self {
            Role::Viewer => &VIEWER,
            Role::Editor => &EDITOR,
            Role::Operator => &OPERATOR,
            Role::Admin => &ALL_PERMISSIONS,
            Role::Custom => &[],
        };
        names.iter().map(|p| p.to_string()).collect()
    }

    /// The preset whose permissions are exactly `permissions` (`Admin` never: it is a user
    /// type, not a list).
    pub fn matching(permissions: &Permissions) -> Option<Role> {
        Role::DEFAULTABLE
            .into_iter()
            .find(|role| role.permissions() == *permissions)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The default role when the configuration names none.
pub const DEFAULT_ROLE: Role = Role::Editor;

const VIEWER: [&str; 7] = [
    Permission::PIPES_READ,
    Permission::CONNECTORS_READ,
    Permission::JOBS_READ,
    Permission::LOGS_READ,
    Permission::PLUGINS_READ,
    Permission::USERS_READ,
    Permission::USERS_WRITE,
];

const EDITOR: [&str; 12] = [
    Permission::PIPES_READ,
    Permission::PIPES_WRITE,
    Permission::PIPES_REGISTER,
    Permission::PIPES_DEFINE_SQL,
    Permission::CONNECTORS_READ,
    Permission::SQL_READ,
    Permission::JOBS_READ,
    Permission::JOBS_EXECUTE,
    Permission::LOGS_READ,
    Permission::PLUGINS_READ,
    Permission::USERS_READ,
    Permission::USERS_WRITE,
];

const OPERATOR: [&str; 18] = [
    Permission::PIPES_READ,
    Permission::PIPES_WRITE,
    Permission::PIPES_REGISTER,
    Permission::PIPES_DEFINE_SQL,
    Permission::ACTIONS_EXECUTE,
    Permission::WEBTERM_USE,
    Permission::CONNECTORS_READ,
    Permission::SQL_READ,
    Permission::JOBS_READ,
    Permission::JOBS_WRITE,
    Permission::JOBS_EXECUTE,
    Permission::JOBS_STOP,
    Permission::JOBS_PAUSE,
    Permission::JOBS_DELETE,
    Permission::LOGS_READ,
    Permission::PLUGINS_READ,
    Permission::USERS_READ,
    Permission::USERS_WRITE,
];

/// Whether a user type is the administrator type.
pub fn is_admin_type(user_type: Option<&str>) -> bool {
    user_type.is_some_and(|t| t.trim().eq_ignore_ascii_case(ADMIN_USER_TYPE))
}

/// The instance's default role: `configured` (`api:permissions:default_role`) when it names
/// `viewer`, `editor` or `operator`; when it is not set, v4's
/// `api:permissions:actions:non_admin: true` (when the configuration sets it) maps to
/// `operator`, as it let every user execute actions; else `editor`. A value that names no
/// default role gives `viewer` (the least permissions) rather than more than intended.
pub fn resolve_default_role(
    configured: Option<&str>,
    legacy_actions_non_admin: Option<bool>,
) -> Role {
    match configured.map(str::trim).filter(|c| !c.is_empty()) {
        Some(text) => match Role::parse(text) {
            Some(role) if Role::DEFAULTABLE.contains(&role) => role,
            _ => Role::Viewer,
        },
        None if legacy_actions_non_admin == Some(true) => Role::Operator,
        None => DEFAULT_ROLE,
    }
}

/// [`resolve_default_role`] from a configuration: `api:permissions:default_role` and
/// `api:permissions:actions:non_admin`.
pub fn default_role_from_config(config: &Config) -> Role {
    let configured = config
        .get(&["api", "permissions", "default_role"])
        .filter(|v| !v.is_null())
        .map(|v| crate::pyfmt::py_str(&v));
    let legacy = config
        .get(&["api", "permissions", "actions", "non_admin"])
        .filter(|v| !v.is_null())
        .map(|v| v.truthy());
    resolve_default_role(configured.as_deref(), legacy)
}

/// `attributes['scopes']` as a list of names, when it is a list (a JSON text of a
/// list too, as some instances return the column as text).
pub fn explicit_scopes(attributes: Option<&Map>) -> Option<Vec<String>> {
    let value = attributes?.get(SCOPES_ATTRIBUTE)?;
    let items = match value {
        Value::List(items) => items.clone(),
        Value::Str(text) => match Value::from_json_str(text) {
            Ok(Value::List(items)) => items,
            _ => return None,
        },
        _ => return None,
    };
    let names: Vec<String> = items
        .iter()
        .map(crate::pyfmt::py_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    Some(names)
}

/// The role preset stored in `attributes['role']` (`None` when absent, unknown, `custom`, or
/// `admin`, which only the user type grants).
pub fn stored_role(attributes: Option<&Map>) -> Option<Role> {
    let text = attributes?.get(ROLE_ATTRIBUTE)?.as_str()?;
    Role::parse(text).filter(|r| Role::DEFAULTABLE.contains(r))
}

/// A user's effective permissions from their type and explicit scopes: an administrator holds
/// every permission; explicit scopes ([`scopes_to_permissions`]) are used as listed; otherwise
/// `default_role`'s (a user's stored role, else the instance default: see
/// [`user_permissions`]).
pub fn effective_permissions<S: AsRef<str>>(
    user_type: Option<&str>,
    explicit_scopes: Option<&[S]>,
    default_role: Role,
) -> Permissions {
    if is_admin_type(user_type) {
        return all_permissions();
    }
    if let Some(scopes) = explicit_scopes {
        return scopes_to_permissions(scopes);
    }
    match default_role {
        Role::Custom => DEFAULT_ROLE.permissions(),
        role => role.permissions(),
    }
}

/// A user's effective permissions from their stored record: [`effective_permissions`] with
/// `attributes['scopes']`, else the role in `attributes['role']`, else `default_role`.
pub fn user_permissions(
    user_type: Option<&str>,
    attributes: Option<&Map>,
    default_role: Role,
) -> Permissions {
    let scopes = explicit_scopes(attributes);
    // A role accompanies permission grids written by this access model. These lists are
    // exact: unchecked new permissions must not return through the legacy-scope aliases.
    if !is_admin_type(user_type)
        && attributes.is_some_and(|attrs| attrs.contains_key(ROLE_ATTRIBUTE))
        && let Some(scopes) = scopes.as_ref()
    {
        return if scopes.iter().any(|scope| scope == ALL_SCOPE) {
            all_permissions()
        } else {
            scopes.iter().cloned().collect()
        };
    }
    let role = stored_role(attributes).unwrap_or(default_role);
    effective_permissions(user_type, scopes.as_deref(), role)
}

/// A user's role (`user.role`): `Admin` for administrators; with explicit permissions, the
/// preset they equal (the stored role first), else `Custom`; else the stored role, else
/// `default_role`.
pub fn user_role(user_type: Option<&str>, attributes: Option<&Map>, default_role: Role) -> Role {
    if is_admin_type(user_type) {
        return Role::Admin;
    }
    let stored = stored_role(attributes);
    if explicit_scopes(attributes).is_some() {
        let permissions = user_permissions(user_type, attributes, default_role);
        if let Some(role) = stored.filter(|r| r.permissions() == permissions) {
            return role;
        }
        return Role::matching(&permissions).unwrap_or(Role::Custom);
    }
    stored.unwrap_or(match default_role {
        Role::Custom => DEFAULT_ROLE,
        role => role,
    })
}

/// The attributes to store for a user given a role preset (`Viewer`, `Editor`, `Operator`):
/// `role` set and explicit `scopes` removed, so the preset's permissions apply. `Admin` is the
/// user type, not an attribute: it leaves the attributes as they are. `Custom` keeps them.
pub fn attributes_with_role(attributes: &Map, role: Role) -> Map {
    let mut out = attributes.clone();
    if Role::DEFAULTABLE.contains(&role) {
        out.insert(ROLE_ATTRIBUTE.into(), Value::Str(role.name().into()));
        out.shift_remove(SCOPES_ATTRIBUTE);
    }
    out
}

/// The attributes to store for a user given explicit permissions: `scopes` set to them (in
/// [`ALL_PERMISSIONS`] order, unknown names after) and `role` set to the preset they equal,
/// else `custom`.
pub fn attributes_with_permissions<S: AsRef<str>>(attributes: &Map, permissions: &[S]) -> Map {
    let set: Permissions = permissions
        .iter()
        .map(|p| p.as_ref().trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();
    let mut out = attributes.clone();
    out.insert(
        SCOPES_ATTRIBUTE.into(),
        Value::List(
            ordered_permissions(&set)
                .into_iter()
                .map(Value::Str)
                .collect(),
        ),
    );
    let role = Role::matching(&set).unwrap_or(Role::Custom);
    out.insert(ROLE_ATTRIBUTE.into(), Value::Str(role.name().into()));
    out
}

/// Permissions in [`ALL_PERMISSIONS`] order, unknown names after (sorted).
pub fn ordered_permissions(permissions: &Permissions) -> Vec<String> {
    let mut out: Vec<String> = ALL_PERMISSIONS
        .iter()
        .filter(|p| permissions.contains(**p))
        .map(|p| p.to_string())
        .collect();
    out.extend(
        permissions
            .iter()
            .filter(|p| !ALL_PERMISSIONS.contains(&p.as_str()))
            .cloned(),
    );
    out
}

/// A token's permissions when it is used: its scopes ([`scopes_to_permissions`], so lists
/// written before 5.0 keep what they allowed) intersected with its user's current effective
/// permissions. A token never holds more than its user.
pub fn token_permissions<S: AsRef<str>>(token_scopes: &[S], user: &Permissions) -> Permissions {
    scopes_to_permissions(token_scopes)
        .intersection(user)
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Teams
// ---------------------------------------------------------------------------------------------

/// The name of the built-in team every user is a member of.
pub const EVERYONE: &str = "Everyone";

/// The id of the built-in team [`EVERYONE`] (the nil UUID).
pub const EVERYONE_TEAM_ID: &str = "00000000-0000-0000-0000-000000000000";

/// A member's role in a team.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamRole {
    /// A member.
    Member,
    /// A manager: adds and removes members and manages the access of the team's pipes.
    Manager,
}

impl TeamRole {
    /// The stored name (`member`, `manager`).
    pub fn name(&self) -> &'static str {
        match self {
            TeamRole::Member => "member",
            TeamRole::Manager => "manager",
        }
    }

    /// Parse a role name (any case).
    pub fn parse(text: &str) -> Option<TeamRole> {
        match text.trim().to_lowercase().as_str() {
            "member" | "members" => Some(TeamRole::Member),
            "manager" | "managers" => Some(TeamRole::Manager),
            _ => None,
        }
    }
}

impl fmt::Display for TeamRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Whether a team name means [`EVERYONE`] (`None`, empty, or `Everyone` in any case).
pub fn is_everyone(team: Option<&str>) -> bool {
    match team.map(str::trim) {
        None | Some("") => true,
        Some(name) => name.eq_ignore_ascii_case(EVERYONE),
    }
}

/// Whether two team names name the same team (names compare without case; `None` is Everyone).
pub fn same_team(a: Option<&str>, b: Option<&str>) -> bool {
    match (is_everyone(a), is_everyone(b)) {
        (true, true) => true,
        (false, false) => {
            a.unwrap_or("").trim().to_lowercase() == b.unwrap_or("").trim().to_lowercase()
        }
        _ => false,
    }
}

/// A team name as stored: `None` for Everyone, else the trimmed name.
pub fn normalize_team(team: Option<&str>) -> Option<String> {
    if is_everyone(team) {
        None
    } else {
        team.map(|t| t.trim().to_string())
    }
}

/// Whether a team name is valid: the username rules (length limits of `static.toml`'s
/// `users`, letters, digits and `_ - . @`), and not the built-in team's name.
pub fn valid_team_name(name: &str) -> SuccessTuple {
    let users = &static_config().users;
    let mut reasons = Vec::new();
    let len = name.chars().count() as i64;
    if len < users.min_username_length {
        reasons.push(format!(
            "Team names must have at least {} characters.",
            users.min_username_length
        ));
    }
    if len > users.max_username_length {
        reasons.push(format!(
            "Team names must contain {} or fewer characters.",
            users.max_username_length
        ));
    }
    let acceptable = ['_', '-', '.', '@'];
    if name
        .chars()
        .any(|c| !c.is_alphanumeric() && !acceptable.contains(&c))
    {
        reasons.push(
            "Team names may only contain letters, digits and the special characters ['_', '-', '.', '@']."
                .to_string(),
        );
    }
    if name.eq_ignore_ascii_case(EVERYONE) {
        reasons.push(format!("'{EVERYONE}' is the built-in team of every user."));
    }
    if reasons.is_empty() {
        return SuccessTuple::success();
    }
    let mut msg = format!("Team name '{name}' is invalid for the following reasons:\n");
    for r in reasons {
        msg.push_str(&format!(" - {r}\n"));
    }
    SuccessTuple::fail(msg)
}

/// A team: an id, a unique name, members with their roles and free-form attributes.
///
/// The built-in team [`EVERYONE`] ([`Team::everyone`]) has every user as a member; its
/// `members` list only the users with a role of their own there (its managers).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Team {
    /// The team id (a UUID; `None` before the team is registered).
    #[serde(default)]
    pub id: Option<String>,
    /// The name (unique without case).
    pub name: String,
    /// Members by username, with their roles, in the order they were added.
    #[serde(default)]
    pub members: IndexMap<String, TeamRole>,
    /// Free-form attributes.
    #[serde(default)]
    pub attributes: Map,
}

impl Team {
    /// A new team (not registered).
    pub fn new(name: &str) -> Team {
        Team {
            id: None,
            name: name.trim().to_string(),
            members: IndexMap::new(),
            attributes: Map::new(),
        }
    }

    /// The built-in team [`EVERYONE`] without a stored record.
    pub fn everyone() -> Team {
        Team {
            id: Some(EVERYONE_TEAM_ID.to_string()),
            name: EVERYONE.to_string(),
            members: IndexMap::new(),
            attributes: Map::new(),
        }
    }

    /// Whether this is the built-in team [`EVERYONE`].
    pub fn is_everyone(&self) -> bool {
        is_everyone(Some(&self.name))
    }

    /// A user's role in this team: for [`EVERYONE`] every user is at least a member.
    pub fn role_of(&self, username: &str) -> Option<TeamRole> {
        match self.members.get(username) {
            Some(role) => Some(*role),
            None if self.is_everyone() => Some(TeamRole::Member),
            None => None,
        }
    }

    /// The usernames of the managers.
    pub fn managers(&self) -> Vec<String> {
        self.members
            .iter()
            .filter(|(_, r)| **r == TeamRole::Manager)
            .map(|(u, _)| u.clone())
            .collect()
    }

    /// The usernames listed in `members` (managers included; for [`EVERYONE`] only those with a
    /// role of their own).
    pub fn member_names(&self) -> Vec<String> {
        self.members.keys().cloned().collect()
    }

    /// Add `username` with `role` (an existing member takes the new role).
    pub fn add_member(&mut self, username: &str, role: TeamRole) {
        self.members.insert(username.trim().to_string(), role);
    }

    /// Remove `username`; `true` when they were listed.
    pub fn remove_member(&mut self, username: &str) -> bool {
        self.members.shift_remove(username.trim()).is_some()
    }

    /// The record as a JSON-like map: `{"id", "name", "members": {username: role},
    /// "attributes"}`.
    pub fn to_value(&self) -> Value {
        let mut out = Map::new();
        out.insert("id".into(), self.id.clone().into());
        out.insert("name".into(), Value::Str(self.name.clone()));
        out.insert("members".into(), members_value(&self.members));
        out.insert("attributes".into(), Value::Map(self.attributes.clone()));
        Value::Map(out)
    }

    /// A team from [`Team::to_value`]'s shape (also `team_id` for `id`, and `members` as a list
    /// of names, of `{"username", "role"}` maps, or as JSON text). `None` without a name.
    pub fn from_value(value: &Value) -> Option<Team> {
        let map = value.as_map()?;
        let name = map
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|n| !n.is_empty())?
            .to_string();
        let id = map
            .get("id")
            .or_else(|| map.get("team_id"))
            .filter(|v| !v.is_null())
            .map(crate::pyfmt::py_str);
        let attributes = match map.get("attributes") {
            Some(Value::Map(m)) => m.clone(),
            Some(Value::Str(text)) => Value::from_json_str(text)
                .map(|v| v.to_map())
                .unwrap_or_default(),
            _ => Map::new(),
        };
        Some(Team {
            id,
            name,
            members: parse_members(map.get("members")),
            attributes,
        })
    }
}

/// Team members as a JSON-like map `{username: role}`.
pub fn members_value(members: &IndexMap<String, TeamRole>) -> Value {
    Value::Map(
        members
            .iter()
            .map(|(u, r)| (u.clone(), Value::Str(r.name().into())))
            .collect(),
    )
}

/// Parse stored members: a map `{username: role}`, a list of usernames (members) or of
/// `{"username", "role"}` maps, or JSON text of either. Unknown roles read as `member`.
pub fn parse_members(value: Option<&Value>) -> IndexMap<String, TeamRole> {
    let mut out = IndexMap::new();
    let parsed;
    let value = match value {
        Some(Value::Str(text)) => {
            parsed = Value::from_json_str(text).unwrap_or(Value::Null);
            &parsed
        }
        Some(v) => v,
        None => return out,
    };
    let role_of = |v: Option<&Value>| {
        v.and_then(Value::as_str)
            .and_then(TeamRole::parse)
            .unwrap_or(TeamRole::Member)
    };
    match value {
        Value::Map(m) => {
            for (u, r) in m {
                if !u.trim().is_empty() {
                    out.insert(u.trim().to_string(), role_of(Some(r)));
                }
            }
        }
        Value::List(items) => {
            for item in items {
                match item {
                    Value::Str(u) if !u.trim().is_empty() => {
                        out.insert(u.trim().to_string(), TeamRole::Member);
                    }
                    Value::Map(m) => {
                        if let Some(u) = m.get("username").and_then(Value::as_str) {
                            out.insert(u.trim().to_string(), role_of(m.get("role")));
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    out
}

/// A user's primary team (the default team of the pipes they create): the team named by
/// `attributes['primary_team']` when the user is in it, else the first of their teams other
/// than Everyone, else Everyone. `user_teams` is the user's teams ([`Principal::teams`]).
pub fn primary_team(attributes: Option<&Map>, user_teams: &[(String, TeamRole)]) -> String {
    let stored = attributes
        .and_then(|a| a.get(PRIMARY_TEAM_ATTRIBUTE))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|t| !t.is_empty());
    if let Some(stored) = stored {
        if is_everyone(Some(stored)) {
            return EVERYONE.to_string();
        }
        if let Some((name, _)) = user_teams
            .iter()
            .find(|(name, _)| same_team(Some(name), Some(stored)))
        {
            return name.clone();
        }
    }
    user_teams
        .iter()
        .find(|(name, _)| !is_everyone(Some(name)))
        .map(|(name, _)| name.clone())
        .unwrap_or_else(|| EVERYONE.to_string())
}

// ---------------------------------------------------------------------------------------------
// Pipe access
// ---------------------------------------------------------------------------------------------

/// The reserved parameter key of a pipe's access.
pub const ACCESS_KEY: &str = "access";
/// The reserved parameter key of a pipe's locks.
pub const LOCKS_KEY: &str = "locks";

/// Who can see a pipe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    /// The members of the pipe's team (and its owner).
    #[default]
    Team,
    /// The owner, the team's managers and administrators.
    Private,
    /// Every user.
    Everyone,
}

impl Visibility {
    /// Every visibility.
    pub const ALL: [Visibility; 3] = [Visibility::Team, Visibility::Private, Visibility::Everyone];

    /// The stored name (`team`, `private`, `everyone`).
    pub fn name(&self) -> &'static str {
        match self {
            Visibility::Team => "team",
            Visibility::Private => "private",
            Visibility::Everyone => "everyone",
        }
    }

    /// Parse a visibility (any case).
    pub fn parse(text: &str) -> Option<Visibility> {
        match text.trim().to_lowercase().as_str() {
            "team" => Some(Visibility::Team),
            "private" => Some(Visibility::Private),
            "everyone" | "all" | "public" => Some(Visibility::Everyone),
            _ => None,
        }
    }
}

impl fmt::Display for Visibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Who can change a pipe (within their permissions).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditMode {
    /// Anyone who can see the pipe.
    #[default]
    Team,
    /// The owner, the team's managers and administrators.
    Owner,
}

impl EditMode {
    /// Every edit mode.
    pub const ALL: [EditMode; 2] = [EditMode::Team, EditMode::Owner];

    /// The stored name (`team`, `owner`).
    pub fn name(&self) -> &'static str {
        match self {
            EditMode::Team => "team",
            EditMode::Owner => "owner",
        }
    }

    /// Parse an edit mode (any case).
    pub fn parse(text: &str) -> Option<EditMode> {
        match text.trim().to_lowercase().as_str() {
            "team" => Some(EditMode::Team),
            "owner" => Some(EditMode::Owner),
            _ => None,
        }
    }
}

impl fmt::Display for EditMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A pipe's access (`parameters['access']`). No `access` key is the default: no owner, the team
/// Everyone, visibility `team`, edit `team` (anyone may see and change the pipe).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PipeAccess {
    /// The owner's username.
    #[serde(default)]
    pub owner: Option<String>,
    /// The team's name (`None` is Everyone).
    #[serde(default)]
    pub team: Option<String>,
    /// Who can see the pipe.
    #[serde(default)]
    pub visibility: Visibility,
    /// Who can change it.
    #[serde(default)]
    pub edit: EditMode,
}

impl PipeAccess {
    /// The access in a pipe's parameters (the default without an `access` key).
    pub fn from_parameters(parameters: &Map) -> PipeAccess {
        PipeAccess::from_value(parameters.get(ACCESS_KEY))
    }

    /// The access of a pipe's parameters (`pipe.parameters` resolved).
    pub fn of_pipe(pipe: &Pipe) -> PipeAccess {
        PipeAccess::from_parameters(pipe.raw_parameters())
    }

    /// Read a stored `access` value leniently: a value that is not a map is the default; an
    /// unknown visibility reads as `private` and an unknown edit mode as `owner` (a typo never
    /// opens a pipe wider than intended). Writers check with [`PipeAccess::parse`].
    pub fn from_value(value: Option<&Value>) -> PipeAccess {
        let parsed;
        let map = match value {
            Some(Value::Map(m)) => m,
            Some(Value::Str(text)) => match Value::from_json_str(text) {
                Ok(Value::Map(m)) => {
                    parsed = m;
                    &parsed
                }
                _ => return PipeAccess::default(),
            },
            _ => return PipeAccess::default(),
        };
        let text = |key: &str| {
            map.get(key)
                .filter(|v| !v.is_null())
                .map(crate::pyfmt::py_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        };
        PipeAccess {
            owner: text("owner"),
            team: normalize_team(text("team").as_deref()),
            visibility: match text("visibility") {
                None => Visibility::Team,
                Some(v) => Visibility::parse(&v).unwrap_or(Visibility::Private),
            },
            edit: match text("edit").or_else(|| text("edit_mode")) {
                None => EditMode::Team,
                Some(v) => EditMode::parse(&v).unwrap_or(EditMode::Owner),
            },
        }
    }

    /// Parse an `access` value strictly (what a writer sets): a map whose keys are `owner`,
    /// `team`, `visibility` and `edit`, with known visibility and edit names. Missing keys take
    /// the defaults.
    pub fn parse(value: &Value) -> Result<PipeAccess, String> {
        let map = match value {
            Value::Null => return Ok(PipeAccess::default()),
            Value::Map(m) => m,
            other => {
                return Err(format!(
                    "`access` must be a dictionary with `owner`, `team`, `visibility` and `edit`, not {}.",
                    other.type_name()
                ));
            }
        };
        for key in map.keys() {
            if !["owner", "team", "visibility", "edit", "edit_mode"].contains(&key.as_str()) {
                return Err(format!(
                    "Unknown `access` key '{key}': use `owner`, `team`, `visibility` and `edit`."
                ));
            }
        }
        if let Some(v) = map.get("visibility").filter(|v| !v.is_null())
            && Visibility::parse(&crate::pyfmt::py_str(v)).is_none()
        {
            return Err(format!(
                "Invalid visibility '{}': use `team`, `private` or `everyone`.",
                crate::pyfmt::py_str(v)
            ));
        }
        if let Some(v) = map
            .get("edit")
            .or_else(|| map.get("edit_mode"))
            .filter(|v| !v.is_null())
            && EditMode::parse(&crate::pyfmt::py_str(v)).is_none()
        {
            return Err(format!(
                "Invalid edit mode '{}': use `team` or `owner`.",
                crate::pyfmt::py_str(v)
            ));
        }
        Ok(PipeAccess::from_value(Some(value)))
    }

    /// The stored form: `{"owner", "team", "visibility", "edit"}` (`team` null for Everyone).
    pub fn to_value(&self) -> Value {
        let mut out = Map::new();
        out.insert("owner".into(), self.owner.clone().into());
        out.insert("team".into(), normalize_team(self.team.as_deref()).into());
        out.insert(
            "visibility".into(),
            Value::Str(self.visibility.name().into()),
        );
        out.insert("edit".into(), Value::Str(self.edit.name().into()));
        Value::Map(out)
    }

    /// The team's name (`Everyone` for none).
    pub fn team_name(&self) -> String {
        normalize_team(self.team.as_deref()).unwrap_or_else(|| EVERYONE.to_string())
    }

    /// Whether this is the default access (no owner, Everyone, `team`, `team`).
    pub fn is_default(&self) -> bool {
        self.owner.is_none()
            && is_everyone(self.team.as_deref())
            && self.visibility == Visibility::Team
            && self.edit == EditMode::Team
    }

    /// Whether `username` owns the pipe.
    pub fn is_owner(&self, username: &str) -> bool {
        !username.is_empty() && self.owner.as_deref() == Some(username)
    }

    /// Whether the pipe belongs to `team` (names compare without case; `None` is Everyone).
    pub fn in_team(&self, team: Option<&str>) -> bool {
        same_team(self.team.as_deref(), team)
    }
}

/// `parameters` with `access` replaced (`None` removes the key).
pub fn parameters_with_access(parameters: &Map, access: Option<&PipeAccess>) -> Map {
    let mut out = parameters.clone();
    match access {
        Some(a) => {
            out.insert(ACCESS_KEY.into(), a.to_value());
        }
        None => {
            out.shift_remove(ACCESS_KEY);
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Locks
// ---------------------------------------------------------------------------------------------

/// A lock on a pipe (`parameters['locks']`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lock {
    /// No parameter edits (changing `locks` itself stays allowed).
    Parameters,
    /// No writes: syncs, replace windows, deduplication, verification syncs.
    Sync,
    /// No row deletes: clears, replace windows, rebuilds.
    Clear,
    /// No drops, rebuilds or table schema changes.
    Drop,
    /// No unregistering.
    Delete,
    /// All of the above.
    All,
}

impl Lock {
    /// The individual locks (everything but `All`), in their canonical order.
    pub const KINDS: [Lock; 5] = [
        Lock::Parameters,
        Lock::Sync,
        Lock::Clear,
        Lock::Drop,
        Lock::Delete,
    ];

    /// The stored name (`parameters`, `sync`, `clear`, `drop`, `delete`, `all`).
    pub fn name(&self) -> &'static str {
        match self {
            Lock::Parameters => "parameters",
            Lock::Sync => "sync",
            Lock::Clear => "clear",
            Lock::Drop => "drop",
            Lock::Delete => "delete",
            Lock::All => "all",
        }
    }

    /// Parse a lock name (any case; `params` and `parameter` for `parameters`, `*` for `all`).
    pub fn parse(text: &str) -> Option<Lock> {
        match text.trim().to_lowercase().as_str() {
            "parameters" | "parameter" | "params" => Some(Lock::Parameters),
            "sync" | "syncs" => Some(Lock::Sync),
            "clear" | "clears" => Some(Lock::Clear),
            "drop" | "drops" => Some(Lock::Drop),
            "delete" | "deletes" => Some(Lock::Delete),
            "all" | "*" => Some(Lock::All),
            _ => None,
        }
    }

    /// What the lock refuses, in words.
    pub fn describe(&self) -> &'static str {
        match self {
            Lock::Parameters => "parameter edits",
            Lock::Sync => "syncs (writes)",
            Lock::Clear => "row deletes",
            Lock::Drop => "drops, rebuilds and schema changes",
            Lock::Delete => "deletion",
            Lock::All => "all changes",
        }
    }
}

impl fmt::Display for Lock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A pipe's locks: a set of [`Lock`]s (`All` covers every kind).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Locks {
    set: BTreeSet<Lock>,
    unknown: Vec<String>,
}

impl Locks {
    /// No locks.
    pub fn new() -> Locks {
        Locks::default()
    }

    /// `all`.
    pub fn all() -> Locks {
        Locks::of(&[Lock::All])
    }

    /// The given locks.
    pub fn of(locks: &[Lock]) -> Locks {
        Locks {
            set: locks.iter().copied().collect(),
            unknown: Vec::new(),
        }
    }

    /// The locks in a pipe's parameters (none without a `locks` key).
    pub fn from_parameters(parameters: &Map) -> Locks {
        Locks::from_value(parameters.get(LOCKS_KEY))
    }

    /// The locks of a pipe's in-memory parameters.
    pub fn of_pipe(pipe: &Pipe) -> Locks {
        Locks::from_parameters(pipe.raw_parameters())
    }

    /// Read a stored `locks` value leniently: a list of names, one name or comma-separated
    /// names, `true` (all) or `false` / null (none). Unknown names are kept in
    /// [`Locks::unknown`] and lock nothing.
    pub fn from_value(value: Option<&Value>) -> Locks {
        let mut out = Locks::new();
        let mut add = |text: &str| {
            for part in text.split(',') {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                match Lock::parse(part) {
                    Some(lock) => {
                        out.set.insert(lock);
                    }
                    None => out.unknown.push(part.to_string()),
                }
            }
        };
        match value {
            None | Some(Value::Null) => {}
            Some(Value::Bool(true)) => add("all"),
            Some(Value::Bool(false)) => {}
            Some(Value::Str(text)) => match Value::from_json_str(text) {
                Ok(Value::List(items)) => {
                    for item in &items {
                        add(&crate::pyfmt::py_str(item));
                    }
                }
                _ => add(text),
            },
            Some(Value::List(items)) => {
                for item in items {
                    add(&crate::pyfmt::py_str(item));
                }
            }
            Some(other) => add(&crate::pyfmt::py_str(other)),
        }
        out
    }

    /// Parse locks a person typed (`drop,sync`, `all`): every name must be known. An empty text
    /// is `all` (the default of `lock pipes`).
    pub fn parse_list(text: &str) -> Result<Locks, String> {
        if text.trim().is_empty() {
            return Ok(Locks::all());
        }
        let locks = Locks::from_value(Some(&Value::Str(text.to_string())));
        if !locks.unknown.is_empty() {
            return Err(format!(
                "Unknown lock{} {}: use {}.",
                if locks.unknown.len() == 1 { "" } else { "s" },
                locks
                    .unknown
                    .iter()
                    .map(|u| format!("'{u}'"))
                    .collect::<Vec<_>>()
                    .join(", "),
                lock_names_hint()
            ));
        }
        Ok(locks)
    }

    /// Whether no lock is held.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Whether `all` is held.
    pub fn is_all(&self) -> bool {
        self.set.contains(&Lock::All)
    }

    /// Whether `lock` is held (directly or through `all`).
    pub fn contains(&self, lock: Lock) -> bool {
        self.is_all() || self.set.contains(&lock)
    }

    /// The held locks as written (`all` stays `all`).
    pub fn locks(&self) -> Vec<Lock> {
        self.set.iter().copied().collect()
    }

    /// The held kinds with `all` expanded.
    pub fn kinds(&self) -> Vec<Lock> {
        if self.is_all() {
            return Lock::KINDS.to_vec();
        }
        self.locks()
    }

    /// The names that were not locks (they lock nothing).
    pub fn unknown(&self) -> &[String] {
        &self.unknown
    }

    /// These locks and `other`'s.
    pub fn with(&self, other: &Locks) -> Locks {
        let mut set = self.set.clone();
        set.extend(other.set.iter().copied());
        if set.contains(&Lock::All) {
            set = [Lock::All].into_iter().collect();
        }
        Locks {
            set,
            unknown: Vec::new(),
        }
    }

    /// These locks without `other`'s: removing `all` removes every lock, and removing one kind
    /// from `all` keeps the other kinds.
    pub fn without(&self, other: &Locks) -> Locks {
        if other.is_all() {
            return Locks::new();
        }
        let set: BTreeSet<Lock> = self
            .kinds()
            .into_iter()
            .filter(|k| !other.set.contains(k))
            .collect();
        let set = if self.is_all() && set.len() == Lock::KINDS.len() {
            [Lock::All].into_iter().collect()
        } else {
            set
        };
        Locks {
            set,
            unknown: Vec::new(),
        }
    }

    /// The stored names (`["all"]`, `["sync", "drop"]`), in the canonical order.
    pub fn names(&self) -> Vec<String> {
        self.set.iter().map(|l| l.name().to_string()).collect()
    }

    /// The stored form: a list of names.
    pub fn to_value(&self) -> Value {
        Value::List(self.names().into_iter().map(Value::Str).collect())
    }

    /// The held locks that refuse `op` (`all` reported as `all`).
    pub fn blocking(&self, op: LockOp) -> Vec<Lock> {
        if self.is_all() {
            return vec![Lock::All];
        }
        op.locks()
            .iter()
            .copied()
            .filter(|l| self.set.contains(l))
            .collect()
    }
}

impl fmt::Display for Locks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.names().join(", "))
    }
}

/// `parameters` with `locks` replaced (no locks removes the key).
pub fn parameters_with_locks(parameters: &Map, locks: &Locks) -> Map {
    let mut out = parameters.clone();
    if locks.is_empty() {
        out.shift_remove(LOCKS_KEY);
    } else {
        out.insert(LOCKS_KEY.into(), locks.to_value());
    }
    out
}

/// `parameters`, `sync`, `clear`, `drop`, `delete` or `all`.
pub fn lock_names_hint() -> String {
    let mut names: Vec<String> = Lock::KINDS
        .iter()
        .map(|l| format!("`{}`", l.name()))
        .collect();
    names.push("`all`".into());
    let last = names.pop().unwrap_or_default();
    format!("{} or {last}", names.join(", "))
}

/// A write that locks may refuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockOp {
    /// Edit the parameters (refused by `parameters`).
    EditParameters,
    /// Sync (write) rows (refused by `sync`).
    Sync,
    /// Replace the rows of a window (`sync --replace`; refused by `sync` and `clear`).
    Replace,
    /// Delete duplicate rows (refused by `sync`).
    Deduplicate,
    /// Delete rows (refused by `clear`).
    Clear,
    /// Drop the table (refused by `drop`).
    Drop,
    /// Clear and fully re-sync after a definition change (refused by `drop`, `clear` and
    /// `sync`).
    Rebuild,
    /// Change the table's schema: `verify schema`, repartitioning, dropping indices (refused by
    /// `drop`).
    Schema,
    /// Unregister the pipe (refused by `delete`).
    Delete,
    /// Drop the table and unregister the pipe (`pipe.delete()`, `delete pipes`; refused by
    /// `delete` and `drop`).
    DeleteAndDrop,
}

impl LockOp {
    /// The locks that refuse this write.
    pub fn locks(&self) -> &'static [Lock] {
        match self {
            LockOp::EditParameters => &[Lock::Parameters],
            LockOp::Sync | LockOp::Deduplicate => &[Lock::Sync],
            LockOp::Replace => &[Lock::Sync, Lock::Clear],
            LockOp::Clear => &[Lock::Clear],
            LockOp::Drop | LockOp::Schema => &[Lock::Drop],
            LockOp::Rebuild => &[Lock::Sync, Lock::Clear, Lock::Drop],
            LockOp::Delete => &[Lock::Delete],
            LockOp::DeleteAndDrop => &[Lock::Drop, Lock::Delete],
        }
    }

    /// The verb of the refusal (`Cannot {verb} Pipe(...)`).
    pub fn verb(&self) -> &'static str {
        match self {
            LockOp::EditParameters => "edit the parameters of",
            LockOp::Sync => "sync",
            LockOp::Replace => "replace rows of",
            LockOp::Deduplicate => "deduplicate",
            LockOp::Clear => "clear",
            LockOp::Drop => "drop",
            LockOp::Rebuild => "rebuild",
            LockOp::Schema => "change the table of",
            LockOp::Delete | LockOp::DeleteAndDrop => "delete",
        }
    }

    /// The name (`edit_parameters`, `sync`, ...).
    pub fn name(&self) -> &'static str {
        match self {
            LockOp::EditParameters => "edit_parameters",
            LockOp::Sync => "sync",
            LockOp::Replace => "replace",
            LockOp::Deduplicate => "deduplicate",
            LockOp::Clear => "clear",
            LockOp::Drop => "drop",
            LockOp::Rebuild => "rebuild",
            LockOp::Schema => "schema",
            LockOp::Delete => "delete",
            LockOp::DeleteAndDrop => "delete_and_drop",
        }
    }

    /// Parse an operation name ([`LockOp::name`], also `edit` / `parameters` and `verify` for
    /// a verification sync).
    pub fn parse(text: &str) -> Option<LockOp> {
        match text.trim().to_lowercase().as_str() {
            "edit_parameters" | "edit" | "parameters" | "register" => Some(LockOp::EditParameters),
            "sync" | "verify" | "write" => Some(LockOp::Sync),
            "replace" => Some(LockOp::Replace),
            "deduplicate" | "dedup" => Some(LockOp::Deduplicate),
            "clear" => Some(LockOp::Clear),
            "drop" => Some(LockOp::Drop),
            "rebuild" => Some(LockOp::Rebuild),
            "schema" | "verify_schema" | "repartition" | "drop_indices" => Some(LockOp::Schema),
            "delete" => Some(LockOp::Delete),
            "delete_and_drop" => Some(LockOp::DeleteAndDrop),
            _ => None,
        }
    }
}

impl From<Lock> for LockOp {
    /// The write a lock kind refuses on its own (`all` maps to the widest, a rebuild).
    fn from(lock: Lock) -> Self {
        match lock {
            Lock::Parameters => LockOp::EditParameters,
            Lock::Sync => LockOp::Sync,
            Lock::Clear => LockOp::Clear,
            Lock::Drop => LockOp::Drop,
            Lock::Delete => LockOp::Delete,
            Lock::All => LockOp::Rebuild,
        }
    }
}

impl fmt::Display for LockOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The text every lock refusal carries (see [`is_lock_refusal`]).
pub const LOCK_REFUSAL_MARKER: &str = "it is locked (";

/// `Ok(())` when `locks` allow `op`; else `Err` naming the locks that refuse it
/// (`it is locked (sync)`). [`lock_refusal`] builds the full message for a pipe.
pub fn check_lock(locks: &Locks, op: impl Into<LockOp>) -> Result<(), String> {
    let held = locks.blocking(op.into());
    if held.is_empty() {
        return Ok(());
    }
    Err(format!("{LOCK_REFUSAL_MARKER}{})", names_of(&held)))
}

fn names_of(locks: &[Lock]) -> String {
    locks
        .iter()
        .map(|l| l.name())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The command that removes `locks` from `pipe` (`mrsm unlock pipes -c ... -m ... [-l ...]
/// [-i ...] --locks sync`).
pub fn unlock_command(pipe: &Pipe, locks: &[Lock]) -> String {
    let mut cmd = format!(
        "mrsm unlock pipes -c {} -m {}",
        pipe.connector_keys(),
        pipe.metric_key()
    );
    if let Some(location) = pipe.location_key() {
        cmd.push_str(&format!(" -l {location}"));
    }
    if pipe.instance_keys() != pipe.default_instance_keys() {
        cmd.push_str(&format!(" -i {}", pipe.instance_keys()));
    }
    if !locks.is_empty() {
        cmd.push_str(&format!(" --locks {}", names_of(locks).replace(", ", ",")));
    }
    cmd
}

/// The refusal of `op` on `pipe` under `locks` (`None` when allowed):
/// `Cannot sync Pipe('sql:main', 'weather'): it is locked (sync). Unlock it with `mrsm unlock
/// pipes -c sql:main -m weather --locks sync` or in the web console (Manage > Locks).`
pub fn lock_refusal(pipe: &Pipe, locks: &Locks, op: impl Into<LockOp>) -> Option<String> {
    let op = op.into();
    let held = locks.blocking(op);
    if held.is_empty() {
        return None;
    }
    // What to unlock for this write: the kinds it needs that are held (`all` held: all of them).
    let to_unlock: Vec<Lock> = op
        .locks()
        .iter()
        .copied()
        .filter(|l| locks.contains(*l))
        .collect();
    Some(format!(
        "Cannot {} {pipe}: {LOCK_REFUSAL_MARKER}{}). Unlock it with `{}` or in the web console (Manage > Locks).",
        op.verb(),
        names_of(&held),
        unlock_command(pipe, &to_unlock)
    ))
}

/// Whether a message is a lock refusal ([`lock_refusal`]).
pub fn is_lock_refusal(message: &str) -> bool {
    message.contains(LOCK_REFUSAL_MARKER) && message.contains("mrsm unlock pipes")
}

/// The top-level parameter keys whose values differ between `old` and `new` (in `new`'s
/// order, then the keys only `old` has), ignoring `ignored`.
pub fn changed_parameter_keys(old: &Map, new: &Map, ignored: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (key, value) in new {
        if ignored.contains(&key.as_str()) {
            continue;
        }
        if old.get(key) != Some(value) {
            out.push(key.clone());
        }
    }
    for key in old.keys() {
        if !ignored.contains(&key.as_str()) && !new.contains_key(key) && !out.contains(key) {
            out.push(key.clone());
        }
    }
    out
}

/// Whether an edit from `old` to `new` is allowed under `locks`: without a `parameters` lock
/// every edit is; with one, only changes to `locks` itself (and, for `access_maintenance`, the
/// `access` changes the team and user stores make when a team is renamed or deleted or a user
/// is deleted).
pub fn parameters_edit_allowed(
    old: &Map,
    new: &Map,
    locks: &Locks,
    access_maintenance: bool,
) -> bool {
    if !locks.contains(Lock::Parameters) {
        return true;
    }
    let ignored: &[&str] = if access_maintenance {
        &[LOCKS_KEY, ACCESS_KEY]
    } else {
        &[LOCKS_KEY]
    };
    changed_parameter_keys(old, new, ignored).is_empty()
}

// ---------------------------------------------------------------------------------------------
// Principals and rules
// ---------------------------------------------------------------------------------------------

/// Who is asking: a user (or a token's user) as the API server, MCP server or console sees them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    /// The username (empty for an anonymous caller).
    pub username: String,
    /// Whether the user is an administrator.
    pub is_admin: bool,
    /// The effective permissions (a token's already intersected with its user's).
    pub permissions: Permissions,
    /// The user's teams with their roles ([`EVERYONE`] included).
    pub teams: Vec<(String, TeamRole)>,
}

impl Principal {
    /// A principal.
    pub fn new(
        username: &str,
        is_admin: bool,
        permissions: Permissions,
        teams: Vec<(String, TeamRole)>,
    ) -> Principal {
        Principal {
            username: username.to_string(),
            is_admin,
            permissions: if is_admin {
                all_permissions()
            } else {
                permissions
            },
            teams,
        }
    }

    /// An administrator (every permission).
    pub fn admin(username: &str) -> Principal {
        Principal::new(
            username,
            true,
            all_permissions(),
            vec![(EVERYONE.to_string(), TeamRole::Member)],
        )
    }

    /// Whether the principal holds `permission` (administrators hold all; `*` is all).
    pub fn has(&self, permission: &str) -> bool {
        self.is_admin
            || self.permissions.contains(permission)
            || self.permissions.contains(ALL_SCOPE)
    }

    /// The principal's role in `team` (`None` is Everyone, where every user is a member).
    pub fn team_role(&self, team: Option<&str>) -> Option<TeamRole> {
        let listed = self
            .teams
            .iter()
            .filter(|(name, _)| same_team(Some(name), team))
            .map(|(_, role)| *role)
            .max();
        match listed {
            Some(role) => Some(role),
            None if is_everyone(team) => Some(TeamRole::Member),
            None => None,
        }
    }

    /// Whether the principal is in `team` (any role; every user is in Everyone).
    pub fn is_member(&self, team: Option<&str>) -> bool {
        self.team_role(team).is_some()
    }

    /// Whether the principal manages `team`.
    pub fn is_manager(&self, team: Option<&str>) -> bool {
        self.team_role(team) == Some(TeamRole::Manager)
    }

    /// The names of the teams the principal is in (Everyone first).
    pub fn team_names(&self) -> Vec<String> {
        let mut out = vec![EVERYONE.to_string()];
        for (name, _) in &self.teams {
            if !out.iter().any(|n| same_team(Some(n), Some(name))) {
                out.push(name.clone());
            }
        }
        out
    }
}

/// Whether `principal` can see a pipe with `access`: administrators and the owner always;
/// visibility `everyone`: every user; `team`: the team's members; `private`: the team's
/// managers.
pub fn can_see(principal: &Principal, access: &PipeAccess) -> bool {
    if principal.is_admin || access.is_owner(&principal.username) {
        return true;
    }
    match access.visibility {
        Visibility::Everyone => true,
        Visibility::Team => principal.is_member(access.team.as_deref()),
        Visibility::Private => principal.is_manager(access.team.as_deref()),
    }
}

/// Whether `principal` may change a pipe with `access` (within their permissions, which the
/// caller checks): edit `team`: anyone who can see it; edit `owner`: the owner, the team's
/// managers and administrators.
pub fn can_edit(principal: &Principal, access: &PipeAccess) -> bool {
    if !can_see(principal, access) {
        return false;
    }
    match access.edit {
        EditMode::Team => true,
        EditMode::Owner => can_manage_access(principal, access),
    }
}

/// Whether `principal` may change a pipe's `access`: its owner, its team's managers and
/// administrators.
pub fn can_manage_access(principal: &Principal, access: &PipeAccess) -> bool {
    principal.is_admin
        || access.is_owner(&principal.username)
        || principal.is_manager(access.team.as_deref())
}

/// Whether `principal` may lock or unlock a pipe through the API or the console: its owner,
/// its team's managers and administrators (locally anyone with access to the instance may).
pub fn can_lock(principal: &Principal, access: &PipeAccess) -> bool {
    can_manage_access(principal, access)
}

/// Whether `viewer` may see the email address of a user in `target_user_teams` (that user's
/// teams): administrators, and the managers of a team the user belongs to (every user belongs to
/// Everyone, so its managers see every address). A user always sees their own
/// ([`can_see_email_of`]).
pub fn can_see_email(viewer: &Principal, target_user_teams: &[(String, TeamRole)]) -> bool {
    viewer.is_admin
        || viewer.is_manager(Some(EVERYONE))
        || target_user_teams
            .iter()
            .any(|(team, _)| viewer.is_manager(Some(team)))
}

/// [`can_see_email`], or the address is the viewer's own.
pub fn can_see_email_of(
    viewer: &Principal,
    target_username: &str,
    target_user_teams: &[(String, TeamRole)],
) -> bool {
    (!viewer.username.is_empty() && viewer.username == target_username)
        || can_see_email(viewer, target_user_teams)
}

/// Whether `principal` may add and remove members of `team` (and change their roles): its
/// managers and administrators.
pub fn can_manage_team(principal: &Principal, team: &str) -> bool {
    principal.is_admin || principal.is_manager(Some(team))
}

/// Whether `principal` may create, rename and delete teams: administrators.
pub fn can_administer_teams(principal: &Principal) -> bool {
    principal.is_admin
}

/// The access of a pipe `principal` registers through the API or the console: `requested`
/// with the owner set to the caller (an administrator may name another owner) and the team
/// defaulting to `primary_team` (the caller's primary team).
pub fn access_for_new_pipe(
    principal: &Principal,
    requested: Option<&PipeAccess>,
    primary_team: &str,
) -> PipeAccess {
    let mut access = requested.cloned().unwrap_or_default();
    let explicit_team = requested
        .and_then(|r| r.team.as_deref())
        .is_some_and(|t| !t.trim().is_empty());
    if !principal.is_admin || requested.is_none_or(|r| r.owner.is_none()) {
        access.owner = (!principal.username.is_empty()).then(|| principal.username.clone());
    }
    if !explicit_team {
        access.team = normalize_team(Some(primary_team));
    }
    access
}

/// Check an access change by `principal` through the API or the console (`old` is the stored
/// access, `new` the requested one): they must manage the pipe's access ([`can_manage_access`]);
/// someone other than an administrator may only give the pipe to a team they are in, and only a
/// team manager (of the old team) or an administrator may give it to another owner (the owner
/// may give up ownership).
pub fn check_access_change(
    principal: &Principal,
    old: &PipeAccess,
    new: &PipeAccess,
) -> Result<(), String> {
    if !can_manage_access(principal, old) {
        return Err(
            "Only the pipe's owner, its team's managers and administrators may change its access."
                .to_string(),
        );
    }
    if principal.is_admin {
        return Ok(());
    }
    if !same_team(old.team.as_deref(), new.team.as_deref())
        && !principal.is_member(new.team.as_deref())
    {
        return Err(format!(
            "You are not a member of the team '{}'.",
            new.team_name()
        ));
    }
    let owner_changed = old.owner != new.owner;
    let gives_up = new.owner.is_none() && old.is_owner(&principal.username);
    if owner_changed && !gives_up && !principal.is_manager(old.team.as_deref()) {
        return Err(
            "Only the team's managers and administrators may give a pipe to another owner."
                .to_string(),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// SQL definitions (AC8)
// ---------------------------------------------------------------------------------------------

/// The fetch parameters of a `sql:` pipe that make up its SQL definition: `fetch` (every key:
/// `definition`, `backtrack_minutes`, ...), and the top-level `sql` and `query`.
pub const SQL_DEFINITION_KEYS: [&str; 3] = ["fetch", "sql", "query"];

/// Whether going from `old` to `new` parameters sets or changes a pipe's SQL definition: its
/// `fetch:definition`, `sql` or `query`, or, for a pipe of a `sql:` connector, any `fetch`
/// parameter. Through the API this needs `pipes:define_sql`.
pub fn sql_definition_changed(connector_keys: &str, old: &Map, new: &Map) -> bool {
    let sql_source = connector_keys.starts_with("sql:") || connector_keys == "sql";
    for key in ["sql", "query"] {
        if old.get(key) != new.get(key) {
            return true;
        }
    }
    let fetch = |m: &Map| m.get("fetch").cloned().unwrap_or(Value::Null);
    let (old_fetch, new_fetch) = (fetch(old), fetch(new));
    if sql_source {
        return old_fetch != new_fetch;
    }
    old_fetch.get("definition") != new_fetch.get("definition")
}

#[cfg(test)]
mod tests;
