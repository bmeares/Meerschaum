//! Roles and a user's effective permissions: administrators, explicit scopes, stored and default
//! roles, and what a credential's scopes leave of its owner's permissions.

use super::*;

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

/// Scopes whose old spelling a credential may still hold: `(legacy, current)`. v4's job route
/// asked for the misspelled `josb:stop`; a token that holds it holds `jobs:stop`.
pub const LEGACY_SPELLINGS: &[(&str, &str)] = &[("josb:stop", "jobs:stop")];

/// A credential's scope list with the current spelling added for each legacy one it holds
/// ([`LEGACY_SPELLINGS`]).
pub fn credential_scopes<S: AsRef<str>>(granted: &[S]) -> Vec<String> {
    let mut out: Vec<String> = granted.iter().map(|s| s.as_ref().to_string()).collect();
    for (legacy, current) in LEGACY_SPELLINGS {
        if out.iter().any(|s| s == legacy) && !out.iter().any(|s| s == current) {
            out.push(current.to_string());
        }
    }
    out
}

/// The permissions a user lends to their credentials: [`user_permissions`], with `*` added for
/// an administrator and when their own permissions list it, so they pass plugins' own scopes too
/// (as in v4) and their credentials keep every scope they list.
pub fn credential_owner_permissions(
    user_type: Option<&str>,
    attributes: Option<&Map>,
    default_role: Role,
) -> Permissions {
    let mut out = user_permissions(user_type, attributes, default_role);
    if is_admin_type(user_type)
        || explicit_scopes(attributes)
            .is_some_and(|scopes| scopes.iter().any(|s| s.trim() == ALL_SCOPE))
    {
        out.insert(ALL_SCOPE.to_string());
    }
    out
}

/// What a credential that grants `granted` may do for an owner holding `owner`
/// ([`credential_owner_permissions`]): their intersection ([`token_permissions`], so lists
/// written before 5.0 keep what they allowed). `*` on either side keeps the other side whole: a
/// `*` credential holds its owner's permissions (a plugin's own scopes included), and an owner
/// with `*` lends a credential every scope it lists. The one rule for the Rust server and the
/// Python API host.
pub fn credential_permissions<S: AsRef<str>>(owner: &Permissions, granted: &[S]) -> Permissions {
    let list = credential_scopes(granted);
    if list.iter().any(|s| s.trim() == ALL_SCOPE) {
        return owner.clone();
    }
    if owner.contains(ALL_SCOPE) {
        return scopes_to_permissions(&list);
    }
    token_permissions(&list, owner)
}

/// A credential's scopes narrowed to its backing token's current scopes (a JWT minted from an API
/// token keeps no more than the token holds now). `*` on either side keeps the other side.
pub fn narrow_to_token_scopes<S: AsRef<str>, T: AsRef<str>>(
    claimed: &[S],
    token: &[T],
) -> Vec<String> {
    let claimed = credential_scopes(claimed);
    let token = credential_scopes(token);
    if token.iter().any(|s| s.trim() == ALL_SCOPE) {
        return claimed;
    }
    if claimed.iter().any(|s| s.trim() == ALL_SCOPE) {
        return token;
    }
    let token = scopes_to_permissions(&token);
    scopes_to_permissions(&claimed)
        .into_iter()
        .filter(|s| token.contains(s))
        .collect()
}
