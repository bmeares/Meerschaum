//! Permissions: the token scopes, their descriptions, and the expansion of scope lists written
//! before the newer permissions existed.

use super::*;

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
