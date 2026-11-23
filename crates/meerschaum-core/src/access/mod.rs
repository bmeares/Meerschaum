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
//! whose members include the teams named in the instance connector's reservation. Anyone who can
//! see a pipe may change it within their permissions unless its edit mode or locks restrict them.
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

mod locks;
mod permissions;
mod pipe_access;
mod roles;
mod rules;
mod teams;

pub use locks::*;
pub use permissions::*;
pub use pipe_access::*;
pub use roles::*;
pub use rules::*;
pub use teams::*;

// ---------------------------------------------------------------------------------------------
// SQL definitions
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
