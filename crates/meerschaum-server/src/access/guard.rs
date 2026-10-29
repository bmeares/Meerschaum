//! What a caller may write into a pipe's parameters through the API (AC4, AC5, AC8): the
//! checks the pipe routes, the Pipelines routes and the MCP tools share.
//!
//! - **New pipes** ([`stamp_new_pipe`]): the owner is the caller (an administrator may name
//!   another), the team defaults to the caller's primary team, and someone other than an
//!   administrator may only name a team they are in.
//! - **Edits** ([`merge_edit`]): `access` and `locks` left out of a replacing edit keep their
//!   stored values; changing `access` needs [`rules::check_access_change`] (the owner, the
//!   team's managers, administrators) and changing `locks` [`rules::can_lock`].
//! - **SQL and targets** ([`check_parameters`]): `pipes:define_sql` to set a definition; no
//!   internal tables, no table of a pipe the caller cannot see (raw and resolved), no
//!   `{{ Pipe() }}` or `reference` to such a pipe, no `MRSM{}` symlinks
//!   ([`crate::sql_guard`]).
//! - **Connectors** ([`check_connector_teams`], [`check_pipe_connector`]): a connector with
//!   `teams: [...]` is for those teams' pipes only: listed to their members, and a pipe that
//!   reads from it is registered, defined or given to a team only by their members, and belongs
//!   to one of them.
//!
//! Administrators (and `--no-auth`) pass the object-ownership and definition-content checks.
//! Credential permissions and the API's allowed source instances still apply; locks are
//! enforced by the core for everyone. Generic API data access also refuses internal targets.

use std::sync::Arc;

use meerschaum_connector::InstanceConnector;
use meerschaum_core::template::referenced_pipes_in_parameters;
use meerschaum_core::{Map, Pipe, Value};

use super::{Access, PipeAccess, rules};
use crate::error::ApiError;
use crate::sql_guard;
use crate::state::AppState;

fn forbidden(message: impl Into<String>) -> ApiError {
    ApiError::new(403, message)
}

/// The parameters of a pipe `access` registers: `access` set by
/// [`rules::access_for_new_pipe`] (left out when it is the default, as without the access
/// model). An `access` the caller may not give is a `403`.
pub fn stamp_new_pipe(access: &Access, parameters: &Map) -> Result<Map, ApiError> {
    let requested = match parameters.get(rules::ACCESS_KEY) {
        None | Some(Value::Null) => None,
        Some(value) => Some(PipeAccess::parse(value).map_err(|e| ApiError::new(400, e))?),
    };
    if access.no_auth {
        return Ok(parameters.clone());
    }
    if let Some(requested) = &requested
        && !access.is_admin()
        && requested.team.is_some()
        && !access.in_team(requested.team.as_deref())
    {
        return Err(forbidden(format!(
            "You are not a member of the team '{}'.",
            requested.team_name()
        )));
    }
    let stamped =
        rules::access_for_new_pipe(&access.principal, requested.as_ref(), &access.primary_team);
    Ok(if stamped.is_default() {
        rules::parameters_with_access(parameters, None)
    } else {
        rules::parameters_with_access(parameters, Some(&stamped))
    })
}

/// The parameters of an edit of a stored pipe (`old`, its registered parameters) to `new`
/// (already patched when the request patches): `access` and `locks` that `new` leaves out keep
/// their stored values; changing them needs the rights to.
pub fn merge_edit(access: &Access, old: &Map, mut new: Map) -> Result<Map, ApiError> {
    for key in [rules::ACCESS_KEY, rules::LOCKS_KEY] {
        if !new.contains_key(key)
            && let Some(value) = old.get(key)
        {
            new.insert(key.to_string(), value.clone());
        }
    }
    if access.no_auth {
        return Ok(new);
    }
    let old_access = PipeAccess::from_parameters(old);
    if old.get(rules::ACCESS_KEY) != new.get(rules::ACCESS_KEY) {
        let requested = match new.get(rules::ACCESS_KEY) {
            None | Some(Value::Null) => PipeAccess::default(),
            Some(value) => PipeAccess::parse(value).map_err(|e| ApiError::new(400, e))?,
        };
        if requested != old_access {
            rules::check_access_change(&access.principal, &old_access, &requested)
                .map_err(forbidden)?;
        }
    }
    if rules::Locks::from_parameters(old) != rules::Locks::from_parameters(&new)
        && !rules::can_lock(&access.principal, &old_access)
    {
        return Err(forbidden(
            "Only the pipe's owner, its team's managers and administrators may lock or unlock it.",
        ));
    }
    Ok(new)
}

/// The connector `teams` restriction (`meerschaum:connectors:<type>:<label>:teams`): the teams
/// whose pipes may use the connector (`None`: every team).
pub fn connector_teams(state: &AppState, connector_keys: &str) -> Option<Vec<String>> {
    let (typ, label) = connector_keys.split_once(':')?;
    let attrs = state.config.connectors.get(typ)?.get(label)?;
    match attrs.get("teams")? {
        Value::List(items) => Some(items.iter().map(Value::py_str).collect()),
        Value::Str(s) => Some(
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
        ),
        Value::Null => None,
        other => Some(vec![other.py_str()]),
    }
}

/// Whether `access` may use the connector `connector_keys` for pipes (or SQL reads): every
/// connector without `teams`; else administrators and the members of a listed team.
pub fn may_use_connector(state: &AppState, access: &Access, connector_keys: &str) -> bool {
    meerschaum_core::access::can_use_connector(
        &access.principal,
        connector_teams(state, connector_keys).as_deref(),
    )
}

/// [`may_use_connector`] as a `403`.
pub fn check_connector_teams(
    state: &AppState,
    access: &Access,
    connector_keys: &str,
) -> Result<(), ApiError> {
    if may_use_connector(state, access, connector_keys) {
        return Ok(());
    }
    Err(forbidden(format!(
        "The connector '{connector_keys}' is reserved for other teams' pipes."
    )))
}

/// A connector with `teams: [...]` is for those teams' pipes: a pipe of `connector_keys` that
/// `access` registers, defines or gives to another team must belong to one of them (`team`, the
/// pipe's team after the change; `None` is Everyone), and the caller must be in one of them.
/// Administrators (and `--no-auth`) pass.
pub fn check_pipe_connector(
    state: &AppState,
    access: &Access,
    connector_keys: &str,
    team: Option<&str>,
) -> Result<(), ApiError> {
    if access.no_auth || access.is_admin() {
        return Ok(());
    }
    let Some(teams) = connector_teams(state, connector_keys) else {
        return Ok(());
    };
    check_connector_teams(state, access, connector_keys)?;
    if teams.iter().any(|t| rules::same_team(Some(t), team)) {
        return Ok(());
    }
    Err(forbidden(format!(
        "The connector '{connector_keys}' is reserved for the pipes of {}: give the pipe to one \
         of them.",
        teams
            .iter()
            .map(|t| format!("'{t}'"))
            .collect::<Vec<_>>()
            .join(", ")
    )))
}

/// Refuse a pipe whose configured or actual physical target aliases an internal or hidden
/// table. A SQL instance may shorten both default names and explicit target parameters.
pub fn check_pipe_target(
    conn: &dyn InstanceConnector,
    pipe: &Pipe,
    hidden: &sql_guard::HiddenTables,
) -> Result<(), ApiError> {
    let target = pipe
        .target(None)
        .map_err(|e| ApiError::new(400, e.to_string()))?;
    for target in std::iter::once(target).chain(conn.pipe_target_aliases(pipe)) {
        sql_guard::check_target(&target, hidden).map_err(forbidden)?;
    }
    Ok(())
}

/// Where a definition runs: a SQL pipe's source connector, otherwise its destination (the
/// framework cannot infer the database used inside a custom connector).
pub fn definition_instance_keys(pipe: &Pipe) -> &str {
    if pipe.connector_keys().starts_with("sql:") {
        pipe.connector_keys()
    } else {
        pipe.instance_keys()
    }
}

/// The hidden tables of a definition's actual SQL source, within the same served-instance
/// and connector-team restrictions as a direct SQL read. An external read-only source with
/// no registered pipes has an empty index; listing it never creates metadata tables.
pub async fn definition_hidden_targets(
    state: &AppState,
    access: &Access,
    pipe: &Pipe,
) -> Result<sql_guard::HiddenTables, ApiError> {
    let keys = definition_instance_keys(pipe);
    state.get_api_connector(Some(keys))?;
    check_connector_teams(state, access, keys)?;
    if access.is_admin() {
        return Ok(sql_guard::HiddenTables::default());
    }
    let index = super::pipe_index(state, Some(keys)).await?;
    Ok(index.hidden_targets(access))
}

/// Check the parameters `new` a caller registers (`old` is `None`) or edits a pipe with (`pipe`
/// has the pipe's keys and `new` as its parameters), on the instance `conn` whose pipes are in
/// `index`. Administrators skip ownership/content checks, but still need the definition
/// permission and a source allowed by the API's instance policy.
pub async fn check_parameters(
    state: &AppState,
    access: &Access,
    conn: &Arc<dyn InstanceConnector>,
    pipe: &Pipe,
    old: Option<&Map>,
    new: &Map,
) -> Result<(), ApiError> {
    let empty = Map::new();
    let defines = sql_guard::changes_definition(pipe.connector_keys(), old, new)
        || rules::sql_definition_changed(pipe.connector_keys(), old.unwrap_or(&empty), new);
    if defines && !access.has(rules::Permission::PIPES_DEFINE_SQL) {
        return Err(ApiError::new(
            403,
            "Missing required scope: 'pipes:define_sql' (setting a pipe's SQL definition).",
        ));
    }
    let definition_hidden = if defines {
        Some(definition_hidden_targets(state, access, pipe).await?)
    } else {
        None
    };
    if access.is_admin() {
        return Ok(());
    }
    let index = super::pipe_index(state, Some(&conn.connector_keys())).await?;
    let hidden = index.hidden_targets(access);
    // Keys derive a target even when no target parameter was supplied. Check that name too:
    // different keys can resolve to the same table (and `mrsm/users` names `mrsm_users`).
    let mut candidate = pipe.clone();
    candidate.set_parameters(new.clone());
    let target = candidate
        .target(None)
        .map_err(|e| ApiError::new(400, e.to_string()))?;
    let previous_target = old.and_then(|parameters| {
        let mut stored = pipe.clone();
        stored.set_parameters(parameters.clone());
        stored.target(None).ok()
    });
    if previous_target.as_deref() != Some(target.as_str()) {
        check_pipe_target(conn.as_ref(), &candidate, &hidden)?;
    }
    // A target of its own: never an internal table or a hidden pipe's.
    for key in sql_guard::TARGET_KEYS {
        if let Some(target) = new.get(*key).filter(|v| v.truthy()).map(Value::py_str)
            && old.and_then(|o| o.get(*key)).map(Value::py_str) != Some(target.clone())
        {
            sql_guard::check_target(&target, &hidden).map_err(forbidden)?;
        }
    }
    // A connector reserved for some teams: their pipes only, registered and defined by their
    // members.
    let new_team = PipeAccess::from_parameters(new).team;
    let team_changed = old.is_none_or(|o| {
        !rules::same_team(
            PipeAccess::from_parameters(o).team.as_deref(),
            new_team.as_deref(),
        )
    });
    if old.is_none() || defines || team_changed {
        check_pipe_connector(state, access, pipe.connector_keys(), new_team.as_deref())?;
    }
    // `{{ Pipe() }}` and `reference(s)` that this edit adds: only to pipes the caller can see.
    let defaults = state.config.pipe_defaults.clone();
    let before: Vec<Pipe> = old
        .map(|o| referenced(pipe, o, &defaults))
        .unwrap_or_default();
    for reference in referenced(pipe, new, &defaults) {
        if before.contains(&reference) {
            continue;
        }
        if !reference_visible(state, access, &reference).await? {
            return Err(forbidden(format!(
                "Refusing to reference {reference}: it is a pipe you cannot see."
            )));
        }
    }
    // The definition as it runs: templates resolved against the instance.
    let (resolved, _) =
        meerschaum_connector::template::resolve_pipe(&candidate, conn.as_ref(), None, &defaults)
            .await;
    if let Ok(target) = resolved.target(None)
        && previous_target.as_deref() != Some(target.as_str())
    {
        check_pipe_target(conn.as_ref(), &resolved, &hidden)?;
    }
    let Some(definition_hidden) = definition_hidden else {
        return Ok(());
    };
    for definition in sql_guard::definitions(new) {
        sql_guard::check_definition(&definition, &definition_hidden).map_err(forbidden)?;
    }
    for definition in sql_guard::definitions(resolved.parameters()) {
        sql_guard::check_definition(&definition, &definition_hidden).map_err(forbidden)?;
    }
    Ok(())
}

/// The pipes a parameters tree references (`{{ Pipe() }}` templates and `reference(s)`).
fn referenced(
    pipe: &Pipe,
    parameters: &Map,
    defaults: &meerschaum_core::pipe::PipeDefaults,
) -> Vec<Pipe> {
    let mut out = referenced_pipes_in_parameters(parameters, Some(pipe), defaults);
    let mut with = pipe.clone();
    with.set_parameters(parameters.clone());
    if let Ok(refs) = with.references(defaults) {
        for r in refs.into_iter().flatten() {
            if !out.contains(&r) {
                out.push(r);
            }
        }
    }
    out
}

/// Whether `access` may see the referenced pipe `reference` (on its own instance, which must be
/// one this server serves to the caller).
async fn reference_visible(
    state: &AppState,
    access: &Access,
    reference: &Pipe,
) -> Result<bool, ApiError> {
    let index = match super::pipe_index(state, Some(reference.instance_keys())).await {
        Ok(index) => index,
        Err(_) => return Ok(false),
    };
    Ok(
        match index.get(
            reference.connector_keys(),
            reference.metric_key(),
            reference.location_key(),
        ) {
            Some(p) => access.can_see_indexed(p),
            None => true,
        },
    )
}

/// A lock refusal of `op` on `pipe` as a `409` (`None` when its locks allow it).
pub fn lock_conflict(pipe: &Pipe, op: rules::LockOp) -> Option<ApiError> {
    let locks = rules::Locks::of_pipe(pipe);
    rules::lock_refusal(pipe, &locks, op).map(|m| ApiError::new(409, m))
}

/// The refusal of an edit from `old` to `new` under `pipe`'s `parameters` lock, as a `409`.
pub fn parameters_lock_conflict(pipe: &Pipe, old: &Map, new: &Map) -> Option<ApiError> {
    let locks = rules::Locks::from_parameters(old);
    if rules::parameters_edit_allowed(old, new, &locks, false) {
        return None;
    }
    rules::lock_refusal(pipe, &locks, rules::LockOp::EditParameters).map(|m| ApiError::new(409, m))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(json: &str) -> Map {
        Value::from_json_str(json).unwrap().to_map()
    }

    fn member(name: &str, teams: &[(&str, rules::TeamRole)]) -> Access {
        let mut all = vec![(rules::EVERYONE.to_string(), rules::TeamRole::Member)];
        all.extend(teams.iter().map(|(t, r)| (t.to_string(), *r)));
        let mut access = Access::everything();
        access.no_auth = false;
        access.principal =
            rules::Principal::new(name, false, rules::Role::Editor.permissions(), all.clone());
        access.primary_team = rules::primary_team(None, &all);
        access
    }

    #[test]
    fn new_pipes_belong_to_their_creator_and_primary_team() {
        let alice = member("alice", &[("data", rules::TeamRole::Member)]);
        let out = stamp_new_pipe(&alice, &map(r#"{"columns": {"datetime": "ts"}}"#)).unwrap();
        let stamped = PipeAccess::from_parameters(&out);
        assert_eq!(stamped.owner.as_deref(), Some("alice"));
        assert_eq!(stamped.team.as_deref(), Some("data"));
        // Owner spoofing: a non-administrator is always the owner.
        let spoof = stamp_new_pipe(&alice, &map(r#"{"access": {"owner": "bob"}}"#)).unwrap();
        assert_eq!(
            PipeAccess::from_parameters(&spoof).owner.as_deref(),
            Some("alice")
        );
        // Only one's own teams.
        assert!(stamp_new_pipe(&alice, &map(r#"{"access": {"team": "ops"}}"#)).is_err());
        assert!(stamp_new_pipe(&alice, &map(r#"{"access": {"visibility": "nope"}}"#)).is_err());
        // `--no-auth` keeps the parameters as given.
        let open = stamp_new_pipe(&Access::everything(), &map("{}")).unwrap();
        assert!(open.get("access").is_none());
    }

    #[test]
    fn edits_keep_access_and_locks_unless_allowed_to_change_them() {
        let old = map(
            r#"{"access": {"owner": "alice", "team": "data"}, "locks": ["drop"], "columns": {}}"#,
        );
        let bob = member("bob", &[("data", rules::TeamRole::Member)]);
        // Left out: kept.
        let out = merge_edit(&bob, &old, map(r#"{"columns": {"id": "x"}}"#)).unwrap();
        assert_eq!(out.get("access"), old.get("access"));
        assert_eq!(out.get("locks"), old.get("locks"));
        // Changed without rights: refused.
        assert!(
            merge_edit(
                &bob,
                &old,
                map(r#"{"access": {"owner": "bob", "team": "data"}}"#)
            )
            .is_err()
        );
        assert!(merge_edit(&bob, &old, map(r#"{"locks": []}"#)).is_err());
        // The owner and the team's managers may.
        let alice = member("alice", &[("data", rules::TeamRole::Member)]);
        assert!(merge_edit(&alice, &old, map(r#"{"locks": []}"#)).is_ok());
        let mia = member("mia", &[("data", rules::TeamRole::Manager)]);
        assert!(
            merge_edit(
                &mia,
                &old,
                map(r#"{"access": {"owner": "mia", "team": "data", "visibility": "private"}}"#)
            )
            .is_ok()
        );
    }
}
