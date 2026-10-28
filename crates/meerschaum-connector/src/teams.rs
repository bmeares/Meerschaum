//! Teams on an instance ([`meerschaum_core::access::Team`]): the internal pipe `mrsm:teams`
//! (target `mrsm_teams`), beside users and tokens, on the API server's user-store instance.
//!
//! The defaults of the `InstanceConnector` team methods live here and work on any instance that
//! can store pipes (SQL, Valkey, custom instance connectors through the worker). The built-in
//! team [`EVERYONE`] has every user as a member and always exists; a stored record (id
//! [`EVERYONE_TEAM_ID`]) only names its managers and attributes.
//!
//! Names are unique without case and follow the username rules
//! ([`access::valid_team_name`]); members must be registered users. Renaming a team renames it in
//! the `access` of the pipes and saved Pipelines that name it and in its users' primary team;
//! deleting one moves them to Everyone. Deleting a user ([`forget_user`]) removes them from every
//! team and clears their ownership of pipes (the pipes stay with their team). These edits run as
//! access maintenance ([`crate::locks::access_maintenance`]): a `parameters` lock does not stop
//! them, since a pipe must not keep naming a team that no longer exists.

use indexmap::IndexMap;
use meerschaum_core::access::{
    self, ACCESS_KEY, EVERYONE, EVERYONE_TEAM_ID, PRIMARY_TEAM_ATTRIBUTE, PipeAccess, Team,
    TeamRole,
};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::value::Value as KValue;
use uuid::Uuid;

use crate::pipe_ops::{self, Doc};
use crate::traits::{ConnectorResult, DataQuery, InstanceConnector, PipesKeysQuery};

/// `InstanceConnector.get_teams_pipe()`: `mrsm:teams`, target `mrsm_teams`, one row per team
/// (`team_id` uuid primary key, `name` unique, `members` and `attributes` json).
pub fn teams_pipe(instance_keys: &str) -> Pipe {
    let s = |pairs: &[(&str, &str)]| -> Value {
        Value::Map(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), Value::Str(v.to_string())))
                .collect(),
        )
    };
    Pipe::builder("mrsm", "teams")
        .instance(instance_keys)
        .kwarg("target", "mrsm_teams")
        .kwarg("temporary", true)
        .kwarg("cache", true)
        .kwarg("static", true)
        .kwarg("null_indices", false)
        .kwarg("columns", s(&[("primary", "team_id")]))
        .kwarg(
            "dtypes",
            s(&[
                ("team_id", "uuid"),
                ("name", "string"),
                ("members", "json"),
                ("attributes", "json"),
            ]),
        )
        .kwarg("indices", s(&[("unique", "name")]))
        .build()
        .expect("static internal pipe keys are valid")
}

/// A stored row as a team (`None` without a name).
fn team_of_doc(doc: &Doc) -> Option<Team> {
    let mut map = Map::new();
    for key in ["team_id", "name", "members", "attributes"] {
        if let Some(v) = doc.get(key).filter(|v| !v.is_na()) {
            map.insert(key.to_string(), pipe_ops::kernel_to_core(v));
        }
    }
    Team::from_value(&Value::Map(map))
}

/// A team as a row.
fn doc_of_team(team: &Team, team_id: Uuid) -> Doc {
    [
        ("team_id".to_string(), KValue::Uuid(team_id)),
        ("name".to_string(), KValue::Str(team.name.clone())),
        (
            "members".to_string(),
            pipe_ops::core_to_kernel(&access::members_value(&team.members)),
        ),
        (
            "attributes".to_string(),
            pipe_ops::core_to_kernel(&Value::Map(team.attributes.clone())),
        ),
    ]
    .into_iter()
    .collect()
}

/// The stored team records (Everyone's too, when it has one), in storage order.
///
/// One record per id, the last one stored: an instance connector that appends rows instead of
/// upserting them (a minimal custom one) keeps every version of a team, and the newest wins.
pub async fn stored_teams<C: InstanceConnector + ?Sized>(conn: &C) -> ConnectorResult<Vec<Team>> {
    let pipe = conn.get_teams_pipe();
    let docs = pipe_ops::get_docs(conn, &pipe, &DataQuery::default()).await?;
    let mut out: Vec<Team> = Vec::with_capacity(docs.len());
    for team in docs.iter().filter_map(team_of_doc) {
        match out.iter().position(|t| t.id.is_some() && t.id == team.id) {
            Some(i) => out[i] = team,
            None => out.push(team),
        }
    }
    Ok(out)
}

/// `InstanceConnector.get_teams()`: Everyone first, then the other teams by name.
pub async fn default_get_teams<C: InstanceConnector + ?Sized>(
    conn: &C,
) -> ConnectorResult<Vec<Team>> {
    let mut stored = stored_teams(conn).await?;
    let everyone = match stored.iter().position(Team::is_everyone) {
        Some(i) => {
            let mut record = stored.remove(i);
            record.name = EVERYONE.to_string();
            record.id = Some(EVERYONE_TEAM_ID.to_string());
            record
        }
        None => Team::everyone(),
    };
    stored.retain(|t| !t.is_everyone());
    stored.sort_by_key(|t| t.name.to_lowercase());
    let mut out = Vec::with_capacity(stored.len() + 1);
    out.push(everyone);
    out.extend(stored);
    Ok(out)
}

/// `InstanceConnector.get_team(name)`: the team named `name` (any case), Everyone included.
pub async fn default_get_team<C: InstanceConnector + ?Sized>(
    conn: &C,
    name: &str,
) -> ConnectorResult<Option<Team>> {
    Ok(conn
        .get_teams()
        .await?
        .into_iter()
        .find(|t| access::same_team(Some(&t.name), Some(name))))
}

/// `InstanceConnector.get_user_teams(username)`: the user's teams with their roles, Everyone
/// first (every user is in it; its record may make them a manager).
pub async fn default_get_user_teams<C: InstanceConnector + ?Sized>(
    conn: &C,
    username: &str,
) -> ConnectorResult<Vec<(String, TeamRole)>> {
    Ok(conn
        .get_teams()
        .await?
        .iter()
        .filter_map(|t| t.role_of(username).map(|role| (t.name.clone(), role)))
        .collect())
}

/// The members of `team` that are not registered users.
async fn unknown_members<C: InstanceConnector + ?Sized>(
    conn: &C,
    team: &Team,
) -> ConnectorResult<Vec<String>> {
    if team.members.is_empty() {
        return Ok(Vec::new());
    }
    let users = conn.get_users().await?;
    Ok(team
        .members
        .keys()
        .filter(|m| !users.iter().any(|u| u == *m))
        .cloned()
        .collect())
}

fn unknown_members_failure(unknown: &[String]) -> SuccessTuple {
    SuccessTuple::fail(format!(
        "{} not registered: {}.",
        if unknown.len() == 1 {
            "This user is"
        } else {
            "These users are"
        },
        unknown
            .iter()
            .map(|u| format!("'{u}'"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// `InstanceConnector.register_team(team)`: a new team with a new id (written to `team.id`).
pub async fn default_register_team<C: InstanceConnector + ?Sized>(
    conn: &C,
    team: &mut Team,
) -> SuccessTuple {
    team.name = team.name.trim().to_string();
    if team.is_everyone() {
        return SuccessTuple::fail(format!(
            "'{EVERYONE}' is the built-in team of every user: edit it instead."
        ));
    }
    let valid = access::valid_team_name(&team.name);
    if !valid.success {
        return valid;
    }
    match stored_teams(conn).await {
        Ok(teams) => {
            if let Some(existing) = teams
                .iter()
                .find(|t| access::same_team(Some(&t.name), Some(&team.name)))
            {
                return SuccessTuple::fail(format!("Team '{}' already exists.", existing.name));
            }
        }
        Err(e) => return SuccessTuple::fail(e.to_string()),
    }
    match unknown_members(conn, team).await {
        Ok(unknown) if !unknown.is_empty() => return unknown_members_failure(&unknown),
        Ok(_) => {}
        Err(e) => return SuccessTuple::fail(e.to_string()),
    }
    let team_id = Uuid::new_v4();
    let pipe = conn.get_teams_pipe();
    let res = pipe_ops::sync_docs(conn, &pipe, vec![doc_of_team(team, team_id)], false).await;
    if !res.success {
        return SuccessTuple::fail(format!(
            "Failed to register team '{}':\n{}",
            team.name, res.message
        ));
    }
    team.id = Some(team_id.to_string());
    SuccessTuple::ok(format!("Registered team '{}'.", team.name))
}

/// The stored record a team update refers to: by id when it has one, else by name.
fn find_stored<'a>(stored: &'a [Team], team: &Team) -> Option<&'a Team> {
    if let Some(id) = team.id.as_deref()
        && let Some(found) = stored.iter().find(|t| t.id.as_deref() == Some(id))
    {
        return Some(found);
    }
    stored
        .iter()
        .find(|t| access::same_team(Some(&t.name), Some(&team.name)))
}

/// `InstanceConnector.edit_team(team)`: store the team's members and attributes (its name stays:
/// rename it with `rename_team`). Everyone's record is created on its first edit.
pub async fn default_edit_team<C: InstanceConnector + ?Sized>(
    conn: &C,
    team: &Team,
) -> SuccessTuple {
    let stored = match stored_teams(conn).await {
        Ok(t) => t,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let (team_id, name) = if team.is_everyone() || team.id.as_deref() == Some(EVERYONE_TEAM_ID) {
        (Uuid::nil(), EVERYONE.to_string())
    } else {
        let Some(existing) = find_stored(&stored, team) else {
            return SuccessTuple::fail(format!("Team '{}' does not exist.", team.name));
        };
        if !access::same_team(Some(&existing.name), Some(&team.name)) {
            return SuccessTuple::fail(format!(
                "Team '{}' is named '{}': rename it with `rename_team`.",
                team.name, existing.name
            ));
        }
        let Some(id) = existing.id.as_deref().and_then(|i| Uuid::parse_str(i).ok()) else {
            return SuccessTuple::fail(format!("Team '{}' has no valid id.", existing.name));
        };
        (id, existing.name.clone())
    };
    match unknown_members(conn, team).await {
        Ok(unknown) if !unknown.is_empty() => return unknown_members_failure(&unknown),
        Ok(_) => {}
        Err(e) => return SuccessTuple::fail(e.to_string()),
    }
    let record = Team {
        id: Some(team_id.to_string()),
        name: name.clone(),
        members: team.members.clone(),
        attributes: team.attributes.clone(),
    };
    let pipe = conn.get_teams_pipe();
    let res = pipe_ops::sync_docs(conn, &pipe, vec![doc_of_team(&record, team_id)], true).await;
    if !res.success {
        return SuccessTuple::fail(format!("Failed to edit team '{name}':\n{}", res.message));
    }
    SuccessTuple::ok(format!("Edited team '{name}'."))
}

/// The pipes (and saved Pipelines) of this instance whose `access` names `team` (any case;
/// Everyone: the pipes without a team), with their registered parameters.
pub async fn team_pipes<C: InstanceConnector + ?Sized>(
    conn: &C,
    team: &str,
) -> ConnectorResult<Vec<Pipe>> {
    registered_pipes_where(conn, |access| access.in_team(Some(team))).await
}

/// The pipes (and saved Pipelines) whose `access` names `username` as their owner.
pub async fn owned_pipes<C: InstanceConnector + ?Sized>(
    conn: &C,
    username: &str,
) -> ConnectorResult<Vec<Pipe>> {
    registered_pipes_where(conn, |access| access.is_owner(username)).await
}

/// Every registered pipe and saved Pipeline (`mrsm:pipeline:*`) whose access satisfies `keep`,
/// with its registered parameters.
async fn registered_pipes_where<C, F>(conn: &C, keep: F) -> ConnectorResult<Vec<Pipe>>
where
    C: InstanceConnector + ?Sized,
    F: Fn(&PipeAccess) -> bool,
{
    let instance_keys = conn.connector_keys();
    let queries = [
        PipesKeysQuery::default(),
        PipesKeysQuery {
            connector_keys: vec![crate::policy::INTERNAL_CONNECTOR_KEYS.to_string()],
            metric_keys: vec!["pipeline".to_string()],
            ..Default::default()
        },
    ];
    let mut out: Vec<Pipe> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for query in &queries {
        let keys = conn.fetch_pipes_keys(query).await?;
        for entry in keys.entries() {
            let location = entry
                .location_key
                .as_deref()
                .filter(|l| !matches!(*l, "None" | "[None]" | "null" | ""));
            let Ok(mut pipe) = Pipe::builder(&entry.connector_keys, &entry.metric_key)
                .location(location)
                .instance(&instance_keys)
                .build()
            else {
                continue;
            };
            if !seen.insert(pipe.pipe_ref()) {
                continue;
            }
            // The dict form carries the parameters; a list form may carry tags, or (Valkey) an
            // empty map: read the registration then.
            let parameters = match &entry.extra {
                Some(Value::Map(m)) if !m.is_empty() => m.clone(),
                _ => match crate::locks::registered_parameters(conn, &pipe).await? {
                    Some(p) => p,
                    None => continue,
                },
            };
            if keep(&PipeAccess::from_parameters(&parameters)) {
                pipe.set_parameters(parameters);
                out.push(pipe);
            }
        }
    }
    Ok(out)
}

/// Rewrite the `access` of `pipes` with `change` (access maintenance: allowed under a
/// `parameters` lock). Returns how many were rewritten, or the failures.
async fn rewrite_access<C, F>(conn: &C, pipes: Vec<Pipe>, change: F) -> Result<usize, String>
where
    C: InstanceConnector + ?Sized,
    F: Fn(&mut PipeAccess),
{
    let mut done = 0;
    let mut errors = Vec::new();
    for mut pipe in pipes {
        let mut access_value = PipeAccess::from_parameters(pipe.raw_parameters());
        change(&mut access_value);
        let mut parameters = pipe.raw_parameters().clone();
        parameters.insert(ACCESS_KEY.to_string(), access_value.to_value());
        pipe.set_parameters(parameters);
        let res = crate::locks::access_maintenance(conn.edit_pipe(&pipe, false)).await;
        if res.success {
            done += 1;
        } else {
            errors.push(format!("{pipe}: {}", res.message));
        }
    }
    if errors.is_empty() {
        Ok(done)
    } else {
        Err(errors.join("\n"))
    }
}

/// Set the primary team of the users whose primary team is `from` to `to` (`None`: their
/// default). Returns the failures.
async fn rewrite_primary_teams<C: InstanceConnector + ?Sized>(
    conn: &C,
    from: &str,
    to: Option<&str>,
) -> Vec<String> {
    let usernames = match conn.get_users().await {
        Ok(u) => u,
        Err(e) => return vec![e.to_string()],
    };
    let mut errors = Vec::new();
    for username in usernames {
        let user = User::new(&username, None);
        let attributes = match conn.get_user_attributes(&user).await {
            Ok(Some(a)) => a,
            Ok(None) => continue,
            Err(e) => {
                errors.push(format!("{username}: {e}"));
                continue;
            }
        };
        let stored = attributes
            .get(PRIMARY_TEAM_ATTRIBUTE)
            .and_then(Value::as_str)
            .map(str::to_string);
        if !stored.is_some_and(|s| {
            !access::is_everyone(Some(&s)) && access::same_team(Some(&s), Some(from))
        }) {
            continue;
        }
        let mut new = attributes.clone();
        new.insert(
            PRIMARY_TEAM_ATTRIBUTE.to_string(),
            to.map(|t| Value::Str(t.to_string())).unwrap_or(Value::Null),
        );
        let res = conn.set_user_attributes(&user, &new).await;
        if !res.success {
            errors.push(format!("{username}: {}", res.message));
        }
    }
    errors
}

fn count_pipes(n: usize) -> String {
    format!("{n} pipe{}", if n == 1 { "" } else { "s" })
}

/// `InstanceConnector.delete_team(name)`: delete the team; its pipes and saved Pipelines move
/// to Everyone and its users' primary team falls back to their default. Everyone cannot be
/// deleted.
pub async fn default_delete_team<C: InstanceConnector + ?Sized>(
    conn: &C,
    name: &str,
) -> SuccessTuple {
    if access::is_everyone(Some(name)) {
        return SuccessTuple::fail(format!(
            "The team '{EVERYONE}' is built in and cannot be deleted."
        ));
    }
    let stored = match stored_teams(conn).await {
        Ok(t) => t,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let Some(team) = stored
        .iter()
        .find(|t| access::same_team(Some(&t.name), Some(name)))
        .cloned()
    else {
        return SuccessTuple::fail(format!("Team '{name}' does not exist."));
    };
    // Move the pipes first: a failure leaves the team in place.
    let pipes = match team_pipes(conn, &team.name).await {
        Ok(p) => p,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let moved = match rewrite_access(conn, pipes, |a| a.team = None).await {
        Ok(n) => n,
        Err(e) => {
            return SuccessTuple::fail(format!(
                "Failed to move the pipes of team '{}' to {EVERYONE}:\n{e}",
                team.name
            ));
        }
    };
    let Some(id) = team.id.clone() else {
        return SuccessTuple::fail(format!("Team '{}' has no id.", team.name));
    };
    let mut params = Map::new();
    params.insert("team_id".into(), Value::Str(id));
    let res = conn
        .clear_pipe(
            &conn.get_teams_pipe(),
            &DataQuery {
                params: Some(params),
                ..Default::default()
            },
        )
        .await;
    if !res.success {
        return SuccessTuple::fail(format!(
            "Failed to delete team '{}':\n{}",
            team.name, res.message
        ));
    }
    for error in rewrite_primary_teams(conn, &team.name, None).await {
        tracing::warn!(
            "Could not reset a primary team after deleting '{}': {error}",
            team.name
        );
    }
    SuccessTuple::ok(format!(
        "Deleted team '{}' ({} moved to {EVERYONE}).",
        team.name,
        count_pipes(moved)
    ))
}

/// `InstanceConnector.rename_team(old, new)`: rename the team, in the `access` of its pipes and
/// saved Pipelines and in its users' primary team. Everyone cannot be renamed.
pub async fn default_rename_team<C: InstanceConnector + ?Sized>(
    conn: &C,
    old: &str,
    new: &str,
) -> SuccessTuple {
    let new = new.trim();
    if access::is_everyone(Some(old)) {
        return SuccessTuple::fail(format!(
            "The team '{EVERYONE}' is built in and cannot be renamed."
        ));
    }
    let valid = access::valid_team_name(new);
    if !valid.success {
        return valid;
    }
    let stored = match stored_teams(conn).await {
        Ok(t) => t,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let Some(team) = stored
        .iter()
        .find(|t| access::same_team(Some(&t.name), Some(old)))
        .cloned()
    else {
        return SuccessTuple::fail(format!("Team '{old}' does not exist."));
    };
    if let Some(other) = stored
        .iter()
        .find(|t| access::same_team(Some(&t.name), Some(new)) && t.id != team.id)
    {
        return SuccessTuple::fail(format!("Team '{}' already exists.", other.name));
    }
    let Some(team_id) = team.id.as_deref().and_then(|i| Uuid::parse_str(i).ok()) else {
        return SuccessTuple::fail(format!("Team '{}' has no valid id.", team.name));
    };
    // The pipes that name the team, read before the rename.
    let pipes = match team_pipes(conn, &team.name).await {
        Ok(p) => p,
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let renamed = Team {
        name: new.to_string(),
        ..team.clone()
    };
    let res = pipe_ops::sync_docs(
        conn,
        &conn.get_teams_pipe(),
        vec![doc_of_team(&renamed, team_id)],
        true,
    )
    .await;
    if !res.success {
        return SuccessTuple::fail(format!(
            "Failed to rename team '{}':\n{}",
            team.name, res.message
        ));
    }
    let updated = match rewrite_access(conn, pipes, |a| a.team = Some(new.to_string())).await {
        Ok(n) => n,
        Err(e) => {
            return SuccessTuple::fail(format!(
                "Renamed team '{}' to '{new}', but some of its pipes still name '{}':\n{e}",
                team.name, team.name
            ));
        }
    };
    for error in rewrite_primary_teams(conn, &team.name, Some(new)).await {
        tracing::warn!(
            "Could not update a primary team after renaming '{}': {error}",
            team.name
        );
    }
    SuccessTuple::ok(format!(
        "Renamed team '{}' to '{new}' ({} updated).",
        team.name,
        count_pipes(updated)
    ))
}

/// After a user is deleted: remove them from every team and clear their ownership of pipes and
/// saved Pipelines (which stay with their team). Best effort: the failures are returned.
pub async fn forget_user<C: InstanceConnector + ?Sized>(conn: &C, username: &str) -> SuccessTuple {
    let mut errors = Vec::new();
    match stored_teams(conn).await {
        Ok(teams) => {
            for mut team in teams {
                if team.remove_member(username) {
                    let res = conn.edit_team(&team).await;
                    if !res.success {
                        errors.push(res.message);
                    }
                }
            }
        }
        Err(e) => errors.push(e.to_string()),
    }
    match owned_pipes(conn, username).await {
        Ok(pipes) => {
            if let Err(e) = rewrite_access(conn, pipes, |a| a.owner = None).await {
                errors.push(e);
            }
        }
        Err(e) => errors.push(e.to_string()),
    }
    if errors.is_empty() {
        SuccessTuple::success()
    } else {
        SuccessTuple::fail(format!(
            "Could not remove user '{username}' from every team and pipe:\n{}",
            errors.join("\n")
        ))
    }
}

/// `InstanceConnector.set_user_attributes(user, attributes)` over the users pipe: replace the
/// user's attributes and nothing else.
pub async fn default_set_user_attributes<C: InstanceConnector + ?Sized>(
    conn: &C,
    user: &User,
    attributes: &Map,
) -> SuccessTuple {
    let user_id = match pipe_ops::resolve_user_id(conn, user).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            return SuccessTuple::fail(format!("User '{}' does not exist.", user.username));
        }
        Err(e) => return SuccessTuple::fail(e.to_string()),
    };
    let mut doc: Doc = IndexMap::new();
    doc.insert("user_id".into(), pipe_ops::user_id_value(&user_id));
    doc.insert(
        "attributes".into(),
        pipe_ops::core_to_kernel(&Value::Map(attributes.clone())),
    );
    let res = pipe_ops::sync_docs(conn, &conn.get_users_pipe(), vec![doc], true).await;
    if !res.success {
        return SuccessTuple::fail(format!(
            "Failed to edit user '{}':\n{}",
            user.username, res.message
        ));
    }
    SuccessTuple::success()
}
