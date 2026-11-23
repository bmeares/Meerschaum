//! Principals and the access rules applied to them: who may see, edit, lock or manage a pipe, see
//! a user or team, and use a connector or plugin.

use super::*;

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
    /// What those teams grant and withhold ([`TeamGrants`]; the default changes nothing).
    #[serde(default)]
    pub grants: TeamGrants,
    /// An execution capability may restrict even ownership and membership to one current team.
    /// The server resolves its immutable team UUID before setting this display-name boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_boundary: Option<String>,
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
            grants: TeamGrants::default(),
            team_boundary: None,
        }
    }

    /// The same principal with `grants` (an administrator is never walled off). A walled-off
    /// principal loses [`withheld_permissions`]: actions and the terminal run on the server with
    /// its own credentials, which reach every team's data, except as their team's sandboxed
    /// processes for a member of a team that runs code ([`TeamGrants::runs_code`]).
    pub fn with_grants(mut self, grants: TeamGrants) -> Principal {
        self.grants = grants;
        if self.is_admin {
            self.grants.isolated = false;
            self.grants.runs_code = false;
        } else if self.grants.isolated {
            if self.permissions.contains(ALL_SCOPE) {
                self.permissions = all_permissions();
            }
            for permission in withheld_permissions(&self.grants) {
                self.permissions.remove(permission);
            }
        } else {
            self.grants.runs_code = false;
        }
        self
    }

    /// Whether the principal is walled off from Everyone ([`TeamGrants::isolated`]).
    pub fn is_isolated(&self) -> bool {
        self.team_boundary.is_some() || (!self.is_admin && self.grants.isolated)
    }

    /// Restrict an execution principal to one team. It is never a global administrator.
    pub fn within_team(mut self, team: &str) -> Self {
        self.team_boundary = Some(team.trim().to_string());
        self.is_admin = false;
        // Existing grants may combine several teams. A host may attach freshly resolved
        // grants for this one team with `with_grants` after imposing the boundary.
        self.grants = TeamGrants::default();
        if self.permissions.remove(ALL_SCOPE) {
            self.permissions.extend(all_permissions());
        }
        self.teams
            .retain(|(name, _)| same_team(Some(name), Some(team)));
        self
    }

    /// Whether an object belongs to the capability's team (`None` for unrestricted callers).
    pub fn permits_team(&self, team: Option<&str>) -> bool {
        self.team_boundary
            .as_deref()
            .is_none_or(|boundary| same_team(Some(boundary), team))
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
        (self.is_admin && self.team_boundary.is_none())
            || self.permissions.contains(permission)
            || self.permissions.contains(ALL_SCOPE)
    }

    /// The principal's role in `team` (`None` is Everyone, where every user is a member).
    pub fn team_role(&self, team: Option<&str>) -> Option<TeamRole> {
        if !self.permits_team(team) {
            return None;
        }
        let listed = self
            .teams
            .iter()
            .filter(|(name, _)| same_team(Some(name), team))
            .map(|(_, role)| *role)
            .max();
        // A walled-off principal is not in Everyone (a manager appointed there still is).
        if is_everyone(team) && self.is_isolated() && self.team_boundary.is_none() {
            return listed.filter(|role| *role == TeamRole::Manager);
        }
        match listed {
            Some(role) => Some(role),
            None if is_everyone(team) => Some(TeamRole::Member),
            None => None,
        }
    }

    /// Whether the principal is in `team` (any role; every user is in Everyone, unless they are
    /// walled off: [`Principal::is_isolated`]).
    pub fn is_member(&self, team: Option<&str>) -> bool {
        self.team_role(team).is_some()
    }

    /// Whether the principal manages `team`.
    pub fn is_manager(&self, team: Option<&str>) -> bool {
        self.team_role(team) == Some(TeamRole::Manager)
    }

    /// The names of the teams the principal is in (Everyone first).
    pub fn team_names(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.is_member(Some(EVERYONE)) {
            out.push(EVERYONE.to_string());
        }
        for (name, _) in &self.teams {
            if !self.permits_team(Some(name)) {
                continue;
            }
            if is_everyone(Some(name)) {
                continue;
            }
            if !out.iter().any(|n| same_team(Some(n), Some(name))) {
                out.push(name.clone());
            }
        }
        out
    }
}

/// Whether the reservation names any team whose role can extend Everyone.
pub fn extends_everyone(reservation: &[String]) -> bool {
    reservation.iter().any(|team| !is_everyone(Some(team)))
}

/// The principal's highest role among the instance's reserved teams.
fn reserved_role(principal: &Principal, reservation: &[String]) -> Option<TeamRole> {
    reservation
        .iter()
        .filter(|team| !is_everyone(Some(team)))
        .filter_map(|team| principal.team_role(Some(team)))
        .max()
}

fn instance_team_role(
    principal: &Principal,
    team: Option<&str>,
    reservation: &[String],
) -> Option<TeamRole> {
    let role = principal.team_role(team);
    if is_everyone(team) {
        role.max(reserved_role(principal, reservation))
    } else {
        role
    }
}

fn permits_pipe_team(principal: &Principal, team: Option<&str>, reservation: &[String]) -> bool {
    principal.permits_team(team)
        || (is_everyone(team)
            && principal.team_boundary.as_deref().is_some_and(|boundary| {
                reservation
                    .iter()
                    .any(|reserved| same_team(Some(reserved), Some(boundary)))
            }))
}

/// Whether `principal` can see a pipe with `access` on this instance: administrators and the owner;
/// visibility `everyone`: every user; `team`: the team's members; `private`: the team's
/// managers. On a reserved instance, the reserved teams are included in Everyone for this pipe.
pub fn can_see(principal: &Principal, access: &PipeAccess, reservation: &[String]) -> bool {
    let team = access.team.as_deref();
    if !permits_pipe_team(principal, team, reservation) {
        return false;
    }
    if principal.is_admin || access.is_owner(&principal.username) {
        return true;
    }
    match access.visibility {
        Visibility::Everyone => {
            !principal.is_isolated()
                || principal.is_member(team)
                || reserved_role(principal, reservation).is_some()
        }
        Visibility::Team => instance_team_role(principal, team, reservation).is_some(),
        Visibility::Private => {
            instance_team_role(principal, team, reservation) == Some(TeamRole::Manager)
        }
    }
}

/// Whether `principal` may change a pipe with `access` (within their permissions, which the
/// caller checks): edit `team`: anyone who can see it; edit `owner`: the owner, the team's
/// managers and administrators.
pub fn can_edit(principal: &Principal, access: &PipeAccess, reservation: &[String]) -> bool {
    if !can_see(principal, access, reservation) {
        return false;
    }
    match access.edit {
        EditMode::Team => true,
        EditMode::Owner => can_manage_access(principal, access, reservation),
    }
}

/// Whether `principal` may change a pipe's `access`: its owner, its team's managers and
/// administrators.
pub fn can_manage_access(
    principal: &Principal,
    access: &PipeAccess,
    reservation: &[String],
) -> bool {
    let team = access.team.as_deref();
    permits_pipe_team(principal, team, reservation)
        && (principal.is_admin
            || access.is_owner(&principal.username)
            || instance_team_role(principal, team, reservation) == Some(TeamRole::Manager))
}

/// Whether the principal may assign a new owner to a pipe of this team.
pub fn can_assign_pipe_owner(
    principal: &Principal,
    team: Option<&str>,
    reservation: &[String],
) -> bool {
    permits_pipe_team(principal, team, reservation)
        && (principal.is_admin
            || instance_team_role(principal, team, reservation) == Some(TeamRole::Manager))
}

/// Whether `principal` may lock or unlock a pipe through the API or the console: its owner,
/// its team's managers and administrators (locally anyone with access to the instance may).
pub fn can_lock(principal: &Principal, access: &PipeAccess, reservation: &[String]) -> bool {
    can_manage_access(principal, access, reservation)
}

/// Whether `viewer` may see the email address of a user in `target_user_teams` (that user's
/// teams): administrators, and the managers of a team the user belongs to (every user belongs to
/// Everyone, so its managers see every address). A user always sees their own
/// ([`can_see_email_of`]).
pub fn can_see_email(viewer: &Principal, target_user_teams: &[(String, TeamRole)]) -> bool {
    (viewer.is_admin && viewer.team_boundary.is_none())
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
    principal.permits_team(Some(team)) && (principal.is_admin || principal.is_manager(Some(team)))
}

/// Whether `principal` may create, rename and delete teams: administrators.
pub fn can_administer_teams(principal: &Principal) -> bool {
    principal.team_boundary.is_none() && principal.is_admin
}

/// How a deleted team's connector reservations (`teams: [<name>]`) are rewritten: a name no team
/// can have (team names never hold a space or `#`), so the connector, its database and data stay
/// but nothing is inherited by a later team of the same name. `id`: the deleted team's id.
pub fn retired_reservation(team: &str, id: Option<&str>) -> String {
    format!("Deleted team {team} #{}", id.unwrap_or("unknown"))
}

/// The `teams` attribute of the named connector, without inherited defaults.
fn connector_teams_attribute<'a>(connectors: &'a Map, connector_keys: &str) -> Option<&'a Value> {
    let (typ, label) = connector_keys.split_once(':')?;
    connectors.get(typ)?.get(label)?.get("teams")
}

/// The `teams` restriction used by the connector gate. An absent or null value is unrestricted.
pub fn connector_teams(connectors: &Map, connector_keys: &str) -> Option<Vec<String>> {
    match connector_teams_attribute(connectors, connector_keys)? {
        Value::List(items) => Some(items.iter().map(Value::py_str).collect()),
        Value::Str(names) => Some(
            names
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect(),
        ),
        Value::Null => None,
        other => Some(vec![other.py_str()]),
    }
}

/// The named teams that extend Everyone on this instance.
pub fn connector_reservation(connectors: &Map, connector_keys: &str) -> Option<Vec<String>> {
    pipe_reservation(connector_teams_attribute(connectors, connector_keys)?)
}

/// The pipe reservation described by a connector's `teams` attribute.
pub fn pipe_reservation(attribute: &Value) -> Option<Vec<String>> {
    match attribute {
        Value::List(items) => Some(
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
        ),
        Value::Str(_) => reservation_names(attribute),
        _ => None,
    }
}

/// The teams a connector's `teams` attribute names (a list, or a comma-separated string).
fn reservation_names(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Null => None,
        Value::List(items) => Some(
            items
                .iter()
                .filter(|v| !v.is_null())
                .map(|v| v.py_str().trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        ),
        Value::Str(text) => Some(
            text.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
        ),
        other => Some(vec![other.py_str()]),
    }
}

/// The connectors (`type:label`, from a `meerschaum:connectors` section) reserved for a team named
/// `team` (any case): what a new team of that name, or one renamed to it, would get.
pub fn reserved_for(connectors: &Map, team: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (typ, labels) in connectors {
        let Some(labels) = labels.as_map() else {
            continue;
        };
        for (label, attrs) in labels {
            let names = attrs
                .as_map()
                .and_then(|a| a.get("teams"))
                .and_then(reservation_names)
                .unwrap_or_default();
            if names.iter().any(|name| same_team(Some(name), Some(team))) {
                out.push(format!("{typ}:{label}"));
            }
        }
    }
    out
}

/// Replace the team `from` (any case) with `to` in every connector's `teams`
/// (`meerschaum:connectors`); the connectors changed (`type:label`). Renaming a team moves its
/// reservations to the new name; deleting one moves them to [`retired_reservation`].
pub fn move_reservations(connectors: &mut Map, from: &str, to: &str) -> Vec<String> {
    let mut changed = Vec::new();
    for (typ, labels) in connectors.iter_mut() {
        let Some(labels) = labels.as_map_mut() else {
            continue;
        };
        for (label, attrs) in labels.iter_mut() {
            let Some(attrs) = attrs.as_map_mut() else {
                continue;
            };
            let Some(names) = attrs.get("teams").and_then(reservation_names) else {
                continue;
            };
            if !names.iter().any(|name| same_team(Some(name), Some(from))) {
                continue;
            }
            let mut moved: Vec<String> = Vec::new();
            for name in names {
                let name = if same_team(Some(&name), Some(from)) {
                    to.to_string()
                } else {
                    name
                };
                if !moved.contains(&name) {
                    moved.push(name);
                }
            }
            attrs.insert(
                "teams".into(),
                Value::List(moved.into_iter().map(Value::Str).collect()),
            );
            changed.push(format!("{typ}:{label}"));
        }
    }
    changed
}

/// Whether a principal may use a connector restricted to these teams (`None`: unrestricted,
/// which a walled-off principal may not use).
pub fn can_use_connector(principal: &Principal, teams: Option<&[String]>) -> bool {
    match teams {
        _ if principal.is_admin && principal.team_boundary.is_none() => true,
        Some(teams) => teams.iter().any(|team| principal.is_member(Some(team))),
        None => !principal.is_isolated(),
    }
}

/// [`can_use_connector`] for the connector `connector_keys`: also when one of the principal's
/// teams was granted it ([`TeamGrants::connectors`]).
pub fn can_use_connector_keys(
    principal: &Principal,
    connector_keys: &str,
    teams: Option<&[String]>,
) -> bool {
    principal.grants.connectors.contains(connector_keys.trim())
        || can_use_connector(principal, teams)
}

/// Whether a principal may give a pipe the source connector `connector_keys`. A pipe's connector
/// keys may be a mere label (data pushed through the API under `plugin:gauge` or `manual:x`):
/// a data plugin (`plugin:<name>`) needs the plugin ([`can_use_plugin`]), a connector the
/// server has (`configured`) follows [`can_use_connector_keys`], and a label that names nothing
/// of the server's is anyone's to use.
pub fn can_name_connector(
    principal: &Principal,
    connector_keys: &str,
    teams: Option<&[String]>,
    configured: bool,
) -> bool {
    let keys = connector_keys.trim();
    if (principal.is_admin && principal.team_boundary.is_none())
        || principal.grants.connectors.contains(keys)
    {
        return true;
    }
    if let Some(plugin) = keys.strip_prefix("plugin:") {
        return can_use_plugin(principal, plugin)
            && teams.is_none_or(|teams| can_use_connector(principal, Some(teams)));
    }
    if !configured && teams.is_none() {
        return true;
    }
    can_use_connector(principal, teams)
}

/// Whether a principal may see and use the plugin `name`: every plugin, unless they are walled
/// off, where it is the plugins their teams were granted or installed.
pub fn can_use_plugin(principal: &Principal, name: &str) -> bool {
    !principal.is_isolated() || principal.grants.plugins.contains(name.trim())
}

/// Whether a principal may install, edit, upload and uninstall plugins on the server:
/// administrators, and the members of a team with `plugins_write`.
pub fn can_write_plugins(principal: &Principal) -> bool {
    (principal.is_admin && principal.team_boundary.is_none()) || principal.grants.plugins_write
}

/// Whether `viewer` may see that `team` exists and who is in it: every team, unless they are
/// walled off, where it is their own teams.
pub fn can_see_team(viewer: &Principal, team: &str) -> bool {
    !viewer.is_isolated() || viewer.is_member(Some(team))
}

/// Whether `viewer` may see the account of a user in `target_user_teams`: every account, unless
/// they are walled off, where it is their own and those of the users sharing one of their teams.
pub fn can_see_user(
    viewer: &Principal,
    target_username: &str,
    target_user_teams: &[(String, TeamRole)],
) -> bool {
    !viewer.is_isolated()
        || viewer.username == target_username
        || target_user_teams
            .iter()
            .any(|(team, _)| !is_everyone(Some(team)) && viewer.is_member(Some(team)))
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
    reservation: &[String],
) -> Result<(), String> {
    if !permits_pipe_team(principal, new.team.as_deref(), reservation) {
        return Err("An execution capability cannot move a pipe outside its team.".into());
    }
    if !can_manage_access(principal, old, reservation) {
        return Err(
            "Only the pipe's owner, its team's managers and administrators may change its access."
                .to_string(),
        );
    }
    if principal.is_admin {
        return Ok(());
    }
    if !same_team(old.team.as_deref(), new.team.as_deref())
        && instance_team_role(principal, new.team.as_deref(), reservation).is_none()
    {
        return Err(format!(
            "You are not a member of the team '{}'.",
            new.team_name()
        ));
    }
    let owner_changed = old.owner != new.owner;
    let gives_up = new.owner.is_none() && old.is_owner(&principal.username);
    if owner_changed
        && !gives_up
        && !can_assign_pipe_owner(principal, old.team.as_deref(), reservation)
    {
        return Err(
            "Only the team's managers and administrators may give a pipe to another owner."
                .to_string(),
        );
    }
    Ok(())
}
