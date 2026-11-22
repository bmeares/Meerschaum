//! Teams: names, membership and roles, the built-in team Everyone, a user's primary team, and an
//! isolated team's settings and the permissions it withholds.

use super::*;

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

/// `attributes['isolated']`: the team's members do not see what belongs to Everyone.
pub const TEAM_ISOLATED_ATTRIBUTE: &str = "isolated";
/// `attributes['instance']`: the team's default instance connector keys.
pub const TEAM_INSTANCE_ATTRIBUTE: &str = "instance";
/// `attributes['connectors']`: the connector keys granted to the team.
pub const TEAM_CONNECTORS_ATTRIBUTE: &str = "connectors";
/// `attributes['plugins']`: the names of the plugins granted to (or installed by) the team.
pub const TEAM_PLUGINS_ATTRIBUTE: &str = "plugins";
/// `attributes['plugins_write']`: the team's members may install, edit and upload plugins.
pub const TEAM_PLUGINS_WRITE_ATTRIBUTE: &str = "plugins_write";
/// `attributes['database']`: the database (or schema) provisioned for the team.
pub const TEAM_DATABASE_ATTRIBUTE: &str = "database";
/// `attributes['grafana']`: the team's Grafana organization.
pub const TEAM_GRAFANA_ATTRIBUTE: &str = "grafana";

fn truthy(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(b)) => *b,
        Some(Value::Str(s)) => matches!(
            s.trim().to_lowercase().as_str(),
            "true" | "yes" | "1" | "on"
        ),
        Some(Value::Int(i)) => *i != 0,
        _ => false,
    }
}

fn name_list(value: Option<&Value>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |text: &str| {
        let text = text.trim();
        if !text.is_empty() && !out.iter().any(|o| o == text) {
            out.push(text.to_string());
        }
    };
    match value {
        Some(Value::List(items)) => {
            for item in items {
                if !item.is_null() {
                    push(&item.py_str());
                }
            }
        }
        Some(Value::Str(text)) => text.split(',').for_each(&mut push),
        _ => {}
    }
    out
}

impl Team {
    /// Whether the team is isolated (`attributes['isolated']`): its members do not see the
    /// pipes, jobs, connectors and plugins of Everyone. The built-in team is never isolated.
    pub fn is_isolated(&self) -> bool {
        !self.is_everyone() && truthy(self.attributes.get(TEAM_ISOLATED_ATTRIBUTE))
    }

    /// The team's default instance connector keys (`attributes['instance']`).
    pub fn default_instance(&self) -> Option<String> {
        self.attributes
            .get(TEAM_INSTANCE_ATTRIBUTE)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|keys| !keys.is_empty())
            .map(str::to_string)
    }

    /// The connector keys granted to the team (`attributes['connectors']`, and its default
    /// instance).
    pub fn granted_connectors(&self) -> Vec<String> {
        let mut out = name_list(self.attributes.get(TEAM_CONNECTORS_ATTRIBUTE));
        if let Some(instance) = self.default_instance()
            && !out.contains(&instance)
        {
            out.insert(0, instance);
        }
        out
    }

    /// The plugins granted to (or installed by) the team (`attributes['plugins']`).
    pub fn granted_plugins(&self) -> Vec<String> {
        name_list(self.attributes.get(TEAM_PLUGINS_ATTRIBUTE))
    }

    /// Whether the team's members may install, edit and upload plugins
    /// (`attributes['plugins_write']`).
    pub fn may_write_plugins(&self) -> bool {
        truthy(self.attributes.get(TEAM_PLUGINS_WRITE_ATTRIBUTE))
    }

    /// Replace the list attribute `key` (`connectors`, `plugins`) with `names` (an empty list
    /// removes the key).
    pub fn set_names(&mut self, key: &str, names: &[String]) {
        let names = name_list(Some(&Value::List(
            names.iter().map(|n| Value::Str(n.clone())).collect(),
        )));
        if names.is_empty() {
            self.attributes.shift_remove(key);
        } else {
            self.attributes.insert(
                key.to_string(),
                Value::List(names.into_iter().map(Value::Str).collect()),
            );
        }
    }
}

/// The permissions a walled-off principal does not hold, whatever their role: running arbitrary
/// actions, the web terminal and chaining instances act with the server's own credentials,
/// outside the team's wall. Syncing and verifying the pipes they may edit needs none of them.
/// A member of a team that runs code ([`TeamGrants::runs_code`]) keeps
/// [`TEAM_EXECUTION_PERMISSIONS`] (5.1): the server then runs their commands and terminal only
/// as the team's own sandboxed processes, never with its credentials. `instance:chain` is always
/// withheld ([`withheld_permissions`]).
pub const ISOLATED_WITHHELD: [&str; 3] = [
    Permission::ACTIONS_EXECUTE,
    Permission::WEBTERM_USE,
    Permission::INSTANCE_CHAIN,
];

/// The withheld permissions a walled-off member of a team that runs code keeps (5.1): what lets
/// them run commands and open the terminal, which the API server runs only as their team's
/// sandboxed processes (team execution) and refuses where it cannot.
pub const TEAM_EXECUTION_PERMISSIONS: [&str; 2] =
    [Permission::ACTIONS_EXECUTE, Permission::WEBTERM_USE];

/// The permissions a principal with `grants` does not hold ([`ISOLATED_WITHHELD`] for a
/// walled-off principal, less [`TEAM_EXECUTION_PERMISSIONS`] when their team runs code; nothing
/// otherwise).
pub fn withheld_permissions(grants: &TeamGrants) -> Vec<&'static str> {
    if !grants.isolated {
        return Vec::new();
    }
    ISOLATED_WITHHELD
        .iter()
        .copied()
        .filter(|p| !(grants.runs_code && TEAM_EXECUTION_PERMISSIONS.contains(p)))
        .collect()
}

/// What a principal's teams grant and withhold: computed from the attributes of the teams the
/// user is in ([`TeamGrants::of`]). The default grants nothing extra and withholds nothing, which
/// is how every user behaved before 5.1.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamGrants {
    /// The principal is walled off: they are in at least one team other than Everyone and every
    /// such team is isolated. They are then not a member of Everyone: they see only what belongs
    /// to their teams and what their teams were granted.
    #[serde(default)]
    pub isolated: bool,
    /// The connector keys their teams were granted.
    #[serde(default)]
    pub connectors: BTreeSet<String>,
    /// The plugins their teams were granted (or installed).
    #[serde(default)]
    pub plugins: BTreeSet<String>,
    /// One of their teams lets its members install, edit and upload plugins.
    #[serde(default)]
    pub plugins_write: bool,
    /// The default instance of their primary team (else of the first of their teams with one).
    #[serde(default)]
    pub instance: Option<String>,
    /// 5.1: the principal is walled off and the team they act for (their primary team, else the
    /// first of their teams) lets its members install, edit and upload plugins: its members can
    /// already run code in the team's processes, so they may run commands there too (team
    /// execution, decided by the API server for each request).
    #[serde(default)]
    pub runs_code: bool,
}

impl TeamGrants {
    /// The grants of a user in `user_teams` (their teams with roles), given the instance's
    /// `teams` records; `primary` is the user's primary team ([`primary_team`]).
    pub fn of(user_teams: &[(String, TeamRole)], teams: &[Team], primary: Option<&str>) -> Self {
        let mut grants = TeamGrants::default();
        let own: Vec<&Team> = teams
            .iter()
            .filter(|team| !team.is_everyone())
            .filter(|team| {
                user_teams
                    .iter()
                    .any(|(name, _)| same_team(Some(name), Some(&team.name)))
            })
            .collect();
        grants.isolated = !own.is_empty() && own.iter().all(|team| team.is_isolated());
        for team in &own {
            grants.connectors.extend(team.granted_connectors());
            grants.plugins.extend(team.granted_plugins());
            grants.plugins_write |= team.may_write_plugins();
        }
        grants.instance = own
            .iter()
            .find(|team| same_team(Some(&team.name), primary))
            .and_then(|team| team.default_instance())
            .or_else(|| own.iter().find_map(|team| team.default_instance()));
        // The team new work runs for (`team_scope::acting_team` without an object).
        grants.runs_code = grants.isolated
            && own
                .iter()
                .find(|team| same_team(Some(&team.name), primary))
                .or_else(|| own.first())
                .is_some_and(|team| team.may_write_plugins());
        grants
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
