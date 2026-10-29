//! `register users` (`actions/register.py`), `edit users` (`actions/edit.py`), `delete users`
//! (`actions/delete.py`) and `show users`, with the access model of rc15 wave 2 (AC1, AC2, AC10):
//!
//! - a user's permissions come from their role (`viewer`, `editor`, `operator`; the instance's
//!   `api:permissions:default_role` when none is stored), explicit permissions (a custom list,
//!   `attributes['scopes']`) or the user type `admin` (every permission);
//! - `register users` gives new users the instance's default role (`--scopes` gives explicit
//!   permissions), and offers to make the first user of an empty instance an admin;
//! - `edit users` changes the password, type, role (or explicit permissions), primary team and
//!   email;
//! - `show users` prints a table: User, Type, Role, Teams, Email.
//!
//! Locally the instance's credentials are the trust boundary: these actions see and change every
//! user. On an `api:` instance the server's rules apply (other users' emails, for example, only
//! for administrators and the managers of a team the user is in).

use meerschaum_core::SuccessTuple;
use meerschaum_core::access::{
    self, ALL_PERMISSIONS, ALL_SCOPE, EVERYONE, PRIMARY_TEAM_ATTRIBUTE, ROLE_ATTRIBUTE, Role,
    SCOPES_ATTRIBUTE, TeamRole,
};
use meerschaum_core::static_config;
use meerschaum_core::users::User;
use meerschaum_core::value::{Map, Value};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::format::rich::console::Renderable;
use crate::format::rich::table::{Column, ROUNDED, Table};
use crate::format::rich::text::Text;
use crate::value::ArgsMap;

use super::ask::{Ask, ask, choose};
use super::teams::{TeamInfo, get_teams};
use super::{
    Instance, check_noask, flag, info, plural, print_tuple, prompt, session, string, warn, words,
    yes_no,
};

/// A password question: `*` per character on a terminal, nothing echoed otherwise (`None` when
/// input ends or on Ctrl-C).
fn secret_prompt(rt: &mut Runtime<'_>, question: &str) -> Option<String> {
    ask(rt, question, &Ask::new().password()).ok()
}

/// `get_password(username, minimum_length)`; `None` when input ends (Python's `EOFError`).
fn get_password(rt: &mut Runtime<'_>, username: &str, min_len: usize) -> Option<String> {
    // PARITY: with `--noask` v4 loops forever on the empty password; treat it as aborted.
    if check_noask(rt, false) {
        return None;
    }
    loop {
        let password = secret_prompt(rt, &format!("Password for user '{username}':"))?;
        if password.chars().count() < min_len {
            warn(
                rt,
                &format!(
                    "Password is too short. Please enter a password that is at least {min_len} characters."
                ),
            );
            continue;
        }
        let confirm = secret_prompt(rt, &format!("Confirm password for user '{username}':"))?;
        if password != confirm {
            warn(rt, "Passwords do not match! Please try again.");
            continue;
        }
        return Some(password);
    }
}

/// `is_valid_email(email)`.
fn is_valid_email(email: &str) -> bool {
    let re = regex::Regex::new(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$").expect("re");
    re.is_match(email)
}

/// `get_email(username, allow_omit=True)`.
fn get_email(rt: &mut Runtime<'_>, username: &str, allow_omit: bool) -> Option<String> {
    loop {
        let q = format!(
            "Email for user '{username}'{}",
            if allow_omit {
                " (empty to omit):"
            } else {
                ": "
            }
        );
        let email = prompt(rt, &q, true, false)?;
        if (allow_omit && email.is_empty()) || is_valid_email(&email) {
            return Some(email);
        }
        warn(rt, "Invalid email! Please try again.");
    }
}

/// The instance's default role (`api:permissions:default_role`, else v4's
/// `api:permissions:actions:non_admin: true` as `operator`, else `editor`).
fn default_role(rt: &mut Runtime<'_>) -> Role {
    access::default_role_from_config(rt.config())
}

/// The names in `permissions` that are no known permission (custom scopes of plugins stay
/// allowed; this only warns).
fn unknown_permissions(permissions: &[String]) -> Vec<String> {
    permissions
        .iter()
        .filter(|p| p.as_str() != ALL_SCOPE && !ALL_PERMISSIONS.contains(&p.as_str()))
        .cloned()
        .collect()
}

/// A user's teams (Everyone left out) from the instance's teams, with their roles.
fn teams_of(teams: &[TeamInfo], username: &str) -> Vec<(String, TeamRole)> {
    teams
        .iter()
        .filter(|t| !t.is_everyone())
        .filter_map(|t| {
            if t.managers.iter().any(|m| m == username) {
                Some((t.name.clone(), TeamRole::Manager))
            } else if t.members.iter().any(|m| m == username) {
                Some((t.name.clone(), TeamRole::Member))
            } else {
                None
            }
        })
        .collect()
}

/// The role a user's attributes give, and whether it is the instance's default (no stored role
/// and no explicit permissions).
fn role_of(user_type: Option<&str>, attributes: Option<&Map>, default: Role) -> (Role, bool) {
    let role = access::user_role(user_type, attributes, default);
    let follows_default = role != Role::Admin
        && access::stored_role(attributes).is_none()
        && access::explicit_scopes(attributes).is_none();
    (role, follows_default)
}

/// How a role reads in prompts and the table: `Editor`, `Editor (default)`.
fn role_label(role: Role, follows_default: bool) -> String {
    if follows_default {
        format!("{} (default)", role.label())
    } else {
        role.label().to_string()
    }
}

/// `register users <username> ... [--scopes ...]`: new users get the instance's default role
/// (`--scopes` gives them explicit permissions instead). The first user of an instance without
/// users may become its administrator.
pub fn register_users(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let action = words(args, "action");
    if action.is_empty() {
        return Ok(SuccessTuple::fail("No users to register."));
    }
    let (yes, noask) = (flag(args, "yes"), flag(args, "noask"));
    let session = session(rt);
    let conn = inst.conn();
    let users_cfg = &static_config().users;
    let min_len = users_cfg.min_username_length.max(0) as usize;
    let mut nonregistered: Vec<String> = Vec::new();
    for username in &action {
        if username.chars().count() < min_len {
            warn(
                rt,
                &format!(
                    "Username '{username}' is too short (less than {min_len} characters). Skipping..."
                ),
            );
            continue;
        }
        let user = User::new(username, None);
        if let Ok(Some(_)) = session.block_on(conn.get_user_id(&user)) {
            warn(
                rt,
                &format!("User '{username}' already exists. Skipping..."),
            );
            continue;
        }
        nonregistered.push(username.clone());
    }
    let scopes = words(args, "scopes");
    let unknown = unknown_permissions(&scopes);
    if !unknown.is_empty() {
        warn(
            rt,
            &format!(
                "Not a built-in permission: {} (kept as custom permissions).",
                unknown.join(", ")
            ),
        );
    }
    // AC1: the first account of an instance without users may become its administrator.
    let mut first_admin = !nonregistered.is_empty()
        && session
            .block_on(conn.get_users())
            .is_ok_and(|users| users.is_empty());
    let mut results: Vec<(String, bool)> = Vec::new();
    for username in &nonregistered {
        let make_admin = first_admin
            && yes_no(
                rt,
                &format!(
                    "No users are registered on instance '{}' yet. Make '{username}' an admin?",
                    inst.keys()
                ),
                "y",
                yes,
                noask,
            )?;
        first_admin = false;
        let min_pw = users_cfg.min_password_length.max(0) as usize;
        let creds = get_password(rt, username, min_pw)
            .and_then(|p| get_email(rt, username, true).map(|e| (p, e)));
        let Some((password, email)) = creds else {
            let remaining: Vec<&String> = nonregistered
                .iter()
                .filter(|u| !results.iter().any(|(r, ok)| r == *u && *ok))
                .collect();
            return Ok(SuccessTuple::fail(format!(
                "Aborted registering users {}",
                remaining
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        };
        let mut user = User::new(username, Some(&password));
        user.email = (!email.is_empty()).then_some(email);
        if make_admin {
            user.user_type = Some(access::ADMIN_USER_TYPE.into());
        }
        if !scopes.is_empty() {
            user.attributes = Some(access::attributes_with_permissions(&Map::new(), &scopes));
        }
        user.instance_keys = Some(inst.keys());
        info(
            rt,
            &format!(
                "Registering user '{username}' to Meerschaum instance '{}'...",
                inst.keys()
            ),
        );
        let res = session.block_on(conn.register_user(&mut user));
        print_tuple(rt, &res, TupleArgs::default());
        results.push((username.clone(), res.success));
    }
    let succeeded = results.iter().filter(|r| r.1).count();
    let failed = results.len() - succeeded;
    Ok(SuccessTuple {
        success: succeeded > 0,
        message: format!(
            "Finished registering {} users.\n    ({succeeded} succeeded, {failed} failed)",
            succeeded + failed
        ),
    })
}

/// The role choices of `edit users`: the presets, explicit permissions, and the instance's
/// default.
fn role_choices(default: Role) -> Vec<(String, Option<Role>)> {
    vec![
        ("Viewer".into(), Some(Role::Viewer)),
        ("Editor".into(), Some(Role::Editor)),
        ("Operator".into(), Some(Role::Operator)),
        ("Custom permissions".into(), Some(Role::Custom)),
        (
            format!("The instance's default ({})", default.label()),
            None,
        ),
    ]
}

/// Ask for explicit permissions (space-separated; `*` is every permission).
fn ask_permissions(
    rt: &mut Runtime<'_>,
    username: &str,
    current: &[String],
) -> Result<Vec<String>, ActionError> {
    let answer = ask(
        rt,
        &format!(
            "Permissions for user '{username}' (space-separated; `*` grants every permission, also plugins' and future ones):"
        ),
        &Ask::new().editable(current.join(" ")),
    )?;
    let permissions: Vec<String> = answer.split_whitespace().map(str::to_string).collect();
    let unknown = unknown_permissions(&permissions);
    if !unknown.is_empty() {
        warn(
            rt,
            &format!(
                "Not a built-in permission: {} (kept as custom permissions).",
                unknown.join(", ")
            ),
        );
    }
    Ok(permissions)
}

/// `attributes` following the instance's default role: no explicit permissions, and the stored
/// role cleared (`null`, so the update is never an empty one that would leave the old role).
fn attributes_with_default_role(attributes: &Map) -> Map {
    let mut out = attributes.clone();
    out.shift_remove(SCOPES_ATTRIBUTE);
    out.insert(ROLE_ATTRIBUTE.into(), Value::Null);
    out
}

/// `edit users <username> ...`: asks whether to change each of the password, the user type, the
/// role (or explicit permissions), the primary team and the email.
pub fn edit_users(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let action = words(args, "action");
    let yes = flag(args, "yes");
    let noask = flag(args, "noask");
    if action.is_empty() {
        return Ok(SuccessTuple::fail("No users to edit."));
    }
    let session = session(rt);
    let conn = inst.conn();
    let min_pw = static_config().users.min_password_length.max(0) as usize;
    let default = default_role(rt);
    let teams = session.block_on(get_teams(&inst)).unwrap_or_default();
    let mut results: Vec<bool> = Vec::new();
    for username in &action {
        let lookup = User::new(username, None);
        let user_id = match session.block_on(conn.get_user_id(&lookup)) {
            Ok(Some(id)) => id,
            Ok(None) => {
                warn(
                    rt,
                    &format!(
                        "User '{username}' is not registered on instance '{}'. Skipping...",
                        inst.keys()
                    ),
                );
                results.push(false);
                continue;
            }
            Err(e) => {
                warn(rt, &format!("Failed to look up user '{username}': {e}"));
                results.push(false);
                continue;
            }
        };
        let with_id = User {
            user_id: Some(user_id.clone()),
            ..lookup.clone()
        };
        let current_type = session
            .block_on(conn.get_user_type(&with_id))
            .ok()
            .flatten()
            .filter(|t| !t.trim().is_empty() && t != "None");
        let current_attrs: Map = session
            .block_on(conn.get_user_attributes(&with_id))
            .ok()
            .flatten()
            .unwrap_or_default();

        // The password.
        let mut password = String::new();
        if yes_no(
            rt,
            &format!("Change the password for user '{username}'?"),
            "n",
            yes,
            noask,
        )? {
            match get_password(rt, username, min_pw) {
                Some(p) => password = p,
                None => {
                    info(rt, &format!("Skipping editing user '{username}'..."));
                    continue;
                }
            }
        }

        // The user type (`Some("")` keeps it).
        let mut user_type: Option<String> = Some(String::new());
        let mut is_admin = access::is_admin_type(current_type.as_deref());
        if yes_no(
            rt,
            &format!(
                "Change the user type for user '{username}' (now: {})?",
                if is_admin { "admin" } else { "regular" }
            ),
            "n",
            yes,
            noask,
        )? {
            is_admin = yes_no(
                rt,
                &format!("Make user '{username}' an admin?"),
                if is_admin { "y" } else { "n" },
                yes,
                noask,
            )?;
            user_type = is_admin.then(|| access::ADMIN_USER_TYPE.to_string());
        }

        // The role, or explicit permissions (admins hold every permission).
        let mut attrs = current_attrs.clone();
        if !is_admin {
            let (role, follows_default) = role_of(None, Some(&current_attrs), default);
            if yes_no(
                rt,
                &format!(
                    "Change the role for user '{username}' (now: {})?",
                    role_label(role, follows_default)
                ),
                "n",
                yes,
                noask,
            )? {
                let choices = role_choices(default);
                let labels: Vec<String> = choices.iter().map(|(l, _)| l.clone()).collect();
                let current_label = if follows_default {
                    labels.last().cloned()
                } else {
                    choices
                        .iter()
                        .find(|(_, r)| *r == Some(role))
                        .map(|(l, _)| l.clone())
                };
                let chosen = choose(
                    rt,
                    &format!("Role for user '{username}':"),
                    &labels,
                    current_label.as_deref(),
                    true,
                )?;
                match chosen.and_then(|c| choices.into_iter().find(|(l, _)| *l == c)) {
                    Some((_, Some(Role::Custom))) => {
                        let current: Vec<String> = access::explicit_scopes(Some(&current_attrs))
                            .unwrap_or_else(|| access::ordered_permissions(&role.permissions()));
                        let permissions = ask_permissions(rt, username, &current)?;
                        attrs = if permissions.is_empty() {
                            attributes_with_default_role(&attrs)
                        } else {
                            access::attributes_with_permissions(&attrs, &permissions)
                        };
                    }
                    Some((_, Some(preset))) => attrs = access::attributes_with_role(&attrs, preset),
                    Some((_, None)) => attrs = attributes_with_default_role(&attrs),
                    None => {}
                }
            }
        }

        // The primary team (the default team of the pipes the user creates).
        let user_teams = teams_of(&teams, username);
        if !user_teams.is_empty() {
            let primary = access::primary_team(Some(&attrs), &user_teams);
            if yes_no(
                rt,
                &format!("Change the primary team for user '{username}' (now: {primary})?"),
                "n",
                yes,
                noask,
            )? {
                let mut names: Vec<String> = user_teams.iter().map(|(n, _)| n.clone()).collect();
                names.push(EVERYONE.to_string());
                if let Some(chosen) = choose(
                    rt,
                    &format!("Primary team for user '{username}':"),
                    &names,
                    Some(&primary),
                    true,
                )? {
                    attrs.insert(PRIMARY_TEAM_ATTRIBUTE.into(), Value::Str(chosen));
                }
            }
        }

        // The email (`Some("")` keeps it).
        let mut email = String::new();
        if yes_no(
            rt,
            &format!("Change the email for user '{username}'?"),
            "n",
            yes,
            noask,
        )? {
            email = get_email(rt, username, false).unwrap_or_default();
        }

        let mut user = User::new(username, Some(&password));
        user.user_id = Some(user_id);
        user.email = Some(email);
        user.user_type = user_type;
        user.attributes = Some(attrs);
        info(
            rt,
            &format!(
                "Editing user '{username}' on Meerschaum instance '{}'...",
                inst.keys()
            ),
        );
        let res = session.block_on(conn.edit_user(&user));
        print_tuple(rt, &res, TupleArgs::default());
        results.push(res.success);
    }
    let succeeded = results.iter().filter(|r| **r).count();
    let failed = results.len() - succeeded;
    let msg = format!(
        "Finished editing {} users\n    ({succeeded} succeeded, {failed} failed).",
        action.len()
    );
    info(rt, &msg);
    Ok(SuccessTuple {
        success: succeeded > 0,
        message: msg,
    })
}

/// How many of the instance's pipes (and saved Pipelines) each user owns.
fn owned_pipes(rt: &mut Runtime<'_>, inst: &Instance, usernames: &[String]) -> Vec<usize> {
    let session = session(rt);
    let conn = inst.conn();
    usernames
        .iter()
        .map(|username| {
            session
                .block_on(meerschaum_connector::teams::owned_pipes(
                    conn.as_ref(),
                    username,
                ))
                .map(|pipes| pipes.len())
                .unwrap_or(0)
        })
        .collect()
}

/// `delete users <username> ...`. The pipes a deleted user owned stay with their teams, without
/// an owner.
pub fn delete_users(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let action = words(args, "action");
    let session = session(rt);
    let conn = inst.conn();
    let mut registered: Vec<User> = Vec::new();
    for username in &action {
        let mut user = User::new(username, None);
        match session.block_on(conn.get_user_id(&user)) {
            Ok(Some(id)) => {
                user.user_id = Some(id);
                registered.push(user);
            }
            _ => info(
                rt,
                &format!("User '{username}' does not exist. Skipping..."),
            ),
        }
    }
    if registered.is_empty() {
        return Ok(SuccessTuple::fail("No users to delete."));
    }
    let usernames: Vec<String> = registered.iter().map(|u| u.username.clone()).collect();
    let owned = owned_pipes(rt, &inst, &usernames);
    let mut question = format!(
        "Are you sure you want to delete these users from Meerschaum instance '{}'?\n",
        inst.keys()
    );
    for (u, n) in registered.iter().zip(&owned) {
        if *n > 0 {
            question.push_str(&format!(
                " - {} (owns {n} pipe{}; they stay with their teams)\n",
                u.username,
                plural(*n)
            ));
        } else {
            question.push_str(&format!(" - {}\n", u.username));
        }
    }
    let answer =
        flag(args, "force") || yes_no(rt, &question, "n", flag(args, "yes"), flag(args, "noask"))?;
    if !answer {
        return Ok(SuccessTuple::fail("No users deleted."));
    }
    // PARITY: v4 keys its results by the *last* username of the question loop, so at most one
    // result is counted (`success[username] = ...`).
    let mut last: Option<bool> = None;
    for user in &registered {
        info(
            rt,
            &format!(
                "Deleting user '{}' from Meerschaum instance '{}'...",
                user.username,
                inst.keys()
            ),
        );
        let res = session.block_on(conn.delete_user(user));
        print_tuple(rt, &res, TupleArgs::default());
        last = Some(res.success);
    }
    let (succeeded, failed) = match last {
        Some(true) => (1, 0),
        Some(false) => (0, 1),
        None => (0, 0),
    };
    Ok(SuccessTuple::ok(format!(
        "Finished deleting {} users.\n    ({succeeded} succeeded, {failed} failed)",
        action.len()
    )))
}

/// One row of `show users`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserRow {
    /// The username.
    pub username: String,
    /// `admin` or `regular` (or another stored type).
    pub user_type: String,
    /// The role as it reads (`Editor`, `Editor (default)`, `Admin`, `Custom`).
    pub role: String,
    /// The teams other than Everyone, managers marked (`analytics (manager), research`).
    pub teams: String,
    /// The email address (empty when unset or not visible).
    pub email: String,
    /// The role's name (`editor`, `admin`, `custom`).
    pub role_name: String,
    /// Whether the role is the instance's default (no role of their own).
    pub default_role: bool,
    /// The teams other than Everyone, with the user's role in each.
    pub team_roles: Vec<(String, TeamRole)>,
}

impl UserRow {
    /// The `--nopretty` object: `{"username", "type", "role", "default_role", "teams":
    /// [{"name", "role"}], "email"}`.
    pub fn to_value(&self) -> Value {
        let mut map = Map::new();
        map.insert("username".into(), Value::Str(self.username.clone()));
        map.insert("type".into(), Value::Str(self.user_type.clone()));
        map.insert("role".into(), Value::Str(self.role_name.clone()));
        map.insert("default_role".into(), Value::Bool(self.default_role));
        map.insert(
            "teams".into(),
            Value::List(
                self.team_roles
                    .iter()
                    .map(|(name, role)| {
                        let mut team = Map::new();
                        team.insert("name".into(), Value::Str(name.clone()));
                        team.insert("role".into(), Value::Str(role.name().into()));
                        Value::Map(team)
                    })
                    .collect(),
            ),
        );
        map.insert(
            "email".into(),
            if self.email.is_empty() {
                Value::Null
            } else {
                Value::Str(self.email.clone())
            },
        );
        Value::Map(map)
    }
}

/// The table row of a user. `server_role` is the role an `api:` instance's server reports (its
/// default role is its own); otherwise the role follows from the attributes and `default`.
pub fn user_row(
    username: &str,
    user_type: Option<&str>,
    attributes: Option<&Map>,
    teams: &[(String, TeamRole)],
    email: Option<&str>,
    default: Role,
    server_role: Option<Role>,
) -> UserRow {
    let (local_role, follows_default) = role_of(user_type, attributes, default);
    let role = server_role.unwrap_or(local_role);
    let follows_default = follows_default && role != Role::Admin;
    let user_type = match user_type
        .map(str::trim)
        .filter(|t| !t.is_empty() && *t != "None")
    {
        Some(t) if access::is_admin_type(Some(t)) => access::ADMIN_USER_TYPE.to_string(),
        Some(t) => t.to_string(),
        None => "regular".to_string(),
    };
    let teams_text = teams
        .iter()
        .map(|(name, role)| match role {
            TeamRole::Manager => format!("{name} (manager)"),
            TeamRole::Member => name.clone(),
        })
        .collect::<Vec<_>>()
        .join(", ");
    UserRow {
        username: username.to_string(),
        user_type,
        role: role_label(role, follows_default),
        teams: teams_text,
        email: email.unwrap_or_default().to_string(),
        role_name: role.name().to_string(),
        default_role: follows_default,
        team_roles: teams.to_vec(),
    }
}

/// `show users`: a table of the instance's users by name (User, Type, Role, Teams, Email). With
/// `--nopretty`, one JSON object per user (`username`, `type`, `role`, `teams`, `email`).
pub fn show_users(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let nopretty = flag(args, "nopretty");
    let session = session(rt);
    let conn = inst.conn();
    let users = match session.block_on(conn.get_users()) {
        Ok(mut u) => {
            u.sort_by_key(|name| (name.to_lowercase(), name.clone()));
            u
        }
        Err(_) => {
            return Ok(SuccessTuple::fail(format!(
                "Failed to get users from instance '{}'",
                string(args, "mrsm_instance").unwrap_or_else(|| "None".into())
            )));
        }
    };
    if users.is_empty() {
        if !nopretty {
            info(
                rt,
                &format!("No users are registered on instance '{}'.", inst.keys()),
            );
        }
        return Ok(SuccessTuple::success());
    }
    let default = default_role(rt);
    let teams = session.block_on(get_teams(&inst)).unwrap_or_default();
    let mut rows: Vec<UserRow> = Vec::new();
    for username in &users {
        let lookup = User::new(username, None);
        let user = match session.block_on(conn.get_user_id(&lookup)) {
            Ok(Some(id)) => User {
                user_id: Some(id),
                ..lookup
            },
            _ => lookup,
        };
        let user_type = session.block_on(conn.get_user_type(&user)).ok().flatten();
        let attributes = session
            .block_on(conn.get_user_attributes(&user))
            .ok()
            .flatten();
        let email = session.block_on(conn.get_user_email(&user)).ok().flatten();
        let server_role = if inst.type_name() == "api" {
            session.block_on(conn.get_user_role(&user)).ok()
        } else {
            None
        };
        rows.push(user_row(
            username,
            user_type.as_deref(),
            attributes.as_ref(),
            &teams_of(&teams, username),
            email.as_deref(),
            default,
            server_role,
        ));
    }
    if nopretty {
        for row in &rows {
            let line = meerschaum_core::pyfmt::json_dumps(
                &row.to_value(),
                meerschaum_core::pyfmt::JsonStyle::Default,
                true,
            );
            rt.print(&format!("{line}\n"));
        }
        return Ok(SuccessTuple::success());
    }
    let title = format!("Registered users for instance '{}'", inst.keys());
    let text = rt.with_formatter(|f| {
        let mut table = Table::new();
        table.box_chars = Some(ROUNDED);
        table.title = Some(Text::new(&title, "bold"));
        for header in ["User", "Type", "Role", "Teams", "Email"] {
            table.add_column(Column {
                header: Text::plain_text(header),
                ..Column::default()
            });
        }
        for row in &rows {
            let cell = |s: &str| Some(Box::new(Text::plain_text(s)) as Box<dyn Renderable>);
            table.add_row(vec![
                cell(&row.username),
                cell(&row.user_type),
                cell(&row.role),
                cell(&row.teams),
                cell(&row.email),
            ]);
        }
        f.console().print(&table)
    });
    rt.print(&text);
    Ok(SuccessTuple::success())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(pairs: &[(&str, Value)]) -> Map {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    fn list(items: &[&str]) -> Value {
        Value::List(items.iter().map(|s| Value::Str(s.to_string())).collect())
    }

    #[test]
    fn rows_name_the_role_and_whether_it_is_the_default() {
        let teams = vec![
            ("analytics".to_string(), TeamRole::Manager),
            ("research".to_string(), TeamRole::Member),
        ];
        let row = user_row(
            "alice",
            None,
            None,
            &teams,
            Some("a@x.io"),
            Role::Editor,
            None,
        );
        assert_eq!(
            (
                row.username.as_str(),
                row.user_type.as_str(),
                row.role.as_str(),
                row.teams.as_str(),
                row.email.as_str()
            ),
            (
                "alice",
                "regular",
                "Editor (default)",
                "analytics (manager), research",
                "a@x.io"
            )
        );
        assert_eq!(
            meerschaum_core::pyfmt::json_dumps(
                &row.to_value(),
                meerschaum_core::pyfmt::JsonStyle::Default,
                true
            ),
            r#"{"username": "alice", "type": "regular", "role": "editor", "default_role": true, "teams": [{"name": "analytics", "role": "manager"}, {"name": "research", "role": "member"}], "email": "a@x.io"}"#
        );
        let admin = user_row("root", Some("admin"), None, &[], None, Role::Editor, None);
        assert_eq!(
            (admin.user_type.as_str(), admin.role.as_str()),
            ("admin", "Admin")
        );
        let viewer = attrs(&[("role", Value::Str("viewer".into()))]);
        assert_eq!(
            user_row("v", None, Some(&viewer), &[], None, Role::Editor, None).role,
            "Viewer"
        );
        let custom = attrs(&[("scopes", list(&["pipes:read", "actions:execute"]))]);
        assert_eq!(
            user_row("c", None, Some(&custom), &[], None, Role::Editor, None).role,
            "Custom"
        );
        // A `null` role (the instance's default chosen in `edit users`) follows the default.
        let default = attributes_with_default_role(&custom);
        assert!(!default.contains_key("scopes"));
        assert_eq!(
            user_row(
                "d",
                Some("None"),
                Some(&default),
                &[],
                None,
                Role::Operator,
                None
            )
            .role,
            "Operator (default)"
        );
        // An `api:` instance's server decides the role (its default role is its own).
        assert_eq!(
            user_row("e", None, None, &[], None, Role::Editor, Some(Role::Viewer)).role,
            "Viewer (default)"
        );
    }

    #[test]
    fn team_membership_reads_managers_first() {
        let teams = vec![
            TeamInfo {
                name: EVERYONE.into(),
                managers: vec!["carol".into()],
                members: vec!["carol".into()],
                ..TeamInfo::default()
            },
            TeamInfo {
                name: "analytics".into(),
                managers: vec!["carol".into()],
                members: vec!["alice".into(), "carol".into()],
                ..TeamInfo::default()
            },
        ];
        assert_eq!(
            teams_of(&teams, "carol"),
            vec![("analytics".to_string(), TeamRole::Manager)]
        );
        assert_eq!(
            teams_of(&teams, "alice"),
            vec![("analytics".to_string(), TeamRole::Member)]
        );
        assert!(teams_of(&teams, "bob").is_empty());
    }

    #[test]
    fn unknown_permissions_are_reported() {
        let given: Vec<String> = ["pipes:read", "*", "weather:forecast"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(unknown_permissions(&given), vec!["weather:forecast"]);
    }
}
