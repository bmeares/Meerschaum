//! `show teams`, `register teams`, `edit teams` and `delete teams` (rc15 wave 2, AC3 / AC10).
//!
//! Every user is a member of the built-in team **Everyone**, which cannot be deleted, renamed or
//! left (its record may name managers). Other teams have members and managers (a manager is a
//! member who also adds and removes members and manages the access of the team's pipes).
//! Renaming a team renames it in the pipes and jobs that name it; deleting one moves them to
//! Everyone. The teams live on the instance like its users (an internal `mrsm` pipe).
//!
//! Locally the instance's credentials are the trust boundary: these actions change any team.
//! On an `api:` instance the server decides who may.

use meerschaum_core::SuccessTuple;
use meerschaum_core::access::{EVERYONE, Team, TeamRole, is_everyone as everyone_name};
use meerschaum_core::value::{Map, Value};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::format::rich::console::Renderable;
use crate::format::rich::table::{Column, ROUNDED, Table};
use crate::format::rich::text::Text;
use crate::value::ArgsMap;

use super::{Instance, flag, info, plural, print_tuple, session, string, warn, words, yes_no};

/// A team as these actions read and write it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TeamInfo {
    /// The team's unique name.
    pub name: String,
    /// The members (managers included), sorted.
    pub members: Vec<String>,
    /// The managers, sorted.
    pub managers: Vec<String>,
    /// The stored record's id (kept on edits).
    pub id: Option<String>,
    /// The stored record's attributes (kept on edits).
    pub attributes: Map,
}

impl TeamInfo {
    fn everyone() -> TeamInfo {
        TeamInfo {
            name: EVERYONE.into(),
            ..TeamInfo::default()
        }
    }

    /// Whether this is the built-in team.
    pub fn is_everyone(&self) -> bool {
        is_everyone(&self.name)
    }

    /// The members who are not managers.
    fn plain_members(&self) -> Vec<String> {
        self.members
            .iter()
            .filter(|m| !self.managers.contains(m))
            .cloned()
            .collect()
    }
}

/// Whether a name is the built-in team's (never an empty one: that is no team at all here).
pub fn is_everyone(name: &str) -> bool {
    !name.trim().is_empty() && everyone_name(Some(name))
}

/// Members and managers as stored: every manager is a member; both sorted, each name once.
fn normalized(mut team: TeamInfo) -> TeamInfo {
    let clean = |names: &mut Vec<String>| {
        for name in names.iter_mut() {
            *name = name.trim().to_string();
        }
        names.retain(|n| !n.is_empty());
        names.sort();
        names.dedup();
    };
    clean(&mut team.managers);
    team.members.extend(team.managers.iter().cloned());
    clean(&mut team.members);
    team
}

// ------------------------------------------------------------------------------------------
// The instance's team store (`InstanceConnector` team methods).
// ------------------------------------------------------------------------------------------

fn from_core(team: Team) -> TeamInfo {
    normalized(TeamInfo {
        members: team.member_names(),
        managers: team.managers(),
        name: team.name,
        id: team.id,
        attributes: team.attributes,
    })
}

fn to_core(team: &TeamInfo) -> Team {
    let mut core = if team.is_everyone() {
        Team::everyone()
    } else {
        Team::new(&team.name)
    };
    if team.id.is_some() {
        core.id = team.id.clone();
    }
    core.attributes = team.attributes.clone();
    for member in team.plain_members() {
        core.add_member(&member, TeamRole::Member);
    }
    for manager in &team.managers {
        core.add_member(manager, TeamRole::Manager);
    }
    core
}

/// Every team registered on the instance (Everyone included when its record exists).
pub async fn get_teams(inst: &Instance) -> Result<Vec<TeamInfo>, String> {
    inst.conn()
        .get_teams()
        .await
        .map(|teams| teams.into_iter().map(from_core).collect())
        .map_err(|e| e.to_string())
}

/// One team, by name.
async fn get_team(inst: &Instance, name: &str) -> Result<Option<TeamInfo>, String> {
    inst.conn()
        .get_team(name)
        .await
        .map(|team| team.map(from_core))
        .map_err(|e| e.to_string())
}

async fn register_team(inst: &Instance, team: &TeamInfo) -> SuccessTuple {
    let mut core = to_core(team);
    inst.conn().register_team(&mut core).await
}

async fn edit_team(inst: &Instance, team: &TeamInfo) -> SuccessTuple {
    inst.conn().edit_team(&to_core(team)).await
}

async fn rename_team(inst: &Instance, old: &str, new: &str) -> SuccessTuple {
    inst.conn().rename_team(old, new).await
}

async fn delete_team(inst: &Instance, name: &str) -> SuccessTuple {
    inst.conn().delete_team(name).await
}

// ------------------------------------------------------------------------------------------
// Helpers.
// ------------------------------------------------------------------------------------------

/// A comma-separated list argument (`--members a,b`).
fn list_arg(args: &ArgsMap, key: &str) -> Option<Vec<String>> {
    string(args, key).map(|text| {
        text.split(',')
            .map(|n| n.trim().to_string())
            .filter(|n| !n.is_empty())
            .collect()
    })
}

/// The usernames among `names` that are not registered on the instance. When the users cannot
/// be listed (an `api:` caller without `users:read`, such as a team's manager), the team store
/// checks them itself.
fn unregistered(
    rt: &mut Runtime<'_>,
    inst: &Instance,
    names: &[String],
) -> Result<Vec<String>, ActionError> {
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let session = session(rt);
    let users = match session.block_on(inst.conn().get_users()) {
        Ok(users) => users,
        Err(_) if inst.type_name() == "api" => return Ok(Vec::new()),
        Err(e) => return Err(ActionError::new("Exception", e.to_string())),
    };
    let mut missing: Vec<String> = names
        .iter()
        .filter(|n| !users.contains(n))
        .cloned()
        .collect();
    missing.sort();
    missing.dedup();
    Ok(missing)
}

fn unregistered_failure(inst: &Instance, missing: &[String]) -> SuccessTuple {
    SuccessTuple::fail(format!(
        "{} not registered on instance '{}': {}.",
        if missing.len() == 1 {
            "This user is"
        } else {
            "These users are"
        },
        inst.keys(),
        missing.join(", ")
    ))
}

/// Whether the jobs that name the instance's teams are this root's. The jobs of an `api:`
/// instance's teams are its server's (its root): its `/teams` routes count and update them.
fn local_jobs(inst: &Instance) -> bool {
    inst.type_name() != "api"
}

/// What names the team: how many of the instance's pipes (and saved Pipelines), and how many
/// jobs (`None` when they cannot be counted). A local instance's jobs are this root's; an `api:`
/// instance's server counts its own pipes and jobs (`GET /teams/{name}?counts=true`, the pipes
/// its caller can see).
fn team_counts(rt: &mut Runtime<'_>, inst: &Instance, team: &str) -> (usize, Option<usize>) {
    let session = session(rt);
    if !local_jobs(inst) {
        let (_, label) = super::session::split_keys(&inst.keys());
        if let Ok(api) = meerschaum_connector_api::APIConnector::from_config(rt.config(), &label)
            && let Ok((pipes, jobs)) = session.block_on(api.get_team_counts(team))
        {
            return (pipes, Some(jobs));
        }
    }
    let conn = inst.conn();
    let pipes = session
        .block_on(meerschaum_connector::teams::team_pipes(conn.as_ref(), team))
        .map(|pipes| pipes.len())
        .unwrap_or(0);
    if !local_jobs(inst) {
        return (pipes, None);
    }
    let ctx = super::jobs::jobs_context(rt);
    (
        pipes,
        Some(meerschaum_jobs::ownership::team_jobs(&ctx, team).len()),
    )
}

/// Rename team `old` in this root's jobs (`None`: move them to Everyone), and say so. Nothing
/// for an `api:` instance (its server updates its own jobs).
fn retag_jobs(rt: &mut Runtime<'_>, inst: &Instance, old: &str, new: Option<&str>) {
    if !local_jobs(inst) {
        return;
    }
    let ctx = super::jobs::jobs_context(rt);
    let retagged = meerschaum_jobs::ownership::retag_team(&ctx, old, new);
    if !retagged.failed.is_empty() {
        warn(rt, &retagged.message(new));
    } else if !retagged.jobs.is_empty() {
        info(rt, &retagged.message(new));
    }
}

/// `N pipes and M jobs` (what names a team; `N pipes` when the jobs are not counted here).
fn counts_text(pipes: usize, jobs: Option<usize>) -> String {
    match jobs {
        Some(jobs) => format!(
            "{pipes} pipe{} and {jobs} job{}",
            plural(pipes),
            plural(jobs)
        ),
        None => format!("{pipes} pipe{}", plural(pipes)),
    }
}

// ------------------------------------------------------------------------------------------
// Actions.
// ------------------------------------------------------------------------------------------

/// `show teams [NAME ...]`: the instance's teams with their managers and members.
pub fn show_teams(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let nopretty = flag(args, "nopretty");
    let names = words(args, "action");
    let session = session(rt);
    let mut teams = match session.block_on(get_teams(&inst)) {
        Ok(teams) => teams,
        Err(e) => {
            return Ok(SuccessTuple::fail(format!(
                "Failed to get the teams of instance '{}': {e}",
                inst.keys()
            )));
        }
    };
    let everyone = teams
        .iter()
        .position(TeamInfo::is_everyone)
        .map(|i| teams.remove(i))
        .unwrap_or_else(TeamInfo::everyone);
    teams.sort_by_key(|t| t.name.to_lowercase());
    teams.insert(0, everyone);
    if !names.is_empty() {
        // Team names compare without case.
        teams.retain(|t| {
            names
                .iter()
                .any(|n| meerschaum_core::access::same_team(Some(n), Some(&t.name)))
        });
        if teams.is_empty() {
            return Ok(SuccessTuple::fail(format!(
                "No team named {} on instance '{}'.",
                super::items_str_with(&names, true, true, "or"),
                inst.keys()
            )));
        }
    }
    if nopretty {
        for team in &teams {
            let mut map = Map::new();
            map.insert("name".into(), Value::Str(team.name.clone()));
            map.insert(
                "managers".into(),
                Value::List(team.managers.iter().cloned().map(Value::Str).collect()),
            );
            // Everyone's members are every user, never stored.
            map.insert(
                "members".into(),
                if team.is_everyone() {
                    Value::Null
                } else {
                    Value::List(team.members.iter().cloned().map(Value::Str).collect())
                },
            );
            let line = meerschaum_core::pyfmt::json_dumps(
                &Value::Map(map),
                meerschaum_core::pyfmt::JsonStyle::Default,
                true,
            );
            rt.print(&format!("{line}\n"));
        }
        return Ok(SuccessTuple::success());
    }
    let title = format!("Teams on instance '{}'", inst.keys());
    let text = rt.with_formatter(|f| {
        let mut table = Table::new();
        table.box_chars = Some(ROUNDED);
        table.title = Some(Text::new(&title, "bold"));
        for header in ["Team", "Managers", "Members"] {
            table.add_column(Column {
                header: Text::plain_text(header),
                ..Column::default()
            });
        }
        for team in &teams {
            let cell = |s: String| Some(Box::new(Text::plain_text(&s)) as Box<dyn Renderable>);
            let members = if team.is_everyone() {
                "every user".to_string()
            } else {
                team.plain_members().join(", ")
            };
            table.add_row(vec![
                cell(team.name.clone()),
                cell(team.managers.join(", ")),
                cell(members),
            ]);
        }
        f.console().print(&table)
    });
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// `register teams NAME ... [--members a,b] [--managers c]`.
pub fn register_teams(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let names = words(args, "action");
    if names.is_empty() {
        return Ok(SuccessTuple::fail("No teams to register."));
    }
    let members = list_arg(args, "members").unwrap_or_default();
    let managers = list_arg(args, "managers").unwrap_or_default();
    let everyone_named: Vec<&String> = names.iter().filter(|n| is_everyone(n)).collect();
    if !everyone_named.is_empty() {
        return Ok(SuccessTuple::fail(format!(
            "'{EVERYONE}' is the built-in team every user belongs to; choose another name."
        )));
    }
    let wanted: Vec<String> = members.iter().chain(&managers).cloned().collect();
    let missing = unregistered(rt, &inst, &wanted)?;
    if !missing.is_empty() {
        return Ok(unregistered_failure(&inst, &missing));
    }
    let session = session(rt);
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for name in &names {
        match session.block_on(get_team(&inst, name)) {
            Ok(Some(_)) => {
                warn(rt, &format!("Team '{name}' already exists. Skipping..."));
                continue;
            }
            Ok(None) => {}
            Err(e) => {
                print_tuple(
                    rt,
                    &SuccessTuple::fail(format!("Failed to look up team '{name}': {e}")),
                    TupleArgs::default(),
                );
                failed += 1;
                continue;
            }
        }
        let team = normalized(TeamInfo {
            name: name.clone(),
            members: members.clone(),
            managers: managers.clone(),
            ..TeamInfo::default()
        });
        info(
            rt,
            &format!(
                "Registering team '{name}' to Meerschaum instance '{}'...",
                inst.keys()
            ),
        );
        let res = session.block_on(register_team(&inst, &team));
        print_tuple(rt, &res, TupleArgs::default());
        if res.success {
            succeeded += 1;
        } else {
            failed += 1;
        }
    }
    Ok(SuccessTuple {
        success: succeeded > 0 && failed == 0,
        message: format!(
            "Finished registering {} team{}.\n    ({succeeded} succeeded, {failed} failed)",
            succeeded + failed,
            plural(succeeded + failed)
        ),
    })
}

/// The team as the editor shows it.
fn team_yaml(team: &TeamInfo) -> Result<String, ActionError> {
    let list = |names: &[String]| Value::List(names.iter().cloned().map(Value::Str).collect());
    let mut map = Map::new();
    map.insert("name".into(), Value::Str(team.name.clone()));
    map.insert("managers".into(), list(&team.managers));
    if !team.is_everyone() {
        map.insert("members".into(), list(&team.plain_members()));
    }
    meerschaum_core::yaml::dump(&Value::Map(map)).map_err(|e| ActionError::new("YAMLError", e.0))
}

/// The team the edited file describes (`Err` with the reason when it is not one).
fn team_from_yaml(text: &str, original: &TeamInfo) -> Result<TeamInfo, String> {
    let value = meerschaum_core::yaml::load(text).map_err(|e| e.0)?;
    let Value::Map(map) = value else {
        return Err("The file must hold `name`, `managers` and `members`.".into());
    };
    let names = |key: &str| -> Result<Vec<String>, String> {
        match map.get(key) {
            None | Some(Value::Null) => Ok(Vec::new()),
            Some(Value::List(items)) => Ok(items.iter().map(Value::py_str).collect()),
            Some(Value::Str(s)) => Ok(s
                .split(',')
                .map(|n| n.trim().to_string())
                .filter(|n| !n.is_empty())
                .collect()),
            Some(_) => Err(format!("`{key}` must be a list of usernames.")),
        }
    };
    let name = match map.get("name") {
        Some(Value::Str(s)) if !s.trim().is_empty() => s.trim().to_string(),
        None | Some(Value::Null) => original.name.clone(),
        Some(_) => return Err("`name` must be the team's name.".into()),
    };
    Ok(normalized(TeamInfo {
        name,
        members: if original.is_everyone() {
            Vec::new()
        } else {
            names("members")?
        },
        managers: names("managers")?,
        id: original.id.clone(),
        attributes: original.attributes.clone(),
    }))
}

/// Open the team in the editor until it is valid: the edited team, or `None` when the file was
/// left unchanged.
fn edit_in_editor(rt: &mut Runtime<'_>, team: &TeamInfo) -> Result<Option<TeamInfo>, ActionError> {
    let dir = rt
        .config()
        .paths()
        .path("CACHE_RESOURCES_PATH")
        .join("teams");
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(format!("{}.yaml", team.name));
    let title = format!("Edit the team '{}'", team.name);
    let top = "#".repeat(title.chars().count() + 4);
    let original = format!("{top}\n# {title} #\n{top}\n\n{}", team_yaml(team)?);
    std::fs::write(&path, &original).map_err(|e| ActionError::new("OSError", e.to_string()))?;
    let result = loop {
        if let Err(failure) = super::editor::edit_file(rt, &path) {
            let _ = std::fs::remove_file(&path);
            return Err(ActionError::new("OSError", failure));
        }
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        if text == original {
            break None;
        }
        match team_from_yaml(&text, team) {
            Ok(edited) => break Some(edited),
            Err(reason) => {
                warn(rt, &format!("Invalid team '{}':\n\n{reason}", team.name));
                if rt
                    .host
                    .read_line(&format!(
                        "Press [Enter] to correct the team '{}' or [CTRL-C] to cancel: ",
                        team.name
                    ))
                    .is_none()
                {
                    let _ = std::fs::remove_file(&path);
                    return Ok(None);
                }
            }
        }
    };
    let _ = std::fs::remove_file(&path);
    Ok(result)
}

/// `edit teams NAME ... [--members a,b] [--managers c]`: the members and managers in the editor
/// (or as given), and a new `name` to rename the team.
pub fn edit_teams(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let names = words(args, "action");
    if names.is_empty() {
        return Ok(SuccessTuple::fail("No teams to edit."));
    }
    let members_arg = list_arg(args, "members");
    let managers_arg = list_arg(args, "managers");
    let (yes, noask, force) = (flag(args, "yes"), flag(args, "noask"), flag(args, "force"));
    let session = session(rt);
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for name in &names {
        let current = match session.block_on(get_team(&inst, name)) {
            Ok(Some(team)) => team,
            Ok(None) if is_everyone(name) => TeamInfo::everyone(),
            Ok(None) => {
                warn(
                    rt,
                    &format!(
                        "Team '{name}' is not registered on instance '{}'. Skipping...",
                        inst.keys()
                    ),
                );
                failed += 1;
                continue;
            }
            Err(e) => {
                print_tuple(
                    rt,
                    &SuccessTuple::fail(format!("Failed to look up team '{name}': {e}")),
                    TupleArgs::default(),
                );
                failed += 1;
                continue;
            }
        };
        let edited = if members_arg.is_some() || managers_arg.is_some() {
            if current.is_everyone() && members_arg.is_some() {
                warn(
                    rt,
                    &format!("Every user is a member of '{EVERYONE}'; only its managers change."),
                );
            }
            Some(normalized(TeamInfo {
                name: current.name.clone(),
                members: if current.is_everyone() {
                    Vec::new()
                } else {
                    members_arg
                        .clone()
                        .unwrap_or_else(|| current.plain_members())
                },
                managers: managers_arg
                    .clone()
                    .unwrap_or_else(|| current.managers.clone()),
                id: current.id.clone(),
                attributes: current.attributes.clone(),
            }))
        } else {
            edit_in_editor(rt, &current)?
        };
        let Some(edited) = edited else {
            info(rt, &format!("Team '{}' is unchanged.", current.name));
            continue;
        };
        let wanted: Vec<String> = edited
            .members
            .iter()
            .chain(&edited.managers)
            .cloned()
            .collect();
        let missing = unregistered(rt, &inst, &wanted)?;
        if !missing.is_empty() {
            print_tuple(
                rt,
                &unregistered_failure(&inst, &missing),
                TupleArgs::default(),
            );
            failed += 1;
            continue;
        }
        // A new name renames the team (and the pipes and jobs that name it) first.
        let mut team_name = current.name.clone();
        if edited.name != current.name {
            if current.is_everyone() || is_everyone(&edited.name) {
                print_tuple(
                    rt,
                    &SuccessTuple::fail(format!(
                        "'{EVERYONE}' is the built-in team: it cannot be renamed, and no team can take its name."
                    )),
                    TupleArgs::default(),
                );
                failed += 1;
                continue;
            }
            let (pipes, jobs) = team_counts(rt, &inst, &current.name);
            let question = format!(
                "Rename team '{}' to '{}'? {} name it.",
                current.name,
                edited.name,
                counts_text(pipes, jobs)
            );
            if !(force || yes_no(rt, &question, "y", yes, noask)?) {
                info(rt, &format!("Team '{}' is unchanged.", current.name));
                continue;
            }
            let res = session.block_on(rename_team(&inst, &current.name, &edited.name));
            print_tuple(rt, &res, TupleArgs::default());
            if !res.success {
                failed += 1;
                continue;
            }
            if jobs.is_some_and(|n| n > 0) {
                retag_jobs(rt, &inst, &current.name, Some(&edited.name));
            }
            team_name = edited.name.clone();
        }
        let after = TeamInfo {
            name: team_name.clone(),
            ..edited
        };
        let before = TeamInfo {
            name: team_name.clone(),
            ..current
        };
        if after.members == before.members && after.managers == before.managers {
            succeeded += 1;
            continue;
        }
        info(
            rt,
            &format!(
                "Editing team '{team_name}' on Meerschaum instance '{}'...",
                inst.keys()
            ),
        );
        let res = session.block_on(edit_team(&inst, &after));
        print_tuple(rt, &res, TupleArgs::default());
        if res.success {
            succeeded += 1;
        } else {
            failed += 1;
        }
    }
    let msg = format!(
        "Finished editing {} team{}.\n    ({succeeded} succeeded, {failed} failed)",
        names.len(),
        plural(names.len())
    );
    Ok(SuccessTuple {
        success: failed == 0,
        message: msg,
    })
}

/// `delete teams NAME ...`: the teams' pipes and jobs move to Everyone (the confirmation counts
/// them).
pub fn delete_teams(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let names = words(args, "action");
    if names.is_empty() {
        return Ok(SuccessTuple::fail("No teams to delete."));
    }
    if names.iter().any(|n| is_everyone(n)) {
        return Ok(SuccessTuple::fail(format!(
            "'{EVERYONE}' is the built-in team every user belongs to and cannot be deleted."
        )));
    }
    let session = session(rt);
    let mut registered: Vec<String> = Vec::new();
    for name in &names {
        match session.block_on(get_team(&inst, name)) {
            Ok(Some(team)) => registered.push(team.name),
            Ok(None) => info(rt, &format!("Team '{name}' does not exist. Skipping...")),
            Err(e) => {
                return Ok(SuccessTuple::fail(format!(
                    "Failed to look up team '{name}': {e}"
                )));
            }
        }
    }
    if registered.is_empty() {
        return Ok(SuccessTuple::fail("No teams to delete."));
    }
    let mut question = format!(
        "Are you sure you want to delete these teams from Meerschaum instance '{}'?\nTheir pipes and jobs move to '{EVERYONE}'.\n",
        inst.keys()
    );
    for name in &registered {
        let (pipes, jobs) = team_counts(rt, &inst, name);
        question.push_str(&format!(" - {name} ({})\n", counts_text(pipes, jobs)));
    }
    let answer =
        flag(args, "force") || yes_no(rt, &question, "n", flag(args, "yes"), flag(args, "noask"))?;
    if !answer {
        return Ok(SuccessTuple::fail("No teams deleted."));
    }
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for name in &registered {
        info(
            rt,
            &format!(
                "Deleting team '{name}' from Meerschaum instance '{}'...",
                inst.keys()
            ),
        );
        let res = session.block_on(delete_team(&inst, name));
        print_tuple(rt, &res, TupleArgs::default());
        if res.success {
            retag_jobs(rt, &inst, name, None);
            succeeded += 1;
        } else {
            failed += 1;
        }
    }
    Ok(SuccessTuple {
        success: failed == 0,
        message: format!(
            "Finished deleting {} team{}.\n    ({succeeded} succeeded, {failed} failed)",
            succeeded + failed,
            plural(succeeded + failed)
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn team(name: &str, members: &[&str], managers: &[&str]) -> TeamInfo {
        normalized(TeamInfo {
            name: name.into(),
            members: members.iter().map(|s| s.to_string()).collect(),
            managers: managers.iter().map(|s| s.to_string()).collect(),
            ..TeamInfo::default()
        })
    }

    #[test]
    fn managers_are_members() {
        let t = team("analytics", &["bob", "alice", "bob"], &["carol"]);
        assert_eq!(t.members, vec!["alice", "bob", "carol"]);
        assert_eq!(t.managers, vec!["carol"]);
        assert_eq!(t.plain_members(), vec!["alice", "bob"]);
    }

    #[test]
    fn the_editor_file_round_trips_and_renames() {
        let t = team("analytics", &["alice"], &["carol"]);
        let text = team_yaml(&t).unwrap();
        assert_eq!(team_from_yaml(&text, &t).unwrap(), t);
        let renamed = text.replace("name: analytics", "name: research");
        assert_eq!(team_from_yaml(&renamed, &t).unwrap().name, "research");
        // A comma-separated string is accepted too, and a missing name keeps the team's.
        let edited = "managers: dave\nmembers: alice, bob\n";
        let parsed = team_from_yaml(edited, &t).unwrap();
        assert_eq!(parsed.name, "analytics");
        assert_eq!(parsed.members, vec!["alice", "bob", "dave"]);
        assert!(team_from_yaml("- a\n- b\n", &t).is_err());
        assert!(team_from_yaml("members: {a: 1}\n", &t).is_err());
    }

    #[test]
    fn counts_name_the_jobs_only_when_they_are_counted_here() {
        assert_eq!(counts_text(2, Some(0)), "2 pipes and 0 jobs");
        assert_eq!(counts_text(1, Some(1)), "1 pipe and 1 job");
        // An `api:` instance's jobs are its server's.
        assert_eq!(counts_text(1, None), "1 pipe");
    }

    #[test]
    fn everyone_has_no_stored_members() {
        let everyone = TeamInfo::everyone();
        let text = team_yaml(&everyone).unwrap();
        assert!(!text.contains("members"));
        let edited = team_from_yaml(
            "name: Everyone\nmanagers: [carol]\nmembers: [x]\n",
            &everyone,
        )
        .unwrap();
        assert!(edited.members.contains(&"carol".to_string()));
        assert!(!edited.members.contains(&"x".to_string()));
        assert!(is_everyone(" everyone "));
    }
}
