//! `edit pipes access`, `lock pipes` and `unlock pipes` (rc15 wave 2, AC4 / AC5 / AC10), and the
//! `copy pipes` rule (a copy keeps its source's locks and never takes its access).
//!
//! A pipe's access is its reserved parameter `access` ([`PipeAccess`]: `owner`, `team`,
//! `visibility`, `edit`) and its locks the reserved parameter `locks` ([`Locks`]). The CLI writes
//! them with the instance's `set_pipe_access`, `lock_pipe` and `unlock_pipe`: locally the
//! instance's credentials are the trust boundary (anyone with them may change access and locks),
//! and on an `api:` instance the server decides who may.
//!
//! A lock refuses an operation everywhere (CLI, Python, jobs, the engine, the API, the console);
//! `-y`, `--force` and `--allow-protected` never bypass one. Unlocking is the only way past it.

use meerschaum_core::SuccessTuple;
use meerschaum_core::access::{
    EVERYONE, EditMode, Lock, Locks, PipeAccess, Visibility, normalize_team,
    parameters_with_access, parameters_with_locks,
};
use meerschaum_core::users::User;
use meerschaum_core::value::Map;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::{OptionsArgs, TupleArgs};
use crate::value::ArgsMap;

use super::ask::{Ask, ask, choose};
use super::pipes::{SelectedPipe, hydrate, selected_pipes};
use super::{Instance, check_noask, flag, info, plural, print_tuple, session, string};

/// A source pipe's parameters as `copy pipes` registers them: without `access` (the copy gets
/// the default access) and without `locks` (set after the copy's data sync), and the source's
/// locks.
pub fn copy_parameters(source: &Map) -> (Map, Locks) {
    let locks = Locks::from_parameters(source);
    let params = parameters_with_locks(&parameters_with_access(source, None), &Locks::new());
    (params, locks)
}

/// The lock names, comma-separated (`drop, sync`; `all` stays `all`).
fn lock_list(locks: &[Lock]) -> String {
    locks
        .iter()
        .map(|l| l.name())
        .collect::<Vec<_>>()
        .join(", ")
}

/// One line for an access: `owner alice, team analytics, private, edited by the owner`.
pub fn describe_access(access: &PipeAccess) -> String {
    let visibility = match access.visibility {
        Visibility::Private => "private",
        Visibility::Everyone => "visible to everyone",
        Visibility::Team => "visible to the team",
    };
    let edit = match access.edit {
        EditMode::Owner => "edited by the owner",
        EditMode::Team => "edited by the team",
    };
    format!(
        "owner {}, team {}, {visibility}, {edit}",
        access.owner.as_deref().unwrap_or("none"),
        access.team_name()
    )
}

/// How a pipe's access or locks change.
enum Change<'a> {
    /// Store this access (`set_pipe_access`; a `parameters` lock refuses it).
    Access(&'a PipeAccess),
    /// Add these locks (`lock_pipe`).
    Lock(&'a Locks),
    /// Remove these locks (`unlock_pipe`).
    Unlock(&'a Locks),
}

/// Write a change of a registered pipe's access or locks through the instance (an `api:`
/// instance asks its server, which checks who may), and keep the pipe's in-memory parameters
/// in step.
fn write_change(
    rt: &mut Runtime<'_>,
    inst: &Instance,
    sp: &mut SelectedPipe,
    change: Change<'_>,
) -> SuccessTuple {
    if sp.pipe.temporary {
        return SuccessTuple::fail("Cannot edit pipes created with `temporary=True` (read-only).");
    }
    let session = session(rt);
    let conn = inst.conn();
    let res = match &change {
        Change::Access(access) => session.block_on(conn.set_pipe_access(&sp.pipe, access)),
        Change::Lock(locks) => session.block_on(conn.lock_pipe(&sp.pipe, locks)),
        Change::Unlock(locks) => session.block_on(conn.unlock_pipe(&sp.pipe, locks)),
    };
    if res.success {
        let params = sp.pipe.raw_parameters();
        let params = match change {
            Change::Access(access) => parameters_with_access(params, Some(access)),
            Change::Lock(locks) => {
                parameters_with_locks(params, &Locks::from_parameters(params).with(locks))
            }
            Change::Unlock(locks) => {
                parameters_with_locks(params, &Locks::from_parameters(params).without(locks))
            }
        };
        sp.pipe.set_parameters(params);
    }
    res
}

/// The selected pipes with their stored parameters, or the failure to return.
type Selected = Result<(Instance, Vec<SelectedPipe>), SuccessTuple>;

fn hydrated_selection(
    rt: &mut Runtime<'_>,
    args: &ArgsMap,
    verb: &str,
) -> Result<Selected, ActionError> {
    let (inst, mut pipes) = selected_pipes(rt, args)?;
    if pipes.is_empty() {
        return Ok(Err(SuccessTuple::fail(format!("No pipes to {verb}."))));
    }
    for sp in pipes.iter_mut() {
        hydrate(rt, &inst, sp);
    }
    Ok(Ok((inst, pipes)))
}

/// One value every pipe shares (`None` when they differ).
fn shared<T: PartialEq + Clone>(values: impl Iterator<Item = T>) -> Option<T> {
    let mut first: Option<T> = None;
    for value in values {
        match &first {
            None => first = Some(value),
            Some(f) if *f == value => {}
            Some(_) => return None,
        }
    }
    first
}

/// A field the command line did not give: asked for, or kept.
enum Field<T> {
    Keep,
    Set(T),
}

/// The team names a pipe may name (`Everyone` first), from the instance.
fn team_names(rt: &mut Runtime<'_>, inst: &Instance) -> Result<Vec<String>, String> {
    let session = session(rt);
    let teams = session
        .block_on(super::teams::get_teams(inst))
        .map_err(|e| {
            format!(
                "Failed to read the teams of instance '{}': {e}",
                inst.keys()
            )
        })?;
    let mut names = vec![EVERYONE.to_string()];
    names.extend(
        teams
            .into_iter()
            .map(|t| t.name)
            .filter(|n| normalize_team(Some(n)).is_some()),
    );
    Ok(names)
}

/// An owner answer: `none` or nothing is no owner.
fn owner_value(text: &str) -> Option<String> {
    let text = text.trim();
    (!text.is_empty() && !text.eq_ignore_ascii_case("none")).then(|| text.to_string())
}

/// `edit pipes access [--owner USER] [--team TEAM] [--visibility V] [--edit E]`: set who owns
/// the selected pipes, which team they belong to, who sees them and who may change them. A
/// field the flags leave out is asked for (and kept with `--noask` / `-y`).
pub fn edit_pipes_access(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let (inst, mut pipes) = match hydrated_selection(rt, args, "edit")? {
        Ok(selection) => selection,
        Err(failure) => return Ok(failure),
    };
    let noask = check_noask(rt, flag(args, "noask")) || flag(args, "yes") || flag(args, "force");
    let options: Vec<String> = pipes.iter().map(|p| p.pipe.to_string()).collect();
    let header = OptionsArgs {
        header: Some("Pipes whose access will be edited:".into()),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&options, &header));
    rt.print(&text);

    let current: Vec<PipeAccess> = pipes
        .iter()
        .map(|p| PipeAccess::from_parameters(p.pipe.raw_parameters()))
        .collect();

    // The owner.
    let owner: Field<Option<String>> = match string(args, "owner") {
        Some(value) => Field::Set(owner_value(&value)),
        None if noask => Field::Keep,
        None => {
            let default = shared(current.iter().map(|a| a.owner.clone()))
                .map(|o| o.unwrap_or_else(|| "none".into()));
            let mut question = Ask::new();
            if let Some(d) = &default {
                question = question.default(d.clone());
            }
            let answer = ask(rt, "Owner (a username, or none):", &question)?;
            if answer.trim().is_empty() {
                Field::Keep
            } else {
                Field::Set(owner_value(&answer))
            }
        }
    };
    if let Field::Set(Some(username)) = &owner {
        let session = session(rt);
        let registered = session
            .block_on(inst.conn().get_user_id(&User::new(username, None)))
            .map_err(|e| ActionError::new("Exception", e.to_string()))?
            .is_some();
        if !registered {
            return Ok(SuccessTuple::fail(format!(
                "User '{username}' is not registered on instance '{}'.",
                inst.keys()
            )));
        }
    }

    // The team.
    let team: Field<Option<String>> = match string(args, "team") {
        Some(value) => Field::Set(normalize_team(Some(&value))),
        None if noask => Field::Keep,
        None => {
            let names = match team_names(rt, &inst) {
                Ok(names) => names,
                Err(message) => return Ok(SuccessTuple::fail(message)),
            };
            let default = shared(current.iter().map(PipeAccess::team_name));
            match choose(rt, "Team:", &names, default.as_deref(), true)? {
                Some(answer) => Field::Set(normalize_team(Some(&answer))),
                None => Field::Keep,
            }
        }
    };
    let team = match team {
        Field::Set(Some(name)) => {
            let names = match team_names(rt, &inst) {
                Ok(names) => names,
                Err(message) => return Ok(SuccessTuple::fail(message)),
            };
            // Team names compare without case; the stored name is the registered one.
            match names.iter().find(|n| n.eq_ignore_ascii_case(&name)) {
                Some(registered) => Field::Set(Some(registered.clone())),
                None => {
                    return Ok(SuccessTuple::fail(format!(
                        "Team '{name}' is not registered on instance '{}' (`mrsm register team {name}`).",
                        inst.keys()
                    )));
                }
            }
        }
        other => other,
    };

    // Who sees the pipes, and who may change them.
    let visibility: Field<Visibility> = match string(args, "visibility") {
        Some(value) => match Visibility::parse(&value) {
            Some(v) => Field::Set(v),
            None => {
                return Ok(SuccessTuple::fail(format!(
                    "Invalid visibility '{value}': use `team`, `private` or `everyone`."
                )));
            }
        },
        None if noask => Field::Keep,
        None => {
            let names: Vec<String> = Visibility::ALL
                .iter()
                .map(|v| v.name().to_string())
                .collect();
            let default = shared(current.iter().map(|a| a.visibility.name().to_string()));
            match choose(
                rt,
                "Who sees the pipes (team: the team; private: the owner and the team's managers; everyone: every user)?",
                &names,
                default.as_deref(),
                true,
            )? {
                Some(answer) => Visibility::parse(&answer).map_or(Field::Keep, Field::Set),
                None => Field::Keep,
            }
        }
    };
    let edit: Field<EditMode> = match string(args, "edit_mode") {
        Some(value) => match EditMode::parse(&value) {
            Some(e) => Field::Set(e),
            None => {
                return Ok(SuccessTuple::fail(format!(
                    "Invalid edit mode '{value}': use `team` or `owner`."
                )));
            }
        },
        None if noask => Field::Keep,
        None => {
            let names: Vec<String> = EditMode::ALL.iter().map(|e| e.name().to_string()).collect();
            let default = shared(current.iter().map(|a| a.edit.name().to_string()));
            match choose(
                rt,
                "Who may change the pipes (team: anyone who sees them; owner: the owner and the team's managers)?",
                &names,
                default.as_deref(),
                true,
            )? {
                Some(answer) => EditMode::parse(&answer).map_or(Field::Keep, Field::Set),
                None => Field::Keep,
            }
        }
    };

    let mut changed = 0usize;
    let mut unchanged = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for (sp, before) in pipes.iter_mut().zip(current) {
        let mut after = before.clone();
        if let Field::Set(owner) = &owner {
            after.owner = owner.clone();
        }
        if let Field::Set(team) = &team {
            after.team = team.clone();
        }
        if let Field::Set(visibility) = &visibility {
            after.visibility = *visibility;
        }
        if let Field::Set(edit) = &edit {
            after.edit = *edit;
        }
        if after == before {
            unchanged += 1;
            info(
                rt,
                &format!("{} already has {}.", sp.pipe, describe_access(&after)),
            );
            continue;
        }
        let res = write_change(rt, &inst, sp, Change::Access(&after));
        if res.success {
            changed += 1;
            print_tuple(
                rt,
                &SuccessTuple::ok(format!("{}: {}.", sp.pipe, describe_access(&after))),
                TupleArgs::default(),
            );
        } else {
            failures.push(format!("{}: {}", sp.pipe, res.message));
            print_tuple(rt, &res, TupleArgs::default());
        }
    }
    Ok(summary(
        "Edited the access of",
        changed,
        unchanged,
        &failures,
    ))
}

/// The result of a bulk access or lock change.
fn summary(verb: &str, changed: usize, unchanged: usize, failures: &[String]) -> SuccessTuple {
    let total = changed + unchanged + failures.len();
    let mut message = format!("{verb} {changed} of {total} pipe{}", plural(total));
    let mut parts = Vec::new();
    if unchanged > 0 {
        parts.push(format!("{unchanged} unchanged"));
    }
    if !failures.is_empty() {
        parts.push(format!("{} failed", failures.len()));
    }
    if !parts.is_empty() {
        message.push_str(&format!(" ({})", parts.join(", ")));
    }
    message.push('.');
    for failure in failures {
        message.push_str(&format!(
            "\n  - {}",
            failure.lines().next().unwrap_or_default()
        ));
    }
    SuccessTuple {
        success: failures.is_empty(),
        message,
    }
}

/// `lock pipes [--locks LOCKS]` (default `all`).
pub fn lock_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    change_locks(args, rt, true)
}

/// `unlock pipes [--locks LOCKS]` (default `all`).
pub fn unlock_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    change_locks(args, rt, false)
}

fn change_locks(args: &ArgsMap, rt: &mut Runtime<'_>, lock: bool) -> ActionResult {
    let verb = if lock { "lock" } else { "unlock" };
    let given = match Locks::parse_list(&string(args, "locks").unwrap_or_default()) {
        Ok(locks) => locks,
        Err(message) => return Ok(SuccessTuple::fail(message)),
    };
    let (inst, mut pipes) = match hydrated_selection(rt, args, verb)? {
        Ok(selection) => selection,
        Err(failure) => return Ok(failure),
    };
    let mut changed = 0usize;
    let mut unchanged = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for sp in pipes.iter_mut() {
        let before = Locks::from_parameters(sp.pipe.raw_parameters());
        let after = if lock {
            before.with(&given)
        } else {
            before.without(&given)
        };
        if after.kinds() == before.kinds() {
            unchanged += 1;
            let state = if before.is_empty() {
                "has no locks".to_string()
            } else {
                format!("is locked ({before})")
            };
            info(rt, &format!("{} {state}.", sp.pipe));
            continue;
        }
        let change = if lock {
            Change::Lock(&given)
        } else {
            Change::Unlock(&given)
        };
        let res = write_change(rt, &inst, sp, change);
        if res.success {
            changed += 1;
            let message = if lock {
                format!("Locked {} ({after}).", sp.pipe)
            } else if after.is_empty() {
                format!("Unlocked {}.", sp.pipe)
            } else {
                format!(
                    "Unlocked {} ({}); still locked ({after}).",
                    sp.pipe,
                    lock_list(
                        &before
                            .kinds()
                            .into_iter()
                            .filter(|k| !after.contains(*k))
                            .collect::<Vec<_>>()
                    )
                )
            };
            print_tuple(rt, &SuccessTuple::ok(message), TupleArgs::default());
        } else {
            failures.push(format!("{}: {}", sp.pipe, res.message));
            print_tuple(rt, &res, TupleArgs::default());
        }
    }
    let verb = if lock { "Locked" } else { "Unlocked" };
    Ok(summary(verb, changed, unchanged, &failures))
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_core::value::Value;

    #[test]
    fn copies_keep_locks_and_lose_access() {
        let mut source = Map::new();
        source.insert("columns".into(), Value::new_map());
        source.insert(
            "access".into(),
            PipeAccess {
                owner: Some("alice".into()),
                ..PipeAccess::default()
            }
            .to_value(),
        );
        source.insert("locks".into(), Value::List(vec![Value::Str("drop".into())]));
        let (params, locks) = copy_parameters(&source);
        assert!(params.contains_key("columns"));
        assert!(!params.contains_key("access"));
        assert!(!params.contains_key("locks"));
        assert_eq!(locks.kinds(), vec![Lock::Drop]);
    }

    #[test]
    fn access_lines_read_plainly() {
        let access = PipeAccess {
            owner: Some("alice".into()),
            team: Some("analytics".into()),
            visibility: Visibility::Private,
            edit: EditMode::Owner,
        };
        assert_eq!(
            describe_access(&access),
            "owner alice, team analytics, private, edited by the owner"
        );
        assert_eq!(
            describe_access(&PipeAccess::default()),
            "owner none, team Everyone, visible to the team, edited by the team"
        );
        assert_eq!(owner_value(" None "), None);
        assert_eq!(owner_value(""), None);
        assert_eq!(owner_value("bob"), Some("bob".into()));
    }

    #[test]
    fn one_value_is_shared_only_when_every_pipe_has_it() {
        assert_eq!(shared(["team", "team"].into_iter()), Some("team"));
        assert_eq!(shared(["team", "owner"].into_iter()), None);
        assert_eq!(shared(std::iter::empty::<&str>()), None);
    }

    #[test]
    fn summaries_count_changes_and_list_failures() {
        let ok = summary("Locked", 2, 1, &[]);
        assert!(ok.success);
        assert_eq!(ok.message, "Locked 2 of 3 pipes (1 unchanged).");
        let failed = summary(
            "Edited the access of",
            0,
            0,
            &["Pipe('a', 'b'): it is locked (parameters).\nmore".to_string()],
        );
        assert!(!failed.success);
        assert_eq!(
            failed.message,
            "Edited the access of 0 of 1 pipe (1 failed).\n  - Pipe('a', 'b'): it is locked (parameters)."
        );
    }
}
