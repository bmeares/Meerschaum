//! `register users` (`actions/register.py`), `edit users` (`actions/edit.py`) and
//! `delete users` (`actions/delete.py`).

use meerschaum_core::users::User;
use meerschaum_core::value::{Map, Value};
use meerschaum_core::{SuccessTuple, static_config};

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::value::ArgsMap;

use super::{check_noask, flag, info, print_tuple, prompt, session, warn, words, yes_no};

/// `get_password(username, minimum_length)`; `None` when input ends (Python's `EOFError`).
fn get_password(rt: &mut Runtime<'_>, username: &str, min_len: usize) -> Option<String> {
    // PARITY: with `--noask` v4 loops forever on the empty password; treat it as aborted.
    if check_noask(rt, false) {
        return None;
    }
    loop {
        let password = prompt(rt, &format!("Password for user '{username}':"), true, false)?;
        if password.chars().count() < min_len {
            warn(
                rt,
                &format!(
                    "Password is too short. Please enter a password that is at least {min_len} characters."
                ),
            );
            continue;
        }
        let confirm = prompt(
            rt,
            &format!("Confirm password for user '{username}':"),
            true,
            false,
        )?;
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

fn default_scopes() -> Vec<String> {
    static_config().tokens.scopes.keys().cloned().collect()
}

/// `register users <username> ...`.
pub fn register_users(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let action = words(args, "action");
    if action.is_empty() {
        return Ok(SuccessTuple::fail("No users to register."));
    }
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
    let scopes: Vec<String> = {
        let s = words(args, "scopes");
        if s.is_empty() { default_scopes() } else { s }
    };
    let mut results: Vec<(String, bool)> = Vec::new();
    for username in &nonregistered {
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
        let mut attrs = Map::new();
        attrs.insert(
            "scopes".into(),
            Value::List(scopes.iter().cloned().map(Value::Str).collect()),
        );
        user.attributes = Some(attrs);
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

/// `edit users <username> ...` (prompts for every change).
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
    let mut results: Vec<bool> = Vec::new();
    for username in &action {
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
        let mut user_type: Option<String> = Some(String::new());
        if yes_no(
            rt,
            &format!("Change the user type for user '{username}'?"),
            "n",
            yes,
            noask,
        )? {
            let is_admin = yes_no(
                rt,
                &format!("Make user '{username}' an admin?"),
                "n",
                yes,
                noask,
            )?;
            user_type = is_admin.then(|| "admin".to_string());
        }
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
        let existing_scopes: Vec<String> = session
            .block_on(conn.get_user_attributes(&User::new(username, None)))
            .ok()
            .flatten()
            .and_then(|a| a.get("scopes").and_then(Value::as_list).cloned())
            .map(|l| l.iter().map(Value::py_str).collect())
            .unwrap_or_default();
        let answer = prompt(
            rt,
            &format!(
                "Scopes for user '{username}' (`*` grants all scopes, including custom and future scopes):"
            ),
            true,
            noask,
        )
        .unwrap_or_else(|| existing_scopes.join(" "));
        // FIX (intentional v4 change): `*` is stored literally, not expanded to the built-in
        // scope list, so it also grants custom (plugin) scopes and scopes added later.
        let scopes: Vec<String> = answer.split_whitespace().map(str::to_string).collect();
        let mut attrs = Map::new();
        attrs.insert(
            "scopes".into(),
            Value::List(scopes.into_iter().map(Value::Str).collect()),
        );
        let mut user = User::new(username, Some(&password));
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

/// `delete users <username> ...`.
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
    let mut question = format!(
        "Are you sure you want to delete these users from Meerschaum instance '{}'?\n",
        inst.keys()
    );
    for u in &registered {
        question.push_str(&format!(" - {}\n", u.username));
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
