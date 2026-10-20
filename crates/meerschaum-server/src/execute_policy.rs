//! What a non-administrator may run through `POST /execute` (wave 18, FX7).
//!
//! The Dash console's Execute box runs an action line as a job on the server with the user's own
//! credentials. The console offers a subset of the CLI (it never lists `sh`, `os`, `api`, ...,
//! nor flags such as `--shell`), but the browser's UI is not a security boundary: the route is
//! reachable by anyone who can mint a bearer. This module is the one place that decides, and the
//! route enforces it for every caller who is not an administrator. The console reads the same
//! lists from `GET /execute/policy` (`policy_json()`), so there is nothing to keep in step by hand;
//! the Python UI keeps a built-in copy only for the case where the server cannot be asked
//! (`meerschaum/api/dash/execute.py::OMIT_ACTIONS`, pinned to [`CONSOLE_OMIT_ACTIONS`] by a test).
//!
//! Administrators keep v4's `/actions` latitude: nothing here applies to them (nor to a server
//! started with `--no-auth`, where everyone is one).
//!
//! The check works on the argument list the job will run (`mrsm <sysargs>`), after `+` chaining
//! (every step is checked) and `:` pipeline flags (checked as flags):
//!
//! 1. the first word of a step is the action and must not be a flag (a flag cannot stand in for
//!    it, so `--shell` before the action does not hide anything);
//! 2. the action must not be in [`CONSOLE_OMIT_ACTIONS`] or [`NON_ADMIN_OMIT_ACTIONS`];
//! 3. for some actions, a word naming a subaction that shows or changes the server's own
//!    configuration, credentials or users is refused ([`DENIED_SUBACTIONS`]);
//! 4. no flag of [`OMIT_FLAGS`] (nor an abbreviation of one, nor `-h` / `-V`, including inside a
//!    cluster of short flags) may appear.

use serde_json::{Value as Json, json};

/// The actions the console never offers (v4's list: they are not for a remote console). Refused to
/// non-administrators on the server too.
pub const CONSOLE_OMIT_ACTIONS: &[&str] = &[
    "api", "sh", "os", "sql", "stack", "reload", "repo", "instance",
];

/// Further actions a non-administrator may not run: they execute arbitrary code (`python`,
/// `start jobs`, `install` of a plugin or package, `upgrade`, `compose`, `setup`), or need a
/// terminal (`bootstrap`, `login`).
pub const NON_ADMIN_OMIT_ACTIONS: &[&str] = &[
    "python",
    "start",
    "install",
    "uninstall",
    "upgrade",
    "compose",
    "setup",
    "bootstrap",
    "login",
];

/// Subactions (as the words the user types, singular, lower case, without `-` and `_`) that a
/// non-administrator may not use after the given action. A word is matched among the step's
/// positional words (not flags nor the values of a flag that takes them), since flags may stand
/// between the action and the subaction.
pub const DENIED_SUBACTIONS: &[(&str, &[&str])] = &[
    // Print the configuration (with connector passwords), the environment or another job's output.
    ("show", &["config", "environment", "log"]),
    ("edit", &["config", "plugin", "token", "user", "job"]),
    ("delete", &["config", "connector", "plugin", "user", "venv"]),
    ("copy", &["connector"]),
    ("register", &["user", "plugin", "token", "connector"]),
];

/// The flags the console does not offer (long form, without the dashes; `-h` and `-V` are their
/// short forms): the ones that open a shell or a window, ask for the help or the version, or let a
/// job run arbitrary shell commands.
pub const OMIT_FLAGS: &[&str] = &[
    "help",
    "gui",
    "version",
    "shell",
    "use-bash",
    "trace",
    "allow-shell-job",
];

/// The short forms of [`OMIT_FLAGS`].
const OMIT_SHORT_FLAGS: &[char] = &['h', 'V'];

/// `show` / `edit` / ... as the words are compared: lower case, without `-` and `_`, singular.
fn normalize(word: &str) -> String {
    let mut s: String = word
        .chars()
        .filter(|c| !matches!(c, '-' | '_'))
        .flat_map(char::to_lowercase)
        .collect();
    if s.len() > 1 && s.ends_with('s') {
        s.pop();
    }
    s
}

/// Whether `arg` is a flag of [`OMIT_FLAGS`] (or an abbreviation of one, which `argparse` accepts).
fn omitted_flag(arg: &str) -> Option<String> {
    if let Some(long) = arg.strip_prefix("--") {
        let name = long.split('=').next().unwrap_or("").replace('_', "-");
        let name = name.to_lowercase();
        if name.is_empty() {
            return None;
        }
        return OMIT_FLAGS
            .iter()
            .any(|flag| flag.starts_with(&name))
            .then(|| format!("--{name}"));
    }
    let short = arg.strip_prefix('-')?;
    // A cluster of short flags (`-yh`): `-h` and `-V` anywhere in it. A value attached to a short
    // flag (`-csql:main`) is not a cluster of letters and is left alone.
    if !short.is_empty()
        && short.chars().all(|c| c.is_ascii_alphabetic())
        && let Some(flag) = short.chars().find(|c| OMIT_SHORT_FLAGS.contains(c))
    {
        return Some(format!("-{flag}"));
    }
    None
}

/// Flags that take a list of values (`-m a b c`): every word up to the next flag is theirs.
const LIST_FLAGS: &[&str] = &[
    "-c",
    "-C",
    "-m",
    "-M",
    "-l",
    "-L",
    "-t",
    "--connector-keys",
    "--metric-keys",
    "--location-keys",
    "--tags",
];

/// Flags that take one value (`-i sql:main`).
const VALUE_FLAGS: &[&str] = &[
    "-i",
    "-I",
    "--mrsm-instance",
    "--instance",
    "-e",
    "--executor-keys",
    "-P",
    "--params",
    "--name",
    "--job-name",
    "--begin",
    "--end",
    "-s",
    "--schedule",
];

/// The words of a step after the action that are not flags nor the values of a flag that takes
/// them (`-m logs` names a metric, it does not ask to show the logs).
fn positionals(words: &[String]) -> Vec<&str> {
    let mut out = Vec::new();
    let (mut in_list, mut skip_one) = (false, false);
    for word in words {
        if word.starts_with('-') {
            in_list = LIST_FLAGS.contains(&word.as_str());
            skip_one = VALUE_FLAGS.contains(&word.as_str());
        } else if skip_one {
            skip_one = false;
        } else if !in_list {
            out.push(word.as_str());
        }
    }
    out
}

/// Check one step (the words between `+`), pipeline flags included.
fn check_step(step: &[String]) -> Result<(), String> {
    let (words, pipeline_flags) = match step.iter().position(|a| a == ":") {
        Some(ix) => (&step[..ix], &step[ix + 1..]),
        None => (step, &step[..0]),
    };
    for arg in words.iter().chain(pipeline_flags) {
        if let Some(flag) = omitted_flag(arg) {
            return Err(format!(
                "The flag '{flag}' cannot be used by non-administrators."
            ));
        }
    }
    let Some(action) = words.first() else {
        return Ok(());
    };
    if action.starts_with('-') {
        return Err(format!(
            "The action must come before any flag ('{action}'). Non-administrators run \
             `<action> [subaction] [flags]`."
        ));
    }
    let normalized = normalize(action);
    if CONSOLE_OMIT_ACTIONS
        .iter()
        .chain(NON_ADMIN_OMIT_ACTIONS)
        .any(|omitted| normalize(omitted) == normalized)
    {
        return Err(format!(
            "The action '{action}' cannot be run by non-administrators."
        ));
    }
    for (denied_action, subactions) in DENIED_SUBACTIONS {
        if normalize(denied_action) != normalized {
            continue;
        }
        if let Some(word) = positionals(&words[1..])
            .into_iter()
            .find(|w| subactions.contains(&normalize(w).as_str()))
        {
            return Err(format!(
                "'{action} {word}' cannot be run by non-administrators."
            ));
        }
    }
    Ok(())
}

/// Check the argument list of a job against the rules for non-administrators. `Err` is the
/// message of the 403.
pub fn check_non_admin(sysargs: &[String]) -> Result<(), String> {
    for step in sysargs.split(|a| a == "+") {
        check_step(step)?;
    }
    Ok(())
}

/// `GET /execute/policy`: the lists above, for the console's own dropdowns.
pub fn policy_json() -> Json {
    json!({
        "omit_actions": CONSOLE_OMIT_ACTIONS,
        "non_admin_omit_actions": NON_ADMIN_OMIT_ACTIONS,
        "denied_subactions": DENIED_SUBACTIONS
            .iter()
            .map(|(action, subactions)| (action.to_string(), json!(subactions)))
            .collect::<serde_json::Map<String, Json>>(),
        "omit_flags": OMIT_FLAGS.iter().map(|f| format!("--{f}")).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn steps_are_checked_after_chaining() {
        assert!(check_non_admin(&s(&["show", "pipes", "+", "sync", "pipes"])).is_ok());
        assert!(check_non_admin(&s(&["show", "pipes", "+", "python"])).is_err());
        assert!(check_non_admin(&s(&["+", "sh", "id"])).is_err());
    }

    #[test]
    fn abbreviations_and_clusters_of_omitted_flags_are_refused() {
        for flag in [
            "--sh",
            "--shell",
            "--use_bash",
            "--he",
            "--v",
            "--SHELL=1",
            "-h",
            "-yV",
        ] {
            assert!(omitted_flag(flag).is_some(), "{flag}");
        }
        for flag in [
            "--debug",
            "--yes",
            "-y",
            "-c",
            "-csql:main",
            "--",
            "--begin",
            "-5",
        ] {
            assert!(omitted_flag(flag).is_none(), "{flag}");
        }
    }

    #[test]
    fn subactions_match_singular_plural_and_case() {
        assert!(check_non_admin(&s(&["show", "Configs"])).is_err());
        assert!(check_non_admin(&s(&["show", "-i", "x", "environment"])).is_err());
        assert!(check_non_admin(&s(&["show", "--nopretty", "config"])).is_err());
        assert!(check_non_admin(&s(&["edit", "pipes"])).is_ok());
        // The values of a flag are not subactions.
        assert!(
            check_non_admin(&s(&["show", "pipes", "-m", "config", "logs", "-l", "None"])).is_ok()
        );
        assert!(check_non_admin(&s(&["show", "pipes", "-i", "config"])).is_ok());
    }

    /// The console's built-in copy (the list it uses when the server cannot be asked) is the
    /// server's list. Skipped outside a checkout of the repository (a packaged crate).
    #[test]
    fn the_consoles_builtin_omit_list_is_the_servers() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../meerschaum/api/dash/execute.py");
        let Ok(source) = std::fs::read_to_string(path) else {
            return;
        };
        let block = source
            .split("OMIT_ACTIONS = frozenset({")
            .nth(1)
            .and_then(|rest| rest.split("})").next())
            .expect("OMIT_ACTIONS in execute.py");
        let mut python: Vec<&str> = block
            .lines()
            .filter_map(|line| line.trim().trim_end_matches(',').strip_prefix('\''))
            .filter_map(|line| line.strip_suffix('\''))
            .collect();
        python.sort_unstable();
        let mut rust = CONSOLE_OMIT_ACTIONS.to_vec();
        rust.sort_unstable();
        assert_eq!(python, rust);
    }

    #[test]
    fn the_published_policy_names_every_list() {
        let policy = policy_json();
        assert_eq!(policy["omit_actions"].as_array().unwrap().len(), 8);
        assert!(
            policy["omit_flags"]
                .as_array()
                .unwrap()
                .iter()
                .any(|f| f == "--shell")
        );
        assert!(policy["denied_subactions"]["show"].is_array());
    }
}
