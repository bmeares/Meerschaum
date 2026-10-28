//! What a caller who is not an administrator may run through `POST /execute` (rc15 access
//! model), on top of [`crate::execute_policy`]'s list of what the console offers.
//!
//! The job `POST /execute` starts runs on the server with the instance's own credentials, which
//! see every pipe. So the server checks the pipes each step of the command selects:
//!
//! - **Pipe operations** the console runs for its Sync, Verify, Deduplicate, Clear, Drop and
//!   Delete buttons (`sync pipes`, `verify pipes`, `verify schema`, `deduplicate pipes`,
//!   `clear pipes`, `drop pipes`, `delete pipes`) need the permission of the operation
//!   (`pipes:write`, `pipes:delete`, `pipes:drop`) and edit rights on every pipe the step's
//!   selection (`-c`, `-m`, `-l`, `-t`, `-i`) matches; `sync pipelines NAME` needs
//!   `pipes:write`, a Pipeline the caller can see and edit rights on every pipe it would sync.
//!   These need no `actions:execute` (the role `editor` runs them) unless the step schedules or
//!   loops (`-s`, `--loop`, `:` flags).
//! - **Every other action** needs `actions:execute` (and `jobs:write`), and when it selects
//!   pipes (`show pipes`, `show data`, `copy pipes`, `lock pipes`, ...), the caller must be able
//!   to see them (show), change them (everything else) or lock them (`lock` / `unlock pipes`),
//!   or manage their access (`edit pipes access`).
//!
//! A selection is checked as written: the keys and tags it names, with the CLI's other filters
//! ignored (so the checked set is never smaller than the one that runs). Narrow a refused
//! selection with `-c`, `-m`, `-l` or `-t`.
//!
//! The same checks apply to the other routes that run an action on the server for a caller who
//! is not an administrator: `POST /actions/{action}` (its keywords read as a command line,
//! [`sysargs_of_keywords`]), `POST /jobs/{name}` (the `api:` executor's jobs) and MCP's
//! `execute_action`. Those also follow the console's list of what a non-administrator may run
//! ([`crate::execute_policy`]): `actions:execute` runs actions, never a shell (`sh`, `python`,
//! `sql`, `--shell`, ...), which only the web terminal (`webterm:use`) gives.

use meerschaum_connector::PipesKeysQuery;

use crate::access::Access;
use crate::backends::PlanOptions;
use crate::error::ApiError;
use crate::state::{AppState, normalize_location};

/// The flags of a pipe selection.
const CONNECTOR_FLAGS: &[&str] = &["-c", "-C", "--connector-keys"];
const METRIC_FLAGS: &[&str] = &["-m", "-M", "--metric-keys"];
const LOCATION_FLAGS: &[&str] = &["-l", "-L", "--location-keys"];
const TAG_FLAGS: &[&str] = &["-t", "--tags"];
const INSTANCE_FLAGS: &[&str] = &["-i", "-I", "--mrsm-instance", "--instance"];
/// Flags that make a step a scheduled or looping job (`actions:execute`).
const REPEAT_FLAGS: &[&str] = &["-s", "--schedule", "--loop", "--min-seconds", "--cron"];

/// What a step does to the pipes it selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Use {
    Read,
    Write,
    Lock,
    ManageAccess,
}

/// What a step is.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    /// A console pipe operation: its permission.
    PipeOperation(&'static str),
    /// `sync pipelines NAME ...`.
    Pipelines(Vec<String>),
    /// Any other action, with what it does to the pipes it selects (if it selects any).
    Action(Option<Use>),
}

/// `show` / `edit` / ... as words compare (see [`crate::execute_policy`]): lower case, without
/// `-` and `_`, singular.
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

/// The values of a flag in a step's words (`-c a b`, `-ca`, `--connector-keys=a`).
fn matched_flag_value(word: &str, flags: &[&str]) -> Option<Option<String>> {
    flags.iter().find_map(|flag| {
        if word == *flag {
            Some(None)
        } else if let Some(rest) = word.strip_prefix(&format!("{flag}=")) {
            Some(Some(rest.to_string()))
        } else if flag.len() == 2
            && !flag.starts_with("--")
            && word.starts_with(flag)
            && word.len() > 2
        {
            Some(Some(word[2..].to_string()))
        } else {
            None
        }
    })
}

fn flag_values(words: &[String], flags: &[&str]) -> Option<Vec<String>> {
    let mut out: Option<Vec<String>> = None;
    let mut i = 0;
    while i < words.len() {
        let word = &words[i];
        let matched = matched_flag_value(word, flags);
        match matched {
            Some(Some(value)) => {
                out.get_or_insert_with(Vec::new).push(value);
                i += 1;
            }
            Some(None) => {
                let values = out.get_or_insert_with(Vec::new);
                i += 1;
                while i < words.len() && !words[i].starts_with('-') {
                    values.push(words[i].clone());
                    i += 1;
                }
            }
            None => i += 1,
        }
    }
    out
}

/// One-value option spellings accepted by the CLI, including abbreviations and short clusters.
pub(crate) fn option_values(
    words: &[String],
    short_flag: char,
    long_flag: &str,
) -> Option<Vec<String>> {
    let mut out = None;
    for (index, word) in words.iter().enumerate() {
        let mut canonical = word.to_string();
        if let Some(long) = word.strip_prefix("--") {
            let name = long.split('=').next().unwrap_or_default();
            if !name.is_empty() && long_flag.starts_with(name) {
                canonical = format!("--{long_flag}{}", &long[name.len()..]);
            }
        } else if let Some(short) = word.strip_prefix('-') {
            // The native parser accepts attached values inside clusters of boolean shorts.
            // Stop at a value-taking option, so `-mweather` remains an ordinary metric.
            for (offset, character) in short.char_indices() {
                if character == short_flag {
                    canonical = format!("-{short_flag}{}", &short[offset + 1..]);
                    break;
                }
                if !matches!(character, 'y' | 'f' | 'd' | 'v' | 'h' | 'V') {
                    break;
                }
            }
        }
        if let Some(value) = matched_flag_value(
            &canonical,
            &[&format!("-{short_flag}"), &format!("--{long_flag}")],
        ) {
            let values = out.get_or_insert_with(Vec::new);
            if let Some(value) = value.or_else(|| words.get(index + 1).cloned()) {
                values.push(value);
            }
        }
    }
    out
}

/// The executor spellings accepted by the real CLI, including attached / equals values.
pub(crate) fn executor_values(words: &[String]) -> Option<Vec<String>> {
    option_values(words, 'e', "executor-keys")
}

fn selected_values(
    words: &[String],
    flags: &[&str],
    patch: &meerschaum_core::Map,
    key: &str,
) -> Option<Vec<String>> {
    match patch.get(key) {
        Some(meerschaum_core::Value::List(values)) => {
            Some(values.iter().map(meerschaum_core::Value::py_str).collect())
        }
        Some(meerschaum_core::Value::Null) => None,
        Some(value) => Some(vec![value.py_str()]),
        None => flag_values(words, flags),
    }
}

/// Flags that take every following word up to the next flag.
const LIST_FLAGS: &[&str] = &[
    "-c",
    "-C",
    "--connector-keys",
    "-m",
    "-M",
    "--metric-keys",
    "-l",
    "-L",
    "--location-keys",
    "-t",
    "--tags",
    "--targets",
    "--target",
    "--locks",
];

/// Flags that take one word.
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
    "--min-seconds",
    "--run-id",
    "--owner",
    "--team",
    "--visibility",
    "--edit",
];

/// The positional words of a step after its action (no flags, no flag values).
fn positionals(words: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let (mut in_list, mut skip_one) = (false, false);
    for word in words {
        if word.starts_with('-') {
            let name = word.split('=').next().unwrap_or(word);
            let attached = word.contains('=');
            in_list = !attached && LIST_FLAGS.contains(&name);
            skip_one = !attached && VALUE_FLAGS.contains(&name);
        } else if skip_one {
            skip_one = false;
        } else if !in_list {
            out.push(word.clone());
        }
    }
    out
}

fn classify(action: &str, positional: &[String]) -> Step {
    let action = normalize(action);
    let sub = positional.first().map(|w| normalize(w));
    let pipe_sub = sub.as_deref().is_some_and(|s| {
        matches!(
            s,
            "pipe" | "data" | "rowcount" | "column" | "schema" | "schemata"
        )
    });
    match (action.as_str(), sub.as_deref()) {
        ("sync" | "verify" | "deduplicate", Some("pipe")) => Step::PipeOperation("pipes:write"),
        ("verify", Some("schema" | "schemata")) => Step::PipeOperation("pipes:write"),
        ("clear", Some("pipe")) => Step::PipeOperation("pipes:delete"),
        ("drop", Some("pipe")) => Step::PipeOperation("pipes:drop"),
        ("delete", Some("pipe")) => Step::PipeOperation("pipes:delete"),
        ("sync", Some("pipeline")) => Step::Pipelines(positional[1..].to_vec()),
        ("show", _) if pipe_sub => Step::Action(Some(Use::Read)),
        ("lock" | "unlock", _) if pipe_sub => Step::Action(Some(Use::Lock)),
        ("edit", Some("pipe"))
            if positional
                .get(1)
                .is_some_and(|w| normalize(w) == "acces" || w == "access") =>
        {
            Step::Action(Some(Use::ManageAccess))
        }
        (_, _) if pipe_sub => Step::Action(Some(Use::Write)),
        _ => Step::Action(None),
    }
}

/// Whether the command needs `actions:execute` (and `jobs:write`): an action other than the
/// console's pipe operations, a scheduled or looping step, or `:` job flags.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExecutePlan {
    /// Whether any step runs an arbitrary action.
    pub needs_actions: bool,
}

/// Check the pipes each step of `sysargs` selects against `caller`'s rights (see the module
/// documentation), and say whether the command needs `actions:execute`. Administrators pass.
pub async fn check(
    state: &AppState,
    caller: &Access,
    sysargs: &[String],
) -> Result<ExecutePlan, ApiError> {
    if caller.no_auth || caller.full_admin() {
        return Ok(ExecutePlan {
            needs_actions: true,
        });
    }
    let mut plan = ExecutePlan::default();
    let steps = crate::execute_policy::command_steps(sysargs)
        .map_err(|message| ApiError::new(403, message))?;
    for command in steps {
        let raw_step = command.words.as_slice();
        let patch = &command.patch;
        let (words, job_flags) = match raw_step.iter().position(|a| a == ":") {
            Some(ix) => (&raw_step[..ix], &raw_step[ix + 1..]),
            None => (raw_step, &raw_step[..0]),
        };
        let Some(action) = words.first() else {
            continue;
        };
        let rest = &words[1..];
        let repeats = !job_flags.is_empty()
            || patch
                .get("loop")
                .is_some_and(meerschaum_core::Value::truthy)
            || patch
                .get("schedule")
                .is_some_and(meerschaum_core::Value::truthy)
            || rest.iter().any(|w| {
                REPEAT_FLAGS
                    .iter()
                    .any(|f| w == f || w.starts_with(&format!("{f}=")))
            });
        let step = classify(action, &positionals(rest));
        match &step {
            Step::PipeOperation(_) | Step::Pipelines(_) if repeats => plan.needs_actions = true,
            Step::Action(_) => plan.needs_actions = true,
            _ => {}
        }
        if let Step::PipeOperation(permission) = &step
            && !caller.has(permission)
        {
            return Err(ApiError::new(
                403,
                format!("Missing required scope: '{permission}'"),
            ));
        }
        if matches!(step, Step::Pipelines(_)) && !caller.has("pipes:write") {
            return Err(ApiError::new(403, "Missing required scope: 'pipes:write'"));
        }
        let instance = selected_values(rest, INSTANCE_FLAGS, patch, "mrsm_instance")
            .and_then(|v| v.into_iter().next())
            .filter(|key| !key.is_empty());
        match step {
            Step::Pipelines(names) => {
                check_pipelines(state, caller, instance.as_deref(), &names).await?;
            }
            Step::PipeOperation(_) => {
                check_selection(state, caller, instance.as_deref(), rest, Use::Write, patch)
                    .await?;
            }
            Step::Action(Some(usage)) => {
                check_selection(state, caller, instance.as_deref(), rest, usage, patch).await?;
            }
            Step::Action(None) => {}
        }
    }
    Ok(plan)
}

/// The keyword arguments of `POST /actions/{action}` (v4's `actions[action](**keywords)`) as the
/// command line they stand for, so the checks of `POST /execute` ([`check`],
/// [`crate::execute_policy::check_non_admin`]) apply to them too: the action, its `action` words,
/// the pipe selection (`connector_keys`, `metric_keys`, `location_keys`, `tags`,
/// `mrsm_instance`), the scheduling keywords (`schedule`, `loop`, `min_seconds`), and every other
/// keyword set to `true` as its flag (`shell: true` is `--shell`).
pub fn sysargs_of_keywords(action: &str, keywords: &meerschaum_core::Map) -> Vec<String> {
    use meerschaum_core::Value;
    let values = |v: &Value| -> Vec<String> {
        match v {
            Value::Null => Vec::new(),
            Value::List(items) => items.iter().map(Value::py_str).collect(),
            other => vec![other.py_str()],
        }
    };
    let mut words = vec![action.to_string()];
    if let Some(v) = keywords.get("action") {
        words.extend(values(v));
    }
    const SELECTION: &[(&str, &str)] = &[
        ("connector_keys", "-c"),
        ("metric_keys", "-m"),
        ("location_keys", "-l"),
        ("tags", "-t"),
        ("mrsm_instance", "-i"),
        ("schedule", "-s"),
        ("min_seconds", "--min-seconds"),
    ];
    for (key, flag) in SELECTION {
        let Some(v) = keywords.get(*key) else {
            continue;
        };
        let given = values(v);
        if given.is_empty() {
            continue;
        }
        words.push(flag.to_string());
        words.extend(given);
    }
    for (key, value) in keywords {
        if key == "action"
            || key == "params"
            || key == "executor_keys"
            || SELECTION.iter().any(|(k, _)| k == key)
        {
            continue;
        }
        if matches!(value, Value::Bool(true)) {
            words.push(format!("--{}", key.replace('_', "-")));
        }
    }
    if let Some(value) = keywords.get("executor_keys").filter(|value| value.truthy()) {
        words.extend(["--executor-keys".into(), value.py_str()]);
    }
    // Keep the canonical chain payload last, as core::chain::parse and both API clients do.
    if let Some(value) = keywords.get("params").filter(|value| !value.is_null()) {
        words.extend(["-P".into(), value.to_json_string()]);
    }
    words
}

/// The instance a step names (`-i`), which must be one this server serves: its pipes' access is
/// what the checks read (`403` naming it otherwise).
fn served_instance(
    state: &AppState,
    instance: Option<&str>,
) -> Result<std::sync::Arc<dyn meerschaum_connector::InstanceConnector>, ApiError> {
    state.get_api_connector(instance).map_err(|e| {
        ApiError::new(
            403,
            format!(
                "You may not use the instance '{}' through this server: {}",
                instance.unwrap_or(&state.config.instance_keys),
                e.detail_text()
            ),
        )
    })
}

async fn check_selection(
    state: &AppState,
    caller: &Access,
    instance: Option<&str>,
    words: &[String],
    usage: Use,
    patch: &meerschaum_core::Map,
) -> Result<(), ApiError> {
    let conn = served_instance(state, instance)?;
    let connector_keys =
        selected_values(words, CONNECTOR_FLAGS, patch, "connector_keys").unwrap_or_default();
    let explicit_mrsm = connector_keys.iter().any(|c| c == "mrsm");
    let query = PipesKeysQuery {
        connector_keys,
        metric_keys: selected_values(words, METRIC_FLAGS, patch, "metric_keys").unwrap_or_default(),
        location_keys: selected_values(words, LOCATION_FLAGS, patch, "location_keys")
            .unwrap_or_default()
            .iter()
            .map(|l| normalize_location(Some(l)))
            .collect(),
        tags: selected_values(words, TAG_FLAGS, patch, "tags").unwrap_or_default(),
        params: None,
    };
    let keys = conn
        .fetch_pipes_keys(&query)
        .await
        .map_err(|e| ApiError::exception("fetch_pipes_keys", e))?;
    let index = crate::access::pipe_index(state, Some(&conn.connector_keys())).await?;
    let mut refused = 0usize;
    for entry in keys.entries() {
        // The CLI leaves the internal `mrsm` pipes out unless `-c mrsm` names them.
        if entry.connector_keys == "mrsm" && !explicit_mrsm {
            continue;
        }
        let lk = normalize_location(entry.location_key.as_deref());
        let Some(p) = index.get(&entry.connector_keys, &entry.metric_key, lk.as_deref()) else {
            continue;
        };
        let allowed = match usage {
            Use::Read => caller.can_see(&p.parameters),
            Use::Write => caller.can_edit(&p.parameters),
            Use::Lock => caller.can_see(&p.parameters) && caller.can_lock(&p.parameters),
            Use::ManageAccess => {
                caller.can_see(&p.parameters) && caller.can_manage_access(&p.parameters)
            }
        };
        if !allowed {
            refused += 1;
        }
    }
    if refused > 0 {
        let what = match usage {
            Use::Read => "see",
            Use::Write => "change",
            Use::Lock => "lock or unlock",
            Use::ManageAccess => "manage the access of",
        };
        return Err(ApiError::new(
            403,
            format!(
                "This command selects {refused} pipe{} you may not {what}. Narrow it with -c, \
                 -m, -l or -t.",
                if refused == 1 { "" } else { "s" }
            ),
        ));
    }
    Ok(())
}

async fn check_pipelines(
    state: &AppState,
    caller: &Access,
    instance: Option<&str>,
    names: &[String],
) -> Result<(), ApiError> {
    let conn = served_instance(state, instance)?;
    for name in names.iter().filter(|n| !n.starts_with('-')) {
        let (definition, _) =
            crate::routes::pipelines::visible_registration(state, caller, &conn, name).await?;
        crate::routes::pipelines::check_members_editable(
            state,
            caller,
            &conn,
            name,
            &definition,
            &PlanOptions::default(),
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn steps_are_classified() {
        assert_eq!(
            classify("sync", &s(&["pipes"])),
            Step::PipeOperation("pipes:write")
        );
        assert_eq!(
            classify("drop", &s(&["pipes"])),
            Step::PipeOperation("pipes:drop")
        );
        assert_eq!(
            classify("clear", &s(&["pipe"])),
            Step::PipeOperation("pipes:delete")
        );
        assert_eq!(
            classify("verify", &s(&["schemata"])),
            Step::PipeOperation("pipes:write")
        );
        assert_eq!(
            classify("sync", &s(&["pipelines", "daily"])),
            Step::Pipelines(vec!["daily".into()])
        );
        assert_eq!(
            classify("show", &s(&["pipes"])),
            Step::Action(Some(Use::Read))
        );
        assert_eq!(classify("show", &s(&["jobs"])), Step::Action(None));
        assert_eq!(
            classify("unlock", &s(&["pipes"])),
            Step::Action(Some(Use::Lock))
        );
        assert_eq!(
            classify("edit", &s(&["pipes", "access"])),
            Step::Action(Some(Use::ManageAccess))
        );
        assert_eq!(
            classify("copy", &s(&["pipes"])),
            Step::Action(Some(Use::Write))
        );
    }

    #[test]
    fn action_keywords_read_as_their_command_line() {
        let keywords = meerschaum_core::Value::from_json_str(
            r#"{"action": ["pipes"], "connector_keys": ["sql:a"], "metric_keys": "w",
                "location_keys": [null], "mrsm_instance": "sql:main", "yes": true,
                "shell": true, "debug": false, "loop": true, "schedule": "every 5 minutes"}"#,
        )
        .unwrap()
        .to_map();
        let words = sysargs_of_keywords("sync", &keywords);
        assert_eq!(&words[..2], &s(&["sync", "pipes"])[..]);
        assert_eq!(flag_values(&words, CONNECTOR_FLAGS).unwrap(), vec!["sql:a"]);
        assert_eq!(flag_values(&words, METRIC_FLAGS).unwrap(), vec!["w"]);
        assert_eq!(
            flag_values(&words, INSTANCE_FLAGS).unwrap(),
            vec!["sql:main"]
        );
        for flag in ["--yes", "--shell", "--loop", "-s"] {
            assert!(words.iter().any(|w| w == flag), "{flag}: {words:?}");
        }
        assert!(!words.iter().any(|w| w == "--debug"), "{words:?}");
        // The console's policy sees the flags and the action.
        assert!(crate::execute_policy::check_non_admin(&words).is_err());
        let show = sysargs_of_keywords(
            "show",
            &meerschaum_core::Value::from_json_str(r#"{"action": ["config"]}"#)
                .unwrap()
                .to_map(),
        );
        assert!(crate::execute_policy::check_non_admin(&show).is_err());
        let sh = sysargs_of_keywords("sh", &meerschaum_core::Map::new());
        assert!(crate::execute_policy::check_non_admin(&sh).is_err());
    }

    #[test]
    fn selection_flags_are_read_in_every_spelling() {
        let words = s(&[
            "pipes",
            "-c",
            "sql:a",
            "sql:b",
            "-msecret",
            "--location-keys=home",
            "-y",
        ]);
        assert_eq!(
            flag_values(&words, CONNECTOR_FLAGS).unwrap(),
            vec!["sql:a", "sql:b"]
        );
        assert_eq!(flag_values(&words, METRIC_FLAGS).unwrap(), vec!["secret"]);
        assert_eq!(flag_values(&words, LOCATION_FLAGS).unwrap(), vec!["home"]);
        assert!(flag_values(&words, TAG_FLAGS).is_none());
        assert_eq!(positionals(&words), vec!["pipes"]);
    }
}
