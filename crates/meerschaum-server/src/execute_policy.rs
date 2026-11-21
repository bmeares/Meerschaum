//! What a non-administrator may run through `POST /execute`.
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

use meerschaum_core::{Map, Value};
use serde_json::{Value as Json, json};

/// One actual action inside a current / legacy chain, with its immediate argument patch.
pub struct CommandStep {
    /// The inline command, with wrapper flags retained after `:` for policy / scope checks.
    pub words: Vec<String>,
    /// Supported native patches. Selection overrides are checked against the same fresh ACLs.
    pub patch: Map,
}

const MAX_CHAIN_DEPTH: usize = 8;
/// The escaped chaining word (`system:arguments:escaped_and_key`).
const ESCAPED_AND: &str = "++";
const MAX_CHAIN_STEPS: usize = 128;
const MAX_CHAIN_WORDS: usize = 4096;
const MAX_CHAIN_BYTES: usize = 1024 * 1024;

fn checked_patch(patch: Option<&Value>) -> Result<Map, String> {
    let Some(patch) = patch.filter(|value| !value.is_null()) else {
        return Ok(Map::new());
    };
    let Some(map) = patch.as_map() else {
        return Err("Remote action-chain patches must be dictionaries.".into());
    };
    let mut accepted = Map::new();
    for (key, value) in map {
        if matches!(
            key.as_str(),
            "connector_keys" | "metric_keys" | "location_keys" | "tags"
        ) {
            if !value.is_null() && !matches!(value, Value::List(_)) {
                return Err(format!(
                    "The action-chain selection '{key}' must be a list or null."
                ));
            }
            accepted.insert(key.clone(), value.clone());
            continue;
        }
        if key == "mrsm_instance" {
            if !value.is_null() && !matches!(value, Value::Str(_)) {
                return Err("The action-chain instance must be a string or null.".into());
            }
            accepted.insert(key.clone(), value.clone());
            continue;
        }
        // Default None / empty values do not grant action or executor privileges.
        if !value.truthy() {
            continue;
        }
        if matches!(key.as_str(), "action" | "sysargs" | "executor_keys") {
            if key == "executor_keys" && value.as_str() == Some("local") {
                continue;
            }
            return Err(format!(
                "Remote action chains cannot patch '{key}'. Put the action in the command line."
            ));
        }
        if key == "params" && value.get(meerschaum_core::chain::LINE_KEY).is_some() {
            return Err("Remote action chains cannot replace a nested chain payload.".into());
        }
        if matches!(value, Value::Bool(_))
            || matches!(key.as_str(), "schedule" | "min_seconds" | "params")
        {
            accepted.insert(key.clone(), value.clone());
            continue;
        }
        return Err(format!(
            "Unsupported action-chain patch '{key}'. Put its flag in the command line."
        ));
    }
    check_step(&crate::execute_access::sysargs_of_keywords(
        "show", &accepted,
    ))?;
    Ok(accepted)
}

/// Expand the real current / v4 chain wire forms through the core's parser and shlex. Both the
/// action policy and object-access checker consume these same bounded steps; neither validates
/// only a wrapper while the native dispatcher executes its hidden commands.
pub fn command_steps(sysargs: &[String]) -> Result<Vec<CommandStep>, String> {
    fn expand(
        words: &[String],
        patch: &Map,
        depth: usize,
        out: &mut Vec<CommandStep>,
        budget: &mut (usize, usize),
    ) -> Result<(), String> {
        budget.0 = budget.0.saturating_add(words.len());
        budget.1 = budget
            .1
            .saturating_add(words.iter().map(String::len).sum::<usize>());
        if depth > MAX_CHAIN_DEPTH || budget.0 > MAX_CHAIN_WORDS || budget.1 > MAX_CHAIN_BYTES {
            return Err("The remote action chain exceeds its validation limit.".into());
        }
        // `++` is the escaped `+` a job turns back into `+` (`Job::new`), so it separates steps
        // here too.
        for step in words
            .split(|word| word == "+" || word == ESCAPED_AND)
            .filter(|step| !step.is_empty())
        {
            if let Some(chain) = meerschaum_core::chain::parse(step) {
                if patch.get("params").is_some_and(Value::truthy) {
                    return Err(
                        "Remote action chains cannot replace a nested chain payload.".into(),
                    );
                }
                let nested_patch = checked_patch(chain.patch.as_ref())?;
                let inner = meerschaum_core::shlex::split(&chain.line)
                    .map_err(|_| "Invalid quoting in the remote action chain.".to_string())?;
                let start = out.len();
                expand(&inner, &nested_patch, depth + 1, out, budget)?;
                // Patches apply to an immediate parsed action. Selection patches on a wrapper
                // do not propagate to its children; the wrapper passes its own nested patch.
                let mut flags = chain.flags;
                let mut patch_flags = crate::execute_access::sysargs_of_keywords("", patch);
                patch_flags.remove(0);
                flags.extend(patch_flags);
                let mut check_flags = vec!["show".into(), "version".into(), ":".into()];
                check_flags.extend(flags.iter().cloned());
                check_step(&check_flags)?;
                if !flags.is_empty() {
                    for item in &mut out[start..] {
                        item.words.push(":".into());
                        item.words.extend(flags.iter().cloned());
                    }
                }
            } else {
                if out.len() >= MAX_CHAIN_STEPS {
                    return Err("The remote action chain contains too many steps.".into());
                }
                out.push(CommandStep {
                    words: step.to_vec(),
                    patch: patch.clone(),
                });
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    expand(sysargs, &Map::new(), 0, &mut out, &mut (0, 0))?;
    Ok(out)
}

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
    // The hidden serialized wrapper can conceal actions and patched arguments from this gate.
    // Ordinary `+` chains remain supported: every visible step is checked below.
    "chain",
];

/// Subactions (as the words the user types, singular, lower case, without `-` and `_`) that a
/// non-administrator may not use after the given action. A word is matched among the step's
/// positional words (not flags nor the values of a flag that takes them), since flags may stand
/// between the action and the subaction.
pub const DENIED_SUBACTIONS: &[(&str, &[&str])] = &[
    // Print the configuration (with connector passwords), the environment, another job's output,
    // or every user's email address (the server shows emails only to administrators and
    // the managers of a user's teams).
    // A masked inventory still includes admin-only addresses and other teams' connector keys.
    // Non-administrators inspect connectors through the scoped API / console instead.
    (
        "show",
        &["config", "connector", "environment", "log", "user"],
    ),
    (
        "edit",
        &[
            "config",
            "definition",
            "plugin",
            "token",
            "user",
            "job",
            "team",
        ],
    ),
    (
        "delete",
        &["config", "connector", "plugin", "user", "venv", "team"],
    ),
    ("copy", &["connector"]),
    // Users and teams are the administrators': the server's `/teams` routes let a team's
    // managers change its members.
    (
        "register",
        &["user", "pipe", "plugin", "token", "connector", "team"],
    ),
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
    // Only boolean shorts may precede another option in a cluster. A value-taking option
    // consumes the remaining text (`-mweather` contains no `-h`, and `-iHOSTV` no `-V`).
    for flag in short.chars() {
        if OMIT_SHORT_FLAGS.contains(&flag) {
            return Some(format!("-{flag}"));
        }
        if !matches!(flag, 'y' | 'f' | 'd' | 'v') {
            break;
        }
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

/// The local CLI is trusted by the storage backend. Remote callers use the dedicated Pipe
/// routes for definitions, ACLs and locks, where the acting principal and SQL guard are applied.
/// Keep ordinary inline metadata edits, parsing both JSON and the CLI's key:value form through
/// the existing shared parser rather than maintaining another parameter syntax here.
fn check_pipe_parameters(step: &[String], patched: Option<&Value>) -> Result<(), String> {
    let Some(action) = step.first() else {
        return Ok(());
    };
    if normalize(action) != "edit"
        || !positionals(&step[1..])
            .iter()
            .any(|word| normalize(word) == "pipe")
    {
        return Ok(());
    }
    let mut parameters = Vec::new();
    for text in crate::execute_access::option_values(step, 'P', "params").unwrap_or_default() {
        parameters.push(meerschaum_core::pyfmt::string_to_dict(&text).map_err(|_| {
            "Invalid remote Pipe parameters. Use the dedicated Pipe API or console.".to_string()
        })?);
    }
    if let Some(value) = patched.filter(|value| !value.is_null()) {
        let Some(map) = value.as_map() else {
            return Err("Remote Pipe parameters must be dictionaries.".into());
        };
        parameters.push(map.clone());
    }
    for parameters in parameters {
        if parameters.contains_key(meerschaum_core::access::ACCESS_KEY)
            || parameters.contains_key(meerschaum_core::access::LOCKS_KEY)
            || parameters.contains_key("fetch")
            || crate::sql_guard::DEFINITION_KEYS
                .iter()
                .any(|path| parameters.contains_key(path[0]))
        {
            return Err(
                "Use the dedicated Pipe API or console to edit SQL definitions, access or locks."
                    .into(),
            );
        }
    }
    Ok(())
}

/// Check one step (the words between `+`), pipeline flags included.
fn check_step(step: &[String]) -> Result<(), String> {
    check_pipe_parameters(step, None)?;
    if crate::execute_access::executor_values(step).is_some_and(|values| {
        values
            .iter()
            .any(|value| !matches!(value.as_str(), "" | "local" | "None"))
    }) {
        return Err("Non-administrators run remote actions with the local executor only.".into());
    }
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
                "'{action} {word}' cannot be run by non-administrators. Use the dedicated API or console."
            ));
        }
    }
    Ok(())
}

/// Check the argument list of a job against the rules for non-administrators. `Err` is the
/// message of the 403.
pub fn check_non_admin(sysargs: &[String]) -> Result<(), String> {
    for step in command_steps(sysargs)? {
        check_step(&step.words)?;
        check_pipe_parameters(&step.words, step.patch.get("params"))?;
        if !step.patch.is_empty() {
            let patch_words =
                crate::execute_access::sysargs_of_keywords(&step.words[0], &step.patch);
            check_step(&patch_words)?;
        }
    }
    Ok(())
}

/// [`check_non_admin`], then what each step's action resolves to in the dispatcher (5.1): a
/// non-administrator runs a step only when it names a Meerschaum action (a core action, or one of
/// a plugin the caller may use) that is not refused. Words that name no action would run as a
/// shell command on the server (the dispatcher's v4 fallthrough), so they are refused here, where
/// the action is resolved by the same code the dispatcher uses
/// ([`ActionsBackend::resolve`](crate::backends::ActionsBackend::resolve)) and not by another name
/// list. `Err` is the message of the 403.
pub async fn check_non_admin_resolved(
    actions: &dyn crate::backends::ActionsBackend,
    caller: &crate::access::Access,
    sysargs: &[String],
) -> Result<(), String> {
    use crate::backends::ActionTarget;
    check_non_admin(sysargs)?;
    for step in command_steps(sysargs)? {
        let words = match step.words.iter().position(|a| a == ":") {
            Some(ix) => &step.words[..ix],
            None => &step.words[..],
        };
        let Some(first) = words.first() else {
            continue;
        };
        match actions.resolve(words).await {
            ActionTarget::Shell => {
                return Err(format!(
                    "'{first}' is not a Meerschaum action. Non-administrators run Meerschaum \
                     actions only, never shell commands."
                ));
            }
            ActionTarget::Action { name, plugin, sub } => {
                // Deny by default: a core command must be one the server can scope.
                if plugin.is_none() && !non_admin_may_run(&name, sub.as_deref()) {
                    let what = match sub.as_deref() {
                        Some(sub) => format!("{name} {sub}"),
                        None => name.clone(),
                    };
                    return Err(format!(
                        "'{what}' is not among the commands non-administrators may run."
                    ));
                }
                let normalized = normalize(&name);
                if CONSOLE_OMIT_ACTIONS
                    .iter()
                    .chain(NON_ADMIN_OMIT_ACTIONS)
                    .any(|omitted| normalize(omitted) == normalized)
                {
                    return Err(format!(
                        "The action '{name}' cannot be run by non-administrators."
                    ));
                }
                if let Some(plugin) = plugin
                    && (!meerschaum_core::access::can_use_plugin(&caller.principal, &plugin)
                        || !crate::executor::source_ceiling::permits_name(caller, &plugin))
                {
                    return Err(format!(
                        "'{first}' is not a Meerschaum action. Non-administrators run Meerschaum \
                         actions only, never shell commands."
                    ));
                }
            }
        }
    }
    Ok(())
}

/// The answer of [`may_run_system_commands`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemCommands {
    /// Yes, anywhere: administrators, and everyone under `--no-auth`.
    Anywhere,
    /// Yes, but only as team execution of `team` ([`crate::team_execution`]): a job of the team
    /// or its terminal, started with the team's configuration, the environment allowlist and the
    /// sandbox applied before the command runs. Never in the API server's process, the shared
    /// workers or a local command line with the server's credentials.
    TeamOnly {
        /// The team.
        team: String,
    },
    /// No: the deny-by-default policy ([`NON_ADMIN_COMMANDS`]) applies. `reason`: why team
    /// execution is not available, for a caller who acts for a team that runs code.
    No {
        /// Why team execution is not available, when it could have been.
        reason: Option<String>,
    },
}

impl SystemCommands {
    /// Whether the caller may run system commands anywhere (an administrator).
    pub fn anywhere(&self) -> bool {
        matches!(self, SystemCommands::Anywhere)
    }
}

/// Whether `caller` may run system commands or arbitrary code on the server: a shell (`sh`,
/// `os`, a word that names no action, `--shell`), `python`, or an interactive program in the web
/// terminal. This is the one place that answers it (5.1): the action routes check a command line
/// with [`check_command`], and the web terminal chooses its shell's mode from it
/// ([`crate::webterm`]). Administrators (and everyone under `--no-auth`, where everyone is one):
/// [`SystemCommands::Anywhere`]. A caller who acts for a team that runs code
/// ([`crate::team_execution::code_running_team`]), when every condition of team execution holds
/// on this server ([`crate::team_execution::status`]): [`SystemCommands::TeamOnly`]. Everyone
/// else: [`SystemCommands::No`], with the failed condition for such a caller.
pub async fn may_run_system_commands(
    state: &crate::state::AppState,
    caller: &crate::access::Access,
) -> SystemCommands {
    if caller.no_auth || caller.full_admin() {
        return SystemCommands::Anywhere;
    }
    let Some(team) = crate::team_execution::code_running_team(caller) else {
        return SystemCommands::No { reason: None };
    };
    let status = crate::team_execution::status(state, &team).await;
    if status.available {
        SystemCommands::TeamOnly { team: team.name }
    } else {
        SystemCommands::No {
            reason: status.reason,
        }
    }
}

/// Where a command line would run, for [`check_command`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Place {
    /// As a job (`POST /execute`, `POST /jobs/{name}`): a team job for a member of an isolated
    /// team.
    Job,
    /// In the API server's own process (`POST /actions/{action}`, MCP `execute_action`, the
    /// restricted web terminal's shell).
    Server,
}

/// What a walled-off caller hears where a command would run in the API server's process.
pub const SERVER_PROCESS_REFUSAL: &str = "This runs inside the API server's process, outside \
    your team's sandbox, so members of isolated teams cannot use it. Run the command with \
    `POST /execute` (the console's Execute box) instead: it runs as a job of your team.";

/// The commands a caller who may not run system commands may run (5.1, deny by default): core
/// actions as `(action, subactions)`, the subaction as the dispatcher resolves it (`""`: the
/// action with no subaction). Each one is one the server can scope to the caller:
/// [`crate::execute_access::check`] then checks every object it selects (pipes, Pipelines, jobs,
/// tokens) with the permissions of the matching route. Any other core action or subaction is
/// refused ([`NON_ADMIN_REFUSED`]); a plugin's action follows the plugin's grants instead. A new
/// native action is refused until it is added here deliberately (`meerschaum-cli`'s
/// `non_admin_commands` test fails while an action is on neither list).
pub const NON_ADMIN_COMMANDS: &[(&str, &[&str])] = &[
    // Informational: nothing about other users' objects.
    ("help", &[""]),
    (
        "show",
        &[
            "version",
            "help",
            "actions",
            "arguments",
            "schedules",
            // Pipes: every selected pipe readable (`pipes:read`).
            "pipes",
            "data",
            "columns",
            "rowcounts",
            "sizes",
            "partitions",
            "targets",
            "tags",
            // Pipelines named, readable with their members (`pipes:read`).
            "pipelines",
            // Jobs named, visible (`jobs:read`).
            "jobs",
            "daemons",
            // The caller's own tokens.
            "tokens",
        ],
    ),
    // Pipe operations: every selected pipe changeable, with the operation's permission.
    ("sync", &["pipes", "pipelines"]),
    ("verify", &["pipes", "rowcounts", "schemas"]),
    ("deduplicate", &["pipes"]),
    ("clear", &["pipes", "pipelines"]),
    ("drop", &["pipes", "pipelines"]),
    ("delete", &["pipes", "pipelines", "jobs"]),
    ("lock", &["pipes"]),
    ("unlock", &["pipes"]),
    ("edit", &["pipes"]),
    ("partition", &["pipes"]),
    ("tag", &["pipes"]),
    // Jobs named, visible, with the job route's permission.
    ("attach", &["jobs", "logs"]),
    ("watch", &["jobs", "logs"]),
    ("stop", &["jobs", "daemons"]),
    ("pause", &["jobs"]),
    ("restart", &["jobs", "daemons"]),
];

/// The native commands refused to a caller who may not run system commands: every core action
/// and subaction not in [`NON_ADMIN_COMMANDS`], listed so that the two lists together are the
/// whole CLI (`meerschaum-cli`'s `non_admin_commands` test). Being listed here refuses nothing by
/// itself: anything not allowed is refused.
pub const NON_ADMIN_REFUSED: &[(&str, &[&str])] = &[
    ("attach", &[""]),
    ("clear", &["", "cache"]),
    ("deduplicate", &[""]),
    ("login", &[""]),
    ("os", &[""]),
    ("pause", &[""]),
    (
        "register",
        &[
            "",
            "connectors",
            "pipes",
            "pipelines",
            "plugins",
            "teams",
            "tokens",
            "users",
        ],
    ),
    ("reload", &[""]),
    ("restart", &[""]),
    ("setup", &["", "plugins"]),
    ("sh", &[""]),
    ("tag", &[""]),
    ("verify", &["", "packages", "plugins", "venvs"]),
    (
        "install",
        &[
            "",
            "completions",
            "drivers",
            "packages",
            "plugins",
            "required",
        ],
    ),
    ("sync", &["", "pipeline"]),
    ("api", &["", "start"]),
    (
        "bootstrap",
        &[
            "",
            "connectors",
            "jobs",
            "pipes",
            "pipelines",
            "plugins",
            "teams",
        ],
    ),
    ("drop", &["", "indices"]),
    ("flush", &["", "indices", "pipes"]),
    ("stack", &[""]),
    ("stop", &["", "pipelines"]),
    (
        "uninstall",
        &["", "completions", "drivers", "packages", "plugins"],
    ),
    ("vacuum", &["", "pipes"]),
    ("prune", &["", "venvs"]),
    ("plan", &["", "pipes", "pipelines"]),
    ("doctor", &[""]),
    ("backup", &["", "postgres"]),
    ("restore", &["", "postgres"]),
    ("watch", &["", "runs"]),
    ("lock", &[""]),
    ("unlock", &[""]),
    (
        "show",
        &[
            "",
            "completions",
            "config",
            "connectors",
            "drivers",
            "environment",
            "graph",
            "logs",
            "packages",
            "plugins",
            "runs",
            "teams",
            "users",
            "venvs",
        ],
    ),
    (
        "delete",
        &[
            "",
            "cache",
            "config",
            "connectors",
            "plugins",
            "teams",
            "users",
            "venvs",
        ],
    ),
    ("partition", &[""]),
    (
        "start",
        &[
            "",
            "api",
            "executor",
            "connectors",
            "daemons",
            "gui",
            "jobs",
            "mcp",
            "pipeline",
            "pipelines",
            "webterm",
            "worker",
        ],
    ),
    ("analyze", &["", "pipes", "pipelines"]),
    ("compress", &["", "pipes"]),
    ("copy", &["", "connectors", "pipes", "pipelines"]),
    ("decompress", &["", "pipes"]),
    (
        "edit",
        &[
            "",
            "config",
            "definition",
            "jobs",
            "pipelines",
            "plugins",
            "teams",
            "tokens",
            "users",
        ],
    ),
    ("index", &["", "pipes"]),
    ("sql", &[""]),
    ("python", &[""]),
    (
        "upgrade",
        &[
            "",
            "extensions",
            "meerschaum",
            "packages",
            "plugins",
            "postgres",
        ],
    ),
    ("compose", &[""]),
];

/// `show` / `pipes` as the lists compare them ([`normalize`]): lower case, no `-` or `_`,
/// singular.
fn same_word(a: &str, b: &str) -> bool {
    normalize(a) == normalize(b)
}

/// Whether a caller who may not run system commands may run the core action `action` with the
/// subaction `sub` ([`NON_ADMIN_COMMANDS`]).
pub fn non_admin_may_run(action: &str, sub: Option<&str>) -> bool {
    let sub = sub.unwrap_or("");
    NON_ADMIN_COMMANDS.iter().any(|(allowed, subs)| {
        same_word(allowed, action)
            && subs
                .iter()
                .any(|s| (s.is_empty() && sub.is_empty()) || (!s.is_empty() && same_word(s, sub)))
    })
}

/// [`NON_ADMIN_COMMANDS`] as the Markdown table `permissions.md` shows (a test keeps the page
/// equal to it).
pub fn non_admin_commands_markdown() -> String {
    let mut out = String::from("| Action | Subactions |\n|---|---|\n");
    for (action, subs) in NON_ADMIN_COMMANDS {
        let subs = if subs == &[""] {
            "(none)".to_string()
        } else {
            subs.iter()
                .map(|s| format!("`{s}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        out.push_str(&format!("| `{action}` | {subs} |\n"));
    }
    out
}

/// The check every route that runs a command line applies to its caller, for a command that
/// runs at `place`: nothing for an administrator; for a walled-off caller, a refusal where it
/// would run in the server's process ([`SERVER_PROCESS_REFUSAL`]); nothing for team execution
/// ([`SystemCommands::TeamOnly`]) of a job (the job is the team's, sandboxed: its launch requires
/// the sandbox); else [`check_non_admin_resolved`], naming why team execution is not available
/// when it could have been. `Err` is the message of the 403.
pub async fn check_command(
    state: &crate::state::AppState,
    caller: &crate::access::Access,
    sysargs: &[String],
    place: Place,
) -> Result<(), String> {
    let answer = may_run_system_commands(state, caller).await;
    if answer.anywhere() {
        return Ok(());
    }
    if place == Place::Server && caller.principal.is_isolated() {
        return Err(SERVER_PROCESS_REFUSAL.to_string());
    }
    if place == Place::Job && matches!(answer, SystemCommands::TeamOnly { .. }) {
        return Ok(());
    }
    check_non_admin_resolved(state.actions.as_ref(), caller, sysargs)
        .await
        .map_err(|message| match answer {
            SystemCommands::No {
                reason: Some(reason),
            } => format!("{message} Your team's commands cannot run here: {reason}."),
            _ => message,
        })
}

fn plain_argument(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value.starts_with('-')
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

/// Admit one explicit source and a new destination pipe, using Main-served SQL instances.
fn bound_copy_instances(sysargs: &[String]) -> Option<(&str, &str)> {
    let [
        verb,
        subject,
        destination,
        instance_flag,
        source,
        connector_flag,
        connector,
        metric_flag,
        metric,
        location_flag,
        location,
        sync_flag,
        mode,
        approval,
        rest @ ..,
    ] = sysargs
    else {
        return None;
    };
    use meerschaum_core::team_scope::api_execution::split_keys;
    let sql_keys =
        |keys: &str| keys.len() <= 128 && split_keys(keys).is_ok_and(|(kind, _)| kind == "sql");
    let pipe_connector = connector
        .split_once(':')
        .is_some_and(|(kind, label)| matches!(kind, "manual" | "sql") && plain_argument(label));
    (verb == "copy"
        && subject == "pipes"
        && instance_flag == "-i"
        && connector_flag == "-c"
        && metric_flag == "-m"
        && location_flag == "-l"
        && sync_flag == "--sync-data"
        && mode == "all"
        && matches!(approval.as_str(), "--yes" | "-y")
        && (rest.is_empty() || rest == ["--nopretty"])
        && source != destination
        && sql_keys(source)
        && sql_keys(destination)
        && pipe_connector
        && plain_argument(metric)
        && plain_argument(location))
    .then_some((source, destination))
}

/// Match the same built-in commands at Main admission and inside the confined CLI.
/// Keep these forms separate from the legacy in-process command permissions.
pub fn bound_command_scopes(sysargs: &[String]) -> Option<&'static [&'static str]> {
    let [action, subject, arguments @ ..] = sysargs else {
        return None;
    };
    if let (action @ ("register" | "edit"), "pipelines", [name, flag, params]) =
        (action.as_str(), subject.as_str(), arguments)
        && matches!(flag.as_str(), "--params" | "-P")
        && meerschaum_core::pipeline::validate_name(name).is_ok()
        && let Ok(value) = serde_json::from_str::<Json>(params)
        && let Some(fields) = value.get("pipeline").unwrap_or(&value).as_object()
        && fields
            .get("name")
            .is_none_or(|value| value.as_str() == Some(name))
    {
        return match action {
            "register" => Some(&["pipes:read", "pipes:register", "pipes:write"]),
            "edit"
                if fields
                    .get("revision")
                    .and_then(Json::as_u64)
                    .is_some_and(|revision| revision < i64::MAX as u64) =>
            {
                Some(&["pipes:read", "pipes:write"])
            }
            _ => None,
        };
    }
    if bound_copy_instances(sysargs).is_some() {
        return Some(&[
            "connectors:read",
            "pipes:read",
            "pipes:register",
            "pipes:write",
        ]);
    }
    let (plain, arguments) = match arguments.split_last() {
        Some((flag, rest)) if flag == "--nopretty" => (true, rest),
        _ => (false, arguments),
    };
    match (action.as_str(), subject.as_str(), arguments) {
        ("show", "graph" | "pipes" | "runs", []) => Some(&["pipes:read"]),
        ("show" | "watch", "run", [id]) if plain_argument(id) => Some(&["pipes:read"]),
        ("watch", "runs", []) => Some(&["pipes:read"]),
        ("show", "logs", [job]) if plain && plain_argument(job) => {
            Some(&["jobs:read", "logs:read"])
        }
        _ => None,
    }
}

/// Check the original caller's capabilities for a built-in command on an API-only profile.
pub fn check_bound_command(
    state: &crate::state::AppState,
    caller: &crate::access::Access,
    sysargs: &[String],
) -> Result<bool, crate::error::ApiError> {
    let Some(scopes) = bound_command_scopes(sysargs) else {
        return Ok(false);
    };
    caller.require(&["actions:execute"], &[])?;
    caller.require(scopes, &[])?;
    if let Some((source, destination)) = bound_copy_instances(sysargs) {
        for keys in [source, destination] {
            crate::access::guard::check_connector_teams(state, caller, keys)?;
            let connector = state.get_api_connector(Some(keys))?;
            if connector.connector_type() != "sql" || connector.connector_keys() != keys {
                return Err(crate::error::ApiError::new(
                    403,
                    "Copy requires native SQL instances served by this Main.",
                ));
            }
        }
    }
    Ok(true)
}

/// This route can admit a new bound command only when the native backend will create
/// an API-profile job for this caller. Its canonical launch checks the command again.
pub(crate) async fn check_bound_job_command(
    state: &crate::state::AppState,
    caller: &crate::access::Access,
    sysargs: &[String],
) -> Result<bool, crate::error::ApiError> {
    use crate::backends::ApiExecution;
    if !matches!(caller.api_execution, ApiExecution::Tracked { .. })
        || !state.jobs.supports_bound_commands()
        || crate::team_execution::code_running_team(caller).is_some()
        || !matches!(
            may_run_system_commands(state, caller).await,
            SystemCommands::No { .. }
        )
    {
        return Ok(false);
    }
    check_bound_command(state, caller, sysargs)
}

/// Everything `POST /execute` decides about running `sysargs` for `caller` (who already holds
/// `jobs:execute`), apart from the job's name: the pipes each step selects
/// ([`crate::execute_access::check`]), `actions:execute` and `jobs:write` for anything but the
/// console's pipe operations, the server's `api:permissions:actions:non_admin`, and
/// [`check_command`]. The restricted web terminal asks the same question for every line its
/// shell runs ([`crate::webterm`]), so the terminal allows exactly what the route allows.
pub async fn authorize_command(
    state: &crate::state::AppState,
    caller: &crate::access::Access,
    sysargs: &[String],
    place: Place,
) -> Result<crate::execute_access::ExecutePlan, crate::error::ApiError> {
    use crate::error::ApiError;
    let plan = crate::execute_access::check(state, caller, sysargs).await?;
    if plan.needs_actions {
        caller.require(&["actions:execute", "jobs:write"], &[])?;
        if !caller.no_auth && !caller.full_admin() {
            let allowed =
                crate::auth::is_user_allowed_to_execute_console(state, caller.auth.as_ref()).await;
            if !allowed.success {
                return Err(ApiError::new(403, allowed.message));
            }
        }
    } else if !caller.no_auth && !caller.is_admin() && !state.config.actions_non_admin {
        return Err(ApiError::new(
            403,
            "The administrator for this server has not allowed users to perform actions.",
        ));
    }
    check_command(state, caller, sysargs, place)
        .await
        .map_err(|message| ApiError::new(403, message))?;
    Ok(plan)
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
            "-vyV",
        ] {
            assert!(omitted_flag(flag).is_some(), "{flag}");
        }
        for flag in [
            "--debug",
            "--yes",
            "-y",
            "-c",
            "-csql:main",
            "-mweather",
            "-ymweather",
            "-vymweather",
            "-iHOSTV",
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
        // Users' emails and teams are the administrators'.
        assert!(check_non_admin(&s(&["show", "users"])).is_err());
        assert!(check_non_admin(&s(&["register", "teams", "data"])).is_err());
        assert!(check_non_admin(&s(&["edit", "team", "data"])).is_err());
        assert!(check_non_admin(&s(&["show", "teams"])).is_ok());
        // The values of a flag are not subactions.
        assert!(
            check_non_admin(&s(&["show", "pipes", "-m", "config", "logs", "-l", "None"])).is_ok()
        );
        assert!(check_non_admin(&s(&["show", "pipes", "-i", "config"])).is_ok());
    }

    #[test]
    fn connector_inventory_cannot_hide_in_aliases_flags_or_chains() {
        for args in [
            s(&["show", "connector"]),
            s(&["show", "connectors", "sql:private"]),
            s(&["show", "ConNectors"]),
            s(&["show", "--nopretty", "connectors"]),
            s(&["show", "-i", "sql:served", "connector"]),
            s(&["show", "pipes", "+", "show", "connectors"]),
            s(&["show", "connectors", ":", "--loop"]),
            s(&["chain", "-P", r#"{"sub_args_line":"show connectors"}"#]),
            s(&[
                "chain",
                "x2",
                "-P",
                r#"{"sub_args_line":"show version","patch_args":{"action":["show","connectors"]}}"#,
            ]),
        ] {
            assert!(check_non_admin(&args).is_err(), "{args:?}");
        }
        // An ordinary Pipe's metric, tag or instance name is not an inspection subaction.
        for args in [
            s(&["show", "pipes", "-m", "connectors", "-t", "connector"]),
            s(&["show", "pipes", "-i", "connectors"]),
            s(&["show", "pipes", "+", "show", "version"]),
        ] {
            assert!(check_non_admin(&args).is_ok(), "{args:?}");
        }
        assert!(
            policy_json()["denied_subactions"]["show"]
                .as_array()
                .unwrap()
                .contains(&json!("connector"))
        );
    }

    #[test]
    fn safe_legacy_chains_and_defaults_are_validated_with_bounded_expansion() {
        for prefix in [vec!["chain"], vec!["start", "pipeline"]] {
            let payload = r#"{"sub_args_line":"show pipes -i sql:served -c plugin:public -m readings + show version","patch_args":{"debug":true,"executor_keys":null,"action":null}}"#;
            let mut words = s(&prefix);
            words.extend(s(&["-P", payload]));
            assert!(check_non_admin(&words).is_ok(), "{words:?}");
            assert_eq!(command_steps(&words).unwrap().len(), 2);
        }
        for executor in [
            "--executor-keys=api:private",
            "--exec=api:private",
            "-eapi:private",
            "-yeapi:private",
        ] {
            assert!(
                check_non_admin(&s(&["show", "version", executor])).is_err(),
                "{executor}"
            );
        }
        assert!(check_non_admin(&s(&["show", "-e", "local", "version"])).is_ok());
        assert!(check_non_admin(&s(&["show", "pipes", "-mweather"])).is_ok());
        let mut nested = "show version".to_string();
        for _ in 0..MAX_CHAIN_DEPTH + 2 {
            let payload = json!({"sub_args_line":nested}).to_string();
            nested = meerschaum_core::shlex::join(&s(&["chain", "-P", &payload]));
        }
        assert!(check_non_admin(&meerschaum_core::shlex::split(&nested).unwrap()).is_err());
        let steps = std::iter::repeat_n(s(&["show", "version", "+"]), MAX_CHAIN_STEPS + 1)
            .flatten()
            .collect::<Vec<_>>();
        assert!(check_non_admin(&steps).is_err());
        assert!(
            check_non_admin(&s(&["show", "version", &"x".repeat(MAX_CHAIN_BYTES + 1)])).is_err()
        );
    }

    #[test]
    fn remote_pipe_registration_and_sensitive_inline_edits_use_dedicated_routes() {
        for words in [
            s(&["register", "pipe"]),
            s(&["register", "pipes", "-P", r#"{"access":{"owner":"alice"}}"#]),
            s(&["edit", "definition"]),
            s(&["edit", "pipes", "definition"]),
        ] {
            assert!(check_non_admin(&words).is_err(), "{words:?}");
        }
        for params in [
            r#"{"fetch":{"definition":"SELECT 42"}}"#,
            r#"{"definition":null}"#,
            r#"{"query":"SELECT 42"}"#,
            r#"{"sql":"SELECT 42"}"#,
            r#"{"access":null}"#,
            r#"{"locks":[]}"#,
            "fetch:{'definition': 'SELECT 42'}",
            "access:{'owner':'alice'}",
            "locks:[]",
        ] {
            for flag in ["-P", "--params", "--par", "-vyP"] {
                let words = if flag == "-P" || flag == "--params" {
                    s(&["edit", "pipes", flag, params])
                } else {
                    s(&["edit", "pipes", &format!("{flag}={params}")])
                };
                assert!(check_non_admin(&words).is_err(), "{words:?}");
            }
        }
        for params in [
            r#"{"tags":["daily"],"description":"notes"}"#,
            "tags:['daily']",
        ] {
            assert!(check_non_admin(&s(&["edit", "pipes", "-P", params])).is_ok());
        }
        // Read filters are not parameter writes, and a benign immediate chain patch stays valid.
        assert!(check_non_admin(&s(&["show", "pipes", "-P", r#"{"sql":"value"}"#])).is_ok());
        for (params, allowed) in [
            (json!({"tags":["daily"]}), true),
            (json!({"fetch":{"definition":"SELECT 42"}}), false),
            (json!({"locks":[]}), false),
        ] {
            let payload =
                json!({"sub_args_line":"edit pipes", "patch_args":{"params":params}}).to_string();
            assert_eq!(
                check_non_admin(&s(&["start", "pipeline", "-P", &payload])).is_ok(),
                allowed,
                "{payload}"
            );
        }
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

    // `may_run_system_commands` and `check_command` need a server state: their tests are in
    // `tests/team_execution_policy.rs` (5.1).

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
