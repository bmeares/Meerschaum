//! The interactive Meerschaum shell, ported from `meerschaum/_internal/shell/` onto `reedline`.
//!
//! - the banner (`shell.<charset>.intro` plus the right-aligned version, colored with
//!   `shell.ansi.intro.rich.style`), the cached update messages ([`updates`]) and the close
//!   message;
//! - the prompt (`shell.<charset>.prompt` with `{username}`, `{instance}`, `{compose}` and
//!   `{executor_keys}`; every literal character colored with `shell.ansi.prompt`, the values
//!   truncated to a quarter of the console width, including v4's truncation of colored values);
//! - `precmd`: `?` help, `exit`/`quit`/`EOF`, `--help`, `:` pipeline flags and `+` chains passed
//!   through as one command line, a leading `mrsm` stripped, unknown actions run with `sh`
//!   (`!` escapes too), the shell's instance / repository / executor added to each step when they
//!   differ from the configured defaults, the debug flag, `print_tuple(skip_common=not debug)`;
//! - the `instance`, `repo`, `executor`, `debug`, `daemon`, `help`, `exit` commands and `clear`;
//! - history in `$MRSM_ROOT_DIR/.mrsm_history`, read and appended in prompt_toolkit's
//!   `FileHistory` format (`shell.max_history` entries kept in memory);
//! - the prompt_toolkit session ([`editor`]): the completion menu while typing with v4's
//!   candidates ([`completer`]), Tab / Shift-Tab / arrow cycling, `ValidAutoSuggest` ghost text,
//!   the bottom toolbar ([`toolbar`]), reverse-i-search, Ctrl-C / Ctrl-D;
//! - the version update check thread ([`updates`]).
//!
//! The CLI daemon (`daemon`, `system.experimental.cli_daemon`) does not exist in v5: actions
//! run natively in the `mrsm` process, so there is no Python process to keep warm. The `daemon`
//! command keeps v4's toggle and message and says so when it is turned on; the toolbar keeps
//! showing the instance (v4 shows `daemon` there while the mode is on).

pub mod completer;
pub mod editor;
pub mod toolbar;
pub mod updates;

use std::io::Write;
use std::path::PathBuf;

use meerschaum_core::value::Value;

use crate::actions::{self, CORE_ACTIONS};
use crate::builtin;
use crate::dispatch::{self, Runtime};
use crate::format::{Formatter, TupleArgs, remove_ansi};
use crate::parse::{parse_arguments, parse_line};
use crate::spec;
use crate::sysargs::{parse_dict_to_sysargs, split_chained_sysargs, split_pipeline_sysargs};
use crate::value::{ArgsMap, PyValue};

pub use completer::SHELL_COMMANDS;

/// The values the prompt shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptState {
    /// The instance keys.
    pub instance: String,
    /// The username of the instance connector.
    pub username: String,
    /// The Compose project name, if running inside one.
    pub compose: Option<String>,
    /// The executor keys.
    pub executor_keys: String,
}

impl PromptState {
    /// The prompt values for an instance (the username comes from the connector's config).
    pub fn new(f: &Formatter<'_>, instance: &str, compose: Option<&str>) -> PromptState {
        PromptState {
            instance: instance.to_string(),
            username: instance_username(f, instance),
            compose: compose.filter(|c| !c.is_empty()).map(str::to_string),
            executor_keys: "local".into(),
        }
    }
}

/// `conn_attrs['username']` (or the username of `conn_attrs['uri']`) for an instance, else
/// `(no username)`.
pub fn instance_username(f: &Formatter<'_>, instance: &str) -> String {
    let none = "(no username)".to_string();
    let Some((typ, label)) = instance.split_once(':') else {
        return none;
    };
    let connectors = f.config().get(&["meerschaum", "connectors", typ]);
    let Some(Value::Map(conns)) = connectors else {
        return none;
    };
    let Some(Value::Map(attrs)) = conns.get(label) else {
        return none;
    };
    let mut merged = match conns.get("default") {
        Some(Value::Map(d)) => d.clone(),
        _ => Default::default(),
    };
    for (k, v) in attrs {
        merged.insert(k.clone(), v.clone());
    }
    if let Some(u) = merged.get("username") {
        return u.py_str();
    }
    if let Some(Value::Str(uri)) = merged.get("uri") {
        return uri
            .split_once("://")
            .and_then(|(_, rest)| rest.rsplit_once('@'))
            .map(|(userinfo, _)| userinfo.split(':').next().unwrap_or("").to_string())
            .unwrap_or(none);
    }
    none
}

/// `truncate_text_for_display(text, max_length, suffix)` (lengths in code points, ANSI codes
/// included).
pub fn truncate_text_for_display(text: &str, max_length: usize, suffix: &str) -> String {
    let len = text.chars().count();
    if len <= max_length {
        return text.to_string();
    }
    let keep = max_length.saturating_sub(suffix.chars().count());
    format!("{}{suffix}", text.chars().take(keep).collect::<String>())
}

/// `get_shell_intro(with_color)`.
pub fn shell_intro(f: &Formatter<'_>, with_color: bool, version: &str) -> String {
    let mut intro = shell_config_str(f, &["shell", f.charset(), "intro"]);
    let width = crate::format::string_width(&intro);
    let tag = format!("v{version}");
    intro.push('\n');
    intro.push_str(&" ".repeat(width.saturating_sub(tag.chars().count())));
    intro.push_str(&tag);
    if !with_color || !f.settings.ansi {
        return intro;
    }
    let style = shell_config_str(f, &["shell", "ansi", "intro", "rich", "style"]);
    f.colored(&intro, &style)
}

fn shell_config_str(f: &Formatter<'_>, keys: &[&str]) -> String {
    f.config().get(keys).map(|v| v.py_str()).unwrap_or_default()
}

/// The prompt template (`shell.<charset>.prompt`, with `{compose}` added to pre-4.0 prompts).
pub fn prompt_template(f: &Formatter<'_>) -> String {
    let mut prompt = shell_config_str(f, &["shell", f.charset(), "prompt"]);
    if !prompt.contains("{compose}") {
        if prompt.contains(" | {executor_keys}") {
            prompt = prompt.replace(" | {executor_keys}", "{compose}");
        } else {
            prompt.push_str("{compose}");
        }
    }
    prompt
}

/// `Shell.update_prompt()`: the rendered prompt string.
pub fn render_prompt(f: &Formatter<'_>, state: &PromptState) -> String {
    let template = prompt_template(f);
    let ansi = f.settings.ansi;
    let style = |key: &str| shell_config_str(f, &["shell", "ansi", key, "rich", "style"]);
    let suffix = if f.settings.unicode { "…" } else { "..." };
    let max_length = f.console().width / 4;
    let mut mask: String = template.clone();
    let blank = |name: &str| "\0".repeat(name.chars().count());

    let mut instance = String::new();
    if template.contains("{instance}") {
        instance = state.instance.clone();
        if ansi {
            instance = f.colored(&instance, &style("instance"));
        }
        mask = mask.replace("{instance}", &blank("{instance}"));
    }
    let mut username = String::new();
    if template.contains("{username}") {
        username = if ansi {
            f.colored(&state.username, &style("username"))
        } else {
            state.username.clone()
        };
        mask = mask.replace("{username}", &blank("{username}"));
    }
    let mut compose = String::new();
    if template.contains("{compose}") {
        let name =
            truncate_text_for_display(state.compose.as_deref().unwrap_or(""), max_length, suffix);
        compose = if name.is_empty() {
            String::new()
        } else if !ansi {
            format!(" | {name}")
        } else {
            format!(
                "{}{}",
                f.colored(" | ", &style("prompt")),
                f.colored(&name, &style("compose"))
            )
        };
        mask = mask.replace("{compose}", &blank("{compose}"));
    }
    let mut executor = String::new();
    if template.contains("{executor_keys}") {
        executor = if ansi {
            f.colored(&remove_ansi(&state.executor_keys), &style("executor"))
        } else {
            state.executor_keys.clone()
        };
        mask = mask.replace("{executor_keys}", &blank("{executor_keys}"));
    }
    let mut remainder: Vec<String> = template.chars().map(|c| c.to_string()).collect();
    for (i, c) in mask.chars().enumerate() {
        if c != '\0' {
            let mut s = c.to_string();
            if ansi {
                s = f.colored(&s, &style("prompt"));
            }
            if i < remainder.len() {
                remainder[i] = s;
            }
        }
    }
    remainder
        .concat()
        .replace("{compose}", &compose)
        .replace(
            "{username}",
            &truncate_text_for_display(&username, max_length, suffix),
        )
        .replace(
            "{instance}",
            &truncate_text_for_display(&instance, max_length, suffix),
        )
        .replace(
            "{executor_keys}",
            &truncate_text_for_display(&executor, max_length, suffix),
        )
}

/// `get_env_project_name()`: the Compose project this process runs in.
pub fn env_project_name(env: fn(&str) -> Option<String>) -> Option<String> {
    let raw = env("MRSM__COMPOSE_CONFIG")?;
    let config = Value::from_json_str(&raw).ok()?;
    if let Some(name) = config.get("project_name").filter(|v| v.truthy()) {
        return Some(name.py_str());
    }
    let file = config.get("__file__")?.as_str()?.to_string();
    PathBuf::from(file)
        .parent()
        .and_then(|p| p.file_stem())
        .map(|s| s.to_string_lossy().into_owned())
}

/// The shell's mutable state (`shell_attrs`).
#[derive(Debug, Clone)]
pub struct ShellState {
    /// The instance for the session.
    pub instance_keys: String,
    /// The repository for the session.
    pub repo_keys: String,
    /// The executor for the session.
    pub executor_keys: String,
    /// `debug` mode.
    pub debug: bool,
    /// `daemon` mode (`system.experimental.cli_daemon`; v5 has no CLI daemon, see the module
    /// docs).
    pub daemon: bool,
}

impl ShellState {
    /// The defaults from the config registry.
    pub fn new(rt: &mut Runtime<'_>, instance: Option<&str>) -> ShellState {
        let config = rt.config();
        let cli_daemon = config
            .get(&["system", "experimental", "cli_daemon"])
            .is_some_and(|v| v.truthy());
        let instance_default = config
            .get(&["meerschaum", "instance"])
            .map(|v| v.py_str())
            .unwrap_or_else(|| "sql:main".into());
        let repo = config
            .get(&["meerschaum", "repository"])
            .map(|v| v.py_str())
            .unwrap_or_else(|| "api:mrsm".into());
        ShellState {
            instance_keys: instance.map(str::to_string).unwrap_or(instance_default),
            repo_keys: repo,
            executor_keys: executor_keys_from_context(rt.env),
            debug: false,
            daemon: cli_daemon,
        }
    }
}

/// `get_executor_keys_from_context()`: `systemd` inside a systemd job, else `local`.
pub fn executor_keys_from_context(env: fn(&str) -> Option<String>) -> String {
    if env("MRSM_SYSTEMD_LOG_PATH").is_some()
        || env("INVOCATION_ID").is_some_and(|_| env("MRSM_DAEMON_ID").is_some())
    {
        "systemd".into()
    } else {
        "local".into()
    }
}

/// What `precmd` decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    /// Keep reading lines.
    Continue,
    /// Leave the shell.
    Exit,
}

fn info(rt: &mut Runtime<'_>, msg: &str) {
    let text = rt.with_formatter(|f| f.info(msg, true));
    rt.print(&text);
}

/// `clear_screen()`: ANSI clear and home (rich's `console.clear()`), or `clear`.
pub fn clear_screen(rt: &mut Runtime<'_>, debug: bool) {
    // `dprint("Skipping screen clear.")` in debug mode.
    if debug {
        return;
    }
    let enabled = rt
        .config()
        .get(&["shell", "clear_screen"])
        .is_none_or(|v| v.truthy());
    if !enabled {
        return;
    }
    let ansi = rt.settings().ansi;
    if ansi && !cfg!(windows) {
        rt.print("\x1b[2J\x1b[H");
    } else {
        let _ = rt.host.run(&dispatch::CommandSpec {
            program: if cfg!(windows) { "cls" } else { "clear" }.into(),
            ..dispatch::CommandSpec::default()
        });
    }
}

fn toggle(state: bool, action: &[String], rt: &mut Runtime<'_>) -> bool {
    let word = action.first().map(String::as_str).unwrap_or("");
    match word.to_lowercase().as_str() {
        "" => !state,
        "on" | "true" => true,
        "off" | "false" => false,
        _ => {
            info(rt, &format!("Unknown state '{word}'. Ignoring..."));
            state
        }
    }
}

fn connector_configured(rt: &mut Runtime<'_>, keys: &str) -> bool {
    let Some((typ, label)) = keys.split_once(':') else {
        return false;
    };
    matches!(
        rt.config().get(&["meerschaum", "connectors", typ, label]),
        Some(Value::Map(_))
    )
}

/// Run a shell command (`do_instance`, `do_repo`, ...).
pub fn shell_command(
    name: &str,
    action: &[String],
    state: &mut ShellState,
    rt: &mut Runtime<'_>,
) -> meerschaum_core::SuccessTuple {
    use meerschaum_core::SuccessTuple;
    match name {
        "instance" => {
            let mut keys = action.first().cloned().unwrap_or_default();
            if keys.is_empty() {
                keys = rt
                    .config()
                    .get(&["meerschaum", "instance"])
                    .map(|v| v.py_str())
                    .unwrap_or_else(|| "sql:main".into());
            }
            if !keys.contains(':') {
                keys.push_str(":main");
            }
            let conn_keys = if connector_configured(rt, &keys) {
                keys
            } else {
                rt.config()
                    .get(&["meerschaum", "instance"])
                    .map(|v| v.py_str())
                    .unwrap_or_else(|| "sql:main".into())
            };
            state.instance_keys = conn_keys.clone();
            info(
                rt,
                &format!("Default instance for the current shell: {conn_keys}"),
            );
            SuccessTuple::success()
        }
        "repo" => {
            let mut keys = action.first().cloned().unwrap_or_default();
            if keys.is_empty() {
                keys = rt
                    .config()
                    .get(&["meerschaum", "repository"])
                    .map(|v| v.py_str())
                    .unwrap_or_else(|| "api:mrsm".into());
            }
            if !keys.contains(':') {
                keys = format!("api:{keys}");
            }
            state.repo_keys = keys.clone();
            info(
                rt,
                &format!("Default repository for the current shell: {keys}"),
            );
            SuccessTuple::success()
        }
        "executor" => {
            let mut keys = action.first().cloned().unwrap_or_default();
            let context = executor_keys_from_context(rt.env);
            if keys.is_empty() {
                keys = context.clone();
            }
            if keys == "systemd" && context != "systemd" {
                let w = rt.with_formatter(|f| {
                    f.warn("Cannot execute via `systemd`, falling back to `local`...")
                });
                rt.eprint(&w);
                keys = "local".into();
            }
            state.executor_keys = keys.clone();
            info(
                rt,
                &format!("Default executor for the current shell: {keys}"),
            );
            SuccessTuple::success()
        }
        "debug" => {
            state.debug = toggle(state.debug, action, rt);
            info(
                rt,
                &format!("Debug mode is {}.", if state.debug { "on" } else { "off" }),
            );
            SuccessTuple::success()
        }
        "daemon" => {
            let was = state.daemon;
            state.daemon = toggle(state.daemon, action, rt);
            info(
                rt,
                &format!(
                    "CLI daemon mode is {}.",
                    if state.daemon { "on" } else { "off" }
                ),
            );
            if state.daemon && !was {
                // v5 decision: there is no CLI daemon to route through (see the module docs).
                info(
                    rt,
                    "The CLI daemon is not used in Meerschaum v5: actions run natively in this \
                     process.",
                );
            }
            SuccessTuple::success()
        }
        _ => SuccessTuple::success(),
    }
}

/// `do_help(line)`.
pub fn do_help(line: &str, rt: &mut Runtime<'_>) {
    let args = parse_line(line, &rt.dates);
    let mut action: Vec<String> = args
        .get("action")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default();
    // `cmd.Cmd` passes the text after the command word.
    if action.first().map(String::as_str) == Some("help") {
        action.remove(0);
    }
    if action.is_empty() {
        let _ = builtin::show_actions(&args, rt);
        return;
    }
    if SHELL_COMMANDS.contains(&action[0].as_str()) {
        rt.print(&format!(
            "{}\n",
            meerschaum_core::pyfmt::dedent(shell_command_doc(&action[0]))
        ));
        return;
    }
    if !CORE_ACTIONS.iter().any(|a| a.name == action[0]) {
        rt.print(&format!("No help on '{}'.\n", crate::shlex::join(&action)));
        return;
    }
    let mut sysargs = action.clone();
    sysargs.push("--help".into());
    dispatch::parse_help(&sysargs, rt);
}

/// The docstrings of the shell's own commands (`Shell.do_*`).
pub fn shell_command_doc(name: &str) -> &'static str {
    match name {
        "instance" => {
            "
        Temporarily set a default Meerschaum instance for the duration of the shell.
        The default instance is loaded from the Meerschaum configuraton file
          (at keys 'meerschaum:instance').

        You can change the default instance with `edit config`.

        Usage:
            instance {instance keys}

        Examples:
            ```
            ### reset to default instance
            instance

            ### set the instance to 'api:main'
            instance api

            ### set the instance to a non-default connector
            instance api:myremoteinstance
            ```

        Note that instances must be configured, be either API or SQL connections,
          and be accessible to this machine over the network.

        "
        }
        "repo" => {
            "
        Temporarily set a default Meerschaum repository for the duration of the shell.
        The default repository (mrsm.io) is loaded from the Meerschaum configuraton file
          (at keys 'meerschaum.repository').

        You can change the default repository with `edit config`.

        Usage:
            repo {API label}

        Examples:
            ### reset to default repository
            repo

            ### set the repo to 'api:main'
            repository api

            ### set the repository to a non-default repo
            repo myremoterepo

        Note that repositories are a subset of instances.
        "
        }
        "executor" => {
            "
        Temporarily set a default Meerschaum executor for the duration of the shell.

        You can change the default repository with `edit config`.

        Usage:
            executor {API label}

        Examples:
            ### reset to default executor
            executor

            ### set the executor to 'api:main'
            executor api:main

        Note that executors are API instances.
        "
        }
        "debug" => {
            "
        Toggle the shell's debug mode.
        If debug = on, append `--debug` to all commands.

        Command:
            `debug {on/true | off/false}`
            Ommitting on / off will toggle the existing value.
        "
        }
        _ => {
            "
        Toggle whether to route commands through the CLI daemon.

        Command:
            `debug {on/true | off/false}`
            Ommitting on / off will toggle the existing value.
        "
        }
    }
}

fn words_of(args: &ArgsMap) -> Vec<String> {
    args.get("action")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default()
}

/// `Shell.precmd(line)` followed by the command: run one line.
///
/// The session validates the plugin trees once per prompt: the next prompt's completion data
/// (or the next line) re-checks them after this command, so a plugin edited meanwhile is picked
/// up, and every manifest lookup within one command is served from memory.
pub fn precmd(line: &str, state: &mut ShellState, rt: &mut Runtime<'_>) -> Control {
    let control = run_line(line, state, rt);
    if let Some(session) = &rt.native {
        session.invalidate_manifest();
    }
    control
}

fn run_line(line: &str, state: &mut ShellState, rt: &mut Runtime<'_>) -> Control {
    let line = line.replace("\\\n", " ");
    clear_screen(rt, state.debug);
    if line.trim().is_empty() {
        return Control::Continue;
    }
    if matches!(line.as_str(), "exit" | "quit" | "EOF") {
        return Control::Exit;
    }
    if let Some(rest) = line.strip_prefix('?') {
        do_help(&format!("help {rest}"), rt);
        return Control::Continue;
    }
    let sysargs = match crate::shlex::split(&line) {
        Ok(s) => s,
        Err(e) => {
            let w = rt.with_formatter(|f| f.warn(&e));
            rt.eprint(&w);
            return Control::Continue;
        }
    };
    let (sysargs, pipeline_args) = split_pipeline_sysargs(&sysargs);
    let chained = match split_chained_sysargs(&sysargs) {
        Ok(c) => c,
        Err(e) => {
            let w = rt.with_formatter(|f| f.warn(&e.message));
            rt.eprint(&w);
            return Control::Continue;
        }
    };
    let mut chained_kwargs: Vec<ArgsMap> = Vec::new();
    for step in &chained {
        match parse_arguments(step, &rt.dates) {
            Ok(a) => chained_kwargs.push(a),
            Err(e) => {
                let w = rt.with_formatter(|f| f.warn(&e.message));
                rt.eprint(&w);
                return Control::Continue;
            }
        }
    }
    if sysargs.iter().any(|a| a == "--help" || a == "-h") {
        dispatch::parse_help(&sysargs, rt);
        return Control::Continue;
    }
    for kwargs in &mut chained_kwargs {
        if !kwargs.get("debug").is_some_and(PyValue::is_truthy) {
            kwargs.insert("debug".into(), PyValue::Bool(state.debug));
        }
    }
    if chained_kwargs.is_empty() || words_of(&chained_kwargs[0]).is_empty() {
        return Control::Continue;
    }
    for kwargs in &mut chained_kwargs {
        let mut action = words_of(kwargs);
        if action.first().map(String::as_str) == Some("mrsm") {
            action.remove(0);
            if action.is_empty() {
                return Control::Continue;
            }
            kwargs.insert("action".into(), PyValue::str_list(&action));
        }
    }

    // Shell commands and help run directly (v4's `positional_only` path and the actions the
    // shell adds to the registry).
    let first = words_of(&chained_kwargs[0]);
    let main = first[0].as_str();
    if main == "help" {
        do_help(&line, rt);
        return Control::Continue;
    }
    if main == "exit" || main == "quit" {
        return Control::Exit;
    }
    if main == "pass" {
        return Control::Continue;
    }
    if SHELL_COMMANDS.contains(&main) && chained_kwargs.len() == 1 {
        let result = shell_command(main, &first[1..], state, rt);
        print_result(&result, state, rt);
        return Control::Continue;
    }
    if main.trim_matches(|c| c == '"' || c == '\'').is_empty() {
        return Control::Continue;
    }

    let instance_default = rt
        .config()
        .get(&["meerschaum", "instance"])
        .map(|v| v.py_str())
        .unwrap_or_default();
    let repo_default = rt
        .config()
        .get(&["meerschaum", "repository"])
        .map(|v| v.py_str())
        .unwrap_or_default();
    let executor_default = executor_keys_from_context(rt.env);
    let mut sysargs_to_execute: Vec<String> = Vec::new();
    for (i, kwargs) in chained_kwargs.iter_mut().enumerate() {
        let mut action = words_of(kwargs);
        let main_name = actions::get_main_action_name(&action, CORE_ACTIONS);
        let main_name = match main_name {
            Some(n) => n.to_string(),
            // A plugin's `@make_action` is an action too (v4 registers it in `actions`); only
            // words nobody claims run as shell commands.
            None if crate::python::manifest(rt)
                .ok()
                .and_then(|m| crate::python::action_name(&action, &m))
                .is_some() =>
            {
                words_of(kwargs)[0].clone()
            }
            None => {
                action.insert(0, "sh".into());
                kwargs.insert("action".into(), PyValue::str_list(&action));
                "sh".into()
            }
        };
        if main_name != "api" {
            let mut add = |key: &str, value: &str, default: &str| {
                if !kwargs.contains_key(key) && value != default {
                    kwargs.insert(key.into(), PyValue::str(value));
                }
            };
            add(
                "mrsm_instance",
                &remove_ansi(&state.instance_keys),
                &instance_default,
            );
            add("repository", &remove_ansi(&state.repo_keys), &repo_default);
            add(
                "executor_keys",
                &remove_ansi(&state.executor_keys),
                &executor_default,
            );
        }
        let step_kwargs: ArgsMap = kwargs
            .iter()
            .filter(|(k, _)| k.as_str() != "line")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let step = if main_name != "stack" {
            match parse_dict_to_sysargs(&step_kwargs) {
                Ok(s) => s,
                Err(e) => {
                    let w = rt.with_formatter(|f| f.warn(&e.message));
                    rt.eprint(&w);
                    return Control::Continue;
                }
            }
        } else {
            chained[i].clone()
        };
        sysargs_to_execute.extend(step);
        sysargs_to_execute.push(spec::keys::AND.to_string());
    }
    sysargs_to_execute.pop();
    if !pipeline_args.is_empty() {
        sysargs_to_execute.push(spec::keys::PIPELINE.to_string());
        sysargs_to_execute.extend(pipeline_args);
    }

    let old_cwd = std::env::current_dir().ok();
    let mut patch = ArgsMap::new();
    patch.insert("shell".into(), PyValue::Bool(true));
    patch.insert("line".into(), PyValue::str(line.clone()));
    rt.in_shell = true;
    crate::native::jobs::reset_stop_request();
    let result = dispatch::entry_with_patch(&sysargs_to_execute, &patch, rt);
    print_result(&result, state, rt);
    if let Some(old) = old_cwd
        && std::env::current_dir().ok().as_ref() != Some(&old)
    {
        let _ = std::env::set_current_dir(old);
    }
    Control::Continue
}

fn print_result(result: &meerschaum_core::SuccessTuple, state: &ShellState, rt: &mut Runtime<'_>) {
    let text = rt.with_formatter(|f| {
        f.print_tuple(
            result,
            TupleArgs {
                skip_common: !state.debug,
                upper_padding: 1,
                lower_padding: 0,
                ..TupleArgs::default()
            },
        )
    });
    rt.print(&text);
}

/// prompt_toolkit's `FileHistory`: entries are `\n# <timestamp>\n+<line>\n...`.
pub fn load_history(path: &std::path::Path) -> Vec<String> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    let mut lines: Vec<&str> = Vec::new();
    for l in text.split('\n') {
        if let Some(rest) = l.strip_prefix('+') {
            lines.push(rest);
        } else if !lines.is_empty() {
            entries.push(lines.join("\n"));
            lines.clear();
        }
    }
    if !lines.is_empty() {
        entries.push(lines.join("\n"));
    }
    entries
}

/// Append one entry in prompt_toolkit's `FileHistory` format.
pub fn append_history(path: &std::path::Path, entry: &str) {
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.6f");
    let mut text = format!("\n# {now}\n");
    for line in entry.split('\n') {
        text.push('+');
        text.push_str(line);
        text.push('\n');
    }
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = f.write_all(text.as_bytes());
    }
}

/// The completion data for the session (`ShellCompleter` reads v4's live config; the shell
/// refreshes this before every prompt).
pub fn completion_data(state: &ShellState, rt: &mut Runtime<'_>) -> completer::CompletionData {
    let config_keys = builtin::possible_config_keys(rt);
    let config = rt.config();
    let connectors =
        completer::connectors_from_config(config.get(&["meerschaum", "connectors"]).as_ref());
    let plugins_dirs = config.paths().plugins_dirs.clone();
    let plugin_names = builtin::plugin_names(&plugins_dirs);
    let ctx = crate::native::jobs::jobs_context(rt);
    let jobs = move |executor_keys: &str| -> Vec<completer::JobEntry> {
        if executor_keys != "local" {
            return Vec::new();
        }
        meerschaum_jobs::get_jobs(&ctx, false)
            .into_iter()
            .map(|(name, mut job)| completer::JobEntry {
                name,
                status: match job.status() {
                    meerschaum_jobs::Status::Running => "running",
                    meerschaum_jobs::Status::Paused => "paused",
                    meerschaum_jobs::Status::Stopped => "stopped",
                }
                .to_string(),
                restart: job.restarts(),
            })
            .collect()
    };
    let mut data = completer::CompletionData {
        connectors,
        data_plugins: Vec::new(),
        plugin_names,
        config_keys,
        executor_keys: remove_ansi(&state.executor_keys),
        jobs: Some(std::sync::Arc::new(jobs)),
        ..completer::CompletionData::default()
    };
    if let Ok(manifest) = crate::python::manifest(rt) {
        data.plugin_actions = manifest
            .plugins
            .values()
            .flat_map(|p| p.actions.clone())
            .collect();
        data.plugin_arguments = manifest
            .plugins
            .values()
            .flat_map(|p| p.arguments.clone())
            .collect();
        data.do_names
            .extend(data.plugin_actions.iter().map(|a| a.name.clone()));
        data.do_names.sort();
        data.do_names.dedup();
        data.data_plugins = manifest
            .plugins
            .values()
            .filter(|p| {
                p.data_functions
                    .iter()
                    .any(|f| matches!(f.as_str(), "fetch" | "sync"))
            })
            .map(|p| format!("plugin:{}", p.name))
            .collect();
        let session = crate::native::session(rt);
        let pool = session.pool(rt.config());
        data.plugins = Some(std::sync::Arc::new(crate::python::Completer::new(
            session, pool, manifest,
        )));
    }
    data
}

/// The toolbar values for the session (`bottom_toolbar()`'s inputs).
pub fn toolbar_model(state: &ShellState, rt: &mut Runtime<'_>) -> toolbar::ToolbarModel {
    let settings = rt.settings();
    let compose = env_project_name(rt.env);
    let instance = remove_ansi(&state.instance_keys);
    let connected = rt.native.as_ref().is_some_and(|n| {
        let (typ, label) = instance
            .split_once(':')
            .unwrap_or((instance.as_str(), "main"));
        n.active().iter().any(|(t, l)| t == typ && l == label)
    });
    let config = rt.config();
    let get = |keys: &[&str]| config.get(keys).map(|v| v.py_str()).unwrap_or_default();
    let charset = if settings.unicode { "unicode" } else { "ascii" };
    toolbar::ToolbarModel {
        enabled: config
            .get(&["shell", "bottom_toolbar", "enabled"])
            .is_some_and(|v| v.truthy()),
        ansi: settings.ansi,
        unicode: settings.unicode,
        instance,
        repo: remove_ansi(&state.repo_keys),
        executor: remove_ansi(&state.executor_keys),
        compose,
        connected,
        instance_style: get(&["shell", "ansi", "instance", "rich", "style"]),
        repo_style: get(&["shell", "ansi", "repo", "rich", "style"]),
        executor_style: get(&["shell", "ansi", "executor", "rich", "style"]),
        compose_style: get(&["shell", "ansi", "compose", "rich", "style"]),
        connected_style: get(&["formatting", "connected", "ansi", "rich", "style"]),
        disconnected_style: get(&["formatting", "disconnected", "ansi", "rich", "style"]),
        connected_icon: get(&["formatting", "connected", charset, "icon"]),
        disconnected_icon: get(&["formatting", "disconnected", charset, "icon"]),
    }
}

/// The prompt for the current state.
pub fn current_prompt(state: &ShellState, rt: &mut Runtime<'_>) -> String {
    let compose = env_project_name(rt.env);
    rt.with_formatter(|f| {
        let mut p = PromptState::new(f, &state.instance_keys, compose.as_deref());
        p.executor_keys = state.executor_keys.clone();
        render_prompt(f, &p)
    })
}

/// The banner (`self.intro`): the intro plus the update messages.
pub fn intro_with_updates(rt: &mut Runtime<'_>) -> String {
    let intro = rt.with_formatter(|f| shell_intro(f, f.settings.ansi, crate::VERSION));
    let settings = rt.settings();
    let charset = if settings.unicode { "unicode" } else { "ascii" };
    let config = rt.config();
    let intro_sgr = config
        .get(&["shell", "ansi", "intro", "rich", "style"])
        .map(|v| v.py_str())
        .and_then(|s| crate::format::rich::style::Style::parse(&s).ok())
        .map(|s| s.ansi_codes())
        .unwrap_or_default();
    format!(
        "{intro}{}{}",
        updates::update_message(config, charset, settings.ansi, crate::VERSION, &intro_sgr),
        updates::plugins_update_message(config, settings.ansi),
    )
}

/// `get_shell(sysargs, mrsm_instance=instance).cmdloop()`.
///
/// `sysargs` (e.g. `-i sql:local` from `mrsm -i sql:local`) replace the banner and run as the
/// first line; `instance` is the session's instance (v4 passes it only through `entry`, not for
/// `mrsm --shell`).
pub fn run(sysargs: &[String], instance: Option<&str>, rt: &mut Runtime<'_>) {
    rt.in_shell = true;
    let mut state = ShellState::new(rt, instance);
    let noninteractive = crate::env::noninteractive(rt.env);
    let interactive = !noninteractive && std::io::IsTerminal::is_terminal(&std::io::stdin());
    // `Shell.__init__` builds the intro (with the cached update messages), then starts the
    // version check thread.
    let intro = intro_with_updates(rt);
    if interactive {
        let _ = updates::run_version_check_thread(rt.config());
    }

    // `preloop`: clear the screen; sysargs replace the intro and run as the first command.
    clear_screen(rt, false);
    let flags: Vec<String> = sysargs.to_vec();
    if flags.is_empty() {
        rt.print(&format!("{intro}\n"));
    } else if precmd(&flags.join(" "), &mut state, rt) == Control::Exit {
        close(rt);
        return;
    }

    let history_path = rt.config().paths().path("SHELL_HISTORY_PATH");
    let max_history = rt
        .config()
        .get(&["shell", "max_history"])
        .and_then(|v| v.as_i64())
        .unwrap_or(1000)
        .max(1) as usize;

    if !interactive {
        // Read commands line by line without line editing (pipes, `MRSM_NONINTERACTIVE`).
        loop {
            let prompt = current_prompt(&state, rt);
            let Some(line) = rt.host.read_line(&prompt) else {
                break;
            };
            if precmd(&line, &mut state, rt) == Control::Exit {
                break;
            }
        }
        close(rt);
        return;
    }

    let mut history = load_history(&history_path);
    let dates = rt.dates.clone();
    let shared: editor::SharedRef = std::sync::Arc::new(std::sync::Mutex::new(
        editor::Shared::new(completion_data(&state, rt), dates),
    ));
    let mut editor = reedline::Reedline::create()
        .with_history(Box::new(
            reedline::FileBackedHistory::new(max_history).unwrap_or_default(),
        ))
        .with_edit_mode(Box::new(editor::ShellEditMode::new(shared.clone())))
        .with_hinter(Box::new(editor::ShellHinter {
            shared: shared.clone(),
        }))
        .with_highlighter(Box::new(editor::ShellHighlighter {
            shared: shared.clone(),
        }))
        .with_quick_completions(true)
        .with_partial_completions(false)
        .with_ansi_colors(true)
        .use_bracketed_paste(true);
    for menu in editor::HookMenu::all(&shared) {
        editor = editor.with_menu(menu);
    }
    loop {
        let prompt_text = current_prompt(&state, rt);
        let data = completion_data(&state, rt);
        let toolbar = toolbar_model(&state, rt);
        {
            let mut s = shared.lock().unwrap_or_else(|e| e.into_inner());
            s.data = data;
            s.toolbar = toolbar;
            s.prompt = prompt_text;
            s.history = history.clone();
            s.begin_line();
        }
        let prompt = editor::ShellPrompt {
            shared: shared.clone(),
        };
        match editor.read_line(&prompt) {
            Ok(reedline::Signal::Success(line)) => {
                // `Buffer.append_to_history`: any non-empty text, unless it repeats the last
                // entry.
                if !line.is_empty() && history.last() != Some(&line) {
                    append_history(&history_path, &line);
                    history.push(line.clone());
                    if history.len() > max_history {
                        history.remove(0);
                    }
                }
                if line.trim().is_empty() {
                    clear_screen(rt, state.debug);
                }
                if precmd(&line, &mut state, rt) == Control::Exit {
                    break;
                }
            }
            Ok(reedline::Signal::CtrlC) => {
                // `KeyboardInterrupt`: print `^C` and run `pass` (which clears the screen).
                rt.print("^C\n");
                let _ = precmd("pass", &mut state, rt);
            }
            Ok(reedline::Signal::CtrlD) => {
                // `EOFError`: `cmd` runs the line `EOF`.
                let _ = precmd("EOF", &mut state, rt);
                break;
            }
            Ok(_) => {}
            Err(e) => {
                rt.eprint(&format!("{e}\n"));
                break;
            }
        }
    }
    close(rt);
}

/// `postloop`: print the close message.
fn close(rt: &mut Runtime<'_>) {
    let text = rt.with_formatter(|f| {
        let msg = shell_config_str(f, &["shell", f.charset(), "close_message"]);
        if f.settings.ansi {
            let style = shell_config_str(f, &["shell", "ansi", "close_message", "rich", "style"]);
            f.colored(&msg, &style)
        } else {
            msg
        }
    });
    rt.print(&format!("\n{text}\n"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_counts_code_points() {
        assert_eq!(truncate_text_for_display("abcdef", 4, "…"), "abc…");
        assert_eq!(truncate_text_for_display("abc", 4, "…"), "abc");
    }

    #[test]
    fn history_roundtrip() {
        let dir = std::env::temp_dir().join(format!("mrsm-history-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join(".mrsm_history");
        let _ = std::fs::remove_file(&path);
        append_history(&path, "show pipes");
        append_history(&path, "sync pipes \\\n-c sql:main");
        assert_eq!(
            load_history(&path),
            vec!["show pipes", "sync pipes \\\n-c sql:main"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
