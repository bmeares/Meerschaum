//! The interactive Meerschaum shell, ported from `meerschaum/_internal/shell/` onto `reedline`.
//!
//! Kept from v4:
//! - the banner (`shell.<charset>.intro` plus the right-aligned version, colored with
//!   `shell.ansi.intro.rich.style`) and the close message;
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
//! - tab completion of actions, subactions, flags, flag choices, config keys and connector keys.
//!
//! Deferred: the bottom toolbar, the version update check, the CLI daemon, and plugin
//! completers.

use std::io::Write;
use std::path::PathBuf;

use meerschaum_core::value::Value;
use reedline::{
    ColumnarMenu, Completer, CompletionResult, DefaultHinter, Emacs, FileBackedHistory, History,
    HistoryItem, KeyCode, KeyModifiers, MenuBuilder, Prompt, PromptEditMode, PromptHistorySearch,
    Reedline, ReedlineEvent, ReedlineMenu, Signal, Span, Suggestion, default_emacs_keybindings,
};

use crate::actions::{self, CORE_ACTIONS};
use crate::builtin;
use crate::dispatch::{self, Runtime};
use crate::format::{Formatter, TupleArgs, remove_ansi};
use crate::parse::{parse_arguments, parse_line};
use crate::spec;
use crate::sysargs::{parse_dict_to_sysargs, split_chained_sysargs, split_pipeline_sysargs};
use crate::value::{ArgsMap, PyValue};

/// The shell's own commands (v4 adds them to the actions registry).
pub const SHELL_COMMANDS: &[&str] = &["instance", "repo", "executor", "debug", "daemon"];

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
    /// `daemon` mode (the CLI daemon is not ported; the flag is kept for `daemon on/off`).
    pub daemon: bool,
}

impl ShellState {
    /// The defaults from the config registry.
    pub fn new(rt: &mut Runtime<'_>, instance: Option<&str>) -> ShellState {
        let config = rt.config();
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
            daemon: false,
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
            state.daemon = toggle(state.daemon, action, rt);
            info(
                rt,
                &format!(
                    "CLI daemon mode is {}.",
                    if state.daemon { "on" } else { "off" }
                ),
            );
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
pub fn precmd(line: &str, state: &mut ShellState, rt: &mut Runtime<'_>) -> Control {
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

/// The completion candidates (`ShellCompleter`).
pub struct ShellCompleter {
    /// The `do_*` names: actions and shell commands.
    pub commands: Vec<String>,
    /// Every option string.
    pub flags: Vec<String>,
    /// Option string -> `choices`.
    pub choices: Vec<(String, Vec<String>)>,
    /// `type:label` for every configured connector.
    pub connectors: Vec<String>,
    /// The top-level config keys.
    pub config_keys: Vec<String>,
}

/// The flags whose value is a connector (`_check_complete_keys`'s `trigger_args`).
const CONNECTOR_FLAGS: &[&str] = &[
    "-c",
    "--connector-keys",
    "-r",
    "--repository",
    "-i",
    "--instance",
    "--mrsm-instance",
];

impl ShellCompleter {
    /// Build the candidates from the actions table, the argument spec and the config.
    pub fn new(rt: &mut Runtime<'_>) -> ShellCompleter {
        let mut commands: Vec<String> = CORE_ACTIONS.iter().map(|a| a.name.to_string()).collect();
        commands.extend(SHELL_COMMANDS.iter().map(|s| s.to_string()));
        commands.extend(["help", "exit", "pass"].iter().map(|s| s.to_string()));
        commands.sort();
        commands.dedup();
        let flags: Vec<String> = spec::ARGUMENTS
            .iter()
            .flat_map(|a| a.option_strings.iter().map(|s| s.to_string()))
            .collect();
        let choices: Vec<(String, Vec<String>)> = spec::ARGUMENTS
            .iter()
            .filter_map(|a| a.choices.map(|c| (a, c)))
            .flat_map(|(a, c)| {
                a.option_strings
                    .iter()
                    .map(move |o| (o.to_string(), c.iter().map(|s| s.to_string()).collect()))
            })
            .collect();
        let mut connectors = Vec::new();
        if let Some(Value::Map(types)) = rt.config().get(&["meerschaum", "connectors"]) {
            for (typ, labels) in types {
                if let Value::Map(labels) = labels {
                    for label in labels.keys() {
                        if label != "default" {
                            connectors.push(format!("{typ}:{label}"));
                        }
                    }
                }
            }
        }
        connectors.sort();
        let config_keys = builtin::possible_config_keys(rt);
        ShellCompleter {
            commands,
            flags,
            choices,
            connectors,
            config_keys,
        }
    }

    /// The candidates for the text before the cursor, and how many characters they replace.
    pub fn candidates(&self, line: &str) -> (Vec<String>, usize) {
        let segment = line.rsplit('+').next().unwrap_or("");
        let ends_with_space = segment.ends_with(' ') || segment.ends_with('\t');
        let tokens: Vec<&str> = segment.split_whitespace().collect();
        let current = if ends_with_space {
            ""
        } else {
            tokens.last().copied().unwrap_or("")
        };
        let previous: Vec<&str> = if ends_with_space {
            tokens.clone()
        } else {
            tokens[..tokens.len().saturating_sub(1)].to_vec()
        };
        let filter = |items: &[String]| -> Vec<String> {
            items
                .iter()
                .filter(|c| c.starts_with(current) && c.as_str() != current)
                .cloned()
                .collect()
        };
        // Flag values: `--flag <choice>` and connector keys.
        if let Some(prev) = previous.last()
            && prev.starts_with('-')
        {
            if let Some((_, c)) = self.choices.iter().find(|(f, _)| f == prev) {
                return (filter(c), current.chars().count());
            }
            if CONNECTOR_FLAGS.contains(prev) {
                return (filter(&self.connectors), current.chars().count());
            }
        }
        if current.starts_with('-') {
            let mut flags = filter(&self.flags);
            flags.sort();
            flags.dedup();
            return (flags, current.chars().count());
        }
        let words: Vec<&str> = previous
            .iter()
            .copied()
            .filter(|w| !w.starts_with('-'))
            .collect();
        match words.as_slice() {
            [] => (filter(&self.commands), current.chars().count()),
            [action] => {
                let action = if *action == "help" { "" } else { action };
                let options: Vec<String> = if action.is_empty() {
                    self.commands.clone()
                } else if action == "compose" {
                    meerschaum_compose::subactions::complete_compose(&[])
                } else if SHELL_COMMANDS.contains(&action)
                    && action != "debug"
                    && action != "daemon"
                {
                    self.connectors.clone()
                } else {
                    let mut subs: Vec<String> = actions::get_subactions(CORE_ACTIONS, action)
                        .iter()
                        .map(|s| s.name.to_string())
                        .collect();
                    subs.sort();
                    subs
                };
                (filter(&options), current.chars().count())
            }
            [action, sub, ..] => {
                let options: Vec<String> = match (*action, *sub) {
                    ("show" | "edit", "config") => self.config_keys.clone(),
                    ("show", "connectors" | "connector") => self.connectors.clone(),
                    ("help", a) => actions::get_subactions(CORE_ACTIONS, a)
                        .iter()
                        .map(|s| s.name.to_string())
                        .collect(),
                    _ => Vec::new(),
                };
                (filter(&options), current.chars().count())
            }
        }
    }
}

impl Completer for ShellCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> CompletionResult {
        let before = &line[..pos.min(line.len())];
        let (candidates, replace_chars) = self.candidates(before);
        let start_char = before.chars().count().saturating_sub(replace_chars);
        let start = before
            .char_indices()
            .nth(start_char)
            .map(|(i, _)| i)
            .unwrap_or(before.len());
        let suggestions: Vec<Suggestion> = candidates
            .into_iter()
            .map(|value| Suggestion {
                value,
                span: Span::new(start, pos),
                append_whitespace: true,
                ..Suggestion::default()
            })
            .collect();
        CompletionResult::fresh(suggestions)
    }
}

/// The prompt for `reedline`: the rendered v4 prompt, no indicator.
pub struct ShellPrompt {
    /// The rendered prompt (it may contain ANSI escapes and a leading newline).
    pub text: String,
}

impl Prompt for ShellPrompt {
    fn render_prompt_left(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed(&self.text)
    }

    fn render_prompt_right(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed("")
    }

    fn render_prompt_indicator(&self, _mode: PromptEditMode) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed("")
    }

    fn render_prompt_multiline_indicator(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed("... ")
    }

    fn render_prompt_history_search_indicator(
        &self,
        search: PromptHistorySearch,
    ) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Owned(format!("(search: {}) ", search.term))
    }

    fn get_prompt_color(&self) -> reedline::Color {
        reedline::Color::Default
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

/// `get_shell(sysargs, mrsm_instance=instance).cmdloop()`.
///
/// `sysargs` (e.g. `-i sql:local` from `mrsm -i sql:local`) replace the banner and run as the
/// first line; `instance` is the session's instance (v4 passes it only through `entry`, not for
/// `mrsm --shell`).
pub fn run(sysargs: &[String], instance: Option<&str>, rt: &mut Runtime<'_>) {
    let mut state = ShellState::new(rt, instance);
    let noninteractive = crate::env::noninteractive(rt.env);

    // `preloop`: clear the screen; sysargs replace the intro and run as the first command.
    clear_screen(rt, false);
    let flags: Vec<String> = sysargs.to_vec();
    if flags.is_empty() {
        let intro = rt.with_formatter(|f| shell_intro(f, f.settings.ansi, crate::VERSION));
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

    if noninteractive || !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
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

    let mut history = FileBackedHistory::new(max_history).unwrap_or_default();
    for entry in load_history(&history_path) {
        let _ = history.save(HistoryItem::from_command_line(entry));
    }
    let completer = ShellCompleter::new(rt);
    let menu = Box::new(ColumnarMenu::default().with_name("completion_menu"));
    let mut keybindings = default_emacs_keybindings();
    keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu("completion_menu".into()),
            ReedlineEvent::MenuNext,
        ]),
    );
    let ansi = rt.settings().ansi;
    let mut editor = Reedline::create()
        .with_history(Box::new(history))
        .with_completer(Box::new(completer))
        .with_menu(ReedlineMenu::EngineCompleter(menu))
        .with_edit_mode(Box::new(Emacs::new(keybindings)))
        .with_hinter(Box::new(DefaultHinter::default()))
        .with_ansi_colors(ansi);
    loop {
        let prompt = ShellPrompt {
            text: current_prompt(&state, rt),
        };
        match editor.read_line(&prompt) {
            Ok(Signal::Success(line)) => {
                if !line.trim().is_empty() {
                    append_history(&history_path, &line);
                } else {
                    clear_screen(rt, state.debug);
                }
                if precmd(&line, &mut state, rt) == Control::Exit {
                    break;
                }
            }
            Ok(Signal::CtrlC) => {
                rt.print("^C\n");
            }
            Ok(Signal::CtrlD) => break,
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
