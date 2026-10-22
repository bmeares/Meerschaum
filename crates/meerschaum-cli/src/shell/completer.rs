//! The shell's completion candidates, ported from v4's `ShellCompleter.get_completions`
//! (`meerschaum/_internal/shell/ShellCompleter.py`), the per-action completers it reaches
//! through `cmd.Cmd.complete_<action>` (`Shell._completer_wrapper`, `_check_complete_keys`,
//! `default_action_completer`, `Shell.complete_instance` / `complete_repo` /
//! `complete_executor`) and the `_complete_*` functions in `meerschaum/actions/*.py`.
//!
//! The algorithm is reproduced literally, quirks included (each marked `PARITY:`), and checked
//! against `tests/fixtures/shell_completions.json`, recorded from v4.1.4.
//!
//! Plugin actions, flags and completers come from the cached worker manifest.

use std::sync::{Arc, LazyLock};

use indexmap::IndexMap;
use meerschaum_core::value::Value;

use crate::actions::{self, ActionDef, CORE_ACTIONS};
use crate::parse::parse_line;
use crate::spec;
use crate::types::DateContext;
use crate::value::PyValue;

/// The shell's own commands, added to v4's `shell_attrs['_actions']` (and to the `do_` names).
pub const SHELL_COMMANDS: &[&str] = &["instance", "repo", "executor", "debug", "daemon"];

/// `Shell` methods that are `do_` names but not actions (`do_help`, `do_exit`, `do_pass`).
pub const SHELL_METHODS: &[&str] = &["help", "exit", "pass"];

/// The shell commands the per-action completer wrapper skips (`reserved_completers`).
const RESERVED_COMPLETERS: &[&str] = &["instance", "repo", "executor"];

/// `meerschaum.utils.packages.packages`: the dependency groups `show packages` completes.
// PARITY: v4 lists its setup extras here; v5 keeps the names for completion.
/// The drivers `install driver` / `show drivers` know (v5).
const DRIVER_NAMES: &[&str] = &["duckdb", "oracle"];

pub const PACKAGE_GROUPS: &[&str] = &[
    "_internal",
    "api",
    "build",
    "cli",
    "compose",
    "core",
    "dash",
    "dev-tools",
    "docs",
    "drivers",
    "drivers-extras",
    "extras",
    "formatting",
    "full",
    "gis",
    "gui",
    "jobs",
    "minimal",
    "polars",
    "required",
    "setup",
    "sql",
    "stack",
];

/// `meerschaum.connectors.instance_types` (with the built-in custom `valkey`).
pub const INSTANCE_TYPES: &[&str] = &["sql", "api", "valkey"];

/// `meerschaum.jobs.executor_types`.
pub const EXECUTOR_TYPES: &[&str] = &["local", "systemd", "api"];

/// The action registry the shell resolves against: the core actions plus the shell commands
/// (`shell_attrs['_actions']`, whose functions are the `Shell.do_*` methods).
pub static SHELL_ACTIONS: LazyLock<&'static [ActionDef]> = LazyLock::new(|| {
    let mut all: Vec<ActionDef> = CORE_ACTIONS.to_vec();
    for name in SHELL_COMMANDS {
        all.push(ActionDef {
            name,
            function: match *name {
                "instance" => "do_instance",
                "repo" => "do_repo",
                "executor" => "do_executor",
                "debug" => "do_debug",
                _ => "do_daemon",
            },
            accepts_executor_keys: true,
            subactions: &[],
        });
    }
    Box::leak(all.into_boxed_slice())
});

/// One completion (`prompt_toolkit.completion.Completion`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completion {
    /// The text to insert.
    pub text: String,
    /// `start_position` (code points, `<= 0`).
    pub start_position: i64,
    /// The display text.
    pub display: String,
    /// How many leading code points of `display` are styled `fg:ansiblue bold`.
    pub highlight: usize,
}

impl Completion {
    fn new(text: String, start: usize, highlight: usize) -> Completion {
        Completion {
            display: text.clone(),
            highlight: highlight.min(text.chars().count()),
            text,
            start_position: -(start as i64),
        }
    }

    /// v4's empty fallback: `Completion('')`.
    fn empty() -> Completion {
        Completion {
            text: String::new(),
            start_position: 0,
            display: String::new(),
            highlight: 0,
        }
    }
}

/// A job as the job completers see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobEntry {
    /// The job name.
    pub name: String,
    /// `running`, `paused` or `stopped`.
    pub status: String,
    /// Whether the job restarts (`get_restart_jobs`).
    pub restart: bool,
}

/// Lists the jobs of an executor.
pub type JobLister = Arc<dyn Fn(&str) -> Vec<JobEntry> + Send + Sync>;
/// Read Pipeline names from a selected registry on demand.
pub type PipelineLister = Arc<dyn Fn(&str) -> Vec<String> + Send + Sync>;

/// The seam for plugin-defined completers (`complete_<action>` in a plugin module).
pub trait PluginCompleter: Send + Sync {
    /// Completions for `action` (the words after the action) and the full `line`, or `None`
    /// when the plugin defines no completer for `main_action`.
    fn complete(&self, main_action: &str, action: &[String], line: &str) -> Option<Vec<String>>;
}

/// Everything the completer reads, captured from the config registry before each prompt (v4
/// reads the config live; the shell refreshes this after every command).
#[derive(Clone)]
pub struct CompletionData {
    /// The `do_` names, sorted (`dir(Shell)`).
    pub do_names: Vec<String>,
    /// The action registry (`shell_attrs['_actions']`).
    pub actions: &'static [ActionDef],
    /// `meerschaum:connectors`: type -> labels (in config order, `default` excluded).
    pub connectors: IndexMap<String, Vec<String>>,
    /// `plugin:<name>` for plugins with `fetch` or `sync` (`get_data_plugins()`).
    pub data_plugins: Vec<String>,
    /// `get_plugins_names()`.
    pub plugin_names: Vec<String>,
    /// `get_possible_keys()` (the config keys).
    pub config_keys: Vec<String>,
    /// The shell's executor keys (`shell_attrs['executor_keys']`).
    pub executor_keys: String,
    /// Lists the jobs of an executor (`get_jobs(executor_keys, include_hidden=False)`).
    pub jobs: Option<JobLister>,
    /// Default registry and lazy name lookup for registered Pipeline completions.
    pub instance_keys: String,
    pub pipelines: Option<PipelineLister>,
    /// Actions declared by installed Python plugins.
    pub plugin_actions: Vec<meerschaum_worker::manifest::ActionManifest>,
    /// Plugin argument declarations.
    pub plugin_arguments: Vec<meerschaum_worker::manifest::ArgumentManifest>,
    /// Plugin completers.
    pub plugins: Option<Arc<dyn PluginCompleter>>,
    /// Retained for source compatibility; additive v5 subactions are always offered.
    pub include_v5_only: bool,
    /// Plugin connector types that can be an instance (`IS_INSTANCE`), for `-i` values.
    pub instance_types: Vec<String>,
}

impl std::fmt::Debug for CompletionData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletionData")
            .field("do_names", &self.do_names)
            .field("connectors", &self.connectors)
            .field("data_plugins", &self.data_plugins)
            .field("plugin_names", &self.plugin_names)
            .field("config_keys", &self.config_keys)
            .field("executor_keys", &self.executor_keys)
            .finish()
    }
}

impl Default for CompletionData {
    fn default() -> Self {
        CompletionData {
            do_names: default_do_names(*SHELL_ACTIONS),
            actions: *SHELL_ACTIONS,
            connectors: IndexMap::new(),
            data_plugins: Vec::new(),
            plugin_names: Vec::new(),
            config_keys: Vec::new(),
            executor_keys: "local".into(),
            jobs: None,
            instance_keys: "sql:main".into(),
            pipelines: None,
            plugins: None,
            plugin_actions: Vec::new(),
            plugin_arguments: Vec::new(),
            include_v5_only: false,
            instance_types: Vec::new(),
        }
    }
}

/// The sorted `do_` names for an action registry (actions, shell commands, `help`, `exit`,
/// `pass`).
pub fn default_do_names(actions: &[ActionDef]) -> Vec<String> {
    // Additive v5 actions are public; fixture exclusions belong in parity tests.
    let mut names: Vec<String> = actions.iter().map(|a| a.name.to_string()).collect();
    names.extend(SHELL_METHODS.iter().map(|s| s.to_string()));
    names.sort();
    names.dedup();
    names
}

/// The first words the program's own command line takes (`mrsm <action>`): every registered action,
/// the v5-only ones included, and none of the shell's own commands.
pub fn cli_do_names(actions: &[ActionDef]) -> Vec<String> {
    let mut names: Vec<String> = actions.iter().map(|a| a.name.to_string()).collect();
    names.sort();
    names.dedup();
    names
}

/// `connectors` from the `meerschaum:connectors` config section.
pub fn connectors_from_config(section: Option<&Value>) -> IndexMap<String, Vec<String>> {
    let mut out = IndexMap::new();
    if let Some(Value::Map(types)) = section {
        for (typ, labels) in types {
            let labels = match labels {
                Value::Map(m) => m.keys().filter(|l| *l != "default").cloned().collect(),
                _ => Vec::new(),
            };
            out.insert(typ.clone(), labels);
        }
    }
    out
}

/// `get_arguments_triggers()`: `{dest: option_strings}` over the parser's actions (a later
/// action with the same `dest` replaces the strings but keeps the first position).
fn arguments_triggers() -> &'static IndexMap<&'static str, Vec<&'static str>> {
    static TRIGGERS: LazyLock<IndexMap<&'static str, Vec<&'static str>>> = LazyLock::new(|| {
        let mut out: IndexMap<&'static str, Vec<&'static str>> = IndexMap::new();
        for a in spec::ARGUMENTS {
            out.insert(a.dest, a.option_strings.to_vec());
        }
        out
    });
    &TRIGGERS
}

/// `_get_flag_choices_map()`.
fn flag_choices(flag: &str) -> Option<&'static [&'static str]> {
    spec::ARGUMENTS
        .iter()
        .filter(|a| a.option_strings.contains(&flag))
        .filter_map(|a| a.choices)
        .next_back()
        .filter(|c| !c.is_empty())
}

/// The `_check_complete_keys` trigger flags, in v4's dict order.
const TRIGGER_ARGS: &[&str] = &[
    "-c",
    "--connector-keys",
    "-r",
    "--repository",
    "-i",
    "--instance",
    "--mrsm-instance",
];

fn chars(s: &str) -> usize {
    s.chars().count()
}

/// Python `str.split(' ')[-1]`.
fn last_space_word(s: &str) -> &str {
    s.rsplit(' ').next().unwrap_or("")
}

/// Python `str.lstrip()` (whitespace).
fn lstrip(s: &str) -> &str {
    s.trim_start_matches(char::is_whitespace)
}

impl CompletionData {
    /// `get_connector_labels(*types, search_term=..., ignore_exact_match=...)`.
    pub fn connector_labels(
        &self,
        types: &[&str],
        search_term: &str,
        ignore_exact_match: bool,
        additional: &[&str],
    ) -> Vec<String> {
        let mut types: Vec<String> = types.iter().map(|t| t.to_string()).collect();
        if types.is_empty() {
            types = self.connectors.keys().cloned().collect();
            types.push("plugin".into());
        }
        let mut conns: Vec<String> = Vec::new();
        for t in &types {
            if t == "plugin" {
                conns.extend(self.data_plugins.iter().cloned());
                continue;
            }
            if let Some(labels) = self.connectors.get(t) {
                conns.extend(labels.iter().map(|l| format!("{t}:{l}")));
            }
        }
        conns.extend(additional.iter().map(|s| s.to_string()));
        let mut out: Vec<String> = conns
            .into_iter()
            .filter(|c| c.starts_with(search_term) && !(ignore_exact_match && c == search_term))
            .collect();
        out.sort();
        out
    }

    /// `ShellCompleter.get_completions(Document(text))`.
    pub fn get_completions(&self, text: &str, dates: &DateContext) -> Vec<Completion> {
        let parts: Vec<&str> = text.split('-').collect();
        let last_action_line = parts[0].rsplit('+').next().unwrap_or("");
        let parsed_text = format!(
            "{}{}",
            last_action_line.replace(' ', "_"),
            parts[1..].join("-")
        );
        if parsed_text.is_empty() {
            return Vec::new();
        }
        let typed_len = chars(lstrip(last_action_line));

        // Flag values: `--flag <choice>`.
        let current_segment = text.rsplit('+').next().unwrap_or("");
        let ends_with_space = current_segment.ends_with(' ') || current_segment.ends_with('\t');
        let tokens: Vec<&str> = current_segment.split_whitespace().collect();
        let mut flag_token: Option<&str> = None;
        let mut partial = "";
        if let Some(last) = tokens.last() {
            if ends_with_space && last.starts_with("--") {
                flag_token = Some(last);
            } else if !ends_with_space
                && tokens.len() >= 2
                && tokens[tokens.len() - 2].starts_with("--")
                && !last.starts_with("--")
            {
                flag_token = Some(tokens[tokens.len() - 2]);
                partial = last;
            }
        }
        if let Some(choices) = flag_token.and_then(flag_choices) {
            return choices
                .iter()
                .filter(|c| c.starts_with(partial))
                .map(|c| Completion::new(c.to_string(), chars(partial), chars(partial)))
                .collect();
        }

        if let Some(argument) = flag_token.and_then(|flag| {
            self.plugin_arguments
                .iter()
                .find(|a| a.option_strings.iter().any(|s| s == flag))
        }) && let Some(choices) = argument.choices.as_array()
        {
            return choices
                .iter()
                .map(|c| {
                    c.as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| c.to_string())
                })
                .filter(|c| c.starts_with(partial))
                .map(|c| Completion::new(c, chars(partial), chars(partial)))
                .collect();
        }

        // `shell.complete(text, i)` → `cmd.Cmd.completenames`.
        let mut out: Vec<Completion> = Vec::new();
        let prefix = parsed_text.trim_start_matches('_');
        for name in &self.do_names {
            if name.starts_with(prefix) {
                out.push(Completion::new(
                    name.replace('_', " "),
                    typed_len,
                    typed_len,
                ));
            }
        }
        let yielded = !out.is_empty();

        let current_action_line = lstrip(text.rsplit('+').next().unwrap_or(""));
        let args = parse_line(current_action_line, dates);
        let action: Vec<String> = args
            .get("action")
            .and_then(PyValue::as_list)
            .map(|l| l.iter().map(PyValue::py_str).collect())
            .unwrap_or_default();
        if let Some(plugin) = self
            .plugin_actions
            .iter()
            .filter(|a| {
                let parts: Vec<_> = a.name.split('_').collect();
                action.len() >= parts.len()
                    && action.iter().zip(parts).all(|(word, part)| word == part)
            })
            .max_by_key(|a| a.name.len())
        {
            if current_action_line.contains(' ') {
                let found = self
                    .check_complete_keys(current_action_line)
                    .or_else(|| {
                        self.plugins
                            .as_ref()
                            .and_then(|p| p.complete(&plugin.name, &action, current_action_line))
                    })
                    .unwrap_or_else(|| {
                        let partial = last_space_word(current_action_line);
                        plugin
                            .subactions
                            .iter()
                            .filter(|s| s.name.starts_with(partial) && s.name != partial)
                            .map(|s| s.name.clone())
                            .collect()
                    });
                let n = chars(last_space_word(text));
                out.extend(found.into_iter().map(|p| Completion::new(p, n, n)));
            }
            return out;
        }
        if action
            .get(1)
            .is_some_and(|word| matches!(word.as_str(), "pipeline" | "pipelines"))
            && (tokens.len() > 2 || current_action_line.ends_with(char::is_whitespace))
            && !last_space_word(current_action_line).starts_with('-')
            && let Some(pipelines) = self.pipelines.as_ref()
        {
            let value_flag = if current_action_line.ends_with(char::is_whitespace) {
                tokens.last().copied()
            } else {
                tokens.iter().rev().nth(1).copied()
            };
            if !value_flag.is_some_and(|word| word.starts_with('-')) {
                let registry = args
                    .get("mrsm_instance")
                    .map(PyValue::py_str)
                    .unwrap_or_else(|| self.instance_keys.clone());
                let partial = last_space_word(current_action_line);
                let n = chars(partial);
                return pipelines(&registry)
                    .into_iter()
                    .filter(|name| name.starts_with(partial))
                    .map(|name| Completion::new(name, n, n))
                    .collect();
            }
        }
        if actions::get_action(&action, self.actions).is_none() {
            return out;
        }
        // PARITY: v4 would raise `TypeError` in `str.replace(None, ...)`; nothing more is added.
        let Some(main) = actions::get_main_action_name(&action, self.actions) else {
            return out;
        };
        // If we haven't yet hit space, don't suggest subactions.
        if !parsed_text.replace(main, "").starts_with('_') {
            return out;
        }
        let possibilities = self.complete_action(
            main,
            last_space_word(current_action_line),
            current_action_line,
            dates,
        );
        if !possibilities.is_empty() {
            let last_word_len = chars(last_space_word(text));
            out.extend(
                possibilities
                    .into_iter()
                    .map(|p| Completion::new(p, last_word_len, last_word_len)),
            );
            return out;
        }
        if !yielded {
            out.push(Completion::empty());
        }
        out
    }

    /// `getattr(shell, f'complete_{main}')(text, line, 0, 0)`, or `[]` without a completer.
    fn complete_action(
        &self,
        main: &str,
        _text: &str,
        line: &str,
        dates: &DateContext,
    ) -> Vec<String> {
        match main {
            "instance" => self.complete_instance(line, dates, false, &[]),
            "repo" => self
                .complete_instance(line, dates, false, &[])
                .into_iter()
                .filter(|r| r.starts_with("api:"))
                .collect(),
            "executor" => self
                .complete_instance(line, dates, true, &["local"])
                .into_iter()
                .filter(|r| EXECUTOR_TYPES.contains(&r.split(':').next().unwrap_or("")))
                .collect(),
            // `debug` and `daemon` are added to `_actions` after the completers are inserted,
            // so the shell has no `complete_debug` / `complete_daemon`.
            "debug" | "daemon" => Vec::new(),
            _ if RESERVED_COMPLETERS.contains(&main) => Vec::new(),
            _ => self.completer_wrapper(main, line, dates),
        }
    }

    /// `Shell.complete_instance(text, line, ...)`.
    fn complete_instance(
        &self,
        line: &str,
        dates: &DateContext,
        executor: bool,
        additional: &[&str],
    ) -> Vec<String> {
        let args = parse_line(line, dates);
        let action = action_words(&args);
        let text = action.get(1).map(String::as_str).unwrap_or("");
        let types = if executor {
            EXECUTOR_TYPES
        } else {
            INSTANCE_TYPES
        };
        self.connector_labels(types, text, true, additional)
    }

    /// `_completer_wrapper(target)(text, line, ...)`.
    fn completer_wrapper(&self, main: &str, line: &str, dates: &DateContext) -> Vec<String> {
        if let Some(keys) = self.check_complete_keys(line) {
            return keys;
        }
        let args = parse_line(line, dates);
        let mut action = action_words(&args);
        let executor_keys = args
            .get("executor_keys")
            .filter(|v| !matches!(v, PyValue::None))
            .map(PyValue::py_str);
        let custom = CUSTOM_COMPLETERS.contains(&main);
        if !custom {
            if let Some(p) = &self.plugins {
                let rest: Vec<String> = action.iter().skip(1).cloned().collect();
                if let Some(found) = p.complete(main, &rest, line) {
                    return found;
                }
            }
            return self.default_action_completer(&action);
        }
        if !action.is_empty() {
            action.remove(0);
        }
        let kw = Kw {
            line,
            executor_keys: executor_keys.as_deref(),
        };
        self.custom_completer(main, action, &kw)
    }

    /// `default_action_completer(action=...)`.
    pub fn default_action_completer(&self, action: &[String]) -> Vec<String> {
        let Some(first) = action.first() else {
            return Vec::new();
        };
        let sub = action.get(1).map(String::as_str).unwrap_or("");
        let mut out: Vec<String> = actions::get_subactions(self.actions, first)
            .iter()
            .map(|s| s.name.to_string())
            .filter(|s| s.starts_with(sub) && s != sub)
            .collect();
        out.sort();
        out
    }

    /// `_check_complete_keys(line)`.
    fn check_complete_keys(&self, line: &str) -> Option<Vec<String>> {
        let last_word = last_space_word(line.trim_end_matches(' '));
        let ends_with_space = line.ends_with(' ');
        if last_word.starts_with('-') {
            let mut possibilities = Vec::new();
            for triggers in arguments_triggers().values() {
                for trigger in triggers {
                    if trigger.starts_with(last_word) && (*trigger != last_word || !ends_with_space)
                    {
                        possibilities.push(trigger.to_string());
                    }
                }
            }
            for argument in &self.plugin_arguments {
                for trigger in &argument.option_strings {
                    if trigger.starts_with(last_word) && (trigger != last_word || !ends_with_space)
                    {
                        possibilities.push(trigger.clone());
                    }
                }
            }
            if !ends_with_space {
                return Some(possibilities);
            }
        }
        if last_word.starts_with('-')
            && !last_word.ends_with('-')
            && ends_with_space
            && !TRIGGER_ARGS.contains(&last_word)
        {
            // Return an empty list so an incomplete line is not parsed.
            return Some(Vec::new());
        }
        if TRIGGER_ARGS.contains(&last_word) {
            return Some(self.connector_labels(&[], "", true, &[]));
        }
        for trigger in TRIGGER_ARGS {
            // PARITY: a substring test, so `--cron` contains the `-c` trigger.
            if line.contains(trigger) {
                if line.trim_end_matches(' ').ends_with(trigger) {
                    return Some(self.connector_labels(&[], "", true, &[]));
                }
                return Some(self.connector_labels(&[], last_word, true, &[]));
            }
        }
        None
    }

    /// The jobs for the job completers (`_complete_delete_jobs`).
    fn complete_jobs(&self, action: &[String], kw: &Kw<'_>, methods: &[&str]) -> Vec<String> {
        let executor_keys = kw.executor_keys.unwrap_or(&self.executor_keys);
        // PARITY: only `local` and `systemd` jobs are listed without a network round trip;
        // v4 also queries `api:` executors here.
        if executor_keys != "local" && executor_keys != "systemd" {
            return Vec::new();
        }
        let Some(list) = &self.jobs else {
            return Vec::new();
        };
        let all = list(executor_keys);
        let jobs: Vec<String> = if methods.is_empty() {
            all.iter().map(|j| j.name.clone()).collect()
        } else {
            let mut names: Vec<String> = Vec::new();
            for method in methods {
                for j in &all {
                    let keep = match *method {
                        "restart" => j.restart,
                        status => j.status == status,
                    };
                    if keep && !names.contains(&j.name) {
                        names.push(j.name.clone());
                    }
                }
            }
            names
        };
        if action.is_empty() {
            return jobs;
        }
        let line_end = last_space_word(kw.line);
        let last = action.last().map(String::as_str).unwrap_or("");
        jobs.into_iter()
            .filter(|name| !action.contains(name))
            .filter(|name| line_end.is_empty() || name.starts_with(last))
            .collect()
    }

    fn keys_completer(keys: &[String], action: &[String]) -> Vec<String> {
        match action.first() {
            None => keys.to_vec(),
            Some(first) => keys
                .iter()
                .filter(|k| k.starts_with(first.as_str()) && *k != first)
                .cloned()
                .collect(),
        }
    }

    /// The driver names `install driver`, `uninstall driver` and `show drivers` take (v5): the
    /// ones not typed yet that start with the word being completed.
    fn driver_completer(names: &[&str], action: &[String], line: &str) -> Vec<String> {
        let prefix = match action.last() {
            Some(last) if !line.ends_with(' ') => last.as_str(),
            _ => "",
        };
        names
            .iter()
            .filter(|n| n.starts_with(prefix) && !action.iter().any(|a| a == **n))
            .map(|n| n.to_string())
            .collect()
    }

    fn edit_plugins(&self, action: &[String], line: &str) -> Vec<String> {
        let names = &self.plugin_names;
        let Some(last) = action.last() else {
            return names.clone();
        };
        if names.contains(last) && line.ends_with(' ') {
            return names
                .iter()
                .filter(|n| !action.contains(n))
                .cloned()
                .collect();
        }
        names
            .iter()
            .filter(|n| n.starts_with(last.as_str()) && !action.contains(n))
            .cloned()
            .collect()
    }

    fn connectors_completer(&self, action: &[String], line: &str) -> Vec<String> {
        let types: Vec<&str> = self.connectors.keys().map(String::as_str).collect();
        let search = if last_space_word(line).is_empty() || action.is_empty() {
            ""
        } else {
            action.last().map(String::as_str).unwrap_or("")
        };
        self.connector_labels(&types, search, true, &[])
    }

    fn show_connectors(&self, action: &[String]) -> Vec<String> {
        let text = action.first().map(String::as_str).unwrap_or("");
        self.connector_labels(&[], text, true, &[])
    }

    fn delete_cache(&self, action: &[String], line: &str) -> Vec<String> {
        let search = if last_space_word(line).is_empty() || action.is_empty() {
            ""
        } else {
            action.last().map(String::as_str).unwrap_or("")
        };
        let exclude: Vec<String> = if action.is_empty() {
            line.split(' ').map(str::to_string).collect()
        } else {
            action.to_vec()
        };
        self.connector_labels(&["sql", "valkey"], search, true, &[])
            .into_iter()
            .filter(|l| !exclude.contains(l))
            .collect()
    }

    fn copy_pipes(&self, action: &[String], line: &str) -> Vec<String> {
        let raw: Vec<&str> = line.split_whitespace().collect();
        let positionals: Vec<&str> = match raw.iter().position(|t| *t == "pipes") {
            Some(ix) => raw[ix + 1..]
                .iter()
                .copied()
                .filter(|t| !t.starts_with('-'))
                .collect(),
            None => Vec::new(),
        };
        let ends_with_space = line.ends_with(' ') || line.ends_with('\t');
        if positionals.len() > 1 || (positionals.len() == 1 && ends_with_space) {
            return Vec::new();
        }
        let search = if ends_with_space || action.is_empty() {
            ""
        } else {
            action.last().map(String::as_str).unwrap_or("")
        };
        self.connector_labels(INSTANCE_TYPES, search, true, &[])
    }

    /// The `_complete_<action>` functions of `meerschaum/actions/*.py` (`action` is the
    /// words after the action).
    fn custom_completer(&self, main: &str, mut action: Vec<String>, kw: &Kw<'_>) -> Vec<String> {
        // The dispatch shared by most completers: a known subaction word, not the word being
        // typed (`kw['line'].split(' ')[-1] != action[0]`), selects a sub-completer.
        let line_end = last_space_word(kw.line);
        let first = action.first().cloned().unwrap_or_default();
        let dispatch = |subs: &[&str], check_line: bool| -> Option<String> {
            (!action.is_empty()
                && subs.contains(&first.as_str())
                && (!check_line || line_end != first))
                .then(|| first.clone())
        };
        let default_for = |name: &str, action: &[String]| {
            let mut full = vec![name.to_string()];
            full.extend(action.iter().cloned());
            self.default_action_completer(&full)
        };
        match main {
            "show" => {
                let subs = [
                    "connector",
                    "connectors",
                    "config",
                    "package",
                    "packages",
                    "job",
                    "jobs",
                    "log",
                    "logs",
                    "driver",
                    "drivers",
                    "completion",
                    "completions",
                ];
                match dispatch(&subs, true) {
                    Some(sub) => {
                        action.remove(0);
                        match sub.as_str() {
                            "driver" | "drivers" => {
                                Self::driver_completer(DRIVER_NAMES, &action, kw.line)
                            }
                            "completion" | "completions" => Self::driver_completer(
                                crate::completions::SHELL_NAMES,
                                &action,
                                kw.line,
                            ),
                            "connector" | "connectors" => self.show_connectors(&action),
                            "config" => Self::keys_completer(&self.config_keys, &action),
                            "package" | "packages" => {
                                let keys: Vec<String> =
                                    PACKAGE_GROUPS.iter().map(|s| s.to_string()).collect();
                                Self::keys_completer(&keys, &action)
                            }
                            _ => self.complete_jobs(&action, kw, &[]),
                        }
                    }
                    None => default_for("show", &action),
                }
            }
            "copy" => match dispatch(&["pipes", "connector", "connectors"], true) {
                Some(sub) => {
                    action.remove(0);
                    if sub == "pipes" {
                        self.copy_pipes(&action, kw.line)
                    } else {
                        self.connectors_completer(&action, kw.line)
                    }
                }
                None => default_for("copy", &action),
            },
            "delete" => {
                let subs = ["connector", "connectors", "config", "job", "jobs", "cache"];
                match dispatch(&subs, true) {
                    Some(sub) => {
                        action.remove(0);
                        match sub.as_str() {
                            "connector" | "connectors" => {
                                self.connectors_completer(&action, kw.line)
                            }
                            "config" => Self::keys_completer(&self.config_keys, &action),
                            "cache" => self.delete_cache(&action, kw.line),
                            _ => self.complete_jobs(&action, kw, &[]),
                        }
                    }
                    None => default_for("delete", &action),
                }
            }
            "attach" => match dispatch(&["job", "jobs", "log", "logs"], true) {
                Some(_) => {
                    action.remove(0);
                    self.complete_jobs(&action, kw, &[])
                }
                None => default_for("attach", &action),
            },
            "clear" => match dispatch(&["cache"], true) {
                Some(_) => {
                    action.remove(0);
                    self.delete_cache(&action, kw.line)
                }
                None => default_for("clear", &action),
            },
            "install" => {
                let subs = [
                    "plugin",
                    "plugins",
                    "package",
                    "packages",
                    "required",
                    "driver",
                    "drivers",
                    "completion",
                    "completions",
                ];
                // PARITY: `install` dispatches without the line check.
                match dispatch(&subs, false) {
                    Some(sub) => {
                        action.remove(0);
                        match sub.as_str() {
                            "driver" | "drivers" => {
                                Self::driver_completer(DRIVER_NAMES, &action, kw.line)
                            }
                            "completion" | "completions" => Self::driver_completer(
                                crate::completions::SHELL_NAMES,
                                &action,
                                kw.line,
                            ),
                            // Disabled in v4 so typing is not interrupted by repository calls.
                            "plugin" | "plugins" | "package" | "packages" => Vec::new(),
                            _ => self.edit_plugins(&action, kw.line),
                        }
                    }
                    None => default_for("install", &action),
                }
            }
            "uninstall" => match dispatch(
                &[
                    "plugin",
                    "plugins",
                    "driver",
                    "drivers",
                    "completion",
                    "completions",
                ],
                true,
            ) {
                Some(sub) => {
                    action.remove(0);
                    if sub.starts_with("completion") {
                        Self::driver_completer(crate::completions::SHELL_NAMES, &action, kw.line)
                    } else if sub.starts_with("driver") {
                        // Only DuckDB's library is downloaded by Meerschaum.
                        Self::driver_completer(&["duckdb"], &action, kw.line)
                    } else {
                        self.edit_plugins(&action, kw.line)
                    }
                }
                None => default_for("uninstall", &action),
            },
            "pause" => match dispatch(&["job", "jobs"], true) {
                Some(_) => {
                    action.remove(0);
                    self.complete_jobs(&action, kw, &["running"])
                }
                // PARITY: `_complete_pause` falls back to `start`'s subactions.
                None => default_for("start", &action),
            },
            "edit" => match dispatch(&["config", "plugin", "plugins", "job", "jobs"], true) {
                Some(sub) => {
                    action.remove(0);
                    match sub.as_str() {
                        "config" => Self::keys_completer(&self.config_keys, &action),
                        "plugin" | "plugins" => self.edit_plugins(&action, kw.line),
                        _ => self.complete_jobs(&action, kw, &[]),
                    }
                }
                None => default_for("edit", &action),
            },
            "register" => {
                // PARITY: `register` dispatches without the line check.
                match dispatch(&["plugin", "plugins", "connector", "connectors"], false) {
                    Some(sub) => {
                        action.remove(0);
                        if sub.starts_with("plugin") {
                            self.edit_plugins(&action, kw.line)
                        } else {
                            self.show_connectors(&action)
                        }
                    }
                    None => default_for("register", &action),
                }
            }
            "login" => {
                let search = action.last().map(String::as_str).unwrap_or("");
                self.connector_labels(&["api"], search, true, &[])
            }
            "setup" => match dispatch(&["plugin", "plugins"], false) {
                Some(_) => {
                    action.remove(0);
                    self.edit_plugins(&action, kw.line)
                }
                None => default_for("setup", &action),
            },
            "sql" => {
                let text = action.first().map(String::as_str).unwrap_or("");
                self.connector_labels(&["sql"], text, true, &[])
                    .into_iter()
                    .map(|l| {
                        l.split_once("sql:")
                            .map(|(_, r)| r.to_string())
                            .unwrap_or(l)
                    })
                    .collect()
            }
            "stop" => match dispatch(&["job", "jobs"], true) {
                Some(_) => {
                    action.remove(0);
                    self.complete_jobs(&action, kw, &["running", "paused", "restart"])
                }
                None => default_for("stop", &action),
            },
            "start" => match dispatch(&["job", "jobs", "connector", "connectors"], true) {
                Some(sub) => {
                    action.remove(0);
                    if sub.starts_with("job") {
                        self.complete_jobs(&action, kw, &["stopped", "paused"])
                    } else {
                        self.show_connectors(&action)
                    }
                }
                None => default_for("start", &action),
            },
            "restart" => match dispatch(&["job", "jobs"], true) {
                Some(_) => {
                    action.remove(0);
                    self.complete_jobs(&action, kw, &["restart"])
                }
                None => default_for("restart", &action),
            },
            "compose" => meerschaum_compose::subactions::complete_compose(&action),
            _ => default_for(main, &action),
        }
    }
}

/// The actions with a `_complete_<action>` function in v4 (`get_completer`).
const CUSTOM_COMPLETERS: &[&str] = &[
    "show",
    "copy",
    "delete",
    "attach",
    "clear",
    "install",
    "uninstall",
    "pause",
    "edit",
    "register",
    "login",
    "setup",
    "sql",
    "stop",
    "start",
    "restart",
    "compose",
];

/// The keyword arguments a completer reads besides `action`.
struct Kw<'a> {
    line: &'a str,
    executor_keys: Option<&'a str>,
}

fn action_words(args: &crate::value::ArgsMap) -> Vec<String> {
    args.get("action")
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default()
}

/// The valid first words for `ValidAutoSuggest` (`'do_' + word in dir(shell)`).
pub fn is_valid_first_word(do_names: &[String], word: &str) -> bool {
    do_names.iter().any(|n| n == word)
}

/// `ValidAutoSuggest.get_suggestion`: the newest history line (last line of each entry first)
/// that starts with the current line and whose first word is a `do_` name.
pub fn auto_suggest(history: &[String], do_names: &[String], text: &str) -> Option<String> {
    let current = text.rsplit('\n').next().unwrap_or("");
    if current.trim().is_empty() {
        return None;
    }
    for entry in history.iter().rev() {
        for line in py_splitlines(entry).into_iter().rev() {
            let first = line.split(' ').next().unwrap_or("");
            if line.starts_with(current) && is_valid_first_word(do_names, first) {
                return Some(line[current.len()..].to_string());
            }
        }
    }
    None
}

/// Python `str.splitlines()` for the line breaks that occur in shell input.
fn py_splitlines(s: &str) -> Vec<&str> {
    let mut out: Vec<&str> = s
        .split('\n')
        .map(|l| l.strip_suffix('\r').unwrap_or(l))
        .collect();
    if s.ends_with('\n') {
        out.pop();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> CompletionData {
        let mut connectors = IndexMap::new();
        connectors.insert("sql".to_string(), vec!["main".into(), "local".into()]);
        connectors.insert("api".to_string(), vec!["mrsm".into()]);
        CompletionData {
            connectors,
            ..CompletionData::default()
        }
    }

    fn texts(c: &[Completion]) -> Vec<&str> {
        c.iter().map(|c| c.text.as_str()).collect()
    }

    #[test]
    fn names_subactions_and_fallback() {
        let d = data();
        let dates = DateContext::system();
        assert_eq!(texts(&d.get_completions("sh", &dates)), vec!["sh", "show"]);
        assert_eq!(texts(&d.get_completions("show pipes", &dates)), vec![""]);
        assert_eq!(
            texts(&d.get_completions("show pipes -i s", &dates)),
            vec!["sql:local", "sql:main"]
        );
        assert!(d.get_completions("", &dates).is_empty());
        assert!(d.get_completions("foo", &dates).is_empty());
    }

    #[test]
    fn suggestions_come_from_valid_history_lines() {
        let names = default_do_names(*SHELL_ACTIONS);
        let history = vec![
            "show pipes".to_string(),
            "notanaction x".to_string(),
            "show version".to_string(),
        ];
        assert_eq!(
            auto_suggest(&history, &names, "sho").as_deref(),
            Some("w version")
        );
        assert_eq!(auto_suggest(&history, &names, "no"), None);
        assert_eq!(auto_suggest(&history, &names, " "), None);
    }
}
